use super::*;
use crate::QueuedEngineDerivationClient;
use alloy_consensus::transaction::Recovered;
use alloy_primitives::{Address, B256, Sealed};
use alloy_provider::Network;
use alloy_rpc_types_engine::{
    ExecutionPayloadV1, ForkchoiceState, ForkchoiceUpdated, PayloadStatus, PayloadStatusEnum,
};
use alloy_rpc_types_eth::BlockTransactions;
use kona_engine::{
    EngineState, FinalizeBlockId, NoopBlockSink, ResetRequest,
    test_utils::{RpcMock, TestEngineStateBuilder, test_block_info, test_engine_client},
};
use kona_protocol::L1BlockInfoBedrock;
use op_alloy_consensus::{OpBlock, OpTxEnvelope, TxDeposit};
use op_alloy_network::Optimism;
use op_alloy_rpc_types_engine::OpExecutionPayloadEnvelope;

type RpcBlock = <Optimism as Network>::BlockResponse;
type RpcTransaction = <Optimism as Network>::TransactionResponse;

/// Exercise the real actor request/drain loop with forty consecutive unsafe blocks, without
/// depending on the removed dev RPC subscriptions. Observe EVERY enqueue, including a burst
/// already waiting in the inbound channel: healthy processing must keep the task queue <= 1.
#[tokio::test(start_paused = true)]
async fn healthy_unsafe_imports_keep_engine_queue_bounded() {
    let config = Arc::new(RollupConfig::default());
    let (client, l1, l2) = test_engine_client(config.clone());
    let initial_head = test_block_info(0);
    let state = TestEngineStateBuilder::new().with_unsafe_head(initial_head).build();
    let (state_tx, state_rx) = watch::channel(state);
    let (requests, request_rx) = mpsc::channel(40);
    let (derivation_tx, _derivation_rx) = mpsc::channel(8);
    let mut actor = EngineActor::new(
        Arc::new(client),
        config,
        QueuedEngineDerivationClient { derivation_actor_request_tx: derivation_tx },
        Engine::new(state, state_tx),
        None,
        request_rx,
        Arc::new(NoopBlockSink),
    );
    let mut expected_heads = vec![initial_head];
    for number in 1..=40 {
        let mut block = OpBlock::default();
        block.header.number = number;
        block.header.timestamp = number * 2;
        block.header.parent_hash = expected_heads.last().unwrap().hash();
        block.body.transactions.push(OpTxEnvelope::Deposit(Sealed::new(TxDeposit {
            input: L1BlockInfoBedrock::default().encode_calldata(),
            ..Default::default()
        })));
        let payload = ExecutionPayloadV1::from_block_slow(&block);
        let imported: OpBlock =
            OpExecutionPayloadEnvelope::V1(payload.clone()).try_into_block().unwrap();
        expected_heads.push(
            L2BlockInfo::from_block_and_genesis(&imported, &RollupConfig::default().genesis)
                .unwrap(),
        );
        l2.expect_params(
            "engine_newPayloadV1",
            serde_json::json!([payload]),
            PayloadStatus::from_status(PayloadStatusEnum::Valid),
        );
        l2.expect_params(
            "engine_forkchoiceUpdatedV3",
            serde_json::json!([
                alloy_rpc_types_engine::ForkchoiceState {
                    head_block_hash: expected_heads.last().unwrap().hash(),
                    safe_block_hash: initial_head.hash(),
                    finalized_block_hash: initial_head.hash(),
                },
                null
            ]),
            ForkchoiceUpdated::new(PayloadStatus::from_status(PayloadStatusEnum::Valid)),
        );
        requests
            .send(EngineActorRequest::ProcessUnsafeL2Block(Box::new(
                OpExecutionPayloadEnvelope::V1(payload),
            )))
            .await
            .unwrap();
    }
    for expected in &expected_heads[..40] {
        time::timeout(Duration::from_secs(1), actor.step()).await.unwrap().unwrap();
        assert_eq!(actor.engine.len(), 1, "no backlog may accumulate while the EL is healthy");
        assert_eq!(state_rx.borrow().sync_state.unsafe_head(), *expected);
    }
    // The final step imports block forty before discovering that its input has closed.
    drop(requests);
    assert!(matches!(actor.step().await, Err(EngineError::ChannelClosed)));
    assert!(actor.engine.is_empty());
    assert_eq!(state_rx.borrow().sync_state.unsafe_head(), expected_heads[40]);
    l1.assert_finished();
    l2.assert_finished();
}

/// An L2 block on `parent_hash` whose first transaction is the L1-info deposit, so it has an
/// [`L2BlockInfo`] under any rollup config.
fn l2_block(number: u64, parent_hash: B256) -> OpBlock {
    let mut block = OpBlock::default();
    block.header.number = number;
    block.header.parent_hash = parent_hash;
    block.header.timestamp = number * 2;
    block.body.transactions.push(OpTxEnvelope::Deposit(Sealed::new(TxDeposit {
        input: L1BlockInfoBedrock::default().encode_calldata(),
        ..Default::default()
    })));
    block
}

fn rpc_block(block: &OpBlock) -> RpcBlock {
    let transactions = block
        .body
        .transactions
        .iter()
        .cloned()
        .map(|transaction| {
            RpcTransaction::from_transaction(
                Recovered::new_unchecked(transaction, Address::ZERO),
                Default::default(),
            )
        })
        .collect();
    RpcBlock::new(
        alloy_rpc_types_eth::Header::new(block.header.clone()),
        BlockTransactions::Full(transactions),
    )
}

fn expect_valid_forkchoice(
    l2: &RpcMock,
    head: L2BlockInfo,
    safe: L2BlockInfo,
    finalized: L2BlockInfo,
) {
    let forkchoice = ForkchoiceState {
        finalized_block_hash: finalized.hash(),
        head_block_hash: head.hash(),
        safe_block_hash: safe.hash(),
    };
    l2.expect_params(
        "engine_forkchoiceUpdatedV3",
        serde_json::json!([forkchoice, null]),
        ForkchoiceUpdated::new(PayloadStatus::from_status(PayloadStatusEnum::Valid)),
    );
}

/// The reset finds the EL's finalized, safe and unsafe heads all at `genesis`, then synchronizes
/// the EL to them.
fn expect_reset_to_genesis(l1: &RpcMock, l2: &RpcMock, genesis_block: &RpcBlock) {
    for label in ["finalized", "safe", "latest"] {
        l2.expect_params("eth_getBlockByNumber", serde_json::json!([label, true]), genesis_block);
    }
    l1.expect(
        "eth_getBlockByHash",
        alloy_rpc_types_eth::Block::<alloy_rpc_types_eth::Transaction>::default(),
    );
    l2.expect(
        "engine_forkchoiceUpdatedV3",
        ForkchoiceUpdated::new(PayloadStatus::from_status(PayloadStatusEnum::Valid)),
    );
}

fn reset_request() -> (EngineActorRequest, mpsc::Receiver<kona_engine::EngineRequestResult<()>>) {
    let (result_tx, result_rx) = mpsc::channel(1);
    (EngineActorRequest::Reset(Box::new(ResetRequest { result_tx })), result_rx)
}

/// optimism#23757: a delegated sequencer exited with `Finalize(BlockNotSafe)` when an engine
/// reset restored a safe head below the finalized block the delegate had just reported. The
/// delegated target must wait across repeated resets without stopping the node, holding the
/// queue, or triggering another reset, and must finalize once the delegated safe head arrives.
#[tokio::test(start_paused = true)]
async fn delegated_finalization_survives_resets_below_target() {
    let genesis_block = RpcBlock::default();
    let mut config = RollupConfig::default();
    config.genesis.l2.hash = genesis_block.header.inner.hash_slow();
    let config = Arc::new(config);
    let genesis = L2BlockInfo::from_block_and_genesis(
        &genesis_block.clone().into_consensus(),
        &config.genesis,
    )
    .unwrap();
    let finalized_block = l2_block(15, B256::with_last_byte(14));
    let delegated_finalized =
        L2BlockInfo::from_block_and_genesis(&finalized_block, &config.genesis).unwrap();
    let delegated_finalize_request = || {
        EngineActorRequest::ProcessFinalizedL2Block(Box::new(FinalizeBlockId::ByHash(
            delegated_finalized.block_info.id(),
        )))
    };
    let delegated_safe = test_block_info(20);
    let unsafe_payload = OpExecutionPayloadEnvelope::V1(ExecutionPayloadV1::from_block_slow(
        &l2_block(1, genesis.hash()),
    ));
    let unsafe_head = L2BlockInfo::from_block_and_genesis(
        &unsafe_payload.clone().try_into_block::<OpTxEnvelope>().unwrap(),
        &config.genesis,
    )
    .unwrap();

    let (client, l1, l2) = test_engine_client(config.clone());
    let (requests, request_rx) = mpsc::channel(8);
    let (derivation_tx, _derivation_rx) = mpsc::channel(32);
    let (state_tx, state_rx) = watch::channel(EngineState::default());
    let mut actor = EngineActor::new(
        Arc::new(client),
        config.clone(),
        QueuedEngineDerivationClient { derivation_actor_request_tx: derivation_tx },
        Engine::new(EngineState::default(), state_tx),
        None,
        request_rx,
        Arc::new(NoopBlockSink),
    );

    // Startup reset, with the delegate's finalized target arriving right behind it.
    let (reset, mut first_reset_result) = reset_request();
    requests.send(reset).await.unwrap();
    actor.step().await.unwrap();
    expect_reset_to_genesis(&l1, &l2, &genesis_block);
    requests.send(delegated_finalize_request()).await.unwrap();
    actor.step().await.unwrap();
    assert!(first_reset_result.try_recv().unwrap().is_ok());
    assert_eq!(state_rx.borrow().sync_state.safe_head(), genesis);

    // The target is above the reset safe head: skipped without a lookup or forkchoice update,
    // and dropped from the queue, so the unsafe import queued behind it runs.
    requests
        .send(EngineActorRequest::ProcessUnsafeL2Block(Box::new(unsafe_payload)))
        .await
        .unwrap();
    actor.step().await.unwrap();
    assert_eq!(actor.engine.len(), 1, "only the unsafe import may remain queued");
    assert!(!actor.reset_pending);
    l2.expect("engine_newPayloadV1", PayloadStatus::from_status(PayloadStatusEnum::Valid));
    expect_valid_forkchoice(&l2, unsafe_head, genesis, genesis);
    let (reset, mut second_reset_result) = reset_request();
    requests.send(reset).await.unwrap();
    actor.step().await.unwrap();
    assert_eq!(state_rx.borrow().sync_state.unsafe_head(), unsafe_head);

    // A second reset lowers the heads again, and the delegate resubmits the same target.
    expect_reset_to_genesis(&l1, &l2, &genesis_block);
    requests.send(delegated_finalize_request()).await.unwrap();
    actor.step().await.unwrap();
    assert!(second_reset_result.try_recv().unwrap().is_ok());
    requests.send(EngineActorRequest::ProcessSafeL2Signal(delegated_safe.into())).await.unwrap();
    actor.step().await.unwrap();
    assert_eq!(actor.engine.len(), 1, "only the delegated safe head may remain queued");
    assert!(!actor.reset_pending);
    assert_eq!(state_rx.borrow().sync_state.finalized_head(), genesis);

    // Once the delegated safe head is applied, the resubmitted target finalizes.
    expect_valid_forkchoice(&l2, delegated_safe, delegated_safe, genesis);
    requests.send(delegated_finalize_request()).await.unwrap();
    actor.step().await.unwrap();
    assert_eq!(state_rx.borrow().sync_state.safe_head(), delegated_safe);
    l2.expect_params(
        "eth_getBlockByHash",
        serde_json::json!([delegated_finalized.hash(), true]),
        rpc_block(&finalized_block),
    );
    expect_valid_forkchoice(&l2, delegated_safe, delegated_safe, delegated_finalized);
    // With nothing left to do, the actor waits for requests instead of retrying.
    assert!(time::timeout(Duration::from_secs(60), actor.step()).await.is_err());
    assert_eq!(state_rx.borrow().sync_state.finalized_head(), delegated_finalized);
    assert!(actor.engine.is_empty());
    l1.assert_finished();
    l2.assert_finished();
}
