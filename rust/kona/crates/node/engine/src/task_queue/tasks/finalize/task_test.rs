use crate::{
    ConsolidateInput, ConsolidateTask, Engine, EngineState, EngineSyncStateUpdate, EngineTask,
    EngineTaskError, EngineTaskErrorSeverity, ExecutableEngineTask, FinalizeBlockId, FinalizeTask,
    FinalizeTaskError, InsertTask, NoopBlockSink,
    test_utils::{RpcMock, TestEngineStateBuilder, test_block_info, test_engine_client},
};
use alloy_primitives::{Address, Sealed};
use alloy_rpc_types_engine::{
    ExecutionPayloadV1, ForkchoiceUpdated, PayloadStatus, PayloadStatusEnum,
};
use alloy_rpc_types_eth::{Block, BlockTransactions, Header};
use kona_genesis::RollupConfig;
use kona_protocol::{L1BlockInfoBedrock, L2BlockInfo};
use op_alloy_consensus::{OpTxEnvelope, TxDeposit};
use op_alloy_rpc_types_engine::OpExecutionPayloadEnvelope;
use rstest::rstest;
use std::sync::Arc;

type RpcBlock = Block<op_alloy_rpc_types::Transaction>;

const SAFE: u64 = 10;
const ABOVE_SAFE: u64 = SAFE + 1;

#[derive(Clone, Copy, Debug)]
enum Lookup {
    ByHash,
    ByNumber,
}

/// What the engine returns when the task looks up the target.
#[derive(Clone, Copy, Debug)]
enum Reply {
    Missing,
    NotQueried,
    OtherHash,
    OtherHeight,
    Target,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Finalized,
    Mismatch,
    NotFound,
    Skipped,
}

/// An L2 block the default rollup config can turn into an [`L2BlockInfo`]. Blocks at the same
/// height with different timestamps have different hashes.
fn l2_block(number: u64, timestamp: u64) -> RpcBlock {
    let header = Header::new(alloy_consensus::Header { number, timestamp, ..Default::default() });
    let l1_info_deposit = OpTxEnvelope::Deposit(Sealed::new(TxDeposit {
        input: L1BlockInfoBedrock::default().encode_calldata(),
        ..Default::default()
    }));
    let l1_info_deposit = op_alloy_rpc_types::Transaction::from_transaction(
        alloy_consensus::transaction::Recovered::new_unchecked(l1_info_deposit, Address::ZERO),
        Default::default(),
    );
    Block::new(header, BlockTransactions::Full(vec![l1_info_deposit]))
}

fn target_block(number: u64) -> RpcBlock {
    l2_block(number, number * 2)
}

fn block_info(block: &RpcBlock) -> L2BlockInfo {
    L2BlockInfo::from_block_and_genesis(
        &block.clone().into_consensus(),
        &RollupConfig::default().genesis,
    )
    .unwrap()
}

fn initial_state() -> EngineState {
    TestEngineStateBuilder::new()
        .with_unsafe_head(test_block_info(20))
        .with_safe_head(test_block_info(SAFE))
        .with_finalized_head(test_block_info(5))
        .build()
}

fn with_finalized(state: EngineState, finalized: L2BlockInfo) -> EngineState {
    let finalized_update =
        EngineSyncStateUpdate { finalized_head: Some(finalized), ..Default::default() };
    EngineState { sync_state: state.sync_state.apply_update(finalized_update), ..state }
}

fn block_id(lookup: Lookup, target: L2BlockInfo) -> FinalizeBlockId {
    match lookup {
        Lookup::ByHash => FinalizeBlockId::ByHash(target.block_info.id()),
        Lookup::ByNumber => FinalizeBlockId::ByNumber(target.block_info.number),
    }
}

fn reply_block(reply: Reply, number: u64) -> Option<RpcBlock> {
    match reply {
        Reply::Missing | Reply::NotQueried => None,
        Reply::OtherHash => Some(l2_block(number, number * 2 + 1)),
        Reply::OtherHeight => Some(target_block(number + 1)),
        Reply::Target => Some(target_block(number)),
    }
}

fn expect_lookup(l2: &RpcMock, block_id: FinalizeBlockId, reply: Option<RpcBlock>) {
    match block_id {
        FinalizeBlockId::ByHash(id) => {
            l2.expect_params("eth_getBlockByHash", serde_json::json!([id.hash, true]), reply)
        }
        FinalizeBlockId::ByNumber(number) => l2.expect_params(
            "eth_getBlockByNumber",
            serde_json::json!([format!("{number:#x}"), true]),
            reply,
        ),
    }
}

fn expect_finalizing_forkchoice(l2: &RpcMock, state: &EngineState, finalized: L2BlockInfo) {
    let forkchoice = with_finalized(*state, finalized).sync_state.create_forkchoice_state();
    l2.expect_params(
        "engine_forkchoiceUpdatedV3",
        serde_json::json!([forkchoice, null]),
        ForkchoiceUpdated::new(PayloadStatus::from_status(PayloadStatusEnum::Valid)),
    );
}

fn outcome_of(result: Result<(), FinalizeTaskError>, finalized_moved: bool) -> Outcome {
    let Err(err) = result else {
        return if finalized_moved { Outcome::Finalized } else { Outcome::Skipped };
    };
    assert_eq!(err.severity(), EngineTaskErrorSeverity::Critical, "{err:?}");
    match err {
        FinalizeTaskError::BlockIdentityMismatch { .. } => Outcome::Mismatch,
        FinalizeTaskError::BlockNotFound(_) => Outcome::NotFound,
        err => panic!("unexpected finalize error: {err:?}"),
    }
}

/// A target above the safe head is skipped, since a reset can lower the safe head below it and a
/// later request finalizes it or a newer block. A pinned target the engine does not have yet is
/// skipped too, so delegated polling can resubmit it. A pinned lookup never settles for a
/// different block, and a by-number target the engine does not have fails hard.
#[rstest]
#[case::by_hash_missing(Lookup::ByHash, SAFE, Reply::Missing, Outcome::Skipped)]
#[case::by_hash_present(Lookup::ByHash, SAFE, Reply::Target, Outcome::Finalized)]
#[case::by_hash_other_height(Lookup::ByHash, SAFE, Reply::OtherHeight, Outcome::Mismatch)]
#[case::by_hash_other_hash(Lookup::ByHash, SAFE, Reply::OtherHash, Outcome::Mismatch)]
#[case::by_hash_above_safe(Lookup::ByHash, ABOVE_SAFE, Reply::NotQueried, Outcome::Skipped)]
#[case::by_number_missing(Lookup::ByNumber, SAFE, Reply::Missing, Outcome::NotFound)]
#[case::by_number_present(Lookup::ByNumber, SAFE, Reply::Target, Outcome::Finalized)]
#[case::by_number_above_safe(Lookup::ByNumber, ABOVE_SAFE, Reply::NotQueried, Outcome::Skipped)]
#[tokio::test]
async fn finalize_target(
    #[case] lookup: Lookup,
    #[case] target_number: u64,
    #[case] reply: Reply,
    #[case] expected: Outcome,
) {
    let cfg = Arc::new(RollupConfig::default());
    let (client, l1, l2) = test_engine_client(cfg.clone());
    let target = block_info(&target_block(target_number));
    let block_id = block_id(lookup, target);
    let initial = initial_state();
    let mut state = initial;
    if !matches!(reply, Reply::NotQueried) {
        expect_lookup(&l2, block_id, reply_block(reply, target_number));
    }
    if expected == Outcome::Finalized {
        expect_finalizing_forkchoice(&l2, &state, target);
    }

    let result = FinalizeTask::new(Arc::new(client), cfg, block_id).execute(&mut state).await;

    let finalized_moved = state.sync_state.finalized_head() != initial.sync_state.finalized_head();
    assert_eq!(outcome_of(result, finalized_moved), expected);
    let expected_state =
        if expected == Outcome::Finalized { with_finalized(initial, target) } else { initial };
    assert_eq!(state, expected_state);
    l1.assert_finished();
    l2.assert_finished();
}

/// While the engine catches up, the pinned target is skipped without a forkchoice update. Once
/// the engine has the block, resubmitting the same target finalizes it.
#[tokio::test]
async fn missing_pinned_target_finalizes_once_engine_has_it() {
    let cfg = Arc::new(RollupConfig::default());
    let (client, l1, l2) = test_engine_client(cfg.clone());
    let target = block_info(&target_block(SAFE));
    let block_id = FinalizeBlockId::ByHash(target.block_info.id());
    let task = FinalizeTask::new(Arc::new(client), cfg, block_id);
    let initial = initial_state();
    let mut state = initial;

    expect_lookup(&l2, block_id, None);
    task.execute(&mut state).await.unwrap();
    assert_eq!(state, initial);
    l2.assert_finished();

    expect_lookup(&l2, block_id, Some(target_block(SAFE)));
    expect_finalizing_forkchoice(&l2, &state, target);
    task.execute(&mut state).await.unwrap();
    assert_eq!(state, with_finalized(initial, target));
    l1.assert_finished();
    l2.assert_finished();
}

/// A delegated safe head ahead of the engine starts EL sync, so the engine answers `SYNCING` and
/// does not have the delegated finalized block yet. Skipping that target must leave the queue
/// empty, so the unsafe imports that let the engine catch up keep running.
#[tokio::test]
async fn missing_pinned_target_does_not_hold_the_queue() {
    let cfg = Arc::new(RollupConfig::default());
    let (client, l1, l2) = test_engine_client(cfg.clone());
    let client = Arc::new(client);
    let state = initial_state();
    let (state_tx, state_rx) = tokio::sync::watch::channel(state);
    let mut engine = Engine::new(state, state_tx);

    let delegated_safe = test_block_info(30);
    l2.expect(
        "engine_forkchoiceUpdatedV3",
        ForkchoiceUpdated::new(PayloadStatus::from_status(PayloadStatusEnum::Syncing)),
    );
    engine.enqueue(EngineTask::Consolidate(Box::new(ConsolidateTask::new(
        client.clone(),
        cfg.clone(),
        ConsolidateInput::BlockInfo(delegated_safe),
        Arc::new(NoopBlockSink),
    ))));
    let delegated_finalized =
        FinalizeBlockId::ByHash(block_info(&target_block(15)).block_info.id());
    expect_lookup(&l2, delegated_finalized, None);
    engine.enqueue(EngineTask::Finalize(Box::new(FinalizeTask::new(
        client.clone(),
        cfg,
        delegated_finalized,
    ))));

    engine.drain().await.unwrap();

    assert!(engine.is_empty(), "a skipped finalization target must not stay queued");
    assert_eq!(state_rx.borrow().sync_state.finalized_head(), state.sync_state.finalized_head());

    let (insert_config, payload) = unsafe_payload_after(delegated_safe);
    l2.expect("engine_newPayloadV1", PayloadStatus::from_status(PayloadStatusEnum::Valid));
    l2.expect(
        "engine_forkchoiceUpdatedV3",
        ForkchoiceUpdated::new(PayloadStatus::from_status(PayloadStatusEnum::Valid)),
    );
    engine.enqueue(EngineTask::Insert(Box::new(InsertTask::new(
        client,
        Arc::new(insert_config),
        payload,
        false,
        Arc::new(NoopBlockSink),
    ))));

    engine.drain().await.unwrap();

    assert!(engine.is_empty());
    assert_eq!(engine.state().sync_state.unsafe_head().block_info.number, 31);
    l1.assert_finished();
    l2.assert_finished();
}

/// Builds the payload for the child of `parent`, with a rollup config whose genesis is that
/// child, so its block info needs no L1-info deposit.
fn unsafe_payload_after(parent: L2BlockInfo) -> (RollupConfig, OpExecutionPayloadEnvelope) {
    let mut block = alloy_consensus::Block::<OpTxEnvelope>::default();
    block.header.number = parent.block_info.number + 1;
    block.header.parent_hash = parent.block_info.hash;
    let payload = OpExecutionPayloadEnvelope::V1(ExecutionPayloadV1::from_block_slow(&block));
    let imported: op_alloy_consensus::OpBlock = payload.clone().try_into_block().unwrap();
    let mut config = RollupConfig::default();
    config.genesis.l2.number = imported.header.number;
    config.genesis.l2.hash = imported.header.hash_slow();
    (config, payload)
}
