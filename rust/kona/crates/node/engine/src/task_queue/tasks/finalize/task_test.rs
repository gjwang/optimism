use crate::{
    EngineState, EngineSyncStateUpdate, ExecutableEngineTask, FinalizeBlockId, FinalizeTask,
    FinalizeTaskError,
    test_utils::{RpcMock, TestEngineStateBuilder, test_block_info, test_engine_client},
};
use alloy_consensus::{Header, transaction::Recovered};
use alloy_eips::BlockNumHash;
use alloy_primitives::{Address, Bytes, Sealed, b256};
use alloy_rpc_types_engine::{ForkchoiceUpdated, PayloadStatus, PayloadStatusEnum};
use alloy_rpc_types_eth::{BlockTransactions, Header as RpcHeader};
use kona_genesis::{ChainGenesis, RollupConfig};
use kona_protocol::{BlockInfo, L1BlockInfoBedrock, L2BlockInfo};
use op_alloy_consensus::{OpTxEnvelope, TxDeposit};
use rstest::rstest;
use std::sync::Arc;

type RpcBlock = alloy_rpc_types_eth::Block<op_alloy_rpc_types::Transaction>;

const STALE: u64 = 5;
const FINALIZED: u64 = 10;
const ADVANCED: u64 = 15;
const SAFE: u64 = 20;
const UNSAFE: u64 = 30;

/// The L2 genesis block, which carries no L1-info deposit.
fn genesis_block() -> RpcBlock {
    RpcBlock::new(RpcHeader::new(Header::default()), BlockTransactions::Full(vec![]))
}

fn rollup_config() -> Arc<RollupConfig> {
    let genesis = BlockNumHash { hash: genesis_block().header.hash, number: 0 };
    Arc::new(RollupConfig {
        genesis: ChainGenesis { l2: genesis, ..Default::default() },
        ..Default::default()
    })
}

/// An L2 block at `number`. Blocks built from different `fork` values share a height but not a
/// hash.
fn l2_block(number: u64, fork: u8) -> RpcBlock {
    let header = Header {
        extra_data: Bytes::from(vec![fork]),
        number,
        timestamp: number * 2,
        ..Default::default()
    };
    let l1_info_deposit = OpTxEnvelope::Deposit(Sealed::new(TxDeposit {
        input: L1BlockInfoBedrock::default().encode_calldata(),
        ..Default::default()
    }));
    let transaction = op_alloy_rpc_types::Transaction::from_transaction(
        Recovered::new_unchecked(l1_info_deposit, Address::ZERO),
        Default::default(),
    );
    RpcBlock::new(RpcHeader::new(header), BlockTransactions::Full(vec![transaction]))
}

fn canonical(number: u64) -> RpcBlock {
    l2_block(number, 0)
}

fn conflicting(number: u64) -> RpcBlock {
    l2_block(number, 1)
}

fn info(block: &RpcBlock) -> L2BlockInfo {
    L2BlockInfo::from_block_and_genesis(&block.clone().into_consensus(), &rollup_config().genesis)
        .unwrap()
}

fn by_hash(block: &RpcBlock) -> FinalizeBlockId {
    FinalizeBlockId::ByHash(info(block).block_info.id())
}

fn by_number(block: &RpcBlock) -> FinalizeBlockId {
    FinalizeBlockId::ByNumber(block.header.number)
}

/// Pins `block`'s hash but claims a different height for it.
fn by_hash_claiming_height(number: u64, block: &RpcBlock) -> FinalizeBlockId {
    FinalizeBlockId::ByHash(BlockNumHash { hash: info(block).block_info.hash, number })
}

fn established() -> L2BlockInfo {
    info(&canonical(FINALIZED))
}

fn uninitialized() -> L2BlockInfo {
    L2BlockInfo::default()
}

fn engine_state(finalized_head: L2BlockInfo) -> EngineState {
    TestEngineStateBuilder::new()
        .with_unsafe_head(test_block_info(UNSAFE))
        .with_safe_head(test_block_info(SAFE))
        .with_finalized_head(finalized_head)
        .build()
}

fn expect_lookup(l2: &RpcMock, target: FinalizeBlockId, served_block: RpcBlock) {
    match target {
        FinalizeBlockId::ByHash(id) => {
            l2.expect_params("eth_getBlockByHash", serde_json::json!([id.hash, true]), served_block)
        }
        FinalizeBlockId::ByNumber(number) => l2.expect_params(
            "eth_getBlockByNumber",
            serde_json::json!([format!("{number:#x}"), true]),
            served_block,
        ),
    }
}

fn expect_finalizing_forkchoice_update(
    l2: &RpcMock,
    state: &EngineState,
    finalized_head: L2BlockInfo,
) {
    let forkchoice = state
        .sync_state
        .apply_update(EngineSyncStateUpdate {
            finalized_head: Some(finalized_head),
            ..Default::default()
        })
        .create_forkchoice_state();
    l2.expect_params(
        "engine_forkchoiceUpdatedV3",
        serde_json::json!([forkchoice, null]),
        ForkchoiceUpdated::new(PayloadStatus::from_status(PayloadStatusEnum::Valid)),
    );
}

/// Whether a finalization target moves the finalized head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    /// The served block becomes the finalized head through a forkchoice update.
    Finalized,
    /// The finalized head is kept and no forkchoice update is sent.
    Kept,
}

/// Targets behind the finalized head, or a pinned hash that conflicts with it, are skipped before
/// asking the execution layer for the block. The strict RPC mock fails on any request.
#[rstest]
#[case::stale_by_hash(by_hash(&canonical(STALE)))]
#[case::stale_by_number(by_number(&canonical(STALE)))]
#[case::conflicting_by_hash(by_hash(&conflicting(FINALIZED)))]
#[tokio::test]
async fn finalize_task_skips_target_without_lookup(#[case] target: FinalizeBlockId) {
    let cfg = rollup_config();
    let (engine_client, l1, l2) = test_engine_client(cfg.clone());
    let mut state = engine_state(established());
    let original_state = state;

    let result = FinalizeTask::new(Arc::new(engine_client), cfg, target).execute(&mut state).await;

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(state, original_state);
    l1.assert_finished();
    l2.assert_finished();
}

/// Finality only moves forward. A block resolved by number that conflicts with the finalized head
/// is kept out, and an identical target sends no redundant forkchoice update.
#[rstest]
#[case::conflicting_by_number(established(), by_number, conflicting(FINALIZED), Outcome::Kept)]
#[case::identical_by_hash(established(), by_hash, canonical(FINALIZED), Outcome::Kept)]
#[case::identical_by_number(established(), by_number, canonical(FINALIZED), Outcome::Kept)]
#[case::advancing_by_hash(established(), by_hash, canonical(ADVANCED), Outcome::Finalized)]
#[case::advancing_by_number(established(), by_number, canonical(ADVANCED), Outcome::Finalized)]
#[case::uninitialized_adopts_genesis(uninitialized(), by_hash, genesis_block(), Outcome::Finalized)]
#[tokio::test]
async fn finalize_task_only_moves_finality_forward(
    #[case] finalized_head: L2BlockInfo,
    #[case] identify: fn(&RpcBlock) -> FinalizeBlockId,
    #[case] served_block: RpcBlock,
    #[case] outcome: Outcome,
) {
    let cfg = rollup_config();
    let (engine_client, l1, l2) = test_engine_client(cfg.clone());
    let mut state = engine_state(finalized_head);
    let target = identify(&served_block);
    let expected_finalized_head = match outcome {
        Outcome::Finalized => info(&served_block),
        Outcome::Kept => finalized_head,
    };
    expect_lookup(&l2, target, served_block);
    if outcome == Outcome::Finalized {
        expect_finalizing_forkchoice_update(&l2, &state, expected_finalized_head);
    }

    let result = FinalizeTask::new(Arc::new(engine_client), cfg, target).execute(&mut state).await;

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(state.sync_state.finalized_head(), expected_finalized_head);
    l1.assert_finished();
    l2.assert_finished();
}

/// A block served at a height other than the requested one is never finalized. Otherwise a
/// malformed by-number response could bypass the stale-target guard and regress finality.
#[rstest]
#[case::by_number_served_stale_block(FinalizeBlockId::ByNumber(ADVANCED), canonical(STALE))]
#[case::by_number_served_higher_block(FinalizeBlockId::ByNumber(ADVANCED), canonical(SAFE))]
#[case::by_hash_served_other_height(by_hash_claiming_height(ADVANCED, &canonical(STALE)), canonical(STALE))]
#[tokio::test]
async fn finalize_task_rejects_block_at_unrequested_height(
    #[case] target: FinalizeBlockId,
    #[case] served_block: RpcBlock,
) {
    let cfg = rollup_config();
    let (engine_client, l1, l2) = test_engine_client(cfg.clone());
    let mut state = engine_state(established());
    let original_state = state;
    expect_lookup(&l2, target, served_block);

    let result = FinalizeTask::new(Arc::new(engine_client), cfg, target).execute(&mut state).await;

    assert!(matches!(result, Err(FinalizeTaskError::BlockNotFound(ADVANCED))), "{result:?}");
    assert_eq!(state, original_state);
    l1.assert_finished();
    l2.assert_finished();
}

/// When the engine receives a `ByHash` finalize request for a block hash it doesn't have,
/// [`FinalizeTask`] must fail with [`FinalizeTaskError::BlockNotFound`] rather than silently
/// finalize whatever it happens to have at the same height.
///
/// Assert the exact by-hash RPC lookup, so a by-number lookup cannot silently finalize a
/// different canonical block at the same height.
#[tokio::test]
async fn finalize_task_by_hash_errors_when_engine_lacks_hash() {
    const N: u64 = 10;
    let hash_a = b256!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let hash_b = b256!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

    let cfg = Arc::new(RollupConfig::default());
    let (engine_client, l1, l2) = test_engine_client(cfg.clone());
    l2.expect_params(
        "eth_getBlockByHash",
        serde_json::json!([hash_b, true]),
        Option::<alloy_rpc_types_eth::Block<op_alloy_rpc_types::Transaction>>::None,
    );

    // Place the safe head at N so the sanity check (`safe_head.number >= block_id.number`) passes
    // and `execute()` reaches the lookup we care about.
    let safe_head = L2BlockInfo {
        block_info: BlockInfo {
            number: N,
            hash: hash_a,
            parent_hash: Default::default(),
            timestamp: N * 2,
        },
        l1_origin: BlockNumHash::default(),
        seq_num: 0,
    };
    let mut state = TestEngineStateBuilder::new()
        .with_unsafe_head(safe_head)
        .with_safe_head(safe_head)
        .with_finalized_head(L2BlockInfo::default())
        .build();

    let task = FinalizeTask::new(
        Arc::new(engine_client),
        cfg,
        FinalizeBlockId::ByHash(BlockNumHash { number: N, hash: hash_b }),
    );

    let result = task.execute(&mut state).await;

    assert!(
        matches!(result, Err(FinalizeTaskError::BlockNotFound(n)) if n == N),
        "expected BlockNotFound({N}) — got {result:?}. The by-hash lookup must fail loudly when \
         the engine lacks the requested hash; instead, the task either succeeded (finalizing the \
         wrong block) or surfaced a different error."
    );
    l1.assert_finished();
    l2.assert_finished();
}
