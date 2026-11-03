use super::synchronize;
use crate::{
    EngineState, EngineSyncStateUpdate, EngineTaskError, EngineTaskErrorSeverity, ForkchoiceStatus,
    test_utils::{RpcMock, TestEngineStateBuilder, test_block_info, test_engine_client},
};
use alloy_rpc_types_engine::{
    ForkchoiceState, ForkchoiceUpdated, INVALID_FORK_CHOICE_STATE_ERROR, PayloadStatus,
    PayloadStatusEnum,
};
use kona_genesis::RollupConfig;
use rstest::rstest;
use std::sync::Arc;

const FORKCHOICE_UPDATED: &str = "engine_forkchoiceUpdatedV3";

/// A forkchoice status, relative to the forkchoice being requested.
#[derive(Clone, Copy, Debug)]
enum Status {
    AppliedOther,
    AppliedTarget,
    SyncingTarget,
    Unsent,
}

/// How the execution layer answers the forkchoice update.
#[derive(Clone, Copy, Debug)]
enum Reply {
    Invalid,
    InvalidForkchoiceState,
    Syncing,
    Valid,
}

/// A synced engine state with random block hashes, so two calls yield different forkchoices.
fn engine_state() -> EngineState {
    TestEngineStateBuilder::new()
        .with_unsafe_head(test_block_info(20))
        .with_safe_head(test_block_info(10))
        .with_finalized_head(test_block_info(5))
        .build()
}

const fn forkchoice_status(
    status: Status,
    target: ForkchoiceState,
    other: ForkchoiceState,
) -> ForkchoiceStatus {
    match status {
        Status::AppliedOther => ForkchoiceStatus::Applied(other),
        Status::AppliedTarget => ForkchoiceStatus::Applied(target),
        Status::SyncingTarget => ForkchoiceStatus::Syncing(target),
        Status::Unsent => ForkchoiceStatus::Unsent,
    }
}

fn expect_forkchoice_updated(l2: &RpcMock, forkchoice: ForkchoiceState, reply: Reply) {
    let status = match reply {
        Reply::InvalidForkchoiceState => {
            l2.expect_error_code(FORKCHOICE_UPDATED, INVALID_FORK_CHOICE_STATE_ERROR as i64);
            return;
        }
        Reply::Invalid => PayloadStatusEnum::Invalid { validation_error: "invalid".into() },
        Reply::Syncing => PayloadStatusEnum::Syncing,
        Reply::Valid => PayloadStatusEnum::Valid,
    };
    l2.expect_params(
        FORKCHOICE_UPDATED,
        serde_json::json!([forkchoice, null]),
        ForkchoiceUpdated::new(PayloadStatus::from_status(status)),
    );
}

/// Sends an unchanged forkchoice and checks that only an acknowledged `VALID` suppresses the
/// request, and that the status records what the execution layer answered.
#[rstest]
#[case::unsent_is_sent(Status::Unsent, Some(Reply::Valid), Status::AppliedTarget, None)]
#[case::applied_target_is_skipped(Status::AppliedTarget, None, Status::AppliedTarget, None)]
#[case::applied_other_is_sent(
    Status::AppliedOther,
    Some(Reply::Valid),
    Status::AppliedTarget,
    None
)]
#[case::syncing_target_is_resent(
    Status::SyncingTarget,
    Some(Reply::Valid),
    Status::AppliedTarget,
    None
)]
#[case::syncing_answer_is_recorded(
    Status::Unsent,
    Some(Reply::Syncing),
    Status::SyncingTarget,
    None
)]
#[case::still_syncing(Status::SyncingTarget, Some(Reply::Syncing), Status::SyncingTarget, None)]
#[case::invalid_payload(
    Status::AppliedOther,
    Some(Reply::Invalid),
    Status::AppliedOther,
    Some(EngineTaskErrorSeverity::Reset)
)]
#[case::invalid_forkchoice_state(
    Status::AppliedOther,
    Some(Reply::InvalidForkchoiceState),
    Status::AppliedOther,
    Some(EngineTaskErrorSeverity::Reset)
)]
#[tokio::test]
async fn synchronize_skips_only_an_applied_forkchoice(
    #[case] status_before: Status,
    #[case] reply: Option<Reply>,
    #[case] status_after: Status,
    #[case] error_severity: Option<EngineTaskErrorSeverity>,
) {
    // The labels already equal the request, as a `SYNCING` answer or an intermediate span-batch
    // update leaves them, so only the forkchoice status can tell whether the request is needed.
    let mut state = engine_state();
    let target = state.sync_state.create_forkchoice_state();
    let other = engine_state().sync_state.create_forkchoice_state();
    state.forkchoice_status = forkchoice_status(status_before, target, other);
    let (client, l1, l2) = test_engine_client(Arc::new(RollupConfig::default()));
    if let Some(reply) = reply {
        expect_forkchoice_updated(&l2, target, reply);
    }

    let result = synchronize(&client, &mut state, EngineSyncStateUpdate::default()).await;

    assert_eq!(result.as_ref().err().map(EngineTaskError::severity), error_severity, "{result:?}");
    assert_eq!(state.forkchoice_status, forkchoice_status(status_after, target, other));
    l1.assert_finished();
    l2.assert_finished();
}

/// An execution layer that answers `SYNCING` and later `VALID` for the same, unchanged target is
/// asked twice, then no more once it applied the forkchoice.
#[tokio::test]
async fn unchanged_target_is_resent_after_syncing_until_valid() {
    let mut state = engine_state();
    let target = state.sync_state.create_forkchoice_state();
    let (client, l1, l2) = test_engine_client(Arc::new(RollupConfig::default()));
    expect_forkchoice_updated(&l2, target, Reply::Syncing);
    expect_forkchoice_updated(&l2, target, Reply::Valid);

    for _ in 0..3 {
        synchronize(&client, &mut state, EngineSyncStateUpdate::default()).await.unwrap();
    }

    assert_eq!(state.forkchoice_status, ForkchoiceStatus::Applied(target));
    l1.assert_finished();
    l2.assert_finished();
}
