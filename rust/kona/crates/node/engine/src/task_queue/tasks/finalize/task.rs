//! A task for finalizing an L2 block.

use crate::{
    EngineClient, EngineState, ExecutableEngineTask, FinalizeBlockId, FinalizeTaskError,
    state::EngineSyncStateUpdate, task_queue::synchronize,
};
use alloy_eips::{BlockId, BlockNumHash};
use async_trait::async_trait;
use derive_more::Constructor;
use kona_genesis::RollupConfig;
use kona_protocol::L2BlockInfo;
use std::sync::Arc;

/// The [`FinalizeTask`] fetches the [`L2BlockInfo`] identified by `block_id`, updates the
/// [`EngineState`], and dispatches a forkchoice update to finalize the block.
///
/// Finality only moves forward: a target below the finalized head, or a different block at the
/// finalized height, is logged and skipped without changing the engine state.
#[derive(Debug, Clone, Constructor)]
pub struct FinalizeTask {
    /// The engine client.
    pub client: Arc<EngineClient>,
    /// The rollup config.
    pub cfg: Arc<RollupConfig>,
    /// Identifier of the L2 block to finalize.
    pub block_id: FinalizeBlockId,
}

impl FinalizeTask {
    /// Whether finalizing a block at `target_number` would move finality backward.
    const fn is_behind_finalized_head(finalized_head: &L2BlockInfo, target_number: u64) -> bool {
        target_number < finalized_head.block_info.number
    }

    /// Whether `target` is a different block at the finalized head's height.
    ///
    /// The default head means finality has not been established yet, so any target, including
    /// the configured genesis, may initialize it.
    fn conflicts_with_finalized_head(finalized_head: &L2BlockInfo, target: BlockNumHash) -> bool {
        *finalized_head != L2BlockInfo::default() &&
            target.number == finalized_head.block_info.number &&
            target.hash != finalized_head.block_info.hash
    }

    fn log_conflicting_target(finalized_head: &L2BlockInfo, target: BlockNumHash) {
        error!(
            target: "engine",
            finalized_hash = %finalized_head.block_info.hash,
            number = target.number,
            target_hash = %target.hash,
            "Ignoring finalization target that conflicts with the finalized head"
        );
    }
}

#[async_trait]
impl ExecutableEngineTask for FinalizeTask {
    type Output = ();

    type Error = FinalizeTaskError;

    async fn execute(&self, state: &mut EngineState) -> Result<(), FinalizeTaskError> {
        let block_number = self.block_id.number();
        let finalized_head = state.sync_state.finalized_head();

        // A lagging source can report an older finalized block. Finality is irreversible, so
        // never ask the execution layer to move it backward.
        if Self::is_behind_finalized_head(&finalized_head, block_number) {
            warn!(
                target: "engine",
                finalized_number = finalized_head.block_info.number,
                target_number = block_number,
                "Ignoring finalization target behind the finalized head"
            );
            return Ok(());
        }

        // Sanity check that the block that is being finalized is at least safe.
        if state.sync_state.safe_head().block_info.number < block_number {
            return Err(FinalizeTaskError::BlockNotSafe);
        }

        // A pinned hash is enough to detect a conflict without asking the execution layer, which
        // may not have the conflicting block at all.
        if let FinalizeBlockId::ByHash(target) = self.block_id &&
            Self::conflicts_with_finalized_head(&finalized_head, target)
        {
            Self::log_conflicting_target(&finalized_head, target);
            return Ok(());
        }

        // Look up by hash when the caller pinned a specific hash (delegated polling supplies
        // `(number, hash)`); otherwise look up by number. Finalization is irreversible, so a stale
        // hash must produce a hard error, not silently finalize whatever the engine has at the
        // same height.
        let lookup: BlockId = match self.block_id {
            FinalizeBlockId::ByHash(id) => id.hash.into(),
            FinalizeBlockId::ByNumber(n) => n.into(),
        };
        let block = self
            .client
            .get_l2_block(lookup)
            .full()
            .await
            .map_err(FinalizeTaskError::TransportError)?
            .ok_or(FinalizeTaskError::BlockNotFound(block_number))?
            .into_consensus();
        let block_info = L2BlockInfo::from_block_and_genesis(&block, &self.client.cfg().genesis)
            .map_err(FinalizeTaskError::FromBlock)?;

        // The engine should never serve a block at a different height than requested. A mismatch
        // indicates an upstream protocol bug or a misuse of the API, and for either identifier it
        // would bypass the guards above, so it must never lead to finalization.
        if block_info.block_info.number != block_number {
            return Err(FinalizeTaskError::BlockNotFound(block_number));
        }

        // A by-number lookup only learns the hash here.
        if Self::conflicts_with_finalized_head(&finalized_head, block_info.block_info.id()) {
            Self::log_conflicting_target(&finalized_head, block_info.block_info.id());
            return Ok(());
        }

        // Dispatch a forkchoice update.
        synchronize(
            self.client.as_ref(),
            state,
            EngineSyncStateUpdate { finalized_head: Some(block_info), ..Default::default() },
        )
        .await?;

        info!(
            target: "engine",
            hash = %block_info.block_info.hash,
            number = block_info.block_info.number,
            "Updated finalized head"
        );

        Ok(())
    }
}
