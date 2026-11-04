//! A task for finalizing an L2 block.

use crate::{
    EngineClient, EngineState, ExecutableEngineTask, FinalizeBlockId, FinalizeTaskError,
    state::EngineSyncStateUpdate, task_queue::synchronize,
};
use alloy_eips::BlockId;
use async_trait::async_trait;
use derive_more::Constructor;
use kona_genesis::RollupConfig;
use kona_protocol::L2BlockInfo;
use std::sync::Arc;

/// The [`FinalizeTask`] fetches the [`L2BlockInfo`] identified by `block_id`, updates the
/// [`EngineState`], and dispatches a forkchoice update to finalize the block.
#[derive(Debug, Clone, Constructor)]
pub struct FinalizeTask {
    /// The engine client.
    pub client: Arc<EngineClient>,
    /// The rollup config.
    pub cfg: Arc<RollupConfig>,
    /// Identifier of the L2 block to finalize.
    pub block_id: FinalizeBlockId,
}

#[async_trait]
impl ExecutableEngineTask for FinalizeTask {
    type Output = ();

    type Error = FinalizeTaskError;

    async fn execute(&self, state: &mut EngineState) -> Result<(), FinalizeTaskError> {
        let block_number = self.block_id.number();

        // Sanity check that the block that is being finalized is at least safe.
        if state.sync_state.safe_head().block_info.number < block_number {
            // A pinned target comes from an upstream, which can run ahead of the local safe head.
            // Like op-node, skip it rather than stop the node; the upstream resubmits it.
            if let FinalizeBlockId::ByHash(target) = self.block_id {
                warn!(
                    target: "engine",
                    ?target,
                    safe_number = state.sync_state.safe_head().block_info.number,
                    "Skipping finalization target above the safe head"
                );
                return Ok(());
            }
            return Err(FinalizeTaskError::BlockNotSafe);
        }

        // Look up by hash when the caller pinned a specific hash (delegated polling supplies
        // `(number, hash)`); otherwise look up by number. Finalization is irreversible, so never
        // finalize the engine's block at the same height in place of a pinned hash.
        let lookup: BlockId = match self.block_id {
            FinalizeBlockId::ByHash(id) => id.hash.into(),
            FinalizeBlockId::ByNumber(n) => n.into(),
        };
        let block = self
            .client
            .get_l2_block(lookup)
            .full()
            .await
            .map_err(FinalizeTaskError::TransportError)?;
        let Some(block) = block else {
            // While the execution layer catches up, a pinned block may not be downloaded yet.
            // Skip it and let the upstream resubmit it: failing would either stop the node or hold
            // this task at the head of the queue, blocking the imports that let the engine catch
            // up.
            if let FinalizeBlockId::ByHash(target) = self.block_id {
                debug!(
                    target: "engine",
                    ?target,
                    "Finalization target is not in the engine yet; awaiting resubmission"
                );
                return Ok(());
            }
            return Err(FinalizeTaskError::BlockNotFound(block_number));
        };
        let block = block.into_consensus();
        let block_info = L2BlockInfo::from_block_and_genesis(&block, &self.client.cfg().genesis)
            .map_err(FinalizeTaskError::FromBlock)?;

        // For ByHash, also assert the returned block matches the caller's pinned hash and height.
        // The engine should never serve a different block for a given hash, but a mismatch
        // indicates either an upstream protocol bug or a misuse of the API and must never lead to
        // silent finalization.
        if let FinalizeBlockId::ByHash(id) = self.block_id &&
            block_info.block_info.id() != id
        {
            return Err(FinalizeTaskError::BlockIdentityMismatch {
                actual: block_info.block_info.id(),
                expected: id,
            });
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
