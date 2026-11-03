//! A task to consolidate the engine state.

use crate::{
    ConsolidateTaskError, EngineClient, EngineState, ExecutableEngineTask, ImportedBlockSink,
    state::EngineSyncStateUpdate,
    task_queue::{build_and_import, synchronize},
};
use alloy_rpc_types_eth::Block;
use async_trait::async_trait;
use kona_genesis::RollupConfig;
use kona_protocol::{L2BlockInfo, OpAttributesWithParent};
use op_alloy_rpc_types::Transaction;
use std::sync::Arc;

/// Input for consolidation - either derived attributes or safe L2 block
#[derive(Debug, Clone)]
pub enum ConsolidateInput {
    /// Consolidate based on derived attributes.
    Attributes(Box<OpAttributesWithParent>),
    /// Derivation Delegation: consolidate based on safe L2 block info.
    BlockInfo(L2BlockInfo),
}

impl From<L2BlockInfo> for ConsolidateInput {
    fn from(v: L2BlockInfo) -> Self {
        Self::BlockInfo(v)
    }
}

impl From<OpAttributesWithParent> for ConsolidateInput {
    fn from(v: OpAttributesWithParent) -> Self {
        Self::Attributes(Box::new(v))
    }
}

impl ConsolidateInput {
    /// Returns the block number for this consolidation input.
    const fn l2_block_number(&self) -> u64 {
        match self {
            Self::Attributes(attributes) => attributes.block_number(),
            Self::BlockInfo(info) => info.block_info.number,
        }
    }

    /// Checks if the block is consistent with this consolidation input.
    fn is_consistent_with_block(&self, cfg: &RollupConfig, block: &Block<Transaction>) -> bool {
        match self {
            Self::Attributes(attributes) => {
                crate::AttributesMatch::check(cfg, attributes, block).is_match()
            }
            Self::BlockInfo(info) => block.header.hash == info.block_info.hash,
        }
    }

    /// Returns true for derived attributes that are not the last block of their span batch.
    /// A delegated [`Self::BlockInfo`] safe head is never part of a span, so it is never
    /// intermediate and always reaches the execution layer.
    const fn is_intermediate_span(&self) -> bool {
        matches!(
            self,
            Self::Attributes(attributes)
                if !attributes.is_last_in_span
        )
    }
}

/// The [`ConsolidateTask`] attempts to consolidate the engine state
/// using the specified payload attributes or block info.
#[derive(Debug, Clone)]
pub struct ConsolidateTask {
    /// The engine client.
    pub client: Arc<EngineClient>,
    /// The [`RollupConfig`].
    pub cfg: Arc<RollupConfig>,
    /// The input for consolidation (either attributes or block info).
    pub input: ConsolidateInput,
    /// Where to hand the decoded block once the engine has canonicalized it.
    pub block_sink: Arc<dyn ImportedBlockSink>,
}

impl ConsolidateTask {
    /// Creates a new [`ConsolidateTask`] with the specified input
    pub const fn new(
        client: Arc<EngineClient>,
        cfg: Arc<RollupConfig>,
        input: ConsolidateInput,
        block_sink: Arc<dyn ImportedBlockSink>,
    ) -> Self {
        Self { client, cfg, input, block_sink }
    }

    /// This is used when the [`ConsolidateTask`] fails to consolidate the engine state
    async fn build_and_import_attributes(
        &self,
        state: &mut EngineState,
        attributes: &OpAttributesWithParent,
    ) -> Result<(), ConsolidateTaskError> {
        build_and_import(
            self.client.as_ref(),
            &self.cfg,
            state,
            attributes.clone(),
            self.block_sink.as_ref(),
        )
        .await?;
        Ok(())
    }

    /// This provides symmetric fallback behavior with `build_and_import`.
    async fn reconcile_to_safe_head(
        &self,
        state: &mut EngineState,
        safe_l2: &L2BlockInfo,
    ) -> Result<(), ConsolidateTaskError> {
        warn!(
            target: "engine",
            safe_l2 = %safe_l2,
            "Apply safe head"
        );

        // We intentionally set the unsafe head to safe_l2 to ensure the engine observes a
        // self-consistent head state. This is required to correctly handle reorgs (where unsafe
        // may be ahead on a non-canonical fork) and to trigger EL sync when the local unsafe head
        // lags behind the safe head.
        synchronize(
            self.client.as_ref(),
            state,
            EngineSyncStateUpdate {
                unsafe_head: Some(*safe_l2),
                safe_head: Some(*safe_l2),
                local_safe_head: Some(*safe_l2),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| {
            warn!(target: "engine", ?e, "Apply safe head failed");
            e
        })?;

        info!(
            target: "engine",
            hash = %safe_l2.block_info.hash,
            number = safe_l2.block_info.number,
            "Updated safe head via follow safe"
        );

        Ok(())
    }

    /// Handles the fallback case when the block doesn't match the input or does not exist.
    async fn reconcile_unsafe_to_safe(
        &self,
        state: &mut EngineState,
    ) -> Result<(), ConsolidateTaskError> {
        match &self.input {
            ConsolidateInput::Attributes(attributes) => {
                self.build_and_import_attributes(state, attributes).await
            }
            ConsolidateInput::BlockInfo(safe_l2) => {
                self.reconcile_to_safe_head(state, safe_l2).await
            }
        }
    }

    /// Attempts consolidation on the engine state.
    #[tracing::instrument(
        name = "consolidate",
        target = "engine",
        level = "info",
        skip_all,
        fields(number = self.input.l2_block_number())
    )]
    pub async fn consolidate(&self, state: &mut EngineState) -> Result<(), ConsolidateTaskError> {
        // Fetch the unsafe L2 block
        let block_num = self.input.l2_block_number();
        let block = match self.client.l2_block_by_label(block_num.into()).await {
            Ok(Some(block)) => block,
            Ok(None) => {
                warn!(target: "engine", "Received `None` block for {}", block_num);
                return Err(ConsolidateTaskError::MissingUnsafeL2Block(block_num));
            }
            Err(_) => {
                return Err(ConsolidateTaskError::FailedToFetchUnsafeL2Block);
            }
        };
        let block_hash = block.header.hash;

        if self.input.is_consistent_with_block(&self.cfg, &block) {
            trace!(
                target: "engine",
                input = ?self.input,
                block_hash = %block_hash,
                "Consolidating engine state",
            );
            let consensus_block =
                block.into_consensus().map_transactions(|t| t.inner.inner.into_inner());
            match L2BlockInfo::from_block_and_genesis(&consensus_block, &self.cfg.genesis) {
                // Only issue a forkchoice update if the attributes are the last in the span
                // batch. This is an optimization to avoid sending a FCU
                // call for every block in the span batch.
                Ok(block_info) if self.input.is_intermediate_span() => {
                    // The next attributes built are this block's child, and ask for its config.
                    self.block_sink.block_imported(consensus_block, block_info);

                    // Apply a transient update to the safe head.
                    state.sync_state = state.sync_state.apply_update(EngineSyncStateUpdate {
                        safe_head: Some(block_info),
                        local_safe_head: Some(block_info),
                        ..Default::default()
                    });

                    info!(
                        target: "engine",
                        hash = %block_info.block_info.hash,
                        number = block_info.block_info.number,
                        "Updated safe head via L1 consolidation"
                    );

                    return Ok(());
                }
                Ok(block_info) => {
                    // The next attributes built are this block's child, and ask for its config.
                    self.block_sink.block_imported(consensus_block, block_info);

                    synchronize(
                        self.client.as_ref(),
                        state,
                        EngineSyncStateUpdate {
                            safe_head: Some(block_info),
                            local_safe_head: Some(block_info),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|e| {
                        warn!(target: "engine", ?e, "Consolidation failed");
                        e
                    })?;

                    info!(
                        target: "engine",
                        hash = %block_info.block_info.hash,
                        number = block_info.block_info.number,
                        "Updated safe head via L1 consolidation"
                    );

                    return Ok(());
                }
                Err(e) => {
                    // Continue on to build the block since we failed to construct the block info.
                    warn!(target: "engine", ?e, "Failed to construct L2BlockInfo, proceeding to build task");
                }
            }
        }

        debug!(
            target: "engine",
            input = ?self.input,
            block_hash = %block_hash,
            "ConsolidateInput mismatch! Initiating reorg",
        );
        // Handle mismatch case - called when consistency check fails
        // or when L2BlockInfo construction fails in Attributes branch
        self.reconcile_unsafe_to_safe(state).await
    }
}

#[async_trait]
impl ExecutableEngineTask for ConsolidateTask {
    type Output = ();

    type Error = ConsolidateTaskError;

    // Behavior depends on how the safe head is provided:
    //
    // - `Attributes`: The safe head is advanced through the normal derivation flow, where the
    //   DerivationActor and EngineActor coordinate both safe and unsafe heads. In this case, we
    //   consolidate as long as the unsafe head has not fallen behind.
    //
    // - `BlockInfo`: The safe head is injected externally by the DerivationActor while delegating
    //   derivation, and is not coordinated with the EngineActor's safe/unsafe heads. If the
    //   injected safe head is ahead of the EngineActor's unsafe head, we reconcile the unsafe chain
    //   up to the safe head instead of consolidating.
    async fn execute(&self, state: &mut EngineState) -> Result<(), ConsolidateTaskError> {
        let safe_head_number = match &self.input {
            ConsolidateInput::Attributes { .. } => state.sync_state.safe_head().block_info.number,
            ConsolidateInput::BlockInfo(safe_block_info) => safe_block_info.block_info.number,
        };
        if safe_head_number < state.sync_state.unsafe_head().block_info.number {
            self.consolidate(state).await
        } else {
            self.reconcile_unsafe_to_safe(state).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        EngineTaskError, ForkchoiceStatus, ImportedBlockSink, NoopBlockSink, SealTaskError,
        test_utils::{
            TestAttributesBuilder, TestEngineStateBuilder, test_block_info, test_engine_client,
        },
    };
    use alloy_consensus::transaction::Recovered;
    use alloy_eips::{BlockNumHash, Encodable2718};
    use alloy_primitives::{Address, B256, Sealed};
    use alloy_rpc_types_engine::{
        ForkchoiceState, ForkchoiceUpdated, PayloadStatus, PayloadStatusEnum,
    };
    use alloy_rpc_types_eth::{Block as RpcBlock, BlockTransactions, Header as RpcHeader};
    use kona_genesis::ChainGenesis;
    use kona_protocol::{BlockInfo, L1BlockInfoBedrock};
    use op_alloy_consensus::{OpTxEnvelope, TxDeposit};

    /// Records the blocks the engine hands over after consolidating.
    #[derive(Debug, Default)]
    struct RecordingSink(std::sync::Mutex<Vec<B256>>);

    impl ImportedBlockSink for RecordingSink {
        fn block_imported(&self, _: op_alloy_consensus::OpBlock, info: L2BlockInfo) {
            self.0.lock().unwrap().push(info.block_info.hash);
        }
    }

    #[tokio::test]
    async fn consolidated_blocks_are_handed_to_the_block_sink() {
        // Consolidation reads the unsafe block in full to compare it against the derived
        // attributes; the next attributes built ask for this same block's system config, so it
        // must reach the sink rather than being dropped.
        let header = RpcHeader::new(alloy_consensus::Header::default());
        let block_hash = header.hash;
        let block: RpcBlock<op_alloy_rpc_types::Transaction> =
            RpcBlock::new(header, BlockTransactions::Full(vec![]));

        // Pin genesis to this block so its L2BlockInfo needs no L1-info deposit.
        let cfg = Arc::new(RollupConfig {
            genesis: ChainGenesis {
                l2: BlockNumHash { hash: block_hash, number: 0 },
                ..Default::default()
            },
            ..Default::default()
        });
        let (client, l1, l2) = test_engine_client(cfg.clone());
        l2.expect_params("eth_getBlockByNumber", serde_json::json!(["0x0", true]), block);
        l2.expect(
            "engine_forkchoiceUpdatedV3",
            ForkchoiceUpdated::new(PayloadStatus::from_status(PayloadStatusEnum::Valid)),
        );
        let client = Arc::new(client);

        let safe_l2 = L2BlockInfo {
            block_info: BlockInfo { hash: block_hash, number: 0, ..Default::default() },
            ..Default::default()
        };
        let sink = Arc::new(RecordingSink::default());
        let task =
            ConsolidateTask::new(client, cfg, ConsolidateInput::BlockInfo(safe_l2), sink.clone());

        task.consolidate(&mut EngineState::default()).await.unwrap();

        assert_eq!(sink.0.lock().unwrap().as_slice(), &[block_hash]);
        l1.assert_finished();
        l2.assert_finished();
    }

    const DEPOSIT: u8 = op_alloy_consensus::DEPOSIT_TX_TYPE_ID;

    fn holocene_cfg() -> Arc<RollupConfig> {
        let mut cfg = RollupConfig::default();
        cfg.hardforks.ecotone_time = Some(0);
        cfg.hardforks.holocene_time = Some(0);
        Arc::new(cfg)
    }

    fn derived(transactions: Vec<alloy_primitives::Bytes>) -> OpAttributesWithParent {
        crate::test_utils::TestAttributesBuilder::new()
            .with_parent(crate::test_utils::test_block_info(10))
            .with_transactions(transactions)
            .build()
    }

    async fn build_derived(
        client: Arc<EngineClient>,
        cfg: Arc<RollupConfig>,
        attributes: &OpAttributesWithParent,
    ) -> ConsolidateTaskError {
        let mut state = crate::test_utils::TestEngineStateBuilder::new()
            .with_unsafe_head(attributes.parent)
            .build();
        let task = ConsolidateTask::new(
            client,
            cfg,
            ConsolidateInput::Attributes(Box::new(attributes.clone())),
            Arc::new(crate::NoopBlockSink),
        );
        task.build_and_import_attributes(&mut state, attributes)
            .await
            .expect_err("the mock never completes a seal")
    }

    // An FCU rejection with -38003 (invalid payload attributes) triggers deposits-only recovery.
    #[tokio::test]
    async fn attributes_rejected_at_fcu_are_replaced_with_deposits_only() {
        let deposit = alloy_primitives::Bytes::from(vec![DEPOSIT, 0x01]);
        let attributes = derived(vec![deposit.clone(), alloy_primitives::Bytes::from(vec![0x02])]);
        let cfg = holocene_cfg();
        let (client, l1, l2) = test_engine_client(cfg.clone());
        l2.expect_error_code("engine_forkchoiceUpdatedV3", -38003);
        let forkchoice = crate::test_utils::TestEngineStateBuilder::new()
            .with_unsafe_head(attributes.parent)
            .build()
            .sync_state
            .create_forkchoice_state();
        l2.expect_params(
            "engine_forkchoiceUpdatedV3",
            serde_json::json!([forkchoice, attributes.as_deposits_only().attributes]),
            alloy_rpc_types_engine::ForkchoiceUpdated {
                payload_status: alloy_rpc_types_engine::PayloadStatus::from_status(
                    alloy_rpc_types_engine::PayloadStatusEnum::Valid,
                ),
                payload_id: Some(alloy_rpc_types_engine::PayloadId::new([1; 8])),
            },
        );
        l2.expect_error("engine_getPayloadV3");
        let err = build_derived(Arc::new(client), cfg, &attributes).await;
        l1.assert_finished();
        l2.assert_finished();
        // The mock fails the replacement's getPayload.
        assert!(
            matches!(
                err,
                ConsolidateTaskError::SealTaskFailed(
                    SealTaskError::DepositOnlyPayloadReattemptFailed
                )
            ),
            "got {err:?}",
        );
    }

    // An internal error at FCU is a failure of the execution layer, not of the batch.
    #[tokio::test]
    async fn internal_error_at_fcu_is_retried_not_replaced() {
        let attributes = derived(vec![
            alloy_primitives::Bytes::from(vec![DEPOSIT, 0x01]),
            alloy_primitives::Bytes::from(vec![0x02]),
        ]);
        let cfg = holocene_cfg();
        let (client, l1, l2) = test_engine_client(cfg.clone());
        l2.expect_error("engine_forkchoiceUpdatedV3");
        let err = build_derived(Arc::new(client), cfg, &attributes).await;
        l1.assert_finished();
        l2.assert_finished();
        assert!(matches!(err, ConsolidateTaskError::BuildTaskFailed(_)), "got {err:?}");
        assert_eq!(err.severity(), crate::EngineTaskErrorSeverity::Temporary);
    }

    #[tokio::test]
    async fn transport_failure_at_fcu_is_retried_not_replaced() {
        let attributes = derived(vec![
            alloy_primitives::Bytes::from(vec![DEPOSIT, 0x01]),
            alloy_primitives::Bytes::from(vec![0x02]),
        ]);
        let cfg = holocene_cfg();
        let (client, l1, l2) = test_engine_client(cfg.clone());
        l2.expect_transport_error("engine_forkchoiceUpdatedV3");
        let err = build_derived(Arc::new(client), cfg, &attributes).await;
        l1.assert_finished();
        l2.assert_finished();
        assert!(matches!(err, ConsolidateTaskError::BuildTaskFailed(_)), "got {err:?}");
        assert_eq!(err.severity(), crate::EngineTaskErrorSeverity::Temporary);
    }

    // Before Holocene, nothing replaces rejected attributes, so the task keeps the rejection.
    #[tokio::test]
    async fn attributes_rejected_at_fcu_before_holocene_are_not_replaced() {
        let attributes = derived(vec![
            alloy_primitives::Bytes::from(vec![DEPOSIT, 0x01]),
            alloy_primitives::Bytes::from(vec![0x02]),
        ]);
        let mut cfg = RollupConfig::default();
        cfg.hardforks.ecotone_time = Some(0);
        let cfg = Arc::new(cfg);
        let (client, l1, l2) = test_engine_client(cfg.clone());
        l2.expect_error_code("engine_forkchoiceUpdatedV3", -38003);
        let err = build_derived(Arc::new(client), cfg, &attributes).await;
        l1.assert_finished();
        l2.assert_finished();
        assert!(
            matches!(&err, ConsolidateTaskError::BuildTaskFailed(build) if build.is_invalid_attributes()),
            "got {err:?}"
        );
    }

    fn expect_valid_forkchoice_updated(
        l2: &crate::test_utils::RpcMock,
        forkchoice: ForkchoiceState,
    ) {
        l2.expect_params(
            "engine_forkchoiceUpdatedV3",
            serde_json::json!([forkchoice, null]),
            ForkchoiceUpdated::new(PayloadStatus::from_status(PayloadStatusEnum::Valid)),
        );
    }

    /// An engine state the execution layer has applied, with the unsafe head ahead of `safe_head`.
    fn applied_engine_state(safe_head: L2BlockInfo) -> EngineState {
        let mut state = TestEngineStateBuilder::new()
            .with_unsafe_head(test_block_info(20))
            .with_safe_head(safe_head)
            .with_finalized_head(test_block_info(5))
            .build();
        state.forkchoice_status =
            ForkchoiceStatus::Applied(state.sync_state.create_forkchoice_state());
        state
    }

    fn forkchoice_with_safe_head(state: &EngineState, safe_head: L2BlockInfo) -> ForkchoiceState {
        state
            .sync_state
            .apply_update(EngineSyncStateUpdate {
                local_safe_head: Some(safe_head),
                safe_head: Some(safe_head),
                ..Default::default()
            })
            .create_forkchoice_state()
    }

    fn block_info_of(cfg: &RollupConfig, block: &RpcBlock<Transaction>) -> L2BlockInfo {
        let consensus_block =
            block.clone().into_consensus().map_transactions(|t| t.inner.inner.into_inner());
        L2BlockInfo::from_block_and_genesis(&consensus_block, &cfg.genesis).unwrap()
    }

    /// A delegated safe head that the execution layer already has, while the unsafe and finalized
    /// heads stay put, still reaches the execution layer: no later unsafe block or finalization
    /// may be needed to deliver it.
    #[tokio::test]
    async fn delegated_safe_head_reaches_execution_layer_with_finalized_unchanged() {
        let header = RpcHeader::new(alloy_consensus::Header { number: 10, ..Default::default() });
        let block_hash = header.hash;
        let block: RpcBlock<Transaction> = RpcBlock::new(header, BlockTransactions::Full(vec![]));
        // Pin genesis to this block so its L2BlockInfo needs no L1-info deposit.
        let cfg = Arc::new(RollupConfig {
            genesis: ChainGenesis {
                l2: BlockNumHash { hash: block_hash, number: 10 },
                ..Default::default()
            },
            ..Default::default()
        });
        let delegated_safe_head = block_info_of(&cfg, &block);
        let mut state = applied_engine_state(test_block_info(5));
        let target = forkchoice_with_safe_head(&state, delegated_safe_head);
        let (client, l1, l2) = test_engine_client(cfg.clone());
        l2.expect_params("eth_getBlockByNumber", serde_json::json!(["0xa", true]), block);
        expect_valid_forkchoice_updated(&l2, target);
        let task = ConsolidateTask::new(
            Arc::new(client),
            cfg,
            ConsolidateInput::BlockInfo(delegated_safe_head),
            Arc::new(NoopBlockSink),
        );

        task.execute(&mut state).await.unwrap();

        assert_eq!(state.sync_state.safe_head(), delegated_safe_head);
        assert_eq!(state.forkchoice_status, ForkchoiceStatus::Applied(target));
        l1.assert_finished();
        l2.assert_finished();
    }

    fn l1_info_deposit() -> OpTxEnvelope {
        OpTxEnvelope::Deposit(Sealed::new(TxDeposit {
            input: L1BlockInfoBedrock::default().encode_calldata(),
            ..Default::default()
        }))
    }

    fn derived_attributes(is_last_in_span: bool) -> OpAttributesWithParent {
        let mut attributes = TestAttributesBuilder::new()
            .with_parent(test_block_info(9))
            .with_transactions(vec![l1_info_deposit().encoded_2718().into()])
            .build();
        attributes.is_last_in_span = is_last_in_span;
        attributes
    }

    /// The unsafe block that `attributes` derive, so consolidation finds it consistent.
    fn unsafe_block_matching(attributes: &OpAttributesWithParent) -> RpcBlock<Transaction> {
        let payload_attributes = &attributes.attributes.payload_attributes;
        let header = alloy_consensus::Header {
            beneficiary: payload_attributes.suggested_fee_recipient,
            gas_limit: attributes.attributes.gas_limit.unwrap(),
            mix_hash: payload_attributes.prev_randao,
            number: attributes.block_number(),
            parent_beacon_block_root: payload_attributes.parent_beacon_block_root,
            parent_hash: attributes.parent.block_info.hash,
            timestamp: payload_attributes.timestamp,
            ..Default::default()
        };
        let deposit = Transaction::from_transaction(
            Recovered::new_unchecked(l1_info_deposit(), Address::ZERO),
            Default::default(),
        );
        RpcBlock::new(RpcHeader::new(header), BlockTransactions::Full(vec![deposit]))
    }

    /// Derived blocks inside a span batch only move the safe label; the span's last block sends
    /// the forkchoice update that delivers it.
    #[rstest::rstest]
    #[case::intermediate_block_defers_forkchoice(false)]
    #[case::last_block_sends_forkchoice(true)]
    #[tokio::test]
    async fn span_batch_sends_forkchoice_only_at_its_last_block(#[case] is_last_in_span: bool) {
        let cfg = Arc::new(RollupConfig::default());
        let attributes = derived_attributes(is_last_in_span);
        let block = unsafe_block_matching(&attributes);
        let derived_safe_head = block_info_of(&cfg, &block);
        let mut state = applied_engine_state(attributes.parent);
        let applied_before = state.forkchoice_status;
        let target = forkchoice_with_safe_head(&state, derived_safe_head);
        let (client, l1, l2) = test_engine_client(cfg.clone());
        l2.expect_params("eth_getBlockByNumber", serde_json::json!(["0xa", true]), block);
        if is_last_in_span {
            expect_valid_forkchoice_updated(&l2, target);
        }
        let task = ConsolidateTask::new(
            Arc::new(client),
            cfg,
            ConsolidateInput::Attributes(Box::new(attributes)),
            Arc::new(NoopBlockSink),
        );

        task.execute(&mut state).await.unwrap();

        assert_eq!(state.sync_state.safe_head(), derived_safe_head);
        let expected_status =
            if is_last_in_span { ForkchoiceStatus::Applied(target) } else { applied_before };
        assert_eq!(state.forkchoice_status, expected_status);
        l1.assert_finished();
        l2.assert_finished();
    }
}
