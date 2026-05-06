//! Per-instance dispatch loop.
//!
//! Owns a `ModuleInstance` and pumps a stream of `BlockEvent`s
//! through it. For each block:
//!   1. Build a `ResolvedBlock` from the typed event payload.
//!   2. Push into the wasmtime `ResourceTable`.
//!   3. Call `handle-event(channel, &handle)`.
//!   4. On success: drop the resource, advance the cursor,
//!      record the supervisor success, refuel the store.
//!   5. On trap: consult the supervisor, handle the outcome
//!      (retry / restart / skip / quarantine).
//!
//! V1 cursor model is "advance only on success or skip-and-mark";
//! traps that fall through to `RestartAndReplay` keep the cursor
//! pinned to the failing block's predecessor so the replay
//! re-runs the block. Cursor is held in-memory only for now;
//! redb-backed persistence lands with the vendored Balius
//! `store.rs`.
//!
//! Forward-compat note: `BlockEvent` is the shape we'd plug
//! against the existing `mitos-core` chain-event source. V1
//! defines it locally (so we don't take a `mitos-core` dep on
//! `mitos-platform` yet); when we wire the real chain follower,
//! the natural move is to promote `BlockEvent` into a
//! `mitos-protocol` enum that both halves share.

use dolos_core::ChainPoint;
use tokio::time::{Duration, sleep};

use crate::block_decode::decode_block;
use crate::registry::{ModuleInstance, ModuleRegistry, ResourceBudget};
use crate::resolved_block::{ResolvedBlock, TxView};
use crate::supervisor::SupervisorOutcome;
use crate::{PlatformError, PlatformResult};
use std::sync::Arc;

use crate::host_fns::{DataPlaneFacade, emit, state_kv};

/// One typed chain event the driver consumes. V1 only carries
/// `Apply { block }` — undo/mark/snapshot will land alongside
/// the chain-follower wiring.
#[derive(Debug, Clone)]
pub enum BlockEvent {
    /// Apply a block forward. `cursor_after` is the chain point
    /// that's reached after the block is applied — what gets
    /// persisted on success.
    Apply {
        slot: u64,
        cursor_after: ChainPoint,
        txs: Vec<TxView>,
    },
}

/// Hook called after every successful cursor advance. Used by
/// `ModuleHost` to persist the cursor to disk so a restart
/// resumes from the last-applied block. Best-effort: failure
/// logs but does not abort the dispatch loop (the in-memory
/// cursor is still updated; we lose at most one block on crash).
pub type CheckpointHook = Arc<dyn Fn(&ChainPoint) + Send + Sync>;

/// Driver state. One per active subscription. Owns the wasmtime
/// instance + supervisor; restartable via the registry.
pub struct Driver {
    instance: ModuleInstance,
    cursor: Option<ChainPoint>,
    budget: ResourceBudget,
    /// Channel to dispatch on. V1 pins to channel 0; per-channel
    /// routing arrives with the registry-driven init handshake.
    dispatch_channel: u32,
    /// Optional checkpoint hook fired after every Applied/Skipped.
    checkpoint_hook: Option<CheckpointHook>,
}

/// Outcome of a single block apply.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ApplyOutcome {
    /// Block applied; cursor advanced.
    Applied,
    /// Block skipped (`SkipAndMark` strategy); cursor advanced
    /// past the failing block.
    Skipped,
    /// Module instance was restarted; the failing block will be
    /// retried on the next call. Caller should re-feed the same
    /// `BlockEvent` (idempotent dispatch is a v1 contract).
    RestartedRetry,
    /// Module is quarantined; cursor NOT advanced. Caller must
    /// surface the failure to the operator.
    Quarantined,
}

impl Driver {
    pub fn new(instance: ModuleInstance, budget: ResourceBudget) -> Self {
        Self {
            instance,
            cursor: None,
            budget,
            dispatch_channel: 0,
            checkpoint_hook: None,
        }
    }

    /// Builder: prime the driver's cursor from a persisted
    /// checkpoint. Used by `ModuleHost` on restart to resume
    /// from the last-applied block rather than chain origin.
    pub fn with_initial_cursor(mut self, cursor: ChainPoint) -> Self {
        self.cursor = Some(cursor);
        self
    }

    /// Builder: install a checkpoint hook fired on every
    /// successful cursor advance.
    pub fn with_checkpoint_hook(mut self, hook: CheckpointHook) -> Self {
        self.checkpoint_hook = Some(hook);
        self
    }

    pub fn cursor(&self) -> Option<&ChainPoint> {
        self.cursor.as_ref()
    }

    /// Convenience: decode + apply in one step. Caller hands raw
    /// block CBOR (e.g. straight off the chain follower's
    /// `TipEvent::Apply(_, block_cbor)` payload) plus the
    /// `ChainPoint` to advance to on success. Decode errors
    /// propagate as `PlatformError::Decode`.
    pub async fn apply_cbor(
        &mut self,
        registry: &ModuleRegistry,
        data_plane: Arc<dyn DataPlaneFacade>,
        kv_factory: impl Fn() -> state_kv::ModuleKv,
        emitter_factory: impl Fn() -> emit::EventSink,
        cbor: &[u8],
        cursor_after: ChainPoint,
    ) -> PlatformResult<ApplyOutcome> {
        let decoded = decode_block(cbor).map_err(|e| {
            // Rich diagnostic context — when this fires we want
            // to identify the failing block precisely. The
            // follower's `?`-propagation logs only the bare
            // `PlatformError::Decode(_)` Display; this `error!`
            // adds the slot + bytes length + CBOR prefix so we
            // can reproduce offline.
            let prefix_len = cbor.len().min(64);
            tracing::error!(
                slot = cursor_after.slot(),
                bytes_len = cbor.len(),
                cbor_prefix = %hex::encode(&cbor[..prefix_len]),
                error = %e,
                "block decode failed; follower will exit",
            );
            PlatformError::Decode(format!("slot={}: {e}", cursor_after.slot()))
        })?;
        let event = BlockEvent::Apply {
            slot: decoded.slot,
            cursor_after,
            txs: decoded.txs,
        };
        self.apply(registry, data_plane, kv_factory, emitter_factory, event)
            .await
    }

    /// Forward a dynamic-interest update to the wasm module's
    /// `update-interest` export. Called from the chain follower
    /// in response to `ClientMessage::Interest` frames received
    /// over a companion's outbound WS — see `dialer.rs` and the
    /// `InterestRouter` plumbing in `host.rs`.
    ///
    /// Items cross the WIT boundary as one CBOR-encoded
    /// `Vec<mitos_protocol::Interest>` blob: host encodes once,
    /// module decodes once. Module-side decode/validation
    /// failures surface as `Err(reason)` from the export and
    /// are logged + propagated as `PlatformError::Decode`; they
    /// do NOT trap the instance or advance the cursor (per the
    /// WIT contract — interest-shape mismatch is a recoverable
    /// deploy-version skew).
    pub async fn update_interest(
        &mut self,
        op: mitos_protocol::InterestOp,
        items: Vec<mitos_protocol::Interest>,
    ) -> PlatformResult<()> {
        // Refuel before the call. update-interest can do
        // arbitrary state-kv work module-side; budget it the
        // same way an apply gets budgeted.
        self.instance.store.set_fuel(self.budget.fuel_per_call)?;
        self.instance
            .store
            .set_epoch_deadline(self.budget.epoch_deadline_ticks);

        let mut buf = Vec::with_capacity(64);
        ciborium::ser::into_writer(&items, &mut buf)
            .map_err(|e| PlatformError::Decode(format!("encode interest items: {e}")))?;

        let bindgen_op = match op {
            mitos_protocol::InterestOp::Add => crate::bindings::InterestOp::Add,
            mitos_protocol::InterestOp::Remove => crate::bindings::InterestOp::Remove,
            mitos_protocol::InterestOp::Replace => crate::bindings::InterestOp::Replace,
        };
        let result = self
            .instance
            .bindings
            .call_update_interest(&mut self.instance.store, bindgen_op, &buf)
            .await?;
        result.map_err(|reason| {
            PlatformError::Decode(format!("module update_interest err: {reason}"))
        })
    }

    /// Apply one block. Handles trap supervision internally;
    /// caller decides whether to keep feeding blocks (Applied /
    /// Skipped / RestartedRetry) or stop (Quarantined).
    pub async fn apply(
        &mut self,
        registry: &ModuleRegistry,
        data_plane: Arc<dyn DataPlaneFacade>,
        kv_factory: impl Fn() -> state_kv::ModuleKv,
        emitter_factory: impl Fn() -> emit::EventSink,
        event: BlockEvent,
    ) -> PlatformResult<ApplyOutcome> {
        let BlockEvent::Apply {
            slot,
            cursor_after,
            txs,
        } = event;

        // Refuel before each call. wasmtime consumes fuel
        // monotonically; without this, the second block would
        // run on the leftover budget from the first.
        self.instance.store.set_fuel(self.budget.fuel_per_call)?;
        self.instance
            .store
            .set_epoch_deadline(self.budget.epoch_deadline_ticks);

        let block = ResolvedBlock::from_views(slot, txs.clone());
        // Stash the cursor on HostState so emit_event can tag
        // every EmittedEvent with the chain point of the
        // currently-dispatching block.
        self.instance.store.data_mut().current_cursor = Some(cursor_after.clone());
        let resource = self.instance.store.data_mut().table.push(block)?;

        let dispatch_result = self
            .instance
            .bindings
            .call_handle_event(&mut self.instance.store, self.dispatch_channel, resource)
            .await;
        self.instance.store.data_mut().current_cursor = None;

        match dispatch_result {
            Ok(()) => {
                self.instance.supervisor.record_success();
                self.cursor = Some(cursor_after.clone());
                self.fire_checkpoint(&cursor_after);
                Ok(ApplyOutcome::Applied)
            }
            Err(trap) => {
                tracing::warn!(
                    error = %trap,
                    slot,
                    "module trapped during handle-event",
                );
                self.handle_trap(
                    registry,
                    data_plane,
                    kv_factory,
                    emitter_factory,
                    cursor_after,
                )
                .await
            }
        }
    }

    /// Fire the checkpoint hook if one's installed. Failure
    /// here does not abort the dispatch loop — we log + carry
    /// on with the in-memory cursor (best-effort persistence).
    fn fire_checkpoint(&self, cursor: &ChainPoint) {
        if let Some(hook) = &self.checkpoint_hook {
            hook(cursor);
        }
    }

    async fn handle_trap(
        &mut self,
        registry: &ModuleRegistry,
        data_plane: Arc<dyn DataPlaneFacade>,
        kv_factory: impl Fn() -> state_kv::ModuleKv,
        emitter_factory: impl Fn() -> emit::EventSink,
        cursor_after: ChainPoint,
    ) -> PlatformResult<ApplyOutcome> {
        // record_trap returns one of {Retry, RestartAndReplay,
        // SkipAndContinue, Quarantine} — each terminal for this
        // call. Caller decides whether to re-feed the same block
        // (Retry / RestartedRetry) or move on.
        match self.instance.supervisor.record_trap() {
            SupervisorOutcome::Retry { backoff_ms } => {
                sleep(Duration::from_millis(backoff_ms as u64)).await;
                Ok(ApplyOutcome::RestartedRetry)
            }
            SupervisorOutcome::RestartAndReplay => {
                tracing::warn!("supervisor: restarting instance for replay");
                let kv = kv_factory();
                let emitter = emitter_factory();
                self.instance = registry
                    .instantiate(data_plane.clone(), kv, emitter, self.budget)
                    .await?;
                Ok(ApplyOutcome::RestartedRetry)
            }
            SupervisorOutcome::SkipAndContinue => {
                tracing::warn!("supervisor: skipping failing block, advancing cursor");
                self.cursor = Some(cursor_after.clone());
                self.fire_checkpoint(&cursor_after);
                Ok(ApplyOutcome::Skipped)
            }
            SupervisorOutcome::Quarantine => {
                tracing::error!("supervisor: quarantining module");
                Err(PlatformError::Quarantined { failures: 1 })
            }
            SupervisorOutcome::Ok => unreachable!("record_trap never returns Ok"),
        }
    }
}
