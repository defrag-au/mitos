//! v2 driver — orchestrates per-block dispatch through the v2
//! event model.
//!
//! Hand the driver a block CBOR + cursor + interest set, and it:
//! 1. Decodes via `mitos_data_plane::block_events::decode_block_v2`.
//! 2. Filters + groups via
//!    `mitos_data_plane::dispatch::build_event_batches` —
//!    requires a data plane reference for prior-output
//!    resolution.
//! 3. For each non-empty `TxEventBatch`: refuels the wasmtime
//!    store, converts the events to WIT shape via
//!    `event_bindings::dispatch_to_wit`, and calls
//!    `handle-events`.
//! 4. After all batches dispatch successfully, fires the
//!    checkpoint hook with the block's cursor.
//!
//! Trap supervision: a wasm trap during `handle-events` flows
//! up to the caller as `Err(PlatformError::Wasmtime(_))`. The
//! follower / host runs the supervisor decision; the driver
//! itself doesn't loop — keeps the dispatch flow easy to read.
//!
//! Bootstrap, rollback, and tick dispatch are separate methods
//! (TBD) on the same `DriverV2` since they share the wasmtime
//! call site.

use std::sync::Arc;

use mitos_data_plane::block_events::decode_block_v2;
use mitos_data_plane::dispatch::build_event_batches;
use mitos_data_plane::{ChainDataPlane, ChainPoint, DispatchEvent, InterestSet, TxEventBatch};

use crate::bindings_v2::DispatchEvent as WitDispatchEvent;
use crate::event_bindings;
use crate::registry_v2::ModuleInstanceV2;
use crate::{PlatformError, PlatformResult};

/// Outcome of one block-dispatch attempt against a v2 module.
/// Trap propagation goes through the function's `Result` return
/// (`wasmtime::Error` doesn't implement `Clone`/`Eq` so we keep
/// it out of the enum and surface it as `Err` on the call).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyOutcomeV2 {
    /// Block decoded, all matching batches dispatched, cursor
    /// advanced. Steady state.
    Applied,
    /// Block had no matching events for the module's interest
    /// set. Cursor advances; no wasm calls fired.
    AppliedEmpty,
}

/// Per-instance v2 driver. Wraps a `ModuleInstanceV2` and tracks
/// cursor advancement.
pub struct DriverV2 {
    instance: ModuleInstanceV2,
    cursor: Option<ChainPoint>,
    /// Per-call fuel budget. Re-applied before every wasm
    /// invocation so each call gets a fresh ceiling.
    fuel_per_call: u64,
    /// Refuel each call with `init_fuel` for the first
    /// `handle-events` of bootstrap-class workloads. Today the
    /// driver doesn't differentiate; bootstrap orchestration
    /// will set this on a per-call basis.
    epoch_deadline_ticks: u64,
}

impl DriverV2 {
    pub fn new(instance: ModuleInstanceV2, budget: crate::registry_v2::ResourceBudget) -> Self {
        Self {
            instance,
            cursor: None,
            fuel_per_call: budget.fuel_per_call,
            epoch_deadline_ticks: budget.epoch_deadline_ticks,
        }
    }

    pub fn cursor(&self) -> Option<&ChainPoint> {
        self.cursor.as_ref()
    }

    /// Push the latest interest set into the host state. The
    /// follower calls this after handling an `Interest` frame
    /// from the companion (v2 dynamic-interest path).
    pub fn set_interest(&mut self, interest: InterestSet) {
        self.instance.store.data_mut().set_interest(interest);
    }

    /// Snapshot the current interest set. Used by the follower's
    /// dynamic-interest path to compute Add/Remove deltas
    /// against the existing predicates without holding a borrow
    /// across the `update_interest` WIT call.
    pub fn interest(&self) -> InterestSet {
        self.instance.store.data().interest().clone()
    }

    /// Forward the CBOR-encoded predicates to the module's
    /// `update-interest` WIT export. The host has already applied
    /// the op to its own `InterestSet` for filtering; this call
    /// gives the module a chance to persist the change via
    /// `state-kv` so a future host restart without an attached
    /// companion still filters correctly.
    ///
    /// Returns the export's own result — `Err(String)` is the
    /// module's typed failure shape, surfaced for logging.
    pub async fn call_update_interest(
        &mut self,
        op: crate::bindings_v2::InterestOp,
        items_cbor: &[u8],
    ) -> wasmtime::Result<Result<(), String>> {
        // Refuel before calling — `update-interest` is a small
        // call but the module may persist via state-kv which
        // burns a few thousand fuel units. Using `fuel_per_call`
        // keeps it bounded.
        self.instance.store.set_fuel(self.fuel_per_call)?;
        self.instance
            .bindings
            .call_update_interest(&mut self.instance.store, op, items_cbor)
            .await
    }

    /// Invoke the module's `rebootstrap` WIT export. The host's
    /// recapture flow (`docs/design/RECAPTURE.md`) calls this
    /// after `start()` so a self-bootstrapping module re-runs its
    /// own cold-start and refills companions whose projected state
    /// was just dropped. Event-driven modules implement it as a
    /// no-op (their refill comes from `run_bootstrap`).
    ///
    /// Fueled with `fuel_per_call` — each call does one bounded
    /// page of the module's cold-start scan and reports a
    /// `RebootstrapStep` (`done` + an `ingested` UTxO count). The
    /// host loops, refuelling per call, until a step comes back
    /// `done`; `Err(String)` is the module's typed failure.
    pub async fn call_rebootstrap(
        &mut self,
    ) -> wasmtime::Result<Result<crate::bindings_v2::RebootstrapStep, String>> {
        self.instance.store.set_fuel(self.fuel_per_call)?;
        // Clear the per-call OOM flag so `classify_trap` reflects
        // only this invocation if it traps.
        self.instance.store.data_mut().limiter_mut().reset_call();

        let result = self
            .instance
            .bindings
            .call_rebootstrap(&mut self.instance.store)
            .await;

        // Feed this call's budget telemetry into the adaptive
        // page sizer so the *next* `rebootstrap` call's
        // `utxos-by-*` pages are sized to fit. AIMD: spare fuel
        // grows the page, fuel pressure or an OOM shrinks it.
        // `get_fuel` is valid whether the call returned or
        // trapped — the Store outlives the trap.
        let fuel_remaining = self.instance.store.get_fuel().unwrap_or(0);
        let fuel_used = self.fuel_per_call.saturating_sub(fuel_remaining);
        let hit_oom = self.instance.store.data().limiter().hit_oom();
        self.instance
            .store
            .data_mut()
            .adaptive_mut()
            .observe(fuel_used, self.fuel_per_call, hit_oom);

        result
    }

    /// Current adaptive page-size clamp, in refs — telemetry for
    /// the recapture loop's logging.
    pub fn adaptive_page_limit(&self) -> u32 {
        self.instance.store.data().adaptive.current()
    }

    /// Classify a trap returned by a guest call on this driver's
    /// instance. Reads the budget limiter off the `Store` to tell
    /// an OOM apart from a fuel exhaustion or a module fault. See
    /// `crate::budget::TrapClass`.
    pub fn classify_trap(&self, err: &wasmtime::Error) -> crate::budget::TrapClass {
        let oom = self.instance.store.data().limiter().hit_oom();
        crate::budget::TrapClass::classify(err, oom)
    }

    /// Lifetime peak linear-memory use of this driver's instance,
    /// in bytes. Telemetry for trap diagnostics + (Phase 2)
    /// adaptive page sizing.
    pub fn peak_memory_bytes(&self) -> usize {
        self.instance.store.data().limiter().peak_memory_bytes()
    }

    /// Apply one block. The driver pulls the InterestSet off the
    /// host state (already populated via `set_interest`) and
    /// resolves prior outputs through the data plane to filter
    /// + dispatch event batches.
    pub async fn apply_block<P: ChainDataPlane + Sync>(
        &mut self,
        cbor: &[u8],
        plane: &P,
    ) -> PlatformResult<ApplyOutcomeV2> {
        // 1. Decode the block.
        let decoded = match decode_block_v2(cbor) {
            Ok(d) => d,
            Err(e) => {
                // Match v1's posture: host-side decode failure is
                // a bad block, not a module bug. Surface as the
                // platform's own decode error so the follower
                // can skip + log.
                return Err(PlatformError::Decode(format!("v2 block decode: {e}")));
            }
        };
        let cursor_after = decoded.cursor.clone();

        // 2. Filter + batch via the dispatch composer.
        let interest = self.instance.store.data().interest().clone();
        let batches = build_event_batches(decoded, &interest, plane)
            .await
            .map_err(|e| PlatformError::Decode(format!("build_event_batches: {e}")))?;

        if batches.is_empty() {
            self.cursor = Some(cursor_after);
            return Ok(ApplyOutcomeV2::AppliedEmpty);
        }

        // 3. Stamp the cursor on host state so emit-event tags
        // emissions correctly, then dispatch each batch with a
        // fresh fuel budget.
        self.instance.store.data_mut().current_cursor = Some(cursor_after.clone());

        for batch in batches {
            self.dispatch_batch(batch)
                .await
                .map_err(PlatformError::Wasmtime)?;
        }

        self.cursor = Some(cursor_after);
        Ok(ApplyOutcomeV2::Applied)
    }

    /// Hand one TX's worth of events to the module via
    /// `handle-events`. Refuels before the call so each batch
    /// gets a fresh ceiling regardless of prior dispatches.
    async fn dispatch_batch(&mut self, batch: TxEventBatch) -> wasmtime::Result<()> {
        // Convert internal events → WIT-bindgen shape. One
        // allocation per event today; a smarter v2.x could
        // reuse buffers across batches.
        let wit_events: Vec<WitDispatchEvent> = batch
            .events
            .into_iter()
            .map(|u| event_bindings::dispatch_to_wit(DispatchEvent::Utxo(Box::new(u))))
            .collect();

        self.instance.store.set_fuel(self.fuel_per_call)?;
        self.instance
            .store
            .set_epoch_deadline(self.epoch_deadline_ticks);
        self.instance
            .bindings
            .call_handle_events(&mut self.instance.store, &wit_events)
            .await
    }

    /// Convenience for bootstrap orchestration: dispatch a
    /// pre-built batch (used by the bootstrap pass which
    /// synthesises events from current-state UTxOs rather than
    /// decoding a block).
    pub async fn dispatch_synthetic_batch(
        &mut self,
        batch: TxEventBatch,
        cursor: ChainPoint,
    ) -> wasmtime::Result<()> {
        self.instance.store.data_mut().current_cursor = Some(cursor);
        self.dispatch_batch(batch).await
    }

    /// Fire a `Rollback` event. Called by the follower on
    /// `TipEvent::Undo` — gives the module a chance to roll its
    /// derived state back via the companion DO.
    pub async fn dispatch_rollback(&mut self, to_cursor: ChainPoint) -> wasmtime::Result<()> {
        let event = event_bindings::dispatch_to_wit(DispatchEvent::Rollback(
            mitos_data_plane::RollbackEvent {
                to_cursor: to_cursor.clone(),
            },
        ));
        self.instance.store.data_mut().current_cursor = Some(to_cursor.clone());
        self.instance.store.set_fuel(self.fuel_per_call)?;
        self.instance
            .store
            .set_epoch_deadline(self.epoch_deadline_ticks);
        self.instance
            .bindings
            .call_handle_events(&mut self.instance.store, &[event])
            .await?;
        self.cursor = Some(to_cursor);
        Ok(())
    }

    /// Fire a single `Tick` event. Called by the tick scheduler
    /// when one of the module's `tick-every` intervals is due.
    pub async fn dispatch_tick(
        &mut self,
        tick: mitos_data_plane::TickEvent,
    ) -> wasmtime::Result<()> {
        let cursor = tick.cursor.clone();
        let event = event_bindings::dispatch_to_wit(DispatchEvent::Tick(tick));
        self.instance.store.data_mut().current_cursor = Some(cursor);
        self.instance.store.set_fuel(self.fuel_per_call)?;
        self.instance
            .store
            .set_epoch_deadline(self.epoch_deadline_ticks);
        self.instance
            .bindings
            .call_handle_events(&mut self.instance.store, &[event])
            .await
    }

    /// Suppress unused-field warning until follower wires up
    /// fuel-yield-interval refresh.
    #[allow(dead_code)]
    fn _budget_marker(&self) -> Arc<()> {
        Arc::new(())
    }
}
