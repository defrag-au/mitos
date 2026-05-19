//! v2 host-fn implementations.
//!
//! Sister to `host_fns/` (v1). v2 uses a different bindgen world
//! (`mitos-module-v2`), so the WIT-generated `Host` traits live
//! at different paths. Each interface in the v2 WIT gets its own
//! submodule here, mirroring v1's layout.
//!
//! Differences from v1:
//! - `chain-data` is extended with `read-tx` and
//!   `datum-by-hash`; method signatures are slimmer (no
//!   `decode-level` parameter — v2 always returns lean outputs).
//! - No `block-context` interface (v2 dispatches typed events;
//!   modules never see a block resource).
//! - `interest` carries an `InterestSet` on the host state so
//!   per-call dispatch can filter.
//!
//! `state-kv`, `emit`, `logging` mirror v1 closely — same store
//! types, just plumbed against v2 bindings.

pub mod chain_data;
pub mod emit;
pub mod interest;
pub mod logging;
pub mod state_kv;

use std::sync::Arc;

use mitos_data_plane::{ChainPoint, InterestSet};
use wasmtime::component::ResourceTable;
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use crate::bindings_v2::{InterestHost, TypesHost};
use crate::host_fns::DataPlaneFacade;

/// Per-instance host state for v2 modules. One per wasmtime
/// `Store`. v2 piggybacks on v1's `state_kv` / `emit` /
/// `DataPlaneFacade` types since those are unchanged in v2.
pub struct HostStateV2 {
    pub table: ResourceTable,

    pub(crate) data_plane: Arc<dyn DataPlaneFacade>,
    pub(crate) kv: crate::host_fns::state_kv::ModuleKv,
    pub(crate) emitter: crate::host_fns::emit::EventSink,
    pub(crate) module_id: String,

    /// Cursor of the current dispatch — set by the driver
    /// before each `call_handle_events`. Used by `emit-event`
    /// to tag emissions with the chain point at which they
    /// were produced.
    pub(crate) current_cursor: Option<ChainPoint>,

    /// Module's currently-watched interest set. Updated by the
    /// host's `update-interest` dispatch path; persisted by the
    /// module via `state-kv` for restart-without-companion
    /// resilience.
    pub(crate) interest: InterestSet,

    /// WASI context. Same shape as v1.
    pub(crate) wasi: WasiCtx,
    pub(crate) wasi_table: ResourceTable,

    /// Per-instance resource-budget telemetry. Wired onto the
    /// `Store` as its `ResourceLimiter` in `registry_v2`; records
    /// peak linear memory + flags a denied/failed `memory.grow`
    /// so the host can classify a trap as OOM. See
    /// `crate::budget` and `WASM_BUDGET_CHUNKING.md`.
    pub(crate) limiter: crate::budget::BudgetLimiter,
}

impl HostStateV2 {
    pub fn new(
        module_id: String,
        data_plane: Arc<dyn DataPlaneFacade>,
        kv: crate::host_fns::state_kv::ModuleKv,
        emitter: crate::host_fns::emit::EventSink,
    ) -> Self {
        Self {
            table: ResourceTable::new(),
            data_plane,
            kv,
            emitter,
            module_id,
            current_cursor: None,
            interest: InterestSet::default(),
            wasi: WasiCtxBuilder::new().build(),
            wasi_table: ResourceTable::new(),
            // Phase 1: observational only — no host-imposed
            // memory ceiling, so the module's declared maximum
            // stays the sole cap.
            limiter: crate::budget::BudgetLimiter::new(None),
        }
    }

    /// Read-only access to this instance's budget telemetry.
    /// The driver reads it after a guest call to classify a trap.
    pub fn limiter(&self) -> &crate::budget::BudgetLimiter {
        &self.limiter
    }

    /// Mutable access to the budget telemetry — used to clear the
    /// per-call OOM flag (`reset_call`) before an invocation the
    /// host intends to classify.
    pub fn limiter_mut(&mut self) -> &mut crate::budget::BudgetLimiter {
        &mut self.limiter
    }

    pub fn module_id(&self) -> &str {
        &self.module_id
    }

    pub fn interest(&self) -> &InterestSet {
        &self.interest
    }

    pub fn set_interest(&mut self, interest: InterestSet) {
        self.interest = interest;
    }
}

impl WasiView for HostStateV2 {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.wasi_table,
        }
    }
}

// Empty interface marker traits — bindgen requires impls even
// for interfaces with no free functions.
impl TypesHost for HostStateV2 {}
impl InterestHost for HostStateV2 {}
