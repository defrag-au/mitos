//! Module registry — load module artifact, version-check,
//! instantiate.
//!
//! V1: one slot. The registry holds the wasmtime `Engine` and a
//! single loaded `Component`; instances are created on demand
//! (one per active CF subscription in production; one per test
//! invocation today).
//!
//! Wasmtime config knobs proven out by the WIT spike (see
//! `MITOS_PLATFORM_V1.md` §"Resolved design questions"):
//!
//! - `wasm_component_model(true)` — required for component-typed
//!   wasm artifacts.
//! - `consume_fuel(true)` — must be set before `store.set_fuel`.
//! - `epoch_interruption(true)` — must be paired with
//!   `store.set_epoch_deadline(N)` before any guest call,
//!   otherwise the first instruction traps as `wasm trap:
//!   interrupt`.
//! - Do NOT call `Config::async_support(true)` — deprecated /
//!   no-op in wasmtime 42+.
//! - Do NOT enable `wasm_component_model_async` — that's the
//!   stream/future ABI which our WIT does not use and wasmtime
//!   44 documents as "_very_ incomplete".

use std::path::Path;
use std::sync::Arc;

use wasmtime::component::{Component, HasSelf, Linker};
use wasmtime::{Config, Engine, Store};

use crate::bindings::MitosModule;
use crate::host_fns::{DataPlaneFacade, HostState, emit, state_kv};
use crate::supervisor::Supervisor;
use crate::{PlatformError, PlatformResult};

/// Wasmtime engine + a single loaded module component. V1 has
/// one slot; v2 will fan out to N tenants by holding a
/// `HashMap<ModuleId, Component>`.
pub struct ModuleRegistry {
    pub engine: Engine,
    pub component: Component,
    pub module_id: String,
    pub linker: Linker<HostState>,
}

/// Major ABI version the host enforces. Modules whose
/// `module-version()` returns a different major refuse to load.
pub const HOST_ABI_MAJOR: u32 = 1;

/// One ready-to-dispatch module instance. Built by the registry
/// from a loaded component + a fresh `HostState`. Holds the
/// `Store`, the bindings, and the supervisor.
pub struct ModuleInstance {
    pub bindings: MitosModule,
    pub store: Store<HostState>,
    pub supervisor: Supervisor,
}

/// Per-instance resource budget. V1: generous defaults that
/// won't trip ownership-style indexers. v2 will tune per-module.
#[derive(Debug, Clone, Copy)]
pub struct ResourceBudget {
    pub fuel_per_call: u64,
    pub fuel_yield_interval: u64,
    pub epoch_deadline_ticks: u64,
}

impl Default for ResourceBudget {
    fn default() -> Self {
        Self {
            fuel_per_call: 100_000_000,
            fuel_yield_interval: 10_000,
            epoch_deadline_ticks: 1_000_000,
        }
    }
}

impl ModuleRegistry {
    /// Build the wasmtime engine with v1's resource-limit posture.
    /// Shared across all instances; the per-instance `Store`
    /// applies the actual limits.
    pub fn build_engine() -> wasmtime::Result<Engine> {
        let mut config = Config::new();
        config.wasm_component_model(true);
        config.epoch_interruption(true);
        config.consume_fuel(true);
        Engine::new(&config)
    }

    /// Build a `Linker` populated with the platform's host
    /// functions. The same linker is reused across all instances
    /// (engine-bound) so we pay the registration cost once.
    pub fn build_linker(engine: &Engine) -> wasmtime::Result<Linker<HostState>> {
        let mut linker = Linker::<HostState>::new(engine);
        // WASI Preview 2 imports first — wasm32-wasip2 std lib
        // pulls these in even when the module doesn't explicitly
        // use them.
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
        // Then platform host fns.
        MitosModule::add_to_linker::<_, HasSelf<HostState>>(&mut linker, |s| s)?;
        Ok(linker)
    }

    /// Load a module artifact from a filesystem path. The
    /// component is compiled here; ABI version handshake happens
    /// at first instantiation (we need a `Store` + bindings to
    /// call `module-version`).
    pub fn load_from_path(
        engine: Engine,
        module_id: String,
        wasm_path: &Path,
    ) -> PlatformResult<Self> {
        let component = Component::from_file(&engine, wasm_path)?;
        let linker = Self::build_linker(&engine)?;
        Ok(Self {
            engine,
            component,
            module_id,
            linker,
        })
    }

    /// Build a fresh `ModuleInstance`. Performs the ABI version
    /// handshake before returning; on mismatch returns
    /// `PlatformError::AbiMismatch`. Reads the module's declared
    /// `trap-policy` and primes the supervisor with it.
    pub async fn instantiate(
        &self,
        data_plane: Arc<dyn DataPlaneFacade>,
        kv: state_kv::ModuleKv,
        emitter: emit::EventSink,
        budget: ResourceBudget,
    ) -> PlatformResult<ModuleInstance> {
        let host_state = HostState::new(self.module_id.clone(), data_plane, kv, emitter);
        let mut store = Store::new(&self.engine, host_state);

        // Resource limits applied before any guest code runs. See
        // module docs above for why each is required up front.
        store.set_fuel(budget.fuel_per_call)?;
        store.fuel_async_yield_interval(Some(budget.fuel_yield_interval))?;
        store.set_epoch_deadline(budget.epoch_deadline_ticks);

        let bindings =
            MitosModule::instantiate_async(&mut store, &self.component, &self.linker).await?;

        // ABI version handshake — first call against the new
        // instance. A module that would mismatch must be rejected
        // before we hand it any state to corrupt.
        let (got_major, got_minor) = bindings.call_module_version(&mut store).await?;
        if got_major != HOST_ABI_MAJOR {
            return Err(PlatformError::AbiMismatch {
                wanted_major: HOST_ABI_MAJOR,
                got_major,
                got_minor,
            });
        }

        // Read the trap policy + retry config the module declares.
        let (strategy, retry) = bindings.call_trap_policy(&mut store).await?;
        let supervisor = Supervisor::new(strategy, retry);

        Ok(ModuleInstance {
            bindings,
            store,
            supervisor,
        })
    }
}
