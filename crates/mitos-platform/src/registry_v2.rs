//! v2 module registry — load + instantiate v2 modules.
//!
//! Sister to `registry.rs` (v1). Same wasmtime engine + linker
//! plumbing, different bindgen world (`mitos-module-v2`) and
//! `HostStateV2` as the Store data type.
//!
//! v2 modules return ABI version `(2, 0)` from
//! `module-version()`. Mismatch is enforced at instantiation —
//! v1 modules can't load through this path and vice-versa.

use std::path::Path;
use std::sync::Arc;

use wasmtime::component::{Component, HasSelf, Linker};
use wasmtime::{Engine, Store};

use crate::bindings_v2::MitosModuleV2;
use crate::host_fns::{DataPlaneFacade, emit, state_kv};
use crate::host_fns_v2::HostStateV2;
use crate::supervisor::Supervisor;
use crate::{PlatformError, PlatformResult};

/// Major ABI version v2 modules must declare. v2 is the only
/// supported ABI; v1 was retired (see MITOS_PLATFORM_V2.md).
pub const HOST_ABI_MAJOR_V2: u32 = 2;

/// Per-instance resource caps the host applies before any guest
/// call. Moved here from `registry.rs` when the v1 dispatch path
/// was retired.
///
/// `init_fuel` is separate from `fuel_per_call` because
/// bootstrap-style modules (jpg.store CO indexer, marketplace
/// indexers that scan a script address on first deploy) walk
/// thousands of UTxOs in `init()`, decoding metadata + hashing
/// per output. That's a one-shot heavyweight cost that doesn't
/// recur on subsequent dispatches; budgeting `init` separately
/// keeps the per-block ceiling tight without starving the
/// bootstrap path.
#[derive(Debug, Clone, Copy)]
pub struct ResourceBudget {
    pub fuel_per_call: u64,
    pub init_fuel: u64,
    pub fuel_yield_interval: u64,
    pub epoch_deadline_ticks: u64,
}

impl Default for ResourceBudget {
    fn default() -> Self {
        Self {
            fuel_per_call: 100_000_000,
            // Empirically calibrated: jpg-co's metadata-fallback
            // bootstrap costs ~630K fuel per output (CBOR parse
            // + hex decode + blake2b match + datum decode +
            // event emit). 10B keeps headroom for ~15K outputs;
            // jpg.store V2 currently sits at ~4.5K. Per-block
            // dispatch keeps the tighter `fuel_per_call` cap.
            init_fuel: 10_000_000_000,
            fuel_yield_interval: 10_000,
            epoch_deadline_ticks: 1_000_000,
        }
    }
}

/// Wasmtime engine + a single loaded v2 component.
pub struct ModuleRegistryV2 {
    pub engine: Engine,
    pub component: Component,
    pub module_id: String,
    pub linker: Linker<HostStateV2>,
}

/// One ready-to-dispatch v2 module instance.
pub struct ModuleInstanceV2 {
    pub bindings: MitosModuleV2,
    pub store: Store<HostStateV2>,
    pub supervisor: Supervisor,
}

impl ModuleRegistryV2 {
    /// Build a wasmtime engine for v2. v2 keeps v1's posture —
    /// component model, fuel-driven cooperative interruption,
    /// epoch-based hard deadlines.
    pub fn build_engine() -> wasmtime::Result<Engine> {
        let mut config = wasmtime::Config::new();
        config.wasm_component_model(true);
        config.epoch_interruption(true);
        config.consume_fuel(true);
        Engine::new(&config)
    }

    /// Build a v2-bindings linker. Same shape as v1's
    /// `build_linker` but against `HostStateV2` and
    /// `MitosModuleV2`.
    pub fn build_linker(engine: &Engine) -> wasmtime::Result<Linker<HostStateV2>> {
        let mut linker = Linker::<HostStateV2>::new(engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
        MitosModuleV2::add_to_linker::<_, HasSelf<HostStateV2>>(&mut linker, |s| s)?;
        Ok(linker)
    }

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

    /// Build a fresh `ModuleInstanceV2`. ABI handshake enforces
    /// v2 modules only.
    pub async fn instantiate(
        &self,
        data_plane: Arc<dyn DataPlaneFacade>,
        kv: state_kv::ModuleKv,
        emitter: emit::EventSink,
        budget: ResourceBudget,
    ) -> PlatformResult<ModuleInstanceV2> {
        let host_state = HostStateV2::new(self.module_id.clone(), data_plane, kv, emitter);
        let mut store = Store::new(&self.engine, host_state);

        store.set_fuel(budget.fuel_per_call)?;
        store.fuel_async_yield_interval(Some(budget.fuel_yield_interval))?;
        store.set_epoch_deadline(budget.epoch_deadline_ticks);

        let bindings =
            MitosModuleV2::instantiate_async(&mut store, &self.component, &self.linker).await?;

        let (got_major, got_minor) = bindings.call_module_version(&mut store).await?;
        if got_major != HOST_ABI_MAJOR_V2 {
            return Err(PlatformError::AbiMismatch {
                wanted_major: HOST_ABI_MAJOR_V2,
                got_major,
                got_minor,
            });
        }

        // Trap policy: the v2 WIT defines the same `trap-strategy`
        // Supervisor speaks the v2 bindgen types directly now
        // that v1 is gone — no conversion shim.
        let (strategy, retry) = bindings.call_trap_policy(&mut store).await?;
        let supervisor = Supervisor::new(strategy, retry);

        Ok(ModuleInstanceV2 {
            bindings,
            store,
            supervisor,
        })
    }
}
