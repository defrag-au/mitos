//! Wasmtime-side dry-load of a module artifact.
//!
//! Used by `mitos-build` (and by the host's upload validation in
//! phase 2) to extract the manifest-required values directly
//! from a built `.wasm` file. The contract is "if this returns
//! Ok, the platform can load the module" — the same engine,
//! linker, and host trait wiring the production runtime uses.
//!
//! Inspecting via `instantiate_async` + calling the exported
//! `module-version()` / `trap-policy()` is the strongest possible
//! signal that the artifact is a valid mitos-platform component:
//! if wasmtime accepts it against our linker, the WIT world
//! matches and the imports resolve.

use std::path::Path;
use std::sync::Arc;

use wasmtime::component::{Component, HasSelf, Linker};
use wasmtime::{Engine, Store};

use crate::bindings::{MitosModule, TrapStrategy};
use crate::host_fns::{DataPlaneFacade, HostState, emit, state_kv};
use crate::registry::ResourceBudget;

/// Snapshot of the values the manifest needs to declare.
/// Returned by `dry_inspect`; callers (build tool, upload
/// handler) use these to either generate or cross-check
/// manifest content.
#[derive(Debug, Clone)]
pub struct InspectResult {
    pub abi_version_major: u32,
    pub abi_version_minor: u32,
    pub trap_strategy: String,
    pub trap_max_retries: u32,
    pub trap_backoff_cap_ms: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum InspectError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("wasmtime engine: {0}")]
    Engine(String),
    #[error("wasmtime component load: {0}")]
    Load(String),
    #[error("wasmtime instantiate: {0}")]
    Instantiate(String),
    #[error("module export call failed: {0}")]
    ExportCall(String),
}

/// Stub data plane for inspect — `module-version()` and
/// `trap-policy()` don't touch chain state, so this never fires.
struct NullDataPlane;

#[async_trait::async_trait]
impl DataPlaneFacade for NullDataPlane {
    async fn read_utxos(
        &self,
        _refs: &[mitos_data_plane::OutputRef],
        _decode: mitos_data_plane::DecodeLevel,
    ) -> mitos_data_plane::DataPlaneResult<
        Vec<(mitos_data_plane::OutputRef, mitos_data_plane::TypedOutput)>,
    > {
        Ok(Vec::new())
    }

    async fn utxos_by_address(
        &self,
        _address: &str,
    ) -> mitos_data_plane::DataPlaneResult<Vec<mitos_data_plane::OutputRef>> {
        Ok(Vec::new())
    }
}

/// Load a wasm component, instantiate against the platform's
/// linker, call the two metadata-bearing exports
/// (`module-version`, `trap-policy`), return the values.
///
/// Failure modes — each maps cleanly to a manifest-validation
/// error the caller surfaces:
/// - `Engine` — wasmtime config invalid (impossible in practice)
/// - `Load` — wasm bytes don't parse as a component, or the
///   component's WIT world isn't ours (linker rejects imports)
/// - `Instantiate` — component instantiated but a host fn
///   resolution failed
/// - `ExportCall` — the exports trapped, returned malformed
///   values, or hit the resource limit during the dry call
pub async fn dry_inspect(wasm_path: &Path) -> Result<InspectResult, InspectError> {
    // Build the same engine the production runtime uses so
    // anything that passes here also passes there.
    let mut config = wasmtime::Config::new();
    config.wasm_component_model(true);
    config.consume_fuel(true);
    config.epoch_interruption(true);
    let engine = Engine::new(&config).map_err(|e| InspectError::Engine(e.to_string()))?;

    let component =
        Component::from_file(&engine, wasm_path).map_err(|e| InspectError::Load(e.to_string()))?;

    // Build the platform's linker (WASI + host fns). If the
    // module's WIT world doesn't match, this is where it'll
    // fail at instantiate time.
    let mut linker = Linker::<HostState>::new(&engine);
    wasmtime_wasi::p2::add_to_linker_async(&mut linker)
        .map_err(|e| InspectError::Load(format!("wasi linker: {e}")))?;
    MitosModule::add_to_linker::<_, HasSelf<HostState>>(&mut linker, |s| s)
        .map_err(|e| InspectError::Load(format!("platform linker: {e}")))?;

    // Minimal host state — the dry call doesn't exercise any of
    // the I/O surface, but the bindings need everything wired.
    let dp: Arc<dyn DataPlaneFacade> = Arc::new(NullDataPlane);
    let kv = state_kv::ModuleKv::new_in_memory();
    let (sink, _events) = emit::EventSink::new();
    let host_state = HostState::new("__inspect__".to_owned(), dp, kv, sink);

    let mut store = Store::new(&engine, host_state);
    // Generous budgets; the dry exports are tiny pure functions.
    let budget = ResourceBudget::default();
    store
        .set_fuel(budget.fuel_per_call)
        .map_err(|e| InspectError::Instantiate(e.to_string()))?;
    store.set_epoch_deadline(budget.epoch_deadline_ticks);

    let bindings = MitosModule::instantiate_async(&mut store, &component, &linker)
        .await
        .map_err(|e| InspectError::Instantiate(e.to_string()))?;

    let (major, minor) = bindings
        .call_module_version(&mut store)
        .await
        .map_err(|e| InspectError::ExportCall(format!("module-version: {e}")))?;

    let (strategy, retry) = bindings
        .call_trap_policy(&mut store)
        .await
        .map_err(|e| InspectError::ExportCall(format!("trap-policy: {e}")))?;

    Ok(InspectResult {
        abi_version_major: major,
        abi_version_minor: minor,
        trap_strategy: trap_strategy_to_string(strategy),
        trap_max_retries: retry.max_retries,
        trap_backoff_cap_ms: retry.backoff_cap_ms,
    })
}

fn trap_strategy_to_string(s: TrapStrategy) -> String {
    match s {
        TrapStrategy::Replay => "replay",
        TrapStrategy::SkipAndMark => "skip-and-mark",
        TrapStrategy::Quarantine => "quarantine",
    }
    .to_owned()
}
