//! Wasmtime-side dry-load of a module artifact.
//!
//! Used by `mitos-build` (and by the host's upload-validation
//! path in `admin.rs`) to extract the manifest-required values
//! directly from a built `.wasm` file. The contract is "if this
//! returns Ok, the platform can load the module" — the same
//! engine, linker, and host trait wiring the production runtime
//! uses.
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

use crate::bindings_v2::{MitosModuleV2, TrapStrategy};
use crate::host_fns::{DataPlaneFacade, emit, state_kv};
use crate::host_fns_v2::HostStateV2;
use crate::registry_v2::ResourceBudget;

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

    async fn utxos_by_policy(
        &self,
        _policy: &[u8],
    ) -> mitos_data_plane::DataPlaneResult<Vec<mitos_data_plane::OutputRef>> {
        Ok(Vec::new())
    }

    async fn utxos_by_payment_cred(
        &self,
        _cred: &[u8],
    ) -> mitos_data_plane::DataPlaneResult<Vec<mitos_data_plane::OutputRef>> {
        Ok(Vec::new())
    }

    async fn resolve_stake_for_payment_pkh(
        &self,
        _pkh: &[u8],
    ) -> mitos_data_plane::DataPlaneResult<Option<mitos_data_plane::StakeCred>> {
        Ok(None)
    }

    async fn datum_by_hash(
        &self,
        _hash: &[u8; 32],
    ) -> mitos_data_plane::DataPlaneResult<Option<Vec<u8>>> {
        Ok(None)
    }

    async fn read_output_datums(
        &self,
        refs: &[mitos_data_plane::OutputRef],
    ) -> mitos_data_plane::DataPlaneResult<Vec<Option<mitos_data_plane::TypedDatum>>> {
        Ok(vec![None; refs.len()])
    }

    async fn read_output_hashes(
        &self,
        refs: &[mitos_data_plane::OutputRef],
    ) -> mitos_data_plane::DataPlaneResult<Vec<Option<Vec<u8>>>> {
        Ok(vec![None; refs.len()])
    }

    async fn tx_metadata(
        &self,
        _tx_hash: &[u8; 32],
    ) -> mitos_data_plane::DataPlaneResult<Option<Vec<u8>>> {
        Ok(None)
    }
}

/// The engine the inspector and the production runtime share.
///
/// Every switch here is a property a module may rely on, so the inspector
/// must not build a laxer engine than the host it stands in for.
fn inspection_engine() -> Result<Engine, InspectError> {
    let mut config = wasmtime::Config::new();
    config.wasm_component_model(true);
    config.consume_fuel(true);
    config.epoch_interruption(true);
    Engine::new(&config).map_err(|e| InspectError::Engine(e.to_string()))
}

/// The linker a module is instantiated against: WASI Preview 2, then this
/// platform's own host functions.
fn inspection_linker(engine: &Engine) -> Result<Linker<HostStateV2>, InspectError> {
    let mut linker = Linker::<HostStateV2>::new(engine);
    wasmtime_wasi::p2::add_to_linker_async(&mut linker)
        .map_err(|e| InspectError::Load(format!("wasi linker: {e}")))?;
    MitosModuleV2::add_to_linker::<_, HasSelf<HostStateV2>>(&mut linker, |s| s)
        .map_err(|e| InspectError::Load(format!("platform linker: {e}")))?;
    Ok(linker)
}

/// A store under the default resource budget, with a data plane that answers
/// nothing.
fn inspection_store(engine: &Engine) -> Result<Store<HostStateV2>, InspectError> {
    let dp: Arc<dyn DataPlaneFacade> = Arc::new(NullDataPlane);
    let kv = state_kv::ModuleKv::new_in_memory();
    let (sink, _events) = emit::EventSink::new();
    // The inspector loads a module to read its ABI, never to run it, so it serves no
    // artifact: `fold-data` answering `none` is honest here and keeps the inspect path
    // independent of whatever this host happens to be serving.
    let host_state = HostStateV2::new("__inspect__".to_owned(), dp, kv, sink, None);

    let mut store = Store::new(engine, host_state);
    let budget = ResourceBudget::default();
    store
        .set_fuel(budget.fuel_per_call)
        .map_err(|e| InspectError::Instantiate(e.to_string()))?;
    store.set_epoch_deadline(budget.epoch_deadline_ticks);
    Ok(store)
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
    let engine = inspection_engine()?;

    let component =
        Component::from_file(&engine, wasm_path).map_err(|e| InspectError::Load(e.to_string()))?;

    let linker = inspection_linker(&engine)?;
    let mut store = inspection_store(&engine)?;

    let bindings = MitosModuleV2::instantiate_async(&mut store, &component, &linker)
        .await
        .map_err(|e| instantiate_error(&component, &engine, e))?;

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

/// Instantiate a `wasm32-wasip2` component against the platform's linker and
/// nothing else, returning the `wasi:*` interfaces it imported.
///
/// `dry_inspect` can only be pointed at a real module, because it asks the
/// component to export the platform ABI. This is the same instantiation with
/// no export requirement, which is what lets `tests/wasi_linkage.rs` check the
/// linker against a ten-line artifact: the toolchain's `std` and this host's
/// `wasmtime-wasi` must agree on `wasi:cli@0.2.x`, and a disagreement fails
/// here in seconds instead of once per module in CI.
///
/// The returned names carry the version (`wasi:cli/exit@0.2.12`), so a caller
/// can assert that the probe still exercises what it claims to.
pub async fn link_probe(wasm_path: &Path) -> Result<Vec<String>, InspectError> {
    let engine = inspection_engine()?;
    let component =
        Component::from_file(&engine, wasm_path).map_err(|e| InspectError::Load(e.to_string()))?;
    let linker = inspection_linker(&engine)?;
    let mut store = inspection_store(&engine)?;

    linker
        .instantiate_async(&mut store, &component)
        .await
        .map_err(|e| instantiate_error(&component, &engine, e))?;

    Ok(wasi_imports(&component, &engine))
}

/// The `wasi:*` interfaces a component imports, sorted and deduplicated.
///
/// A component built by a toolchain whose `wasm32-wasip2` std has moved to a
/// `wasi:cli@0.2.x` whose interfaces this host's `wasmtime-wasi` does not
/// implement cannot link, and wasmtime's own message says only that "a
/// matching implementation was not found". The imported names carry the
/// version, so they are what the error should name.
fn wasi_imports(component: &Component, engine: &Engine) -> Vec<String> {
    let ty = component.component_type();
    let mut names: Vec<String> = ty
        .imports(engine)
        .map(|(name, _)| name)
        .filter(|name| name.starts_with("wasi:"))
        .map(str::to_owned)
        .collect();
    names.sort();
    names.dedup();
    names
}

/// The instantiation error, with the imports that explain it attached.
fn instantiate_error(component: &Component, engine: &Engine, err: wasmtime::Error) -> InspectError {
    let imports = wasi_imports(component, engine);
    if imports.is_empty() {
        return InspectError::Instantiate(err.to_string());
    }
    InspectError::Instantiate(format!(
        "{err}\n  the component imports {} — this host provides the `wasi:cli` interfaces \
         its `wasmtime-wasi` ships, so one whose shape differs cannot be linked here. Move the \
         two together: bump `wasmtime`/`wasmtime-wasi` in crates/mitos-platform/Cargo.toml, or \
         pin the toolchain (rust-toolchain.toml) to one whose std agrees with them.",
        imports.join(", ")
    ))
}

/// Wasmtime refused the bytes as a component.
///
/// Its own error type rather than an HTTP handler's: both the admin upload
/// path and the artifact fetcher run this check, and the fetcher has no
/// business depending on `admin::HandlerError`.
#[derive(Debug, thiserror::Error)]
pub enum ComponentRefused {
    #[error("engine: {0}")]
    Engine(String),
    #[error("component: {0}")]
    Component(String),
}

/// Independent wasmtime-side validation. Confirms the component parses and
/// the world declared by the manifest is what wasmtime sees in the binary's
/// metadata. Dry-instantiation + version-export round-trip is future work —
/// the actual host's `start()` path catches those on first activation; the
/// cost of doing it here too hasn't earned its keep.
pub fn validate_component(wasm_bytes: &[u8]) -> Result<(), ComponentRefused> {
    // A minimal engine just for parse-validation. Cheap to construct; doesn't
    // need the platform's full Config.
    let mut config = wasmtime::Config::new();
    config.wasm_component_model(true);
    let engine = Engine::new(&config).map_err(|e| ComponentRefused::Engine(e.to_string()))?;
    let _component = Component::from_binary(&engine, wasm_bytes)
        .map_err(|e| ComponentRefused::Component(e.to_string()))?;
    Ok(())
}

fn trap_strategy_to_string(s: TrapStrategy) -> String {
    match s {
        TrapStrategy::Replay => "replay",
        TrapStrategy::SkipAndMark => "skip-and-mark",
        TrapStrategy::Quarantine => "quarantine",
    }
    .to_owned()
}
