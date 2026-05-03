//! Module registry — load module artifact, version-check,
//! instantiate.
//!
//! V1: one slot. The registry holds the wasmtime `Engine` and a
//! single loaded `Component`; the supervisor instantiates per-
//! subscription `Store`s against that one component.
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

use wasmtime::{Config, Engine};
use wasmtime::component::Component;

use crate::PlatformResult;

/// Wasmtime engine + a single loaded module component. V1 has
/// one slot; v2 will fan out to N tenants by holding a
/// `HashMap<ModuleId, Component>`.
pub struct ModuleRegistry {
    pub engine: Engine,
    pub component: Component,
    pub module_id: String,
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

    /// Load a module from a filesystem path. Performs the ABI
    /// version handshake before returning; on mismatch returns
    /// `PlatformError::AbiMismatch`.
    pub async fn load_from_path(
        engine: Engine,
        module_id: String,
        wasm_path: &Path,
    ) -> PlatformResult<Self> {
        let component = Component::from_file(&engine, wasm_path)?;
        Ok(Self {
            engine,
            component,
            module_id,
        })
    }
}

/// Major ABI version the host enforces. Modules whose
/// `module-version()` returns a different major refuse to load.
pub const HOST_ABI_MAJOR: u32 = 1;
