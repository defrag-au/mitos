//! `wasmtime::component::bindgen!` invocation. Generates the
//! host trait + dispatcher from `../wit/world.wit`.
//!
//! Bindgen knobs proven out by the WIT spike at
//! `spikes/wit-spike/`:
//!
//! - `imports: { default: async | trappable }` — host fns return
//!   `Future<Result<T, wasmtime::Error>>`. Stable since wasmtime
//!   12; the same shape Spin v4 ships in production.
//! - `exports: { default: async }` — guest exports awaited
//!   host-side; required for fuel-driven cooperative yield.
//! - `with:` path format is `<package>:<name>/<interface>.<resource>`
//!   — DOT before resource, NOT slash. (See
//!   `wasmtime-internal-wit-bindgen-44.0.1/src/lib.rs:1196-1203`.)
//!
//! Empty interface-level marker traits (`types::Host`,
//! `block_context::Host`) must be implemented on the host data
//! type even when the interface has no methods — bindgen
//! requires them.

wasmtime::component::bindgen!({
    path: "wit",
    world: "mitos-module",
    imports: { default: async | trappable },
    exports: { default: async },
    with: {
        "mitos:platform/block-context.resolved-block": crate::resolved_block::ResolvedBlock,
    },
});

// Re-export the bindgen-generated trait surface at stable
// crate-internal paths so subsystems (registry, host_fns,
// supervisor) don't reach into the macro-generated module tree
// directly. `MitosModule` (the world's component handle with
// `instantiate_async`, `call_*`, `add_to_linker`) is generated
// at this module's level by the `bindgen!` macro and is already
// addressable as `crate::bindings::MitosModule` — no re-export
// needed.
//
// Note: `RetryPolicy` and `TrapStrategy` are pulled into the
// bindgen-generated module's top-level scope by the world's
// `use types.{...}` declaration, so they are accessible as
// `super::RetryPolicy` / `super::TrapStrategy` from inside this
// crate without explicit re-export here.

pub use mitos::platform::block_context::{Host as BlockContextHost, HostResolvedBlock};
pub use mitos::platform::chain_data::Host as ChainDataHost;
pub use mitos::platform::emit::Host as EmitHost;
pub use mitos::platform::logging::{Host as LoggingHost, LogLevel};
pub use mitos::platform::state_kv::Host as StateKvHost;
pub use mitos::platform::types::{
    AssetEntry, AssetId, DecodeLevel, Host as TypesHost, OutputRef, TypedOutput,
};
