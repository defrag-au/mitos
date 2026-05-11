//! v2 `wasmtime::component::bindgen!` invocation. Generates the
//! host trait + dispatcher from `../wit-v2/world.wit`.
//!
//! Sister to `bindings.rs` (v1). Both worlds coexist for the v2
//! migration window; once all modules ship under v2, v1 bindings
//! and the surrounding dispatch path get deleted.
//!
//! Key shape differences from v1:
//!
//! - **No `block-context` interface.** Modules don't see a
//!   `resolved-block` resource. The platform dispatches typed
//!   `dispatch-event` values via `handle-events`.
//! - **`chain-data` adds `read-tx`** — full TX rollup for
//!   modules that need cross-participant lookups (atomic swap
//!   counterparties, marketplace-fee inspection).
//! - **`interest` predicate vocabulary expanded** —
//!   `at-stake-cred`, `tick-every` join `at-address`,
//!   `holds-policy`, `holds-asset`.
//!
//! Bindgen knobs match v1 (async imports/exports, trappable host
//! fns) — same wasmtime engine + linker can serve both worlds
//! at runtime; ABI version handshake routes per-module.

wasmtime::component::bindgen!({
    path: "wit-v2",
    world: "mitos-module-v2",
    imports: { default: async | trappable },
    exports: { default: async },
});

// Re-export the bindgen-generated trait surface at stable
// crate-internal paths. v2 has no `block-context` resource,
// so no resource-bearing trait re-exports needed; everything
// is plain `Host` traits per interface.
pub use mitos::platform_v2::chain_data::Host as ChainDataHost;
pub use mitos::platform_v2::emit::Host as EmitHost;
pub use mitos::platform_v2::interest::Host as InterestHost;
pub use mitos::platform_v2::logging::{Host as LoggingHost, LogLevel};
pub use mitos::platform_v2::state_kv::Host as StateKvHost;
// `DispatchEvent`, `TrapStrategy`, `RetryPolicy` are pulled into
// the top-level scope by the world's `use types.{...}` clause,
// so they're addressable as `crate::bindings_v2::DispatchEvent`
// without an explicit re-export here.
pub use mitos::platform_v2::types::{
    AssetEntry, AssetId, ChainPoint, ConsumedEvent, ConsumedInput, Host as TypesHost, MintEntry,
    MintedEvent, OutputRef, ProducedEvent, ReferencedEvent, ReferencedInput, RollbackEvent,
    SpecificPoint, StakeCred, TickEvent, TxContextEvent, TxRecord, TypedDatum, TypedOutput,
    UtxoEvent, ValidityInterval,
};
