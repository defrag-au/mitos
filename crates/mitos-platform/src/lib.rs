//! Mitos platform — wasm-isolated module runtime (v2).
//!
//! See `../../../docs/strategy/MITOS_PLATFORM_V2.md` for the
//! full design rationale. Headlines:
//!
//! - **eUTXO event-stream dispatch.** Modules see typed events
//!   (`Produced`, `Consumed`, `TxContext`, `Tick`, `Rollback`)
//!   filtered host-side against their declared interest set.
//! - **WIT-defined ABI.** `wit-v2/world.wit` is the contract;
//!   `wasmtime::component::bindgen!` generates host bindings,
//!   `wit-bindgen` generates guest stubs.
//! - **Resource limits from day one.** Fuel + epoch interruption
//!   + `ResourceLimiter` configured before any guest call.
//! - **Author-declared trap strategy.** Module exports
//!   `trap-policy()`; supervisor consults it on every trap.
//! - **Manifest-driven bootstrap.** Per-address interest
//!   predicates trigger a synthetic event scan at module start,
//!   so historical state hydrates through the same dispatch
//!   path as live chain activity.
//!
//! Crate layout:
//!
//! - `bindings_v2` — wasmtime `bindgen!` invocation for the
//!   `mitos:platform-v2` world
//! - `host_fns` — shared `DataPlaneFacade` trait + `state_kv` +
//!   `emit` channel types
//! - `host_fns_v2` — host-side impls of the v2 WIT-imported
//!   interfaces (chain-data, state-kv, emit, logging, interest)
//! - `registry_v2` — load module artifact, version-check,
//!   instantiate, `ResourceBudget`
//! - `driver_v2`, `follower_v2`, `host_v2` — per-block dispatch,
//!   chain follower, lifecycle manager
//! - `bootstrap_v2` — synthetic event scan for manifest interest
//! - `supervisor` — trap policy enforcement + bounded retry +
//!   quarantine
//! - `vendored` — files vendored from upstream (Apache-2.0;
//!   see `vendored/balius/NOTICE`)
//!
//! The v1 dispatch path (block-CBOR-resource model with
//! `ResolvedBlock`, `Driver`, `ModuleHost`, `host_unified`)
//! was retired in May 2026 — both production modules
//! (`ownership`, `jpg-co`) had migrated to v2 and no remaining
//! consumer needed v1 dispatch.

pub mod admin;
pub mod bindings_v2;
pub mod bootstrap_v2;
pub mod compaction;
pub mod companions;
pub mod dialer;
pub mod driver_v2;
pub mod emissions;
pub mod event_bindings;
pub mod follower_v2;
pub mod host_fns;
pub mod host_fns_v2;
pub mod host_v2;
pub mod indexer_bridge;
pub mod inspect;
pub mod lag_tolerant;
pub mod manifest;
pub mod registry_v2;
pub mod storage;
pub mod supervisor;
pub mod trap_context;
pub mod vendored;

/// Re-exported so bundle consumers can construct
/// `Arc<dyn ChainDataPlane>` to pass into
/// `admin::admin_router_with_host` without a separate dep on
/// `mitos-data-plane`.
pub use mitos_data_plane::ChainDataPlane;
/// Re-export the platform-internal `ChainPoint` from
/// `mitos-data-plane` so consumers (like the bundle binary) can
/// take/return it without a separate dep on `mitos-data-plane`.
pub use mitos_data_plane::ChainPoint;
pub use registry_v2::{ModuleInstanceV2, ModuleRegistryV2, ResourceBudget};
pub use supervisor::{Supervisor, SupervisorOutcome};

/// Platform-level error surface. Per-host-fn errors flow back
/// across the WIT boundary as `wasmtime::Error` (the `trappable`
/// imports knob makes that the natural shape); platform-level
/// errors (load, version-mismatch, supervisor) use this enum.
#[derive(Debug, thiserror::Error)]
pub enum PlatformError {
    #[error(
        "module ABI version mismatch: host wants {wanted_major}.x, module is {got_major}.{got_minor}"
    )]
    AbiMismatch {
        wanted_major: u32,
        got_major: u32,
        got_minor: u32,
    },

    #[error("module quarantined after {failures} consecutive failures")]
    Quarantined { failures: u32 },

    #[error("wasmtime: {0}")]
    Wasmtime(#[from] wasmtime::Error),

    #[error("resource table: {0}")]
    ResourceTable(#[from] wasmtime::component::ResourceTableError),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("storage: {0}")]
    Storage(#[from] crate::storage::StorageError),

    #[error("block decode: {0}")]
    Decode(String),

    /// A `recapture_module` call arrived for a module that already
    /// has an in-flight recapture. The endpoint maps this to
    /// HTTP 409. See `docs/design/RECAPTURE.md`.
    #[error("recapture already in flight for module `{0}`")]
    RecaptureInProgress(String),

    /// Per-companion `RecaptureReady` ACK didn't arrive within
    /// timeout, or a companion's outbound channel was closed
    /// mid-protocol. Bootstrap-refill is NOT fired in this case
    /// — the partial dApp-state cleanup would seed ghost rows.
    /// Maps to HTTP 504 from the admin endpoint.
    #[error("recapture coordination failed: {0}")]
    RecaptureCoordination(String),
}

pub type PlatformResult<T> = Result<T, PlatformError>;
