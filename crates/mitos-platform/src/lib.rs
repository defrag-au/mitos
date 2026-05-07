//! Mitos platform v1 — wasm-isolated module runtime.
//!
//! See `../../../docs/strategy/MITOS_PLATFORM_V1.md` for the
//! full design rationale and v1 scope. Headlines:
//!
//! - **One module slot, filesystem-loaded.** Multi-tenant
//!   hosting, HTTP control plane, OCI registry — all deferred.
//! - **WIT-defined ABI.** `wit/world.wit` is the contract;
//!   `wasmtime::component::bindgen!` generates host bindings,
//!   `wit-bindgen` generates guest stubs.
//! - **Resource limits from day one.** Fuel + epoch interruption
//!   + `ResourceLimiter` configured before any guest call.
//! - **Author-declared trap strategy.** Module exports
//!   `trap-policy()`; supervisor consults it on every trap.
//! - **Lazy consumed-input resolution.** `resolved-block`
//!   resource resolves on first access, memoises for block
//!   lifetime. Pure-output indexers pay zero.
//!
//! Crate layout mirrors the doc's "Platform layer breakdown":
//!
//! - `bindings` — wasmtime `bindgen!` invocation + re-exports
//! - `host_fns` — host-side impls of the WIT-imported
//!   interfaces (chain-data, state-kv, emit, logging,
//!   block-context)
//! - `resolved_block` — per-block resource backed by lazy
//!   data-plane resolution
//! - `registry` — load module artifact, version-check,
//!   instantiate
//! - `supervisor` — trap policy enforcement + bounded retry +
//!   quarantine
//! - `vendored` — files vendored from upstream (Apache-2.0;
//!   see `vendored/balius/NOTICE`)

pub mod admin;
pub mod bindings;
pub mod block_decode;
pub mod compaction;
pub mod companions;
pub mod dialer;
pub mod driver;
pub mod emissions;
pub mod follower;
pub mod host;
pub mod host_fns;
pub mod inspect;
pub mod lag_tolerant;
pub mod manifest;
pub mod registry;
pub mod resolved_block;
pub mod storage;
pub mod supervisor;
pub mod trap_context;
pub mod vendored;

pub use block_decode::{DecodedBlock, decode_block};
pub use driver::{ApplyOutcome, BlockEvent, Driver};
pub use follower::run_chain_follower;
pub use registry::{ModuleInstance, ModuleRegistry, ResourceBudget};
pub use resolved_block::{ResolvedBlock, TxView};
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
}

pub type PlatformResult<T> = Result<T, PlatformError>;
