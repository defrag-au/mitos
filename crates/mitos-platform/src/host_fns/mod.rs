//! Host-side impls of the WIT-imported interfaces.
//!
//! Each WIT interface has a sibling submodule:
//! - `chain_data` — proxies into `mitos-data-plane`
//! - `state_kv` — backs the per-module redb table
//! - `emit` — fans events out to the CF replication WS
//! - `logging` — funnels module logs into `tracing`
//! - `block_context` — exposes the per-block `ResolvedBlock`
//!   resource via lazy data-plane resolution
//!
//! The umbrella `HostState` here is the wasmtime `Store` data
//! type. It implements every WIT-imported `Host` trait, so the
//! `add_to_linker` call uses the canonical `HasSelf<HostState>`
//! idiom proven out in the spike:
//!
//! ```ignore
//! MitosModule::add_to_linker::<_, HasSelf<HostState>>(
//!     &mut linker, |s| s,
//! )?;
//! ```

pub mod block_context;
pub mod chain_data;
pub mod emit;
pub mod logging;
pub mod state_kv;

use std::sync::Arc;

use dolos_core::ChainPoint;
use wasmtime::component::ResourceTable;
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use crate::bindings::{BlockContextHost, TypesHost};

/// Per-instance host state. One per wasmtime `Store`.
///
/// V1: a single instance hosts a single module slot. V2 would
/// fan this out per-tenant; the shape is forward-compatible.
pub struct HostState {
    /// Long-lived resource table. Resources (today: `ResolvedBlock`;
    /// tomorrow: tx views, datum handles) push/delete through here.
    pub table: ResourceTable,

    /// Data plane handle — proxied through `chain_data` host fns.
    /// `Arc` so cloning into the host fn closures is cheap.
    pub(crate) data_plane: Arc<dyn DataPlaneFacade>,

    /// Per-module redb-backed KV. V1: one module → one table;
    /// keys are namespaced under the module ID.
    pub(crate) kv: state_kv::ModuleKv,

    /// Event sink — host fans out via the existing CF replication
    /// machinery.
    pub(crate) emitter: emit::EventSink,

    /// Module identifier — surfaces in logs + metrics.
    pub(crate) module_id: String,

    /// Cursor of the block currently being dispatched. Set by
    /// the driver immediately before each `call_handle_event`;
    /// read by `emit_event` so emissions can be tagged with
    /// the chain point at which they were produced. `None`
    /// outside an active dispatch (init, idle).
    pub(crate) current_cursor: Option<ChainPoint>,

    /// WASI context. Modules built against `wasm32-wasip2` pull
    /// in `wasi:io`, `wasi:cli`, etc. as imports via the std lib;
    /// `wasmtime_wasi::add_to_linker_async` populates the linker
    /// with default impls. We give every module a minimal ctx
    /// (no real fs/network surface) — the platform's intended
    /// I/O all flows through our typed host fns.
    pub(crate) wasi: WasiCtx,
    pub(crate) wasi_table: ResourceTable,
}

impl HostState {
    pub fn new(
        module_id: String,
        data_plane: Arc<dyn DataPlaneFacade>,
        kv: state_kv::ModuleKv,
        emitter: emit::EventSink,
    ) -> Self {
        Self {
            table: ResourceTable::new(),
            data_plane,
            kv,
            emitter,
            module_id,
            current_cursor: None,
            wasi: WasiCtxBuilder::new().build(),
            wasi_table: ResourceTable::new(),
        }
    }
}

// `wasmtime_wasi`'s `add_to_linker_async` requires the store
// data to expose a `WasiView`. We give it a separate
// `wasi_table` so WASI-owned resources can't collide with our
// platform-owned `ResolvedBlock` table.

impl WasiView for HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.wasi_table,
        }
    }
}

// Empty interface marker traits. Bindgen requires these even
// though `types`, `block-context`, and `interest` (each interface
// itself) have no free functions — `interest` defines only the
// `interest-op` variant; the `update-interest` function is a
// world-level export, not an interface-level free function.
impl TypesHost for HostState {}
impl BlockContextHost for HostState {}
impl crate::bindings::InterestHost for HostState {}

/// Trait the platform crate uses to talk to the data plane,
/// kept narrow on purpose. `mitos-data-plane::ChainDataPlane`
/// is the production impl (via the blanket impl below); tests
/// stub a smaller fake.
///
/// Method shape mirrors the underlying — pairs of
/// `(OutputRef, TypedOutput)` — so the conversion at the WIT
/// boundary stays trivial. Missing entries are silently omitted
/// (per the underlying contract).
#[async_trait::async_trait]
pub trait DataPlaneFacade: Send + Sync + 'static {
    async fn read_utxos(
        &self,
        refs: &[mitos_data_plane::OutputRef],
        decode: mitos_data_plane::DecodeLevel,
    ) -> mitos_data_plane::DataPlaneResult<
        Vec<(mitos_data_plane::OutputRef, mitos_data_plane::TypedOutput)>,
    >;

    async fn utxos_by_address(
        &self,
        address: &str,
    ) -> mitos_data_plane::DataPlaneResult<Vec<mitos_data_plane::OutputRef>>;
}

/// Blanket impl: any `ChainDataPlane` is a `DataPlaneFacade`.
/// Production wires `LocalDataPlane` here; the facade trait
/// exists primarily so tests can stub a smaller fake without
/// implementing the full ChainDataPlane surface.
#[async_trait::async_trait]
impl<T> DataPlaneFacade for T
where
    T: mitos_data_plane::ChainDataPlane + Send + Sync + 'static,
{
    async fn read_utxos(
        &self,
        refs: &[mitos_data_plane::OutputRef],
        decode: mitos_data_plane::DecodeLevel,
    ) -> mitos_data_plane::DataPlaneResult<
        Vec<(mitos_data_plane::OutputRef, mitos_data_plane::TypedOutput)>,
    > {
        mitos_data_plane::ChainDataPlane::read_utxos(self, refs, decode).await
    }

    async fn utxos_by_address(
        &self,
        address: &str,
    ) -> mitos_data_plane::DataPlaneResult<Vec<mitos_data_plane::OutputRef>> {
        mitos_data_plane::ChainDataPlane::utxos_by_address(self, address).await
    }
}

/// Owning adapter wrapping a `Domain` for the platform's
/// `Arc<dyn DataPlaneFacade>` slot. `LocalDataPlane` borrows the
/// domain for its lifetime; this adapter owns a clone (the
/// `Domain` trait already requires `Clone`) so the resulting
/// facade is `'static` and works behind `Arc<dyn …>`.
///
/// Constructs a fresh `LocalDataPlane` per call — cheap, since
/// `LocalDataPlane::new` is just a struct-literal over the
/// borrowed domain.
pub struct DomainDataPlane<D: dolos_core::Domain> {
    domain: D,
}

impl<D: dolos_core::Domain> DomainDataPlane<D> {
    pub fn new(domain: D) -> Self {
        Self { domain }
    }
}

#[async_trait::async_trait]
impl<D> DataPlaneFacade for DomainDataPlane<D>
where
    D: dolos_core::Domain + 'static,
{
    async fn read_utxos(
        &self,
        refs: &[mitos_data_plane::OutputRef],
        decode: mitos_data_plane::DecodeLevel,
    ) -> mitos_data_plane::DataPlaneResult<
        Vec<(mitos_data_plane::OutputRef, mitos_data_plane::TypedOutput)>,
    > {
        let plane = mitos_data_plane::LocalDataPlane::new(&self.domain);
        mitos_data_plane::ChainDataPlane::read_utxos(&plane, refs, decode).await
    }

    async fn utxos_by_address(
        &self,
        address: &str,
    ) -> mitos_data_plane::DataPlaneResult<Vec<mitos_data_plane::OutputRef>> {
        let plane = mitos_data_plane::LocalDataPlane::new(&self.domain);
        mitos_data_plane::ChainDataPlane::utxos_by_address(&plane, address).await
    }
}
