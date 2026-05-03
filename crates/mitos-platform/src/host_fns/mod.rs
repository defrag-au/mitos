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

use wasmtime::component::ResourceTable;

use crate::bindings::{BlockContextHost, TypesHost};

/// Per-instance host state. One per wasmtime `Store`.
///
/// V1: a single instance hosts a single module slot. V2 would
/// fan this out per-tenant; the shape is forward-compatible.
pub struct HostState {
    /// Long-lived resource table. Resources (today: `ResolvedBlock`;
    /// tomorrow: tx views, datum handles) push/delete through here.
    pub(crate) table: ResourceTable,

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
        }
    }
}

// Empty interface marker traits. Bindgen requires these even
// though `types` and `block-context` (the interface itself)
// have no free functions.
impl TypesHost for HostState {}
impl BlockContextHost for HostState {}

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
}
