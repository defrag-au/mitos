//! Shared host-fn types: `DataPlaneFacade` trait + the v2 host
//! fns' supporting `state_kv` / `emit` modules. Originally the
//! v1 host-fn impls lived here too; v1 was retired (see
//! `mitos/docs/strategy/MITOS_PLATFORM_V2.md`) and the v2 impls
//! live under `host_fns_v2/`. This module is what's left of the
//! shared surface — both halves of the v2 platform see it for
//! the trait, the kv enum, and the emit channel types.

pub mod emit;
pub mod state_kv;

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

    /// Resolve a datum hash to its raw CBOR bytes via the
    /// underlying state's witness-datum index. Returns `None`
    /// when the hash isn't present (datum was inline-only and
    /// never landed in the witness-datum index, or the UTxO
    /// referencing it has been spent and its refcount dropped
    /// to zero).
    async fn datum_by_hash(
        &self,
        hash: &[u8; 32],
    ) -> mitos_data_plane::DataPlaneResult<Option<Vec<u8>>>;

    /// Bulk datum lookup paired with `read_utxos` for the
    /// bootstrap pattern. Returns parallel `Option<(hash,
    /// payload)>` per ref — caller-blind inline / hash
    /// resolution.
    async fn read_output_datums(
        &self,
        refs: &[mitos_data_plane::OutputRef],
    ) -> mitos_data_plane::DataPlaneResult<Vec<Option<(Vec<u8>, Vec<u8>)>>>;

    /// Datum hashes per ref, decoupled from byte resolution —
    /// `Some(hash)` even when the host couldn't resolve the
    /// payload (e.g. metadata-only datums). Caller pairs with
    /// `tx_metadata` + their own decoder when bytes weren't
    /// resolved.
    async fn read_output_hashes(
        &self,
        refs: &[mitos_data_plane::OutputRef],
    ) -> mitos_data_plane::DataPlaneResult<Vec<Option<Vec<u8>>>>;

    /// Auxiliary-data CBOR for a transaction by hash. `None`
    /// when the tx has no aux data or the archive doesn't
    /// know it.
    async fn tx_metadata(
        &self,
        tx_hash: &[u8; 32],
    ) -> mitos_data_plane::DataPlaneResult<Option<Vec<u8>>>;

    /// Full TX rollup. Default impl returns `None` so test
    /// fakes don't have to implement it; the blanket
    /// `impl<T: ChainDataPlane>` overrides for production.
    async fn read_tx(
        &self,
        _tx_hash: &pallas_primitives::Hash<32>,
    ) -> mitos_data_plane::DataPlaneResult<Option<mitos_data_plane::TxRecord>> {
        Ok(None)
    }
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

    async fn datum_by_hash(
        &self,
        hash: &[u8; 32],
    ) -> mitos_data_plane::DataPlaneResult<Option<Vec<u8>>> {
        let phash = pallas_primitives::Hash::<32>::from(*hash);
        match mitos_data_plane::ChainDataPlane::read_datum(self, &phash).await? {
            Some(td) => Ok(td.original_cbor),
            None => Ok(None),
        }
    }

    async fn read_output_datums(
        &self,
        refs: &[mitos_data_plane::OutputRef],
    ) -> mitos_data_plane::DataPlaneResult<Vec<Option<(Vec<u8>, Vec<u8>)>>> {
        let datums =
            mitos_data_plane::ChainDataPlane::read_output_datums(self, refs).await?;
        Ok(datums
            .into_iter()
            .map(|opt| {
                opt.and_then(|td| td.original_cbor.map(|payload| (td.hash.to_vec(), payload)))
            })
            .collect())
    }

    async fn read_output_hashes(
        &self,
        refs: &[mitos_data_plane::OutputRef],
    ) -> mitos_data_plane::DataPlaneResult<Vec<Option<Vec<u8>>>> {
        let hashes =
            mitos_data_plane::ChainDataPlane::output_datum_hashes(self, refs).await?;
        Ok(hashes.into_iter().map(|o| o.map(|h| h.to_vec())).collect())
    }

    async fn tx_metadata(
        &self,
        tx_hash: &[u8; 32],
    ) -> mitos_data_plane::DataPlaneResult<Option<Vec<u8>>> {
        let phash = pallas_primitives::Hash::<32>::from(*tx_hash);
        mitos_data_plane::ChainDataPlane::tx_metadata(self, &phash).await
    }

    async fn read_tx(
        &self,
        tx_hash: &pallas_primitives::Hash<32>,
    ) -> mitos_data_plane::DataPlaneResult<Option<mitos_data_plane::TxRecord>> {
        mitos_data_plane::ChainDataPlane::read_tx(self, tx_hash).await
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

// `DomainDataPlane` impls `ChainDataPlane` directly; the
// blanket `impl<T: ChainDataPlane> DataPlaneFacade for T` above
// gives us `DataPlaneFacade` for free. v1 host wires through
// the facade trait object; v2 host uses the concrete shape so
// the dispatch composer's generic `&P: ChainDataPlane` bound
// resolves without a vtable lookup.
//
// Each method constructs a fresh `LocalDataPlane` borrowed
// against `&self.domain`. `LocalDataPlane::new` is a struct
// literal — no allocation, no cloning.
#[async_trait::async_trait]
impl<D> mitos_data_plane::ChainDataPlane for DomainDataPlane<D>
where
    D: dolos_core::Domain + 'static,
{
    async fn read_utxo(
        &self,
        oref: &mitos_data_plane::OutputRef,
        decode: mitos_data_plane::DecodeLevel,
    ) -> mitos_data_plane::DataPlaneResult<Option<mitos_data_plane::TypedOutput>> {
        let plane = mitos_data_plane::LocalDataPlane::new(&self.domain);
        plane.read_utxo(oref, decode).await
    }

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

    async fn search_utxos(
        &self,
        predicate: &mitos_data_plane::UtxoPredicate,
        decode: mitos_data_plane::DecodeLevel,
        page: mitos_data_plane::PageRequest,
    ) -> mitos_data_plane::DataPlaneResult<
        mitos_data_plane::Page<(mitos_data_plane::OutputRef, mitos_data_plane::TypedOutput)>,
    > {
        let plane = mitos_data_plane::LocalDataPlane::new(&self.domain);
        plane.search_utxos(predicate, decode, page).await
    }

    async fn utxos_by_address(
        &self,
        address: &str,
    ) -> mitos_data_plane::DataPlaneResult<Vec<mitos_data_plane::OutputRef>> {
        let plane = mitos_data_plane::LocalDataPlane::new(&self.domain);
        mitos_data_plane::ChainDataPlane::utxos_by_address(&plane, address).await
    }

    async fn tx_metadata(
        &self,
        tx_hash: &pallas_primitives::Hash<32>,
    ) -> mitos_data_plane::DataPlaneResult<Option<Vec<u8>>> {
        let plane = mitos_data_plane::LocalDataPlane::new(&self.domain);
        mitos_data_plane::ChainDataPlane::tx_metadata(&plane, tx_hash).await
    }

    async fn read_tx(
        &self,
        tx_hash: &pallas_primitives::Hash<32>,
    ) -> mitos_data_plane::DataPlaneResult<Option<mitos_data_plane::TxRecord>> {
        let plane = mitos_data_plane::LocalDataPlane::new(&self.domain);
        mitos_data_plane::ChainDataPlane::read_tx(&plane, tx_hash).await
    }

    async fn read_block(
        &self,
        slot: u64,
    ) -> mitos_data_plane::DataPlaneResult<Option<Vec<u8>>> {
        let plane = mitos_data_plane::LocalDataPlane::new(&self.domain);
        mitos_data_plane::ChainDataPlane::read_block(&plane, slot).await
    }

    async fn slot_by_tx_hash(
        &self,
        tx_hash: &pallas_primitives::Hash<32>,
    ) -> mitos_data_plane::DataPlaneResult<Option<u64>> {
        let plane = mitos_data_plane::LocalDataPlane::new(&self.domain);
        mitos_data_plane::ChainDataPlane::slot_by_tx_hash(&plane, tx_hash).await
    }

    async fn read_datum(
        &self,
        hash: &pallas_primitives::Hash<32>,
    ) -> mitos_data_plane::DataPlaneResult<Option<mitos_data_plane::TypedDatum>> {
        let plane = mitos_data_plane::LocalDataPlane::new(&self.domain);
        plane.read_datum(hash).await
    }

    async fn read_script(
        &self,
        hash: &pallas_primitives::Hash<28>,
    ) -> mitos_data_plane::DataPlaneResult<Option<mitos_data_plane::TypedScript>> {
        let plane = mitos_data_plane::LocalDataPlane::new(&self.domain);
        plane.read_script(hash).await
    }

    async fn total_supply(
        &self,
        policy: &cardano_assets::PolicyId,
        asset_name_hex: Option<&str>,
    ) -> mitos_data_plane::DataPlaneResult<u64> {
        let plane = mitos_data_plane::LocalDataPlane::new(&self.domain);
        plane.total_supply(policy, asset_name_hex).await
    }

    async fn holder_count(
        &self,
        policy: &cardano_assets::PolicyId,
    ) -> mitos_data_plane::DataPlaneResult<u64> {
        let plane = mitos_data_plane::LocalDataPlane::new(&self.domain);
        plane.holder_count(policy).await
    }

    async fn tip(&self) -> mitos_data_plane::DataPlaneResult<mitos_data_plane::ChainTip> {
        let plane = mitos_data_plane::LocalDataPlane::new(&self.domain);
        plane.tip().await
    }

    async fn protocol_params(
        &self,
    ) -> mitos_data_plane::DataPlaneResult<mitos_data_plane::types::ProtocolParameters> {
        let plane = mitos_data_plane::LocalDataPlane::new(&self.domain);
        plane.protocol_params().await
    }
}
