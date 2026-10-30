//! The one concrete `ChainDataPlane` when the plane is chosen at runtime.
//!
//! The v2 host is generic over `P: ChainDataPlane` so the dispatch composer can
//! call it without a vtable. A runtime choice between dolos and the live book
//! would give the host two different `P`s, so the choice is made once, here, and
//! every method delegates to the selected plane. The cost is one match per call;
//! the plane's work is a row lookup or a mmap read, not the match.
//!
//! The tail is unchanged either way: `bundle.rs` still follows dolos. This enum
//! selects the *data plane*, which is what L1 cuts over.

use async_trait::async_trait;
use cardano_assets::PolicyId;
use mitos_data_plane::types::{ProtocolParameters, StakeCred, TxRecord};
use mitos_data_plane::{
    AssetMintState, BookDataPlane, ChainDataPlane, ChainTip, DataPlaneResult, DecodeLevel,
    OutputRef, Page, PageRequest, TypedDatum, TypedOutput, TypedScript, UtxoPredicate,
};

use crate::host_fns::DomainDataPlane;

/// The selected data plane. `Dolos` is the existing domain-backed adapter;
/// `Book` is a published live-book artifact.
///
/// The book is boxed because it is the large variant (it holds mmapped files)
/// while the dolos adapter is a handle, and the enum is built once and shared
/// behind an `Arc` — one allocation at startup buys a small value everywhere it
/// moves.
pub enum SelectedPlane<D: dolos_core::Domain> {
    Dolos(DomainDataPlane<D>),
    Book(Box<BookDataPlane>),
}

impl<D: dolos_core::Domain> SelectedPlane<D> {
    /// The name a startup log line can print without matching on the generic.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Dolos(_) => "dolos",
            Self::Book(_) => "book",
        }
    }
}

#[async_trait]
impl<D> ChainDataPlane for SelectedPlane<D>
where
    D: dolos_core::Domain + Send + Sync + 'static,
{
    async fn read_utxo(
        &self,
        oref: &OutputRef,
        decode: DecodeLevel,
    ) -> DataPlaneResult<Option<TypedOutput>> {
        match self {
            Self::Dolos(plane) => plane.read_utxo(oref, decode).await,
            Self::Book(plane) => plane.read_utxo(oref, decode).await,
        }
    }

    async fn read_utxos(
        &self,
        orefs: &[OutputRef],
        decode: DecodeLevel,
    ) -> DataPlaneResult<Vec<(OutputRef, TypedOutput)>> {
        match self {
            Self::Dolos(plane) => plane.read_utxos(orefs, decode).await,
            Self::Book(plane) => plane.read_utxos(orefs, decode).await,
        }
    }

    async fn search_utxos(
        &self,
        predicate: &UtxoPredicate,
        decode: DecodeLevel,
        page: PageRequest,
    ) -> DataPlaneResult<Page<(OutputRef, TypedOutput)>> {
        match self {
            Self::Dolos(plane) => plane.search_utxos(predicate, decode, page).await,
            Self::Book(plane) => plane.search_utxos(predicate, decode, page).await,
        }
    }

    async fn utxos_by_address(&self, address: &str) -> DataPlaneResult<Vec<OutputRef>> {
        match self {
            Self::Dolos(plane) => plane.utxos_by_address(address).await,
            Self::Book(plane) => plane.utxos_by_address(address).await,
        }
    }

    async fn utxos_by_policy(&self, policy: &[u8]) -> DataPlaneResult<Vec<OutputRef>> {
        match self {
            Self::Dolos(plane) => plane.utxos_by_policy(policy).await,
            Self::Book(plane) => plane.utxos_by_policy(policy).await,
        }
    }

    async fn utxos_by_payment_cred(&self, cred: &[u8]) -> DataPlaneResult<Vec<OutputRef>> {
        match self {
            Self::Dolos(plane) => plane.utxos_by_payment_cred(cred).await,
            Self::Book(plane) => plane.utxos_by_payment_cred(cred).await,
        }
    }

    async fn resolve_stake_for_payment_pkh(
        &self,
        pkh: &[u8],
    ) -> DataPlaneResult<Option<StakeCred>> {
        match self {
            Self::Dolos(plane) => plane.resolve_stake_for_payment_pkh(pkh).await,
            Self::Book(plane) => plane.resolve_stake_for_payment_pkh(pkh).await,
        }
    }

    async fn read_output_datums(
        &self,
        refs: &[OutputRef],
    ) -> DataPlaneResult<Vec<Option<TypedDatum>>> {
        match self {
            Self::Dolos(plane) => plane.read_output_datums(refs).await,
            Self::Book(plane) => plane.read_output_datums(refs).await,
        }
    }

    async fn output_datum_hashes(
        &self,
        refs: &[OutputRef],
    ) -> DataPlaneResult<Vec<Option<pallas_primitives::Hash<32>>>> {
        match self {
            Self::Dolos(plane) => plane.output_datum_hashes(refs).await,
            Self::Book(plane) => plane.output_datum_hashes(refs).await,
        }
    }

    async fn tx_metadata(
        &self,
        tx_hash: &pallas_primitives::Hash<32>,
    ) -> DataPlaneResult<Option<Vec<u8>>> {
        match self {
            Self::Dolos(plane) => plane.tx_metadata(tx_hash).await,
            Self::Book(plane) => plane.tx_metadata(tx_hash).await,
        }
    }

    async fn read_tx(
        &self,
        tx_hash: &pallas_primitives::Hash<32>,
    ) -> DataPlaneResult<Option<TxRecord>> {
        match self {
            Self::Dolos(plane) => plane.read_tx(tx_hash).await,
            Self::Book(plane) => plane.read_tx(tx_hash).await,
        }
    }

    async fn read_block(&self, slot: u64) -> DataPlaneResult<Option<Vec<u8>>> {
        match self {
            Self::Dolos(plane) => plane.read_block(slot).await,
            Self::Book(plane) => plane.read_block(slot).await,
        }
    }

    async fn slot_by_tx_hash(
        &self,
        tx_hash: &pallas_primitives::Hash<32>,
    ) -> DataPlaneResult<Option<u64>> {
        match self {
            Self::Dolos(plane) => plane.slot_by_tx_hash(tx_hash).await,
            Self::Book(plane) => plane.slot_by_tx_hash(tx_hash).await,
        }
    }

    async fn read_datum(
        &self,
        hash: &pallas_primitives::Hash<32>,
    ) -> DataPlaneResult<Option<TypedDatum>> {
        match self {
            Self::Dolos(plane) => plane.read_datum(hash).await,
            Self::Book(plane) => plane.read_datum(hash).await,
        }
    }

    async fn read_script(
        &self,
        hash: &pallas_primitives::Hash<28>,
    ) -> DataPlaneResult<Option<TypedScript>> {
        match self {
            Self::Dolos(plane) => plane.read_script(hash).await,
            Self::Book(plane) => plane.read_script(hash).await,
        }
    }

    async fn total_supply(
        &self,
        policy: &PolicyId,
        asset_name_hex: Option<&str>,
    ) -> DataPlaneResult<u64> {
        match self {
            Self::Dolos(plane) => plane.total_supply(policy, asset_name_hex).await,
            Self::Book(plane) => plane.total_supply(policy, asset_name_hex).await,
        }
    }

    async fn asset_state(
        &self,
        policy: &[u8],
        asset_name: &[u8],
    ) -> DataPlaneResult<Option<AssetMintState>> {
        match self {
            Self::Dolos(plane) => plane.asset_state(policy, asset_name).await,
            Self::Book(plane) => plane.asset_state(policy, asset_name).await,
        }
    }

    async fn holder_count(&self, policy: &PolicyId) -> DataPlaneResult<u64> {
        match self {
            Self::Dolos(plane) => plane.holder_count(policy).await,
            Self::Book(plane) => plane.holder_count(policy).await,
        }
    }

    async fn tip(&self) -> DataPlaneResult<ChainTip> {
        match self {
            Self::Dolos(plane) => plane.tip().await,
            Self::Book(plane) => plane.tip().await,
        }
    }

    async fn protocol_params(&self) -> DataPlaneResult<ProtocolParameters> {
        match self {
            Self::Dolos(plane) => plane.protocol_params().await,
            Self::Book(plane) => plane.protocol_params().await,
        }
    }

    async fn archive_horizon_slot(&self) -> DataPlaneResult<Option<u64>> {
        match self {
            Self::Dolos(plane) => plane.archive_horizon_slot().await,
            Self::Book(plane) => plane.archive_horizon_slot().await,
        }
    }
}
