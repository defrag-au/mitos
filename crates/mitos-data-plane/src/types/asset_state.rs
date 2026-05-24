//! `AssetMintState` — per-asset mint/burn provenance projected from
//! dolos's always-on `AssetState` ledger (namespace `"assets"`).
//!
//! Unlike the UTxO / archive indexes, this is *state*: the pointers
//! survive the archive horizon even after the minting block body has
//! been pruned. The CIP-25 metadata facade uses `initial_tx` to find
//! the label-721-bearing mint transaction without a Maestro catalog
//! lookup — see `docs/design/COLLECTION_MODULES.md`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetMintState {
    /// Earliest mint transaction for this asset. For CIP-25 this is
    /// the tx whose auxiliary data carries the label-721 metadata;
    /// `tx_metadata(initial_tx)` then resolves the payload (dolos
    /// archive when retained, Maestro fallback when pruned).
    pub initial_tx: Option<[u8; 32]>,
    /// Slot of `initial_tx`.
    pub initial_slot: Option<u64>,
    /// Number of mint transactions observed for this asset.
    pub mint_tx_count: u64,
    /// Transaction the visitor flagged as carrying this asset's
    /// metadata — the CIP-25 mint (label 721) or CIP-68 reference
    /// token (label 100) tx. May be `None` for pre-bootstrap CIP-25
    /// assets; prefer `initial_tx` for the CIP-25 facade.
    pub metadata_tx: Option<[u8; 32]>,
    /// Net supply = total minted − total burned (`0` ⇒ fully burned).
    pub quantity: i128,
}
