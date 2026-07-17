//! Wire-format event types for the `wayup-store-offer` community
//! module — offer lifecycle on Wayup collection/asset-offer
//! contracts.
//!
//! Sibling of [`crate::jpg_store_offer`]: the decoded datum
//! resolves to the same field set (bidder, target policy /
//! asset-names, lovelace), so the event surface mirrors it. The
//! differences are all in the wasm module's recognition logic
//! (payment-credential watch, no-metadata datum recovery,
//! redeemer-agnostic accept/cancel discrimination), not on the
//! wire — see `mitos/docs/design/WAYUP_STORE_OFFER_MODULE.md`.
//!
//! The four variants mirror jpg.store's because consumers track
//! the same lifecycle:
//!
//! - **Create** — a new offer UTxO at the Wayup offer script.
//! - **Cancel** — bidder retrieves their locked lovelace; no
//!   asset transfer (bidder's owner key signs).
//! - **Accept** — a seller delivers an asset matching the
//!   offer's target; the bidder receives the asset.
//! - **Update** — bidder modifies the offer in place
//!   (consume + re-produce at the offer script in one TX).
//!
//! All variants surface `bidder_pkh` (the datum's owner key —
//! the bidder's stake credential for Wayup) so downstream "my
//! offers" views pivot on one identity across the lifecycle.

use serde::{Deserialize, Serialize};

/// Wayup offer-contract version. Only one contract is live
/// today; the enum is kept for forward-compat with a future
/// contract revision (mirrors jpg.store's V2/V3 split).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WayupStoreOfferVersion {
    V1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OfferCreate {
    /// 56-char lowercase hex of the bidder's owner key (datum
    /// field 0 — the bidder's stake credential for Wayup).
    pub bidder_pkh: String,
    /// 64-char lowercase hex of the create TX.
    pub tx_hash: String,
    /// Output index within `tx_hash` of the offer UTxO.
    pub output_index: u32,
    /// Lovelace locked at the offer script (the bid amount,
    /// including the standard min-utxo overhead).
    pub lovelace: u64,
    /// Raw datum CBOR — preserved for forensics + so the
    /// companion can build a cancel TX against the offer's
    /// actual on-chain bytes without a chain round-trip.
    #[serde(with = "serde_bytes")]
    pub datum_cbor: Vec<u8>,
    /// Policy this offer targets, when the datum specifies one.
    pub target_policy: Option<String>,
    /// Asset names (lowercase hex) the offer is constrained to.
    /// Empty for collection-wide offers (any asset under
    /// `target_policy` qualifies); non-empty for asset-specific
    /// offers.
    pub target_asset_names: Vec<String>,
    pub co_version: WayupStoreOfferVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OfferCancel {
    pub bidder_pkh: String,
    /// 64-char lowercase hex of the cancel TX.
    pub tx_hash: String,
    /// Identifies which prior `OfferCreate` is being cancelled.
    pub prior_tx_hash: String,
    pub prior_output_index: u32,
    pub target_policy: Option<String>,
    pub co_version: WayupStoreOfferVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OfferAccept {
    pub bidder_pkh: String,
    /// 64-char lowercase hex of the accept TX.
    pub tx_hash: String,
    pub prior_tx_hash: String,
    pub prior_output_index: u32,
    /// The asset the seller delivered to the bidder. For a
    /// collection-wide offer this is whichever asset the seller
    /// picked from the allowed policy; for an asset-specific
    /// offer it's the asset the offer was tied to.
    pub policy: String,
    pub asset_name_hex: String,
    /// Lovelace the bidder had locked in the offer (the bid).
    /// Read from the consumed offer UTxO — NOT inferred from
    /// outputs, since Wayup folds the seller's proceeds into
    /// change rather than a dedicated output.
    pub price_lovelace: u64,
    /// Bech32 address that received the delivered asset (the
    /// bidder's wallet). Wayup commingles the seller's proceeds
    /// into change, so a reliable *seller* address is not
    /// recoverable from the offer-script spend alone; this field
    /// is left empty. Retained for shape-parity with jpg.store's
    /// `OfferAccept`.
    pub seller_address: String,
    pub co_version: WayupStoreOfferVersion,
    /// True when the consumed offer was collection-wide (empty
    /// target-asset list — the seller picked which asset to deliver);
    /// false for an offer tied to a specific asset. Consumers use this
    /// to split bid-side sales: asset-specific accepts are deliberate
    /// purchases; CO fills generally aren't (e.g. not quest-worthy).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub collection_offer: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OfferUpdate {
    pub bidder_pkh: String,
    pub tx_hash: String,
    /// Identifies the prior `OfferCreate` being updated.
    pub prior_tx_hash: String,
    pub prior_output_index: u32,
    /// New offer UTxO produced in the same TX.
    pub new_output_index: u32,
    pub previous_lovelace: u64,
    pub new_lovelace: u64,
    /// Raw datum CBOR for the new offer UTxO (consumers need it
    /// to build a cancel TX against the updated offer's bytes).
    #[serde(with = "serde_bytes")]
    pub datum_cbor: Vec<u8>,
    pub target_policy: Option<String>,
    pub target_asset_names: Vec<String>,
    pub co_version: WayupStoreOfferVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WayupStoreOffer {
    Create(OfferCreate),
    Cancel(OfferCancel),
    Accept(OfferAccept),
    Update(OfferUpdate),
}

#[cfg(feature = "decode")]
pub fn decode_emit(channel: u32, payload: &[u8]) -> Option<String> {
    if channel != 0 {
        return None;
    }
    let event: WayupStoreOffer = ciborium::de::from_reader(payload).ok()?;
    serde_json::to_string_pretty(&event).ok()
}
