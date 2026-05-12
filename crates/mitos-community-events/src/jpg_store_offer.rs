//! Wire-format event types for the `jpg-store-offer` community
//! module — offer lifecycle on jpg.store CO contracts.
//!
//! Successor to the retired `mitos_community_events::jpg_co`
//! 2-variant `Created` / `Spent` surface, with a richer
//! 4-variant event set:
//!
//! - `Create` (was: `Created`) — same field set, finer name
//! - `Cancel` / `Accept` / `Update` (was: collapsed under `Spent`)
//!   — split based on redeemer + produced-output flow
//!
//! Consumers tracking offer-state need to distinguish these
//! three because the chain-level effects differ:
//!
//! - **Cancel**: bidder retrieves their locked lovelace; no
//!   asset transfer.
//! - **Accept**: someone (seller) provides the asset matching
//!   the offer's target; bidder receives the asset, seller
//!   receives the lovelace.
//! - **Update**: bidder modifies the offer in place
//!   (consume + re-produce at the same offer script address,
//!   typically a price change). The chain sees a fresh offer
//!   UTxO; consumers update their "outstanding offer" record
//!   rather than removing it.
//!
//! All variants surface `bidder_pkh` so downstream "my offers"
//! views can pivot on the same identity used at create time.

use serde::{Deserialize, Serialize};

/// jpg.store CO contract version. V2 and V3 share the same
/// underlying script; they differ only in address-encoding (V2
/// uses one staking credential, V3 uses another).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JpgStoreOfferVersion {
    V2,
    V3,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OfferCreate {
    /// 56-char lowercase hex pkh of the bidder (payment cred
    /// extracted from datum field 0).
    pub bidder_pkh: String,
    /// 64-char lowercase hex of the create TX.
    pub tx_hash: String,
    /// Output index within `tx_hash` of the offer UTxO.
    pub output_index: u32,
    /// Lovelace locked at the offer script (the offer amount,
    /// including the standard 2 ADA min-utxo overhead).
    pub lovelace: u64,
    /// Raw datum CBOR — preserved for forensics + so the
    /// companion can decode richer fields when consumers need
    /// them later. Also used at consume time to identify which
    /// offer the consume refers to.
    #[serde(with = "serde_bytes")]
    pub datum_cbor: Vec<u8>,
    /// Policy this offer targets, when the datum specifies one.
    /// `None` when the datum carries an allow-list instead of a
    /// single policy (rare).
    pub target_policy: Option<String>,
    /// Asset names (lowercase hex) the offer is constrained to.
    /// Empty for collection-wide offers (any asset under
    /// `target_policy` qualifies); non-empty for asset-specific
    /// offers (e.g. a single Handle).
    pub target_asset_names: Vec<String>,
    pub co_version: JpgStoreOfferVersion,
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
    pub co_version: JpgStoreOfferVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OfferAccept {
    pub bidder_pkh: String,
    /// 64-char lowercase hex of the accept TX.
    pub tx_hash: String,
    pub prior_tx_hash: String,
    pub prior_output_index: u32,
    /// The asset the seller delivered. For a collection-wide
    /// offer this is whichever asset the seller picked from the
    /// allowed policy; for an asset-specific offer it's the
    /// asset the offer was tied to.
    pub policy: String,
    pub asset_name_hex: String,
    /// Lovelace the bidder paid (= the offer UTxO's locked
    /// lovelace minus any change retained by the bidder, which
    /// the chain settles by the script's payout rules).
    pub price_lovelace: u64,
    /// Bech32 of the address that received the lovelace. The
    /// seller's identity; payment-cred extraction is left to
    /// consumers (avoids pulling bech32 → cred decoders into
    /// the wasm module).
    pub seller_address: String,
    pub co_version: JpgStoreOfferVersion,
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
    /// Raw datum CBOR for the new offer UTxO. Same field as
    /// `OfferCreate.datum_cbor` — consumers need it so they can
    /// build a cancel TX against the updated offer's actual
    /// on-chain bytes (which differ from the prior offer's
    /// because the lovelace amount is encoded in the datum).
    /// Without this, downstream cancel-TX construction for an
    /// updated offer would fail script validation.
    #[serde(with = "serde_bytes")]
    pub datum_cbor: Vec<u8>,
    pub target_policy: Option<String>,
    /// Asset names (lowercase hex) the new offer is constrained
    /// to. Preserved from the prior offer (updates change price,
    /// not target). Empty for collection-wide offers.
    pub target_asset_names: Vec<String>,
    pub co_version: JpgStoreOfferVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum JpgStoreOffer {
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
    let event: JpgStoreOffer = ciborium::de::from_reader(payload).ok()?;
    serde_json::to_string_pretty(&event).ok()
}
