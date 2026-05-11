//! Wire-format event types for the `jpg-store-sale` community
//! module — completed sales on jpg.store sale contracts.
//!
//! Listing lifecycle events (create/update/unlisting) live in
//! the sibling `jpg-store-listing` module. Sales are a distinct
//! consumer concern (volume tracking, royalty audit, sale ledger)
//! and ship on their own subscription so consumers who only care
//! about one concern don't have to filter the other out.
//!
//! ## Detection
//!
//! - `Consumed` event at a jpg.store sale script address with
//!   redeemer = constructor 0 (Buy/Accept; on-wire CBOR
//!   `d8799f00ff` for the typical V1/V2/V3 shape, but any
//!   constructor-0 PlutusData on a script consume at a jpg
//!   address qualifies).
//! - The listing's datum (resolved via `chain_data::datum_by_hash`
//!   from the witness set the sale TX itself reveals) gives us
//!   the agreed payouts: jpg fee + optional royalty + seller take.
//! - The asset moves to a non-script address — the buyer. We
//!   identify the buyer by finding the Produced output that
//!   received the asset.

use serde::{Deserialize, Serialize};

pub use crate::jpg_store_listing::{JpgStoreContractVersion, ListingPayout};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sale {
    /// 56-char lowercase hex policy id.
    pub policy: String,
    /// Lowercase hex of the on-chain asset name bytes.
    pub asset_name_hex: String,
    /// 64-char lowercase hex tx hash of the sale TX.
    pub tx_hash: String,
    /// Seller's payment-credential pkh (from the consumed
    /// listing's datum owner_pkh field).
    pub seller_pkh: String,
    /// Buyer's full bech32 address (the address that received
    /// the asset in this TX).
    pub buyer_address: String,
    /// The agreed payouts from the listing datum — total sale
    /// price is the sum of these lovelace amounts. Consumers
    /// derive fee/royalty/seller-take by matching pkhs against
    /// their own known fee/royalty registries.
    pub payouts: Vec<ListingPayout>,
    /// Total sale price in lovelace (sum of payouts).
    pub price_lovelace: u64,
    pub contract_version: JpgStoreContractVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum JpgStoreSale {
    Sale(Sale),
}

#[cfg(feature = "decode")]
pub fn decode_emit(channel: u32, payload: &[u8]) -> Option<String> {
    if channel != 0 {
        return None;
    }
    let event: JpgStoreSale = ciborium::de::from_reader(payload).ok()?;
    serde_json::to_string_pretty(&event).ok()
}
