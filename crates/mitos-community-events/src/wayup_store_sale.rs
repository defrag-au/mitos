//! Wire-format event types for the `wayup-store-sale` community
//! module — completed fixed-price sales on the Wayup marketplace.
//!
//! Listing lifecycle events live in the sibling
//! `wayup-store-listing` module; offer (bid) lifecycle in
//! `wayup-store-offer` (whose `Accept` is the bid-side sale).
//!
//! ## Detection
//!
//! - `Consumed` event at the Wayup sale validator (payment
//!   credential match — the listing address staking part varies
//!   per seller) with a constructor-0 redeemer. On-wire the Buy
//!   redeemer is `Constr 0 [Int n]` where `n` varies per spend
//!   (an input index — e.g. `d8799f00ff`, `d8799f09ff`), so
//!   matching is on the constructor prefix (`d879…`), never the
//!   full bytes.
//! - The listing's datum gives the agreed payouts (seller take +
//!   fee/royalty); total price is their sum.
//! - The asset moves to a non-script output — the buyer.
//!
//! Cancel/delist spends use constructor 1 (`d87a80`) and are the
//! listing module's concern.

use serde::{Deserialize, Serialize};

pub use crate::marketplace::ListingPayout;
pub use crate::wayup_store_listing::WayupStoreContractVersion;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sale {
    /// 56-char lowercase hex policy id.
    pub policy: String,
    /// Lowercase hex of the on-chain asset name bytes.
    pub asset_name_hex: String,
    /// 64-char lowercase hex tx hash of the sale TX.
    pub tx_hash: String,
    /// Seller's STAKE credential, lowercase hex (28 bytes), from
    /// the consumed listing's datum owner field. (Wayup datums
    /// carry the seller's stake key hash, not a payment pkh.)
    pub seller_stake_pkh: String,
    /// Buyer's full bech32 address (the address that received the
    /// asset in this TX).
    pub buyer_address: String,
    /// The agreed payouts from the listing datum — total sale
    /// price is the sum of these lovelace amounts. Note Wayup's
    /// platform fee (2%, 1–10 ADA) is paid on top by the buyer and
    /// is not part of the datum payouts.
    pub payouts: Vec<ListingPayout>,
    /// Total sale price in lovelace (sum of payouts).
    pub price_lovelace: u64,
    pub contract_version: WayupStoreContractVersion,
    /// When this listing UTxO escrows multiple assets (a bundle sold
    /// together for one all-in price), the number of assets in it.
    /// `price_lovelace` is then the WHOLE-BUNDLE total, repeated on every
    /// member's event — consumers must partition bundle members out of
    /// single-asset floor/comparable math and count bundle sales once.
    /// `None` for ordinary single-asset listings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundle_size: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WayupStoreSale {
    Sale(Sale),
}

#[cfg(feature = "decode")]
pub fn decode_emit(channel: u32, payload: &[u8]) -> Option<String> {
    if channel != 0 {
        return None;
    }
    let event: WayupStoreSale = ciborium::de::from_reader(payload).ok()?;
    serde_json::to_string_pretty(&event).ok()
}
