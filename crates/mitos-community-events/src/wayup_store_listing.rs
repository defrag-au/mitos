//! Wire-format event types for the `wayup-store-listing` community
//! module — listing lifecycle on the Wayup marketplace (create,
//! update, unlisting).
//!
//! Sale events live in a sibling `wayup-store-sale` module; offer
//! (bid) lifecycle in `wayup-store-offer`. This one stays narrowly
//! scoped to the listing book.
//!
//! ## Coverage
//!
//! - `ListingCreate` — output produced at the Wayup sale validator
//!   (payment credential `a76f0fb8…`; the staking part varies per
//!   seller, so the module watches by payment credential) with a
//!   listing datum.
//! - `ListingUpdate` — listing UTxO consumed with the cancel
//!   redeemer and a new one produced at the validator for the same
//!   asset in the same TX (price edit; observed as multi-asset
//!   batches on-chain).
//! - `Unlisting` — listing UTxO consumed with the cancel redeemer
//!   (constructor 1, `d87a80`) and the asset returned to the
//!   seller's wallet.
//!
//! ## Datum shape
//!
//! Identical structure to jpg.store listings:
//!
//! ```text
//! Listing = Constructor 0 [ payouts: List<Payout>, owner: Bytes(28) ]
//! Payout  = Constructor 0 [ Address, Lovelace ]
//! ```
//!
//! Total list price is the sum of payout lovelace. **The `owner`
//! field carries the seller's STAKE credential** (unlike jpg.store,
//! where it is a payment pkh) — the same convention as Wayup's
//! offer datum `bidder` field. Consumers matching sellers to
//! wallets should compare against stake credentials.

use serde::{Deserialize, Serialize};

pub use crate::marketplace::ListingPayout;

/// Wayup sale/listing contract version. One validator today.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WayupStoreContractVersion {
    V1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListingCreate {
    /// 56-char lowercase hex policy id.
    pub policy: String,
    /// Lowercase hex of the on-chain asset name bytes.
    pub asset_name_hex: String,
    /// 64-char lowercase hex tx hash that produced this listing.
    pub tx_hash: String,
    /// Output index within `tx_hash` of the listing UTxO.
    pub output_index: u32,
    /// Total list price = sum of all payout lovelace amounts.
    pub price_lovelace: u64,
    /// Seller's STAKE credential, lowercase hex (28 bytes), from
    /// the datum's owner field.
    pub seller_stake_pkh: String,
    /// Full payouts list from the datum — seller take plus fee /
    /// royalty recipients.
    pub payouts: Vec<ListingPayout>,
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
pub struct ListingUpdate {
    pub policy: String,
    pub asset_name_hex: String,
    /// 64-char lowercase hex tx hash containing both the consumed
    /// prior listing and the produced replacement.
    pub tx_hash: String,
    /// Output index of the NEW (post-update) listing UTxO.
    pub output_index: u32,
    pub previous_price_lovelace: u64,
    pub new_price_lovelace: u64,
    /// Seller's STAKE credential, lowercase hex (28 bytes).
    pub seller_stake_pkh: String,
    /// Payouts on the new listing.
    pub payouts: Vec<ListingPayout>,
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
pub struct Unlisting {
    pub policy: String,
    pub asset_name_hex: String,
    /// 64-char lowercase hex tx hash where the listing was
    /// consumed with the cancel redeemer.
    pub tx_hash: String,
    /// Seller's STAKE credential (from the cancelled listing's
    /// datum owner field).
    pub seller_stake_pkh: String,
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

/// Emit-channel discriminator (channel 0).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WayupStoreListing {
    Create(ListingCreate),
    Update(ListingUpdate),
    Unlisting(Unlisting),
}

#[cfg(feature = "decode")]
pub fn decode_emit(channel: u32, payload: &[u8]) -> Option<String> {
    if channel != 0 {
        return None;
    }
    let event: WayupStoreListing = ciborium::de::from_reader(payload).ok()?;
    serde_json::to_string_pretty(&event).ok()
}
