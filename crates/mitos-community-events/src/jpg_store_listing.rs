//! Wire-format event types for the `jpg-store-listing` community
//! module — listing lifecycle on jpg.store (create, update,
//! unlisting).
//!
//! Sale events live in a sibling `jpg-store-sale` module; this
//! one stays narrowly scoped to listing book lifecycle.
//!
//! ## Coverage
//!
//! - `ListingCreate` — output produced at a jpg.store sale script
//!   with a listing datum (price + seller + payouts).
//! - `ListingUpdate` — listing UTxO consumed and a new one
//!   produced at the same script for the same asset, with a
//!   different payouts list (price changed).
//! - `Unlisting` — listing UTxO consumed with the cancel
//!   redeemer (constructor 1 = Cancel for jpg.store V1-V3).
//!   Asset returns to the seller's wallet.
//!
//! ## Datum shape (V2/V3)
//!
//! ```text
//! Listing = Constructor 0 [ payouts: List<Payout>, owner_pkh: Bytes ]
//! Payout  = Constructor 0 [ Address, Lovelace ]
//! ```
//!
//! Total list price is the sum of payout lovelace. Each payout
//! is bech32-derivable from its `(payment_credential, stake_credential)`
//! Plutus tuple but we surface raw `pkh` hex to keep the module
//! free of address-encoding logic — consumers reconstruct
//! addresses when they want them.

use serde::{Deserialize, Serialize};

// Re-export for existing consumers — the payout shape is shared
// marketplace vocabulary and lives in `crate::marketplace` now.
pub use crate::marketplace::ListingPayout;

/// jpg.store sale contract version. Determined by the script
/// address the listing UTxO lives at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JpgStoreContractVersion {
    V1,
    V2,
    V3,
    V4,
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
    /// Seller's payment-credential pkh, lowercase hex.
    /// Sourced from the datum's owner_pkh field.
    pub seller_pkh: String,
    /// Full payouts list from the datum — typically two entries
    /// (marketplace fee + seller take), occasionally more when
    /// royalty splits are encoded as additional payouts.
    pub payouts: Vec<ListingPayout>,
    /// Which jpg.store sale contract version this listing is at.
    pub contract_version: JpgStoreContractVersion,
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
    /// 64-char lowercase hex tx hash containing both the
    /// consumed prior listing and the produced replacement.
    pub tx_hash: String,
    /// Output index of the NEW (post-update) listing UTxO.
    pub output_index: u32,
    pub previous_price_lovelace: u64,
    pub new_price_lovelace: u64,
    pub seller_pkh: String,
    /// Payouts on the new listing.
    pub payouts: Vec<ListingPayout>,
    pub contract_version: JpgStoreContractVersion,
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
    /// Seller's payment-credential pkh (from the cancelled
    /// listing's datum).
    pub seller_pkh: String,
    pub contract_version: JpgStoreContractVersion,
    /// When this listing UTxO escrows multiple assets (a bundle sold
    /// together for one all-in price), the number of assets in it.
    /// `price_lovelace` is then the WHOLE-BUNDLE total, repeated on every
    /// member's event — consumers must partition bundle members out of
    /// single-asset floor/comparable math and count bundle sales once.
    /// `None` for ordinary single-asset listings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundle_size: Option<u32>,
}

/// Emit-channel discriminator. Consumers can subscribe to one
/// channel by routing on this tag at the companion-runtime
/// layer (see `mitos-companion::MitosChannel`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum JpgStoreListing {
    Create(ListingCreate),
    Update(ListingUpdate),
    Unlisting(Unlisting),
}

#[cfg(feature = "decode")]
pub fn decode_emit(channel: u32, payload: &[u8]) -> Option<String> {
    if channel != 0 {
        return None;
    }
    let event: JpgStoreListing = ciborium::de::from_reader(payload).ok()?;
    serde_json::to_string_pretty(&event).ok()
}
