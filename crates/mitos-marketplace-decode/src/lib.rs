//! `mitos-marketplace-decode` — the single source of truth for turning a
//! Cardano transaction into typed jpg.store / Wayup **sale** events.
//!
//! The `*-store-sale` community modules used to carry a private copy of this
//! decode each; this crate is the shared unit so the *live* modules and the
//! *historical backfill* in `cnft.dev-workers` run byte-identical logic.
//!
//! ## Shape
//!
//! Callers map their own transaction representation into the neutral
//! [`DecodeTx`] (resolving datum hashes to bytes on their side — mitos via the
//! `chain_data::datum_by_hash` host import, the worker via Maestro/Koios), then
//! call [`sales::decode_jpg_sales`] / [`sales::decode_wayup_sales`], which
//! return the exact `mitos_community_events` wire structs both sides already
//! speak.
//!
//! Pure and wasm-safe: only `pallas-codec` / `pallas-primitives` /
//! `pallas-addresses` — no host imports, no I/O.

pub mod datum;
pub mod listings;
pub mod offer_datum;
pub mod offer_lifecycle;
pub mod offers;
pub mod sales;

pub use datum::{DecodedListing, is_buy_redeemer, is_cancel_redeemer};
pub use listings::{decode_jpg_listings, decode_wayup_listings};
pub use offer_datum::{DecodedOffer, decode_jpg_offer_datum, decode_wayup_offer_datum};
pub use offer_lifecycle::{decode_jpg_offer_lifecycle, decode_wayup_offer_lifecycle};
pub use offers::{
    WayupOfferConfig, classify_jpg_offer_address, decode_jpg_offer_accepts,
    decode_wayup_offer_accepts,
};
pub use sales::{
    MatchedSale, WayupSaleConfig, classify_jpg_address, decode_jpg_sales, decode_wayup_sales,
};

/// A native asset (policy id + on-chain asset-name bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetId {
    pub policy: Vec<u8>,
    pub name: Vec<u8>,
}

/// A resolved transaction input (a UTxO being spent), carrying everything the
/// sale decode needs: the listing's address, its **resolved** inline/witness
/// datum CBOR (the caller resolves any hash-only datum first), the spend
/// redeemer, and the escrowed assets.
#[derive(Debug, Clone, Default)]
pub struct TxInput {
    pub address: String,
    pub lovelace: u64,
    pub assets: Vec<AssetId>,
    /// Resolved datum CBOR (`None` if unresolved / absent).
    pub datum: Option<Vec<u8>>,
    /// Spend redeemer CBOR (`None` for non-script inputs).
    pub redeemer: Option<Vec<u8>>,
    /// The UTxO reference being spent (origin tx hash + output index).
    /// Offer-accepts surface this as `prior_tx_hash`/`prior_output_index`;
    /// the sale decode ignores it (default empty/0).
    pub oref_tx_hash: Vec<u8>,
    pub oref_index: u32,
}

/// An output's datum, carrying whichever of the inline payload / datum hash the
/// chain provides. Only listing (and offer) *lifecycle* decode reads it — to
/// price a freshly produced ask/bid UTxO; sale/accept decode reads datums from
/// spent inputs, so it leaves [`TxOutput::datum`] `None`.
///
/// The split matters because listing lifecycle resolution is asymmetric: a jpg
/// *create* reads `payload` only and never resolves `hash` (the boot-stall
/// fix), while updates/delists (and Wayup creates) fall back to the caller's
/// hash resolver. Keeping both fields lets the decode apply that policy itself.
#[derive(Debug, Clone, Default)]
pub struct OutputDatum {
    /// Inline datum CBOR. Empty when the output commits only a hash.
    pub payload: Vec<u8>,
    /// 32-byte datum hash. Empty when the output carries an inline datum or none.
    pub hash: Vec<u8>,
}

/// A resolved transaction output.
#[derive(Debug, Clone, Default)]
pub struct TxOutput {
    pub address: String,
    pub lovelace: u64,
    pub assets: Vec<AssetId>,
    /// On-chain output index within the producing tx. Listing/offer lifecycle
    /// decode reports it as `output_index`; sale/accept decode ignores it
    /// (default 0).
    pub index: u32,
    /// The output's datum (see [`OutputDatum`]). Only listing/offer lifecycle
    /// decode reads it; sale/accept decode leaves it `None`.
    pub datum: Option<OutputDatum>,
}

/// A whole transaction, in the neutral shape the decoders consume.
#[derive(Debug, Clone, Default)]
pub struct DecodeTx {
    /// 32-byte tx hash.
    pub tx_hash: Vec<u8>,
    pub inputs: Vec<TxInput>,
    pub outputs: Vec<TxOutput>,
    /// 28-byte key hashes from the tx's `required_signers`. Only the Wayup
    /// offer-accept decode consults this (a bidder-signed spend is a cancel,
    /// not an accept); every other decode leaves it empty.
    pub required_signers: Vec<Vec<u8>>,
}
