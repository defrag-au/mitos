//! Marketplace domain — sales, listings, offers across NFT
//! marketplace protocols (jpg.store, wayup, dropspot, SpaceBudz bid
//! board, ...).
//!
//! Layout: `Marketplace` is the kind axis (one variant per event
//! kind). Each variant carries a payload — a struct for kinds whose
//! shape is shared across brands (Sale, Listing, Offer{Create,
//! Accept, Update}, Unlisting), an inner sum-type for kinds where
//! brand semantics genuinely differ (`OfferCancel`).
//!
//! See `docs/design/SUBSCRIPTION_MECHANICS.md` "Event payloads
//! (what mitos emits)" for the rationale on this asymmetry.

use cardano_assets::{AssetId, PolicyId};
use enumset::EnumSetType;
use pipeline_types::PricedAsset;
use serde::{Deserialize, Serialize};

use super::common::{Address, Lovelace, OutputRef, PlutusBytes};

/// Brand identity for a marketplace event. Flat C-style enum so
/// it's usable as an `EnumSet` key.
///
/// `Unknown` is for brand-script addresses the classifier hasn't
/// catalogued yet; the carrying script string lives inside the
/// payload (either a brand-tagged field or, for divergent-shape
/// kinds, an `Unknown { brand_script, raw }` payload arm).
#[derive(Debug, EnumSetType, Hash, Serialize, Deserialize)]
pub enum MarketplaceBrand {
    JpgStore,
    Wayup,
    Dropspot,
    SpaceBudzBidBoard,
    Unknown,
}

/// Marketplace event taxonomy.
///
/// We don't use `strum::EnumDiscriminants` here because the
/// auto-derived `Copy/Clone/PartialEq/Eq` on the generated kind
/// enum collides with `enumset::EnumSetType`'s blanket derives —
/// strum 0.28 has no opt-out for the defaults. Hand-rolling the
/// kind enum + `From<&Marketplace>` is ~15 lines and lets us own
/// the trait surface explicitly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Marketplace {
    Sale(SalePayload),
    ListingCreate(ListingPayload),
    ListingUpdate(ListingPayload),
    Unlisting(UnlistingPayload),
    OfferCreate(OfferCreatePayload),
    OfferAccept(OfferAcceptPayload),
    OfferUpdate(OfferUpdatePayload),
    OfferCancel(OfferCancelPayload),
}

/// Discriminant-only projection of `Marketplace`. Hand-mirrored from
/// the variant list above; `From<&Marketplace>` keeps them aligned
/// at compile time — adding a `Marketplace` variant without
/// extending this enum is a non-exhaustive match error.
#[derive(Debug, EnumSetType, Hash, Serialize, Deserialize)]
pub enum MarketplaceEventKind {
    Sale,
    ListingCreate,
    ListingUpdate,
    Unlisting,
    OfferCreate,
    OfferAccept,
    OfferUpdate,
    OfferCancel,
}

impl From<&Marketplace> for MarketplaceEventKind {
    fn from(m: &Marketplace) -> Self {
        match m {
            Marketplace::Sale(_) => MarketplaceEventKind::Sale,
            Marketplace::ListingCreate(_) => MarketplaceEventKind::ListingCreate,
            Marketplace::ListingUpdate(_) => MarketplaceEventKind::ListingUpdate,
            Marketplace::Unlisting(_) => MarketplaceEventKind::Unlisting,
            Marketplace::OfferCreate(_) => MarketplaceEventKind::OfferCreate,
            Marketplace::OfferAccept(_) => MarketplaceEventKind::OfferAccept,
            Marketplace::OfferUpdate(_) => MarketplaceEventKind::OfferUpdate,
            Marketplace::OfferCancel(_) => MarketplaceEventKind::OfferCancel,
        }
    }
}

impl Marketplace {
    /// Brand identity of this event. Mixed dispatch — struct-field
    /// read for shared-shape variants, inner match for divergent.
    pub fn brand(&self) -> MarketplaceBrand {
        match self {
            Marketplace::Sale(p) => p.brand,
            Marketplace::ListingCreate(p) | Marketplace::ListingUpdate(p) => p.brand,
            Marketplace::Unlisting(p) => p.brand,
            Marketplace::OfferCreate(p) => p.brand,
            Marketplace::OfferAccept(p) => p.brand,
            Marketplace::OfferUpdate(p) => p.brand,
            Marketplace::OfferCancel(c) => c.brand(),
        }
    }

    /// Event-kind discriminant — drops the payload, leaves the
    /// kind. Used by selectors as the second axis of
    /// `MarketplaceSelector::Filter`.
    pub fn kind(&self) -> MarketplaceEventKind {
        self.into()
    }
}

/// Shared shape across every marketplace's sale event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SalePayload {
    pub brand: MarketplaceBrand,
    pub asset: PricedAsset,
    pub seller: Address,
    pub buyer: Address,
    pub royalty_lovelace: Option<Lovelace>,
}

/// Listing create / update — bundle-aware (`assets` is plural to
/// cover multi-asset listings).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListingPayload {
    pub brand: MarketplaceBrand,
    pub assets: Vec<PricedAsset>,
    pub seller: Address,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnlistingPayload {
    pub brand: MarketplaceBrand,
    pub assets: Vec<AssetId>,
    pub seller: Address,
}

/// Offer creation. `policy_id` is at the top level so per-asset and
/// collection-wide offers share the shape; `asset_name_hex = None`
/// signals a collection-wide offer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OfferCreatePayload {
    pub brand: MarketplaceBrand,
    pub policy_id: PolicyId,
    pub asset_name_hex: Option<String>,
    pub price_lovelace: Lovelace,
    pub bidder: Address,
}

/// Offer accept — the asset that fulfilled the offer, plus the
/// price the bidder paid.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OfferAcceptPayload {
    pub brand: MarketplaceBrand,
    pub asset: AssetId,
    pub price_lovelace: Lovelace,
    pub bidder: Address,
    pub seller: Address,
}

/// Offer update — price change to an existing offer. `delta` is
/// signed because an update can lower a bid as well as raise it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OfferUpdatePayload {
    pub brand: MarketplaceBrand,
    pub policy_id: PolicyId,
    pub asset_name_hex: Option<String>,
    pub new_price_lovelace: Lovelace,
    pub delta_lovelace: i64,
    pub bidder: Address,
}

/// Offer cancellation — the load-bearing case for brand-as-variant.
/// Cancel semantics genuinely differ per brand: jpg.store needs a
/// `script_ref` for the contract reference input, wayup uses an
/// inline redeemer only, etc. Consumers that build cancel
/// transactions match on this inner enum to extract the brand-
/// specific data they need.
///
/// `redeemer` / `script_ref` are `Option`-typed because brand
/// identification is upstream of cancel-payload decoding: the
/// existing classifier surfaces the brand + policy + bidder for
/// cancels but doesn't extract the on-chain redeemer/script-ref
/// bytes. A future enhancement populates them for consumers that
/// build cancel TXs; until then `None` is the honest answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OfferCancelPayload {
    JpgStore {
        policy_id: PolicyId,
        asset_name_hex: Option<String>,
        bidder: Address,
        redeemer: Option<PlutusBytes>,
        script_ref: Option<OutputRef>,
    },
    Wayup {
        policy_id: PolicyId,
        asset_name_hex: Option<String>,
        bidder: Address,
        redeemer: Option<PlutusBytes>,
    },
    /// Unrecognised brand. The script string lets a consumer triage
    /// (catalogue the new brand, ignore, or attempt a generic cancel).
    Unknown {
        brand_script: String,
        policy_id: PolicyId,
        raw: Option<PlutusBytes>,
    },
}

impl OfferCancelPayload {
    pub fn brand(&self) -> MarketplaceBrand {
        match self {
            OfferCancelPayload::JpgStore { .. } => MarketplaceBrand::JpgStore,
            OfferCancelPayload::Wayup { .. } => MarketplaceBrand::Wayup,
            OfferCancelPayload::Unknown { .. } => MarketplaceBrand::Unknown,
        }
    }
}
