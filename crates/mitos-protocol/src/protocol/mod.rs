//! Typed protocol-event taxonomy.
//!
//! See `../../docs/design/SUBSCRIPTION_MECHANICS.md` for the full
//! design rationale. Headlines:
//!
//! - Outer dimension within a domain is **kind**, not brand. Brand
//!   is carried inside each variant — a struct field for kinds whose
//!   shape is shared across brands (`Sale`, `ListingCreate`, etc.),
//!   an inner sum-type variant for kinds where brand semantics
//!   diverge (`OfferCancel`, where each brand's redeemer/script-ref
//!   shape differs).
//! - Each domain has a hand-written `XxxEventKind` enum mirroring
//!   the variants of its payload-carrying enum, plus an
//!   `impl From<&Xxx> for XxxEventKind` that the compiler keeps in
//!   sync via exhaustive match. We don't use
//!   `strum::EnumDiscriminants` because its auto-derived
//!   `Copy/Clone/PartialEq/Eq` on the generated kind enum collides
//!   with `enumset::EnumSetType`'s blanket derives, and strum 0.28
//!   has no opt-out for the defaults. The kind enums *are*
//!   `EnumSetType`, which makes them usable as keys in
//!   `enumset::EnumSet` for the selector machinery.
//! - Brand enums are flat `EnumSetType` C-style enums; the
//!   `Unknown` variant is just another member, selectable via
//!   `EnumSet::contains`. Unrecognised contracts surface their
//!   `script` string inside the divergent-shape variant's `Unknown`
//!   payload arm — never as a string in the brand enum itself.
//!
//! Phase 1 lands the type definitions only. The marketplace
//! indexer's translator from `TxClassification` -> `Marketplace`
//! and the trait surgery (`Scope = Interest`) come in Phase 2 / 4
//! respectively.

mod common;
mod dex;
mod lending;
mod marketplace;

pub use common::{Address, AssetRole, Lovelace, OutputRef, PlutusBytes, ProtocolEvent};
pub use dex::{Dex, DexBrand, DexEventKind, SwapPayload};
pub use lending::{BorrowPayload, Lending, LendingBrand, LendingEventKind};
pub use marketplace::{
    ListingPayload, Marketplace, MarketplaceBrand, MarketplaceEventKind, OfferAcceptPayload,
    OfferCancelPayload, OfferCreatePayload, OfferUpdatePayload, SalePayload, UnlistingPayload,
};

use serde::{Deserialize, Serialize};

/// Top-level domain tag. One arm per "bounded problem domain on
/// chain" — NFT marketplaces, DEXes, lending protocols, ...
///
/// A `ProtocolEvent` carries exactly one `Domain` value. Cross-
/// domain queries on the consumer side ("any cancel from
/// marketplace OR lending") match by walking the variants; they
/// don't require a flat global event-kind enum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Domain {
    Marketplace(Marketplace),
    Dex(Dex),
    Lending(Lending),
}
