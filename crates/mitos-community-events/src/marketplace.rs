//! Marketplace-agnostic wire vocabulary shared across the
//! `<brand>-store-{listing,sale,offer}` module families.
//!
//! Types here must stay venue-neutral: anything specific to one
//! marketplace's contracts (versions, redeemer quirks) belongs in
//! that marketplace's own module file.

use serde::{Deserialize, Serialize};

/// One recipient of a listing's proceeds, decoded from the
/// on-chain payout list (`Constr 0 [ Address, Lovelace ]` — the
/// shape jpg.store and Wayup share).
///
/// A listing's total price is the sum of its payout lovelace.
/// Consumers derive fee / royalty / seller-take by matching the
/// credentials against their own registries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListingPayout {
    /// Recipient payment-credential hash, lowercase hex (28 bytes).
    pub payment_pkh: String,
    /// Optional stake credential, lowercase hex (28 bytes). Some
    /// listings encode enterprise-only recipients.
    pub stake_pkh: Option<String>,
    /// Lovelace this recipient receives when the listing is bought.
    pub lovelace: u64,
}

/// One asset and how much of it. NFTs are `quantity: 1`; a fungible
/// consideration (an offer denominated in a project token) carries its
/// real amount.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetAmount {
    /// Policy id, lowercase hex (28 bytes).
    pub policy: String,
    /// Asset name, lowercase hex.
    pub name: String,
    pub quantity: u64,
}

/// What a bidder actually put up — which is **not always ADA**.
///
/// An offer's consideration is whatever the bidder locked in the offer UTxO.
/// Usually that is lovelace, and reading the UTxO's balance gives the bid
/// directly. But Wayup also carries **asset-denominated offers** — swap this
/// NFT for that one — where the UTxO holds the offered assets and only enough
/// lovelace to carry them.
///
/// Modelling that as a `u64` made those offers indistinguishable from very
/// cheap ADA bids, and they are not cheap, they are **not priced in ADA at
/// all**. Measured on mainnet 2026-09-18: 137 of 9,526 Wayup offer events
/// (1.4%) recorded a price of exactly 2.5 ADA — every one of them a
/// min-ADA figure carrying a swap, and every one of them feeding realized-price
/// medians as though someone had paid 2.5 ADA for an NFT.
///
/// So the point of this enum is that **a non-ADA consideration cannot be
/// mistaken for a number**. A consumer building a price series matches
/// [`Lovelace`](Self::Lovelace) and skips the rest, rather than filtering on a
/// magic threshold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AssetPrice {
    /// Paid in ADA. The only variant comparable with other prices.
    Lovelace(u64),
    /// Consideration includes assets — a swap, or assets plus ADA.
    ///
    /// **`lovelace` is the UTxO's whole balance and therefore includes
    /// whatever min-ADA the assets required.** The two are deliberately NOT
    /// separated: telling "50 ADA plus an NFT" from "an NFT carried by 2.5 ADA
    /// of min-ADA" means computing the UTxO's minimum, which depends on the
    /// serialised width of every quantity in it — a calculation this codebase
    /// has already got wrong once. Reporting the observed balance is honest;
    /// reporting a "swap" with the min-ADA quietly subtracted would be a guess
    /// wearing a type. If we ever compute min-ADA reliably, a `Swap` reading
    /// derives from this — it does not need to have been stored.
    Bundle {
        lovelace: u64,
        assets: Vec<AssetAmount>,
    },
    /// The event decoded but its consideration could not be determined.
    /// Distinct from zero, which would claim nothing was paid.
    Unknown,
}

impl AssetPrice {
    /// The bid in lovelace, and **only** when it is genuinely an ADA bid.
    ///
    /// The one accessor a price series should use: a [`Bundle`](Self::Bundle)
    /// answers `None` rather than handing back a min-ADA balance that would
    /// sink a median.
    pub fn lovelace(&self) -> Option<u64> {
        match self {
            Self::Lovelace(v) => Some(*v),
            Self::Bundle { .. } | Self::Unknown => None,
        }
    }

    /// Discriminator for a plain ADA price, for the many call sites that know
    /// statically they are one (a listing's payout sum, a sale) and would
    /// otherwise spell the string themselves and drift from [`Self::kind`].
    pub const LOVELACE_KIND: &'static str = "lovelace";

    /// Stable discriminator for storage and logs.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Lovelace(_) => Self::LOVELACE_KIND,
            Self::Bundle { .. } => "bundle",
            Self::Unknown => "unknown",
        }
    }
}
