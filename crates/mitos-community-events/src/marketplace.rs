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
/// # Why `Lovelace` is a STRUCT variant, not `Lovelace(u64)`
///
/// Serde's internally-tagged representation — `#[serde(tag = "kind")]` — cannot
/// serialize a newtype variant holding a primitive: there is nowhere to put the
/// `"kind"` key alongside a bare integer. It only works when the newtype wraps
/// a map. The failure is at RUNTIME, not compile time:
///
/// ```text
/// emit serialize failed: cannot serialize tagged newtype variant
///                        AssetPrice::Lovelace containing an integer
/// ```
///
/// It was `Lovelace(u64)`, and the consequence was that **every ADA-denominated
/// offer event silently failed to emit** — the module logged a warning and
/// dispatched nothing. Invisible for as long as the golden scenarios that
/// would have caught it were failing earlier, on a stale-ABI module artifact.
///
/// Changing the shape cost nothing precisely because the variant had never
/// successfully serialized: no stored event and no consumer could depend on it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AssetPrice {
    /// Paid in ADA. The only variant comparable with other prices.
    Lovelace { lovelace: u64 },
    /// Paid **in kind** — the consideration includes assets. A peer-to-peer
    /// trade, in user-facing terms; see [`Self::label`].
    ///
    /// Named for the consideration, NOT for the shape of the value. "Bundle"
    /// was the obvious word and is the wrong one: `market_events` already has a
    /// `bundle_size` column meaning *several assets sold together in one sale*,
    /// so a `price_kind = 'bundle'` beside it would put two unrelated senses of
    /// the word in the same row.
    ///
    /// **`lovelace` is the UTxO's whole balance and therefore includes
    /// whatever min-ADA the assets required.** The two are deliberately NOT
    /// separated: telling "50 ADA plus an NFT" from "an NFT carried by 2.5 ADA
    /// of min-ADA" means computing the UTxO's minimum, which depends on the
    /// serialised width of every quantity in it — a calculation this codebase
    /// has already got wrong once. Reporting the observed balance is honest;
    /// reporting a bare "swap" with the min-ADA quietly subtracted would be a
    /// guess wearing a type. If we ever compute min-ADA reliably, that reading
    /// derives from this — it does not need to have been stored.
    InKind {
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
            Self::Lovelace { lovelace } => Some(*lovelace),
            Self::InKind { .. } | Self::Unknown => None,
        }
    }

    /// The lovelace the offer UTxO actually holds, whatever the consideration
    /// was — **a ledger fact, not a price.**
    ///
    /// The distinction is the whole point of having two accessors. A consumer
    /// building a transaction against the offer (a cancel, a spend) MUST have
    /// the real balance and must not be handed `None` because the bid happened
    /// to be assets; a consumer computing a median MUST NOT be handed a
    /// min-ADA figure. Same number, opposite requirements — so
    /// [`lovelace`](Self::lovelace) answers "what was the bid" and this answers
    /// "what is in the UTxO".
    pub fn locked_lovelace(&self) -> u64 {
        match self {
            Self::Lovelace { lovelace } | Self::InKind { lovelace, .. } => *lovelace,
            Self::Unknown => 0,
        }
    }

    /// What to call this **to a reader**, as against [`Self::kind`]'s storage
    /// slug.
    ///
    /// The two are deliberately different words for the same thing. `in_kind`
    /// classifies the consideration precisely, which is what a column wants;
    /// it tells a person nothing. What actually happened is that two people
    /// swapped assets directly instead of one paying the other — a **P2P
    /// trade** — and that is real market activity worth naming as such rather
    /// than presenting as a sale with a missing price.
    ///
    /// Kept beside `kind()` so the pair cannot drift, which is the usual fate
    /// of a display string defined at whichever call site needed it first.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Lovelace { .. } => "ADA",
            Self::InKind { .. } => "P2P trade",
            Self::Unknown => "unknown",
        }
    }

    /// Discriminator for a plain ADA price, for the many call sites that know
    /// statically they are one (a listing's payout sum, a sale) and would
    /// otherwise spell the string themselves and drift from [`Self::kind`].
    pub const LOVELACE_KIND: &'static str = "lovelace";

    /// Stable discriminator for storage and logs.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Lovelace { .. } => Self::LOVELACE_KIND,
            Self::InKind { .. } => "in_kind",
            Self::Unknown => "unknown",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// EVERY variant must survive the encoding a module actually emits in.
    ///
    /// This exists because `Lovelace` did not. It was `Lovelace(u64)`, and
    /// serde's internally-tagged representation cannot serialize a newtype
    /// variant holding a primitive — so every ADA-denominated offer event
    /// failed to emit, at runtime, with nothing but a warning in the module
    /// log. A type that compiles and cannot be written is not caught by
    /// anything else here.
    ///
    /// Round-trip rather than just encode: a shape that writes but does not
    /// read back is the same bug one step later.
    #[test]
    fn every_price_variant_round_trips_through_cbor() {
        for price in [
            AssetPrice::Lovelace {
                lovelace: 45_000_000,
            },
            AssetPrice::InKind {
                lovelace: 2_500_000,
                assets: vec![AssetAmount {
                    policy: "aa".repeat(28),
                    name: "deadbeef".into(),
                    quantity: 3,
                }],
            },
            AssetPrice::Unknown,
        ] {
            let mut buf = Vec::new();
            ciborium::ser::into_writer(&price, &mut buf)
                .unwrap_or_else(|e| panic!("{} did not serialise: {e}", price.kind()));

            let back: AssetPrice = ciborium::de::from_reader(buf.as_slice())
                .unwrap_or_else(|e| panic!("{} did not deserialise: {e}", price.kind()));
            assert_eq!(back, price);
        }
    }

    /// The quantity is load-bearing and was pinned at 1 until the neutral
    /// decode shape carried amounts — so assert it survives rather than
    /// trusting that it is only ever an NFT.
    #[test]
    fn in_kind_keeps_its_quantities() {
        let price = AssetPrice::InKind {
            lovelace: 2_000_000,
            assets: vec![AssetAmount {
                policy: "bb".repeat(28),
                name: "01".into(),
                quantity: 250,
            }],
        };
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&price, &mut buf).unwrap();
        let back: AssetPrice = ciborium::de::from_reader(buf.as_slice()).unwrap();

        let AssetPrice::InKind { assets, .. } = back else {
            panic!("variant changed");
        };
        assert_eq!(assets[0].quantity, 250);
    }
}
