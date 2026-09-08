//! Pure redeemer + listing-datum decode primitives.
//!
//! jpg.store and Wayup share the listing-datum shape
//! (`Constr 0 [ List<Payout>, Bytes(owner_credential) ]`), so the datum decode
//! is one implementation for both venues.
//!
//! They do **not** share their redeemer constructors — that is [`Venue`]'s
//! whole reason to exist, and this header previously claimed the opposite. Two
//! things therefore differ per venue:
//!
//! - **Redeemer constructors are OPPOSITE.** See [`Venue`].
//! - **`owner_credential`**: jpg encodes the seller's **payment** pkh, Wayup
//!   the seller's **stake** credential — callers label
//!   [`DecodedListing::cred_hex`] accordingly.

use mitos_community_events::marketplace::ListingPayout;
use pallas_primitives::{BigInt, PlutusData};

/// Which venue's redeemer convention applies.
///
/// **The two venues are OPPOSITE, and this must never collapse into one shared
/// predicate.** It was one, on the stated assumption that jpg.store and Wayup
/// "share the buy/cancel redeemer constructors". They do not. Measured on chain
/// 2026-09-08 by tallying real spends and checking which satisfied the listing
/// datum's payouts:
///
/// | venue | buy | delist |
/// |---|---|---|
/// | jpg.store (V1–V3) | constructor **1** (`d87a…`) | constructor 0 (`d879…`) |
/// | Wayup | constructor **0** (`d879…`) | constructor 1 (`d87a…`) |
///
/// Evidence: Wayup tx `6e2ef8b9…` spends 25 listings with constructor 0 and
/// pays every datum payout; jpg txs `f917009c…` / `f940d7e7…` use constructor 1
/// and pay theirs, while jpg constructor-0 spends pay **none** of them (the
/// asset goes back to the seller — a delist).
///
/// The shared version was right for Wayup and inverted for jpg, so jpg sales
/// were never recorded and every jpg purchase was booked as an Unlisting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Venue {
    JpgStore,
    Wayup,
}

impl Venue {
    /// Every variant, so a caller adding a venue is forced to state its
    /// convention rather than inherit someone else's.
    pub const ALL: [Venue; 2] = [Venue::JpgStore, Venue::Wayup];

    /// CBOR constructor prefix this venue uses for a **buy**.
    fn buy_prefix(self) -> [u8; 2] {
        match self {
            Venue::JpgStore => [0xd8, 0x7a],
            Venue::Wayup => [0xd8, 0x79],
        }
    }

    /// Does `bytes` spend a listing as a purchase under this venue's contract?
    ///
    /// Prefix match, never full bytes: the redeemer sometimes carries a field
    /// (an input index) and a richer constructor must still read as a buy.
    pub fn is_buy_redeemer(self, bytes: &[u8]) -> bool {
        bytes.starts_with(&self.buy_prefix())
    }

    /// Does `bytes` spend a listing as a delist (cancel)? The other
    /// constructor — the two paths are exhaustive for these contracts.
    pub fn is_delist_redeemer(self, bytes: &[u8]) -> bool {
        bytes.len() >= 2
            && bytes.starts_with(&[0xd8])
            && !self.is_buy_redeemer(bytes)
            && (bytes[1] == 0x79 || bytes[1] == 0x7a)
    }
}

/// A decoded listing (ask) datum. `Default` is the empty listing (no payouts,
/// no owner credential) — the honest-about-unknowns value a create emits when
/// its hash-only datum can't be resolved.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DecodedListing {
    /// The agreed payouts; the listing's total price is their lovelace sum.
    pub payouts: Vec<ListingPayout>,
    /// Hex of the datum's owner credential (`fields[1]`). jpg → seller payment
    /// pkh; Wayup → seller stake credential. Empty when absent/misshaped.
    pub cred_hex: String,
}

/// Decode a listing (ask) datum. Handles both field orders seen in the wild:
/// wayup + jpg V2 + the jpg *sale* path are `Constr 0 [ payouts, owner ]`
/// (payouts-first); jpg V1/V3 *listing* datums are `Constr 0 [ owner, payouts ]`
/// (owner-first). We locate the payout `Array` and the owner `Bytes` by shape
/// rather than position. Returns `None` when neither field is a payout array
/// (e.g. jpg V4 listings, whose datum carries no payouts — those need ADA-flow
/// decode instead).
pub fn decode_listing_datum(cbor: &[u8]) -> Option<DecodedListing> {
    let pd: PlutusData = pallas_codec::minicbor::decode(cbor).ok()?;
    let outer = match pd {
        PlutusData::Constr(c) => c,
        _ => return None,
    };
    let fields: Vec<PlutusData> = outer.fields.into();
    if fields.len() < 2 {
        return None;
    }
    // Payouts = the Array field; owner credential = the Bytes field. Prefer
    // fields[0]=Array (payouts-first) to keep the historical wayup/jpg-sale
    // decode bit-identical; fall back to fields[1]=Array (jpg V1/V3 owner-first).
    let (payouts_field, cred_field) = match (&fields[0], &fields[1]) {
        (PlutusData::Array(_), _) => (&fields[0], &fields[1]),
        (_, PlutusData::Array(_)) => (&fields[1], &fields[0]),
        _ => return None,
    };
    let payouts = match payouts_field {
        PlutusData::Array(items) => items.iter().filter_map(decode_payout).collect::<Vec<_>>(),
        _ => return None,
    };
    let cred_hex = match cred_field {
        PlutusData::BoundedBytes(b) => hex::encode(&**b),
        _ => String::new(),
    };
    Some(DecodedListing { payouts, cred_hex })
}

/// Decode one payout entry: `Constr 0 [ Address, amount ]`. The `amount` is
/// either a bare `Lovelace` `BigInt` (wayup, jpg sale) or a `Value` map
/// (`{policy: {asset: amount}}`, jpg V1/V3 listings) — in the latter case we
/// take the ADA lovelace, which is the sole meaningful (largest) integer in an
/// ADA-only payout.
pub fn decode_payout(pd: &PlutusData) -> Option<ListingPayout> {
    let constr = match pd {
        PlutusData::Constr(c) => c,
        _ => return None,
    };
    let fields: Vec<PlutusData> = constr.fields.clone().into();
    if fields.len() < 2 {
        return None;
    }
    let (payment_pkh, stake_pkh) = match &fields[0] {
        PlutusData::Constr(addr) => {
            let addr_fields: Vec<PlutusData> = addr.fields.clone().into();
            let payment = decode_credential_bytes(addr_fields.first())?;
            let stake = addr_fields.get(1).and_then(decode_maybe_stake);
            (payment, stake)
        }
        _ => return None,
    };
    let lovelace = match &fields[1] {
        PlutusData::BigInt(i) => decode_bigint_u64(i)?,
        // A `Value` map (or its wrapper) — the ADA lovelace is the largest int.
        other => max_bigint(other)?,
    };
    Some(ListingPayout {
        payment_pkh: hex::encode(payment_pkh),
        stake_pkh: stake_pkh.map(hex::encode),
        lovelace,
    })
}

/// Largest positive `BigInt` anywhere in a PlutusData subtree — used to pull the
/// ADA lovelace out of a `Value`-shaped payout amount (whose only large integer
/// is the ADA quantity; policy/asset ints are 0 or small).
fn max_bigint(pd: &PlutusData) -> Option<u64> {
    let mut best: Option<u64> = None;
    fn walk(pd: &PlutusData, best: &mut Option<u64>) {
        match pd {
            PlutusData::BigInt(i) => {
                if let Some(v) = decode_bigint_u64(i)
                    && best.is_none_or(|b| v > b)
                {
                    *best = Some(v);
                }
            }
            PlutusData::Array(items) => {
                for x in items.iter() {
                    walk(x, best);
                }
            }
            PlutusData::Constr(c) => {
                let f: Vec<PlutusData> = c.fields.clone().into();
                for x in &f {
                    walk(x, best);
                }
            }
            PlutusData::Map(kv) => {
                for (k, v) in kv.iter() {
                    walk(k, best);
                    walk(v, best);
                }
            }
            _ => {}
        }
    }
    walk(pd, &mut best);
    best
}

/// Plutus address-credential extractor: `Constr 0 [ Bytes ]`.
fn decode_credential_bytes(pd: Option<&PlutusData>) -> Option<Vec<u8>> {
    match pd? {
        PlutusData::Constr(c) => {
            let fields: Vec<PlutusData> = c.fields.clone().into();
            match fields.first()? {
                PlutusData::BoundedBytes(b) => Some((**b).to_vec()),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Optional stake credential (`Just (StakingHash (KeyHash b))`).
fn decode_maybe_stake(pd: &PlutusData) -> Option<Vec<u8>> {
    let outer = match pd {
        PlutusData::Constr(c) => c,
        _ => return None,
    };
    if outer.any_constructor.unwrap_or(0) != 0 {
        return None;
    }
    let outer_fields: Vec<PlutusData> = outer.fields.clone().into();
    let mut cur = outer_fields.into_iter().next()?;
    for _ in 0..3 {
        match cur {
            PlutusData::Constr(c) => {
                let f: Vec<PlutusData> = c.fields.into();
                cur = f.into_iter().next()?;
            }
            PlutusData::BoundedBytes(b) => return Some((*b).to_vec()),
            _ => return None,
        }
    }
    None
}

/// Big-int → u64 (positive only).
fn decode_bigint_u64(i: &BigInt) -> Option<u64> {
    match i {
        BigInt::Int(n) => {
            let v = i128::from(*n);
            if v < 0 { None } else { u64::try_from(v).ok() }
        }
        BigInt::BigUInt(b) => {
            let bytes: &[u8] = b;
            if bytes.len() > 8 {
                return None;
            }
            let mut buf = [0u8; 8];
            buf[8 - bytes.len()..].copy_from_slice(bytes);
            Some(u64::from_be_bytes(buf))
        }
        BigInt::BigNInt(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONSTR_0: [u8; 3] = [0xd8, 0x79, 0x80];
    const CONSTR_1: [u8; 3] = [0xd8, 0x7a, 0x80];

    /// Wayup buys with constructor 0. Measured: tx `6e2ef8b9…` spends 25
    /// listings this way and pays every datum payout.
    #[test]
    fn wayup_buys_with_constructor_0() {
        assert!(Venue::Wayup.is_buy_redeemer(&CONSTR_0));
        assert!(Venue::Wayup.is_delist_redeemer(&CONSTR_1));
        // A redeemer carrying a field (an input index) is still a buy.
        assert!(Venue::Wayup.is_buy_redeemer(&[0xd8, 0x79, 0x9f, 0x09, 0xff]));
    }

    /// jpg.store buys with constructor **1** — the opposite of Wayup.
    /// Measured: `f917009c…` / `f940d7e7…` use constructor 1 and satisfy the
    /// datum's payouts; constructor-0 spends satisfy none and return the asset
    /// to the seller.
    #[test]
    fn jpg_buys_with_constructor_1() {
        assert!(Venue::JpgStore.is_buy_redeemer(&CONSTR_1));
        assert!(Venue::JpgStore.is_delist_redeemer(&CONSTR_0));
        assert!(Venue::JpgStore.is_buy_redeemer(&[0xd8, 0x7a, 0x9f, 0x00, 0xff]));
    }

    /// The guard that would have caught the original bug: no constructor may
    /// mean the same thing at both venues. If a future edit makes the two
    /// conventions agree, that is the bug, not a simplification.
    #[test]
    fn the_two_venues_disagree_on_every_constructor() {
        for redeemer in [CONSTR_0, CONSTR_1] {
            assert_ne!(
                Venue::JpgStore.is_buy_redeemer(&redeemer),
                Venue::Wayup.is_buy_redeemer(&redeemer),
                "jpg.store and Wayup use OPPOSITE redeemer constructors; a shared \
                 predicate is wrong for one of them"
            );
        }
    }

    #[test]
    fn nonsense_redeemers_are_neither() {
        for venue in Venue::ALL {
            assert!(!venue.is_buy_redeemer(&[]));
            assert!(!venue.is_delist_redeemer(&[]));
            // Not a constructor at all.
            assert!(!venue.is_delist_redeemer(&[0x00]));
        }
    }

    #[test]
    fn non_constr_datum_is_rejected() {
        // A bare integer (0x00) is not a listing datum.
        assert!(decode_listing_datum(&[0x00]).is_none());
        assert!(decode_listing_datum(&[]).is_none());
    }

    // A real jpg listing datum pulled from the market-ledger open book
    // (PuggyBlynderz3996). Payouts sum to 50 ADA (2.5 + 1.0 + 46.5).
    const JPG_V3_LISTING: &str = "D8799F581C1D2EE1E7B130575A4E0DDAF043320C63FD28DA2F13ED42FCCF0CD1799FD8799FD8799FD8799F581CE0682F6B1904600A351398A4FFD10A6D1E58556AFAE4A631B0EF748EFFD8799FD8799FD8799F581CAF02D326DE59084ACB0C1E4B4C6FF66EAB1D0CCCE97234E6D22A851FFFFFFFFFA140D8799F00A1401A002625A0FFFFD8799FD8799FD8799F581C70E60F3B5EA7153E0ACC7A803E4401D44B8ED1BAE1C7BAAAD1A62A72FFD8799FD8799FD8799F581C1E78AAE7C90CC36D624F7B3BB6D86B52696DC84E490F343EBA89005FFFFFFFFFA140D8799F00A1401A000F4240FFFFD8799FD8799FD8799F581C1D2EE1E7B130575A4E0DDAF043320C63FD28DA2F13ED42FCCF0CD179FFD8799FD8799FD8799F581CAF02D326DE59084ACB0C1E4B4C6FF66EAB1D0CCCE97234E6D22A851FFFFFFFFFA140D8799F00A1401A02C588A0FFFFFFFF";

    #[test]
    fn decodes_jpg_v3_owner_first_listing() {
        let cbor = hex::decode(JPG_V3_LISTING).unwrap();
        let d = decode_listing_datum(&cbor).expect("jpg V3 listing should decode");
        // owner-first: cred is the seller payment pkh (fields[0] bytes).
        assert_eq!(
            d.cred_hex,
            "1d2ee1e7b130575a4e0ddaf043320c63fd28da2f13ed42fccf0cd179"
        );
        assert_eq!(d.payouts.len(), 3);
        let total: u64 = d.payouts.iter().map(|p| p.lovelace).sum();
        assert_eq!(total, 50_000_000); // 2.5 + 1.0 + 46.5 ADA
    }
}
