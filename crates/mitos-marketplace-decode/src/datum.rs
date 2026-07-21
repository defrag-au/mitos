//! Pure redeemer + listing-datum decode primitives.
//!
//! jpg.store and Wayup share the listing-datum shape
//! (`Constr 0 [ List<Payout>, Bytes(owner_credential) ]`) and the buy/cancel
//! redeemer constructors, so this is one implementation for both venues. The
//! only interpretation difference is the `owner_credential`: jpg encodes the
//! seller's **payment** pkh, Wayup the seller's **stake** credential — callers
//! label [`DecodedListing::cred_hex`] accordingly.

use mitos_community_events::marketplace::ListingPayout;
use pallas_primitives::{BigInt, PlutusData};

/// jpg.store / Wayup Buy redeemer: constructor 0. On-wire the field varies per
/// spend (often an input index), so match on the `d879` constructor prefix
/// only, never the full bytes.
pub fn is_buy_redeemer(bytes: &[u8]) -> bool {
    bytes.starts_with(&[0xd8, 0x79])
}

/// Cancel / delist redeemer: constructor 1 (`d87a…`). The listing modules'
/// domain, not the sale modules' — exposed so callers can discriminate.
pub fn is_cancel_redeemer(bytes: &[u8]) -> bool {
    bytes.starts_with(&[0xd8, 0x7a])
}

/// A decoded listing (ask) datum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedListing {
    /// The agreed payouts; the listing's total price is their lovelace sum.
    pub payouts: Vec<ListingPayout>,
    /// Hex of the datum's owner credential (`fields[1]`). jpg → seller payment
    /// pkh; Wayup → seller stake credential. Empty when absent/misshaped.
    pub cred_hex: String,
}

/// Decode `Constr 0 [ List<Payout>, Bytes(owner_credential) ]`. Returns `None`
/// when the CBOR doesn't match (e.g. jpg V4 listings, whose datum carries no
/// payout list — those need ADA-flow decode instead).
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
    let payouts = match &fields[0] {
        PlutusData::Array(items) => items.iter().filter_map(decode_payout).collect::<Vec<_>>(),
        _ => return None,
    };
    let cred_hex = match &fields[1] {
        PlutusData::BoundedBytes(b) => hex::encode(&**b),
        _ => String::new(),
    };
    Some(DecodedListing { payouts, cred_hex })
}

/// Decode one payout entry: `Constr 0 [ Address, Lovelace ]`.
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
        _ => return None,
    };
    Some(ListingPayout {
        payment_pkh: hex::encode(payment_pkh),
        stake_pkh: stake_pkh.map(hex::encode),
        lovelace,
    })
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

    #[test]
    fn buy_redeemer_matches_constructor_0_prefix() {
        // d8799f00ff (indefinite Constr 0 [0]) and d8799f09ff both = Buy.
        assert!(is_buy_redeemer(&[0xd8, 0x79, 0x9f, 0x00, 0xff]));
        assert!(is_buy_redeemer(&[0xd8, 0x79, 0x9f, 0x09, 0xff]));
        assert!(!is_buy_redeemer(&[0xd8, 0x7a, 0x80])); // Cancel
        assert!(!is_buy_redeemer(&[]));
    }

    #[test]
    fn cancel_redeemer_matches_constructor_1_prefix() {
        assert!(is_cancel_redeemer(&[0xd8, 0x7a, 0x80]));
        assert!(!is_cancel_redeemer(&[0xd8, 0x79, 0x9f, 0x00, 0xff]));
    }

    #[test]
    fn non_constr_datum_is_rejected() {
        // A bare integer (0x00) is not a listing datum.
        assert!(decode_listing_datum(&[0x00]).is_none());
        assert!(decode_listing_datum(&[]).is_none());
    }
}
