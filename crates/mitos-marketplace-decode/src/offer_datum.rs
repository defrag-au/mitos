//! Pure collection-offer (bid) datum decode primitives.
//!
//! jpg.store and Wayup share the offer datum shape
//! (`Constr 0 [ Bytes(bidder_owner_key), Array<Payout> ]`) but differ in how the
//! *target* payout is located and in what they need from it:
//!
//! - **jpg.store** takes the **last** payout and reads its policy + asset names;
//!   it does not need the NFT recipient (accept matching is by first delivered
//!   asset).
//! - **Wayup** scans **all** payouts (`find_map`) for the one carrying a non-ADA
//!   policy (the buyer payout — fee/royalty payouts carry the empty ADA policy),
//!   and additionally decodes that payout's address to the recipient payment
//!   credential (accept matching requires delivery to *that* wallet).
//!
//! This is the single source both the live mitos offer modules and the worker
//! firehose decode with. `bidder_pkh` is the datum's owner key — jpg encodes the
//! bidder's **payment** pkh, Wayup the bidder's **stake** credential.

use pallas_primitives::{Constr, PlutusData};

/// A decoded collection-offer datum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedOffer {
    /// Hex of the datum's owner key (`fields[0]`). jpg → bidder payment pkh;
    /// Wayup → bidder stake credential.
    pub bidder_pkh: String,
    /// The policy the offer targets, when the datum's payout yields one.
    pub target_policy: Option<String>,
    /// Asset names (lowercase hex) the offer is constrained to. Empty ⇒
    /// collection-wide (any asset under `target_policy` qualifies).
    pub target_asset_names: Vec<String>,
    /// Payment credential of the NFT-payout recipient (the bidder's wallet).
    /// Wayup only — jpg does not encode it and leaves this `None`.
    pub target_recipient: Option<[u8; 28]>,
}

fn is_constructor_zero(c: &Constr<PlutusData>) -> bool {
    c.tag == 121 && c.any_constructor.is_none()
}

/// Common outer decode: `Constr 0 [ Bytes(bidder,28), Array<Payout> ]`. Returns
/// `(bidder_pkh_hex, payouts)`.
fn decode_offer_outer(cbor: &[u8]) -> Option<(String, Vec<PlutusData>)> {
    let pd: PlutusData = pallas_codec::minicbor::decode(cbor).ok()?;
    let PlutusData::Constr(constr) = pd else {
        return None;
    };
    if !is_constructor_zero(&constr) {
        return None;
    }
    let fields: Vec<PlutusData> = constr.fields.into();
    if fields.len() != 2 {
        return None;
    }
    let mut iter = fields.into_iter();
    let bidder_pkh = match iter.next()? {
        PlutusData::BoundedBytes(b) if b.len() == 28 => hex::encode(&*b),
        _ => return None,
    };
    let payouts = match iter.next()? {
        PlutusData::Array(arr) => arr.to_vec(),
        _ => Vec::new(),
    };
    Some((bidder_pkh, payouts))
}

/// Asset names from a value entry `Constr0[flag, Map<name, qty>]`. Empty map ⇒
/// collection-wide; populated ⇒ the listed asset-specific names.
fn extract_asset_names(v: &PlutusData) -> Vec<String> {
    let PlutusData::Constr(constr) = v else {
        return Vec::new();
    };
    let inner_map = constr.fields.iter().find_map(|f| match f {
        PlutusData::Map(m) => Some(m),
        _ => None,
    });
    let Some(pairs) = inner_map else {
        return Vec::new();
    };
    pairs
        .iter()
        .filter_map(|(k, _)| match k {
            PlutusData::BoundedBytes(b) => Some(hex::encode(&**b)),
            _ => None,
        })
        .collect()
}

/// Decode a jpg.store CO datum. Target policy/assets come from the **last**
/// payout (jpg's convention).
pub fn decode_jpg_offer_datum(cbor: &[u8]) -> Option<DecodedOffer> {
    let (bidder_pkh, payouts) = decode_offer_outer(cbor)?;
    let (target_policy, target_asset_names) = payouts
        .last()
        .map(jpg_extract_target)
        .unwrap_or_default();
    Some(DecodedOffer {
        bidder_pkh,
        target_policy,
        target_asset_names,
        target_recipient: None,
    })
}

/// jpg target extraction: a payout `Constr0[Address, Value]` where
/// `Value = Map<PolicyId, Constr0[flag, Map<AssetName, qty>]>`; take the first
/// 28-byte policy key + its asset names.
fn jpg_extract_target(payout: &PlutusData) -> (Option<String>, Vec<String>) {
    let PlutusData::Constr(constr) = payout else {
        return Default::default();
    };
    if !is_constructor_zero(constr) || constr.fields.len() != 2 {
        return Default::default();
    }
    let PlutusData::Map(pairs) = &constr.fields[1] else {
        return Default::default();
    };
    for (k, v) in pairs.iter() {
        let PlutusData::BoundedBytes(policy_bytes) = k else {
            continue;
        };
        if policy_bytes.len() != 28 {
            continue;
        }
        return (Some(hex::encode(&**policy_bytes)), extract_asset_names(v));
    }
    Default::default()
}

/// Decode a Wayup offer datum. Target payout is found by scanning **all**
/// payouts for the one carrying a non-ADA policy; the payout's address yields
/// the recipient payment credential.
pub fn decode_wayup_offer_datum(cbor: &[u8]) -> Option<DecodedOffer> {
    let (bidder_pkh, payouts) = decode_offer_outer(cbor)?;
    let (target_policy, target_asset_names, target_recipient) = payouts
        .iter()
        .find_map(wayup_extract_payout_target)
        .map(|(p, n, r)| (Some(p), n, Some(r)))
        .unwrap_or((None, Vec::new(), None));
    Some(DecodedOffer {
        bidder_pkh,
        target_policy,
        target_asset_names,
        target_recipient,
    })
}

/// Wayup target extraction: for the buyer payout (value map carries a 28-byte
/// non-ADA policy key) returns `(policy_hex, asset_names, recipient_cred)`;
/// `None` for ADA fee/royalty payouts.
fn wayup_extract_payout_target(payout: &PlutusData) -> Option<(String, Vec<String>, [u8; 28])> {
    let PlutusData::Constr(constr) = payout else {
        return None;
    };
    if !is_constructor_zero(constr) || constr.fields.len() != 2 {
        return None;
    }
    let PlutusData::Map(pairs) = &constr.fields[1] else {
        return None;
    };
    let (policy_hex, names) = pairs.iter().find_map(|(k, v)| match k {
        PlutusData::BoundedBytes(b) if b.len() == 28 => Some((hex::encode(&**b), extract_asset_names(v))),
        _ => None,
    })?;
    let recipient = extract_address_payment_cred(&constr.fields[0])?;
    Some((policy_hex, names, recipient))
}

/// Payment credential (28 bytes) from a Plutus `Address`
/// (`Constr[Credential, ...]`, `Credential` = `Constr0[pkh]` / `Constr1[hash]`).
fn extract_address_payment_cred(addr: &PlutusData) -> Option<[u8; 28]> {
    let PlutusData::Constr(c) = addr else {
        return None;
    };
    let PlutusData::Constr(cred) = c.fields.first()? else {
        return None;
    };
    let bytes = cred.fields.iter().find_map(|f| match f {
        PlutusData::BoundedBytes(b) if b.len() == 28 => Some(b),
        _ => None,
    })?;
    let mut out = [0u8; 28];
    out.copy_from_slice(bytes);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_constr_and_wrong_arity() {
        assert!(decode_jpg_offer_datum(&[0x00]).is_none());
        assert!(decode_wayup_offer_datum(&[]).is_none());
    }
}
