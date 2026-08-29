//! Offer-accept assembly: match a consumed collection-offer (bid) to the output
//! that delivered its target asset, and project to the venue wire events.
//!
//! Only the **Accept** branch of the offer lifecycle lives here — the realized
//! trade, the part the historical firehose and the live modules must decode
//! identically. Create / Cancel / Update correlation is stateful (in-batch
//! pairing) and stays with the live modules; this crate sees one [`DecodeTx`] at
//! a time and reports the accepts in it.
//!
//! Accept-vs-cancel discrimination is venue-specific:
//! - **jpg.store**: the redeemer carries the signal — cancel is `d87980`
//!   (constructor 0), accept is `d87a80` (constructor 1). An accept delivers the
//!   target asset to a non-offer output (the seller's output holding the NFT).
//! - **Wayup**: the redeemer is `d87a80` for both, so we discriminate on flow —
//!   an accept delivers a target-policy asset to the bidder's own wallet
//!   (`target_recipient`) AND the bidder is not among the tx's required signers
//!   (only a cancel needs the bidder's signature to reclaim the locked lovelace).
//!
//! The consumed offer's locked lovelace is the bid price (`price_lovelace`) —
//! read from the input, never inferred from outputs.
//!
//! ## Batched fills
//!
//! A tx may spend several offers at once — notably one seller filling two of the
//! *same* bidder's collection offers, where both offers carry the same target
//! policy and (on Wayup) the same recipient credential, so the two are
//! indistinguishable by matching rules alone. Each delivery is therefore
//! **claimed**: an output/asset already reported for an earlier offer in the same
//! tx is skipped by the next. Without that, first-match-wins reports the first
//! delivery once per offer and loses every other asset in the tx.

use std::collections::HashSet;

use mitos_community_events::jpg_store_offer::{
    JpgStoreOfferVersion, OfferAccept as JpgOfferAccept,
};
use mitos_community_events::wayup_store_offer::{
    OfferAccept as WayupOfferAccept, WayupStoreOfferVersion,
};

use crate::offer_datum::{DecodedOffer, decode_jpg_offer_datum, decode_wayup_offer_datum};
use crate::sales::address_payment_cred;
use crate::{AssetId, DecodeTx, TxOutput};

// ============================================================
// jpg.store
// ============================================================

/// jpg.store CO (collection-offer) V2 script address. V2 and V3 share the
/// underlying script; only V2 is live (the manifest's `v3_address` is empty), so
/// V3 is intentionally absent here until a real V3 address exists.
const JPG_OFFER_V2_ADDR: &str = "addr1xxgx3far7qygq0k6epa0zcvcvrevmn0ypsnfsue94nsn3tfvjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8eks2utwdd";

/// Classify a jpg.store offer-contract address to its version, or `None`.
pub fn classify_jpg_offer_address(addr: &str) -> Option<JpgStoreOfferVersion> {
    match addr {
        JPG_OFFER_V2_ADDR => Some(JpgStoreOfferVersion::V2),
        _ => None,
    }
}

/// jpg.store offer **cancel** redeemer: constructor 0, empty (`d87980`). Note
/// this is inverted vs the sale contract (where constructor 0 = Buy).
fn is_jpg_offer_cancel(redeemer: &[u8]) -> bool {
    redeemer == [0xd8, 0x79, 0x80]
}

/// Decode every completed jpg.store offer-accept in a transaction.
///
/// A consumed offer UTxO at a jpg CO address, spent with a non-cancel redeemer,
/// whose datum yields a target policy, is an accept when a non-offer output
/// carries a matching asset. Collection-wide offers (no asset names) accept any
/// asset under the target policy; asset-specific offers require the delivered
/// name to be in the allow-list. Accepts whose asset can't be identified are
/// dropped (a partial accept carries no pricing signal). Deliveries are claimed
/// across the tx, so a batched fill reports one asset per offer.
///
/// Returns the bare [`JpgOfferAccept`] wire structs (this decode never produces
/// the other lifecycle variants — Create/Cancel/Update stay with the live
/// module's stateful buffer). Callers that need the enum wrap with
/// `JpgStoreOffer::Accept`.
pub fn decode_jpg_offer_accepts(tx: &DecodeTx) -> Vec<JpgOfferAccept> {
    let mut out = Vec::new();
    let mut claimed = ClaimedDeliveries::default();
    for input in &tx.inputs {
        let Some(version) = classify_jpg_offer_address(&input.address) else {
            continue;
        };
        let Some(redeemer) = input.redeemer.as_ref() else {
            continue;
        };
        if is_jpg_offer_cancel(redeemer) {
            continue;
        }
        let Some(datum) = input.datum.as_ref() else {
            continue;
        };
        let Some(decoded) = decode_jpg_offer_datum(datum) else {
            continue;
        };
        // jpg matches the first non-offer output bearing the target policy —
        // the seller's output holding the NFT (jpg does not credential-match the
        // recipient the way Wayup does).
        let Some((policy, asset_name_hex, seller_address)) =
            jpg_find_delivered(&decoded, non_offer_outputs(tx), &mut claimed)
        else {
            continue;
        };
        out.push(JpgOfferAccept {
            bidder_pkh: decoded.bidder_pkh,
            tx_hash: hex::encode(&tx.tx_hash),
            prior_tx_hash: hex::encode(&input.oref_tx_hash),
            prior_output_index: input.oref_index,
            policy,
            asset_name_hex,
            price_lovelace: input.lovelace,
            seller_address,
            co_version: version,
            collection_offer: decoded.target_asset_names.is_empty(),
        });
    }
    out
}

/// First unclaimed non-offer output delivering a target-policy asset →
/// `(policy_hex, asset_name_hex, seller_address)`.
fn jpg_find_delivered<'a>(
    decoded: &DecodedOffer,
    outputs: impl Iterator<Item = &'a TxOutput>,
    claimed: &mut ClaimedDeliveries,
) -> Option<(String, String, String)> {
    let target_policy = decoded.target_policy.as_deref()?;
    let target_policy_bytes = hex::decode(target_policy).ok()?;
    let target_asset_set = asset_name_set(decoded);
    for out in outputs {
        for asset in &out.assets {
            if asset.policy != target_policy_bytes {
                continue;
            }
            if let Some(ref set) = target_asset_set
                && !set.iter().any(|n| n == &asset.name)
            {
                continue;
            }
            if !claim(claimed, out, asset) {
                continue;
            }
            return Some((
                target_policy.to_owned(),
                hex::encode(&asset.name),
                out.address.clone(),
            ));
        }
    }
    None
}

// ============================================================
// Wayup
// ============================================================

/// Static configuration for the Wayup offer decode — the offer contract's
/// payment credential (offer UTxOs share it, staking part varying per bidder).
#[derive(Debug, Clone, Default)]
pub struct WayupOfferConfig {
    offer_payment_cred: Option<[u8; 28]>,
}

impl WayupOfferConfig {
    /// Build from the 56-char hex offer payment credential. An empty/invalid
    /// value classifies nothing (safe default).
    pub fn from_hex(cred_hex: &str) -> Self {
        Self {
            offer_payment_cred: parse_cred(cred_hex),
        }
    }

    /// Whether an address sits at the Wayup offer payment credential.
    pub fn is_offer_address(&self, addr: &str) -> bool {
        match self.offer_payment_cred {
            Some(cred) => address_payment_cred(addr) == Some(cred),
            None => false,
        }
    }
}

/// Decode every completed Wayup offer-accept in a transaction.
///
/// Wayup's redeemer is `d87a80` for both accept and cancel, so an accept is a
/// consumed offer UTxO whose target-policy asset is delivered to the bidder's
/// own wallet (`target_recipient`) AND whose bidder is not among the tx's
/// required signers. Recipient-credential matching (not just policy) is what
/// excludes the seller's change and, in a batched tx, a listing of another asset
/// from the same collection. Deliveries are claimed across the tx, so a seller
/// filling two of one bidder's offers reports one asset per offer rather than
/// the first asset twice. Returns the bare [`WayupOfferAccept`] structs (see
/// [`decode_jpg_offer_accepts`] re the enum).
pub fn decode_wayup_offer_accepts(tx: &DecodeTx, cfg: &WayupOfferConfig) -> Vec<WayupOfferAccept> {
    let mut out = Vec::new();
    let mut claimed = ClaimedDeliveries::default();
    for input in &tx.inputs {
        if !cfg.is_offer_address(&input.address) {
            continue;
        }
        let Some(datum) = input.datum.as_ref() else {
            continue;
        };
        let Some(decoded) = decode_wayup_offer_datum(datum) else {
            continue;
        };
        // A bidder-signed spend is a cancel (the bidder reclaims their lovelace),
        // never an accept.
        if bidder_in_signers(&decoded.bidder_pkh, &tx.required_signers) {
            continue;
        }
        let Some((policy, asset_name_hex)) =
            wayup_find_delivered(&decoded, non_offer_outputs(tx), &mut claimed)
        else {
            continue;
        };
        out.push(WayupOfferAccept {
            bidder_pkh: decoded.bidder_pkh,
            tx_hash: hex::encode(&tx.tx_hash),
            prior_tx_hash: hex::encode(&input.oref_tx_hash),
            prior_output_index: input.oref_index,
            policy,
            asset_name_hex,
            price_lovelace: input.lovelace,
            // Wayup commingles seller proceeds into change — no reliable seller.
            seller_address: String::new(),
            co_version: WayupStoreOfferVersion::V1,
            collection_offer: decoded.target_asset_names.is_empty(),
        });
    }
    out
}

/// First unclaimed output delivering the target-policy asset to the bidder's own
/// wallet → `(policy_hex, asset_name_hex)`.
fn wayup_find_delivered<'a>(
    decoded: &DecodedOffer,
    outputs: impl Iterator<Item = &'a TxOutput>,
    claimed: &mut ClaimedDeliveries,
) -> Option<(String, String)> {
    let target_policy = decoded.target_policy.as_deref()?;
    let target_policy_bytes = hex::decode(target_policy).ok()?;
    let target_recipient = decoded.target_recipient?;
    let target_asset_set = asset_name_set(decoded);
    for out in outputs {
        if address_payment_cred(&out.address) != Some(target_recipient) {
            continue;
        }
        for asset in &out.assets {
            if asset.policy != target_policy_bytes {
                continue;
            }
            if let Some(ref set) = target_asset_set
                && !set.iter().any(|n| n == &asset.name)
            {
                continue;
            }
            if !claim(claimed, out, asset) {
                continue;
            }
            return Some((target_policy.to_owned(), hex::encode(&asset.name)));
        }
    }
    None
}

/// Is the bidder's owner key (hex) among the tx's required signers?
fn bidder_in_signers(bidder_pkh: &str, signers: &[Vec<u8>]) -> bool {
    let Ok(bidder) = hex::decode(bidder_pkh) else {
        return false;
    };
    signers.iter().any(|s| s.as_slice() == bidder.as_slice())
}

// ============================================================
// shared helpers
// ============================================================

/// Deliveries already reported by an earlier offer in the same tx, keyed by
/// `(output index, asset name)`. One physical asset settles exactly one offer,
/// so a second offer matching the same output/asset must keep looking.
type ClaimedDeliveries = HashSet<(u32, Vec<u8>)>;

/// Claim a delivery for the offer currently being decoded. `false` when an
/// earlier offer in this tx already took it.
fn claim(claimed: &mut ClaimedDeliveries, out: &TxOutput, asset: &AssetId) -> bool {
    claimed.insert((out.index, asset.name.clone()))
}

/// Tx outputs that are NOT at an offer address of *either* venue — the
/// candidate accept-delivery outputs. (An accept's delivery never lands back at
/// an offer script; excluding both venues keeps the two decoders symmetric.)
fn non_offer_outputs(tx: &DecodeTx) -> impl Iterator<Item = &TxOutput> {
    tx.outputs
        .iter()
        .filter(|o| classify_jpg_offer_address(&o.address).is_none())
}

/// The offer's asset-name allow-list as decoded bytes, or `None` for a
/// collection-wide offer (empty list).
fn asset_name_set(decoded: &DecodedOffer) -> Option<Vec<Vec<u8>>> {
    if decoded.target_asset_names.is_empty() {
        None
    } else {
        Some(
            decoded
                .target_asset_names
                .iter()
                .filter_map(|n| hex::decode(n).ok())
                .collect(),
        )
    }
}

fn parse_cred(hex_str: &str) -> Option<[u8; 28]> {
    if hex_str.is_empty() {
        return None;
    }
    let bytes = hex::decode(hex_str).ok()?;
    if bytes.len() != 28 {
        return None;
    }
    let mut cred = [0u8; 28];
    cred.copy_from_slice(&bytes);
    Some(cred)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AssetId, TxInput};

    // Real Wayup offer-accept datum (collection-wide Mekanism bid, 55 ADA),
    // from `wayup-store-offer/tests/fixtures/offer-accept`.
    const WAYUP_DATUM_HEX: &str = "d8799f581ccba51a2e5b5b802a0402a87b762d2ecc6b1a9b6d0dd708daf07b82ad9fd8799fd8799fd8799f581c5f08a64f580e581735070e1b1d2ce29ae6942ab45ccff5a1747d2283ffd8799fd8799fd8799f581c28f17fdd2d8b8f559ad61e899e31eae90b9e209cbedb1ee8a8c6c7d1ffffffffa140d8799f00a1401a0010c8e0ffffd8799fd8799fd8799f581c4a00e5040c2d7e201a9c20744ace64bf28d1cda55999a4931e406922ffd8799fd8799fd8799f581ccba51a2e5b5b802a0402a87b762d2ecc6b1a9b6d0dd708daf07b82adffffffffa1581cffa56051fda3d106a96f09c3d209d4bf24a117406fb813fb8b4548e3d8799f01a0ffffd8799fd8799fd8799f581cf32d80def34c2b88fcf3e35d105d52ccc80f06d39f12c1fd7e511ed9ffd8799fd8799fd8799f581ca4334ffe273878ea169e3d90a7271639e250724bcb2bc86c4b029dcbffffffffa140d8799f00a1401a003abf11ffffffff";
    const WAYUP_OFFER_CRED: &str = "27d46ecbec94b052d8f875cf3beafd0e8ca40e8ad069f677e0a128ea";
    const WAYUP_BIDDER: &str = "cba51a2e5b5b802a0402a87b762d2ecc6b1a9b6d0dd708daf07b82ad";
    const MEKANISM_POLICY: &str = "ffa56051fda3d106a96f09c3d209d4bf24a117406fb813fb8b4548e3";
    const MEKANISM_2212: &str = "4d656b616e69736d32323132";
    const MEKANISM_3131: &str = "4d656b616e69736d33313331";
    const WAYUP_RECIPIENT_CRED: &str = "4a00e5040c2d7e201a9c20744ace64bf28d1cda55999a4931e406922";

    // Real jpg.store offer-accept datum (collection-wide tappy bid, 153 ADA),
    // from `jpg-store-offer/tests/fixtures/offer-accept`.
    const JPG_DATUM_HEX: &str = "d8799f581ccd55cd8d31dd837222a878bda41bd1ef578eb9ec6d05778de039e6cf9fd8799fd8799fd87a9f581c84cc25ea4c29951d40b443b95bbc5676bc425470f96376d1984af9abffd8799fd8799fd87a9f581c2c967f4bd28944b06462e13c5e3f5d5fa6e03f8567569438cd833e6dffffffffa140d8799f00a1401a003d6aa8ffffd8799fd8799fd8799f581c04996cec3ca429ea79b3c710682713e31223f79f4f7a7fd7ee2a17d5ffd8799fd8799fd8799f581cf0a884feabaf419f18ed9d643dc27d3ccf6d204fda17d3b60fbeb8d6ffffffffa140d8799f00a1401a0074bad0ffffd8799fd8799fd8799f581ccd55cd8d31dd837222a878bda41bd1ef578eb9ec6d05778de039e6cfffd8799fd8799fd8799f581ca464f2f85212cb01bbab8235cf9aab6cf801c11c1bee75f9d684850affffffffa1581ce3ff4ab89245ede61b3e2beab0443dbcc7ea8ca2c017478e4e8990e2d8799f01a0ffffffff";
    const JPG_BIDDER: &str = "cd55cd8d31dd837222a878bda41bd1ef578eb9ec6d05778de039e6cf";
    const TAPPY_POLICY: &str = "e3ff4ab89245ede61b3e2beab0443dbcc7ea8ca2c017478e4e8990e2";
    const TAPPY_3589: &str = "746170707933353839";
    const TAPPY_1234: &str = "746170707931323334";
    const JPG_SELLER_ADDR: &str = "addr1q8x4tnvdx8wcxu3z4putmfqm68h40r4ea3ks2auduqu7dnayvne0s5sjevqmh2uzxh8e42mvlqquz8qmae6ln45ys59q79cap8";

    /// Build a mainnet enterprise (no-stake) bech32 address for a payment cred —
    /// enough to exercise the credential-match paths.
    fn enterprise_addr(cred_hex: &str) -> String {
        let cred = hex::decode(cred_hex).unwrap();
        let mut bytes = vec![0x61u8];
        bytes.extend_from_slice(&cred);
        pallas_addresses::Address::from_bytes(&bytes)
            .unwrap()
            .to_bech32()
            .unwrap()
    }

    fn asset(policy_hex: &str, name_hex: &str) -> AssetId {
        AssetId {
            policy: hex::decode(policy_hex).unwrap(),
            name: hex::decode(name_hex).unwrap(),
        }
    }

    #[test]
    fn wayup_datum_decodes_target_and_recipient() {
        let d = decode_wayup_offer_datum(&hex::decode(WAYUP_DATUM_HEX).unwrap()).unwrap();
        assert_eq!(d.bidder_pkh, WAYUP_BIDDER);
        assert_eq!(d.target_policy.as_deref(), Some(MEKANISM_POLICY));
        assert!(d.target_asset_names.is_empty()); // collection-wide
        assert_eq!(
            d.target_recipient,
            Some(
                hex::decode(WAYUP_RECIPIENT_CRED)
                    .unwrap()
                    .try_into()
                    .unwrap()
            )
        );
    }

    #[test]
    fn jpg_datum_decodes_last_payout_target() {
        let d = decode_jpg_offer_datum(&hex::decode(JPG_DATUM_HEX).unwrap()).unwrap();
        assert_eq!(d.bidder_pkh, JPG_BIDDER);
        assert_eq!(d.target_policy.as_deref(), Some(TAPPY_POLICY));
        assert!(d.target_asset_names.is_empty());
        assert_eq!(d.target_recipient, None); // jpg doesn't encode it
    }

    fn wayup_offer_input(redeemer: Option<Vec<u8>>) -> TxInput {
        TxInput {
            address: enterprise_addr(WAYUP_OFFER_CRED),
            lovelace: 55_000_000,
            assets: vec![],
            datum: Some(hex::decode(WAYUP_DATUM_HEX).unwrap()),
            redeemer,
            oref_tx_hash: hex::decode(
                "a6d2229a5f6fcdec6c1f4473df516e80e60fbf47d6a8c263384c6448a1a77f88",
            )
            .unwrap(),
            oref_index: 1,
        }
    }

    /// The same bidder's offer, spent from a different oref — the batched-fill
    /// shape, where the two offers are identical apart from where they sat.
    fn wayup_offer_input_at(oref_index: u32) -> TxInput {
        TxInput {
            oref_index,
            ..wayup_offer_input(Some(vec![0xd8, 0x7a, 0x80]))
        }
    }

    #[test]
    fn wayup_accept_picks_recipient_asset_not_change() {
        let tx = DecodeTx {
            tx_hash: hex::decode(
                "361985963006a6ed1e3ab4a338d8ee19a464712f7d62d8d03772e2b0553651f7",
            )
            .unwrap(),
            inputs: vec![wayup_offer_input(Some(vec![0xd8, 0x7a, 0x80]))],
            outputs: vec![
                // Seller's change output also carries a Mekanism under the same
                // policy — must NOT be reported (wrong recipient cred).
                TxOutput {
                    address: enterprise_addr(WAYUP_BIDDER),
                    lovelace: 2_000_000,
                    assets: vec![asset(MEKANISM_POLICY, "4d656b616e69736d39393939")],
                    ..Default::default()
                },
                // Delivery to the bidder's own wallet.
                TxOutput {
                    address: enterprise_addr(WAYUP_RECIPIENT_CRED),
                    lovelace: 2_000_000,
                    assets: vec![asset(MEKANISM_POLICY, MEKANISM_2212)],
                    ..Default::default()
                },
            ],
            required_signers: vec![],
        };
        let cfg = WayupOfferConfig::from_hex(WAYUP_OFFER_CRED);
        let accepts = decode_wayup_offer_accepts(&tx, &cfg);
        assert_eq!(accepts.len(), 1);
        let a = &accepts[0];
        assert_eq!(a.policy, MEKANISM_POLICY);
        assert_eq!(a.asset_name_hex, MEKANISM_2212);
        assert_eq!(a.price_lovelace, 55_000_000);
        assert_eq!(a.prior_output_index, 1);
        assert!(a.collection_offer);
        assert_eq!(a.seller_address, "");
    }

    #[test]
    fn wayup_batched_fill_pairs_each_offer_with_its_own_asset() {
        // Mainnet shape (b92da74b…): one seller filling two of the SAME
        // bidder's collection offers in one tx. Both offers carry the same
        // target policy and recipient credential, so nothing but claiming
        // distinguishes them — first-match-wins reported the first delivery
        // twice and dropped the second asset entirely.
        let tx = DecodeTx {
            tx_hash: hex::decode(
                "b92da74ba21088a5d7537a23c2a526c8e5e0e3b4f3d6d2d98d8d7e406541f1b2",
            )
            .unwrap(),
            inputs: vec![wayup_offer_input_at(0), wayup_offer_input_at(3)],
            outputs: vec![
                TxOutput {
                    address: enterprise_addr(WAYUP_RECIPIENT_CRED),
                    lovelace: 1_168_010,
                    assets: vec![asset(MEKANISM_POLICY, MEKANISM_2212)],
                    index: 1,
                    ..Default::default()
                },
                TxOutput {
                    address: enterprise_addr(WAYUP_RECIPIENT_CRED),
                    lovelace: 1_163_700,
                    assets: vec![asset(MEKANISM_POLICY, MEKANISM_3131)],
                    index: 4,
                    ..Default::default()
                },
            ],
            required_signers: vec![],
        };
        let cfg = WayupOfferConfig::from_hex(WAYUP_OFFER_CRED);
        let accepts = decode_wayup_offer_accepts(&tx, &cfg);

        assert_eq!(accepts.len(), 2);
        // Offers in input order, deliveries in output order.
        assert_eq!(accepts[0].prior_output_index, 0);
        assert_eq!(accepts[0].asset_name_hex, MEKANISM_2212);
        assert_eq!(accepts[1].prior_output_index, 3);
        assert_eq!(accepts[1].asset_name_hex, MEKANISM_3131);
    }

    #[test]
    fn wayup_offer_with_no_asset_left_to_claim_is_not_an_accept() {
        // Two offers spent but only one asset delivered: the second offer went
        // somewhere else (a cancel batched alongside the fill). Reporting it as
        // an accept would invent a sale — better to drop it than to double-count
        // the one asset that did move.
        let tx = DecodeTx {
            tx_hash: vec![],
            inputs: vec![wayup_offer_input_at(0), wayup_offer_input_at(3)],
            outputs: vec![TxOutput {
                address: enterprise_addr(WAYUP_RECIPIENT_CRED),
                lovelace: 2_000_000,
                assets: vec![asset(MEKANISM_POLICY, MEKANISM_2212)],
                index: 1,
                ..Default::default()
            }],
            required_signers: vec![],
        };
        let cfg = WayupOfferConfig::from_hex(WAYUP_OFFER_CRED);
        let accepts = decode_wayup_offer_accepts(&tx, &cfg);

        assert_eq!(accepts.len(), 1);
        assert_eq!(accepts[0].prior_output_index, 0);
        assert_eq!(accepts[0].asset_name_hex, MEKANISM_2212);
    }

    #[test]
    fn wayup_bidder_signed_is_cancel_not_accept() {
        let tx = DecodeTx {
            tx_hash: vec![],
            inputs: vec![wayup_offer_input(Some(vec![0xd8, 0x7a, 0x80]))],
            outputs: vec![TxOutput {
                address: enterprise_addr(WAYUP_RECIPIENT_CRED),
                lovelace: 2_000_000,
                assets: vec![asset(MEKANISM_POLICY, MEKANISM_2212)],
                ..Default::default()
            }],
            // Bidder signed → reclaiming (cancel), never an accept.
            required_signers: vec![hex::decode(WAYUP_BIDDER).unwrap()],
        };
        let cfg = WayupOfferConfig::from_hex(WAYUP_OFFER_CRED);
        assert!(decode_wayup_offer_accepts(&tx, &cfg).is_empty());
    }

    fn jpg_tx(redeemer: Vec<u8>) -> DecodeTx {
        DecodeTx {
            tx_hash: hex::decode(
                "a09bd6429cfad3c48886d3e469b539a1359102535e143c9bff186a86cd2f5708",
            )
            .unwrap(),
            inputs: vec![TxInput {
                address: JPG_OFFER_V2_ADDR.to_string(),
                lovelace: 153_000_000,
                assets: vec![],
                datum: Some(hex::decode(JPG_DATUM_HEX).unwrap()),
                redeemer: Some(redeemer),
                oref_tx_hash: hex::decode(
                    "96a05c1728258b335776ed384941346f397479a273ec73fe17a0e8963bb4325e",
                )
                .unwrap(),
                oref_index: 0,
            }],
            outputs: vec![TxOutput {
                address: JPG_SELLER_ADDR.to_string(),
                lovelace: 2_000_000,
                assets: vec![asset(TAPPY_POLICY, TAPPY_3589)],
                ..Default::default()
            }],
            required_signers: vec![],
        }
    }

    #[test]
    fn jpg_accept_matches_first_delivered_asset() {
        // Accept redeemer d87a80 (constructor 1 — inverted vs the sale contract).
        let accepts = decode_jpg_offer_accepts(&jpg_tx(vec![0xd8, 0x7a, 0x80]));
        assert_eq!(accepts.len(), 1);
        let a = &accepts[0];
        assert_eq!(a.bidder_pkh, JPG_BIDDER);
        assert_eq!(a.policy, TAPPY_POLICY);
        assert_eq!(a.asset_name_hex, TAPPY_3589);
        assert_eq!(a.price_lovelace, 153_000_000);
        assert_eq!(a.seller_address, JPG_SELLER_ADDR);
        assert!(a.collection_offer);
        assert_eq!(a.prior_output_index, 0);
    }

    #[test]
    fn jpg_batched_fill_pairs_each_offer_with_its_own_asset() {
        // jpg matches on policy alone (no recipient credential in the datum),
        // so two of one bidder's collection offers filled together are even
        // less distinguishable than on Wayup. Same claiming rule.
        let base = jpg_tx(vec![0xd8, 0x7a, 0x80]);
        let second_offer = TxInput {
            oref_index: 1,
            ..base.inputs[0].clone()
        };
        let tx = DecodeTx {
            inputs: vec![base.inputs[0].clone(), second_offer],
            outputs: vec![
                TxOutput {
                    address: JPG_SELLER_ADDR.to_string(),
                    lovelace: 2_000_000,
                    assets: vec![asset(TAPPY_POLICY, TAPPY_3589)],
                    index: 0,
                    ..Default::default()
                },
                TxOutput {
                    address: JPG_SELLER_ADDR.to_string(),
                    lovelace: 2_000_000,
                    assets: vec![asset(TAPPY_POLICY, TAPPY_1234)],
                    index: 2,
                    ..Default::default()
                },
            ],
            ..base
        };

        let accepts = decode_jpg_offer_accepts(&tx);
        assert_eq!(accepts.len(), 2);
        assert_eq!(accepts[0].asset_name_hex, TAPPY_3589);
        assert_eq!(accepts[1].asset_name_hex, TAPPY_1234);
    }

    #[test]
    fn jpg_cancel_redeemer_is_not_accept() {
        // Cancel redeemer d87980 (constructor 0) → no accept.
        assert!(decode_jpg_offer_accepts(&jpg_tx(vec![0xd8, 0x79, 0x80])).is_empty());
    }
}
