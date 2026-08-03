//! Listing lifecycle assembly: classify the listing UTxOs a transaction produces
//! and consumes into Create / Update / Unlisting, and project to the venue wire
//! events. Extracted from the live `{jpg,wayup}-store-listing` modules so the
//! walker and the modules decode listing lifecycle byte-identically (the same
//! single-source-of-truth move already made for sales and offer-accepts).
//!
//! ## Within-tx classification (mirrors the modules' `flush_buffer`)
//!
//! A produced listing paired with a consumed listing for the *same asset* is an
//! `Update`; a produced listing alone is a `Create`; a cancel-redeemer consume
//! with no paired produce is an `Unlisting`. Bundles (a listing UTxO escrowing
//! more than one asset) emit one event per member, each stamped with the bundle
//! size. Events are ordered produced-first (by `(policy, asset_name)`), then the
//! remaining unlistings (same ordering) — the modules' `BTreeMap` iteration
//! order, preserved so goldens stay byte-identical.
//!
//! ## Datum resolution (the subtle part)
//!
//! Listing datums are hash-only on jpg (and often on Wayup); the preimage is
//! revealed later, in the spend tx's witness set. Resolution is asymmetric and
//! **must be preserved**:
//!
//! - jpg **create** reads the produced output's inline `payload` only and NEVER
//!   falls back to the `resolve` closure — a deliberate boot-stall fix (a
//!   fallback provider would otherwise issue one call per listing on every
//!   bootstrap re-scan of the stranded jpg book). An unresolved create emits
//!   empty payouts (honest-about-unknowns), not a dropped event.
//! - Wayup **create** and every **update** / **unlisting** read the payload and,
//!   when empty, fall back to `resolve` (datum hash → CBOR bytes).
//!
//! The consumed (prior) side is passed already-resolved in [`TxInput::datum`]
//! (the caller resolves it however it can — the module via `datum_by_hash`, the
//! walker via the spend tx's witnesses). Only the produced side needs the
//! payload-vs-hash split, so it rides in [`TxOutput::datum`] and this module
//! applies `resolve` per the policy above.

use std::collections::BTreeMap;

use mitos_community_events::jpg_store_listing::{
    JpgStoreContractVersion, JpgStoreListing, ListingCreate as JpgListingCreate,
    ListingUpdate as JpgListingUpdate, Unlisting as JpgUnlisting,
};
use mitos_community_events::marketplace::ListingPayout;
use mitos_community_events::wayup_store_listing::{
    ListingCreate as WayupListingCreate, ListingUpdate as WayupListingUpdate,
    Unlisting as WayupUnlisting, WayupStoreContractVersion, WayupStoreListing,
};

use crate::datum::decode_listing_datum;
use crate::sales::{WayupSaleConfig, classify_jpg_address};
use crate::{DecodeTx, OutputDatum};

/// jpg.store / Wayup listing **cancel** (delist) redeemer: constructor 1 with
/// empty fields, exactly `d87a80`. The live modules match the full three bytes
/// (not the `d87a` prefix `datum::is_cancel_redeemer` uses), so a richer
/// constructor-1 redeemer would NOT be treated as a delist — preserved here to
/// keep goldens byte-identical.
fn is_delist_redeemer(bytes: &[u8]) -> bool {
    bytes == [0xd8, 0x7a, 0x80]
}

fn sum_payouts(payouts: &[ListingPayout]) -> u64 {
    payouts.iter().map(|p| p.lovelace).sum()
}

/// Resolve a produced output's datum to CBOR bytes: the inline `payload` when
/// present, else the `hash` via the caller's `resolve` closure. `None` when the
/// output has no datum at all.
fn resolve_produced(
    datum: Option<&OutputDatum>,
    resolve: &impl Fn(&[u8]) -> Option<Vec<u8>>,
) -> Option<Vec<u8>> {
    let d = datum?;
    if !d.payload.is_empty() {
        return Some(d.payload.clone());
    }
    resolve(&d.hash)
}

/// A neutral (venue-agnostic) listing lifecycle event, projected to the venue
/// wire type by [`project_jpg`] / [`project_wayup`]. `cred_hex` is the datum's
/// owner credential — jpg's seller payment pkh, Wayup's seller stake credential.
enum ListingEvent<V> {
    Create {
        policy_hex: String,
        asset_name_hex: String,
        tx_hash_hex: String,
        output_index: u32,
        price_lovelace: u64,
        cred_hex: String,
        payouts: Vec<ListingPayout>,
        version: V,
        bundle_size: Option<u32>,
    },
    Update {
        policy_hex: String,
        asset_name_hex: String,
        tx_hash_hex: String,
        output_index: u32,
        previous_price_lovelace: u64,
        new_price_lovelace: u64,
        cred_hex: String,
        payouts: Vec<ListingPayout>,
        version: V,
        bundle_size: Option<u32>,
    },
    Unlisting {
        policy_hex: String,
        asset_name_hex: String,
        tx_hash_hex: String,
        cred_hex: String,
        version: V,
        bundle_size: Option<u32>,
    },
}

struct ProducedListing<V> {
    output_index: u32,
    datum: Option<OutputDatum>,
    version: V,
    bundle_size: Option<u32>,
}

struct ConsumedListing<V> {
    /// Already-resolved prior-listing datum bytes (see module docs).
    prior_datum: Option<Vec<u8>>,
    version: V,
    bundle_size: Option<u32>,
}

/// Venue-agnostic listing classifier. `classify` gates which addresses are the
/// venue's listing contract (returning its version tag); `create_uses_resolver`
/// selects the create-path datum policy (jpg = payload-only `false`; Wayup =
/// payload-then-resolver `true`). See the module docs for the resolution split.
fn collect_listings<V: Clone>(
    tx: &DecodeTx,
    classify: impl Fn(&str) -> Option<V>,
    resolve: impl Fn(&[u8]) -> Option<Vec<u8>>,
    create_uses_resolver: bool,
) -> Vec<ListingEvent<V>> {
    let tx_hash_hex = hex::encode(&tx.tx_hash);

    // Produced listings keyed by `(policy, asset_name)`; bundles insert one
    // entry per escrowed asset (same key shape as the live modules).
    let mut produced: BTreeMap<(Vec<u8>, Vec<u8>), ProducedListing<V>> = BTreeMap::new();
    for output in &tx.outputs {
        let Some(version) = classify(&output.address) else {
            continue;
        };
        let bundle_size = (output.assets.len() > 1).then_some(output.assets.len() as u32);
        for asset in &output.assets {
            produced.insert(
                (asset.policy.clone(), asset.name.clone()),
                ProducedListing {
                    output_index: output.index,
                    datum: output.datum.clone(),
                    version: version.clone(),
                    bundle_size,
                },
            );
        }
    }

    // Consumed listings: only cancel-redeemer spends at the venue (a Buy spend
    // is the sale module's domain). `TxInput::datum` is already resolved.
    let mut consumed: BTreeMap<(Vec<u8>, Vec<u8>), ConsumedListing<V>> = BTreeMap::new();
    for input in &tx.inputs {
        let Some(version) = classify(&input.address) else {
            continue;
        };
        let Some(redeemer) = input.redeemer.as_ref() else {
            continue;
        };
        if !is_delist_redeemer(redeemer) {
            continue;
        }
        let bundle_size = (input.assets.len() > 1).then_some(input.assets.len() as u32);
        for asset in &input.assets {
            consumed.insert(
                (asset.policy.clone(), asset.name.clone()),
                ConsumedListing {
                    prior_datum: input.datum.clone(),
                    version: version.clone(),
                    bundle_size,
                },
            );
        }
    }

    let mut out = Vec::new();
    for ((policy, asset_name), produced) in produced {
        let policy_hex = hex::encode(&policy);
        let asset_name_hex = hex::encode(&asset_name);

        // Create path reads the inline payload only. Updates (and Wayup creates)
        // additionally fall back to the resolver.
        let payload_decoded = produced
            .datum
            .as_ref()
            .filter(|d| !d.payload.is_empty())
            .and_then(|d| decode_listing_datum(&d.payload));

        if let Some(prior) = consumed.remove(&(policy.clone(), asset_name.clone())) {
            let decoded = payload_decoded
                .or_else(|| {
                    resolve_produced(produced.datum.as_ref(), &resolve)
                        .and_then(|b| decode_listing_datum(&b))
                })
                .unwrap_or_default();
            let new_price = sum_payouts(&decoded.payouts);
            let previous_price = prior
                .prior_datum
                .as_deref()
                .and_then(decode_listing_datum)
                .map(|d| sum_payouts(&d.payouts))
                .unwrap_or(0);
            out.push(ListingEvent::Update {
                policy_hex,
                asset_name_hex,
                tx_hash_hex: tx_hash_hex.clone(),
                output_index: produced.output_index,
                previous_price_lovelace: previous_price,
                new_price_lovelace: new_price,
                cred_hex: decoded.cred_hex,
                payouts: decoded.payouts,
                version: produced.version,
                bundle_size: produced.bundle_size,
            });
        } else {
            let decoded = if create_uses_resolver {
                payload_decoded.or_else(|| {
                    resolve_produced(produced.datum.as_ref(), &resolve)
                        .and_then(|b| decode_listing_datum(&b))
                })
            } else {
                payload_decoded
            }
            .unwrap_or_default();
            let price = sum_payouts(&decoded.payouts);
            out.push(ListingEvent::Create {
                policy_hex,
                asset_name_hex,
                tx_hash_hex: tx_hash_hex.clone(),
                output_index: produced.output_index,
                price_lovelace: price,
                cred_hex: decoded.cred_hex,
                payouts: decoded.payouts,
                version: produced.version,
                bundle_size: produced.bundle_size,
            });
        }
    }

    // Remaining consumes (no paired produce) are unlistings.
    for ((policy, asset_name), prior) in consumed {
        let cred_hex = prior
            .prior_datum
            .as_deref()
            .and_then(decode_listing_datum)
            .map(|d| d.cred_hex)
            .unwrap_or_default();
        out.push(ListingEvent::Unlisting {
            policy_hex: hex::encode(&policy),
            asset_name_hex: hex::encode(&asset_name),
            tx_hash_hex: tx_hash_hex.clone(),
            cred_hex,
            version: prior.version,
            bundle_size: prior.bundle_size,
        });
    }

    out
}

// ============================================================
// jpg.store
// ============================================================

/// Decode every jpg.store listing lifecycle event in a transaction.
///
/// `resolve` maps a datum hash to its CBOR bytes (the live module wraps
/// `chain_data::datum_by_hash`; the walker resolves from the spend tx's
/// witnesses). It is consulted for update datums only — jpg creates are
/// payload-only by design (see the module docs).
pub fn decode_jpg_listings(
    tx: &DecodeTx,
    resolve: impl Fn(&[u8]) -> Option<Vec<u8>>,
) -> Vec<JpgStoreListing> {
    collect_listings(tx, classify_jpg_address, resolve, false)
        .into_iter()
        .map(project_jpg)
        .collect()
}

fn project_jpg(ev: ListingEvent<JpgStoreContractVersion>) -> JpgStoreListing {
    match ev {
        ListingEvent::Create {
            policy_hex,
            asset_name_hex,
            tx_hash_hex,
            output_index,
            price_lovelace,
            cred_hex,
            payouts,
            version,
            bundle_size,
        } => JpgStoreListing::Create(JpgListingCreate {
            policy: policy_hex,
            asset_name_hex,
            tx_hash: tx_hash_hex,
            output_index,
            price_lovelace,
            seller_pkh: cred_hex,
            payouts,
            contract_version: version,
            bundle_size,
        }),
        ListingEvent::Update {
            policy_hex,
            asset_name_hex,
            tx_hash_hex,
            output_index,
            previous_price_lovelace,
            new_price_lovelace,
            cred_hex,
            payouts,
            version,
            bundle_size,
        } => JpgStoreListing::Update(JpgListingUpdate {
            policy: policy_hex,
            asset_name_hex,
            tx_hash: tx_hash_hex,
            output_index,
            previous_price_lovelace,
            new_price_lovelace,
            seller_pkh: cred_hex,
            payouts,
            contract_version: version,
            bundle_size,
        }),
        ListingEvent::Unlisting {
            policy_hex,
            asset_name_hex,
            tx_hash_hex,
            cred_hex,
            version,
            bundle_size,
        } => JpgStoreListing::Unlisting(JpgUnlisting {
            policy: policy_hex,
            asset_name_hex,
            tx_hash: tx_hash_hex,
            seller_pkh: cred_hex,
            contract_version: version,
            bundle_size,
        }),
    }
}

// ============================================================
// Wayup
// ============================================================

/// Decode every Wayup listing lifecycle event in a transaction. `cfg` supplies
/// the sale validator's payment credential (listings sit at addresses sharing
/// it); reuse the same [`WayupSaleConfig`] the sale decode takes (the fee
/// credential is unused here). Wayup creates fall back to `resolve` when the
/// inline payload is absent (unlike jpg).
pub fn decode_wayup_listings(
    tx: &DecodeTx,
    cfg: &WayupSaleConfig,
    resolve: impl Fn(&[u8]) -> Option<Vec<u8>>,
) -> Vec<WayupStoreListing> {
    collect_listings(
        tx,
        |addr| {
            cfg.is_listing_address(addr)
                .then_some(WayupStoreContractVersion::V1)
        },
        resolve,
        true,
    )
    .into_iter()
    .map(project_wayup)
    .collect()
}

fn project_wayup(ev: ListingEvent<WayupStoreContractVersion>) -> WayupStoreListing {
    match ev {
        ListingEvent::Create {
            policy_hex,
            asset_name_hex,
            tx_hash_hex,
            output_index,
            price_lovelace,
            cred_hex,
            payouts,
            version,
            bundle_size,
        } => WayupStoreListing::Create(WayupListingCreate {
            policy: policy_hex,
            asset_name_hex,
            tx_hash: tx_hash_hex,
            output_index,
            price_lovelace,
            seller_stake_pkh: cred_hex,
            payouts,
            contract_version: version,
            bundle_size,
        }),
        ListingEvent::Update {
            policy_hex,
            asset_name_hex,
            tx_hash_hex,
            output_index,
            previous_price_lovelace,
            new_price_lovelace,
            cred_hex,
            payouts,
            version,
            bundle_size,
        } => WayupStoreListing::Update(WayupListingUpdate {
            policy: policy_hex,
            asset_name_hex,
            tx_hash: tx_hash_hex,
            output_index,
            previous_price_lovelace,
            new_price_lovelace,
            seller_stake_pkh: cred_hex,
            payouts,
            contract_version: version,
            bundle_size,
        }),
        ListingEvent::Unlisting {
            policy_hex,
            asset_name_hex,
            tx_hash_hex,
            cred_hex,
            version,
            bundle_size,
        } => WayupStoreListing::Unlisting(WayupUnlisting {
            policy: policy_hex,
            asset_name_hex,
            tx_hash: tx_hash_hex,
            seller_stake_pkh: cred_hex,
            contract_version: version,
            bundle_size,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AssetId, TxInput, TxOutput};

    const JPG_V2_ADDR: &str = "addr1x8rjw3pawl0kelu4mj3c8x20fsczf5pl744s9mxz9v8n7efvjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8ekstg4qrx";

    fn asset() -> AssetId {
        AssetId {
            policy: vec![1; 28],
            name: b"Bud".to_vec(),
        }
    }

    /// Build a listing datum `Constr 0 [ [payout…], Bytes(seller) ]`, payout
    /// `Constr 0 [ Addr(Constr0[Constr0[pkh]], no-stake), amount ]`. Amounts are
    /// pre-encoded CBOR uints (e.g. `1a389fd980` = 950 ADA).
    fn listing_datum(seller_pkh: &str, payouts: &[(&str, &str)]) -> Vec<u8> {
        let mut s = String::from("d8799f9f");
        for (pkh, amount_cbor) in payouts {
            s.push_str(&format!(
                "d8799fd8799fd8799f581c{pkh}ffd87a80ff{amount_cbor}ff"
            ));
        }
        s.push_str(&format!("ff581c{seller_pkh}ff"));
        hex::decode(s).expect("valid hex")
    }

    /// A produced jpg listing output carrying an inline datum payload.
    fn produced_output(datum_payload: Vec<u8>) -> TxOutput {
        TxOutput {
            address: JPG_V2_ADDR.into(),
            lovelace: 2_000_000,
            assets: vec![asset()],
            index: 0,
            datum: Some(OutputDatum {
                payload: datum_payload,
                hash: Vec::new(),
            }),
        }
    }

    /// A create with an inline datum yields a `Create` priced at the payout sum,
    /// with the seller pkh from the datum. No resolver call needed.
    #[test]
    fn jpg_inline_create() {
        let seller = "bb".repeat(28);
        let tx = DecodeTx {
            tx_hash: vec![0xab; 32],
            outputs: vec![produced_output(listing_datum(
                &seller,
                &[(&seller, "1a389fd980")],
            ))],
            ..Default::default()
        };
        let events = decode_jpg_listings(&tx, |_| panic!("create must not resolve"));
        assert_eq!(events.len(), 1);
        let JpgStoreListing::Create(c) = &events[0] else {
            panic!("expected Create");
        };
        assert_eq!(c.price_lovelace, 950_000_000);
        assert_eq!(c.seller_pkh, seller);
        assert_eq!(c.output_index, 0);
        assert!(c.bundle_size.is_none());
    }

    /// A hash-only jpg create emits empty payouts and, critically, NEVER invokes
    /// the resolver — the boot-stall fix. (The closure panics if called.)
    #[test]
    fn jpg_hash_only_create_never_resolves() {
        let tx = DecodeTx {
            tx_hash: vec![0xab; 32],
            outputs: vec![TxOutput {
                address: JPG_V2_ADDR.into(),
                lovelace: 2_000_000,
                assets: vec![asset()],
                index: 3,
                datum: Some(OutputDatum {
                    payload: Vec::new(),
                    hash: vec![0x99; 32],
                }),
            }],
            ..Default::default()
        };
        let events = decode_jpg_listings(&tx, |_| panic!("create must not resolve"));
        let JpgStoreListing::Create(c) = &events[0] else {
            panic!("expected Create");
        };
        assert!(c.payouts.is_empty());
        assert_eq!(c.price_lovelace, 0);
        assert_eq!(c.seller_pkh, "");
        assert_eq!(c.output_index, 3);
    }

    /// A cancel-redeemer consume with no paired produce is an `Unlisting`, with
    /// the seller pkh recovered from the (pre-resolved) prior datum.
    #[test]
    fn jpg_delist_is_unlisting() {
        let seller = "cc".repeat(28);
        let tx = DecodeTx {
            tx_hash: vec![0xcd; 32],
            inputs: vec![TxInput {
                address: JPG_V2_ADDR.into(),
                assets: vec![asset()],
                datum: Some(listing_datum(&seller, &[(&seller, "1a389fd980")])),
                redeemer: Some(vec![0xd8, 0x7a, 0x80]),
                ..Default::default()
            }],
            ..Default::default()
        };
        let events = decode_jpg_listings(&tx, |_| None);
        assert_eq!(events.len(), 1);
        let JpgStoreListing::Unlisting(u) = &events[0] else {
            panic!("expected Unlisting");
        };
        assert_eq!(u.seller_pkh, seller);
    }

    /// A consume + produce for the same asset in one tx is an `Update` carrying
    /// both prices; the update path DOES consult the resolver for the new datum.
    #[test]
    fn jpg_update_pairs_consume_and_produce() {
        let seller = "bb".repeat(28);
        let prior = listing_datum(&seller, &[(&seller, "1a389fd980")]); // 950
        let new = listing_datum(&seller, &[(&seller, "1a3b9aca00")]); // 1000
        let new_hash = vec![0x77; 32];
        let tx = DecodeTx {
            tx_hash: vec![0xab; 32],
            inputs: vec![TxInput {
                address: JPG_V2_ADDR.into(),
                assets: vec![asset()],
                datum: Some(prior),
                redeemer: Some(vec![0xd8, 0x7a, 0x80]),
                ..Default::default()
            }],
            outputs: vec![TxOutput {
                address: JPG_V2_ADDR.into(),
                lovelace: 2_000_000,
                assets: vec![asset()],
                index: 1,
                // hash-only: forces the update path to use the resolver.
                datum: Some(OutputDatum {
                    payload: Vec::new(),
                    hash: new_hash.clone(),
                }),
            }],
            ..Default::default()
        };
        let events = decode_jpg_listings(&tx, move |h| (h == new_hash).then(|| new.clone()));
        assert_eq!(events.len(), 1);
        let JpgStoreListing::Update(u) = &events[0] else {
            panic!("expected Update");
        };
        assert_eq!(u.previous_price_lovelace, 950_000_000);
        assert_eq!(u.new_price_lovelace, 1_000_000_000);
        assert_eq!(u.output_index, 1);
    }

    /// Wayup creates DO fall back to the resolver when the payload is absent.
    #[test]
    fn wayup_hash_only_create_uses_resolver() {
        let seller = "dd".repeat(28);
        let datum = listing_datum(&seller, &[(&seller, "1a389fd980")]);
        let hash = vec![0x55; 32];
        let cfg = WayupSaleConfig::from_hex(
            &hex::encode(crate::sales::address_payment_cred(JPG_V2_ADDR).unwrap()),
            "",
        );
        let tx = DecodeTx {
            tx_hash: vec![0xee; 32],
            outputs: vec![TxOutput {
                address: JPG_V2_ADDR.into(),
                lovelace: 2_000_000,
                assets: vec![asset()],
                index: 0,
                datum: Some(OutputDatum {
                    payload: Vec::new(),
                    hash: hash.clone(),
                }),
            }],
            ..Default::default()
        };
        let events = decode_wayup_listings(&tx, &cfg, move |h| (h == hash).then(|| datum.clone()));
        let WayupStoreListing::Create(c) = &events[0] else {
            panic!("expected Create");
        };
        assert_eq!(c.price_lovelace, 950_000_000);
        assert_eq!(c.seller_stake_pkh, seller);
    }
}
