//! Offer (bid) lifecycle assembly: classify the collection-offer UTxOs a
//! transaction produces and consumes into Create / Cancel / Update / Accept, and
//! project to the venue wire events. Extracted from the live
//! `{jpg,wayup}-store-offer` modules so the walker and the modules decode offer
//! lifecycle byte-identically. The **Accept** branch already lives in
//! [`crate::offers`]; this module wraps it with the stateful Create/Cancel/Update
//! correlation the modules used to carry.
//!
//! ## Within-tx classification (mirrors the modules' `flush_buffer`)
//!
//! Per bidder, a spend paired 1:1 with a produce is an atomic edit → `Update`
//! (only when the bidder has exactly one consume AND one produce in the tx;
//! batches emit independent events, since N-to-N pairing has no reliable
//! correspondence). Otherwise a consume is an `Accept` (the crate's accept decode
//! matched a delivered asset to this offer's oref) or a `Cancel`; a produce with
//! no pairing is a `Create`. Emission order: all consumes (tx-input order), then
//! unpaired produces (tx-output order) — the modules' order, preserved for
//! byte-identical goldens.
//!
//! Venue differences:
//! - **jpg**: the redeemer signals cancel (`d87980`, inverted vs the sale
//!   contract). A non-cancel consume with no crate-matched asset still emits a
//!   *partial* `Accept` (lifecycle event preserved, asset unknown), never a
//!   Cancel.
//! - **Wayup**: the redeemer carries no signal; accept-vs-cancel is decided
//!   inside [`crate::offers::decode_wayup_offer_accepts`] (bidder-signed ⇒
//!   cancel). A consume with no crate accept and no pairing is a `Cancel`.
//!
//! ## Datum resolution
//!
//! Unlike listings, offer datums have no create-payload-only rule — the caller
//! resolves every offer datum eagerly (jpg via the tx-metadata labels-50+
//! convention against the offer's origin tx; Wayup via `datum_by_hash`) and
//! passes the resolved CBOR: consumed offers in [`TxInput::datum`], produced
//! offers in [`crate::OutputDatum::payload`]. No resolver closure is needed here.

use std::collections::{HashMap, HashSet};

use mitos_community_events::jpg_store_offer::{
    JpgStoreOffer, JpgStoreOfferVersion, OfferAccept as JpgOfferAccept,
    OfferCancel as JpgOfferCancel, OfferCreate as JpgOfferCreate, OfferUpdate as JpgOfferUpdate,
};
use mitos_community_events::wayup_store_offer::{
    OfferCancel as WayupOfferCancel, OfferCreate as WayupOfferCreate,
    OfferUpdate as WayupOfferUpdate, WayupStoreOffer, WayupStoreOfferVersion,
};

use crate::DecodeTx;
use crate::offer_datum::{DecodedOffer, decode_jpg_offer_datum, decode_wayup_offer_datum};
use crate::offers::{
    WayupOfferConfig, classify_jpg_offer_address, decode_jpg_offer_accepts,
    decode_wayup_offer_accepts,
};

/// jpg.store offer **cancel** redeemer: constructor 0, empty (`d87980`) —
/// inverted vs the sale contract (where 0 = Buy).
fn is_jpg_offer_cancel(redeemer: &[u8]) -> bool {
    redeemer == [0xd8, 0x79, 0x80]
}

/// A consumed offer UTxO (a spent bid) with its resolved datum.
struct OfferInput<V> {
    prior_tx_hash: Vec<u8>,
    prior_output_index: u32,
    prior_lovelace: u64,
    redeemer: Option<Vec<u8>>,
    version: V,
    decoded: DecodedOffer,
}

/// A produced offer UTxO (a fresh/replacement bid) with its resolved datum.
struct OfferOutput<V> {
    output_index: u32,
    lovelace: u64,
    datum_bytes: Vec<u8>,
    version: V,
    decoded: DecodedOffer,
}

/// Collect the consumed + produced offer UTxOs of a tx, decoding each datum.
/// Consumed datums come pre-resolved from [`TxInput::datum`]; produced datums
/// from [`crate::OutputDatum::payload`] (see the module docs on resolution).
fn collect_offers<V: Clone>(
    tx: &DecodeTx,
    classify: impl Fn(&str) -> Option<V>,
    decode_datum: impl Fn(&[u8]) -> Option<DecodedOffer>,
) -> (Vec<OfferInput<V>>, Vec<OfferOutput<V>>) {
    let mut consumed = Vec::new();
    for input in &tx.inputs {
        let Some(version) = classify(&input.address) else {
            continue;
        };
        let Some(datum) = input.datum.as_ref() else {
            continue;
        };
        let Some(decoded) = decode_datum(datum) else {
            continue;
        };
        consumed.push(OfferInput {
            prior_tx_hash: input.oref_tx_hash.clone(),
            prior_output_index: input.oref_index,
            prior_lovelace: input.lovelace,
            redeemer: input.redeemer.clone(),
            version,
            decoded,
        });
    }

    let mut produced = Vec::new();
    for output in &tx.outputs {
        let Some(version) = classify(&output.address) else {
            continue;
        };
        let Some(datum) = output.datum.as_ref().filter(|d| !d.payload.is_empty()) else {
            continue;
        };
        let Some(decoded) = decode_datum(&datum.payload) else {
            continue;
        };
        produced.push(OfferOutput {
            output_index: output.index,
            lovelace: output.lovelace,
            datum_bytes: datum.payload.clone(),
            version,
            decoded,
        });
    }

    (consumed, produced)
}

/// Bidders with exactly one consume AND one produce this tx (atomic-edit /
/// Update detection). Snapshotted up front, so taking produced slots afterward
/// doesn't affect membership. Batches (any bidder with >1 of either) are absent,
/// so they fall through to independent Cancel/Accept + Create events.
fn update_pair_bidders<V>(
    consumed: &[OfferInput<V>],
    produced: &[Option<OfferOutput<V>>],
) -> HashSet<String> {
    let mut consume_count: HashMap<String, usize> = HashMap::new();
    for c in consumed {
        *consume_count
            .entry(c.decoded.bidder_pkh.clone())
            .or_default() += 1;
    }
    let mut produce_count: HashMap<String, usize> = HashMap::new();
    for p in produced.iter().flatten() {
        *produce_count
            .entry(p.decoded.bidder_pkh.clone())
            .or_default() += 1;
    }
    consume_count
        .into_iter()
        .filter(|(pkh, c)| *c == 1 && produce_count.get(pkh).copied().unwrap_or(0) == 1)
        .map(|(pkh, _)| pkh)
        .collect()
}

/// Take the first unpaired produced slot for `bidder_pkh` (pairing is by bidder
/// alone; the count guard already confirmed exactly one of each).
fn take_paired_produced<V>(
    produced_slots: &mut [Option<OfferOutput<V>>],
    bidder_pkh: &str,
) -> Option<OfferOutput<V>> {
    produced_slots
        .iter_mut()
        .find(|s| {
            s.as_ref()
                .is_some_and(|p| p.decoded.bidder_pkh == bidder_pkh)
        })
        .and_then(|s| s.take())
}

// ============================================================
// jpg.store
// ============================================================

/// Decode every jpg.store offer lifecycle event in a transaction. Cancel is
/// redeemer-signalled (`d87980`); a non-cancel consume the accept decode can't
/// asset-match still emits a partial `Accept`.
pub fn decode_jpg_offer_lifecycle(tx: &DecodeTx) -> Vec<JpgStoreOffer> {
    let tx_hash_hex = hex::encode(&tx.tx_hash);
    let mut accepts: HashMap<(String, u32), JpgOfferAccept> = decode_jpg_offer_accepts(tx)
        .into_iter()
        .map(|a| ((a.prior_tx_hash.clone(), a.prior_output_index), a))
        .collect();
    let (consumed, produced) =
        collect_offers(tx, classify_jpg_offer_address, decode_jpg_offer_datum);
    let mut produced_slots: Vec<Option<OfferOutput<JpgStoreOfferVersion>>> =
        produced.into_iter().map(Some).collect();
    let update_bidders = update_pair_bidders(&consumed, &produced_slots);

    let mut out = Vec::new();
    for consume in consumed {
        let bidder_pkh = consume.decoded.bidder_pkh.clone();

        if update_bidders.contains(&bidder_pkh)
            && let Some(produced) = take_paired_produced(&mut produced_slots, &bidder_pkh)
        {
            out.push(JpgStoreOffer::Update(JpgOfferUpdate {
                bidder_pkh,
                tx_hash: tx_hash_hex.clone(),
                prior_tx_hash: hex::encode(&consume.prior_tx_hash),
                prior_output_index: consume.prior_output_index,
                new_output_index: produced.output_index,
                previous_lovelace: consume.prior_lovelace,
                new_lovelace: produced.lovelace,
                datum_cbor: produced.datum_bytes,
                target_policy: produced.decoded.target_policy,
                target_asset_names: produced.decoded.target_asset_names,
                co_version: produced.version,
            }));
            continue;
        }

        if consume.redeemer.as_deref().is_some_and(is_jpg_offer_cancel) {
            out.push(JpgStoreOffer::Cancel(JpgOfferCancel {
                bidder_pkh,
                tx_hash: tx_hash_hex.clone(),
                prior_tx_hash: hex::encode(&consume.prior_tx_hash),
                prior_output_index: consume.prior_output_index,
                target_policy: consume.decoded.target_policy,
                co_version: consume.version,
            }));
            continue;
        }

        // Non-cancel consume: the crate's accept decode matched an asset (real
        // Accept), or it couldn't (partial Accept — lifecycle event preserved).
        let key = (
            hex::encode(&consume.prior_tx_hash),
            consume.prior_output_index,
        );
        if let Some(accept) = accepts.remove(&key) {
            out.push(JpgStoreOffer::Accept(accept));
        } else {
            out.push(JpgStoreOffer::Accept(JpgOfferAccept {
                bidder_pkh,
                tx_hash: tx_hash_hex.clone(),
                prior_tx_hash: hex::encode(&consume.prior_tx_hash),
                prior_output_index: consume.prior_output_index,
                policy: String::new(),
                asset_name_hex: String::new(),
                price_lovelace: consume.prior_lovelace,
                seller_address: String::new(),
                co_version: consume.version,
                collection_offer: consume.decoded.target_asset_names.is_empty(),
            }));
        }
    }

    for p in produced_slots.into_iter().flatten() {
        out.push(JpgStoreOffer::Create(JpgOfferCreate {
            bidder_pkh: p.decoded.bidder_pkh,
            tx_hash: tx_hash_hex.clone(),
            output_index: p.output_index,
            lovelace: p.lovelace,
            datum_cbor: p.datum_bytes,
            target_policy: p.decoded.target_policy,
            target_asset_names: p.decoded.target_asset_names,
            co_version: p.version,
        }));
    }

    out
}

// ============================================================
// Wayup
// ============================================================

/// Decode every Wayup offer lifecycle event in a transaction. Accept-vs-cancel
/// is decided inside [`decode_wayup_offer_accepts`] (bidder-signed ⇒ cancel); a
/// consume with no crate accept and no Update pairing is a `Cancel`.
pub fn decode_wayup_offer_lifecycle(tx: &DecodeTx, cfg: &WayupOfferConfig) -> Vec<WayupStoreOffer> {
    let tx_hash_hex = hex::encode(&tx.tx_hash);
    let mut accepts = decode_wayup_offer_accepts(tx, cfg)
        .into_iter()
        .map(|a| ((a.prior_tx_hash.clone(), a.prior_output_index), a))
        .collect::<HashMap<_, _>>();
    let (consumed, produced) = collect_offers(
        tx,
        |addr| {
            cfg.is_offer_address(addr)
                .then_some(WayupStoreOfferVersion::V1)
        },
        decode_wayup_offer_datum,
    );
    let mut produced_slots: Vec<Option<OfferOutput<WayupStoreOfferVersion>>> =
        produced.into_iter().map(Some).collect();
    let update_bidders = update_pair_bidders(&consumed, &produced_slots);

    let mut out = Vec::new();
    for consume in consumed {
        let bidder_pkh = consume.decoded.bidder_pkh.clone();

        if update_bidders.contains(&bidder_pkh)
            && let Some(produced) = take_paired_produced(&mut produced_slots, &bidder_pkh)
        {
            out.push(WayupStoreOffer::Update(WayupOfferUpdate {
                bidder_pkh,
                tx_hash: tx_hash_hex.clone(),
                prior_tx_hash: hex::encode(&consume.prior_tx_hash),
                prior_output_index: consume.prior_output_index,
                new_output_index: produced.output_index,
                previous_lovelace: consume.prior_lovelace,
                new_lovelace: produced.lovelace,
                datum_cbor: produced.datum_bytes,
                target_policy: produced.decoded.target_policy,
                target_asset_names: produced.decoded.target_asset_names,
                co_version: WayupStoreOfferVersion::V1,
            }));
            continue;
        }

        let key = (
            hex::encode(&consume.prior_tx_hash),
            consume.prior_output_index,
        );
        if let Some(accept) = accepts.remove(&key) {
            out.push(WayupStoreOffer::Accept(accept));
        } else {
            out.push(WayupStoreOffer::Cancel(WayupOfferCancel {
                bidder_pkh,
                tx_hash: tx_hash_hex.clone(),
                prior_tx_hash: hex::encode(&consume.prior_tx_hash),
                prior_output_index: consume.prior_output_index,
                target_policy: consume.decoded.target_policy,
                co_version: WayupStoreOfferVersion::V1,
            }));
        }
    }

    for p in produced_slots.into_iter().flatten() {
        out.push(WayupStoreOffer::Create(WayupOfferCreate {
            bidder_pkh: p.decoded.bidder_pkh,
            tx_hash: tx_hash_hex.clone(),
            output_index: p.output_index,
            lovelace: p.lovelace,
            datum_cbor: p.datum_bytes,
            target_policy: p.decoded.target_policy,
            target_asset_names: p.decoded.target_asset_names,
            co_version: WayupStoreOfferVersion::V1,
        }));
    }

    out
}
