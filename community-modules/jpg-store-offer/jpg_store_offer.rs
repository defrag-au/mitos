//! jpg.store offer lifecycle community module.
//!
//! Watches the V2 + V3 CO script addresses and emits typed
//! `JpgStoreOffer::{Create, Cancel, Accept, Update}` events
//! per offer lifecycle stage.
//!
//! ## Datum recovery
//!
//! jpg.store offer outputs commit to a hash-only datum on
//! chain. Bytes are revealed via the well-known "labels-50+"
//! convention: the offer's create TX carries the datum CBOR
//! split across metadata labels 50..=63, hex-encoded, each
//! chunk a string. We reconstruct, hash-verify with blake2b
//! against the output's datum hash, then decode the PlutusData.
//!
//! Lifted from the existing `jpg-co` module with minor adapt
//! (rename `JpgCoVersion → JpgStoreOfferVersion`, finer event
//! surface on consume).
//!
//! ## Consume-side discrimination
//!
//! With the platform's redeemer-data path now correctly
//! delivering just the `PlutusData` bytes (see
//! `block_events.rs` canonical-order fix + data-only encoding),
//! the jpg.store offer contract uses **inverted** redeemer
//! semantics vs the sale contract:
//!
//! - Redeemer `d87980` (constructor 0, empty) → Cancel
//! - Redeemer `d87a80` (constructor 1, empty) → Accept
//!
//! (Sale contracts use the opposite mapping: 0 = Buy/Sale,
//! 1 = Cancel. Empirically verified against mainnet TXs in
//! the golden fixtures.)
//!
//! If the consume's TX also produces an output at the same
//! offer-script address for the same bidder, the consume is
//! reclassified as `Update` (cancel + recreate atomic).

use std::cell::RefCell;
use std::collections::BTreeMap;

use mitos_community_events::jpg_store_offer::{
    JpgStoreOffer, JpgStoreOfferVersion, OfferAccept, OfferCancel, OfferCreate, OfferUpdate,
};
use pallas_codec::minicbor::data::Type;
use pallas_crypto::hash::Hasher;
use pallas_primitives::{Constr, PlutusData};
use serde::Deserialize;

use crate::mitos::platform_v2::chain_data;
use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::types::{
    ConsumedEvent, ProducedEvent, TypedDatum, UtxoEvent,
};

const LOG_TARGET: &str = "jpg-store-offer-module";

#[derive(Debug, Clone, Deserialize)]
struct Config {
    #[serde(default)]
    v2_address: String,
    #[serde(default)]
    v3_address: String,
}

// Addresses are configured at init time from the manifest's
// `[interest]` section. RefCell since the wasm world is
// single-threaded.
thread_local! {
    static V2_ADDRESS: RefCell<String> = const { RefCell::new(String::new()) };
    static V3_ADDRESS: RefCell<String> = const { RefCell::new(String::new()) };
}

fn watched_version(addr: &str) -> Option<JpgStoreOfferVersion> {
    if V2_ADDRESS.with(|a| a.borrow().as_str() == addr) {
        return Some(JpgStoreOfferVersion::V2);
    }
    if V3_ADDRESS.with(|a| a.borrow().as_str() == addr) {
        return Some(JpgStoreOfferVersion::V3);
    }
    None
}

#[derive(Debug, Clone)]
struct DecodedOfferDatum {
    bidder_pkh: String,
    target_policy: Option<String>,
    target_asset_names: Vec<String>,
}

/// Decode a jpg.store CO datum (same wire shape as the
/// existing `jpg-co` module).
fn decode_offer_datum(cbor: &[u8]) -> Option<DecodedOfferDatum> {
    let pd: PlutusData = pallas_codec::minicbor::decode(cbor).ok()?;
    let constr = match pd {
        PlutusData::Constr(c) => c,
        _ => return None,
    };
    if !is_constructor_zero(&constr) {
        return None;
    }
    let fields: Vec<PlutusData> = constr.fields.into();
    if fields.len() != 2 {
        return None;
    }
    let bidder_pkh = match &fields[0] {
        PlutusData::BoundedBytes(b) if b.len() == 28 => hex::encode(&**b),
        _ => return None,
    };
    let (target_policy, target_asset_names) = match &fields[1] {
        PlutusData::Array(payouts) => payouts
            .last()
            .map(extract_target_policy_and_assets)
            .unwrap_or_default(),
        _ => Default::default(),
    };
    Some(DecodedOfferDatum {
        bidder_pkh,
        target_policy,
        target_asset_names,
    })
}

fn is_constructor_zero(c: &Constr<PlutusData>) -> bool {
    c.tag == 121 && c.any_constructor.is_none()
}

fn extract_target_policy_and_assets(payout: &PlutusData) -> (Option<String>, Vec<String>) {
    let constr = match payout {
        PlutusData::Constr(c) => c,
        _ => return Default::default(),
    };
    if !is_constructor_zero(constr) || constr.fields.len() != 2 {
        return Default::default();
    }
    let pairs = match &constr.fields[1] {
        PlutusData::Map(m) => m,
        _ => return Default::default(),
    };
    for (k, v) in pairs.iter() {
        let PlutusData::BoundedBytes(policy_bytes) = k else {
            continue;
        };
        if policy_bytes.len() != 28 {
            continue;
        }
        let policy_hex = hex::encode(&**policy_bytes);
        let asset_names = extract_asset_names(v);
        return (Some(policy_hex), asset_names);
    }
    Default::default()
}

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

/// Resolve datum bytes for an event. Host populates `payload`
/// when it could resolve (inline / witness-set); when empty,
/// we fall back to jpg.store's labels-50+ aux-data convention
/// and hash-verify candidate reconstructions.
fn resolve_datum_bytes(
    tx_hash: &[u8],
    datum_hash: &[u8],
    payload: &[u8],
) -> Option<Vec<u8>> {
    if !payload.is_empty() {
        return Some(payload.to_vec());
    }
    let aux = chain_data::tx_metadata(tx_hash)?;
    for candidate in parse_metadata_datums(&aux) {
        if candidate.len() % 2 != 0
            || !candidate.bytes().all(|b| b.is_ascii_hexdigit())
        {
            continue;
        }
        let bytes = hex::decode(&candidate).ok()?;
        let h = Hasher::<256>::hash(&bytes);
        if h.as_ref() == datum_hash {
            return Some(bytes);
        }
    }
    None
}

/// Walk aux-data for jpg.store's labels-50+ chunked-hex
/// convention. Same parser as `jpg-co::parse_metadata_datums`.
fn parse_metadata_datums(aux_cbor: &[u8]) -> Vec<String> {
    let mut entries: Vec<(u64, String)> = Vec::new();
    if extract_metadata_entries(aux_cbor, &mut entries).is_err() {
        return Vec::new();
    }
    entries.sort_by_key(|(k, _)| *k);

    let mut datums = Vec::new();
    let mut current = String::new();
    for (label, val) in entries {
        if label < 50 {
            continue;
        }
        if val.contains("::") {
            continue;
        }
        if let Some((prefix, _)) = val.split_once(',') {
            if !prefix.is_empty() {
                current.push_str(prefix);
            }
            if !current.is_empty() {
                datums.push(std::mem::take(&mut current));
            }
        } else {
            current.push_str(&val);
        }
    }
    if !current.is_empty() {
        datums.push(current);
    }
    datums
}

fn extract_metadata_entries(
    aux_cbor: &[u8],
    out: &mut Vec<(u64, String)>,
) -> Result<(), pallas_codec::minicbor::decode::Error> {
    let mut d = pallas_codec::minicbor::Decoder::new(aux_cbor);

    if d.datatype()? == Type::Tag {
        let _tag = d.tag()?;
        let outer_len = d.map()?;
        let mut found = false;
        let mut i = 0u64;
        loop {
            if let Some(n) = outer_len
                && i >= n
            {
                break;
            }
            if outer_len.is_none() && d.datatype()? == Type::Break {
                d.skip()?;
                break;
            }
            let key: u64 = d.u64()?;
            if key == 0 {
                found = true;
                break;
            }
            d.skip()?;
            i += 1;
        }
        if !found {
            return Ok(());
        }
    }

    let map_len = d.map()?;
    let mut i = 0u64;
    loop {
        if let Some(n) = map_len
            && i >= n
        {
            break;
        }
        if map_len.is_none() && d.datatype()? == Type::Break {
            d.skip()?;
            break;
        }
        let label: u64 = d.u64()?;
        match d.datatype()? {
            Type::String => {
                let s: &str = d.str()?;
                out.push((label, s.to_owned()));
            }
            _ => {
                d.skip()?;
            }
        }
        i += 1;
    }
    Ok(())
}

// ============================================================
// In-batch correlation buffer
// ============================================================

/// One Consumed event at a watched offer address. Buffered
/// during `handle_events` so we can pair it with Produced
/// events at the same address (= Update) before deciding
/// whether to emit Cancel or Accept.
#[derive(Clone)]
struct ConsumedOffer {
    prior_tx_hash: Vec<u8>,
    prior_output_index: u32,
    prior_datum_bytes: Vec<u8>,
    prior_lovelace: u64,
    redeemer: Option<Vec<u8>>,
    co_version: JpgStoreOfferVersion,
    decoded: DecodedOfferDatum,
}

/// One Produced event at a watched offer address. Used both
/// for the OfferCreate emission and to spot Update pairs.
#[derive(Clone)]
struct ProducedOffer {
    tx_hash: Vec<u8>,
    output_index: u32,
    lovelace: u64,
    datum_bytes: Vec<u8>,
    co_version: JpgStoreOfferVersion,
    decoded: DecodedOfferDatum,
}

/// Captured Produced events at non-script addresses. Used at
/// Accept-time to find which output received the offer's
/// target asset (= the bidder receiving their purchased NFT).
#[derive(Clone)]
struct ProducedNonScript {
    address: String,
    assets: Vec<(Vec<u8>, Vec<u8>)>,
    lovelace: u64,
}

#[derive(Default)]
struct TxBuffer {
    /// Consumed offers keyed by `bidder_pkh` — pair with
    /// `produced_offers` for Update detection.
    consumed_offers: BTreeMap<String, ConsumedOffer>,
    /// Produced offers keyed by `bidder_pkh`.
    produced_offers: BTreeMap<String, ProducedOffer>,
    /// Produced outputs at non-script addresses (potential
    /// accept buyers).
    produced_other: Vec<ProducedNonScript>,
    /// tx_hash of the consuming/producing TX (all events in
    /// one `handle_events` call belong to the same TX).
    tx_hash: Option<Vec<u8>>,
}

fn handle_produced(p: &ProducedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(p.tx_hash.clone());
    }
    // Watched address → record as a produced offer.
    if let Some(version) = watched_version(&p.output.address) {
        let Some(typed_datum) = p.datum.as_ref() else {
            return;
        };
        let Some(datum_bytes) =
            resolve_datum_bytes(&p.tx_hash, &typed_datum.hash, &typed_datum.payload)
        else {
            return;
        };
        let Some(decoded) = decode_offer_datum(&datum_bytes) else {
            return;
        };
        buf.produced_offers.insert(
            decoded.bidder_pkh.clone(),
            ProducedOffer {
                tx_hash: p.tx_hash.clone(),
                output_index: p.oref.index,
                lovelace: p.output.lovelace,
                datum_bytes,
                co_version: version,
                decoded,
            },
        );
        return;
    }
    // Non-script address → record as a potential
    // accept-buyer output.
    let assets: Vec<(Vec<u8>, Vec<u8>)> = p
        .output
        .assets
        .iter()
        .map(|a| (a.asset.policy.clone(), a.asset.name.clone()))
        .collect();
    if !assets.is_empty() {
        buf.produced_other.push(ProducedNonScript {
            address: p.output.address.clone(),
            assets,
            lovelace: p.output.lovelace,
        });
    }
}

fn handle_consumed(c: &ConsumedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(c.consuming_tx_hash.clone());
    }
    let Some(version) = watched_version(&c.prior_output.address) else {
        return;
    };
    let Some(prior_datum) = c.prior_datum.as_ref() else {
        return;
    };
    let Some(datum_bytes) = resolve_datum_bytes(
        &c.oref.tx_hash,
        &prior_datum.hash,
        &prior_datum.payload,
    ) else {
        return;
    };
    let Some(decoded) = decode_offer_datum(&datum_bytes) else {
        return;
    };
    buf.consumed_offers.insert(
        decoded.bidder_pkh.clone(),
        ConsumedOffer {
            prior_tx_hash: c.oref.tx_hash.clone(),
            prior_output_index: c.oref.index,
            prior_datum_bytes: datum_bytes,
            prior_lovelace: c.prior_output.lovelace,
            redeemer: c.redeemer.clone(),
            co_version: version,
            decoded,
        },
    );
}

fn flush_buffer(mut buf: TxBuffer) {
    let Some(tx_hash) = buf.tx_hash.take() else {
        return;
    };
    let tx_hash_hex = hex::encode(&tx_hash);

    // Step 1: walk consumed offers. Each one is either an
    // Update (paired with a produced offer for the same
    // bidder), an Accept (target asset appears at a non-script
    // output), or a Cancel.
    let consumed = std::mem::take(&mut buf.consumed_offers);
    for (bidder_pkh, consume) in consumed {
        // Update path: same bidder has a Produced offer in
        // this TX → consume the produced entry too so we
        // don't also emit OfferCreate for it.
        if let Some(produced) = buf.produced_offers.remove(&bidder_pkh) {
            emit_offer(&JpgStoreOffer::Update(OfferUpdate {
                bidder_pkh: bidder_pkh.clone(),
                tx_hash: tx_hash_hex.clone(),
                prior_tx_hash: hex::encode(&consume.prior_tx_hash),
                prior_output_index: consume.prior_output_index,
                new_output_index: produced.output_index,
                previous_lovelace: consume.prior_lovelace,
                new_lovelace: produced.lovelace,
                datum_cbor: produced.datum_bytes,
                target_policy: produced.decoded.target_policy,
                target_asset_names: produced.decoded.target_asset_names,
                co_version: produced.co_version,
            }));
            continue;
        }

        // Accept vs Cancel discrimination. jpg.store offer
        // contracts use constructor 0 (`d87980`) for Cancel and
        // constructor 1 (`d87a80`) for Accept — opposite of
        // the sale-contract convention.
        let is_cancel = consume
            .redeemer
            .as_ref()
            .is_some_and(|r| r.as_slice() == [0xd8, 0x79, 0x80]);
        if is_cancel {
            emit_offer(&JpgStoreOffer::Cancel(OfferCancel {
                bidder_pkh,
                tx_hash: tx_hash_hex.clone(),
                prior_tx_hash: hex::encode(&consume.prior_tx_hash),
                prior_output_index: consume.prior_output_index,
                target_policy: consume.decoded.target_policy,
                co_version: consume.co_version,
            }));
            continue;
        }

        // Accept: find the produced non-script output that
        // received an asset matching the offer's target. For
        // a collection-wide offer (no asset names), any asset
        // under `target_policy` qualifies; for an asset-
        // specific offer the asset_name must be in the allow-
        // list.
        let target_policy = match &consume.decoded.target_policy {
            Some(p) => p.clone(),
            None => {
                // No target policy means the datum's payouts
                // didn't yield one — fall through to a
                // policy-less Accept emission so consumers
                // see the lifecycle event even without the
                // delivered asset details.
                emit_accept_partial(
                    &bidder_pkh,
                    &tx_hash_hex,
                    &consume,
                );
                continue;
            }
        };
        let target_policy_bytes = match hex::decode(&target_policy) {
            Ok(b) => b,
            Err(_) => {
                emit_accept_partial(&bidder_pkh, &tx_hash_hex, &consume);
                continue;
            }
        };
        let target_asset_set: Option<Vec<Vec<u8>>> =
            if consume.decoded.target_asset_names.is_empty() {
                None
            } else {
                Some(
                    consume
                        .decoded
                        .target_asset_names
                        .iter()
                        .filter_map(|n| hex::decode(n).ok())
                        .collect(),
                )
            };
        let mut emitted = false;
        for out in &buf.produced_other {
            for (policy, name) in &out.assets {
                if policy.as_slice() != target_policy_bytes.as_slice() {
                    continue;
                }
                if let Some(ref set) = target_asset_set
                    && !set.iter().any(|n| n.as_slice() == name.as_slice())
                {
                    continue;
                }
                emit_offer(&JpgStoreOffer::Accept(OfferAccept {
                    bidder_pkh: bidder_pkh.clone(),
                    tx_hash: tx_hash_hex.clone(),
                    prior_tx_hash: hex::encode(&consume.prior_tx_hash),
                    prior_output_index: consume.prior_output_index,
                    policy: target_policy.clone(),
                    asset_name_hex: hex::encode(name),
                    price_lovelace: consume.prior_lovelace,
                    seller_address: out.address.clone(),
                    co_version: consume.co_version,
                }));
                emitted = true;
                break;
            }
            if emitted {
                break;
            }
        }
        if !emitted {
            // Couldn't match the asset — emit a partial
            // Accept so the lifecycle event isn't lost.
            emit_accept_partial(&bidder_pkh, &tx_hash_hex, &consume);
        }
    }

    // Step 2: walk remaining produced offers (no paired
    // consume → fresh OfferCreate).
    let produced = std::mem::take(&mut buf.produced_offers);
    for (bidder_pkh, p) in produced {
        emit_offer(&JpgStoreOffer::Create(OfferCreate {
            bidder_pkh,
            tx_hash: hex::encode(&p.tx_hash),
            output_index: p.output_index,
            lovelace: p.lovelace,
            datum_cbor: p.datum_bytes,
            target_policy: p.decoded.target_policy,
            target_asset_names: p.decoded.target_asset_names,
            co_version: p.co_version,
        }));
    }
}

/// Emit a best-effort Accept when the produced asset can't be
/// matched. Keeps the lifecycle event observable; consumers
/// can still pivot on bidder + tx_hash, just won't know what
/// the bidder received without an external lookup.
fn emit_accept_partial(bidder_pkh: &str, tx_hash_hex: &str, consume: &ConsumedOffer) {
    emit_offer(&JpgStoreOffer::Accept(OfferAccept {
        bidder_pkh: bidder_pkh.to_owned(),
        tx_hash: tx_hash_hex.to_owned(),
        prior_tx_hash: hex::encode(&consume.prior_tx_hash),
        prior_output_index: consume.prior_output_index,
        policy: String::new(),
        asset_name_hex: String::new(),
        price_lovelace: consume.prior_lovelace,
        seller_address: String::new(),
        co_version: consume.co_version,
    }));
    let _ = consume.prior_datum_bytes.len();
}

fn emit_offer(event: &JpgStoreOffer) {
    let mut buf = Vec::new();
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Warn,
            LOG_TARGET,
            &format!("emit serialize failed: {e}"),
        );
        return;
    }
    emit::emit_event(0, &buf);
}

// ============================================================
// v2 Guest impl
// ============================================================

struct Module;

impl Guest for Module {
    fn module_version() -> (u32, u32) {
        (2, 0)
    }

    fn trap_policy() -> (TrapStrategy, RetryPolicy) {
        (
            TrapStrategy::Replay,
            RetryPolicy {
                max_retries: 3,
                backoff_cap_ms: 1_000,
            },
        )
    }

    fn init(config: Vec<u8>) {
        logging::log(
            LogLevel::Info,
            LOG_TARGET,
            &format!("init: enter (config_bytes={})", config.len()),
        );
        if config.is_empty() {
            return;
        }
        let cfg: Config = match ciborium::de::from_reader(config.as_slice()) {
            Ok(c) => c,
            Err(e) => {
                logging::log(
                    LogLevel::Error,
                    LOG_TARGET,
                    &format!("init: decode config failed: {e}"),
                );
                return;
            }
        };
        V2_ADDRESS.with(|a| *a.borrow_mut() = cfg.v2_address);
        V3_ADDRESS.with(|a| *a.borrow_mut() = cfg.v3_address);
        logging::log(LogLevel::Info, LOG_TARGET, "init: addresses stored");
    }

    fn handle_events(events: Vec<DispatchEvent>) {
        let mut buf = TxBuffer::default();
        for event in events {
            match event {
                DispatchEvent::Utxo(UtxoEvent::Produced(p)) => handle_produced(&p, &mut buf),
                DispatchEvent::Utxo(UtxoEvent::Consumed(c)) => handle_consumed(&c, &mut buf),
                _ => {}
            }
        }
        flush_buffer(buf);
    }

    fn update_interest(_op: InterestOp, _items_cbor: Vec<u8>) -> Result<(), String> {
        Ok(())
    }
}

export!(Module);
