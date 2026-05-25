//! jpg.store sales community module.
//!
//! Emits one `JpgStoreSale::Sale` per completed sale on the
//! jpg.store sale contracts (V1/V2/V3/V4). Listing lifecycle
//! events live in the sibling `jpg-store-listing` module.
//!
//! ## Detection
//!
//! In each `handle_events` call (one TX's events):
//!
//! 1. Walk `Consumed` events. For each one at a jpg.store sale
//!    script address whose redeemer is **constructor 0** (Buy /
//!    Accept; on-wire CBOR starts with `d879…`):
//!    - resolve the prior listing's datum (via the witness set
//!      the sale TX itself reveals)
//!    - extract payouts + seller_pkh from the datum
//!    - record `(policy, asset_name)` in a "needs buyer" set
//! 2. Walk `Produced` events. For each one that contains one of
//!    the "needs buyer" assets at a non-jpg-script address,
//!    record the recipient's address as the buyer.
//! 3. Emit one `Sale` event per matched sale.
//!
//! Constructor 1 redeemers (Cancel/Unlisting) are skipped — that's
//! jpg-store-listing's domain.

use std::collections::BTreeMap;

use mitos_community_events::jpg_store_sale::{
    JpgStoreContractVersion, JpgStoreSale, ListingPayout, Sale,
};
use pallas_primitives::PlutusData;

use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::types::{
    ConsumedEvent, ProducedEvent, TypedDatum, UtxoEvent,
};

const LOG_TARGET: &str = "jpg-store-sale-module";

const JPG_V1_ADDR: &str = "addr1zxgx3far7qygq0k6epa0zcvcvrevmn0ypsnfsue94nsn3tvpw288a4x0xf8pxgcntelxmyclq83s0ykeehchz2wtspks905plm";
const JPG_V2_ADDR: &str = "addr1x8rjw3pawl0kelu4mj3c8x20fsczf5pl744s9mxz9v8n7efvjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8ekstg4qrx";
const JPG_V3_ADDR: &str = "addr1w8rjw3pawl0kelu4mj3c8x20fsczf5pl744s9mxz9v8n7efvjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8ekstg4qrx";
const JPG_V4_ADDR: &str = "addr1w999n67e47he8y0v36hjtzluargwu25zw94f6lqnm82aqqsg4xkcp";

fn classify_address(addr: &str) -> Option<JpgStoreContractVersion> {
    match addr {
        JPG_V1_ADDR => Some(JpgStoreContractVersion::V1),
        JPG_V2_ADDR => Some(JpgStoreContractVersion::V2),
        JPG_V3_ADDR => Some(JpgStoreContractVersion::V3),
        JPG_V4_ADDR => Some(JpgStoreContractVersion::V4),
        _ => None,
    }
}

/// Sale-side state for one consumed listing UTxO. We carry
/// enough to emit the event once we find the buyer in the
/// produced outputs.
#[derive(Clone)]
struct PendingSale {
    seller_pkh: String,
    payouts: Vec<ListingPayout>,
    price_lovelace: u64,
    contract_version: JpgStoreContractVersion,
}

#[derive(Default)]
struct TxBuffer {
    /// `(policy, asset_name)` → PendingSale, populated as we
    /// process Consumed events at jpg-scripts with the Buy
    /// redeemer. Cleared as we match Produced outputs to find
    /// the buyer.
    pending: BTreeMap<(Vec<u8>, Vec<u8>), PendingSale>,
    /// Buffer Produced events so we can walk them at flush time
    /// after all Consumed events have populated `pending`. (v2
    /// dispatch order is referenced → consumed → produced, so
    /// in principle we could match per-Produced live — buffering
    /// keeps the matching logic simple and order-independent.)
    produced: Vec<(String, Vec<(Vec<u8>, Vec<u8>)>)>,
    tx_hash: Option<Vec<u8>>,
}

fn handle_consumed(c: &ConsumedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(c.consuming_tx_hash.clone());
    }
    let Some(version) = classify_address(&c.prior_output.address) else {
        return;
    };
    let Some(redeemer_bytes) = c.redeemer.as_ref() else {
        return;
    };
    if !is_buy_redeemer(redeemer_bytes) {
        return;
    }
    let Some(datum_bytes) = resolve_datum_bytes(c.prior_datum.as_ref()) else {
        logging::log(
            LogLevel::Warn,
            LOG_TARGET,
            "consumed listing has no resolvable datum; skipping sale emit",
        );
        return;
    };
    let Some(decoded) = decode_listing_datum(&datum_bytes) else {
        logging::log(
            LogLevel::Warn,
            LOG_TARGET,
            "consumed listing datum didn't match expected shape; skipping",
        );
        return;
    };
    let price_lovelace = decoded.payouts.iter().map(|p| p.lovelace).sum::<u64>();
    for entry in &c.prior_output.assets {
        buf.pending.insert(
            (entry.asset.policy.clone(), entry.asset.name.clone()),
            PendingSale {
                seller_pkh: decoded.seller_pkh.clone(),
                payouts: decoded.payouts.clone(),
                price_lovelace,
                contract_version: version,
            },
        );
    }
}

fn handle_produced(p: &ProducedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(p.tx_hash.clone());
    }
    // Skip outputs back to any jpg-script — those are either
    // listing updates (handled by jpg-store-listing) or
    // accidental fee sinks. Sales send assets to a non-script
    // address (the buyer).
    if classify_address(&p.output.address).is_some() {
        return;
    }
    let assets: Vec<(Vec<u8>, Vec<u8>)> = p
        .output
        .assets
        .iter()
        .map(|a| (a.asset.policy.clone(), a.asset.name.clone()))
        .collect();
    if !assets.is_empty() {
        buf.produced.push((p.output.address.clone(), assets));
    }
}

fn flush_buffer(buf: TxBuffer) {
    let Some(tx_hash) = buf.tx_hash else {
        return;
    };
    let tx_hash_hex = hex::encode(&tx_hash);
    let mut pending = buf.pending;

    for (buyer_address, assets) in buf.produced {
        for asset_key in assets {
            let Some(sale) = pending.remove(&asset_key) else {
                continue;
            };
            let (policy, asset_name) = asset_key;
            emit_sale(&JpgStoreSale::Sale(Sale {
                policy: hex::encode(&policy),
                asset_name_hex: hex::encode(&asset_name),
                tx_hash: tx_hash_hex.clone(),
                seller_pkh: sale.seller_pkh,
                buyer_address: buyer_address.clone(),
                payouts: sale.payouts,
                price_lovelace: sale.price_lovelace,
                contract_version: sale.contract_version,
            }));
        }
    }

    // Any pending sales we couldn't match to a buyer — log so
    // operators see the gap.
    for ((policy, asset_name), _) in pending {
        logging::log(
            LogLevel::Warn,
            LOG_TARGET,
            &format!(
                "consumed listing {}/{} had Buy redeemer but no produced output matched as buyer",
                hex::encode(&policy),
                hex::encode(&asset_name)
            ),
        );
    }
}

fn emit_sale(event: &JpgStoreSale) {
    let mut buf = Vec::with_capacity(512);
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode JpgStoreSale failed: {e}"),
        );
        return;
    }
    emit::emit_event(0, &buf);
}

/// jpg.store V1/V2/V3 Buy redeemer: constructor 0 with one
/// uint field (typically zero). The on-wire CBOR is the
/// indefinite-length form `d879 9f 00 ff`. We match
/// permissively on "starts with d879" so future redeemer
/// shapes that use the same constructor (e.g. richer fields)
/// still classify as Buy.
fn is_buy_redeemer(bytes: &[u8]) -> bool {
    bytes.starts_with(&[0xd8, 0x79])
}

fn resolve_datum_bytes(d: Option<&TypedDatum>) -> Option<Vec<u8>> {
    let d = d?;
    if !d.payload.is_empty() {
        return Some(d.payload.clone());
    }
    crate::mitos::platform_v2::chain_data::datum_by_hash(&d.hash)
}

struct DecodedListing {
    payouts: Vec<ListingPayout>,
    seller_pkh: String,
}

fn decode_listing_datum(cbor: &[u8]) -> Option<DecodedListing> {
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
        PlutusData::Array(items) => items
            .iter()
            .filter_map(decode_payout)
            .collect::<Vec<ListingPayout>>(),
        _ => return None,
    };
    let seller_pkh = match &fields[1] {
        PlutusData::BoundedBytes(b) => hex::encode(&**b),
        _ => String::new(),
    };
    Some(DecodedListing {
        payouts,
        seller_pkh,
    })
}

fn decode_payout(pd: &PlutusData) -> Option<ListingPayout> {
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

fn decode_bigint_u64(i: &pallas_primitives::BigInt) -> Option<u64> {
    match i {
        pallas_primitives::BigInt::Int(n) => {
            let v = i128::from(*n);
            if v < 0 { None } else { u64::try_from(v).ok() }
        }
        pallas_primitives::BigInt::BigUInt(b) => {
            let bytes: &[u8] = &**b;
            if bytes.len() > 8 {
                return None;
            }
            let mut buf = [0u8; 8];
            buf[8 - bytes.len()..].copy_from_slice(bytes);
            Some(u64::from_be_bytes(buf))
        }
        pallas_primitives::BigInt::BigNInt(_) => None,
    }
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
    }

    fn handle_events(events: Vec<DispatchEvent>) {
        let mut buf = TxBuffer::default();
        for event in events {
            match event {
                DispatchEvent::Utxo(UtxoEvent::Consumed(c)) => handle_consumed(&c, &mut buf),
                DispatchEvent::Utxo(UtxoEvent::Produced(p)) => handle_produced(&p, &mut buf),
                _ => {}
            }
        }
        flush_buffer(buf);
    }

    fn update_interest(_op: InterestOp, _items_cbor: Vec<u8>) -> Result<(), String> {
        Ok(())
    }

    /// No-op: event-driven modules are refilled host-side by
    /// `run_bootstrap` over the manifest `[interest]`. See the
    /// `rebootstrap` export in `wit-v2/world.wit`. One call,
    /// immediately `done`.
    fn rebootstrap(_mode: RebootstrapMode) -> Result<RebootstrapStep, String> {
        Ok(RebootstrapStep {
            done: true,
            ingested: 0,
        })
    }
}

export!(Module);
