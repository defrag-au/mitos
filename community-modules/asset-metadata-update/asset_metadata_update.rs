//! Asset metadata-update community module — emits an
//! `AssetMetadataUpdate` event when an existing asset's
//! metadata changes (not the initial mint).
//!
//! ## Detection paths
//!
//! Two refresh patterns, dispatched as separate enum variants:
//!
//! - **CIP-25 refresh**: a `Minted` event with
//!   `quantity_delta <= 0` (burn or net-zero re-mint cycle) for
//!   which the TX's label-721 aux-data carries metadata for the
//!   asset. The label-721-on-burn pattern is the historical hack
//!   used by chain indexers to rotate NFT metadata without
//!   transferring ownership.
//! - **CIP-68 refresh**: a TX that *consumes* an output holding
//!   a `_100`-prefixed reference asset and *produces* a new
//!   output for the same `(policy, asset_name)` carrying a
//!   different datum. No mint entry needed — the reference
//!   asset moves between UTxOs and the datum is the metadata.
//!
//! ## Buffering
//!
//! Within one `handle-events` invocation (= one TX):
//!
//! 1. Walk `Minted`. Stash non-positive entries that have a
//!    label-721 entry for emission as `Cip25` variants. Also
//!    track the set of `(policy, asset_name)` with positive mint
//!    — used to exclude fresh CIP-68 mints from the refresh
//!    path.
//! 2. Walk `Consumed`. For prior outputs containing a `_100`-
//!    prefixed asset, capture `(policy, asset_name) → datum`.
//! 3. Walk `Produced`. For outputs containing a `_100`-prefixed
//!    asset, capture `(policy, asset_name) → datum`.
//! 4. Flush. For each `(policy, asset_name)` present in both
//!    Consumed and Produced where the datums differ and the
//!    asset was not freshly minted in this TX, emit a `Cip68`
//!    variant.
//!
//! ## Interest
//!
//! Empty default — companion declares policies via
//! `/api/_interest/asset-metadata-update/subscribe` (kind =
//! "policy"). The platform's `holds-policy` predicate delivers
//! every event variant for TXs touching the watched policy, so
//! both the CIP-25 mint-burn signal and the CIP-68 ref-respend
//! signal arrive in one batch.
//!
//! See `mitos/docs/design/MINT_BURN_MODULES.md`.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use mitos_community_events::asset_metadata_update::AssetMetadataUpdate;
use minicbor::data::Type;
use pallas_primitives::PlutusData;

use crate::mitos::platform_v2::chain_data;
use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::types::{
    ConsumedEvent, MintedEvent, ProducedEvent, TypedDatum, UtxoEvent,
};

const LOG_TARGET: &str = "asset-metadata-update-module";

// CIP-67 label tag for CIP-68 reference tokens.
const CIP67_LABEL_REFERENCE: u32 = 100;

/// Parse the CIP-67 label prefix on an asset-name. Returns
/// `Some((label, suffix))` for correctly-prefixed names.
fn parse_cip67(name: &[u8]) -> Option<(u32, &[u8])> {
    if name.len() < 4 || name[0] != 0 {
        return None;
    }
    let label = u32::from(name[1]) << 8 | u32::from(name[2]);
    Some((label, &name[4..]))
}

#[derive(Clone)]
struct DatumPayload {
    hash: Vec<u8>,
    payload: Vec<u8>,
}

impl From<&TypedDatum> for DatumPayload {
    fn from(d: &TypedDatum) -> Self {
        Self {
            hash: d.hash.clone(),
            payload: d.payload.clone(),
        }
    }
}

#[derive(Default)]
struct TxBuffer {
    /// `_100`-asset datums captured from `Consumed.prior-datum`.
    consumed_refs: HashMap<(Vec<u8>, Vec<u8>), DatumPayload>,
    /// `_100`-asset datums captured from `Produced.datum`.
    produced_refs: HashMap<(Vec<u8>, Vec<u8>), DatumPayload>,
    /// Assets with positive `quantity_delta` mint entries in
    /// this TX — used to exclude fresh CIP-68 mints from the
    /// refresh path.
    minted_positive: HashSet<(Vec<u8>, Vec<u8>)>,
    /// tx_hash from the first event seen in this batch.
    tx_hash: Option<Vec<u8>>,
}

// Per-handler aux-data cache: same TX's events share one
// `tx-metadata` lookup.
thread_local! {
    static AUX_CACHE: RefCell<HashMap<Vec<u8>, Option<Vec<u8>>>> = RefCell::new(HashMap::new());
}

fn lookup_aux_data(tx_hash: &[u8]) -> Option<Vec<u8>> {
    AUX_CACHE.with(|cache| {
        let mut c = cache.borrow_mut();
        if let Some(cached) = c.get(tx_hash) {
            return cached.clone();
        }
        let fetched = chain_data::tx_metadata(&tx_hash.to_vec());
        c.insert(tx_hash.to_vec(), fetched.clone());
        fetched
    })
}

fn emit_event(event: &AssetMetadataUpdate) {
    let mut buf = Vec::with_capacity(512);
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode AssetMetadataUpdate failed: {e}"),
        );
        return;
    }
    emit::emit_event(0, &buf);
}

fn resolve_datum_bytes(d: &DatumPayload) -> Option<Vec<u8>> {
    if !d.payload.is_empty() {
        return Some(d.payload.clone());
    }
    chain_data::datum_by_hash(&d.hash)
}

// ============================================================
// CIP-25 metadata extraction (label-721 in aux-data CBOR)
// ============================================================

fn extract_cip25_metadata(aux: &[u8], policy: &[u8], asset_name: &[u8]) -> Option<String> {
    // Aux-data wrapper shapes — see cip_25_mint.rs docstring for
    // detail. Shelley/Allegra/Mary use the array form; Alonzo+
    // wraps in CBOR tag 259 + an int-keyed map (metadata is at
    // key 0); pre-Shelley test fixtures may be bare.
    let mut d = minicbor::Decoder::new(aux);

    let was_tagged = if d.datatype().ok()? == Type::Tag {
        d.tag().ok()?;
        true
    } else {
        false
    };

    let dt = d.datatype().ok()?;
    if dt == Type::Array || dt == Type::ArrayIndef {
        let _arr = d.array().ok()?;
        if d.datatype().ok()? == Type::Null {
            return None;
        }
    } else if was_tagged {
        let len = d.map().ok()?;
        let mut i = 0u64;
        let mut found_0 = false;
        loop {
            if let Some(n) = len
                && i >= n
            {
                break;
            }
            if len.is_none() && d.datatype().ok()? == Type::Break {
                d.skip().ok()?;
                break;
            }
            let key: u64 = d.u64().ok()?;
            if key == 0 {
                found_0 = true;
                break;
            }
            d.skip().ok()?;
            i += 1;
        }
        if !found_0 {
            return None;
        }
    }

    let len = d.map().ok()?;
    let mut i = 0u64;
    let mut found_721 = false;
    loop {
        if let Some(n) = len
            && i >= n
        {
            break;
        }
        if len.is_none() && d.datatype().ok()? == Type::Break {
            d.skip().ok()?;
            break;
        }
        let key: u64 = d.u64().ok()?;
        if key == 721 {
            found_721 = true;
            break;
        }
        d.skip().ok()?;
        i += 1;
    }
    if !found_721 {
        return None;
    }

    let policy_hex = hex::encode(policy);
    let policy_bytes = policy.to_vec();
    let value = find_in_map_by_key_str_or_bytes(&mut d, &policy_hex, &policy_bytes)?;

    let mut inner = minicbor::Decoder::new(&value);
    let asset_name_utf8 = std::str::from_utf8(asset_name).ok();
    let asset_name_bytes = asset_name.to_vec();
    let asset_value = match asset_name_utf8 {
        Some(s) => find_in_map_by_key_str_or_bytes(&mut inner, s, &asset_name_bytes)?,
        None => find_in_map_by_key_str_or_bytes(&mut inner, "", &asset_name_bytes)?,
    };

    cbor_to_json_string(&asset_value).ok()
}

fn find_in_map_by_key_str_or_bytes(
    d: &mut minicbor::Decoder<'_>,
    as_str: &str,
    as_bytes: &[u8],
) -> Option<Vec<u8>> {
    let len = d.map().ok()?;
    let mut i = 0u64;
    loop {
        if let Some(n) = len
            && i >= n
        {
            return None;
        }
        if len.is_none() && d.datatype().ok()? == Type::Break {
            d.skip().ok()?;
            return None;
        }
        let key_match = match d.datatype().ok()? {
            Type::String => {
                let k: &str = d.str().ok()?;
                k == as_str
            }
            Type::Bytes => {
                let k: &[u8] = d.bytes().ok()?;
                k == as_bytes
            }
            _ => {
                d.skip().ok()?;
                d.skip().ok()?;
                i += 1;
                continue;
            }
        };
        let value_start = d.position();
        d.skip().ok()?;
        let value_end = d.position();
        if key_match {
            return Some(d.input()[value_start..value_end].to_vec());
        }
        i += 1;
    }
}

fn cbor_to_json_string(cbor: &[u8]) -> Result<String, String> {
    let mut d = minicbor::Decoder::new(cbor);
    let value = cbor_to_json_value(&mut d)?;
    serde_json::to_string(&value).map_err(|e| format!("json: {e}"))
}

fn cbor_to_json_value(d: &mut minicbor::Decoder<'_>) -> Result<serde_json::Value, String> {
    use serde_json::Value;
    let dt = d.datatype().map_err(|e| format!("dt: {e}"))?;
    Ok(match dt {
        Type::U8 | Type::U16 | Type::U32 | Type::U64 => {
            Value::Number(d.u64().map_err(|e| format!("u64: {e}"))?.into())
        }
        Type::I8 | Type::I16 | Type::I32 | Type::I64 => {
            let v = d.i64().map_err(|e| format!("i64: {e}"))?;
            Value::Number(v.into())
        }
        Type::F16 | Type::F32 | Type::F64 => {
            let f = d.f64().map_err(|e| format!("f64: {e}"))?;
            serde_json::Number::from_f64(f)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        Type::Bool => Value::Bool(d.bool().map_err(|e| format!("bool: {e}"))?),
        Type::Null | Type::Undefined => {
            d.skip().map_err(|e| format!("skip null: {e}"))?;
            Value::Null
        }
        Type::String | Type::StringIndef => {
            let s: String = d.str().map_err(|e| format!("str: {e}"))?.to_owned();
            Value::String(s)
        }
        Type::Bytes | Type::BytesIndef => {
            let bytes: Vec<u8> = d.bytes().map_err(|e| format!("bytes: {e}"))?.to_vec();
            match std::str::from_utf8(&bytes) {
                Ok(s) => Value::String(s.to_owned()),
                Err(_) => Value::String(format!("0x{}", hex::encode(&bytes))),
            }
        }
        Type::Array | Type::ArrayIndef => {
            let len = d.array().map_err(|e| format!("array: {e}"))?;
            let mut out = Vec::new();
            let mut i = 0u64;
            loop {
                if let Some(n) = len
                    && i >= n
                {
                    break;
                }
                if len.is_none() && d.datatype().map_err(|e| format!("dt: {e}"))? == Type::Break {
                    d.skip().map_err(|e| format!("skip break: {e}"))?;
                    break;
                }
                out.push(cbor_to_json_value(d)?);
                i += 1;
            }
            Value::Array(out)
        }
        Type::Map | Type::MapIndef => {
            let len = d.map().map_err(|e| format!("map: {e}"))?;
            let mut obj = serde_json::Map::new();
            let mut i = 0u64;
            loop {
                if let Some(n) = len
                    && i >= n
                {
                    break;
                }
                if len.is_none() && d.datatype().map_err(|e| format!("dt: {e}"))? == Type::Break {
                    d.skip().map_err(|e| format!("skip break: {e}"))?;
                    break;
                }
                let key_dt = d.datatype().map_err(|e| format!("key dt: {e}"))?;
                let key = match key_dt {
                    Type::String => d.str().map_err(|e| format!("key str: {e}"))?.to_owned(),
                    Type::Bytes => {
                        let b: &[u8] = d.bytes().map_err(|e| format!("key bytes: {e}"))?;
                        std::str::from_utf8(b)
                            .map(|s| s.to_owned())
                            .unwrap_or_else(|_| format!("0x{}", hex::encode(b)))
                    }
                    Type::U8 | Type::U16 | Type::U32 | Type::U64 => {
                        d.u64().map_err(|e| format!("key u64: {e}"))?.to_string()
                    }
                    Type::I8 | Type::I16 | Type::I32 | Type::I64 => {
                        d.i64().map_err(|e| format!("key i64: {e}"))?.to_string()
                    }
                    _ => {
                        d.skip().map_err(|e| format!("skip oddkey: {e}"))?;
                        d.skip().map_err(|e| format!("skip oddval: {e}"))?;
                        i += 1;
                        continue;
                    }
                };
                obj.insert(key, cbor_to_json_value(d)?);
                i += 1;
            }
            Value::Object(obj)
        }
        Type::Tag => {
            d.tag().map_err(|e| format!("tag: {e}"))?;
            cbor_to_json_value(d)?
        }
        _ => {
            d.skip().map_err(|e| format!("skip unknown: {e}"))?;
            Value::Null
        }
    })
}

// ============================================================
// CIP-68 metadata extraction (Constructor-0 PlutusData)
// ============================================================

fn extract_cip68_metadata_json(datum_cbor: &[u8]) -> Option<String> {
    let pd: PlutusData = pallas_codec::minicbor::decode(datum_cbor).ok()?;
    let constr = match pd {
        PlutusData::Constr(c) => c,
        _ => return None,
    };
    if constr.tag != 121 && constr.any_constructor != Some(0) {
        return None;
    }
    let fields: Vec<PlutusData> = constr.fields.into();
    if fields.is_empty() {
        return None;
    }
    plutus_to_json_string(&fields[0]).ok()
}

fn plutus_to_json_string(pd: &PlutusData) -> Result<String, String> {
    let value = plutus_to_json_value(pd)?;
    serde_json::to_string(&value).map_err(|e| format!("json: {e}"))
}

fn plutus_to_json_value(pd: &PlutusData) -> Result<serde_json::Value, String> {
    use serde_json::Value;
    Ok(match pd {
        PlutusData::Constr(c) => {
            let mut obj = serde_json::Map::new();
            let ctor = c.any_constructor.unwrap_or(0);
            obj.insert(
                "__constructor".to_owned(),
                Value::Number(serde_json::Number::from(ctor)),
            );
            let mut fields_arr = Vec::new();
            for f in c.fields.iter() {
                fields_arr.push(plutus_to_json_value(f)?);
            }
            obj.insert("fields".to_owned(), Value::Array(fields_arr));
            Value::Object(obj)
        }
        PlutusData::Map(entries) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in entries.iter() {
                let key = plutus_key_to_string(k);
                obj.insert(key, plutus_to_json_value(v)?);
            }
            Value::Object(obj)
        }
        PlutusData::BigInt(i) => match i {
            pallas_primitives::BigInt::Int(n) => {
                let raw = i128::from(*n);
                if let Ok(v) = i64::try_from(raw) {
                    Value::Number(v.into())
                } else {
                    Value::String(raw.to_string())
                }
            }
            pallas_primitives::BigInt::BigUInt(b) | pallas_primitives::BigInt::BigNInt(b) => {
                Value::String(format!("0x{}", hex::encode(&**b)))
            }
        },
        PlutusData::BoundedBytes(b) => {
            let bytes: &[u8] = &**b;
            match std::str::from_utf8(bytes) {
                Ok(s) => Value::String(s.to_owned()),
                Err(_) => Value::String(format!("0x{}", hex::encode(bytes))),
            }
        }
        PlutusData::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for item in arr.iter() {
                out.push(plutus_to_json_value(item)?);
            }
            Value::Array(out)
        }
    })
}

fn plutus_key_to_string(pd: &PlutusData) -> String {
    match pd {
        PlutusData::BoundedBytes(b) => match std::str::from_utf8(b) {
            Ok(s) => s.to_owned(),
            Err(_) => format!("0x{}", hex::encode(&**b)),
        },
        PlutusData::BigInt(pallas_primitives::BigInt::Int(n)) => i128::from(*n).to_string(),
        _ => {
            let mut buf = Vec::new();
            if pallas_codec::minicbor::encode(pd, &mut buf).is_ok() {
                format!("0x{}", hex::encode(&buf))
            } else {
                "<unencodable>".to_owned()
            }
        }
    }
}

// ============================================================
// Event handling
// ============================================================

fn handle_minted(m: &MintedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(m.tx_hash.clone());
    }
    if m.quantity_delta > 0 {
        buf.minted_positive
            .insert((m.policy.clone(), m.asset_name.clone()));
        return;
    }
    // Non-positive (burn or net-zero re-mint half): try the
    // CIP-25 refresh path. The cip-25-mint module already
    // handles the positive-mint label-721 path.
    let aux = match lookup_aux_data(&m.tx_hash) {
        Some(a) => a,
        None => return,
    };
    let metadata_json = extract_cip25_metadata(&aux, &m.policy, &m.asset_name);
    if metadata_json.is_none() {
        // Burn with no metadata refresh — that's `standard-burn`'s
        // territory. Nothing to emit here.
        return;
    }
    emit_event(&AssetMetadataUpdate::Cip25 {
        policy: hex::encode(&m.policy),
        asset_name_hex: hex::encode(&m.asset_name),
        tx_hash: hex::encode(&m.tx_hash),
        new_metadata_json: metadata_json,
    });
}

fn handle_consumed(c: &ConsumedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(c.consuming_tx_hash.clone());
    }
    let Some(datum) = c.prior_datum.as_ref() else {
        return;
    };
    for entry in &c.prior_output.assets {
        if let Some((CIP67_LABEL_REFERENCE, _suffix)) = parse_cip67(&entry.asset.name) {
            buf.consumed_refs.insert(
                (entry.asset.policy.clone(), entry.asset.name.clone()),
                DatumPayload::from(datum),
            );
        }
    }
}

fn handle_produced(p: &ProducedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(p.tx_hash.clone());
    }
    let Some(datum) = p.datum.as_ref() else {
        return;
    };
    for entry in &p.output.assets {
        if let Some((CIP67_LABEL_REFERENCE, _suffix)) = parse_cip67(&entry.asset.name) {
            buf.produced_refs.insert(
                (entry.asset.policy.clone(), entry.asset.name.clone()),
                DatumPayload::from(datum),
            );
        }
    }
}

fn flush_cip68_updates(mut buf: TxBuffer) {
    let tx_hash = match buf.tx_hash.take() {
        Some(h) => h,
        None => return,
    };
    let tx_hash_hex = hex::encode(&tx_hash);

    let consumed = std::mem::take(&mut buf.consumed_refs);
    for ((policy, asset_name), prior) in consumed {
        if buf.minted_positive.contains(&(policy.clone(), asset_name.clone())) {
            // Reference asset freshly minted in this TX — that's
            // `cip-68-mint`'s domain, not an update.
            continue;
        }
        let Some(produced) = buf.produced_refs.get(&(policy.clone(), asset_name.clone())) else {
            // No paired Produced — the reference asset was burned
            // (or moved off-platform). Not an update.
            continue;
        };
        let prior_bytes = match resolve_datum_bytes(&prior) {
            Some(b) => b,
            None => continue,
        };
        let new_bytes = match resolve_datum_bytes(produced) {
            Some(b) => b,
            None => continue,
        };
        if prior_bytes == new_bytes {
            // Reference UTxO moved but datum unchanged — not a
            // metadata update (could be a Plutus action that
            // doesn't change metadata).
            continue;
        }
        let previous_metadata_json = extract_cip68_metadata_json(&prior_bytes);
        let new_metadata_json = extract_cip68_metadata_json(&new_bytes);
        emit_event(&AssetMetadataUpdate::Cip68 {
            policy: hex::encode(&policy),
            reference_asset_name_hex: hex::encode(&asset_name),
            tx_hash: tx_hash_hex.clone(),
            previous_datum_cbor: prior_bytes,
            new_datum_cbor: new_bytes,
            previous_metadata_json,
            new_metadata_json,
        });
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
        AUX_CACHE.with(|c| c.borrow_mut().clear());

        let mut buf = TxBuffer::default();
        for event in events {
            match event {
                DispatchEvent::Utxo(UtxoEvent::Minted(m)) => handle_minted(&m, &mut buf),
                DispatchEvent::Utxo(UtxoEvent::Consumed(c)) => handle_consumed(&c, &mut buf),
                DispatchEvent::Utxo(UtxoEvent::Produced(p)) => handle_produced(&p, &mut buf),
                _ => {}
            }
        }
        flush_cip68_updates(buf);
    }

    fn update_interest(_op: InterestOp, _items_cbor: Vec<u8>) -> Result<(), String> {
        Ok(())
    }
}

export!(Module);
