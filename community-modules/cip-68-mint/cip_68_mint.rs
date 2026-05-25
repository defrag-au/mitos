//! CIP-68 mint community module — paired reference + user-token
//! mints with decoded reference-token metadata.
//!
//! ## Detection model
//!
//! Within one `handle-events` invocation (= one TX's events):
//!
//! 1. Walk all `Minted` events. For each, check the asset
//!    name's CIP-67 prefix. Stash into one of:
//!    - `ref_mints: (suffix → asset_name + quantity)` for label 100
//!    - `user_mints: (suffix → asset_name + quantity + label)` for 222/333/444
//! 2. Walk all `Produced` events. For each output whose
//!    `assets` contains a `_100`-prefixed asset, capture the
//!    output's datum bytes keyed by suffix.
//! 3. For each `user_mints[suffix]`:
//!    - If `ref_mints[suffix]` is present: pair them. Resolve
//!      the datum (from the captured Produced output or via
//!      `datum-by-hash` if it's hash-only). Decode the CIP-68
//!      Constructor-0 structure for the metadata map.
//!    - Else: emit with `reference = None` (user-only re-mint,
//!      RFT re-stocking scenario).
//!
//! Reference-only mints (initial deploy with no user tokens) are
//! not emitted; they're rare and not in the v1 consumer
//! requirements per `MINT_BURN_MODULES.md`.
//!
//! ## Interest
//!
//! Empty default; companion declares policies via
//! `/api/_interest/cip-68-mint/subscribe` (kind = "policy").
//! Platform's `holds-policy` predicate filters both `Minted`
//! and `Produced` events, so both arrive for the same TX when
//! the TX involves a watched policy.
//!
//! See `mitos/docs/design/MINT_BURN_MODULES.md`.

use std::collections::BTreeMap;

use mitos_community_events::cip68_mint::{Cip68Mint, Cip68RefInfo, Cip68UserInfo};
use pallas_primitives::PlutusData;

use crate::mitos::platform_v2::chain_data;
use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::types::{MintedEvent, ProducedEvent, UtxoEvent};

const LOG_TARGET: &str = "cip-68-mint-module";

// CIP-67 label tags used by CIP-68.
const CIP67_LABEL_REFERENCE: u32 = 100;
const CIP67_LABEL_NFT: u32 = 222;
const CIP67_LABEL_FT: u32 = 333;
const CIP67_LABEL_RFT: u32 = 444;

/// Parse the CIP-67 label prefix on an asset-name. CIP-67's
/// 4-byte prefix is bit-packed:
///
/// ```text
///   bits  0..4   leading zero nibble
///   bits  4..20  16-bit label
///   bits 20..28  8-bit CRC-8 checksum
///   bits 28..32  trailing zero nibble
/// ```
///
/// So for label 100 the bytes are `00 06 43 b0` — the label
/// straddles a nibble boundary across bytes 0..3, not bytes 1
/// and 2 directly. Returns `(label, suffix_bytes)` on success,
/// `None` otherwise. Checksum isn't verified — the chain only
/// produces correctly-formed prefixes; we honour whatever label
/// is encoded.
fn parse_cip67(name: &[u8]) -> Option<(u32, &[u8])> {
    if name.len() < 4 {
        return None;
    }
    // Top nibble of byte 0 must be zero (CIP-67 framing).
    if name[0] & 0xf0 != 0 {
        return None;
    }
    // Bottom nibble of byte 3 must be zero (CIP-67 framing).
    if name[3] & 0x0f != 0 {
        return None;
    }
    let label = (u32::from(name[0] & 0x0f) << 12)
        | (u32::from(name[1]) << 4)
        | (u32::from(name[2]) >> 4);
    Some((label, &name[4..]))
}

struct RefCandidate {
    asset_name: Vec<u8>,
    quantity: u64,
}

struct UserCandidate {
    asset_name: Vec<u8>,
    quantity: u64,
    label: u32,
}

#[derive(Default)]
struct TxBuffer {
    /// Reference-token mints, keyed by (policy, suffix).
    /// `BTreeMap` (not `HashMap`) so the flush iteration order is
    /// deterministic — important for golden testing, replay
    /// equivalence across operators, and downstream consumers
    /// that index by emission order.
    ref_mints: BTreeMap<(Vec<u8>, Vec<u8>), RefCandidate>,
    /// User-token mints, keyed by (policy, suffix). See
    /// `ref_mints` for the BTreeMap rationale.
    user_mints: BTreeMap<(Vec<u8>, Vec<u8>), UserCandidate>,
    /// Datum bytes captured from `Produced` events that contain
    /// a `_100`-prefixed asset, keyed by (policy, full
    /// asset_name of the reference). The full asset_name (not
    /// suffix) is the key because `Produced` events don't
    /// re-derive the suffix — we look up the matching ref
    /// asset_name when emitting.
    ref_datums: BTreeMap<(Vec<u8>, Vec<u8>), DatumPayload>,
    /// tx_hash from the first event seen in this batch (all
    /// events in one `handle-events` call belong to the same
    /// TX).
    tx_hash: Option<Vec<u8>>,
}

#[derive(Clone)]
struct DatumPayload {
    hash: Vec<u8>,
    payload: Vec<u8>,
}

fn emit_cip68_mint(event: &Cip68Mint) {
    let mut buf = Vec::with_capacity(1024);
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode Cip68Mint failed: {e}"),
        );
        return;
    }
    // Key by policy hex so the host's SB6b interest filter fans this
    // mint only to companions watching this policy (a keyless emission
    // is broadcast to every companion — cross-policy pollution).
    emit::emit_event_keyed(0, event.policy.as_bytes(), &buf);
}

/// Resolve datum bytes. If the produced event carried inline
/// datum bytes (`payload` non-empty), use those. Otherwise
/// fall back to `chain-data::datum-by-hash` for hash-only
/// datums.
fn resolve_datum_bytes(d: &DatumPayload) -> Option<Vec<u8>> {
    if !d.payload.is_empty() {
        return Some(d.payload.clone());
    }
    chain_data::datum_by_hash(&d.hash)
}

/// Decode the CIP-68 Constructor-0 structure and extract the
/// metadata map as a JSON-serialised string. Returns `None`
/// if the datum doesn't match the spec.
fn extract_cip68_metadata_json(datum_cbor: &[u8]) -> Option<String> {
    let pd: PlutusData = pallas_codec::minicbor::decode(datum_cbor).ok()?;
    let constr = match pd {
        PlutusData::Constr(c) => c,
        _ => return None,
    };
    // CIP-68 reference datum: Constructor 0 with 3 fields:
    //   [0] metadata map: Map(name_bytes -> any)
    //   [1] version: Int
    //   [2] extra: any
    if constr.tag != 121 && constr.any_constructor != Some(0) {
        return None;
    }
    let fields: Vec<PlutusData> = constr.fields.into();
    if fields.is_empty() {
        return None;
    }
    plutus_to_json_string(&fields[0]).ok()
}

/// Convert a `PlutusData` value to a JSON string. Bytestrings
/// become UTF-8 strings if valid, otherwise hex-prefixed
/// strings. Constructors become objects with a special
/// `__constructor` key. Maps become objects (keys
/// string-coerced). Lists become arrays. Ints become numbers
/// (with i128 fallback to string for the overflow range).
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
        PlutusData::BigInt(i) => {
            // Try i64 first; fall back to string if out of range.
            match i {
                pallas_primitives::BigInt::Int(n) => {
                    let raw = i128::from(*n);
                    if let Ok(v) = i64::try_from(raw) {
                        Value::Number(v.into())
                    } else {
                        Value::String(raw.to_string())
                    }
                }
                pallas_primitives::BigInt::BigUInt(b)
                | pallas_primitives::BigInt::BigNInt(b) => {
                    Value::String(format!("0x{}", hex::encode(&**b)))
                }
            }
        }
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
            // Less common — non-string keys in CIP-68 metadata
            // are spec-violations but we shouldn't lose them.
            let mut buf = Vec::new();
            if pallas_codec::minicbor::encode(pd, &mut buf).is_ok() {
                format!("0x{}", hex::encode(&buf))
            } else {
                "<unencodable>".to_owned()
            }
        }
    }
}

fn handle_minted(m: &MintedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(m.tx_hash.clone());
    }
    if m.quantity_delta <= 0 {
        // Burns: handled by `standard-burn`. Ignored here.
        return;
    }
    let Some((label, suffix)) = parse_cip67(&m.asset_name) else {
        // Non-CIP-67-prefixed mint under a watched policy:
        // could be CIP-25-shape or freeform. Ignored here.
        return;
    };
    let quantity = match u64::try_from(m.quantity_delta) {
        Ok(q) => q,
        Err(_) => return,
    };
    let key = (m.policy.clone(), suffix.to_vec());
    match label {
        CIP67_LABEL_REFERENCE => {
            buf.ref_mints.insert(
                key,
                RefCandidate {
                    asset_name: m.asset_name.clone(),
                    quantity,
                },
            );
        }
        CIP67_LABEL_NFT | CIP67_LABEL_FT | CIP67_LABEL_RFT => {
            buf.user_mints.insert(
                key,
                UserCandidate {
                    asset_name: m.asset_name.clone(),
                    quantity,
                    label,
                },
            );
        }
        _ => {
            // Other CIP-67 labels reserved for future use; skip
            // until they're standardised + a consumer asks.
        }
    }
}

fn handle_produced(p: &ProducedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(p.tx_hash.clone());
    }
    // Look for a `_100`-prefixed asset in this output; capture
    // its datum bytes. Outputs commonly hold exactly one
    // reference asset per UTxO (one ref per family), so we
    // emit one map entry per matching asset found.
    let Some(typed_datum) = p.datum.as_ref() else {
        return;
    };
    for entry in &p.output.assets {
        if let Some((CIP67_LABEL_REFERENCE, _suffix)) = parse_cip67(&entry.asset.name) {
            buf.ref_datums.insert(
                (entry.asset.policy.clone(), entry.asset.name.clone()),
                DatumPayload {
                    hash: typed_datum.hash.clone(),
                    payload: typed_datum.payload.clone(),
                },
            );
        }
    }
}

fn flush_buffer(mut buf: TxBuffer) {
    let tx_hash = match buf.tx_hash.take() {
        Some(h) => h,
        None => return,
    };
    let tx_hash_hex = hex::encode(&tx_hash);

    // Drain user-mints; for each, look for a paired ref-mint by
    // (policy, suffix). The user/ref maps are keyed by
    // (policy, suffix) so the lookup is O(1).
    let user_mints = std::mem::take(&mut buf.user_mints);
    for ((policy, suffix), user) in user_mints {
        let policy_hex = hex::encode(&policy);
        let reference = buf.ref_mints.remove(&(policy.clone(), suffix.clone())).and_then(
            |ref_mint| {
                // Find the datum captured for this ref's full
                // asset_name. None if the Produced event wasn't
                // delivered (rare — would mean the platform
                // filtered the ref output for some reason).
                let datum = buf
                    .ref_datums
                    .get(&(policy.clone(), ref_mint.asset_name.clone()))
                    .cloned();
                let datum_bytes = datum.as_ref().and_then(resolve_datum_bytes);
                let datum_cbor = datum_bytes.clone().unwrap_or_default();
                let metadata_json = datum_bytes.as_deref().and_then(extract_cip68_metadata_json);
                Some(Cip68RefInfo {
                    asset_name_hex: hex::encode(&ref_mint.asset_name),
                    quantity: ref_mint.quantity,
                    datum_cbor,
                    metadata_json,
                })
            },
        );

        // Ensure `suffix` is referenced so the compiler accepts
        // the key destructure for HashMap iteration.
        let _ = suffix;

        emit_cip68_mint(&Cip68Mint {
            policy: policy_hex,
            tx_hash: tx_hash_hex.clone(),
            reference,
            user: Cip68UserInfo {
                asset_name_hex: hex::encode(&user.asset_name),
                quantity: user.quantity,
                cip67_label: user.label,
            },
        });
    }

    // Any remaining ref_mints without a paired user-mint are
    // reference-only mints — skipped in v1 (see module
    // docstring rationale).
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
        // Per `handle-events` call = one TX's events. Buffer
        // state lives in-scope here so no thread-local
        // resets are needed.
        let mut buf = TxBuffer::default();
        for event in events {
            match event {
                DispatchEvent::Utxo(UtxoEvent::Minted(m)) => handle_minted(&m, &mut buf),
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
