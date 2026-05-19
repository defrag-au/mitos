//! CIP-25 mint community module — emits `Cip25Mint` events for
//! positive `tx.mint` entries on watched policies whose TX
//! carries label-721 metadata for the asset.
//!
//! ## Decode path
//!
//! 1. Receive `MintedEvent` from the platform's pre-filtered
//!    stream (filtered by `holds-policy` for the consumer's
//!    watched policies).
//! 2. If `quantity_delta <= 0`, skip — burns are
//!    `standard-burn`'s concern.
//! 3. Call `chain-data::tx-metadata(tx_hash)` once per matching
//!    TX to retrieve the aux-data CBOR bytes.
//! 4. Walk the aux-data top-level map for key `721u64`.
//! 5. Walk the label-721 sub-map for the asset's policy key
//!    (CIP-25 spec says "policy bytes"; in practice it's the
//!    hex-string representation — try both).
//! 6. Walk the policy sub-map for the asset's name key (again,
//!    try both hex and UTF-8 bytes).
//! 7. Convert the resulting CBOR value to JSON via the local
//!    `cbor_to_json` helper.
//!
//! TX-metadata calls per TX are batched implicitly — multiple
//! `MintedEvent`s for the same `tx_hash` would each trigger one
//! call. For v1 we accept the redundant calls; the data plane
//! lookup is in-process redb so cost is bounded. Future
//! optimisation: cache the aux-data within a `handle-events`
//! invocation since events for one TX arrive together.
//!
//! ## Interest
//!
//! Empty default — companion declares policies via
//! `/api/_interest/cip-25-mint/subscribe` (kind = "policy").
//! Platform's bootstrap synthesises `Produced` events for
//! current UTxOs holding the policy; this module ignores them
//! (mint detection is `Minted`-event-driven, not state-driven —
//! see the bootstrap caveat in `MINT_BURN_MODULES.md`).
//!
//! See `mitos/docs/design/MINT_BURN_MODULES.md`.

use std::cell::RefCell;
use std::collections::HashMap;

use mitos_community_events::cip25_mint::Cip25Mint;
use minicbor::data::Type;

use crate::mitos::platform_v2::chain_data;
use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::types::{MintedEvent, UtxoEvent};

const LOG_TARGET: &str = "cip-25-mint-module";

// Per-handler aux-data cache: `Minted` events for one TX
// arrive in one `handle-events` call, so caching by tx_hash
// across that call avoids redundant `tx-metadata` lookups
// when a TX mints multiple assets.
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

fn emit_cip25_mint(event: &Cip25Mint) {
    let mut buf = Vec::with_capacity(512);
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode Cip25Mint failed: {e}"),
        );
        return;
    }
    emit::emit_event(0, &buf);
}

/// Extract the label-721 entry's metadata for a specific asset.
/// Returns `Some(json)` on success, `None` if the entry isn't
/// present or decoding fails.
///
/// Aux-data wraps the CIP-10-labelled metadata map in one of
/// three era-specific shapes:
///   - Shelley/Allegra/Mary: `[metadata_map, native_scripts]`
///     (array form, element 0 is the metadata map).
///   - Alonzo+/Babbage/Conway: `#6.259({ 0 => metadata_map,
///     1 => native_scripts, 2..4 => plutus scripts })`
///     (CBOR tag 259 + map keyed by ints).
///   - Bare (test fixtures, never seen on chain): metadata map
///     directly.
/// In all cases the inner `metadata_map` is `{ label_u64 => any }`
/// and we want the value at `label = 721`.
fn extract_cip25_metadata(
    aux: &[u8],
    policy: &[u8],
    asset_name: &[u8],
) -> Option<String> {
    let mut d = minicbor::Decoder::new(aux);

    // Peel a leading CBOR tag — when present, it's the Alonzo+
    // `#6.259` discriminator. Tag value isn't validated; any tag
    // is treated as a transparent wrapper, then the inside is
    // expected to be the `{ 0 => metadata, 1 => scripts, ... }`
    // map and we navigate to key 0.
    let was_tagged = if d.datatype().ok()? == Type::Tag {
        d.tag().ok()?;
        true
    } else {
        false
    };

    let dt = d.datatype().ok()?;
    if dt == Type::Array || dt == Type::ArrayIndef {
        // Shelley/Allegra/Mary array form. Element 0 is the
        // metadata map (or null).
        let _arr = d.array().ok()?;
        if d.datatype().ok()? == Type::Null {
            return None;
        }
        // Cursor is now on the inner metadata map.
    } else if was_tagged {
        // Alonzo+ inner map: { 0 => metadata, 1 => scripts, ... }.
        // Walk to find key 0.
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
        // Cursor is now on the metadata sub-map at key 0.
    }
    // Else: bare CIP-10 metadata map at this level — cursor is
    // already on it.

    // Top-level metadata map. Walk to find key = 721.
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

    // Label-721 value is a map keyed by policy. The CIP-25 spec
    // wobbles on whether keys are bytes (hex) or strings; in
    // practice mainnet has both forms. Try both.
    let policy_hex = hex::encode(policy);
    let policy_bytes = policy.to_vec();
    let value = find_in_map_by_key_str_or_bytes(&mut d, &policy_hex, &policy_bytes)?;

    // Inside the policy entry: another map keyed by asset name
    // (again either utf-8 string or raw bytes).
    let mut inner = minicbor::Decoder::new(&value);
    let asset_name_utf8 = std::str::from_utf8(asset_name).ok();
    let asset_name_bytes = asset_name.to_vec();
    let asset_value = match asset_name_utf8 {
        Some(s) => find_in_map_by_key_str_or_bytes(&mut inner, s, &asset_name_bytes)?,
        None => find_in_map_by_key_str_or_bytes(&mut inner, "", &asset_name_bytes)?,
    };

    // Convert the asset's metadata-value CBOR to JSON.
    cbor_to_json_string(&asset_value).ok()
}

/// Walk a CBOR map, find an entry whose key matches either
/// `as_str` or `as_bytes`, return the value's raw CBOR bytes.
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
        // Capture the value's CBOR bytes by snapshotting decoder
        // position before/after `skip`.
        let value_start = d.position();
        d.skip().ok()?;
        let value_end = d.position();
        if key_match {
            return Some(d.input()[value_start..value_end].to_vec());
        }
        i += 1;
    }
}

/// Convert raw CBOR bytes to a JSON string. Mirrors the
/// CBOR-to-JSON conversion used by chain indexers: maps become
/// objects (keys serialised), lists become arrays, bytes become
/// UTF-8 strings if valid otherwise hex-prefixed strings,
/// integers as numbers, floats as numbers, bool/null verbatim.
fn cbor_to_json_string(cbor: &[u8]) -> Result<String, String> {
    let mut d = minicbor::Decoder::new(cbor);
    let value = cbor_to_json_value(&mut d)?;
    serde_json::to_string(&value).map_err(|e| format!("serialise json: {e}"))
}

fn cbor_to_json_value(
    d: &mut minicbor::Decoder<'_>,
) -> Result<serde_json::Value, String> {
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
            serde_json::Number::from_f64(f).map(Value::Number).unwrap_or(Value::Null)
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
            // Prefer UTF-8 string if the bytes are valid;
            // otherwise hex-encode (CIP-25 images / files often
            // carry IPFS-CIDs or URLs as bytes that happen to be
            // valid ASCII).
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
                // Keys: stringify any CBOR value into a flat
                // string so JSON objects are well-formed.
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
                        // Skip the value too — we can't represent
                        // this entry in JSON.
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
            // CBOR tags: pass through to the inner value. CIP-25
            // doesn't use tags meaningfully; treat as transparent.
            d.tag().map_err(|e| format!("tag: {e}"))?;
            cbor_to_json_value(d)?
        }
        _ => {
            d.skip().map_err(|e| format!("skip unknown: {e}"))?;
            Value::Null
        }
    })
}

fn handle_minted(m: &MintedEvent) {
    // Skip burns (those are `standard-burn`'s concern).
    if m.quantity_delta <= 0 {
        return;
    }
    let quantity = match u64::try_from(m.quantity_delta) {
        Ok(q) => q,
        Err(_) => {
            logging::log(
                LogLevel::Error,
                LOG_TARGET,
                &format!("mint quantity overflow: delta={}", m.quantity_delta),
            );
            return;
        }
    };

    // Look up the TX's aux-data. `None` means no metadata
    // present — we still emit the mint event with
    // `metadata_json = None` so consumers observe the mint and
    // can choose whether to backfill metadata out-of-band.
    let metadata_json = lookup_aux_data(&m.tx_hash)
        .as_deref()
        .and_then(|aux| extract_cip25_metadata(aux, &m.policy, &m.asset_name));

    emit_cip25_mint(&Cip25Mint {
        policy: hex::encode(&m.policy),
        asset_name_hex: hex::encode(&m.asset_name),
        tx_hash: hex::encode(&m.tx_hash),
        quantity,
        metadata_json,
    });
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
        // Clear the per-handler aux-data cache so we don't
        // retain entries across invocations (each call holds
        // one TX or a small batch).
        AUX_CACHE.with(|c| c.borrow_mut().clear());

        for event in events {
            if let DispatchEvent::Utxo(UtxoEvent::Minted(m)) = event {
                handle_minted(&m);
            }
        }
    }

    fn update_interest(_op: InterestOp, _items_cbor: Vec<u8>) -> Result<(), String> {
        Ok(())
    }

    /// No-op: event-driven modules are refilled host-side by
    /// `run_bootstrap` over the manifest `[interest]`. See the
    /// `rebootstrap` export in `wit-v2/world.wit`. One call,
    /// immediately `done`.
    fn rebootstrap() -> Result<RebootstrapStep, String> {
        Ok(RebootstrapStep {
            done: true,
            ingested: 0,
        })
    }
}

export!(Module);
