//! Collection-metadata community module — emits a per-policy
//! ledger of CIP-68 reference-token metadata and per-TX
//! `Initial` / `Updated` / `Burned` events as ref tokens are
//! minted, datum-rotated, or destroyed.
//!
//! Sibling to `collection-holders`. Same subscription model
//! (`holds_policy(X)`), same chunked-snapshot+delta shape,
//! different projection: this module answers "what is the
//! current metadata for each asset identity" rather than "who
//! holds what".
//!
//! See `docs/design/COLLECTION_MODULES.md` for the design
//! and the CIP-68 facade rationale.
//!
//! ## Phase 2 scope (this file at landing time)
//!
//! - Module scaffolding: all v2 Guest exports stubbed.
//! - Interest management: tracked-policy set persisted to
//!   state-kv, restored on init (mirrors collection-holders).
//! - Wire-format event types via
//!   `mitos_community_events::collection_metadata`.
//! - Cold-start (Phase 2B) and per-TX live tail (Phase 2C)
//!   are stubbed with explicit TODOs.
//!
//! ## Phase 3 scope (deferred)
//!
//! - CIP-25 facade: extend the CIP-67 prefix filter to also
//!   pick up mint-time TX metadata via
//!   `chain_data::tx_metadata` + module-internal Maestro
//!   enumeration for historical mints past the archive
//!   horizon.
//! - `MetadataStandard::Cip25` entries emitted with
//!   `immutable: true, version: 1`.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashSet};

use mitos_community_events::collection_metadata::{
    MetadataBurned, MetadataEntry, MetadataEvent, MetadataInitial, MetadataStandard,
    MetadataUpdated, SnapshotBegin, SnapshotChunk, SnapshotEnd,
};
use mitos_module_kit::{BootstrapCursor, BootstrapIo, ChunkedBootstrap, ScannedPage};
use pallas_primitives::PlutusData;
use serde::{Deserialize, Serialize};

use crate::mitos::platform_v2::chain_data;
use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::state_kv;
use crate::mitos::platform_v2::types::{
    ConsumedEvent, OutputRef as WitOutputRef, ProducedEvent, RollbackEvent, UtxoEvent,
};

const LOG_TARGET: &str = "collection-metadata-module";

/// CIP-67 label `100` byte prefix — identifies a CIP-68
/// reference token whose datum carries the canonical metadata.
/// 4 bytes: `0x00 0x06 0x43 0xb0`.
const CIP68_REF_TOKEN_PREFIX: [u8; 4] = [0x00, 0x06, 0x43, 0xb0];

/// Per-page hint for the cold-start scan. `utxos_by_policy` is
/// paged; the host clamps each returned page to its own
/// adaptive per-call budget.
const COLD_START_PAGE_HINT: u32 = 10_000;

/// state-kv key under which we persist the set of currently-
/// tracked policies (CBOR list of 56-char hex strings).
const KV_TRACKED_POLICIES: &str = "tracked-policies";

/// state-kv key prefix for the **sharded** per-asset metadata
/// store: one kv key per ref-token, `mentry:<policy_hex>:<suffix_hex>`
/// → CBOR `StoredEntry`. Sharded (vs a single per-policy blob) so
/// no rebootstrap/cold-start/live-tail call ever serializes the
/// whole ledger — the deferred "pathological accumulator" fix from
/// `WASM_BUDGET_CHUNKING.md`. The kv keys themselves are the single
/// source of truth for which assets exist (enumerated via
/// `state_kv::list_values`), so there's no separate key index to
/// drift.
const KV_SHARD_PREFIX: &str = "mentry:";

/// state-kv key for the re-entrant `rebootstrap` continuation
/// cursor — the index of the policy currently being re-scanned
/// (8 BE bytes). Durable so a host restart mid-round resumes
/// at the right policy. Per-page progress within a policy is
/// thread-local + volatile.
const KV_REBOOTSTRAP_CURSOR: &str = "rebootstrap-cursor";

/// Scope for the *next* `rebootstrap` pump: the policies a
/// subscribe-time Add just brought in. When present, `rebootstrap`
/// cold-starts only these (the per-policy auto-onboard, driven by
/// the host pump after `update-interest`); when absent, `rebootstrap`
/// is a full recapture over all tracked policies. Cleared once the
/// scoped round completes. Encoded as ciborium `Vec<Vec<u8>>` of
/// 28-byte policy hashes.
const KV_ONBOARD_PREDICATES: &str = "onboard-predicates";

/// Entries per `SnapshotChunk` when emitting a chunked
/// metadata snapshot. Metadata payloads are larger than
/// holdings (KB per entry vs ~80 bytes per holding), so the
/// chunk size is smaller — `holder-distribution`-style 1000
/// would build hundreds of KB per emit and risk fuel
/// exhaustion. Per `WASM_BUDGET_CHUNKING.md`, a chunk of ~100
/// metadata entries is the safe upper bound.
const SNAPSHOT_CHUNK_ENTRIES: usize = 100;

/// 28-byte hash size for policy ids.
const HASH_BYTES: usize = 28;

thread_local! {
    /// Currently-tracked policy ids (28-byte hashes). Cleared
    /// on `Remove`/`Replace` ops, repopulated on `Add`.
    /// Persisted via `KV_TRACKED_POLICIES` so a host restart
    /// without an attached companion keeps filtering correctly.
    static TRACKED_POLICIES: RefCell<HashSet<[u8; HASH_BYTES]>> = RefCell::new(HashSet::new());

    /// In-flight re-entrant `rebootstrap` driver. `None` between
    /// rounds; rebuilt from the durable cursor
    /// (`KV_REBOOTSTRAP_CURSOR`) after a trap drops the wasm
    /// instance. Holds no resident ledger — entries are sharded to
    /// state-kv as they're scanned, and the emit phase reads them
    /// back by offset, so a re-instantiation resumes mid-snapshot
    /// instead of restarting (the metadata-loop fix). See
    /// `mitos_module_kit::ChunkedBootstrap`.
    static REBOOTSTRAP_DRIVER: RefCell<Option<ChunkedBootstrap>> = const { RefCell::new(None) };
}

// ============================================================
// In-memory ledger
// ============================================================

/// One stored entry in the per-policy metadata ledger.
///
/// Keyed in the ledger by the `_100` ref-token's name suffix
/// (the bytes after the `000643b0` prefix) — that's the shared
/// identity across the ref and user halves of a CIP-68 pair.
/// Persisted as-is alongside its `datum_hash` so the live-tail
/// path (Phase 2C) can detect "datum unchanged" vs "datum
/// changed" respends without re-decoding the prior datum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredEntry {
    /// The wire-shape entry that gets emitted in snapshots and
    /// `Initial`/`Updated` events.
    entry: MetadataEntry,
    /// 32-byte hash of the datum CBOR that produced this
    /// entry. Used in Phase 2C to detect no-op respends
    /// (same datum hash → same datum → no `Updated` emit).
    datum_hash: Vec<u8>,
}

// ── Sharded per-asset metadata store ──
// One kv key per ref-token: `mentry:<policy_hex>:<suffix_hex>` →
// CBOR `StoredEntry`. No single call ever serializes the whole
// ledger, and `state_kv::list_values` enumerates a policy's assets
// directly (single source of truth — no separate key index).

fn shard_prefix(policy_hex: &str) -> String {
    format!("{KV_SHARD_PREFIX}{policy_hex}:")
}

fn shard_key_for(policy_hex: &str, suffix: &[u8]) -> String {
    format!("{KV_SHARD_PREFIX}{policy_hex}:{}", hex::encode(suffix))
}

fn shard_get_entry(policy_hex: &str, suffix: &[u8]) -> Option<StoredEntry> {
    let bytes = state_kv::get_value(&shard_key_for(policy_hex, suffix))?;
    ciborium::de::from_reader(bytes.as_slice()).ok()
}

fn shard_put_entry(policy_hex: &str, suffix: &[u8], stored: &StoredEntry) {
    let mut buf = Vec::with_capacity(256);
    if let Err(e) = ciborium::ser::into_writer(stored, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode metadata shard for {policy_hex}: {e}"),
        );
        return;
    }
    state_kv::set_value(&shard_key_for(policy_hex, suffix), &buf);
}

fn shard_delete_entry(policy_hex: &str, suffix: &[u8]) {
    state_kv::delete_value(&shard_key_for(policy_hex, suffix));
}

/// Delete every shard for a policy (on un-track / before a fresh
/// re-scan) in one txn.
fn shard_clear_policy(policy_hex: &str) {
    state_kv::delete_prefix(&shard_prefix(policy_hex));
}

// ============================================================
// CIP-68 datum decoding
// ============================================================

/// Strip the `000643b0` CIP-67 label-100 prefix from an
/// asset name. Returns `None` for any name that's too short
/// or doesn't carry the ref-token prefix.
fn cip68_ref_token_suffix(asset_name: &[u8]) -> Option<&[u8]> {
    if asset_name.len() < CIP68_REF_TOKEN_PREFIX.len() {
        return None;
    }
    if &asset_name[..CIP68_REF_TOKEN_PREFIX.len()] != CIP68_REF_TOKEN_PREFIX.as_slice() {
        return None;
    }
    Some(&asset_name[CIP68_REF_TOKEN_PREFIX.len()..])
}

/// Decode a CIP-68 datum (PlutusData Constructor 0). Returns
/// `(metadata_json, version, standard)` on success, `None` if
/// the CBOR doesn't match the spec.
///
/// CIP-68 datum shape:
///   - V1: `Constructor 0 [metadata_map, version]`           (2 fields)
///   - V2: `Constructor 0 [metadata_map, version, extra]`    (3 fields)
///
/// `metadata_map` is `Map(bytestring -> any)` per spec; we
/// walk it via `plutus_to_json_value` to produce a JSON shape
/// matching `cip-25-mint`'s convention.
fn decode_cip68_datum(datum_cbor: &[u8]) -> Option<(Option<String>, u64, MetadataStandard)> {
    let pd: PlutusData = pallas_codec::minicbor::decode(datum_cbor).ok()?;
    let constr = match pd {
        PlutusData::Constr(c) => c,
        _ => return None,
    };
    // Constructor 0 per spec; tag-121 is alonzo's `Constr 0`
    // encoding, `any_constructor: Some(0)` is the unbounded form.
    if constr.tag != 121 && constr.any_constructor != Some(0) {
        return None;
    }
    let fields: Vec<PlutusData> = constr.fields.into();
    let standard = match fields.len() {
        2 => MetadataStandard::Cip68V1,
        3 => MetadataStandard::Cip68V2,
        _ => return None,
    };
    let metadata_json = fields.first().and_then(|f| plutus_to_json_string(f).ok());
    let version = fields.get(1).and_then(plutus_to_u64).unwrap_or(1);
    Some((metadata_json, version, standard))
}

fn plutus_to_u64(pd: &PlutusData) -> Option<u64> {
    match pd {
        PlutusData::BigInt(pallas_primitives::BigInt::Int(n)) => {
            let raw = i128::from(*n);
            u64::try_from(raw).ok()
        }
        _ => None,
    }
}

/// Render a `PlutusData` value as a JSON string. Same
/// conventions as `cip-25-mint`'s `plutus_to_json_string`:
/// bytestrings → UTF-8 if valid else `0x...` hex; constructors →
/// objects with `__constructor` + `fields`; maps → objects with
/// string-coerced keys; ints → numbers; lists → arrays.
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
            pallas_primitives::BigInt::BigUInt(b)
            | pallas_primitives::BigInt::BigNInt(b) => {
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
            // Non-string keys aren't spec-compliant for CIP-68
            // metadata but shouldn't crash us. Encode whatever
            // we got as hex so consumers can recover it.
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
// Wire-side interest predicate mirror
// ============================================================

/// Minimal mirror of the on-wire `InterestPredicate` enum —
/// same pattern as `collection-holders` and
/// `holder-distribution`. We only care about `HoldsPolicy` but
/// capture the other variants so the deserialiser can step
/// over them without erroring.
#[derive(Debug, Deserialize)]
enum InterestPredicateWire {
    AtAddress(serde::de::IgnoredAny),
    AtStakeCred(serde::de::IgnoredAny),
    HoldsPolicy(String),
    HoldsAsset {
        #[allow(dead_code)]
        policy: serde::de::IgnoredAny,
        #[allow(dead_code)]
        asset_name: serde::de::IgnoredAny,
    },
    TickEvery(#[allow(dead_code)] u32),
}

// ============================================================
// Interest management
// ============================================================

fn apply_interest_update(op: InterestOp, items_cbor: &[u8]) {
    let predicates: Vec<InterestPredicateWire> = match ciborium::de::from_reader(items_cbor) {
        Ok(v) => v,
        Err(e) => {
            logging::log(
                LogLevel::Error,
                LOG_TARGET,
                &format!("decode interest predicates failed: {e}"),
            );
            return;
        }
    };

    let policies: Vec<[u8; HASH_BYTES]> = predicates
        .into_iter()
        .filter_map(|p| match p {
            InterestPredicateWire::HoldsPolicy(hex_str) => {
                let bytes = hex::decode(&hex_str).ok()?;
                if bytes.len() != HASH_BYTES {
                    return None;
                }
                let mut arr = [0u8; HASH_BYTES];
                arr.copy_from_slice(&bytes);
                Some(arr)
            }
            _ => None,
        })
        .collect();

    let mut added: Vec<[u8; HASH_BYTES]> = Vec::new();
    let mut removed: Vec<[u8; HASH_BYTES]> = Vec::new();
    TRACKED_POLICIES.with(|set| {
        let mut set = set.borrow_mut();
        match op {
            InterestOp::Replace => {
                let prev = std::mem::take(&mut *set);
                set.extend(policies.iter().copied());
                for p in set.iter() {
                    if !prev.contains(p) {
                        added.push(*p);
                    }
                }
                for p in prev.iter() {
                    if !set.contains(p) {
                        removed.push(*p);
                    }
                }
            }
            InterestOp::Add => {
                for p in &policies {
                    if set.insert(*p) {
                        added.push(*p);
                    }
                }
            }
            InterestOp::Remove => {
                for p in &policies {
                    if set.remove(p) {
                        removed.push(*p);
                    }
                }
            }
        }
    });

    persist_tracked_policies();

    // Drop per-policy ledger state on un-track. Resolved
    // decision #1 in `COLLECTION_MODULES.md` — no TTL, no
    // refcount; rebuild on next subscribe.
    for policy in &removed {
        let policy_hex = hex::encode(policy);
        shard_clear_policy(&policy_hex);
        logging::log(
            LogLevel::Info,
            LOG_TARGET,
            &format!("dropped metadata shards for untracked policy {policy_hex}"),
        );
    }

    // Cold-start each newly-added policy via the budget-safe chunked
    // `rebootstrap` pump rather than an inline single-fuel scan. The
    // inline scan traps + rolls back for large collections (10k+),
    // emitting nothing — so add-collection silently failed to ingest
    // big collections. Instead, record the added policies as the next
    // rebootstrap's scope; the host pumps the re-entrant chunked
    // rebootstrap over just these after `update-interest` returns,
    // cold-starting any size. Idempotent: re-adds merge into the scope.
    if !added.is_empty() {
        seed_onboard_scope(&added);
    }
}

/// Record `added` policies as the scope for the next `rebootstrap`
/// pump. Merges with any still-pending scope (rapid successive adds
/// all cold-start) and resets the durable cursor + in-process driver
/// so the pump restarts cleanly over the scoped set.
fn seed_onboard_scope(added: &[[u8; HASH_BYTES]]) {
    let mut scope: Vec<Vec<u8>> = read_onboard_scope().unwrap_or_default();
    for p in added {
        let v = p.to_vec();
        if !scope.contains(&v) {
            scope.push(v);
        }
    }
    scope.sort_unstable();
    let mut buf = Vec::with_capacity(8 + scope.len() * (HASH_BYTES + 2));
    if ciborium::ser::into_writer(&scope, &mut buf).is_ok() {
        state_kv::set_value(KV_ONBOARD_PREDICATES, &buf);
    }
    state_kv::delete_value(KV_REBOOTSTRAP_CURSOR);
    REBOOTSTRAP_DRIVER.with(|c| *c.borrow_mut() = None);
}

/// Decode the pending onboard scope (policies a subscribe-time Add
/// queued for cold-start), if any.
fn read_onboard_scope() -> Option<Vec<Vec<u8>>> {
    let bytes = state_kv::get_value(KV_ONBOARD_PREDICATES)?;
    ciborium::de::from_reader(&bytes[..]).ok()
}

fn persist_tracked_policies() {
    let policies: Vec<String> =
        TRACKED_POLICIES.with(|set| set.borrow().iter().map(hex::encode).collect());
    let mut buf = Vec::with_capacity(64 + policies.len() * 64);
    if ciborium::ser::into_writer(&policies, &mut buf).is_ok() {
        state_kv::set_value(KV_TRACKED_POLICIES, &buf);
    }
}

fn restore_tracked_policies() {
    let Some(bytes) = state_kv::get_value(KV_TRACKED_POLICIES) else {
        return;
    };
    let policies: Vec<String> = match ciborium::de::from_reader(bytes.as_slice()) {
        Ok(v) => v,
        Err(e) => {
            logging::log(
                LogLevel::Warn,
                LOG_TARGET,
                &format!("decode tracked-policies failed: {e}"),
            );
            return;
        }
    };
    TRACKED_POLICIES.with(|set| {
        let mut set = set.borrow_mut();
        for hex_str in policies {
            let Ok(bytes) = hex::decode(&hex_str) else {
                continue;
            };
            if bytes.len() != HASH_BYTES {
                continue;
            }
            let mut arr = [0u8; HASH_BYTES];
            arr.copy_from_slice(&bytes);
            set.insert(arr);
        }
    });
}

// ============================================================
// Emission helpers
// ============================================================

fn emit_event(event: &MetadataEvent) {
    let mut buf = Vec::with_capacity(2048);
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode MetadataEvent failed: {e}"),
        );
        return;
    }
    // Partition key = policy hex bytes: per-policy dialer lane +
    // host-side per-companion interest routing (prevents recapture
    // cross-contamination across companions). See
    // `docs/design/HOLDER_DISTRIBUTION_DRIVER_MIGRATION.md` §5.
    emit::emit_event_keyed(0, event_policy(event).as_bytes(), &buf);
}

/// The policy hex every `MetadataEvent` carries — used as the
/// emission partition / interest-routing key.
fn event_policy(event: &MetadataEvent) -> &str {
    match event {
        MetadataEvent::SnapshotBegin(b) => &b.policy,
        MetadataEvent::SnapshotChunk(c) => &c.policy,
        MetadataEvent::SnapshotEnd(e) => &e.policy,
        MetadataEvent::Initial(i) => &i.policy,
        MetadataEvent::Updated(u) => &u.policy,
        MetadataEvent::Burned(b) => &b.policy,
    }
}

/// Decode one page of UTxOs into `(suffix, StoredEntry)` pairs for
/// every CIP-68 ref token of `policy`. Used by the re-entrant
/// `rebootstrap` driver's `scan_page` (cold-start + recapture).
fn decode_page(policy: &[u8; HASH_BYTES], refs: &[WitOutputRef]) -> Vec<(Vec<u8>, StoredEntry)> {
    if refs.is_empty() {
        return Vec::new();
    }
    let utxos = chain_data::read_utxos(refs);
    let datums = chain_data::read_output_datums(refs);

    // CRITICAL: `read_utxos` is NOT parallel to `refs` — the host
    // skips outputs that fail to project and appends archive-fallback
    // hits in a second pass, so its order and length differ from the
    // input. `read_output_datums`, by contrast, IS strictly parallel
    // to `refs` (one entry per ref, in order). Pairing them by
    // positional index therefore mis-assigns each datum to the wrong
    // UTxO — putting one asset's metadata onto another asset. Key the
    // datums by output-ref and look each UTxO's datum up by its own
    // ref. (One `read_output_datums` pass.)
    let mut datum_idx: BTreeMap<(Vec<u8>, u32), usize> = BTreeMap::new();
    for (i, r) in refs.iter().enumerate() {
        datum_idx.insert((r.tx_hash.clone(), r.index), i);
    }

    let mut out = Vec::new();
    for (oref, output) in utxos.iter() {
        let datum_opt = datum_idx
            .get(&(oref.tx_hash.clone(), oref.index))
            .and_then(|&i| datums.get(i))
            .and_then(|d| d.as_ref());
        for asset in &output.assets {
            if asset.asset.policy != policy {
                continue;
            }
            let Some(suffix) = cip68_ref_token_suffix(&asset.asset.name) else {
                continue;
            };
            // Resolve the datum bytes. Inline datums arrive with a
            // populated `payload`; hash-only datums (common — many
            // minters store the CIP-68 datum by hash rather than
            // inline in the ref UTxO) arrive payload-empty and need
            // the `datum_by_hash` fallback. Same resolver as the live
            // `handle_produced` path so cold-start/rebootstrap have
            // identical coverage.
            let Some(datum) = datum_opt else { continue };
            let Some(datum_bytes) = resolve_datum_bytes(datum) else {
                continue;
            };
            let Some((metadata_json, version, standard)) = decode_cip68_datum(&datum_bytes) else {
                continue;
            };
            let entry = MetadataEntry {
                asset_name_hex: hex::encode(suffix),
                metadata_json,
                standard,
                version,
                immutable: false,
                source_tx: hex::encode(&oref.tx_hash),
            };
            out.push((
                suffix.to_vec(),
                StoredEntry {
                    entry,
                    datum_hash: datum.hash.clone(),
                },
            ));
        }
    }
    out
}

// ============================================================
// Per-TX event handling
// ============================================================

/// Per-`handle_events` buffer. One Cardano TX's worth of
/// CIP-68 ref-token events filtered to tracked policies.
/// Flushed (Initial / Updated / Burned emit + ledger update)
/// at the end of the dispatch call.
#[derive(Default)]
struct TxBuffer {
    tx_hash: Option<Vec<u8>>,
    slot: u64,
    /// `(policy, suffix)` → Produced event payload we need
    /// post-pairing: the ref token's new datum payload + hash.
    /// `BTreeMap` for deterministic iteration order at flush
    /// time (golden-test stability).
    produced_refs: BTreeMap<([u8; HASH_BYTES], Vec<u8>), (Vec<u8>, Vec<u8>)>,
    /// `(policy, suffix)` set of consumed ref tokens. Anything
    /// here without a matching `produced_refs` entry at flush
    /// time is a burn.
    consumed_refs: BTreeSet<([u8; HASH_BYTES], Vec<u8>)>,
}

fn slot_from_cursor(c: &crate::mitos::platform_v2::types::ChainPoint) -> u64 {
    match c {
        crate::mitos::platform_v2::types::ChainPoint::Specific(sp) => sp.slot,
        crate::mitos::platform_v2::types::ChainPoint::SlotOnly(s) => *s,
        crate::mitos::platform_v2::types::ChainPoint::Origin => 0,
    }
}

fn handle_produced(p: &ProducedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(p.tx_hash.clone());
    }
    if buf.slot == 0 {
        buf.slot = slot_from_cursor(&p.cursor);
    }
    let Some(datum) = &p.datum else {
        return;
    };
    // Resolve the datum bytes. Live dispatch carries inline
    // datum bytes in `payload`; hash-only datums arrive with
    // `payload` empty and we fall back to `datum_by_hash`
    // (witness datums harvested from the block, or whatever the
    // data plane resolves). Same pattern as `cip-68-mint`.
    let Some(payload) = resolve_datum_bytes(datum) else {
        return;
    };
    TRACKED_POLICIES.with(|set| {
        let set = set.borrow();
        for asset in &p.output.assets {
            let Some(policy_arr) = policy_in_set(&asset.asset.policy, &set) else {
                continue;
            };
            let Some(suffix) = cip68_ref_token_suffix(&asset.asset.name) else {
                continue;
            };
            buf.produced_refs.insert(
                (policy_arr, suffix.to_vec()),
                (datum.hash.clone(), payload.clone()),
            );
        }
    });
}

/// Resolve datum CBOR bytes. Inline datums arrive with
/// `payload` non-empty; hash-only datums arrive with `payload`
/// empty and we fall back to `chain_data::datum_by_hash`.
/// Returns `None` if neither path produces bytes.
fn resolve_datum_bytes(d: &crate::mitos::platform_v2::types::TypedDatum) -> Option<Vec<u8>> {
    if !d.payload.is_empty() {
        return Some(d.payload.clone());
    }
    chain_data::datum_by_hash(&d.hash)
}

fn handle_consumed(c: &ConsumedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(c.consuming_tx_hash.clone());
    }
    if buf.slot == 0 {
        buf.slot = slot_from_cursor(&c.cursor);
    }
    TRACKED_POLICIES.with(|set| {
        let set = set.borrow();
        for asset in &c.prior_output.assets {
            let Some(policy_arr) = policy_in_set(&asset.asset.policy, &set) else {
                continue;
            };
            let Some(suffix) = cip68_ref_token_suffix(&asset.asset.name) else {
                continue;
            };
            buf.consumed_refs.insert((policy_arr, suffix.to_vec()));
        }
    });
}

/// Return the 28-byte array form of `policy_bytes` if and only
/// if it's in the tracked set. Avoids allocating a `Vec` per
/// asset just to do the contains-check.
fn policy_in_set(
    policy_bytes: &[u8],
    set: &HashSet<[u8; HASH_BYTES]>,
) -> Option<[u8; HASH_BYTES]> {
    if policy_bytes.len() != HASH_BYTES {
        return None;
    }
    let mut arr = [0u8; HASH_BYTES];
    arr.copy_from_slice(policy_bytes);
    if set.contains(&arr) {
        Some(arr)
    } else {
        None
    }
}

fn handle_rollback(_r: &RollbackEvent) {
    // Same rationale as collection-holders: chain-point-keyed
    // idempotency at the application layer absorbs re-applied
    // events post-rollback. Log for operator visibility.
    logging::log(
        LogLevel::Info,
        LOG_TARGET,
        "rollback received — relying on re-apply convergence",
    );
}

/// At end-of-TX: pair Produced and Consumed ref-token events,
/// emit Initial/Updated/Burned, update the ledger.
fn flush_buffer(buf: TxBuffer) {
    let Some(tx_hash) = buf.tx_hash else {
        return;
    };
    let tx_hash_hex = hex::encode(&tx_hash);
    let slot = buf.slot;

    // Group by policy so we load each ledger once. BTreeMap
    // for deterministic outer iteration (golden-test stability).
    let mut by_policy: BTreeMap<[u8; HASH_BYTES], Vec<(Vec<u8>, Option<(Vec<u8>, Vec<u8>)>)>> =
        BTreeMap::new();
    for ((policy, suffix), datum) in buf.produced_refs {
        by_policy
            .entry(policy)
            .or_default()
            .push((suffix, Some(datum)));
    }
    for (policy, suffix) in buf.consumed_refs {
        // Skip consumed entries that also have a Produced — those
        // are respends, handled by the Produced side (Updated or
        // no-op).
        let already_produced = by_policy
            .get(&policy)
            .map(|entries| entries.iter().any(|(s, _)| s == &suffix))
            .unwrap_or(false);
        if !already_produced {
            by_policy.entry(policy).or_default().push((suffix, None));
        }
    }

    for (policy, entries) in by_policy {
        let policy_hex = hex::encode(policy);

        for (suffix, datum_opt) in entries {
            let asset_name_hex = hex::encode(&suffix);
            match datum_opt {
                Some((datum_hash, datum_payload)) => {
                    let Some((metadata_json, version, standard)) =
                        decode_cip68_datum(&datum_payload)
                    else {
                        continue;
                    };
                    let entry = MetadataEntry {
                        asset_name_hex: asset_name_hex.clone(),
                        metadata_json,
                        standard,
                        version,
                        immutable: false,
                        source_tx: tx_hash_hex.clone(),
                    };
                    // Per-asset shard lookup — no whole-ledger load.
                    match shard_get_entry(&policy_hex, &suffix) {
                        None => {
                            // First sighting — Initial.
                            emit_event(&MetadataEvent::Initial(MetadataInitial {
                                policy: policy_hex.clone(),
                                slot,
                                tx_hash: tx_hash_hex.clone(),
                                entry: entry.clone(),
                            }));
                            shard_put_entry(&policy_hex, &suffix, &StoredEntry { entry, datum_hash });
                        }
                        Some(prev) if prev.datum_hash == datum_hash => {
                            // Same-datum respend — no emit. Could
                            // be a stake-delegation change on the
                            // ref UTxO or a script-context interaction
                            // that didn't actually rotate metadata.
                        }
                        Some(prev) => {
                            // Datum changed — Updated.
                            let prior_version = prev.entry.version;
                            emit_event(&MetadataEvent::Updated(MetadataUpdated {
                                policy: policy_hex.clone(),
                                slot,
                                tx_hash: tx_hash_hex.clone(),
                                entry: entry.clone(),
                                prior_version,
                            }));
                            shard_put_entry(&policy_hex, &suffix, &StoredEntry { entry, datum_hash });
                        }
                    }
                }
                None => {
                    // Consumed without re-produce — Burned.
                    emit_event(&MetadataEvent::Burned(MetadataBurned {
                        policy: policy_hex.clone(),
                        slot,
                        tx_hash: tx_hash_hex.clone(),
                        asset_name_hex,
                    }));
                    shard_delete_entry(&policy_hex, &suffix);
                }
            }
        }
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
        restore_tracked_policies();
        let count = TRACKED_POLICIES.with(|s| s.borrow().len());
        logging::log(
            LogLevel::Info,
            LOG_TARGET,
            &format!("init: restored {count} tracked policies"),
        );
    }

    fn handle_events(events: Vec<DispatchEvent>) {
        let any_tracked = TRACKED_POLICIES.with(|s| !s.borrow().is_empty());
        if !any_tracked {
            return;
        }
        let mut buf = TxBuffer::default();
        for event in events {
            match event {
                DispatchEvent::Utxo(UtxoEvent::Produced(p)) => handle_produced(&p, &mut buf),
                DispatchEvent::Utxo(UtxoEvent::Consumed(c)) => handle_consumed(&c, &mut buf),
                DispatchEvent::Rollback(r) => handle_rollback(&r),
                _ => {}
            }
        }
        flush_buffer(buf);
    }

    fn update_interest(op: InterestOp, items_cbor: Vec<u8>) -> Result<(), String> {
        apply_interest_update(op, &items_cbor);
        Ok(())
    }

    /// Re-emit the metadata snapshot for tracked policies, one
    /// bounded unit of work per call, via the shared re-entrant
    /// [`ChunkedBootstrap`] driver: scan a page → shard each entry →
    /// emit one `SnapshotChunk` per call from the shards. Every
    /// progress field is durable in state-kv, so a trap mid-emit
    /// resumes at the offset instead of restarting the policy (the
    /// out-of-fuel `SnapshotBegin`-loop fix). No call ever
    /// serializes the whole ledger.
    fn rebootstrap() -> Result<RebootstrapStep, String> {
        REBOOTSTRAP_DRIVER.with(|cell| {
            let mut slot = cell.borrow_mut();
            if slot.is_none() {
                // Scope the round: a pending onboard set (subscribe-time
                // Add) cold-starts just those policies; otherwise this is
                // a full recapture over every tracked policy. The host
                // pumps this same export in both cases.
                let mut predicates: Vec<Vec<u8>> = read_onboard_scope().unwrap_or_else(|| {
                    TRACKED_POLICIES.with(|s| s.borrow().iter().map(|p| p.to_vec()).collect())
                });
                predicates.sort_unstable();
                let cursor =
                    state_kv::get_value(KV_REBOOTSTRAP_CURSOR).and_then(|b| BootstrapCursor::decode(&b));
                *slot = Some(ChunkedBootstrap::resume(predicates, cursor));
            }
            let driver = slot.as_mut().expect("driver initialised above");
            let mut io = MetadataIo;
            let out = driver.step(&mut io);
            if out.done {
                *slot = None;
                // Clear the scoped onboard set so a future recapture
                // defaults to the full tracked set.
                state_kv::delete_value(KV_ONBOARD_PREDICATES);
            }
            Ok(RebootstrapStep {
                done: out.done,
                ingested: out.ingested,
            })
        })
    }
}

/// `BootstrapIo` adapter wiring the kit driver to this module's
/// host-fns (chain scan, sharded state-kv, typed emission). A
/// zero-sized handle — all state lives in state-kv / the chain.
struct MetadataIo;

/// Coerce a predicate slice (always a 28-byte policy hash) to the
/// array form the chain-data host-fn wants.
fn policy_arr(predicate: &[u8]) -> [u8; HASH_BYTES] {
    let mut a = [0u8; HASH_BYTES];
    a.copy_from_slice(predicate);
    a
}

impl BootstrapIo for MetadataIo {
    type Entry = StoredEntry;

    fn scan_page(&mut self, predicate: &[u8], after: Option<&[u8]>) -> ScannedPage<StoredEntry> {
        let policy = policy_arr(predicate);
        let page = chain_data::utxos_by_policy(&policy, after, COLD_START_PAGE_HINT);
        let ingested = page.refs.len() as u64;
        let entries = decode_page(&policy, &page.refs);
        ScannedPage {
            entries,
            next: page.next,
            anchor_slot: page.anchor_slot,
            ingested,
        }
    }

    fn shard_get_many(&self, predicate: &[u8], keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
        let policy_hex = hex::encode(predicate);
        let kv_keys: Vec<String> = keys.iter().map(|k| shard_key_for(&policy_hex, k)).collect();
        state_kv::get_many(&kv_keys)
    }
    fn shard_put_many(&mut self, predicate: &[u8], entries: &[(Vec<u8>, Vec<u8>)]) {
        let policy_hex = hex::encode(predicate);
        let kv_entries: Vec<(String, Vec<u8>)> = entries
            .iter()
            .map(|(k, v)| (shard_key_for(&policy_hex, k), v.clone()))
            .collect();
        state_kv::set_many(&kv_entries);
    }
    fn shard_scan(
        &self,
        predicate: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let policy_hex = hex::encode(predicate);
        let prefix = shard_prefix(&policy_hex);
        let after_key = after.map(|a| shard_key_for(&policy_hex, a));
        state_kv::kv_scan(&prefix, after_key.as_deref(), limit as u32)
            .into_iter()
            .filter_map(|(k, v)| {
                // strip `prefix` + hex-decode → the suffix entry_key.
                k.strip_prefix(prefix.as_str())
                    .and_then(|h| hex::decode(h).ok())
                    .map(|ek| (ek, v))
            })
            .collect()
    }
    fn shard_clear(&mut self, predicate: &[u8]) {
        shard_clear_policy(&hex::encode(predicate));
    }

    fn encode_entry(&self, entry: &StoredEntry) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);
        let _ = ciborium::ser::into_writer(entry, &mut buf);
        buf
    }
    fn decode_entry(&self, bytes: &[u8]) -> Option<StoredEntry> {
        ciborium::de::from_reader(bytes).ok()
    }

    // CIP-68 datum rotation replaces; each ref-token appears once →
    // last-write-wins (the default). No `accumulates`/`merge` override.

    fn load_cursor(&self) -> Option<Vec<u8>> {
        state_kv::get_value(KV_REBOOTSTRAP_CURSOR)
    }
    fn save_cursor(&mut self, bytes: &[u8]) {
        state_kv::set_value(KV_REBOOTSTRAP_CURSOR, bytes);
    }
    fn clear_cursor(&mut self) {
        state_kv::delete_value(KV_REBOOTSTRAP_CURSOR);
    }

    fn emit_begin(&mut self, predicate: &[u8], anchor_slot: u64) {
        emit_event(&MetadataEvent::SnapshotBegin(SnapshotBegin {
            policy: hex::encode(predicate),
            cursor_slot: anchor_slot,
            cursor_hash_hex: String::new(),
        }));
    }
    fn emit_chunk(&mut self, predicate: &[u8], entries: Vec<StoredEntry>) {
        emit_event(&MetadataEvent::SnapshotChunk(SnapshotChunk {
            policy: hex::encode(predicate),
            entries: entries.into_iter().map(|s| s.entry).collect(),
        }));
    }
    fn emit_end(&mut self, predicate: &[u8], total: u64) {
        emit_event(&MetadataEvent::SnapshotEnd(SnapshotEnd {
            policy: hex::encode(predicate),
            entry_count: total,
        }));
    }
    fn chunk_size(&self) -> usize {
        SNAPSHOT_CHUNK_ENTRIES
    }
}

export!(Module);
