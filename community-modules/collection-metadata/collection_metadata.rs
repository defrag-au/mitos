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
use mitos_module_kit::ReentrantRound;
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
/// tracked policies (CBOR list of 56-char hex strings). The
/// per-policy metadata ledger is keyed `metadata_ledger:<policy_hex>`.
const KV_TRACKED_POLICIES: &str = "tracked-policies";

/// state-kv key prefix for per-policy metadata ledgers. Full
/// key: `metadata_ledger:<policy_hex>`. Populated by the
/// cold-start scan (Phase 2B) and mutated by per-TX events
/// (Phase 2C).
#[allow(dead_code)]
const KV_METADATA_LEDGER_PREFIX: &str = "metadata_ledger:";

/// state-kv key for the re-entrant `rebootstrap` continuation
/// cursor — the index of the policy currently being re-scanned
/// (8 BE bytes). Durable so a host restart mid-round resumes
/// at the right policy. Per-page progress within a policy is
/// thread-local + volatile.
const KV_REBOOTSTRAP_CURSOR: &str = "rebootstrap-cursor";

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

    /// In-flight `rebootstrap` round. `None` between rounds.
    /// Same shape + same rationale as collection-holders'
    /// REBOOTSTRAP_STATE — accumulator is a per-policy
    /// `MetadataLedger`; durable cursor in state-kv is just
    /// the `predicate_idx`.
    static REBOOTSTRAP_STATE: RefCell<Option<ReentrantRound<[u8; HASH_BYTES], MetadataLedger>>> =
        const { RefCell::new(None) };

    /// In-progress chunked-snapshot emit for the policy whose
    /// scan just finished. Drained one `SnapshotChunk` per
    /// `rebootstrap` call.
    static REBOOTSTRAP_EMIT: RefCell<Option<EmitState>> = const { RefCell::new(None) };
}

/// A chunked metadata snapshot mid-emit. Drained
/// `SNAPSHOT_CHUNK_ENTRIES` at a time.
struct EmitState {
    /// 56-char hex policy id — every event of the sequence
    /// carries it.
    policy_hex: String,
    /// The full entries list, built once when the scan
    /// completed.
    entries: Vec<MetadataEntry>,
    /// Offset into `entries` — the next chunk starts here.
    offset: usize,
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

/// Per-policy metadata ledger as held in memory + persisted to
/// state-kv. Keyed by the `_100`-suffix bytes so a consumer
/// joining holdings → metadata strips the user-token prefix
/// once and looks up directly.
#[derive(Default, Serialize, Deserialize)]
struct MetadataLedger {
    entries: BTreeMap<Vec<u8>, StoredEntry>,
}

fn ledger_key(policy_hex: &str) -> String {
    format!("{KV_METADATA_LEDGER_PREFIX}{policy_hex}")
}

#[allow(dead_code)] // used by Phase 2C `handle_produced`
fn load_ledger(policy_hex: &str) -> MetadataLedger {
    let Some(bytes) = state_kv::get_value(&ledger_key(policy_hex)) else {
        return MetadataLedger::default();
    };
    ciborium::de::from_reader(bytes.as_slice()).unwrap_or_default()
}

fn persist_ledger(policy_hex: &str, ledger: &MetadataLedger) {
    let mut buf = Vec::with_capacity(4096);
    if let Err(e) = ciborium::ser::into_writer(ledger, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode metadata ledger for {policy_hex}: {e}"),
        );
        return;
    }
    state_kv::set_value(&ledger_key(policy_hex), &buf);
}

fn delete_ledger(policy_hex: &str) {
    state_kv::delete_value(&ledger_key(policy_hex));
}

fn save_rebootstrap_cursor(predicate_idx: usize) {
    state_kv::set_value(KV_REBOOTSTRAP_CURSOR, &(predicate_idx as u64).to_be_bytes());
}

fn load_rebootstrap_cursor() -> usize {
    state_kv::get_value(KV_REBOOTSTRAP_CURSOR)
        .and_then(|b| <[u8; 8]>::try_from(b.as_slice()).ok())
        .map(|b| u64::from_be_bytes(b) as usize)
        .unwrap_or(0)
}

fn clear_rebootstrap_cursor() {
    state_kv::delete_value(KV_REBOOTSTRAP_CURSOR);
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
        delete_ledger(&policy_hex);
        logging::log(
            LogLevel::Info,
            LOG_TARGET,
            &format!("dropped metadata ledger for untracked policy {policy_hex}"),
        );
    }

    // Cold-start each newly-added policy. Single fuel budget per
    // call; for very large collections the host's adaptive page
    // sizing on `utxos_by_policy` keeps each call within budget.
    // Pathological cases (100k+ ref tokens) recover via the
    // chunked `rebootstrap` path (Phase 2C).
    for policy in &added {
        cold_start(policy);
    }
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
    emit::emit_event(0, &buf);
}

/// Emit `SnapshotBegin` and stage the entries in
/// `REBOOTSTRAP_EMIT` so the `rebootstrap` state machine
/// drains one `SnapshotChunk` per call.
fn open_chunked_emit(policy_hex: String, entries: Vec<MetadataEntry>, anchor_slot: u64) {
    emit_event(&MetadataEvent::SnapshotBegin(SnapshotBegin {
        policy: policy_hex.clone(),
        cursor_slot: anchor_slot,
        cursor_hash_hex: String::new(),
    }));
    REBOOTSTRAP_EMIT.with(|cell| {
        *cell.borrow_mut() = Some(EmitState {
            policy_hex,
            entries,
            offset: 0,
        });
    });
}

/// Emit the full chunked snapshot sequence for one policy in
/// a single fuel budget: `SnapshotBegin` → `SnapshotChunk` × N
/// → `SnapshotEnd`. Used by the live `update_interest(Add,
/// ...)` cold-start path. The recapture path (`rebootstrap`)
/// spreads emission across many calls via the re-entrant
/// state machine.
fn emit_full_snapshot(policy_hex: &str, ledger: &MetadataLedger, anchor_slot: u64) {
    emit_event(&MetadataEvent::SnapshotBegin(SnapshotBegin {
        policy: policy_hex.to_string(),
        cursor_slot: anchor_slot,
        cursor_hash_hex: String::new(),
    }));
    let entries: Vec<MetadataEntry> = ledger
        .entries
        .values()
        .map(|stored| stored.entry.clone())
        .collect();
    let total = entries.len() as u64;
    for chunk in entries.chunks(SNAPSHOT_CHUNK_ENTRIES) {
        emit_event(&MetadataEvent::SnapshotChunk(SnapshotChunk {
            policy: policy_hex.to_string(),
            entries: chunk.to_vec(),
        }));
    }
    emit_event(&MetadataEvent::SnapshotEnd(SnapshotEnd {
        policy: policy_hex.to_string(),
        entry_count: total,
    }));
}

// ============================================================
// Cold-start scan
// ============================================================

/// Run the bootstrap scan for a newly-tracked policy. Page
/// through `utxos_by_policy`, ask the host for each page's
/// datums via `read_output_datums`, decode `_100`-prefixed
/// ref-token outputs as CIP-68 Constructor 0, build the
/// ledger, emit the chunked snapshot.
///
/// `typed-output` doesn't carry the datum in v2's data plane —
/// `read_utxos` only resolves address + value. We get the
/// datums via a parallel `read_output_datums` call against
/// the same ref list. The two arrays are positionally
/// aligned per the host-fn contract.
fn cold_start(policy: &[u8; HASH_BYTES]) {
    let mut ledger = MetadataLedger::default();
    let mut after: Option<Vec<u8>> = None;
    let mut anchor_slot: u64;
    let mut total_utxos: usize = 0;

    loop {
        let page = chain_data::utxos_by_policy(policy, after.as_deref(), COLD_START_PAGE_HINT);
        anchor_slot = page.anchor_slot;
        total_utxos += page.refs.len();
        fold_page(&mut ledger, policy, &page.refs);
        match page.next {
            Some(token) => after = Some(token),
            None => break,
        }
    }

    let policy_hex = hex::encode(policy);
    let entry_count = ledger.entries.len();
    persist_ledger(&policy_hex, &ledger);
    emit_full_snapshot(&policy_hex, &ledger, anchor_slot);

    logging::log(
        LogLevel::Info,
        LOG_TARGET,
        &format!(
            "cold-start policy={policy_hex}: {total_utxos} UTxO(s) → {entry_count} ref-token entry(ies) @ slot {anchor_slot}"
        ),
    );
}

/// For one page of UTxOs, resolve `(typed_output, datum)` per
/// ref and fold any CIP-68 ref-token entries into the ledger.
fn fold_page(ledger: &mut MetadataLedger, policy: &[u8; HASH_BYTES], refs: &[WitOutputRef]) {
    if refs.is_empty() {
        return;
    }
    let utxos = chain_data::read_utxos(refs);
    let datums = chain_data::read_output_datums(refs);

    // The host returns both lists in the same order as `refs`,
    // but `read_utxos` is `Vec<(OutputRef, TypedOutput)>` while
    // `read_output_datums` is `Vec<Option<TypedDatum>>`. Index
    // datums by position so we can zip them up. A page where
    // sizes mismatch is a host-bug; skip the surplus.
    for (idx, (oref, out)) in utxos.iter().enumerate() {
        let datum_opt = datums.get(idx).and_then(|d| d.as_ref());
        for asset in &out.assets {
            if asset.asset.policy != policy {
                continue;
            }
            let Some(suffix) = cip68_ref_token_suffix(&asset.asset.name) else {
                continue;
            };
            // Need the datum payload to decode. Hash-only
            // datums (payload empty) mean the host couldn't
            // resolve; we skip — CIP-68 ref tokens almost
            // always carry inline datums, so this is rare.
            let Some(datum) = datum_opt else { continue };
            if datum.payload.is_empty() {
                continue;
            }
            let Some((metadata_json, version, standard)) =
                decode_cip68_datum(&datum.payload)
            else {
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
            ledger.entries.insert(
                suffix.to_vec(),
                StoredEntry {
                    entry,
                    datum_hash: datum.hash.clone(),
                },
            );
        }
    }
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
        let mut ledger = load_ledger(&policy_hex);
        let mut mutated = false;

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
                    match ledger.entries.get(&suffix) {
                        None => {
                            // First sighting — Initial.
                            emit_event(&MetadataEvent::Initial(MetadataInitial {
                                policy: policy_hex.clone(),
                                slot,
                                tx_hash: tx_hash_hex.clone(),
                                entry: entry.clone(),
                            }));
                            ledger.entries.insert(
                                suffix.clone(),
                                StoredEntry {
                                    entry,
                                    datum_hash,
                                },
                            );
                            mutated = true;
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
                            ledger.entries.insert(
                                suffix.clone(),
                                StoredEntry {
                                    entry,
                                    datum_hash,
                                },
                            );
                            mutated = true;
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
                    if ledger.entries.remove(&suffix).is_some() {
                        mutated = true;
                    }
                }
            }
        }

        if mutated {
            persist_ledger(&policy_hex, &ledger);
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

    /// Re-emit the metadata ledger for tracked policies — one
    /// bounded unit of work per call. Mirrors
    /// `collection-holders::rebootstrap`'s two-phase state
    /// machine: drain in-flight `SnapshotChunk` emissions
    /// first, otherwise scan one page of the current policy.
    fn rebootstrap() -> Result<RebootstrapStep, String> {
        // ── Emit phase ──
        enum EmitOutcome {
            NotEmitting,
            Chunk,
            Closed,
        }
        let outcome = REBOOTSTRAP_EMIT.with(|cell| {
            let mut slot = cell.borrow_mut();
            let Some(state) = slot.as_mut() else {
                return EmitOutcome::NotEmitting;
            };
            if state.offset >= state.entries.len() {
                emit_event(&MetadataEvent::SnapshotEnd(SnapshotEnd {
                    policy: state.policy_hex.clone(),
                    entry_count: state.entries.len() as u64,
                }));
                *slot = None;
                EmitOutcome::Closed
            } else {
                let end =
                    (state.offset + SNAPSHOT_CHUNK_ENTRIES).min(state.entries.len());
                emit_event(&MetadataEvent::SnapshotChunk(SnapshotChunk {
                    policy: state.policy_hex.clone(),
                    entries: state.entries[state.offset..end].to_vec(),
                }));
                state.offset = end;
                EmitOutcome::Chunk
            }
        });
        match outcome {
            EmitOutcome::Chunk => {
                return Ok(RebootstrapStep {
                    done: false,
                    ingested: 0,
                });
            }
            EmitOutcome::Closed => {
                return REBOOTSTRAP_STATE.with(|cell| {
                    let mut state = cell.borrow_mut();
                    let round = state
                        .as_mut()
                        .expect("round present while an emit is in flight");
                    let adv = round.finish_predicate();
                    if adv.round_done {
                        clear_rebootstrap_cursor();
                        *state = None;
                    } else {
                        save_rebootstrap_cursor(adv.predicate_idx);
                    }
                    Ok(RebootstrapStep {
                        done: adv.round_done,
                        ingested: 0,
                    })
                });
            }
            EmitOutcome::NotEmitting => {}
        }

        // ── Scan phase ──
        REBOOTSTRAP_STATE.with(|cell| {
            let mut state = cell.borrow_mut();

            if state.is_none() {
                let mut policies: Vec<[u8; HASH_BYTES]> =
                    TRACKED_POLICIES.with(|s| s.borrow().iter().copied().collect());
                policies.sort_unstable();
                *state = Some(ReentrantRound::resume(policies, load_rebootstrap_cursor()));
            }
            let round = state.as_mut().expect("round initialised above");

            let Some(&policy) = round.current() else {
                clear_rebootstrap_cursor();
                *state = None;
                return Ok(RebootstrapStep {
                    done: true,
                    ingested: 0,
                });
            };

            let page =
                chain_data::utxos_by_policy(&policy, round.after(), COLD_START_PAGE_HINT);
            let ingested = page.refs.len() as u64;
            let anchor_slot = page.anchor_slot;
            fold_page(round.acc_mut(), &policy, &page.refs);

            match page.next {
                Some(token) => {
                    round.page_more(ingested, token);
                    Ok(RebootstrapStep {
                        done: false,
                        ingested,
                    })
                }
                None => {
                    round.page_last(ingested);
                    let total_utxos = round.items() as usize;
                    let ledger = std::mem::take(round.acc_mut());
                    let policy_hex = hex::encode(policy);
                    persist_ledger(&policy_hex, &ledger);
                    let entries: Vec<MetadataEntry> = ledger
                        .entries
                        .values()
                        .map(|stored| stored.entry.clone())
                        .collect();
                    logging::log(
                        LogLevel::Info,
                        LOG_TARGET,
                        &format!(
                            "rebootstrap policy={policy_hex}: {total_utxos} UTxO(s) → {} entry(ies) @ slot {anchor_slot}; opening chunked emit",
                            entries.len()
                        ),
                    );
                    open_chunked_emit(policy_hex, entries, anchor_slot);
                    Ok(RebootstrapStep {
                        done: false,
                        ingested,
                    })
                }
            }
        })
    }
}

export!(Module);
