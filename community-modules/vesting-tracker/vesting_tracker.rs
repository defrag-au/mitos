//! Vesting-tracker community module — watches Shield Vest /
//! CrowdLock contract addresses + payment credentials and emits
//! lock/unlock events as UTxOs at them appear and get spent.
//!
//! ## Two interest predicates
//!
//! - `AtAddress(bech32)` — project-vest contracts at a single
//!   known address. Cold-start uses `utxos_by_address`.
//! - `AtPaymentCred(28b_hex)` — CrowdLock-style sweeps where the
//!   payment part is fixed (script hash) but the staking part
//!   varies per UTxO. Cold-start uses `utxos_by_payment_cred`.
//!
//! Both register through `update_interest`; both feed the same
//! datum decoder + emission path. The wire shape carries
//! `interest_kind` so consumers know which scope a snapshot
//! replaces.
//!
//! ## Shield datum
//!
//! Both Shield project vests and CrowdLock user vests use:
//!
//! ```text
//! Constructor 0 [
//!   Int(unlock_ts_ms),
//!   List[ Bytes(owner_payment_key_hash_28b) ]
//! ]
//! ```
//!
//! The chain doesn't carry a VestStyle discriminator; the module
//! derives it from the locking TX's metadata key 674 message
//! ("Shield Vest - Crowd Lock" → CrowdLock, "Shield Vest" →
//! Shield, anything else → Unknown).
//!
//! ## Owner stake-cred resolution
//!
//! The datum carries the owner's *payment* PKH. We resolve it
//! to a stake credential via the chain-data
//! `resolve_stake_for_payment_pkh` host-fn (one redb scan over
//! the by-payment-cred index). `None` for enterprise-only
//! owners; consumers should keep the lock visible.
//!
//! ## Emission shape
//!
//! - `Snapshot` on registration / rollback — full set of current
//!   locks under one interest scope.
//! - `Locked` per `(produced UTxO, policy, asset_name)` matching
//!   a tracked address/cred at handle-events time.
//! - `Unlocked` per `consumed UTxO` matching a tracked
//!   address/cred. Identifies by `lock_ref` only — the consumer
//!   deletes the row keyed on the UTxO ref regardless of
//!   whether we'd previously surfaced it.

use std::cell::RefCell;
use std::collections::HashSet;

use mitos_community_events::vesting_tracker::{
    InterestKind, LockEntry, LockRef, SnapshotBegin, SnapshotChunk, SnapshotEnd, VestStyle,
    VestingEvent, VestingLock, VestingUnlock,
};
use mitos_module_kit::{BootstrapCursor, BootstrapIo, ChunkedBootstrap, ScannedPage};
use mitos_vesting_decode::decode_vesting_datum;
use pallas_addresses::{Address, ShelleyPaymentPart};
use pallas_codec::minicbor::data::Type as CborType;
use serde::{Deserialize, Serialize};

use crate::mitos::platform_v2::chain_data;
use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::state_kv;
use crate::mitos::platform_v2::types::{
    AssetEntry as WitAssetEntry, ChainPoint, ConsumedEvent, OutputRef as WitOutputRef,
    ProducedEvent, StakeCred as WitStakeCred, TypedDatum, UtxoEvent,
};

const LOG_TARGET: &str = "vesting-tracker-module";

const HASH_BYTES: usize = 28;

/// state-kv key under which we persist the set of currently-
/// tracked interest predicates (CBOR
/// `(Vec<String>, Vec<[u8; 28]>)`). The host-side bootstrap flag
/// is separate; this is the module's view of which scopes the
/// companion has subscribed it to.
const KV_TRACKED_INTERESTS: &str = "tracked-interests";

/// state-kv key for the re-entrant `rebootstrap` continuation
/// cursor — the index of the predicate currently being
/// re-scanned (8 BE bytes). Durable so a host restart mid-round
/// resumes at the right predicate; per-page progress within a
/// predicate is thread-local + volatile (a trap/restart restarts
/// the current predicate from page 0, safe — each predicate
/// emits a full authoritative `Snapshot`).
const KV_REBOOTSTRAP_CURSOR: &str = "rebootstrap-cursor";

/// Per-page hint for the cold-start scan. `utxos_by_address` /
/// `utxos_by_payment_cred` are paged (`WASM_BUDGET_CHUNKING.md`);
/// this is a generous upper bound — the host clamps each page to
/// its own adaptive per-call budget, so the scan never holds
/// more than one clamped page of refs at once.
const COLD_START_PAGE_HINT: u32 = 10_000;

/// Sharded lock store prefix: one kv key per lock entry,
/// `vlock:<predicate_hex>:<entry_key_hex>` → CBOR `LockEntry`, where
/// `entry_key` = `tx_hash(32) ‖ index(4 BE) ‖ asset_name` (sorts as
/// `(tx_hash, index, asset_name)` — the deterministic snapshot order,
/// without the O(n log n) single-budget sort the resident-Vec path
/// needed). `predicate_hex` namespaces each watched address / payment
/// cred. Sharded so no cold-start / recapture call ever materialises
/// the whole lock set in wasm memory. See
/// `docs/design/HOLDER_DISTRIBUTION_DRIVER_MIGRATION.md`.
const KV_LOCK_SHARD_PREFIX: &str = "vlock:";

/// Scope for the *next* `rebootstrap` pump: the predicates a
/// subscribe-time Add brought in (per-predicate auto-onboard, driven
/// by the host pump after `update-interest`). Absent ⇒ full recapture
/// over all tracked predicates. ciborium `Vec<Vec<u8>>` of encoded
/// predicates (see [`encode_predicate`]).
const KV_ONBOARD_PREDICATES: &str = "onboard-predicates";

/// Locks per `SnapshotChunk` when emitting a chunked snapshot.
/// Bounds the CBOR buffer one `emit` builds in wasm memory — see
/// `WASM_BUDGET_CHUNKING.md` "Output — chunked snapshot emission".
const SNAPSHOT_CHUNK_LOCKS: usize = 1_000;

thread_local! {
    /// Watched lock-contract addresses (bech32). Mirrors the
    /// host-side interest set; persisted via state-kv so a host
    /// restart without an attached companion keeps the module
    /// pre-armed.
    static TRACKED_ADDRESSES: RefCell<HashSet<String>> = RefCell::new(HashSet::new());

    /// Watched payment credentials (28-byte hash). Same purpose
    /// as `TRACKED_ADDRESSES` for the CrowdLock-style sweep.
    static TRACKED_PAYMENT_CREDS: RefCell<HashSet<[u8; HASH_BYTES]>> = RefCell::new(HashSet::new());

    /// In-flight re-entrant `rebootstrap` driver. `None` between
    /// rounds; rebuilt from the durable cursor after a trap. Holds no
    /// resident lock set — locks shard to state-kv per page (REPLACE),
    /// emit pages them back by key cursor. See [`ChunkedBootstrap`].
    static REBOOTSTRAP_DRIVER: RefCell<Option<ChunkedBootstrap>> = const { RefCell::new(None) };
}

/// One predicate of a `rebootstrap` round — a watched address or
/// a watched payment credential.
#[derive(Clone)]
enum RebootstrapPredicate {
    Address(String),
    PaymentCred([u8; HASH_BYTES]),
}

impl RebootstrapPredicate {
    /// Encode to the kit's opaque `Vec<u8>` predicate form:
    /// tag(1) ‖ payload. `0x00` = address utf8, `0x01` = 28-byte cred.
    fn encode(&self) -> Vec<u8> {
        match self {
            RebootstrapPredicate::Address(addr) => {
                let mut v = Vec::with_capacity(1 + addr.len());
                v.push(0x00);
                v.extend_from_slice(addr.as_bytes());
                v
            }
            RebootstrapPredicate::PaymentCred(cred) => {
                let mut v = Vec::with_capacity(1 + HASH_BYTES);
                v.push(0x01);
                v.extend_from_slice(cred);
                v
            }
        }
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        match bytes.split_first() {
            Some((0x00, rest)) => {
                Some(RebootstrapPredicate::Address(String::from_utf8(rest.to_vec()).ok()?))
            }
            Some((0x01, rest)) => {
                Some(RebootstrapPredicate::PaymentCred(<[u8; HASH_BYTES]>::try_from(rest).ok()?))
            }
            _ => None,
        }
    }

    /// The wire `(InterestKind, interest_value)` this predicate emits
    /// its snapshot under.
    fn interest(&self) -> (InterestKind, String) {
        match self {
            RebootstrapPredicate::Address(addr) => (InterestKind::Address, addr.clone()),
            RebootstrapPredicate::PaymentCred(cred) => {
                (InterestKind::PaymentCred, hex::encode(cred))
            }
        }
    }
}

// ── Sharded per-lock store ──
// `vlock:<predicate_hex>:<entry_key_hex>` → CBOR `LockEntry`. The kv
// keys are the source of truth; the kit driver pages them.

/// Sortable per-lock shard key: `tx_hash(32) ‖ index(4 BE) ‖
/// asset_name`. Sorts as `(tx_hash, index, asset_name)` — the
/// deterministic snapshot order the resident-Vec path achieved with a
/// full sort.
fn lock_entry_key(lock: &LockEntry) -> Vec<u8> {
    let tx = hex::decode(&lock.utxo_ref.tx_hash).unwrap_or_default();
    let name = hex::decode(&lock.asset_name_hex).unwrap_or_default();
    let mut k = Vec::with_capacity(tx.len() + 4 + name.len());
    k.extend_from_slice(&tx);
    k.extend_from_slice(&lock.utxo_ref.index.to_be_bytes());
    k.extend_from_slice(&name);
    k
}

fn lock_shard_prefix(predicate_hex: &str) -> String {
    format!("{KV_LOCK_SHARD_PREFIX}{predicate_hex}:")
}
fn lock_shard_kv_key(predicate_hex: &str, entry_key: &[u8]) -> String {
    format!("{KV_LOCK_SHARD_PREFIX}{predicate_hex}:{}", hex::encode(entry_key))
}

// ============================================================
// Wire-side interest predicate mirror
// ============================================================

/// Local mirror of the on-wire `InterestPredicate` enum — same
/// pattern as holder-distribution. We capture `AtAddress` +
/// `AtPaymentCred` and step over the others.
#[derive(Debug, Deserialize)]
enum InterestPredicateWire {
    AtAddress(String),
    /// CBOR array of u8 (serde default for `[u8; 28]`); we
    /// length-validate to 28 at use.
    AtPaymentCred(Vec<u8>),
    AtStakeCred(serde::de::IgnoredAny),
    HoldsPolicy(serde::de::IgnoredAny),
    HoldsAsset {
        #[allow(dead_code)]
        policy: serde::de::IgnoredAny,
        #[allow(dead_code)]
        asset_name: serde::de::IgnoredAny,
    },
    TickEvery(#[allow(dead_code)] u32),
}

// ============================================================
// Persisted tracked-interests blob
// ============================================================

#[derive(Default, Serialize, Deserialize)]
struct PersistedInterests {
    addresses: Vec<String>,
    payment_creds: Vec<String>, // hex-encoded 28-byte
}

fn persist_tracked_interests() {
    let mut blob = PersistedInterests::default();
    TRACKED_ADDRESSES.with(|set| {
        blob.addresses = set.borrow().iter().cloned().collect();
    });
    TRACKED_PAYMENT_CREDS.with(|set| {
        blob.payment_creds = set.borrow().iter().map(hex::encode).collect();
    });
    let mut buf = Vec::with_capacity(256);
    if ciborium::ser::into_writer(&blob, &mut buf).is_ok() {
        state_kv::set_value(KV_TRACKED_INTERESTS, &buf);
    }
}

fn restore_tracked_interests() {
    let Some(bytes) = state_kv::get_value(KV_TRACKED_INTERESTS) else {
        return;
    };
    let blob: PersistedInterests = match ciborium::de::from_reader(bytes.as_slice()) {
        Ok(v) => v,
        Err(_) => return,
    };
    let mut addr_count = 0usize;
    TRACKED_ADDRESSES.with(|set| {
        let mut set = set.borrow_mut();
        set.clear();
        for a in blob.addresses {
            set.insert(a);
            addr_count += 1;
        }
    });
    let mut cred_count = 0usize;
    TRACKED_PAYMENT_CREDS.with(|set| {
        let mut set = set.borrow_mut();
        set.clear();
        for h in blob.payment_creds {
            if let Ok(bytes) = hex::decode(&h) {
                if bytes.len() == HASH_BYTES {
                    let mut arr = [0u8; HASH_BYTES];
                    arr.copy_from_slice(&bytes);
                    set.insert(arr);
                    cred_count += 1;
                }
            }
        }
    });
    logging::log(
        LogLevel::Info,
        LOG_TARGET,
        &format!("restored {addr_count} address(es) + {cred_count} payment-cred(s) from state-kv"),
    );
}

// The re-entrant `rebootstrap` continuation cursor is owned by the
// shared [`ChunkedBootstrap`] driver (kit-encoded), persisted via
// [`VestingIo`]'s `load_cursor` / `save_cursor` / `clear_cursor` on
// `KV_REBOOTSTRAP_CURSOR`.

// Vesting lock datum decode lives in the shared
// `mitos_vesting_decode` crate (imported above) so this module
// and `holder-distribution`'s vesting decomposition share one
// source of truth. `VestingDatum` is the rename of the local
// `DecodedDatum` from the pre-extraction layout.

/// Resolve a `TypedDatum` to its raw CBOR bytes. Inline datums
/// carry `payload` directly; hash-only datums fall back to
/// `chain_data::datum_by_hash`.
fn resolve_datum_bytes(d: Option<&TypedDatum>) -> Option<Vec<u8>> {
    let d = d?;
    if !d.payload.is_empty() {
        return Some(d.payload.clone());
    }
    chain_data::datum_by_hash(&d.hash)
}

// ============================================================
// VestStyle from TX metadata 674
// ============================================================

/// Look up metadata key 674's `msg` array and classify the
/// vest style. Returns `VestStyle::Unknown` when:
/// - the TX has no aux data
/// - aux data has no label 674
/// - the value at 674 isn't `{ "msg": [<string>...] }`
/// - the msg strings don't match either prefix
fn vest_style_from_tx(tx_hash: &[u8]) -> VestStyle {
    let Some(aux) = chain_data::tx_metadata(&tx_hash.to_vec()) else {
        return VestStyle::Unknown;
    };
    classify_vest_style_msg(&aux).unwrap_or(VestStyle::Unknown)
}

/// Inner: walks aux-data CBOR (handling both the Alonzo+ tagged
/// map form and the pre-Alonzo array form) to find key 674's
/// `msg` array, then matches the first string. Returns `None`
/// on any structural mismatch.
fn classify_vest_style_msg(aux: &[u8]) -> Option<VestStyle> {
    let mut d = pallas_codec::minicbor::Decoder::new(aux);

    // Optional outer tag (Alonzo+ `#6.259`); strip it.
    let was_tagged = matches!(d.datatype().ok()?, CborType::Tag);
    if was_tagged {
        d.tag().ok()?;
    }

    // Two top-level shapes: tagged → `{ 0 => metadata_map, 1 => ... }`,
    // pre-Alonzo → `[metadata_map, native_scripts]`, or bare
    // metadata map.
    let dt = d.datatype().ok()?;
    if dt == CborType::Array || dt == CborType::ArrayIndef {
        let _arr = d.array().ok()?;
        if d.datatype().ok()? == CborType::Null {
            return None;
        }
        // cursor now on metadata_map
    } else if was_tagged {
        // Outer map keyed by ints; walk to key 0.
        let len = d.map().ok()?;
        let mut i = 0u64;
        let mut found = false;
        loop {
            if let Some(n) = len {
                if i >= n {
                    break;
                }
            }
            if len.is_none() && d.datatype().ok()? == CborType::Break {
                d.skip().ok()?;
                break;
            }
            let key: u64 = d.u64().ok()?;
            if key == 0 {
                found = true;
                break;
            }
            d.skip().ok()?;
            i += 1;
        }
        if !found {
            return None;
        }
    }
    // Else bare-map shape; cursor already on metadata_map.

    // Walk metadata_map to find label 674.
    let len = d.map().ok()?;
    let mut i = 0u64;
    let mut found_674 = false;
    loop {
        if let Some(n) = len {
            if i >= n {
                break;
            }
        }
        if len.is_none() && d.datatype().ok()? == CborType::Break {
            d.skip().ok()?;
            break;
        }
        let key: u64 = d.u64().ok()?;
        if key == 674 {
            found_674 = true;
            break;
        }
        d.skip().ok()?;
        i += 1;
    }
    if !found_674 {
        return None;
    }

    // Value at 674: `{ "msg": [<string>...] }`. Walk to find
    // the `msg` key, then iterate the array's first string.
    let value_len = d.map().ok()?;
    let mut i = 0u64;
    let mut msg_str: Option<String> = None;
    loop {
        if let Some(n) = value_len {
            if i >= n {
                break;
            }
        }
        if value_len.is_none() && d.datatype().ok()? == CborType::Break {
            d.skip().ok()?;
            break;
        }
        // CIP-25/674 keys are strings, but accept bytes too as a
        // belt-and-braces measure.
        let key_match = match d.datatype().ok()? {
            CborType::String => d.str().ok()? == "msg",
            CborType::Bytes => d.bytes().ok()? == b"msg",
            _ => {
                d.skip().ok()?;
                d.skip().ok()?;
                i += 1;
                continue;
            }
        };
        if key_match {
            // Value should be a list of strings; concatenate
            // them and look for "Crowd Lock" / "Shield".
            if matches!(d.datatype().ok()?, CborType::Array | CborType::ArrayIndef) {
                let arr_len = d.array().ok()?;
                let mut joined = String::new();
                let mut j = 0u64;
                loop {
                    if let Some(n) = arr_len {
                        if j >= n {
                            break;
                        }
                    }
                    if arr_len.is_none() && d.datatype().ok()? == CborType::Break {
                        d.skip().ok()?;
                        break;
                    }
                    match d.datatype().ok()? {
                        CborType::String => {
                            let s = d.str().ok()?;
                            joined.push_str(s);
                            joined.push(' ');
                        }
                        _ => {
                            d.skip().ok()?;
                        }
                    }
                    j += 1;
                }
                msg_str = Some(joined);
            } else if matches!(d.datatype().ok()?, CborType::String) {
                msg_str = Some(d.str().ok()?.to_owned());
            }
            break;
        }
        d.skip().ok()?;
        i += 1;
    }
    let msg = msg_str?;
    if msg.contains("Crowd Lock") {
        Some(VestStyle::CrowdLock)
    } else if msg.contains("Shield") {
        Some(VestStyle::Shield)
    } else {
        None
    }
}

// ============================================================
// Address → payment-cred extraction (for AtPaymentCred matching)
// ============================================================

/// Return the 28-byte payment credential of a bech32 Cardano
/// address, regardless of key vs script.
fn payment_cred_bytes(address: &str) -> Option<[u8; HASH_BYTES]> {
    let addr = Address::from_bech32(address).ok()?;
    let shelley = match addr {
        Address::Shelley(s) => s,
        _ => return None,
    };
    match shelley.payment() {
        ShelleyPaymentPart::Key(h) => Some((**h).into()),
        ShelleyPaymentPart::Script(h) => Some((**h).into()),
    }
}

// ============================================================
// Match output against tracked interests
// ============================================================

/// Decide whether an output matches any tracked interest, and
/// which (interest_kind, interest_value) it matched. Returns the
/// FIRST matching scope — if the same physical address is
/// watched both directly (AtAddress) AND via its payment-cred
/// (AtPaymentCred), AtAddress wins (more specific).
fn match_interest(address: &str) -> Option<(InterestKind, String)> {
    let addr_match = TRACKED_ADDRESSES.with(|set| set.borrow().contains(address));
    if addr_match {
        return Some((InterestKind::Address, address.to_owned()));
    }
    let cred = payment_cred_bytes(address)?;
    let cred_match = TRACKED_PAYMENT_CREDS.with(|set| set.borrow().contains(&cred));
    if cred_match {
        return Some((InterestKind::PaymentCred, hex::encode(cred)));
    }
    None
}

// ============================================================
// Build LockEntry from a UTxO
// ============================================================

/// Decode a UTxO's datum + metadata into one `LockEntry` per
/// non-lovelace asset. Returns empty Vec when the datum can't be
/// decoded or the UTxO carries no non-lovelace assets.
fn build_lock_entries(
    address: &str,
    oref_tx_hash: &[u8],
    oref_index: u32,
    assets: &[WitAssetEntry],
    datum: Option<&TypedDatum>,
    vest_style: VestStyle,
) -> Vec<LockEntry> {
    let Some(datum_bytes) = resolve_datum_bytes(datum) else {
        return Vec::new();
    };
    let Some(decoded) = decode_vesting_datum(&datum_bytes) else {
        return Vec::new();
    };

    let owner_stake_cred_hex = resolve_owner_stake(&decoded.owner_pkh_hex);

    let tx_hash_hex = hex::encode(oref_tx_hash);
    let mut entries = Vec::new();
    for asset in assets {
        // Skip lovelace — assets list has only non-lovelace
        // entries already in the WIT shape, but defensive in
        // case of future shape drift.
        if asset.asset.policy.is_empty() {
            continue;
        }
        entries.push(LockEntry {
            utxo_ref: LockRef {
                tx_hash: tx_hash_hex.clone(),
                index: oref_index,
            },
            lock_address: address.to_owned(),
            policy: hex::encode(&asset.asset.policy),
            asset_name_hex: hex::encode(&asset.asset.name),
            amount: asset.quantity,
            owner_pkh: decoded.owner_pkh_hex.clone(),
            owner_stake_cred_hex: owner_stake_cred_hex.clone(),
            unlock_ts_ms: decoded.unlock_ts_ms,
            vest_style,
            locked_at_tx: tx_hash_hex.clone(),
        });
    }
    entries
}

/// Resolve a payment PKH → stake-cred hex via the host-fn.
/// Returns `None` for enterprise-only owners.
fn resolve_owner_stake(owner_pkh_hex: &str) -> Option<String> {
    let pkh = hex::decode(owner_pkh_hex).ok()?;
    let resolved = chain_data::resolve_stake_for_payment_pkh(&pkh)?;
    let bytes = match resolved {
        WitStakeCred::KeyHash(b) | WitStakeCred::ScriptHash(b) => b,
    };
    Some(hex::encode(bytes))
}

// ============================================================
// Cold-start (onboard scope → host-pumped chunked rebootstrap)
// ============================================================

/// Record `added` predicates as the scope for the next `rebootstrap`
/// pump. Merges with any still-pending scope (rapid successive adds
/// all cold-start) and resets the durable cursor + in-process driver
/// so the pump restarts cleanly over the scoped set. Mirrors
/// collection-holders / holder-distribution — the inline single-fuel
/// cold-start (resident Vec + sort) is gone; the host pumps the
/// budget-safe chunked rebootstrap over just these predicates.
fn seed_onboard_scope(added: &[RebootstrapPredicate]) {
    let mut scope: Vec<Vec<u8>> = read_onboard_scope().unwrap_or_default();
    for p in added {
        let enc = p.encode();
        if !scope.contains(&enc) {
            scope.push(enc);
        }
    }
    scope.sort_unstable();
    let mut buf = Vec::with_capacity(64 + scope.len() * 40);
    if ciborium::ser::into_writer(&scope, &mut buf).is_ok() {
        state_kv::set_value(KV_ONBOARD_PREDICATES, &buf);
    }
    state_kv::delete_value(KV_REBOOTSTRAP_CURSOR);
    REBOOTSTRAP_DRIVER.with(|c| *c.borrow_mut() = None);
}

/// Decode the pending onboard scope (encoded predicates), if any.
fn read_onboard_scope() -> Option<Vec<Vec<u8>>> {
    let bytes = state_kv::get_value(KV_ONBOARD_PREDICATES)?;
    ciborium::de::from_reader(&bytes[..]).ok()
}

/// Common cold-start body: bulk-resolve each ref's output +
/// datum, derive VestStyle from the producing TX's metadata,
/// build per-asset LockEntries.
fn build_snapshot_locks(refs: &[WitOutputRef]) -> Vec<LockEntry> {
    if refs.is_empty() {
        return Vec::new();
    }
    let mut all_locks = Vec::with_capacity(refs.len());
    for chunk in refs.chunks(1024) {
        // `read_utxos` returns each output paired with its own
        // ref, and *not* in input order. Re-derive the ref list
        // from that returned order so `read_output_datums`
        // (which is positionally parallel to its input) lines up
        // 1:1 with the outputs.
        let outputs = chain_data::read_utxos(&chunk.to_vec());
        let output_refs: Vec<WitOutputRef> = outputs.iter().map(|(r, _)| r.clone()).collect();
        let datums = chain_data::read_output_datums(&output_refs);
        for ((oref, out), datum) in outputs.iter().zip(datums.iter()) {
            let style = vest_style_from_tx(&oref.tx_hash);
            let entries = build_lock_entries(
                &out.address,
                &oref.tx_hash,
                oref.index,
                &out.assets,
                datum.as_ref(),
                style,
            );
            all_locks.extend(entries);
        }
    }
    all_locks
}

// Snapshot emission is driven by the shared [`ChunkedBootstrap`]
// driver via [`VestingIo`]'s `emit_begin` / `emit_chunk` / `emit_end`
// — the locks are sharded (sorted by `lock_entry_key`) so the emit
// pages them in order, with no resident lock-list + sort.

// ============================================================
// Live event handling
// ============================================================

fn handle_produced(p: &ProducedEvent) {
    let Some((interest_kind, interest_value)) = match_interest(&p.output.address) else {
        return;
    };
    let style = vest_style_from_tx(&p.tx_hash);
    let entries = build_lock_entries(
        &p.output.address,
        &p.tx_hash,
        p.oref.index,
        &p.output.assets,
        p.datum.as_ref(),
        style,
    );
    for lock in entries {
        emit_event(&VestingEvent::Locked(VestingLock {
            interest_kind,
            interest_value: interest_value.clone(),
            lock,
        }));
    }
}

fn handle_consumed(c: &ConsumedEvent) {
    let Some((interest_kind, interest_value)) = match_interest(&c.prior_output.address) else {
        return;
    };
    let slot = match &c.cursor {
        ChainPoint::Specific(sp) => sp.slot,
        ChainPoint::SlotOnly(s) => *s,
        ChainPoint::Origin => 0,
    };
    let lock_ref = LockRef {
        tx_hash: hex::encode(&c.oref.tx_hash),
        index: c.oref.index,
    };
    emit_event(&VestingEvent::Unlocked(VestingUnlock {
        interest_kind,
        interest_value,
        lock_ref,
        consuming_tx_hash: hex::encode(&c.consuming_tx_hash),
        slot,
    }));
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

    let mut new_addresses: Vec<String> = Vec::new();
    let mut new_creds: Vec<[u8; HASH_BYTES]> = Vec::new();
    for pred in predicates {
        match pred {
            InterestPredicateWire::AtAddress(s) => new_addresses.push(s),
            InterestPredicateWire::AtPaymentCred(b) => {
                if b.len() == HASH_BYTES {
                    let mut arr = [0u8; HASH_BYTES];
                    arr.copy_from_slice(&b);
                    new_creds.push(arr);
                }
            }
            _ => {}
        }
    }

    let mut added_addresses: Vec<String> = Vec::new();
    let mut added_creds: Vec<[u8; HASH_BYTES]> = Vec::new();

    match op {
        InterestOp::Replace => {
            TRACKED_ADDRESSES.with(|set| {
                let mut set = set.borrow_mut();
                let prev = std::mem::take(&mut *set);
                for a in &new_addresses {
                    set.insert(a.clone());
                    if !prev.contains(a) {
                        added_addresses.push(a.clone());
                    }
                }
            });
            TRACKED_PAYMENT_CREDS.with(|set| {
                let mut set = set.borrow_mut();
                let prev = std::mem::take(&mut *set);
                for c in &new_creds {
                    set.insert(*c);
                    if !prev.contains(c) {
                        added_creds.push(*c);
                    }
                }
            });
        }
        InterestOp::Add => {
            TRACKED_ADDRESSES.with(|set| {
                let mut set = set.borrow_mut();
                for a in &new_addresses {
                    if set.insert(a.clone()) {
                        added_addresses.push(a.clone());
                    }
                }
            });
            TRACKED_PAYMENT_CREDS.with(|set| {
                let mut set = set.borrow_mut();
                for c in &new_creds {
                    if set.insert(*c) {
                        added_creds.push(*c);
                    }
                }
            });
        }
        InterestOp::Remove => {
            TRACKED_ADDRESSES.with(|set| {
                let mut set = set.borrow_mut();
                for a in &new_addresses {
                    set.remove(a);
                }
            });
            TRACKED_PAYMENT_CREDS.with(|set| {
                let mut set = set.borrow_mut();
                for c in &new_creds {
                    set.remove(c);
                }
            });
            // Drop the sharded lock state for each removed predicate.
            for a in &new_addresses {
                let pred = RebootstrapPredicate::Address(a.clone());
                state_kv::delete_prefix(&lock_shard_prefix(&hex::encode(pred.encode())));
            }
            for c in &new_creds {
                let pred = RebootstrapPredicate::PaymentCred(*c);
                state_kv::delete_prefix(&lock_shard_prefix(&hex::encode(pred.encode())));
            }
        }
    }

    persist_tracked_interests();

    // Cold-start each newly-added predicate via the budget-safe chunked
    // `rebootstrap` pump (the host drives it after `update-interest`)
    // rather than an inline single-fuel scan: record the added
    // predicates as the next rebootstrap's scope.
    let added: Vec<RebootstrapPredicate> = added_addresses
        .into_iter()
        .map(RebootstrapPredicate::Address)
        .chain(added_creds.into_iter().map(RebootstrapPredicate::PaymentCred))
        .collect();
    if !added.is_empty() {
        seed_onboard_scope(&added);
    }
}

// ============================================================
// Emit
// ============================================================

fn emit_event(event: &VestingEvent) {
    let mut buf = Vec::with_capacity(2048);
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode VestingEvent failed: {e}"),
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
        restore_tracked_interests();
    }

    fn handle_events(events: Vec<DispatchEvent>) {
        let any_tracked = TRACKED_ADDRESSES.with(|s| !s.borrow().is_empty())
            || TRACKED_PAYMENT_CREDS.with(|s| !s.borrow().is_empty());
        if !any_tracked {
            return;
        }
        for event in events {
            match event {
                DispatchEvent::Utxo(UtxoEvent::Produced(p)) => handle_produced(&p),
                DispatchEvent::Utxo(UtxoEvent::Consumed(c)) => handle_consumed(&c),
                _ => {}
            }
        }
    }

    fn update_interest(op: InterestOp, items_cbor: Vec<u8>) -> Result<(), String> {
        apply_interest_update(op, &items_cbor);
        Ok(())
    }

    /// Re-emit lock snapshots for watched addresses + payment
    /// credentials via the shared re-entrant [`ChunkedBootstrap`]
    /// driver: scan a page → shard each lock (REPLACE) → emit one
    /// `SnapshotChunk` per call from the sorted shards. No resident
    /// lock set + sort; every progress field is durable.
    fn rebootstrap() -> Result<RebootstrapStep, String> {
        REBOOTSTRAP_DRIVER.with(|cell| {
            let mut slot = cell.borrow_mut();
            if slot.is_none() {
                // Scope: a pending onboard set (subscribe-time Add)
                // cold-starts just those predicates; else a full
                // recapture over every tracked predicate. Predicates
                // are kit-encoded `Vec<u8>` (addresses, then creds).
                let predicates: Vec<Vec<u8>> = read_onboard_scope().unwrap_or_else(|| {
                    let mut addrs: Vec<String> =
                        TRACKED_ADDRESSES.with(|s| s.borrow().iter().cloned().collect());
                    addrs.sort_unstable();
                    let mut creds: Vec<[u8; HASH_BYTES]> =
                        TRACKED_PAYMENT_CREDS.with(|s| s.borrow().iter().copied().collect());
                    creds.sort_unstable();
                    let mut preds: Vec<Vec<u8>> = addrs
                        .into_iter()
                        .map(|a| RebootstrapPredicate::Address(a).encode())
                        .collect();
                    preds.extend(
                        creds
                            .into_iter()
                            .map(|c| RebootstrapPredicate::PaymentCred(c).encode()),
                    );
                    preds
                });
                let mut predicates = predicates;
                predicates.sort_unstable();
                let cursor = state_kv::get_value(KV_REBOOTSTRAP_CURSOR)
                    .and_then(|b| BootstrapCursor::decode(&b));
                *slot = Some(ChunkedBootstrap::resume(predicates, cursor));
            }
            let driver = slot.as_mut().expect("driver initialised above");
            let mut io = VestingIo;
            let out = driver.step(&mut io);
            if out.done {
                *slot = None;
                state_kv::delete_value(KV_ONBOARD_PREDICATES);
            }
            Ok(RebootstrapStep {
                done: out.done,
                ingested: out.ingested,
            })
        })
    }
}

/// `BootstrapIo` adapter wiring the kit driver to vesting-tracker's
/// host-fns. REPLACE semantics — each lock UTxO is a distinct entry
/// (no SUM, no decomp). Predicates are kit-encoded
/// [`RebootstrapPredicate`]s; shards are namespaced per predicate.
struct VestingIo;

impl BootstrapIo for VestingIo {
    type Entry = LockEntry;

    fn scan_page(&mut self, predicate: &[u8], after: Option<&[u8]>) -> ScannedPage<LockEntry> {
        let page = match RebootstrapPredicate::decode(predicate) {
            Some(RebootstrapPredicate::Address(addr)) => {
                chain_data::utxos_by_address(&addr, after, COLD_START_PAGE_HINT)
            }
            Some(RebootstrapPredicate::PaymentCred(cred)) => {
                chain_data::utxos_by_payment_cred(&cred, after, COLD_START_PAGE_HINT)
            }
            None => {
                return ScannedPage {
                    entries: Vec::new(),
                    next: None,
                    anchor_slot: 0,
                    ingested: 0,
                };
            }
        };
        let ingested = page.refs.len() as u64;
        let entries: Vec<(Vec<u8>, LockEntry)> = build_snapshot_locks(&page.refs)
            .into_iter()
            .map(|lock| (lock_entry_key(&lock), lock))
            .collect();
        ScannedPage {
            entries,
            next: page.next,
            anchor_slot: page.anchor_slot,
            ingested,
        }
    }

    fn shard_get_many(&self, predicate: &[u8], keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
        // REPLACE (`accumulates = false`) — the kit never reads priors.
        let _ = (predicate, keys);
        keys.iter().map(|_| None).collect()
    }
    fn shard_put_many(&mut self, predicate: &[u8], entries: &[(Vec<u8>, Vec<u8>)]) {
        let predicate_hex = hex::encode(predicate);
        let kv_entries: Vec<(String, Vec<u8>)> = entries
            .iter()
            .map(|(k, v)| (lock_shard_kv_key(&predicate_hex, k), v.clone()))
            .collect();
        state_kv::set_many(&kv_entries);
    }
    fn shard_scan(
        &self,
        predicate: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let predicate_hex = hex::encode(predicate);
        let prefix = lock_shard_prefix(&predicate_hex);
        let after_key = after.map(|a| lock_shard_kv_key(&predicate_hex, a));
        state_kv::kv_scan(&prefix, after_key.as_deref(), limit as u32)
            .into_iter()
            .filter_map(|(k, v)| {
                k.strip_prefix(prefix.as_str())
                    .and_then(|h| hex::decode(h).ok())
                    .map(|ek| (ek, v))
            })
            .collect()
    }
    fn shard_clear(&mut self, predicate: &[u8]) {
        state_kv::delete_prefix(&lock_shard_prefix(&hex::encode(predicate)));
    }

    fn encode_entry(&self, entry: &LockEntry) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);
        let _ = ciborium::ser::into_writer(entry, &mut buf);
        buf
    }
    fn decode_entry(&self, bytes: &[u8]) -> Option<LockEntry> {
        ciborium::de::from_reader(bytes).ok()
    }
    // REPLACE — each lock UTxO is distinct; default `accumulates=false`
    // + `merge`=replace are correct.

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
        let (interest_kind, interest_value) = predicate_interest(predicate);
        emit_event(&VestingEvent::SnapshotBegin(SnapshotBegin {
            interest_kind,
            interest_value,
            cursor_slot: anchor_slot,
            cursor_hash_hex: String::new(),
        }));
    }
    fn emit_chunk(&mut self, predicate: &[u8], entries: Vec<LockEntry>) {
        let (interest_kind, interest_value) = predicate_interest(predicate);
        emit_event(&VestingEvent::SnapshotChunk(SnapshotChunk {
            interest_kind,
            interest_value,
            locks: entries,
        }));
    }
    fn emit_end(&mut self, predicate: &[u8], total: u64) {
        let (interest_kind, interest_value) = predicate_interest(predicate);
        emit_event(&VestingEvent::SnapshotEnd(SnapshotEnd {
            interest_kind,
            interest_value,
            lock_count: total,
        }));
    }
    fn chunk_size(&self) -> usize {
        SNAPSHOT_CHUNK_LOCKS
    }
}

/// Resolve a kit predicate to the wire `(InterestKind, value)`; a
/// malformed predicate (shouldn't happen) degrades to an Address scope
/// with an empty value rather than panicking the emit.
fn predicate_interest(predicate: &[u8]) -> (InterestKind, String) {
    RebootstrapPredicate::decode(predicate)
        .map(|p| p.interest())
        .unwrap_or((InterestKind::Address, String::new()))
}

export!(Module);
