//! Holder-distribution community module — emits a current
//! holder ledger per tracked policy and per-TX deltas as
//! holdings change.
//!
//! ## Lifecycle
//!
//! 1. Consumer registers `holds_policy(X)` via
//!    `update_interest(Add, ...)`. The module:
//!    - Calls `chain_data::utxos_by_policy(X)` to enumerate
//!      every current UTxO holding the policy (dolos's
//!      `BY_POLICY` index — sub-second even for active
//!      policies).
//!    - Reads each UTxO's address + asset multiset, splits the
//!      policy's assets into the address's stake credential,
//!      and accumulates a per-stake-cred ledger.
//!    - Persists the ledger to state-kv keyed by policy hex.
//!    - Emits the ledger as a chunked `SnapshotBegin` →
//!      `SnapshotChunk` × N → `SnapshotEnd` sequence.
//!
//! 2. Each subsequent `handle_events` batch (one Cardano TX):
//!    - Walks Produced + Consumed events for tracked policies.
//!    - Computes per-stake-cred delta from the TX's net effect.
//!    - Persists the updated ledger.
//!    - Emits a `HolderEvent::Delta` enumerating the changed
//!      holders' new post-TX balances.
//!
//! 3. Removing a `holds_policy(X)` (`update_interest(Remove,
//!    ...)`) clears the in-memory tracking set; the persisted
//!    ledger stays in state-kv (cheap, no GC in Phase 1 —
//!    explicit re-add re-uses the prior ledger as a cache).
//!
//! ## Address → holder identity
//!
//! Uses `pallas-addresses::Address::from_bech32` to parse each
//! output's bech32. Shelley addresses with a Key or Script
//! delegation part become `HolderId::Stake(hex)` — grouped by
//! the 28-byte staking credential. Shelley enterprise addresses
//! (no delegation part) become `HolderId::Enterprise(addr)` —
//! each is its own holder, so the worker can classify burn
//! sinks and other config-flagged enterprise contracts.
//! Pointer-stake (rare, ~1%) and Byron addresses are dropped.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};

use mitos_community_events::holder_distribution::{
    AssetBalance, HolderDelta, HolderEntry, HolderEvent, HolderId, HolderRole, SnapshotBegin,
    SnapshotChunk, SnapshotEnd,
};
use mitos_community_events::vesting_tracker::{LockEntry, LockRef, VestStyle};
use mitos_dex_decode::{cswap, lp_share, splash};
use mitos_module_kit::ReentrantRound;
use mitos_vesting_decode::{crowd_lock, decode_vesting_datum};
use pallas_addresses::{Address, ShelleyDelegationPart, ShelleyPaymentPart};
use serde::{Deserialize, Serialize};

use crate::mitos::platform_v2::chain_data;
use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::state_kv;
use crate::mitos::platform_v2::types::{
    AssetEntry as WitAssetEntry, ConsumedEvent, OutputRef as WitOutputRef, ProducedEvent,
    StakeCred as WitStakeCred, TypedDatum, UtxoEvent,
};

const LOG_TARGET: &str = "holder-distribution-module";

/// state-kv key under which we persist the set of currently-
/// tracked policies (CBOR list of 56-char hex strings). The
/// per-policy ledger is keyed `ledger:<policy_hex>`.
const KV_TRACKED_POLICIES: &str = "tracked-policies";

/// state-kv key for the re-entrant `rebootstrap` continuation
/// cursor — the index of the predicate currently being
/// re-scanned (8 BE bytes). Durable so a host restart mid-round
/// resumes at the right predicate. Per-page progress within a
/// predicate is thread-local + volatile; a trap or restart
/// restarts the current predicate from page 0 (safe — each
/// predicate emits a full authoritative `Snapshot`).
const KV_REBOOTSTRAP_CURSOR: &str = "rebootstrap-cursor";

/// Per-page hint for the cold-start scan. `utxos_by_policy` is
/// paged (see `WASM_BUDGET_CHUNKING.md`); this is a generous
/// upper bound — the host clamps each returned page to its own
/// adaptive per-call budget, so the module never holds more than
/// one clamped page of refs at once.
const COLD_START_PAGE_HINT: u32 = 10_000;

/// Holders per `SnapshotChunk` when emitting a chunked snapshot.
/// Bounds the CBOR buffer one `emit` builds in wasm memory — see
/// `WASM_BUDGET_CHUNKING.md` "Output — chunked snapshot emission".
const SNAPSHOT_CHUNK_HOLDERS: usize = 1_000;

/// 28-byte hash size used everywhere for stake credentials,
/// policy ids, payment hashes.
const HASH_BYTES: usize = 28;

thread_local! {
    /// Currently-tracked policy ids (28-byte hashes). Cleared on
    /// `Remove`/`Replace` ops, repopulated on `Add`. Persisted
    /// via `KV_TRACKED_POLICIES` so a host restart without an
    /// attached companion keeps filtering correctly.
    static TRACKED_POLICIES: RefCell<HashSet<[u8; HASH_BYTES]>> = RefCell::new(HashSet::new());

    /// In-flight `rebootstrap` round. `None` between rounds.
    /// `ReentrantRound` (from `mitos-module-kit`) owns the
    /// predicate list, the `predicate_idx`, the page cursor, and
    /// the per-predicate `ScanAcc` accumulator. Resident in
    /// the wasm instance across the host's re-entrant call loop;
    /// a trap or host restart discards it and the round resumes
    /// from the durable state-kv cursor (`predicate_idx`).
    static REBOOTSTRAP_STATE: RefCell<Option<ReentrantRound<[u8; HASH_BYTES], ScanAcc>>> =
        RefCell::new(None);

    /// In-progress LP-pool decomposition for the predicate whose
    /// holder scan just finished and which has a DEX pool. `None`
    /// when no decomposition is in flight. Each `rebootstrap`
    /// call scans one page of the LP-token holder set; when the
    /// last page lands the ledger is decomposed and the emit
    /// opens. See `rebootstrap` / `begin_decomp` and
    /// `docs/design/HOLDER_DISTRIBUTION_LP_DECOMPOSITION.md`.
    static REBOOTSTRAP_DECOMP: RefCell<Option<DecompState>> = RefCell::new(None);

    /// In-progress chunked-snapshot emit for the predicate whose
    /// scan just finished. `None` when no emit is in flight. The
    /// `rebootstrap` state machine drains one `SnapshotChunk`
    /// from here per call — emitting the whole holder list in a
    /// single fuel budget traps for a large policy (prod
    /// recapture, 2026-05-19). See `rebootstrap` / `begin_emit`.
    static REBOOTSTRAP_EMIT: RefCell<Option<EmitState>> = RefCell::new(None);
}

/// Per-predicate `rebootstrap` scan accumulator. The holder
/// ledger built page-by-page, plus the output-ref of the DEX
/// pool UTxO if one was spotted among the holders (auto-discovery
/// — the pool is just a holder of the policy whose address is a
/// known DEX pool script). Volatile — discarded on trap.
#[derive(Default)]
struct ScanAcc {
    ledger: PolicyLedger,
    /// First-spotted DEX pool UTxO holding this policy, if any.
    pool_ref: Option<WitOutputRef>,
    /// Output-refs of CrowdLock vesting locks holding this
    /// policy, accumulated across the holder scan. Drives the
    /// vesting-decomposition step at scan-end.
    vesting_lock_refs: Vec<WitOutputRef>,
}

/// Per-page result from `fold_page`. The scan caller merges
/// these into its running accumulator (`ScanAcc` for
/// `rebootstrap`, local fields for `cold_start`).
#[derive(Default)]
struct FoldHits {
    pool_ref: Option<WitOutputRef>,
    vesting_lock_refs: Vec<WitOutputRef>,
}

/// An LP-pool decomposition mid-scan: the raw holder ledger of
/// the policy, plus the LP-token holder enumeration accumulating
/// page by page. When the LP scan finishes, the ledger is
/// decomposed (pool aggregate redistributed to LP providers) and
/// the decomposed holder list is handed to the chunked emit.
struct DecompState {
    /// 56-char hex policy id.
    policy_hex: String,
    /// Frozen-scan tip the snapshot is consistent as-of.
    anchor_slot: u64,
    /// UTxO count from the holder scan — for the log line.
    total_utxos: usize,
    /// The raw holder ledger (pool aggregate intact). Persisted
    /// as-is; decomposition transforms only the *emitted* list.
    ledger: PolicyLedger,
    /// Total LP supply in circulation, from the pool datum — the
    /// share denominator.
    total_lp_tokens: u64,
    /// LP-token policy id; the `utxos_by_policy` scan target.
    lp_policy: [u8; HASH_BYTES],
    /// Page cursor for the LP-token holder scan.
    lp_after: Option<Vec<u8>>,
    /// LP-token holdings accumulated so far: per-holder
    /// LP-token quantity.
    lp_holders: BTreeMap<LedgerKey, u64>,
    /// CrowdLock vesting-lock UTxO refs collected during the
    /// holder scan; the vesting-decomposition step processes
    /// them when the LP scan completes.
    vesting_lock_refs: Vec<WitOutputRef>,
    /// Holder's-own policy id, kept for the vesting-decomp call
    /// (`decompose_vesting` needs it to filter assets).
    policy: [u8; HASH_BYTES],
}

/// A chunked snapshot mid-emit: the materialised holder list for
/// one predicate, drained `SNAPSHOT_CHUNK_HOLDERS` at a time.
struct EmitState {
    /// 56-char hex policy id — every event of the sequence
    /// carries it.
    policy_hex: String,
    /// The full holder list, built once when the scan completed.
    holders: Vec<HolderEntry>,
    /// Index of the next holder to emit.
    offset: usize,
}

/// In-memory ledger key — and the wire identity, in a leaner
/// internal form. Stake-credential holders are keyed by the
/// 28-byte cred hash (avoids a per-holder hex roundtrip);
/// enterprise holders by their full bech32 address (each
/// enterprise address is its own holder, not collapsed). Pointer
/// and Byron addresses fall outside this and are dropped at
/// extraction.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
enum LedgerKey {
    Stake([u8; HASH_BYTES]),
    Enterprise(String),
}

/// `(asset_name_hex_bytes -> quantity)` for one (policy, holder).
type AssetMap = BTreeMap<Vec<u8>, u64>;

/// Per-policy ledger as held in memory + persisted to state-kv.
/// Keyed by `LedgerKey` so enterprise holders are surfaced
/// distinctly (the old `None`-bucket collapse is retired). The
/// persistence format mirrors this 1-1; a deploy after a
/// `LedgerKey` shape change leaves old on-disk ledgers
/// undeserialisable → `load_ledger` returns default → recapture
/// rebuilds.
#[derive(Default, Serialize, Deserialize)]
struct PolicyLedger {
    holders: BTreeMap<LedgerKey, AssetMap>,
}

// ============================================================
// Wire-side interest predicate mirror
// ============================================================

/// Minimal mirror of the on-wire `InterestPredicate` enum —
/// same pattern as burn-address. We only care about
/// `HoldsPolicy` but capture the other variants so the
/// deserialiser can step over them without erroring.
///
/// `PolicyId` serializes as a 56-char hex string (not raw
/// bytes) — that's the canonical shape from `cardano-assets`.
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
// Holder identity + role
// ============================================================

/// Parse a bech32 address into a `LedgerKey`. Shelley addresses
/// with a Key or Script delegation part become
/// `LedgerKey::Stake(hash)`; Shelley addresses with no delegation
/// (enterprise) become `LedgerKey::Enterprise(addr)` — each
/// enterprise address is its own holder. Pointer-stake addresses
/// (~1% of addresses) and Byron addresses are dropped (`None`).
fn extract_holder_key(address: &str) -> Option<LedgerKey> {
    let addr = Address::from_bech32(address).ok()?;
    let shelley = match addr {
        Address::Shelley(s) => s,
        _ => return None, // Byron — no native asset support
    };
    match shelley.delegation() {
        ShelleyDelegationPart::Key(h) => {
            let bytes: [u8; HASH_BYTES] = (**h).into();
            Some(LedgerKey::Stake(bytes))
        }
        ShelleyDelegationPart::Script(h) => {
            let bytes: [u8; HASH_BYTES] = (**h).into();
            Some(LedgerKey::Stake(bytes))
        }
        ShelleyDelegationPart::Null => {
            // Enterprise — no stake credential. Surface the
            // address itself as the holder key so a `/dev/null`
            // burn sink (or any enterprise wallet) reaches the
            // worker for config-side classification.
            Some(LedgerKey::Enterprise(address.to_string()))
        }
        // Pointer delegation: rare, dropped (matches the
        // pre-generalised collapse behaviour for non-Null,
        // non-Key/Script shapes).
        _ => None,
    }
}

/// Convert an in-memory `LedgerKey` to its wire `HolderId`.
fn key_to_id(key: &LedgerKey) -> HolderId {
    match key {
        LedgerKey::Stake(bytes) => HolderId::Stake(hex::encode(bytes)),
        LedgerKey::Enterprise(addr) => HolderId::Enterprise(addr.clone()),
    }
}

/// The module's role tag for a holder, from what it recognises
/// while scanning. Pool addresses come from the shared
/// `mitos-dex-decode` constants; the worker layers
/// project-config classification (burns, treasury, …) on top.
/// Cached on first call — bech32 → cred parse, once.
fn holder_role_for(key: &LedgerKey) -> HolderRole {
    use std::sync::LazyLock;
    static CSWAP_POOL_KEY: LazyLock<Option<LedgerKey>> =
        LazyLock::new(|| extract_holder_key(cswap::POOL_SCRIPT_ADDR));
    static SPLASH_POOL_KEY: LazyLock<Option<LedgerKey>> =
        LazyLock::new(|| extract_holder_key(splash::POOL_SCRIPT_ADDR));
    if CSWAP_POOL_KEY.as_ref() == Some(key) || SPLASH_POOL_KEY.as_ref() == Some(key) {
        HolderRole::DexPool
    } else {
        HolderRole::Wallet
    }
}

/// 28-byte payment credential (key or script) of a bech32
/// address. `None` for Byron / unparseable.
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

/// True when `address` is a CrowdLock vesting lock — payment
/// credential matches the platform's shared script hash. Such
/// outputs bypass the holder ledger; the vesting-decomposition
/// step attributes their locked tokens to owners' `vests`.
fn is_crowdlock_lock(address: &str) -> bool {
    payment_cred_bytes(address)
        .map(|c| crowd_lock::is_crowd_lock(&c))
        .unwrap_or(false)
}

// ============================================================
// Ledger mutation
// ============================================================

/// Add `(asset_name, qty)` under `(policy, key)` in the ledger.
fn ledger_add(ledger: &mut PolicyLedger, key: LedgerKey, asset_name: &[u8], qty: u64) {
    let entry = ledger.holders.entry(key).or_default();
    let total = entry.entry(asset_name.to_vec()).or_insert(0);
    *total = total.saturating_add(qty);
}

/// Subtract `(asset_name, qty)` under `(policy, key)` in the
/// ledger. Removes the asset entry when it hits zero; removes
/// the holder entry when their last asset hits zero. Saturating
/// subtract — if a UTxO accounting bug ever drives a balance
/// negative we'd rather report 0 than crash.
fn ledger_sub(ledger: &mut PolicyLedger, key: &LedgerKey, asset_name: &[u8], qty: u64) {
    if let Some(entry) = ledger.holders.get_mut(key) {
        if let Some(total) = entry.get_mut(asset_name) {
            *total = total.saturating_sub(qty);
            if *total == 0 {
                entry.remove(asset_name);
            }
        }
        if entry.is_empty() {
            ledger.holders.remove(key);
        }
    }
}

/// Apply one `(address, assets)` produced output to the
/// ledger for a single policy. Touched holders are inserted
/// into `touched` for later delta emission. Outputs whose
/// address doesn't parse to a usable holder key (pointer-stake,
/// Byron) are dropped.
fn apply_produced_to_ledger(
    ledger: &mut PolicyLedger,
    policy: &[u8],
    address: &str,
    assets: &[WitAssetEntry],
    touched: &mut HashSet<LedgerKey>,
) {
    // CrowdLock vesting locks bypass the ledger: the
    // vesting-decomposition step attributes their locked X to
    // the owner's `vests`, never to the contract's stake cred.
    if is_crowdlock_lock(address) {
        return;
    }
    let Some(key) = extract_holder_key(address) else {
        return;
    };
    let mut touched_any = false;
    for entry in assets {
        if entry.asset.policy != policy {
            continue;
        }
        ledger_add(ledger, key.clone(), &entry.asset.name, entry.quantity);
        touched_any = true;
    }
    if touched_any {
        touched.insert(key);
    }
}

/// Symmetric for consumed.
fn apply_consumed_to_ledger(
    ledger: &mut PolicyLedger,
    policy: &[u8],
    address: &str,
    assets: &[WitAssetEntry],
    touched: &mut HashSet<LedgerKey>,
) {
    // Symmetric to `apply_produced_to_ledger`: CrowdLock locks
    // bypass the ledger in both directions.
    if is_crowdlock_lock(address) {
        return;
    }
    let Some(key) = extract_holder_key(address) else {
        return;
    };
    let mut touched_any = false;
    for entry in assets {
        if entry.asset.policy != policy {
            continue;
        }
        ledger_sub(ledger, &key, &entry.asset.name, entry.quantity);
        touched_any = true;
    }
    if touched_any {
        touched.insert(key);
    }
}

// ============================================================
// Cold-start
// ============================================================

/// Resolve + fold one page of a policy scan into `ledger`. Both
/// `refs` and the resolved outputs are dropped at return, so the
/// only state that grows across pages is the holder-bounded
/// `ledger` itself.
///
/// Returns the page's `FoldHits` — the DEX pool UTxO if one is
/// spotted in this page, plus the refs of any CrowdLock vesting
/// lock UTxOs (which bypass the ledger; vesting decomposition
/// attributes their locked X to owners' `vests`).
///
/// `read_utxos` returns each output paired with its own ref —
/// the result is not in `refs` order, so the ref must come from
/// the tuple, never the input position.
fn fold_page(
    ledger: &mut PolicyLedger,
    policy: &[u8; HASH_BYTES],
    refs: &[WitOutputRef],
) -> FoldHits {
    let mut hits = FoldHits::default();
    for (r, out) in chain_data::read_utxos(refs) {
        // CrowdLock vesting lock — bypass the ledger; the
        // vesting-decomposition step attributes locked tokens
        // to owners' `vests`.
        if is_crowdlock_lock(&out.address) {
            hits.vesting_lock_refs.push(r);
            continue;
        }
        let mut _touched_dummy: HashSet<LedgerKey> = HashSet::new();
        apply_produced_to_ledger(ledger, policy, &out.address, &out.assets, &mut _touched_dummy);
        if hits.pool_ref.is_none() && out.address == cswap::POOL_SCRIPT_ADDR {
            hits.pool_ref = Some(r);
        }
    }
    hits
}

/// Emit a policy's snapshot as a chunked `SnapshotBegin` →
/// `SnapshotChunk` × N → `SnapshotEnd` sequence, **in one call**.
/// Used by the live `update_interest` add path (`cold_start`) —
/// that host call is not re-entrant, so the emit can't be
/// spread. The recapture path (`rebootstrap`) instead drains the
/// emit one chunk per call via `open_chunked_emit` +
/// `REBOOTSTRAP_EMIT`, which keeps a large policy's snapshot
/// inside the per-call fuel budget.
///
/// `anchor_slot` is the frozen-scan tip the materialised UTxO
/// set was consistent as-of. Consumers treat the sequence as an
/// authoritative replacement and resume delta replay from the
/// first event after this slot.
fn emit_full_snapshot(policy_hex: &str, holders: Vec<HolderEntry>, anchor_slot: u64) {
    emit_event(&HolderEvent::SnapshotBegin(SnapshotBegin {
        policy: policy_hex.to_string(),
        cursor_slot: anchor_slot,
        cursor_hash_hex: String::new(),
    }));
    for chunk in holders.chunks(SNAPSHOT_CHUNK_HOLDERS) {
        emit_event(&HolderEvent::SnapshotChunk(SnapshotChunk {
            policy: policy_hex.to_string(),
            holders: chunk.to_vec(),
        }));
    }
    emit_event(&HolderEvent::SnapshotEnd(SnapshotEnd {
        policy: policy_hex.to_string(),
        holder_count: holders.len() as u64,
    }));
}

/// Emit `SnapshotBegin` and stage the holder list in
/// `REBOOTSTRAP_EMIT` so the `rebootstrap` state machine can
/// drain one `SnapshotChunk` per call — a large holder set's
/// snapshot is spread across many fuel-budgeted calls, not
/// serialised in one.
fn open_chunked_emit(policy_hex: String, holders: Vec<HolderEntry>, anchor_slot: u64) {
    emit_event(&HolderEvent::SnapshotBegin(SnapshotBegin {
        policy: policy_hex.clone(),
        cursor_slot: anchor_slot,
        cursor_hash_hex: String::new(),
    }));
    REBOOTSTRAP_EMIT.with(|cell| {
        *cell.borrow_mut() = Some(EmitState {
            policy_hex,
            holders,
            offset: 0,
        });
    });
}

/// Persist a predicate's raw ledger and open its chunked emit —
/// the no-LP-pool `rebootstrap` path. `persist_ledger` +
/// `ledger_to_holders` are one pass here, but both are sort-free
/// and far lighter than the chunk emits they used to share a
/// budget with.
fn begin_emit(
    policy: &[u8; HASH_BYTES],
    ledger: &PolicyLedger,
    vesting_lock_refs: &[WitOutputRef],
    anchor_slot: u64,
    total_utxos: usize,
) {
    let policy_hex = hex::encode(policy);
    persist_ledger(&policy_hex, ledger);
    let plain = ledger_to_holders(ledger);
    let vests = decompose_vesting(policy, vesting_lock_refs);
    if !vests.is_empty() {
        logging::log(
            LogLevel::Info,
            LOG_TARGET,
            &format!(
                "rebootstrap policy={policy_hex}: vesting decomposed across {} owner(s)",
                vests.len()
            ),
        );
    }
    let holders = attach_vests(plain, vests);
    logging::log(
        LogLevel::Info,
        LOG_TARGET,
        &format!(
            "rebootstrap policy={policy_hex}: {total_utxos} UTxO(s) → {} holder(s) @ slot {anchor_slot}; emitting chunked snapshot",
            holders.len()
        ),
    );
    open_chunked_emit(policy_hex, holders, anchor_slot);
}

/// Run the bootstrap scan for a newly-registered policy in one
/// call: page through `utxos_by_policy` → fold each page →
/// (LP-decompose if a DEX pool was found) → emit the snapshot.
/// Used by the live `update_interest` add path.
///
/// The scan is **paged** (`WASM_BUDGET_CHUNKING.md`): each call
/// to `utxos_by_policy` returns one host-clamped page, so the
/// only resident state is the holder-bounded ledger. The
/// re-entrant `rebootstrap` path (recapture) spreads the same
/// scan — and the LP-token scan — across many fuel-budgeted
/// calls; see `rebootstrap`.
fn cold_start(policy: &[u8; HASH_BYTES]) {
    let mut ledger = PolicyLedger::default();
    let mut after: Option<Vec<u8>> = None;
    // Assigned on every loop iteration before it is read after
    // the loop — the `loop` body always runs at least once.
    let mut anchor_slot: u64;
    let mut total_utxos: usize = 0;
    let mut pool_ref: Option<WitOutputRef> = None;
    let mut vesting_lock_refs: Vec<WitOutputRef> = Vec::new();

    loop {
        let page = chain_data::utxos_by_policy(policy, after.as_deref(), COLD_START_PAGE_HINT);
        anchor_slot = page.anchor_slot;
        total_utxos += page.refs.len();
        let hits = fold_page(&mut ledger, policy, &page.refs);
        if pool_ref.is_none() {
            pool_ref = hits.pool_ref;
        }
        vesting_lock_refs.extend(hits.vesting_lock_refs);
        match page.next {
            Some(token) => after = Some(token),
            None => break,
        }
    }

    let policy_hex = hex::encode(policy);
    // Persist the *raw* ledger (pool aggregate intact) — deltas
    // keep accounting against it. Decomposition transforms only
    // the emitted snapshot.
    persist_ledger(&policy_hex, &ledger);
    let holders = build_decomposed_holders(
        &policy_hex,
        policy,
        &ledger,
        pool_ref.as_ref(),
        &vesting_lock_refs,
    );
    let holder_count = holders.len();
    emit_full_snapshot(&policy_hex, holders, anchor_slot);

    logging::log(
        LogLevel::Info,
        LOG_TARGET,
        &format!(
            "cold-start policy={policy_hex}: {total_utxos} UTxO(s) → {holder_count} holder(s) @ slot {anchor_slot}"
        ),
    );
}

/// Render the in-memory `PolicyLedger` as the wire-shape
/// `Vec<HolderEntry>`, in ledger order, with no LP attribution
/// (`lp_amount: 0`).
///
/// No by-quantity sort: that is an `O(n log n)` pass in a single
/// fuel budget and a large holder set traps on it (prod
/// recapture, 2026-05-19 — see `WASM_BUDGET_CHUNKING.md`). The
/// consumer DB-inserts each holder, so wire order is immaterial;
/// "top N holders" is a consumer-side query, not a wire
/// guarantee. Order is still deterministic — `holders` is a
/// `BTreeMap` keyed by stake credential.
fn ledger_to_holders(ledger: &PolicyLedger) -> Vec<HolderEntry> {
    ledger
        .holders
        .iter()
        .map(|(key, assets)| HolderEntry {
            id: key_to_id(key),
            assets: assets_map_to_vec(assets),
            lp_amount: 0,
            role: holder_role_for(key),
            vests: Vec::new(),
        })
        .collect()
}

fn assets_map_to_vec(map: &AssetMap) -> Vec<AssetBalance> {
    let mut out: Vec<AssetBalance> = map
        .iter()
        .map(|(name, qty)| AssetBalance {
            asset_name_hex: hex::encode(name),
            quantity: *qty,
        })
        .collect();
    // BTreeMap iter is already in asset-name order — keep that
    // ordering on the wire.
    out.sort_by(|a, b| a.asset_name_hex.cmp(&b.asset_name_hex));
    out
}

// ============================================================
// LP-pool decomposition
// ============================================================
//
// A DEX liquidity pool holds an aggregate of the tracked policy
// on behalf of every wallet that provided liquidity. Left alone
// it shows as one giant "holder". Decomposition redistributes
// that aggregate to the LP providers, proportional to their
// LP-token share, and records the redistributed quantity as each
// holder's `lp_amount`. The pool is auto-discovered (it is a
// holder of the policy at a known DEX pool script); the LP-token
// holder set is a second `utxos_by_policy` scan. See
// `docs/design/HOLDER_DISTRIBUTION_LP_DECOMPOSITION.md`.

/// Resolve a `TypedDatum` to its CBOR bytes — inline payload if
/// present, else a hash lookup.
fn resolve_datum_bytes(d: &TypedDatum) -> Option<Vec<u8>> {
    if !d.payload.is_empty() {
        return Some(d.payload.clone());
    }
    chain_data::datum_by_hash(&d.hash)
}

/// Read + decode the CSwap pool datum at a detected pool UTxO.
fn read_pool_datum(pool_ref: &WitOutputRef) -> Option<cswap::CswapPoolDatum> {
    let datums = chain_data::read_output_datums(std::slice::from_ref(pool_ref));
    let cbor = datums
        .into_iter()
        .next()
        .flatten()
        .and_then(|d| resolve_datum_bytes(&d))?;
    cswap::decode_pool_datum(&cbor)
}

/// Resolve one page of an LP-token policy scan and accumulate
/// per-stake-credential LP-token holdings into `lp_holders`.
///
/// Unstaked LP sits at user wallets — stake credential straight
/// off the address. Staked LP sits at the CSwap farm contract,
/// one shared address that carries no staker identity, so the
/// staker is recovered from the farm UTxO's staking datum. A
/// farm UTxO whose datum doesn't decode is skipped — its share
/// stays with the residual pool entry rather than misattributed.
fn fold_lp_page(
    lp_holders: &mut BTreeMap<LedgerKey, u64>,
    lp_policy: &[u8; HASH_BYTES],
    refs: &[WitOutputRef],
) {
    // `read_utxos` returns each output paired with its own ref
    // (not in input order). Farm UTxOs carry the staker only in
    // their datum — collect their refs and bulk-resolve staking
    // datums. `read_output_datums` *is* positionally parallel to
    // its input, so the `farm_refs`/`farm_datums` zip is sound.
    let utxos = chain_data::read_utxos(refs);
    let farm_refs: Vec<WitOutputRef> = utxos
        .iter()
        .filter(|(_, out)| out.address == cswap::FARM_SCRIPT_ADDR)
        .map(|(r, _)| r.clone())
        .collect();
    let farm_datums = chain_data::read_output_datums(&farm_refs);
    let staker_by_ref: HashMap<(Vec<u8>, u32), [u8; HASH_BYTES]> = farm_refs
        .iter()
        .zip(farm_datums)
        .filter_map(|(r, datum)| {
            let cbor = datum.as_ref().and_then(resolve_datum_bytes)?;
            let staker = cswap::decode_staking_datum(&cbor)?;
            Some(((r.tx_hash.clone(), r.index), staker))
        })
        .collect();

    for (r, out) in utxos {
        let lp_qty: u64 = out
            .assets
            .iter()
            .filter(|a| a.asset.policy.as_slice() == lp_policy.as_slice())
            .map(|a| a.quantity)
            .sum();
        if lp_qty == 0 {
            continue;
        }
        let key: LedgerKey = if out.address == cswap::FARM_SCRIPT_ADDR {
            match staker_by_ref.get(&(r.tx_hash.clone(), r.index)) {
                Some(hash) => LedgerKey::Stake(*hash),
                None => {
                    logging::log(
                        LogLevel::Warn,
                        LOG_TARGET,
                        "LP decomposition: a farm UTxO's staking datum did not decode — its share stays with the pool residual",
                    );
                    continue;
                }
            }
        } else {
            match extract_holder_key(&out.address) {
                Some(k) => k,
                None => continue,
            }
        };
        *lp_holders.entry(key).or_insert(0) += lp_qty;
    }
}

/// Transform the raw holder ledger into the LP-decomposed holder
/// list: drop the pool's aggregate holding and redistribute it
/// to the wallets that provided liquidity, proportional to their
/// LP-token share. The rounding remainder (and any LP not
/// enumerated) stays as a residual pool entry. The raw ledger is
/// untouched — deltas keep accounting against it; only the
/// emitted list is decomposed.
fn decompose_holders(
    ledger: &PolicyLedger,
    total_lp_tokens: u64,
    lp_holders: &BTreeMap<LedgerKey, u64>,
) -> Vec<HolderEntry> {
    let pool_key = extract_holder_key(cswap::POOL_SCRIPT_ADDR)
        .expect("cswap pool address parses to a stake key");
    let pool_reserve: AssetMap = ledger.holders.get(&pool_key).cloned().unwrap_or_default();

    // Working set: every holder except the pool, each carrying a
    // running `lp_amount` of how much of the balance is
    // LP-derived (0 for plain holders).
    let mut out: BTreeMap<LedgerKey, (AssetMap, u64)> = ledger
        .holders
        .iter()
        .filter(|(k, _)| *k != &pool_key)
        .map(|(k, assets)| (k.clone(), (assets.clone(), 0u64)))
        .collect();

    let mut residual = pool_reserve.clone();
    for (lp_key, lp_qty) in lp_holders {
        let entry = out.entry(lp_key.clone()).or_default();
        for (name, reserve) in &pool_reserve {
            let share = lp_share(*lp_qty, *reserve, total_lp_tokens);
            if share == 0 {
                continue;
            }
            *entry.0.entry(name.clone()).or_insert(0) += share;
            entry.1 += share;
            if let Some(rem) = residual.get_mut(name) {
                *rem = rem.saturating_sub(share);
            }
        }
    }

    // Rounding remainder (and any LP not enumerated) stays a
    // residual pool entry — the Mothership band shrinks to it.
    let residual: AssetMap = residual.into_iter().filter(|(_, q)| *q > 0).collect();
    if !residual.is_empty() {
        out.insert(pool_key, (residual, 0));
    }

    out.into_iter()
        .map(|(key, (assets, lp_amount))| HolderEntry {
            id: key_to_id(&key),
            assets: assets_map_to_vec(&assets),
            lp_amount,
            role: holder_role_for(&key),
            vests: Vec::new(),
        })
        .collect()
}

/// `cold_start`'s decomposition step: if a DEX pool was found,
/// read its datum, scan the LP-token holder set (paged loop —
/// `cold_start` is one host call), and decompose. Otherwise emit
/// the ledger plainly. A pool whose datum doesn't decode also
/// falls back to a plain emit.
fn decompose_or_plain(
    policy_hex: &str,
    ledger: &PolicyLedger,
    pool_ref: Option<&WitOutputRef>,
) -> Vec<HolderEntry> {
    let Some(pool_ref) = pool_ref else {
        return ledger_to_holders(ledger);
    };
    let Some(datum) = read_pool_datum(pool_ref) else {
        logging::log(
            LogLevel::Warn,
            LOG_TARGET,
            &format!(
                "cold-start policy={policy_hex}: pool UTxO datum did not decode — emitting without LP decomposition"
            ),
        );
        return ledger_to_holders(ledger);
    };
    let lp_policy: [u8; HASH_BYTES] = match datum.lp_policy.as_slice().try_into() {
        Ok(arr) => arr,
        Err(_) => {
            logging::log(
                LogLevel::Warn,
                LOG_TARGET,
                &format!(
                    "cold-start policy={policy_hex}: pool datum lp_policy is not 28 bytes — skipping LP decomposition"
                ),
            );
            return ledger_to_holders(ledger);
        }
    };

    let mut lp_holders: BTreeMap<LedgerKey, u64> = BTreeMap::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let page = chain_data::utxos_by_policy(&lp_policy, after.as_deref(), COLD_START_PAGE_HINT);
        fold_lp_page(&mut lp_holders, &lp_policy, &page.refs);
        match page.next {
            Some(token) => after = Some(token),
            None => break,
        }
    }
    logging::log(
        LogLevel::Info,
        LOG_TARGET,
        &format!(
            "cold-start policy={policy_hex}: DEX pool decomposed across {} LP provider(s)",
            lp_holders.len()
        ),
    );
    decompose_holders(ledger, datum.total_lp_tokens, &lp_holders)
}

/// Read each CrowdLock lock UTxO, decode its datum, resolve the
/// owner's stake credential, and group resulting `LockEntry`s
/// by owner. The locked tokens are deliberately NOT in the
/// holder ledger (they're skipped at `apply_produced_to_ledger`
/// / `fold_page` time), so this step just builds the per-owner
/// vests map — the snapshot construction then attaches each
/// owner's vests to their `HolderEntry`.
///
/// One-shot for v1: large vesting sets may hit the per-call
/// fuel budget; re-instantiate-on-trap retries with a smaller
/// adaptive page. Re-entrant chunking can land later if needed.
fn decompose_vesting(
    policy: &[u8; HASH_BYTES],
    lock_refs: &[WitOutputRef],
) -> BTreeMap<LedgerKey, Vec<LockEntry>> {
    if lock_refs.is_empty() {
        return BTreeMap::new();
    }
    let utxos = chain_data::read_utxos(lock_refs);
    let datums = chain_data::read_output_datums(lock_refs);
    // `read_output_datums` is positionally parallel to its
    // input; key the datums by ref so we can correlate with the
    // unordered `read_utxos` outputs.
    let datum_by_ref: HashMap<(Vec<u8>, u32), TypedDatum> = lock_refs
        .iter()
        .zip(datums)
        .filter_map(|(r, d)| d.map(|d| ((r.tx_hash.clone(), r.index), d)))
        .collect();

    let policy_hex = hex::encode(policy);
    let mut out: BTreeMap<LedgerKey, Vec<LockEntry>> = BTreeMap::new();
    for (lock_ref, out_) in utxos {
        let Some(datum) = datum_by_ref.get(&(lock_ref.tx_hash.clone(), lock_ref.index)) else {
            continue;
        };
        let Some(cbor) = resolve_datum_bytes(datum) else {
            continue;
        };
        let Some(vd) = decode_vesting_datum(&cbor) else {
            continue;
        };
        let Ok(pkh) = hex::decode(&vd.owner_pkh_hex) else {
            continue;
        };
        let Some(stake_cred) = chain_data::resolve_stake_for_payment_pkh(&pkh) else {
            logging::log(
                LogLevel::Warn,
                LOG_TARGET,
                &format!(
                    "vesting decomposition: owner pkh {} did not resolve to a stake cred — lock skipped",
                    vd.owner_pkh_hex
                ),
            );
            continue;
        };
        let owner_bytes: Vec<u8> = match &stake_cred {
            WitStakeCred::KeyHash(b) | WitStakeCred::ScriptHash(b) => b.clone(),
        };
        let owner_hash: [u8; HASH_BYTES] = match owner_bytes.as_slice().try_into() {
            Ok(arr) => arr,
            Err(_) => continue,
        };
        let owner_key = LedgerKey::Stake(owner_hash);
        let owner_stake_cred_hex = Some(hex::encode(&owner_bytes));

        for asset in &out_.assets {
            if asset.asset.policy != policy {
                continue;
            }
            let lock_entry = LockEntry {
                utxo_ref: LockRef {
                    tx_hash: hex::encode(&lock_ref.tx_hash),
                    index: lock_ref.index,
                },
                lock_address: out_.address.clone(),
                policy: policy_hex.clone(),
                asset_name_hex: hex::encode(&asset.asset.name),
                amount: asset.quantity,
                owner_pkh: vd.owner_pkh_hex.clone(),
                owner_stake_cred_hex: owner_stake_cred_hex.clone(),
                unlock_ts_ms: vd.unlock_ts_ms,
                // VestStyle.CrowdLock — `holder-distribution`
                // only recognises CrowdLock contracts (shared
                // payment-cred). Shield's per-project addresses
                // remain a future extension; until then any
                // Shield-style lock won't be picked up here and
                // will surface via `vesting-tracker` events on
                // the consumer side as today.
                vest_style: VestStyle::CrowdLock,
                locked_at_tx: hex::encode(&lock_ref.tx_hash),
            };
            out.entry(owner_key.clone()).or_default().push(lock_entry);
        }
    }
    out
}

/// Attach per-owner vest lists onto an already-built holder
/// list, creating new `HolderEntry`s for vest-only owners (no
/// liquid X holdings). Preserves existing `assets` / `lp_amount`
/// / `role` on holders that already exist.
fn attach_vests(
    mut holders: Vec<HolderEntry>,
    vests: BTreeMap<LedgerKey, Vec<LockEntry>>,
) -> Vec<HolderEntry> {
    if vests.is_empty() {
        return holders;
    }
    // Build an index over the existing holders by `HolderId` —
    // O(n) once, then O(1) lookup per vest owner.
    let mut idx: HashMap<HolderId, usize> = HashMap::with_capacity(holders.len());
    for (i, h) in holders.iter().enumerate() {
        idx.insert(h.id.clone(), i);
    }
    for (owner_key, owner_vests) in vests {
        let owner_id = key_to_id(&owner_key);
        if let Some(&i) = idx.get(&owner_id) {
            holders[i].vests = owner_vests;
        } else {
            idx.insert(owner_id.clone(), holders.len());
            holders.push(HolderEntry {
                id: owner_id,
                assets: Vec::new(),
                lp_amount: 0,
                role: HolderRole::Wallet,
                vests: owner_vests,
            });
        }
    }
    holders
}

/// Unified snapshot construction: LP decomposition + vesting
/// decomposition + attach. The single entry point used by
/// `cold_start`; `rebootstrap` uses the same logic but spread
/// across its re-entrant phases.
fn build_decomposed_holders(
    policy_hex: &str,
    policy: &[u8; HASH_BYTES],
    ledger: &PolicyLedger,
    pool_ref: Option<&WitOutputRef>,
    vesting_lock_refs: &[WitOutputRef],
) -> Vec<HolderEntry> {
    let lp_decomposed = decompose_or_plain(policy_hex, ledger, pool_ref);
    let vests = decompose_vesting(policy, vesting_lock_refs);
    if !vests.is_empty() {
        logging::log(
            LogLevel::Info,
            LOG_TARGET,
            &format!(
                "policy={policy_hex}: vesting decomposed across {} owner(s)",
                vests.len()
            ),
        );
    }
    attach_vests(lp_decomposed, vests)
}

/// Open the LP-pool decomposition for a `rebootstrap` predicate
/// whose holder scan just finished and which has a DEX pool.
/// Persists the raw ledger, reads + decodes the pool datum, and
/// stages a `DecompState` for the re-entrant LP-token scan.
/// Falls back to opening the plain chunked emit when the pool
/// datum doesn't decode.
fn begin_decomp(
    policy: &[u8; HASH_BYTES],
    ledger: PolicyLedger,
    pool_ref: &WitOutputRef,
    vesting_lock_refs: Vec<WitOutputRef>,
    anchor_slot: u64,
    total_utxos: usize,
) {
    let policy_hex = hex::encode(policy);
    // Persist the *raw* ledger — decomposition transforms only
    // the emitted snapshot, not the persisted delta-accounting
    // state.
    persist_ledger(&policy_hex, &ledger);

    let datum = read_pool_datum(pool_ref);
    let lp_policy: Option<[u8; HASH_BYTES]> = datum
        .as_ref()
        .and_then(|d| d.lp_policy.as_slice().try_into().ok());

    match (datum, lp_policy) {
        (Some(datum), Some(lp_policy)) => {
            logging::log(
                LogLevel::Info,
                LOG_TARGET,
                &format!(
                    "rebootstrap policy={policy_hex}: DEX pool detected — decomposing LP across holders"
                ),
            );
            REBOOTSTRAP_DECOMP.with(|cell| {
                *cell.borrow_mut() = Some(DecompState {
                    policy_hex,
                    anchor_slot,
                    total_utxos,
                    ledger,
                    total_lp_tokens: datum.total_lp_tokens,
                    lp_policy,
                    lp_after: None,
                    lp_holders: BTreeMap::new(),
                    vesting_lock_refs,
                    policy: *policy,
                });
            });
        }
        _ => {
            logging::log(
                LogLevel::Warn,
                LOG_TARGET,
                &format!(
                    "rebootstrap policy={policy_hex}: pool UTxO datum did not decode — emitting without LP decomposition"
                ),
            );
            // Vesting decomposition still runs even when LP
            // doesn't — the two decompositions are independent.
            let plain = ledger_to_holders(&ledger);
            let vests = decompose_vesting(policy, &vesting_lock_refs);
            if !vests.is_empty() {
                logging::log(
                    LogLevel::Info,
                    LOG_TARGET,
                    &format!(
                        "rebootstrap policy={policy_hex}: vesting decomposed across {} owner(s)",
                        vests.len()
                    ),
                );
            }
            let holders = attach_vests(plain, vests);
            logging::log(
                LogLevel::Info,
                LOG_TARGET,
                &format!(
                    "rebootstrap policy={policy_hex}: {total_utxos} UTxO(s) → {} holder(s) @ slot {anchor_slot}; emitting chunked snapshot",
                    holders.len()
                ),
            );
            open_chunked_emit(policy_hex, holders, anchor_slot);
        }
    }
}

// ============================================================
// Per-TX deltas
// ============================================================

#[derive(Default)]
struct TxBuffer {
    tx_hash: Option<Vec<u8>>,
    slot: u64,
    /// Per-policy: produced events affecting that policy.
    produced: HashMap<[u8; HASH_BYTES], Vec<ProducedEvent>>,
    /// Per-policy: consumed events affecting that policy.
    consumed: HashMap<[u8; HASH_BYTES], Vec<ConsumedEvent>>,
}

fn touches_policy(assets: &[WitAssetEntry], policy: &[u8; HASH_BYTES]) -> bool {
    assets.iter().any(|e| e.asset.policy == policy)
}

fn handle_produced(p: &ProducedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(p.tx_hash.clone());
    }
    if buf.slot == 0 {
        if let crate::mitos::platform_v2::types::ChainPoint::Specific(sp) = &p.cursor {
            buf.slot = sp.slot;
        } else if let crate::mitos::platform_v2::types::ChainPoint::SlotOnly(s) = &p.cursor {
            buf.slot = *s;
        }
    }
    TRACKED_POLICIES.with(|set| {
        let set = set.borrow();
        for policy in set.iter() {
            if touches_policy(&p.output.assets, policy) {
                buf.produced.entry(*policy).or_default().push(p.clone());
            }
        }
    });
}

fn handle_consumed(c: &ConsumedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(c.consuming_tx_hash.clone());
    }
    if buf.slot == 0 {
        if let crate::mitos::platform_v2::types::ChainPoint::Specific(sp) = &c.cursor {
            buf.slot = sp.slot;
        } else if let crate::mitos::platform_v2::types::ChainPoint::SlotOnly(s) = &c.cursor {
            buf.slot = *s;
        }
    }
    TRACKED_POLICIES.with(|set| {
        let set = set.borrow();
        for policy in set.iter() {
            if touches_policy(&c.prior_output.assets, policy) {
                buf.consumed.entry(*policy).or_default().push(c.clone());
            }
        }
    });
}

/// For each touched policy in the buffer, apply produced +
/// consumed events to its ledger, persist, and emit a Delta.
fn flush_buffer(buf: TxBuffer) {
    let Some(tx_hash) = buf.tx_hash else {
        return;
    };
    let tx_hash_hex = hex::encode(&tx_hash);
    let slot = buf.slot;

    // Union of policies seen in either produced or consumed
    // sides of this TX.
    let mut policies: HashSet<[u8; HASH_BYTES]> = HashSet::new();
    policies.extend(buf.produced.keys().copied());
    policies.extend(buf.consumed.keys().copied());

    for policy in policies {
        let policy_hex = hex::encode(policy);
        let mut ledger = load_ledger(&policy_hex);
        let mut touched: HashSet<LedgerKey> = HashSet::new();

        if let Some(events) = buf.consumed.get(&policy) {
            for c in events {
                apply_consumed_to_ledger(
                    &mut ledger,
                    &policy,
                    &c.prior_output.address,
                    &c.prior_output.assets,
                    &mut touched,
                );
            }
        }
        if let Some(events) = buf.produced.get(&policy) {
            for p in events {
                apply_produced_to_ledger(
                    &mut ledger,
                    &policy,
                    &p.output.address,
                    &p.output.assets,
                    &mut touched,
                );
            }
        }

        persist_ledger(&policy_hex, &ledger);

        // Build the changed-holders list — for each touched
        // ledger key, report its post-TX balance (empty assets
        // if the holder dropped to zero and was removed).
        let mut changed: Vec<HolderEntry> = touched
            .into_iter()
            .map(|key| {
                let assets = ledger
                    .holders
                    .get(&key)
                    .map(assets_map_to_vec)
                    .unwrap_or_default();
                HolderEntry {
                    id: key_to_id(&key),
                    assets,
                    // Deltas carry raw post-TX balances; LP +
                    // vesting attribution are snapshot-time
                    // transforms, recomputed on the next
                    // cold-start / recapture.
                    lp_amount: 0,
                    role: holder_role_for(&key),
                    vests: Vec::new(),
                }
            })
            .collect();
        changed.sort_by(|a, b| a.id.cmp(&b.id));

        emit_event(&HolderEvent::Delta(HolderDelta {
            policy: policy_hex,
            tx_hash: tx_hash_hex.clone(),
            slot,
            changed,
        }));
    }
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
                    set.remove(p);
                }
            }
        }
    });

    persist_tracked_policies();

    for policy in added {
        cold_start(&policy);
    }
}

fn persist_tracked_policies() {
    let policies: Vec<String> = TRACKED_POLICIES.with(|set| {
        set.borrow().iter().map(|p| hex::encode(p)).collect()
    });
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
        Err(_) => return,
    };
    let mut count = 0usize;
    TRACKED_POLICIES.with(|set| {
        let mut set = set.borrow_mut();
        set.clear();
        for hex_str in policies {
            if let Ok(bytes) = hex::decode(&hex_str) {
                if bytes.len() == HASH_BYTES {
                    let mut arr = [0u8; HASH_BYTES];
                    arr.copy_from_slice(&bytes);
                    set.insert(arr);
                    count += 1;
                }
            }
        }
    });
    logging::log(
        LogLevel::Info,
        LOG_TARGET,
        &format!("restored {count} tracked policy(ies) from state-kv"),
    );
}

// ============================================================
// Ledger persistence
// ============================================================

fn ledger_key(policy_hex: &str) -> String {
    format!("ledger:{policy_hex}")
}

fn persist_ledger(policy_hex: &str, ledger: &PolicyLedger) {
    let mut buf = Vec::with_capacity(2048);
    if let Err(e) = ciborium::ser::into_writer(ledger, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode ledger for {policy_hex}: {e}"),
        );
        return;
    }
    state_kv::set_value(&ledger_key(policy_hex), &buf);
}

fn load_ledger(policy_hex: &str) -> PolicyLedger {
    let Some(bytes) = state_kv::get_value(&ledger_key(policy_hex)) else {
        return PolicyLedger::default();
    };
    ciborium::de::from_reader(bytes.as_slice()).unwrap_or_default()
}

// ============================================================
// Rebootstrap continuation cursor (durable predicate index)
// ============================================================

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
// Emit
// ============================================================

fn emit_event(event: &HolderEvent) {
    let mut buf = Vec::with_capacity(2048);
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode HolderEvent: {e}"),
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
        restore_tracked_policies();
    }

    fn handle_events(events: Vec<DispatchEvent>) {
        // Skip work when no policies are tracked — host should
        // not dispatch in this case anyway, but defensive.
        let any_tracked = TRACKED_POLICIES.with(|s| !s.borrow().is_empty());
        if !any_tracked {
            return;
        }
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

    fn update_interest(op: InterestOp, items_cbor: Vec<u8>) -> Result<(), String> {
        apply_interest_update(op, &items_cbor);
        Ok(())
    }

    /// Re-emit the holder ledger for tracked policies — **one
    /// bounded unit of work per call** (`WASM_BUDGET_CHUNKING.md`).
    /// The host loops, refuelling each call, until a step comes
    /// back `done`. Each call does exactly one of:
    ///
    /// - **emit phase** — if a chunked snapshot is mid-emit
    ///   (`REBOOTSTRAP_EMIT`), emit one `SnapshotChunk`, or the
    ///   closing `SnapshotEnd`;
    /// - **decomposition phase** — if an LP-pool decomposition is
    ///   mid-scan (`REBOOTSTRAP_DECOMP`), scan one page of the
    ///   LP-token holder set; when the last page lands, decompose
    ///   the ledger and open the chunked emit;
    /// - **scan phase** — otherwise, scan one page of the current
    ///   predicate's UTxO set, folding it into the ledger; when
    ///   the last page lands, open the LP decomposition
    ///   (`begin_decomp`) if a DEX pool was found, else the
    ///   chunked emit (`begin_emit`).
    ///
    /// A page of UTxOs fits one fuel budget and a chunk of
    /// holders fits one fuel budget; a whole large policy's scan
    /// *or* LP-token scan *or* snapshot does not — hence each is
    /// spread across calls.
    ///
    /// Round state (predicate list + page cursor + accumulating
    /// ledger) and the in-flight emit are thread-local — resident
    /// across the host's loop. The durable cursor in `state-kv`
    /// (`KV_REBOOTSTRAP_CURSOR`) is only the `predicate_idx`, and
    /// it is **not advanced until the predicate's emit closes** —
    /// so a trap or host restart anywhere in a predicate (scan or
    /// emit) restarts it from page 0, re-scanning + re-emitting.
    /// That is safe: the consumer wipes its projection on
    /// `SnapshotBegin`, so a re-emit discards any partial.
    ///
    /// `init` restores `TRACKED_POLICIES` from `state-kv`, so the
    /// module knows what to re-scan; idempotent — recapture may
    /// run a round repeatedly.
    fn rebootstrap() -> Result<RebootstrapStep, String> {
        // ── Emit phase ── Drain an in-flight chunked snapshot
        // one `SnapshotChunk` per call. Emitting the whole holder
        // list in a single budget traps for a large policy.
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
            if state.offset >= state.holders.len() {
                emit_event(&HolderEvent::SnapshotEnd(SnapshotEnd {
                    policy: state.policy_hex.clone(),
                    holder_count: state.holders.len() as u64,
                }));
                *slot = None;
                EmitOutcome::Closed
            } else {
                let end = (state.offset + SNAPSHOT_CHUNK_HOLDERS).min(state.holders.len());
                emit_event(&HolderEvent::SnapshotChunk(SnapshotChunk {
                    policy: state.policy_hex.clone(),
                    holders: state.holders[state.offset..end].to_vec(),
                }));
                state.offset = end;
                EmitOutcome::Chunk
            }
        });
        match outcome {
            // More chunks (or the `SnapshotEnd`) still to come.
            EmitOutcome::Chunk => {
                return Ok(RebootstrapStep {
                    done: false,
                    ingested: 0,
                });
            }
            // The predicate's snapshot is fully emitted — advance
            // the durable cursor past it now.
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

        // ── Decomposition phase ── If an LP-pool decomposition
        // is in flight (the just-scanned predicate has a DEX
        // pool), scan one page of the LP-token holder set. When
        // the last page lands, decompose the ledger and open the
        // chunked emit. Re-entrant for the same fuel-budget
        // reasons as the holder scan.
        enum DecompOutcome {
            NotDecomposing,
            More(u64),
            Done {
                policy_hex: String,
                anchor_slot: u64,
                total_utxos: usize,
                holders: Vec<HolderEntry>,
            },
        }
        let decomp = REBOOTSTRAP_DECOMP.with(|cell| {
            let mut slot = cell.borrow_mut();
            let Some(st) = slot.as_mut() else {
                return DecompOutcome::NotDecomposing;
            };
            let page = chain_data::utxos_by_policy(
                &st.lp_policy,
                st.lp_after.as_deref(),
                COLD_START_PAGE_HINT,
            );
            let ingested = page.refs.len() as u64;
            fold_lp_page(&mut st.lp_holders, &st.lp_policy, &page.refs);
            match page.next {
                Some(token) => {
                    st.lp_after = Some(token);
                    DecompOutcome::More(ingested)
                }
                None => {
                    // LP scan complete. Build the LP-decomposed
                    // holder list, then run vesting decomp +
                    // attach in one shot. Both decomps share the
                    // anchor and the raw ledger.
                    let lp_holders =
                        decompose_holders(&st.ledger, st.total_lp_tokens, &st.lp_holders);
                    let vests = decompose_vesting(&st.policy, &st.vesting_lock_refs);
                    let policy_hex = st.policy_hex.clone();
                    if !vests.is_empty() {
                        logging::log(
                            LogLevel::Info,
                            LOG_TARGET,
                            &format!(
                                "rebootstrap policy={policy_hex}: vesting decomposed across {} owner(s)",
                                vests.len()
                            ),
                        );
                    }
                    let holders = attach_vests(lp_holders, vests);
                    let done = DecompOutcome::Done {
                        policy_hex,
                        anchor_slot: st.anchor_slot,
                        total_utxos: st.total_utxos,
                        holders,
                    };
                    *slot = None;
                    done
                }
            }
        });
        match decomp {
            // More LP-token pages still to scan.
            DecompOutcome::More(ingested) => {
                return Ok(RebootstrapStep {
                    done: false,
                    ingested,
                });
            }
            // LP scan complete — the decomposed holder list is
            // ready; open the chunked emit.
            DecompOutcome::Done {
                policy_hex,
                anchor_slot,
                total_utxos,
                holders,
            } => {
                logging::log(
                    LogLevel::Info,
                    LOG_TARGET,
                    &format!(
                        "rebootstrap policy={policy_hex}: {total_utxos} UTxO(s) → {} holder(s) (LP-decomposed) @ slot {anchor_slot}; emitting chunked snapshot",
                        holders.len()
                    ),
                );
                open_chunked_emit(policy_hex, holders, anchor_slot);
                return Ok(RebootstrapStep {
                    done: false,
                    ingested: 0,
                });
            }
            DecompOutcome::NotDecomposing => {}
        }

        // ── Scan phase ──
        REBOOTSTRAP_STATE.with(|cell| {
            let mut state = cell.borrow_mut();

            // First call of a round (or the thread-local was
            // wiped by a trap/restart) — rebuild round state. The
            // policy list is sorted so the durable `predicate_idx`
            // cursor is stable across a host restart.
            if state.is_none() {
                let mut policies: Vec<[u8; HASH_BYTES]> =
                    TRACKED_POLICIES.with(|s| s.borrow().iter().copied().collect());
                policies.sort_unstable();
                *state = Some(ReentrantRound::resume(policies, load_rebootstrap_cursor()));
            }
            let round = state.as_mut().expect("round initialised above");

            // No predicates left (empty tracked set, or resumed
            // past the end) — round done.
            let Some(&policy) = round.current() else {
                clear_rebootstrap_cursor();
                *state = None;
                return Ok(RebootstrapStep {
                    done: true,
                    ingested: 0,
                });
            };

            // Process exactly one page of the current predicate.
            let page =
                chain_data::utxos_by_policy(&policy, round.after(), COLD_START_PAGE_HINT);
            let ingested = page.refs.len() as u64;
            let anchor_slot = page.anchor_slot;
            let hits = fold_page(&mut round.acc_mut().ledger, &policy, &page.refs);
            {
                let acc = round.acc_mut();
                if acc.pool_ref.is_none() {
                    acc.pool_ref = hits.pool_ref;
                }
                acc.vesting_lock_refs.extend(hits.vesting_lock_refs);
            }

            match page.next {
                Some(token) => {
                    // More pages for this predicate — keep the round.
                    round.page_more(ingested, token);
                    Ok(RebootstrapStep {
                        done: false,
                        ingested,
                    })
                }
                None => {
                    // Predicate fully scanned. If a DEX pool was
                    // found among the holders, open the LP-pool
                    // decomposition; otherwise open the chunked
                    // emit directly. Either way the durable cursor
                    // is NOT advanced until the emit closes (see
                    // the doc above).
                    round.page_last(ingested);
                    let total_utxos = round.items() as usize;
                    let pool_ref = round.acc_mut().pool_ref.take();
                    let vesting_lock_refs =
                        std::mem::take(&mut round.acc_mut().vesting_lock_refs);
                    let ledger = std::mem::take(&mut round.acc_mut().ledger);
                    match pool_ref {
                        Some(pref) => begin_decomp(
                            &policy,
                            ledger,
                            &pref,
                            vesting_lock_refs,
                            anchor_slot,
                            total_utxos,
                        ),
                        None => begin_emit(
                            &policy,
                            &ledger,
                            &vesting_lock_refs,
                            anchor_slot,
                            total_utxos,
                        ),
                    }
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
