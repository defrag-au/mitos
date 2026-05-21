//! Collection-holders community module — emits a per-policy
//! ledger of `(asset_name, holder, quantity)` holdings and
//! per-TX movement deltas for NFT- and RFT-shaped policies.
//!
//! Sibling to `holder-distribution`: covers policies where
//! every asset name is a distinct collectible identity (NFT /
//! RFT shape) rather than CNT distribution. See
//! `docs/design/COLLECTION_MODULES.md`.
//!
//! ## Lifecycle
//!
//! 1. Consumer registers `holds_policy(X)` via
//!    `update_interest(Add, ...)`. The module:
//!    - Calls `chain_data::utxos_by_policy(X)` to enumerate
//!      every current UTxO holding the policy (dolos's
//!      `BY_POLICY` index — sub-second even for active
//!      policies).
//!    - Decodes each output's address into a `HolderRef`
//!      (`Stake` / `Payment` / `Script`) and accumulates a
//!      per-`(asset_name, holder)` ledger.
//!    - Persists the ledger to state-kv keyed by policy hex.
//!    - Emits the ledger as a chunked `SnapshotBegin` →
//!      `SnapshotChunk` × N → `SnapshotEnd` sequence.
//!
//! 2. Each subsequent `handle_events` batch (one Cardano TX):
//!    - Walks Produced + Consumed events for tracked policies.
//!    - Computes per-asset movements from the TX's net effect.
//!    - Persists the updated ledger.
//!    - Emits a `CollectionDelta` enumerating the movements.
//!
//! 3. Removing `holds_policy(X)` (`update_interest(Remove, ...)`)
//!    clears the in-memory tracking set; the persisted ledger
//!    is dropped from state-kv (per `COLLECTION_MODULES.md`
//!    resolved-decision #1 — clear immediately, no TTL).
//!
//! ## Why script addresses surface as data
//!
//! Maestro's `/policy/{id}/accounts` groups by stake credential
//! and omits script-locked supply (marketplace escrows). For
//! collectible policies that's a real data loss — listed NFTs
//! are part of the supply picture. This module surfaces
//! `HolderRef::Script` so consumers (collection-ownership,
//! TCG worker) can include, exclude, or re-attribute
//! marketplace-held supply via their own policy.
//!
//! ## Chunk 1 scope (this file at landing time)
//!
//! - Module scaffolding: all v2 Guest exports stubbed.
//! - Interest management: tracked-policy set persisted to
//!   state-kv, restored on init.
//! - Wire-format event types via
//!   `mitos_community_events::collection_holders`.
//! - Cold-start (`Chunk 2`) and per-TX delta walk (`Chunk 3`)
//!   are stubbed with explicit TODOs; the module compiles and
//!   is dispatch-safe but doesn't yet emit anything.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};

use mitos_community_events::collection_holders::{
    CollectionDelta, CollectionEvent, Holding, HolderRef, Movement, SnapshotBegin, SnapshotChunk,
    SnapshotEnd,
};
use mitos_module_kit::ReentrantRound;
use pallas_addresses::{Address, ShelleyDelegationPart, ShelleyPaymentPart};
use serde::{Deserialize, Serialize};

use crate::mitos::platform_v2::chain_data;
use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::state_kv;
use crate::mitos::platform_v2::types::{
    AssetEntry as WitAssetEntry, ConsumedEvent, OutputRef as WitOutputRef, ProducedEvent,
    RollbackEvent, UtxoEvent,
};

const LOG_TARGET: &str = "collection-holders-module";

/// state-kv key under which we persist the set of currently-
/// tracked policies (CBOR list of 56-char hex strings). The
/// per-policy ledger is keyed `ledger:<policy_hex>`.
const KV_TRACKED_POLICIES: &str = "tracked-policies";

/// state-kv key prefix for per-policy holding ledgers. Full
/// key: `ledger:<policy_hex>`.
const KV_LEDGER_PREFIX: &str = "ledger:";

/// state-kv key for the re-entrant `rebootstrap` continuation
/// cursor — the index of the policy currently being re-scanned
/// (8 BE bytes). Durable so a host restart mid-round resumes
/// at the right policy. Per-page progress within a policy is
/// thread-local + volatile; a trap or restart restarts the
/// current policy from page 0 (safe — each policy emits a full
/// authoritative `SnapshotBegin` → ... → `SnapshotEnd`).
const KV_REBOOTSTRAP_CURSOR: &str = "rebootstrap-cursor";

/// Holdings per `SnapshotChunk` when emitting a chunked
/// snapshot. Bounds the CBOR buffer one `emit` builds in wasm
/// memory — see `WASM_BUDGET_CHUNKING.md` "Output — chunked
/// snapshot emission".
const SNAPSHOT_CHUNK_HOLDINGS: usize = 1_000;

/// Per-page hint for the cold-start scan. `utxos_by_policy` is
/// paged; the host clamps each returned page to its own
/// adaptive per-call budget, so the module never holds more
/// than one clamped page of refs at once.
const COLD_START_PAGE_HINT: u32 = 10_000;

/// 28-byte hash size used everywhere for policy ids and stake
/// credentials.
const HASH_BYTES: usize = 28;

thread_local! {
    /// Currently-tracked policy ids (28-byte hashes). Cleared
    /// on `Remove`/`Replace` ops, repopulated on `Add`.
    /// Persisted via `KV_TRACKED_POLICIES` so a host restart
    /// without an attached companion keeps filtering correctly.
    static TRACKED_POLICIES: RefCell<HashSet<[u8; HASH_BYTES]>> = RefCell::new(HashSet::new());

    /// In-flight `rebootstrap` round. `None` between rounds.
    /// `ReentrantRound` (from `mitos-module-kit`) owns the
    /// policy list, the `predicate_idx`, the page cursor, and
    /// the per-policy `PolicyLedger` accumulator. Resident in
    /// the wasm instance across the host's re-entrant call
    /// loop; a trap or host restart discards it and the round
    /// resumes from the durable state-kv cursor
    /// (`predicate_idx` only).
    static REBOOTSTRAP_STATE: RefCell<Option<ReentrantRound<[u8; HASH_BYTES], PolicyLedger>>> =
        const { RefCell::new(None) };

    /// In-progress chunked-snapshot emit for the policy whose
    /// scan just finished. `None` when no emit is in flight.
    /// The `rebootstrap` state machine drains one
    /// `SnapshotChunk` from here per call — emitting the whole
    /// holdings list in a single fuel budget traps for a large
    /// policy.
    static REBOOTSTRAP_EMIT: RefCell<Option<EmitState>> = const { RefCell::new(None) };
}

/// A chunked snapshot mid-emit: the materialised holdings list
/// for one policy, drained `SNAPSHOT_CHUNK_HOLDINGS` at a time.
struct EmitState {
    /// 56-char hex policy id — every event of the sequence
    /// carries it.
    policy_hex: String,
    /// The full holdings list, built once when the scan
    /// completed.
    holdings: Vec<Holding>,
    /// Offset into `holdings` — the next chunk starts here.
    offset: usize,
}

// ============================================================
// In-memory ledger
// ============================================================

/// Internal stable identity for a holder. Mirrors the wire
/// `HolderRef` variants but **without the `Script.label` field**
/// — labels are a presentation concern derived from the
/// `address-registry` at emission time, not a ledger key. Two
/// holdings at the same script address are the same holder
/// regardless of whether the registry has been updated between
/// snapshots.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
enum HolderKey {
    /// 28-byte stake credential (Key or Script delegation
    /// part). Both delegation kinds collapse to the same
    /// representation — the wire format is intentionally
    /// network-agnostic, so consumers compute the bech32
    /// stake address themselves at presentation time. The
    /// Key-vs-Script edge (rare for NFT holdings) is
    /// recoverable consumer-side via byte-level credential
    /// comparison.
    Stake([u8; HASH_BYTES]),
    /// Enterprise (no-stake-cred) wallet with a `Key` payment
    /// credential. Bech32 carries the full address.
    Payment(String),
    /// Script address — `Script` payment credential, no
    /// delegation (`addr1w...` enterprise script). Frankenscript
    /// addresses (`addr1z...` / `addr1x...` — Script payment +
    /// stake-delegated) are emitted as `Stake` instead, since
    /// the stake credential is the owner's identity.
    Script(String),
}

impl HolderKey {
    /// Build the wire-shape `HolderRef` for emission. Script
    /// labels resolve here via the (TODO follow-up)
    /// `address-registry` — `None` for now keeps the wire
    /// shape stable and the lookup integration small.
    fn to_wire(&self) -> HolderRef {
        match self {
            HolderKey::Stake(bytes) => HolderRef::Stake(hex::encode(bytes)),
            HolderKey::Payment(addr) => HolderRef::Payment(addr.clone()),
            HolderKey::Script(addr) => HolderRef::Script {
                addr: addr.clone(),
                // TODO(follow-up): resolve via
                // shared-crates/address-registry once the
                // wasm-build compatibility of that crate is
                // verified. Resolved decision #3 in
                // `docs/design/COLLECTION_MODULES.md`.
                label: None,
            },
        }
    }
}

/// `asset_name_hex_bytes -> quantity` for one (policy, holder).
/// Matches the holder-distribution pattern — nest-by-holder.
type AssetMap = BTreeMap<Vec<u8>, u64>;

/// Per-policy ledger as held in memory + persisted to state-kv.
/// Keyed by `HolderKey` so script-held supply surfaces
/// distinctly from stake-delegated supply.
#[derive(Default, Serialize, Deserialize)]
struct PolicyLedger {
    holdings: BTreeMap<HolderKey, AssetMap>,
}

fn ledger_key(policy_hex: &str) -> String {
    format!("{KV_LEDGER_PREFIX}{policy_hex}")
}

#[allow(dead_code)] // Used by handle_produced / handle_consumed (Chunk 3).
fn load_ledger(policy_hex: &str) -> PolicyLedger {
    let Some(bytes) = state_kv::get_value(&ledger_key(policy_hex)) else {
        return PolicyLedger::default();
    };
    ciborium::de::from_reader(bytes.as_slice()).unwrap_or_default()
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
// Address → HolderKey
// ============================================================

/// Decode a bech32 address into a `HolderKey`. Returns `None`
/// for unsupported address shapes (Byron, pointer-stake) — the
/// caller drops the holding rather than mis-categorising.
///
/// **Frankenaddress note:** modern Cardano smart contracts
/// (jpg.store v3, Splash, etc.) use Script-payment +
/// Key-delegation addresses where the staking part carries the
/// owner's stake key. The contract locks the asset but the
/// owner's identity travels with it via the stake credential.
/// We promote these to `Stake` rather than `Script` — the
/// stake credential IS the asset owner from a downstream
/// consumer's perspective. Only true enterprise-script
/// (`addr1w...`, no stake part) remain as `Script`.
///
/// Decision tree:
/// - Byron / non-Shelley → `None`
/// - Shelley + any payment + `Key`/`Script` delegation →
///   `Stake(28-byte cred)` — covers both regular delegated
///   wallets AND frankenscript marketplace lockings. Both
///   delegation kinds collapse to the same 28-byte
///   representation; consumers compute bech32 themselves
///   using their network context.
/// - Shelley + `Key` payment + `Null` delegation →
///   `Payment(addr)` — enterprise wallet, no stake key.
/// - Shelley + `Script` payment + `Null` delegation →
///   `Script(addr)` — true enterprise-script (rare).
/// - Shelley + `Pointer` delegation → `None` (rare, dropped).
fn extract_holder_key(address: &str) -> Option<HolderKey> {
    let addr = Address::from_bech32(address).ok()?;
    let shelley = match addr {
        Address::Shelley(s) => s,
        _ => return None,
    };

    // If the address has a stake-delegation credential (Key or
    // Script), the stake credential IS the holder identity.
    // This is the dominant case — regular wallets AND
    // frankenscript marketplace lockings both fall here.
    match shelley.delegation() {
        ShelleyDelegationPart::Key(h) => {
            let bytes: [u8; HASH_BYTES] = (**h).into();
            return Some(HolderKey::Stake(bytes));
        }
        ShelleyDelegationPart::Script(h) => {
            let bytes: [u8; HASH_BYTES] = (**h).into();
            return Some(HolderKey::Stake(bytes));
        }
        ShelleyDelegationPart::Null => {}
        // Pointer delegation: rare, dropped.
        _ => return None,
    }

    // No stake delegation — branch on payment credential type.
    match shelley.payment() {
        ShelleyPaymentPart::Key(_) => Some(HolderKey::Payment(address.to_string())),
        ShelleyPaymentPart::Script(_) => Some(HolderKey::Script(address.to_string())),
    }
}

// ============================================================
// Ledger mutation — apply a single output
// ============================================================

/// Add an output's policy-X assets to the ledger. Idempotent
/// at the (asset_name, holder) level — repeated calls for the
/// same UTxO accumulate, which is correct since one wallet
/// can hold multiple UTxOs each carrying the same asset (RFT
/// holdings split across UTxOs).
fn apply_output_to_ledger(
    ledger: &mut PolicyLedger,
    policy: &[u8],
    address: &str,
    assets: &[WitAssetEntry],
) {
    let Some(key) = extract_holder_key(address) else {
        return;
    };
    let entry = ledger.holdings.entry(key).or_default();
    for asset in assets {
        if asset.asset.policy != policy {
            continue;
        }
        *entry.entry(asset.asset.name.clone()).or_insert(0) += asset.quantity;
    }
}

/// Read one page of UTxOs and fold their policy-X holdings
/// into the ledger.
fn fold_page(ledger: &mut PolicyLedger, policy: &[u8; HASH_BYTES], refs: &[WitOutputRef]) {
    for (_oref, out) in chain_data::read_utxos(refs) {
        apply_output_to_ledger(ledger, policy, &out.address, &out.assets);
    }
}

// ============================================================
// Wire-side interest predicate mirror
// ============================================================

/// Minimal mirror of the on-wire `InterestPredicate` enum —
/// same pattern as `holder-distribution`. We only care about
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

    // Drop per-policy ledger state for any policy that just
    // left the tracked set. Resolved-decision #1 in the design
    // doc — no TTL, no refcount; rebuild on next subscribe.
    for policy in &removed {
        let policy_hex = hex::encode(policy);
        delete_ledger(&policy_hex);
        logging::log(
            LogLevel::Info,
            LOG_TARGET,
            &format!("dropped ledger for untracked policy {policy_hex}"),
        );
    }

    // Cold-start each newly-added policy. Single fuel budget
    // per call — for very large collections (10k+ supply) the
    // host's adaptive page sizing keeps each `utxos_by_policy`
    // call within budget; the loop fits multiple pages per
    // fuel envelope. For pathological cases (100k+) recapture
    // via the chunked `rebootstrap` entry point is the safety
    // net (Chunk 2 follow-up).
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

fn emit_event(event: &CollectionEvent) {
    let mut buf = Vec::with_capacity(512);
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode CollectionEvent failed: {e}"),
        );
        return;
    }
    emit::emit_event(0, &buf);
}

/// Flatten the in-memory ledger into the wire-shape
/// `Vec<Holding>` for snapshot emission. Ordering is
/// deterministic: outer iteration over `BTreeMap<HolderKey,
/// _>` gives holders in `HolderKey`'s natural order, inner
/// iteration over `AssetMap` gives asset names in byte order.
/// Consumers DB-insert each holding, so wire order is
/// immaterial for correctness, but determinism makes golden
/// testing simpler.
fn ledger_to_holdings(ledger: &PolicyLedger) -> Vec<Holding> {
    let mut out: Vec<Holding> = Vec::new();
    for (key, assets) in &ledger.holdings {
        let holder = key.to_wire();
        for (name_bytes, qty) in assets {
            out.push(Holding {
                asset_name_hex: hex::encode(name_bytes),
                holder: holder.clone(),
                quantity: *qty,
            });
        }
    }
    out
}

/// Emit `SnapshotBegin` and stage the holdings list in
/// `REBOOTSTRAP_EMIT` so the `rebootstrap` state machine can
/// drain one `SnapshotChunk` per call. Emitting the whole list
/// in one fuel budget traps for a large policy (per
/// holder-distribution's 2026-05-19 prod-recapture finding).
fn open_chunked_emit(policy_hex: String, holdings: Vec<Holding>, anchor_slot: u64) {
    emit_event(&CollectionEvent::SnapshotBegin(SnapshotBegin {
        policy: policy_hex.clone(),
        cursor_slot: anchor_slot,
        cursor_hash_hex: String::new(),
    }));
    REBOOTSTRAP_EMIT.with(|cell| {
        *cell.borrow_mut() = Some(EmitState {
            policy_hex,
            holdings,
            offset: 0,
        });
    });
}

/// Emit the full chunked snapshot sequence for one policy in a
/// single fuel budget. `SnapshotBegin` → `SnapshotChunk` × N →
/// `SnapshotEnd`.
///
/// Used by the live `update_interest(Add, ...)` cold-start path
/// where the whole emission is expected to fit one budget. The
/// recapture path (`rebootstrap`) spreads emission across many
/// calls via [`open_chunked_emit`] + the re-entrant state
/// machine.
fn emit_full_snapshot(policy_hex: &str, holdings: Vec<Holding>, anchor_slot: u64) {
    emit_event(&CollectionEvent::SnapshotBegin(SnapshotBegin {
        policy: policy_hex.to_string(),
        cursor_slot: anchor_slot,
        // Anchor block hash isn't surfaced by `utxo-page`
        // today; consumers don't rely on it (slot is the
        // ordering key). Left empty pending a host-side
        // addition if needed.
        cursor_hash_hex: String::new(),
    }));
    for chunk in holdings.chunks(SNAPSHOT_CHUNK_HOLDINGS) {
        emit_event(&CollectionEvent::SnapshotChunk(SnapshotChunk {
            policy: policy_hex.to_string(),
            holdings: chunk.to_vec(),
        }));
    }
    emit_event(&CollectionEvent::SnapshotEnd(SnapshotEnd {
        policy: policy_hex.to_string(),
        holding_count: holdings.len() as u64,
    }));
}

// ============================================================
// Cold-start scan
// ============================================================

/// Run the bootstrap scan for a newly-tracked policy in one
/// fuel budget: page through `utxos_by_policy` → fold each
/// page into the ledger → persist → emit the chunked
/// snapshot. Used by the live `update_interest(Add, ...)`
/// path.
///
/// The scan is **paged** (`WASM_BUDGET_CHUNKING.md`): each
/// call to `utxos_by_policy` returns one host-clamped page,
/// so the only resident state is the holder-bounded ledger.
/// The re-entrant `rebootstrap` path (recapture) spreads the
/// same scan across many fuel-budgeted calls; see
/// `Module::rebootstrap` (Chunk 2 follow-up).
fn cold_start(policy: &[u8; HASH_BYTES]) {
    let mut ledger = PolicyLedger::default();
    let mut after: Option<Vec<u8>> = None;
    // Assigned on every loop iteration before it is read after
    // the loop — the `loop` body always runs at least once.
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
    persist_ledger(&policy_hex, &ledger);
    let holdings = ledger_to_holdings(&ledger);
    let holding_count = holdings.len();
    emit_full_snapshot(&policy_hex, holdings, anchor_slot);

    logging::log(
        LogLevel::Info,
        LOG_TARGET,
        &format!(
            "cold-start policy={policy_hex}: {total_utxos} UTxO(s) → {holding_count} holding(s) @ slot {anchor_slot}"
        ),
    );
}

// ============================================================
// Per-TX event handling
// ============================================================

/// Per-`handle_events` buffer. One Cardano TX's worth of
/// produced + consumed events filtered to tracked policies.
/// Flushed (deltas computed + ledger updated + `CollectionDelta`
/// emitted) at the end of the dispatch call.
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
        buf.slot = slot_from_cursor(&c.cursor);
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

fn handle_rollback(r: &RollbackEvent) {
    // The v2 contract: companion's apply_event must be idempotent
    // at the chain-point level, and the host re-applies events
    // after a rollback (consumer's UPSERT/DELETE pattern handles
    // re-delivery). Per `MITOS_PLATFORM_V2.md` "Rollbacks", the
    // platform re-feeds events from `to_cursor` forward and the
    // chain-point-keyed ledger naturally converges.
    //
    // We log here for operator visibility; no module-side
    // bookkeeping is needed since we don't keep a per-cursor
    // history. The persisted ledger will diverge briefly between
    // `to_cursor` and the next re-application sweep — acceptable,
    // as the consumer's projection isn't authoritative until
    // events catch back up.
    let slot = slot_from_cursor(&r.to_cursor);
    logging::log(
        LogLevel::Info,
        LOG_TARGET,
        &format!("rollback to slot {slot} — relying on re-apply convergence"),
    );
}

/// Compute per-(asset, holder) net deltas across the TX, then
/// emit `Movement`s with greedy from/to pairing. Updates the
/// persisted ledger as a side-effect.
fn flush_buffer(buf: TxBuffer) {
    let Some(tx_hash) = buf.tx_hash else {
        return;
    };
    let tx_hash_hex = hex::encode(&tx_hash);
    let slot = buf.slot;

    let mut policies: HashSet<[u8; HASH_BYTES]> = HashSet::new();
    policies.extend(buf.produced.keys().copied());
    policies.extend(buf.consumed.keys().copied());

    for policy in policies {
        let policy_hex = hex::encode(policy);
        let mut ledger = load_ledger(&policy_hex);

        // Build per-asset, per-holder net delta map.
        // Positive = produced (gained), negative = consumed (lost).
        // i64 — RFT quantities fit in i64 comfortably for realistic
        // edition sizes; CNT-scale supply would overflow but those
        // shouldn't be routed here (the module hard-rejects FT
        // classification — Chunk 2 follow-up).
        let mut deltas: BTreeMap<Vec<u8>, BTreeMap<HolderKey, i64>> = BTreeMap::new();

        if let Some(events) = buf.consumed.get(&policy) {
            for c in events {
                let Some(key) = extract_holder_key(&c.prior_output.address) else {
                    continue;
                };
                for asset in &c.prior_output.assets {
                    if asset.asset.policy != policy {
                        continue;
                    }
                    let per_asset = deltas.entry(asset.asset.name.clone()).or_default();
                    *per_asset.entry(key.clone()).or_insert(0) -= asset.quantity as i64;
                }
            }
        }
        if let Some(events) = buf.produced.get(&policy) {
            for p in events {
                let Some(key) = extract_holder_key(&p.output.address) else {
                    continue;
                };
                for asset in &p.output.assets {
                    if asset.asset.policy != policy {
                        continue;
                    }
                    let per_asset = deltas.entry(asset.asset.name.clone()).or_default();
                    *per_asset.entry(key.clone()).or_insert(0) += asset.quantity as i64;
                }
            }
        }

        // Apply non-zero deltas to the ledger + build Movement
        // list with greedy from/to pairing per asset.
        let mut movements: Vec<Movement> = Vec::new();

        for (asset_name, per_holder) in deltas {
            // Update the persisted ledger first — same-holder
            // zero deltas (change outputs) net out and never
            // surface as movements, but they don't need ledger
            // updates either.
            for (key, delta) in &per_holder {
                if *delta == 0 {
                    continue;
                }
                let entry = ledger.holdings.entry(key.clone()).or_default();
                let prev = *entry.get(&asset_name).unwrap_or(&0) as i64;
                let new = prev + delta;
                if new <= 0 {
                    entry.remove(&asset_name);
                    if entry.is_empty() {
                        ledger.holdings.remove(key);
                    }
                } else {
                    entry.insert(asset_name.clone(), new as u64);
                }
            }

            // Split per-holder deltas into positive (gainers)
            // and negative (losers, qty absolute).
            let mut pos: Vec<(HolderKey, u64)> = Vec::new();
            let mut neg: Vec<(HolderKey, u64)> = Vec::new();
            for (key, delta) in per_holder {
                match delta.cmp(&0) {
                    std::cmp::Ordering::Greater => pos.push((key, delta as u64)),
                    std::cmp::Ordering::Less => neg.push((key, (-delta) as u64)),
                    std::cmp::Ordering::Equal => {}
                }
            }

            // Greedy pairing: match negative with positive,
            // emitting a single `from→to` Movement for the
            // overlap. Unmatched negative residue is a burn
            // (to = None); unmatched positive residue is a
            // mint (from = None).
            //
            // Guard with `is_empty` checks rather than
            // `while let (Some, Some) = (pos.pop(), neg.pop())`:
            // the latter pops from BOTH sides unconditionally
            // each iteration, so a pure-mint TX (neg empty,
            // pos non-empty) would `pop()` the gainer from pos,
            // fail the pattern match, and silently discard it.
            let asset_name_hex = hex::encode(&asset_name);
            while !pos.is_empty() && !neg.is_empty() {
                let mut p = pos.pop().unwrap();
                let mut n = neg.pop().unwrap();
                let q = p.1.min(n.1);
                movements.push(Movement {
                    asset_name_hex: asset_name_hex.clone(),
                    from: Some(n.0.to_wire()),
                    to: Some(p.0.to_wire()),
                    quantity: q,
                });
                p.1 -= q;
                n.1 -= q;
                if p.1 > 0 {
                    pos.push(p);
                }
                if n.1 > 0 {
                    neg.push(n);
                }
            }
            for (key, qty) in pos {
                movements.push(Movement {
                    asset_name_hex: asset_name_hex.clone(),
                    from: None,
                    to: Some(key.to_wire()),
                    quantity: qty,
                });
            }
            for (key, qty) in neg {
                movements.push(Movement {
                    asset_name_hex: asset_name_hex.clone(),
                    from: Some(key.to_wire()),
                    to: None,
                    quantity: qty,
                });
            }
        }

        persist_ledger(&policy_hex, &ledger);

        if !movements.is_empty() {
            emit_event(&CollectionEvent::Delta(CollectionDelta {
                policy: policy_hex,
                tx_hash: tx_hash_hex.clone(),
                slot,
                movements,
            }));
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
                // tx-context, referenced, minted, tick — not
                // consumed by collection-holders. Mints surface
                // as Produced events (asset materialising into
                // a holder) which the produced handler covers;
                // burns surface as Consumed without matching
                // Produced.
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
    ///   closing `SnapshotEnd` (which also advances the durable
    ///   predicate cursor past the just-finished policy);
    /// - **scan phase** — otherwise, scan one page of the
    ///   current policy's UTxO set, folding it into the ledger;
    ///   when the last page lands, persist the ledger and open
    ///   the chunked emit (`open_chunked_emit`).
    ///
    /// A page of UTxOs fits one fuel budget and a chunk of
    /// holdings fits one fuel budget; a whole large policy's
    /// scan *or* snapshot does not — hence each is spread
    /// across calls.
    ///
    /// Round state (policy list + page cursor + accumulating
    /// ledger) and the in-flight emit are thread-local —
    /// resident across the host's loop. The durable cursor in
    /// `state-kv` (`KV_REBOOTSTRAP_CURSOR`) is only the
    /// `predicate_idx`, and it is **not advanced until the
    /// predicate's emit closes** — so a trap or host restart
    /// anywhere in a policy (scan or emit) restarts it from
    /// page 0, re-scanning + re-emitting. Safe because the
    /// consumer wipes its projection on `SnapshotBegin`.
    fn rebootstrap() -> Result<RebootstrapStep, String> {
        // ── Emit phase ── Drain an in-flight chunked snapshot
        // one `SnapshotChunk` per call.
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
            if state.offset >= state.holdings.len() {
                emit_event(&CollectionEvent::SnapshotEnd(SnapshotEnd {
                    policy: state.policy_hex.clone(),
                    holding_count: state.holdings.len() as u64,
                }));
                *slot = None;
                EmitOutcome::Closed
            } else {
                let end =
                    (state.offset + SNAPSHOT_CHUNK_HOLDINGS).min(state.holdings.len());
                emit_event(&CollectionEvent::SnapshotChunk(SnapshotChunk {
                    policy: state.policy_hex.clone(),
                    holdings: state.holdings[state.offset..end].to_vec(),
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

            // First call of a round (or the thread-local was
            // wiped by a trap/restart) — rebuild round state.
            // The policy list is sorted so the durable
            // `predicate_idx` cursor is stable across a host
            // restart.
            if state.is_none() {
                let mut policies: Vec<[u8; HASH_BYTES]> =
                    TRACKED_POLICIES.with(|s| s.borrow().iter().copied().collect());
                policies.sort_unstable();
                *state = Some(ReentrantRound::resume(policies, load_rebootstrap_cursor()));
            }
            let round = state.as_mut().expect("round initialised above");

            // No policies left (empty tracked set, or resumed
            // past the end) — round done.
            let Some(&policy) = round.current() else {
                clear_rebootstrap_cursor();
                *state = None;
                return Ok(RebootstrapStep {
                    done: true,
                    ingested: 0,
                });
            };

            // Process exactly one page of the current policy.
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
                    let holdings = ledger_to_holdings(&ledger);
                    logging::log(
                        LogLevel::Info,
                        LOG_TARGET,
                        &format!(
                            "rebootstrap policy={policy_hex}: {total_utxos} UTxO(s) → {} holding(s) @ slot {anchor_slot}; opening chunked emit",
                            holdings.len()
                        ),
                    );
                    open_chunked_emit(policy_hex, holdings, anchor_slot);
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
