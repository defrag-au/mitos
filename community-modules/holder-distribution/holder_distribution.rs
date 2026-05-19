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
//!    - Emits a `HolderEvent::Snapshot` with the full ledger.
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
//! ## Address → stake credential extraction
//!
//! Uses `pallas-addresses::Address::from_bech32` to parse each
//! output's bech32 + extract the staking part. Enterprise
//! addresses (no stake) are aggregated under `stake_cred_hex:
//! None`; pointer-stake addresses (rare) get hex-encoded
//! pointer bytes. Byron addresses are ignored (no native asset
//! support pre-Shelley).

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};

use mitos_community_events::holder_distribution::{
    AssetBalance, HolderDelta, HolderEntry, HolderEvent, HolderSnapshot,
};
use pallas_addresses::{Address, ShelleyDelegationPart};
use serde::{Deserialize, Serialize};

use crate::mitos::platform_v2::chain_data;
use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::state_kv;
use crate::mitos::platform_v2::types::{
    AssetEntry as WitAssetEntry, ConsumedEvent, ProducedEvent, UtxoEvent,
};

const LOG_TARGET: &str = "holder-distribution-module";

/// state-kv key under which we persist the set of currently-
/// tracked policies (CBOR list of 56-char hex strings). The
/// per-policy ledger is keyed `ledger:<policy_hex>`.
const KV_TRACKED_POLICIES: &str = "tracked-policies";

/// Read-cap for the cold-start scan. Matches the host's
/// internal cap; reaching this means the policy is too active
/// to fit in one shot and we should fall back to event-replay
/// hydration (not yet implemented).
const COLD_START_CAP: usize = 100_000;

/// 28-byte hash size used everywhere for stake credentials,
/// policy ids, payment hashes.
const HASH_BYTES: usize = 28;

thread_local! {
    /// Currently-tracked policy ids (28-byte hashes). Cleared on
    /// `Remove`/`Replace` ops, repopulated on `Add`. Persisted
    /// via `KV_TRACKED_POLICIES` so a host restart without an
    /// attached companion keeps filtering correctly.
    static TRACKED_POLICIES: RefCell<HashSet<[u8; HASH_BYTES]>> = RefCell::new(HashSet::new());

    /// Pending policies for an in-progress `rebootstrap` round.
    /// `None` between rounds. The host's recapture loop drains
    /// this one cold-start per `rebootstrap` call so each gets a
    /// fresh fuel budget.
    static REBOOTSTRAP_QUEUE: RefCell<Option<Vec<[u8; HASH_BYTES]>>> = RefCell::new(None);
}

/// In-memory ledger key. `Some(stake_hash)` for key/script
/// stake creds; `None` for enterprise (no-stake) outputs.
type LedgerKey = Option<[u8; HASH_BYTES]>;

/// `(asset_name_hex_bytes -> quantity)` for one (policy, holder).
type AssetMap = BTreeMap<Vec<u8>, u64>;

/// Per-policy ledger as held in memory + persisted to state-kv.
/// Inner map keyed by stake-cred bytes (with the enterprise
/// bucket under `vec![]` since BTreeMap keys must be Ord +
/// `Option<[u8;28]>` doesn't serialize to a stable on-wire shape).
///
/// The persistence format mirrors this 1-1.
#[derive(Default, Serialize, Deserialize)]
struct PolicyLedger {
    /// `stake_cred_bytes (empty Vec for enterprise) -> {asset_name -> qty}`.
    holders: BTreeMap<Vec<u8>, AssetMap>,
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
// Stake credential extraction
// ============================================================

/// Extract a stake-credential key for ledger storage from a
/// bech32 address. Returns `None` for enterprise / Byron / any
/// shape we can't classify — those holders get bucketed under
/// the no-stake aggregator.
fn extract_stake_key(address: &str) -> LedgerKey {
    let addr = Address::from_bech32(address).ok()?;
    let shelley = match addr {
        Address::Shelley(s) => s,
        _ => return None, // Byron, etc.
    };
    match shelley.delegation() {
        ShelleyDelegationPart::Key(h) => {
            let bytes: [u8; HASH_BYTES] = (**h).into();
            Some(bytes)
        }
        ShelleyDelegationPart::Script(h) => {
            let bytes: [u8; HASH_BYTES] = (**h).into();
            Some(bytes)
        }
        // Pointer and Null get bucketed with enterprise — pointer
        // delegation is rare (~1% of addresses) and Null is the
        // enterprise shape. Lumping them is fine for v1; pointer
        // delegation can be surfaced separately later if needed.
        _ => None,
    }
}

fn ledger_key_bytes(key: LedgerKey) -> Vec<u8> {
    match key {
        Some(h) => h.to_vec(),
        None => Vec::new(),
    }
}

fn stake_cred_hex(key: &[u8]) -> Option<String> {
    if key.is_empty() {
        None
    } else {
        Some(hex::encode(key))
    }
}

// ============================================================
// Ledger mutation
// ============================================================

/// Add `(asset_name, qty)` under `(policy, key)` in the ledger.
fn ledger_add(ledger: &mut PolicyLedger, key: LedgerKey, asset_name: &[u8], qty: u64) {
    let kbytes = ledger_key_bytes(key);
    let entry = ledger.holders.entry(kbytes).or_default();
    let total = entry.entry(asset_name.to_vec()).or_insert(0);
    *total = total.saturating_add(qty);
}

/// Subtract `(asset_name, qty)` under `(policy, key)` in the
/// ledger. Removes the asset entry when it hits zero; removes
/// the holder entry when their last asset hits zero. Saturating
/// subtract — if a UTxO accounting bug ever drives a balance
/// negative we'd rather report 0 than crash.
fn ledger_sub(ledger: &mut PolicyLedger, key: LedgerKey, asset_name: &[u8], qty: u64) {
    let kbytes = ledger_key_bytes(key);
    if let Some(entry) = ledger.holders.get_mut(&kbytes) {
        if let Some(total) = entry.get_mut(asset_name) {
            *total = total.saturating_sub(qty);
            if *total == 0 {
                entry.remove(asset_name);
            }
        }
        if entry.is_empty() {
            ledger.holders.remove(&kbytes);
        }
    }
}

/// Apply one `(address, assets)` produced output to the
/// ledger for a single policy. Touched holders are inserted
/// into `touched` for later delta emission.
fn apply_produced_to_ledger(
    ledger: &mut PolicyLedger,
    policy: &[u8],
    address: &str,
    assets: &[WitAssetEntry],
    touched: &mut HashSet<Vec<u8>>,
) {
    let key = extract_stake_key(address);
    let kbytes = ledger_key_bytes(key);
    for entry in assets {
        if entry.asset.policy != policy {
            continue;
        }
        ledger_add(ledger, key, &entry.asset.name, entry.quantity);
        touched.insert(kbytes.clone());
    }
}

/// Symmetric for consumed.
fn apply_consumed_to_ledger(
    ledger: &mut PolicyLedger,
    policy: &[u8],
    address: &str,
    assets: &[WitAssetEntry],
    touched: &mut HashSet<Vec<u8>>,
) {
    let key = extract_stake_key(address);
    let kbytes = ledger_key_bytes(key);
    for entry in assets {
        if entry.asset.policy != policy {
            continue;
        }
        ledger_sub(ledger, key, &entry.asset.name, entry.quantity);
        touched.insert(kbytes.clone());
    }
}

// ============================================================
// Cold-start
// ============================================================

/// Run the bootstrap scan for a newly-registered policy:
/// `utxos_by_policy(X)` → `read_utxos(refs)` → build ledger →
/// persist → emit `Snapshot`.
fn cold_start(policy: &[u8; HASH_BYTES]) {
    let policy_hex = hex::encode(policy);
    let refs = chain_data::utxos_by_policy(policy);
    if refs.len() >= COLD_START_CAP {
        // Hit the host-side cap. The ledger we'd build would be
        // partial; rather than emit a partial snapshot, log
        // and leave the policy un-snapshotted. Live deltas will
        // still arrive and apply against an empty ledger —
        // consumers will see incorrect cumulative totals until
        // we surface a pagination story.
        logging::log(
            LogLevel::Warn,
            LOG_TARGET,
            &format!(
                "cold-start policy={policy_hex}: hit COLD_START_CAP={COLD_START_CAP}; snapshot suppressed"
            ),
        );
        return;
    }

    // Bulk-resolve refs in chunks (host enforces its own batch
    // limits; 1K at a time stays well under any plausible cap).
    let mut ledger = PolicyLedger::default();
    for chunk in refs.chunks(1024) {
        let outputs = chain_data::read_utxos(&chunk.to_vec());
        for out in outputs {
            let mut _touched_dummy: HashSet<Vec<u8>> = HashSet::new();
            apply_produced_to_ledger(
                &mut ledger,
                policy,
                &out.address,
                &out.assets,
                &mut _touched_dummy,
            );
        }
    }

    persist_ledger(&policy_hex, &ledger);

    let holders = ledger_to_holders(&ledger);
    let snapshot = HolderSnapshot {
        policy: policy_hex.clone(),
        // Cold-start cursor is "now" — we don't have a clean
        // anchor without a TX-level event in hand. Consumers
        // treat the snapshot as authoritative replacement and
        // start replaying deltas from the first event after it
        // arrives. The slot field stays 0 in this phase; a
        // future `chain_data::tip()` host-fn would let us
        // populate the real cursor.
        cursor_slot: 0,
        cursor_hash_hex: String::new(),
        holders,
    };
    emit_event(&HolderEvent::Snapshot(snapshot));
    logging::log(
        LogLevel::Info,
        LOG_TARGET,
        &format!(
            "cold-start policy={policy_hex}: {} UTxO(s) → {} holder(s)",
            refs.len(),
            ledger.holders.len()
        ),
    );
}

/// Render the in-memory `PolicyLedger` as the wire-shape
/// `Vec<HolderEntry>` — sorted by total quantity descending
/// for deterministic golden tests and to make top-N consumer
/// slicing cheap.
fn ledger_to_holders(ledger: &PolicyLedger) -> Vec<HolderEntry> {
    let mut out: Vec<HolderEntry> = ledger
        .holders
        .iter()
        .map(|(kbytes, assets)| HolderEntry {
            stake_cred_hex: stake_cred_hex(kbytes),
            assets: assets_map_to_vec(assets),
        })
        .collect();
    out.sort_by(|a, b| {
        let a_total: u64 = a.assets.iter().map(|e| e.quantity).sum();
        let b_total: u64 = b.assets.iter().map(|e| e.quantity).sum();
        b_total
            .cmp(&a_total)
            // Tie-break on stake_cred_hex for stable order in
            // the (rare) case of equal totals.
            .then_with(|| a.stake_cred_hex.cmp(&b.stake_cred_hex))
    });
    out
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
        let mut touched: HashSet<Vec<u8>> = HashSet::new();

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
            .map(|kbytes| {
                let assets = ledger
                    .holders
                    .get(&kbytes)
                    .map(assets_map_to_vec)
                    .unwrap_or_default();
                HolderEntry {
                    stake_cred_hex: stake_cred_hex(&kbytes),
                    assets,
                }
            })
            .collect();
        changed.sort_by(|a, b| a.stake_cred_hex.cmp(&b.stake_cred_hex));

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

    /// Re-emit the holder ledger for tracked policies. Processes
    /// **one policy per call** so the host can refuel between
    /// cold-starts — a multi-policy scan overruns a single wasm
    /// call's fuel budget. Returns `1` when a policy was
    /// re-scanned, `0` when the round is complete; the host's
    /// recapture loop calls until `0`.
    ///
    /// `init` restores `TRACKED_POLICIES` from `state-kv`, so the
    /// module knows what to re-scan. `cold_start` re-scans
    /// `utxos_by_policy` and emits a fresh `Snapshot`, so this is
    /// idempotent — recapture may run it repeatedly.
    fn rebootstrap() -> Result<u64, String> {
        let next = REBOOTSTRAP_QUEUE.with(|q| {
            let mut q = q.borrow_mut();
            if q.is_none() {
                // Start of a round — snapshot the tracked set.
                *q = Some(
                    TRACKED_POLICIES.with(|s| s.borrow().iter().copied().collect()),
                );
            }
            q.as_mut().and_then(|pending| pending.pop())
        });
        match next {
            Some(policy) => {
                cold_start(&policy);
                Ok(1)
            }
            None => {
                // Round complete — reset for the next recapture.
                REBOOTSTRAP_QUEUE.with(|q| *q.borrow_mut() = None);
                Ok(0)
            }
        }
    }
}

export!(Module);
