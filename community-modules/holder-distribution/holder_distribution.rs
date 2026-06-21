//! Holder-distribution community module — emits a current holder
//! ledger per tracked policy and per-TX deltas as holdings change,
//! with DEX-LP and CrowdLock-vesting decomposition of the snapshot.
//!
//! ## Scalable, sharded architecture (SB5)
//!
//! Holdings are **sharded** to state-kv — one key per holder
//! (`ledger:<policy_hex>:<holder_key_hex>` → CBOR
//! [`LedgerEntry`]) — so no cold-start / recapture / live-tail call
//! ever serializes the whole ledger. Cold-start + recapture run
//! through the shared re-entrant [`ChunkedBootstrap`] driver
//! (`mitos-module-kit`): scan one page → SUM-merge each holder's
//! assets into its shard (batched read+write per page) → (decompose)
//! → emit one `SnapshotChunk` per call from the shards. Every
//! progress field is durable; nothing holder-count-sized is resident.
//! See `docs/design/HOLDER_DISTRIBUTION_DRIVER_MIGRATION.md`.
//!
//! ## Decomposition (the [`Phase::Decomp`] hook)
//!
//! Between scan and emit, [`HolderDistIo::decomp_step`] runs when a
//! tracked policy has a DEX pool and/or CrowdLock vesting:
//!
//! - **LP-pool**: a DEX liquidity pool holds an aggregate of the
//!   policy on behalf of LP providers. The pool is auto-discovered
//!   (a holder at `cswap::POOL_SCRIPT_ADDR`); its datum gives the
//!   LP-token policy + total supply. A paged scan of the LP-token
//!   holder set (with CSwap farm staking-datum resolution) yields
//!   each provider's share; the pool's aggregate is redistributed
//!   proportionally, the rounding remainder kept as a residual pool
//!   entry.
//! - **Vesting**: CrowdLock lock UTxOs bypass the holder ledger;
//!   their locked tokens are attributed to owners as `vests`.
//!
//! The decomposed set is materialised into a second `decomp:` shard
//! prefix (paged — bounded at scale), which the emit then pages. A
//! policy with neither pool nor vesting takes the fast path: decomp
//! is a no-op and the emit pages the raw `ledger:` shards directly.
//!
//! ## Address → holder identity
//!
//! `pallas-addresses` parses each output's bech32. Shelley addresses
//! with a Key or Script delegation part become `HolderId::Stake`
//! (grouped by the 28-byte staking credential); enterprise addresses
//! become `HolderId::Enterprise`. Pointer-stake (~1%) and Byron are
//! dropped.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};

use mitos_community_events::holder_distribution::{
    AssetBalance, HolderDelta, HolderEntry, HolderEvent, HolderId, HolderRole, SnapshotBegin,
    SnapshotChunk, SnapshotEnd,
};
use mitos_community_events::vesting_tracker::{LockEntry, LockRef, VestStyle};
use mitos_dex_decode::{cswap, lp_share, splash};
use mitos_module_kit::{BootstrapCursor, BootstrapIo, ChunkedBootstrap, DecompStep, ScannedPage};
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

/// state-kv key: CBOR list of 56-char hex policy ids currently tracked.
const KV_TRACKED_POLICIES: &str = "tracked-policies";

/// Sharded raw-ledger prefix: `ledger:<policy_hex>:<holder_key_hex>`
/// → CBOR [`LedgerEntry`] (raw SUM; `lp_amount`/`vests` empty).
const KV_LEDGER_PREFIX: &str = "ledger:";

/// Decomposed-snapshot prefix: `decomp:<policy_hex>:<holder_key_hex>`
/// → CBOR [`LedgerEntry`] (LP shares + vests attributed). Written by
/// `decomp_step`, paged by the emit, cleared at `emit_end`.
const KV_DECOMP_PREFIX: &str = "decomp:";

/// Per-predicate scan side-state: `meta:<policy_hex>` → CBOR
/// [`MetaState`] (auto-discovered pool ref + vesting lock refs +
/// decomposed flag). Written during scan, read by decomp + emit,
/// cleared at `emit_end`.
const KV_META_PREFIX: &str = "meta:";

/// Re-entrant `rebootstrap` continuation cursor (kit-encoded).
const KV_REBOOTSTRAP_CURSOR: &str = "rebootstrap-cursor";

/// Scope for the next `rebootstrap` pump — the policies a
/// subscribe-time Add brought in (per-policy auto-onboard). Absent ⇒
/// full recapture over all tracked policies. ciborium `Vec<Vec<u8>>`.
const KV_ONBOARD_PREDICATES: &str = "onboard-predicates";

/// Holders per `SnapshotChunk`.
const SNAPSHOT_CHUNK_HOLDERS: usize = 1_000;

/// Per-page hint for the cold-start scan (host clamps to its budget).
const COLD_START_PAGE_HINT: u32 = 10_000;

/// 28-byte hash size (stake creds, policy ids, payment hashes).
const HASH_BYTES: usize = 28;

thread_local! {
    /// Currently-tracked policy ids. Persisted via
    /// `KV_TRACKED_POLICIES`, restored on init.
    static TRACKED_POLICIES: RefCell<HashSet<[u8; HASH_BYTES]>> = RefCell::new(HashSet::new());

    /// In-flight re-entrant `rebootstrap` driver. Holds no resident
    /// ledger — holdings shard to state-kv per page. Rebuilt from the
    /// durable cursor after a trap. See [`ChunkedBootstrap`].
    static REBOOTSTRAP_DRIVER: RefCell<Option<ChunkedBootstrap>> = const { RefCell::new(None) };

    /// In-flight decomposition state for the predicate currently in
    /// [`Phase::Decomp`]. Resident (LP-provider + vest-owner counts
    /// are bounded; the holder-count-sized materialise is *paged*).
    /// Lost on a trap → re-derived from the durable `meta:` +
    /// `ledger:` shards (decomp restarts for the predicate, which
    /// re-clears `decomp:` first — idempotent).
    static DECOMP_STATE: RefCell<Option<DecompState>> = const { RefCell::new(None) };
}

// ============================================================
// Holder identity
// ============================================================

/// In-memory ledger key + wire identity (leaner internal form).
/// Stake holders keyed by 28-byte cred; enterprise by full bech32.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
enum LedgerKey {
    Stake([u8; HASH_BYTES]),
    Enterprise(String),
}

/// `asset_name_hex_bytes -> quantity` for one (policy, holder).
type AssetMap = BTreeMap<Vec<u8>, u64>;

/// One sharded holder entry — the kit driver's `Entry`. In `ledger:`
/// shards `lp_amount`/`vests` are empty (raw SUM); in `decomp:` shards
/// they carry the LP attribution + vests. `holder` rides along so the
/// emit can render the wire `HolderEntry` from a shard alone.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LedgerEntry {
    holder: LedgerKey,
    assets: AssetMap,
    #[serde(default)]
    lp_amount: u64,
    #[serde(default)]
    vests: Vec<LockEntry>,
}

/// `meta:<policy_hex>` side-state, written during the scan.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct MetaState {
    /// First-spotted DEX pool UTxO holding the policy, if any.
    pool_ref: Option<OutputRefCbor>,
    /// CrowdLock vesting-lock UTxO refs (bypass the ledger).
    vesting_lock_refs: Vec<OutputRefCbor>,
    /// `true` once decomp materialised `decomp:` shards (so emit pages
    /// `decomp:`); `false`/absent ⇒ emit pages `ledger:` (fast path).
    #[serde(default)]
    decomposed: bool,
}

/// Serializable mirror of the WIT `OutputRef` (the WIT type isn't
/// `Serialize`).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OutputRefCbor {
    tx_hash: Vec<u8>,
    index: u32,
}

impl OutputRefCbor {
    fn from_wit(r: &WitOutputRef) -> Self {
        Self {
            tx_hash: r.tx_hash.clone(),
            index: r.index,
        }
    }
    fn to_wit(&self) -> WitOutputRef {
        WitOutputRef {
            tx_hash: self.tx_hash.clone(),
            index: self.index,
        }
    }
}

fn key_to_id(key: &LedgerKey) -> HolderId {
    match key {
        LedgerKey::Stake(bytes) => HolderId::Stake(hex::encode(bytes)),
        LedgerKey::Enterprise(addr) => HolderId::Enterprise(addr.clone()),
    }
}

/// Parse a bech32 address into a `LedgerKey` (Stake for Key/Script
/// delegation, Enterprise for Null delegation). Pointer + Byron → None.
fn extract_holder_key(address: &str) -> Option<LedgerKey> {
    let addr = Address::from_bech32(address).ok()?;
    let shelley = match addr {
        Address::Shelley(s) => s,
        _ => return None,
    };
    match shelley.delegation() {
        ShelleyDelegationPart::Key(h) => Some(LedgerKey::Stake((**h).into())),
        ShelleyDelegationPart::Script(h) => Some(LedgerKey::Stake((**h).into())),
        ShelleyDelegationPart::Null => Some(LedgerKey::Enterprise(address.to_string())),
        _ => None,
    }
}

/// Role tag from what the module recognises (DEX pool scripts from the
/// shared `mitos-dex-decode` constants); cached on first call.
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

/// 28-byte payment credential of a bech32 address.
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

/// True when `address` is a CrowdLock vesting lock.
fn is_crowdlock_lock(address: &str) -> bool {
    payment_cred_bytes(address)
        .map(|c| crowd_lock::is_crowd_lock(&c))
        .unwrap_or(false)
}

fn assets_map_to_vec(map: &AssetMap) -> Vec<AssetBalance> {
    let mut out: Vec<AssetBalance> = map
        .iter()
        .map(|(name, qty)| AssetBalance {
            asset_name_hex: hex::encode(name),
            quantity: *qty,
        })
        .collect();
    out.sort_by(|a, b| a.asset_name_hex.cmp(&b.asset_name_hex));
    out
}

/// Render a sharded [`LedgerEntry`] as the wire `HolderEntry`.
fn entry_to_wire(entry: &LedgerEntry) -> HolderEntry {
    HolderEntry {
        id: key_to_id(&entry.holder),
        assets: assets_map_to_vec(&entry.assets),
        lp_amount: entry.lp_amount,
        role: holder_role_for(&entry.holder),
        vests: entry.vests.clone(),
    }
}

// ============================================================
// Shard helpers
// ============================================================

/// Opaque per-holder shard key: CBOR-encoded `LedgerKey`.
fn holder_entry_key(holder: &LedgerKey) -> Vec<u8> {
    let mut buf = Vec::with_capacity(40);
    let _ = ciborium::ser::into_writer(holder, &mut buf);
    buf
}

fn ledger_prefix(policy_hex: &str) -> String {
    format!("{KV_LEDGER_PREFIX}{policy_hex}:")
}
fn ledger_kv_key(policy_hex: &str, entry_key: &[u8]) -> String {
    format!("{KV_LEDGER_PREFIX}{policy_hex}:{}", hex::encode(entry_key))
}
fn decomp_prefix(policy_hex: &str) -> String {
    format!("{KV_DECOMP_PREFIX}{policy_hex}:")
}
fn decomp_kv_key(policy_hex: &str, entry_key: &[u8]) -> String {
    format!("{KV_DECOMP_PREFIX}{policy_hex}:{}", hex::encode(entry_key))
}
fn meta_kv_key(policy_hex: &str) -> String {
    format!("{KV_META_PREFIX}{policy_hex}")
}

fn read_meta(policy_hex: &str) -> MetaState {
    state_kv::get_value(&meta_kv_key(policy_hex))
        .and_then(|b| ciborium::de::from_reader(b.as_slice()).ok())
        .unwrap_or_default()
}
fn write_meta(policy_hex: &str, meta: &MetaState) {
    let mut buf = Vec::with_capacity(128);
    if ciborium::ser::into_writer(meta, &mut buf).is_ok() {
        state_kv::set_value(&meta_kv_key(policy_hex), &buf);
    }
}

fn read_ledger_entry(policy_hex: &str, entry_key: &[u8]) -> Option<LedgerEntry> {
    let bytes = state_kv::get_value(&ledger_kv_key(policy_hex, entry_key))?;
    ciborium::de::from_reader(bytes.as_slice()).ok()
}
fn write_ledger_entry(policy_hex: &str, entry_key: &[u8], entry: &LedgerEntry) {
    let mut buf = Vec::with_capacity(128);
    if ciborium::ser::into_writer(entry, &mut buf).is_ok() {
        state_kv::set_value(&ledger_kv_key(policy_hex, entry_key), &buf);
    }
}
fn delete_ledger_entry(policy_hex: &str, entry_key: &[u8]) {
    state_kv::delete_value(&ledger_kv_key(policy_hex, entry_key));
}

/// Drop all per-policy state-kv (ledger + decomp + meta) on un-track.
fn clear_policy_state(policy_hex: &str) {
    state_kv::delete_prefix(&ledger_prefix(policy_hex));
    state_kv::delete_prefix(&decomp_prefix(policy_hex));
    state_kv::delete_value(&meta_kv_key(policy_hex));
}

// ============================================================
// Ledger folding (scan + delta)
// ============================================================

fn ledger_add(map: &mut AssetMap, asset_name: &[u8], qty: u64) {
    let total = map.entry(asset_name.to_vec()).or_insert(0);
    *total = total.saturating_add(qty);
}

// ============================================================
// LP-pool decomposition helpers
// ============================================================

fn resolve_datum_bytes(d: &TypedDatum) -> Option<Vec<u8>> {
    if !d.payload.is_empty() {
        return Some(d.payload.clone());
    }
    chain_data::datum_by_hash(&d.hash)
}

fn read_pool_datum(pool_ref: &WitOutputRef) -> Option<cswap::CswapPoolDatum> {
    let datums = chain_data::read_output_datums(std::slice::from_ref(pool_ref));
    let cbor = datums
        .into_iter()
        .next()
        .flatten()
        .and_then(|d| resolve_datum_bytes(&d))?;
    cswap::decode_pool_datum(&cbor)
}

/// Resolve one page of an LP-token policy scan into per-holder LP qty.
/// Staked LP at the CSwap farm is attributed to the staker via the
/// farm UTxO's staking datum.
fn fold_lp_page(
    lp_holders: &mut BTreeMap<LedgerKey, u64>,
    lp_policy: &[u8; HASH_BYTES],
    refs: &[WitOutputRef],
) {
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

// ============================================================
// Vesting decomposition
// ============================================================

/// Read each CrowdLock lock UTxO + datum, resolve the owner's stake
/// credential, group per-owner `LockEntry`s. Bounded by lock count.
fn decompose_vesting(
    policy: &[u8; HASH_BYTES],
    lock_refs: &[WitOutputRef],
) -> BTreeMap<LedgerKey, Vec<LockEntry>> {
    if lock_refs.is_empty() {
        return BTreeMap::new();
    }
    let utxos = chain_data::read_utxos(lock_refs);
    let datums = chain_data::read_output_datums(lock_refs);
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
        let Ok(owner_hash) = <[u8; HASH_BYTES]>::try_from(owner_bytes.as_slice()) else {
            continue;
        };
        let owner_key = LedgerKey::Stake(owner_hash);
        let owner_stake_cred_hex = Some(hex::encode(&owner_bytes));

        for asset in &out_.assets {
            if asset.asset.policy != policy {
                continue;
            }
            out.entry(owner_key.clone()).or_default().push(LockEntry {
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
                vest_style: VestStyle::CrowdLock,
                locked_at_tx: hex::encode(&lock_ref.tx_hash),
            });
        }
    }
    out
}

// ============================================================
// Decomposition state machine (Phase::Decomp)
// ============================================================

/// Sub-phase of an in-flight decomposition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecompSub {
    /// Paging the LP-token holder set into `lp_holders`.
    LpScan,
    /// Paging the raw `ledger:` shards → `decomp:` shards.
    MaterializeLedger,
    /// Writing the tail: LP-only / vest-only owners + pool residual.
    MaterializeTail,
}

/// Resident decomposition working set for one predicate. Bounded
/// parts (LP providers, vest owners) are held in memory; the
/// holder-count-sized materialise pages the `ledger:` shards. Lost on
/// a trap → re-derived from the durable `meta:` + `ledger:` shards.
struct DecompState {
    policy: [u8; HASH_BYTES],
    policy_hex: String,
    sub: DecompSub,
    pool_key: Option<LedgerKey>,
    /// The pool's raw aggregate (its `ledger:` AssetMap), captured
    /// before redistribution.
    pool_reserve: AssetMap,
    total_lp_tokens: u64,
    lp_policy: Option<[u8; HASH_BYTES]>,
    lp_after: Option<Vec<u8>>,
    lp_holders: BTreeMap<LedgerKey, u64>,
    /// Per-holder LP-derived asset amounts (computed once after the LP
    /// scan); `lp_amount` = sum across assets.
    lp_shares: BTreeMap<LedgerKey, AssetMap>,
    /// Rounding remainder kept as the pool residual.
    residual: AssetMap,
    vests: BTreeMap<LedgerKey, Vec<LockEntry>>,
    /// Key cursor into the `ledger:` shards for the materialise pass.
    mat_after: Option<Vec<u8>>,
    /// LP / vest owners already emitted via the ledger pass (so the
    /// tail emits only those with no liquid holdings).
    visited: HashSet<LedgerKey>,
}

/// Marker the kit threads through `BootstrapCursor.decomp_cursor` — its
/// only role is "decomp in progress" (the real state is resident +
/// restart-on-trap). One byte keeps the cursor non-empty.
const DECOMP_MARKER: &[u8] = b"d";

/// Compute the LP redistribution plan once the LP scan completes:
/// each provider's per-asset share of the pool reserve, and the
/// residual remainder.
fn compute_lp_plan(st: &mut DecompState) {
    let mut residual = st.pool_reserve.clone();
    let mut shares: BTreeMap<LedgerKey, AssetMap> = BTreeMap::new();
    for (lp_key, lp_qty) in &st.lp_holders {
        let entry = shares.entry(lp_key.clone()).or_default();
        for (name, reserve) in &st.pool_reserve {
            let share = lp_share(*lp_qty, *reserve, st.total_lp_tokens);
            if share == 0 {
                continue;
            }
            *entry.entry(name.clone()).or_insert(0) += share;
            if let Some(rem) = residual.get_mut(name) {
                *rem = rem.saturating_sub(share);
            }
        }
    }
    st.lp_shares = shares;
    st.residual = residual.into_iter().filter(|(_, q)| *q > 0).collect();
}

/// Write one decomposed holder shard, folding in LP shares + vests for
/// `holder` on top of `base` assets. Marks the holder visited.
fn write_decomp_holder(st: &mut DecompState, holder: &LedgerKey, base: AssetMap) {
    let mut assets = base;
    let mut lp_amount = 0u64;
    if let Some(share) = st.lp_shares.get(holder) {
        for (name, qty) in share {
            ledger_add(&mut assets, name, *qty);
            lp_amount = lp_amount.saturating_add(*qty);
        }
    }
    let vests = st.vests.get(holder).cloned().unwrap_or_default();
    st.visited.insert(holder.clone());
    let entry = LedgerEntry {
        holder: holder.clone(),
        assets,
        lp_amount,
        vests,
    };
    let entry_key = holder_entry_key(holder);
    let mut buf = Vec::with_capacity(128);
    if ciborium::ser::into_writer(&entry, &mut buf).is_ok() {
        state_kv::set_value(&decomp_kv_key(&st.policy_hex, &entry_key), &buf);
    }
}

// ============================================================
// Wire-side interest predicate mirror
// ============================================================

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
                <[u8; HASH_BYTES]>::try_from(bytes.as_slice()).ok()
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

    for policy in &removed {
        let policy_hex = hex::encode(policy);
        clear_policy_state(&policy_hex);
        logging::log(
            LogLevel::Info,
            LOG_TARGET,
            &format!("dropped state for untracked policy {policy_hex}"),
        );
    }
    // Evict removed policies from any pending onboard scope so a
    // dropped orphan can't be re-scanned by the next onboard pump.
    drop_from_onboard_scope(&removed);

    if !added.is_empty() {
        seed_onboard_scope(&added);
    }
}

/// Record `added` policies as the scope for the next onboard
/// `rebootstrap` pump. REPLACES (not merges) the pending scope — see
/// the matching note in collection-holders: onboards run sequentially,
/// so replacing prevents a never-completing policy from wedging the
/// scope and being re-scanned on every future add.
fn seed_onboard_scope(added: &[[u8; HASH_BYTES]]) {
    let mut scope: Vec<Vec<u8>> = added.iter().map(|p| p.to_vec()).collect();
    scope.sort_unstable();
    let mut buf = Vec::with_capacity(8 + scope.len() * (HASH_BYTES + 2));
    if ciborium::ser::into_writer(&scope, &mut buf).is_ok() {
        state_kv::set_value(KV_ONBOARD_PREDICATES, &buf);
    }
    state_kv::delete_value(KV_REBOOTSTRAP_CURSOR);
    REBOOTSTRAP_DRIVER.with(|c| *c.borrow_mut() = None);
    DECOMP_STATE.with(|c| *c.borrow_mut() = None);
}

/// Drop `removed` policies from any pending onboard scope (on
/// Remove/Replace), so an untracked orphan can't linger and be
/// re-scanned. Deletes the key once empty.
fn drop_from_onboard_scope(removed: &[[u8; HASH_BYTES]]) {
    if removed.is_empty() {
        return;
    }
    let Some(mut scope) = read_onboard_scope() else {
        return;
    };
    let before = scope.len();
    scope.retain(|p| !removed.iter().any(|r| r.as_slice() == p.as_slice()));
    if scope.len() == before {
        return;
    }
    if scope.is_empty() {
        state_kv::delete_value(KV_ONBOARD_PREDICATES);
    } else {
        let mut buf = Vec::with_capacity(8 + scope.len() * (HASH_BYTES + 2));
        if ciborium::ser::into_writer(&scope, &mut buf).is_ok() {
            state_kv::set_value(KV_ONBOARD_PREDICATES, &buf);
        }
    }
}

fn read_onboard_scope() -> Option<Vec<Vec<u8>>> {
    let bytes = state_kv::get_value(KV_ONBOARD_PREDICATES)?;
    ciborium::de::from_reader(&bytes[..]).ok()
}

/// Scope for an `Onboard` rebootstrap pump: the subscribe-seeded scope
/// PLUS any tracked policy that has no persisted ledger state yet (i.e.
/// was never scanned).
///
/// The self-heal term makes onboarding *converge* even when the explicit
/// seed was missed — e.g. a host-restart `Replace` re-asserts a policy
/// into `TRACKED_POLICIES` without re-seeding the onboard scope (so the
/// seed-on-`added` path is a no-op), or a first seed/pump that ingested
/// nothing leaves a tracked-but-empty policy. Without it such a policy is
/// stranded until the next *whole-module* recapture (the symptom that left
/// freshly-onboarded ad-hoc tokens showing no holders).
///
/// Bounded: a fully-onboarded module has ledger state for every tracked
/// policy, so the self-heal term is empty and an empty onboard scope stays
/// a no-op — never a full re-scan of every policy (that remains
/// recapture's job, `RebootstrapMode::Full`).
fn onboard_scope_with_self_heal() -> Vec<Vec<u8>> {
    let mut scope = read_onboard_scope().unwrap_or_default();
    let seeded: HashSet<Vec<u8>> = scope.iter().cloned().collect();
    let unscanned: Vec<Vec<u8>> = TRACKED_POLICIES.with(|set| {
        set.borrow()
            .iter()
            .map(|p| p.to_vec())
            .filter(|p| !seeded.contains(p))
            .filter(|p| !policy_has_state(&hex::encode(p)))
            .collect()
    });
    scope.extend(unscanned);
    scope
}

/// True when the policy has at least one persisted `ledger:` shard — the
/// reliable "has been scanned at least once" signal (the `meta:` shard is
/// only written for pool/vesting policies, so it can't be used here).
/// Cheap single-row prefix probe.
fn policy_has_state(policy_hex: &str) -> bool {
    state_kv::kv_scan(&ledger_prefix(policy_hex), None, 1)
        .into_iter()
        .next()
        .is_some()
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
        Err(_) => return,
    };
    TRACKED_POLICIES.with(|set| {
        let mut set = set.borrow_mut();
        for hex_str in policies {
            if let Ok(bytes) = hex::decode(&hex_str) {
                if let Ok(arr) = <[u8; HASH_BYTES]>::try_from(bytes.as_slice()) {
                    set.insert(arr);
                }
            }
        }
    });
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
    // Partition key = policy hex bytes (same rationale as
    // collection-holders: per-policy lane ordering + per-companion
    // interest routing). See DRIVER_MIGRATION §5.
    emit::emit_event_keyed(0, event_policy(event).as_bytes(), &buf);
}

fn event_policy(event: &HolderEvent) -> &str {
    match event {
        HolderEvent::SnapshotBegin(b) => &b.policy,
        HolderEvent::SnapshotChunk(c) => &c.policy,
        HolderEvent::SnapshotEnd(e) => &e.policy,
        HolderEvent::Delta(d) => &d.policy,
    }
}

// ============================================================
// Per-TX deltas (sharded)
// ============================================================

#[derive(Default)]
struct TxBuffer {
    tx_hash: Option<Vec<u8>>,
    slot: u64,
    produced: HashMap<[u8; HASH_BYTES], Vec<ProducedEvent>>,
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
        for policy in set.borrow().iter() {
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
        for policy in set.borrow().iter() {
            if touches_policy(&c.prior_output.assets, policy) {
                buf.consumed.entry(*policy).or_default().push(c.clone());
            }
        }
    });
}

/// Apply produced+consumed to the sharded ledger for each touched
/// policy + emit a `Delta` of the changed holders' post-TX balances.
/// CrowdLock locks bypass the ledger (matching the snapshot). Deltas
/// carry RAW balances (lp/vest attribution is a snapshot-time
/// transform, recomputed at the next recapture).
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
        // Per-(holder) net delta of this policy's assets across the TX.
        let mut deltas: BTreeMap<LedgerKey, BTreeMap<Vec<u8>, i64>> = BTreeMap::new();

        if let Some(events) = buf.consumed.get(&policy) {
            for c in events {
                if is_crowdlock_lock(&c.prior_output.address) {
                    continue;
                }
                let Some(key) = extract_holder_key(&c.prior_output.address) else {
                    continue;
                };
                for asset in &c.prior_output.assets {
                    if asset.asset.policy != policy {
                        continue;
                    }
                    *deltas
                        .entry(key.clone())
                        .or_default()
                        .entry(asset.asset.name.clone())
                        .or_insert(0) -= asset.quantity as i64;
                }
            }
        }
        if let Some(events) = buf.produced.get(&policy) {
            for p in events {
                if is_crowdlock_lock(&p.output.address) {
                    continue;
                }
                let Some(key) = extract_holder_key(&p.output.address) else {
                    continue;
                };
                for asset in &p.output.assets {
                    if asset.asset.policy != policy {
                        continue;
                    }
                    *deltas
                        .entry(key.clone())
                        .or_default()
                        .entry(asset.asset.name.clone())
                        .or_insert(0) += asset.quantity as i64;
                }
            }
        }

        let mut changed: Vec<HolderEntry> = Vec::new();
        for (key, per_asset) in deltas {
            let entry_key = holder_entry_key(&key);
            let mut entry = read_ledger_entry(&policy_hex, &entry_key).unwrap_or(LedgerEntry {
                holder: key.clone(),
                assets: AssetMap::new(),
                lp_amount: 0,
                vests: Vec::new(),
            });
            let mut touched = false;
            for (name, delta) in per_asset {
                if delta == 0 {
                    continue;
                }
                touched = true;
                let prev = *entry.assets.get(&name).unwrap_or(&0) as i64;
                let new = prev + delta;
                if new <= 0 {
                    entry.assets.remove(&name);
                } else {
                    entry.assets.insert(name, new as u64);
                }
            }
            if !touched {
                continue;
            }
            if entry.assets.is_empty() {
                delete_ledger_entry(&policy_hex, &entry_key);
            } else {
                write_ledger_entry(&policy_hex, &entry_key, &entry);
            }
            // Report the post-TX balance (empty assets if zeroed out).
            changed.push(HolderEntry {
                id: key_to_id(&key),
                assets: assets_map_to_vec(&entry.assets),
                lp_amount: 0,
                role: holder_role_for(&key),
                vests: Vec::new(),
            });
        }

        if !changed.is_empty() {
            changed.sort_by(|a, b| a.id.cmp(&b.id));
            emit_event(&HolderEvent::Delta(HolderDelta {
                policy: policy_hex,
                tx_hash: tx_hash_hex.clone(),
                slot,
                changed,
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
                _ => {}
            }
        }
        flush_buffer(buf);
    }

    fn update_interest(op: InterestOp, items_cbor: Vec<u8>) -> Result<(), String> {
        apply_interest_update(op, &items_cbor);
        Ok(())
    }

    /// Re-emit the holder snapshot for tracked policies via the shared
    /// re-entrant [`ChunkedBootstrap`] driver: scan→shard (SUM) →
    /// decompose (LP + vesting) → emit, one bounded unit per call.
    fn rebootstrap(mode: RebootstrapMode) -> Result<RebootstrapStep, String> {
        REBOOTSTRAP_DRIVER.with(|cell| {
            let mut slot = cell.borrow_mut();
            if slot.is_none() {
                // Explicit mode (no implicit scope-or-full inference):
                // Onboard scans only the seeded onboard scope; Full
                // re-scans every tracked policy (recapture).
                let mut predicates: Vec<Vec<u8>> = match mode {
                    RebootstrapMode::Onboard => onboard_scope_with_self_heal(),
                    RebootstrapMode::Full => {
                        TRACKED_POLICIES.with(|s| s.borrow().iter().map(|p| p.to_vec()).collect())
                    }
                };
                // An empty Onboard scope is a no-op — never a full scan.
                if predicates.is_empty() {
                    return Ok(RebootstrapStep {
                        done: true,
                        ingested: 0,
                    });
                }
                predicates.sort_unstable();
                let cursor = state_kv::get_value(KV_REBOOTSTRAP_CURSOR)
                    .and_then(|b| BootstrapCursor::decode(&b));
                *slot = Some(ChunkedBootstrap::resume(predicates, cursor));
            }
            let driver = slot.as_mut().expect("driver initialised above");
            let mut io = HolderDistIo;
            let out = driver.step(&mut io);
            if out.done {
                *slot = None;
                if matches!(mode, RebootstrapMode::Onboard) {
                    state_kv::delete_value(KV_ONBOARD_PREDICATES);
                }
            }
            Ok(RebootstrapStep {
                done: out.done,
                ingested: out.ingested,
            })
        })
    }
}

/// `BootstrapIo` adapter. SUM semantics for the raw ledger; the
/// `Phase::Decomp` hook runs LP + vesting decomposition.
struct HolderDistIo;

fn policy_arr(predicate: &[u8]) -> [u8; HASH_BYTES] {
    let mut a = [0u8; HASH_BYTES];
    a.copy_from_slice(predicate);
    a
}

impl HolderDistIo {
    /// (Re-)derive the decomposition working set from the durable
    /// `meta:` + `ledger:` shards. Called on the first `decomp_step`
    /// for a predicate and after a trap (resident state lost). Returns
    /// `None` when there's nothing to decompose (no pool, no vesting).
    fn init_decomp(&self, predicate: &[u8], anchor_slot: u64) -> Option<DecompState> {
        let _ = anchor_slot;
        let policy = policy_arr(predicate);
        let policy_hex = hex::encode(predicate);
        let meta = read_meta(&policy_hex);
        if meta.pool_ref.is_none() && meta.vesting_lock_refs.is_empty() {
            return None;
        }
        // Fresh decomp shards (idempotent restart after a trap).
        state_kv::delete_prefix(&decomp_prefix(&policy_hex));

        let vests = decompose_vesting(
            &policy,
            &meta
                .vesting_lock_refs
                .iter()
                .map(|r| r.to_wit())
                .collect::<Vec<_>>(),
        );

        // Pool plan, if a pool was discovered + its datum decodes.
        let mut pool_key = None;
        let mut pool_reserve = AssetMap::new();
        let mut total_lp_tokens = 0u64;
        let mut lp_policy = None;
        if let Some(pref) = &meta.pool_ref {
            if let Some(datum) = read_pool_datum(&pref.to_wit()) {
                if let Ok(lpp) = <[u8; HASH_BYTES]>::try_from(datum.lp_policy.as_slice()) {
                    let pk = extract_holder_key(cswap::POOL_SCRIPT_ADDR);
                    let reserve = pk
                        .as_ref()
                        .and_then(|k| read_ledger_entry(&policy_hex, &holder_entry_key(k)))
                        .map(|e| e.assets)
                        .unwrap_or_default();
                    pool_key = pk;
                    pool_reserve = reserve;
                    total_lp_tokens = datum.total_lp_tokens;
                    lp_policy = Some(lpp);
                } else {
                    logging::log(
                        LogLevel::Warn,
                        LOG_TARGET,
                        &format!("decomp policy={policy_hex}: pool datum lp_policy not 28 bytes — skipping LP"),
                    );
                }
            } else {
                logging::log(
                    LogLevel::Warn,
                    LOG_TARGET,
                    &format!("decomp policy={policy_hex}: pool datum did not decode — skipping LP"),
                );
            }
        }

        let sub = if lp_policy.is_some() {
            DecompSub::LpScan
        } else {
            // Vesting-only (or pool-without-decodable-datum): no LP
            // shares, straight to materialise.
            DecompSub::MaterializeLedger
        };

        Some(DecompState {
            policy,
            policy_hex,
            sub,
            pool_key,
            pool_reserve,
            total_lp_tokens,
            lp_policy,
            lp_after: None,
            lp_holders: BTreeMap::new(),
            lp_shares: BTreeMap::new(),
            residual: AssetMap::new(),
            vests,
            mat_after: None,
            visited: HashSet::new(),
        })
    }
}

impl BootstrapIo for HolderDistIo {
    type Entry = LedgerEntry;

    fn scan_page(&mut self, predicate: &[u8], after: Option<&[u8]>) -> ScannedPage<LedgerEntry> {
        let policy = policy_arr(predicate);
        let policy_hex = hex::encode(predicate);
        let page = chain_data::utxos_by_policy(&policy, after, COLD_START_PAGE_HINT);
        let ingested = page.refs.len() as u64;

        // Fold the page into per-holder AssetMaps, collecting pool +
        // vesting side-state into the meta shard. CrowdLock locks
        // bypass the ledger (their tokens become owners' vests).
        let mut per_holder: BTreeMap<LedgerKey, AssetMap> = BTreeMap::new();
        let mut meta = read_meta(&policy_hex);
        let mut meta_dirty = false;
        for (oref, out) in chain_data::read_utxos(&page.refs) {
            if is_crowdlock_lock(&out.address) {
                meta.vesting_lock_refs.push(OutputRefCbor::from_wit(&oref));
                meta_dirty = true;
                continue;
            }
            if meta.pool_ref.is_none() && out.address == cswap::POOL_SCRIPT_ADDR {
                meta.pool_ref = Some(OutputRefCbor::from_wit(&oref));
                meta_dirty = true;
            }
            let Some(key) = extract_holder_key(&out.address) else {
                continue;
            };
            let map = per_holder.entry(key).or_default();
            for asset in &out.assets {
                if asset.asset.policy != policy {
                    continue;
                }
                ledger_add(map, &asset.asset.name, asset.quantity);
            }
        }
        if meta_dirty {
            write_meta(&policy_hex, &meta);
        }

        let entries: Vec<(Vec<u8>, LedgerEntry)> = per_holder
            .into_iter()
            .map(|(holder, assets)| {
                (
                    holder_entry_key(&holder),
                    LedgerEntry {
                        holder,
                        assets,
                        lp_amount: 0,
                        vests: Vec::new(),
                    },
                )
            })
            .collect();
        ScannedPage {
            entries,
            next: page.next,
            anchor_slot: page.anchor_slot,
            ingested,
        }
    }

    fn shard_get_many(&self, predicate: &[u8], keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
        let policy_hex = hex::encode(predicate);
        let kv_keys: Vec<String> = keys.iter().map(|k| ledger_kv_key(&policy_hex, k)).collect();
        state_kv::get_many(&kv_keys)
    }

    fn shard_put_many(&mut self, predicate: &[u8], entries: &[(Vec<u8>, Vec<u8>)]) {
        let policy_hex = hex::encode(predicate);
        let kv_entries: Vec<(String, Vec<u8>)> = entries
            .iter()
            .map(|(k, v)| (ledger_kv_key(&policy_hex, k), v.clone()))
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
        // Emit pages `decomp:` when the predicate decomposed, else the
        // raw `ledger:` shards (NFT / no-pool-no-vesting fast path).
        let decomposed = read_meta(&policy_hex).decomposed;
        let (prefix, mk): (String, fn(&str, &[u8]) -> String) = if decomposed {
            (decomp_prefix(&policy_hex), decomp_kv_key)
        } else {
            (ledger_prefix(&policy_hex), ledger_kv_key)
        };
        let after_key = after.map(|a| mk(&policy_hex, a));
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
        let policy_hex = hex::encode(predicate);
        // Fresh scan: clear raw ledger + any stale decomp/meta side-state.
        state_kv::delete_prefix(&ledger_prefix(&policy_hex));
        state_kv::delete_prefix(&decomp_prefix(&policy_hex));
        state_kv::delete_value(&meta_kv_key(&policy_hex));
        DECOMP_STATE.with(|c| *c.borrow_mut() = None);
    }

    fn encode_entry(&self, entry: &LedgerEntry) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);
        let _ = ciborium::ser::into_writer(entry, &mut buf);
        buf
    }
    fn decode_entry(&self, bytes: &[u8]) -> Option<LedgerEntry> {
        ciborium::de::from_reader(bytes).ok()
    }

    fn accumulates(&self) -> bool {
        true
    }
    fn merge(&self, prior: Option<LedgerEntry>, incoming: LedgerEntry) -> LedgerEntry {
        match prior {
            Some(mut p) => {
                for (name, qty) in incoming.assets {
                    let total = p.assets.entry(name).or_insert(0);
                    *total = total.saturating_add(qty);
                }
                // lp_amount/vests are decomp-time only; both raw during scan.
                p
            }
            None => incoming,
        }
    }

    fn decomp_step(
        &mut self,
        predicate: &[u8],
        anchor_slot: u64,
        _cursor: Option<&[u8]>,
    ) -> DecompStep {
        DECOMP_STATE.with(|cell| {
            let mut slot = cell.borrow_mut();
            if slot.is_none() {
                match self.init_decomp(predicate, anchor_slot) {
                    Some(st) => {
                        // Mark decomposed so the emit pages `decomp:`.
                        let mut meta = read_meta(&st.policy_hex);
                        meta.decomposed = true;
                        write_meta(&st.policy_hex, &meta);
                        *slot = Some(st);
                    }
                    None => {
                        // Nothing to decompose — emit pages raw ledger.
                        return DecompStep {
                            done: true,
                            ingested: 0,
                            next_cursor: None,
                        };
                    }
                }
            }
            let st = slot.as_mut().expect("decomp state present");

            match st.sub {
                DecompSub::LpScan => {
                    let lp_policy = st.lp_policy.expect("lp_policy set in LpScan");
                    let page = chain_data::utxos_by_policy(
                        &lp_policy,
                        st.lp_after.as_deref(),
                        COLD_START_PAGE_HINT,
                    );
                    let ingested = page.refs.len() as u64;
                    fold_lp_page(&mut st.lp_holders, &lp_policy, &page.refs);
                    match page.next {
                        Some(token) => {
                            st.lp_after = Some(token);
                        }
                        None => {
                            compute_lp_plan(st);
                            st.sub = DecompSub::MaterializeLedger;
                        }
                    }
                    DecompStep {
                        done: false,
                        ingested,
                        next_cursor: Some(DECOMP_MARKER.to_vec()),
                    }
                }
                DecompSub::MaterializeLedger => {
                    // Page the raw ledger shards → decomp shards, folding
                    // in LP shares + vests. The pool entry is skipped
                    // here; its residual is written in the tail.
                    let policy_hex = st.policy_hex.clone();
                    let prefix = ledger_prefix(&policy_hex);
                    let after_key = st.mat_after.as_ref().map(|a| ledger_kv_key(&policy_hex, a));
                    let rows = state_kv::kv_scan(
                        &prefix,
                        after_key.as_deref(),
                        SNAPSHOT_CHUNK_HOLDERS as u32,
                    );
                    if rows.is_empty() {
                        st.sub = DecompSub::MaterializeTail;
                        return DecompStep {
                            done: false,
                            ingested: 0,
                            next_cursor: Some(DECOMP_MARKER.to_vec()),
                        };
                    }
                    let mut last_entry_key: Option<Vec<u8>> = None;
                    let pool_key = st.pool_key.clone();
                    for (k, v) in rows {
                        let Some(entry_key) = k
                            .strip_prefix(prefix.as_str())
                            .and_then(|h| hex::decode(h).ok())
                        else {
                            continue;
                        };
                        last_entry_key = Some(entry_key.clone());
                        let Some(entry): Option<LedgerEntry> =
                            ciborium::de::from_reader(v.as_slice()).ok()
                        else {
                            continue;
                        };
                        // The pool's raw aggregate is replaced by its
                        // residual (written in the tail) — skip here.
                        if pool_key.as_ref() == Some(&entry.holder) {
                            continue;
                        }
                        write_decomp_holder(st, &entry.holder, entry.assets);
                    }
                    st.mat_after = last_entry_key;
                    DecompStep {
                        done: false,
                        ingested: 0,
                        next_cursor: Some(DECOMP_MARKER.to_vec()),
                    }
                }
                DecompSub::MaterializeTail => {
                    // LP-only providers (LP holders with no liquid
                    // holdings, hence not visited via the ledger pass).
                    let lp_only: Vec<LedgerKey> = st
                        .lp_shares
                        .keys()
                        .filter(|k| !st.visited.contains(*k))
                        .cloned()
                        .collect();
                    for holder in lp_only {
                        write_decomp_holder(st, &holder, AssetMap::new());
                    }
                    // Vest-only owners (no liquid holdings, no LP).
                    let vest_only: Vec<LedgerKey> = st
                        .vests
                        .keys()
                        .filter(|k| !st.visited.contains(*k))
                        .cloned()
                        .collect();
                    for holder in vest_only {
                        write_decomp_holder(st, &holder, AssetMap::new());
                    }
                    // Pool residual (rounding remainder + un-enumerated LP).
                    if let Some(pool_key) = st.pool_key.clone() {
                        if !st.residual.is_empty() {
                            let residual = std::mem::take(&mut st.residual);
                            let entry = LedgerEntry {
                                holder: pool_key.clone(),
                                assets: residual,
                                lp_amount: 0,
                                vests: st.vests.get(&pool_key).cloned().unwrap_or_default(),
                            };
                            let entry_key = holder_entry_key(&pool_key);
                            let mut buf = Vec::with_capacity(128);
                            if ciborium::ser::into_writer(&entry, &mut buf).is_ok() {
                                state_kv::set_value(
                                    &decomp_kv_key(&st.policy_hex, &entry_key),
                                    &buf,
                                );
                            }
                        }
                    }
                    *slot = None;
                    DecompStep {
                        done: true,
                        ingested: 0,
                        next_cursor: None,
                    }
                }
            }
        })
    }

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
        emit_event(&HolderEvent::SnapshotBegin(SnapshotBegin {
            policy: hex::encode(predicate),
            cursor_slot: anchor_slot,
            cursor_hash_hex: String::new(),
        }));
    }
    fn emit_chunk(&mut self, predicate: &[u8], entries: Vec<LedgerEntry>) {
        let holders: Vec<HolderEntry> = entries.iter().map(entry_to_wire).collect();
        emit_event(&HolderEvent::SnapshotChunk(SnapshotChunk {
            policy: hex::encode(predicate),
            holders,
        }));
    }
    fn emit_end(&mut self, predicate: &[u8], total: u64) {
        let policy_hex = hex::encode(predicate);
        emit_event(&HolderEvent::SnapshotEnd(SnapshotEnd {
            policy: policy_hex.clone(),
            holder_count: total,
        }));
        // Snapshot-only side-state: drop decomp shards + meta; the raw
        // `ledger:` shards persist for delta accounting.
        state_kv::delete_prefix(&decomp_prefix(&policy_hex));
        state_kv::delete_value(&meta_kv_key(&policy_hex));
        DECOMP_STATE.with(|c| *c.borrow_mut() = None);
    }
    fn chunk_size(&self) -> usize {
        SNAPSHOT_CHUNK_HOLDERS
    }
}

export!(Module);
