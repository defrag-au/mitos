//! Wire-format event types for the `vesting-tracker` community
//! module.
//!
//! See `docs/design/VESTING_TRACKER_MODULE.md` for the design
//! rationale. The module watches lock-contract addresses (Shield
//! project vests) and lock-contract payment credentials (Shield
//! CrowdLock user vests) and emits `VestingEvent`s as locks
//! appear, get spent (unlocked), or get re-locked.
//!
//! ## Shared datum format
//!
//! Both Shield project vests and CrowdLock user vests use the
//! same on-chain Plutus datum:
//!
//! ```text
//! Constructor 0 [
//!   Int(unlock_ts_ms),                              // field 0
//!   List[ Bytes(owner_payment_key_hash_28b) ]       // field 1
//! ]
//! ```
//!
//! The chain doesn't carry a `VestStyle` discriminator; the
//! module derives it from TX metadata key `674`'s `msg` array
//! ("Shield Vest - Crowd Lock" → CrowdLock, "Shield Vest" →
//! Shield, otherwise Unknown).
//!
//! ## Identity + UTxO-keyed events
//!
//! Each lock UTxO is identified by `(tx_hash, output_index)` —
//! its `OutputRef`. Consumers persist locks keyed by this ref so
//! `Unlocked` events can do precise deletions without scanning
//! by owner. One UTxO may carry multiple non-lovelace assets;
//! the module emits one `LockEntry` per `(utxo, policy,
//! asset_name)` triple. Consumers dedup by all three.
//!
//! ## Owner stake-cred resolution
//!
//! The datum carries a 28-byte owner *payment* key hash. The
//! module resolves it to a stake credential via the chain-data
//! `resolve-stake-for-payment-pkh` host-fn (a thin redb scan
//! over the by-payment-cred index, returning the first stake
//! part found). Enterprise-only or freshly-emptied wallets
//! return `owner_stake_cred_hex: None` — consumers should keep
//! the lock visible (e.g. as an unresolved Vesting row) rather
//! than dropping it.
//!
//! ## Snapshot vs incremental
//!
//! `Snapshot` is full state replacement for the watched
//! `interest_value`: consumer wipes all locks under that key
//! and re-inserts. `Locked` and `Unlocked` are incremental.
//! Modules emit `Snapshot` on registration (cold-start) and
//! after a rollback; `Locked` / `Unlocked` per matching TX.

use serde::{Deserialize, Serialize};

/// On-chain reference to a lock UTxO. Wire-encoded as
/// `(tx_hash_hex, index)` for compactness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockRef {
    /// 64-char lowercase hex of the producing TX hash.
    pub tx_hash: String,
    pub index: u32,
}

/// Which kind of interest predicate this snapshot covers.
/// Consumers use this to scope `Snapshot` replacement: a
/// snapshot wipes prior locks under the same `interest_kind` +
/// `interest_value` pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterestKind {
    /// Module is watching a fixed bech32 address (project vests
    /// at a known contract address).
    Address,
    /// Module is watching all UTxOs at a payment credential
    /// (CrowdLock-style user vests where the staking part
    /// varies per UTxO).
    PaymentCred,
}

/// Module-authoritative VestStyle derived from TX metadata
/// `674` at the locking TX. `Unknown` for unrecognised or
/// missing metadata — consumers should treat as generic vest
/// rather than dropping the entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VestStyle {
    Shield,
    CrowdLock,
    Unknown,
}

/// One lock UTxO carrying one asset, with owner + unlock
/// metadata extracted from the datum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockEntry {
    /// Identity — used by consumers to match `Unlocked` events.
    pub utxo_ref: LockRef,
    /// Full bech32 address of the lock UTxO. Fixed for project
    /// vests at a known contract; varies across CrowdLock UTxOs
    /// (different staking parts per lock).
    pub lock_address: String,
    /// 56-char lowercase hex of the locked asset's policy id.
    pub policy: String,
    /// Lowercase hex of the locked asset's name. May be empty
    /// for single-asset fungible policies.
    pub asset_name_hex: String,
    /// Amount of the locked asset under this UTxO.
    pub amount: u64,
    /// 56-char lowercase hex of the owner's payment key hash,
    /// extracted from the datum.
    pub owner_pkh: String,
    /// 56-char lowercase hex of the owner's stake credential
    /// (key or script), resolved by the module via
    /// `resolve-stake-for-payment-pkh`. `None` when:
    /// - the PKH has no current UTxOs anywhere (rare)
    /// - all UTxOs using the PKH are enterprise (no-stake)
    pub owner_stake_cred_hex: Option<String>,
    /// Milliseconds-since-UNIX-epoch from the datum's first
    /// field. Consumers compare against `Date.now()` to render
    /// unlock dates.
    pub unlock_ts_ms: u64,
    /// VestStyle derived from the locking TX's metadata `674`.
    pub vest_style: VestStyle,
    /// 64-char lowercase hex of the TX that created this lock.
    pub locked_at_tx: String,
}

/// Opens a **chunked snapshot** sequence for one interest
/// registration. A full snapshot is a `SnapshotBegin` →
/// `SnapshotChunk` × N → `SnapshotEnd` sequence rather than a
/// single event, so the module never builds the whole lock-list
/// CBOR in wasm linear memory at once (`WASM_BUDGET_CHUNKING.md`).
///
/// Consumer semantics: on `SnapshotBegin`, wipe the interest's
/// projected rows — the sequence is an authoritative replacement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotBegin {
    pub interest_kind: InterestKind,
    /// Bech32 address (when `Address`) or hex payment cred
    /// (when `PaymentCred`).
    pub interest_value: String,
    /// Slot at which this snapshot is valid.
    pub cursor_slot: u64,
    /// 64-char lowercase hex of the block hash at `cursor_slot`.
    pub cursor_hash_hex: String,
}

/// One bounded chunk of a snapshot's lock list. Many
/// `SnapshotChunk`s follow one `SnapshotBegin`. Across all
/// chunks of one sequence, locks are deterministically ordered
/// (by utxo_ref then asset_name_hex) for stable golden testing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotChunk {
    pub interest_kind: InterestKind,
    pub interest_value: String,
    pub locks: Vec<LockEntry>,
}

/// Closes a chunked snapshot sequence. On receipt the consumer
/// marks the interest's projection authoritative.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotEnd {
    pub interest_kind: InterestKind,
    pub interest_value: String,
    /// Total locks across every `SnapshotChunk` of this sequence.
    pub lock_count: u64,
}

/// One lock that came into existence in a TX matching the
/// module's interest. Emitted per `(utxo, policy, asset_name)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VestingLock {
    pub interest_kind: InterestKind,
    pub interest_value: String,
    pub lock: LockEntry,
}

/// Identifies a lock UTxO that was spent in a TX. Consumers
/// delete the matching `holder_vests` row keyed on `lock_ref`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VestingUnlock {
    pub interest_kind: InterestKind,
    pub interest_value: String,
    pub lock_ref: LockRef,
    /// 64-char lowercase hex of the TX that consumed the lock.
    pub consuming_tx_hash: String,
    pub slot: u64,
}

/// One emission. A chunked snapshot (`SnapshotBegin` →
/// `SnapshotChunk` × N → `SnapshotEnd`) is an authoritative
/// full-state replacement; `Locked`/`Unlocked` are incremental.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VestingEvent {
    SnapshotBegin(SnapshotBegin),
    SnapshotChunk(SnapshotChunk),
    SnapshotEnd(SnapshotEnd),
    Locked(VestingLock),
    Unlocked(VestingUnlock),
}

#[cfg(feature = "decode")]
pub fn decode_emit(channel: u32, payload: &[u8]) -> Option<String> {
    if channel != 0 {
        return None;
    }
    let event: VestingEvent = ciborium::de::from_reader(payload).ok()?;
    serde_json::to_string_pretty(&event).ok()
}
