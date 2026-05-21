//! Wire-format event types for the `collection-holders`
//! community module.
//!
//! See `docs/design/COLLECTION_MODULES.md` for the design.
//! `collection-holders` covers policies where every asset name
//! is a distinct collectible identity — NFTs (qty=1 per asset
//! name) and RFTs (qty>1 per asset name, bounded conceptually
//! by edition design). Sibling to `holder-distribution`, which
//! covers CNT distribution and is shaped differently (holder →
//! total qty rather than `(asset_name, holder, qty)`).
//!
//! The module emits a `CollectionEvent` per tracked policy:
//!
//! - A **chunked snapshot** on registration (or after a
//!   rollback) — `SnapshotBegin` → `SnapshotChunk` × N →
//!   `SnapshotEnd` — the full `(asset_name, holder, quantity)`
//!   ledger at the cursor, split into bounded chunks so the
//!   module never builds the whole holdings CBOR at once
//!   (see `WASM_BUDGET_CHUNKING.md`).
//! - `Delta` on each TX touching a tracked policy — per-asset
//!   movements within that TX, after change-output netting.
//!
//! ## Why `Holding` is a flat tuple, not a nested HolderEntry
//!
//! `holder-distribution` groups balances under one `HolderEntry`
//! per holder because CNT policies typically have one asset
//! name and the holder is the natural grouping. Collection-
//! shaped policies are the inverse: many asset names per
//! policy, often one holder per asset name. Flattening to
//! `(asset_name, holder, quantity)` tuples lets consumers index
//! either direction (by asset_name or by holder) without
//! pre-grouping work.
//!
//! ## Why `HolderRef::Script` surfaces script addresses as data
//!
//! Maestro's `/policy/{id}/accounts` groups by stake credential
//! and omits script-locked supply (marketplace escrows, vesting
//! contracts). For a collectible-shaped policy that's a real
//! data loss — listed NFTs are part of the supply picture.
//! Surfacing `HolderRef::Script` lets consumers (collection-
//! ownership, future TCG) decide whether to include, exclude,
//! or re-attribute marketplace-held supply via consumer-side
//! marketplace knowledge.

use serde::{Deserialize, Serialize};

/// Where supply currently lives. Three variants cover the
/// shapes seen on collectible policies:
///
/// - `Stake` — delegated address with a stake credential
///   (typical user wallets). 56-char lowercase hex of the
///   28-byte staking credential. Network-agnostic on the
///   wire — consumers compute `stake1...` (mainnet) /
///   `stake_test1...` (testnet) bech32 themselves using
///   their deployment context. The hex collapses Key vs
///   Script delegation; consumers that need to distinguish
///   handle the rare Script-staking case via byte-level
///   credential comparison.
/// - `Payment` — enterprise / no-stake address (rare for
///   typical wallets, common for multi-sig or
///   programmatically-generated addresses). Carries the full
///   bech32 since there's no smaller stable identifier.
/// - `Script` — script address (marketplace contract, vesting,
///   etc.). `label` is `Some` when the script is recognised in
///   the shared `address-registry`; consumers can branch on
///   the label for marketplace-aware projections without
///   maintaining their own registry.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HolderRef {
    /// 56-char lowercase hex of the 28-byte stake credential.
    /// Network-agnostic; consumer derives bech32.
    Stake(String),
    /// Bech32 enterprise / no-stake address.
    Payment(String),
    /// Bech32 script address.
    Script {
        addr: String,
        /// Tag from `shared-crates/address-registry`, e.g.
        /// `"jpg.store v3"`, `"wayup"`, `"crowdlock"`. `None`
        /// when the script address isn't registered.
        label: Option<String>,
    },
}

/// One holder's holding of one asset name under one policy.
/// `quantity` is always 1 for NFTs; meaningful for RFTs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Holding {
    /// Lowercase hex asset name. May be empty (single-asset
    /// policy with empty name — rare but legal).
    pub asset_name_hex: String,
    pub holder: HolderRef,
    pub quantity: u64,
}

/// Opens a **chunked snapshot** sequence for one policy. A full
/// snapshot is a `SnapshotBegin` → `SnapshotChunk` × N →
/// `SnapshotEnd` sequence rather than a single event, so the
/// emitting module never builds the whole holdings CBOR in
/// wasm linear memory at once (see `WASM_BUDGET_CHUNKING.md`).
///
/// Consumer semantics: on `SnapshotBegin`, wipe the policy's
/// projected rows — the sequence is an authoritative
/// replacement of prior state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotBegin {
    /// 56-char lowercase hex of the policy id this snapshot
    /// covers.
    pub policy: String,
    /// Slot at which this snapshot is valid.
    pub cursor_slot: u64,
    /// 64-char lowercase hex of the block hash at `cursor_slot`.
    pub cursor_hash_hex: String,
}

/// One bounded chunk of a snapshot's holdings list. Many
/// `SnapshotChunk`s follow one `SnapshotBegin`. Consumer
/// inserts each chunk's holdings into the (wiped) projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotChunk {
    /// 56-char lowercase hex of the policy id.
    pub policy: String,
    /// A bounded slice of the policy's holdings. Across all
    /// chunks of one sequence, holdings are sorted by
    /// `(asset_name_hex, holder)` ascending — the
    /// deterministic ordering makes golden testing simpler.
    pub holdings: Vec<Holding>,
}

/// Closes a chunked snapshot sequence. On receipt the consumer
/// marks the policy's projection authoritative.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotEnd {
    /// 56-char lowercase hex of the policy id.
    pub policy: String,
    /// Total holdings across every `SnapshotChunk` of this
    /// sequence — lets the consumer sanity-check it received
    /// the whole sequence.
    pub holding_count: u64,
}

/// One asset movement within a TX. `from = None` represents a
/// mint (asset materialised into `to`); `to = None` represents
/// a burn (asset consumed from `from` with no replacement).
/// Both `None` is not a valid state and never emitted.
///
/// Same-holder zero-delta movements (change outputs) are
/// netted out before emission — only movements that actually
/// change the holdings ledger appear.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Movement {
    /// Lowercase hex asset name.
    pub asset_name_hex: String,
    pub from: Option<HolderRef>,
    pub to: Option<HolderRef>,
    pub quantity: u64,
}

/// Movements within one TX touching the policy. Consumers
/// apply each movement to their projected holdings ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionDelta {
    /// 56-char lowercase hex of the policy id.
    pub policy: String,
    /// 64-char lowercase hex of the TX that produced these
    /// movements.
    pub tx_hash: String,
    /// Slot the TX was included at.
    pub slot: u64,
    /// Non-empty list of net movements. A TX that nets to zero
    /// movement (rare; possible if every asset of the policy
    /// returns to its prior holder) does not emit a `Delta`.
    pub movements: Vec<Movement>,
}

/// One emission. A chunked snapshot (`SnapshotBegin` →
/// `SnapshotChunk` × N → `SnapshotEnd`) is an authoritative
/// full-state replacement; `Delta` is an incremental update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CollectionEvent {
    SnapshotBegin(SnapshotBegin),
    SnapshotChunk(SnapshotChunk),
    SnapshotEnd(SnapshotEnd),
    Delta(CollectionDelta),
}

#[cfg(feature = "decode")]
pub fn decode_emit(channel: u32, payload: &[u8]) -> Option<String> {
    if channel != 0 {
        return None;
    }
    let event: CollectionEvent = ciborium::de::from_reader(payload).ok()?;
    serde_json::to_string_pretty(&event).ok()
}
