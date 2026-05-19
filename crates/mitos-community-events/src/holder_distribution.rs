//! Wire-format event types for the `holder-distribution`
//! community module.
//!
//! See `docs/design/HOLDER_DISTRIBUTION_MODULE.md` for the
//! design rationale. The module emits a `HolderEvent` per
//! tracked policy:
//!
//! - A **chunked snapshot** on registration (or after a
//!   rollback) — `SnapshotBegin` → `SnapshotChunk` × N →
//!   `SnapshotEnd` — the full per-stake-credential balance
//!   ledger at the cursor, split into bounded chunks so the
//!   module never builds the whole holder-list CBOR at once
//!   (see `WASM_BUDGET_CHUNKING.md`).
//! - `Delta` on each TX touching a tracked policy — per-wallet
//!   balance changes resulting from that TX.
//!
//! Consumers (e.g. `cnft.dev-workers`'s holder-map worker)
//! apply a snapshot sequence once and then incrementally update
//! from deltas. A snapshot sequence is an authoritative
//! replacement of prior state; a delta is additive.
//!
//! ## Why per-asset balances per holder
//!
//! A Cardano policy can house multiple distinct asset names
//! (NFT collections — each unit is its own asset name; some
//! fungible policies issue multiple sub-tokens). The
//! `HolderEntry.assets: Vec<AssetBalance>` shape covers both
//! shapes without consumers having to special-case. Pure
//! fungible tokens get length-1 vectors; collection holders
//! get N entries.
//!
//! ## Why stake-credential keyed (not address-keyed)
//!
//! Holders typically have many addresses (one per UTxO) under
//! one stake credential. Keying the ledger by stake cred
//! aggregates a wallet's holdings into one entry — the
//! semantic users care about. Enterprise (no-stake) addresses
//! are bundled under `stake_cred_hex: None`.

use serde::{Deserialize, Serialize};

/// Per-asset balance under one policy held by one entity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetBalance {
    /// Lowercase hex asset name. May be empty (single fungible
    /// token under a policy with an empty asset name — rare
    /// but legal).
    pub asset_name_hex: String,
    pub quantity: u64,
}

/// One holder's stake of one policy. For pure fungible tokens
/// `assets` is length-1; for NFT-collection policies it can be
/// length-N where each entry is a distinct asset name held by
/// this stake credential.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HolderEntry {
    /// 56-char lowercase hex of the stake credential (payment
    /// or staking key/script hash, 28 bytes). `None` aggregates
    /// all enterprise (no-stake) addresses holding this policy
    /// — splitting per-enterprise-address would lose the
    /// stake-cred grouping consumers expect.
    pub stake_cred_hex: Option<String>,
    /// Per-asset-name balances under this policy. Sorted by
    /// `asset_name_hex` for deterministic wire shape (so
    /// snapshot-diffing across module versions is stable).
    pub assets: Vec<AssetBalance>,
}

/// Opens a **chunked snapshot** sequence for one policy. A full
/// snapshot is a `SnapshotBegin` → `SnapshotChunk` × N →
/// `SnapshotEnd` sequence rather than a single event, so the
/// emitting module never builds the whole holder-list CBOR in
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

/// One bounded chunk of a snapshot's holder list. Many
/// `SnapshotChunk`s follow one `SnapshotBegin`. Consumer
/// inserts each chunk's holders into the (wiped) projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotChunk {
    /// 56-char lowercase hex of the policy id.
    pub policy: String,
    /// A bounded slice of the policy's holders. Across all
    /// chunks of one sequence, holders are sorted by total
    /// quantity descending — the deterministic ordering makes
    /// golden testing simpler and top-N consumer slicing cheap.
    pub holders: Vec<HolderEntry>,
}

/// Closes a chunked snapshot sequence. On receipt the consumer
/// marks the policy's projection authoritative.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotEnd {
    /// 56-char lowercase hex of the policy id.
    pub policy: String,
    /// Total holders across every `SnapshotChunk` of this
    /// sequence — lets the consumer sanity-check it received
    /// the whole sequence.
    pub holder_count: u64,
}

/// Holders whose balances changed in one TX touching the
/// policy. Consumers apply each entry as a replacement of the
/// holder's prior state — a holder dropping to zero appears
/// here with `assets: []` (consumer removes them from the
/// projected ledger).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HolderDelta {
    /// 56-char lowercase hex of the policy id.
    pub policy: String,
    /// 64-char lowercase hex of the TX that caused these
    /// changes.
    pub tx_hash: String,
    /// Slot the TX was included at.
    pub slot: u64,
    /// Holders whose post-TX balance differs from their pre-TX
    /// state. Each entry carries the *new* post-TX balances —
    /// not the per-entry delta. Empty `assets` ⇒ holder dropped
    /// to zero under this policy.
    pub changed: Vec<HolderEntry>,
}

/// One emission. A chunked snapshot (`SnapshotBegin` →
/// `SnapshotChunk` × N → `SnapshotEnd`) is an authoritative
/// full-state replacement; `Delta` is an incremental update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HolderEvent {
    SnapshotBegin(SnapshotBegin),
    SnapshotChunk(SnapshotChunk),
    SnapshotEnd(SnapshotEnd),
    Delta(HolderDelta),
}

#[cfg(feature = "decode")]
pub fn decode_emit(channel: u32, payload: &[u8]) -> Option<String> {
    if channel != 0 {
        return None;
    }
    let event: HolderEvent = ciborium::de::from_reader(payload).ok()?;
    serde_json::to_string_pretty(&event).ok()
}
