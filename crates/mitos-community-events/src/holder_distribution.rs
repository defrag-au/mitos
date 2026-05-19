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

/// How `holder-distribution` identifies a holder. Generalised
/// from the older `stake_cred_hex: Option<String>` so enterprise
/// (no-stake-credential) holders are surfaced distinctly rather
/// than collapsed into a single dropped bucket — required for
/// worker-side burn classification (a `/dev/null` sink is an
/// enterprise address) and a latent under-counting fix on its
/// own (real enterprise wallets were invisible before).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HolderId {
    /// Delegated address — grouped by its 28-byte stake
    /// credential (56-char lowercase hex). The dominant case.
    Stake(String),
    /// Enterprise (no-stake) address — the full bech32 address.
    /// These share no stake-cred grouping, so each is its own
    /// holder. Pointer-stake and Byron addresses are still
    /// dropped (not surfaced as enterprise).
    Enterprise(String),
}

/// The module's best knowledge of what a credential is, from
/// the contracts it recognised while decomposing. The worker
/// refines this with project-config classification (burn sinks,
/// known wallets, treasury) on top.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum HolderRole {
    /// A plain holder — the default; also what an enterprise
    /// holder gets until the worker classifies it.
    #[default]
    Wallet,
    /// A DEX pool the module recognised — either the residual
    /// after LP decomposition, or a pool kind not decomposed
    /// yet (Splash, before Splash decomposition lands).
    DexPool,
    /// A vesting contract — the residual after vesting
    /// decomposition (most locked tokens are attributed to
    /// owners via `HolderEntry::vests`).
    VestingContract,
}

/// One holder's stake of one policy. For pure fungible tokens
/// `assets` is length-1; for NFT-collection policies it can be
/// length-N where each entry is a distinct asset name held by
/// this holder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HolderEntry {
    /// The holder's identity — stake credential for delegated
    /// addresses, full bech32 for enterprise.
    pub id: HolderId,
    /// Liquid holdings of this policy. Vested tokens are NOT
    /// folded in here — `assets + Σ vests.amount` is the true
    /// total.
    pub assets: Vec<AssetBalance>,
    /// LP-decomposition attribution — the portion of `assets`
    /// redistributed from a DEX pool to this holder (the
    /// "Scout Vessels" band). Already included in `assets`. `0`
    /// for a non-LP holder. See
    /// `docs/design/HOLDER_DISTRIBUTION_LP_DECOMPOSITION.md`.
    #[serde(default)]
    pub lp_amount: u64,
    /// The module's role tag for this holder. The worker's
    /// classifier reads this first, then overrides with config
    /// (e.g. enterprise → burn) where applicable.
    #[serde(default)]
    pub role: HolderRole,
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
