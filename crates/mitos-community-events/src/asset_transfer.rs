//! Wire-format event types for the `asset-transfer` community
//! module — net asset movements between addresses on a per-TX
//! basis.
//!
//! Scope: pairs of `(from_address, to_address, quantity)` for
//! each `(policy, asset_name)` whose holders changed in a TX.
//! Mints, burns, and address→same-address residuals (change
//! outputs) are out of scope:
//!
//! - **Mints** — net positive without a corresponding negative
//!   sender. Surfaced by the mint modules
//!   (`cip-25-mint`, `cip-68-mint`).
//! - **Burns** — net negative without a corresponding receiver.
//!   Surfaced by the burn modules (`standard-burn`,
//!   `burn-address`).
//! - **Change outputs** — `(address, asset)` pairs whose net
//!   delta is zero after the TX. Dropped via the per-TX netting
//!   pass before pair emission.
//!
//! Consumers wanting the full holder lifecycle for a policy
//! subscribe to `{asset-transfer, mint, metadata-update}` and
//! roll up at the consumer side.
//!
//! ## Multi-sender FT topology
//!
//! For TXs where multiple addresses contribute the same asset
//! (DEX swaps, atomic swaps, batched marketplace), the module
//! picks the primary sender deterministically: largest
//! `|delta|`, ties broken by address lex sort. Each receiver
//! gets one `Transfer` event attributed to that primary sender.
//! For NFTs (qty = 1, supply = 1 per asset) this is by
//! construction unambiguous since exactly one address can have
//! a negative delta.

use serde::{Deserialize, Serialize};

/// A net asset movement between two addresses within a single TX.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transfer {
    /// 56-char lowercase hex policy id.
    pub policy: String,
    /// Lowercase hex asset name. Empty string for the ada-only
    /// case (which this module never emits since lovelace isn't
    /// a native asset).
    pub asset_name_hex: String,
    /// Quantity transferred. NFTs = 1; FTs = whatever moved.
    /// `u64` because per-receiver allocations are always
    /// non-negative — the signed delta is internal to the
    /// netting pass.
    pub quantity: u64,
    /// Bech32 address that net-sent this quantity. For
    /// multi-sender FT topologies, picked deterministically as
    /// the largest-|delta| sender (ties broken by address lex
    /// sort).
    pub from_address: String,
    /// Bech32 address that net-received this quantity.
    pub to_address: String,
    /// 64-char lowercase hex consuming TX.
    pub tx_hash: String,
    /// Absolute slot of the TX's block.
    pub slot: u64,
}

/// Single-variant tagged enum — single-variant today but the
/// shape leaves room for future additions (e.g. an explicit
/// `ChangeRetained` debug event) without a breaking wire change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AssetTransfer {
    Transfer(Transfer),
}

#[cfg(feature = "decode")]
pub fn decode_emit(channel: u32, payload: &[u8]) -> Option<String> {
    if channel != 0 {
        return None;
    }
    let event: AssetTransfer = ciborium::de::from_reader(payload).ok()?;
    serde_json::to_string_pretty(&event).ok()
}
