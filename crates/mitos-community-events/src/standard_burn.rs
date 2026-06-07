//! Wire-format event types for the `standard-burn` community
//! module — token destruction via negative `tx.mint` entries.
//!
//! Burns are emitted one per `(policy, asset_name)` pair with a
//! negative `quantity_delta` in the TX's mint field, where the
//! policy is in the consumer's declared interest set.
//!
//! No metadata is carried — burns are destruction events. The
//! consumer knows the `(policy, asset_name)` from context (e.g.
//! their own prior mint events or asset-listing tables).
//!
//! See `mitos/docs/design/MINT_BURN_MODULES.md` for the family
//! design.

use serde::{Deserialize, Serialize};

/// One asset destruction event. Quantity is positive in the
/// emitted event (the negation of `quantity_delta` on the wire
/// chain-side) for ergonomic downstream comparisons.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Burn {
    /// 56-char lowercase hex policy id.
    pub policy: String,
    /// Lowercase hex of the on-chain asset-name bytes.
    pub asset_name_hex: String,
    /// 64-char lowercase hex tx hash that contained the negative
    /// mint entry.
    pub tx_hash: String,
    /// Quantity destroyed in this TX (positive — the absolute
    /// value of the `quantity_delta`).
    pub quantity_burned: u64,
    /// Absolute slot of the TX's block. Lets a consumer order
    /// burns and revert by slot on a rollback. `#[serde(default)]`
    /// keeps the field additive — payloads emitted before this
    /// field was introduced decode with `slot = 0`.
    #[serde(default)]
    pub slot: u64,
}

#[cfg(feature = "decode")]
pub fn decode_emit(channel: u32, payload: &[u8]) -> Option<String> {
    if channel != 0 {
        return None;
    }
    let event: Burn = ciborium::de::from_reader(payload).ok()?;
    serde_json::to_string_pretty(&event).ok()
}
