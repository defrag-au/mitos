//! Burn domain — asset destruction events.
//!
//! Top-tier `Domain` arm. Emitted by the `mint-burn-indexer` for
//! each negative entry in a tx's `mint` field. Burns have no
//! destination address (the asset is destroyed); `previous_owner`
//! is the source address whose holding decreased.
//!
//! Burn is top-tier (not nested under any other domain) because
//! the destination-address concept doesn't apply — there's no
//! `new_owner` because the asset ceases to exist.
//!
//! See `docs/design/DOMAIN_REFACTOR.md` for the design rationale.

use serde::{Deserialize, Serialize};

use super::common::Address;

/// Per-input burn quantity. Emitted once per (asset, source)
/// pair within a tx — a burn that consumes N tokens across M
/// source addresses emits up to M events for the same asset
/// (one per source).
///
/// Asset identity (policy_id + asset_name_hex) lives on the
/// `ProtocolEvent` envelope; this payload carries only the
/// burn-specific details.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BurnPayload {
    /// Bech32 address of the source — the address that
    /// previously held the now-destroyed UTxO.
    pub previous_owner: Address,
    /// Quantity burned in this event. Always 1 for NFTs;
    /// arbitrary for fungibles and RFTs.
    pub amount: u64,
}
