//! Mint domain — asset creation events.
//!
//! Top-tier `Domain` arm. Emitted by the `mint-burn-indexer` for
//! each positive entry in a tx's `mint` field. Mints have no
//! source address (the asset is created from nothing); `minter`
//! is the recipient address holding the freshly-minted UTxO.
//!
//! Mint is top-tier (not nested under marketplace or anywhere
//! else) because the source-address concept doesn't apply —
//! there's no `previous_owner` because there was no prior
//! holding.
//!
//! See `docs/design/DOMAIN_REFACTOR.md` for the design rationale.

use serde::{Deserialize, Serialize};

use super::common::Address;

/// Per-output mint quantity. Emitted once per (asset, recipient)
/// pair within a tx — a mint that spreads N tokens across M
/// recipient addresses emits up to M events for the same asset
/// (one per recipient).
///
/// Asset identity (policy_id + asset_name_hex) lives on the
/// `ProtocolEvent` envelope; this payload carries only the
/// mint-specific details.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MintPayload {
    /// Bech32 address of the recipient — the address holding
    /// the freshly-minted UTxO output.
    pub minter: Address,
    /// Quantity minted in this event. Always 1 for NFTs;
    /// arbitrary for fungibles and RFTs.
    pub amount: u64,
}
