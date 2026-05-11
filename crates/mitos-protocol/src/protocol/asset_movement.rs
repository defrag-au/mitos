//! AssetMovement domain — residual classification.
//!
//! Top-tier `Domain` arm emitted by the dispatcher's
//! `none_match-indexer` tail-step when an asset transfer in a tx
//! wasn't claimed by any specific-domain indexer. This is the
//! home for P2P transfers between wallets, transfers through
//! unrecognised marketplace scripts, and movements through dApps
//! mitos doesn't yet have a domain for.
//!
//! Consumers can rely on `previous_owner != new_owner` at the
//! wallet level — the indexer suppresses self-transfers by
//! comparing stake credentials when both sides have one (HD-
//! wallet change going to a different payment address but the
//! same stake key counts as same-wallet). For non-Shelley
//! addresses without a stake credential, the rule falls back to
//! bech32 string equality.
//!
//! See `docs/design/DOMAIN_REFACTOR.md` for the design rationale.

use serde::{Deserialize, Serialize};

use super::common::Address;

/// Per-asset movement that wasn't claimed by a specific-domain
/// indexer. One event per (asset, source, destination, amount)
/// quadruple after same-wallet suppression. A tx that splits
/// 100 tokens (30 to Bob, 70 back to self as change) emits one
/// event with `amount: 30` — the change leg is suppressed.
///
/// Asset identity (policy_id + asset_name_hex) lives on the
/// `ProtocolEvent` envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetMovementPayload {
    /// Bech32 address of the source.
    pub previous_owner: Address,
    /// Bech32 address of the destination. Guaranteed not to
    /// resolve to the same wallet as `previous_owner` (per the
    /// indexer-side same-wallet suppression rule).
    pub new_owner: Address,
    /// Quantity moved in this event. Always 1 for NFTs;
    /// arbitrary for fungibles and RFTs.
    pub amount: u64,
}
