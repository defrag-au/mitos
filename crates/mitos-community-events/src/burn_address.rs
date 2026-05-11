//! Wire-format event types for the `burn-address` community
//! module — assets sent to addresses the consumer flagged as
//! burn sinks.
//!
//! The "what counts as a burn address" decision lives entirely
//! on the consumer side; this module just watches outputs at
//! whatever address set the companion has registered via
//! `/api/_interest/burn-address/subscribe` (`kind = "address"`).
//! Canonical examples: the `$burnsnek` SNEK burn address, a
//! TCG's "graveyard" or "fusion-altar" address.
//!
//! One event per `(asset, watched-address)` pair landing in an
//! output. An output that carries 50 distinct assets to the
//! same burn address emits 50 events.
//!
//! No metadata is carried — consumers know `(policy, asset_name)`
//! from their own context.
//!
//! See `mitos/docs/design/MINT_BURN_MODULES.md` for the family
//! design.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddressBurn {
    /// 56-char lowercase hex policy id.
    pub policy: String,
    /// Lowercase hex of the on-chain asset-name bytes.
    pub asset_name_hex: String,
    /// 64-char lowercase hex tx hash that produced the output.
    pub tx_hash: String,
    /// Output index within `tx_hash`.
    pub output_index: u32,
    /// Quantity of this asset landing in the watched-address
    /// output.
    pub quantity: u64,
    /// The watched address the asset landed at (bech32). Echoed
    /// back so consumers tracking multiple burn sinks can route.
    pub burn_address: String,
}
