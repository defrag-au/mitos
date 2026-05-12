//! Wire-format event types for `<brand>-dex` community modules
//! (CSWAP / Minswap / Splash / future DEX brands).
//!
//! See `docs/design/DEX_COMMUNITY_MODULES.md` for the design
//! rationale. One shared event-type crate; each per-brand wasm
//! module emits `DexAction` variants on channel 0 so consumers
//! subscribing across brands see a uniform wire shape.
//!
//! Phase 1 ships `Swap` only. Future variants (`LiquidityAdd`,
//! `LiquidityRemove`, `ZapIn`, `OrderCreate`, `OrderCancel`,
//! `StakeRewardClaim`) land additively — the `tag = "kind"`
//! discriminator means existing consumers reading `Swap` continue
//! to work unchanged as new variants appear.
//!
//! ## `dex_brand` and `contract_version`
//!
//! `dex_brand` is the human-readable label resolved from the
//! module's internal `address → brand_label` map. A single module
//! can cover multiple branded products sharing the same contract
//! family (e.g. a Minswap-fork DEX gets its own label without
//! needing a new module). `contract_version` distinguishes
//! versions within a brand ("V1" / "V2" / "V3") — `None` for
//! single-version brands like CSWAP.

use serde::{Deserialize, Serialize};

/// One asset moving through a swap. ADA is encoded as `Lovelace`
/// rather than a synthetic `policy=""/name=""` native to keep
/// the wire shape unambiguous on the consumer side.
///
/// Both variants are struct-shaped (rather than `Lovelace(u64)`)
/// because the `#[serde(tag = "kind")]` discriminator requires
/// variants ciborium can encode as maps; a newtype variant
/// wrapping a primitive (the original `Lovelace(u64)`) trips a
/// ciborium serializer error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SwapAsset {
    Lovelace {
        quantity: u64,
    },
    Native {
        /// 56-char lowercase hex policy id.
        policy: String,
        /// Lowercase hex asset name (may be empty for empty-name
        /// assets).
        asset_name_hex: String,
        quantity: u64,
    },
}

/// Pool reserves snapshot at a single point in time. `base` and
/// `quote` follow the pool datum's own convention (CSWAP names
/// them this way: quote is the pricing denominator, base is the
/// asset being priced). The quantities live inside the
/// `SwapAsset` variant — `Lovelace(N)` for ADA legs, `Native {
/// quantity, … }` for token legs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolReserves {
    pub base_asset: SwapAsset,
    pub quote_asset: SwapAsset,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Swap {
    /// 64-char lowercase hex of the consuming TX.
    pub tx_hash: String,
    /// Absolute slot.
    pub slot: u64,
    /// Brand label resolved from the module's internal
    /// address-to-brand map. For `cswap-dex`: "CSWAP". A
    /// hypothetical `minswap-dex` would emit "Minswap" /
    /// "MinswapFork" / etc.
    pub dex_brand: String,
    /// Contract version within the brand ("V1" / "V2" / "V3").
    /// `None` for single-version brands like CSWAP.
    pub contract_version: Option<String>,
    /// Bech32 address of the wallet that received `asset_out`.
    pub swapper_address: String,
    /// What the swapper sent in.
    pub asset_in: SwapAsset,
    /// What the swapper got out.
    pub asset_out: SwapAsset,

    // ===== Datum-driven richness =====
    //
    // Populated when the swap routed through a recognised pool
    // and the datums decoded cleanly. Defensive `Option<>` so a
    // decoder fault degrades gracefully rather than blocking the
    // emission; consumers always see structural fields above.
    /// Canonical asset-pair fingerprint — stable across pool
    /// contract versions for the same asset pair so consumers
    /// can union `ADA/<TOKEN>` events without fragmentation when
    /// the brand launches a new pool version.
    pub pool_id: Option<String>,
    /// Pool reserves immediately before the swap (consumed pool
    /// UTxO's datum + value).
    pub pool_reserves_before: Option<PoolReserves>,
    /// Pool reserves immediately after the swap (produced pool
    /// UTxO's datum + value).
    pub pool_reserves_after: Option<PoolReserves>,
    /// Pool's configured swap fee in basis points (e.g. 85 =
    /// 0.85%). For asymmetric-fee pools (Minswap V2) this
    /// surfaces the effective fee for *this swap's direction*;
    /// symmetric pools (CSWAP, Splash V1, Minswap V1) carry
    /// their single rate.
    pub pool_fee_bps: Option<u32>,
    /// Lovelace paid to the batcher for executing the swap.
    /// `None` for pool-direct swaps (no batcher hop).
    pub batcher_fee_lovelace: Option<u64>,
    /// Effective price the swapper got, encoded as
    /// `(asset_out_quantity, asset_in_quantity)` rational so
    /// consumers can pick precision rather than inherit a
    /// lossy f64 from the wire.
    pub effective_price: Option<(u64, u64)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DexAction {
    Swap(Swap),
    // Future variants (additive, no wire break):
    //   LiquidityAdd(LiquidityEvent)
    //   LiquidityRemove(LiquidityEvent)
    //   ZapIn(ZapEvent)
    //   OrderCreate(Order)
    //   OrderCancel(OrderRef)
    //   StakeRewardClaim(ClaimEvent)
}

#[cfg(feature = "decode")]
pub fn decode_emit(channel: u32, payload: &[u8]) -> Option<String> {
    if channel != 0 {
        return None;
    }
    let event: DexAction = ciborium::de::from_reader(payload).ok()?;
    serde_json::to_string_pretty(&event).ok()
}
