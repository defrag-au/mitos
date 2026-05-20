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
    /// May be an empty string when the immediate beneficiary is
    /// a script address (e.g. an aggregator-wrapped order); in
    /// that case `originator_address` carries the actual user
    /// wallet that funded the swap, if discoverable.
    pub swapper_address: String,
    /// Bech32 address of the dominant user wallet (by lovelace)
    /// among the consuming TX's inputs, surfaced when the
    /// immediate `swapper_address` is empty (no user-wallet
    /// `asset_out` output found — typically because the
    /// destination is a script address belonging to an
    /// aggregator / order-routing wrapper). `None` when the
    /// `swapper_address` already identifies the user wallet
    /// directly, or when no key-wallet input could be found.
    /// Consumers wanting "who initiated the trade" should prefer
    /// this field over `swapper_address` when present.
    #[serde(default)]
    pub originator_address: Option<String>,
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
pub struct LiquidityAdd {
    pub tx_hash: String,
    pub slot: u64,
    pub dex_brand: String,
    pub contract_version: Option<String>,
    /// Bech32 of the wallet that supplied the liquidity and
    /// received the LP tokens.
    pub provider_address: String,
    /// What the provider added on each side of the pair.
    /// Asymmetric ratios (zap-style) appear as an uneven split
    /// — no separate `ZapIn` variant; consumers detect zap
    /// shape from the ratio against `pool_reserves_before`.
    pub quote_added: SwapAsset,
    pub base_added: SwapAsset,
    /// LP tokens minted to the provider. Raw asset (policy +
    /// name + quantity); LP→pool mapping stays off-module so
    /// the module doesn't need to maintain a runtime
    /// LP-policy registry.
    pub lp_received: SwapAsset,
    pub pool_id: Option<String>,
    pub pool_reserves_before: Option<PoolReserves>,
    pub pool_reserves_after: Option<PoolReserves>,
    /// Informational — the pool's swap fee. Doesn't apply to
    /// the LP-add itself but useful so consumers can render
    /// "you joined a pool charging X bps".
    pub pool_fee_bps: Option<u32>,
    pub batcher_fee_lovelace: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiquidityRemove {
    pub tx_hash: String,
    pub slot: u64,
    pub dex_brand: String,
    pub contract_version: Option<String>,
    pub provider_address: String,
    /// LP tokens burnt or returned by the provider.
    pub lp_burnt: SwapAsset,
    pub quote_withdrawn: SwapAsset,
    pub base_withdrawn: SwapAsset,
    pub pool_id: Option<String>,
    pub pool_reserves_before: Option<PoolReserves>,
    pub pool_reserves_after: Option<PoolReserves>,
    pub pool_fee_bps: Option<u32>,
    pub batcher_fee_lovelace: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FarmStake {
    pub tx_hash: String,
    pub slot: u64,
    pub dex_brand: String,
    pub contract_version: Option<String>,
    pub staker_address: String,
    /// LP token added to the farm. Raw asset; LP→pool mapping
    /// stays off-module (consumers join against their own
    /// pool catalogue if they need the pool ID).
    pub lp_token: SwapAsset,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FarmUnstake {
    pub tx_hash: String,
    pub slot: u64,
    pub dex_brand: String,
    pub contract_version: Option<String>,
    pub staker_address: String,
    pub lp_token: SwapAsset,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RewardClaim {
    pub tx_hash: String,
    pub slot: u64,
    pub dex_brand: String,
    pub contract_version: Option<String>,
    pub claimer_address: String,
    /// Reward asset(s) received by the claimer. Vector so
    /// multi-asset rewards (some farms pay multiple tokens
    /// per claim) and single-asset rewards share one shape.
    pub rewards: Vec<SwapAsset>,
}

/// What kind of order was cancelled. Useful so consumers can
/// route the cancel to the right "outstanding orders" list
/// without re-resolving the original order TX.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderKind {
    Swap,
    LiquidityAdd,
    LiquidityRemove,
    /// Order datum shape didn't match any known variant —
    /// emit anyway so consumers see the cancel happened, but
    /// can't classify it precisely. Useful signal for "we
    /// found a new order shape we should add a decoder for".
    Unknown,
}

/// Open order at a batcher / order script consumed via cancel
/// redeemer. Cancellation is the outcome (not just intent) —
/// unlike order *submission* which we don't emit because the
/// matching fill carries the actual outcome, a cancel has no
/// fill counterpart.
///
/// **NOT YET WIRED for `cswap-dex`** — CSWAP's contract appears
/// to lack a user-callable cancel path (every recent
/// order-script consume on mainnet has used a fill redeemer,
/// never `d87980`). The struct is defined here so consumers can
/// match the wire shape now; `splash-dex` (where cancels are
/// real and well-documented) will be the first module to
/// actually emit this variant. See
/// `docs/design/DEX_COMMUNITY_MODULES.md` for the investigation
/// notes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderCancel {
    pub tx_hash: String,
    pub slot: u64,
    pub dex_brand: String,
    pub contract_version: Option<String>,
    pub order_kind: OrderKind,
    pub canceller_address: String,
    /// The order UTxO that was consumed by the cancel. Lets
    /// consumers reconcile a cancel against the prior submit.
    pub prior_order_tx_hash: String,
    pub prior_order_output_index: u32,
    /// Lovelace returned to the canceller — order's locked
    /// lovelace minus the network fee.
    pub refund_lovelace: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DexAction {
    /// Pool swap — opposing reserve deltas.
    Swap(Swap),
    /// Two-sided (50/50) or zap-style (asymmetric) liquidity
    /// provision. Both reserves grow in the same TX.
    LiquidityAdd(LiquidityAdd),
    /// LP burnt to withdraw liquidity. Both reserves shrink.
    LiquidityRemove(LiquidityRemove),
    /// LP tokens locked into a farm script.
    FarmStake(FarmStake),
    /// LP tokens released from a farm script.
    FarmUnstake(FarmUnstake),
    /// Farm rewards distributed to a staker. Detection signal
    /// is brand-specific — for CSWAP it's an operator key wallet
    /// rather than a Plutus script.
    RewardClaim(RewardClaim),
    /// Open order at a batcher / order script consumed via the
    /// cancel redeemer. **Wire-defined only — no module emits
    /// this yet.** Reserved for `splash-dex` (where on-chain
    /// cancels are common and well-documented). CSWAP appears
    /// to lack a user-callable cancel path on-chain; see the
    /// `OrderCancel` struct comment for the investigation.
    OrderCancel(OrderCancel),
}

#[cfg(feature = "decode")]
pub fn decode_emit(channel: u32, payload: &[u8]) -> Option<String> {
    if channel != 0 {
        return None;
    }
    let event: DexAction = ciborium::de::from_reader(payload).ok()?;
    serde_json::to_string_pretty(&event).ok()
}
