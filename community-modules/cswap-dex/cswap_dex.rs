//! CSWAP DEX swap-recognition community module.
//!
//! Emits `DexAction::Swap` for swaps routing through CSWAP's
//! single pool script address. CSWAP runs all 82 pools at one
//! address (`POOL_SCRIPT_ADDR`) so interest is a single
//! `at-address` predicate.
//!
//! ## Detection
//!
//! Within one `handle_events` batch (= one TX) we look for:
//!
//! - `Consumed` events at `POOL_SCRIPT_ADDR` with a decodable
//!   pool datum → "pool input" (reserves *before*).
//! - `Produced` events at `POOL_SCRIPT_ADDR` with a decodable
//!   pool datum → "pool output" (reserves *after*).
//!
//! At flush we match consumed/produced pools by their
//! `(quote_policy, quote_name, base_policy, base_name)` tuple
//! (the pair the pool exists for). For each matched pair:
//!
//! - Compute reserves before/after from the pool UTxO's
//!   `lovelace + assets` field, gated by the datum's pair
//!   identities.
//! - Determine swap direction from the sign of the deltas — one
//!   side grows (asset_in went into the pool), the other shrinks
//!   (asset_out came out).
//! - Find the user's wallet output: a `Produced` event at an
//!   address that isn't the CSWAP pool nor the CSWAP order script,
//!   carrying `asset_out`. For batcher-routed TXs with multiple
//!   user fills the current implementation emits a single event
//!   keyed on the first such output; richer per-user splitting
//!   needs the order datums (Phase 1 TODO — defer until we have
//!   a batcher-fill fixture in hand).
//!
//! ## Datum
//!
//! CSWAP pool datum (Constr 121, 8 fields):
//! ```text
//! [0] totalLpTokens: BigInt
//! [1] poolFee:       BigInt   (basis points)
//! [2] quotePolicy:   Bytes    (empty for ADA-paired pools)
//! [3] quoteName:     Bytes
//! [4] basePolicy:    Bytes
//! [5] baseName:      Bytes
//! [6] lpTokenPolicy: Bytes
//! [7] lpTokenName:   Bytes
//! ```
//!
//! `quote` is the pricing denominator (typically ADA), `base` is
//! the asset being priced. Phase 1 lifts the decoder + constant
//! product math from `shared-crates/cardano-tx/dex/cswap/pool.rs`
//! and extends it to extract the asset-pair identities (which the
//! shared decoder doesn't surface — it only returns totalLpTokens
//! + poolFee for builder-side math).
//!
//! ## TODO: `OrderCancel` not emitted
//!
//! The `DexAction::OrderCancel` variant is wire-defined but no
//! detection runs here. Investigation against mainnet history
//! (2026-05) showed that:
//!
//! - The CSWAP TX-builder code in cnft.dev-workers marks CSWAP
//!   order cancellation as "not yet implemented".
//! - Every one of the ~30 most-recent order-script consumes
//!   on-chain uses a fill redeemer (Constr 2, `d87b9f…`).
//!   None used the expected cancel redeemer (`d87980` =
//!   Constr 0 with empty fields).
//! - The CSWAP UI shows a "cancel" affordance, but the TXs
//!   marked as such are aggregator-mediated submissions, not
//!   on-chain script unlocks.
//!
//! Best current theory: CSWAP's order contract may not expose
//! a user-callable cancel path at all (orders sit at the
//! batcher until filled). `OrderCancel` will first land in
//! `splash-dex`, where Splash supports proper on-chain cancels
//! with a working builder in shared-crates. If a real CSWAP
//! cancel TX surfaces in the future, the detection path is a
//! single-fixture addition to this module.

use std::collections::BTreeMap;

use mitos_community_events::dex::{
    DexAction, FarmStake, FarmUnstake, LiquidityAdd, LiquidityRemove, PoolReserves, RewardClaim,
    Swap, SwapAsset,
};
use pallas_codec::minicbor;
use pallas_primitives::{BigInt, PlutusData};

use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::types::{
    ChainPoint, ConsumedEvent, ProducedEvent, TypedDatum, TypedOutput, UtxoEvent,
};

const LOG_TARGET: &str = "cswap-dex-module";

const DEX_BRAND: &str = "CSWAP";

/// CSWAP pool script address. All 82 pools live here. CSWAP
/// uses a single canonical stake credential across its pools
/// (unlike Splash, which varies stake creds per pool — to be
/// handled with a prefix-match interest set when `splash-dex`
/// lands).
const POOL_SCRIPT_ADDR: &str =
    "addr1z8ke0c9p89rjfwmuh98jpt8ky74uy5mffjft3zlcld9h7ml3lmln3mwk0y3zsh3gs3dzqlwa9rjzrxawkwm4udw9axhs6fuu6e";

/// CSWAP order / batcher script address prefix (51 chars =
/// `addr1z` + header byte + 28-byte payment hash worth of bech32).
/// Each user's order has its own stake credential glued to the
/// same payment script, so we prefix-match rather than enumerate.
/// Used at flush time to identify consumed orders so we can
/// compute `batcher_fee_lovelace`.
const ORDER_SCRIPT_ADDR_PREFIX: &str =
    "addr1z8d9k3aw6w24eyfjacy809h68dv2rwnpw0arrfau98jk6nh";

/// CSWAP farm script address. LP tokens get locked here when a
/// user stakes for yield. CSWAP uses one canonical farm script
/// (confirmed against staked-farm fixtures); should new farm
/// variants appear we'd add their bech32 here.
const FARM_SCRIPT_ADDR: &str =
    "addr1z9xf82dwn6aaaftz6dnjslhkgu0pvtxrhfqsxqm8h42u7uggjrxwszhuqj73gufx56c8qwnuhvf2nw5dzdr5f50rqr5qt8sqcf";

/// CSWAP reward-request collection wallet (key-only `addr1v`).
/// Users send a flat 2 ADA "claim ticket" here when initiating
/// a harvest. The distribution TX later consumes this 2 ADA
/// alongside a reward-asset input from the treasury and pays
/// the reward to the user — that consume is our detection signal
/// for `RewardClaim`. CSWAP retains the spread between the flat
/// fee and the actual TX cost as protocol revenue.
///
/// Operator-managed key wallet rather than a Plutus script —
/// fragile if CSWAP rotates the address. See
/// "Detection-signal stability" in the design doc.
const REWARD_REQUEST_WALLET: &str =
    "addr1vyw4d7mypuwvt54twjr49z9ctzl86wv3ee8sv7guuv5ft7gd7y66g";

/// CSWAP treasury / reward-source wallet. Observed sourcing
/// reward assets in distribution TXs. Used as an exclude-list
/// entry so its outputs aren't mistaken for `RewardClaim`
/// recipients (the wallet is `addr1v` and would otherwise pass
/// the user-wallet prefix check).
const TREASURY_WALLET: &str =
    "addr1v92ecw588ply6nwk35lczw9nh48u044sxqkwzwfmz4tcneqff35jr";

/// Bech32 prefixes used to identify a *user wallet* output —
/// payment credential is a key (not a script). Cardano header
/// byte's payment-type half:
///
/// - `addr1q` — key payment + key stake (the common wallet shape)
/// - `addr1u` — key payment + pointer stake (rare)
/// - `addr1v` — key-only enterprise (no stake)
///
/// Script-locked addresses (`addr1z` / `addr1x` / `addr1w`) are
/// pool / order / batcher / aggregator-router outputs that we
/// don't credit as the swapper. Testnet variants
/// (`addr_test1q` / `…u` / `…v`) covered by the helper below.
const MAINNET_USER_PREFIXES: &[&str] = &["addr1q", "addr1u", "addr1v"];
const TESTNET_USER_PREFIXES: &[&str] = &["addr_test1q", "addr_test1u", "addr_test1v"];

/// One pool input captured from the event stream.
struct PoolInput {
    /// Decoded pool datum (asset pair + fee).
    datum: CswapPoolDatum,
    /// Pool UTxO's lovelace + assets — drives reserves_before.
    prior_output: TypedOutput,
}

/// One pool output captured from the event stream. The datum
/// is decoded both to derive `PairKey` for the BTreeMap *and*
/// to expose `total_lp_tokens` so we can compute the LP-supply
/// delta over consume/produce — that drives `lp_received` for
/// `LiquidityAdd` and `lp_burnt` for `LiquidityRemove`.
struct PoolOutput {
    datum: CswapPoolDatum,
    output: TypedOutput,
}

#[derive(Default)]
struct TxBuffer {
    /// Pool inputs keyed by the pair identity from their datum.
    /// `(quote_policy, quote_name, base_policy, base_name)`.
    pool_inputs: BTreeMap<PairKey, PoolInput>,
    /// Pool outputs keyed the same way.
    pool_outputs: BTreeMap<PairKey, PoolOutput>,
    /// All produced outputs — needed at flush to find the user
    /// wallet output (the non-script recipient of asset_out /
    /// LP / withdrawn liquidity).
    produced: Vec<ProducedEvent>,
    /// All consumed events. Used at flush to find user wallet
    /// inputs (FarmStake staker identification) and for the
    /// batcher-fee derivation path. Kept as raw events rather
    /// than pre-filtered so future variants can inspect them
    /// without a TxBuffer schema change.
    consumed: Vec<ConsumedEvent>,
    /// Consumed order/batcher UTxOs — convenience subset of
    /// `consumed`, populated at handle-time to avoid rescanning
    /// for the batcher-fee derivation hot path.
    consumed_orders: Vec<ConsumedEvent>,
    /// Farm-script consume + produce events. CSWAP runs one
    /// canonical farm script; one consume + one produce per TX
    /// is the expected shape.
    farm_input: Option<ConsumedEvent>,
    farm_output: Option<ProducedEvent>,
    /// Did this TX consume the CSWAP reward-request collection
    /// 2 ADA claim ticket? If so, user-wallet outputs holding
    /// non-ADA assets are reward distributions.
    consumed_reward_request: bool,
    tx_hash: Option<Vec<u8>>,
    slot: Option<u64>,
}

/// `(quote_policy, quote_name, base_policy, base_name)` — the
/// stable identity of a pool independent of which side of the
/// swap is happening.
type PairKey = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>);

fn handle_produced(p: &ProducedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(p.tx_hash.clone());
    }
    if buf.slot.is_none() {
        buf.slot = chain_point_slot(&p.cursor);
    }
    if p.output.address == POOL_SCRIPT_ADDR {
        if let Some(datum) = p
            .datum
            .as_ref()
            .and_then(resolve_datum_bytes)
            .as_deref()
            .and_then(decode_pool_datum)
        {
            buf.pool_outputs.insert(
                datum.pair_key(),
                PoolOutput {
                    datum,
                    output: p.output.clone(),
                },
            );
        }
    } else if p.output.address == FARM_SCRIPT_ADDR {
        buf.farm_output = Some(p.clone());
    }
    buf.produced.push(p.clone());
}

fn handle_consumed(c: &ConsumedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(c.consuming_tx_hash.clone());
    }
    if buf.slot.is_none() {
        buf.slot = chain_point_slot(&c.cursor);
    }
    if c.prior_output.address == POOL_SCRIPT_ADDR {
        if let Some(datum) = c
            .prior_datum
            .as_ref()
            .and_then(resolve_datum_bytes)
            .as_deref()
            .and_then(decode_pool_datum)
        {
            buf.pool_inputs.insert(
                datum.pair_key(),
                PoolInput {
                    datum,
                    prior_output: c.prior_output.clone(),
                },
            );
        }
    } else if c.prior_output.address.starts_with(ORDER_SCRIPT_ADDR_PREFIX) {
        buf.consumed_orders.push(c.clone());
    } else if c.prior_output.address == FARM_SCRIPT_ADDR {
        buf.farm_input = Some(c.clone());
    } else if c.prior_output.address == REWARD_REQUEST_WALLET {
        buf.consumed_reward_request = true;
    }
    buf.consumed.push(c.clone());
}

fn flush_buffer(buf: TxBuffer) {
    let Some(tx_hash) = buf.tx_hash else {
        return;
    };
    let tx_hash_hex = hex::encode(&tx_hash);
    let slot = buf.slot.unwrap_or(0);

    let mut pool_outputs = buf.pool_outputs;

    for (pair, pool_in) in buf.pool_inputs {
        let Some(pool_out) = pool_outputs.remove(&pair) else {
            // Consumed pool with no matching produced — pool
            // retirement or unusual flow; not a swap.
            continue;
        };
        emit_for_pool_pair(
            &tx_hash_hex,
            slot,
            &pool_in,
            &pool_out,
            &buf.produced,
            &buf.consumed,
            &buf.consumed_orders,
        );
    }
    // Produced pools with no matching consumed (pool creation
    // TXs etc.) aren't swaps; ignore.

    // Farm stake / unstake: one farm consume + one farm produce
    // in the same TX → LP-token delta tells us direction.
    if let (Some(fi), Some(fo)) = (buf.farm_input.as_ref(), buf.farm_output.as_ref()) {
        emit_for_farm_pair(&tx_hash_hex, slot, fi, fo, &buf.consumed, &buf.produced);
    }

    // Reward distribution: a TX consuming the 2 ADA claim
    // ticket from the request-collection wallet is paying out
    // a harvest. Each user wallet output carrying non-ADA
    // assets is a claimant.
    if buf.consumed_reward_request {
        emit_reward_claims(&tx_hash_hex, slot, &buf.produced);
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_for_pool_pair(
    tx_hash_hex: &str,
    slot: u64,
    pool_in: &PoolInput,
    pool_out: &PoolOutput,
    produced: &[ProducedEvent],
    consumed: &[ConsumedEvent],
    consumed_orders: &[ConsumedEvent],
) {
    let datum = &pool_in.datum;

    // Reserves before/after. Datum tells us which assets are
    // base/quote; the pool UTxO's lovelace + asset entries give
    // us the quantities.
    let quote_before = pool_value(&pool_in.prior_output, &datum.quote_policy, &datum.quote_name);
    let base_before = pool_value(&pool_in.prior_output, &datum.base_policy, &datum.base_name);
    let quote_after = pool_value(&pool_out.output, &datum.quote_policy, &datum.quote_name);
    let base_after = pool_value(&pool_out.output, &datum.base_policy, &datum.base_name);

    let delta_quote = quote_after as i128 - quote_before as i128;
    let delta_base = base_after as i128 - base_before as i128;

    let reserves_before = Some(PoolReserves {
        base_asset: asset_from_pair(&datum.base_policy, &datum.base_name, base_before),
        quote_asset: asset_from_pair(&datum.quote_policy, &datum.quote_name, quote_before),
    });
    let reserves_after = Some(PoolReserves {
        base_asset: asset_from_pair(&datum.base_policy, &datum.base_name, base_after),
        quote_asset: asset_from_pair(&datum.quote_policy, &datum.quote_name, quote_after),
    });

    // Dispatch on LP-supply delta first — that's the
    // load-bearing signal:
    //
    //   - LP supply grew  → LiquidityAdd (any pool-reserve
    //     shape, including asymmetric "zap-in")
    //   - LP supply shrank → LiquidityRemove (any pool-reserve
    //     shape, including asymmetric "zap-out" where only one
    //     leg's reserves move)
    //   - LP supply unchanged → must be a Swap (opposing-sign
    //     reserve deltas) or non-actionable transition
    //
    // Using reserve deltas as the primary signal misses
    // asymmetric LP events; the LP-supply delta is the cleanest
    // indicator that someone minted/burnt liquidity tokens.
    let lp_delta = pool_out.datum.total_lp_tokens as i128 - datum.total_lp_tokens as i128;

    match lp_delta.signum() {
        1 => emit_liquidity_add(
            tx_hash_hex,
            slot,
            datum,
            delta_quote,
            delta_base,
            pool_out,
            produced,
            consumed_orders,
            reserves_before,
            reserves_after,
        ),
        -1 => emit_liquidity_remove(
            tx_hash_hex,
            slot,
            datum,
            delta_quote,
            delta_base,
            pool_out,
            produced,
            consumed,
            consumed_orders,
            reserves_before,
            reserves_after,
        ),
        _ => {
            // LP supply unchanged — opposing reserve-delta signs
            // mean a swap; anything else is a transition we
            // haven't characterised.
            match (delta_quote.signum(), delta_base.signum()) {
                (1, -1) | (-1, 1) => emit_swap(
                    tx_hash_hex,
                    slot,
                    datum,
                    delta_quote,
                    delta_base,
                    produced,
                    consumed_orders,
                    reserves_before,
                    reserves_after,
                ),
                _ => logging::log(
                    LogLevel::Info,
                    LOG_TARGET,
                    &format!(
                        "tx={tx_hash_hex}: unrecognised pool transition (lp_delta={lp_delta}, delta_quote={delta_quote}, delta_base={delta_base}), skipping"
                    ),
                ),
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_swap(
    tx_hash_hex: &str,
    slot: u64,
    datum: &CswapPoolDatum,
    delta_quote: i128,
    delta_base: i128,
    produced: &[ProducedEvent],
    consumed_orders: &[ConsumedEvent],
    reserves_before: Option<PoolReserves>,
    reserves_after: Option<PoolReserves>,
) {
    let (asset_in, asset_out, in_qty, out_qty) = if delta_quote > 0 {
        // Swapper paid quote → received base.
        (
            asset_from_pair(&datum.quote_policy, &datum.quote_name, delta_quote as u64),
            asset_from_pair(&datum.base_policy, &datum.base_name, (-delta_base) as u64),
            delta_quote as u64,
            (-delta_base) as u64,
        )
    } else {
        (
            asset_from_pair(&datum.base_policy, &datum.base_name, delta_base as u64),
            asset_from_pair(
                &datum.quote_policy,
                &datum.quote_name,
                (-delta_quote) as u64,
            ),
            delta_base as u64,
            (-delta_quote) as u64,
        )
    };

    let swapper = find_swapper(produced, &asset_out);
    let swapper_address = swapper
        .as_ref()
        .map(|s| s.address.clone())
        .unwrap_or_default();
    let batcher_fee_lovelace =
        derive_batcher_fee_lovelace(consumed_orders, &asset_in, swapper.as_ref());

    emit_event(&DexAction::Swap(Swap {
        tx_hash: tx_hash_hex.to_string(),
        slot,
        dex_brand: DEX_BRAND.to_string(),
        contract_version: None,
        swapper_address,
        asset_in,
        asset_out,
        pool_id: Some(pool_id_for_pair(datum)),
        pool_reserves_before: reserves_before,
        pool_reserves_after: reserves_after,
        pool_fee_bps: u32::try_from(datum.pool_fee_bps).ok(),
        batcher_fee_lovelace,
        effective_price: Some((out_qty, in_qty)),
    }));
}

#[allow(clippy::too_many_arguments)]
fn emit_liquidity_add(
    tx_hash_hex: &str,
    slot: u64,
    datum: &CswapPoolDatum,
    delta_quote: i128,
    delta_base: i128,
    pool_out: &PoolOutput,
    produced: &[ProducedEvent],
    consumed_orders: &[ConsumedEvent],
    reserves_before: Option<PoolReserves>,
    reserves_after: Option<PoolReserves>,
) {
    // For zap-in (asymmetric) adds, one side's delta may be
    // zero or negative (e.g. the contract internally swaps
    // before adding). Surface only the positive part so the
    // event reports "ADA actually added to pool".
    let quote_added_qty = u64::try_from(delta_quote.max(0)).unwrap_or(u64::MAX);
    let base_added_qty = u64::try_from(delta_base.max(0)).unwrap_or(u64::MAX);
    let quote_added = asset_from_pair(&datum.quote_policy, &datum.quote_name, quote_added_qty);
    let base_added = asset_from_pair(&datum.base_policy, &datum.base_name, base_added_qty);

    // LP token minted = supply delta on the consumed→produced
    // datum's totalLpTokens. Defensive saturating sub so a
    // datum-decode quirk doesn't panic the module.
    let lp_minted = pool_out
        .datum
        .total_lp_tokens
        .saturating_sub(datum.total_lp_tokens);
    let lp_received_asset = SwapAsset::Native {
        policy: hex::encode(&datum.lp_policy),
        asset_name_hex: hex::encode(&datum.lp_name),
        quantity: lp_minted,
    };

    let provider = find_lp_recipient(produced, &datum.lp_policy, &datum.lp_name);
    let provider_address = provider
        .as_ref()
        .map(|p| p.address.clone())
        .unwrap_or_default();

    let batcher_fee_lovelace = derive_batcher_fee_lovelace(
        consumed_orders,
        // The "user paid" side for an LP add is the quote leg
        // (typically ADA) — same accounting as a quote-in swap.
        &quote_added,
        provider.as_ref(),
    );

    emit_event(&DexAction::LiquidityAdd(LiquidityAdd {
        tx_hash: tx_hash_hex.to_string(),
        slot,
        dex_brand: DEX_BRAND.to_string(),
        contract_version: None,
        provider_address,
        quote_added,
        base_added,
        lp_received: lp_received_asset,
        pool_id: Some(pool_id_for_pair(datum)),
        pool_reserves_before: reserves_before,
        pool_reserves_after: reserves_after,
        pool_fee_bps: u32::try_from(datum.pool_fee_bps).ok(),
        batcher_fee_lovelace,
    }));
}

#[allow(clippy::too_many_arguments)]
fn emit_liquidity_remove(
    tx_hash_hex: &str,
    slot: u64,
    datum: &CswapPoolDatum,
    delta_quote: i128,
    delta_base: i128,
    pool_out: &PoolOutput,
    produced: &[ProducedEvent],
    consumed: &[ConsumedEvent],
    consumed_orders: &[ConsumedEvent],
    reserves_before: Option<PoolReserves>,
    reserves_after: Option<PoolReserves>,
) {
    // For zap-out (asymmetric) removes, one side's delta may
    // be zero — the user took the proceeds entirely on the
    // other side. Negate the negative pool-delta to get the
    // amount the user received.
    let quote_withdrawn_qty = u64::try_from((-delta_quote).max(0)).unwrap_or(u64::MAX);
    let base_withdrawn_qty = u64::try_from((-delta_base).max(0)).unwrap_or(u64::MAX);
    let quote_withdrawn =
        asset_from_pair(&datum.quote_policy, &datum.quote_name, quote_withdrawn_qty);
    let base_withdrawn =
        asset_from_pair(&datum.base_policy, &datum.base_name, base_withdrawn_qty);

    let lp_burnt = datum
        .total_lp_tokens
        .saturating_sub(pool_out.datum.total_lp_tokens);
    let lp_burnt_asset = SwapAsset::Native {
        policy: hex::encode(&datum.lp_policy),
        asset_name_hex: hex::encode(&datum.lp_name),
        quantity: lp_burnt,
    };

    // Recipient identification:
    //   1) Balanced remove → wallet output holds the base
    //      asset (preferred — most specific match).
    //   2) Asymmetric zap-out → base side wasn't withdrawn, so
    //      address-by-asset fails. Fall through to "user wallet
    //      output whose lovelace lands within ~5 ADA of the
    //      quote_withdrawn" — the user's proceeds get a ~2 ADA
    //      min-utxo overhead, comfortably inside the tolerance.
    let _ = consumed;
    let provider_address = find_wallet_with_asset(produced, &datum.base_policy, &datum.base_name)
        .map(|r| r.address)
        .or_else(|| find_zap_recipient(produced, quote_withdrawn_qty))
        .unwrap_or_default();

    // No clean batcher-fee derivation yet for LP removes —
    // the order-deposit accounting differs from swaps (user's
    // input includes their LP tokens, not deposit lovelace).
    // Leave as `None` until we have a fixture to characterise.
    let _ = consumed_orders;
    let batcher_fee_lovelace = None;

    emit_event(&DexAction::LiquidityRemove(LiquidityRemove {
        tx_hash: tx_hash_hex.to_string(),
        slot,
        dex_brand: DEX_BRAND.to_string(),
        contract_version: None,
        provider_address,
        lp_burnt: lp_burnt_asset,
        quote_withdrawn,
        base_withdrawn,
        pool_id: Some(pool_id_for_pair(datum)),
        pool_reserves_before: reserves_before,
        pool_reserves_after: reserves_after,
        pool_fee_bps: u32::try_from(datum.pool_fee_bps).ok(),
        batcher_fee_lovelace,
    }));
}

/// Dispatch on the LP-token delta sign in the farm UTxO.
/// Positive delta → `FarmStake`; negative → `FarmUnstake`.
/// Zero or all-zero deltas (e.g. a farm-config update that
/// touches the UTxO without moving LP) get dropped with a log.
fn emit_for_farm_pair(
    tx_hash_hex: &str,
    slot: u64,
    fi: &ConsumedEvent,
    fo: &ProducedEvent,
    consumed: &[ConsumedEvent],
    produced: &[ProducedEvent],
) {
    // Find the asset whose quantity changed across the farm
    // UTxO's consume/produce. The farm carries a per-farm NFT
    // (qty 1, unchanged) alongside the staked LP token (qty
    // changes by ±delta). Walking unique (policy, name) pairs
    // and selecting the one with a non-zero delta isolates the
    // LP token without us needing to know the farm-NFT policy.
    let Some((lp_policy, lp_name, delta)) = farm_lp_delta(&fi.prior_output, &fo.output) else {
        logging::log(
            LogLevel::Info,
            LOG_TARGET,
            &format!(
                "tx={tx_hash_hex}: farm UTxO transition with no LP-token delta, skipping"
            ),
        );
        return;
    };

    let abs = u64::try_from(delta.unsigned_abs()).unwrap_or(u64::MAX);
    let lp_token = SwapAsset::Native {
        policy: hex::encode(&lp_policy),
        asset_name_hex: hex::encode(&lp_name),
        quantity: abs,
    };

    if delta > 0 {
        // FarmStake: user wallet INPUT carried the LP token in.
        let staker_address = find_consumed_wallet_with_asset(consumed, &lp_policy, &lp_name)
            .unwrap_or_default();
        emit_event(&DexAction::FarmStake(FarmStake {
            tx_hash: tx_hash_hex.to_string(),
            slot,
            dex_brand: DEX_BRAND.to_string(),
            contract_version: None,
            staker_address,
            lp_token,
        }));
    } else {
        // FarmUnstake: user wallet OUTPUT received the LP back.
        let staker_address = find_wallet_with_asset(produced, &lp_policy, &lp_name)
            .map(|s| s.address)
            .unwrap_or_default();
        emit_event(&DexAction::FarmUnstake(FarmUnstake {
            tx_hash: tx_hash_hex.to_string(),
            slot,
            dex_brand: DEX_BRAND.to_string(),
            contract_version: None,
            staker_address,
            lp_token,
        }));
    }
}

/// Walk the union of native assets in the farm's consumed +
/// produced UTxO; return the first one with a non-zero quantity
/// delta. CSWAP farms hold a single LP token + a farm NFT
/// (qty 1, unchanged), so the non-zero delta uniquely picks the
/// LP token without naming the farm-NFT policy.
fn farm_lp_delta(
    prior: &TypedOutput,
    new: &TypedOutput,
) -> Option<(Vec<u8>, Vec<u8>, i128)> {
    let mut seen: BTreeMap<(Vec<u8>, Vec<u8>), i128> = BTreeMap::new();
    for e in &prior.assets {
        *seen
            .entry((e.asset.policy.clone(), e.asset.name.clone()))
            .or_insert(0) -= e.quantity as i128;
    }
    for e in &new.assets {
        *seen
            .entry((e.asset.policy.clone(), e.asset.name.clone()))
            .or_insert(0) += e.quantity as i128;
    }
    seen.into_iter()
        .find(|(_, d)| *d != 0)
        .map(|((p, n), d)| (p, n, d))
}

/// Emit one `RewardClaim` per non-operator wallet output that
/// received native assets in a harvest-distribution TX.
/// Operator-managed wallets (`TREASURY_WALLET`) are filtered
/// out — their outputs are operational change, not rewards. The
/// detection-signal is the request-collection 2 ADA consume,
/// not the output addresses themselves, so this stays
/// distribution-shape agnostic.
fn emit_reward_claims(tx_hash_hex: &str, slot: u64, produced: &[ProducedEvent]) {
    for p in produced {
        let addr = &p.output.address;
        if !is_user_wallet(addr) || addr == TREASURY_WALLET || addr == REWARD_REQUEST_WALLET {
            continue;
        }
        let rewards: Vec<SwapAsset> = p
            .output
            .assets
            .iter()
            .map(|e| SwapAsset::Native {
                policy: hex::encode(&e.asset.policy),
                asset_name_hex: hex::encode(&e.asset.name),
                quantity: e.quantity,
            })
            .collect();
        if rewards.is_empty() {
            continue;
        }
        emit_event(&DexAction::RewardClaim(RewardClaim {
            tx_hash: tx_hash_hex.to_string(),
            slot,
            dex_brand: DEX_BRAND.to_string(),
            contract_version: None,
            claimer_address: addr.clone(),
            rewards,
        }));
    }
}

/// First user-wallet consumed event that held the named asset.
/// Used to identify the FarmStake staker from a wallet that
/// surrendered LP tokens into the farm.
fn find_consumed_wallet_with_asset(
    consumed: &[ConsumedEvent],
    policy: &[u8],
    name: &[u8],
) -> Option<String> {
    for c in consumed {
        if !is_user_wallet(&c.prior_output.address) {
            continue;
        }
        if c.prior_output
            .assets
            .iter()
            .any(|e| e.asset.policy == policy && e.asset.name == name)
        {
            return Some(c.prior_output.address.clone());
        }
    }
    None
}

/// User wallet output whose lovelace is closest to the target
/// — used to identify the LP-remove recipient on an asymmetric
/// (zap-out) flow where the user wallet doesn't carry the base
/// asset on the way out. The user's "min_receive ADA" lands as
/// `target_lovelace + min_utxo_overhead`, which sits within
/// `TOLERANCE` of the actual zap-out quote_withdrawn. The
/// batcher's own change wallet (which receives ADA orders of
/// magnitude larger or smaller than the swap proceeds) is
/// implicitly excluded by the tolerance band.
const ASYMMETRIC_RECIPIENT_TOLERANCE: u64 = 5_000_000;

fn find_zap_recipient(produced: &[ProducedEvent], target_lovelace: u64) -> Option<String> {
    let target = target_lovelace as i128;
    let tol = ASYMMETRIC_RECIPIENT_TOLERANCE as i128;
    produced
        .iter()
        .filter(|p| is_user_wallet(&p.output.address))
        .map(|p| ((p.output.lovelace as i128 - target).abs(), &p.output.address))
        .filter(|(d, _)| *d <= tol)
        .min_by_key(|(d, _)| *d)
        .map(|(_, addr)| addr.clone())
}

/// Sum a pool UTxO's holding of the asset identified by
/// `(policy, name)`. Empty policy + empty name = lovelace; the
/// CSWAP datum encodes ADA-paired pools this way.
fn pool_value(out: &TypedOutput, policy: &[u8], name: &[u8]) -> u64 {
    if policy.is_empty() && name.is_empty() {
        return out.lovelace;
    }
    out.assets
        .iter()
        .filter(|e| e.asset.policy == policy && e.asset.name == name)
        .map(|e| e.quantity)
        .sum()
}

fn asset_from_pair(policy: &[u8], name: &[u8], quantity: u64) -> SwapAsset {
    if policy.is_empty() && name.is_empty() {
        SwapAsset::Lovelace { quantity }
    } else {
        SwapAsset::Native {
            policy: hex::encode(policy),
            asset_name_hex: hex::encode(name),
            quantity,
        }
    }
}

/// First produced output at a user-wallet address that carries
/// `asset_out`. Phase 1: single-user assumption — batcher TXs
/// with multiple fills will return the first matching wallet,
/// which is a known limitation to be addressed once we have a
/// batcher-fill golden fixture (the per-order datums govern how
/// to split fills cleanly).
///
/// Returns the address *and* the output's lovelace, because the
/// latter is the user's `min_receive` ADA leg — needed for the
/// `batcher_fee_lovelace` derivation.
fn find_swapper(produced: &[ProducedEvent], asset_out: &SwapAsset) -> Option<SwapperInfo> {
    for p in produced {
        if !is_user_wallet(&p.output.address) {
            continue;
        }
        if output_contains(&p.output, asset_out) {
            return Some(SwapperInfo {
                address: p.output.address.clone(),
                output_lovelace: p.output.lovelace,
            });
        }
    }
    None
}

struct SwapperInfo {
    address: String,
    /// Lovelace held in the recipient's wallet output. For
    /// ADA-in swaps this is the protocol-mandated ~2 ADA
    /// `min_receive` ADA leg returned alongside the bought
    /// tokens. For LP-add fills it's the min-utxo ADA on the
    /// LP-token output. Either way: the user's residual ADA
    /// in this TX.
    output_lovelace: u64,
}

/// First user-wallet produced output containing the LP token.
/// Used by `LiquidityAdd` to identify the provider.
fn find_lp_recipient(
    produced: &[ProducedEvent],
    lp_policy: &[u8],
    lp_name: &[u8],
) -> Option<SwapperInfo> {
    find_wallet_with_asset(produced, lp_policy, lp_name)
}

/// First user-wallet produced output that carries the named
/// native asset. Generic helper used by LP add/remove to
/// identify the participating user.
fn find_wallet_with_asset(
    produced: &[ProducedEvent],
    policy: &[u8],
    name: &[u8],
) -> Option<SwapperInfo> {
    for p in produced {
        if !is_user_wallet(&p.output.address) {
            continue;
        }
        if p.output
            .assets
            .iter()
            .any(|e| e.asset.policy == policy && e.asset.name == name)
        {
            return Some(SwapperInfo {
                address: p.output.address.clone(),
                output_lovelace: p.output.lovelace,
            });
        }
    }
    None
}

/// Compute the batcher's lovelace cut for a single-order
/// batcher-routed ADA-in fill:
///
/// ```text
/// batcher_fee = sum(order_deposits) − pool_quote_inflow − swapper_min_utxo_back
/// ```
///
/// Returns `None` when we can't confidently attribute the fee:
///
/// - **No consumed orders** → either a pool-direct swap (no
///   batcher hop) or a flow we haven't characterised yet.
/// - **Multiple consumed orders** → batcher-multi-fill TX;
///   per-user fee splitting needs the order datums. Deferred
///   until we have a multi-fill golden fixture in hand.
/// - **Non-Lovelace `asset_in`** → token→ADA / token→token; the
///   ADA-flow arithmetic doesn't apply directly. Deferred.
/// - **No identifiable swapper output** → can't subtract the
///   `min_receive` ADA leg, so the residual would conflate
///   batcher fee + min-utxo carryover.
/// - **Arithmetic underflow** → defensive guard; means our
///   understanding of the flow is off for this TX shape.
fn derive_batcher_fee_lovelace(
    consumed_orders: &[ConsumedEvent],
    asset_in: &SwapAsset,
    swapper: Option<&SwapperInfo>,
) -> Option<u64> {
    if consumed_orders.len() != 1 {
        return None;
    }
    let SwapAsset::Lovelace {
        quantity: pool_inflow,
    } = asset_in
    else {
        return None;
    };
    let swapper = swapper?;
    let deposit = consumed_orders[0].prior_output.lovelace;
    deposit
        .checked_sub(*pool_inflow)
        .and_then(|x| x.checked_sub(swapper.output_lovelace))
}

fn is_user_wallet(addr: &str) -> bool {
    MAINNET_USER_PREFIXES.iter().any(|p| addr.starts_with(p))
        || TESTNET_USER_PREFIXES.iter().any(|p| addr.starts_with(p))
}

fn output_contains(out: &TypedOutput, asset: &SwapAsset) -> bool {
    match asset {
        SwapAsset::Lovelace { quantity } => out.lovelace >= *quantity / 2, // tolerate dust + min-utxo
        SwapAsset::Native {
            policy,
            asset_name_hex,
            ..
        } => {
            let policy_bytes = match hex::decode(policy) {
                Ok(b) => b,
                Err(_) => return false,
            };
            let name_bytes = match hex::decode(asset_name_hex) {
                Ok(b) => b,
                Err(_) => return false,
            };
            out.assets
                .iter()
                .any(|e| e.asset.policy == policy_bytes && e.asset.name == name_bytes)
        }
    }
}

/// Stable, sorted pair fingerprint. Different pools / contract
/// versions for the same asset pair produce the same `pool_id`
/// so cross-DEX consumers can union events without fragmenting.
/// Format: `<side_a>|<side_b>` where each side is either
/// `"lovelace"` (ADA / empty-policy empty-name) or
/// `<policy_hex>:<name_hex>`. Sides are sorted alphabetically,
/// and `"lovelace"` (starts with 'l') sorts after any hex side
/// (max hex char is 'f'), giving a consistent `<token>|lovelace`
/// shape for ADA-paired pools.
fn pool_id_for_pair(d: &CswapPoolDatum) -> String {
    let a = pair_side(&d.quote_policy, &d.quote_name);
    let b = pair_side(&d.base_policy, &d.base_name);
    let mut sides = [a, b];
    sides.sort();
    format!("{}|{}", sides[0], sides[1])
}

fn pair_side(policy: &[u8], name: &[u8]) -> String {
    if policy.is_empty() && name.is_empty() {
        "lovelace".to_string()
    } else {
        format!("{}:{}", hex::encode(policy), hex::encode(name))
    }
}

fn emit_event(event: &DexAction) {
    let mut buf = Vec::with_capacity(512);
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode DexAction failed: {e}"),
        );
        return;
    }
    emit::emit_event(0, &buf);
}

fn resolve_datum_bytes(d: &TypedDatum) -> Option<Vec<u8>> {
    if !d.payload.is_empty() {
        return Some(d.payload.clone());
    }
    crate::mitos::platform_v2::chain_data::datum_by_hash(&d.hash)
}

fn chain_point_slot(cp: &ChainPoint) -> Option<u64> {
    match cp {
        ChainPoint::Origin => None,
        ChainPoint::SlotOnly(s) => Some(*s),
        ChainPoint::Specific(p) => Some(p.slot),
    }
}

// ============================================================
// Datum decoder
// ============================================================

/// Decoded CSWAP pool datum. Lifted from
/// `shared-crates/cardano-tx/dex/cswap/pool.rs` and extended to
/// surface every field the action-recognition pipeline needs:
/// the four pair-identity fields (the shared decoder skips
/// them — TX-builder side only needs total LP + fee), the
/// LP-token identity (used for LiquidityAdd/Remove to find
/// the recipient wallet output), and the total LP supply
/// (delta over consume/produce gives `lp_received` /
/// `lp_burnt` directly without watching mint events).
#[derive(Debug, Clone)]
struct CswapPoolDatum {
    total_lp_tokens: u64,
    pool_fee_bps: u64,
    quote_policy: Vec<u8>,
    quote_name: Vec<u8>,
    base_policy: Vec<u8>,
    base_name: Vec<u8>,
    lp_policy: Vec<u8>,
    lp_name: Vec<u8>,
}

impl CswapPoolDatum {
    fn pair_key(&self) -> PairKey {
        (
            self.quote_policy.clone(),
            self.quote_name.clone(),
            self.base_policy.clone(),
            self.base_name.clone(),
        )
    }
}

fn decode_pool_datum(cbor: &[u8]) -> Option<CswapPoolDatum> {
    let pd: PlutusData = minicbor::decode(cbor).ok()?;
    let constr = match pd {
        PlutusData::Constr(c) => c,
        _ => return None,
    };
    // CSWAP pool datum: Constr tag 121 (== alternative 0), 8
    // fields. Tag check guards against unrelated PlutusData
    // landing at the pool address (shouldn't happen in normal
    // CSWAP operation, but defensive).
    if constr.tag != 121 {
        return None;
    }
    let fields: Vec<PlutusData> = constr.fields.into();
    if fields.len() != 8 {
        return None;
    }
    let total_lp_tokens = bigint_u64(&fields[0])?;
    let pool_fee_bps = bigint_u64(&fields[1])?;
    let quote_policy = bounded_bytes(&fields[2])?;
    let quote_name = bounded_bytes(&fields[3])?;
    let base_policy = bounded_bytes(&fields[4])?;
    let base_name = bounded_bytes(&fields[5])?;
    let lp_policy = bounded_bytes(&fields[6])?;
    let lp_name = bounded_bytes(&fields[7])?;
    Some(CswapPoolDatum {
        total_lp_tokens,
        pool_fee_bps,
        quote_policy,
        quote_name,
        base_policy,
        base_name,
        lp_policy,
        lp_name,
    })
}

fn bigint_u64(pd: &PlutusData) -> Option<u64> {
    match pd {
        PlutusData::BigInt(BigInt::Int(i)) => {
            let v: i128 = (*i).into();
            if v < 0 {
                None
            } else {
                u64::try_from(v).ok()
            }
        }
        PlutusData::BigInt(BigInt::BigUInt(b)) => {
            let bytes: &[u8] = b;
            if bytes.len() > 8 {
                return None;
            }
            let mut buf = [0u8; 8];
            buf[8 - bytes.len()..].copy_from_slice(bytes);
            Some(u64::from_be_bytes(buf))
        }
        _ => None,
    }
}

fn bounded_bytes(pd: &PlutusData) -> Option<Vec<u8>> {
    match pd {
        PlutusData::BoundedBytes(b) => Some((**b).to_vec()),
        _ => None,
    }
}

// ============================================================
// v2 Guest impl
// ============================================================

struct Module;

impl Guest for Module {
    fn module_version() -> (u32, u32) {
        (2, 0)
    }

    fn trap_policy() -> (TrapStrategy, RetryPolicy) {
        (
            TrapStrategy::Replay,
            RetryPolicy {
                max_retries: 3,
                backoff_cap_ms: 1_000,
            },
        )
    }

    fn init(config: Vec<u8>) {
        logging::log(
            LogLevel::Info,
            LOG_TARGET,
            &format!("init: enter (config_bytes={})", config.len()),
        );
    }

    fn handle_events(events: Vec<DispatchEvent>) {
        let mut buf = TxBuffer::default();
        for event in events {
            match event {
                DispatchEvent::Utxo(UtxoEvent::Produced(p)) => handle_produced(&p, &mut buf),
                DispatchEvent::Utxo(UtxoEvent::Consumed(c)) => handle_consumed(&c, &mut buf),
                _ => {}
            }
        }
        flush_buffer(buf);
    }

    fn update_interest(_op: InterestOp, _items_cbor: Vec<u8>) -> Result<(), String> {
        // Interest is fully static (declared in cswap_dex.toml).
        Ok(())
    }
}

export!(Module);
