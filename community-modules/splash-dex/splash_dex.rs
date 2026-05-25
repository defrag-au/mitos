//! Splash DEX swap-recognition community module.
//!
//! Emits `DexAction::{Swap, LiquidityAdd, LiquidityRemove,
//! OrderCancel}` for activity on Splash's V3 pool + order
//! contracts. Splash's design differs from CSWAP in two
//! load-bearing ways that ripple through the module:
//!
//! 1. **LP-token supply is held in the pool, not the user.**
//!    Splash pre-mints `u64::MAX` LP tokens at pool creation
//!    and stores them at the pool address. Each
//!    `LiquidityAdd` transfers some to the user (pool's LP
//!    holdings *decrease*); each `LiquidityRemove` reclaims
//!    them (pool's LP holdings *increase*). The
//!    user-circulation supply is `u64::MAX − pool_lp_holdings`.
//!    Our `LiquidityAdd` / `LiquidityRemove` dispatch therefore
//!    inverts the sign of `pool_lp_delta` vs the CSWAP module.
//!
//! 2. **The order script address varies by user.** Each user's
//!    SpotOrderV3 UTxO carries their own stake credential glued
//!    to the global SpotOrderV3 payment script. We can't
//!    enumerate all order addresses, so we prefix-match on the
//!    payment-cred-bearing 51-char prefix. The pool address is
//!    fixed (script payment + script stake).
//!
//! ## Pool datum (Constr 0, 14 fields)
//!
//! ```text
//! [0]  Constr 0 [Bytes(pool_nft_policy), Bytes(pool_nft_name)]
//! [1]  Constr 0 [Bytes(quote_policy),    Bytes(quote_name)]
//! [2]  Constr 0 [Bytes(base_policy),     Bytes(base_name)]
//! [3]  Constr 0 [Bytes(lp_policy),       Bytes(lp_name)]
//! [4]  Int                                  // pool param (TBD)
//! [5]  Int                                  // pool fee num A
//! [6]  Int                                  // pool fee num B
//! [7..10] Ints                              // counters / config (TBD)
//! [11] List of Constr                       // executor allowlist
//! [12] Bytes                                // admin / treasury pkh
//! [13] Bytes(32)                            // some hash (TBD)
//! [14] Int(3)                               // version marker
//! ```
//!
//! Field [5] is surfaced as `pool_fee_bps` for now (empirically
//! 50 across observed pools; doesn't exactly match the
//! constant-product residual but is the documented LP-side fee).
//!
//! ## OrderCancel
//!
//! When a SpotOrderV3 UTxO is consumed with redeemer `d87980`
//! (Constr 0, no fields), the order has been cancelled by its
//! owner. The order datum's destination address gives the
//! canceller. We don't emit `OrderCancel` until we have a real
//! Splash cancel fixture in hand — pending user supply.

use std::collections::BTreeMap;

use mitos_community_events::dex::{
    DexAction, LiquidityAdd, LiquidityRemove, PoolReserves, Swap, SwapAsset,
};
use mitos_dex_decode::splash::{ORDER_SCRIPT_ADDR_PREFIX, POOL_SCRIPT_ADDR};
use pallas_codec::minicbor;
use pallas_primitives::{BigInt, PlutusData};

use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::types::{
    ChainPoint, ConsumedEvent, ProducedEvent, TypedDatum, TypedOutput, UtxoEvent,
};

const LOG_TARGET: &str = "splash-dex-module";

const DEX_BRAND: &str = "Splash";

// Splash pool / order script addresses live in
// `mitos_dex_decode::splash` (imported above) so `splash-dex`
// and `holder-distribution` (for `DexPool` role tagging) share
// one source of truth.

/// Cancel-redeemer for SpotOrderV3 (Constr 0 with no fields).
/// Used by `OrderCancel` detection (pending fixture).
#[allow(dead_code)]
const CANCEL_REDEEMER: &[u8] = &[0xd8, 0x79, 0x80];

use mitos_dex_decode::originator::{find_originator as decode_find_originator, is_user_wallet};

/// One pool input captured from the event stream.
struct PoolInput {
    datum: SplashPoolDatum,
    prior_output: TypedOutput,
}

/// One pool output captured from the event stream. Datum is
/// retained for the LP-supply delta (Splash holds LP in pool,
/// so we read its quantity from both prior and produced UTxOs;
/// the datum is consulted for LP-token identity).
struct PoolOutput {
    #[allow(dead_code)]
    datum: SplashPoolDatum,
    output: TypedOutput,
}

#[derive(Default)]
struct TxBuffer {
    pool_inputs: BTreeMap<PairKey, PoolInput>,
    pool_outputs: BTreeMap<PairKey, PoolOutput>,
    produced: Vec<ProducedEvent>,
    /// All consumed events — kept generic so future variants
    /// (OrderCancel etc.) can inspect them without a buffer
    /// schema change. `emit_swap`'s `find_originator` consumes
    /// this to surface the user-funded address on
    /// aggregator-routed swaps.
    consumed: Vec<ConsumedEvent>,
    tx_hash: Option<Vec<u8>>,
    slot: Option<u64>,
}

/// `(quote_policy, quote_name, base_policy, base_name)` —
/// stable pair identity from the pool datum.
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
            continue;
        };
        emit_for_pool_pair(
            &tx_hash_hex,
            slot,
            &pool_in,
            &pool_out,
            &buf.produced,
            &buf.consumed,
        );
    }
}

fn emit_for_pool_pair(
    tx_hash_hex: &str,
    slot: u64,
    pool_in: &PoolInput,
    pool_out: &PoolOutput,
    produced: &[ProducedEvent],
    consumed: &[ConsumedEvent],
) {
    let datum = &pool_in.datum;

    let quote_before = pool_value(&pool_in.prior_output, &datum.quote_policy, &datum.quote_name);
    let base_before = pool_value(&pool_in.prior_output, &datum.base_policy, &datum.base_name);
    let quote_after = pool_value(&pool_out.output, &datum.quote_policy, &datum.quote_name);
    let base_after = pool_value(&pool_out.output, &datum.base_policy, &datum.base_name);

    let delta_quote = quote_after as i128 - quote_before as i128;
    let delta_base = base_after as i128 - base_before as i128;

    // Splash holds the LP token in the pool, so:
    //   pool_lp_before > pool_lp_after  → LP minted to user (Add)
    //   pool_lp_before < pool_lp_after  → LP reclaimed by pool (Remove)
    //   equal                           → Swap
    // The supply-delta-for-users is the negative of pool's
    // own delta — same sign convention as cswap-dex once
    // flipped.
    let pool_lp_before = pool_value(&pool_in.prior_output, &datum.lp_policy, &datum.lp_name);
    let pool_lp_after = pool_value(&pool_out.output, &datum.lp_policy, &datum.lp_name);
    let lp_delta_for_users = pool_lp_before as i128 - pool_lp_after as i128;

    let reserves_before = Some(PoolReserves {
        base_asset: asset_from_pair(&datum.base_policy, &datum.base_name, base_before),
        quote_asset: asset_from_pair(&datum.quote_policy, &datum.quote_name, quote_before),
    });
    let reserves_after = Some(PoolReserves {
        base_asset: asset_from_pair(&datum.base_policy, &datum.base_name, base_after),
        quote_asset: asset_from_pair(&datum.quote_policy, &datum.quote_name, quote_after),
    });

    match lp_delta_for_users.signum() {
        1 => emit_liquidity_add(
            tx_hash_hex,
            slot,
            datum,
            delta_quote,
            delta_base,
            lp_delta_for_users as u64,
            produced,
            reserves_before,
            reserves_after,
        ),
        -1 => emit_liquidity_remove(
            tx_hash_hex,
            slot,
            datum,
            delta_quote,
            delta_base,
            (-lp_delta_for_users) as u64,
            produced,
            reserves_before,
            reserves_after,
        ),
        _ => match (delta_quote.signum(), delta_base.signum()) {
            (1, -1) | (-1, 1) => emit_swap(
                tx_hash_hex,
                slot,
                datum,
                delta_quote,
                delta_base,
                produced,
                consumed,
                reserves_before,
                reserves_after,
            ),
            _ => logging::log(
                LogLevel::Info,
                LOG_TARGET,
                &format!(
                    "tx={tx_hash_hex}: unrecognised pool transition (lp_delta={lp_delta_for_users}, delta_quote={delta_quote}, delta_base={delta_base}), skipping"
                ),
            ),
        },
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_swap(
    tx_hash_hex: &str,
    slot: u64,
    datum: &SplashPoolDatum,
    delta_quote: i128,
    delta_base: i128,
    produced: &[ProducedEvent],
    consumed: &[ConsumedEvent],
    reserves_before: Option<PoolReserves>,
    reserves_after: Option<PoolReserves>,
) {
    let (asset_in, asset_out, in_qty, out_qty) = if delta_quote > 0 {
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

    let swapper_address = find_swapper(produced, &asset_out)
        .map(|s| s.address)
        .unwrap_or_default();

    // Aggregator-routed swaps land `asset_out` at a script
    // address (the routing wrapper's escrow), so `find_swapper`
    // returns None and `swapper_address` is empty. Surface the
    // dominant key-wallet input by lovelace as a best-effort
    // user-identity fallback. `None` for direct swaps (already
    // identified by `swapper_address`) and for swaps with no
    // recognisable key-wallet inputs.
    let originator_address = if swapper_address.is_empty() {
        find_originator(consumed)
    } else {
        None
    };

    emit_event(&DexAction::Swap(Swap {
        tx_hash: tx_hash_hex.to_string(),
        slot,
        dex_brand: DEX_BRAND.to_string(),
        contract_version: Some("V3".to_string()),
        swapper_address,
        originator_address,
        asset_in,
        asset_out,
        pool_id: Some(pool_id_for_pair(datum)),
        pool_reserves_before: reserves_before,
        pool_reserves_after: reserves_after,
        pool_fee_bps: u32::try_from(datum.pool_fee_bps).ok(),
        // Splash's executor fee lives in the order datum, not
        // the pool UTxO. Surfacing it cleanly requires the
        // order-datum decoder; deferred until we have an LP
        // fill fixture that exercises the decode path.
        batcher_fee_lovelace: None,
        effective_price: Some((out_qty, in_qty)),
    }));
}

/// Project the `ConsumedEvent` slice into the
/// `(address, lovelace)` shape `mitos-dex-decode::originator`
/// expects, then delegate to the shared helper. Returns the
/// owned `String` since `ConsumedEvent` is not `'static`.
fn find_originator(consumed: &[ConsumedEvent]) -> Option<String> {
    decode_find_originator(
        consumed
            .iter()
            .map(|c| (c.prior_output.address.as_str(), c.prior_output.lovelace)),
    )
    .map(String::from)
}

#[allow(clippy::too_many_arguments)]
fn emit_liquidity_add(
    tx_hash_hex: &str,
    slot: u64,
    datum: &SplashPoolDatum,
    delta_quote: i128,
    delta_base: i128,
    lp_minted: u64,
    produced: &[ProducedEvent],
    reserves_before: Option<PoolReserves>,
    reserves_after: Option<PoolReserves>,
) {
    let quote_added_qty = u64::try_from(delta_quote.max(0)).unwrap_or(u64::MAX);
    let base_added_qty = u64::try_from(delta_base.max(0)).unwrap_or(u64::MAX);
    let quote_added = asset_from_pair(&datum.quote_policy, &datum.quote_name, quote_added_qty);
    let base_added = asset_from_pair(&datum.base_policy, &datum.base_name, base_added_qty);
    let lp_received = SwapAsset::Native {
        policy: hex::encode(&datum.lp_policy),
        asset_name_hex: hex::encode(&datum.lp_name),
        quantity: lp_minted,
    };

    let provider_address = find_lp_recipient(produced, &datum.lp_policy, &datum.lp_name)
        .map(|p| p.address)
        .unwrap_or_default();

    emit_event(&DexAction::LiquidityAdd(LiquidityAdd {
        tx_hash: tx_hash_hex.to_string(),
        slot,
        dex_brand: DEX_BRAND.to_string(),
        contract_version: Some("V3".to_string()),
        provider_address,
        quote_added,
        base_added,
        lp_received,
        pool_id: Some(pool_id_for_pair(datum)),
        pool_reserves_before: reserves_before,
        pool_reserves_after: reserves_after,
        pool_fee_bps: u32::try_from(datum.pool_fee_bps).ok(),
        batcher_fee_lovelace: None,
    }));
}

#[allow(clippy::too_many_arguments)]
fn emit_liquidity_remove(
    tx_hash_hex: &str,
    slot: u64,
    datum: &SplashPoolDatum,
    delta_quote: i128,
    delta_base: i128,
    lp_burnt: u64,
    produced: &[ProducedEvent],
    reserves_before: Option<PoolReserves>,
    reserves_after: Option<PoolReserves>,
) {
    let quote_withdrawn_qty = u64::try_from((-delta_quote).max(0)).unwrap_or(u64::MAX);
    let base_withdrawn_qty = u64::try_from((-delta_base).max(0)).unwrap_or(u64::MAX);
    let quote_withdrawn =
        asset_from_pair(&datum.quote_policy, &datum.quote_name, quote_withdrawn_qty);
    let base_withdrawn =
        asset_from_pair(&datum.base_policy, &datum.base_name, base_withdrawn_qty);
    let lp_burnt_asset = SwapAsset::Native {
        policy: hex::encode(&datum.lp_policy),
        asset_name_hex: hex::encode(&datum.lp_name),
        quantity: lp_burnt,
    };

    let provider_address = find_wallet_with_asset(produced, &datum.base_policy, &datum.base_name)
        .map(|r| r.address)
        .or_else(|| find_zap_recipient(produced, quote_withdrawn_qty))
        .unwrap_or_default();

    emit_event(&DexAction::LiquidityRemove(LiquidityRemove {
        tx_hash: tx_hash_hex.to_string(),
        slot,
        dex_brand: DEX_BRAND.to_string(),
        contract_version: Some("V3".to_string()),
        provider_address,
        lp_burnt: lp_burnt_asset,
        quote_withdrawn,
        base_withdrawn,
        pool_id: Some(pool_id_for_pair(datum)),
        pool_reserves_before: reserves_before,
        pool_reserves_after: reserves_after,
        pool_fee_bps: u32::try_from(datum.pool_fee_bps).ok(),
        batcher_fee_lovelace: None,
    }));
}

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

struct SwapperInfo {
    address: String,
    #[allow(dead_code)]
    output_lovelace: u64,
}

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

fn output_contains(out: &TypedOutput, asset: &SwapAsset) -> bool {
    match asset {
        SwapAsset::Lovelace { quantity } => out.lovelace >= *quantity / 2,
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

fn find_lp_recipient(
    produced: &[ProducedEvent],
    lp_policy: &[u8],
    lp_name: &[u8],
) -> Option<SwapperInfo> {
    find_wallet_with_asset(produced, lp_policy, lp_name)
}

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

fn pool_id_for_pair(d: &SplashPoolDatum) -> String {
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
// Pool datum decoder
// ============================================================

/// Decoded Splash V3 pool datum. Only the fields we use are
/// surfaced; remaining ints (executor allowlist, treasury PKH,
/// 32-byte hash, version) are skipped at decode time.
#[derive(Debug, Clone)]
struct SplashPoolDatum {
    quote_policy: Vec<u8>,
    quote_name: Vec<u8>,
    base_policy: Vec<u8>,
    base_name: Vec<u8>,
    lp_policy: Vec<u8>,
    lp_name: Vec<u8>,
    /// LP-side swap fee in basis points. Surfaced from datum
    /// field [5]; doesn't exactly match the empirical effective
    /// fee (which combines LP fee + treasury fee), but is the
    /// documented value consumers compare against.
    pool_fee_bps: u64,
}

impl SplashPoolDatum {
    fn pair_key(&self) -> PairKey {
        (
            self.quote_policy.clone(),
            self.quote_name.clone(),
            self.base_policy.clone(),
            self.base_name.clone(),
        )
    }
}

/// Splash V3 pool datum: Constr 0 with 14 fields. We only need
/// fields [1] (quote asset), [2] (base asset), [3] (LP token),
/// and [5] (LP fee bps). The rest are skipped (executor list,
/// counters, etc.).
fn decode_pool_datum(cbor: &[u8]) -> Option<SplashPoolDatum> {
    let pd: PlutusData = minicbor::decode(cbor).ok()?;
    let constr = match pd {
        PlutusData::Constr(c) => c,
        _ => return None,
    };
    // Constr tag 121 = alternative 0.
    if constr.tag != 121 {
        return None;
    }
    let fields: Vec<PlutusData> = constr.fields.into();
    if fields.len() < 7 {
        return None;
    }
    let (quote_policy, quote_name) = asset_pair(&fields[1])?;
    let (base_policy, base_name) = asset_pair(&fields[2])?;
    let (lp_policy, lp_name) = asset_pair(&fields[3])?;
    let pool_fee_bps = bigint_u64(&fields[5])?;
    Some(SplashPoolDatum {
        quote_policy,
        quote_name,
        base_policy,
        base_name,
        lp_policy,
        lp_name,
        pool_fee_bps,
    })
}

/// Decode a `Constr 0 [Bytes(policy), Bytes(name)]` sub-datum
/// into `(policy, name)` byte tuples.
fn asset_pair(pd: &PlutusData) -> Option<(Vec<u8>, Vec<u8>)> {
    let constr = match pd {
        PlutusData::Constr(c) => c,
        _ => return None,
    };
    let inner: Vec<PlutusData> = constr.fields.clone().into();
    if inner.len() != 2 {
        return None;
    }
    let policy = bounded_bytes(&inner[0])?;
    let name = bounded_bytes(&inner[1])?;
    Some((policy, name))
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
        Ok(())
    }

    /// No-op: event-driven modules are refilled host-side by
    /// `run_bootstrap` over the manifest `[interest]`. See the
    /// `rebootstrap` export in `wit-v2/world.wit`. One call,
    /// immediately `done`.
    fn rebootstrap(_mode: RebootstrapMode) -> Result<RebootstrapStep, String> {
        Ok(RebootstrapStep {
            done: true,
            ingested: 0,
        })
    }
}

export!(Module);
