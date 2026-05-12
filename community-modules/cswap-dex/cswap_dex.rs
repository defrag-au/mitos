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

use std::collections::BTreeMap;

use mitos_community_events::dex::{DexAction, PoolReserves, Swap, SwapAsset};
use pallas_codec::minicbor;
use pallas_primitives::{BigInt, PlutusData};

use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::types::{
    ChainPoint, ConsumedEvent, ProducedEvent, TypedDatum, TypedOutput, UtxoEvent,
};

const LOG_TARGET: &str = "cswap-dex-module";

const DEX_BRAND: &str = "CSWAP";

/// CSWAP pool script address. All 82 pools live here.
const POOL_SCRIPT_ADDR: &str =
    "addr1z8ke0c9p89rjfwmuh98jpt8ky74uy5mffjft3zlcld9h7ml3lmln3mwk0y3zsh3gs3dzqlwa9rjzrxawkwm4udw9axhs6fuu6e";

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
/// is decoded only to derive `PairKey` for the BTreeMap; once
/// keyed, reserves_after comes from `output.lovelace + assets`
/// gated by the *consumed* pool's datum (we trust the produced
/// pool to be the same pair when the keys match).
struct PoolOutput {
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
    /// wallet output (the non-script recipient of asset_out).
    produced: Vec<ProducedEvent>,
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
        emit_for_pool_pair(&tx_hash_hex, slot, &pool_in, &pool_out, &buf.produced);
    }
    // Produced pools with no matching consumed (pool creation
    // TXs etc.) aren't swaps; ignore.
}

fn emit_for_pool_pair(
    tx_hash_hex: &str,
    slot: u64,
    pool_in: &PoolInput,
    pool_out: &PoolOutput,
    produced: &[ProducedEvent],
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

    // Directional check: exactly one side grew, one shrank.
    // Liquidity events (both sides grow), pool init, or
    // accounting weirdness don't qualify as swaps in Phase 1.
    if !((delta_quote > 0 && delta_base < 0) || (delta_quote < 0 && delta_base > 0)) {
        logging::log(
            LogLevel::Info,
            LOG_TARGET,
            &format!(
                "tx={tx_hash_hex}: non-swap pool transition (delta_quote={delta_quote}, delta_base={delta_base}), skipping"
            ),
        );
        return;
    }

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

    let swapper_address = find_swapper_address(produced, &asset_out).unwrap_or_default();

    let reserves_before = Some(PoolReserves {
        base_asset: asset_from_pair(&datum.base_policy, &datum.base_name, base_before),
        quote_asset: asset_from_pair(&datum.quote_policy, &datum.quote_name, quote_before),
    });
    let reserves_after = Some(PoolReserves {
        base_asset: asset_from_pair(&datum.base_policy, &datum.base_name, base_after),
        quote_asset: asset_from_pair(&datum.quote_policy, &datum.quote_name, quote_after),
    });

    let event = DexAction::Swap(Swap {
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
        // Batcher-fee extraction defers to a fixture — set
        // `None` until we observe a real batcher-routed TX
        // and decide how to surface the fee delta.
        batcher_fee_lovelace: None,
        effective_price: Some((out_qty, in_qty)),
    });

    emit_event(&event);
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

/// First produced output at a non-CSWAP-script address that
/// carries `asset_out`. Phase 1: single-user assumption — batcher
/// TXs with multiple fills will return the first matching wallet,
/// which is a known limitation to be addressed once we have a
/// batcher-fill golden fixture (the per-order datums govern how
/// to split fills cleanly).
fn find_swapper_address(produced: &[ProducedEvent], asset_out: &SwapAsset) -> Option<String> {
    for p in produced {
        if !is_user_wallet(&p.output.address) {
            continue;
        }
        if output_contains(&p.output, asset_out) {
            return Some(p.output.address.clone());
        }
    }
    None
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

/// Decoded CSWAP pool datum — pair identity + fee. Lifted from
/// `shared-crates/cardano-tx/dex/cswap/pool.rs` and extended to
/// extract the four pair-identity fields the shared decoder
/// doesn't surface (the builder side only needs LP + fee).
#[derive(Debug, Clone)]
struct CswapPoolDatum {
    pool_fee_bps: u64,
    quote_policy: Vec<u8>,
    quote_name: Vec<u8>,
    base_policy: Vec<u8>,
    base_name: Vec<u8>,
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
    let pool_fee_bps = bigint_u64(&fields[1])?;
    let quote_policy = bounded_bytes(&fields[2])?;
    let quote_name = bounded_bytes(&fields[3])?;
    let base_policy = bounded_bytes(&fields[4])?;
    let base_name = bounded_bytes(&fields[5])?;
    Some(CswapPoolDatum {
        pool_fee_bps,
        quote_policy,
        quote_name,
        base_policy,
        base_name,
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
