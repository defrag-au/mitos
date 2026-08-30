//! Pool recognition — the reserve curve's input.
//!
//! A pool output is recognised structurally: the watched asset sits at a known
//! DEX pool script. CSwap, Splash V3 and WingRiders V2 each put every pool at
//! one canonical address, so those are exact string matches. Minswap derives a
//! stake part per pool, so it is matched on the **payment credential** — a
//! full-address set would need an entry per pool and miss every new one.
//!
//! ## Reserves are sourced per DEX, and the difference is enormous
//!
//! There is no single right answer, which is why [`ReserveSource`] is recorded
//! on every row rather than assumed:
//!
//! | DEX | reserve is | why |
//! |---|---|---|
//! | CSwap, Splash | the UTxO value | the pool holds nothing else |
//! | Minswap V2 | the datum's `reserveA`/`reserveB` | the UTxO also holds an ADA deposit, accrued fees, its NFT and unissued LP |
//! | WingRiders V2 | value **minus** the declared treasuries | it publishes what it owes, not what it holds |
//!
//! Measured on chain 2026-08-30, reading value where the datum was
//! authoritative overstated one Minswap pool's token side **314 million-fold**
//! and one WingRiders pool's **156,000-fold**. A dead pool reads as deep
//! liquidity and nothing errors.
//!
//! ## Not every pool can price the token
//!
//! Only an ADA-paired pool can. Of 15 live WingRiders V2 pools sampled, just 4
//! were — the rest are token/token, whose lovelace is a min-UTxO carrier. Such
//! a pool still holds real supply and still counts toward the `pool` cohort;
//! it simply contributes nothing to the price. See
//! [`PoolObservation::ada_paired`].
//!
//! ## Identity, and how firmly it is known
//!
//! The pool *instance* key matters because one address holds many pools. CSwap
//! publishes it: the pool datum carries `lpTokenPolicy` + `lpTokenName`, and an
//! LP token is one-per-pool by construction. Splash has no datum decoder in
//! `mitos-dex-decode` yet (it still lives inline in the community module), so
//! its key is derived from the pool UTxO's own value — the single asset that is
//! neither ADA nor the watched token. That is a weaker claim, and [`KeyBasis`]
//! carries the difference rather than flattening it.

use mitos_chain_walk::decode::{Asset, DecodedOutput};
use mitos_dex_decode::cswap;

/// Where a row's reserves were read from.
///
/// Recorded per row rather than remembered, because the right answer differs
/// per DEX and getting it wrong is silent and enormous — measured on chain:
/// a Minswap V2 pool read from value overstated one side **314 million-fold**,
/// and a WingRiders pool read from value overstated the other **156,000-fold**.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReserveSource {
    /// The pool UTxO's own value. Correct only where the pool holds nothing
    /// but reserves — CSwap and Splash.
    Value,
    /// Published in the pool's own datum. Minswap V2, whose UTxO also carries
    /// an ADA deposit, accrued fees, its NFT and unissued LP.
    Datum,
    /// The UTxO's value less the non-tradeable balance the datum declares —
    /// WingRiders' `treasuryA`/`treasuryB`, SundaeSwap V3's `protocol_fees`.
    /// Both publish what they owe rather than what they hold.
    ValueMinusTreasury,
}

impl ReserveSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            ReserveSource::Value => "value",
            ReserveSource::Datum => "datum",
            ReserveSource::ValueMinusTreasury => "value-minus-treasury",
        }
    }
}

/// How firmly the pool instance is identified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyBasis {
    /// The LP asset named by the pool's own datum. One-per-pool by
    /// construction — the strong claim.
    Datum,
    /// The lone non-ADA, non-watched asset in the pool UTxO's value. Very
    /// likely the LP token or pool NFT, but inferred rather than published.
    Value,
    /// Several candidate assets in the value and no decoder to choose between
    /// them — a pool holding both an LP token and a pool NFT, typically. The
    /// pool is real and its reserves are right; only its *identity* is
    /// unresolved, which matters when one address holds several pools for the
    /// same token.
    Ambiguous,
    /// No candidate at all. Distinguished from [`KeyBasis::Ambiguous`] because
    /// the two want different fixes: this one says the pool shape is not what
    /// was assumed, that one says a decoder is missing.
    Unknown,
}

impl KeyBasis {
    pub fn as_str(&self) -> &'static str {
        match self {
            KeyBasis::Datum => "datum",
            KeyBasis::Value => "value",
            KeyBasis::Ambiguous => "ambiguous",
            KeyBasis::Unknown => "unknown",
        }
    }
}

/// One pool observation at one transaction.
pub struct PoolObservation {
    pub dex: &'static str,
    pub address: String,
    /// The pool-instance key — LP policy + name where known.
    pub key_policy: Vec<u8>,
    pub key_name: Vec<u8>,
    pub key_basis: KeyBasis,
    /// Whether the pool's other side is ADA.
    ///
    /// **Only an ADA-paired pool can price the token**, and this is not an
    /// edge case: of 15 live WingRiders V2 pools sampled 2026-08-30, only 4
    /// paired with ADA — the rest are token/token (NIGHT/IAG, EDM/HKDG,
    /// NIGHT/USDA, ßUSDM/iUSD…). A token/token pool's lovelace is its
    /// min-UTxO carrier, so reading it as a quote reserve produces a spot
    /// price wrong by orders of magnitude, silently.
    ///
    /// Such a pool still holds real supply and must still be counted in the
    /// `pool` cohort — it just cannot contribute to the price. The two uses
    /// are separated here rather than left for a caller to remember.
    pub ada_paired: bool,
    /// Watched-asset reserve.
    pub base_reserve: i64,
    /// Lovelace reserve.
    ///
    /// Includes the output's min-UTxO carrier ADA, which is a couple of ADA
    /// against pool reserves in the hundreds of thousands — under a
    /// thousandth of a percent on spot. Recorded whole rather than netted:
    /// deducting a carrier estimate from a real reserve is the kind of
    /// correction that is wrong more often than the error it fixes.
    pub quote_reserve: i64,
    pub fee_bps: Option<i64>,
    pub total_lp: Option<i64>,
    pub reserve_source: ReserveSource,
}

/// Recognise a pool output, if this is one.
///
/// `qty` is the watched-asset quantity already extracted by the walk.
pub fn recognise(
    out: &DecodedOutput,
    qty: i64,
    watched_policy: &[u8],
    watched_name: &[u8],
    witness_datum: Option<&[u8]>,
) -> Option<PoolObservation> {
    let datum = out.inline_datum.as_deref().or(witness_datum);

    // Minswap and WingRiders are matched on the PAYMENT credential — their
    // stake part is contract-derived per pool, so a full-address set would
    // need an entry each and miss every new one. CSwap and Splash genuinely
    // are single addresses.
    let cred = crate::cohort::payment_cred(&out.address);
    if let Some(cred) = cred {
        if mitos_dex_decode::minswap::is_minswap_v2(&cred) {
            return minswap_v2(out, qty, datum);
        }
        if mitos_dex_decode::wingriders::is_wingriders_v2(&cred) {
            return wingriders_v2(out, qty, datum, watched_policy, watched_name);
        }
        if mitos_dex_decode::splash::is_splash_pool(&cred) {
            return splash(out, qty, datum);
        }
        if mitos_dex_decode::sundae::is_sundae_v3(&cred) {
            return sundae_v3(out, qty, datum, watched_policy, watched_name);
        }
        // The three PlutusV1 contracts. Their datums are hash-committed rather
        // than inline, but the preimage rides in the CREATING transaction's
        // witness set — which `witness_datum` above already carries — so they
        // decode in the same pass as everything else. See `sundae.rs`'s module
        // docs for how that was established.
        if mitos_dex_decode::sundae::is_sundae_v1(&cred) {
            return sundae_v1(out, qty, datum, watched_policy, watched_name);
        }
        if mitos_dex_decode::minswap::is_minswap_v1(&cred) {
            return minswap_v1(out, qty, datum, watched_policy, watched_name);
        }
        if mitos_dex_decode::wingriders::is_wingriders_v1(&cred) {
            return wingriders_v1(out, qty, datum, watched_policy, watched_name);
        }
    }

    let dex = if out.address == cswap::POOL_SCRIPT_ADDR {
        "cswap"
    } else {
        return None;
    };

    // CSwap publishes its instance key, its fee and its LP supply. Take them.
    if dex == "cswap"
        && let Some(bytes) = datum
        && let Some(d) = cswap::decode_pool_datum(bytes)
    {
        // CSwap names its pair in the datum, so ADA-pairing is published
        // rather than inferred: ADA is the empty policy and empty name.
        let ada_paired = (d.quote_policy.is_empty() && d.quote_name.is_empty())
            || (d.base_policy.is_empty() && d.base_name.is_empty());
        return Some(PoolObservation {
            dex,
            address: out.address.clone(),
            key_policy: d.lp_policy,
            key_name: d.lp_name,
            key_basis: KeyBasis::Datum,
            ada_paired,
            base_reserve: qty,
            quote_reserve: out.lovelace as i64,
            fee_bps: Some(d.pool_fee_bps as i64),
            total_lp: Some(d.total_lp_tokens as i64),
            reserve_source: ReserveSource::Value,
        });
    }

    // Otherwise fall back to the value: the lone asset that is neither ADA nor
    // the token we are following.
    let (key_policy, key_name, key_basis) =
        match value_key(&out.assets, watched_policy, watched_name) {
            ValueKey::One(a) => (a.policy.clone(), a.name.clone(), KeyBasis::Value),
            ValueKey::Many => (Vec::new(), Vec::new(), KeyBasis::Ambiguous),
            ValueKey::None => (Vec::new(), Vec::new(), KeyBasis::Unknown),
        };

    Some(PoolObservation {
        dex,
        address: out.address.clone(),
        key_policy,
        key_name,
        key_basis,
        // Without a decoded datum the pair is unknown, so ADA-pairing is
        // INFERRED from the value: a pool holding meaningful ADA beyond its
        // min-UTxO carrier is ADA-paired. Conservative by design — a
        // token/token pool wrongly treated as ADA-paired would publish a spot
        // price off by orders of magnitude, whereas the reverse merely omits
        // it from the price while still counting its supply.
        ada_paired: out.lovelace as i64 > MIN_UTXO_CARRIER_CEILING,
        base_reserve: qty,
        quote_reserve: out.lovelace as i64,
        fee_bps: None,
        total_lp: None,
        reserve_source: ReserveSource::Value,
    })
}

/// Below this much lovelace, a pool's OWN spot price is not worth quoting.
///
/// ## What this does and does not gate
///
/// It gates *quoting a single pool*. It does **not** remove the pool's reserves
/// from the aggregate, and that distinction is the whole point.
///
/// The liquidity-weighted spot is `Σquote / Σbase` — a merged pool, not an
/// average of prices — so a dust pool already contributes dust to both sums.
/// Measured on WRT across 7 pools: applying this floor moves the aggregate by
/// **0.0027%** (cap 2,165,204 → 2,165,146 ADA). Reserve-summing *is* the
/// outlier defence, and it is structural rather than a filter.
///
/// So excluding thin pools from the aggregate would be pointless — and reaching
/// for a robust *statistic* instead is worse than the thing it replaces. On
/// those same 7 pools:
///
/// ```text
/// reserve-weighted (what we do)   0.02165204
/// unweighted MEDIAN               0.02307297   +6.6%
/// unweighted mean                69.28         3,200x
/// ```
///
/// The mean is destroyed by one dead pool, as expected. The median survives
/// that but is still 6.6% out, because with 7 pools it lands on a 7-ADA pool
/// and so answers "what does a typical pool think" when the question is "what
/// is this token worth". Depth is the information a median throws away, and
/// here depth spans five orders of magnitude. **Sum reserves; do not trim or
/// median across pool prices.**
///
/// ## Where a thin pool really does lie
///
/// Its own quoted price. Measured on WRT, per-pool spot against the real
/// 0.0217 ADA:
///
/// ```text
///      1.0 ADA deep  ->  484.74 ADA   22,388x
///      3.9 ADA       ->    0.0711         3.3x
///      7.1 ADA       ->    0.0231         1.07x
///     12.0 ADA       ->    0.0453         2.1x
///     22.1 ADA       ->    0.0217         1.003x
///  42,921 ADA        ->    0.0217         1.000x
/// ```
///
/// Errors above 2× stop by ~22 ADA, so 100 leaves an order of magnitude of
/// margin at no measurable cost to the aggregate. Note the band is not
/// monotonic — the 7-ADA pool is accurate and the 12-ADA one is 2× out — which
/// is why this is a floor below which a price is *withheld*, not a correction
/// applied to it.
pub const MIN_QUOTABLE_LOVELACE: i64 = 100_000_000;

/// Is this pool deep enough for its own spot price to mean anything?
///
/// A `false` pool is still a real pool: its supply counts, its reserves join
/// the aggregate. Only the per-pool price claim is withheld — the same split
/// as `ada_paired`, where counting supply and pricing it are separate claims.
pub fn is_quotable(quote_reserve: i64) -> bool {
    quote_reserve >= MIN_QUOTABLE_LOVELACE
}

/// Above this much lovelace, a pool output is holding ADA as a *reserve*
/// rather than as the carrier every token-bearing UTxO must pay.
///
/// Sampled token/token pools sit at 3 ADA; a real ADA reserve is orders of
/// magnitude above that. 10 ADA leaves headroom for a fatter carrier without
/// admitting a dead pool.
const MIN_UTXO_CARRIER_CEILING: i64 = 10_000_000;

enum ValueKey<'a> {
    One(&'a Asset),
    /// More than one candidate — reported rather than resolved by picking.
    Many,
    None,
}

/// Splash — reserves are the UTxO value; the datum names the pool.
///
/// Both known pool contracts decode the same way. The pool NFT is a genuine
/// one-per-pool instance key, which upgrades Splash from the value-inferred
/// `ambiguous` identity it had while only its address was known.
fn splash(out: &DecodedOutput, qty: i64, datum: Option<&[u8]>) -> Option<PoolObservation> {
    let d = datum.and_then(mitos_dex_decode::splash::decode_pool_datum);
    let (key_policy, key_name, key_basis, ada_paired) = match &d {
        Some(p) => (
            p.pool_nft.policy.clone(),
            p.pool_nft.name.clone(),
            KeyBasis::Datum,
            p.is_ada_paired(),
        ),
        // A pool we recognise by credential but cannot read. Reserves are
        // still the value — that part does not depend on the datum — so it is
        // recorded rather than dropped, with its identity marked unknown.
        None => (
            Vec::new(),
            Vec::new(),
            KeyBasis::Unknown,
            out.lovelace as i64 > MIN_UTXO_CARRIER_CEILING,
        ),
    };
    Some(PoolObservation {
        dex: "splash",
        address: out.address.clone(),
        key_policy,
        key_name,
        key_basis,
        ada_paired,
        base_reserve: qty,
        quote_reserve: out.lovelace as i64,
        fee_bps: None,
        total_lp: None,
        reserve_source: ReserveSource::Value,
    })
}

/// SundaeSwap V3 — reserves are the value less the declared `protocol_fees`.
///
/// The only remaining DEX whose pool state can be read straight off an unspent
/// output: 996 of 1,000 sampled V3 pool UTxOs carry an inline datum. It is also
/// the only one that hands over a usable **fee**, so its realisable figure is
/// not flagged optimistic the way Splash's and WingRiders' are.
fn sundae_v3(
    out: &DecodedOutput,
    qty: i64,
    datum: Option<&[u8]>,
    watched_policy: &[u8],
    watched_name: &[u8],
) -> Option<PoolObservation> {
    let d = datum.and_then(mitos_dex_decode::sundae::decode_v3_pool_datum)?;
    let watched_is_a = d.asset_a_policy == watched_policy && d.asset_a_name == watched_name;
    let (value_a, value_b) = if watched_is_a {
        (qty as u64, out.lovelace)
    } else {
        (out.lovelace, qty as u64)
    };
    let (base, quote, ada_paired) = match d.ada_pair(value_a, value_b) {
        Some((ada, token)) => (token, ada, true),
        // Token/token: real supply, no ADA price.
        None => (qty as u64, 0, false),
    };
    Some(PoolObservation {
        dex: "sundae-v3",
        address: out.address.clone(),
        // The pool script mints its own NFT, and the NFT's name is the datum's
        // `ident` behind a CIP-68 label — one per pool, so this is a genuine
        // instance key rather than the shared-policy `ambiguous` that Minswap
        // V2 and WingRiders V2 are still stuck on.
        key_policy: mitos_dex_decode::sundae::POOL_NFT_POLICY.to_vec(),
        key_name: d.nft_name(),
        key_basis: KeyBasis::Datum,
        ada_paired,
        base_reserve: i64::try_from(base).unwrap_or(i64::MAX),
        quote_reserve: i64::try_from(quote).unwrap_or(i64::MAX),
        // Sundae's fee is already in ten-thousandths, which is basis points.
        // The conservative side of bid/ask — they differ on 7 of 996 pools.
        fee_bps: Some(d.max_fee_per_10_thousand() as i64),
        total_lp: Some(i64::try_from(d.circulating_lp).unwrap_or(i64::MAX)),
        reserve_source: ReserveSource::ValueMinusTreasury,
    })
}

/// SundaeSwap V1 — reserves are the value; the datum carries an explicit fee.
///
/// No netting on either side: unlike V3 there is no `protocol_fees` field, and
/// unlike WingRiders there is no treasury. Checked rather than assumed — across
/// 10 live pools the value-sourced reserves fit the pool's own `total_lp` to a
/// median 1.0% under `sqrt(a·b)`.
fn sundae_v1(
    out: &DecodedOutput,
    qty: i64,
    datum: Option<&[u8]>,
    watched_policy: &[u8],
    watched_name: &[u8],
) -> Option<PoolObservation> {
    let d = datum.and_then(mitos_dex_decode::sundae::decode_v1_pool_datum)?;
    let watched_is_a = d.asset_a_policy == watched_policy && d.asset_a_name == watched_name;
    let (value_a, value_b) = if watched_is_a {
        (qty as u64, out.lovelace)
    } else {
        (out.lovelace, qty as u64)
    };
    let (base, quote, ada_paired) = match d.ada_pair(value_a, value_b) {
        Some((ada, token)) => (token, ada, true),
        None => (qty as u64, 0, false),
    };
    Some(PoolObservation {
        dex: "sundae-v1",
        address: out.address.clone(),
        key_policy: mitos_dex_decode::sundae::V1_NFT_POLICY.to_vec(),
        key_name: d.nft_name(),
        key_basis: KeyBasis::Datum,
        ada_paired,
        base_reserve: i64::try_from(base).unwrap_or(i64::MAX),
        quote_reserve: i64::try_from(quote).unwrap_or(i64::MAX),
        // A real fraction from the datum — 1/100, 3/1000 and 1/2000 all occur,
        // so this is computed, never assumed.
        fee_bps: d.fee_bps().map(|f| f as i64),
        total_lp: Some(i64::try_from(d.total_lp).unwrap_or(i64::MAX)),
        reserve_source: ReserveSource::Value,
    })
}

/// Minswap V1 — reserves are the value, less two bookkeeping tokens.
///
/// V1 publishes no reserves (V2's `reserveA`/`reserveB` arrived later), so the
/// value is the source. A V1 pool holds exactly four things: its pool NFT, the
/// `MINSWAP` factory token, and the two sides. The watched quantity the walk
/// extracted is already only the watched asset, so nothing needs filtering
/// here — but the pool NFT is the instance key, and that is worth taking.
fn minswap_v1(
    out: &DecodedOutput,
    qty: i64,
    datum: Option<&[u8]>,
    watched_policy: &[u8],
    watched_name: &[u8],
) -> Option<PoolObservation> {
    let d = datum.and_then(mitos_dex_decode::minswap::decode_v1_pool_datum)?;
    let watched_is_a = d.asset_a.policy == watched_policy && d.asset_a.name == watched_name;
    let (value_a, value_b) = if watched_is_a {
        (qty as u64, out.lovelace)
    } else {
        (out.lovelace, qty as u64)
    };
    let (base, quote, ada_paired) = match d.ada_pair(value_a, value_b) {
        Some((ada, token)) => (token, ada, true),
        None => (qty as u64, 0, false),
    };
    // One NFT per pool, so its name is a genuine instance key — better than the
    // shared-policy `ambiguous` V2 is still stuck on.
    let nft = out
        .assets
        .iter()
        .find(|a| a.policy == mitos_dex_decode::minswap::V1_POOL_NFT_POLICY);
    let (key_policy, key_name, key_basis) = match nft {
        Some(a) => (a.policy.clone(), a.name.clone(), KeyBasis::Value),
        None => (Vec::new(), Vec::new(), KeyBasis::Unknown),
    };
    Some(PoolObservation {
        dex: "minswap-v1",
        address: out.address.clone(),
        key_policy,
        key_name,
        key_basis,
        ada_paired,
        base_reserve: i64::try_from(base).unwrap_or(i64::MAX),
        quote_reserve: i64::try_from(quote).unwrap_or(i64::MAX),
        // V1's datum carries no fee — it was a protocol constant, and this
        // crate has not verified which. Reported unknown rather than guessed.
        fee_bps: None,
        total_lp: Some(i64::try_from(d.total_liquidity).unwrap_or(i64::MAX)),
        reserve_source: ReserveSource::Value,
    })
}

/// WingRiders V1 — the same value-minus-treasury rule as V2, nested deeper.
fn wingriders_v1(
    out: &DecodedOutput,
    qty: i64,
    datum: Option<&[u8]>,
    watched_policy: &[u8],
    watched_name: &[u8],
) -> Option<PoolObservation> {
    let d = datum.and_then(mitos_dex_decode::wingriders::decode_v1_pool_datum)?;
    let watched_is_a = d.asset_a_policy == watched_policy && d.asset_a_name == watched_name;
    let (value_a, value_b) = if watched_is_a {
        (qty as u64, out.lovelace)
    } else {
        (out.lovelace, qty as u64)
    };
    let (base, quote, ada_paired) = match d.ada_pair(value_a, value_b) {
        Some((ada, token)) => (token, ada, true),
        None => (qty as u64, 0, false),
    };
    // V1's pool NFT is named `L` on every pool, so it marks a pool without
    // identifying one. The LP token under the same policy carries a per-pool
    // 32-byte name — that is the instance key.
    let lp = out.assets.iter().find(|a| {
        a.policy == mitos_dex_decode::wingriders::V1_LP_POLICY
            && a.name != mitos_dex_decode::wingriders::V1_POOL_NFT_NAME
    });
    let (key_policy, key_name, key_basis) = match lp {
        Some(a) => (a.policy.clone(), a.name.clone(), KeyBasis::Value),
        None => (Vec::new(), Vec::new(), KeyBasis::Unknown),
    };
    Some(PoolObservation {
        dex: "wingriders-v1",
        address: out.address.clone(),
        key_policy,
        key_name,
        key_basis,
        ada_paired,
        base_reserve: i64::try_from(base).unwrap_or(i64::MAX),
        quote_reserve: i64::try_from(quote).unwrap_or(i64::MAX),
        fee_bps: None,
        total_lp: None,
        reserve_source: ReserveSource::ValueMinusTreasury,
    })
}

/// Minswap V2 — reserves come from the datum, never the value.
fn minswap_v2(out: &DecodedOutput, qty: i64, datum: Option<&[u8]>) -> Option<PoolObservation> {
    let d = datum.and_then(mitos_dex_decode::minswap::decode_v2_pool_datum)?;
    // Without the datum there is nothing usable: the value would overstate
    // reserves by the ADA deposit and every accrued fee, and on a dead pool by
    // millions of times. Better to record no pool than a fictional one.
    let (ada, token) = match d.ada_pair() {
        Some(pair) => pair,
        // Token/token: real supply, no ADA price. Reserves left at the
        // watched-asset quantity so the supply still counts.
        None => {
            return Some(PoolObservation {
                dex: "minswap-v2",
                address: out.address.clone(),
                key_policy: mitos_dex_decode::minswap::V2_AUTHEN_POLICY.to_vec(),
                key_name: Vec::new(),
                key_basis: KeyBasis::Datum,
                ada_paired: false,
                base_reserve: qty,
                quote_reserve: 0,
                fee_bps: Some(d.fee_a_bps as i64),
                total_lp: Some(d.total_liquidity as i64),
                reserve_source: ReserveSource::Datum,
            });
        }
    };
    Some(PoolObservation {
        dex: "minswap-v2",
        address: out.address.clone(),
        // The authen policy is shared across every V2 pool, so the LP NAME is
        // what distinguishes them. Left empty until the value-side lookup that
        // recovers it lands; `key_basis` says the identity is partial.
        key_policy: mitos_dex_decode::minswap::V2_AUTHEN_POLICY.to_vec(),
        key_name: Vec::new(),
        key_basis: KeyBasis::Ambiguous,
        ada_paired: true,
        base_reserve: i64::try_from(token).unwrap_or(i64::MAX),
        quote_reserve: i64::try_from(ada).unwrap_or(i64::MAX),
        fee_bps: Some(d.fee_a_bps as i64),
        total_lp: Some(d.total_liquidity as i64),
        reserve_source: ReserveSource::Datum,
    })
}

/// WingRiders V2 — reserves are the value less the declared treasuries.
fn wingriders_v2(
    out: &DecodedOutput,
    qty: i64,
    datum: Option<&[u8]>,
    watched_policy: &[u8],
    watched_name: &[u8],
) -> Option<PoolObservation> {
    let d = datum.and_then(mitos_dex_decode::wingriders::decode_v2_pool_datum)?;
    let watched_is_a = d.asset_a_policy == watched_policy && d.asset_a_name == watched_name;
    // The two sides' holdings: ADA from the output's lovelace, the watched
    // token from the quantity the walk already extracted.
    let (value_a, value_b) = if watched_is_a {
        (qty as u64, out.lovelace)
    } else {
        (out.lovelace, qty as u64)
    };
    let ada_pair = d.ada_pair(value_a, value_b);
    let (base, quote, ada_paired) = match ada_pair {
        Some((ada, token)) => (token, ada, true),
        None => (qty as u64, 0, false),
    };
    Some(PoolObservation {
        dex: "wingriders-v2",
        address: out.address.clone(),
        key_policy: mitos_dex_decode::wingriders::V2_LP_POLICY.to_vec(),
        key_name: Vec::new(),
        key_basis: KeyBasis::Ambiguous,
        ada_paired,
        base_reserve: i64::try_from(base).unwrap_or(i64::MAX),
        quote_reserve: i64::try_from(quote).unwrap_or(i64::MAX),
        // WingRiders' fee is a numerator over the denominator at field 9;
        // not surfaced by the decoder yet, so reported as unknown rather than
        // guessed — an assumed fee makes the realisable figure quietly wrong.
        fee_bps: None,
        total_lp: None,
        reserve_source: ReserveSource::ValueMinusTreasury,
    })
}

/// The single non-ADA, non-watched asset in a pool's value.
///
/// Ambiguity is returned as ambiguity. Picking the first, or the one with
/// quantity 1, or the alphabetically-lowest policy would all "work" and would
/// all be a guess dressed as an identity.
fn value_key<'a>(assets: &'a [Asset], watched_policy: &[u8], watched_name: &[u8]) -> ValueKey<'a> {
    let mut candidates = assets
        .iter()
        .filter(|a| !a.policy.is_empty())
        .filter(|a| !(a.policy == watched_policy && a.name == watched_name));
    let Some(first) = candidates.next() else {
        return ValueKey::None;
    };
    match candidates.next() {
        Some(_) => ValueKey::Many,
        None => ValueKey::One(first),
    }
}

/// Constant-product output for selling `sell` of the base asset into a pool.
///
/// `(quote * sell_after_fee) / (base + sell_after_fee)` — the standard
/// `x*y=k` result with the fee taken off the input, which is how both of these
/// DEXes charge it. Saturating rather than wrapping; a pool with a zero
/// reserve yields nothing rather than dividing by zero.
pub fn constant_product_out(base: i64, quote: i64, sell: i64, fee_bps: i64) -> i64 {
    if base <= 0 || quote <= 0 || sell <= 0 {
        return 0;
    }
    let fee_bps = fee_bps.clamp(0, 10_000) as i128;
    let sell_after_fee = (sell as i128) * (10_000 - fee_bps) / 10_000;
    if sell_after_fee <= 0 {
        return 0;
    }
    let out = (quote as i128 * sell_after_fee) / (base as i128 + sell_after_fee);
    i64::try_from(out).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The real ADA/NIGHT SundaeSwap V3 pool at `590a3273…#0`, read from chain
    /// 2026-08-30. The whole path — address → credential → decoder →
    /// observation — is exercised on a live output rather than a fixture, so a
    /// break anywhere in the chain fails here.
    fn night_pool_output() -> DecodedOutput {
        DecodedOutput {
            address: mitos_dex_decode::sundae::POOL_ADDR.to_string(),
            lovelace: 233_854_410_616,
            assets: vec![
                Asset {
                    policy: mitos_dex_decode::sundae::POOL_NFT_POLICY.to_vec(),
                    name: hex::decode(
                        "000de1405b5d1f9da977498b5faf3efb83693b0442ed5f49d00d9b986a409c0b",
                    )
                    .unwrap(),
                },
                Asset {
                    policy: NIGHT_POLICY.to_vec(),
                    name: b"NIGHT".to_vec(),
                },
            ],
            index: 0,
            datum_hash: None,
            inline_datum: Some(hex::decode(NIGHT_POOL_DATUM).unwrap()),
            min_utxo: 0,
        }
    }

    const NIGHT_POOL_DATUM: &str = "d8799f581c5b5d1f9da977498b5faf3efb83693b0442ed5f49d00d9b986a409c0b9f9f4040ff9f581c0691b2fecca1ac4f53cb6dfb00b7013e561d1f34403b957cbb5af1fa454e49474854ffff1b0000008e9655d073181e181ed87a80001b00000001d2e8715fff";
    const NIGHT_POLICY: [u8; 28] = [
        0x06, 0x91, 0xb2, 0xfe, 0xcc, 0xa1, 0xac, 0x4f, 0x53, 0xcb, 0x6d, 0xfb, 0x00, 0xb7, 0x01,
        0x3e, 0x56, 0x1d, 0x1f, 0x34, 0x40, 0x3b, 0x95, 0x7c, 0xbb, 0x5a, 0xf1, 0xfa,
    ];
    const NIGHT_HELD: i64 = 2_378_727_173_839;

    /// Build a pool output whose datum arrives as a WITNESS datum rather than
    /// inline — the PlutusV1 shape. The hash is left `None` because
    /// `recognise` takes the already-resolved witness datum as an argument;
    /// what these tests pin is that the V1 path uses it at all.
    fn hash_datum_output(addr: &str, lovelace: u64, assets: &[(&str, &str, u64)]) -> DecodedOutput {
        DecodedOutput {
            address: addr.to_string(),
            lovelace,
            assets: assets
                .iter()
                .map(|(p, n, _)| Asset {
                    policy: hex::decode(p).unwrap(),
                    name: hex::decode(n).unwrap(),
                })
                .collect(),
            index: 0,
            datum_hash: None,
            // The defining property of these three DEXes: NOT inline.
            inline_datum: None,
            min_utxo: 0,
        }
    }

    const MINSWAP_V1_ADDR: &str = "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha";
    const WINGRIDERS_V1_ADDR: &str = "addr1w8nvjzjeydcn4atcd93aac8allvrpjn7pjr2qsweukpnayghhwcpj";
    const SUNDAE_V1_ADDR: &str = "addr1w9qzpelu9hn45pefc0xr4ac4kdxeswq7pndul2vuj59u8tqaxdznu";

    #[test]
    fn a_live_minswap_v1_pool_is_recognised_from_a_witness_datum() {
        // Real ADA/CHIMPY pool `57e81fa7…#0`. V1 publishes no reserves, so the
        // ADA side is the raw lovelace and the token side the walk's quantity.
        let out = hash_datum_output(
            MINSWAP_V1_ADDR,
            718_437_917,
            &[
                (
                    "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                    "59c07da19612c9456ea9140c3dbc757b8085386156da971c8780a46216358d53",
                    1,
                ),
                (
                    "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f",
                    "4d494e53574150",
                    1,
                ),
            ],
        );
        let chimpy =
            hex::decode("ff791cdf3857627970df7f7930bfb7c8eee3ce45df43860d68b9ef60").unwrap();
        let datum = hex::decode(MINSWAP_V1_DATUM).unwrap();
        let obs = recognise(&out, 1_356_132_463_270, &chimpy, b"CHIMPY", Some(&datum))
            .expect("V1 credential must be recognised");
        assert_eq!(obs.dex, "minswap-v1");
        assert!(obs.ada_paired);
        assert_eq!(obs.quote_reserve, 718_437_917);
        assert_eq!(obs.base_reserve, 1_356_132_463_270);
        assert_eq!(obs.total_lp, Some(30_955_673_379));
        // One NFT per pool, so the identity is exact rather than ambiguous.
        assert_eq!(obs.key_basis, KeyBasis::Value);
        assert_eq!(
            hex::encode(&obs.key_name),
            "59c07da19612c9456ea9140c3dbc757b8085386156da971c8780a46216358d53"
        );
    }

    #[test]
    fn a_live_wingriders_v1_pool_nets_its_treasury() {
        // Real ADA/WRT pool `899c739a…#0`: 42,924,008,929 lovelace less a
        // 3,239,431 treasury, and 1,982,469,155,671 WRT less 169,660,062.
        let out = hash_datum_output(
            WINGRIDERS_V1_ADDR,
            42_924_008_929,
            &[
                (
                    "026a18d04a0c642759bb3d83b12e3344894e5c1c7b2aeb1a2113a570",
                    "4c",
                    1,
                ),
                (
                    "026a18d04a0c642759bb3d83b12e3344894e5c1c7b2aeb1a2113a570",
                    "dec347c549f618e80d97682b5b4c6985256503bbb3f3955831f5679cdb8de72f",
                    9_223_371_772_717_121_030,
                ),
            ],
        );
        let wrt = hex::decode("c0ee29a85b13209423b10447d3c2e6a50641a15c57770e27cb9d5073").unwrap();
        let datum = hex::decode(WINGRIDERS_V1_DATUM).unwrap();
        let obs = recognise(&out, 1_982_469_155_671, &wrt, b"WingRiders", Some(&datum))
            .expect("V1 credential must be recognised");
        assert_eq!(obs.dex, "wingriders-v1");
        assert_eq!(obs.quote_reserve, 42_920_769_498);
        assert_eq!(obs.base_reserve, 1_982_299_495_609);
        assert_eq!(obs.reserve_source, ReserveSource::ValueMinusTreasury);
        // The `L` NFT marks a pool; the 32-byte LP name identifies it.
        assert_eq!(obs.key_basis, KeyBasis::Value);
        assert_eq!(
            hex::encode(&obs.key_name),
            "dec347c549f618e80d97682b5b4c6985256503bbb3f3955831f5679cdb8de72f"
        );
    }

    #[test]
    fn a_live_sundae_v1_pool_reports_its_real_fee() {
        // Real ADA/ADAMARS pool `16b4f233…#0`. V1's fee is a fraction — 1/100
        // here — so a hard-coded basis would misprice it.
        let out = hash_datum_output(
            SUNDAE_V1_ADDR,
            632_695_954,
            &[(
                "0029cb7c88c7567b63d1a512c0ed626aa169688ec980730c0473b913",
                "7020af02",
                1,
            )],
        );
        let adamars =
            hex::decode("dba8e004cdec2ac9d53b8aad67b1d6527dffe99a2efe3a1ea04a00d2").unwrap();
        let datum = hex::decode(SUNDAE_V1_DATUM).unwrap();
        let obs = recognise(&out, 85_688_442_537, &adamars, b"ADAMARS", Some(&datum))
            .expect("V1 credential must be recognised");
        assert_eq!(obs.dex, "sundae-v1");
        assert_eq!(obs.quote_reserve, 632_695_954);
        assert_eq!(obs.base_reserve, 85_688_442_537);
        assert_eq!(obs.fee_bps, Some(100));
        assert_eq!(obs.total_lp, Some(7_348_469_228));
        assert_eq!(obs.key_basis, KeyBasis::Datum);
        // The datum-derived key must name the NFT the pool actually holds.
        assert_eq!(hex::encode(&obs.key_name), "7020af02");
    }

    #[test]
    fn every_v1_pool_is_dropped_rather_than_priced_without_its_datum() {
        // These three carry no inline datum, so if the creating transaction's
        // witness set were ever missed, the value alone must NOT stand in for
        // a reserve — a WingRiders pool read that way carries its treasury.
        for (addr, policy, name) in [
            (
                MINSWAP_V1_ADDR,
                "ff791cdf3857627970df7f7930bfb7c8eee3ce45df43860d68b9ef60",
                "CHIMPY",
            ),
            (
                WINGRIDERS_V1_ADDR,
                "c0ee29a85b13209423b10447d3c2e6a50641a15c57770e27cb9d5073",
                "WingRiders",
            ),
            (
                SUNDAE_V1_ADDR,
                "dba8e004cdec2ac9d53b8aad67b1d6527dffe99a2efe3a1ea04a00d2",
                "ADAMARS",
            ),
        ] {
            let out = hash_datum_output(addr, 1_000_000, &[]);
            let pol = hex::decode(policy).unwrap();
            assert!(
                recognise(&out, 1, &pol, name.as_bytes(), None).is_none(),
                "{addr} must not be priced without its datum"
            );
        }
    }

    const MINSWAP_V1_DATUM: &str = "d8799fd8799f4040ffd8799f581cff791cdf3857627970df7f7930bfb7c8eee3ce45df43860d68b9ef60464348494d5059ff1b00000007351a17231b0000000744785751d8799fd8799fd8799fd8799f581caafb1196434cb837fd6f21323ca37b302dff6387e8a84b3fa28faf56ffd8799fd8799fd8799f581c52563c5410bff6a0d43ccebb7c37e1f69f5eb260552521adff33b9c2ffffffffd87a80ffffff";
    const WINGRIDERS_V1_DATUM: &str = "d8799f581c86ae9eebd8b97944a45201e4aec1330a72291af2d071644bba015959d8799fd8799fd8799f4040ffd8799f581cc0ee29a85b13209423b10447d3c2e6a50641a15c57770e27cb9d50734a57696e67526964657273ffff1b000001a0465ecc881a00316e071a0a1cce9effff";
    const SUNDAE_V1_DATUM: &str = "d8799fd8799fd8799f4040ffd8799f581cdba8e004cdec2ac9d53b8aad67b1d6527dffe99a2efe3a1ea04a00d2474144414d415253ffff42af021b00000001b600bdecd8799f011864ffff";

    #[test]
    fn a_live_sundae_v3_pool_is_recognised_and_netted() {
        let out = night_pool_output();
        let obs = recognise(&out, NIGHT_HELD, &NIGHT_POLICY, b"NIGHT", None)
            .expect("the V3 credential must be recognised");
        assert_eq!(obs.dex, "sundae-v3");
        assert!(obs.ada_paired);
        assert_eq!(obs.base_reserve, NIGHT_HELD);
        // 233,854,410,616 held less 7,833,416,031 of protocol fees. Reading
        // the raw value would put spot 3.5% high on a pool this size.
        assert_eq!(obs.quote_reserve, 226_020_994_585);
        assert_eq!(obs.reserve_source, ReserveSource::ValueMinusTreasury);
        assert_eq!(obs.fee_bps, Some(30));
        assert_eq!(obs.total_lp, Some(612_407_562_355));
    }

    #[test]
    fn a_v3_pool_is_keyed_by_its_own_nft() {
        // The instance key must match the NFT actually sitting in the value —
        // that equality is what makes it a key rather than a label. Minswap V2
        // and WingRiders V2 are still `Ambiguous` precisely because they
        // cannot do this.
        let out = night_pool_output();
        let obs = recognise(&out, NIGHT_HELD, &NIGHT_POLICY, b"NIGHT", None).unwrap();
        assert_eq!(obs.key_basis, KeyBasis::Datum);
        assert!(
            out.assets
                .iter()
                .any(|a| a.policy == obs.key_policy && a.name == obs.key_name),
            "the datum-derived key must name an asset the pool actually holds"
        );
    }

    #[test]
    fn a_v3_pool_with_no_datum_is_not_priced_from_its_raw_value() {
        // Without the datum there is no protocol-fee figure, and the raw
        // value overstates the ADA side. Dropping the observation is correct;
        // recording it with unnetted reserves would move spot silently.
        let mut out = night_pool_output();
        out.inline_datum = None;
        assert!(recognise(&out, NIGHT_HELD, &NIGHT_POLICY, b"NIGHT", None).is_none());
    }

    #[test]
    fn a_dust_pool_is_not_quotable_but_a_real_one_is() {
        // The measured case: a dead Minswap V2 pool holding 0.98 ADA quoted
        // WRT at 484.74 against a real 0.0217 — 22,388x.
        assert!(!is_quotable(982_564));
        assert!(!is_quotable(12_000_000));
        assert!(is_quotable(42_920_769_498));
    }

    #[test]
    fn the_floor_gates_quoting_and_not_the_aggregate() {
        // The distinction the constant exists to make. Summing reserves across
        // every pool — thin ones included — is what makes the aggregate
        // immune, so the floor must never be applied to the sum.
        //
        // WRT's seven pools: dropping the six below 100 ADA changes the
        // weighted spot by 0.0027%, whereas an unweighted MEDIAN of the same
        // seven per-pool prices is 6.6% out. Reserve-summing beats
        // outlier-trimming, which is the opposite of the usual instinct.
        let all: [(i64, i64); 7] = [
            (16_208_862_418_284, 350_944_450_797),
            (1_982_299_495_609, 42_920_769_498),
            (1_018_790_504, 22_117_084),
            (264_859_902, 12_000_000),
            (305_814_081, 7_056_040),
            (55_023_231, 3_909_569),
            (2_027, 982_564),
        ];
        let weighted = |ps: &[(i64, i64)]| -> f64 {
            let b: i64 = ps.iter().map(|p| p.0).sum();
            let q: i64 = ps.iter().map(|p| p.1).sum();
            q as f64 / b as f64
        };
        let full = weighted(&all);
        let deep: Vec<_> = all.iter().copied().filter(|p| is_quotable(p.1)).collect();
        let filtered = weighted(&deep);
        assert!(
            (full - filtered).abs() / full < 0.0001,
            "the floor must be a no-op on the aggregate: {full} vs {filtered}"
        );

        // And the statistic that "removes outliers" is still worse than the
        // sum it would replace — 6.6% out, because a median discards exactly
        // the depth information that matters when depth spans five orders of
        // magnitude. Bounded on both sides so neither an improvement nor a
        // regression in this comparison can pass unnoticed.
        let mut spots: Vec<f64> = all.iter().map(|(b, q)| *q as f64 / *b as f64).collect();
        spots.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = spots[spots.len() / 2];
        let median_err = (median - full).abs() / full;
        assert!(
            (0.05..0.10).contains(&median_err),
            "median of per-pool prices should be ~0.066 out, got {median_err:.4}"
        );
    }

    #[test]
    fn constant_product_matches_hand_worked_case() {
        // A pool of 1000 base / 1000 quote, selling 1000 base with no fee,
        // returns half the quote reserve: 1000 * 1000 / 2000.
        assert_eq!(constant_product_out(1_000, 1_000, 1_000, 0), 500);
    }

    #[test]
    fn fee_reduces_the_effective_input() {
        // 85 bps off the input leaves 991.5 -> 1000*991/1991.
        let no_fee = constant_product_out(1_000, 1_000, 1_000, 0);
        let with_fee = constant_product_out(1_000, 1_000, 1_000, 85);
        assert!(with_fee < no_fee);
    }

    #[test]
    fn selling_more_than_the_pool_cannot_drain_it() {
        // The curve is asymptotic: no finite sale returns the whole reserve.
        let out = constant_product_out(1_000, 1_000, i64::MAX / 4, 0);
        assert!(
            out < 1_000,
            "constant product must never return the full quote reserve"
        );
    }

    #[test]
    fn degenerate_pools_yield_nothing() {
        assert_eq!(constant_product_out(0, 1_000, 100, 0), 0);
        assert_eq!(constant_product_out(1_000, 0, 100, 0), 0);
        assert_eq!(constant_product_out(1_000, 1_000, 0, 0), 0);
    }

    #[test]
    fn value_key_refuses_to_guess_between_two_candidates() {
        let watched = Asset {
            policy: vec![1; 28],
            name: b"TOK".to_vec(),
        };
        let lp = Asset {
            policy: vec![2; 28],
            name: b"LP".to_vec(),
        };
        let nft = Asset {
            policy: vec![3; 28],
            name: b"NFT".to_vec(),
        };
        let one = vec![watched.clone(), lp.clone()];
        assert!(matches!(
            value_key(&one, &watched.policy, &watched.name),
            ValueKey::One(a) if a.name == b"LP".to_vec()
        ));
        // Two candidates must report ambiguity, not pick one.
        let two = vec![watched.clone(), lp, nft];
        assert!(matches!(
            value_key(&two, &watched.policy, &watched.name),
            ValueKey::Many
        ));
        // No candidate is a different failure from too many.
        let none = vec![watched.clone()];
        assert!(matches!(
            value_key(&none, &watched.policy, &watched.name),
            ValueKey::None
        ));
    }
}
