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
        // SundaeSwap V1 is PlutusV1, so its pool state is committed as a datum
        // HASH whose preimage only appears when the output is spent — one
        // interaction after the state it describes. Recognised so its holdings
        // land in the `pool` cohort rather than surfacing as an unnamed script;
        // deliberately unpriced until the walker carries a hash→datum cache.
        if mitos_dex_decode::sundae::is_sundae_v1(&cred) {
            return Some(unpriced_pool("sundae-v1", out, qty));
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

/// A pool recognised by credential whose reserves cannot be read.
///
/// Counts the supply — which is what the cohort needs — without contributing a
/// price. `ada_paired: false` is the load-bearing part: it keeps the pool out
/// of the reserve curve entirely, rather than letting an unnetted value stand
/// in for a reserve and quietly move spot.
fn unpriced_pool(dex: &'static str, out: &DecodedOutput, qty: i64) -> PoolObservation {
    PoolObservation {
        dex,
        address: out.address.clone(),
        key_policy: Vec::new(),
        key_name: Vec::new(),
        key_basis: KeyBasis::Unknown,
        ada_paired: false,
        base_reserve: qty,
        quote_reserve: 0,
        fee_bps: None,
        total_lp: None,
        reserve_source: ReserveSource::Value,
    }
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
