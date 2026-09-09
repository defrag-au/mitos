//! Price, as far as ONE policy's archive can honestly take it.
//!
//! # What this does and does not claim
//!
//! An archive holds observations of pools that hold *its* asset. That is
//! enough to price the asset against whatever those pools pair it with, and no
//! further. A `TOKEN/OTHER` pool prices TOKEN in OTHER; turning that into ADA
//! needs OTHER's own archive, which is a different file and deliberately a
//! different caller's problem — see [`Spot::unresolved`].
//!
//! So this module resolves the direct case and **names what it cannot do**,
//! rather than quietly dropping it. That is the same rule the observation rows
//! follow one layer down.
//!
//! # The four rules, and why each has cost something
//!
//! 1. **Piecewise-constant, never interpolated.** Reserves are unchanged
//!    between observations, so the price at a slot is the last observation at
//!    or before it. Interpolating invents trades that did not happen.
//! 2. **Undefined before the first pool, never zero.** A token with no pool
//!    has no price. Zero is a number and would be drawn as one.
//! 3. **The aggregate is `Σquote / Σbase` — a MERGED pool, not an average of
//!    prices.** This is the load-bearing one. Measured on $WRT across seven
//!    pools: the median of the per-pool prices was **+6.6%** and the mean
//!    **3,200×** out, because the median lands on a 7-ADA pool and answers
//!    "what does a typical pool think" when the question is "what is this
//!    worth". Depth is exactly what a robust statistic discards. Summing
//!    reserves IS the outlier defence, structurally — a dust pool contributes
//!    dust to both sums.
//! 4. **A depth floor gates quoting ONE pool; it never filters the sum.**
//!    Applying a floor of 0 → 100 ADA moved $WRT's aggregate by 0.0027%. The
//!    floor exists so a caller does not publish a single dead pool's price — a
//!    Minswap pool with 2,027 raw WRT against 982,564 lovelace quoted 484.74
//!    ADA/token against a real 0.0217, **22,388× out**. Error-versus-depth is
//!    NOT monotonic, so the rule is to withhold a thin pool's quote, never to
//!    correct it.

use std::collections::HashMap;

use crate::observation::Observation;

/// A unit, as the observation rows spell it. ADA is the empty policy with the
/// empty name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Unit {
    pub policy: Vec<u8>,
    pub name: Vec<u8>,
}

impl Unit {
    pub fn ada() -> Self {
        Unit {
            policy: Vec::new(),
            name: Vec::new(),
        }
    }
    pub fn is_ada(&self) -> bool {
        self.policy.is_empty() && self.name.is_empty()
    }
}

/// Below this much of the quote asset, one pool's OWN price is not worth
/// publishing. Mirrors `mitos_pool_observe::MIN_QUOTABLE_LOVELACE`.
///
/// A parameter rather than a shared constant on purpose: this crate is read by
/// consumers that must not link the decode stack, and a second copy of a
/// constant is a constant that can silently disagree. Callers that have both
/// should pass the decoder's value.
pub const DEFAULT_FLOOR_LOVELACE: i64 = 100_000_000;

/// The aggregate of every pool pairing the watched asset with one unit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PairDepth {
    /// What the watched asset is paired WITH.
    pub quote_unit: Unit,
    /// Σ of the watched asset's reserves across contributing pools.
    pub base: i128,
    /// Σ of the quote asset's reserves.
    pub quote: i128,
    pub pools: usize,
    /// The thinnest quote-side reserve of any contributing pool — what a
    /// caller judges the aggregate's weakest link by, and the figure a path
    /// through several pairs must carry forward as its own weakest hop.
    pub thinnest: i64,
}

impl PairDepth {
    /// Quote units per one base unit. `None` on a zero base — a pool holding
    /// none of the asset prices nothing.
    pub fn rate(&self) -> Option<f64> {
        (self.base > 0).then(|| self.quote as f64 / self.base as f64)
    }

    /// Whether any single contributing pool cleared the floor. An aggregate
    /// made entirely of dust is still summed — rule 4 — but a caller deciding
    /// whether to PUBLISH it wants to know.
    pub fn any_pool_above(&self, floor: i64) -> bool {
        self.thinnest >= floor
    }
}

/// What the archive can say about price at one slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spot {
    pub slot: u64,
    /// The ADA-paired aggregate — the only pair an archive can resolve alone.
    /// `None` before the first ADA pool: undefined, not zero.
    pub ada: Option<PairDepth>,
    /// Pairs this archive holds and CANNOT price by itself. Each needs the
    /// quote unit's own archive to complete a path. Named rather than dropped,
    /// so a caller can see there is liquidity it is not counting.
    pub unresolved: Vec<PairDepth>,
    /// Reserves held under a pricing model this crate does not evaluate —
    /// a launchpad bonding curve, whose price is not `x·y = k`.
    ///
    /// Separate from [`Spot::unresolved`] because the two are different
    /// problems: an unresolved pair needs ANOTHER ARCHIVE, this needs a
    /// FORMULA. Counting it as a pool is the specific error that halved
    /// $PERP's price on the first end-to-end run.
    pub unpriceable: Vec<PairDepth>,
}

impl Spot {
    /// Lovelace per one whole base unit, from the ADA pairs alone.
    pub fn lovelace_per_unit(&self) -> Option<f64> {
        self.ada.as_ref().and_then(PairDepth::rate)
    }

    /// True when this archive holds liquidity it could not price — the
    /// difference between "there is no more depth" and "we cannot see it from
    /// here".
    pub fn has_unresolved(&self) -> bool {
        !self.unresolved.is_empty()
    }
}

/// Resolve the price at `slot` from a policy's observations.
///
/// `observations` need not be sorted. Only rows a decoder claimed, with a
/// measured quote reserve, can contribute — an unmeasured far side cannot be
/// summed, and guessing a zero for it would drag the aggregate toward zero
/// exactly where the data is thinnest.
pub fn spot_at(observations: &[Observation], slot: u64) -> Spot {
    // Latest observation at or before `slot`, per pool. The pool is keyed by
    // its ADDRESS plus its instance key: one address hosts many pools on every
    // venue that derives a stake part per pool, and merging them would sum
    // unrelated reserves.
    let mut latest: HashMap<(String, Vec<u8>, Vec<u8>), &Observation> = HashMap::new();
    for o in observations.iter().filter(|o| o.slot <= slot) {
        let Some(d) = &o.decoded else { continue };
        let key = (o.address.clone(), d.key_policy.clone(), d.key_name.clone());
        latest
            .entry(key)
            .and_modify(|cur| {
                // Ties broken by tx_hash so the answer cannot depend on the
                // order rows happened to arrive in.
                if (o.slot, &o.tx_hash) > (cur.slot, &cur.tx_hash) {
                    *cur = o;
                }
            })
            .or_insert(o);
    }

    let mut by_pair: HashMap<Unit, PairDepth> = HashMap::new();
    let mut off_model: HashMap<Unit, PairDepth> = HashMap::new();
    for o in latest.values() {
        let Some(d) = &o.decoded else { continue };
        let (Some(qp), Some(qn), Some(qr)) = (&d.quote_policy, &d.quote_name, d.quote_reserve)
        else {
            // Pair unknown, or known but unmeasured. Both are real states and
            // neither can be summed. See `Observation`.
            continue;
        };
        if d.base_reserve <= 0 || qr <= 0 {
            continue;
        }
        let unit = Unit {
            policy: qp.clone(),
            name: qn.clone(),
        };
        // Anything not KNOWN to be constant-product is set aside rather than
        // summed. An empty `pricing` — a row written before the column existed
        // — lands here too: "not known to be priceable" is the safe reading,
        // and the unsafe one halves a token's price.
        let bucket = match d.pricing.as_str() {
            crate::observation::pricing::CONSTANT_PRODUCT => &mut by_pair,
            _ => &mut off_model,
        };
        let e = bucket.entry(unit.clone()).or_insert(PairDepth {
            quote_unit: unit,
            base: 0,
            quote: 0,
            pools: 0,
            thinnest: i64::MAX,
        });
        e.base += d.base_reserve as i128;
        e.quote += qr as i128;
        e.pools += 1;
        e.thinnest = e.thinnest.min(qr);
    }

    let mut ada = None;
    let mut unresolved: Vec<PairDepth> = Vec::new();
    for (unit, depth) in by_pair {
        match unit.is_ada() {
            true => ada = Some(depth),
            false => unresolved.push(depth),
        }
    }
    // Deterministic order for a caller that renders or hashes it.
    unresolved.sort_by(|a, b| b.quote.cmp(&a.quote).then(a.quote_unit.cmp(&b.quote_unit)));
    let mut unpriceable: Vec<PairDepth> = off_model.into_values().collect();
    unpriceable.sort_by(|a, b| b.quote.cmp(&a.quote).then(a.quote_unit.cmp(&b.quote_unit)));

    Spot {
        slot,
        ada,
        unresolved,
        unpriceable,
    }
}

/// Every slot at which the price could have changed — one per observation that
/// a decoder claimed. The scrub axis, and the reason a spine never needs to
/// interpolate.
pub fn price_slots(observations: &[Observation]) -> Vec<u64> {
    let mut v: Vec<u64> = observations
        .iter()
        .filter(|o| o.decoded.is_some())
        .map(|o| o.slot)
        .collect();
    v.sort_unstable();
    v.dedup();
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observation::{Decoded, pricing};

    fn obs(
        slot: u64,
        addr: &str,
        key: &[u8],
        base: i64,
        quote: Option<i64>,
        ada: bool,
    ) -> Observation {
        obs_with(slot, addr, key, base, quote, ada, pricing::CONSTANT_PRODUCT)
    }

    #[allow(clippy::too_many_arguments)]
    fn obs_with(
        slot: u64,
        addr: &str,
        key: &[u8],
        base: i64,
        quote: Option<i64>,
        ada: bool,
        model: &str,
    ) -> Observation {
        Observation {
            slot,
            block_time: 1_700_000_000 + slot,
            tx_hash: vec![slot as u8; 32],
            address: addr.into(),
            lovelace: quote.unwrap_or(0),
            unit_name: b"TOK".to_vec(),
            unit_amount: base,
            datum: None,
            decoded: Some(Decoded {
                venue: "test".into(),
                key_policy: vec![9; 28],
                key_name: key.to_vec(),
                key_basis: "datum".into(),
                base_reserve: base,
                quote_policy: Some(if ada { Vec::new() } else { vec![7; 28] }),
                quote_name: Some(if ada { Vec::new() } else { b"USD".to_vec() }),
                quote_reserve: quote,
                fee_bps: None,
                total_lp: None,
                reserve_source: "value".into(),
                pricing: model.into(),
            }),
        }
    }

    /// Rule 2. A token with no pool has no price, and zero is not the answer.
    #[test]
    fn before_the_first_pool_the_price_is_undefined_not_zero() {
        let rows = vec![obs(100, "a", b"p1", 1_000, Some(2_000), true)];
        let before = spot_at(&rows, 99);
        assert_eq!(before.ada, None);
        assert_eq!(before.lovelace_per_unit(), None);
        assert!(spot_at(&rows, 100).lovelace_per_unit().is_some());
    }

    /// Rule 1. Reserves do not move between observations, so neither does the
    /// price — the value at slot 150 is the observation from slot 100.
    #[test]
    fn the_price_is_piecewise_constant_between_observations() {
        let rows = vec![
            obs(100, "a", b"p1", 1_000, Some(2_000), true),
            obs(200, "a", b"p1", 1_000, Some(4_000), true),
        ];
        assert_eq!(spot_at(&rows, 100).lovelace_per_unit(), Some(2.0));
        assert_eq!(spot_at(&rows, 150).lovelace_per_unit(), Some(2.0));
        assert_eq!(spot_at(&rows, 199).lovelace_per_unit(), Some(2.0));
        assert_eq!(spot_at(&rows, 200).lovelace_per_unit(), Some(4.0));
    }

    /// Rule 3, and the one that has cost the most elsewhere. A deep pool at 2.0
    /// and a dust pool at 1000.0 merge to ~2.0 — NOT to the 501.0 an average
    /// would give, nor the 1000.0 a median of two would land on.
    #[test]
    fn pools_merge_by_reserve_not_by_averaging_their_prices() {
        let rows = vec![
            obs(100, "deep", b"p1", 1_000_000, Some(2_000_000), true),
            obs(100, "dust", b"p2", 10, Some(10_000), true),
        ];
        let spot = spot_at(&rows, 100);
        let rate = spot.lovelace_per_unit().unwrap();
        assert!(
            (rate - 2.01).abs() < 0.02,
            "merged pool should sit next to the deep pool, got {rate}"
        );
        let d = spot.ada.unwrap();
        assert_eq!(d.pools, 2);
        // Rule 4: the dust pool is IN the sum. The floor is for judging a
        // single pool's own quote, never for filtering the aggregate.
        assert_eq!(d.thinnest, 10_000);
        assert!(!d.any_pool_above(DEFAULT_FLOOR_LOVELACE));
    }

    /// A pair the archive holds but cannot resolve is NAMED, not dropped —
    /// otherwise a token with deep token/token liquidity reads as having none.
    #[test]
    fn a_token_token_pair_is_reported_as_unresolved() {
        let rows = vec![
            obs(100, "ada", b"p1", 1_000, Some(2_000), true),
            obs(100, "usd", b"p2", 5_000, Some(9_000), false),
        ];
        let spot = spot_at(&rows, 100);
        assert_eq!(spot.lovelace_per_unit(), Some(2.0));
        assert!(spot.has_unresolved());
        assert_eq!(spot.unresolved.len(), 1);
        assert_eq!(spot.unresolved[0].quote_unit.name, b"USD".to_vec());
        assert_eq!(spot.unresolved[0].quote, 9_000);
    }

    /// An unmeasured far side cannot be summed. Treating it as zero would drag
    /// the aggregate toward zero exactly where the data is thinnest.
    #[test]
    fn an_unmeasured_quote_reserve_contributes_nothing() {
        let rows = vec![
            obs(100, "ada", b"p1", 1_000, Some(2_000), true),
            obs(100, "unk", b"p2", 5_000, None, false),
        ];
        let spot = spot_at(&rows, 100);
        assert_eq!(spot.lovelace_per_unit(), Some(2.0));
        assert!(!spot.has_unresolved(), "unmeasured is not a priceable pair");
    }

    /// Two pools at ONE address — every venue that derives a stake part per
    /// pool does this. Keying by address alone would sum unrelated reserves.
    #[test]
    fn two_pools_at_one_address_stay_separate() {
        let rows = vec![
            obs(100, "shared", b"pool-a", 1_000, Some(2_000), true),
            obs(100, "shared", b"pool-b", 3_000, Some(9_000), true),
        ];
        let d = spot_at(&rows, 100).ada.unwrap();
        assert_eq!(d.pools, 2);
        assert_eq!(d.base, 4_000);
        assert_eq!(d.quote, 11_000);
    }

    /// THE REGRESSION. A bonding curve holds ADA and tokens exactly like a
    /// pool, so summing it into `Σquote/Σbase` halved $PERP's price on the
    /// first end-to-end run: 0.00011675 against a real 0.00022585, because the
    /// curve's 261,194,031 tokens joined a 189M base.
    ///
    /// It must be set aside and REPORTED — the reserves are real, the model is
    /// what this crate cannot evaluate.
    #[test]
    fn a_bonding_curve_never_joins_the_constant_product_aggregate() {
        let rows = vec![
            obs(100, "pool", b"p1", 1_000, Some(2_000), true),
            obs_with(
                100,
                "curve",
                b"c1",
                9_000,
                Some(3_000),
                true,
                pricing::BONDING_CURVE,
            ),
        ];
        let spot = spot_at(&rows, 100);
        // The pool alone, unpolluted.
        assert_eq!(spot.lovelace_per_unit(), Some(2.0));
        assert_eq!(spot.ada.as_ref().unwrap().pools, 1);
        assert_eq!(spot.ada.as_ref().unwrap().base, 1_000);
        // And the curve's liquidity is named, not vanished.
        assert_eq!(spot.unpriceable.len(), 1);
        assert_eq!(spot.unpriceable[0].base, 9_000);
        assert_eq!(spot.unpriceable[0].quote, 3_000);
    }

    /// A row written before the `pricing` column existed says nothing about
    /// its model, and "nothing" must not be read as "the common case" — that
    /// is how the same bug returns through an old file.
    #[test]
    fn an_unstated_pricing_model_is_not_assumed_priceable() {
        let rows = vec![obs_with(100, "old", b"p1", 1_000, Some(2_000), true, "")];
        let spot = spot_at(&rows, 100);
        assert_eq!(spot.lovelace_per_unit(), None);
        assert_eq!(spot.unpriceable.len(), 1);
    }

    #[test]
    fn price_slots_are_the_scrub_axis() {
        let rows = vec![
            obs(200, "a", b"p1", 1, Some(1), true),
            obs(100, "a", b"p1", 1, Some(1), true),
            obs(200, "b", b"p2", 1, Some(1), true),
        ];
        assert_eq!(price_slots(&rows), vec![100, 200]);
    }
}
