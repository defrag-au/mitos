//! Projections — every number the surface shows, as a pure function of the
//! artifacts at a time `t`.
//!
//! This is the crate's whole point restated as code: nothing here reads
//! mutable state, so the walker, the frontend and a third party auditing us
//! all run the same arithmetic and get the same answer for any past instant.
//!
//! It lives beside the wire types rather than in a crate of its own because
//! both halves are the same dependency class — pure `serde` + `postcard`, no
//! I/O, no runtime, `wasm32`-safe — and splitting two small pure modules would
//! be ceremony. It follows the discipline the `tiers` crate states: *measuring
//! is the caller's job; deciding is this module's.* Callers hand in decoded
//! pages and get numbers back.
//!
//! Being pure is also what makes it **host-compiled and therefore tested**. A
//! frontend that puts this logic behind a `wasm32`-only module never runs its
//! tests — flow-explorer records exactly that failure, and a bug that shipped
//! because of it.
//!
//! # The scrub is checkpoint plus bounded replay
//!
//! The log is totally ordered, so a projection at `t` is a prefix of it.
//! [`cohorts_at`] binary-searches the checkpoints, takes the nearest one at or
//! before `t`, and replays at most `stride` transactions of detail from there —
//! O(stride), not O(n), and flat as the log grows. With only the spine loaded,
//! [`cohorts_at_checkpoint`] answers at checkpoint resolution, which is exact
//! at the points it names.

use crate::{Detail, Spine};

/// Supply by cohort at an instant, plus how it was obtained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CohortTotals {
    /// Parallel to [`Spine::cohorts`].
    pub totals: Vec<i64>,
    /// Holders with a non-zero balance.
    pub holders: u32,
    /// The transaction ordinal these totals are as of.
    pub as_of_tx: u32,
    /// True when detail was replayed to reach exactly `t`; false when this is
    /// the nearest checkpoint and the caller has spine only.
    ///
    /// Surfaced rather than hidden because the difference is visible on a
    /// chart: checkpoint-resolution totals step where exact ones would slope,
    /// and a UI that cannot tell the two apart will present the coarser one as
    /// though it were the finer.
    pub exact: bool,
}

/// The last checkpoint at or before `slot`.
fn checkpoint_before(spine: &Spine, slot: u64) -> Option<usize> {
    let slots = crate::delta_decode(&spine.cp_slots);
    if slots.is_empty() || slot < slots[0] {
        return None;
    }
    // partition_point gives the first index PAST the target, so step back one.
    Some(slots.partition_point(|s| *s <= slot) - 1)
}

/// Cohort totals from the spine alone, at checkpoint resolution.
///
/// Exact at the transaction the checkpoint names. `None` when `slot` precedes
/// the first checkpoint — before the token's first transaction there is no
/// supply, and returning zeros would be a claim rather than an absence.
pub fn cohorts_at_checkpoint(spine: &Spine, slot: u64) -> Option<CohortTotals> {
    let i = checkpoint_before(spine, slot)?;
    Some(CohortTotals {
        totals: spine.checkpoint_totals(i)?.to_vec(),
        holders: *spine.cp_holders.get(i)?,
        as_of_tx: *spine.cp_tx_ord.get(i)?,
        exact: false,
    })
}

/// Cohort totals at `slot`, exact — nearest checkpoint plus a bounded replay.
///
/// Holder count is recomputed over the replayed window only, so it is exact
/// for parties that moved in the window and inherited from the checkpoint
/// otherwise. Getting a holder count exactly right at an arbitrary `t` needs
/// per-party balances, which is what the detail tier is for; this keeps the
/// checkpoint's count and adjusts for parties crossing zero in the window.
pub fn cohorts_at(spine: &Spine, detail: &Detail, slot: u64) -> Option<CohortTotals> {
    let mut base = cohorts_at_checkpoint(spine, slot)?;
    let tx_slots = crate::delta_decode(&detail.tx_slots);

    // Replay every transaction after the checkpoint up to and including `slot`.
    let start = base.as_of_tx as usize + 1;
    let end = tx_slots.partition_point(|s| *s <= slot);
    if end <= start {
        base.exact = true;
        return Some(base);
    }

    let first_mv = detail.mv_tx.partition_point(|t| (*t as usize) < start);
    let last_mv = detail.mv_tx.partition_point(|t| (*t as usize) < end);
    for i in first_mv..last_mv {
        let party = detail.mv_party[i] as usize;
        let cohort = detail.party_cohort[party] as usize;
        if let Some(t) = base.totals.get_mut(cohort) {
            *t += detail.mv_amount[i];
        }
    }

    base.as_of_tx = (end - 1) as u32;
    base.exact = true;
    Some(base)
}

/// Reserves summed across pools at `slot`.
///
/// Piecewise-constant: each pool contributes its most recent observation at or
/// before `slot`, because between pool-touching transactions the reserves
/// genuinely did not change. **Never interpolate across these** — a smoothed
/// line invents data the ledger can be checked against and found not to hold.
pub fn reserves_at(spine: &Spine, slot: u64) -> Option<(i64, i64)> {
    let slots = crate::delta_decode(&spine.rc_slots);
    let upto = slots.partition_point(|s| *s <= slot);
    if upto == 0 {
        return None;
    }
    let mut latest: Vec<Option<(i64, i64)>> = vec![None; spine.pools.len()];
    for i in 0..upto {
        let p = spine.rc_pool[i] as usize;
        if let Some(slot_entry) = latest.get_mut(p) {
            *slot_entry = Some((spine.rc_base[i], spine.rc_quote[i]));
        }
    }
    let (base, quote) = latest
        .iter()
        .flatten()
        .fold((0i64, 0i64), |(b, q), (pb, pq)| (b + pb, q + pq));
    (base > 0).then_some((base, quote))
}

/// Lovelace per token at `slot`, liquidity-weighted across pools.
///
/// `None` before any pool exists. **Not zero** — before the first pool there
/// is no price, and a chart that draws zero, or back-fills the first price
/// leftward, lies about precisely the period a launch view exists to examine.
pub fn spot_at(spine: &Spine, slot: u64) -> Option<f64> {
    let (base, quote) = reserves_at(spine, slot)?;
    Some(quote as f64 / base as f64)
}

/// Constant-product output for selling `sell` of the base asset.
///
/// `(quote * sell_after_fee) / (base + sell_after_fee)` — `x*y=k` with the fee
/// taken off the input, which is how these DEXes charge it.
pub fn constant_product_out(base: i64, quote: i64, sell: i64, fee_bps: u32) -> i64 {
    if base <= 0 || quote <= 0 || sell <= 0 {
        return 0;
    }
    let fee = fee_bps.min(10_000) as i128;
    let after_fee = (sell as i128) * (10_000 - fee) / 10_000;
    if after_fee <= 0 {
        return 0;
    }
    let out = (quote as i128 * after_fee) / (base as i128 + after_fee);
    i64::try_from(out).unwrap_or(i64::MAX)
}

/// The cap band at an instant, in lovelace.
#[derive(Debug, Clone, PartialEq)]
pub struct Cap {
    pub spot_lovelace: f64,
    /// All supply at spot. The number everyone quotes.
    pub notional: i64,
    /// What the certainly-sellable float would fetch sold into the curve.
    pub realisable_low: i64,
    /// As above, treating unidentified script holdings as sellable too.
    pub realisable_high: i64,
    /// Supply whose liquidity is genuinely unknown — the width of the band.
    pub uncertain: i64,
}

impl Cap {
    /// Realisable over notional, as a percentage. The liquidity honesty ratio.
    pub fn honesty_ratio(&self) -> (f64, f64) {
        if self.notional <= 0 {
            return (0.0, 0.0);
        }
        let n = self.notional as f64;
        (
            100.0 * self.realisable_low as f64 / n,
            100.0 * self.realisable_high as f64 / n,
        )
    }
}

/// Which cohorts feed the sell quantity.
///
/// Three deliberate choices, each argued in `TOKEN_LEDGER.md`:
///
/// - **Pooled supply counts toward the notional cap.** It is the most
///   reachable supply on chain. Excluding it would quietly pre-apply half the
///   correction the realisable figure exists to make.
/// - **The sell quantity excludes pooled supply.** Forced, not chosen: selling
///   the pool into itself is incoherent. So the two figures have different
///   denominators on purpose.
/// - **Unidentified script holdings widen the band rather than pick a side.**
///   Some are open orders (sellable), some are locks (not). Reporting a point
///   estimate would be a claim neither reading supports.
fn sell_quantities(spine: &Spine, totals: &[i64]) -> (i64, i64) {
    let ix = |name: &str| spine.cohorts.iter().position(|c| c == name);
    let get = |name: &str| ix(name).and_then(|i| totals.get(i)).copied().unwrap_or(0);
    let certain = get("wallet");
    let uncertain = get("script");
    (certain, certain + uncertain)
}

/// The cap band at `slot`, from the spine (and detail, when available).
///
/// Vesting that has passed its unlock is claimable today, so it joins the
/// sellable floor — read from the spine's own per-checkpoint split rather than
/// passed in, which is what makes the artifacts self-sufficient.
pub fn cap_at(spine: &Spine, detail: Option<&Detail>, slot: u64) -> Option<Cap> {
    let spot = spot_at(spine, slot)?;
    let totals = match detail {
        Some(d) => cohorts_at(spine, d, slot)?,
        None => cohorts_at_checkpoint(spine, slot)?,
    };
    let (base, quote) = reserves_at(spine, slot)?;

    // Weighted-average fee over pools that published one. A pool with no
    // decoded fee must not silently become zero-fee; the caller is told how
    // many were missing so the UI can say the figure is optimistic.
    let fees: Vec<u32> = spine.pools.iter().filter_map(|p| p.fee_bps).collect();
    let fee_bps = if fees.is_empty() {
        0
    } else {
        fees.iter().sum::<u32>() / fees.len() as u32
    };

    let supply: i64 = totals.totals.iter().sum();
    let (low, high) = sell_quantities(spine, &totals.totals);
    // Matured vesting is claimable now, so it raises the floor AND the
    // ceiling — it widens nothing, because there is no uncertainty about it.
    let matured = vesting_matured_at(spine, slot);
    let (low, high) = (low + matured, high + matured);

    Some(Cap {
        spot_lovelace: spot,
        notional: ((supply as f64) * spot) as i64,
        realisable_low: constant_product_out(base, quote, low, fee_bps),
        realisable_high: constant_product_out(base, quote, high, fee_bps),
        uncertain: high - low,
    })
}

/// Vesting past its unlock at `slot`, at checkpoint resolution.
///
/// Zero before the first checkpoint rather than `None`: a caller asking about
/// a time with no supply is already handled by the cohort projections, and a
/// nil maturity is the correct contribution to a floor that does not exist.
pub fn vesting_matured_at(spine: &Spine, slot: u64) -> i64 {
    checkpoint_before(spine, slot)
        .and_then(|i| spine.cp_vest_matured.get(i))
        .copied()
        .unwrap_or(0)
}

/// Vesting still before its unlock at `slot`. Cannot move.
pub fn vesting_locked_at(spine: &Spine, slot: u64) -> i64 {
    checkpoint_before(spine, slot)
        .and_then(|i| spine.cp_vest_locked.get(i))
        .copied()
        .unwrap_or(0)
}

/// How many pools published a fee, out of how many exist.
///
/// A realisable figure computed with a missing fee is optimistic by that
/// pool's fee, and the UI is expected to say so rather than present it flat.
pub fn fee_coverage(spine: &Spine) -> (usize, usize) {
    (
        spine.pools.iter().filter(|p| p.fee_bps.is_some()).count(),
        spine.pools.len(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AssetId, PoolMeta, WIRE_VERSION, delta_encode};

    /// Two cohorts, three checkpoints, one pool.
    fn spine() -> Spine {
        Spine {
            version: WIRE_VERSION,
            asset: AssetId {
                policy: [7u8; 28],
                asset_name: b"TOK".to_vec(),
                decimals: 0,
            },
            domain: (100, 300),
            last_block_time: 1_700_000_000,
            nominal_supply: 1_000,
            cohorts: vec!["pool".into(), "script".into(), "wallet".into()],
            checkpoint_stride: 2,
            cp_slots: delta_encode(&[100, 200, 300]),
            cp_tx_ord: vec![0, 2, 4],
            cp_holders: vec![1, 2, 3],
            // pool, script, wallet
            cp_totals: vec![0, 0, 1000, 400, 100, 500, 400, 100, 500],
            cp_vest_matured: vec![0, 0, 0],
            cp_vest_locked: vec![0, 0, 0],
            pools: vec![PoolMeta {
                dex: "cswap".into(),
                key_policy: vec![1; 28],
                key_name: b"LP".to_vec(),
                key_basis: "datum".into(),
                fee_bps: Some(0),
            }],
            rc_slots: delta_encode(&[150, 250]),
            rc_pool: vec![0, 0],
            rc_base: vec![400, 400],
            rc_quote: vec![4_000_000, 8_000_000],
            rc_eps_bps: 0,
        }
    }

    fn detail() -> Detail {
        Detail {
            version: WIRE_VERSION,
            bases: vec!["decoded".into(), "chain".into()],
            // Party 0 is the pool (cohort 0), party 1 a wallet (cohort 2).
            // Addresses live in `PartyIds` and no projection needs them.
            party_cohort: vec![0, 2],
            party_basis: vec![0, 1],
            tx_slots: delta_encode(&[100, 150, 200, 250, 300]),
            tx_net_mint: vec![1000, 0, 0, 0, 0],
            // tx 3 moves 50 from wallet to pool; nothing else after cp@200.
            mv_tx: vec![0, 3, 3],
            mv_party: vec![1, 1, 0],
            mv_amount: vec![1000, -50, 50],
        }
    }

    #[test]
    fn before_the_first_checkpoint_there_is_no_answer_not_a_zero() {
        assert!(cohorts_at_checkpoint(&spine(), 50).is_none());
        assert!(spot_at(&spine(), 50).is_none(), "no pool yet ⇒ no price");
    }

    #[test]
    fn spine_only_answers_at_checkpoint_resolution_and_says_so() {
        let got = cohorts_at_checkpoint(&spine(), 250).unwrap();
        assert_eq!(got.as_of_tx, 2, "must fall back to the checkpoint at 200");
        assert!(!got.exact);
        assert_eq!(got.totals, vec![400, 100, 500]);
    }

    #[test]
    fn detail_replay_reaches_the_exact_instant() {
        let (s, d) = (spine(), detail());
        // At 250, tx 3 has happened: 50 moved wallet -> pool.
        let got = cohorts_at(&s, &d, 250).unwrap();
        assert!(got.exact);
        assert_eq!(got.as_of_tx, 3);
        assert_eq!(got.totals, vec![450, 100, 450]);
        // Supply is conserved by the replay — a movement that changed the
        // total would mean the columns disagree with the checkpoint.
        assert_eq!(got.totals.iter().sum::<i64>(), 1000);
    }

    #[test]
    fn replay_and_checkpoint_agree_where_they_meet() {
        let (s, d) = (spine(), detail());
        let exact = cohorts_at(&s, &d, 200).unwrap();
        let cp = cohorts_at_checkpoint(&s, 200).unwrap();
        assert_eq!(
            exact.totals, cp.totals,
            "at a checkpoint's own slot the replay must add nothing"
        );
    }

    #[test]
    fn price_is_piecewise_constant_never_interpolated() {
        let s = spine();
        // Between the two reserve points the price holds at the earlier one.
        assert_eq!(spot_at(&s, 150), spot_at(&s, 200));
        assert_eq!(spot_at(&s, 150), Some(10_000.0));
        // And steps at the next observation rather than sloping toward it.
        assert_eq!(spot_at(&s, 250), Some(20_000.0));
    }

    #[test]
    fn the_cap_band_widens_by_exactly_the_unidentified_supply() {
        let (s, d) = (spine(), detail());
        let cap = cap_at(&s, Some(&d), 250).unwrap();
        // script = 100 at this instant, and that is the whole spread.
        assert_eq!(cap.uncertain, 100);
        assert!(cap.realisable_high > cap.realisable_low);
        let (lo, hi) = cap.honesty_ratio();
        assert!(lo < hi && hi < 100.0, "realisable cannot reach notional");
    }

    #[test]
    fn matured_vesting_joins_the_sellable_floor() {
        let (s, d) = (spine(), detail());
        let mut s2 = spine();
        s2.cp_vest_matured = vec![0, 50, 50];
        let without = cap_at(&s, Some(&d), 250).unwrap();
        let with = cap_at(&s2, Some(&d), 250).unwrap();
        assert!(
            with.realisable_low > without.realisable_low,
            "supply that is claimable today must raise the floor"
        );
        assert_eq!(
            with.uncertain, without.uncertain,
            "matured vesting is certain, so it must not widen the band"
        );
    }

    #[test]
    fn maturity_is_read_at_the_checkpoint_not_from_tip() {
        // Backdating tip's maturity onto every earlier checkpoint is exactly
        // the bug this per-checkpoint column exists to prevent.
        let mut s = spine();
        s.cp_vest_matured = vec![0, 0, 50];
        assert_eq!(vesting_matured_at(&s, 100), 0);
        assert_eq!(vesting_matured_at(&s, 250), 0, "still the 200 checkpoint");
        assert_eq!(vesting_matured_at(&s, 300), 50);
    }

    #[test]
    fn fee_coverage_is_reported_not_assumed() {
        let mut s = spine();
        assert_eq!(fee_coverage(&s), (1, 1));
        s.pools[0].fee_bps = None;
        assert_eq!(fee_coverage(&s), (0, 1));
    }
}
