//! Evenly-sampled series over a token's whole domain — the ONE code path
//! behind both the compiled-in fixtures (`token-ledger fixture` prints this
//! as Rust source) and any frontend loading artifacts at runtime. The two
//! consuming the same function is what stops a fixture drifting from what a
//! live surface would compute.
//!
//! Sampled evenly across the domain rather than truncated: the tail of a
//! token's life is usually the flat part, and a series that stops early
//! makes every chart look livelier than the token was.

use crate::projections as proj;
use crate::{Detail, Spine};

/// Mainnet slot → unix seconds. Chain-constant, so it lives with the wire
/// format every consumer already has, not with the walker.
pub fn slot_to_unix(slot: u64) -> i64 {
    const SHELLEY_START_SLOT: u64 = 4_492_800;
    const SHELLEY_START_UNIX: u64 = 1_596_059_091;
    const BYRON_START_UNIX: u64 = 1_506_203_091;
    if slot >= SHELLEY_START_SLOT {
        (SHELLEY_START_UNIX + (slot - SHELLEY_START_SLOT)) as i64
    } else {
        (BYRON_START_UNIX + slot * 20) as i64
    }
}

pub struct SampleRow {
    pub unix: i64,
    /// Parallel to [`SampleSeries::cohorts`].
    pub totals: Vec<i64>,
    pub holders: u32,
    /// Quote reserve summed across pools — whether a real market existed.
    pub ada_depth: i64,
    pub spot: f64,
}

pub struct PoolInfo {
    pub dex: String,
    pub key_basis: String,
    /// The pool does not exist before this.
    pub first_seen_unix: i64,
}

pub struct SampleSeries {
    pub cohorts: Vec<String>,
    pub supply: i64,
    pub decimals: u8,
    pub rows: Vec<SampleRow>,
    pub pools: Vec<PoolInfo>,
    /// Per row, per pool: watched-asset reserve. 0 = does not exist yet.
    pub pool_series: Vec<Vec<i64>>,
    /// Party ordinals of the top wallet-cohort holders by PEAK balance —
    /// a whale that exited early still shaped the story. Addresses for
    /// these ordinals live in `parties.bin`.
    pub top_holders: Vec<u32>,
    /// Per row: balance per top holder, plus a final RESIDUAL column
    /// (every other wallet-cohort holder summed). Empty without detail.
    pub holder_series: Vec<Vec<i64>>,
}

/// Sample the whole domain at `points` evenly-spaced slots.
///
/// With `detail`, cohort totals are exact and the per-holder series is
/// replayed from the movement columns; spine-only falls back to checkpoint
/// resolution and no holder series.
pub fn sample(
    spine: &Spine,
    detail: Option<&Detail>,
    points: usize,
    top_holders: usize,
) -> SampleSeries {
    let (lo, hi) = spine.domain;
    let n = points.max(2);
    let at = |i: usize| lo + ((hi - lo) as f64 * (i as f64 / (n - 1) as f64)) as u64;

    // ---- cohort rows ---------------------------------------------------
    let mut rows = Vec::with_capacity(n);
    for i in 0..n {
        let slot = at(i);
        let Some(c) = (match detail {
            Some(d) => proj::cohorts_at(spine, d, slot),
            None => proj::cohorts_at_checkpoint(spine, slot),
        }) else {
            continue;
        };
        let ada_depth = proj::reserves_at(spine, slot).map_or(0, |(_, quote)| quote);
        let spot = proj::spot_at(spine, slot).unwrap_or(0.0);
        rows.push(SampleRow {
            unix: slot_to_unix(slot),
            totals: c.totals,
            holders: c.holders,
            ada_depth,
            spot,
        });
    }

    // ---- per-pool meta + series ---------------------------------------
    // A pool DOES NOT EXIST until created; aggregate `pool` throws that
    // away. Read straight off the reserve curve (piecewise-constant).
    let rc_slots = crate::delta_decode(&spine.rc_slots);
    let mut first_seen = vec![u64::MAX; spine.pools.len()];
    for (i, p) in spine.rc_pool.iter().enumerate() {
        let e = &mut first_seen[*p as usize];
        *e = (*e).min(rc_slots[i]);
    }
    let pools = spine
        .pools
        .iter()
        .zip(&first_seen)
        .map(|(p, f)| PoolInfo {
            dex: p.dex.clone(),
            key_basis: p.key_basis.clone(),
            first_seen_unix: slot_to_unix(*f),
        })
        .collect();
    let mut pool_series = Vec::with_capacity(n);
    for i in 0..n {
        let slot = at(i);
        let upto = rc_slots.partition_point(|s| *s <= slot);
        let mut latest = vec![0i64; spine.pools.len()];
        for k in 0..upto {
            latest[spine.rc_pool[k] as usize] = spine.rc_base[k];
        }
        pool_series.push(latest);
    }

    // ---- per-holder series (wallet cohort) -----------------------------
    // `mv_party` keeps parties LINKED while identity stays cold — exactly
    // what a holder-coloured surface needs. Replay to per-party balances,
    // rank by peak, fold everyone else into a residual column.
    let mut top = Vec::new();
    let mut holder_series = Vec::new();
    if let (Some(d), Some(wallet)) = (detail, spine.cohorts.iter().position(|c| c == "wallet")) {
        let tx_slots = crate::delta_decode(&d.tx_slots);
        let parties = d.party_cohort.len();
        let mut bal = vec![0i64; parties];
        let mut peak = vec![0i64; parties];
        let mut snapshots: Vec<Vec<i64>> = Vec::with_capacity(n);
        let mut mv = 0usize;
        for i in 0..n {
            let slot = at(i);
            let tx_end = tx_slots.partition_point(|s| *s <= slot);
            while mv < d.mv_party.len() && (d.mv_tx[mv] as usize) < tx_end {
                let p = d.mv_party[mv] as usize;
                bal[p] += d.mv_amount[mv];
                peak[p] = peak[p].max(bal[p]);
                mv += 1;
            }
            snapshots.push(bal.clone());
        }
        let mut idx: Vec<usize> = (0..parties)
            .filter(|p| d.party_cohort[*p] as usize == wallet)
            .collect();
        idx.sort_by_key(|p| std::cmp::Reverse(peak[*p]));
        idx.truncate(top_holders);
        top = idx.iter().map(|p| *p as u32).collect();
        let all: Vec<usize> = (0..parties)
            .filter(|p| d.party_cohort[*p] as usize == wallet)
            .collect();
        for snap in &snapshots {
            let mut row: Vec<i64> = idx.iter().map(|p| snap[*p].max(0)).collect();
            let total: i64 = all.iter().map(|p| snap[*p].max(0)).sum();
            let named: i64 = row.iter().sum();
            row.push((total - named).max(0));
            holder_series.push(row);
        }
    }

    SampleSeries {
        cohorts: spine.cohorts.clone(),
        supply: spine.nominal_supply,
        decimals: spine.asset.decimals,
        rows,
        pools,
        pool_series,
        top_holders: top,
        holder_series,
    }
}
