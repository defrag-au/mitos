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

/// Inverse of [`slot_to_unix`].
///
/// A consumer driving a playhead in clock time needs to ask the movement
/// columns — which are keyed by SLOT — what had happened by then, so the
/// round trip has to exist somewhere. It belongs next to its forward
/// direction rather than being re-derived, slightly differently, in each
/// program that needs it.
pub fn unix_to_slot(unix: i64) -> u64 {
    const SHELLEY_START_SLOT: u64 = 4_492_800;
    const SHELLEY_START_UNIX: i64 = 1_596_059_091;
    const BYRON_START_UNIX: i64 = 1_506_203_091;
    if unix >= SHELLEY_START_UNIX {
        SHELLEY_START_SLOT + (unix - SHELLEY_START_UNIX) as u64
    } else {
        ((unix - BYRON_START_UNIX).max(0) / 20) as u64
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
    /// The vesting cohort's two REAL states (checkpoint resolution). A
    /// single "vesting" number hides the story that matters: matured is
    /// claimable NOW — latent sell pressure — while locked cannot move.
    pub vest_locked: i64,
    pub vest_matured: i64,
}

pub struct PoolInfo {
    pub dex: String,
    pub key_basis: String,
    /// The pool does not exist before this.
    pub first_seen_unix: i64,
}

/// Loudest impulses kept per window — a reactive surface renders spikes,
/// not a ledger, and the aggregates beside them stay exact regardless.
const MAX_IMPULSES: usize = 32;

/// One party's balance change in one transaction — the RAW event, with no
/// pairing heuristic applied.
///
/// The ledger's primitive is a signed per-(tx, party) delta, deliberately
/// **not** a directed `(from, to)` edge: a multi-party transaction cannot
/// be resolved into pairs without a heuristic, and the obvious one is
/// wrong exactly where the interesting activity is. A visualization that
/// wants arrows may pair these itself — and owns that decision.
#[derive(Clone, Debug)]
pub struct Impulse {
    pub unix: i64,
    /// Signed: negative left this party, positive arrived.
    pub amount: i64,
    /// The party's cohort — index into [`SampleSeries::cohorts`].
    pub cohort: u16,
    /// Opaque party ordinal; addresses live in `parties.bin`.
    pub party: u32,
}

/// What MOVED in one sample window — the reactive input channel.
///
/// Cohort totals say what a token IS at time t; this says what it DID.
/// A surface driven only by sampled state has to invent plausible motion
/// between snapshots; given these it can react to the chain's own rhythm —
/// quiet weeks are still, a distribution day is a flood.
#[derive(Clone, Debug, Default)]
pub struct WindowEvents {
    pub txs: u32,
    pub movements: u32,
    /// Σ|delta| over every movement in the window — the "beat".
    pub volume: i64,
    /// Net change per cohort across the window, parallel to `cohorts`.
    /// Sums to the window's net mint (zero for a fixed-supply token).
    pub cohort_delta: Vec<i64>,
    /// The loudest individual movements, descending by magnitude. May be
    /// empty (spine-only load, or a fixture that omits them).
    pub top: Vec<Impulse>,
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
    /// Per row: what moved in the window ENDING at that row. Empty without
    /// detail — the spine alone carries state, not events.
    pub events: Vec<WindowEvents>,
    /// Dated events worth NAMING, in time order. Every one is derived from
    /// the ledger, never guessed.
    pub milestones: Vec<Milestone>,
}

/// A moment in a token's life that deserves a label.
///
/// Cohort levels answer "what is true now"; these answer "what HAPPENED",
/// which is the question a timeline view is really asking. They exist as
/// data rather than as something each surface re-derives, so a chart's
/// annotation, a signpost in a 3-D view and a line in a report all agree
/// on when a token graduated.
#[derive(Clone, Debug)]
pub struct Milestone {
    pub unix: i64,
    pub kind: MilestoneKind,
    /// Short display label, already resolved.
    pub label: String,
}

// `Copy` + `Eq`: a fieldless tag that consumers match on constantly, so
// making them clone or borrow it is friction with no upside.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MilestoneKind {
    /// First observation of the asset — the token exists.
    Mint,
    /// The first pool was created: the end of the bonding phase, and the
    /// only moment on a launchpad token where the market changes KIND
    /// rather than degree.
    Graduation,
    /// A venue's pool appeared (including the first).
    PoolOpened,
    /// Supply first reached a proven-unspendable sink.
    FirstBurn,
    /// Locked supply first became claimable — latent sell pressure
    /// arriving, which no level chart marks.
    VestingUnlock,
}

/// Choose the slots to sample at.
///
/// 🔑 **Not uniform, and that is the point.** Uniform-in-slot sampling
/// puts the resolution where the CLOCK is, not where the token's life is:
/// measured on $Dong, the entire bonding→LP graduation — the curve
/// draining 80%→2% and Splash appearing — landed inside **one sample of
/// 200, holding 4,495 transactions, 11.6% of the token's whole lifetime
/// activity**. Every consumer then stretched that single sample across a
/// third of its display (because `warped_axis` weights screen time by
/// activity) and animated it with pure interpolation. The axis was
/// warping by activity while the sampling was not.
///
/// So: half the budget uniform in slot space, so dormancy is still
/// represented and a chart still has a time axis; half at TRANSACTION
/// QUANTILES, so busy stretches get proportional resolution. Pool
/// creation slots are forced in, because "the moment a venue existed" is
/// exactly the boundary a viewer is looking for and landing next to it is
/// not the same as landing on it.
fn sample_slots(spine: &Spine, detail: Option<&Detail>, n: usize) -> Vec<u64> {
    let (lo, hi) = spine.domain;
    let uniform = |i: usize, count: usize| -> u64 {
        lo + ((hi - lo) as f64 * (i as f64 / (count.max(2) - 1) as f64)) as u64
    };

    // Spine-only: no transaction slots to weight by, so uniform is all
    // that is available.
    let Some(d) = detail else {
        return (0..n).map(|i| uniform(i, n)).collect();
    };
    let tx_slots = crate::delta_decode(&d.tx_slots);
    if tx_slots.is_empty() {
        return (0..n).map(|i| uniform(i, n)).collect();
    }

    let half = n / 2;
    let mut out: Vec<u64> = (0..half).map(|i| uniform(i, half)).collect();
    let rest = n - half;
    for k in 0..rest {
        let q = k as f64 / rest.max(1) as f64;
        let idx = ((tx_slots.len() - 1) as f64 * q) as usize;
        out.push(tx_slots[idx]);
    }
    // Force a sample at every pool's first appearance.
    let rc_slots = crate::delta_decode(&spine.rc_slots);
    let mut first = vec![u64::MAX; spine.pools.len()];
    for (i, p) in spine.rc_pool.iter().enumerate() {
        let e = &mut first[*p as usize];
        *e = (*e).min(rc_slots[i]);
    }

    // ── the bonding phase gets a GUARANTEED BUDGET ────────────────────
    //
    // 🔑 Both schemes above rate the bonding phase as negligible, because
    // by their measures it is: on $Dong it is hours out of two years
    // (so ~0 uniform samples) carrying 15 of 38,720 transactions (so ~0
    // quantile samples). It therefore landed as ONE span from mint to the
    // first pool — the single most important stretch of a launchpad
    // token's life, the one where the distribution is actually decided,
    // rendered as a straight line between two measurements.
    //
    // The schemes are not wrong; applying them GLOBALLY to a phase that
    // is globally tiny is. So the phase gets its own allocation, and the
    // same two schemes are applied WITHIN it.
    let graduation = first.iter().copied().min().unwrap_or(u64::MAX);
    if graduation != u64::MAX && graduation > lo {
        let budget = (n / 8).max(12);
        let bond_half = budget / 2;
        // Uniform in slot across the window.
        for i in 0..bond_half {
            let f = i as f64 / bond_half.max(1) as f64;
            out.push(lo + ((graduation - lo) as f64 * f) as u64);
        }
        // Transaction quantiles restricted to the window — the curve
        // selling down is what the phase IS, so sample equal amounts of
        // it rather than equal amounts of time.
        let b = tx_slots.partition_point(|s| *s < graduation);
        if b > 0 {
            let k = budget - bond_half;
            for j in 0..k {
                let q = j as f64 / k.max(1) as f64;
                out.push(tx_slots[((b - 1) as f64 * q) as usize]);
            }
            // The LAST state before graduation. Without it the transition
            // interpolates from wherever the previous sample landed
            // straight into the live era, so the curve is never seen
            // near-empty — which is exactly the moment it matters.
            out.push(tx_slots[b - 1]);
        }
    }

    out.extend(first.into_iter().filter(|s| *s != u64::MAX && *s >= lo));
    out.push(hi);
    out.sort_unstable();
    out.dedup();
    out
}

/// Sample the whole domain, denser where the token was busy.
///
/// With `detail`, cohort totals are exact and the per-holder series and
/// event channel are replayed from the movement columns; spine-only falls
/// back to checkpoint resolution, uniform slots, and no events.
pub fn sample(
    spine: &Spine,
    detail: Option<&Detail>,
    points: usize,
    top_holders: usize,
) -> SampleSeries {
    let slots = sample_slots(spine, detail, points.max(2));

    // ---- cohort rows ---------------------------------------------------
    // `kept` is the slots that actually produced a row. Every other series
    // below is built from it, so all of them stay the same length — a
    // consumer indexes `rows[i]`, `pool_series[i]` and `events[i]` as one
    // record, and a skipped slot must not shift them out of step.
    let mut rows = Vec::with_capacity(slots.len());
    let mut kept: Vec<u64> = Vec::with_capacity(slots.len());
    for &slot in &slots {
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
            vest_locked: proj::vesting_locked_at(spine, slot),
            vest_matured: proj::vesting_matured_at(spine, slot),
        });
        kept.push(slot);
    }
    let n = kept.len();
    let at = |i: usize| kept[i];

    // ---- per-pool meta + series ---------------------------------------
    // A pool DOES NOT EXIST until created; aggregate `pool` throws that
    // away. Read straight off the reserve curve (piecewise-constant).
    let rc_slots = crate::delta_decode(&spine.rc_slots);
    let mut first_seen = vec![u64::MAX; spine.pools.len()];
    for (i, p) in spine.rc_pool.iter().enumerate() {
        let e = &mut first_seen[*p as usize];
        *e = (*e).min(rc_slots[i]);
    }
    let pools: Vec<PoolInfo> = spine
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

    // ---- movement replay: events + per-holder series --------------------
    // ONE pass over the movement columns feeds both. `mv_party` keeps
    // parties LINKED while identity stays cold, which is what a
    // holder-coloured surface needs; the same deltas are the EVENT STREAM
    // a reactive surface reacts to.
    let mut top = Vec::new();
    let mut holder_series = Vec::new();
    let mut events: Vec<WindowEvents> = Vec::with_capacity(n);
    if let Some(d) = detail {
        let tx_slots = crate::delta_decode(&d.tx_slots);
        let parties = d.party_cohort.len();
        let wallet = spine.cohorts.iter().position(|c| c == "wallet");
        let mut bal = vec![0i64; parties];
        let mut peak = vec![0i64; parties];
        let mut snapshots: Vec<Vec<i64>> = Vec::with_capacity(n);
        let mut mv = 0usize;
        let mut prev_tx = 0usize;
        for i in 0..n {
            let slot = at(i);
            let tx_end = tx_slots.partition_point(|s| *s <= slot);
            let mut w = WindowEvents {
                txs: (tx_end - prev_tx.min(tx_end)) as u32,
                cohort_delta: vec![0; spine.cohorts.len()],
                ..Default::default()
            };
            while mv < d.mv_party.len() && (d.mv_tx[mv] as usize) < tx_end {
                let p = d.mv_party[mv] as usize;
                let amount = d.mv_amount[mv];
                bal[p] += amount;
                peak[p] = peak[p].max(bal[p]);

                w.movements += 1;
                w.volume += amount.unsigned_abs() as i64;
                let cohort = d.party_cohort[p] as usize;
                if let Some(c) = w.cohort_delta.get_mut(cohort) {
                    *c += amount;
                }
                w.top.push(Impulse {
                    unix: slot_to_unix(tx_slots[d.mv_tx[mv] as usize]),
                    amount,
                    cohort: d.party_cohort[p],
                    party: d.mv_party[mv],
                });
                mv += 1;
            }
            // Keep only the loudest impulses: a window can hold thousands
            // on a busy token, and a reactive surface renders spikes, not
            // a ledger. Aggregates above stay EXACT over every movement.
            w.top
                .sort_by_key(|im| std::cmp::Reverse(im.amount.unsigned_abs()));
            w.top.truncate(MAX_IMPULSES);
            events.push(w);
            prev_tx = tx_end;
            snapshots.push(bal.clone());
        }

        if let Some(wallet) = wallet {
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
    }

    SampleSeries {
        cohorts: spine.cohorts.clone(),
        supply: spine.nominal_supply,
        decimals: spine.asset.decimals,
        milestones: milestones(&rows, &pools, &spine.cohorts),
        rows,
        pools,
        pool_series,
        top_holders: top,
        holder_series,
        events,
    }
}

/// Derive the named moments from the sampled series and pool metadata.
///
/// Sample-resolution by construction: a milestone says "by this sample it
/// had happened", which is the honest claim for a series that does not
/// carry every block. Ordering is chronological so a timeline can walk it
/// forward.
fn milestones(rows: &[SampleRow], pools: &[PoolInfo], cohorts: &[String]) -> Vec<Milestone> {
    let mut out = Vec::new();
    if let Some(first) = rows.first() {
        out.push(Milestone {
            unix: first.unix,
            kind: MilestoneKind::Mint,
            label: "mint".to_string(),
        });
    }

    // Every venue's opening, and the FIRST of them is graduation — the
    // end of the bonding phase. Emitted as two milestones on purpose: a
    // view may want to mark the regime change differently from "another
    // venue appeared".
    let mut opened: Vec<&PoolInfo> = pools.iter().filter(|p| p.first_seen_unix > 0).collect();
    opened.sort_by_key(|p| p.first_seen_unix);
    if let Some(g) = opened.first() {
        out.push(Milestone {
            unix: g.first_seen_unix,
            kind: MilestoneKind::Graduation,
            label: "graduated".to_string(),
        });
    }
    for p in &opened {
        out.push(Milestone {
            unix: p.first_seen_unix,
            kind: MilestoneKind::PoolOpened,
            label: format!("{} LP", p.dex),
        });
    }

    // First burn: the sample at which a proven-unspendable sink first
    // holds supply.
    if let Some(bi) = cohorts.iter().position(|c| c == "burn") {
        if let Some(r) = rows
            .iter()
            .find(|r| r.totals.get(bi).is_some_and(|v| *v > 0))
        {
            out.push(Milestone {
                unix: r.unix,
                kind: MilestoneKind::FirstBurn,
                label: "first burn".to_string(),
            });
        }
    }

    // First maturity: locked supply becoming claimable is the arrival of
    // sell pressure, and nothing in a level chart marks it.
    if let Some(r) = rows.iter().find(|r| r.vest_matured > 0) {
        out.push(Milestone {
            unix: r.unix,
            kind: MilestoneKind::VestingUnlock,
            label: "vesting claimable".to_string(),
        });
    }

    out.sort_by_key(|m| m.unix);
    out
}
