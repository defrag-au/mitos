//! The price and volume SERIES — one fold, every time-based statistic.
//!
//! # Why one module rather than six
//!
//! 24-hour volume, the 1h/24h/7d/30d changes, the all-time high and the
//! all-time low look like six features. They are one: a price at every moment
//! it changed, and a value for every trade. Computed separately they would be
//! six passes over the archive that could disagree about what the price was at
//! a given slot — which is the drift [`crate::view`] exists to end, in its
//! time-series form.
//!
//! # ⚠️ THIS COULD NOT HAVE WORKED BEFORE 2026-09-11
//!
//! A trade is valued at the pool state prevailing when it happened, so a window
//! with no pool observations in it has nothing to value its trades against. The
//! volatile tail wrote movements and **no observations**, so the most recent
//! ~32 hours held every trade and no price — precisely the window "24-hour
//! volume" is about. Fixing that (`reverse::scan_blocks` now observes) is what
//! made this module possible, not merely easier.
//!
//! # The two rules
//!
//! 1. **A trade is valued at the PRE-SWAP price.** `Kind::rank` sorts `Fill`
//!    (3) before `PoolState` (10) inside a slot, so when a fill is folded the
//!    view still holds the pool as it stood before the swap. That is the price
//!    the trader saw; the post-swap state is the consequence, not the terms.
//! 2. **Only fills count toward volume.** A swap is two or three transactions —
//!    placement, fill, and sometimes a refund — and counting the placement as
//!    well would double every figure. `BatchedFill` counts its amount ONCE and
//!    is never decomposed.

use crate::price::Unit;
use crate::story::{Kind, Story};
use crate::view::{PolicyView, Projection};

/// The price at one moment. Points exist only where the price CHANGED — a
/// piecewise-constant series, the same shape the archive itself has.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    pub slot: u64,
    pub unix: u64,
    /// Lovelace per RAW unit. ⚠️ Never scaled by decimals here: this crate
    /// does not know them, and a series that guessed would be wrong by 10ⁿ.
    pub spot: f64,
}

/// One trade, valued.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Trade {
    pub slot: u64,
    pub unix: u64,
    /// Units that changed hands, always positive.
    pub units: i64,
    /// `units × spot`, in lovelace. ⚠️ Zero where no ADA pool had been
    /// observed yet — a trade we can see and cannot value. Counted by
    /// [`Series::unvalued`] rather than dropped.
    pub lovelace: i64,
    /// True when the asset went INTO the pool: the holder sold.
    pub sold: bool,
}

/// The folded series.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Series {
    /// Slot-ascending, one per price change.
    pub points: Vec<Point>,
    /// Slot-ascending.
    pub trades: Vec<Trade>,
    /// Trades seen before any ADA pool was observed, so they could not be
    /// valued. ⚠️ Reported: a volume figure missing trades should say so, not
    /// look small.
    pub unvalued: u64,
}

/// Fold a story into the series.
///
/// The story must be slot-ascending — [`crate::story::build`] guarantees it.
pub fn fold(story: &Story, p: &Projection) -> Series {
    let mut view = PolicyView::default();
    let mut out = Series::default();
    // The last price pushed, so a repeated sighting at an unchanged rate does
    // not become a point. A pool is re-observed on every touch, and most
    // touches do not move the aggregate.
    let mut last: Option<f64> = None;

    for e in &story.events {
        match &e.kind {
            // ⚠️ VALUED FIRST, before the state that this trade caused is
            // folded — see rule 1. `Kind::rank` guarantees the ordering; this
            // relies on it and would silently use the post-swap price if the
            // ranks were ever reordered.
            Kind::Fill {
                amount, into_pool, ..
            } => {
                out.push_trade(e.slot, e.unix, *amount, last, *into_pool);
            }
            Kind::BatchedFill { amount, .. } => {
                out.push_trade(e.slot, e.unix, *amount, last, false);
            }
            Kind::PoolState(_) | Kind::CurveState { .. } => {
                view.apply_event(e, &story.pools);
                // Evaluated AT THIS SLOT, so the staleness horizon is measured
                // from the moment being described rather than from now. A pool
                // last touched on day 1 IS the market on day 20 and is dead by
                // day 200 — the same reserves, opposite verdicts.
                let spot = view.spot(e.slot, p).lovelace_per_unit();
                if let Some(spot) = spot
                    && last.is_none_or(|l| l != spot)
                {
                    out.points.push(Point {
                        slot: e.slot,
                        unix: e.unix,
                        spot,
                    });
                    last = Some(spot);
                }
            }
            _ => {}
        }
    }
    out
}

impl Series {
    fn push_trade(&mut self, slot: u64, unix: u64, amount: i64, spot: Option<f64>, sold: bool) {
        let units = amount.abs();
        let lovelace = match spot {
            Some(s) => (units as f64 * s) as i64,
            None => {
                self.unvalued += 1;
                0
            }
        };
        self.trades.push(Trade {
            slot,
            unix,
            units,
            lovelace,
            sold,
        });
    }

    /// The newest price. `None` on a policy that never had an ADA pool —
    /// undefined, not zero.
    pub fn spot(&self) -> Option<f64> {
        self.points.last().map(|p| p.spot)
    }

    /// The price as it stood at `unix`: the last point at or before it.
    ///
    /// ⚠️ `None` BEFORE the first point, never the first price. A token did not
    /// trade at its launch price for the months preceding its launch, and a
    /// "30-day change" computed against a price that did not exist is an
    /// invented number.
    pub fn spot_at(&self, unix: u64) -> Option<f64> {
        let i = self.points.partition_point(|p| p.unix <= unix);
        (i > 0).then(|| self.points[i - 1].spot)
    }

    /// Percentage change over the last `secs`, as of `now`.
    pub fn change_pct(&self, now: u64, secs: u64) -> Option<f64> {
        let then = self.spot_at(now.saturating_sub(secs))?;
        let now_spot = self.spot_at(now).or_else(|| self.spot())?;
        (then > 0.0).then(|| (now_spot - then) / then * 100.0)
    }

    /// Highest and lowest price the series ever reached.
    pub fn high(&self) -> Option<Point> {
        self.points
            .iter()
            .copied()
            .max_by(|a, b| a.spot.total_cmp(&b.spot))
    }

    pub fn low(&self) -> Option<Point> {
        self.points
            .iter()
            .copied()
            .min_by(|a, b| a.spot.total_cmp(&b.spot))
    }

    /// Lovelace traded since `unix`, and how many trades that was.
    pub fn volume_since(&self, unix: u64) -> (i64, u64) {
        let i = self.trades.partition_point(|t| t.unix < unix);
        let slice = &self.trades[i..];
        (slice.iter().map(|t| t.lovelace).sum(), slice.len() as u64)
    }

    /// Everything a stat strip asks for, from one walk of the series.
    pub fn stats(&self, now: u64) -> Stats {
        let (volume_24h, trades_24h) = self.volume_since(now.saturating_sub(DAY));
        Stats {
            spot: self.spot(),
            change_1h: self.change_pct(now, HOUR),
            change_24h: self.change_pct(now, DAY),
            change_7d: self.change_pct(now, 7 * DAY),
            change_30d: self.change_pct(now, 30 * DAY),
            volume_24h_lovelace: volume_24h,
            trades_24h,
            high: self.high(),
            low: self.low(),
            points: self.points.len(),
            trades: self.trades.len(),
            unvalued: self.unvalued,
        }
    }
}

pub const HOUR: u64 = 3_600;
pub const DAY: u64 = 86_400;

/// The stat strip, in one struct.
#[derive(Debug, Clone, PartialEq)]
pub struct Stats {
    /// Lovelace per RAW unit. `None` where no ADA pool was ever observed.
    pub spot: Option<f64>,
    /// ⚠️ Each is `None` where the series does not reach back that far — NOT
    /// 0%. "Unchanged over 30 days" and "we have 3 days of history" are
    /// different statements and a reader acts differently on them.
    pub change_1h: Option<f64>,
    pub change_24h: Option<f64>,
    pub change_7d: Option<f64>,
    pub change_30d: Option<f64>,
    pub volume_24h_lovelace: i64,
    pub trades_24h: u64,
    pub high: Option<Point>,
    pub low: Option<Point>,
    /// How much series there is — the honest bound on everything above.
    pub points: usize,
    pub trades: usize,
    /// Trades that could not be valued; see [`Series::unvalued`].
    pub unvalued: u64,
}

/// A quote unit the series can price against. ADA only, today — the same limit
/// `crate::price` documents, restated here so a caller does not assume a
/// token-paired series exists.
pub fn quote_unit() -> Unit {
    Unit::ada()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observation::pricing;
    use crate::story::{PoolIdent, PoolState, StoryEvent, Trader};

    fn story_of(events: Vec<StoryEvent>) -> Story {
        Story {
            events,
            pools: vec![PoolIdent {
                venue: "splash".into(),
                address: "addr_a".into(),
                key_policy: vec![0u8; 28],
                key_name: b"K".to_vec(),
            }],
            distinct_slots: 1,
            markers: Vec::new(),
        }
    }

    fn ev(slot: u64, kind: Kind) -> StoryEvent {
        StoryEvent {
            slot,
            unix: 1_700_000_000 + slot,
            tx: vec![slot as u8],
            unit: b"U".to_vec(),
            kind,
        }
    }

    fn pool(base: i64, quote: i64) -> Kind {
        Kind::PoolState(PoolState {
            pool: 0,
            base,
            quote: Some(quote),
            quote_policy: None,
            quote_name: None,
            pricing: pricing::CONSTANT_PRODUCT.into(),
        })
    }

    fn fill(amount: i64, into_pool: bool) -> Kind {
        Kind::Fill {
            venue: "splash".into(),
            party: Trader::Unknown,
            amount,
            into_pool,
        }
    }

    /// 🔑 A trade is valued at the price BEFORE it moved the pool. The story
    /// sorts `Fill` before `PoolState` inside a slot for exactly this.
    #[test]
    fn a_trade_is_valued_at_the_pre_swap_price() {
        let s = fold(
            &story_of(vec![
                ev(10, pool(1_000, 10_000)), // spot 10
                ev(20, fill(100, true)),     // valued at 10 → 1,000
                ev(20, pool(1_100, 20_000)), // spot ~18.18 AFTER the swap
            ]),
            &Projection::default(),
        );
        assert_eq!(s.trades.len(), 1);
        assert_eq!(s.trades[0].lovelace, 1_000, "pre-swap price, not post");
        assert_eq!(s.unvalued, 0);
    }

    /// ⚠️ A trade before any pool sighting cannot be valued, and saying so
    /// beats a volume figure that is quietly short.
    #[test]
    fn a_trade_with_no_price_yet_is_counted_as_unvalued() {
        let s = fold(
            &story_of(vec![ev(5, fill(100, false)), ev(10, pool(1_000, 10_000))]),
            &Projection::default(),
        );
        assert_eq!(s.unvalued, 1);
        assert_eq!(s.trades[0].lovelace, 0);
        assert_eq!(s.volume_since(0).0, 0, "and it adds nothing to volume");
    }

    /// Points exist only where the price MOVED — a pool is re-observed on every
    /// touch and most touches do not move the aggregate.
    #[test]
    fn an_unchanged_price_does_not_add_a_point() {
        let s = fold(
            &story_of(vec![
                ev(10, pool(1_000, 10_000)),
                ev(20, pool(1_000, 10_000)),
                ev(30, pool(1_000, 20_000)),
            ]),
            &Projection::default(),
        );
        assert_eq!(s.points.len(), 2);
        assert_eq!(s.points[0].spot, 10.0);
        assert_eq!(s.points[1].spot, 20.0);
    }

    /// ⚠️ `None` before the first point, NOT the first price. A 30-day change
    /// on a 3-day-old token would otherwise read 0% instead of "unknown".
    #[test]
    fn a_change_over_a_window_the_series_does_not_reach_is_unknown() {
        let base = 1_700_000_010;
        let s = fold(
            &story_of(vec![
                ev(10, pool(1_000, 10_000)),
                ev(20, pool(1_000, 20_000)),
            ]),
            &Projection::default(),
        );
        assert_eq!(s.spot_at(base - 1), None, "before the first point");
        // 30 days back is before the series begins.
        assert_eq!(s.change_pct(base + 20, 30 * DAY), None);
        // Across the two points it is a real +100%.
        let c = s.change_pct(base + 20, 15).expect("in range");
        assert!((c - 100.0).abs() < 1e-6, "got {c}");
    }

    #[test]
    fn high_and_low_are_the_whole_series() {
        let s = fold(
            &story_of(vec![
                ev(10, pool(1_000, 10_000)), // 10
                ev(20, pool(1_000, 50_000)), // 50
                ev(30, pool(1_000, 5_000)),  // 5
            ]),
            &Projection::default(),
        );
        assert_eq!(s.high().unwrap().spot, 50.0);
        assert_eq!(s.low().unwrap().spot, 5.0);
        assert_eq!(s.high().unwrap().slot, 20);
    }

    /// ⚠️ Only fills count. A placement is the SAME units moving into the order
    /// contract, so counting it would double every volume figure.
    #[test]
    fn a_placement_is_not_volume() {
        let s = fold(
            &story_of(vec![
                ev(10, pool(1_000, 10_000)),
                ev(
                    20,
                    Kind::Placement {
                        venue: "splash".into(),
                        party: Trader::Unknown,
                        amount: 100,
                    },
                ),
                ev(21, fill(100, true)),
            ]),
            &Projection::default(),
        );
        assert_eq!(s.trades.len(), 1, "the placement is not a trade");
        assert_eq!(s.volume_since(0).0, 1_000);
    }

    #[test]
    fn volume_is_windowed_by_time_not_by_count() {
        let base = 1_700_000_000;
        let s = fold(
            &story_of(vec![
                ev(10, pool(1_000, 10_000)),
                ev(20, fill(100, true)),
                ev(30, fill(200, false)),
            ]),
            &Projection::default(),
        );
        // Everything.
        assert_eq!(s.volume_since(0), (3_000, 2));
        // Only the newer trade.
        assert_eq!(s.volume_since(base + 25), (2_000, 1));
        // Nothing.
        assert_eq!(s.volume_since(base + 100), (0, 0));
    }
}
