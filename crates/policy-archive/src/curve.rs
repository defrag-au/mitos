//! A bonding curve's REALISED price — the one price that needs no formula.
//!
//! [`crate::view::Bucket::OffModel`] keeps a snek.fun curve's reserves and
//! refuses to price them, because its rate is not `quote / base` and the curve
//! is unsolved (`SNEK_FUN_LAUNCH_LIFECYCLE.md`). That refusal is about the
//! CURVE. What actually happened is not in doubt: a trade consumes the curve's
//! UTxO and recreates it in one transaction, so the rate the trader got is two
//! numbers subtracted from each other, in the same unit every
//! [`crate::series::Point`] already carries.
//!
//! ```text
//! rate = Δlovelace / Δtokens
//! ```
//!
//! No model, no constants, no formula. What it cannot give is DEPTH — the
//! realisable band still needs the curve solved — which is the split
//! `PRICE_ORACLE.md` already draws between last-traded and depth-weighted
//! prices.
//!
//! ⚠️ **That is an AVERAGE over the step, and only a step small beside the
//! remaining inventory is a marginal.** MEASURED on $Dong: its last step crossed
//! 250,447,526 of the 511,650,550 tokens still on the curve, so the 29.80 it
//! realised is the mean over half the remaining range — where $PERP, whose last
//! step was 3.9M tokens, lands within 1% of the chart's marginal. A consumer
//! plotting [`Point::spot`] is plotting what each step PAID, which is the right
//! thing for a fill and the wrong thing for a quote.
//!
//! ⚠️ **Two adjacent sightings are not always a trade.** A settlement can move
//! ADA with the token side untouched. MEASURED on $Aliens, the curve reached its
//! 18,188.4 ADA cap and the graduated pool was seeded from 17,981.4 ADA against
//! an unchanged 263,536,754 tokens — 207 ADA leaving with no token moving.
//! Divided naively that is an infinite rate. Every step that is not a trade is
//! named in [`Fold::skipped`] rather than dropped or smoothed.
//!
//! ⚠️ **The launch transaction is not a delta.** A curve's first sighting
//! already contains whatever the launch bought, so there is no predecessor to
//! subtract. [`launch_price`] recovers that rate from the seed and the supply
//! instead.

use crate::series::Point;

/// One sighting of a curve's UTxO, as an observation row carries it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sighting {
    pub slot: u64,
    pub unix: u64,
    /// Lovelace the curve held.
    pub lovelace: i64,
    /// Tokens still ON the curve — the unsold inventory.
    pub tokens_left: i64,
}

/// Why the step between two sightings is not a price.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// ADA moved and the token side did not: a fee leaving, a settlement, a
    /// min-ADA change. MEASURED on $Aliens at graduation — 207 ADA, no tokens.
    NoTokenMovement,
    /// Both sides moved the SAME way. A trade moves them in opposite directions:
    /// ADA in takes tokens out, and a sale does the reverse.
    OppositeSides,
    /// The sighting is older than the one before it.
    OutOfOrder,
}

/// One step that produced no price, and why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Skip {
    pub slot: u64,
    pub unix: u64,
    pub reason: SkipReason,
}

/// Every price a curve realised, and every step that was not one.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Fold {
    /// One [`Point`] per trade, in sighting order.
    pub realised: Vec<Point>,
    pub skipped: Vec<Skip>,
}

/// Price every trade in `sightings`, and name every step that is not one.
///
/// `sightings` must be in the order the curve was touched — slot ascending, and
/// within a slot in transaction order. A slot that goes backwards is REPORTED
/// as [`SkipReason::OutOfOrder`] rather than sorted, because the order is the
/// archive's; re-deriving it here would hide a broken reader.
pub fn realised(sightings: &[Sighting]) -> Fold {
    let mut fold = Fold::default();
    for pair in sightings.windows(2) {
        let (before, after) = (pair[0], pair[1]);
        if after.slot < before.slot {
            fold.skipped.push(Skip {
                slot: after.slot,
                unix: after.unix,
                reason: SkipReason::OutOfOrder,
            });
            continue;
        }
        // ADA in and tokens out — both positive on a buy, both negative on a
        // sale. Scaled, not signed: a sale is the same trade in reverse.
        let lovelace = after.lovelace - before.lovelace;
        let tokens = before.tokens_left - after.tokens_left;
        if tokens == 0 {
            if lovelace != 0 {
                fold.skipped.push(Skip {
                    slot: after.slot,
                    unix: after.unix,
                    reason: SkipReason::NoTokenMovement,
                });
            }
            continue;
        }
        if (lovelace > 0) != (tokens > 0) {
            fold.skipped.push(Skip {
                slot: after.slot,
                unix: after.unix,
                reason: SkipReason::OppositeSides,
            });
            continue;
        }
        fold.realised.push(Point {
            slot: after.slot,
            unix: after.unix,
            spot: lovelace.unsigned_abs() as f64 / tokens.unsigned_abs() as f64,
        });
    }
    fold
}

/// The rate the launch transaction itself realised.
///
/// A curve's first sighting holds `seed_lovelace` plus whatever the launch
/// bought, against `supply` minus what that bought — so the pair differences
/// against the seed rather than against a previous sighting.
///
/// `None` when the first sighting still holds the whole supply (nothing was
/// bought at launch, which is the ordinary case) or the two sides disagree.
pub fn launch_price(first: &Sighting, supply: i64, seed_lovelace: i64) -> Option<f64> {
    let tokens = supply - first.tokens_left;
    let lovelace = first.lovelace - seed_lovelace;
    if tokens <= 0 || lovelace <= 0 {
        return None;
    }
    Some(lovelace as f64 / tokens as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(slot: u64, lovelace: i64, tokens_left: i64) -> Sighting {
        Sighting {
            slot,
            unix: slot,
            lovelace,
            tokens_left,
        }
    }

    #[test]
    fn a_buy_is_priced_from_its_two_sides() {
        let fold = realised(&[at(1, 3_000_000, 1_000), at(2, 13_000_000, 900)]);
        assert!(fold.skipped.is_empty(), "{:?}", fold.skipped);
        // 100 ADA for 100 tokens.
        assert_eq!(fold.realised.len(), 1);
        assert_eq!(fold.realised[0].spot, 100_000.0);
    }

    #[test]
    fn a_sale_is_the_same_trade_in_reverse() {
        let fold = realised(&[at(1, 13_000_000, 900), at(2, 8_000_000, 950)]);
        assert_eq!(fold.realised.len(), 1);
        assert_eq!(fold.realised[0].spot, 100_000.0);
    }

    /// The shape MEASURED on $Aliens at graduation: ADA leaves, no token moves.
    #[test]
    fn a_settlement_with_no_token_movement_is_named_not_split() {
        let fold = realised(&[
            at(1, 18_188_400_000, 263_536_754),
            at(2, 17_981_400_000, 263_536_754),
        ]);
        assert!(fold.realised.is_empty(), "207 ADA is not a price");
        assert_eq!(
            fold.skipped,
            [Skip {
                slot: 2,
                unix: 2,
                reason: SkipReason::NoTokenMovement,
            }]
        );
    }

    /// A curve is re-observed on every touch of the token, not only on trades.
    #[test]
    fn a_reobservation_that_changes_nothing_is_silent() {
        let fold = realised(&[at(1, 5, 5), at(2, 5, 5)]);
        assert_eq!(
            fold,
            Fold::default(),
            "nothing moved, so nothing is refused"
        );
    }

    #[test]
    fn sides_that_move_together_are_refused() {
        let fold = realised(&[at(1, 3_000_000, 1_000), at(2, 4_000_000, 1_010)]);
        assert!(fold.realised.is_empty());
        assert_eq!(fold.skipped[0].reason, SkipReason::OppositeSides);
    }

    #[test]
    fn a_step_backwards_in_slot_is_reported_not_sorted() {
        let fold = realised(&[at(5, 5, 5), at(4, 6, 4)]);
        assert!(fold.realised.is_empty());
        assert_eq!(fold.skipped[0].reason, SkipReason::OutOfOrder);
    }

    #[test]
    fn nothing_is_priced_from_nothing() {
        assert_eq!(realised(&[]), Fold::default());
        assert_eq!(realised(&[at(1, 1, 1)]), Fold::default());
    }

    /// A launch can carry a buy inside the mint transaction, and the first
    /// sighting is then the only record of it.
    #[test]
    fn the_launch_price_comes_from_the_seed_not_a_predecessor() {
        let first = at(1, 3_000_000 + 900_000_000, 100);
        assert_eq!(launch_price(&first, 1_000, 3_000_000), Some(1_000_000.0));
    }

    #[test]
    fn a_launch_that_bought_nothing_has_no_price() {
        assert_eq!(
            launch_price(&at(1, 3_000_000, 1_000), 1_000, 3_000_000),
            None
        );
    }
}
