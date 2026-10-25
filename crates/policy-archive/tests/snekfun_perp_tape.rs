//! A SECOND tape, and a DIFFERENT regime: $PERP bonded out inside two blocks.
//!
//! # Provenance
//!
//! `fixtures/snekfun/perp-launch.json` is the whole
//! `GET /policy/d9d59e71…/launch?format=json` response from cardano-infra's
//! token-ledger serve, fetched 2026-10-25 and copied **byte for byte**. Nothing
//! here comes from snek.fun.
//!
//! # Why a second tape, when $Aliens already passes
//!
//! Because $PERP is the shape $Aliens is not, and a fold that only ever met one
//! shape is fitted rather than tested:
//!
//! - **The creator took 70% of supply in the launch transaction.** MEASURED in
//!   `SNEK_FUN_LAUNCH_LIFECYCLE.md` as 9,440 ADA spent in the mint transaction;
//!   this tape's first row is that buy, so the bonding phase opens at 86% of
//!   the cap.
//! - **The phase's own average and its last trade differ by 4.7%**, where
//!   $Aliens's differ by 264%. A consumer that learned a ratio from $Aliens and
//!   applied it here would be wrong by a factor of ~56.
//! - **The full seed→cap range still steepens 2.81×**, so the CURVE is the same
//!   family. Only how much of it the bonding phase traversed differs — which is
//!   the honest way to say "the curve steepens" without pinning a number that
//!   belongs to one token's launch.
//!
//! It also confirms the two things $Aliens could not, on its own: the tape ends
//! exactly on the datum cap, and **no step moves ADA with the token side
//! untouched** — not even at graduation.

use policy_archive::curve::{Sighting, SkipReason, launch_price, realised};
use serde::Deserialize;

const SUPPLY: i64 = 1_000_000_000;
const SEED_LOVELACE: i64 = 3_000_000;

/// The datum's cap, INCLUDING the seed. Tier A: 10,988.175 ADA collected.
const CAP_LOVELACE: i64 = 10_991_175_000;

/// Where the curve stopped. Matches `SNEK_FUN_LAUNCH_LIFECYCLE.md`'s
/// "261,194,031 PERP" for this pool to the unit.
const GRADUATION_TOKENS_LEFT: i64 = 261_194_031;

const TAPE: &str = include_str!("fixtures/snekfun/perp-launch.json");

#[derive(Deserialize)]
struct Tape {
    points: Vec<Row>,
    /// The endpoint's own arithmetic: `(cap − seed) × supply / tokens_left`.
    /// It is the tier's market-cap target, not a reserve — see
    /// `snekfun_price_oracle.rs`, where it refuses to reconcile as one.
    #[serde(default)]
    graduation_market_cap_lovelace: Option<i64>,
}

#[derive(Deserialize, Clone, Copy)]
struct Row {
    slot: u64,
    unix: u64,
    lovelace: i64,
    tokens_left: i64,
}

fn sighting(row: Row) -> Sighting {
    Sighting {
        slot: row.slot,
        unix: row.unix,
        lovelace: row.lovelace,
        tokens_left: row.tokens_left,
    }
}

fn tape() -> Tape {
    serde_json::from_str(TAPE).expect("the PERP tape")
}

fn sightings() -> Vec<Sighting> {
    tape().points.into_iter().map(sighting).collect()
}

/// Eleven sightings, three slots, ending on the cap.
///
/// Nine of the eleven share ONE slot: the phase ran at block speed, which is
/// why `/launch` orders within a slot by curve position rather than by time.
#[test]
fn the_tape_ends_exactly_on_the_tier_a_cap() {
    let tape = tape();
    let rows = &tape.points;
    assert_eq!(rows.len(), 11);

    let slots: std::collections::BTreeSet<u64> = rows.iter().map(|r| r.slot).collect();
    assert_eq!(slots.len(), 3, "nine sightings share the middle slot");

    let first = rows[0];
    assert_eq!(first.slot, 145_246_220, "the launch slot");
    assert_eq!(first.unix, 1_736_812_511);
    assert_eq!(first.lovelace, 9_440_340_600);

    let last = rows[rows.len() - 1];
    assert_eq!(last.unix, 1_736_812_567, "56 s later — the whole phase");
    assert_eq!(last.lovelace, CAP_LOVELACE, "the curve reached its cap");
    assert_eq!(last.tokens_left, GRADUATION_TOKENS_LEFT);

    assert_eq!(CAP_LOVELACE - SEED_LOVELACE, 10_988_175_000);
    assert_eq!(
        tape.graduation_market_cap_lovelace,
        Some(42_069_012_672),
        "the tier-A graduation target"
    );
}

/// Ten steps, ten trades, and the fold is an identity with no adjustment.
#[test]
fn every_step_is_a_trade_and_none_moves_ada_alone() {
    let sightings = sightings();
    let fold = realised(&sightings);

    assert_eq!(fold.realised.len(), 10);
    assert!(fold.skipped.is_empty(), "{:?}", fold.skipped);

    let (first, last) = (sightings[0], sightings[sightings.len() - 1]);
    let lovelace: i64 = last.lovelace - first.lovelace;
    let tokens: i64 = first.tokens_left - last.tokens_left;
    assert_eq!(lovelace, 1_550_834_400);
    assert_eq!(tokens, 38_805_970);

    // No rounding steps at all here, so the identity needs no adjustment: every
    // step of the tape is one of the ten trades.
    let mut priced_lovelace = 0i64;
    let mut priced_tokens = 0i64;
    for pair in sightings.windows(2) {
        let (before, after) = (pair[0], pair[1]);
        let d_lovelace = after.lovelace - before.lovelace;
        let d_tokens = before.tokens_left - after.tokens_left;
        assert!(
            (d_lovelace > 0) == (d_tokens > 0) && d_tokens != 0,
            "slot {} is not a trade",
            after.slot
        );
        priced_lovelace += d_lovelace;
        priced_tokens += d_tokens;
    }
    assert_eq!(priced_lovelace, lovelace);
    assert_eq!(priced_tokens, tokens);
}

/// The creator's buy is the launch transaction, and it crossed 70% of supply.
#[test]
fn the_creator_bought_seventy_percent_inside_the_launch_transaction() {
    let first = sightings()[0];

    let bought = SUPPLY - first.tokens_left;
    assert_eq!(bought, 699_999_999, "70% of supply, to the unit");
    assert_eq!(first.lovelace - SEED_LOVELACE, 9_437_340_600);

    let price = launch_price(&first, SUPPLY, SEED_LOVELACE).expect("a launch that bought");
    assert!(
        (price - 9_437_340_600.0 / 699_999_999.0).abs() < 1e-9,
        "the launch realised {price}"
    );
    // 9,440 ADA for 70% of supply, as the lifecycle doc records it.
    assert!((13.48..13.49).contains(&price), "{price}");
}

/// A phase can be almost flat; the full curve still steepens.
///
/// The last trade is 41.85 and the phase average 39.96 — 1.047×, against
/// $Aliens's 2.64×. Over the FULL seed→cap range the same curve gives 2.81×,
/// so the difference is how much of the curve was left to trade, not a
/// different curve.
#[test]
fn a_phase_can_be_almost_flat_and_the_full_curve_still_steepens() {
    let fold = realised(&sightings());
    let last = fold.realised.last().expect("a last trade");

    let priced: f64 = 1_550_834_400.0 / 38_805_970.0;
    assert!((priced - 39.9638).abs() < 1e-3, "{priced}");
    assert!((last.spot - 41.8519).abs() < 1e-3, "{}", last.spot);

    let phase = last.spot / priced;
    assert!((phase - 1.0473).abs() < 0.001, "phase ratio {phase}");
    assert!(phase < 1.1, "the bonding phase barely moved the price");

    let whole = (CAP_LOVELACE - SEED_LOVELACE) as f64 / (SUPPLY - GRADUATION_TOKENS_LEFT) as f64;
    assert!((whole - 14.8729).abs() < 1e-3, "{whole}");
    let curved = last.spot / whole;
    assert!((curved - 2.814).abs() < 0.01, "full-range ratio {curved}");

    // The same conclusion `snekfun_curve_tape.rs` reaches on $Aliens, without
    // either tape being able to state it alone: the phase ratio is NOT a
    // property of the curve.
    assert!(
        curved > phase * 2.0,
        "the full range bends {curved}× while the phase bent {phase}×"
    );
}

/// The `OppositeSides` arm stays unexercised — a recorded gap, not a claim.
#[test]
fn no_step_in_this_tape_is_anything_but_a_trade() {
    let fold = realised(&sightings());
    assert!(
        !fold
            .skipped
            .iter()
            .any(|s| matches!(s.reason, SkipReason::OppositeSides | SkipReason::OutOfOrder)),
        "{:?}",
        fold.skipped
    );
}
