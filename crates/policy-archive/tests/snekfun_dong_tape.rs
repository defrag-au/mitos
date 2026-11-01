//! A THIRD tape: $Dong, the oldest launch here — 2024-09, tier A, and a curve
//! whose last step was half the remaining inventory.
//!
//! # Provenance
//!
//! `fixtures/snekfun/dong-launch.json` is the whole
//! `GET /policy/141eea77…/launch?format=json` response from cardano-infra's
//! token-ledger serve, fetched 2026-10-25 — the day the policy was walked, which
//! took **93.1 s** from admission to complete for 14,120 rows across two years
//! of history. Nothing in this file comes from snek.fun.
//!
//! # What this tape adds
//!
//! - **A third tier-A token, with the SAME graduation inventory** as $PERP:
//!   261,194,026 tokens against $PERP's 261,194,031, five apart. The cap is a
//!   market-cap target, so two launches a year apart stop within five raw units
//!   of each other on the same cap. That is the tier constant, measured twice.
//! - **The creator's share matches the lifecycle doc exactly**: 201,339,019
//!   tokens (20.13%) for 463 ADA.
//! - **A step that is nothing like a marginal.** The last step crosses
//!   250,456,524 of the 511,650,550 tokens still on the curve — 49% of the
//!   inventory — so the 29.80 it realises is the AVERAGE over nearly half the
//!   remaining range, and the price the curve actually ended at is somewhere
//!   above it. $PERP, at the same inventory, closed at 41.85 on a 3.9M-token
//!   step. The two numbers are not comparable, and neither is a quote.

use policy_archive::curve::{Sighting, SkipReason, launch_price, realised};
use serde::Deserialize;

const SUPPLY: i64 = 1_000_000_000;
const SEED_LOVELACE: i64 = 3_000_000;

/// Tier A, the same datum cap as $PERP.
const CAP_LOVELACE: i64 = 10_991_175_000;
const GRADUATION_TOKENS_LEFT: i64 = 261_194_026;

/// $PERP's graduation inventory, from its own tape. Five raw units apart on the
/// same cap, a year apart in time.
const PERP_GRADUATION_TOKENS_LEFT: i64 = 261_194_031;

const TAPE: &str = include_str!("fixtures/snekfun/dong-launch.json");

#[derive(Deserialize)]
struct Tape {
    points: Vec<Row>,
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

fn sightings() -> Vec<Sighting> {
    serde_json::from_str::<Tape>(TAPE)
        .expect("the Dong tape")
        .points
        .into_iter()
        .map(sighting)
        .collect()
}

/// Eleven sightings over nine slots, ending on the tier-A cap — and within five
/// raw units of $PERP's inventory, a year later.
#[test]
fn two_tier_a_launches_stop_within_five_raw_units_of_each_other() {
    let sightings = sightings();
    assert_eq!(sightings.len(), 11);

    let slots: std::collections::BTreeSet<u64> = sightings.iter().map(|s| s.slot).collect();
    assert_eq!(slots.len(), 9, "two slots carry more than one sighting");

    let first = sightings[0];
    assert_eq!(first.slot, 133_983_726, "the launch slot, 2024-09-05");
    assert_eq!(first.unix, 1_725_550_017);
    assert_eq!(first.lovelace, 463_000_000);

    let last = sightings[sightings.len() - 1];
    assert_eq!(last.lovelace, CAP_LOVELACE, "the curve reached its cap");
    assert_eq!(last.tokens_left, GRADUATION_TOKENS_LEFT);
    assert_eq!(
        PERP_GRADUATION_TOKENS_LEFT - GRADUATION_TOKENS_LEFT,
        5,
        "the same market-cap target, a year apart"
    );

    // 5,276 slots ≈ 1.47 hours of bonding, against $PERP's 56 seconds and
    // $Aliens's 6.8 days.
    assert_eq!(last.unix - first.unix, 5_276);
}

/// A launch buy, priced from the seed, matching the lifecycle doc.
#[test]
fn the_creator_bought_twenty_percent_for_463_ada() {
    let first = sightings()[0];

    let bought = SUPPLY - first.tokens_left;
    assert_eq!(bought, 201_339_019, "20.13% of supply");
    assert_eq!(
        first.lovelace - SEED_LOVELACE,
        460_000_000,
        "460 ADA above the seed is the doc's 463 ADA in the curve"
    );

    let price = launch_price(&first, SUPPLY, SEED_LOVELACE).expect("a launch that bought");
    assert!(
        (price - 460_000_000.0 / 201_339_019.0).abs() < 1e-9,
        "the launch realised {price}"
    );
    assert!((2.28..2.29).contains(&price), "{price}");
}

/// Every step is a trade, and the fold is an identity with no adjustment.
#[test]
fn every_step_is_a_trade_and_none_moves_ada_alone() {
    let sightings = sightings();
    let fold = realised(&sightings);

    assert_eq!(fold.realised.len(), 10);
    assert!(fold.skipped.is_empty(), "{:?}", fold.skipped);

    let (first, last) = (sightings[0], sightings[sightings.len() - 1]);
    assert_eq!(last.lovelace - first.lovelace, 10_528_175_000);
    assert_eq!(first.tokens_left - last.tokens_left, 537_466_955);
}

/// ⚠️ The last step is an AVERAGE over half the remaining curve, not a marginal.
///
/// 250,456,524 of 511,650,550 tokens — 49% of the inventory — moved in that one
/// step. On a curve that only rises as tokens leave, the average over a step is
/// BELOW the price at its end, so **29.80 is a lower bound on the graduation
/// marginal, not the marginal**. The step before it crossed 101.9M tokens and
/// realised 15.637, which brackets the handover between them.
#[test]
fn the_last_step_is_an_average_not_a_marginal() {
    let sightings = sightings();
    let fold = realised(&sightings);

    let before = sightings[sightings.len() - 2];
    let crossed = before.tokens_left - GRADUATION_TOKENS_LEFT;
    assert_eq!(crossed, 250_456_524);
    assert_eq!(before.tokens_left, 511_650_550);
    // 48.95%, not "about a third": this is the step a reader would mistake for
    // a graduation quote if it were labelled one.
    assert!(
        crossed * 100 / before.tokens_left >= 48,
        "the step crossed {crossed} of {} tokens left",
        before.tokens_left
    );

    let last = fold.realised.last().expect("a last trade");
    assert!((last.spot - 29.8024).abs() < 1e-3, "{}", last.spot);

    let previous = fold.realised[fold.realised.len() - 2];
    assert!((previous.spot - 15.6372).abs() < 1e-3, "{}", previous.spot);
    assert!(
        last.spot > previous.spot,
        "the curve was rising, so the later average is the higher one"
    );
}

/// The third tape in which no step moves ADA without a token — the claim now
/// holds across three regimes and two years.
#[test]
fn nothing_here_is_anything_but_a_trade() {
    let fold = realised(&sightings());
    assert!(
        !fold.skipped.iter().any(|s| matches!(
            s.reason,
            SkipReason::OppositeSides | SkipReason::OutOfOrder | SkipReason::NoTokenMovement
        )),
        "{:?}",
        fold.skipped
    );
}
