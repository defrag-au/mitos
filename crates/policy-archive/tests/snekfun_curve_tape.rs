//! The FOLD against the real tape — 446 sightings of $Aliens's curve, out of our
//! own archive.
//!
//! # Provenance
//!
//! `fixtures/snekfun/aliens-launch.json` is the whole
//! `GET /policy/16657df…/launch?format=json` response from cardano-infra's
//! token-ledger serve, fetched 2026-10-25 and copied **byte for byte** —
//! `progress` and the endpoint's own metadata included, so there is nothing to
//! doubt about the transcription. The test reads the four fields [`Sighting`]
//! carries and ignores the rest. Nothing in this file comes from snek.fun.
//!
//! # Why this test exists
//!
//! [`policy_archive::curve::realised`] was written against two endpoints and a
//! sample of chart bars. That is enough to pin a unit and a shape, and not
//! enough to trust a price: it never saw the middle of a curve, the sells, or
//! the inside of a slot that carries several trades. This tape is the middle,
//! the sells, and 54 slots with more than one sighting.
//!
//! What it settles, MEASURED 2026-10-25:
//!
//! - **442 of 445 steps are trades.** The three that are not are rounding
//!   inside a slot — 21, 11 and 11 lovelace with no token movement, 43 lovelace
//!   in total. There is no settlement in this tape.
//! - **No step has the two sides moving the same way**, so the fold's
//!   `OppositeSides` arm is unexercised here rather than wrong. 126 steps have
//!   `tokens_left` RISING, and those are sales: the curve trades both ways
//!   before it graduates.
//! - **The fold is an identity over the whole tape**: every step's delta plus
//!   the three rounding steps sum to `last − first` on BOTH sides, to the
//!   lovelace. The priced steps alone land 43 lovelace short, because those
//!   three steps moved ADA with no token — which is the identity, not an
//!   exception to it.
//! - **The curve steepens by 2.6×**, which is why `Bucket::OffModel` refuses to
//!   price it. Under constant product the same reserves would give 1.48×, so
//!   the difference is not a rounding argument.
//!
//! # The oracle, and what it says
//!
//! Against `charts.snek.fun`'s `min1` series the fold agrees per trade to about
//! 1%: the last trade is 68.119 by this fold and 69.000 by their close, 1.3%
//! apart. The one step outside 5% is a block that carries two trades, where our
//! figure is the volume-weighted average and theirs is the marginal after the
//! second — lower on a buy, which is the expected direction.
//!
//! ⚠️ **Do not check this against their `hour1` series.** For 16 consecutive
//! hours the bar tagged `T` carries exactly the open, close and volume of the
//! `min1` aggregate over `[T − 3600, T)`, so the hourly series ends 45 minutes
//! before graduation and its last close (49.924) is not the graduation price.
//!
//! ⚠️ **`/launch` sorts within a slot by `(slot, lovelace)`, not by transaction
//! order.** Ascending lovelace is ascending position up the curve for buys, so
//! this tape cannot show a sell followed by a buy inside one block. The three
//! rounding steps are what that sort looks like when two observations are the
//! same state.

use policy_archive::curve::{Sighting, SkipReason, launch_price, realised};
use serde::Deserialize;

/// snek.fun mints exactly this and the chart divides by exactly this — pinned in
/// `snekfun_price_oracle.rs`, not assumed here.
const SUPPLY: i64 = 1_000_000_000;

/// The lovelace a fresh curve holds before anybody buys. The datum's
/// `ada_cap_threshold` INCLUDES it; snek.fun's API reports the cap without it.
const SEED_LOVELACE: i64 = 3_000_000;

/// The cap off the curve's own datum, from the same `/launch` response.
const CAP_LOVELACE: i64 = 18_191_400_000;

/// What the curve still held when it graduated, from the same response. Matches
/// the `y` snek.fun's pool feed publishes for this token, to the unit.
const GRADUATION_TOKENS_LEFT: i64 = 263_536_754;

const TAPE: &str = include_str!("fixtures/snekfun/aliens-launch.json");

/// The endpoint's response. Every other key it carries — `venue`, `state`,
/// `basis`, the per-point `progress` — is ignored here; serde drops what the
/// struct does not name.
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

fn rows() -> Vec<Row> {
    serde_json::from_str::<Tape>(TAPE)
        .expect("the sighting tape")
        .points
}

fn sightings() -> Vec<Sighting> {
    rows().into_iter().map(sighting).collect()
}

/// The tape is the curve's whole life, in curve order, ending on the cap.
///
/// A tape that stopped early would price the bonding phase over a fraction of
/// its range and every average below would be quietly wrong, so this is checked
/// before anything is folded.
#[test]
fn the_tape_is_the_whole_curve_and_ends_exactly_on_the_cap() {
    let rows = rows();
    assert_eq!(rows.len(), 446, "every touch of the curve");

    let slots: std::collections::BTreeSet<u64> = rows.iter().map(|r| r.slot).collect();
    assert_eq!(slots.len(), 373, "54 slots carry more than one sighting");
    assert!(
        rows.windows(2).all(|p| p[0].slot <= p[1].slot),
        "the tape is in curve order, oldest first"
    );

    let first = rows[0];
    assert_eq!(first.slot, 179_562_649, "the launch slot");
    assert_eq!(first.unix, 1_771_128_940);
    assert_eq!(
        first.lovelace, 93_000_000,
        "the seed plus the creator's buy"
    );
    assert_eq!(first.tokens_left, 965_309_014);

    let last = rows[rows.len() - 1];
    assert_eq!(last.slot, 180_147_623);
    assert_eq!(last.unix, 1_771_713_914, "graduation");
    assert_eq!(last.lovelace, CAP_LOVELACE, "the curve reached its cap");
    assert_eq!(last.tokens_left, GRADUATION_TOKENS_LEFT);

    // The two vocabularies are the same number separated by the seed. This is
    // the whole of the "3 ADA" that looks like it went missing between the
    // datum and snek.fun's API.
    assert_eq!(CAP_LOVELACE - SEED_LOVELACE, 18_188_400_000);
}

/// Every step but three is a trade, and the three are rounding.
#[test]
fn the_fold_prices_every_step_except_three_rounding_steps() {
    let sightings = sightings();
    let fold = realised(&sightings);

    assert_eq!(fold.realised.len(), 442);
    assert_eq!(fold.skipped.len(), 3);
    assert!(
        fold.skipped
            .iter()
            .all(|s| s.reason == SkipReason::NoTokenMovement),
        "{:?}",
        fold.skipped
    );

    // The three steps move 21, 11 and 11 lovelace between them. A step that
    // moved real ADA with the token side untouched is a settlement, and would
    // land outside this bound.
    let rounding: i64 = sightings
        .windows(2)
        .filter(|p| p[1].tokens_left == p[0].tokens_left)
        .map(|p| (p[1].lovelace - p[0].lovelace).abs())
        .sum();
    assert_eq!(
        rounding, 43,
        "43 lovelace of rounding, not an ADA settlement"
    );
}

/// No step in the tape moves ADA with the token side untouched, beyond
/// rounding, and none moves both sides the same way.
///
/// An earlier reading of this token reported a 207 ADA settlement at graduation,
/// where the curve reached its cap and the pool was seeded from less with the
/// token side unchanged. **It is not in this tape.** The last row is the cap
/// with the pool feed's exact inventory, and there is no row after it.
#[test]
fn not_one_step_in_the_tape_is_a_settlement() {
    let fold = realised(&sightings());

    let count = |reason: SkipReason| fold.skipped.iter().filter(|s| s.reason == reason).count();
    assert_eq!(
        count(SkipReason::OppositeSides),
        0,
        "a sale is a trade in reverse"
    );
    assert_eq!(
        count(SkipReason::OutOfOrder),
        0,
        "the tape is in curve order"
    );
}

/// The fold is an identity over the whole tape, on both sides.
///
/// This is the strongest thing the tape can say: no lovelace and no token is
/// unaccounted for between the first sighting and the last. A fold that dropped
/// a step, mis-signed one, or priced a step twice breaks it.
#[test]
fn the_fold_accounts_for_every_lovelace_and_token_on_the_curve() {
    let sightings = sightings();
    let fold = realised(&sightings);
    let (first, last) = (sightings[0], sightings[sightings.len() - 1]);

    // Signed, so a sell subtracts and the total lands on the endpoints.
    let mut lovelace = 0i64;
    let mut tokens = 0i64;
    let mut priced = 0usize;
    for pair in sightings.windows(2) {
        let (before, after) = (pair[0], pair[1]);
        let d_lovelace = after.lovelace - before.lovelace;
        let d_tokens = before.tokens_left - after.tokens_left;
        if d_tokens == 0 || (d_lovelace > 0) != (d_tokens > 0) {
            continue;
        }
        lovelace += d_lovelace;
        tokens += d_tokens;
        priced += 1;
    }

    assert_eq!(
        priced,
        fold.realised.len(),
        "the fold priced the same steps"
    );
    assert_eq!(tokens, first.tokens_left - last.tokens_left);
    assert_eq!(tokens, 701_772_260);
    // The three rounding steps moved 43 lovelace with the token side untouched,
    // so the priced steps land exactly 43 short of the endpoint difference.
    assert_eq!(lovelace, last.lovelace - first.lovelace - 43);
    assert_eq!(lovelace, 18_098_399_957);
}

/// The launch buy is inside the first sighting, so it needs the seed rather
/// than a predecessor.
#[test]
fn the_launch_buy_lives_inside_the_first_sighting_only() {
    let sightings = sightings();
    let first = sightings[0];

    let bought = SUPPLY - first.tokens_left;
    assert_eq!(bought, 34_690_986, "the creator's buy at launch");
    assert_eq!(first.lovelace - SEED_LOVELACE, 90_000_000);

    let price = launch_price(&first, SUPPLY, SEED_LOVELACE).expect("a launch that bought");
    assert!(
        (price - 90_000_000.0 / 34_690_986.0).abs() < 1e-9,
        "the launch realised {price}"
    );
    // An AVERAGE over 34.7M tokens, which is why it sits below the chart's high
    // of 2.9514 for the same hour: that high is the marginal at the end of the
    // buy, and this is the rate the buyer actually paid.
    assert!((2.59..2.60).contains(&price), "{price}");
}

/// The phase average and the last trade are 2.6× apart — measured, not modelled.
///
/// This is the number that forbids pricing a bonding curve with constant
/// product. The same reserves under `x·y = k` give a marginal/average ratio near
/// 1.48 over this range.
#[test]
fn the_curve_steepens_because_it_is_not_a_constant_product() {
    let fold = realised(&sightings());
    let last = fold.realised.last().expect("a last trade");

    let priced = 18_098_399_957.0 / 701_772_260.0;
    let ratio = last.spot / priced;
    assert!(
        ratio > 1.5,
        "constant product would be ~1.48, this is {ratio}"
    );
    assert!(
        (ratio - 2.641).abs() < 0.01,
        "last {} / average {priced}",
        last.spot
    );

    // Including the launch buy the phase average is 24.697, and the graduation
    // marginal is 2.758× it. Both readings are recorded because a consumer may
    // reasonably fold from the seed or from the first trade.
    let whole = (CAP_LOVELACE - SEED_LOVELACE) as f64 / (SUPPLY - GRADUATION_TOKENS_LEFT) as f64;
    assert!((whole - 24.697).abs() < 0.001, "{whole}");
    assert!((last.spot / whole - 2.758).abs() < 0.01);

    // Against snek.fun's own `min1` close at graduation (69.0005), 1.3% apart.
    assert!((last.spot - 68.1194).abs() < 1e-3, "{}", last.spot);
}
