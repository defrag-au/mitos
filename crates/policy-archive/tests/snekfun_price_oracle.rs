//! The snek.fun chart as a price ORACLE — their numbers against our fold.
//!
//! # Provenance
//!
//! Both fixtures under `fixtures/snekfun/` were fetched 2026-10-25 and each
//! carries the URL it came from. `aliens-hour1.json` is a SAMPLE of the 162
//! hourly bars `charts.snek.fun` returns for $Aliens's bonding window
//! (`1771128940` → `1771713914`); the values are verbatim, only the selection is
//! ours. Nothing else here is recorded from their API.
//!
//! # What this is not
//!
//! No test reaches the network and no value from snek.fun is a runtime input.
//! A disagreement means the fold is wrong or this reading of their unit is —
//! never that production should ask snek.fun for a price.
//!
//! # What the two sources are for
//!
//! The **pools feed** pins the envelope: the reserves at the cap, the tokens left
//! and the launch time. The **chart** pins the shape and, in its first bar, the
//! unit.
//!
//! ⚠️ **This file no longer checks a price against their chart.** It used to
//! compare the whole-phase average to the close of `aliens-hour1.json`'s last
//! bar and call the ratio a marginal. That bar is not the graduation price: their
//! hourly series labels a bucket by its END, so the series stops 45 minutes
//! before the cap, and the `2.02` it produced was an artefact of the label. The
//! measured figure is 2.76, and it lives in `snekfun_curve_tape.rs`, against our
//! own observation tape rather than theirs. Removed 2026-10-25 — it asserted a
//! guess.

use serde::Deserialize;

/// snek.fun launches mint 1,000,000,000 tokens, and the chart divides by exactly
/// that. [`the_launch_bar_is_the_virtual_reserve_over_supply`] is what proves it:
/// against the token's chain supply (1,000,000,001) the same arithmetic lands
/// 2.5e-9 low.
const SNEK_FUN_SUPPLY: i64 = 1_000_000_000;

const BARS: &str = include_str!("fixtures/snekfun/aliens-hour1.json");
const POOL: &str = include_str!("fixtures/snekfun/aliens-pool.json");
const NOT_A_LAUNCH: &str = include_str!("fixtures/snekfun/not-a-launch.json");

#[derive(Deserialize)]
struct ChartFile {
    bars: Vec<Bar>,
}

/// One `LastBarWs` bar, in the unit snek.fun quotes: lovelace per raw token.
#[derive(Deserialize, Clone)]
struct Bar {
    time: u64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: i64,
}

#[derive(Deserialize)]
struct PoolFile {
    pool: Pool,
}

#[derive(Deserialize)]
struct Pool {
    x: Amount,
    y: Amount,
    #[serde(rename = "adaCapThreshold")]
    ada_cap_threshold: i64,
    /// Exposed, and NOT the virtual reserve — see [`a_num_is_not_a_virtual_reserve`].
    #[serde(rename = "aNum")]
    a_num: i64,
    #[serde(rename = "bNum")]
    b_num: i64,
}

/// An amount on their wire is a decimal string, not a JSON number.
#[derive(Deserialize)]
struct Amount {
    amount: String,
}

fn value(amount: &Amount) -> i64 {
    amount.amount.parse().expect("a decimal amount on the wire")
}

fn bars() -> Vec<Bar> {
    serde_json::from_str::<ChartFile>(BARS)
        .expect("the sampled bars")
        .bars
}

fn pool() -> Pool {
    serde_json::from_str::<PoolFile>(POOL)
        .expect("the pool state")
        .pool
}

/// Their chart's launch price is the virtual reserve over the supply, exactly.
///
/// `bNum` is `2,545,182`, and `× 1000 / 1e9` is `2.545182` — the first bar's
/// open to the last digit. That is the unit: **lovelace per raw token**, the
/// same one [`policy_archive::series::Point::spot`] carries, so a fold's output
/// is comparable without a conversion.
#[test]
fn the_launch_bar_is_the_virtual_reserve_over_supply() {
    let launch = &bars()[0];
    assert_eq!(launch.time, 1_771_131_600, "the first bar of the window");

    let virtual_reserve = pool().b_num as f64 * 1000.0;
    let implied = virtual_reserve / SNEK_FUN_SUPPLY as f64;
    assert!(
        (implied - launch.open).abs() < 1e-9,
        "bNum×1000/supply = {implied}, the chart opens at {}",
        launch.open
    );
}

/// Every bar brackets its own open and close — a fixture sanity check, and the
/// reason `low`/`high` are carried at all.
#[test]
fn every_sampled_bar_brackets_its_own_open_and_close() {
    for bar in bars() {
        assert!(bar.low <= bar.open.min(bar.close), "{}", bar.time);
        assert!(bar.high >= bar.open.max(bar.close), "{}", bar.time);
    }
}

/// $Aliens sat still for 4.6 days after launch, then bonded out in about two.
///
/// The launch hour carries volume and no price move (the creator's buy sets the
/// opening price rather than moving it); the pause carries neither; and the bar
/// that first moves opens exactly where the pause left the price.
#[test]
fn the_chart_is_flat_until_the_curve_is_next_touched() {
    let bars = bars();
    let launch = bars[0].clone();

    assert_eq!(launch.close, launch.open, "no move in the launch hour");
    assert!(
        launch.volume > 0,
        "the launch bought, so the hour has volume"
    );

    for bar in &bars[1..3] {
        assert_eq!(bar.close, launch.open, "{}", bar.time);
        assert_eq!(bar.volume, 0, "{}", bar.time);
    }

    let first_move = &bars[3];
    assert_eq!(first_move.time, 1_771_534_800);
    assert_eq!(
        first_move.open, launch.open,
        "opens where the pause left it"
    );
    assert!(first_move.close > first_move.open);
}

/// The pool feed's own two figures agree with the design doc's independent
/// derivation: "736,463,246 sold for 18,188 ADA".
#[test]
fn the_pool_is_holding_exactly_its_cap() {
    let pool = pool();
    assert_eq!(
        value(&pool.x),
        pool.ada_cap_threshold,
        "the curve is at its cap"
    );
    assert_eq!(value(&pool.y), 263_536_754, "tokens still on the curve");
    assert_eq!(
        SNEK_FUN_SUPPLY - value(&pool.y),
        736_463_246,
        "supply less the inventory left is the doc's sold figure"
    );
}

/// `aNum` is exposed but does not reconcile with the reserves: `122,525` ADA
/// against a curve that held `18,188`, and not the virtual reserve either
/// (`bNum × 1000`). Whatever it parameterises is still open, and the fold
/// deliberately uses neither it nor `bNum` — the whole point of a delta.
#[test]
fn a_num_is_not_a_virtual_reserve() {
    let pool = pool();
    assert_ne!(pool.b_num * 1000, pool.a_num);
}

#[derive(Deserialize)]
struct NotALaunchFile {
    http: u16,
    body: Refusal,
}

#[derive(Deserialize)]
struct Refusal {
    what: String,
}

/// A token that is not a snek.fun launch is refused by NAMING the asset, not
/// answered with an empty pool. Recorded so the "no curve" boundary is a known
/// shape rather than a guess.
///
/// ⚠️ This is the nearest case a local fixture can reach. A snek.fun launch that
/// never graduated is a different case — the feed would answer with a live curve
/// — and pinning it needs a policy id from the launch census, which lives on the
/// box.
#[test]
fn a_token_that_is_not_a_launch_is_refused_by_name() {
    let file: NotALaunchFile = serde_json::from_str(NOT_A_LAUNCH).expect("the recorded 404");
    assert_eq!(file.http, 404);
    assert!(file.body.what.contains("AssetClass"), "{}", file.body.what);
}
