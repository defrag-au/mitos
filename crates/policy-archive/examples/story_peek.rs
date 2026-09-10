//! `story_peek` — read a `/story` wire response and say what is in it.
//!
//! # Why this exists
//!
//! The stream is binary, so `curl | less` tells you nothing, and every defect
//! found in it so far was found by a CONSUMER folding it — venue names that
//! did not join, a pool column read one index early, a token/token pair
//! erasing a measured ADA reserve. Each time, the loop was "rebuild the wasm
//! bundle, deploy, look at the page", which is minutes per question.
//!
//! This is the same fold in one second, next to the archive it came from.
//!
//! ```text
//! curl -s 'localhost:8184/policy/<policy>/story?limit=5000' -o /tmp/s.bin
//! cargo run --release --example story_peek -- /tmp/s.bin
//! ```
//!
//! ⚠️ It is a DIAGNOSTIC, not a spec. The frontend's fold is the thing
//! shipped; when the two disagree, one of them is wrong and neither is
//! automatically right.

use std::collections::BTreeMap;

use policy_archive::story::{Kind, StoryEvent, wire};

fn main() {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: story_peek <wire.bin>");
        std::process::exit(2);
    };
    let bytes = std::fs::read(&path).expect("read wire file");
    let stream = match wire::from_bytes(&bytes) {
        Ok(s) => s,
        // ⚠️ A version refusal is the answer, not a failure: the origin and
        // this build disagree, and reading on would read the bytes at the
        // wrong shape.
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };
    let events = wire::decode(&stream);

    println!("version   {}", stream.version);
    println!("policy    {}", stream.policy);
    println!("window    {} .. {}", stream.from_slot, stream.to_slot);
    println!("complete  {}", stream.complete);
    println!("events    {}", events.len());
    println!("markers   {}", stream.markers.len());
    println!("bytes     {}", bytes.len());

    // ── kinds ────────────────────────────────────────────────────────────
    let mut kinds: BTreeMap<&str, usize> = BTreeMap::new();
    for e in &events {
        *kinds.entry(kind_name(&e.kind)).or_default() += 1;
    }
    println!("\nkinds");
    for (k, n) in &kinds {
        println!("  {k:<16} {n:>7}");
    }

    // ── venues, BY POOL ──────────────────────────────────────────────────
    //
    // ⚠️ `(venue, pool)`, because grouping by venue alone is precisely the
    // bug this was written to catch: one venue runs many pools and the newest
    // sighting is whichever moved last, not the venue's state.
    println!("\npools  (venue / pool key)");
    let mut pools: BTreeMap<(String, String), (i64, Option<i64>, String, String, usize)> =
        BTreeMap::new();
    for e in &events {
        if let Kind::PoolState(p) = &e.kind {
            let key = (
                p.venue.clone(),
                p.pool.as_deref().map(hex).unwrap_or_else(|| "—".into()),
            );
            let quote_unit = match p.quote_policy.as_deref() {
                None | Some([]) => "ADA".to_string(),
                Some(pol) => hex(&pol[..pol.len().min(6)]),
            };
            let n = pools.get(&key).map(|e| e.4).unwrap_or(0) + 1;
            pools.insert(
                key,
                (p.base, p.quote, quote_unit, p.pricing.clone(), n),
            );
        }
    }
    for ((venue, pool), (base, quote, unit, pricing, n)) in &pools {
        let quote = match quote {
            Some(q) if unit == "ADA" => format!("{} ADA", q / 1_000_000),
            Some(q) => format!("{q} {unit}"),
            None => "not published".into(),
        };
        println!("  {venue:<14} {:<14} base {base:>18}  quote {quote:<22} {pricing:<17} ×{n}",
            elide(pool));
    }

    // ── fills, by venue — these must JOIN the pools above ────────────────
    let mut fills: BTreeMap<&str, (usize, usize)> = BTreeMap::new();
    for e in &events {
        if let Kind::Fill {
            venue, into_pool, ..
        } = &e.kind
        {
            let f = fills.entry(venue.as_str()).or_default();
            f.0 += 1;
            if *into_pool {
                f.1 += 1;
            }
        }
    }
    println!("\nfills  (venue names MUST match the pool table above)");
    for (venue, (n, sold)) in &fills {
        let has_pool = pools.keys().any(|(v, _)| v == venue);
        let flag = match has_pool {
            true => "",
            false => "   ⚠️ NO POOL OF THIS NAME — the two halves are not joining",
        };
        println!("  {venue:<14} {n:>6} fills  ({sold} sells){flag}");
    }
}

/// ⚠️ EXHAUSTIVE, deliberately — no `_` arm. An "other" bucket is how a
/// diagnostic lies to you: this printed `other 507` for a window whose real
/// answer was that its pool sightings had all landed in `CurveState`.
fn kind_name(k: &Kind) -> &'static str {
    match k {
        Kind::Mint { .. } => "mint",
        Kind::Burn { .. } => "burn",
        Kind::Transfer { .. } => "transfer",
        Kind::ArrivedFromBelowFloor { .. } => "below-floor",
        Kind::Ambiguous { .. } => "ambiguous",
        Kind::Fill { .. } => "fill",
        Kind::Placement { .. } => "placement",
        Kind::Cancellation { .. } => "cancellation",
        Kind::BatchedFill { .. } => "batched-fill",
        Kind::PoolState(_) => "pool-state",
        Kind::CurveState { .. } => "curve-state",
        Kind::UnclaimedScript { .. } => "unclaimed",
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn elide(s: &str) -> String {
    match s.len() > 12 {
        true => format!("{}…", &s[..11]),
        false => s.to_string(),
    }
}

/// The peek is only as good as the stream, so it re-states nothing it cannot
/// see: an absent field prints as absent, never as zero.
#[allow(dead_code)]
fn _events_are_slot_ascending(events: &[StoryEvent]) -> bool {
    events.windows(2).all(|w| w[0].slot <= w[1].slot)
}
