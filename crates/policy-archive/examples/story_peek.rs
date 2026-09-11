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

/// One pool as the window last saw it, plus how often it was seen.
struct Seen {
    base: i64,
    quote: Option<i64>,
    quote_unit: String,
    pricing: String,
    sightings: usize,
}

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
    let story = wire::decode(&stream);
    let events = &story.events;

    println!("version   {}", stream.version);
    println!("policy    {}", stream.policy);
    println!("window    {} .. {}", stream.from_slot, stream.to_slot);
    println!("complete  {}", stream.complete);
    println!("events    {}", events.len());
    println!("markers   {}", stream.markers.len());
    println!("bytes     {}", bytes.len());

    // ── kinds ────────────────────────────────────────────────────────────
    let mut kinds: BTreeMap<&str, usize> = BTreeMap::new();
    for e in events {
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
    let mut pools: BTreeMap<(String, String), Seen> = BTreeMap::new();
    for e in events {
        if let Kind::PoolState(p) = &e.kind {
            let key = pool_label(&story, p.pool);
            let sightings = pools.get(&key).map(|s| s.sightings).unwrap_or(0) + 1;
            pools.insert(
                key,
                Seen {
                    base: p.base,
                    quote: p.quote,
                    // ADA is an empty policy — or none at all.
                    quote_unit: match p.quote_policy.as_deref() {
                        None | Some([]) => "ADA".to_string(),
                        Some(pol) => hex(&pol[..pol.len().min(6)]),
                    },
                    pricing: p.pricing.clone(),
                    sightings,
                },
            );
        }
    }
    // ⚠️ A CURVE IS ITS OWN KIND. `build` emits `CurveState` for bonding-curve
    // pricing and `PoolState` for everything else, so a table of `PoolState`
    // alone reports a launchpad token as having no venue — which is how
    // $Aliens showed 439 snek.fun fills against nothing at all.
    for e in events {
        if let Kind::CurveState {
            pool,
            lovelace,
            tokens_left,
            progress,
        } = &e.kind
        {
            let key = pool_label(&story, *pool);
            let sightings = pools.get(&key).map(|s| s.sightings).unwrap_or(0) + 1;
            pools.insert(
                key,
                Seen {
                    base: *tokens_left,
                    quote: Some(*lovelace),
                    quote_unit: "ADA".to_string(),
                    pricing: match progress {
                        Some(p) => format!("curve {:.0}%", p * 100.0),
                        None => "curve (datum?)".to_string(),
                    },
                    sightings,
                },
            );
        }
    }
    for ((venue, pool), s) in &pools {
        let quote = match s.quote {
            Some(q) if s.quote_unit == "ADA" => format!("{} ADA", q / 1_000_000),
            Some(q) => format!("{q} {}", s.quote_unit),
            // ⚠️ Not zero.
            None => "not published".into(),
        };
        println!(
            "  {venue:<14} {:<14} base {:>18}  quote {quote:<22} {:<17} ×{}",
            elide(pool),
            s.base,
            s.pricing,
            s.sightings,
        );
    }

    // ── UNCLAIMED, by payment credential ─────────────────────────────────
    //
    // The visible edge of our coverage, and the most actionable thing in the
    // stream: a credential holding a lot of the asset that no decoder claims
    // is the next decoder worth writing. Grouped by the PAYMENT part, because
    // an order contract derives its stake part per trader — so one contract
    // shows up as hundreds of addresses and one credential.
    let mut unclaimed: BTreeMap<String, (usize, usize, i64)> = BTreeMap::new();
    for e in events {
        if let Kind::UnclaimedScript {
            address, amount, ..
        } = &e.kind
        {
            let cred = policy_archive::trade::address_parts(address)
                .map(|(c, _)| c)
                .unwrap_or_else(|| address.clone());
            let entry = unclaimed.entry(cred).or_default();
            entry.0 += 1;
            entry.2 = entry.2.max(*amount);
        }
    }
    // Distinct addresses per credential — the tell for a per-trader contract.
    let mut addrs: BTreeMap<String, std::collections::HashSet<&str>> = BTreeMap::new();
    for e in events {
        if let Kind::UnclaimedScript { address, .. } = &e.kind {
            let cred = policy_archive::trade::address_parts(address)
                .map(|(c, _)| c)
                .unwrap_or_else(|| address.clone());
            addrs.entry(cred).or_default().insert(address.as_str());
        }
    }
    if !unclaimed.is_empty() {
        println!("\nunclaimed script outputs, by PAYMENT credential");
        let mut rows: Vec<_> = unclaimed.iter().collect();
        rows.sort_by_key(|(_, (n, _, _))| std::cmp::Reverse(*n));
        for (cred, (n, _, largest)) in rows.into_iter().take(8) {
            let distinct = addrs.get(cred).map(|s| s.len()).unwrap_or(0);
            // ⚠️ MANY ADDRESSES, ONE CREDENTIAL is the signature of a contract
            // that derives its stake part per user — an order contract. One
            // address is a singleton: a treasury, a vesting lock, a farm.
            let shape = match distinct > 1 {
                true => "per-user stake ⇒ likely an ORDER contract",
                false => "one address ⇒ a lock, treasury or farm",
            };
            println!(
                "  {cred}  {n:>5} outputs  {distinct:>5} addresses  max {largest:>16}  {shape}"
            );
            // ⚠️ A SAMPLE ADDRESS, because a CREDENTIAL cannot be looked up
            // and an address can. This is the handle you paste into an
            // explorer to find a transaction that spends from the contract,
            // which is the only way to learn what it is — and identifying one
            // is what turns a few hundred "unclaimed" outputs into order legs.
            if let Some(a) = addrs.get(cred).and_then(|s| s.iter().min()) {
                println!("      e.g. {a}");
            }
        }
    }

    // ── fills, by venue — these must JOIN the pools above ────────────────
    let mut fills: BTreeMap<&str, (usize, usize)> = BTreeMap::new();
    for e in events {
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

    // ── THE CLIENT FOLD ──────────────────────────────────────────────────
    //
    // 🔑 This is what wire v4 was for. Nothing here reads the archive: it
    // folds the STREAM with the same `PolicyView` the box folds observations
    // with, so the figure below is what a browser can compute for itself with
    // no round trip. Compare it to `token-ledger archive --policy <p>`.
    //
    // ⚠️ A WINDOW IS NOT THE ARCHIVE. These agree only when the window
    // contains a sighting of every pool — so a short window reads LOW, and
    // that is a fact about coverage rather than a disagreement. `pools folded`
    // against the archive's pool count is what says which you are looking at.
    let view = policy_archive::view::PolicyView::from_story(&story);
    let at = view.ceiling().unwrap_or(0);
    let spot = view.spot(at, &policy_archive::view::Projection::default());
    println!("\nclient fold  (PolicyView over THIS STREAM — no archive access)");
    println!(
        "  pools folded {}   candidates {}   unresolved refs {}",
        view.pool_count(),
        view.candidates(),
        view.unresolved_pool_refs(),
    );
    match spot.ada.as_ref() {
        Some(d) => println!(
            "  price        {:.8} ADA/unit at slot {at}  ({} ADA pool(s), Σbase {}, Σquote {})",
            d.rate().unwrap_or(0.0) / 1_000_000.0,
            d.pools,
            d.base,
            d.quote,
        ),
        None => println!("  price        UNDEFINED — no ADA-paired pool in this window"),
    }
    for (label, set) in [
        ("unresolved", &spot.unresolved),
        ("unpriceable", &spot.unpriceable),
        ("stale", &spot.stale),
    ] {
        for u in set {
            println!(
                "  {label:<12} Σquote {} across {} pool(s)",
                u.quote, u.pools
            );
        }
    }
}

/// One pool, as a table key: its venue and enough of its identity to tell it
/// from its siblings.
///
/// ⚠️ Reads the pool TABLE rather than the sighting. Before v4 a pool state
/// carried only its instance key, so every pool on a venue that mints no pool
/// NFT keyed as `—` and they all collapsed into one row — which is exactly the
/// merge this diagnostic exists to expose.
fn pool_label(story: &policy_archive::story::Story, ix: u32) -> (String, String) {
    match story.pool(ix) {
        Some(p) => {
            let key = match p.key_name.is_empty() {
                // No pool NFT: the address IS the identity, so show that
                // rather than a dash that reads as "unknown".
                true => format!("@{}", &p.address[..p.address.len().min(18)]),
                false => hex(&p.key_name),
            };
            (p.venue.clone(), key)
        }
        // A row pointing outside the table is a bug in the producer, and
        // naming it beats rendering it as a pool that merely has no venue.
        None => ("?".to_string(), format!("UNRESOLVED #{ix}")),
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
