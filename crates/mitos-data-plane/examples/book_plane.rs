//! Drive the module path through `BookDataPlane`, at whatever scale the artifact is.
//!
//! ```sh
//! cargo run -p mitos-data-plane --example book_plane -- \
//!     --dir /srv/data/fold-book/full3 --policies 200 --limit 2000
//! ```
//!
//! Why this exists rather than a test: the unit tests check the adapter against a
//! two-row artifact, and what needs checking at scale is the *cost* — of the
//! address encoding, the asset hexing and the datum projection, none of which are
//! in the book's own reader and all of which are on the path a module takes. It
//! also prints what the trait refuses, because a refusal is part of the contract
//! and should be visible rather than discovered.
//!
//! It samples from `policy.tbl` for the same reason `fold-probe lookup
//! --cross-check` does: it is a key the artifact hands out, it needs no new API to
//! enumerate, and it spreads the sample across the whole live set rather than
//! across the newest chunk.

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;

use cardano_assets::PolicyId;
use mitos_data_plane::{BookDataPlane, ChainDataPlane, DatumKind, DecodeLevel};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut dir = PathBuf::new();
    let mut policies = 100usize;
    let mut limit = 1_000usize;
    let mut with_datum = true;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--dir" => dir = PathBuf::from(args.next().expect("--dir needs a path")),
            "--policies" => policies = args.next().expect("value").parse()?,
            "--limit" => limit = args.next().expect("value").parse()?,
            // The decode level is the one axis a caller controls, and it is also
            // how the projection's cost is attributed: the difference between the
            // two runs *is* datum resolution.
            "--lean" => with_datum = false,
            other => return Err(format!("unknown argument `{other}`").into()),
        }
    }
    let decode = if with_datum {
        DecodeLevel::WithDatum
    } else {
        DecodeLevel::Lean
    };
    if dir.as_os_str().is_empty() {
        return Err("--dir is required".into());
    }

    let t = Instant::now();
    let plane = BookDataPlane::open(&dir)?;
    println!("book_plane — {}", dir.display());
    println!(
        "  open       {} rows in {:.3} s",
        plane.rows(),
        t.elapsed().as_secs_f64()
    );

    // What the plane can say about where it is. It cannot, and the refusal is the
    // point: a module that needs a chain position needs `LIVE_BOOK.md` phase 4.
    match plane.tip().await {
        Ok(tip) => println!(
            "  tip        slot {} (the book records no position)",
            tip.slot()
        ),
        Err(e) => println!("  tip        refused: {e}"),
    }

    // Sample policies, and through each one the whole module path: enumerate refs
    // with the trait, then read every ref back with the trait.
    let base = plane.base();
    let n_policies = base.policies().len();
    if n_policies == 0 {
        return Err("the artifact holds no policies".into());
    }
    let stride = (n_policies / policies.max(1)).max(1);

    let (mut outputs, mut assets, mut datums, mut inline_datums) = (0u64, 0u64, 0u64, 0u64);
    let (mut byref_nanos, mut read_nanos) = (0u128, 0u128);
    let (mut policy_refs, mut largest) = (0u64, (0u64, String::new()));
    // A row is reachable from **every** policy it holds, so sampling policy by
    // policy reads heavy rows once per policy they carry. The shape totals are
    // therefore counted over unique refs — without this, "assets per output"
    // measures how many policies a row holds rather than how many assets it has.
    // (Measured the hard way: the biased figure was 53 assets per output against a
    // whole-store average of 1.7.)
    let mut seen: HashSet<[u8; 36]> = HashSet::new();

    let t = Instant::now();
    let mut id = 0usize;
    while id < n_policies {
        let policy_bytes = *base.policies().get(id as u32).expect("in range");
        let t = Instant::now();
        let refs = plane.utxos_by_policy(&policy_bytes).await?;
        byref_nanos += t.elapsed().as_nanos();
        policy_refs += refs.len() as u64;
        if refs.len() as u64 > largest.0 {
            largest = (refs.len() as u64, hex::encode(policy_bytes));
        }

        // Read every ref back, capped per policy so a single 20,000-holder
        // collection cannot dominate the sample.
        for oref in refs.iter().take(limit) {
            let t = Instant::now();
            let Some(out) = plane.read_utxo(oref, decode).await? else {
                continue;
            };
            read_nanos += t.elapsed().as_nanos();
            let mut key = [0u8; 36];
            key[..32].copy_from_slice(oref.tx_hash.as_ref());
            key[32..].copy_from_slice(&oref.index.to_le_bytes());
            if !seen.insert(key) {
                continue;
            }
            outputs += 1;
            assets += out.assets.len() as u64;
            if let Some(d) = &out.datum {
                datums += 1;
                if d.kind == DatumKind::Inline {
                    inline_datums += 1;
                }
            }
        }
        id += stride;
    }
    let secs = t.elapsed().as_secs_f64();

    println!(
        "  sampled    {} of {n_policies} policies · {policy_refs} refs enumerated · \
         {} unique outputs read in {secs:.1} s",
        policies.min(n_policies),
        seen.len(),
    );
    println!(
        "  cost       {} ns per `utxos_by_policy` ref · {} ns per `read_utxo` ({decode:?})",
        byref_nanos / policy_refs.max(1) as u128,
        read_nanos / (seen.len() as u128).max(1),
    );
    println!(
        "  shape      {:.1} asset entries per output read · {datums} datums ({inline_datums} inline)",
        assets as f64 / outputs.max(1) as f64,
    );
    // The shape figure above is **size-biased and that is the point**: a row holding
    // N assets is N times likelier to be reached from a uniformly-sampled policy,
    // so a policy scan returns the heavy outputs a module actually has to price.
    // Printed beside the row-average so the difference is visible rather than
    // mistaken for a bug.
    let meta = base.meta();
    println!(
        "  row avg    {:.2} asset entries per row over the whole artifact ({} / {}) — the \
         gap is the bias, not an error",
        meta.live_assets as f64 / (meta.rows.max(1)) as f64,
        meta.live_assets,
        meta.rows,
    );
    println!("  largest    {} holds {} refs", largest.1, largest.0);

    // The most expensive method here, on the largest policy found: its definition
    // is distinct *addresses*, so it reads every holder row.
    if largest.0 > 0 {
        let t = Instant::now();
        let holders = plane
            .holder_count(&PolicyId::new(largest.1.clone())?)
            .await?;
        println!(
            "  holders    {} distinct addresses behind {} refs in {:.2} s",
            holders,
            largest.0,
            t.elapsed().as_secs_f64()
        );
    }

    // The surfaces a book-backed plane does not have, printed rather than assumed:
    // each is a named gap, not a silent `None`.
    for (what, err) in [
        (
            "read_datum",
            plane.read_datum(&[0u8; 32].into()).await.err(),
        ),
        (
            "search_utxos",
            plane
                .search_utxos(
                    &mitos_data_plane::UtxoPredicate::any(),
                    DecodeLevel::Lean,
                    mitos_data_plane::PageRequest::first(1),
                )
                .await
                .err(),
        ),
    ] {
        if let Some(e) = err {
            println!("  refused    {what}: {e}");
        }
    }
    Ok(())
}
