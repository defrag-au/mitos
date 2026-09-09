//! End-to-end: run `tx-index`'s extraction pass over real chunks, harvest
//! mints from the SAME decode, write policy segments, read them back.
//!
//! This is the wiring the CLI will do, and the proof that the shared-pass
//! claim is real rather than documented — the observer callback is the only
//! seam between the two crates.
//!
//! ```text
//! cargo run --release --example harvest -- <immutable> <from> <count> [policy-hex]
//! ```
//!
//! With a policy id it also answers, from the segments it just wrote, the
//! question the whole crate exists for: what did this policy mint, and when
//! did it FIRST mint.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::time::Instant;

use anyhow::{Result, bail};
use policy_index::{Mints, Record, TxSpans, segment};

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        bail!("usage: harvest <immutable> <from-chunk> <count> [policy-hex]");
    }
    let immutable = Path::new(&args[1]);
    let from: u16 = args[2].parse()?;
    let count: u16 = args[3].parse()?;
    let want = args.get(4).map(hex::decode).transpose()?;

    let out = tempfile::tempdir()?;
    let started = Instant::now();
    let mut total = Mints::default();
    let mut tx_entries = 0u64;

    for chunk in from..from + count {
        if !immutable.join(format!("{chunk:05}.chunk")).exists() {
            continue;
        }
        let mut mints = Mints::default();
        // ONE decode, TWO indexes. The tx entries come back as the return
        // value; the mints arrive through the observer.
        let extracted = tx_index::extract::extract_chunk_observed(
            immutable,
            chunk,
            &mut |block, i, loc, aux| {
                mints.push_tx(
                    block,
                    i,
                    TxSpans {
                        chunk: loc.chunk,
                        body_offset: loc.offset,
                        body_len: loc.len as usize,
                        aux_offset: aux.map(|a| a.offset).unwrap_or(0),
                        aux_len: aux.map(|a| a.len as usize).unwrap_or(0),
                    },
                );
            },
        )?;
        tx_entries += extracted.entries.len() as u64;

        mints.sort();
        segment::write_segment(out.path(), chunk, &mints.records)?;
        total.records.extend(mints.records);
        total.invalid_skipped += mints.invalid_skipped;
        total.oversize_skipped += mints.oversize_skipped;
    }
    let secs = started.elapsed().as_secs_f64();

    // Read the segments BACK — a count held in memory proves nothing about
    // what landed on disk.
    let mut from_disk: Vec<Record> = Vec::new();
    for chunk in segment::list_segments(out.path())? {
        let seg = segment::Segment::open(&segment::segment_path(out.path(), chunk))?;
        from_disk.extend(seg.records());
    }

    let assets: HashSet<(u64, u64)> = from_disk
        .iter()
        .map(|r| (r.policy_prefix, r.name_prefix))
        .collect();
    let policies: HashSet<u64> = from_disk.iter().map(|r| r.policy_prefix).collect();
    let bytes: u64 = segment::list_segments(out.path())?
        .iter()
        .filter_map(|c| std::fs::metadata(segment::segment_path(out.path(), *c)).ok())
        .map(|m| m.len())
        .sum();

    println!("chunks           {from}..{}  in {secs:.2}s", from + count);
    println!("tx entries       {tx_entries}");
    println!(
        "mint records     {} (on disk {})",
        total.records.len(),
        from_disk.len()
    );
    println!("distinct assets  {}", assets.len());
    println!("distinct policies {}", policies.len());
    println!("invalid skipped  {}", total.invalid_skipped);
    println!("oversize skipped {}", total.oversize_skipped);
    println!("segment bytes    {bytes}");
    if total.records.len() != from_disk.len() {
        bail!("what was written and what reads back disagree");
    }

    if let Some(policy) = want {
        let prefix = policy_index::policy_prefix(&policy);
        let mine: Vec<&Record> = from_disk
            .iter()
            .filter(|r| r.policy_prefix == prefix)
            .collect();
        let names: HashSet<u64> = mine.iter().map(|r| r.name_prefix).collect();
        let by_chunk: BTreeMap<u16, usize> = mine.iter().fold(BTreeMap::new(), |mut m, r| {
            *m.entry(r.chunk).or_default() += 1;
            m
        });
        println!("--- policy {} ---", args[4]);
        println!("mint events      {}", mine.len());
        println!("distinct assets  {}", names.len());
        println!(
            "burns            {}",
            mine.iter().filter(|r| r.burned).count()
        );
        println!("first chunk      {:?}", by_chunk.keys().next());
        println!(
            "with metadata    {}",
            mine.iter().filter(|r| r.aux_span().is_some()).count()
        );
    }
    Ok(())
}
