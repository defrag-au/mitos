//! The whole pipeline on real chunks: `tx-index`'s extraction pass, mints
//! harvested from the SAME decode, segments, compaction, structural
//! verification, and a lookup.
//!
//! This is the wiring the CLI will do, and the proof that the shared-pass
//! claim is real rather than documented — the observer callback is the only
//! seam between the two crates.
//!
//! ```text
//! harvest <immutable> <out-dir> <from> <count> [policy-hex] [asset-hex]
//! ```

use std::collections::HashSet;
use std::path::Path;
use std::time::Instant;

use anyhow::{Result, bail};
use policy_index::{Base, Mints, TxSpans, base_path, compact, segment};

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 5 {
        bail!("usage: harvest <immutable> <out-dir> <from-chunk> <count> [policy-hex] [asset-hex]");
    }
    let immutable = Path::new(&args[1]);
    let out = Path::new(&args[2]);
    let from: u16 = args[3].parse()?;
    let count: u16 = args[4].parse()?;

    // ── extract ───────────────────────────────────────────────────────────
    let started = Instant::now();
    let (mut records, mut invalid, mut oversize, mut txs, mut bytes) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    let mut done = 0u32;
    for chunk in from..from.saturating_add(count) {
        if !immutable.join(format!("{chunk:05}.chunk")).exists() {
            continue;
        }
        // Segments are immutable once written, so an interrupted build
        // resumes rather than restarting.
        if segment::segment_path(out, chunk).exists() {
            done += 1;
            continue;
        }
        let mut mints = Mints::default();
        // ONE decode, TWO indexes: tx entries return, mints arrive by callback.
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
        txs += extracted.entries.len() as u64;
        bytes += extracted.bytes;
        mints.sort();
        records += mints.records.len() as u64;
        invalid += mints.invalid_skipped;
        oversize += mints.oversize_skipped;
        segment::write_segment(out, chunk, &mints.records)?;
        done += 1;
        if done % 1000 == 0 {
            let mb = bytes as f64 / 1_048_576.0;
            println!(
                "  {done} chunks, {records} records, {:.0} MB/s",
                mb / started.elapsed().as_secs_f64().max(0.001)
            );
        }
    }
    println!(
        "extract   {done} chunks, {txs} txs, {records} mint records, \
         {invalid} invalid skipped, {oversize} oversize skipped, {:.1}s",
        started.elapsed().as_secs_f64()
    );

    // ── compact ───────────────────────────────────────────────────────────
    let stats = compact(out)?;
    println!(
        "compact   {} records, {} policies, {:.1} MiB, {:.1}s",
        stats.records,
        stats.policies,
        stats.file_len as f64 / 1_048_576.0,
        stats.wall_secs
    );

    // ── verify ────────────────────────────────────────────────────────────
    let base = Base::open(&base_path(out))?;
    let t = Instant::now();
    base.verify_structure()?;
    println!(
        "verify    ordering, fences and permutation agree ({:.1}s)",
        t.elapsed().as_secs_f64()
    );

    // ── look one up ───────────────────────────────────────────────────────
    if let Some(hex_policy) = args.get(5) {
        let policy = hex::decode(hex_policy)?;
        let prefix = policy_index::policy_prefix(&policy);
        let t = Instant::now();
        let first = base.first_chunk_of(prefix);
        let probe_us = t.elapsed().as_secs_f64() * 1e6;
        let all = base.records_of(prefix);
        let assets: HashSet<u64> = all.iter().map(|r| r.name_prefix).collect();
        println!("--- policy {hex_policy} ---");
        println!("first chunk      {first:?}   (floor probe in {probe_us:.0} µs)");
        println!("mint events      {}", all.len());
        println!("distinct assets  {}", assets.len());
        println!("burns            {}", all.iter().filter(|r| r.burned).count());
        println!(
            "with metadata    {}",
            all.iter().filter(|r| r.aux_span().is_some()).count()
        );
        let timeline = base.records_by_time(prefix);
        if let (Some(a), Some(b)) = (timeline.first(), timeline.last()) {
            println!("minted over      chunk {} .. {}", a.chunk, b.chunk);
        }
        if let Some(hex_asset) = args.get(6) {
            let name = hex::decode(hex_asset)?;
            let np = policy_index::name_prefix(&name);
            match base.origin(prefix, np) {
                Some(r) => println!(
                    "asset {hex_asset}: chunk {}, body {}+{}, metadata {:?}",
                    r.chunk,
                    r.offset,
                    r.len,
                    r.aux_span()
                ),
                None => println!("asset {hex_asset}: no origin found"),
            }
        }
    }
    Ok(())
}
