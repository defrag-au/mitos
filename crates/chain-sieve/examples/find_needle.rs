//! `find_needle` — locate chunks containing a target, by hex.
//!
//! The CLI shape of the floor-discovery primitive: [`first_hit_chunk`]
//! parallel-scans raw chunk bytes for a pattern set and returns the earliest
//! chunk containing any of them. A 28-byte identifier (a policy id, a wallet
//! credential) is matched three ways so one invocation covers every encoding it
//! uses on chain — raw bytes, as they appear in output values and addresses;
//! lowercase and uppercase hex text, as they appear as CIP-25 metadata map keys
//! and in project metadata.
//!
//! ```sh
//! # the earliest chunk containing the needle
//! cargo run --release -p chain-sieve --example find_needle -- <immutable> <hex>
//!
//! # every chunk containing it — the life of the target, chunk by chunk
//! cargo run --release -p chain-sieve --example find_needle -- <immutable> <hex> --all
//! ```

use std::collections::{BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use chain_sieve::{first_hit_chunk, list_chunks, Needles, ScanProgress};
use mitos_chain_walk::mithril::CHUNK_SLOTS;

fn main() -> Result<()> {
    let mut all = false;
    let mut positional: Vec<String> = Vec::new();
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--all" => all = true,
            "-h" | "--help" => {
                usage();
                return Ok(());
            }
            other if other.starts_with("--") => bail!("unknown flag {other}"),
            other => positional.push(other.to_string()),
        }
    }
    let dir = PathBuf::from(
        positional
            .first()
            .context("usage: find_needle <immutable dir> <hex> [threads] [--all]")?,
    );
    let needle_hex = positional
        .get(1)
        .context("usage: find_needle <immutable dir> <hex> [threads] [--all]")?;
    let threads = positional
        .get(2)
        .map(|t| t.parse().context("threads"))
        .transpose()?
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        });

    let lower = needle_hex.trim().to_lowercase();
    let raw = hex_decode(&lower)?;
    if raw.is_empty() {
        bail!("empty needle");
    }
    let patterns = vec![
        raw.clone(),
        lower.clone().into_bytes(),
        lower.to_uppercase().into_bytes(),
    ];

    let chunks = list_chunks(&dir, 0)?;
    println!(
        "# find_needle · {} · {} raw bytes · {} chunks · {threads} threads{}",
        dir.display(),
        raw.len(),
        chunks.len(),
        if all { " · all hits" } else { "" },
    );

    if all {
        scan_all(&dir, &chunks, threads, &patterns)?;
    } else {
        let hit = first_hit_chunk(&dir, &chunks, threads, &patterns, &progress)?;
        match hit {
            Some(chunk) => println!(
                "first hit · chunk {chunk:05} · slots {}..{}",
                chunk * CHUNK_SLOTS,
                (chunk + 1) * CHUNK_SLOTS,
            ),
            None => println!("no chunk contains the needle"),
        }
    }
    Ok(())
}

/// Every chunk containing any pattern — no cutoff, sorted.
fn scan_all(dir: &Path, chunks: &[u64], threads: usize, patterns: &[Vec<u8>]) -> Result<()> {
    let queue: Mutex<VecDeque<u64>> = Mutex::new(chunks.iter().copied().collect());
    let hits: Mutex<BTreeSet<u64>> = Mutex::new(BTreeSet::new());
    let done = AtomicU64::new(0);
    let bytes_done = AtomicU64::new(0);
    let total = chunks.len() as u64;
    let started = Instant::now();

    std::thread::scope(|scope| -> Result<()> {
        let handles: Vec<_> = (0..threads.max(1))
            .map(|_| {
                let queue = &queue;
                let hits = &hits;
                let done = &done;
                let bytes_done = &bytes_done;
                scope.spawn(move || -> Result<()> {
                    let needles = Needles::new(patterns)?;
                    loop {
                        let chunk = { queue.lock().expect("queue").pop_front() };
                        let Some(chunk) = chunk else { break };
                        let path = dir.join(format!("{chunk:05}.chunk"));
                        let bytes = std::fs::read(&path)
                            .with_context(|| format!("reading {}", path.display()))?;
                        if needles.hit(&bytes) {
                            hits.lock().expect("hits").insert(chunk);
                        }
                        let d = done.fetch_add(1, Ordering::Relaxed) + 1;
                        let b = bytes_done.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                        if d.is_multiple_of(500) {
                            let secs = started.elapsed().as_secs_f64();
                            eprintln!(
                                "scanned {d} of {total} chunks · {:.2} GB/s",
                                b as f64 / 1e9 / secs,
                            );
                        }
                    }
                    Ok(())
                })
            })
            .collect();
        for handle in handles {
            handle.join().expect("worker panicked")?;
        }
        Ok(())
    })?;

    let hits = hits.into_inner().expect("hits");
    println!("# {} of {} chunks contain the needle", hits.len(), total);
    for &chunk in &hits {
        println!(
            "chunk {chunk:05} · slots {}..{}",
            chunk * CHUNK_SLOTS,
            (chunk + 1) * CHUNK_SLOTS,
        );
    }
    Ok(())
}

fn progress(p: ScanProgress<'_>) {
    eprintln!(
        "scanned {} of {} chunks · {:.2} GB/s",
        p.done, p.total, p.gb_per_s
    );
}

fn usage() {
    eprintln!("usage: find_needle <immutable dir> <hex> [threads] [--all]");
}

fn hex_decode(s: &str) -> Result<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        bail!("hex needle is odd-length");
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks(2) {
        let hi = hex_val(pair[0]).context("hex digit")?;
        let lo = hex_val(pair[1]).context("hex digit")?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_val(c: u8) -> Option<u8> {
    Some(match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => return None,
    })
}
