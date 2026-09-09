//! Sender resolution — pass C.
//!
//! A receipt's funding inputs name source txs by HASH, and a tx's hash is
//! computed, never stored in chain bytes — so there is nothing to memmem
//! for. Before the tx index existed this pass decoded and hashed every tx in
//! 32-chunk bands, newest first, until the wanted set emptied: ~150 s per
//! wallet on cardano-infra even with early exit, and a windowed job had to
//! leave older senders unnamed to keep that bounded.
//!
//! Now each wanted outref is one point lookup in `tx-index` (prefix
//! directory → body offset → pread → verify → decode), so age costs nothing
//! and every foreign input gets named. The sweep survives for exactly one
//! case: chunks newer than the index's last rebuild — a Mithril refresh
//! landed and `tx-index-refresh` has not finished — and it runs only over
//! those chunks, only for the outrefs the index did not know. No index at
//! all (a laptop) falls back to the full sweep with a warning.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use mitos_chain_walk::mithril::CHUNK_SLOTS;
use mitos_chain_walk::open_blocks;
use pallas_traverse::MultiEraBlock;
use tx_index::Index;

use crate::progress::{Prog, Progress};

/// (source tx hash, output index) → (address display, lovelace).
pub type Sources = HashMap<([u8; 32], u32), (String, u64)>;
/// source tx hash → the output indices needed.
pub type Wanted = HashMap<[u8; 32], Vec<u32>>;

pub struct Resolved {
    pub sources: Sources,
    pub wall_secs: f64,
    /// Outrefs named by the tx index.
    pub via_index: usize,
    /// Outrefs named by the decode+hash sweep (index gap or no index).
    pub via_sweep: usize,
    /// Wanted outrefs still unnamed: volatile-tip sources, or chunks
    /// outside the sweep window when no index was available.
    pub unresolved: usize,
}

/// Name the senders behind `wanted`. `chunks` bounds the SWEEP only — the
/// index has no window — and is further cut to what the index has not
/// covered yet.
pub fn senders(
    immutable: &Path,
    index_dir: Option<&Path>,
    chunks: &[u64],
    wanted: &Wanted,
    threads: usize,
    on: Prog<'_>,
) -> Result<Resolved> {
    let started = Instant::now();
    let mut sources = Sources::new();
    let mut remaining: Wanted = wanted.clone();
    let mut sweep_chunks: Vec<u64> = chunks.to_vec();
    let mut via_index = 0;

    if let Some((idx, newest)) = index_dir.and_then(|dir| open_index(dir, immutable)) {
        let (found, missed) = lookup(&idx, &remaining, threads, on)?;
        via_index = found.len();
        sources.extend(found);
        remaining = missed;
        sweep_chunks.retain(|c| *c > newest);
        tracing::info!(
            via_index,
            missed_hashes = remaining.len(),
            sweep_chunks = sweep_chunks.len(),
            "tx index resolved"
        );
    }

    let mut via_sweep = 0;
    if !remaining.is_empty() && !sweep_chunks.is_empty() {
        let swept = sweep(immutable, &sweep_chunks, &remaining, threads, on)?;
        via_sweep = swept.len();
        sources.extend(swept);
    }

    let unresolved = wanted
        .iter()
        .map(|(h, idxs)| {
            idxs.iter()
                .filter(|i| !sources.contains_key(&(*h, **i)))
                .count()
        })
        .sum();

    Ok(Resolved {
        sources,
        wall_secs: started.elapsed().as_secs_f64(),
        via_index,
        via_sweep,
        unresolved,
    })
}

/// The index and the newest chunk it covers, or `None` (with a warning)
/// when there is nothing usable at `dir`.
fn open_index(dir: &Path, immutable: &Path) -> Option<(Index, u64)> {
    match Index::open(dir, immutable) {
        Ok(idx) => match idx.coverage().newest_chunk {
            Some(n) => Some((idx, u64::from(n))),
            None => {
                tracing::warn!(
                    "tx index at {} has no segments; falling back to the decode+hash sweep",
                    dir.display()
                );
                None
            }
        },
        Err(e) => {
            tracing::warn!(
                "tx index at {} unavailable ({e:#}); falling back to the decode+hash sweep",
                dir.display()
            );
            None
        }
    }
}

/// One point lookup per wanted hash, spread over `threads`. Returns what
/// was named and the hashes the index does not hold.
fn lookup(idx: &Index, wanted: &Wanted, threads: usize, on: Prog<'_>) -> Result<(Sources, Wanted)> {
    let items: Vec<(&[u8; 32], &Vec<u32>)> = wanted.iter().collect();
    let total = items.len();
    let done = AtomicUsize::new(0);
    let per = items.len().div_ceil(threads.max(1)).max(1);

    let parts: Vec<(Sources, Wanted)> = std::thread::scope(|s| {
        let handles: Vec<_> = items
            .chunks(per)
            .map(|slice| {
                let done = &done;
                s.spawn(move || -> Result<(Sources, Wanted)> {
                    let mut found = Sources::new();
                    let mut missed = Wanted::new();
                    for (h, idxs) in slice {
                        match idx
                            .tx(h)
                            .with_context(|| format!("looking up {}", hex::encode(h)))?
                        {
                            Some(body) => {
                                let outs = body.outputs()?;
                                for i in idxs.iter() {
                                    // An index past the tx's outputs is a
                                    // malformed wanted entry; it stays
                                    // unnamed rather than mis-named.
                                    if let Some(o) = outs.get(*i as usize) {
                                        found.insert((**h, *i), (o.address.clone(), o.lovelace));
                                    }
                                }
                            }
                            None => {
                                missed.insert(**h, (*idxs).clone());
                            }
                        }
                        let d = done.fetch_add(1, Ordering::Relaxed) + 1;
                        if d.is_multiple_of(1_000) || d == total {
                            on(Progress::Lookup { done: d, total });
                        }
                    }
                    Ok((found, missed))
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("lookup worker panicked"))
            .collect::<Result<Vec<_>>>()
    })?;

    let mut found = Sources::new();
    let mut missed = Wanted::new();
    for (f, m) in parts {
        found.extend(f);
        missed.extend(m);
    }
    Ok((found, missed))
}

/// The pre-index pass: decode+hash every tx in `chunks` (newest first)
/// until every wanted source is found.
fn sweep(
    immutable: &Path,
    chunks: &[u64],
    wanted: &Wanted,
    threads: usize,
    on: Prog<'_>,
) -> Result<Sources> {
    // Bands of contiguous chunks, popped NEWEST first.
    const BAND: usize = 32;
    let mut bands: Vec<(u64, u64)> = chunks
        .chunks(BAND)
        .map(|c| (c[0], c[c.len() - 1]))
        .collect();
    let queue: Mutex<Vec<(u64, u64)>> = Mutex::new({
        bands.sort_unstable();
        bands
    });

    let remaining = AtomicUsize::new(wanted.len());
    let total_bands = queue.lock().expect("queue").len();
    let done_bands = AtomicUsize::new(0);

    let worker = || -> Result<Sources> {
        let mut local = Sources::new();
        loop {
            if remaining.load(Ordering::Relaxed) == 0 {
                break;
            }
            let band = { queue.lock().expect("queue").pop() };
            let Some((first, last)) = band else { break };
            let start = first * CHUNK_SLOTS;
            let end = (last + 1) * CHUNK_SLOTS;
            let blocks = open_blocks(immutable, Some((start, Vec::new())))
                .with_context(|| format!("seeking band {first}..={last}"))?;
            for raw in blocks {
                let raw = raw.map_err(|e| anyhow::anyhow!("reading block: {e:?}"))?;
                let block = MultiEraBlock::decode(&raw)
                    .map_err(|e| anyhow::anyhow!("decoding block: {e:?}"))?;
                if block.slot() >= end {
                    break;
                }
                for tx in block.txs() {
                    let mut h = [0u8; 32];
                    h.copy_from_slice(tx.hash().as_ref());
                    let Some(indices) = wanted.get(&h) else {
                        continue;
                    };
                    let outputs = tx.outputs();
                    for idx in indices {
                        let Some(o) = outputs.get(*idx as usize) else {
                            continue;
                        };
                        let addr = o
                            .address()
                            .map(|a| a.to_string())
                            .unwrap_or_else(|_| "<unparsable>".into());
                        local.insert((h, *idx), (addr, o.value().coin()));
                    }
                    remaining.fetch_sub(1, Ordering::Relaxed);
                }
            }
            let db = done_bands.fetch_add(1, Ordering::Relaxed) + 1;
            if db.is_multiple_of(10) {
                on(Progress::Resolve {
                    done: db,
                    total: total_bands,
                    wanted_left: remaining.load(Ordering::Relaxed),
                });
            }
        }
        Ok(local)
    };

    let locals: Vec<Sources> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads.max(1)).map(|_| s.spawn(worker)).collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("worker panicked"))
            .collect::<Result<Vec<_>>>()
    })?;

    let mut sources = Sources::new();
    for l in locals {
        sources.extend(l);
    }
    Ok(sources)
}
