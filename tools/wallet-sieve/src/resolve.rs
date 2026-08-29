//! Sender resolution — the one pass that cannot sieve.
//!
//! A receipt's funding inputs name source txs by HASH, and a tx's hash is
//! computed, never stored in chain bytes — so there is nothing to memmem for.
//! This pass decodes every block in range and hashes every tx against the
//! wanted set. It is the expensive worst case the spike exists to measure;
//! the mitigations are (a) newest-first band order + early exit once every
//! wanted hash is found, because UTxOs skew young, and (b) the wanted set is
//! only the FOREIGN inputs — the wallet's own money never needs it.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use mitos_chain_walk::mithril::CHUNK_SLOTS;
use mitos_chain_walk::open_blocks;
use pallas_traverse::MultiEraBlock;

use crate::progress::{Prog, Progress};

pub struct Resolved {
    /// (source tx hash, output index) → (address display, lovelace).
    pub sources: HashMap<([u8; 32], u32), (String, u64)>,
    pub wall_secs: f64,
}

/// Decode+hash every tx in `chunks` (newest first) until every wanted source
/// is found. `wanted` maps source tx hash → the output indices needed.
pub fn senders(
    immutable: &Path,
    chunks: &[u64],
    wanted: &HashMap<[u8; 32], Vec<u32>>,
    threads: usize,
    on: Prog<'_>,
) -> Result<Resolved> {
    let started = Instant::now();
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

    struct Local {
        sources: HashMap<([u8; 32], u32), (String, u64)>,
        blocks: u64,
        txs: u64,
    }

    let worker = || -> Result<Local> {
        let mut local = Local {
            sources: HashMap::new(),
            blocks: 0,
            txs: 0,
        };
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
                local.blocks += 1;
                for tx in block.txs() {
                    local.txs += 1;
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
                        local.sources.insert((h, *idx), (addr, o.value().coin()));
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

    let locals: Vec<Local> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads.max(1)).map(|_| s.spawn(worker)).collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("worker panicked"))
            .collect::<Result<Vec<_>>>()
    })?;

    let mut sources = HashMap::new();
    for l in locals {
        sources.extend(l.sources);
    }
    Ok(Resolved {
        sources,
        wall_secs: started.elapsed().as_secs_f64(),
    })
}
