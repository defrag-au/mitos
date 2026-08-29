//! Orchestration — the pass sequence both faces share.
//!
//! The CLI (`scan`) and the hosted service (`serve`) run the same excavation:
//! pass A over the scan range, classify against a seeded owned set, optional
//! sweep pass, optional sender resolution. The only difference between a cold
//! excavation and an incremental refresh is the seed: an empty owned map and
//! the Shelley floor versus the persisted UTxO set and `cursor + 1`.

use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;
use mitos_chain_walk::mithril::immutable_file_for_slot;

use crate::progress::{Prog, Progress};
use crate::{classify, resolve, scan};

/// Shelley start — Shelley credentials cannot appear in earlier chunks.
pub const SHELLEY_START_SLOT: u64 = 4_492_800;

pub struct Params<'a> {
    pub immutable: &'a Path,
    pub creds: Vec<[u8; 28]>,
    /// First chunk pass A scans (Shelley chunk for cold, cursor+1 for
    /// incremental).
    pub scan_from_chunk: u64,
    /// UTxO set carried in from a prior run (empty for cold).
    pub seed_owned: crate::classify::OwnedSet,
    pub threads: usize,
    pub sweeps: bool,
    pub resolve: bool,
}

pub struct Outcome {
    pub timeline: classify::Timeline,
    /// (source tx hash, output index) → (address, lovelace) for foreign inputs.
    pub sources: HashMap<([u8; 32], u32), (String, u64)>,
    pub pass_a: scan::ScanStats,
    pub pass_b: Option<scan::ScanStats>,
    pub resolve_secs: Option<f64>,
}

/// One wallet in a shared sweep.
pub struct BatchTarget {
    pub creds: Vec<[u8; 28]>,
    /// Slot-granular cursor: txs strictly below this are already processed
    /// (a prior run's chunk OR tail coverage) and must not re-classify.
    pub scan_from_slot: u64,
    pub seed_owned: crate::classify::OwnedSet,
}

/// A slice of chain to sweep. `to_slot` is exclusive; `None` means "to the
/// tip", which is also the only case that scans the tail spool.
#[derive(Clone, Copy, Debug)]
pub struct ScanRange {
    pub from_slot: u64,
    pub to_slot: Option<u64>,
}

/// MANY wallets, ONE sweep of `range` — returns the RAW hits per target plus
/// the newest slot covered. Scan cost is chain-size-bound, so every queued
/// wallet rides the same pass; hits are attributed per target by the scanner,
/// and each target keeps only txs at/after its own cursor, so cold and
/// incremental wallets mix in one batch without double-counting.
///
/// Raw rather than classified because progressive excavation classifies ONCE
/// over two sweeps: a wallet's direction depends on the UTxOs it held BEFORE
/// the window, so a recent-only pass cannot be trusted on its own and the
/// deep pass must re-classify the combined find. Resolution is likewise the
/// caller's job — it wants to merge across the whole batch.
pub fn scan_batch(
    immutable: &Path,
    tail_db: Option<&Path>,
    batch: &[BatchTarget],
    range: ScanRange,
    threads: usize,
    on: Prog<'_>,
) -> Result<(Vec<Vec<scan::FoundTx>>, u64)> {
    let shelley_chunk = immutable_file_for_slot(SHELLEY_START_SLOT);
    let all_chunks = scan::list_chunks(immutable, shelley_chunk)?;
    let newest = all_chunks.last().copied().unwrap_or(shelley_chunk);
    let floor_chunk = immutable_file_for_slot(range.from_slot);
    let ceil_chunk = range.to_slot.map(immutable_file_for_slot);
    let a_chunks: Vec<u64> = all_chunks
        .iter()
        .copied()
        .filter(|c| *c >= floor_chunk && ceil_chunk.is_none_or(|top| *c <= top))
        .collect();

    let keep = |t: &BatchTarget, slot: u64| {
        slot >= range.from_slot
            && slot >= t.scan_from_slot
            && range.to_slot.is_none_or(|top| slot < top)
    };

    let mut per_target: Vec<Vec<scan::FoundTx>> = batch.iter().map(|_| Vec::new()).collect();
    if !a_chunks.is_empty() {
        let targets: Vec<Vec<[u8; 28]>> = batch.iter().map(|t| t.creds.clone()).collect();
        let (found, _stats) = scan::cred_scan(immutable, &a_chunks, &targets, threads, on)?;
        for f in found {
            if keep(&batch[f.target_idx], f.slot) {
                per_target[f.target_idx].push(f);
            }
        }
    }

    // The spool only matters when the range runs to the tip.
    let chunk_end_slot = (newest + 1) * mitos_chain_walk::mithril::CHUNK_SLOTS - 1;
    let mut scanned_to_slot = range
        .to_slot
        .map(|t| t.saturating_sub(1))
        .unwrap_or(chunk_end_slot);
    if range.to_slot.is_none()
        && let Some(tail_path) = tail_db
        && let Ok(conn) = crate::tail::open_ro(tail_path)
    {
        let (blocks, high) = crate::tail::scannable_blocks(&conn, chunk_end_slot)?;
        if !blocks.is_empty() {
            on(Progress::Phase {
                label: "tail",
                detail: "sieving spool blocks",
            });
            let targets: Vec<Vec<[u8; 28]>> = batch.iter().map(|t| t.creds.clone()).collect();
            let mut found = Vec::new();
            for (_, cbor) in &blocks {
                let Ok(block) = pallas_traverse::MultiEraBlock::decode(cbor) else {
                    continue;
                };
                scan::extract_cred_hits(&block, &targets, &mut found);
            }
            for f in found {
                if keep(&batch[f.target_idx], f.slot) {
                    per_target[f.target_idx].push(f);
                }
            }
        }
        if let Some(high) = high {
            scanned_to_slot = scanned_to_slot.max(high);
        }
    }
    Ok((per_target, scanned_to_slot))
}

pub fn run(p: Params<'_>, on: Prog<'_>) -> Result<Outcome> {
    // The full universe (resolve must reach below the scan floor) and the
    // pass-A range above it.
    let shelley_chunk = immutable_file_for_slot(SHELLEY_START_SLOT);
    let all_chunks = scan::list_chunks(p.immutable, shelley_chunk)?;
    let a_chunks: Vec<u64> = all_chunks
        .iter()
        .copied()
        .filter(|c| *c >= p.scan_from_chunk)
        .collect();

    let mut found = Vec::new();
    let mut pass_a = scan::ScanStats::default();
    if !a_chunks.is_empty() {
        on(Progress::Phase {
            label: "scan",
            detail: "sieving credentials",
        });
        (found, pass_a) = scan::cred_scan(
            p.immutable,
            &a_chunks,
            std::slice::from_ref(&p.creds),
            p.threads,
            on,
        )?;
    }
    let mut timeline = classify::build_with(p.seed_owned.clone(), &found);

    let mut pass_b = None;
    if p.sweeps && !timeline.own_hashes.is_empty() && timeline.first_slot != u64::MAX {
        on(Progress::Phase {
            label: "sweeps",
            detail: "sieving own tx hashes",
        });
        let first_chunk = immutable_file_for_slot(timeline.first_slot);
        let b_chunks: Vec<u64> = a_chunks
            .iter()
            .copied()
            .filter(|c| *c >= first_chunk)
            .collect();
        let (swept, b_stats) = scan::sweep_scan(
            p.immutable,
            &b_chunks,
            &timeline.own_hashes,
            &timeline.owned,
            p.threads,
            on,
        )?;
        pass_b = Some(b_stats);
        if !swept.is_empty() {
            found.extend(swept);
            timeline = classify::build_with(p.seed_owned.clone(), &found);
        }
    }

    let mut sources = HashMap::new();
    let mut resolve_secs = None;
    if p.resolve && !timeline.txs.is_empty() {
        let mut wanted: HashMap<[u8; 32], Vec<u32>> = HashMap::new();
        for tx in &timeline.txs {
            for (h, idx) in &tx.foreign_inputs {
                wanted.entry(*h).or_default().push(*idx);
            }
        }
        if !wanted.is_empty() {
            on(Progress::Phase {
                label: "resolve",
                detail: "naming senders",
            });
            let last_chunk = immutable_file_for_slot(timeline.last_slot);
            let c_chunks: Vec<u64> = all_chunks
                .iter()
                .copied()
                .filter(|c| *c <= last_chunk)
                .collect();
            let r = resolve::senders(p.immutable, &c_chunks, &wanted, p.threads, on)?;
            sources = r.sources;
            resolve_secs = Some(r.wall_secs);
        }
    }

    Ok(Outcome {
        timeline,
        sources,
        pass_a,
        pass_b,
        resolve_secs,
    })
}
