//! The byte-sieve core — SIMD substring search over raw Mithril chunk bytes,
//! CBOR decode only on hit. Extracted from wallet-sieve so every "find my
//! needle in 225 GB" tool shares one scanning machine.
//!
//! The insight both consumers rest on: the thing being searched for appears
//! as RAW BYTES inside any transaction that touches it — a wallet's 28-byte
//! credential inside every output address that pays it; a token's 28-byte
//! policy id inside every output value that carries it (and the ledger's
//! balance rule means a token cannot move through a tx without appearing in
//! an output or the mint field, so the byte filter is COMPLETE, not a
//! heuristic). Scan cost is chain-size-bound, not target-size-bound.
//!
//! Two primitives, for the two consumer shapes:
//!
//! - [`scan_extract`] — wallet-sieve's harness, generic over the hit type:
//!   parallel workers pull chunk numbers off a queue, memmem the raw file,
//!   and only a HIT chunk gets block treatment (fuzzy slot-seek + per-block
//!   memmem + decode + extract). Hits come back UNORDERED — fine for
//!   stateless extraction, attributed per target.
//!
//! - [`Needles::hit`] used directly as a per-block gate inside an existing
//!   ORDERED pass — for stateful consumers like token-ledger's walk, whose
//!   UTxO buffer and conservation invariant need blocks in slot order. The
//!   sequential reader still yields every raw block; the gate skips the
//!   DECODE, which is where the walk's CPU actually goes.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use aho_corasick::AhoCorasick;
use anyhow::{bail, Context, Result};
use memchr::memmem::Finder;
use mitos_chain_walk::mithril::CHUNK_SLOTS;
use mitos_chain_walk::open_blocks;
use pallas_traverse::MultiEraBlock;

/// Scan progress, reported every ~100 chunks.
#[derive(Clone, Copy, Debug)]
pub struct ScanProgress<'a> {
    pub pass: &'a str,
    pub done: u64,
    pub total: u64,
    pub gb_per_s: f64,
}

/// The callback shape every pass takes.
pub type OnProgress<'x> = &'x (dyn Fn(ScanProgress<'_>) + Sync);

#[derive(Default)]
pub struct ScanStats {
    pub chunks: u64,
    pub bytes: u64,
    pub hit_chunks: u64,
    pub hit_blocks: u64,
    /// Blocks whose bytes matched but the extractor produced nothing —
    /// datum/metadata mentions or byte coincidences. A false-positive gauge.
    pub unmatched_hit_blocks: u64,
    pub wall_secs: f64,
}

/// Sorted chunk numbers on disk within `[floor_chunk, newest)` — the newest
/// file is excluded (still growing under Mithril semantics; the tail belongs
/// to a spool, not the sieve).
pub fn list_chunks(immutable: &Path, floor_chunk: u64) -> Result<Vec<u64>> {
    let mut nums: Vec<u64> = std::fs::read_dir(immutable)
        .with_context(|| format!("reading {}", immutable.display()))?
        .filter_map(|e| {
            let name = e.ok()?.file_name().into_string().ok()?;
            let stem = name.strip_suffix(".chunk")?;
            stem.parse::<u64>().ok()
        })
        .collect();
    nums.sort_unstable();
    if nums.len() < 2 {
        bail!("need at least 2 chunk files (the newest is excluded as still-growing)");
    }
    nums.pop();
    Ok(nums.into_iter().filter(|n| *n >= floor_chunk).collect())
}

/// Any of the target patterns, over any byte slice. Single-pattern uses a
/// SIMD `Finder`; wide pattern sets use Aho-Corasick.
pub enum Needles<'a> {
    Finders(Vec<Finder<'a>>),
    Automaton(AhoCorasick),
}

impl Needles<'_> {
    /// Pick the right machinery for a pattern set: SIMD finders for a few
    /// patterns, an automaton once the set is wide.
    pub fn new(patterns: &[Vec<u8>]) -> Result<Needles<'static>> {
        Ok(if patterns.len() > 3 {
            Needles::Automaton(AhoCorasick::new(patterns).context("building automaton")?)
        } else {
            Needles::Finders(
                patterns
                    .iter()
                    .map(|p| Finder::new(p).into_owned())
                    .collect(),
            )
        })
    }

    pub fn hit(&self, haystack: &[u8]) -> bool {
        match self {
            Needles::Finders(fs) => fs.iter().any(|f| f.find(haystack).is_some()),
            Needles::Automaton(ac) => ac.is_match(haystack),
        }
    }
}

/// Find the EARLIEST chunk containing any needle — floor discovery for a
/// target nobody has registered a floor slot for.
///
/// Parallel workers pull chunks in ascending order and share an
/// earliest-hit cutoff, so chunks past a known hit are skipped without
/// being read. The scan runs at parallel `fs::read` speed (~3 GB/s
/// measured) versus the sequential block reader's ~600 MB/s — which turns
/// a genesis-floor cold start from ~15 minutes of reading into ~1–2
/// minutes of scanning plus a walk over only the target's actual lifetime.
pub fn first_hit_chunk(
    immutable: &Path,
    chunks: &[u64],
    threads: usize,
    patterns: &[Vec<u8>],
    on: OnProgress<'_>,
) -> Result<Option<u64>> {
    let started = Instant::now();
    let queue: Mutex<VecDeque<u64>> = Mutex::new(chunks.iter().copied().collect());
    let earliest = AtomicU64::new(u64::MAX);
    let done_chunks = AtomicU64::new(0);
    let done_bytes = AtomicU64::new(0);
    let total = chunks.len() as u64;

    std::thread::scope(|s| -> Result<()> {
        let handles: Vec<_> = (0..threads.max(1))
            .map(|_| {
                let queue = &queue;
                let earliest = &earliest;
                let done_chunks = &done_chunks;
                let done_bytes = &done_bytes;
                s.spawn(move || -> Result<()> {
                    let needles = Needles::new(patterns)?;
                    loop {
                        let chunk = { queue.lock().expect("queue").pop_front() };
                        let Some(chunk) = chunk else { break };
                        // A hit at or before this chunk already exists —
                        // nothing later can be the FIRST.
                        if chunk >= earliest.load(Ordering::Relaxed) {
                            continue;
                        }
                        let path: PathBuf = immutable.join(format!("{chunk:05}.chunk"));
                        let bytes = std::fs::read(&path)
                            .with_context(|| format!("reading {}", path.display()))?;
                        let dc = done_chunks.fetch_add(1, Ordering::Relaxed) + 1;
                        let db = done_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                        if dc.is_multiple_of(200) {
                            let secs = started.elapsed().as_secs_f64();
                            on(ScanProgress {
                                pass: "floor",
                                done: dc,
                                total,
                                gb_per_s: db as f64 / 1e9 / secs,
                            });
                        }
                        if needles.hit(&bytes) {
                            earliest.fetch_min(chunk, Ordering::Relaxed);
                        }
                    }
                    Ok(())
                })
            })
            .collect();
        for h in handles {
            h.join().expect("worker panicked")?;
        }
        Ok(())
    })?;

    let hit = earliest.load(Ordering::Relaxed);
    Ok((hit != u64::MAX).then_some(hit))
}

/// The parallel scan-and-extract harness: chunk queue → raw memmem → block
/// pass on hit → `extract` into `T`s. Hit order is NOT slot order — workers
/// own whole chunks. Stateful consumers should gate their own ordered pass
/// with [`Needles::hit`] instead.
pub fn scan_extract<'a, T, MkNeedles>(
    immutable: &Path,
    chunks: &[u64],
    threads: usize,
    pass: &str,
    on: OnProgress<'_>,
    mk_needles: MkNeedles,
    extract: &(dyn Fn(&MultiEraBlock<'_>, &Needles<'_>, &mut Vec<T>) + Sync),
) -> Result<(Vec<T>, ScanStats)>
where
    T: Send,
    MkNeedles: Fn() -> Needles<'a> + Sync,
{
    let started = Instant::now();
    let queue: Mutex<VecDeque<u64>> = Mutex::new(chunks.iter().copied().collect());
    let done_chunks = AtomicU64::new(0);
    let done_bytes = AtomicU64::new(0);
    let total = chunks.len() as u64;

    let worker = |_: usize| -> Result<(Vec<T>, ScanStats)> {
        let needles = mk_needles();
        let mut found = Vec::new();
        let mut stats = ScanStats::default();
        loop {
            let chunk = { queue.lock().expect("queue").pop_front() };
            let Some(chunk) = chunk else { break };
            let path: PathBuf = immutable.join(format!("{chunk:05}.chunk"));
            let bytes =
                std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
            stats.chunks += 1;
            stats.bytes += bytes.len() as u64;
            let dc = done_chunks.fetch_add(1, Ordering::Relaxed) + 1;
            let db = done_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
            if dc.is_multiple_of(100) {
                let secs = started.elapsed().as_secs_f64();
                on(ScanProgress {
                    pass,
                    done: dc,
                    total,
                    gb_per_s: db as f64 / 1e9 / secs,
                });
            }
            if !needles.hit(&bytes) {
                continue;
            }
            stats.hit_chunks += 1;
            drop(bytes);

            // Block pass over just this chunk: fuzzy-seek to its first slot,
            // stop at the next chunk's.
            let start = chunk * CHUNK_SLOTS;
            let end = (chunk + 1) * CHUNK_SLOTS;
            let blocks = open_blocks(immutable, Some((start, Vec::new())))
                .with_context(|| format!("seeking chunk {chunk}"))?;
            for raw in blocks {
                let raw = raw.map_err(|e| anyhow::anyhow!("reading block: {e:?}"))?;
                let block = MultiEraBlock::decode(&raw)
                    .map_err(|e| anyhow::anyhow!("decoding block in chunk {chunk}: {e:?}"))?;
                if block.slot() >= end {
                    break;
                }
                if !needles.hit(&raw) {
                    continue;
                }
                stats.hit_blocks += 1;
                let before = found.len();
                extract(&block, &needles, &mut found);
                if found.len() == before {
                    stats.unmatched_hit_blocks += 1;
                }
            }
        }
        Ok((found, stats))
    };

    let per_thread: Vec<(Vec<T>, ScanStats)> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads.max(1))
            .map(|i| s.spawn(move || worker(i)))
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("worker panicked"))
            .collect::<Result<Vec<_>>>()
    })?;

    let mut found = Vec::new();
    let mut stats = ScanStats::default();
    for (f, st) in per_thread {
        found.extend(f);
        stats.chunks += st.chunks;
        stats.bytes += st.bytes;
        stats.hit_chunks += st.hit_chunks;
        stats.hit_blocks += st.hit_blocks;
        stats.unmatched_hit_blocks += st.unmatched_hit_blocks;
    }
    stats.wall_secs = started.elapsed().as_secs_f64();
    Ok((found, stats))
}
