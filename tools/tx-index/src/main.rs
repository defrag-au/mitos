//! tx-index — tx hash → (chunk, body offset) over the Mithril chunk store.
//!
//! - `build` — extract a segment for every completed chunk that lacks one,
//!   then compact into `base.idx` (the refresh cron's step).
//! - `compact` — rebuild `base.idx` from the segments on disk.
//! - `verify` — re-read sampled bodies and re-hash them against their
//!   entries: proof the recorded offsets are right, on real chunk bytes.
//! - `lookup` — one hash (optionally one output) as JSON.
//! - `stats` — what the index covers.
//! - `serve` — the loopback HTTP read surface.
//!
//! Design: cnft.dev-workers docs/design/TX_INDEX.md.

mod serve;
mod verify;

use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use tx_index::compact::compact;
use tx_index::extract::{extract_to_segment, list_chunks};
use tx_index::segment::list_segments;
use tx_index::wire::{OutputResponse, TxResponse};
use tx_index::{Index, Resolution};

#[derive(Parser, Debug)]
#[command(about = "tx hash → chunk offset index over a Mithril immutable DB")]
enum Cmd {
    /// Extract missing segments, then compact (the refresh step).
    Build(BuildArgs),
    /// Rebuild base.idx from the segments on disk.
    Compact(DirArgs),
    /// Look one tx hash up.
    Lookup(LookupArgs),
    /// Report coverage.
    Stats(DirArgs),
    /// Re-hash sampled bodies against their entries.
    Verify(verify::VerifyArgs),
    /// Loopback HTTP read surface.
    Serve(serve::ServeArgs),
}

#[derive(clap::Args, Debug)]
struct DirArgs {
    /// Index directory (segments/ + base.idx).
    #[arg(long)]
    index_dir: PathBuf,

    /// Immutable DB dir (the one full of NNNNN.chunk files).
    #[arg(long)]
    immutable: PathBuf,
}

/// When `build` compacts after extracting.
#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
enum CompactPolicy {
    /// Compact when there is no base yet, or the tail has grown past
    /// `--tail-limit` segments.
    Auto,
    Always,
    Never,
}

#[derive(clap::Args, Debug)]
struct BuildArgs {
    #[command(flatten)]
    dirs: DirArgs,

    /// Extraction threads. Each holds one whole chunk (≤ ~100 MB) in memory.
    #[arg(long, default_value_t = 8)]
    threads: usize,

    #[arg(long, value_enum, default_value_t = CompactPolicy::Auto)]
    compact: CompactPolicy,

    /// `--compact auto`: tail segments tolerated before a rebuild.
    #[arg(long, default_value_t = 8)]
    tail_limit: usize,

    /// Stop after this many NEW segments (0 = no limit). For trying the
    /// extractor on a subset before committing a box to a full pass.
    #[arg(long, default_value_t = 0)]
    max_chunks: usize,
}

#[derive(clap::Args, Debug)]
struct LookupArgs {
    #[command(flatten)]
    dirs: DirArgs,

    /// 64-hex tx hash.
    hash: String,

    /// Output index to resolve; omitted = the whole body + every output.
    #[arg(long)]
    index: Option<u32>,
}

fn main() -> Result<()> {
    match Cmd::parse() {
        Cmd::Build(a) => build(a),
        Cmd::Compact(a) => {
            init_logging();
            let st = compact(&a.index_dir)?;
            tracing::info!(?st, "compacted");
            Ok(())
        }
        Cmd::Lookup(a) => lookup(a),
        Cmd::Stats(a) => {
            let idx = Index::open(&a.index_dir, &a.immutable)?;
            let cov = idx.coverage();
            println!(
                "{}",
                serde_json::to_string_pretty(&serve::coverage_json(&cov))?
            );
            Ok(())
        }
        Cmd::Verify(a) => verify::run(a),
        Cmd::Serve(a) => serve::run(a),
    }
}

fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
}

fn build(a: BuildArgs) -> Result<()> {
    init_logging();
    let immutable = &a.dirs.immutable;
    let index_dir = &a.dirs.index_dir;
    std::fs::create_dir_all(index_dir)
        .with_context(|| format!("creating {}", index_dir.display()))?;

    let chunks = list_chunks(immutable)?;
    let have: HashSet<u16> = list_segments(index_dir)?.into_iter().collect();
    let mut todo: Vec<u16> = chunks
        .iter()
        .copied()
        .filter(|c| !have.contains(c))
        .collect();
    if a.max_chunks > 0 {
        todo.truncate(a.max_chunks);
    }
    tracing::info!(
        chunks = chunks.len(),
        segments = have.len(),
        todo = todo.len(),
        threads = a.threads,
        "extracting"
    );

    let new_segments = extract_all(immutable, index_dir, &todo, a.threads)?;

    let base_last = tx_index::base::BaseFile::open(&tx_index::base::base_path(index_dir))
        .ok()
        .map(|b| b.header.last_chunk);
    let tail = list_segments(index_dir)?
        .into_iter()
        .filter(|c| base_last.is_none_or(|l| *c > l))
        .count();
    let do_compact = match a.compact {
        CompactPolicy::Always => true,
        CompactPolicy::Never => false,
        CompactPolicy::Auto => base_last.is_none() || tail > a.tail_limit,
    };
    tracing::info!(
        new_segments,
        ?base_last,
        tail,
        do_compact,
        "extraction done"
    );
    if do_compact {
        let st = compact(index_dir)?;
        tracing::info!(
            segments = st.segments,
            first_chunk = st.first_chunk,
            last_chunk = st.last_chunk,
            entries = st.entries,
            mib = st.file_len / (1 << 20),
            secs = format!("{:.1}", st.wall_secs),
            "compacted"
        );
    }
    Ok(())
}

/// Parallel extraction: workers pull chunk numbers off a queue; each chunk
/// is one `fs::read` + decode + hash + segment write, independent of every
/// other, so hit order does not matter and a crash mid-run loses nothing
/// already renamed into `segments/`.
fn extract_all(
    immutable: &std::path::Path,
    index_dir: &std::path::Path,
    todo: &[u16],
    threads: usize,
) -> Result<usize> {
    if todo.is_empty() {
        return Ok(0);
    }
    let started = Instant::now();
    let queue: Mutex<VecDeque<u16>> = Mutex::new(todo.iter().copied().collect());
    let done = AtomicU64::new(0);
    let bytes = AtomicU64::new(0);
    let txs = AtomicU64::new(0);
    let total = todo.len() as u64;

    std::thread::scope(|s| -> Result<()> {
        let handles: Vec<_> = (0..threads.max(1))
            .map(|_| {
                let queue = &queue;
                let done = &done;
                let bytes = &bytes;
                let txs = &txs;
                s.spawn(move || -> Result<()> {
                    loop {
                        let chunk = { queue.lock().expect("queue").pop_front() };
                        let Some(chunk) = chunk else { break };
                        let ex = extract_to_segment(immutable, index_dir, chunk)
                            .with_context(|| format!("extracting chunk {chunk}"))?;
                        let d = done.fetch_add(1, Ordering::Relaxed) + 1;
                        let b = bytes.fetch_add(ex.bytes, Ordering::Relaxed) + ex.bytes;
                        let t = txs.fetch_add(ex.entries.len() as u64, Ordering::Relaxed)
                            + ex.entries.len() as u64;
                        if d.is_multiple_of(50) || d == total {
                            let secs = started.elapsed().as_secs_f64();
                            tracing::info!(
                                done = d,
                                total,
                                txs = t,
                                gb = format!("{:.1}", b as f64 / 1e9),
                                mb_per_s = format!("{:.0}", b as f64 / 1e6 / secs.max(1e-9)),
                                "extract progress"
                            );
                        }
                    }
                    Ok(())
                })
            })
            .collect();
        for h in handles {
            h.join().expect("extract worker panicked")?;
        }
        Ok(())
    })?;
    Ok(todo.len())
}

fn parse_hash(s: &str) -> Result<[u8; 32]> {
    let v = hex::decode(s.trim()).context("tx hash is not hex")?;
    let arr: [u8; 32] = v
        .try_into()
        .map_err(|v: Vec<u8>| anyhow::anyhow!("tx hash is {} bytes, want 32", v.len()))?;
    Ok(arr)
}

fn lookup(a: LookupArgs) -> Result<()> {
    let idx = Index::open(&a.dirs.index_dir, &a.dirs.immutable)?;
    let hash = parse_hash(&a.hash)?;
    match a.index {
        None => {
            let Some(body) = idx.tx(&hash)? else {
                bail!("no body with hash {} in any completed chunk", a.hash);
            };
            let outputs = body.outputs()?;
            let resp = TxResponse {
                tx_hash: hex::encode(body.hash),
                era: body.era.to_string(),
                chunk: body.loc.chunk,
                offset: body.loc.offset,
                body_cbor: hex::encode(&body.cbor),
                outputs,
            };
            println!("{}", serde_json::to_string_pretty(&resp)?);
        }
        Some(index) => match idx.resolve(&hash, index)? {
            Resolution::Found { body, output } => {
                let resp = OutputResponse {
                    tx_hash: hex::encode(body.hash),
                    index,
                    era: body.era.to_string(),
                    chunk: body.loc.chunk,
                    output,
                };
                println!("{}", serde_json::to_string_pretty(&resp)?);
            }
            Resolution::NoSuchOutput { outputs, .. } => {
                bail!(
                    "tx {} has {outputs} outputs; index {index} does not exist",
                    a.hash
                )
            }
            Resolution::UnknownTx => {
                bail!("no body with hash {} in any completed chunk", a.hash)
            }
        },
    }
    Ok(())
}
