//! wallet-sieve — one wallet's flow story from raw chain bytes, NO index.
//!
//! Three passes over a certified Mithril immutable DB:
//!
//! - **A. cred scan** — memmem the wallet's 28-byte credentials across raw
//!   chunk files, decode only hit blocks. Finds every output paying the
//!   wallet, and (via change outputs) most spends.
//! - **B. sweeps** (`--sweeps`) — memmem the wallet's own tx hashes to catch
//!   change-less spends (a spending tx names its source hash in raw bytes).
//!   Measured near-useless on a real wallet (2 of 870 txs) — off by default.
//! - **C. resolve** (`--resolve`) — decode+hash txs newest-first to name the
//!   senders behind receipts; early-exits once every wanted source is found.
//!
//! Two faces: `scan` (one-shot CLI, JSONL out) and `serve` (hosted read
//! surface with a per-wallet cache, incremental refresh from a chunk cursor,
//! and bearer auth — the market-ledger serve pattern).
//!
//! Measured 2026-08-25 on cardano-infra ($djo, full history): pass A 71.5s
//! for 225.5 GB at 3.15 GB/s with zero false-positive blocks; resolve 150.3s
//! with early exit. The product framing lives in the cnft.dev-workers
//! flow-explorer notes.

mod classify;
mod excavate;
mod market;
mod progress;
mod report;
mod resolve;
mod scan;
mod serve;
mod tail;
mod target;

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use mitos_chain_walk::mithril::immutable_file_for_slot;
use mitos_chain_walk::slot_to_unix;

use crate::excavate::SHELLEY_START_SLOT;
use crate::progress::Progress;

#[derive(Parser, Debug)]
#[command(about = "Single-wallet flow excavation over a Mithril immutable DB — no index")]
enum Cmd {
    /// One-shot excavation: JSONL rows to stdout/file, timings to stderr.
    Scan(ScanArgs),
    /// Hosted read surface: per-wallet cache, refresh jobs, bearer auth.
    Serve(serve::ServeArgs),
}

#[derive(clap::Args, Debug)]
struct ScanArgs {
    /// Immutable DB dir (the one full of NNNNN.chunk files).
    #[arg(long)]
    immutable: PathBuf,

    /// stake1…, addr1…, or 28-byte hex credential.
    #[arg(long)]
    target: String,

    /// Don't scan below this slot (default: Shelley start).
    #[arg(long, default_value_t = SHELLEY_START_SLOT)]
    floor_slot: u64,

    /// Worker threads. Default leaves headroom for co-tenant services.
    #[arg(long, default_value_t = 10)]
    threads: usize,

    /// Pass B: catch change-less spends via the wallet's own tx hashes.
    #[arg(long)]
    sweeps: bool,

    /// Pass C: resolve sender addresses behind receipts (decode+hash pass).
    #[arg(long)]
    resolve: bool,

    /// JSONL output path (default: stdout).
    #[arg(long)]
    out: Option<PathBuf>,
}

fn main() -> Result<()> {
    match Cmd::parse() {
        Cmd::Scan(args) => run_scan(args),
        Cmd::Serve(args) => serve::run(args),
    }
}

fn run_scan(args: ScanArgs) -> Result<()> {
    let creds = target::parse(&args.target)?;
    let labels: Vec<String> = creds
        .iter()
        .map(|c| format!("{}:{}", c.label, hex::encode(c.bytes)))
        .collect();
    eprintln!("target credentials: {}", labels.join(", "));

    let on = |p: Progress<'_>| match p {
        Progress::Scan {
            pass,
            done,
            total,
            gb_per_s,
        } => {
            if done.is_multiple_of(500) {
                eprintln!("  … {pass}: {done}/{total} chunks, {gb_per_s:.2} GB/s");
            }
        }
        Progress::Resolve {
            done,
            total,
            wanted_left,
        } => {
            if done.is_multiple_of(20) {
                eprintln!("  … resolve: {done}/{total} bands, {wanted_left} sources still wanted");
            }
        }
        Progress::Phase { label, detail } => eprintln!("phase {label}: {detail}"),
    };

    let outcome = excavate::run(
        excavate::Params {
            immutable: &args.immutable,
            creds: creds.iter().map(|c| c.bytes).collect(),
            scan_from_chunk: immutable_file_for_slot(args.floor_slot),
            seed_owned: HashMap::new(),
            threads: args.threads,
            sweeps: args.sweeps,
            resolve: args.resolve,
        },
        &on,
    )?;

    let a = &outcome.pass_a;
    eprintln!(
        "pass A (cred sieve): {:.1}s — {:.1} GB at {:.2} GB/s, {} hit chunks, {} hit blocks ({} unmatched), {} txs",
        a.wall_secs,
        a.bytes as f64 / 1e9,
        a.bytes as f64 / 1e9 / a.wall_secs.max(0.001),
        a.hit_chunks,
        a.hit_blocks,
        a.unmatched_hit_blocks,
        outcome.timeline.txs.len()
    );
    if let Some(b) = &outcome.pass_b {
        eprintln!(
            "pass B (sweep sieve): {:.1}s — {:.1} GB, {} hit chunks",
            b.wall_secs,
            b.bytes as f64 / 1e9,
            b.hit_chunks
        );
    }
    if let Some(secs) = outcome.resolve_secs {
        eprintln!(
            "pass C (resolve): {secs:.1}s — {} sources named",
            outcome.sources.len()
        );
    }

    let timeline = &outcome.timeline;
    if timeline.txs.is_empty() {
        eprintln!("no activity found for target");
        return Ok(());
    }
    eprintln!(
        "timeline: {} txs, {} → {}",
        timeline.txs.len(),
        report::fmt_unix(slot_to_unix(timeline.first_slot)),
        report::fmt_unix(slot_to_unix(timeline.last_slot))
    );

    let mut out: Box<dyn Write> = match &args.out {
        Some(p) => {
            Box::new(std::fs::File::create(p).with_context(|| format!("creating {}", p.display()))?)
        }
        None => Box::new(std::io::stdout().lock()),
    };
    let mut received = 0u64;
    let mut sent = 0u64;
    for tx in &timeline.txs {
        received += tx.lovelace_in;
        sent += tx.lovelace_out;
        let row = report::row_for(tx, &outcome.sources);
        let line = serde_json::to_string(&row)?;
        writeln!(out, "{line}")?;
    }
    eprintln!(
        "totals: in {:.1} ₳, out {:.1} ₳, net {:.1} ₳ over {} txs ({} outrefs still held)",
        received as f64 / 1e6,
        sent as f64 / 1e6,
        (received as i64 - sent as i64) as f64 / 1e6,
        timeline.txs.len(),
        timeline.owned.len()
    );
    Ok(())
}
