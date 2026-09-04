//! token-ledger — a replayable movement log for one Cardano native token.
//!
//! The fourth walker in the `market-ledger` / `project-ledger` family, and the
//! substrate `docs/design/TOKEN_LEDGER.md` (in cnft.dev-workers) describes:
//! every movement of a watched asset, recorded as signed per-party deltas over
//! certified Mithril history, so that balance, holder count, float,
//! concentration and market cap are all *projections at a time `t`* rather than
//! stored numbers.
//!
//! Chain plumbing is `mitos-chain-walk`; this tool adds none of its own.
//!
//! Modes (this slice ships the first two):
//!
//! - `walk`  — immutable-DB chunks → the local ledger
//! - `stats` — derived balances at tip, for reconciliation against a known total
//! - `export`/`serve` — later; see the design doc's three-tier artifact

mod archive;
mod buffer;
mod cohort;
mod export;
mod policy_api;
mod pools;
mod registry;
mod reverse;
mod segments;
mod serve;
mod store;
mod walk;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    version,
    about = "Replayable movement log for one Cardano native token"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Walk certified immutable-DB history into the ledger.
    Walk(walk::WalkArgs),
    /// Walk BACKWARD from the archive's floor, newest first, into Parquet.
    ///
    /// The progressive counterpart to `walk`. `walk` is complete-or-nothing:
    /// it starts at the policy's first mint so its buffer is complete and every
    /// balance projection over it is exact, and it produces the newest row
    /// LAST. This produces the newest row FIRST and deepens on demand, which is
    /// what a feed needs and what makes a policy nobody has indexed viewable in
    /// seconds rather than after a full history walk.
    ///
    /// No database: each pass writes one stamped Parquet file plus the
    /// inputs it is still waiting on, and the policy's `manifest.json` is the
    /// only record. Cost follows ACTIVITY in the window, not the policy's
    /// supply.
    Reverse(reverse::ReverseArgs),
    /// Read a policy archive back the way a Worker would — footers first,
    /// then only the row groups a page or a lookup needs — and report what it
    /// cost in requests and bytes.
    Archive(archive::InspectArgs),
    /// Fold a policy's passes into ONE file at its root — fewer footers per
    /// read, one object per policy for R2. Runs on its own after every few
    /// passes; this forces it.
    Rollup(segments::RollupArgs),
    /// Write a policy's bundle — manifest plus every footer, one blob for
    /// KV — from its manifest. Landing writes one; this backfills.
    Bundle(archive::BundleArgs),
    /// Derived balances at tip — the reconciliation surface.
    Stats {
        #[arg(long)]
        db: PathBuf,
        /// How many holders to print.
        #[arg(long, default_value_t = 20)]
        top: usize,
    },
    /// Write the spine + detail artifacts a frontend loads.
    Export(export::ExportArgs),
    /// Hosted "any token on demand": sieve walk → export → push, behind a
    /// poll endpoint. Flow-explorer's serve pattern applied to tokens.
    Serve(serve::ServeArgs),
    /// Read the artifacts back with no database — what a consumer sees.
    Inspect {
        #[arg(long, default_value = ".")]
        dir: PathBuf,
        #[arg(long)]
        token: String,
        /// Evaluate at this slot instead of the end of the domain.
        #[arg(long)]
        at_slot: Option<u64>,
    },
    /// Write the catalogue card from an already-exported spine.
    ///
    /// `export` writes this too, so the usual path needs nothing. This
    /// exists for the tokens pushed BEFORE the card existed: their
    /// artifacts are complete and a catalogue cannot see them, and
    /// re-walking a token to regenerate a 300-byte JSON sidecar would be
    /// absurd when the spine on disk already holds every field.
    Card {
        #[arg(long, default_value = ".")]
        dir: PathBuf,
        #[arg(long)]
        token: String,
    },
    /// Emit a real series as Rust source, for widget development.
    ///
    /// Widget work needs data with the awkward shapes real tokens have —
    /// a 98.7% collapse, a band that swells and never releases, four years of
    /// near-flat tail. Synthetic curves flatter a chart and hide exactly the
    /// cases that decide whether a form works.
    Fixture {
        #[arg(long, default_value = ".")]
        dir: PathBuf,
        #[arg(long)]
        token: String,
        /// Roughly how many points to emit. Sampled evenly across the domain.
        #[arg(long, default_value_t = 240)]
        points: usize,
    },
    /// Interrogate the unclassified band — do these contracts look like locks?
    Probe {
        #[arg(long)]
        db: PathBuf,
    },
    /// Re-derive party cohorts from their addresses. No chain access.
    Classify {
        #[arg(long)]
        db: PathBuf,
        #[arg(long, default_value = "tokens.toml")]
        tokens: PathBuf,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    match Cli::parse().command {
        Command::Walk(args) => walk::run(args),
        Command::Reverse(args) => reverse::run(args),
        Command::Archive(args) => archive::inspect(args),
        Command::Rollup(args) => segments::run_rollup(args),
        Command::Bundle(args) => archive::bundle(args),
        Command::Stats { db, top } => walk::stats(&db, top),
        Command::Export(args) => export::run(args),
        Command::Serve(args) => serve::run(args),
        Command::Inspect {
            dir,
            token,
            at_slot,
        } => export::inspect(&dir, &token, at_slot),
        Command::Card { dir, token } => export::card_from_dir(&dir, &token),
        Command::Fixture { dir, token, points } => export::fixture(&dir, &token, points),
        Command::Probe { db } => walk::probe(&db),
        Command::Classify { db, tokens } => walk::classify(&db, &tokens),
    }
}
