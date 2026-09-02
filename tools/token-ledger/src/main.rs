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

mod buffer;
mod cohort;
mod export;
mod pools;
mod registry;
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
