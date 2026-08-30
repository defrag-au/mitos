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
        Command::Inspect {
            dir,
            token,
            at_slot,
        } => export::inspect(&dir, &token, at_slot),
        Command::Probe { db } => walk::probe(&db),
        Command::Classify { db, tokens } => walk::classify(&db, &tokens),
    }
}
