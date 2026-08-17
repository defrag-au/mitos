//! project-ledger — pure ingestion of one NFT project's two-sided mint view.
//!
//! One binary, modes: `bootstrap` (Mithril snapshot → verified immutable DB;
//! usually skipped — point `walk` at market-ledger's dir) / `seed` (floor +
//! initial frontier) / `walk` (decode chunks from the floor: activity, policy
//! assets, watched-party net flows, growing frontier) / `stats` / `reset`.
//! Coming: `classify` (kind projection), `enrich` (copy the policy's secondary
//! sales in from market-ledger), `export` (Parquet + manifest + checksums —
//! THE artifact), `backfill`.
//!
//! Never a follower: no tip tail, no live store. A refresh is a re-ingest that
//! yields a new snapshot version. Design: `cnft.dev-workers/docs/design/
//! PROJECT_LEDGER_IMPORTER.md`.

mod activity;
mod asset_class;
mod koios;
mod mint;
mod party;
mod registry;
mod resolve;
mod seed;
mod state;
mod store;
mod walk;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    version,
    about = "Project ledger walker: mint window + royalties, one slot-keyed ledger"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Download + verify a Mithril snapshot (or a partial immutable-file range).
    Bootstrap(mitos_chain_walk::mithril::BootstrapArgs),
    /// Establish the floor (indexer-seeded, walk-proven) and the initial frontier.
    Seed(seed::SeedArgs),
    /// Walk certified history from the floor.
    Walk(walk::WalkArgs),
    /// Row counts + meta — a quick look at what a ledger holds.
    Stats(StatsArgs),
    /// Delete the ledger + checkpoint mirror for a clean restart (dry-run unless --yes).
    Reset(ResetArgs),
}

#[derive(clap::Args, Debug)]
struct StatsArgs {
    #[arg(long, default_value = "project-ledger.db")]
    db: PathBuf,
}

#[derive(clap::Args, Debug)]
struct ResetArgs {
    #[arg(long, default_value = "project-ledger.db")]
    db: PathBuf,
    /// Checkpoint file (default: `<db>.checkpoint.json`).
    #[arg(long)]
    checkpoint_file: Option<PathBuf>,
    /// Also delete this export tree.
    #[arg(long)]
    export: Option<PathBuf>,
    /// Actually delete.
    #[arg(long)]
    yes: bool,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    match Cli::parse().command {
        Command::Bootstrap(args) => mitos_chain_walk::mithril::bootstrap(args),
        Command::Seed(args) => seed::run(args),
        Command::Walk(args) => walk::run(args),
        Command::Stats(args) => stats(args),
        Command::Reset(args) => reset(args),
    }
}

fn stats(args: StatsArgs) -> Result<()> {
    let ledger = store::Ledger::open(&args.db)?;
    for k in [
        seed::META_PROJECT,
        seed::META_POLICY,
        seed::META_POLICY_LABEL,
        seed::META_FLOOR_SLOT,
        seed::META_FLOOR_SOURCE,
        seed::META_FLOOR_BASIS,
        seed::META_WALK_START,
        seed::META_EXPECTED_ASSETS,
        seed::META_SUPPLY,
        seed::META_CEILING_SLOT,
        seed::META_LAST_MINT_SLOT,
        seed::META_ROYALTY_ADDR,
        seed::META_ROYALTY_RATE,
        seed::META_SIGNER_CREDS,
    ] {
        if let Some(v) = ledger.meta_get(k)? {
            let v = if v.len() > 80 {
                format!("{}…", &v[..80])
            } else {
                v
            };
            println!("{k:<18} {v}");
        }
    }
    if let Some((slot, hash)) = ledger.cursor()? {
        println!("{:<18} {slot} {}", "cursor", hex::encode(hash));
    }
    for t in [
        "party",
        "asset_event",
        "tx_delta",
        "value_event",
        "value_kind",
        "secondary_sale",
        "outref_buffer",
        "outref_cache",
        "asset_holder",
    ] {
        println!("{t:<18} {}", ledger.count(t)?);
    }
    Ok(())
}

fn reset(args: ResetArgs) -> Result<()> {
    use mitos_chain_walk::checkpoint::{default_path, reset_files, wipe};
    let checkpoint = args
        .checkpoint_file
        .unwrap_or_else(|| default_path(&args.db));
    if !args.yes {
        println!("reset (dry-run; pass --yes to delete) would remove:");
        for f in reset_files(&args.db, &checkpoint) {
            if f.exists() {
                println!("  {}", f.display());
            }
        }
        if let Some(e) = &args.export
            && e.exists()
        {
            println!("  {}", e.display());
        }
        return Ok(());
    }
    let removed = wipe(&args.db, &checkpoint, args.export.as_deref())?;
    for f in &removed {
        tracing::info!(path = %f.display(), "reset: removed");
    }
    Ok(())
}
