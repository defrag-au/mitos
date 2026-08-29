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
mod alias;
mod asset_class;
mod classify;
mod enrich;
mod koios;
mod local;
mod mint;
mod party;
mod provenance;
mod registry;
mod resolve;
mod score;
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
    /// Resolve a walk's unresolved inputs LOCALLY, out of the snapshot.
    ///
    /// Run between two walks: the first records what it could not resolve, this
    /// fetches it from the immutable DB, the second books correctly. Without it
    /// a walk cannot tell an incoming payment from the wallet's own change.
    ResolveLocal(local::LocalArgs),
    /// Copy the policy's VENUE sales in from market-ledger.
    ///
    /// The walk sees an asset move and a net change; it cannot see that the
    /// movement was a sale. Worse, a marketplace seller funds their own sale
    /// transaction, so the change rule correctly skips the proceeds and the
    /// trade reads as a give-away. This joins market-ledger in by tx_hash.
    ///
    /// Venue sales ONLY — a peer-to-peer trade leaves no marketplace event.
    /// Cheap and recomputable: re-run after any walk.
    Enrich(enrich::EnrichArgs),
    /// Name the counterparties the chain can identify — DEX pools, batchers,
    /// marketplace contracts.
    ///
    /// Without it a swap's RETURN leg reads as project income: value the
    /// treasury sent out, coming back after a conversion. Reports what it could
    /// not name, because a registry fails silently.
    Classify(classify::ClassifyArgs),
    /// Score transactions and parties by investigative INTEREST.
    ///
    /// Attention, never fact: scores appear in no exported figure, and every
    /// score decomposes into signal rows that sum to it. Runs LOCALLY against
    /// the ledger + the app's annotations sidecar — human classifications are
    /// the primary signal, and they never leave the operator's machine.
    Score(score::ScoreArgs),
    /// Measure the EFFECTIVE team-funded mint supply: direct core mints plus
    /// mints by wallets whose funding traces to the core cluster through up
    /// to two intermediaries.
    ///
    /// The dark-wallet detector: compare the output against the project's
    /// ADVERTISED allocation — the chain cannot know what was promised, but it
    /// knows who paid for every mint. Every flagged holder prints its funding
    /// legs with tx hashes. Needs a `--watch-holders` walk; runs locally with
    /// the annotations sidecar, like `score`.
    Provenance(provenance::ProvenanceArgs),
    /// Export core/founder assertions from the app's annotations sidecar as
    /// `[[wallet]]` registry fragments.
    ///
    /// The bridge in the curation loop: assert once in the app, export, review,
    /// append to the box registry, re-walk. Tentative assertions stay app-side.
    EmitRegistry(score::EmitRegistryArgs),
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
    /// Also discard the resolved-input cache, deleting the ledger file outright.
    ///
    /// Off by default: `outref_cache` is paid for by a full snapshot scan and a
    /// reset is normally a prelude to the re-walk that spends it.
    #[arg(long)]
    purge_cache: bool,
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
        Command::ResolveLocal(args) => local::resolve_local(&args),
        Command::Enrich(args) => enrich::run(&args),
        Command::Classify(args) => classify::run(&args),
        Command::Score(args) => score::run(&args),
        Command::EmitRegistry(args) => score::emit_registry(&args),
        Command::Provenance(args) => provenance::run(&args),
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
        "mint_payment",
        "party_alias",
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
        if args.purge_cache {
            for f in reset_files(&args.db, &checkpoint) {
                if f.exists() {
                    println!("  {}", f.display());
                }
            }
        } else {
            println!("  every walk-derived table in {}", args.db.display());
            println!(
                "  KEEPING outref_cache + wanted_outref (pass --purge-cache to drop them too)"
            );
            if checkpoint.exists() {
                println!("  {}", checkpoint.display());
            }
        }
        if let Some(e) = &args.export
            && e.exists()
        {
            println!("  {}", e.display());
        }
        return Ok(());
    }

    if args.purge_cache {
        let removed = wipe(&args.db, &checkpoint, args.export.as_deref())?;
        for f in &removed {
            tracing::info!(path = %f.display(), "reset: removed");
        }
        return Ok(());
    }

    // Keep the file, clear only what the walk derives — see `reset_derived`.
    if args.db.exists() {
        let mut ledger = store::Ledger::open(&args.db)?;
        let cleared = ledger.reset_derived()?;
        let (wanted, have) = ledger.wanted_progress()?;
        for (table, n) in &cleared {
            tracing::debug!(table, rows = n, "reset: cleared");
        }
        tracing::info!(
            db = %args.db.display(),
            rows_cleared = cleared.iter().map(|(_, n)| n).sum::<u64>(),
            resolution_cache_kept = format!("{have}/{wanted} refs closed"),
            "reset: ledger cleared, resolution cache kept"
        );
    }
    // `-wal`/`-shm` are deliberately left to sqlite: the ledger is still a live
    // database here, and removing them by hand is only safe if the close was
    // clean — a bad bet against a corrupted 2 GB file.
    if checkpoint.exists() {
        std::fs::remove_file(&checkpoint)?;
        tracing::info!(path = %checkpoint.display(), "reset: removed");
    }
    if let Some(e) = &args.export
        && e.exists()
    {
        std::fs::remove_dir_all(e)?;
        tracing::info!(path = %e.display(), "reset: removed");
    }
    Ok(())
}
