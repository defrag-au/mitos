//! `reset` — the CLI half of a clean restart. The crash-visible checkpoint
//! mirror and the wipe itself live in `mitos_chain_walk::checkpoint` (shared
//! with project-ledger); this keeps market-ledger's defaults + dry-run.

use std::path::PathBuf;

use anyhow::Result;
use mitos_chain_walk::checkpoint::reset_files;
pub use mitos_chain_walk::checkpoint::{CheckpointFile, default_path, now_unix, wipe, write};

#[derive(clap::Args, Debug)]
pub struct ResetArgs {
    /// Ledger sqlite path.
    #[arg(long, default_value = "market-ledger.db")]
    db: PathBuf,

    /// Checkpoint file (default: `<db>.checkpoint.json`).
    #[arg(long)]
    checkpoint_file: Option<PathBuf>,

    /// Also delete this Parquet tree.
    #[arg(long)]
    parquet: Option<PathBuf>,

    /// Actually delete. Without this, only prints what would be removed.
    #[arg(long)]
    yes: bool,
}

pub fn run_reset(args: ResetArgs) -> Result<()> {
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
        if let Some(pq) = &args.parquet
            && pq.exists()
        {
            println!("  {}", pq.display());
        }
        return Ok(());
    }
    let removed = wipe(&args.db, &checkpoint, args.parquet.as_deref())?;
    for f in &removed {
        tracing::info!(path = %f.display(), "reset: removed");
    }
    tracing::info!(count = removed.len(), "reset: complete");
    Ok(())
}
