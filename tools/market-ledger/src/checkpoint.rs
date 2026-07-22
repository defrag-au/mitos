//! Crash-visible progress + reset.
//!
//! The resumable state proper lives in the sqlite ledger (`walk_cursor` +
//! `outref_buffer`); this writes a small JSON **mirror** of the last committed
//! checkpoint next to it, so a crashed/killed process leaves an at-a-glance
//! marker (`cat <db>.checkpoint.json`) of exactly where the resumable point is.
//! Written atomically (temp + rename) at the same cadence as the sqlite
//! checkpoint, so the file never runs ahead of what a resume would actually use.
//!
//! `reset` wipes the ledger + checkpoint (+ optional Parquet) so a walk starts
//! clean — either via the `reset` subcommand or a `--reset-flag` file the walk
//! consumes at startup (drop the file, next run cleans + restarts).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Serialize;

/// A small JSON mirror of the last committed walk checkpoint.
#[derive(Serialize)]
pub struct CheckpointFile {
    /// Absolute slot of the last checkpointed block — the resumable point.
    pub last_slot: u64,
    pub last_block_height: Option<u64>,
    /// Hex of the last checkpointed block hash (empty on the final flush).
    pub last_block_hash: String,
    pub scanned_blocks: u64,
    pub in_range_blocks: u64,
    pub inserted_rows: u64,
    /// Open-book size (unspent watched outputs currently buffered).
    pub open_book: usize,
    pub venues: Vec<String>,
    /// Wall-clock unix seconds this file was written.
    pub updated_unix: u64,
    /// `true` on the final flush (the walk finished / reached its stop bound).
    pub done: bool,
}

/// Write the checkpoint JSON atomically (temp + rename — a crash leaves either
/// the old file or the new one, never a partial).
pub fn write(path: &Path, cp: &CheckpointFile) -> Result<()> {
    let json = serde_json::to_string_pretty(cp).context("serialising checkpoint")?;
    let tmp = with_suffix(path, ".tmp");
    std::fs::write(&tmp, json).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("renaming {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

/// The default checkpoint-file path for a ledger db (`foo.db` → `foo.checkpoint.json`).
pub fn default_path(db: &Path) -> PathBuf {
    db.with_extension("checkpoint.json")
}

pub fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Delete the ledger (db + `-wal` + `-shm`), the checkpoint file, and optionally
/// the Parquet tree, so the next walk starts clean. Missing files are skipped.
pub fn wipe(db: &Path, checkpoint: &Path, parquet: Option<&Path>) -> Result<Vec<PathBuf>> {
    let mut removed = Vec::new();
    let files = [
        db.to_path_buf(),
        with_suffix(db, "-wal"),
        with_suffix(db, "-shm"),
        checkpoint.to_path_buf(),
    ];
    for f in files {
        if f.exists() {
            std::fs::remove_file(&f).with_context(|| format!("removing {}", f.display()))?;
            removed.push(f);
        }
    }
    if let Some(pq) = parquet
        && pq.exists()
    {
        std::fs::remove_dir_all(pq).with_context(|| format!("removing {}", pq.display()))?;
        removed.push(pq.to_path_buf());
    }
    Ok(removed)
}

/// Append a raw suffix to a path's file name (`foo.db` + `-wal` → `foo.db-wal`).
fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(suffix);
    PathBuf::from(s)
}

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
        for f in [
            args.db.clone(),
            with_suffix(&args.db, "-wal"),
            with_suffix(&args.db, "-shm"),
            checkpoint.clone(),
        ] {
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
