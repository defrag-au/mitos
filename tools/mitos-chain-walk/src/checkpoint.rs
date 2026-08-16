//! Crash-visible progress mirror + wipe.
//!
//! A walker's resumable state proper lives in its own store (market-ledger:
//! `walk_cursor` + `outref_buffer`; project-ledger: those plus the frontier).
//! This writes a small JSON **mirror** of the last committed checkpoint next to
//! it, so a crashed/killed process leaves an at-a-glance marker
//! (`cat <db>.checkpoint.json`) of exactly where the resumable point is.
//! Written atomically (temp + rename) at the same cadence as the store
//! checkpoint, so the file never runs ahead of what a resume would actually use.
//!
//! [`wipe`] is the shared half of a walker's `reset`: db (+ `-wal`/`-shm`),
//! checkpoint file, and an optional extra tree. The CLI wrapper (dry-run,
//! defaults) stays with each walker.

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
    /// What the walk is scoped to — venue names for market-ledger, the policy
    /// id for project-ledger. Free-form so the file stays readable by eye.
    pub scope: Vec<String>,
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

/// The files a reset would touch for `db` — the sqlite trio + the checkpoint
/// mirror. Shared by [`wipe`] and walkers' dry-run listings so the two never
/// disagree about what "reset" means.
pub fn reset_files(db: &Path, checkpoint: &Path) -> [PathBuf; 4] {
    [
        db.to_path_buf(),
        with_suffix(db, "-wal"),
        with_suffix(db, "-shm"),
        checkpoint.to_path_buf(),
    ]
}

/// Delete the ledger (db + `-wal` + `-shm`), the checkpoint file, and optionally
/// an extra tree (market-ledger's Parquet dir; project-ledger's export dir), so
/// the next walk starts clean. Missing files are skipped.
pub fn wipe(db: &Path, checkpoint: &Path, extra_tree: Option<&Path>) -> Result<Vec<PathBuf>> {
    let mut removed = Vec::new();
    for f in reset_files(db, checkpoint) {
        if f.exists() {
            std::fs::remove_file(&f).with_context(|| format!("removing {}", f.display()))?;
            removed.push(f);
        }
    }
    if let Some(tree) = extra_tree
        && tree.exists()
    {
        std::fs::remove_dir_all(tree).with_context(|| format!("removing {}", tree.display()))?;
        removed.push(tree.to_path_buf());
    }
    Ok(removed)
}

/// Append a raw suffix to a path's file name (`foo.db` + `-wal` → `foo.db-wal`).
pub fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(suffix);
    PathBuf::from(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suffix_and_default_paths() {
        let db = Path::new("/tmp/x/ledger.db");
        assert_eq!(
            with_suffix(db, "-wal"),
            PathBuf::from("/tmp/x/ledger.db-wal")
        );
        assert_eq!(
            default_path(db),
            PathBuf::from("/tmp/x/ledger.checkpoint.json")
        );
    }

    #[test]
    fn write_is_atomic_and_readable() {
        let dir = std::env::temp_dir().join(format!("mcw-cp-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("t.checkpoint.json");
        write(
            &path,
            &CheckpointFile {
                last_slot: 7,
                last_block_height: Some(1),
                last_block_hash: "ab".into(),
                scanned_blocks: 1,
                in_range_blocks: 1,
                inserted_rows: 0,
                open_book: 0,
                scope: vec!["p".into()],
                updated_unix: 0,
                done: false,
            },
        )
        .unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("\"last_slot\": 7"));
        assert!(!with_suffix(&path, ".tmp").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
