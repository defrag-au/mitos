//! Read-only handle on the ledger.
//!
//! `Db` holds only the path; every request opens its own read-only
//! connection inside `spawn_blocking` (a read-only WAL open is microseconds,
//! readers never block the single walker writer, and rusqlite `Connection`
//! isn't `Sync` so it can't live in shared axum state). This is also the
//! seam where the phase-2 DuckDB union view (sealed Parquet + live sqlite)
//! slots in: handlers talk to `Db`, not raw SQL.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;

#[derive(Clone, Debug)]
pub struct Db {
    path: PathBuf,
}

/// `GET /health` body.
#[derive(Debug, Serialize)]
pub struct HealthSnapshot {
    pub rows: i64,
    pub slot_min: Option<i64>,
    pub slot_max: Option<i64>,
    pub latest_block_time: Option<i64>,
    /// `now - latest_block_time`. For a snapshot-walked corpus this is
    /// "snapshot age" and is expected to be large.
    pub freshness_secs: Option<i64>,
    pub cursors: Vec<VenueCursor>,
    pub partitions: Vec<Partition>,
}

#[derive(Debug, Serialize)]
pub struct VenueCursor {
    pub venue: String,
    pub slot: i64,
    pub block_hash: String,
}

#[derive(Debug, Serialize)]
pub struct Partition {
    pub venue: String,
    pub year: i64,
    pub month: i64,
    pub sealed_slot: i64,
    pub rows: i64,
}

impl Db {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Open a read-only connection (same pattern as `stats.rs`). `NO_MUTEX`
    /// is safe: each connection stays on the one blocking thread that
    /// opened it.
    pub fn open_ro(&self) -> Result<Connection> {
        Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .with_context(|| format!("opening ledger {} read-only", self.path.display()))
    }

    pub fn health(&self) -> Result<HealthSnapshot> {
        let conn = self.open_ro()?;

        let rows: i64 = conn.query_row("SELECT COUNT(*) FROM market_events", [], |r| r.get(0))?;
        let (slot_min, slot_max, latest_block_time): (Option<i64>, Option<i64>, Option<i64>) = conn
            .query_row(
                "SELECT MIN(slot), MAX(slot), MAX(block_time) FROM market_events",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;

        let mut cursors = Vec::new();
        let mut stmt =
            conn.prepare("SELECT venue, slot, block_hash FROM walk_cursor ORDER BY venue")?;
        let cursor_rows = stmt.query_map([], |r| {
            Ok(VenueCursor {
                venue: r.get(0)?,
                slot: r.get(1)?,
                block_hash: hex::encode(r.get::<_, Vec<u8>>(2)?),
            })
        })?;
        for c in cursor_rows {
            cursors.push(c?);
        }

        let mut partitions = Vec::new();
        let mut stmt = conn.prepare(
            "SELECT venue, year, month, sealed_slot, rows FROM partition_manifest
             ORDER BY venue, year, month",
        )?;
        let partition_rows = stmt.query_map([], |r| {
            Ok(Partition {
                venue: r.get(0)?,
                year: r.get(1)?,
                month: r.get(2)?,
                sealed_slot: r.get(3)?,
                rows: r.get(4)?,
            })
        })?;
        for p in partition_rows {
            partitions.push(p?);
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let freshness_secs = latest_block_time.map(|t| now - t);

        Ok(HealthSnapshot {
            rows,
            slot_min,
            slot_max,
            latest_block_time,
            freshness_secs,
            cursors,
            partitions,
        })
    }
}
