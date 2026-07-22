//! `seal` — write completed months to per-venue Parquet partitions via the
//! `duckdb` CLI, and record them in `partition_manifest`.
//!
//! Per-venue partitioning (`events/venue=<v>/year=<y>/month=<m>.parquet`) is what
//! makes venue re-runs safe: a new venue's backfill creates its own partition
//! tree; sealed partitions of other venues are never rewritten. Sealed = a
//! complete month (strictly before the ledger's newest month, unless `--all`).
//! Shells the duckdb CLI (not the bundled crate) to keep the on-box build light;
//! in-process DuckDB is a serve-mode (Phase 2) concern.

use std::path::PathBuf;
use std::process::Command;

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OptionalExtension, params};

#[derive(clap::Args, Debug)]
pub struct SealArgs {
    /// Ledger sqlite path.
    #[arg(long, default_value = "market-ledger.db")]
    db: PathBuf,

    /// Root for the hive-partitioned Parquet tree
    /// (`<out-dir>/events/venue=/year=/month=.parquet`).
    #[arg(long, default_value = "parquet")]
    out_dir: PathBuf,

    /// Path to the `duckdb` CLI.
    #[arg(long, env = "DUCKDB", default_value = "duckdb")]
    duckdb: String,

    /// Seal even the current (incomplete) month.
    #[arg(long)]
    all: bool,
}

pub fn run(args: SealArgs) -> Result<()> {
    let conn = Connection::open(&args.db)
        .with_context(|| format!("opening ledger {}", args.db.display()))?;

    let max_bt: Option<i64> = conn
        .query_row("SELECT MAX(block_time) FROM market_events", [], |r| {
            r.get(0)
        })
        .optional()?
        .flatten();
    let Some(max_bt) = max_bt else {
        tracing::info!("seal: ledger is empty, nothing to seal");
        return Ok(());
    };
    // Current (incomplete) month of the ledger — sealed only under `--all`.
    let (cur_y, cur_m): (i64, i64) = conn.query_row(
        "SELECT CAST(strftime('%Y', datetime(?1,'unixepoch')) AS INTEGER),
                CAST(strftime('%m', datetime(?1,'unixepoch')) AS INTEGER)",
        [max_bt],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;

    let mut stmt = conn.prepare(
        "SELECT venue,
                CAST(strftime('%Y', datetime(block_time,'unixepoch')) AS INTEGER),
                CAST(strftime('%m', datetime(block_time,'unixepoch')) AS INTEGER),
                COUNT(*), MAX(slot)
         FROM market_events GROUP BY 1, 2, 3 ORDER BY 1, 2, 3",
    )?;
    let partitions: Vec<(String, i64, i64, i64, i64)> = stmt
        .query_map([], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
        })?
        .collect::<rusqlite::Result<_>>()?;
    drop(stmt);

    let db_abs = std::fs::canonicalize(&args.db).unwrap_or_else(|_| args.db.clone());
    let mut sealed = 0usize;
    for (venue, year, month, count, max_slot) in partitions {
        let already: Option<i64> = conn
            .query_row(
                "SELECT 1 FROM partition_manifest WHERE venue=?1 AND year=?2 AND month=?3",
                params![venue, year, month],
                |r| r.get(0),
            )
            .optional()?;
        if already.is_some() {
            continue;
        }
        if !args.all && (year, month) >= (cur_y, cur_m) {
            continue; // incomplete current month
        }

        let dir = args
            .out_dir
            .join("events")
            .join(format!("venue={venue}"))
            .join(format!("year={year}"));
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("creating partition dir {}", dir.display()))?;
        let path = dir.join(format!("month={month:02}.parquet"));

        let sql = format!(
            "INSTALL sqlite; LOAD sqlite; \
             ATTACH '{db}' AS l (TYPE sqlite, READ_ONLY); \
             COPY (SELECT * FROM l.market_events \
                   WHERE venue = '{venue}' \
                     AND year(to_timestamp(block_time)) = {year} \
                     AND month(to_timestamp(block_time)) = {month}) \
             TO '{out}' (FORMAT PARQUET);",
            db = db_abs.display(),
            out = path.display(),
        );
        let status = Command::new(&args.duckdb)
            .arg("-c")
            .arg(&sql)
            .status()
            .with_context(|| {
                format!(
                    "spawning `{}` (is the duckdb CLI installed / on PATH? override with \
                     --duckdb or $DUCKDB)",
                    args.duckdb
                )
            })?;
        if !status.success() {
            bail!("duckdb seal failed for {venue} {year}-{month:02} ({status})");
        }

        conn.execute(
            "INSERT OR REPLACE INTO partition_manifest (venue, year, month, sealed_slot, rows)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![venue, year, month, max_slot, count],
        )?;
        tracing::info!(%venue, year, month, count, path = %path.display(), "seal: wrote partition");
        sealed += 1;
    }

    tracing::info!(sealed, "seal: complete");
    Ok(())
}
