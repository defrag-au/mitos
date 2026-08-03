//! `stats` — corpus-sanity report over the ledger. Post-walk sanity per the
//! design doc: venue × kind split, fills/month, buyer-price vs settlement
//! premium (jpg-era ≈ 0, Wayup / Wayup-settled-jpg ≈ +2%), slot/time extent, and
//! the per-venue cursor + sealed-partition state. Read-only; run it on-box after
//! a walk to eyeball the corpus before uploading.

use std::path::PathBuf;

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};

#[derive(clap::Args, Debug)]
pub struct StatsArgs {
    /// Ledger sqlite path.
    #[arg(long, default_value = "market-ledger.db")]
    db: PathBuf,
}

pub fn run(args: StatsArgs) -> Result<()> {
    let conn = Connection::open_with_flags(&args.db, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("opening ledger {} read-only", args.db.display()))?;

    let total: i64 = conn.query_row("SELECT COUNT(*) FROM market_events", [], |r| r.get(0))?;
    println!("market_events rows: {total}");
    if total == 0 {
        return Ok(());
    }

    let (min_slot, max_slot): (i64, i64) =
        conn.query_row("SELECT MIN(slot), MAX(slot) FROM market_events", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })?;
    println!("slot extent: {min_slot} … {max_slot}");

    println!("\nvenue × kind:");
    let mut stmt = conn
        .prepare("SELECT venue, kind, COUNT(*) FROM market_events GROUP BY 1, 2 ORDER BY 1, 2")?;
    for row in stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, i64>(2)?,
        ))
    })? {
        let (venue, kind, n) = row?;
        println!("  {venue:<8} {kind:<26} {n:>10}");
    }

    println!("\nfills/month (sold + offer accepts), by venue:");
    let mut stmt = conn.prepare(
        "SELECT venue,
                strftime('%Y-%m', datetime(block_time,'unixepoch')) ym,
                COUNT(*)
         FROM market_events
         WHERE kind IN ('sold','offer_accepted','collection_offer_accepted')
         GROUP BY 1, 2 ORDER BY 1, 2",
    )?;
    for row in stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, i64>(2)?,
        ))
    })? {
        let (venue, ym, n) = row?;
        println!("  {venue:<8} {ym}  {n:>8}");
    }

    println!("\nbuyer-price premium on sold rows (avg buyer/settlement − 1):");
    let mut stmt = conn.prepare(
        "SELECT venue,
                AVG(CAST(buyer_price_lovelace AS REAL) / price_lovelace) - 1.0,
                COUNT(*)
         FROM market_events
         WHERE kind = 'sold' AND price_lovelace > 0 AND buyer_price_lovelace IS NOT NULL
         GROUP BY 1 ORDER BY 1",
    )?;
    for row in stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, f64>(1)?,
            r.get::<_, i64>(2)?,
        ))
    })? {
        let (venue, premium, n) = row?;
        println!("  {venue:<8} {:+.2}%  (n={n})", premium * 100.0);
    }

    println!("\nwalk cursors:");
    let mut stmt = conn.prepare("SELECT venue, slot FROM walk_cursor ORDER BY venue")?;
    for row in stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))? {
        let (venue, slot) = row?;
        println!("  {venue:<8} slot {slot}");
    }
    let open_book: i64 = conn.query_row("SELECT COUNT(*) FROM outref_buffer", [], |r| r.get(0))?;
    println!("open book (unspent watched outputs buffered): {open_book}");

    let sealed: i64 =
        conn.query_row("SELECT COUNT(*) FROM partition_manifest", [], |r| r.get(0))?;
    println!("sealed parquet partitions: {sealed}");

    Ok(())
}
