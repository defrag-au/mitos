//! `prune` — delete phantom `sold` rows left by cross-venue migrations.
//!
//! Before the marketplace-decode fix (`mitos-marketplace-decode` `sales.rs`
//! `is_marketplace_escrow`), re-listing an NFT from one venue's escrow to
//! another's (e.g. jpg → Wayup) was mis-booked as a `sold` at the origin venue,
//! with the receiving contract as the "buyer". The fix stops NEW such rows, but
//! `market_events` is append-only (`INSERT OR IGNORE`), so historical phantoms
//! must be deleted explicitly — a re-walk can't remove them.
//!
//! A migration writes two co-located rows for the same `(tx_hash, policy_id,
//! asset_name_hex)`: the phantom `sold` at the origin venue AND a genuine
//! `listed` at the destination venue (its `WayupStoreListing::Create`). A real
//! sale delivers the NFT to a buyer wallet and has no co-located cross-venue
//! `listed`. That co-occurrence is the pruning predicate.
//!
//! Caveat: a genuine buy-and-relist-on-another-venue in a single atomic tx (an
//! aggregator flow) also matches and would be pruned — rare, and the decoder
//! mis-prices it anyway. Dry-run by default; pass `--yes` to delete.

use std::path::PathBuf;

use anyhow::{Context, Result};
use rusqlite::Connection;

/// The phantom set: a `sold` event sharing `(tx_hash, policy_id,
/// asset_name_hex)` with a `listed` event at a DIFFERENT marketplace — the
/// signature of a cross-venue migration mis-booked as a sale. Direction-agnostic
/// (`marketplace <> marketplace` catches jpg → Wayup and Wayup → jpg alike).
const PHANTOM_ROWIDS: &str = "\
    SELECT s.rowid FROM market_events s \
    JOIN market_events l \
      ON l.tx_hash = s.tx_hash \
     AND l.policy_id = s.policy_id \
     AND l.asset_name_hex = s.asset_name_hex \
     AND l.kind = 'listed' \
     AND l.marketplace <> s.marketplace \
    WHERE s.kind = 'sold'";

#[derive(clap::Args, Debug)]
pub struct PruneArgs {
    /// Ledger sqlite path.
    #[arg(long, default_value = "market-ledger.db")]
    db: PathBuf,

    /// Actually delete. Without this flag, prune only reports what it would do.
    #[arg(long)]
    yes: bool,
}

pub fn run(args: PruneArgs) -> Result<()> {
    let conn = Connection::open(&args.db)
        .with_context(|| format!("opening ledger {}", args.db.display()))?;

    let matched = count_phantoms(&conn)?;
    println!("phantom cross-venue `sold` rows: {matched}");
    if matched == 0 {
        println!("nothing to prune.");
        return Ok(());
    }

    print_sample(&conn)?;

    if !args.yes {
        println!("\ndry-run — re-run with --yes to delete these {matched} rows.");
        return Ok(());
    }

    let deleted = delete_phantoms(&conn)?;
    println!("\ndeleted {deleted} phantom `sold` rows.");
    Ok(())
}

/// How many rows the pruning predicate matches.
fn count_phantoms(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row(&format!("SELECT COUNT(*) FROM ({PHANTOM_ROWIDS})"), [], |r| {
        r.get(0)
    })?)
}

/// Delete the matched rows; returns the number removed.
fn delete_phantoms(conn: &Connection) -> Result<usize> {
    Ok(conn.execute(
        &format!("DELETE FROM market_events WHERE rowid IN ({PHANTOM_ROWIDS})"),
        [],
    )?)
}

/// Print the newest 10 matches as `origin → destination  tx  policy.asset`.
fn print_sample(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT s.tx_hash, s.marketplace, l.marketplace, s.policy_id, s.asset_name_hex \
         FROM market_events s \
         JOIN market_events l \
           ON l.tx_hash = s.tx_hash AND l.policy_id = s.policy_id \
          AND l.asset_name_hex = s.asset_name_hex \
          AND l.kind = 'listed' AND l.marketplace <> s.marketplace \
         WHERE s.kind = 'sold' \
         ORDER BY s.slot DESC LIMIT 10",
    )?;
    println!("\nsample (newest 10)  origin → destination  tx  policy.asset:");
    for row in stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, String>(4)?,
        ))
    })? {
        let (tx, origin, dest, policy, asset) = row?;
        println!("  {origin} → {dest}  {tx}  {policy}.{asset}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal `market_events` fixture — just the columns the predicate reads.
    fn fixture() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE market_events (
                tx_hash TEXT, policy_id TEXT, asset_name_hex TEXT,
                kind TEXT, marketplace TEXT, slot INTEGER
            );",
        )
        .unwrap();
        conn
    }

    fn insert(conn: &Connection, tx: &str, kind: &str, mp: &str, slot: i64) {
        conn.execute(
            "INSERT INTO market_events (tx_hash, policy_id, asset_name_hex, kind, marketplace, slot)
             VALUES (?, 'pol', 'asset', ?, ?, ?)",
            rusqlite::params![tx, kind, mp, slot],
        )
        .unwrap();
    }

    #[test]
    fn prunes_only_cross_venue_migration_sales() {
        let conn = fixture();
        // Migration tx: phantom jpg `sold` + genuine wayup `listed`, same asset.
        insert(&conn, "mig", "sold", "jpg.store", 100);
        insert(&conn, "mig", "listed", "wayup", 100);
        // Genuine jpg sale: `sold` with NO co-located cross-venue `listed`.
        insert(&conn, "realsale", "sold", "jpg.store", 101);
        // A plain listing on its own — must survive untouched.
        insert(&conn, "plainlist", "listed", "wayup", 102);

        assert_eq!(count_phantoms(&conn).unwrap(), 1);
        assert_eq!(delete_phantoms(&conn).unwrap(), 1);

        // The genuine sale, the real listing, and the migration's listed row remain.
        let remaining: i64 = conn
            .query_row("SELECT COUNT(*) FROM market_events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(remaining, 3);
        let phantom_gone: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM market_events WHERE tx_hash='mig' AND kind='sold'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(phantom_gone, 0);
        let real_sale_kept: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM market_events WHERE tx_hash='realsale' AND kind='sold'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(real_sale_kept, 1);
    }

    #[test]
    fn same_venue_relist_is_not_pruned() {
        let conn = fixture();
        // A same-venue price update: `sold` + `listed` at the SAME marketplace
        // must NOT match (that's not a cross-venue migration, and shouldn't
        // produce a phantom sold in the first place — but guard the predicate).
        insert(&conn, "update", "sold", "wayup", 200);
        insert(&conn, "update", "listed", "wayup", 200);
        assert_eq!(count_phantoms(&conn).unwrap(), 0);
    }
}
