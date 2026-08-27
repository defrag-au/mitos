//! The per-wallet cache — one sqlite db, WAL, single writer (the job worker),
//! read-only connections per request (the market-ledger serve arrangement).
//!
//! What is persisted per canonical target: the flow rows (with resolved
//! senders serialized as JSON text), the still-held outref set (the seed an
//! incremental refresh continues classification from), and a cursor row.
//! Everything here is derived and rebuildable from a cold re-excavation —
//! the chain stays the only source of truth.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use mitos_chain_walk::slot_to_unix;
use rusqlite::{Connection, OpenFlags, params};

use crate::report;

pub fn open_rw(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS wallet (
             target TEXT PRIMARY KEY,
             display TEXT NOT NULL,
             scanned_to_chunk INTEGER NOT NULL,
             first_slot INTEGER,
             last_slot INTEGER,
             updated_unix INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS flow (
             target TEXT NOT NULL,
             slot INTEGER NOT NULL,
             tx_idx INTEGER NOT NULL,
             tx_hash BLOB NOT NULL,
             kind TEXT NOT NULL,
             lovelace_in INTEGER NOT NULL,
             lovelace_out INTEGER NOT NULL,
             assets_in INTEGER NOT NULL,
             assets_out INTEGER NOT NULL,
             senders TEXT,
             recipients TEXT,
             PRIMARY KEY (target, tx_hash)
         );
         CREATE INDEX IF NOT EXISTS flow_target_slot ON flow(target, slot DESC);
         CREATE TABLE IF NOT EXISTS owned (
             target TEXT NOT NULL,
             tx_hash BLOB NOT NULL,
             idx INTEGER NOT NULL,
             lovelace INTEGER NOT NULL,
             assets INTEGER NOT NULL,
             PRIMARY KEY (target, tx_hash, idx)
         );",
    )?;
    // CREATE IF NOT EXISTS never adds a column to an existing table — the
    // banked sqlite trap. Idempotent ALTER; "duplicate column" is fine.
    if let Err(e) = conn.execute("ALTER TABLE flow ADD COLUMN recipients TEXT", []) {
        let msg = e.to_string();
        if !msg.contains("duplicate column") {
            return Err(e).context("adding flow.recipients");
        }
    }
    if let Err(e) = conn.execute("ALTER TABLE wallet ADD COLUMN scanned_to_slot INTEGER", []) {
        let msg = e.to_string();
        if !msg.contains("duplicate column") {
            return Err(e).context("adding wallet.scanned_to_slot");
        }
    }
    if let Err(e) = conn.execute("ALTER TABLE flow ADD COLUMN assets TEXT", []) {
        let msg = e.to_string();
        if !msg.contains("duplicate column") {
            return Err(e).context("adding flow.assets");
        }
    }
    if let Err(e) = conn.execute("ALTER TABLE owned ADD COLUMN units TEXT", []) {
        let msg = e.to_string();
        if !msg.contains("duplicate column") {
            return Err(e).context("adding owned.units");
        }
    }
    if let Err(e) = conn.execute("ALTER TABLE flow ADD COLUMN market TEXT", []) {
        let msg = e.to_string();
        if !msg.contains("duplicate column") {
            return Err(e).context("adding flow.market");
        }
    }
    if let Err(e) = conn.execute("ALTER TABLE wallet ADD COLUMN deep_pending INTEGER", []) {
        let msg = e.to_string();
        if !msg.contains("duplicate column") {
            return Err(e).context("adding wallet.deep_pending");
        }
    }
    Ok(conn)
}

/// Stored rows that have never been market-checked, newest first.
///
/// Enrichment works off the CACHE, not off the batch that just classified:
/// an incremental run produces few (often zero) new txs, and a wallet's
/// existing rows would otherwise never be labelled at all. Capped so a deep
/// history backfills across runs instead of stalling one.
pub fn hashes_needing_market(
    conn: &Connection,
    canonical: &str,
    limit: u32,
) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT tx_hash FROM flow
         WHERE target = ?1 AND market IS NULL
         ORDER BY slot DESC LIMIT ?2",
    )?;
    let mut rows = stmt.query(params![canonical, limit])?;
    let mut out = Vec::new();
    while let Some(r) = rows.next()? {
        out.push(hex::encode(r.get::<_, Vec<u8>>(0)?));
    }
    Ok(out)
}

/// Stitch market-ledger verdicts onto stored rows (the enrichment pass, like
/// [`update_senders`] — rows are already live, this only names them).
pub fn update_market(
    conn: &Connection,
    canonical: &str,
    found: &HashMap<String, crate::market::MarketEvent>,
) -> Result<usize> {
    let mut stmt =
        conn.prepare("UPDATE flow SET market = ?1 WHERE target = ?2 AND tx_hash = ?3")?;
    let mut n = 0;
    for (tx, event) in found {
        let hash = match hex::decode(tx) {
            Ok(h) => h,
            Err(_) => continue,
        };
        n += stmt.execute(params![serde_json::to_string(event)?, canonical, hash])?;
    }
    Ok(n)
}

pub fn open_ro(path: &Path) -> Result<Connection> {
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("opening {} read-only", path.display()))
}

pub struct WalletMeta {
    pub scanned_to_chunk: u64,
    /// Slot-granular cursor (tail-aware). `None` on legacy rows.
    pub scanned_to_slot: Option<u64>,
    /// The deep backfill below the initial window hasn't run yet.
    pub deep_pending: bool,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
    pub updated_unix: u64,
}

pub fn load_wallet(conn: &Connection, canonical: &str) -> Result<Option<WalletMeta>> {
    let mut stmt = conn.prepare(
        "SELECT scanned_to_chunk, scanned_to_slot, first_slot, last_slot, updated_unix,
                COALESCE(deep_pending, 0)
         FROM wallet WHERE target = ?1",
    )?;
    let mut rows = stmt.query(params![canonical])?;
    match rows.next()? {
        Some(r) => Ok(Some(WalletMeta {
            scanned_to_chunk: r.get(0)?,
            scanned_to_slot: r.get(1)?,
            first_slot: r.get(2)?,
            last_slot: r.get(3)?,
            updated_unix: r.get(4)?,
            deep_pending: r.get::<_, i64>(5)? != 0,
        })),
        None => Ok(None),
    }
}

pub fn load_owned(conn: &Connection, canonical: &str) -> Result<crate::classify::OwnedSet> {
    let mut stmt =
        conn.prepare("SELECT tx_hash, idx, lovelace, units FROM owned WHERE target = ?1")?;
    let mut out = HashMap::new();
    let mut rows = stmt.query(params![canonical])?;
    while let Some(r) = rows.next()? {
        let hash: Vec<u8> = r.get(0)?;
        let mut h = [0u8; 32];
        h.copy_from_slice(&hash);
        // Pre-units rows carry no identities; they still net correctly on
        // lovelace, they just can't name what a spend gave up.
        let units: Vec<crate::scan::AssetUnit> = match r.get::<_, Option<String>>(3)? {
            Some(j) => serde_json::from_str(&j).context("parsing owned units")?,
            None => Vec::new(),
        };
        out.insert((h, r.get::<_, u32>(1)?), (r.get::<_, u64>(2)?, units));
    }
    Ok(out)
}

/// How a store behaves within progressive excavation.
#[derive(Clone, Copy, Default)]
pub struct StoreOpts {
    /// Overwrite existing rows (the deep pass re-states what it now knows).
    pub replace: bool,
    /// History below the scanned window is still missing.
    pub deep_pending: bool,
}

/// Persist one wallet's excavation result. Returns the number of NEW flow
/// rows. `sources` may be empty (early emit) — senders back-fill later via
/// [`update_senders`].
#[allow(clippy::too_many_arguments)]
pub fn store_timeline(
    conn: &mut Connection,
    canonical: &str,
    display: &str,
    timeline: &crate::classify::Timeline,
    sources: &HashMap<([u8; 32], u32), (String, u64)>,
    scanned_to_chunk: u64,
    scanned_to_slot: u64,
    now_unix: u64,
    opts: StoreOpts,
) -> Result<usize> {
    let tx = conn.transaction()?;
    let mut inserted = 0usize;
    {
        // The deep pass RE-classifies history it now understands (a spend of
        // a pre-window UTxO reads as a receive until that UTxO is known), so
        // it must overwrite rather than skip.
        let verb = if opts.replace {
            "INSERT OR REPLACE"
        } else {
            "INSERT OR IGNORE"
        };
        let mut ins = tx.prepare(&format!(
            "{verb} INTO flow
                 (target, slot, tx_idx, tx_hash, kind, lovelace_in, lovelace_out,
                  assets_in, assets_out, senders, recipients, assets)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)"
        ))?;
        for (i, t) in timeline.txs.iter().enumerate() {
            let row = report::row_for(t, sources);
            let senders = row
                .senders
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?;
            let recipients = row
                .recipients
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?;
            let assets = row.assets.as_ref().map(serde_json::to_string).transpose()?;
            inserted += ins.execute(params![
                canonical,
                t.slot,
                i as u32,
                t.hash.as_slice(),
                row.kind,
                t.lovelace_in,
                t.lovelace_out,
                t.assets_in,
                t.assets_out,
                senders,
                recipients,
                assets,
            ])?;
        }
        tx.execute("DELETE FROM owned WHERE target = ?1", params![canonical])?;
        let mut own = tx.prepare(
            "INSERT INTO owned (target, tx_hash, idx, lovelace, assets, units)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )?;
        for ((hash, idx), (lovelace, units)) in &timeline.owned {
            own.execute(params![
                canonical,
                hash.as_slice(),
                idx,
                lovelace,
                units.len() as u32,
                serde_json::to_string(units)?,
            ])?;
        }
        let (new_first, new_last) = if timeline.txs.is_empty() {
            (None, None)
        } else {
            (Some(timeline.first_slot), Some(timeline.last_slot))
        };
        tx.execute(
            "INSERT INTO wallet (target, display, scanned_to_chunk, scanned_to_slot, first_slot,
                                 last_slot, updated_unix, deep_pending)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(target) DO UPDATE SET
                 scanned_to_chunk = excluded.scanned_to_chunk,
                 scanned_to_slot = excluded.scanned_to_slot,
                 deep_pending = excluded.deep_pending,
                 first_slot = COALESCE(MIN(wallet.first_slot, excluded.first_slot),
                                        wallet.first_slot, excluded.first_slot),
                 last_slot = COALESCE(MAX(wallet.last_slot, excluded.last_slot),
                                       wallet.last_slot, excluded.last_slot),
                 updated_unix = excluded.updated_unix",
            params![
                canonical,
                display,
                scanned_to_chunk,
                scanned_to_slot,
                new_first,
                new_last,
                now_unix,
                opts.deep_pending as i32,
            ],
        )?;
    }
    tx.commit()?;
    Ok(inserted)
}

/// Back-fill sender JSON onto already-stored rows (the early-emit follow-up).
pub fn update_senders(
    conn: &Connection,
    canonical: &str,
    timeline: &crate::classify::Timeline,
    sources: &HashMap<([u8; 32], u32), (String, u64)>,
) -> Result<()> {
    let mut stmt =
        conn.prepare("UPDATE flow SET senders = ?1 WHERE target = ?2 AND tx_hash = ?3")?;
    for t in &timeline.txs {
        let row = report::row_for(t, sources);
        if let Some(senders) = &row.senders {
            stmt.execute(params![
                serde_json::to_string(senders)?,
                canonical,
                t.hash.as_slice()
            ])?;
        }
    }
    Ok(())
}

pub fn count_flows(conn: &Connection, canonical: &str) -> Result<u64> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM flow WHERE target = ?1",
        params![canonical],
        |r| r.get(0),
    )?)
}

/// Every cached wallet: (canonical, display) — the batch-refresh roster.
pub fn list_wallets(conn: &Connection) -> Result<Vec<(String, String)>> {
    let mut stmt = conn.prepare("SELECT target, display FROM wallet")?;
    let mut out = Vec::new();
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        out.push((r.get(0)?, r.get(1)?));
    }
    Ok(out)
}

pub fn wallet_count(conn: &Connection) -> Result<u64> {
    Ok(conn.query_row("SELECT COUNT(*) FROM wallet", [], |r| r.get(0))?)
}

/// Newest-first rows, optionally strictly below `before_slot` (pagination).
pub fn query_rows(
    conn: &Connection,
    canonical: &str,
    limit: u32,
    before_slot: Option<u64>,
) -> Result<Vec<report::Row>> {
    let mut stmt = conn.prepare(
        "SELECT slot, tx_hash, kind, lovelace_in, lovelace_out, assets_in, assets_out, senders,
                recipients, assets, market
         FROM flow
         WHERE target = ?1 AND (?2 IS NULL OR slot < ?2)
         ORDER BY slot DESC, tx_idx DESC
         LIMIT ?3",
    )?;
    let mut out = Vec::new();
    let mut rows = stmt.query(params![canonical, before_slot, limit])?;
    while let Some(r) = rows.next()? {
        let slot: u64 = r.get(0)?;
        let hash: Vec<u8> = r.get(1)?;
        let (lovelace_in, lovelace_out): (u64, u64) = (r.get(3)?, r.get(4)?);
        let senders: Option<String> = r.get(7)?;
        let senders: Option<Vec<report::Sender>> = match senders {
            Some(s) => Some(serde_json::from_str(&s).context("parsing stored senders")?),
            None => None,
        };
        let recipients: Option<String> = r.get(8)?;
        let recipients: Option<Vec<report::Sender>> = match recipients {
            Some(s) => Some(serde_json::from_str(&s).context("parsing stored recipients")?),
            None => None,
        };
        let assets: Option<String> = r.get(9)?;
        let assets: Option<Vec<report::AssetEntry>> = match assets {
            Some(s) => Some(serde_json::from_str(&s).context("parsing stored assets")?),
            None => None,
        };
        let market: Option<String> = r.get(10)?;
        let market: Option<crate::market::MarketEvent> = match market {
            Some(s) => serde_json::from_str(&s).ok(),
            None => None,
        };
        out.push(report::Row {
            kind: r.get(2)?,
            slot,
            time: report::fmt_unix(slot_to_unix(slot)),
            tx: hex::encode(hash),
            lovelace_in,
            lovelace_out,
            net_lovelace: lovelace_in as i64 - lovelace_out as i64,
            assets_in: r.get(5)?,
            assets_out: r.get(6)?,
            senders,
            recipients,
            assets,
            market,
        });
    }
    Ok(out)
}
