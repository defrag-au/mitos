//! The sqlite operational ledger.
//!
//! `market_events` mirrors D1 `0005-market-events.sql` (plus `venue`), so the
//! ledger / D1 upload / Parquet share one schema. `walk_cursor` (per venue) and
//! `outref_buffer` (open-book snapshot) make walks resumable: a checkpoint
//! writes both so a resume/rebootstrap reloads the open book and never re-walks.
//! Single writer (this binary), WAL mode. Append-only via `INSERT OR IGNORE`.

use std::path::Path;

use anyhow::{Context, Result};
use pallas_primitives::Hash;
use rusqlite::{Connection, params};

use crate::buffer::{BufferedOutput, OutrefBuffer};
use crate::row::MarketEventRow;
use mitos_chain_walk::decode::Asset;

pub(crate) const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS market_events (
    tx_hash              TEXT    NOT NULL,
    policy_id            TEXT    NOT NULL,
    asset_name_hex       TEXT    NOT NULL,
    fingerprint          TEXT,
    kind                 TEXT    NOT NULL,
    price_lovelace       INTEGER,
    buyer_price_lovelace INTEGER,
    seller_stake         TEXT,
    buyer_stake          TEXT,
    marketplace          TEXT    NOT NULL,
    bundle_size          INTEGER,
    output_index         INTEGER,
    fee_waived           INTEGER NOT NULL DEFAULT 0,
    slot                 INTEGER NOT NULL,
    block_height         INTEGER,
    block_time           INTEGER NOT NULL,
    source               TEXT    NOT NULL,
    venue                TEXT    NOT NULL,
    PRIMARY KEY (tx_hash, policy_id, asset_name_hex, kind)
);
CREATE INDEX IF NOT EXISTS idx_me_policy_kind_slot ON market_events(policy_id, kind, slot);
CREATE INDEX IF NOT EXISTS idx_me_fingerprint_slot ON market_events(fingerprint, slot);
CREATE INDEX IF NOT EXISTS idx_me_venue_slot ON market_events(venue, slot);

CREATE TABLE IF NOT EXISTS walk_cursor (
    venue        TEXT    PRIMARY KEY,
    slot         INTEGER NOT NULL,
    block_hash   BLOB    NOT NULL
);

CREATE TABLE IF NOT EXISTS outref_buffer (
    tx_hash       BLOB    NOT NULL,
    output_index  INTEGER NOT NULL,
    address       TEXT    NOT NULL,
    lovelace      INTEGER NOT NULL,
    assets        TEXT    NOT NULL,
    datum_bytes   BLOB,
    datum_hash    BLOB,
    venue         TEXT    NOT NULL,
    PRIMARY KEY (tx_hash, output_index)
);

CREATE TABLE IF NOT EXISTS volatile_blocks (
    slot         INTEGER NOT NULL,
    hash         BLOB    NOT NULL,
    height       INTEGER,
    cbor         BLOB    NOT NULL,
    PRIMARY KEY (slot, hash)
);

CREATE TABLE IF NOT EXISTS partition_manifest (
    venue        TEXT    NOT NULL,
    year         INTEGER NOT NULL,
    month        INTEGER NOT NULL,
    sealed_slot  INTEGER NOT NULL,
    rows         INTEGER NOT NULL,
    PRIMARY KEY (venue, year, month)
);

-- Current live listings — a query-ready projection of the open book's listing
-- subset with the price DECODED (buffer-driven; see MARKET_LEDGER_LISTINGS.md).
-- One active listing per (policy, asset): the asset is escrowed in the listing
-- script, so it can be in at most one listing UTxO at a time. `price_lovelace`
-- is NULL only for the rare jpg listing whose datum isn't locally resolvable.
CREATE TABLE IF NOT EXISTS listings (
    policy_id       TEXT    NOT NULL,
    asset_name_hex  TEXT    NOT NULL,
    venue           TEXT    NOT NULL,
    price_lovelace  INTEGER,
    seller_stake    TEXT,
    tx_hash         TEXT    NOT NULL,
    output_index    INTEGER NOT NULL,
    listed_slot     INTEGER NOT NULL,
    listed_time     INTEGER NOT NULL,
    PRIMARY KEY (policy_id, asset_name_hex)
);
CREATE INDEX IF NOT EXISTS idx_listings_policy ON listings(policy_id, price_lovelace);

-- Persistent datum retention: content-addressed hash → CBOR bytes. Written
-- through at resolve time (produce) and when a spend finally reveals a
-- previously hash-only preimage — retaining datums the moving `outref_buffer`
-- evicts on spend, so history can be re-decoded without a re-walk and a hash is
-- never re-resolved. Append-only (content-addressed ⇒ immutable), so writes use
-- INSERT OR IGNORE and NEVER go through the checkpoint wipe/rewrite path.
CREATE TABLE IF NOT EXISTS datum_cache (
    datum_hash  BLOB PRIMARY KEY,
    datum_bytes BLOB NOT NULL
);
";

pub struct Ledger {
    conn: Connection,
}

/// A raw block held in the volatile window (follow mode). The stored `height`
/// column is observability-only — replay recomputes it from the block itself.
pub struct VolatileBlock {
    pub slot: u64,
    pub hash: Vec<u8>,
    pub cbor: Vec<u8>,
}

/// A current live listing (the query-ready projection of the open book).
/// `price_lovelace` is `None` only for the rare jpg listing whose datum isn't
/// locally resolvable.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Listing {
    pub policy_id: String,
    pub asset_name_hex: String,
    pub venue: String,
    pub price_lovelace: Option<u64>,
    pub seller_stake: Option<String>,
    pub tx_hash: String,
    pub output_index: u32,
    pub listed_slot: u64,
    pub listed_time: i64,
}

/// A buffer-driven mutation of the `listings` projection, produced by
/// `process_tx` as watched Sale-channel outputs are buffered (`Upsert`) or
/// spent (`Delete`). Deletes precede upserts within a tx, so a reprice
/// (delete old UTxO + upsert new UTxO, same asset) nets to the new listing.
pub(crate) enum ListingOp {
    Upsert(Listing),
    Delete {
        policy_id: String,
        asset_name_hex: String,
    },
}

impl Ledger {
    pub fn open(path: &Path) -> Result<Self> {
        let conn =
            Connection::open(path).with_context(|| format!("opening ledger {}", path.display()))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
            .context("setting sqlite pragmas")?;
        conn.execute_batch(SCHEMA)
            .context("creating ledger schema")?;
        Ok(Self { conn })
    }

    /// Append events (idempotent — `INSERT OR IGNORE` on the PK). Returns the
    /// number of rows actually inserted.
    pub fn insert_events(&mut self, rows: &[MarketEventRow]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let tx = self.conn.transaction()?;
        let mut inserted = 0usize;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO market_events (
                    tx_hash, policy_id, asset_name_hex, fingerprint, kind,
                    price_lovelace, buyer_price_lovelace, seller_stake, buyer_stake,
                    marketplace, bundle_size, output_index, fee_waived,
                    slot, block_height, block_time, source, venue
                 ) VALUES (?,?,?,?,?, ?,?,?,?, ?,?,?,?, ?,?,?,?,?)",
            )?;
            for r in rows {
                inserted += stmt.execute(params![
                    r.tx_hash,
                    r.policy_id,
                    r.asset_name_hex,
                    r.fingerprint,
                    r.kind,
                    r.price_lovelace.map(u64_i64),
                    r.buyer_price_lovelace.map(u64_i64),
                    r.seller_stake,
                    r.buyer_stake,
                    r.marketplace,
                    r.bundle_size,
                    r.output_index,
                    r.fee_waived as i64,
                    u64_i64(r.slot),
                    r.block_height.map(u64_i64),
                    u64_i64(r.block_time),
                    "walk",
                    r.venue,
                ])?;
            }
        }
        tx.commit()?;
        Ok(inserted)
    }

    /// Write resolved datums through to the persistent cache. Content-addressed
    /// and immutable, so `INSERT OR IGNORE` is idempotent (replay-safe) and
    /// never overwrites — a hash's bytes never change. Returns newly-inserted
    /// count. Never called from `checkpoint` (which wipes+rewrites its tables).
    pub fn insert_datums(&mut self, entries: &[(Hash<32>, Vec<u8>)]) -> Result<usize> {
        if entries.is_empty() {
            return Ok(0);
        }
        let tx = self.conn.transaction()?;
        let mut inserted = 0usize;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO datum_cache (datum_hash, datum_bytes) VALUES (?,?)",
            )?;
            for (hash, bytes) in entries {
                inserted += stmt.execute(params![hash.as_ref(), bytes])?;
            }
        }
        tx.commit()?;
        Ok(inserted)
    }

    /// Look up a datum's CBOR bytes by hash (the read side of the cache — the
    /// final fallback in the LOCAL-FIRST resolution chain).
    pub fn get_datum(&self, hash: &Hash<32>) -> Result<Option<Vec<u8>>> {
        Ok(self
            .conn
            .query_row(
                "SELECT datum_bytes FROM datum_cache WHERE datum_hash = ?",
                [hash.as_ref()],
                |r| r.get::<_, Vec<u8>>(0),
            )
            .ok())
    }

    /// Persist the open-book snapshot + per-venue cursor in one transaction — a
    /// resumable checkpoint. The buffer is rewritten wholesale (it's small).
    pub fn checkpoint(
        &mut self,
        buffer: &OutrefBuffer,
        venues: &[&str],
        slot: u64,
        block_hash: &[u8],
    ) -> Result<()> {
        let tx = self.conn.transaction()?;
        for venue in venues {
            tx.execute(
                "INSERT INTO walk_cursor (venue, slot, block_hash) VALUES (?,?,?)
                 ON CONFLICT(venue) DO UPDATE SET slot=excluded.slot, block_hash=excluded.block_hash",
                params![venue, u64_i64(slot), block_hash],
            )?;
        }
        tx.execute("DELETE FROM outref_buffer", [])?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO outref_buffer
                    (tx_hash, output_index, address, lovelace, assets, datum_bytes, datum_hash, venue)
                 VALUES (?,?,?,?,?,?,?,?)",
            )?;
            for (oref, out) in buffer.entries() {
                stmt.execute(params![
                    oref.0.as_ref(),
                    oref.1,
                    out.address,
                    u64_i64(out.lovelace),
                    encode_assets(&out.assets),
                    out.datum_bytes,
                    out.datum_hash.map(|h| h.as_ref().to_vec()),
                    out.venue,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Lowest per-venue cursor slot over `venues` (a venue with no cursor is
    /// absent, so resume falls back to its `earliest_slot`). `None` if none have
    /// a cursor yet (cold start).
    pub fn min_cursor_slot(&self, venues: &[&str]) -> Result<Option<u64>> {
        let mut min: Option<u64> = None;
        for venue in venues {
            let slot: Option<i64> = self
                .conn
                .query_row(
                    "SELECT slot FROM walk_cursor WHERE venue = ?",
                    [venue],
                    |r| r.get(0),
                )
                .ok();
            if let Some(s) = slot {
                let s = s as u64;
                min = Some(min.map_or(s, |m| m.min(s)));
            }
        }
        Ok(min)
    }

    /// The persisted cursor as an intersect-able point: the (slot, block_hash)
    /// of the LOWEST venue cursor over `venues`, skipping cursors persisted
    /// with an empty hash. `None` if no venue has a usable cursor.
    pub fn cursor_point(&self, venues: &[&str]) -> Result<Option<(u64, Vec<u8>)>> {
        let mut min: Option<(u64, Vec<u8>)> = None;
        for venue in venues {
            let row: Option<(i64, Vec<u8>)> = self
                .conn
                .query_row(
                    "SELECT slot, block_hash FROM walk_cursor WHERE venue = ?",
                    [venue],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .ok();
            if let Some((slot, hash)) = row
                && hash.len() == 32
                && min.as_ref().is_none_or(|(m, _)| (slot as u64) < *m)
            {
                min = Some((slot as u64, hash));
            }
        }
        Ok(min)
    }

    // --- volatile window (follow mode) ---------------------------------------
    //
    // Raw block CBOR for the un-settled tail (≤ k blocks past the boundary
    // checkpoint). On rollback: truncate events + volatile past the point,
    // rebuild the live buffer from the boundary checkpoint + a replay of the
    // surviving volatile blocks.

    pub fn insert_volatile(
        &mut self,
        slot: u64,
        hash: &[u8],
        height: Option<u64>,
        cbor: &[u8],
    ) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO volatile_blocks (slot, hash, height, cbor) VALUES (?,?,?,?)",
            params![u64_i64(slot), hash, height.map(u64_i64), cbor],
        )?;
        Ok(())
    }

    pub fn volatile_count(&self) -> Result<u64> {
        let n: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM volatile_blocks", [], |r| r.get(0))?;
        Ok(n as u64)
    }

    /// Volatile blocks with `slot > after`, oldest first.
    pub fn volatile_after(&self, after: u64) -> Result<Vec<VolatileBlock>> {
        let mut stmt = self
            .conn
            .prepare("SELECT slot, hash, cbor FROM volatile_blocks WHERE slot > ? ORDER BY slot")?;
        let rows = stmt.query_map([u64_i64(after)], |r| {
            Ok(VolatileBlock {
                slot: r.get::<_, i64>(0)? as u64,
                hash: r.get(1)?,
                cbor: r.get(2)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    /// Oldest volatile block (the next boundary-advance candidate).
    /// The oldest `limit` volatile blocks (slot ASC) — a boundary-advance batch.
    pub fn volatile_oldest(&self, limit: u64) -> Result<Vec<VolatileBlock>> {
        let mut stmt = self
            .conn
            .prepare("SELECT slot, hash, cbor FROM volatile_blocks ORDER BY slot LIMIT ?")?;
        let rows = stmt.query_map([limit as i64], |r| {
            Ok(VolatileBlock {
                slot: r.get::<_, i64>(0)? as u64,
                hash: r.get(1)?,
                cbor: r.get(2)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    // --- listings (current-listings projection) ---------------------------------

    /// Wipe the listings projection — the first step of an authoritative
    /// rebuild from the open book (seed / post-rollback).
    pub fn clear_listings(&mut self) -> Result<()> {
        self.conn.execute("DELETE FROM listings", [])?;
        Ok(())
    }

    /// Apply buffer-driven listing ops in order (see [`ListingOp`]).
    pub fn apply_listing_ops(&mut self, ops: &[ListingOp]) -> Result<()> {
        for op in ops {
            match op {
                ListingOp::Upsert(l) => self.upsert_listing(l)?,
                ListingOp::Delete {
                    policy_id,
                    asset_name_hex,
                } => self.delete_listing(policy_id, asset_name_hex)?,
            }
        }
        Ok(())
    }

    /// Upsert a listing (create or reprice) — in-place on the `(policy, asset)`
    /// PK, so a reprice replaces the row (new price + current UTxO).
    pub fn upsert_listing(&mut self, l: &Listing) -> Result<()> {
        self.conn.execute(
            "INSERT INTO listings (policy_id, asset_name_hex, venue, price_lovelace,
                 seller_stake, tx_hash, output_index, listed_slot, listed_time)
             VALUES (?,?,?,?,?,?,?,?,?)
             ON CONFLICT(policy_id, asset_name_hex) DO UPDATE SET
                 venue=excluded.venue, price_lovelace=excluded.price_lovelace,
                 seller_stake=excluded.seller_stake, tx_hash=excluded.tx_hash,
                 output_index=excluded.output_index, listed_slot=excluded.listed_slot,
                 listed_time=excluded.listed_time",
            params![
                l.policy_id,
                l.asset_name_hex,
                l.venue,
                l.price_lovelace.map(u64_i64),
                l.seller_stake,
                l.tx_hash,
                l.output_index,
                u64_i64(l.listed_slot),
                l.listed_time,
            ],
        )?;
        Ok(())
    }

    /// Remove a listing (delist / sale / buffer-evict).
    pub fn delete_listing(&mut self, policy_id: &str, asset_name_hex: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM listings WHERE policy_id=? AND asset_name_hex=?",
            params![policy_id, asset_name_hex],
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    /// Current listings for a policy, cheapest-first (unpriced last), for
    /// `/listings`. The `LIMIT` caps a page.
    pub fn listings_for_policy(&self, policy_id: &str, limit: u32) -> Result<Vec<Listing>> {
        let mut stmt = self.conn.prepare(
            "SELECT policy_id, asset_name_hex, venue, price_lovelace, seller_stake,
                    tx_hash, output_index, listed_slot, listed_time
             FROM listings WHERE policy_id=?
             ORDER BY price_lovelace IS NULL, price_lovelace ASC LIMIT ?",
        )?;
        let rows = stmt.query_map(params![policy_id, limit], |r| {
            Ok(Listing {
                policy_id: r.get(0)?,
                asset_name_hex: r.get(1)?,
                venue: r.get(2)?,
                price_lovelace: r.get::<_, Option<i64>>(3)?.map(|v| v as u64),
                seller_stake: r.get(4)?,
                tx_hash: r.get(5)?,
                output_index: r.get::<_, i64>(6)? as u32,
                listed_slot: r.get::<_, i64>(7)? as u64,
                listed_time: r.get(8)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    #[allow(dead_code)]
    /// `(count, floor_lovelace)` for a policy — the `/listings` page header.
    /// Floor is the min over priced listings; `None` if none are priced.
    pub fn listing_summary(&self, policy_id: &str) -> Result<(u64, Option<u64>)> {
        self.conn
            .query_row(
                "SELECT COUNT(*), MIN(price_lovelace) FROM listings WHERE policy_id=?",
                [policy_id],
                |r| {
                    let count: i64 = r.get(0)?;
                    let floor: Option<i64> = r.get(1)?;
                    Ok((count as u64, floor.map(|v| v as u64)))
                },
            )
            .map_err(Into::into)
    }

    /// Drop volatile blocks with `slot <= upto` (after a boundary advance —
    /// also the startup sweep for rows orphaned by a crash mid-advance).
    pub fn delete_volatile_upto(&mut self, upto: u64) -> Result<usize> {
        Ok(self.conn.execute(
            "DELETE FROM volatile_blocks WHERE slot <= ?",
            [u64_i64(upto)],
        )?)
    }

    /// Drop volatile blocks past a rollback point.
    pub fn delete_volatile_after(&mut self, after: u64) -> Result<usize> {
        Ok(self.conn.execute(
            "DELETE FROM volatile_blocks WHERE slot > ?",
            [u64_i64(after)],
        )?)
    }

    /// Drop events past a rollback point. Follow-mode only: every row with
    /// `slot > point` necessarily came from a rolled-back volatile block.
    pub fn delete_events_after(&mut self, after: u64) -> Result<usize> {
        Ok(self
            .conn
            .execute("DELETE FROM market_events WHERE slot > ?", [u64_i64(after)])?)
    }

    /// Reload the open-book buffer from the last checkpoint.
    pub fn load_buffer(&self) -> Result<OutrefBuffer> {
        let mut buffer = OutrefBuffer::default();
        let mut stmt = self.conn.prepare(
            "SELECT tx_hash, output_index, address, lovelace, assets, datum_bytes, datum_hash, venue
             FROM outref_buffer",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, i64>(1)? as u32,
                r.get::<_, String>(2)?,
                r.get::<_, i64>(3)? as u64,
                r.get::<_, String>(4)?,
                r.get::<_, Option<Vec<u8>>>(5)?,
                r.get::<_, Option<Vec<u8>>>(6)?,
                r.get::<_, String>(7)?,
            ))
        })?;
        for row in rows {
            let (tx_hash, index, address, lovelace, assets, datum_bytes, datum_hash, venue) = row?;
            let oref = (hash32(&tx_hash)?, index);
            buffer.insert(
                oref,
                BufferedOutput {
                    address,
                    lovelace,
                    assets: decode_assets(&assets)?,
                    datum_bytes,
                    datum_hash: datum_hash.as_deref().map(hash32).transpose()?,
                    venue,
                },
            );
        }
        Ok(buffer)
    }
}

fn u64_i64(v: u64) -> i64 {
    v as i64
}

fn hash32(bytes: &[u8]) -> Result<Hash<32>> {
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("expected 32-byte hash, got {}", bytes.len()))?;
    Ok(Hash::from(arr))
}

/// Assets as a JSON array of `[policy_hex, name_hex]` pairs.
fn encode_assets(assets: &[Asset]) -> String {
    let pairs: Vec<[String; 2]> = assets
        .iter()
        .map(|a| [hex::encode(&a.policy), hex::encode(&a.name)])
        .collect();
    serde_json::to_string(&pairs).unwrap_or_else(|_| "[]".into())
}

fn decode_assets(s: &str) -> Result<Vec<Asset>> {
    let pairs: Vec<[String; 2]> = serde_json::from_str(s).context("decoding buffered assets")?;
    pairs
        .into_iter()
        .map(|[p, n]| {
            Ok(Asset {
                policy: hex::decode(&p).context("asset policy hex")?,
                name: hex::decode(&n).context("asset name hex")?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sold_row() -> MarketEventRow {
        MarketEventRow {
            tx_hash: "aa".into(),
            policy_id: "pp".into(),
            asset_name_hex: "nn".into(),
            fingerprint: None,
            kind: "sold".into(),
            price_lovelace: Some(1_000),
            buyer_price_lovelace: Some(1_020),
            seller_stake: None,
            buyer_stake: None,
            marketplace: "jpg.store".into(),
            bundle_size: None,
            output_index: None,
            fee_waived: false,
            slot: 5,
            block_height: Some(2),
            block_time: 10,
            venue: "jpg".into(),
        }
    }

    #[test]
    fn insert_is_idempotent() {
        let mut led = Ledger::open(Path::new(":memory:")).unwrap();
        assert_eq!(led.insert_events(&[sold_row()]).unwrap(), 1);
        // Same PK → INSERT OR IGNORE writes nothing.
        assert_eq!(led.insert_events(&[sold_row()]).unwrap(), 0);
    }

    #[test]
    fn buffer_and_cursor_roundtrip() {
        let mut led = Ledger::open(Path::new(":memory:")).unwrap();
        let mut buf = OutrefBuffer::default();
        let oref = (Hash::from([1u8; 32]), 3u32);
        buf.insert(
            oref,
            BufferedOutput {
                address: "addr1abc".into(),
                lovelace: 42,
                assets: vec![Asset {
                    policy: vec![9u8; 28],
                    name: b"Bud".to_vec(),
                }],
                datum_bytes: Some(vec![0xd8, 0x79, 0x80]),
                datum_hash: Some(Hash::from([2u8; 32])),
                venue: "jpg".into(),
            },
        );
        led.checkpoint(&buf, &["jpg", "wayup"], 5, &[7u8; 32])
            .unwrap();

        // Cursor recorded for both venues; min is the walk slot.
        assert_eq!(led.min_cursor_slot(&["jpg", "wayup"]).unwrap(), Some(5));
        assert_eq!(led.min_cursor_slot(&["absent"]).unwrap(), None);

        let mut reloaded = led.load_buffer().unwrap();
        assert_eq!(reloaded.len(), 1);
        let got = reloaded
            .take(&oref)
            .expect("buffered output survives reload");
        assert_eq!(got.address, "addr1abc");
        assert_eq!(got.lovelace, 42);
        assert_eq!(got.assets.len(), 1);
        assert_eq!(hex::encode(&got.assets[0].name), hex::encode(b"Bud"));
        assert_eq!(got.datum_bytes.as_deref(), Some(&[0xd8, 0x79, 0x80][..]));
        assert_eq!(got.datum_hash, Some(Hash::from([2u8; 32])));
        assert_eq!(got.venue, "jpg");
    }

    #[test]
    fn listings_upsert_reprice_delete_and_query() {
        let mut led = Ledger::open(Path::new(":memory:")).unwrap();
        let mk = |asset: &str, price: Option<u64>, slot: u64| Listing {
            policy_id: "p".into(),
            asset_name_hex: asset.into(),
            venue: "wayup".into(),
            price_lovelace: price,
            seller_stake: Some("stake1x".into()),
            tx_hash: "tx".into(),
            output_index: 0,
            listed_slot: slot,
            listed_time: slot as i64,
        };
        led.upsert_listing(&mk("a", Some(30_000_000), 100)).unwrap();
        led.upsert_listing(&mk("b", Some(10_000_000), 101)).unwrap();
        led.upsert_listing(&mk("c", None, 102)).unwrap(); // unpriced jpg
        // Reprice `a` in place (same PK) — still 3 rows, new price.
        led.upsert_listing(&mk("a", Some(5_000_000), 200)).unwrap();

        let rows = led.listings_for_policy("p", 100).unwrap();
        assert_eq!(rows.len(), 3);
        // Cheapest first, unpriced last.
        assert_eq!(
            rows.iter()
                .map(|r| r.asset_name_hex.as_str())
                .collect::<Vec<_>>(),
            ["a", "b", "c"]
        );
        assert_eq!(rows[0].price_lovelace, Some(5_000_000)); // reprice applied
        assert_eq!(led.listing_summary("p").unwrap(), (3, Some(5_000_000)));

        led.delete_listing("p", "a").unwrap();
        assert_eq!(led.listing_summary("p").unwrap(), (2, Some(10_000_000)));
    }

    #[test]
    fn datum_cache_roundtrip_and_idempotent() {
        let mut led = Ledger::open(Path::new(":memory:")).unwrap();
        let h1 = Hash::from([1u8; 32]);
        let h2 = Hash::from([2u8; 32]);
        let bytes1 = vec![0xd8, 0x79, 0x80];

        // First write of two distinct hashes inserts both.
        assert_eq!(
            led.insert_datums(&[(h1, bytes1.clone()), (h2, vec![0x01])])
                .unwrap(),
            2
        );
        // Read back by hash.
        assert_eq!(led.get_datum(&h1).unwrap().as_deref(), Some(&bytes1[..]));
        assert_eq!(led.get_datum(&h2).unwrap().as_deref(), Some(&[0x01][..]));
        // A missing hash is None.
        assert_eq!(led.get_datum(&Hash::from([9u8; 32])).unwrap(), None);

        // Content-addressed + immutable: re-inserting the same hash writes
        // nothing (INSERT OR IGNORE) and never overwrites the stored bytes,
        // even if handed different bytes for the same key.
        assert_eq!(
            led.insert_datums(&[(h1, vec![0xff]), (h2, vec![0xff])])
                .unwrap(),
            0
        );
        assert_eq!(led.get_datum(&h1).unwrap().as_deref(), Some(&bytes1[..]));

        // Empty batch is a no-op.
        assert_eq!(led.insert_datums(&[]).unwrap(), 0);
    }
}
