//! The index: sqlite tables, writers, and the reads `trace` needs.
//!
//! Written once by `index`, censused by `suppress`, queried by `trace`. Nothing
//! here is a follower — a refresh is a re-ingest, exactly as `project-ledger`
//! settled it.
//!
//! ## Why groups, not edges
//!
//! `cosign` stores a transaction's signer set as N rows sharing a `group_id`,
//! not N² pairwise edges. Union-find consumes groups natively, and the measured
//! rate (0.45 rows/tx, mostly two-key groups) makes the per-row overhead the
//! dominant cost — which is also why the evidence `tx_hash` lives once in
//! `cosign_group` rather than on every row.

use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{Connection, params};

use crate::witness::KeyHash;

pub struct Index {
    pub conn: Connection,
}

const SCHEMA: &str = r#"
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- One row per transaction that contributed a co-signing group. The tx hash is
-- THE evidence: every merge `trace` reports cites one, so a claim is always
-- re-derivable from the chain rather than trusted.
CREATE TABLE IF NOT EXISTS cosign_group (
    group_id INTEGER PRIMARY KEY,
    tx_hash  BLOB NOT NULL,
    slot     INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS cosign (
    group_id INTEGER NOT NULL,
    key_hash BLOB NOT NULL,
    PRIMARY KEY (group_id, key_hash)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS cosign_by_key ON cosign (key_hash);

-- payment_cred <-> stake_cred, first sighting. Turns a cluster of anonymous
-- key hashes into `stake1…` wallets a human can open.
CREATE TABLE IF NOT EXISTS cred_pair (
    payment_cred BLOB NOT NULL,
    stake_cred   BLOB NOT NULL,
    stake_script INTEGER NOT NULL,
    first_slot   INTEGER NOT NULL,
    PRIMARY KEY (payment_cred, stake_cred)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS cred_pair_by_stake ON cred_pair (stake_cred);

-- Stake credentials named EXPLICITLY by a tx (certificate or withdrawal).
-- Supplies the labelling the witness set cannot: which 28-byte hashes in a
-- cluster are stake credentials, so they render as `stake1…` wallets directly.
CREATE TABLE IF NOT EXISTS stake_event (
    tx_hash    BLOB NOT NULL,
    stake_cred BLOB NOT NULL,
    kind       TEXT NOT NULL,
    is_script  INTEGER NOT NULL,
    slot       INTEGER NOT NULL,
    PRIMARY KEY (tx_hash, stake_cred, kind)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS stake_event_by_cred ON stake_event (stake_cred);

-- Written by `suppress`. Operator keys excluded from union-find, WITH their
-- measured degree, so the exclusion is auditable rather than a silent drop.
CREATE TABLE IF NOT EXISTS suppressed_key (
    key_hash BLOB PRIMARY KEY,
    degree   INTEGER NOT NULL,
    reason   TEXT NOT NULL
) WITHOUT ROWID;
"#;

impl Index {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)
            .with_context(|| format!("opening index at {}", path.display()))?;
        conn.execute_batch(SCHEMA).context("applying schema")?;
        Ok(Self { conn })
    }

    pub fn set_meta(&self, key: &str, value: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO meta (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn get_meta(&self, key: &str) -> Result<Option<String>> {
        let mut st = self.conn.prepare("SELECT value FROM meta WHERE key = ?1")?;
        let mut rows = st.query(params![key])?;
        Ok(match rows.next()? {
            Some(r) => Some(r.get(0)?),
            None => None,
        })
    }

    /// Highest `group_id` written, so a resumed run does not collide.
    pub fn max_group_id(&self) -> Result<i64> {
        Ok(self.conn.query_row(
            "SELECT COALESCE(MAX(group_id), 0) FROM cosign_group",
            [],
            |r| r.get(0),
        )?)
    }

    pub fn counts(&self) -> Result<(i64, i64, i64, i64)> {
        let q = |sql: &str| -> Result<i64> { Ok(self.conn.query_row(sql, [], |r| r.get(0))?) };
        Ok((
            q("SELECT COUNT(*) FROM cosign_group")?,
            q("SELECT COUNT(*) FROM cosign")?,
            q("SELECT COUNT(*) FROM cred_pair")?,
            q("SELECT COUNT(*) FROM suppressed_key")?,
        ))
    }

    pub fn stake_event_count(&self) -> Result<i64> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM stake_event", [], |r| r.get(0))?)
    }

    /// Is this credential known to be a STAKE credential?
    ///
    /// Returns the kinds seen and how many transactions named it, so a report
    /// can distinguish "delegated once" from "withdrew rewards 362 times".
    pub fn stake_cred_info(&self, cred: &KeyHash) -> Result<Option<(i64, bool)>> {
        let mut st = self.conn.prepare_cached(
            "SELECT COUNT(*), MAX(is_script) FROM stake_event WHERE stake_cred = ?1",
        )?;
        let mut rows = st.query(params![&cred[..]])?;
        Ok(match rows.next()? {
            Some(r) => {
                let n: i64 = r.get(0)?;
                if n == 0 {
                    None
                } else {
                    Some((n, r.get::<_, i64>(1)? != 0))
                }
            }
            None => None,
        })
    }

    /// Transactions that named two or more DISTINCT stake credentials, one of
    /// them `cred`. Nobody withdraws another person's rewards or registers
    /// their stake key, so a shared transaction here is same-owner evidence
    /// independent of the co-signing graph.
    pub fn stake_cooccurrences(&self, cred: &KeyHash) -> Result<Vec<(KeyHash, [u8; 32], String)>> {
        let mut st = self.conn.prepare_cached(
            "SELECT b.stake_cred, b.tx_hash, b.kind
               FROM stake_event a
               JOIN stake_event b
                 ON b.tx_hash = a.tx_hash AND b.stake_cred <> a.stake_cred
              WHERE a.stake_cred = ?1 AND b.is_script = 0
              GROUP BY b.stake_cred",
        )?;
        let mut rows = st.query(params![&cred[..]])?;
        let mut out = Vec::new();
        while let Some(r) = rows.next()? {
            let raw: Vec<u8> = r.get(1)?;
            let mut hash = [0u8; 32];
            if raw.len() == 32 {
                hash.copy_from_slice(&raw);
            }
            out.push((key_of(&r.get::<_, Vec<u8>>(0)?), hash, r.get(2)?));
        }
        Ok(out)
    }

    /// Groups and credential pairs from one batch, in a single transaction.
    pub fn write_batch(
        &mut self,
        groups: &[(i64, [u8; 32], u64)],
        members: &[(i64, KeyHash)],
        pairs: &[(KeyHash, KeyHash, bool, u64)],
        stake_events: &[([u8; 32], KeyHash, &'static str, bool, u64)],
    ) -> Result<()> {
        let tx = self.conn.transaction()?;
        {
            let mut g = tx.prepare_cached(
                "INSERT OR IGNORE INTO cosign_group (group_id, tx_hash, slot) VALUES (?1, ?2, ?3)",
            )?;
            for (id, hash, slot) in groups {
                g.execute(params![id, &hash[..], *slot as i64])?;
            }
            let mut m = tx.prepare_cached(
                "INSERT OR IGNORE INTO cosign (group_id, key_hash) VALUES (?1, ?2)",
            )?;
            for (id, key) in members {
                m.execute(params![id, &key[..]])?;
            }
            let mut p = tx.prepare_cached(
                "INSERT OR IGNORE INTO cred_pair
                   (payment_cred, stake_cred, stake_script, first_slot)
                 VALUES (?1, ?2, ?3, ?4)",
            )?;
            for (pay, stake, is_script, slot) in pairs {
                p.execute(params![
                    &pay[..],
                    &stake[..],
                    *is_script as i64,
                    *slot as i64
                ])?;
            }
            let mut s = tx.prepare_cached(
                "INSERT OR IGNORE INTO stake_event
                   (tx_hash, stake_cred, kind, is_script, slot)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            for (hash, cred, kind, is_script, slot) in stake_events {
                s.execute(params![
                    &hash[..],
                    &cred[..],
                    *kind,
                    *is_script as i64,
                    *slot as i64
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Degree census: for every key, how many DISTINCT other keys it has ever
    /// co-signed with, materialised into `key_degree`.
    ///
    /// **This runs in SQL on purpose.** The first implementation built the
    /// co-signer graph in Rust — `HashMap<KeyHash, HashSet<KeyHash>>` — which is
    /// fine on the few-thousand-key window it was written against and
    /// catastrophic on the real index: **24,042,244 distinct keys over
    /// 71,043,815 rows**, tens of GB of hash-set overhead, on a box with 16 GB
    /// free and four production services running. Streaming it through sqlite
    /// keeps the working set on disk, where 500 GB is free.
    ///
    /// The self-join is cheap despite appearances: `cosign`'s primary key leads
    /// with `group_id`, so matching a row to its group-mates is an index seek,
    /// and groups are capped at `--max-group` (8) members.
    pub fn build_key_degree(&mut self) -> Result<i64> {
        self.conn.execute_batch(
            "DROP TABLE IF EXISTS key_degree;
             CREATE TABLE key_degree (
                 key_hash BLOB PRIMARY KEY,
                 degree   INTEGER NOT NULL,
                 groups   INTEGER NOT NULL
             ) WITHOUT ROWID;
             INSERT INTO key_degree (key_hash, degree, groups)
             SELECT a.key_hash,
                    COUNT(DISTINCT b.key_hash),
                    COUNT(DISTINCT a.group_id)
               FROM cosign a
               JOIN cosign b
                 ON b.group_id = a.group_id AND b.key_hash <> a.key_hash
              GROUP BY a.key_hash;
             CREATE INDEX key_degree_by_degree ON key_degree (degree DESC);",
        )?;
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM key_degree", [], |r| r.get(0))?)
    }

    pub fn have_key_degree(&self) -> Result<bool> {
        let n: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='key_degree'",
            [],
            |r| r.get(0),
        )?;
        Ok(n > 0)
    }

    /// How many keys fall in `[lo, hi]`.
    pub fn degree_bucket(&self, lo: i64, hi: i64) -> Result<i64> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM key_degree WHERE degree >= ?1 AND degree <= ?2",
            params![lo, hi],
            |r| r.get(0),
        )?)
    }

    /// The degree at rank `n` (0-based) descending — the percentile cut.
    pub fn degree_at_rank(&self, n: i64) -> Result<i64> {
        Ok(self
            .conn
            .query_row(
                "SELECT degree FROM key_degree ORDER BY degree DESC LIMIT 1 OFFSET ?1",
                params![n],
                |r| r.get(0),
            )
            .unwrap_or(i64::MAX))
    }

    pub fn top_by_degree(&self, n: usize) -> Result<Vec<(KeyHash, i64, i64)>> {
        let mut st = self.conn.prepare(
            "SELECT key_hash, degree, groups FROM key_degree ORDER BY degree DESC LIMIT ?1",
        )?;
        let mut rows = st.query(params![n as i64])?;
        let mut out = Vec::new();
        while let Some(r) = rows.next()? {
            out.push((key_of(&r.get::<_, Vec<u8>>(0)?), r.get(1)?, r.get(2)?));
        }
        Ok(out)
    }

    /// `(degree, groups)` for one key, when the census has been run.
    pub fn degree_of(&self, key: &KeyHash) -> Result<Option<(i64, i64)>> {
        if !self.have_key_degree()? {
            return Ok(None);
        }
        let mut st = self
            .conn
            .prepare_cached("SELECT degree, groups FROM key_degree WHERE key_hash = ?1")?;
        let mut rows = st.query(params![&key[..]])?;
        Ok(match rows.next()? {
            Some(r) => Some((r.get(0)?, r.get(1)?)),
            None => None,
        })
    }

    pub fn key_degree_count(&self) -> Result<i64> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM key_degree", [], |r| r.get(0))?)
    }

    /// Write the suppression list straight from `key_degree` — never through a
    /// Rust-side Vec, for the same reason the census itself is SQL.
    pub fn suppress_above(&mut self, threshold: i64) -> Result<usize> {
        let n = self.conn.execute(
            "INSERT OR REPLACE INTO suppressed_key (key_hash, degree, reason)
             SELECT key_hash, degree, 'degree' FROM key_degree WHERE degree > ?1",
            params![threshold],
        )?;
        Ok(n)
    }

    pub fn clear_suppressed(&mut self) -> Result<()> {
        self.conn.execute("DELETE FROM suppressed_key", [])?;
        Ok(())
    }

    #[cfg(test)]
    pub fn write_suppressed(&mut self, rows: &[(KeyHash, usize, &str)]) -> Result<usize> {
        let tx = self.conn.transaction()?;
        let mut n = 0;
        {
            let mut st = tx.prepare_cached(
                "INSERT OR REPLACE INTO suppressed_key (key_hash, degree, reason)
                 VALUES (?1, ?2, ?3)",
            )?;
            for (k, d, reason) in rows {
                n += st.execute(params![&k[..], *d as i64, reason])?;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    pub fn suppressed(&self) -> Result<HashSet<KeyHash>> {
        let mut out = HashSet::new();
        let mut st = self.conn.prepare("SELECT key_hash FROM suppressed_key")?;
        let mut rows = st.query([])?;
        while let Some(r) = rows.next()? {
            out.insert(key_of(&r.get::<_, Vec<u8>>(0)?));
        }
        Ok(out)
    }

    /// Every group a key appears in, with its evidence.
    pub fn groups_for_key(&self, key: &KeyHash) -> Result<Vec<(i64, [u8; 32], u64)>> {
        let mut st = self.conn.prepare_cached(
            "SELECT g.group_id, g.tx_hash, g.slot
               FROM cosign c JOIN cosign_group g ON g.group_id = c.group_id
              WHERE c.key_hash = ?1",
        )?;
        let mut rows = st.query(params![&key[..]])?;
        let mut out = Vec::new();
        while let Some(r) = rows.next()? {
            let raw: Vec<u8> = r.get(1)?;
            let mut hash = [0u8; 32];
            if raw.len() == 32 {
                hash.copy_from_slice(&raw);
            }
            out.push((r.get(0)?, hash, r.get::<_, i64>(2)? as u64));
        }
        Ok(out)
    }

    pub fn members_of_group(&self, group_id: i64) -> Result<Vec<KeyHash>> {
        let mut st = self
            .conn
            .prepare_cached("SELECT key_hash FROM cosign WHERE group_id = ?1")?;
        let mut rows = st.query(params![group_id])?;
        let mut out = Vec::new();
        while let Some(r) = rows.next()? {
            out.push(key_of(&r.get::<_, Vec<u8>>(0)?));
        }
        Ok(out)
    }

    /// Stake credentials ever seen beside a payment credential.
    pub fn stakes_for_payment(&self, key: &KeyHash) -> Result<Vec<(KeyHash, bool)>> {
        let mut st = self.conn.prepare_cached(
            "SELECT stake_cred, stake_script FROM cred_pair WHERE payment_cred = ?1",
        )?;
        let mut rows = st.query(params![&key[..]])?;
        let mut out = Vec::new();
        while let Some(r) = rows.next()? {
            out.push((key_of(&r.get::<_, Vec<u8>>(0)?), r.get::<_, i64>(1)? != 0));
        }
        Ok(out)
    }

    /// Payment credentials ever seen beside a stake credential — how a
    /// `stake1…` seed becomes a set of keys to start the walk from.
    pub fn payments_for_stake(&self, stake: &KeyHash) -> Result<Vec<KeyHash>> {
        let mut st = self
            .conn
            .prepare_cached("SELECT payment_cred FROM cred_pair WHERE stake_cred = ?1")?;
        let mut rows = st.query(params![&stake[..]])?;
        let mut out = Vec::new();
        while let Some(r) = rows.next()? {
            out.push(key_of(&r.get::<_, Vec<u8>>(0)?));
        }
        Ok(out)
    }
}

fn key_of(b: &[u8]) -> KeyHash {
    let mut out = [0u8; 28];
    let n = b.len().min(28);
    out[..n].copy_from_slice(&b[..n]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(b: u8) -> KeyHash {
        [b; 28]
    }

    fn mem() -> Index {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        Index { conn }
    }

    #[test]
    fn groups_round_trip_with_evidence() {
        let mut ix = mem();
        ix.write_batch(
            &[(1, [7u8; 32], 100)],
            &[(1, k(1)), (1, k(2))],
            &[(k(1), k(9), false, 100)],
            &[([7u8; 32], k(9), "withdraw", false, 100)],
        )
        .unwrap();

        // A stake credential named by a cert/withdrawal must be recognisable as
        // one, so a cluster member can render as a wallet without needing a
        // cred_pair sighting.
        assert_eq!(ix.stake_cred_info(&k(9)).unwrap(), Some((1, false)));
        assert_eq!(ix.stake_cred_info(&k(1)).unwrap(), None);

        let gs = ix.groups_for_key(&k(1)).unwrap();
        assert_eq!(gs.len(), 1);
        assert_eq!(gs[0].1, [7u8; 32], "the evidence hash must survive");
        assert_eq!(ix.members_of_group(1).unwrap().len(), 2);
        assert_eq!(ix.stakes_for_payment(&k(1)).unwrap(), vec![(k(9), false)]);
        assert_eq!(ix.payments_for_stake(&k(9)).unwrap(), vec![k(1)]);
    }

    #[test]
    fn degree_counts_distinct_cosigners_not_appearances() {
        let mut ix = mem();
        // k1 co-signs with k2 three times, and with k3 once. Degree is 2, NOT
        // 4 — this is the whole point of the metric: an active wallet that
        // repeatedly signs with its own other key must not look like a hub.
        ix.write_batch(
            &[
                (1, [0; 32], 1),
                (2, [0; 32], 2),
                (3, [0; 32], 3),
                (4, [0; 32], 4),
            ],
            &[
                (1, k(1)),
                (1, k(2)),
                (2, k(1)),
                (2, k(2)),
                (3, k(1)),
                (3, k(2)),
                (4, k(1)),
                (4, k(3)),
            ],
            &[],
            &[],
        )
        .unwrap();
        assert_eq!(ix.build_key_degree().unwrap(), 3);

        let deg = |key: KeyHash| -> i64 {
            ix.conn
                .query_row(
                    "SELECT degree FROM key_degree WHERE key_hash = ?1",
                    params![&key[..]],
                    |r| r.get(0),
                )
                .unwrap()
        };
        assert_eq!(deg(k(1)), 2);
        assert_eq!(deg(k(2)), 1);
        assert_eq!(deg(k(3)), 1);

        // groups counts appearances, and the two must not be confused.
        let groups: i64 = ix
            .conn
            .query_row(
                "SELECT groups FROM key_degree WHERE key_hash = ?1",
                params![&k(1)[..]],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(groups, 4);
    }

    #[test]
    fn suppress_above_writes_only_the_tail() {
        let mut ix = mem();
        ix.write_batch(
            &[(1, [0; 32], 1), (2, [0; 32], 2)],
            &[(1, k(1)), (1, k(2)), (2, k(1)), (2, k(3))],
            &[],
            &[],
        )
        .unwrap();
        ix.build_key_degree().unwrap();
        // k1 has degree 2; k2 and k3 have degree 1.
        assert_eq!(ix.suppress_above(1).unwrap(), 1);
        let s = ix.suppressed().unwrap();
        assert!(s.contains(&k(1)));
        assert!(!s.contains(&k(2)));
    }

    #[test]
    fn suppression_round_trips() {
        let mut ix = mem();
        ix.write_suppressed(&[(k(5), 900, "degree")]).unwrap();
        assert!(ix.suppressed().unwrap().contains(&k(5)));
    }
}
