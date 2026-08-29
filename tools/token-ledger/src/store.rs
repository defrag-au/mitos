//! The ledger — full fidelity, on the walk box, one sqlite per watched token.
//!
//! Per-token rather than one shared db with a policy column, for the reason
//! market-ledger partitions Parquet per venue: a re-walk of one token must
//! never rewrite another's rows, and a SNEK-class walk must not bloat a file
//! every other token shares.
//!
//! ## The primitive is a signed delta, not a directed pair
//!
//! `delta(tx_ord, party_id) -> amount` — one row per party whose balance
//! changed in a transaction. **Not** `(from, to, quantity)`.
//!
//! A directed pair cannot be derived from a multi-party transaction without a
//! heuristic, and the obvious one ("largest absolute delta is the sender") is
//! wrong exactly where the interesting activity is — DEX swaps, batched
//! marketplace fills, atomic swaps. A signed delta needs no heuristic and is
//! sufficient for every balance-shaped projection:
//!
//! ```text
//! balance(party, t) = Σ delta where tx.slot <= t
//! ```
//!
//! Directed flows for visualisation are a later derivation over these rows,
//! and one that should carry its ambiguity explicitly rather than bake a guess
//! into storage.
//!
//! ## Conservation is the self-check
//!
//! Within one transaction the deltas must sum to the net mint of the watched
//! asset — zero for a pure transfer, positive for a mint, negative for a burn.
//! That invariant is free, exact, and catches the entire class of attribution
//! bugs this walker could have. It is asserted per transaction and violations
//! are counted and reported rather than swallowed.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use pallas_primitives::Hash;
use rusqlite::{Connection, OptionalExtension, params};

use crate::buffer::{BufferedOutput, OutrefBuffer};

pub struct Ledger {
    conn: Connection,
    /// address → party_id, so the hot path doesn't round-trip sqlite per row.
    parties: HashMap<String, i64>,
    next_tx_ord: i64,
}

/// One transaction that touched the watched asset.
pub struct TxRow {
    pub tx_hash: Hash<32>,
    pub slot: u64,
    pub block_time: u64,
    /// Net mint of the watched asset in this tx (0 / +mint / −burn).
    pub net_mint: i64,
    /// `(address, stake, signed amount)` for every party whose balance moved.
    pub deltas: Vec<(String, Option<String>, i64)>,
}

impl Ledger {
    pub fn open(path: &Path) -> Result<Self> {
        let conn =
            Connection::open(path).with_context(|| format!("opening ledger {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        // The walk is a single writer doing millions of small inserts; the
        // default per-statement fsync dominates. Crash safety is covered by
        // the cursor being written in the same transaction as the rows it
        // accounts for — a torn tail replays, it does not corrupt.
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS tx (
                 tx_ord     INTEGER PRIMARY KEY,
                 tx_hash    BLOB    NOT NULL UNIQUE,
                 slot       INTEGER NOT NULL,
                 block_time INTEGER NOT NULL,
                 net_mint   INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS tx_slot ON tx(slot);

             CREATE TABLE IF NOT EXISTS party (
                 party_id INTEGER PRIMARY KEY,
                 address  TEXT NOT NULL UNIQUE,
                 stake    TEXT
             );
             CREATE INDEX IF NOT EXISTS party_stake ON party(stake);

             CREATE TABLE IF NOT EXISTS delta (
                 tx_ord   INTEGER NOT NULL,
                 party_id INTEGER NOT NULL,
                 amount   INTEGER NOT NULL,
                 PRIMARY KEY (tx_ord, party_id)
             ) WITHOUT ROWID;
             CREATE INDEX IF NOT EXISTS delta_party ON delta(party_id);

             CREATE TABLE IF NOT EXISTS cursor (
                 k          TEXT PRIMARY KEY,
                 slot       INTEGER NOT NULL,
                 block_hash BLOB
             );

             -- Live UTxOs holding the watched asset, so a resume reloads the
             -- open set instead of re-walking. Without this a resumed walk
             -- silently fails to resolve every input produced before the
             -- resume point.
             CREATE TABLE IF NOT EXISTS buffered (
                 oref_hash BLOB    NOT NULL,
                 oref_idx  INTEGER NOT NULL,
                 address   TEXT    NOT NULL,
                 stake     TEXT,
                 qty       INTEGER NOT NULL,
                 PRIMARY KEY (oref_hash, oref_idx)
             ) WITHOUT ROWID;",
        )?;

        let next_tx_ord: i64 = conn
            .query_row("SELECT COALESCE(MAX(tx_ord), -1) + 1 FROM tx", [], |r| {
                r.get(0)
            })
            .unwrap_or(0);

        let mut parties = HashMap::new();
        {
            let mut stmt = conn.prepare("SELECT address, party_id FROM party")?;
            let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
            for row in rows {
                let (addr, id) = row?;
                parties.insert(addr, id);
            }
        }

        Ok(Self {
            conn,
            parties,
            next_tx_ord,
        })
    }

    /// Drop every row — what `--fresh` means.
    ///
    /// `--fresh` used to reset only the in-memory buffer and the cursor, which
    /// left the previous walk's rows in place and silently double-counted the
    /// overlap. A flag that says "start over" has to actually start over.
    pub fn wipe(&mut self) -> Result<()> {
        self.conn.execute_batch(
            "DELETE FROM delta; DELETE FROM tx; DELETE FROM party;
             DELETE FROM cursor; DELETE FROM buffered;",
        )?;
        self.parties.clear();
        self.next_tx_ord = 0;
        Ok(())
    }

    /// Deltas whose parent transaction is missing. Must always be zero — a
    /// non-zero count means balances are being computed over rows no
    /// transaction accounts for.
    pub fn orphan_deltas(&self) -> Result<i64> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM delta d LEFT JOIN tx t USING (tx_ord)
             WHERE t.tx_ord IS NULL",
            [],
            |r| r.get(0),
        )?)
    }

    /// Resume point: the last slot committed by a previous walk.
    pub fn resume_slot(&self) -> Result<Option<u64>> {
        Ok(self
            .conn
            .query_row("SELECT slot FROM cursor WHERE k = 'walk'", [], |r| {
                r.get::<_, i64>(0)
            })
            .optional()?
            .map(|s| s as u64))
    }

    pub fn load_buffer(&self) -> Result<OutrefBuffer> {
        let mut buf = OutrefBuffer::default();
        let mut stmt = self
            .conn
            .prepare("SELECT oref_hash, oref_idx, address, stake, qty FROM buffered")?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, i64>(4)?,
            ))
        })?;
        for row in rows {
            let (hash, idx, address, stake, qty) = row?;
            let h: [u8; 32] = hash
                .try_into()
                .map_err(|_| anyhow::anyhow!("buffered oref hash is not 32 bytes"))?;
            buf.insert(
                (Hash::from(h), idx as u32),
                BufferedOutput {
                    address,
                    stake,
                    qty,
                },
            );
        }
        Ok(buf)
    }

    /// Commit one block's transactions plus the cursor, atomically.
    ///
    /// Rows and cursor move together — a crash mid-walk leaves a consistent
    /// ledger whose cursor points at the last fully-written block.
    pub fn commit_block(
        &mut self,
        txs: &[TxRow],
        slot: u64,
        block_hash: &Hash<32>,
        buffer: &OutrefBuffer,
        persist_buffer: bool,
    ) -> Result<()> {
        let tx = self.conn.transaction()?;

        for row in txs {
            // A transaction is recorded once, ever. If this hash is already
            // here — a resume that overlapped, a re-run — skip the whole row
            // including its deltas.
            //
            // The earlier shape inserted the tx with `OR IGNORE` but wrote the
            // deltas unconditionally under a fresh ordinal, so a re-walk left
            // deltas with no parent tx and double-counted every balance. The
            // supply reconciliation caught it; this makes it unrepresentable.
            let inserted = tx.execute(
                "INSERT OR IGNORE INTO tx (tx_ord, tx_hash, slot, block_time, net_mint)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    self.next_tx_ord,
                    row.tx_hash.as_ref(),
                    row.slot as i64,
                    row.block_time as i64,
                    row.net_mint
                ],
            )?;
            if inserted == 0 {
                continue;
            }
            let ord = self.next_tx_ord;
            self.next_tx_ord += 1;

            for (address, stake, amount) in &row.deltas {
                let party_id = match self.parties.get(address) {
                    Some(id) => *id,
                    None => {
                        tx.execute(
                            "INSERT OR IGNORE INTO party (address, stake) VALUES (?1, ?2)",
                            params![address, stake],
                        )?;
                        let id: i64 = tx.query_row(
                            "SELECT party_id FROM party WHERE address = ?1",
                            params![address],
                            |r| r.get(0),
                        )?;
                        self.parties.insert(address.clone(), id);
                        id
                    }
                };
                tx.execute(
                    "INSERT OR IGNORE INTO delta (tx_ord, party_id, amount)
                     VALUES (?1, ?2, ?3)",
                    params![ord, party_id, amount],
                )?;
            }
        }

        tx.execute(
            "INSERT INTO cursor (k, slot, block_hash) VALUES ('walk', ?1, ?2)
             ON CONFLICT(k) DO UPDATE SET slot = ?1, block_hash = ?2",
            params![slot as i64, block_hash.as_ref()],
        )?;

        if persist_buffer {
            tx.execute("DELETE FROM buffered", [])?;
            {
                let mut stmt = tx.prepare(
                    "INSERT INTO buffered (oref_hash, oref_idx, address, stake, qty)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                )?;
                for (oref, out) in buffer.entries() {
                    stmt.execute(params![
                        oref.0.as_ref(),
                        oref.1 as i64,
                        out.address,
                        out.stake,
                        out.qty
                    ])?;
                }
            }
        }

        tx.commit()?;
        Ok(())
    }

    /// Derived balances at tip: `(address, stake, balance)`, non-zero only,
    /// descending. This is the projection the walk is verified against.
    pub fn balances(&self) -> Result<Vec<(String, Option<String>, i64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT p.address, p.stake, SUM(d.amount) AS bal
             FROM delta d JOIN party p USING (party_id)
             GROUP BY d.party_id HAVING bal <> 0
             ORDER BY bal DESC",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn counts(&self) -> Result<(i64, i64, i64)> {
        let txs: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM tx", [], |r| r.get(0))?;
        let deltas: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM delta", [], |r| r.get(0))?;
        let parties: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM party", [], |r| r.get(0))?;
        Ok((txs, deltas, parties))
    }

    /// Net minted over the whole walk — must equal total supply at tip.
    pub fn net_mint_total(&self) -> Result<i64> {
        Ok(self
            .conn
            .query_row("SELECT COALESCE(SUM(net_mint), 0) FROM tx", [], |r| {
                r.get(0)
            })?)
    }

    /// Sum of every delta ever recorded — must equal `net_mint_total`.
    pub fn delta_total(&self) -> Result<i64> {
        Ok(self
            .conn
            .query_row("SELECT COALESCE(SUM(amount), 0) FROM delta", [], |r| {
                r.get(0)
            })?)
    }
}
