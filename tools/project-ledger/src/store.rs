//! The sqlite write store — walk-time only. The artifact frontends read is the
//! Parquet export; this file is the transactional substrate a long ingest and
//! `backfill` need. Single writer, WAL, rows append-only via `INSERT OR IGNORE`
//! on their natural keys (which is also how a backfill merges: there is nothing
//! to dedup).
//!
//! Inherited from market-ledger because it was paid for there: a wholesale
//! rewrite of the small checkpoint state at `--checkpoint-every` (never per
//! block — that was the 0.6 blk/s bug), an append-only content-addressed cache
//! that lives OUTSIDE the checkpoint wipe path (`outref_cache`), and per-block
//! batched inserts.

use std::path::Path;

use anyhow::{Context, Result};
use chain_ledger::{AliasKind, Frontier};
use mitos_chain_walk::decode::OutRef;
use pallas_primitives::Hash;
use rusqlite::{Connection, OptionalExtension, params};

use crate::activity::Activity;
use crate::state::{Buffer, BufferedOutput, Holders, WalkState};

pub const SCHEMA: &str = "
-- What the walk is, where it started, and what it was proven against.
CREATE TABLE IF NOT EXISTS walk_meta (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);

-- Node table: a query-ready projection of the frontier at the last checkpoint.
-- The frontier BLOB (below) is the source of truth for resume; this is for
-- reading and for the export.
CREATE TABLE IF NOT EXISTS party (
    key                    TEXT PRIMARY KEY,
    has_stake              INTEGER NOT NULL,
    role                   TEXT NOT NULL,
    label                  TEXT,
    source                 TEXT,
    watched_from_slot      INTEGER NOT NULL,
    promoted_by            TEXT,
    promoted_tx            TEXT,
    expand                 INTEGER NOT NULL,
    terminal_reason        TEXT,
    frozen_at_slot         INTEGER,
    promoted_via_terminal  INTEGER NOT NULL DEFAULT 0,
    receipts               INTEGER NOT NULL DEFAULT 0,
    counterparties         INTEGER NOT NULL DEFAULT 0
);

-- Edge tables, both slot-keyed so one playhead scrubs both.
CREATE TABLE IF NOT EXISTS asset_event (
    tx_hash     TEXT    NOT NULL,
    policy_id   TEXT    NOT NULL,
    asset_name  TEXT    NOT NULL,          -- hex
    -- CIP-67 label class: reference | nft | ft | rft | plain. A CIP-68
    -- collection mints TWO tokens per NFT; only holder-facing classes count
    -- as ownership (see `asset_class.rs`).
    asset_class TEXT    NOT NULL DEFAULT 'plain',
    kind        TEXT    NOT NULL,          -- mint | transfer | burn
    from_party  TEXT,
    to_party    TEXT,
    quantity    INTEGER NOT NULL,
    slot        INTEGER NOT NULL,
    block_time  INTEGER NOT NULL,
    PRIMARY KEY (tx_hash, asset_name, kind, to_party)
);
CREATE INDEX IF NOT EXISTS idx_ae_slot ON asset_event(slot);
CREATE INDEX IF NOT EXISTS idx_ae_to ON asset_event(to_party, slot);

-- A watched party's NET movement in one tx — the fact. Mirrors the case
-- tool's `tx_delta`; there is deliberately no gross figure anywhere.
CREATE TABLE IF NOT EXISTS tx_delta (
    tx_hash            TEXT    NOT NULL,
    party              TEXT    NOT NULL,
    delta              INTEGER NOT NULL,   -- lovelace, signed
    slot               INTEGER NOT NULL,
    block_time         INTEGER NOT NULL,
    unresolved_inputs  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (tx_hash, party)
);
CREATE INDEX IF NOT EXISTS idx_td_party_slot ON tx_delta(party, slot);

-- Directed attribution of that net across the tx's counterparties (pro-rata,
-- `chain_ledger::movements`). One row per (party, counterparty).
CREATE TABLE IF NOT EXISTS value_event (
    tx_hash            TEXT    NOT NULL,
    party              TEXT    NOT NULL,   -- a watched party
    counterparty       TEXT    NOT NULL,
    delta              INTEGER NOT NULL,   -- lovelace, signed from party's view
    slot               INTEGER NOT NULL,
    block_time         INTEGER NOT NULL,
    unresolved_inputs  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (tx_hash, party, counterparty)
);
CREATE INDEX IF NOT EXISTS idx_ve_party_slot ON value_event(party, slot);
CREATE INDEX IF NOT EXISTS idx_ve_slot ON value_event(slot);

-- What the treasury actually MOVED, per unit — ADA and every native token.
--
-- `value_event` answers how much ADA moved between two parties, pro-rata
-- across the tx. That model cannot carry tokens: a project pays for
-- things in USDM, off-ramps through a stable, and gets paid in assets (Mekka
-- did all three), and none of it is lovelace.
--
-- This table is deliberately a DIFFERENT attribution, not an extension of the
-- same one, and says so: ONE ROW PER OUTPUT BUNDLE, from the tx dominant
-- funder to that output's recipient. No pro-rata split is invented — an output
-- is an indivisible fact, and its recipient is exact. What IS a judgement is
-- the payer when a tx is funded by several parties; `payers` records how many
-- there were, so a 1 is exact and anything above it is visibly an attribution.
--
-- Change is NOT a flow: an output back to a party that also funded the tx is
-- skipped, or every transaction would report paying itself.
--
-- `unit` is `lovelace` or `<policy_hex>.<name_hex>` — the Cardano convention,
-- so a unit string here is the same string every indexer uses.
CREATE TABLE IF NOT EXISTS unit_flow (
    tx_hash       TEXT    NOT NULL,
    output_index  INTEGER NOT NULL,
    party         TEXT    NOT NULL,        -- a watched party (either end)
    counterparty  TEXT    NOT NULL,
    unit          TEXT    NOT NULL,
    -- Signed from `party`'s view: negative is out of the treasury.
    quantity      INTEGER NOT NULL,
    payers        INTEGER NOT NULL DEFAULT 1,
    -- The protocol floor this output had to hold, for `unit = 'lovelace'` rows
    -- on an output that also carried a token; 0 otherwise. A token cannot sit
    -- on chain without ADA to pay for its bytes, and that ADA is a carrier, not
    -- a payment. Stored rather than deducted: an output can hold a token AND
    -- real value, so `quantity - min_utxo` is the meaningful figure and only a
    -- reader can decide the threshold. See `DecodedOutput::min_utxo`.
    min_utxo      INTEGER NOT NULL DEFAULT 0,
    slot          INTEGER NOT NULL,
    block_time    INTEGER NOT NULL,
    PRIMARY KEY (tx_hash, output_index, party, unit)
);
CREATE INDEX IF NOT EXISTS idx_uf_party_slot ON unit_flow(party, slot);
CREATE INDEX IF NOT EXISTS idx_uf_unit ON unit_flow(unit, slot);

-- Interpretation, recomputable without a re-walk (`classify`).
CREATE TABLE IF NOT EXISTS value_kind (
    tx_hash      TEXT NOT NULL,
    party        TEXT NOT NULL,
    counterparty TEXT NOT NULL,
    kind         TEXT NOT NULL,
    PRIMARY KEY (tx_hash, party, counterparty)
);

-- Where the mint transaction sent the money.
--
-- Mints bake distribution INTO the mint tx: the buyer pays once and the tx
-- splits it — most to a treasury, often a cut to artists, sometimes a platform
-- fee. Modelling the mint as a single destination loses all of that.
--
-- The rule needs no input resolution: in a tx that mints the policy, a lovelace
-- output to a party that received NO policy asset in that same tx is a payment
-- destination. Outputs to the asset recipient are the buyer's own change and
-- the minAda riding along with the token; outputs carrying the reference token
-- are its deposit. Both are excluded by that one test.
CREATE TABLE IF NOT EXISTS mint_payment (
    tx_hash     TEXT    NOT NULL,
    destination TEXT    NOT NULL,
    lovelace    INTEGER NOT NULL,
    slot        INTEGER NOT NULL,
    block_time  INTEGER NOT NULL,
    PRIMARY KEY (tx_hash, destination)
);
CREATE INDEX IF NOT EXISTS idx_mp_slot ON mint_payment(slot);
CREATE INDEX IF NOT EXISTS idx_mp_dest ON mint_payment(destination);

-- Every name a tracked wallet goes by, so a reader can FIND it by any of them.
--
-- A wallet is a stake key to us, but people hold an address or a $handle. Both
-- are observable during the walk with no indexer: every output is already
-- resolved to a party, so its payment address is free; and an ADA Handle is
-- just an asset under one well-known policy, so an output carrying one names
-- its receiver. Recorded only for wallets the ledger tracks (holders + watched
-- parties) — the set anyone would search for — which keeps this bounded by
-- the project, not the chain. `kind` ∈ address | handle.
CREATE TABLE IF NOT EXISTS party_alias (
    party  TEXT NOT NULL,
    kind   TEXT NOT NULL,
    value  TEXT NOT NULL,
    slot   INTEGER NOT NULL,          -- first seen
    PRIMARY KEY (party, kind, value)
);
CREATE INDEX IF NOT EXISTS idx_pa_value ON party_alias(value);

-- Copied in by `enrich` so the case exports self-contained.
CREATE TABLE IF NOT EXISTS secondary_sale (
    tx_hash        TEXT NOT NULL,
    asset_name     TEXT NOT NULL,
    venue          TEXT NOT NULL,
    price_lovelace INTEGER,
    seller         TEXT,
    buyer          TEXT,
    slot           INTEGER NOT NULL,
    PRIMARY KEY (tx_hash, asset_name)
);

-- What a counterparty IS, where the chain can say so: a DEX pool, a batcher,
-- a marketplace contract, an aggregator.
--
-- Without this, value returning from a swap reads as project income. On Mekka
-- that was 74,626 ₳ of a supposed 132,590 ₳ of "unexplained" inbound — money
-- the treasury had sent out moments earlier, coming back in a different unit.
-- It is the change-vs-receipt error one level up: a ROUND TRIP booked as
-- revenue.
--
-- Interpretation, not observation, so it is recomputable without a re-walk and
-- `reset` clears it. `source` records WHERE the claim came from, because "this
-- address is Minswap" is an assertion inherited from a registry, not something
-- this walk observed.
CREATE TABLE IF NOT EXISTS counterparty_kind (
    key    TEXT PRIMARY KEY,       -- party key as it appears in unit_flow
    kind   TEXT NOT NULL,          -- exchange | marketplace | aggregator | …
    label  TEXT,                   -- "Minswap", "Splash", …
    source TEXT NOT NULL           -- how it was decided
);

-- Checkpoint state (rewritten wholesale at each checkpoint).
CREATE TABLE IF NOT EXISTS walk_cursor (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    slot        INTEGER NOT NULL,
    block_hash  BLOB    NOT NULL
);
CREATE TABLE IF NOT EXISTS frontier_blob (
    id    INTEGER PRIMARY KEY CHECK (id = 1),
    blob  BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS activity_counts (
    id    INTEGER PRIMARY KEY CHECK (id = 1),
    blob  BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS outref_buffer (
    tx_hash       BLOB    NOT NULL,
    output_index  INTEGER NOT NULL,
    address       TEXT    NOT NULL,
    lovelace      INTEGER NOT NULL,
    assets        BLOB    NOT NULL,
    party         TEXT    NOT NULL,
    has_stake     INTEGER NOT NULL,
    PRIMARY KEY (tx_hash, output_index)
);
CREATE TABLE IF NOT EXISTS asset_holder (
    asset_name  TEXT PRIMARY KEY,           -- hex
    party       TEXT NOT NULL,
    since_slot  INTEGER NOT NULL
);

-- The input-resolution ladder's persistent rung: (tx_hash, idx) → what the
-- output was. Append-only, content-addressed, NEVER wiped by a checkpoint —
-- a rebootstrap must not re-fetch a ref.
CREATE TABLE IF NOT EXISTS outref_cache (
    tx_hash       BLOB    NOT NULL,
    output_index  INTEGER NOT NULL,
    address       TEXT    NOT NULL,
    lovelace      INTEGER NOT NULL,
    assets        BLOB    NOT NULL,
    PRIMARY KEY (tx_hash, output_index)
);

-- Refs a walk NEEDED and could not resolve — the shopping list for
-- `resolve-local`.
--
-- Without this the walk knows it failed but not what it failed ON, and the
-- knowledge dies with the run: nothing downstream can tell a receipt from the
-- wallet's own change coming back, and nothing can go and find out. Recording
-- the ref costs one row and turns an unrecoverable gap into a fetch list.
--
-- Kept after resolution rather than deleted, so a second pass can report how
-- much of the list it closed instead of silently shrinking it.
CREATE TABLE IF NOT EXISTS wanted_outref (
    tx_hash       BLOB    NOT NULL,
    output_index  INTEGER NOT NULL,
    first_slot    INTEGER NOT NULL,   -- slot of the tx that wanted it
    PRIMARY KEY (tx_hash, output_index)
);
";

/// Bring an EXISTING ledger's schema up to date.
///
/// **`CREATE TABLE IF NOT EXISTS` never adds a column.** A ledger created before
/// a column existed keeps its old shape forever, the `IF NOT EXISTS` silently
/// does nothing, and the first write fails with "no such column" — or worse,
/// a read returns the old shape and looks like a UI bug. `reset` deliberately
/// keeps the file (to preserve `outref_cache`), so a re-walk does NOT recreate
/// the table and cannot be relied on to migrate it either.
///
/// Adds are idempotent by inspecting `PRAGMA table_info` rather than relying on
/// the error text of a failed `ALTER`.
fn migrate(conn: &Connection) -> Result<()> {
    for (table, column, decl) in [(
        "unit_flow",
        "min_utxo",
        "min_utxo INTEGER NOT NULL DEFAULT 0",
    )] {
        if !has_column(conn, table, column)? {
            conn.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {decl}"))
                .with_context(|| format!("adding {table}.{column}"))?;
            tracing::info!(table, column, "store: migrated schema");
        }
    }
    Ok(())
}

fn has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        if r.get::<_, String>(1)? == column {
            return Ok(true);
        }
    }
    Ok(false)
}

/// A row of `secondary_sale` — a venue sale copied in by `enrich`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecondarySaleRow {
    pub tx_hash: String,
    pub asset_name: String,
    pub venue: String,
    pub price_lovelace: Option<i64>,
    /// `None` where the venue did not record one — common on collection
    /// offers. Absence, never an empty party key.
    pub seller: Option<String>,
    pub buyer: Option<String>,
    pub slot: i64,
}

/// Tables a `reset` clears: everything a walk derives and would re-derive.
pub const RESET_DERIVED_TABLES: [&str; 16] = [
    "counterparty_kind",
    "walk_meta",
    "party",
    "asset_event",
    "tx_delta",
    "value_event",
    "unit_flow",
    "value_kind",
    "mint_payment",
    "party_alias",
    "secondary_sale",
    "walk_cursor",
    "frontier_blob",
    "activity_counts",
    "outref_buffer",
    "asset_holder",
];

/// Tables a `reset` KEEPS: the input-resolution layer, which is bought with a
/// snapshot scan rather than derived, and which the next walk is there to use.
pub const RESET_KEPT_TABLES: [&str; 2] = ["outref_cache", "wanted_outref"];

/// A row of `asset_event`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetEventRow {
    pub tx_hash: String,
    pub policy_id: String,
    pub asset_name: String,
    pub asset_class: &'static str,
    pub kind: &'static str,
    pub from_party: Option<String>,
    pub to_party: Option<String>,
    pub quantity: i64,
    pub slot: u64,
    pub block_time: u64,
}

/// A row of `tx_delta`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxDeltaRow {
    pub tx_hash: String,
    pub party: String,
    pub delta: i64,
    pub slot: u64,
    pub block_time: u64,
    pub unresolved_inputs: u32,
}

/// A row of `value_event`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueEventRow {
    pub tx_hash: String,
    pub party: String,
    pub counterparty: String,
    pub delta: i64,
    pub slot: u64,
    pub block_time: u64,
    pub unresolved_inputs: u32,
}

/// A row of `unit_flow` — one output's worth of one unit, moving.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnitFlowRow {
    pub tx_hash: String,
    pub output_index: u32,
    pub party: String,
    pub counterparty: String,
    /// `lovelace`, or `<policy_hex>.<name_hex>`.
    pub unit: String,
    pub quantity: i64,
    pub payers: u32,
    /// Protocol floor of the OUTPUT this row came from — see the column comment
    /// in `SCHEMA`. Non-zero only on `lovelace` rows of asset-bearing outputs.
    pub min_utxo: u64,
    pub slot: u64,
    pub block_time: u64,
}

/// The Cardano unit string: `lovelace`, else `<policy_hex>.<name_hex>`.
pub fn unit_of(policy: &[u8], name: &[u8]) -> String {
    format!("{}.{}", hex::encode(policy), hex::encode(name))
}

/// A row of `mint_payment` — where a mint tx sent money.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MintPaymentRow {
    pub tx_hash: String,
    pub destination: String,
    pub lovelace: i64,
    pub slot: u64,
    pub block_time: u64,
}

/// A row of `party_alias` — one name a tracked wallet goes by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AliasRow {
    pub party: String,
    pub kind: AliasKind,
    pub value: String,
    pub slot: u64,
}

/// A resolved output from the cache/ladder (no party — resolve on read).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedOutput {
    pub address: String,
    pub lovelace: u64,
    pub assets: Vec<(Vec<u8>, Vec<u8>)>,
}

pub struct Ledger {
    conn: Connection,
}

impl Ledger {
    pub fn open(path: &Path) -> Result<Self> {
        let conn =
            Connection::open(path).with_context(|| format!("opening ledger {}", path.display()))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
        conn.execute_batch(SCHEMA)?;
        migrate(&conn)?;
        Ok(Self { conn })
    }

    #[cfg(test)]
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        migrate(&conn)?;
        Ok(Self { conn })
    }

    // --- meta -----------------------------------------------------------------

    pub fn meta_get(&self, k: &str) -> Result<Option<String>> {
        Ok(self
            .conn
            .query_row("SELECT v FROM walk_meta WHERE k = ?", [k], |r| r.get(0))
            .optional()?)
    }

    pub fn meta_set(&self, k: &str, v: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO walk_meta (k, v) VALUES (?, ?)
             ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            params![k, v],
        )?;
        Ok(())
    }

    // --- rows -----------------------------------------------------------------

    pub fn insert_asset_events(&mut self, rows: &[AssetEventRow]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let tx = self.conn.transaction()?;
        let mut n = 0;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO asset_event
                 (tx_hash, policy_id, asset_name, asset_class, kind, from_party, to_party,
                  quantity, slot, block_time)
                 VALUES (?,?,?,?,?,?,?,?,?,?)",
            )?;
            for r in rows {
                n += stmt.execute(params![
                    r.tx_hash,
                    r.policy_id,
                    r.asset_name,
                    r.asset_class,
                    r.kind,
                    r.from_party,
                    r.to_party,
                    r.quantity,
                    u64_i64(r.slot),
                    u64_i64(r.block_time),
                ])?;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    pub fn insert_tx_deltas(&mut self, rows: &[TxDeltaRow]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let tx = self.conn.transaction()?;
        let mut n = 0;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO tx_delta
                 (tx_hash, party, delta, slot, block_time, unresolved_inputs)
                 VALUES (?,?,?,?,?,?)",
            )?;
            for r in rows {
                n += stmt.execute(params![
                    r.tx_hash,
                    r.party,
                    r.delta,
                    u64_i64(r.slot),
                    u64_i64(r.block_time),
                    r.unresolved_inputs,
                ])?;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    pub fn insert_aliases(&mut self, rows: &[AliasRow]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let tx = self.conn.transaction()?;
        let mut n = 0;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO party_alias (party, kind, value, slot) VALUES (?,?,?,?)",
            )?;
            for r in rows {
                n += stmt.execute(params![r.party, r.kind.as_str(), r.value, u64_i64(r.slot)])?;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    pub fn insert_mint_payments(&mut self, rows: &[MintPaymentRow]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let tx = self.conn.transaction()?;
        let mut n = 0;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO mint_payment
                 (tx_hash, destination, lovelace, slot, block_time) VALUES (?,?,?,?,?)",
            )?;
            for r in rows {
                n += stmt.execute(params![
                    r.tx_hash,
                    r.destination,
                    r.lovelace,
                    u64_i64(r.slot),
                    u64_i64(r.block_time),
                ])?;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    pub fn insert_unit_flows(&mut self, rows: &[UnitFlowRow]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let tx = self.conn.transaction()?;
        let mut n = 0;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO unit_flow
                 (tx_hash, output_index, party, counterparty, unit, quantity, payers,
                  min_utxo, slot, block_time)
                 VALUES (?,?,?,?,?,?,?,?,?,?)",
            )?;
            for r in rows {
                n += stmt.execute(params![
                    r.tx_hash,
                    r.output_index,
                    r.party,
                    r.counterparty,
                    r.unit,
                    r.quantity,
                    r.payers,
                    u64_i64(r.min_utxo),
                    u64_i64(r.slot),
                    u64_i64(r.block_time),
                ])?;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    pub fn insert_value_events(&mut self, rows: &[ValueEventRow]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let tx = self.conn.transaction()?;
        let mut n = 0;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO value_event
                 (tx_hash, party, counterparty, delta, slot, block_time, unresolved_inputs)
                 VALUES (?,?,?,?,?,?,?)",
            )?;
            for r in rows {
                n += stmt.execute(params![
                    r.tx_hash,
                    r.party,
                    r.counterparty,
                    r.delta,
                    u64_i64(r.slot),
                    u64_i64(r.block_time),
                    r.unresolved_inputs,
                ])?;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    pub fn count(&self, table: &str) -> Result<u64> {
        let n: i64 = self
            .conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))?;
        Ok(n as u64)
    }

    // --- outref cache (append-only) ------------------------------------------

    pub fn cache_get(&self, oref: &OutRef) -> Result<Option<CachedOutput>> {
        Ok(self
            .conn
            .query_row(
                "SELECT address, lovelace, assets FROM outref_cache
                 WHERE tx_hash = ? AND output_index = ?",
                params![oref.0.as_ref(), oref.1],
                |r| {
                    let assets: Vec<u8> = r.get(2)?;
                    Ok(CachedOutput {
                        address: r.get(0)?,
                        lovelace: r.get::<_, i64>(1)? as u64,
                        assets: decode_assets(&assets),
                    })
                },
            )
            .optional()?)
    }

    pub fn cache_put(&mut self, entries: &[(OutRef, CachedOutput)]) -> Result<usize> {
        if entries.is_empty() {
            return Ok(0);
        }
        let tx = self.conn.transaction()?;
        let mut n = 0;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO outref_cache (tx_hash, output_index, address, lovelace, assets)
                 VALUES (?,?,?,?,?)",
            )?;
            for (oref, o) in entries {
                n += stmt.execute(params![
                    oref.0.as_ref(),
                    oref.1,
                    o.address,
                    u64_i64(o.lovelace),
                    encode_assets(&o.assets),
                ])?;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    /// Record what counterparties ARE. Idempotent on the key.
    pub fn put_counterparty_kinds(
        &mut self,
        rows: &[(String, &'static str, Option<String>, String)],
    ) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let tx = self.conn.transaction()?;
        let mut n = 0;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR REPLACE INTO counterparty_kind (key, kind, label, source)
                 VALUES (?,?,?,?)",
            )?;
            for (key, kind, label, source) in rows {
                n += stmt.execute(params![key, kind, label, source])?;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    /// Every distinct counterparty in `unit_flow`, with the addresses it is
    /// known by — the registry keys on ADDRESSES, but a staked party's key is
    /// its stake credential, so the addresses come from `party_alias`.
    pub fn counterparties_with_addresses(&self) -> Result<Vec<(String, Vec<String>)>> {
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT counterparty FROM unit_flow WHERE counterparty <> ''",
        )?;
        let keys: Vec<String> = stmt
            .query_map([], |r| r.get(0))?
            .filter_map(Result::ok)
            .collect();
        let mut addr = self
            .conn
            .prepare("SELECT value FROM party_alias WHERE party = ? AND kind = 'address'")?;
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            let mut addrs: Vec<String> = addr
                .query_map([&k], |r| r.get(0))?
                .filter_map(Result::ok)
                .collect();
            // A stakeless party IS an address — the key itself is a candidate.
            if k.starts_with("addr") {
                addrs.push(k.clone());
            }
            out.push((k, addrs));
        }
        Ok(out)
    }

    /// Counterparties carrying real value that `classify` could NOT name,
    /// biggest first — the coverage report's teeth.
    pub fn unclassified_by_value(&self, limit: usize) -> Result<Vec<(String, u64, i64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT f.counterparty, COUNT(*) AS n, SUM(ABS(f.quantity)) AS vol
             FROM unit_flow f
             WHERE f.unit = 'lovelace' AND f.counterparty <> ''
               AND NOT EXISTS (SELECT 1 FROM counterparty_kind k WHERE k.key = f.counterparty)
             GROUP BY f.counterparty ORDER BY vol DESC LIMIT ?",
        )?;
        let rows = stmt.query_map([limit as i64], |r| {
            Ok((r.get(0)?, r.get::<_, i64>(1)? as u64, r.get(2)?))
        })?;
        Ok(rows.filter_map(Result::ok).collect())
    }

    /// Write venue sales in. Idempotent on `(tx_hash, asset_name)`, so a
    /// re-run after market-ledger has walked further merges rather than
    /// duplicates — `REPLACE` not `IGNORE`, because a corrected price from a
    /// later market walk should win over the one already here.
    pub fn put_secondary_sales(&mut self, rows: &[SecondarySaleRow]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let tx = self.conn.transaction()?;
        let mut n = 0;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR REPLACE INTO secondary_sale
                 (tx_hash, asset_name, venue, price_lovelace, seller, buyer, slot)
                 VALUES (?,?,?,?,?,?,?)",
            )?;
            for r in rows {
                n += stmt.execute(params![
                    r.tx_hash,
                    r.asset_name,
                    r.venue,
                    r.price_lovelace,
                    r.seller,
                    r.buyer,
                    r.slot,
                ])?;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    // --- the wanted list ------------------------------------------------------

    /// Note refs a walk needed and could not resolve.
    pub fn wanted_put(&mut self, refs: &[(OutRef, u64)]) -> Result<usize> {
        if refs.is_empty() {
            return Ok(0);
        }
        let tx = self.conn.transaction()?;
        let mut n = 0;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO wanted_outref (tx_hash, output_index, first_slot)
                 VALUES (?,?,?)",
            )?;
            for (oref, slot) in refs {
                n += stmt.execute(params![oref.0.as_ref(), oref.1, u64_i64(*slot)])?;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    /// The DISTINCT transaction hashes on the wanted list.
    ///
    /// Hashes, not refs: a local scan matches whole transactions, and one tx
    /// commonly holds several wanted outputs. Handing back the deduped hash set
    /// keeps the scan's hot-loop lookup to a single hash compare.
    pub fn wanted_tx_hashes(&self) -> Result<std::collections::HashSet<[u8; 32]>> {
        let mut stmt = self
            .conn
            .prepare("SELECT DISTINCT tx_hash FROM wanted_outref")?;
        let rows = stmt.query_map([], |r| r.get::<_, Vec<u8>>(0))?;
        let mut out = std::collections::HashSet::new();
        for h in rows.filter_map(Result::ok) {
            if h.len() == 32 {
                let mut k = [0u8; 32];
                k.copy_from_slice(&h);
                out.insert(k);
            }
        }
        Ok(out)
    }

    /// `(wanted, of which now in the cache)` — how much of the list is closed.
    pub fn wanted_progress(&self) -> Result<(u64, u64)> {
        let wanted: u64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM wanted_outref", [], |r| r.get(0))?;
        let got: u64 = self.conn.query_row(
            "SELECT COUNT(*) FROM wanted_outref w
             JOIN outref_cache c ON c.tx_hash = w.tx_hash AND c.output_index = w.output_index",
            [],
            |r| r.get(0),
        )?;
        Ok((wanted, got))
    }

    /// The earliest slot at which anything on the list was wanted.
    ///
    /// NOT where a local scan should start — the ref points at an output made
    /// EARLIER than the tx that spent it, by an unknown margin. It is the upper
    /// bound: scanning from here finds nothing, so it is the answer to "how far
    /// back must I go?" being strictly further than this.
    pub fn wanted_first_slot(&self) -> Result<Option<u64>> {
        Ok(self
            .conn
            .query_row("SELECT MIN(first_slot) FROM wanted_outref", [], |r| {
                r.get::<_, Option<i64>>(0)
            })?
            .map(|v| v as u64))
    }

    /// Clear everything a walk DERIVES, keeping the resolution layer
    /// (`outref_cache` + `wanted_outref`) so a re-walk starts clean but not
    /// poor.
    ///
    /// This is what makes the three-step flow work at all. The cache is the
    /// expensive thing — a full snapshot scan — and it lives in this same file,
    /// so deleting the file between `resolve-local` and the re-walk would throw
    /// away the very work the re-walk exists to use, silently: the walk would
    /// simply find nothing cached and book change as income again.
    ///
    /// The wanted list is kept alongside it because the two are only meaningful
    /// together — `closed = have/wanted` is how an operator knows whether the
    /// next walk can be believed.
    ///
    /// Returns the rows cleared per table, for the operator to see.
    pub fn reset_derived(&mut self) -> Result<Vec<(&'static str, u64)>> {
        debug_assert!(
            RESET_DERIVED_TABLES
                .iter()
                .all(|t| !RESET_KEPT_TABLES.contains(t)),
            "a table cannot be both cleared and kept"
        );
        let tx = self.conn.transaction()?;
        let mut cleared = Vec::new();
        for t in RESET_DERIVED_TABLES {
            let n: u64 = tx.query_row(&format!("SELECT COUNT(*) FROM {t}"), [], |r| r.get(0))?;
            tx.execute(&format!("DELETE FROM {t}"), [])?;
            cleared.push((t, n));
        }
        tx.commit()?;
        // Without this the file keeps the high-water mark of the walk it just
        // discarded — gigabytes of free pages on a box that also holds a 215 GB
        // snapshot.
        self.conn.execute_batch("VACUUM")?;
        Ok(cleared)
    }

    // --- checkpoint / restore --------------------------------------------------

    /// Persist cursor + frontier + buffer + activity + holders in ONE
    /// transaction, and refresh the `party` projection. Wholesale rewrite of the
    /// small tables; the frontier and activity go in as blobs.
    pub fn checkpoint(&mut self, state: &WalkState, slot: u64, block_hash: &[u8]) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT INTO walk_cursor (id, slot, block_hash) VALUES (1, ?, ?)
             ON CONFLICT(id) DO UPDATE SET slot = excluded.slot, block_hash = excluded.block_hash",
            params![u64_i64(slot), block_hash],
        )?;
        let fblob = postcard::to_allocvec(&state.frontier).context("serialising frontier")?;
        tx.execute(
            "INSERT INTO frontier_blob (id, blob) VALUES (1, ?)
             ON CONFLICT(id) DO UPDATE SET blob = excluded.blob",
            params![fblob],
        )?;
        tx.execute(
            "INSERT INTO activity_counts (id, blob) VALUES (1, ?)
             ON CONFLICT(id) DO UPDATE SET blob = excluded.blob",
            params![state.activity.to_blob()],
        )?;
        tx.execute("DELETE FROM outref_buffer", [])?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO outref_buffer
                 (tx_hash, output_index, address, lovelace, assets, party, has_stake)
                 VALUES (?,?,?,?,?,?,?)",
            )?;
            for (oref, o) in state.buffer.entries() {
                stmt.execute(params![
                    oref.0.as_ref(),
                    oref.1,
                    o.address,
                    u64_i64(o.lovelace),
                    encode_assets(&o.assets),
                    o.party,
                    o.has_stake as i64,
                ])?;
            }
        }
        tx.execute("DELETE FROM asset_holder", [])?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO asset_holder (asset_name, party, since_slot) VALUES (?,?,?)",
            )?;
            for (asset, (party, since)) in state.holders.entries() {
                stmt.execute(params![hex::encode(asset), party, u64_i64(*since)])?;
            }
        }
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO party
                 (key, has_stake, role, watched_from_slot, promoted_by, promoted_tx, expand,
                  terminal_reason, frozen_at_slot, promoted_via_terminal, receipts, counterparties)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                 ON CONFLICT(key) DO UPDATE SET
                   role = excluded.role,
                   watched_from_slot = excluded.watched_from_slot,
                   promoted_by = excluded.promoted_by,
                   promoted_tx = excluded.promoted_tx,
                   expand = excluded.expand,
                   terminal_reason = excluded.terminal_reason,
                   frozen_at_slot = excluded.frozen_at_slot,
                   promoted_via_terminal = excluded.promoted_via_terminal,
                   receipts = excluded.receipts,
                   counterparties = excluded.counterparties",
            )?;
            for m in state.frontier.members() {
                stmt.execute(params![
                    m.party.key,
                    m.party.has_stake_credential as i64,
                    role_str(m.role),
                    u64_i64(m.watched_from_slot),
                    m.promoted_by.as_ref().map(|p| p.key.clone()),
                    m.promoted_tx,
                    m.expand as i64,
                    m.terminal_reason.map(reason_str),
                    m.frozen_at_slot.map(u64_i64),
                    m.promoted_via_terminal as i64,
                    m.receipts,
                    m.counterparties.len() as i64,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Set the registry-declared label + source on a party row (labels are not
    /// frontier state; they come from the registry at seed time).
    pub fn label_party(&self, key: &str, label: &str, source: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE party SET label = ?, source = ? WHERE key = ?",
            params![label, source, key],
        )?;
        Ok(())
    }

    /// The persisted cursor, if any.
    pub fn cursor(&self) -> Result<Option<(u64, Vec<u8>)>> {
        Ok(self
            .conn
            .query_row(
                "SELECT slot, block_hash FROM walk_cursor WHERE id = 1",
                [],
                |r| Ok((r.get::<_, i64>(0)? as u64, r.get(1)?)),
            )
            .optional()?)
    }

    /// Restore the checkpointed state (`None` on a cold ledger).
    pub fn restore(&self) -> Result<Option<WalkState>> {
        let fblob: Option<Vec<u8>> = self
            .conn
            .query_row("SELECT blob FROM frontier_blob WHERE id = 1", [], |r| {
                r.get(0)
            })
            .optional()?;
        let Some(fblob) = fblob else {
            return Ok(None);
        };
        let frontier: Frontier = postcard::from_bytes(&fblob).context("decoding frontier")?;
        let ablob: Vec<u8> = self
            .conn
            .query_row("SELECT blob FROM activity_counts WHERE id = 1", [], |r| {
                r.get(0)
            })
            .optional()?
            .unwrap_or_default();
        let activity = if ablob.is_empty() {
            Activity::default()
        } else {
            Activity::from_blob(&ablob)?
        };

        let mut buffer = Buffer::default();
        {
            let mut stmt = self.conn.prepare(
                "SELECT tx_hash, output_index, address, lovelace, assets, party, has_stake
                 FROM outref_buffer",
            )?;
            let rows = stmt.query_map([], |r| {
                let h: Vec<u8> = r.get(0)?;
                let idx: u32 = r.get(1)?;
                let assets: Vec<u8> = r.get(4)?;
                Ok((
                    h,
                    idx,
                    BufferedOutput {
                        address: r.get(2)?,
                        lovelace: r.get::<_, i64>(3)? as u64,
                        assets: decode_assets(&assets),
                        party: r.get(5)?,
                        has_stake: r.get::<_, i64>(6)? != 0,
                    },
                ))
            })?;
            for row in rows {
                let (h, idx, out) = row?;
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&h);
                buffer.insert((Hash::new(hash), idx), out);
            }
        }

        let mut holders = Holders::default();
        {
            let mut stmt = self
                .conn
                .prepare("SELECT asset_name, party, since_slot FROM asset_holder")?;
            let rows = stmt.query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)? as u64,
                ))
            })?;
            for row in rows {
                let (name, party, since) = row?;
                let bytes = hex::decode(name).unwrap_or_default();
                holders.set(&bytes, &party, since);
            }
        }

        Ok(Some(WalkState {
            frontier,
            buffer,
            activity,
            holders,
        }))
    }
}

pub fn role_str(r: chain_ledger::Role) -> &'static str {
    match r {
        chain_ledger::Role::Declared => "declared",
        chain_ledger::Role::Signer => "signer",
        chain_ledger::Role::Royalty => "royalty",
        chain_ledger::Role::Promoted => "promoted",
        chain_ledger::Role::MintPayee => "mint_payee",
    }
}

pub fn reason_str(r: chain_ledger::TerminalReason) -> &'static str {
    match r {
        chain_ledger::TerminalReason::Stakeless => "stakeless",
        chain_ledger::TerminalReason::Receipts => "receipts",
        chain_ledger::TerminalReason::Counterparties => "counterparties",
        chain_ledger::TerminalReason::Declared => "declared",
    }
}

fn encode_assets(a: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    postcard::to_allocvec(a).expect("assets serialise")
}

fn decode_assets(b: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    postcard::from_bytes(b).unwrap_or_default()
}

fn u64_i64(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chain_ledger::{Party, Role, Thresholds};

    fn state() -> WalkState {
        let mut frontier = Frontier::new(Thresholds::default(), []);
        frontier
            .seed(Party::cardano_stake("stake1treasury"), Role::Declared, 100)
            .unwrap();
        let mut buffer = Buffer::default();
        buffer.insert(
            (Hash::new([1u8; 32]), 0),
            BufferedOutput {
                address: "addr1x".into(),
                lovelace: 5_000_000,
                assets: vec![(vec![9u8; 28], b"Mekka001".to_vec())],
                party: "stake1treasury".into(),
                has_stake: true,
            },
        );
        let mut activity = Activity::default();
        activity.bump([3u8; 28]);
        let mut holders = Holders::default();
        holders.set(b"Mekka001", "stake1buyer", 120);
        WalkState {
            frontier,
            buffer,
            activity,
            holders,
        }
    }

    #[test]
    fn checkpoint_roundtrips_all_state() {
        let mut l = Ledger::open_in_memory().unwrap();
        assert!(l.restore().unwrap().is_none());
        assert!(l.cursor().unwrap().is_none());
        let s = state();
        l.checkpoint(&s, 150, &[7u8; 32]).unwrap();
        let back = l.restore().unwrap().unwrap();
        assert_eq!(back.frontier, s.frontier);
        assert_eq!(back.activity, s.activity);
        assert_eq!(back.holders, s.holders);
        assert_eq!(back.buffer.len(), 1);
        let o = back.buffer.entries().next().unwrap().1;
        assert_eq!(o.lovelace, 5_000_000);
        assert_eq!(o.assets[0].1, b"Mekka001");
        assert_eq!(l.cursor().unwrap(), Some((150, vec![7u8; 32])));
        // party projection landed
        assert_eq!(l.count("party").unwrap(), 1);
        l.label_party("stake1treasury", "S1 treasury", "registry")
            .unwrap();
    }

    #[test]
    fn rows_are_idempotent_on_key() {
        let mut l = Ledger::open_in_memory().unwrap();
        let row = ValueEventRow {
            tx_hash: "aa".into(),
            party: "stake1t".into(),
            counterparty: "stake1b".into(),
            delta: 100,
            slot: 1,
            block_time: 1,
            unresolved_inputs: 0,
        };
        assert_eq!(
            l.insert_value_events(&[row.clone(), row.clone()]).unwrap(),
            1
        );
        assert_eq!(l.insert_value_events(&[row]).unwrap(), 0);
        let a = AssetEventRow {
            tx_hash: "aa".into(),
            policy_id: "pp".into(),
            asset_name: "4d".into(),
            asset_class: "nft",
            kind: "mint",
            from_party: None,
            to_party: Some("stake1b".into()),
            quantity: 1,
            slot: 1,
            block_time: 1,
        };
        assert_eq!(l.insert_asset_events(std::slice::from_ref(&a)).unwrap(), 1);
        assert_eq!(l.insert_asset_events(&[a]).unwrap(), 0);
        assert_eq!(l.count("asset_event").unwrap(), 1);
    }

    /// The shopping list a walk leaves for `resolve-local`.
    ///
    /// An unresolved input disables the change rule — a wallet's own output
    /// coming back can only be recognised as change if we know that wallet
    /// funded the tx — so a ref we failed on is a receipt that may be nothing
    /// of the kind. Recording WHICH ref is what makes that recoverable.
    #[test]
    fn the_wanted_list_records_misses_and_reports_how_many_are_closed() {
        let mut l = Ledger::open_in_memory().unwrap();
        let a = (Hash::new([7u8; 32]), 0u32);
        let b = (Hash::new([7u8; 32]), 1u32); // same tx, second output
        let c = (Hash::new([9u8; 32]), 0u32);
        assert_eq!(l.wanted_put(&[(a, 500), (b, 500), (c, 700)]).unwrap(), 3);
        // Idempotent: a re-walk must not grow the list.
        assert_eq!(l.wanted_put(&[(a, 500)]).unwrap(), 0);

        // Scanning matches whole TRANSACTIONS, so the list dedupes to hashes.
        assert_eq!(
            l.wanted_tx_hashes().unwrap().len(),
            2,
            "three refs across two transactions is two hashes to look for"
        );
        assert_eq!(l.wanted_progress().unwrap(), (3, 0));
        assert_eq!(l.wanted_first_slot().unwrap(), Some(500));

        // Resolving one closes part of the list; the list itself is kept, so a
        // partial resolve reports as partial instead of silently shrinking.
        l.cache_put(&[(
            a,
            CachedOutput {
                address: "addr1x".into(),
                lovelace: 5,
                assets: vec![],
            },
        )])
        .unwrap();
        assert_eq!(l.wanted_progress().unwrap(), (3, 1));
    }

    #[test]
    fn outref_cache_is_append_only_and_survives_checkpoint() {
        let mut l = Ledger::open_in_memory().unwrap();
        let oref = (Hash::new([2u8; 32]), 3);
        let out = CachedOutput {
            address: "addr1y".into(),
            lovelace: 42,
            assets: vec![],
        };
        assert!(l.cache_get(&oref).unwrap().is_none());
        l.cache_put(&[(oref, out.clone())]).unwrap();
        assert_eq!(l.cache_get(&oref).unwrap(), Some(out.clone()));
        l.checkpoint(&state(), 1, &[0u8; 32]).unwrap();
        assert_eq!(l.cache_get(&oref).unwrap(), Some(out));
        l.meta_set("floor_slot", "123").unwrap();
        assert_eq!(l.meta_get("floor_slot").unwrap().as_deref(), Some("123"));
    }

    /// A reset exists to prepare a RE-walk, and the whole point of the re-walk
    /// is to spend the resolution cache. Losing it here would leave no visible
    /// symptom — just change booked as income all over again.
    #[test]
    fn reset_derived_clears_the_walk_but_keeps_the_resolution_layer() {
        let mut l = Ledger::open_in_memory().unwrap();
        let oref = (Hash::new([7u8; 32]), 1);
        let out = CachedOutput {
            address: "addr1z".into(),
            lovelace: 99,
            assets: vec![],
        };
        l.cache_put(&[(oref, out.clone())]).unwrap();
        l.wanted_put(&[(oref, 500)]).unwrap();
        l.meta_set("floor_slot", "160419671").unwrap();
        l.checkpoint(&state(), 42, &[9u8; 32]).unwrap();
        assert_eq!(l.wanted_progress().unwrap(), (1, 1));

        l.reset_derived().unwrap();

        assert_eq!(l.cache_get(&oref).unwrap(), Some(out));
        assert_eq!(l.wanted_progress().unwrap(), (1, 1));
        assert_eq!(l.wanted_first_slot().unwrap(), Some(500));
        // ...and the walk really is gone, so `seed` starts from nothing.
        assert_eq!(l.meta_get("floor_slot").unwrap(), None);
        assert_eq!(l.count("walk_cursor").unwrap(), 0);
        assert_eq!(l.count("party").unwrap(), 0);
    }

    /// `CREATE TABLE IF NOT EXISTS` silently does NOTHING to an existing table,
    /// so a column added to `SCHEMA` never reaches a ledger that already
    /// exists — and `reset` deliberately keeps the file, so a re-walk does not
    /// rescue it either. The first write then fails on "no such column".
    #[test]
    fn migrate_adds_a_column_to_a_ledger_created_before_it_existed() {
        let conn = Connection::open_in_memory().unwrap();
        // The pre-min_utxo shape, as an old ledger on disk still has it.
        conn.execute_batch(
            "CREATE TABLE unit_flow (
                 tx_hash TEXT NOT NULL, output_index INTEGER NOT NULL,
                 party TEXT NOT NULL, counterparty TEXT NOT NULL, unit TEXT NOT NULL,
                 quantity INTEGER NOT NULL, payers INTEGER NOT NULL DEFAULT 1,
                 slot INTEGER NOT NULL, block_time INTEGER NOT NULL,
                 PRIMARY KEY (tx_hash, output_index, party, unit));",
        )
        .unwrap();
        assert!(!has_column(&conn, "unit_flow", "min_utxo").unwrap());

        // What `open` does: the IF NOT EXISTS pass, then the migration.
        conn.execute_batch(SCHEMA).unwrap();
        assert!(
            !has_column(&conn, "unit_flow", "min_utxo").unwrap(),
            "SCHEMA alone must NOT be trusted to add it — that is the trap"
        );
        migrate(&conn).unwrap();
        assert!(has_column(&conn, "unit_flow", "min_utxo").unwrap());

        // Idempotent: opening again must not fail on a duplicate column.
        migrate(&conn).unwrap();
        assert!(has_column(&conn, "unit_flow", "min_utxo").unwrap());
    }

    /// `reset_derived` names its tables in a list, so a table added to `SCHEMA`
    /// later would silently survive a reset and poison the next walk with stale
    /// rows. Ask sqlite what actually exists instead of trusting the list.
    #[test]
    fn reset_derived_covers_every_table_in_the_schema() {
        let l = Ledger::open_in_memory().unwrap();
        let mut stmt = l
            .conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
            )
            .unwrap();
        let actual: std::collections::BTreeSet<String> = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let accounted: std::collections::BTreeSet<String> = RESET_DERIVED_TABLES
            .iter()
            .chain(RESET_KEPT_TABLES.iter())
            .map(|s| (*s).to_string())
            .collect();
        assert_eq!(
            actual, accounted,
            "a table exists that `reset_derived` neither clears nor deliberately keeps"
        );
    }
}
