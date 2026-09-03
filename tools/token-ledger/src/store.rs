//! The ledger — full fidelity, on the walk box, one sqlite per watched token.
//!
//! Per-token rather than one shared db with a policy column, for the reason
//! market-ledger partitions Parquet per venue: a re-walk of one token must
//! never rewrite another's rows, and a SNEK-class walk must not bloat a file
//! every other token shares.
//!
//! ## The primitive is a signed delta, not a directed pair
//!
//! `delta(tx_ord, party_id, unit_id) -> amount` — one row per party per unit
//! whose balance changed in a transaction. **Not** `(from, to, quantity)`.
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
//! Within one transaction the deltas for EACH UNIT must sum to that unit's net
//! mint — zero for a pure transfer, positive for a mint, negative for a burn.
//! That invariant is free, exact, and catches the entire class of attribution
//! bugs this walker could have. It is asserted per transaction and violations
//! are counted and reported rather than swallowed.
//!
//! Per unit rather than per transaction is what keeps it alive when a ledger
//! follows a whole policy: summed across units the check still balances while a
//! gained unit silently cancels a lost one, which is exactly the mis-attribution
//! it exists to catch. See `walk::conservation_breaches`, which is pure and
//! tested against that case.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result, bail};
use pallas_primitives::Hash;
use rusqlite::{Connection, OptionalExtension, params};

use crate::buffer::{BufferedOutput, OutrefBuffer};
use crate::pools::PoolObservation;

/// Cursor keys, named once. A reader that spelled `walked_from` where the
/// writer says `walk_from` once read `None`, concluded the ledger covered
/// nothing, and quietly did nothing — a typo that produced a plausible answer.
pub mod cursor_key {
    /// Highest slot covered.
    pub const WALK_TO: &str = "walk_to";
}

pub struct Ledger {
    conn: Connection,
    /// address → party_id, so the hot path doesn't round-trip sqlite per row.
    parties: HashMap<String, i64>,
    /// (dex, key_policy, key_name) → pool_id, same reason.
    pools: HashMap<(String, Vec<u8>, Vec<u8>), i64>,
    /// asset-name bytes → unit_id. Same reason again, and it matters more here:
    /// a policy-wide walk touches this on every delta of every transaction,
    /// where the pool and party caches see far fewer distinct keys.
    units: HashMap<Vec<u8>, i64>,
    next_tx_ord: i64,
}

/// One party's balance at tip.
pub struct Balance {
    pub address: String,
    pub stake: Option<String>,
    /// `None` until `classify_parties` has run.
    pub cohort: Option<String>,
    pub amount: i64,
}

/// A live output at a script address we could not name.
pub struct UnnamedOutput {
    pub address: String,
    pub qty: i64,
    pub datum_cbor: Option<Vec<u8>>,
    pub datum_hash: Option<Vec<u8>>,
}

/// One transaction, as `export` reads it back out.
pub struct TxRecord {
    pub tx_ord: i64,
    pub tx_hash: Vec<u8>,
    pub slot: u64,
    /// Wall time of the containing block. Maturity is judged against this, so
    /// a walk over an old snapshot dates itself honestly instead of borrowing
    /// the operator's clock.
    pub block_time: u64,
    pub net_mint: i64,
}

/// One party, as `export` reads it back out.
pub struct PartyRecord {
    pub party_id: i64,
    pub address: String,
    pub stake: Option<String>,
    pub cohort: Option<String>,
    pub basis: Option<String>,
}

/// Which asset a ledger holds — recorded by the walk so readers are
/// self-describing rather than trusting a flag.
pub struct AssetMeta {
    pub name: String,
    pub policy: Vec<u8>,
    /// The single watched asset, or `None` when this ledger follows the WHOLE
    /// policy. Distinct from `Some(vec![])`, which is the asset whose on-chain
    /// name is genuinely zero bytes.
    pub asset_name: Option<Vec<u8>>,
    /// `None` = unknown, render raw. Not the same as `Some(0)`.
    pub decimals: Option<u8>,
}

impl AssetMeta {
    /// Whole-token scale: `10^decimals`, or 1 when the scale is unknown.
    ///
    /// Returning 1 for unknown means quantities pass through unscaled, which is
    /// the raw count — the honest reading when nobody has told us otherwise.
    pub fn scale(&self) -> f64 {
        10f64.powi(self.decimals.unwrap_or(0) as i32)
    }
}

/// One point on the reserve curve, joined to its pool.
pub struct CurvePoint {
    pub tx_ord: i64,
    pub dex: String,
    pub key_policy: Vec<u8>,
    pub key_name: Vec<u8>,
    pub key_basis: String,
    pub fee_bps: Option<i64>,
    pub base_reserve: i64,
    pub quote_reserve: i64,
}

/// One lock position across its whole life.
pub struct LockLifetime {
    pub created_tx_ord: i64,
    /// `None` while still live at tip.
    pub spent_tx_ord: Option<i64>,
    pub qty: i64,
    pub unlock_ts_ms: u64,
}

/// One live lock position.
pub struct LockPosition {
    pub qty: i64,
    pub unlock_ts_ms: u64,
}

/// Live locks plus the ledger's own tip time — the two halves of "what is
/// still locked", kept together so maturity is never judged against the wrong
/// clock.
pub struct LockSnapshot {
    pub positions: Vec<LockPosition>,
    /// Last block time in the ledger. `None` for an empty ledger.
    pub tip_time: Option<u64>,
}

/// A pool's latest reserves — one row per pool, at the walk's end.
pub struct PoolTip {
    pub dex: String,
    pub key_basis: String,
    pub base_reserve: i64,
    pub quote_reserve: i64,
    pub fee_bps: Option<i64>,
}

/// One transaction that touched the watched asset.
pub struct TxRow {
    pub tx_hash: Hash<32>,
    pub slot: u64,
    pub block_time: u64,
    /// Net mint per unit in this tx (0 / +mint / −burn), keyed by asset-name
    /// bytes. Empty for the overwhelming majority of transactions.
    ///
    /// The stored `tx.net_mint` is the SUM of these, which is what the global
    /// reconciliation compares against `Σ delta.amount`. That total stays
    /// correct policy-wide because every unit conserves independently.
    pub net_mint: Vec<(Vec<u8>, i64)>,
    /// Every party whose balance moved, per unit.
    pub deltas: Vec<DeltaRow>,
    /// Pool reserves observed in this tx's outputs.
    pub pools: Vec<PoolObservation>,
    /// Lock positions this tx opened.
    pub locks_created: Vec<LockCreated>,
    /// Lock positions this tx closed (outrefs it spent).
    pub locks_spent: Vec<(Hash<32>, u32)>,
}

/// How far a ledger's coverage reaches — and therefore what its delta sums
/// actually mean.
///
/// **Three states, and an `Option<bool>` cannot hold them.** That was the first
/// shape here and it collapsed the interesting distinction: `None` had to stand
/// for "nobody recorded it", which is neither "complete" nor "partial" and is
/// answered differently from both. A reader has to be able to tell "these are
/// holdings" from "these are a window of movement" from "we do not know", and
/// only the third is fixed by re-running a walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completeness {
    /// Coverage reaches the policy's beginning. Delta sums ARE holdings, and
    /// every supply-derived projection is defined.
    Complete,
    /// Coverage reaches a floor and stops. Sums are net movement inside that
    /// window: an arrival whose source sits below the floor has no matching
    /// departure, so they are not holdings and nothing may divide by them.
    Partial,
    /// Written before completeness was recorded. Genuinely unknown — and
    /// settled by an incremental walk, which is what a reader should be told.
    Unrecorded,
}

impl Completeness {
    /// Every variant.
    ///
    /// Exercised by tests rather than by the binary — the production code
    /// matches exhaustively, which the compiler already enforces. Kept because
    /// the tests that assert every state has a wire spelling and a manifest
    /// spelling are exactly what stops a fourth variant shipping half-wired.
    #[cfg_attr(not(test), allow(dead_code))]
    pub const ALL: [Completeness; 3] = [
        Completeness::Complete,
        Completeness::Partial,
        Completeness::Unrecorded,
    ];

    /// Stored form. `None` for [`Completeness::Unrecorded`], which is the
    /// absence of a row rather than a value.
    pub fn code(self) -> Option<i64> {
        match self {
            Completeness::Complete => Some(1),
            Completeness::Partial => Some(0),
            Completeness::Unrecorded => None,
        }
    }

    pub fn from_code(code: Option<i64>) -> Self {
        match code {
            Some(1) => Completeness::Complete,
            Some(_) => Completeness::Partial,
            None => Completeness::Unrecorded,
        }
    }

    /// The wire spelling. Matches `shared_types::policy_feed::Completeness`'s
    /// serde representation and `policy_archive::Completeness::as_wire` — one
    /// vocabulary across the tunnel, and a mismatch is a frontend that
    /// silently reads every ledger as unrecorded. Pinned by a test in
    /// `policy_api`; the binary itself now speaks through the archive's copy.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn as_wire(self) -> &'static str {
        match self {
            Completeness::Complete => "complete",
            Completeness::Partial => "partial",
            Completeness::Unrecorded => "unrecorded",
        }
    }

    /// May delta sums be presented as HOLDINGS?
    ///
    /// Only when known complete. `Unrecorded` is refused here even though it is
    /// allowed to keep the supply reports — labelling movements as holdings is
    /// a claim a reader believes, and it is the one mistake worth being
    /// asymmetric about.
    pub fn balances_are_holdings(self) -> bool {
        matches!(self, Completeness::Complete)
    }

    /// May the mint / cascade / cap reports run? Each divides by supply.
    ///
    /// `Unrecorded` passes: withholding them from every pre-flag ledger would
    /// break the tool for every token it already serves, and the header has
    /// already said the coverage is unknown.
    pub fn supply_reports_defined(self) -> bool {
        !matches!(self, Completeness::Partial)
    }
}

/// What a ledger covers — what `stats` reports alongside the balances.
pub struct Coverage {
    /// Lowest slot covered. `None` on a ledger no walk has recorded.
    pub walked_from: Option<u64>,
    /// Inputs a walk registered as wanting a source and never resolved — the
    /// honest gap on a ledger written before its first mint was known.
    pub unresolved: u64,
}

/// The `Ledger`'s in-memory id caches, borrowed together.
///
/// Bundled because `write_rows` needs all four while a `Transaction` holds a
/// borrow of `conn`, and passing them as four parameters put the function over
/// clippy's argument threshold for no gain in clarity.
struct Caches<'a> {
    parties: &'a mut HashMap<String, i64>,
    units: &'a mut HashMap<Vec<u8>, i64>,
    pools: &'a mut HashMap<(String, Vec<u8>, Vec<u8>), i64>,
    next_tx_ord: &'a mut i64,
}

/// One party's signed movement of one unit, within one transaction.
///
/// A struct rather than a 4-tuple because the tuple it replaced was already at
/// three fields and the unit makes four, at which point `.2` stops telling a
/// reader anything and the stake and the name are both `Option`-ish strings
/// waiting to be transposed.
pub struct DeltaRow {
    pub address: String,
    pub stake: Option<String>,
    /// On-chain asset-name bytes of the unit that moved.
    pub name: Vec<u8>,
    pub amount: i64,
}

/// A lock position opened by a transaction.
pub struct LockCreated {
    pub oref: (Hash<32>, u32),
    pub address: String,
    pub qty: i64,
    pub unlock_ts_ms: u64,
    pub owner_pkh: Option<String>,
}

/// Add a column if the table doesn't already have it.
///
/// Idempotent, and the only safe way to widen a table that may predate the
/// column — `CREATE TABLE IF NOT EXISTS` silently does nothing on an existing
/// table, so a new column in the CREATE reaches fresh databases only.
fn ensure_column(conn: &Connection, table: &str, column: &str, ty: &str) -> Result<()> {
    let present: bool = {
        let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
        let mut rows = stmt.query([])?;
        let mut found = false;
        while let Some(r) = rows.next()? {
            if r.get::<_, String>(1)? == column {
                found = true;
                break;
            }
        }
        found
    };
    if !present {
        conn.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column} {ty}"))?;
    }
    Ok(())
}

/// Per-unit quantities on a buffered output, as stored bytes.
///
/// A hand-rolled encoding rather than a serde format: this is written once per
/// buffered UTxO on every buffer persist — tens of thousands of rows, every few
/// hundred thousand blocks — and it never leaves this file, so there is no wire
/// compatibility to honour. `u8` name length is sound because a Cardano asset
/// name is capped at 32 bytes by the ledger rules.
fn encode_units(units: &[(Vec<u8>, i64)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(units.len() * 16);
    for (name, qty) in units {
        out.push(name.len() as u8);
        out.extend_from_slice(name);
        out.extend_from_slice(&qty.to_le_bytes());
    }
    out
}

fn decode_units(mut blob: &[u8]) -> Result<Vec<(Vec<u8>, i64)>> {
    let mut units = Vec::new();
    while !blob.is_empty() {
        let len = blob[0] as usize;
        // A truncated record means the row is unreadable, and a buffer silently
        // short of entries is the one failure this walk cannot detect later —
        // every input it fails to resolve becomes a conservation violation
        // attributed to the wrong transaction. Refuse loudly instead.
        if blob.len() < 1 + len + 8 {
            bail!("buffered units blob is truncated");
        }
        let name = blob[1..1 + len].to_vec();
        let qty = i64::from_le_bytes(
            blob[1 + len..1 + len + 8]
                .try_into()
                .expect("8 bytes checked above"),
        );
        units.push((name, qty));
        blob = &blob[1 + len + 8..];
    }
    Ok(units)
}

/// Give `delta` a unit dimension, rebuilding the table because its PRIMARY KEY
/// changes.
///
/// `ALTER TABLE … ADD COLUMN` cannot widen a primary key, and the key is the
/// whole point: under `PRIMARY KEY (tx_ord, party_id)` a transaction in which
/// one party moves two different units of the same policy collides, and the
/// `INSERT OR IGNORE` in `commit_block` silently drops the second. That is the
/// exact bug policy-wide watching exists to avoid, so the rebuild is not
/// optional tidying.
///
/// Existing rows are attributed to the ledger's single watched asset, read from
/// `meta` — sound precisely because a ledger written under the old schema
/// watched exactly one asset by construction.
fn migrate_delta_to_units(conn: &Connection) -> Result<()> {
    let has_unit_id: bool = {
        let mut stmt = conn.prepare("PRAGMA table_info(delta)")?;
        let mut rows = stmt.query([])?;
        let mut found = false;
        while let Some(r) = rows.next()? {
            if r.get::<_, String>(1)? == "unit_id" {
                found = true;
                break;
            }
        }
        found
    };
    // The index lives here rather than in the schema batch, so it is only ever
    // created once `delta` is known to carry the column — on a fresh ledger
    // that is immediately, on an existing one only after the rebuild below.
    if has_unit_id {
        conn.execute_batch("CREATE INDEX IF NOT EXISTS delta_unit ON delta(unit_id);")?;
        return Ok(());
    }

    // The asset this ledger was watching. Absent only on a ledger with no
    // `meta` row — i.e. one that has never walked — where there are no deltas
    // to attribute either, so the empty name is harmless.
    let existing_name: Vec<u8> = conn
        .query_row("SELECT asset_name FROM meta WHERE k = 'asset'", [], |r| {
            r.get(0)
        })
        .unwrap_or_default();

    conn.execute(
        "INSERT OR IGNORE INTO unit (name) VALUES (?1)",
        params![existing_name],
    )?;
    let unit_id: i64 = conn.query_row(
        "SELECT unit_id FROM unit WHERE name = ?1",
        params![existing_name],
        |r| r.get(0),
    )?;

    conn.execute_batch(&format!(
        "CREATE TABLE delta_new (
             tx_ord   INTEGER NOT NULL,
             party_id INTEGER NOT NULL,
             unit_id  INTEGER NOT NULL,
             amount   INTEGER NOT NULL,
             PRIMARY KEY (tx_ord, party_id, unit_id)
         ) WITHOUT ROWID;
         INSERT INTO delta_new (tx_ord, party_id, unit_id, amount)
             SELECT tx_ord, party_id, {unit_id}, amount FROM delta;
         DROP TABLE delta;
         ALTER TABLE delta_new RENAME TO delta;
         CREATE INDEX IF NOT EXISTS delta_party ON delta(party_id);
         CREATE INDEX IF NOT EXISTS delta_unit ON delta(unit_id);"
    ))?;
    tracing::info!("store: delta migrated to carry a unit dimension");
    Ok(())
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

             -- `cohort` + `basis` are DERIVED from the address, the pool set
             -- and the sink registry — never from the walk. That is what makes
             -- reclassification a re-derivation rather than a re-walk, and it
             -- is why they are nullable: a party exists the moment it moves,
             -- and is classified afterwards.
             CREATE TABLE IF NOT EXISTS party (
                 party_id INTEGER PRIMARY KEY,
                 address  TEXT NOT NULL UNIQUE,
                 stake    TEXT,
                 cohort   TEXT,
                 basis    TEXT
             );
             CREATE INDEX IF NOT EXISTS party_stake ON party(stake);

             -- `unit_id` is in the KEY, not merely a column: without it a tx in
             -- which one party moves two units of the same policy collides and
             -- the second is silently dropped. See `migrate_delta_to_units`.
             CREATE TABLE IF NOT EXISTS delta (
                 tx_ord   INTEGER NOT NULL,
                 party_id INTEGER NOT NULL,
                 unit_id  INTEGER NOT NULL,
                 amount   INTEGER NOT NULL,
                 PRIMARY KEY (tx_ord, party_id, unit_id)
             ) WITHOUT ROWID;
             CREATE INDEX IF NOT EXISTS delta_party ON delta(party_id);
             -- NOTE: the `delta_unit` index is created by
             -- `migrate_delta_to_units`, NOT here. On an existing ledger the
             -- CREATE TABLE above is a no-op, so `delta` still has no
             -- `unit_id` when this batch runs and an index naming that column
             -- fails outright — taking the whole `open` with it. Caught on a
             -- real ledger the first time this ran off the laptop.

             -- One row per pool instance ever seen. `key_basis` records how
             -- firmly the instance is identified (published in the datum vs
             -- inferred from the value vs not at all) — a missing decoder must
             -- show up as weak evidence, never as absence.
             CREATE TABLE IF NOT EXISTS pool (
                 pool_id    INTEGER PRIMARY KEY,
                 dex        TEXT NOT NULL,
                 address    TEXT NOT NULL,
                 key_policy BLOB NOT NULL,
                 key_name   BLOB NOT NULL,
                 key_basis  TEXT NOT NULL,
                 UNIQUE (dex, key_policy, key_name)
             );

             -- The reserve curve. One row per pool-touching transaction;
             -- reserves are unchanged between rows, so spot price is exact at
             -- every slot and piecewise-constant in between. Never interpolate
             -- across these — the price genuinely did not move.
             -- `ada_paired` decides whether a pool may contribute to the
             -- PRICE. Its supply counts either way — a token/token pool holds
             -- real tokens — but its lovelace is a min-UTxO carrier, not a
             -- quote reserve, and pricing from it is wrong by orders of
             -- magnitude. Most WingRiders V2 pools are token/token.
             CREATE TABLE IF NOT EXISTS pool_state (
                 tx_ord         INTEGER NOT NULL,
                 pool_id        INTEGER NOT NULL,
                 base_reserve   INTEGER NOT NULL,
                 quote_reserve  INTEGER NOT NULL,
                 fee_bps        INTEGER,
                 total_lp       INTEGER,
                 reserve_source TEXT NOT NULL,
                 ada_paired     INTEGER NOT NULL DEFAULT 1,
                 PRIMARY KEY (tx_ord, pool_id)
             ) WITHOUT ROWID;

             -- Lock LIFETIMES, not just live locks. `buffered` holds the open
             -- set at tip, which answers what is locked NOW but cannot answer
             -- what was locked THEN: a lock created and claimed mid-history
             -- leaves no trace there. Recording both ends lets the export state
             -- maturity at any past checkpoint, which is what makes the cap
             -- band correct over the whole domain rather than only at the end.
             CREATE TABLE IF NOT EXISTS lock (
                 oref_hash      BLOB    NOT NULL,
                 oref_idx       INTEGER NOT NULL,
                 created_tx_ord INTEGER NOT NULL,
                 spent_tx_ord   INTEGER,
                 address        TEXT    NOT NULL,
                 qty            INTEGER NOT NULL,
                 unlock_ts_ms   INTEGER NOT NULL,
                 owner_pkh      TEXT,
                 PRIMARY KEY (oref_hash, oref_idx)
             ) WITHOUT ROWID;
             CREATE INDEX IF NOT EXISTS lock_created ON lock(created_tx_ord);

             CREATE TABLE IF NOT EXISTS cursor (
                 k          TEXT PRIMARY KEY,
                 slot       INTEGER NOT NULL,
                 block_hash BLOB
             );

             -- Which asset this ledger is ABOUT. One row, written by the walk.
             --
             -- The db is per-token precisely so no row needs a policy column,
             -- but that left it unable to say what it held: `stats --db` had no
             -- identity at all, so it could not scale a price by the token's
             -- decimals and printed CSWAP spot as 0.00000000. Recording it here
             -- rather than adding a --token flag to every read command means a
             -- reader cannot be pointed at the wrong registry entry.
             --
             -- decimals is NULLABLE and null means UNKNOWN, not zero: render
             -- raw. Decimals are not on chain, so an absent value is a real
             -- state, not a missing default.
             CREATE TABLE IF NOT EXISTS meta (
                 k          TEXT PRIMARY KEY,
                 name       TEXT NOT NULL,
                 policy     BLOB NOT NULL,
                 asset_name BLOB NOT NULL,
                 decimals   INTEGER
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
             ) WITHOUT ROWID;

             -- Every asset name this ledger has seen under its policy.
             --
             -- A single-asset ledger holds exactly one row here and every
             -- aggregate below reads identically to the way it did before this
             -- table existed. A POLICY-WIDE ledger holds one row per unit, and
             -- that is what keeps the per-tx conservation check meaningful:
             -- summed across units, a tx moving one unit out and another in
             -- nets to zero and hides both moves.
             CREATE TABLE IF NOT EXISTS unit (
                 unit_id INTEGER PRIMARY KEY,
                 name    BLOB NOT NULL UNIQUE
             );

             -- Per-unit mint breakdown. `tx.net_mint` remains the total ACROSS
             -- units, which is still exactly what the global reconciliation
             -- wants — every unit conserves, so their sum conserves — while
             -- this answers which unit actually moved. Empty for the
             -- overwhelming majority of transactions, which mint nothing.
             CREATE TABLE IF NOT EXISTS tx_mint (
                 tx_ord  INTEGER NOT NULL,
                 unit_id INTEGER NOT NULL,
                 amount  INTEGER NOT NULL,
                 PRIMARY KEY (tx_ord, unit_id)
             ) WITHOUT ROWID;

             -- Outrefs a reverse pass is still waiting on: spent by a
             -- transaction already written, source not yet reached.
             --
             -- PERSISTED, because a reverse walk's whole point is to deepen
             -- across separate runs. Held only in memory, a second pass starts
             -- with no record of what the first was waiting for and can never
             -- resolve it — measured on SpaceBudz, pass 2 backfilled nothing
             -- into pass 1's rows. The forward walk's buffer is persisted for
             -- exactly the same reason.
             CREATE TABLE IF NOT EXISTS pending_input (
                 oref_hash BLOB    NOT NULL,
                 oref_idx  INTEGER NOT NULL,
                 spender   BLOB    NOT NULL,
                 PRIMARY KEY (oref_hash, oref_idx, spender)
             ) WITHOUT ROWID;",
        )?;

        // `CREATE TABLE IF NOT EXISTS` is a NO-OP on an existing table — it
        // will never add a column. A ledger written before `cohort`/`basis`
        // existed keeps the old shape silently, and the first statement that
        // names the new column fails at runtime. Add them explicitly, then
        // build the index that depends on them.
        ensure_column(&conn, "party", "cohort", "TEXT")?;
        ensure_column(&conn, "party", "basis", "TEXT")?;
        // Defaults to 1 so a ledger written before the column keeps its
        // existing behaviour — every pool it recorded was ADA-paired, because
        // only CSwap and Splash were decoded and both of this token's pools
        // pair with ADA.
        ensure_column(
            &conn,
            "pool_state",
            "ada_paired",
            "INTEGER NOT NULL DEFAULT 1",
        )?;
        ensure_column(&conn, "buffered", "unlock_ts_ms", "INTEGER")?;
        ensure_column(&conn, "buffered", "owner_pkh", "TEXT")?;
        ensure_column(&conn, "buffered", "datum_cbor", "BLOB")?;
        ensure_column(&conn, "buffered", "datum_hash", "BLOB")?;
        // Per-unit quantities on a buffered output. `qty` stays the total, so
        // a ledger written before this column reloads its buffer with the same
        // arithmetic it always had; the breakdown is synthesised from `meta`
        // for those rows, which is correct precisely because such a ledger
        // watched exactly one asset.
        ensure_column(&conn, "buffered", "units", "BLOB")?;
        ensure_column(&conn, "meta", "policy_wide", "INTEGER")?;
        conn.execute_batch("CREATE INDEX IF NOT EXISTS party_cohort ON party(cohort);")?;
        migrate_delta_to_units(&conn)?;

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

        let mut pools = HashMap::new();
        {
            let mut stmt = conn.prepare("SELECT dex, key_policy, key_name, pool_id FROM pool")?;
            let rows = stmt.query_map([], |r| {
                Ok((
                    (
                        r.get::<_, String>(0)?,
                        r.get::<_, Vec<u8>>(1)?,
                        r.get::<_, Vec<u8>>(2)?,
                    ),
                    r.get::<_, i64>(3)?,
                ))
            })?;
            for row in rows {
                let (k, id) = row?;
                pools.insert(k, id);
            }
        }

        let mut units = HashMap::new();
        {
            let mut stmt = conn.prepare("SELECT name, unit_id FROM unit")?;
            let rows =
                stmt.query_map([], |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, i64>(1)?)))?;
            for row in rows {
                let (name, id) = row?;
                units.insert(name, id);
            }
        }

        Ok(Self {
            conn,
            parties,
            pools,
            units,
            next_tx_ord,
        })
    }

    /// `unit_id` for an asset name, inserting the unit on first sight.
    ///
    /// Takes the cache as a parameter rather than `&mut self` so it can be
    /// called while a `Transaction` holds a borrow of `self.conn`.
    fn unit_id(
        tx: &rusqlite::Transaction<'_>,
        cache: &mut HashMap<Vec<u8>, i64>,
        name: &[u8],
    ) -> Result<i64> {
        if let Some(id) = cache.get(name) {
            return Ok(*id);
        }
        tx.execute(
            "INSERT OR IGNORE INTO unit (name) VALUES (?1)",
            params![name],
        )?;
        let id: i64 = tx.query_row(
            "SELECT unit_id FROM unit WHERE name = ?1",
            params![name],
            |r| r.get(0),
        )?;
        cache.insert(name.to_vec(), id);
        Ok(id)
    }

    /// Drop every row — what `--fresh` means.
    ///
    /// `--fresh` used to reset only the in-memory buffer and the cursor, which
    /// left the previous walk's rows in place and silently double-counted the
    /// overlap. A flag that says "start over" has to actually start over.
    pub fn wipe(&mut self) -> Result<()> {
        self.conn.execute_batch(
            "DELETE FROM delta; DELETE FROM tx; DELETE FROM party;
             DELETE FROM pool_state; DELETE FROM pool;
             DELETE FROM cursor; DELETE FROM buffered;",
        )?;
        self.parties.clear();
        self.pools.clear();
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

    /// Resume point: the last slot committed by a previous walk. The FORWARD
    /// frontier — see [`Self::walked_from`] for its counterpart.
    pub fn resume_slot(&self) -> Result<Option<u64>> {
        Ok(self
            .conn
            .query_row("SELECT slot FROM cursor WHERE k = 'walk'", [], |r| {
                r.get::<_, i64>(0)
            })
            .optional()?
            .map(|s| s as u64))
    }

    /// The FLOOR: the lowest slot this ledger has contiguous coverage from.
    ///
    /// The counterpart to [`Self::resume_slot`], and the pair is the whole
    /// coverage statement — `[walked_from, resume_slot]`. Without this a reader
    /// cannot tell "the policy starts here" from "we stopped looking here",
    /// which is the same distinction wallet-sieve draws with
    /// `scanned_from_slot` against `scanned_to_slot`, and for the same reason:
    /// a timeline built on the first reading silently presents a partial walk
    /// as a whole history.
    ///
    /// It is also what makes buffer completeness a recorded FACT rather than an
    /// assumption — a resumed partial walk used to claim completeness simply
    /// because it had a cursor.
    ///
    /// `None` means no walk has recorded a floor: either the ledger is empty,
    /// or it predates this cursor. Callers must treat that as "unknown", never
    /// as genesis.
    pub fn walked_from(&self) -> Result<Option<u64>> {
        Ok(self
            .conn
            .query_row("SELECT slot FROM cursor WHERE k = 'walk_from'", [], |r| {
                r.get::<_, i64>(0)
            })
            .optional()?
            .map(|s| s as u64))
    }

    /// Record how far this ledger's coverage reaches.
    ///
    /// A RECORDED FACT, not a re-derivation, because the ledger alone cannot
    /// establish it. `walked_from <= first_slot` looks like the test and is
    /// exactly backwards: on a shallow walk the floor sits below the earliest
    /// transaction it happened to find, so every partial ledger would pass.
    /// And `walked_from == 0` would call a complete walk from a registered
    /// first-mint floor "partial", which is the opposite error.
    ///
    /// Only the walk knows — it holds the registry entry — so it writes the
    /// answer down and every reader trusts it.
    pub fn set_completeness(&self, state: Completeness) -> Result<()> {
        let Some(code) = state.code() else {
            // `Unrecorded` is the ABSENCE of a row, not a value. Writing it
            // would claim we had decided "unknown", which is not a decision.
            return Ok(());
        };
        self.conn.execute(
            "INSERT INTO cursor (k, slot) VALUES ('coverage_complete', ?1)
             ON CONFLICT(k) DO UPDATE SET slot = ?1",
            params![code],
        )?;
        Ok(())
    }

    /// How far this ledger's coverage reaches. Absent row ⇒
    /// [`Completeness::Unrecorded`].
    pub fn completeness(&self) -> Result<Completeness> {
        let code: Option<i64> = self
            .conn
            .query_row(
                "SELECT slot FROM cursor WHERE k = 'coverage_complete'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        Ok(Completeness::from_code(code))
    }

    /// The UPPER bound of coverage — the counterpart to [`Self::walked_from`].
    ///
    /// Recorded because the pair is the only honest statement of what a ledger
    /// holds, and neither walk direction can supply it alone. A forward walk's
    /// frontier is its resume cursor; a reverse walk deliberately never moves
    /// that cursor, so a reverse-only ledger would have no upper bound at all
    /// and `MAX(tx.slot)` is not one — that is the newest row found, not the
    /// slot below which we stopped looking.
    ///
    /// `max` rather than assignment: coverage only ever grows upward, and a
    /// deepening pass that starts lower must not retract the top.
    pub fn set_walked_to(&self, slot: u64) -> Result<()> {
        self.conn.execute(
            "INSERT INTO cursor (k, slot) VALUES (?1, ?2)
             ON CONFLICT(k) DO UPDATE SET slot = max(slot, excluded.slot)",
            params![cursor_key::WALK_TO, slot as i64],
        )?;
        Ok(())
    }

    /// Lower the floor to `slot`, never raise it.
    ///
    /// `min` rather than assignment because coverage only ever grows downward:
    /// a later shallow walk over a ledger that already reaches deeper must not
    /// erase what it knows. That is exactly the shape of a refresh running after
    /// a deep backfill, which is the common case rather than an edge one.
    pub fn set_walked_from(&self, slot: u64) -> Result<()> {
        self.conn.execute(
            "INSERT INTO cursor (k, slot) VALUES ('walk_from', ?1)
             ON CONFLICT(k) DO UPDATE SET slot = min(slot, excluded.slot)",
            params![slot as i64],
        )?;
        Ok(())
    }

    /// Record which asset this ledger is about. Idempotent; the walk calls it.
    /// Stamp what this ledger is about. `asset_name` is `None` for a
    /// whole-policy watch.
    ///
    /// Policy mode is recorded in a separate `policy_wide` flag rather than as
    /// a NULL `asset_name`, because the column is `NOT NULL` on every deployed
    /// ledger and sqlite cannot relax that without a table rebuild. The flag
    /// also keeps the empty-name asset representable, which a NULL-or-empty
    /// encoding would have quietly lost.
    pub fn put_meta(
        &self,
        name: &str,
        policy: &[u8],
        asset_name: Option<&[u8]>,
        decimals: Option<u8>,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO meta (k, name, policy, asset_name, decimals, policy_wide)
             VALUES ('asset', ?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(k) DO UPDATE SET
                 name = excluded.name,
                 policy = excluded.policy,
                 asset_name = excluded.asset_name,
                 decimals = excluded.decimals,
                 policy_wide = excluded.policy_wide",
            params![
                name,
                policy,
                asset_name.unwrap_or(&[]),
                decimals,
                i64::from(asset_name.is_none())
            ],
        )?;
        Ok(())
    }

    /// What this ledger is about, if a walk has recorded it.
    ///
    /// `None` for a ledger written before the `meta` table existed — the read
    /// commands degrade to unscaled output rather than failing, since an old db
    /// is still perfectly valid, just silent about its own units.
    pub fn asset_meta(&self) -> Result<Option<AssetMeta>> {
        Ok(self
            .conn
            .query_row(
                "SELECT name, policy, asset_name, decimals, policy_wide
                 FROM meta WHERE k = 'asset'",
                [],
                |r| {
                    let policy_wide = r.get::<_, Option<i64>>(4)?.unwrap_or(0) != 0;
                    Ok(AssetMeta {
                        name: r.get(0)?,
                        policy: r.get(1)?,
                        // A ledger written before the flag existed watched a
                        // single asset by construction, so an absent flag reads
                        // as `Some` — the pre-policy behaviour, unchanged.
                        asset_name: (!policy_wide).then(|| r.get(2)).transpose()?,
                        decimals: r
                            .get::<_, Option<i64>>(3)?
                            .and_then(|d| u8::try_from(d).ok()),
                    })
                },
            )
            .optional()?)
    }

    pub fn load_buffer(&self) -> Result<OutrefBuffer> {
        let mut buf = OutrefBuffer::default();
        // What a row written before the `units` column was about. A ledger of
        // that vintage watched exactly one asset by construction, so attributing
        // its whole `qty` to that asset is exact, not a guess.
        let legacy_unit: Vec<u8> = self
            .asset_meta()?
            .and_then(|m| m.asset_name)
            .unwrap_or_default();
        let mut stmt = self.conn.prepare(
            "SELECT oref_hash, oref_idx, address, stake, qty, unlock_ts_ms, owner_pkh,
                    datum_cbor, datum_hash, units
             FROM buffered",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, Option<i64>>(5)?,
                r.get::<_, Option<String>>(6)?,
                r.get::<_, Option<Vec<u8>>>(7)?,
                r.get::<_, Option<Vec<u8>>>(8)?,
                r.get::<_, Option<Vec<u8>>>(9)?,
            ))
        })?;
        for row in rows {
            let (hash, idx, address, stake, qty, unlock, owner, datum, dhash, units) = row?;
            let h: [u8; 32] = hash
                .try_into()
                .map_err(|_| anyhow::anyhow!("buffered oref hash is not 32 bytes"))?;
            let units = match units.as_deref().filter(|b| !b.is_empty()) {
                Some(blob) => decode_units(blob)?,
                None => vec![(legacy_unit.clone(), qty)],
            };
            buf.insert(
                (Hash::from(h), idx as u32),
                BufferedOutput {
                    address,
                    stake,
                    units,
                    unlock_ts_ms: unlock.map(|v| v as u64),
                    owner_pkh: owner,
                    datum_cbor: datum,
                    datum_hash: dhash,
                },
            );
        }
        Ok(buf)
    }

    /// Write transaction rows and everything hanging off them.
    ///
    /// Takes the caches as a parameter rather than `&mut self` because a
    /// `Transaction` already holds a borrow of `self.conn`.
    fn write_rows(tx: &rusqlite::Transaction<'_>, c: &mut Caches<'_>, txs: &[TxRow]) -> Result<()> {
        for row in txs {
            // A transaction is recorded once, ever. If this hash is already
            // here — a resume that overlapped, a re-run — skip the whole row
            // including its deltas.
            //
            // The earlier shape inserted the tx with `OR IGNORE` but wrote the
            // deltas unconditionally under a fresh ordinal, so a re-walk left
            // deltas with no parent tx and double-counted every balance. The
            // supply reconciliation caught it; this makes it unrepresentable.
            //
            // The reverse pass leans on this too: it meets a transaction's
            // outputs first and its sources later, and the later visit must not
            // create a second row. Its backfill goes through `add_deltas`.
            let net_mint_total: i64 = row.net_mint.iter().map(|(_, a)| *a).sum();
            let inserted = tx.execute(
                "INSERT OR IGNORE INTO tx (tx_ord, tx_hash, slot, block_time, net_mint)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    *c.next_tx_ord,
                    row.tx_hash.as_ref(),
                    row.slot as i64,
                    row.block_time as i64,
                    net_mint_total
                ],
            )?;
            if inserted == 0 {
                continue;
            }
            let ord = *c.next_tx_ord;
            *c.next_tx_ord += 1;

            for (name, amount) in &row.net_mint {
                let unit_id = Self::unit_id(tx, c.units, name)?;
                tx.execute(
                    "INSERT OR IGNORE INTO tx_mint (tx_ord, unit_id, amount)
                     VALUES (?1, ?2, ?3)",
                    params![ord, unit_id, amount],
                )?;
            }

            for d in &row.deltas {
                let party_id = match c.parties.get(&d.address) {
                    Some(id) => *id,
                    None => {
                        tx.execute(
                            "INSERT OR IGNORE INTO party (address, stake) VALUES (?1, ?2)",
                            params![d.address, d.stake],
                        )?;
                        let id: i64 = tx.query_row(
                            "SELECT party_id FROM party WHERE address = ?1",
                            params![d.address],
                            |r| r.get(0),
                        )?;
                        c.parties.insert(d.address.clone(), id);
                        id
                    }
                };
                let unit_id = Self::unit_id(tx, c.units, &d.name)?;
                tx.execute(
                    "INSERT OR IGNORE INTO delta (tx_ord, party_id, unit_id, amount)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![ord, party_id, unit_id, d.amount],
                )?;
            }

            for lock in &row.locks_created {
                tx.execute(
                    "INSERT OR IGNORE INTO lock
                         (oref_hash, oref_idx, created_tx_ord, spent_tx_ord,
                          address, qty, unlock_ts_ms, owner_pkh)
                     VALUES (?1, ?2, ?3, NULL, ?4, ?5, ?6, ?7)",
                    params![
                        lock.oref.0.as_ref(),
                        lock.oref.1 as i64,
                        ord,
                        lock.address,
                        lock.qty,
                        lock.unlock_ts_ms as i64,
                        lock.owner_pkh
                    ],
                )?;
            }
            for (hash, idx) in &row.locks_spent {
                tx.execute(
                    "UPDATE lock SET spent_tx_ord = ?3
                     WHERE oref_hash = ?1 AND oref_idx = ?2 AND spent_tx_ord IS NULL",
                    params![hash.as_ref(), *idx as i64, ord],
                )?;
            }

            for obs in &row.pools {
                let key = (
                    obs.dex.to_string(),
                    obs.key_policy.clone(),
                    obs.key_name.clone(),
                );
                let pool_id = match c.pools.get(&key) {
                    Some(id) => *id,
                    None => {
                        tx.execute(
                            "INSERT OR IGNORE INTO pool
                                 (dex, address, key_policy, key_name, key_basis)
                             VALUES (?1, ?2, ?3, ?4, ?5)",
                            params![
                                obs.dex,
                                obs.address,
                                obs.key_policy,
                                obs.key_name,
                                obs.key_basis.as_str()
                            ],
                        )?;
                        let id: i64 = tx.query_row(
                            "SELECT pool_id FROM pool
                             WHERE dex = ?1 AND key_policy = ?2 AND key_name = ?3",
                            params![obs.dex, obs.key_policy, obs.key_name],
                            |r| r.get(0),
                        )?;
                        c.pools.insert(key, id);
                        id
                    }
                };
                tx.execute(
                    "INSERT OR IGNORE INTO pool_state
                         (tx_ord, pool_id, base_reserve, quote_reserve,
                          fee_bps, total_lp, reserve_source, ada_paired)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        ord,
                        pool_id,
                        obs.base_reserve,
                        obs.quote_reserve,
                        obs.fee_bps,
                        obs.total_lp,
                        obs.reserve_source.as_str(),
                        obs.ada_paired as i64
                    ],
                )?;
            }
        }
        Ok(())
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
        let mut caches = Caches {
            parties: &mut self.parties,
            units: &mut self.units,
            pools: &mut self.pools,
            next_tx_ord: &mut self.next_tx_ord,
        };
        Self::write_rows(&tx, &mut caches, txs)?;

        tx.execute(
            "INSERT INTO cursor (k, slot, block_hash) VALUES ('walk', ?1, ?2)
             ON CONFLICT(k) DO UPDATE SET slot = ?1, block_hash = ?2",
            params![slot as i64, block_hash.as_ref()],
        )?;

        if persist_buffer {
            tx.execute("DELETE FROM buffered", [])?;
            {
                let mut stmt = tx.prepare(
                    "INSERT INTO buffered
                         (oref_hash, oref_idx, address, stake, qty, units,
                          unlock_ts_ms, owner_pkh, datum_cbor, datum_hash)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                )?;
                for (oref, out) in buffer.entries() {
                    stmt.execute(params![
                        oref.0.as_ref(),
                        oref.1 as i64,
                        out.address,
                        out.stake,
                        // `qty` stays the TOTAL across units. Kept written so a
                        // rollback to a build without `units` reloads a buffer
                        // whose arithmetic is still right for a single-asset
                        // ledger, which is every deployed one.
                        out.total(),
                        encode_units(&out.units),
                        out.unlock_ts_ms.map(|v| v as i64),
                        out.owner_pkh,
                        out.datum_cbor,
                        out.datum_hash
                    ])?;
                }
            }
        }

        tx.commit()?;
        Ok(())
    }

    /// Derived balances at tip, non-zero only, descending. This is the
    /// projection the walk is verified against.
    pub fn balances(&self) -> Result<Vec<Balance>> {
        let mut stmt = self.conn.prepare(
            "SELECT p.address, p.stake, p.cohort, SUM(d.amount) AS bal
             FROM delta d JOIN party p USING (party_id)
             GROUP BY d.party_id HAVING bal <> 0
             ORDER BY bal DESC",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(Balance {
                address: r.get(0)?,
                stake: r.get(1)?,
                cohort: r.get(2)?,
                amount: r.get(3)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    // NOTE: the reverse walk's backfill (`commit_chunk`, `add_deltas`) and
    // its `pending_input` bookkeeping used to live here. The reverse walk no
    // longer touches sqlite at all — see `reverse.rs` and `archive.rs`; the
    // `pending_input` table is created for compatibility with ledgers that
    // already have it and is otherwise unused.

    /// What this ledger covers.
    pub fn coverage(&self) -> Result<Coverage> {
        Ok(Coverage {
            walked_from: self.walked_from()?,
            unresolved: self
                .conn
                .query_row("SELECT COUNT(*) FROM pending_input", [], |r| {
                    r.get::<_, i64>(0)
                })? as u64,
        })
    }

    /// Re-derive every party's cohort from its address.
    ///
    /// Idempotent and cheap, so it runs at the end of each walk and is also
    /// exposed as its own subcommand — registering a new sink or landing a new
    /// pool decoder should reclassify history without touching the chain.
    /// Returns the number of parties classified.
    pub fn classify_parties(&mut self, sinks: &[String], lock_creds: &[[u8; 28]]) -> Result<usize> {
        let pools = self.pool_addresses()?;
        let addresses: Vec<(i64, String)> = {
            let mut stmt = self.conn.prepare("SELECT party_id, address FROM party")?;
            let rows = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?;
            rows.collect::<Result<Vec<_>, _>>()?
        };

        let tx = self.conn.transaction()?;
        {
            let mut stmt =
                tx.prepare("UPDATE party SET cohort = ?2, basis = ?3 WHERE party_id = ?1")?;
            for (id, address) in &addresses {
                let c = crate::cohort::classify(address, sinks, &pools, lock_creds);
                stmt.execute(params![id, c.cohort.as_str(), c.basis])?;
            }
        }
        tx.commit()?;
        Ok(addresses.len())
    }

    /// Supply by cohort at tip — the cascade's input.
    pub fn cohort_totals(&self) -> Result<Vec<(String, i64, i64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT COALESCE(p.cohort, 'unclassified') AS c,
                    COUNT(*) AS n, SUM(bal) AS total
             FROM (SELECT party_id, SUM(amount) AS bal FROM delta
                   GROUP BY party_id HAVING bal <> 0) b
             JOIN party p USING (party_id)
             GROUP BY c ORDER BY total DESC",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Each pool's most recent reserves — the tip of the reserve curve.
    ///
    /// "Most recent" is by `tx_ord`, which is walk order and therefore chain
    /// order: `(slot, position in block)`. Ordering by slot alone would be
    /// ambiguous for two pool touches in the same block, and on a launch that
    /// is exactly when it matters.
    pub fn pool_tips(&self) -> Result<Vec<PoolTip>> {
        let mut stmt = self.conn.prepare(
            "SELECT p.dex, p.key_basis, s.base_reserve, s.quote_reserve, s.fee_bps
             FROM pool p
             JOIN pool_state s ON s.pool_id = p.pool_id
             WHERE s.ada_paired = 1
               AND s.tx_ord = (
                 SELECT MAX(tx_ord) FROM pool_state
                 WHERE pool_id = p.pool_id AND ada_paired = 1
             )
             ORDER BY s.quote_reserve DESC",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(PoolTip {
                dex: r.get(0)?,
                key_basis: r.get(1)?,
                base_reserve: r.get(2)?,
                quote_reserve: r.get(3)?,
                fee_bps: r.get(4)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Live lock positions at tip, and the tip's block time.
    ///
    /// Maturity is judged against the **ledger's** last block, not wall clock:
    /// a walk over a snapshot is as-of that snapshot, and dating it from the
    /// operator's laptop would silently mature positions the ledger has not
    /// yet seen unlock.
    pub fn locked_positions(&self) -> Result<LockSnapshot> {
        let mut stmt = self
            .conn
            .prepare("SELECT qty, unlock_ts_ms FROM buffered WHERE unlock_ts_ms IS NOT NULL")?;
        let rows = stmt.query_map([], |r| {
            Ok(LockPosition {
                qty: r.get(0)?,
                unlock_ts_ms: r.get::<_, i64>(1)? as u64,
            })
        })?;
        let positions = rows.collect::<Result<Vec<_>, _>>()?;
        let tip: Option<i64> = self
            .conn
            .query_row("SELECT MAX(block_time) FROM tx", [], |r| r.get(0))
            .optional()?
            .flatten();
        Ok(LockSnapshot {
            positions,
            tip_time: tip.map(|t| t as u64),
        })
    }

    /// Every lock ever opened, with when it opened and closed.
    pub fn lock_lifetimes(&self) -> Result<Vec<LockLifetime>> {
        let mut stmt = self.conn.prepare(
            "SELECT created_tx_ord, spent_tx_ord, qty, unlock_ts_ms
             FROM lock ORDER BY created_tx_ord",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(LockLifetime {
                created_tx_ord: r.get(0)?,
                spent_tx_ord: r.get(1)?,
                qty: r.get(2)?,
                unlock_ts_ms: r.get::<_, i64>(3)? as u64,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Every transaction in chain order.
    pub fn all_txs(&self) -> Result<Vec<TxRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT tx_ord, tx_hash, slot, block_time, net_mint FROM tx ORDER BY tx_ord",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(TxRecord {
                tx_ord: r.get(0)?,
                tx_hash: r.get(1)?,
                slot: r.get::<_, i64>(2)? as u64,
                block_time: r.get::<_, i64>(3)? as u64,
                net_mint: r.get(4)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Every movement in chain order: `(tx_ord, party_id, amount)`.
    pub fn all_movements(&self) -> Result<Vec<(i64, i64, i64)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT tx_ord, party_id, amount FROM delta ORDER BY tx_ord, party_id")?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Every party in `party_id` order.
    pub fn all_parties(&self) -> Result<Vec<PartyRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT party_id, address, stake, cohort, basis FROM party ORDER BY party_id",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(PartyRecord {
                party_id: r.get(0)?,
                address: r.get(1)?,
                stake: r.get(2)?,
                cohort: r.get(3)?,
                basis: r.get(4)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// The whole reserve curve in chain order.
    pub fn reserve_curve(&self) -> Result<Vec<CurvePoint>> {
        let mut stmt = self.conn.prepare(
            "SELECT s.tx_ord, p.dex, p.key_policy, p.key_name, p.key_basis,
                    s.fee_bps, s.base_reserve, s.quote_reserve
             FROM pool_state s JOIN pool p USING (pool_id)
             WHERE s.ada_paired = 1
             ORDER BY s.tx_ord, p.pool_id",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(CurvePoint {
                tx_ord: r.get(0)?,
                dex: r.get(1)?,
                key_policy: r.get(2)?,
                key_name: r.get(3)?,
                key_basis: r.get(4)?,
                fee_bps: r.get(5)?,
                base_reserve: r.get(6)?,
                quote_reserve: r.get(7)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Where supply went in the transactions that minted it.
    ///
    /// "Received a positive delta in a minting transaction" is a chain fact,
    /// not an inference, and it is the first question of any launch forensic:
    /// a token whose supply went overwhelmingly to one contract at mint had a
    /// launchpad, and one that went to a wallet did not.
    pub fn mint_distribution(&self) -> Result<Vec<(String, Option<String>, i64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT p.address, p.cohort, SUM(d.amount) AS got
             FROM tx t JOIN delta d USING (tx_ord) JOIN party p USING (party_id)
             WHERE t.net_mint > 0 AND d.amount > 0
             GROUP BY p.party_id ORDER BY got DESC",
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

    /// Live outputs at unnamed script addresses, with whatever datum they
    /// carried — the input to `probe`.
    pub fn unnamed_script_outputs(&self) -> Result<Vec<UnnamedOutput>> {
        let mut stmt = self.conn.prepare(
            "SELECT b.address, b.qty, b.datum_cbor, b.datum_hash
             FROM buffered b
             LEFT JOIN party p ON p.address = b.address
             WHERE COALESCE(p.cohort, '') = 'script'
             ORDER BY b.qty DESC",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(UnnamedOutput {
                address: r.get(0)?,
                qty: r.get(1)?,
                datum_cbor: r.get(2)?,
                datum_hash: r.get(3)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// `(pools, reserve-curve rows)`.
    pub fn pool_counts(&self) -> Result<(i64, i64)> {
        let pools: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM pool", [], |r| r.get(0))?;
        let states: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM pool_state", [], |r| r.get(0))?;
        Ok((pools, states))
    }

    /// Addresses that are pools, so a balance projection can separate pooled
    /// supply from the rest without re-deriving the classification.
    pub fn pool_addresses(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare("SELECT DISTINCT address FROM pool")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
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
