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

use std::collections::BTreeSet;
use std::path::Path;

use anyhow::{Context, Result};
use chain_ledger::{AliasKind, Frontier};
use mitos_chain_walk::decode::OutRef;
use pallas_primitives::Hash;
use rusqlite::{Connection, OptionalExtension, params};

use crate::activity::Activity;
use crate::state::{Buffer, BufferedOutput, Holders, Relays, WalkState};

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
    counterparties         INTEGER NOT NULL DEFAULT 0,
    -- Does the PROJECT own this wallet? Asserted by a human (registry
    -- (registry role = treasury, or `seed --project-side`), never derived: the
    -- chain cannot say who owns anything. It is the boundary every
    -- returned/unreconciled verdict is measured against, so a wrong 1 here
    -- launders an extraction into a deployment.
    project_side           INTEGER NOT NULL DEFAULT 0
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
-- policy_id: which collection sold. NULL on rows written before the column
-- existed, which were always the case's own policy. A row whose policy is NOT
-- the case's marks a watched party trading OTHER collections — ordinary
-- shopping the app hides by default, surfaceable because a marketplace hop is
-- also a way to move funds between two colluding wallets.
CREATE TABLE IF NOT EXISTS secondary_sale (
    tx_hash        TEXT NOT NULL,
    asset_name     TEXT NOT NULL,
    venue          TEXT NOT NULL,
    price_lovelace INTEGER,
    seller         TEXT,
    buyer          TEXT,
    slot           INTEGER NOT NULL,
    policy_id      TEXT,
    PRIMARY KEY (tx_hash, asset_name)
);

-- What a counterparty IS, where the chain can say so: a DEX pool, a batcher,
-- a marketplace contract, an aggregator.
--
-- Without this, value returning from a swap reads as project income. On Mekka
-- that was 74,626 of a supposed 132,590 ADA of unexplained inbound — money the
-- treasury had sent out moments earlier, coming back in a different unit. It is
-- the change-vs-receipt error one level up: a ROUND TRIP booked as revenue.
--
-- Interpretation, not observation, so it is recomputable without a re-walk and
-- `reset` clears it. `source` records WHERE the claim came from: an address
-- being Minswap is a claim inherited from a registry, not something this walk
-- observed.
-- WHO it is. `name` may be NULL: a wallet can be unmistakably an exchange hot
-- wallet by shape while which exchange it belongs to is unknowable from chain
-- data. Forcing a name would mean inventing one.
CREATE TABLE IF NOT EXISTS counterparty_kind (
    key    TEXT PRIMARY KEY,       -- party key as it appears in unit_flow
    name   TEXT,                   -- Minswap, bank.pillar, … NULL when unknown
    source TEXT NOT NULL           -- how it was decided
);

-- WHAT it does. One row per capability, because an entity commonly has
-- several: `bank.pillar` is a minting provider AND an airdrop payer, and the
-- single-label version of this table had to discard one of those facts.
--
-- `basis` is per-capability, not per-entity, because they arrive from different
-- evidence: `minting` is OBSERVED in a mint transaction's fund split, `dex` is
-- ASSERTED by an address registry, `cex` is DERIVED from fan-out shape. A
-- reader must be able to tell which claim is which.
CREATE TABLE IF NOT EXISTS counterparty_capability (
    key        TEXT NOT NULL,
    capability TEXT NOT NULL,      -- cex | dex | minting | airdrop | …
    basis      TEXT NOT NULL,      -- observed | derived | asserted
    source     TEXT NOT NULL,
    PRIMARY KEY (key, capability)
);
CREATE INDEX IF NOT EXISTS idx_cc_capability ON counterparty_capability(capability);

-- Interest engine tables — see PROJECT_LEDGER_INTEREST.md. All of it is
-- ATTENTION, never fact: recomputed wholesale by every `score` run, cleared by
-- `reset`, never exported as a figure. The signal rows are the evidence and
-- they SUM to the score — a hidden row would show up as arithmetic that
-- doesn't.
CREATE TABLE IF NOT EXISTS tx_signal (
    tx_hash TEXT NOT NULL,
    signal  TEXT NOT NULL,
    weight  REAL NOT NULL,
    basis   TEXT NOT NULL,          -- fixed per signal, stored for the reader
    detail  TEXT,                   -- human-readable: the WHY, verbatim
    PRIMARY KEY (tx_hash, signal)
);
CREATE TABLE IF NOT EXISTS tx_interest (
    tx_hash TEXT PRIMARY KEY,
    score   REAL NOT NULL,
    round   INTEGER NOT NULL        -- which propagation round settled it
);
CREATE INDEX IF NOT EXISTS idx_ti_score ON tx_interest(score);
CREATE TABLE IF NOT EXISTS party_signal (
    key    TEXT NOT NULL,
    signal TEXT NOT NULL,
    weight REAL NOT NULL,
    basis  TEXT NOT NULL,
    detail TEXT,
    PRIMARY KEY (key, signal)
);
CREATE TABLE IF NOT EXISTS party_interest (
    key                  TEXT PRIMARY KEY,
    score                REAL NOT NULL,
    -- The tool's PROPOSAL, never an assertion: class from the app vocabulary
    -- (core|associate|customer), capability from the provider vocabulary.
    -- Confidence is capped below `confirmed`, which is reserved for humans.
    proposed_class       TEXT,
    proposed_capability  TEXT,
    proposed_confidence  TEXT
);
CREATE INDEX IF NOT EXISTS idx_pi_score ON party_interest(score);
-- Assertion never silences evidence; evidence never overrides assertion.
-- When they conflict, BOTH sides go here and a human decides.
CREATE TABLE IF NOT EXISTS disagreement (
    key        TEXT NOT NULL,
    kind       TEXT NOT NULL,       -- classification | evidence
    human_says TEXT NOT NULL,
    tool_says  TEXT NOT NULL,
    severity   REAL NOT NULL,       -- interest x tool confidence
    detail     TEXT,
    PRIMARY KEY (key, kind)
);
-- Weights, window, rounds, and a fingerprint of the annotation state each run
-- consumed — a re-tune must be visible, not a silent reinterpretation.
CREATE TABLE IF NOT EXISTS score_meta (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
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

-- Handles seen on TRACKED wallets during the genesis resolve scan.
--
-- KEPT through reset, like the rest of the resolution layer. The walk's own
-- alias pass only sees handles that MOVE inside the walk window — a wallet
-- holding its handle quietly since before the floor is never named (dwess
-- held $dwess_art through the whole S2 window and stayed anonymous). The
-- genesis scan reads every block anyway; harvesting handle sightings there
-- costs no extra IO, and `seed` re-emits them as party aliases.
CREATE TABLE IF NOT EXISTS discovered_handle (
    party       TEXT    NOT NULL,
    handle      TEXT    NOT NULL,
    first_slot  INTEGER NOT NULL,
    PRIMARY KEY (party, handle)
);

-- Stake keys seen RECEIVING a holder-facing asset of the tracked policy — the
-- collection's holders, discovered by walking.
--
-- KEPT through reset, like the resolution layer, and for the same reason: it
-- is what the next walk is for. A holder discovered mid-walk was seated too
-- late to book their earlier transactions — on a queued mint the PAYMENT
-- precedes the fulfilment, so the purchase leg is exactly what a first pass
-- misses. The re-walk seeds these from the floor and books it.
CREATE TABLE IF NOT EXISTS discovered_holder (
    key         TEXT    NOT NULL PRIMARY KEY,
    first_slot  INTEGER NOT NULL    -- slot of the first receipt seen
);

-- A watched party paid a bare address that swept the money onward within
-- minutes. The trail would otherwise STOP at that address.
--
-- This is the single-use exchange deposit pattern: a fresh stakeless address
-- with exactly one receipt and one spend. Left unfollowed it reads as an
-- off-ramp (`classify` infers exactly that from the one-way shape), which
-- says 'money left the chain here' and quietly discards the one fact that
-- matters -- WHERE it went. Following one hop turns four anonymous exits into
-- four deposits at one identifiable service.
--
-- One hop only, and the relay is NEVER promoted: this adds depth to the trail
-- without adding breadth to the watch set. `to_addr` is recorded, not
-- watched -- a sweep target is typically a custodial hot wallet, and seating
-- one would recruit thousands of unrelated wallets.
CREATE TABLE IF NOT EXISTS relay_hop (
    relay_addr  TEXT    NOT NULL,   -- the single-use address in the middle
    from_party  TEXT    NOT NULL,   -- watched party that funded it
    to_addr     TEXT    NOT NULL,   -- where the sweep actually landed
    unit        TEXT    NOT NULL,
    quantity    INTEGER NOT NULL,   -- as swept onward, not as received
    in_tx       TEXT    NOT NULL,
    out_tx      TEXT    NOT NULL,
    in_slot     INTEGER NOT NULL,
    out_slot    INTEGER NOT NULL,   -- out_slot - in_slot is the dwell time
    PRIMARY KEY (relay_addr, unit, out_tx)
);
CREATE INDEX IF NOT EXISTS idx_relay_from ON relay_hop(from_party);
CREATE INDEX IF NOT EXISTS idx_relay_to   ON relay_hop(to_addr);

-- The BALANCE-SHEET side: assets of OTHER policies arriving at a wallet the
-- project owns.
--
-- The walk records every unit of VALUE but only this policy's ASSETS, so a
-- deployment paid in ADA and returned in someone else's NFTs has its
-- departure captured and its arrival missing — and an honest allocation
-- renders exactly like an extraction. Measured on Octaverse: 6,000 ADA out,
-- 62 Mekka S2 back to the project's holding wallet inside 35 minutes, and the
-- ledger held only the outflow.
--
-- Bounded to project-side parties ON PURPOSE. 'Every asset everywhere' is the
-- frontier explosion in a new dimension — one wallet in that case touched 12
-- policies and a busy one touches hundreds — but the wallets a project owns
-- are a curated handful, so this costs almost nothing.
CREATE TABLE IF NOT EXISTS asset_inflow (
    party       TEXT    NOT NULL,   -- the project-side wallet that received
    policy_id   TEXT    NOT NULL,   -- FOREIGN policy: never this project's
    asset_name  TEXT    NOT NULL,   -- hex
    quantity    INTEGER NOT NULL,
    from_party  TEXT,               -- NULL when the inputs were unresolved
    tx_hash     TEXT    NOT NULL,
    slot        INTEGER NOT NULL,
    block_time  INTEGER NOT NULL,
    PRIMARY KEY (tx_hash, party, policy_id, asset_name)
);
CREATE INDEX IF NOT EXISTS idx_ai_party  ON asset_inflow(party);
CREATE INDEX IF NOT EXISTS idx_ai_policy ON asset_inflow(policy_id);
CREATE INDEX IF NOT EXISTS idx_ai_slot   ON asset_inflow(slot);
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
    // `counterparty_kind` carried a single `kind` string before capabilities
    // existed. DROP rather than ALTER: the table is pure interpretation,
    // rebuilt by `classify` in seconds, and its old shape cannot represent an
    // entity with two functions — so there is nothing in it worth migrating.
    // Dropping is also what makes the new `name` column appear at all, since
    // `CREATE TABLE IF NOT EXISTS` is a no-op on an existing table.
    if has_column(conn, "counterparty_kind", "kind")? {
        conn.execute_batch("DROP TABLE counterparty_kind")?;
        conn.execute_batch(SCHEMA)?;
        tracing::info!(
            "store: migrated counterparty_kind to name + capabilities — re-run `classify`"
        );
    }
    for (table, column, decl) in [
        (
            "unit_flow",
            "min_utxo",
            "min_utxo INTEGER NOT NULL DEFAULT 0",
        ),
        // NULL on pre-existing rows = the case's own policy (see SCHEMA).
        ("secondary_sale", "policy_id", "policy_id TEXT"),
        // 0 on pre-existing rows is the SAFE default: a ledger walked before
        // project-side existed has never declared one, and defaulting to 1
        // would silently claim every watched wallet is the project's.
        (
            "party",
            "project_side",
            "project_side INTEGER NOT NULL DEFAULT 0",
        ),
    ] {
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

/// One counterparty's identity and capabilities, as `classify` establishes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CounterpartyRow {
    pub key: String,
    /// `None` when the shape is unmistakable but the identity is not.
    pub name: Option<String>,
    /// `(capability, basis)` — basis is per-capability because they come from
    /// different evidence. See the `counterparty_capability` schema comment.
    pub capabilities: Vec<(chain_ledger::ProviderCapability, chain_ledger::Basis)>,
    pub source: String,
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
    /// Which collection sold. `None` only on rows predating the column —
    /// those were always the case's own policy.
    pub policy_id: Option<String>,
}

/// Tables a `reset` clears: everything a walk derives and would re-derive.
pub const RESET_DERIVED_TABLES: [&str; 25] = [
    // Derived: the next walk reads the same blocks and re-records it. Keeping
    // it would let an inflow captured under an OLD project-side set survive a
    // re-walk that would no longer draw it.
    "asset_inflow",
    // Derived, not kept: a relay hop is reconstructed by the next walk from
    // the same blocks. Keeping it would let a pass-through inferred under an
    // old window survive a re-walk that would no longer draw it.
    "relay_hop",
    "counterparty_kind",
    "counterparty_capability",
    "tx_signal",
    "tx_interest",
    "party_signal",
    "party_interest",
    "disagreement",
    "score_meta",
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

/// Tables a `reset` KEEPS: the input-resolution layer plus the discovered
/// holder and handle sets — all bought by scanning rather than derived, and
/// all what the NEXT walk is there to use.
pub const RESET_KEPT_TABLES: [&str; 4] = [
    "outref_cache",
    "wanted_outref",
    "discovered_holder",
    "discovered_handle",
];

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

/// `(address, distinct payers, legs out, legs back, lovelace in)` — the raw
/// measurement behind the off-ramp verdict. See [`Ledger::stakeless_exit_shapes`].
pub type ExitShape = (String, u32, u32, u32, i128);

/// One confirmed pass-through: a watched party's money, seen leaving the bare
/// address it was sent to. See the `relay_hop` table comment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayHopRow {
    pub relay_addr: String,
    pub from_party: String,
    pub to_addr: String,
    pub unit: String,
    pub quantity: i64,
    pub in_tx: String,
    pub out_tx: String,
    pub in_slot: u64,
    pub out_slot: u64,
}

/// A row of `asset_inflow` — a FOREIGN policy's asset arriving at a wallet
/// the project owns. The return leg the walk could not otherwise see.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetInflowRow {
    pub party: String,
    pub policy_id: String,
    pub asset_name: String,
    pub quantity: i64,
    pub from_party: Option<String>,
    pub tx_hash: String,
    pub slot: u64,
    pub block_time: i64,
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

    /// Raw connection, for the `score` post-pass.
    ///
    /// Scoring is ~fifteen read queries plus a bulk write, all of it
    /// recomputed wholesale per run; giving each query a named method here
    /// would bloat the walk-time store with display-adjacent concerns. The
    /// walk's own writes stay behind the typed methods.
    pub(crate) fn connection(&mut self) -> &mut Connection {
        &mut self.conn
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

    pub fn insert_relay_hops(&mut self, rows: &[RelayHopRow]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let tx = self.conn.transaction()?;
        let mut n = 0;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO relay_hop
                 (relay_addr, from_party, to_addr, unit, quantity,
                  in_tx, out_tx, in_slot, out_slot)
                 VALUES (?,?,?,?,?,?,?,?,?)",
            )?;
            for r in rows {
                n += stmt.execute(params![
                    r.relay_addr,
                    r.from_party,
                    r.to_addr,
                    r.unit,
                    r.quantity,
                    r.in_tx,
                    r.out_tx,
                    u64_i64(r.in_slot),
                    u64_i64(r.out_slot),
                ])?;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    pub fn insert_asset_inflows(&mut self, rows: &[AssetInflowRow]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let tx = self.conn.transaction()?;
        let mut n = 0;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO asset_inflow
                 (party, policy_id, asset_name, quantity, from_party,
                  tx_hash, slot, block_time)
                 VALUES (?,?,?,?,?,?,?,?)",
            )?;
            for r in rows {
                n += stmt.execute(params![
                    r.party,
                    r.policy_id,
                    r.asset_name,
                    r.quantity,
                    r.from_party,
                    r.tx_hash,
                    u64_i64(r.slot),
                    r.block_time,
                ])?;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    /// Mark a party as owned by the project — the boundary every
    /// returned/unreconciled verdict is measured against.
    ///
    /// An UPDATE, like [`Ledger::label_party`], and for the same reason: the
    /// `party` table is a projection the checkpoint writes, so `seed` must
    /// checkpoint FIRST and assert afterwards. The checkpoint's own upsert
    /// deliberately does not list `project_side`, so a later walk cannot
    /// clear an assertion a human made.
    pub fn set_project_side(&self, key: &str, source: &str) -> Result<usize> {
        Ok(self.conn.execute(
            "UPDATE party SET project_side = 1, source = COALESCE(source, ?) WHERE key = ?",
            params![source, key],
        )?)
    }

    /// Every wallet the project owns. The walk reads this once at start-up to
    /// decide whose incoming foreign assets are worth recording.
    pub fn project_side_parties(&self) -> Result<BTreeSet<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT key FROM party WHERE project_side = 1")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
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

    /// Record what counterparties ARE — identity and capabilities.
    ///
    /// Capabilities are written ADDITIVELY (`INSERT OR IGNORE`), never as a
    /// replacement set. They arrive from different evidence in different
    /// passes — `minting` from a mint's fund split, `dex` from a registry,
    /// `cex` from fan-out shape — and a later pass that knows one fact must not
    /// erase a fact an earlier one established. That erasure is precisely what
    /// the single-label table did to a provider with two functions.
    ///
    /// The name IS replaced, because a better name supersedes a worse one and
    /// there is only ever one.
    pub fn put_counterparty(&mut self, rows: &[CounterpartyRow]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let tx = self.conn.transaction()?;
        let mut n = 0;
        {
            let mut who = tx.prepare_cached(
                "INSERT INTO counterparty_kind (key, name, source) VALUES (?,?,?)
                 ON CONFLICT(key) DO UPDATE SET
                   name = COALESCE(excluded.name, counterparty_kind.name),
                   source = excluded.source",
            )?;
            let mut what = tx.prepare_cached(
                "INSERT OR IGNORE INTO counterparty_capability
                 (key, capability, basis, source) VALUES (?,?,?,?)",
            )?;
            for r in rows {
                who.execute(params![r.key, r.name, r.source])?;
                for (cap, basis) in &r.capabilities {
                    n += what.execute(params![r.key, cap.as_str(), basis.as_str(), r.source])?;
                }
                if r.capabilities.is_empty() {
                    n += 1;
                }
            }
        }
        tx.commit()?;
        Ok(n)
    }

    /// Drop every counterparty identity and capability, for `classify` to
    /// rebuild from scratch.
    ///
    /// Safe because classify is these tables' ONLY writer and everything it
    /// writes is derived from the ledger plus the address registry. The
    /// additive design of [`put_counterparty`](Self::put_counterparty) is for
    /// passes WITHIN a run accruing different evidence; between runs it means
    /// a fixed rule cannot retract a broken rule's rows, which is how 168
    /// customers stayed named "Splash" until this existed.
    pub fn reset_counterparties(&mut self) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute("DELETE FROM counterparty_capability", [])?;
        tx.execute("DELETE FROM counterparty_kind", [])?;
        tx.commit()?;
        Ok(())
    }

    /// Every distinct counterparty in `unit_flow`, with the addresses it is
    /// known by — the registry keys on ADDRESSES, but a staked party's key is
    /// its stake credential, so the addresses come from `party_alias`.
    pub fn counterparties_with_addresses(&self) -> Result<Vec<(String, Vec<String>)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT DISTINCT counterparty FROM unit_flow WHERE counterparty <> ''")?;
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

    /// Parties the project's own MINT transactions paid — mint providers.
    ///
    /// Derived, not asserted: a `mint_payment` row is read off the mint tx's
    /// fund split. This is what separates a service the project USED from an
    /// exchange it merely passed money through, and the difference is not
    /// cosmetic — a mint provider's onward payments (airdrops, artist splits)
    /// ARE project activity, while everything beyond an exchange is somebody
    /// else's business.
    /// Per-destination mint fund-split totals: `(key, lovelace, distinct
    /// mint txs)` — the input to the dominant-destination rule.
    pub fn mint_payment_totals(&self) -> Result<Vec<(String, i128, u32)>> {
        let mut stmt = self.conn.prepare(
            "SELECT destination, SUM(lovelace), COUNT(DISTINCT tx_hash)
             FROM mint_payment WHERE destination <> '' GROUP BY destination",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                i128::from(r.get::<_, i64>(1)?),
                r.get::<_, i64>(2)? as u32,
            ))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("reading mint payment totals")
    }

    pub fn mint_payment_destinations(&self) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT DISTINCT destination FROM mint_payment WHERE destination <> ''")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("reading mint payment destinations")
    }

    /// What a counterparty is established to DO, with the basis of each claim.
    ///
    /// Read surface for consumers (the app charts by capability), so it is not
    /// dead merely because this binary only writes.
    #[allow(dead_code)]
    pub fn capabilities_of(
        &self,
        key: &str,
    ) -> Result<Vec<(chain_ledger::ProviderCapability, chain_ledger::Basis)>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT capability, basis FROM counterparty_capability
             WHERE key = ? ORDER BY capability",
        )?;
        let rows = stmt.query_map([key], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (cap, basis) = row?;
            // An unparseable capability is an ERROR, not a skip: it means the
            // vocabulary moved on and a reader is being shown less than the
            // data holds. The basis degrades to `Asserted` instead, because
            // there the cautious direction is downward.
            let cap = cap
                .parse::<chain_ledger::ProviderCapability>()
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            out.push((cap, chain_ledger::Basis::parse(&basis)));
        }
        Ok(out)
    }

    /// The name a counterparty is known by, if anything has established one.
    ///
    /// `None` is a real answer, not a gap: a wallet can be unmistakably a CEX
    /// hot wallet by shape while which exchange it belongs to is unknowable.
    #[allow(dead_code)]
    pub fn counterparty_name(&self, key: &str) -> Result<Option<String>> {
        Ok(self
            .conn
            .query_row(
                "SELECT name FROM counterparty_kind WHERE key = ?",
                [key],
                |r| r.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten())
    }

    /// `(distinct wallets paid, distinct wallets paid BY)` for a counterparty.
    ///
    /// The asymmetry is the whole signal. An exchange hot wallet pays thousands
    /// and receives from a handful, because customer deposits land on per-user
    /// addresses and the hot wallet is replenished from cold storage. A busy
    /// project wallet, by contrast, has traffic in both directions.
    ///
    /// Every watched party's key — the set whose fan-out is measurable.
    pub fn party_keys(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare("SELECT key FROM party")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("reading party keys")
    }

    /// **Only measurable for a WATCHED PARTY.** Returns `None` otherwise, and
    /// that distinction is the whole correctness of the rule.
    ///
    /// For a party, `unit_flow` holds every flow it had, so counting distinct
    /// counterparties gives its true fan-out — 2,390 and 3,081 on the two real
    /// hot wallets. For a mere counterparty we see ONLY its dealings with our
    /// watch set, so the count is bounded by the number of watched parties
    /// (178 here) and cannot be compared to a threshold in the hundreds.
    ///
    /// The first version of this counted distinct `party` for a given
    /// counterparty, which is exactly that bounded quantity. It could never
    /// reach the threshold, so the CEX rule silently matched nothing and the
    /// empty result read as "no exchanges found" rather than "not measurable".
    /// `None` now says which of those it is.
    pub fn fan_shape(&self, key: &str) -> Result<Option<(u64, u64)>> {
        let is_party: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM party WHERE key = ?)",
            [key],
            |r| r.get(0),
        )?;
        if !is_party {
            return Ok(None);
        }
        let mut stmt = self.conn.prepare_cached(
            "SELECT
               COUNT(DISTINCT CASE WHEN quantity < 0 THEN counterparty END),
               COUNT(DISTINCT CASE WHEN quantity > 0 THEN counterparty END)
             FROM unit_flow WHERE party = ? AND unit = 'lovelace' AND counterparty <> ''",
        )?;
        let (paid, paid_by): (i64, i64) = stmt.query_row([key], |r| Ok((r.get(0)?, r.get(1)?)))?;
        Ok(Some((paid.max(0) as u64, paid_by.max(0) as u64)))
    }

    /// Counterparties busy enough to be a service, that nothing has named yet.
    ///
    /// Ordered by volume so the report reads worst-first. Excludes anything
    /// already in `counterparty_kind`: a registry claim is evidence about WHO,
    /// and must not be overwritten by an inference about WHAT.
    pub fn busy_unnamed_counterparties(&self, min_flows: u64) -> Result<Vec<(String, u64, i64)>> {
        let mut stmt = self.conn.prepare(
            // Volume sums ONLY lovelace. A token's raw quantity can be
            // astronomical (18 decimals is common), and summing ABS across all
            // units over hundreds of thousands of rows overflows i64 — at which
            // point sqlite quietly promotes the result to REAL and every row
            // fails to decode as an integer. Activity is counted across all
            // units; only the ADA figure is summed.
            "SELECT f.counterparty, COUNT(*) AS n,
                    SUM(CASE WHEN f.unit = 'lovelace' THEN ABS(f.quantity) ELSE 0 END) AS vol
             FROM unit_flow f
             WHERE f.counterparty <> ''
               AND NOT EXISTS (SELECT 1 FROM counterparty_kind k WHERE k.key = f.counterparty)
             GROUP BY f.counterparty HAVING n >= ? ORDER BY vol DESC",
        )?;
        let rows = stmt.query_map([min_flows as i64], |r| {
            let key: String = r.get(0)?;
            let flows: i64 = r.get(1)?;
            let volume: i64 = r.get(2)?;
            Ok((key, flows as u64, volume))
        })?;
        // Errors PROPAGATE. `filter_map(Result::ok)` here returned an empty set
        // from a query that matched 102 rows, and reported success — a silent
        // drop is indistinguishable from "nothing matched", which is exactly
        // the failure this tool exists to avoid making elsewhere.
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("reading busy unnamed counterparties")
    }

    /// Stakeless counterparties with the OFF-RAMP candidate shape, one row
    /// per address: how many watched wallets ever paid it, how many payment
    /// legs, whether anything EVER came back, and the total that left.
    ///
    /// The verdict (few payers, nothing back, enough legs) belongs to
    /// `classify`; this just measures. One-way-ness is measured over BOOKED
    /// rows only — for an unwatched address that is exactly "no watched
    /// wallet ever received from it", which is the strongest claim an
    /// offline ledger can make.
    pub fn stakeless_exit_shapes(&self, min_legs: u32) -> Result<Vec<ExitShape>> {
        let mut stmt = self.conn.prepare(
            "SELECT counterparty,
                    COUNT(DISTINCT CASE WHEN quantity < 0 THEN party END),
                    SUM(CASE WHEN quantity < 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN quantity > 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN quantity < 0 THEN -quantity ELSE 0 END)
             FROM unit_flow
             WHERE counterparty LIKE 'addr1%' AND unit = 'lovelace'
             GROUP BY counterparty
             HAVING SUM(CASE WHEN quantity < 0 THEN 1 ELSE 0 END) >= ?1",
        )?;
        let rows = stmt.query_map([min_legs], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)? as u32,
                r.get::<_, i64>(2)? as u32,
                r.get::<_, i64>(3)? as u32,
                i128::from(r.get::<_, i64>(4)?),
            ))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("reading stakeless exit shapes")
    }

    /// Addresses confirmed to be pass-throughs by the walk — the middle of a
    /// `relay_hop`. These must never be read as destinations.
    pub fn relay_addresses(&self) -> Result<std::collections::BTreeSet<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT DISTINCT relay_addr FROM relay_hop")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<_>>()
            .context("reading relay addresses")
    }

    /// Where the relays swept TO, ranked by how many distinct single-use
    /// addresses fed each one.
    ///
    /// One address collecting from many fresh deposit addresses is the
    /// custodial sweep pattern — it is the exchange, and the relays were its
    /// per-customer doors. Two separate relays landing on the same wallet is
    /// already more than coincidence; the threshold is the caller's.
    pub fn relay_sweep_targets(&self, min_relays: u32) -> Result<Vec<(String, u32, u32, i128)>> {
        let mut stmt = self.conn.prepare(
            "SELECT to_addr,
                    COUNT(DISTINCT relay_addr),
                    COUNT(DISTINCT from_party),
                    SUM(quantity)
             FROM relay_hop
             WHERE unit = 'lovelace'
             GROUP BY to_addr
             HAVING COUNT(DISTINCT relay_addr) >= ?1
             ORDER BY SUM(quantity) DESC",
        )?;
        let rows = stmt.query_map([min_relays], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)? as u32,
                r.get::<_, i64>(2)? as u32,
                i128::from(r.get::<_, i64>(3)?),
            ))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("reading relay sweep targets")
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
            let key: String = r.get(0)?;
            let flows: i64 = r.get(1)?;
            let volume: i64 = r.get(2)?;
            Ok((key, flows as u64, volume))
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
                 (tx_hash, asset_name, venue, price_lovelace, seller, buyer, slot, policy_id)
                 VALUES (?,?,?,?,?,?,?,?)",
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
                    r.policy_id,
                ])?;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    // --- discovered holders ---------------------------------------------------

    /// Record holders seen receiving the collection's holder-facing assets.
    /// `INSERT OR IGNORE` keeps the FIRST sighting's slot — coverage widens,
    /// never narrows.
    pub fn put_discovered_holders(&mut self, rows: &[(String, u64)]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let tx = self.conn.transaction()?;
        let mut n = 0;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO discovered_holder (key, first_slot) VALUES (?,?)",
            )?;
            for (key, slot) in rows {
                n += stmt.execute(params![key, u64_i64(*slot)])?;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    /// Every holder a previous walk discovered — `seed` seats these from the
    /// floor so the re-walk books their PRE-purchase legs too.
    pub fn discovered_holders(&self) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT key FROM discovered_holder ORDER BY key")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("reading discovered holders")
    }

    /// Record handle sightings from the genesis scan. First sighting's slot
    /// wins — the alias is "this wallet has gone by this name".
    pub fn put_discovered_handles(&mut self, rows: &[(String, String, u64)]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let tx = self.conn.transaction()?;
        let mut n = 0;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO discovered_handle (party, handle, first_slot)
                 VALUES (?,?,?)",
            )?;
            for (party, handle, slot) in rows {
                n += stmt.execute(params![party, handle, u64_i64(*slot)])?;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    /// Handle sightings for `seed` to re-emit as party aliases after a reset.
    pub fn discovered_handles(&self) -> Result<Vec<(String, String, u64)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT party, handle, first_slot FROM discovered_handle ORDER BY party")?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)? as u64,
            ))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("reading discovered handles")
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

        // Relay candidates are deliberately NOT checkpointed: an unswept
        // candidate expires within the window anyway, so a resume that starts
        // with none loses at most the relays straddling the checkpoint, and
        // persisting them would let a stale pass-through survive a re-walk.
        Ok(Some(WalkState {
            frontier,
            buffer,
            activity,
            holders,
            relays: Relays::default(),
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
        chain_ledger::Role::Holder => "holder",
    }
}

pub fn reason_str(r: chain_ledger::TerminalReason) -> &'static str {
    match r {
        chain_ledger::TerminalReason::Stakeless => "stakeless",
        chain_ledger::TerminalReason::Receipts => "receipts",
        chain_ledger::TerminalReason::Counterparties => "counterparties",
        chain_ledger::TerminalReason::Declared => "declared",
        chain_ledger::TerminalReason::Payees => "payees",
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
            relays: Relays::default(),
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

    /// The Mekka compounding shape, in miniature: four throwaway addresses,
    /// one destination. One relay alone must NOT raise a sweep target — that
    /// is just a payment — but the set of them must.
    #[test]
    fn relays_landing_on_one_wallet_surface_it_as_a_sweep_target() {
        let mut l = Ledger::open_in_memory().unwrap();
        let hop = |relay: &str, to: &str, q: i64| RelayHopRow {
            relay_addr: relay.into(),
            from_party: "stake1compounding".into(),
            to_addr: to.into(),
            unit: "lovelace".into(),
            quantity: q,
            in_tx: format!("in{relay}"),
            out_tx: format!("out{relay}"),
            in_slot: 100,
            out_slot: 200,
        };
        l.insert_relay_hops(&[
            hop("addr1vr1", "addr1vswap", 7_500_000_000),
            hop("addr1vr2", "addr1vswap", 4_000_000_000),
            hop("addr1vr3", "addr1vswap", 1_800_000_000),
            hop("addr1vr4", "addr1vlonely", 800_000_000),
        ])
        .unwrap();

        let targets = l.relay_sweep_targets(2).unwrap();
        assert_eq!(targets.len(), 1, "one destination clears the bar, not two");
        let (key, relays, parties, total) = &targets[0];
        assert_eq!(key, "addr1vswap");
        assert_eq!(*relays, 3);
        assert_eq!(*parties, 1);
        assert_eq!(*total, 13_300_000_000);

        assert_eq!(
            l.relay_addresses().unwrap().len(),
            4,
            "every confirmed pass-through is known, including the lonely one"
        );
    }

    #[test]
    fn a_relay_hop_is_idempotent_on_its_key() {
        let mut l = Ledger::open_in_memory().unwrap();
        let row = RelayHopRow {
            relay_addr: "addr1vr1".into(),
            from_party: "stake1c".into(),
            to_addr: "addr1vswap".into(),
            unit: "lovelace".into(),
            quantity: 7_500_000_000,
            in_tx: "in".into(),
            out_tx: "out".into(),
            in_slot: 100,
            out_slot: 200,
        };
        assert_eq!(l.insert_relay_hops(&[row.clone(), row.clone()]).unwrap(), 1);
        assert_eq!(l.insert_relay_hops(&[row]).unwrap(), 0);
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

    /// `project_side` is a HUMAN assertion about who owns a wallet, and the
    /// checkpoint's party upsert deliberately does not list it — so a walk
    /// cannot clear what a curator declared. It must also survive `reset`,
    /// which is a prelude to the re-walk that spends it.
    #[test]
    fn a_project_side_assertion_survives_the_walk_that_rewrites_the_party_row() {
        let l = Ledger::open_in_memory().unwrap();
        l.conn
            .execute(
                "INSERT INTO party (key, has_stake, role, watched_from_slot, expand)
                 VALUES ('stake1treasury', 1, 'declared', 100, 1)",
                [],
            )
            .unwrap();
        assert_eq!(l.project_side_parties().unwrap().len(), 0, "0 by default");

        assert_eq!(l.set_project_side("stake1treasury", "registry").unwrap(), 1);
        assert!(l.project_side_parties().unwrap().contains("stake1treasury"));

        // The checkpoint's own upsert: every frontier-derived column, and
        // NOT project_side.
        l.conn
            .execute(
                "INSERT INTO party (key, has_stake, role, watched_from_slot, expand)
                 VALUES ('stake1treasury', 1, 'promoted', 200, 0)
                 ON CONFLICT(key) DO UPDATE SET role = excluded.role, expand = excluded.expand",
                [],
            )
            .unwrap();
        assert!(
            l.project_side_parties().unwrap().contains("stake1treasury"),
            "a re-walk must not silently un-declare the project's own wallet"
        );
    }

    /// Naming a party that has no row is a NO-OP, and the caller is told so —
    /// `--project-side` on an unseated wallet must not look like it worked.
    #[test]
    fn marking_an_unseated_party_reports_zero_rather_than_inventing_one() {
        let l = Ledger::open_in_memory().unwrap();
        assert_eq!(l.set_project_side("stake1nobody", "cli").unwrap(), 0);
        assert!(l.project_side_parties().unwrap().is_empty());
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
