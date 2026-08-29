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
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};

use crate::report;

pub fn open_rw(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    // Two scan lanes now write here. WAL serialises writers, so an overlap is
    // ordinary rather than exceptional — wait for the lock instead of failing
    // a sweep that has already read gigabytes of chain.
    conn.busy_timeout(std::time::Duration::from_secs(30))?;
    migrate(&conn)?;
    Ok(conn)
}

/// Schema + every idempotent migration, split out from [`open_rw`] so tests
/// can raise a real cache on an in-memory connection rather than a temp file.
fn migrate(conn: &Connection) -> Result<()> {
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
    // `deep_pending` is GONE, superseded by `scanned_from_slot`. Dropped
    // rather than left in place: a deployed cache carries values written by
    // the buggy incremental that never backfilled, so the column is not
    // merely redundant — it is actively wrong, and the next reader to reach
    // for it would repeat the misread that returned 898 rows instead of
    // 3,235. "no such column" means it was already gone.
    if let Err(e) = conn.execute("ALTER TABLE wallet DROP COLUMN deep_pending", []) {
        let msg = e.to_string();
        if !msg.contains("no such column") {
            return Err(e).context("dropping wallet.deep_pending");
        }
    }
    // An explicit ALTER, like every column above it: `CREATE TABLE IF NOT
    // EXISTS` silently does nothing on an existing table, so a new column in
    // the CREATE alone would be missing on every deployed cache.
    if let Err(e) = conn.execute("ALTER TABLE flow ADD COLUMN asset_total INTEGER", []) {
        let msg = e.to_string();
        if !msg.contains("duplicate column") {
            return Err(e).context("adding flow.asset_total");
        }
    }
    for (table, col, ty) in [
        ("flow", "locked", "INTEGER"),
        ("flow", "unlocked", "INTEGER"),
        ("owned", "script", "INTEGER"),
        ("flow", "locked_assets", "TEXT"),
        // How far BACK this wallet has been scanned. See [`ScanTarget`] — the
        // now-dropped `deep_pending` boolean it replaced could only say
        // "shallow or not", which is why the only remedy it could express was
        // a full 219 GB sweep.
        //
        // NULL on rows written before this column existed: depth unknown, so
        // the next request that needs depth re-establishes it. Assuming
        // "already deep" would permanently strand exactly the wallets this
        // change exists to repair.
        ("wallet", "scanned_from_slot", "INTEGER"),
        // When a CLIENT last asked for this wallet — the eviction signal.
        //
        // Deliberately NOT `updated_unix`: the refresh watcher re-stores every
        // cached wallet whenever chain coverage advances, roughly every twenty
        // minutes, so `updated_unix` is near-identical across the whole table
        // and orders nothing. It measures our own housekeeping, not anybody's
        // interest. NULL means never asked for since the column existed, which
        // sorts first and so is evicted first — correct, since a wallet nobody
        // has requested is the cheapest thing to lose.
        ("wallet", "last_seen_unix", "INTEGER"),
        // Rows held when the backfill gave up on this wallet, or NULL if it
        // never did. See `MAX_WALLET_ROWS` in `jobs`.
        //
        // Persisted so the verdict survives the scan that reached it. This is
        // the "blacklist", except nothing is curated: it populates itself from
        // measured row counts, so a rotated exchange address is caught on its
        // own merits rather than needing to be recognised.
        ("wallet", "oversize_rows", "INTEGER"),
    ] {
        if let Err(e) = conn.execute(&format!("ALTER TABLE {table} ADD COLUMN {col} {ty}"), []) {
            let msg = e.to_string();
            if !msg.contains("duplicate column") {
                return Err(e).context(format!("adding {table}.{col}"));
            }
        }
    }
    Ok(())
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

/// How far back a scan has gone, or is being asked to go.
///
/// Replaces a `deep_pending: bool`, which could only distinguish "shallow"
/// from "not" — so the only backfill it could express was all the way to
/// genesis. With tiers at 30/90/182/365 days a 90-day reader was made to pay
/// for a 219 GB sweep because 3 GB and 219 GB were the only two options.
///
/// Depth is stored as the oldest slot covered, so "does what we hold satisfy
/// what is being asked for?" is one comparison rather than a special case per
/// tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanTarget {
    /// Covered back to this slot, inclusive.
    Since(u64),
    /// Covered to the start of the chain — nothing older exists to fetch.
    Genesis,
}

impl ScanTarget {
    /// The oldest slot this target covers.
    pub fn floor(self) -> u64 {
        match self {
            Self::Since(slot) => slot.max(crate::excavate::SHELLEY_START_SLOT),
            Self::Genesis => crate::excavate::SHELLEY_START_SLOT,
        }
    }

    /// Does what we already hold satisfy `wanted`?
    ///
    /// Deeper coverage is a LOWER floor, so this is `<=` — the direction is
    /// easy to invert, and inverting it would make every request look already
    /// satisfied and nothing would ever backfill.
    pub fn covers(self, wanted: Self) -> bool {
        self.floor() <= wanted.floor()
    }

    /// What a request is asking for. `None` days is the whole chain.
    pub fn wanted(window_days: Option<u64>, now_slot: u64) -> Self {
        match window_days.filter(|d| *d > 0) {
            Some(days) => Self::Since(now_slot.saturating_sub(days * 86_400)),
            None => Self::Genesis,
        }
    }

    /// Read back from storage. `Genesis` is recorded as the Shelley start.
    pub fn from_slot(slot: u64) -> Self {
        if slot <= crate::excavate::SHELLEY_START_SLOT {
            Self::Genesis
        } else {
            Self::Since(slot)
        }
    }
}

pub struct WalletMeta {
    pub scanned_to_chunk: u64,
    /// Slot-granular cursor (tail-aware). `None` on legacy rows.
    pub scanned_to_slot: Option<u64>,
    /// How far BACK this wallet has been scanned. `None` = unknown (never
    /// scanned, or written before the column existed).
    pub scanned_from: Option<ScanTarget>,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
    pub updated_unix: u64,
    /// Rows held when the backfill gave up, or `None` if it never did.
    ///
    /// `Some` means this wallet's history is deliberately INCOMPLETE and no
    /// further backfill will be attempted — the scan would cost a full-chain
    /// read and hundreds of MB for an address nobody can scroll.
    pub oversize_rows: Option<u64>,
}

impl WalletMeta {
    /// Is history still missing below what has been scanned, for a request
    /// wanting `wanted`?
    ///
    /// Unknown depth counts as NOT covered: a wallet whose floor was never
    /// recorded is exactly the one that needs re-establishing.
    pub fn needs_backfill(&self, wanted: ScanTarget) -> bool {
        !self.scanned_from.is_some_and(|held| held.covers(wanted))
    }
}

#[cfg(test)]
mod scan_target_tests {
    use super::*;
    use crate::excavate::SHELLEY_START_SLOT;

    const NOW: u64 = SHELLEY_START_SLOT + 200_000_000;
    const DAY: u64 = 86_400;

    fn meta(scanned_from: Option<ScanTarget>) -> WalletMeta {
        WalletMeta {
            scanned_to_chunk: 0,
            scanned_to_slot: Some(NOW),
            scanned_from,
            first_slot: None,
            last_slot: None,
            updated_unix: 0,
            oversize_rows: None,
        }
    }

    /// THE REGRESSION. A wallet first seen under a 30-day window, then asked
    /// for the full chain, must backfill. The old code decided this from
    /// whether the wallet had ever been scanned, so the answer was always
    /// "no" and full-chain requests silently did nothing but a tail refresh.
    #[test]
    fn a_shallow_wallet_asked_for_everything_backfills() {
        let held = ScanTarget::wanted(Some(30), NOW);
        assert!(meta(Some(held)).needs_backfill(ScanTarget::Genesis));
    }

    /// The point of the type: a 90-day tier over a 90-day scan is COMPLETE and
    /// must not be dragged through a 219 GB sweep just because the only other
    /// option used to be "deep".
    #[test]
    fn a_window_already_covered_does_not_backfill() {
        let held = ScanTarget::wanted(Some(90), NOW);
        assert!(!meta(Some(held)).needs_backfill(ScanTarget::wanted(Some(90), NOW)));
        assert!(
            !meta(Some(held)).needs_backfill(ScanTarget::wanted(Some(30), NOW)),
            "a shallower request over a deeper scan is already satisfied"
        );
    }

    /// Each tier gets exactly its own depth — the reason a bool could not do
    /// this job.
    #[test]
    fn a_deeper_window_over_a_shallower_scan_backfills() {
        let held = ScanTarget::wanted(Some(30), NOW);
        assert!(meta(Some(held)).needs_backfill(ScanTarget::wanted(Some(90), NOW)));
        assert!(meta(Some(held)).needs_backfill(ScanTarget::wanted(Some(365), NOW)));
    }

    /// Genesis satisfies everything, including another genesis request — so a
    /// fully-scanned wallet never re-sweeps the chain.
    #[test]
    fn genesis_covers_every_request() {
        let m = meta(Some(ScanTarget::Genesis));
        assert!(!m.needs_backfill(ScanTarget::Genesis));
        assert!(!m.needs_backfill(ScanTarget::wanted(Some(365), NOW)));
    }

    /// Unknown depth must read as "not covered". Assuming the opposite would
    /// permanently strand every wallet written before the column existed —
    /// exactly the ones this change exists to repair.
    ///
    /// Not hypothetical: the first deploy trusted the old `deep_pending` flag
    /// as a hint for legacy rows, and $boef came back claiming genesis
    /// coverage while holding 898 rows of a nine-month history.
    #[test]
    fn unknown_depth_always_backfills() {
        assert!(meta(None).needs_backfill(ScanTarget::wanted(Some(30), NOW)));
        assert!(meta(None).needs_backfill(ScanTarget::Genesis));
    }

    /// Deeper coverage is a LOWER slot. Inverting this comparison would make
    /// every request look satisfied and nothing would ever backfill again.
    #[test]
    fn coverage_compares_in_the_right_direction() {
        let deep = ScanTarget::Since(SHELLEY_START_SLOT + 10);
        let shallow = ScanTarget::Since(SHELLEY_START_SLOT + 10_000);
        assert!(deep.covers(shallow));
        assert!(!shallow.covers(deep));
    }

    /// A window reaching past the chain's start is genesis, not a negative
    /// slot — `saturating_sub` plus the floor clamp.
    #[test]
    fn an_absurd_window_clamps_to_genesis() {
        let t = ScanTarget::wanted(Some(100_000), NOW);
        assert_eq!(t.floor(), SHELLEY_START_SLOT);
        assert!(t.covers(ScanTarget::Genesis));
    }

    /// Round-tripping through storage: a floor at or below Shelley is genesis.
    #[test]
    fn storage_round_trips_through_the_floor() {
        assert_eq!(
            ScanTarget::from_slot(SHELLEY_START_SLOT),
            ScanTarget::Genesis
        );
        assert_eq!(ScanTarget::from_slot(0), ScanTarget::Genesis);
        let mid = SHELLEY_START_SLOT + 5 * DAY;
        assert_eq!(ScanTarget::from_slot(mid), ScanTarget::Since(mid));
        assert_eq!(ScanTarget::Since(mid).floor(), mid);
    }

    /// A zero-day window is not "zero history" — it is the absence of a
    /// window, which the HTTP layer already treats as full chain.
    #[test]
    fn a_zero_window_means_the_whole_chain() {
        assert_eq!(ScanTarget::wanted(Some(0), NOW), ScanTarget::Genesis);
    }
}

pub fn load_wallet(conn: &Connection, canonical: &str) -> Result<Option<WalletMeta>> {
    let mut stmt = conn.prepare(
        "SELECT scanned_to_chunk, scanned_to_slot, first_slot, last_slot, updated_unix,
                scanned_from_slot, oversize_rows
         FROM wallet WHERE target = ?1",
    )?;
    let mut rows = stmt.query(params![canonical])?;
    match rows.next()? {
        Some(r) => {
            // NULL depth is UNKNOWN, full stop.
            //
            // A row predating this column also carried a `deep_pending`
            // boolean, and `0` there looked salvageable as "no shallow window
            // was applied". It was not: the bug this column exists to fix
            // meant every incremental refresh took the no-floor path and
            // CLEARED that flag without ever running the backfill, so `0`
            // said "some refresh happened", not "the chain was read to
            // genesis". Verified on the box — $boef carried `deep_pending =
            // 0` while holding 898 rows starting at slot 188,499,807, three
            // months of a nine-month wallet. The column is dropped on open
            // for exactly that reason: it cannot be trusted, so nothing
            // should be able to reach for it.
            let recorded: Option<u64> = r.get(5)?;
            let scanned_from = recorded.map(ScanTarget::from_slot);
            Ok(Some(WalletMeta {
                scanned_to_chunk: r.get(0)?,
                scanned_to_slot: r.get(1)?,
                first_slot: r.get(2)?,
                last_slot: r.get(3)?,
                updated_unix: r.get(4)?,
                scanned_from,
                oversize_rows: r.get(6)?,
            }))
        }
        None => Ok(None),
    }
}

pub fn load_owned(conn: &Connection, canonical: &str) -> Result<crate::classify::OwnedSet> {
    let mut stmt =
        conn.prepare("SELECT tx_hash, idx, lovelace, units, script FROM owned WHERE target = ?1")?;
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
        // NULL on rows stored before the column existed — read as
        // key-controlled, which is what the overwhelming majority are.
        let script = r.get::<_, Option<i64>>(4)?.unwrap_or(0) != 0;
        out.insert(
            (h, r.get::<_, u32>(1)?),
            (r.get::<_, u64>(2)?, units, script),
        );
    }
    Ok(out)
}

/// How a store behaves within progressive excavation.
#[derive(Clone, Copy, Default)]
pub struct StoreOpts {
    /// Overwrite existing rows (the deep pass re-states what it now knows).
    pub replace: bool,
    /// How far back this write actually covers. `None` means the depth is
    /// not being asserted by this write — an incremental tail refresh, which
    /// must leave the recorded floor alone rather than claim the shallow
    /// range it just scanned.
    pub scanned_from: Option<ScanTarget>,
    /// Rows held when the backfill gave up on this wallet. `None` leaves any
    /// existing verdict alone — an ordinary refresh must not clear a cap.
    pub oversize_rows: Option<u64>,
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
                  assets_in, assets_out, senders, recipients, assets, asset_total,
                  locked, unlocked, locked_assets)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)"
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
                row.asset_total,
                row.locked,
                row.unlocked,
                row.locked_assets
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()?,
            ])?;
        }
        tx.execute("DELETE FROM owned WHERE target = ?1", params![canonical])?;
        let mut own = tx.prepare(
            "INSERT INTO owned (target, tx_hash, idx, lovelace, assets, units, script)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )?;
        for ((hash, idx), (lovelace, units, script)) in &timeline.owned {
            own.execute(params![
                canonical,
                hash.as_slice(),
                idx,
                lovelace,
                units.len() as u32,
                serde_json::to_string(units)?,
                *script as i64,
            ])?;
        }
        let (new_first, new_last) = if timeline.txs.is_empty() {
            (None, None)
        } else {
            (Some(timeline.first_slot), Some(timeline.last_slot))
        };
        tx.execute(
            "INSERT INTO wallet (target, display, scanned_to_chunk, scanned_to_slot, first_slot,
                                 last_slot, updated_unix, scanned_from_slot, oversize_rows)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(target) DO UPDATE SET
                 scanned_to_chunk = excluded.scanned_to_chunk,
                 scanned_to_slot = excluded.scanned_to_slot,
                 -- COALESCE keeps a cap once set: an ordinary incremental
                 -- passes NULL here, and must not read as \"no longer
                 -- oversize\" and re-arm a full-chain sweep every refresh.
                 oversize_rows = COALESCE(excluded.oversize_rows, wallet.oversize_rows),
                 -- Depth only ever DEEPENS. A shallow refresh over a wallet
                 -- already scanned to genesis must not raise the recorded
                 -- floor and re-strand the history it already holds.
                 scanned_from_slot = MIN(
                     COALESCE(wallet.scanned_from_slot, excluded.scanned_from_slot),
                     COALESCE(excluded.scanned_from_slot, wallet.scanned_from_slot)
                 ),
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
                opts.scanned_from.map(|t: ScanTarget| t.floor()),
                opts.oversize_rows,
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

/// What the cache currently costs, for the budget decision and the gauge.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    /// Size of the database FILE, which is what fills the disk. Includes
    /// pages freed by a delete but not yet reclaimed — see [`evict_to_budget`],
    /// where that distinction is the whole problem.
    pub bytes: u64,
    /// Bytes holding live rows. `bytes - live_bytes` is reclaimable slack.
    pub live_bytes: u64,
    pub wallets: u64,
    pub flows: u64,
    /// Rows held by the single biggest wallet.
    ///
    /// Worth a gauge of its own because the distribution is savagely skewed:
    /// one exchange address held 98.3% of the rows in this cache while every
    /// ordinary wallet sat between one and four thousand. A total alone hides
    /// that, and it is the shape that decides whether a budget is generous or
    /// about to start thrashing.
    pub largest_wallet_flows: u64,
    /// Wallets the backfill gave up on. A rising count is the system-level
    /// signal that people are pointing the tool at exchange-scale addresses.
    pub capped_wallets: u64,
}

pub fn cache_stats(conn: &Connection) -> Result<CacheStats> {
    let page_size: u64 = conn.query_row("PRAGMA page_size", [], |r| r.get(0))?;
    let page_count: u64 = conn.query_row("PRAGMA page_count", [], |r| r.get(0))?;
    let freelist: u64 = conn.query_row("PRAGMA freelist_count", [], |r| r.get(0))?;
    Ok(CacheStats {
        bytes: page_count * page_size,
        live_bytes: page_count.saturating_sub(freelist) * page_size,
        wallets: wallet_count(conn)?,
        flows: conn.query_row("SELECT COUNT(*) FROM flow", [], |r| r.get(0))?,
        largest_wallet_flows: conn
            .query_row(
                "SELECT COUNT(*) AS n FROM flow GROUP BY target ORDER BY n DESC LIMIT 1",
                [],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0),
        capped_wallets: conn.query_row(
            "SELECT COUNT(*) FROM wallet WHERE oversize_rows IS NOT NULL",
            [],
            |r| r.get(0),
        )?,
    })
}

/// Outcome of one janitor pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Evicted {
    pub wallets: usize,
    pub flows: u64,
    pub before: u64,
    pub after: u64,
    /// Over budget with nothing left that is safe to drop. The budget is too
    /// small for the traffic, and the operator needs to hear about it rather
    /// than have the cache start evicting wallets people are looking at.
    pub stuck: bool,
}

/// Drop least-recently-REQUESTED wallets until the cache fits `budget`.
///
/// Two properties matter more than the eviction itself:
///
/// 1. **It reclaims.** `auto_vacuum` is off, so a DELETE returns pages to a
///    freelist and the file never shrinks — row count would fall while disk
///    usage sat exactly where it was, which is the opposite of the point. A
///    VACUUM after the deletes is what makes this a disk bound at all.
/// 2. **It refuses to thrash.** Anything requested within `protect` is off
///    limits. Evicting a wallet somebody is watching buys a few hundred MB
///    and spends a 219 GB re-read to get it back — strictly worse than being
///    over budget, so the pass gives up and reports [`Evicted::stuck`].
pub fn evict_to_budget(
    conn: &mut Connection,
    budget: u64,
    protect: std::time::Duration,
    now_unix: u64,
) -> Result<Evicted> {
    let before = cache_stats(conn)?.bytes;
    let mut out = Evicted {
        before,
        after: before,
        ..Default::default()
    };
    if before <= budget {
        return Ok(out);
    }
    let cutoff = now_unix.saturating_sub(protect.as_secs());
    loop {
        // Live bytes, not file bytes: within a pass the deletes have not been
        // reclaimed yet, so the file size cannot fall and the loop would
        // evict the entire cache before noticing it was done.
        let stats = cache_stats(conn)?;
        if stats.live_bytes <= budget {
            break;
        }
        // NULL sorts first under ASC in sqlite — never requested, so cheapest
        // to lose.
        let victim: Option<(String, String)> = conn
            .query_row(
                "SELECT target, display FROM wallet
                 WHERE COALESCE(last_seen_unix, 0) < ?1
                 ORDER BY last_seen_unix ASC LIMIT 1",
                params![cutoff],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let Some((target, label)) = victim else {
            out.stuck = true;
            break;
        };
        let tx = conn.transaction()?;
        let flows: u64 = tx.query_row(
            "SELECT COUNT(*) FROM flow WHERE target = ?1",
            params![&target],
            |r| r.get(0),
        )?;
        tx.execute("DELETE FROM flow WHERE target = ?1", params![&target])?;
        tx.execute("DELETE FROM owned WHERE target = ?1", params![&target])?;
        tx.execute("DELETE FROM wallet WHERE target = ?1", params![&target])?;
        tx.commit()?;
        out.wallets += 1;
        out.flows += flows;
        tracing::info!(wallet = %label, flows, "cache evicted");
    }
    if out.wallets > 0 {
        // Reclaim. Without this the deletes are invisible on disk.
        conn.execute_batch("VACUUM")?;
    }
    out.after = cache_stats(conn)?.bytes;
    Ok(out)
}

/// Record that a client asked for these wallets.
pub fn touch_seen(conn: &Connection, seen: &[(String, u64)]) -> Result<()> {
    let mut stmt = conn.prepare(
        "UPDATE wallet SET last_seen_unix = MAX(COALESCE(last_seen_unix, 0), ?2)
         WHERE target = ?1",
    )?;
    for (target, at) in seen {
        stmt.execute(params![target, at])?;
    }
    Ok(())
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
                recipients, assets, market, asset_total, locked, unlocked, locked_assets
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
            // NULL on rows written before the column existed — read as 0,
            // which the consumer treats as "unknown" and falls back to the
            // list's own length.
            asset_total: r.get::<_, Option<u32>>(11)?.unwrap_or(0),
            locked: r.get::<_, Option<u64>>(12)?.unwrap_or(0),
            unlocked: r.get::<_, Option<u64>>(13)?.unwrap_or(0),
            locked_assets: match r.get::<_, Option<String>>(14)? {
                Some(s) => Some(serde_json::from_str(&s).context("parsing locked_assets")?),
                None => None,
            },
            market,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod cache_tests {
    use super::*;
    use std::time::Duration;

    const HOUR: u64 = 3600;
    const NOW: u64 = 1_800_000_000;

    fn cache() -> Connection {
        let conn = Connection::open_in_memory().expect("in-memory db");
        migrate(&conn).expect("migrate");
        conn
    }

    /// A wallet holding `rows` flows, last requested at `seen` (`None` = never).
    fn add_wallet(conn: &Connection, target: &str, rows: u32, seen: Option<u64>) {
        conn.execute(
            "INSERT INTO wallet (target, display, scanned_to_chunk, updated_unix, last_seen_unix)
             VALUES (?1, ?1, 0, ?2, ?3)",
            params![target, NOW, seen],
        )
        .expect("insert wallet");
        // Padded so the rows cost real PAGES — the budget is in bytes, and a
        // handful of skinny rows would fit any budget and test nothing.
        let pad = "x".repeat(512);
        for i in 0..rows {
            conn.execute(
                "INSERT INTO flow (target, slot, tx_idx, tx_hash, kind, lovelace_in,
                                   lovelace_out, assets_in, assets_out, senders)
                 VALUES (?1, ?2, 0, ?3, 'transfer', 0, 0, 0, 0, ?4)",
                params![target, i as u64, format!("{target}-{i}").as_bytes(), pad],
            )
            .expect("insert flow");
        }
    }

    /// The core ordering rule: eviction follows REQUEST recency, so the wallet
    /// nobody has asked for goes and the one in active use stays — even though
    /// both were re-stored by the refresh watcher at the same moment.
    #[test]
    fn the_least_recently_requested_wallet_goes_first() {
        let mut conn = cache();
        add_wallet(&conn, "stale", 300, Some(NOW - 90 * HOUR));
        add_wallet(&conn, "hot", 300, Some(NOW - HOUR));
        let budget = cache_stats(&conn).unwrap().live_bytes * 2 / 3;

        let out = evict_to_budget(&mut conn, budget, Duration::from_secs(48 * HOUR), NOW).unwrap();

        assert_eq!(out.wallets, 1);
        assert!(!out.stuck);
        let left: Vec<String> = conn
            .prepare("SELECT target FROM wallet")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(left, vec!["hot".to_string()]);
        // The rows go with it, not just the wallet row.
        let orphans: u64 = conn
            .query_row(
                "SELECT COUNT(*) FROM flow WHERE target = 'stale'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(orphans, 0);
    }

    /// The property the whole feature rests on. `auto_vacuum` is off, so a
    /// DELETE alone returns pages to a freelist and the FILE never shrinks —
    /// row count would fall while disk usage sat exactly where it was.
    #[test]
    fn eviction_actually_reclaims_disk() {
        let mut conn = cache();
        add_wallet(&conn, "stale", 800, None);
        add_wallet(&conn, "hot", 100, Some(NOW));
        let budget = cache_stats(&conn).unwrap().live_bytes / 4;

        let out = evict_to_budget(&mut conn, budget, Duration::from_secs(48 * HOUR), NOW).unwrap();

        assert!(out.wallets >= 1);
        assert!(
            out.after < out.before,
            "file did not shrink: {} -> {} (missing VACUUM?)",
            out.before,
            out.after
        );
    }

    /// A wallet nobody has ever asked for sorts FIRST — NULL under ASC in
    /// sqlite — because it is the cheapest thing in the cache to lose.
    #[test]
    fn a_never_requested_wallet_is_the_first_to_go() {
        let mut conn = cache();
        add_wallet(&conn, "never", 300, None);
        add_wallet(&conn, "ancient", 300, Some(NOW - 500 * HOUR));
        let budget = cache_stats(&conn).unwrap().live_bytes * 2 / 3;

        evict_to_budget(&mut conn, budget, Duration::from_secs(48 * HOUR), NOW).unwrap();

        let left: Vec<String> = conn
            .prepare("SELECT target FROM wallet")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(left, vec!["ancient".to_string()]);
    }

    /// Refusing to thrash. Everything over budget is in active use, so the
    /// pass evicts NOTHING and says so: dropping a wallet somebody is watching
    /// reclaims a few hundred MB and spends a full re-scan to get it back,
    /// which is strictly worse than sitting over budget.
    #[test]
    fn wallets_in_active_use_are_never_evicted() {
        let mut conn = cache();
        add_wallet(&conn, "hot-a", 400, Some(NOW - HOUR));
        add_wallet(&conn, "hot-b", 400, Some(NOW - 2 * HOUR));
        let budget = cache_stats(&conn).unwrap().live_bytes / 4;

        let out = evict_to_budget(&mut conn, budget, Duration::from_secs(48 * HOUR), NOW).unwrap();

        assert_eq!(out.wallets, 0);
        assert!(out.stuck, "the operator has to hear about this");
        assert_eq!(wallet_count(&conn).unwrap(), 2);
    }

    /// Under budget is a no-op — no deletes, and no VACUUM rewriting the whole
    /// file every five minutes for nothing.
    #[test]
    fn a_cache_within_budget_is_left_alone() {
        let mut conn = cache();
        add_wallet(&conn, "a", 100, None);
        let before = cache_stats(&conn).unwrap();

        let out =
            evict_to_budget(&mut conn, before.bytes * 4, Duration::from_secs(HOUR), NOW).unwrap();

        assert_eq!(out.wallets, 0);
        assert!(!out.stuck);
        assert_eq!(cache_stats(&conn).unwrap().wallets, before.wallets);
    }

    /// Interest only ever moves FORWARD. Marks are flushed in whatever order
    /// the buffer drains, and an out-of-order write must not age a wallet
    /// backwards into the eviction queue.
    #[test]
    fn recorded_interest_never_moves_backwards() {
        let conn = cache();
        add_wallet(&conn, "a", 1, Some(NOW));
        touch_seen(&conn, &[("a".into(), NOW - 100 * HOUR)]).unwrap();
        let seen: u64 = conn
            .query_row(
                "SELECT last_seen_unix FROM wallet WHERE target='a'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(seen, NOW);

        touch_seen(&conn, &[("a".into(), NOW + HOUR)]).unwrap();
        let seen: u64 = conn
            .query_row(
                "SELECT last_seen_unix FROM wallet WHERE target='a'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(seen, NOW + HOUR);
    }

    /// A cap must SURVIVE the ordinary refreshes that follow it.
    ///
    /// The refresh watcher re-stores every cached wallet every twenty minutes
    /// and passes `oversize_rows: None`, meaning "no verdict from this write".
    /// Were that stored literally it would clear the cap within the hour and
    /// re-arm a 219 GB sweep the next time anyone opened the address — a loop
    /// that would look exactly like the service being slow for no reason.
    #[test]
    fn an_ordinary_refresh_does_not_clear_a_cap() {
        let conn = cache();
        add_wallet(&conn, "whale", 1, None);
        conn.execute(
            "UPDATE wallet SET oversize_rows = 50000 WHERE target = 'whale'",
            [],
        )
        .unwrap();

        // What an incremental writes: a fresh cursor, no verdict.
        conn.execute(
            "INSERT INTO wallet (target, display, scanned_to_chunk, updated_unix, oversize_rows)
             VALUES ('whale', 'whale', 9, 123, NULL)
             ON CONFLICT(target) DO UPDATE SET
                 oversize_rows = COALESCE(excluded.oversize_rows, wallet.oversize_rows),
                 updated_unix = excluded.updated_unix",
            [],
        )
        .unwrap();

        let held: Option<u64> = conn
            .query_row(
                "SELECT oversize_rows FROM wallet WHERE target='whale'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(held, Some(50_000), "the cap was cleared by a refresh");
    }

    /// The capped count is the system-level signal — a rising number means
    /// people are pointing the tool at exchange-scale addresses.
    #[test]
    fn stats_count_capped_wallets() {
        let conn = cache();
        add_wallet(&conn, "whale", 2, None);
        add_wallet(&conn, "ordinary", 2, None);
        assert_eq!(cache_stats(&conn).unwrap().capped_wallets, 0);
        conn.execute(
            "UPDATE wallet SET oversize_rows = 50000 WHERE target = 'whale'",
            [],
        )
        .unwrap();
        assert_eq!(cache_stats(&conn).unwrap().capped_wallets, 1);
    }

    /// The skew that makes a byte budget the right shape: one wallet held 98%
    /// of a real cache, so the biggest-wallet gauge has to track the outlier
    /// rather than an average that hides it.
    #[test]
    fn stats_report_the_biggest_wallet_not_the_average() {
        let conn = cache();
        add_wallet(&conn, "whale", 500, None);
        add_wallet(&conn, "small", 5, None);
        let s = cache_stats(&conn).unwrap();
        assert_eq!(s.wallets, 2);
        assert_eq!(s.flows, 505);
        assert_eq!(s.largest_wallet_flows, 500);
    }
}
