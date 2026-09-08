//! The VOLATILE tail — the stretch between the immutable tip and the chain
//! tip, which no Mithril snapshot will ever contain.
//!
//! # It reads the box's spool; it does not follow the chain
//!
//! `wallet-sieve` already keeps a **chain-tail spool** at
//! `/opt/wallet-sieve/tail.db` (`tools/wallet-sieve/src/tail.rs`): raw block
//! CBOR from the last COMPLETE Mithril chunk to the tip, appended by one N2N
//! chainsync follower, truncated on rollback, and pruned after every Mithril
//! refresh of everything the chunk store has come to cover. Its range is
//! therefore exactly `[immutable_tip, live_tip)` — the volatile range,
//! already on disk, already maintained, 189 MB and about a minute behind the
//! chain. Measured 2026-09-06: the spool held 34,134 blocks over slots
//! 196,408,804–197,098,108 while this module's first cut was fetching the
//! same 34,000 blocks over a connection of its own.
//!
//! So token-ledger opens it READ-ONLY and is a fourth reader of a box
//! facility, beside the snapshot and the tx-index. It is not a second
//! follower: the constraint the user locked on 2026-07-22 is "NOT a second
//! follower STORE" — never open mitos's live redb, which has a
//! single-writer lock. sqlite in WAL mode is the opposite case, and
//! `wallet-sieve` exports `open_ro` for exactly this.
//!
//! # One derivation, not two
//!
//! The spool's own design note is the reason this module is short: *"NO
//! extraction at ingest, ever: the spool is bytes, the sieve is the
//! reader."* Because the tail is BYTES, the ordinary reverse [`Scan`] reads
//! it through the same path as a chunk — [`reverse::scan_blocks`] — so
//! there is ONE definition of what rows a transaction produces, one
//! resolution story (sources inside the range met by the descent, sources
//! below the immutable tip settled through the tx-index, whose coverage
//! ends exactly there), and no rollback bookkeeping here at all: the spool
//! truncates on rollback and the tail is re-derived whole on the next tick.
//!
//! The first cut of this module did the opposite — fetched its own blocks
//! and derived rows FORWARD with a bespoke outref buffer and a rollback undo
//! log. That was a second implementation of the movement derivation, which
//! is the drift the manifest was moved into `policy-archive` to avoid.
//!
//! # It is REPLACED, never summed
//!
//! The archive's read rule is "sum every file by `(tx, unit, party)`", and
//! that is only sound for stretches that cannot change. This one can: a
//! rollback within `k` (2,160 blocks, about twelve hours) retracts blocks
//! already in the file. So the tail is ONE file per policy, rewritten whole
//! every tick, recorded as a [`RangeKind::Volatile`] range, and the manifest
//! keeps at most one. When the snapshot refreshes, the spool prunes what the
//! chunks now cover, a top-up job reads that stretch from the chunks, and
//! the tail moves up to sit above the new tip.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use policy_archive::Stamp;
use rusqlite::{Connection, OpenFlags};

use crate::archive::{self, PassEntry, RangeKind};
use crate::policy_api::PolicyHub;
use crate::reverse;
use crate::segments::{self, SegmentWriter};
use crate::walk::Watched;

/// How often the tail is re-derived and republished, by default. Every tick
/// is a Parquet file, an R2 put and a KV put PER POLICY whose tail moved —
/// a cost per policy per tick, not per block.
pub const DEFAULT_REFRESH_SECS: u64 = 300;

/// How far behind the spool's own tip to stop reading.
///
/// wallet-sieve's scans hold the same distance back (its
/// `SAFETY_DEPTH_SLOTS`) so a shallow rollback can never reach a wallet
/// cache. The tail is replaced whole on every tick so a retracted block
/// corrects itself here within one tick — but a published archive is read
/// from KV at the edge, and a row that appears and then vanishes is worse
/// than a row that arrives five minutes late.
const SAFETY_DEPTH_SLOTS: u64 = 300;

#[derive(clap::Args, Debug, Clone)]
pub struct TipArgs {
    /// wallet-sieve's chain-tail spool, READ-ONLY: raw block CBOR from the
    /// last complete Mithril chunk to the chain tip. Without it there is no
    /// volatile tail and every archive stops at the immutable tip.
    #[arg(long)]
    pub tail_db: Option<PathBuf>,

    /// How often to re-derive and republish the tail.
    #[arg(long, default_value_t = DEFAULT_REFRESH_SECS)]
    pub tip_refresh_secs: u64,
}

/// Start the tail thread. Nothing without `--tail-db`.
pub fn spawn(hub: Arc<PolicyHub>, args: TipArgs, publish: Option<crate::publish::Targets>) {
    let Some(spool) = args.tail_db.clone() else {
        return;
    };
    let every = Duration::from_secs(args.tip_refresh_secs.max(30));
    std::thread::Builder::new()
        .name("tip".into())
        .spawn(move || {
            let publishing = crate::scheduler::Publishing::new(publish);
            tracing::info!(spool = %spool.display(), secs = every.as_secs(), "tip: reading the chain-tail spool");
            loop {
                std::thread::sleep(every);
                if let Err(e) = refresh(&hub, &spool, &publishing) {
                    tracing::warn!(error = %format!("{e:#}"), "tip: could not refresh the tail");
                }
            }
        })
        .expect("spawn tip thread");
}

fn open_ro(path: &Path) -> Result<Connection> {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .with_context(|| format!("opening {} read-only", path.display()))
}

/// What the spool can answer for right now.
struct Extent {
    /// Newest slot it holds.
    tip: u64,
    blocks: u64,
}

fn extent(conn: &Connection) -> Result<Option<Extent>> {
    let row: Option<(Option<u64>, u64)> = conn
        .query_row("SELECT MAX(slot), COUNT(*) FROM tail_blocks", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .ok();
    Ok(match row {
        Some((Some(tip), blocks)) => Some(Extent { tip, blocks }),
        _ => None,
    })
}

/// One tick: read the spool once, gate it once, and re-derive every
/// archived policy's tail from the same blocks.
fn refresh(hub: &PolicyHub, spool: &Path, publishing: &crate::scheduler::Publishing) -> Result<()> {
    let policies = hub.archived_policies();
    if policies.is_empty() {
        return Ok(());
    }
    let conn = open_ro(spool)?;
    let Some(ext) = extent(&conn)? else {
        tracing::debug!("tip: the spool is empty");
        return Ok(());
    };
    // The tail begins where the settled coverage ends. The spool prunes to
    // the same boundary from its own side — both read `list_chunks` over the
    // same immutable directory — so this is a check, not an adjustment.
    let floor = hub.tip_slot();
    let ceiling = ext.tip.saturating_sub(SAFETY_DEPTH_SLOTS) + 1;
    if ceiling <= floor {
        tracing::debug!(
            floor,
            spool_tip = ext.tip,
            "tip: nothing above the immutable tip yet"
        );
        return Ok(());
    }

    // ONE read and ONE gate for every policy: most blocks in the tail touch
    // none of them, and a miss costs one memmem rather than a decode per
    // policy.
    let started = Instant::now();
    let watched: Vec<(String, Vec<u8>)> = policies
        .iter()
        .filter_map(|p| hex::decode(p).ok().map(|b| (p.clone(), b)))
        .collect();
    let needles =
        chain_sieve::Needles::new(&watched.iter().map(|(_, b)| b.clone()).collect::<Vec<_>>())?;
    let blocks = load(&conn, floor, ceiling, &needles)?;
    let scanned = started.elapsed();

    let index = hub.index.as_ref().map(|h| h.get());
    for (policy, bytes) in &watched {
        match write_tail(
            hub,
            policy,
            bytes,
            &blocks,
            floor,
            ceiling,
            index.as_deref(),
        ) {
            // Only republish when the tail actually holds something new.
            Ok(Wrote::Tail) => publishing.publish(hub.policy_dir(policy), policy),
            Ok(Wrote::Nothing) => {}
            Err(e) => {
                tracing::warn!(policy, error = %format!("{e:#}"), "tip: could not write the tail")
            }
        }
    }
    tracing::info!(
        floor,
        ceiling,
        spool_blocks = ext.blocks,
        candidates = blocks.len(),
        policies = watched.len(),
        read_secs = format!("{:.1}", scanned.as_secs_f64()),
        total_secs = format!("{:.1}", started.elapsed().as_secs_f64()),
        "tip: tail refreshed"
    );
    Ok(())
}

/// Every spool block in `[floor, ceiling)` that touches ANY watched policy,
/// NEWEST FIRST — the order the reverse scan reads in.
///
/// Streams the whole range through sqlite and keeps only the hits, so what
/// stays in memory is a policy's activity rather than a day of chain.
fn load(
    conn: &Connection,
    floor: u64,
    ceiling: u64,
    needles: &chain_sieve::Needles<'_>,
) -> Result<Vec<(u64, Vec<u8>)>> {
    let mut stmt = conn.prepare(
        "SELECT slot, cbor FROM tail_blocks WHERE slot >= ?1 AND slot < ?2 ORDER BY slot DESC",
    )?;
    let mut rows = stmt.query(rusqlite::params![floor, ceiling])?;
    let mut out = Vec::new();
    while let Some(r) = rows.next()? {
        let cbor: Vec<u8> = r.get(1)?;
        if needles.hit(&cbor) {
            out.push((r.get(0)?, cbor));
        }
    }
    Ok(out)
}

/// Did this policy get a tail file?
enum Wrote {
    Tail,
    Nothing,
}

fn write_tail(
    hub: &PolicyHub,
    policy: &str,
    policy_bytes: &[u8],
    blocks: &[(u64, Vec<u8>)],
    floor: u64,
    ceiling: u64,
    index: Option<&tx_index::Index>,
) -> Result<Wrote> {
    let dir = hub.policy_dir(policy);
    // UNDER THE POLICY'S LANDING LOCK: a walk job may be landing a range of
    // its own right now, and both are a read-modify-write of one manifest.
    let lock = hub.landing_lock(policy);
    let _held = lock.lock().expect("landing lock");
    let Some(mut manifest) = archive::load_manifest(&dir)? else {
        // No archive yet — nothing to hang a tail off.
        return Ok(Wrote::Nothing);
    };
    // From the scheduler's allocator, not the manifest: a walk job may
    // already hold the number `manifest.next_seq()` would hand out.
    let seq = hub.next_pass_seq(policy);
    let tip_dir = dir.join(PassEntry::tip_dir_name(seq));
    if tip_dir.exists() {
        std::fs::remove_dir_all(&tip_dir)?;
    }
    let sealed_unix = reverse::now_unix();
    let stamp = Stamp {
        policy_hex: policy.to_string(),
        completeness: manifest.completeness(),
        walk_from: manifest.walk_from(),
        walk_to: Some(ceiling),
        covered_from: floor,
        covered_to: ceiling.saturating_sub(1),
        sealed_unix,
    };
    let mut writer = SegmentWriter::new(&tip_dir, stamp.clone())?;
    let out = reverse::scan_blocks(
        blocks.to_vec(),
        policy_bytes,
        // An ARCHIVE is per policy id, never per unit: the manifest is keyed
        // by the policy and the feed shows every unit under it.
        &Watched::Policy,
        floor,
        ceiling,
        &mut writer,
        index,
    )?;
    let segments = writer.finish()?;
    // The ceiling at infinity: every row of the tail is its own, so the
    // merge folds them and drops what nets to nothing, exactly as a landed
    // range's compaction does.
    let compacted = segments::compact(&tip_dir, &segments, u64::MAX, &stamp)?;

    // AT MOST ONE volatile entry, replaced whole. Never summed with the last
    // one: that stretch can roll back, and rewriting it is what makes the
    // sum rule safe for every other file in the archive.
    let previous: Vec<String> = manifest
        .passes
        .iter()
        .filter(|p| p.kind == RangeKind::Volatile)
        .map(|p| p.dir.clone())
        .collect();
    manifest.passes.retain(|p| p.kind == RangeKind::Immutable);
    manifest.passes.push(PassEntry {
        seq,
        dir: PassEntry::tip_dir_name(seq),
        ceiling,
        floor,
        windows: Vec::new(),
        kind: RangeKind::Volatile,
        rolled_up: false,
        movements: Some(compacted.movements),
        corrections: compacted.corrections,
        // The tail observes nothing: it is re-derived whole every tick, and
        // the immutable pass that later covers the same slots writes the
        // observations for them.
        observations: None,
        segments: Vec::new(),
        // The tail carries NO state forward: it is re-derived whole every
        // tick, so there is no sidecar and nothing for a later job to load.
        pending: 0,
        found: out.written,
        written: compacted.written,
        backfilled: out.backfilled,
        units: compacted.units,
        secs: 0.0,
        written_unix: sealed_unix,
    });
    manifest.updated_unix = sealed_unix;
    archive::store_manifest(&dir, &manifest)?;
    archive::store_bundle(&dir, &manifest)?;
    // Only once the manifest names the new one.
    for prev in previous {
        let _ = std::fs::remove_dir_all(dir.join(prev));
    }
    if compacted.written > 0 {
        tracing::info!(
            policy,
            floor,
            ceiling,
            found = out.written,
            written = compacted.written,
            by_index = out.by_index,
            unresolved = out.unresolved,
            "tip: tail written"
        );
    }
    Ok(Wrote::Tail)
}
