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

use std::collections::HashMap;
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

/// How often the FLOOR sidecar is extended from the tail.
///
/// The spool is re-read every tick anyway; what this gate protects is the
/// sidecar, which is one sorted array rewritten whole — 3.6 MiB at 232,745
/// policies. Hourly that is ~86 MiB a day; every tick it would be ~1 GB a day for
/// the same information. A launch is therefore at most an hour invisible to
/// admission, which is finer than the six-hour chunk the index itself is built
/// from.
pub const FLOOR_SECS: u64 = 3_600;

/// Start the tail thread. Nothing without `--tail-db`.
///
/// ⚠️ **It does not publish.** Landing a tail leaves the archive's
/// `published.json` stale, and `token-ledger sync` — a separate process — is
/// what makes R2 match disk. That split is why a slow or failing publish can no
/// longer delay the next tick: publishing was 206 s of a 300 s interval, against
/// 0.1 s of reading.
pub fn spawn(hub: Arc<PolicyHub>, args: TipArgs) {
    let Some(spool) = args.tail_db.clone() else {
        return;
    };
    let every = Duration::from_secs(args.tip_refresh_secs.max(30));
    std::thread::Builder::new()
        .name("tip".into())
        .spawn(move || {
            tracing::info!(spool = %spool.display(), secs = every.as_secs(), floor_secs = FLOOR_SECS, "tip: reading the chain-tail spool");
            let mut floors_at: Option<Instant> = None;
            loop {
                std::thread::sleep(every);
                if let Err(e) = refresh(&hub, &spool) {
                    tracing::warn!(error = %format!("{e:#}"), "tip: could not refresh the tail");
                }
                if floors_at.is_none_or(|t| t.elapsed() >= Duration::from_secs(FLOOR_SECS)) {
                    if let Err(e) = refresh_floors(&hub, &spool) {
                        tracing::warn!(error = %format!("{e:#}"), "floor: could not extend the policy index");
                    }
                    floors_at = Some(Instant::now());
                }
            }
        })
        .expect("spawn tip thread");
}

/// One tick of the floor sidecar: the policies minted since the last complete
/// chunk, written into `runs.pidx`.
///
/// ⚠️ **This is the only way such a policy gets a floor at all.** `policy-index`
/// is built from complete chunks, so a launch younger than the last one is absent
/// from it — and absent means admission REFUSES rather than reading every chunk
/// back to genesis (MEASURED on $BUNNY: 229.5 s, 234 of its 235 passes empty).
fn refresh_floors(hub: &PolicyHub, spool: &Path) -> Result<()> {
    if hub.policy_index_dir.is_none() {
        return Ok(());
    }
    let conn = open_ro(spool)?;
    let Some(ext) = extent(&conn)? else {
        return Ok(());
    };
    // The same window the tail is derived over: the settled coverage's end to the
    // spool's tip, less the rollback depth.
    let floor = hub.tip_slot();
    let ceiling = ext.tip.saturating_sub(SAFETY_DEPTH_SLOTS) + 1;
    if ceiling <= floor {
        return Ok(());
    }
    extend_floors(hub, &conn, floor, ceiling)
}

/// Stream the tail's blocks and hand `policy-index` the policies it has not seen.
///
/// One block at a time: the whole tail is ~34,000 blocks and holding them decoded
/// would be hundreds of MB for a set of floors that is a few dozen rows.
fn extend_floors(hub: &PolicyHub, conn: &Connection, floor: u64, ceiling: u64) -> Result<()> {
    let Some(dir) = hub.policy_index_dir.as_deref() else {
        return Ok(());
    };
    let started = Instant::now();
    let mut stmt = conn.prepare(
        "SELECT slot, cbor FROM tail_blocks WHERE slot >= ?1 AND slot < ?2 ORDER BY slot DESC",
    )?;
    let mut rows = stmt.query(rusqlite::params![floor, ceiling])?;

    let mut first: HashMap<u64, u16> = HashMap::new();
    let mut blocks = 0u64;
    let mut undecodable = 0u64;
    let mut mints = 0u64;
    while let Some(r) = rows.next()? {
        let slot: u64 = r.get(0)?;
        let cbor: Vec<u8> = r.get(1)?;
        let policies = match policy_index::mints_in_block(&cbor) {
            Ok(p) => p,
            Err(_) => {
                undecodable += 1;
                continue;
            }
        };
        // THE VOLATILE TAIL HAS NO CHUNK FILES. A slot's chunk is
        // `slot / CHUNK_SLOTS` by construction, so the floor this produces is the
        // chunk that WILL hold the mint once the snapshot reaches it — and, as
        // always, its first slot sits at or below the mint itself.
        let chunk = (slot / crate::registry::CHUNK_SLOTS) as u16;
        for prefix in policies {
            first
                .entry(prefix)
                .and_modify(|c| *c = (*c).min(chunk))
                .or_insert(chunk);
            mints += 1;
        }
        blocks += 1;
    }

    let candidates: Vec<policy_index::FloorRun> = first
        .into_iter()
        .map(|(policy_prefix, first_chunk)| policy_index::FloorRun {
            policy_prefix,
            first_chunk,
        })
        .collect();
    if candidates.is_empty() {
        tracing::debug!(blocks, "floor: no mint in this stretch of the tail");
        return Ok(());
    }
    match policy_index::add_floors(dir, &candidates)? {
        Some(added) => tracing::info!(
            blocks,
            undecodable,
            mints,
            seen = candidates.len(),
            added = added.added,
            held = added.held,
            covers = format!("{}..={}", added.covers.0, added.covers.1),
            secs = format!("{:.1}", started.elapsed().as_secs_f64()),
            "floor: tail extended the policy index"
        ),
        None => tracing::info!(
            dir = %dir.display(),
            "floor: no policy index to extend — a sidecar alone would answer \"not found\" for every older policy"
        ),
    }
    Ok(())
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
fn refresh(hub: &PolicyHub, spool: &Path) -> Result<()> {
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
    let mut with_rows = Phases::default();
    let mut without_rows = Phases::default();
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
            // ⚠️ THE DERIVED SERIES IS STILL NOT WRITTEN HERE. It carries the
            // fold's state at a boundary, and a reader continues it through
            // the tail — so a tail rebuild changes what a reader folds, not
            // what the artifact says. Writing it here re-folded the WHOLE
            // archive for all 31 policies every cycle (163 s -> 236 s of a
            // 300 s interval) and would have grown with the archive.
            //
            // The archive moved, so R2 is now behind: `sync` republishes this
            // policy from `published.json` versus the manifest. What happens
            // HERE is only the in-process cache — this is the one place that
            // knows the archive changed, and both caches are keyed on the
            // version this write just changed, so the next `/live` finds them
            // current instead of paying for the folds.
            Ok(Wrote::Tail { mut phases, landed }) => {
                let warming = Instant::now();
                crate::policy_api::warm(hub, policy);
                phases.warm = warming.elapsed();
                match landed {
                    true => with_rows.add(phases),
                    false => without_rows.add(phases),
                }
            }
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
        // Where the tick goes. `moved` is the work a reader needed; `quiet`
        // is a pass that wrote a file, a manifest and a bundle for no rows —
        // the share a real `Wrote::Nothing` would give back.
        moved_policies = with_rows.count,
        quiet_policies = without_rows.count,
        moved_ms = with_rows.ms(),
        quiet_ms = without_rows.ms(),
        lock_ms = with_rows.lock.as_millis(),
        scan_ms = with_rows.scan.as_millis(),
        parquet_ms = with_rows.parquet.as_millis(),
        manifest_ms = with_rows.manifest.as_millis(),
        warm_ms = with_rows.warm.as_millis(),
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
    /// A pass was written. `landed` is whether it holds any rows — the
    /// distinction that decides whether the archive changed for a reader, and
    /// therefore whether it needs publishing at all.
    Tail {
        phases: Phases,
        landed: bool,
    },
    Nothing,
}

/// Where one policy's pass spent its time.
///
/// ⚠️ MEASUREMENT, not decoration. The tick costs ~165 ms per policy and the
/// split between the scan, the Parquet write and the bundle decides which
/// optimisation is worth doing — see `docs/design/TAIL_REFRESH.md`. `landed`
/// separates the work that has to happen from the work a zero-row pass is
/// doing for nothing.
#[derive(Debug, Default, Clone, Copy)]
struct Phases {
    lock: Duration,
    scan: Duration,
    parquet: Duration,
    manifest: Duration,
    warm: Duration,
    count: u64,
}

impl Phases {
    fn add(&mut self, other: Self) {
        self.lock += other.lock;
        self.scan += other.scan;
        self.parquet += other.parquet;
        self.manifest += other.manifest;
        self.warm += other.warm;
        self.count += other.count;
    }

    fn ms(&self) -> u128 {
        (self.lock + self.scan + self.parquet + self.manifest + self.warm).as_millis()
    }
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
    let mut phases = Phases::default();
    // UNDER THE POLICY'S LANDING LOCK: a walk job may be landing a range of
    // its own right now, and both are a read-modify-write of one manifest.
    let waiting = Instant::now();
    let lock = hub.landing_lock(policy);
    let _held = lock.lock().expect("landing lock");
    phases.lock = waiting.elapsed();
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
    let mut observations: Vec<policy_archive::Observation> = Vec::new();
    let scanning = Instant::now();
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
        &mut observations,
    )?;
    phases.scan = scanning.elapsed();
    let parcelling = Instant::now();
    let segments = writer.finish()?;

    // Sorted by slot so the footer's statistics can be seeked on, exactly as
    // an immutable pass writes its own.
    let mut observations_entry = None;
    if !observations.is_empty() {
        observations.sort_by_key(|o| (o.slot, o.address.clone()));
        let path = tip_dir.join(policy_archive::OBSERVATIONS);
        let mut w = policy_archive::ObservationWriter::new(std::fs::File::create(&path)?, &stamp)?;
        let rows = observations.len();
        for o in observations {
            w.push(o);
        }
        let written = w.close()?;
        tracing::info!(
            policy,
            rows = written.rows,
            max_slot = ?written.max_slot,
            "tip: observations written — the volatile range now carries pool state"
        );
        let _ = rows;
        observations_entry = Some(archive::FileEntry {
            file: policy_archive::OBSERVATIONS.to_string(),
            rows: written.rows,
            min_slot: written.min_slot,
            max_slot: written.max_slot,
            units: 0,
        });
    }
    // The ceiling at infinity: every row of the tail is its own, so the
    // merge folds them and drops what nets to nothing, exactly as a landed
    // range's compaction does.
    let compacted = segments::compact(&tip_dir, &segments, u64::MAX, &stamp)?;
    let landed = compacted.written > 0;
    phases.parquet = parcelling.elapsed();

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
        // ⚠️ NAMED, or it reaches no consumer: the publisher uploads exactly
        // the files the manifest names and the pruner deletes what it does
        // not. Writing the file without recording it left the whole tier on
        // the box once already.
        observations: observations_entry,
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
    // MERGE the profile, exactly as a landed immutable pass does. Without
    // this a token launched above the last complete chunk has no profile at
    // all: `/supply` reports `class: unknown`, and the site refuses it a
    // market cap for want of a scale the tail can actually see. Plumbing is
    // excluded at the scan, so a launchpad pool's state NFT cannot flip a
    // fungible token to `mixed`.
    let mut prof = manifest.profile.take().unwrap_or_default();
    prof.units_seen = prof.units_seen.max(out.profile.units_seen);
    prof.fungible_units = prof.fungible_units.max(out.profile.fungible_units);
    prof.single_units = prof.single_units.max(out.profile.single_units);
    manifest.profile = Some(prof);
    manifest.updated_unix = sealed_unix;
    let writing = Instant::now();
    archive::store_manifest(&dir, &manifest)?;
    archive::store_bundle(&dir, &manifest)?;
    phases.manifest = writing.elapsed();
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
    phases.count = 1;
    Ok(Wrote::Tail { phases, landed })
}
