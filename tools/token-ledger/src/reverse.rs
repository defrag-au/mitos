//! `reverse` — walk history BACKWARD from the tip, newest first, into the
//! policy archive. No database, and no model in memory.
//!
//! The forward walk in [`crate::walk`] is the complete one: it starts at or
//! below the policy's first mint, so its outref buffer is complete by
//! construction and every projection over it — float, holder count,
//! concentration, market cap — is exact. It is also the wrong shape for a feed.
//! A feed wants the newest row FIRST, and a forward walk produces the newest
//! row LAST, so nothing can be shown until the whole window is done.
//!
//! This mode inverts that: chunk-descending, newest first, one pass at a time,
//! each pass reaching further back than the last.
//!
//! # The pass spills; the archive is the state
//!
//! A pass holds a bounded buffer of rows and flushes it to disk as a
//! **segment** as it walks ([`crate::segments`]). Nothing on the box outlives
//! the pass except the Mithril snapshot it read and the artifacts it wrote —
//! which is what lets the same walk run on a satellite that owns nothing but
//! a snapshot, publishes into R2 while it walks, and runs beside other
//! policies without the memory of each being the limit.
//!
//! # The buffer inverts
//!
//! Forward, the buffer holds outputs awaiting their spender. Backward, the
//! output that created an input sits at a LOWER slot — ground not covered
//! yet — so the state is [`Pending`]: transactions already found, waiting for
//! their sources to come into view. When a source appears, the spender's
//! missing negative delta is written as ONE MORE SIGNED ROW — a correction —
//! whichever pass found the spender. Readers sum by `(transaction, unit,
//! party)`, the same rule the sqlite ledger's `ON CONFLICT … amount +
//! excluded.amount` enforced, now applied at read time over immutable files;
//! compaction applies it once when the pass lands. A row lands readable and
//! is corrected later rather than withheld until perfect.
//!
//! # Resolution does not depend on contiguity
//!
//! With a **tx-index** over the same snapshot ([`Hooks::resolver`]) a job
//! looks each unbalanced spender's inputs up by hash and writes the source
//! rows inside the job. Nothing then ties one range to the range below it,
//! which is what lets several jobs walk one policy at once
//! (`crate::scheduler`, and step four of
//! `docs/design/POLICY_WALK_SCHEDULER.md`). What the index cannot answer —
//! a chunk newer than its base, an era it does not decode — still falls to
//! [`Pending`], and the descent's own resolution against the outputs it
//! passes is the fast path that settles it.
//!
//! # What bounds it
//!
//! `Pending` grows with unresolved inputs and shrinks as they resolve, so it
//! is bounded by the transactions still missing a source, never by supply.
//! Two rules keep it honest: inputs are registered ONLY for a transaction
//! whose deltas do not balance (conservation says exactly which are missing a
//! source), and a spender is RETIRED — its remaining inputs forgotten — the
//! moment its units balance, because a balanced transaction's other inputs
//! were carrying ADA, not the asset, and would never resolve. Measured on a
//! full ClayNation walk: 745,400 transactions found, 1,659,023 resolutions,
//! and nothing pending at the end.
//!
//! # The sieve gate stays complete
//!
//! A watched input's creating output necessarily carried the policy id, so
//! its block — and its chunk — is a sieve hit. Skipping non-hit chunks cannot
//! lose a resolution.

use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use mitos_chain_walk::decode::{OutRef, decode_tx};
use mitos_chain_walk::mithril::CHUNK_SLOTS;
use mitos_chain_walk::{open_blocks, slot_to_unix};
use pallas_primitives::Hash;
use pallas_traverse::MultiEraBlock;
use policy_archive::{Completeness, Movement, Stamp};

use crate::archive::{
    self, FileEntry, Manifest, PassEntry, PendingFile, PendingSpender, RangeKind, SlotRange,
};
use crate::registry;
use crate::segments::{self, SegmentWriter};
use crate::walk::{Watched, units_in_output};

/// Slots per day on Cardano — one slot per second.
const SLOTS_PER_DAY: u64 = 86_400;

#[derive(clap::Args, Debug)]
pub struct ReverseArgs {
    /// Data dir holding the immutable DB (expects `<data-dir>/immutable`).
    #[arg(long)]
    pub data_dir: PathBuf,

    /// Token registry TOML.
    #[arg(long, default_value = "tokens.toml")]
    pub tokens: PathBuf,

    /// Registered name, `<policy>.<name_hex>` unit, or a bare 56-hex POLICY.
    #[arg(long)]
    pub token: String,

    /// Archive root. The pass writes `<archive-dir>/<policy_hex>/pass-NNNN/`
    /// and replaces `<archive-dir>/<policy_hex>/manifest.json`.
    #[arg(long, default_value = "archive")]
    pub archive_dir: PathBuf,

    /// How much further back to reach, in days, measured from the archive's
    /// current floor (or from the tip on a cold policy).
    ///
    /// Absent — the hosted surface's shape — means ALL THE WAY: to the
    /// policy's first mint when the registry or `--to-slot` names it, else to
    /// genesis. A pass reads the chain in its window and nothing else,
    /// whatever the policy's supply; run it again to extend coverage downward
    /// and resolve sources for rows already published.
    #[arg(long)]
    pub days: Option<u64>,

    /// Absolute floor to stop at. With `--days` too, the NEARER bound wins —
    /// a preview is ten days with the mint as its floor. The hosted surface
    /// passes the policy's first mint here.
    #[arg(long)]
    pub to_slot: Option<u64>,

    /// Start here rather than at the bottom of what is already read. A
    /// stretch inside the pass that an earlier pass read is scanned for
    /// resolution only, never written twice.
    #[arg(long)]
    pub from_slot: Option<u64>,

    /// The policy's first mint, when the caller knows it (the hosted
    /// surface learns it from Koios). Recorded in the manifest; the bound
    /// below which nothing will ever look. Distinct from `--to-slot`, which
    /// is where THIS pass stops.
    #[arg(long)]
    pub first_mint: Option<u64>,

    /// Read this window as a SEEK: a reader asked for it, so it is a
    /// bounded window rather than a step of a descent. Only the summary
    /// line differs now that every job resolves through the index.
    #[arg(long)]
    pub seek: bool,

    /// A tx-index over the SAME snapshot (`tx-index build --index-dir`).
    /// With it, a job resolves its spenders' inputs by hash inside the job
    /// instead of waiting for the descent to reach them — which is what
    /// makes ranges independent, and so what lets several jobs walk one
    /// policy at once. The hosted surface passes the serve's index; this
    /// is how the CLI runs the same walk.
    #[arg(long)]
    pub tx_index_dir: Option<PathBuf>,

    /// Leave the pass as its segments rather than compacting them into one
    /// file at the end. For measuring, and for a box that would rather hand
    /// compaction to something else.
    #[arg(long)]
    pub no_compact: bool,

    /// Skip the memmem gate and decode every block. Only useful for isolating
    /// a suspected gate bug, since the gate is complete by the balance rule.
    #[arg(long)]
    pub no_sieve: bool,

    /// Write movements only — no observation tier.
    ///
    /// Ingestion speed is a first-class goal for this walk, so the cost of
    /// observing is a lever rather than a fact. Measure with and without on
    /// the policy in question before deciding: the marginal cost is a
    /// payment-credential test on every output, plus a datum copy and a decode
    /// for the ones at script addresses — which is small for a fungible token
    /// with a handful of pools, and is dominated by the CANDIDATE rule on a
    /// collection where every listing is a script output.
    #[arg(long)]
    pub no_observe: bool,

    /// Log a progress line every N chunks.
    #[arg(long, default_value_t = 250)]
    pub report_every: u64,
}

/// What a pass produced.
pub struct Outcome {
    /// Lowest slot now covered — the archive's new floor.
    pub floor: u64,
    /// Transactions the archive now holds from this pass. Fewer than the
    /// walk found: a transaction the asset only passed through as change
    /// moved nothing and has no rows. Equal to `found` when left
    /// uncompacted, since only compaction drops them.
    pub written: u64,
    /// Delta rows resolved onto transactions — this pass's, or earlier ones'.
    pub backfilled: u64,
    /// Inputs the tx-index answered INSIDE this job, rather than leaving
    /// for a descent that would have to reach them. What makes ranges
    /// independent; the figure step four of the scheduler design gates on.
    pub by_index: u64,
    /// Spenders still waiting on a source. The honest gap.
    pub unresolved: u64,
    /// Bytes of `pending.bin` — the carried state, measured.
    pub pending_bytes: u64,
}

/// Run one reverse pass. The CLI face: logs progress and prints a summary.
pub fn run(args: ReverseArgs) -> Result<()> {
    let report_every = args.report_every.max(1);
    let on: OnProgress<'_> = &|p| {
        // Chunks are ~6h of chain, so a full-history pass emits thousands. Log
        // every Nth, but ALWAYS log one that corrected rows or flushed a
        // segment: those are the events a consumer most needs to see.
        if p.chunks_done.is_multiple_of(report_every)
            || !p.updated.is_empty()
            || !p.flushed.is_empty()
        {
            tracing::info!(
                pct = format!("{:.1}%", p.fraction() * 100.0),
                floor = p.floor,
                date = %slot_date(p.floor),
                chunks = format!("{}/{}", p.chunks_done, p.chunks_total),
                written = p.written,
                updated = p.updated.len(),
                pending = p.pending,
                flushed = p.flushed.len(),
                "reverse: progress"
            );
        }
    };
    // The CLI walks alone: no lock, its own sequence. It resolves through
    // the index only when pointed at one — without it, the descent's own
    // resolution is the whole story, as it always was.
    let index = match &args.tx_index_dir {
        Some(dir) => {
            let index = tx_index::Index::open(dir, &args.data_dir.join("immutable"))
                .with_context(|| format!("opening tx-index at {}", dir.display()))?;
            let cov = index.coverage();
            tracing::info!(
                dir = %dir.display(),
                base_entries = cov.base_entries,
                newest_chunk = ?cov.newest_chunk,
                "reverse: tx-index open — inputs resolve inside the job"
            );
            Some(index)
        }
        None => None,
    };
    let out = run_reporting(
        args,
        Hooks {
            on,
            resolver: index.as_ref(),
            land_lock: None,
            seq: None,
        },
    )?;

    println!("transactions written        = {}", out.written);
    println!("delta rows backfilled       = {}", out.backfilled);
    println!("  of those, by the index    = {}", out.by_index);
    println!("sources still below floor   = {}", out.unresolved);
    println!("pending sidecar             = {} bytes", out.pending_bytes);
    if out.unresolved > 0 {
        println!(
            "  run again to reach deeper — each pass resolves sources for rows \
             already published"
        );
    }
    Ok(())
}

/// One reverse pass, reporting and interruptible through `hooks` — the
/// programmatic face.
pub fn run_reporting(args: ReverseArgs, hooks: Hooks<'_>) -> Result<Outcome> {
    let started = Instant::now();
    let token = registry::load_or_unit(&args.tokens, &args.token)?;
    let policy = token.policy_bytes()?;
    let policy_hex = hex::encode(&policy);
    let watched = match token.asset_name_bytes()? {
        Some(name) => Watched::Unit(name),
        None => Watched::Policy,
    };

    let immutable = args.data_dir.join("immutable");
    if !immutable.is_dir() {
        bail!(
            "immutable DB not found at {} — point --data-dir at a bootstrapped snapshot",
            immutable.display()
        );
    }

    let dir = archive::policy_dir(&args.archive_dir, &policy_hex);
    let mut manifest = archive::load_manifest(&dir)?.unwrap_or_else(|| Manifest::new(&policy_hex));

    let all_chunks = chain_sieve::list_chunks(&immutable, 0)?;
    let tip_slot = all_chunks
        .last()
        .map(|c| (c + 1) * CHUNK_SLOTS)
        .unwrap_or(0);

    // The descent continues DOWNWARD from the bottom of the TOP stretch —
    // not from the lowest slot ever read, which after a seek window may sit
    // far below with a hole above it. The hole is what the descent is for.
    // On a cold policy there is no stretch yet and the tip is the ceiling.
    //
    // IMMUTABLE stretches only. The volatile tail's rows are replaced on
    // every refresh, so a stretch it covers is not read: a top-up that
    // walks up into it must still write its own rows, or the next refresh
    // shrinks the tail out from under them.
    let covered = manifest.immutable_ranges();
    let ceiling = args
        .from_slot
        .unwrap_or_else(|| covered.last().map_or(tip_slot, |top| top.from));
    // Never dig below the policy's own first mint when we know it: there is
    // nothing there, and reading it is pure cost. `--to-slot` IS the first
    // mint when the hosted surface passes it (from Koios); counting it here
    // is what lets a pass that reaches it record COMPLETE — the first full
    // ClayNation walk reached its mint exactly and was stamped partial for
    // want of this. Recorded in the manifest so a later pass on a box
    // without the registry entry still stops there.
    let first_mint = token
        .floor_slot
        .or(manifest.first_mint_slot)
        .or(args.first_mint);
    let floor = pass_floor(ceiling, args.to_slot, args.days, first_mint);
    let mode = match args.seek {
        true => Mode::Seek,
        false => Mode::Descent,
    };

    // The carried state: everything any earlier pass was still waiting
    // for, wherever its sidecar is. The UNION, because passes need not be
    // contiguous any more; a spender already settled is harmless to load
    // again, since its outref is spent once on chain and resolves once.
    let prior = archive::load_pending_union(&dir, &manifest)?;
    let mut pending = Pending::load(prior.spenders);
    let carried = pending.len();

    if floor >= ceiling || manifest.uncovered(floor, ceiling).is_empty() {
        tracing::info!(
            ceiling,
            floor,
            "reverse: nothing to do — everything between floor and ceiling is read"
        );
        return Ok(Outcome {
            floor: ceiling,
            written: 0,
            backfilled: 0,
            by_index: 0,
            unresolved: carried as u64,
            pending_bytes: 0,
        });
    }

    tracing::info!(
        token = %token.name,
        policy = %policy_hex,
        policy_wide = matches!(watched, Watched::Policy),
        floor,
        ceiling,
        days = ?args.days,
        carried,
        "reverse: pass starting"
    );

    // The pass directory, fresh. A pass that died mid-walk left segments
    // here that no manifest names; they are not this pass's and would only
    // waste disk. The sequence comes from the caller when jobs run
    // concurrently on one policy — two jobs reading `next_seq` would agree.
    let seq = hooks.seq.unwrap_or_else(|| manifest.next_seq());
    let pass_dir = dir.join(PassEntry::dir_name(seq));
    if pass_dir.exists() {
        std::fs::remove_dir_all(&pass_dir)?;
    }
    let mut writer = SegmentWriter::new(
        &pass_dir,
        Stamp {
            policy_hex: policy_hex.clone(),
            completeness: manifest.completeness(),
            walk_from: manifest.walk_from(),
            walk_to: manifest.walk_to(),
            covered_from: 0,
            covered_to: 0,
            sealed_unix: now_unix(),
        },
    )?;

    let mut observations: Vec<policy_archive::Observation> = Vec::new();
    let walked = pass(
        &immutable,
        &all_chunks,
        &policy,
        &watched,
        floor,
        ceiling,
        covered,
        mode,
        &mut writer,
        &mut pending,
        !args.no_sieve,
        &hooks,
        (!args.no_observe).then_some(&mut observations),
    )?;
    let segments = writer.finish()?;

    let out = land(Landing {
        dir: &dir,
        seq,
        lock: hooks.land_lock,
        manifest: &mut manifest,
        policy_hex: &policy_hex,
        first_mint,
        ceiling,
        walked,
        segments,
        pending: &pending,
        compact: !args.no_compact,
        secs: started.elapsed().as_secs_f64(),
        observations,
    })?;

    tracing::info!(
        floor = out.floor,
        written = out.written,
        carried,
        backfilled = out.backfilled,
        by_index = out.by_index,
        unresolved = out.unresolved,
        pending_bytes = out.pending_bytes,
        secs = format!("{:.1}", started.elapsed().as_secs_f64()),
        "reverse: pass complete"
    );
    Ok(out)
}

/// Where a pass stops: the NEARER of an absolute floor and a day limit,
/// never below the first mint.
///
/// The first version let `to_slot` win outright, so a PREVIEW — ten days
/// with the mint as its floor — walked to the mint: a full pass running
/// inside the chunk hook of another full pass. Both bounds apply.
pub fn pass_floor(
    ceiling: u64,
    to_slot: Option<u64>,
    days: Option<u64>,
    first_mint: Option<u64>,
) -> u64 {
    let by_days = days.map(|d| ceiling.saturating_sub(d * SLOTS_PER_DAY));
    let asked = match (to_slot, by_days) {
        (Some(s), Some(d)) => s.max(d),
        (Some(s), None) => s,
        (None, Some(d)) => d,
        // Everything. Clamped to the first mint below when one is known.
        (None, None) => 0,
    };
    first_mint.map_or(asked, |first| asked.max(first))
}

/// Everything a finished walk hands to the landing.
struct Landing<'a> {
    dir: &'a Path,
    /// The pass's sequence — the directory its files are already in.
    seq: u32,
    /// Held across the manifest read-modify-write when jobs land
    /// concurrently on one policy. `None` for a lone CLI pass.
    lock: Option<&'a std::sync::Mutex<()>>,
    manifest: &'a mut Manifest,
    policy_hex: &'a str,
    first_mint: Option<u64>,
    ceiling: u64,
    walked: Walked,
    segments: Vec<FileEntry>,
    pending: &'a Pending,
    compact: bool,
    secs: f64,
    /// What the pass saw at script addresses, decoded or not.
    observations: Vec<policy_archive::Observation>,
}

/// Land one pass: compact its segments (or keep them), write the pending
/// sidecar, and LAST the manifest.
///
/// Separate from the chunk walk so the archive's write path can be exercised
/// against synthetic segments — the walk needs Mithril chunks, the archive
/// does not.
fn land(l: Landing<'_>) -> Result<Outcome> {
    let Landing {
        dir,
        seq,
        lock,
        manifest,
        policy_hex,
        first_mint,
        ceiling,
        walked,
        segments,
        pending,
        compact,
        secs,
        mut observations,
    } = l;
    // UNDER THE POLICY'S LOCK from here: another job may have landed since
    // this one loaded the manifest, so what is on disk is the truth and
    // this pass is appended to it.
    let _held = lock.map(|l| l.lock().expect("landing lock"));
    if let Some(fresh) = archive::load_manifest(dir)? {
        *manifest = fresh;
    }
    let pass_dir = dir.join(PassEntry::dir_name(seq));
    std::fs::create_dir_all(&pass_dir)?;
    // What the ledger will cover once this pass is in: for the file
    // stamp, which is written before the pass entry exists.
    let new_walk_from = Some(
        manifest
            .walk_from()
            .map_or(walked.floor, |f| f.min(walked.floor)),
    );
    let new_walk_to = Some(manifest.walk_to().map_or(ceiling, |t| t.max(ceiling)));

    // Has the archive reached the policy's beginning? Only two things
    // establish it: reaching genesis, or reaching the registered first mint.
    // A pass that stops short records PARTIAL — it just walked and did not
    // get there. A `Complete` archive is never demoted by a later pass.
    let reached_beginning = walked.floor == 0 || first_mint.is_some_and(|f| walked.floor <= f);
    let completeness = match (reached_beginning, manifest.completeness()) {
        (true, _) | (_, Completeness::Complete) => Completeness::Complete,
        (false, _) => Completeness::Partial,
    };
    let sealed_unix = now_unix();
    let stamp = Stamp {
        policy_hex: policy_hex.to_string(),
        completeness,
        walk_from: new_walk_from,
        walk_to: new_walk_to,
        covered_from: walked.floor,
        covered_to: ceiling.saturating_sub(1),
        sealed_unix,
    };

    let (movements, corrections, kept, written, units) = match compact {
        true => {
            let c = segments::compact(&pass_dir, &segments, ceiling, &stamp)?;
            (
                Some(c.movements),
                c.corrections,
                Vec::new(),
                c.written,
                c.units,
            )
        }
        false => (None, None, segments, walked.written, 0),
    };
    let pending_file = pending.to_file();
    let pending_bytes = archive::store_pending(&pass_dir.join(archive::PENDING), &pending_file)?;

    // Observations, if the pass took any. Written BEFORE the manifest for the
    // same reason every other file is: a reader that sees the pass sees its
    // files. Sorted by slot so the footer's statistics can be seeked on.
    if !observations.is_empty() {
        observations.sort_by_key(|o| (o.slot, o.address.clone()));
        let path = pass_dir.join(policy_archive::OBSERVATIONS);
        let mut w =
            policy_archive::ObservationWriter::new(std::fs::File::create(&path)?, &stamp)?;
        for o in observations {
            w.push(o);
        }
        let written = w.close()?;
        tracing::info!(
            rows = written.rows,
            bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0),
            "pass: observations written"
        );
    }

    // Coverage and completeness are DERIVED from the passes now; the
    // manifest's cached fields are refreshed on write.
    manifest.first_mint_slot = first_mint;
    manifest.updated_unix = sealed_unix;
    // MERGE, never replace. Each pass sees one window; a policy's fungible
    // unit may have moved only in a stretch this pass did not cover, and
    // overwriting would let the class flip as passes land out of order.
    // `complete` is the manifest's own coverage answer, not a pass's.
    let mut prof = manifest.profile.take().unwrap_or_default();
    prof.units_seen = prof.units_seen.max(walked.profile.units_seen);
    prof.fungible_units = prof.fungible_units.max(walked.profile.fungible_units);
    prof.single_units = prof.single_units.max(walked.profile.single_units);
    manifest.profile = Some(prof);
    manifest.passes.push(PassEntry {
        seq,
        dir: PassEntry::dir_name(seq),
        ceiling,
        floor: walked.floor,
        windows: Vec::new(),
        kind: RangeKind::Immutable,
        rolled_up: false,
        movements,
        corrections,
        segments: kept,
        pending: pending_file.spenders.len() as u64,
        found: walked.written,
        written,
        backfilled: walked.backfilled as u64,
        units,
        secs,
        written_unix: sealed_unix,
    });
    // LAST. A reader that opens the manifest sees a pass whose files exist.
    archive::store_manifest(dir, manifest)?;
    // And the bundle the push puts in KV, from the files that manifest names.
    archive::store_bundle(dir, manifest)?;

    // ROLLUP, once enough passes have piled up: every reader pays a footer
    // per file, and a satellite publishing to R2 wants one object per
    // policy. Its own manifest write, after this one, so a failure here
    // leaves a landed pass rather than a lost one.
    if compact {
        // IMMUTABLE passes only. The volatile tail is one file a reader
        // always opens and a rollup can never fold, so counting it toward
        // the threshold just fires the rollup a pass early — it does not
        // reduce anything.
        let loose = manifest
            .passes
            .iter()
            .filter(|p| !p.rolled_up && p.kind == RangeKind::Immutable)
            .count();
        if loose >= segments::ROLLUP_AFTER_PASSES {
            let t = Instant::now();
            segments::rollup(dir, manifest, now_unix(), segments::RollupReason::Routine)?;
            tracing::info!(
                passes = loose,
                secs = format!("{:.1}", t.elapsed().as_secs_f64()),
                "reverse: rolled up"
            );
        }
    }

    Ok(Outcome {
        floor: walked.floor,
        written,
        backfilled: walked.backfilled as u64,
        by_index: walked.by_index,
        unresolved: pending_file.spenders.len() as u64,
        pending_bytes,
    })
}

pub(crate) fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// A slot as `YYYY-MM-DD`, for saying how far back a pass has reached.
pub fn slot_date(slot: u64) -> String {
    unix_date(slot_to_unix(slot))
}

/// Unix seconds as `YYYY-MM-DD`. Civil-from-days (Hinnant), so this needs no
/// date dependency.
pub fn unix_date(unix: u64) -> String {
    let days = (unix / 86_400) as i64;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

// ─── one transaction, as found ───────────────────────────────────────────────

/// One transaction as the walk first meets it. Shared with the volatile
/// tail's forward follower ([`crate::tip`]) so there is ONE definition of
/// what rows a transaction produces.
pub struct PassTx {
    pub hash: Hash<32>,
    pub slot: u64,
    pub block_time: u64,
    /// Per unit: 0 / +mint / −burn.
    pub net_mint: Vec<(Vec<u8>, i64)>,
    /// `(unit, address, amount)` — its outputs.
    pub deltas: Vec<(Vec<u8>, String, i64)>,
}

impl PassTx {
    /// This transaction's rows as first found — its outputs, plus a
    /// placeholder for a minted-or-burned unit that reached nobody.
    /// Resolutions arrive later as further signed rows and fold on top.
    pub fn rows(&self) -> Vec<Movement> {
        let hash = self.hash.as_ref().to_vec();
        let mut rows: Vec<Movement> = self
            .deltas
            .iter()
            .map(|(unit, address, amount)| Movement {
                slot: self.slot,
                block_time: self.block_time,
                tx_hash: hash.clone(),
                unit_name: unit.clone(),
                address: address.clone(),
                amount: *amount,
                net_mint: self
                    .net_mint
                    .iter()
                    .find(|(n, _)| n == unit)
                    .map_or(0, |(_, a)| *a),
            })
            .collect();
        for (unit, amount) in &self.net_mint {
            if !self.deltas.iter().any(|(u, _, _)| u == unit) {
                rows.push(Movement {
                    slot: self.slot,
                    block_time: self.block_time,
                    tx_hash: hash.clone(),
                    unit_name: unit.clone(),
                    address: String::new(),
                    amount: 0,
                    net_mint: *amount,
                });
            }
        }
        rows
    }
}

// ─── pending ─────────────────────────────────────────────────────────────────

/// A transaction waiting on its sources.
struct Spender {
    hash: Hash<32>,
    slot: u64,
    block_time: u64,
    /// Per unit, the amount still unattributed. A unit at zero is settled.
    missing: HashMap<Vec<u8>, i64>,
    net_mint: Vec<(Vec<u8>, i64)>,
    outrefs: Vec<OutRef>,
    retired: bool,
    /// Loaded from an earlier sidecar rather than found by this job. When
    /// it settles here, the sidecar says so, or the next job to merge the
    /// older file would carry it forever.
    carried: bool,
}

impl Spender {
    fn settled(&self) -> bool {
        self.missing.values().all(|m| *m <= 0)
    }
}

/// One resolved negative delta, as the row it becomes.
struct Resolution {
    spender: Hash<32>,
    row: Movement,
}

/// What an out-of-band lookup of an outref can say about it — the three
/// answers [`Pending::settle_with`] acts on differently.
enum Lookup {
    /// The output existed and carried watched units: a source, resolved.
    Held {
        units: Vec<(Vec<u8>, i64)>,
        address: String,
    },
    /// The output existed and carried none of them, so it is not the source
    /// of anything missing. Forgotten, not carried.
    NotOurs,
    /// No answer — a chunk the index does not cover, an era it does not
    /// decode, a read that failed. Left pending, exactly as with no index.
    Unknown,
}

/// Outrefs spent by transactions already found, whose creating outputs are
/// still below the walk.
#[derive(Default)]
pub struct Pending {
    spenders: Vec<Spender>,
    /// outref → spenders waiting on it. An outref is spent once on chain, but
    /// the same spender can be re-registered across passes, so it is a list
    /// with duplicates refused.
    wants: HashMap<OutRef, Vec<usize>>,
}

impl Pending {
    /// Rebuild from what the previous pass left behind.
    pub fn load(spenders: Vec<PendingSpender>) -> Self {
        let mut p = Pending::default();
        for s in spenders {
            let idx = p.spenders.len();
            let outrefs: Vec<OutRef> = s
                .outrefs
                .iter()
                .map(|(h, i)| (Hash::from(*h), *i))
                .collect();
            for oref in &outrefs {
                p.wants.entry(*oref).or_default().push(idx);
            }
            p.spenders.push(Spender {
                hash: Hash::from(s.tx_hash),
                slot: s.slot,
                block_time: s.block_time,
                missing: s.missing.into_iter().collect(),
                net_mint: s.net_mint,
                outrefs,
                retired: false,
                carried: true,
            });
        }
        p
    }

    /// Register a transaction found in this pass whose deltas do not balance.
    fn want(&mut self, tx: &PassTx, missing: HashMap<Vec<u8>, i64>, inputs: &[OutRef]) {
        let idx = self.spenders.len();
        for oref in inputs {
            let slot = self.wants.entry(*oref).or_default();
            if !slot.contains(&idx) {
                slot.push(idx);
            }
        }
        self.spenders.push(Spender {
            hash: tx.hash,
            slot: tx.slot,
            block_time: tx.block_time,
            missing,
            net_mint: tx.net_mint.clone(),
            outrefs: inputs.to_vec(),
            retired: false,
            carried: false,
        });
    }

    /// An output has come into view. Everyone waiting on it learns who held
    /// it and how much — their missing negatives — and a spender whose units
    /// now balance is retired along with its remaining wants.
    fn resolve(
        &mut self,
        oref: &OutRef,
        units: &[(Vec<u8>, i64)],
        address: &str,
    ) -> Vec<Resolution> {
        let Some(waiting) = self.wants.remove(oref) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for idx in waiting {
            let s = &mut self.spenders[idx];
            if s.retired {
                continue;
            }
            for (unit, qty) in units {
                if let Some(m) = s.missing.get_mut(unit) {
                    *m -= qty;
                }
                let net_mint = s
                    .net_mint
                    .iter()
                    .find(|(n, _)| n == unit)
                    .map_or(0, |(_, a)| *a);
                out.push(Resolution {
                    spender: s.hash,
                    row: Movement {
                        slot: s.slot,
                        block_time: s.block_time,
                        tx_hash: s.hash.as_ref().to_vec(),
                        unit_name: unit.clone(),
                        address: address.to_string(),
                        amount: -qty,
                        net_mint,
                    },
                });
            }
            s.outrefs.retain(|o| o != oref);
            if s.settled() {
                // Its other inputs were carrying ADA. Forget them, or they
                // sit in the pending set forever — measured at 1,062,528
                // entries on a ledger that was provably complete.
                s.retired = true;
                let leftovers = std::mem::take(&mut s.outrefs);
                for o in leftovers {
                    if let Some(list) = self.wants.get_mut(&o) {
                        list.retain(|i| *i != idx);
                        if list.is_empty() {
                            self.wants.remove(&o);
                        }
                    }
                }
            }
        }
        out
    }

    /// Drop an outref nobody can be waiting on any more: the index answered
    /// it and the output carried none of the watched units, so it is not the
    /// source of anything missing. Without this the spender carries a dead
    /// ADA input in its sidecar for ever.
    fn forget(&mut self, oref: &OutRef) {
        for idx in self.wants.remove(oref).unwrap_or_default() {
            self.spenders[idx].outrefs.retain(|o| o != oref);
        }
    }

    /// Settle what the walk itself did not meet, through the tx-index.
    ///
    /// **Run once, at the END of a job.** A source inside the job's own
    /// range costs nothing to resolve because the descent walks past it and
    /// [`Pending::resolve`] fires for free; only sources BELOW the range are
    /// worth a lookup. Measured on a 60-day ClayNation window: resolving
    /// eagerly, per transaction, cost 617 µs per input cold — a quarter of
    /// an hour added to a full walk — against a few hundred lookups per job
    /// deferred, for the same archive.
    ///
    /// Only this job's OWN spenders. A carried one — loaded from an earlier
    /// job's sidecar — may be waiting on a source that another job running
    /// right now will walk past, and two jobs both writing that correction
    /// would double it under the sum rule. Carried spenders settle the way
    /// they always have: the descent reaches their source.
    ///
    /// What the index cannot answer (a chunk newer than its base, an era it
    /// does not decode) stays pending, exactly as without an index.
    fn settle_through_index(
        &mut self,
        index: &tx_index::Index,
        policy_hex: &str,
        watched: &Watched,
        counted: &mut u64,
    ) -> Vec<Resolution> {
        self.settle_with(|oref| {
            let Some(hash) = oref.0.as_ref().first_chunk::<32>() else {
                return Lookup::Unknown;
            };
            match index.resolve(hash, oref.1) {
                Ok(tx_index::Resolution::Found { output, .. }) => {
                    *counted += 1;
                    let units: Vec<(Vec<u8>, i64)> = output
                        .assets
                        .iter()
                        .filter(|a| a.policy == policy_hex)
                        .filter_map(|a| {
                            let name = hex::decode(&a.name).ok()?;
                            watched
                                .matches(&name)
                                .then_some((name, i64::try_from(a.quantity).unwrap_or(i64::MAX)))
                        })
                        .collect();
                    match units.is_empty() {
                        true => Lookup::NotOurs,
                        false => Lookup::Held {
                            units,
                            address: output.address.clone(),
                        },
                    }
                }
                // Not indexed: the descent will meet it, or a lower job will.
                Ok(_) => Lookup::Unknown,
                Err(e) => {
                    tracing::debug!(error = %e, "reverse: index lookup failed; leaving it pending");
                    Lookup::Unknown
                }
            }
        })
    }

    /// [`Pending::settle_through_index`] with the lookup as an argument, so
    /// the rule that decides WHICH spenders are offered to it can be tested
    /// without a 123-million-entry index on disk.
    fn settle_with(&mut self, mut lookup: impl FnMut(&OutRef) -> Lookup) -> Vec<Resolution> {
        // A snapshot: `resolve` removes outrefs as it goes, and retiring a
        // settled spender takes its siblings' wants with it.
        let open: Vec<OutRef> = self
            .spenders
            .iter()
            .filter(|s| !s.carried && !s.retired)
            .flat_map(|s| s.outrefs.clone())
            .collect();
        let mut out = Vec::new();
        for oref in open {
            if !self.wants.contains_key(&oref) {
                continue;
            }
            match lookup(&oref) {
                Lookup::Held { units, address } => {
                    out.extend(self.resolve(&oref, &units, &address))
                }
                Lookup::NotOurs => self.forget(&oref),
                Lookup::Unknown => {}
            }
        }
        out
    }

    /// Spenders still waiting.
    pub fn len(&self) -> usize {
        self.spenders
            .iter()
            .filter(|s| !s.retired && !s.outrefs.is_empty())
            .count()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The carried state, as the next pass will load it — plus which of the
    /// spenders it loaded are settled now.
    fn to_file(&self) -> PendingFile {
        PendingFile {
            spenders: self
                .spenders
                .iter()
                .filter(|s| !s.retired && !s.outrefs.is_empty())
                .map(|s| PendingSpender {
                    tx_hash: *s.hash.as_ref().first_chunk::<32>().expect("32-byte hash"),
                    slot: s.slot,
                    block_time: s.block_time,
                    missing: s.missing.iter().map(|(k, v)| (k.clone(), *v)).collect(),
                    net_mint: s.net_mint.clone(),
                    outrefs: s
                        .outrefs
                        .iter()
                        .map(|(h, i)| (*h.as_ref().first_chunk::<32>().expect("32-byte hash"), *i))
                        .collect(),
                })
                .collect(),
            settled: self
                .spenders
                .iter()
                .filter(|s| s.carried && (s.retired || s.outrefs.is_empty()))
                .map(|s| *s.hash.as_ref().first_chunk::<32>().expect("32-byte hash"))
                .collect(),
        }
    }
}

// ─── the pass ────────────────────────────────────────────────────────────────

/// Where a pass has got to, emitted after every chunk.
pub struct Progress<'a> {
    /// Lowest slot covered so far. Moves DOWN as the pass runs.
    pub floor: u64,
    /// Where this pass is heading — the requested floor.
    pub target_floor: u64,
    /// Where it started.
    pub ceiling: u64,
    pub chunks_done: u64,
    pub chunks_total: u64,
    /// Transactions found so far by this pass.
    pub written: u64,
    /// Transactions whose deltas CHANGED in this chunk — a source resolved.
    pub updated: &'a [Hash<32>],
    /// Spenders still waiting on a source below the floor.
    pub pending: usize,
    /// THE LOWER LOD: every archive row this chunk produced — new
    /// transactions' rows and every resolution as a signed row — in the same
    /// shape the Parquet will hold. A hosted surface mirrors these into a
    /// small buffer until `flushed` says they are on disk.
    pub rows: &'a [Movement],
    /// Segments written at the end of this chunk, if the buffer was flushed.
    /// They hold everything buffered so far INCLUDING `rows`, so a mirror
    /// clears its buffer on this rather than appending.
    pub flushed: &'a [FileEntry],
}

impl Progress<'_> {
    /// How far through the requested range, 0.0–1.0. Saturating: a pass with
    /// nothing to do reads as finished, not as a division by zero.
    pub fn fraction(&self) -> f64 {
        let span = self.ceiling.saturating_sub(self.target_floor);
        if span == 0 {
            return 1.0;
        }
        let done = self.ceiling.saturating_sub(self.floor);
        (done as f64 / span as f64).clamp(0.0, 1.0)
    }
}

pub type OnProgress<'x> = &'x dyn Fn(Progress<'_>);

/// What the hosted surface hooks into a pass. A CLI run reports and
/// nothing else.
///
/// A pass is ONE JOB over one range now — the scheduler decides which
/// ranges, in what order, on which thread (`crate::scheduler`). Nothing
/// here interrupts a pass or nests one inside another.
pub struct Hooks<'a> {
    pub on: OnProgress<'a>,
    /// tx hash → body over the same snapshot. With it, EVERY job resolves
    /// its spenders' inputs inside the job, so no job depends on the range
    /// below it having been read; without it, a spender waits in the
    /// pending set for a descent to reach its source.
    pub resolver: Option<&'a tx_index::Index>,
    /// Held across the manifest read-modify-write at landing, when jobs on
    /// one policy run concurrently.
    pub land_lock: Option<&'a std::sync::Mutex<()>>,
    /// The pass's sequence, allocated by the caller for the same reason.
    /// `None` reads the manifest's next.
    pub seq: Option<u32>,
}

/// What the chunk walk itself produced, before anything lands.
struct Walked {
    floor: u64,
    written: u64,
    backfilled: usize,
    /// Inputs the index answered inside the job — see [`Outcome::by_index`].
    by_index: u64,
    /// What this pass learned about the policy's units. MERGED into the
    /// manifest's rather than replacing it: a pass sees its own window, and a
    /// ten-day window can miss a unit class the policy has had for years.
    profile: policy_archive::Profile,
}

/// Chunk numbers to visit, highest first, covering `[floor, ceiling)`.
///
/// The chunk LIST comes from the directory rather than from arithmetic: chunk
/// files are dense in practice and sparse in principle, and inventing numbers
/// that are not on disk turns a gap into a read error mid-pass.
pub fn chunks_descending(chunks: &[u64], floor: u64, ceiling: u64) -> Vec<u64> {
    let lo = floor / CHUNK_SLOTS;
    let hi = ceiling.div_ceil(CHUNK_SLOTS);
    let mut wanted: Vec<u64> = chunks
        .iter()
        .copied()
        .filter(|c| *c >= lo && *c <= hi)
        .collect();
    wanted.sort_unstable_by(|a, b| b.cmp(a));
    wanted
}

fn chunk_path(immutable: &Path, chunk: u64) -> PathBuf {
    immutable.join(format!("{chunk:05}.chunk"))
}

/// Drop one chunk's pages from the page cache, once this job is finished
/// with it.
///
/// cardano-infra runs market-ledger, two mitos nodes and a tx-index serve
/// against the same 216 GB snapshot and the same 31 GB of RAM. A full walk
/// streams every chunk through the sieve gate and reads almost none of them
/// twice, so without this it evicts every other service's working set on the
/// way past — the co-tenancy condition in
/// `docs/design/POLICY_WALK_SCHEDULER.md`.
///
/// Called at the END of a chunk, not at the end of the sieve read: the
/// blocks are decoded and the index resolutions taken after the gate, and a
/// spender's source is very often an output in the same chunk.
///
/// Best effort. `POSIX_FADV_DONTNEED` drops clean pages only, which is every
/// page of a file nothing writes; a failure costs cache, never correctness.
/// Linux only — a dev Mac has no `posix_fadvise` and nothing to protect.
fn drop_chunk_pages(immutable: &Path, chunk: u64) {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        if let Ok(f) = File::open(chunk_path(immutable, chunk)) {
            // SAFETY: a live fd, and the advice takes no buffer.
            unsafe { libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (immutable, chunk);
}

/// One chunk's blocks, newest first, gated at the chunk AND the block.
///
/// The immutable DB only reads FORWARD, so "reverse" is chunk-descending with
/// each chunk read forward and flipped. The chunk-level gate — one `fs::read`
/// plus one memmem — skips the whole file on a miss, which is almost every
/// file, since a policy's activity is a thin slice of the chain.
fn chunk_blocks_newest_first(
    immutable: &Path,
    chunk: u64,
    needles: Option<&chain_sieve::Needles<'_>>,
) -> Result<Vec<(u64, Vec<u8>)>> {
    let start = chunk * CHUNK_SLOTS;
    let end = (chunk + 1) * CHUNK_SLOTS;

    if let Some(n) = needles {
        let path = chunk_path(immutable, chunk);
        let mut file = File::open(&path).with_context(|| format!("opening {}", path.display()))?;
        let mut bytes = Vec::with_capacity(file.metadata()?.len() as usize);
        file.read_to_end(&mut bytes)
            .with_context(|| format!("reading {}", path.display()))?;
        if !n.hit(&bytes) {
            return Ok(Vec::new());
        }
    }

    let blocks = open_blocks(immutable, Some((start, Vec::new())))
        .with_context(|| format!("seeking chunk {chunk}"))?;
    let mut out = Vec::new();
    for raw in blocks {
        let raw = raw.map_err(|e| anyhow::anyhow!("reading block in chunk {chunk}: {e:?}"))?;
        if let Some(n) = needles
            && !n.hit(&raw)
        {
            continue;
        }
        let slot = match MultiEraBlock::decode(&raw) {
            Ok(b) => b.slot(),
            Err(e) => return Err(anyhow::anyhow!("decoding block in chunk {chunk}: {e:?}")),
        };
        if slot >= end {
            break;
        }
        if slot < start {
            continue;
        }
        out.push((slot, raw));
    }
    out.reverse();
    Ok(out)
}

/// Which walk a range is read for. Both resolve their inputs through the
/// index and both scan an already-read stretch for resolution only — the
/// difference is who asked and how the job is reported, not what it does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// A step of a descent: the next stretch below what a policy has read.
    Descent,
    /// A bounded window a reader asked for, out of the descent's order.
    Seek,
}

/// What one chunk produced.
struct ChunkOut {
    /// Every archive row — own rows and resolutions — for the live view.
    rows: Vec<Movement>,
    /// Spenders whose deltas changed.
    updated: Vec<Hash<32>>,
    /// Lowest block slot read, if any block was in range.
    lowest: Option<u64>,
}

/// The per-chunk machinery, shared by the descent and its detours.
struct Scan<'a> {
    immutable: &'a Path,
    policy: &'a [u8],
    policy_hex: String,
    watched: &'a Watched,
    needles: Option<&'a chain_sieve::Needles<'a>>,
    writer: &'a mut SegmentWriter,
    pending: &'a mut Pending,
    resolver: Option<&'a tx_index::Index>,
    /// Stretches ALREADY READ — by an earlier pass, or by a detour of this
    /// one. A block inside one is scanned for RESOLUTION ONLY: its outputs
    /// settle spenders found above it, and its transactions are not
    /// written again, so each transaction has exactly one set of own rows
    /// however many jobs pass over it.
    covered: Vec<SlotRange>,
    written: u64,
    backfilled: usize,
    /// Inputs the index answered inside this job rather than the descent.
    resolved_by_index: u64,
    /// Who may annotate a script output. `None` records raw candidates only —
    /// which is still worth doing, because the bytes a decoder would need are
    /// what the archive is keeping.
    observers: Option<&'a [Box<dyn crate::observer::OutputObserver>]>,
    /// `None` on the fast-ingest path — the tier is skipped whole.
    observations: Option<&'a mut Vec<policy_archive::Observation>>,
    /// Tier-1 profile, accumulated as units come into view. Costs a set
    /// insert per unit sighting and answers what the policy IS.
    profile: policy_archive::Profile,
    units_seen: std::collections::HashSet<Vec<u8>>,
    fungible_units: std::collections::HashSet<Vec<u8>>,
}

impl Scan<'_> {
    /// One chunk of the immutable directory.
    fn chunk(&mut self, chunk: u64, lo: u64, hi: u64) -> Result<ChunkOut> {
        let blocks = chunk_blocks_newest_first(self.immutable, chunk, self.needles)?;
        let out = self.blocks(blocks, lo, hi)?;
        // Done with this chunk: hand its pages back rather than evict the
        // other services on the box.
        drop_chunk_pages(self.immutable, chunk);
        Ok(out)
    }

    /// Blocks ALREADY IN HAND, newest first — the seam the volatile tail
    /// comes in through ([`crate::tip`]). Blocks arrive gated: the chunk
    /// path gates in `chunk_blocks_newest_first`, the spool path gates once
    /// for every watched policy at load.
    ///
    /// This is the ONE definition of what rows a transaction produces. The
    /// tail used to have its own, forward, with its own outref buffer — two
    /// implementations of the same derivation, which is the drift the
    /// manifest was moved into the crate to avoid.
    fn blocks(&mut self, blocks: Vec<(u64, Vec<u8>)>, lo: u64, hi: u64) -> Result<ChunkOut> {
        // Resolutions found in this chunk. A source and its spender can sit
        // in the SAME chunk; as rows they simply sum, so order is free.
        let mut resolved: Vec<Resolution> = Vec::new();
        let mut rows: Vec<Movement> = Vec::new();
        let mut lowest: Option<u64> = None;

        for (slot, raw) in blocks {
            if slot < lo || slot >= hi {
                continue;
            }
            lowest = Some(lowest.map_or(slot, |l| l.min(slot)));
            let read_before = self.covered.iter().any(|r| r.contains(slot));
            let blk = MultiEraBlock::decode(&raw)
                .map_err(|e| anyhow::anyhow!("decoding block at slot {slot}: {e:?}"))?;
            let block_time = slot_to_unix(slot);

            let txs: Vec<_> = blk.txs();
            for tx in txs.iter().rev() {
                let dtx = decode_tx(tx);

                let mut net_mint: HashMap<Vec<u8>, i64> = HashMap::new();
                for pa in tx.mints().iter() {
                    if pa.policy().as_ref() != self.policy {
                        continue;
                    }
                    for a in pa.assets().iter() {
                        if !self.watched.matches(a.name()) {
                            continue;
                        }
                        *net_mint.entry(a.name().to_vec()).or_insert(0) +=
                            a.mint_coin().unwrap_or(0);
                    }
                }

                // OUTPUTS: this transaction's own positive deltas, AND the
                // resolution of whatever spent them later.
                let mut deltas: Vec<(Vec<u8>, String, i64)> = Vec::new();
                let mut seen: Vec<policy_archive::Observation> = Vec::new();
                for out in &dtx.outputs {
                    let units = units_in_output(tx, out, self.policy, self.watched);
                    if units.is_empty() {
                        continue;
                    }
                    for (name, qty) in &units {
                        deltas.push((name.clone(), out.address.clone(), *qty));
                    }
                    // Only SCRIPT-held outputs. A wallet holding the token is
                    // a movement and nothing more; there is no state on it to
                    // observe, and recording every one would multiply the file
                    // by the holder count for no reader.
                    // Every unit sighting feeds the profile, script-held or
                    // not — what a policy IS does not depend on where it sits.
                    for (name, qty) in &units {
                        let was = self.fungible_units.contains(name);
                        let first = self.units_seen.insert(name.clone());
                        self.profile.observe(first, was, *qty);
                        if *qty > 1 {
                            self.fungible_units.insert(name.clone());
                        }
                    }
                    if mitos_cohort::is_script_address(&out.address) {
                        let datum: Option<&[u8]> = out
                            .datum_hash
                            .as_ref()
                            .and_then(|h| dtx.witness_datums.get(h))
                            .map(|v| v.as_slice())
                            .or(out.inline_datum.as_deref());
                        for (name, qty) in &units {
                            let decoded = self.observers.and_then(|obs| {
                                crate::observer::observe_all(
                                    obs, out, *qty, self.policy, name, datum,
                                )
                            });
                            // PROFILE GATE. A decoded observation is always
                            // worth keeping; an undecoded CANDIDATE is only
                            // worth keeping where a pool could plausibly be —
                            // i.e. where the output holds a quantity. On a
                            // collection every listing escrow holds exactly
                            // one, and keeping those means millions of rows
                            // whose meaning is market-ledger's to supply, not
                            // this archive's.
                            if decoded.is_none()
                                && !policy_archive::Profile::keep_candidate(*qty)
                            {
                                continue;
                            }
                            seen.push(policy_archive::Observation {
                                slot,
                                block_time,
                                tx_hash: dtx.tx_hash.to_vec(),
                                address: out.address.clone(),
                                lovelace: out.lovelace as i64,
                                unit_name: name.clone(),
                                unit_amount: *qty,
                                datum: datum.map(|d| d.to_vec()),
                                decoded,
                            });
                        }
                    }
                    resolved.extend(self.pending.resolve(
                        &(dtx.tx_hash, out.index),
                        &units,
                        &out.address,
                    ));
                }

                let touches_us = !deltas.is_empty() || !net_mint.is_empty();
                if !touches_us {
                    continue;
                }
                // READ BEFORE — by an earlier pass or a detour. Its outputs
                // have just resolved whatever was waiting on them; its rows
                // exist. Writing them again would double them in the
                // sum-merge, which is what happened on 901ba6e9 when one
                // window was read four times.
                if read_before {
                    continue;
                }
                // Kept only past the read-before gate, for the same reason the
                // movement rows are: a window read twice would double them.
                if let Some(sink) = self.observations.as_mut() {
                    sink.append(&mut seen);
                }

                // INPUTS: register interest ONLY where a source is missing —
                // per unit, by how much. Conservation says exactly that.
                let mut sums: HashMap<Vec<u8>, i64> = HashMap::new();
                for (unit, _, amount) in &deltas {
                    *sums.entry(unit.clone()).or_insert(0) += amount;
                }
                for unit in net_mint.keys() {
                    sums.entry(unit.clone()).or_insert(0);
                }
                let missing: HashMap<Vec<u8>, i64> = sums
                    .into_iter()
                    .filter_map(|(unit, sum)| {
                        let minted = net_mint.get(&unit).copied().unwrap_or(0);
                        (sum > minted).then_some((unit, sum - minted))
                    })
                    .collect();

                let row = PassTx {
                    hash: dtx.tx_hash,
                    slot,
                    block_time,
                    net_mint: net_mint.into_iter().collect(),
                    deltas,
                };
                if !missing.is_empty() {
                    // Interest, not a lookup: the descent is about to walk
                    // past most of these sources for nothing. Whatever it
                    // does not meet is settled through the index at the end
                    // of the job — see [`Pending::settle_through_index`].
                    let inputs: Vec<OutRef> = dtx.inputs.iter().map(|i| i.oref).collect();
                    self.pending.want(&row, missing, &inputs);
                }
                let own = row.rows();
                rows.extend(own.iter().cloned());
                self.writer.push_own(own);
                self.written += 1;
            }
        }

        // This chunk's resolutions, as correction rows — whichever pass the
        // spender came from. Compaction folds this pass's own onto their
        // transactions; the reader sums the rest.
        let mut updated: Vec<Hash<32>> = Vec::new();
        for r in resolved {
            self.backfilled += 1;
            if !updated.contains(&r.spender) {
                updated.push(r.spender);
            }
            rows.push(r.row.clone());
            self.writer.push_corr(r.row);
        }
        Ok(ChunkOut {
            rows,
            updated,
            lowest,
        })
    }

    /// After the last chunk: settle what the walk did not meet, through the
    /// index, and write those resolutions as correction rows like any other.
    /// The rows come back so the live tier sees them before the job lands.
    ///
    /// This is what makes a range independent of the range below it, and so
    /// what lets several jobs walk one policy at once. Nothing without an
    /// index: the spenders stay in the sidecar for a lower job to meet.
    fn settle(&mut self) -> (Vec<Movement>, Vec<Hash<32>>) {
        let Some(index) = self.resolver else {
            return (Vec::new(), Vec::new());
        };
        let settled = self.pending.settle_through_index(
            index,
            &self.policy_hex,
            self.watched,
            &mut self.resolved_by_index,
        );
        let mut rows = Vec::with_capacity(settled.len());
        let mut updated: Vec<Hash<32>> = Vec::new();
        for r in settled {
            self.backfilled += 1;
            if !updated.contains(&r.spender) {
                updated.push(r.spender);
            }
            rows.push(r.row.clone());
            self.writer.push_corr(r.row);
        }
        (rows, updated)
    }
}

/// Read blocks ALREADY IN HAND as one range — the volatile tail's entry
/// point ([`crate::tip`]).
///
/// The stretch above the immutable tip is not in any chunk file, so its
/// blocks come from wallet-sieve's chain-tail spool. Everything after that
/// is identical to a descent over chunks: the same [`Scan`], the same
/// `(tx, unit, party)` rows, the same [`Pending`] for sources inside the
/// range, and the same end-of-range settle through the index for sources
/// below it — which here means below the immutable tip, exactly where the
/// index's coverage ends.
///
/// `blocks` must be NEWEST FIRST and already gated.
pub fn scan_blocks(
    blocks: Vec<(u64, Vec<u8>)>,
    policy: &[u8],
    watched: &Watched,
    floor: u64,
    ceiling: u64,
    writer: &mut SegmentWriter,
    resolver: Option<&tx_index::Index>,
) -> Result<Outcome> {
    // The tail carries NO state between refreshes: it is re-derived whole
    // every time, because it is replaced whole every time. So a fresh
    // pending set, and whatever it cannot settle is an honest gap rather
    // than something to hand to the next pass.
    let mut pending = Pending::default();
    let mut scan = Scan {
        immutable: Path::new(""),
        policy,
        policy_hex: hex::encode(policy),
        watched,
        needles: None,
        writer,
        pending: &mut pending,
        resolver,
        covered: Vec::new(),
        written: 0,
        backfilled: 0,
        resolved_by_index: 0,
        profile: policy_archive::Profile::default(),
        units_seen: std::collections::HashSet::new(),
        fungible_units: std::collections::HashSet::new(),
        // The chain tail is re-derived whole on every refresh, so anything
        // observed here would be written again by the immutable pass that
        // later covers the same slots. Observations come from the archive's
        // own passes only.
        observers: None,
        observations: None,
    };
    // The rows went to the writer; nothing here needs them a second time.
    scan.blocks(blocks, floor, ceiling)?;
    scan.settle();
    Ok(Outcome {
        floor,
        written: scan.written,
        // `settle` counts its own resolutions into `backfilled` too.
        backfilled: scan.backfilled as u64,
        by_index: scan.resolved_by_index,
        unresolved: scan.pending.len() as u64,
        pending_bytes: 0,
    })
}

/// Walk `[floor, ceiling)` backward into segments, resolving sources as they
/// come into view.
#[allow(clippy::too_many_arguments)]
fn pass(
    immutable: &Path,
    chunks: &[u64],
    policy: &[u8],
    watched: &Watched,
    floor: u64,
    ceiling: u64,
    covered: Vec<SlotRange>,
    mode: Mode,
    writer: &mut SegmentWriter,
    pending: &mut Pending,
    sieve: bool,
    hooks: &Hooks<'_>,
    // Held for the pass rather than spilled like movements. Observations are
    // an order of magnitude sparser — $PERP has ~5,000 pool states against
    // 29,545 movements — so the segment machinery would be ceremony. If a
    // policy is ever found where this is not true, the measurement will say so
    // before the memory does.
    //
    // `None` skips the tier entirely (`--no-observe`), which is the fast-ingest
    // path: no credential test, no datum copy, no decode.
    observations: Option<&mut Vec<policy_archive::Observation>>,
) -> Result<Walked> {
    let policy_vec = policy.to_vec();
    let needles = sieve
        .then(|| chain_sieve::Needles::new(std::slice::from_ref(&policy_vec)))
        .transpose()?;
    let observers = crate::observer::default_observers();
    let mut scan = Scan {
        immutable,
        policy,
        policy_hex: hex::encode(policy),
        watched,
        needles: needles.as_ref(),
        writer,
        pending,
        resolver: hooks.resolver,
        covered,
        written: 0,
        backfilled: 0,
        resolved_by_index: 0,
        profile: policy_archive::Profile::default(),
        units_seen: std::collections::HashSet::new(),
        fungible_units: std::collections::HashSet::new(),
        // Both or neither: an observer with nowhere to put its answer would
        // decode for nothing.
        observers: observations.is_some().then_some(&observers),
        observations,
    };

    let mut lowest = ceiling;
    let ordered = chunks_descending(chunks, floor, ceiling);
    let chunks_total = ordered.len() as u64;

    for (done, chunk) in ordered.into_iter().enumerate() {
        let chunks_done = done as u64 + 1;
        let out = scan.chunk(chunk, floor, ceiling)?;
        if let Some(l) = out.lowest {
            lowest = lowest.min(l);
        }
        let flushed = scan.writer.end_chunk()?;
        (hooks.on)(Progress {
            floor: lowest,
            target_floor: floor,
            ceiling,
            chunks_done,
            chunks_total,
            written: scan.written,
            updated: &out.updated,
            pending: scan.pending.len(),
            rows: &out.rows,
            flushed: &flushed,
        });
    }
    // THE LEFTOVERS, once and at the end: everything the descent walked
    // past has already resolved itself for nothing.
    let waiting_before = scan.pending.len();
    let (settled, updated) = scan.settle();
    let flushed = scan.writer.end_chunk()?;
    (hooks.on)(Progress {
        floor: lowest,
        target_floor: floor,
        ceiling,
        chunks_done: chunks_total,
        chunks_total,
        written: scan.written,
        updated: &updated,
        pending: scan.pending.len(),
        rows: &settled,
        flushed: &flushed,
    });
    tracing::info!(
        floor,
        ceiling,
        ?mode,
        waiting_before,
        by_index = scan.resolved_by_index,
        still_waiting = scan.pending.len(),
        written = scan.written,
        "reverse: range read"
    );

    // Coverage is where the pass STOPPED LOOKING, not the deepest row it
    // found: a quiet stretch below the last hit was read and held nothing,
    // which is a fact worth keeping. Only a range with no chunks on disk at
    // all extends nothing.
    Ok(Walked {
        floor: if chunks_total == 0 { ceiling } else { floor },
        written: scan.written,
        backfilled: scan.backfilled,
        by_index: scan.resolved_by_index,
        profile: scan.profile,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::PolicyArchive;

    fn h(b: u8) -> Hash<32> {
        Hash::from([b; 32])
    }

    fn tx(b: u8, deltas: Vec<(&str, &str, i64)>) -> PassTx {
        PassTx {
            hash: h(b),
            slot: 1_000 + b as u64,
            block_time: 1_700_000_000 + b as u64,
            net_mint: Vec::new(),
            deltas: deltas
                .into_iter()
                .map(|(u, a, n)| (u.as_bytes().to_vec(), a.to_string(), n))
                .collect(),
        }
    }

    fn missing(unit: &str, n: i64) -> HashMap<Vec<u8>, i64> {
        HashMap::from([(unit.as_bytes().to_vec(), n)])
    }

    /// Resolving an outref hands its holder to the spender and removes it
    /// from the open set — a second sighting resolves nothing.
    #[test]
    fn resolving_an_outref_removes_it_from_the_open_set() {
        let mut p = Pending::default();
        let spender = tx(1, vec![("A", "bob", 1)]);
        p.want(&spender, missing("A", 1), &[(h(9), 0), (h(9), 1)]);
        let got = p.resolve(&(h(9), 0), &[(b"A".to_vec(), 1)], "alice");
        assert_eq!(got.len(), 1);
        assert_eq!(
            (got[0].row.amount, got[0].row.address.as_str()),
            (-1, "alice")
        );
        assert_eq!(got[0].row.slot, 1_001, "the row sits at the SPENDER's slot");
        assert!(
            p.resolve(&(h(9), 0), &[(b"A".to_vec(), 1)], "alice")
                .is_empty()
        );
    }

    /// THE RETIREMENT RULE. Once the spender's units balance, its OTHER
    /// inputs are forgotten — they were carrying ADA and would wait forever.
    #[test]
    fn a_balanced_spender_forgets_its_other_inputs() {
        let mut p = Pending::default();
        let spender = tx(1, vec![("A", "bob", 1)]);
        p.want(&spender, missing("A", 1), &[(h(9), 0), (h(8), 3)]);
        assert_eq!(p.len(), 1);
        p.resolve(&(h(9), 0), &[(b"A".to_vec(), 1)], "alice");
        assert!(p.is_empty(), "settled, so nothing is pending");
        assert!(
            p.resolve(&(h(8), 3), &[(b"A".to_vec(), 1)], "carol")
                .is_empty()
        );
        assert!(p.to_file().spenders.is_empty());
    }

    /// A batched fill missing two sources stays pending after one arrives.
    #[test]
    fn a_partly_resolved_spender_stays_pending() {
        let mut p = Pending::default();
        let spender = tx(1, vec![("A", "bob", 2)]);
        p.want(&spender, missing("A", 2), &[(h(9), 0), (h(8), 0)]);
        p.resolve(&(h(9), 0), &[(b"A".to_vec(), 1)], "alice");
        assert_eq!(p.len(), 1);
        let file = p.to_file();
        assert_eq!(file.spenders[0].missing, vec![(b"A".to_vec(), 1)]);
        assert_eq!(file.spenders[0].outrefs, vec![([8; 32], 0)]);
    }

    /// A CARRIED spender that settles is named in the sidecar, so a later
    /// job merging an older file — jobs land out of order in the pool —
    /// drops it instead of carrying it forever. A spender found and
    /// settled in the same job is nobody's business.
    #[test]
    fn a_carried_spender_that_settles_is_reported_settled() {
        let mut first = Pending::default();
        first.want(&tx(1, vec![("A", "bob", 1)]), missing("A", 1), &[(h(9), 0)]);
        first.want(&tx(2, vec![("A", "cat", 1)]), missing("A", 1), &[(h(8), 0)]);
        let file = first.to_file();
        assert!(file.settled.is_empty(), "nothing was carried into this job");

        let mut next = Pending::load(file.spenders);
        next.resolve(&(h(9), 0), &[(b"A".to_vec(), 1)], "alice");
        next.want(&tx(3, vec![("A", "dan", 1)]), missing("A", 1), &[(h(7), 0)]);
        next.resolve(&(h(7), 0), &[(b"A".to_vec(), 1)], "erin");
        let file = next.to_file();
        assert_eq!(
            file.settled,
            vec![[1u8; 32]],
            "the carried one, settled here"
        );
        assert_eq!(file.spenders.len(), 1, "tx 2 is still waiting");
        assert_eq!(file.spenders[0].tx_hash, [2u8; 32]);
    }

    /// THE END-OF-JOB SETTLE, and the rule that keeps it from doubling
    /// rows: only spenders this job FOUND go to the index. A carried one
    /// may be waiting on a source that another job running right now walks
    /// past, and both writing that correction would double it under the
    /// sum rule.
    #[test]
    fn only_this_jobs_own_spenders_are_settled_through_the_index() {
        let mut p = Pending::default();
        // Carried from an earlier job's sidecar.
        p.spenders.push(Spender {
            hash: h(1),
            slot: 900,
            block_time: 1,
            missing: missing("A", 1),
            net_mint: Vec::new(),
            outrefs: vec![(h(9), 0)],
            retired: false,
            carried: true,
        });
        p.wants.insert((h(9), 0), vec![0]);
        // Found here.
        p.want(&tx(2, vec![("A", "bob", 1)]), missing("A", 1), &[(h(8), 0)]);

        let mut asked: Vec<OutRef> = Vec::new();
        let got = p.settle_with(|oref| {
            asked.push(*oref);
            Lookup::Held {
                units: vec![(b"A".to_vec(), 1)],
                address: "alice".into(),
            }
        });
        assert_eq!(asked, vec![(h(8), 0)], "the carried spender is not offered");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].spender, h(2));
        assert_eq!((got[0].row.amount, got[0].row.slot), (-1, 1_002));
        assert_eq!(p.len(), 1, "the carried one is still waiting");
    }

    /// An input the index knows and that carried none of our units is not
    /// the source of anything: FORGET it, or the spender hauls a dead ADA
    /// input through every sidecar from here down.
    #[test]
    fn an_input_that_carried_none_of_our_units_is_forgotten() {
        let mut p = Pending::default();
        p.want(
            &tx(1, vec![("A", "bob", 1)]),
            missing("A", 1),
            &[(h(9), 0), (h(8), 0)],
        );
        let got = p.settle_with(|oref| match *oref == (h(9), 0) {
            true => Lookup::NotOurs,
            false => Lookup::Held {
                units: vec![(b"A".to_vec(), 1)],
                address: "alice".into(),
            },
        });
        assert_eq!(got.len(), 1, "only the real source produced a row");
        assert!(p.is_empty());
        assert!(p.to_file().spenders.is_empty(), "nothing carried onward");
    }

    /// What the index cannot answer stays pending, exactly as with no index
    /// at all — the descent, or a lower job, will meet it.
    #[test]
    fn an_input_the_index_cannot_answer_stays_pending() {
        let mut p = Pending::default();
        p.want(&tx(1, vec![("A", "bob", 1)]), missing("A", 1), &[(h(9), 0)]);
        assert!(p.settle_with(|_| Lookup::Unknown).is_empty());
        assert_eq!(p.len(), 1);
        assert_eq!(p.to_file().spenders[0].outrefs, vec![([9; 32], 0)]);
    }

    /// The carried state round-trips: what the next pass loads resolves
    /// exactly what this one was waiting for.
    #[test]
    fn pending_survives_a_pass_boundary() {
        let mut p = Pending::default();
        let spender = tx(1, vec![("A", "bob", 1)]);
        p.want(&spender, missing("A", 1), &[(h(9), 0)]);
        let mut next = Pending::load(p.to_file().spenders);
        assert_eq!(next.len(), 1);
        let got = next.resolve(&(h(9), 0), &[(b"A".to_vec(), 1)], "alice");
        assert_eq!(got[0].row.slot, 1_001);
    }

    /// A transaction's rows as found: outputs first, a placeholder for an
    /// unattributed burn.
    #[test]
    fn a_transactions_rows_match_the_archives_shape() {
        let mut t = tx(1, vec![("A", "alice", 1)]);
        t.net_mint = vec![(b"A".to_vec(), 0), (b"C".to_vec(), -1)];
        let rows = t.rows();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            (rows[0].unit_name.as_slice(), rows[0].amount),
            (b"A".as_slice(), 1)
        );
        assert!(rows[1].is_placeholder());
        assert_eq!(rows[1].net_mint, -1);
        let folded = crate::archive::fold_rows(rows);
        assert_eq!(folded[0].units.len(), 2);
    }

    fn stamp(policy_hex: &str) -> Stamp {
        Stamp {
            policy_hex: policy_hex.to_string(),
            completeness: Completeness::Unrecorded,
            walk_from: None,
            walk_to: None,
            covered_from: 0,
            covered_to: 0,
            sealed_unix: 0,
        }
    }

    /// THE ARCHIVE END TO END, without a chain: two passes, spilled as
    /// segments and compacted on landing, the second resolving a source for
    /// a row the first published, read back through the same reader the API
    /// uses.
    #[test]
    fn two_passes_land_an_archive_the_reader_folds_correctly() {
        let tmp = tempfile::tempdir().unwrap();
        let policy_hex = "ab".repeat(28);
        let dir = tmp.path().join(&policy_hex);
        let mut manifest = Manifest::new(&policy_hex);

        // PASS 1: newest window. tx 5 received unit A from a source below the
        // floor (missing 1), tx 6 minted unit B.
        let mut w =
            SegmentWriter::new(&dir.join(PassEntry::dir_name(0)), stamp(&policy_hex)).unwrap();
        let five = tx(5, vec![("A", "bob", 1)]);
        let mut six = tx(6, vec![("B", "carol", 1)]);
        six.net_mint = vec![(b"B".to_vec(), 1)];
        w.push_own(five.rows());
        w.push_own(six.rows());
        let mut pending = Pending::default();
        pending.want(&five, missing("A", 1), &[(h(9), 0), (h(8), 0)]);
        let out = land(Landing {
            dir: &dir,
            seq: manifest.next_seq(),
            lock: None,
            manifest: &mut manifest,
            policy_hex: &policy_hex,
            first_mint: Some(500),
            ceiling: 2_000,
            walked: Walked {
                floor: 1_004,
                written: 2,
                backfilled: 0,
                by_index: 0,
                profile: policy_archive::Profile::default(),
            },
            segments: w.finish().unwrap(),
            pending: &pending,
            compact: true,
            secs: 0.0,
            observations: Vec::new(),
        })
        .unwrap();
        assert_eq!((out.unresolved, out.written), (1, 2));

        let mut a = PolicyArchive::open(&dir).unwrap().expect("manifest");
        assert_eq!(a.manifest.completeness, "partial");
        let cov = a.coverage();
        assert_eq!((cov.walked_from, cov.walked_to), (Some(1_004), Some(2_000)));
        assert_eq!(cov.total_txs, 2);
        let page = a.feed_rows(10, None).unwrap();
        let f = page.iter().find(|r| r.tx_hash == h(5).as_ref()).unwrap();
        assert_eq!(f.units[0].parties.len(), 1, "only the arrival is known");

        // PASS 2: deeper, and left UNCOMPACTED. The source of tx 5's input
        // comes into view — held by alice — a correction to a row pass 1
        // published; and the walk reaches the registered first mint.
        let mut pending = Pending::load(
            archive::load_pending(&dir.join(PassEntry::dir_name(0)).join(archive::PENDING))
                .unwrap()
                .spenders,
        );
        let mut w =
            SegmentWriter::new(&dir.join(PassEntry::dir_name(1)), stamp(&policy_hex)).unwrap();
        for r in pending.resolve(&(h(9), 0), &[(b"A".to_vec(), 1)], "alice") {
            w.push_corr(r.row);
        }
        w.push_own(tx(3, vec![("A", "alice", 1)]).rows());
        let out = land(Landing {
            dir: &dir,
            seq: manifest.next_seq(),
            lock: None,
            manifest: &mut manifest,
            policy_hex: &policy_hex,
            first_mint: Some(500),
            ceiling: 1_004,
            walked: Walked {
                floor: 500,
                written: 1,
                backfilled: 1,
                by_index: 0,
                profile: policy_archive::Profile::default(),
            },
            segments: w.finish().unwrap(),
            pending: &pending,
            compact: false,
            secs: 0.0,
            observations: Vec::new(),
        })
        .unwrap();
        assert_eq!(out.unresolved, 0, "settled, and its ADA input forgotten");

        let mut a = PolicyArchive::open(&dir).unwrap().expect("manifest");
        assert_eq!(a.manifest.completeness, "complete", "reached the mint");
        assert_eq!(
            a.manifest.passes[1].segments.len(),
            2,
            "seg + corr, uncompacted"
        );
        let cov = a.coverage();
        assert_eq!((cov.walked_from, cov.walked_to), (Some(500), Some(2_000)));
        assert_eq!(cov.total_txs, 3);

        // The feed folds the correction in: tx 5 is now alice → bob.
        let page = a.feed_rows(10, None).unwrap();
        assert_eq!(page.len(), 3);
        assert_eq!(page[0].tx_hash, h(6).as_ref(), "newest first");
        let five = page.iter().find(|r| r.tx_hash == h(5).as_ref()).unwrap();
        let mut parties: Vec<(String, i64)> = five.units[0]
            .parties
            .iter()
            .map(|p| (p.address.clone(), p.amount))
            .collect();
        parties.sort();
        assert_eq!(parties, vec![("alice".into(), -1), ("bob".into(), 1)]);

        // Point lookup and paging agree.
        let at = a.feed_row_at(h(5).as_ref()).unwrap().expect("found");
        assert_eq!(at.units[0].parties.len(), 2);
        assert!(a.feed_row_at(h(77).as_ref()).unwrap().is_none());
        let older = a.feed_rows(10, Some(1_005)).unwrap();
        assert_eq!(older.len(), 1);
        assert_eq!(older[0].tx_hash, h(3).as_ref());

        // Density counts the correction as a movement, never a transaction.
        let density = a.density(86_400);
        assert_eq!(density.iter().map(|b| b.txs).sum::<u64>(), 3);
        assert_eq!(density.iter().map(|b| b.movements).sum::<u64>(), 4);
    }

    #[test]
    fn chunks_come_back_highest_first_and_only_if_present() {
        let on_disk = [10u64, 11, 12, 14, 15];
        let got = chunks_descending(&on_disk, 11 * CHUNK_SLOTS, 15 * CHUNK_SLOTS);
        assert_eq!(got, vec![15, 14, 12, 11]);
    }

    /// A preview is ten days WITH the mint as its floor; the nearer bound
    /// wins, and a policy minted last week previews to completion.
    #[test]
    fn a_pass_stops_at_the_nearer_of_its_bounds() {
        let ceiling = 1_000 * SLOTS_PER_DAY;
        let mint = 500 * SLOTS_PER_DAY;
        assert_eq!(
            pass_floor(ceiling, Some(mint), Some(10), Some(mint)),
            990 * SLOTS_PER_DAY,
            "ten days, not five hundred"
        );
        assert_eq!(pass_floor(ceiling, Some(mint), None, Some(mint)), mint);
        assert_eq!(
            pass_floor(
                ceiling,
                Some(997 * SLOTS_PER_DAY),
                Some(10),
                Some(997 * SLOTS_PER_DAY)
            ),
            997 * SLOTS_PER_DAY,
            "a policy minted three days ago previews to its mint"
        );
        assert_eq!(pass_floor(ceiling, None, None, None), 0);
        assert_eq!(
            pass_floor(ceiling, None, Some(10), Some(995 * SLOTS_PER_DAY)),
            995 * SLOTS_PER_DAY
        );
    }

    #[test]
    fn the_chunk_holding_the_floor_is_included() {
        let on_disk = [10u64, 11, 12];
        let got = chunks_descending(&on_disk, 11 * CHUNK_SLOTS + 5, 12 * CHUNK_SLOTS);
        assert!(got.contains(&11), "{got:?}");
    }

    fn prog(floor: u64, target: u64, ceiling: u64) -> Progress<'static> {
        Progress {
            floor,
            target_floor: target,
            ceiling,
            chunks_done: 0,
            chunks_total: 0,
            written: 0,
            updated: &[],
            pending: 0,
            rows: &[],
            flushed: &[],
        }
    }

    #[test]
    fn progress_measures_the_floor_falling_toward_the_target() {
        assert!((prog(75, 50, 100).fraction() - 0.5).abs() < 1e-9);
        assert!((prog(100, 50, 100).fraction()).abs() < 1e-9);
        assert!((prog(50, 50, 100).fraction() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn an_empty_range_reads_as_complete_rather_than_dividing_by_zero() {
        assert!((prog(10, 10, 10).fraction() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn overshooting_the_target_clamps_at_one() {
        assert!((prog(10, 50, 100).fraction() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn a_slot_renders_as_its_calendar_date() {
        // Shelley start, 2020-07-29.
        assert_eq!(slot_date(4_492_800), "2020-07-29");
        assert_eq!(unix_date(0), "1970-01-01");
    }
}
