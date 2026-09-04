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
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use mitos_chain_walk::decode::{OutRef, decode_tx};
use mitos_chain_walk::mithril::CHUNK_SLOTS;
use mitos_chain_walk::{open_blocks, slot_to_unix};
use pallas_primitives::Hash;
use pallas_traverse::MultiEraBlock;
use policy_archive::{Completeness, Movement, Stamp};

use crate::archive::{self, FileEntry, Manifest, PassEntry, PendingFile, PendingSpender};
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

    /// Absolute floor to stop at, overriding `--days`. The hosted surface
    /// passes the policy's first mint here.
    #[arg(long)]
    pub to_slot: Option<u64>,

    /// Leave the pass as its segments rather than compacting them into one
    /// file at the end. For measuring, and for a box that would rather hand
    /// compaction to something else.
    #[arg(long)]
    pub no_compact: bool,

    /// Skip the memmem gate and decode every block. Only useful for isolating
    /// a suspected gate bug, since the gate is complete by the balance rule.
    #[arg(long)]
    pub no_sieve: bool,

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
    /// Spenders still waiting on a source. The honest gap.
    pub unresolved: u64,
    /// Bytes of `pending.bin` — the carried state, measured.
    pub pending_bytes: u64,
}

/// Run one reverse pass. The CLI face: logs progress and prints a summary.
pub fn run(args: ReverseArgs) -> Result<()> {
    let report_every = args.report_every.max(1);
    let out = run_reporting(args, &|p| {
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
    })?;

    println!("transactions written        = {}", out.written);
    println!("delta rows backfilled       = {}", out.backfilled);
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

/// One reverse pass, reporting through `on` — the programmatic face.
pub fn run_reporting(args: ReverseArgs, on: OnProgress<'_>) -> Result<Outcome> {
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

    // A pass runs from the existing floor DOWNWARD, so coverage stays
    // contiguous. On a cold policy there is no floor yet and the tip is the
    // ceiling — the first pass establishes it.
    let ceiling = manifest.walk_from.unwrap_or(tip_slot);
    let floor = match (args.to_slot, args.days) {
        (Some(s), _) => s,
        (None, Some(days)) => ceiling.saturating_sub(days * SLOTS_PER_DAY),
        // Everything. Clamped to the first mint below when one is known.
        (None, None) => 0,
    };
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
        .or(args.to_slot);
    let floor = first_mint.map_or(floor, |first| floor.max(first));

    // The carried state: what the previous pass was still waiting for.
    let prior_pending = match manifest.latest_pass() {
        Some(p) => archive::load_pending(&dir.join(&p.dir).join(archive::PENDING))?,
        None => PendingFile::default(),
    };
    let mut pending = Pending::load(prior_pending.spenders);
    let carried = pending.len();

    if floor >= ceiling {
        tracing::info!(
            ceiling,
            floor,
            "reverse: nothing to do — coverage already reaches the requested floor"
        );
        return Ok(Outcome {
            floor: ceiling,
            written: 0,
            backfilled: 0,
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
    // waste disk.
    let seq = manifest.next_seq();
    let pass_dir = dir.join(PassEntry::dir_name(seq));
    if pass_dir.exists() {
        std::fs::remove_dir_all(&pass_dir)?;
    }
    let mut writer = SegmentWriter::new(
        &pass_dir,
        Stamp {
            policy_hex: policy_hex.clone(),
            completeness: manifest.completeness(),
            walk_from: manifest.walk_from,
            walk_to: manifest.walk_to,
            covered_from: 0,
            covered_to: 0,
            sealed_unix: now_unix(),
        },
    )?;

    let walked = pass(
        &immutable,
        &all_chunks,
        &policy,
        &watched,
        floor,
        ceiling,
        &mut writer,
        &mut pending,
        !args.no_sieve,
        on,
    )?;
    let segments = writer.finish()?;

    let out = land(Landing {
        dir: &dir,
        manifest: &mut manifest,
        policy_hex: &policy_hex,
        first_mint,
        ceiling,
        walked,
        segments,
        pending: &pending,
        compact: !args.no_compact,
        secs: started.elapsed().as_secs_f64(),
    })?;

    tracing::info!(
        floor = out.floor,
        written = out.written,
        carried,
        backfilled = out.backfilled,
        unresolved = out.unresolved,
        pending_bytes = out.pending_bytes,
        secs = format!("{:.1}", started.elapsed().as_secs_f64()),
        "reverse: pass complete"
    );
    Ok(out)
}

/// Everything a finished walk hands to the landing.
struct Landing<'a> {
    dir: &'a Path,
    manifest: &'a mut Manifest,
    policy_hex: &'a str,
    first_mint: Option<u64>,
    ceiling: u64,
    walked: Walked,
    segments: Vec<FileEntry>,
    pending: &'a Pending,
    compact: bool,
    secs: f64,
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
        manifest,
        policy_hex,
        first_mint,
        ceiling,
        walked,
        segments,
        pending,
        compact,
        secs,
    } = l;
    let seq = manifest.next_seq();
    let pass_dir = dir.join(PassEntry::dir_name(seq));
    std::fs::create_dir_all(&pass_dir)?;
    let new_walk_from = Some(
        manifest
            .walk_from
            .map_or(walked.floor, |f| f.min(walked.floor)),
    );
    let new_walk_to = Some(manifest.walk_to.map_or(ceiling, |t| t.max(ceiling)));

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

    manifest.first_mint_slot = first_mint;
    manifest.completeness = completeness.as_wire().to_string();
    manifest.walk_from = new_walk_from;
    manifest.walk_to = new_walk_to;
    manifest.updated_unix = sealed_unix;
    manifest.passes.push(PassEntry {
        seq,
        dir: PassEntry::dir_name(seq),
        ceiling,
        floor: walked.floor,
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

    Ok(Outcome {
        floor: walked.floor,
        written,
        backfilled: walked.backfilled as u64,
        unresolved: pending_file.spenders.len() as u64,
        pending_bytes,
    })
}

fn now_unix() -> u64 {
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

/// One transaction as the walk first meets it.
struct PassTx {
    hash: Hash<32>,
    slot: u64,
    block_time: u64,
    /// Per unit: 0 / +mint / −burn.
    net_mint: Vec<(Vec<u8>, i64)>,
    /// `(unit, address, amount)` — its outputs.
    deltas: Vec<(Vec<u8>, String, i64)>,
}

impl PassTx {
    /// This transaction's rows as first found — its outputs, plus a
    /// placeholder for a minted-or-burned unit that reached nobody.
    /// Resolutions arrive later as further signed rows and fold on top.
    fn rows(&self) -> Vec<Movement> {
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

    /// The carried state, as the next pass will load it.
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

/// What the chunk walk itself produced, before anything lands.
struct Walked {
    floor: u64,
    written: u64,
    backfilled: usize,
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
        let path = immutable.join(format!("{chunk:05}.chunk"));
        let bytes = std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
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
    writer: &mut SegmentWriter,
    pending: &mut Pending,
    sieve: bool,
    on: OnProgress<'_>,
) -> Result<Walked> {
    let needles = sieve
        .then(|| chain_sieve::Needles::new(std::slice::from_ref(&policy.to_vec())))
        .transpose()?;

    let mut written = 0u64;
    let mut backfilled = 0usize;
    let mut lowest = ceiling;
    let ordered = chunks_descending(chunks, floor, ceiling);
    let chunks_total = ordered.len() as u64;
    let mut chunks_done = 0u64;

    for chunk in ordered {
        let blocks = chunk_blocks_newest_first(immutable, chunk, needles.as_ref())?;
        // Resolutions found in this chunk. A source and its spender can sit
        // in the SAME chunk; as rows they simply sum, so order is free.
        let mut resolved: Vec<Resolution> = Vec::new();
        // What this chunk adds, as archive rows, for the live view.
        let mut chunk_rows: Vec<Movement> = Vec::new();

        for (slot, raw) in blocks {
            if slot < floor || slot >= ceiling {
                continue;
            }
            lowest = lowest.min(slot);
            let blk = MultiEraBlock::decode(&raw)
                .map_err(|e| anyhow::anyhow!("decoding block at slot {slot}: {e:?}"))?;
            let block_time = slot_to_unix(slot);

            let txs: Vec<_> = blk.txs();
            for tx in txs.iter().rev() {
                let dtx = decode_tx(tx);

                let mut net_mint: HashMap<Vec<u8>, i64> = HashMap::new();
                for pa in tx.mints().iter() {
                    if pa.policy().as_ref() != policy {
                        continue;
                    }
                    for a in pa.assets().iter() {
                        if !watched.matches(a.name()) {
                            continue;
                        }
                        *net_mint.entry(a.name().to_vec()).or_insert(0) +=
                            a.mint_coin().unwrap_or(0);
                    }
                }

                // OUTPUTS: this transaction's own positive deltas, AND the
                // resolution of whatever spent them later.
                let mut deltas: Vec<(Vec<u8>, String, i64)> = Vec::new();
                for out in &dtx.outputs {
                    let units = units_in_output(tx, out, policy, watched);
                    if units.is_empty() {
                        continue;
                    }
                    for (name, qty) in &units {
                        deltas.push((name.clone(), out.address.clone(), *qty));
                    }
                    resolved.extend(pending.resolve(
                        &(dtx.tx_hash, out.index),
                        &units,
                        &out.address,
                    ));
                }

                let touches_us = !deltas.is_empty() || !net_mint.is_empty();
                if !touches_us {
                    continue;
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
                    let inputs: Vec<OutRef> = dtx.inputs.iter().map(|i| i.oref).collect();
                    pending.want(&row, missing, &inputs);
                }
                let rows = row.rows();
                chunk_rows.extend(rows.iter().cloned());
                writer.push_own(rows);
                written += 1;
            }
        }

        // This chunk's resolutions, as correction rows — whichever pass the
        // spender came from. Compaction folds this pass's own onto their
        // transactions; the reader sums the rest.
        let mut updated: Vec<Hash<32>> = Vec::new();
        for r in resolved {
            backfilled += 1;
            if !updated.contains(&r.spender) {
                updated.push(r.spender);
            }
            chunk_rows.push(r.row.clone());
            writer.push_corr(r.row);
        }

        chunks_done += 1;
        let flushed = writer.end_chunk()?;
        on(Progress {
            floor: lowest,
            target_floor: floor,
            ceiling,
            chunks_done,
            chunks_total,
            written,
            updated: &updated,
            pending: pending.len(),
            rows: &chunk_rows,
            flushed: &flushed,
        });
    }

    // Coverage is where the pass STOPPED LOOKING, not the deepest row it
    // found: a quiet stretch below the last hit was read and held nothing,
    // which is a fact worth keeping. Only a range with no chunks on disk at
    // all extends nothing.
    Ok(Walked {
        floor: if chunks_total == 0 { ceiling } else { floor },
        written,
        backfilled,
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
            manifest: &mut manifest,
            policy_hex: &policy_hex,
            first_mint: Some(500),
            ceiling: 2_000,
            walked: Walked {
                floor: 1_004,
                written: 2,
                backfilled: 0,
            },
            segments: w.finish().unwrap(),
            pending: &pending,
            compact: true,
            secs: 0.0,
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
            manifest: &mut manifest,
            policy_hex: &policy_hex,
            first_mint: Some(500),
            ceiling: 1_004,
            walked: Walked {
                floor: 500,
                written: 1,
                backfilled: 1,
            },
            segments: w.finish().unwrap(),
            pending: &pending,
            compact: false,
            secs: 0.0,
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
