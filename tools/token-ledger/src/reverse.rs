//! `reverse` — walk history BACKWARD from the tip, emitting newest first.
//!
//! The forward walk in [`crate::walk`] is the complete one: it starts at or
//! below the policy's first mint, so its outref buffer is complete by
//! construction and every projection over it — float, holder count,
//! concentration, market cap — is exact. It is also the wrong shape for a feed.
//! A feed wants the newest row FIRST, and a forward walk produces the newest row
//! LAST, so nothing can be shown until the whole window is done.
//!
//! This mode inverts that: chunk-descending, newest first, rows available in
//! seconds and improving as it goes.
//!
//! # The buffer inverts too
//!
//! Forward, the buffer holds outputs awaiting their spender: a tx spends outref
//! X, `buffer.take(X)` says who held it, and the negative delta is known
//! immediately.
//!
//! Backward, that is impossible — the output that created X sits at a LOWER
//! slot, which is ground this walk has not covered yet. So the state inverts:
//! [`Pending`] holds outrefs already spent by transactions we have already
//! written, waiting for their creating output to come into view. Every
//! transaction is therefore written in two stages:
//!
//! - its **positive** deltas immediately, from its own outputs
//! - its **negative** deltas later, on reaching the transactions further back
//!   that created what it spent — via [`crate::store::Ledger::add_deltas`]
//!
//! That is the same progressive-enrichment shape the wallet view already speaks
//! over its socket (`FlowDelta::RowsUpdated`): a row lands readable and is
//! corrected in place rather than withheld until perfect.
//!
//! # What bounds it
//!
//! `Pending` grows with unresolved inputs and shrinks as they resolve, so it is
//! bounded by the transactions still missing a source rather than by the
//! policy's supply. That is the property that makes this safe on a collection a
//! completeness-first walker cannot touch at all: cost follows ACTIVITY in the
//! window, never the size of the holder set.
//!
//! # The sieve gate stays complete
//!
//! A watched input's creating output necessarily carried the policy id, so its
//! block — and its chunk — is a sieve hit. Skipping non-hit chunks therefore
//! cannot lose a resolution. Non-watched inputs (the ADA that funds a fee) may
//! never be visited at all, which costs nothing: they were never owed a delta.
//! Conservation is what distinguishes the two, and it needs no help from here —
//! see [`crate::walk::BreachKind`].

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use mitos_chain_walk::decode::decode_tx;
use mitos_chain_walk::mithril::CHUNK_SLOTS;
use mitos_chain_walk::{open_blocks, slot_to_unix};
use pallas_primitives::Hash;
use pallas_traverse::MultiEraBlock;

use crate::registry;
use crate::store::{DeltaRow, Ledger, TxRow};
use crate::walk::{Watched, stake_of, units_in_output};

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

    /// Ledger sqlite path (default: `<token>.db`).
    #[arg(long)]
    pub db: Option<PathBuf>,

    /// How much further back to reach, in days, measured from the ledger's
    /// current floor (or from the tip on a cold ledger).
    ///
    /// This is the WINDOW, and it is the whole cost control: a pass reads the
    /// chain in that window and nothing else, whatever the policy's supply. Run
    /// it again to go deeper — each pass extends coverage downward and resolves
    /// sources for rows already written.
    #[arg(long, default_value_t = 30)]
    pub days: u64,

    /// Absolute floor to stop at, overriding `--days`.
    #[arg(long)]
    pub to_slot: Option<u64>,

    /// Start the pass HERE instead of at the ledger's current floor — a probe
    /// into an arbitrary moment in history.
    ///
    /// Seeking is cheap: the immutable DB is binary-searched to the chunk by a
    /// slot-only fuzzy seek, and the chunk gate then reads only the files in
    /// range. Reaching a window four years back costs what that window costs,
    /// not what the intervening history costs.
    ///
    /// **A probe does NOT advance the ledger's recorded coverage.** Coverage is
    /// a single contiguous range `[walked_from, tip]`, and a window detached
    /// from it cannot be described by one floor — lowering it would claim
    /// everything in between had been walked. Rows and resolutions are still
    /// written and are still true; only the coverage claim is withheld.
    #[arg(long)]
    pub from_slot: Option<u64>,

    /// Skip the memmem gate and decode every block. Slower by roughly the ratio
    /// of hit blocks to all blocks; only useful for isolating a suspected gate
    /// bug, since the gate is complete by the ledger's balance rule.
    #[arg(long)]
    pub no_sieve: bool,

    /// Log a progress line every N chunks. A chunk is ~6h of chain, so a
    /// full-history pass covers thousands.
    ///
    /// Chunks that CORRECTED rows are always logged regardless — a backfill is
    /// the event a consumer most needs and the rarest to happen.
    #[arg(long, default_value_t = 250)]
    pub report_every: u64,
}

/// Run one reverse pass, extending the ledger's coverage downward.
///
/// The CLI face: logs progress on `--report-every` and prints a summary.
pub fn run(args: ReverseArgs) -> Result<()> {
    let report_every = args.report_every.max(1);
    let (written, backfilled, unresolved) = run_reporting(args, &|p| {
        // Chunks are ~6h of chain, so a full-history pass emits thousands. Log
        // every Nth, but ALWAYS log one that corrected rows: a backfill is the
        // event a consumer most needs to see and the rarest to occur.
        if p.chunks_done.is_multiple_of(report_every) || !p.updated.is_empty() {
            tracing::info!(
                pct = format!("{:.1}%", p.fraction() * 100.0),
                floor = p.floor,
                date = %slot_date(p.floor),
                chunks = format!("{}/{}", p.chunks_done, p.chunks_total),
                written = p.written,
                updated = p.updated.len(),
                pending = p.pending,
                "reverse: progress"
            );
        }
    })?;

    println!("transactions written        = {written}");
    println!("delta rows backfilled       = {backfilled}");
    println!("sources still below floor   = {unresolved}");
    if unresolved > 0 {
        println!(
            "  run again to reach deeper — each pass resolves sources for rows \
             already written"
        );
    }
    Ok(())
}

/// One reverse pass, reporting through `on` — the programmatic face.
///
/// Returns `(written, backfilled, unresolved)`. Separated from [`run`] so the
/// hosted surface can drive a pass and turn its progress into job state without
/// going through stdout, which is the only thing the CLI face adds.
pub fn run_reporting(args: ReverseArgs, on: OnProgress<'_>) -> Result<(u64, usize, usize)> {
    let token = registry::load_or_unit(&args.tokens, &args.token)?;
    let policy = token.policy_bytes()?;
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

    let db_path = args
        .db
        .clone()
        .unwrap_or_else(|| PathBuf::from(format!("{}.db", token.name)));
    let mut ledger = Ledger::open(&db_path)?;
    ledger.put_meta(
        &token.name,
        &policy,
        token.asset_name_bytes()?.as_deref(),
        token.resolved_decimals(),
    )?;

    let all_chunks = chain_sieve::list_chunks(&immutable, 0)?;
    let tip_slot = all_chunks
        .last()
        .map(|c| (c + 1) * CHUNK_SLOTS)
        .unwrap_or(0);

    // A pass runs from the existing floor DOWNWARD, so coverage stays
    // contiguous. On a cold ledger there is no floor yet and the tip is the
    // ceiling — the first pass is the one that establishes it.
    let coverage_floor = ledger.walked_from()?;
    let ceiling = args
        .from_slot
        .unwrap_or_else(|| coverage_floor.unwrap_or(tip_slot));
    // A pass extends coverage only when it starts exactly where coverage
    // currently ends. Anywhere else is a detached window, and a single
    // `walked_from` cannot describe two ranges.
    //
    // A COLD ledger is not a free pass. Coverage means `[walked_from, tip]`, so
    // a probe that starts below the tip is detached whether or not anything
    // came before it — the first version read "no existing floor" as "anything
    // is contiguous" and let a 2022 probe claim coverage it plainly did not
    // have.
    let contiguous = match args.from_slot {
        None => true,
        Some(from) => coverage_floor == Some(from),
    };
    let floor = match args.to_slot {
        Some(s) => s,
        None => ceiling.saturating_sub(args.days * SLOTS_PER_DAY),
    };
    // Never dig below the policy's own first mint when we know it: there is
    // nothing there, and reading it is pure cost.
    let floor = token.floor_slot.map_or(floor, |first| floor.max(first));

    if floor >= ceiling {
        tracing::info!(
            ceiling,
            floor,
            "reverse: nothing to do — coverage already reaches the requested floor"
        );
        return Ok((0, 0, pending_count(&ledger)));
    }

    tracing::info!(
        token = %token.name,
        policy = %token.policy,
        policy_wide = watched_is_policy(&watched),
        floor,
        ceiling,
        days = args.days,
        "reverse: pass starting"
    );

    // Carried across runs: a pass that started empty could never resolve what
    // an earlier one was waiting for, which made deepening useless.
    let mut pending = Pending::load(ledger.load_pending()?);
    let carried = pending.len();
    // Published BEFORE the pass, so a poller that arrives mid-run has a
    // denominator. Written after the fact it would be useless for exactly the
    // window it exists to describe.
    ledger.set_walk_target(floor)?;
    let outcome = pass(
        &mut ledger,
        &immutable,
        &all_chunks,
        &policy,
        &watched,
        floor,
        ceiling,
        &mut pending,
        !args.no_sieve,
        contiguous,
        on,
    )?;

    // The top of what this ledger covers. A reverse pass starts at the ceiling
    // and works down, so the ceiling IS the upper bound — and nothing else
    // records it, since reverse never moves the forward cursor.
    if contiguous {
        ledger.set_walked_to(ceiling)?;
    }
    ledger.put_pending(&pending.entries())?;
    // Target collapses onto the achieved floor: a finished pass is idle, not a
    // pass sitting at 100% forever. A poller reads the two being equal as
    // "nothing running".
    ledger.set_walk_target(outcome.floor)?;

    // Has this ledger reached the policy's beginning?
    //
    // Only two things establish it: reaching genesis, or reaching a registered
    // first-mint floor. A PROBE never does — it writes true rows into a
    // detached window and claims no coverage, so it must not claim completeness
    // either.
    //
    // A pass that stops short records PARTIAL, because it knows that for a
    // fact: it just walked and did not get there. Leaving it unrecorded threw
    // that away and made a ledger we had measured report "coverage unknown".
    //
    // The one thing never done is DEMOTING a `Complete` ledger: a later shallow
    // pass over a fully-walked history has learned nothing that unmakes the
    // earlier walk, and treating it as evidence would let routine refreshes
    // erase a hard-won verdict.
    use crate::store::Completeness;
    let reached_beginning = contiguous
        && (outcome.floor == 0 || token.floor_slot.is_some_and(|first| outcome.floor <= first));
    let already = ledger.completeness()?;
    let next = match (reached_beginning, already) {
        (true, _) => Some(Completeness::Complete),
        (false, Completeness::Complete) => None,
        (false, _) if contiguous => Some(Completeness::Partial),
        // A probe establishes nothing about coverage either way.
        (false, _) => None,
    };
    if let Some(next) = next {
        ledger.set_completeness(next)?;
    }

    tracing::info!(
        floor = outcome.floor,
        written = outcome.written,
        carried,
        backfilled = outcome.backfilled,
        unresolved = outcome.unresolved,
        "reverse: pass complete"
    );
    Ok((outcome.written, outcome.backfilled, outcome.unresolved))
}

fn watched_is_policy(w: &Watched) -> bool {
    matches!(w, Watched::Policy)
}

/// Pending count for an early return, where no pass ran to report one.
fn pending_count(ledger: &Ledger) -> usize {
    ledger.load_pending().map(|p| p.len()).unwrap_or(0)
}

/// A slot as `YYYY-MM-DD`, for saying how far back a pass has reached.
///
/// The slot number is the machine's answer and means nothing to a reader
/// watching a progress bar: "reaching back to 2021-03-14" is the statement, and
/// a UI should not have to carry a slot-to-date conversion to make it.
///
/// Civil-from-days (Hinnant), so this needs no date dependency.
pub fn slot_date(slot: u64) -> String {
    let days = (slot_to_unix(slot) / 86_400) as i64;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

/// An outref spent by a transaction we have already written, whose creating
/// output we have not reached yet.
///
/// The reverse counterpart of a buffered output. Keyed by outref because that
/// is what a later (earlier-in-slot) transaction will present when it creates
/// the thing — the same join the forward buffer makes, in the other direction.
#[derive(Default)]
pub struct Pending {
    /// outref → the transactions that spent it.
    ///
    /// A `Vec` because a single outref can only be spent once on chain, but a
    /// resumed run can legitimately re-record the same waiter, and collapsing
    /// that to a single slot would drop the second half of a re-walk.
    waiting: HashMap<(Hash<32>, u32), Vec<Hash<32>>>,
    /// Changes since the last drain — the unit of PERSISTENCE.
    ///
    /// Kept as a changelog rather than rewriting the whole set per chunk
    /// because the set runs to tens of thousands of entries while a chunk
    /// touches a handful, and the floor cursor advances every chunk. Those two
    /// have to move together: a pass killed with the floor advanced but its
    /// pending unsaved leaves sources below that floor permanently
    /// unresolvable, because nothing will ever look for them again.
    added: Vec<((Hash<32>, u32), Hash<32>)>,
    removed: Vec<(Hash<32>, u32)>,
}

impl Pending {
    /// Rebuild from what a previous pass left behind.
    pub fn load(entries: Vec<((Hash<32>, u32), Hash<32>)>) -> Self {
        let mut waiting: HashMap<(Hash<32>, u32), Vec<Hash<32>>> = HashMap::new();
        for (oref, spender) in entries {
            waiting.entry(oref).or_default().push(spender);
        }
        // Loaded entries are already ON DISK, so the changelog starts empty.
        // Seeding it with them would re-insert every row on the first chunk —
        // harmless under `INSERT OR IGNORE`, but it would turn a handful of
        // writes per chunk into tens of thousands.
        Self {
            waiting,
            ..Default::default()
        }
    }

    /// Flatten for persistence.
    pub fn entries(&self) -> Vec<((Hash<32>, u32), Hash<32>)> {
        self.waiting
            .iter()
            .flat_map(|(oref, spenders)| spenders.iter().map(move |s| (*oref, *s)))
            .collect()
    }

    fn want(&mut self, oref: (Hash<32>, u32), spender: Hash<32>) {
        let slot = self.waiting.entry(oref).or_default();
        // A resumed pass reloads what it already recorded, so the same waiter
        // can arrive twice. Duplicates would emit the same backfill twice —
        // harmless only because `add_deltas` ignores conflicts, which is a
        // guard to lean on, not a reason to create the condition.
        if !slot.contains(&spender) {
            slot.push(spender);
            self.added.push((oref, spender));
        }
    }

    /// Transactions waiting on this outref, removing it from the open set.
    fn resolve(&mut self, oref: &(Hash<32>, u32)) -> Option<Vec<Hash<32>>> {
        let found = self.waiting.remove(oref);
        if found.is_some() {
            self.removed.push(*oref);
        }
        found
    }

    /// Drain the changelog for persistence.
    fn take_changes(&mut self) -> (Vec<((Hash<32>, u32), Hash<32>)>, Vec<(Hash<32>, u32)>) {
        (
            std::mem::take(&mut self.added),
            std::mem::take(&mut self.removed),
        )
    }

    pub fn len(&self) -> usize {
        self.waiting.len()
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.waiting.is_empty()
    }
}

/// Where a pass has got to, emitted after every chunk it commits.
///
/// A reverse pass exists to feed a live surface, so it has to SAY where it is
/// while it runs. Without this a consumer sees rows appear in the ledger with
/// no way to tell whether more are coming, how far back the walk has reached,
/// or which already-delivered rows just changed underneath it — measured on the
/// SpaceBudz full-history run, which committed 49,591 transactions while
/// emitting a single log line.
///
/// Emitted per CHUNK rather than per row: a chunk is the commit unit, so it is
/// the only boundary at which the ledger is consistent for a reader, and it is
/// ~6h of chain — frequent enough for a progress bar, rare enough not to flood
/// a socket.
pub struct Progress<'a> {
    /// Lowest slot covered so far. Moves DOWN as the pass runs.
    pub floor: u64,
    /// Where this pass is heading — the requested floor.
    pub target_floor: u64,
    /// Where it started. `ceiling - floor` over `ceiling - target_floor` is the
    /// fraction done.
    pub ceiling: u64,
    pub chunks_done: u64,
    pub chunks_total: u64,
    /// Transactions written so far by this pass.
    pub written: u64,
    /// Transactions whose deltas CHANGED in this chunk — a source resolved.
    ///
    /// The payload a `RowsUpdated` needs. A count cannot serve it: the consumer
    /// has to know WHICH rows to re-read, and a reverse walk corrects rows it
    /// delivered minutes earlier.
    pub updated: &'a [Hash<32>],
    /// Outrefs still waiting on a source below the floor.
    pub pending: usize,
}

impl Progress<'_> {
    /// How far through the requested range, 0.0–1.0.
    ///
    /// Saturating rather than panicking on a zero-width range: a pass with
    /// nothing to do is a legitimate state and a progress bar should read it as
    /// finished, not divide by zero.
    pub fn fraction(&self) -> f64 {
        let span = self.ceiling.saturating_sub(self.target_floor);
        if span == 0 {
            return 1.0;
        }
        let done = self.ceiling.saturating_sub(self.floor);
        (done as f64 / span as f64).clamp(0.0, 1.0)
    }
}

/// The callback shape a pass reports through. Mirrors the forward walk's
/// `Prog<'_>` so both walkers report the same way.
pub type OnProgress<'x> = &'x dyn Fn(Progress<'_>);

/// How far a reverse pass got, and what it cost.
pub struct Outcome {
    /// Lowest slot now covered — the ledger's new floor.
    pub floor: u64,
    /// Transactions written by this pass.
    pub written: u64,
    /// Delta rows backfilled onto transactions written earlier, here or by a
    /// previous pass. The measure of the walk resolving its own history.
    pub backfilled: usize,
    /// Outrefs still waiting on a source below the floor. The honest gap, and
    /// what a deeper pass would close.
    pub unresolved: usize,
}

/// Chunk numbers to visit, highest first, covering `[floor, ceiling)`.
///
/// Descending is the whole point, but the chunk LIST still has to come from the
/// directory rather than from arithmetic: chunk files are dense in practice and
/// sparse in principle, and inventing numbers that are not on disk turns a gap
/// into a read error halfway through a pass.
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

/// One chunk's transactions, newest first.
///
/// The immutable DB only reads FORWARD — `open_blocks` seeks to a point and
/// streams upward — so "reverse" is chunk-descending with each chunk read
/// forward and flipped. A chunk is 21,600 slots (~6h), which is fine grain for a
/// feed and a trivial amount to hold: only blocks that pass the sieve gate are
/// decoded, and only their watched transactions are kept.
fn chunk_txs_newest_first(
    immutable: &Path,
    chunk: u64,
    needles: Option<&chain_sieve::Needles<'_>>,
) -> Result<Vec<(u64, Vec<u8>)>> {
    let start = chunk * CHUNK_SLOTS;
    let end = (chunk + 1) * CHUNK_SLOTS;

    // CHUNK-LEVEL GATE FIRST — the cheap half, and the one that decides whether
    // a deep pass is minutes or an hour.
    //
    // A per-BLOCK gate still pays the sequential reader for every block in the
    // file (~600 MB/s) even when nothing in it is ours. Reading the raw chunk
    // with one `fs::read` and one memmem runs at parallel-read speed and skips
    // the whole file on a miss — which is almost every file, since a policy's
    // activity is a thin slice of the chain. Same trick, same reason, as
    // `chain_sieve::scan_extract`.
    //
    // Sound for the same reason the block gate is: a transaction touching the
    // policy carries its id in an output value or the mint field, so a chunk
    // whose bytes lack it cannot hold one.
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
        // Cheap slot peek before any gate: decoding is the cost this whole
        // design exists to avoid, but we must decode to learn the slot. The
        // sieve check first is therefore strictly cheaper on a miss.
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

/// Walk `[floor, ceiling)` backward, writing rows newest-first and resolving
/// sources as they come into view.
///
/// `ceiling` is exclusive and is normally the ledger's existing floor, so a
/// pass extends coverage downward contiguously — which is what lets the caller
/// lower `walked_from` for every chunk that commits.
#[allow(clippy::too_many_arguments)]
pub fn pass(
    ledger: &mut Ledger,
    immutable: &Path,
    chunks: &[u64],
    policy: &[u8],
    watched: &Watched,
    floor: u64,
    ceiling: u64,
    pending: &mut Pending,
    sieve: bool,
    // Does this pass extend the ledger's contiguous coverage, or is it a
    // detached probe? See `ReverseArgs::from_slot`.
    contiguous: bool,
    on: OnProgress<'_>,
) -> Result<Outcome> {
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
        let blocks = chunk_txs_newest_first(immutable, chunk, needles.as_ref())?;
        let mut rows: Vec<TxRow> = Vec::new();
        // Backfills discovered in this chunk, applied after its rows are
        // written: a source and its spender can sit in the SAME chunk, and
        // applying the backfill first would find no parent transaction.
        let mut resolved: Vec<(Hash<32>, DeltaRow)> = Vec::new();

        for (slot, raw) in blocks {
            if slot < floor || slot >= ceiling {
                continue;
            }
            lowest = lowest.min(slot);
            let blk = MultiEraBlock::decode(&raw)
                .map_err(|e| anyhow::anyhow!("decoding block at slot {slot}: {e:?}"))?;
            let block_time = slot_to_unix(slot);

            // Transactions within a block are reversed too, so the emitted
            // order is a strict newest-first total order rather than one that
            // is newest-first between blocks and oldest-first inside them.
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

                // OUTPUTS: known now, and they are two things at once — this
                // transaction's own positive deltas, AND the resolution of
                // whatever spent them later.
                let mut deltas: Vec<DeltaRow> = Vec::new();
                for out in &dtx.outputs {
                    let units = units_in_output(tx, out, policy, watched);
                    if units.is_empty() {
                        continue;
                    }
                    let stake = stake_of(&out.address);
                    for (name, qty) in &units {
                        deltas.push(DeltaRow {
                            address: out.address.clone(),
                            stake: stake.clone(),
                            name: name.clone(),
                            amount: *qty,
                        });
                    }
                    // Anyone waiting on this outref now learns who held it and
                    // how much — their MISSING NEGATIVE.
                    if let Some(spenders) = pending.resolve(&(dtx.tx_hash, out.index)) {
                        for spender in spenders {
                            for (name, qty) in &units {
                                resolved.push((
                                    spender,
                                    DeltaRow {
                                        address: out.address.clone(),
                                        stake: stake.clone(),
                                        name: name.clone(),
                                        amount: -qty,
                                    },
                                ));
                            }
                        }
                    }
                }

                // Any transaction that MOVES a watched asset necessarily has it
                // in an output — or, for a burn, in the mint field. So this is
                // complete: a transaction with neither touches nothing of ours.
                let touches_us = !deltas.is_empty() || !net_mint.is_empty();
                if !touches_us {
                    continue;
                }

                // INPUTS: register interest ONLY where a source is actually
                // missing.
                //
                // Conservation says exactly that. A transaction whose deltas
                // already balance has every party accounted for, so none of its
                // inputs held a watched asset and waiting on them is waiting
                // forever. Registering them all instead put every ADA input of
                // every watched transaction into the pending set — unbounded
                // growth, on entries that can never resolve.
                //
                // `Unattributed` is the missing-negative case; see
                // `walk::BreachKind`.
                let as_map: HashMap<(String, Vec<u8>), (Option<String>, i64)> = deltas
                    .iter()
                    .map(|d| {
                        (
                            (d.address.clone(), d.name.clone()),
                            (d.stake.clone(), d.amount),
                        )
                    })
                    .collect();
                let missing_source = crate::walk::conservation_breaches(&as_map, &net_mint)
                    .iter()
                    .any(|b| b.kind == crate::walk::BreachKind::Unattributed);
                if missing_source {
                    for inp in &dtx.inputs {
                        pending.want(inp.oref, dtx.tx_hash);
                    }
                }

                rows.push(TxRow {
                    tx_hash: dtx.tx_hash,
                    slot,
                    block_time,
                    net_mint: net_mint.into_iter().collect(),
                    deltas,
                    // Pool reserves and lock lifetimes are COMPLETENESS-shaped
                    // projections: both are read off the live open set, which a
                    // reverse pass does not have. They stay the forward walk's
                    // job, and leaving them empty here is what stops a partial
                    // pass publishing a half-populated curve.
                    pools: Vec::new(),
                    locks_created: Vec::new(),
                    locks_spent: Vec::new(),
                });
            }
        }

        chunks_done += 1;
        written += rows.len() as u64;
        if !rows.is_empty() {
            // The buffer is not this mode's state, so nothing is persisted
            // through it; the cursor `commit_block` would write is the FORWARD
            // frontier, which a reverse pass must not move. `commit_rows`
            // exists for exactly that.
            ledger.commit_rows(&rows)?;
        }

        let mut by_tx: HashMap<Hash<32>, Vec<DeltaRow>> = HashMap::new();
        for (spender, delta) in resolved {
            by_tx.entry(spender).or_default().push(delta);
        }
        // WHICH transactions changed, not just how many. A consumer that was
        // handed these rows earlier has to re-read exactly these.
        let updated: Vec<Hash<32>> = by_tx.keys().copied().collect();
        let backfills: Vec<(Hash<32>, Vec<DeltaRow>)> = by_tx.into_iter().collect();

        // EVERY chunk, including one that held nothing of ours.
        //
        // Coverage extends downward one chunk at a time, so the floor moves
        // with committed work rather than at the end of the pass. A pass killed
        // halfway leaves a ledger that knows exactly how deep it got — and,
        // because the pending changes ride the SAME transaction, exactly what
        // it was still waiting for at that depth.
        //
        // An empty chunk still advances it: we READ that chunk and it held
        // nothing, which is a fact worth keeping. Skipping the write here made
        // the recorded floor stop at the deepest chunk that happened to contain
        // a transaction, so every later pass re-read the quiet stretch below it.
        let (added, removed) = pending.take_changes();
        backfilled += ledger.commit_chunk(
            &backfills,
            &added,
            &removed,
            contiguous.then(|| chunk * CHUNK_SLOTS),
        )?;

        // AFTER the commit, so a consumer that reacts by reading the ledger
        // finds the rows this event is telling it about. Reported before the
        // commit, the read would race and come back short.
        on(Progress {
            floor: lowest,
            target_floor: floor,
            ceiling,
            chunks_done,
            chunks_total,
            written,
            updated: &updated,
            pending: pending.len(),
        });
    }

    Ok(Outcome {
        floor: lowest,
        written,
        backfilled,
        unresolved: pending.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(b: u8) -> Hash<32> {
        Hash::from([b; 32])
    }

    /// Descending, and only chunks that exist on disk. Inventing a number that
    /// is not there turns a sparse range into a read error mid-pass.
    #[test]
    fn chunks_come_back_highest_first_and_only_if_present() {
        let on_disk = [10u64, 11, 12, 14, 15];
        let got = chunks_descending(&on_disk, 11 * CHUNK_SLOTS, 15 * CHUNK_SLOTS);
        assert_eq!(got, vec![15, 14, 12, 11]);
    }

    /// The range is inclusive of the chunk holding the floor — a floor mid-chunk
    /// still needs that chunk read, because the slots above it inside the chunk
    /// are in range.
    #[test]
    fn the_chunk_holding_the_floor_is_included() {
        let on_disk = [10u64, 11, 12];
        let got = chunks_descending(&on_disk, 11 * CHUNK_SLOTS + 5, 12 * CHUNK_SLOTS);
        assert!(got.contains(&11), "{got:?}");
    }

    /// An outref is resolved once and then gone: leaving it in the open set
    /// would re-apply its negative delta on the next pass that saw the same
    /// source, which the `INSERT OR IGNORE` in `add_deltas` would mask rather
    /// than fix.
    #[test]
    fn resolving_an_outref_removes_it_from_the_open_set() {
        let mut p = Pending::default();
        p.want((h(1), 0), h(9));
        assert_eq!(p.len(), 1);

        let waiters = p.resolve(&(h(1), 0)).expect("a waiter");
        assert_eq!(waiters, vec![h(9)]);
        assert!(p.is_empty());
        assert!(p.resolve(&(h(1), 0)).is_none(), "resolved only once");
    }

    /// Two transactions can wait on different outputs of the same transaction,
    /// and both must be served when it comes into view.
    #[test]
    fn separate_outputs_of_one_source_serve_separate_spenders() {
        let mut p = Pending::default();
        p.want((h(1), 0), h(8));
        p.want((h(1), 1), h(9));
        assert_eq!(p.resolve(&(h(1), 0)).unwrap(), vec![h(8)]);
        assert_eq!(p.resolve(&(h(1), 1)).unwrap(), vec![h(9)]);
    }

    /// Nothing waiting means nothing to serve — the common case, since most
    /// inputs of a watched transaction never carried the asset.
    #[test]
    fn an_unwanted_outref_resolves_to_nothing() {
        let mut p = Pending::default();
        assert!(p.resolve(&(h(1), 0)).is_none());
    }

    // ── progress reporting ──────────────────────────────────────────────────

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
        }
    }

    /// A reverse pass counts DOWN, so progress is how far the floor has fallen
    /// from the ceiling toward the target — not how far it is above zero.
    #[test]
    fn progress_measures_the_floor_falling_toward_the_target() {
        assert_eq!(prog(1_000, 0, 1_000).fraction(), 0.0, "nothing covered yet");
        assert_eq!(prog(500, 0, 1_000).fraction(), 0.5);
        assert_eq!(prog(0, 0, 1_000).fraction(), 1.0, "reached the target");
    }

    /// A pass with nothing to do reads as finished. Dividing by a zero-width
    /// range would be the alternative, on a state that legitimately occurs the
    /// moment coverage already reaches the requested floor.
    #[test]
    fn an_empty_range_reads_as_complete_rather_than_dividing_by_zero() {
        assert_eq!(prog(500, 500, 500).fraction(), 1.0);
    }

    /// The floor can sit below the target when the last chunk overshoots — a
    /// chunk is 21,600 slots and the target rarely lands on a boundary. That
    /// must clamp, never report over 100%.
    #[test]
    fn overshooting_the_target_clamps_at_one() {
        assert_eq!(prog(0, 100, 1_000).fraction(), 1.0);
    }

    /// The date is what a reader actually sees. Pinned against the Shelley
    /// start slot, whose date is independently known.
    #[test]
    fn a_slot_renders_as_its_calendar_date() {
        // Shelley began 2020-07-29.
        assert_eq!(slot_date(4_492_800), "2020-07-29");
    }
}
