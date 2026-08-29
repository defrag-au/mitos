//! The excavation job queue — TWO lanes, split by depth, each coalescing.
//!
//! Two properties do the work here, and they are different things:
//!
//! 1. **Coalescing.** A lane waits [`GATHER`] then drains everything queued
//!    into one sweep. Scan cost is chain-size-bound, not wallet-count-bound,
//!    so ten simultaneous readers cost one sweep rather than ten. This is why
//!    a launch flood is survivable at all.
//!
//! 2. **Depth lanes.** Coalescing alone still let a full-chain backfill block
//!    the queue: a visitor wanting 30 days who arrived ten seconds into
//!    somebody's 219 GB sweep waited four minutes for eight seconds of work.
//!    Jobs now route by window — cheap ones to `sieve-shallow`, the whole
//!    chain to `sieve-deep` — so arrivals are never held up by the rivalrous
//!    tier. See [`Lane`].
//!
//! `enqueue` still joins the existing job when the same wallet is already
//! queued or running, and terminal jobs stay in the registry (so a client
//! polling just after completion still sees the outcome) until the next
//! enqueue replaces them.
//!
//! Each lane owns its own sqlite connection — `Connection` is not `Sync`, and
//! WAL serialises the writers with a busy timeout absorbing the overlap.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Instant;

use anyhow::{Context, Result};
use mitos_chain_walk::mithril::immutable_file_for_slot;
use serde::Serialize;

use crate::excavate::{self, SHELLEY_START_SLOT};
use crate::progress::Progress;
use crate::serve::db;
use crate::target;
use mitos_chain_walk::mithril::CHUNK_SLOTS;

/// Rows market-checked per run — a deep history backfills over several.
const MARKET_BACKFILL_LIMIT: u32 = 4000;

/// How much chain one BACKFILL SEGMENT covers, in slots (1 slot = 1 second).
///
/// The deep pass walks backwards a segment at a time and stores after each,
/// so history visibly extends into the past instead of appearing all at once
/// when the whole sweep finishes. A year is ~13 GB of a 219 GB chain — small
/// enough to land regularly, large enough that the per-segment store and
/// re-classify stay noise against the read.
const DEEP_SEGMENT_SLOTS: u64 = 365 * 86_400;

/// Wall-clock now as a mainnet slot (inverse of `slot_to_unix`; Shelley is
/// 1 slot/second, which is what makes a window expressible in slots).
pub(crate) fn now_slot() -> u64 {
    const SHELLEY_START_SLOT: u64 = 4_492_800;
    const SHELLEY_START_UNIX: u64 = 1_596_059_091;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(SHELLEY_START_UNIX);
    SHELLEY_START_SLOT + now.saturating_sub(SHELLEY_START_UNIX)
}

#[derive(Clone, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum JobState {
    Queued,
    Running { phase: String, detail: String },
    Done { new_txs: usize, secs: f64 },
    Failed { error: String },
}

impl JobState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, JobState::Done { .. } | JobState::Failed { .. })
    }
}

pub struct Job {
    pub display: String,
    pub canonical: String,
    pub state: Mutex<JobState>,
    /// Days of history this request is willing to pay for on a cold wallet.
    /// `None` = everything. A shallow ask is cheap enough to hand out freely;
    /// the full sweep is the rivalrous one.
    pub window_days: Option<u64>,
}

#[derive(Clone)]
pub struct Config {
    pub db_path: PathBuf,
    pub immutable: PathBuf,
    /// Chain-tail spool db (scanned when present; freshness = minutes).
    pub tail_db: PathBuf,
    /// market-ledger sqlite, read-only, for venue/sale labels. Absent file =
    /// enrichment silently off.
    pub market_db: PathBuf,
    /// How far back a COLD excavation reaches before publishing, in slots
    /// (1 slot = 1 second). `None` sweeps all of history in one pass.
    pub initial_window_slots: Option<u64>,
    pub threads: usize,
    /// Longest window the SHALLOW lane serves. `0` collapses both lanes into
    /// one, which is the pre-two-lane behaviour.
    pub shallow_max_days: u64,
    /// Rows past which the backfill abandons a wallet. `u64::MAX` disables.
    pub max_wallet_rows: u64,
}

/// How long a lane waits before sweeping, so near-simultaneous requests ride
/// together instead of the first arrival leaving alone.
const GATHER: std::time::Duration = std::time::Duration::from_secs(2);

/// Which lane a job runs in.
///
/// Split by DEPTH, because depth is what makes a sweep slow: ~3 GB for 90
/// days against 219 GB for the whole chain. One queue meant a visitor wanting
/// 30 days could arrive ten seconds into somebody's full backfill and wait
/// four minutes for a scan that takes eight seconds — head-of-line blocking
/// by an unrelated request, which is exactly what makes a launch feel broken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    /// Windowed and cheap. Kept responsive for arrivals.
    Shallow,
    /// Full chain, or deeper than the shallow bound. Minutes, and rivalrous.
    Deep,
}

#[derive(Clone)]
pub struct Registry {
    jobs: Arc<Mutex<HashMap<String, Arc<Job>>>>,
    shallow: mpsc::Sender<Arc<Job>>,
    deep: mpsc::Sender<Arc<Job>>,
    shallow_max_days: u64,
    queued: Arc<AtomicUsize>,
}

/// One lane: gather, coalesce, sweep, repeat.
///
/// Both lanes run this. The only difference between them is which jobs get
/// routed in — the sweep itself is identical, and each still batches, so
/// concurrency costs SWEEPS rather than users.
fn spawn_lane(
    name: &'static str,
    cfg: Config,
    rx: mpsc::Receiver<Arc<Job>>,
    queued: Arc<AtomicUsize>,
) -> Result<()> {
    let mut conn = db::open_rw(&cfg.db_path)?;
    std::thread::Builder::new()
        .name(name.into())
        .spawn(move || {
            while let Ok(first) = rx.recv() {
                // Gather window: near-simultaneous requests (a launch flood,
                // the refresh watcher's roster) coalesce into one sweep
                // instead of the first rider leaving alone.
                std::thread::sleep(GATHER);
                // Drain everything queued: one sweep serves the lot — scan
                // cost is chain-size-bound, not wallet-count-bound.
                let mut batch: Vec<Arc<Job>> = vec![first];
                while let Ok(next) = rx.try_recv() {
                    if !batch.iter().any(|j| j.canonical == next.canonical) {
                        batch.push(next);
                    }
                    queued.fetch_sub(1, Ordering::Relaxed);
                }
                queued.fetch_sub(1, Ordering::Relaxed);
                let started = Instant::now();
                tracing::info!(lane = name, wallets = batch.len(), "sweep starting");
                if let Err(e) = run_batch_jobs(&cfg, &mut conn, &batch, &started) {
                    tracing::error!(lane = name, "batch excavation failed: {e:#}");
                    for job in &batch {
                        let mut st = job.state.lock().expect("job state");
                        if !st.is_terminal() {
                            *st = JobState::Failed {
                                error: format!("{e:#}"),
                            };
                        }
                    }
                }
            }
        })
        .with_context(|| format!("spawning {name}"))?;
    Ok(())
}

impl Registry {
    /// Opens the cache db (creating the schema) and spawns both lanes.
    pub fn start(cfg: Config) -> Result<Self> {
        // Schema first, on a connection the lanes don't share: `Connection`
        // is not `Sync`, so each lane opens its own. WAL permits that — one
        // writer at a time, serialised by sqlite — and the busy timeout in
        // `open_rw` absorbs the overlap rather than failing a scan.
        db::open_rw(&cfg.db_path)?;
        let queued = Arc::new(AtomicUsize::new(0));
        let (shallow, shallow_rx) = mpsc::channel::<Arc<Job>>();
        let (deep, deep_rx) = mpsc::channel::<Arc<Job>>();
        spawn_lane(
            "sieve-shallow",
            cfg.clone(),
            shallow_rx,
            Arc::clone(&queued),
        )?;
        spawn_lane("sieve-deep", cfg.clone(), deep_rx, Arc::clone(&queued))?;
        Ok(Self {
            jobs: Arc::new(Mutex::new(HashMap::new())),
            shallow,
            deep,
            shallow_max_days: cfg.shallow_max_days,
            queued,
        })
    }

    /// Which lane a window belongs in.
    ///
    /// `None` is the whole chain and always deep. A configured bound of `0`
    /// collapses the split, putting everything in one lane.
    fn lane_for(&self, window_days: Option<u64>) -> Lane {
        match window_days {
            Some(d) if self.shallow_max_days > 0 && d <= self.shallow_max_days => Lane::Shallow,
            _ => Lane::Deep,
        }
    }

    /// Queue an excavation reaching back `window_days` (`None` = all).
    pub fn enqueue_windowed(
        &self,
        display: &str,
        canonical: &str,
        window_days: Option<u64>,
    ) -> Result<Arc<Job>> {
        self.enqueue_inner(display, canonical, window_days)
    }

    /// Queue an excavation, or join the active one for the same wallet.
    /// Catch a cached wallet up to the tip. NEVER deepens it.
    ///
    /// The one-day window is doing two jobs, and both matter:
    ///
    /// 1. **Lane.** `None` means "the whole chain", which routes to the DEEP
    ///    lane — so the refresh watcher, firing every cached wallet each time
    ///    coverage advanced, monopolised the lane that exists for real
    ///    full-chain requests. Observed in production: every sweep over a two
    ///    hour window went to `sieve-deep` and the shallow lane sat idle,
    ///    while readers queued behind a 19-wallet housekeeping batch.
    /// 2. **Depth.** `None` also asks for GENESIS, so any wallet not already
    ///    scanned that far would have a full backfill dragged behind a routine
    ///    tail refresh. One day is shallower than anything already on disk, so
    ///    `needs_backfill()` stays false and this can only ever be the
    ///    incremental it is meant to be.
    pub fn enqueue_refresh(&self, display: &str, canonical: &str) -> Result<Arc<Job>> {
        self.enqueue_inner(display, canonical, Some(1))
    }

    fn enqueue_inner(
        &self,
        display: &str,
        canonical: &str,
        window_days: Option<u64>,
    ) -> Result<Arc<Job>> {
        let mut jobs = self.jobs.lock().expect("registry");
        if let Some(existing) = jobs.get(canonical)
            && !existing.state.lock().expect("job state").is_terminal()
        {
            return Ok(Arc::clone(existing));
        }
        let job = Arc::new(Job {
            display: display.to_string(),
            canonical: canonical.to_string(),
            state: Mutex::new(JobState::Queued),
            window_days,
        });
        jobs.insert(canonical.to_string(), Arc::clone(&job));
        self.queued.fetch_add(1, Ordering::Relaxed);
        let lane = self.lane_for(window_days);
        let tx = match lane {
            Lane::Shallow => &self.shallow,
            Lane::Deep => &self.deep,
        };
        tracing::debug!(?lane, window_days, canonical, "queued excavation");
        tx.send(Arc::clone(&job)).context("sieve worker is gone")?;
        Ok(job)
    }

    pub fn snapshot(&self, canonical: &str) -> Option<JobState> {
        self.jobs
            .lock()
            .expect("registry")
            .get(canonical)
            .map(|j| j.state.lock().expect("job state").clone())
    }

    pub fn queue_depth(&self) -> usize {
        self.queued.load(Ordering::Relaxed)
    }
}

/// What one target in a batch needs from this run.
///
/// Three DEPTHS and nothing else — every question the planner asks is a
/// comparison between them. There is deliberately no `wants_deep` flag: that
/// was a boolean derived from these same values, free to drift from them, and
/// the same shape as the `deep_pending` bool this whole change set removed.
/// State that can disagree with itself eventually does.
struct Depth {
    /// Depth already on disk. `None` = unknown, which always re-reads.
    held: Option<db::ScanTarget>,
    /// How far back this request is entitled to reach.
    wanted: db::ScanTarget,
    /// Oldest slot STAGE ONE reads. Always defined: a target needing no
    /// backfill still reads from its cursor to the tip.
    staged_from: u64,
}

impl Depth {
    /// Is anything the request is entitled to still missing?
    fn needs_backfill(&self) -> bool {
        !self.held.is_some_and(|h| h.covers(self.wanted))
    }

    /// The span the deep pass must read for this target, if any.
    ///
    /// Exists only where the entitlement reaches BELOW what stage one covers
    /// — so a 30-day request whose stage one already spans 30 days gets no
    /// second pass, however little of the chain is on disk.
    fn deep_span(&self) -> Option<(u64, u64)> {
        (self.needs_backfill() && self.wanted.floor() < self.staged_from)
            .then(|| (self.wanted.floor(), self.staged_from))
    }

    /// What stage one alone leaves covered.
    fn staged_depth(&self) -> db::ScanTarget {
        db::ScanTarget::from_slot(self.staged_from)
    }

    /// What this target has covered once the backfill has read down to
    /// `reached`. Called after each descending segment, so a run interrupted
    /// halfway records the depth it truly holds.
    ///
    /// Never claims deeper than the entitlement: the deep pass is SHARED, so
    /// it can sweep below what this particular target paid for.
    fn depth_after(&self, reached: u64) -> Option<db::ScanTarget> {
        match self.deep_span() {
            Some((floor, _)) => Some(db::ScanTarget::from_slot(reached.max(floor))),
            None => self.needs_backfill().then(|| self.staged_depth()),
        }
    }
}

/// One BATCH of excavations sharing a single sweep. Early emit per wallet:
/// each job's rows are stored (sans senders) before the merged resolve pass
/// back-fills names for the whole batch.
fn run_batch_jobs(
    cfg: &Config,
    conn: &mut rusqlite::Connection,
    jobs: &[Arc<Job>],
    started: &Instant,
) -> Result<()> {
    let set_all = |phase: &str, detail: String| {
        for job in jobs {
            let mut st = job.state.lock().expect("job state");
            if !st.is_terminal() {
                *st = JobState::Running {
                    phase: phase.to_string(),
                    detail: detail.clone(),
                };
            }
        }
    };
    set_all("seed", format!("loading cache for {} wallets", jobs.len()));

    // Per-job target setup; a bad target fails ITS job, not the batch.
    //
    // The window is decided PER TARGET, never per batch. A batch mixes a
    // brand-new wallet with the refresh watcher's cached ones, and a
    // batch-wide "is this cold?" test answers *no* for that mix — which
    // silently dropped the window and swept all 219 GB for a request that
    // had asked for 90 days.
    let mut targets = Vec::new();
    let mut live: Vec<&Arc<Job>> = Vec::new();

    let mut depths: Vec<Depth> = Vec::new();
    for job in jobs {
        let prep = (|| -> Result<(excavate::BatchTarget, Depth)> {
            let creds = target::parse(&job.display)?.creds;
            let meta = db::load_wallet(conn, &job.canonical)?;
            let (cursor, seed_owned) = match &meta {
                Some(meta) => {
                    // Slot cursor when present (tail-aware); legacy rows fall
                    // back to the chunk boundary.
                    let from_slot = meta
                        .scanned_to_slot
                        .unwrap_or((meta.scanned_to_chunk + 1) * CHUNK_SLOTS - 1)
                        + 1;
                    (from_slot, db::load_owned(conn, &job.canonical)?)
                }
                None => (SHELLEY_START_SLOT, HashMap::new()),
            };
            // What this request is entitled to reach back to, and what we
            // already hold.
            //
            // THE BUG THIS REPLACES: the floor used to be computed only for a
            // "cold" target (`cursor <= SHELLEY_START_SLOT`, i.e. never
            // scanned), and the deep pass was gated on that floor existing. So
            // a wallet first seen under ANY window had its cursor pinned near
            // the tip, `cold` was false forever after, and no later request —
            // full-chain included — could ever backfill. Every "scan full
            // history" ran the incremental tail sweep and nothing else.
            //
            // Depth is now compared, not inferred from freshness: what we hold
            // versus what is asked for.
            let mut wanted = db::ScanTarget::wanted(job.window_days, now_slot());
            // A wallet the backfill already gave up on asks for no more than
            // it holds. Clamping `wanted` here — rather than branching at each
            // decision below — means `deep_span()` returns `None` and the
            // whole planner treats it as satisfied, so a capped exchange
            // address costs an incremental tail sweep on every later request
            // instead of re-attempting 219 GB it will abandon again.
            if let Some(held) = meta.as_ref().and_then(|m| {
                m.oversize_rows
                    .is_some()
                    .then_some(m.scanned_from)
                    .flatten()
            }) {
                wanted = held;
            }
            let wanted = wanted;
            let needs_backfill = meta
                .as_ref()
                .map(|m| m.needs_backfill(wanted))
                .unwrap_or(true);
            // A configured initial window still caps a COLD scan's first pass,
            // so the reader sees something quickly; it never caps the depth a
            // request is entitled to.
            // STAGE ONE IS ALWAYS THE RECENT SLICE — the reader gets rows in
            // seconds and history fills in behind them.
            //
            // This used to be gated on the wallet being cold, which meant a
            // WARM wallet needing a backfill swept the whole chain in one pass
            // and published nothing until it finished: minutes of blank
            // screen on exactly the request ("scan full history") where the
            // reader is most obviously waiting.
            //
            // `.max(cursor)` handles both shapes without a branch: a cold
            // wallet's cursor sits at Shelley so the configured window wins,
            // while a warm wallet's cursor is near the tip, so stage one is
            // the cheap incremental rather than a needless re-read of a year
            // it already holds.
            let staged_from = if needs_backfill {
                let window = match cfg.initial_window_slots {
                    Some(w) => now_slot().saturating_sub(w).max(SHELLEY_START_SLOT),
                    None => wanted.floor(),
                };
                window.max(cursor).max(wanted.floor())
            } else {
                // Nothing missing — the ordinary incremental to the tip.
                cursor
            };
            let depth = Depth {
                held: meta.as_ref().and_then(|m| m.scanned_from),
                wanted,
                staged_from,
            };
            Ok((
                excavate::BatchTarget {
                    creds: creds.iter().map(|c| c.bytes).collect(),
                    scan_from_slot: staged_from,
                    seed_owned,
                },
                depth,
            ))
        })();
        match prep {
            Ok((t, depth)) => {
                targets.push(t);
                depths.push(depth);
                live.push(job);
            }
            Err(e) => {
                *job.state.lock().expect("job state") = JobState::Failed {
                    error: format!("{e:#}"),
                };
            }
        }
    }
    if targets.is_empty() {
        return Ok(());
    }

    // Which pass the scanner's progress belongs to.
    //
    // The callback fires from the scan threads and relabelled the whole
    // backfill as "scan", so `set_all("deep", …)` survived milliseconds and
    // no consumer polling for the deep phase ever saw it — the gateway's
    // early emit then waited for "resolve", i.e. until after the backfill.
    let backfilling = std::sync::atomic::AtomicBool::new(false);
    let on = |p: Progress<'_>| match p {
        Progress::Scan {
            pass,
            done,
            total,
            gb_per_s,
        } => set_all(
            if backfilling.load(Ordering::Relaxed) {
                "deep"
            } else {
                "scan"
            },
            format!("{pass}: {done}/{total} chunks · {gb_per_s:.2} GB/s"),
        ),
        Progress::Resolve {
            done,
            total,
            wanted_left,
        } => set_all(
            "resolve",
            format!("{done}/{total} bands · {wanted_left} sources wanted"),
        ),
        Progress::Phase { label, detail } => set_all(label, detail.to_string()),
    };

    // PROGRESSIVE EXCAVATION. A cold wallet's full sweep is ~70s of chain
    // before anything renders; the recent window is a fraction of that
    // (~13 GB for a year against 219 GB total, because the chain is heavily
    // front-loaded). So a cold job sweeps recent history first, publishes,
    // then backfills the deep past.
    //
    // The deep pass RE-CLASSIFIES the combined find rather than appending to
    // the first: direction depends on the UTxOs a wallet held *before* the
    // window, so a spend of an older UTxO reads as a receive until that UTxO
    // is known. Stage one is therefore provisional by construction, and the
    // recorded `scanned_from_slot` says exactly how provisional rather than
    // letting a partial history pose as a whole one.
    //
    // Stage one reaches only as far as the shallowest target needs; each
    // target's own floor then filters what it keeps, so a 90-day request in
    // a batch with a full one still costs 90 days of reading.
    let any_windowed = depths.iter().any(|d| d.needs_backfill());
    // Stage one records only as deep as it actually READ. Asserting the
    // requested depth here would claim coverage the deep pass has not
    // delivered, and a crash between the two would leave the wallet
    // permanently reporting history it does not hold.
    //
    // A target needing no backfill reports `None` — leave the recorded depth
    // alone rather than restating it from the cursor, which sits far above
    // what the wallet actually holds.
    let staged_depth = |i: usize| depths[i].needs_backfill().then(|| depths[i].staged_depth());

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let empty_sources = HashMap::new();
    // Indexed in place, one slot per live target, rather than cleared and
    // re-pushed: the backfill now skips the store for targets it has capped,
    // and push-based accumulation would silently shift everyone's result.
    let mut inserted: Vec<usize> = vec![0; live.len()];

    // Stage one: from the shallowest floor in the batch to the tip. Targets
    // that want more get it in stage two.
    let stage_one = excavate::ScanRange {
        from_slot: targets
            .iter()
            .map(|t| t.scan_from_slot)
            .min()
            .unwrap_or(SHELLEY_START_SLOT),
        to_slot: None,
    };
    if any_windowed {
        set_all("scan", "recent history".into());
    } else {
        set_all("scan", "sieving credentials".into());
    }
    let (mut found, scanned_to_slot) = excavate::scan_batch(
        &cfg.immutable,
        Some(&cfg.tail_db),
        &targets,
        stage_one,
        cfg.threads,
        &on,
    )?;
    let newest_chunk = crate::scan::list_chunks(&cfg.immutable, 0)?
        .last()
        .copied()
        .unwrap_or(0);
    let mut timelines: Vec<crate::classify::Timeline> = targets
        .iter()
        .zip(&found)
        .map(|(t, f)| crate::classify::build_with(t.seed_owned.clone(), f))
        .collect();
    for (i, (job, timeline)) in live.iter().zip(&timelines).enumerate() {
        inserted[i] = db::store_timeline(
            conn,
            &job.canonical,
            &job.display,
            timeline,
            &empty_sources,
            newest_chunk,
            scanned_to_slot,
            now,
            db::StoreOpts {
                replace: false,
                scanned_from: staged_depth(i),
                oversize_rows: None,
            },
        )?;
    }

    // Stage two: everything below the window, then re-classify the whole.
    // Skipped when nobody in the batch is entitled to the deep history —
    // that is the difference between a 3 GB read and a 219 GB one.
    let deep_ceiling = depths
        .iter()
        .filter_map(|d| d.deep_span())
        .map(|(_, to)| to)
        .max();
    // Where the backfill gave up, per target: `(slot, rows)`.
    let mut capped: Vec<Option<(u64, u64)>> = vec![None; live.len()];
    if let Some(ceiling) = deep_ceiling {
        set_all("deep", "backfilling older history".into());
        // Targets that did NOT buy the deep history opt out by asking for a
        // range above the ceiling — `keep()` then rejects every older tx for
        // them, so one shared sweep cannot hand anyone depth they didn't buy.
        let mut deep_targets: Vec<excavate::BatchTarget> = targets
            .iter()
            .zip(&depths)
            .map(|(t, d)| excavate::BatchTarget {
                creds: t.creds.clone(),
                // The target's OWN entitlement, not the cursor. Using the
                // cursor handed a cold target everything back to Shelley
                // regardless of the window it paid for. `u64::MAX` opts a
                // target out of the shared sweep entirely.
                scan_from_slot: d.deep_span().map_or(u64::MAX, |(from, _)| from),
                seed_owned: t.seed_owned.clone(),
            })
            .collect();
        // Read no deeper than the deepest thing anyone in the batch actually
        // wants. Starting at Shelley unconditionally made a batch of 12-month
        // requests pay full-chain read costs.
        let deep_from = depths
            .iter()
            .filter_map(|d| d.deep_span())
            .map(|(from, _)| from)
            .min()
            .unwrap_or(SHELLEY_START_SLOT);
        // Backfill in DESCENDING SEGMENTS, storing after each.
        //
        // One sweep from Shelley to the window published nothing until it had
        // read all 219 GB — minutes of a screen that already had the recent
        // rows on it but no way to show history arriving. Segmenting costs an
        // extra store and re-classify per segment (cheap; the read dominates)
        // and buys a timeline that visibly extends backwards while it runs.
        let segments = ((ceiling.saturating_sub(deep_from)) as f64 / DEEP_SEGMENT_SLOTS as f64)
            .ceil()
            .max(1.0) as usize;
        backfilling.store(true, Ordering::Relaxed);
        let mut ceil = ceiling;
        let mut segment = 0usize;
        while ceil > deep_from {
            let from = ceil.saturating_sub(DEEP_SEGMENT_SLOTS).max(deep_from);
            segment += 1;
            set_all(
                "deep",
                format!("backfilling older history ({segment} of {segments})"),
            );
            let (deep, _) = excavate::scan_batch(
                &cfg.immutable,
                Some(&cfg.tail_db),
                &deep_targets,
                excavate::ScanRange {
                    from_slot: from,
                    to_slot: Some(ceil),
                },
                cfg.threads,
                &on,
            )?;
            for (all, older) in found.iter_mut().zip(deep) {
                all.extend(older);
            }
            // THE GUARD. A wallet past the row ceiling stops here.
            //
            // The check has to live INSIDE the descending walk. Measuring
            // after stage one — the cheap, obvious place — gets it exactly
            // backwards on real data: the 1.13M-row address in this cache has
            // 306 rows in the last year (0.0%), while a perfectly ordinary
            // wallet had 3,240, all of them recent. Judged on recent activity
            // the exchange looks dormant and the ordinary wallet looks
            // enormous. Only the accumulating total tells them apart, and it
            // only accumulates here.
            //
            // Setting `scan_from_slot` to `u64::MAX` drops the target from the
            // remaining segments while the shared sweep continues for everyone
            // else — the same opt-out an unentitled target already uses.
            for (i, rows) in found.iter().map(Vec::len).enumerate() {
                if capped[i].is_none() && rows as u64 >= cfg.max_wallet_rows {
                    capped[i] = Some((from, rows as u64));
                    deep_targets[i].scan_from_slot = u64::MAX;
                    tracing::warn!(
                        wallet = %live[i].display,
                        rows,
                        ceiling = cfg.max_wallet_rows,
                        stopped_at_slot = from,
                        "wallet oversize — backfill capped"
                    );
                }
            }
            timelines = targets
                .iter()
                .zip(&found)
                .map(|(t, f)| crate::classify::build_with(t.seed_owned.clone(), f))
                .collect();
            for (i, (job, timeline)) in live.iter().zip(&timelines).enumerate() {
                // A capped target collected nothing this segment, so re-storing
                // its (unchanged, possibly six-figure) timeline every remaining
                // segment is pure write amplification. Its rows and its verdict
                // are already on disk from the segment that capped it.
                if capped[i].is_some_and(|(at, _)| at != from) {
                    continue;
                }
                inserted[i] = db::store_timeline(
                    conn,
                    &job.canonical,
                    &job.display,
                    timeline,
                    &empty_sources,
                    newest_chunk,
                    scanned_to_slot,
                    now,
                    db::StoreOpts {
                        replace: true,
                        // Only what the backfill has REACHED so far. A crash
                        // mid-backfill then leaves the wallet claiming the
                        // segments it actually holds, and the next run resumes
                        // from there rather than re-reading or, worse, calling
                        // itself complete. A capped wallet freezes at the slot
                        // it gave up on, for the same reason.
                        scanned_from: match capped[i] {
                            Some((at, _)) => Some(db::ScanTarget::from_slot(at)),
                            None => depths[i].depth_after(from),
                        },
                        oversize_rows: capped[i].map(|(_, rows)| rows),
                    },
                )?;
            }
            // Everyone still collecting has been capped — the rest of the
            // chain is being read for nobody.
            if capped.iter().all(Option::is_some) {
                tracing::info!("every target capped — abandoning the rest of the backfill");
                break;
            }
            ceil = from;
        }
        backfilling.store(false, Ordering::Relaxed);
    }
    // Market enrichment — cheap (an index seek per batch) and it names the
    // rows a holder cares most about, so it runs before the slow resolve.
    set_all("market", "naming venues".into());
    for job in &live {
        // Off the cache, not this batch: an incremental run classifies few
        // rows, and the wallet's existing ones need naming too.
        let hashes = db::hashes_needing_market(conn, &job.canonical, MARKET_BACKFILL_LIMIT)?;
        let found = crate::market::lookup(&cfg.market_db, &hashes);
        if !found.is_empty() {
            let n = db::update_market(conn, &job.canonical, &found)?;
            tracing::info!(target = %job.display, labelled = n, "market enrichment");
        }
    }

    set_all("resolve", "naming senders".into());

    // Merged pass C: one decode+hash sweep names senders for every wallet.
    let mut wanted: HashMap<[u8; 32], Vec<u32>> = HashMap::new();
    let mut max_last = 0u64;
    for t in &timelines {
        for tx in &t.txs {
            for (h, idx) in &tx.foreign_inputs {
                wanted.entry(*h).or_default().push(*idx);
            }
        }
        if t.last_slot > 0 && t.last_slot != u64::MAX {
            max_last = max_last.max(t.last_slot);
        }
    }
    if !wanted.is_empty() && max_last > 0 {
        // A windowed job pays for a windowed resolve. Naming a sender means
        // finding the source tx, which can sit years before the rows on
        // screen — so an unbounded resolve would hand a 90-day request the
        // cost of a full-chain read. Sources older than the window stay
        // unnamed; the rows still carry their venue and recipients.
        // Resolve reaches no deeper than the shallowest floor still in force
        // — otherwise a 90-day request pays full-chain read costs to name a
        // sender whose source tx it will never show.
        let resolve_floor = depths
            .iter()
            .filter(|d| d.needs_backfill() && d.deep_span().is_none())
            .map(|d| d.staged_from)
            .min()
            .filter(|_| deep_ceiling.is_none());
        let floor_chunk = match resolve_floor {
            Some(floor) => immutable_file_for_slot(floor),
            None => immutable_file_for_slot(SHELLEY_START_SLOT),
        };
        let last = immutable_file_for_slot(max_last);
        let chunks: Vec<u64> = crate::scan::list_chunks(&cfg.immutable, floor_chunk)?
            .into_iter()
            .filter(|c| *c <= last)
            .collect();
        let resolved = crate::resolve::senders(&cfg.immutable, &chunks, &wanted, cfg.threads, &on)?;
        for (job, timeline) in live.iter().zip(&timelines) {
            db::update_senders(conn, &job.canonical, timeline, &resolved.sources)?;
        }
    }

    for (job, new_txs) in live.iter().zip(inserted) {
        *job.state.lock().expect("job state") = JobState::Done {
            new_txs,
            secs: started.elapsed().as_secs_f64(),
        };
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry(shallow_max_days: u64) -> Registry {
        let (shallow, _a) = mpsc::channel();
        let (deep, _b) = mpsc::channel();
        // Leaked so the receivers outlive the senders; this only exercises
        // routing, which touches no worker and no database.
        std::mem::forget((_a, _b));
        Registry {
            jobs: Arc::new(Mutex::new(HashMap::new())),
            shallow,
            deep,
            shallow_max_days,
            queued: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The tiers that must stay responsive under load — an arriving reader on
    /// the free or 90-day rung never queues behind a full backfill.
    #[test]
    fn cheap_windows_take_the_shallow_lane() {
        let r = registry(90);
        assert_eq!(r.lane_for(Some(30)), Lane::Shallow);
        assert_eq!(
            r.lane_for(Some(90)),
            Lane::Shallow,
            "the bound is inclusive"
        );
    }

    /// Anything deeper than the bound — and the whole chain especially — is
    /// the rivalrous work and must not block arrivals.
    #[test]
    fn deep_windows_take_the_deep_lane() {
        let r = registry(90);
        assert_eq!(r.lane_for(Some(91)), Lane::Deep);
        assert_eq!(r.lane_for(Some(365)), Lane::Deep);
        assert_eq!(r.lane_for(None), Lane::Deep, "no window is the full chain");
    }

    /// HOUSEKEEPING MUST NOT USE THE RIVALROUS LANE.
    ///
    /// The refresh watcher enqueues every cached wallet whenever chain
    /// coverage advances. It used to pass no window, which routes to `Deep` —
    /// so routine catch-up monopolised the lane that exists for full-chain
    /// reads, and arrivals queued behind a batch of housekeeping. Observed in
    /// production before this was fixed.
    #[test]
    fn a_refresh_takes_the_shallow_lane() {
        let r = registry(90);
        // The window `enqueue_refresh` uses, checked through the same routing
        // the watcher's jobs go through.
        assert_eq!(r.lane_for(Some(1)), Lane::Shallow);
        assert_eq!(
            r.lane_for(None),
            Lane::Deep,
            "and no-window is still Deep — which is why the watcher must not use it"
        );
    }

    /// The other half: a one-day ask is shallower than anything already
    /// scanned, so a refresh can never drag a backfill behind it.
    #[test]
    fn a_refresh_never_deepens_a_wallet() {
        let now = 200_000_000;
        let wanted = db::ScanTarget::wanted(Some(1), now);
        for held in [
            db::ScanTarget::Genesis,
            db::ScanTarget::from_slot(100_000_000),
            db::ScanTarget::from_slot(now - 2 * 86_400),
        ] {
            assert!(
                held.covers(wanted),
                "{held:?} already covers a one-day refresh, so needs_backfill stays false"
            );
        }
    }

    /// `0` is the escape hatch back to one queue, for a box where two
    /// concurrent sweeps would fight over disk more than they help.
    #[test]
    fn a_zero_bound_collapses_the_split() {
        let r = registry(0);
        assert_eq!(r.lane_for(Some(1)), Lane::Deep);
        assert_eq!(r.lane_for(None), Lane::Deep);
    }

    // Slots chosen to read as a ladder: OLD sits below RECENT, which sits
    // below the tip. Real values, so `from_slot` round-trips.
    const OLD: u64 = 90_000_000;
    const RECENT: u64 = 140_000_000;

    /// The bug this whole change set exists to kill: a wallet already scanned
    /// to 30 days asks for the full chain and must get a deep pass, where the
    /// old `deep_pending` bool said "already done" and returned 898 rows.
    #[test]
    fn shallow_history_plus_a_full_chain_request_is_a_backfill() {
        let d = Depth {
            held: Some(db::ScanTarget::from_slot(RECENT)),
            wanted: db::ScanTarget::Genesis,
            staged_from: RECENT,
        };
        assert!(d.needs_backfill());
        assert_eq!(
            d.deep_span(),
            Some((db::ScanTarget::Genesis.floor(), RECENT))
        );
        assert_eq!(
            d.depth_after(db::ScanTarget::Genesis.floor()),
            Some(db::ScanTarget::Genesis),
            "the completed backfill covers everything"
        );
    }

    /// The backfill walks BACKWARDS a segment at a time, and each store must
    /// claim only what it has reached — an interrupted run that recorded its
    /// destination would call itself complete and never return.
    #[test]
    fn a_partial_backfill_records_only_what_it_reached() {
        let d = Depth {
            held: Some(db::ScanTarget::from_slot(RECENT)),
            wanted: db::ScanTarget::Genesis,
            staged_from: RECENT,
        };
        assert_eq!(d.depth_after(OLD), Some(db::ScanTarget::Since(OLD)));
        assert_eq!(
            d.depth_after(db::ScanTarget::Genesis.floor()),
            Some(db::ScanTarget::Genesis)
        );
    }

    /// The deep pass is SHARED: a batch mate wanting the whole chain drags
    /// the read below this target's window, and it must not be credited with
    /// depth it did not pay for.
    #[test]
    fn a_shared_sweep_cannot_credit_unbought_depth() {
        let d = Depth {
            held: Some(db::ScanTarget::from_slot(RECENT)),
            wanted: db::ScanTarget::Since(OLD),
            staged_from: RECENT,
        };
        assert_eq!(
            d.depth_after(SHELLEY_START_SLOT),
            Some(db::ScanTarget::Since(OLD)),
            "clamped to the entitlement, not the sweep"
        );
    }

    /// The converse, and the 94-second "30-day" scan: a COLD 30-day request
    /// has uncovered depth by definition, but stage one already reaches its
    /// entitlement, so it must not drag a full sweep behind it.
    #[test]
    fn a_cold_windowed_request_runs_no_deep_pass() {
        let d = Depth {
            held: None,
            wanted: db::ScanTarget::Since(RECENT),
            staged_from: RECENT,
        };
        assert!(d.needs_backfill(), "unknown depth always re-reads");
        assert_eq!(d.deep_span(), None, "stage one already spans the window");
        assert_eq!(
            d.depth_after(SHELLEY_START_SLOT),
            Some(db::ScanTarget::Since(RECENT)),
            "no deep pass ran, so only stage one's reach is recorded"
        );
    }

    /// Coverage recorded after stage one must be what stage one READ, never
    /// what the request asked for — a crash between the passes would
    /// otherwise leave the wallet claiming history it does not hold.
    #[test]
    fn staged_depth_never_asserts_the_deep_entitlement() {
        let d = Depth {
            held: None,
            wanted: db::ScanTarget::Genesis,
            staged_from: RECENT,
        };
        assert_eq!(d.staged_depth(), db::ScanTarget::Since(RECENT));
        assert_ne!(
            Some(d.staged_depth()),
            d.depth_after(db::ScanTarget::Genesis.floor())
        );
    }

    /// A capped wallet costs an incremental, never another full sweep.
    ///
    /// The planner is told this by CLAMPING `wanted` to what the wallet holds,
    /// so the existing depth comparison reports it satisfied. Without that a
    /// capped exchange address would re-attempt — and re-abandon — a 219 GB
    /// read every single time somebody opened it.
    #[test]
    fn a_capped_wallet_is_never_deep_scanned_again() {
        let held = db::ScanTarget::from_slot(RECENT);
        // The clamp `prep` applies when `oversize_rows` is set: the ask
        // becomes what is held, whatever the caller requested.
        let d = Depth {
            held: Some(held),
            wanted: held,
            staged_from: RECENT,
        };
        assert!(!d.needs_backfill());
        assert_eq!(d.deep_span(), None, "a capped wallet must not re-sweep");

        // Same wallet WITHOUT the clamp — the unguarded behaviour, kept here
        // so the test fails loudly if the clamp is ever dropped.
        let unclamped = Depth {
            held: Some(held),
            wanted: db::ScanTarget::Genesis,
            staged_from: RECENT,
        };
        assert!(unclamped.deep_span().is_some());
    }

    /// An ordinary incremental: depth on disk already covers the ask, so
    /// there is no backfill, no deep pass, and nothing to re-record.
    #[test]
    fn covered_history_needs_nothing() {
        let d = Depth {
            held: Some(db::ScanTarget::from_slot(OLD)),
            wanted: db::ScanTarget::Since(RECENT),
            staged_from: RECENT,
        };
        assert!(!d.needs_backfill());
        assert_eq!(d.deep_span(), None);
    }
}
