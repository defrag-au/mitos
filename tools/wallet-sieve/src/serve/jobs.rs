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
    pub fn enqueue(&self, display: &str, canonical: &str) -> Result<Arc<Job>> {
        self.enqueue_inner(display, canonical, None)
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

#[cfg(test)]
mod lane_tests {
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

    /// `0` is the escape hatch back to one queue, for a box where two
    /// concurrent sweeps would fight over disk more than they help.
    #[test]
    fn a_zero_bound_collapses_the_split() {
        let r = registry(0);
        assert_eq!(r.lane_for(Some(1)), Lane::Deep);
        assert_eq!(r.lane_for(None), Lane::Deep);
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
    /// What one target in the batch needs from this run.
    ///
    /// A struct rather than a tuple because the three booleans-and-slots it
    /// carries were previously positional, and `depths[i].1.is_some()` was
    /// doing load-bearing work in four places with no name attached to it.
    struct Depth {
        /// Where the incremental sweep would start from (the stored cursor).
        cursor: u64,
        /// Oldest slot stage one reaches, when a backfill is needed at all.
        floor: Option<u64>,
        /// This target still has history missing below what is held.
        wants_deep: bool,
        /// How far back the request is entitled to reach.
        wanted: db::ScanTarget,
    }

    let mut depths: Vec<Depth> = Vec::new();
    for job in jobs {
        let prep = (|| -> Result<(excavate::BatchTarget, u64, Option<u64>, bool, db::ScanTarget)> {
            let creds = target::parse(&job.display)?;
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
            let wanted = db::ScanTarget::wanted(job.window_days, now_slot());
            let needs_backfill = meta
                .as_ref()
                .map(|m| m.needs_backfill(wanted))
                .unwrap_or(true);
            // A configured initial window still caps a COLD scan's first pass,
            // so the reader sees something quickly; it never caps the depth a
            // request is entitled to.
            let cold = cursor <= SHELLEY_START_SLOT;
            let floor = if needs_backfill {
                let first_pass = match (cold, cfg.initial_window_slots) {
                    (true, Some(w)) => now_slot().saturating_sub(w).max(SHELLEY_START_SLOT),
                    _ => wanted.floor(),
                };
                // Never let the staged first pass start OLDER than the target.
                Some(first_pass.max(wanted.floor()))
            } else {
                None
            };
            let wants_deep = needs_backfill;
            Ok((
                excavate::BatchTarget {
                    creds: creds.iter().map(|c| c.bytes).collect(),
                    scan_from_slot: floor.unwrap_or(cursor),
                    seed_owned,
                },
                cursor,
                floor,
                wants_deep,
                wanted,
            ))
        })();
        match prep {
            Ok((t, cursor, floor, wants_deep, wanted)) => {
                targets.push(t);
                depths.push(Depth {
                    cursor,
                    floor,
                    wants_deep,
                    wanted,
                });
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

    let on = |p: Progress<'_>| match p {
        Progress::Scan {
            pass,
            done,
            total,
            gb_per_s,
        } => set_all(
            "scan",
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
    // is known. Stage one is therefore provisional by construction, and
    // `deep_pending` says so rather than letting a partial history pose as a
    // whole one.
    // Stage one reaches only as far as the shallowest target needs; each
    // target's own floor then filters what it keeps, so a 90-day request in
    // a batch with a full one still costs 90 days of reading.
    let any_windowed = depths.iter().any(|d| d.floor.is_some());
    // What stage one alone leaves covered: only as deep as it actually read.
    // Asserting the requested depth here would record coverage the deep pass
    // has not delivered yet, and a crash between the two would leave the
    // wallet permanently claiming history it does not hold.
    let staged_depth = |i: usize| depths[i].floor.map(db::ScanTarget::from_slot);
    // After the deep pass the request's full depth IS covered.
    let final_depth = |i: usize| {
        if depths[i].wants_deep {
            Some(depths[i].wanted)
        } else {
            staged_depth(i)
        }
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let empty_sources = HashMap::new();
    let mut inserted: Vec<usize> = Vec::with_capacity(live.len());

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
        inserted.push(db::store_timeline(
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
            },
        )?);
    }

    // Stage two: everything below the window, then re-classify the whole.
    // Skipped when nobody in the batch is entitled to the deep history —
    // that is the difference between a 3 GB read and a 219 GB one.
    let deep_ceiling = depths
        .iter()
        .filter(|d| d.wants_deep && d.floor.is_some())
        .filter_map(|d| d.floor)
        .max();
    if let Some(ceiling) = deep_ceiling {
        set_all("deep", "backfilling older history".into());
        // Targets that did NOT buy the deep history opt out by asking for a
        // range above the ceiling — `keep()` then rejects every older tx for
        // them, so one shared sweep cannot hand anyone depth they didn't buy.
        let deep_targets: Vec<excavate::BatchTarget> = targets
            .iter()
            .zip(&depths)
            .map(
                |(
                    t,
                    Depth {
                        cursor, wants_deep, ..
                    },
                )| excavate::BatchTarget {
                    creds: t.creds.clone(),
                    scan_from_slot: if *wants_deep { *cursor } else { u64::MAX },
                    seed_owned: t.seed_owned.clone(),
                },
            )
            .collect();
        let (deep, _) = excavate::scan_batch(
            &cfg.immutable,
            Some(&cfg.tail_db),
            &deep_targets,
            excavate::ScanRange {
                from_slot: SHELLEY_START_SLOT,
                to_slot: Some(ceiling),
            },
            cfg.threads,
            &on,
        )?;
        for (all, older) in found.iter_mut().zip(deep) {
            all.extend(older);
        }
        timelines = targets
            .iter()
            .zip(&found)
            .map(|(t, f)| crate::classify::build_with(t.seed_owned.clone(), f))
            .collect();
        inserted.clear();
        for (i, (job, timeline)) in live.iter().zip(&timelines).enumerate() {
            inserted.push(db::store_timeline(
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
                    // The deep pass has now delivered what this target asked
                    // for, so record its FULL depth — not the staged floor.
                    scanned_from: final_depth(i),
                },
            )?);
        }
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
            .filter(|d| d.floor.is_some() && !d.wants_deep)
            .filter_map(|d| d.floor)
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
