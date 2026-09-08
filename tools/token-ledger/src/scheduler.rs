//! The pool: policy walks as bounded range JOBS, run by walk workers and
//! seek workers, landed under a per-policy lock.
//!
//! `docs/design/POLICY_WALK_SCHEDULER.md` is the design; this is its
//! second step. What it replaces: one thread, one running pass at a time,
//! and the hooks that pass grew to serve readers who wanted stretches it
//! had not reached — detours for seeks, previews for policies queued
//! behind it. Each of those was a scheduler written as a special case
//! inside one loop.
//!
//! # Jobs
//!
//! A [`Job`] is one policy, one slot range, one kind. It runs as an
//! ordinary reverse pass over exactly that range and lands one manifest
//! entry — a range. Coverage is the set of ranges, so a job never needs to
//! know what other jobs did except through the manifest.
//!
//! - A **walk** is not a job. It is a GENERATOR: while a policy has a gap
//!   between its descent's frontier and its mint, the walk table emits the
//!   job for the next [`JOB_CHUNKS`] chunks below. Walk workers pull from
//!   the table round-robin across policies, so four policies descend at
//!   once and none waits behind another for longer than one job — and when
//!   nothing else wants them, all four land on the same policy. A new
//!   policy's first job is its newest ten days — what the preview used to
//!   be, with nothing special about it.
//! - A **seek** is a job for the window ending at the slot a reader asked
//!   for. Its own queue, its own workers, never behind a walk.
//!
//! # The frontier, not a flag
//!
//! Several walk jobs run on one policy at once because a job resolves its
//! own spenders' inputs through the tx-index rather than waiting for the
//! range below it to be read (`reverse::Pending::settle_through_index`,
//! run once at the end of every job). Nothing ties
//! one range to another, so the only thing the table has to track is which
//! ranges are CLAIMED: what the manifest says has been read, plus what is
//! in flight. The next job is the [`JOB_CHUNKS`] chunks below the bottom of
//! the topmost claimed stretch. Jobs therefore never overlap by
//! construction, and a stretch further down that a seek read is not the
//! frontier — it is a stretch the descent meets and merges with on its way
//! past, scanning it for resolution only.
//!
//! # Landing
//!
//! Every job lands under the policy's lock: reload the manifest, append the
//! range, write, and — when this was the last job in flight and no walk is
//! wanted — publish. The lock is held for the manifest write and the
//! compaction, never across a chunk read.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

use anyhow::Result;
use mitos_chain_walk::mithril::CHUNK_SLOTS;

use crate::archive::{self, SlotRange};
use crate::policy_api::{Live, PolicyHub, PolicyJob};
use crate::reverse;

/// The movement graph keys parties by STAKE — the wallet — which is worth
/// about ten times on the wire (ClayNation: 2.20 MB against 16.94 MB) and is
/// the right node for "who traded with whom". It merges addresses sharing a
/// staking credential, which is wrong for forensic work; that case rebuilds
/// with `token-ledger graph` and no `--by-stake`. See
/// `policy_archive::graph`.
const GRAPH_BY_STAKE: bool = true;

/// A job's reach: forty chunks, about ten days of chain, seconds through
/// the sieve gate. The unit of fairness between policies and of a seek's
/// window.
pub const JOB_CHUNKS: u64 = 40;

/// How many workers of each kind.
#[derive(Debug, Clone, Copy)]
pub struct Pool {
    pub walk_workers: usize,
    pub seek_workers: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobKind {
    /// The next stretch of a policy's descent.
    Walk,
    /// A window a reader asked for.
    Seek,
}

#[derive(Debug, Clone)]
pub struct Job {
    pub policy: String,
    pub kind: JobKind,
    /// `[from, to)`.
    pub range: SlotRange,
    /// The policy's first mint, when known — recorded on the manifest and
    /// the bound below which no job looks.
    pub first_mint: Option<u64>,
}

/// How far one walk job has got. Kept per job so a policy walked by four
/// workers at once still reports ONE descent to a reader.
#[derive(Debug, Clone, Copy)]
struct JobProgress {
    /// The range the job was handed — its identity in the table.
    range: SlotRange,
    /// Lowest slot it has read down to. Starts at the range's ceiling.
    floor: u64,
    chunks_done: u64,
    chunks_total: u64,
    written: u64,
    /// Spenders this job is still waiting on a source for.
    pending: usize,
}

/// One policy's place in the walk table.
struct WalkState {
    first_mint: Option<u64>,
    /// Walk jobs in flight for this policy, and where each has got to.
    /// Their ranges are CLAIMED: the next job is generated below all of
    /// them. Several at once because resolution happens inside a job.
    in_flight: Vec<JobProgress>,
    started: Instant,
    /// Transactions written by this walk's LANDED jobs, for the `Done`
    /// report. Jobs still in flight are counted through `in_flight`.
    written: u64,
    backfilled: u64,
}

impl WalkState {
    fn ranges(&self) -> Vec<SlotRange> {
        self.in_flight.iter().map(|j| j.range).collect()
    }

    /// The whole walk as ONE statement: how far down its jobs have reached
    /// between them, their chunks and rows summed, and the transactions the
    /// reporting job corrected in the chunk it just read — which is what a
    /// consumer re-reads. `None` before any job has reported a chunk.
    fn report(&self, top: u64, mint: u64, updated: Vec<String>) -> Option<PolicyJob> {
        let floor = self.in_flight.iter().map(|j| j.floor).min()?;
        let span = top.saturating_sub(mint).max(1) as f64;
        Some(PolicyJob::Running {
            fraction: (top.saturating_sub(floor) as f64 / span).clamp(0.0, 1.0),
            floor,
            date: reverse::slot_date(floor),
            chunks_done: self.in_flight.iter().map(|j| j.chunks_done).sum(),
            chunks_total: self.in_flight.iter().map(|j| j.chunks_total).sum(),
            written: self.written + self.in_flight.iter().map(|j| j.written).sum::<u64>(),
            updated,
            unresolved: self.in_flight.iter().map(|j| j.pending).max().unwrap_or(0),
        })
    }
}

/// What a walk worker gets when it asks for work.
enum Next {
    Job(Job),
    /// This policy's descent reached its mint: report and publish.
    Finished {
        policy: String,
        written: u64,
        backfilled: u64,
        secs: f64,
    },
}

/// `POST /policy/{p}/seek` — what the scheduler did with it.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SeekResponse {
    pub accepted: bool,
    /// Why not, when not: `already_read` | `in_flight` | `below_mint`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub because: Option<&'static str>,
    /// The window that is, or will be, read for this seek.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window: Option<SlotRange>,
}

/// A job running right now: what it is reading, and the live tier a read
/// folds on top of the archive while it does.
struct InFlight {
    /// The whole stretch the job will read, not how far it has got: a seek
    /// into a range a walk job is on its way through is not a second read.
    range: SlotRange,
    live: Arc<Mutex<Live>>,
}

pub struct Scheduler {
    seeks: Mutex<VecDeque<Job>>,
    seek_wake: Condvar,
    /// Policies with a walk wanted, in round-robin order.
    walks: Mutex<(HashMap<String, WalkState>, VecDeque<String>)>,
    walk_wake: Condvar,
    /// Jobs in flight per policy, walk and seek alike.
    inflight: Mutex<HashMap<String, Vec<InFlight>>>,
    /// Landing lock per policy.
    locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    /// Next pass sequence per policy, allocated here so concurrent jobs on
    /// one policy never share one.
    seqs: Mutex<HashMap<String, u32>>,
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            seeks: Mutex::new(VecDeque::new()),
            seek_wake: Condvar::new(),
            walks: Mutex::new((HashMap::new(), VecDeque::new())),
            walk_wake: Condvar::new(),
            inflight: Mutex::new(HashMap::new()),
            locks: Mutex::new(HashMap::new()),
            seqs: Mutex::new(HashMap::new()),
        }
    }

    // ── what the API asks ────────────────────────────────────────────────

    /// Walk this policy to its mint. Joining a walk already wanted is a
    /// no-op; the mint is kept if it was not known before.
    pub fn want_walk(&self, policy: &str, first_mint: Option<u64>) -> bool {
        let mut walks = self.walks.lock().expect("walks");
        let (states, order) = &mut *walks;
        if let Some(state) = states.get_mut(policy) {
            state.first_mint = state.first_mint.or(first_mint);
            return false;
        }
        states.insert(
            policy.to_string(),
            WalkState {
                first_mint,
                in_flight: Vec::new(),
                started: Instant::now(),
                written: 0,
                backfilled: 0,
            },
        );
        order.push_back(policy.to_string());
        drop(walks);
        self.walk_wake.notify_one();
        true
    }

    /// Is a walk wanted or running for this policy?
    pub fn walking(&self, policy: &str) -> bool {
        self.walks.lock().expect("walks").0.contains_key(policy)
    }

    /// Read the window ending at `at`, if nobody has. Declined when the
    /// stretch is read, when a seek over it is already queued or running,
    /// or when it sits below the mint.
    pub fn seek(&self, hub: &PolicyHub, policy: &str, at: u64) -> SeekResponse {
        let manifest = archive::load_manifest(&hub.policy_dir(policy))
            .ok()
            .flatten();
        let first_mint = manifest
            .as_ref()
            .and_then(|m| m.first_mint_slot)
            .or_else(|| {
                self.walks
                    .lock()
                    .expect("walks")
                    .0
                    .get(policy)
                    .and_then(|s| s.first_mint)
            });
        let floor = first_mint.unwrap_or(0);
        if at <= floor {
            return SeekResponse {
                accepted: false,
                because: Some("below_mint"),
                window: None,
            };
        }
        let from = floor.max(at.saturating_sub(JOB_CHUNKS * CHUNK_SLOTS));
        let window = SlotRange::new(from, at);
        // Read already? Only what the MANIFEST says: a job in flight over
        // it answers the next check.
        if manifest
            .as_ref()
            .is_some_and(|m| m.uncovered(from, at).is_empty())
        {
            return SeekResponse {
                accepted: false,
                because: Some("already_read"),
                window: Some(window),
            };
        }
        let covered_by_job = |r: &SlotRange| at > r.from && at <= r.to;
        let queued = self
            .seeks
            .lock()
            .expect("seeks")
            .iter()
            .any(|j| j.policy == policy && covered_by_job(&j.range));
        // A job in flight is reading its WHOLE range, however far down it
        // has got so far — a seek into a stretch a walk job is on its way
        // through is not a second read.
        let running = self
            .inflight
            .lock()
            .expect("inflight")
            .get(policy)
            .is_some_and(|jobs| jobs.iter().any(|j| covered_by_job(&j.range)));
        if queued || running {
            return SeekResponse {
                accepted: false,
                because: Some("in_flight"),
                window: Some(window),
            };
        }
        self.seeks.lock().expect("seeks").push_back(Job {
            policy: policy.to_string(),
            kind: JobKind::Seek,
            range: window,
            first_mint,
        });
        self.seek_wake.notify_one();
        SeekResponse {
            accepted: true,
            because: None,
            window: Some(window),
        }
    }

    /// Every job in flight for this policy, for the read path.
    pub fn inflight_for(&self, policy: &str) -> Vec<Arc<Mutex<Live>>> {
        self.inflight
            .lock()
            .expect("inflight")
            .get(policy)
            .map(|jobs| jobs.iter().map(|j| Arc::clone(&j.live)).collect())
            .unwrap_or_default()
    }

    /// Is anything queued, running or wanted for this policy? Publishing
    /// waits until nothing is.
    fn busy(&self, policy: &str) -> bool {
        self.walking(policy)
            || self
                .inflight
                .lock()
                .expect("inflight")
                .get(policy)
                .is_some_and(|v| !v.is_empty())
            || self
                .seeks
                .lock()
                .expect("seeks")
                .iter()
                .any(|j| j.policy == policy)
    }

    // ── what the workers ask ─────────────────────────────────────────────

    pub(crate) fn lock_for(&self, policy: &str) -> Arc<Mutex<()>> {
        Arc::clone(
            self.locks
                .lock()
                .expect("locks")
                .entry(policy.to_string())
                .or_default(),
        )
    }

    /// A sequence for a new pass on this policy: the manifest's next, the
    /// first time, then counting up here.
    pub(crate) fn next_seq(&self, hub: &PolicyHub, policy: &str) -> u32 {
        let mut seqs = self.seqs.lock().expect("seqs");
        let next = seqs.entry(policy.to_string()).or_insert_with(|| {
            archive::load_manifest(&hub.policy_dir(policy))
                .ok()
                .flatten()
                .map_or(0, |m| m.next_seq())
        });
        let seq = *next;
        *next += 1;
        seq
    }

    fn next_seek(&self) -> Job {
        let mut seeks = self.seeks.lock().expect("seeks");
        loop {
            if let Some(job) = seeks.pop_front() {
                return job;
            }
            seeks = self.seek_wake.wait(seeks).expect("seeks");
        }
    }

    /// The next walk job, round-robin over the policies that want one.
    /// Several jobs may be in flight for one policy; the next is generated
    /// below all of them, so four workers can descend one large policy
    /// together when nothing else wants them. A policy whose gap has closed
    /// AND whose last job has landed is reported finished. Blocks until
    /// there is something to do.
    fn next_walk(&self, hub: &PolicyHub) -> Next {
        let mut walks = self.walks.lock().expect("walks");
        loop {
            let (states, order) = &mut *walks;
            let candidates = order.len();
            for _ in 0..candidates {
                let Some(policy) = order.pop_front() else {
                    break;
                };
                let Some(state) = states.get_mut(&policy) else {
                    continue;
                };
                match walk_job(hub, &policy, state.first_mint, &state.ranges()) {
                    Some(job) => {
                        state.in_flight.push(JobProgress {
                            range: job.range,
                            floor: job.range.to,
                            chunks_done: 0,
                            chunks_total: 0,
                            written: 0,
                            pending: 0,
                        });
                        order.push_back(policy);
                        return Next::Job(job);
                    }
                    // Nothing left to generate — but a walk is not over
                    // until its last job has landed, or the report would
                    // claim a descent that is still writing.
                    None if !state.in_flight.is_empty() => {
                        order.push_back(policy);
                        continue;
                    }
                    None => {
                        let done = states.remove(&policy).expect("present");
                        return Next::Finished {
                            policy,
                            written: done.written,
                            backfilled: done.backfilled,
                            secs: done.started.elapsed().as_secs_f64(),
                        };
                    }
                }
            }
            walks = self.walk_wake.wait(walks).expect("walks");
        }
    }

    fn job_started(&self, hub: &PolicyHub, job: &Job) -> (Arc<Mutex<Live>>, u32) {
        let seq = self.next_seq(hub, &job.policy);
        let mut live = Live::new(seq);
        live.ceiling = job.range.to;
        live.floor = job.range.to;
        let live = Arc::new(Mutex::new(live));
        self.inflight
            .lock()
            .expect("inflight")
            .entry(job.policy.clone())
            .or_default()
            .push(InFlight {
                range: job.range,
                live: Arc::clone(&live),
            });
        (live, seq)
    }

    fn job_finished(&self, job: &Job, live: &Arc<Mutex<Live>>, out: Option<&reverse::Outcome>) {
        {
            let mut inflight = self.inflight.lock().expect("inflight");
            if let Some(v) = inflight.get_mut(&job.policy) {
                v.retain(|f| !Arc::ptr_eq(&f.live, live));
                if v.is_empty() {
                    inflight.remove(&job.policy);
                }
            }
        }
        if job.kind == JobKind::Walk {
            let mut walks = self.walks.lock().expect("walks");
            if let Some(state) = walks.0.get_mut(&job.policy) {
                state.in_flight.retain(|j| j.range != job.range);
                if let Some(o) = out {
                    state.written += o.written;
                    state.backfilled += o.backfilled;
                }
            }
            drop(walks);
            // ALL, not one: several walk workers may be waiting, and after
            // this range lands the frontier has moved for every one of them.
            self.walk_wake.notify_all();
        }
    }

    /// A walk job read a chunk. Fold it into the policy's one report — the
    /// descent a reader sees is all of its jobs together, not whichever
    /// thread spoke last.
    fn walk_progress(
        &self,
        policy: &str,
        range: SlotRange,
        p: &reverse::Progress<'_>,
        top: u64,
        mint: u64,
    ) -> Option<PolicyJob> {
        let mut walks = self.walks.lock().expect("walks");
        let state = walks.0.get_mut(policy)?;
        let job = state.in_flight.iter_mut().find(|j| j.range == range)?;
        job.floor = p.floor;
        job.chunks_done = p.chunks_done;
        job.chunks_total = p.chunks_total;
        job.written = p.written;
        job.pending = p.pending;
        let updated = p.updated.iter().map(|h| hex::encode(h.as_ref())).collect();
        state.report(top, mint, updated)
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

/// The next stretch of a policy's descent: [`JOB_CHUNKS`] chunks below the
/// FRONTIER — the bottom of the topmost stretch that is read or being read
/// — down to the mint. `None` when the frontier has reached it.
///
/// `in_flight` is what this policy's other walk jobs have claimed. Counting
/// it is the whole of "several jobs per policy": the ranges are disjoint by
/// construction, so no two jobs write the same transaction and the manifest
/// needs no arbitration beyond its landing lock.
///
/// A stretch below a hole — a seek's window — is deliberately NOT the
/// frontier. The descent walks down to it and the merge closes the hole;
/// the blocks it has already read are scanned for resolution only
/// (`reverse::Scan::covered`).
fn walk_job(
    hub: &PolicyHub,
    policy: &str,
    first_mint: Option<u64>,
    in_flight: &[SlotRange],
) -> Option<Job> {
    let manifest = archive::load_manifest(&hub.policy_dir(policy))
        .ok()
        .flatten();
    let first_mint = manifest
        .as_ref()
        .and_then(|m| m.first_mint_slot)
        .or(first_mint);
    // IMMUTABLE ranges: the volatile tail sits ABOVE the immutable tip, so
    // counting it would put the frontier at the tip's own slot and the
    // descent would never generate the job below it.
    let mut claimed: Vec<SlotRange> = manifest
        .as_ref()
        .map_or_else(Vec::new, |m| m.immutable_ranges());
    claimed.extend_from_slice(in_flight);
    Some(Job {
        policy: policy.to_string(),
        kind: JobKind::Walk,
        range: next_range(claimed, hub.tip_slot(), first_mint.unwrap_or(0))?,
        first_mint,
    })
}

/// The arithmetic of the frontier, on its own: given every stretch read or
/// being read, the snapshot's tip and the policy's floor, the next range to
/// hand a walk worker.
///
/// TOP-UPS COME FIRST. A daily snapshot refresh puts new chunks above
/// everything the archive has read, and those are the NEWEST rows — the ones
/// a reader is most likely to be looking at — so they are read before the
/// descent goes another step deeper. A top-up depends on inputs resolving
/// inside the job, because its spenders' sources are in the stretch below it
/// that the job never rescans.
fn next_range(claimed: Vec<SlotRange>, tip: u64, bound: u64) -> Option<SlotRange> {
    let reach = JOB_CHUNKS * CHUNK_SLOTS;
    let Some(top) = policy_archive::merge_ranges(claimed).pop() else {
        // Cold: a policy's first job is the newest stretch below the tip.
        return (tip > bound).then(|| SlotRange::new(bound.max(tip.saturating_sub(reach)), tip));
    };
    if top.to < tip {
        return Some(SlotRange::new(top.to, tip.min(top.to + reach)));
    }
    if top.from <= bound {
        return None;
    }
    Some(SlotRange::new(
        bound.max(top.from.saturating_sub(reach)),
        top.from,
    ))
}

// ── workers ──────────────────────────────────────────────────────────────

/// How often the sweeper looks for a snapshot refresh. A directory listing
/// and a manifest per policy; the refresh itself is daily, so this only has
/// to be small against a day.
const SWEEP_SECS: u64 = 600;

/// Watch for the snapshot growing, and give every archive the top-up it now
/// wants.
///
/// A refresh raises `tip_slot()` and every policy's settled coverage stops
/// where it did — so without this the archives quietly stay a day behind
/// the chunks on disk forever, and only the volatile tail moves. Nothing
/// here decides HOW to top up: it asks for a walk, and `walk_job`'s frontier
/// arithmetic emits the range above the top stretch.
///
/// Idempotent by construction. A policy already at the tip is not asked for
/// (a walk that finishes with nothing to do would report `Done` and
/// republish on every tick), and a policy already walking joins the walk it
/// already has.
fn sweeper(hub: Arc<PolicyHub>) {
    loop {
        std::thread::sleep(std::time::Duration::from_secs(SWEEP_SECS));
        let tip = hub.tip_slot();
        for policy in hub.archived_policies() {
            if hub.sched.walking(&policy) {
                continue;
            }
            let behind = archive::load_manifest(&hub.policy_dir(&policy))
                .ok()
                .flatten()
                .and_then(|m| m.immutable_walk_to())
                .is_some_and(|to| to < tip);
            if behind && hub.sched.want_walk(&policy, None) {
                tracing::info!(policy, tip, "policy: topping up after a snapshot refresh");
            }
        }
    }
}

/// Start the pool. Each worker owns a runtime and a publisher, so a landed
/// archive is published from the thread that landed it.
pub fn spawn(hub: Arc<PolicyHub>, pool: Pool, publish: Option<crate::publish::Targets>) {
    let sweep = Arc::clone(&hub);
    std::thread::Builder::new()
        .name("sweep".into())
        .spawn(move || sweeper(sweep))
        .expect("spawn sweeper");
    for n in 0..pool.walk_workers {
        let hub = Arc::clone(&hub);
        let publish = publish.clone();
        std::thread::Builder::new()
            .name(format!("walk-{n}"))
            .spawn(move || worker(hub, publish, JobKind::Walk))
            .expect("spawn walk worker");
    }
    for n in 0..pool.seek_workers {
        let hub = Arc::clone(&hub);
        let publish = publish.clone();
        std::thread::Builder::new()
            .name(format!("seek-{n}"))
            .spawn(move || worker(hub, publish, JobKind::Seek))
            .expect("spawn seek worker");
    }
    tracing::info!(
        walk = pool.walk_workers,
        seek = pool.seek_workers,
        "policy: pool up"
    );
}

pub(crate) struct Publishing {
    runtime: tokio::runtime::Runtime,
    publisher: Option<crate::publish::Publisher>,
}

impl Publishing {
    pub(crate) fn new(targets: Option<crate::publish::Targets>) -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("publish runtime");
        let publisher = targets.and_then(|t| match crate::publish::Publisher::new(t) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(error = %format!("{e:#}"), "policy: publisher not built — archives stay local");
                None
            }
        });
        Self { runtime, publisher }
    }

    pub(crate) fn publish(&self, dir: PathBuf, policy: &str) {
        let Some(p) = &self.publisher else {
            return;
        };
        match self.runtime.block_on(p.publish(&dir, policy)) {
            Ok(rec) if rec.all_done() => {
                tracing::info!(policy, outcome = %rec.summary(), "policy: archive published")
            }
            Ok(rec) => {
                tracing::warn!(policy, outcome = %rec.summary(), "policy: publish INCOMPLETE")
            }
            Err(e) => {
                tracing::warn!(policy, error = %format!("{e:#}"), "policy: publish failed")
            }
        }
    }
}

fn worker(hub: Arc<PolicyHub>, publish: Option<crate::publish::Targets>, kind: JobKind) {
    let publishing = Publishing::new(publish);
    loop {
        let job = match kind {
            JobKind::Seek => hub.sched.next_seek(),
            JobKind::Walk => match hub.sched.next_walk(&hub) {
                Next::Job(job) => job,
                Next::Finished {
                    policy,
                    written,
                    backfilled,
                    secs,
                } => {
                    let unresolved = archive::load_manifest(&hub.policy_dir(&policy))
                        .ok()
                        .flatten()
                        .and_then(|m| m.latest_pass().map(|p| p.pending))
                        .unwrap_or(0);
                    hub.set(
                        &policy,
                        PolicyJob::Done {
                            written,
                            backfilled: backfilled as usize,
                            unresolved: unresolved as usize,
                            secs,
                        },
                    );
                    tracing::info!(
                        policy,
                        written,
                        secs = format!("{secs:.1}"),
                        "policy: walk complete"
                    );
                    if !hub.sched.busy(&policy) {
                        // THE MOVEMENT GRAPH, before the publish that would
                        // carry it. Here rather than on every landing: it is
                        // a full read of the archive (5.9 s on ClayNation),
                        // and a walk completes once — including once per
                        // daily top-up, which is exactly the cadence the
                        // artifact wants. A failure is logged and never
                        // blocks the archive itself.
                        if let Err(e) = crate::archive::write_graph(
                            &hub.policy_dir(&policy),
                            &policy,
                            GRAPH_BY_STAKE,
                        ) {
                            tracing::warn!(policy, error = %format!("{e:#}"), "policy: movement graph not built");
                        }
                        publishing.publish(hub.policy_dir(&policy), &policy);
                    }
                    continue;
                }
            },
        };
        let (live, seq) = hub.sched.job_started(&hub, &job);
        let outcome = run_job(&hub, &job, &live, seq);
        match &outcome {
            Ok(_) => {}
            Err(e) => {
                tracing::error!(policy = job.policy, kind = ?job.kind, from = job.range.from, to = job.range.to, error = %format!("{e:#}"), "policy: job failed");
                if job.kind == JobKind::Walk {
                    hub.set(
                        &job.policy,
                        PolicyJob::Failed {
                            error: format!("{e:#}"),
                        },
                    );
                    // Take the policy off the table: a failing descent
                    // would otherwise be retried forever.
                    hub.sched.walks.lock().expect("walks").0.remove(&job.policy);
                }
            }
        }
        hub.sched.job_finished(&job, &live, outcome.as_ref().ok());
        // A seek that landed on a policy nobody is walking is the only
        // reason a seek worker publishes.
        if job.kind == JobKind::Seek && outcome.is_ok() && !hub.sched.busy(&job.policy) {
            publishing.publish(hub.policy_dir(&job.policy), &job.policy);
        }
    }
}

fn run_job(
    hub: &PolicyHub,
    job: &Job,
    live: &Arc<Mutex<Live>>,
    seq: u32,
) -> Result<reverse::Outcome> {
    let args = reverse::ReverseArgs {
        data_dir: hub.data_dir.clone(),
        tokens: hub.tokens.clone(),
        token: job.policy.clone(),
        archive_dir: hub.archive_dir.clone(),
        days: None,
        to_slot: Some(job.range.from),
        from_slot: Some(job.range.to),
        first_mint: job.first_mint,
        // The hosted surface learns the mint from its CALLER (`?to_slot=`),
        // so a job that arrived without one has no floor and would read to
        // genesis. Probing per JOB would be one Koios call per job on a
        // policy walked by four workers; it belongs once per policy, at
        // admission, and `want_walk` is where that would go.
        probe_first_mint: false,
        seek: job.kind == JobKind::Seek,
        // The serve's index is already open and hot-swappable; the CLI flag
        // is for running the same job by hand.
        tx_index_dir: None,
        no_compact: false,
        no_sieve: false,
        // The hosted surface observes. Ingestion speed matters most on a cold
        // policy a reader is waiting for, and that is the FIRST job's ten-day
        // window, not the descent behind it — so this is the place to revisit
        // if the measurement says the tier costs a reader anything.
        no_observe: false,
        report_every: u64::MAX,
    };
    let pass_dir = hub
        .policy_dir(&job.policy)
        .join(archive::PassEntry::dir_name(seq));
    // The whole walk's frame, for the fraction a reader sees: from the
    // top of the archive down to the mint, not this job's forty chunks.
    let (top, mint) = {
        let m = archive::load_manifest(&hub.policy_dir(&job.policy))
            .ok()
            .flatten();
        (
            m.as_ref()
                .and_then(|m| m.walk_to())
                .unwrap_or(job.range.to)
                .max(job.range.to),
            job.first_mint.unwrap_or(0),
        )
    };
    let index = hub.index.as_ref().map(|h| {
        if let Err(e) = h.reload_if_changed() {
            tracing::warn!(error = %format!("{e:#}"), "policy: tx-index reload failed; using the mapping in hand");
        }
        h.get()
    });
    let on: reverse::OnProgress<'_> = &|p| {
        // The live tier's lock is released before the table's: `view` holds
        // several live guards at once, and taking the two in one order
        // everywhere is what keeps that from deadlocking.
        {
            let mut l = live.lock().expect("live");
            l.floor = p.floor;
            l.ceiling = p.ceiling;
            l.pending = p.pending;
            l.publish(&pass_dir, p.rows, p.flushed);
        }
        if job.kind == JobKind::Walk
            && let Some(state) = hub
                .sched
                .walk_progress(&job.policy, job.range, &p, top, mint)
        {
            hub.set(&job.policy, state);
        }
    };
    let lock = hub.sched.lock_for(&job.policy);
    reverse::run_reporting(
        args,
        reverse::Hooks {
            on,
            resolver: index.as_deref(),
            land_lock: Some(&lock),
            seq: Some(seq),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const K: u64 = JOB_CHUNKS * CHUNK_SLOTS;

    /// A COLD policy starts at the snapshot's tip; each job in flight moves
    /// the frontier down by its own range, so four workers get four
    /// disjoint stretches without any of them having landed.
    #[test]
    fn jobs_stack_below_each_other_without_waiting_to_land() {
        let tip = 100 * K;
        let mut claimed: Vec<SlotRange> = Vec::new();
        let mut handed = Vec::new();
        for _ in 0..4 {
            let r = next_range(claimed.clone(), tip, 0).expect("a range");
            claimed.push(r);
            handed.push(r);
        }
        assert_eq!(
            handed,
            vec![
                SlotRange::new(99 * K, 100 * K),
                SlotRange::new(98 * K, 99 * K),
                SlotRange::new(97 * K, 98 * K),
                SlotRange::new(96 * K, 97 * K),
            ]
        );
        // Disjoint by construction — no two jobs write one transaction.
        assert_eq!(policy_archive::merge_ranges(claimed).len(), 1);
    }

    /// A stretch below a HOLE — a seek's window — is not the frontier. The
    /// descent keeps going down toward it and the merge closes the hole.
    #[test]
    fn a_seeks_window_below_a_hole_is_not_the_frontier() {
        let tip = 100 * K;
        let claimed = vec![SlotRange::new(90 * K, tip), SlotRange::new(50 * K, 51 * K)];
        assert_eq!(
            next_range(claimed, tip, 0),
            Some(SlotRange::new(89 * K, 90 * K)),
            "below the TOP stretch, not below the seek window"
        );
    }

    /// AFTER A SNAPSHOT REFRESH the archive is a day short at the TOP, and
    /// that gap is read before the descent takes another step down — those
    /// are the newest rows, which is what a reader is looking at. Once it
    /// is read the descent resumes where it was.
    #[test]
    fn a_snapshot_refresh_is_topped_up_before_the_descent_continues() {
        let was = 100 * K;
        let now = was + 8_640; // a day of new chunks
        let read = SlotRange::new(60 * K, was);
        assert_eq!(
            next_range(vec![read], now, 0),
            Some(SlotRange::new(was, now)),
            "the new stretch, above everything read"
        );
        // Landed: the descent picks up below the frontier again.
        assert_eq!(
            next_range(vec![read, SlotRange::new(was, now)], now, 0),
            Some(SlotRange::new(59 * K, 60 * K))
        );
    }

    /// A top-up larger than one job is bounded like any other job, and the
    /// next one continues from where it stopped.
    #[test]
    fn a_long_top_up_is_split_into_jobs() {
        let was = 100 * K;
        let now = was + 3 * K;
        let read = SlotRange::new(60 * K, was);
        let first = next_range(vec![read], now, 0).expect("a top-up");
        assert_eq!(first, SlotRange::new(was, was + K));
        assert_eq!(
            next_range(vec![read, first], now, 0),
            Some(SlotRange::new(was + K, was + 2 * K))
        );
    }

    /// The mint is the floor: the last job stops at it, and after that
    /// there is nothing to generate.
    #[test]
    fn the_last_job_stops_at_the_mint_and_then_the_walk_is_over() {
        let tip = 100 * K;
        let mint = 99 * K + 7;
        let last =
            next_range(vec![SlotRange::new(99 * K + 500, tip)], tip, mint).expect("one job left");
        assert_eq!(last, SlotRange::new(mint, 99 * K + 500));
        assert_eq!(
            next_range(vec![SlotRange::new(mint, tip)], tip, mint),
            None,
            "the frontier is the mint"
        );
    }
}
