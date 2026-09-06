//! The QUERY-shaped read surface for a policy feed — wallet-sieve's serve
//! pattern, pointed at a policy instead of a wallet.
//!
//! # Why not the artifact path next door
//!
//! [`crate::serve`] answers `/api/token/{unit}` by building `spine.bin.gz` and
//! pushing it to R2, and a client fetches a static file. That is right for a
//! finished token's supply curve and wrong for a feed, for three reasons:
//!
//! - a reverse pass **corrects rows it has already published**, and an
//!   immutable artifact cannot express that;
//! - a feed is paged and windowed per reader, which is a query, not a file;
//! - coverage and progress change every chunk, and a consumer needs to watch
//!   them advance rather than poll for a finished object.
//!
//! So this sits ALONGSIDE that path rather than replacing it — token-explorer
//! depends on the artifacts and is untouched.
//!
//! # The shape is deliberately wallet-sieve's
//!
//! | wallet-sieve | here |
//! | --- | --- |
//! | `GET /flows/{target}` | `GET /policy/{policy}` |
//! | `GET /flows/{target}/tx/{hash}` | `GET /policy/{policy}/tx/{hash}` |
//! | `POST /flows/{target}/refresh` | `POST /policy/{policy}/refresh` |
//! | `GET /flows/{target}/events` | `GET /policy/{policy}/events` |
//!
//! Not for tidiness: `flow-explorer`'s `sieve.rs` already speaks this
//! vocabulary — job state, coverage floor, `history_pending` — so the worker
//! side is a near-copy against a different base URL rather than a new protocol.
//!
//! # Two rules carried over verbatim
//!
//! **A read never starts a walk.** `GET` answers from what is on disk. Only
//! `POST /refresh` queues work, and it is one pass at a time. The card path in
//! particular is reached by unauthenticated link previews on the consumer side,
//! and a read that queued a 200 GB sweep would be a denial-of-service surface.
//!
//! **Direction is derived here, never stored.** `delta` is signed and
//! undirected on purpose (see [`crate::store`]'s header); this layer turns it
//! into a feed row and is allowed to answer "ambiguous".

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use anyhow::Result;
use axum::extract::{Path as AxPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::ReceiverStream;

use policy_archive::Movement;

use crate::archive::{self, PolicyArchive};
use crate::reverse;

/// How a reverse pass for one policy is going.
///
/// `Running` carries real progress rather than a phase string: a policy pass
/// can run for minutes and the consumer is driving a progress bar, not a
/// spinner. The figures come straight from `reverse::Progress`.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum PolicyJob {
    Queued,
    Running {
        /// 0.0–1.0 through the requested range.
        fraction: f64,
        /// Lowest slot reached so far, and its date — "reaching back to…".
        floor: u64,
        date: String,
        chunks_done: u64,
        chunks_total: u64,
        written: u64,
        /// Transactions corrected in the most recent chunk. The consumer
        /// re-reads exactly these.
        updated: Vec<String>,
        unresolved: usize,
    },
    Done {
        written: u64,
        backfilled: usize,
        unresolved: usize,
        secs: f64,
    },
    Failed {
        error: String,
    },
}

impl PolicyJob {
    pub fn is_terminal(&self) -> bool {
        matches!(self, PolicyJob::Done { .. } | PolicyJob::Failed { .. })
    }
}

/// A queued reverse pass.
struct PassRequest {
    policy: String,
    /// Stop here — the policy's first mint, when the caller knows it. `None`
    /// walks to the registry's floor, or genesis.
    to_slot: Option<u64>,
}

/// THE LOWER LOD — what a running pass has walked so far.
///
/// A pass spills its rows to disk as SEGMENTS while it walks
/// (`crate::segments`), in the archive's own format, so most of what it has
/// found is already servable through the same reader as the archive. What
/// is not on disk yet — at most one segment's worth — is mirrored here from
/// `Progress::rows` and cleared on `Progress::flushed`. Reads open the
/// archive WITH the pass's segments and fold the buffer on top: the same
/// shape at a lower level of durability, and a reader scrubbing into the
/// stretch being walked sees what has been walked so far.
///
/// `seq` is the manifest sequence the pass will write. A reader checks the
/// manifest for it: present means the pass landed, its segments have been
/// compacted into the archive's files, and this is stale — which closes the
/// race between the manifest rename and this being cleared, without a lock
/// across two structures.
pub struct Live {
    pub seq: u32,
    pub floor: u64,
    pub ceiling: u64,
    pub pending: usize,
    /// Segments flushed so far, in order, servable now.
    pub segments: Vec<(PathBuf, archive::FileKind)>,
    /// Rows not yet in a segment.
    pub buffer: Vec<Movement>,
    /// A detour in progress — the seek it is reaching for.
    pub reaching: Option<u64>,
    /// Windows below `floor` that detours have read, `(from, to)`.
    pub detours: Vec<(u64, u64)>,
}

impl Live {
    fn new(seq: u32) -> Self {
        Self {
            seq,
            floor: u64::MAX,
            ceiling: 0,
            pending: 0,
            segments: Vec::new(),
            buffer: Vec::new(),
            reaching: None,
            detours: Vec::new(),
        }
    }

    /// Would a detour ending at `at` re-read a stretch already read? `to`
    /// inclusive: a seek asks for rows BELOW its slot, and a window read
    /// up to that slot has them.
    fn detoured(&self, at: u64) -> bool {
        self.detours
            .iter()
            .any(|(from, to)| at > *from && at <= *to)
    }

    /// Take a chunk's rows — or, if the pass flushed at the end of it, the
    /// segments that now hold everything buffered so far.
    fn publish(&mut self, pass_dir: &Path, rows: &[Movement], flushed: &[archive::FileEntry]) {
        if flushed.is_empty() {
            self.buffer.extend_from_slice(rows);
            return;
        }
        for f in flushed {
            self.segments
                .push((pass_dir.join(&f.file), crate::segments::kind_of(&f.file)));
        }
        self.buffer.clear();
    }

    /// Is this row one of the pass's OWN transactions, as against a
    /// correction to one an earlier pass archived?
    fn own(&self, m: &Movement) -> bool {
        m.slot < self.ceiling || self.ceiling == 0
    }

    /// Transactions in the buffer this pass FOUND — the segments' are counted
    /// by their footers.
    fn buffered_txs(&self) -> u64 {
        self.buffer
            .iter()
            .filter(|m| self.own(m))
            .map(|m| m.tx_hash.as_slice())
            .collect::<HashSet<_>>()
            .len() as u64
    }

    fn buffered_slots(&self) -> (Option<u64>, Option<u64>) {
        (
            self.buffer.iter().map(|m| m.slot).min(),
            self.buffer.iter().map(|m| m.slot).max(),
        )
    }

    /// The buffer's histogram: transactions from the pass's own rows,
    /// movements from all of them.
    fn buffered_density(&self, bucket_secs: u64) -> Vec<policy_archive::DensityBucket> {
        let own: Vec<Movement> = self
            .buffer
            .iter()
            .filter(|m| self.own(m))
            .cloned()
            .collect();
        let mut out = crate::archive::density_of(&own, bucket_secs);
        let width = bucket_secs.max(1);
        for m in self
            .buffer
            .iter()
            .filter(|m| !self.own(m) && !m.is_placeholder())
        {
            let from = m.block_time / width * width;
            match out.iter_mut().find(|b| b.from_unix == from) {
                Some(b) => b.movements += 1,
                None => out.push(policy_archive::DensityBucket {
                    from_unix: from,
                    to_unix: from + width,
                    movements: 1,
                    ..policy_archive::DensityBucket::default()
                }),
            }
        }
        out.sort_by_key(|b| b.from_unix);
        out
    }

    /// The rows behind a page: the newest `limit` transactions below
    /// `before`, with every row of theirs — corrections included. Two passes
    /// over the rows and no clone of the rest, because this runs on every
    /// 2-second poll while a pass holds hundreds of thousands of rows.
    ///
    /// Candidates are this pass's OWN transactions — rows below its ceiling.
    /// Rows above it are corrections to transactions an earlier pass
    /// archived; those ride along only when their transaction is in
    /// `archived`, the page the archive just produced, so a correction the
    /// pass found lands on the archived row it corrects.
    fn page(&self, limit: usize, before: u64, archived: &HashSet<Vec<u8>>) -> Vec<Movement> {
        use std::collections::BTreeSet;
        let mut top: BTreeSet<(u64, &[u8])> = BTreeSet::new();
        for m in self
            .buffer
            .iter()
            .filter(|m| self.own(m) && m.slot < before)
        {
            let key = (m.slot, m.tx_hash.as_slice());
            if top.len() < limit {
                top.insert(key);
            } else if top.first().is_some_and(|min| key > *min) {
                top.pop_first();
                top.insert(key);
            }
        }
        let wanted: HashSet<&[u8]> = top.iter().map(|(_, h)| *h).collect();
        self.buffer
            .iter()
            .filter(|m| wanted.contains(m.tx_hash.as_slice()) || archived.contains(&m.tx_hash))
            .cloned()
            .collect()
    }
}

/// What a read sees: the archive — opened WITH a running pass's segments —
/// and the pass's buffer, or neither for a policy nobody has touched.
struct View<'a> {
    archive: Option<PolicyArchive>,
    live: Option<std::sync::MutexGuard<'a, Live>>,
}

fn view<'a>(hub: &PolicyHub, policy: &str, live: &'a Option<Arc<Mutex<Live>>>) -> Result<View<'a>> {
    let dir = hub.policy_dir(policy);
    let guard = live.as_ref().map(|l| l.lock().expect("live"));
    // Landed? Then the segments are compacted away and the buffer is stale;
    // the archive alone is the truth.
    let manifest = archive::load_manifest(&dir)?;
    let active = guard
        .as_deref()
        .is_some_and(|l| !manifest.is_some_and(|m| m.passes.iter().any(|p| p.seq == l.seq)));
    let segments: Vec<(PathBuf, archive::FileKind)> = match (active, guard.as_deref()) {
        (true, Some(l)) => l.segments.clone(),
        _ => Vec::new(),
    };
    let archive = PolicyArchive::open_with(&dir, &segments)?;
    Ok(View {
        archive,
        live: if active { guard } else { None },
    })
}

pub struct PolicyHub {
    pub data_dir: PathBuf,
    pub tokens: PathBuf,
    /// Root of the per-policy archives — Parquet plus a manifest, no
    /// database. See `archive.rs`.
    pub archive_dir: PathBuf,
    jobs: Mutex<HashMap<String, PolicyJob>>,
    /// The in-progress pass per policy, if one is running.
    live: Mutex<HashMap<String, Arc<Mutex<Live>>>>,
    /// The newest seek below the floor per policy, waiting for the running
    /// pass to take it between chunks. Newest wins: a reader dragging the
    /// spine sends several and only the last one matters.
    detours: Mutex<HashMap<String, u64>>,
    /// tx hash → body over the snapshot, for resolving a detour's inputs on
    /// the spot. `None` without `--tx-index-dir`.
    index: Option<tx_index::IndexHandle>,
    /// Passes queued and not yet started, with their floors — what the
    /// running pass looks at between chunks to give a waiting policy a
    /// PREVIEW. Removed when the pass starts.
    queued: Mutex<HashMap<String, Option<u64>>>,
    /// Policies that have had their preview this time round, so a long
    /// queue does not preview the same one on every chunk.
    previewed: Mutex<HashSet<String>>,
    queue: mpsc::Sender<PassRequest>,
    bearer: Option<String>,
}

impl PolicyHub {
    /// `publish`: where a landed archive goes — R2 and the KV bundle — or
    /// `None` to leave it on disk. See `publish.rs`. `tx_index_dir`: a
    /// tx-index over the same snapshot, for detours.
    pub fn new(
        data_dir: PathBuf,
        tokens: PathBuf,
        archive_dir: PathBuf,
        publish: Option<crate::publish::Targets>,
        tx_index_dir: Option<PathBuf>,
        bearer: Option<String>,
    ) -> Arc<Self> {
        let (tx, rx) = mpsc::channel::<PassRequest>();
        let index = tx_index_dir.and_then(|dir| {
            match tx_index::IndexHandle::open(&dir, &data_dir.join("immutable")) {
                Ok(h) => {
                    let cov = h.get().coverage();
                    tracing::info!(
                        dir = %dir.display(),
                        base_entries = cov.base_entries,
                        newest_chunk = ?cov.newest_chunk,
                        "policy: tx-index open — detours resolve inputs on the spot"
                    );
                    Some(h)
                }
                Err(e) => {
                    tracing::warn!(dir = %dir.display(), error = %format!("{e:#}"), "policy: tx-index NOT open — detours leave inputs to the descent");
                    None
                }
            }
        });
        let hub = Arc::new(Self {
            data_dir,
            tokens,
            archive_dir,
            jobs: Mutex::new(HashMap::new()),
            live: Mutex::new(HashMap::new()),
            detours: Mutex::new(HashMap::new()),
            index,
            queued: Mutex::new(HashMap::new()),
            previewed: Mutex::new(HashSet::new()),
            queue: tx,
            bearer,
        });
        // ONE worker, for the reason the artifact builder next door has one:
        // passes are disk-bound against a shared snapshot and two interleaving
        // would slow both. A queued request says so rather than starting.
        {
            let hub = Arc::clone(&hub);
            std::thread::spawn(move || {
                // The publisher and its runtime live on THIS thread for the
                // life of the loop: its HTTP clients are used from the one
                // runtime that created their connections.
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("publish runtime");
                let publisher = publish.and_then(|targets| {
                    tracing::info!(targets = %targets.describe(), "policy: publishing landed archives");
                    match crate::publish::Publisher::new(targets) {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::error!(error = %format!("{e:#}"), "policy: publisher not built — archives stay local");
                            None
                        }
                    }
                });
                for req in rx {
                    let started = Instant::now();
                    hub.queued.lock().expect("queued").remove(&req.policy);
                    let outcome = run_pass(&hub, &req, Depth::Full);
                    // The next time this policy queues, it may be previewed
                    // again — its archive will say whether that is needed.
                    hub.previewed.lock().expect("previewed").remove(&req.policy);
                    let state = match outcome {
                        Ok(out) => PolicyJob::Done {
                            written: out.written,
                            backfilled: out.backfilled as usize,
                            unresolved: out.unresolved as usize,
                            secs: started.elapsed().as_secs_f64(),
                        },
                        Err(e) => {
                            tracing::error!(
                                policy = req.policy,
                                error = %format!("{e:#}"),
                                "policy: pass failed"
                            );
                            PolicyJob::Failed {
                                error: format!("{e:#}"),
                            }
                        }
                    };
                    // The archive holds it now — or, on failure, nobody does.
                    hub.live.lock().expect("live").remove(&req.policy);
                    let landed = matches!(state, PolicyJob::Done { .. });
                    hub.set(&req.policy, state);
                    // PUBLISH. Synchronous on this thread, deliberately: the
                    // next pass would otherwise rewrite the directory under
                    // a publish still reading it. A failed publish is
                    // recorded (`published.json`) and logged, and never
                    // fails the pass — the archive is on disk either way
                    // and the next landing publishes again.
                    if landed && let Some(p) = &publisher {
                        let dir = hub.policy_dir(&req.policy);
                        match runtime.block_on(p.publish(&dir, &req.policy)) {
                            Ok(rec) if rec.all_done() => {
                                tracing::info!(policy = req.policy, outcome = %rec.summary(), "policy: archive published")
                            }
                            Ok(rec) => {
                                tracing::warn!(policy = req.policy, outcome = %rec.summary(), "policy: publish INCOMPLETE")
                            }
                            Err(e) => {
                                tracing::warn!(policy = req.policy, error = %format!("{e:#}"), "policy: publish failed")
                            }
                        }
                    }
                }
            });
        }
        hub
    }

    /// The running pass's in-memory rows for a policy, if any.
    fn live_for(&self, policy: &str) -> Option<Arc<Mutex<Live>>> {
        self.live.lock().expect("live").get(policy).cloned()
    }

    fn set(&self, policy: &str, state: PolicyJob) {
        self.jobs
            .lock()
            .expect("jobs")
            .insert(policy.to_string(), state);
    }

    fn get(&self, policy: &str) -> Option<PolicyJob> {
        self.jobs.lock().expect("jobs").get(policy).cloned()
    }

    /// The policy's archive directory.
    fn policy_dir(&self, policy: &str) -> PathBuf {
        crate::archive::policy_dir(&self.archive_dir, policy)
    }

    /// Is a pass queued or running for this policy?
    fn walking(&self, policy: &str) -> bool {
        self.get(policy).is_some_and(|j| !j.is_terminal())
    }

    /// Ask the running pass to read the stretch ending at `at` next.
    /// Newest wins; a stretch already read, or one the descent has already
    /// passed, is declined with the reason.
    fn request_detour(&self, policy: &str, at: u64) -> DetourResponse {
        let Some(live) = self.live_for(policy) else {
            return DetourResponse {
                accepted: false,
                because: Some("not_walking"),
                reaching: None,
            };
        };
        let (floor, reaching, read) = {
            let l = live.lock().expect("live");
            (l.floor, l.reaching, l.detoured(at))
        };
        if read {
            return DetourResponse {
                accepted: false,
                because: Some("already_read"),
                reaching,
            };
        }
        if at >= floor {
            return DetourResponse {
                accepted: false,
                because: Some("above_floor"),
                reaching,
            };
        }
        self.detours
            .lock()
            .expect("detours")
            .insert(policy.to_string(), at);
        DetourResponse {
            accepted: true,
            because: None,
            reaching: Some(at),
        }
    }

    /// The pass's side: the newest seek for this policy, taken once.
    fn take_detour(&self, policy: &str) -> Option<u64> {
        self.detours.lock().expect("detours").remove(policy)
    }

    /// Between chunks of `running`: give ONE policy waiting behind it a
    /// preview — a short pass over its newest days, landed as its first
    /// pass — so its page shows rows and a spine while it waits. Only a
    /// policy nobody has walked; one that already has an archive has rows
    /// to show. The full pass, when its turn comes, continues downward from
    /// the preview's floor, so nothing is read twice.
    fn preview_queued(&self, running: &str) {
        let next = {
            let queued = self.queued.lock().expect("queued");
            let previewed = self.previewed.lock().expect("previewed");
            queued
                .iter()
                .find(|(p, _)| p.as_str() != running && !previewed.contains(*p))
                .map(|(p, floor)| (p.clone(), *floor))
        };
        let Some((policy, to_slot)) = next else {
            return;
        };
        self.previewed
            .lock()
            .expect("previewed")
            .insert(policy.clone());
        let cold = match crate::archive::load_manifest(&self.policy_dir(&policy)) {
            Ok(m) => m.is_none(),
            Err(e) => {
                tracing::warn!(policy, error = %format!("{e:#}"), "policy: preview skipped — manifest unreadable");
                return;
            }
        };
        if !cold {
            return;
        }
        let started = Instant::now();
        let req = PassRequest { policy, to_slot };
        match run_pass(self, &req, Depth::Preview) {
            Ok(out) => tracing::info!(
                policy = req.policy,
                written = out.written,
                secs = format!("{:.1}", started.elapsed().as_secs_f64()),
                "policy: preview landed — the full pass is still queued"
            ),
            Err(e) => {
                tracing::warn!(policy = req.policy, error = %format!("{e:#}"), "policy: preview failed")
            }
        }
        self.live.lock().expect("live").remove(&req.policy);
        // Still waiting for its full pass: the preview did not change that,
        // and the page must keep saying so.
        self.set(&req.policy, PolicyJob::Queued);
    }

    fn authorised(&self, headers: &HeaderMap) -> bool {
        let Some(expected) = &self.bearer else {
            return true;
        };
        headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .is_some_and(|t| t == expected)
    }
}

/// How far a pass goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Depth {
    /// ALL THE WAY. Tiers gate what a reader SEES, never how far the walk
    /// goes — one walk per policy, shared by everyone.
    Full,
    /// The newest [`PREVIEW_DAYS`], for a policy waiting behind another
    /// pass. Lands as an ordinary first pass; the full one continues from
    /// its floor.
    Preview,
}

/// A preview's reach. About forty chunks — seconds of reading through the
/// sieve gate — and enough of a spine to scrub.
const PREVIEW_DAYS: u64 = 10;

fn run_pass(hub: &PolicyHub, req: &PassRequest, depth: Depth) -> Result<reverse::Outcome> {
    let args = reverse::ReverseArgs {
        data_dir: hub.data_dir.clone(),
        tokens: hub.tokens.clone(),
        token: req.policy.clone(),
        archive_dir: hub.archive_dir.clone(),
        days: match depth {
            Depth::Full => None,
            Depth::Preview => Some(PREVIEW_DAYS),
        },
        // The mint floor holds for a preview too: a policy minted last
        // week previews to completion and the full pass finds nothing left.
        to_slot: req.to_slot,
        // From the bottom of the top stretch, as the descent always has.
        from_slot: None,
        no_compact: false,
        no_sieve: false,
        report_every: u64::MAX,
    };
    // The sequence this pass will write, read before it starts, so a reader
    // can tell "landed" from "still in memory" by looking at the manifest.
    let seq =
        crate::archive::load_manifest(&hub.policy_dir(&req.policy))?.map_or(0, |m| m.next_seq());
    let live = Arc::new(Mutex::new(Live::new(seq)));
    hub.live
        .lock()
        .expect("live")
        .insert(req.policy.clone(), Arc::clone(&live));

    let policy = req.policy.clone();
    let pass_dir = hub
        .policy_dir(&req.policy)
        .join(archive::PassEntry::dir_name(seq));
    let hub_ref = hub;
    // The index, as it stands when the pass starts. A refresh that lands
    // mid-pass is picked up by the next one; a lookup that misses falls to
    // the descent either way.
    let index = hub.index.as_ref().map(|h| {
        if let Err(e) = h.reload_if_changed() {
            tracing::warn!(error = %format!("{e:#}"), "policy: tx-index reload failed; using the mapping in hand");
        }
        h.get()
    });
    let on: reverse::OnProgress<'_> = &|p| {
        {
            let mut l = live.lock().expect("live");
            l.floor = p.floor;
            l.ceiling = p.ceiling;
            l.pending = p.pending;
            l.reaching = p.reaching;
            l.detours = p.detours.to_vec();
            l.publish(&pass_dir, p.rows, p.flushed);
        }
        hub_ref.set(
            &policy,
            PolicyJob::Running {
                fraction: p.fraction(),
                floor: p.floor,
                date: reverse::slot_date(p.floor),
                chunks_done: p.chunks_done,
                chunks_total: p.chunks_total,
                written: p.written,
                updated: p.updated.iter().map(|h| hex::encode(h.as_ref())).collect(),
                unresolved: p.pending,
            },
        );
    };
    let take_detour = || hub_ref.take_detour(&req.policy);
    // Only a FULL pass previews the queue behind it; a preview is itself
    // the interruption and does not nest.
    let preview_queued = || hub_ref.preview_queued(&req.policy);
    let nothing = || {};
    let between_chunks: &dyn Fn() = match depth {
        Depth::Full => &preview_queued,
        Depth::Preview => &nothing,
    };
    reverse::run_reporting(
        args,
        reverse::Hooks {
            on,
            take_detour: &take_detour,
            between_chunks,
            resolver: index.as_deref(),
        },
    )
}

/// Coverage as a reader sees it: the archive's, widened by whatever a running
/// pass has reached. `None` for both is a policy nobody has touched.
fn merged_coverage(
    archive: Option<&PolicyArchive>,
    live: Option<&Live>,
    walking: bool,
) -> CoverageDto {
    let cov = archive.map(|a| a.coverage());
    let live_floor = live.map(|l| l.floor).filter(|f| *f != u64::MAX);
    let live_ceiling = live.map(|l| l.ceiling).filter(|c| *c != 0);
    let min = |a: Option<u64>, b: Option<u64>| match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    let max = |a: Option<u64>, b: Option<u64>| match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    };
    CoverageDto {
        cached: cov.is_some() || live.is_some(),
        walked_from: min(cov.as_ref().and_then(|c| c.walked_from), live_floor),
        walked_to: max(cov.as_ref().and_then(|c| c.walked_to), live_ceiling),
        first_slot: min(
            cov.as_ref().and_then(|c| c.first_slot),
            live.and_then(|l| l.buffered_slots().0),
        ),
        last_slot: max(
            cov.as_ref().and_then(|c| c.last_slot),
            live.and_then(|l| l.buffered_slots().1),
        ),
        total_txs: cov.as_ref().map_or(0, |c| c.total_txs) + live.map_or(0, Live::buffered_txs),
        units: cov.as_ref().map_or(0, |c| c.units),
        unresolved: live.map_or_else(
            || cov.as_ref().map_or(0, |c| c.unresolved),
            |l| l.pending as u64,
        ),
        walking,
        // While a pass runs the verdict is not in yet; the archive's stands
        // until the pass lands and rewrites it.
        completeness: cov
            .as_ref()
            .map_or(policy_archive::Completeness::Unrecorded, |c| c.completeness)
            .as_wire(),
        reaching: live.and_then(|l| l.reaching),
        detours: live.map_or_else(Vec::new, |l| {
            l.detours
                .iter()
                .map(|(from, to)| SlotSpanDto {
                    from: *from,
                    to: *to,
                })
                .collect()
        }),
        // THE RANGES: what the archive has read, plus what the running pass
        // has reached so far and the windows its detours took — merged, so
        // a reader asks one question of one list.
        ranges: {
            let mut all: Vec<archive::SlotRange> =
                cov.as_ref().map_or_else(Vec::new, |c| c.ranges.clone());
            if let Some(l) = live {
                if let (Some(from), Some(to)) = (live_floor, live_ceiling) {
                    all.push(archive::SlotRange::new(from, to));
                }
                all.extend(
                    l.detours
                        .iter()
                        .map(|(from, to)| archive::SlotRange::new(*from, *to)),
                );
            }
            policy_archive::merge_ranges(all)
                .into_iter()
                .map(|r| SlotSpanDto {
                    from: r.from,
                    to: r.to,
                })
                .collect()
        },
        reading: match (live_floor, live_ceiling) {
            (Some(from), Some(to)) if walking => Some(SlotSpanDto { from, to }),
            _ => None,
        },
    }
}

// ─── wire types ──────────────────────────────────────────────────────────────

/// One transaction on the feed. Typed throughout, per the house rule.
#[derive(Serialize)]
pub struct FeedRowDto {
    pub tx: String,
    pub slot: u64,
    /// Unix seconds of the containing block.
    pub time: u64,
    pub units: Vec<UnitMoveDto>,
}

#[derive(Serialize)]
pub struct UnitMoveDto {
    pub name_hex: String,
    /// Net mint: 0 transfer, positive mint, negative burn.
    pub net_mint: i64,
    /// What happened, as far as the deltas can say.
    pub direction: DirectionDto,
    /// Every party that moved this unit — the primitive, so a consumer can
    /// reach its own verdict where `direction` declines to.
    pub parties: Vec<PartyMoveDto>,
}

/// The read-time derivation `store` refuses to bake in.
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DirectionDto {
    /// Exactly one loser and one gainer.
    Transfer { from: String, to: String },
    /// Created here; one recipient.
    Mint { to: String },
    /// Destroyed here; one source.
    Burn { from: String },
    /// Several parties on a side — a batched fill, a bulk move. Stated, never
    /// guessed: the "largest delta is the sender" rule is wrong exactly here.
    Ambiguous,
    /// The source sits below the walk's floor, so only the arrival is known.
    /// Distinct from `Ambiguous`: this resolves by walking deeper, that does
    /// not resolve at all.
    SourceBelowFloor { to: String },
}

#[derive(Serialize)]
pub struct PartyMoveDto {
    pub address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stake: Option<String>,
    pub amount: i64,
}

#[derive(Serialize)]
pub struct CoverageDto {
    /// Has any pass ever run for this policy?
    pub cached: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub walked_from: Option<u64>,
    /// Highest slot covered — the top of the coverage claim. `last_slot` is
    /// the newest ROW, which is not the same thing: covered-and-quiet and
    /// not-covered look identical without this.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub walked_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_slot: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_slot: Option<u64>,
    pub total_txs: u64,
    pub units: u64,
    /// Movements whose source is below the floor. Non-zero is NORMAL on a
    /// progressive walk and must reach the screen — a feed that hides it
    /// presents a partial history as a whole one.
    pub unresolved: u64,
    /// A pass is running and coverage is still moving.
    pub walking: bool,
    /// `complete` | `partial` | `unrecorded` — whether these numbers are
    /// holdings, a window of movement, or unknown.
    ///
    /// On the wire because the consumer cannot derive it: `walked_from` alone
    /// looks sufficient and is not — on a shallow walk the floor sits below the
    /// earliest transaction found, so every partial ledger would read as
    /// complete. Only the walk knows, so only the walk says.
    pub completeness: &'static str,
    /// A detour is reading the stretch around this slot right now.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reaching: Option<u64>,
    /// Stretches below `walked_from` that detours have read.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub detours: Vec<SlotSpanDto>,
    /// Every stretch READ — the archive's ranges, the running pass's reach
    /// and its detours — merged, ascending. `walked_from`/`walked_to` are
    /// its extremes; `detours` its members below the descent.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub ranges: Vec<SlotSpanDto>,
    /// The stretch the running pass is reading right now, if one is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reading: Option<SlotSpanDto>,
}

#[derive(Serialize)]
pub struct SlotSpanDto {
    pub from: u64,
    pub to: u64,
}

/// `POST /policy/{policy}/detour?at=S` — what the origin did with it.
#[derive(Serialize)]
pub struct DetourResponse {
    /// Queued for the running pass to read next.
    pub accepted: bool,
    /// Why not, when not: `not_walking` | `already_read` | `above_floor`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub because: Option<&'static str>,
    /// The seek the pass is reaching for now — this one, or a newer one
    /// that superseded it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reaching: Option<u64>,
}

#[derive(Deserialize)]
pub struct DetourQuery {
    pub at: u64,
}

#[derive(Serialize)]
pub struct PolicyFeedResponse {
    pub policy: String,
    #[serde(flatten)]
    pub coverage: CoverageDto,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job: Option<PolicyJob>,
    pub rows: Vec<FeedRowDto>,
}

#[derive(Serialize)]
pub struct PolicyRowAtResponse {
    pub policy: String,
    /// False means no pass has ever run — distinct from "ran, and this
    /// transaction is not one of ours". A consumer renders them differently.
    pub cached: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row: Option<FeedRowDto>,
}

#[derive(Deserialize)]
pub struct FeedQuery {
    pub limit: Option<u32>,
    pub before_slot: Option<u64>,
}

#[derive(Deserialize)]
pub struct RefreshQuery {
    /// Where to stop — the policy's first mint. Absent walks to the
    /// registry's floor, or genesis.
    pub to_slot: Option<u64>,
}

#[derive(Deserialize)]
pub struct DensityQuery {
    /// Bucket width in seconds. Defaults to a day, which is the archive's own
    /// resolution; coarser is summed up, finer is answered at a day.
    pub bucket: Option<u64>,
}

/// One bar of the activity histogram.
#[derive(Serialize)]
pub struct DensityBucketDto {
    /// Bucket start, unix seconds.
    pub t: u64,
    /// Bucket width, seconds.
    pub span: u64,
    /// Attributed movements — rows with a party.
    pub movements: u64,
    pub txs: u64,
    /// Transactions that minted / burned any unit.
    pub mints: u64,
    pub burns: u64,
}

/// `GET /policy/{p}/density` — the density tier. How much happened when,
/// over EVERYTHING the archive covers, from the footers alone.
///
/// The same shape the archive's footer answers with, so the consumer's spine
/// draws the true shape of a policy's life whether or not any rows have been
/// paged in.
#[derive(Serialize)]
pub struct PolicyDensityResponse {
    pub policy: String,
    pub cached: bool,
    pub bucket_secs: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub walked_from: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub walked_to: Option<u64>,
    pub completeness: &'static str,
    pub buckets: Vec<DensityBucketDto>,
}

// ─── derivation ──────────────────────────────────────────────────────────────

/// Turn signed per-party deltas into a direction, or decline.
///
/// Split out and pure because it is the one piece of judgement in this module,
/// and the failure mode — confidently naming the wrong sender on a batched fill
/// — is invisible in the output.
pub fn direction(unit: &archive::UnitMove) -> DirectionDto {
    use policy_archive::feed::Direction;
    // The rule lives in the crate, so a Worker reading R2 derives the same
    // direction from the same rows. This is only the spelling.
    match policy_archive::feed::direction(unit) {
        Direction::Mint { to } => DirectionDto::Mint { to },
        Direction::Burn { from } => DirectionDto::Burn { from },
        Direction::Transfer { from, to } => DirectionDto::Transfer { from, to },
        Direction::SourceBelowFloor { to } => DirectionDto::SourceBelowFloor { to },
        Direction::Ambiguous => DirectionDto::Ambiguous,
    }
}

fn to_dto(row: archive::FeedRow) -> FeedRowDto {
    FeedRowDto {
        tx: hex::encode(&row.tx_hash),
        slot: row.slot,
        time: row.block_time,
        units: row
            .units
            .into_iter()
            .map(|u| UnitMoveDto {
                name_hex: hex::encode(&u.name),
                net_mint: u.net_mint,
                direction: direction(&u),
                parties: u
                    .parties
                    .iter()
                    .map(|p| PartyMoveDto {
                        stake: crate::walk::stake_of(&p.address),
                        address: p.address.clone(),
                        amount: p.amount,
                    })
                    .collect(),
            })
            .collect(),
    }
}

// ─── handlers ────────────────────────────────────────────────────────────────

pub struct ApiError(StatusCode, String);

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (self.0, self.1).into_response()
    }
}

fn parse_policy(p: &str) -> Result<String, ApiError> {
    let p = p.to_lowercase();
    if p.len() != 56 || !p.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "expected a 56-character hex policy id".into(),
        ));
    }
    Ok(p)
}

fn gate(hub: &PolicyHub, headers: &HeaderMap) -> Result<(), ApiError> {
    if hub.authorised(headers) {
        Ok(())
    } else {
        Err(ApiError(StatusCode::UNAUTHORIZED, "unauthorized".into()))
    }
}

/// `GET /policy/{policy}` — cached rows newest-first, plus coverage and any
/// running pass. **Never starts one.**
pub async fn feed(
    State(hub): State<Arc<PolicyHub>>,
    headers: HeaderMap,
    AxPath(policy): AxPath<String>,
    Query(q): Query<FeedQuery>,
) -> Result<Json<PolicyFeedResponse>, ApiError> {
    gate(&hub, &headers)?;
    let policy = parse_policy(&policy)?;
    let job = hub.get(&policy);
    let walking = hub.walking(&policy);
    let limit = q.limit.unwrap_or(200).clamp(0, 5_000);
    let before = q.before_slot.unwrap_or(u64::MAX);

    // TWO TIERS OF THE SAME ROWS: the archive's (segments included), and the
    // running pass's buffer. Both are unfolded movements, appended and
    // folded once, so a correction the live pass found for an archived
    // transaction lands on it.
    let live = hub.live_for(&policy);
    let View {
        mut archive,
        live: live_view,
    } = view(&hub, &policy, &live).map_err(internal)?;
    let mut rows: Vec<Movement> = match archive.as_mut() {
        Some(a) if limit > 0 => a.movements_page(limit, q.before_slot).map_err(internal)?,
        _ => Vec::new(),
    };
    if let Some(l) = live_view.as_deref()
        && limit > 0
    {
        let archived: HashSet<Vec<u8>> = rows.iter().map(|m| m.tx_hash.clone()).collect();
        rows.extend(l.page(limit as usize, before, &archived));
    }
    let coverage = merged_coverage(archive.as_ref(), live_view.as_deref(), walking);
    let mut folded = crate::archive::fold_rows(rows);
    folded.truncate(limit as usize);
    Ok(Json(PolicyFeedResponse {
        policy,
        coverage,
        job,
        rows: folded.into_iter().map(to_dto).collect(),
    }))
}

/// `GET /policy/{policy}/density?bucket=` — the histogram, from footers.
pub async fn density(
    State(hub): State<Arc<PolicyHub>>,
    headers: HeaderMap,
    AxPath(policy): AxPath<String>,
    Query(q): Query<DensityQuery>,
) -> Result<Json<PolicyDensityResponse>, ApiError> {
    gate(&hub, &headers)?;
    let policy = parse_policy(&policy)?;
    // An hour at the finest, a month at the coarsest — anything else is a
    // request for the raw rows, which is the feed's job.
    let bucket_secs = q.bucket.unwrap_or(86_400).clamp(3_600, 31 * 86_400);
    let live = hub.live_for(&policy);
    let View {
        archive,
        live: live_view,
    } = view(&hub, &policy, &live).map_err(internal)?;
    // The footers' histogram — segments included — plus the buffer's.
    let from_archive = archive
        .as_ref()
        .map_or_else(Vec::new, |a| a.density(bucket_secs));
    let from_live = live_view
        .as_deref()
        .map_or_else(Vec::new, |l| l.buffered_density(bucket_secs));
    let coverage = merged_coverage(archive.as_ref(), live_view.as_deref(), hub.walking(&policy));
    let buckets = crate::archive::merge_density(from_archive, from_live)
        .into_iter()
        .map(|b| DensityBucketDto {
            t: b.from_unix,
            span: b.to_unix - b.from_unix,
            movements: b.movements,
            txs: b.txs,
            mints: b.mints,
            burns: b.burns,
        })
        .collect();
    Ok(Json(PolicyDensityResponse {
        policy,
        cached: coverage.cached,
        bucket_secs,
        walked_from: coverage.walked_from,
        walked_to: coverage.walked_to,
        completeness: coverage.completeness,
        buckets,
    }))
}

/// `GET /policy/{policy}/tx/{hash}` — ONE row by hash. Never scans.
pub async fn row_at(
    State(hub): State<Arc<PolicyHub>>,
    headers: HeaderMap,
    AxPath((policy, hash)): AxPath<(String, String)>,
) -> Result<Json<PolicyRowAtResponse>, ApiError> {
    gate(&hub, &headers)?;
    let policy = parse_policy(&policy)?;
    let raw = hex::decode(&hash)
        .ok()
        .filter(|b| b.len() == 32)
        .ok_or_else(|| {
            ApiError(
                StatusCode::BAD_REQUEST,
                "expected a 32-byte transaction hash in hex".into(),
            )
        })?;
    let live = hub.live_for(&policy);
    let View {
        mut archive,
        live: live_view,
    } = view(&hub, &policy, &live).map_err(internal)?;
    let mut rows = match archive.as_mut() {
        Some(a) => a.movements_of(&raw).map_err(internal)?,
        None => Vec::new(),
    };
    if let Some(l) = live_view.as_deref() {
        rows.extend(l.buffer.iter().filter(|m| m.tx_hash == raw).cloned());
    }
    let cached = archive.is_some() || live_view.is_some();
    Ok(Json(PolicyRowAtResponse {
        policy,
        cached,
        row: crate::archive::fold_rows(rows).pop().map(to_dto),
    }))
}

/// `POST /policy/{policy}/refresh?to_slot=S` — queue THE reverse pass.
///
/// One walk per policy, all the way to `to_slot` (the first mint, as the
/// caller learned it) or to the registry's floor or genesis. Tiers gate what
/// a reader sees, never how far this goes.
///
/// The ONLY route that starts work. Joining an existing pass rather than
/// stacking a second is deliberate: passes are serialised anyway, and a queue
/// of duplicates for one policy would starve every other caller.
pub async fn refresh(
    State(hub): State<Arc<PolicyHub>>,
    headers: HeaderMap,
    AxPath(policy): AxPath<String>,
    Query(q): Query<RefreshQuery>,
) -> Result<Json<PolicyJob>, ApiError> {
    gate(&hub, &headers)?;
    let policy = parse_policy(&policy)?;
    if let Some(state) = hub.get(&policy)
        && !state.is_terminal()
    {
        return Ok(Json(state));
    }
    hub.set(&policy, PolicyJob::Queued);
    hub.queued
        .lock()
        .expect("queued")
        .insert(policy.clone(), q.to_slot);
    let _ = hub.queue.send(PassRequest {
        policy: policy.clone(),
        to_slot: q.to_slot,
    });
    Ok(Json(PolicyJob::Queued))
}

/// `POST /policy/{policy}/detour?at=S` — ask the running pass to read the
/// stretch ending at `S` next, ahead of its descent.
///
/// The seek-below-the-floor path. A reader scrubbing to 2024 on a policy
/// whose walk is at 2025 would otherwise wait for the descent to get there;
/// this reads a bounded window at the playhead within seconds. Newest
/// request wins — a reader dragging the spine sends several and only the
/// last matters — and a stretch a detour has already read is not read
/// twice. Only meaningful while a pass runs: a landed archive has no floor
/// to be below.
pub async fn detour(
    State(hub): State<Arc<PolicyHub>>,
    headers: HeaderMap,
    AxPath(policy): AxPath<String>,
    Query(q): Query<DetourQuery>,
) -> Result<Json<DetourResponse>, ApiError> {
    gate(&hub, &headers)?;
    let policy = parse_policy(&policy)?;
    Ok(Json(hub.request_detour(&policy, q.at)))
}

/// `GET /policy/{policy}/events` — job progress until terminal.
#[derive(Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum Idle {
    Idle,
}

pub async fn events(
    State(hub): State<Arc<PolicyHub>>,
    headers: HeaderMap,
    AxPath(policy): AxPath<String>,
) -> Result<Sse<ReceiverStream<Result<Event, Infallible>>>, ApiError> {
    gate(&hub, &headers)?;
    let policy = parse_policy(&policy)?;
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(8);
    tokio::spawn(async move {
        loop {
            let (event, done) = match hub.get(&policy) {
                Some(s) => {
                    let done = s.is_terminal();
                    (Event::default().json_data(&s), done)
                }
                None => (Event::default().json_data(&Idle::Idle), true),
            };
            let Ok(event) = event else { break };
            if tx.send(Ok(event)).await.is_err() {
                break;
            }
            if done {
                break;
            }
            tokio::time::sleep(Duration::from_millis(700)).await;
        }
    });
    Ok(Sse::new(ReceiverStream::new(rx)).keep_alive(KeepAlive::default()))
}

fn internal(e: anyhow::Error) -> ApiError {
    ApiError(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"))
}

/// The policy routes, for mounting beside the artifact path.
pub fn router(hub: Arc<PolicyHub>) -> Router {
    Router::new()
        .route("/policy/{policy}", get(feed))
        .route("/policy/{policy}/tx/{hash}", get(row_at))
        .route("/policy/{policy}/density", get(density))
        .route("/policy/{policy}/refresh", post(refresh))
        .route("/policy/{policy}/detour", post(detour))
        .route("/policy/{policy}/events", get(events))
        .with_state(hub)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn party(addr: &str, amount: i64) -> archive::PartyMove {
        archive::PartyMove {
            address: addr.into(),
            amount,
        }
    }

    fn unit(net_mint: i64, parties: Vec<archive::PartyMove>) -> archive::UnitMove {
        archive::UnitMove {
            name: b"SpaceBud904".to_vec(),
            net_mint,
            parties,
        }
    }

    /// The ordinary case: one loser, one gainer, nothing minted.
    #[test]
    fn one_each_side_is_a_transfer() {
        let d = direction(&unit(0, vec![party("alice", -1), party("bob", 1)]));
        assert!(
            matches!(d, DirectionDto::Transfer { ref from, ref to } if from == "alice" && to == "bob"),
        );
    }

    #[test]
    fn a_mint_names_only_its_recipient() {
        let d = direction(&unit(1, vec![party("bob", 1)]));
        assert!(matches!(d, DirectionDto::Mint { ref to } if to == "bob"));
    }

    #[test]
    fn a_burn_names_only_its_source() {
        let d = direction(&unit(-1, vec![party("alice", -1)]));
        assert!(matches!(d, DirectionDto::Burn { ref from } if from == "alice"));
    }

    /// THE CASE THE STORE REFUSES TO GUESS.
    ///
    /// A batched fill has several parties on a side. "Largest delta is the
    /// sender" is wrong exactly here, so the answer is that there isn't one —
    /// and the parties travel alongside so a consumer can do better if it
    /// knows more than we do.
    #[test]
    fn several_parties_on_a_side_is_ambiguous_not_a_guess() {
        let d = direction(&unit(
            0,
            vec![party("a", -1), party("b", -1), party("c", 2)],
        ));
        assert!(
            matches!(d, DirectionDto::Ambiguous),
            "{:?}",
            "not ambiguous"
        );
    }

    /// A gainer with no loser and no mint is the progressive walk's signature:
    /// the source output sits below the floor. Distinct from ambiguity because
    /// walking deeper RESOLVES it, and a UI should say so rather than shrug.
    #[test]
    fn an_arrival_with_no_source_reads_as_below_the_floor() {
        let d = direction(&unit(0, vec![party("bob", 1)]));
        assert!(matches!(d, DirectionDto::SourceBelowFloor { ref to } if to == "bob"));
    }

    /// THE THREE SPELLINGS ARE ONE VOCABULARY.
    ///
    /// `store::Completeness::as_wire` (the forward walk's sqlite ledger),
    /// `policy_archive::Completeness::as_wire` (the archive's footer and
    /// manifest — what this API now serves) and `shared_types::policy_feed::
    /// Completeness`'s serde representation cross the tunnel as the same
    /// strings. A drift is silent in the worst way: an unknown variant
    /// deserialises to the `Unrecorded` default, so every ledger would quietly
    /// read as "coverage unknown" and no error would ever be raised.
    ///
    /// Pinned here rather than in the consumer because this is the side that
    /// writes them.
    #[test]
    fn the_wire_spellings_are_exhaustive_and_stable() {
        use crate::store::Completeness;
        let spellings: Vec<&str> = Completeness::ALL.iter().map(|c| c.as_wire()).collect();
        assert_eq!(spellings, vec!["complete", "partial", "unrecorded"]);
        // Distinct, or two states would collapse into one on the wire.
        let mut sorted = spellings.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), Completeness::ALL.len());
        let archive: Vec<&str> = policy_archive::Completeness::ALL
            .iter()
            .map(|c| c.as_wire())
            .collect();
        assert_eq!(archive, spellings, "the archive speaks the ledger's words");
    }

    /// The stored form round-trips, and an ABSENT row is `Unrecorded` — the
    /// distinction the `Option<bool>` this replaced could not express.
    #[test]
    fn completeness_round_trips_through_its_stored_code() {
        use crate::store::Completeness;
        for state in Completeness::ALL {
            assert_eq!(Completeness::from_code(state.code()), state, "{state:?}");
        }
        assert_eq!(Completeness::from_code(None), Completeness::Unrecorded);
    }

    /// The asymmetry is load-bearing: a partial ledger must not label its
    /// movements as holdings, but an unrecorded one keeps its supply reports
    /// rather than breaking every pre-flag token.
    #[test]
    fn only_a_complete_ledger_reports_holdings_but_only_partial_loses_supply_reports() {
        use crate::store::Completeness;
        assert!(Completeness::Complete.balances_are_holdings());
        assert!(!Completeness::Partial.balances_are_holdings());
        assert!(
            !Completeness::Unrecorded.balances_are_holdings(),
            "unknown must not be presented as holdings"
        );

        assert!(Completeness::Complete.supply_reports_defined());
        assert!(!Completeness::Partial.supply_reports_defined());
        assert!(
            Completeness::Unrecorded.supply_reports_defined(),
            "pre-flag ledgers keep working"
        );
    }

    fn live_row(slot: u64, tx: u8, unit: &str, addr: &str, amount: i64, net_mint: i64) -> Movement {
        Movement {
            slot,
            block_time: 1_700_000_000 + slot,
            tx_hash: vec![tx; 32],
            unit_name: unit.as_bytes().to_vec(),
            address: addr.to_string(),
            amount,
            net_mint,
        }
    }

    /// The live buffer: its histogram counts transactions once however many
    /// rows they have, a correction to an ARCHIVED transaction (above the
    /// ceiling) adds a movement but never a transaction, a page takes the
    /// newest transactions with every row of theirs, and a flush replaces
    /// the buffer with the segments that now hold it.
    #[test]
    fn the_live_tier_counts_and_pages_like_the_archive() {
        let mut live = Live::new(3);
        live.ceiling = 1_000;
        let dir = std::path::Path::new("/tmp/pass");
        live.publish(
            dir,
            &[
                live_row(900, 1, "A", "alice", 1, 1),
                live_row(900, 1, "B", "alice", 1, 1),
                live_row(800, 2, "A", "bob", 1, 0),
                live_row(800, 2, "A", "carol", -1, 0),
                // A correction for a transaction an earlier pass published.
                live_row(1_500, 9, "A", "dave", -1, 0),
            ],
            &[],
        );
        assert_eq!(live.buffered_txs(), 2);
        assert_eq!(live.buffered_slots(), (Some(800), Some(1_500)));
        let d = live.buffered_density(86_400);
        assert_eq!(d.iter().map(|b| b.txs).sum::<u64>(), 2);
        assert_eq!(d.iter().map(|b| b.mints).sum::<u64>(), 1);
        assert_eq!(d.iter().map(|b| b.movements).sum::<u64>(), 5);

        let none: HashSet<Vec<u8>> = HashSet::new();
        let page = live.page(1, u64::MAX, &none);
        assert!(page.iter().all(|m| m.tx_hash == vec![1; 32]), "{page:?}");
        assert_eq!(page.len(), 2);
        let older = live.page(1, 900, &none);
        assert!(older.iter().all(|m| m.tx_hash == vec![2; 32]));
        // The correction rides along only with the archived transaction it
        // corrects.
        let archived: HashSet<Vec<u8>> = [vec![9; 32]].into_iter().collect();
        let with = live.page(1, u64::MAX, &archived);
        assert_eq!(with.len(), 3);
        assert!(with.iter().any(|m| m.tx_hash == vec![9; 32]));
        assert_eq!(older.len(), 2, "both rows of the transaction come along");

        // A flush: the segments take over, the buffer empties.
        live.publish(
            dir,
            &[live_row(700, 3, "A", "erin", 1, 0)],
            &[archive::FileEntry {
                file: "seg-0000.parquet".into(),
                rows: 6,
                min_slot: Some(700),
                max_slot: Some(1_500),
                units: 0,
            }],
        );
        assert!(live.buffer.is_empty());
        assert_eq!(live.segments.len(), 1);
        assert_eq!(live.segments[0].1, archive::FileKind::Movements);
        assert_eq!(live.buffered_txs(), 0);
    }

    /// Zero-amount parties are filtered at write time, but a row assembled from
    /// an older ledger could carry one; it must not turn a clean transfer into
    /// an ambiguous mess.
    #[test]
    fn a_transfer_survives_a_stray_zero_party() {
        let d = direction(&unit(
            0,
            vec![party("alice", -1), party("bob", 1), party("idle", 0)],
        ));
        assert!(matches!(d, DirectionDto::Transfer { .. }));
    }
}
