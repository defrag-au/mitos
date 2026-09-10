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
use std::sync::{Arc, Mutex};
use std::time::Duration;

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

/// THE LOWER LOD — what a running JOB has walked so far. One per job in
/// flight; a policy with a walk job and two seek jobs running has three.
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
}

impl Live {
    pub fn new(seq: u32) -> Self {
        Self {
            seq,
            floor: u64::MAX,
            ceiling: 0,
            pending: 0,
            segments: Vec::new(),
            buffer: Vec::new(),
        }
    }

    /// Take a chunk's rows — or, if the pass flushed at the end of it, the
    /// segments that now hold everything buffered so far.
    pub fn publish(&mut self, pass_dir: &Path, rows: &[Movement], flushed: &[archive::FileEntry]) {
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

/// What a read sees: the archive — opened WITH every running job's segments
/// — and the jobs' buffers, or neither for a policy nobody has touched.
struct View<'a> {
    archive: Option<PolicyArchive>,
    lives: Vec<std::sync::MutexGuard<'a, Live>>,
}

fn view<'a>(hub: &PolicyHub, policy: &str, jobs: &'a [Arc<Mutex<Live>>]) -> Result<View<'a>> {
    let dir = hub.policy_dir(policy);
    let manifest = archive::load_manifest(&dir)?;
    // A job that has LANDED has its segments compacted away and its buffer
    // stale; the manifest names its sequence and the archive alone is the
    // truth for it. The guards are taken in one order everywhere — the
    // scheduler's list order — so two reads cannot deadlock.
    let lives: Vec<std::sync::MutexGuard<'a, Live>> = jobs
        .iter()
        .map(|l| l.lock().expect("live"))
        .filter(|l| {
            !manifest
                .as_ref()
                .is_some_and(|m| m.passes.iter().any(|p| p.seq == l.seq))
        })
        .collect();
    let segments: Vec<(PathBuf, archive::FileKind)> =
        lives.iter().flat_map(|l| l.segments.clone()).collect();
    let archive = PolicyArchive::open_with(&dir, &segments)?;
    Ok(View { archive, lives })
}

pub struct PolicyHub {
    pub data_dir: PathBuf,
    pub tokens: PathBuf,
    /// Root of the per-policy archives — Parquet plus a manifest, no
    /// database. See `archive.rs`.
    pub archive_dir: PathBuf,
    jobs: Mutex<HashMap<String, PolicyJob>>,
    /// tx hash → body over the snapshot, for resolving a seek's inputs on
    /// the spot. `None` without `--tx-index-dir`.
    pub(crate) index: Option<tx_index::IndexHandle>,
    /// (policy → its first mint's chunk) over the same snapshot. Held OPEN
    /// rather than opened per lookup: the answer is a binary search over a
    /// resident table, so opening a 666 MiB mapping each time would cost
    /// orders of magnitude more than the lookup it serves.
    ///
    /// `None` without `--policy-index-dir`, and the walk then falls back to
    /// Koios exactly as before.
    pub(crate) policy_index: Option<policy_index::Base>,
    /// The pool: walk jobs, seek jobs, what is in flight. See
    /// `scheduler.rs`.
    pub(crate) sched: crate::scheduler::Scheduler,
    bearer: Option<String>,
}

/// What a [`PolicyHub`] is built from.
///
/// ⚠️ A STRUCT rather than eight positional arguments, and not merely to
/// satisfy a lint: four of these are `Option<PathBuf>`/`Option<String>` in a
/// row, so transposing two at the call site would compile cleanly and open the
/// wrong index — the tx-index as the policy index, say, which fails its magic
/// check loudly, or two paths swapped, which does not.
pub struct HubConfig {
    pub data_dir: PathBuf,
    pub tokens: PathBuf,
    pub archive_dir: PathBuf,
    /// Where a landed archive goes — R2 and the KV bundle — or `None` to
    /// leave it on disk. See `publish.rs`.
    pub publish: Option<crate::publish::Targets>,
    /// A tx-index over the same snapshot, for seeks.
    pub tx_index_dir: Option<PathBuf>,
    /// A policy-index over the same snapshot, for first mints at admission.
    pub policy_index_dir: Option<PathBuf>,
    /// How many workers of each kind.
    pub pool: crate::scheduler::Pool,
    pub bearer: Option<String>,
}

impl PolicyHub {
    pub fn new(cfg: HubConfig) -> Arc<Self> {
        let HubConfig {
            data_dir,
            tokens,
            archive_dir,
            publish,
            tx_index_dir,
            policy_index_dir,
            pool,
            bearer,
        } = cfg;
        let index = tx_index_dir.and_then(|dir| {
            match tx_index::IndexHandle::open(&dir, &data_dir.join("immutable")) {
                Ok(h) => {
                    let cov = h.get().coverage();
                    tracing::info!(
                        dir = %dir.display(),
                        base_entries = cov.base_entries,
                        newest_chunk = ?cov.newest_chunk,
                        "policy: tx-index open — seeks resolve inputs on the spot"
                    );
                    Some(h)
                }
                Err(e) => {
                    tracing::warn!(dir = %dir.display(), error = %format!("{e:#}"), "policy: tx-index NOT open — seeks leave inputs to the descent");
                    None
                }
            }
        });
        let policy_index = policy_index_dir.and_then(|dir| {
            let path = policy_index::base_path(&dir);
            match policy_index::Base::open(&path) {
                Ok(b) => {
                    let (from, to) = b.covers();
                    tracing::info!(
                        path = %path.display(),
                        records = b.len(),
                        policies = b.policies(),
                        covers = format!("{from}..={to}"),
                        "policy: policy-index open — first mints resolve locally"
                    );
                    Some(b)
                }
                Err(e) => {
                    // A warning, not a failure: the floor probe falls back to
                    // Koios, and a walk with no floor is slow rather than
                    // wrong. Refusing to start over a missing index would make
                    // an optimisation load-bearing.
                    tracing::warn!(path = %path.display(), error = %format!("{e:#}"), "policy: policy-index NOT open — first mints fall back to koios");
                    None
                }
            }
        });
        if let Some(t) = &publish {
            tracing::info!(targets = %t.describe(), "policy: publishing landed archives");
        }
        let hub = Arc::new(Self {
            data_dir,
            tokens,
            archive_dir,
            jobs: Mutex::new(HashMap::new()),
            index,
            policy_index,
            sched: crate::scheduler::Scheduler::new(),
            bearer,
        });
        crate::scheduler::spawn(Arc::clone(&hub), pool, publish);
        hub
    }

    /// The policy's first mint from the local index, as a slot.
    ///
    /// Called at ADMISSION — once per policy, when a caller asked for a walk
    /// without saying where the mint is. Not per job: four walk workers on one
    /// policy would otherwise ask four times, and on the Koios path that was
    /// four network calls.
    ///
    /// 🔑 **A chunk's first slot is by construction at or below any mint
    /// inside it**, so this under-shoots by less than one chunk with no margin
    /// arithmetic. That matters because the error directions are not
    /// symmetric: a floor too LOW costs reading, a floor too HIGH truncates an
    /// archive AND lets it call itself complete.
    ///
    /// `None` for a policy the index has not reached — which is exactly the
    /// newly-minted policy the snapshot predates, and precisely why the Koios
    /// fallback still earns its place.
    pub(crate) fn first_mint_from_index(&self, policy_hex: &str) -> Option<u64> {
        let base = self.policy_index.as_ref()?;
        let policy = hex::decode(policy_hex).ok()?;
        let chunk = base.first_chunk_of(policy_index::policy_prefix(&policy))?;
        let slot = u64::from(chunk) * crate::registry::CHUNK_SLOTS;
        tracing::info!(
            policy = policy_hex,
            slot,
            chunk,
            source = "policy-index",
            "policy: first mint at admission"
        );
        Some(slot)
    }

    pub(crate) fn set(&self, policy: &str, state: PolicyJob) {
        self.jobs
            .lock()
            .expect("jobs")
            .insert(policy.to_string(), state);
    }

    fn get(&self, policy: &str) -> Option<PolicyJob> {
        self.jobs.lock().expect("jobs").get(policy).cloned()
    }

    /// The policy's archive directory.
    pub(crate) fn policy_dir(&self, policy: &str) -> PathBuf {
        crate::archive::policy_dir(&self.archive_dir, policy)
    }

    /// Every policy with an archive on disk, sorted — what the volatile
    /// tail follower keeps a tail for. From the DIRECTORY rather than a
    /// registry: an archive is the only thing that makes a tail useful, and
    /// it is also the only thing a tail can be recorded against.
    pub(crate) fn archived_policies(&self) -> Vec<String> {
        let Ok(entries) = std::fs::read_dir(&self.archive_dir) else {
            return Vec::new();
        };
        let mut out: Vec<String> = entries
            .flatten()
            .filter(|e| e.path().join(crate::archive::MANIFEST).exists())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.len() == 56 && n.chars().all(|c| c.is_ascii_hexdigit()))
            .collect();
        out.sort();
        out
    }

    /// A sequence for a new pass on this policy, from the SAME allocator
    /// the scheduler's jobs draw from.
    ///
    /// The volatile tail used to take `manifest.next_seq()` of its own,
    /// which collides: the scheduler counts up in memory from the sequence
    /// it read once, so a tail that guessed the same number would put two
    /// passes with one `seq` in the manifest and leave `latest_pass()`
    /// ambiguous.
    pub(crate) fn next_pass_seq(&self, policy: &str) -> u32 {
        self.sched.next_seq(self, policy)
    }

    /// The lock held across a manifest read-modify-write for one policy.
    /// Shared with the scheduler's landings — the volatile tail is one more
    /// writer of the same file.
    pub(crate) fn landing_lock(&self, policy: &str) -> Arc<Mutex<()>> {
        self.sched.lock_for(policy)
    }

    /// The snapshot's tip: the first slot of the chunk after the newest on
    /// disk. Read from the directory each time — a refresh can land while
    /// the service runs, and listing nine thousand names is milliseconds.
    pub(crate) fn tip_slot(&self) -> u64 {
        chain_sieve::list_chunks(&self.data_dir.join("immutable"), 0)
            .ok()
            .and_then(|c| c.last().copied())
            .map_or(0, |c| (c + 1) * mitos_chain_walk::mithril::CHUNK_SLOTS)
    }

    /// Is a walk wanted or running for this policy?
    fn walking(&self, policy: &str) -> bool {
        self.sched.walking(policy)
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

/// Coverage as a reader sees it: the archive's, widened by whatever the
/// running jobs have reached. Nothing for either is a policy nobody has
/// touched.
fn merged_coverage(archive: Option<&PolicyArchive>, lives: &[&Live], walking: bool) -> CoverageDto {
    let cov = archive.map(|a| a.coverage());
    let live_floor = lives
        .iter()
        .map(|l| l.floor)
        .filter(|f| *f != u64::MAX)
        .min();
    let live_ceiling = lives.iter().map(|l| l.ceiling).filter(|c| *c != 0).max();
    let min = |a: Option<u64>, b: Option<u64>| match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    let max = |a: Option<u64>, b: Option<u64>| match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    };
    CoverageDto {
        cached: cov.is_some() || !lives.is_empty(),
        walked_from: min(cov.as_ref().and_then(|c| c.walked_from), live_floor),
        walked_to: max(cov.as_ref().and_then(|c| c.walked_to), live_ceiling),
        first_slot: min(
            cov.as_ref().and_then(|c| c.first_slot),
            lives.iter().filter_map(|l| l.buffered_slots().0).min(),
        ),
        last_slot: max(
            cov.as_ref().and_then(|c| c.last_slot),
            lives.iter().filter_map(|l| l.buffered_slots().1).max(),
        ),
        total_txs: cov.as_ref().map_or(0, |c| c.total_txs)
            + lives.iter().map(|l| l.buffered_txs()).sum::<u64>(),
        units: cov.as_ref().map_or(0, |c| c.units),
        unresolved: lives
            .iter()
            .map(|l| l.pending as u64)
            .max()
            .unwrap_or_else(|| cov.as_ref().map_or(0, |c| c.unresolved)),
        walking,
        // While a job runs the verdict is not in yet; the archive's stands
        // until the job lands and the manifest is rederived.
        completeness: cov
            .as_ref()
            .map_or(policy_archive::Completeness::Unrecorded, |c| c.completeness)
            .as_wire(),
        // THE RANGES: what the archive has read, tagged by which chain it
        // came from, plus what the running jobs have reached so far. The
        // immutable stretches merge with each other and the volatile tail
        // stands apart — they are summed differently, so a reader has to be
        // able to tell them apart.
        ranges: {
            let mut immutable: Vec<archive::SlotRange> = cov
                .as_ref()
                .map_or_else(Vec::new, |c| c.immutable_ranges.clone());
            immutable.extend(lives.iter().filter_map(|l| reached(l)));
            let mut out: Vec<SlotSpanDto> = policy_archive::merge_ranges(immutable)
                .into_iter()
                .map(SlotSpanDto::reading)
                .collect();
            out.extend(
                cov.as_ref()
                    .into_iter()
                    .flat_map(|c| c.volatile.iter().copied())
                    .map(SlotSpanDto::of),
            );
            out.sort_by_key(|s| (s.from, s.to));
            out
        },
        reading: lives
            .iter()
            .filter_map(|l| reached(l))
            .map(SlotSpanDto::reading)
            .collect(),
    }
}

/// The stretch a running job has read so far: from where it has got to,
/// up to its ceiling. `None` before its first chunk.
fn reached(l: &Live) -> Option<archive::SlotRange> {
    match (l.floor, l.ceiling) {
        (u64::MAX, _) | (_, 0) => None,
        (from, to) => Some(archive::SlotRange::new(from.min(to), to)),
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
    /// Every stretch READ — the archive's ranges and what every running
    /// job has reached so far — merged, ascending. `walked_from` /
    /// `walked_to` are its extremes.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub ranges: Vec<SlotSpanDto>,
    /// The stretches jobs are reading right now.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub reading: Vec<SlotSpanDto>,
}

#[derive(Serialize)]
pub struct SlotSpanDto {
    pub from: u64,
    pub to: u64,
    /// `immutable` | `volatile`. A consumer that predates the volatile tail
    /// defaults it to immutable, which is what every span used to be.
    pub kind: &'static str,
}

impl SlotSpanDto {
    fn of(span: policy_archive::manifest::Span) -> Self {
        Self {
            from: span.from,
            to: span.to,
            kind: match span.kind {
                archive::RangeKind::Immutable => "immutable",
                archive::RangeKind::Volatile => "volatile",
            },
        }
    }

    /// A stretch a job is reading right now. Always settled: a job only
    /// reads the immutable chain.
    fn reading(r: archive::SlotRange) -> Self {
        Self {
            from: r.from,
            to: r.to,
            kind: "immutable",
        }
    }
}

#[derive(Deserialize)]
pub struct SeekQuery {
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
    // running jobs' buffers. Both are unfolded movements, appended and
    // folded once, so a correction a live job found for an archived
    // transaction lands on it.
    let jobs = hub.sched.inflight_for(&policy);
    let View { mut archive, lives } = view(&hub, &policy, &jobs).map_err(internal)?;
    let mut rows: Vec<Movement> = match archive.as_mut() {
        Some(a) if limit > 0 => a.movements_page(limit, q.before_slot).map_err(internal)?,
        _ => Vec::new(),
    };
    if limit > 0 {
        let archived: HashSet<Vec<u8>> = rows.iter().map(|m| m.tx_hash.clone()).collect();
        for l in &lives {
            rows.extend(l.page(limit as usize, before, &archived));
        }
    }
    let live_refs: Vec<&Live> = lives.iter().map(|g| &**g).collect();
    let coverage = merged_coverage(archive.as_ref(), &live_refs, walking);
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
    let jobs = hub.sched.inflight_for(&policy);
    let View { archive, lives } = view(&hub, &policy, &jobs).map_err(internal)?;
    // The footers' histogram — segments included — plus every buffer's.
    let from_archive = archive
        .as_ref()
        .map_or_else(Vec::new, |a| a.density(bucket_secs));
    let from_live = lives.iter().fold(Vec::new(), |acc, l| {
        crate::archive::merge_density(acc, l.buffered_density(bucket_secs))
    });
    let live_refs: Vec<&Live> = lives.iter().map(|g| &**g).collect();
    let coverage = merged_coverage(archive.as_ref(), &live_refs, hub.walking(&policy));
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

// ─── the interpretive tiers ──────────────────────────────────────────────────
//
// Everything below serves what the `archive` CLI has been able to print since
// 2026-09-08 but nothing could FETCH. The rows, the graph and the density tier
// were reachable over HTTP; price, trades and the supply reconciliation were
// not, so a consumer could draw who-moved-what and nothing about what any of
// it meant.

/// One side of a pair, as far as this policy's own archive can state it.
#[derive(Serialize)]
pub struct PairDepthDto {
    /// ⚠️ **RAW quote units per RAW base unit — not a display price.**
    ///
    /// Named the long way on purpose. For an ADA pair the quote is
    /// **lovelace**, so $PERP reads `226.58` here and `0.00022658 ADA` on a
    /// screen — a consumer that renders this field directly is wrong by 10⁶.
    ///
    /// Converting to a display price needs BOTH sides' decimals, and this
    /// archive knows neither: decimals live in the token registry, and the
    /// on-chain asset name is identity only. That is why the conversion is
    /// left to the caller rather than guessed at here.
    ///
    /// `None` on a zero base — a pool holding none of the asset prices
    /// nothing. **Never rendered as zero.**
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote_per_base_raw: Option<f64>,
    pub base: String,
    pub quote: String,
    pub pools: usize,
    /// The thinnest quote-side reserve of any contributing pool — the
    /// aggregate's weakest link.
    pub thinnest: i64,
    /// Hex policy of what the asset is paired WITH; **empty means ADA**, and
    /// that is the only case where the quote's decimals are known (6).
    pub quote_policy: String,
    /// Hex asset name — IDENTITY only. Never decode it for display; the token
    /// registry is authoritative for that.
    pub quote_name: String,
}

fn depth_dto(d: &policy_archive::PairDepth) -> PairDepthDto {
    PairDepthDto {
        quote_per_base_raw: d.rate(),
        // i128 as a string: a reserve can exceed what JSON numbers carry
        // safely, and a silently-rounded reserve is a silently-wrong price.
        base: d.base.to_string(),
        quote: d.quote.to_string(),
        pools: d.pools,
        thinnest: d.thinnest,
        quote_policy: hex::encode(&d.quote_unit.policy),
        quote_name: hex::encode(&d.quote_unit.name),
    }
}

/// `GET /policy/{p}/price` — spot, and a name for everything it cannot price.
///
/// ⚠️ The three lists are not degrees of confidence, they are different
/// CLAIMS, and collapsing them is how a price halves without anyone noticing:
///
/// - `ada` — priced, constant-product, ADA-paired. `None` is **UNDEFINED**,
///   never zero.
/// - `unresolved` — real reserves against a non-ADA unit. Pricing it needs
///   THAT unit's own archive; this one cannot and does not guess.
/// - `unpriceable` — real reserves under a model this crate will not evaluate
///   (a bonding curve is not constant-product). Reported so the liquidity is
///   visible without being priced.
#[derive(Serialize)]
pub struct PolicyPriceResponse {
    pub policy: String,
    pub cached: bool,
    /// The slot the price is defended AT. Piecewise-constant, so this is the
    /// last figure the archive can defend — not an estimate of "now".
    pub at_slot: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ada: Option<PairDepthDto>,
    /// Every contributing pool sits below the depth floor: the aggregate is
    /// still the right sum, but no single pool is worth quoting alone.
    pub thin: bool,
    pub depth_floor_lovelace: i64,
    pub unresolved: Vec<PairDepthDto>,
    pub unpriceable: Vec<PairDepthDto>,
    pub observations: usize,
}

pub async fn price(
    State(hub): State<Arc<PolicyHub>>,
    headers: HeaderMap,
    AxPath(policy): AxPath<String>,
) -> Result<Json<PolicyPriceResponse>, ApiError> {
    gate(&hub, &headers)?;
    let policy = parse_policy(&policy)?;
    let dir = hub.policy_dir(&policy);
    let manifest = archive::load_manifest(&dir).map_err(internal)?;
    let rows = match &manifest {
        Some(m) => crate::archive::read_observations(&dir, m).map_err(internal)?,
        None => Vec::new(),
    };
    let at = rows.iter().map(|o| o.slot).max().unwrap_or(0);
    let spot = policy_archive::spot_at(&rows, at);
    let floor = policy_archive::price::DEFAULT_FLOOR_LOVELACE;
    Ok(Json(PolicyPriceResponse {
        policy,
        cached: manifest.is_some(),
        at_slot: at,
        thin: spot.ada.as_ref().is_some_and(|d| !d.any_pool_above(floor)),
        ada: spot.ada.as_ref().map(depth_dto),
        depth_floor_lovelace: floor,
        unresolved: spot.unresolved.iter().map(depth_dto).collect(),
        unpriceable: spot.unpriceable.iter().map(depth_dto).collect(),
        observations: rows.len(),
    }))
}

/// `GET /policy/{p}/trades` — the fold, over the newest page of movements.
///
/// This is the answer to "why does this token show lots of transfers rather
/// than dex trades": a swap is two or three transactions, and the movements
/// alone cannot say which. Folding them names fills, placements and
/// cancellations — a cancellation being `order → wallet`, which is
/// indistinguishable from an ordinary transfer unless you know the contract.
#[derive(Serialize)]
pub struct PolicyTradesResponse {
    pub policy: String,
    pub cached: bool,
    /// Movements folded to produce this.
    pub over_movements: usize,
    pub venue_events: usize,
    pub transfers: usize,
    pub fills: usize,
    pub placements: usize,
    pub cancellations: usize,
    /// Several orders into one pool. **Named and counted, never decomposed** —
    /// "the largest mover is the trader" is wrong exactly here.
    pub batched_fills: usize,
    /// Fills whose trader the venue encodes in the order contract's stake.
    pub trader_named: usize,
    /// Fills on a venue whose order contract is ONE shared address, so the
    /// trader is in the placement leg and this fill cannot name them.
    /// ⚠️ Distinct from "we could not work it out".
    pub trader_not_encoded_by_venue: usize,
}

pub async fn trades(
    State(hub): State<Arc<PolicyHub>>,
    headers: HeaderMap,
    AxPath(policy): AxPath<String>,
    Query(q): Query<FeedQuery>,
) -> Result<Json<PolicyTradesResponse>, ApiError> {
    use policy_archive::trade::{Event, Party};
    gate(&hub, &headers)?;
    let policy = parse_policy(&policy)?;
    let jobs = hub.sched.inflight_for(&policy);
    let View { archive, .. } = view(&hub, &policy, &jobs).map_err(internal)?;
    let Some(mut a) = archive else {
        return Ok(Json(PolicyTradesResponse {
            policy,
            cached: false,
            over_movements: 0,
            venue_events: 0,
            transfers: 0,
            fills: 0,
            placements: 0,
            cancellations: 0,
            batched_fills: 0,
            trader_named: 0,
            trader_not_encoded_by_venue: 0,
        }));
    };
    let limit = q.limit.unwrap_or(2_000).clamp(1, 5_000);
    let rows = a.feed_rows(limit, None).map_err(internal)?;
    let folded = policy_archive::trade::fold(&rows, &crate::archive::venue_roles());

    let mut r = PolicyTradesResponse {
        policy,
        cached: true,
        over_movements: folded.len(),
        venue_events: 0,
        transfers: 0,
        fills: 0,
        placements: 0,
        cancellations: 0,
        batched_fills: 0,
        trader_named: 0,
        trader_not_encoded_by_venue: 0,
    };
    for f in &folded {
        let e = &f.event;
        match e {
            Event::Fill { party, .. } => {
                r.fills += 1;
                match party {
                    Party::Stake(_) | Party::Wallet(_) => r.trader_named += 1,
                    Party::NotEncodedByVenue => r.trader_not_encoded_by_venue += 1,
                    Party::Ambiguous => {}
                }
            }
            Event::Placement { .. } => r.placements += 1,
            Event::Cancellation { .. } => r.cancellations += 1,
            Event::BatchedFill { .. } => r.batched_fills += 1,
            Event::Transfer => r.transfers += 1,
        }
    }
    r.venue_events = r.over_movements - r.transfers;
    Ok(Json(r))
}

/// `GET /policy/{p}/supply` — the archive reconciled against ITSELF, plus what
/// the policy IS.
///
/// Both sides come out of the same file, so this needs no second source. The
/// **sign** of the gap is the whole check: positive is a source the walk has
/// not descended to (normal on a partial archive, damning on a complete one),
/// negative is supply minted that reached nobody (impossible at any
/// completeness).
#[derive(Serialize)]
pub struct PolicySupplyResponse {
    pub policy: String,
    pub cached: bool,
    pub completeness: &'static str,
    /// `Σ net_mint`, counted once per `(transaction, unit)`.
    pub minted: i64,
    /// `Σ amount` over every party row.
    pub moved: i64,
    /// `balanced` | `below-floor` | `failed`.
    pub verdict: &'static str,
    /// Outstanding supply, when the verdict is not `balanced`.
    pub gap: i64,
    /// Units contributing to that gap.
    pub gap_units: usize,
    /// Why a failure is a failure rather than a coverage number.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub because: Option<&'static str>,
    /// `fungible` | `collection` | `mixed` | `unknown` — what the units ARE,
    /// which decides whether a cohort band or a cascade even means anything.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub class: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub units_seen: Option<u64>,
}

pub async fn supply(
    State(hub): State<Arc<PolicyHub>>,
    headers: HeaderMap,
    AxPath(policy): AxPath<String>,
) -> Result<Json<PolicySupplyResponse>, ApiError> {
    use policy_archive::supply::Verdict;
    gate(&hub, &headers)?;
    let policy = parse_policy(&policy)?;
    let jobs = hub.sched.inflight_for(&policy);
    let View { archive, .. } = view(&hub, &policy, &jobs).map_err(internal)?;
    let Some(mut a) = archive else {
        return Ok(Json(PolicySupplyResponse {
            policy,
            cached: false,
            completeness: policy_archive::Completeness::Unrecorded.as_wire(),
            minted: 0,
            moved: 0,
            verdict: "balanced",
            gap: 0,
            gap_units: 0,
            because: None,
            class: None,
            units_seen: None,
        }));
    };
    let completeness = a.manifest.completeness();
    let profile = a.manifest.profile.clone();
    let r = a
        .reconcile(policy_archive::Diagnose::Totals)
        .map_err(internal)?;
    let balances = r.balances();
    let (verdict, gap, gap_units, because) = match r.verdict(completeness) {
        Verdict::Balanced => ("balanced", 0, 0, None),
        Verdict::BelowFloor { gap, units } => ("below-floor", gap, units, None),
        Verdict::Failed { offenders, because } => (
            "failed",
            offenders.iter().map(|b| b.gap()).sum(),
            offenders.len(),
            Some(because.as_wire()),
        ),
    };
    Ok(Json(PolicySupplyResponse {
        policy,
        cached: true,
        completeness: completeness.as_wire(),
        minted: balances.iter().map(|b| b.minted).sum(),
        moved: balances.iter().map(|b| b.moved).sum(),
        verdict,
        gap,
        gap_units,
        because,
        class: profile.as_ref().map(|p| p.class().as_str()),
        units_seen: profile.as_ref().map(|p| p.units_seen),
    }))
}

/// One sighting of the bonding curve — a point on the launch timeline.
#[derive(Serialize)]
pub struct CurvePointDto {
    pub slot: u64,
    pub unix: u64,
    /// Lovelace the curve held at this sighting.
    pub lovelace: i64,
    /// Tokens still ON the curve — the unsold inventory. ⚠️ Never part of
    /// float: it has never been owned by anyone.
    pub tokens_left: i64,
    /// How far up its own cap, 0.0–1.0. `None` when the datum did not decode,
    /// which is a different statement from 0.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress: Option<f64>,
}

/// How we know what state the launch is in. The codebase's basis discipline:
/// an inference and an observation must not read the same.
#[derive(Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum LaunchBasis {
    /// The curve reached its cap in an observation we hold.
    CurveAtCap,
    /// Curve sightings stop and other venues' begin — it trades elsewhere now.
    TradedElsewhereAfter,
    /// Curve sightings, none at cap, nothing after.
    StillOnTheCurve,
    /// No curve observation at all.
    NoLaunchObserved,
}

impl LaunchBasis {
    fn state(self) -> &'static str {
        match self {
            LaunchBasis::CurveAtCap | LaunchBasis::TradedElsewhereAfter => "graduated",
            LaunchBasis::StillOnTheCurve => "bonding",
            LaunchBasis::NoLaunchObserved => "unknown",
        }
    }
}

/// `GET /policy/{p}/launch` — a launchpad token's most consequential event,
/// and the shape of it over time.
///
/// Until 2026-09-08 a launch rendered as a transfer to an unnamed script. This
/// is the same data, named.
///
/// Serves the POINTS as well as the summary on purpose: a consumer building
/// its own timeline should not have to accept our reduction of it.
#[derive(Serialize)]
pub struct PolicyLaunchResponse {
    pub policy: String,
    pub cached: bool,
    /// `snek.fun`, or absent when no launchpad observation exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub venue: Option<String>,
    pub state: &'static str,
    pub basis: LaunchBasis,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub launched_at_slot: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub launched_at_unix: Option<u64>,
    /// Lovelace at which the pool graduates, INCLUDING the seed, straight
    /// from the datum.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ada_cap_threshold: Option<i64>,
    /// ⚠️ The seed is lovelace the pool was CREATED holding, not money the
    /// curve took. Netting it is the difference between 42,069.01 ADA and a
    /// number that looks right.
    pub curve_seed_lovelace: i64,
    /// What the curve had to collect: `ada_cap_threshold − seed`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ada_to_collect: Option<i64>,
    /// The market cap it graduates at — only when a sighting at the cap lets
    /// us state the tokens left there. Absent rather than estimated.
    ///
    /// ⚠️ This is priced off what the CURVE COLLECTED. The DEX pool that
    /// results opens ~1.88% lower, because a flat ~209.58 ADA executor fee
    /// leaves in the graduation transaction. Rendering one beside the other
    /// without naming both reads as an arithmetic bug rather than a fee.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub graduation_market_cap_lovelace: Option<i64>,
    /// The timeline, ordered by `(slot, curve position)`.
    ///
    /// ⚠️ **Within a slot this is curve position, NOT proven transaction
    /// order.** Sightings sharing a slot are ordered by ascending lovelace,
    /// which is ascending position up the curve — recovering their true
    /// transaction order would mean following the chain of curve UTxOs. A sell
    /// moves back DOWN the curve, so one occurring inside a single block would
    /// appear out of sequence here.
    ///
    /// This matters more than it sounds: MEASURED on $PERP, ten of eleven
    /// sightings share one slot.
    pub points: Vec<CurvePointDto>,
    /// How many distinct slots the points cover. `1` means the whole launch
    /// happened inside a single block, and no ordering within it is proven.
    pub distinct_slots: usize,
    /// Venue observations recorded AFTER the last curve sighting — the
    /// evidence behind `traded-elsewhere-after`.
    pub venues_after: Vec<String>,
}

pub async fn launch(
    State(hub): State<Arc<PolicyHub>>,
    headers: HeaderMap,
    AxPath(policy): AxPath<String>,
) -> Result<Json<PolicyLaunchResponse>, ApiError> {
    gate(&hub, &headers)?;
    let policy = parse_policy(&policy)?;
    let dir = hub.policy_dir(&policy);
    let manifest = archive::load_manifest(&dir).map_err(internal)?;
    let rows = match &manifest {
        Some(m) => crate::archive::read_observations(&dir, m).map_err(internal)?,
        None => Vec::new(),
    };

    // Curve sightings, oldest first. The observer tags them `BONDING_CURVE`
    // rather than by venue name, because "not constant-product" is the
    // property that matters and a second launchpad would share it.
    let mut curve: Vec<&policy_archive::Observation> = rows
        .iter()
        .filter(|o| {
            o.decoded
                .as_ref()
                .is_some_and(|d| d.pricing == policy_archive::observation::pricing::BONDING_CURVE)
        })
        .collect();
    // ⚠️ SLOT, then CURVE POSITION — and the second key is not a tiebreak for
    // tidiness.
    //
    // MEASURED on $PERP: ten of eleven sightings share ONE slot, because the
    // whole bonding happened inside a single block. Sorting by slot alone
    // leaves those ten in the order the parquet happened to yield, so a
    // consumer drawing a line gets a zigzag through the same instant.
    //
    // Within a slot the true TRANSACTION order is not recoverable here — it
    // would mean following the chain of curve UTxOs, each spending the last.
    // What IS recoverable is the curve's own position: lovelace in and tokens
    // out move strictly together, so ascending lovelace is ascending position
    // up the curve. That is the meaningful axis for a bonding curve and it is
    // deterministic.
    //
    // It is NOT proven time order: a sell moves back down the curve, and one
    // inside a single block would be re-ordered by this. Stated in the
    // response so a consumer can decide whether that matters to it, rather
    // than discovering it from a chart that looks fine.
    curve.sort_by(|a, b| {
        a.slot
            .cmp(&b.slot)
            .then_with(|| a.lovelace.cmp(&b.lovelace))
    });

    let venue = curve
        .first()
        .and_then(|o| o.decoded.as_ref())
        .map(|d| d.venue.clone());
    let last_curve_slot = curve.last().map(|o| o.slot);

    // Anything else, at a venue, after the curve went quiet.
    let mut venues_after: Vec<String> = rows
        .iter()
        .filter(|o| last_curve_slot.is_some_and(|s| o.slot > s))
        .filter_map(|o| o.decoded.as_ref())
        .filter(|d| d.pricing != policy_archive::observation::pricing::BONDING_CURVE)
        .map(|d| d.venue.clone())
        .collect();
    venues_after.sort();
    venues_after.dedup();

    // The datum carries the cap. Take it from the newest sighting that
    // decodes: the parameters do not change, and the newest is likeliest to
    // be the one nearest the cap.
    let pool = curve
        .iter()
        .rev()
        .filter_map(|o| o.datum.as_deref())
        .find_map(mitos_launchpad_decode::decode_bonding_datum);

    let points: Vec<CurvePointDto> = curve
        .iter()
        .map(|o| CurvePointDto {
            slot: o.slot,
            unix: o.block_time,
            lovelace: o.lovelace,
            tokens_left: o.unit_amount,
            progress: pool.as_ref().and_then(|p| p.progress(o.lovelace)),
        })
        .collect();

    // At the cap? Use THAT point's tokens_left; anything else would be an
    // estimate wearing an exact number's clothes.
    let at_cap = points
        .iter()
        .find(|p| p.progress.is_some_and(|f| f >= 0.99));

    // Supply, for the market cap. A full reconcile for one figure, but this
    // route is not polled the way the feed is, and the alternative is asking
    // the caller to join two responses to learn the headline fact about a
    // launch.
    let supply = match at_cap.is_some() {
        false => None,
        true => {
            let jobs = hub.sched.inflight_for(&policy);
            let View { archive, .. } = view(&hub, &policy, &jobs).map_err(internal)?;
            archive.and_then(|mut a| {
                a.reconcile(policy_archive::Diagnose::Totals)
                    .ok()
                    .map(|r| r.balances().iter().map(|b| b.minted).sum::<i64>())
                    .filter(|s| *s > 0)
            })
        }
    };
    let basis = match (curve.is_empty(), at_cap.is_some(), venues_after.is_empty()) {
        (true, _, _) => LaunchBasis::NoLaunchObserved,
        (false, true, _) => LaunchBasis::CurveAtCap,
        (false, false, false) => LaunchBasis::TradedElsewhereAfter,
        (false, false, true) => LaunchBasis::StillOnTheCurve,
    };

    Ok(Json(PolicyLaunchResponse {
        policy,
        cached: manifest.is_some(),
        venue,
        state: basis.state(),
        basis,
        launched_at_slot: curve.first().map(|o| o.slot),
        launched_at_unix: curve.first().map(|o| o.block_time),
        ada_cap_threshold: pool.as_ref().map(|p| p.ada_cap_threshold),
        curve_seed_lovelace: mitos_launchpad_decode::CURVE_SEED_LOVELACE,
        ada_to_collect: pool
            .as_ref()
            .map(|p| p.ada_cap_threshold - mitos_launchpad_decode::CURVE_SEED_LOVELACE),
        graduation_market_cap_lovelace: match (&pool, at_cap, supply) {
            // Needs all three: the cap from the datum, the tokens still on
            // the curve AT the cap, and the token's supply. Absent when any
            // is missing — an estimate here would wear an exact number's
            // clothes.
            (Some(p), Some(c), Some(s)) => p.graduation_market_cap(s, c.tokens_left),
            _ => None,
        },
        distinct_slots: {
            let mut s: Vec<u64> = points.iter().map(|p| p.slot).collect();
            s.dedup();
            s.len()
        },
        points,
        venues_after,
    }))
}

#[derive(Deserialize)]
pub struct StoryQuery {
    /// Window floor, inclusive. Absent = as far back as `limit` reaches.
    pub from: Option<u64>,
    /// Window ceiling, exclusive.
    pub to: Option<u64>,
    pub limit: Option<u32>,
    /// `1` to include 32-byte transaction hashes. **Off by default**: they are
    /// the single largest cost in the payload, and a chart does not need them.
    pub txs: Option<u8>,
    /// `json` for a debugging / non-Rust view. The default is postcard.
    pub format: Option<String>,
}

/// `GET /policy/{p}/story` — **one ordered stream that tells the token's
/// story**, and the surface a visualisation framework is meant to build on.
///
/// The other routes here slice the archive by KIND OF ANALYSIS, which makes us
/// the ones deciding what questions are askable and leaves a consumer joining
/// several time bases before it can draw anything. This slices by TIME and
/// makes the kind a property of each event, so every visualisation is a fold:
/// price is a filter to pool states, holders a fold over transfers, volume a
/// fold over fills, the launch a filter to curve states.
///
/// ⚠️ **Postcard by default, not JSON**, and `policy_archive::story::wire`
/// explains why at length: field names and addresses repeated per event, 64
/// hex characters where 32 bytes would do, and — the silent one — a JS number
/// cannot hold an `i64`. `?format=json` exists for debugging and for consumers
/// that cannot decode postcard; it is not the intended path.
pub async fn story(
    State(hub): State<Arc<PolicyHub>>,
    headers: HeaderMap,
    AxPath(policy): AxPath<String>,
    Query(q): Query<StoryQuery>,
) -> Result<axum::response::Response, ApiError> {
    use axum::response::IntoResponse;
    use policy_archive::story::wire;

    gate(&hub, &headers)?;
    let policy = parse_policy(&policy)?;
    let dir = hub.policy_dir(&policy);
    let manifest = archive::load_manifest(&dir).map_err(internal)?;
    let observations = match &manifest {
        Some(m) => crate::archive::read_observations(&dir, m).map_err(internal)?,
        None => Vec::new(),
    };

    let jobs = hub.sched.inflight_for(&policy);
    let View { archive, .. } = view(&hub, &policy, &jobs).map_err(internal)?;
    let limit = q.limit.unwrap_or(2_000).clamp(1, 5_000);
    let to = q.to.unwrap_or(u64::MAX);
    let from = q.from.unwrap_or(0);

    let rows = match archive {
        Some(mut a) => a.feed_rows(limit, q.to).map_err(internal)?,
        None => Vec::new(),
    };
    // `feed_rows` pages NEWEST-first from a ceiling; the window's floor is
    // applied here. Stated rather than hidden: a caller asking for a deep
    // window with a small limit gets the newest end of it, not the oldest.
    let rows: Vec<_> = rows.into_iter().filter(|r| r.slot >= from).collect();
    let obs: Vec<_> = observations
        .into_iter()
        .filter(|o| o.slot >= from && o.slot < to)
        .collect();

    let built = policy_archive::story::build(&rows, &obs, &crate::archive::venue_roles());
    let lo = built.events.first().map(|e| e.slot).unwrap_or(from);
    let hi = built.events.last().map(|e| e.slot).unwrap_or(from);
    let complete = manifest
        .as_ref()
        .is_some_and(|m| m.completeness() == policy_archive::Completeness::Complete);
    let stream = wire::encode(
        &built,
        &policy,
        lo,
        hi,
        complete,
        match q.txs.unwrap_or(0) {
            0 => wire::Txs::Omit,
            _ => wire::Txs::Include,
        },
    );

    match q.format.as_deref() {
        Some("json") => Ok(Json(stream).into_response()),
        _ => {
            let bytes = wire::to_bytes(&stream).map_err(|e| internal(anyhow::anyhow!("{e}")))?;
            Ok((
                [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                bytes,
            )
                .into_response())
        }
    }
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
    let jobs = hub.sched.inflight_for(&policy);
    let View { mut archive, lives } = view(&hub, &policy, &jobs).map_err(internal)?;
    let mut rows = match archive.as_mut() {
        Some(a) => a.movements_of(&raw).map_err(internal)?,
        None => Vec::new(),
    };
    for l in &lives {
        rows.extend(l.buffer.iter().filter(|m| m.tx_hash == raw).cloned());
    }
    let cached = archive.is_some() || !lives.is_empty();
    Ok(Json(PolicyRowAtResponse {
        policy,
        cached,
        row: crate::archive::fold_rows(rows).pop().map(to_dto),
    }))
}

/// `POST /policy/{policy}/refresh?to_slot=S` — walk this policy to its
/// mint.
///
/// `to_slot` is the first mint as the caller learned it (the query keeps
/// its old name). One walk per policy, whoever asked: joining a walk
/// already wanted is a no-op. Tiers gate what a reader sees, never how far
/// this goes. With the pool, the walk's first job — the newest ten days —
/// starts within one job of whatever is running.
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
        && hub.walking(&policy)
    {
        return Ok(Json(state));
    }
    hub.set(&policy, PolicyJob::Queued);
    // The CALLER's `?to_slot=` first — it is an assertion by whoever asked and
    // may be better informed than the snapshot. The index only answers when
    // nothing said, which used to mean walking to genesis.
    let first_mint = q.to_slot.or_else(|| hub.first_mint_from_index(&policy));
    hub.sched.want_walk(&policy, first_mint);
    Ok(Json(PolicyJob::Queued))
}

/// `POST /policy/{policy}/seek?at=S` — read the window ending at `S`.
///
/// The reader asked for a stretch nothing has read; its job goes to the
/// seek workers, never behind a walk, and its inputs resolve through the
/// tx-index on the spot. Declined when the stretch is read, when a job
/// over it is already queued or running, or when it lies below the mint.
pub async fn seek(
    State(hub): State<Arc<PolicyHub>>,
    headers: HeaderMap,
    AxPath(policy): AxPath<String>,
    Query(q): Query<SeekQuery>,
) -> Result<Json<crate::scheduler::SeekResponse>, ApiError> {
    gate(&hub, &headers)?;
    let policy = parse_policy(&policy)?;
    Ok(Json(hub.sched.seek(&hub, &policy, q.at)))
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
        // The interpretive tiers — what the movements MEAN. Present in the
        // archive since 2026-09-08 and unreachable over HTTP until now.
        .route("/policy/{policy}/price", get(price))
        .route("/policy/{policy}/trades", get(trades))
        .route("/policy/{policy}/supply", get(supply))
        .route("/policy/{policy}/launch", get(launch))
        // THE stream. The routes above are summaries derived from the same
        // archive; this is the substrate a consumer folds for itself.
        .route("/policy/{policy}/story", get(story))
        .route("/policy/{policy}/refresh", post(refresh))
        .route("/policy/{policy}/seek", post(seek))
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
