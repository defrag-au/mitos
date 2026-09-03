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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::Infallible;
use std::path::PathBuf;
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

/// THE LOWER LOD — what a running pass has walked so far, in memory.
///
/// A pass writes its Parquet ONCE, at the end, so without this a reader
/// scrubbing into the stretch being walked would see nothing for minutes.
/// The pass hands over every chunk's rows as it goes (`Progress::rows`) and
/// they are served, folded, exactly as the archive's rows are — the same
/// shape at a lower level of durability. When the pass lands the archive
/// holds the same rows and this is dropped.
///
/// `seq` is the manifest sequence the pass will write. A reader checks the
/// manifest for it: present means the pass landed and these rows are now
/// duplicates of the archive's, so they are ignored — which closes the race
/// between the manifest rename and this being cleared, without a lock across
/// two structures.
pub struct Live {
    pub seq: u32,
    pub floor: u64,
    pub ceiling: u64,
    pub rows: Vec<Movement>,
    pub pending: usize,
    pub min_slot: Option<u64>,
    pub max_slot: Option<u64>,
    /// The histogram, kept as rows arrive — per DAY, with the transaction
    /// sets that make "distinct transactions" and "transactions that minted"
    /// answerable without re-reading the rows. A full-history ClayNation
    /// pass holds 360k transactions; recomputing this per poll cost 1.5 s.
    density: BTreeMap<u64, LiveBucket>,
}

/// One day of a running pass.
#[derive(Default)]
struct LiveBucket {
    txs: HashSet<Vec<u8>>,
    mints: HashSet<Vec<u8>>,
    burns: HashSet<Vec<u8>>,
    movements: u64,
}

const LIVE_BUCKET_SECS: u64 = 86_400;

impl Live {
    fn new(seq: u32) -> Self {
        Self {
            seq,
            floor: u64::MAX,
            ceiling: 0,
            rows: Vec::new(),
            pending: 0,
            min_slot: None,
            max_slot: None,
            density: BTreeMap::new(),
        }
    }

    /// Take a chunk's rows.
    fn publish(&mut self, rows: &[Movement]) {
        for m in rows {
            self.min_slot = Some(self.min_slot.map_or(m.slot, |s| s.min(m.slot)));
            self.max_slot = Some(self.max_slot.map_or(m.slot, |s| s.max(m.slot)));
            let day = m.block_time / LIVE_BUCKET_SECS * LIVE_BUCKET_SECS;
            let b = self.density.entry(day).or_default();
            // A correction to an EARLIER pass's transaction sits above this
            // pass's ceiling; its transaction is the archive's to count.
            if m.slot < self.ceiling || self.ceiling == 0 {
                b.txs.insert(m.tx_hash.clone());
                if m.net_mint > 0 {
                    b.mints.insert(m.tx_hash.clone());
                }
                if m.net_mint < 0 {
                    b.burns.insert(m.tx_hash.clone());
                }
            }
            if !m.is_placeholder() {
                b.movements += 1;
            }
        }
        self.rows.extend_from_slice(rows);
    }

    /// Transactions this pass has FOUND.
    fn found_txs(&self) -> u64 {
        self.density.values().map(|b| b.txs.len() as u64).sum()
    }

    /// The histogram at `bucket_secs` — a day at the finest.
    fn density(&self, bucket_secs: u64) -> Vec<policy_archive::DensityBucket> {
        let width = bucket_secs.max(LIVE_BUCKET_SECS);
        let mut out: BTreeMap<u64, policy_archive::DensityBucket> = BTreeMap::new();
        for (day, b) in &self.density {
            let from = day / width * width;
            let slot = out.entry(from).or_insert(policy_archive::DensityBucket {
                from_unix: from,
                to_unix: from + width,
                ..policy_archive::DensityBucket::default()
            });
            slot.movements += b.movements;
            slot.txs += b.txs.len() as u64;
            slot.mints += b.mints.len() as u64;
            slot.burns += b.burns.len() as u64;
        }
        out.into_values().collect()
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
        let own = |m: &Movement| m.slot < self.ceiling || self.ceiling == 0;
        let mut top: BTreeSet<(u64, &[u8])> = BTreeSet::new();
        for m in self.rows.iter().filter(|m| own(m) && m.slot < before) {
            let key = (m.slot, m.tx_hash.as_slice());
            if top.len() < limit {
                top.insert(key);
            } else if top.first().is_some_and(|min| key > *min) {
                top.pop_first();
                top.insert(key);
            }
        }
        let wanted: HashSet<&[u8]> = top.iter().map(|(_, h)| *h).collect();
        self.rows
            .iter()
            .filter(|m| wanted.contains(m.tx_hash.as_slice()) || archived.contains(&m.tx_hash))
            .cloned()
            .collect()
    }
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
    queue: mpsc::Sender<PassRequest>,
    bearer: Option<String>,
}

impl PolicyHub {
    pub fn new(
        data_dir: PathBuf,
        tokens: PathBuf,
        archive_dir: PathBuf,
        bearer: Option<String>,
    ) -> Arc<Self> {
        let (tx, rx) = mpsc::channel::<PassRequest>();
        let hub = Arc::new(Self {
            data_dir,
            tokens,
            archive_dir,
            jobs: Mutex::new(HashMap::new()),
            live: Mutex::new(HashMap::new()),
            queue: tx,
            bearer,
        });
        // ONE worker, for the reason the artifact builder next door has one:
        // passes are disk-bound against a shared snapshot and two interleaving
        // would slow both. A queued request says so rather than starting.
        {
            let hub = Arc::clone(&hub);
            std::thread::spawn(move || {
                for req in rx {
                    let started = Instant::now();
                    let outcome = run_pass(&hub, &req);
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
                    hub.set(&req.policy, state);
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

fn run_pass(hub: &PolicyHub, req: &PassRequest) -> Result<reverse::Outcome> {
    let args = reverse::ReverseArgs {
        data_dir: hub.data_dir.clone(),
        tokens: hub.tokens.clone(),
        token: req.policy.clone(),
        archive_dir: hub.archive_dir.clone(),
        // ALL THE WAY. Tiers gate what a reader SEES, never how far the
        // walk goes — one walk per policy, shared by everyone.
        days: None,
        to_slot: req.to_slot,
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
    let hub_ref = hub;
    reverse::run_reporting(args, &|p| {
        {
            let mut l = live.lock().expect("live");
            l.floor = p.floor;
            l.ceiling = p.ceiling;
            l.pending = p.pending;
            l.publish(p.rows);
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
    })
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
            live.and_then(|l| l.min_slot),
        ),
        last_slot: max(
            cov.as_ref().and_then(|c| c.last_slot),
            live.and_then(|l| l.max_slot),
        ),
        total_txs: cov.as_ref().map_or(0, |c| c.total_txs) + live.map_or(0, Live::found_txs),
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
    }
}

/// Has the pass that produced `live` landed in the archive? If so its rows
/// are the archive's rows and must not be counted twice.
fn landed(archive: Option<&PolicyArchive>, live: &Live) -> bool {
    archive.is_some_and(|a| a.manifest.passes.iter().any(|p| p.seq == live.seq))
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
    let losers: Vec<&archive::PartyMove> = unit.parties.iter().filter(|p| p.amount < 0).collect();
    let gainers: Vec<&archive::PartyMove> = unit.parties.iter().filter(|p| p.amount > 0).collect();
    match (losers.as_slice(), gainers.as_slice(), unit.net_mint) {
        // Created here and landed in one place.
        ([], [to], m) if m > 0 => DirectionDto::Mint {
            to: to.address.clone(),
        },
        // Destroyed here, out of one place.
        ([from], [], m) if m < 0 => DirectionDto::Burn {
            from: from.address.clone(),
        },
        ([from], [to], 0) => DirectionDto::Transfer {
            from: from.address.clone(),
            to: to.address.clone(),
        },
        // Arrived from nobody we know, and nothing was minted: the source is
        // below the floor. A deeper pass resolves this into a Transfer.
        ([], [to], 0) => DirectionDto::SourceBelowFloor {
            to: to.address.clone(),
        },
        _ => DirectionDto::Ambiguous,
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
                        address: p.address.clone(),
                        stake: p.stake.clone(),
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

    // TWO TIERS OF THE SAME ROWS: the archive's, and the running pass's.
    // Both are unfolded movements, appended and folded once, so a correction
    // the live pass found for an archived transaction lands on it.
    let mut archive = PolicyArchive::open(&hub.policy_dir(&policy)).map_err(internal)?;
    let mut rows: Vec<Movement> = match archive.as_mut() {
        Some(a) if limit > 0 => a.movements_page(limit, q.before_slot).map_err(internal)?,
        _ => Vec::new(),
    };
    let live = hub.live_for(&policy);
    let live_guard = live.as_ref().map(|l| l.lock().expect("live"));
    let live_view = live_guard
        .as_deref()
        .filter(|l| !landed(archive.as_ref(), l));
    if let Some(l) = live_view
        && limit > 0
    {
        let archived: HashSet<Vec<u8>> = rows.iter().map(|m| m.tx_hash.clone()).collect();
        rows.extend(l.page(limit as usize, before, &archived));
    }
    let coverage = merged_coverage(archive.as_ref(), live_view, walking);
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
    let archive = PolicyArchive::open(&hub.policy_dir(&policy)).map_err(internal)?;
    let live = hub.live_for(&policy);
    let live_guard = live.as_ref().map(|l| l.lock().expect("live"));
    let live_view = live_guard
        .as_deref()
        .filter(|l| !landed(archive.as_ref(), l));
    // The footers' histogram plus the running pass's, bucket for bucket.
    let from_archive = archive
        .as_ref()
        .map_or_else(Vec::new, |a| a.density(bucket_secs));
    let from_live = live_view.map_or_else(Vec::new, |l| l.density(bucket_secs));
    let coverage = merged_coverage(archive.as_ref(), live_view, hub.walking(&policy));
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
    let mut archive = PolicyArchive::open(&hub.policy_dir(&policy)).map_err(internal)?;
    let mut rows = match archive.as_mut() {
        Some(a) => a.movements_of(&raw).map_err(internal)?,
        None => Vec::new(),
    };
    let live = hub.live_for(&policy);
    let live_guard = live.as_ref().map(|l| l.lock().expect("live"));
    let live_view = live_guard
        .as_deref()
        .filter(|l| !landed(archive.as_ref(), l));
    if let Some(l) = live_view {
        rows.extend(l.rows.iter().filter(|m| m.tx_hash == raw).cloned());
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
    let _ = hub.queue.send(PassRequest {
        policy: policy.clone(),
        to_slot: q.to_slot,
    });
    Ok(Json(PolicyJob::Queued))
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
        .route("/policy/{policy}/events", get(events))
        .with_state(hub)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn party(addr: &str, amount: i64) -> archive::PartyMove {
        archive::PartyMove {
            address: addr.into(),
            stake: None,
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

    /// The live tier: its histogram counts transactions once however many
    /// rows they have, a correction to an ARCHIVED transaction (above the
    /// ceiling) adds a movement but never a transaction, and a page takes
    /// the newest transactions with every row of theirs.
    #[test]
    fn the_live_tier_counts_and_pages_like_the_archive() {
        let mut live = Live::new(3);
        live.ceiling = 1_000;
        live.publish(&[
            live_row(900, 1, "A", "alice", 1, 1),
            live_row(900, 1, "B", "alice", 1, 1),
            live_row(800, 2, "A", "bob", 1, 0),
            live_row(800, 2, "A", "carol", -1, 0),
            // A correction for a transaction an earlier pass published.
            live_row(1_500, 9, "A", "dave", -1, 0),
        ]);
        assert_eq!(live.found_txs(), 2);
        assert_eq!((live.min_slot, live.max_slot), (Some(800), Some(1_500)));
        let d = live.density(86_400);
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
