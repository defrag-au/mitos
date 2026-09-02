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

use std::collections::HashMap;
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

use crate::reverse;
use crate::store::{self, Ledger};

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
    days: u64,
}

pub struct PolicyHub {
    pub data_dir: PathBuf,
    pub tokens: PathBuf,
    pub db_dir: PathBuf,
    jobs: Mutex<HashMap<String, PolicyJob>>,
    queue: mpsc::Sender<PassRequest>,
    bearer: Option<String>,
}

impl PolicyHub {
    pub fn new(
        data_dir: PathBuf,
        tokens: PathBuf,
        db_dir: PathBuf,
        bearer: Option<String>,
    ) -> Arc<Self> {
        let (tx, rx) = mpsc::channel::<PassRequest>();
        let hub = Arc::new(Self {
            data_dir,
            tokens,
            db_dir,
            jobs: Mutex::new(HashMap::new()),
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
                        Ok((written, backfilled, unresolved)) => PolicyJob::Done {
                            written,
                            backfilled,
                            unresolved,
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
                    hub.set(&req.policy, state);
                }
            });
        }
        hub
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

    /// Ledger path for a policy. One file per policy, like the walkers'.
    fn db_path(&self, policy: &str) -> PathBuf {
        self.db_dir.join(format!("{policy}.db"))
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

fn run_pass(hub: &PolicyHub, req: &PassRequest) -> Result<(u64, usize, usize)> {
    let args = reverse::ReverseArgs {
        data_dir: hub.data_dir.clone(),
        tokens: hub.tokens.clone(),
        token: req.policy.clone(),
        db: Some(hub.db_path(&req.policy)),
        days: req.days,
        to_slot: None,
        from_slot: None,
        no_sieve: false,
        report_every: u64::MAX,
    };
    let policy = req.policy.clone();
    let hub_ref = hub;
    reverse::run_reporting(args, &|p| {
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
    /// How much deeper to reach. One pass; call again to go further.
    pub days: Option<u64>,
}

// ─── derivation ──────────────────────────────────────────────────────────────

/// Turn signed per-party deltas into a direction, or decline.
///
/// Split out and pure because it is the one piece of judgement in this module,
/// and the failure mode — confidently naming the wrong sender on a batched fill
/// — is invisible in the output.
pub fn direction(unit: &store::UnitMove) -> DirectionDto {
    let losers: Vec<&store::PartyMove> = unit.parties.iter().filter(|p| p.amount < 0).collect();
    let gainers: Vec<&store::PartyMove> = unit.parties.iter().filter(|p| p.amount > 0).collect();
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

fn to_dto(row: store::FeedRow) -> FeedRowDto {
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
    let path = hub.db_path(&policy);
    if !path.exists() {
        return Ok(Json(PolicyFeedResponse {
            policy,
            coverage: CoverageDto {
                cached: false,
                walked_from: None,
                first_slot: None,
                last_slot: None,
                total_txs: 0,
                units: 0,
                unresolved: 0,
                walking: false,
                // No ledger, so nothing has been established about one. Not
                // `partial`: that would claim a window exists.
                completeness: crate::store::Completeness::Unrecorded.as_wire(),
            },
            job,
            rows: Vec::new(),
        }));
    }
    let ledger = Ledger::open(&path).map_err(internal)?;
    let cov = ledger.coverage().map_err(internal)?;
    let rows = ledger
        .feed_rows(q.limit.unwrap_or(200), q.before_slot)
        .map_err(internal)?;
    Ok(Json(PolicyFeedResponse {
        policy,
        coverage: CoverageDto {
            cached: true,
            walked_from: cov.walked_from,
            first_slot: cov.first_slot,
            last_slot: cov.last_slot,
            total_txs: cov.total_txs,
            units: cov.units,
            unresolved: cov.unresolved,
            // Target below the floor means a pass is still descending toward
            // it. Equal means idle — which is why the target collapses onto
            // the floor when a pass finishes.
            walking: matches!(
                (cov.walk_target, cov.walked_from),
                (Some(t), Some(f)) if t < f
            ),
            completeness: ledger.completeness().map_err(internal)?.as_wire(),
        },
        job,
        rows: rows.into_iter().map(to_dto).collect(),
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
    let path = hub.db_path(&policy);
    if !path.exists() {
        return Ok(Json(PolicyRowAtResponse {
            policy,
            cached: false,
            row: None,
        }));
    }
    let ledger = Ledger::open(&path).map_err(internal)?;
    let row = ledger.feed_row_at(&raw).map_err(internal)?;
    Ok(Json(PolicyRowAtResponse {
        policy,
        cached: true,
        row: row.map(to_dto),
    }))
}

/// `POST /policy/{policy}/refresh?days=N` — queue one reverse pass.
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
        days: q.days.unwrap_or(30),
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
        .route("/policy/{policy}/refresh", post(refresh))
        .route("/policy/{policy}/events", get(events))
        .with_state(hub)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn party(addr: &str, amount: i64) -> store::PartyMove {
        store::PartyMove {
            address: addr.into(),
            stake: None,
            amount,
        }
    }

    fn unit(net_mint: i64, parties: Vec<store::PartyMove>) -> store::UnitMove {
        store::UnitMove {
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

    /// THE TWO SPELLINGS ARE ONE VOCABULARY.
    ///
    /// `store::Completeness::as_wire` and `shared_types::policy_feed::
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
