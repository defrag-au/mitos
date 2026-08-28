//! HTTP handlers — typed responses only, per the house rule.

use std::convert::Infallible;
use std::time::Duration;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::ReceiverStream;

use crate::report;
use crate::serve::jobs::JobState;
use crate::serve::{AppState, db};
use crate::target;

/// anyhow → 500 with the chain as the body.
pub struct AppError(anyhow::Error);

impl<E: Into<anyhow::Error>> From<E> for AppError {
    fn from(e: E) -> Self {
        Self(e.into())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("{:#}", self.0)).into_response()
    }
}

#[derive(Serialize)]
pub struct Health {
    pub status: &'static str,
    pub uptime_secs: u64,
    pub wallets: u64,
    pub queue_depth: usize,
}

pub async fn health(State(state): State<AppState>) -> Json<Health> {
    let wallets = db::open_ro(&state.db_path)
        .and_then(|c| db::wallet_count(&c))
        .unwrap_or(0);
    Json(Health {
        status: "ok",
        uptime_secs: state.started.elapsed().as_secs(),
        wallets,
        queue_depth: state.registry.queue_depth(),
    })
}

#[derive(Deserialize)]
pub struct FlowsQuery {
    pub limit: Option<u32>,
    pub before_slot: Option<u64>,
    /// The window the CALLER is entitled to, so `deep_pending` can answer
    /// "is anything still missing *for you*" rather than "is this scanned to
    /// genesis". Without it a 90-day reader over a complete 90-day scan is
    /// told history is still coming, and goes chasing a backfill they neither
    /// need nor can see.
    ///
    /// Absent means the whole chain — the conservative reading, since it can
    /// only over-report missing history, never under-report it.
    pub window_days: Option<u64>,
}

#[derive(Serialize)]
pub struct FlowsResponse {
    pub target: String,
    pub canonical: String,
    /// Present when a refresh job exists for this wallet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job: Option<JobState>,
    pub cached: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scanned_to_chunk: Option<u64>,
    /// Slot-granular coverage horizon (tail-aware) — the honest "data as of"
    /// figure for a freshness badge.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scanned_to_slot: Option<u64>,
    /// History older than the published window is still being excavated, so
    /// `first_slot` is a floor on what is known rather than the wallet's
    /// actual beginning.
    ///
    /// Derived from [`Self::scanned_from_slot`] against the window this
    /// request asked for. Kept because consumers already read it; prefer the
    /// slot, which says HOW deep rather than merely "not deep enough".
    #[serde(default)]
    pub deep_pending: bool,
    /// The oldest slot actually scanned for this wallet.
    ///
    /// The counterpart to `scanned_to_slot`: that is the forward frontier,
    /// this is the floor. Without it a consumer cannot tell "we looked and
    /// the wallet starts here" from "we stopped looking here" — so a UI
    /// timeline built on `first_slot` silently presents a partial scan as the
    /// wallet's whole life.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scanned_from_slot: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_unix: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_slot: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_slot: Option<u64>,
    pub total_txs: u64,
    pub rows: Vec<report::Row>,
}

fn parse_target(t: &str) -> Result<(Vec<target::Cred>, String), (StatusCode, String)> {
    let creds =
        target::parse(t).map_err(|e| (StatusCode::BAD_REQUEST, format!("bad target: {e:#}")))?;
    let canonical = target::canonical(&creds);
    Ok((creds, canonical))
}

pub async fn flows(
    State(state): State<AppState>,
    Path(t): Path<String>,
    Query(q): Query<FlowsQuery>,
) -> Result<Json<FlowsResponse>, Response> {
    let (_, canonical) = parse_target(&t).map_err(|e| e.into_response())?;
    let limit = q.limit.unwrap_or(500).min(5000);
    let job = state.registry.snapshot(&canonical);

    let conn = match db::open_ro(&state.db_path) {
        Ok(c) => c,
        Err(_) => {
            // No cache file yet — nothing scanned by anyone.
            return Ok(Json(FlowsResponse {
                target: t,
                canonical,
                job,
                cached: false,
                scanned_to_chunk: None,
                scanned_to_slot: None,
                deep_pending: false,
                scanned_from_slot: None,
                updated_unix: None,
                first_slot: None,
                last_slot: None,
                total_txs: 0,
                rows: Vec::new(),
            }));
        }
    };
    let meta = db::load_wallet(&conn, &canonical).map_err(|e| AppError(e).into_response())?;
    let (rows, total) = match &meta {
        Some(_) => (
            db::query_rows(&conn, &canonical, limit, q.before_slot)
                .map_err(|e| AppError(e).into_response())?,
            db::count_flows(&conn, &canonical).map_err(|e| AppError(e).into_response())?,
        ),
        None => (Vec::new(), 0),
    };
    Ok(Json(FlowsResponse {
        target: t,
        canonical,
        job,
        cached: meta.is_some(),
        scanned_to_chunk: meta.as_ref().map(|m| m.scanned_to_chunk),
        scanned_to_slot: meta.as_ref().and_then(|m| m.scanned_to_slot),
        // "Still missing history" is now relative to WHAT WAS ASKED FOR: a
        // 90-day reader over a 90-day scan is complete, and telling them
        // otherwise sends them chasing a backfill they do not need.
        deep_pending: meta.as_ref().is_some_and(|m| {
            m.needs_backfill(db::ScanTarget::wanted(
                q.window_days,
                super::jobs::now_slot(),
            ))
        }),
        scanned_from_slot: meta
            .as_ref()
            .and_then(|m| m.scanned_from.map(|t| t.floor())),
        updated_unix: meta.as_ref().map(|m| m.updated_unix),
        first_slot: meta.as_ref().and_then(|m| m.first_slot),
        last_slot: meta.as_ref().and_then(|m| m.last_slot),
        total_txs: total,
        rows,
    }))
}

#[derive(Serialize)]
pub struct RefreshResponse {
    pub canonical: String,
    #[serde(flatten)]
    pub job: JobState,
}

#[derive(Deserialize)]
pub struct RefreshQuery {
    /// Days of history to reach for on a COLD wallet. Absent or `0` means
    /// everything. A caller that only serves a 90-day view should ask for 90
    /// days: it costs the box ~3 GB instead of 219 GB, which is what makes a
    /// free tier affordable.
    pub window_days: Option<u64>,
}

pub async fn refresh(
    State(state): State<AppState>,
    Path(t): Path<String>,
    Query(q): Query<RefreshQuery>,
) -> Result<Json<RefreshResponse>, Response> {
    let (_, canonical) = parse_target(&t).map_err(|e| e.into_response())?;
    let job = state
        .registry
        .enqueue_windowed(&t, &canonical, q.window_days.filter(|d| *d > 0))
        .map_err(|e| AppError(e).into_response())?;
    let snapshot = job.state.lock().expect("job state").clone();
    Ok(Json(RefreshResponse {
        canonical,
        job: snapshot,
    }))
}

/// Job progress for this wallet, one JSON event per ~700ms, ending after the
/// terminal state. No job ⇒ a single `idle` event.
#[derive(Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum EventBody {
    Idle,
}

pub async fn events(
    State(state): State<AppState>,
    Path(t): Path<String>,
) -> Result<Sse<ReceiverStream<Result<Event, Infallible>>>, Response> {
    let (_, canonical) = parse_target(&t).map_err(|e| e.into_response())?;
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(8);
    let registry = state.registry.clone();
    tokio::spawn(async move {
        loop {
            let (event, done) = match registry.snapshot(&canonical) {
                Some(s) => {
                    let done = s.is_terminal();
                    (Event::default().json_data(&s), done)
                }
                None => (Event::default().json_data(&EventBody::Idle), true),
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
