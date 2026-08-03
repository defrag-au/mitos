//! `/health` and `/events` handlers.

use axum::Json;
use axum::extract::{Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use market_ledger_wire::EventKind;
use serde::{Deserialize, Serialize};

use super::AppState;
use super::query::{Cursor, EventFilter, LedgerRow};
use super::{encode, query};

/// Handler errors → JSON `{"error": …}` with a mapped status.
pub enum ApiError {
    BadRequest(String),
    Internal(anyhow::Error),
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        ApiError::Internal(e)
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ApiError::Internal(e) => {
                tracing::error!("internal error: {e:#}");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
            }
        };
        (status, Json(ErrorBody { error: message })).into_response()
    }
}

pub async fn health(State(state): State<AppState>) -> Result<Response, ApiError> {
    let db = state.db.clone();
    let snapshot = tokio::task::spawn_blocking(move || db.health())
        .await
        .map_err(|e| ApiError::Internal(e.into()))??;
    Ok(Json(snapshot).into_response())
}

#[derive(Debug, Deserialize)]
pub struct EventsParams {
    venue: Option<String>,
    policy: Option<String>,
    asset: Option<String>,
    name: Option<String>,
    kind: Option<String>,
    from_slot: Option<u64>,
    to_slot: Option<u64>,
    limit: Option<u32>,
    cursor: Option<String>,
    format: Option<String>,
}

/// `?format=json` debug body (text-form rows, as stored).
#[derive(Serialize)]
struct EventsJson {
    events: Vec<LedgerRow>,
    next_cursor: Option<String>,
}

fn parse_filter(p: &EventsParams, state: &AppState) -> Result<EventFilter, ApiError> {
    let bad = |m: &str| ApiError::BadRequest(m.to_string());

    if let Some(policy) = &p.policy
        && (policy.len() != 56 || !policy.chars().all(|c| c.is_ascii_hexdigit()))
    {
        return Err(bad("policy must be 56 hex chars"));
    }
    if let Some(asset) = &p.asset {
        if !asset.starts_with("asset1") {
            return Err(bad("asset must be a CIP-14 fingerprint (asset1…)"));
        }
        if p.policy.is_some() {
            return Err(bad("provide either asset or policy(+name), not both"));
        }
    }
    if let Some(name) = &p.name {
        if p.policy.is_none() {
            return Err(bad("name requires policy"));
        }
        if !name.chars().all(|c| c.is_ascii_hexdigit()) || name.len() % 2 != 0 {
            return Err(bad("name must be hex (asset_name_hex)"));
        }
    }

    let mut kinds = Vec::new();
    if let Some(kind_param) = &p.kind {
        for k in kind_param.split(',') {
            match EventKind::from_db_str(k.trim()) {
                Some(kind) => kinds.push(kind.as_db_str()),
                None => return Err(ApiError::BadRequest(format!("unknown kind: {k}"))),
            }
        }
    }

    let cursor = match &p.cursor {
        Some(c) => Some(Cursor::parse(c).ok_or_else(|| bad("malformed cursor"))?),
        None => None,
    };

    Ok(EventFilter {
        venue: p.venue.clone(),
        policy: p.policy.clone(),
        fingerprint: p.asset.clone(),
        asset_name_hex: p.name.clone().map(|n| n.to_lowercase()),
        kinds,
        from_slot: p.from_slot,
        to_slot: p.to_slot,
        cursor,
        limit: p.limit.unwrap_or(state.default_limit).min(state.max_limit),
    })
}

pub async fn events(
    State(state): State<AppState>,
    Query(params): Query<EventsParams>,
) -> Result<Response, ApiError> {
    let filter = parse_filter(&params, &state)?;
    let db = state.db.clone();
    let (rows, next) = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
        let conn = db.open_ro()?;
        query::fetch_events(&conn, &filter)
    })
    .await
    .map_err(|e| ApiError::Internal(e.into()))??;

    if params.format.as_deref() == Some("json") {
        return Ok(Json(EventsJson {
            events: rows,
            next_cursor: next.map(|c| c.encode()),
        })
        .into_response());
    }

    let page = encode::build_page(rows, next)?;
    let bytes = market_ledger_wire::encode_events_page(&page)
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("wire encode failed: {e}")))?;
    Ok(([(header::CONTENT_TYPE, "application/octet-stream")], bytes).into_response())
}

/// `/count` response — the single-asset fill count for the filter.
#[derive(Serialize)]
struct CountJson {
    count: u64,
}

/// `GET /count` — a lightweight aggregate (no rows) over the same filter as
/// `/events`: how many single-asset fills match. Used for period-over-period
/// trend baselines (e.g. this 30d vs the prior 30d) where fetching the rows
/// would be wasteful.
pub async fn count(
    State(state): State<AppState>,
    Query(params): Query<EventsParams>,
) -> Result<Response, ApiError> {
    let filter = parse_filter(&params, &state)?;
    let db = state.db.clone();
    let count = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
        let conn = db.open_ro()?;
        query::count_events(&conn, &filter)
    })
    .await
    .map_err(|e| ApiError::Internal(e.into()))??;

    Ok(Json(CountJson { count }).into_response())
}

#[derive(Debug, Deserialize)]
pub struct ListingsParams {
    policy: Option<String>,
    venue: Option<String>,
    limit: Option<u32>,
    format: Option<String>,
}

/// `?format=json` debug body for `/listings`.
#[derive(Serialize)]
struct ListingsJson {
    count: u64,
    floor_lovelace: Option<u64>,
    listings: Vec<crate::store::Listing>,
}

pub async fn listings(
    State(state): State<AppState>,
    Query(params): Query<ListingsParams>,
) -> Result<Response, ApiError> {
    let policy = params
        .policy
        .clone()
        .ok_or_else(|| ApiError::BadRequest("policy is required".into()))?;
    if policy.len() != 56 || !policy.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(ApiError::BadRequest("policy must be 56 hex chars".into()));
    }
    let venue = params.venue.clone();
    // Default to the hard cap so all of a policy's listings come back in one
    // page (per-policy counts sit well under it); no cursor pagination yet.
    let limit = params.limit.unwrap_or(state.max_limit).min(state.max_limit);

    let db = state.db.clone();
    let (rows, count) = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
        let conn = db.open_ro()?;
        query::fetch_listings(&conn, &policy, venue.as_deref(), limit)
    })
    .await
    .map_err(|e| ApiError::Internal(e.into()))??;

    if params.format.as_deref() == Some("json") {
        let floor = rows.iter().filter_map(|r| r.price_lovelace).min();
        return Ok(Json(ListingsJson {
            count,
            floor_lovelace: floor,
            listings: rows,
        })
        .into_response());
    }

    let page = encode::build_listings_page(rows, count)?;
    let bytes = market_ledger_wire::encode_listings_page(&page)
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("wire encode failed: {e}")))?;
    Ok(([(header::CONTENT_TYPE, "application/octet-stream")], bytes).into_response())
}
