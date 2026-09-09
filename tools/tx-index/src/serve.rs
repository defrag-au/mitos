//! `serve` — the loopback read surface (market-ledger / wallet-sieve serve
//! pattern: axum, loopback listen, bearer-gated data routes, `/health`
//! open, tokio confined to [`run`]).
//!
//! Surface:
//! - `GET  /health` — coverage; open.
//! - `GET  /tx/{hash}` — the body (hex) + every output, decoded.
//! - `GET  /tx/{hash}/out/{index}` — one output.
//! - `GET  /tx/{hash}/aux` — the tx's auxiliary data (metadata) CBOR.
//! - `POST /aux` — `{tx_hashes:[…]}` → one aux result per hash, in order.
//!   For walkers resolving a whole book; bounded by batches, not listings.
//! - `POST /resolve` — `{items:[{tx_hash,index}]}` → one result per item,
//!   in order, each with its own status. For walkers resolving in batches.
//!
//! The index is hot-swapped: a background task re-checks the index dir
//! every `--reload-secs` and re-maps when a refresh has landed a new base
//! or segment. Requests in flight keep the mapping they started with.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::extract::{Path, Request, State};
use axum::http::{Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use tower_http::cors::{Any, CorsLayer};
use tx_index::wire::{
    AuxBatchRequest, AuxBatchResponse, AuxResponse, HealthResponse, OutputResponse, ResolveRequest,
    ResolveResponse, ResolveResult, ResolveStatus, TxResponse,
};
use tx_index::{Coverage, IndexHandle, Resolution};

pub const TOKEN_ENV: &str = "TX_INDEX_TOKEN";

/// Batch ceiling for `POST /resolve` — a walker wanting more sends more
/// requests; one request never pins a worker for long.
const MAX_BATCH: usize = 1_000;

#[derive(clap::Args, Debug)]
pub struct ServeArgs {
    /// Listen address. Keep it loopback; anything external goes through a
    /// CF tunnel (market-ledger.defrag.cc pattern). 8186: the box's ledger
    /// ports run 8181 mitos, 8182 preprod, 8183 market-ledger, 8184
    /// wallet-sieve, 8185 token-ledger.
    #[arg(long, env = "TX_INDEX_LISTEN", default_value = "127.0.0.1:8186")]
    listen: String,

    /// Index directory (segments/ + base.idx).
    #[arg(long)]
    index_dir: std::path::PathBuf,

    /// Immutable DB dir the located bodies are read from.
    #[arg(long)]
    immutable: std::path::PathBuf,

    /// How often to check the index dir for a new base/segments.
    #[arg(long, default_value_t = 30)]
    reload_secs: u64,
}

#[derive(Clone)]
struct AppState {
    index: Arc<IndexHandle>,
    started: Instant,
    token: Option<String>,
}

pub fn run(args: ServeArgs) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let token = match std::env::var(TOKEN_ENV) {
        Ok(t) if !t.is_empty() => {
            tracing::info!("auth token loaded; data routes require it");
            Some(t)
        }
        _ => {
            tracing::warn!(
                "{TOKEN_ENV} not set — serving in open mode (no auth). \
                 Set it before exposing this beyond localhost."
            );
            None
        }
    };

    let index = Arc::new(IndexHandle::open(&args.index_dir, &args.immutable)?);
    let cov = index.get().coverage();
    tracing::info!(?cov, "index mapped");

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move {
        let state = AppState {
            index: index.clone(),
            started: Instant::now(),
            token,
        };

        {
            let index = index.clone();
            let every = Duration::from_secs(args.reload_secs.max(1));
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(every);
                tick.tick().await;
                loop {
                    tick.tick().await;
                    let idx = index.clone();
                    match tokio::task::spawn_blocking(move || idx.reload_if_changed()).await {
                        Ok(Ok(true)) => {
                            let cov = index.get().coverage();
                            tracing::info!(?cov, "index re-mapped after on-disk change");
                        }
                        Ok(Ok(false)) => {}
                        Ok(Err(e)) => tracing::warn!("index reload failed: {e:#}"),
                        Err(e) => tracing::warn!("index reload task panicked: {e}"),
                    }
                }
            });
        }

        let data = Router::new()
            .route("/tx/{hash}", get(tx))
            .route("/tx/{hash}/aux", get(aux))
            .route("/tx/{hash}/out/{index}", get(output))
            .route("/aux", post(aux_batch))
            .route("/resolve", post(resolve))
            .layer(middleware::from_fn_with_state(state.clone(), require_auth));

        let app = Router::new()
            .route("/health", get(health))
            .merge(data)
            .layer(
                CorsLayer::new()
                    .allow_origin(Any)
                    .allow_methods([Method::GET, Method::POST])
                    .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE]),
            )
            .with_state(state);

        let listener = tokio::net::TcpListener::bind(&args.listen)
            .await
            .with_context(|| format!("binding {}", args.listen))?;
        tracing::info!(listen = %args.listen, "serving");
        axum::serve(listener, app).await.context("serve")
    })
}

async fn require_auth(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if let Some(expected) = state.token.as_deref() {
        let provided = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.strip_prefix("Bearer "));
        match provided {
            Some(p) if constant_time_eq(p.as_bytes(), expected.as_bytes()) => {}
            _ => return Err(StatusCode::UNAUTHORIZED),
        }
    }
    Ok(next.run(req).await)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// A refusal as status + message (the wallet-sieve `AppError` shape).
struct AppError {
    status: StatusCode,
    message: String,
}

impl AppError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

impl<E: Into<anyhow::Error>> From<E> for AppError {
    fn from(e: E) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, format!("{:#}", e.into()))
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (self.status, self.message).into_response()
    }
}

pub fn coverage_json(cov: &Coverage) -> HealthResponse {
    HealthResponse {
        status: "ok",
        uptime_secs: 0,
        base_first_chunk: cov.base_first_chunk,
        base_last_chunk: cov.base_last_chunk,
        base_entries: cov.base_entries,
        tail_segments: cov.tail_segments,
        tail_entries: cov.tail_entries,
        newest_chunk: cov.newest_chunk,
    }
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let cov = state.index.get().coverage();
    let mut h = coverage_json(&cov);
    h.uptime_secs = state.started.elapsed().as_secs();
    Json(h)
}

fn parse_hash(s: &str) -> Result<[u8; 32], AppError> {
    let v = hex::decode(s.trim())
        .map_err(|_| AppError::new(StatusCode::BAD_REQUEST, "tx hash is not hex"))?;
    v.try_into().map_err(|v: Vec<u8>| {
        AppError::new(
            StatusCode::BAD_REQUEST,
            format!("tx hash is {} bytes, want 32", v.len()),
        )
    })
}

async fn tx(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<Json<TxResponse>, AppError> {
    let h = parse_hash(&hash)?;
    let idx = state.index.get();
    let body = tokio::task::spawn_blocking(move || idx.tx(&h))
        .await
        .map_err(|e| AppError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))??;
    let Some(body) = body else {
        return Err(AppError::new(
            StatusCode::NOT_FOUND,
            "no body with that hash in any completed chunk",
        ));
    };
    let outputs = body.outputs()?;
    Ok(Json(TxResponse {
        tx_hash: hex::encode(body.hash),
        era: body.era.to_string(),
        chunk: body.loc.chunk,
        offset: body.loc.offset,
        body_cbor: hex::encode(&body.cbor),
        outputs,
    }))
}

/// A transaction's auxiliary data. All three outcomes are 200 — the caller
/// branches on `status`, never on the HTTP code. See [`AuxResponse`].
async fn aux(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<Json<AuxResponse>, AppError> {
    let h = parse_hash(&hash)?;
    let idx = state.index.get();
    // `tx()` rather than `tx_metadata()`: the response reports the era and
    // chunk, which only the located body carries, and it is the same read.
    let found = tokio::task::spawn_blocking(move || {
        let Some(body) = idx.tx(&h)? else {
            return Ok::<_, anyhow::Error>(None);
        };
        let cbor = body.aux.map(|loc| idx.read_span(loc)).transpose()?;
        Ok(Some((body, cbor)))
    })
    .await
    .map_err(|e| AppError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))??;

    Ok(Json(match found {
        Some((body, Some(cbor))) => AuxResponse::Found {
            tx_hash: hex::encode(body.hash),
            era: body.era.to_string(),
            chunk: body.loc.chunk,
            aux_cbor: hex::encode(cbor),
        },
        Some((body, None)) => AuxResponse::NoMetadata {
            tx_hash: hex::encode(body.hash),
            era: body.era.to_string(),
            chunk: body.loc.chunk,
        },
        None => AuxResponse::UnknownTx {
            tx_hash: hex::encode(h),
        },
    }))
}

/// `POST /aux` — auxiliary data for many transactions in one round trip.
/// Same per-item semantics as [`aux`], one result per requested hash in order.
async fn aux_batch(
    State(state): State<AppState>,
    Json(req): Json<AuxBatchRequest>,
) -> Result<Json<AuxBatchResponse>, AppError> {
    if req.tx_hashes.len() > MAX_BATCH {
        return Err(AppError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "{} hashes; the batch ceiling is {MAX_BATCH}",
                req.tx_hashes.len()
            ),
        ));
    }
    let idx = state.index.get();
    let results = tokio::task::spawn_blocking(move || {
        req.tx_hashes
            .into_iter()
            .map(|hex_hash| {
                // A malformed hash is reported as unknown rather than failing
                // the batch: one bad entry must not cost the caller the other
                // 999 lookups it asked for.
                let Ok(h) = parse_hash(&hex_hash) else {
                    return AuxResponse::UnknownTx { tx_hash: hex_hash };
                };
                match idx.tx(&h) {
                    Ok(Some(body)) => {
                        let cbor = body.aux.map(|loc| idx.read_span(loc)).transpose();
                        match cbor {
                            Ok(Some(cbor)) => AuxResponse::Found {
                                tx_hash: hex::encode(body.hash),
                                era: body.era.to_string(),
                                chunk: body.loc.chunk,
                                aux_cbor: hex::encode(cbor),
                            },
                            Ok(None) => AuxResponse::NoMetadata {
                                tx_hash: hex::encode(body.hash),
                                era: body.era.to_string(),
                                chunk: body.loc.chunk,
                            },
                            Err(e) => {
                                tracing::warn!(tx = %hex_hash, error = %format!("{e:#}"), "batch aux read failed");
                                AuxResponse::UnknownTx { tx_hash: hex_hash }
                            }
                        }
                    }
                    Ok(None) => AuxResponse::UnknownTx { tx_hash: hex_hash },
                    Err(e) => {
                        tracing::warn!(tx = %hex_hash, error = %format!("{e:#}"), "batch aux lookup failed");
                        AuxResponse::UnknownTx { tx_hash: hex_hash }
                    }
                }
            })
            .collect::<Vec<_>>()
    })
    .await
    .map_err(|e| AppError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(AuxBatchResponse { results }))
}

async fn output(
    State(state): State<AppState>,
    Path((hash, index)): Path<(String, u32)>,
) -> Result<Json<OutputResponse>, AppError> {
    let h = parse_hash(&hash)?;
    let idx = state.index.get();
    let res = tokio::task::spawn_blocking(move || idx.resolve(&h, index))
        .await
        .map_err(|e| AppError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))??;
    match res {
        Resolution::Found { body, output } => Ok(Json(OutputResponse {
            tx_hash: hex::encode(body.hash),
            index,
            era: body.era.to_string(),
            chunk: body.loc.chunk,
            output,
        })),
        Resolution::NoSuchOutput { outputs, .. } => Err(AppError::new(
            StatusCode::NOT_FOUND,
            format!("tx has {outputs} outputs; index {index} does not exist"),
        )),
        Resolution::UnknownTx => Err(AppError::new(
            StatusCode::NOT_FOUND,
            "no body with that hash in any completed chunk",
        )),
    }
}

async fn resolve(
    State(state): State<AppState>,
    Json(req): Json<ResolveRequest>,
) -> Result<Json<ResolveResponse>, AppError> {
    if req.items.len() > MAX_BATCH {
        return Err(AppError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "{} items; the batch ceiling is {MAX_BATCH}",
                req.items.len()
            ),
        ));
    }
    let idx = state.index.get();
    let results = tokio::task::spawn_blocking(move || {
        req.items
            .into_iter()
            .map(|item| {
                let base = ResolveResult {
                    tx_hash: item.tx_hash.clone(),
                    index: item.index,
                    status: ResolveStatus::Error,
                    output: None,
                    message: None,
                };
                let h = match parse_hash(&item.tx_hash) {
                    Ok(h) => h,
                    Err(e) => {
                        return ResolveResult {
                            status: ResolveStatus::BadRequest,
                            message: Some(e.message),
                            ..base
                        };
                    }
                };
                match idx.resolve(&h, item.index) {
                    Ok(Resolution::Found { output, .. }) => ResolveResult {
                        status: ResolveStatus::Found,
                        output: Some(output),
                        ..base
                    },
                    Ok(Resolution::NoSuchOutput { outputs, .. }) => ResolveResult {
                        status: ResolveStatus::NoSuchOutput,
                        message: Some(format!("tx has {outputs} outputs")),
                        ..base
                    },
                    Ok(Resolution::UnknownTx) => ResolveResult {
                        status: ResolveStatus::UnknownTx,
                        ..base
                    },
                    Err(e) => ResolveResult {
                        message: Some(format!("{e:#}")),
                        ..base
                    },
                }
            })
            .collect::<Vec<_>>()
    })
    .await
    .map_err(|e| AppError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ResolveResponse { results }))
}
