//! Bundle: owns the domain, registers indexers, runs the framework.
//!
//! Replaces the hand-rolled composition that lived inline in
//! `bundles/default/src/main.rs` during Phase 1. Bundle authors now
//! write:
//!
//! ```ignore
//! let mut bundle = Bundle::new(domain, config, listen_addr);
//! bundle.add_indexer(JpgCoIndexer::new()?);
//! bundle.add_indexer(OwnershipIndexer::new()?);
//! bundle.run(exit).await?;
//! ```
//!
//! The generic `add_indexer<I: Indexer<DomainAdapter>>` keeps each
//! indexer's associated `Scope`/`Change` types at the registration
//! boundary; internally `IndexerAdapter<I>` wraps it into the
//! object-safe `IndexerHandle` so the bundle can store a
//! heterogeneous collection. See `docs/design/CF_REPLICATION.md` for
//! why the trait is non-object-safe.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get};
use dolos::adapters::DomainAdapter;
use dolos_core::Domain;
use dolos_core::config::RootConfig;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::auth::AuthToken;
use crate::handle::{IndexerAdapter, IndexerHandle};
use crate::indexer::Indexer;
use crate::replicate::replicate_router;
use crate::replicator::{ConnState, Replicator, Subscription, SubscriptionId};
use crate::{run_dispatcher, spawn_sync_pipeline};

pub struct Bundle {
    domain: DomainAdapter,
    config: RootConfig,
    listen: SocketAddr,
    data_dir: PathBuf,
    indexers: Vec<Arc<dyn IndexerHandle>>,
}

impl Bundle {
    /// Construct a bundle.
    ///
    /// `data_dir` is where mitos stores its own state (subscription
    /// registry, future per-indexer materialized views). Independent
    /// of the Dolos data dir referenced by `config` — Dolos's data
    /// dir is under its own ownership, mitos doesn't write there.
    pub fn new(
        domain: DomainAdapter,
        config: RootConfig,
        listen: SocketAddr,
        data_dir: PathBuf,
    ) -> Self {
        Self {
            domain,
            config,
            listen,
            data_dir,
            indexers: Vec::new(),
        }
    }

    /// Register an indexer with the bundle. The indexer's `Scope`
    /// and `Change` types are erased into CBOR-bytes-at-the-boundary
    /// inside the adapter; bundle code never sees the erased form.
    pub fn add_indexer<I>(&mut self, indexer: I)
    where
        I: Indexer<DomainAdapter> + 'static,
    {
        let adapter = IndexerAdapter::<I>::new(indexer);
        self.indexers.push(Arc::new(adapter));
    }

    /// Print a summary of the loaded configuration to stdout
    /// without spawning the chain follower or any dispatchers, then
    /// return. Used by `mitos --print-config-only` to validate
    /// startup state offline (env vars, paths, persisted
    /// subscriptions).
    pub fn print_config_summary(self) -> anyhow::Result<()> {
        let Bundle {
            domain: _,
            config,
            listen,
            data_dir,
            indexers,
        } = self;

        println!("# mitos config summary");
        println!();
        println!("listen:        {listen}");
        println!("data_dir:      {}", data_dir.display());
        println!("storage.wal:   {:?}", config.storage.wal.path());
        println!("storage.state: {:?}", config.storage.state.path());
        println!();
        println!("indexers ({}):", indexers.len());
        for h in &indexers {
            println!("  - {}", h.name());
        }
        println!();

        let auth = AuthToken::from_env();
        println!("auth:          {}", if auth.is_open() { "OPEN" } else { "set" });
        println!();

        let replicator_path = data_dir.join("subscriptions.redb");
        let persisted = Replicator::list_persisted(&replicator_path)?;
        println!(
            "persisted subscriptions ({}, from {}):",
            persisted.len(),
            replicator_path.display()
        );
        for (id, sub) in persisted {
            println!(
                "  [{id}] indexer={} target={} cursor={:?} scope_bytes={}",
                sub.indexer,
                sub.target_url,
                sub.cursor,
                sub.scope.len()
            );
        }
        Ok(())
    }

    /// Run the bundle: spawn the chain-sync pipeline, bootstrap each
    /// indexer, start its dispatcher, mount HTTP routes (per-indexer +
    /// `/replicate/{indexer}` test surface + `/_admin/subscriptions`),
    /// and serve until `exit` is cancelled.
    pub async fn run(self, exit: CancellationToken) -> anyhow::Result<()> {
        let Bundle {
            domain,
            config,
            listen,
            data_dir,
            indexers,
        } = self;

        let sync_handle = spawn_sync_pipeline(domain.clone(), &config, exit.clone())?;
        info!("chain-sync pipeline spawned");

        let started_at = SystemTime::now();
        let indexer_names: Vec<String> = indexers.iter().map(|h| h.name().to_string()).collect();

        let mut dispatcher_handles: Vec<JoinHandle<()>> = Vec::with_capacity(indexers.len());
        let mut app = axum::Router::new();

        for ix in &indexers {
            let name = ix.name();

            let from = ix.bootstrap(&domain).await?;
            info!(indexer = %name, ?from, "indexer bootstrapped");

            let subscription = domain
                .watch_tip(Some(from.clone()))
                .map_err(|e| anyhow::anyhow!("watch_tip for {name}: {e:?}"))?;

            let ix_clone = ix.clone();
            let domain_clone = domain.clone();
            let handle = tokio::spawn(async move {
                run_dispatcher(ix_clone, domain_clone, subscription).await;
            });
            dispatcher_handles.push(handle);

            app = app.nest(&format!("/{name}"), ix.routes());
        }

        let auth = AuthToken::from_env();

        // Mount CF replication test surface (server-accepted WS) and
        // admin endpoints for managing outbound `Replicator`
        // subscriptions (production WS-client direction). Both gated
        // by Bearer-token auth (see auth.rs).
        app = app.merge(replicate_router(&indexers, domain.clone(), auth.clone()));

        let replicator_path = data_dir.join("subscriptions.redb");
        let replicator = Arc::new(Replicator::new(
            &indexers,
            domain.clone(),
            &replicator_path,
            auth.clone(),
        )?);
        info!(path = %replicator_path.display(), "replicator registry opened");
        app = app.merge(admin_router(replicator.clone(), auth.clone()));

        // /health surfaces replicator state — open (no auth) so a
        // status page or LB health check can hit it without
        // credentials. No sensitive data is exposed.
        let health_state = HealthState {
            started_at,
            indexers: indexer_names,
            replicator: replicator.clone(),
        };
        app = app.route(
            "/health",
            get(handle_health).with_state(health_state),
        );

        let listener = tokio::net::TcpListener::bind(listen).await?;
        info!(addr = %listen, "HTTP server listening");

        let serve = axum::serve(listener, app)
            .with_graceful_shutdown(async move { exit.cancelled().await });

        tokio::select! {
            result = serve => {
                if let Err(e) = result {
                    error!(error = %e, "HTTP server exited with error");
                }
            }
            _ = sync_handle => {
                info!("sync pipeline exited");
            }
        }

        for h in dispatcher_handles {
            h.abort();
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------
// Admin endpoints: /_admin/subscriptions
// ---------------------------------------------------------------------

#[derive(Clone)]
struct AdminState {
    replicator: Arc<Replicator>,
}

fn admin_router(replicator: Arc<Replicator>, auth: AuthToken) -> axum::Router {
    let state = AdminState { replicator };
    axum::Router::new()
        .route(
            "/_admin/subscriptions",
            get(list_subscriptions).post(add_subscription),
        )
        .route("/_admin/subscriptions/{id}", delete(remove_subscription))
        .with_state(state)
        .layer(axum::middleware::from_fn_with_state(
            auth,
            crate::auth::require_auth,
        ))
}

#[derive(Serialize)]
struct SubscriptionEntry {
    id: SubscriptionId,
    sub: Subscription,
    state: ConnState,
}

async fn list_subscriptions(State(state): State<AdminState>) -> Json<Vec<SubscriptionEntry>> {
    let entries = state
        .replicator
        .list()
        .await
        .into_iter()
        .map(|(id, sub, state)| SubscriptionEntry { id, sub, state })
        .collect();
    Json(entries)
}

/// Friendly admin-API shape. The framework looks up the indexer by
/// name to convert `scope` (JSON) → typed `Scope` → CBOR, and
/// parses `cursor` from a friendly string. Avoids forcing admin
/// clients to know the indexer's CBOR encoding or the dolos-core
/// `ChainPoint` JSON shape.
///
/// Examples:
/// ```json
/// {
///   "indexer": "collection-ownership",
///   "target_url": "wss://collection-ownership-mitos.<acct>.workers.dev/_internal/replicate?policy_id=abc...",
///   "scope": {"policy_id": "abc..."},
///   "cursor": "origin"
/// }
/// ```
///
/// `cursor` accepts:
/// - `"origin"` — start from chain origin
/// - `"<slot>"` — start from a slot (no hash)
/// - `"<slot>:<hash_hex>"` — specific block at a slot
#[derive(Deserialize)]
struct AddSubscription {
    indexer: String,
    target_url: String,
    scope: serde_json::Value,
    cursor: String,
}

#[derive(Serialize)]
struct AddedResponse {
    id: SubscriptionId,
}

async fn add_subscription(
    State(state): State<AdminState>,
    Json(body): Json<AddSubscription>,
) -> Result<Json<AddedResponse>, (StatusCode, String)> {
    // Look up the indexer so we can encode the scope using its
    // typed `Scope` shape. Returns 400 if the indexer is unknown.
    let handle = state
        .replicator
        .indexer_handle(&body.indexer)
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                format!("unknown indexer: {}", body.indexer),
            )
        })?;

    let scope_cbor = handle
        .encode_scope_from_json(&body.scope)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let cursor = parse_friendly_cursor(&body.cursor)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let sub = Subscription {
        indexer: body.indexer,
        target_url: body.target_url,
        scope: scope_cbor,
        cursor,
    };
    state
        .replicator
        .add(sub)
        .await
        .map(|id| Json(AddedResponse { id }))
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))
}

fn parse_friendly_cursor(s: &str) -> anyhow::Result<dolos_core::ChainPoint> {
    if s.eq_ignore_ascii_case("origin") {
        return Ok(dolos_core::ChainPoint::Origin);
    }
    if let Some((slot, hash_hex)) = s.split_once(':') {
        let slot: u64 = slot.parse().map_err(|e| anyhow::anyhow!("bad slot: {e}"))?;
        let hash_bytes =
            hex::decode(hash_hex).map_err(|e| anyhow::anyhow!("bad hash hex: {e}"))?;
        if hash_bytes.len() != 32 {
            anyhow::bail!("hash must be 32 bytes; got {}", hash_bytes.len());
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&hash_bytes);
        return Ok(dolos_core::ChainPoint::Specific(
            slot,
            dolos_core::BlockHash::from(arr),
        ));
    }
    let slot: u64 = s.parse().map_err(|e| anyhow::anyhow!("bad slot: {e}"))?;
    Ok(dolos_core::ChainPoint::Slot(slot))
}

async fn remove_subscription(
    State(state): State<AdminState>,
    Path(id): Path<SubscriptionId>,
) -> impl IntoResponse {
    if state.replicator.remove(id).await {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

#[derive(Clone)]
struct HealthState {
    started_at: SystemTime,
    indexers: Vec<String>,
    replicator: Arc<Replicator>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    uptime_secs: u64,
    indexers: Vec<String>,
    replicator: crate::replicator::ReplicatorSummary,
}

async fn handle_health(State(state): State<HealthState>) -> Json<HealthResponse> {
    let uptime_secs = SystemTime::now()
        .duration_since(state.started_at)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let summary = state.replicator.summary().await;
    Json(HealthResponse {
        status: "ok",
        uptime_secs,
        indexers: state.indexers,
        replicator: summary,
    })
}
