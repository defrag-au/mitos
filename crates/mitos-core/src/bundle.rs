//! Bundle: owns the domain, registers indexers, runs the framework.
//!
//! Bundle authors compose the framework as:
//!
//! ```ignore
//! let mut bundle = Bundle::new(domain, config, listen_addr);
//! bundle.add_indexer(OwnershipIndexer::new()?);
//! bundle.add_indexer(MarketplaceIndexer::new()?);
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
use crate::coordinator::TxClaimCoordinator;
use crate::handle::{IndexerAdapter, IndexerHandle};
use crate::indexer::Indexer;
use crate::replicate::replicate_router;
use crate::replicator::{ConnState, Replicator, Subscription, SubscriptionId};
use crate::{run_dispatcher, run_synchronized_dispatcher, spawn_sync_pipeline};

/// Reserved name for the residual-pass indexer. The synchronised
/// dispatcher detects an indexer with this name and routes it to
/// the residual tail-step.
const NONE_MATCH_NAME: &str = "none-match";

pub struct Bundle {
    domain: DomainAdapter,
    config: RootConfig,
    listen: SocketAddr,
    data_dir: PathBuf,
    indexers: Vec<Arc<dyn IndexerHandle>>,
    /// Filesystem path for wasm-module artifacts. `None` = wasm
    /// hosting disabled; the bundle runs as a pure
    /// statically-composed indexer host (today's behaviour).
    /// `Some(path)` = enable `/_admin/modules/*` + auto-resume
    /// any modules already activated under that path.
    modules_dir: Option<PathBuf>,
    /// Filesystem path to the community-modules source tree
    /// (`mitos/community-modules/`). When set + wasm hosting is
    /// enabled, the bundle walks each `<name>/build/` directory
    /// at startup and activates any pre-built artifact whose
    /// sha differs from the currently-active one. See
    /// `docs/strategy/COMMUNITY_MODULES.md`.
    community_modules_dir: Option<PathBuf>,
    /// When `Some`, residual-pass mode is enabled. `run()` uses
    /// the synchronised dispatcher (single task across all
    /// indexers) instead of one task per indexer, so the residual
    /// `none-match` indexer can read claims accumulated by
    /// specific-domain indexers within the same Apply event. See
    /// `docs/design/DOMAIN_REFACTOR.md`.
    residual_coordinator: Option<TxClaimCoordinator>,
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
            modules_dir: None,
            community_modules_dir: None,
            residual_coordinator: None,
        }
    }

    /// Enable residual-pass mode for `Domain::AssetMovement`
    /// emission.
    ///
    /// Returns the `TxClaimCoordinator` the caller must hand to
    /// `NoneMatchIndexer::new(...)` (registered via the regular
    /// `add_indexer` path). The bundle's `run()` then switches
    /// from per-indexer dispatchers to a single synchronised
    /// dispatcher so the residual indexer can see claims
    /// accumulated by specific-domain indexers for the same
    /// Apply event.
    ///
    /// Calling this method commits the bundle to synchronised
    /// dispatch — every registered indexer goes through the
    /// single-task loop, not just the residual one. This is
    /// load-bearing for correctness: claims and the events that
    /// match them must all be visible against the same per-Apply
    /// coordinator state.
    ///
    /// See `docs/design/DOMAIN_REFACTOR.md`.
    pub fn enable_residual_pass(&mut self) -> TxClaimCoordinator {
        let coord = TxClaimCoordinator::new();
        self.residual_coordinator = Some(coord.clone());
        coord
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

    /// Enable wasm-module hosting. After this is called:
    /// - `/_admin/modules/*` routes are mounted
    /// - any modules already registered under `modules_dir`
    ///   get their followers auto-started on `run` (resume)
    /// - successful uploads via `POST /_admin/modules/{id}`
    ///   start (or replace) a running follower for that module
    ///
    /// Per `MITOS_PLATFORM_DEPLOYMENT.md`: the modules dir is
    /// the canonical artifact storage location, e.g.
    /// `/var/lib/mitos/modules/`. Layout is one subdir per
    /// module id: `<id>/current.wasm`, `<id>/manifest.toml`,
    /// `<id>/cursor.cbor`, `<id>/<sha>.wasm` (rollback target).
    pub fn enable_modules(&mut self, modules_dir: PathBuf) {
        self.modules_dir = Some(modules_dir);
    }

    /// Enable community-module auto-load from a source tree path.
    /// Requires `enable_modules` to also be set (community modules
    /// activate into the same modules_dir). At startup, the bundle
    /// walks `<community_modules_dir>/<name>/build/` for each
    /// subdirectory and calls `ModuleStorage::activate` for any
    /// artifact whose sha differs from the currently-active one.
    ///
    /// Typical value: the `community-modules/` directory at the
    /// root of the mitos source tree. Pre-built artifacts under
    /// each module's `build/` subdir are produced by `mitos-build`
    /// either at release time or as a deploy step.
    ///
    /// See `docs/strategy/COMMUNITY_MODULES.md`.
    pub fn enable_community_modules(&mut self, community_modules_dir: PathBuf) {
        self.community_modules_dir = Some(community_modules_dir);
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
            modules_dir,
            community_modules_dir,
            residual_coordinator,
        } = self;

        let sync_handle = spawn_sync_pipeline(domain.clone(), &config, exit.clone())?;
        info!("chain-sync pipeline spawned");

        let started_at = SystemTime::now();
        let indexer_names: Vec<String> = indexers.iter().map(|h| h.name().to_string()).collect();

        let mut dispatcher_handles: Vec<JoinHandle<()>> = Vec::with_capacity(indexers.len());
        let mut app = axum::Router::new();

        // Bootstrap each indexer (lets them initialise their state
        // and report a starting cursor) and mount their HTTP routes.
        // The dispatcher path that consumes these results then
        // diverges based on whether residual-pass mode is enabled.
        let mut bootstrap_results: Vec<(Arc<dyn IndexerHandle>, dolos_core::ChainPoint)> =
            Vec::with_capacity(indexers.len());
        for ix in &indexers {
            let name = ix.name();
            let from = ix.bootstrap(&domain).await?;
            info!(indexer = %name, ?from, "indexer bootstrapped");
            bootstrap_results.push((ix.clone(), from));
            app = app.nest(&format!("/{name}"), ix.routes());
        }

        if let Some(coordinator) = residual_coordinator {
            // Synchronised dispatch — one task across all indexers,
            // with the residual `none-match` indexer running last
            // per Apply event so it sees the accumulated claim set.
            let mut none_match_opt: Option<Arc<dyn IndexerHandle>> = None;
            let mut specific_indexers: Vec<Arc<dyn IndexerHandle>> = Vec::new();
            let mut starting_cursor: Option<dolos_core::ChainPoint> = None;
            for (ix, from) in bootstrap_results {
                if ix.name() == NONE_MATCH_NAME {
                    none_match_opt = Some(ix);
                } else {
                    if starting_cursor.is_none() {
                        starting_cursor = Some(from);
                    }
                    specific_indexers.push(ix);
                }
            }
            let none_match = none_match_opt.ok_or_else(|| {
                anyhow::anyhow!(
                    "residual pass enabled (Bundle::enable_residual_pass) but no `{NONE_MATCH_NAME}` \
                     indexer was registered — call `bundle.add_indexer(NoneMatchIndexer::new(coord))`"
                )
            })?;

            let from = starting_cursor.unwrap_or(dolos_core::ChainPoint::Origin);
            let subscription = domain
                .watch_tip(Some(from))
                .map_err(|e| anyhow::anyhow!("watch_tip for synchronised dispatch: {e:?}"))?;

            let domain_clone = domain.clone();
            let handle = tokio::spawn(async move {
                run_synchronized_dispatcher(
                    specific_indexers,
                    none_match,
                    coordinator,
                    domain_clone,
                    subscription,
                )
                .await;
            });
            dispatcher_handles.push(handle);
        } else {
            // Per-indexer dispatch — today's default. Each indexer
            // has its own subscription + task; they run in parallel
            // with no cross-indexer coordination.
            for (ix, from) in bootstrap_results {
                let name = ix.name();
                let subscription = domain
                    .watch_tip(Some(from.clone()))
                    .map_err(|e| anyhow::anyhow!("watch_tip for {name}: {e:?}"))?;
                let domain_clone = domain.clone();
                let handle = tokio::spawn(async move {
                    run_dispatcher(ix, domain_clone, subscription).await;
                });
                dispatcher_handles.push(handle);
            }
        }

        let auth = AuthToken::from_env();

        // Mount CF replication test surface (server-accepted WS) and
        // admin endpoints for managing outbound `Replicator`
        // subscriptions (production WS-client direction). Both gated
        // by Bearer-token auth (see auth.rs).
        app = app.merge(replicate_router(&indexers, domain.clone(), auth.clone()));

        let replicator_path = data_dir.join("subscriptions.redb");
        let replicator = Arc::new(
            Replicator::new(&indexers, domain.clone(), &replicator_path, auth.clone()).await?,
        );
        info!(path = %replicator_path.display(), "replicator registry opened");
        app = app.merge(admin_router(replicator.clone(), auth.clone()));

        // Wasm-module hosting (Bundle::enable_modules). Mounts
        // /_admin/modules/* and auto-starts followers for any
        // modules already registered on disk. The host handle
        // is captured outside the scope so the shutdown path
        // can call `stop_all` — without that, follower tasks
        // keep tokio's runtime alive past the bundle's serve
        // future and systemd hits its 90s graceful-shutdown
        // timeout.
        let mut module_host: Option<Arc<dyn mitos_platform::host_v2::ModuleHostHandle>> = None;
        let mut module_compaction_handle: Option<tokio::task::JoinHandle<()>> = None;
        if let Some(modules_dir) = modules_dir.as_ref() {
            std::fs::create_dir_all(modules_dir).map_err(|e| {
                anyhow::anyhow!("creating modules dir {}: {e}", modules_dir.display())
            })?;
            let storage = mitos_platform::storage::ModuleStorage::new(modules_dir.clone());
            let engine = mitos_platform::registry_v2::ModuleRegistryV2::build_engine()
                .map_err(|e| anyhow::anyhow!("module engine: {e}"))?;
            // Single DomainDataPlane instance: v2 wires it as the
            // concrete `Arc<DomainDataPlane<_>>` so the dispatch
            // composer's generic bound resolves without a vtable
            // hop. The `dp` view (`Arc<dyn DataPlaneFacade>`) is
            // what the per-instance host fns see.
            let chain_plane = Arc::new(mitos_platform::host_fns::DomainDataPlane::new(
                domain.clone(),
            ));
            let dp: Arc<dyn mitos_platform::host_fns::DataPlaneFacade> = chain_plane.clone();

            // Subscription factory: each follower gets its own
            // tip-watch. Two layers of fix applied:
            //
            // 1. Resume from persisted cursor when available;
            //    on fresh deploy fall back to the domain's
            //    current chain tip — NOT to None, because
            //    `watch_tip(None)` triggers a full WAL-replay
            //    of the entire retention window (~10k+
            //    historical slots), which is wrong default for
            //    wasm-module followers (backfill should happen
            //    via `read_utxos` data-plane queries, not chain
            //    replay).
            //
            // 2. Use `LagTolerantSubscription` instead of
            //    dolos's `watch_tip` directly. Dolos's
            //    TipSubscription panics on
            //    `broadcast::RecvError::Lagged(_)` via
            //    unwrap; the wrapper handles lag gracefully
            //    by re-fetching missed blocks from the WAL.
            //    See `mitos_platform::lag_tolerant`.
            let domain_for_factory = Arc::new(domain.clone());
            let sub_factory: mitos_platform::host_v2::SubscriptionFactory<
                mitos_platform::lag_tolerant::LagTolerantSubscription<DomainAdapter>,
            > = Arc::new(move |cursor: Option<mitos_platform::ChainPoint>| {
                use dolos_core::StateStore;
                // Convert mitos's ChainPoint back to dolos's at
                // this boundary — `LagTolerantSubscription` works
                // directly against `dolos_core::TipEvent` and its
                // chain-point. mitos's owned type insulates the
                // platform consumers from dolos drift.
                let from = cursor
                    .map(dolos_core::ChainPoint::from)
                    .or_else(|| domain_for_factory.state().read_cursor().ok().flatten());
                mitos_platform::lag_tolerant::LagTolerantSubscription::new(
                    domain_for_factory.clone(),
                    &domain_for_factory.tip_broadcast,
                    from,
                )
                .expect("LagTolerantSubscription::new (WAL iter_blocks failed)")
            });

            // Per-module redb-backed KV — crash-safe across
            // restarts. Each module gets its own `kv.redb`
            // alongside `cursor.redb` + `current.wasm`. Routed
            // through `ModuleStorage::kv_store` so all opens
            // share a single cached `Arc<Database>` handle per
            // module — redb is single-writer-per-process and
            // a second `Database::open` of the same file fails
            // with `Database already open`.
            let storage_for_kv = storage.clone();
            let kv_factory: mitos_platform::host_v2::KvFactory = Arc::new(move |id: &str| {
                match storage_for_kv.kv_store(id, None) {
                    Ok(kv) => mitos_platform::host_fns::state_kv::ModuleKv::Redb(kv),
                    Err(e) => {
                        // Fall back to in-memory rather than refusing
                        // to start. The error logs loudly; cursor
                        // resume still works (cursor lives in its own
                        // file), so the operator can investigate
                        // without taking the host down. In practice
                        // open() failure here means a permissions or
                        // disk-full issue that affects the whole
                        // modules dir.
                        error!(
                            module = %id,
                            error = %e,
                            "redb KV open failed; falling back to in-memory"
                        );
                        mitos_platform::host_fns::state_kv::ModuleKv::new_in_memory()
                    }
                }
            });
            let emitter_factory: mitos_platform::host_v2::EmitterFactory =
                Arc::new(mitos_platform::host_fns::emit::EventSink::new);

            // Single v2 host (the v1 dispatch path was retired —
            // see `mitos/docs/strategy/MITOS_PLATFORM_V2.md`). All
            // production modules now build against ABI 2.
            let budget = mitos_platform::registry_v2::ResourceBudget::default();
            let host = Arc::new(mitos_platform::host_v2::ModuleHostV2::new(
                storage.clone(),
                engine,
                dp.clone(),
                chain_plane,
                sub_factory,
                kv_factory,
                emitter_factory,
                budget,
            ));

            // Reserved-name check: any on-disk module whose id
            // collides with an in-tree indexer's name must abort
            // startup. Loud failure beats silent shadowing —
            // operator deleted-then-re-added the indexer crate
            // and the module needs renaming or removing. See
            // `docs/design/UNIFIED_SUBSCRIBE.md` step 4.
            let reserved: std::collections::HashSet<String> =
                indexers.iter().map(|h| h.name().to_string()).collect();
            match storage.list_modules() {
                Ok(ids) => {
                    let collisions: Vec<String> =
                        ids.into_iter().filter(|id| reserved.contains(id)).collect();
                    if !collisions.is_empty() {
                        anyhow::bail!(
                            "wasm modules on disk shadow reserved in-tree indexer names: {:?}. \
                             Rename or delete these modules before restart (see \
                             docs/design/UNIFIED_SUBSCRIBE.md).",
                            collisions
                        );
                    }
                }
                Err(e) => {
                    error!(error = %e, "reserved-name check: list_modules failed; continuing");
                }
            }

            // Community-module auto-load (if enabled). Walks the
            // configured source tree and activates any pre-built
            // artifact whose sha differs from what's already on
            // disk. Runs BEFORE `auto_resume` so the follower
            // pass picks up newly-activated community modules in
            // the same startup cycle. Reserved-name guard above
            // already protects against community modules
            // colliding with in-tree indexer names.
            //
            // See `docs/strategy/COMMUNITY_MODULES.md`.
            if let Some(cm_dir) = community_modules_dir.as_ref() {
                let activated = crate::community_modules::auto_load(cm_dir, &storage);
                info!(
                    community_modules_dir = %cm_dir.display(),
                    activated_count = activated.len(),
                    activated = ?activated,
                    "community-modules auto-load complete"
                );
            }

            // Auto-resume: walk every manifest on disk and start
            // it. Single bad module is logged and skipped so it
            // can't abort bundle startup.
            host.auto_resume().await;

            // The platform admin router has its own AuthToken
            // type — same shape, same env var, separate crate.
            let platform_auth = mitos_platform::admin::AuthToken::from_env();
            let host_for_admin = host.clone() as Arc<dyn mitos_platform::host_v2::ModuleHostHandle>;

            // Companion dial supervisor: maintains an outbound
            // WS to each registered companion so the host can
            // deliver emissions. `start_all()` redials any
            // companion persisted from a previous run; the
            // companion subscribe handler hands fresh
            // registrations to it as they arrive.
            //
            // Hand the dialer an `Arc<dyn InterestRouter>` cloned
            // off the host so inbound `ClientMessage::Interest`
            // frames get routed into the right module's follower
            // task and call its `update-interest` export.
            let interest_router: Arc<dyn mitos_platform::host_v2::InterestRouter> = host.clone();

            // In-tree indexer bridge — what the unified subscribe
            // handler uses to route `SubscribeTarget::Indexer`
            // requests. Shared between the dialer (for
            // `spawn_dial` of indexer-target dial loops) and the
            // subscribe handler (for `contains` / `is_internal`
            // validation). See `docs/design/UNIFIED_SUBSCRIBE.md`.
            let core_bridge = std::sync::Arc::new(crate::indexer_bridge::CoreIndexerBridge::new(
                indexers.to_vec(),
                domain.clone(),
                auth.clone(),
            ));
            let indexer_bridge: mitos_platform::indexer_bridge::IndexerBridgeHandle =
                core_bridge.clone();

            let dialer = mitos_platform::dialer::CompanionDialer::new(
                storage.clone(),
                platform_auth.clone(),
                Some(interest_router),
            )
            .with_indexer_bridge(indexer_bridge.clone());
            // Wire the dialer into the host so the recapture
            // orchestrator (admin `/_admin/modules/{id}/recapture`)
            // can drive companion-side cleanup before re-running
            // bootstrap. `set_dialer` is `&self` via OnceLock so
            // we can call it after Arc-wrapping the host. See
            // `docs/design/RECAPTURE.md`.
            host.set_dialer(dialer.clone());
            dialer.start_all().await;
            app = app.merge(mitos_platform::companions::companion_router(
                storage.clone(),
                platform_auth.clone(),
                Some(dialer.clone()),
                Some(indexer_bridge),
            ));

            // Pass the reserved-names set so the upload handler
            // rejects modules whose id collides with an in-tree
            // indexer (see `docs/design/UNIFIED_SUBSCRIBE.md`
            // step 4).
            let reserved_names = core_bridge.reserved_names_owned();
            app = app.merge(mitos_platform::admin::admin_router_with_host(
                storage.clone(),
                host_for_admin,
                reserved_names,
                platform_auth,
            ));

            // Periodic emissions-log compaction. Hourly sweep
            // ages out Acked rows (7d) + flips stuck Pending
            // rows to Timeout (24h). See `mitos_platform::compaction`.
            let compaction_handle = mitos_platform::compaction::spawn(
                storage,
                host.clone() as Arc<dyn mitos_platform::host_v2::ModuleHostHandle>,
                exit.clone(),
            );

            module_host = Some(host);
            module_compaction_handle = Some(compaction_handle);
            info!(
                modules_dir = %modules_dir.display(),
                "wasm-module hosting enabled"
            );
        }

        // /health surfaces replicator state — open (no auth) so a
        // status page or LB health check can hit it without
        // credentials. No sensitive data is exposed.
        let health_state = HealthState {
            started_at,
            indexers: indexer_names,
            replicator: replicator.clone(),
        };
        app = app.route("/health", get(handle_health).with_state(health_state));

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

        // Compaction task observes the same `exit` token used
        // for graceful shutdown — already cancelled by now —
        // so it'll have stopped on its own. Await the join
        // handle to confirm cleanup before module followers
        // tear down redb handles.
        if let Some(handle) = module_compaction_handle {
            let _ = handle.await;
        }

        // Stop wasm-module followers cleanly so cursor checkpoints
        // get their final write before tokio's runtime shuts down.
        // Each `stop` cancels its slot's CancellationToken and awaits
        // task termination; loop bounded by however many slots are
        // running.
        if let Some(host) = module_host {
            tracing::info!("stopping wasm-module followers");
            host.stop_all().await;
        }

        Ok(())
    }
}

/// Print a summary of the loaded configuration to stdout without
/// opening the Dolos data stores or spawning anything. Used by
/// `mitos --print-config-only` to validate startup state offline
/// (env vars, paths, persisted subscriptions) — crucially, this
/// does NOT acquire the WAL lock, so it's safe to run while
/// another mitos instance is live against the same data dir.
pub fn print_config_summary(
    config: &RootConfig,
    listen: SocketAddr,
    data_dir: &std::path::Path,
    indexer_names: &[&'static str],
) -> anyhow::Result<()> {
    println!("# mitos config summary");
    println!();
    println!("listen:        {listen}");
    println!("data_dir:      {}", data_dir.display());
    println!("storage.wal:   {:?}", config.storage.wal.path());
    println!("storage.state: {:?}", config.storage.state.path());
    println!();
    println!("indexers ({}):", indexer_names.len());
    for n in indexer_names {
        println!("  - {n}");
    }
    println!();

    let auth = AuthToken::from_env();
    println!(
        "auth:          {}",
        if auth.is_open() { "OPEN" } else { "set" }
    );
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
///   "target_url": "wss://collections-mitos.<acct>.workers.dev/_internal/replicate?policy_id=abc...",
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
        let hash_bytes = hex::decode(hash_hex).map_err(|e| anyhow::anyhow!("bad hash hex: {e}"))?;
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
