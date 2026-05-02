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
use std::sync::Arc;

use dolos::adapters::DomainAdapter;
use dolos_core::Domain;
use dolos_core::config::RootConfig;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::handle::{IndexerAdapter, IndexerHandle};
use crate::indexer::Indexer;
use crate::replicate::replicate_router;
use crate::{run_dispatcher, spawn_sync_pipeline};

pub struct Bundle {
    domain: DomainAdapter,
    config: RootConfig,
    listen: SocketAddr,
    indexers: Vec<Arc<dyn IndexerHandle>>,
}

impl Bundle {
    pub fn new(domain: DomainAdapter, config: RootConfig, listen: SocketAddr) -> Self {
        Self {
            domain,
            config,
            listen,
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

    /// Run the bundle: spawn the chain-sync pipeline, bootstrap each
    /// indexer, start its dispatcher, mount HTTP routes plus the CF
    /// replication WebSocket endpoints, and serve until `exit` is
    /// cancelled.
    pub async fn run(self, exit: CancellationToken) -> anyhow::Result<()> {
        let Bundle {
            domain,
            config,
            listen,
            indexers,
        } = self;

        let sync_handle = spawn_sync_pipeline(domain.clone(), &config, exit.clone())?;
        info!("chain-sync pipeline spawned");

        let mut dispatcher_handles: Vec<JoinHandle<()>> = Vec::with_capacity(indexers.len());
        let mut app = axum::Router::new().route("/health", axum::routing::get(handle_health));

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

        // Mount CF replication endpoints on the same listener.
        app = app.merge(replicate_router(&indexers, domain.clone()));

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

async fn handle_health() -> &'static str {
    // Phase 4.5+: aggregate per-indexer cursor lag.
    "ok"
}
