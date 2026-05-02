//! `Replicator`: outbound WebSocket client driver.
//!
//! Production replication is mitos-as-WS-client → CF-DO-as-WS-server,
//! because only inbound-accepted sockets get DO Hibernation. See
//! `docs/design/CF_REPLICATION.md` § Connection direction.
//!
//! For each registered subscription the `Replicator` runs a tokio
//! task that:
//!
//! 1. Dials the target URL with `tokio_tungstenite::connect_async`.
//! 2. Wraps the resulting stream in a `WsTransport` and hands it to
//!    `IndexerHandle::run_subscriber` — the same protocol logic the
//!    server-side `/replicate/{indexer}` endpoint uses.
//! 3. On disconnect or error, waits with exponential backoff and
//!    reconnects.
//!
//! Subscriptions are kept in an in-memory registry. Phase 4.5+ adds
//! redb persistence so subscriptions survive mitos restart.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dolos::adapters::DomainAdapter;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tracing::{error, info, warn};
use url::Url;

use crate::handle::IndexerHandle;
use crate::replicate::{ClientMessage, encode_client};
use crate::transport::TungsteniteWs;

/// Identifier for one registered subscription. The framework
/// generates these so the registry can list/remove individual
/// subscriptions without the caller having to remember the exact
/// scope bytes.
pub type SubscriptionId = u64;

/// One outbound subscription mitos maintains: which indexer to feed
/// from, which target to dial, what scope (CBOR-encoded for the
/// indexer's `Scope` type) the consumer cares about, and the
/// last-applied cursor the consumer should resume from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub indexer: String,
    pub target_url: String,
    #[serde(with = "serde_bytes")]
    pub scope: Vec<u8>,
    pub cursor: dolos_core::ChainPoint,
}

/// Maintains the registry of active outbound subscriptions plus the
/// per-subscription tasks that drive them.
pub struct Replicator {
    indexers: HashMap<String, Arc<dyn IndexerHandle>>,
    domain: DomainAdapter,
    inner: Arc<Mutex<ReplicatorInner>>,
}

struct ReplicatorInner {
    next_id: SubscriptionId,
    subs: HashMap<SubscriptionId, ActiveSub>,
}

struct ActiveSub {
    sub: Subscription,
    task: JoinHandle<()>,
}

impl Replicator {
    pub fn new(handles: &[Arc<dyn IndexerHandle>], domain: DomainAdapter) -> Self {
        let mut indexers = HashMap::new();
        for h in handles {
            indexers.insert(h.name().to_string(), h.clone());
        }
        Self {
            indexers,
            domain,
            inner: Arc::new(Mutex::new(ReplicatorInner {
                next_id: 1,
                subs: HashMap::new(),
            })),
        }
    }

    /// Register a new outbound subscription. Spawns the dial loop
    /// immediately. Returns the assigned `SubscriptionId`.
    pub async fn add(&self, sub: Subscription) -> anyhow::Result<SubscriptionId> {
        let handle = self
            .indexers
            .get(&sub.indexer)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown indexer: {}", sub.indexer))?;

        let mut inner = self.inner.lock().await;
        let id = inner.next_id;
        inner.next_id += 1;

        let task = tokio::spawn(run_subscription(
            id,
            sub.clone(),
            handle,
            self.domain.clone(),
        ));
        inner.subs.insert(id, ActiveSub { sub, task });
        Ok(id)
    }

    /// Drop a subscription by id. Aborts its task; the connection
    /// will be torn down on the next message boundary.
    pub async fn remove(&self, id: SubscriptionId) -> bool {
        let mut inner = self.inner.lock().await;
        if let Some(active) = inner.subs.remove(&id) {
            active.task.abort();
            true
        } else {
            false
        }
    }

    /// Snapshot of the current registry. Returns id + subscription
    /// for each active entry.
    pub async fn list(&self) -> Vec<(SubscriptionId, Subscription)> {
        let inner = self.inner.lock().await;
        inner
            .subs
            .iter()
            .map(|(id, active)| (*id, active.sub.clone()))
            .collect()
    }
}

/// Per-subscription task: dial → run protocol → reconnect with
/// backoff. Loops forever; aborted by `Replicator::remove`.
async fn run_subscription(
    id: SubscriptionId,
    sub: Subscription,
    handle: Arc<dyn IndexerHandle>,
    domain: DomainAdapter,
) {
    let url = match Url::parse(&sub.target_url) {
        Ok(u) => u,
        Err(e) => {
            error!(id, target = %sub.target_url, error = %e, "invalid subscription URL — task exiting");
            return;
        }
    };

    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(60);

    loop {
        match dial_and_run(&url, &sub, &handle, &domain).await {
            Ok(()) => {
                info!(id, indexer = %sub.indexer, "subscription disconnected cleanly; reconnecting");
                backoff = Duration::from_secs(1);
            }
            Err(e) => {
                warn!(
                    id,
                    indexer = %sub.indexer,
                    target = %sub.target_url,
                    error = %e,
                    backoff_secs = backoff.as_secs(),
                    "subscription error; backing off before reconnect"
                );
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

async fn dial_and_run(
    url: &Url,
    sub: &Subscription,
    handle: &Arc<dyn IndexerHandle>,
    domain: &DomainAdapter,
) -> anyhow::Result<()> {
    let (stream, _resp) = connect_async(url.as_str())
        .await
        .map_err(|e| anyhow::anyhow!("ws connect: {e}"))?;
    info!(target = %url, indexer = %sub.indexer, "outbound ws connected");

    // The DO will send the real `Subscribe` on connect (its scope is
    // implicit from the connection it accepted, but it still
    // includes its last-applied cursor). For prototype use against
    // synthetic peers that don't speak the protocol yet, the
    // `Replicator` injects a synthetic Subscribe as the first frame
    // so `run_subscriber` can run end-to-end without a live DO.
    let subscribe = ClientMessage::Subscribe {
        scope: sub.scope.clone(),
        cursor: sub.cursor.clone(),
    };
    let bytes = encode_client(&subscribe)?;

    let transport: Box<dyn crate::transport::WsTransport> =
        Box::new(InjectFirst {
            inner: Box::new(TungsteniteWs(stream)),
            injected: Some(bytes),
        });

    handle
        .run_subscriber(transport, domain.clone())
        .await
        .map_err(|e| anyhow::anyhow!("run_subscriber: {e}"))
}

/// Wrap a transport so its first received frame is `frame`, with
/// subsequent frames passing through to the underlying transport
/// unchanged. Used to inject a synthetic Subscribe on outbound
/// connections (see `dial_and_run`).
struct InjectFirst {
    inner: Box<dyn crate::transport::WsTransport>,
    injected: Option<Vec<u8>>,
}

#[async_trait::async_trait]
impl crate::transport::WsTransport for InjectFirst {
    async fn recv_binary(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
        if let Some(b) = self.injected.take() {
            return Ok(Some(b));
        }
        self.inner.recv_binary().await
    }

    async fn send_binary(&mut self, bytes: Vec<u8>) -> anyhow::Result<()> {
        self.inner.send_binary(bytes).await
    }
}
