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
//! Subscriptions are persisted to a redb file under the bundle's
//! data directory so they survive mitos restart. The on-disk schema
//! is one table keyed by `SubscriptionId` → CBOR-encoded
//! `Subscription`. The id counter is recorded in a meta table.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::RwLock;
use std::time::{Duration, SystemTime};

use dolos::adapters::DomainAdapter;
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tracing::{error, info, warn};
use url::Url;

use crate::auth::AuthToken;
use crate::handle::IndexerHandle;
use crate::replicate::{ClientMessage, chain_point_to_wire, encode_client};
use crate::transport::TungsteniteWs;

/// redb table holding `SubscriptionId` → CBOR(`Subscription`).
///
/// Bumped from `subscriptions` to `subscriptions_v2` for the
/// Phase 4 trait surgery: per-indexer `Scope` types changed
/// (`OwnershipScope` → `Vec<Interest>`, `()` → `Vec<Interest>`)
/// and the persisted CBOR is no longer decodable. Old rows in
/// the v1 table are silently abandoned — the CF consumer
/// re-subscribes on first reconnect with the new shape. The on-
/// disk file keeps the old table; nothing to delete.
const SUBS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("subscriptions_v2");
/// redb table for meta values (next_id counter).
const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("meta_v2");
const META_NEXT_ID: &str = "next_id";

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
/// per-subscription tasks that drive them. Backed by redb on disk,
/// so subscriptions survive mitos restart.
pub struct Replicator {
    indexers: HashMap<String, Arc<dyn IndexerHandle>>,
    domain: DomainAdapter,
    db: Arc<Database>,
    auth: AuthToken,
    inner: Arc<Mutex<ReplicatorInner>>,
}

struct ReplicatorInner {
    next_id: SubscriptionId,
    subs: HashMap<SubscriptionId, ActiveSub>,
}

struct ActiveSub {
    sub: Subscription,
    state: Arc<RwLock<ConnState>>,
    task: JoinHandle<()>,
}

/// Status reported per-subscription in `Replicator::list` and rolled
/// up in `Replicator::summary`. Updated by the per-subscription
/// dial loop on every state transition.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnStatus {
    /// Task spawned, dial in flight.
    Connecting,
    /// `connect_async` succeeded; `run_subscriber` running.
    Connected,
    /// Clean disconnect from the peer; will reconnect.
    Disconnected,
    /// Dial failed; sleeping `backoff_secs` before next attempt.
    BackingOff,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnState {
    pub status: ConnStatus,
    /// Unix-seconds timestamp of last successful connect, if any.
    pub last_connected_at: Option<u64>,
    /// Most recent failure message, if any.
    pub last_error: Option<String>,
    /// Current backoff duration (only meaningful while
    /// `BackingOff`).
    pub backoff_secs: u64,
}

impl Default for ConnState {
    fn default() -> Self {
        Self {
            status: ConnStatus::Connecting,
            last_connected_at: None,
            last_error: None,
            backoff_secs: 0,
        }
    }
}

/// One-line snapshot of the registry: total subscriptions and
/// counts by status. For the bundle's `/health` endpoint and human
/// observers.
#[derive(Debug, Clone, Serialize)]
pub struct ReplicatorSummary {
    pub total: usize,
    pub connecting: usize,
    pub connected: usize,
    pub disconnected: usize,
    pub backing_off: usize,
}

impl Replicator {
    /// Open (or create) the persistent registry at `db_path` and
    /// re-spawn dial loops for every persisted subscription.
    ///
    /// Async because restoring subscriptions requires acquiring
    /// the inner Mutex from within a tokio runtime context — the
    /// blocking variant panics with `Cannot block the current
    /// thread from within a runtime`.
    pub async fn new(
        handles: &[Arc<dyn IndexerHandle>],
        domain: DomainAdapter,
        db_path: impl AsRef<Path>,
        auth: AuthToken,
    ) -> anyhow::Result<Self> {
        let mut indexers = HashMap::new();
        for h in handles {
            indexers.insert(h.name().to_string(), h.clone());
        }

        let path: PathBuf = db_path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let db = Database::create(&path)
            .map_err(|e| anyhow::anyhow!("open replicator db at {}: {e}", path.display()))?;

        // Make sure tables exist so list/insert calls don't panic
        // on first run.
        let txn = db
            .begin_write()
            .map_err(|e| anyhow::anyhow!("begin_write: {e}"))?;
        {
            let _ = txn
                .open_table(SUBS_TABLE)
                .map_err(|e| anyhow::anyhow!("open subs table: {e}"))?;
            let _ = txn
                .open_table(META_TABLE)
                .map_err(|e| anyhow::anyhow!("open meta table: {e}"))?;
        }
        txn.commit()
            .map_err(|e| anyhow::anyhow!("commit init: {e}"))?;

        let (persisted, next_id) = load_persisted(&db)?;

        let replicator = Self {
            indexers,
            domain,
            db: Arc::new(db),
            auth,
            inner: Arc::new(Mutex::new(ReplicatorInner {
                next_id,
                subs: HashMap::new(),
            })),
        };

        // Re-spawn dial loops for every persisted entry.
        for (id, sub) in persisted {
            replicator.respawn_existing(id, sub).await;
        }

        Ok(replicator)
    }

    /// Register a new outbound subscription. Persists to redb, then
    /// spawns the dial loop. Returns the assigned `SubscriptionId`.
    pub async fn add(&self, sub: Subscription) -> anyhow::Result<SubscriptionId> {
        let handle = self
            .indexers
            .get(&sub.indexer)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown indexer: {}", sub.indexer))?;

        let mut inner = self.inner.lock().await;
        let id = inner.next_id;
        inner.next_id += 1;

        // Persist before spawning the task — if the write fails we
        // don't want a runaway connection without a registry entry.
        write_subscription(&self.db, id, &sub, inner.next_id)?;

        let state = Arc::new(RwLock::new(ConnState::default()));
        let task = tokio::spawn(run_subscription(
            id,
            sub.clone(),
            handle,
            self.domain.clone(),
            self.auth.clone(),
            state.clone(),
        ));
        inner.subs.insert(id, ActiveSub { sub, state, task });
        Ok(id)
    }

    /// Drop a subscription by id. Removes from redb, aborts its
    /// task; the connection will be torn down on the next message
    /// boundary.
    pub async fn remove(&self, id: SubscriptionId) -> bool {
        let mut inner = self.inner.lock().await;
        let removed = inner.subs.remove(&id);
        let _ = delete_subscription(&self.db, id);
        if let Some(active) = removed {
            active.task.abort();
            true
        } else {
            false
        }
    }

    /// Snapshot of the current registry. Returns id + subscription
    /// + current connection state for each active entry.
    pub async fn list(&self) -> Vec<(SubscriptionId, Subscription, ConnState)> {
        let inner = self.inner.lock().await;
        inner
            .subs
            .iter()
            .map(|(id, active)| {
                let state = active.state.read().map(|g| g.clone()).unwrap_or_default();
                (*id, active.sub.clone(), state)
            })
            .collect()
    }

    /// Summary: count of subscriptions by status. Cheap; for use
    /// in the bundle's `/health` endpoint.
    pub async fn summary(&self) -> ReplicatorSummary {
        let inner = self.inner.lock().await;
        let mut s = ReplicatorSummary {
            total: inner.subs.len(),
            connecting: 0,
            connected: 0,
            disconnected: 0,
            backing_off: 0,
        };
        for active in inner.subs.values() {
            let status = active
                .state
                .read()
                .map(|g| g.status.clone())
                .unwrap_or(ConnStatus::Connecting);
            match status {
                ConnStatus::Connecting => s.connecting += 1,
                ConnStatus::Connected => s.connected += 1,
                ConnStatus::Disconnected => s.disconnected += 1,
                ConnStatus::BackingOff => s.backing_off += 1,
            }
        }
        s
    }

    /// Read the persisted subscription registry without spawning
    /// dial loops. Used by the bundle's `--print-config-only` flag
    /// for offline inspection. No `IndexerHandle` lookup is
    /// performed; entries that reference unknown indexers come
    /// through unchanged.
    pub fn list_persisted(
        db_path: impl AsRef<Path>,
    ) -> anyhow::Result<Vec<(SubscriptionId, Subscription)>> {
        let path = db_path.as_ref();
        if !path.exists() {
            return Ok(Vec::new());
        }
        let db = Database::open(path)
            .map_err(|e| anyhow::anyhow!("open replicator db at {}: {e}", path.display()))?;
        let (subs, _next_id) = load_persisted(&db)?;
        Ok(subs)
    }

    /// Look up a registered indexer handle by name. Used by the
    /// admin endpoint to encode JSON scope into CBOR for the named
    /// indexer's `Scope` type before persisting the subscription.
    pub fn indexer_handle(&self, name: &str) -> Option<Arc<dyn IndexerHandle>> {
        self.indexers.get(name).cloned()
    }

    /// Restore a persisted subscription on startup — does not
    /// re-write to redb (it's already there) and does not bump the
    /// id counter. Used only by `new`. Async because we're inside
    /// a tokio runtime when called.
    async fn respawn_existing(&self, id: SubscriptionId, sub: Subscription) {
        let handle = match self.indexers.get(&sub.indexer).cloned() {
            Some(h) => h,
            None => {
                warn!(
                    id,
                    indexer = %sub.indexer,
                    "persisted subscription references unknown indexer; leaving in registry but not dialing"
                );
                return;
            }
        };
        let state = Arc::new(RwLock::new(ConnState::default()));
        let task = tokio::spawn(run_subscription(
            id,
            sub.clone(),
            handle,
            self.domain.clone(),
            self.auth.clone(),
            state.clone(),
        ));
        let mut inner = self.inner.lock().await;
        inner.subs.insert(id, ActiveSub { sub, state, task });
    }
}

fn load_persisted(db: &Database) -> anyhow::Result<(Vec<(SubscriptionId, Subscription)>, u64)> {
    let txn = db
        .begin_read()
        .map_err(|e| anyhow::anyhow!("begin_read: {e}"))?;
    let subs_table = txn
        .open_table(SUBS_TABLE)
        .map_err(|e| anyhow::anyhow!("open subs table: {e}"))?;
    let meta_table = txn
        .open_table(META_TABLE)
        .map_err(|e| anyhow::anyhow!("open meta table: {e}"))?;

    let mut subs: Vec<(SubscriptionId, Subscription)> = Vec::new();
    for entry in subs_table
        .iter()
        .map_err(|e| anyhow::anyhow!("iter subs: {e}"))?
    {
        let (id, value) = entry.map_err(|e| anyhow::anyhow!("iter subs entry: {e}"))?;
        let bytes = value.value();
        match ciborium::from_reader::<Subscription, _>(bytes) {
            Ok(sub) => subs.push((id.value(), sub)),
            Err(e) => warn!(id = id.value(), error = %e, "skipping corrupt subscription"),
        }
    }

    let next_id = meta_table
        .get(META_NEXT_ID)
        .map_err(|e| anyhow::anyhow!("read meta: {e}"))?
        .map(|g| g.value())
        .unwrap_or(1);

    Ok((subs, next_id))
}

fn write_subscription(
    db: &Database,
    id: SubscriptionId,
    sub: &Subscription,
    next_id: u64,
) -> anyhow::Result<()> {
    let mut buf = Vec::with_capacity(128);
    ciborium::into_writer(sub, &mut buf)
        .map_err(|e| anyhow::anyhow!("encode subscription: {e}"))?;

    let txn = db
        .begin_write()
        .map_err(|e| anyhow::anyhow!("begin_write: {e}"))?;
    {
        let mut subs = txn
            .open_table(SUBS_TABLE)
            .map_err(|e| anyhow::anyhow!("open subs table: {e}"))?;
        subs.insert(id, buf.as_slice())
            .map_err(|e| anyhow::anyhow!("insert subscription: {e}"))?;
        let mut meta = txn
            .open_table(META_TABLE)
            .map_err(|e| anyhow::anyhow!("open meta table: {e}"))?;
        meta.insert(META_NEXT_ID, next_id)
            .map_err(|e| anyhow::anyhow!("insert next_id: {e}"))?;
    }
    txn.commit()
        .map_err(|e| anyhow::anyhow!("commit subscription: {e}"))
}

fn delete_subscription(db: &Database, id: SubscriptionId) -> anyhow::Result<()> {
    let txn = db
        .begin_write()
        .map_err(|e| anyhow::anyhow!("begin_write: {e}"))?;
    {
        let mut subs = txn
            .open_table(SUBS_TABLE)
            .map_err(|e| anyhow::anyhow!("open subs table: {e}"))?;
        subs.remove(id)
            .map_err(|e| anyhow::anyhow!("remove subscription: {e}"))?;
    }
    txn.commit()
        .map_err(|e| anyhow::anyhow!("commit delete: {e}"))
}

/// Per-subscription task: dial → run protocol → reconnect with
/// backoff. Loops forever; aborted by `Replicator::remove`.
async fn run_subscription(
    id: SubscriptionId,
    sub: Subscription,
    handle: Arc<dyn IndexerHandle>,
    domain: DomainAdapter,
    auth: AuthToken,
    state: Arc<RwLock<ConnState>>,
) {
    let url = match Url::parse(&sub.target_url) {
        Ok(u) => u,
        Err(e) => {
            error!(id, target = %sub.target_url, error = %e, "invalid subscription URL — task exiting");
            set_state(&state, |s| {
                s.status = ConnStatus::BackingOff;
                s.last_error = Some(format!("invalid URL: {e}"));
                s.backoff_secs = 0;
            });
            return;
        }
    };

    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(60);

    loop {
        set_state(&state, |s| {
            s.status = ConnStatus::Connecting;
            s.backoff_secs = 0;
        });

        match dial_and_run(&url, &sub, &handle, &domain, &auth, &state).await {
            Ok(()) => {
                info!(id, indexer = %sub.indexer, "subscription disconnected cleanly; reconnecting");
                set_state(&state, |s| {
                    s.status = ConnStatus::Disconnected;
                    s.last_error = None;
                });
                backoff = Duration::from_secs(1);
            }
            Err(e) => {
                let err_msg = e.to_string();
                warn!(
                    id,
                    indexer = %sub.indexer,
                    target = %sub.target_url,
                    error = %err_msg,
                    backoff_secs = backoff.as_secs(),
                    "subscription error; backing off before reconnect"
                );
                set_state(&state, |s| {
                    s.status = ConnStatus::BackingOff;
                    s.last_error = Some(err_msg);
                    s.backoff_secs = backoff.as_secs();
                });
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

fn set_state<F: FnOnce(&mut ConnState)>(state: &Arc<RwLock<ConnState>>, f: F) {
    if let Ok(mut g) = state.write() {
        f(&mut g);
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

async fn dial_and_run(
    url: &Url,
    sub: &Subscription,
    handle: &Arc<dyn IndexerHandle>,
    domain: &DomainAdapter,
    auth: &AuthToken,
    state: &Arc<RwLock<ConnState>>,
) -> anyhow::Result<()> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;

    // Build a client request from the URL so we can attach the
    // Authorization header for the upgrade.
    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|e| anyhow::anyhow!("build ws request: {e}"))?;
    if let Some(token) = auth.as_deref() {
        let value = format!("Bearer {token}").parse().map_err(|e| {
            anyhow::anyhow!("invalid auth token (must be ASCII for HTTP header): {e}")
        })?;
        request.headers_mut().insert(AUTHORIZATION, value);
    }

    let (stream, _resp) = connect_async(request)
        .await
        .map_err(|e| anyhow::anyhow!("ws connect: {e}"))?;
    info!(target = %url, indexer = %sub.indexer, "outbound ws connected");
    set_state(state, |s| {
        s.status = ConnStatus::Connected;
        s.last_connected_at = Some(unix_now());
        s.last_error = None;
        s.backoff_secs = 0;
    });

    // The DO will send the real `Subscribe` on connect (its scope is
    // implicit from the connection it accepted, but it still
    // includes its last-applied cursor). For prototype use against
    // synthetic peers that don't speak the protocol yet, the
    // `Replicator` injects a synthetic Subscribe as the first frame
    // so `run_subscriber` can run end-to-end without a live DO.
    let subscribe = ClientMessage::Subscribe {
        scope: sub.scope.clone(),
        cursor: chain_point_to_wire(&sub.cursor),
    };
    let bytes = encode_client(&subscribe).map_err(|e| anyhow::anyhow!("{e}"))?;

    let transport: Box<dyn crate::transport::WsTransport> = Box::new(InjectFirst {
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
