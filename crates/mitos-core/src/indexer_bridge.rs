//! In-tree indexer bridge — host-side implementation of
//! `mitos_platform::indexer_bridge::IndexerBridge`.
//!
//! Provides the machinery `mitos-platform`'s unified-subscribe
//! handler needs to handle `SubscribeTarget::Indexer { name }`
//! requests: looking up indexer handles by name, gating on
//! `is_internal()`, and spawning the per-companion outbound WS
//! dial loop that bridges the indexer's broadcast channel to the
//! companion's `/_internal/replicate` URL.
//!
//! The dial-loop machinery mirrors `crate::replicator::
//! dial_and_run` — both reuse `IndexerHandle::run_subscriber` as
//! the broadcast-to-WS pump. Difference: the legacy `Replicator`
//! is driven by operator-driven `mitos-admin add` registrations,
//! while this bridge is driven by the companion-initiated
//! handshake on `/api/companions/subscribe`. Once unified
//! subscribe is the only entry point, `Replicator` retires (see
//! `docs/design/UNIFIED_SUBSCRIBE.md` step 6).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dolos::adapters::DomainAdapter;
use mitos_platform::indexer_bridge::IndexerBridge;
use mitos_protocol::{ClientMessage, SubscribeRequest, SubscribeTarget};
use tokio::task::JoinHandle;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, http::header::AUTHORIZATION},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use url::Url;

use crate::auth::AuthToken;
use crate::handle::IndexerHandle;
use crate::replicate::encode_client;
use crate::transport::{TungsteniteWs, WsTransport};

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Host-side `IndexerBridge` impl. Holds the bundle's indexer
/// list (for name lookup + internal-flag check) and the
/// `DomainAdapter` needed by `IndexerHandle::run_subscriber`.
pub struct CoreIndexerBridge {
    indexers: HashMap<String, Arc<dyn IndexerHandle>>,
    domain: DomainAdapter,
    auth: AuthToken,
}

impl CoreIndexerBridge {
    /// Build from the bundle's registered indexers + the host's
    /// `DomainAdapter`. Caller is responsible for ensuring
    /// indexers' names are unique (Bundle enforces this at
    /// registration time today by appending to a Vec, but
    /// reserved-name + collision enforcement is a separate
    /// concern — see `docs/design/UNIFIED_SUBSCRIBE.md` step 4).
    pub fn new(
        indexers: Vec<Arc<dyn IndexerHandle>>,
        domain: DomainAdapter,
        auth: AuthToken,
    ) -> Self {
        let mut map = HashMap::with_capacity(indexers.len());
        for h in indexers {
            map.insert(h.name().to_string(), h);
        }
        Self {
            indexers: map,
            domain,
            auth,
        }
    }

    /// Owned snapshot of registered indexer names. Used by the
    /// bundle to pass into `admin_router_with_host` for reserved-
    /// name enforcement at wasm-module upload time.
    pub fn reserved_names_owned(&self) -> Vec<String> {
        self.indexers.keys().cloned().collect()
    }
}

impl IndexerBridge for CoreIndexerBridge {
    fn contains(&self, name: &str) -> bool {
        self.indexers.contains_key(name)
    }

    fn is_internal(&self, name: &str) -> bool {
        self.indexers
            .get(name)
            .map(|h| h.is_internal())
            .unwrap_or(false)
    }

    fn reserved_names(&self) -> Vec<String> {
        self.indexers.keys().cloned().collect()
    }

    fn spawn_dial(
        &self,
        req: SubscribeRequest,
        target: SubscribeTarget,
        cancel: CancellationToken,
    ) -> JoinHandle<()> {
        // Validation upstream guarantees the target is
        // `Indexer { name }` and the name exists + is public.
        // Defensive: if we're somehow asked to dial a missing or
        // internal indexer here, log and return a no-op task.
        let indexer_name = target.name().to_string();
        let companion_key = req.companion_key.clone();

        let Some(handle) = self.indexers.get(&indexer_name).cloned() else {
            warn!(
                indexer = %indexer_name,
                companion_key = %companion_key,
                "spawn_dial: indexer not found (validation gap?); no-op task"
            );
            return tokio::spawn(async {});
        };
        if handle.is_internal() {
            warn!(
                indexer = %indexer_name,
                companion_key = %companion_key,
                "spawn_dial: indexer is internal (validation gap?); no-op task"
            );
            return tokio::spawn(async {});
        }

        let domain = self.domain.clone();
        let auth = self.auth.clone();

        tokio::spawn(async move {
            run_indexer_dial(handle, req, target, domain, auth, cancel).await;
        })
    }
}

/// Per-companion dial supervisor for an `Indexer` target. Loops
/// on connect → handover-to-`run_subscriber` → reconnect with
/// backoff, until `cancel` fires.
async fn run_indexer_dial(
    handle: Arc<dyn IndexerHandle>,
    req: SubscribeRequest,
    target: SubscribeTarget,
    domain: DomainAdapter,
    auth: AuthToken,
    cancel: CancellationToken,
) {
    let indexer_name = target.name().to_string();
    let companion_key = req.companion_key.clone();

    // URL template supports both `{key}` and `{target}`
    // substitution — multi-target companions land each
    // target's WS at `/_internal/replicate-<target_name>`.
    let url_str = match req
        .dial_back
        .as_ref()
        .and_then(|d| d.url.clone())
        .map(|u| u.replace("{key}", &companion_key).replace("{target}", &indexer_name))
    {
        Some(u) => u,
        None => {
            error!(
                indexer = %indexer_name,
                companion_key = %companion_key,
                "no dial_back URL configured; indexer dial task exiting"
            );
            return;
        }
    };
    let parsed = match Url::parse(&url_str) {
        Ok(u) => u,
        Err(e) => {
            error!(
                indexer = %indexer_name,
                companion_key = %companion_key,
                url = %url_str,
                error = %e,
                "invalid dial_back URL; indexer dial task exiting"
            );
            return;
        }
    };

    info!(
        indexer = %indexer_name,
        companion_key = %companion_key,
        target = %parsed,
        "indexer dial loop starting"
    );

    let mut backoff = INITIAL_BACKOFF;
    loop {
        if cancel.is_cancelled() {
            return;
        }
        match dial_once(&parsed, &handle, &req, &domain, &auth).await {
            Ok(()) => {
                info!(
                    indexer = %indexer_name,
                    companion_key = %companion_key,
                    "indexer dial loop exited cleanly; redialing"
                );
                backoff = INITIAL_BACKOFF;
            }
            Err(e) => {
                warn!(
                    indexer = %indexer_name,
                    companion_key = %companion_key,
                    target = %parsed,
                    error = %e,
                    backoff_secs = backoff.as_secs(),
                    "indexer dial errored; backing off"
                );
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Single dial attempt: connect → wrap in
/// `TungsteniteWs + InjectFirst(synthetic Subscribe)` →
/// `IndexerHandle::run_subscriber`. Returns when the WS closes
/// or `run_subscriber` errors.
async fn dial_once(
    url: &Url,
    handle: &Arc<dyn IndexerHandle>,
    req: &SubscribeRequest,
    domain: &DomainAdapter,
    auth: &AuthToken,
) -> anyhow::Result<()> {
    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|e| anyhow::anyhow!("build ws request: {e}"))?;
    // Per-companion dial-back auth override takes precedence;
    // otherwise the bundle's `MITOS_AUTH_TOKEN` is the bearer.
    if let Some((header_name, header_value)) = req.dial_back.as_ref().and_then(|dial| {
        dial.auth_header
            .as_deref()
            .zip(dial.auth_value.as_deref())
    }) {
        let name = header_name
            .parse::<tokio_tungstenite::tungstenite::http::HeaderName>()
            .map_err(|e| anyhow::anyhow!("auth header name: {e}"))?;
        let value = header_value
            .parse()
            .map_err(|e| anyhow::anyhow!("auth header value: {e}"))?;
        request.headers_mut().insert(name, value);
    }
    if !request.headers().contains_key(AUTHORIZATION)
        && let Some(token) = auth.as_deref()
    {
        let value = format!("Bearer {token}")
            .parse()
            .map_err(|e| anyhow::anyhow!("default auth header (ascii?): {e}"))?;
        request.headers_mut().insert(AUTHORIZATION, value);
    }

    let (stream, _resp) = connect_async(request)
        .await
        .map_err(|e| anyhow::anyhow!("ws connect: {e}"))?;

    // Build the synthetic Subscribe frame. The indexer's `Scope`
    // is `Vec<Interest>` for both marketplace-indexer and
    // mint-burn-indexer (validated above by the companion).
    // CBOR-encode the companion's declared `interests` and inject
    // it as the first frame `run_subscriber` decodes.
    let scope_bytes = {
        let mut buf = Vec::with_capacity(64);
        ciborium::ser::into_writer(&req.interests, &mut buf)
            .map_err(|e| anyhow::anyhow!("encode scope: {e}"))?;
        buf
    };
    // The wire `cursor` field is already in `mitos_protocol::ChainPoint`
    // (the wire shape) on both sides of the handshake — no
    // conversion through dolos's `ChainPoint` needed. Fresh
    // companions with no persisted cursor get `Origin`.
    let cursor = req
        .resume_from
        .clone()
        .unwrap_or(mitos_protocol::ChainPoint::Origin);
    let subscribe = ClientMessage::Subscribe {
        scope: scope_bytes,
        cursor,
    };
    let frame = encode_client(&subscribe).map_err(|e| anyhow::anyhow!("encode subscribe: {e}"))?;

    let transport: Box<dyn WsTransport> = Box::new(InjectFirst {
        inner: Box::new(TungsteniteWs(stream)),
        injected: Some(frame),
    });

    handle
        .run_subscriber(transport, domain.clone(), None)
        .await
        .map_err(|e| anyhow::anyhow!("run_subscriber: {e}"))
}

/// Same shape as `crate::replicator::InjectFirst`. Re-declared
/// here so the bridge crate doesn't depend on `replicator`'s
/// private items — `replicator` is on the retirement path and
/// shouldn't grow new dependants.
struct InjectFirst {
    inner: Box<dyn WsTransport>,
    injected: Option<Vec<u8>>,
}

#[async_trait::async_trait]
impl WsTransport for InjectFirst {
    async fn recv_binary(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
        if let Some(bytes) = self.injected.take() {
            return Ok(Some(bytes));
        }
        self.inner.recv_binary().await
    }

    async fn send_binary(&mut self, bytes: Vec<u8>) -> anyhow::Result<()> {
        self.inner.send_binary(bytes).await
    }
}
