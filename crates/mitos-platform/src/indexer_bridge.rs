//! Indexer-target bridge trait.
//!
//! The host's unified subscribe path (`/api/companions/subscribe`)
//! needs two pieces of behaviour for `SubscribeTarget::Indexer`
//! requests that aren't part of the wasm-module flow:
//!
//! 1. **Validation** — does the named indexer exist? Is it
//!    framework-internal (e.g. `none-match`, which can't be
//!    subscribed to from companion modules)?
//! 2. **Dial supervision** — spawn an outbound WS task that pumps
//!    the indexer's broadcast channel to the companion's
//!    `/_internal/replicate` URL.
//!
//! Both responsibilities require access to `mitos-core`'s
//! `IndexerHandle` + `DomainAdapter`, but `mitos-platform` can't
//! depend on `mitos-core` without creating a circular dependency
//! (`mitos-core` already depends on `mitos-platform` for wasm
//! module hosting). This trait is the abstraction `mitos-platform`
//! exposes for the host implementation to plug in.
//!
//! `mitos-core::Bundle` provides the concrete implementation
//! (wired during `enable_modules` or a future
//! `enable_companion_subscribe`); `mitos-platform` only
//! references the trait. See `docs/design/UNIFIED_SUBSCRIBE.md`.

use std::sync::Arc;

use mitos_protocol::SubscribeRequest;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Abstract view of the host's in-tree indexers + the machinery
/// to dial a companion against one. Implementation lives in
/// `mitos-core`.
pub trait IndexerBridge: Send + Sync {
    /// Whether an indexer with this name is registered with the
    /// host bundle.
    fn contains(&self, name: &str) -> bool;

    /// Whether the named indexer is marked internal
    /// (`Indexer::is_internal() == true`). Returns `false` if the
    /// indexer doesn't exist — callers must check `contains` first.
    fn is_internal(&self, name: &str) -> bool;

    /// Reserved names — set of all registered indexer identifiers.
    /// Used by reserved-name enforcement at wasm-module upload
    /// time (a module can't shadow an indexer).
    fn reserved_names(&self) -> Vec<String>;

    /// Spawn the per-companion dial supervisor task for an
    /// `Indexer`-target subscribe request. The implementation:
    ///
    /// - dials the companion's `/_internal/replicate` URL,
    /// - injects a synthetic `ClientMessage::Subscribe` carrying
    ///   the request's `interests` (CBOR-encoded as
    ///   `Vec<Interest>` — the in-tree indexer's `Scope` type),
    /// - hands the WS to `IndexerHandle::run_subscriber` which
    ///   bridges the indexer's broadcast channel to the WS,
    /// - reconnects on disconnect with exponential backoff until
    ///   `cancel` is triggered.
    ///
    /// The returned `JoinHandle` is what the dialer's supervisor
    /// stores so re-registration can cancel the prior task.
    fn spawn_dial(&self, req: SubscribeRequest, cancel: CancellationToken) -> JoinHandle<()>;
}

/// Shared handle type — what callers pass around at the trait
/// boundary. Cheap to clone.
pub type IndexerBridgeHandle = Arc<dyn IndexerBridge>;
