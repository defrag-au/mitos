//! Unified module host — single facade in front of the v1 and
//! v2 lifecycle managers.
//!
//! Per-module ABI routing: every `start(id)` reads the
//! manifest's `abi.version_major` and dispatches to either
//! `ModuleHost` (v1) or `ModuleHostV2` (v2). `stop(id)` looks
//! up which host owns the running slot and forwards the call.
//! `list_running` aggregates both.
//!
//! This is the type the bundle binary wires into the admin
//! router — operators don't see v1 vs v2 in the API surface;
//! deploy + restart + delete just work for either ABI.

use std::collections::HashMap;
use std::sync::Arc;

use dolos_core::TipSubscription;
use mitos_data_plane::ChainDataPlane;
use tokio::sync::Mutex;

use crate::host::{InterestRouteError, InterestRouter, ModuleHost, ModuleHostHandle};
use crate::host_v2::ModuleHostV2;
use crate::storage::ModuleStorage;
use crate::{PlatformError, PlatformResult};

/// Map of running modules → ABI version. Lets `stop()` route
/// the call to the right host without re-reading the manifest
/// (fast path; no I/O on the hot stop path).
type AbiRoutes = Arc<Mutex<HashMap<String, u32>>>;

/// Owns both v1 and v2 hosts and forwards lifecycle calls based
/// on each module's declared ABI version.
pub struct UnifiedModuleHost<S, P>
where
    S: TipSubscription + 'static,
    P: ChainDataPlane + Send + Sync + 'static,
{
    storage: ModuleStorage,
    v1: Arc<ModuleHost<S>>,
    v2: Arc<ModuleHostV2<S, P>>,
    routes: AbiRoutes,
}

impl<S, P> UnifiedModuleHost<S, P>
where
    S: TipSubscription + 'static,
    P: ChainDataPlane + Send + Sync + 'static,
{
    pub fn new(
        storage: ModuleStorage,
        v1: Arc<ModuleHost<S>>,
        v2: Arc<ModuleHostV2<S, P>>,
    ) -> Self {
        Self {
            storage,
            v1,
            v2,
            routes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Resume any modules whose manifests already sit on disk.
    /// Called once at host start. Replaces v1's bundle-level
    /// auto-resume logic; routes per-module by manifest ABI.
    pub async fn auto_resume(&self) {
        let module_ids = match self.storage.list_modules() {
            Ok(ids) => ids,
            Err(e) => {
                tracing::error!(error = %e, "auto-resume: list_modules failed");
                return;
            }
        };
        for id in module_ids {
            match self.start(&id).await {
                Ok(()) => tracing::info!(module = %id, "auto-resumed"),
                Err(e) => tracing::error!(
                    module = %id,
                    error = %e,
                    "auto-resume failed; module not running",
                ),
            }
        }
    }

    /// Reads the module's manifest, checks its ABI major
    /// version, dispatches to the right host. Records the
    /// chosen route so subsequent stop()/replace() can find it
    /// without re-reading the manifest.
    pub async fn start(&self, id: &str) -> PlatformResult<()> {
        let manifest = self
            .storage
            .read_manifest(id)?
            .ok_or_else(|| PlatformError::Decode(format!("no manifest for {id}")))?;
        let abi = manifest.abi.version_major;
        match abi {
            1 => self.v1.start(id).await?,
            2 => self.v2.start(id).await?,
            other => {
                return Err(PlatformError::AbiMismatch {
                    wanted_major: 0, // sentinel: "any supported"
                    got_major: other,
                    got_minor: manifest.abi.version_minor,
                });
            }
        }
        self.routes.lock().await.insert(id.to_owned(), abi);
        Ok(())
    }
}

#[async_trait::async_trait]
impl<S, P> ModuleHostHandle for UnifiedModuleHost<S, P>
where
    S: TipSubscription + 'static,
    P: ChainDataPlane + Send + Sync + 'static,
{
    async fn replace(&self, id: &str) -> PlatformResult<()> {
        // `replace` is start-after-stop. The new artifact may
        // have a different ABI than the running one (rare but
        // not impossible — operator switches a module from v1
        // to v2). Read the current manifest fresh so the route
        // reflects the new wasm, not the old one.
        self.start(id).await
    }

    async fn stop(&self, id: &str) -> PlatformResult<()> {
        let abi = self.routes.lock().await.remove(id);
        match abi {
            Some(2) => self.v2.stop(id).await,
            // v1 (1), unknown abi, or no entry in route map —
            // fall through to v1.stop. For genuinely-untracked
            // modules try both so an operator-driven stop after
            // a host restart still cleans up.
            Some(1) => self.v1.stop(id).await,
            _ => {
                let _ = self.v1.stop(id).await;
                self.v2.stop(id).await
            }
        }
    }

    async fn stop_all(&self) {
        self.v1.stop_all().await;
        self.v2.stop_all().await;
        self.routes.lock().await.clear();
    }

    async fn list_running(&self) -> Vec<String> {
        let mut out = self.v1.list().await;
        out.extend(self.v2.list().await);
        out.sort();
        out.dedup();
        out
    }
}

/// Forward `update-interest` frames to v1 modules. v2 modules
/// don't have a wire interest update path yet (see
/// `follower_v2.rs` docs).
#[async_trait::async_trait]
impl<S, P> InterestRouter for UnifiedModuleHost<S, P>
where
    S: TipSubscription + 'static,
    P: ChainDataPlane + Send + Sync + 'static,
{
    async fn route_interest(
        &self,
        module_id: &str,
        op: mitos_protocol::InterestOp,
        items: Vec<mitos_protocol::Interest>,
    ) -> Result<(), InterestRouteError> {
        let abi = self.routes.lock().await.get(module_id).copied();
        match abi {
            Some(2) => {
                // v2 wire-interest path is a follow-up. For
                // now log + drop so the dialer doesn't error
                // on a v2 module that has the WS connected.
                tracing::debug!(
                    module = %module_id,
                    "v2 interest update dropped (wire path TBD)",
                );
                Ok(())
            }
            // v1 (or unknown — try v1 by default since the
            // route map only tracks running modules and a
            // race could see an in-flight start before the
            // map insert).
            _ => self.v1.route_interest(module_id, op, items).await,
        }
    }
}
