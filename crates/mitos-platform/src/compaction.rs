//! Periodic emissions-log compaction.
//!
//! Runs on an interval (default 1 hour) and compacts every
//! running module's `EmissionsStore`:
//!
//! - **Acked** rows older than 7 days are purged. Consumers
//!   have confirmed delivery; we keep them around briefly for
//!   diagnostic visibility (`mitos-admin emissions list
//!   --status acked`) and then drop.
//! - **Pending** rows older than 24 hours flip to
//!   `Timeout`. Likely the WS dropped before the Ack arrived;
//!   operators decide whether to `mitos-admin emissions-replay`.
//!
//! `Queued`, `Nacked`, and `Timeout` rows are never auto-purged
//! — they're either actionable (Queued waiting for delivery,
//! Nacked needs investigation) or already terminal-with-
//! signal (Timeout). The operator surface
//! (`mitos-admin emissions purge --status nacked,timeout`)
//! handles those manually when an operator decides to.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::host_v2::ModuleHostHandle;
use crate::storage::ModuleStorage;

/// Default sweep interval. Hourly is plenty — the policies
/// operate on day-scale thresholds.
pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(3600);

/// Default `Acked` retention — 7 days.
pub const DEFAULT_ACKED_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 3600);

/// Default `Pending` timeout — 24 hours.
pub const DEFAULT_PENDING_MAX_AGE: Duration = Duration::from_secs(24 * 3600);

/// Spawn the compaction task. Returns a `JoinHandle` the
/// bundle can cancel via the supplied token at shutdown so the
/// next sweep doesn't fire mid-shutdown and race redb close.
pub fn spawn(
    storage: ModuleStorage,
    host: Arc<dyn ModuleHostHandle>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    spawn_with_config(
        storage,
        host,
        cancel,
        DEFAULT_SWEEP_INTERVAL,
        DEFAULT_ACKED_MAX_AGE,
        DEFAULT_PENDING_MAX_AGE,
    )
}

/// As `spawn`, with explicit policy knobs. Tests use shorter
/// intervals + ages.
pub fn spawn_with_config(
    storage: ModuleStorage,
    host: Arc<dyn ModuleHostHandle>,
    cancel: CancellationToken,
    sweep_interval: Duration,
    acked_max_age: Duration,
    pending_max_age: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(sweep_interval);
        // Skip the immediate-fire first tick so we don't
        // sweep on host startup before any rows exist.
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!("emissions compaction task cancelled");
                    return;
                }
                _ = tick.tick() => {
                    sweep_all(&storage, host.as_ref(), acked_max_age, pending_max_age).await;
                }
            }
        }
    })
}

async fn sweep_all(
    storage: &ModuleStorage,
    host: &dyn ModuleHostHandle,
    acked_max_age: Duration,
    pending_max_age: Duration,
) {
    let modules = host.list_running().await;
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    for module_id in modules {
        // EVERY instance directory of the module, not only the current one.
        // Each keeps its own `emissions.redb`, and one an operator stopped —
        // or that a client has moved off — is exactly the store that
        // accumulates `Acked` rows nothing will ever purge.
        let Ok(module) = mitos_lifecycle::ids::ModuleId::parse(&module_id) else {
            continue;
        };
        let instances = match storage.list_instances(&module) {
            Ok(instances) => instances,
            Err(e) => {
                tracing::warn!(
                    module = %module_id,
                    error = %e,
                    "compaction: list_instances failed; skipping this module",
                );
                continue;
            }
        };
        for key in instances {
            let store = match storage.emissions_store_at(&key) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(
                        module = %module_id,
                        instance = %key.dir_name(),
                        error = %e,
                        "compaction: open EmissionsStore failed; skipping",
                    );
                    continue;
                }
            };
            match store.compact(now_secs, acked_max_age.as_secs(), pending_max_age.as_secs()) {
                Ok((timed_out, purged)) if timed_out > 0 || purged > 0 => {
                    tracing::info!(
                        module = %module_id,
                        instance = %key.dir_name(),
                        timed_out_pending = timed_out,
                        purged_acked = purged,
                        "emissions compacted",
                    );
                }
                Ok(_) => {
                    // Quiet path — no rows aged out. Logged at
                    // debug to keep operator timelines clean.
                    tracing::debug!(
                        module = %module_id,
                        instance = %key.dir_name(),
                        "emissions compaction: nothing to do",
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        module = %module_id,
                        instance = %key.dir_name(),
                        error = %e,
                        "emissions compaction failed",
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::emissions::EmissionStatus;
    use crate::host_v2::ModuleHostHandle;
    use crate::host_v2::testing::FakeHost;
    use mitos_lifecycle::ids::{InstanceKey, ModuleId, Sha256Hex};

    const MODULE: &str = "ownership-indexer";

    /// The store that most needs compacting is the one nothing is reading:
    /// an instance an operator stopped, or one a client has moved off,
    /// accumulates `Acked` rows that only this sweep will ever purge. Before
    /// WO-07 it visited the module's CURRENT instance and no other.
    #[tokio::test]
    async fn every_instances_store_is_compacted() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());
        let wasm = b"compaction fixture";
        storage
            .activate(
                &crate::companions::testing::manifest_for(MODULE, wasm),
                wasm,
            )
            .unwrap();
        let current = storage.current_instance(MODULE).unwrap().unwrap();

        let other_wasm = b"a second build";
        let other_manifest = crate::companions::testing::manifest_for(MODULE, other_wasm);
        storage.put_artifact(&other_manifest, other_wasm).unwrap();
        let other = InstanceKey {
            module: ModuleId::parse(MODULE).unwrap(),
            wasm: Sha256Hex::parse(&crate::manifest::sha256_hex(other_wasm)).unwrap(),
            config: None,
        };
        storage.write_instance_marker(&other).unwrap();

        // One long-acked row in each store.
        for key in [&current, &other] {
            let store = storage.emissions_store_at(key).unwrap();
            let id = store
                .append(
                    "policy_a",
                    "ownership.cnft.dev",
                    MODULE,
                    mitos_protocol::ChainPoint::Slot(1),
                    b"payload".to_vec(),
                    Vec::new(),
                    EmissionStatus::Queued,
                    "unix:0",
                )
                .unwrap();
            store
                .update_status(id, EmissionStatus::Acked, "unix:0", None)
                .unwrap();
        }

        let host = FakeHost::new(storage.clone());
        host.replace(MODULE).await.unwrap();
        let week = Duration::from_secs(7 * 24 * 3600);
        sweep_all(&storage, &host, week, week).await;

        for key in [&current, &other] {
            assert!(
                storage
                    .emissions_store_at(key)
                    .unwrap()
                    .list_filtered(|_| true)
                    .unwrap()
                    .is_empty(),
                "{} was not compacted",
                key.dir_name(),
            );
        }
    }
}
