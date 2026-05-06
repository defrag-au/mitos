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

use crate::host::ModuleHostHandle;
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
        let store = match storage.emissions_store(&module_id) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    module = %module_id,
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
                    timed_out_pending = timed_out,
                    purged_acked = purged,
                    "emissions compacted",
                );
            }
            Ok(_) => {
                // Quiet path — no rows aged out. Logged at
                // debug to keep operator timelines clean.
                tracing::debug!(module = %module_id, "emissions compaction: nothing to do");
            }
            Err(e) => {
                tracing::warn!(
                    module = %module_id,
                    error = %e,
                    "emissions compaction failed",
                );
            }
        }
    }
}
