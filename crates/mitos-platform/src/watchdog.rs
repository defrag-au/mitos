//! Module watchdog — revives stopped-but-subscribed modules.
//!
//! A module with registered companions should always have a live
//! follower. Historically two paths violated that silently: a
//! follower task exiting with a dispatch error (supervisor wiring was
//! a "v2.x follow-up"), and a cancelled/failed `start` inside the
//! recapture flow leaving the slot empty (the 2026-06/07
//! holder-distribution outage — the module sat dead for two weeks
//! while subscribes kept "succeeding" and dropping their interest
//! mutations).
//!
//! The watchdog closes the class: every tick it compares the set of
//! modules that *should* be running (companions registered) against
//! truthful liveness (`list_running`, which excludes finished
//! follower tasks) and restarts the difference. After a revive it
//! re-asserts the module's scan-interest from the persisted companion
//! set (`CompanionDialer::reconcile_module_interest`) so a companion
//! that subscribed while the module was dead — whose interest
//! mutation was dropped with `NotRunning` — is picked up instead of
//! staying stranded until the next full host restart.
//!
//! Guards:
//! - skips modules with a recapture or bootstrap in flight (both
//!   drive `start` themselves; racing them would stop the follower
//!   they're about to install);
//! - first tick is delayed past bundle startup so it can't double
//!   `auto_resume`'s work;
//! - restart outcomes land on the event ring (`watchdog_restart`) so
//!   a crash-looping module is visible in `/_admin/events` +
//!   `mitos_events_total`, not just journald.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::dialer::CompanionDialer;
use crate::events::{EventKind, EventRing};
use crate::host_v2::ModuleHostHandle;
use crate::storage::ModuleStorage;

/// Sweep cadence. A dead-but-subscribed module is an outage for its
/// companions, so the loop runs much tighter than compaction; the
/// tick itself is a handful of in-memory snapshots + a storage list,
/// so a short interval is cheap.
pub const DEFAULT_TICK: Duration = Duration::from_secs(60);

/// Delay before the first sweep so the watchdog can't race
/// `auto_resume` / `dialer.start_all()` during bundle startup (a
/// module mid-cold-start with its bootstrap flag not yet written
/// would otherwise look dead and get double-started).
pub const FIRST_TICK_DELAY: Duration = Duration::from_secs(120);

/// Spawn the watchdog task. Returns the `JoinHandle`; the bundle
/// cancels it via `cancel` at shutdown.
pub fn spawn(
    storage: ModuleStorage,
    host: Arc<dyn ModuleHostHandle>,
    dialer: CompanionDialer,
    events: EventRing,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    spawn_with_config(
        storage,
        host,
        dialer,
        events,
        cancel,
        DEFAULT_TICK,
        FIRST_TICK_DELAY,
    )
}

/// As `spawn`, with explicit knobs (tests use shorter intervals).
pub fn spawn_with_config(
    storage: ModuleStorage,
    host: Arc<dyn ModuleHostHandle>,
    dialer: CompanionDialer,
    events: EventRing,
    cancel: CancellationToken,
    tick_interval: Duration,
    first_tick_delay: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let start_at = tokio::time::Instant::now() + first_tick_delay;
        let mut tick = tokio::time::interval_at(start_at, tick_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!("module watchdog cancelled");
                    return;
                }
                _ = tick.tick() => {
                    sweep(&storage, host.as_ref(), &dialer, &events).await;
                }
            }
        }
    })
}

async fn sweep(
    storage: &ModuleStorage,
    host: &dyn ModuleHostHandle,
    dialer: &CompanionDialer,
    events: &EventRing,
) {
    let modules = match storage.list_modules() {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(error = %e, "watchdog: list_modules failed; skipping sweep");
            return;
        }
    };
    let running: HashSet<String> = host.list_running().await.into_iter().collect();
    let busy: HashSet<String> = host
        .recapture_in_flight()
        .await
        .into_iter()
        .chain(host.bootstrap_in_flight().await)
        .collect();

    for id in modules {
        if running.contains(&id) || busy.contains(&id) {
            continue;
        }
        if storage.count_companions(&id) == 0 {
            // Nothing subscribed — a stopped module is a valid idle
            // state, not an outage.
            continue;
        }
        tracing::warn!(
            module = %id,
            "watchdog: module has registered companions but no live \
             follower; restarting"
        );
        let outcome = match host.replace(&id).await {
            Ok(()) => {
                // Re-assert scan-interest from the persisted
                // companion set — subscribes that landed while the
                // module was dead dropped their interest mutation.
                dialer.reconcile_module_interest(&id).await;
                tracing::info!(module = %id, "watchdog: module restarted");
                "restarted"
            }
            Err(e) => {
                tracing::error!(
                    module = %id,
                    error = %e,
                    "watchdog: restart failed; will retry next tick"
                );
                "failed"
            }
        };
        events.record(
            id.as_str(),
            EventKind::WatchdogRestart {
                outcome: outcome.to_owned(),
            },
        );
    }
}
