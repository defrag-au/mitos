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
//! INSTANCES that *should* be running against truthful liveness
//! (`running_instances`, which excludes finished follower tasks) and
//! restarts the difference. Per instance, because a module with two of them
//! reads as "running" while either lives, and the dead one would then stay
//! dead for ever. After a revive it
//! re-asserts the module's scan-interest from the persisted companion
//! set (`CompanionDialer::reconcile_module_interest`) so a companion
//! that subscribed while the module was dead — whose interest
//! mutation was dropped with `NotRunning` — is picked up instead of
//! staying stranded until the next full host restart.
//!
//! "Should be running" is the LIFECYCLE's answer
//! (`LifecycleManager::expects_follower`), not a companion count. Two
//! consequences, both intended:
//!
//! - **An operator-stopped module is no longer revived.** v2 counted
//!   companions, so `DELETE /_admin/modules/{id}` on a SUBSCRIBED
//!   module was undone within one tick — not merely on the next
//!   restart. That is the bug this closes.
//! - **A pinned module with no companions that crashes IS revived.**
//!   v2 treated "nothing subscribed" as a valid idle state and left
//!   it dead. If an operator pinned it, they want it running.
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

use mitos_lifecycle::ids::InstanceKey;
use mitos_lifecycle::instance::BootAction;

use crate::dialer::CompanionDialer;
use crate::events::{EventKind, EventRing};
use crate::host_v2::ModuleHostHandle;
use crate::lifecycle::LifecycleManager;
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
    lifecycle: Arc<LifecycleManager>,
    dialer: CompanionDialer,
    events: EventRing,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    spawn_with_config(
        storage,
        host,
        lifecycle,
        dialer,
        events,
        cancel,
        DEFAULT_TICK,
        FIRST_TICK_DELAY,
    )
}

/// As `spawn`, with explicit knobs (tests use shorter intervals).
#[allow(clippy::too_many_arguments)]
pub fn spawn_with_config(
    storage: ModuleStorage,
    host: Arc<dyn ModuleHostHandle>,
    lifecycle: Arc<LifecycleManager>,
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
                    sweep(&storage, host.as_ref(), &lifecycle, &dialer, &events).await;
                }
            }
        }
    })
}

pub(crate) async fn sweep(
    storage: &ModuleStorage,
    host: &dyn ModuleHostHandle,
    lifecycle: &LifecycleManager,
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
    let busy: HashSet<String> = host
        .recapture_in_flight()
        .await
        .into_iter()
        .chain(host.bootstrap_in_flight().await)
        .collect();

    for id in modules {
        if busy.contains(&id) {
            continue;
        }
        // Per INSTANCE: a module with two instances is "running" as far as
        // `list_running` is concerned when either of them is, so asking that
        // would leave a dead second follower dead for ever.
        let running: HashSet<InstanceKey> = host.running_instances(&id).await.into_iter().collect();
        let instances = match lifecycle.instances(&id) {
            Ok(instances) => instances,
            Err(e) => {
                tracing::warn!(
                    module = %id,
                    error = %e,
                    "watchdog: cannot list this module's instances; leaving it alone",
                );
                continue;
            }
        };

        // ONCE per module per sweep, however many of its instances were
        // revived. `reconcile_module_interest` routes the union to every
        // running instance already, and the dialer's own comment warns that
        // repeating it per companion re-triggers a module's cold-start
        // machinery.
        let mut revived = false;
        for view in instances {
            let key = view.key();
            if running.contains(key) {
                continue;
            }
            // The lifecycle decides, not a companion count. An instance an
            // operator stopped stays stopped — that is the whole fix. An
            // `Unmanaged` one is never started; nothing here made it.
            match view.on_boot() {
                BootAction::StartFollower => {}
                BootAction::LeaveStopped => continue,
            }
            // Everything above is a SNAPSHOT, taken with no lock. The restart
            // itself goes through the lifecycle, which takes the module lock
            // and asks again — see `LifecycleManager::revive` for what an
            // upload does in the gap this would otherwise act on.
            let outcome = match lifecycle.revive(key).await {
                crate::lifecycle::Revival::NotNeeded => continue,
                crate::lifecycle::Revival::Restarted => {
                    revived = true;
                    tracing::warn!(
                        module = %id,
                        instance = %key.dir_name(),
                        "watchdog: instance had no live follower; restarted",
                    );
                    "restarted"
                }
                crate::lifecycle::Revival::Failed(e) => {
                    tracing::error!(
                        module = %id,
                        instance = %key.dir_name(),
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
        if revived {
            // Re-assert scan-interest from the persisted companion set —
            // subscribes that landed while the module was dead dropped their
            // interest mutation.
            dialer.reconcile_module_interest(&id).await;
        }
    }
}
