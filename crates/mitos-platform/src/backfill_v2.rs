//! Backfill plane — disposable instances for unbounded cold-start work.
//!
//! The two-plane split (`docs/design/COLD_START_TRAP_ISOLATION.md`):
//! a module's **live** dispatch instance is bounded and durable; its
//! **backfill** work (subscribe-time onboard cold-start + recapture
//! rebootstrap) is unbounded and disposable. They must not share a
//! wasm instance, because a backfill trap poisons the wasm component
//! ("cannot enter component instance") and would brick live holder
//! tracking for every collection on the module.
//!
//! This module owns the backfill plane:
//! - [`BackfillFactory`] — everything needed to instantiate a fresh,
//!   disposable driver for one module. Cloneable + `Send + Sync` so it
//!   moves into the follower task, which spins its own backfill
//!   instances for onboard cold-starts without ever touching the live
//!   dispatch instance.
//! - [`run_rebootstrap_pump`] — drives a module's chunked `rebootstrap`
//!   to completion on a disposable instance, with proactive recycling,
//!   reactive re-instantiation, and a progress-aware retry budget.
//!
//! All module state is durable in state-kv (a shared `Arc<Database>`
//! handed out per module by `ModuleStorage::kv_store`) and all output
//! flows through the emit sink, so a fresh backfill instance shares the
//! live instance's state-kv by construction — the same cross-instance
//! visibility the recapture re-instantiate-on-trap path already relies
//! on (`WASM_BUDGET_CHUNKING.md`).

use std::sync::Arc;

use crate::budget::TrapClass;
use crate::driver_v2::DriverV2;
use crate::host_fns::{DataPlaneFacade, emit};
use crate::host_v2::KvFactory;
use crate::registry_v2::{ModuleRegistryV2, ResourceBudget};
use crate::storage::ModuleStorage;
use crate::trap_context::{TrapContextLogger, write_fixture};
use crate::{PlatformError, PlatformResult};

/// Everything needed to instantiate a fresh, disposable backfill
/// instance for one module. Built once in `host_v2::start`, used there
/// for the recapture pump, and cloned into the follower task for
/// onboard cold-starts.
#[derive(Clone)]
pub struct BackfillFactory {
    pub module_id: String,
    pub registry: Arc<ModuleRegistryV2>,
    /// Data plane the disposable instance's host-fns resolve against.
    /// Same `CachingDataPlane` the live instance uses.
    pub caching_plane: Arc<dyn DataPlaneFacade>,
    pub config: Vec<u8>,
    /// The **live** follower's emit sink — backfill snapshots must land
    /// on the same drain task that feeds the emissions store, so a
    /// disposable instance shares the live sink rather than a fresh one.
    pub sink: emit::EventSink,
    pub kv_factory: KvFactory,
    pub storage: ModuleStorage,
    pub budget: ResourceBudget,
}

impl BackfillFactory {
    /// Build a fresh instance + driver. Mirrors what `host_v2::start`
    /// does for the live driver — instantiate → optional `seed_page`
    /// (a shrunk adaptive page carried from a trapped instance) →
    /// `init` (with trap-context capture). An `init` trap writes a
    /// replay fixture and surfaces as `Err`.
    pub async fn instantiate(
        &self,
        seed_page: Option<u32>,
    ) -> PlatformResult<(DriverV2, Arc<TrapContextLogger>)> {
        let kv = (self.kv_factory)(&self.module_id);
        let trap_logger = Arc::new(TrapContextLogger::new(self.caching_plane.clone()));

        let mut instance = self
            .registry
            .instantiate(trap_logger.clone(), kv, self.sink.clone(), self.budget)
            .await?;

        // Carry a shrunk page from a trapped instance into the fresh
        // sizer, before any guest call.
        if let Some(page) = seed_page {
            instance.store.data_mut().adaptive_mut().seed_current(page);
        }

        instance.store.set_fuel(self.budget.init_fuel)?;
        // Clear the per-call OOM flag so an `init` trap classifies
        // against this call only.
        instance.store.data_mut().limiter_mut().reset_call();
        if let Err(e) = instance
            .bindings
            .call_init(&mut instance.store, &self.config)
            .await
        {
            let trap = TrapClass::classify(&e, instance.store.data().limiter().hit_oom());
            let peak_memory = instance.store.data().limiter().peak_memory_bytes();
            let snap = trap_logger.snapshot();
            let path = self.storage.last_trap_path(&self.module_id);
            match write_fixture(&snap, &self.module_id, &path) {
                Ok(()) => tracing::error!(
                    module = %self.module_id,
                    trap = %trap,
                    peak_memory_bytes = peak_memory,
                    fixture = %path.display(),
                    "backfill init trapped; fixture written for local replay (mitos-run --fixture)",
                ),
                Err(write_err) => tracing::warn!(
                    module = %self.module_id,
                    trap = %trap,
                    peak_memory_bytes = peak_memory,
                    error = %write_err,
                    "backfill init trapped; failed to write trap fixture",
                ),
            }
            return Err(PlatformError::Wasmtime(e));
        }

        Ok((DriverV2::new(instance, self.budget), trap_logger))
    }
}

/// Result of a backfill rebootstrap pump.
pub(crate) struct BackfillOutcome {
    /// Total UTxOs ingested across all pages — the real refill count.
    pub ingested: u64,
    /// Host calls (pages) made — telemetry.
    pub steps: u64,
    /// Times a fresh instance was built mid-pump (proactive recycles +
    /// reactive re-instantiations) — telemetry.
    pub rebuilds: u32,
    /// Terminal outcome: `"completed"`, `"partial"`, or `"failed"`.
    pub outcome: &'static str,
}

/// Proactive recycle threshold. wasm linear memory only grows (it never
/// shrinks back to the OS), so a long scan's allocator high-water climbs
/// until it hits the module's declared maximum and OOMs. Recycling the
/// instance — rebuild from the durable cursor with zeroed memory —
/// *before* that wall keeps a large scan flowing without ever tripping
/// the OOM trap. 256 MiB leaves ample headroom under the wasm32 ceiling
/// while keeping recycles infrequent.
const RECYCLE_MEMORY_BYTES: usize = 256 * 1024 * 1024;

/// Defensive bound on host calls — a well-behaved module drains a finite
/// scan; this only guards a module that never returns `done`.
const MAX_STEPS: u64 = 10_000_000;

/// Absolute ceiling on *reactive* (trap-driven) re-instantiations before
/// the pump gives up the round with a partial refill. Proactive memory
/// recycles (below) are NOT counted — only `OutOfFuel`/`OutOfMemory`
/// traps. This guarantees termination: a module that keeps trapping —
/// e.g. a policy whose UTxOs are so asset-dense that even a `MIN_PAGE`
/// (64-ref) page overruns the per-call fuel budget, where the adaptive
/// sizer is already pinned at the floor and cannot shrink further — is
/// "pathological" (see `budget.rs` `MIN_PAGE`) and the host gives up
/// rather than churning forever. Operator `recapture` resumes from the
/// durable cursor, so a partial refill is recoverable. Sized generously
/// so genuine adaptive-sizer convergence (256→128→64) and a large
/// collection that legitimately re-instantiates a handful of times still
/// complete; only sustained trapping aborts.
const MAX_TRAP_REBUILDS: u32 = 64;

/// Drive a module's chunked `rebootstrap` to completion on a
/// **disposable** backfill instance owned by this function. Never
/// touches the caller's live dispatch instance, so a trap here cannot
/// poison live holder tracking.
///
/// Resilience (`COLD_START_TRAP_ISOLATION.md`):
/// - **Proactive recycle** — rebuild before the OOM wall when peak
///   linear memory crosses [`RECYCLE_MEMORY_BYTES`].
/// - **Reactive re-instantiate** — on an `OutOfFuel`/`OutOfMemory` trap,
///   rebuild carrying the shrunk adaptive page and retry.
/// - **Bounded termination** — reactive trap-rebuilds are capped at
///   [`MAX_TRAP_REBUILDS`] (proactive recycles are not counted), so a
///   pathological policy whose page is pinned at the `MIN_PAGE` floor and
///   keeps trapping gives up the round with a partial refill instead of
///   churning forever. (An earlier progress-aware variant reset the cap
///   on any page that ingested > 0; that was unsound — an interleaved
///   light page resets it every cycle, so a sustained floor-trap never
///   terminates. The absolute cap is the guarantee.)
///
/// The disposable instance is dropped on return.
pub(crate) async fn run_rebootstrap_pump(
    factory: &BackfillFactory,
    mode: crate::bindings_v2::RebootstrapMode,
) -> BackfillOutcome {
    let module_id = factory.module_id.as_str();

    let (mut driver, mut trap_logger) = match factory.instantiate(None).await {
        Ok(d) => d,
        Err(e) => {
            tracing::error!(
                module = %module_id,
                error = %e,
                "backfill: initial instantiate failed; refill skipped",
            );
            return BackfillOutcome {
                ingested: 0,
                steps: 0,
                rebuilds: 0,
                outcome: "failed",
            };
        }
    };

    let mut ingested: u64 = 0;
    let mut steps: u64 = 0;
    let mut rebuilds: u32 = 0;
    // Reactive (trap-driven) re-instantiations only — proactive memory
    // recycles are not counted. Never reset: an absolute bound that
    // guarantees the pump terminates even when the page is pinned at the
    // floor and keeps trapping (see `MAX_TRAP_REBUILDS`).
    let mut trap_rebuilds: u32 = 0;

    let outcome = loop {
        match driver.call_rebootstrap(mode).await {
            Ok(Ok(step)) => {
                ingested += step.ingested;
                steps += 1;
                if step.done {
                    break "completed";
                }
                if steps >= MAX_STEPS {
                    tracing::error!(
                        module = %module_id,
                        steps,
                        "backfill: exceeded step cap; aborting",
                    );
                    break "partial";
                }
                // Proactive recycle before the memory wall.
                if driver.peak_memory_bytes() >= RECYCLE_MEMORY_BYTES {
                    let retry_page = driver.adaptive_page_limit();
                    rebuilds += 1;
                    tracing::debug!(
                        module = %module_id,
                        peak_memory_bytes = driver.peak_memory_bytes(),
                        rebuilds,
                        retry_page,
                        "backfill: proactive instance recycle (memory high-water)",
                    );
                    match factory.instantiate(Some(retry_page)).await {
                        Ok((d, tl)) => {
                            driver = d;
                            trap_logger = tl;
                        }
                        Err(e) => {
                            tracing::error!(
                                module = %module_id,
                                error = %e,
                                "backfill: recycle re-instantiate failed; refill may be partial",
                            );
                            break "partial";
                        }
                    }
                }
            }
            Ok(Err(msg)) => {
                tracing::error!(
                    module = %module_id,
                    error = %msg,
                    "backfill: rebootstrap returned an error",
                );
                break "failed";
            }
            Err(e) => {
                let trap = driver.classify_trap(&e);
                match trap {
                    TrapClass::OutOfFuel | TrapClass::OutOfMemory => {
                        // Retryable: the page overran the per-call
                        // budget and `call_rebootstrap` already shrank
                        // the adaptive sizer. Re-instantiate carrying
                        // the smaller page — the fresh instance re-inits
                        // its round from the durable cursor, a clean
                        // restart of the trapped predicate.
                        trap_rebuilds += 1;
                        rebuilds += 1;
                        if trap_rebuilds > MAX_TRAP_REBUILDS {
                            tracing::error!(
                                module = %module_id,
                                trap = %trap,
                                trap_rebuilds,
                                peak_memory_bytes = driver.peak_memory_bytes(),
                                adaptive_page_limit = driver.adaptive_page_limit(),
                                "backfill: exceeded trap-rebuild ceiling (pathological — page \
                                 likely pinned at the floor and still trapping); giving up the \
                                 round, refill is partial. Operator recapture resumes from cursor.",
                            );
                            break "partial";
                        }
                        let retry_page = driver.adaptive_page_limit();
                        tracing::warn!(
                            module = %module_id,
                            trap = %trap,
                            peak_memory_bytes = driver.peak_memory_bytes(),
                            retry_page,
                            trap_rebuilds,
                            "backfill: page trapped; re-instantiating + retrying at a smaller page",
                        );
                        match factory.instantiate(Some(retry_page)).await {
                            Ok((d, tl)) => {
                                driver = d;
                                trap_logger = tl;
                            }
                            Err(re) => {
                                tracing::error!(
                                    module = %module_id,
                                    error = %re,
                                    "backfill: re-instantiate after trap failed; refill may be partial",
                                );
                                break "partial";
                            }
                        }
                    }
                    TrapClass::Timeout | TrapClass::Fault => {
                        // Not resize-retryable. Write a replay fixture
                        // (the onboard path previously wrote none) then
                        // abort — operator recapture is the safety net.
                        let snap = trap_logger.snapshot();
                        let path = factory.storage.last_trap_path(module_id);
                        match write_fixture(&snap, module_id, &path) {
                            Ok(()) => tracing::error!(
                                module = %module_id,
                                trap = %trap,
                                peak_memory_bytes = driver.peak_memory_bytes(),
                                fixture = %path.display(),
                                "backfill: rebootstrap trapped (non-retryable); fixture written; \
                                 refill may be partial",
                            ),
                            Err(write_err) => tracing::error!(
                                module = %module_id,
                                trap = %trap,
                                error = %write_err,
                                "backfill: rebootstrap trapped (non-retryable); failed to write \
                                 fixture; refill may be partial",
                            ),
                        }
                        break "partial";
                    }
                }
            }
        }
    };

    if ingested > 0 {
        tracing::info!(
            module = %module_id,
            utxos_ingested = ingested,
            steps,
            rebuilds,
            adaptive_page_limit = driver.adaptive_page_limit(),
            outcome,
            "backfill: rebootstrap pump complete",
        );
    }

    BackfillOutcome {
        ingested,
        steps,
        rebuilds,
        outcome,
    }
}
