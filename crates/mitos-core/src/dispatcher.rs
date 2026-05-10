use std::sync::Arc;

use dolos::adapters::DomainAdapter;
use dolos_core::{TipEvent, TipSubscription};
use tracing::{error, info};

use crate::coordinator::TxClaimCoordinator;
use crate::handle::IndexerHandle;

/// Run a single indexer's event loop forever.
///
/// Loops on `next_tip().await`, dispatches each event to the indexer's
/// `handle_event`, logs errors, retries on next event. Indexers are
/// passed as type-erased `IndexerHandle` trait objects (the `Indexer`
/// trait itself is non-object-safe due to its associated `Scope` and
/// `Change` types; see `handle.rs`).
pub async fn run_dispatcher(
    indexer: Arc<dyn IndexerHandle>,
    domain: DomainAdapter,
    mut subscription: <DomainAdapter as dolos_core::Domain>::TipSubscription,
) {
    let name = indexer.name();
    info!(indexer = %name, "dispatcher started");

    loop {
        let event = subscription.next_tip().await;
        let result = indexer.handle_event(&domain, &event).await;
        if let Err(e) = result {
            error!(
                indexer = %name,
                error = %e,
                event = %event_summary(&event),
                "handle_event failed; continuing — re-delivery on next start \
                 will retry if cursor not yet advanced"
            );
        }
    }
}

fn event_summary(event: &TipEvent) -> &'static str {
    match event {
        TipEvent::Mark(_) => "Mark",
        TipEvent::Apply(_, _) => "Apply",
        TipEvent::Undo(_, _) => "Undo",
    }
}

/// Run the synchronised dispatcher — single task that processes
/// one event across all indexers in lockstep, accumulating
/// movement claims into a shared `TxClaimCoordinator` so the
/// residual `none_match-indexer` can read them and skip
/// already-classified asset transfers.
///
/// This is the entry point for residual-pass mode (see
/// `docs/design/DOMAIN_REFACTOR.md` "Dispatch mechanism"). When
/// `Bundle::enable_residual_pass()` is called, the bundle uses
/// this loop instead of one `run_dispatcher` task per indexer —
/// because the residual pass needs to wait until all specific
/// indexers have classified the tx before it can decide what's
/// unclaimed.
///
/// Per-event flow:
///
/// 1. `coordinator.clear()` — claims are ephemeral per Apply
/// 2. Each specific indexer runs `handle_event` sequentially;
///    its returned claims accumulate in the coordinator (only
///    on Apply events — Undo / Mark have no claim semantics)
/// 3. `none_match-indexer` runs `handle_event` last; it reads
///    the coordinator internally to skip already-classified
///    movements when emitting `Domain::AssetMovement`.
///
/// Errors from individual indexers are logged and the loop
/// continues — same idempotency guarantee as `run_dispatcher`:
/// re-delivery on next start retries if the cursor hasn't
/// advanced.
pub async fn run_synchronized_dispatcher(
    specific_indexers: Vec<Arc<dyn IndexerHandle>>,
    none_match: Arc<dyn IndexerHandle>,
    coordinator: TxClaimCoordinator,
    domain: DomainAdapter,
    mut subscription: <DomainAdapter as dolos_core::Domain>::TipSubscription,
) {
    info!(
        specific_count = specific_indexers.len(),
        residual = %none_match.name(),
        "synchronised dispatcher started"
    );

    loop {
        let event = subscription.next_tip().await;

        // Reset for this Apply. Cheap — just clears the inner
        // `DashMap`. `Undo` and `Mark` get a clean (empty)
        // coordinator handed to them too; they don't accumulate
        // claims (per design, claims are Apply-only).
        coordinator.clear();

        let is_apply = matches!(event, TipEvent::Apply(_, _));

        for ix in &specific_indexers {
            match ix.handle_event(&domain, &event).await {
                Ok(claims) => {
                    if is_apply && !claims.is_empty() {
                        coordinator.add_all(claims);
                    }
                }
                Err(e) => {
                    error!(
                        indexer = %ix.name(),
                        error = %e,
                        event = %event_summary(&event),
                        "specific-indexer handle_event failed; continuing"
                    );
                }
            }
        }

        // Residual pass last — sees the accumulated claim set.
        if let Err(e) = none_match.handle_event(&domain, &event).await {
            error!(
                indexer = %none_match.name(),
                error = %e,
                event = %event_summary(&event),
                "residual handle_event failed; continuing"
            );
        }
    }
}
