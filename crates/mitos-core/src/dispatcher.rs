use std::sync::Arc;

use dolos::adapters::DomainAdapter;
use dolos_core::{TipEvent, TipSubscription};
use tracing::{error, info};

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
