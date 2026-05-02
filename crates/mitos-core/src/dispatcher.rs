use std::sync::Arc;

use dolos_core::{Domain, TipEvent, TipSubscription};
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::Indexer;

/// Run a single indexer's event loop forever.
///
/// Loops on `next_tip().await`, dispatches each event to the indexer's
/// `handle_event`, logs errors, retries on next event. Indexers are passed
/// as trait objects behind `Arc<Mutex<...>>` so the bundle can hold the
/// same handle for `routes()` while the dispatcher takes `&mut` for
/// `handle_event`.
pub async fn run_dispatcher<D: Domain>(
    indexer: Arc<Mutex<dyn Indexer<D>>>,
    domain: D,
    mut subscription: D::TipSubscription,
) {
    let name = indexer.lock().await.name();
    info!(indexer = %name, "dispatcher started");

    loop {
        let event = subscription.next_tip().await;
        let result = {
            let mut guard = indexer.lock().await;
            guard.handle_event(&domain, &event).await
        };
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
