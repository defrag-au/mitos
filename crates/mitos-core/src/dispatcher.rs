use std::sync::Arc;

use dolos_core::{Domain, TipEvent, TipSubscription};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::Indexer;

/// Run a single indexer's event loop forever. Owns the indexer + its tip
/// subscription; loops on `next_tip().await`, dispatches each event to the
/// indexer's `handle_event`, logs errors, retries on next event.
///
/// The indexer is wrapped in `Arc<Mutex<_>>` because the bundle keeps a
/// reference for `routes()` while the dispatcher needs `&mut` to call
/// `handle_event`. In practice the `routes()` calls are read-only against
/// shared state inside the indexer, but the trait's `&mut self` on
/// `handle_event` forces the synchronisation primitive at the framework
/// level.
pub async fn run_dispatcher<D, I>(
    indexer: Arc<Mutex<I>>,
    domain: D,
    mut subscription: D::TipSubscription,
) where
    D: Domain,
    I: Indexer<D> + 'static,
{
    let name = {
        let ix = indexer.lock().await;
        ix.name()
    };
    info!(indexer = %name, "dispatcher started");

    loop {
        let event = subscription.next_tip().await;
        if let Err(e) = handle_one(&indexer, &domain, &event).await {
            error!(
                indexer = %name,
                error = %e,
                event = ?event_summary(&event),
                "handle_event failed; continuing — re-delivery on next start \
                 will retry if cursor not yet advanced"
            );
        }
    }
}

async fn handle_one<D, I>(
    indexer: &Arc<Mutex<I>>,
    domain: &D,
    event: &TipEvent,
) -> anyhow::Result<()>
where
    D: Domain,
    I: Indexer<D>,
{
    let mut ix = indexer.lock().await;
    ix.handle_event(domain, event).await
}

fn event_summary(event: &TipEvent) -> &'static str {
    match event {
        TipEvent::Mark(_) => "Mark",
        TipEvent::Apply(_, _) => "Apply",
        TipEvent::Undo(_, _) => "Undo",
    }
}

// Tracing span scaffold for future use — once we wire structured tracing,
// we'll wrap each event's handling in a span tagged with indexer name +
// event kind + chain point.
#[allow(dead_code)]
fn _placeholder_to_silence_unused_warn() {
    let _ = warn!;
}
