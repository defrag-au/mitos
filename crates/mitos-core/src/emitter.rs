//! Per-call emitter: how an indexer's `handle_event` produces change
//! records visible to CF replication consumers.
//!
//! The framework constructs a fresh `Emitter` for each `handle_event`
//! call, stamped with the cursor of the chain event being processed.
//! The indexer calls `emit.apply(change)` / `emit.undo()` for each
//! change record produced. Records flow into the indexer's broadcast
//! channel, where per-consumer WebSocket pumps pick them up and
//! deliver them downstream.
//!
//! Mark events are auto-emitted by the framework; indexers don't need
//! to call `emit.mark()` themselves.

use dolos_core::ChainPoint;
use tokio::sync::broadcast;

/// One change record on an indexer's broadcast channel, before
/// per-consumer scope filtering and wire encoding.
#[derive(Debug, Clone)]
pub enum EmittedRecord<C> {
    Apply { cursor: ChainPoint, change: C },
    Undo { cursor: ChainPoint },
    Mark { cursor: ChainPoint },
}

impl<C> EmittedRecord<C> {
    pub fn cursor(&self) -> &ChainPoint {
        match self {
            Self::Apply { cursor, .. } => cursor,
            Self::Undo { cursor } => cursor,
            Self::Mark { cursor } => cursor,
        }
    }
}

/// Emitter handed to `Indexer::handle_event`. Holds a clone of the
/// indexer's broadcast sender plus the cursor of the current event.
///
/// Sends are non-blocking and ignore lagged-receiver errors — a slow
/// consumer is the consumer's problem (handled by the per-consumer
/// retransmit buffer + reconnect path), not the indexer's.
pub struct Emitter<C: Clone + Send + Sync + 'static> {
    tx: broadcast::Sender<EmittedRecord<C>>,
    cursor: ChainPoint,
}

impl<C: Clone + Send + Sync + 'static> Emitter<C> {
    pub fn new(tx: broadcast::Sender<EmittedRecord<C>>, cursor: ChainPoint) -> Self {
        Self { tx, cursor }
    }

    /// Emit an `Apply` change record at the current cursor.
    pub fn apply(&self, change: C) {
        let _ = self.tx.send(EmittedRecord::Apply {
            cursor: self.cursor.clone(),
            change,
        });
    }

    /// Emit an `Undo` for the current cursor. Indexers call this from
    /// their `handle_event(TipEvent::Undo, ...)` arm to signal that
    /// the block at `cursor` has been rolled back.
    pub fn undo(&self) {
        let _ = self.tx.send(EmittedRecord::Undo {
            cursor: self.cursor.clone(),
        });
    }
}
