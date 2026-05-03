//! `emit` interface — module event emission.
//!
//! Modules CBOR-encode their typed `Change` events and call
//! `emit-event(channel, bytes)`. The host fans these out to
//! the existing CF replication WS without re-encoding.
//!
//! V1 stub: `EventSink` is a typed channel that the
//! subscription lifecycle code drains. The real impl will wire
//! into `mitos-core`'s replication path.

use crate::bindings::EmitHost;
use crate::host_fns::HostState;

/// Sink for emitted events. One per module instance; drained by
/// the replication side.
pub struct EventSink {
    sender: tokio::sync::mpsc::UnboundedSender<EmittedEvent>,
}

#[derive(Debug)]
pub struct EmittedEvent {
    pub module_id: String,
    pub channel: u32,
    pub payload: Vec<u8>,
}

impl EventSink {
    pub fn new() -> (Self, tokio::sync::mpsc::UnboundedReceiver<EmittedEvent>) {
        let (sender, recv) = tokio::sync::mpsc::unbounded_channel();
        (Self { sender }, recv)
    }
}

impl EmitHost for HostState {
    async fn emit_event(&mut self, channel: u32, event: Vec<u8>) -> wasmtime::Result<()> {
        let event = EmittedEvent {
            module_id: self.module_id.clone(),
            channel,
            payload: event,
        };
        if self.emitter.sender.send(event).is_err() {
            // Receiver dropped — supervisor is tearing down.
            // Surface as a trap so the dispatch loop unwinds.
            return Err(wasmtime::Error::msg("event sink closed"));
        }
        Ok(())
    }
}
