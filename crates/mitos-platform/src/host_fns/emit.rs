//! `emit` interface — module event emission.
//!
//! Modules CBOR-encode their typed `Change` events and call
//! `emit-event(channel, bytes)`. The host fans these out to
//! the existing CF replication WS without re-encoding.
//!
//! V1 stub: `EventSink` is a typed channel that the
//! subscription lifecycle code drains. The real impl will wire
//! into `mitos-core`'s replication path.

use mitos_protocol::ChainPoint as WireChainPoint;

use crate::bindings::EmitHost;
use crate::host_fns::HostState;

/// Convert dolos's `ChainPoint` to the wire shape used in
/// emissions storage + delivered to companions. Inlined here
/// to keep mitos-platform free of the mitos-core dep.
fn to_wire(point: &dolos_core::ChainPoint) -> WireChainPoint {
    match point {
        dolos_core::ChainPoint::Origin => WireChainPoint::Origin,
        dolos_core::ChainPoint::Slot(s) => WireChainPoint::Slot(*s),
        dolos_core::ChainPoint::Specific(s, h) => WireChainPoint::Specific(*s, h.to_string()),
    }
}

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
    /// Chain point at which the module produced this event.
    /// Sourced from `HostState::current_cursor`, which the
    /// driver populates before each dispatch. Stored in wire
    /// shape so downstream (`EmissionsStore`, dialer) can use
    /// it without re-conversion.
    pub chain_point: WireChainPoint,
}

impl EventSink {
    pub fn new() -> (Self, tokio::sync::mpsc::UnboundedReceiver<EmittedEvent>) {
        let (sender, recv) = tokio::sync::mpsc::unbounded_channel();
        (Self { sender }, recv)
    }
}

impl EmitHost for HostState {
    async fn emit_event(&mut self, channel: u32, event: Vec<u8>) -> wasmtime::Result<()> {
        // The driver always sets `current_cursor` before
        // dispatch, so this should be `Some` whenever a module
        // calls emit. If it's None we still emit (so init-time
        // emissions don't silently drop), tagging with Origin.
        let chain_point = self
            .current_cursor
            .as_ref()
            .map(to_wire)
            .unwrap_or(WireChainPoint::Origin);
        let event = EmittedEvent {
            module_id: self.module_id.clone(),
            channel,
            payload: event,
            chain_point,
        };
        if self.emitter.sender.send(event).is_err() {
            // Receiver dropped — supervisor is tearing down.
            // Surface as a trap so the dispatch loop unwinds.
            return Err(wasmtime::Error::msg("event sink closed"));
        }
        Ok(())
    }
}
