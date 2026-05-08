//! Shared `emit` channel — typed sink for module emissions.
//!
//! Modules CBOR-encode their typed `Change` events and call
//! `emit-event(channel, bytes)` (see `host_fns_v2::emit`). The
//! host fans these out to the dialer's per-companion delivery
//! pipeline without re-encoding. This module only carries the
//! channel types; the v2 `EmitHost` impl lives in
//! `host_fns_v2/emit.rs`.

use mitos_protocol::ChainPoint as WireChainPoint;

/// Sink for emitted events. One per module instance; drained by
/// the replication side.
pub struct EventSink {
    // Crate-visible so the v2 host fns can push events through
    // it without duplicating the emit channel plumbing.
    pub(crate) sender: tokio::sync::mpsc::UnboundedSender<EmittedEvent>,
}

#[derive(Debug)]
pub struct EmittedEvent {
    pub module_id: String,
    pub channel: u32,
    pub payload: Vec<u8>,
    /// Chain point at which the module produced this event.
    /// Sourced from `HostStateV2::current_cursor`, which the
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
