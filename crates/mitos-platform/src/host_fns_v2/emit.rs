//! v2 `emit` interface — same shape as v1, plumbed against v2
//! bindings. Pushes events through the same `EventSink` v1
//! uses so downstream replication / fan-out is unchanged.

use mitos_data_plane::ChainPoint;
use mitos_protocol::ChainPoint as WireChainPoint;

use crate::bindings_v2::EmitHost;
use crate::host_fns::emit::EmittedEvent;
use crate::host_fns_v2::HostStateV2;

fn to_wire(point: &ChainPoint) -> WireChainPoint {
    match point {
        ChainPoint::Origin => WireChainPoint::Origin,
        ChainPoint::Slot(s) => WireChainPoint::Slot(*s),
        ChainPoint::Specific(s, h) => WireChainPoint::Specific(*s, h.to_string()),
    }
}

impl EmitHost for HostStateV2 {
    async fn emit_event(&mut self, channel: u32, event: Vec<u8>) -> wasmtime::Result<()> {
        self.emit(channel, Vec::new(), event)
    }

    async fn emit_event_keyed(
        &mut self,
        channel: u32,
        partition_key: Vec<u8>,
        event: Vec<u8>,
    ) -> wasmtime::Result<()> {
        self.emit(channel, partition_key, event)
    }
}

impl HostStateV2 {
    fn emit(
        &mut self,
        channel: u32,
        partition_key: Vec<u8>,
        payload: Vec<u8>,
    ) -> wasmtime::Result<()> {
        let chain_point = self
            .current_cursor
            .as_ref()
            .map(to_wire)
            .unwrap_or(WireChainPoint::Origin);
        let event = EmittedEvent {
            module_id: self.module_id.clone(),
            channel,
            payload,
            chain_point,
            partition_key,
        };
        if self.emitter.sender.send(event).is_err() {
            return Err(wasmtime::Error::msg("event sink closed"));
        }
        Ok(())
    }
}
