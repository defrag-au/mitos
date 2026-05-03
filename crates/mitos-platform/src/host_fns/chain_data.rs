//! `chain-data` interface — proxies into the data plane.
//!
//! Bulk shape only (`read-utxos`). Single-utxo lookups go via
//! the `block-context` resource so they can be lazy-cached per
//! block.

use crate::bindings::{ChainDataHost, DecodeLevel, OutputRef, TypedOutput};
use crate::host_fns::HostState;

impl ChainDataHost for HostState {
    async fn read_utxos(
        &mut self,
        refs: Vec<OutputRef>,
        decode: DecodeLevel,
    ) -> wasmtime::Result<Vec<TypedOutput>> {
        let dp_refs: Vec<mitos_data_plane::OutputRef> =
            refs.iter().map(into_dp_ref).collect();
        let dp_decode = into_dp_decode(decode);
        let outputs = self
            .data_plane
            .read_utxos(&dp_refs, dp_decode)
            .await
            .map_err(|e| wasmtime::Error::msg(e.to_string()))?;
        Ok(outputs.into_iter().map(from_dp_output).collect())
    }
}

fn into_dp_ref(_r: &OutputRef) -> mitos_data_plane::OutputRef {
    todo!("convert WIT OutputRef -> mitos_data_plane::OutputRef")
}

fn into_dp_decode(_d: DecodeLevel) -> mitos_data_plane::DecodeLevel {
    todo!("map WIT DecodeLevel -> mitos_data_plane::DecodeLevel")
}

fn from_dp_output(_o: mitos_data_plane::TypedOutput) -> TypedOutput {
    todo!("project mitos_data_plane::TypedOutput -> WIT TypedOutput")
}
