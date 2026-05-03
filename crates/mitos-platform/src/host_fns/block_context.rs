//! `block-context` interface — `resolved-block` resource
//! method impls.
//!
//! Lazy-resolution policy: `get_consumed_input` first looks up
//! the cache (`ResolvedBlock::consumed_input_cache`); on miss,
//! it issues a single-ref `read_utxos` against the data plane,
//! caches the result for the block's lifetime, returns to the
//! guest. `get_consumed_inputs` does a bulk fetch for one tx.
//!
//! See `MITOS_PLATFORM_V1.md` §"Resolved design questions" #3
//! for the rationale (eager pre-resolution would charge
//! ownership-style indexers hundreds of pointless lookups per
//! block; lazy + memoise is the right default).

use wasmtime::component::Resource;

use crate::bindings::{HostResolvedBlock, TypedOutput};
use crate::host_fns::HostState;
use crate::resolved_block::ResolvedBlock;

impl HostResolvedBlock for HostState {
    async fn slot(&mut self, self_: Resource<ResolvedBlock>) -> wasmtime::Result<u64> {
        Ok(self.table.get(&self_)?.slot)
    }

    async fn tx_count(&mut self, self_: Resource<ResolvedBlock>) -> wasmtime::Result<u32> {
        Ok(self.table.get(&self_)?.tx_count)
    }

    async fn get_consumed_input(
        &mut self,
        _self_: Resource<ResolvedBlock>,
        _tx_idx: u32,
        _input_idx: u32,
    ) -> wasmtime::Result<Option<TypedOutput>> {
        // V1 stub. Real impl:
        // 1. Look up `txs[tx_idx].consumed_input_refs[input_idx]` to get the OutputRef.
        // 2. Check `consumed_input_cache.entry(key)`.
        // 3. On miss: `data_plane.read_utxos(&[ref], DecodeLevel::Lean)`.
        // 4. Memoise + return.
        Ok(None)
    }

    async fn get_consumed_inputs(
        &mut self,
        _self_: Resource<ResolvedBlock>,
        _tx_idx: u32,
    ) -> wasmtime::Result<Vec<Option<TypedOutput>>> {
        // V1 stub. Real impl:
        // 1. Pull all consumed_input_refs for this tx.
        // 2. Partition into hits/misses against the cache.
        // 3. One bulk `read_utxos` for the misses.
        // 4. Merge + memoise + return in original order.
        Ok(Vec::new())
    }

    async fn drop(&mut self, rep: Resource<ResolvedBlock>) -> wasmtime::Result<()> {
        // Resource passed as `borrow<>` — host retains ownership.
        // We still implement drop so wasmtime can wire up the
        // resource lifecycle hook.
        let _ = self.table.delete(rep);
        Ok(())
    }
}
