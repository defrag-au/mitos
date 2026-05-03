//! `block-context` interface — `resolved-block` resource
//! method impls.
//!
//! Lazy-resolution policy: `get_consumed_input` first looks up
//! the cache (`ResolvedBlock::consumed_input_cache`); on miss,
//! it issues a single-ref `read_utxos` against the data plane,
//! caches the result for the block's lifetime, returns to the
//! guest. `get_consumed_inputs` does a bulk fetch for one tx
//! (one redb read transaction, one batched B-tree walk).
//!
//! See `MITOS_PLATFORM_V1.md` §"Resolved design questions" #3
//! for the rationale (eager pre-resolution would charge
//! ownership-style indexers hundreds of pointless lookups per
//! block; lazy + memoise is the right default).

use mitos_data_plane::DecodeLevel;
use wasmtime::component::Resource;

use crate::bindings::{HostResolvedBlock, TypedOutput};
use crate::host_fns::HostState;
use crate::host_fns::chain_data::{from_dp_output, into_dp_ref_owned};
use crate::resolved_block::{OutputRefKey, ResolvedBlock};

impl HostResolvedBlock for HostState {
    async fn slot(&mut self, self_: Resource<ResolvedBlock>) -> wasmtime::Result<u64> {
        Ok(self.table.get(&self_)?.slot)
    }

    async fn tx_count(&mut self, self_: Resource<ResolvedBlock>) -> wasmtime::Result<u32> {
        Ok(self.table.get(&self_)?.tx_count)
    }

    async fn get_consumed_input(
        &mut self,
        self_: Resource<ResolvedBlock>,
        tx_idx: u32,
        input_idx: u32,
    ) -> wasmtime::Result<Option<TypedOutput>> {
        // Phase 1: project the (tx_idx, input_idx) addressing into
        // a concrete OutputRef and cache key, while only borrowing
        // the resource. Bail if either index is out of range.
        let (oref, key) = {
            let block = self.table.get(&self_)?;
            let tx = block
                .txs
                .get(tx_idx as usize)
                .ok_or_else(|| wasmtime::Error::msg(format!("tx_idx {tx_idx} out of range")))?;
            let oref = tx
                .consumed_input_refs
                .get(input_idx as usize)
                .ok_or_else(|| {
                    wasmtime::Error::msg(format!(
                        "input_idx {input_idx} out of range for tx {tx_idx}"
                    ))
                })?
                .clone();
            let key = OutputRefKey::from(&oref);
            (oref, key)
        };

        // Phase 2: fast path — cache hit. Returns the projected
        // WIT shape directly, no data plane round-trip.
        if let Some(cached) = self.table.get(&self_)?.consumed_input_cache.get(&key) {
            return Ok(cached.clone());
        }

        // Phase 3: cache miss — single-ref data plane fetch.
        let dp_ref = into_dp_ref_owned(&oref)?;
        let pairs = self
            .data_plane
            .read_utxos(&[dp_ref], DecodeLevel::Lean)
            .await
            .map_err(|e| wasmtime::Error::msg(e.to_string()))?;
        let resolved = pairs
            .into_iter()
            .next()
            .map(|(_, out)| from_dp_output(out));

        // Phase 4: memoise + return. Even `None` is cached so
        // repeated lookups for missing inputs don't re-hit redb.
        let block = self.table.get_mut(&self_)?;
        block.consumed_input_cache.insert(key, resolved.clone());
        Ok(resolved)
    }

    async fn get_consumed_inputs(
        &mut self,
        self_: Resource<ResolvedBlock>,
        tx_idx: u32,
    ) -> wasmtime::Result<Vec<Option<TypedOutput>>> {
        // Phase 1: collect the tx's input refs + identify which
        // are already cached. Misses go to the data plane in one
        // batched call (amortises the redb read-tx open cost).
        let (orefs, mut results) = {
            let block = self.table.get(&self_)?;
            let tx = block
                .txs
                .get(tx_idx as usize)
                .ok_or_else(|| wasmtime::Error::msg(format!("tx_idx {tx_idx} out of range")))?;
            let orefs = tx.consumed_input_refs.clone();
            let cache = &block.consumed_input_cache;
            let results: Vec<Option<Option<TypedOutput>>> = orefs
                .iter()
                .map(|r| cache.get(&OutputRefKey::from(r)).cloned())
                .collect();
            (orefs, results)
        };

        // Phase 2: assemble the miss-set + parallel index list so
        // we can splice resolved values back into `results`.
        let mut miss_indices = Vec::new();
        let mut miss_refs = Vec::new();
        for (i, slot) in results.iter().enumerate() {
            if slot.is_none() {
                miss_indices.push(i);
                miss_refs.push(into_dp_ref_owned(&orefs[i])?);
            }
        }

        // Phase 3: bulk fetch the misses. The data plane returns
        // `(OutputRef, TypedOutput)` pairs only for hits — order
        // is by hit, not by request, so we re-index by OutputRef.
        if !miss_refs.is_empty() {
            let pairs = self
                .data_plane
                .read_utxos(&miss_refs, DecodeLevel::Lean)
                .await
                .map_err(|e| wasmtime::Error::msg(e.to_string()))?;
            let by_ref: std::collections::HashMap<_, _> = pairs.into_iter().collect();
            for &i in &miss_indices {
                let dp_ref: mitos_data_plane::OutputRef = into_dp_ref_owned(&orefs[i])?;
                let resolved = by_ref.get(&dp_ref).cloned().map(from_dp_output);
                results[i] = Some(resolved);
            }
        }

        // Phase 4: write back to cache + project to WIT shape.
        let block = self.table.get_mut(&self_)?;
        let mut out = Vec::with_capacity(orefs.len());
        for (i, slot) in results.into_iter().enumerate() {
            let resolved = slot.unwrap_or(None);
            block
                .consumed_input_cache
                .insert(OutputRefKey::from(&orefs[i]), resolved.clone());
            out.push(resolved);
        }
        Ok(out)
    }

    async fn drop(&mut self, rep: Resource<ResolvedBlock>) -> wasmtime::Result<()> {
        // Resource passed as `borrow<>` — host retains ownership.
        // We still implement drop so wasmtime can wire up the
        // resource lifecycle hook.
        let _ = self.table.delete(rep);
        Ok(())
    }
}
