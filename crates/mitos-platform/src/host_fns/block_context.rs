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

use crate::bindings::{HostResolvedBlock, TypedDatum, TypedOutput};
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

    async fn tx_hash(
        &mut self,
        self_: Resource<ResolvedBlock>,
        tx_idx: u32,
    ) -> wasmtime::Result<Vec<u8>> {
        let block = self.table.get(&self_)?;
        let tx = block
            .txs
            .get(tx_idx as usize)
            .ok_or_else(|| wasmtime::Error::msg(format!("tx_idx {tx_idx} out of range")))?;
        Ok(tx.tx_hash.clone())
    }

    async fn output_count(
        &mut self,
        self_: Resource<ResolvedBlock>,
        tx_idx: u32,
    ) -> wasmtime::Result<u32> {
        let block = self.table.get(&self_)?;
        let tx = block
            .txs
            .get(tx_idx as usize)
            .ok_or_else(|| wasmtime::Error::msg(format!("tx_idx {tx_idx} out of range")))?;
        Ok(tx.outputs.len() as u32)
    }

    async fn get_output(
        &mut self,
        self_: Resource<ResolvedBlock>,
        tx_idx: u32,
        output_idx: u32,
    ) -> wasmtime::Result<TypedOutput> {
        let block = self.table.get(&self_)?;
        let tx = block
            .txs
            .get(tx_idx as usize)
            .ok_or_else(|| wasmtime::Error::msg(format!("tx_idx {tx_idx} out of range")))?;
        let output = tx.outputs.get(output_idx as usize).ok_or_else(|| {
            wasmtime::Error::msg(format!(
                "output_idx {output_idx} out of range for tx {tx_idx}"
            ))
        })?;
        Ok(output.clone())
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
        let resolved = pairs.into_iter().next().map(|(_, out)| from_dp_output(out));

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

    async fn get_output_datum(
        &mut self,
        self_: Resource<ResolvedBlock>,
        tx_idx: u32,
        output_idx: u32,
    ) -> wasmtime::Result<Option<TypedDatum>> {
        // STUB — full data-plane datum-resolution wiring is the
        // outstanding piece of WIT extension work tracked in
        // `docs/design/WIT_DATUM_EXTENSION.md` ("What needs
        // implementing host-side"). Today's `LocalDataPlane`
        // hardcodes `datum: None` in `project_output()`; the full
        // path needs (a) inline-datum extraction from the decoded
        // output, (b) same-block witness-set resolution during
        // block decode, (c) cross-block hash → bytes via Dolos's
        // archive (`domain.query().plutus_data(&hash)`).
        //
        // Until that lands, the method is reachable but always
        // returns `None`. Modules that call it (e.g. the planned
        // `jpg-co` indexer in cnft.dev-workers) degrade gracefully
        // — they emit zero events for blocks whose CO outputs
        // need datum resolution. This is fine for an initial
        // deployment because Phase 0 of the JPG CO design path
        // doesn't depend on the module at all (cancel-by-May-23
        // uses Maestro polling).
        //
        // Range-validate to surface bad indices loudly even
        // though we'd return `None` either way; matches the
        // existing `get_output` shape so guests get consistent
        // error messages once the real wiring lands.
        let block = self.table.get(&self_)?;
        let tx = block
            .txs
            .get(tx_idx as usize)
            .ok_or_else(|| wasmtime::Error::msg(format!("tx_idx {tx_idx} out of range")))?;
        let _ = tx.outputs.get(output_idx as usize).ok_or_else(|| {
            wasmtime::Error::msg(format!(
                "output_idx {output_idx} out of range for tx {tx_idx}"
            ))
        })?;
        Ok(None)
    }

    async fn get_consumed_input_datum(
        &mut self,
        self_: Resource<ResolvedBlock>,
        tx_idx: u32,
        input_idx: u32,
    ) -> wasmtime::Result<Option<TypedDatum>> {
        // STUB — see `get_output_datum`. Same outstanding work,
        // applied to consumed-input-side datum resolution. The
        // existing `get_consumed_input` lookup path with its
        // `consumed_input_cache` is the natural extension point
        // (parallel `datum_cache: HashMap<OutputRefKey, Option<TypedDatum>>`
        // on `ResolvedBlock`); the data-plane bump is the
        // remaining lift.
        //
        // Index-validates for consistency with `get_consumed_input`.
        let block = self.table.get(&self_)?;
        let tx = block
            .txs
            .get(tx_idx as usize)
            .ok_or_else(|| wasmtime::Error::msg(format!("tx_idx {tx_idx} out of range")))?;
        let _ = tx
            .consumed_input_refs
            .get(input_idx as usize)
            .ok_or_else(|| {
                wasmtime::Error::msg(format!(
                    "input_idx {input_idx} out of range for tx {tx_idx}"
                ))
            })?;
        Ok(None)
    }

    async fn drop(&mut self, rep: Resource<ResolvedBlock>) -> wasmtime::Result<()> {
        // Resource passed as `borrow<>` — host retains ownership.
        // We still implement drop so wasmtime can wire up the
        // resource lifecycle hook.
        let _ = self.table.delete(rep);
        Ok(())
    }
}
