//! `resolved-block` resource — per-block context handed to
//! modules via `handle-event`.
//!
//! Lifetime model (proven out by the WIT spike at
//! `spikes/wit-spike/`):
//!
//! - One `ResourceTable` lives the lifetime of the wasmtime
//!   `Store` (i.e. the module instance). Allocated once at
//!   instantiation; reused across blocks.
//! - Per block: `table.push(ResolvedBlock { … })` → call
//!   `handle-event(channel, &handle)` (resource passed as
//!   `borrow`) → `table.delete(handle)` after the call returns.
//! - `borrow<>` semantics mean the guest cannot stash the handle
//!   past dispatch. The Rust borrow checker on the bindgen-
//!   generated guest stub enforces this at compile time.
//!
//! Lazy resolution: `get_consumed_input` triggers a single-ref
//! `read_utxos` against the data plane on first access, caches
//! the result in `consumed_input_cache` for the block's
//! lifetime. `get_consumed_inputs` does a bulk fetch for one
//! tx's inputs in a single data-plane call (amortises the
//! redb read-transaction open cost).

use std::collections::HashMap;

use crate::bindings::{OutputRef, TypedOutput};

/// Host-side per-block state. Stored in the `ResourceTable`
/// behind a `Resource<ResolvedBlock>` handle that the guest
/// receives as `borrow`.
pub struct ResolvedBlock {
    pub slot: u64,
    pub tx_count: u32,

    /// Decoded transactions, indexed by position in the block.
    /// V1 stub — real impl will hold pallas `MultiEraTx`s plus
    /// the input-ref list per tx so `get_consumed_input` knows
    /// which `OutputRef` to resolve.
    #[allow(dead_code)]
    pub(crate) txs: Vec<TxView>,

    /// Memoised consumed-input lookups. Keyed by the `OutputRef`
    /// of the consumed input, not by `(tx_idx, input_idx)` —
    /// re-resolving the same UTxO across txs in the same block
    /// hits the cache.
    #[allow(dead_code)] // wired up when host_fns::block_context lazy-resolution lands
    pub(crate) consumed_input_cache: HashMap<OutputRefKey, Option<TypedOutput>>,
}

/// Hashable key for the consumed-input cache. `OutputRef` from
/// the bindgen has byte slices that don't impl `Hash` directly;
/// we project to a tuple form.
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub(crate) struct OutputRefKey {
    pub tx_hash: Vec<u8>,
    pub index: u32,
}

impl From<&OutputRef> for OutputRefKey {
    fn from(r: &OutputRef) -> Self {
        Self {
            tx_hash: r.tx_hash.clone(),
            index: r.index,
        }
    }
}

/// Per-tx view inside `ResolvedBlock`. Holds enough to answer
/// `tx-count` / `get-consumed-input` / `get-consumed-inputs`
/// without re-decoding.
#[derive(Debug, Clone)]
pub struct TxView {
    /// `OutputRef`s of inputs consumed by this tx, in order.
    /// `get_consumed_input(tx_idx, i)` resolves
    /// `txs[tx_idx].consumed_input_refs[i]`.
    pub consumed_input_refs: Vec<OutputRef>,
}

impl ResolvedBlock {
    /// Construct from a decoded block. V1 stub — accepts the
    /// pre-extracted shape; `MitosBlockSource` does the actual
    /// pallas decode.
    pub fn from_views(slot: u64, txs: Vec<TxView>) -> Self {
        Self {
            slot,
            tx_count: txs.len() as u32,
            txs,
            consumed_input_cache: HashMap::new(),
        }
    }
}
