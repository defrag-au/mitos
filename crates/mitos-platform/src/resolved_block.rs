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

use crate::bindings::{OutputRef, TypedDatum, TypedOutput};

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

    /// Memoised datum resolutions for consumed inputs.
    /// `get-consumed-input-datum(tx_idx, input_idx)` populates
    /// this on first call by fetching the prior output at
    /// `with-datum` decode level via the data plane. Same
    /// `OutputRefKey` shape as `consumed_input_cache` — different
    /// payload type so we don't change the existing
    /// `read_utxos(Lean)` shape of that cache.
    pub(crate) consumed_input_datum_cache: HashMap<OutputRefKey, Option<TypedDatum>>,
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
/// `tx-hash` / `output-count` / `get-output` /
/// `get-consumed-input` / `get-consumed-inputs` /
/// `get-output-datum` without re-decoding.
#[derive(Debug, Clone)]
pub struct TxView {
    /// 32-byte tx hash. Modules emit as event metadata.
    pub tx_hash: Vec<u8>,
    /// Outputs produced by this tx, in declaration order.
    /// Already projected to the WIT shape; host pre-decodes
    /// these once when building the `ResolvedBlock`.
    pub outputs: Vec<crate::bindings::TypedOutput>,
    /// Per-output datum info, parallel to `outputs`. `None` =
    /// output has no datum on chain. `Some(info)` carries the
    /// hash, plus inline bytes if the on-chain datum was
    /// inline (`DatumOption::Data`). Hash-only entries are
    /// resolved on demand via the data plane's witness-datum
    /// state index.
    pub output_datums: Vec<Option<OutputDatumInfo>>,
    /// `OutputRef`s of inputs consumed by this tx, in order.
    /// `get_consumed_input(tx_idx, i)` resolves
    /// `txs[tx_idx].consumed_input_refs[i]`.
    pub consumed_input_refs: Vec<OutputRef>,
}

/// Per-output datum extraction outcome. Inline datums carry
/// their CBOR directly (the on-chain bytes are already in hand
/// during block decode); hash-attached datums record only the
/// hash. The guest sees only the resolved bytes — see
/// `host_fns::block_context::get_output_datum`.
#[derive(Debug, Clone)]
pub struct OutputDatumInfo {
    pub hash: pallas::crypto::hash::Hash<32>,
    /// `Some` when the on-chain datum was inline
    /// (`DatumOption::Data`); the bytes are the original CBOR
    /// of the inner `PlutusData`. `None` for hash-attached
    /// datums — the host resolves these via Dolos's
    /// `DATUM_NS` state index when the guest asks.
    pub inline_bytes: Option<Vec<u8>>,
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
            consumed_input_datum_cache: HashMap::new(),
        }
    }
}
