//! v2 dispatch event types.
//!
//! These mirror the v2 WIT's event variants
//! (`crates/mitos-platform/wit-v2/world.wit`) but with stronger
//! Rust types: `Hash<28>` instead of `list<u8>` for key hashes,
//! `Hash<32>` for tx/datum hashes, `PolicyId` for policy ids.
//! Conversion to/from the bindgen-generated WIT types happens
//! at the wasm boundary in `mitos-platform`.
//!
//! Owning these host-side gives us:
//! - Compile-time guarantees on hash/policy lengths.
//! - Dolos-driven block decoding that produces these directly,
//!   bypassing one boundary conversion.
//! - Test fixtures (mitos-run) that read/write events without
//!   going through bindgen.

use cardano_assets::PolicyId;
use pallas_primitives::Hash;
use serde::{Deserialize, Serialize};

use crate::types::{ChainPoint, OutputRef, TypedDatum, TypedOutput};

/// Validity interval — TX-level slot bounds. Either bound can
/// be absent (open-ended TX).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidityInterval {
    pub valid_from: Option<u64>,
    pub valid_to: Option<u64>,
}

// ==========================================================
// Per-event payloads. One per WIT variant arm.
// ==========================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProducedEvent {
    pub cursor: ChainPoint,
    pub tx_hash: Hash<32>,
    pub tx_idx: u32,
    pub oref: OutputRef,
    pub output: TypedOutput,
    pub datum: Option<TypedDatum>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConsumedEvent {
    pub cursor: ChainPoint,
    pub consuming_tx_hash: Hash<32>,
    pub consuming_tx_idx: u32,
    pub oref: OutputRef,
    pub prior_output: TypedOutput,
    pub prior_datum: Option<TypedDatum>,
    /// Plutus redeemer bytes. `None` for key-locked inputs.
    pub redeemer: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReferencedEvent {
    pub cursor: ChainPoint,
    pub referencing_tx_hash: Hash<32>,
    pub referencing_tx_idx: u32,
    pub oref: OutputRef,
    pub prior_output: TypedOutput,
    pub prior_datum: Option<TypedDatum>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MintedEvent {
    pub cursor: ChainPoint,
    pub tx_hash: Hash<32>,
    pub tx_idx: u32,
    pub policy: PolicyId,
    pub asset_name: Vec<u8>,
    /// Signed: positive = mint, negative = burn.
    pub quantity_delta: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxContextEvent {
    pub cursor: ChainPoint,
    pub tx_hash: Hash<32>,
    pub tx_idx: u32,
    pub validity_interval: ValidityInterval,
    pub required_signers: Vec<Hash<28>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickEvent {
    pub cursor: ChainPoint,
    pub timestamp: u64,
    pub interval_seconds: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollbackEvent {
    pub to_cursor: ChainPoint,
}

// ==========================================================
// Variants
// ==========================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UtxoEvent {
    /// `tx-context` first within a TX batch — see dispatch
    /// ordering in `MITOS_PLATFORM_V2.md`.
    TxContext(TxContextEvent),
    Referenced(ReferencedEvent),
    Consumed(ConsumedEvent),
    Produced(ProducedEvent),
    Minted(MintedEvent),
}

impl UtxoEvent {
    pub fn cursor(&self) -> &ChainPoint {
        match self {
            UtxoEvent::TxContext(e) => &e.cursor,
            UtxoEvent::Referenced(e) => &e.cursor,
            UtxoEvent::Consumed(e) => &e.cursor,
            UtxoEvent::Produced(e) => &e.cursor,
            UtxoEvent::Minted(e) => &e.cursor,
        }
    }

    pub fn tx_idx(&self) -> u32 {
        match self {
            UtxoEvent::TxContext(e) => e.tx_idx,
            UtxoEvent::Referenced(e) => e.referencing_tx_idx,
            UtxoEvent::Consumed(e) => e.consuming_tx_idx,
            UtxoEvent::Produced(e) => e.tx_idx,
            UtxoEvent::Minted(e) => e.tx_idx,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DispatchEvent {
    Utxo(UtxoEvent),
    Tick(TickEvent),
    Rollback(RollbackEvent),
}

/// All events emitted by one Cardano TX, in dispatch order.
/// Constructed by the block decoder; the platform calls
/// `handle-events` once per non-empty batch.
///
/// Invariant: events are sorted as
/// `tx-context → referenced → consumed → produced → minted`.
#[derive(Debug, Clone, PartialEq)]
pub struct TxEventBatch {
    pub tx_idx: u32,
    pub events: Vec<UtxoEvent>,
}

impl TxEventBatch {
    pub fn new(tx_idx: u32) -> Self {
        Self {
            tx_idx,
            events: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn push(&mut self, event: UtxoEvent) {
        self.events.push(event);
    }
}
