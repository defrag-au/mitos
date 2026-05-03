//! `DecodeLevel` — request-side knob controlling how much
//! expensive resolution work the plane performs server-side.
//!
//! Replaces the originally-sketched `FieldMask`. utxorpc's
//! field-by-field mask is the right shape for protobuf-over-gRPC
//! (saves wire bytes + selective decode). Our primary transport
//! is in-process Rust where wire bytes don't exist; the cost
//! that *does* matter is server-side resolution work
//! (witness-set lookups, PlutusData decode, script CBOR decode).
//! Three named levels capture the meaningful axis cleanly.
//!
//! See `MITOS_DATA_PLANE_API.md` "Open design questions" for the
//! full reasoning.

use serde::{Deserialize, Serialize};

/// How much server-side resolution work the plane performs per
/// output. Each level is a strict superset of the previous one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum DecodeLevel {
    /// Address + lovelace + asset multiset. No datum lookup, no
    /// script decode. Cheapest — no extra redb txns beyond the
    /// output fetch itself. Use when scanning many outputs and
    /// only the address / asset fields matter.
    #[default]
    Lean,
    /// Lean + datum resolution. The plane performs the
    /// witness-set lookup for hash-referenced datums and decodes
    /// PlutusData. Costs an extra redb lookup per
    /// hash-referenced datum and a CBOR decode pass.
    WithDatum,
    /// WithDatum + reference script decode. The plane resolves
    /// `script_ref` and produces a typed `TypedScript`. Most
    /// expensive level; use only when the script is genuinely
    /// needed (e.g. building cancel txs that reference the
    /// script).
    Full,
}

impl DecodeLevel {
    /// Does this level require datum resolution?
    pub fn includes_datum(self) -> bool {
        matches!(self, DecodeLevel::WithDatum | DecodeLevel::Full)
    }

    /// Does this level require script decode?
    pub fn includes_script(self) -> bool {
        matches!(self, DecodeLevel::Full)
    }
}
