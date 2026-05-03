//! `OutputRef` — typed (tx_hash, output_index) pair.
//!
//! Local equivalent of `dolos_core::TxoRef` and
//! `pallas_primitives::TransactionInput`. We re-define here so
//! the data-plane public surface doesn't transitively pull dolos
//! types — consumers code against `OutputRef`, the `LocalDataPlane`
//! impl translates to `TxoRef` at the boundary.

use pallas_primitives::Hash;
use serde::{Deserialize, Serialize};

/// Reference to a transaction output by `(tx_hash, index)`.
///
/// `tx_hash` is the 32-byte Cardano transaction hash. We use
/// `pallas_primitives::Hash<32>` for the typed form — its serde
/// impl emits hex-string on the wire, which is the convention
/// established elsewhere in the mitos protocol surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OutputRef {
    pub tx_hash: Hash<32>,
    pub index: u32,
}

impl OutputRef {
    pub fn new(tx_hash: Hash<32>, index: u32) -> Self {
        Self { tx_hash, index }
    }

    /// Construct from raw bytes + index. Panics if `tx_hash` isn't
    /// 32 bytes; use `try_from_bytes` for fallible.
    pub fn from_bytes(tx_hash: [u8; 32], index: u32) -> Self {
        Self {
            tx_hash: Hash::from(tx_hash),
            index,
        }
    }
}

impl std::fmt::Display for OutputRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}", hex::encode(self.tx_hash), self.index)
    }
}

/// Convert from dolos's TxoRef (used at the LocalDataPlane
/// boundary). Lossless.
impl From<dolos_core::TxoRef> for OutputRef {
    fn from(r: dolos_core::TxoRef) -> Self {
        Self {
            tx_hash: r.0,
            index: r.1,
        }
    }
}

impl From<OutputRef> for dolos_core::TxoRef {
    fn from(r: OutputRef) -> Self {
        dolos_core::TxoRef(r.tx_hash, r.index)
    }
}
