//! The outref buffer — local input resolution.
//!
//! Block bodies reference inputs as `(tx_hash, index)` only; they don't carry
//! the consumed output's address / value / datum. Walking forward, we buffer
//! every output produced at a watched credential, keyed by its outref; when a
//! later tx spends one, we `take` it and have everything needed to build a
//! resolved `TxInput` — no indexer call. Buffer cardinality ≈ the open book
//! across enabled venues (tens of thousands, in memory).
//!
//! The buffer will be persisted to sqlite alongside the cursor at each
//! checkpoint (next slice) so a resume/rebootstrap reloads the open book and
//! never re-walks — a resume without it would silently drop every event whose
//! listing predates the resume point.

use std::collections::HashMap;

use pallas_primitives::Hash;

use crate::decode::{Asset, OutRef};

/// A buffered watched output — everything a later spend needs to reconstruct a
/// resolved input.
#[derive(Clone)]
pub struct BufferedOutput {
    pub address: String,
    pub lovelace: u64,
    pub assets: Vec<Asset>,
    /// Resolved datum CBOR when we could resolve it at produce time (inline, the
    /// producing tx's witnesses, or jpg metadata). `None` for hash-only listing
    /// datums that aren't revealed until the spend — resolved then from the
    /// SPENDING tx's witnesses via `datum_hash`.
    pub datum_bytes: Option<Vec<u8>>,
    pub datum_hash: Option<Hash<32>>,
    /// Which venue this output routes to (dispatch via `VenueRegistry::decoder`).
    pub venue: String,
}

// Clone: follow mode keeps two buffers — live (tip) and boundary (k-deep) —
// and rebuilds live from a boundary clone on rollback.
#[derive(Default, Clone)]
pub struct OutrefBuffer {
    map: HashMap<OutRef, BufferedOutput>,
}

impl OutrefBuffer {
    /// Buffer a produced watched output.
    pub fn insert(&mut self, oref: OutRef, out: BufferedOutput) {
        self.map.insert(oref, out);
    }

    /// Take (and evict) a spent output, if it was watched.
    pub fn take(&mut self, oref: &OutRef) -> Option<BufferedOutput> {
        self.map.remove(oref)
    }

    /// Open-book size (watched outputs currently un-spent).
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Iterate the buffered outputs (for checkpoint persistence).
    pub fn entries(&self) -> impl Iterator<Item = (&OutRef, &BufferedOutput)> {
        self.map.iter()
    }
}
