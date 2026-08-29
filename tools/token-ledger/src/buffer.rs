//! The outref buffer — local input resolution.
//!
//! Block bodies reference inputs as `(tx_hash, index)` only; they carry nothing
//! about the consumed output. Walking forward, we buffer every output that
//! holds the watched asset, keyed by its outref; when a later tx spends one we
//! `take` it and know exactly whose balance fell and by how much — no indexer
//! call, no guess.
//!
//! **This buffer is complete by construction**, which is the property the whole
//! attribution rests on: any input carrying the watched asset must have been an
//! output carrying it earlier, and a walk that starts at or before the policy's
//! first mint has necessarily seen that output. Contrast market-ledger, where a
//! listing can predate any floor and a cold buffer silently drops events.
//!
//! Cardinality is the live UTxO set holding the token — tens of thousands at
//! worst, comfortably in memory.

use std::collections::HashMap;

use mitos_chain_walk::decode::OutRef;

/// A buffered output holding the watched asset.
#[derive(Clone, Debug)]
pub struct BufferedOutput {
    /// Full payment address (bech32). The party key.
    ///
    /// Deliberately not the stake address: CSwap collapses all of its pools
    /// onto one stake credential, so keying parties by stake would merge
    /// distinct pools into a single holder. The stake part is carried
    /// alongside for grouping, never as identity.
    pub address: String,
    pub stake: Option<String>,
    /// Quantity of the watched asset in this output.
    pub qty: i64,
}

#[derive(Default, Clone)]
pub struct OutrefBuffer {
    map: HashMap<OutRef, BufferedOutput>,
}

impl OutrefBuffer {
    pub fn insert(&mut self, oref: OutRef, out: BufferedOutput) {
        self.map.insert(oref, out);
    }

    /// Take (and evict) a spent output, if it held the watched asset.
    pub fn take(&mut self, oref: &OutRef) -> Option<BufferedOutput> {
        self.map.remove(oref)
    }

    /// Live UTxOs holding the token.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn entries(&self) -> impl Iterator<Item = (&OutRef, &BufferedOutput)> {
        self.map.iter()
    }

    /// Total watched-asset quantity currently live.
    ///
    /// At tip this must equal circulating supply, which is the walk's
    /// end-to-end reconciliation check.
    pub fn total_qty(&self) -> i128 {
        self.map.values().map(|b| b.qty as i128).sum()
    }
}
