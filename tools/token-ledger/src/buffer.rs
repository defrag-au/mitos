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
    /// Quantity of each watched unit in this output, keyed by asset-name bytes.
    ///
    /// A `Vec` rather than a map: a single-asset watch has exactly one entry
    /// and an NFT output typically one or two, so the linear scan beats hashing
    /// and the ordering stays stable for the persisted encoding.
    pub units: Vec<(Vec<u8>, i64)>,
    /// From a decoded lock datum: when this position unlocks (ms since epoch).
    ///
    /// Carried on the live UTxO rather than in a side table because a vesting
    /// position *is* a UTxO — it exists exactly as long as the output does, and
    /// spending it is the claim. That makes "what is still locked at tip" a
    /// read of the open set rather than a join against history.
    pub unlock_ts_ms: Option<u64>,
    /// The real owner behind the lock contract, from the same datum. The
    /// contract address is the holder; this is the beneficiary.
    pub owner_pkh: Option<String>,
    /// Raw datum CBOR, kept only for outputs at *unidentified* script
    /// addresses — not pools, not registered lock platforms, not wallets.
    ///
    /// This is the raw material for `probe`: the unclassified band is the
    /// thing the surface most wants shrunk, and the cheapest way to shrink it
    /// is to look at what those contracts actually say. Bounded by the live
    /// script UTxO set, which is small precisely because these are the
    /// addresses we could not name.
    pub datum_cbor: Option<Vec<u8>>,
    /// The output's datum *hash*, when it had one.
    ///
    /// Distinguishes the two ways [`Self::datum_cbor`] can be `None`, which
    /// mean opposite things for a probe: no datum at all (this contract does
    /// not carry state, so it is not a lock), versus a hash whose preimage the
    /// chain has not revealed. A hash-only datum is disclosed by the
    /// *spending* transaction, so for an output that is still unspent it is
    /// genuinely unavailable — an untestable case, not a negative one.
    pub datum_hash: Option<Vec<u8>>,
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

    /// Total watched quantity currently live, summed across units.
    ///
    /// At tip this must equal circulating supply, which is the walk's
    /// end-to-end reconciliation check. Summing across units is the right
    /// aggregate for that check even policy-wide: every unit conserves
    /// independently, so their total conserves too.
    pub fn total_qty(&self) -> i128 {
        self.map
            .values()
            .flat_map(|b| b.units.iter())
            .map(|(_, q)| *q as i128)
            .sum()
    }
}

impl BufferedOutput {
    /// This output's total across every watched unit — what the single-asset
    /// `qty` field used to be, kept as a method so call sites that genuinely
    /// want the aggregate read as though they asked for one.
    pub fn total(&self) -> i64 {
        self.units.iter().map(|(_, q)| *q).sum()
    }
}
