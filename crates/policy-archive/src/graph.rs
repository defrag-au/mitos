//! The MOVEMENT GRAPH — a policy's whole history reduced to who moved units
//! to whom.
//!
//! # Why this exists as an artifact rather than a query
//!
//! The feed is ROWS and a graph is EDGES, and paging rows to draw a graph does
//! not work: ClayNation is 334,831 transactions, which is ~670 pages at about
//! a second each through the Worker. Eleven minutes of stutter for a picture.
//!
//! So the reduction is done ONCE, on the box, from the rollup — one sorted,
//! folded file per policy — and shipped as a single object a browser fetches
//! and then holds. Measured on ClayNation 2026-09-06:
//!
//! | keyed by | parties | edges | gzipped |
//! |---|---|---|---|
//! | payment address | 262,427 | 308,674 | ~20 MB |
//! | **stake address** | **35,345** | **75,353** | **~2 MB** |
//!
//! **The party dictionary is the cost, not the edges** — 27.3 MB against
//! 3.7 MB payment-keyed. That is why [`PartyKey`] exists and why `Stake` is
//! the default: it is worth about ten times, and it is the difference between
//! an artifact you fetch and one you cannot.
//!
//! # The caveat that travels with `PartyKey::Stake`
//!
//! Keying by stake merges every address sharing a staking credential. That is
//! usually what a WALLET-level graph wants — one node for a marketplace rather
//! than ten thousand — but it is not always right: Splash and DexHunter run
//! fourteen scripts on one credential, and CSwap collapses every pool onto a
//! single stake, which is exactly why the ARCHIVE stores payment addresses.
//! Forensic work that turns on telling fronts apart needs
//! [`PartyKey::Payment`]. The graph says which it used; a reader must not
//! assume.
//!
//! # Wire discipline
//!
//! postcard, positional, [`MovementGraph::format`] FIRST so a decoder can peek
//! byte zero before committing. Positional means **append-only is still
//! breaking**: any change to a field or its order is a new
//! [`GRAPH_FORMAT`] and a new type, never a silent reinterpretation of old
//! bytes. Same contract `market-ledger-wire` states for its pages.

use serde::{Deserialize, Serialize};

/// Bumped on ANY change to the shape below — the encoding is positional.
pub const GRAPH_FORMAT: u8 = 2;

/// The object's name beside the manifest — RAW on disk, always.
///
/// R2 does not compress for you: a body is stored pre-compressed with
/// `Content-Encoding: gzip` and the browser decompresses it transparently.
/// The publisher decides that per file FROM THE MEASURED RESULT and appends
/// `.gz` to the key when it pays, because whether an artifact compresses
/// depends on what is in it — the token path's `txids`, a wall of 32-byte
/// hashes, came out 6 KB LARGER gzipped. Files stay raw here so a reader on
/// the box never has to care about transport encoding.
pub const GRAPH: &str = "graph.bin";

/// What a node in the graph IS.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartyKey {
    /// The stake address — the wallet. Ten times smaller, and the right node
    /// for "who traded with whom". Merges shared staking credentials; see the
    /// module header.
    #[default]
    Stake,
    /// The payment address — the UTxO's own address, as the archive stores
    /// it. Nothing is merged, and the artifact is an order of magnitude
    /// bigger.
    Payment,
}

impl PartyKey {
    pub const ALL: [PartyKey; 2] = [PartyKey::Stake, PartyKey::Payment];

    pub fn as_wire(&self) -> &'static str {
        match self {
            PartyKey::Stake => "stake",
            PartyKey::Payment => "payment",
        }
    }
}

/// One directed relationship, aggregated over the policy's whole history.
///
/// `first_slot`/`last_slot` are what make the graph playable: a reader can
/// brush a time range and fade edges in and out without holding the
/// transactions behind them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edge {
    /// Index into [`MovementGraph::parties`].
    pub from: u32,
    pub to: u32,
    /// Transfers along this edge.
    pub count: u32,
    /// Units moved, summed. One per transfer for an NFT policy; a real
    /// quantity for a fungible one.
    pub units: u64,
    pub first_slot: u64,
    pub last_slot: u64,
}

/// A mint or a burn. NOT an edge: a mint has no sender and a burn no
/// recipient, and inventing a counterparty for either would be a lie the
/// archive is careful not to tell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartyCount {
    pub party: u32,
    pub count: u32,
    pub units: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MovementGraph {
    /// FIRST — peek before decoding.
    pub format: u8,
    /// Policy id, hex.
    pub policy: String,
    pub keyed_by: PartyKey,
    /// The coverage this was built from, so a reader can say what it is a
    /// graph OF rather than assuming "everything".
    pub from_slot: u64,
    pub to_slot: u64,
    /// True when the archive it came from reached the policy's first mint.
    pub complete: bool,
    pub built_unix: u64,
    /// Transactions the archive held.
    pub txs: u64,
    /// Transfers — one edge occurrence each. `edges` is these, aggregated.
    pub transfers: u64,
    /// Unit moves the graph could NOT place: several parties on a side, so
    /// the pairing is genuinely unknown. Never guessed, always counted, so a
    /// reader knows how much of the history the picture leaves out.
    pub ambiguous: u64,
    /// Arrivals whose source sits below the archive's floor. Zero on a
    /// complete archive.
    pub source_below_floor: u64,
    /// Transfers DROPPED as self-shuffles: a wallet moving units between its
    /// own payment addresses, which stake-keying collapses onto one node.
    ///
    /// Kept as a number because it is 72–83% of all transfers and its
    /// absence from `edges` would otherwise be silent. Measured on
    /// ClayNation: 658,299 of 661,922 stake self-loops had a key credential
    /// on both sides — a wallet reorganising its own UTxOs, which is not a
    /// relationship between parties and renders as a blob pointing at
    /// itself.
    ///
    /// The 3,623 that did NOT are still edges: a SCRIPT address on either
    /// side means two distinct contracts sharing a staking credential
    /// (Splash and DexHunter run fourteen scripts on one), and that is real
    /// flow. The distinction is what the address IS, never the shape of the
    /// edge.
    ///
    /// Always zero when [`PartyKey::Payment`] — a payment address moving to
    /// itself nets out in the fold and never reaches here.
    pub self_shuffles: u64,
    /// The dictionary. Every `u32` elsewhere indexes it.
    pub parties: Vec<String>,
    /// Ascending by `(from, to)` — deterministic, and it compresses better.
    pub edges: Vec<Edge>,
    pub mints: Vec<PartyCount>,
    pub burns: Vec<PartyCount>,
}

#[derive(Debug, thiserror::Error)]
pub enum GraphError {
    #[error("graph is format {found}, this build reads {expected}")]
    Version { found: u8, expected: u8 },
    #[error("decoding the graph: {0}")]
    Decode(#[from] postcard::Error),
}

impl MovementGraph {
    pub fn encode(&self) -> Result<Vec<u8>, GraphError> {
        Ok(postcard::to_stdvec(self)?)
    }

    /// Version-checked decode. The check is on the DECODED first field rather
    /// than a raw byte peek because postcard writes `format` first anyway;
    /// what matters is that a mismatch is loud instead of a struct read out
    /// of alignment.
    pub fn decode(bytes: &[u8]) -> Result<Self, GraphError> {
        let g: MovementGraph = postcard::from_bytes(bytes)?;
        match g.format == GRAPH_FORMAT {
            true => Ok(g),
            false => Err(GraphError::Version {
                found: g.format,
                expected: GRAPH_FORMAT,
            }),
        }
    }

    /// Every party's degree, in and out — what a layout sizes nodes by.
    /// Derived rather than stored: it is one pass over `edges`, and a reader
    /// that wants it can afford that far more cheaply than the wire can
    /// afford to carry it.
    pub fn degrees(&self) -> Vec<(u32, u32)> {
        let mut out = vec![(0u32, 0u32); self.parties.len()];
        for e in &self.edges {
            if let Some(d) = out.get_mut(e.from as usize) {
                d.1 += 1;
            }
            if let Some(d) = out.get_mut(e.to as usize) {
                d.0 += 1;
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn graph() -> MovementGraph {
        MovementGraph {
            format: GRAPH_FORMAT,
            policy: "ab".repeat(28),
            keyed_by: PartyKey::Stake,
            from_slot: 100,
            to_slot: 900,
            complete: true,
            built_unix: 1_788_000_000,
            txs: 3,
            transfers: 3,
            ambiguous: 1,
            source_below_floor: 0,
            self_shuffles: 12,
            parties: vec!["alice".into(), "bob".into(), "carol".into()],
            edges: vec![
                Edge {
                    from: 0,
                    to: 1,
                    count: 2,
                    units: 2,
                    first_slot: 100,
                    last_slot: 400,
                },
                Edge {
                    from: 1,
                    to: 2,
                    count: 1,
                    units: 1,
                    first_slot: 500,
                    last_slot: 500,
                },
            ],
            mints: vec![PartyCount {
                party: 0,
                count: 1,
                units: 10,
            }],
            burns: Vec::new(),
        }
    }

    #[test]
    fn it_round_trips() {
        let g = graph();
        assert_eq!(MovementGraph::decode(&g.encode().unwrap()).unwrap(), g);
    }

    /// The encoding is POSITIONAL, so a graph from another format must fail
    /// loudly rather than be read out of alignment into plausible nonsense.
    #[test]
    fn a_graph_from_another_format_is_refused() {
        let mut g = graph();
        g.format = GRAPH_FORMAT + 7;
        let err = MovementGraph::decode(&g.encode().unwrap()).unwrap_err();
        assert!(
            matches!(err, GraphError::Version { found, expected }
                if found == GRAPH_FORMAT + 7 && expected == GRAPH_FORMAT),
            "{err}"
        );
    }

    /// Degrees are DERIVED — carrying them would be wire for something a
    /// reader computes in one pass.
    #[test]
    fn degrees_come_from_the_edges() {
        let d = graph().degrees();
        assert_eq!(d[0], (0, 1), "alice: one out");
        assert_eq!(d[1], (1, 1), "bob: one in, one out");
        assert_eq!(d[2], (1, 0), "carol: one in");
    }

    /// A wallet shuffling its own UTxOs is NOT a relationship, but its
    /// absence must not be silent — the count is 72–83% of all transfers.
    #[test]
    fn dropped_self_shuffles_are_still_counted() {
        let g = graph();
        assert_eq!(g.self_shuffles, 12);
        assert!(
            !g.edges.iter().any(|e| e.from == e.to),
            "a wallet self-shuffle is not an edge"
        );
        let back = MovementGraph::decode(&g.encode().unwrap()).unwrap();
        assert_eq!(back.self_shuffles, g.self_shuffles);
    }

    /// The party key is on the wire because a reader CANNOT infer it, and
    /// reading a stake-keyed graph as though it were payment-keyed would
    /// silently merge distinct contracts into one node.
    #[test]
    fn the_party_key_survives_the_wire() {
        for key in PartyKey::ALL {
            let mut g = graph();
            g.keyed_by = key;
            assert_eq!(
                MovementGraph::decode(&g.encode().unwrap())
                    .unwrap()
                    .keyed_by,
                key
            );
        }
        assert_eq!(PartyKey::default(), PartyKey::Stake);
    }
}
