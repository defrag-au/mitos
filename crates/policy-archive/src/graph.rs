//! The MOVEMENT LOG — every change of custody a policy's archive holds, as a
//! stream a frontend can play.
//!
//! The graph is a FOLD of this, not a substitute for it. The first cut of
//! this module shipped the fold — aggregated `(from, to, count, first_slot,
//! last_slot)` edges — and that was the wrong shape: both widgets that would
//! draw it take the individual movements
//! (`HolderField::AssetMove { timestamp, asset, from, to }`,
//! `FlowRing::RingFlow { timestamp, from, to, quantity }`), and edges cannot
//! be un-folded back into events. [`MovementGraph::edges`] derives the
//! aggregate in one client-side pass; nothing derives the stream from it.
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
//! # Columns, not rows
//!
//! [`Moves`] is parallel arrays rather than an array of structs: each column
//! is homogeneous and compresses far better, and `slots` is delta-encoded
//! ascending. Same shape `token-ledger-wire` uses for the token spine, for
//! the same reason.
//!
//! # Interpretation is the reader's
//!
//! The stream is LITERAL on-chain movement. A marketplace listing really is
//! a transfer to a script address, so it appears as one, and a sale appears
//! as a second movement out of it. A reader that holds a list of custody
//! addresses can decline to move the dot on the way IN and let it fly from
//! the seller on the way OUT — which renders seller → buyer without anything
//! here having to collapse it, and makes adding a marketplace a list entry
//! rather than a re-derivation of every policy. That is the same rule the
//! archive already keeps for direction: derived at read time, never stored.
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
pub const GRAPH_FORMAT: u8 = 4;

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

/// No party: a mint has no sender, a burn no recipient.
pub const NOBODY: u32 = u32::MAX;

/// The stream, as columns. Every array is the same length — one entry per
/// movement, slot-ascending.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Moves {
    /// DELTA-encoded from the previous movement; the first is absolute.
    /// Ascending, so every delta is non-negative and most are small.
    pub slot_deltas: Vec<u64>,
    /// Index into [`MovementGraph::units`].
    pub assets: Vec<u32>,
    /// Index into [`MovementGraph::parties`], or [`NOBODY`] for a mint.
    pub from: Vec<u32>,
    /// Index into [`MovementGraph::parties`], or [`NOBODY`] for a burn.
    pub to: Vec<u32>,
    pub quantities: Vec<u64>,
    /// When the FROM side was a SCRIPT address: `1 + index` into
    /// [`MovementGraph::scripts`]. `0` means it was an ordinary wallet.
    ///
    /// The PAYMENT CREDENTIAL, not the address, and that is the whole trick:
    /// Wayup's sale validator keeps the seller's staking part, so every
    /// listing has a different address but the SAME credential —
    /// `a76f0fb8…` on all 2,455 of Perps' listings. Interning the credential
    /// turns a per-seller dictionary into one entry, and it is also what the
    /// venue registry matches on.
    ///
    /// Offset by one so "not a script" is a zero, which is one varint byte —
    /// and it is the overwhelmingly common case (91.6% of ClayNation's
    /// movements touch no contract at all).
    pub from_script: Vec<u32>,
    /// As [`Self::from_script`], for the TO side. A listing.
    pub to_script: Vec<u32>,
}

/// One movement, as a reader wants it: absolute slot, resolved indices.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Movement {
    pub slot: u64,
    pub asset: u32,
    /// `None` is a mint.
    pub from: Option<u32>,
    /// `None` is a burn.
    pub to: Option<u32>,
    pub quantity: u64,
    /// The FROM side's script credential, indexed into
    /// [`MovementGraph::scripts`]. `None` is an ordinary wallet.
    pub from_script: Option<u32>,
    /// The TO side's — a deposit into a contract, which for a marketplace
    /// validator is a LISTING.
    pub to_script: Option<u32>,
}

impl Moves {
    pub fn len(&self) -> usize {
        self.slot_deltas.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slot_deltas.is_empty()
    }

    /// The stream with slots re-accumulated and sentinels turned back into
    /// `None`. Allocation-free; a caller maps it straight into `AssetMove`.
    pub fn iter(&self) -> impl Iterator<Item = Movement> + '_ {
        let mut slot = 0u64;
        (0..self.len()).map(move |i| {
            slot += self.slot_deltas[i];
            // The script columns are OFFSET BY ONE so absent is a zero; a
            // graph written before they existed has empty columns, which
            // reads as "no contract anywhere" rather than panicking.
            let script = |col: &[u32]| col.get(i).copied().filter(|n| *n > 0).map(|n| n - 1);
            Movement {
                slot,
                asset: self.assets[i],
                from: (self.from[i] != NOBODY).then_some(self.from[i]),
                to: (self.to[i] != NOBODY).then_some(self.to[i]),
                quantity: self.quantities[i],
                from_script: script(&self.from_script),
                to_script: script(&self.to_script),
            }
        })
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
    /// The party dictionary. Every party index elsewhere addresses it.
    pub parties: Vec<String>,
    /// The asset dictionary — unit names, hex. A dot's stable identity.
    pub units: Vec<String>,
    /// SCRIPT PAYMENT CREDENTIALS, hex, addressed by [`Moves::from_script`]
    /// and [`Moves::to_script`].
    ///
    /// Tiny by construction — a policy touches a handful of contracts, and
    /// the credential is shared across every address a validator issues. It
    /// is deliberately NOT a judgement: this says "a contract was on this
    /// side", never which contract or what it meant. A reader matches it
    /// against the venue registry and decides whether that is custody worth
    /// marking rather than a move worth drawing, which is what keeps adding
    /// a marketplace a registry entry instead of a re-derivation.
    pub scripts: Vec<String>,
    /// THE STREAM. Everything else here is a summary of it.
    pub moves: Moves,
}

#[derive(Debug, thiserror::Error)]
pub enum GraphError {
    #[error(
        "movement log is format {found}, this build reads {expected} — a stale copy, \
         not a broken one"
    )]
    Version { found: u8, expected: u8 },
    #[error("movement log is empty")]
    Empty,
    #[error("decoding the graph: {0}")]
    Decode(#[from] postcard::Error),
}

impl MovementGraph {
    pub fn encode(&self) -> Result<Vec<u8>, GraphError> {
        Ok(postcard::to_stdvec(self)?)
    }

    /// Version-checked decode. The check is on BYTE ZERO, before anything is
    /// deserialized.
    ///
    /// It has to be that way round. `format` is a `u8` and postcard writes it
    /// first and unencoded, so the byte is readable without trusting the rest
    /// — and the rest cannot be trusted: an OLDER log has fewer columns than
    /// this build expects, so deserializing it first fails deep inside as
    /// "Hit the end of buffer, expected more data", which reads as a corrupt
    /// artifact when the truth is a stale one. That distinction is the whole
    /// value of a version byte, and checking it after the fact throws it away.
    pub fn decode(bytes: &[u8]) -> Result<Self, GraphError> {
        match bytes.first() {
            Some(&GRAPH_FORMAT) => {}
            Some(&found) => {
                return Err(GraphError::Version {
                    found,
                    expected: GRAPH_FORMAT,
                });
            }
            None => return Err(GraphError::Empty),
        }
        let g: MovementGraph = postcard::from_bytes(bytes)?;
        match g.format == GRAPH_FORMAT {
            true => Ok(g),
            false => Err(GraphError::Version {
                found: g.format,
                expected: GRAPH_FORMAT,
            }),
        }
    }

    /// THE GRAPH: the stream folded to `(from, to)` edges, ascending.
    ///
    /// Derived, never stored — one pass over the movements, which a reader
    /// can afford far more cheaply than the wire can afford to carry a fold
    /// it cannot reverse. Mints and burns are not edges: they have one end.
    pub fn edges(&self) -> Vec<Edge> {
        let mut by: std::collections::BTreeMap<(u32, u32), Edge> =
            std::collections::BTreeMap::new();
        for m in self.moves.iter() {
            let (Some(from), Some(to)) = (m.from, m.to) else {
                continue;
            };
            let e = by.entry((from, to)).or_insert(Edge {
                from,
                to,
                count: 0,
                units: 0,
                first_slot: m.slot,
                last_slot: m.slot,
            });
            e.count += 1;
            e.units += m.quantity;
            e.first_slot = e.first_slot.min(m.slot);
            e.last_slot = e.last_slot.max(m.slot);
        }
        by.into_values().collect()
    }

    /// Every party's degree, in and out — what a layout sizes nodes by.
    pub fn degrees(&self) -> Vec<(u32, u32)> {
        let mut out = vec![(0u32, 0u32); self.parties.len()];
        for e in self.edges() {
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

    /// alice mints 10, sends 1 to bob, bob sends it to carol, carol burns it.
    fn log() -> MovementGraph {
        MovementGraph {
            format: GRAPH_FORMAT,
            policy: "ab".repeat(28),
            keyed_by: PartyKey::Stake,
            from_slot: 100,
            to_slot: 400,
            complete: true,
            built_unix: 1_788_000_000,
            txs: 4,
            transfers: 2,
            ambiguous: 1,
            source_below_floor: 0,
            self_shuffles: 12,
            parties: vec!["alice".into(), "bob".into(), "carol".into()],
            units: vec!["assetA".into()],
            // Wayup's sale validator credential — the real one, because the
            // case this exists for is its per-seller addresses collapsing to
            // a single entry here.
            scripts: vec!["a76f0fb801a29f591e9871576508d85b0b5f3c38774f65032f58fdad".into()],
            moves: Moves {
                // 100, 200, 300, 400
                slot_deltas: vec![100, 100, 100, 100],
                assets: vec![0, 0, 0, 0],
                from: vec![NOBODY, 0, 1, 2],
                to: vec![0, 1, 2, NOBODY],
                quantities: vec![10, 1, 1, 1],
                // bob→carol was a LISTING: carol's side is the validator.
                from_script: vec![0, 0, 0, 0],
                to_script: vec![0, 0, 1, 0],
            },
        }
    }

    /// A CONTRACT ON ONE SIDE is a fact the stream carries; what it MEANS is
    /// the reader's. Wayup's validator keeps the seller's staking part, so a
    /// listing has the same party on both sides and would be indistinguishable
    /// from a wallet reshuffle without this — 2,455 of Perps' 17,381
    /// movements are exactly that case.
    #[test]
    fn a_listing_is_marked_by_the_credential_on_its_far_side() {
        let g = log();
        let m: Vec<Movement> = g.moves.iter().collect();
        assert_eq!(m[1].to_script, None, "an ordinary transfer to a wallet");
        assert_eq!(
            m[2].to_script.and_then(|i| g.scripts.get(i as usize)),
            Some(&"a76f0fb801a29f591e9871576508d85b0b5f3c38774f65032f58fdad".to_string()),
            "the deposit into the validator"
        );
        assert!(
            m.iter().all(|x| x.from_script.is_none()),
            "nothing came back OUT of a contract in this log"
        );
    }

    /// The script columns are offset by one so "no contract" costs a single
    /// varint byte — the case for 91.6% of ClayNation's movements — and so a
    /// log written before they existed reads as no contracts rather than
    /// panicking on a short column.
    #[test]
    fn an_absent_script_column_reads_as_no_contract() {
        let mut g = log();
        g.moves.from_script.clear();
        g.moves.to_script.clear();
        assert!(
            g.moves
                .iter()
                .all(|m| m.from_script.is_none() && m.to_script.is_none())
        );
        assert_eq!(g.moves.iter().count(), 4, "every movement still reads");
    }

    #[test]
    fn it_round_trips() {
        let g = log();
        assert_eq!(MovementGraph::decode(&g.encode().unwrap()).unwrap(), g);
    }

    /// Slots are delta-encoded on the wire and absolute to a reader — the
    /// whole point of the column being small numbers.
    #[test]
    fn the_stream_reads_back_as_absolute_slots() {
        let g = log();
        let m: Vec<Movement> = g.moves.iter().collect();
        assert_eq!(
            m.iter().map(|x| x.slot).collect::<Vec<_>>(),
            vec![100, 200, 300, 400]
        );
        assert_eq!(m[0].from, None, "a mint has no sender");
        assert_eq!(m[3].to, None, "a burn has no recipient");
        assert_eq!(m[1].from, Some(0));
    }

    /// THE GRAPH IS A FOLD OF THE STREAM. Mints and burns have one end and
    /// are not edges; the two real transfers are.
    #[test]
    fn edges_are_derived_from_the_movements() {
        let e = log().edges();
        assert_eq!(e.len(), 2, "{e:?}");
        assert_eq!((e[0].from, e[0].to, e[0].count), (0, 1, 1));
        assert_eq!((e[1].from, e[1].to, e[1].count), (1, 2, 1));
        assert_eq!(e[0].first_slot, 200);
    }

    /// A wallet shuffling its own UTxOs is NOT a movement between parties,
    /// but its absence must not be silent — it is 72–83% of all transfers.
    #[test]
    fn dropped_self_shuffles_are_still_counted() {
        let g = log();
        assert_eq!(g.self_shuffles, 12);
        assert!(!g.edges().iter().any(|e| e.from == e.to));
        assert_eq!(
            MovementGraph::decode(&g.encode().unwrap())
                .unwrap()
                .self_shuffles,
            12
        );
    }

    #[test]
    fn degrees_come_from_the_edges() {
        let d = log().degrees();
        // A MINT IS NOT AN EDGE, so it adds no in-degree: alice's dot
        // arrives from the emitter, which is not a party. A wallet that only
        // ever minted therefore has degree zero and must be sized by what it
        // HOLDS, never by this.
        assert_eq!(d[0], (0, 1), "alice: minted in (not an edge), sent out");
        assert_eq!(d[1], (1, 1), "bob: in and out");
        assert_eq!(d[2], (1, 0), "carol: in only — the burn is not an edge");
    }

    /// The encoding is POSITIONAL, so a log from another format must fail
    /// loudly rather than be read out of alignment into plausible nonsense.
    #[test]
    fn a_log_from_another_format_is_refused() {
        let mut g = log();
        g.format = GRAPH_FORMAT + 7;
        let err = MovementGraph::decode(&g.encode().unwrap()).unwrap_err();
        assert!(matches!(err, GraphError::Version { .. }), "{err}");
    }

    /// A STALE log must say it is stale. An older one has fewer columns, so
    /// deserializing before checking the version fails deep inside with
    /// "Hit the end of buffer" — which sent a real debugging session after a
    /// corrupt artifact when the artifact was fine and the CACHE was old.
    #[test]
    fn an_older_log_reads_as_stale_rather_than_corrupt() {
        // The shortest possible "previous format": a version byte and
        // nothing else. Any older encoding is some prefix like this.
        let err = MovementGraph::decode(&[GRAPH_FORMAT - 1]).unwrap_err();
        match err {
            GraphError::Version { found, expected } => {
                assert_eq!((found, expected), (GRAPH_FORMAT - 1, GRAPH_FORMAT));
                assert!(err.to_string().contains("stale"), "{err}");
            }
            other => panic!("wanted a version error, got {other}"),
        }
        assert!(matches!(
            MovementGraph::decode(&[]).unwrap_err(),
            GraphError::Empty
        ));
    }

    /// The party key is on the wire because a reader CANNOT infer it, and
    /// reading a stake-keyed log as though it were payment-keyed would
    /// silently merge distinct contracts into one node.
    #[test]
    fn the_party_key_survives_the_wire() {
        for key in PartyKey::ALL {
            let mut g = log();
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
