//! In-memory walk state that is checkpointed alongside the cursor.
//!
//! Four things, all of which a resume must restore exactly for the walk to be
//! deterministic: the [`Frontier`] (who is watched, from when, and why), the
//! [`Buffer`] (the watched parties' current UTxO set — local input resolution),
//! the [`Activity`] counter (the terminal rule's global proxy), and the
//! [`Holders`] map (which party holds each policy asset right now, so a
//! transfer's `from` never needs input resolution).

use std::collections::HashMap;

use chain_ledger::Frontier;
use mitos_chain_walk::decode::{Asset, OutRef};
use serde::{Deserialize, Serialize};

use crate::activity::Activity;

/// A buffered output at a watched party — everything a later spend needs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BufferedOutput {
    pub address: String,
    pub lovelace: u64,
    pub assets: Vec<(Vec<u8>, Vec<u8>)>,
    /// Party key of the holder (the watched party).
    pub party: String,
    pub has_stake: bool,
}

#[allow(dead_code)]
impl BufferedOutput {
    pub fn asset_list(&self) -> Vec<Asset> {
        self.assets
            .iter()
            .map(|(p, n)| Asset {
                policy: p.clone(),
                name: n.clone(),
            })
            .collect()
    }
}

/// The watched parties' open UTxO set, keyed by outref.
#[derive(Debug, Default, Clone)]
pub struct Buffer {
    map: HashMap<OutRef, BufferedOutput>,
}

#[allow(dead_code)]
impl Buffer {
    pub fn insert(&mut self, oref: OutRef, out: BufferedOutput) {
        self.map.insert(oref, out);
    }

    pub fn take(&mut self, oref: &OutRef) -> Option<BufferedOutput> {
        self.map.remove(oref)
    }

    pub fn contains(&self, oref: &OutRef) -> bool {
        self.map.contains_key(oref)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn entries(&self) -> impl Iterator<Item = (&OutRef, &BufferedOutput)> {
        self.map.iter()
    }
}

/// Current holder of each policy asset (asset name bytes → party key, since
/// slot). Updated on every output carrying the asset; a transfer's `from` is
/// the previous entry, so asset events need no input resolution at all.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Holders {
    map: HashMap<Vec<u8>, (String, u64)>,
    /// How many policy assets each party currently holds — kept in step with
    /// `map` so "does this party hold anything?" is O(1) per output, not a
    /// scan over every asset.
    per_party: HashMap<String, u32>,
}

#[allow(dead_code)]
impl Holders {
    /// Record `party` as the holder of `asset` from `slot`; returns the previous
    /// holder (if any) — the transfer's `from`.
    pub fn set(&mut self, asset: &[u8], party: &str, slot: u64) -> Option<String> {
        let prev = self
            .map
            .insert(asset.to_vec(), (party.to_owned(), slot))
            .map(|(p, _)| p);
        if let Some(p) = &prev {
            self.dec(p);
        }
        *self.per_party.entry(party.to_owned()).or_insert(0) += 1;
        prev
    }

    pub fn get(&self, asset: &[u8]) -> Option<&str> {
        self.map.get(asset).map(|(p, _)| p.as_str())
    }

    pub fn remove(&mut self, asset: &[u8]) -> Option<String> {
        let prev = self.map.remove(asset).map(|(p, _)| p);
        if let Some(p) = &prev {
            self.dec(p);
        }
        prev
    }

    /// Whether `party` currently holds at least one policy asset.
    pub fn holds(&self, party: &str) -> bool {
        self.per_party.get(party).is_some_and(|n| *n > 0)
    }

    fn dec(&mut self, party: &str) {
        if let Some(n) = self.per_party.get_mut(party) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                self.per_party.remove(party);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn entries(&self) -> impl Iterator<Item = (&Vec<u8>, &(String, u64))> {
        self.map.iter()
    }
}

/// Everything a checkpoint persists besides the rows and the cursor.
pub struct WalkState {
    pub frontier: Frontier,
    pub buffer: Buffer,
    pub activity: Activity,
    pub holders: Holders,
}
