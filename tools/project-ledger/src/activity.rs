//! Global receipt counter — the custodial-scale proxy the terminal rule needs
//! at promotion time, computed from the stream with no indexer.
//!
//! One `u32` per staking credential seen receiving an output in the walk
//! window. Stakeless parties are terminal by shape already, so only staked
//! credentials are counted; that bounds the map by stake keys ever active
//! (~1–1.5M mainnet), roughly 50–100 MB, and it is checkpointed as a postcard
//! blob so a resumed walk sees the same counts as an uninterrupted one
//! (determinism of the frontier depends on it). If this ever proves too fat, a
//! Count-Min sketch is the drop-in — it overestimates only, which fails safe
//! for a terminal rule.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Activity {
    counts: HashMap<[u8; 28], u32>,
}

#[allow(dead_code)]
impl Activity {
    pub fn bump(&mut self, cred: [u8; 28]) {
        let c = self.counts.entry(cred).or_insert(0);
        *c = c.saturating_add(1);
    }

    pub fn get(&self, cred: &[u8; 28]) -> u32 {
        self.counts.get(cred).copied().unwrap_or(0)
    }

    pub fn len(&self) -> usize {
        self.counts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }

    pub fn to_blob(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("activity serialises")
    }

    pub fn from_blob(b: &[u8]) -> anyhow::Result<Self> {
        Ok(postcard::from_bytes(b)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_and_roundtrips() {
        let mut a = Activity::default();
        let k = [7u8; 28];
        a.bump(k);
        a.bump(k);
        a.bump([1u8; 28]);
        assert_eq!(a.get(&k), 2);
        assert_eq!(a.get(&[9u8; 28]), 0);
        let back = Activity::from_blob(&a.to_blob()).unwrap();
        assert_eq!(a, back);
    }
}
