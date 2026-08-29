//! The input-resolution ladder.
//!
//! Block bodies reference inputs as `(tx_hash, index)` only. Net deltas need
//! every input of any tx that touches a watched party — the payer's identity
//! in the mint window, and a promoted wallet's pre-watch UTxOs on its first
//! spend — so an unresolved input would silently turn a net figure into a gross
//! one, which is the failure the whole model exists to foreclose.
//!
//! ```text
//! buffer         the watched parties' UTxO set (walk-maintained, free)
//! → outref_cache persistent, append-only, write-through
//! → remote       Koios /utxo_info, ≤100 refs per POST, once per ref ever
//! ```
//!
//! What cannot be resolved is COUNTED, never guessed: the caller writes rows
//! with `unresolved_inputs = n` and the UI renders them partial.

use std::collections::HashMap;

use anyhow::Result;
use mitos_chain_walk::decode::OutRef;

use crate::koios::Koios;
use crate::store::{CachedOutput, Ledger};

/// The remote rung. Trait so a walk can run offline (tests; a box with no
/// egress) and so the source is swappable.
pub trait Remote {
    /// Resolve refs (`tx_hash#idx`); missing ones are simply absent from the map.
    fn resolve(&self, refs: &[OutRef]) -> Result<HashMap<OutRef, CachedOutput>>;
}

/// No network: everything not in the buffer or cache stays unresolved.
pub struct Offline;

impl Remote for Offline {
    fn resolve(&self, _refs: &[OutRef]) -> Result<HashMap<OutRef, CachedOutput>> {
        Ok(HashMap::new())
    }
}

impl Remote for Koios {
    fn resolve(&self, refs: &[OutRef]) -> Result<HashMap<OutRef, CachedOutput>> {
        let mut out = HashMap::new();
        for chunk in refs.chunks(100) {
            let keys: Vec<String> = chunk
                .iter()
                .map(|(h, i)| format!("{}#{i}", hex::encode(h.as_ref())))
                .collect();
            for row in self.utxo_info(&keys)? {
                let Ok(hb) = hex::decode(&row.tx_hash) else {
                    continue;
                };
                if hb.len() != 32 {
                    continue;
                }
                let mut h = [0u8; 32];
                h.copy_from_slice(&hb);
                let lovelace = row.value.parse::<u64>().unwrap_or(0);
                let assets = row
                    .asset_list
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|a| {
                        Some((
                            hex::decode(a.policy_id).ok()?,
                            hex::decode(a.asset_name).ok()?,
                        ))
                    })
                    .collect();
                out.insert(
                    (pallas_primitives::Hash::new(h), row.tx_index),
                    CachedOutput {
                        address: row.address,
                        lovelace,
                        assets,
                    },
                );
            }
        }
        Ok(out)
    }
}

/// Resolve `refs` through cache → remote, writing remote hits through to the
/// cache. The buffer rung is the caller's (it needs `&mut` for `take`).
/// Returns what was found; the caller counts the rest as unresolved.
pub fn resolve_missing(
    ledger: &mut Ledger,
    remote: &dyn Remote,
    refs: &[OutRef],
    stats: &mut LadderStats,
) -> Result<HashMap<OutRef, CachedOutput>> {
    let mut found = HashMap::new();
    let mut ask = Vec::new();
    for r in refs {
        match ledger.cache_get(r)? {
            Some(o) => {
                stats.cache_hits += 1;
                found.insert(*r, o);
            }
            None => ask.push(*r),
        }
    }
    if !ask.is_empty() {
        let fetched = remote.resolve(&ask)?;
        stats.remote_calls += ask.len().div_ceil(100) as u64;
        stats.remote_hits += fetched.len() as u64;
        stats.unresolved += (ask.len() - fetched.len()) as u64;
        let write: Vec<(OutRef, CachedOutput)> =
            fetched.iter().map(|(k, v)| (*k, v.clone())).collect();
        ledger.cache_put(&write)?;
        found.extend(fetched);
    }
    Ok(found)
}

#[derive(Debug, Default, Clone, Copy)]
pub struct LadderStats {
    pub buffer_hits: u64,
    pub cache_hits: u64,
    pub remote_calls: u64,
    pub remote_hits: u64,
    pub unresolved: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use pallas_primitives::Hash;

    struct Fake(HashMap<OutRef, CachedOutput>);
    impl Remote for Fake {
        fn resolve(&self, refs: &[OutRef]) -> Result<HashMap<OutRef, CachedOutput>> {
            Ok(refs
                .iter()
                .filter_map(|r| self.0.get(r).map(|o| (*r, o.clone())))
                .collect())
        }
    }

    #[test]
    fn ladder_writes_through_and_counts_unresolved() {
        let mut ledger = Ledger::open_in_memory().unwrap();
        let known = (Hash::new([1u8; 32]), 0u32);
        let unknown = (Hash::new([2u8; 32]), 1u32);
        let out = CachedOutput {
            address: "addr1z".into(),
            lovelace: 9,
            assets: vec![],
        };
        let remote = Fake(HashMap::from([(known, out.clone())]));
        let mut stats = LadderStats::default();

        let got = resolve_missing(&mut ledger, &remote, &[known, unknown], &mut stats).unwrap();
        assert_eq!(got.get(&known), Some(&out));
        assert!(!got.contains_key(&unknown));
        assert_eq!(stats.remote_calls, 1);
        assert_eq!(stats.remote_hits, 1);
        assert_eq!(stats.unresolved, 1);

        // Second time: cache, no remote.
        let got = resolve_missing(&mut ledger, &Offline, &[known], &mut stats).unwrap();
        assert_eq!(got.get(&known), Some(&out));
        assert_eq!(stats.cache_hits, 1);
    }
}
