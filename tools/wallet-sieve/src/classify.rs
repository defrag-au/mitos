//! Timeline classification — a pure in-memory post-pass.
//!
//! Workers scan chunks out of order, so ownership can only be settled AFTER
//! the scan: sort every found tx into chain order, then walk once forward
//! maintaining the set of outrefs the wallet currently owns. A tx spending an
//! owned outref is an outflow valued from the pass-A record — NO input
//! resolution needed to net a wallet's own money (the resolution ladder is
//! only for naming counterparties). This is what keeps the sign right; the
//! banked `resolve-local` lesson is that guessing here flips net totals.

use std::collections::{HashMap, HashSet};

use crate::scan::{AssetUnit, FoundTx};

/// One UTxO the wallet holds: its lovelace and the assets riding with it.
/// Carrying identities (not a count) is what lets a SPEND say which asset
/// left — the outputs of a send belong to someone else, so the only record
/// of what the wallet gave up is what it was holding.
pub type OwnedUtxo = (u64, Vec<AssetUnit>);

/// The wallet's UTxO set: outref → [`OwnedUtxo`].
pub type OwnedSet = HashMap<([u8; 32], u32), OwnedUtxo>;

/// A net asset movement for one transaction: positive arrived, negative left.
#[derive(Clone, Debug)]
pub struct AssetMove {
    pub policy: String,
    pub name_hex: String,
    pub quantity: i64,
    /// This asset was created in THIS transaction. A fact off the mint field,
    /// not an inference from the shape of the movement — which is what makes
    /// "mint" sayable at all. `false` also covers a burn: see
    /// [`AssetMove::burned`].
    pub minted: bool,
    /// This asset was destroyed in this transaction.
    pub burned: bool,
}

pub struct TimelineTx {
    pub slot: u64,
    pub hash: [u8; 32],
    pub kind: &'static str,
    pub lovelace_in: u64,
    pub lovelace_out: u64,
    pub assets_in: u32,
    pub assets_out: u32,
    /// Which assets moved, netted per unit — the row's actual story.
    pub asset_moves: Vec<AssetMove>,
    /// Inputs NOT owned by the wallet — the funding side, for resolution.
    pub foreign_inputs: Vec<([u8; 32], u32)>,
    /// Destinations of a SEND (non-target outputs, lovelace-descending,
    /// capped). Empty for receives — their counterparty comes from
    /// resolution instead.
    pub recipients: Vec<(String, u64)>,
}

pub struct Timeline {
    pub txs: Vec<TimelineTx>,
    /// Outrefs still held at the end of the KNOWN timeline — i.e. the
    /// wallet's UTxO set as far as the sieve can see. Pass B's filter: a
    /// change-less spend consumes exactly these.
    pub owned: OwnedSet,
    pub own_hashes: Vec<[u8; 32]>,
    pub first_slot: u64,
    pub last_slot: u64,
}

/// Incremental variant: `seed_owned` is the wallet's UTxO set as of a prior
/// run's cursor; `found` is only what a scan of NEWER chunks produced. The
/// returned timeline covers just the new txs, with `owned` updated through
/// them — persist it as the next seed.
pub fn build_with(seed_owned: OwnedSet, found: &[FoundTx]) -> Timeline {
    let mut ordered: Vec<&FoundTx> = found.iter().collect();
    ordered.sort_by_key(|t| (t.slot, t.tx_idx));

    let mut owned: OwnedSet = seed_owned;
    let mut own_hashes: Vec<[u8; 32]> = Vec::new();
    let mut seen: HashSet<[u8; 32]> = HashSet::new();
    let mut txs = Vec::with_capacity(ordered.len());
    let (mut first_slot, mut last_slot) = (u64::MAX, 0u64);

    for tx in ordered {
        if !seen.insert(tx.hash) {
            continue;
        }
        first_slot = first_slot.min(tx.slot);
        last_slot = last_slot.max(tx.slot);

        // Net asset movement, keyed by unit: what the spent UTxOs carried
        // away (negative) against what the new ones brought in (positive).
        // Netting matters — a wallet that spends a UTxO and gets the same
        // token back as change did not "move" it.
        let mut moves: HashMap<(String, String), i64> = HashMap::new();

        let mut lovelace_out = 0u64;
        let mut assets_out = 0u32;
        let mut spent_own = 0usize;
        let mut foreign_inputs = Vec::new();
        for oref in &tx.inputs {
            if let Some((l, units)) = owned.remove(oref) {
                lovelace_out += l;
                assets_out += units.len() as u32;
                for u in units {
                    *moves.entry((u.policy, u.name_hex)).or_default() -= u.quantity as i64;
                }
                spent_own += 1;
            } else {
                foreign_inputs.push(*oref);
            }
        }

        let lovelace_in: u64 = tx.out_hits.iter().map(|h| h.lovelace).sum();
        let assets_in: u32 = tx.out_hits.iter().map(|h| h.assets).sum();
        for h in &tx.out_hits {
            for u in &h.units {
                *moves
                    .entry((u.policy.clone(), u.name_hex.clone()))
                    .or_default() += u.quantity as i64;
            }
            owned.insert((tx.hash, h.index), (h.lovelace, h.units.clone()));
        }
        // The tx's mint field, by unit, so a movement can be marked with what
        // the chain actually did rather than with what its shape suggests.
        let minted: std::collections::HashMap<(&str, &str), i64> = tx
            .minted
            .iter()
            .map(|m| ((m.policy.as_str(), m.name_hex.as_str()), m.quantity))
            .collect();
        let mut asset_moves: Vec<AssetMove> = moves
            .into_iter()
            .filter(|(_, q)| *q != 0)
            .map(|((policy, name_hex), quantity)| {
                let mint = minted
                    .get(&(policy.as_str(), name_hex.as_str()))
                    .copied()
                    .unwrap_or(0);
                AssetMove {
                    policy,
                    name_hex,
                    quantity,
                    minted: mint > 0,
                    burned: mint < 0,
                }
            })
            .collect();
        // Biggest movements first, so a capped display shows what matters.
        asset_moves.sort_by_key(|m| std::cmp::Reverse(m.quantity.abs()));
        asset_moves.truncate(12);
        if !tx.out_hits.is_empty() {
            own_hashes.push(tx.hash);
        }

        let kind = if spent_own == 0 {
            "receive"
        } else if tx.out_hits.len() as u32 == tx.total_outputs {
            "internal"
        } else {
            "send"
        };
        let recipients = if kind == "send" {
            let mut r = tx.other_outputs.clone();
            r.sort_by_key(|(_, l)| std::cmp::Reverse(*l));
            r.truncate(8);
            r
        } else {
            Vec::new()
        };
        txs.push(TimelineTx {
            slot: tx.slot,
            hash: tx.hash,
            kind,
            lovelace_in,
            lovelace_out,
            assets_in,
            assets_out,
            asset_moves,
            foreign_inputs,
            recipients,
        });
    }

    Timeline {
        txs,
        owned,
        own_hashes,
        first_slot,
        last_slot,
    }
}
