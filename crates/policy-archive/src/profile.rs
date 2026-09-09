//! What a policy's units ARE — the question nothing in this pipeline asked
//! until 2026-09-08, and the one every downstream decision needs answered.
//!
//! # Why a walk needs this and not just a reader
//!
//! The archive treats every policy identically, so a fungible token, a
//! 10,000-piece collection and a CIP-68 mixed policy are ingested the same way
//! and rendered the same way. That is the root of the complaint this whole
//! design started from: a native token came out as "lots of transfers" because
//! nothing ever asked what it was looking at.
//!
//! It also decides a cost. The observation tier keeps every script-held output
//! as a candidate so a decoder added later re-derives from the archive instead
//! of re-walking. That is right for a token, whose script outputs are a handful
//! of pool addresses — $PERP has ~950. It is wrong for a collection, whose
//! script outputs are one marketplace escrow per listing, where the count runs
//! to millions and the interpretation belongs to market-ledger anyway.
//!
//! # The signal is the QUANTITY, and it is the same at both granularities
//!
//! A unit ever held in quantity is fungible; a unit only ever held one at a
//! time is a collectible. That is decidable per output, which is where the
//! candidate rule needs it, and it accumulates into a policy-level summary,
//! which is what a reader needs. One rule, no chicken-and-egg: the walk does
//! not have to know the policy's class before it can classify an output.
//!
//! CIP-68 labels (`333`/`444` fungible, `222` a collectible) are a *declared*
//! and therefore stronger signal, and folding them in would sharpen the edge
//! cases. Deliberately not done yet: it needs `cardano-assets` in this crate,
//! which every archive reader links, and the quantity rule settles every real
//! policy checked so far without it.

use serde::{Deserialize, Serialize};

/// What the policy's units are, taken together.
///
/// `Unknown` is a first-class answer and must never be read as one of the
/// others — the distinction between "we looked and it is a token" and "we have
/// not seen enough to say" is the one this codebase keeps paying for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Class {
    /// At least one unit held in quantity, none held singly. A token.
    Fungible,
    /// Every unit seen only ever one at a time. A collection.
    Collection,
    /// Both, under one policy. CIP-68 makes this ordinary rather than exotic,
    /// so a single `TokenType` per policy is always going to be a lie
    /// somewhere.
    Mixed,
    /// Nothing seen yet.
    Unknown,
}

impl Class {
    pub fn as_str(self) -> &'static str {
        match self {
            Class::Fungible => "fungible",
            Class::Collection => "collection",
            Class::Mixed => "mixed",
            Class::Unknown => "unknown",
        }
    }
}

/// A policy's tier-1 profile: what its units are, and how firmly.
///
/// Accumulated by the walk as it sees units, and stamped into the manifest so
/// a reader knows what the archive ASSUMED — including, crucially, whether
/// candidates were kept.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Profile {
    /// Distinct unit names seen.
    pub units_seen: u64,
    /// Units observed at least once in a quantity greater than one.
    pub fungible_units: u64,
    /// Units only ever observed one at a time.
    pub single_units: u64,
    /// Whether the walk that produced this reached the policy's first mint. A
    /// profile from a partial walk is a sample, and a sample of the newest ten
    /// days can miss a unit class entirely.
    pub complete: bool,
}

impl Profile {
    /// Fold one observed `(unit, quantity)` in.
    ///
    /// `seen_fungible` is whether this unit has ALREADY been counted fungible,
    /// which the caller tracks per unit name — one output holding two of a
    /// thing settles it forever, and a later output holding one must not undo
    /// that.
    pub fn observe(&mut self, first_sighting: bool, was_fungible: bool, qty: i64) {
        if first_sighting {
            self.units_seen += 1;
            match qty > 1 {
                true => self.fungible_units += 1,
                false => self.single_units += 1,
            }
        } else if qty > 1 && !was_fungible {
            // Promotion is one-way: quantity is proof, a single sighting is
            // only the absence of it.
            self.fungible_units += 1;
            self.single_units = self.single_units.saturating_sub(1);
        }
    }

    pub fn class(&self) -> Class {
        match (self.fungible_units > 0, self.single_units > 0) {
            (true, true) => Class::Mixed,
            (true, false) => Class::Fungible,
            (false, true) => Class::Collection,
            (false, false) => Class::Unknown,
        }
    }

    /// Whether a script output holding `qty` of a unit is worth keeping as an
    /// UNDECODED candidate.
    ///
    /// Quantity, not the policy's class, because the class can be `Mixed` and
    /// because this is decidable at the output with no lookahead. A pool holds
    /// many; a marketplace escrow holds exactly one and is market-ledger's to
    /// interpret, not this archive's.
    ///
    /// ⚠️ The known limit: a pool holding exactly ONE of something would be
    /// skipped. No such pool exists among the venues decoded here — every one
    /// holds a reserve — and a DECODED observation is written regardless of
    /// quantity, so this only ever gates the speculative tier.
    pub fn keep_candidate(qty: i64) -> bool {
        qty > 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_token_reads_fungible_and_a_collection_reads_collection() {
        let mut token = Profile::default();
        token.observe(true, false, 1_000_000);
        assert_eq!(token.class(), Class::Fungible);

        let mut coll = Profile::default();
        for _ in 0..5 {
            coll.observe(true, false, 1);
        }
        assert_eq!(coll.class(), Class::Collection);
        assert_eq!(coll.units_seen, 5);
    }

    /// CIP-68 makes this ordinary: one policy, a 222 collectible and a 333
    /// fungible. A single class per policy would be wrong here whichever one
    /// it picked.
    #[test]
    fn one_policy_can_be_both() {
        let mut p = Profile::default();
        p.observe(true, false, 1);
        p.observe(true, false, 5_000);
        assert_eq!(p.class(), Class::Mixed);
    }

    /// Quantity is PROOF of fungibility; a single sighting is only the absence
    /// of proof. So promotion is one-way and a later single must not undo it.
    #[test]
    fn fungibility_is_proved_once_and_never_retracted() {
        let mut p = Profile::default();
        p.observe(true, false, 1); // first sight: looks single
        assert_eq!(p.class(), Class::Collection);
        p.observe(false, false, 900); // now proved fungible
        assert_eq!(p.class(), Class::Fungible);
        assert_eq!(p.fungible_units, 1);
        assert_eq!(p.single_units, 0);
        p.observe(false, true, 1); // a later single changes nothing
        assert_eq!(p.class(), Class::Fungible);
        assert_eq!(p.units_seen, 1);
    }

    /// Nothing seen is its own answer.
    #[test]
    fn an_empty_profile_is_unknown_not_a_guess() {
        assert_eq!(Profile::default().class(), Class::Unknown);
    }

    /// The gate that keeps a collection's millions of listing escrows out of
    /// the candidate tier, and lets a pool's reserves in.
    #[test]
    fn only_a_quantity_is_worth_keeping_speculatively() {
        assert!(!Profile::keep_candidate(1), "an NFT escrow is not a pool");
        assert!(Profile::keep_candidate(2));
        assert!(Profile::keep_candidate(1_000_000));
        assert!(!Profile::keep_candidate(0));
    }
}
