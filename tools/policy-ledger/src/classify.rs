//! Is this policy an NFT collection, and what is it called?
//!
//! The rule below was DERIVED FROM the corpus, not assumed and then confirmed:
//! `policy-index/examples/profile` emitted one evidence row per policy and the
//! rule was scored offline against the 14,667 jpg.store rows that carry a REAL
//! curated name. This module applies that rule to the same
//! [`PolicyEvidence`] the survey emits — one extraction, two consumers, so a
//! re-score always measures what the builder actually does.
//!
//! MEASURED 2026-09-15 against those 14,667:
//!
//! | | recall | flags others | catalogue |
//! |---|---|---|---|
//! | without the editions clause | 98.81% | 7.3% | 30,255 |
//! | **with it** | **99.27%** | 7.4% | **30,619** |
//!
//! 🔑 The `max quantity <= 100` clause is LOAD-BEARING. Without it 167 real
//! collections are rejected — `ColoredCoin` (max 80), `Heads of Cards` (75),
//! `Cardania - Founders Cards` (2,500), `Pixel Tiles` (1,000).
//!
//! ⚠️ **"Is this an NFT collection" is NOT "is any quantity > 1".** An art
//! collection with 10 editions of each piece, or a PFP set with one accidental
//! duplicate from a minting error, is still an NFT collection. Supply shape is
//! evidence, never a verdict. MEASURED: grading the naive "qty>1 ⇒ fungible"
//! rule against all 38,992 jpg rows put its error at 24.6%; against the 14,667
//! NAMED ones, 2.6%. The first number was measuring the answer key, whose
//! `Unnamed` rows are largely genuine fungibles with quantities up to
//! `i64::MAX`.
//!
//! ⚠️ **Classification is ORTHOGONAL to ingestion-admissibility.** ADA Handle
//! — 318,136 assets, every one at quantity 1, CIP-25 present, no ticker — is a
//! textbook NFT collection by every structural signal here, and is exactly
//! what must not be auto-ingested. Nothing in this module tries to protect the
//! ingestion path; the [`EntryFlags::HAZARD`] bit is advisory metadata so a
//! consumer can explain itself, and the block lives in `collection-ownership`.

use std::collections::{HashMap, HashSet};

use collection_catalogue::{Alias, Entry, EntryFlags, HAZARD_ASSETS, NameOrigin};
use policy_index::PolicyEvidence;

/// Curated inputs the chain cannot supply. Both optional: without them the
/// catalogue is purely chain-derived and still correct, just missing the 2,551
/// collections only jpg.store names.
#[derive(Default)]
pub struct Overlay {
    /// jpg.store `display_name` by policy id, placeholders already dropped.
    pub curated: HashMap<[u8; 28], String>,
    /// Policies `collection-ownership` already tracks.
    pub tracked: HashSet<[u8; 28]>,
}

/// Why a policy is not in the catalogue.
///
/// ⚠️ A named reason per rejection, not a `bool` — the funnel is how the rule
/// gets re-argued. "30,619 accepted" tells you nothing about whether the
/// editions clause is still earning its place; "167 would fall out of
/// `SupplyShape`" does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Rejected {
    /// No transaction we read yielded the full policy id, so the entry could
    /// not be keyed. ⚠️ Counted, never silently dropped — MEASURED 0 of
    /// 231,616 on 2026-09-15, and a number that starts moving means the spans
    /// have drifted.
    Unreadable,
    /// A CIP-68 (333) FT label, or `decimals` in CIP-25. The issuer said so.
    FungibleLabel,
    /// Fewer than two distinct assets — 81.4% of mainnet policies are
    /// single-asset one-off mints.
    SingleAsset,
    /// A `ticker` on a handful of assets. Signal lift MEASURED at −22.8.
    TickerAndTiny,
    /// A mint of more than 10,000 of one asset is a token supply.
    SupplyTooLarge,
    /// Neither CIP-25 nor a CIP-68 NFT/reference label — nothing claims this
    /// is a collectible at all.
    NoMetadataStandard,
    /// Most assets minted above 1, and above the editions ceiling.
    SupplyShape,
    /// No usable quantity was observed. ⚠️ Rejected rather than accepted: the
    /// editions clause reads `max <= 100`, which an absent maximum of 0
    /// satisfies, so silence would otherwise be admitted as evidence.
    NoSupplyEvidence,
}

impl Rejected {
    pub fn as_str(&self) -> &'static str {
        match self {
            Rejected::Unreadable => "unreadable",
            Rejected::FungibleLabel => "fungible_label",
            Rejected::SingleAsset => "single_asset",
            Rejected::TickerAndTiny => "ticker_and_tiny",
            Rejected::SupplyTooLarge => "supply_too_large",
            Rejected::NoMetadataStandard => "no_metadata_standard",
            Rejected::SupplyShape => "supply_shape",
            Rejected::NoSupplyEvidence => "no_supply_evidence",
        }
    }
}

/// The ceiling on a single asset's mint quantity. Above this it is a token
/// supply, not an edition.
const MAX_EDITION_QUANTITY: i64 = 10_000;

/// A collection is accepted on supply shape when most assets are 1-of-1 OR
/// nothing exceeds this. 🔑 LOAD-BEARING — see the module header.
const EDITIONS_CEILING: i64 = 100;

/// A `ticker` is only damning on a handful of assets.
const TICKER_ASSET_LIMIT: u32 = 5;

/// Longest alias accepted, in characters.
///
/// ⚠️ A DROP, not a truncation. CIP-25 chunks long strings into ≤64-byte
/// parts and a `collection` field is free to concatenate as many as it likes;
/// a 4 KB "name" is not a name, and truncating it to 128 characters would mint
/// a plausible-looking wrong one — the same failure mode as the CIP-67
/// front-truncation that turned "Boobert's First Christmas" into
/// "stmasBoobert". Dropping says "no name", which is true.
const MAX_NAME_CHARS: usize = 128;

/// What one policy turned out to be.
pub enum Verdict {
    Collection(Box<Entry>),
    Rejected(Rejected),
}

/// Apply the rule to one policy's evidence.
pub fn classify(evidence: &PolicyEvidence, overlay: &Overlay) -> Verdict {
    // The entry is keyed by the FULL 28-byte id, which only the mint field
    // carries — the index stores an 8-byte prefix. No id, no entry.
    let Some(policy) = evidence.policy else {
        return Verdict::Rejected(Rejected::Unreadable);
    };

    // `distinct_name_prefixes` is the policy's size from the index's
    // `PolicyRun` — the whole run, not the sampled transactions. 🔑 That is
    // what makes the size guard free: no chunk read, no metadata decode.
    let assets = evidence.distinct_name_prefixes;
    let qty = &evidence.qty;

    if evidence.cip67.ft > 0 || evidence.has_decimals {
        return Verdict::Rejected(Rejected::FungibleLabel);
    }
    if assets < 2 {
        return Verdict::Rejected(Rejected::SingleAsset);
    }
    if evidence.has_ticker && assets <= TICKER_ASSET_LIMIT {
        return Verdict::Rejected(Rejected::TickerAndTiny);
    }
    if qty.max > MAX_EDITION_QUANTITY {
        return Verdict::Rejected(Rejected::SupplyTooLarge);
    }
    let has_standard =
        evidence.labels.contains(&721) || evidence.cip67.nft > 0 || evidence.cip67.reference > 0;
    if !has_standard {
        return Verdict::Rejected(Rejected::NoMetadataStandard);
    }
    if qty.eq1 == 0 && qty.gt1 == 0 {
        return Verdict::Rejected(Rejected::NoSupplyEvidence);
    }
    // Most assets are 1-of-1, OR the whole policy stays under the editions
    // ceiling. Either is enough; requiring both rejects `Pixel Tiles`.
    if qty.eq1 <= qty.gt1 && qty.max > EDITIONS_CEILING {
        return Verdict::Rejected(Rejected::SupplyShape);
    }

    let curated = overlay.curated.get(&policy);
    let mut flags = EntryFlags::default();
    // 🔑 Free — policy size is known before anything is touched.
    flags.set(EntryFlags::HAZARD, assets >= HAZARD_ASSETS);
    flags.set(EntryFlags::TRACKED, overlay.tracked.contains(&policy));
    flags.set(
        EntryFlags::CIP68,
        evidence.cip67.nft > 0 || evidence.cip67.reference > 0,
    );
    flags.set(EntryFlags::JPG_VERIFIED, curated.is_some());

    Verdict::Collection(Box::new(Entry {
        policy,
        assets,
        names: aliases(evidence, curated.map(String::as_str)),
        flags,
        ext: Vec::new(),
    }))
}

/// Every known alias, best first, deduped case-insensitively.
///
/// ⇒ **INDEX ALL ALIASES, DO NOT PICK A WINNER.** MEASURED: jpg names 37.6% of
/// its own rows, chain derivation names 81.0%, and the UNION names 87.5% —
/// chain names 19,450 collections jpg leaves blank, jpg uniquely names 2,551
/// (including ADA Handle, whose handles share no prefix to derive from). A
/// search box wants recall, not a canonical name, and both sources are wrong
/// sometimes where they overlap.
fn aliases(evidence: &PolicyEvidence, curated: Option<&str>) -> Vec<Alias> {
    let mut candidates: Vec<Alias> = Vec::with_capacity(4);
    let mut push = |name: Option<&str>, origin: NameOrigin| {
        if let Some(n) = name.map(str::trim).filter(|n| usable(n)) {
            candidates.push(Alias::new(n, origin));
        }
    };
    push(
        evidence.declared_collection.as_deref(),
        NameOrigin::Declared,
    );
    push(
        evidence.derived_from_meta.as_deref(),
        NameOrigin::DerivedFromMetadata,
    );
    push(
        evidence.derived_from_onchain.as_deref(),
        NameOrigin::DerivedFromAssetNames,
    );
    push(curated, NameOrigin::CuratedJpg);

    // Best first — by TRUST, which is deliberately not the wire discriminant.
    candidates.sort_by_key(|a| a.origin.rank());
    // Case-insensitive dedup keeping the best-ranked occurrence. "Clay Nation"
    // declared and "clay nation" derived are one alias, not two.
    let mut seen: HashSet<String> = HashSet::with_capacity(candidates.len());
    candidates.retain(|a| seen.insert(a.name.to_lowercase()));
    candidates
}

/// Is this string a name at all?
fn usable(name: &str) -> bool {
    !name.is_empty()
        && name.chars().count() <= MAX_NAME_CHARS
        // Control characters reach here from asset names that happen to be
        // valid UTF-8 without being text.
        && !name.chars().any(|c| c.is_control())
}

#[cfg(test)]
mod tests {
    use super::*;
    use policy_index::{Cip67Shape, QtyShape};

    fn evidence() -> PolicyEvidence {
        PolicyEvidence {
            policy: Some([0x11; 28]),
            distinct_name_prefixes: 1_000,
            labels: vec![721],
            qty: QtyShape {
                min: 1,
                max: 1,
                eq1: 64,
                gt1: 0,
                distinct: 1,
            },
            ..Default::default()
        }
    }

    fn verdict(e: &PolicyEvidence) -> Result<Entry, Rejected> {
        match classify(e, &Overlay::default()) {
            Verdict::Collection(entry) => Ok(*entry),
            Verdict::Rejected(r) => Err(r),
        }
    }

    #[test]
    fn a_plain_pfp_collection_is_accepted() {
        let entry = verdict(&evidence()).expect("a 1,000-asset CIP-25 1-of-1 set");
        assert_eq!(entry.assets, 1_000);
        assert!(!entry.is_hazard());
    }

    /// 🔑 **The editions clause.** Without `max <= 100`, 167 real collections
    /// are rejected. `Heads of Cards` mints 75 of each card and most assets
    /// are therefore NOT quantity 1 — the other half of the clause is what
    /// saves it.
    #[test]
    fn an_editions_collection_survives_having_no_one_of_ones() {
        let mut e = evidence();
        e.qty = QtyShape {
            min: 75,
            max: 75,
            eq1: 0,
            gt1: 64,
            distinct: 1,
        };
        assert!(
            verdict(&e).is_ok(),
            "75 of each is an edition, not a supply"
        );

        // And one above the ceiling, with no 1-of-1s, is not.
        e.qty.max = 2_500;
        e.qty.min = 2_500;
        assert_eq!(verdict(&e), Err(Rejected::SupplyShape));
    }

    /// ⚠️ NOT "is any quantity > 1". A PFP set with one mint-error twin is
    /// still an NFT collection, and the majority clause is what keeps it.
    #[test]
    fn one_accidental_duplicate_does_not_make_a_collection_fungible() {
        let mut e = evidence();
        e.qty = QtyShape {
            min: 1,
            max: 2,
            eq1: 63,
            gt1: 1,
            distinct: 2,
        };
        assert!(verdict(&e).is_ok());
    }

    #[test]
    fn the_issuers_own_fungible_tells_are_believed() {
        let mut e = evidence();
        e.has_decimals = true;
        assert_eq!(verdict(&e), Err(Rejected::FungibleLabel));

        let mut e = evidence();
        e.cip67.ft = 1;
        assert_eq!(verdict(&e), Err(Rejected::FungibleLabel));

        // A ticker only damns a handful of assets — a big collection with a
        // ticker in its metadata is still a collection.
        let mut e = evidence();
        e.has_ticker = true;
        e.distinct_name_prefixes = 4;
        assert_eq!(verdict(&e), Err(Rejected::TickerAndTiny));
        e.distinct_name_prefixes = 1_000;
        assert!(verdict(&e).is_ok());
    }

    #[test]
    fn a_one_off_mint_and_a_token_supply_are_both_out() {
        let mut e = evidence();
        e.distinct_name_prefixes = 1;
        assert_eq!(verdict(&e), Err(Rejected::SingleAsset));

        let mut e = evidence();
        e.qty.max = 1_000_000_000;
        assert_eq!(verdict(&e), Err(Rejected::SupplyTooLarge));
    }

    #[test]
    fn something_claiming_no_standard_at_all_is_out() {
        let mut e = evidence();
        e.labels = Vec::new();
        assert_eq!(verdict(&e), Err(Rejected::NoMetadataStandard));

        // A CIP-68 pair needs no 721 label.
        e.cip67.nft = 10;
        let entry = verdict(&e).expect("CIP-68 NFTs are a standard");
        assert!(entry.flags.contains(EntryFlags::CIP68));
    }

    /// ⚠️ Silence is not evidence. `max <= 100` is satisfied by an absent
    /// maximum of 0, so a policy whose spans yielded no quantity would
    /// otherwise be ACCEPTED on nothing at all.
    #[test]
    fn no_observed_quantity_is_rejected_rather_than_read_as_a_low_one() {
        let mut e = evidence();
        e.qty = QtyShape::default();
        assert_eq!(verdict(&e), Err(Rejected::NoSupplyEvidence));
    }

    /// The entry cannot be keyed without the full id, and that has to be
    /// COUNTED — a number that starts moving means the spans drifted.
    #[test]
    fn a_policy_whose_id_was_never_recovered_is_reported_not_dropped() {
        let mut e = evidence();
        e.policy = None;
        assert_eq!(verdict(&e), Err(Rejected::Unreadable));
    }

    /// ⚠️ ADA Handle is a textbook NFT collection by every structural signal
    /// and is exactly what must not be auto-ingested. It is IN the catalogue,
    /// flagged — discovery and admissibility are different questions.
    #[test]
    fn the_hazard_tier_is_listed_and_flagged_not_excluded() {
        let mut e = evidence();
        e.distinct_name_prefixes = 318_136;
        let entry = verdict(&e).expect("a real collection, however large");
        assert!(entry.is_hazard());
        assert_eq!(entry.assets, 318_136, "the ingestion DENOMINATOR, too");

        let mut e = evidence();
        e.distinct_name_prefixes = HAZARD_ASSETS - 1;
        assert!(!verdict(&e).unwrap().is_hazard(), "the threshold is >=");
    }

    /// Every source's name is kept, ordered by trust and deduped by case —
    /// a search box wants recall, not a winner.
    #[test]
    fn all_aliases_are_indexed_best_first() {
        let mut e = evidence();
        e.declared_collection = Some("Clay Nation".into());
        e.derived_from_meta = Some("clay nation".into()); // same name, worse source
        e.derived_from_onchain = Some("ClayNation".into()); // genuinely different
        let mut overlay = Overlay::default();
        overlay
            .curated
            .insert([0x11; 28], "Clay Nation by Clay Mates".into());
        overlay.tracked.insert([0x11; 28]);

        let entry = match classify(&e, &overlay) {
            Verdict::Collection(entry) => *entry,
            Verdict::Rejected(r) => panic!("rejected: {r:?}"),
        };
        assert_eq!(
            entry
                .names
                .iter()
                .map(|a| (a.name.as_str(), a.origin))
                .collect::<Vec<_>>(),
            vec![
                ("Clay Nation", NameOrigin::Declared),
                ("Clay Nation by Clay Mates", NameOrigin::CuratedJpg),
                ("ClayNation", NameOrigin::DerivedFromAssetNames),
            ],
            "the case-duplicate is dropped and the best source keeps the slot"
        );
        assert!(entry.flags.contains(EntryFlags::TRACKED));
        assert!(entry.flags.contains(EntryFlags::JPG_VERIFIED));
    }

    /// A classified collection chain could not name is still a collection
    /// that EXISTS. Dropping it would make the catalogue lie about the corpus
    /// in exactly the way the hazard tier is not allowed to.
    #[test]
    fn a_nameless_collection_is_still_an_entry() {
        let entry = verdict(&evidence()).unwrap();
        assert!(entry.names.is_empty());
        assert_eq!(entry.best_name(), None);
    }

    /// ⚠️ A 4 KB `collection` field is DROPPED, not truncated. Truncating
    /// mints a plausible-looking wrong name — the "stmasBoobert" failure.
    #[test]
    fn an_absurd_name_is_dropped_rather_than_truncated() {
        let mut e = evidence();
        let huge = "A".repeat(MAX_NAME_CHARS + 1);
        e.declared_collection = Some(huge.clone());
        e.derived_from_meta = Some("Real Name".into());
        let entry = verdict(&e).unwrap();
        assert_eq!(
            entry
                .names
                .iter()
                .map(|a| a.name.as_str())
                .collect::<Vec<_>>(),
            vec!["Real Name"],
            "the oversized one is gone entirely, not shortened"
        );

        // Exactly at the limit is fine — the boundary is inclusive.
        e.declared_collection = Some("A".repeat(MAX_NAME_CHARS));
        assert_eq!(verdict(&e).unwrap().names.len(), 2);
    }

    #[test]
    fn blank_and_control_character_names_are_not_names() {
        let mut e = evidence();
        e.declared_collection = Some("   ".into());
        e.derived_from_meta = Some("bad\u{0}name".into());
        assert!(verdict(&e).unwrap().names.is_empty());
    }

    /// The funnel is what lets the rule be re-argued, so every reason has to
    /// report as itself.
    #[test]
    fn every_rejection_reason_has_a_distinct_label() {
        let all = [
            Rejected::Unreadable,
            Rejected::FungibleLabel,
            Rejected::SingleAsset,
            Rejected::TickerAndTiny,
            Rejected::SupplyTooLarge,
            Rejected::NoMetadataStandard,
            Rejected::SupplyShape,
            Rejected::NoSupplyEvidence,
        ];
        let mut labels: Vec<&str> = all.iter().map(|r| r.as_str()).collect();
        labels.sort_unstable();
        labels.dedup();
        assert_eq!(labels.len(), all.len());
    }

    /// The `Cip67Shape` import is load-bearing for the CIP-68 arm above;
    /// keep the default honest so a field added there defaults to "absent".
    #[test]
    fn an_empty_cip67_shape_claims_nothing() {
        let s = Cip67Shape::default();
        assert_eq!((s.ft, s.nft, s.reference, s.rft), (0, 0, 0, 0));
    }
}
