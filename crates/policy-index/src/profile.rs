//! What IS this policy? — names and unit class, read back off chain through
//! the index's two spans.
//!
//! Each [`Record`] carries the span of the minting transaction's BODY and the
//! span of that transaction's AUXILIARY DATA, which is where CIP-25 lives.
//! This module reads both and returns [`PolicyEvidence`]: what was observed,
//! not what it means.
//!
//! # ⚠️ EVIDENCE, NOT A CLASSIFICATION
//!
//! Nothing here decides whether a policy is an NFT collection, and that is the
//! point. The rule was derived FROM this output, scored offline against the
//! 38,992 curated jpg.store names that are the only answer key available; a
//! module that both produced the evidence and rendered the verdict would have
//! no way to be re-scored when the rule moves.
//!
//! Two consumers, deliberately:
//!
//! - `examples/profile` — the SURVEY INSTRUMENT. One JSONL row per policy, no
//!   filtering, for offline scoring. It is how the classifier gets re-scored,
//!   so it must keep working.
//! - `policy-ledger catalogue build` — applies the classifier to the same
//!   evidence and emits the published artifact.
//!
//! Both read this module. A derivation that drifted between them would mean
//! the catalogue shipped names the survey never scored.
//!
//! # Why this is cheap
//!
//! Per policy it is TWO `pread`s of at most 64 KiB each (both spans are `u16`
//! lengths), not a chunk read. An earlier draft read the whole 68 MB chunk per
//! sampled policy the way `verify` does — right for a stride walk that
//! revisits chunks, and catastrophically wrong here, where a sample spreads
//! across nearly every chunk: 3,000 policies would have read ~200 GB.
//!
//! MEASURED 2026-09-15 on cardano-infra: all 231,616 policies in 2 m 04 s,
//! zero span decode failures.

use std::collections::BTreeSet;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use pallas_codec::minicbor::Decoder;

use crate::format::Record;

// ---------------------------------------------------------------------------
// Generic CBOR, because metadata is user-supplied and era-shaped
// ---------------------------------------------------------------------------

/// A CBOR value, decoded without pallas's era machinery.
///
/// Auxiliary data changes shape across eras (a bare metadata map in Shelley,
/// an array in Allegra, a tag-259 map from Alonzo) and the metadata INSIDE it
/// is arbitrary user data. Decoding into era types would reject exactly the
/// malformed-but-present metadata this survey most wants to count.
///
/// Every variant must exist for the decode to stay aligned even where a
/// consumer never reads the payload — a skipped integer and a mis-typed one
/// desynchronise the enclosing map identically.
#[derive(Clone, Debug, PartialEq)]
pub enum Val {
    U(u64),
    I(i64),
    B(Vec<u8>),
    T(String),
    A(Vec<Val>),
    M(Vec<(Val, Val)>),
    Other,
}

impl Val {
    /// Map lookup by text key, CASE-INSENSITIVELY.
    ///
    /// ⚠️ Not fastidiousness. A census over 38,992 policies found `collection`
    /// on 9.7% and `Collection` on a further 6.9% — they are different keys in
    /// CBOR and an exact-match lookup silently halves the coverage of the
    /// single best name signal there is (19.8% → 32.0%).
    pub fn get(&self, key: &str) -> Option<&Val> {
        match self {
            Val::M(entries) => entries.iter().find_map(|(k, v)| match k {
                Val::T(s) if s.eq_ignore_ascii_case(key) => Some(v),
                _ => None,
            }),
            _ => None,
        }
    }

    /// Does this map carry a key, case-insensitively?
    pub fn has(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    pub fn as_text(&self) -> Option<String> {
        match self {
            Val::T(s) => Some(s.clone()),
            // CIP-25 long strings are chunked into an array of ≤64-byte parts.
            Val::A(parts) => {
                let mut out = String::new();
                for p in parts {
                    match p {
                        Val::T(s) => out.push_str(s),
                        _ => return None,
                    }
                }
                Some(out)
            }
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&[(Val, Val)]> {
        match self {
            Val::M(e) => Some(e),
            _ => None,
        }
    }
}

/// Is there another entry in the container currently being read?
///
/// ⚠️ Indefinite-length containers are not an edge case in this corpus — see
/// the header of `verify::more`, where treating `Ok(None)` as "no map" once
/// reported 342 of 3,000 sound records as faulty. Same trap, same handling.
fn more(d: &mut Decoder<'_>, len: Option<u64>, seen: u64) -> Option<bool> {
    match len {
        Some(n) => Some(seen < n),
        None => match d.datatype().ok()? {
            pallas_codec::minicbor::data::Type::Break => {
                d.skip().ok()?;
                Some(false)
            }
            _ => Some(true),
        },
    }
}

/// Decode one CBOR value generically. `depth` guards against a hostile nest.
pub fn value(d: &mut Decoder<'_>, depth: u32) -> Option<Val> {
    use pallas_codec::minicbor::data::Type as T;
    if depth > 24 {
        d.skip().ok()?;
        return Some(Val::Other);
    }
    Some(match d.datatype().ok()? {
        T::U8 | T::U16 | T::U32 | T::U64 => Val::U(d.u64().ok()?),
        T::I8 | T::I16 | T::I32 | T::I64 => Val::I(d.i64().ok()?),
        T::Bytes | T::BytesIndef => Val::B(d.bytes().ok()?.to_vec()),
        T::String | T::StringIndef => Val::T(d.str().ok()?.to_string()),
        T::Array | T::ArrayIndef => {
            let n = d.array().ok()?;
            let mut out = Vec::new();
            let mut i = 0u64;
            while more(d, n, i)? {
                i += 1;
                out.push(value(d, depth + 1)?);
            }
            Val::A(out)
        }
        T::Map | T::MapIndef => {
            let n = d.map().ok()?;
            let mut out = Vec::new();
            let mut i = 0u64;
            while more(d, n, i)? {
                i += 1;
                let k = value(d, depth + 1)?;
                let v = value(d, depth + 1)?;
                out.push((k, v));
            }
            Val::M(out)
        }
        T::Tag => {
            // Alonzo auxiliary data is tag 259 around the map. Transparent
            // here: the tag says how to read the payload, not what it means.
            d.tag().ok()?;
            value(d, depth + 1)?
        }
        _ => {
            d.skip().ok()?;
            Val::Other
        }
    })
}

// ---------------------------------------------------------------------------
// The two spans
// ---------------------------------------------------------------------------

/// One entry of a mint field: which policy, which asset name, how many.
/// Negative means a burn.
pub type MintEntry = (Vec<u8>, Vec<u8>, i64);

/// `(policy, asset_name, quantity)` from a body's mint field.
///
/// `verify::mint_field` deliberately skips the quantity — it only needs to
/// confirm a span points at the right asset. The quantity is the whole
/// question here, so this keeps it. Negative means a burn.
pub fn mint_with_qty(body: &[u8]) -> Option<Vec<MintEntry>> {
    let mut d = Decoder::new(body);
    let outer = d.map().ok()?;
    let mut i = 0u64;
    while more(&mut d, outer, i)? {
        i += 1;
        let key = d.u32().ok()?;
        if key != 9 {
            d.skip().ok()?;
            continue;
        }
        let policies = d.map().ok()?;
        let mut out = Vec::new();
        let mut p = 0u64;
        while more(&mut d, policies, p)? {
            p += 1;
            let policy = d.bytes().ok()?.to_vec();
            let assets = d.map().ok()?;
            let mut a = 0u64;
            while more(&mut d, assets, a)? {
                a += 1;
                let name = d.bytes().ok()?.to_vec();
                // Quantity is i64 in every era that has native assets, but a
                // malformed one must not abort the whole policy — record 0 and
                // let the caller see it as "no usable quantity".
                let qty = match d.datatype().ok()? {
                    pallas_codec::minicbor::data::Type::U8
                    | pallas_codec::minicbor::data::Type::U16
                    | pallas_codec::minicbor::data::Type::U32
                    | pallas_codec::minicbor::data::Type::U64 => d.u64().ok()? as i64,
                    _ => d.i64().ok().unwrap_or(0),
                };
                out.push((policy.clone(), name, qty));
            }
        }
        return Some(out);
    }
    None
}

/// The metadata map inside auxiliary data, whatever era shape wraps it.
pub fn metadata_of(aux: &[u8]) -> Option<Vec<(Val, Val)>> {
    let mut d = Decoder::new(aux);
    let v = value(&mut d, 0)?;
    match v {
        // Allegra+: [metadata, native_scripts, ...]
        Val::A(items) => items.into_iter().find_map(|i| match i {
            Val::M(m) => Some(m),
            _ => None,
        }),
        // Alonzo+ (tag stripped by `value`): { 0 => metadata, 1 => .. }
        // Shelley: the metadata map itself.
        Val::M(entries) => {
            let keyed = entries.iter().find_map(|(k, v)| match (k, v) {
                (Val::U(0), Val::M(m)) => Some(m.clone()),
                _ => None,
            });
            Some(keyed.unwrap_or(entries))
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Evidence
// ---------------------------------------------------------------------------

/// The supply shape of the assets observed.
///
/// ⚠️ Evidence, never a verdict. An art collection with 10 editions of each
/// piece, or a PFP set with one accidental duplicate from a minting error, is
/// still an NFT collection.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QtyShape {
    pub min: i64,
    pub max: i64,
    /// Assets minted with exactly 1 — the NFT shape.
    pub eq1: u32,
    /// Assets minted with more than 1. Editions and mint-error twins live
    /// here alongside genuine fungibles, which is the point.
    pub gt1: u32,
    /// Distinct quantities seen. An editions collection has few (10, 25);
    /// a fungible usually has one enormous one.
    pub distinct: u32,
}

/// How the observed asset names label themselves under CIP-67.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Cip67Shape {
    /// 000643b0 — the (100) reference token of a CIP-68 pair.
    pub reference: u32,
    /// 000de140 — (222) NFT.
    pub nft: u32,
    /// 0014df10 — (333) FT.
    pub ft: u32,
    /// 001bc280 — (444) rich fungible.
    pub rft: u32,
    /// No recognised CIP-67 label: a plain CIP-25-era asset name.
    pub unlabelled: u32,
}

/// What was observed about one policy. Every field is an observation.
#[derive(Clone, Debug, Default)]
pub struct PolicyEvidence {
    /// Full 28-byte policy id, recovered from the mint field. The index only
    /// stores an 8-byte prefix, so this is the first place it exists.
    pub policy: Option<[u8; 28]>,
    /// The index's 8-byte key for this policy.
    pub prefix: u64,
    /// Mint AND burn events in the index for this policy.
    pub events: u32,
    pub burn_events: u32,
    /// Distinct asset name prefixes in the index — the policy's SIZE, known
    /// without reading a chunk, and the catalogue's `assets` field.
    pub distinct_name_prefixes: u32,
    pub first_chunk: u16,
    /// Transactions actually read for this policy.
    pub sampled_txs: u32,
    /// Distinct full asset names observed in those transactions.
    pub assets_seen: u32,
    pub qty: QtyShape,
    pub cip67: Cip67Shape,
    /// Top-level metadata labels present (721 = CIP-25, 777 = CIP-27 royalty).
    pub labels: Vec<u64>,
    /// Assets carrying a CIP-25 entry in the transactions read.
    pub cip25_assets: u32,
    /// A sample of CIP-25 `name` values, for the derivation and for eyeballing.
    pub names: Vec<String>,
    /// On-chain asset names as UTF-8, where they are valid UTF-8.
    pub onchain_names: Vec<String>,
    /// Longest common prefix over `names`, trimmed of numbering punctuation.
    pub derived_from_meta: Option<String>,
    /// The same over `onchain_names`.
    pub derived_from_onchain: Option<String>,
    /// A `collection`-ish key found directly in the CIP-25 asset entry, if any.
    pub declared_collection: Option<String>,
    /// Every attribute key seen in this policy's CIP-25 asset entries.
    ///
    /// A census, not a lookup: `name` is the only key CIP-25 mandates, so
    /// whether a better collection-level field exists in practice is a
    /// question about what creators actually wrote, and guessing key names to
    /// probe for would only ever confirm the guess.
    pub cip25_keys: Vec<String>,
    /// A `ticker` appeared in a CIP-25 entry — an issuer treating this as a
    /// fungible token.
    pub has_ticker: bool,
    /// A `decimals` appeared — the same tell, and the stronger of the two.
    pub has_decimals: bool,
    /// Spans that could not be read or decoded, so a zero above can be told
    /// apart from a failure.
    pub body_decode_failures: u32,
    pub aux_decode_failures: u32,
}

impl PolicyEvidence {
    /// The policy id as 56-hex, when the mint field yielded it.
    pub fn policy_hex(&self) -> Option<String> {
        self.policy.map(hex::encode)
    }
}

/// Longest common prefix, then trimmed back past any numbering tail.
///
/// "SpaceBudz #1234" / "SpaceBudz #1235" share "SpaceBudz #123" — the raw LCP
/// eats into the number, so the trim is not cosmetic. Returns `None` for a
/// prefix too short to be a name, which is the honest answer for a policy
/// whose assets share nothing.
pub fn common_prefix(names: &[String]) -> Option<String> {
    let first = names.first()?;
    let mut end = first.len();
    for n in &names[1..] {
        let common = first
            .chars()
            .zip(n.chars())
            .take_while(|(a, b)| a == b)
            .map(|(a, _)| a.len_utf8())
            .sum::<usize>();
        end = end.min(common);
    }
    // A single name gets the same trim as a shared prefix: a 1-of-1 art piece
    // is named "Collection | 060", and the number is no more part of the
    // collection's name there than it is in "SpaceBudz #1234".
    Some(trim_numbering(&first[..end])).filter(|s| s.chars().count() >= 2)
}

/// Trim a numbering tail: digits, then the punctuation that introduced them.
///
/// Done in two passes on purpose. One combined character class would eat
/// "Vol" out of "Clay Nation Vol" — the digits have to go first so the
/// separator trim only ever runs against what the digits left behind.
pub fn trim_numbering(s: &str) -> String {
    let no_digits = s.trim_end_matches(|c: char| c.is_ascii_digit());
    no_digits
        .trim_end_matches(|c: char| {
            matches!(
                c,
                '#' | '-' | '_' | ' ' | '.' | '/' | '(' | '[' | '|' | ':' | ','
            )
        })
        .to_string()
}

/// The four CIP-67 label prefixes in use: (100) reference, (222) NFT,
/// (333) FT, (444) rich fungible.
pub const CIP67_LABELS: [&str; 4] = ["000643b0", "000de140", "0014df10", "001bc280"];

pub fn has_cip67_label(name: &[u8]) -> bool {
    name.get(..4)
        .map(hex::encode)
        .is_some_and(|h| CIP67_LABELS.contains(&h.as_str()))
}

fn cip67_bucket(name: &[u8], shape: &mut Cip67Shape) {
    match name.get(..4).map(hex::encode).as_deref() {
        Some("000643b0") => shape.reference += 1,
        Some("000de140") => shape.nft += 1,
        Some("0014df10") => shape.ft += 1,
        Some("001bc280") => shape.rft += 1,
        _ => shape.unlabelled += 1,
    }
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

/// One open chunk file, reused across consecutive tasks in the same chunk.
///
/// Callers should visit policies in chunk order — the handle is a cache of
/// one, and the whole corpus in first-chunk order opens ~9,000 files instead
/// of 231,616.
pub struct ChunkReader {
    immutable: PathBuf,
    open: Option<(u16, File)>,
}

impl ChunkReader {
    pub fn new(immutable: impl AsRef<Path>) -> Self {
        Self {
            immutable: immutable.as_ref().to_path_buf(),
            open: None,
        }
    }

    fn get(&mut self, chunk: u16) -> Option<&File> {
        if self.open.as_ref().map(|(c, _)| *c) != Some(chunk) {
            let path = self.immutable.join(format!("{chunk:05}.chunk"));
            let f = File::open(&path).ok()?;
            self.open = Some((chunk, f));
        }
        self.open.as_ref().map(|(_, f)| f)
    }

    /// One span, by `pread`. `None` for a zero length (no aux data) or an
    /// unreadable chunk.
    pub fn span(&mut self, chunk: u16, offset: u32, len: u16) -> Option<Vec<u8>> {
        if len == 0 {
            return None;
        }
        let f = self.get(chunk)?;
        let mut buf = vec![0u8; len as usize];
        f.read_exact_at(&mut buf, offset as u64).ok()?;
        Some(buf)
    }
}

/// How many transactions to read per policy.
///
/// One is not enough: a 1-of-1 art policy mints one asset per transaction, so
/// a single read yields a single name and no common prefix to measure. More
/// than a handful buys little — a batch mint puts the whole collection in the
/// first transaction anyway.
pub const TXS_PER_POLICY: usize = 8;

/// Cap on names carried into the evidence, so one 10,000-asset batch mint does
/// not dominate. The derivation runs over the same capped set.
pub const NAME_SAMPLE: usize = 64;

/// Read a policy's spans and report what was there.
///
/// `records` must be the policy's whole run (`Base::records_of`, or the slice
/// a whole-corpus walk already holds).
pub fn profile_policy(files: &mut ChunkReader, prefix: u64, records: &[Record]) -> PolicyEvidence {
    let mut distinct_names = BTreeSet::new();
    for r in records {
        distinct_names.insert(r.name_prefix);
    }

    let mut out = PolicyEvidence {
        prefix,
        events: records.len() as u32,
        burn_events: records.iter().filter(|r| r.burned).count() as u32,
        distinct_name_prefixes: distinct_names.len() as u32,
        first_chunk: records.iter().map(|r| r.chunk).min().unwrap_or(0),
        ..Default::default()
    };

    // Distinct transactions, oldest first — then a STRIDE across them, not
    // the first N.
    //
    // ⚠️ MEASURED, not theoretical. Taking the earliest 8 transactions derived
    // "Cardano Kidz" as "Cardano Kidz NFT 0011": a policy that mints in dated
    // sub-series puts one series in its opening transactions, so a
    // head-anchored sample sees a prefix the collection as a whole does not
    // share. This is the same trap `verify_sample` documents, and the first
    // draft of the profiler walked straight into it after quoting the warning.
    let mut spans: Vec<&Record> = records.iter().filter(|r| !r.burned).collect();
    spans.sort_by_key(|r| (r.chunk, r.offset));
    spans.dedup_by_key(|r| (r.chunk, r.offset));
    let stride = spans.len().div_ceil(TXS_PER_POLICY).max(1);
    // The first mint is kept unconditionally — it is the one transaction a
    // real implementation is certain to have, and the floor probe's answer.
    let mut spans: Vec<&Record> = spans
        .first()
        .copied()
        .into_iter()
        .chain(spans.iter().skip(1).step_by(stride).copied())
        .collect();
    spans.truncate(TXS_PER_POLICY);

    let mut qtys = BTreeSet::new();
    let mut assets = BTreeSet::new();
    let mut labels = BTreeSet::new();
    let mut cip25_keys = BTreeSet::new();
    let mut names: Vec<String> = Vec::new();
    let mut onchain: Vec<String> = Vec::new();

    for r in spans {
        let Some(body) = files.span(r.chunk, r.offset, r.len) else {
            out.body_decode_failures += 1;
            continue;
        };
        let Some(mints) = mint_with_qty(&body) else {
            out.body_decode_failures += 1;
            continue;
        };
        out.sampled_txs += 1;

        // The mint map may carry several policies; keep only ours. ⚠️ Through
        // the crate's own `policy_prefix`, never a hand-rolled `from_*_bytes`
        // — it is big-endian while the record's on-disk encoding is
        // little-endian, and getting that backwards silently matches nothing
        // and reports every policy as unreadable.
        let mine: Vec<_> = mints
            .iter()
            .filter(|(p, _, _)| crate::format::policy_prefix(p) == prefix)
            .collect();
        if let Some((p, _, _)) = mine.first()
            && out.policy.is_none()
        {
            out.policy = p.as_slice().try_into().ok();
        }

        for (_, name, qty) in &mine {
            if assets.insert(name.clone()) {
                cip67_bucket(name, &mut out.cip67);
                // ⚠️ Strip the 4-byte CIP-67 label ONLY when there is one.
                // An earlier draft stripped unconditionally and turned
                // "Boobert's First Christmas" into "stmasBoobert" — a
                // front-truncation that still looks like a plausible name,
                // which is why it survived a read-through and only fell out
                // of scoring against curated names.
                let bare = match has_cip67_label(name) {
                    true => name.get(4..).unwrap_or(name),
                    false => name.as_slice(),
                };
                if let Ok(s) = std::str::from_utf8(bare)
                    && !s.is_empty()
                    && onchain.len() < NAME_SAMPLE
                {
                    onchain.push(s.to_string());
                }
            }
            qtys.insert(*qty);
            if *qty == 1 {
                out.qty.eq1 += 1;
            } else if *qty > 1 {
                out.qty.gt1 += 1;
            }
        }

        // The aux span — the pointer nothing read until the catalogue needed it.
        let Some(aux) = files.span(r.chunk, r.aux_offset, r.aux_len) else {
            continue;
        };
        let Some(meta) = metadata_of(&aux) else {
            out.aux_decode_failures += 1;
            continue;
        };
        for (k, v) in &meta {
            if let Val::U(label) = k {
                labels.insert(*label);
                if *label != 721 {
                    continue;
                }
                // 721 => { policy => { asset => { name, .. } }, version }
                let Some(policies) = v.as_map() else { continue };
                for (_, per_policy) in policies {
                    let Some(entries) = per_policy.as_map() else {
                        continue;
                    };
                    for (asset_key, attrs) in entries {
                        if matches!(asset_key, Val::T(s) if s == "version") {
                            continue;
                        }
                        out.cip25_assets += 1;
                        if let Some(n) = attrs.get("name").and_then(|n| n.as_text())
                            && names.len() < NAME_SAMPLE
                        {
                            names.push(n);
                        }
                        if let Some(entries) = attrs.as_map() {
                            for (ak, _) in entries {
                                if let Val::T(k) = ak {
                                    cip25_keys.insert(k.clone());
                                }
                            }
                        }
                        if out.declared_collection.is_none() {
                            out.declared_collection = attrs
                                .get("collection")
                                .or_else(|| attrs.get("project"))
                                .and_then(|c| c.as_text())
                                .filter(|s| !s.trim().is_empty());
                        }
                        // FT tells: CIP-25 was written for NFTs, but fungible
                        // issuers reuse it and reach for these two. Recorded
                        // as evidence, never as the verdict — an art edition
                        // has a quantity too, and no ticker.
                        out.has_ticker |= attrs.has("ticker");
                        out.has_decimals |= attrs.has("decimals");
                    }
                }
            }
        }
    }

    out.assets_seen = assets.len() as u32;
    out.qty.min = qtys.iter().copied().min().unwrap_or(0);
    out.qty.max = qtys.iter().copied().max().unwrap_or(0);
    out.qty.distinct = qtys.len() as u32;
    out.labels = labels.into_iter().collect();
    out.cip25_keys = cip25_keys.into_iter().collect();
    out.derived_from_meta = common_prefix(&names);
    out.derived_from_onchain = common_prefix(&onchain);
    out.names = names;
    out.onchain_names = onchain;
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The raw LCP of "SpaceBudz #1234" and "SpaceBudz #1235" eats into the
    /// number; the trim is what makes the derivation usable.
    #[test]
    fn a_common_prefix_is_trimmed_back_past_its_numbering() {
        let names = vec!["SpaceBudz #1234".to_string(), "SpaceBudz #1235".to_string()];
        assert_eq!(common_prefix(&names).as_deref(), Some("SpaceBudz"));
    }

    /// ⚠️ The digits go FIRST. One combined character class eats "Vol" out of
    /// "Clay Nation Vol 3".
    #[test]
    fn trimming_does_not_eat_a_word_that_precedes_the_number() {
        assert_eq!(trim_numbering("Clay Nation Vol 3"), "Clay Nation Vol");
        assert_eq!(trim_numbering("Cardano Kidz #0011"), "Cardano Kidz");
    }

    /// A policy whose assets share nothing has no derivable name, and saying
    /// so is the honest answer — a one-character "prefix" is not a name.
    #[test]
    fn nothing_in_common_derives_nothing() {
        let names = vec!["alpha".to_string(), "beta".to_string()];
        assert_eq!(common_prefix(&names), None);
        assert_eq!(common_prefix(&[]), None);
    }

    /// ⚠️ "Boobert's First Christmas" became "stmasBoobert" when the 4-byte
    /// CIP-67 label was stripped unconditionally — a front-truncation that
    /// still reads as a plausible name, which is why review missed it.
    #[test]
    fn a_cip67_label_is_only_stripped_when_one_is_present() {
        let labelled = hex::decode("000de140536e656b").unwrap(); // (222) + "Snek"
        assert!(has_cip67_label(&labelled));
        assert_eq!(&labelled[4..], b"Snek");

        let plain = b"Boobert's First Christmas";
        assert!(!has_cip67_label(plain), "no label here to strip");
    }

    /// `collection` and `Collection` are DIFFERENT CBOR KEYS, and the
    /// case-insensitive lookup is what took the best name signal from 19.8%
    /// to 32.0% coverage.
    #[test]
    fn a_map_lookup_ignores_the_key_case() {
        let m = Val::M(vec![(
            Val::T("Collection".into()),
            Val::T("Clay Nation".into()),
        )]);
        assert_eq!(
            m.get("collection").and_then(|v| v.as_text()).as_deref(),
            Some("Clay Nation")
        );
        assert!(m.has("COLLECTION"));
        assert!(!m.has("project"));
    }

    /// CIP-25 chunks a long string into ≤64-byte parts; a consumer reading
    /// only `Val::T` silently loses every description and long name.
    #[test]
    fn a_chunked_string_reads_as_one_text() {
        let v = Val::A(vec![Val::T("Clay ".into()), Val::T("Nation".into())]);
        assert_eq!(v.as_text().as_deref(), Some("Clay Nation"));
        // A mixed array is not a chunked string.
        assert_eq!(Val::A(vec![Val::T("a".into()), Val::U(1)]).as_text(), None);
    }
}
