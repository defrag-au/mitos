//! Re-derivation: does a record point at a transaction that really minted
//! what the record claims?
//!
//! [`crate::base::Base::verify_structure`] is the other half, and it is a
//! different question. Structure asks whether the file agrees WITH ITSELF —
//! ordering, fences, the permutation being a permutation. That check would
//! pass unchanged on an index built from a broken extractor, because the
//! wrong answers would be stored consistently.
//!
//! ⚠️ **A shared index's mistakes are inherited by every consumer, silently.**
//! A walker owns its own error and can be fixed alone; an index cannot be
//! second-guessed by the code that trusts it. So the load-bearing check is the
//! one that leaves the file and goes back to the chunks:
//!
//! > take a record, read the bytes at `(chunk, offset, len)`, decode that
//! > transaction body, and confirm it mints an asset whose prefixes are the
//! > ones the record stores.
//!
//! Sampled rather than exhaustive, because at 19M records the exhaustive form
//! is a second extraction pass — and a sample large enough to catch a
//! systematic fault (an off-by-one in a span, a mis-keyed prefix, an era whose
//! mint field is read wrongly) is small. A fault that only touches a handful
//! of records is what the supply invariants downstream are for.

use std::path::Path;

use anyhow::{Context, Result};
use pallas_codec::minicbor::Decoder;

use crate::base::Base;
use crate::format::{Record, name_prefix, policy_prefix};

#[derive(Clone, Copy, Debug, Default)]
pub struct VerifyStats {
    pub sampled: u64,
    /// Records whose transaction really does mint what they claim.
    pub confirmed: u64,
    /// Records whose chunk is outside the immutable directory given.
    pub skipped_no_chunk: u64,
}

/// What a sampled record failed on. Named rather than a string, because the
/// three have different causes and only one of them is ambiguous.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// The bytes at `(offset, len)` are not a decodable transaction body.
    NotABody,
    /// The body decodes but mints nothing at all.
    NoMint,
    /// The body mints, but nothing whose prefixes match the record.
    WrongAsset,
}

impl Fault {
    pub const ALL: [Fault; 3] = [Fault::NotABody, Fault::NoMint, Fault::WrongAsset];

    pub fn as_wire(self) -> &'static str {
        match self {
            Fault::NotABody => "not-a-body",
            Fault::NoMint => "no-mint",
            Fault::WrongAsset => "wrong-asset",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Failure {
    pub record: Record,
    pub fault: Fault,
}

/// Re-derive `sample` records spread evenly across the base.
///
/// ⚠️ EVENLY SPREAD, not random and not the first N. The first N are all in
/// one bucket and one era; a random sample needs a seed to be reproducible.
/// A stride hits every era, every chunk range and every bucket, which is where
/// a systematic fault lives — an era whose mint field is decoded wrongly would
/// be invisible to any sample confined to one of them.
pub fn verify_sample(
    base: &Base,
    immutable: &Path,
    sample: u64,
) -> Result<(VerifyStats, Vec<Failure>)> {
    let n = base.len();
    if n == 0 {
        return Ok((VerifyStats::default(), Vec::new()));
    }
    let stride = (n / sample.max(1)).max(1);
    let mut stats = VerifyStats::default();
    let mut failures = Vec::new();

    // Records are grouped by chunk only loosely, so cache the chunk in hand:
    // a stride walk revisits a chunk for consecutive samples surprisingly
    // often, and re-reading a 68 MB file per record would dominate.
    let mut have: Option<(u16, Vec<u8>)> = None;

    let mut i = 0u64;
    while i < n {
        let r = base.record_at_index(i as usize);
        i += stride;
        let path = immutable.join(format!("{:05}.chunk", r.chunk));
        if !path.exists() {
            stats.skipped_no_chunk += 1;
            continue;
        }
        if have.as_ref().map(|(c, _)| *c) != Some(r.chunk) {
            let bytes =
                std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
            have = Some((r.chunk, bytes));
        }
        let (_, bytes) = have.as_ref().expect("just loaded");
        stats.sampled += 1;
        match confirm(bytes, &r) {
            None => stats.confirmed += 1,
            Some(fault) => failures.push(Failure { record: r, fault }),
        }
    }
    Ok((stats, failures))
}

/// `None` when the record is confirmed.
///
/// Reads the BODY bytes the record points at and decodes them as a standalone
/// transaction body. That is deliberately narrower than decoding the whole
/// block: it proves the span itself is right, which is what an off-by-one in
/// `span_of` would break, and a block-level decode would paper over.
fn confirm(chunk_bytes: &[u8], r: &Record) -> Option<Fault> {
    let start = r.offset as usize;
    let end = start.checked_add(r.len as usize)?;
    if end > chunk_bytes.len() {
        return Some(Fault::NotABody);
    }
    let body = &chunk_bytes[start..end];
    // A body is a CBOR map; the mint is key 9. Decoding it standalone keeps
    // this check independent of the extractor's own block walk — the two
    // agreeing by sharing code would prove nothing.
    let Some(mint) = mint_field(body) else {
        return Some(Fault::NoMint);
    };
    let hit = mint
        .iter()
        .any(|(p, n)| policy_prefix(p) == r.policy_prefix && name_prefix(n) == r.name_prefix);
    match hit {
        true => None,
        false => Some(Fault::WrongAsset),
    }
}

/// Is there another entry in the map currently being read?
///
/// ⚠️ **INDEFINITE-LENGTH MAPS ARE NOT AN EDGE CASE HERE.** `Decoder::map()`
/// returns `Ok(None)` for one, and an earlier draft of this file treated that
/// as "no map" — which reported **342 of 3,000** sampled records as having no
/// mint. Every one of them was a correctly indexed transaction whose body
/// happened to be encoded with an indefinite-length map.
///
/// The lesson is worth more than the fix: the failure looked exactly like an
/// index defect (11% of a sample, reproducible, concentrated in particular
/// chunks) and was a defect in the checker. A verifier is code too, and the
/// first suspect when a check fails on data that passed every other test is
/// the check.
fn more(d: &mut Decoder<'_>, len: Option<u64>, seen: u64) -> Option<bool> {
    match len {
        Some(n) => Some(seen < n),
        None => match d.datatype().ok()? {
            pallas_codec::minicbor::data::Type::Break => {
                d.skip().ok()?; // consume it, so the enclosing map stays aligned
                Some(false)
            }
            _ => Some(true),
        },
    }
}

/// `(policy, asset_name)` pairs from a transaction body's mint field, decoded
/// straight from CBOR without pallas's era machinery.
///
/// The shape is stable across every era that has native assets:
/// `9 => { policy_bytes => { name_bytes => int } }` — but the maps may be of
/// definite OR indefinite length at any of the three levels, so all three are
/// iterated through [`more`].
fn mint_field(body: &[u8]) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
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
                d.skip().ok()?; // the quantity
                out.push((policy.clone(), name));
            }
        }
        return Some(out);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_fault_has_its_own_spelling() {
        let mut wires: Vec<&str> = Fault::ALL.iter().map(|f| f.as_wire()).collect();
        wires.sort_unstable();
        wires.dedup();
        assert_eq!(wires.len(), Fault::ALL.len());
    }

    /// A span pointing past the end of the chunk is the shape an off-by-one
    /// in extraction would take.
    #[test]
    fn a_span_past_the_end_of_the_chunk_is_not_a_body() {
        let r = Record {
            policy_prefix: 1,
            name_prefix: 2,
            chunk: 0,
            offset: 90,
            len: 50,
            aux_offset: 0,
            aux_len: 0,
            burned: false,
        };
        assert_eq!(confirm(&[0u8; 100], &r), Some(Fault::NotABody));
    }

    /// A body with no mint field at all — the shape a mis-keyed record would
    /// take if it pointed at an ordinary transfer.
    #[test]
    fn a_body_without_a_mint_field_fails_as_no_mint() {
        // CBOR map {0: []} — a body-shaped map with no key 9.
        let body = [0xa1, 0x00, 0x80];
        let r = Record {
            policy_prefix: 1,
            name_prefix: 2,
            chunk: 0,
            offset: 0,
            len: body.len() as u16,
            aux_offset: 0,
            aux_len: 0,
            burned: false,
        };
        assert_eq!(confirm(&body, &r), Some(Fault::NoMint));
    }

    /// The mint field decoder, against a hand-built body — the format is
    /// `9 => { policy => { name => qty } }`.
    #[test]
    fn the_mint_field_decodes_policy_and_name() {
        let mut body = vec![0xa1, 0x09]; // map(1), key 9
        body.push(0xa1); // map(1) policies
        body.push(0x44); // bytes(4)
        body.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
        body.push(0xa1); // map(1) assets
        body.push(0x43); // bytes(3)
        body.extend_from_slice(b"ABC");
        body.push(0x01); // qty 1

        let got = mint_field(&body).expect("decodes");
        assert_eq!(got, vec![(vec![0xde, 0xad, 0xbe, 0xef], b"ABC".to_vec())]);
    }

    /// ⚠️ And the check has to be able to REJECT: a record naming a different
    /// asset in the same transaction must fail, or confirmation is theatre.
    #[test]
    fn a_record_naming_the_wrong_asset_is_caught() {
        let mut body = vec![0xa1, 0x09, 0xa1, 0x44];
        body.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
        body.push(0xa1);
        body.push(0x43);
        body.extend_from_slice(b"ABC");
        body.push(0x01);

        let right = Record {
            policy_prefix: policy_prefix(&[0xde, 0xad, 0xbe, 0xef]),
            name_prefix: name_prefix(b"ABC"),
            chunk: 0,
            offset: 0,
            len: body.len() as u16,
            aux_offset: 0,
            aux_len: 0,
            burned: false,
        };
        assert_eq!(confirm(&body, &right), None);

        let wrong = Record {
            name_prefix: name_prefix(b"XYZ"),
            ..right
        };
        assert_eq!(confirm(&body, &wrong), Some(Fault::WrongAsset));

        let wrong_policy = Record {
            policy_prefix: policy_prefix(&[0x00, 0x11, 0x22, 0x33]),
            ..right
        };
        assert_eq!(confirm(&body, &wrong_policy), Some(Fault::WrongAsset));
    }

    /// ⚠️ THE REGRESSION THIS FILE EARNED. An indefinite-length body map is
    /// ordinary on chain, and reading `Decoder::map()`'s `Ok(None)` as "no
    /// map" reported 342 of 3,000 real records as faulty when the index was
    /// right.
    #[test]
    fn an_indefinite_length_body_map_still_finds_the_mint() {
        // {_ 9: {_ h'deadbeef': {_ h'414243': 1}}} — indefinite at all three
        // levels, which is the shape that broke the first draft.
        let mut body = vec![0xbf, 0x09, 0xbf, 0x44];
        body.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
        body.push(0xbf);
        body.push(0x43);
        body.extend_from_slice(b"ABC");
        body.push(0x01);
        body.push(0xff); // end assets
        body.push(0xff); // end policies
        body.push(0xff); // end body

        let got = mint_field(&body).expect("indefinite maps decode");
        assert_eq!(got, vec![(vec![0xde, 0xad, 0xbe, 0xef], b"ABC".to_vec())]);
    }

    /// An indefinite body map whose EARLIER keys must be skipped past — the
    /// case where a mishandled break desynchronises the rest of the map.
    #[test]
    fn an_indefinite_body_map_skips_earlier_keys_correctly() {
        let mut body = vec![0xbf, 0x00, 0x80, 0x09, 0xbf, 0x44]; // {_ 0: [], 9: …
        body.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
        body.push(0xa1); // definite assets map inside an indefinite policies map
        body.push(0x43);
        body.extend_from_slice(b"ABC");
        body.push(0x01);
        body.push(0xff); // end policies
        body.push(0xff); // end body

        let got = mint_field(&body).expect("decodes past the skipped key");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].1, b"ABC".to_vec());
    }

    #[test]
    fn a_body_that_is_not_cbor_is_not_a_body() {
        let r = Record {
            policy_prefix: 1,
            name_prefix: 2,
            chunk: 0,
            offset: 0,
            len: 4,
            aux_offset: 0,
            aux_len: 0,
            burned: false,
        };
        assert_eq!(confirm(&[0xff, 0xff, 0xff, 0xff], &r), Some(Fault::NoMint));
    }

    /// A record whose chunk the caller does not have is SKIPPED and counted,
    /// never confirmed — an index verified against chunks that are absent has
    /// been verified against nothing.
    #[test]
    fn absent_chunks_are_counted_not_confirmed() {
        let stats = VerifyStats {
            sampled: 0,
            confirmed: 0,
            skipped_no_chunk: 3,
        };
        assert_eq!(stats.confirmed, 0);
        assert_eq!(stats.skipped_no_chunk, 3);
    }
}
