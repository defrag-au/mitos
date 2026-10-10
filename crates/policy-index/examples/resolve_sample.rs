//! `resolve_sample` — the SAMPLED RESOLVED BUILD, gate 2 of
//! `cnft.dev-workers/docs/design/ASSET_METADATA_INDEX.md`.
//!
//! For every policy in the policy index: take a stride sample of up to
//! `--txs-per-policy` minting transactions, read each one's body + auxiliary
//! span straight out of the chunks, resolve its CIP-25 document through
//! `cardano-assets`, emit one resolved record per minted asset, and assemble
//! the result into real `asset-metadata-index` segments, directory and
//! pointer. The numbers and the fixture bytes fall out of the same run.
//!
//! # The questions this exists to answer
//!
//! - bytes per record, and how much of that is the payload;
//! - the `(chunk, policy)` slice distribution — the format's one UNRESOLVED;
//! - the CIP-68 inline vs hash-attached split — how much tail the datum
//!   capture has to serve;
//! - build wall time for the tx-sampled work (the backfill's unit cost);
//! - the busiest chunks' segment sizes.
//!
//! # ⚠️ Sampled, never a backfill
//!
//! Each policy contributes at most `--txs-per-policy` transactions, so a
//! segment written here holds the sampled transactions' assets only. The
//! per-record shape is real, and the worst-case slice is present whenever it
//! lives in a single transaction (the first transaction of every policy is
//! kept unconditionally); the totals are not. The slice report therefore
//! counts events from the FULL policy index — the counts are exact — and
//! prices them at the measured bytes, which is an estimate. The written
//! artifact is fixture material, not a publishable build.
//!
//! # CIP-68: the reference-token datum, three ways
//!
//! A CIP-68 asset's metadata lives on its reference token (`000643b0` + the
//! asset's suffix) as a Plutus datum, in a transaction's output — not in the
//! aux span. The sampler walks the minting body's outputs generically (no
//! era guess) and resolves the datum by whichever of the three routes the
//! chain used: an **inline** datum (unwrapped from the ledger's
//! `#6.24(bytes .cbor plutus_data)` envelope), a **hash-attached** datum (the
//! preimage looked up in the published capture, `--datum-set`), or a user
//! token minted apart from its reference token (the **origin chase**: the
//! reference token is itself an indexed mint, so its own record leads back
//! to the datum). The record's headline fields come from the platform's own
//! `decode_cip68_datum`, so the index cannot disagree with a live consumer
//! about what a datum means.
//!
//! # The payload
//!
//! A record's payload is the asset's own label-721 entry, re-encoded
//! canonically from the decoded `Metadatum` (minicbor, definite lengths). It
//! is semantically the chain's document; a byte-exact span slice would differ
//! only in CBOR framing and is deferred with the format's other v1 omissions.
//! A reader decodes it as `minicbor::decode::<Metadatum>`, then calls
//! `cardano_assets::decode_document` — the same call this builder used, so
//! the two cannot disagree about what a document means.
//!
//! ```text
//! cargo run --release --example resolve_sample -- \
//!     --index-dir /opt/policy-index \
//!     --immutable /opt/market-ledger/snapshot-full/db/immutable \
//!     --out /opt/scratchpad/ami-sample \
//!     --datum-set /srv/data/datum-set/publish
//! ```

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use asset_metadata_index as ami;
use cardano_assets::{Asset, decode_cip68_datum, decode_document};
use live_book::format::DatumTable;
use live_book::publish::{DatumCatalogue, datum_catalogue_key};
use pallas_codec::minicbor;
use pallas_codec::minicbor::data::Type;
use pallas_crypto::hash::Hasher;
use pallas_primitives::Metadatum;
use pallas_primitives::alonzo::AuxiliaryData;
use policy_index::profile::{ChunkReader, TXS_PER_POLICY};
use policy_index::{Base, Record as IndexRecord, base_path};

/// 28-byte policy ids, as the record format states them.
type PolicyId = [u8; ami::POLICY_ID_LEN];

// ---------------------------------------------------------------------------
// The build accumulator
// ---------------------------------------------------------------------------

/// Everything the sample carries between the read loop and the write phase.
#[derive(Default)]
struct Build {
    /// chunk → policy → records, the segment assembly input.
    segments: BTreeMap<u16, BTreeMap<PolicyId, Vec<ami::Record>>>,
    /// policy → chunks, the directory assembly input.
    directory: BTreeMap<PolicyId, BTreeSet<u16>>,
}

/// Exact event counts for one `(chunk, policy-prefix)` slice, from the full
/// index. Prefix granularity: the index's records do not carry the full
/// policy id (only the mint field does), and for counting it does not matter —
/// a prefix collision merges two policies' slices, which the sample's own
/// records would have to guard against but the distribution does not.
#[derive(Default, Clone, Copy)]
struct SliceEvents {
    mints: u32,
    burns: u32,
}

// ---------------------------------------------------------------------------
// Counters
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Stats {
    policies: u64,
    policies_empty: u64,
    txs_sampled: u64,
    txs_read: u64,
    body_bytes: u64,
    aux_bytes: u64,
    body_missing: u64,
    body_decode_failures: u64,
    candidate_not_ours: u64,
    entries_other_policies: u64,
    txs_no_aux: u64,
    txs_aux_decode_failed: u64,
    txs_no_721: u64,
    policy_absent_from_721: u64,
    burn_entries_skipped: u64,
    records: u64,
    cip25_records: u64,
    cip25_declined: u64,
    cip68_records: u64,
    cip68_inline: u64,
    cip68_hash_in_window: u64,
    cip68_hash_by_sweep: u64,
    cip68_hash_missed: u64,
    cip68_no_datum: u64,
    cip68_unknown_datum: u64,
    cip68_pair_absent: u64,
    cip68_chases: u64,
    cip68_origin_resolved: u64,
    cip68_own_token: u64,
    cip68_chase_read_failures: u64,
    cip68_decode_declined: u64,
    cip68_shape_declined: u64,
    cip68_scan_failures: u64,
    unresolved_records: u64,
    unresolved_ref: u64,
    unresolved_nft: u64,
    unresolved_ft: u64,
    unresolved_rft: u64,
    unresolved_bare: u64,
    malformed_policy: u64,
    payload_encode_failures: u64,
    record_bytes: u64,
    name_bytes: u64,
    image_bytes: u64,
    media_type_bytes: u64,
    media_link_bytes: u64,
    payload_bytes: u64,
    record_lens: Vec<u64>,
}

impl Stats {
    fn mean_record_bytes(&self) -> f64 {
        (self.record_bytes as f64) / (self.records.max(1) as f64)
    }

    fn mean_name_bytes(&self) -> f64 {
        (self.name_bytes as f64) / (self.records.max(1) as f64)
    }
}

// ---------------------------------------------------------------------------
// Auxiliary-data navigation
// ---------------------------------------------------------------------------

/// The metadata map inside auxiliary data, whatever era shape wraps it — the
/// same three-arm match `cardano-assets`'s `cip25::aux_metadata` makes
/// (private there; restated so one measurement does not fork the crate's
/// public API).
fn aux_metadata(aux: &AuxiliaryData) -> Option<&pallas_primitives::Metadata> {
    match aux {
        AuxiliaryData::Shelley(m) => Some(m),
        AuxiliaryData::ShelleyMa(d) => Some(&d.transaction_metadata),
        AuxiliaryData::PostAlonzo(d) => d.metadata.as_ref(),
    }
}

/// Find a value in a `Metadatum::Map` by a key matching either its text form
/// or its raw-bytes form — `cip25::find_in_map`'s rule, restated.
fn find_in_map<'a>(m: &'a Metadatum, as_str: &str, as_bytes: &[u8]) -> Option<&'a Metadatum> {
    let Metadatum::Map(pairs) = m else {
        return None;
    };
    for (k, v) in pairs.iter() {
        let hit = match k {
            Metadatum::Text(s) => s == as_str,
            Metadatum::Bytes(b) => b.as_slice() == as_bytes,
            _ => false,
        };
        if hit {
            return Some(v);
        }
    }
    None
}

/// One policy's `{ asset name => document }` map for a transaction, indexed
/// by the document keys' bytes.
///
/// The index exists so a batch mint is not quadratic: `find_in_map` is a
/// linear scan, and a 100-asset transaction against a 100-asset map would
/// cost 10,000 comparisons per document instead of 100. Keys are
/// canonicalised to their byte form, which is what both of `find_in_map`'s
/// arms compare against the minted name — first duplicate wins, as in the
/// crate's linear scan.
fn policy_asset_map<'a>(
    label_721: Option<&'a Metadatum>,
    policy: &[u8],
) -> Option<HashMap<Vec<u8>, &'a Metadatum>> {
    let policy_map = find_in_map(label_721?, &hex::encode(policy), policy)?;
    let Metadatum::Map(pairs) = policy_map else {
        return Some(HashMap::new());
    };
    let mut out: HashMap<Vec<u8>, &'a Metadatum> = HashMap::new();
    for (k, v) in pairs.iter() {
        let key = match k {
            Metadatum::Text(s) => s.as_bytes().to_vec(),
            Metadatum::Bytes(b) => b.to_vec(),
            _ => continue,
        };
        out.entry(key).or_insert(v);
    }
    Some(out)
}

// ---------------------------------------------------------------------------
// CIP-68: the reference-token datum
// ---------------------------------------------------------------------------

/// The four CIP-67 label prefixes, as bytes —
/// `policy_index::profile::CIP67_LABELS`, which the label census uses as hex.
const CIP67_LABELS: [[u8; 4]; 4] = [
    [0x00, 0x06, 0x43, 0xb0], // 100 — reference token
    [0x00, 0x0d, 0xe1, 0x40], // 222 — NFT
    [0x00, 0x14, 0xdf, 0x10], // 333 — FT
    [0x00, 0x1b, 0xc2, 0x80], // 444 — RFT
];

/// The label a CIP-68 reference token carries — where the metadata datum is.
const REFERENCE_LABEL: [u8; 4] = CIP67_LABELS[0];

fn cip67_labelled(name: &[u8]) -> bool {
    name.get(..4)
        .is_some_and(|head| CIP67_LABELS.iter().any(|label| head == label))
}

/// One output's multiasset names and datum, as far as this index cares.
#[derive(Default)]
struct ScannedOutput {
    /// `(policy, name)` pairs from the output's value.
    names: Vec<(Vec<u8>, Vec<u8>)>,
    /// The output's datum, when it carries one.
    datum: Option<ScannedDatum>,
}

/// What an output's datum option was.
enum ScannedDatum {
    /// The inline datum — Plutus-data CBOR, already unwrapped from the
    /// ledger's `#6.24` envelope.
    Inline(Vec<u8>),
    /// A hash-attached datum: the preimage needs the capture.
    Hash([u8; 32]),
    /// A datum option this walk did not recognise.
    Unknown,
}

/// Is there another entry in a possibly-indefinite container?
///
/// `policy_index::profile::more` is private; this is the same twelve lines.
/// ⚠️ Treating the indefinite case as "empty" once reported 342 of 3,000
/// sound records as faulty in `verify.rs` — the trap is documented there.
fn more(d: &mut minicbor::Decoder<'_>, len: Option<u64>, seen: u64) -> Option<bool> {
    match len {
        Some(n) => Some(seen < n),
        None => match d.datatype().ok()? {
            Type::Break => {
                d.skip().ok()?;
                Some(false)
            }
            _ => Some(true),
        },
    }
}

/// Walk a transaction body's outputs, generically.
///
/// ⚠️ Generic on purpose. The sampler holds the body's raw span and the era
/// is recorded nowhere it can read; a hand walk costs no era guess and cannot
/// read one era's shape as another's. It runs at most once per transaction,
/// and only for transactions whose mint field carried a CIP-67 label — the
/// rare tail, not the corpus.
fn scan_outputs(body: &[u8]) -> Option<Vec<ScannedOutput>> {
    let mut d = minicbor::Decoder::new(body);
    let outer = d.map().ok()?;
    let mut seen = 0u64;
    while more(&mut d, outer, seen)? {
        seen += 1;
        let key = d.u32().ok()?;
        if key != 1 {
            d.skip().ok()?;
            continue;
        }
        let outs = d.array().ok()?;
        let mut n = 0u64;
        let mut out = Vec::new();
        while more(&mut d, outs, n)? {
            n += 1;
            out.push(scan_output(&mut d, body)?);
        }
        return Some(out);
    }
    Some(Vec::new())
}

/// One output item: post-Babbage's array `[address, amount, datum?, script?]`
/// or map `{0:…, 1: amount, 2: datum, 3: …}`. Pre-Babbage arrays carry a bare
/// datum hash in the third position.
fn scan_output(d: &mut minicbor::Decoder<'_>, body: &[u8]) -> Option<ScannedOutput> {
    let mut scan = ScannedOutput::default();
    match d.datatype().ok()? {
        Type::Array => {
            let len = d.array().ok()?;
            let mut n = 0u64;
            while more(d, len, n)? {
                n += 1;
                match n {
                    1 => d.skip().ok()?,
                    2 => scan_amount(d, &mut scan)?,
                    3 => scan_datum(d, body, &mut scan)?,
                    _ => d.skip().ok()?,
                }
            }
        }
        Type::Map => {
            let len = d.map().ok()?;
            let mut n = 0u64;
            while more(d, len, n)? {
                n += 1;
                match d.u32().ok()? {
                    1 => scan_amount(d, &mut scan)?,
                    2 => scan_datum(d, body, &mut scan)?,
                    _ => d.skip().ok()?,
                }
            }
        }
        _ => d.skip().ok()?,
    }
    Some(scan)
}

/// An output's value: a bare coin, or `[coin, { policy => { name => qty } }]`.
fn scan_amount(d: &mut minicbor::Decoder<'_>, scan: &mut ScannedOutput) -> Option<()> {
    match d.datatype().ok()? {
        Type::U8 | Type::U16 | Type::U32 | Type::U64 => d.skip().ok()?,
        Type::Array => {
            let len = d.array().ok()?;
            let mut n = 0u64;
            while more(d, len, n)? {
                n += 1;
                if n == 2 {
                    scan_multiasset(d, scan)?;
                } else {
                    d.skip().ok()?;
                }
            }
        }
        _ => d.skip().ok()?,
    }
    Some(())
}

/// `{ policy => { name => qty } }` — every name recorded, every quantity skipped.
fn scan_multiasset(d: &mut minicbor::Decoder<'_>, scan: &mut ScannedOutput) -> Option<()> {
    let len = d.map().ok()?;
    let mut n = 0u64;
    while more(d, len, n)? {
        n += 1;
        let policy = d.bytes().ok()?.to_vec();
        let assets = d.map().ok()?;
        let mut a = 0u64;
        while more(d, assets, a)? {
            a += 1;
            let name = d.bytes().ok()?.to_vec();
            d.skip().ok()?; // the quantity — never read here
            scan.names.push((policy.clone(), name));
        }
    }
    Some(())
}

/// An output's datum: `[0, hash]` or `[1, inline]` post-Babbage, a bare hash
/// in the pre-Babbage array position, absent otherwise.
fn scan_datum(d: &mut minicbor::Decoder<'_>, body: &[u8], scan: &mut ScannedOutput) -> Option<()> {
    match d.datatype().ok()? {
        Type::Array => {
            let len = d.array().ok()?;
            if d.u64().ok()? == 1 {
                // The inline datum. ⚠️ On chain it is the ledger's
                // `#6.24(bytes .cbor plutus_data)` wrapper — MEASURED: every
                // inline reference datum in the 8900s window is `d81858…` —
                // and `decode_cip68_datum` expects the unwrapped PlutusData,
                // which is also the shape the witness-set venue carries. So
                // the payload is the inner bytes; an unknown tag, or a
                // producer that wrote the datum bare, is captured verbatim.
                let vstart = d.position();
                if d.datatype().ok()? == Type::Tag && d.tag().ok()?.as_u64() == 24 {
                    let inner = d.bytes().ok()?;
                    scan.datum = Some(ScannedDatum::Inline(inner.to_vec()));
                } else {
                    d.skip().ok()?;
                    scan.datum = Some(ScannedDatum::Inline(body[vstart..d.position()].to_vec()));
                }
            } else {
                let h = d.bytes().ok()?;
                scan.datum = Some(match <[u8; 32]>::try_from(h) {
                    Ok(hash) => ScannedDatum::Hash(hash),
                    Err(_) => ScannedDatum::Unknown,
                });
            }
            // Drain any trailing element (definite encodings carry none).
            let mut extra = 2u64;
            while more(d, len, extra)? {
                extra += 1;
                d.skip().ok()?;
            }
        }
        Type::Bytes => {
            let b = d.bytes().ok()?;
            scan.datum = Some(match <[u8; 32]>::try_from(b) {
                Ok(hash) => ScannedDatum::Hash(hash),
                Err(_) => ScannedDatum::Unknown,
            });
        }
        _ => {
            d.skip().ok()?;
            scan.datum = Some(ScannedDatum::Unknown);
        }
    }
    Some(())
}

/// The published datum capture — every window pair, opened up front.
///
/// The windows are the fold's chunk ranges (`DatumCatalogue::covering_index`
/// is the one definition of "which window holds this chunk"); a hash that is
/// not in the covering window is swept for across the rest, because a
/// hash-attached datum's preimage is only guaranteed to have been *witnessed*
/// somewhere in the capture — a spend may have revealed it later than the
/// mint that committed to it.
struct DatumSet {
    catalogue: DatumCatalogue,
    tables: Vec<DatumTable>,
}

/// Where a hash-attached preimage was found.
#[derive(Clone, Copy)]
enum DatumFound {
    /// The window covering the chunk the datum option was seen in.
    Window,
    /// Another window — the preimage was revealed later (or earlier) than
    /// the committing transaction.
    Sweep,
}

impl DatumSet {
    /// Open the set rooted at the publish directory (the one holding
    /// `datum/v1/`).
    fn open(root: &Path) -> Result<DatumSet> {
        let catalogue = DatumCatalogue::load(&root.join(datum_catalogue_key()))?;
        let mut tables = Vec::with_capacity(catalogue.windows().len());
        for entry in catalogue.windows() {
            tables.push(DatumTable::open_pair(
                &root.join(&entry.table.key),
                &root.join(&entry.blob.key),
            )?);
        }
        Ok(DatumSet { catalogue, tables })
    }

    fn get(&self, chunk: u16, hash: &[u8; 32]) -> Option<(&[u8], DatumFound)> {
        if let Some(at) = self.catalogue.covering_index(chunk)
            && let Some(bytes) = self.tables[at].get(hash)
        {
            return Some((bytes, DatumFound::Window));
        }
        for table in self.tables.iter().rev() {
            if let Some(bytes) = table.get(hash) {
                return Some((bytes, DatumFound::Sweep));
            }
        }
        None
    }

    /// `(windows, distinct datums, first slot, last slot)` for the report.
    fn summary(&self) -> (usize, u64, u64, u64) {
        let windows = &self.catalogue.windows();
        (
            self.tables.len(),
            windows.iter().map(|w| w.entries).sum(),
            windows.iter().map(|w| w.first_slot).min().unwrap_or(0),
            windows.iter().map(|w| w.last_slot).max().unwrap_or(0),
        )
    }
}

/// What one transaction's outputs said about an asset's output.
enum RefDatum {
    /// The datum's bytes, and where they came from.
    Resolved(Vec<u8>, Venue),
    /// An output was found; its datum did not resolve.
    Missing(MissWhy),
    /// No output held the name — the token lives elsewhere.
    Absent,
}

/// How a datum was read.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Venue {
    Inline,
    HashInWindow,
    HashBySweep,
}

/// Why a datum did not resolve.
#[derive(Clone, Copy)]
enum MissWhy {
    HashMissed,
    NoDatum,
    Unknown,
}

/// Resolve the datum an output commits to for `wanted`, from one body's
/// outputs.
///
/// Counts no outcome — the caller owns the outcome counter, so an entry that
/// tries several routes is still counted once. The one counter here is the
/// scan metric: how often a body would not scan at all.
fn datum_in(
    body: &[u8],
    outputs: &mut Option<Option<Vec<ScannedOutput>>>,
    policy: &[u8],
    wanted: &[u8],
    chunk: u16,
    datums: Option<&DatumSet>,
    stats: &mut Stats,
) -> RefDatum {
    let scanned = outputs.get_or_insert_with(|| {
        let scanned = scan_outputs(body);
        if scanned.is_none() {
            stats.cip68_scan_failures += 1;
        }
        scanned
    });
    let Some(outputs) = scanned.as_ref() else {
        return RefDatum::Absent;
    };
    for out in outputs {
        if !out.names.iter().any(|(p, n)| p == policy && n == wanted) {
            continue;
        }
        return match &out.datum {
            Some(ScannedDatum::Inline(datum)) => RefDatum::Resolved(datum.clone(), Venue::Inline),
            Some(ScannedDatum::Hash(hash)) => match datums.and_then(|d| d.get(chunk, hash)) {
                Some((bytes, DatumFound::Window)) => {
                    RefDatum::Resolved(bytes.to_vec(), Venue::HashInWindow)
                }
                Some((bytes, DatumFound::Sweep)) => {
                    RefDatum::Resolved(bytes.to_vec(), Venue::HashBySweep)
                }
                None => RefDatum::Missing(MissWhy::HashMissed),
            },
            Some(ScannedDatum::Unknown) => RefDatum::Missing(MissWhy::Unknown),
            None => RefDatum::Missing(MissWhy::NoDatum),
        };
    }
    RefDatum::Absent
}

/// Count one resolved datum's venue.
fn count_venue(stats: &mut Stats, venue: Venue) {
    match venue {
        Venue::Inline => stats.cip68_inline += 1,
        Venue::HashInWindow => stats.cip68_hash_in_window += 1,
        Venue::HashBySweep => stats.cip68_hash_by_sweep += 1,
    }
}

/// Count one entry's miss reason.
fn count_miss(stats: &mut Stats, why: MissWhy) {
    match why {
        MissWhy::HashMissed => stats.cip68_hash_missed += 1,
        MissWhy::NoDatum => stats.cip68_no_datum += 1,
        MissWhy::Unknown => stats.cip68_unknown_datum += 1,
    }
}

/// The datum a labelled mint's asset promises, resolved by whichever route
/// the chain used:
///
/// 1. the reference token (`000643b0` + the suffix) among the minting
///    transaction's own outputs;
/// 2. the reference token's own origin — a user token minted apart from its
///    reference token still finds the datum where the pair was created;
/// 3. the entry's **own** output, for the dialect that mints no reference
///    token and puts the datum on the token itself.
///
/// Exactly one outcome is counted per entry, whatever number of routes it
/// took.
#[allow(clippy::too_many_arguments)]
fn cip68_datum(
    reader: &mut ChunkReader,
    base: &Base,
    datums: Option<&DatumSet>,
    body: &[u8],
    outputs: &mut Option<Option<Vec<ScannedOutput>>>,
    policy: &[u8],
    name: &[u8],
    here: (u16, u32),
    stats: &mut Stats,
) -> Option<Vec<u8>> {
    let mut ref_name = Vec::with_capacity(name.len());
    ref_name.extend_from_slice(&REFERENCE_LABEL);
    ref_name.extend_from_slice(&name[4..]);

    // 1. The reference token, here.
    match datum_in(body, outputs, policy, &ref_name, here.0, datums, stats) {
        RefDatum::Resolved(datum, venue) => {
            count_venue(stats, venue);
            return Some(datum);
        }
        RefDatum::Missing(why) => {
            // A reference output exists here; its datum is the asset's answer.
            count_miss(stats, why);
            return None;
        }
        RefDatum::Absent => {}
    }

    // 2. The reference token's origin — it is itself an indexed mint, so its
    // own record leads back to the transaction that created the datum.
    stats.cip68_chases += 1;
    if let Some(origin) = base.origin(
        policy_index::policy_prefix(policy),
        policy_index::name_prefix(&ref_name),
    ) && (origin.chunk, origin.offset) != here
    {
        match reader.span(origin.chunk, origin.offset, origin.len) {
            Some(origin_body) => {
                let mut origin_outputs = Some(scan_outputs(&origin_body));
                match datum_in(
                    &origin_body,
                    &mut origin_outputs,
                    policy,
                    &ref_name,
                    origin.chunk,
                    datums,
                    stats,
                ) {
                    RefDatum::Resolved(datum, venue) => {
                        stats.cip68_origin_resolved += 1;
                        count_venue(stats, venue);
                        return Some(datum);
                    }
                    RefDatum::Missing(why) => {
                        count_miss(stats, why);
                        return None;
                    }
                    RefDatum::Absent => {}
                }
            }
            None => stats.cip68_chase_read_failures += 1,
        }
    }

    // 3. The dialect with no reference token: the entry's own output.
    match datum_in(body, outputs, policy, name, here.0, datums, stats) {
        RefDatum::Resolved(datum, venue) => {
            stats.cip68_own_token += 1;
            count_venue(stats, venue);
            Some(datum)
        }
        RefDatum::Missing(why) => {
            count_miss(stats, why);
            None
        }
        RefDatum::Absent => {
            stats.cip68_pair_absent += 1;
            None
        }
    }
}

/// A resolved CIP-68 record's headline fields, via the platform's own decode —
/// the same call `collection-ownership` makes, so the record cannot disagree
/// with a live consumer. `media[]` stays empty: the v1 `Asset` projection
/// drops `files[]`, and the payload carries them for whoever wants them.
fn cip68_fields(datum: &[u8], stats: &mut Stats) -> (Vec<u8>, Vec<u8>) {
    match decode_cip68_datum(datum) {
        Ok(metadata) => {
            let asset = Asset::from(metadata.metadata);
            (
                asset.image.into_bytes(),
                asset.media_type.unwrap_or_default().into_bytes(),
            )
        }
        Err(e) => {
            // Two different failures: framing (the bytes are not the PlutusData
            // we think they are) and shape (they are, but the v1 `AssetMetadata`
            // projection has no variant for this document — image-less FT-shaped
            // maps, commonly). The record keeps its payload either way.
            match e {
                cardano_assets::Cip68Error::Metadata(_) => stats.cip68_shape_declined += 1,
                _ => stats.cip68_decode_declined += 1,
            }
            (Vec::new(), Vec::new())
        }
    }
}

// ---------------------------------------------------------------------------
// The sampling
// ---------------------------------------------------------------------------

/// Up to `cap` transaction spans for a policy — profile's stride, verbatim.
///
/// ⚠️ STRIDE, not the first N. A dated sub-series puts one series in a
/// policy's opening transactions, so head-anchored sampling sees a prefix the
/// collection as a whole does not share — MEASURED on "Cardano Kidz". The
/// first transaction is kept unconditionally: it is the one every
/// implementation is certain to hold.
fn sample_txs(records: &[IndexRecord], cap: usize) -> Vec<&IndexRecord> {
    let mut spans: Vec<&IndexRecord> = records.iter().filter(|r| !r.burned).collect();
    spans.sort_by_key(|r| (r.chunk, r.offset));
    spans.dedup_by_key(|r| (r.chunk, r.offset));
    let stride = spans.len().div_ceil(cap.max(1)).max(1);
    let mut out: Vec<&IndexRecord> = spans
        .first()
        .copied()
        .into_iter()
        .chain(spans.iter().skip(1).step_by(stride).copied())
        .collect();
    out.truncate(cap);
    out
}

/// One transaction's per-policy document maps: policy bytes → (minted name
/// bytes → document entry), built once and reused across the transaction's
/// assets.
type AssetMaps<'a> = Vec<(Vec<u8>, Option<HashMap<Vec<u8>, &'a Metadatum>>)>;

/// Resolve one sampled minting transaction into records.
fn resolve_tx(
    reader: &mut ChunkReader,
    base: &Base,
    datums: Option<&DatumSet>,
    build: &mut Build,
    stats: &mut Stats,
    r: &IndexRecord,
) {
    stats.txs_read += 1;
    let Some(body) = reader.span(r.chunk, r.offset, r.len) else {
        stats.body_missing += 1;
        return;
    };
    stats.body_bytes += body.len() as u64;
    let Some(mints) = policy_index::profile::mint_with_qty(&body) else {
        stats.body_decode_failures += 1;
        return;
    };
    let mine: Vec<&(Vec<u8>, Vec<u8>, i64)> = mints
        .iter()
        .filter(|(p, _, _)| policy_index::policy_prefix(p) == r.policy_prefix)
        .collect();
    stats.entries_other_policies += (mints.len() - mine.len()) as u64;
    if mine.is_empty() {
        // The index's name prefix is hashed; the candidate is confirmed in
        // the decoded mint field. Empty means the prefix collided with a
        // different policy — a candidate, never an answer.
        stats.candidate_not_ours += 1;
        return;
    }

    // The aux span once per transaction, because every asset of the
    // transaction shares it.
    let aux_bytes = reader.span(r.chunk, r.aux_offset, r.aux_len);
    let aux_data: Option<AuxiliaryData> = match &aux_bytes {
        Some(a) => {
            stats.aux_bytes += a.len() as u64;
            match minicbor::decode::<AuxiliaryData>(a) {
                Ok(d) => Some(d),
                Err(_) => {
                    stats.txs_aux_decode_failed += 1;
                    None
                }
            }
        }
        None => {
            stats.txs_no_aux += 1;
            None
        }
    };
    let label_721: Option<&Metadatum> = aux_data
        .as_ref()
        .and_then(aux_metadata)
        .and_then(|m| m.get(&721u64));
    if label_721.is_none() && aux_data.is_some() {
        stats.txs_no_721 += 1;
    }

    // Per-policy asset maps, built once per (transaction, policy) so a batch
    // mint costs one map build, not one linear scan per asset.
    let mut maps: AssetMaps<'_> = Vec::new();
    // The body's outputs, scanned at most once and only when a labelled mint
    // needs them (outer None = not scanned yet, inner None = scan failed).
    let mut outputs: Option<Option<Vec<ScannedOutput>>> = None;

    for (policy, name, qty) in mine {
        if *qty <= 0 {
            stats.burn_entries_skipped += 1;
            continue;
        }
        let Ok(policy_id) = PolicyId::try_from(policy.as_slice()) else {
            stats.malformed_policy += 1;
            continue;
        };

        let assets = match maps.iter().find(|(p, _)| p == policy) {
            Some((_, m)) => m,
            None => {
                let m = policy_asset_map(label_721, policy);
                if m.is_none() && label_721.is_some() {
                    stats.policy_absent_from_721 += 1;
                }
                maps.push((policy.clone(), m));
                &maps.last().expect("just pushed").1
            }
        };
        let attrs = assets
            .as_ref()
            .and_then(|m| m.get(name.as_slice()).copied());

        let (family, image, media_type, media, payload) = match attrs {
            Some(document) => {
                let payload = match minicbor::to_vec(document) {
                    Ok(bytes) => bytes,
                    Err(_) => {
                        stats.payload_encode_failures += 1;
                        Vec::new()
                    }
                };
                match decode_document(policy, name, document) {
                    Some(decoded) => {
                        stats.cip25_records += 1;
                        let image = decoded.asset.image().unwrap_or("").as_bytes().to_vec();
                        let media_type = decoded
                            .asset
                            .media_type()
                            .as_str()
                            .unwrap_or("")
                            .as_bytes()
                            .to_vec();
                        let media: Vec<ami::MediaLink> = decoded
                            .asset
                            .files()
                            .map(|m| ami::MediaLink {
                                src: m.src.as_bytes().to_vec(),
                                media_type: m.media_type.as_str().unwrap_or("").as_bytes().to_vec(),
                                name: m.name.as_deref().unwrap_or("").as_bytes().to_vec(),
                            })
                            .collect();
                        (ami::Family::Cip25, image, media_type, media, payload)
                    }
                    None => {
                        // The chain stated a CIP-25 document; the walk could
                        // not turn it into an asset. Payload kept — a reader
                        // may still want the raw document.
                        stats.cip25_declined += 1;
                        (
                            ami::Family::Cip25,
                            Vec::new(),
                            Vec::new(),
                            Vec::new(),
                            payload,
                        )
                    }
                }
            }
            None => {
                // No label-721 entry. The chain may still have stated CIP-68:
                // a CIP-67-labelled name whose reference token's datum this
                // transaction carries.
                let datum = if cip67_labelled(name) {
                    cip68_datum(
                        reader,
                        base,
                        datums,
                        &body,
                        &mut outputs,
                        policy,
                        name,
                        (r.chunk, r.offset),
                        stats,
                    )
                } else {
                    None
                };
                match datum {
                    Some(datum) => {
                        let (image, media_type) = cip68_fields(&datum, stats);
                        stats.cip68_records += 1;
                        (ami::Family::Cip68, image, media_type, Vec::new(), datum)
                    }
                    None => {
                        stats.unresolved_records += 1;
                        match name.get(..4).map(hex::encode).as_deref() {
                            Some("000643b0") => stats.unresolved_ref += 1,
                            Some("000de140") => stats.unresolved_nft += 1,
                            Some("0014df10") => stats.unresolved_ft += 1,
                            Some("001bc280") => stats.unresolved_rft += 1,
                            _ => stats.unresolved_bare += 1,
                        }
                        (
                            ami::Family::None,
                            Vec::new(),
                            Vec::new(),
                            Vec::new(),
                            Vec::new(),
                        )
                    }
                }
            }
        };

        let media_link_bytes: u64 = media
            .iter()
            .map(|m| (6 + m.src.len() + m.media_type.len() + m.name.len()) as u64)
            .sum();
        let record = ami::Record {
            family,
            event: ami::Event::Mint,
            policy_id,
            provenance: ami::Provenance {
                chunk: r.chunk,
                body_offset: r.offset,
                body_len: r.len,
                aux_offset: r.aux_offset,
                aux_len: r.aux_len,
            },
            name: name.clone(),
            image,
            media_type,
            media,
            payload,
        };

        stats.records += 1;
        let len = record.encoded_len() as u64;
        stats.record_bytes += len;
        stats.name_bytes += record.name.len() as u64;
        stats.image_bytes += record.image.len() as u64;
        stats.media_type_bytes += record.media_type.len() as u64;
        stats.media_link_bytes += media_link_bytes;
        stats.payload_bytes += record.payload.len() as u64;
        stats.record_lens.push(len);

        build
            .segments
            .entry(r.chunk)
            .or_default()
            .entry(policy_id)
            .or_default()
            .push(record);
        build
            .directory
            .entry(policy_id)
            .or_default()
            .insert(r.chunk);
    }
}

// ---------------------------------------------------------------------------
// Reporting helpers
// ---------------------------------------------------------------------------

fn pct(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = (((sorted.len() - 1) as f64) * p).round() as usize;
    sorted[idx]
}

fn bytes(n: u64) -> String {
    const UNITS: [(&str, u64); 4] = [
        ("TiB", 1 << 40),
        ("GiB", 1 << 30),
        ("MiB", 1 << 20),
        ("KiB", 1 << 10),
    ];
    for (unit, scale) in UNITS {
        if n >= scale {
            return format!("{:.1} {unit}", n as f64 / scale as f64);
        }
    }
    format!("{n} B")
}

// ---------------------------------------------------------------------------

struct Args {
    index_dir: PathBuf,
    immutable: PathBuf,
    out: PathBuf,
    txs_per_policy: usize,
    limit_policies: usize,
    min_first_chunk: u16,
    datum_set: Option<PathBuf>,
}

fn parse_args() -> Result<Args> {
    let mut index_dir = None;
    let mut immutable = None;
    let mut out = None;
    let mut txs_per_policy = TXS_PER_POLICY;
    let mut limit_policies = 0usize;
    let mut min_first_chunk = 0u16;
    let mut datum_set = None;
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--index-dir" => index_dir = it.next().map(PathBuf::from),
            "--immutable" => immutable = it.next().map(PathBuf::from),
            "--out" => out = it.next().map(PathBuf::from),
            "--txs-per-policy" => {
                txs_per_policy = it
                    .next()
                    .context("--txs-per-policy needs a value")?
                    .parse()?
            }
            "--limit-policies" => {
                limit_policies = it
                    .next()
                    .context("--limit-policies needs a value")?
                    .parse()?
            }
            "--min-first-chunk" => {
                min_first_chunk = it
                    .next()
                    .context("--min-first-chunk needs a value")?
                    .parse()?
            }
            "--datum-set" => datum_set = it.next().map(PathBuf::from),
            other => bail!("unknown argument {other}"),
        }
    }
    Ok(Args {
        index_dir: index_dir.context("--index-dir is required")?,
        immutable: immutable.context("--immutable is required")?,
        out: out.context("--out is required")?,
        txs_per_policy,
        limit_policies,
        min_first_chunk,
        datum_set,
    })
}

fn main() -> Result<()> {
    let args = parse_args()?;
    let base = Base::open(&base_path(&args.index_dir))?;
    let (first_chunk, last_chunk) = base.covers();
    eprintln!(
        "base: {} records, {} policies, chunks {first_chunk}..={last_chunk}",
        base.len(),
        base.policies()
    );

    // Chunk order, so `ChunkReader`'s single open handle is reused instead of
    // thrashing once per policy (the sample visits ~9,000 files, not 231k).
    let mut runs: Vec<_> = base.runs().collect();
    runs.sort_by_key(|r| r.first_chunk);

    let mut files = ChunkReader::new(&args.immutable);
    let mut build = Build::default();
    let mut stats = Stats::default();

    let datums = match &args.datum_set {
        Some(root) => {
            let set = DatumSet::open(root)?;
            let (windows, entries, first, last) = set.summary();
            eprintln!("datum set: {windows} windows, {entries} datums, slots {first}..={last}");
            Some(set)
        }
        None => None,
    };

    // Exact slice counts from the full index — every record, not the sample —
    // and the full build's read set: one read per distinct mint/burn span.
    let mut slices: BTreeMap<(u16, u64), SliceEvents> = BTreeMap::new();
    let mut tx_spans: HashSet<(u16, u32)> = HashSet::new();

    let started = Instant::now();
    let mut seen = 0usize;
    for run in &runs {
        // `--min-first-chunk` scopes the sample to an era (the run list is
        // first-chunk ordered). The limit counts policies that pass the
        // filter, so a bounded era sample does not fill up on skipped runs.
        if run.first_chunk < args.min_first_chunk {
            continue;
        }
        if args.limit_policies > 0 && seen >= args.limit_policies {
            break;
        }
        seen += 1;
        stats.policies += 1;
        let records = base.records_of(run.policy_prefix);
        for r in &records {
            tx_spans.insert((r.chunk, r.offset));
            let slice = slices.entry((r.chunk, run.policy_prefix)).or_default();
            if r.burned {
                slice.burns += 1;
            } else {
                slice.mints += 1;
            }
        }
        let spans = sample_txs(&records, args.txs_per_policy);
        if spans.is_empty() {
            stats.policies_empty += 1;
        }
        for r in spans {
            stats.txs_sampled += 1;
            resolve_tx(
                &mut files,
                &base,
                datums.as_ref(),
                &mut build,
                &mut stats,
                r,
            );
        }
        if seen.is_multiple_of(5000) {
            eprintln!(
                "  {seen}/{} policies, {} records, {:.0}s",
                runs.len(),
                stats.records,
                started.elapsed().as_secs_f64()
            );
        }
    }
    let read_secs = started.elapsed().as_secs_f64();

    // ── write the artifact ────────────────────────────────────────────────
    let encode_started = Instant::now();
    let segments_dir = args.out.join("segments");
    let directory_dir = args.out.join("directory");
    fs::create_dir_all(&segments_dir)?;
    fs::create_dir_all(&directory_dir)?;

    let mut segment_sizes: Vec<(u16, usize, u32, u32)> = Vec::new();
    let mut segment_bytes_total: u64 = 0;
    let (mut min_chunk, mut max_chunk) = (u16::MAX, 0u16);
    let mut segments = build.segments;
    while let Some((chunk, policies)) = segments.pop_first() {
        let entries: Vec<(PolicyId, Vec<ami::Record>)> = policies.into_iter().collect();
        let index_count = entries.len() as u32;
        let record_count: u32 = entries.iter().map(|(_, r)| r.len() as u32).sum();
        let mut bytes = ami::SegmentBuilder {
            network: ami::NETWORK_MAINNET,
            chunk,
            entries,
        }
        .encode()?;
        // The builder leaves `records_hash` zero for the caller to fill; the
        // verifier refuses a zero, so a missed hash is a caught bug, not a
        // trusted value.
        let mut header = ami::SegmentHeader::parse(&bytes)?;
        let hash = Hasher::<256>::hash(&bytes[header.records_off as usize..]);
        header.records_hash.copy_from_slice(hash.as_ref());
        bytes[..ami::SEGMENT_HEADER_LEN].copy_from_slice(&header.encode());

        fs::write(segments_dir.join(format!("{chunk:05}.bin")), &bytes)?;
        segment_bytes_total += bytes.len() as u64;
        min_chunk = min_chunk.min(chunk);
        max_chunk = max_chunk.max(chunk);
        segment_sizes.push((chunk, bytes.len(), record_count, index_count));
    }

    let mut shard_sizes: Vec<(u8, usize)> = Vec::new();
    let directory_through = max_chunk;
    for shard in 0u8..=255 {
        let entries: Vec<ami::DirectoryEntry> = build
            .directory
            .iter()
            .filter(|(p, _)| p[0] == shard)
            .map(|(p, chunks)| ami::DirectoryEntry {
                policy_id: *p,
                chunks: chunks.iter().copied().collect(),
            })
            .collect();
        if entries.is_empty() {
            continue;
        }
        let bytes = ami::DirectoryShard {
            network: ami::NETWORK_MAINNET,
            shard,
            directory_through_chunk: directory_through,
            entries,
        }
        .encode()?;
        fs::write(directory_dir.join(format!("{shard:02x}.bin")), &bytes)?;
        shard_sizes.push((shard, bytes.len()));
    }

    let published_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let pointer = ami::Pointer {
        network: ami::NETWORK_MAINNET,
        built_through_chunk: max_chunk,
        directory_through_chunk: directory_through,
        record_count: stats.records,
        published_at,
    };
    fs::write(args.out.join("current"), pointer.encode())?;
    let encode_secs = encode_started.elapsed().as_secs_f64();

    // ── report ────────────────────────────────────────────────────────────
    let mut report = String::new();
    let mut line = |s: String| {
        println!("{s}");
        report.push_str(&s);
        report.push('\n');
    };

    line("resolve_sample — the sampled resolved build (ASSET_METADATA_INDEX.md, gate 2)".into());
    line(String::new());
    line(format!("index       {}", args.index_dir.display()));
    line(format!(
        "  base      {} records, {} policies, chunks {first_chunk}..={last_chunk}",
        base.len(),
        base.policies()
    ));
    line(format!("immutable   {}", args.immutable.display()));
    line(format!("out         {}", args.out.display()));
    if let Some(root) = &args.datum_set {
        let (windows, entries, first, last) = datums
            .as_ref()
            .map(DatumSet::summary)
            .unwrap_or((0, 0, 0, 0));
        line(format!(
            "datum set   {} ({windows} windows, {entries} datums, slots {first}..={last})",
            root.display()
        ));
    }
    line(format!(
        "sample      {} of {} policies (strided at ≤{} txs), {} with no mint event",
        stats.policies,
        runs.len(),
        args.txs_per_policy,
        stats.policies_empty
    ));
    if args.min_first_chunk > 0 {
        line(format!(
            "  filter    policies first seen at or after chunk {}",
            args.min_first_chunk
        ));
    }
    line(String::new());

    line(format!("read        {read_secs:.1}s"));
    line(format!(
        "  txs       {} sampled, {} resolved, {} records",
        stats.txs_sampled, stats.txs_read, stats.records
    ));
    let tx_skips = stats.body_missing
        + stats.body_decode_failures
        + stats.candidate_not_ours
        + stats.txs_aux_decode_failed;
    line(format!(
        "  skipped   body missing {}, body decode {}, candidate-not-ours {}, aux decode {}",
        stats.body_missing,
        stats.body_decode_failures,
        stats.candidate_not_ours,
        stats.txs_aux_decode_failed
    ));
    line(format!(
        "  pread     {} bodies + {} aux ({})",
        bytes(stats.body_bytes),
        bytes(stats.aux_bytes),
        if tx_skips == 0 { "clean" } else { "with skips" }
    ));
    line(format!(
        "  aux       absent {}, no label 721 {}, policy absent from 721 {}",
        stats.txs_no_aux, stats.txs_no_721, stats.policy_absent_from_721
    ));
    line(format!(
        "  entries   other policies {}, burns skipped {}, malformed policy {}",
        stats.entries_other_policies, stats.burn_entries_skipped, stats.malformed_policy
    ));
    let per_tx_us = read_secs * 1e6 / stats.txs_sampled.max(1) as f64;
    let full_secs = per_tx_us * tx_spans.len() as f64 / 1e6;
    line(format!(
        "  read set  {} distinct mint/burn transactions — what a full build reads",
        tx_spans.len()
    ));
    line(format!(
        "  capacity  {per_tx_us:.0} µs/tx sampled ⇒ full build ≈ {full_secs:.0}s single-threaded (linear), ≈{:.0}s on 8 threads",
        full_secs / 8.0
    ));
    line(String::new());

    line(format!(
        "records     {}  ({} CIP-25, {} CIP-68, {} unresolved)",
        stats.records,
        stats.cip25_records + stats.cip25_declined,
        stats.cip68_records,
        stats.unresolved_records
    ));
    line(format!(
        "  declined  decode_document {} of CIP-25; CIP-68 framing {} + asset shape {} (payloads kept)",
        stats.cip25_declined, stats.cip68_decode_declined, stats.cip68_shape_declined
    ));
    line(format!(
        "  cip68     {} inline | {} hash in-window | {} hash by sweep | {} via origin chase ({} chases) | {} own-token dialect",
        stats.cip68_inline,
        stats.cip68_hash_in_window,
        stats.cip68_hash_by_sweep,
        stats.cip68_origin_resolved,
        stats.cip68_chases,
        stats.cip68_own_token
    ));
    line(format!(
        "  cip68-miss hash {} | no datum {} | unknown {} | no ref origin {} | scan failed {} | chase read failed {}",
        stats.cip68_hash_missed,
        stats.cip68_no_datum,
        stats.cip68_unknown_datum,
        stats.cip68_pair_absent,
        stats.cip68_scan_failures,
        stats.cip68_chase_read_failures
    ));
    line(format!(
        "  unresolved by label  ref {}, nft {}, ft {}, rft {}, bare {}",
        stats.unresolved_ref,
        stats.unresolved_nft,
        stats.unresolved_ft,
        stats.unresolved_rft,
        stats.unresolved_bare
    ));
    line(format!(
        "  share     {:.2}% of the index's {} events",
        (stats.records as f64) / (base.len().max(1) as f64) * 100.0,
        base.len()
    ));
    line(String::new());

    stats.record_lens.sort_unstable();
    let lens = &stats.record_lens;
    line(format!(
        "bytes/record mean {} | p50 {} | p90 {} | p99 {} | max {}",
        bytes((stats.mean_record_bytes().round()) as u64),
        bytes(pct(lens, 0.50)),
        bytes(pct(lens, 0.90)),
        bytes(pct(lens, 0.99)),
        bytes(lens.last().copied().unwrap_or(0))
    ));
    let total = stats.record_bytes.max(1) as f64;
    let overhead = segment_bytes_total as f64 / total;
    line(format!(
        "  payload   {:.1}% ({})",
        stats.payload_bytes as f64 / total * 100.0,
        bytes(stats.payload_bytes)
    ));
    line(format!(
        "  image     {:.1}% | media type {:.1}% | media links {:.1}% | name {:.1}% | head {:.1}%",
        stats.image_bytes as f64 / total * 100.0,
        stats.media_type_bytes as f64 / total * 100.0,
        stats.media_link_bytes as f64 / total * 100.0,
        stats.name_bytes as f64 / total * 100.0,
        (stats.record_bytes
            - stats.name_bytes
            - stats.image_bytes
            - stats.media_type_bytes
            - stats.media_link_bytes
            - stats.payload_bytes) as f64
            / total
            * 100.0
    ));
    // What the whole corpus would hold: the index's exact event counts,
    // priced at the sampled per-record bytes. Mints use the sampled mean;
    // burns are priced at the record head + the sampled mean name.
    let (all_mints, all_burns): (u64, u64) = slices.values().fold((0u64, 0u64), |(m, b), s| {
        (m + s.mints as u64, b + s.burns as u64)
    });
    let burn_record = ami::RECORD_HEAD_LEN as f64 + stats.mean_name_bytes();
    let full_records =
        all_mints as f64 * stats.mean_record_bytes() + all_burns as f64 * burn_record;
    line(format!(
        "  full est  {} events ({} mints, {} burns) ⇒ {} of records, ≈{} of segments at the sample's {overhead:.2}× index overhead",
        all_mints + all_burns,
        all_mints,
        all_burns,
        bytes(full_records.round() as u64),
        bytes((full_records * overhead).round() as u64)
    ));
    line(String::new());

    // The slice distribution: exact event counts from the index, priced at
    // the measured per-record bytes (same pricing as the full estimate).
    let mean_mint = stats.mean_record_bytes();
    let mut slice_events: Vec<u64> = slices
        .values()
        .map(|s| (s.mints + s.burns) as u64)
        .collect();
    let mut slice_bytes: Vec<u64> = slices
        .values()
        .map(|s| (s.mints as f64 * mean_mint + s.burns as f64 * burn_record).round() as u64)
        .collect();
    slice_events.sort_unstable();
    slice_bytes.sort_unstable();
    let over = |v: &[u64], t: u64| v.iter().filter(|x| **x > t).count();
    line(format!(
        "slices      {} (chunk, policy-prefix) pairs, exact event counts",
        slices.len()
    ));
    line(format!(
        "  events    mean {:.1} | p50 {} | p99 {} | p99.9 {} | max {}",
        slice_events.iter().sum::<u64>() as f64 / slice_events.len().max(1) as f64,
        pct(&slice_events, 0.50),
        pct(&slice_events, 0.99),
        pct(&slice_events, 0.999),
        slice_events.last().copied().unwrap_or(0)
    ));
    line(format!(
        "  bytes     mean {} | p99 {} | p99.9 {} | max {}  (estimated)",
        bytes((slice_bytes.iter().sum::<u64>() as f64 / slice_bytes.len().max(1) as f64) as u64),
        bytes(pct(&slice_bytes, 0.99)),
        bytes(pct(&slice_bytes, 0.999)),
        bytes(slice_bytes.last().copied().unwrap_or(0))
    ));
    line(format!(
        "  over      256 KiB {} | 1 MiB {} | 4 MiB {}",
        over(&slice_bytes, 256 << 10),
        over(&slice_bytes, 1 << 20),
        over(&slice_bytes, 4 << 20)
    ));
    line(String::new());

    segment_sizes.sort_by_key(|(_, len, _, _)| std::cmp::Reverse(*len));
    let segment_files = segment_sizes.len();
    line(format!(
        "segments    {} files (chunks {min_chunk}..={max_chunk}), {} total, mean {}, max {}",
        segment_files,
        bytes(segment_bytes_total),
        bytes(segment_bytes_total / segment_files.max(1) as u64),
        bytes(segment_sizes.first().map_or(0, |(_, l, _, _)| *l as u64))
    ));
    for (chunk, len, records, policies) in segment_sizes.iter().take(5) {
        line(format!(
            "  chunk {chunk:05}  {}  ({records} records, {policies} policies)",
            bytes(*len as u64)
        ));
    }
    let shard_total: u64 = shard_sizes.iter().map(|(_, l)| *l as u64).sum();
    shard_sizes.sort_by_key(|(_, l)| std::cmp::Reverse(*l));
    line(format!(
        "directory   {} shards, {} total, max {} (shard {:02x})",
        shard_sizes.len(),
        bytes(shard_total),
        bytes(shard_sizes.first().map_or(0, |(_, l)| *l as u64)),
        shard_sizes.first().map_or(0, |(s, _)| *s)
    ));
    line(format!(
        "encode      {encode_secs:.1}s (segments + hash + directory + pointer)"
    ));
    line(String::new());

    // The self-check the format's own arithmetic affords: the records region
    // must equal the sum of the records' encoded lengths.
    let expected_region: u64 = segment_sizes
        .iter()
        .map(|(_, len, _, policies)| {
            (*len as u64)
                - (ami::SEGMENT_HEADER_LEN as u64)
                - (*policies as u64 * ami::INDEX_ENTRY_LEN as u64)
        })
        .sum();
    line(format!(
        "self-check  records regions {} vs Σ encoded_len {}",
        bytes(expected_region),
        bytes(stats.record_bytes)
    ));

    fs::write(args.out.join("summary.txt"), &report)?;
    eprintln!(
        "wrote summary to {}",
        args.out.join("summary.txt").display()
    );
    Ok(())
}
