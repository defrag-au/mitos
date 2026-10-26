//! The policy archive on disk — what a reverse pass writes, and what the
//! policy API reads. No database anywhere in it.
//!
//! # Layout
//!
//! ```text
//! <archive-dir>/<policy_hex>/
//!   manifest.json            — passes, coverage, completeness; written LAST
//!   pass-0000/movements.parquet   — rows for transactions found in the pass
//!   pass-0000/pending.bin         — inputs still wanting a source, after it
//!   pass-0001/movements.parquet
//!   pass-0001/corrections.parquet — deltas RESOLVED for earlier passes' rows
//!   pass-0001/pending.bin
//! ```
//!
//! Every file is immutable once written. A pass adds a directory and replaces
//! the manifest by rename, so a reader never sees a half-written archive; on
//! R2 the same shape is "write the objects, then flip the manifest with a
//! conditional put" — see `docs/design/POLICY_ARCHIVE_AND_SCALE.md`.
//!
//! # The one piece of carried state
//!
//! A reverse walk meets a transaction's outputs first and its sources later,
//! so it needs to remember which inputs it is still waiting on. That used to
//! be a sqlite table; here it is [`PendingSpender`] rows in `pending.bin`,
//! the FULL set as the pass left it, so the next pass loads exactly one file.
//! It is the honest measure of the input-resolution problem: the file's size
//! is how much a satellite has to carry between passes, and if it grows past
//! what a sidecar should hold, that is the signal to solve resolution with
//! real storage rather than to hide it in a database again.
//!
//! # Reading
//!
//! [`PolicyArchive`] opens every file's FOOTER through the range protocol the
//! `policy-archive` crate defines and nothing else, then fetches row groups on
//! demand for a feed page or a hash lookup. Locally the "remote" is a seeked
//! file; the code path is the one a Worker would run against R2, which is why
//! the CLI reports how many bytes it fetched.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use policy_archive::{Archive, Completeness, DensityBucket, Movement, SparseBytes};
use serde::{Deserialize, Serialize};

// The manifest, the file kinds and the feed shapes live in the crate, so a
// Worker reading R2 parses exactly what the box wrote.
#[cfg(test)]
pub use policy_archive::feed::PartyMove;
pub use policy_archive::feed::{FeedRow, UnitMove, fold_rows};
pub use policy_archive::manifest::{
    CORRECTIONS, FileEntry, FileKind, MANIFEST, MANIFEST_FORMAT, MOVEMENTS, Manifest, PENDING,
    PassEntry, RangeKind, SlotRange, kind_of,
};

/// A transaction still waiting on inputs — the carried state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingSpender {
    pub tx_hash: [u8; 32],
    pub slot: u64,
    pub block_time: u64,
    /// Per unit, how much is still unattributed — the missing negatives.
    /// Retired once every entry reaches zero.
    pub missing: Vec<(Vec<u8>, i64)>,
    /// Per unit net mint, so a correction row can carry the same `net_mint`
    /// the transaction's own rows do.
    pub net_mint: Vec<(Vec<u8>, i64)>,
    /// Inputs not yet seen. Any of them might be the source.
    pub outrefs: Vec<([u8; 32], u32)>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PendingFile {
    pub spenders: Vec<PendingSpender>,
    /// Spenders this job LOADED from an earlier sidecar and settled. A
    /// later job merging every sidecar it can find — jobs land out of
    /// order now — drops these rather than carrying a settled spender
    /// forever because an older file still lists it. Only carried ones:
    /// a job's own settled spenders were never in any file.
    #[serde(default)]
    pub settled: Vec<[u8; 32]>,
}

/// The sidecar before `settled` existed. postcard is not self-describing,
/// so an old file has to be read as the old shape.
#[derive(Debug, Default, Deserialize)]
struct PendingFileV1 {
    spenders: Vec<PendingSpender>,
}

pub fn policy_dir(root: &Path, policy_hex: &str) -> PathBuf {
    root.join(policy_hex)
}

pub fn load_manifest(dir: &Path) -> Result<Option<Manifest>> {
    let path = dir.join(MANIFEST);
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
    let m = Manifest::from_json(&raw).with_context(|| format!("parsing {}", path.display()))?;
    if m.format > MANIFEST_FORMAT {
        bail!(
            "{} is manifest format {} — newer than this binary ({MANIFEST_FORMAT})",
            path.display(),
            m.format
        );
    }
    Ok(Some(m))
}

/// Replace the manifest atomically. Written LAST by a pass, so an archive
/// either has a pass or does not; never half of one.
pub fn store_manifest(dir: &Path, m: &Manifest) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let tmp = dir.join(format!("{MANIFEST}.tmp"));
    std::fs::write(&tmp, m.to_json()?)?;
    std::fs::rename(&tmp, dir.join(MANIFEST))?;
    Ok(())
}

/// The manifest plus every file's footer, as one blob beside the manifest —
/// what `publish` puts in KV so a Worker opens the archive in ONE read instead
/// of a manifest and two tails per file.
///
/// ⚠️ **`publish` is the only automatic writer.** Every other manifest write —
/// a landed walk, a rollup, a tail — leaves the bundle stale on purpose: it is
/// derived, nothing local reads it, and rebuilding it there re-read a footer
/// per file for a blob the next publish regenerates. A bundle on disk that a
/// `published.json` has not yet caught up with is normal.
pub fn store_bundle(dir: &Path, m: &Manifest) -> Result<PathBuf> {
    use policy_archive::reader::{FOOTER_HINT, footer_length, footer_request};
    let mut files = Vec::new();
    for (rel, kind) in m.files() {
        // The bundle is a cache of movement FOOTERS, so an observations
        // footer has no place in it — a reader opening the archive from the
        // bundle would find a file it cannot parse.
        if kind == policy_archive::FileKind::Observations {
            continue;
        }
        let path = dir.join(&rel);
        let mut f = File::open(&path).with_context(|| format!("opening {}", path.display()))?;
        let total = f.metadata()?.len();
        let (start, len) = footer_request(total, FOOTER_HINT);
        let mut tail = vec![0u8; len as usize];
        f.seek(SeekFrom::Start(start))?;
        f.read_exact(&mut tail)?;
        let need = footer_length(&tail)?;
        let (footer_start, footer) = if need > len {
            let (start, len) = footer_request(total, need);
            let mut whole = vec![0u8; len as usize];
            f.seek(SeekFrom::Start(start))?;
            f.read_exact(&mut whole)?;
            (start, whole)
        } else {
            // Exactly the footer, not the whole tail request.
            (total - need, tail.split_off((len - need) as usize))
        };
        files.push(policy_archive::BundledFooter {
            file: rel,
            total_len: total,
            footer_start,
            footer,
        });
    }
    let bundle = policy_archive::Bundle {
        format: policy_archive::BUNDLE_FORMAT,
        manifest: m.to_json()?,
        files,
    };
    let path = dir.join(policy_archive::BUNDLE);
    let tmp = dir.join(format!("{}.tmp", policy_archive::BUNDLE));
    std::fs::write(&tmp, bundle.encode()?)?;
    std::fs::rename(&tmp, &path)?;
    Ok(path)
}

/// `token-ledger bundle` — (re)write a policy's bundle from its manifest.
/// `publish` writes one; this is for backfilling an archive and for looking.
#[derive(clap::Args, Debug)]
pub struct BundleArgs {
    /// Archive root (`<root>/<policy_hex>/manifest.json`).
    #[arg(long, default_value = "archive")]
    pub archive_dir: PathBuf,
    /// 56-hex policy id.
    #[arg(long)]
    pub policy: String,
}

pub fn bundle(args: BundleArgs) -> Result<()> {
    let dir = policy_dir(&args.archive_dir, &args.policy.to_lowercase());
    let Some(m) = load_manifest(&dir)? else {
        bail!("no archive at {}", dir.display());
    };
    let path = store_bundle(&dir, &m)?;
    let len = std::fs::metadata(&path)?.len();
    println!(
        "wrote {} — {} files, {len} bytes",
        path.display(),
        m.files().len()
    );
    Ok(())
}

pub fn load_pending(path: &Path) -> Result<PendingFile> {
    if !path.exists() {
        return Ok(PendingFile::default());
    }
    let raw = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    match postcard::from_bytes::<PendingFile>(&raw) {
        Ok(f) => Ok(f),
        Err(_) => {
            let v1: PendingFileV1 = postcard::from_bytes(&raw)
                .with_context(|| format!("decoding {}", path.display()))?;
            Ok(PendingFile {
                spenders: v1.spenders,
                settled: Vec::new(),
            })
        }
    }
}

/// Every sidecar's state, merged: the copy from the LATEST-landed file
/// wins per spender, and a spender any file says was settled is dropped.
/// Exact when jobs land in sequence; when they race, a spender the latest
/// job never loaded is picked up from the older file that has it.
pub fn load_pending_union(dir: &Path, manifest: &Manifest) -> Result<PendingFile> {
    let mut files = manifest.pending_files();
    files.sort_by_key(|f| std::cmp::Reverse(f.0));
    let mut by_hash: HashMap<[u8; 32], PendingSpender> = HashMap::new();
    let mut settled: HashSet<[u8; 32]> = HashSet::new();
    for (_, rel) in files {
        let path = dir.join(&rel);
        if !path.exists() {
            continue;
        }
        let f = load_pending(&path)?;
        settled.extend(f.settled.iter().copied());
        for s in f.spenders {
            by_hash.entry(s.tx_hash).or_insert(s);
        }
    }
    Ok(PendingFile {
        spenders: by_hash
            .into_values()
            .filter(|s| !settled.contains(&s.tx_hash))
            .collect(),
        settled: Vec::new(),
    })
}

pub fn store_pending(path: &Path, p: &PendingFile) -> Result<u64> {
    let bytes = postcard::to_stdvec(p)?;
    std::fs::write(path, &bytes)?;
    Ok(bytes.len() as u64)
}

/// A file read by ranges — the local stand-in for an R2 object. Counts what
/// it fetched so the CLI can say how much of the file a read touched.
pub struct RangeFile {
    file: File,
    len: u64,
    pub requests: usize,
    pub fetched: u64,
}

impl RangeFile {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
        let len = file.metadata()?.len();
        Ok(Self {
            file,
            len,
            requests: 0,
            fetched: 0,
        })
    }

    pub fn size(&self) -> u64 {
        self.len
    }

    pub fn fetch(&mut self, into: &mut SparseBytes, start: u64, len: u64) -> Result<()> {
        if into.has(start, len) {
            return Ok(());
        }
        let mut buf = vec![0u8; len as usize];
        self.file.seek(SeekFrom::Start(start))?;
        self.file.read_exact(&mut buf)?;
        self.requests += 1;
        self.fetched += len;
        into.insert(start, buf.into());
        Ok(())
    }
}

/// The footer protocol against a local file: the tail, then the exact footer
/// if the tail fell short. What every reader here starts from.
pub fn open_footer(path: &Path) -> Result<(RangeFile, SparseBytes, Archive)> {
    let mut source = RangeFile::open(path)?;
    let mut bytes = SparseBytes::new(source.size());
    let (start, len) =
        policy_archive::reader::footer_request(source.size(), policy_archive::reader::FOOTER_HINT);
    source.fetch(&mut bytes, start, len)?;
    let tail_start = source.size().saturating_sub(8);
    // `SparseBytes` has no public byte accessor by design; re-read the eight
    // bytes through the file rather than widen the crate's API.
    let mut last8 = vec![0u8; (source.size() - tail_start) as usize];
    source.file.seek(SeekFrom::Start(tail_start))?;
    source.file.read_exact(&mut last8)?;
    let need = policy_archive::reader::footer_length(&last8)?;
    if need > len {
        let (start, len) = policy_archive::reader::footer_request(source.size(), need);
        source.fetch(&mut bytes, start, len)?;
    }
    let archive =
        Archive::open(&bytes).with_context(|| format!("opening footer of {}", path.display()))?;
    Ok((source, bytes, archive))
}

struct OpenFile {
    kind: FileKind,
    /// Which chain this file's pass read. Carried because a VOLATILE file is
    /// replaced whole on every refresh and may legitimately cover a stretch an
    /// immutable pass has also written rows for — so anything that TOTALS
    /// across files has to exclude it. See [`PolicyArchive::reconcile`].
    range: RangeKind,
    source: RangeFile,
    bytes: SparseBytes,
    archive: Archive,
    /// Bloom filters fetched for every group.
    blooms_loaded: bool,
}

impl OpenFile {
    fn open(path: &Path, kind: FileKind, range: RangeKind) -> Result<Self> {
        let (source, bytes, archive) = open_footer(path)?;
        Ok(Self {
            kind,
            range,
            source,
            bytes,
            archive,
            blooms_loaded: false,
        })
    }

    fn fetch_group(&mut self, g: usize) -> Result<()> {
        let (s, l) = self.archive.group_range(g);
        self.source.fetch(&mut self.bytes, s, l)
    }

    fn fetch_blooms(&mut self) -> Result<()> {
        if self.blooms_loaded {
            return Ok(());
        }
        for g in 0..self.archive.num_groups() {
            if let Some((s, l)) = self.archive.bloom_range(g) {
                self.source.fetch(&mut self.bytes, s, l)?;
            }
        }
        self.blooms_loaded = true;
        Ok(())
    }
}

/// What the archive covers — the same statement the sqlite ledger used to
/// make, derived from the manifest and the footers.
pub struct Coverage {
    /// The SETTLED stretches, merged, ascending — the coverage a reader
    /// can rely on. `walked_from`/`walked_to` below are the extremes of
    /// these AND the tail together.
    pub immutable_ranges: Vec<SlotRange>,
    /// The live tail above the immutable tip, if the box is following it.
    /// At most one, and replaced whole on every refresh.
    pub volatile: Vec<policy_archive::manifest::Span>,
    pub walked_from: Option<u64>,
    pub walked_to: Option<u64>,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
    pub total_txs: u64,
    /// A LOWER BOUND: the most distinct units any one pass saw.
    pub units: u64,
    pub unresolved: u64,
    pub completeness: Completeness,
}

/// Where a supply reconciliation's rows came from — the axis that decides
/// whether a total is final.
///
/// ⚠️ NOT the launch lifecycle. `/launch` already reports `bonding` vs
/// `graduated`, and the two axes are independent: a token can be bonded and
/// still carry `Tail` supply on the day it graduates, and a token still on its
/// curve can be fully `Snapshot` once a Mithril snapshot covers it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SupplyBasis {
    /// Certified Mithril history. The default and the normal case.
    #[default]
    Snapshot,
    /// Includes the chain-tail spool above the last complete chunk, and at
    /// least one MINT or BURN came from it. A rollback can retract those rows
    /// until the next snapshot certifies them, so the supply figure is not
    /// final.
    ///
    /// Transfers from the tail are always counted into `moved`; they do not
    /// change `minted` and therefore do not, on their own, make the supply
    /// provisional.
    Tail,
}

impl SupplyBasis {
    /// The wire default: a standard token carries no `basis` field at all.
    pub fn is_snapshot(&self) -> bool {
        matches!(self, Self::Snapshot)
    }
}

/// A supply reconciliation over the current view.
pub struct Reconciliation {
    pub reconciler: policy_archive::Reconciler,
    pub basis: SupplyBasis,
}

/// Whether a unit is part of what a supply figure counts, as opposed to
/// plumbing that shares the policy — a CIP-67 reference twin, a CIP-27 royalty
/// token, or a launchpad pool's state NFT.
///
/// The same rule `segments` uses for the unit count, from the same crate: two
/// definitions of "what is a unit" would let the header and the supply total
/// disagree.
pub(crate) fn is_user_unit(name: &[u8]) -> bool {
    cardano_assets::AssetRole::of_bytes(name).standing() == cardano_assets::UnitStanding::Unit
}

/// One policy's archive, opened to its footers.
pub struct PolicyArchive {
    pub manifest: Manifest,
    files: Vec<OpenFile>,
}

impl PolicyArchive {
    /// `None` when no pass has ever run for this policy.
    pub fn open(dir: &Path) -> Result<Option<Self>> {
        Self::open_with(dir, &[])
    }

    /// The archive plus `extra` files that are not in the manifest yet — a
    /// running pass's segments, which the hub knows about before they land.
    /// `None` when there is neither a manifest nor anything extra.
    pub fn open_with(dir: &Path, extra: &[(PathBuf, FileKind)]) -> Result<Option<Self>> {
        let manifest = load_manifest(dir)?;
        if manifest.is_none() && extra.is_empty() {
            return Ok(None);
        }
        let manifest = manifest.unwrap_or_else(|| {
            Manifest::new(
                &dir.file_name()
                    .map(|f| f.to_string_lossy().into_owned())
                    .unwrap_or_default(),
            )
        });
        // Which of the manifest's files belong to IMMUTABLE passes. The
        // rollup already makes this distinction for the same underlying
        // reason; totals need it too.
        let immutable: std::collections::HashSet<String> = manifest
            .immutable_files()
            .into_iter()
            .map(|(f, _)| f)
            .collect();
        let mut files = Vec::new();
        for (rel, kind) in manifest.files() {
            // A movements reader must never be handed an observations file:
            // different schema, and `kind_of`'s fallthrough is `Movements`, so
            // the failure would be a decode error rather than a skip. The
            // observation tier is read through `policy_archive::observation`.
            if kind == policy_archive::FileKind::Observations {
                continue;
            }
            let range = match immutable.contains(&rel) {
                true => RangeKind::Immutable,
                false => RangeKind::Volatile,
            };
            files.push(OpenFile::open(&dir.join(rel), kind, range)?);
        }
        // A running pass's segments can vanish under us when it lands and
        // compacts them; one request seeing the archive without them is
        // better than one request failing. Volatile by nature: they are the
        // live view of a pass that has not landed.
        for (path, kind) in extra {
            if path.exists() {
                files.push(OpenFile::open(path, *kind, RangeKind::Volatile)?);
            }
        }
        Ok(Some(Self { manifest, files }))
    }

    /// Files alone, no manifest — for reading a compaction's output back.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn open_files(files: &[(PathBuf, FileKind)]) -> Result<Self> {
        let mut opened = Vec::new();
        for (path, kind) in files {
            // No manifest to say otherwise, and this reads a compaction's
            // OUTPUT — immutable by construction.
            opened.push(OpenFile::open(path, *kind, RangeKind::Immutable)?);
        }
        Ok(Self {
            manifest: Manifest::new(""),
            files: opened,
        })
    }

    /// Bytes and requests so far, across every file — what a Worker would
    /// have paid R2 for the same reads.
    pub fn fetched(&self) -> (usize, u64) {
        self.files.iter().fold((0, 0), |(r, b), f| {
            (r + f.source.requests, b + f.source.fetched)
        })
    }

    pub fn coverage(&self) -> Coverage {
        // From the FOOTERS, so a running pass's segments count the moment
        // they are opened, before any manifest names them.
        let ranges = self
            .files
            .iter()
            .filter(|f| f.kind == FileKind::Movements)
            .flat_map(|f| (0..f.archive.num_groups()).filter_map(|g| f.archive.slot_range(g)));
        let first_slot = ranges.clone().map(|(lo, _)| lo).min();
        let last_slot = ranges.map(|(_, hi)| hi).max();
        let total_txs = self
            .files
            .iter()
            .filter(|f| f.kind == FileKind::Movements)
            .flat_map(|f| f.archive.groups().iter().map(|g| g.txs))
            .sum();
        Coverage {
            immutable_ranges: self.manifest.immutable_ranges(),
            volatile: self
                .manifest
                .spans()
                .into_iter()
                .filter(|s| s.kind == RangeKind::Volatile)
                .collect(),
            walked_from: self.manifest.walk_from(),
            walked_to: self.manifest.walk_to(),
            first_slot,
            last_slot,
            total_txs,
            // ONE implementation, on the manifest. This was a second copy of
            // the same `max` that had already drifted from it — it never
            // consulted `rollup` at all, so a fully folded archive reported
            // the count of whichever pass happened to be largest, from files
            // that no longer exist. See [`Manifest::units`].
            units: self.manifest.units(),
            unresolved: self.manifest.latest_pass().map_or(0, |p| p.pending),
            completeness: self.manifest.completeness(),
        }
    }

    /// The density tier, from footers alone: transactions, mints and burns
    /// from the movement files; movement counts from those AND the
    /// corrections, since a resolved source is a movement of the same
    /// transaction. Buckets coarser than the files' own are summed up;
    /// finer ones cannot be answered and fall back to the files' resolution.
    pub fn density(&self, bucket_secs: u64) -> Vec<DensityBucket> {
        let mut by: BTreeMap<u64, DensityBucket> = BTreeMap::new();
        for f in &self.files {
            let native = f.archive.bucket_secs();
            let width = if bucket_secs >= native {
                bucket_secs
            } else {
                native
            };
            for b in f.archive.density() {
                let from = b.from_unix / width * width;
                let slot = by.entry(from).or_insert(DensityBucket {
                    from_unix: from,
                    to_unix: from + width,
                    ..DensityBucket::default()
                });
                slot.movements += b.movements;
                if f.kind == FileKind::Movements {
                    slot.txs += b.txs;
                    slot.mints += b.mints;
                    slot.burns += b.burns;
                }
            }
        }
        by.into_values().collect()
    }

    /// Newest-first page of transactions, with every correction any later
    /// pass made to them folded in.
    pub fn feed_rows(&mut self, limit: u32, before_slot: Option<u64>) -> Result<Vec<FeedRow>> {
        let rows = self.movements_page(limit, before_slot)?;
        let mut folded = fold_rows(rows);
        folded.truncate(limit.clamp(1, 5_000) as usize);
        Ok(folded)
    }

    /// The UNFOLDED rows behind a page: the newest `limit` transactions'
    /// movements below `before_slot`, plus every correction to them. A
    /// caller with more rows of its own — the live view of a running pass —
    /// appends them and folds once.
    pub fn movements_page(
        &mut self,
        limit: u32,
        before_slot: Option<u64>,
    ) -> Result<Vec<Movement>> {
        let limit = limit.clamp(1, 5_000) as usize;
        let before = before_slot.unwrap_or(u64::MAX);

        // Every movement group that could hold a row below `before`, newest
        // first by its top slot.
        let mut candidates: Vec<(usize, usize, u64)> = Vec::new();
        for (i, f) in self.files.iter().enumerate() {
            if f.kind != FileKind::Movements {
                continue;
            }
            for g in 0..f.archive.num_groups() {
                if let Some((lo, hi)) = f.archive.slot_range(g)
                    && lo < before
                {
                    candidates.push((i, g, hi));
                }
            }
        }
        candidates.sort_by_key(|c| std::cmp::Reverse(c.2));

        let mut rows: Vec<Movement> = Vec::new();
        let mut txs: HashSet<Vec<u8>> = HashSet::new();
        for (i, g, _) in candidates {
            if txs.len() >= limit {
                break;
            }
            let f = &mut self.files[i];
            f.fetch_group(g)?;
            for m in f.archive.read_group(&f.bytes, g)? {
                if m.slot < before {
                    txs.insert(m.tx_hash.clone());
                    rows.push(m);
                }
            }
        }
        self.attach_corrections(&mut rows, &txs)?;
        Ok(rows)
    }

    /// The supply invariant over the IMMUTABLE archive — see
    /// [`policy_archive::supply`].
    ///
    /// Streams every group through the reconciler rather than collecting rows,
    /// because this is the one report that cannot sample: a page would balance
    /// or not by accident of where it was cut.
    ///
    /// ⚠️ Reads MOVEMENTS **and** CORRECTIONS. A correction file is where a
    /// later pass records the source it finally resolved, so reconciling
    /// movements alone reports every one of those as still below the floor —
    /// a gap that shrinks to zero only if you read the files that closed it.
    ///
    /// ⚠️ IMMUTABLE FILES ONLY, and this is a correctness requirement rather
    /// than a performance one. The volatile tail is a projection of the last
    /// few hours that is REPLACED WHOLE on every refresh, and a top-up walk
    /// that climbs into a stretch the tail also covers must write its own rows
    /// anyway ("or the next refresh shrinks the tail out from under them" — see
    /// `reverse.rs`). So for a window, the same transaction legitimately has
    /// rows in both.
    ///
    /// A total across both then double-counts AMOUNTS while counting `net_mint`
    /// once per `(tx, unit)` — which reads as `moved > minted`, i.e. a positive
    /// gap, i.e. exactly the shape of a real fault. MEASURED 2026-09-09: this
    /// produced RECONCILIATION FAILED on 7 of 23 live policies (`units=229
    /// gap=1` and similar) whose archives were, on direct inspection, perfectly
    /// balanced.
    ///
    /// 🔑 A check that cries wolf is worse than no check: it gets muted, and
    /// then it is not there for the real one.
    pub fn reconcile(
        &mut self,
        diagnose: policy_archive::Diagnose,
    ) -> Result<policy_archive::Reconciler> {
        self.reconcile_files(diagnose, false, true)
            .map(|(reconciler, _)| reconciler)
    }

    /// The supply invariant over the CURRENT view, including the volatile tail
    /// when it cannot double-count.
    ///
    /// [`Self::reconcile`] is the right answer for a landed pass: it grades the
    /// immutable archive on its own. This is the right answer for a READER, and
    /// the distinction is not cosmetic — a token launched after the last
    /// Mithril snapshot has its ENTIRE history in the tail, so the immutable
    /// reconcile reports `minted: 0` for it and the site shows a zero market
    /// cap until the next weekly refresh.
    ///
    /// The tail is counted only when its floor is at or above every immutable
    /// pass's ceiling: then the two stretches are disjoint and no transaction
    /// can be in both. After a snapshot refresh a top-up writes immutable rows
    /// into the stretch the tail still covers, the ranges overlap, and the tail
    /// is skipped exactly as [`Self::reconcile`] always does.
    ///
    /// Only USER units are reconciled. CIP-67 reference twins and royalty
    /// tokens are plumbing: they share the policy, they are not what the supply
    /// is a count of, and a launchpad pool's state NFT under the token's own
    /// policy would otherwise inflate a 1,000,000,000 mint to 1,000,000,002.
    /// `segments` applies the same rule to the unit count, from the same crate.
    pub fn reconcile_current(
        &mut self,
        diagnose: policy_archive::Diagnose,
    ) -> Result<Reconciliation> {
        let basis = match self.volatile_is_separate() {
            true => SupplyBasis::Tail,
            false => SupplyBasis::Snapshot,
        };
        let (reconciler, volatile_mint) =
            self.reconcile_files(diagnose, basis == SupplyBasis::Tail, false)?;
        Ok(Reconciliation {
            reconciler,
            // A tail with transfers only does not move supply. The ordinary
            // case — a live token with recent trades but no new mint — stays
            // `snapshot`, which is the default a reader should see.
            basis: match volatile_mint {
                true => SupplyBasis::Tail,
                false => SupplyBasis::Snapshot,
            },
        })
    }

    /// Whether the volatile tail sits entirely ABOVE the immutable coverage.
    ///
    /// Conservative in the only direction that matters: a manifest with no pass
    /// entries — a hand-built archive, or one written before coverage was
    /// recorded — cannot prove separation, so the answer is `false`.
    fn volatile_is_separate(&self) -> bool {
        let Some(volatile_floor) = self
            .manifest
            .passes
            .iter()
            .filter(|p| p.kind == RangeKind::Volatile)
            .map(|p| p.floor)
            .min()
        else {
            return false;
        };
        let Some(max_immutable) = self
            .manifest
            .passes
            .iter()
            .filter(|p| p.kind == RangeKind::Immutable)
            .map(|p| p.ceiling)
            .max()
        else {
            // The tail is the only pass: every row is volatile, and there is
            // no immutable row for it to double-count.
            return true;
        };
        volatile_floor >= max_immutable
    }

    fn reconcile_files(
        &mut self,
        diagnose: policy_archive::Diagnose,
        include_volatile: bool,
        include_plumbing: bool,
    ) -> Result<(policy_archive::Reconciler, bool)> {
        let mut r = policy_archive::Reconciler::new(diagnose);
        let mut volatile_mint = false;
        for i in 0..self.files.len() {
            match self.files[i].kind {
                FileKind::Movements | FileKind::Corrections => {}
                // A different schema entirely; parsing it here would be the
                // exact mistake `kind_of` is ordered to prevent.
                FileKind::Observations => continue,
            }
            match self.files[i].range {
                RangeKind::Immutable => {}
                RangeKind::Volatile if include_volatile => {}
                RangeKind::Volatile => continue,
            }
            let volatile = self.files[i].range == RangeKind::Volatile;
            // ⚠️ Fetch the file's groups ONCE and decode them through ONE
            // reader. `read_group` builds a `SerializedFileReader` per call,
            // which re-parses the parquet footer and EVERY row group's thrift
            // metadata — per group, per pass. Measured as a real share of a
            // `/live` on a 38 MB archive (2026-10-24), and this is the one fold
            // that reads every group of both movements and corrections files.
            let groups = self.files[i].archive.num_groups();
            for g in 0..groups {
                self.files[i].fetch_group(g)?;
            }
            let f = &self.files[i];
            let reader = f.archive.reader(&f.bytes)?;
            for g in 0..groups {
                for m in Archive::read_group_with(&reader, g)?.iter() {
                    if !(include_plumbing || is_user_unit(&m.unit_name)) {
                        continue;
                    }
                    // \u26a0\ufe0f `net_mint`, not `amount`: a transfer in the tail
                    // changes `moved` and not supply, so it must not mark the
                    // supply provisional.
                    if volatile && m.net_mint != 0 {
                        volatile_mint = true;
                    }
                    r.observe(m);
                }
            }
        }
        Ok((r, volatile_mint))
    }

    /// Every row of one transaction, unfolded — see [`Self::movements_page`].
    pub fn movements_of(&mut self, tx_hash: &[u8]) -> Result<Vec<Movement>> {
        let mut rows = Vec::new();
        for f in &mut self.files {
            f.fetch_blooms()?;
            let candidates = f.archive.candidate_groups(&f.bytes, tx_hash)?;
            for g in candidates {
                f.fetch_group(g)?;
            }
            rows.extend(f.archive.find_tx(&f.bytes, tx_hash)?);
        }
        Ok(rows)
    }

    /// One transaction by hash, through the bloom filters.
    pub fn feed_row_at(&mut self, tx_hash: &[u8]) -> Result<Option<FeedRow>> {
        Ok(fold_rows(self.movements_of(tx_hash)?).pop())
    }

    /// Pull in corrections for the transactions in hand.
    fn attach_corrections(
        &mut self,
        rows: &mut Vec<Movement>,
        txs: &HashSet<Vec<u8>>,
    ) -> Result<()> {
        let (Some(lo), Some(hi)) = (
            rows.iter().map(|m| m.slot).min(),
            rows.iter().map(|m| m.slot).max(),
        ) else {
            return Ok(());
        };
        for f in &mut self.files {
            if f.kind != FileKind::Corrections {
                continue;
            }
            for g in f.archive.groups_overlapping(lo, hi) {
                f.fetch_group(g)?;
                rows.extend(
                    f.archive
                        .read_group(&f.bytes, g)?
                        .into_iter()
                        .filter(|m| txs.contains(&m.tx_hash)),
                );
            }
        }
        Ok(())
    }
}

/// The density of a set of rows — what a footer would say about them, computed
/// the same way. The live tier keeps its histogram incrementally instead
/// (`policy_api::Live`); this is the reference the test pins it against.
#[cfg_attr(not(test), allow(dead_code))]
pub fn density_of(rows: &[Movement], bucket_secs: u64) -> Vec<DensityBucket> {
    let bucket_secs = bucket_secs.max(1);
    // Per bucket: distinct transactions, and per transaction whether any row
    // minted or burned.
    let mut txs: BTreeMap<u64, HashMap<&[u8], (bool, bool)>> = BTreeMap::new();
    let mut movements: BTreeMap<u64, u64> = BTreeMap::new();
    for m in rows {
        let from = m.block_time / bucket_secs * bucket_secs;
        let flags = txs
            .entry(from)
            .or_default()
            .entry(&m.tx_hash)
            .or_insert((false, false));
        flags.0 |= m.net_mint > 0;
        flags.1 |= m.net_mint < 0;
        if !m.is_placeholder() {
            *movements.entry(from).or_insert(0) += 1;
        }
    }
    txs.into_iter()
        .map(|(from, by_tx)| DensityBucket {
            from_unix: from,
            to_unix: from + bucket_secs,
            movements: movements.get(&from).copied().unwrap_or(0),
            txs: by_tx.len() as u64,
            mints: by_tx.values().filter(|(m, _)| *m).count() as u64,
            burns: by_tx.values().filter(|(_, b)| *b).count() as u64,
        })
        .collect()
}

/// Sum two histograms bucket for bucket.
pub fn merge_density(a: Vec<DensityBucket>, b: Vec<DensityBucket>) -> Vec<DensityBucket> {
    let mut by: BTreeMap<u64, DensityBucket> = BTreeMap::new();
    for x in a.into_iter().chain(b) {
        let slot = by.entry(x.from_unix).or_insert(DensityBucket {
            from_unix: x.from_unix,
            to_unix: x.to_unix,
            ..DensityBucket::default()
        });
        slot.to_unix = slot.to_unix.max(x.to_unix);
        slot.movements += x.movements;
        slot.txs += x.txs;
        slot.mints += x.mints;
        slot.burns += x.burns;
    }
    by.into_values().collect()
}

// ─── CLI ─────────────────────────────────────────────────────────────────────

#[derive(clap::Args, Debug)]
pub struct InspectArgs {
    /// Archive root (`<root>/<policy_hex>/manifest.json`).
    #[arg(long, default_value = "archive")]
    pub archive_dir: PathBuf,
    /// 56-hex policy id.
    #[arg(long)]
    pub policy: String,
    /// Print a feed page of this many transactions.
    #[arg(long)]
    pub limit: Option<u32>,
    /// Look one transaction up by hash.
    #[arg(long)]
    pub tx: Option<String>,
    /// Print every density bucket rather than a summary.
    #[arg(long)]
    pub density: bool,
}

/// `token-ledger graph` — what a party-to-party movement graph over a whole
/// policy would actually weigh.
///
/// The feed is rows and a graph is EDGES, which is a reduction: 1.6M
/// movements on ClayNation collapse to however many distinct
/// `(from, to)` pairs there are, and that number — not the row count — is
/// what decides whether the graph can be one fetch the browser holds
/// locally rather than six hundred paged requests. Measured here rather
/// than estimated, because the archive is on disk and guessing at it is
/// how the footer-size question went wrong the first time.
///
/// Reads through [`PolicyArchive`], the same reader the Worker uses.
#[derive(clap::Args, Debug)]
pub struct GraphArgs {
    /// Archive root (`<root>/<policy_hex>/manifest.json`).
    #[arg(long, default_value = "archive")]
    pub archive_dir: PathBuf,
    /// 56-hex policy id.
    #[arg(long)]
    pub policy: String,
    /// Print the heaviest edges.
    #[arg(long, default_value_t = 10)]
    pub top: usize,
    /// Key parties by STAKE address rather than payment address.
    ///
    /// The archive stores payment addresses deliberately — CSwap collapses
    /// every pool onto one stake credential, so keying by stake would merge
    /// distinct pools into one holder. A movement GRAPH usually wants the
    /// opposite: a wallet, not a UTxO address. This measures what that
    /// costs and what it saves.
    #[arg(long)]
    pub by_stake: bool,
    /// Print the party dictionary, one per line, and nothing else — so its
    /// real compressed size can be measured rather than guessed at.
    #[arg(long)]
    pub dump_parties: bool,
    /// WRITE the artifact to `<policy>/graph.bin` (postcard, raw), and report
    /// what it weighs raw and gzipped. Without this the command only
    /// measures.
    #[arg(long)]
    pub write: bool,
    /// Break the stake-keyed SELF-LOOPS down by what the underlying payment
    /// addresses are, so "these are just UTxO shuffles" can be checked
    /// rather than assumed. See [`SelfLoops`].
    #[arg(long)]
    pub self_loops: bool,
    /// Everything that moved into or out of ONE party, and which contract
    /// credential each movement carried.
    ///
    /// The question this answers: a pile the frontend should have suppressed
    /// is on screen — did its inbound movements carry a script credential at
    /// all? A marketplace that settles through an ordinary wallet for some
    /// flows is indistinguishable from a holder without asking.
    #[arg(long)]
    pub party: Option<String>,
}

/// Where a transfer's two ends sit: an ordinary wallet, or a script.
///
/// A marketplace listing is a real transfer to a CONTRACT, so the archive
/// records `seller → venue` and later `venue → buyer` — two edges through a
/// party that is not a counterparty. Counting them is how we find out
/// whether the movement graph is mostly people trading, or mostly everyone
/// touching jpg.store.
#[derive(Default)]
struct Ends {
    wallet_to_wallet: u64,
    wallet_to_script: u64,
    script_to_wallet: u64,
    script_to_script: u64,
}

/// Why a stake-keyed edge points at its own node.
///
/// The reason this is a MEASUREMENT and not an assumption: a self-loop is
/// usually a wallet moving units between its own payment addresses, which is
/// noise in a movement graph. But protocols share staking credentials —
/// Splash and DexHunter run fourteen scripts on one — so a trade between two
/// distinct CONTRACT addresses collapses to a self-loop too, and that is real
/// flow. Dropping both because they look alike would hide the second.
#[derive(Default)]
struct SelfLoops {
    /// Both sides are key-credential addresses: one wallet's own addresses.
    /// A genuine shuffle.
    wallet: u64,
    /// Either side is a SCRIPT address. Distinct contracts merged by a
    /// shared staking credential — real movement, not a shuffle.
    script: u64,
    /// Identical payment address on both sides. Should not survive the fold
    /// (it nets to zero), so a non-zero count here is worth knowing about.
    same_address: u64,
    /// The SCRIPT addresses that appeared in a self-loop, and how often.
    ///
    /// Which contract it is decides what the movement MEANS: a marketplace
    /// whose validator keeps the seller's staking part turns a listing into
    /// a self-loop, and that is a listing to be marked rather than a move to
    /// be drawn. Naming them is how that stops being a guess.
    scripts: HashMap<String, u64>,
}

/// What the archive's rows reduce to.
#[derive(Default)]
struct Graph {
    by_stake: bool,
    txs: u64,
    unit_moves: u64,
    /// One loser, one gainer — an edge.
    transfers: u64,
    mints: u64,
    burns: u64,
    /// A gainer whose source is below the floor. On a COMPLETE archive this
    /// should be zero; anything else is an honest hole in the graph.
    source_below_floor: u64,
    /// Several parties on a side — a batched fill. NOT an edge: the
    /// pairing is genuinely unknown and the archive refuses to guess it.
    ambiguous: u64,
    /// Parties interned to ids, so an edge is 8 bytes rather than two
    /// bech32 strings.
    parties: HashMap<String, u32>,
    /// THE STREAM, unsorted — one entry per movement. Sorted by slot at
    /// `finish`, because the archive is paged newest-first.
    moves: Vec<RawMove>,
    /// Unit name hex → index into the asset dictionary.
    units: HashMap<String, u32>,
    /// Script PAYMENT CREDENTIAL hex → index. Tiny: a validator issues one
    /// address per seller but they all share this.
    scripts: HashMap<String, u32>,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
    /// Only meaningful when keyed by stake.
    loops: SelfLoops,
    /// Transfers dropped as wallet self-shuffles.
    self_shuffles: u64,
    /// Where transfers' ends sit — see [`Ends`].
    ends: Ends,
}

/// One movement as found, before the stream is ordered and delta-encoded.
struct RawMove {
    slot: u64,
    asset: u32,
    /// `NOBODY` for a mint.
    from: u32,
    /// `NOBODY` for a burn.
    to: u32,
    quantity: u64,
    /// `1 + index` into the script dictionary, or 0 for an ordinary wallet.
    from_script: u32,
    to_script: u32,
}

impl Graph {
    /// The artifact: dictionaries, and the movement stream ordered and
    /// delta-encoded.
    fn finish(self, policy: &str, complete: bool) -> policy_archive::MovementGraph {
        let mut all = vec![String::new(); self.parties.len()];
        for (addr, id) in &self.parties {
            all[*id as usize] = addr.clone();
        }
        // PRUNE the dictionary. Dropping wallet self-shuffles can leave
        // parties nothing references. The dictionary is the artifact's
        // dominant cost — 2.33 MB of ClayNation's 3.34 MB — so carrying
        // addresses no movement names would be paying for silence.
        let mut keep = vec![false; all.len()];
        for m in &self.moves {
            for p in [m.from, m.to] {
                if p != policy_archive::NOBODY {
                    keep[p as usize] = true;
                }
            }
        }
        let mut remap = vec![policy_archive::NOBODY; all.len()];
        let mut parties = Vec::new();
        for (old, addr) in all.into_iter().enumerate() {
            if keep[old] {
                remap[old] = parties.len() as u32;
                parties.push(addr);
            }
        }
        let id = |p: u32| match p == policy_archive::NOBODY {
            true => policy_archive::NOBODY,
            false => remap[p as usize],
        };

        let mut units = vec![String::new(); self.units.len()];
        for (name, i) in &self.units {
            units[*i as usize] = name.clone();
        }
        let mut scripts = vec![String::new(); self.scripts.len()];
        for (cred, i) in &self.scripts {
            scripts[*i as usize] = cred.clone();
        }

        // SLOT-ASCENDING: the archive pages newest-first, and a stream a
        // frontend plays has to run forwards. Deltas are only small if the
        // order is right.
        let mut moves = self.moves;
        moves.sort_unstable_by_key(|m| (m.slot, m.asset));
        let mut out = policy_archive::Moves::default();
        let mut prev = 0u64;
        for m in &moves {
            out.slot_deltas.push(m.slot - prev);
            prev = m.slot;
            out.assets.push(m.asset);
            out.from.push(id(m.from));
            out.to.push(id(m.to));
            out.quantities.push(m.quantity);
            out.from_script.push(m.from_script);
            out.to_script.push(m.to_script);
        }

        policy_archive::MovementGraph {
            format: policy_archive::GRAPH_FORMAT,
            policy: policy.to_string(),
            keyed_by: match self.by_stake {
                true => policy_archive::PartyKey::Stake,
                false => policy_archive::PartyKey::Payment,
            },
            from_slot: self.first_slot.unwrap_or(0),
            to_slot: self.last_slot.unwrap_or(0),
            complete,
            built_unix: crate::reverse::now_unix(),
            txs: self.txs,
            transfers: self.transfers,
            ambiguous: self.ambiguous,
            source_below_floor: self.source_below_floor,
            self_shuffles: self.self_shuffles,
            parties,
            units,
            scripts,
            moves: out,
        }
    }
}

/// A log, and what reading the archive to build it cost.
pub struct BuiltGraph {
    pub graph: policy_archive::MovementGraph,
    pub requests: usize,
    pub bytes: u64,
    pub secs: f64,
    loops: SelfLoops,
    ends: Ends,
}

impl Graph {
    /// The graph's key for an address: the wallet behind it, or the address
    /// itself when there is no stake part (an enterprise or script address).
    fn key(&self, addr: &str) -> String {
        match self.by_stake {
            true => crate::walk::stake_of(addr).unwrap_or_else(|| addr.to_string()),
            false => addr.to_string(),
        }
    }

    fn party(&mut self, addr: &str) -> u32 {
        if let Some(id) = self.parties.get(addr) {
            return *id;
        }
        let id = self.parties.len() as u32;
        self.parties.insert(addr.to_string(), id);
        id
    }

    /// The asset's stable identity — the dot's key in a holder field.
    fn asset(&mut self, name: &[u8]) -> u32 {
        let hex = hex::encode(name);
        if let Some(id) = self.units.get(&hex) {
            return *id;
        }
        let id = self.units.len() as u32;
        self.units.insert(hex, id);
        id
    }

    /// Units moved on one side of a transfer — the magnitude, taken from the
    /// gainer so it is positive whichever way the row was written.
    fn moved(unit: &UnitMove) -> u64 {
        unit.parties
            .iter()
            .filter(|p| p.amount > 0)
            .map(|p| p.amount as u64)
            .sum()
    }

    /// Is this a SCRIPT address? A marketplace listing is a transfer to one.
    fn is_script(addr: &str) -> bool {
        matches!(
            pallas_addresses::Address::from_bech32(addr),
            Ok(pallas_addresses::Address::Shelley(sh)) if sh.payment().is_script()
        )
    }

    /// `1 + index` of this address's script payment credential, or 0 when it
    /// is an ordinary wallet.
    ///
    /// The CREDENTIAL, not the address: Wayup's validator issues a different
    /// address per seller — the staking part varies — but every one of them
    /// carries `a76f0fb8…`, so interning the credential turns thousands of
    /// listings into one dictionary entry and gives a reader something the
    /// venue registry can actually match.
    fn script_cred(&mut self, addr: &str) -> u32 {
        let Ok(pallas_addresses::Address::Shelley(sh)) =
            pallas_addresses::Address::from_bech32(addr)
        else {
            return 0;
        };
        let hex = match sh.payment() {
            pallas_addresses::ShelleyPaymentPart::Script(h) => hex::encode(h.as_ref()),
            pallas_addresses::ShelleyPaymentPart::Key(_) => return 0,
        };
        if let Some(id) = self.scripts.get(&hex) {
            return id + 1;
        }
        let id = self.scripts.len() as u32;
        self.scripts.insert(hex, id);
        id + 1
    }

    fn add(&mut self, row: &FeedRow) {
        self.txs += 1;
        self.first_slot = Some(self.first_slot.map_or(row.slot, |s: u64| s.min(row.slot)));
        self.last_slot = Some(self.last_slot.map_or(row.slot, |s: u64| s.max(row.slot)));
        for unit in &row.units {
            self.unit_moves += 1;
            let units = Self::moved(unit);
            match policy_archive::feed::direction(unit) {
                policy_archive::feed::Direction::Transfer { from, to } => {
                    self.transfers += 1;
                    match (Self::is_script(&from), Self::is_script(&to)) {
                        (false, false) => self.ends.wallet_to_wallet += 1,
                        (false, true) => self.ends.wallet_to_script += 1,
                        (true, false) => self.ends.script_to_wallet += 1,
                        (true, true) => self.ends.script_to_script += 1,
                    }
                    // A stake-keyed self-loop is one of two different things,
                    // and only one of them is noise. Decided by what the
                    // ADDRESS IS, never by the shape of the movement.
                    if self.by_stake && self.key(&from) == self.key(&to) {
                        match (from == to, Self::is_script(&from) || Self::is_script(&to)) {
                            // Nets out in the fold; should never reach here.
                            (true, _) => self.loops.same_address += 1,
                            // Distinct CONTRACTS sharing a staking credential
                            // — real flow, and it stays a movement.
                            (false, true) => {
                                self.loops.script += 1;
                                for a in [&from, &to] {
                                    if Self::is_script(a) {
                                        *self.loops.scripts.entry(a.clone()).or_insert(0) += 1;
                                    }
                                }
                            }
                            // A wallet reorganising its own UTxOs. Not a
                            // movement between parties: counted, dropped.
                            (false, false) => {
                                self.loops.wallet += 1;
                                self.self_shuffles += 1;
                                continue;
                            }
                        }
                    }
                    let asset = self.asset(&unit.name);
                    // WHICH contract, when there was one. Recorded before
                    // the party keys, because stake-keying is what throws
                    // this away: a Wayup listing keeps the seller's stake on
                    // both sides and is indistinguishable from a reshuffle
                    // without it.
                    let (fs, ts) = (self.script_cred(&from), self.script_cred(&to));
                    let (f, t) = (self.party(&self.key(&from)), self.party(&self.key(&to)));
                    self.moves.push(RawMove {
                        slot: row.slot,
                        asset,
                        from: f,
                        to: t,
                        quantity: units,
                        from_script: fs,
                        to_script: ts,
                    });
                }
                policy_archive::feed::Direction::Mint { to } => {
                    self.mints += 1;
                    let asset = self.asset(&unit.name);
                    let ts = self.script_cred(&to);
                    let t = self.party(&self.key(&to));
                    self.moves.push(RawMove {
                        slot: row.slot,
                        asset,
                        from: policy_archive::NOBODY,
                        to: t,
                        quantity: units,
                        from_script: 0,
                        to_script: ts,
                    });
                }
                policy_archive::feed::Direction::Burn { from } => {
                    self.burns += 1;
                    // A burn's magnitude is the LOSER's side; `moved` reads
                    // gainers, so take it from the negative parties.
                    let qty: u64 = unit
                        .parties
                        .iter()
                        .filter(|p| p.amount < 0)
                        .map(|p| p.amount.unsigned_abs())
                        .sum();
                    let asset = self.asset(&unit.name);
                    let fs = self.script_cred(&from);
                    let f = self.party(&self.key(&from));
                    self.moves.push(RawMove {
                        slot: row.slot,
                        asset,
                        from: f,
                        to: policy_archive::NOBODY,
                        quantity: qty,
                        from_script: fs,
                        to_script: 0,
                    });
                }
                // We know it arrived but not from where. Emitting it as a
                // mint would be a lie; it is counted and left out, which on
                // a COMPLETE archive is zero movements.
                policy_archive::feed::Direction::SourceBelowFloor { .. } => {
                    self.source_below_floor += 1
                }
                policy_archive::feed::Direction::Ambiguous => self.ambiguous += 1,
            }
        }
    }
}

pub fn build_graph(dir: &Path, policy: &str, by_stake: bool) -> Result<Option<BuiltGraph>> {
    let Some(mut a) = PolicyArchive::open(dir)? else {
        return Ok(None);
    };
    let complete = a.manifest.completeness() == Completeness::Complete;
    let started = std::time::Instant::now();

    // Page the whole archive newest-first through the reader a Worker uses.
    // `before` walks down by slot; a page that returns nothing new ends it,
    // which also guards the pathological case of one slot holding more
    // transactions than a page.
    let mut g = Graph {
        by_stake,
        ..Graph::default()
    };
    let mut before: Option<u64> = None;
    const PAGE: u32 = 5_000;
    loop {
        let page = a.feed_rows(PAGE, before)?;
        let Some(oldest) = page.iter().map(|r| r.slot).min() else {
            break;
        };
        for row in &page {
            g.add(row);
        }
        let next = Some(oldest);
        if next == before {
            break;
        }
        before = next;
    }
    let (requests, bytes) = a.fetched();
    let loops = std::mem::take(&mut g.loops);
    let ends = std::mem::take(&mut g.ends);
    Ok(Some(BuiltGraph {
        graph: g.finish(policy, complete),
        requests,
        bytes,
        secs: started.elapsed().as_secs_f64(),
        loops,
        ends,
    }))
}

/// An open archive plus the WHOLE-HISTORY decode it can hand out once.
///
/// ⚠️ Why this exists at all. A holder balance and an all-time price series are
/// both cumulative folds over every movement, so each opened the archive and
/// decoded all of it separately — twice over 38 MB of movements parquet for a
/// single `/live` on the widest archive here. The rows are the same rows; only
/// the fold differs, so the decode is done once and lent to both.
///
/// The walk is [`PolicyArchive::movements_page`] driven to exhaustion, which is
/// exactly what each fold did on its own, so nothing about the row set changes.
pub struct History {
    archive: PolicyArchive,
    all: Option<Vec<Movement>>,
}

impl History {
    /// `None` when no pass has ever run for this policy.
    pub fn open(dir: &Path) -> Result<Option<Self>> {
        Ok(PolicyArchive::open(dir)?.map(|archive| Self { archive, all: None }))
    }

    /// Every movement in the archive, decoded ONCE.
    ///
    /// ⚠️ The WHOLE archive, not a window: a balance is cumulative over all
    /// history, and a page of it is not a smaller version of the answer, it is
    /// a different and wrong one — see [`build_holders`].
    pub fn all_movements(&mut self) -> Result<&[Movement]> {
        if self.all.is_none() {
            let mut all: Vec<Movement> = Vec::new();
            let mut before: Option<u64> = None;
            const PAGE: u32 = 5_000;
            loop {
                let page = self.archive.movements_page(PAGE, before)?;
                let Some(oldest) = page.iter().map(|m| m.slot).min() else {
                    break;
                };
                all.extend(page);
                let next = Some(oldest);
                // A page that does not advance ends the walk — which also
                // guards the pathological case of one slot holding more rows
                // than a page.
                if next == before {
                    break;
                }
                before = next;
            }
            self.all = Some(all);
        }
        Ok(self.all.as_deref().unwrap_or(&[]))
    }

    /// Every movement ABOVE `slot`, decoded — the tail half of a carried fold.
    ///
    /// ⚠️ Skips whole row groups by their recorded slot range, so the cost is
    /// proportional to what is above `slot` rather than to the archive. That is
    /// the entire point: the archive grows, and the tail does not.
    ///
    /// Corrections are attached exactly as [`PolicyArchive::movements_page`]
    /// attaches them, because a correction is where a later pass records the
    /// source it finally resolved — reading movements alone would report those
    /// rows as unresolved again.
    pub fn movements_above(&mut self, slot: u64) -> Result<Vec<Movement>> {
        let mut candidates: Vec<(usize, usize, u64)> = Vec::new();
        for (i, f) in self.archive.files.iter().enumerate() {
            if f.kind != FileKind::Movements {
                continue;
            }
            for g in 0..f.archive.num_groups() {
                if let Some((_, hi)) = f.archive.slot_range(g)
                    && hi > slot
                {
                    candidates.push((i, g, hi));
                }
            }
        }
        candidates.sort_by_key(|c| std::cmp::Reverse(c.2));

        let mut rows: Vec<Movement> = Vec::new();
        let mut txs: HashSet<Vec<u8>> = HashSet::new();
        for (i, g, _) in candidates {
            let f = &mut self.archive.files[i];
            f.fetch_group(g)?;
            for m in f.archive.read_group(&f.bytes, g)? {
                if m.slot > slot {
                    txs.insert(m.tx_hash.clone());
                    rows.push(m);
                }
            }
        }
        self.archive.attach_corrections(&mut rows, &txs)?;
        Ok(rows)
    }
}

/// Fold every movement in an archive into a holder table.
///
/// ⚠️ Pages the WHOLE archive, newest-first, exactly as [`build_graph`] does —
/// a balance is a cumulative fold over all history, and a page of it is not a
/// smaller version of the answer, it is a different and wrong one.
///
/// Costs one pass over the movement rows: $PERP is 15,258 of them, $NIKEPIG's
/// archive rather more. That is why the result is worth caching upstream and
/// why the route below takes a `limit` on the OUTPUT rather than the input.
pub fn build_holders(dir: &Path) -> Result<Option<policy_archive::holders::Fold>> {
    let Some(mut h) = History::open(dir)? else {
        return Ok(None);
    };
    Ok(Some(holders_from(&mut h)?))
}

/// [`build_holders`] against an ALREADY-OPEN [`History`].
///
/// Split out so a caller that also wants the series shares ONE decode of the
/// history: `/live` asks for both, and each used to walk the archive alone.
pub fn holders_from(h: &mut History) -> Result<policy_archive::holders::Fold> {
    Ok(policy_archive::holders::fold(
        h.all_movements()?.iter().cloned(),
    ))
}

/// Fold a policy's WHOLE archive into the series, and keep what a later fold
/// needs to continue it.
///
/// ⚠️ The whole archive, not a window. An all-time high over the last 5,000
/// movements is not an all-time high, and a 30-day change needs 30 days.
///
/// Built from the STORY rather than from movements and observations
/// separately, so the merge onto one slot spine — and the intra-slot ordering
/// that values a trade at the PRE-swap price — has exactly one definition.
///
/// `settled_to` is the highest slot EITHER tier reached, because a tail fold
/// resumes above it: taking the movements' tip when an observation sat higher
/// would drop that observation from every read.
pub fn carry_from(
    h: &mut History,
    observations: &[policy_archive::Observation],
) -> Result<policy_archive::series::Carry> {
    let movements = h.all_movements()?.to_vec();
    let settled_to = movements
        .iter()
        .map(|m| m.slot)
        .chain(observations.iter().map(|o| o.slot))
        .max()
        .unwrap_or(0);
    let roles = venue_roles_for(observations);
    let story = policy_archive::story::build(&fold_rows(movements), observations, &roles);
    let (series, view) = policy_archive::series::fold_onto(
        &story,
        &policy_archive::view::Projection::default(),
        &policy_archive::series::Carry::default(),
    );
    Ok(policy_archive::series::Carry {
        series,
        view,
        roles,
        settled_to,
    })
}

/// Continue a carried series through everything ABOVE its boundary.
///
/// This is the read that replaced folding the whole archive: the rows are the
/// ones above `carry.settled_to`, which the archive keeps in its volatile
/// files, and the group walk skips any row group whose recorded slot range
/// ends below it. Cost is proportional to the tail, not to the history.
///
/// ⚠️ A GAP would be silent. If the archive's own coverage does not reach
/// continuously from `settled_to` to the tail, a fold here would simply not
/// see the rows in between, and the series would lose a stretch of trades
/// without saying so. The caller checks that and folds the whole archive
/// instead — see `policy_api::live_folds`.
pub fn tail_series(
    h: &mut History,
    dir: &Path,
    manifest: &Manifest,
    carry: &policy_archive::series::Carry,
) -> Result<policy_archive::series::Series> {
    let observations = read_observations_above(dir, manifest, carry.settled_to)?;
    let movements = h.movements_above(carry.settled_to)?;
    // The tail's own observations can name a venue the settled half never saw
    // (a launch that happened since). The carried table wins where they
    // disagree — see `Roles::merge_missing`.
    let mut roles = carry.roles.clone();
    roles.merge_missing(&venue_roles_for(&observations));
    let story = policy_archive::story::build(&fold_rows(movements), &observations, &roles);
    Ok(policy_archive::series::fold_onto(
        &story,
        &policy_archive::view::Projection::default(),
        carry,
    )
    .0)
}

/// Fold the WHOLE archive into the price + volume series and write it beside
/// the graph, as [`policy_archive::SERIES`].
///
/// ⚠️ This exists to keep the fold OFF the request path. `/live` folded the
/// whole archive on every cache miss — 6.6 s of a 10.3 s document on the widest
/// archive here — and it misses every time the tail rebuild moves the archive,
/// which is every 300 s. Writing it where the archive is already published
/// moves that cost into the publisher, which does the same work for
/// `graph.bin`.
///
/// Written through a RENAME: `/live` reads this while the scheduler writes it,
/// and half a postcard stream decodes as corruption rather than as "not yet".
/// Write BOTH derived artefacts — `series.bin` and `holders.bin` — from ONE
/// read of the archive.
///
/// ⚠️ One decode, not two. Each fold walks every movement row and the walk is
/// the expensive half; [`History`] memoises it, so the second fold is nearly
/// free. They also share a boundary, so a reader folds one tail for the pair.
pub fn write_derived(dir: &Path) -> Result<()> {
    let Some(manifest) = load_manifest(dir)? else {
        return Ok(());
    };
    let observations = read_observations(dir, &manifest)?;
    let Some(mut history) = History::open(dir)? else {
        return Ok(());
    };
    let series = carry_from(&mut history, &observations)?;
    let held = holders_carry_from(&mut history)?;
    let settled_to = series.settled_to;
    let key = immutable_to(&manifest);
    store_series(dir, key, &series)?;
    store_holders(dir, key, settled_to, &held)?;
    // THE SUMMARY, from the same decode and the same series: the third reader
    // of these rows, so it costs a sort and a fold, never another read.
    let complete = manifest.completeness() == Completeness::Complete;
    let policy = policy_of(dir);
    let (summary, checkpoints) = policy_archive::summary::build(&policy_archive::summary::Inputs {
        policy: &policy,
        series: &series.series,
        movements: history.all_movements()?,
        immutable_to: key,
        complete,
        classify: &classify_holder,
    });
    store_summary(dir, &summary, &checkpoints)?;
    Ok(())
}

/// The policy a directory holds: its name, as [`policy_dir`] builds it.
fn policy_of(dir: &Path) -> String {
    dir.file_name()
        .map(|n| n.to_string_lossy().to_lowercase())
        .unwrap_or_default()
}

/// What kind of holder an address is, by what the address registry can state
/// about the ADDRESS: a burn sink, then the registry's script categories by
/// payment credential and then by address. A script nobody named, with no
/// stake part, is a contract. Everything else is a person.
///
/// WHAT, never WHO. A service run from an ordinary wallet (an exchange's hot
/// wallet, a minting provider, a staking programme) is a claim about people,
/// kept in the published stake-service table (`address_registry::published`)
/// and applied by whoever DRAWS the summary, so a correction reaches every
/// screen without rewriting a summary. This used to name such wallets from
/// the compiled stake table and store them as venues, which put a person's
/// wallet (listed once as Wayup) in summaries as a venue, and would have
/// stored `$perpstaking` as one too.
pub fn classify_holder(address: &str) -> (policy_archive::summary::Role, Option<String>) {
    use address_registry::registry::{
        AddressCategory as AC, RegistryNetwork, ScriptCategory as SC, lookup_address_match,
        lookup_payment_credential,
    };
    use policy_archive::summary::Role;

    let parts = policy_archive::trade::address_parts(address);
    let by_credential = parts
        .as_ref()
        .and_then(|(pay, _)| lookup_payment_credential(pay))
        .map(|e| (&e.category, e.category.to_string()));
    let by_address = || {
        lookup_address_match(address, RegistryNetwork::Mainnet)
            .map(|(cat, _)| (cat, cat.to_string()))
    };
    if let Some((category, label)) = by_credential.or_else(by_address) {
        let role = match category {
            AC::Script(SC::Burn { .. }) => Some(Role::Burn),
            AC::Script(SC::Vesting { .. }) => Some(Role::Lock),
            AC::Script(SC::Staking { .. }) => Some(Role::Staking),
            AC::Marketplace(_) | AC::Script(_) => Some(Role::Venue),
            AC::Unknown => None,
        };
        if let Some(role) = role {
            return (role, Some(label));
        }
    }
    let stake = parts.as_ref().and_then(|(_, stake)| stake.as_deref());
    let script = address_registry::registry::payment_credential_is_script(address);
    match (script, stake) {
        (true, None) => (Role::Contract, None),
        _ => (Role::Person, None),
    }
}

/// Write the summary and its checkpoint files, and remove checkpoint files it
/// no longer names.
///
/// The summary through a RENAME, as the series is: a reader can open it while
/// this writes. A checkpoint file is named by its content
/// ([`policy_archive::summary::checkpoint_rel`]), so one already on disk is
/// left alone, and the publisher uploads only the new ones.
pub fn store_summary(
    dir: &Path,
    summary: &policy_archive::Summary,
    checkpoints: &[policy_archive::Checkpoint],
) -> Result<usize> {
    let cp_dir = dir.join(policy_archive::CHECKPOINTS);
    std::fs::create_dir_all(&cp_dir).with_context(|| format!("creating {}", cp_dir.display()))?;
    for cp in checkpoints {
        let raw = cp.encode().map_err(|e| anyhow::anyhow!("{e}"))?;
        let path = dir.join(policy_archive::summary::checkpoint_rel(cp.slot, &raw));
        if path.exists() {
            continue;
        }
        let tmp = path.with_extension("bin.tmp");
        std::fs::write(&tmp, &raw).with_context(|| format!("writing {}", tmp.display()))?;
        std::fs::rename(&tmp, &path)
            .with_context(|| format!("renaming into {}", path.display()))?;
    }
    let named = summary.files();
    for entry in std::fs::read_dir(&cp_dir)? {
        let entry = entry?;
        let rel = format!(
            "{}/{}",
            policy_archive::CHECKPOINTS,
            entry.file_name().to_string_lossy()
        );
        if !named.contains(&rel) {
            std::fs::remove_file(entry.path())
                .with_context(|| format!("removing {}", entry.path().display()))?;
        }
    }

    let raw = summary.encode().map_err(|e| anyhow::anyhow!("{e}"))?;
    let path = dir.join(policy_archive::SUMMARY);
    let tmp = dir.join(format!("{}.tmp", policy_archive::SUMMARY));
    std::fs::write(&tmp, &raw).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, &path).with_context(|| format!("renaming into {}", path.display()))?;
    tracing::info!(
        policy = summary.policy,
        bytes = raw.len(),
        days = summary.daily.len(),
        people = summary.distribution.people,
        checkpoints = checkpoints.len(),
        "policy: summary written"
    );
    Ok(raw.len())
}

/// The written summary, whatever archive version it describes. `Ok(None)` when
/// there is none.
pub fn read_summary(dir: &Path) -> Result<Option<policy_archive::Summary>> {
    let path = dir.join(policy_archive::SUMMARY);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    Ok(Some(
        policy_archive::Summary::decode(&bytes).map_err(|e| anyhow::anyhow!("{e}"))?,
    ))
}

/// `token-ledger summary` — fold and write one policy's summary by hand, and
/// say what is in it.
#[derive(clap::Args, Debug)]
pub struct SummaryArgs {
    /// Archive root (`<root>/<policy_hex>/manifest.json`).
    #[arg(long, default_value = "archive")]
    pub archive_dir: PathBuf,
    /// 56-hex policy id.
    #[arg(long)]
    pub policy: String,
}

pub fn summary(args: SummaryArgs) -> Result<()> {
    let dir = policy_dir(&args.archive_dir, &args.policy.to_lowercase());
    let started = std::time::Instant::now();
    write_derived(&dir)?;
    let Some(s) = read_summary(&dir)? else {
        bail!("no archive at {}", dir.display());
    };
    let d = &s.distribution;
    println!(
        "{}: folded in {:.1}s, complete {}, {} days, {} hourly, {} unvalued trades",
        s.policy,
        started.elapsed().as_secs_f64(),
        s.complete,
        s.daily.len(),
        s.hourly.len(),
        s.unvalued
    );
    println!(
        "held {} burned {} circulating {}; {} parties, {} people, {} negative",
        d.held, d.burned, d.circulating, d.parties, d.people, d.negative
    );
    println!(
        "top 1 / 10 / 100: {:.2}% / {:.2}% / {:.2}%",
        d.top1_ppm as f64 / 1e4,
        d.top10_ppm as f64 / 1e4,
        d.top100_ppm as f64 / 1e4
    );
    for b in &d.bands {
        println!(
            "  band ≥{:>7.3}%: {:>6} people, {}",
            b.floor_ppm as f64 / 1e4,
            b.people,
            b.amount
        );
    }
    for r in &d.roles {
        println!("  {:?}: {} parties, {}", r.role, r.parties, r.amount);
    }
    let bytes: u64 = s
        .checkpoints
        .iter()
        .filter_map(|c| std::fs::metadata(dir.join(&c.rel)).ok())
        .map(|m| m.len())
        .sum();
    println!(
        "{} checkpoints, {} bytes; summary {} bytes",
        s.checkpoints.len(),
        bytes,
        std::fs::metadata(dir.join(policy_archive::SUMMARY)).map_or(0, |m| m.len())
    );
    Ok(())
}

/// The balance state at the archive's tip, for writing as an artefact.
pub fn holders_carry_from(h: &mut History) -> Result<policy_archive::holders::Carry> {
    Ok(policy_archive::holders::carry(
        h.all_movements()?.iter().cloned(),
    ))
}

/// Continue a carried balance table through every movement ABOVE `settled_to`.
///
/// Cheap by construction: [`History::movements_above`] skips whole row groups by
/// their recorded slot range, so this reads the tail rather than the history.
pub fn holders_tail(
    h: &mut History,
    carry: &policy_archive::holders::Carry,
    settled_to: u64,
) -> Result<policy_archive::holders::Fold> {
    Ok(policy_archive::holders::fold_onto(
        h.movements_above(settled_to)?.into_iter(),
        carry,
    ))
}

/// Write an ALREADY-FOLDED balance table as [`policy_archive::HOLDERS`].
pub fn store_holders(
    dir: &Path,
    immutable_to: u64,
    settled_to: u64,
    carry: &policy_archive::holders::Carry,
) -> Result<usize> {
    let raw = policy_archive::HoldersFile {
        format: policy_archive::HOLDERS_FORMAT,
        immutable_to,
        settled_to,
        carry: carry.clone(),
    }
    .encode()
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    let path = dir.join(policy_archive::HOLDERS);
    let tmp = dir.join(format!("{}.tmp", policy_archive::HOLDERS));
    std::fs::write(&tmp, &raw).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, &path).with_context(|| format!("renaming into {}", path.display()))?;
    Ok(raw.len())
}

/// The written balance table, when it describes the archive version in hand.
pub fn read_holders(dir: &Path, immutable_to: u64) -> Result<Option<policy_archive::HoldersFile>> {
    let path = dir.join(policy_archive::HOLDERS);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let file = policy_archive::HoldersFile::decode(&bytes)?;
    Ok((file.immutable_to == immutable_to).then_some(file))
}

/// The archive's IMMUTABLE coverage ceiling — what invalidates a written
/// series.
///
/// ⚠️ Not `updated_unix`: the tail rebuild moves that every 300 s without
/// changing a single row at or below the carried boundary. What does
/// invalidate the artefact is immutable coverage growing, because those rows
/// are below every reader's start point and only a re-fold includes them.
pub fn immutable_to(m: &Manifest) -> u64 {
    m.passes
        .iter()
        .filter(|p| p.kind == RangeKind::Immutable)
        .map(|p| p.ceiling)
        .max()
        .unwrap_or(0)
}

/// Write an ALREADY-FOLDED series as [`policy_archive::SERIES`].
///
/// Split from [`write_series`] so a reader that has just paid for the fold can
/// record it: `/live` builds the series on a cache miss anyway, and writing it
/// there is what makes the artefact exist for a policy the publisher has not
/// rewritten since the last restart.
///
/// Written through a RENAME: `/live` reads this while the scheduler writes it,
/// and half a postcard stream decodes as corruption rather than as "not yet".
pub fn store_series(
    dir: &Path,
    immutable_to: u64,
    carry: &policy_archive::series::Carry,
) -> Result<usize> {
    let raw = policy_archive::SeriesFile {
        format: policy_archive::SERIES_FORMAT,
        immutable_to,
        carry: carry.clone(),
    }
    .encode()
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    let path = dir.join(policy_archive::SERIES);
    let tmp = dir.join(format!("{}.tmp", policy_archive::SERIES));
    std::fs::write(&tmp, &raw).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, &path).with_context(|| format!("renaming into {}", path.display()))?;
    Ok(raw.len())
}

/// The written series, when it describes the archive version in hand.
///
/// `Ok(None)` is the ordinary answer for an archive a publisher has not
/// rewritten yet — the caller folds instead, which is what every read did
/// before this existed. `Err` means the file is there and unusable, which is
/// worth saying out loud rather than silently paying for the fold again.
pub fn read_series(dir: &Path, immutable_to: u64) -> Result<Option<policy_archive::series::Carry>> {
    let path = dir.join(policy_archive::SERIES);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let file = policy_archive::SeriesFile::decode(&bytes)?;
    Ok((file.immutable_to == immutable_to).then_some(file.carry))
}

/// Build the graph and write it beside the manifest, RAW. Returns its size,
/// or `None` when the policy has no archive. The publisher compresses at
/// upload time and decides then whether that pays.
pub fn write_graph(dir: &Path, policy: &str, by_stake: bool) -> Result<Option<usize>> {
    let Some(built) = build_graph(dir, policy, by_stake)? else {
        return Ok(None);
    };
    let raw = built.graph.encode()?;
    std::fs::write(dir.join(policy_archive::GRAPH), &raw)?;
    tracing::info!(
        policy,
        parties = built.graph.parties.len(),
        movements = built.graph.moves.len(),
        bytes = raw.len(),
        secs = format!("{:.1}", built.secs),
        "policy: movement graph written"
    );
    Ok(Some(raw.len()))
}

pub fn graph(args: GraphArgs) -> Result<()> {
    let dir = policy_dir(&args.archive_dir, &args.policy.to_lowercase());
    let policy = args.policy.to_lowercase();
    let Some(built) = build_graph(&dir, &policy, args.by_stake)? else {
        println!("no archive at {}", dir.display());
        return Ok(());
    };
    let g = &built.graph;

    if args.dump_parties {
        for addr in &g.parties {
            println!("{addr}");
        }
        return Ok(());
    }
    let edges = g.edges();
    let mints = g.moves.iter().filter(|m| m.from.is_none()).count();
    let burns = g.moves.iter().filter(|m| m.to.is_none()).count();
    println!("policy         {}", g.policy);
    println!(
        "keyed by       {}",
        match g.keyed_by {
            policy_archive::PartyKey::Stake => "stake address (the wallet)",
            policy_archive::PartyKey::Payment => "payment address (the UTxO)",
        }
    );
    println!(
        "completeness   {}",
        match g.complete {
            true => "complete",
            false => "partial",
        }
    );
    println!(
        "read           {} requests, {:.1} MB, {:.1}s",
        built.requests,
        built.bytes as f64 / 1e6,
        built.secs
    );
    println!("transactions   {}", g.txs);
    println!(
        "unit moves     {}",
        g.transfers + mints as u64 + burns as u64 + g.ambiguous + g.source_below_floor
    );
    println!("  transfers    {}  (an edge each)", g.transfers);
    println!("  mints        {mints}");
    println!("  burns        {burns}");
    println!("  below floor  {}", g.source_below_floor);
    println!(
        "  ambiguous    {}  (batched — no edge, never guessed)",
        g.ambiguous
    );
    println!(
        "  self-shuffle {}  (a wallet's own UTxOs — DROPPED, see below)",
        g.self_shuffles
    );
    println!(
        "parties        {}  (after pruning any nothing references)",
        g.parties.len()
    );
    println!(
        "movements     {}  (the stream a frontend plays)",
        g.moves.len()
    );
    println!("assets         {}", g.units.len());
    // The contracts the stream touched, by payment credential. A handful by
    // construction — a validator issues an address per seller and they all
    // share one credential — so printing them whole is the point: this is
    // what a reader matches against the venue registry.
    println!("contracts      {}", g.scripts.len());
    for (i, cred) in g.scripts.iter().enumerate() {
        let touched = g
            .moves
            .iter()
            .filter(|m| m.from_script == Some(i as u32) || m.to_script == Some(i as u32))
            .count();
        println!("  {touched:>7}×  {cred}");
    }

    // ONE PARTY, and what its movements actually carried.
    if let Some(want) = args.party.as_deref() {
        let id = g.parties.iter().position(|p| p == want);
        match id {
            None => println!("party          {want} — not in this log"),
            Some(id) => {
                let id = id as u32;
                let cred = |i: Option<u32>| match i.and_then(|i| g.scripts.get(i as usize)) {
                    Some(c) => c.as_str(),
                    None => "(a wallet — no script credential)",
                };
                let mut inbound: BTreeMap<&str, u64> = BTreeMap::new();
                let mut outbound: BTreeMap<&str, u64> = BTreeMap::new();
                for m in g.moves.iter() {
                    if m.to == Some(id) {
                        *inbound.entry(cred(m.to_script)).or_insert(0) += 1;
                    }
                    if m.from == Some(id) {
                        *outbound.entry(cred(m.from_script)).or_insert(0) += 1;
                    }
                }
                println!("party          {want}");
                println!("  INBOUND — what the destination address was:");
                for (c, n) in &inbound {
                    println!("    {n:>7}×  {c}");
                }
                println!("  OUTBOUND — what the source address was:");
                for (c, n) in &outbound {
                    println!("    {n:>7}×  {c}");
                }
            }
        }
    }
    println!(
        "DISTINCT EDGES {}  (derived: the stream folded)",
        edges.len()
    );

    if args.self_loops {
        let e = &built.ends;
        let via_script = e.wallet_to_script + e.script_to_wallet + e.script_to_script;
        println!("WHERE ENDS SIT (every transfer, before the self-shuffle drop)");
        println!("  wallet → wallet {}", e.wallet_to_wallet);
        println!(
            "  wallet → script {}  (a listing, or any contract deposit)",
            e.wallet_to_script
        );
        println!(
            "  script → wallet {}  (a sale settling, or a delist)",
            e.script_to_wallet
        );
        println!("  script → script {}", e.script_to_script);
        println!(
            "  VIA A CONTRACT  {via_script}  = {:.1}% of transfers",
            100.0 * via_script as f64 / built.graph.transfers.max(1) as f64
        );
        let l = &built.loops;
        let total = l.wallet + l.script + l.same_address;
        println!("SELF-LOOPS     {total} transfers land on their own node");
        println!(
            "  wallet       {}  (both sides key-credential — a genuine UTxO shuffle)",
            l.wallet
        );
        println!(
            "  script       {}  (a SCRIPT address either side — distinct contracts sharing a staking credential, NOT a shuffle)",
            l.script
        );
        println!(
            "  same address {}  (should be zero: it nets out in the fold)",
            l.same_address
        );
        // WHICH CONTRACTS. A self-loop through a marketplace validator that
        // keeps the seller's staking part is a LISTING, not a move — and
        // that can only be told apart by naming the address.
        let mut scripts: Vec<(&String, &u64)> = l.scripts.iter().collect();
        scripts.sort_by_key(|(_, n)| std::cmp::Reverse(**n));
        for (addr, n) in scripts.iter().take(args.top.max(5)) {
            println!("    {n:>7}×  {addr}");
        }
    }

    if args.top > 0 && !edges.is_empty() {
        let mut top: Vec<&policy_archive::Edge> = edges.iter().collect();
        top.sort_by_key(|e| std::cmp::Reverse(e.count));
        println!("heaviest edges");
        let short = |id: u32| {
            g.parties
                .get(id as usize)
                .map(|a| match a.len() > 20 {
                    true => format!("{}…{}", &a[..12], &a[a.len() - 6..]),
                    false => a.clone(),
                })
                .unwrap_or_default()
        };
        for e in top.iter().take(args.top) {
            println!("  {:>6}×  {} → {}", e.count, short(e.from), short(e.to));
        }
    }

    if args.write {
        let raw = g.encode()?;
        let path = dir.join(policy_archive::GRAPH);
        std::fs::write(&path, &raw)?;
        // MEASURE the compression rather than assume it: the token path's
        // `txids` artifact came out 6 KB larger gzipped, and whether a body
        // compresses depends on what is in it. The publisher makes the same
        // comparison at upload time; this is the number that decides.
        let gz = gzip(&raw)?;
        println!(
            "wrote          {} — {:.2} MB raw, {:.2} MB gzipped ({:.1}×)",
            path.display(),
            raw.len() as f64 / 1e6,
            gz.len() as f64 / 1e6,
            raw.len() as f64 / gz.len().max(1) as f64
        );
        if gz.len() >= raw.len() {
            println!("               gzip made it BIGGER — the publisher will store it raw");
        }
        // Read it back through the decoder a frontend would use. An artifact
        // nothing can parse is the failure this catches, and the reason the
        // reader landed with the writer everywhere else in this tool.
        let back = policy_archive::MovementGraph::decode(&raw)?;
        println!(
            "verified       {} parties, {} edges, keyed by {}",
            back.parties.len(),
            back.edges().len(),
            back.keyed_by.as_wire()
        );
    }
    Ok(())
}

/// gzip, in process. The token path shells out to `gzip -9` from a script the
/// user called "an incredibly brittle surface"; this is the same compression
/// without the shell.
pub fn gzip(bytes: &[u8]) -> Result<Vec<u8>> {
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::Write;
    let mut e = GzEncoder::new(Vec::new(), Compression::best());
    e.write_all(bytes)?;
    Ok(e.finish()?)
}

/// `token-ledger archive` — read an archive back the way a Worker would,
/// and say what it cost.
/// What the fold makes of a page of the feed — the difference between "lots of
/// transfers" and a list of trades.
/// The archive reconciled against itself.
///
/// Graded, not asserted: on a partial archive the gap is the supply whose
/// source the walk has not descended to, which is a coverage number worth
/// printing rather than a failure worth hiding.
fn report_supply(a: &mut PolicyArchive) -> Result<()> {
    use policy_archive::supply::{Because, Diagnose, Verdict};

    let completeness = a.manifest.completeness();
    // Named offenders: `inspect` is the diagnostic surface, and a total with
    // no transaction behind it is not actionable.
    let current = a.reconcile_current(Diagnose::PerTransaction)?;
    let r = current.reconciler;
    let balances = r.balances();
    if balances.is_empty() {
        return Ok(());
    }
    let minted: i64 = balances.iter().map(|b| b.minted).sum();
    let moved: i64 = balances.iter().map(|b| b.moved).sum();
    println!(
        "supply      Σ net_mint={minted} Σ amounts={moved} over {} unit(s)",
        balances.len()
    );
    println!(
        "  basis: {}",
        match current.basis {
            SupplyBasis::Snapshot => "certified Mithril history",
            SupplyBasis::Tail => {
                "includes the volatile chain tail — provisional until the next snapshot"
            }
        }
    );
    match r.verdict(completeness) {
        Verdict::Balanced => println!("  reconciled: the archive balances"),
        Verdict::BelowFloor { gap, units } => println!(
            "  {gap} still sourced BELOW THE FLOOR across {units} unit(s) — \
             coverage, not an error; it closes as the walk deepens"
        ),
        Verdict::Failed { offenders, because } => {
            println!("  *** RECONCILIATION FAILED — {} ***", because.as_wire());
            match because {
                Because::CompleteButUnbalanced => println!(
                    "  the archive claims to reach the first mint, so nothing is \
                     left below the floor to explain this"
                ),
                Because::MintedButUnattributed => println!(
                    "  supply was minted that no party row received — coverage \
                     cannot produce this; a mint's recipients are its own outputs"
                ),
            }
            for b in offenders.iter().take(5) {
                println!(
                    "  {:>16}  minted={} moved={} gap={}",
                    unit_label(&b.name),
                    b.minted,
                    b.moved,
                    b.gap()
                );
            }
            report_offending_txs(&r);
        }
    }
    Ok(())
}

/// The transactions behind a failure, so the next step is `archive --tx <hash>`
/// rather than a re-walk.
fn report_offending_txs(r: &policy_archive::Reconciler) {
    let txs = r.offenders();
    if txs.is_empty() {
        return;
    }
    let long: i64 = txs.iter().map(|o| o.gap()).filter(|g| *g > 0).sum();
    let short: i64 = txs.iter().map(|o| o.gap()).filter(|g| *g < 0).sum();
    println!(
        "  {} transaction(s) did not conserve: {long} unsourced, {short} unreceived",
        txs.len()
    );
    for o in txs.iter().take(10) {
        println!(
            "    {} {:>14}  net_mint={} moved={} gap={}",
            hex::encode(&o.tx_hash),
            unit_label(&o.name),
            o.net_mint,
            o.moved,
            o.gap()
        );
    }
}

/// A unit's asset name as text where it is text, hex where it is not.
fn unit_label(name: &[u8]) -> String {
    match std::str::from_utf8(name) {
        Ok(s) if s.chars().all(|c| !c.is_control()) => s.to_string(),
        Ok(_) | Err(_) => hex::encode(name),
    }
}

fn report_trades(a: &mut PolicyArchive, dir: &Path) -> Result<()> {
    use policy_archive::trade::{Event, Party};
    use std::collections::BTreeMap;

    // A page rather than the whole archive: this is a report, and the fold is
    // per-transaction so a sample is representative of the shape.
    let rows = a.feed_rows(2_000, None)?;
    if rows.is_empty() {
        return Ok(());
    }
    let observations = match load_manifest(dir)? {
        Some(m) => read_observations(dir, &m)?,
        None => Vec::new(),
    };
    let roles = venue_roles_for(&observations);
    let folded = policy_archive::trade::fold(&rows, &roles);

    let mut kinds: BTreeMap<&str, usize> = BTreeMap::new();
    let mut named = 0usize;
    let mut unnamed_by_venue = 0usize;
    for f in &folded {
        let e = &f.event;
        let k = match e {
            Event::Fill { party, .. } => {
                match party {
                    Party::Stake(_) | Party::Wallet(_) => named += 1,
                    Party::NotEncodedByVenue => unnamed_by_venue += 1,
                    Party::Ambiguous => {}
                }
                "fill"
            }
            Event::Placement { .. } => "placement",
            Event::Cancellation { .. } => "cancellation",
            Event::BatchedFill { .. } => "batched fill",
            Event::Transfer => "transfer",
        };
        *kinds.entry(k).or_default() += 1;
    }
    let total = folded.len();
    let traded: usize = total - kinds.get("transfer").copied().unwrap_or(0);
    println!(
        "trades      over the newest {total} movements: {traded} are venue activity, \
         {} plain transfers",
        kinds.get("transfer").copied().unwrap_or(0)
    );
    for (k, n) in &kinds {
        if *k != "transfer" {
            println!("  {k:14} {n}");
        }
    }
    if named + unnamed_by_venue > 0 {
        println!(
            "  trader named on {named} fill(s); {unnamed_by_venue} on a venue whose order \
             contract is ONE shared address, so the trader is in the placement leg"
        );
    }
    Ok(())
}

/// Venue roles by payment credential, from `mitos-dex-decode`.
///
/// Built here rather than in `policy-archive`, which is linked by consumers
/// that must not pull in a decode stack — the same seam `mitos_cohort::classify`
/// uses for pools and lock platforms. And built from the DECODE crate rather
/// than `address-registry`, because only the decode crate distinguishes a
/// pool from an order contract: the registry records both as
/// `Exchange { label }`.
pub fn venue_roles() -> policy_archive::trade::Roles {
    use mitos_dex_decode::venue::{self, SiteKey, SiteRole};
    use policy_archive::trade::{OrderKeying, Role, Roles};

    let hex = |b: &[u8; 28]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
    let mut r = Roles::default();

    // ⚠️ FROM THE REGISTRY, never a second hand-written list.
    //
    // This function USED to enumerate the venues itself, and it named four
    // where `mitos-pool-observe::recognise` decodes seven. The observation
    // side and the fold side are two readings of the same contracts, and two
    // lists of the same thing drift.
    //
    // MEASURED on $DONUT: 765 SundaeSwap V3 pool states holding 6,028 ₳, and
    // every one of its swaps classified as a plain transfer, because Sundae's
    // credential was in one list and not the other. `venue::SITES` is now the
    // only list, and `every_recognised_pool_contract_has_a_site` fails the
    // build if a decoder is added without one.
    for site in venue::SITES {
        let role = match site.role {
            SiteRole::Pool => Role::Pool,
            SiteRole::Order => Role::Order,
        };
        let cred = match site.key {
            SiteKey::Cred(c) => Some(hex(&c)),
            // An address-keyed site still registers by CREDENTIAL — the fold
            // matches on the payment part, and deriving it here is what every
            // other consumer must do too.
            SiteKey::Address(a) => policy_archive::trade::address_parts(a).map(|(c, _)| c),
        };
        let Some(cred) = cred else {
            continue;
        };
        // A contract that serves every trader through ONE address cannot name
        // who traded. Declared by the registry rather than assumed here.
        if site.shared {
            r.keying.insert(cred.clone(), OrderKeying::Shared);
        }
        r.register(cred, role, site.venue);
    }

    // The LAUNCHPAD's two contracts. A different crate — snek.fun is not a
    // DEX — and the curve takes `Role::Curve` rather than `Role::Pool`,
    // because its price is not `x·y=k`.
    //
    // ⚠️ Registering the ORDER contract is what turns a wallet's payment into
    // a PLACEMENT and the batcher's spend into a FILL. Without it both read as
    // plain transfers, and $Aliens carried 144 of them as "unclaimed".
    if let Some((cred, _)) =
        policy_archive::trade::address_parts(mitos_launchpad_decode::BONDING_CURVE_ADDR)
    {
        r.register(cred, Role::Curve, venue::SNEK_FUN);
    }
    r.register(
        hex(&mitos_launchpad_decode::ORDER_CRED),
        Role::Order,
        venue::SNEK_FUN,
    );
    // LumpFun's ORDER contract IS shared across every launch — one address for
    // every trader, no stake part — so a movement through it cannot name who
    // placed the order. Its curve is per-launch and is registered by
    // [`venue_roles_for`], from the observations that found it.
    r.register(
        hex(&mitos_launchpad_decode::LUMPFUN_ORDER_CRED),
        Role::Order,
        venue::LUMPFUN_V5,
    );
    r.keying.insert(
        hex(&mitos_launchpad_decode::LUMPFUN_ORDER_CRED),
        OrderKeying::Shared,
    );
    r
}

/// [`venue_roles`], plus the per-launch LumpFun curves these observations
/// found.
///
/// ⚠️ **A per-launch credential cannot live in a static table.** LumpFun
/// parameterises its curve validator with the token, so every pool has its own
/// script hash; the only place a complete set exists is the observation tier,
/// which recognised each curve by its state NFT. Registering them here is what
/// turns a buy's `curve → wallet` movement into a [`policy_archive::trade::Event::Fill`]
/// instead of a plain transfer — the same failure `venue::SITES` was created to
/// end, in the one shape a registry cannot hold.
///
/// The curve takes `Role::Curve`, not `Role::Pool`: a fill against it is a
/// trade, and it is not a DEX pool for any other purpose.
pub fn venue_roles_for(
    observations: &[policy_archive::Observation],
) -> policy_archive::trade::Roles {
    use mitos_dex_decode::venue;
    use policy_archive::trade::Role;

    let mut r = venue_roles();
    for o in observations {
        let Some(d) = &o.decoded else { continue };
        if d.venue != venue::LUMPFUN_V5 {
            continue;
        }
        let Some((cred, _)) = policy_archive::trade::address_parts(&o.address) else {
            continue;
        };
        r.register(cred, Role::Curve, venue::LUMPFUN_V5);
    }
    r
}

/// What the observation tier adds to a policy: who was decoded, what was kept
/// undecoded, and the price the archive can defend at its own tip.
/// Every observation the archive holds, across its passes.
///
/// ⚠️ Read from the pass directories rather than through `PolicyArchive`,
/// which deliberately skips `FileKind::Observations` — a different schema, and
/// `kind_of`'s fallthrough is `Movements`, so handing one to a movements
/// reader is a decode error rather than a skip.
///
/// Shared by the CLI report and the `/policy/{p}/price` route so the two
/// cannot drift into pricing from different row sets.
/// Every observation the archive holds, from wherever the manifest says it is.
///
/// # ⚠️ A NAMED FILE THAT IS MISSING IS AN ERROR, NOT A ZERO
///
/// This used to `continue` past a path that did not exist, and that silence
/// cost the whole interpretive tier: a routine rollup removed the pass
/// directories while the manifest went on naming `pass-0000/observations.
/// parquet`, so `/price` answered `observations: 0`, `/story` carried no pool
/// state, and a token trading on three venues rendered as if it had never been
/// priced. Nothing anywhere said a file was missing.
///
/// A pass that recorded no observations names none, which is the honest zero
/// and still returns nothing. A pass that names one and cannot produce it is
/// a broken archive and now says so.
pub fn read_observations(dir: &Path, m: &Manifest) -> Result<Vec<policy_archive::Observation>> {
    let mut rows: Vec<policy_archive::Observation> = Vec::new();
    for p in &m.passes {
        let Some(rel) = p.observations_path() else {
            continue;
        };
        let path = dir.join(&rel);
        let bytes = std::fs::read(&path).with_context(|| {
            format!(
                "pass {} names {rel} and it is not there — the archive is \
                 incomplete and needs re-walking, not reading",
                p.seq,
            )
        })?;
        rows.extend(policy_archive::read_all(&bytes)?);
    }
    Ok(rows)
}

/// Every observation ABOVE `slot` — the tail half of a carried fold.
///
/// Reads the VOLATILE passes, plus any pass whose ceiling is above `slot`.
/// Everything at or below `slot` is already in the carried series, and an
/// immutable pass normally cannot hold a row above it — `settled_to` is the
/// maximum slot EITHER tier reached when the artefact was written.
///
/// ⚠️ The `ceiling > slot` arm is not decoration. A walk can land new
/// immutable rows above an artefact written before it, and the artefact is
/// only rewritten once that walk completes. Reading volatile passes alone in
/// that window would return a series with no pool state for the new stretch —
/// a price that silently stops moving rather than an error.
///
/// The slot filter stays on both arms as a second check, because being wrong
/// here would double-count a trade rather than fail.
pub fn read_observations_above(
    dir: &Path,
    m: &Manifest,
    slot: u64,
) -> Result<Vec<policy_archive::Observation>> {
    let mut rows: Vec<policy_archive::Observation> = Vec::new();
    for p in &m.passes {
        if p.kind != RangeKind::Volatile && p.ceiling <= slot {
            continue;
        }
        let Some(rel) = p.observations_path() else {
            continue;
        };
        let path = dir.join(&rel);
        let bytes = std::fs::read(&path).with_context(|| {
            format!(
                "pass {} names {rel} and it is not there — the archive is \
                 incomplete and needs re-walking, not reading",
                p.seq,
            )
        })?;
        rows.extend(
            policy_archive::read_all(&bytes)?
                .into_iter()
                .filter(|o| o.slot > slot),
        );
    }
    Ok(rows)
}

fn report_observations(dir: &Path, m: &Manifest) -> Result<()> {
    let rows = read_observations(dir, m)?;
    let bytes: u64 = m
        .passes
        .iter()
        .filter_map(|p| std::fs::metadata(dir.join(&p.dir).join(policy_archive::OBSERVATIONS)).ok())
        .map(|f| f.len())
        .sum();
    if rows.is_empty() {
        return Ok(());
    }

    let decoded = rows.iter().filter(|o| o.decoded.is_some()).count();
    let with_datum = rows.iter().filter(|o| o.datum.is_some()).count();
    println!(
        "observations {} rows, {bytes} bytes — {decoded} decoded, {} kept as \
         UNDECODED CANDIDATES ({with_datum} carry a datum a later decoder can re-read)",
        rows.len(),
        rows.len() - decoded,
    );
    let mut by_venue: BTreeMap<&str, usize> = BTreeMap::new();
    for o in rows.iter().filter_map(|o| o.decoded.as_ref()) {
        *by_venue.entry(o.venue.as_str()).or_default() += 1;
    }
    for (venue, n) in &by_venue {
        println!("  {venue:14} {n} observations");
    }

    // Price at the deepest slot the archive reaches. Piecewise-constant, so
    // this is the last figure the archive can defend rather than an estimate
    // of "now".
    let at = rows.iter().map(|o| o.slot).max().unwrap_or(0);
    // ONE fold, then every projection below reads off it — the price, the
    // depth notes and the per-pool table all describe the same state.
    let view = policy_archive::view::PolicyView::from_observations(&rows);
    let spot = view.spot(at, &policy_archive::view::Projection::default());
    match spot.ada.as_ref() {
        Some(d) => {
            println!(
                "price       {:.8} ADA/unit at slot {at}  ({} ADA pool(s), Σbase {}, \
                 Σquote {} lovelace)",
                d.rate().unwrap_or(0.0) / 1_000_000.0,
                d.pools,
                d.base,
                d.quote
            );
            let floor = policy_archive::price::DEFAULT_FLOOR_LOVELACE;
            if !d.any_pool_above(floor) {
                println!(
                    "  ⚠ NOT ONE contributing pool clears the {} ADA depth floor — the \
                     aggregate is still the right sum, but nothing here is worth quoting \
                     on its own",
                    floor / 1_000_000
                );
            } else if !d.all_pools_above(floor) {
                // ⚠️ A DIFFERENT AND MUCH MILDER STATEMENT, and conflating the
                // two is what made $NIKEPIG — 54,853 ₳ deep on Minswap V2 —
                // report that nothing could be traded at this price.
                println!(
                    "  note: the thinnest contributing pool holds {} lovelace, under the \
                     {} ADA floor; its reserves count toward the sum but its own price \
                     would not be worth quoting",
                    d.thinnest,
                    floor / 1_000_000
                );
            }
        }
        // Undefined, never zero.
        None => println!("price       UNDEFINED at slot {at} — no ADA-paired pool observed"),
    }

    // ⚠️ EVERY CONTRIBUTING POOL, WITH THE SLOT IT WAS LAST SEEN AT.
    //
    // The aggregate above sums each pool's LAST observation, whenever that
    // was, because an observation is only written when the pool's UTxO is
    // touched WHILE HOLDING the asset. A pool whose liquidity was withdrawn
    // stops being observed at the moment BEFORE it emptied — so its final,
    // full reserves sit in the sum for ever, priced at whatever the token was
    // worth then.
    //
    // That is invisible in an aggregate and obvious in this table, which is
    // the only reason it is printed.
    //
    // # ⚠️ READ OFF THE SAME FOLD AS THE PRICE, deliberately
    //
    // This used to run its OWN pass over the rows, and it disagreed with the
    // aggregate above it in four ways at once — it keyed a pool by address and
    // key NAME (dropping the key policy), summed `unit_amount` where the price
    // sums `base_reserve`, broke slot ties the other way round, and marked
    // every pool "counted" that was not aged out, including pools the price
    // never summed because they are on another pricing model or have no
    // measurable far side. A table whose job is to explain the number above it
    // cannot be derived separately from that number.
    let projection = policy_archive::view::Projection::default();
    // ADA pairs AND pairs nothing could name.
    //
    // ⚠️ The unnamed ones are here on purpose. A venue recognised the pool and
    // could not say what it pairs with, which is a finding — and the previous
    // table treated "no pair named" as "paired with ADA" and printed an ADA
    // rate for it. $PERP had a Minswap V2 pool quoted at 0.00015969 ADA that
    // holds no ADA at all. Dropping such a pool would swap a wrong number for
    // a missing one; it is shown, marked, and given no rate.
    let mut per_pool: Vec<_> = view
        .pools()
        .filter(|(_, p)| {
            p.quote_unit
                .as_ref()
                .is_none_or(policy_archive::Unit::is_ada)
        })
        .collect();
    if per_pool.len() > 1 {
        println!(
            "  pools at their LAST sighting  (⌀ aged out · ≠ another pricing model · \
             · nothing summable; blank = counted in the price above)"
        );
        per_pool.sort_by_key(|(k, p)| (std::cmp::Reverse(p.slot), k.address.clone()));
        for (key, pool) in per_pool {
            let behind = at.saturating_sub(pool.slot);
            // ~1 slot per second on Cardano, so days are a fair rendering.
            let age = match behind {
                0..=86_400 => "current".to_string(),
                n => format!("{} days", n / 86_400),
            };
            let counted = match pool.bucket(at, &projection) {
                policy_archive::view::Bucket::Priced => " ",
                policy_archive::view::Bucket::Stale => "⌀",
                policy_archive::view::Bucket::OffModel => "≠",
                policy_archive::view::Bucket::Unusable => "·",
            };
            let quote = pool.quote.unwrap_or(0);
            // A rate ONLY where the far side is known to be ADA. An unnamed
            // pair has a real reserve and no unit to divide by.
            let rate = match (pool.base > 0, pool.quote, &pool.quote_unit) {
                (true, Some(q), Some(_)) => {
                    format!("{:.8} ADA", q as f64 / pool.base as f64 / 1e6)
                }
                (_, _, None) => "PAIR UNNAMED".to_string(),
                _ => "— ADA".to_string(),
            };
            println!(
                "  {counted} {:<14} {:>16} base {:>15} lovelace  {rate:>14}  {age:>9}  {}",
                pool.venue,
                pool.base,
                quote,
                &key.address[..key.address.len().min(24)],
            );
        }
    }
    // ⚠️ REAL WHEN WRITTEN, POSSIBLY WITHDRAWN SINCE. Reported so the reader
    // sees liquidity the price deliberately does not count.
    for u in &spot.stale {
        println!(
            "  STALE       {} base / {} quote across {} pool(s) — last seen over {} days \
             before the price slot, so not counted: a pool nobody has arbitraged in a \
             month is either empty or not at market",
            u.base,
            u.quote,
            u.pools,
            policy_archive::price::DEFAULT_STALE_AFTER_SLOTS / 86_400,
        );
    }
    for u in &spot.unresolved {
        println!(
            "  UNRESOLVED  paired with {}.{} — Σquote {} across {} pool(s); pricing it \
             needs that unit's OWN archive",
            hex::encode(&u.quote_unit.policy),
            String::from_utf8_lossy(&u.quote_unit.name),
            u.quote,
            u.pools
        );
    }
    // Real reserves under a model this crate will not evaluate. Reported so
    // the liquidity is visible without being priced — the distinction that
    // halved $PERP's price when it was missing.
    for u in &spot.unpriceable {
        println!(
            "  OFF-MODEL   {} base / {} quote across {} venue(s) — held, but not \
             constant-product, so deliberately absent from the price above",
            u.base, u.quote, u.pools
        );
    }
    Ok(())
}

pub fn inspect(args: InspectArgs) -> Result<()> {
    let dir = policy_dir(&args.archive_dir, &args.policy.to_lowercase());
    let Some(mut a) = PolicyArchive::open(&dir)? else {
        println!("no archive at {}", dir.display());
        return Ok(());
    };
    let m = &a.manifest;
    println!("policy      {}", m.policy);
    println!("completeness {}", m.completeness);
    println!(
        "coverage    {:?} .. {:?}  ({} passes)",
        m.walk_from,
        m.walk_to,
        m.passes.len()
    );
    if let Some(p) = &m.profile {
        // What the archive DECIDED this policy is — and therefore whether
        // undecoded candidates were kept at all. Without it, "no candidates"
        // and "we did not keep any" are indistinguishable.
        println!(
            "profile     {} — {} unit(s): {} held in quantity, {} only ever singly",
            p.class().as_str(),
            p.units_seen,
            p.fungible_units,
            p.single_units
        );
        if p.class() == policy_archive::Class::Collection {
            println!(
                "  candidates NOT kept: every script output holds one, so they are \
                 marketplace escrows — market-ledger's to interpret, not this archive's"
            );
        }
    }
    if let Some(r) = &m.rollup {
        println!(
            "rollup      {} rows={} units={} through pass {:?}",
            r.file, r.rows, r.units, m.rolled_up_through
        );
    }
    for p in &m.passes {
        println!(
            "  {} [{}, {}) found={} written={} backfilled={} pending={} units={} {:.1}s{}",
            p.dir,
            p.floor,
            p.ceiling,
            p.found,
            p.written,
            p.backfilled,
            p.pending,
            p.units,
            p.secs,
            p.corrections
                .as_ref()
                .map(|c| format!(" corrections={}", c.rows))
                .unwrap_or_default()
        );
        if !p.segments.is_empty() {
            println!(
                "    uncompacted: {} segments, {} rows",
                p.segments.len(),
                p.segments.iter().map(|s| s.rows).sum::<u64>()
            );
        }
    }
    let cov = a.coverage();
    println!(
        "rows        txs={} first_slot={:?} last_slot={:?} unresolved={}",
        cov.total_txs, cov.first_slot, cov.last_slot, cov.unresolved
    );
    let (reqs, bytes) = a.fetched();
    println!("footers     {reqs} requests, {bytes} bytes");

    report_observations(&dir, m)?;
    report_supply(&mut a)?;
    report_trades(&mut a, &dir)?;

    let density = a.density(86_400);
    println!("density     {} daily buckets", density.len());
    if args.density {
        for b in &density {
            println!(
                "  {}  txs={} mv={} mint={} burn={}",
                crate::reverse::unix_date(b.from_unix),
                b.txs,
                b.movements,
                b.mints,
                b.burns
            );
        }
    } else {
        let mut busiest: Vec<&DensityBucket> = density.iter().collect();
        busiest.sort_by_key(|b| std::cmp::Reverse(b.txs));
        for b in busiest.iter().take(5) {
            println!(
                "  busiest {}  txs={} mv={} mint={} burn={}",
                crate::reverse::unix_date(b.from_unix),
                b.txs,
                b.movements,
                b.mints,
                b.burns
            );
        }
    }

    if let Some(limit) = args.limit {
        let page = a.feed_rows(limit, None)?;
        println!("feed        {} rows (newest first)", page.len());
        for row in &page {
            print_row(row);
        }
        let (r2, b2) = a.fetched();
        println!(
            "            +{} requests, +{} bytes for the page",
            r2 - reqs,
            b2 - bytes
        );
    }
    if let Some(tx) = &args.tx {
        let raw = hex::decode(tx).context("tx hash hex")?;
        let (r0, b0) = a.fetched();
        match a.feed_row_at(&raw)? {
            Some(row) => print_row(&row),
            None => println!("tx {tx}: not in this archive"),
        }
        let (r1, b1) = a.fetched();
        println!(
            "            +{} requests, +{} bytes for the lookup",
            r1 - r0,
            b1 - b0
        );
    }
    Ok(())
}

fn print_row(row: &FeedRow) {
    println!("  {} slot {}", hex::encode(&row.tx_hash), row.slot);
    for u in &row.units {
        println!(
            "    unit {} net_mint {}",
            String::from_utf8_lossy(&u.name),
            u.net_mint
        );
        for p in &u.parties {
            println!("      {:+} {}", p.amount, p.address);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A wallet address under `stake`, with an arbitrary key payment part.
    fn wallet_under(stake: &str) -> String {
        use pallas_addresses::{
            Address, Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart,
            StakePayload,
        };
        let Ok(Address::Stake(s)) = Address::from_bech32(stake) else {
            panic!("{stake} is a stake address");
        };
        let StakePayload::Stake(hash) = s.payload() else {
            panic!("{stake} is a key stake credential");
        };
        let payment = ShelleyPaymentPart::key_hash([7u8; 28].into());
        Address::Shelley(ShelleyAddress::new(
            Network::Mainnet,
            payment,
            ShelleyDelegationPart::key_hash(*hash),
        ))
        .to_bech32()
        .expect("encodes")
    }

    /// WHO stays out of the summary. `$perpstaking` is named by the stake
    /// table as PERP staking, and the wallet once listed as Wayup is a
    /// person's; the summary stores both as what their addresses are, a
    /// wallet, with no label, and a screen applies the published table.
    #[test]
    fn a_wallet_run_service_is_stored_as_what_its_address_is() {
        for stake in [
            "stake1uxw8pmwq7my926m9xuk5zpy83at8hyzqla4qxs843sfjznsgtwgxy",
            "stake1uy50zl7a9k9c74v66c0gn833at5sh83qnjldk8hg4rrv05g3mmskr",
            "stake1uxwcw3rr36r8q9e5egy3e74cmazpjvx4t5ajsq50xhm568celda4g",
        ] {
            assert_eq!(
                classify_holder(&wallet_under(stake)),
                (policy_archive::summary::Role::Person, None),
                "{stake}"
            );
        }
    }

    fn mv(tx: u8, unit: impl AsRef<[u8]>, addr: &str, amount: i64, net_mint: i64) -> Movement {
        Movement {
            slot: 100 + tx as u64,
            block_time: 1_700_000_000 + tx as u64,
            tx_hash: vec![tx; 32],
            unit_name: unit.as_ref().to_vec(),
            address: addr.to_string(),
            amount,
            net_mint,
            tx_index: tx as u32,
        }
    }

    fn stamp_for(policy: &str) -> policy_archive::Stamp {
        policy_archive::Stamp {
            policy_hex: policy.to_string(),
            completeness: Completeness::Complete,
            walk_from: Some(0),
            walk_to: Some(1_000),
            covered_from: 0,
            covered_to: 999,
            sealed_unix: 0,
        }
    }

    /// A pass entry for the separation test — only `kind`, `floor` and
    /// `ceiling` are read by `volatile_is_separate`.
    fn pass(seq: u32, kind: RangeKind, floor: u64, ceiling: u64) -> PassEntry {
        PassEntry {
            seq,
            dir: PassEntry::dir_name(seq),
            ceiling,
            floor,
            windows: Vec::new(),
            kind,
            rolled_up: false,
            movements: None,
            corrections: None,
            observations: None,
            segments: Vec::new(),
            pending: 0,
            found: 0,
            written: 0,
            backfilled: 0,
            units: 0,
            secs: 0.0,
            written_unix: 0,
        }
    }

    /// A tail TRANSFER is counted into `moved` but does not make the supply
    /// figure provisional: it changes no mint. A live token with recent trades
    /// and no new mint therefore stays on the ordinary `snapshot` answer.
    #[test]
    fn a_disjoint_volatile_transfer_is_counted_without_marking_supply() {
        let dir = tempfile::tempdir().unwrap();
        let stamp = stamp_for("aa");
        let immutable = vec![mv(1, "A", "alice", 10, 10)];
        let tail = vec![mv(2, "A", "alice", -10, 0), mv(2, "A", "bob", 10, 0)];
        let mv_path = dir.path().join("movements.parquet");
        let tail_path = dir.path().join("tail.parquet");
        crate::segments::write_file(&mv_path, &stamp, immutable).unwrap();
        crate::segments::write_file(&tail_path, &stamp, tail).unwrap();

        let mut manifest = Manifest::new("aa");
        manifest.passes.push(pass(0, RangeKind::Immutable, 0, 200));
        manifest.passes.push(pass(1, RangeKind::Volatile, 200, 400));
        let mut archive = PolicyArchive {
            manifest,
            files: vec![
                OpenFile::open(&mv_path, FileKind::Movements, RangeKind::Immutable).unwrap(),
                OpenFile::open(&tail_path, FileKind::Movements, RangeKind::Volatile).unwrap(),
            ],
        };
        let current = archive
            .reconcile_current(policy_archive::Diagnose::Totals)
            .unwrap();
        assert_eq!(
            current.basis,
            SupplyBasis::Snapshot,
            "a tail transfer does not move supply"
        );
        assert_eq!(current.reconciler.balances()[0].minted, 10);
        assert_eq!(
            current.reconciler.verdict(Completeness::Complete),
            policy_archive::Verdict::Balanced,
            "the tail continues the immutable history; it does not duplicate it"
        );

        // With no recorded coverage, separation cannot be proven, so the tail is
        // left out. That is the conservative default, and the reason the
        // existing overlap test keeps passing.
        let mut blind = PolicyArchive {
            manifest: Manifest::new("aa"),
            files: vec![
                OpenFile::open(&mv_path, FileKind::Movements, RangeKind::Immutable).unwrap(),
                OpenFile::open(&tail_path, FileKind::Movements, RangeKind::Volatile).unwrap(),
            ],
        };
        assert_eq!(
            blind
                .reconcile_current(policy_archive::Diagnose::Totals)
                .unwrap()
                .basis,
            SupplyBasis::Snapshot
        );
    }

    /// A token launched after the last Mithril snapshot has its MINT in the
    /// tail. The immutable reconcile reports zero for it — the reader's answer
    /// must include the tail and say the supply is not yet certified.
    #[test]
    fn a_tail_mint_marks_the_supply_as_tail() {
        let dir = tempfile::tempdir().unwrap();
        let stamp = stamp_for("aa");
        let immutable = vec![mv(1, "A", "alice", 10, 10)];
        let tail = vec![mv(2, "A", "bob", 5, 5)];
        let mv_path = dir.path().join("movements.parquet");
        let tail_path = dir.path().join("tail.parquet");
        crate::segments::write_file(&mv_path, &stamp, immutable).unwrap();
        crate::segments::write_file(&tail_path, &stamp, tail).unwrap();

        let mut manifest = Manifest::new("aa");
        manifest.passes.push(pass(0, RangeKind::Immutable, 0, 200));
        manifest.passes.push(pass(1, RangeKind::Volatile, 200, 400));
        let mut archive = PolicyArchive {
            manifest,
            files: vec![
                OpenFile::open(&mv_path, FileKind::Movements, RangeKind::Immutable).unwrap(),
                OpenFile::open(&tail_path, FileKind::Movements, RangeKind::Volatile).unwrap(),
            ],
        };
        let current = archive
            .reconcile_current(policy_archive::Diagnose::Totals)
            .unwrap();
        assert_eq!(current.basis, SupplyBasis::Tail);
        assert_eq!(current.reconciler.balances()[0].minted, 15);
        assert_eq!(
            current.reconciler.verdict(Completeness::Complete),
            policy_archive::Verdict::Balanced
        );
    }

    /// A launchpad pool's state NFT shares the token's policy. Before this
    /// filter, $BUNNY's 1,000,000,000 mint reconciled as 1,000,000,002 — the
    /// state NFT and one other CIP-67 unit were counted as supply.
    #[test]
    fn plumbing_units_are_not_part_of_supply() {
        let dir = tempfile::tempdir().unwrap();
        let stamp = stamp_for("aa");
        let mut nft = vec![0x00, 0x06, 0x43, 0xb0];
        nft.extend_from_slice(b"POOL");
        let rows = vec![
            mv(1, "BUNNY", "alice", 1_000_000_000, 1_000_000_000),
            mv(1, nft.as_slice(), "pool", 1, 1),
        ];
        let path = dir.path().join("movements.parquet");
        crate::segments::write_file(&path, &stamp, rows).unwrap();
        let mut archive = PolicyArchive {
            manifest: Manifest::new("aa"),
            files: vec![OpenFile::open(&path, FileKind::Movements, RangeKind::Immutable).unwrap()],
        };
        let current = archive
            .reconcile_current(policy_archive::Diagnose::Totals)
            .unwrap();
        let balances = current.reconciler.balances();
        assert_eq!(balances.len(), 1, "the state NFT is not a user unit");
        assert_eq!(balances[0].minted, 1_000_000_000);
    }

    /// ⚠️ THE FALSE ALARM THIS GUARD EXISTS FOR.
    ///
    /// The volatile tail is REPLACED WHOLE on every refresh, and a top-up walk
    /// that climbs into a stretch the tail also covers must write its own rows
    /// anyway. So for a window, the same transaction legitimately has rows in
    /// an immutable file AND in the tail.
    ///
    /// Totalling across both double-counts AMOUNTS while counting `net_mint`
    /// once per `(tx, unit)` — which reads as `moved > minted`: a positive gap,
    /// indistinguishable in shape from a real fault. MEASURED 2026-09-09 on the
    /// live box: RECONCILIATION FAILED on 7 of 23 policies whose archives were,
    /// on inspection, perfectly balanced.
    ///
    /// A check that cries wolf gets muted, and then it is not there for the
    /// real one.
    #[test]
    fn the_volatile_tail_is_excluded_from_the_supply_total() {
        let dir = tempfile::tempdir().unwrap();
        let stamp = stamp_for("aa");

        // A mint of 10 to alice, then alice → bob, both in the immutable file.
        let immutable = vec![
            mv(1, "A", "alice", 10, 10),
            mv(2, "A", "alice", -10, 0),
            mv(2, "A", "bob", 10, 0),
        ];
        // The SAME transaction 2, as the TAIL saw it: the arrival at bob, with
        // the source leg absent because the tail records what it read and the
        // resolution of alice's spend lives in the immutable file.
        //
        // ⚠️ It has to be the UNMATCHED leg to reproduce the fault. A doubled
        // transfer nets to nothing and hides the bug — the first draft of this
        // test duplicated both legs and passed for the wrong reason.
        let tail = vec![mv(2, "A", "bob", 10, 0)];

        let mv_path = dir.path().join("movements.parquet");
        let tail_path = dir.path().join("tail.parquet");
        crate::segments::write_file(&mv_path, &stamp, immutable).unwrap();
        crate::segments::write_file(&tail_path, &stamp, tail).unwrap();

        let both = PolicyArchive {
            manifest: Manifest::new("aa"),
            files: vec![
                OpenFile::open(&mv_path, FileKind::Movements, RangeKind::Immutable).unwrap(),
                OpenFile::open(&tail_path, FileKind::Movements, RangeKind::Volatile).unwrap(),
            ],
        };
        let mut both = both;
        let r = both.reconcile(policy_archive::Diagnose::Totals).unwrap();
        assert_eq!(
            r.verdict(Completeness::Complete),
            policy_archive::Verdict::Balanced,
            "the tail's duplicate of tx 2 must not be summed into the total"
        );

        // And the guard must be load-bearing: tagging the tail IMMUTABLE — as
        // the pre-fix reader effectively did — reproduces the false alarm.
        let mut mistagged = PolicyArchive {
            manifest: Manifest::new("aa"),
            files: vec![
                OpenFile::open(&mv_path, FileKind::Movements, RangeKind::Immutable).unwrap(),
                OpenFile::open(&tail_path, FileKind::Movements, RangeKind::Immutable).unwrap(),
            ],
        };
        let bad = mistagged
            .reconcile(policy_archive::Diagnose::Totals)
            .unwrap();
        assert_ne!(
            bad.verdict(Completeness::Complete),
            policy_archive::Verdict::Balanced,
            "if this passes, the test is not exercising the guard"
        );
    }

    /// The sum rule: an output (+1) and its later-resolved source (−1) for
    /// the SAME party net to nothing and the party disappears — change
    /// returned to the sender is not a movement. Two different parties are
    /// a transfer.
    #[test]
    fn a_party_appearing_on_both_sides_nets_out() {
        let rows = vec![
            mv(1, "A", "alice", 1, 0),
            mv(1, "A", "alice", -1, 0),
            mv(1, "B", "bob", 1, 0),
            mv(1, "B", "alice", -1, 0),
        ];
        let out = fold_rows(rows);
        assert_eq!(out.len(), 1);
        assert!(
            !out[0].units.iter().any(|u| u.name == b"A"),
            "alice's change is not a movement, so A is not on the row"
        );
        let b = out[0].units.iter().find(|u| u.name == b"B").unwrap();
        assert_eq!(b.parties.len(), 2);
    }

    /// A transaction whose every unit netted to nothing — the asset went in
    /// and came back out as change — is not on the feed at all.
    #[test]
    fn a_transaction_that_moved_nothing_is_not_a_row() {
        let rows = vec![mv(1, "A", "alice", 1, 0), mv(1, "A", "alice", -1, 0)];
        assert!(fold_rows(rows).is_empty());
        // …but one unit netting out does not take a sibling that moved.
        let rows = vec![
            mv(2, "A", "alice", 1, 0),
            mv(2, "A", "alice", -1, 0),
            mv(2, "B", "bob", 1, 0),
        ];
        let out = fold_rows(rows);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].units.len(), 1);
        assert_eq!(out[0].units[0].name, b"B");
    }

    /// A placeholder keeps the unit on the row with its mint and no party.
    #[test]
    fn a_placeholder_keeps_the_unit_without_a_party() {
        let mut p = mv(2, "A", "", 0, -1);
        p.address.clear();
        let out = fold_rows(vec![p]);
        assert_eq!(out[0].units[0].net_mint, -1);
        assert!(out[0].units[0].parties.is_empty());
    }

    /// Live rows bucket the way a footer would: transactions distinct per
    /// day, mints and burns per transaction, placeholders not movements.
    #[test]
    fn density_of_rows_counts_like_a_footer() {
        let mut burn = mv(3, "C", "", 0, -1);
        burn.address.clear();
        let rows = vec![
            mv(1, "A", "alice", 1, 1),
            mv(1, "A", "alice", 1, 1),
            mv(2, "B", "bob", 1, 0),
            burn,
        ];
        let d = density_of(&rows, 86_400);
        assert_eq!(d.len(), 1);
        assert_eq!(
            (d[0].txs, d[0].mints, d[0].burns, d[0].movements),
            (3, 1, 1, 3)
        );
        let merged = merge_density(d.clone(), d);
        assert_eq!(merged[0].txs, 6);
    }

    #[test]
    fn rows_come_back_newest_first() {
        let out = fold_rows(vec![
            mv(1, "A", "a", 1, 0),
            mv(3, "A", "a", 1, 0),
            mv(2, "A", "a", 1, 0),
        ]);
        let slots: Vec<u64> = out.iter().map(|r| r.slot).collect();
        assert_eq!(slots, vec![103, 102, 101]);
    }

    #[test]
    fn the_manifest_round_trips_and_names_its_passes() {
        let mut m = Manifest::new("ab");
        m.passes.push(PassEntry {
            seq: 0,
            dir: PassEntry::dir_name(0),
            ceiling: 10,
            floor: 5,
            windows: Vec::new(),
            kind: RangeKind::Immutable,
            rolled_up: false,
            movements: None,
            corrections: None,
            observations: None,
            segments: Vec::new(),
            pending: 0,
            found: 0,
            written: 0,
            backfilled: 0,
            units: 0,
            secs: 0.0,
            written_unix: 0,
        });
        assert_eq!(m.next_seq(), 1);
        assert_eq!(m.passes[0].dir, "pass-0000");
        let back: Manifest = serde_json::from_slice(&serde_json::to_vec(&m).unwrap()).unwrap();
        assert_eq!(back, m);
        // DERIVED now: a pass over [5, 10) is a record, so this is a partial
        // ledger rather than an unrecorded one. `Unrecorded` is what a
        // manifest with no ranges at all says.
        assert_eq!(back.completeness(), Completeness::Partial);
        assert_eq!(back.ranges(), vec![SlotRange::new(5, 10)]);
    }

    /// A sidecar from before `settled` existed still loads — postcard is
    /// not self-describing, so the old shape is tried second.
    #[test]
    fn an_old_pending_file_still_loads() {
        let dir = std::env::temp_dir().join(format!("tl-pending-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(PENDING);
        // The 1-byte file every complete archive on the box carries: an
        // empty spender list, nothing after it.
        std::fs::write(&path, [0u8]).unwrap();
        let f = load_pending(&path).unwrap();
        assert!(f.spenders.is_empty() && f.settled.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Jobs land out of order, so the carried state is the MERGE of every
    /// sidecar: the latest-landed copy per spender, minus anything a job
    /// reported settled.
    #[test]
    fn the_pending_union_takes_the_latest_copy_and_drops_the_settled() {
        let dir = std::env::temp_dir().join(format!("tl-union-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let spender = |h: u8, outrefs: usize| PendingSpender {
            tx_hash: [h; 32],
            slot: 1,
            block_time: 2,
            missing: vec![(b"A".to_vec(), 1)],
            net_mint: vec![],
            outrefs: (0..outrefs as u32).map(|i| ([h; 32], i)).collect(),
        };
        let mut m = Manifest::new("ab");
        for (seq, unix) in [(0u32, 100u64), (1, 200)] {
            let pass_dir = dir.join(PassEntry::dir_name(seq));
            std::fs::create_dir_all(&pass_dir).unwrap();
            m.passes.push(PassEntry {
                seq,
                dir: PassEntry::dir_name(seq),
                ceiling: 10,
                floor: 5,
                windows: Vec::new(),
                kind: RangeKind::Immutable,
                rolled_up: false,
                movements: None,
                corrections: None,
                observations: None,
                segments: Vec::new(),
                pending: 0,
                found: 0,
                written: 0,
                backfilled: 0,
                units: 0,
                secs: 0.0,
                written_unix: unix,
            });
        }
        // Pass 0 (older): spenders 1 (three outrefs) and 2. Pass 1 (newer):
        // spender 1 with one outref left, and it settled spender 2.
        store_pending(
            &dir.join("pass-0000").join(PENDING),
            &PendingFile {
                spenders: vec![spender(1, 3), spender(2, 2)],
                settled: Vec::new(),
            },
        )
        .unwrap();
        store_pending(
            &dir.join("pass-0001").join(PENDING),
            &PendingFile {
                spenders: vec![spender(1, 1), spender(3, 1)],
                settled: vec![[2; 32]],
            },
        )
        .unwrap();
        let union = load_pending_union(&dir, &m).unwrap();
        let mut hashes: Vec<u8> = union.spenders.iter().map(|s| s.tx_hash[0]).collect();
        hashes.sort_unstable();
        assert_eq!(hashes, vec![1, 3], "2 was settled; 1 and 3 remain");
        let one = union.spenders.iter().find(|s| s.tx_hash[0] == 1).unwrap();
        assert_eq!(one.outrefs.len(), 1, "the newer copy of spender 1 wins");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_pending_file_round_trips() {
        let p = PendingFile {
            spenders: vec![PendingSpender {
                tx_hash: [7; 32],
                slot: 1,
                block_time: 2,
                missing: vec![(b"A".to_vec(), 3)],
                net_mint: vec![],
                outrefs: vec![([1; 32], 0), ([2; 32], 5)],
            }],
            settled: vec![[9; 32]],
        };
        let bytes = postcard::to_stdvec(&p).unwrap();
        let back: PendingFile = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back.spenders, p.spenders);
    }

    /// ⚠️ THE $DONUT TEST, at the level where both halves are visible.
    ///
    /// `venue::SITES` is the registry and this is its only consumer for the
    /// fold, so what matters here is that the translation LOSES NOTHING: every
    /// site must land in `Roles`, under its own venue name, with the shared
    /// flag intact.
    ///
    /// It was a hand-written list naming four venues while
    /// `mitos-pool-observe` decoded seven, and $DONUT rendered as an untraded
    /// token with 765 SundaeSwap pool sightings against zero fills.
    #[test]
    fn every_site_reaches_the_trade_fold() {
        use mitos_dex_decode::venue::{self, SiteKey, SiteRole};
        let roles = venue_roles();
        let hex = |b: &[u8; 28]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();

        for site in venue::SITES {
            let cred = match site.key {
                SiteKey::Cred(c) => hex(&c),
                SiteKey::Address(a) => {
                    policy_archive::trade::address_parts(a)
                        .expect("a site address must parse")
                        .0
                }
            };
            let got = roles.role_of(&cred);
            assert_eq!(
                got,
                Some(match site.role {
                    SiteRole::Pool => policy_archive::trade::Role::Pool,
                    SiteRole::Order => policy_archive::trade::Role::Order,
                }),
                "{} ({:?}) did not reach the fold",
                site.venue,
                site.role,
            );
            assert_eq!(
                roles.name_of(&cred),
                Some(site.venue),
                "{}'s credential joins under the wrong name — a pool state and \
                 a fill would then describe the same venue and never join",
                site.venue,
            );
        }

        // The four that were missing, named explicitly: a regression here is
        // the exact defect, and a count assertion would not say which.
        for v in [
            venue::SUNDAE_V3,
            venue::SUNDAE_V1,
            venue::WINGRIDERS_V2,
            venue::WINGRIDERS_V1,
        ] {
            assert!(
                venue::SITES.iter().any(|s| s.venue == v),
                "{v} is decoded by the observer and must be foldable",
            );
        }

        // The launchpad is NOT a DEX site — its own crate, and a `Curve` role,
        // because its price is not `x·y=k`. BOTH its contracts must land.
        let curve =
            policy_archive::trade::address_parts(mitos_launchpad_decode::BONDING_CURVE_ADDR)
                .unwrap()
                .0;
        assert_eq!(
            roles.role_of(&curve),
            Some(policy_archive::trade::Role::Curve)
        );
        assert_eq!(roles.name_of(&curve), Some(venue::SNEK_FUN));

        // ⚠️ THE CURVE'S ADDRESS AND ITS CREDENTIAL MUST AGREE. The crate
        // offers both, for consumers that match either way, and two spellings
        // of one contract is precisely how a venue's halves stop joining.
        // Asserted here because this is where a bech32 decoder is in scope.
        assert_eq!(
            curve,
            hex(&mitos_launchpad_decode::BONDING_CURVE_CRED),
            "the curve address does not carry the curve credential",
        );

        // ⚠️ The ORDER contract is what turns a wallet's payment into a
        // PLACEMENT and the batcher's spend into a FILL. Without it $Aliens
        // carried 144 of them as "unclaimed".
        let order = hex(&mitos_launchpad_decode::ORDER_CRED);
        assert_eq!(
            roles.role_of(&order),
            Some(policy_archive::trade::Role::Order)
        );
        assert_eq!(roles.name_of(&order), Some(venue::SNEK_FUN));
        assert_ne!(order, curve, "the order is not the curve");
    }

    /// ⚠️ A LumpFun curve is parameterised per launch, so its credential cannot
    /// be in the static table. If the observation tier's discoveries are not
    /// folded into the roles, every buy of the token reads as a plain transfer
    /// — the $DONUT failure, in the one shape a registry cannot hold.
    #[test]
    fn a_discovered_lumpfun_curve_turns_a_curve_payment_into_a_fill() {
        use policy_archive::Observation;
        use policy_archive::feed::{FeedRow, PartyMove, UnitMove};
        use policy_archive::observation::Decoded;
        use policy_archive::trade::{Event, Party, Role};

        // The Dust Bunny pool and the wallet that received the sample buy's
        // tokens (`1f7e8e57…`).
        const POOL: &str = "addr1wyj2cxrm7pkfz0sx6j6lgueg8035yrs4ry9xupcvcxlrg6cywmqny";
        const BUYER: &str = "addr1q99qpegypskhugq6nss8gjkwvjlj35wd54venfynreqxjgkt55dzuk6msq4qgq4g0dmz6tkvdvdfkmgd6uyd4urms2ksl0yc60";

        let obs = Observation {
            slot: 201_233_716,
            block_time: 1_792_800_007,
            tx_hash: vec![0x1f; 32],
            address: POOL.into(),
            lovelace: 1_562_000_000,
            unit_name: b"BUNNY".to_vec(),
            unit_amount: 733_043_488,
            datum: None,
            decoded: Some(Decoded {
                venue: mitos_dex_decode::venue::LUMPFUN_V5.to_string(),
                key_policy: hex::decode("a1b085f414fb7799d378e90e39d24c9e54c841a311d72b2f57cba293")
                    .unwrap(),
                key_name: mitos_launchpad_decode::LUMPFUN_STATE_NFT_NAME.to_vec(),
                key_basis: "datum".into(),
                base_reserve: 858_381_090,
                quote_policy: Some(Vec::new()),
                quote_name: Some(Vec::new()),
                quote_reserve: Some(1_555_000_000 + 5_000_000_130),
                fee_bps: None,
                total_lp: None,
                reserve_source: "datum-plus-virtual".into(),
                pricing: policy_archive::observation::pricing::CONSTANT_PRODUCT_VIRTUAL.into(),
            }),
        };

        let roles = venue_roles_for(std::slice::from_ref(&obs));
        let (curve, _) = policy_archive::trade::address_parts(POOL).unwrap();
        assert_eq!(roles.role_of(&curve), Some(Role::Curve));
        assert_eq!(
            roles.name_of(&curve),
            Some(mitos_dex_decode::venue::LUMPFUN_V5)
        );

        // A buy: the tokens leave the curve into the buyer's wallet.
        let rows = vec![FeedRow {
            tx_hash: vec![0x1f; 32],
            slot: 201_233_716,
            block_time: 1_792_800_007,
            units: vec![UnitMove {
                name: b"BUNNY".to_vec(),
                net_mint: 0,
                parties: vec![
                    PartyMove {
                        address: POOL.into(),
                        amount: -27_014_353,
                    },
                    PartyMove {
                        address: BUYER.into(),
                        amount: 27_014_353,
                    },
                ],
            }],
        }];
        let folded = policy_archive::trade::fold(&rows, &roles);
        assert_eq!(folded.len(), 1);
        match &folded[0].event {
            Event::Fill {
                venue_cred,
                party,
                amount,
                into_pool,
            } => {
                assert_eq!(venue_cred, &curve);
                assert_eq!(party, &Party::Wallet(BUYER.into()));
                assert_eq!(*amount, 27_014_353);
                assert!(!into_pool, "a buy takes the asset OUT of the curve");
            }
            other => panic!("expected a Fill, got {other:?}"),
        }

        // And the static table alone cannot know it — the discovery is what did.
        assert!(venue_roles().role_of(&curve).is_none());
    }
}
