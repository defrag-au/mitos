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

use crate::walk::stake_of;

/// One transaction, as a feed reads it.
pub struct FeedRow {
    pub tx_hash: Vec<u8>,
    pub slot: u64,
    pub block_time: u64,
    /// One entry per unit of the policy that this transaction touched.
    pub units: Vec<UnitMove>,
}

/// One unit's movement within one transaction.
pub struct UnitMove {
    /// On-chain asset-name bytes.
    pub name: Vec<u8>,
    /// Net mint for this unit here: 0 transfer, positive mint, negative burn.
    pub net_mint: i64,
    /// Every party whose balance of this unit changed.
    ///
    /// The PRIMITIVE, carried as-is. Direction is derived from it rather than
    /// stored, and the derivation has to be able to decline — a batched
    /// marketplace fill has several losers and several gainers, and the obvious
    /// "biggest is the sender" rule is wrong exactly there.
    pub parties: Vec<PartyMove>,
}

/// One party's signed movement of one unit.
pub struct PartyMove {
    pub address: String,
    pub stake: Option<String>,
    pub amount: i64,
}

pub const MANIFEST: &str = "manifest.json";
pub const MOVEMENTS: &str = "movements.parquet";
pub const CORRECTIONS: &str = "corrections.parquet";
pub const PENDING: &str = "pending.bin";
pub const MANIFEST_FORMAT: u32 = 1;

/// The archive's own record of itself.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub format: u32,
    pub policy: String,
    /// The registered first-mint floor, if the registry knew one when a pass
    /// ran. The bound below which no pass will look.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_mint_slot: Option<u64>,
    /// `complete` | `partial` | `unrecorded` — the feed's own words.
    pub completeness: String,
    /// Lowest slot covered, across passes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub walk_from: Option<u64>,
    /// Highest slot covered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub walk_to: Option<u64>,
    pub passes: Vec<PassEntry>,
    pub updated_unix: u64,
}

impl Manifest {
    pub fn new(policy_hex: &str) -> Self {
        Manifest {
            format: MANIFEST_FORMAT,
            policy: policy_hex.to_string(),
            first_mint_slot: None,
            completeness: Completeness::Unrecorded.as_wire().to_string(),
            walk_from: None,
            walk_to: None,
            passes: Vec::new(),
            updated_unix: 0,
        }
    }

    pub fn completeness(&self) -> Completeness {
        Completeness::from_wire(&self.completeness).unwrap_or(Completeness::Unrecorded)
    }

    /// The newest pass — the one whose pending set is current.
    pub fn latest_pass(&self) -> Option<&PassEntry> {
        self.passes.iter().max_by_key(|p| p.seq)
    }

    pub fn next_seq(&self) -> u32 {
        self.latest_pass().map_or(0, |p| p.seq + 1)
    }
}

/// One pass, as the manifest records it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PassEntry {
    pub seq: u32,
    /// Directory under the policy's, e.g. `pass-0003`.
    pub dir: String,
    /// The range this pass walked: `[floor, ceiling)`.
    pub ceiling: u64,
    pub floor: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub movements: Option<FileEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub corrections: Option<FileEntry>,
    /// Spenders still waiting on a source after this pass.
    pub pending: u64,
    /// Transactions the walk FOUND touching the policy in the pass's range.
    #[serde(default)]
    pub found: u64,
    /// Transactions with rows in `movements` — fewer than `found`, because
    /// a transaction the asset only rode through as change moved nothing.
    pub written: u64,
    /// Delta rows resolved for rows written earlier — here or by an earlier
    /// pass.
    pub backfilled: u64,
    /// Distinct units seen in this pass. A lower bound on the policy's, since
    /// a window sees only what moved in it.
    pub units: u64,
    pub secs: f64,
    pub written_unix: u64,
}

impl PassEntry {
    pub fn dir_name(seq: u32) -> String {
        format!("pass-{seq:04}")
    }
}

/// One Parquet file, as the manifest records it — enough to decide whether
/// to open it without opening it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FileEntry {
    pub file: String,
    pub rows: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_slot: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_slot: Option<u64>,
}

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
    let m: Manifest =
        serde_json::from_slice(&raw).with_context(|| format!("parsing {}", path.display()))?;
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
    std::fs::write(&tmp, serde_json::to_vec_pretty(m)?)?;
    std::fs::rename(&tmp, dir.join(MANIFEST))?;
    Ok(())
}

pub fn load_pending(path: &Path) -> Result<PendingFile> {
    if !path.exists() {
        return Ok(PendingFile::default());
    }
    let raw = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    postcard::from_bytes(&raw).with_context(|| format!("decoding {}", path.display()))
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileKind {
    Movements,
    Corrections,
}

struct OpenFile {
    kind: FileKind,
    source: RangeFile,
    bytes: SparseBytes,
    archive: Archive,
    /// Bloom filters fetched for every group.
    blooms_loaded: bool,
}

impl OpenFile {
    /// The footer protocol: the tail, then the exact footer if the tail fell
    /// short.
    fn open(path: &Path, kind: FileKind) -> Result<Self> {
        let mut source = RangeFile::open(path)?;
        let mut bytes = SparseBytes::new(source.size());
        let (start, len) = policy_archive::reader::footer_request(
            source.size(),
            policy_archive::reader::FOOTER_HINT,
        );
        source.fetch(&mut bytes, start, len)?;
        let tail_start = source.size().saturating_sub(8);
        let mut tail = SparseBytes::new(source.size());
        source.fetch(&mut tail, tail_start, source.size() - tail_start)?;
        // `SparseBytes` has no public byte accessor by design; re-read the
        // eight bytes through the file rather than widen the crate's API.
        let mut last8 = vec![0u8; (source.size() - tail_start) as usize];
        source.file.seek(SeekFrom::Start(tail_start))?;
        source.file.read_exact(&mut last8)?;
        let need = policy_archive::reader::footer_length(&last8)?;
        if need > len {
            let (start, len) = policy_archive::reader::footer_request(source.size(), need);
            source.fetch(&mut bytes, start, len)?;
        }
        let archive = Archive::open(&bytes)
            .with_context(|| format!("opening footer of {}", path.display()))?;
        Ok(Self {
            kind,
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

/// One policy's archive, opened to its footers.
pub struct PolicyArchive {
    pub manifest: Manifest,
    files: Vec<OpenFile>,
}

impl PolicyArchive {
    /// `None` when no pass has ever run for this policy.
    pub fn open(dir: &Path) -> Result<Option<Self>> {
        let Some(manifest) = load_manifest(dir)? else {
            return Ok(None);
        };
        let mut files = Vec::new();
        for pass in &manifest.passes {
            let base = dir.join(&pass.dir);
            if let Some(f) = &pass.movements {
                files.push(OpenFile::open(&base.join(&f.file), FileKind::Movements)?);
            }
            if let Some(f) = &pass.corrections {
                files.push(OpenFile::open(&base.join(&f.file), FileKind::Corrections)?);
            }
        }
        Ok(Some(Self { manifest, files }))
    }

    /// Bytes and requests so far, across every file — what a Worker would
    /// have paid R2 for the same reads.
    pub fn fetched(&self) -> (usize, u64) {
        self.files.iter().fold((0, 0), |(r, b), f| {
            (r + f.source.requests, b + f.source.fetched)
        })
    }

    pub fn coverage(&self) -> Coverage {
        let movement_entries = self
            .manifest
            .passes
            .iter()
            .filter_map(|p| p.movements.as_ref());
        let first_slot = movement_entries.clone().filter_map(|f| f.min_slot).min();
        let last_slot = movement_entries.filter_map(|f| f.max_slot).max();
        let total_txs = self
            .files
            .iter()
            .filter(|f| f.kind == FileKind::Movements)
            .flat_map(|f| f.archive.groups().iter().map(|g| g.txs))
            .sum();
        Coverage {
            walked_from: self.manifest.walk_from,
            walked_to: self.manifest.walk_to,
            first_slot,
            last_slot,
            total_txs,
            units: self
                .manifest
                .passes
                .iter()
                .map(|p| p.units)
                .max()
                .unwrap_or(0),
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

/// Rows → feed rows. Sums per `(transaction, unit, party)` — the rule the
/// sqlite `ON CONFLICT … amount + excluded.amount` used to enforce — drops
/// parties that net to nothing, keeps a unit with no parties when a
/// placeholder said the transaction minted or burned it, and orders
/// newest-first.
pub fn fold_rows(rows: Vec<Movement>) -> Vec<FeedRow> {
    struct Acc {
        slot: u64,
        block_time: u64,
        units: BTreeMap<Vec<u8>, (i64, BTreeMap<String, i64>)>,
    }
    let mut by_tx: HashMap<Vec<u8>, Acc> = HashMap::new();
    for m in rows {
        let acc = by_tx.entry(m.tx_hash.clone()).or_insert_with(|| Acc {
            slot: m.slot,
            block_time: m.block_time,
            units: BTreeMap::new(),
        });
        let unit = acc.units.entry(m.unit_name.clone()).or_default();
        if m.net_mint != 0 {
            unit.0 = m.net_mint;
        }
        if !m.is_placeholder() {
            *unit.1.entry(m.address.clone()).or_insert(0) += m.amount;
        }
    }
    let mut out: Vec<FeedRow> = by_tx
        .into_iter()
        .filter_map(|(tx_hash, acc)| {
            let units: Vec<UnitMove> = acc
                .units
                .into_iter()
                .map(|(name, (net_mint, parties))| UnitMove {
                    name,
                    net_mint,
                    parties: parties
                        .into_iter()
                        .filter(|(_, amount)| *amount != 0)
                        .map(|(address, amount)| PartyMove {
                            stake: stake_of(&address),
                            address,
                            amount,
                        })
                        .collect(),
                })
                // A unit whose parties all netted to nothing and that was
                // neither minted nor burned did not MOVE — the asset rode
                // through the transaction as change. Measured on SpaceBudz:
                // 89 of 160 transactions in a 120-day window. Shown, it would
                // read as "ambiguous", which it is not; it is nothing.
                .filter(|u| !u.parties.is_empty() || u.net_mint != 0)
                .collect();
            (!units.is_empty()).then_some(FeedRow {
                tx_hash,
                slot: acc.slot,
                block_time: acc.block_time,
                units,
            })
        })
        .collect();
    out.sort_by(|a, b| b.slot.cmp(&a.slot).then_with(|| b.tx_hash.cmp(&a.tx_hash)));
    out
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

/// `token-ledger archive` — read an archive back the way a Worker would,
/// and say what it cost.
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
    }
    let cov = a.coverage();
    println!(
        "rows        txs={} first_slot={:?} last_slot={:?} unresolved={}",
        cov.total_txs, cov.first_slot, cov.last_slot, cov.unresolved
    );
    let (reqs, bytes) = a.fetched();
    println!("footers     {reqs} requests, {bytes} bytes");

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

    fn mv(tx: u8, unit: &str, addr: &str, amount: i64, net_mint: i64) -> Movement {
        Movement {
            slot: 100 + tx as u64,
            block_time: 1_700_000_000 + tx as u64,
            tx_hash: vec![tx; 32],
            unit_name: unit.as_bytes().to_vec(),
            address: addr.to_string(),
            amount,
            net_mint,
        }
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
            movements: None,
            corrections: None,
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
        assert_eq!(back.completeness(), Completeness::Unrecorded);
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
        };
        let bytes = postcard::to_stdvec(&p).unwrap();
        let back: PendingFile = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back.spenders, p.spenders);
    }
}
