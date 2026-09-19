//! `policy-ledger catalogue build` — the whole-corpus pass.
//!
//! Every policy in `base.pidx`, two `pread`s each, classified, sorted, encoded
//! to postcard, written atomically.
//!
//! # ⚠️ Chunk order, not policy order
//!
//! The pass visits policies sorted by their FIRST CHUNK. [`ChunkReader`] caches
//! one open file, so a corpus walked in chunk order opens ~9,000 files; the
//! same walk in prefix order opens one per policy and thrashes 231,616 times
//! against a 217 GB store.
//!
//! # ⚠️ Determinism is load-bearing
//!
//! The entries are sorted by policy id before encoding, so two builds of the
//! same corpus produce byte-identical CONTENT — which is the only reason the
//! change detection in `publish` can work at all. A `HashMap` iteration order
//! anywhere on this path would republish 1.7 MiB every day to say nothing new.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use collection_catalogue::{CATALOGUE_VERSION, Catalogue, Entry};
use policy_index::profile::{ChunkReader, profile_policy};
use policy_index::{Base, Record, base_path};

use crate::classify::{Overlay, Verdict, classify};
use crate::curated::{OverlayStats, load_curated, load_tracked};

#[derive(clap::Args, Debug)]
pub struct BuildArgs {
    /// The policy index root (`<dir>/policy/base.pidx`).
    #[arg(long)]
    pub index_dir: PathBuf,
    /// The Mithril immutable chunk store the index's spans point into.
    #[arg(long)]
    pub immutable: PathBuf,
    /// Where to write the artifact.
    #[arg(long, default_value = "catalogue.bin")]
    pub out: PathBuf,
    /// jpg.store's curated names: `{ "<56-hex policy>": "<name>" }`.
    /// OPTIONAL — without it the catalogue is purely chain-derived.
    #[arg(long)]
    pub curated: Option<PathBuf>,
    /// Policies `collection-ownership` already tracks, one 56-hex id per line.
    #[arg(long)]
    pub tracked: Option<PathBuf>,
    /// Stop after this many policies — for a smoke run. ⚠️ Produces a PARTIAL
    /// artifact; never publish one.
    #[arg(long)]
    pub limit: Option<usize>,
    /// Also write one JSON line per ACCEPTED entry, for eyeballing what the
    /// classifier let in.
    #[arg(long)]
    pub dump: Option<PathBuf>,
}

/// What a build did. Printed, and the shape `--dump` reports in.
#[derive(Debug, Default)]
pub struct Report {
    pub policies: usize,
    pub accepted: usize,
    pub named: usize,
    pub hazard: usize,
    pub tracked: usize,
    pub cip68: usize,
    pub jpg_verified: usize,
    pub aliases: usize,
    /// ⚠️ The FUNNEL. "30,619 accepted" says nothing about whether the rule is
    /// still right; the per-reason counts are how it gets re-argued.
    pub rejected: BTreeMap<&'static str, usize>,
    pub raw_bytes: usize,
    pub secs: f64,
}

impl Report {
    pub fn describe(&self) -> String {
        let funnel = self
            .rejected
            .iter()
            .map(|(k, v)| format!("{k} {v}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "{} policies → {} entries ({} named, {} aliases); \
             hazard {}, cip68 {}, tracked {}, jpg {}; \
             {:.1} KiB raw in {:.1}s\n  rejected: {funnel}",
            self.policies,
            self.accepted,
            self.named,
            self.aliases,
            self.hazard,
            self.cip68,
            self.tracked,
            self.jpg_verified,
            self.raw_bytes as f64 / 1024.0,
            self.secs,
        )
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

pub fn run(args: BuildArgs) -> Result<()> {
    let started = Instant::now();

    let mut overlay = Overlay::default();
    let mut overlay_stats = OverlayStats::default();
    if let Some(path) = &args.curated {
        load_curated(path, &mut overlay, &mut overlay_stats)?;
    }
    if let Some(path) = &args.tracked {
        load_tracked(path, &mut overlay, &mut overlay_stats)?;
    }
    eprintln!(
        "overlay: {} curated names kept of {} rows ({} placeholder, {} unparsed); {} tracked",
        overlay_stats.curated_kept,
        overlay_stats.curated_rows,
        overlay_stats.curated_placeholder,
        overlay_stats.curated_unparsed,
        overlay_stats.tracked,
    );

    let base = Base::open(&base_path(&args.index_dir))?;
    let (first_chunk, built_through_chunk) = base.covers();
    eprintln!(
        "index: {} records, {} policies, chunks {first_chunk}..={built_through_chunk}",
        base.len(),
        base.policies(),
    );

    // Chunk order — see the module header.
    let mut runs: Vec<_> = base.runs().collect();
    runs.sort_by_key(|r| r.first_chunk);
    if let Some(limit) = args.limit {
        eprintln!("⚠️  --limit {limit}: this artifact is PARTIAL, do not publish it");
        runs.truncate(limit);
    }

    let mut reader = ChunkReader::new(&args.immutable);
    let mut report = Report {
        policies: runs.len(),
        ..Default::default()
    };
    let mut entries: Vec<Entry> = Vec::new();
    let mut records: Vec<Record> = Vec::new();

    for (i, run) in runs.iter().enumerate() {
        records.clear();
        records.extend(
            (run.start as usize..(run.start + run.len) as usize).map(|i| base.record_at_index(i)),
        );
        let evidence = profile_policy(&mut reader, run.policy_prefix, &records);
        match classify(&evidence, &overlay) {
            Verdict::Collection(entry) => entries.push(*entry),
            Verdict::Rejected(reason) => *report.rejected.entry(reason.as_str()).or_default() += 1,
        }
        if (i + 1).is_multiple_of(25_000) {
            eprintln!("  {}/{} policies", i + 1, runs.len());
        }
    }

    // ⚠️ Deterministic order — the change detection depends on it.
    entries.sort_unstable_by_key(|e| e.policy);

    report.accepted = entries.len();
    for e in &entries {
        report.aliases += e.names.len();
        if !e.names.is_empty() {
            report.named += 1;
        }
        if e.flags.contains(collection_catalogue::EntryFlags::HAZARD) {
            report.hazard += 1;
        }
        if e.flags.contains(collection_catalogue::EntryFlags::TRACKED) {
            report.tracked += 1;
        }
        if e.flags.contains(collection_catalogue::EntryFlags::CIP68) {
            report.cip68 += 1;
        }
        if e.flags
            .contains(collection_catalogue::EntryFlags::JPG_VERIFIED)
        {
            report.jpg_verified += 1;
        }
    }

    if let Some(path) = &args.dump {
        dump(path, &entries)?;
    }

    let catalogue = Catalogue {
        version: CATALOGUE_VERSION,
        built_through_chunk,
        built_at: now_unix(),
        entries,
        ext: Vec::new(),
    };
    let bytes = collection_catalogue::encode(&catalogue).context("encoding the catalogue")?;
    report.raw_bytes = bytes.len();
    report.secs = started.elapsed().as_secs_f64();

    // Atomic: a half-written artifact is indistinguishable from a corrupt one
    // to the publish step that reads it next.
    let tmp = args.out.with_extension("bin.tmp");
    std::fs::write(&tmp, &bytes).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, &args.out)
        .with_context(|| format!("renaming into {}", args.out.display()))?;

    println!("{}", report.describe());
    println!("wrote {}", args.out.display());
    Ok(())
}

/// One JSON line per accepted entry — for eyeballing, not for consumption.
/// The artifact is the contract; this is a debugging view of it.
fn dump(path: &std::path::Path, entries: &[Entry]) -> Result<()> {
    use std::io::{BufWriter, Write};

    #[derive(serde::Serialize)]
    struct Row<'a> {
        policy: String,
        assets: u32,
        flags: u8,
        names: Vec<(&'a str, u8)>,
    }

    let mut w = BufWriter::new(
        std::fs::File::create(path).with_context(|| format!("creating {}", path.display()))?,
    );
    for e in entries {
        let row = Row {
            policy: e.policy_hex(),
            assets: e.assets,
            flags: e.flags.0,
            names: e
                .names
                .iter()
                .map(|a| (a.name.as_str(), a.origin as u8))
                .collect(),
        };
        writeln!(w, "{}", serde_json::to_string(&row)?)?;
    }
    w.flush()?;
    eprintln!("dumped {} entries to {}", entries.len(), path.display());
    Ok(())
}
