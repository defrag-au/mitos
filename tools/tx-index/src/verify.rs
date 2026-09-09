//! `verify` — the index's own audit. For a sample of entries in each
//! segment: read the body the entry points at, hash it, check the hash's
//! prefix is the entry's prefix, and decode its outputs by the segment's
//! era. A wrong offset, a wrong length, a wrong era byte or a body type
//! this build cannot decode all surface here as counted failures, and one
//! full hash per era is printed so `lookup` can be exercised by hand.

use std::collections::BTreeMap;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use tx_index::decode;
use tx_index::extract::chunk_path;
use tx_index::format::{era_from_u8, prefix_of};
use tx_index::segment::{SegmentFile, list_segments, segment_path};
use tx_index::tx_hash;

#[derive(clap::Args, Debug)]
pub struct VerifyArgs {
    /// Index directory (segments/ + base.idx).
    #[arg(long)]
    index_dir: PathBuf,

    /// Immutable DB dir the bodies are read from.
    #[arg(long)]
    immutable: PathBuf,

    /// Check every Nth entry of each segment (1 = every entry).
    #[arg(long, default_value_t = 97)]
    stride: usize,

    /// Only these chunks (default: every segment on disk).
    #[arg(long, value_delimiter = ',')]
    chunks: Vec<u16>,

    /// Failures to print in full before summarising.
    #[arg(long, default_value_t = 10)]
    show: usize,
}

#[derive(Default, Debug)]
struct Tally {
    checked: u64,
    ok: u64,
    prefix_mismatch: u64,
    read_failed: u64,
    decode_failed: u64,
    outputs: u64,
}

pub fn run(a: VerifyArgs) -> Result<()> {
    let chunks = if a.chunks.is_empty() {
        list_segments(&a.index_dir)?
    } else {
        a.chunks.clone()
    };
    if chunks.is_empty() {
        bail!("no segments under {}", a.index_dir.display());
    }
    let stride = a.stride.max(1);

    let mut per_era: BTreeMap<String, Tally> = BTreeMap::new();
    let mut sample_hash: BTreeMap<String, String> = BTreeMap::new();
    let mut shown = 0usize;

    for chunk in chunks {
        let seg = SegmentFile::open(&segment_path(&a.index_dir, chunk))
            .with_context(|| format!("opening segment {chunk}"))?;
        let era = era_from_u8(seg.header.era)?;
        let era_name = era.to_string();
        let tally = per_era.entry(era_name.clone()).or_default();

        let path = chunk_path(&a.immutable, chunk);
        let file = File::open(&path).with_context(|| format!("opening {}", path.display()))?;

        for i in (0..seg.len()).step_by(stride) {
            let e = seg.entry(i);
            tally.checked += 1;
            let mut body = vec![0u8; usize::from(e.loc.len)];
            if let Err(err) = file.read_exact_at(&mut body, u64::from(e.loc.offset)) {
                tally.read_failed += 1;
                if shown < a.show {
                    eprintln!(
                        "chunk {chunk} entry {i}: read failed at {}: {err}",
                        e.loc.offset
                    );
                    shown += 1;
                }
                continue;
            }
            let arr = tx_hash(&body);
            if prefix_of(&arr) != e.prefix {
                tally.prefix_mismatch += 1;
                if shown < a.show {
                    eprintln!(
                        "chunk {chunk} entry {i}: body at {}+{} hashes to {} (prefix {:016x}), entry says {:016x}",
                        e.loc.offset,
                        e.loc.len,
                        hex::encode(arr),
                        prefix_of(&arr),
                        e.prefix
                    );
                    shown += 1;
                }
                continue;
            }
            match decode::outputs(era, &body) {
                Ok(outs) => {
                    tally.ok += 1;
                    tally.outputs += outs.len() as u64;
                    sample_hash
                        .entry(era_name.clone())
                        .or_insert_with(|| hex::encode(arr));
                }
                Err(err) => {
                    tally.decode_failed += 1;
                    if shown < a.show {
                        eprintln!(
                            "chunk {chunk} entry {i}: {} decodes but outputs failed: {err:#}",
                            hex::encode(arr)
                        );
                        shown += 1;
                    }
                }
            }
        }
    }

    let mut failed = false;
    println!(
        "{:<10} {:>9} {:>9} {:>8} {:>6} {:>7} {:>10}",
        "era", "checked", "ok", "prefix✗", "read✗", "decode✗", "outputs"
    );
    for (era, t) in &per_era {
        println!(
            "{era:<10} {:>9} {:>9} {:>8} {:>6} {:>7} {:>10}",
            t.checked, t.ok, t.prefix_mismatch, t.read_failed, t.decode_failed, t.outputs
        );
        failed |= t.ok != t.checked;
    }
    println!();
    for (era, h) in &sample_hash {
        println!("sample {era:<8} {h}");
    }
    if failed {
        bail!("verification found failures");
    }
    Ok(())
}
