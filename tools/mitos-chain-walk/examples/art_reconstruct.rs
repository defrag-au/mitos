//! Art reconstruction — the needle pass's hits, stitched back into images.
//!
//! `crates/chain-sieve/examples/art_needles.rs` established the shape: on-chain
//! images ride in tx metadata as a CIP-25-style data URI split across a CBOR
//! array of ≤64-byte strings — first string `data:image/…;base64,`, the rest the
//! payload, 64 characters at a time. This example is the inverse: walk a chunk
//! window's blocks, decode every tx's metadata, find those arrays (or an inline
//! data-URI string, bare base64, hex, or a byte string), stitch, decode, check
//! against the format's own magic, and write real files plus a manifest.
//! `text/html` payloads are first-class too — the interactive pieces are pages,
//! not containers, so a declared HTML mime is kept as-is when no image magic
//! matches. `--datums` adds the CIP-68 pass: reference-token metadata rides as
//! an inline datum, and the same payload shapes are found in Plutus Data bytes
//! instead of metadata strings.
//!
//! ```sh
//! # a window
//! cargo run --release -p mitos-chain-walk --example art_reconstruct -- \
//!     --db /Volumes/cardano-dev/cardano/mithril/db/immutable \
//!     --chunks 2509,3346,3400-3410 --out .tmp/art
//!
//! # the full atlas — a worker pool over every chunk the directory holds
//! cargo run --release -p mitos-chain-walk --example art_reconstruct -- \
//!     --db /Volumes/cardano-dev/cardano/mithril/db/immutable \
//!     --all --out .tmp/atlas
//! ```
//!
//! Blocks are decoded with the same pallas decode the walkers use, one chunk
//! per work item. Pieces are named by a 64-bit FNV of their bytes, so the same
//! artwork minted a thousand times lands as one file with an occurrence count
//! in `manifest.tsv`; the manifest is checkpointed every 1000 chunks and rows
//! are sorted by slot at the end, so a parallel run is deterministic.

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::Parser;
use mitos_chain_walk::mithril::CHUNK_SLOTS;
use mitos_chain_walk::open_blocks;
use pallas_primitives::PlutusData;
use pallas_primitives::alonzo::Metadatum;
use pallas_primitives::babbage::DatumOption;
use pallas_traverse::{MultiEraBlock, MultiEraOutput, MultiEraTx};

/// Bare-base64 prefixes of the container magics (PNG, JPEG, GIF, WebP).
const B64_PREFIXES: &[&str] = &["iVBORw0KGgo", "/9j/4", "R0lGOD", "UklGR"];

/// Bare-hex prefixes of the same magics, matched case-insensitively.
const HEX_PREFIXES: &[&str] = &["89504e470d0a1a0a", "ffd8ff", "47494638", "52494646"];

#[derive(Parser)]
#[command(about = "Reconstruct on-chain images and documents from tx metadata in a chunk window")]
struct Args {
    /// The immutable DB root — `<data-dir>/immutable`, not the data dir.
    #[arg(long)]
    db: PathBuf,

    /// Chunk numbers: `2509,3346,3400-3410`.
    #[arg(long, conflicts_with = "all")]
    chunks: Option<String>,

    /// Every `.chunk` file the directory holds.
    #[arg(long)]
    all: bool,

    /// Worker threads over the chunk queue.
    #[arg(long)]
    threads: Option<usize>,

    /// Where decoded pieces and the manifest go.
    #[arg(long, default_value = ".tmp/art")]
    out: PathBuf,

    /// Print every reconstructed piece as it is found.
    #[arg(long)]
    verbose: bool,

    /// Walk inline datums too — CIP-68 reference-token metadata.
    #[arg(long)]
    datums: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let chunks = match (&args.chunks, args.all) {
        (Some(spec), _) => parse_chunks(spec)?,
        (None, true) => list_chunks(&args.db)?,
        (None, false) => bail!("give --chunks a,b-c or --all"),
    };
    let threads = args
        .threads
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        })
        .max(1);
    fs::create_dir_all(&args.out).with_context(|| format!("creating {}", args.out.display()))?;

    let started = Instant::now();
    let total = chunks.len() as u64;
    let queue: Mutex<VecDeque<u64>> = Mutex::new(chunks.iter().copied().collect());
    let atlas: Mutex<Atlas> = Mutex::new(Atlas::default());
    let done = AtomicU64::new(0);
    let images = AtomicU64::new(0);
    let bytes = AtomicU64::new(0);
    let db = args.db.as_path();
    let out = args.out.as_path();
    let verbose = args.verbose;
    let datums = args.datums;

    eprintln!(
        "# art reconstruct · {} · {} chunks · {threads} threads",
        db.display(),
        total,
    );
    std::thread::scope(|scope| -> Result<()> {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let queue = &queue;
                let atlas = &atlas;
                let done = &done;
                let images = &images;
                let bytes = &bytes;
                scope.spawn(move || -> Result<()> {
                    loop {
                        let chunk = { queue.lock().expect("queue").pop_front() };
                        let Some(chunk) = chunk else { break };
                        let path = db.join(format!("{chunk:05}.chunk"));
                        let size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                        let mut found = Vec::new();
                        scan_chunk(db, chunk, datums, &mut found)?;
                        bytes.fetch_add(size, Ordering::Relaxed);
                        {
                            let mut guard = atlas.lock().expect("atlas");
                            for candidate in found {
                                images.fetch_add(1, Ordering::Relaxed);
                                guard.insert(candidate, out, verbose)?;
                            }
                        }
                        let d = done.fetch_add(1, Ordering::Relaxed) + 1;
                        if d.is_multiple_of(250) {
                            let secs = started.elapsed().as_secs_f64().max(0.001);
                            let gib = bytes.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0 * 1024.0);
                            let mibs = bytes.load(Ordering::Relaxed) as f64 / 1e6 / secs;
                            eprintln!(
                                "scanned {d} of {total} chunks · {gib:.1} GiB · {} images · {mibs:.0} MB/s",
                                images.load(Ordering::Relaxed),
                            );
                        }
                        if d.is_multiple_of(1000) {
                            let mut guard = atlas.lock().expect("atlas");
                            guard.checkpoint(out)?;
                            eprintln!("checkpoint · manifest.tsv written at {d} chunks");
                        }
                    }
                    Ok(())
                })
            })
            .collect();
        for handle in handles {
            handle.join().expect("worker panicked")?;
        }
        Ok(())
    })?;

    let mut atlas = atlas.into_inner().expect("atlas");
    atlas.sort_rows();
    atlas.write_manifest(&args.out)?;

    let found: u64 = atlas.rows.iter().map(|r| r.occurrences as u64).sum();
    let unique = atlas.rows.len();
    let written: u64 = atlas.rows.iter().map(|r| r.bytes as u64).sum();
    let mut formats: Vec<(&str, usize)> = Vec::new();
    for row in &atlas.rows {
        match formats.iter_mut().find(|(f, _)| *f == row.format) {
            Some((_, n)) => *n += 1,
            None => formats.push((row.format, 1)),
        }
    }
    let formats = formats
        .iter()
        .map(|(f, n)| format!("{f} {n}"))
        .collect::<Vec<_>>()
        .join(" · ");

    println!();
    println!(
        "# art reconstruct · {} · {} chunks · {} images · {unique} unique",
        args.db.display(),
        total,
        found,
    );
    println!(
        "# {} duplicate occurrences · {} written · formats {formats}",
        found - unique as u64,
        human(written),
    );
    println!(
        "# out {} · manifest.tsv · {:.1}s",
        args.out.display(),
        started.elapsed().as_secs_f64(),
    );
    Ok(())
}

/// One chunk through the block walk: decode every block, walk every tx's
/// metadata, collect candidates.
fn scan_chunk(db: &Path, chunk: u64, datums: bool, found: &mut Vec<Candidate>) -> Result<()> {
    let start = chunk * CHUNK_SLOTS;
    let end = (chunk + 1) * CHUNK_SLOTS;
    let blocks = open_blocks(db, Some((start, Vec::new())))
        .with_context(|| format!("seeking chunk {chunk:05}"))?;
    for raw in blocks {
        let raw = raw.map_err(|e| anyhow::anyhow!("reading block: {e:?}"))?;
        let block = MultiEraBlock::decode(&raw)
            .map_err(|e| anyhow::anyhow!("decoding block in chunk {chunk:05}: {e:?}"))?;
        if block.slot() >= end {
            break;
        }
        for tx in block.txs() {
            let tx_hash = hex::encode(tx.hash().as_ref());
            let meta = tx.metadata();
            let pairs: Vec<(u64, &Metadatum)> = meta.collect();
            if !pairs.is_empty() {
                let mut path = Vec::new();
                for (label, value) in pairs {
                    walk(value, label, block.slot(), &tx_hash, &mut path, found);
                }
            }
            if datums {
                walk_output_datums(&tx, block.slot(), &tx_hash, found);
            }
        }
    }
    Ok(())
}

/// Inline-datum pass: a CIP-68 reference token carries its metadata as a
/// Plutus-Data map inside the output's datum, so the same payload shapes the
/// metadata walk knows (data URIs in bytes, arrays to join, container magic)
/// appear here with `BoundedBytes` where a metadata string would be.
fn walk_output_datums(tx: &MultiEraTx<'_>, slot: u64, tx_hash: &str, found: &mut Vec<Candidate>) {
    for output in tx.outputs() {
        let Some(DatumOption::Data(d)) = output.datum() else {
            continue;
        };
        let (label, asset) = match reference_asset(&output) {
            Some((policy, name, label)) => (label, format!("{policy}/{}", name_display(&name))),
            None => (0, "?".to_string()),
        };
        let mut path = vec![asset, "datum".to_string()];
        walk_plutus(d.deref().deref(), label, slot, tx_hash, &mut path, found);
    }
}

/// The output's CIP-67 reference asset (label 100), else its first asset:
/// policy hex, name bytes, CIP-67 label (0 when the name carries none).
fn reference_asset(output: &MultiEraOutput<'_>) -> Option<(String, Vec<u8>, u64)> {
    let mut first = None;
    for policy_assets in output.value().assets() {
        let policy = hex::encode(policy_assets.policy().as_ref());
        for asset in policy_assets.assets() {
            let name = asset.name().to_vec();
            let label = cip67_label(&name).unwrap_or(0);
            if label == 100 {
                return Some((policy, name, label));
            }
            first.get_or_insert_with(|| (policy.clone(), name, label));
        }
    }
    first
}

/// The CIP-67 label encoded in a 4-byte asset-name prefix, if the framing
/// holds: top nibble of byte 0 zero, bottom nibble of byte 3 zero.
fn cip67_label(name: &[u8]) -> Option<u64> {
    if name.len() < 4 || name[0] & 0xf0 != 0 || name[3] & 0x0f != 0 {
        return None;
    }
    let label =
        (u32::from(name[0] & 0x0f) << 12) | (u32::from(name[1]) << 4) | (u32::from(name[2]) >> 4);
    Some(u64::from(label))
}

fn name_display(name: &[u8]) -> String {
    match std::str::from_utf8(name) {
        Ok(s) if s.chars().all(|c| !c.is_control()) => truncate(s, 24),
        _ => truncate(&hex::encode(name), 24),
    }
}

/// Walk a decoded datum for payload-shaped leaves — the Plutus twin of
/// [`walk`]. Bytes carry both the binary and the textual (data URI) forms,
/// and an array of bytes is joined the way a string array is.
fn walk_plutus(
    value: &PlutusData,
    label: u64,
    slot: u64,
    tx: &str,
    path: &mut Vec<String>,
    found: &mut Vec<Candidate>,
) {
    if path.len() > 16 {
        return;
    }
    match value {
        PlutusData::Constr(c) => {
            for (i, field) in c.fields.iter().enumerate() {
                path.push(format!("[{i}]"));
                walk_plutus(field, label, slot, tx, path, found);
                path.pop();
            }
        }
        PlutusData::Map(entries) => {
            for (k, v) in entries.iter() {
                path.push(plutus_key(k));
                walk_plutus(v, label, slot, tx, path, found);
                path.pop();
            }
        }
        PlutusData::Array(items) => {
            if let Some(c) = crack_plutus(value) {
                found.push(finish(c, label, slot, tx, path));
                return;
            }
            for (i, item) in items.iter().enumerate() {
                path.push(format!("[{i}]"));
                walk_plutus(item, label, slot, tx, path, found);
                path.pop();
            }
        }
        PlutusData::BoundedBytes(b) => {
            if let Some(c) = crack_bytes(b) {
                found.push(finish(c, label, slot, tx, path));
            }
        }
        PlutusData::BigInt(_) => {}
    }
}

/// The payload out of a datum leaf or a bytes-array.
fn crack_plutus(value: &PlutusData) -> Option<Cracked> {
    match value {
        PlutusData::BoundedBytes(b) => crack_bytes(b),
        PlutusData::Array(items) => {
            let mut joined = Vec::new();
            for item in items.iter() {
                match item {
                    PlutusData::BoundedBytes(b) => joined.extend_from_slice(b),
                    _ => return None,
                }
            }
            crack_bytes(&joined)
        }
        _ => None,
    }
}

fn plutus_key(k: &PlutusData) -> String {
    match k {
        PlutusData::BoundedBytes(b) => match std::str::from_utf8(b) {
            Ok(s) => truncate(s, 24),
            Err(_) => truncate(&hex::encode(b.deref()), 24),
        },
        PlutusData::BigInt(pallas_primitives::BigInt::Int(n)) => i128::from(*n).to_string(),
        _ => "?".to_string(),
    }
}

/// Every `.chunk` file in the directory, newest included — the volume holds a
/// static copy, not a live store.
fn list_chunks(dir: &Path) -> Result<Vec<u64>> {
    let mut nums: Vec<u64> = fs::read_dir(dir)
        .with_context(|| format!("reading {}", dir.display()))?
        .filter_map(|e| {
            let name = e.ok()?.file_name().into_string().ok()?;
            let stem = name.strip_suffix(".chunk")?;
            stem.parse::<u64>().ok()
        })
        .collect();
    nums.sort_unstable();
    if nums.is_empty() {
        bail!("no .chunk files in {}", dir.display());
    }
    Ok(nums)
}

/// The deduplicated atlas: FNV → row index, plus the rows themselves.
#[derive(Default)]
struct Atlas {
    index: HashMap<u64, usize>,
    rows: Vec<Row>,
}

impl Atlas {
    /// Insert a candidate. First sighting writes the file; later ones bump the
    /// occurrence count.
    fn insert(&mut self, c: Candidate, out: &Path, verbose: bool) -> Result<()> {
        let hash = fnv1a(&c.bytes);
        if let Some(&i) = self.index.get(&hash) {
            self.rows[i].occurrences += 1;
            return Ok(());
        }
        let file = format!("{hash:016x}.{}", c.format);
        let path = out.join(&file);
        fs::write(&path, &c.bytes).with_context(|| format!("writing {}", path.display()))?;
        if verbose {
            let dims = match c.dims {
                Some((w, h)) => format!(" {w}x{h}"),
                None => String::new(),
            };
            println!(
                "{file} · {}{dims} · {} · slot {} · tx {}… · label {} · {}",
                c.format,
                human(c.bytes.len() as u64),
                c.slot,
                &c.tx[..8.min(c.tx.len())],
                c.label,
                c.path,
            );
        }
        self.index.insert(hash, self.rows.len());
        self.rows.push(Row {
            file,
            format: c.format,
            dims: c.dims,
            bytes: c.bytes.len(),
            occurrences: 1,
            complete: c.complete,
            slot: c.slot,
            tx: c.tx,
            label: c.label,
            path: c.path,
            mime: c.mime,
        });
        Ok(())
    }

    /// Slot order — deterministic however the workers interleaved.
    fn sort_rows(&mut self) {
        self.rows
            .sort_by(|a, b| (a.slot, &a.tx, &a.path).cmp(&(b.slot, &b.tx, &b.path)));
    }

    fn write_manifest(&self, out: &Path) -> Result<()> {
        let mut buf = String::from(
            "file\tformat\twidth\theight\tbytes\toccurrences\tcomplete\tslot\ttx\tlabel\tpath\tmime\n",
        );
        for e in &self.rows {
            let (w, h) = e
                .dims
                .map(|(w, h)| (w.to_string(), h.to_string()))
                .unwrap_or_default();
            buf.push_str(&format!(
                "{}\t{}\t{w}\t{h}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                e.file,
                e.format,
                e.bytes,
                e.occurrences,
                e.complete,
                e.slot,
                e.tx,
                e.label,
                e.path,
                e.mime,
            ));
        }
        let path = out.join("manifest.tsv");
        fs::write(&path, buf).with_context(|| format!("writing {}", path.display()))?;
        Ok(())
    }

    fn checkpoint(&mut self, out: &Path) -> Result<()> {
        self.sort_rows();
        self.write_manifest(out)
    }
}

struct Row {
    file: String,
    format: &'static str,
    dims: Option<(u32, u32)>,
    bytes: usize,
    occurrences: usize,
    complete: bool,
    slot: u64,
    tx: String,
    label: u64,
    path: String,
    mime: String,
}

/// One image-shaped value found in a tx's metadata, before dedup.
struct Candidate {
    slot: u64,
    tx: String,
    label: u64,
    path: String,
    mime: String,
    format: &'static str,
    dims: Option<(u32, u32)>,
    bytes: Vec<u8>,
    complete: bool,
}

/// Descend the metadata tree, testing every value that can carry a payload.
fn walk(
    value: &Metadatum,
    label: u64,
    slot: u64,
    tx: &str,
    path: &mut Vec<String>,
    found: &mut Vec<Candidate>,
) {
    if path.len() > 16 {
        return;
    }
    match value {
        Metadatum::Map(items) => {
            for (k, v) in items.iter() {
                path.push(key_label(k));
                walk(v, label, slot, tx, path, found);
                path.pop();
            }
        }
        Metadatum::Array(items) => {
            // An array of strings can be one payload to be joined; try it whole
            // before descending into the elements.
            if let Some(c) = crack(value) {
                found.push(finish(c, label, slot, tx, path));
                return;
            }
            for (i, item) in items.iter().enumerate() {
                path.push(format!("[{i}]"));
                walk(item, label, slot, tx, path, found);
                path.pop();
            }
        }
        Metadatum::Text(_) | Metadatum::Bytes(_) => {
            if let Some(c) = crack(value) {
                found.push(finish(c, label, slot, tx, path));
            }
        }
        Metadatum::Int(_) => {}
    }
}

fn finish(cracked: Cracked, label: u64, slot: u64, tx: &str, path: &[String]) -> Candidate {
    Candidate {
        slot,
        tx: tx.to_string(),
        label,
        path: path.join("/"),
        mime: cracked.mime,
        format: cracked.format,
        dims: cracked.dims,
        bytes: cracked.bytes,
        complete: cracked.complete,
    }
}

struct Cracked {
    mime: String,
    format: &'static str,
    dims: Option<(u32, u32)>,
    bytes: Vec<u8>,
    complete: bool,
}

/// A metadata value that decodes to image bytes, whatever encoding it used.
fn crack(v: &Metadatum) -> Option<Cracked> {
    if let Metadatum::Bytes(b) = v
        && let Some(c) = crack_bytes(b)
    {
        return Some(c);
    }
    let text = joined_text(v)?;
    crack_text(&text)
}

/// A byte payload: an image container's magic, or text inside bytes.
fn crack_bytes(b: &[u8]) -> Option<Cracked> {
    if let Some(s) = sniff(b) {
        return Some(Cracked {
            mime: String::new(),
            format: s.format,
            dims: s.dims,
            bytes: b.to_vec(),
            complete: s.complete,
        });
    }
    let text = std::str::from_utf8(b).ok()?;
    crack_text(text)
}

/// A textual payload: a data URI, bare base64, or bare hex.
fn crack_text(text: &str) -> Option<Cracked> {
    let text = text.trim();
    if let Some(rest) = text.strip_prefix("data:") {
        let comma = rest.find(',')?;
        let meta = &rest[..comma];
        let body = &rest[comma + 1..];
        let mime = meta.split(';').next().unwrap_or_default().to_string();
        if meta.contains("base64") {
            let raw = b64_decode(body)?;
            if let Some(s) = sniff(&raw) {
                return Some(Cracked {
                    mime,
                    format: s.format,
                    dims: s.dims,
                    bytes: raw,
                    complete: s.complete,
                });
            }
            if is_html_mime(&mime) {
                return Some(Cracked {
                    mime,
                    format: "html",
                    dims: None,
                    bytes: raw,
                    complete: true,
                });
            }
            return None;
        }
        if let Some(format) = sniff_text(body) {
            return Some(Cracked {
                mime,
                format,
                dims: None,
                bytes: body.as_bytes().to_vec(),
                complete: true,
            });
        }
        if is_html_mime(&mime) {
            return Some(Cracked {
                mime,
                format: "html",
                dims: None,
                bytes: body.as_bytes().to_vec(),
                complete: true,
            });
        }
        return None;
    }
    // Bare payloads (no data URI): require the container to be complete — the
    // magic alone is too weak, and hex-encoded CBOR teaches that lesson.
    if B64_PREFIXES.iter().any(|p| text.starts_with(p)) {
        let raw = b64_decode(text)?;
        let s = sniff(&raw)?;
        if s.complete {
            return Some(Cracked {
                mime: String::new(),
                format: s.format,
                dims: s.dims,
                bytes: raw,
                complete: s.complete,
            });
        }
    }
    let head = text.as_bytes();
    if HEX_PREFIXES
        .iter()
        .any(|p| head.len() >= p.len() && head[..p.len()].eq_ignore_ascii_case(p.as_bytes()))
    {
        let raw = hex_lenient(text)?;
        let s = sniff(&raw)?;
        if s.complete {
            return Some(Cracked {
                mime: String::new(),
                format: s.format,
                dims: s.dims,
                bytes: raw,
                complete: s.complete,
            });
        }
    }
    None
}

/// The string form of a value that can carry text: a string, or an array of
/// strings to be concatenated (the CIP-25 large-image convention).
fn joined_text(v: &Metadatum) -> Option<String> {
    match v {
        Metadatum::Text(s) => Some(s.clone()),
        Metadatum::Array(items) => {
            let mut out = String::new();
            for item in items {
                match item {
                    Metadatum::Text(s) => out.push_str(s),
                    _ => return None,
                }
            }
            Some(out)
        }
        _ => None,
    }
}

/// Magic sniffing: format, declared dimensions, and whether the container has
/// its own trailer (a complete file, not a truncated head).
struct Sniffed {
    format: &'static str,
    dims: Option<(u32, u32)>,
    complete: bool,
}

fn sniff(bytes: &[u8]) -> Option<Sniffed> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        let dims = if bytes.len() >= 24 {
            let w = u32::from_be_bytes(bytes[16..20].try_into().ok()?);
            let h = u32::from_be_bytes(bytes[20..24].try_into().ok()?);
            Some((w, h))
        } else {
            None
        };
        let complete = bytes.ends_with(&[0xAE, 0x42, 0x60, 0x82]);
        return Some(Sniffed {
            format: "png",
            dims,
            complete,
        });
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        let dims = if bytes.len() >= 10 {
            let w = u16::from_le_bytes(bytes[6..8].try_into().ok()?) as u32;
            let h = u16::from_le_bytes(bytes[8..10].try_into().ok()?) as u32;
            Some((w, h))
        } else {
            None
        };
        return Some(Sniffed {
            format: "gif",
            dims,
            complete: bytes.ends_with(&[0x3B]),
        });
    }
    if bytes.starts_with(b"\xff\xd8\xff") {
        return Some(Sniffed {
            format: "jpeg",
            dims: None,
            complete: bytes.ends_with(&[0xFF, 0xD9]),
        });
    }
    if bytes.starts_with(b"RIFF") && bytes.len() >= 12 && &bytes[8..12] == b"WEBP" {
        let complete = u32::from_le_bytes(bytes[4..8].try_into().unwrap_or([0; 4])) as usize + 8
            == bytes.len();
        return Some(Sniffed {
            format: "webp",
            dims: None,
            complete,
        });
    }
    let text = std::str::from_utf8(bytes).ok()?;
    sniff_text(text).map(|format| Sniffed {
        format,
        dims: None,
        complete: true,
    })
}

/// The text formats that ride as data URIs — HTML (including the wrapped-in-HTML
/// smuggling shape that appeared in the scan) and SVG.
fn sniff_text(body: &str) -> Option<&'static str> {
    let text = body.trim_start();
    let head: String = text
        .chars()
        .take(256)
        .collect::<String>()
        .to_ascii_lowercase();
    if head.starts_with("<!doctype") || head.contains("<html") {
        return Some("html");
    }
    if text.starts_with("<svg") || text.starts_with("<?xml") || text.contains("<svg") {
        return Some("svg");
    }
    None
}

/// The mimes that mean "this payload is a document, not a container".
fn is_html_mime(mime: &str) -> bool {
    mime.eq_ignore_ascii_case("text/html") || mime.eq_ignore_ascii_case("application/xhtml+xml")
}

/// Lenient base64: whitespace and padding skipped, `-`/`_` tolerated, any other
/// byte aborts. The strictness is the validation — image bytes decode, junk does
/// not survive `sniff`.
fn b64_decode(s: &str) -> Option<Vec<u8>> {
    let mut acc: u32 = 0;
    let mut n = 0;
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    for byte in s.bytes() {
        let v = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' | b'-' => 62,
            b'/' | b'_' => 63,
            b'=' | b'\n' | b'\r' | b' ' | b'\t' => continue,
            _ => return None,
        } as u32;
        acc = (acc << 6) | v;
        n += 1;
        if n == 4 {
            out.push((acc >> 16) as u8);
            out.push((acc >> 8) as u8);
            out.push(acc as u8);
            acc = 0;
            n = 0;
        }
    }
    match n {
        0 => {}
        2 => out.push((acc >> 4) as u8),
        3 => {
            out.push((acc >> 10) as u8);
            out.push((acc >> 2) as u8);
        }
        _ => return None,
    }
    Some(out)
}

fn hex_lenient(s: &str) -> Option<Vec<u8>> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut i = 0;
    while i + 1 < bytes.len() {
        let (Some(hi), Some(lo)) = (hex_val(bytes[i]), hex_val(bytes[i + 1])) else {
            break;
        };
        out.push((hi << 4) | lo);
        i += 2;
    }
    (!out.is_empty()).then_some(out)
}

fn hex_val(c: u8) -> Option<u8> {
    Some(match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => return None,
    })
}

fn key_label(k: &Metadatum) -> String {
    match k {
        Metadatum::Text(s) => truncate(s, 24),
        Metadatum::Bytes(b) => truncate(&hex::encode(b.to_vec()), 24),
        Metadatum::Int(i) => i.to_string(),
        _ => "?".to_string(),
    }
}

fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(n).collect();
        out.push('…');
        out
    }
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn human(n: u64) -> String {
    let n = n as f64;
    if n < 1024.0 {
        format!("{n:.0} B")
    } else if n < 1024.0 * 1024.0 {
        format!("{:.1} KiB", n / 1024.0)
    } else {
        format!("{:.1} MiB", n / (1024.0 * 1024.0))
    }
}

fn parse_chunks(spec: &str) -> Result<Vec<u64>> {
    let mut out = Vec::new();
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        match part.split_once('-') {
            Some((a, b)) => {
                let a: u64 = a.trim().parse().context("chunk range start")?;
                let b: u64 = b.trim().parse().context("chunk range end")?;
                if b < a || b - a > 100_000 {
                    bail!("chunk range {part} is malformed");
                }
                out.extend(a..=b);
            }
            None => out.push(part.parse().context("chunk number")?),
        }
    }
    if out.is_empty() {
        bail!("no chunks given");
    }
    Ok(out)
}
