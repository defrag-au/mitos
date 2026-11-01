//! `art_needles` — scan raw chunk files for on-chain art.
//!
//! A spike, not a tool: it answers two questions before anything is built on
//! the answer — how much image-shaped data is in the chain, and what a bite
//! looks like. It prints a per-needle table, then samples each needle's hits
//! with the context needed to judge precision: a base64 run's length, a PNG's
//! declared dimensions, or the bytes around a raw magic.
//!
//! ```sh
//! cargo run -p chain-sieve --example art_needles -- \
//!     /Volumes/cardano-dev/cardano/mithril/db/immutable
//! cargo run -p chain-sieve --example art_needles -- <dir> --chunks 0,1000,9000 --print 8
//! cargo run -p chain-sieve --example art_needles -- <dir> --all
//! ```
//!
//! The default is a spread of 12 chunks sampled across the store, read
//! single-threaded. This is the raw scan `chain_sieve::scan_extract` wraps —
//! it is not built on `Needles` because the gate is deliberately a bool, and a
//! spike needs per-pattern identity, offsets, and the bytes around a hit.

use std::path::{Path, PathBuf};
use std::time::Instant;

use aho_corasick::AhoCorasick;
use anyhow::{bail, Context, Result};

const MIB: f64 = 1024.0 * 1024.0;

/// How a needle's magic is encoded in the bytes.
#[derive(Clone, Copy, PartialEq)]
enum Enc {
    /// The bytes as stored, i.e. a file header.
    Raw,
    /// Base64 — how a payload usually rides in tx metadata.
    B64,
    /// Hex — how jpg.store's label-50+ preimages ride.
    Hex,
}

/// One needle: name, encoding, bytes.
const NEEDLES: &[(&str, Enc, &[u8])] = &[
    ("png.raw", Enc::Raw, b"\x89PNG\r\n\x1a\n"),
    ("png.b64", Enc::B64, b"iVBORw0KGgo"),
    ("png.hex", Enc::Hex, b"89504e470d0a1a0a"),
    ("png.hex.upper", Enc::Hex, b"89504E470D0A1A0A"),
    ("jpeg.raw", Enc::Raw, b"\xff\xd8\xff"),
    ("jpeg.b64.jfif", Enc::B64, b"/9j/4AAQ"),
    ("jpeg.b64.exif", Enc::B64, b"/9j/4Q"),
    ("jpeg.hex", Enc::Hex, b"ffd8ff"),
    ("gif.raw89", Enc::Raw, b"GIF89a"),
    ("gif.raw87", Enc::Raw, b"GIF87a"),
    ("gif.b64", Enc::B64, b"R0lGOD"),
    ("webp.raw", Enc::Raw, b"WEBPVP8"),
    ("webp.b64", Enc::B64, b"UklGR"),
    ("avif.raw", Enc::Raw, b"ftypavif"),
    ("heic.raw", Enc::Raw, b"ftypheic"),
    ("svg.raw", Enc::Raw, b"<svg"),
    ("svg.xml", Enc::Raw, b"<svg xmlns"),
];

const B64_CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";
const HEX_CHARS: &[u8] = b"0123456789abcdefABCDEF";

struct Args {
    dir: PathBuf,
    chunks: Option<Vec<u64>>,
    all: bool,
    spread: usize,
    print: usize,
}

fn main() -> Result<()> {
    let args = parse_args()?;
    let nums = list_chunks(&args.dir)?;
    let selected = if args.all {
        nums
    } else if let Some(cs) = args.chunks {
        cs
    } else {
        spread(&nums, args.spread)
    };

    let ac = AhoCorasick::new(NEEDLES.iter().map(|n| n.2)).context("building automaton")?;
    let mut hits = vec![0u64; NEEDLES.len()];
    let mut hit_chunks = vec![0u64; NEEDLES.len()];
    let mut samples: Vec<Vec<String>> = vec![Vec::new(); NEEDLES.len()];
    let mut bytes_scanned = 0u64;

    let loud = selected.len() <= 64;
    let started = Instant::now();
    for (i, &n) in selected.iter().enumerate() {
        let path = args.dir.join(format!("{n:05}.chunk"));
        let bytes = std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        bytes_scanned += bytes.len() as u64;
        if loud || i.is_multiple_of(200) {
            eprintln!("scanned {n:05}.chunk · {:.1} MiB", bytes.len() as f64 / MIB);
        }
        let mut seen = vec![false; NEEDLES.len()];
        for m in ac.find_iter(bytes.as_slice()) {
            let p = m.pattern().as_usize();
            hits[p] += 1;
            seen[p] = true;
            if samples[p].len() < args.print {
                samples[p].push(sample(&bytes, p, n, m.start()));
            }
        }
        for (p, s) in seen.iter().enumerate() {
            if *s {
                hit_chunks[p] += 1;
            }
        }
    }
    let secs = started.elapsed().as_secs_f64().max(0.001);

    println!();
    println!(
        "# art needle probe · {} · {} chunks · {:.1} MiB · {:.1}s ({:.0} MiB/s)",
        args.dir.display(),
        selected.len(),
        bytes_scanned as f64 / MIB,
        secs,
        bytes_scanned as f64 / MIB / secs
    );
    report(&hits, &hit_chunks, bytes_scanned, &samples);
    Ok(())
}

fn report(hits: &[u64], hit_chunks: &[u64], bytes: u64, samples: &[Vec<String>]) {
    let gib = bytes as f64 / (1024.0 * MIB);
    let mut order: Vec<usize> = (0..NEEDLES.len()).collect();
    order.sort_by_key(|&i| std::cmp::Reverse(hits[i]));
    let w = NEEDLES.iter().map(|n| n.0.len()).max().unwrap_or(8);
    println!();
    println!(
        "  {:<w$} {:>9} {:>8} {:>9}",
        "needle", "hits", "chunks", "per GiB"
    );
    for &i in &order {
        let name = NEEDLES[i].0;
        let per = if gib > 0.0 { hits[i] as f64 / gib } else { 0.0 };
        println!(
            "  {name:<w$} {:>9} {:>8} {:>9.1}",
            hits[i], hit_chunks[i], per
        );
    }
    if samples.iter().any(|s| !s.is_empty()) {
        println!();
        println!("# samples");
        for (i, ss) in samples.iter().enumerate() {
            if ss.is_empty() {
                continue;
            }
            println!("--- {}", NEEDLES[i].0);
            for s in ss {
                println!("{s}");
            }
        }
    }
}

/// One hit, formatted for the report: where it is, what shape it has, and
/// enough of it to tell whether it is really an image.
fn sample(bytes: &[u8], p: usize, chunk: u64, at: usize) -> String {
    let (name, enc, _) = NEEDLES[p];
    let mut out = format!("  chunk {chunk:05} +0x{at:x}");
    match enc {
        Enc::Raw => {
            if name == "png.raw" {
                if let Some((w, h)) = dims_from_ihdr(&bytes[at..(at + 24).min(bytes.len())]) {
                    out.push_str(&format!(" · PNG {w}x{h}"));
                }
            }
            let lo = at.saturating_sub(16);
            let hi = (at + 32).min(bytes.len());
            out.push_str(&format!("\n    {}", escaped(&bytes[lo..hi])));
        }
        Enc::B64 => {
            let run = charset_run(bytes, at, B64_CHARS);
            out.push_str(&format!(
                " · run {run} chars (~{})",
                human(run as u64 * 3 / 4)
            ));
            if name == "png.b64" {
                if let Some((w, h)) = png_dims_b64(&bytes[at..(at + 32).min(bytes.len())]) {
                    out.push_str(&format!(" · PNG {w}x{h}"));
                }
            }
            let end = (at + 96).min(at + run);
            out.push_str(&format!(
                "\n    {}…",
                String::from_utf8_lossy(&bytes[at..end])
            ));
        }
        Enc::Hex => {
            let run = charset_run(bytes, at, HEX_CHARS);
            out.push_str(&format!(" · run {run} chars (~{})", human(run as u64 / 2)));
            if name.starts_with("png.hex") {
                if let Some((w, h)) = png_dims_hex(&bytes[at..(at + 48).min(bytes.len())]) {
                    out.push_str(&format!(" · PNG {w}x{h}"));
                }
            }
            let end = (at + 96).min(at + run);
            out.push_str(&format!(
                "\n    {}…",
                String::from_utf8_lossy(&bytes[at..end])
            ));
        }
    }
    out
}

/// Every `NNNNN.chunk` in the directory. Unlike `chain_sieve::list_chunks`
/// this keeps the newest — the volume holds a static copy, not a live store.
fn list_chunks(dir: &Path) -> Result<Vec<u64>> {
    let mut nums: Vec<u64> = std::fs::read_dir(dir)
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

/// An even spread of `n` chunk numbers across the store.
fn spread(nums: &[u64], n: usize) -> Vec<u64> {
    if nums.len() <= n {
        return nums.to_vec();
    }
    (0..n)
        .map(|i| nums[i * (nums.len() - 1) / (n - 1).max(1)])
        .collect()
}

fn parse_args() -> Result<Args> {
    let mut dir: Option<PathBuf> = None;
    let mut chunks = None;
    let mut all = false;
    let mut spread = 12usize;
    let mut print = 5usize;
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--chunks" => {
                let v = it.next().context("--chunks needs a value")?;
                chunks = Some(
                    v.split(',')
                        .map(|s| s.trim().parse::<u64>())
                        .collect::<std::result::Result<Vec<_>, _>>()?,
                );
            }
            "--all" => all = true,
            "--spread" => spread = it.next().context("--spread needs a value")?.parse()?,
            "--print" => print = it.next().context("--print needs a value")?.parse()?,
            "-h" | "--help" => {
                usage();
                std::process::exit(0);
            }
            other if other.starts_with("--") => bail!("unknown flag {other}"),
            other => {
                if dir.is_some() {
                    bail!("one directory, not two: {other}");
                }
                dir = Some(PathBuf::from(other));
            }
        }
    }
    Ok(Args {
        dir: dir.context(
            "usage: art_needles <immutable dir> [--chunks a,b,c] [--spread N] [--all] [--print N]",
        )?,
        chunks,
        all,
        spread,
        print,
    })
}

fn usage() {
    eprintln!("usage: art_needles <immutable dir> [--chunks 0,1000,9000] [--spread N] [--all] [--print N]");
}

fn charset_run(bytes: &[u8], at: usize, set: &[u8]) -> usize {
    bytes[at..].iter().take_while(|b| set.contains(b)).count()
}

fn escaped(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| {
            if (0x20..0x7f).contains(b) {
                *b as char
            } else {
                '.'
            }
        })
        .collect()
}

fn human(n: u64) -> String {
    if n < 1024 {
        format!("{n} B")
    } else if n < 1024 * 1024 {
        format!("{:.1} KiB", n as f64 / 1024.0)
    } else {
        format!("{:.1} MiB", n as f64 / MIB)
    }
}

/// A PNG's declared size, from the first 24 bytes of the stream: the 8-byte
/// magic followed by the IHDR chunk's width and height.
fn dims_from_ihdr(head: &[u8]) -> Option<(u32, u32)> {
    if head.len() < 24 || &head[..8] != b"\x89PNG\r\n\x1a\n" {
        return None;
    }
    let w = u32::from_be_bytes(head[16..20].try_into().ok()?);
    let h = u32::from_be_bytes(head[20..24].try_into().ok()?);
    Some((w, h))
}

fn b64_val(b: u8) -> Option<u32> {
    Some(match b {
        b'A'..=b'Z' => (b - b'A') as u32,
        b'a'..=b'z' => (b - b'a' + 26) as u32,
        b'0'..=b'9' => (b - b'0' + 52) as u32,
        b'+' => 62,
        b'/' => 63,
        b'=' => 0,
        _ => return None,
    })
}

fn png_dims_b64(s: &[u8]) -> Option<(u32, u32)> {
    let mut head = [0u8; 24];
    for (i, slot) in head.iter_mut().enumerate() {
        let c = i / 3 * 4;
        let v0 = b64_val(*s.get(c)?)?;
        let v1 = b64_val(*s.get(c + 1)?)?;
        let v2 = b64_val(*s.get(c + 2)?)?;
        let v3 = b64_val(*s.get(c + 3)?)?;
        let triple = (v0 << 18) | (v1 << 12) | (v2 << 6) | v3;
        *slot = (triple >> (16 - 8 * (i % 3))) as u8;
    }
    dims_from_ihdr(&head)
}

fn hex_val(b: u8) -> Option<u8> {
    Some(match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => return None,
    })
}

fn png_dims_hex(s: &[u8]) -> Option<(u32, u32)> {
    let mut head = [0u8; 24];
    for (i, slot) in head.iter_mut().enumerate() {
        let hi = hex_val(*s.get(2 * i)?)?;
        let lo = hex_val(*s.get(2 * i + 1)?)?;
        *slot = (hi << 4) | lo;
    }
    dims_from_ihdr(&head)
}
