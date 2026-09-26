//! `mithril-source` — prove a chunk is addressable, and stream it.
//!
//! `check` is the one that matters: it walks every block through the real windowed
//! reader and verifies each against the checks the node recorded for it, so a
//! successful run says the sidecars, the offsets and the range arithmetic all agree
//! with the bytes — with no local copy to compare against.

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use pallas_crypto::hash::Hasher;
use pallas_traverse::{Era, MultiEraBlock};

use mithril_source::{
    Chunk, HttpMirror, LocalDir, RangeSource, SECONDARY_ENTRY_LEN, Span, parse_primary,
};

#[derive(Parser)]
#[command(
    name = "mithril-source",
    version,
    about = "Read a Mithril snapshot's immutable files over HTTP ranges"
)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Parse a chunk's sidecars and verify every block against them.
    Check {
        /// A snapshot's immutable directory, its database directory, or a mirror URL.
        #[arg(default_value = "https://mithril.defrag.cc/mainnet")]
        source: String,
        #[arg(long)]
        chunk: u16,
    },
    /// Stream a chunk's blocks through the windowed range reader.
    Walk {
        #[arg(default_value = "https://mithril.defrag.cc/mainnet")]
        source: String,
        #[arg(long)]
        chunk: u16,
        /// Stop after printing this many blocks.
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Print the bytes of one span of a chunk.
    Span {
        #[arg(default_value = "https://mithril.defrag.cc/mainnet")]
        source: String,
        #[arg(long)]
        chunk: u16,
        #[arg(long)]
        offset: u64,
        #[arg(long)]
        len: u64,
    },
    /// Resolve scattered block bodies in batched range requests, and verify each.
    Batch {
        #[arg(default_value = "https://mithril.defrag.cc/mainnet")]
        source: String,
        #[arg(long)]
        chunk: u16,
        /// How many blocks to resolve.
        #[arg(long, default_value_t = 200)]
        count: usize,
        /// Take every Nth block, so the spans are scattered rather than contiguous.
        #[arg(long, default_value_t = 20)]
        stride: usize,
    },
}

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    let outcome = match cli.command {
        Cmd::Check { source, chunk } => check(&source, chunk),
        Cmd::Walk {
            source,
            chunk,
            limit,
        } => walk(&source, chunk, limit),
        Cmd::Span {
            source,
            chunk,
            offset,
            len,
        } => span(&source, chunk, offset, len),
        Cmd::Batch {
            source,
            chunk,
            count,
            stride,
        } => batch(&source, chunk, count, stride),
    };
    match outcome {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            std::process::ExitCode::FAILURE
        }
    }
}

/// Where the bytes come from, plus a count of what it cost to get them.
///
/// The count is the point: a batched read's whole claim is that it is ONE request,
/// and a claim like that should be checkable rather than hoped for.
enum Source {
    Http(Arc<HttpMirror>),
    Local(Arc<LocalDir>),
}

impl Source {
    /// A mirror URL, a snapshot's `immutable/` directory, or the database directory
    /// holding it.
    fn open(arg: &str) -> Result<Source> {
        if arg.starts_with("http://") || arg.starts_with("https://") {
            return Ok(Source::Http(Arc::new(HttpMirror::new(arg)?)));
        }
        let given = std::path::PathBuf::from(arg);
        // Accept the database directory or the `immutable/` inside it — `mithril-client`
        // writes one, this reads the other, and making the caller remember which is
        // which ends in the wrong path being passed.
        let dir = if given.join("immutable").is_dir() {
            given.join("immutable")
        } else {
            given
        };
        if !dir.is_dir() {
            bail!(
                "{} is neither a directory nor an http(s) URL",
                dir.display()
            );
        }
        Ok(Source::Local(Arc::new(LocalDir::new(dir))))
    }

    fn as_range_source(&self) -> Arc<dyn RangeSource> {
        match self {
            Source::Http(m) => {
                let owned: Arc<HttpMirror> = Arc::clone(m);
                owned
            }
            Source::Local(d) => {
                let owned: Arc<LocalDir> = Arc::clone(d);
                owned
            }
        }
    }

    /// HTTP requests made so far. A local directory makes none.
    fn requests(&self) -> u64 {
        match self {
            Source::Http(m) => m.requests(),
            Source::Local(_) => 0,
        }
    }

    fn is_remote(&self) -> bool {
        matches!(self, Source::Http(_))
    }
}

fn check(source_arg: &str, number: u16) -> Result<()> {
    let source = Source::open(source_arg)?.as_range_source();
    let base = format!("{number:05}");

    let primary_bytes = source
        .read_all(&format!("{base}.primary"))
        .with_context(|| format!("reading {base}.primary"))?;
    let primary = parse_primary(&primary_bytes)?;
    let secondary = source
        .read_all(&format!("{base}.secondary"))
        .with_context(|| format!("reading {base}.secondary"))?;

    println!(
        "sidecars    {base}.primary {} bytes · version {} · {} slots · {} occupied",
        primary_bytes.len(),
        primary.version(),
        primary.slots(),
        primary.occupied_count(),
    );
    println!(
        "            {base}.secondary {} bytes · {} entries of {SECONDARY_ENTRY_LEN}",
        secondary.len(),
        secondary.len() / SECONDARY_ENTRY_LEN,
    );

    let chunk = Chunk::open(Arc::clone(&source), number)?;
    let entries = chunk.entries();
    let last = entries.last().expect("Chunk::open refuses an empty chunk");
    println!(
        "chunk       {} bytes · {} blocks · slots {}..={} · offsets {}..={}",
        chunk.len(),
        chunk.block_count(),
        entries[0].slot,
        last.slot,
        entries[0].block_offset,
        last.block_offset,
    );

    let mut covered = 0u64;
    for i in 0..chunk.block_count() {
        covered += chunk.block_len(i)?;
    }
    println!(
        "coverage    {covered} of {} bytes across {} blocks",
        chunk.len(),
        chunk.block_count(),
    );

    let started = Instant::now();
    let mut blocks = 0u64;
    let mut bytes = 0u64;
    let (mut crc_ok, mut crc_bad) = (0u64, 0u64);
    let (mut header_ok, mut header_bad, mut header_unreadable, mut header_na) =
        (0u64, 0u64, 0u64, 0u64);
    let (mut decoded, mut undecodable) = (0u64, 0u64);
    let mut complaints: Vec<String> = Vec::new();

    for (i, block) in chunk.blocks().enumerate() {
        let block = block?;
        let entry = entries[i];
        blocks += 1;
        bytes += block.bytes.len() as u64;

        if crc32fast::hash(&block.bytes) == entry.checksum {
            crc_ok += 1;
        } else {
            crc_bad += 1;
            complain(
                &mut complaints,
                format!(
                    "block {i} (slot {}, offset {}): CRC32 mismatch",
                    entry.slot, entry.block_offset
                ),
            );
        }

        // The header hash is blake2b-256 of the header slice, which is what the block
        // hash IS from Shelley on. Byron hashes a block differently, so the field does
        // not hold a header hash there and the check does not apply — measured, not
        // assumed: on chunk 00000 the CRC32 matches for all 21,587 blocks and the
        // decode succeeds for all 21,587, while the header comparison matches none.
        match MultiEraBlock::decode(&block.bytes) {
            Ok(decoded_block) => {
                decoded += 1;
                if matches!(decoded_block.era(), Era::Byron) {
                    header_na += 1;
                    continue;
                }
                match entry.header_span() {
                    Some(span) if span.end <= block.bytes.len() => {
                        let got = Hasher::<256>::hash(&block.bytes[span]);
                        if got.as_ref() == entry.header_hash {
                            header_ok += 1;
                        } else {
                            header_bad += 1;
                            complain(
                                &mut complaints,
                                format!(
                                    "block {i} (slot {}, offset {}): header hash mismatch",
                                    entry.slot, entry.block_offset
                                ),
                            );
                        }
                    }
                    span => {
                        header_unreadable += 1;
                        complain(
                            &mut complaints,
                            format!(
                                "block {i} (slot {}, offset {}): header span {span:?} does not fit {} bytes",
                                entry.slot,
                                entry.block_offset,
                                block.bytes.len()
                            ),
                        );
                    }
                }
            }
            Err(err) => {
                undecodable += 1;
                complain(
                    &mut complaints,
                    format!(
                        "block {i} (slot {}, offset {}): decode failed: {err:?}",
                        entry.slot, entry.block_offset
                    ),
                );
            }
        }
    }

    println!(
        "walk        {blocks} blocks · {bytes} bytes in {:.0} ms",
        started.elapsed().as_secs_f64() * 1000.0
    );
    println!("crc32       {crc_ok} ok · {crc_bad} mismatched");
    println!(
        "header      {header_ok} ok · {header_bad} mismatched · {header_unreadable} unreadable · {header_na} not applicable (Byron)"
    );
    println!("decode      {decoded} ok · {undecodable} failed");
    for c in &complaints {
        println!("  ! {c}");
    }

    let problems = crc_bad + header_bad + header_unreadable + undecodable;
    if problems > 0 {
        bail!("{problems} problem(s) across {blocks} blocks of {base}");
    }
    Ok(())
}

fn complain(complaints: &mut Vec<String>, line: String) {
    if complaints.len() < 5 {
        complaints.push(line);
    }
}

fn walk(source_arg: &str, number: u16, limit: Option<usize>) -> Result<()> {
    let source = Source::open(source_arg)?.as_range_source();
    let chunk = Chunk::open(source, number)?;
    println!(
        "chunk {number:05} · {} blocks · {} bytes",
        chunk.block_count(),
        chunk.len()
    );

    let mut blocks = 0u64;
    let mut bytes = 0u64;
    for block in chunk.blocks() {
        let block = block?;
        blocks += 1;
        bytes += block.bytes.len() as u64;
        if limit.is_none_or(|l| blocks as usize <= l) {
            println!(
                "  slot {:>6}  offset {:>12}  {:>7} bytes  {}",
                block.slot,
                block.offset,
                block.bytes.len(),
                preview(&block.bytes, 24)
            );
        }
        if limit.is_some_and(|l| blocks as usize >= l) {
            break;
        }
    }
    println!(
        "walked {blocks} of {} blocks · {bytes} bytes",
        chunk.block_count()
    );
    Ok(())
}

fn span(source_arg: &str, number: u16, offset: u64, len: u64) -> Result<()> {
    let source = Source::open(source_arg)?.as_range_source();
    let chunk = Chunk::open(source, number)?;
    let got = chunk.read_span(offset, len)?;
    println!("chunk {number:05} · {} bytes at {offset}", got.len());
    println!("{}", hex::encode(&got));
    Ok(())
}

/// Resolve a scattered set of block bodies through the batched reader, and check
/// each against the CRC32 the node recorded for it.
///
/// The spans are deliberately scattered — every `stride`th block — because
/// contiguous blocks merge into a single span and would demonstrate nothing about
/// batching. The verification is the same one `check` uses, so a pass says the
/// multipart parts were reassembled into the right bytes.
fn batch(source_arg: &str, number: u16, count: usize, stride: usize) -> Result<()> {
    if stride == 0 {
        bail!("--stride must be at least 1");
    }
    let source = Source::open(source_arg)?;
    let chunk = Chunk::open(source.as_range_source(), number)?;
    let entries = chunk.entries();

    let chosen: Vec<usize> = (0..entries.len()).step_by(stride).take(count).collect();
    if chosen.is_empty() {
        bail!("chunk {number:05} has no blocks to resolve");
    }
    let spans: Vec<Span> = chosen
        .iter()
        .map(|&i| Ok(Span::new(entries[i].block_offset, chunk.block_len(i)?)))
        .collect::<Result<_>>()?;

    let before = source.requests();
    let started = Instant::now();
    let got = chunk.read_spans(&spans)?;
    let elapsed = started.elapsed();
    let requests = if source.is_remote() {
        (source.requests() - before).to_string()
    } else {
        "n/a (local)".to_string()
    };

    println!(
        "chunk {number:05} · {} of {} blocks · stride {stride}",
        spans.len(),
        entries.len()
    );
    println!(
        "fetched     {} spans · {} bytes in {:.0} ms · {requests} request(s)",
        got.len(),
        got.iter().map(Vec::len).sum::<usize>(),
        elapsed.as_secs_f64() * 1000.0,
    );

    let mut ok = 0u64;
    let mut bad = 0u64;
    for (n, &i) in chosen.iter().enumerate() {
        let entry = entries[i];
        if crc32fast::hash(&got[n]) == entry.checksum {
            ok += 1;
        } else {
            bad += 1;
            if bad <= 3 {
                println!(
                    "  ! block {i} (slot {}, offset {}): CRC32 mismatch",
                    entry.slot, entry.block_offset
                );
            }
        }
    }
    println!("crc32       {ok} ok · {bad} mismatched");
    if bad > 0 {
        bail!(
            "{bad} of {} batched spans are not the bytes the node recorded",
            spans.len()
        );
    }
    Ok(())
}

fn preview(bytes: &[u8], n: usize) -> String {
    let take = n.min(bytes.len());
    if take < bytes.len() {
        format!("{}…", hex::encode(&bytes[..take]))
    } else {
        hex::encode(&bytes[..take])
    }
}
