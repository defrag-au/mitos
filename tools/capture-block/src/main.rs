//! `capture-block`: read-only block fetch from a dolos archive.
//!
//! Opens the archive's redb index in **read-only mode** and
//! reads the block bytes directly from the flatfile segments —
//! no dolos dependency, no `Database::create()`, no writer
//! setup. The tool cannot mutate the archive even if it tried.
//!
//! IMPORTANT: redb takes an exclusive file lock on open, so the
//! tool **fails to open** the archive if mitos / dolos is
//! currently running against the same data dir. Stop the
//! writer before running, then restart it after. This is the
//! same constraint the future reader/writer split (see
//! `MITOS_ISOLATION_ROADMAP.md`) is designed to fix; for now,
//! brief downtime is acceptable.
//!
//! Storage format (dolos v1.0.3, redb3 backend):
//! - `<storage.path>/archive/index` — redb file, table `blocks`,
//!   key=u64 (block slot), value=16-byte packed BlockLocation
//!   `{segment_id u32 BE, offset u64 BE, length u32 BE}`
//! - `<storage.path>/archive/{segment_id:06}.segment` — append-
//!   only block CBOR; read by `seek(offset) + read_exact(length)`
//!
//! Usage:
//!   capture-block --config dolos.toml --slot 90000000 \
//!                 --out crates/mitos-platform/tests/fixtures/90000000.block.cbor

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use redb::{Builder, ReadableDatabase, ReadableTable, TableDefinition};
use serde::Deserialize;

/// Dolos archive index table — same definition the writer side uses.
const BLOCKS_TABLE: TableDefinition<'static, u64, &'static [u8]> = TableDefinition::new("blocks");

/// One Cardano epoch worth of slots per segment file (matches
/// `dolos_redb3::archive::flatfiles::SLOTS_PER_SEGMENT`).
const SLOTS_PER_SEGMENT: u64 = 432_000;

/// Packed BlockLocation size — `{segment_id, offset, length}` as
/// big-endian `u32 + u64 + u32`.
const BLOCK_LOCATION_SIZE: usize = 16;

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Capture one block's raw CBOR from a dolos archive (read-only)",
    long_about = "Read-only block fetch. Opens the dolos archive index via \
                  redb's read-only mode and reads block bytes directly from \
                  the flatfile segments. Mitos / dolos must NOT be running \
                  against the same data dir — redb takes an exclusive file \
                  lock at open time and will return DatabaseAlreadyOpen \
                  otherwise. Stop the writer first, then restart it after."
)]
struct Args {
    /// Path to the mitos / dolos config TOML. Only `storage.path`
    /// is read; everything else in the file is ignored.
    #[arg(long, default_value = "dolos.toml")]
    config: PathBuf,

    /// Slot to capture. Pass an integer slot number, or `tip`
    /// to grab whatever the archive currently considers the tip.
    #[arg(long)]
    slot: SlotSpec,

    /// Output file path. Convention for the equivalence test
    /// fixture is
    /// `crates/mitos-platform/tests/fixtures/<slot>.block.cbor`.
    #[arg(long)]
    out: PathBuf,

    /// Don't actually write the file; just decode + summarise.
    /// Useful for confirming a slot has the content you want
    /// before committing.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Clone)]
enum SlotSpec {
    Tip,
    Number(u64),
}

impl std::str::FromStr for SlotSpec {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("tip") {
            Ok(Self::Tip)
        } else {
            s.parse::<u64>()
                .map(Self::Number)
                .map_err(|e| anyhow::anyhow!("--slot must be `tip` or an integer: {e}"))
        }
    }
}

/// Minimal config slice — only `[storage].path` is needed for
/// read-only block fetch. Reusing dolos's full `RootConfig` would
/// drag the whole dolos dep tree in.
#[derive(Debug, Deserialize)]
struct DolosConfig {
    storage: StorageSlice,
}

#[derive(Debug, Deserialize)]
struct StorageSlice {
    path: PathBuf,
}

#[derive(Debug, Clone, Copy)]
struct BlockLocation {
    segment_id: u32,
    offset: u64,
    length: u32,
}

impl BlockLocation {
    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        if bytes.len() < BLOCK_LOCATION_SIZE {
            anyhow::bail!(
                "BlockLocation expected {} bytes, got {}",
                BLOCK_LOCATION_SIZE,
                bytes.len()
            );
        }
        Ok(Self {
            segment_id: u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
            offset: u64::from_be_bytes(bytes[4..12].try_into().unwrap()),
            length: u32::from_be_bytes(bytes[12..16].try_into().unwrap()),
        })
    }
}

fn segment_path(archive_dir: &std::path::Path, segment_id: u32) -> PathBuf {
    archive_dir.join(format!("{segment_id:06}.segment"))
}

fn read_block(archive_dir: &std::path::Path, loc: &BlockLocation) -> anyhow::Result<Vec<u8>> {
    let path = segment_path(archive_dir, loc.segment_id);
    let mut file = File::open(&path)
        .with_context(|| format!("opening segment file {}", path.display()))?;
    file.seek(SeekFrom::Start(loc.offset))?;
    let mut buf = vec![0u8; loc.length as usize];
    file.read_exact(&mut buf)
        .context("read_exact from segment")?;
    Ok(buf)
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    // Parse config — just enough to find `storage.path`.
    let cfg_text = std::fs::read_to_string(&args.config)
        .with_context(|| format!("reading config {}", args.config.display()))?;
    let cfg: DolosConfig = toml::from_str(&cfg_text)
        .context("parsing dolos.toml [storage] section")?;
    let archive_dir = cfg.storage.path.join("archive");
    let index_path = archive_dir.join("index");
    tracing::info!(archive_dir = %archive_dir.display(), "opening archive read-only");

    // Open redb in read-only mode. Returns DatabaseAlreadyOpen if
    // mitos / dolos holds the writer lock (which is the safe
    // failure mode — better to bail with a clear error than to
    // race with the writer).
    let db = Builder::new()
        .open_read_only(&index_path)
        .with_context(|| {
            format!(
                "opening redb at {} read-only \
                 (is mitos / dolos still running? \
                 redb takes an exclusive file lock on open)",
                index_path.display()
            )
        })?;
    let rx = db.begin_read().context("redb begin_read")?;
    let table = rx.open_table(BLOCKS_TABLE).context("open `blocks` table")?;

    // Resolve slot → BlockLocation.
    let (slot, loc) = match args.slot {
        SlotSpec::Tip => {
            let entry = table
                .last()
                .context("read tip from blocks table")?
                .context("archive `blocks` table is empty")?;
            let slot = entry.0.value();
            let loc = BlockLocation::from_bytes(entry.1.value())?;
            tracing::info!(slot, "resolved tip");
            (slot, loc)
        }
        SlotSpec::Number(n) => {
            let raw = table
                .get(n)
                .with_context(|| format!("looking up slot {n}"))?
                .with_context(|| {
                    format!(
                        "no block at slot {n} (archive may not cover this slot — \
                         segment_id would be {})",
                        (n / SLOTS_PER_SEGMENT)
                    )
                })?;
            let loc = BlockLocation::from_bytes(raw.value())?;
            (n, loc)
        }
    };
    tracing::info!(
        slot,
        segment_id = loc.segment_id,
        offset = loc.offset,
        length = loc.length,
        "found block",
    );

    // Read CBOR bytes from the flatfile segment.
    let bytes = read_block(&archive_dir, &loc)?;
    tracing::info!(slot, bytes = bytes.len(), "fetched block CBOR");

    // Round-trip through the platform's decoder so we know the
    // bytes are exactly what the equivalence test will accept.
    let decoded = mitos_platform::decode_block(&bytes)
        .context("decode_block round-trip — fetched CBOR is not pallas-decodable")?;
    tracing::info!(
        decoded_slot = decoded.slot,
        tx_count = decoded.txs.len(),
        "decoded ok",
    );

    // Content summary so the user can sanity-check before committing.
    let mut total_outputs = 0usize;
    let mut total_assets = 0usize;
    let mut policies = std::collections::BTreeSet::new();
    for tx in &decoded.txs {
        total_outputs += tx.outputs.len();
        for out in &tx.outputs {
            total_assets += out.assets.len();
            for asset in &out.assets {
                policies.insert(hex::encode(&asset.asset.policy));
            }
        }
    }
    tracing::info!(
        total_outputs,
        total_assets,
        distinct_policies = policies.len(),
        "block content summary",
    );
    if !policies.is_empty() && policies.len() <= 8 {
        tracing::info!(?policies, "policies present");
    }

    if args.dry_run {
        tracing::info!("--dry-run: not writing");
        return Ok(());
    }

    if let Some(parent) = args.out.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    std::fs::write(&args.out, &bytes)
        .with_context(|| format!("writing {}", args.out.display()))?;
    tracing::info!(out = %args.out.display(), bytes = bytes.len(), "wrote fixture");
    Ok(())
}
