//! Mithril snapshot bootstrap — shell out to the `mithril-client` CLI (not the
//! SDK) to download + verify a certified immutable DB into a data dir. A walker
//! then reads `<download-dir>/immutable`. Keep the unpacked DB between runs and
//! refresh only when a re-run needs newer history; several walkers can share
//! one directory (market-ledger and project-ledger do, on cardano-infra).
//!
//! Partial ranges: `--start`/`--end` are immutable FILE numbers (≈21,600 slots
//! each on mainnet, i.e. `slot / 21_600`). A project walk with a known floor
//! can pull just its window instead of the ~250 GB full DB. Needs a
//! `mithril-client` with incremental cardano-db support; if yours wants extra
//! flags for that path (e.g. `--backend v2`), pass them via `--client-arg`.

use std::path::PathBuf;
use std::process::Command;

use anyhow::{Context, Result, bail};

/// Release-mainnet aggregator + genesis verification key (public, well-known).
pub const MAINNET_AGGREGATOR: &str =
    "https://aggregator.release-mainnet.api.mithril.network/aggregator";
pub const MAINNET_GENESIS_VKEY: &str = "5b3139312c36362c3134302c3138352c3133382c31312c3233372c3230372c3235302c3134342c32372c322c3138382c33302c31322c38312c3135352c3230342c31302c3137392c37352c32332c3133382c3139362c3231372c352c31342c32302c35372c37392c33392c3137365d";

/// Mainnet immutable-DB chunk size in slots. Immutable file `N` covers slots
/// `[N * CHUNK_SLOTS, (N + 1) * CHUNK_SLOTS)`; the numbering is continuous
/// across the Byron→Shelley boundary because Byron files were one 21,600-slot
/// epoch each.
pub const CHUNK_SLOTS: u64 = 21_600;

/// The immutable file number that contains `slot`.
pub fn immutable_file_for_slot(slot: u64) -> u64 {
    slot / CHUNK_SLOTS
}

#[derive(clap::Args, Debug)]
pub struct BootstrapArgs {
    /// Where to download + unpack the snapshot. The walk reads
    /// `<download-dir>/immutable`.
    #[arg(long)]
    pub download_dir: PathBuf,

    /// Mithril aggregator endpoint.
    #[arg(long, env = "AGGREGATOR_ENDPOINT", default_value = MAINNET_AGGREGATOR)]
    pub aggregator: String,

    /// Genesis verification key (defaults to release-mainnet).
    #[arg(long, env = "GENESIS_VERIFICATION_KEY", default_value = MAINNET_GENESIS_VKEY)]
    pub genesis_key: String,

    /// Snapshot digest to download (`latest`, or a specific digest).
    #[arg(long, default_value = "latest")]
    pub digest: String,

    /// First immutable file number to download (partial range; see module doc).
    #[arg(long)]
    pub start: Option<u64>,

    /// Last immutable file number to download (partial range; see module doc).
    #[arg(long)]
    pub end: Option<u64>,

    /// Path to the `mithril-client` binary.
    #[arg(long, env = "MITHRIL_CLIENT", default_value = "mithril-client")]
    pub client: String,

    /// Extra arguments passed verbatim to `mithril-client cardano-db download`
    /// (repeatable) — e.g. `--client-arg=--backend --client-arg=v2`.
    #[arg(long = "client-arg")]
    pub client_args: Vec<String>,
}

/// Run `mithril-client cardano-db download <digest> --download-dir <dir>`, with
/// the aggregator + genesis key passed via env (the CLI reads both). Streams the
/// child's stdout/stderr straight through — this is a long, chatty download.
pub fn bootstrap(args: BootstrapArgs) -> Result<()> {
    std::fs::create_dir_all(&args.download_dir)
        .with_context(|| format!("creating download dir {}", args.download_dir.display()))?;

    if let (Some(s), Some(e)) = (args.start, args.end)
        && s > e
    {
        bail!("--start {s} is after --end {e}");
    }

    tracing::info!(
        aggregator = %args.aggregator,
        digest = %args.digest,
        start = ?args.start,
        end = ?args.end,
        dir = %args.download_dir.display(),
        "mithril bootstrap: invoking {}",
        args.client
    );

    let mut cmd = Command::new(&args.client);
    cmd.arg("cardano-db")
        .arg("download")
        .arg(&args.digest)
        .arg("--download-dir")
        .arg(&args.download_dir);
    if let Some(s) = args.start {
        cmd.arg("--start").arg(s.to_string());
    }
    if let Some(e) = args.end {
        cmd.arg("--end").arg(e.to_string());
    }
    for extra in &args.client_args {
        cmd.arg(extra);
    }
    let status = cmd
        .env("AGGREGATOR_ENDPOINT", &args.aggregator)
        .env("GENESIS_VERIFICATION_KEY", &args.genesis_key)
        .status()
        .with_context(|| {
            format!(
                "spawning `{}` (is mithril-client installed / on PATH? override with \
                 --client or $MITHRIL_CLIENT)",
                args.client
            )
        })?;

    if !status.success() {
        bail!("mithril-client exited with {status}");
    }

    let immutable = args.download_dir.join("immutable");
    if !immutable.is_dir() {
        bail!(
            "download succeeded but {} is missing — check the snapshot layout",
            immutable.display()
        );
    }
    tracing::info!(immutable = %immutable.display(), "bootstrap complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn immutable_file_numbering() {
        assert_eq!(immutable_file_for_slot(0), 0);
        assert_eq!(immutable_file_for_slot(21_599), 0);
        assert_eq!(immutable_file_for_slot(21_600), 1);
        // Shelley start (4_492_800) = exactly 208 Byron epochs.
        assert_eq!(immutable_file_for_slot(4_492_800), 208);
    }
}
