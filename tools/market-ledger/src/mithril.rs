//! Mithril snapshot bootstrap — shell out to the `mithril-client` CLI (not the
//! SDK) to download + verify a certified immutable DB into a data dir. The walk
//! then reads `<download-dir>/immutable`. Per the design, keep the unpacked DB
//! between runs and refresh only when a re-run needs newer history.

use std::path::PathBuf;
use std::process::Command;

use anyhow::{Context, Result, bail};

/// Release-mainnet aggregator + genesis verification key (public, well-known).
const MAINNET_AGGREGATOR: &str =
    "https://aggregator.release-mainnet.api.mithril.network/aggregator";
const MAINNET_GENESIS_VKEY: &str = "5b3139312c36362c3134302c3138352c3133382c31312c3233372c3230372c3235302c3134342c32372c322c3138382c33302c31322c38312c3135352c3230342c31302c3137392c37352c32332c3133382c3139362c3231372c352c31342c32302c35372c37392c33392c3137365d";

#[derive(clap::Args, Debug)]
pub struct BootstrapArgs {
    /// Where to download + unpack the snapshot. The walk reads
    /// `<download-dir>/immutable`.
    #[arg(long)]
    download_dir: PathBuf,

    /// Mithril aggregator endpoint.
    #[arg(long, env = "AGGREGATOR_ENDPOINT", default_value = MAINNET_AGGREGATOR)]
    aggregator: String,

    /// Genesis verification key (defaults to release-mainnet).
    #[arg(long, env = "GENESIS_VERIFICATION_KEY", default_value = MAINNET_GENESIS_VKEY)]
    genesis_key: String,

    /// Snapshot digest to download (`latest`, or a specific digest).
    #[arg(long, default_value = "latest")]
    digest: String,

    /// Path to the `mithril-client` binary.
    #[arg(long, env = "MITHRIL_CLIENT", default_value = "mithril-client")]
    client: String,
}

/// Run `mithril-client cardano-db download <digest> --download-dir <dir>`, with
/// the aggregator + genesis key passed via env (the CLI reads both). Streams the
/// child's stdout/stderr straight through — this is a long, chatty download.
pub fn bootstrap(args: BootstrapArgs) -> Result<()> {
    std::fs::create_dir_all(&args.download_dir)
        .with_context(|| format!("creating download dir {}", args.download_dir.display()))?;

    tracing::info!(
        aggregator = %args.aggregator,
        digest = %args.digest,
        dir = %args.download_dir.display(),
        "mithril bootstrap: invoking {}",
        args.client
    );

    let status = Command::new(&args.client)
        .arg("cardano-db")
        .arg("download")
        .arg(&args.digest)
        .arg("--download-dir")
        .arg(&args.download_dir)
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
