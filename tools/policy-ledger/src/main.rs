//! `policy-ledger` — tooling over the POLICY INDEX.
//!
//! The index answers "which transaction minted this" in one `pread`. This tool
//! asks the next question: **what collections exist, and what are they
//! called?** — and publishes the answer as an artifact a command palette can
//! fetch once at startup and then search locally per keystroke.
//!
//! ```text
//! policy-ledger catalogue build   --index-dir … --immutable … --out …
//! policy-ledger catalogue publish --artifact …
//! policy-ledger catalogue inspect --artifact … [--find snek]
//! ```
//!
//! # ⚠️ It reads. It never walks.
//!
//! `base.pidx` is rebuilt daily by `tx-index build --policy-index-dir`, riding
//! an extraction pass that already decodes every block at a MEASURED +1.8%.
//! The catalogue is a READER of that result: it gets its freshness for free
//! and adds no pass. Giving this tool its own walk would cost +100% and buy
//! nothing.
//!
//! Design: `cnft.dev-workers/docs/design/COLLECTION_CATALOGUE.md`.
//! Deploy/build/test loop: `docs/operations/mitos-operations.md`.

mod build;
mod classify;
mod curated;
mod inspect;
mod publish;

use anyhow::Result;
use clap::{Parser, Subcommand};

/// ⚠️ **MAINNET ONLY, and that is fine** (decided 2026-09-15). It is in the
/// key rather than assumed, so a preprod artifact could never silently
/// overwrite this one — the same lesson `reference_iiif_preprod_env` records.
pub const NETWORK: &str = "mainnet";

#[derive(Parser, Debug)]
#[command(
    name = "policy-ledger",
    about = "Artifacts derived from the policy index"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// The collection catalogue: build, publish, inspect.
    #[command(subcommand)]
    Catalogue(CatalogueCommand),
}

#[derive(Subcommand, Debug)]
enum CatalogueCommand {
    /// Derive the catalogue from the index and write `catalogue.bin`.
    Build(build::BuildArgs),
    /// Publish a built catalogue to R2 + KV, IF ITS CONTENT CHANGED.
    Publish(publish::PublishArgs),
    /// Read a built catalogue back and report what is in it.
    Inspect(inspect::InspectArgs),
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Catalogue(CatalogueCommand::Build(a)) => build::run(a),
        Command::Catalogue(CatalogueCommand::Publish(a)) => publish::run(a),
        Command::Catalogue(CatalogueCommand::Inspect(a)) => inspect::run(a),
    }
}
