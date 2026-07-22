//! market-ledger — deep marketplace-history walker + ledger for CNFT venues.
//!
//! One binary, three modes: `walk` (this + the next slices — snapshot walk into
//! a local ledger), `serve` (phase 2, HTTP read surface), `follow` (phase 3,
//! mitos companion tip-follow). Decode stays in `mitos-marketplace-decode`; this
//! tool maps certified chain data into that crate's `DecodeTx` and stores what it
//! returns. NOT a second chain follower — the walk reads Mithril immutable-DB
//! chunk files, never the live redb stores.

mod buffer;
mod decode;
mod metadata;
mod mithril;
mod row;
mod store;
mod venue;
mod walk;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    version,
    about = "Deep marketplace-history walker + ledger for CNFT venues"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Walk certified immutable-DB history and decode marketplace events.
    Walk(walk::WalkArgs),
    /// Download + verify a Mithril snapshot's immutable DB into a data dir.
    Bootstrap(mithril::BootstrapArgs),
    /// Serve the ledger over HTTP (phase 2 — not yet implemented).
    Serve,
    /// Follow the chain tip via the mitos companion protocol (phase 3 — not yet implemented).
    Follow,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    match Cli::parse().command {
        Command::Walk(args) => walk::run(args),
        Command::Bootstrap(args) => mithril::bootstrap(args),
        Command::Serve => anyhow::bail!("`serve` is phase 2 and not yet implemented"),
        Command::Follow => anyhow::bail!("`follow` is phase 3 and not yet implemented"),
    }
}
