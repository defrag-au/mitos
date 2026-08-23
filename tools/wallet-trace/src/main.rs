//! wallet-trace — who is the same hand?
//!
//! Given a `$handle`, an address, or a stake key, produce the cluster of
//! credentials operated by one signer, with the transaction hash that joined
//! each pair. A cluster, never a person — see WALLET_TRACE.md §"What the chain
//! cannot tell you".
//!
//! The design rests on one fact: Cardano requires every key-locked input to be
//! authorised by a signature in the transaction's own witness set, so the
//! co-signing group is readable from block bytes alone. No input resolution, no
//! outref ladder. That makes the walk stateless and forward-only, which in turn
//! makes a CHAIN-WIDE index affordable — built once, reused by every case,
//! rather than re-walked per investigation like `project-ledger`.
//!
//! Modes: `probe` (measure the hit rate — build step 1, and the only mode that
//! exists yet). Coming: `index` (chain-wide co-signing + handle tables),
//! `suppress` (degree census → excluded operator keys), `trace` (union-find
//! query against the index).
//!
//! Design: `cnft.dev-workers/docs/design/WALLET_TRACE.md`.

mod creds;
mod index;
mod probe;
mod store;
mod suppress;
mod trace;
mod witness;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    version,
    about = "Wallet identity clustering from vkey witness sets, over a certified Mithril snapshot"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Download + verify a Mithril snapshot (or a partial immutable-file range).
    ///
    /// Usually skipped: point `probe` at the dir `market-ledger` and
    /// `project-ledger` already share.
    Bootstrap(mitos_chain_walk::mithril::BootstrapArgs),

    /// Measure the co-signing hit rate. Writes nothing.
    ///
    /// MEASURED 2026-08-23: 0.45 rows/tx, stable across four and a half years.
    /// Re-run it on a new window before trusting a projection.
    Probe(probe::ProbeArgs),

    /// Build the chain-wide co-signing index. One forward pass, no input
    /// resolution, no frontier.
    Index(index::IndexArgs),

    /// Census co-signer degree and mark operator keys excluded from clustering.
    ///
    /// Run between `index` and `trace`. Without it one batcher key merges the
    /// entire chain into a single party.
    Suppress(suppress::SuppressArgs),

    /// Expand a seed credential into the wallets that share a hand, with the
    /// transaction hash behind every merge.
    Trace(trace::TraceArgs),
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    match Cli::parse().command {
        Command::Bootstrap(args) => mitos_chain_walk::mithril::bootstrap(args),
        Command::Probe(args) => probe::probe(&args),
        Command::Index(args) => index::index(&args),
        Command::Suppress(args) => suppress::suppress(&args),
        Command::Trace(args) => trace::trace(&args),
    }
}
