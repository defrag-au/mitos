//! `mithril-snapshot` — validate an unpacked Mithril snapshot, publish it, catch it up.
//!
//! - `validate <dir>` reads the directory only — gaps, incomplete triples, strays. It
//!   refuses a structurally broken tree, and it does not pretend to know the boundary.
//! - `verify <dir>` asks the aggregator for the boundary and the digests and reconciles
//!   the tree against both. `--hash` also proves the bytes.
//! - `sync <dir>` fetches the immutable files Mithril publishes that are missing
//!   locally through `mithril-client` as a library, proving them against the snapshot's
//!   certificate in a staging directory and renaming only the verified parts in.
//! - `publish <dir> --dest r2:… [--sync] [--run]` drives rclone, so what reaches R2
//!   behind an immutable cache header is what the aggregator publishes.
//!
//! Every command takes either the database directory or the `immutable/` directory
//! inside it — see [`resolve`].
//!
//! `lib.rs` explains why the boundary cannot come from the directory, `aggregator.rs`
//! the two routes that publish it, and `ingest.rs` what the library verifies.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};

use mithril_snapshot::aggregator::{Aggregator, MAINNET_AGGREGATOR, MAINNET_GENESIS_VKEY};
use mithril_snapshot::ingest;
use mithril_snapshot::{Inventory, PART_EXTS, Reconciliation, Report, lift, verify_parts};

#[derive(Parser)]
#[command(
    name = "mithril-snapshot",
    about = "Validate an unpacked Mithril snapshot, catch it up, and publish it to R2",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Check a snapshot's structure on disk, without asking Mithril anything.
    ///
    /// Reports gaps, incomplete triples and stray files, and refuses a tree that is not
    /// publishable. It cannot tell where Mithril's boundary is: Mithril's own rule is
    /// relative to the directory, so "the last file" is a different file in every tree.
    /// `verify` replaces that guess with the published boundary.
    Validate {
        /// The database directory, or the immutable directory inside it.
        dir: PathBuf,
    },

    /// Reconcile a snapshot against what Mithril has published.
    ///
    /// Reports the aggregator's boundary, which local parts are publishable, which
    /// numbers Mithril publishes that are absent locally, and — with `--hash` — whether
    /// our bytes are the published ones.
    Verify {
        /// The database directory, or the immutable directory inside it.
        dir: PathBuf,
        /// Aggregator endpoint.
        #[arg(long)]
        aggregator: Option<String>,
        /// SHA-256 every local part and compare it against the published digest.
        ///
        /// Streaming, so memory stays flat, but it reads the whole tree and is a real
        /// CPU load — build `--release` before trusting the timing, and see `--jobs`.
        #[arg(long)]
        hash: bool,
        /// Files to hash at once [default: available parallelism].
        #[arg(long)]
        jobs: Option<usize>,
    },

    /// Fetch the immutable files Mithril publishes that are missing locally.
    ///
    /// Ingests through `mithril-client` as a LIBRARY rather than the CLI: the range is
    /// downloaded into a staging directory and proved against the snapshot's certificate
    /// there, so only verified parts reach the database. `mithril_snapshot::ingest` has
    /// the sequence, and the one step of the CLI's deliberately not reproduced.
    Sync {
        /// The database directory, or the immutable directory inside it.
        dir: PathBuf,
        /// Aggregator endpoint.
        #[arg(long)]
        aggregator: Option<String>,
        /// Genesis verification key, which the certificate chain is checked against.
        #[arg(long, env = "GENESIS_VERIFICATION_KEY")]
        genesis_key: Option<String>,
        /// Where a fetch is staged and verified [default: beside the database].
        ///
        /// Only verified parts are renamed out of it, so it is the only directory a
        /// failed transfer can damage.
        #[arg(long)]
        staging: Option<PathBuf>,
        /// Fetch, verify and move in — instead of printing what would be fetched.
        #[arg(long)]
        run: bool,
    },

    /// Print — or run — the rclone invocation that publishes a snapshot to R2.
    ///
    /// Excludes any local part Mithril publishes no digest for, so nothing reaches R2
    /// behind an immutable cache header without the aggregator's own claim that it is
    /// immutable.
    Publish {
        /// The database directory, or the immutable directory inside it.
        dir: PathBuf,
        /// rclone destination, e.g. r2:mithril-snapshots/mainnet
        ///
        /// A STABLE prefix: a newer snapshot is a superset of sealed chunks, so
        /// versioning the prefix by immutable number would re-upload the whole tree.
        #[arg(long)]
        dest: String,
        /// Catch the database up to Mithril before uploading — everything `sync` does,
        /// with the default genesis key and staging directory.
        #[arg(long)]
        sync: bool,
        /// Execute instead of printing: the fetch if `--sync` was given, then rclone.
        #[arg(long)]
        run: bool,
        /// Aggregator endpoint.
        #[arg(long)]
        aggregator: Option<String>,
    },
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Cmd::Validate { dir } => validate(&dir),
        Cmd::Verify {
            dir,
            aggregator,
            hash,
            jobs,
        } => verify(&dir, aggregator, hash, jobs),
        Cmd::Sync {
            dir,
            aggregator,
            genesis_key,
            staging,
            run,
        } => sync(
            &dir,
            aggregator,
            &genesis_key.unwrap_or_else(|| MAINNET_GENESIS_VKEY.to_string()),
            staging,
            run,
        ),
        Cmd::Publish {
            dir,
            dest,
            sync,
            run,
            aggregator,
        } => publish(&dir, &dest, sync, run, aggregator),
    }
}

fn endpoint(aggregator: Option<String>) -> String {
    aggregator.unwrap_or_else(|| MAINNET_AGGREGATOR.to_string())
}

/// The immutable directory inside `dir`, and the database directory that holds it.
///
/// Every command takes either, because `mithril-client` writes a *database* directory
/// (the one containing `immutable/`) while everything this tool reads lives in
/// `immutable/`. Making the caller remember which verb wants which is the kind of
/// friction that ends in the wrong path being handed over.
fn resolve(dir: &Path) -> (PathBuf, PathBuf) {
    let nested = dir.join("immutable");
    if nested.is_dir() {
        (nested, dir.to_path_buf())
    } else {
        let db_dir = dir
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| dir.to_path_buf());

        (dir.to_path_buf(), db_dir)
    }
}

fn validate(dir: &Path) -> Result<()> {
    let (immutable, _) = resolve(dir);
    let inventory = Inventory::scan(&immutable)?;
    if inventory.parts.is_empty() {
        bail!("no immutable files in {}", immutable.display());
    }
    let report = Report::build(&inventory);

    println!("snapshot   {}", immutable.display());
    println!(
        "range      {} — {} numbers, {} parts",
        range_of(inventory.numbers()),
        inventory.parts.len(),
        inventory.part_count()
    );

    if report.gaps.is_empty() {
        println!("gaps       none");
    } else {
        println!("gaps       {}", report.gaps.len());
        for gap in &report.gaps {
            println!("           {:05}–{:05} absent", gap.from, gap.to);
        }
    }

    if report.incomplete.is_empty() {
        println!("triples    complete");
    } else {
        println!("triples    {} incomplete", report.incomplete.len());
        for incomplete in &report.incomplete {
            println!(
                "           {:05} missing {}",
                incomplete.number,
                incomplete.missing.join(", ")
            );
        }
    }

    println!("strays     {}", report.strays.len());
    for path in &report.strays {
        println!("           {}", path.display());
    }

    if !report.ok() {
        bail!("snapshot is not publishable — see the lines above");
    }

    // The offline view, stated as a guess rather than a fact. Nothing above is wrong;
    // it is incomplete, and saying so is the point.
    println!();
    println!("offline    a directory cannot name its own boundary. Mithril's rule is relative to");
    println!("           it — `filter(|f| f.number < last_number)` — so the last file is a");
    println!("           different file in every tree, and holding it back takes a sealed chunk");
    println!("           off a restored snapshot instead of protecting anything.");
    if let Some(number) = report.highest {
        println!(
            "           taken as unsealed: {number:05} — {} of {} numbers would go up unproven.",
            report.finalized.len(),
            inventory.parts.len()
        );
    }
    println!("           `verify` settles that against what Mithril publishes.");

    Ok(())
}

fn verify(dir: &Path, aggregator: Option<String>, hash: bool, jobs: Option<usize>) -> Result<()> {
    let (immutable, db_dir) = resolve(dir);
    let inventory = Inventory::scan(&immutable)?;
    if inventory.parts.is_empty() {
        bail!("no immutable files in {}", immutable.display());
    }
    let report = Report::build(&inventory);

    let client = Aggregator::new(endpoint(aggregator))?;
    println!("snapshot    {}", immutable.display());
    println!("aggregator  {}", client.base());

    let snapshots = client.snapshots()?;
    let latest = snapshots
        .first()
        .context("the aggregator listed no snapshots to compare against")?;
    println!(
        "beacon      {:05} — epoch {}, cardano-node {}, created {}, snapshot {}",
        latest.beacon.immutable_file_number,
        latest.beacon.epoch,
        latest.cardano_node_version,
        latest.created_at,
        short(&latest.hash)
    );

    let digests = client.digests()?;
    let recon = Reconciliation::build(&inventory, &digests);
    let sealed = digests.sealed_numbers();

    println!(
        "local       {} — {} numbers, {} parts",
        range_of(inventory.numbers()),
        inventory.parts.len(),
        recon.local_parts
    );
    println!(
        "published   {} — {} numbers with all {} parts, {} digests in the map",
        range_of(sealed.iter().copied()),
        sealed.len(),
        PART_EXTS.len(),
        digests.len()
    );
    println!();

    let hold_back = recon.hold_back();
    if hold_back.is_empty() {
        println!("held back   none — every local part carries a published digest");
    } else {
        println!(
            "held back   {} part(s) Mithril publishes no digest for:",
            hold_back.len()
        );
        for name in &hold_back {
            println!("              {name}");
        }
    }

    let runs = recon.fetch_runs(latest.beacon.immutable_file_number);
    if runs.is_empty() {
        println!("missing     none — Mithril publishes nothing this tree lacks");
    } else {
        println!(
            "missing     {} number(s) Mithril publishes that are absent locally:",
            runs.iter().map(|run| run.to - run.from + 1).sum::<u64>()
        );
        for run in &runs {
            println!("              {:05}–{:05}", run.from, run.to);
        }
        println!("            `sync {}` fetches them", db_dir.display());
    }

    if recon.incomplete_published.is_empty() {
        println!("triples     every number Mithril publishes has all its parts here");
    } else {
        println!(
            "triples     {} number(s) inside Mithril's published range are missing a part:",
            recon.incomplete_published.len()
        );
        for incomplete in &recon.incomplete_published {
            println!(
                "              {:05} missing {}",
                incomplete.number,
                incomplete.missing.join(", ")
            );
        }
    }

    // Gates before the expensive pass, so a tree that is not a snapshot does not spend
    // eight minutes being hashed.
    if !report.ok() {
        bail!(
            "snapshot is not structurally publishable — run `mithril-snapshot validate {}`",
            db_dir.display()
        );
    }
    let unexplained = recon.unexplained_holds();
    if !unexplained.is_empty() {
        bail!(
            "{} number(s) below the top have no published digest ({}) — this tree is ahead of \
             Mithril, or mixed from two sources, which is not something to publish around",
            unexplained.len(),
            numbers(&unexplained)
        );
    }
    if !recon.incomplete_published.is_empty() {
        bail!(
            "{} number(s) inside Mithril's published range are missing a part — see `triples` above",
            recon.incomplete_published.len()
        );
    }

    println!();
    if hash {
        let jobs = jobs.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        });
        println!(
            "hashing     {} part(s) with a published digest, {jobs} at a time, streaming",
            recon.published.len()
        );
        let started = std::time::Instant::now();
        let mismatches = verify_parts(&inventory, &digests, jobs)?;

        if mismatches.is_empty() {
            println!(
                "digests     all {} parts match what Mithril publishes ({:?})",
                recon.published.len(),
                started.elapsed()
            );
        } else {
            for mismatch in &mismatches {
                println!("            {}", mismatch.name);
                println!("              published {}", mismatch.published);
                println!("              local     {}", mismatch.local);
            }
            bail!(
                "{} of {} parts do not match the published digest — this copy is not the \
                 published one; fetch it again rather than uploading it",
                mismatches.len(),
                recon.published.len()
            );
        }
    } else {
        println!(
            "digests     not checked — add --hash to SHA-256 all {} published parts",
            recon.published.len()
        );
    }

    Ok(())
}

/// Fetch the missing numbers, one run at a time, verified in a staging directory.
fn sync(
    dir: &Path,
    aggregator: Option<String>,
    genesis_key: &str,
    staging: Option<PathBuf>,
    run: bool,
) -> Result<()> {
    let (immutable, db_dir) = resolve(dir);
    if !immutable.is_dir() {
        bail!(
            "{} is not a directory — `sync` catches an existing database up. For a first \
             restore, fetch the whole range once with `mithril-client cardano-db download \
             latest --download-dir {}`",
            immutable.display(),
            db_dir.display()
        );
    }
    let inventory = Inventory::scan(&immutable)?;

    let endpoint = endpoint(aggregator);
    let api = Aggregator::new(endpoint.clone())?;
    let snapshots = api.snapshots()?;
    let latest = snapshots
        .first()
        .context("the aggregator listed no snapshots to fetch from")?;
    let boundary = latest.beacon.immutable_file_number;
    let digests = api.digests()?;
    let recon = Reconciliation::build(&inventory, &digests);
    let sealed = digests.sealed_numbers();

    println!("database    {}", db_dir.display());
    println!(
        "local       {} — {} numbers, {} parts",
        range_of(inventory.numbers()),
        inventory.parts.len(),
        recon.local_parts
    );
    println!(
        "published   {} — {} numbers, {} parts",
        range_of(sealed.iter().copied()),
        sealed.len(),
        digests.len()
    );
    println!(
        "beacon      {boundary:05} — epoch {}, created {}",
        latest.beacon.epoch, latest.created_at
    );
    println!();

    let runs = recon.fetch_runs(boundary);
    if runs.is_empty() {
        println!("up to date  nothing Mithril publishes is missing");
        return Ok(());
    }

    println!(
        "behind      {} number(s):",
        runs.iter().map(|gap| gap.to - gap.from + 1).sum::<u64>()
    );
    for gap in &runs {
        println!("              {:05}–{:05}", gap.from, gap.to);
    }
    println!();

    // A sibling of the database, so the rename into it stays within one filesystem.
    let staging = staging.unwrap_or_else(|| {
        db_dir
            .parent()
            .unwrap_or(&db_dir)
            .join(".mithril-sync-staging")
    });

    if !run {
        println!("staging     {}", staging.display());
        println!();
        println!("(re-run with --run to fetch, verify and move them in)");
        return Ok(());
    }

    println!();
    let runtime = tokio::runtime::Runtime::new().context("starting a tokio runtime")?;
    let jobs = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    for gap in &runs {
        // A fresh staging directory per run: `download_unpack` leaves `allow_override`
        // false, so it refuses a directory that already holds an `immutable/`.
        clear_staging(&staging, &db_dir)?;
        fs::create_dir_all(&staging).with_context(|| format!("creating {}", staging.display()))?;

        let ingested = runtime.block_on(ingest::fetch(
            &endpoint,
            genesis_key,
            &staging,
            gap.from,
            gap.to,
        ))?;
        println!(
            "ingested    {:05}–{:05} — snapshot {}, certificate {}, beacon {:05}",
            gap.from,
            gap.to,
            short(&ingested.snapshot_hash),
            short(&ingested.certificate_hash),
            ingested.beacon
        );

        // Independent of the certificate, and the check that governs what gets served:
        // the bytes against the digest map the aggregator publishes.
        let staged = Inventory::scan(&staging.join("immutable"))?;
        let mismatches = verify_parts(&staged, &digests, jobs)?;
        if !mismatches.is_empty() {
            for mismatch in &mismatches {
                println!("              {}", mismatch.name);
                println!("                published {}", mismatch.published);
                println!("                local     {}", mismatch.local);
            }
            bail!(
                "{} of {} ingested part(s) do not match the published digest — nothing was \
                 moved into {}, which is untouched",
                mismatches.len(),
                staged.part_count(),
                immutable.display()
            );
        }

        let moved = lift(&staging.join("immutable"), &immutable)?;
        println!(
            "moved       {} part(s) into {}",
            moved.len(),
            immutable.display()
        );
        clear_staging(&staging, &db_dir)?;
    }

    let after = Inventory::scan(&immutable)?;
    let still = Reconciliation::build(&after, &digests).fetch_runs(boundary);
    println!();
    println!(
        "local       {} — {} numbers, {} parts",
        range_of(after.numbers()),
        after.parts.len(),
        after.part_count()
    );
    if still.is_empty() {
        println!("up to date  caught up to {boundary:05}");
    } else {
        println!(
            "still       {} number(s) behind — re-run sync",
            still.iter().map(|gap| gap.to - gap.from + 1).sum::<u64>()
        );
    }

    Ok(())
}

fn publish(
    dir: &Path,
    dest: &str,
    sync_first: bool,
    run: bool,
    aggregator: Option<String>,
) -> Result<()> {
    if sync_first {
        sync(dir, aggregator.clone(), MAINNET_GENESIS_VKEY, None, run)?;
        println!();
    }

    let (immutable, _) = resolve(dir);
    let inventory = Inventory::scan(&immutable)?;
    if inventory.parts.is_empty() {
        bail!("no immutable files in {}", immutable.display());
    }
    let report = Report::build(&inventory);
    if !report.ok() {
        bail!(
            "refusing to publish {} — run `mithril-snapshot validate {}` first",
            immutable.display(),
            immutable.display()
        );
    }

    let client = Aggregator::new(endpoint(aggregator))?;
    let digests = client.digests()?;
    let recon = Reconciliation::build(&inventory, &digests);

    let unexplained = recon.unexplained_holds();
    if !unexplained.is_empty() {
        bail!(
            "refusing to publish: {} number(s) below the top have no published digest ({})",
            unexplained.len(),
            numbers(&unexplained)
        );
    }
    if !recon.incomplete_published.is_empty() {
        bail!(
            "refusing to publish: {} number(s) inside Mithril's published range are missing a part",
            recon.incomplete_published.len()
        );
    }

    let hold_back = recon.hold_back();
    if hold_back.is_empty() {
        println!(
            "publishing  every local part carries a published digest ({} of {} parts)",
            recon.published.len(),
            recon.local_parts
        );
    } else {
        println!(
            "holding back {} part(s) Mithril publishes no digest for:",
            hold_back.len()
        );
        for name in &hold_back {
            println!("            {name}");
        }
    }
    if recon.missing_count() > 0 {
        println!(
            "note        Mithril publishes {} number(s) this tree lacks — `verify` lists them",
            recon.missing_count()
        );
    }
    println!();

    let argv = rclone_argv(&immutable, dest, &hold_back);
    println!("{}", shell_line(&argv));
    println!();
    println!("note  R2 does not implement version-qualified HEAD, and rclone verifies with");
    println!("      one — so every object logs `NotImplemented: 501 ... Attempt 1/5 failed`");
    println!("      and the run reads like a failed upload. It is not. Set `no_head` on the");
    println!("      remote: RCLONE_CONFIG_R2_NO_HEAD=true, or `no_head = true` under [r2]");
    println!("      in rclone.conf. tools/token-ledger/push-artifacts.sh traced this.");

    if run {
        let status = Command::new(&argv[0])
            .args(&argv[1..])
            .status()
            .with_context(|| format!("running {}", argv[0]))?;
        if !status.success() {
            bail!("rclone exited with {status}");
        }
    } else {
        println!();
        println!("(re-run with --run to execute)");
    }

    Ok(())
}

/// Remove a staging directory, refusing anything that is not one.
///
/// A staging directory is the one thing here that may be deleted, and only because the
/// next run creates it again. It is never the database, and never an ancestor of it —
/// both of which would turn a cleanup into a data loss.
fn clear_staging(staging: &Path, db_dir: &Path) -> Result<()> {
    if staging == db_dir || db_dir.starts_with(staging) {
        bail!(
            "refusing to remove {} — it is, or contains, the database at {}",
            staging.display(),
            db_dir.display()
        );
    }
    if !staging.exists() {
        return Ok(());
    }

    fs::remove_dir_all(staging).with_context(|| format!("removing {}", staging.display()))
}

/// The spanning range a set of numbers covers, `00000–09202`.
///
/// Both callers hand over ascending numbers, so the first and last are the bounds.
fn range_of(numbers: impl IntoIterator<Item = u64>) -> String {
    let numbers: Vec<u64> = numbers.into_iter().collect();
    match (numbers.first(), numbers.last()) {
        (Some(first), Some(last)) => format!("{first:05}–{last:05}"),
        _ => "none".to_string(),
    }
}

fn numbers(numbers: &[u64]) -> String {
    numbers
        .iter()
        .map(|number| format!("{number:05}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The first twelve characters of a hash, enough to name a snapshot.
fn short(hash: &str) -> String {
    hash.chars().take(12).collect()
}

/// The proven R2 invocation: three file types, all binary, all immutable.
///
/// `--s3-chunk-size` is part of the contract rather than tuning — rclone can only
/// re-compute a multipart ETag if this matches what the upload used.
fn rclone_argv(dir: &Path, dest: &str, excludes: &[&str]) -> Vec<String> {
    let mut argv = vec!["rclone".to_string(), "copy".to_string()];
    argv.push(dir.display().to_string());
    argv.push(dest.to_string());

    // Anchored to the transfer root, so a bare name cannot match a deeper path.
    for name in excludes {
        argv.push("--exclude".to_string());
        argv.push(format!("/{name}"));
    }

    for arg in [
        "--header-upload",
        "Content-Type: application/octet-stream",
        "--header-upload",
        "Cache-Control: public, max-age=31536000, immutable",
        "--transfers",
        "16",
        "--checkers",
        "16",
        // A newer snapshot is a SUPERSET of sealed chunks, byte-identical
        // (`ARTIFACT_SHARDING.md` §1), so a file already at the destination is
        // the file we would upload. Skipping it by name is both faster and
        // stricter than rclone's size+modtime comparison.
        "--ignore-existing",
        "--s3-chunk-size",
        "16M",
        "--retries",
        "5",
        "--progress",
    ] {
        argv.push(arg.to_string());
    }

    argv
}

/// Quote only what needs it, so an argument holding a space (`Content-Type: …`)
/// survives a copy-paste into a shell.
fn shell_line(argv: &[String]) -> String {
    argv.iter()
        .map(|arg| {
            if arg.contains(' ') {
                format!("\"{arg}\"")
            } else {
                arg.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}
