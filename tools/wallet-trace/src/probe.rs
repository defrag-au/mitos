//! `probe` — build step 1 of `WALLET_TRACE.md`: measure before building.
//!
//! The whole index design turns on one number nobody in this workspace knows:
//! **what fraction of mainnet transactions have 2..N distinct signing keys?**
//! Single-signature payments dominate the chain and contribute nothing; the
//! interesting minority is the entire cost. At one end the artifact is a couple
//! of hundred megabytes and a worker can consume it; at the other it is tens of
//! gigabytes and it cannot.
//!
//! The precedent for not skipping this is in the same family of docs:
//! `PROJECT_LEDGER_IMPORTER.md` priced its input-resolution ladder at ~10,000
//! refs and measured 2,786,844 — wrong by more than two orders of magnitude,
//! discovered only after the build.
//!
//! This pass writes NOTHING. It decodes, counts, and reports.
//!
//! ## Reading the output honestly
//!
//! Transaction density varies enormously across chain history — an immutable
//! file from the Byron era is nearly empty, one from a busy 2025 stretch is
//! not. **A projection from a single file is an order-of-magnitude estimate at
//! best.** Probe three widely separated windows before believing any total; the
//! report says so itself rather than trusting the reader to remember.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use mitos_chain_walk::mithril::CHUNK_SLOTS;
use mitos_chain_walk::open_blocks;
use pallas_traverse::MultiEraBlock;

use crate::witness::{KeyHash, signer_keys};

/// Distinct-signer counts are bucketed exactly up to here, then lumped into an
/// overflow bin. 128 is far above any plausible `--max-group`, so every group
/// size the design might actually adopt is exact.
const HIST_MAX: usize = 128;

/// Group sizes the report prices, so the cap can be chosen from data rather
/// than taste.
const GROUP_CAPS: [usize; 5] = [2, 4, 8, 16, 32];

/// Per-key co-signer sets stop growing here. A key with this many distinct
/// co-signers is definitively an operator key, and the exact figure past that
/// point changes no decision — but an uncapped set on a batcher would grow
/// without bound and take the probe's memory with it.
const COSIGNER_CAP: usize = 4_096;

#[derive(clap::Args, Debug)]
pub struct ProbeArgs {
    /// Snapshot root; the scan reads `<data-dir>/immutable`.
    #[arg(long, required_unless_present = "block_files")]
    pub data_dir: Option<PathBuf>,

    /// Directory of `*.block.cbor` fixtures, searched recursively.
    ///
    /// For validating the decode path without a snapshot. These fixtures were
    /// hand-picked for interesting behaviour (DEX swaps, mints, batches), so
    /// they are emphatically NOT a representative sample — the report refuses
    /// to project from them.
    #[arg(long, conflicts_with = "data_dir")]
    pub block_files: Option<PathBuf>,

    /// Slot to start at. Omitted = genesis, which is almost never what you
    /// want for a probe — the early chain is nearly empty and will flatter the
    /// hit rate badly.
    #[arg(long)]
    pub from_slot: Option<u64>,

    /// Stop after this slot.
    #[arg(long)]
    pub to_slot: Option<u64>,

    /// Stop after this many immutable files' worth of slots (21,600 each),
    /// counted from the first block actually seen. The unit the design doc
    /// talks in.
    #[arg(long, default_value_t = 1)]
    pub files: u64,

    /// Report the top N keys by co-signer degree — the batcher check.
    #[arg(long, default_value_t = 20)]
    pub top_keys: usize,

    /// Group-size cap used for the degree census (not for the row histogram,
    /// which prices every cap in `GROUP_CAPS`).
    #[arg(long, default_value_t = 8)]
    pub max_group: usize,

    /// Bytes per `cosign` row, all-in, for the size projection. Default 180
    /// assumes a `WITHOUT ROWID` primary key over (tx_hash, key_hash, slot)
    /// plus the `cosign_by_key` secondary index, with btree overhead. Adjust
    /// once a real table exists rather than trusting this.
    #[arg(long, default_value_t = 180)]
    pub bytes_per_row: u64,

    /// Immutable files in the full chain, for the per-file projection. 9,058
    /// on the cardano-infra snapshot as of 2026-08-23; override as it grows.
    #[arg(long, default_value_t = 9_058)]
    pub chain_files: u64,

    /// Total mainnet transactions, for the per-TX projection.
    ///
    /// MEASURED 2026-08-23: rows-per-TX is stable to within ~20% across five
    /// years of chain history, while transactions-per-immutable-file varies by
    /// 4×. So scaling by a transaction count is the robust projection and
    /// scaling by file count is not — the per-file figure silently extrapolates
    /// whatever local density the probed window happened to have.
    ///
    /// 0 = skip. Get the real figure from an explorer rather than guessing.
    #[arg(long, default_value_t = 0)]
    pub chain_txs: u64,
}

#[derive(Default)]
struct KeyStat {
    txs: u64,
    cosigners: HashSet<KeyHash>,
    capped: bool,
}

#[derive(Default)]
struct Stats {
    blocks: u64,
    txs: u64,
    first_slot: Option<u64>,
    last_slot: u64,
    /// Index = distinct signing keys, clamped into the overflow bin.
    hist: Vec<u64>,
    /// Transactions carrying Byron bootstrap witnesses (excluded from keys).
    bootstrap_txs: u64,
    /// Transactions with bootstrap witnesses and NO vkey witnesses at all —
    /// wholly invisible to witness clustering. This is the coverage gap the
    /// Byron exclusion actually costs, and it is a number, not a footnote.
    bootstrap_only_txs: u64,
    degree: HashMap<KeyHash, KeyStat>,
}

impl Stats {
    fn new() -> Self {
        Self {
            hist: vec![0; HIST_MAX + 1],
            ..Default::default()
        }
    }

    fn observe(&mut self, tx: &pallas_traverse::MultiEraTx<'_>, slot: u64, max_group: usize) {
        self.txs += 1;
        let signers = signer_keys(tx);
        if signers.bootstrap > 0 {
            self.bootstrap_txs += 1;
            if signers.is_empty() {
                self.bootstrap_only_txs += 1;
            }
        }
        self.hist[signers.len().min(HIST_MAX)] += 1;

        if !signers.is_group() || signers.len() > max_group {
            return;
        }
        let keys: Vec<KeyHash> = signers.keys.iter().copied().collect();
        for k in &keys {
            let e = self.degree.entry(*k).or_default();
            e.txs += 1;
            for other in &keys {
                if other == k {
                    continue;
                }
                if e.cosigners.len() >= COSIGNER_CAP {
                    e.capped = true;
                    break;
                }
                e.cosigners.insert(*other);
            }
        }
        let _ = slot;
    }

    /// Rows the `cosign` table would hold at a given group cap: a transaction
    /// with `n` distinct keys writes `n` rows, and only when `2 <= n <= cap`.
    fn rows_at(&self, cap: usize) -> u64 {
        (2..=cap.min(HIST_MAX))
            .map(|n| self.hist[n] * n as u64)
            .sum()
    }

    fn group_txs_at(&self, cap: usize) -> u64 {
        (2..=cap.min(HIST_MAX)).map(|n| self.hist[n]).sum()
    }

    fn slots_covered(&self) -> u64 {
        match self.first_slot {
            Some(f) if self.last_slot >= f => self.last_slot - f + 1,
            _ => 0,
        }
    }
}

pub fn probe(args: &ProbeArgs) -> Result<()> {
    if args.max_group < 2 {
        bail!("--max-group must be at least 2; a group of one joins nothing");
    }
    let mut stats = Stats::new();
    let from_fixtures = args.block_files.is_some();

    // Bound the borrow of the immutable dir to this scope by naming it first.
    let immutable_dir = args.data_dir.as_ref().map(|d| d.join("immutable"));
    let blocks: Box<dyn Iterator<Item = Result<Vec<u8>>>> =
        match (&immutable_dir, &args.block_files) {
            (Some(dir), _) => {
                // An EMPTY hash is a slot-only FUZZY seek: it binary-searches the
                // chunk list instead of decoding everything below the floor, which
                // is over an hour of CPU on mainnet.
                let it = open_blocks(dir, args.from_slot.map(|s| (s, Vec::new())))
                    .context("opening the immutable DB for a probe")?;
                Box::new(it.map(|b| b.map_err(|e| anyhow::anyhow!("reading block: {e:?}"))))
            }
            (None, Some(dir)) => {
                let files = fixture_files(dir)?;
                if files.is_empty() {
                    bail!("no *.block.cbor found under {}", dir.display());
                }
                tracing::info!(count = files.len(), "probe: reading block fixtures");
                Box::new(files.into_iter().map(|p| {
                    std::fs::read(&p).with_context(|| format!("reading fixture {}", p.display()))
                }))
            }
            (None, None) => bail!("pass --data-dir or --block-files"),
        };

    let span_limit = args.files.saturating_mul(CHUNK_SLOTS);
    for block in blocks {
        let bytes = block?;
        let blk = MultiEraBlock::decode(&bytes)
            .map_err(|e| anyhow::anyhow!("decoding block at ~#{}: {e:?}", stats.blocks))?;
        let slot = blk.slot();

        if let Some(t) = args.to_slot
            && slot > t
        {
            break;
        }
        // Fixtures are unordered and scattered across chain history, so a slot
        // span means nothing there — only the immutable DB is sequential.
        if !from_fixtures {
            match stats.first_slot {
                None => stats.first_slot = Some(slot),
                Some(f) if span_limit > 0 && slot.saturating_sub(f) >= span_limit => break,
                _ => {}
            }
        } else if stats.first_slot.is_none() {
            stats.first_slot = Some(slot);
        }

        stats.blocks += 1;
        stats.last_slot = stats.last_slot.max(slot);
        for tx in blk.txs() {
            stats.observe(&tx, slot, args.max_group);
        }

        if stats.blocks.is_multiple_of(5_000) {
            tracing::info!(
                blocks = stats.blocks,
                txs = stats.txs,
                slot,
                "probe: scanning"
            );
        }
    }

    report(&stats, args, from_fixtures);
    Ok(())
}

/// Every `*.block.cbor` under `dir`, deduplicated by file name.
///
/// Fixtures are named `<slot>.block.cbor` and the same block is deliberately
/// copied into several modules' fixture sets. Counting it once per copy would
/// silently weight whichever blocks happen to be popular test material — which
/// is exactly the bias this mode is already vulnerable to.
fn fixture_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).with_context(|| format!("reading {}", d.display()))? {
            let path = entry?.path();
            if path.is_dir() {
                // `target/` holds build artifacts, not fixtures, and descending
                // into it turns a fast scan into a multi-gigabyte crawl.
                if path.file_name().is_some_and(|n| n == "target") {
                    continue;
                }
                stack.push(path);
            } else if path.to_string_lossy().ends_with(".block.cbor")
                && let Some(name) = path.file_name()
                && seen.insert(name.to_owned())
            {
                out.push(path);
            }
        }
    }
    out.sort();
    Ok(out)
}

fn pct(n: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        n as f64 * 100.0 / total as f64
    }
}

fn report(s: &Stats, args: &ProbeArgs, from_fixtures: bool) {
    let t = s.txs;
    println!();
    println!("wallet-trace probe");
    println!("  blocks {}   txs {}", s.blocks, t);
    if let Some(f) = s.first_slot {
        if from_fixtures {
            // Fixtures are scattered across chain history, so the span between
            // the lowest and highest is not a window and must not be read as
            // one — printing "≈ 884 immutable files" here would be a lie.
            println!("  slots  {}..{}  (SCATTERED, not a window)", f, s.last_slot);
        } else {
            println!(
                "  slots  {}..{}  ({} slots ≈ {:.2} immutable files)",
                f,
                s.last_slot,
                s.slots_covered(),
                s.slots_covered() as f64 / CHUNK_SLOTS as f64
            );
        }
    }
    if t == 0 {
        println!("\n  no transactions seen — nothing to measure");
        return;
    }

    println!("\n  distinct signing keys per tx");
    let buckets: [(&str, usize, usize); 8] = [
        ("0", 0, 0),
        ("1", 1, 1),
        ("2", 2, 2),
        ("3", 3, 3),
        ("4-8", 4, 8),
        ("9-16", 9, 16),
        ("17-32", 17, 32),
        ("33+", 33, HIST_MAX),
    ];
    for (label, lo, hi) in buckets {
        let n: u64 = (lo..=hi).map(|i| s.hist[i]).sum();
        let note = match lo {
            0 => "  script-only / Byron",
            1 => "  contributes nothing",
            _ => "",
        };
        println!("  {label:>6} | {n:>10}  {:>6.2}%{note}", pct(n, t));
    }
    if s.bootstrap_txs > 0 {
        println!(
            "\n  Byron bootstrap witnesses (EXCLUDED from keys by design — see witness.rs)\n  \
             {:>10} tx(s) carried one  ({:.2}%)\n  \
             {:>10} tx(s) had NO vkey witness at all ({:.2}%) — INVISIBLE to clustering",
            s.bootstrap_txs,
            pct(s.bootstrap_txs, t),
            s.bootstrap_only_txs,
            pct(s.bootstrap_only_txs, t)
        );
    }

    println!("\n  cosign rows written, by --max-group");
    for cap in GROUP_CAPS {
        let rows = s.rows_at(cap);
        let gtxs = s.group_txs_at(cap);
        println!(
            "  {cap:>6} | {rows:>10} rows   {:>6.3} rows/tx   ({} txs, {:.2}% of all)",
            rows as f64 / t as f64,
            gtxs,
            pct(gtxs, t)
        );
    }

    if from_fixtures {
        println!(
            "\n  NO PROJECTION: fixtures are hand-picked for interesting behaviour\n  \
             (DEX swaps, mints, batches) and over-represent multi-signature txs by\n  \
             an unknown factor. This run validates the DECODE PATH, not the rate."
        );
    } else {
        // Measured across three windows spanning 2021→2026: rows-per-TX moved
        // only 0.41→0.51, while transactions-per-file moved 5,800→23,700. So a
        // per-TX projection is the trustworthy one and a per-file projection
        // mostly reports how busy the probed window happened to be.
        if args.chain_txs > 0 {
            println!(
                "\n  projection over {} mainnet txs (ROBUST — rows/tx is stable)",
                args.chain_txs
            );
            for cap in GROUP_CAPS {
                let total = (s.rows_at(cap) as f64 / t as f64) * args.chain_txs as f64;
                let gb = total * args.bytes_per_row as f64 / 1e9;
                println!("  {cap:>6} | {total:>14.0} rows   ≈ {gb:>8.1} GB");
            }
            println!(
                "  (at --bytes-per-row {}; see the flag's help for what it assumes)",
                args.bytes_per_row
            );
        }

        let files = s.slots_covered() as f64 / CHUNK_SLOTS as f64;
        if files > 0.0 {
            println!(
                "\n  projection over {} immutable files (WEAK — extrapolates this\n  \
                 window's tx density, which varies 4× across chain history)",
                args.chain_files
            );
            for cap in GROUP_CAPS {
                let total = (s.rows_at(cap) as f64 / files) * args.chain_files as f64;
                let gb = total * args.bytes_per_row as f64 / 1e9;
                println!("  {cap:>6} | {total:>14.0} rows   ≈ {gb:>8.1} GB");
            }
        }
        if args.chain_txs == 0 {
            println!(
                "\n  Pass --chain-txs <total mainnet txs> for the projection that\n  \
                 actually holds up. This window covers {files:.2} file(s)."
            );
        }
    }

    println!(
        "\n  top {} keys by co-signer degree (groups of 2..={})",
        args.top_keys, args.max_group
    );
    let mut keys: Vec<(&KeyHash, &KeyStat)> = s.degree.iter().collect();
    keys.sort_by(|a, b| {
        b.1.cosigners
            .len()
            .cmp(&a.1.cosigners.len())
            .then(b.1.txs.cmp(&a.1.txs))
    });
    if keys.is_empty() {
        println!("     (no groups found in this window)");
    }
    for (i, (k, st)) in keys.iter().take(args.top_keys).enumerate() {
        println!(
            "  {:>4}. {}…  degree {:>6}{}  in {:>6} txs",
            i + 1,
            &hex::encode(k)[..16],
            st.cosigners.len(),
            if st.capped { "+" } else { " " },
            st.txs
        );
    }
    println!(
        "\n  distinct keys in groups: {}   (a '+' degree hit the {} cap)",
        s.degree.len(),
        COSIGNER_CAP
    );
    println!(
        "\n  DECISION: --max-group and --max-degree are the two guards keeping\n  \
         batchers out of the clustering, and both are unvalidated until the\n  \
         numbers above come from a representative window."
    );
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats_with(hist: &[(usize, u64)]) -> Stats {
        let mut s = Stats::new();
        for (n, count) in hist {
            s.hist[*n] = *count;
            s.txs += count;
        }
        s
    }

    #[test]
    fn rows_count_group_members_not_groups() {
        // 10 txs with 2 keys, 5 with 3, 1 with 40.
        let s = stats_with(&[(1, 100), (2, 10), (3, 5), (40, 1)]);
        assert_eq!(s.rows_at(2), 20); // 10 × 2
        assert_eq!(s.rows_at(4), 35); // + 5 × 3
        assert_eq!(s.rows_at(32), 35); // the 40-key tx is still excluded
        // Single-signature txs never contribute, whatever the cap.
        assert_eq!(s.group_txs_at(32), 15);
    }

    #[test]
    fn cap_excludes_oversized_groups_entirely() {
        let s = stats_with(&[(8, 1), (9, 1)]);
        assert_eq!(s.rows_at(8), 8);
        assert_eq!(s.rows_at(9), 17);
    }

    #[test]
    fn slot_span_is_inclusive() {
        let mut s = Stats::new();
        s.first_slot = Some(100);
        s.last_slot = 100;
        assert_eq!(s.slots_covered(), 1);
        s.last_slot = 199;
        assert_eq!(s.slots_covered(), 100);
    }
}
