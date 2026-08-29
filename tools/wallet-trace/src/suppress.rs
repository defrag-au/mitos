//! `suppress` — the degree census, and the guard that decides whether the
//! clustering is a tool or a single blob.
//!
//! A batcher does not merely transact with everyone, it **co-signs** with them.
//! One unguarded exchange or DEX key merges the entire chain into a single
//! party, which is the dominant failure mode of this whole approach — the same
//! shape that took `project-ledger`'s frontier to 1.09M rows / 20,678 parties.
//!
//! The rule is `project-ledger`'s relay rule recast for keys: *a relay is used
//! ONCE; armed twice, it is somebody's wallet.* A credential that has co-signed
//! with hundreds of otherwise-unrelated credentials is infrastructure.
//!
//! Separate from `index` on purpose: degree is only knowable after a full pass,
//! and re-running with a different threshold must never mean re-walking the
//! chain. This is the dial an operator actually turns.

use std::path::PathBuf;

use anyhow::Result;

use crate::store::Index;

#[derive(clap::Args, Debug)]
pub struct SuppressArgs {
    #[arg(long, default_value = "wallet-trace.db")]
    pub db: PathBuf,

    /// Exclude keys that have co-signed with more than this many DISTINCT
    /// other keys.
    ///
    /// An ABSOLUTE degree is inherently index-relative: the same key scores
    /// higher on a wider slot range, so a number tuned on one index is wrong on
    /// the next. Prefer `--top-percent`. Kept because sometimes you know the
    /// number you want.
    #[arg(long)]
    pub max_degree: Option<usize>,

    /// Exclude the top N% of keys by degree. Index-size independent, which is
    /// why it is the default.
    ///
    /// MEASURED 2026-08-23: the degree distribution is steeply long-tailed —
    /// 66% of keys sit at degree 0–1, and a key at degree 30 was already in the
    /// top 0.2% AND produced a 15-wallet false merge. 0.5% cuts well inside the
    /// infrastructure tail without touching ordinary wallets.
    #[arg(long, default_value_t = 0.5)]
    pub top_percent: f64,

    /// Report the distribution and what would be excluded; write nothing.
    #[arg(long)]
    pub dry_run: bool,

    /// Show the top N keys by degree.
    #[arg(long, default_value_t = 25)]
    pub top: usize,

    /// Rebuild `key_degree` even if it already exists. The census is the slow
    /// half; changing only the threshold should reuse it.
    #[arg(long)]
    pub recensus: bool,
}

pub fn suppress(args: &SuppressArgs) -> Result<()> {
    let mut ix = Index::open(&args.db)?;

    // The census is the expensive half and its result is stable for a given
    // index, so it is cached in `key_degree`. Re-running with a different
    // threshold must never recompute it — that is the whole reason `suppress`
    // is separate from `index` in the first place.
    let total = if args.recensus || !ix.have_key_degree()? {
        tracing::info!("suppress: building key_degree (streams in sqlite; this is the slow part)");
        ix.build_key_degree()?
    } else {
        let n = ix.key_degree_count()?;
        tracing::info!(
            keys = n,
            "suppress: reusing cached key_degree (--recensus to rebuild)"
        );
        n
    };

    if total == 0 {
        println!("no co-signing groups in this index — run `index` first");
        return Ok(());
    }

    println!("\ndegree distribution over {total} keys");
    // Buckets straddle the plausible threshold range so the cliff — if there is
    // one — is visible rather than inferred.
    for (lo, hi) in [
        (0i64, 1i64),
        (2, 4),
        (5, 9),
        (10, 24),
        (25, 49),
        (50, 99),
        (100, 249),
        (250, 999),
        (1_000, 9_999),
        (10_000, i64::MAX),
    ] {
        let n = ix.degree_bucket(lo, hi)?;
        let label = if hi == i64::MAX {
            format!("{lo}+")
        } else {
            format!("{lo}-{hi}")
        };
        println!(
            "  {label:>12} | {n:>10}  {:>6.2}%",
            n as f64 * 100.0 / total as f64
        );
    }

    if args.top > 0 {
        println!("\ntop {} keys by degree", args.top);
        for (i, (k, d, g)) in ix.top_by_degree(args.top)?.iter().enumerate() {
            println!(
                "  {:>4}. {}  degree {d:>9}  in {g:>9} groups",
                i + 1,
                hex::encode(k)
            );
        }
    }

    // An explicit --max-degree wins; otherwise the percentile picks the cut,
    // and the degree it resolved to is PRINTED so the choice stays inspectable
    // rather than becoming an opaque knob.
    let (threshold, basis) = match args.max_degree {
        Some(d) => (d as i64, format!("--max-degree {d}")),
        None => {
            let rank = ((total as f64) * args.top_percent / 100.0).ceil() as i64;
            let t = ix.degree_at_rank(rank.min(total - 1))?;
            (
                t,
                format!("--top-percent {} → degree > {t}", args.top_percent),
            )
        }
    };

    let would = ix.degree_bucket(threshold + 1, i64::MAX)?;
    println!(
        "\nat {basis}: {would} of {total} keys excluded ({:.4}%)",
        would as f64 * 100.0 / total as f64
    );

    if args.dry_run {
        println!("(dry run — nothing written)");
        return Ok(());
    }
    ix.clear_suppressed()?;
    let n = ix.suppress_above(threshold)?;
    println!("wrote {n} suppressed keys");
    Ok(())
}
