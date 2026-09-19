//! `profile` — the SURVEY INSTRUMENT over `policy_index::profile`.
//!
//! One JSONL row per policy, no filtering, for offline scoring. It answers two
//! questions the catalogue surfaces need and no fresh source currently does:
//!
//! 1. **Can a collection NAME be derived from CIP-25?** CIP-25 has no
//!    collection-name field — only per-asset `name`. So a collection name has
//!    to be derived, and the obvious derivation (longest common prefix over a
//!    policy's asset names) has failure modes that need measuring, not
//!    assuming.
//! 2. **Is this policy an NFT collection or a fungible token?** ⚠️ NOT the
//!    same question as "is any quantity > 1". An art collection with 10
//!    editions of each piece, or a PFP set with one accidental duplicate
//!    ("twins") from a minting error, is still an NFT collection. Supply shape
//!    is evidence, not a verdict.
//!
//! ⚠️ **THIS EMITS EVIDENCE, NOT A CLASSIFICATION.** The point is to decide
//! the rule FROM the data rather than encode a guess and then find data that
//! agrees with it. Every field below is an observation; the scoring happens
//! offline against the 38,992 curated jpg.store names we already hold, which
//! is the only answer key available.
//!
//! # Its relationship to the catalogue builder
//!
//! The extraction moved into `policy_index::profile` when
//! `policy-ledger catalogue build` became its second consumer. **This file is
//! now the serialisation and the sampling and nothing else** — deliberately,
//! because a JSONL row shape is the survey's contract and not the crate's, and
//! because the builder must classify the SAME evidence this instrument scores.
//!
//! ```text
//! cargo run --release --example profile -- \
//!     --index-dir /opt/policy-index/mainnet \
//!     --immutable /opt/market-ledger/snapshot-full/db/immutable \
//!     --sample 3000 --out /tmp/profile.jsonl
//! ```

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use policy_index::profile::{ChunkReader, PolicyEvidence, profile_policy};
use policy_index::{Base, Record, base_path};
use serde::Serialize;

// ---------------------------------------------------------------------------
// The JSONL row
// ---------------------------------------------------------------------------

/// ⚠️ **The field names here are a contract with the offline scoring
/// scripts.** Every measurement in `COLLECTION_CATALOGUE.md` was taken from
/// this shape; renaming a field silently re-scores nothing and breaks the
/// comparison. It mirrors [`PolicyEvidence`] one-for-one on purpose.
#[derive(Serialize)]
struct Row<'a> {
    policy: Option<String>,
    prefix: String,
    events: u32,
    burn_events: u32,
    distinct_name_prefixes: u32,
    first_chunk: u16,
    sampled_txs: u32,
    assets_seen: u32,
    qty: Qty,
    cip67: Cip67,
    labels: &'a [u64],
    cip25_assets: u32,
    names: &'a [String],
    onchain_names: &'a [String],
    derived_from_meta: Option<&'a str>,
    derived_from_onchain: Option<&'a str>,
    declared_collection: Option<&'a str>,
    cip25_keys: &'a [String],
    has_ticker: bool,
    has_decimals: bool,
    body_decode_failures: u32,
    aux_decode_failures: u32,
}

#[derive(Serialize)]
struct Qty {
    min: i64,
    max: i64,
    eq1: u32,
    gt1: u32,
    distinct: u32,
}

#[derive(Serialize)]
struct Cip67 {
    reference: u32,
    nft: u32,
    ft: u32,
    rft: u32,
    unlabelled: u32,
}

impl<'a> From<&'a PolicyEvidence> for Row<'a> {
    fn from(e: &'a PolicyEvidence) -> Self {
        Row {
            policy: e.policy_hex(),
            prefix: format!("{:016x}", e.prefix),
            events: e.events,
            burn_events: e.burn_events,
            distinct_name_prefixes: e.distinct_name_prefixes,
            first_chunk: e.first_chunk,
            sampled_txs: e.sampled_txs,
            assets_seen: e.assets_seen,
            qty: Qty {
                min: e.qty.min,
                max: e.qty.max,
                eq1: e.qty.eq1,
                gt1: e.qty.gt1,
                distinct: e.qty.distinct,
            },
            cip67: Cip67 {
                reference: e.cip67.reference,
                nft: e.cip67.nft,
                ft: e.cip67.ft,
                rft: e.cip67.rft,
                unlabelled: e.cip67.unlabelled,
            },
            labels: &e.labels,
            cip25_assets: e.cip25_assets,
            names: &e.names,
            onchain_names: &e.onchain_names,
            derived_from_meta: e.derived_from_meta.as_deref(),
            derived_from_onchain: e.derived_from_onchain.as_deref(),
            declared_collection: e.declared_collection.as_deref(),
            cip25_keys: &e.cip25_keys,
            has_ticker: e.has_ticker,
            has_decimals: e.has_decimals,
            body_decode_failures: e.body_decode_failures,
            aux_decode_failures: e.aux_decode_failures,
        }
    }
}

// ---------------------------------------------------------------------------

struct Args {
    index_dir: PathBuf,
    immutable: PathBuf,
    sample: usize,
    out: PathBuf,
}

fn parse_args() -> Result<Args> {
    let mut index_dir = None;
    let mut immutable = None;
    let mut sample = 3000usize;
    let mut out = PathBuf::from("/tmp/profile.jsonl");
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--index-dir" => index_dir = it.next().map(PathBuf::from),
            "--immutable" => immutable = it.next().map(PathBuf::from),
            "--sample" => sample = it.next().context("--sample needs a value")?.parse()?,
            "--out" => out = it.next().map(PathBuf::from).context("--out needs a path")?,
            other => bail!("unknown argument {other}"),
        }
    }
    Ok(Args {
        index_dir: index_dir.context("--index-dir is required")?,
        immutable: immutable.context("--immutable is required")?,
        sample,
        out,
    })
}

fn main() -> Result<()> {
    let args = parse_args()?;
    let base = Base::open(&base_path(&args.index_dir))?;
    eprintln!(
        "base: {} records, {} policies, chunks {:?}",
        base.len(),
        base.policies(),
        base.covers()
    );

    // The policy side table, read straight through. An earlier draft
    // recovered the runs by scanning all ~15M records because the accessor
    // was private; `Base::runs` says the same thing in 231k reads.
    let runs: Vec<_> = base.runs().collect();
    eprintln!("recovered {} runs", runs.len());

    // ⚠️ STRIDE, not the first N and not random. The first N share a prefix
    // bucket and an era; a stride crosses every bucket and every era, which is
    // where a systematic difference (a launchpad's naming convention, a CIP-68
    // cohort) would otherwise hide. Same argument as `verify_sample`.
    let stride = (runs.len() / args.sample.max(1)).max(1);
    let picked: Vec<_> = runs.iter().step_by(stride).take(args.sample).collect();
    eprintln!("sampling {} policies (stride {stride})", picked.len());

    let mut files = ChunkReader::new(&args.immutable);
    let mut w = BufWriter::new(File::create(&args.out)?);
    let mut done = 0usize;
    // Chunk order, so the single open file handle is reused instead of
    // thrashing once per policy.
    let mut ordered: Vec<_> = picked
        .iter()
        .map(|run| {
            let recs: Vec<Record> = (run.start as usize..(run.start + run.len) as usize)
                .map(|i| base.record_at_index(i))
                .collect();
            (run.first_chunk, run.policy_prefix, recs)
        })
        .collect();
    ordered.sort_by_key(|(c, _, _)| *c);

    for (_, prefix, recs) in &ordered {
        let evidence = profile_policy(&mut files, *prefix, recs);
        writeln!(w, "{}", serde_json::to_string(&Row::from(&evidence))?)?;
        done += 1;
        if done.is_multiple_of(500) {
            eprintln!("  {done}/{}", ordered.len());
        }
    }
    w.flush()?;
    eprintln!("wrote {} profiles to {}", done, args.out.display());
    Ok(())
}
