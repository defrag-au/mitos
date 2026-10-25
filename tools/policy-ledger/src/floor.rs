//! `policy-ledger policy first-mint` — the floor probe, as a verb.
//!
//! The same question `token-ledger` asks at admission, asked by a person: which
//! chunk holds this policy's earliest mint. It reads only `runs.pidx` (3.7 MiB,
//! 16 bytes a policy) — never the 668 MiB base — so it is a single binary search
//! over a file the page cache keeps resident.
//!
//! # Why a verb and not a library call
//!
//! Two callers needed it from outside Rust: a frontend that wants to pass
//! `?to_slot=` before asking for a walk, and whoever is looking at an archive
//! that started at genesis and wondering why. Both can now ask without starting
//! a walk, which is the whole point — the answer is free and the walk is not.
//!
//! ⚠️ **Absence is not evidence.** A policy the index has not reached returns
//! "not found" and this exits non-zero: the index covers the chunks it was built
//! over, and a policy minted after them is exactly the case it cannot answer.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};

/// Mainnet immutable-DB chunk size in slots — mirrors
/// `mitos_chain_walk::mithril::CHUNK_SLOTS`, restated so a reader-facing verb
/// can report a slot without pulling the bootstrap module in.
const CHUNK_SLOTS: u64 = 21_600;

#[derive(clap::Args, Debug)]
pub struct FirstMintArgs {
    /// A 56-character policy id, hex. The asset name is not part of the
    /// question — a policy's floor is the policy's, not a unit's.
    pub policy: String,
    /// The policy index directory — the one holding `policy/runs.pidx`.
    #[arg(long, env = "POLICY_INDEX_DIR")]
    pub index_dir: PathBuf,
}

pub fn first_mint(args: FirstMintArgs) -> Result<()> {
    let raw = args.policy.trim();
    let policy = hex::decode(raw).with_context(|| format!("policy {raw} is not hex"))?;
    if policy.len() != 28 {
        bail!(
            "policy {raw} decodes to {} bytes; a policy id is 28",
            policy.len()
        );
    }

    let floors = policy_index::Floors::open(&args.index_dir)
        .with_context(|| format!("opening the policy index at {}", args.index_dir.display()))?
        .with_context(|| {
            format!(
                "no policy index under {} — neither {} nor {}",
                args.index_dir.display(),
                policy_index::runs_path(&args.index_dir).display(),
                policy_index::base_path(&args.index_dir).display()
            )
        })?;

    let prefix = policy_index::policy_prefix(&policy);
    let (from, to) = floors.covers();
    println!("policy  {raw}");
    println!(
        "index   {}  {}  covers {from}..={to}  policies {}",
        args.index_dir.display(),
        floors.source(),
        floors.len()
    );

    let Some(chunk) = floors.first_chunk_of(prefix) else {
        bail!(
            "policy {raw} is not in the index — minted after chunk {to}, or never minted. \
             Pass ?to_slot= from another source rather than walking from genesis"
        );
    };
    println!(
        "floor   chunk {chunk}  slot {}  (the chunk's first slot — at or below the mint)",
        u64::from(chunk) * CHUNK_SLOTS
    );
    Ok(())
}
