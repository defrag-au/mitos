//! What a MINT-keyed companion table would cost to extract — item 1 of
//! `cnft.dev-workers/docs/design/POLICY_INDEX.md`, which gates the rest of it.
//!
//! The claim under test: the extraction pass already decodes every block, so
//! adding mints rides work that is already paid. This measures rather than
//! assumes, because the whole proposal is re-costed if it is wrong.
//!
//! Two full passes over the same bytes, read once so the second is not simply
//! the first with a warm page cache:
//!
//! - **A** — what extraction does today: split CBOR items, decode each block,
//!   hash every transaction body.
//! - **B** — the same, plus reading the block's `invalid_transactions` and
//!   walking each body's `mint` field.
//!
//! ⚠️ The passes are run A,B,B,A per chunk and the best of each taken. A
//! straight A-then-B comparison measures the CPU's mood as much as the work,
//! and the delta under test is expected to be small enough for that to matter.
//!
//! It also counts what the index would CONTAIN — mint events, distinct
//! policies, distinct assets — because "how many distinct policies" is an open
//! question in the design note and this pass is already visiting every mint.
//!
//! ```text
//! cargo run --release --example mint_cost -- <immutable-dir> <from-chunk> <count>
//! ```

use std::collections::HashSet;
use std::path::Path;
use std::time::Instant;

use anyhow::{Result, anyhow, bail};
use pallas_codec::minicbor::Decoder;
use pallas_crypto::hash::Hasher;
use pallas_traverse::MultiEraBlock;

/// One transaction body, and whether the block voided it.
struct Counted {
    mint_events: u64,
    policies: HashSet<Vec<u8>>,
    assets: HashSet<(Vec<u8>, Vec<u8>)>,
    invalid_txs: u64,
    /// Hashes of the transactions the BLOCK ITSELF declared invalid. Printed
    /// because the one previously identified was diagnosed from a third-party
    /// API's missing fields; naming it from our own certified chunks turns
    /// "consistent with a phase-2 failure" into "the block says so".
    invalid_hashes: Vec<String>,
    txs: u64,
    /// ⚠️ FORMAT PRESSURE. The tx-index entry stores `len` and `aux_len` as
    /// **u16**, justified by the 16 KB protocol cap on a transaction. A
    /// policy-index entry inherits both fields, and ON-CHAIN ART is where
    /// metadata gets large — so this measures the real maxima rather than
    /// trusting the cap, and counts anything that would not fit.
    max_body: usize,
    max_aux: usize,
    over_u16: u64,
}

impl Counted {
    fn new() -> Self {
        Self {
            mint_events: 0,
            policies: HashSet::new(),
            assets: HashSet::new(),
            invalid_txs: 0,
            invalid_hashes: Vec::new(),
            txs: 0,
            max_body: 0,
            max_aux: 0,
            over_u16: 0,
        }
    }
}

/// Split a chunk into blocks the way `extract.rs` does — its own CBOR walk, so
/// every offset is a fact about the bytes actually seen.
fn for_each_block(bytes: &[u8], mut f: impl FnMut(&MultiEraBlock<'_>) -> Result<()>) -> Result<()> {
    let mut pos = 0usize;
    while pos < bytes.len() {
        let mut d = Decoder::new(&bytes[pos..]);
        d.skip().map_err(|e| anyhow!("splitting at {pos}: {e}"))?;
        let len = d.position();
        if len == 0 {
            bail!("zero-length CBOR item at {pos}");
        }
        let block = MultiEraBlock::decode(&bytes[pos..pos + len])
            .map_err(|e| anyhow!("decoding block at {pos}: {e:?}"))?;
        f(&block)?;
        pos += len;
    }
    Ok(())
}

/// PASS A — today's work: decode, then hash every body.
fn pass_a(bytes: &[u8]) -> Result<u64> {
    let mut n = 0u64;
    for_each_block(bytes, |block| {
        for (body, _aux) in bodies(block) {
            let _ = Hasher::<256>::hash(body);
            n += 1;
        }
        Ok(())
    })?;
    Ok(n)
}

/// PASS B — the same, plus validity and mints.
fn pass_b(bytes: &[u8], c: &mut Counted) -> Result<u64> {
    let mut n = 0u64;
    for_each_block(bytes, |block| {
        // Block-level, read ONCE per block, not per transaction. A mint in a
        // phase-2 failed transaction never happened — see
        // `reference_phase2_failed_tx_outputs`.
        let invalid: Vec<u32> = invalid_transactions(block);
        for (i, (body, aux)) in bodies(block).into_iter().enumerate() {
            let hash = Hasher::<256>::hash(body);
            n += 1;
            c.txs += 1;
            if invalid.contains(&(i as u32)) {
                c.invalid_txs += 1;
                c.invalid_hashes.push(hex::encode(hash));
                continue;
            }
            // Format pressure, recorded for every transaction that MINTS —
            // an index entry is only written for those, so a huge non-mint
            // transaction is not this format's problem.
            if mints_of(block, i, c) {
                c.max_body = c.max_body.max(body.len());
                let aux_len = aux.map(|a| a.len()).unwrap_or(0);
                c.max_aux = c.max_aux.max(aux_len);
                if body.len() > u16::MAX as usize || aux_len > u16::MAX as usize {
                    c.over_u16 += 1;
                }
            }
        }
        Ok(())
    })?;
    Ok(n)
}

/// Raw body slices with their auxiliary-data slices, exactly as
/// `extract.rs::txs` takes them — no `block.txs()`, so no witness-set clone.
fn bodies<'a>(block: &'a MultiEraBlock<'_>) -> Vec<(&'a [u8], Option<&'a [u8]>)> {
    macro_rules! raw_bodies {
        ($b:expr) => {
            $b.transaction_bodies
                .iter()
                .enumerate()
                .map(|(i, k)| {
                    let idx = u32::try_from(i).expect("tx index fits u32");
                    (
                        k.raw_cbor(),
                        $b.auxiliary_data_set.get(&idx).map(|a| a.raw_cbor()),
                    )
                })
                .collect()
        };
    }
    match block {
        MultiEraBlock::EpochBoundary(_) => Vec::new(),
        // Byron predates transaction metadata entirely.
        MultiEraBlock::Byron(b) => b
            .body
            .tx_payload
            .iter()
            .map(|p| (p.transaction.raw_cbor(), None))
            .collect(),
        MultiEraBlock::AlonzoCompatible(b, _) => raw_bodies!(b),
        MultiEraBlock::Babbage(b) => raw_bodies!(b),
        MultiEraBlock::Conway(b) => raw_bodies!(b),
        _ => Vec::new(),
    }
}

fn invalid_transactions(block: &MultiEraBlock<'_>) -> Vec<u32> {
    macro_rules! invalid {
        ($b:expr) => {
            $b.invalid_transactions
                .as_ref()
                .map(|v| v.to_vec())
                .unwrap_or_default()
        };
    }
    match block {
        MultiEraBlock::AlonzoCompatible(b, _) => invalid!(b),
        MultiEraBlock::Babbage(b) => invalid!(b),
        MultiEraBlock::Conway(b) => invalid!(b),
        _ => Vec::new(),
    }
}

/// Walk transaction `i`'s mint field. THIS is the added work under test — and
/// note the body is ALREADY DECODED, so it is a field read plus a walk over
/// however many assets were minted, not a decode.
/// Returns whether this transaction minted anything.
fn mints_of(block: &MultiEraBlock<'_>, i: usize, c: &mut Counted) -> bool {
    macro_rules! walk {
        ($b:expr) => {{
            let Some(body) = $b.transaction_bodies.get(i) else {
                return false;
            };
            let Some(mint) = body.mint.as_ref() else {
                return false;
            };
            for (policy, assets) in mint.iter() {
                for (name, _qty) in assets.iter() {
                    c.mint_events += 1;
                    c.policies.insert(policy.to_vec());
                    c.assets.insert((policy.to_vec(), name.to_vec()));
                }
            }
            true
        }};
    }
    match block {
        MultiEraBlock::AlonzoCompatible(b, _) => walk!(b),
        MultiEraBlock::Babbage(b) => walk!(b),
        MultiEraBlock::Conway(b) => walk!(b),
        _ => false,
    }
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        bail!("usage: mint_cost <immutable-dir> <from-chunk> <count>");
    }
    let dir = Path::new(&args[1]);
    let from: u16 = args[2].parse()?;
    let count: u16 = args[3].parse()?;

    let mut a_secs = 0f64;
    let mut b_secs = 0f64;
    let mut total_bytes = 0u64;
    let mut txs = 0u64;
    let mut c = Counted::new();

    for chunk in from..from + count {
        let path = dir.join(format!("{chunk:05}.chunk"));
        if !path.exists() {
            continue;
        }
        // Read ONCE. Both passes then run over identical in-memory bytes, so
        // the comparison is CPU work and not file-cache warmth.
        let bytes = std::fs::read(&path)?;
        total_bytes += bytes.len() as u64;

        // A,B,B,A and take the best of each: the delta under test is small
        // enough that ordering bias would otherwise be part of the answer.
        let t = Instant::now();
        txs = pass_a(&bytes)?;
        let a1 = t.elapsed().as_secs_f64();

        let mut scratch = Counted::new();
        let t = Instant::now();
        pass_b(&bytes, &mut scratch)?;
        let b1 = t.elapsed().as_secs_f64();

        let mut scratch2 = Counted::new();
        let t = Instant::now();
        pass_b(&bytes, &mut scratch2)?;
        let b2 = t.elapsed().as_secs_f64();

        let t = Instant::now();
        pass_a(&bytes)?;
        let a2 = t.elapsed().as_secs_f64();

        a_secs += a1.min(a2);
        b_secs += b1.min(b2);

        // Counts from one B pass only — the second would double them.
        c.mint_events += scratch.mint_events;
        c.invalid_txs += scratch.invalid_txs;
        c.invalid_hashes.extend(scratch.invalid_hashes);
        c.txs += scratch.txs;
        c.policies.extend(scratch.policies);
        c.assets.extend(scratch.assets);
        c.max_body = c.max_body.max(scratch.max_body);
        c.max_aux = c.max_aux.max(scratch.max_aux);
        c.over_u16 += scratch.over_u16;
    }

    let mb = total_bytes as f64 / 1_048_576.0;
    let delta = b_secs - a_secs;
    println!("chunks       {from}..{}  ({mb:.1} MB)", from + count);
    println!("transactions {}  (last chunk {txs})", c.txs);
    println!("pass A       {a_secs:.3}s  ({:.0} MB/s)", mb / a_secs);
    println!("pass B       {b_secs:.3}s  ({:.0} MB/s)", mb / b_secs);
    println!(
        "DELTA        {delta:+.3}s  = {:+.1}% of the pass",
        delta / a_secs * 100.0
    );
    println!("--- what the index would hold, from these chunks ---");
    println!("mint events      {}", c.mint_events);
    println!("distinct assets  {}", c.assets.len());
    println!("distinct policies {}", c.policies.len());
    println!(
        "invalid txs      {}  (their mints NEVER happened)",
        c.invalid_txs
    );
    for h in c.invalid_hashes.iter().take(10) {
        println!("  declared invalid BY THE BLOCK: {h}");
    }
    println!("--- format pressure on MINTING txs (entry uses u16 for both) ---");
    println!("max body bytes   {}", c.max_body);
    println!("max aux bytes    {}", c.max_aux);
    println!("over u16 (65535) {}", c.over_u16);
    Ok(())
}
