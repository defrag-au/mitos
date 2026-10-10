//! What a REFERENCE-TOKEN update log costs on top of the mint harvest —
//! the open measurement in `ASSET_METADATA_INDEX.md`.
//!
//! The resolved asset index needs one thing the policy index cannot see: a
//! CIP-68 reference-token **respend** (a reveal, trait shop, seasonal variant)
//! is a transfer with a new datum, carries no mint field, and so is invisible
//! to a mint-keyed index. The proposed fix is a companion observer riding the
//! SAME extraction pass — not a second pass, which `POLICY_INDEX.md` measured
//! at +100%.
//!
//! The question this answers: **what does that observer add, on top of the
//! +1.8% the mint harvest already pays?** `mint_cost.rs` answered the first
//! half; this is the second.
//!
//! Three full passes over the same bytes, read once so none is merely another
//! with a warm page cache:
//!
//! - **A** — what extraction does today: split CBOR items, decode each block,
//!   hash every transaction body.
//! - **B** — the same, plus validity and the mint walk (the already-accepted
//!   harvest).
//! - **C** — the same, plus scanning every valid transaction's OUTPUTS for
//!   names carrying the CIP-67 reference label `000643b0`.
//!
//! The number that matters is **C−B**: the reference scan's marginal cost.
//! It is reported alongside B−A so both halves of the observer can be quoted
//! separately.
//!
//! ⚠️ The passes are run A,B,C,C,B,A per chunk and the best of each taken — a
//! straight A-then-C comparison measures the CPU's mood as much as the work.
//!
//! ⚠️ The scan walks the raw primitives (`transaction_bodies[i].outputs`),
//! not `block.txs()` — the same reason `extract.rs` takes body slices
//! directly: `txs()` CLONES every witness set, which is 16.4% of decode by
//! fold-probe's measurement and would pollute the comparison.
//!
//! ```text
//! cargo run --release --example ref_cost -- <immutable-dir> <from-chunk> <count>
//! ```

use std::collections::HashSet;
use std::path::Path;
use std::time::Instant;

use anyhow::{Result, anyhow, bail};
use pallas_codec::minicbor::Decoder;
use pallas_crypto::hash::Hasher;
use pallas_primitives::{alonzo, babbage, conway};
use pallas_traverse::MultiEraBlock;

/// CIP-67 label 100, the CIP-68 reference token: `000643b0`.
const REFERENCE_LABEL: [u8; 4] = [0x00, 0x06, 0x43, 0xb0];

/// What the mint harvest would hold — for context and to prove B does the
/// same work `mint_cost.rs` measured.
#[derive(Default)]
struct Mints {
    events: u64,
    policies: HashSet<Vec<u8>>,
    assets: HashSet<(Vec<u8>, Vec<u8>)>,
}

/// What the reference scan would hold.
#[derive(Default)]
struct Refs {
    /// Outputs visited — the denominator the marginal cost scales with.
    outputs: u64,
    /// Asset entries inspected (the inner loop's actual iterations).
    entries: u64,
    /// Names carrying the reference label.
    events: u64,
    policies: HashSet<Vec<u8>>,
    pairs: HashSet<(Vec<u8>, Vec<u8>)>,
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

/// PASS B — the already-accepted harvest: validity + mints.
fn pass_b(bytes: &[u8], m: &mut Mints) -> Result<u64> {
    let mut n = 0u64;
    for_each_block(bytes, |block| {
        let invalid: Vec<u32> = invalid_transactions(block);
        for (i, (body, _aux)) in bodies(block).into_iter().enumerate() {
            let _ = Hasher::<256>::hash(body);
            n += 1;
            if invalid.contains(&(i as u32)) {
                continue;
            }
            mints_of(block, i, m);
        }
        Ok(())
    })?;
    Ok(n)
}

/// PASS C — B plus the reference-label output scan. The delta against B is
/// the number this example exists for.
fn pass_c(bytes: &[u8], m: &mut Mints, r: &mut Refs) -> Result<u64> {
    let mut n = 0u64;
    for_each_block(bytes, |block| {
        let invalid: Vec<u32> = invalid_transactions(block);
        for (i, (body, _aux)) in bodies(block).into_iter().enumerate() {
            let _ = Hasher::<256>::hash(body);
            n += 1;
            // A mint or respend in a phase-2 failed transaction never
            // happened — the same gate the policy index applies at the only
            // place a record is created.
            if invalid.contains(&(i as u32)) {
                continue;
            }
            mints_of(block, i, m);
            refs_of(block, i, r);
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

/// Walk transaction `i`'s mint field, exactly as `policy-index` does.
fn mints_of(block: &MultiEraBlock<'_>, i: usize, m: &mut Mints) {
    macro_rules! walk {
        ($b:expr) => {{
            let Some(body) = $b.transaction_bodies.get(i) else {
                return;
            };
            let Some(mint) = body.mint.as_ref() else {
                return;
            };
            for (policy, assets) in mint.iter() {
                for (name, _qty) in assets.iter() {
                    m.events += 1;
                    m.policies.insert(policy.to_vec());
                    m.assets.insert((policy.to_vec(), name.to_vec()));
                }
            }
        }};
    }
    match block {
        MultiEraBlock::AlonzoCompatible(b, _) => walk!(b),
        MultiEraBlock::Babbage(b) => walk!(b),
        MultiEraBlock::Conway(b) => walk!(b),
        _ => {}
    }
}

/// The added work under test: every output's multiasset names, checked for the
/// reference label. The primitives are already decoded, so this is a walk over
/// the value maps — a field read plus a name comparison per entry.
fn refs_of(block: &MultiEraBlock<'_>, i: usize, r: &mut Refs) {
    macro_rules! note {
        ($policy:expr, $name:expr) => {{
            r.entries += 1;
            if $name.starts_with(&REFERENCE_LABEL) {
                r.events += 1;
                r.policies.insert($policy.to_vec());
                r.pairs.insert(($policy.to_vec(), $name.to_vec()));
            }
        }};
    }
    macro_rules! walk_alonzo_value {
        ($v:expr) => {
            if let alonzo::Value::Multiasset(_, m) = $v {
                for (policy, assets) in m.iter() {
                    for name in assets.keys() {
                        note!(policy, name);
                    }
                }
            }
        };
    }
    macro_rules! walk_conway_value {
        ($v:expr) => {
            if let conway::Value::Multiasset(_, m) = $v {
                for (policy, assets) in m.iter() {
                    for name in assets.keys() {
                        note!(policy, name);
                    }
                }
            }
        };
    }
    macro_rules! walk_alonzo_outputs {
        ($b:expr) => {{
            let Some(body) = $b.transaction_bodies.get(i) else {
                return;
            };
            for out in body.outputs.iter() {
                r.outputs += 1;
                walk_alonzo_value!(&out.amount);
            }
        }};
    }
    match block {
        MultiEraBlock::AlonzoCompatible(b, _) => walk_alonzo_outputs!(b),
        MultiEraBlock::Babbage(b) => {
            let Some(body) = b.transaction_bodies.get(i) else {
                return;
            };
            for out in body.outputs.iter() {
                r.outputs += 1;
                match &**out {
                    babbage::TransactionOutput::Legacy(x) => walk_alonzo_value!(&x.amount),
                    babbage::TransactionOutput::PostAlonzo(x) => walk_alonzo_value!(&x.value),
                }
            }
        }
        MultiEraBlock::Conway(b) => {
            let Some(body) = b.transaction_bodies.get(i) else {
                return;
            };
            for out in body.outputs.iter() {
                r.outputs += 1;
                match out {
                    conway::TransactionOutput::Legacy(x) => walk_alonzo_value!(&x.amount),
                    conway::TransactionOutput::PostAlonzo(x) => walk_conway_value!(&x.value),
                }
            }
        }
        _ => {}
    }
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        bail!("usage: ref_cost <immutable-dir> <from-chunk> <count>");
    }
    let dir = Path::new(&args[1]);
    let from: u16 = args[2].parse()?;
    let count: u16 = args[3].parse()?;

    let mut a_secs = 0f64;
    let mut b_secs = 0f64;
    let mut c_secs = 0f64;
    let mut total_bytes = 0u64;
    let mut mints = Mints::default();
    let mut refs = Refs::default();

    for chunk in from..from + count {
        let path = dir.join(format!("{chunk:05}.chunk"));
        if !path.exists() {
            continue;
        }
        // Read ONCE. All passes then run over identical in-memory bytes, so
        // the comparison is CPU work and not file-cache warmth.
        let bytes = std::fs::read(&path)?;
        total_bytes += bytes.len() as u64;

        // A,B,C,C,B,A and take the best of each.
        let t = Instant::now();
        pass_a(&bytes)?;
        let a1 = t.elapsed().as_secs_f64();

        let mut b_scratch = Mints::default();
        let t = Instant::now();
        pass_b(&bytes, &mut b_scratch)?;
        let b1 = t.elapsed().as_secs_f64();

        let mut c_scratch = Mints::default();
        let mut c_refs = Refs::default();
        let t = Instant::now();
        pass_c(&bytes, &mut c_scratch, &mut c_refs)?;
        let c1 = t.elapsed().as_secs_f64();

        let mut c2_scratch = Mints::default();
        let mut c2_refs = Refs::default();
        let t = Instant::now();
        pass_c(&bytes, &mut c2_scratch, &mut c2_refs)?;
        let c2 = t.elapsed().as_secs_f64();

        let mut b2_scratch = Mints::default();
        let t = Instant::now();
        pass_b(&bytes, &mut b2_scratch)?;
        let b2 = t.elapsed().as_secs_f64();

        let t = Instant::now();
        pass_a(&bytes)?;
        let a2 = t.elapsed().as_secs_f64();

        a_secs += a1.min(a2);
        b_secs += b1.min(b2);
        c_secs += c1.min(c2);

        // Counts from one B and one C pass only — the second of each would
        // double them.
        mints.events += b_scratch.events;
        mints.policies.extend(b_scratch.policies);
        mints.assets.extend(b_scratch.assets);
        refs.outputs += c_refs.outputs;
        refs.entries += c_refs.entries;
        refs.events += c_refs.events;
        refs.policies.extend(c_refs.policies);
        refs.pairs.extend(c_refs.pairs);
    }

    let mb = total_bytes as f64 / 1_048_576.0;
    let d_ab = b_secs - a_secs;
    let d_bc = c_secs - b_secs;
    println!("chunks       {from}..{}  ({mb:.1} MB)", from + count);
    println!("pass A       {a_secs:.3}s  ({:.0} MB/s)", mb / a_secs);
    println!("pass B       {b_secs:.3}s  ({:.0} MB/s)", mb / b_secs);
    println!("pass C       {c_secs:.3}s  ({:.0} MB/s)", mb / c_secs);
    println!(
        "DELTA mints  {d_ab:+.3}s  = {:+.1}% of A   (the already-accepted harvest)",
        d_ab / a_secs * 100.0
    );
    println!(
        "DELTA refs   {d_bc:+.3}s  = {:+.1}% of B   (the marginal cost under test)",
        d_bc / b_secs * 100.0
    );
    println!("--- what the mint harvest would hold, from these chunks ---");
    println!("mint events       {}", mints.events);
    println!("distinct assets   {}", mints.assets.len());
    println!("distinct policies {}", mints.policies.len());
    println!("--- what the reference scan would hold, from these chunks ---");
    println!("outputs scanned   {}", refs.outputs);
    println!("asset entries     {}", refs.entries);
    println!("reference events  {}", refs.events);
    println!("distinct pairs    {}", refs.pairs.len());
    println!("distinct policies {}", refs.policies.len());
    Ok(())
}
