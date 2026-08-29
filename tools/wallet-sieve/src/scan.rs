//! The sieve — parallel raw-byte search over chunk files, decode on hit.
//!
//! Workers pull chunk numbers off a shared queue, `fs::read` the raw file and
//! memmem it for the patterns. Only a HIT chunk gets block treatment: a fuzzy
//! slot-seek (`open_blocks` with an empty hash) to the chunk's first slot,
//! then per-block memmem again so only hit BLOCKS pay for a full decode. Hits
//! are rare for any one wallet, so the steady state is disk-bandwidth-bound
//! byte scanning — that is the entire design.
//!
//! Two passes share this machinery:
//! - **cred scan** (pass A): patterns are the wallet's 28-byte credentials;
//!   hits are outputs paying the wallet.
//! - **sweep scan** (pass B): patterns are the wallet's own 32-byte tx hashes
//!   (a spending tx carries the source hash raw in its input), catching
//!   spends that left no change output behind. Uses Aho-Corasick because the
//!   pattern set is hundreds wide.
//!
//! The newest chunk file is deliberately excluded: it is still growing
//! (Mithril semantics), and pallas-hardano's own reader pops it for the same
//! reason. The tail belongs to the spool, not the sieve.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use aho_corasick::AhoCorasick;
use anyhow::{Context, Result, bail};
use memchr::memmem::Finder;
use mitos_chain_walk::mithril::CHUNK_SLOTS;
use mitos_chain_walk::open_blocks;
use pallas_addresses::{Address, ShelleyDelegationPart, ShelleyPaymentPart};
use pallas_traverse::MultiEraBlock;

use crate::progress::{Prog, Progress};

/// A native asset that moved: policy + on-chain name (both hex) + quantity.
/// Identity, not just a count — "received 3 assets" is not a story, "received
/// HOSKY Cash Grab #1729" is.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AssetUnit {
    pub policy: String,
    pub name_hex: String,
    pub quantity: u64,
}

/// Per-output cap on recorded identities.
///
/// **This has to be generous, and a small value is actively WRONG.** A
/// collector's UTxO routinely carries hundreds of assets, and a sale spends
/// that whole UTxO and takes the remainder back as change. Classification
/// nets the two, so the assets that stayed cancel and only the ones that
/// left survive — which is exactly the story a card wants to tell. But that
/// only works if BOTH sides were recorded in full: truncate them and the
/// surviving evidence is whichever units happened to sort first, so a sale
/// out of a 217-asset UTxO netted to *nothing* under the original cap of 16.
///
/// This is a guard against a pathological dust sweep, not a budget.
const MAX_UNITS_PER_OUTPUT: usize = 1024;

/// One output paying the target.
pub struct OutHit {
    pub index: u32,
    pub lovelace: u64,
    pub assets: u32,
    pub units: Vec<AssetUnit>,
    /// The payment credential is a SCRIPT, so this output is not spendable
    /// with the wallet's key even though its stake credential is the
    /// wallet's.
    ///
    /// This is the gap between what a stake-keyed sieve sees and what a wallet
    /// shows. An offer, a listing or an order parks funds at a contract
    /// address that keeps the customer's delegation — so the money still earns
    /// the wallet its rewards and still matches the credential scan, while
    /// being completely beyond the reach of its spending key. Netting without
    /// this reports a 147 ₳ offer as a 1.4 ₳ fee.
    pub script: bool,
}

/// One transaction that touches the target credentials.
pub struct FoundTx {
    /// Which batch target this hit belongs to (index into the `targets`
    /// slice given to [`cred_scan`]). A tx paying two watched wallets yields
    /// one FoundTx per wallet, each with its own out_hits.
    pub target_idx: usize,
    pub slot: u64,
    /// Position within the block — with `slot` this is a total order.
    pub tx_idx: u32,
    pub hash: [u8; 32],
    pub out_hits: Vec<OutHit>,
    /// Every input outref, for spend classification + sender resolution.
    pub inputs: Vec<([u8; 32], u32)>,
    pub total_outputs: u32,
    /// Outputs NOT paying the target: (address, lovelace). For a send these
    /// are the destinations — the counterparty side no resolution pass can
    /// otherwise name.
    pub other_outputs: Vec<(String, u64)>,
    /// Assets this transaction MINTED or BURNED, positive for minted.
    ///
    /// The only place "these assets did not exist before" is a fact rather
    /// than a guess. Netting inputs against outputs cannot tell a mint from a
    /// purchase — both look like assets arriving from a counterparty you just
    /// paid — so without this field a delivery can only ever be inferred from
    /// its shape. The tx is already decoded here, so reading it is free.
    pub minted: Vec<MintedUnit>,
}

/// One asset's mint-field entry. Signed: a burn is a negative quantity, and
/// collapsing that to "touched the mint field" would call a burn a mint.
#[derive(Clone, Debug)]
pub struct MintedUnit {
    pub policy: String,
    pub name_hex: String,
    pub quantity: i64,
}

#[derive(Default)]
pub struct ScanStats {
    pub chunks: u64,
    pub bytes: u64,
    pub hit_chunks: u64,
    pub hit_blocks: u64,
    /// Blocks whose bytes matched but no tx output did — datum/metadata
    /// mentions or byte coincidences. A false-positive rate gauge.
    pub unmatched_hit_blocks: u64,
    pub wall_secs: f64,
}

/// Sorted chunk numbers on disk within `[floor_chunk, newest)` — the newest
/// file is excluded (still growing).
pub fn list_chunks(immutable: &Path, floor_chunk: u64) -> Result<Vec<u64>> {
    let mut nums: Vec<u64> = std::fs::read_dir(immutable)
        .with_context(|| format!("reading {}", immutable.display()))?
        .filter_map(|e| {
            let name = e.ok()?.file_name().into_string().ok()?;
            let stem = name.strip_suffix(".chunk")?;
            stem.parse::<u64>().ok()
        })
        .collect();
    nums.sort_unstable();
    if nums.len() < 2 {
        bail!("need at least 2 chunk files (the newest is excluded as still-growing)");
    }
    nums.pop();
    Ok(nums.into_iter().filter(|n| *n >= floor_chunk).collect())
}

/// Any of the target patterns, over any byte slice. Both pass shapes fit
/// behind this one trait object-free enum so the worker loop is shared.
enum Needles<'a> {
    Creds(Vec<Finder<'a>>),
    Hashes(AhoCorasick),
}

impl Needles<'_> {
    fn hit(&self, haystack: &[u8]) -> bool {
        match self {
            Needles::Creds(fs) => fs.iter().any(|f| f.find(haystack).is_some()),
            Needles::Hashes(ac) => ac.is_match(haystack),
        }
    }
}

/// Pass A: find every tx with an output paying any target's credentials —
/// MANY wallets in ONE sweep (scan cost is chain-size-bound, so everyone
/// queued rides the same pass). Hits are attributed per target.
pub fn cred_scan(
    immutable: &Path,
    chunks: &[u64],
    targets: &[Vec<[u8; 28]>],
    threads: usize,
    on: Prog<'_>,
) -> Result<(Vec<FoundTx>, ScanStats)> {
    let flat: Vec<[u8; 28]> = targets.iter().flatten().copied().collect();
    let ac = (flat.len() > 3)
        .then(|| AhoCorasick::new(&flat))
        .transpose()
        .context("building cred automaton")?;
    run(
        immutable,
        chunks,
        threads,
        "cred",
        on,
        || match &ac {
            Some(ac) => Needles::Hashes(ac.clone()),
            None => Needles::Creds(flat.iter().map(Finder::new).collect()),
        },
        &|block, _needles, out| {
            extract_cred_hits(block, targets, out);
        },
    )
}

/// Pass B: find every tx whose INPUTS name one of the wallet's own tx hashes.
/// `owned` filters to outrefs the wallet actually owned (a hash hit alone also
/// matches strangers spending sibling outputs of a tx that paid us).
pub fn sweep_scan(
    immutable: &Path,
    chunks: &[u64],
    own_hashes: &[[u8; 32]],
    owned: &crate::classify::OwnedSet,
    threads: usize,
    on: Prog<'_>,
) -> Result<(Vec<FoundTx>, ScanStats)> {
    let ac = AhoCorasick::new(own_hashes).context("building sweep automaton")?;
    run(
        immutable,
        chunks,
        threads,
        "sweep",
        on,
        move || Needles::Hashes(ac.clone()),
        &|block, needles, out| {
            extract_sweep_hits(block, needles, owned, out);
        },
    )
}

/// The shared worker harness: chunk queue → raw memmem → block pass on hit.
#[allow(clippy::too_many_arguments)]
fn run<'a, MkNeedles>(
    immutable: &Path,
    chunks: &[u64],
    threads: usize,
    pass: &str,
    on: Prog<'_>,
    mk_needles: MkNeedles,
    extract: &(dyn Fn(&MultiEraBlock<'_>, &Needles<'_>, &mut Vec<FoundTx>) + Sync),
) -> Result<(Vec<FoundTx>, ScanStats)>
where
    MkNeedles: Fn() -> Needles<'a> + Sync,
{
    let started = Instant::now();
    let queue: Mutex<VecDeque<u64>> = Mutex::new(chunks.iter().copied().collect());
    let done_chunks = AtomicU64::new(0);
    let done_bytes = AtomicU64::new(0);
    let total = chunks.len() as u64;

    let worker = |_: usize| -> Result<(Vec<FoundTx>, ScanStats)> {
        let needles = mk_needles();
        let mut found = Vec::new();
        let mut stats = ScanStats::default();
        loop {
            let chunk = { queue.lock().expect("queue").pop_front() };
            let Some(chunk) = chunk else { break };
            let path: PathBuf = immutable.join(format!("{chunk:05}.chunk"));
            let bytes =
                std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
            stats.chunks += 1;
            stats.bytes += bytes.len() as u64;
            let dc = done_chunks.fetch_add(1, Ordering::Relaxed) + 1;
            let db = done_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
            if dc.is_multiple_of(100) {
                let secs = started.elapsed().as_secs_f64();
                on(Progress::Scan {
                    pass,
                    done: dc,
                    total,
                    gb_per_s: db as f64 / 1e9 / secs,
                });
            }
            if !needles.hit(&bytes) {
                continue;
            }
            stats.hit_chunks += 1;
            drop(bytes);

            // Block pass over just this chunk: fuzzy-seek to its first slot,
            // stop at the next chunk's.
            let start = chunk * CHUNK_SLOTS;
            let end = (chunk + 1) * CHUNK_SLOTS;
            let blocks = open_blocks(immutable, Some((start, Vec::new())))
                .with_context(|| format!("seeking chunk {chunk}"))?;
            for raw in blocks {
                let raw = raw.map_err(|e| anyhow::anyhow!("reading block: {e:?}"))?;
                let block = MultiEraBlock::decode(&raw)
                    .map_err(|e| anyhow::anyhow!("decoding block in chunk {chunk}: {e:?}"))?;
                if block.slot() >= end {
                    break;
                }
                if !needles.hit(&raw) {
                    continue;
                }
                stats.hit_blocks += 1;
                let before = found.len();
                extract(&block, &needles, &mut found);
                if found.len() == before {
                    stats.unmatched_hit_blocks += 1;
                }
            }
        }
        Ok((found, stats))
    };

    let per_thread: Vec<(Vec<FoundTx>, ScanStats)> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads.max(1))
            .map(|i| s.spawn(move || worker(i)))
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("worker panicked"))
            .collect::<Result<Vec<_>>>()
    })?;

    let mut found = Vec::new();
    let mut stats = ScanStats::default();
    for (f, st) in per_thread {
        found.extend(f);
        stats.chunks += st.chunks;
        stats.bytes += st.bytes;
        stats.hit_chunks += st.hit_chunks;
        stats.hit_blocks += st.hit_blocks;
        stats.unmatched_hit_blocks += st.unmatched_hit_blocks;
    }
    stats.wall_secs = started.elapsed().as_secs_f64();
    Ok((found, stats))
}

/// 32-byte pallas `Hash` → owned array.
/// The transaction's mint field, flattened. Empty for the overwhelming
/// majority of transactions, which mint nothing.
fn minted_units(tx: &pallas_traverse::MultiEraTx<'_>) -> Vec<MintedUnit> {
    tx.mints()
        .iter()
        .flat_map(|pa| {
            let policy = hex::encode(pa.policy());
            pa.assets()
                .iter()
                .map(|a| MintedUnit {
                    policy: policy.clone(),
                    name_hex: hex::encode(a.name()),
                    // Signed on purpose — `any_coin` keeps the sign, and a
                    // burn must not read as a mint. Saturating rather than
                    // wrapping: a quantity past i64 is absurd, and wrapping
                    // one would flip a mint into a burn.
                    quantity: a.any_coin().clamp(i64::MIN as i128, i64::MAX as i128) as i64,
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn h32(h: impl AsRef<[u8]>) -> [u8; 32] {
    let mut out = [0u8; 32];
    out.copy_from_slice(h.as_ref());
    out
}

/// Does this address pay to a SCRIPT rather than a key?
///
/// Non-Shelley (Byron) addresses are always key-controlled, so `false`.
fn addr_pays_to_script(addr: &Address) -> bool {
    matches!(
        addr,
        Address::Shelley(sh) if matches!(sh.payment(), ShelleyPaymentPart::Script(_))
    )
}

/// The two 28-byte credential slots of an output address, when Shelley.
fn addr_creds(addr: &Address) -> (Option<[u8; 28]>, Option<[u8; 28]>) {
    match addr {
        Address::Shelley(sh) => {
            let p = match sh.payment() {
                ShelleyPaymentPart::Key(h) | ShelleyPaymentPart::Script(h) => {
                    h.as_slice().try_into().ok()
                }
            };
            let d = match sh.delegation() {
                ShelleyDelegationPart::Key(h) | ShelleyDelegationPart::Script(h) => {
                    h.as_slice().try_into().ok()
                }
                _ => None,
            };
            (p, d)
        }
        _ => (None, None),
    }
}

/// Also the tail-spool extraction path — the spool hands decoded blocks
/// straight here so chunks and tail share one attribution code path.
pub(crate) fn extract_cred_hits(
    block: &MultiEraBlock<'_>,
    targets: &[Vec<[u8; 28]>],
    out: &mut Vec<FoundTx>,
) {
    let slot = block.slot();
    for (ti, tx) in block.txs().iter().enumerate() {
        let outputs = tx.outputs();
        // Decode each output once; per-output, which targets it pays.
        struct Decoded {
            address: String,
            lovelace: u64,
            assets: u32,
            units: Vec<AssetUnit>,
            matched: Vec<usize>,
            script: bool,
        }
        let mut decoded: Vec<Decoded> = Vec::with_capacity(outputs.len());
        let mut any = false;
        for o in outputs.iter() {
            let Ok(addr) = o.address() else { continue };
            let (p, d) = addr_creds(&addr);
            let script = addr_pays_to_script(&addr);
            let matched: Vec<usize> = targets
                .iter()
                .enumerate()
                .filter(|(_, creds)| {
                    creds
                        .iter()
                        .any(|c| p.as_ref() == Some(c) || d.as_ref() == Some(c))
                })
                .map(|(t, _)| t)
                .collect();
            any |= !matched.is_empty();
            let value = o.value();
            let assets: u32 = value
                .assets()
                .iter()
                .map(|pa| pa.assets().len() as u32)
                .sum();
            // Identities only for outputs that pay a target — the rest are
            // counterparty outputs, where the address is the story.
            let units: Vec<AssetUnit> = if matched.is_empty() {
                Vec::new()
            } else {
                value
                    .assets()
                    .iter()
                    .flat_map(|pa| {
                        let policy = hex::encode(pa.policy());
                        pa.assets()
                            .iter()
                            .map(|a| AssetUnit {
                                policy: policy.clone(),
                                name_hex: hex::encode(a.name()),
                                quantity: a.any_coin().unsigned_abs() as u64,
                            })
                            .collect::<Vec<_>>()
                    })
                    .take(MAX_UNITS_PER_OUTPUT)
                    .collect()
            };
            if assets as usize > MAX_UNITS_PER_OUTPUT {
                // Netting can no longer be trusted for this output — say so
                // rather than quietly showing a wrong (or empty) move list.
                tracing::warn!(
                    assets,
                    cap = MAX_UNITS_PER_OUTPUT,
                    "output exceeds unit cap; asset moves for this tx are partial"
                );
            }
            decoded.push(Decoded {
                address: addr.to_string(),
                lovelace: value.coin(),
                assets,
                units,
                matched,
                script,
            });
        }
        if !any {
            continue;
        }
        let inputs: Vec<([u8; 32], u32)> = tx
            .consumes()
            .iter()
            .map(|i| (h32(i.hash()), i.index() as u32))
            .collect();
        let hash = h32(tx.hash());
        // Decoded once for the whole tx, not per target: the mint field is a
        // property of the transaction, and two watched wallets in one mint
        // both want the same answer.
        let minted = minted_units(tx);
        for (t, _) in targets.iter().enumerate() {
            let mut out_hits = Vec::new();
            let mut other_outputs = Vec::new();
            for (idx, d) in decoded.iter().enumerate() {
                if d.matched.contains(&t) {
                    out_hits.push(OutHit {
                        index: idx as u32,
                        lovelace: d.lovelace,
                        assets: d.assets,
                        units: d.units.clone(),
                        script: d.script,
                    });
                } else {
                    other_outputs.push((d.address.clone(), d.lovelace));
                }
            }
            if out_hits.is_empty() {
                continue;
            }
            out.push(FoundTx {
                target_idx: t,
                slot,
                tx_idx: ti as u32,
                hash,
                out_hits,
                inputs: inputs.clone(),
                total_outputs: outputs.len() as u32,
                other_outputs,
                minted: minted.clone(),
            });
        }
    }
}

fn extract_sweep_hits(
    block: &MultiEraBlock<'_>,
    _needles: &Needles<'_>,
    owned: &crate::classify::OwnedSet,
    out: &mut Vec<FoundTx>,
) {
    let slot = block.slot();
    for (ti, tx) in block.txs().iter().enumerate() {
        let inputs: Vec<([u8; 32], u32)> = tx
            .consumes()
            .iter()
            .map(|i| (h32(i.hash()), i.index() as u32))
            .collect();
        if !inputs.iter().any(|oref| owned.contains_key(oref)) {
            continue;
        }
        let outputs = tx.outputs();
        let other_outputs = outputs
            .iter()
            .filter_map(|o| {
                let addr = o.address().ok()?;
                Some((addr.to_string(), o.value().coin()))
            })
            .collect();
        out.push(FoundTx {
            target_idx: 0,
            slot,
            tx_idx: ti as u32,
            hash: h32(tx.hash()),
            out_hits: Vec::new(),
            inputs,
            total_outputs: outputs.len() as u32,
            other_outputs,
            // Pass B sweeps txs that SPEND a known output — a wallet giving
            // something up. Its mint field is read here too: a burn is exactly
            // this shape, and calling it a plain send would hide the asset
            // ceasing to exist.
            minted: minted_units(tx),
        });
    }
}
