//! `slotling-backtest` — what a block-lottery threshold would actually have done.
//!
//! Walks certified Mithril history and, for every Shelley-or-later block,
//! derives the application lottery value `blake2b-256(domain || VRF output)`,
//! then counts how many blocks fall below each candidate threshold. That
//! answers the one question that sets a mint's pace: at threshold `t`, how
//! many blocks a day hatch, and whose?
//!
//! # The leader column is a control, and it is meant to look wrong
//!
//! Every threshold is also tallied against the chain's OWN leader value — what
//! you get by selecting on `MultiEraHeader::leader_vrf_output()` directly. A
//! block exists *because* its leader value fell below its producer's
//! stake-dependent threshold `T(sigma) = 1-(1-f)^sigma`, so across blocks that
//! were actually made, leader values are uniform on `[0, T(sigma))`, not on
//! `[0, 1)`. A fixed threshold applied to them therefore fires far too often,
//! and hardest for the smallest pools. The lottery column should track its
//! expectation; the leader column should not. Printed side by side, the run is
//! its own proof that the lottery value is the right number to select on.
//!
//! The leader value here is pallas's `derive_tagged_vrf_output`, the node's own
//! range extension, so this tool measures the chain rather than a local
//! reimplementation of it.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::Parser;
use mitos_chain_walk::mithril::CHUNK_SLOTS;
use mitos_chain_walk::open_blocks;
use pallas_crypto::hash::Hasher;
use pallas_traverse::{MultiEraBlock, MultiEraHeader};

/// Slots per second is 1 from Shelley on, so a slot span is a second span.
const SECS_PER_DAY: f64 = 86_400.0;

/// Bits an `f64` mantissa holds exactly. Mirrors `chain_heartbeat::VrfValue`:
/// dividing all 64 leading bits would round the largest values up to exactly
/// `1.0` and break the half-open range a fraction promises.
const F64_MANTISSA_BITS: u32 = 53;
const TWO_POW_53: f64 = 9_007_199_254_740_992.0;

#[derive(Parser)]
#[command(
    version,
    about = "Measure a block-lottery threshold over certified Cardano history"
)]
struct Cli {
    /// Immutable DB (Mithril snapshot) directory.
    #[arg(long, default_value = "/opt/market-ledger/snapshot-full/db/immutable")]
    immutable: PathBuf,

    /// Domain tag for the lottery value. Version it: a later rule change must
    /// not be mistakable for the old one.
    #[arg(long, default_value = "slotlings-v1")]
    domain: String,

    /// Candidate thresholds, as fractions of the value's bound. The first is
    /// the "primary" one the per-pool and CSV detail is reported for.
    #[arg(
        long,
        value_delimiter = ',',
        default_values_t = [0.05, 0.01, 0.005_555_6, 0.001, 0.000_5]
    )]
    thresholds: Vec<f64>,

    /// First slot to include.
    #[arg(long, default_value_t = 0)]
    from_slot: u64,

    /// Last slot to include, exclusive. Defaults to the end of the chunks.
    #[arg(long)]
    to_slot: Option<u64>,

    /// Walk only the first N chunks of the range — a quick shape-check before
    /// committing to the whole of history.
    #[arg(long)]
    sample_chunks: Option<usize>,

    #[arg(long, default_value_t = 8)]
    threads: usize,

    /// Write every block qualifying at the primary threshold here, as CSV.
    #[arg(long)]
    csv: Option<PathBuf>,
}

/// What one block contributes. Byron and epoch-boundary blocks have no VRF and
/// never become one of these.
struct BlockFacts {
    slot: u64,
    height: u64,
    pool: [u8; 28],
    body_size: u64,
    tx_count: u32,
    /// Slots since the previous block on chain.
    ///
    /// ⚠️ MEASURED, not assumed: this DOES correlate with how full the block
    /// is — Pearson +0.52, Spearman +0.67 over a year of hatches. An earlier
    /// version of this comment claimed it could not, reasoning that block
    /// arrival is memoryless. Memorylessness governs ARRIVAL TIMES; it says
    /// nothing about block contents, and a longer gap gives the mempool more
    /// time to fill, so the next block is bigger.
    ///
    /// `body_size` and `tx_count` are tighter still (Spearman 0.90) and are
    /// one axis wearing two hats. Measure the joint distribution before
    /// selling any two block facts as two traits.
    gap: Option<u64>,
    epoch: u64,
    slot_in_epoch: u64,
    /// The lottery draw for the configured domain.
    lottery: f64,
    /// The chain's own leader value, as a fraction of its bound.
    leader: f64,
}

#[derive(Clone)]
struct Qualifying {
    slot: u64,
    height: u64,
    pool: [u8; 28],
    body_size: u64,
    tx_count: u32,
    gap: Option<u64>,
    epoch: u64,
    slot_in_epoch: u64,
    lottery: f64,
}

#[derive(Default)]
struct Tally {
    blocks: u64,
    /// Byron-era and epoch-boundary blocks, which carry no VRF.
    no_vrf: u64,
    first_slot: Option<u64>,
    last_slot: u64,
    body_size_total: u128,
    /// Hits per configured threshold, for each of the two values.
    lottery_hits: Vec<u64>,
    leader_hits: Vec<u64>,
    /// Blocks produced per pool, over the whole walk.
    pool_blocks: HashMap<[u8; 28], u64>,
    /// Pools selected at the primary threshold, by each value.
    lottery_pools: HashMap<[u8; 28], u64>,
    leader_pools: HashMap<[u8; 28], u64>,
    qualifying: Vec<Qualifying>,
}

impl Tally {
    fn new(thresholds: usize) -> Self {
        Self {
            lottery_hits: vec![0; thresholds],
            leader_hits: vec![0; thresholds],
            ..Default::default()
        }
    }

    fn record(&mut self, facts: &BlockFacts, thresholds: &[f64], keep_detail: bool) {
        self.blocks += 1;
        self.first_slot = Some(self.first_slot.unwrap_or(facts.slot).min(facts.slot));
        self.last_slot = self.last_slot.max(facts.slot);
        self.body_size_total += u128::from(facts.body_size);
        *self.pool_blocks.entry(facts.pool).or_default() += 1;

        for (i, threshold) in thresholds.iter().enumerate() {
            if facts.lottery < *threshold {
                self.lottery_hits[i] += 1;
                if i == 0 {
                    *self.lottery_pools.entry(facts.pool).or_default() += 1;
                    if keep_detail {
                        self.qualifying.push(Qualifying {
                            slot: facts.slot,
                            height: facts.height,
                            pool: facts.pool,
                            body_size: facts.body_size,
                            tx_count: facts.tx_count,
                            gap: facts.gap,
                            epoch: facts.epoch,
                            slot_in_epoch: facts.slot_in_epoch,
                            lottery: facts.lottery,
                        });
                    }
                }
            }
            if facts.leader < *threshold {
                self.leader_hits[i] += 1;
                if i == 0 {
                    *self.leader_pools.entry(facts.pool).or_default() += 1;
                }
            }
        }
    }

    fn merge(&mut self, other: Tally) {
        self.blocks += other.blocks;
        self.no_vrf += other.no_vrf;
        self.body_size_total += other.body_size_total;
        self.last_slot = self.last_slot.max(other.last_slot);
        self.first_slot = match (self.first_slot, other.first_slot) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        for (i, hits) in other.lottery_hits.iter().enumerate() {
            self.lottery_hits[i] += hits;
        }
        for (i, hits) in other.leader_hits.iter().enumerate() {
            self.leader_hits[i] += hits;
        }
        for (pool, n) in other.pool_blocks {
            *self.pool_blocks.entry(pool).or_default() += n;
        }
        for (pool, n) in other.lottery_pools {
            *self.lottery_pools.entry(pool).or_default() += n;
        }
        for (pool, n) in other.leader_pools {
            *self.leader_pools.entry(pool).or_default() += n;
        }
        self.qualifying.extend(other.qualifying);
    }
}

/// A big-endian byte string as a fraction of `2^(8*len)`, in `[0, 1)`.
fn fraction(bytes: &[u8]) -> f64 {
    if bytes.len() < 8 {
        // Too short to be a VRF value; never let it win a threshold test.
        return 1.0;
    }
    let mut lead = [0u8; 8];
    lead.copy_from_slice(&bytes[..8]);
    (u64::from_be_bytes(lead) >> (64 - F64_MANTISSA_BITS)) as f64 / TWO_POW_53
}

fn facts(block: &MultiEraBlock<'_>, domain: &[u8], prev_slot: Option<u64>) -> Option<BlockFacts> {
    let header = block.header();
    // The RAW output, which the producer cannot choose — not the already
    // range-extended leader value.
    let (vrf_output, body_size) = match &header {
        MultiEraHeader::BabbageCompatible(x) => (
            x.header_body.vrf_result.0.to_vec(),
            x.header_body.block_body_size,
        ),
        MultiEraHeader::ShelleyCompatible(x) => (
            x.header_body.leader_vrf.0.to_vec(),
            x.header_body.block_body_size,
        ),
        MultiEraHeader::Byron(_) | MultiEraHeader::EpochBoundary(_) => return None,
    };

    let mut pool = [0u8; 28];
    pool.copy_from_slice(Hasher::<224>::hash(header.issuer_vkey()?).as_ref());

    let mut tagged = Vec::with_capacity(domain.len() + vrf_output.len());
    tagged.extend_from_slice(domain);
    tagged.extend_from_slice(&vrf_output);

    let slot = block.slot();
    let (epoch, slot_in_epoch) = epoch_position(slot);
    Some(BlockFacts {
        slot,
        height: block.number(),
        pool,
        body_size,
        tx_count: block.tx_count() as u32,
        gap: prev_slot.map(|p| slot.saturating_sub(p)),
        epoch,
        slot_in_epoch,
        lottery: fraction(Hasher::<256>::hash(&tagged).as_ref()),
        leader: fraction(&header.leader_vrf_output().ok()?),
    })
}

/// Mainnet slot → `(epoch, slot within that epoch)`.
///
/// Shelley began at epoch 208, slot 4_492_800, and every epoch since is
/// 432_000 one-second slots. A slot↔time mapping that forgets the era offset
/// is wrong by weeks and says nothing about it, so this is checked against a
/// value computed elsewhere — see the test.
fn epoch_position(slot: u64) -> (u64, u64) {
    const SHELLEY_START_SLOT: u64 = 4_492_800;
    const SHELLEY_START_EPOCH: u64 = 208;
    const EPOCH_SLOTS: u64 = 432_000;
    let since = slot.saturating_sub(SHELLEY_START_SLOT);
    (
        SHELLEY_START_EPOCH + since / EPOCH_SLOTS,
        since % EPOCH_SLOTS,
    )
}

/// Sorted chunk numbers, NEWEST EXCLUDED — the newest immutable file is still
/// being appended to and Mithril re-ships it whole on every refresh. Same rule
/// as `tx_index::extract::list_chunks`.
fn list_chunks(immutable: &Path) -> Result<Vec<u64>> {
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
        bail!(
            "need at least 2 chunk files under {} (the newest is excluded as still-growing)",
            immutable.display()
        );
    }
    nums.pop();
    Ok(nums)
}

fn walk_band(
    immutable: &Path,
    from_slot: u64,
    to_slot: u64,
    domain: &[u8],
    thresholds: &[f64],
    keep_detail: bool,
) -> Result<Tally> {
    let mut tally = Tally::new(thresholds.len());
    let blocks = open_blocks(immutable, Some((from_slot, Vec::new())))
        .with_context(|| format!("seeking to slot {from_slot}"))?;
    let mut prev_slot: Option<u64> = None;
    for raw in blocks {
        let raw = raw.map_err(|e| anyhow::anyhow!("reading block: {e:?}"))?;
        let block =
            MultiEraBlock::decode(&raw).map_err(|e| anyhow::anyhow!("decoding block: {e:?}"))?;
        if block.slot() >= to_slot {
            break;
        }
        match facts(&block, domain, prev_slot) {
            Some(facts) => tally.record(&facts, thresholds, keep_detail),
            None => tally.no_vrf += 1,
        }
        // Every block advances the clock, including the Byron ones no beat can
        // be made from: a gap is to the previous BLOCK, never to the previous
        // qualifying one.
        prev_slot = Some(block.slot());
    }
    Ok(tally)
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let cli = Cli::parse();

    if cli.thresholds.is_empty() {
        bail!("give at least one --thresholds value");
    }
    if let Some(bad) = cli.thresholds.iter().find(|t| !(**t > 0.0 && **t < 1.0)) {
        bail!("threshold {bad} is not a fraction in (0, 1)");
    }

    let chunks = list_chunks(&cli.immutable)?;
    let first_chunk = *chunks.first().expect("checked non-empty");
    let last_chunk = *chunks.last().expect("checked non-empty");

    let mut from_chunk = (cli.from_slot / CHUNK_SLOTS).max(first_chunk);
    let mut to_chunk = match cli.to_slot {
        Some(slot) => (slot / CHUNK_SLOTS).min(last_chunk),
        None => last_chunk,
    };
    if let Some(sample) = cli.sample_chunks {
        to_chunk = to_chunk.min(from_chunk + sample as u64 - 1);
    }
    if from_chunk > to_chunk {
        bail!("empty chunk range: {from_chunk}..={to_chunk}");
    }
    from_chunk = from_chunk.max(first_chunk);

    let to_slot = cli
        .to_slot
        .unwrap_or(u64::MAX)
        .min((to_chunk + 1) * CHUNK_SLOTS);
    let threads = cli.threads.max(1);
    let span = to_chunk - from_chunk + 1;
    let per_band = span.div_ceil(threads as u64);

    tracing::info!(
        chunks = span,
        from_chunk,
        to_chunk,
        threads,
        domain = %cli.domain,
        "slotling-backtest: walking"
    );
    let started = Instant::now();

    let domain = cli.domain.as_bytes();
    let thresholds = cli.thresholds.clone();
    let keep_detail = cli.csv.is_some();
    let immutable = cli.immutable.as_path();

    let parts: Vec<Result<Tally>> = std::thread::scope(|s| {
        let mut handles = Vec::new();
        for band in 0..threads as u64 {
            let band_first = from_chunk + band * per_band;
            if band_first > to_chunk {
                break;
            }
            let band_last = (band_first + per_band - 1).min(to_chunk);
            let band_from = (band_first * CHUNK_SLOTS).max(cli.from_slot);
            let band_to = ((band_last + 1) * CHUNK_SLOTS).min(to_slot);
            let thresholds = thresholds.clone();
            handles.push(s.spawn(move || {
                walk_band(
                    immutable,
                    band_from,
                    band_to,
                    domain,
                    &thresholds,
                    keep_detail,
                )
            }));
        }
        handles
            .into_iter()
            .map(|h| h.join().expect("band"))
            .collect()
    });

    let mut tally = Tally::new(thresholds.len());
    for part in parts {
        tally.merge(part?);
    }

    report(&cli, &tally, &thresholds, started.elapsed().as_secs_f64())?;
    Ok(())
}

fn report(cli: &Cli, tally: &Tally, thresholds: &[f64], wall_secs: f64) -> Result<()> {
    let blocks = tally.blocks;
    if blocks == 0 {
        bail!("no Shelley-or-later blocks in the requested range");
    }
    let first = tally.first_slot.unwrap_or_default();
    let days = ((tally.last_slot - first) as f64 / SECS_PER_DAY).max(f64::EPSILON);
    let blocks_per_day = blocks as f64 / days;

    let mut out = String::new();
    writeln!(
        out,
        "\nwalked {blocks} blocks with a VRF ({no_vrf} Byron/EBB skipped) \
         over slots {first}..{last} — {days:.1} days, {blocks_per_day:.0} blocks/day, \
         mean body {mean_body:.0} bytes, in {wall_secs:.1}s",
        no_vrf = tally.no_vrf,
        last = tally.last_slot,
        mean_body = tally.body_size_total as f64 / blocks as f64,
    )?;
    writeln!(out, "domain tag: {:?}\n", cli.domain)?;

    writeln!(
        out,
        "{:>10}  {:>10} {:>9} {:>7}   {:>10} {:>9} {:>7}",
        "threshold", "lottery", "per day", "obs/exp", "leader", "per day", "obs/exp"
    )?;
    for (i, threshold) in thresholds.iter().enumerate() {
        let expected = blocks as f64 * threshold;
        let lottery = tally.lottery_hits[i];
        let leader = tally.leader_hits[i];
        writeln!(
            out,
            "{threshold:>10.6}  {lottery:>10} {lot_day:>9.2} {lot_ratio:>7.2}   \
             {leader:>10} {lead_day:>9.2} {lead_ratio:>7.2}",
            lot_day = lottery as f64 / days,
            lot_ratio = lottery as f64 / expected,
            lead_day = leader as f64 / days,
            lead_ratio = leader as f64 / expected,
        )?;
    }

    writeln!(
        out,
        "\nobs/exp near 1.00 means the draw is uniform over blocks. The leader \
         column is expected to drift: it is bounded by each producer's own \
         stake-dependent threshold, not by 1."
    )?;

    let primary = thresholds[0];
    writeln!(
        out,
        "\nat the primary threshold {primary:.6}: {pools} distinct pools selected, \
         out of {total} that produced blocks",
        pools = tally.lottery_pools.len(),
        total = tally.pool_blocks.len(),
    )?;
    writeln!(
        out,
        "  (the leader value would have selected {} pools — a different, \
         stake-skewed set)",
        tally.leader_pools.len()
    )?;

    let mut top: Vec<_> = tally.pool_blocks.iter().collect();
    top.sort_by_key(|(_, n)| std::cmp::Reverse(**n));
    writeln!(out, "\nbusiest pools (clan rarity is the inverse of this):")?;
    for (pool, produced) in top.iter().take(5) {
        let hatched = tally.lottery_pools.get(*pool).copied().unwrap_or(0);
        writeln!(
            out,
            "  {} {produced:>8} blocks  {hatched:>5} hatched  {share:>6.3}% of blocks",
            hex::encode(pool),
            share = **produced as f64 * 100.0 / blocks as f64,
        )?;
    }
    println!("{out}");

    if let Some(path) = &cli.csv {
        let mut file =
            std::fs::File::create(path).with_context(|| format!("creating {}", path.display()))?;
        writeln!(
            file,
            "slot,height,pool,body_size,tx_count,gap,epoch,slot_in_epoch,lottery"
        )?;
        let mut rows = tally.qualifying.clone();
        rows.sort_by_key(|q| q.slot);
        for q in rows {
            // An empty `gap` is the first block of a band, which has no
            // predecessor inside it — NOT a zero-slot gap. Blank rather than 0
            // so an analysis cannot average the two together.
            let gap = q.gap.map(|g| g.to_string()).unwrap_or_default();
            writeln!(
                file,
                "{},{},{},{},{},{},{},{},{:.9}",
                q.slot,
                q.height,
                hex::encode(q.pool),
                q.body_size,
                q.tx_count,
                gap,
                q.epoch,
                q.slot_in_epoch,
                q.lottery
            )?;
        }
        tracing::info!(path = %path.display(), "wrote qualifying blocks");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Checked against a number this crate did not compute: the live
    /// `/heartbeat` snapshot reported epoch 655, slot_in_epoch 345_935 at slot
    /// 197_942_735. An era offset that is forgotten or wrong puts this out by
    /// weeks while still looking entirely plausible, so the assertion has to
    /// come from outside.
    #[test]
    fn the_epoch_of_a_slot_matches_what_the_chain_reports() {
        assert_eq!(epoch_position(197_942_735), (655, 345_935));
        // Shelley's first slot IS the epoch-208 boundary, and epochs run
        // 432_000 slots from there.
        assert_eq!(epoch_position(4_492_800), (208, 0));
        assert_eq!(epoch_position(4_492_800 + 432_000), (209, 0));
        assert_eq!(epoch_position(4_492_800 + 432_000 - 1), (208, 431_999));
    }
}
