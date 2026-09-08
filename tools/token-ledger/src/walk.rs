//! `walk` — iterate certified immutable-DB history and record every movement
//! of the watched asset, or of every asset under the watched policy, as signed
//! per-party per-unit deltas.
//!
//! Per transaction: take the spent watched outputs back out of the buffer
//! (that is the whole of input resolution — see `buffer`), buffer the produced
//! ones, read the mint field, and net it all into one signed delta per party
//! PER UNIT. A transaction that touches neither the buffer nor the mint field
//! is skipped before any allocation, which is what keeps the walk at
//! block-decode speed.
//!
//! [`Watched`] is the one place the unit-versus-policy decision is made; every
//! output, mint entry and buffer row below asks it rather than re-deriving the
//! answer.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use pallas_addresses::{Address, StakeAddress};
use pallas_traverse::MultiEraBlock;

use mitos_chain_walk::decode::{DecodedOutput, decode_tx};
use mitos_chain_walk::{open_blocks, slot_to_unix};

use crate::buffer::{BufferedOutput, OutrefBuffer};
// Both were modules here until 2026-09-08. They are crates now so the archive
// path and a Worker can reach them; aliased to their old names so the call
// sites below read unchanged.
use crate::registry;
use crate::store::{AssetMeta, Balance, Completeness, Ledger, TxRow};
use mitos_cohort as cohort;
use mitos_pool_observe as pools;

#[derive(clap::Args, Debug)]
pub struct WalkArgs {
    /// Data dir holding the immutable DB (expects `<data-dir>/immutable`).
    #[arg(long)]
    pub data_dir: PathBuf,

    /// Token registry TOML.
    #[arg(long, default_value = "tokens.toml")]
    pub tokens: PathBuf,

    /// Which token in the registry to walk.
    #[arg(long)]
    pub token: String,

    /// Ledger sqlite path (default: `<token>.db`).
    #[arg(long)]
    pub db: Option<PathBuf>,

    /// Start slot. Defaults to the resume cursor, else the token's
    /// `floor_slot`, else genesis.
    #[arg(long)]
    pub from_slot: Option<u64>,

    /// Ignore any resume cursor and buffered open set; walk from the floor.
    #[arg(long)]
    pub fresh: bool,

    /// Persist the outref buffer every N blocks. Larger is faster to walk and
    /// costs more re-walk after a crash.
    #[arg(long, default_value_t = 20_000)]
    pub buffer_every: u64,

    /// Stop after this many in-range blocks (smoke tests).
    #[arg(long)]
    pub max_blocks: Option<u64>,

    /// Sieve mode: memmem each raw block for the policy id and only DECODE
    /// on hit — the wallet-sieve technique applied to the walk. The gate is
    /// COMPLETE, not heuristic: any tx moving the asset must carry the
    /// policy id in an output's value or the mint field (the ledger's
    /// balance rule leaves no third way), so a skipped block cannot hold
    /// token history. The walk's CPU is almost entirely decode, so this is
    /// the difference between ~30 minutes and ~1 minute on a 2-year token.
    #[arg(long)]
    pub sieve: bool,
}

/// Bech32 stake address from a payment address's delegation part.
pub(crate) fn stake_of(addr: &str) -> Option<String> {
    match Address::from_bech32(addr).ok()? {
        Address::Shelley(sh) => {
            let stake: StakeAddress = sh.try_into().ok()?;
            stake.to_bech32().ok()
        }
        _ => None,
    }
}

/// Which assets under the policy this ledger follows.
///
/// An enum rather than an `Option<Vec<u8>>` threaded through six call sites: at
/// each of them the question is "does this name count?", and a bare `Option`
/// makes every one of them re-derive the answer — which is where a `None`
/// silently comes to mean "no assets" instead of "all of them".
pub(crate) enum Watched {
    /// One asset. The original behaviour, and still what every registered
    /// fungible token uses.
    Unit(Vec<u8>),
    /// Every asset under the policy — an NFT collection, or a policy that
    /// minted under more than one name.
    Policy,
}

impl Watched {
    pub(crate) fn matches(&self, name: &[u8]) -> bool {
        match self {
            Watched::Unit(want) => name == want.as_slice(),
            Watched::Policy => true,
        }
    }
}

/// Every watched unit in this output, with its quantity.
///
/// Replaces the old `holds_watched` + `qty_from_output` pair. They were split
/// because `DecodedOutput.assets` carries asset *identity* only and the
/// quantity had to be read off the raw output; that is still true, so the cheap
/// identity scan still gates the raw read — the overwhelming majority of
/// outputs on the chain are not ours and never touch the second half.
///
/// Zero-quantity entries are dropped: an asset named in the value with a
/// quantity of nothing is not a holding, and letting it through would put a
/// no-op delta on the row.
pub(crate) fn units_in_output(
    tx: &pallas_traverse::MultiEraTx<'_>,
    out: &DecodedOutput,
    policy: &[u8],
    watched: &Watched,
) -> Vec<(Vec<u8>, i64)> {
    if !out
        .assets
        .iter()
        .any(|a| a.policy == policy && watched.matches(&a.name))
    {
        return Vec::new();
    }
    let outputs = tx.outputs();
    let Some(raw) = outputs.get(out.index as usize) else {
        return Vec::new();
    };
    let mut units: HashMap<Vec<u8>, i64> = HashMap::new();
    for pa in raw.value().assets().iter() {
        if pa.policy().as_ref() != policy {
            continue;
        }
        for a in pa.assets().iter() {
            if !watched.matches(a.name()) {
                continue;
            }
            // Output quantities are u64 on the wire but the delta arithmetic is
            // signed. A token whose supply exceeds i64::MAX would saturate here
            // rather than wrap — no such token exists on Cardano (SNEK, the
            // largest, is 7.6e10), but saturating beats a silent negative
            // balance.
            let qty = i64::try_from(a.output_coin().unwrap_or(0)).unwrap_or(i64::MAX);
            *units.entry(a.name().to_vec()).or_insert(0) += qty;
        }
    }
    units.retain(|_, q| *q != 0);
    units.into_iter().collect()
}

/// `(address, unit) → (stake, amount)`: one transaction's deltas, keyed the way
/// the conservation check reads them.
pub(crate) type PartyUnitDeltas = HashMap<(String, Vec<u8>), (Option<String>, i64)>;

/// One unit whose deltas did not sum to its net mint.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Breach {
    pub(crate) name: Vec<u8>,
    pub(crate) delta_sum: i128,
    pub(crate) net_mint: i64,
    pub(crate) kind: BreachKind,
}

/// Why a unit failed to balance — and whether that is a bug or a known gap.
///
/// **This distinction is what makes a progressive (windowed or reverse) walk
/// possible at all.** Such a walk starts with a cold buffer partway through
/// history, so transactions spending outputs created below its floor are
/// unresolvable *by construction*. Without separating the two, every one of
/// them logs as a violation and the real signal drowns — which is also a latent
/// trap in the existing `--from-slot`, where a cold buffer already produces
/// exactly this noise.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum BreachKind {
    /// `delta_sum > net_mint` — a MISSING NEGATIVE. Tokens arrived from a
    /// holder we never buffered, which is precisely the shape of an input whose
    /// creating output sits below the walk's floor.
    ///
    /// Expected while the buffer is incomplete, and the count of these is the
    /// honest progress metric for a progressive walk: it converges to zero as
    /// the floor is lowered toward the policy's first mint.
    Unattributed,
    /// `delta_sum < net_mint` — tokens left parties without arriving anywhere,
    /// or were minted without landing.
    ///
    /// A cold buffer CANNOT cause this: outputs are read in full from the
    /// transaction itself, so nothing we are owed can be missing on the
    /// positive side. This is a real attribution bug at any floor, and stays an
    /// error even on a deliberately partial walk.
    Impossible,
}

/// Conservation, PER UNIT: within a transaction the deltas for each unit must
/// sum to that unit's net mint — zero for a pure transfer, positive for a mint,
/// negative for a burn. Anything else means an input we failed to resolve or an
/// output we mis-read, which is the entire class of attribution bug this walker
/// could have, caught for free.
///
/// **Per unit rather than per transaction is what keeps the check alive
/// policy-wide.** Summed across units it would still balance while a gained
/// unit silently cancelled a lost one — precisely the mis-attribution the check
/// exists to catch, and the reason this ledger could only ever watch a single
/// asset before the unit reached the delta key.
///
/// Pure, and separated from the walk for that reason: it is the invariant the
/// whole walker rests on, and it needs no chain to exercise.
pub(crate) fn conservation_breaches(
    deltas: &PartyUnitDeltas,
    net_mint: &HashMap<Vec<u8>, i64>,
) -> Vec<Breach> {
    let mut sums: HashMap<&[u8], i128> = HashMap::new();
    for ((_, name), (_, amount)) in deltas {
        *sums.entry(name.as_slice()).or_insert(0) += *amount as i128;
    }
    // A unit that was minted but reached no party we track still has to be
    // checked — that case IS a violation, so seeding from the mint side too is
    // what stops it going unnoticed.
    for name in net_mint.keys() {
        sums.entry(name.as_slice()).or_insert(0);
    }
    let mut breaches: Vec<Breach> = sums
        .into_iter()
        .filter_map(|(name, delta_sum)| {
            let minted = net_mint.get(name).copied().unwrap_or(0);
            let kind = match delta_sum.cmp(&(minted as i128)) {
                std::cmp::Ordering::Equal => return None,
                std::cmp::Ordering::Greater => BreachKind::Unattributed,
                std::cmp::Ordering::Less => BreachKind::Impossible,
            };
            Some(Breach {
                name: name.to_vec(),
                delta_sum,
                net_mint: minted,
                kind,
            })
        })
        .collect();
    // Deterministic order so a log line — and a test — reads the same way twice.
    breaches.sort_by(|a, b| a.name.cmp(&b.name));
    breaches
}

/// Can a walk covering from `coverage_from` have a complete outref buffer?
///
/// Complete means every input carrying the asset was seen as an output first,
/// which holds when coverage begins at or below the policy's first mint. It is
/// what decides whether an unbalanced unit is a bug or a known gap — see
/// [`BreachKind`].
///
/// **Every unknown resolves to "not complete".** The two readings are not
/// symmetric: calling a partial walk complete turns its expected gaps into a
/// wall of logged violations and, worse, asserts a supply reconciliation it
/// cannot satisfy. Calling a complete walk partial merely reports a zero.
fn coverage_is_complete(coverage_from: Option<u64>, first_mint: Option<u64>) -> bool {
    match (coverage_from, first_mint) {
        // Genesis covers everything, whatever the registry says.
        (Some(0), _) => true,
        (Some(from), Some(first_mint)) => from <= first_mint,
        // No registered first mint: only a genesis walk can be known-complete.
        // Assuming otherwise would silently bless every shallow walk of an
        // unregistered policy — which is every on-demand one.
        (Some(_), None) => false,
        // A ledger with rows but no recorded floor predates the `walk_from`
        // cursor. Its coverage is genuinely unknown.
        (None, _) => false,
    }
}

pub fn run(args: WalkArgs) -> Result<()> {
    let token = registry::load_or_unit(&args.tokens, &args.token)?;
    let policy = token.policy_bytes()?;
    let asset_name = token.asset_name_bytes()?;
    // ONE decision about what counts, made here and carried through every
    // output, mint field and buffer entry below.
    let watched = match asset_name.clone() {
        Some(name) => Watched::Unit(name),
        None => Watched::Policy,
    };

    let immutable_dir = args.data_dir.join("immutable");
    if !immutable_dir.is_dir() {
        bail!(
            "immutable DB not found at {} — point --data-dir at a bootstrapped snapshot",
            immutable_dir.display()
        );
    }

    // Loaded before the walk: the datum-capture decision needs to know which
    // scripts are already-named lock platforms, so it only keeps datums for
    // the genuinely unnamed ones.
    let sinks: Vec<String> = registry::load_sinks(&args.tokens)?
        .into_iter()
        .map(|s| s.address)
        .collect();
    let lock_creds: Vec<[u8; 28]> = registry::load_lock_platforms(&args.tokens)?
        .iter()
        .map(registry::LockPlatform::cred_bytes)
        .collect::<Result<_>>()?;

    let db_path = args
        .db
        .clone()
        .unwrap_or_else(|| PathBuf::from(format!("{}.db", token.name)));
    let mut ledger = Ledger::open(&db_path)?;
    if args.fresh {
        tracing::warn!(db = %db_path.display(), "walk: --fresh — wiping ledger");
        ledger.wipe()?;
    }

    let mut buffer = if args.fresh {
        OutrefBuffer::default()
    } else {
        ledger.load_buffer()?
    };
    let resume = if args.fresh {
        None
    } else {
        ledger.resume_slot()?
    };

    // Resume means "start after the last committed block", not "at it" — the
    // cursor names a block already written, and re-walking it would double
    // every delta in it. `INSERT OR IGNORE` on tx_hash covers the race, but
    // being explicit is cheaper than relying on it.
    let floor = args
        .from_slot
        .or_else(|| resume.map(|s| s + 1))
        .or(token.floor_slot)
        .unwrap_or(0);

    if resume.is_none() && args.from_slot.is_none() && token.floor_slot.is_none() {
        tracing::warn!(
            "walk: no floor_slot for `{}` and no resume — walking from genesis. \
             Set floor_slot to the policy's first mint; it is both faster and \
             exactly as correct.",
            token.name
        );
    }

    // CAN this walk's buffer be complete?
    //
    // Complete means every input carrying the asset was seen as an output
    // first, which holds when coverage begins at or below the policy's first
    // mint. See [`BreachKind`] for why the distinction has to exist.
    //
    // Derived from the RECORDED floor, not from whether a cursor exists. A
    // resume inherits the coverage of the walk that filled the buffer, and that
    // walk may itself have been partial — assuming otherwise is how a
    // deliberately shallow walk comes to report its own known gaps as
    // conservation violations on the very next run.
    let coverage_from = match resume {
        // Continuing an existing ledger: the buffer on disk was built from
        // wherever that ledger actually reaches.
        Some(_) => ledger.walked_from()?,
        // Starting fresh: this run's own floor is the coverage.
        None => Some(floor),
    };
    let buffer_complete = coverage_is_complete(coverage_from, token.floor_slot);
    if !buffer_complete {
        tracing::warn!(
            coverage_from = ?coverage_from,
            floor_slot = ?token.floor_slot,
            "walk: PARTIAL — coverage does not reach the policy's first mint. \
             Movements whose source predates the floor are reported as \
             unattributed rather than as conservation violations."
        );
    }

    // Stamp the ledger with what it is about, so every read command is
    // self-describing instead of trusting a flag it could be given wrongly.
    // Written on resume too, so a registry edit (a decimals value arriving)
    // reaches an existing db without a re-walk.
    let decimals = token.resolved_decimals();
    ledger.put_meta(&token.name, &policy, asset_name.as_deref(), decimals)?;

    tracing::info!(
        token = %token.name,
        policy = %token.policy,
        decimals = ?decimals,
        floor,
        floor_file = ?token.floor_file(),
        resumed = resume.is_some(),
        open_utxos = buffer.len(),
        db = %db_path.display(),
        "walk: starting"
    );

    // An unregistered token has no floor_slot and nothing to resume from.
    // Rather than reading the whole chain through the sequential block
    // reader (~600 MB/s), discover the first policy-bearing chunk with the
    // parallel sieve scan (~3 GB/s, early-cutoff) and floor there — before
    // its first appearance the asset did not exist, so the floor is
    // complete by construction, same argument as a registry floor.
    let mut floor = floor;
    if args.sieve && floor == 0 {
        let chunks = chain_sieve::list_chunks(&immutable_dir, 0)?;
        let threads = std::thread::available_parallelism()
            .map(|n| n.get().min(8))
            .unwrap_or(4);
        tracing::info!(
            chunks = chunks.len(),
            threads,
            "walk: no floor — discovering first policy-bearing chunk"
        );
        match chain_sieve::first_hit_chunk(
            &immutable_dir,
            &chunks,
            threads,
            std::slice::from_ref(&policy),
            &|p| {
                if p.done.is_multiple_of(1_000) {
                    tracing::info!(
                        done = p.done,
                        total = p.total,
                        gb_per_s = format!("{:.2}", p.gb_per_s),
                        "walk: floor scan"
                    );
                }
            },
        )? {
            Some(chunk) => {
                floor = chunk * mitos_chain_walk::mithril::CHUNK_SLOTS;
                tracing::info!(chunk, floor, "walk: floor discovered");
            }
            None => tracing::warn!("walk: policy never appears on chain — nothing to walk"),
        }
    }

    // Seek straight to the floor rather than decoding everything below it.
    // An EMPTY block hash is pallas-hardano's slot-only fuzzy seek: it
    // binary-searches the chunk list and yields the first block at
    // `slot >= floor`, skipping whole chunk files. Decoding down to a
    // ~180M-slot floor instead is over an hour of CPU.
    //
    // Safe here in a way it is not for market-ledger, whose `--from-point`
    // warns about a cold buffer: our floor is the policy's first mint, before
    // which the asset did not exist, so there is nothing earlier to have
    // missed. A resume floor is likewise covered — the buffer is reloaded.
    let seek = (floor > 0).then(|| (floor, Vec::new()));
    let blocks = open_blocks(&immutable_dir, seek)?;

    // Sieve gate: one 28-byte SIMD needle, shared machinery with
    // wallet-sieve. Applied to the RAW block bytes before decode.
    let sieve = args
        .sieve
        .then(|| chain_sieve::Needles::new(std::slice::from_ref(&policy)))
        .transpose()?;
    let mut gated: u64 = 0;

    let mut scanned: u64 = 0;
    let mut in_range: u64 = 0;
    let mut touched: u64 = 0;
    let mut violations: u64 = 0;
    // Movements whose source output sits below this walk's floor. Zero on a
    // complete walk by definition; on a progressive one it is the gap, and it
    // shrinks as the floor is lowered.
    let mut unattributed: u64 = 0;
    let mut undecoded_locks: u64 = 0;
    // Seeded from the resume point, NOT 0. The final `commit_block` below
    // writes this as the cursor, and it is only updated inside the block loop —
    // so a resumed walk that finds NO new blocks (the snapshot has not advanced
    // since last time, which is the common case when re-running) would otherwise
    // write a cursor of 0 and destroy the resume point. The next walk then
    // restarts from genesis: ~23 minutes instead of seconds, silently.
    //
    // Found via the R2 layout, which keys artifacts by this slot — $Aliens
    // published under `…/0/` and the wrong version was visible in the key.
    let mut last_slot: u64 = resume.unwrap_or(0);

    for block in blocks {
        let bytes = block.map_err(|e| anyhow::anyhow!("reading block from chunk: {e:?}"))?;
        if let Some(needle) = &sieve
            && !needle.hit(&bytes)
        {
            gated += 1;
            if gated.is_multiple_of(1_000_000) {
                tracing::info!(gated, "walk: sieve skipping");
            }
            continue;
        }
        let blk = MultiEraBlock::decode(&bytes)
            .map_err(|e| anyhow::anyhow!("decoding block at ~#{scanned}: {e:?}"))?;
        scanned += 1;
        let slot = blk.slot();

        if slot < floor {
            if scanned.is_multiple_of(1_000_000) {
                tracing::info!(scanned, slot, "walk: skipping toward floor");
            }
            continue;
        }
        in_range += 1;
        last_slot = slot;
        let block_time = slot_to_unix(slot);

        let mut rows: Vec<TxRow> = Vec::new();

        for tx in blk.txs() {
            // Net mint per watched unit. Read from the raw tx — the shared
            // decode surface doesn't carry the mint field.
            let mut net_mint: HashMap<Vec<u8>, i64> = HashMap::new();
            for pa in tx.mints().iter() {
                if pa.policy().as_ref() != policy.as_slice() {
                    continue;
                }
                for a in pa.assets().iter() {
                    if !watched.matches(a.name()) {
                        continue;
                    }
                    *net_mint.entry(a.name().to_vec()).or_insert(0) += a.mint_coin().unwrap_or(0);
                }
            }

            let dtx = decode_tx(&tx);

            // Inputs: whatever this tx spent that we were holding.
            //
            // Keyed by (address, unit) rather than by address alone. Summed
            // across units, a tx moving one unit out and another in nets to
            // zero and hides BOTH moves — which is also what would silently
            // defeat the conservation check below.
            let mut deltas: HashMap<(String, Vec<u8>), (Option<String>, i64)> = HashMap::new();
            let mut locks_spent = Vec::new();
            for inp in &dtx.inputs {
                if let Some(b) = buffer.take(&inp.oref) {
                    // A spent lock closes here. Recorded so maturity can be
                    // stated at any past instant, not just at tip — the live
                    // set alone cannot say a lock existed and was claimed.
                    if b.unlock_ts_ms.is_some() {
                        locks_spent.push(inp.oref);
                    }
                    for (name, qty) in &b.units {
                        let e = deltas
                            .entry((b.address.clone(), name.clone()))
                            .or_insert((b.stake.clone(), 0));
                        e.1 -= qty;
                    }
                }
            }

            // Outputs: whatever it produced that we now hold.
            let mut pool_obs = Vec::new();
            let mut locks_created = Vec::new();
            for out in &dtx.outputs {
                let units = units_in_output(&tx, out, &policy, &watched);
                if units.is_empty() {
                    continue;
                }

                // A pool output is still an ordinary holder for balance
                // purposes — it just also contributes a point on the reserve
                // curve. Both, not either.
                let witness_datum: Option<&[u8]> = out
                    .datum_hash
                    .as_ref()
                    .and_then(|h| dtx.witness_datums.get(h))
                    .map(Vec::as_slice);
                // Recognised PER UNIT: a pool is keyed on the specific asset it
                // quotes, so asking about the policy as a whole has no meaning.
                // Policy-wide this simply never fires for an NFT collection,
                // which has no pool keyed on any one of its items.
                let mut pool_hit = false;
                for (name, qty) in &units {
                    if let Some(obs) = pools::recognise(out, *qty, &policy, name, witness_datum) {
                        pool_obs.push(obs);
                        pool_hit = true;
                    }
                }

                // A lock position carries its own schedule. Decode it here, on
                // the output, so "what is still locked" is a read of the live
                // UTxO set rather than a reconstruction from history.
                let datum = witness_datum.or(out.inline_datum.as_deref());
                let cred = cohort::payment_cred(&out.address);
                let is_lock_platform = cred.as_ref().is_some_and(|c| {
                    mitos_vesting_decode::crowd_lock::is_crowd_lock(c)
                        || mitos_vesting_decode::snek_fun::is_snek_fun(c)
                });

                let (unlock_ts_ms, owner_pkh) = if is_lock_platform {
                    match datum.and_then(mitos_vesting_decode::decode_vesting_datum) {
                        Some(d) => (Some(d.unlock_ts_ms), Some(d.owner_pkh_hex)),
                        None => {
                            undecoded_locks += 1;
                            (None, None)
                        }
                    }
                } else {
                    (None, None)
                };

                // Keep the datum for script outputs we could not name, so the
                // unclassified band can be interrogated offline rather than by
                // re-walking. Pools and known platforms already decoded theirs;
                // wallets have nothing to say.
                // Keep the datum for any non-wallet, non-pool output whose
                // schedule we failed to read — unnamed scripts AND registered
                // lock platforms whose shape we cannot decode yet. The latter
                // is the more valuable case: it is the raw material for writing
                // the missing decoder, and keying capture on "unnamed" alone
                // meant registering a platform silently stopped collecting the
                // evidence needed to understand it.
                let worth_keeping = !pool_hit
                    && unlock_ts_ms.is_none()
                    && cohort::classify(&out.address, &sinks, &[], &lock_creds).cohort
                        != cohort::Cohort::Wallet;
                let datum_cbor = worth_keeping.then(|| datum.map(<[u8]>::to_vec)).flatten();
                let datum_hash = out.datum_hash.as_ref().map(|h| h.as_ref().to_vec());

                if let Some(unlock) = unlock_ts_ms {
                    locks_created.push(crate::store::LockCreated {
                        oref: (dtx.tx_hash, out.index),
                        address: out.address.clone(),
                        // A lock's size is its total across units. Lock
                        // platforms are a fungible-token shape and no
                        // registered one escrows a mixed bag; a policy-wide
                        // walk over an NFT collection creates none of these at
                        // all, because the recognition is by payment credential.
                        qty: units.iter().map(|(_, q)| *q).sum(),
                        unlock_ts_ms: unlock,
                        owner_pkh: owner_pkh.clone(),
                    });
                }

                let stake = stake_of(&out.address);
                buffer.insert(
                    (dtx.tx_hash, out.index),
                    BufferedOutput {
                        address: out.address.clone(),
                        stake: stake.clone(),
                        units: units.clone(),
                        unlock_ts_ms,
                        owner_pkh,
                        datum_cbor,
                        datum_hash,
                    },
                );
                for (name, qty) in units {
                    let e = deltas
                        .entry((out.address.clone(), name))
                        .or_insert((stake.clone(), 0));
                    e.1 += qty;
                }
            }

            if deltas.is_empty()
                && net_mint.is_empty()
                && pool_obs.is_empty()
                && locks_created.is_empty()
                && locks_spent.is_empty()
            {
                continue;
            }

            for breach in conservation_breaches(&deltas, &net_mint) {
                match breach.kind {
                    // A real bug at any floor — a cold buffer cannot produce it.
                    BreachKind::Impossible => {
                        violations += 1;
                        tracing::error!(
                            tx = %hex::encode(dtx.tx_hash.as_ref()),
                            slot,
                            unit = %hex::encode(&breach.name),
                            delta_sum = %breach.delta_sum,
                            net_mint = breach.net_mint,
                            "walk: CONSERVATION VIOLATION — deltas do not sum to net mint"
                        );
                    }
                    // Tokens from a holder below the floor. On a complete walk
                    // this is still a bug and still counted; on a deliberately
                    // partial one it is the expected, converging gap — so it is
                    // reported separately either way rather than drowning the
                    // signal.
                    BreachKind::Unattributed if buffer_complete => {
                        violations += 1;
                        tracing::error!(
                            tx = %hex::encode(dtx.tx_hash.as_ref()),
                            slot,
                            unit = %hex::encode(&breach.name),
                            delta_sum = %breach.delta_sum,
                            net_mint = breach.net_mint,
                            "walk: CONSERVATION VIOLATION — value from an unbuffered holder \
                             on a walk whose buffer should be complete"
                        );
                    }
                    BreachKind::Unattributed => {
                        unattributed += 1;
                        tracing::debug!(
                            tx = %hex::encode(dtx.tx_hash.as_ref()),
                            slot,
                            unit = %hex::encode(&breach.name),
                            "walk: source below floor — unattributed"
                        );
                    }
                }
            }

            touched += 1;
            rows.push(TxRow {
                tx_hash: dtx.tx_hash,
                slot,
                block_time,
                net_mint: net_mint.into_iter().collect(),
                deltas: deltas
                    .into_iter()
                    .filter(|(_, (_, amount))| *amount != 0)
                    .map(
                        |((address, name), (stake, amount))| crate::store::DeltaRow {
                            address,
                            stake,
                            name,
                            amount,
                        },
                    )
                    .collect(),
                pools: pool_obs,
                locks_created,
                locks_spent,
            });
        }

        let persist_buffer = in_range.is_multiple_of(args.buffer_every);
        if !rows.is_empty() || persist_buffer {
            ledger.commit_block(&rows, slot, &blk.hash(), &buffer, persist_buffer)?;
        }

        if in_range.is_multiple_of(500_000) {
            tracing::info!(
                in_range,
                slot,
                touched,
                open_utxos = buffer.len(),
                live_qty = %buffer.total_qty(),
                "walk: progress"
            );
        }

        if let Some(max) = args.max_blocks
            && in_range >= max
        {
            tracing::info!(in_range, "walk: --max-blocks reached");
            break;
        }
    }

    // Final buffer persist so a resume picks up the true open set. The cursor
    // slot is real; the hash is a placeholder because we no longer hold the
    // last block. Nothing reads it yet — `follow`'s rollback path will, and
    // will need the real one.
    ledger.commit_block(
        &[],
        last_slot,
        &pallas_primitives::Hash::from([0u8; 32]),
        &buffer,
        true,
    )?;

    // Record the FLOOR this run covered, completing the coverage pair with the
    // forward cursor `commit_block` just wrote.
    //
    // At the END rather than the start, and this is the subtle half: lowering
    // the floor before the segment is walked would claim coverage of ground the
    // run has not reached yet, and a crash there leaves a ledger asserting a
    // contiguous range with a hole in it. Written here, `[walked_from, walk]`
    // is only ever widened by work that actually completed.
    //
    // `floor` is the EFFECTIVE floor, so it carries the sieve's first-hit
    // discovery when that ran — coverage from the policy's first appearance is
    // complete, because nothing below it holds the policy at all.
    ledger.set_walked_from(floor)?;
    // And the top. A forward walk's frontier is where it stopped, which is the
    // same slot the resume cursor names — recorded here too so `seal` reads ONE
    // coverage pair regardless of which direction built the ledger.
    ledger.set_walked_to(last_slot)?;
    // Written by the only code that can decide it: this function holds the
    // registry entry, and `buffer_complete` is the verdict every reader needs
    // but none can re-derive from the ledger alone.
    ledger.set_completeness(match buffer_complete {
        true => Completeness::Complete,
        false => Completeness::Partial,
    })?;

    // Classify from the addresses just recorded. Derived, so it costs a pass
    // over the party table and never a re-walk.
    let classified = ledger.classify_parties(&sinks, &lock_creds)?;
    tracing::info!(
        classified,
        sinks = sinks.len(),
        lock_platforms = lock_creds.len(),
        "walk: parties classified"
    );

    let (txs, delta_rows, parties) = ledger.counts()?;
    let minted = ledger.net_mint_total()?;
    let delta_sum = ledger.delta_total()?;
    let live = buffer.total_qty();

    tracing::info!(
        scanned,
        gated,
        in_range,
        touched,
        txs,
        delta_rows,
        parties,
        violations,
        unattributed,
        buffer_complete,
        "walk: done"
    );

    // End-to-end reconciliation. These three must agree; if they don't, every
    // number downstream is wrong and it is better to say so loudly here than
    // to ship a plausible chart.
    //
    // A PARTIAL walk cannot balance and must not claim to: its deltas are
    // missing every source below the floor, so `Σ deltas` legitimately exceeds
    // `Σ net_mint`. Asserting the identity anyway would print RECONCILIATION
    // FAILED on a walk that is behaving exactly as designed, and the one number
    // that means anything there is how much is still unattributed.
    let orphans = ledger.orphan_deltas()?;
    println!("net minted (Σ tx.net_mint)   = {minted}");
    println!("Σ all deltas                 = {delta_sum}");
    println!("live in buffer (circulating) = {live}");
    println!("orphan deltas                = {orphans}");
    if !buffer_complete {
        println!("unattributed movements       = {unattributed}");
        println!(
            "PARTIAL WALK — supply cannot balance from this floor; \
             lower it toward the policy's first mint to close the gap"
        );
    } else if minted as i128 != live || delta_sum != minted || orphans != 0 {
        println!("*** RECONCILIATION FAILED — supply does not balance ***");
    } else {
        println!("reconciled: supply balances");
    }
    if violations > 0 {
        println!("*** {violations} conservation violations — see logs ***");
    }
    if undecoded_locks > 0 {
        // Reported, never assumed liquid: a lock we could not read is not the
        // same as no lock, and the difference is supply the cascade would
        // otherwise hand to the float.
        println!("*** {undecoded_locks} lock outputs whose datum did not decode ***");
    }

    Ok(())
}

/// Re-derive cohorts without touching the chain.
///
/// The point of the separation: registering a new sink or landing a new pool
/// decoder reclassifies all of history in a second, because a cohort is a
/// function of the stored address rather than something the walk decided.
pub fn classify(db: &std::path::Path, tokens: &std::path::Path) -> Result<()> {
    let mut ledger = Ledger::open(db).with_context(|| format!("opening {}", db.display()))?;
    let sinks = registry::load_sinks(tokens)?;
    for s in &sinks {
        println!("sink {} — {}", s.address, s.evidence.trim());
    }
    let platforms = registry::load_lock_platforms(tokens)?;
    for p in &platforms {
        println!(
            "lock platform {} ({}) — {}",
            p.name,
            p.payment_cred,
            p.evidence.trim()
        );
    }
    let addresses: Vec<String> = sinks.iter().map(|s| s.address.clone()).collect();
    let creds: Vec<[u8; 28]> = platforms
        .iter()
        .map(registry::LockPlatform::cred_bytes)
        .collect::<Result<_>>()?;
    let n = ledger.classify_parties(&addresses, &creds)?;
    println!(
        "classified {n} parties against {} sink(s) and {} lock platform(s)",
        addresses.len(),
        creds.len()
    );
    Ok(())
}

/// Interrogate the unclassified band: do these contracts look like locks?
///
/// For every live output at a script address we could not name, try the shared
/// lock-datum decode. A platform that copied a known implementation — which is
/// common, these contracts get forked — will decode cleanly even though its
/// payment credential is not in any registry.
///
/// **This reports; it does not classify.** A `Constr 0 [Int, List[Bytes28]]` is
/// a shape, and shapes can coincide. What the probe produces is the evidence a
/// human needs to register a credential deliberately, which keeps the
/// classification ladder honest: registered credential is a decision, datum
/// shape is a hint.
pub fn probe(db: &std::path::Path) -> Result<()> {
    let ledger = Ledger::open(db).with_context(|| format!("opening {}", db.display()))?;
    let outputs = ledger.unnamed_script_outputs()?;
    if outputs.is_empty() {
        println!("no unnamed script outputs — nothing to probe");
        return Ok(());
    }

    // Group by payment credential, not address: a lock platform that glues a
    // per-locker stake part onto one shared script would otherwise look like
    // many unrelated contracts.
    let mut groups: HashMap<String, ProbeGroup> = HashMap::new();
    for out in &outputs {
        let cred = cohort::payment_cred(&out.address)
            .map(hex::encode)
            .unwrap_or_else(|| "?".into());
        let g = groups.entry(cred).or_default();
        g.utxos += 1;
        g.qty += out.qty;
        g.addresses.insert(out.address.clone());
        match out
            .datum_cbor
            .as_deref()
            .and_then(mitos_vesting_decode::decode_vesting_datum)
        {
            Some(d) => {
                g.decoded += 1;
                g.owners.insert(d.owner_pkh_hex);
                g.unlock_min = Some(
                    g.unlock_min
                        .map_or(d.unlock_ts_ms, |m: u64| m.min(d.unlock_ts_ms)),
                );
                g.unlock_max = Some(
                    g.unlock_max
                        .map_or(d.unlock_ts_ms, |m: u64| m.max(d.unlock_ts_ms)),
                );
            }
            None if out.datum_cbor.is_some() => g.datum_present_undecoded += 1,
            // A hash with no preimage is untestable, not negative. Note what
            // this bucket actually means: the walk already looked in the
            // CREATING transaction's witness set, so landing here says the
            // creator chose not to attach the datum — leaving the spend as the
            // only thing that can reveal it, and these outputs are unspent.
            //
            // Attaching at creation is a choice, not a rule. Every sampled DEX
            // pool does it (a batcher has to be able to read pool state), which
            // is why the PlutusV1 pool decoders need no deferred cache. A lock
            // contract has no such obligation. Counting this as "not a lock"
            // would be a conclusion the chain has not offered.
            None if out.datum_hash.is_some() => g.hash_only += 1,
            None => g.no_datum += 1,
        }
    }

    // Descending balance, then by CREDENTIAL — the tiebreak is load-bearing.
    // Groups come out of a `HashMap`, whose iteration order is randomised per
    // process, so two credentials holding the SAME amount swapped places
    // between runs of an otherwise identical command. Found by
    // `golden-capture.sh` on its first check: $CSWAP has two contracts holding
    // exactly 2,500,000,000 each, and they alternated.
    let mut ordered: Vec<_> = groups.into_iter().collect();
    ordered.sort_by(|(a_cred, a), (b_cred, b)| b.qty.cmp(&a.qty).then(a_cred.cmp(b_cred)));

    for (cred, g) in ordered {
        println!("\npayment credential {cred}");
        println!(
            "  {} tokens across {} UTxOs / {} address(es)",
            g.qty,
            g.utxos,
            g.addresses.len()
        );
        println!(
            "  lock-datum decode: {}/{} ok · {} readable but a different shape · \
             {} hash-only (UNTESTABLE — unspent) · {} no datum at all",
            g.decoded, g.utxos, g.datum_present_undecoded, g.hash_only, g.no_datum
        );
        if g.decoded == 0 && g.hash_only == 0 && g.datum_present_undecoded == 0 {
            println!("  ⇒ carries no datum — not a lock of any kind. Some other contract.");
        } else if g.decoded == 0 && g.datum_present_undecoded > 0 {
            println!("  ⇒ has state, but NOT the Shield/CrowdLock shape — a different design.");
        } else if g.decoded == 0 {
            println!(
                "  ⇒ undetermined: the datum is hash-only and unspent, so the chain \
                 has not revealed it. Not evidence either way."
            );
        }
        if g.decoded > 0 {
            println!(
                "  ⇒ MATCHES the Shield/CrowdLock lock shape. {} distinct owner(s).",
                g.owners.len()
            );
            if let (Some(lo), Some(hi)) = (g.unlock_min, g.unlock_max) {
                println!("     unlock {} .. {} (unix ms)", lo, hi);
            }
            println!(
                "     To count it as vesting, register this credential with evidence — \
                 a shape match is a hint, not a decision."
            );
        }
        for a in g.addresses.iter().take(3) {
            println!("     {a}");
        }
    }
    Ok(())
}

#[derive(Default)]
struct ProbeGroup {
    utxos: u64,
    qty: i64,
    decoded: u64,
    datum_present_undecoded: u64,
    hash_only: u64,
    no_datum: u64,
    addresses: std::collections::BTreeSet<String>,
    owners: std::collections::BTreeSet<String>,
    unlock_min: Option<u64>,
    unlock_max: Option<u64>,
}

pub fn stats(db: &std::path::Path, top: usize) -> Result<()> {
    let ledger = Ledger::open(db).with_context(|| format!("opening {}", db.display()))?;
    let (txs, deltas, parties) = ledger.counts()?;
    let balances = ledger.balances()?;
    let total: i128 = balances.iter().map(|b| b.amount as i128).sum();

    // Say what this ledger is about before saying anything about it. `stats`
    // takes only a path, so without this the output is a wall of numbers with
    // no stated subject — and the reader has to trust the filename.
    let meta = ledger.asset_meta()?;
    match &meta {
        Some(m) => println!(
            "ledger: {}  {}  {}",
            m.name,
            match &m.asset_name {
                Some(n) => format!("{}.{}", hex::encode(&m.policy), hex::encode(n)),
                None => format!("{} (whole policy)", hex::encode(&m.policy)),
            },
            match m.decimals {
                Some(d) => format!("{d} dp"),
                None => "decimals unknown".to_string(),
            }
        ),
        None => {
            println!("ledger: (walked before the asset was recorded — re-run `walk` to stamp it)")
        }
    }
    println!("txs {txs}  deltas {deltas}  parties seen {parties}");

    // IS THIS A BALANCE TABLE, OR A WINDOW OF MOVEMENT?
    //
    // `balances` sums every delta on record. That IS the holder table when the
    // walk reached the policy's first mint — every arrival has its matching
    // departure. On a PARTIAL ledger it is not: a reverse pass records the
    // arrival and leaves the departure below its floor, so the sums are net
    // movement inside the covered window and nothing more.
    //
    // Presenting them the same way is how "23 holders, total held 86" gets
    // printed for a 10,000-NFT collection with thousands of holders — measured,
    // on SpaceBudz, before this check existed. The numbers were not wrong; the
    // LABELS were, which is worse, because a wrong label is believed.
    let coverage = ledger.coverage()?;
    // THREE states, not two. "Not known to be complete" is its own answer: a
    // ledger written before the flag existed may well be complete — $PERP's
    // deltas sum to exactly its 1,000,000,000 supply with nothing unresolved —
    // and calling that PARTIAL is as much a mislabel as the reverse. Say what
    // is known, and say how to settle it.
    let complete = ledger.completeness()?;
    match complete {
        Completeness::Complete => {
            debug_assert!(complete.balances_are_holdings());
            println!("holders with non-zero balance: {}", balances.len());
            println!("total held: {total}");
        }
        Completeness::Partial => {
            println!(
                "PARTIAL LEDGER — coverage reaches back only to slot {}, so these are NET \
                 MOVEMENTS in that window, NOT holdings.",
                coverage
                    .walked_from
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "(unrecorded)".into())
            );
            println!(
                "parties that moved: {}  ·  net units moved: {total}  ·  \
                 movements still missing a source: {}",
                balances.len(),
                coverage.unresolved
            );
            println!("  deepen with `reverse` until the unresolved count stops falling.");
        }
        Completeness::Unrecorded => {
            println!(
                "COVERAGE UNRECORDED — this ledger predates the completeness flag, so \
                 whether these are holdings or a window of movement is not known."
            );
            println!(
                "non-zero balances: {}  ·  net units: {total}  ·  unresolved sources: {}",
                balances.len(),
                coverage.unresolved
            );
            println!("  re-run `walk` to stamp it — an incremental resume is enough.");
        }
    }
    println!();
    for b in balances.iter().take(top) {
        let pct = if total > 0 {
            100.0 * (b.amount as f64) / (total as f64)
        } else {
            0.0
        };
        let short = if b.address.len() > 32 {
            &b.address[..32]
        } else {
            &b.address
        };
        let cohort = b.cohort.as_deref().unwrap_or("?");
        // Stake is shown for grouping, never as identity — CSwap collapses all
        // of its pools onto one stake credential.
        let st = b.stake.as_deref().unwrap_or("-");
        let st_short = if st.len() > 20 { &st[..20] } else { st };
        println!(
            "{:>16}  {pct:5.2}%  {:<7}  {short}…  {st_short}",
            b.amount, cohort
        );
    }

    // EVERY REPORT BELOW DIVIDES BY `total` AS IF IT WERE SUPPLY.
    //
    // "where supply landed at mint", "how concentrated is the float", "what is
    // the market cap" — all three are statements about a whole history, and a
    // partial ledger has not got one. Worse, they would still PRINT: a mint
    // report over a window that never reached the mint shows the earliest
    // arrivals it happens to have and calls them the launch.
    //
    // Skipped rather than caveated, because a caveat above a plausible-looking
    // distribution table is not read.
    //
    // The asymmetry between this and `balances_are_holdings` is deliberate and
    // argued on `Completeness` itself, so both call sites cannot drift apart.
    if complete.supply_reports_defined() {
        mint_report(&ledger, total)?;
        cascade_report(&ledger, total)?;
        cap_report(&ledger, &balances, total, ledger.asset_meta()?.as_ref())?;
    } else {
        println!();
        println!(
            "mint, cascade and cap reports SKIPPED — each divides by supply, and a \
             partial ledger has no supply figure to divide by."
        );
    }
    Ok(())
}

/// Where supply landed at mint — the first question of a launch forensic.
fn mint_report(ledger: &Ledger, total: i128) -> Result<()> {
    let dist = ledger.mint_distribution()?;
    if dist.is_empty() {
        return Ok(());
    }
    println!();
    println!("AT MINT");
    for (address, cohort, got) in dist.iter().take(6) {
        let short = if address.len() > 40 {
            &address[..40]
        } else {
            address
        };
        println!(
            "  {:>16}  {:5.2}%  {:<7}  {short}…",
            got,
            100.0 * *got as f64 / total as f64,
            cohort.as_deref().unwrap_or("?")
        );
    }
    // A launchpad is the difference between "the team held the supply" and
    // "a contract sold it", and the two read identically on a holder chart.
    if let Some((_, cohort, got)) = dist.first()
        && cohort.as_deref() != Some("wallet")
        && *got as f64 / total as f64 > 0.5
    {
        println!(
            "  ⇒ {:.1}% went straight to a CONTRACT at mint — launchpad-shaped, \
             not a team allocation.",
            100.0 * *got as f64 / total as f64
        );
    }
    Ok(())
}

/// Split live lock positions into `(still_locked, matured, undated)`.
///
/// `undated` is vesting-cohort supply whose datum did not decode. It counts as
/// **locked**, deliberately: a lock we cannot read is not an absent lock, and
/// the failure mode of the opposite choice is handing supply to the float on
/// the strength of our own decode gap.
fn vesting_split(ledger: &Ledger) -> Result<(i64, i64, i64)> {
    let snap = ledger.locked_positions()?;
    let cohort_total: i64 = ledger
        .cohort_totals()?
        .iter()
        .find(|(c, _, _)| c == "vesting")
        .map(|(_, _, t)| *t)
        .unwrap_or(0);

    let Some(tip_secs) = snap.tip_time else {
        return Ok((cohort_total, 0, cohort_total));
    };
    let tip_ms = tip_secs.saturating_mul(1_000);
    let mut locked = 0i64;
    let mut matured = 0i64;
    for p in &snap.positions {
        if p.unlock_ts_ms > tip_ms {
            locked += p.qty;
        } else {
            matured += p.qty;
        }
    }
    let dated: i64 = locked + matured;
    let undated = (cohort_total - dated).max(0);
    Ok((locked + undated, matured, undated))
}

/// The supply cascade — the reduction to reachable float, with the evidence
/// quality of each step visible.
///
/// The reduction *is* the finding, so it is rendered rather than collapsed
/// into one number. Only `burn` is subtracted: it is the one cohort that is
/// provably gone. `script` stays in the float because we cannot show it is
/// locked — but it is its own band, because the difference between "known
/// liquid" and "not yet looked at" is what this view exists to make visible.
fn cascade_report(ledger: &Ledger, total: i128) -> Result<()> {
    let totals = ledger.cohort_totals()?;
    if totals.iter().all(|(c, _, _)| c == "unclassified") {
        println!("\n(parties unclassified — run `classify`)");
        return Ok(());
    }

    let get = |name: &str| -> i64 {
        totals
            .iter()
            .find(|(c, _, _)| c == name)
            .map(|(_, _, t)| *t)
            .unwrap_or(0)
    };
    let count = |name: &str| -> i64 {
        totals
            .iter()
            .find(|(c, _, _)| c == name)
            .map(|(_, n, _)| *n)
            .unwrap_or(0)
    };

    let burn = get("burn");
    let pool = get("pool");
    let vesting = get("vesting");
    let script = get("script");
    let wallet = get("wallet");
    let float = total as i64 - burn;
    let pct = |v: i64| 100.0 * v as f64 / total as f64;
    let (locked, matured, undated) = vesting_split(ledger)?;

    println!();
    println!("SUPPLY CASCADE");
    println!("  nominal supply       {total:>16}");
    println!(
        "− provably unspendable {:>14}  {:5.2}%  ({} sink addr, basis: proven)",
        burn,
        pct(burn),
        count("burn")
    );
    println!("= reachable float      {float:>14}  {:5.2}%", pct(float));
    println!(
        "    of which pooled    {:>14}  {:5.2}%  ({} pools, basis: decoded)",
        pool,
        pct(pool),
        count("pool")
    );
    if vesting > 0 {
        println!(
            "    vesting            {:>14}  {:5.2}%  ({} addrs, basis: decoded)",
            vesting,
            pct(vesting),
            count("vesting")
        );
        println!("      still locked   {:>14}  {:5.2}%", locked, pct(locked));
        // Matured-but-unclaimed is the interesting half: supply that is free
        // to move and has not, which is latent sell pressure rather than a
        // lock. A single "vesting" number hides it completely.
        println!(
            "      matured        {:>14}  {:5.2}%  (claimable now, unclaimed)",
            matured,
            pct(matured)
        );
        if undated > 0 {
            println!(
                "      datum unread   {:>14}  {:5.2}%  (treated as LOCKED — a lock we \
                 cannot read is not an absent lock)",
                undated,
                pct(undated)
            );
        }
    }
    println!(
        "    script-held        {:>14}  {:5.2}%  ({} addrs, basis: chain — KIND UNKNOWN)",
        script,
        pct(script),
        count("script")
    );
    println!(
        "    key wallets        {:>14}  {:5.2}%  ({} addrs, basis: chain)",
        wallet,
        pct(wallet),
        count("wallet")
    );
    if script > 0 {
        println!(
            "  note: script-held may be vesting, locked, or an open order — \
             identifying it needs the declared layer, not the chain."
        );
    }
    Ok(())
}

/// The cap band: notional against realisable, and the ratio between them.
///
/// Three deliberate choices, each argued in `TOKEN_LEDGER.md`:
///
/// - **Pooled supply counts as float.** It is the most reachable supply on
///   chain — buying it needs no counterparty to agree. Excluding it would
///   quietly pre-apply half the correction the realisable figure exists to
///   make, and the honesty ratio would then understate the illiquidity.
/// - **The realisable quantity excludes pooled supply.** Forced, not chosen:
///   selling the pool into itself is incoherent. So the two figures have
///   different denominators on purpose, which is why it is spelled out here
///   rather than left for a reader to infer a bug.
/// - **Reserves are summed across pools before the curve is walked.** For
///   constant-product pools sitting at the same price, an optimal split across
///   them yields exactly the merged-pool result, so this is exact rather than
///   an approximation — but it stops being exact if the pools' prices diverge,
///   which is worth revisiting when a third pool appears.
///
/// `meta` supplies the token's decimals. Prices are quoted per WHOLE token, so
/// without it a 6-dp token's spot renders a million times too small — CSWAP
/// printed `0.00000000 ADA` at 8 decimal places against a real 0.00231. Only
/// the *display* is affected: the caps multiply a raw supply by a raw-unit
/// price, so the scale cancels and those figures were always right.
fn cap_report(
    ledger: &Ledger,
    balances: &[Balance],
    total: i128,
    meta: Option<&AssetMeta>,
) -> Result<()> {
    let tips = ledger.pool_tips()?;
    let (pool_count, curve_rows) = ledger.pool_counts()?;
    println!();
    println!("pools {pool_count}  reserve-curve rows {curve_rows}");
    if tips.is_empty() {
        println!("no pools decoded — no price, and therefore no cap. Not zero: undefined.");
        return Ok(());
    }

    let by_cohort = |name: &str| -> i128 {
        balances
            .iter()
            .filter(|b| b.cohort.as_deref() == Some(name))
            .map(|b| b.amount as i128)
            .sum()
    };
    let script_held = by_cohort("script");
    let wallets = by_cohort("wallet");
    let (_, matured, _) = vesting_split(ledger)?;
    // Nothing provably-gone can be sold, and a pool cannot be sold into
    // itself. Still-locked vesting is out of both bounds — it genuinely cannot
    // move. **Matured vesting is in both**: it is claimable now, so excluding
    // it would understate what could hit the market today. What remains
    // uncertain is only the unidentified scripts, which is why the answer is a
    // range and why identifying vesting narrowed it from both sides.
    let sell_low = wallets + matured as i128;
    let sell_high = sell_low + script_held;

    let base: i128 = tips.iter().map(|t| t.base_reserve as i128).sum();
    let quote: i128 = tips.iter().map(|t| t.quote_reserve as i128).sum();
    // ADA per WHOLE token. `scale` is 1 when decimals are unknown, which leaves
    // the price per raw unit — wrong-looking rather than quietly wrong, and the
    // trailing note says which it is.
    let scale = meta.map_or(1.0, AssetMeta::scale);
    let mut thin = 0usize;
    for t in &tips {
        // A pool too shallow to quote still shows its reserves — they are real
        // and they join the aggregate below. Only its own price is withheld,
        // because a 1-ADA pool quoted WRT at 484 ADA against a real 0.0217.
        let quotable = pools::is_quotable(t.quote_reserve);
        if !quotable {
            thin += 1;
        }
        let spot = match (t.base_reserve > 0, quotable) {
            (true, true) => format!(
                "{:.8} ADA",
                t.quote_reserve as f64 / t.base_reserve as f64 / 1e6 * scale
            ),
            (true, false) => "  too thin to quote".to_string(),
            (false, _) => "           no depth".to_string(),
        };
        println!(
            "  {:<7} base {:>14}  quote {:>15} lovelace  spot {:>19}  fee {:?}  key:{}",
            t.dex, t.base_reserve, t.quote_reserve, spot, t.fee_bps, t.key_basis
        );
    }
    if thin > 0 {
        println!(
            "  note: {thin} of {} pools hold under {} ADA — their reserves still count \
             toward the aggregate, but a pool that thin cannot price anything",
            tips.len(),
            pools::MIN_QUOTABLE_LOVELACE / 1_000_000
        );
    }
    if base <= 0 {
        println!("pools hold none of the asset — price undefined");
        return Ok(());
    }

    // Lovelace per token. Kept in lovelace for the arithmetic below — the
    // caps divide by 1e6 once, at the point of display, rather than carrying a
    // scaled float through the curve.
    let spot_lovelace = quote as f64 / base as f64;
    let notional = (total as f64) * spot_lovelace / 1e6;

    // Weighted-average fee across the pools, so a missing fee (Splash has no
    // shared datum decoder yet) doesn't silently become zero.
    let fee_known: Vec<i64> = tips.iter().filter_map(|t| t.fee_bps).collect();
    let fee_bps = if fee_known.is_empty() {
        0
    } else {
        fee_known.iter().sum::<i64>() / fee_known.len() as i64
    };

    let realise = |sell: i128| -> f64 {
        pools::constant_product_out(
            i64::try_from(base).unwrap_or(i64::MAX),
            i64::try_from(quote).unwrap_or(i64::MAX),
            i64::try_from(sell).unwrap_or(i64::MAX),
            fee_bps,
        ) as f64
            / 1e6
    };
    let (low, high) = (realise(sell_low), realise(sell_high));
    let ratio = |v: f64| {
        if notional > 0.0 {
            100.0 * v / notional
        } else {
            0.0
        }
    };

    println!();
    println!(
        "spot                  {:>16.8} ADA/token   (liquidity-weighted, piecewise-constant)",
        spot_lovelace / 1e6 * scale
    );
    match meta.and_then(|m| m.decimals) {
        Some(d) if d > 0 => println!("  quoted per whole token at {d} dp"),
        // Explicit 0 dp and "nobody has told us" print the same number but are
        // different claims, so they say different things. The second is a
        // prompt to go and source the value, not a result.
        Some(_) => {}
        None => println!(
            "  ⚠ decimals UNKNOWN — this is the price per RAW unit, not per token. \
             Set `decimals` in tokens.toml and re-run the walk."
        ),
    }
    println!("notional cap          {notional:>16.0} ADA   (all supply x spot)");
    if script_held > 0 {
        // Two bounds because the middle is genuinely unknown, and a single
        // number here would be a claim we cannot support either way: treating
        // script-held as liquid flatters the ratio, treating it as locked
        // understates it.
        println!(
            "realisable  {:>10.0} .. {:<10.0} ADA   (sell {} .. {} into the curve)",
            low, high, sell_low, sell_high
        );
        println!("honesty ratio {:>14.1}% .. {:.1}%", ratio(low), ratio(high));
        println!(
            "  the spread is {} script-held tokens of unknown liquidity — \
             not noise, an unanswered question",
            script_held
        );
    } else {
        println!("realisable            {high:>16.0} ADA   (sellable float into the curve)");
        println!("honesty ratio         {:>16.1}%", ratio(high));
    }
    if fee_known.len() < tips.len() {
        println!(
            "  note: {} of {} pools have no decoded fee — realisable is optimistic by that pool's fee",
            tips.len() - fee_known.len(),
            tips.len()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two assets under one policy — an NFT collection, or a policy that minted
    /// under more than one name.
    const A: &[u8] = b"AlienOne";
    const B: &[u8] = b"AlienTwo";

    fn deltas(entries: &[(&str, &[u8], i64)]) -> HashMap<(String, Vec<u8>), (Option<String>, i64)> {
        entries
            .iter()
            .map(|(addr, name, amount)| ((addr.to_string(), name.to_vec()), (None, *amount)))
            .collect()
    }

    fn mints(entries: &[(&[u8], i64)]) -> HashMap<Vec<u8>, i64> {
        entries
            .iter()
            .map(|(name, amount)| (name.to_vec(), *amount))
            .collect()
    }

    /// A pure transfer conserves: one party down, another up, nothing minted.
    #[test]
    fn a_transfer_conserves() {
        let d = deltas(&[("alice", A, -5), ("bob", A, 5)]);
        assert!(conservation_breaches(&d, &HashMap::new()).is_empty());
    }

    /// THE REASON THE UNIT IS ON THE KEY.
    ///
    /// Alice gives up one unit and receives a different one under the same
    /// policy — an NFT trade, or a swap. Summed across units her deltas cancel
    /// to zero, so the transaction looks perfectly conserved while BOTH moves
    /// have been mis-attributed. Per unit, each one is a breach.
    #[test]
    fn a_cross_unit_swap_does_not_cancel_itself_out() {
        let d = deltas(&[("alice", A, -1), ("alice", B, 1)]);
        let breaches = conservation_breaches(&d, &HashMap::new());
        assert_eq!(
            breaches.len(),
            2,
            "both units must be reported: {breaches:?}"
        );
        assert_eq!(breaches[0].name, A.to_vec());
        assert_eq!(breaches[0].delta_sum, -1);
        assert_eq!(breaches[1].name, B.to_vec());
        assert_eq!(breaches[1].delta_sum, 1);

        // And the check this replaced — one scalar per party, summed over the
        // whole tx — saw nothing at all. Stated here so the regression is
        // unmistakable if anyone collapses the key again.
        let summed: i128 = d.values().map(|(_, a)| *a as i128).sum();
        assert_eq!(summed, 0, "the old per-tx check was blind to this");
    }

    /// A mint conserves when the minted quantity reaches somebody.
    #[test]
    fn a_mint_conserves_when_it_lands_somewhere() {
        let d = deltas(&[("alice", A, 100)]);
        assert!(conservation_breaches(&d, &mints(&[(A, 100)])).is_empty());
    }

    /// A burn is the same statement with the sign flipped.
    #[test]
    fn a_burn_conserves() {
        let d = deltas(&[("alice", A, -100)]);
        assert!(conservation_breaches(&d, &mints(&[(A, -100)])).is_empty());
    }

    /// A unit minted that reaches NO tracked party is a violation, and it is
    /// visible only because the mint side seeds the comparison too. Reading the
    /// deltas alone, this transaction has nothing to say.
    #[test]
    fn a_mint_that_reaches_nobody_is_a_breach() {
        let breaches = conservation_breaches(&HashMap::new(), &mints(&[(A, 100)]));
        assert_eq!(breaches.len(), 1);
        assert_eq!(breaches[0].delta_sum, 0);
        assert_eq!(breaches[0].net_mint, 100);
    }

    /// Two units minted in one transaction — a collection's batch mint — each
    /// conserve independently.
    #[test]
    fn units_conserve_independently_within_one_tx() {
        let d = deltas(&[("alice", A, 1), ("bob", B, 1)]);
        assert!(conservation_breaches(&d, &mints(&[(A, 1), (B, 1)])).is_empty());
    }

    // ── the progressive-walk distinction ────────────────────────────────────

    /// THE PREREQUISITE FOR A REVERSE OR WINDOWED WALK.
    ///
    /// Alice's coins arrive at Bob, but Alice's output was created below the
    /// walk's floor so it was never buffered — we see only `bob +5`. The sum
    /// comes out ABOVE the net mint, because the negative half is the half
    /// that went missing. That is a known gap, not a bug.
    #[test]
    fn a_source_below_the_floor_reads_as_unattributed() {
        let d = deltas(&[("bob", A, 5)]);
        let breaches = conservation_breaches(&d, &HashMap::new());
        assert_eq!(breaches.len(), 1);
        assert_eq!(breaches[0].kind, BreachKind::Unattributed);
        assert_eq!(breaches[0].delta_sum, 5);
        assert_eq!(breaches[0].net_mint, 0);
    }

    /// The other direction can NEVER be caused by a cold buffer: outputs are
    /// read in full from the transaction itself, so nothing owed to us can go
    /// missing on the positive side. Tokens leaving without arriving is a real
    /// attribution bug at any floor.
    #[test]
    fn value_leaving_without_arriving_is_always_a_bug() {
        let d = deltas(&[("alice", A, -5)]);
        let breaches = conservation_breaches(&d, &HashMap::new());
        assert_eq!(breaches.len(), 1);
        assert_eq!(breaches[0].kind, BreachKind::Impossible);
    }

    /// A mint that lands nowhere is the same shape — minted, never received —
    /// and must stay loud rather than hide among the floor gaps.
    #[test]
    fn a_mint_that_reaches_nobody_is_impossible_not_unattributed() {
        let breaches = conservation_breaches(&HashMap::new(), &mints(&[(A, 100)]));
        assert_eq!(breaches.len(), 1);
        assert_eq!(breaches[0].kind, BreachKind::Impossible);
    }

    /// A partial walk classifies per unit, not per transaction: one unit's
    /// source can sit below the floor while another's is fully resolved in the
    /// same transaction, and collapsing them would either hide the bug or
    /// invent one.
    #[test]
    fn units_are_classified_independently_within_one_tx() {
        let d = deltas(&[("bob", A, 5), ("alice", B, -1), ("carol", B, 1)]);
        let breaches = conservation_breaches(&d, &HashMap::new());
        assert_eq!(breaches.len(), 1, "only A is short: {breaches:?}");
        assert_eq!(breaches[0].name, A.to_vec());
        assert_eq!(breaches[0].kind, BreachKind::Unattributed);
    }

    // ── coverage completeness ───────────────────────────────────────────────

    /// Reaching the policy's first mint is what completeness means.
    #[test]
    fn coverage_from_at_or_below_the_first_mint_is_complete() {
        assert!(coverage_is_complete(Some(1_000), Some(1_000)));
        assert!(coverage_is_complete(Some(999), Some(1_000)));
        assert!(!coverage_is_complete(Some(1_001), Some(1_000)));
    }

    /// Genesis covers everything, whatever the registry does or does not say.
    #[test]
    fn a_genesis_walk_is_complete_without_a_registered_floor() {
        assert!(coverage_is_complete(Some(0), None));
        assert!(coverage_is_complete(Some(0), Some(5_000_000)));
    }

    /// THE CASE THAT MATTERS FOR ON-DEMAND POLICIES.
    ///
    /// An unregistered policy has no known first mint, so a walk starting part
    /// way up cannot be shown to be complete — and must not be assumed so.
    /// Every on-demand policy arrives in exactly this state.
    #[test]
    fn a_shallow_walk_of_an_unregistered_policy_is_not_complete() {
        assert!(!coverage_is_complete(Some(90_000_000), None));
    }

    /// A ledger predating the `walk_from` cursor has genuinely unknown
    /// coverage. The safe reading reports gaps as gaps rather than as bugs —
    /// which is also what a resumed partial walk used to get wrong by assuming
    /// that having a cursor implied having walked from the bottom.
    #[test]
    fn unknown_coverage_is_never_assumed_complete() {
        assert!(!coverage_is_complete(None, Some(1_000)));
        assert!(!coverage_is_complete(None, None));
    }

    /// Policy mode takes every name, including the empty one.
    #[test]
    fn a_policy_watch_matches_every_name() {
        let w = Watched::Policy;
        assert!(w.matches(A));
        assert!(w.matches(B));
        assert!(w.matches(b""));
    }

    /// A unit watch takes exactly its own name. The empty-named asset is a real
    /// asset and must not be confused with "no name given" — which is the whole
    /// reason `asset_name` is an `Option` rather than a `String`.
    #[test]
    fn a_unit_watch_matches_only_itself() {
        let w = Watched::Unit(A.to_vec());
        assert!(w.matches(A));
        assert!(!w.matches(B));
        assert!(!w.matches(b""));

        let empty = Watched::Unit(Vec::new());
        assert!(empty.matches(b""));
        assert!(!empty.matches(A));
    }
}
