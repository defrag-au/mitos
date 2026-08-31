//! `walk` — iterate certified immutable-DB history and record every movement
//! of one watched asset as signed per-party deltas.
//!
//! Per transaction: take the spent watched outputs back out of the buffer
//! (that is the whole of input resolution — see `buffer`), buffer the produced
//! ones, read the mint field, and net it all into one signed delta per party.
//! A transaction that touches neither the buffer nor the mint field is skipped
//! before any allocation, which is what keeps the walk at block-decode speed.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use pallas_addresses::{Address, StakeAddress};
use pallas_traverse::MultiEraBlock;

use mitos_chain_walk::decode::{DecodedOutput, decode_tx};
use mitos_chain_walk::{open_blocks, slot_to_unix};

use crate::buffer::{BufferedOutput, OutrefBuffer};
use crate::cohort;
use crate::pools;
use crate::registry;
use crate::store::{AssetMeta, Balance, Ledger, TxRow};

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
fn stake_of(addr: &str) -> Option<String> {
    match Address::from_bech32(addr).ok()? {
        Address::Shelley(sh) => {
            let stake: StakeAddress = sh.try_into().ok()?;
            stake.to_bech32().ok()
        }
        _ => None,
    }
}

/// Does this output carry the watched asset?
///
/// `DecodedOutput.assets` carries asset *identity* only, so this answers
/// presence and `qty_from_output` reads the quantity off the raw output. The
/// cheap check runs first — the overwhelming majority of outputs on the chain
/// are not ours.
fn holds_watched(out: &DecodedOutput, policy: &[u8], name: &[u8]) -> bool {
    out.assets
        .iter()
        .any(|a| a.policy == policy && a.name == name)
}

pub fn run(args: WalkArgs) -> Result<()> {
    let token = registry::load(&args.tokens, &args.token)?;
    let policy = token.policy_bytes()?;
    let asset_name = token.asset_name_bytes()?;

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

    // Stamp the ledger with what it is about, so every read command is
    // self-describing instead of trusting a flag it could be given wrongly.
    // Written on resume too, so a registry edit (a decimals value arriving)
    // reaches an existing db without a re-walk.
    let decimals = token.resolved_decimals();
    ledger.put_meta(&token.name, &policy, &asset_name, decimals)?;

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
        .then(|| chain_sieve::Needles::new(&[policy.clone()]))
        .transpose()?;
    let mut gated: u64 = 0;

    let mut scanned: u64 = 0;
    let mut in_range: u64 = 0;
    let mut touched: u64 = 0;
    let mut violations: u64 = 0;
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
        if let Some(needle) = &sieve {
            if !needle.hit(&bytes) {
                gated += 1;
                if gated.is_multiple_of(1_000_000) {
                    tracing::info!(gated, "walk: sieve skipping");
                }
                continue;
            }
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
            // Net mint of the watched asset. Read from the raw tx — the
            // shared decode surface doesn't carry the mint field.
            let net_mint: i64 = tx
                .mints()
                .iter()
                .filter(|pa| pa.policy().as_ref() == policy.as_slice())
                .flat_map(|pa| {
                    pa.assets()
                        .iter()
                        .filter(|a| a.name() == asset_name.as_slice())
                        .map(|a| a.mint_coin().unwrap_or(0))
                        .collect::<Vec<_>>()
                })
                .sum();

            let dtx = decode_tx(&tx);

            // Inputs: whatever this tx spent that we were holding.
            let mut deltas: HashMap<String, (Option<String>, i64)> = HashMap::new();
            let mut locks_spent = Vec::new();
            for inp in &dtx.inputs {
                if let Some(b) = buffer.take(&inp.oref) {
                    // A spent lock closes here. Recorded so maturity can be
                    // stated at any past instant, not just at tip — the live
                    // set alone cannot say a lock existed and was claimed.
                    if b.unlock_ts_ms.is_some() {
                        locks_spent.push(inp.oref);
                    }
                    let e = deltas.entry(b.address).or_insert((b.stake, 0));
                    e.1 -= b.qty;
                }
            }

            // Outputs: whatever it produced that we now hold.
            let mut pool_obs = Vec::new();
            let mut locks_created = Vec::new();
            for out in &dtx.outputs {
                if !holds_watched(out, &policy, &asset_name) {
                    continue;
                }
                let qty = qty_from_output(&tx, out.index, &policy, &asset_name);
                if qty == 0 {
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
                let mut pool_hit = false;
                if let Some(obs) = pools::recognise(out, qty, &policy, &asset_name, witness_datum) {
                    pool_obs.push(obs);
                    pool_hit = true;
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
                        qty,
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
                        qty,
                        unlock_ts_ms,
                        owner_pkh,
                        datum_cbor,
                        datum_hash,
                    },
                );
                let e = deltas.entry(out.address.clone()).or_insert((stake, 0));
                e.1 += qty;
            }

            if deltas.is_empty()
                && net_mint == 0
                && pool_obs.is_empty()
                && locks_created.is_empty()
                && locks_spent.is_empty()
            {
                continue;
            }

            // Conservation: within a tx the deltas must sum to the net mint.
            // Anything else means an input we failed to resolve or an output
            // we mis-read — the entire class of attribution bug this walker
            // could have, caught for free.
            let sum: i128 = deltas.values().map(|(_, a)| *a as i128).sum();
            if sum != net_mint as i128 {
                violations += 1;
                tracing::error!(
                    tx = %hex::encode(dtx.tx_hash.as_ref()),
                    slot,
                    delta_sum = %sum,
                    net_mint,
                    "walk: CONSERVATION VIOLATION — deltas do not sum to net mint"
                );
            }

            touched += 1;
            rows.push(TxRow {
                tx_hash: dtx.tx_hash,
                slot,
                block_time,
                net_mint,
                deltas: deltas
                    .into_iter()
                    .filter(|(_, (_, amount))| *amount != 0)
                    .map(|(address, (stake, amount))| (address, stake, amount))
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
        "walk: done"
    );

    // End-to-end reconciliation. These three must agree; if they don't, every
    // number downstream is wrong and it is better to say so loudly here than
    // to ship a plausible chart.
    let orphans = ledger.orphan_deltas()?;
    println!("net minted (Σ tx.net_mint)   = {minted}");
    println!("Σ all deltas                 = {delta_sum}");
    println!("live in buffer (circulating) = {live}");
    println!("orphan deltas                = {orphans}");
    if minted as i128 != live || delta_sum != minted || orphans != 0 {
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

/// Quantity of the watched asset in output `index` of `tx`.
///
/// Read off the raw output rather than the shared `DecodedOutput`, which
/// carries asset identity but not quantity.
fn qty_from_output(
    tx: &pallas_traverse::MultiEraTx<'_>,
    index: u32,
    policy: &[u8],
    name: &[u8],
) -> i64 {
    let outputs = tx.outputs();
    let Some(out) = outputs.get(index as usize) else {
        return 0;
    };
    let total: u64 = out
        .value()
        .assets()
        .iter()
        .filter(|pa| pa.policy().as_ref() == policy)
        .flat_map(|pa| {
            pa.assets()
                .iter()
                .filter(|a| a.name() == name)
                .map(|a| a.output_coin().unwrap_or(0))
                .collect::<Vec<_>>()
        })
        .sum();
    // Output quantities are u64 on the wire but the delta arithmetic is
    // signed. A token whose supply exceeds i64::MAX would saturate here rather
    // than wrap — no such token exists on Cardano (SNEK, the largest, is
    // 7.6e10), but saturating beats a silent negative balance.
    i64::try_from(total).unwrap_or(i64::MAX)
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

    let mut ordered: Vec<_> = groups.into_iter().collect();
    ordered.sort_by_key(|(_, g)| -g.qty);

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
            "ledger: {}  {}.{}  {}",
            m.name,
            hex::encode(&m.policy),
            hex::encode(&m.asset_name),
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
    println!("holders with non-zero balance: {}", balances.len());
    println!("total held: {total}");
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

    mint_report(&ledger, total)?;
    cascade_report(&ledger, total)?;
    cap_report(&ledger, &balances, total, ledger.asset_meta()?.as_ref())?;
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
        crate::pools::constant_product_out(
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
