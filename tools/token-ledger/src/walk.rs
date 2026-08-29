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
use crate::registry;
use crate::store::{Ledger, TxRow};

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

    tracing::info!(
        token = %token.name,
        policy = %token.policy,
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

    let mut scanned: u64 = 0;
    let mut in_range: u64 = 0;
    let mut touched: u64 = 0;
    let mut violations: u64 = 0;
    let mut last_slot: u64 = 0;

    for block in blocks {
        let bytes = block.map_err(|e| anyhow::anyhow!("reading block from chunk: {e:?}"))?;
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
            for inp in &dtx.inputs {
                if let Some(b) = buffer.take(&inp.oref) {
                    let e = deltas.entry(b.address).or_insert((b.stake, 0));
                    e.1 -= b.qty;
                }
            }

            // Outputs: whatever it produced that we now hold.
            for out in &dtx.outputs {
                if !holds_watched(out, &policy, &asset_name) {
                    continue;
                }
                let qty = qty_from_output(&tx, out.index, &policy, &asset_name);
                if qty == 0 {
                    continue;
                }
                let stake = stake_of(&out.address);
                buffer.insert(
                    (dtx.tx_hash, out.index),
                    BufferedOutput {
                        address: out.address.clone(),
                        stake: stake.clone(),
                        qty,
                    },
                );
                let e = deltas.entry(out.address.clone()).or_insert((stake, 0));
                e.1 += qty;
            }

            if deltas.is_empty() && net_mint == 0 {
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

    // Final buffer persist so a resume picks up the true open set.
    ledger.commit_block(
        &[],
        last_slot,
        &pallas_primitives::Hash::from([0u8; 32]),
        &buffer,
        true,
    )?;

    let (txs, delta_rows, parties) = ledger.counts()?;
    let minted = ledger.net_mint_total()?;
    let delta_sum = ledger.delta_total()?;
    let live = buffer.total_qty();

    tracing::info!(
        scanned,
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

pub fn stats(db: &std::path::Path, top: usize) -> Result<()> {
    let ledger = Ledger::open(db).with_context(|| format!("opening {}", db.display()))?;
    let (txs, deltas, parties) = ledger.counts()?;
    let balances = ledger.balances()?;
    let total: i128 = balances.iter().map(|(_, _, b)| *b as i128).sum();

    println!("txs {txs}  deltas {deltas}  parties seen {parties}");
    println!("holders with non-zero balance: {}", balances.len());
    println!("total held: {total}");
    println!();
    for (addr, stake, bal) in balances.iter().take(top) {
        let pct = if total > 0 {
            100.0 * (*bal as f64) / (total as f64)
        } else {
            0.0
        };
        let short = if addr.len() > 32 { &addr[..32] } else { addr };
        let st = stake.as_deref().unwrap_or("-");
        let st_short = if st.len() > 20 { &st[..20] } else { st };
        println!("{bal:>16}  {pct:5.2}%  {short}…  {st_short}");
    }
    Ok(())
}
