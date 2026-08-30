//! `export` — the ledger as two artifacts a frontend can scrub.
//!
//! The spine carries the reserve curve and cohort checkpoints and drives every
//! always-on face; the detail carries the movement columns and streams in
//! behind it. See `token-ledger-wire` for the format and why it is columnar.
//!
//! ## Checkpoints are computed here, once, over the complete log
//!
//! That is the whole point of the offline walk: the frontend never recomputes
//! history, it resumes from the nearest fact. A projection at an arbitrary `t`
//! is *nearest checkpoint plus a bounded replay*, so a scrub costs O(stride)
//! rather than O(n) and stays flat as the log grows.
//!
//! The totals written are exact at the transaction they name. The stride
//! bounds **resolution**, never correctness — and it bounds two things at
//! once, which is easy to miss: the frontend's replay cost *and* how finely a
//! spine-only rendering (a token too large to ship detail for) can draw the
//! cascade. A stride chosen only for replay cost will look coarse on the chart.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use token_ledger_wire as wire;

use crate::registry;
use crate::store::Ledger;

#[derive(clap::Args, Debug)]
pub struct ExportArgs {
    #[arg(long)]
    pub db: PathBuf,

    #[arg(long, default_value = "tokens.toml")]
    pub tokens: PathBuf,

    /// Which token in the registry this ledger holds.
    #[arg(long)]
    pub token: String,

    /// Directory to write `<token>.spine.bin` and `<token>.detail.bin` into.
    #[arg(long, default_value = ".")]
    pub out_dir: PathBuf,

    /// Transactions between cohort checkpoints. Smaller means a bigger spine
    /// and a finer spine-only chart; larger means the reverse.
    #[arg(long, default_value_t = 64)]
    pub stride: u32,
}

pub fn run(args: ExportArgs) -> Result<()> {
    let token = registry::load(&args.tokens, &args.token)?;
    let ledger =
        Ledger::open(&args.db).with_context(|| format!("opening {}", args.db.display()))?;

    let txs = ledger.all_txs()?;
    let movements = ledger.all_movements()?;
    let parties = ledger.all_parties()?;
    let curve = ledger.reserve_curve()?;
    if txs.is_empty() {
        anyhow::bail!("ledger is empty — run `walk` first");
    }

    // Cohort dictionary. Built from what the parties actually carry rather
    // than a hardcoded list, so a new cohort needs no change here.
    let mut cohorts: Vec<String> = parties
        .iter()
        .map(|p| p.cohort.clone().unwrap_or_else(|| "unclassified".into()))
        .collect();
    cohorts.sort();
    cohorts.dedup();
    let cohort_ix: HashMap<&str, u16> = cohorts
        .iter()
        .enumerate()
        .map(|(i, c)| (c.as_str(), i as u16))
        .collect();

    // party_id is a sqlite rowid, not a dense index — remap so the wire
    // columns can be plain offsets into `parties`.
    let party_ix: HashMap<i64, u32> = parties
        .iter()
        .enumerate()
        .map(|(i, p)| (p.party_id, i as u32))
        .collect();
    let tx_ix: HashMap<i64, u32> = txs
        .iter()
        .enumerate()
        .map(|(i, t)| (t.tx_ord, i as u32))
        .collect();

    let wire_parties: Vec<wire::PartyMeta> = parties
        .iter()
        .map(|p| wire::PartyMeta {
            address: p.address.clone(),
            stake: p.stake.clone(),
            cohort: *cohort_ix
                .get(p.cohort.as_deref().unwrap_or("unclassified"))
                .unwrap_or(&0),
            basis: p.basis.clone().unwrap_or_else(|| "unknown".into()),
        })
        .collect();

    // ---- Detail ---------------------------------------------------------
    let mut tx_hashes = Vec::with_capacity(txs.len());
    let mut tx_slots_abs = Vec::with_capacity(txs.len());
    let mut tx_net_mint = Vec::with_capacity(txs.len());
    for t in &txs {
        let h: [u8; 32] = t
            .tx_hash
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("tx hash is not 32 bytes"))?;
        tx_hashes.push(h);
        tx_slots_abs.push(t.slot);
        tx_net_mint.push(t.net_mint);
    }

    let mut mv_tx = Vec::with_capacity(movements.len());
    let mut mv_party = Vec::with_capacity(movements.len());
    let mut mv_amount = Vec::with_capacity(movements.len());
    for (tx_ord, party_id, amount) in &movements {
        mv_tx.push(*tx_ix.get(tx_ord).context("movement names an unknown tx")?);
        mv_party.push(
            *party_ix
                .get(party_id)
                .context("movement names an unknown party")?,
        );
        mv_amount.push(*amount);
    }

    let tx_ids = wire::TxIds {
        version: wire::WIRE_VERSION,
        hashes: tx_hashes,
    };

    let detail = wire::Detail {
        version: wire::WIRE_VERSION,
        parties: wire_parties,
        tx_slots: wire::delta_encode(&tx_slots_abs),
        tx_net_mint,
        mv_tx,
        mv_party,
        mv_amount,
    };

    // ---- Spine ----------------------------------------------------------
    // Replay the movements once, emitting a checkpoint every `stride`
    // transactions. One pass, so the cost is the same whatever the stride.
    let stride = args.stride.max(1) as usize;
    let mut running: Vec<i64> = vec![0; cohorts.len()];
    let mut balances: HashMap<u32, i64> = HashMap::new();
    let mut cp_slots_abs = Vec::new();
    let mut cp_tx_ord = Vec::new();
    let mut cp_holders = Vec::new();
    let mut cp_totals = Vec::new();

    let mut mv = 0usize;
    for (i, slot) in tx_slots_abs.iter().enumerate() {
        while mv < detail.mv_tx.len() && detail.mv_tx[mv] as usize == i {
            let p = detail.mv_party[mv];
            let amount = detail.mv_amount[mv];
            let cohort = detail.parties[p as usize].cohort as usize;
            running[cohort] += amount;
            *balances.entry(p).or_insert(0) += amount;
            mv += 1;
        }
        // Checkpoint after applying the transaction, so a checkpoint's totals
        // are the state *as of* the transaction it names — inclusive, which is
        // what "resume replaying from tx_ord+1" needs.
        if i % stride == 0 || i == tx_slots_abs.len() - 1 {
            cp_slots_abs.push(*slot);
            cp_tx_ord.push(i as u32);
            cp_holders.push(balances.values().filter(|b| **b != 0).count() as u32);
            cp_totals.extend_from_slice(&running);
        }
    }

    // Pool dictionary, keyed the way the ledger keys pools.
    let mut pools: Vec<wire::PoolMeta> = Vec::new();
    let mut pool_ix: HashMap<(String, Vec<u8>, Vec<u8>), u16> = HashMap::new();
    let mut rc_slots_abs = Vec::new();
    let mut rc_pool = Vec::new();
    let mut rc_base = Vec::new();
    let mut rc_quote = Vec::new();
    for c in &curve {
        let key = (c.dex.clone(), c.key_policy.clone(), c.key_name.clone());
        let idx = *pool_ix.entry(key).or_insert_with(|| {
            pools.push(wire::PoolMeta {
                dex: c.dex.clone(),
                key_policy: c.key_policy.clone(),
                key_name: c.key_name.clone(),
                key_basis: c.key_basis.clone(),
                fee_bps: c.fee_bps.map(|f| f as u32),
            });
            (pools.len() - 1) as u16
        });
        let ti = *tx_ix
            .get(&c.tx_ord)
            .context("reserve point names an unknown tx")?;
        rc_slots_abs.push(tx_slots_abs[ti as usize]);
        rc_pool.push(idx);
        rc_base.push(c.base_reserve);
        rc_quote.push(c.quote_reserve);
    }

    let policy: [u8; 28] = token
        .policy_bytes()?
        .try_into()
        .map_err(|_| anyhow::anyhow!("policy is not 28 bytes"))?;

    let spine = wire::Spine {
        version: wire::WIRE_VERSION,
        asset: wire::AssetId {
            policy,
            asset_name: token.asset_name_bytes()?,
            decimals: token.decimals,
        },
        domain: (
            *tx_slots_abs.first().unwrap_or(&0),
            *tx_slots_abs.last().unwrap_or(&0),
        ),
        last_block_time: ledger.locked_positions()?.tip_time.unwrap_or(0),
        nominal_supply: ledger.net_mint_total()?,
        cohorts: cohorts.clone(),
        checkpoint_stride: args.stride,
        cp_slots: wire::delta_encode(&cp_slots_abs),
        cp_tx_ord,
        cp_holders,
        cp_totals,
        pools,
        rc_slots: wire::delta_encode(&rc_slots_abs),
        rc_pool,
        rc_base,
        rc_quote,
    };

    // The last checkpoint must equal the ledger's own totals, or the spine is
    // telling a different story from the database it came from. Cheap to
    // check, and the one bug that would make every chart quietly wrong.
    verify_last_checkpoint(&spine, &ledger)?;

    let spine_bytes = wire::encode_spine(&spine)?;
    let detail_bytes = wire::encode_detail(&detail)?;
    let txid_bytes = wire::encode_tx_ids(&tx_ids)?;

    std::fs::create_dir_all(&args.out_dir)?;
    let spine_path = args.out_dir.join(format!("{}.spine.bin", token.name));
    let detail_path = args.out_dir.join(format!("{}.detail.bin", token.name));
    let txid_path = args.out_dir.join(format!("{}.txids.bin", token.name));
    write(&spine_path, &spine_bytes)?;
    write(&detail_path, &detail_bytes)?;
    write(&txid_path, &txid_bytes)?;

    // Decode what was just written. An artifact that cannot be read back is
    // worse than no artifact, and the validators only run on shapes — this
    // catches an encoder that produced bytes nothing can parse.
    let round_spine = wire::decode_spine(&std::fs::read(&spine_path)?)?;
    let round_detail = wire::decode_detail(&std::fs::read(&detail_path)?)?;
    let round_ids = wire::decode_tx_ids(&std::fs::read(&txid_path)?)?;
    anyhow::ensure!(round_spine == spine, "spine did not round-trip");
    anyhow::ensure!(round_detail == detail, "detail did not round-trip");
    anyhow::ensure!(round_ids == tx_ids, "tx ids did not round-trip");
    // The two files are parallel only by construction — neither can enforce
    // it alone, so check here rather than let a consumer discover it by
    // opening the wrong transaction.
    anyhow::ensure!(
        tx_ids.hashes.len() == detail.tx_count(),
        "tx-id count does not match the detail page's transaction count"
    );

    report(
        &spine,
        &detail,
        &spine_bytes,
        &detail_bytes,
        &txid_bytes,
        &[&spine_path, &detail_path, &txid_path],
    );
    Ok(())
}

fn write(path: &Path, bytes: &[u8]) -> Result<()> {
    std::fs::write(path, bytes).with_context(|| format!("writing {}", path.display()))
}

/// The final checkpoint must reproduce the ledger's own cohort totals.
fn verify_last_checkpoint(spine: &wire::Spine, ledger: &Ledger) -> Result<()> {
    let Some(last) = spine.checkpoint_count().checked_sub(1) else {
        return Ok(());
    };
    let totals = spine
        .checkpoint_totals(last)
        .context("last checkpoint has no totals")?;
    let from_db = ledger.cohort_totals()?;
    for (cohort, _, total) in &from_db {
        let ix = spine
            .cohorts
            .iter()
            .position(|c| c == cohort)
            .with_context(|| format!("cohort `{cohort}` missing from the spine dictionary"))?;
        anyhow::ensure!(
            totals[ix] == *total,
            "spine disagrees with the ledger on cohort `{cohort}`: {} vs {total}",
            totals[ix]
        );
    }
    Ok(())
}

fn report(
    spine: &wire::Spine,
    detail: &wire::Detail,
    spine_bytes: &[u8],
    detail_bytes: &[u8],
    txid_bytes: &[u8],
    paths: &[&Path],
) {
    let kb = |n: usize| n as f64 / 1024.0;
    println!(
        "spine   {:>9} bytes ({:>8.1} KB)  {} checkpoints (stride {}), {} reserve points, {} pools",
        spine_bytes.len(),
        kb(spine_bytes.len()),
        spine.checkpoint_count(),
        spine.checkpoint_stride,
        spine.reserve_point_count(),
        spine.pools.len()
    );
    println!(
        "detail  {:>9} bytes ({:>8.1} KB)  {} movements, {} txs, {} parties",
        detail_bytes.len(),
        kb(detail_bytes.len()),
        detail.movement_count(),
        detail.tx_count(),
        detail.parties.len()
    );
    if detail.movement_count() > 0 {
        println!(
            "        {:.1} bytes per movement (detail total / movements)",
            detail_bytes.len() as f64 / detail.movement_count() as f64
        );
    }
    println!(
        "txids   {:>9} bytes ({:>8.1} KB)  {} hashes — click-through only, never loaded to scrub",
        txid_bytes.len(),
        kb(txid_bytes.len()),
        detail.tx_count()
    );
    for p in paths {
        println!("wrote {}", p.display());
    }
}
