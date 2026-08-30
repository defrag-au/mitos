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

    /// How far the reserve curve may deviate from true reserves, in basis
    /// points. `0` keeps every observation and the curve is exact.
    ///
    /// This is the spine's real size lever. Checkpoints compress and the curve
    /// does not: on WRT, raising the stride from 64 to 16,384 only moved the
    /// spine 957 KB → 634 KB gzipped, because 51,318 reserve points dominate.
    #[arg(long, default_value_t = 10)]
    pub curve_eps_bps: u32,
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

    // Parties split hot from cold. The cohort and basis a scrub needs are two
    // small integers; the address it does not need is 103 characters. Keeping
    // them together made the dictionary 61.8% of the detail artifact.
    let mut bases: Vec<String> = Vec::new();
    let mut basis_ix: HashMap<String, u16> = HashMap::new();
    let mut party_cohort = Vec::with_capacity(parties.len());
    let mut party_basis = Vec::with_capacity(parties.len());
    let mut addresses = Vec::with_capacity(parties.len());
    let mut stakes = Vec::with_capacity(parties.len());
    for p in &parties {
        party_cohort.push(
            *cohort_ix
                .get(p.cohort.as_deref().unwrap_or("unclassified"))
                .unwrap_or(&0),
        );
        // Four distinct strings across every party, so interning turns a
        // repeated word into a byte.
        let basis = p.basis.clone().unwrap_or_else(|| "unknown".into());
        let next = bases.len() as u16;
        let ix = *basis_ix.entry(basis.clone()).or_insert_with(|| {
            bases.push(basis);
            next
        });
        party_basis.push(ix);
        addresses.push(p.address.clone());
        stakes.push(p.stake.clone());
    }
    let party_ids = wire::PartyIds {
        version: wire::WIRE_VERSION,
        addresses,
        stakes,
    };

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
        bases,
        party_cohort,
        party_basis,
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
    let mut cp_vest_matured = Vec::new();
    let mut cp_vest_locked = Vec::new();
    let locks = ledger.lock_lifetimes()?;
    let tx_times: Vec<u64> = txs.iter().map(|t| t.block_time).collect();

    let mut mv = 0usize;
    for (i, slot) in tx_slots_abs.iter().enumerate() {
        while mv < detail.mv_tx.len() && detail.mv_tx[mv] as usize == i {
            let p = detail.mv_party[mv];
            let amount = detail.mv_amount[mv];
            let cohort = detail.party_cohort[p as usize] as usize;
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
            let (matured, locked) = vesting_at(&locks, i as i64, tx_times[i]);
            cp_vest_matured.push(matured);
            cp_vest_locked.push(locked);
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

    // Reduce by price-change magnitude, never by time. A quiet pool that then
    // moves sharply must keep the move; a busy pool trading flat need not keep
    // every tick. The bound travels in the artifact so a consumer can render
    // it rather than imply a precision the curve does not have.
    let full_points = rc_slots_abs.len();
    let keep = wire::reduce_curve(&rc_pool, &rc_base, &rc_quote, args.curve_eps_bps);
    if keep.len() < full_points {
        let take = |v: &Vec<i64>| keep.iter().map(|&i| v[i]).collect::<Vec<_>>();
        rc_base = take(&rc_base);
        rc_quote = take(&rc_quote);
        rc_pool = keep.iter().map(|&i| rc_pool[i]).collect();
        rc_slots_abs = keep.iter().map(|&i| rc_slots_abs[i]).collect();
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
            // The wire type carries a plain `u8`, where 0 and "unknown" both
            // mean render raw — the same collapse `chain-ledger` makes, and
            // harmless downstream because both format identically. The
            // distinction only matters at registration time, which is where
            // `Option` lives.
            decimals: token.resolved_decimals().unwrap_or(0),
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
        cp_vest_matured,
        cp_vest_locked,
        pools,
        rc_slots: wire::delta_encode(&rc_slots_abs),
        rc_pool,
        rc_base,
        rc_quote,
        rc_eps_bps: args.curve_eps_bps,
    };

    // The last checkpoint must equal the ledger's own totals, or the spine is
    // telling a different story from the database it came from. Cheap to
    // check, and the one bug that would make every chart quietly wrong.
    verify_last_checkpoint(&spine, &ledger)?;

    let spine_bytes = wire::encode_spine(&spine)?;
    let detail_bytes = wire::encode_detail(&detail)?;
    let txid_bytes = wire::encode_tx_ids(&tx_ids)?;
    let party_bytes = wire::encode_party_ids(&party_ids)?;

    std::fs::create_dir_all(&args.out_dir)?;
    let spine_path = args.out_dir.join(format!("{}.spine.bin", token.name));
    let detail_path = args.out_dir.join(format!("{}.detail.bin", token.name));
    let txid_path = args.out_dir.join(format!("{}.txids.bin", token.name));
    let party_path = args.out_dir.join(format!("{}.parties.bin", token.name));
    write(&spine_path, &spine_bytes)?;
    write(&detail_path, &detail_bytes)?;
    write(&txid_path, &txid_bytes)?;
    write(&party_path, &party_bytes)?;

    // Decode what was just written. An artifact that cannot be read back is
    // worse than no artifact, and the validators only run on shapes — this
    // catches an encoder that produced bytes nothing can parse.
    let round_spine = wire::decode_spine(&std::fs::read(&spine_path)?)?;
    let round_detail = wire::decode_detail(&std::fs::read(&detail_path)?)?;
    let round_ids = wire::decode_tx_ids(&std::fs::read(&txid_path)?)?;
    let round_parties = wire::decode_party_ids(&std::fs::read(&party_path)?)?;
    anyhow::ensure!(round_spine == spine, "spine did not round-trip");
    anyhow::ensure!(round_detail == detail, "detail did not round-trip");
    anyhow::ensure!(round_ids == tx_ids, "tx ids did not round-trip");
    anyhow::ensure!(round_parties == party_ids, "party ids did not round-trip");
    // These files are parallel only by construction — none can enforce it
    // alone, so check here rather than let a consumer discover it by opening
    // the wrong transaction or labelling a movement with another party's
    // address. An index shift renders perfectly and is entirely wrong.
    anyhow::ensure!(
        tx_ids.hashes.len() == detail.tx_count(),
        "tx-id count does not match the detail page's transaction count"
    );
    anyhow::ensure!(
        party_ids.agrees_with(&detail),
        "party-id count does not match the detail page's party count"
    );

    report(
        &spine,
        &detail,
        &spine_bytes,
        &detail_bytes,
        &txid_bytes,
        &party_bytes,
        &[&spine_path, &detail_path, &txid_path, &party_path],
    );
    Ok(())
}

/// Read the artifacts back with no database and print what a consumer sees.
///
/// This is the export's real test. `stats` answers from sqlite; `inspect`
/// answers from the files alone, through the same `projections` a frontend
/// will use. If the two disagree, the artifact is not a faithful carrier of
/// the ledger — and that is a failure no amount of round-trip checking would
/// catch, because the bytes would decode perfectly into the wrong numbers.
pub fn inspect(dir: &Path, token: &str, at_slot: Option<u64>) -> Result<()> {
    use token_ledger_wire::projections as proj;

    let spine = wire::decode_spine(&std::fs::read(dir.join(format!("{token}.spine.bin")))?)?;
    let detail = wire::decode_detail(&std::fs::read(dir.join(format!("{token}.detail.bin")))?)?;
    let slot = at_slot.unwrap_or(spine.domain.1);

    println!(
        "domain {}..{}  ({} txs, {} movements, {} parties)",
        spine.domain.0,
        spine.domain.1,
        detail.tx_count(),
        detail.movement_count(),
        detail.party_count()
    );

    // Spine-only first, then with detail — the two tiers a frontend loads in
    // sequence, so the difference between them is visible rather than assumed.
    if let Some(c) = proj::cohorts_at_checkpoint(&spine, slot) {
        println!(
            "\nSPINE ONLY (checkpoint resolution, as of tx {})",
            c.as_of_tx
        );
        print_cohorts(&spine, &c);
    }
    let Some(exact) = proj::cohorts_at(&spine, &detail, slot) else {
        println!("no data at slot {slot}");
        return Ok(());
    };
    println!("\n+ DETAIL (exact, as of tx {})", exact.as_of_tx);
    print_cohorts(&spine, &exact);

    match proj::cap_at(&spine, Some(&detail), slot) {
        Some(cap) => {
            let (lo, hi) = cap.honesty_ratio();
            let (with_fee, pools) = proj::fee_coverage(&spine);
            // Per WHOLE token. The artifact carries its own decimals, so
            // `inspect` needs no registry — but it does have to apply them.
            // `stats` was fixed for this and its twin here was not, which is
            // how the same number came out right on one surface and a million
            // times small on the other.
            let scale = 10f64.powi(spine.asset.decimals as i32);
            println!(
                "\nspot          {:.8} ADA/token",
                cap.spot_lovelace / 1e6 * scale
            );
            println!("notional cap  {:>12} ADA", cap.notional / 1_000_000);
            println!(
                "realisable    {:>12} .. {} ADA",
                cap.realisable_low / 1_000_000,
                cap.realisable_high / 1_000_000
            );
            println!("honesty ratio {lo:>12.1}% .. {hi:.1}%");
            println!(
                "uncertain     {:>12} tokens of unknown liquidity",
                cap.uncertain
            );
            if with_fee < pools {
                println!(
                    "  note: {} of {pools} pools have no decoded fee — realisable is optimistic",
                    pools - with_fee
                );
            }
        }
        None => println!("\nno price at this slot — no pool existed yet"),
    }
    println!(
        "\nvesting at this instant: {} matured / {} still locked",
        proj::vesting_matured_at(&spine, slot),
        proj::vesting_locked_at(&spine, slot)
    );
    Ok(())
}

fn print_cohorts(spine: &wire::Spine, c: &token_ledger_wire::projections::CohortTotals) {
    let total: i64 = c.totals.iter().sum();
    for (name, amount) in spine.cohorts.iter().zip(&c.totals) {
        if *amount == 0 {
            continue;
        }
        let pct = if total > 0 {
            100.0 * *amount as f64 / total as f64
        } else {
            0.0
        };
        println!("  {name:<8} {amount:>14}  {pct:5.2}%");
    }
    println!("  {:<8} {total:>14}  holders {}", "TOTAL", c.holders);
}

/// Vesting `(matured, locked)` as of transaction `tx_ord` at `block_time`.
///
/// A lock counts when it was open at that transaction — created at or before
/// it, and either still live or spent afterwards. That is why lock *lifetimes*
/// are recorded rather than the live set: a lock created and claimed inside the
/// history leaves no trace at tip, and using tip's live set would silently
/// backdate today's state onto every past checkpoint.
fn vesting_at(locks: &[crate::store::LockLifetime], tx_ord: i64, block_time: u64) -> (i64, i64) {
    let now_ms = block_time.saturating_mul(1_000);
    let mut matured = 0i64;
    let mut locked = 0i64;
    for l in locks {
        let open = l.created_tx_ord <= tx_ord && l.spent_tx_ord.is_none_or(|s| s > tx_ord);
        if !open {
            continue;
        }
        if l.unlock_ts_ms <= now_ms {
            matured += l.qty;
        } else {
            locked += l.qty;
        }
    }
    (matured, locked)
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
    party_bytes: &[u8],
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
    if spine.rc_eps_bps > 0 {
        println!(
            "        curve reduced to ±{} bps — the only inexact part of the spine",
            spine.rc_eps_bps
        );
    }
    println!(
        "detail  {:>9} bytes ({:>8.1} KB)  {} movements, {} txs, {} parties",
        detail_bytes.len(),
        kb(detail_bytes.len()),
        detail.movement_count(),
        detail.tx_count(),
        detail.party_count()
    );
    if detail.movement_count() > 0 {
        let attrs = detail.party_attr_bytes();
        println!(
            "        {:.1} B/movement  ·  party attrs {} B (global — would be \
             duplicated into every chunk)",
            detail_bytes.len() as f64 / detail.movement_count() as f64,
            attrs
        );
        // What an inline per-movement cohort byte would cost. If it is cheap,
        // a chunk stops needing the global attr table at all and becomes
        // self-contained — which is what chunking actually wants.
        println!(
            "        inline mv_cohort would be {} B raw over {} distinct values",
            detail.inline_cohort_bytes(),
            detail.cohort_span()
        );
    }
    println!(
        "txids   {:>9} bytes ({:>8.1} KB)  {} hashes — click-through only, never loaded to scrub",
        txid_bytes.len(),
        kb(txid_bytes.len()),
        detail.tx_count()
    );
    println!(
        "parties {:>9} bytes ({:>8.1} KB)  {} addresses — click-through only, never loaded to scrub",
        party_bytes.len(),
        kb(party_bytes.len()),
        detail.party_count()
    );
    for p in paths {
        println!("wrote {}", p.display());
    }
}
