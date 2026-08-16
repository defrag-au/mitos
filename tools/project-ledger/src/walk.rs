//! `walk` — iterate certified immutable-DB history from the walk start, and
//! for every block: count activity, follow the policy's assets, and book the
//! watched parties' net flows — growing the frontier as the treasury pays.
//!
//! Per tx (see `process_tx`):
//! 1. resolve every output address to a party (+ bump the activity counter);
//! 2. if the tx mints the policy: seed the signer credential(s) and the CIP-27
//!    royalty address, record the ceiling;
//! 3. asset events from the holder map — mint / transfer / burn — with NO input
//!    resolution (the previous holder is the `from`);
//! 4. if the tx touches a watched party (an output to one, or an input from the
//!    buffer): resolve ALL inputs through the ladder, build a `TxView`, take
//!    net deltas + movements, feed the frontier, write rows, buffer the outputs
//!    now held by members.
//!
//! Rows go in per block; the checkpoint (cursor + frontier + buffer + activity
//! + holders, one transaction) every `--checkpoint-every` in-range blocks.
//!
//! Resume replays `[cursor, crash)` idempotently — rows are keyed, promotion is
//! a pure function of the stream.

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use chain_ledger::{Chain, FrontierOutcome, Movement, Party, Role, TxInput, TxOutput, TxView};
use mitos_chain_walk::checkpoint::{self, CheckpointFile};
use mitos_chain_walk::decode::{DecodedTx, OutRef, decode_tx};
use mitos_chain_walk::slot_to_unix;
use pallas_traverse::{MultiEraBlock, MultiEraTx};

use crate::koios::Koios;
use crate::mint::{cip27_royalty, policy_script};
use crate::party::{Resolved, resolve_str};
use crate::resolve::{LadderStats, Offline, Remote, resolve_missing};
use crate::seed::*;
use crate::state::{BufferedOutput, WalkState};
use crate::store::{AssetEventRow, Ledger, TxDeltaRow, ValueEventRow};

#[derive(clap::Args, Debug)]
pub struct WalkArgs {
    /// Data dir holding the immutable DB (expects `<data-dir>/immutable`).
    /// Point it at market-ledger's dir — no second bootstrap.
    #[arg(long)]
    data_dir: PathBuf,

    /// Ledger sqlite path (must be seeded).
    #[arg(long, default_value = "project-ledger.db")]
    db: PathBuf,

    /// Input-resolution remote: `koios` (default) or `offline` (unresolved
    /// inputs are counted, never guessed).
    #[arg(long, default_value = "koios")]
    remote: String,

    #[arg(long, env = "KOIOS_BASE")]
    koios_base: Option<String>,

    #[arg(long, env = "KOIOS_TOKEN")]
    koios_token: Option<String>,

    /// Checkpoint (persist state + cursor) every N in-range blocks. Never per
    /// block.
    #[arg(long, default_value_t = 50_000)]
    checkpoint_every: u64,

    /// Stop after this many in-range blocks (0 = no limit) — smoke tests.
    #[arg(long, default_value_t = 0)]
    max_blocks: u64,

    /// Stop after this slot (default: the snapshot tip).
    #[arg(long)]
    to_slot: Option<u64>,

    /// Crash-visible progress mirror (default: `<db>.checkpoint.json`).
    #[arg(long)]
    checkpoint_file: Option<PathBuf>,
}

pub fn run(args: WalkArgs) -> Result<()> {
    let immutable_dir = args.data_dir.join("immutable");
    let mut ledger = Ledger::open(&args.db)?;
    let policy_hex = ledger
        .meta_get(META_POLICY)?
        .context("ledger is not seeded — run `project-ledger seed` first")?;
    let policy = policy_bytes(&policy_hex)?;
    let mut state = ledger
        .restore()?
        .context("no checkpoint state — run `seed`")?;
    let (cursor_slot, cursor_hash) = ledger.cursor()?.context("no cursor — run `seed`")?;
    let resumed = cursor_hash.len() == 32;
    let floor = cursor_slot;
    let expected_assets: Option<u64> = ledger
        .meta_get(META_EXPECTED_ASSETS)?
        .and_then(|s| s.parse().ok());
    let mut signer_creds: BTreeSet<[u8; 28]> = ledger
        .meta_get(META_SIGNER_CREDS)?
        .map(|s| {
            s.split(',')
                .filter_map(|h| {
                    let b = hex::decode(h).ok()?;
                    (b.len() == 28).then(|| {
                        let mut k = [0u8; 28];
                        k.copy_from_slice(&b);
                        k
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let mut minted: BTreeSet<Vec<u8>> = ledger
        .meta_get(META_MINTED_ASSETS)?
        .map(|s| s.split(',').filter_map(|h| hex::decode(h).ok()).collect())
        .unwrap_or_default();

    let remote: Box<dyn Remote> = match args.remote.as_str() {
        "koios" => Box::new(Koios::new(
            args.koios_base.clone(),
            args.koios_token.clone(),
        )?),
        "offline" => Box::new(Offline),
        other => bail!("--remote must be koios|offline, got {other}"),
    };
    let checkpoint_path = args
        .checkpoint_file
        .clone()
        .unwrap_or_else(|| checkpoint::default_path(&args.db));

    tracing::info!(
        policy = %policy_hex,
        floor,
        resumed,
        members = state.frontier.len(),
        open_utxos = state.buffer.len(),
        holders = state.holders.len(),
        dir = %immutable_dir.display(),
        "walk: starting"
    );

    let from_point = resumed.then(|| (cursor_slot, cursor_hash.clone()));
    let blocks = mitos_chain_walk::open_blocks(&immutable_dir, from_point)?;

    let mut scanned = 0u64;
    let mut in_range = 0u64;
    let mut inserted = 0u64;
    let mut last_slot = 0u64;
    let mut last_hash: Option<Vec<u8>> = None;
    let mut stats = LadderStats::default();
    let mut rows = Rows::default();
    let mut ctx = TxCtx {
        policy,
        policy_hex: policy_hex.clone(),
        slot: 0,
        time: 0,
        signer_creds: &mut signer_creds,
        minted: &mut minted,
        ceiling: ledger
            .meta_get(META_CEILING_SLOT)?
            .and_then(|s| s.parse().ok()),
        royalty: ledger.meta_get(META_ROYALTY_ADDR)?,
        royalty_rate: ledger.meta_get(META_ROYALTY_RATE)?,
        last_mint_slot: ledger
            .meta_get(META_LAST_MINT_SLOT)?
            .and_then(|s| s.parse().ok()),
    };

    for block in blocks {
        let bytes = block.map_err(|e| anyhow::anyhow!("reading block from chunk: {e:?}"))?;
        let blk = MultiEraBlock::decode(&bytes)
            .map_err(|e| anyhow::anyhow!("decoding block at ~#{scanned}: {e:?}"))?;
        scanned += 1;
        let slot = blk.slot();
        // Below the floor: skip. On resume the point block itself was already
        // processed — skip it too, or its receipts double-count.
        if slot < floor || (resumed && slot <= cursor_slot) {
            if scanned.is_multiple_of(500_000) {
                tracing::info!(scanned, slot, "walk: skipping toward floor");
            }
            continue;
        }
        if let Some(t) = args.to_slot
            && slot > t
        {
            tracing::info!(to_slot = t, "walk: reached --to-slot");
            break;
        }
        in_range += 1;
        last_slot = slot;
        last_hash = Some(blk.hash().as_ref().to_vec());
        ctx.slot = slot;
        ctx.time = slot_to_unix(slot);

        for tx in blk.txs() {
            let d = decode_tx(&tx);
            process_tx(
                &tx,
                &d,
                &mut ctx,
                &mut state,
                &mut ledger,
                remote.as_ref(),
                &mut rows,
                &mut stats,
            )?;
        }
        inserted += rows.flush(&mut ledger)? as u64;

        if in_range.is_multiple_of(args.checkpoint_every.max(1)) {
            let hash = blk.hash();
            let frozen = reevaluate(&mut state, slot);
            for (p, r) in &frozen {
                tracing::warn!(party = %p.key, reason = ?r, slot, "walk: frontier froze a member");
            }
            do_checkpoint(&mut ledger, &state, &ctx, slot, hash.as_ref())?;
            checkpoint::write(
                &checkpoint_path,
                &CheckpointFile {
                    last_slot: slot,
                    last_block_height: Some(blk.number()),
                    last_block_hash: hex::encode(hash.as_ref()),
                    scanned_blocks: scanned,
                    in_range_blocks: in_range,
                    inserted_rows: inserted,
                    open_book: state.buffer.len(),
                    scope: vec![policy_hex.clone()],
                    updated_unix: checkpoint::now_unix(),
                    done: false,
                },
            )?;
            tracing::info!(
                scanned,
                in_range,
                slot,
                inserted,
                members = state.frontier.len(),
                open_utxos = state.buffer.len(),
                holders = state.holders.len(),
                minted = ctx.minted.len(),
                ladder = ?stats,
                "walk: checkpoint"
            );
        }
        if args.max_blocks != 0 && in_range >= args.max_blocks {
            tracing::info!(max_blocks = args.max_blocks, "walk: max-blocks reached");
            break;
        }
    }

    // Final flush + checkpoint + the floor test.
    if let Some(hash) = &last_hash {
        let frozen = reevaluate(&mut state, last_slot);
        for (p, r) in &frozen {
            tracing::warn!(party = %p.key, reason = ?r, "walk: frontier froze a member (final)");
        }
        do_checkpoint(&mut ledger, &state, &ctx, last_slot, hash)?;
    }
    let minted_n = ctx.minted.len() as u64;
    let basis = match expected_assets {
        Some(exp) if exp == minted_n => "observed",
        Some(exp) => {
            tracing::warn!(
                expected = exp,
                minted = minted_n,
                "walk: FLOOR TEST FAILED — the walk did not see every asset the policy minted; \
                 the floor is wrong or the walk stopped early"
            );
            "asserted"
        }
        None => "asserted",
    };
    ledger.meta_set(META_FLOOR_BASIS, basis)?;
    checkpoint::write(
        &checkpoint_path,
        &CheckpointFile {
            last_slot,
            last_block_height: None,
            last_block_hash: last_hash.as_deref().map(hex::encode).unwrap_or_default(),
            scanned_blocks: scanned,
            in_range_blocks: in_range,
            inserted_rows: inserted,
            open_book: state.buffer.len(),
            scope: vec![policy_hex.clone()],
            updated_unix: checkpoint::now_unix(),
            done: true,
        },
    )?;
    tracing::info!(
        scanned,
        in_range,
        last_slot,
        inserted,
        members = state.frontier.len(),
        minted = minted_n,
        expected = ?expected_assets,
        floor_basis = basis,
        ladder = ?stats,
        "walk: complete"
    );
    Ok(())
}

fn do_checkpoint(
    ledger: &mut Ledger,
    state: &WalkState,
    ctx: &TxCtx<'_>,
    slot: u64,
    hash: &[u8],
) -> Result<()> {
    ledger.checkpoint(state, slot, hash)?;
    ledger.meta_set(
        META_SIGNER_CREDS,
        &ctx.signer_creds
            .iter()
            .map(hex::encode)
            .collect::<Vec<_>>()
            .join(","),
    )?;
    ledger.meta_set(
        META_MINTED_ASSETS,
        &ctx.minted
            .iter()
            .map(hex::encode)
            .collect::<Vec<_>>()
            .join(","),
    )?;
    if let Some(c) = ctx.ceiling {
        ledger.meta_set(META_CEILING_SLOT, &c.to_string())?;
        ledger.meta_set(META_CEILING_SOURCE, "native_script_before")?;
    }
    if let Some(l) = ctx.last_mint_slot {
        ledger.meta_set(META_LAST_MINT_SLOT, &l.to_string())?;
    }
    if let Some(a) = &ctx.royalty {
        ledger.meta_set(META_ROYALTY_ADDR, a)?;
    }
    if let Some(r) = &ctx.royalty_rate {
        ledger.meta_set(META_ROYALTY_RATE, r)?;
    }
    Ok(())
}

/// Re-check every expanding member against the thresholds with the global
/// activity counts (split borrow: activity read, frontier written).
fn reevaluate(state: &mut WalkState, slot: u64) -> Vec<(Party, chain_ledger::TerminalReason)> {
    let WalkState {
        frontier, activity, ..
    } = state;
    frontier.reevaluate(slot, &|p: &Party| {
        stake_cred_of(p).map(|c| activity.get(&c))
    })
}

/// The staking credential behind a stake-keyed party (decodes the bech32).
fn stake_cred_of(p: &Party) -> Option<[u8; 28]> {
    if !p.has_stake_credential {
        return None;
    }
    let addr = pallas_addresses::Address::from_bech32(&p.key).ok()?;
    match addr {
        pallas_addresses::Address::Stake(s) => {
            let b = s.payload().as_ref();
            (b.len() == 28).then(|| {
                let mut k = [0u8; 28];
                k.copy_from_slice(b);
                k
            })
        }
        _ => None,
    }
}

fn policy_bytes(hex_id: &str) -> Result<[u8; 28]> {
    let v = hex::decode(hex_id).context("policy id hex")?;
    if v.len() != 28 {
        bail!("policy id must be 28 bytes");
    }
    let mut out = [0u8; 28];
    out.copy_from_slice(&v);
    Ok(out)
}

/// Per-walk mutable context threaded through `process_tx`.
pub struct TxCtx<'a> {
    pub policy: [u8; 28],
    pub policy_hex: String,
    pub slot: u64,
    pub time: u64,
    pub signer_creds: &'a mut BTreeSet<[u8; 28]>,
    pub minted: &'a mut BTreeSet<Vec<u8>>,
    pub ceiling: Option<u64>,
    pub royalty: Option<String>,
    pub royalty_rate: Option<String>,
    pub last_mint_slot: Option<u64>,
}

/// Rows accumulated per block, flushed together.
#[derive(Default)]
pub struct Rows {
    pub assets: Vec<AssetEventRow>,
    pub deltas: Vec<TxDeltaRow>,
    pub values: Vec<ValueEventRow>,
}

impl Rows {
    pub fn flush(&mut self, ledger: &mut Ledger) -> Result<usize> {
        let n = ledger.insert_asset_events(&self.assets)?
            + ledger.insert_tx_deltas(&self.deltas)?
            + ledger.insert_value_events(&self.values)?;
        self.assets.clear();
        self.deltas.clear();
        self.values.clear();
        Ok(n)
    }
}

/// One resolved output of the current tx.
struct Out<'a> {
    idx: u32,
    resolved: Resolved,
    lovelace: u64,
    assets: &'a [mitos_chain_walk::decode::Asset],
    /// (asset name, quantity) for assets under THE policy.
    policy_assets: Vec<(Vec<u8>, u64)>,
}

#[allow(clippy::too_many_arguments)]
pub fn process_tx(
    tx: &MultiEraTx<'_>,
    d: &DecodedTx,
    ctx: &mut TxCtx<'_>,
    state: &mut WalkState,
    ledger: &mut Ledger,
    remote: &dyn Remote,
    rows: &mut Rows,
    stats: &mut LadderStats,
) -> Result<()> {
    let tx_hex = hex::encode(d.tx_hash.as_ref());

    // 1. outputs → parties, activity, policy-asset amounts.
    let pallas_outs = tx.outputs();
    let outs: Vec<Out<'_>> = d
        .outputs
        .iter()
        .map(|o| {
            let resolved = resolve_str(&o.address);
            if let Some(c) = resolved.stake_cred {
                state.activity.bump(c);
            }
            let policy_assets = pallas_outs
                .get(o.index as usize)
                .map(|po| {
                    po.value()
                        .assets()
                        .iter()
                        .filter(|pa| pa.policy().as_ref() == ctx.policy)
                        .flat_map(|pa| {
                            pa.assets()
                                .iter()
                                .map(|a| (a.name().to_vec(), a.output_coin().unwrap_or(0)))
                                .collect::<Vec<_>>()
                        })
                        .collect()
                })
                .unwrap_or_default();
            Out {
                idx: o.index,
                resolved,
                lovelace: o.lovelace,
                assets: &o.assets,
                policy_assets,
            }
        })
        .collect();

    // 2. mints of the policy → seeds + ceiling + asset events.
    let mints: Vec<(Vec<u8>, i64)> = tx
        .mints()
        .iter()
        .filter(|pa| pa.policy().as_ref() == ctx.policy)
        .flat_map(|pa| {
            pa.assets()
                .iter()
                .map(|a| (a.name().to_vec(), a.mint_coin().unwrap_or(0)))
                .collect::<Vec<_>>()
        })
        .collect();
    let mut minted_now: BTreeSet<Vec<u8>> = BTreeSet::new();
    if !mints.is_empty() {
        ctx.last_mint_slot = Some(ctx.slot);
        if let Some(ps) = policy_script(tx, &ctx.policy) {
            for s in ps.signers {
                ctx.signer_creds.insert(s);
            }
            if let Some(b) = ps.before_slot {
                ctx.ceiling = Some(ctx.ceiling.map_or(b, |c| c.min(b)));
            }
        }
        if ctx.royalty.is_none()
            && let Some(r) = cip27_royalty(tx)
        {
            let rp = resolve_str(&r.addr);
            tracing::info!(addr = %r.addr, party = %rp.party.key, rate = ?r.rate, slot = ctx.slot, "walk: CIP-27 royalty address observed");
            state
                .frontier
                .seed(rp.party, Role::Royalty, ctx.slot)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            ctx.royalty = Some(r.addr);
            ctx.royalty_rate = r.rate;
        }
        for (name, amount) in &mints {
            if *amount > 0 {
                minted_now.insert(name.clone());
                ctx.minted.insert(name.clone());
                for o in outs
                    .iter()
                    .filter(|o| o.policy_assets.iter().any(|(n, _)| n == name))
                {
                    let qty = o
                        .policy_assets
                        .iter()
                        .find(|(n, _)| n == name)
                        .map(|(_, q)| *q)
                        .unwrap_or(0);
                    state.holders.set(name, &o.resolved.party.key, ctx.slot);
                    rows.assets.push(AssetEventRow {
                        tx_hash: tx_hex.clone(),
                        policy_id: ctx.policy_hex.clone(),
                        asset_name: hex::encode(name),
                        kind: "mint",
                        from_party: None,
                        to_party: Some(o.resolved.party.key.clone()),
                        quantity: qty as i64,
                        slot: ctx.slot,
                        block_time: ctx.time,
                    });
                }
            } else if *amount < 0 {
                let from = state.holders.remove(name);
                rows.assets.push(AssetEventRow {
                    tx_hash: tx_hex.clone(),
                    policy_id: ctx.policy_hex.clone(),
                    asset_name: hex::encode(name),
                    kind: "burn",
                    from_party: from,
                    to_party: None,
                    quantity: -amount,
                    slot: ctx.slot,
                    block_time: ctx.time,
                });
            }
        }
    }

    // Signer seed: any output whose payment credential is a policy signer.
    for o in &outs {
        if let Some(pc) = o.resolved.payment_cred
            && ctx.signer_creds.contains(&pc)
            && !state.frontier.is_member(&o.resolved.party)
        {
            tracing::info!(party = %o.resolved.party.key, slot = ctx.slot, "walk: policy signer address observed");
            state
                .frontier
                .seed(o.resolved.party.clone(), Role::Signer, ctx.slot)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
    }

    // 3. transfers (holder map; no input resolution).
    for o in &outs {
        for (name, qty) in &o.policy_assets {
            if minted_now.contains(name) {
                continue;
            }
            let to = &o.resolved.party.key;
            let prev = state.holders.set(name, to, ctx.slot);
            if prev.as_deref() == Some(to.as_str()) {
                continue; // self-transfer / consolidation
            }
            rows.assets.push(AssetEventRow {
                tx_hash: tx_hex.clone(),
                policy_id: ctx.policy_hex.clone(),
                asset_name: hex::encode(name),
                kind: "transfer",
                from_party: prev,
                to_party: Some(to.clone()),
                quantity: *qty as i64,
                slot: ctx.slot,
                block_time: ctx.time,
            });
        }
    }

    // 4. value flows — only if the tx touches a watched party.
    let touches = outs
        .iter()
        .any(|o| state.frontier.is_member(&o.resolved.party))
        || d.inputs.iter().any(|i| state.buffer.contains(&i.oref));
    if !touches {
        return Ok(());
    }

    // Inputs: buffer → ladder.
    let mut inputs: Vec<(Party, u64, Option<[u8; 28]>)> = Vec::new();
    let mut missing: Vec<OutRef> = Vec::new();
    for inp in &d.inputs {
        if let Some(b) = state.buffer.take(&inp.oref) {
            stats.buffer_hits += 1;
            let r = resolve_str(&b.address);
            inputs.push((r.party, b.lovelace, r.stake_cred));
        } else {
            missing.push(inp.oref);
        }
    }
    let found = resolve_missing(ledger, remote, &missing, stats)?;
    let mut unresolved = 0u32;
    for oref in &missing {
        match found.get(oref) {
            Some(c) => {
                let r = resolve_str(&c.address);
                inputs.push((r.party, c.lovelace, r.stake_cred));
            }
            None => unresolved += 1,
        }
    }

    // TxView.
    let mut parties: Vec<Party> = Vec::new();
    let mut creds: HashMap<String, Option<[u8; 28]>> = HashMap::new();
    let mut idx_of = |p: &Party, cred: Option<[u8; 28]>, parties: &mut Vec<Party>| -> usize {
        creds.entry(p.key.clone()).or_insert(cred);
        match parties.iter().position(|q| q == p) {
            Some(i) => i,
            None => {
                parties.push(p.clone());
                parties.len() - 1
            }
        }
    };
    let mut v_inputs = Vec::new();
    for (p, lovelace, cred) in &inputs {
        let i = idx_of(p, *cred, &mut parties);
        v_inputs.push(TxInput {
            party: i,
            value: *lovelace as i128,
            source: None,
        });
    }
    let mut v_outputs = Vec::new();
    for o in &outs {
        let i = idx_of(&o.resolved.party, o.resolved.stake_cred, &mut parties);
        v_outputs.push(TxOutput {
            party: i,
            value: o.lovelace as i128,
        });
    }
    let view = TxView {
        chain: Chain::Cardano,
        tx_id: tx_hex.clone(),
        timestamp: ctx.time as i64,
        parties,
        inputs: v_inputs,
        outputs: v_outputs,
    };
    let deltas = chain_ledger::net_deltas(&view);
    let mvs: Vec<Movement> = chain_ledger::movements(&view);

    // Frontier: every movement, with the receiver's global activity count.
    for mv in &mvs {
        let global = creds
            .get(&mv.to.key)
            .copied()
            .flatten()
            .map(|c| state.activity.get(&c));
        match state.frontier.on_movement(mv, ctx.slot, global) {
            FrontierOutcome::Promoted { terminal } => {
                tracing::debug!(party = %mv.to.key, from = %mv.from.key, ?terminal, slot = ctx.slot, "walk: promoted");
            }
            FrontierOutcome::Frozen(reason) => {
                tracing::warn!(party = %mv.to.key, ?reason, slot = ctx.slot, "walk: frontier froze a member");
            }
            _ => {}
        }
    }

    // Rows (membership evaluated AFTER promotion so a party promoted here gets
    // its first receipt).
    for dlt in &deltas {
        if state.frontier.is_member(&dlt.party) {
            rows.deltas.push(TxDeltaRow {
                tx_hash: tx_hex.clone(),
                party: dlt.party.key.clone(),
                delta: dlt.delta as i64,
                slot: ctx.slot,
                block_time: ctx.time,
                unresolved_inputs: unresolved,
            });
        }
    }
    for mv in &mvs {
        if state.frontier.is_member(&mv.from) {
            rows.values.push(ValueEventRow {
                tx_hash: tx_hex.clone(),
                party: mv.from.key.clone(),
                counterparty: mv.to.key.clone(),
                delta: -(mv.value as i64),
                slot: ctx.slot,
                block_time: ctx.time,
                unresolved_inputs: unresolved,
            });
        }
        if state.frontier.is_member(&mv.to) {
            rows.values.push(ValueEventRow {
                tx_hash: tx_hex.clone(),
                party: mv.to.key.clone(),
                counterparty: mv.from.key.clone(),
                delta: mv.value as i64,
                slot: ctx.slot,
                block_time: ctx.time,
                unresolved_inputs: unresolved,
            });
        }
    }

    // Buffer the outputs now held by members.
    for o in &outs {
        if state.frontier.is_member(&o.resolved.party) {
            state.buffer.insert(
                (d.tx_hash, o.idx),
                BufferedOutput {
                    address: d.outputs[o.idx as usize].address.clone(),
                    lovelace: o.lovelace,
                    assets: o
                        .assets
                        .iter()
                        .map(|a| (a.policy.clone(), a.name.clone()))
                        .collect(),
                    party: o.resolved.party.key.clone(),
                    has_stake: o.resolved.party.has_stake_credential,
                },
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activity::Activity;
    use crate::state::{Buffer, Holders};
    use chain_ledger::{Frontier, Thresholds};
    use std::path::PathBuf;

    /// Drive `process_tx` over every tx of a captured mainnet block with a
    /// policy nobody minted and an empty frontier: nothing touches, activity is
    /// counted, no rows, no panics in the pallas paths.
    #[test]
    fn mainnet_block_fixture_smoke() {
        let fixture =
            PathBuf::from("../../crates/mitos-platform/tests/fixtures/186000000.block.cbor");
        if !fixture.exists() {
            eprintln!("skipping: fixture not present at {}", fixture.display());
            return;
        }
        let cbor = std::fs::read(&fixture).unwrap();
        let blk = MultiEraBlock::decode(&cbor).unwrap();
        let mut ledger = Ledger::open_in_memory().unwrap();
        let mut state = WalkState {
            frontier: Frontier::new(Thresholds::default(), []),
            buffer: Buffer::default(),
            activity: Activity::default(),
            holders: Holders::default(),
        };
        let mut signer_creds = BTreeSet::new();
        let mut minted = BTreeSet::new();
        let mut ctx = TxCtx {
            policy: [0xAB; 28],
            policy_hex: hex::encode([0xAB; 28]),
            slot: blk.slot(),
            time: slot_to_unix(blk.slot()),
            signer_creds: &mut signer_creds,
            minted: &mut minted,
            ceiling: None,
            royalty: None,
            royalty_rate: None,
            last_mint_slot: None,
        };
        let mut rows = Rows::default();
        let mut stats = LadderStats::default();
        let mut txs = 0;
        for tx in blk.txs() {
            let d = decode_tx(&tx);
            process_tx(
                &tx,
                &d,
                &mut ctx,
                &mut state,
                &mut ledger,
                &Offline,
                &mut rows,
                &mut stats,
            )
            .unwrap();
            txs += 1;
        }
        assert!(txs > 0);
        assert!(!state.activity.is_empty(), "outputs bumped activity");
        assert!(rows.assets.is_empty() && rows.deltas.is_empty() && rows.values.is_empty());
        assert!(state.buffer.is_empty());
        assert_eq!(stats.remote_calls, 0);
    }

    /// Seed the frontier with the stake key of the FIRST output of the first
    /// tx: that tx now touches, inputs are unresolved offline, the party gets a
    /// tx_delta row flagged partial, and its outputs land in the buffer.
    #[test]
    fn touching_tx_books_rows_and_buffers_outputs() {
        let fixture =
            PathBuf::from("../../crates/mitos-platform/tests/fixtures/186000000.block.cbor");
        if !fixture.exists() {
            return;
        }
        let cbor = std::fs::read(&fixture).unwrap();
        let blk = MultiEraBlock::decode(&cbor).unwrap();
        let txs = blk.txs();
        // Find a tx with a stake-keyed output.
        let (tx, seed) = txs
            .iter()
            .find_map(|tx| {
                let d = decode_tx(tx);
                d.outputs.iter().find_map(|o| {
                    let r = resolve_str(&o.address);
                    r.party.has_stake_credential.then_some((tx, r.party))
                })
            })
            .expect("a stake-keyed output in the fixture");
        let d = decode_tx(tx);
        let mut ledger = Ledger::open_in_memory().unwrap();
        let mut frontier = Frontier::new(Thresholds::default(), []);
        frontier.seed(seed.clone(), Role::Declared, 0).unwrap();
        let mut state = WalkState {
            frontier,
            buffer: Buffer::default(),
            activity: Activity::default(),
            holders: Holders::default(),
        };
        let mut signer_creds = BTreeSet::new();
        let mut minted = BTreeSet::new();
        let mut ctx = TxCtx {
            policy: [0xAB; 28],
            policy_hex: hex::encode([0xAB; 28]),
            slot: blk.slot(),
            time: slot_to_unix(blk.slot()),
            signer_creds: &mut signer_creds,
            minted: &mut minted,
            ceiling: None,
            royalty: None,
            royalty_rate: None,
            last_mint_slot: None,
        };
        let mut rows = Rows::default();
        let mut stats = LadderStats::default();
        process_tx(
            tx,
            &d,
            &mut ctx,
            &mut state,
            &mut ledger,
            &Offline,
            &mut rows,
            &mut stats,
        )
        .unwrap();
        // Offline: every input unresolved, counted not guessed.
        assert_eq!(stats.unresolved as usize, d.inputs.len());
        let mine: Vec<_> = rows.deltas.iter().filter(|r| r.party == seed.key).collect();
        assert_eq!(mine.len(), 1, "one net delta for the seeded party");
        assert_eq!(mine[0].unresolved_inputs as usize, d.inputs.len());
        assert!(
            mine[0].delta > 0,
            "with no inputs resolved the party's delta is its outputs"
        );
        // Its outputs are now buffered.
        let buffered = state
            .buffer
            .entries()
            .filter(|(_, o)| o.party == seed.key)
            .count();
        assert!(buffered >= 1);
        // Nothing was promoted: no member paid anyone (inputs unresolved).
        assert_eq!(state.frontier.len(), 1);
    }
}
