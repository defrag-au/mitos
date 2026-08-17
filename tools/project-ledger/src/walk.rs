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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use chain_ledger::{
    AliasKind, Chain, FrontierOutcome, Movement, Party, Role, TxInput, TxOutput, TxView,
};
use mitos_chain_walk::checkpoint::{self, CheckpointFile};
use mitos_chain_walk::decode::{DecodedTx, OutRef, decode_tx};
use mitos_chain_walk::slot_to_unix;
use pallas_traverse::{MultiEraBlock, MultiEraTx};

use crate::asset_class::AssetClass;
use crate::koios::Koios;
use crate::mint::{cip27_royalty, policy_script};
use crate::party::{Resolved, resolve_str};
use crate::resolve::{LadderStats, Offline, Remote, resolve_missing};
use crate::seed::*;
use crate::state::{BufferedOutput, WalkState};
use crate::store::{
    AliasRow, AssetEventRow, Ledger, MintPaymentRow, TxDeltaRow, UnitFlowRow, ValueEventRow,
};

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

    /// How many promotion hops from a seed the frontier may expand.
    ///
    /// Phase 1 (the thing to get right first) is **funds in + assets out**, with
    /// the mint transactions as the distribution cue — that is hop 0, the
    /// declared mint wallets themselves. Hop 1 adds the **direct** recipients of
    /// distribution funds, which is the next thing that matters. Beyond that the
    /// frontier starts inferring rather than observing, and the first real run
    /// showed how fast that degrades (20,678 parties; see the
    /// project-ledger-frontier-explosion note).
    ///
    ///   0 = seeds only — no expansion at all. Counterparties are still RECORDED
    ///       on every row; they simply don't become watched parties themselves.
    ///   1 = + direct recipients of treasury/mint outflows  (DEFAULT)
    ///   2 = + their recipients (the design doc's "two hops")
    ///  64 = effectively unbounded (the original, explosive behaviour)
    #[arg(long, default_value_t = 1)]
    max_hops: u32,
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

    // ALWAYS seek — never stream from genesis. A 32-byte cursor hash seeks
    // precisely (resume); the EMPTY hash `seed` writes is pallas-hardano's
    // FUZZY seek, which binary-searches the chunk list for the first block at
    // slot >= walk_start. For a 2025 mint that skips ~7,400 of ~8,900 chunk
    // files; decoding from genesis to reach the floor would cost over an hour
    // of CPU before the first useful block.
    let blocks =
        mitos_chain_walk::open_blocks(&immutable_dir, Some((cursor_slot, cursor_hash.clone())))?;

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
        max_hops: args.max_hops,
        minted_holdings: ledger
            .meta_get(META_MINTED_HOLDINGS)?
            .map(|s| s.split(',').filter_map(|h| hex::decode(h).ok()).collect())
            .unwrap_or_default(),
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
        supply = ctx.minted_holdings.len(),
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
    ledger.meta_set(
        META_MINTED_HOLDINGS,
        &ctx.minted_holdings
            .iter()
            .map(hex::encode)
            .collect::<Vec<_>>()
            .join(","),
    )?;
    // The collection's REAL supply — what a holder chart scales to. Distinct
    // from `expected_assets` (every asset minted under the policy), which is
    // what proves floor coverage: a CIP-68 mint emits two per NFT.
    ledger.meta_set(META_SUPPLY, &ctx.minted_holdings.len().to_string())?;
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

/// Hops from the nearest seed to `party`, by following `promoted_by`.
///
/// Seeds are 0. `None` if the chain is broken or longer than a sane bound (a
/// cycle cannot occur — promotion only ever adds a NEW member — but the guard
/// keeps this total).
fn hops_from_seed(frontier: &chain_ledger::Frontier, party: &Party) -> Option<u32> {
    let mut cur = frontier.member(party)?;
    let mut hops = 0u32;
    while let Some(parent) = &cur.promoted_by {
        hops += 1;
        if hops > 64 {
            return None;
        }
        cur = frontier.member(parent)?;
    }
    Some(hops)
}

/// Whether a movement to `to_key` may promote it into the frontier.
///
/// False only for a NON-member receiving a policy asset in the same tx — the
/// buyer side of a sale. Already-watched parties are unaffected, so their
/// receipt counters keep advancing whether or not they also bought something.
fn may_promote(asset_receivers: &BTreeSet<&str>, to_key: &str, is_member: bool) -> bool {
    is_member || !asset_receivers.contains(to_key)
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
    /// Promotion-hop bound; 0 = unbounded.
    pub max_hops: u32,
    /// Distinct HOLDER-FACING assets minted (excludes CIP-68 reference tokens
    /// and labelled fungibles). This is the collection's real supply — Mekka S1
    /// mints 10,001 policy assets but is a 5,000-NFT collection.
    pub minted_holdings: BTreeSet<Vec<u8>>,
}

/// Rows accumulated per block, flushed together.
#[derive(Default)]
pub struct Rows {
    pub assets: Vec<AssetEventRow>,
    pub mint_payments: Vec<MintPaymentRow>,
    pub aliases: Vec<AliasRow>,
    pub deltas: Vec<TxDeltaRow>,
    pub values: Vec<ValueEventRow>,
    pub units: Vec<UnitFlowRow>,
}

impl Rows {
    pub fn flush(&mut self, ledger: &mut Ledger) -> Result<usize> {
        let n = ledger.insert_asset_events(&self.assets)?
            + ledger.insert_mint_payments(&self.mint_payments)?
            + ledger.insert_aliases(&self.aliases)?
            + ledger.insert_tx_deltas(&self.deltas)?
            + ledger.insert_value_events(&self.values)?
            + ledger.insert_unit_flows(&self.units)?;
        self.assets.clear();
        self.mint_payments.clear();
        self.aliases.clear();
        self.deltas.clear();
        self.values.clear();
        self.units.clear();
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
    /// EVERY native asset in this output, with its quantity: `(policy, name,
    /// qty)`. `decode::Asset` carries identity only, so the quantities come
    /// straight off the pallas output — no change to the shared walk crate and
    /// none to the live market-ledger that also uses it.
    bundle: Vec<(Vec<u8>, Vec<u8>, u64)>,
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
            let bundle = pallas_outs
                .get(o.index as usize)
                .map(|po| {
                    po.value()
                        .assets()
                        .iter()
                        .flat_map(|pa| {
                            let policy = pa.policy().as_slice().to_vec();
                            pa.assets()
                                .iter()
                                .map(|a| {
                                    (
                                        policy.clone(),
                                        a.name().to_vec(),
                                        a.output_coin().unwrap_or(0),
                                    )
                                })
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
                bundle,
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
                if AssetClass::of(name).is_holding() {
                    ctx.minted_holdings.insert(name.clone());
                }
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
                        asset_class: AssetClass::of(name).as_str(),
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
                    asset_class: AssetClass::of(name).as_str(),
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

    // ── the mint transaction's fund split ────────────────────────────────
    //
    // A mint bakes distribution INTO the mint tx: the buyer pays once, and the
    // transaction splits it — most to a treasury, often a cut to an artist,
    // sometimes a platform fee. Reading the mint as a single destination loses
    // every one of those legs, and they are the distribution story.
    //
    // The test needs no input resolution: an output to a party that received NO
    // policy asset in this tx is money going somewhere. That single rule also
    // excludes the buyer's change and the minAda riding with each token (both
    // land on the asset recipient) and the reference token's deposit (that
    // output carries a policy asset).
    if !mints.is_empty() {
        let asset_parties: BTreeSet<&str> = outs
            .iter()
            .filter(|o| !o.policy_assets.is_empty())
            .map(|o| o.resolved.party.key.as_str())
            .collect();

        let mut by_dest: BTreeMap<&str, u64> = BTreeMap::new();
        for o in &outs {
            if !asset_parties.contains(o.resolved.party.key.as_str()) {
                *by_dest.entry(o.resolved.party.key.as_str()).or_insert(0) += o.lovelace;
            }
        }
        for (dest, lovelace) in by_dest {
            rows.mint_payments.push(MintPaymentRow {
                tx_hash: tx_hex.clone(),
                destination: dest.to_owned(),
                lovelace: lovelace as i64,
                slot: ctx.slot,
                block_time: ctx.time,
            });
            // NOT seeded as a watched party — deliberately, for now.
            //
            // These destinations ARE the project's money, so watching their
            // onward flows is the next phase. But a mint's payees include the
            // minting platform's own fee wallet, which is custodial-scale: as a
            // hop-0 seed it is exempt from the terminal rule and recruits every
            // counterparty it touches. Measured: seeding them took the frontier
            // from 64 parties to 2,826 and the ledger from 1.7GB to 2.2GB,
            // while adding nothing to the fund split — `mint_payment` above
            // already records every leg.
            //
            // Doing this properly needs a `Role::MintPayee` in `chain-ledger`
            // that is NOT exempt from the terminal rule, so an artist wallet is
            // watched and expands while a platform wallet is recorded and
            // frozen. That is a shared-crates change, and it belongs with the
            // phase that actually consumes the onward flows.
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
                asset_class: AssetClass::of(name).as_str(),
                kind: "transfer",
                from_party: prev,
                to_party: Some(to.clone()),
                quantity: *qty as i64,
                slot: ctx.slot,
                block_time: ctx.time,
            });
        }
    }

    // 3b. aliases — every name a TRACKED wallet goes by, so a reader can find
    // it by whatever they have to hand. Tracked = watched party or a current
    // holder (this tx's holdings are already applied above). Payment addresses
    // are free (the output is already resolved); ADA Handles are assets under
    // one policy, so an output carrying one names its receiver. Both without an
    // indexer. `INSERT OR IGNORE` on (party, kind, value) makes re-seeing cheap.
    for o in &outs {
        let key = o.resolved.party.key.as_str();
        let tracked = state.frontier.is_member(&o.resolved.party) || state.holders.holds(key);
        if !tracked {
            continue;
        }
        let address = &d.outputs[o.idx as usize].address;
        if address != key {
            rows.aliases.push(AliasRow {
                party: key.to_owned(),
                kind: AliasKind::Address,
                value: address.clone(),
                slot: ctx.slot,
            });
        }
        for h in crate::alias::handles_in(o.assets) {
            rows.aliases.push(AliasRow {
                party: key.to_owned(),
                kind: AliasKind::Handle,
                value: h,
                slot: ctx.slot,
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

    // A movement that DELIVERS a policy asset to its receiver is a SALE, not a
    // payment — so it must not promote. The receiver is a customer, already
    // recorded in full on the assets-out side as an `asset_event`; treating the
    // delivery leg as "the treasury paid this wallet" makes every one of a
    // mint's thousands of buyers a watched ops wallet, and each of those then
    // expands until the custodial threshold freezes it.
    //
    // The first real run surfaced exactly that: 527k `tx_delta` rows against
    // 1.1k minted assets, 88 members frozen, before this guard existed. The
    // terminal rule can't catch it — a customer is low-volume, so it looks
    // nothing like a custodian.
    //
    // Only PROMOTION is suppressed. An already-watched party that happens to
    // buy an asset still counts the receipt normally.
    let asset_receivers: BTreeSet<&str> = outs
        .iter()
        .filter(|o| !o.policy_assets.is_empty())
        .map(|o| o.resolved.party.key.as_str())
        .collect();

    // Frontier: every movement, with the receiver's global activity count.
    for mv in &mvs {
        let already = state.frontier.is_member(&mv.to);
        if !may_promote(&asset_receivers, &mv.to.key, already) {
            continue;
        }
        // Hop bound: only an EXPANDING member within the bound may promote. A
        // party at the limit is still RECORDED — it appears as the counterparty
        // on the payer's rows — it just never becomes a watch-set member whose
        // own payments recruit further.
        if !already
            && hops_from_seed(&state.frontier, &mv.from).is_none_or(|h| h + 1 > ctx.max_hops)
        {
            continue;
        }
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

    // Unit flows — what actually MOVED, ADA and every token, per output.
    //
    // The treasury does not only spend ADA: it off-ramps through a stable, pays
    // suppliers in USDM, and gets paid in assets. None of that is lovelace, so
    // none of it is in `value_event`. Attribution here is per OUTPUT rather than
    // pro-rata: the recipient of an output is exact, and only the payer is a
    // judgement when several parties funded the tx (recorded as `payers`).
    book_unit_flows(&outs, &inputs, state, ctx, &tx_hex, unresolved, rows);

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

/// Book one `unit_flow` row per (output, unit) where a watched party is on
/// either end.
///
/// The payer is the input party that put in the most lovelace. That is a
/// judgement, so `payers` carries how many distinct parties funded the tx: 1 is
/// exact, more than 1 says "attributed" on the row itself rather than in a
/// footnote nobody reads.
fn book_unit_flows(
    outs: &[Out<'_>],
    inputs: &[(Party, u64, Option<[u8; 28]>)],
    state: &WalkState,
    ctx: &TxCtx<'_>,
    tx_hex: &str,
    unresolved: u32,
    rows: &mut Rows,
) {
    // Funders, by lovelace contributed. A party can appear on several inputs.
    let mut by_payer: BTreeMap<&str, (u64, &Party)> = BTreeMap::new();
    for (p, lovelace, _) in inputs {
        let e = by_payer.entry(p.key.as_str()).or_insert((0, p));
        e.0 += lovelace;
    }
    // The dominant funder. Ties break on the key so a replay books identically.
    // Every input unresolved (an offline walk, a stranger's UTxO): there is no
    // payer to NAME, and inventing one is the failure this tool exists to
    // prevent. But the receipt itself is still a fact — the recipient and the
    // quantity are read straight off the output — so it is recorded with an
    // EMPTY counterparty rather than dropped. Silently losing every inbound
    // payment would be the worse lie: "the treasury was never paid in USDM"
    // reads identically to "we could not tell who paid".
    let payer = by_payer
        .iter()
        .max_by_key(|(k, (v, _))| (*v, std::cmp::Reverse(*k)))
        .map(|(_, &(_, p))| p);
    // A DOMINANT FUNDER IS ONLY KNOWABLE WHEN EVERY INPUT RESOLVED.
    //
    // `by_payer` sums the inputs we could resolve; the outputs are all of them.
    // With even one input unresolved, a watched party holding a small resolved
    // input becomes "the payer" of money that came from somewhere else entirely
    // — and every output of the tx is then booked as the treasury spending.
    //
    // Measured on Mekka S1 before this guard: tx 92b2f6c0's true treasury delta
    // is -2,374 ADA and it was booked as 1,893,881 ADA out, because the tx had
    // two unresolved inputs. 96.4% of transactions in an offline walk have at
    // least one, so the heuristic was wrong far more often than it was right.
    //
    // Receipts are NOT affected: an output to a watched party is exact no matter
    // where the money came from. So an offline walk gives complete inbound and
    // only fully-resolved outbound; `--remote koios` is what fills the rest in.
    let attributable = unresolved == 0;
    let payers = by_payer.len() as u32;
    let payer_watched = attributable && payer.is_some_and(|p| state.frontier.is_member(p));
    let payer_key = match attributable {
        true => payer.map(|p| p.key.as_str()).unwrap_or(""),
        false => "",
    };

    for o in outs {
        let to = &o.resolved.party;
        // An output back to a funder is CHANGE, not a payment.
        if by_payer.contains_key(to.key.as_str()) {
            continue;
        }
        let to_watched = state.frontier.is_member(to);
        if !payer_watched && !to_watched {
            continue;
        }
        let mut units: Vec<(String, u64)> = Vec::new();
        if o.lovelace > 0 {
            units.push(("lovelace".to_string(), o.lovelace));
        }
        for (policy, name, qty) in &o.bundle {
            if *qty > 0 {
                units.push((crate::store::unit_of(policy, name), *qty));
            }
        }
        for (unit, qty) in units {
            let q = qty.min(i64::MAX as u64) as i64;
            if payer_watched {
                rows.units.push(UnitFlowRow {
                    tx_hash: tx_hex.to_string(),
                    output_index: o.idx,
                    party: payer_key.to_string(),
                    counterparty: to.key.clone(),
                    unit: unit.clone(),
                    quantity: -q,
                    payers,
                    slot: ctx.slot,
                    block_time: ctx.time,
                });
            }
            if to_watched {
                rows.units.push(UnitFlowRow {
                    tx_hash: tx_hex.to_string(),
                    output_index: o.idx,
                    party: to.key.clone(),
                    counterparty: payer_key.to_string(),
                    unit,
                    quantity: q,
                    payers,
                    slot: ctx.slot,
                    block_time: ctx.time,
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activity::Activity;
    use crate::state::{Buffer, Holders};
    use chain_ledger::{Frontier, Thresholds};
    use std::path::PathBuf;

    #[test]
    fn asset_delivery_does_not_promote_a_stranger_but_members_still_count() {
        let mut receivers = BTreeSet::new();
        receivers.insert("stake1buyer");
        // A stranger receiving a policy asset is a customer — never promoted.
        assert!(!may_promote(&receivers, "stake1buyer", false));
        // The same wallet, once watched, still counts its receipts.
        assert!(may_promote(&receivers, "stake1buyer", true));
        // A stranger paid WITHOUT receiving an asset is a genuine ops-wallet
        // candidate — this is the case the expanding frontier exists for.
        assert!(may_promote(&receivers, "stake1contractor", false));
    }

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
            max_hops: 0,
            minted_holdings: BTreeSet::new(),
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
            max_hops: 0,
            minted_holdings: BTreeSet::new(),
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

    // ── unit flows ────────────────────────────────────────────────────────

    fn party(key: &str) -> Party {
        Party {
            key: key.to_string(),
            chain: Chain::Cardano,
            has_stake_credential: true,
        }
    }

    fn out(
        idx: u32,
        to: &str,
        lovelace: u64,
        bundle: Vec<(Vec<u8>, Vec<u8>, u64)>,
    ) -> Out<'static> {
        Out {
            idx,
            resolved: Resolved {
                party: party(to),
                stake_cred: None,
                payment_cred: None,
                payment_is_script: false,
            },
            lovelace,
            assets: &[],
            policy_assets: Vec::new(),
            bundle,
        }
    }

    fn seeded(watched: &[&str]) -> WalkState {
        let mut frontier = Frontier::new(Thresholds::default(), []);
        for k in watched {
            frontier.seed(party(k), Role::Declared, 0).unwrap();
        }
        WalkState {
            frontier,
            buffer: Buffer::default(),
            activity: Activity::default(),
            holders: Holders::default(),
        }
    }

    fn run_flows(
        outs: &[Out<'_>],
        inputs: &[(Party, u64, Option<[u8; 28]>)],
        state: &WalkState,
        unresolved: u32,
    ) -> Vec<UnitFlowRow> {
        let mut signer_creds = BTreeSet::new();
        let mut minted = BTreeSet::new();
        let ctx = TxCtx {
            policy: [0xAB; 28],
            policy_hex: hex::encode([0xAB; 28]),
            slot: 7,
            time: 1_700_000_000,
            signer_creds: &mut signer_creds,
            minted: &mut minted,
            ceiling: None,
            royalty: None,
            royalty_rate: None,
            last_mint_slot: None,
            max_hops: 0,
            minted_holdings: BTreeSet::new(),
        };
        let mut rows = Rows::default();
        book_unit_flows(outs, inputs, state, &ctx, "txhash", unresolved, &mut rows);
        rows.units
    }

    fn flows(
        outs: &[Out<'_>],
        inputs: &[(Party, u64, Option<[u8; 28]>)],
        watched: &[&str],
    ) -> Vec<UnitFlowRow> {
        let state = seeded(watched);
        let mut signer_creds = BTreeSet::new();
        let mut minted = BTreeSet::new();
        let ctx = TxCtx {
            policy: [0xAB; 28],
            policy_hex: hex::encode([0xAB; 28]),
            slot: 7,
            time: 1_700_000_000,
            signer_creds: &mut signer_creds,
            minted: &mut minted,
            ceiling: None,
            royalty: None,
            royalty_rate: None,
            last_mint_slot: None,
            max_hops: 0,
            minted_holdings: BTreeSet::new(),
        };
        let mut rows = Rows::default();
        book_unit_flows(outs, inputs, &state, &ctx, "txhash", 0, &mut rows);
        rows.units
    }

    const USDM: [u8; 4] = [0xc4, 0x8c, 0xbb, 0x3d];

    /// The case the whole table exists for: the treasury pays a supplier in a
    /// stable, not in ADA. Both legs of the bundle are booked, and the ADA
    /// riding along with the token is not mistaken for the payment.
    #[test]
    fn a_token_payment_is_booked_per_unit() {
        let rows = flows(
            &[out(
                0,
                "stake1supplier",
                1_400_000,
                vec![(USDM.to_vec(), b"USDM".to_vec(), 1_500_000_000)],
            )],
            &[(party("stake1treasury"), 900_000_000, None)],
            &["stake1treasury"],
        );
        assert_eq!(rows.len(), 2, "one row per unit in the output");
        let usdm = rows
            .iter()
            .find(|r| {
                r.unit.contains("55534d4d")
                    || r.unit == format!("{}.{}", hex::encode(USDM), hex::encode(b"USDM"))
            })
            .expect("the USDM leg");
        assert_eq!(usdm.quantity, -1_500_000_000, "out of the treasury");
        assert_eq!(usdm.counterparty, "stake1supplier");
        assert_eq!(usdm.payers, 1, "single funder — exact, not attributed");
        let ada = rows.iter().find(|r| r.unit == "lovelace").unwrap();
        assert_eq!(ada.quantity, -1_400_000);
    }

    /// Being PAID in assets — the other direction, same table. Mekka did this.
    #[test]
    fn the_treasury_being_paid_in_assets_is_the_same_table() {
        let rows = flows(
            &[out(
                0,
                "stake1treasury",
                2_000_000,
                vec![(vec![0x11; 28], b"SOMETOKEN".to_vec(), 42)],
            )],
            &[(party("stake1payer"), 500_000_000, None)],
            &["stake1treasury"],
        );
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.quantity > 0), "inbound is positive");
        assert!(rows.iter().all(|r| r.counterparty == "stake1payer"));
    }

    /// Change is not a payment. Without this every tx reports the treasury
    /// paying itself the balance of its own wallet.
    #[test]
    fn change_back_to_a_funder_is_not_a_flow() {
        let rows = flows(
            &[
                out(0, "stake1supplier", 5_000_000, vec![]),
                out(1, "stake1treasury", 900_000_000, vec![]), // change
            ],
            &[(party("stake1treasury"), 906_000_000, None)],
            &["stake1treasury"],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].counterparty, "stake1supplier");
        assert_eq!(rows[0].quantity, -5_000_000);
    }

    /// Several funders means the payer is a JUDGEMENT — the row says so, and
    /// the dominant funder is the one named.
    #[test]
    fn multiple_funders_are_recorded_as_such() {
        let rows = flows(
            &[out(0, "stake1supplier", 5_000_000, vec![])],
            &[
                (party("stake1treasury"), 900_000_000, None),
                (party("stake1cosigner"), 10_000_000, None),
            ],
            &["stake1treasury"],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].party, "stake1treasury", "the dominant funder");
        assert_eq!(rows[0].payers, 2, "and the ambiguity is on the row");
    }

    /// An unresolvable payer must not delete the receipt. The recipient and
    /// the quantity are read off the output and are exact; only the payer is
    /// unknown, and an empty counterparty says so.
    #[test]
    fn an_unresolved_payer_still_records_the_receipt() {
        let rows = flows(
            &[out(
                0,
                "stake1treasury",
                3_000_000,
                vec![(USDM.to_vec(), b"USDM".to_vec(), 250)],
            )],
            &[],
            &["stake1treasury"],
        );
        assert_eq!(rows.len(), 2, "the receipt survives an unknown payer");
        assert!(rows.iter().all(|r| r.counterparty.is_empty()));
        assert!(
            rows.iter().all(|r| r.payers == 0),
            "0 payers = nobody named"
        );
        assert!(rows.iter().all(|r| r.quantity > 0));
    }

    /// The guard that matters most: with ANY input unresolved there is no
    /// knowable funder, so nothing outbound may be attributed — but the
    /// receipt, which is read off the output, still stands.
    #[test]
    fn an_unresolved_input_forbids_outbound_attribution() {
        let outs = [
            out(0, "stake1stranger", 1_893_881_000_000, vec![]),
            out(1, "stake1treasury", 5_000_000, vec![]),
        ];
        let inputs = [(party("stake1treasury"), 900_000_000, None)];
        // Fully resolved: the treasury really is the funder.
        let state = seeded(&["stake1treasury"]);
        let clean = run_flows(&outs, &inputs, &state, 0);
        assert!(
            clean
                .iter()
                .any(|r| r.quantity < 0 && r.counterparty == "stake1stranger"),
            "a fully-resolved tx still attributes outbound"
        );
        // One unresolved input: the 1.89M could have come from anywhere.
        let dirty = run_flows(&outs, &inputs, &state, 1);
        assert!(
            dirty.iter().all(|r| r.quantity > 0),
            "nothing outbound may be attributed: {dirty:?}"
        );
        assert!(
            dirty.iter().all(|r| r.counterparty.is_empty()),
            "and no payer is named"
        );
    }

    /// A tx between two strangers writes nothing, even though it is decoded.
    #[test]
    fn a_tx_that_touches_nobody_watched_writes_nothing() {
        let rows = flows(
            &[out(0, "stake1bob", 5_000_000, vec![])],
            &[(party("stake1alice"), 6_000_000, None)],
            &["stake1treasury"],
        );
        assert!(rows.is_empty());
    }
}
