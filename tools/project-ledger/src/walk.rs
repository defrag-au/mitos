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
use crate::state::{BufferedOutput, RelayCandidate, WalkState};
use crate::store::{
    AliasRow, AssetEventRow, AssetInflowRow, Ledger, MintPaymentRow, RelayHopRow, TxDeltaRow,
    UnitFlowRow, ValueEventRow,
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

    /// Seat every wallet that RECEIVES a holder-facing asset of the policy as
    /// a watched-but-never-expanding party (`Role::Holder`).
    ///
    /// The money frontier cannot reach these wallets: promotion follows
    /// outbound VALUE edges, and on a queued mint the buyer never funds the
    /// mint transaction — payment and fulfilment are separate txs. Without
    /// this the collection's own customers are invisible and their purchase
    /// legs unattributed. Bounded by the real holder base (they recruit
    /// nobody), but on a large old collection it seats every historical
    /// holder, which grows the walk — disable for a money-only pass.
    ///
    /// Discovery persists across `reset` (the `discovered_holder` table), so
    /// the standard two-pass pipeline seats them FROM THE FLOOR on the second
    /// walk and books the payment legs that PRECEDE each holder's first
    /// receipt.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    watch_holders: bool,

    /// Follow money ONE HOP through single-use bare addresses.
    ///
    /// A watched wallet pays a fresh stakeless address; minutes later that
    /// address forwards everything and is never used again. That is an
    /// exchange deposit, and without this the trail simply stops there —
    /// `classify` then reads the one-way shape as an off-ramp, which asserts
    /// the money left the chain while discarding where it actually went.
    ///
    /// Cheap and bounded: candidates expire after `--relay-window-slots`, a
    /// relay is followed exactly one hop, and NOTHING is promoted. It adds
    /// depth to the trail, never breadth to the watch set.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    follow_relays: bool,

    /// How long a bare address may hold the money and still read as a relay
    /// (slots; 1 slot ≈ 1 second). Default 2 hours.
    #[arg(long, default_value_t = 7200)]
    relay_window_slots: u64,

    /// Ignore bare outputs below this (lovelace). A relay carries a payment;
    /// below the min-UTxO floor it is carrier ADA on an asset transfer.
    #[arg(long, default_value_t = 5_000_000)]
    relay_min_lovelace: u64,
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
    // Read ONCE, before the walk: the project boundary is asserted at seed and
    // cannot change mid-walk. Empty is the normal case for an un-curated
    // ledger and makes the whole capture a single `is_empty` check per tx.
    let project_side = ledger.project_side_parties()?;
    if !project_side.is_empty() {
        tracing::info!(
            wallets = project_side.len(),
            "walk: project-side wallets declared — foreign assets arriving at them \
             will be recorded as return legs (`asset_inflow`)"
        );
    }
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
        watch_holders: args.watch_holders,
        follow_relays: args.follow_relays,
        relay_window_slots: args.relay_window_slots,
        relay_min_lovelace: args.relay_min_lovelace,
        project_side: &project_side,
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

        // Expire relay candidates that were never swept. Without this the map
        // grows for the length of the walk: every bare address a watched party
        // ever paid would be held forever, which is the frontier explosion in
        // a different container.
        if ctx.follow_relays && in_range.is_multiple_of(2_000) {
            state
                .relays
                .evict_before(slot, ctx.relay_window_slots.saturating_mul(2));
        }

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
    // Per-class breakdown, so a CIP-68 collection's numbers read in the terms
    // a person thinks in: "2,031 of 2,055" is really "1,015 of ~1,027 pairs",
    // and half the raw count being reference tokens is the STANDARD, not a
    // burn mystery. The floor test itself stays set-based over every asset
    // name — the indexer counts names too, so that comparison is exact.
    let mut by_class: std::collections::BTreeMap<&'static str, u64> =
        std::collections::BTreeMap::new();
    for name in ctx.minted.iter() {
        *by_class.entry(AssetClass::of(name).as_str()).or_insert(0) += 1;
    }
    let classes = by_class
        .iter()
        .map(|(k, v)| format!("{v} {k}"))
        .collect::<Vec<_>>()
        .join(" + ");
    let basis = match expected_assets {
        Some(exp) if exp == minted_n => "observed",
        Some(exp) => {
            tracing::warn!(
                expected = exp,
                minted = minted_n,
                classes,
                "walk: FLOOR TEST FAILED — the walk did not see every asset the policy minted; \
                 the floor is wrong, the walk stopped early, or the collection was STILL \
                 MINTING at the snapshot tip (expected counts every asset name the indexer \
                 knows, reference tokens included)"
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
        classes,
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
///
/// ## The tempting fix that is WRONG, and why
///
/// This rule uses *received an asset* as a proxy for *is a customer*, and the
/// proxy fails on the case the tool exists to find: **a project paying people
/// in assets**. Mekka's founder did that for a period, and every recipient of
/// the arrangement looked identical to a buyer and was refused forever.
///
/// The obvious repair is "a customer is someone who PAID — so refuse only an
/// asset receiver who also FUNDED this transaction". **Do not do this.** Nearly
/// all Cardano CNFT trades are NON-ATOMIC: two transactions a few minutes
/// apart, one sending the asset and the other the ADA. So an ordinary buyer
/// does not fund the transaction that delivers their asset, and that rule would
/// promote essentially every customer — thousands of them — as though each had
/// been paid in kind. It inverts the very judgement it was meant to sharpen.
///
/// Within-transaction funding only identifies a buyer in an ATOMIC swap, which
/// is the rare shape here, not the common one.
///
/// Telling a purchase from payment-in-kind therefore needs the COUNTER-PAYMENT:
/// a matching value transfer between the same two parties, in either direction,
/// within a short window. That is not decidable while streaming forward — the
/// counter-payment may not have happened yet — so it belongs in a post-pass
/// over the finished ledger, whose findings can then seed the next walk.
fn may_promote(asset_receivers: &BTreeSet<&str>, to_key: &str, is_member: bool) -> bool {
    is_member || !asset_receivers.contains(to_key)
}

/// The parties that took DELIVERY of the collection in this transaction — the
/// buyers, whose outputs are therefore change and minAda rather than payment.
///
/// This is the exclusion the mint fund-split rests on, and it must ask for a
/// HOLDING (`AssetClass::is_holding`) rather than for any policy asset. The
/// thing being excluded is *the buyer*, and only a buyer takes delivery of a
/// user token. A CIP-68 reference token goes to the PROJECT's own metadata
/// address, so a party known only by that receipt is not a buyer, and lovelace
/// landing on it is revenue.
///
/// **Measured on Octaverse (`6817db27…`)**: for the mint's first two days the
/// treasury address and the reference-token address were different payment
/// credentials under ONE stake key — and the stake key IS the party key. An
/// any-policy-asset form therefore put **22,722.61 ₳ of mint proceeds behind the
/// buyer-change exclusion** and reported the take as 24,081.47 ₳: 48.5% of the
/// money gone, with no warning. Asking for a holding restores it, and cannot
/// re-admit change — the reference token's own minAda does now book as a
/// payment, but that carrier ADA is the project's, not the buyer's.
fn delivery_parties<'a>(outs: &'a [Out<'a>]) -> BTreeSet<&'a str> {
    outs.iter()
        .filter(|o| {
            o.policy_assets
                .iter()
                .any(|(n, _)| AssetClass::of(n).is_holding())
        })
        .map(|o| o.resolved.party.key.as_str())
        .collect()
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
    /// Seat collection holders as watched-but-never-expanding parties
    /// (`Role::Holder`). See `WalkArgs::watch_holders`.
    pub watch_holders: bool,
    /// Follow money one hop through single-use bare addresses. See `relay_hop`.
    pub follow_relays: bool,
    /// How long a bare address may hold the money and still read as a relay.
    pub relay_window_slots: u64,
    /// Ignore bare outputs below this — a relay carries a payment, not dust.
    pub relay_min_lovelace: u64,
    /// Wallets the PROJECT OWNS (`party.project_side`), asserted at seed.
    ///
    /// Foreign-policy assets landing on one of these are the return leg of a
    /// deployment and get recorded to `asset_inflow`. Empty is the normal
    /// case and costs nothing — the whole block is skipped.
    pub project_side: &'a BTreeSet<String>,
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
    /// `(stake key, first-seen slot)` of collection holders — persisted to the
    /// KEPT `discovered_holder` table so the next pass can seat them from the
    /// floor.
    pub holders: Vec<(String, u64)>,
    /// Confirmed pass-throughs — a watched party's money seen leaving the bare
    /// address it was paid into. See the `relay_hop` table.
    pub relays: Vec<RelayHopRow>,
    /// Foreign-policy assets arriving at a project-owned wallet — the return
    /// leg. See the `asset_inflow` table.
    pub inflows: Vec<AssetInflowRow>,
}

impl Rows {
    pub fn flush(&mut self, ledger: &mut Ledger) -> Result<usize> {
        let n = ledger.insert_asset_events(&self.assets)?
            + ledger.insert_mint_payments(&self.mint_payments)?
            + ledger.insert_aliases(&self.aliases)?
            + ledger.insert_tx_deltas(&self.deltas)?
            + ledger.insert_value_events(&self.values)?
            + ledger.insert_unit_flows(&self.units)?
            + ledger.insert_relay_hops(&self.relays)?
            + ledger.insert_asset_inflows(&self.inflows)?
            + ledger.put_discovered_holders(&self.holders)?;
        self.assets.clear();
        self.mint_payments.clear();
        self.aliases.clear();
        self.deltas.clear();
        self.values.clear();
        self.units.clear();
        self.relays.clear();
        self.inflows.clear();
        self.holders.clear();
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
    /// Protocol floor for this output; 0 when it carries no assets. Booked onto
    /// the `lovelace` flow row so a reader can tell carrier ADA from payment —
    /// see `DecodedOutput::min_utxo`.
    min_utxo: u64,
}

/// Seat the receiver of a holder-facing collection asset as a HOLDER —
/// watched, never expanding — and record the discovery in the kept table.
///
/// Stake-keyed receivers only. A stakeless receiver is an off-ramp shape (and
/// often a script); a marketplace escrow with the seller's staking credential
/// resolves to the seller's stake key, which is the right wallet to watch
/// anyway. Reference tokens and labelled fungibles never seat anyone.
fn seat_holder(name: &[u8], o: &Out<'_>, state: &mut WalkState, ctx: &TxCtx<'_>, rows: &mut Rows) {
    if !ctx.watch_holders
        || !AssetClass::of(name).is_holding()
        || !o.resolved.party.has_stake_credential
    {
        return;
    }
    state
        .frontier
        .seed_holder(o.resolved.party.clone(), ctx.slot);
    rows.holders.push((o.resolved.party.key.clone(), ctx.slot));
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
                min_utxo: o.min_utxo,
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
                    seat_holder(name, o, state, ctx, rows);
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

    // ── the balance-sheet side: what came BACK ───────────────────────────
    //
    // The walk records every unit of value but only THIS policy's assets, so
    // a deployment paid in ADA and returned in another project's NFTs has its
    // departure captured and its arrival nowhere. Read without the return
    // leg, an honest allocation is indistinguishable from an extraction —
    // measured on Octaverse, where 6,000 ₳ left the treasury and 62 Mekka S2
    // came back to the project's holding wallet inside 35 minutes, and the
    // ledger held only the outflow.
    //
    // Bounded to wallets the project OWNS. Recording every asset for every
    // watched party would be the frontier explosion in a new dimension; a
    // curated handful of project wallets costs nothing, and the empty case
    // (no project_side declared) skips entirely.
    if !ctx.project_side.is_empty() {
        for o in &outs {
            if !ctx.project_side.contains(o.resolved.party.key.as_str()) {
                continue;
            }
            for (policy, name, qty) in &o.bundle {
                // THIS policy's assets already have a home in `asset_event`;
                // recording them here too would double-count the collection
                // against its own supply.
                if policy.as_slice() == ctx.policy.as_slice() {
                    continue;
                }
                rows.inflows.push(AssetInflowRow {
                    party: o.resolved.party.key.clone(),
                    policy_id: hex::encode(policy),
                    asset_name: hex::encode(name),
                    quantity: *qty as i64,
                    // The sender needs resolved inputs, which this point in
                    // the walk does not have. The pairing rule is "assets
                    // arrived inside the window after value left", and that
                    // needs no payer — so leave it null rather than guess.
                    from_party: None,
                    tx_hash: tx_hex.clone(),
                    slot: ctx.slot,
                    block_time: ctx.time as i64,
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
    // The test needs no input resolution: an output to a party that took no
    // DELIVERY in this tx is money going somewhere. See `delivery_parties`.
    if !mints.is_empty() {
        let asset_parties = delivery_parties(&outs);

        // Carries the `Party`, not just its key: the destination has to be
        // seeded below, and re-deriving a party from a key string is not
        // possible — the key IS the derived form.
        let mut by_dest: BTreeMap<&str, (u64, &Party)> = BTreeMap::new();
        for o in &outs {
            if !asset_parties.contains(o.resolved.party.key.as_str()) {
                let e = by_dest
                    .entry(o.resolved.party.key.as_str())
                    .or_insert((0, &o.resolved.party));
                e.0 += o.lovelace;
            }
        }
        for (dest, (lovelace, dest_party)) in by_dest {
            rows.mint_payments.push(MintPaymentRow {
                tx_hash: tx_hex.clone(),
                destination: dest.to_owned(),
                lovelace: lovelace as i64,
                slot: ctx.slot,
                block_time: ctx.time,
            });
            // SEAT THE DESTINATION. Nothing else can.
            //
            // The frontier only grows along OUTBOUND edges from a member, and
            // the payer here is the buyer — a stranger. So no promotion path
            // ever reaches the wallets the mint money lands in: measured on
            // Mekka, 2 of 3 mint destinations had no `party` row, one of them
            // holding 23,092 ₳ across 126 payments. Unseated means undrawable,
            // and what cannot be drawn is the project's capital coming IN.
            //
            // An earlier attempt seeded these as an ordinary role and took the
            // frontier from 64 parties to 2,826, because seeded roles are
            // exempt from the terminal rule and a mint's payees include the
            // platform's custodial fee wallet. `Role::MintPayee` exists for
            // exactly this: seeded (so it has a row) but NOT exempt (so scale
            // still freezes it). An artist wallet expands; a platform wallet is
            // recorded and frozen.
            //
            // `seed` is idempotent and keeps the lowest slot, so re-seeing a
            // destination across 700 mint txs costs a map lookup. A destination
            // already watched keeps the role it has — this never demotes a
            // declared treasury to a payee.
            if !state.frontier.is_member(dest_party) {
                tracing::info!(party = %dest, lovelace, slot = ctx.slot, "walk: mint payee seated");
                state
                    .frontier
                    .seed((*dest_party).clone(), Role::MintPayee, ctx.slot)
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
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
            let to = o.resolved.party.key.clone();
            let prev = state.holders.set(name, &to, ctx.slot);
            seat_holder(name, o, state, ctx, rows);
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

    // 3b. RELAY SWEEP — a bare address we paid is spending what we sent it.
    //
    // Handled before the `touches` gate because a relay is by construction NOT
    // a watched party: nothing here would otherwise make the walk look at this
    // transaction, which is precisely why the trail used to stop one hop short.
    let taken: Vec<RelayCandidate> = d
        .inputs
        .iter()
        .filter_map(|i| state.relays.take(&i.oref))
        .collect();
    // A relay is used ONCE. If this address has been paid more than once by
    // the watch set it is a wallet or a service, and the sweep is just what
    // wallets do — see `Relays::seen`.
    let swept: Vec<RelayCandidate> = taken
        .into_iter()
        .filter(|c| state.relays.is_single_use(&c.address))
        .collect();
    if !swept.is_empty() {
        record_relay_hops(&swept, &outs, ctx, &tx_hex, rows);
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
    // WRITE DOWN WHAT WE COULD NOT RESOLVE.
    //
    // An unresolved input is not merely a missing number: it disables the
    // change rule below, because a wallet's own output coming back can only be
    // recognised as change if we know that wallet funded the tx. Every ref we
    // fail on is therefore a receipt that may be nothing of the kind, and the
    // only way to settle it later is to know which ref to go and fetch.
    // `resolve-local` reads this list straight back out of the snapshot.
    let mut wanted: Vec<(OutRef, u64)> = Vec::new();
    for oref in &missing {
        match found.get(oref) {
            Some(c) => {
                let r = resolve_str(&c.address);
                inputs.push((r.party, c.lovelace, r.stake_cred));
            }
            None => {
                unresolved += 1;
                wanted.push((*oref, ctx.slot));
            }
        }
    }
    ledger.wanted_put(&wanted)?;

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

    // Arm relay candidates: a member paid a bare KEY address that is not
    // itself watched.
    //
    // Three exclusions, each of which a first run proved necessary:
    //
    // - STAKE-KEYED. A wallet with a staking credential is somebody's actual
    //   wallet — they want their delegation — so its receipts are holdings to
    //   reason about, not a conduit. Exchange deposit addresses are bare by
    //   construction.
    // - SCRIPT payment credentials. A script that takes money and passes it on
    //   within minutes is a CONTRACT — a DEX batcher, an order, an escrow —
    //   and following it as though it were a deposit address turns routine
    //   swap plumbing into "the treasury's money went here". The Minswap
    //   batcher alone drew 5,892 hops before this guard. `classify` refuses
    //   scripts in the off-ramp rule for exactly this reason; the same trap
    //   applies one hop further out.
    // - BYRON addresses. Legacy addresses carry no staking credential AT ALL,
    //   so "stakeless" says nothing about them — they are ordinary old
    //   wallets, and 54 were caught before this guard.
    if !ctx.follow_relays {
        return Ok(());
    }
    let payer = dominant_member_payer(&inputs, state);
    if let Some(from_party) = payer {
        for o in &outs {
            if o.resolved.party.has_stake_credential
                || o.resolved.payment_is_script
                || !o.resolved.party.key.starts_with("addr")
                || state.frontier.is_member(&o.resolved.party)
                || o.lovelace < ctx.relay_min_lovelace
            {
                continue;
            }
            state.relays.arm(
                (d.tx_hash, o.idx),
                RelayCandidate {
                    address: d.outputs[o.idx as usize].address.clone(),
                    from_party: from_party.clone(),
                    lovelace: o.lovelace,
                    tx: tx_hex.clone(),
                    slot: ctx.slot,
                },
            );
        }
    }
    Ok(())
}

/// The watched party that put the most lovelace into this transaction, if any.
///
/// A relay hop is only worth recording when we can name whose money it was.
/// Where several members funded a tx the largest is attributed, matching the
/// `payers` judgement `book_unit_flows` already makes.
fn dominant_member_payer(
    inputs: &[(Party, u64, Option<[u8; 28]>)],
    state: &WalkState,
) -> Option<String> {
    let mut best: Option<(&str, u64)> = None;
    for (p, lovelace, _) in inputs {
        if !state.frontier.is_member(p) {
            continue;
        }
        match best {
            Some((_, v)) if v >= *lovelace => {}
            _ => best = Some((p.key.as_str(), *lovelace)),
        }
    }
    best.map(|(k, _)| k.to_owned())
}

/// Book where a swept relay's money actually went.
///
/// Change back to the relay itself is skipped: an address that keeps a slice
/// is still passing the rest on, and the destination is the finding. Outputs
/// are recorded individually rather than summed — a sweep that fans into two
/// destinations is two facts, not an average.
fn record_relay_hops(
    swept: &[RelayCandidate],
    outs: &[Out<'_>],
    ctx: &TxCtx<'_>,
    out_tx: &str,
    rows: &mut Rows,
) {
    for c in swept {
        // Dwell time is the whole signal. A bare address that sits on the
        // money for days is somebody's wallet; one that forwards within
        // minutes is plumbing.
        let dwell = ctx.slot.saturating_sub(c.slot);
        if dwell > ctx.relay_window_slots {
            tracing::debug!(
                relay = %c.address, dwell, "walk: bare address spent too late to read as a relay"
            );
            continue;
        }
        for o in outs {
            if o.resolved.party.key == c.address || o.lovelace == 0 {
                continue;
            }
            rows.relays.push(RelayHopRow {
                relay_addr: c.address.clone(),
                from_party: c.from_party.clone(),
                to_addr: o.resolved.party.key.clone(),
                unit: "lovelace".to_owned(),
                quantity: o.lovelace as i64,
                in_tx: c.tx.clone(),
                out_tx: out_tx.to_owned(),
                in_slot: c.slot,
                out_slot: ctx.slot,
            });
        }
        tracing::debug!(
            relay = %c.address, from = %c.from_party, dwell,
            ada = c.lovelace as f64 / 1e6,
            "walk: followed a relay hop"
        );
    }
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
            // The floor prices the OUTPUT, and it is its ADA that is pinned by
            // it — a token row carrying the same number would read as though
            // the token itself had a minimum, which is meaningless.
            let min_utxo = if unit == "lovelace" { o.min_utxo } else { 0 };
            if payer_watched {
                rows.units.push(UnitFlowRow {
                    tx_hash: tx_hex.to_string(),
                    output_index: o.idx,
                    party: payer_key.to_string(),
                    counterparty: to.key.clone(),
                    unit: unit.clone(),
                    quantity: -q,
                    payers,
                    min_utxo,
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
                    min_utxo,
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

    /// No project boundary declared — the default for every walk test, and the
    /// state an un-curated ledger is in. The return-leg capture is skipped
    /// entirely, so these tests exercise the walk unchanged.
    static NO_PROJECT_SIDE: std::sync::LazyLock<BTreeSet<String>> =
        std::sync::LazyLock::new(BTreeSet::new);
    use crate::activity::Activity;
    use crate::state::{Buffer, Holders, Relays};
    use chain_ledger::{Frontier, Thresholds};
    use pallas_primitives::Hash;
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
            relays: Relays::default(),
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
            watch_holders: true,
            follow_relays: true,
            relay_window_slots: 7200,
            relay_min_lovelace: 5_000_000,
            project_side: &NO_PROJECT_SIDE,
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
            relays: Relays::default(),
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
            watch_holders: true,
            follow_relays: true,
            relay_window_slots: 7200,
            relay_min_lovelace: 5_000_000,
            project_side: &NO_PROJECT_SIDE,
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
            min_utxo: 0,
        }
    }

    /// As `out`, but for an output pinned by the protocol floor — a token
    /// riding with the ADA that pays for its bytes.
    fn out_with_floor(
        idx: u32,
        to: &str,
        lovelace: u64,
        bundle: Vec<(Vec<u8>, Vec<u8>, u64)>,
        min_utxo: u64,
    ) -> Out<'static> {
        Out {
            min_utxo,
            ..out(idx, to, lovelace, bundle)
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
            relays: Relays::default(),
        }
    }

    // --- relay hops --------------------------------------------------------

    fn relay_ctx<'a>(
        slot: u64,
        signer_creds: &'a mut BTreeSet<[u8; 28]>,
        minted: &'a mut BTreeSet<Vec<u8>>,
    ) -> TxCtx<'a> {
        TxCtx {
            policy: [0xAB; 28],
            policy_hex: hex::encode([0xAB; 28]),
            slot,
            time: 1_700_000_000,
            signer_creds,
            minted,
            ceiling: None,
            royalty: None,
            royalty_rate: None,
            last_mint_slot: None,
            max_hops: 0,
            minted_holdings: BTreeSet::new(),
            watch_holders: true,
            follow_relays: true,
            relay_window_slots: 7200,
            relay_min_lovelace: 5_000_000,
            project_side: &NO_PROJECT_SIDE,
        }
    }

    fn candidate(slot: u64) -> RelayCandidate {
        candidate_at("addr1vrelay", slot)
    }

    fn candidate_at(address: &str, slot: u64) -> RelayCandidate {
        RelayCandidate {
            address: address.to_owned(),
            from_party: "stake1compounding".to_owned(),
            lovelace: 7_500_000_000,
            tx: "intx".to_owned(),
            slot,
        }
    }

    /// The case this exists for: the trail must name the SWEEP TARGET, not
    /// stop at the bare address in the middle.
    #[test]
    fn a_swept_relay_names_where_the_money_actually_went() {
        let mut sc = BTreeSet::new();
        let mut m = BTreeSet::new();
        let ctx = relay_ctx(1_600, &mut sc, &mut m);
        let outs = [out(0, "addr1vswaphotwallet", 7_499_300_000, vec![])];
        let mut rows = Rows::default();
        record_relay_hops(&[candidate(1_000)], &outs, &ctx, "outtx", &mut rows);

        assert_eq!(rows.relays.len(), 1);
        let r = &rows.relays[0];
        assert_eq!(r.to_addr, "addr1vswaphotwallet");
        assert_eq!(r.from_party, "stake1compounding");
        assert_eq!(r.relay_addr, "addr1vrelay");
        assert_eq!(r.in_tx, "intx");
        assert_eq!(r.out_tx, "outtx");
        assert_eq!(
            r.quantity, 7_499_300_000,
            "the row carries what was SWEPT ONWARD, not what was received"
        );
    }

    /// Dwell time is the discriminator. An address that sits on the money for
    /// days is somebody's wallet, and calling it a conduit would put words in
    /// the chain's mouth.
    #[test]
    fn a_bare_address_that_holds_the_money_too_long_is_not_a_relay() {
        let mut sc = BTreeSet::new();
        let mut m = BTreeSet::new();
        let ctx = relay_ctx(1_000 + 7_201, &mut sc, &mut m);
        let outs = [out(0, "addr1vswaphotwallet", 7_499_300_000, vec![])];
        let mut rows = Rows::default();
        record_relay_hops(&[candidate(1_000)], &outs, &ctx, "outtx", &mut rows);
        assert!(rows.relays.is_empty());
    }

    /// A relay that keeps a slice still passes the rest on; the change output
    /// back to itself is not a destination.
    #[test]
    fn change_back_to_the_relay_is_not_a_destination() {
        let mut sc = BTreeSet::new();
        let mut m = BTreeSet::new();
        let ctx = relay_ctx(1_100, &mut sc, &mut m);
        let outs = [
            out(0, "addr1vswaphotwallet", 7_000_000_000, vec![]),
            out(1, "addr1vrelay", 499_300_000, vec![]),
        ];
        let mut rows = Rows::default();
        record_relay_hops(&[candidate(1_000)], &outs, &ctx, "outtx", &mut rows);
        assert_eq!(rows.relays.len(), 1);
        assert_eq!(rows.relays[0].to_addr, "addr1vswaphotwallet");
    }

    /// A sweep that fans out is several facts, not an average of them.
    #[test]
    fn a_fan_out_sweep_records_every_destination() {
        let mut sc = BTreeSet::new();
        let mut m = BTreeSet::new();
        let ctx = relay_ctx(1_100, &mut sc, &mut m);
        let outs = [
            out(0, "addr1vfirst", 4_000_000_000, vec![]),
            out(1, "addr1vsecond", 3_499_300_000, vec![]),
        ];
        let mut rows = Rows::default();
        record_relay_hops(&[candidate(1_000)], &outs, &ctx, "outtx", &mut rows);
        assert_eq!(rows.relays.len(), 2);
    }

    #[test]
    fn the_largest_watched_funder_is_the_one_attributed() {
        let state = seeded(&["stake1treasury", "stake1compounding"]);
        let inputs = [
            (party("stake1treasury"), 10_000_000, None),
            (party("stake1compounding"), 900_000_000, None),
            (party("stake1stranger"), 5_000_000_000, None),
        ];
        assert_eq!(
            dominant_member_payer(&inputs, &state).as_deref(),
            Some("stake1compounding"),
            "the biggest input is unwatched — attribution follows the biggest WATCHED one"
        );
    }

    #[test]
    fn a_tx_no_watched_party_funded_arms_no_relay() {
        let state = seeded(&["stake1treasury"]);
        let inputs = [(party("stake1stranger"), 5_000_000_000, None)];
        assert!(dominant_member_payer(&inputs, &state).is_none());
    }

    /// THE regression that matters. A first run over Mekka S1 without the
    /// single-use test produced 160,589 hops across 1,338 "relays" totalling
    /// 70.7 billion ada — more than the whole supply — because busy service
    /// wallets satisfy "stakeless and spent quickly" all day long. One was
    /// armed 88,282 times.
    #[test]
    fn an_address_armed_twice_is_a_wallet_not_a_relay() {
        let mut r = Relays::default();
        assert!(
            r.arm((Hash::new([1u8; 32]), 0), candidate(1_000)),
            "first sighting is a plausible throwaway"
        );
        assert!(r.is_single_use("addr1vrelay"));

        assert!(
            !r.arm((Hash::new([2u8; 32]), 0), candidate(2_000)),
            "second sighting disqualifies it"
        );
        assert!(
            !r.is_single_use("addr1vrelay"),
            "and it stays disqualified — a hot wallet must never record a hop"
        );
    }

    /// The bound that stops this becoming a second frontier.
    #[test]
    fn unswept_candidates_are_evicted() {
        let mut r = Relays::default();
        // Distinct addresses: two sightings of the SAME one would (correctly)
        // be refused by the single-use rule, which is a different test.
        r.insert(
            (Hash::new([1u8; 32]), 0),
            candidate_at("addr1vstale", 1_000),
        );
        r.insert(
            (Hash::new([2u8; 32]), 0),
            candidate_at("addr1vfresh", 9_000),
        );
        r.evict_before(10_000, 7_200);
        assert_eq!(r.len(), 1, "the stale candidate is dropped, the fresh kept");
        assert!(r.contains(&(Hash::new([2u8; 32]), 0)));
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
            watch_holders: true,
            follow_relays: true,
            relay_window_slots: 7200,
            relay_min_lovelace: 5_000_000,
            project_side: &NO_PROJECT_SIDE,
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
            watch_holders: true,
            follow_relays: true,
            relay_window_slots: 7200,
            relay_min_lovelace: 5_000_000,
            project_side: &NO_PROJECT_SIDE,
        };
        let mut rows = Rows::default();
        book_unit_flows(outs, inputs, &state, &ctx, "txhash", 0, &mut rows);
        rows.units
    }

    /// The queued-mint gap: on tx `ff4d4914…` the buyer received 10 S2 NFTs
    /// without funding the mint tx at all (payment was an earlier tx), so no
    /// money edge could ever seat them. Receiving a HOLDER-FACING asset now
    /// seats the wallet — watched, never expanding — while reference-token
    /// plumbing seats nobody and an expanding member is never downgraded.
    #[test]
    fn receiving_a_user_token_seats_a_holder_but_a_reference_seats_nobody() {
        let mut state = seeded(&["treasury"]);
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
            watch_holders: true,
            follow_relays: true,
            relay_window_slots: 7200,
            relay_min_lovelace: 5_000_000,
            project_side: &NO_PROJECT_SIDE,
        };
        let mut rows = Rows::default();
        let user_token = hex::decode("000de1404d4430303031").unwrap();
        let reference = hex::decode("000643b04d4430303031").unwrap();

        // The buyer receives the user token: seated, watched, recruits nobody.
        seat_holder(
            &user_token,
            &out(0, "buyer", 1_500_000, vec![]),
            &mut state,
            &ctx,
            &mut rows,
        );
        let m = state.frontier.member(&party("buyer")).expect("seated");
        assert_eq!(m.role, Role::Holder);
        assert!(!state.frontier.expands(&party("buyer")));
        assert_eq!(rows.holders, vec![("buyer".to_string(), 7)]);

        // The metadata script receives the reference token: nobody is seated.
        seat_holder(
            &reference,
            &out(1, "meta-script", 1_310_240, vec![]),
            &mut state,
            &ctx,
            &mut rows,
        );
        assert!(!state.frontier.is_member(&party("meta-script")));

        // The treasury buying its own asset stays Declared and expanding.
        seat_holder(
            &user_token,
            &out(2, "treasury", 1_500_000, vec![]),
            &mut state,
            &ctx,
            &mut rows,
        );
        let t = state.frontier.member(&party("treasury")).unwrap();
        assert_eq!(t.role, Role::Declared);
        assert!(state.frontier.expands(&party("treasury")));
    }

    /// The Octaverse regression: a CIP-68 project whose treasury address and
    /// reference-token address share ONE stake key — which is the party key.
    ///
    /// Excluding every party that touched a policy asset hid 48.5% of that
    /// mint's proceeds behind the buyer-change rule. Only DELIVERY of a
    /// holder-facing token marks a buyer.
    #[test]
    fn a_reference_token_receipt_does_not_make_the_project_a_buyer() {
        let user_token = hex::decode("000de1404d4430303031").unwrap();
        let reference = hex::decode("000643b04d4430303031").unwrap();

        let mut buyer = out(0, "buyer", 1_194_000, vec![]);
        buyer.policy_assets = vec![(user_token, 1)];
        // Same party as the treasury output below — one stake key, two payment
        // credentials — holding only the metadata token.
        let mut meta = out(1, "project", 1_340_000, vec![]);
        meta.policy_assets = vec![(reference, 1)];
        let treasury = out(2, "project", 147_196_000, vec![]);

        let outs = [buyer, meta, treasury];
        let delivered = delivery_parties(&outs);
        assert!(
            delivered.contains("buyer"),
            "the user token marks its recipient as a buyer"
        );
        assert!(
            !delivered.contains("project"),
            "a reference token is metadata plumbing, not delivery — so the \
             147.196 ₳ on the same party is revenue, not change"
        );
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

    /// A token cannot sit on chain without ADA to pay for its bytes, and that
    /// ADA is a CARRIER, not a payment — booking it as value is how a mint's
    /// asset distribution becomes tens of thousands of meaningless ADA flows.
    ///
    /// The floor is recorded, never deducted: an output can hold a token AND
    /// real value, and only a reader can decide the threshold. So the ADA leg
    /// keeps its full quantity and gains the floor beside it.
    #[test]
    fn the_protocol_floor_rides_on_the_ada_leg_only() {
        let rows = flows(
            &[out_with_floor(
                0,
                "stake1buyer",
                1_262_830, // a bare NFT output: essentially all floor
                vec![(USDM.to_vec(), b"NFT1".to_vec(), 1)],
                1_262_830,
            )],
            &[(party("stake1treasury"), 900_000_000, None)],
            &["stake1treasury"],
        );
        let ada = rows.iter().find(|r| r.unit == "lovelace").unwrap();
        assert_eq!(ada.quantity, -1_262_830, "the quantity is NOT reduced");
        assert_eq!(ada.min_utxo, 1_262_830, "and the floor sits beside it");
        assert!(
            ada.quantity.unsigned_abs() <= ada.min_utxo,
            "at the floor = pure carrier, which is what a reader filters on"
        );
        let token = rows.iter().find(|r| r.unit != "lovelace").unwrap();
        assert_eq!(
            token.min_utxo, 0,
            "a token has no floor of its own — only the output's ADA is pinned"
        );
    }

    /// The case a blanket "ignore ADA on token outputs" rule gets wrong, and on
    /// Mekka it would have got it wrong 93.6% of the time: real value moving in
    /// the same output as a token.
    #[test]
    fn value_above_the_floor_survives() {
        let rows = flows(
            &[out_with_floor(
                0,
                "stake1artist",
                500_000_000,
                vec![(USDM.to_vec(), b"NFT1".to_vec(), 1)],
                1_262_830,
            )],
            &[(party("stake1treasury"), 900_000_000, None)],
            &["stake1treasury"],
        );
        let ada = rows.iter().find(|r| r.unit == "lovelace").unwrap();
        assert_eq!(ada.quantity, -500_000_000);
        assert!(
            ada.quantity.unsigned_abs() > ada.min_utxo,
            "500 ADA with an NFT attached is a payment, not dust"
        );
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
