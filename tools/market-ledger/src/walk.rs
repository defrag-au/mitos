//! `walk` — iterate certified immutable-DB history, decode each block, and drive
//! the outref buffer + `DecodeTx` assembly into the marketplace decoders, then
//! append the decoded events to the sqlite ledger.
//!
//! Per tx: buffer produced watched outputs (resolving their datum locally), take
//! any spent watched outputs back out of the buffer to build resolved inputs,
//! assemble one `DecodeTx`, dispatch the crate decoders for each venue the tx
//! touched, and map the events to `market_events` rows. Per checkpoint: persist
//! the open-book buffer + per-venue cursor so a resume reloads the book and
//! never re-walks.

use std::collections::BTreeSet;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use mitos_marketplace_decode::{
    AssetId, DecodeTx, OutputDatum, TxInput, TxOutput, decode_jpg_listings,
    decode_jpg_offer_lifecycle, decode_jpg_sales, decode_listing_datum, decode_wayup_listings,
    decode_wayup_offer_lifecycle, decode_wayup_sales,
};
use pallas_hardano::storage::immutable::{self, FallibleBlock, Point};
use pallas_primitives::Hash;
use pallas_traverse::MultiEraBlock;

use crate::buffer::{BufferedOutput, OutrefBuffer};
use crate::checkpoint::{self, CheckpointFile};
use crate::decode::{Asset, DecodedOutput, DecodedTx, decode_tx};
use crate::metadata;
use crate::row::{self, BlockCtx, MarketEventRow};
use crate::store::{Ledger, Listing, ListingOp};
use crate::venue::{Channel, VenueDecoder, VenueRegistry};

#[derive(clap::Args, Debug)]
pub struct WalkArgs {
    /// Data dir holding the immutable DB (expects `<data-dir>/immutable`).
    #[arg(long)]
    data_dir: PathBuf,

    /// Ledger sqlite path.
    #[arg(long, default_value = "market-ledger.db")]
    db: PathBuf,

    /// Venue registry TOML.
    #[arg(long, default_value = "venues.toml")]
    venues: PathBuf,

    /// Comma-separated venues to enable (default: every venue in the registry).
    #[arg(long, value_delimiter = ',')]
    venue: Vec<String>,

    /// Start slot (default: the resume cursor, else the lowest venue floor).
    #[arg(long)]
    from_slot: Option<u64>,

    /// Seek the immutable DB directly to `<slot>:<block_hash_hex>` instead of
    /// streaming from genesis — skips the pre-floor decode for controlled tests.
    /// The outref buffer starts cold, so sales/accepts of listings/offers created
    /// BEFORE this point won't resolve; pick a point early enough to cover them.
    #[arg(long)]
    from_point: Option<String>,

    /// Ignore any saved cursor/buffer and walk from the floor.
    #[arg(long)]
    fresh: bool,

    /// Checkpoint (persist buffer + cursor) every N in-range blocks.
    #[arg(long, default_value_t = 50_000)]
    checkpoint_every: u64,

    /// Stop after this many in-range blocks (0 = no limit) — for smoke tests.
    #[arg(long, default_value_t = 0)]
    max_blocks: u64,

    /// Crash-visible progress mirror written at each checkpoint (default:
    /// `<db>.checkpoint.json`).
    #[arg(long)]
    checkpoint_file: Option<PathBuf>,

    /// If this file exists at startup, wipe the ledger + checkpoint and restart
    /// fresh, then consume the flag — for automated clean restarts.
    #[arg(long)]
    reset_flag: Option<PathBuf>,

    /// When resetting via `--reset-flag`, also delete this Parquet tree.
    #[arg(long)]
    reset_parquet: Option<PathBuf>,
}

pub fn run(args: WalkArgs) -> Result<()> {
    let registry = VenueRegistry::load(&args.venues, &args.venue)?;
    let immutable_dir = args.data_dir.join("immutable");
    if !immutable_dir.is_dir() {
        bail!(
            "immutable DB not found at {} — run `market-ledger bootstrap` first",
            immutable_dir.display()
        );
    }

    let enabled: Vec<String> = registry.venue_names().map(str::to_owned).collect();
    let enabled_refs: Vec<&str> = enabled.iter().map(String::as_str).collect();

    let checkpoint_path = args
        .checkpoint_file
        .clone()
        .unwrap_or_else(|| checkpoint::default_path(&args.db));

    // Reset flag: if present, wipe the ledger + checkpoint (+ optional Parquet)
    // and consume the flag, so the walk below opens a fresh ledger.
    if let Some(flag) = &args.reset_flag
        && flag.exists()
    {
        tracing::warn!(flag = %flag.display(), "walk: reset flag present — wiping ledger");
        let removed = checkpoint::wipe(&args.db, &checkpoint_path, args.reset_parquet.as_deref())?;
        for f in &removed {
            tracing::info!(path = %f.display(), "reset: removed");
        }
        std::fs::remove_file(flag)
            .with_context(|| format!("consuming reset flag {}", flag.display()))?;
    }

    let mut ledger = Ledger::open(&args.db)?;
    let mut buffer = if args.fresh {
        OutrefBuffer::default()
    } else {
        ledger.load_buffer()?
    };
    let resume = if args.fresh {
        None
    } else {
        ledger.min_cursor_slot(&enabled_refs)?
    };

    // `--from-point <slot>:<hash>` seeks the reader; its slot is also the floor.
    let from_point = args.from_point.as_deref().map(parse_point).transpose()?;
    let point_slot = from_point.as_ref().map(|(slot, _)| *slot);
    let floor = point_slot
        .or(args.from_slot)
        .or(resume)
        .unwrap_or_else(|| registry.min_earliest_slot());

    if from_point.is_some() && buffer.is_empty() {
        tracing::warn!(
            "walk: --from-point with a cold buffer — sales/accepts/delists of \
             listings created before the point won't resolve"
        );
    }

    tracing::info!(
        venues = ?enabled,
        floor,
        seek = from_point.is_some(),
        resumed = resume.is_some(),
        open_book = buffer.len(),
        dir = %immutable_dir.display(),
        db = %args.db.display(),
        "walk: starting"
    );

    // Seek to the point (efficient) or stream from genesis.
    let blocks: Box<dyn Iterator<Item = FallibleBlock>> = match from_point {
        Some((slot, hash)) => {
            immutable::read_blocks_from_point(&immutable_dir, Point::Specific(slot, hash))
                .map_err(|e| anyhow::anyhow!("seeking immutable DB to point: {e:?}"))?
        }
        None => Box::new(immutable::read_blocks(&immutable_dir).map_err(|e| {
            anyhow::anyhow!("opening immutable DB at {}: {e:?}", immutable_dir.display())
        })?),
    };

    let mut scanned: u64 = 0;
    let mut in_range: u64 = 0;
    let mut inserted: u64 = 0;
    let mut last_slot: u64 = 0;
    let mut last_hash: Option<Hash<32>> = None;
    let mut rows: Vec<MarketEventRow> = Vec::new();
    let mut listing_ops: Vec<ListingOp> = Vec::new();

    for block in blocks {
        let bytes = block.map_err(|e| anyhow::anyhow!("reading block from chunk: {e:?}"))?;
        let blk = MultiEraBlock::decode(&bytes)
            .map_err(|e| anyhow::anyhow!("decoding block at ~#{scanned}: {e:?}"))?;
        scanned += 1;
        let slot = blk.slot();
        last_slot = slot;
        last_hash = Some(blk.hash());

        if slot < floor {
            if scanned.is_multiple_of(500_000) {
                tracing::info!(scanned, slot, "walk: skipping toward floor");
            }
            continue;
        }
        in_range += 1;

        let ctx = BlockCtx {
            slot,
            height: Some(blk.number()),
            time: slot_to_unix(slot),
        };
        for tx in blk.txs() {
            process_tx(
                decode_tx(&tx),
                &registry,
                &mut buffer,
                &ctx,
                &mut rows,
                &mut listing_ops,
            );
        }
        inserted += ledger.insert_events(&rows)? as u64;
        ledger.apply_listing_ops(&listing_ops)?;
        rows.clear();
        listing_ops.clear();

        if in_range.is_multiple_of(args.checkpoint_every.max(1)) {
            let hash = blk.hash();
            ledger.checkpoint(&buffer, &enabled_refs, slot, hash.as_ref())?;
            checkpoint::write(
                &checkpoint_path,
                &CheckpointFile {
                    last_slot: slot,
                    last_block_height: Some(blk.number()),
                    last_block_hash: hex::encode(hash.as_ref()),
                    scanned_blocks: scanned,
                    in_range_blocks: in_range,
                    inserted_rows: inserted,
                    open_book: buffer.len(),
                    venues: enabled.clone(),
                    updated_unix: checkpoint::now_unix(),
                    done: false,
                },
            )?;
            tracing::info!(
                scanned,
                in_range,
                slot,
                inserted,
                open_book = buffer.len(),
                "walk: checkpoint"
            );
        }
        if args.max_blocks != 0 && in_range >= args.max_blocks {
            tracing::info!(max_blocks = args.max_blocks, "walk: max-blocks reached");
            break;
        }
    }

    // Final flush + checkpoint (+ a `done` marker in the crash-visible file).
    // The REAL last block hash is persisted so `follow` can chainsync-intersect
    // at the cursor (an empty hash can't be intersected).
    let final_hash = last_hash.map(|h| h.as_ref().to_vec()).unwrap_or_default();
    if last_slot >= floor {
        ledger.checkpoint(&buffer, &enabled_refs, last_slot, &final_hash)?;
    }
    checkpoint::write(
        &checkpoint_path,
        &CheckpointFile {
            last_slot,
            last_block_height: None,
            last_block_hash: hex::encode(&final_hash),
            scanned_blocks: scanned,
            in_range_blocks: in_range,
            inserted_rows: inserted,
            open_book: buffer.len(),
            venues: enabled.clone(),
            updated_unix: checkpoint::now_unix(),
            done: true,
        },
    )?;
    tracing::info!(
        scanned,
        in_range,
        last_slot,
        inserted,
        open_book = buffer.len(),
        checkpoint = %checkpoint_path.display(),
        "walk: complete"
    );
    Ok(())
}

/// Shared with `follow` — the per-tx buffer/decode/row pipeline is identical
/// whether the block came from an immutable chunk or the chainsync tail.
pub(crate) fn process_tx(
    d: DecodedTx,
    registry: &VenueRegistry,
    buffer: &mut OutrefBuffer,
    ctx: &BlockCtx,
    rows: &mut Vec<MarketEventRow>,
    listing_ops: &mut Vec<ListingOp>,
) {
    let mut touched: BTreeSet<String> = BTreeSet::new();

    // 1. Resolve consumed watched inputs from the buffer (local resolution — no
    //    indexer call). A hash-only listing datum unresolved at produce time is
    //    resolved now from THIS (spending) tx's witnesses.
    let mut inputs: Vec<TxInput> = Vec::new();
    for inp in &d.inputs {
        if let Some(b) = buffer.take(&inp.oref) {
            touched.insert(b.venue.clone());
            // Listings maintenance: a spend of a Sale-channel (listing) UTxO
            // removes its listing row(s). Deletes precede this tx's upserts, so
            // a reprice (spend old + produce new) nets to the new listing.
            if registry.watch_for(&b.address).map(|w| w.channel) == Some(Channel::Sale) {
                for a in &b.assets {
                    listing_ops.push(ListingOp::Delete {
                        policy_id: hex::encode(&a.policy),
                        asset_name_hex: hex::encode(&a.name),
                    });
                }
            }
            let datum = b
                .datum_bytes
                .clone()
                .or_else(|| b.datum_hash.and_then(|h| d.witness_datums.get(&h).cloned()));
            inputs.push(TxInput {
                address: b.address,
                lovelace: b.lovelace,
                assets: b.assets.iter().map(asset_id).collect(),
                datum,
                redeemer: inp.redeemer.clone(),
                oref_tx_hash: inp.oref.0.as_ref().to_vec(),
                oref_index: inp.oref.1,
            });
        }
    }

    // 2. Buffer produced watched outputs, resolving each datum locally.
    for out in &d.outputs {
        if let Some(w) = registry.watch_for(&out.address) {
            touched.insert(w.venue.clone());
            let is_jpg = matches!(registry.decoder(&w.venue), Some(VenueDecoder::Jpg));
            let datum_bytes = resolve_produced_datum(out, &d, is_jpg);
            // Listings maintenance: a produced Sale-channel output is a
            // (re)listing — upsert one row per escrowed asset (bundles fan out),
            // priced from the resolved datum (None for unresolvable hash-only jpg).
            if w.channel == Channel::Sale {
                push_listing_upserts(
                    listing_ops,
                    &w.venue,
                    datum_bytes.as_deref(),
                    &out.assets,
                    &hex::encode(d.tx_hash.as_ref()),
                    out.index,
                    ctx.slot,
                    ctx.time as i64,
                );
            }
            buffer.insert(
                (d.tx_hash, out.index),
                BufferedOutput {
                    address: out.address.clone(),
                    lovelace: out.lovelace,
                    assets: out.assets.clone(),
                    datum_bytes,
                    datum_hash: out.datum_hash,
                    venue: w.venue.clone(),
                },
            );
        }
    }

    if touched.is_empty() {
        return;
    }

    // 3. Assemble the DecodeTx: all outputs (buyer / fee / produced listings +
    //    offers), the resolved watched inputs, and required signers.
    let outputs: Vec<TxOutput> = d
        .outputs
        .iter()
        .map(|o| build_output(o, registry, &d))
        .collect();
    let dtx = DecodeTx {
        tx_hash: d.tx_hash.as_ref().to_vec(),
        inputs,
        outputs,
        required_signers: d.required_signers.clone(),
    };

    // Listing decode resolves hash-only NEW-listing datums from this tx's
    // witnesses (local-first; a datum_cache / Koios fallback layers in later).
    let resolve = |hash: &[u8]| -> Option<Vec<u8>> {
        let arr: [u8; 32] = hash.try_into().ok()?;
        d.witness_datums.get(&Hash::from(arr)).cloned()
    };

    // 4. Dispatch each touched venue's decoders → market_events rows.
    for venue in &touched {
        match registry.decoder(venue) {
            Some(VenueDecoder::Jpg) => {
                for e in decode_jpg_sales(&dtx) {
                    rows.push(row::from_jpg_sale(&e, ctx, venue));
                }
                for e in decode_jpg_listings(&dtx, resolve) {
                    rows.push(row::from_jpg_listing(&e, ctx, venue));
                }
                for e in decode_jpg_offer_lifecycle(&dtx) {
                    rows.push(row::from_jpg_offer(&e, ctx, venue));
                }
            }
            Some(VenueDecoder::Wayup { sale, offer }) => {
                for e in decode_wayup_sales(&dtx, sale) {
                    rows.push(row::from_wayup_sale(&e, ctx, venue));
                }
                for e in decode_wayup_listings(&dtx, sale, resolve) {
                    rows.push(row::from_wayup_listing(&e, ctx, venue));
                }
                for e in decode_wayup_offer_lifecycle(&dtx, offer) {
                    rows.push(row::from_wayup_offer(&e, ctx, venue));
                }
            }
            None => {}
        }
    }
}

/// Build the listing upsert(s) for a produced Sale-channel output: one row per
/// escrowed asset (a bundle escrows several), priced by the payout sum (the
/// ask; the fee-fold to buyer-price happens at serve time). Price/seller are
/// `None` when the datum can't be decoded (unresolvable hash-only jpg).
#[allow(clippy::too_many_arguments)]
fn push_listing_upserts(
    ops: &mut Vec<ListingOp>,
    venue: &str,
    datum_bytes: Option<&[u8]>,
    assets: &[Asset],
    tx_hash_hex: &str,
    output_index: u32,
    slot: u64,
    time: i64,
) {
    let decoded = datum_bytes.and_then(decode_listing_datum);
    let price = decoded
        .as_ref()
        .map(|dl| dl.payouts.iter().map(|p| p.lovelace).sum::<u64>())
        .filter(|&p| p > 0);
    let seller = decoded
        .as_ref()
        .map(|dl| dl.cred_hex.clone())
        .filter(|s| !s.is_empty());
    for a in assets {
        ops.push(ListingOp::Upsert(Listing {
            policy_id: hex::encode(&a.policy),
            asset_name_hex: hex::encode(&a.name),
            venue: venue.to_string(),
            price_lovelace: price,
            seller_stake: seller.clone(),
            tx_hash: tx_hash_hex.to_string(),
            output_index,
            listed_slot: slot,
            listed_time: time,
        }));
    }
}

/// Rebuild the `listings` projection from scratch off the open book — the
/// authoritative source (evict-on-spend, no ghosts). Used to SEED an
/// already-walked ledger and to heal after a rollback. `slot`/`time` stamp the
/// rebuilt rows (the buffer doesn't retain per-listing creation slots;
/// incremental maintenance restamps the real slot on the next reprice).
pub(crate) fn rebuild_listings(
    ledger: &mut Ledger,
    registry: &VenueRegistry,
    buffer: &OutrefBuffer,
    slot: u64,
    time: i64,
) -> Result<()> {
    let mut ops: Vec<ListingOp> = Vec::new();
    for (oref, b) in buffer.entries() {
        if registry.watch_for(&b.address).map(|w| w.channel) == Some(Channel::Sale) {
            push_listing_upserts(
                &mut ops,
                &b.venue,
                b.datum_bytes.as_deref(),
                &b.assets,
                &hex::encode(oref.0.as_ref()),
                oref.1,
                slot,
                time,
            );
        }
    }
    ledger.clear_listings()?;
    ledger.apply_listing_ops(&ops)?;
    Ok(())
}

/// Resolve a produced output's datum locally: inline, else this tx's witnesses,
/// else (jpg) the labels-50 metadata reconstruction.
fn resolve_produced_datum(out: &DecodedOutput, d: &DecodedTx, is_jpg: bool) -> Option<Vec<u8>> {
    out.inline_datum
        .clone()
        .or_else(|| {
            out.datum_hash
                .and_then(|h| d.witness_datums.get(&h).cloned())
        })
        .or_else(|| {
            if is_jpg {
                match (out.datum_hash, d.aux_data.as_ref()) {
                    (Some(h), Some(aux)) => metadata::recover_datum(aux, h.as_ref()),
                    _ => None,
                }
            } else {
                None
            }
        })
}

/// Build a neutral `TxOutput`. Offer outputs carry their datum resolved into
/// `payload` (offer decode has no resolver); sale/listing outputs carry the
/// inline payload + hash so the listing decode applies its own resolution
/// policy (create = payload-only; update = resolver).
fn build_output(o: &DecodedOutput, registry: &VenueRegistry, d: &DecodedTx) -> TxOutput {
    let datum = match registry
        .watch_for(&o.address)
        .map(|w| (w.venue.clone(), w.channel))
    {
        Some((venue, Channel::Offer)) => {
            let is_jpg = matches!(registry.decoder(&venue), Some(VenueDecoder::Jpg));
            Some(OutputDatum {
                payload: resolve_produced_datum(o, d, is_jpg).unwrap_or_default(),
                hash: Vec::new(),
            })
        }
        Some((_, Channel::Sale)) => Some(OutputDatum {
            payload: o.inline_datum.clone().unwrap_or_default(),
            hash: o
                .datum_hash
                .map(|h| h.as_ref().to_vec())
                .unwrap_or_default(),
        }),
        None => None,
    };
    TxOutput {
        address: o.address.clone(),
        lovelace: o.lovelace,
        assets: o.assets.iter().map(asset_id).collect(),
        index: o.index,
        datum,
    }
}

fn asset_id(a: &Asset) -> AssetId {
    AssetId {
        policy: a.policy.clone(),
        name: a.name.clone(),
    }
}

/// Parse `--from-point` as `<slot>:<block_hash_hex>` (32-byte hash).
fn parse_point(s: &str) -> Result<(u64, Vec<u8>)> {
    let (slot, hash) = s
        .split_once(':')
        .context("--from-point must be `<slot>:<block_hash_hex>`")?;
    let slot: u64 = slot.trim().parse().context("--from-point slot")?;
    let hash = hex::decode(hash.trim()).context("--from-point hash hex")?;
    if hash.len() != 32 {
        bail!("--from-point hash must be 32 bytes, got {}", hash.len());
    }
    Ok((slot, hash))
}

/// Mainnet slot → unix seconds. Shelley (slot ≥ 4_492_800) is 1s/slot from
/// 1_596_059_091; Byron before that is 20s/slot (marketplace activity is all
/// post-Shelley, so the Byron branch is only a floor sanity fallback).
pub(crate) fn slot_to_unix(slot: u64) -> u64 {
    const SHELLEY_START_SLOT: u64 = 4_492_800;
    const SHELLEY_START_UNIX: u64 = 1_596_059_091;
    const BYRON_START_UNIX: u64 = 1_506_203_091;
    if slot >= SHELLEY_START_SLOT {
        SHELLEY_START_UNIX + (slot - SHELLEY_START_SLOT)
    } else {
        BYRON_START_UNIX + slot * 20
    }
}
