//! `walk` — iterate certified immutable-DB history, decode each block, and drive
//! the outref buffer + `DecodeTx` assembly into the marketplace decoders.
//!
//! Per tx: buffer produced watched outputs (resolving their datum locally), take
//! any spent watched outputs back out of the buffer to build resolved inputs,
//! assemble one `DecodeTx`, and dispatch the crate decoders for each venue the
//! tx touched. This slice counts the decoded events by venue + kind; the sqlite
//! ingest + cursor/buffer persistence land next.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use anyhow::{Result, bail};
use mitos_community_events::jpg_store_listing::JpgStoreListing;
use mitos_community_events::jpg_store_offer::JpgStoreOffer;
use mitos_community_events::wayup_store_listing::WayupStoreListing;
use mitos_community_events::wayup_store_offer::WayupStoreOffer;
use mitos_marketplace_decode::{
    AssetId, DecodeTx, OutputDatum, TxInput, TxOutput, decode_jpg_listings,
    decode_jpg_offer_lifecycle, decode_jpg_sales, decode_wayup_listings,
    decode_wayup_offer_lifecycle, decode_wayup_sales,
};
use pallas_hardano::storage::immutable;
use pallas_primitives::Hash;
use pallas_traverse::MultiEraBlock;

use crate::buffer::{BufferedOutput, OutrefBuffer};
use crate::decode::{Asset, DecodedOutput, DecodedTx, decode_tx};
use crate::metadata;
use crate::venue::{Channel, VenueDecoder, VenueRegistry};

#[derive(clap::Args, Debug)]
pub struct WalkArgs {
    /// Data dir holding the immutable DB (expects `<data-dir>/immutable`).
    #[arg(long)]
    data_dir: PathBuf,

    /// Venue registry TOML.
    #[arg(long, default_value = "venues.toml")]
    venues: PathBuf,

    /// Comma-separated venues to enable (default: every venue in the registry).
    #[arg(long, value_delimiter = ',')]
    venue: Vec<String>,

    /// Start slot (default: the lowest enabled venue's `earliest_slot`).
    #[arg(long)]
    from_slot: Option<u64>,

    /// Stop after this many in-range blocks (0 = no limit) — for smoke tests.
    #[arg(long, default_value_t = 0)]
    max_blocks: u64,
}

pub fn run(args: WalkArgs) -> Result<()> {
    let registry = VenueRegistry::load(&args.venues, &args.venue)?;
    let floor = args
        .from_slot
        .unwrap_or_else(|| registry.min_earliest_slot());
    let immutable_dir = args.data_dir.join("immutable");
    if !immutable_dir.is_dir() {
        bail!(
            "immutable DB not found at {} — run `market-ledger bootstrap` first",
            immutable_dir.display()
        );
    }

    let venues: Vec<&str> = registry.venue_names().collect();
    tracing::info!(?venues, floor, dir = %immutable_dir.display(), "walk: starting");

    let blocks = immutable::read_blocks(&immutable_dir).map_err(|e| {
        anyhow::anyhow!("opening immutable DB at {}: {e:?}", immutable_dir.display())
    })?;

    let mut buffer = OutrefBuffer::default();
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    let mut scanned: u64 = 0;
    let mut in_range: u64 = 0;
    let mut last_slot: u64 = 0;

    for block in blocks {
        let bytes = block.map_err(|e| anyhow::anyhow!("reading block from chunk: {e:?}"))?;
        let blk = MultiEraBlock::decode(&bytes)
            .map_err(|e| anyhow::anyhow!("decoding block at ~#{scanned}: {e:?}"))?;
        scanned += 1;
        let slot = blk.slot();
        last_slot = slot;

        if slot < floor {
            if scanned.is_multiple_of(500_000) {
                tracing::info!(scanned, slot, "walk: skipping toward floor");
            }
            continue;
        }
        in_range += 1;

        for tx in blk.txs() {
            process_tx(decode_tx(&tx), &registry, &mut buffer, &mut counts);
        }

        if in_range.is_multiple_of(100_000) {
            tracing::info!(
                scanned,
                in_range,
                slot,
                open_book = buffer.len(),
                "walk: progress"
            );
        }
        if args.max_blocks != 0 && in_range >= args.max_blocks {
            tracing::info!(max_blocks = args.max_blocks, "walk: max-blocks reached");
            break;
        }
    }

    tracing::info!(
        scanned,
        in_range,
        last_slot,
        open_book = buffer.len(),
        ?counts,
        "walk: complete"
    );
    Ok(())
}

fn process_tx(
    d: DecodedTx,
    registry: &VenueRegistry,
    buffer: &mut OutrefBuffer,
    counts: &mut BTreeMap<String, u64>,
) {
    let mut touched: BTreeSet<String> = BTreeSet::new();

    // 1. Resolve consumed watched inputs from the buffer (local resolution — no
    //    indexer call). A hash-only listing datum unresolved at produce time is
    //    resolved now from THIS (spending) tx's witnesses.
    let mut inputs: Vec<TxInput> = Vec::new();
    for inp in &d.inputs {
        if let Some(b) = buffer.take(&inp.oref) {
            touched.insert(b.venue.clone());
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

    // 4. Dispatch each touched venue's decoders.
    for venue in &touched {
        match registry.decoder(venue) {
            Some(VenueDecoder::Jpg) => {
                for _ in decode_jpg_sales(&dtx) {
                    bump(counts, venue, "sold");
                }
                for e in decode_jpg_listings(&dtx, resolve) {
                    bump(counts, venue, jpg_listing_kind(&e));
                }
                for e in decode_jpg_offer_lifecycle(&dtx) {
                    bump(counts, venue, jpg_offer_kind(&e));
                }
            }
            Some(VenueDecoder::Wayup { sale, offer }) => {
                for _ in decode_wayup_sales(&dtx, sale) {
                    bump(counts, venue, "sold");
                }
                for e in decode_wayup_listings(&dtx, sale, resolve) {
                    bump(counts, venue, wayup_listing_kind(&e));
                }
                for e in decode_wayup_offer_lifecycle(&dtx, offer) {
                    bump(counts, venue, wayup_offer_kind(&e));
                }
            }
            None => {}
        }
    }
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

fn bump(counts: &mut BTreeMap<String, u64>, venue: &str, kind: &str) {
    *counts.entry(format!("{venue}:{kind}")).or_default() += 1;
}

fn jpg_listing_kind(e: &JpgStoreListing) -> &'static str {
    match e {
        JpgStoreListing::Create(_) => "listed",
        JpgStoreListing::Update(_) => "price_change",
        JpgStoreListing::Unlisting(_) => "delisted",
    }
}

fn wayup_listing_kind(e: &WayupStoreListing) -> &'static str {
    match e {
        WayupStoreListing::Create(_) => "listed",
        WayupStoreListing::Update(_) => "price_change",
        WayupStoreListing::Unlisting(_) => "delisted",
    }
}

fn jpg_offer_kind(e: &JpgStoreOffer) -> &'static str {
    match e {
        JpgStoreOffer::Create(_) => "offer_created",
        JpgStoreOffer::Cancel(_) => "offer_cancelled",
        JpgStoreOffer::Update(_) => "offer_updated",
        JpgStoreOffer::Accept(a) if a.collection_offer => "collection_offer_accepted",
        JpgStoreOffer::Accept(_) => "offer_accepted",
    }
}

fn wayup_offer_kind(e: &WayupStoreOffer) -> &'static str {
    match e {
        WayupStoreOffer::Create(_) => "offer_created",
        WayupStoreOffer::Cancel(_) => "offer_cancelled",
        WayupStoreOffer::Update(_) => "offer_updated",
        WayupStoreOffer::Accept(a) if a.collection_offer => "collection_offer_accepted",
        WayupStoreOffer::Accept(_) => "offer_accepted",
    }
}
