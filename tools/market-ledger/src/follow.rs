//! `follow` — self-contained chainsync tip tail (phase 3).
//!
//! A pallas-network N2N chainsync + blockfetch client feeding the exact same
//! `process_tx` path as `walk`. NOT a mitos companion subscriber (design
//! decision 2026-07-22 — see MARKET_LEDGER.md "Follow mode"): no WASM module
//! hot path, no second follower store; the tool's own persisted outref buffer
//! is the inputs cache, warm from the preceding walk.
//!
//! # Topology
//!
//! Walk a fresh snapshot to near-tip, then `follow` intersects chainsync at
//! the persisted walk cursor and streams forward. Two buffers:
//!
//! - **live** — at tip; every fetched block is processed into it and its
//!   events inserted immediately (`INSERT OR IGNORE`, idempotent).
//! - **boundary** — trails tip by k blocks (`--volatile-blocks`). Raw block
//!   CBOR for the boundary..tip window is kept in `volatile_blocks`. The
//!   boundary checkpoint rewrites the whole open book (tens of thousands of
//!   rows on a deep ledger), so it's sealed in BATCHES (`--checkpoint-batch`):
//!   the window grows to k + batch, then one checkpoint seals a batch of
//!   blocks into the boundary buffer (same `walk_cursor`/`outref_buffer`
//!   tables walk uses) and drops their CBOR. Amortizing the rewrite this way
//!   is the difference between a crawling and a network-bound catch-up.
//!
//! On `RollBackward(point)`: events + volatile blocks past the point are
//! deleted, live is rebuilt as boundary + replay of the surviving window.
//! A rollback past the boundary (> k blocks — beyond Ouroboros finality)
//! aborts with a re-walk instruction.
//!
//! Crash-safety is ordering-based: per block, events insert BEFORE the
//! volatile block; a crash between the two re-fetches that block on resume
//! (intersect = newest volatile) and re-inserts idempotently. A crash during
//! a boundary advance leaves `slot <= boundary` volatile rows behind, swept
//! at startup.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use pallas_network::facades::PeerClient;
use pallas_network::miniprotocols::Point;
use pallas_network::miniprotocols::chainsync::{HeaderContent, N2NClient, NextResponse};
use pallas_primitives::Hash;
use pallas_traverse::{MultiEraBlock, MultiEraHeader};

use crate::buffer::OutrefBuffer;
use crate::decode::decode_tx;
use crate::row::{BlockCtx, MarketEventRow};
use crate::store::Ledger;
use crate::venue::VenueRegistry;
use crate::walk::{process_tx, slot_to_unix};

#[derive(clap::Args, Debug)]
pub struct FollowArgs {
    /// Ledger sqlite path (the same db a prior `walk` populated).
    #[arg(long, default_value = "market-ledger.db")]
    db: PathBuf,

    /// Venue registry TOML.
    #[arg(long, default_value = "venues.toml")]
    venues: PathBuf,

    /// Comma-separated venues to enable (default: every venue in the registry).
    #[arg(long, value_delimiter = ',')]
    venue: Vec<String>,

    /// N2N peer to chainsync from. Prefer a localhost dolos if one exposes an
    /// o7s listener; a public relay otherwise.
    #[arg(
        long,
        env = "MARKET_LEDGER_PEER",
        default_value = "backbone.cardano.iog.io:3001"
    )]
    peer: String,

    /// Network magic (mainnet).
    #[arg(long, default_value_t = 764_824_073)]
    magic: u64,

    /// Volatile window size k, in blocks (the Ouroboros security parameter —
    /// rollbacks deeper than this abort).
    #[arg(long, default_value_t = 2160)]
    volatile_blocks: u64,

    /// Boundary-advance batch size. The boundary checkpoint rewrites the whole
    /// open book (tens of thousands of rows on a deep ledger), so it's sealed
    /// in batches: the window grows to k + this before one checkpoint seals a
    /// batch of blocks. Bigger = cheaper catch-up, slightly more replay on
    /// resume + a slightly larger retained window.
    #[arg(long, default_value_t = 1000)]
    checkpoint_batch: u64,

    /// Intersect override `<slot>:<block_hash_hex>` (default: the persisted
    /// walk cursor). The buffer is only warm at the CURSOR — an arbitrary
    /// point drops events whose listings predate it (same caveat as walk's
    /// `--from-point`).
    #[arg(long)]
    from_point: Option<String>,

    /// Stop after this many blocks (0 = run until interrupted) — smoke tests.
    #[arg(long, default_value_t = 0)]
    max_blocks: u64,
}

struct FollowState {
    live: OutrefBuffer,
    boundary: OutrefBuffer,
    boundary_slot: u64,
}

pub fn run(args: FollowArgs) -> Result<()> {
    let registry = VenueRegistry::load(&args.venues, &args.venue)?;
    let enabled: Vec<String> = registry.venue_names().map(str::to_owned).collect();
    let enabled_refs: Vec<&str> = enabled.iter().map(String::as_str).collect();

    let mut ledger = Ledger::open(&args.db)?;

    // Boundary = the persisted checkpoint (buffer + cursor).
    let cursor = ledger.cursor_point(&enabled_refs)?;
    let override_point = args.from_point.as_deref().map(parse_point).transpose()?;
    let (boundary_slot, boundary_hash) = match (&override_point, &cursor) {
        (Some((slot, hash)), _) => (*slot, hash.clone()),
        (None, Some((slot, hash))) => (*slot, hash.clone()),
        (None, None) => bail!(
            "no usable walk cursor in {} — run `market-ledger walk` first (or pass \
             --from-point <slot>:<hash>, accepting a cold buffer)",
            args.db.display()
        ),
    };
    let boundary = ledger.load_buffer()?;
    if override_point.is_some() && boundary.is_empty() {
        tracing::warn!(
            "follow: --from-point with a cold buffer — events on listings created \
             before the point won't resolve"
        );
    }

    // Sweep volatile rows a crashed boundary-advance left behind, then rebuild
    // the live state: boundary + replay of the surviving volatile window.
    ledger.delete_volatile_upto(boundary_slot)?;
    let mut state = FollowState {
        live: boundary.clone(),
        boundary,
        boundary_slot,
    };
    let volatile = ledger.volatile_after(boundary_slot)?;
    let mut intersect = (boundary_slot, boundary_hash);
    let mut rows: Vec<MarketEventRow> = Vec::new();
    for vb in &volatile {
        apply_block(&vb.cbor, &registry, &mut state.live, &mut rows)?;
        ledger.insert_events(&rows)?; // idempotent — usually all no-ops
        rows.clear();
        intersect = (vb.slot, vb.hash.clone());
    }

    tracing::info!(
        venues = ?enabled,
        peer = %args.peer,
        intersect_slot = intersect.0,
        open_book = state.live.len(),
        volatile = volatile.len(),
        k = args.volatile_blocks,
        "follow: starting"
    );

    let rt = tokio::runtime::Runtime::new().context("building tokio runtime")?;
    rt.block_on(async {
        tokio::select! {
            r = follow_loop(&args, &registry, &enabled_refs, &mut ledger, &mut state, intersect) => r,
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("follow: interrupted — state is checkpoint-consistent, re-run to resume");
                Ok(())
            }
        }
    })
}

async fn follow_loop(
    args: &FollowArgs,
    registry: &VenueRegistry,
    venues: &[&str],
    ledger: &mut Ledger,
    state: &mut FollowState,
    intersect: (u64, Vec<u8>),
) -> Result<()> {
    let mut peer = PeerClient::connect(&args.peer, args.magic)
        .await
        .with_context(|| format!("connecting to peer {}", args.peer))?;

    let (point, tip) = peer
        .chainsync()
        .find_intersect(vec![Point::Specific(intersect.0, intersect.1.clone())])
        .await
        .context("find_intersect")?;
    let point = point.with_context(|| {
        format!(
            "peer has no block at slot {} with our cursor hash — the cursor may \
             predate the peer's horizon or sit on an orphaned fork",
            intersect.0
        )
    })?;
    tracing::info!(
        intersect_slot = point.slot_or_default(),
        tip_slot = tip.0.slot_or_default(),
        behind = tip
            .0
            .slot_or_default()
            .saturating_sub(point.slot_or_default()),
        "follow: intersected"
    );

    let mut processed: u64 = 0;
    let mut inserted_total: u64 = 0;
    let mut at_tip_logged = false;
    let mut rows: Vec<MarketEventRow> = Vec::new();

    loop {
        let next = next_or_await(peer.chainsync()).await?;
        match next {
            NextResponse::RollForward(content, tip) => {
                let (slot, hash) = header_point(&content)?;
                let body = peer
                    .blockfetch()
                    .fetch_single(Point::Specific(slot, hash.as_ref().to_vec()))
                    .await
                    .with_context(|| format!("blockfetch at slot {slot}"))?;

                // Events BEFORE volatile (crash between = idempotent re-apply).
                let height = apply_block(&body, registry, &mut state.live, &mut rows)?;
                let inserted = ledger.insert_events(&rows)?;
                inserted_total += inserted as u64;
                if inserted > 0 {
                    tracing::info!(
                        slot,
                        inserted,
                        open_book = state.live.len(),
                        "follow: events"
                    );
                }
                rows.clear();
                ledger.insert_volatile(slot, hash.as_ref(), Some(height), &body)?;

                advance_boundary(
                    args.volatile_blocks,
                    args.checkpoint_batch,
                    registry,
                    venues,
                    ledger,
                    state,
                )?;

                processed += 1;
                let behind = tip.0.slot_or_default().saturating_sub(slot);
                if behind > 300 {
                    at_tip_logged = false;
                    if processed.is_multiple_of(500) {
                        tracing::info!(
                            processed,
                            slot,
                            behind_slots = behind,
                            inserted = inserted_total,
                            "follow: catching up"
                        );
                    }
                } else if !at_tip_logged {
                    at_tip_logged = true;
                    tracing::info!(slot, processed, "follow: at tip");
                }
                if args.max_blocks != 0 && processed >= args.max_blocks {
                    tracing::info!(max_blocks = args.max_blocks, "follow: max-blocks reached");
                    return Ok(());
                }
            }
            NextResponse::RollBackward(point, _tip) => {
                rollback(&point, registry, ledger, state)?;
            }
            NextResponse::Await => {}
        }
    }
}

/// One `RequestNext`, waiting out the tip-idle `Await` for the forced reply.
async fn next_or_await(cs: &mut N2NClient) -> Result<NextResponse<HeaderContent>> {
    match cs.request_next().await.context("chainsync request_next")? {
        NextResponse::Await => cs
            .recv_while_must_reply()
            .await
            .context("chainsync await reply"),
        other => Ok(other),
    }
}

fn header_point(content: &HeaderContent) -> Result<(u64, Hash<32>)> {
    let header = MultiEraHeader::decode(
        content.variant,
        content.byron_prefix.map(|(sub, _)| sub),
        &content.cbor,
    )
    .map_err(|e| anyhow::anyhow!("decoding header: {e:?}"))?;
    Ok((header.slot(), header.hash()))
}

/// Decode a block and run every tx through the walk pipeline against `buffer`.
/// Returns the block height.
fn apply_block(
    bytes: &[u8],
    registry: &VenueRegistry,
    buffer: &mut OutrefBuffer,
    rows: &mut Vec<MarketEventRow>,
) -> Result<u64> {
    let blk = MultiEraBlock::decode(bytes).map_err(|e| anyhow::anyhow!("decoding block: {e:?}"))?;
    let ctx = BlockCtx {
        slot: blk.slot(),
        height: Some(blk.number()),
        time: slot_to_unix(blk.slot()),
    };
    for tx in blk.txs() {
        process_tx(decode_tx(&tx), registry, buffer, &ctx, rows);
    }
    Ok(blk.number())
}

/// While the volatile window exceeds k: replay the oldest block into the
/// boundary buffer, checkpoint it (buffer + cursor in one txn), drop the raw
/// block. Order is crash-safe: a crash after checkpoint but before the delete
/// only leaves `slot <= boundary` rows for the startup sweep.
fn advance_boundary(
    k: u64,
    batch: u64,
    registry: &VenueRegistry,
    venues: &[&str],
    ledger: &mut Ledger,
    state: &mut FollowState,
) -> Result<()> {
    // Cheap gate: only touch the DB once the window has grown a full batch past
    // k. Most blocks return here. `checkpoint` rewrites the entire open book
    // (tens of thousands of rows on a deep ledger, ~1.5s), so we amortize ONE
    // rewrite over `batch` sealed blocks instead of firing it per block — the
    // difference between a ~0.6 blk/s and a network-bound catch-up.
    let count = ledger.volatile_count()?;
    if count <= k + batch {
        return Ok(());
    }

    // Seal everything except the newest k blocks, replaying each into the
    // boundary buffer. Order matters for crash-safety: replay + checkpoint
    // (persist the new boundary) BEFORE deleting the sealed CBOR — a crash
    // between checkpoint and delete only leaves already-sealed rows for the
    // startup sweep; a crash before checkpoint replays them again (idempotent).
    let to_seal = count - k;
    let blocks = ledger.volatile_oldest(to_seal)?;
    let mut scratch: Vec<MarketEventRow> = Vec::new();
    let mut sealed: Option<(u64, Vec<u8>)> = None;
    for vb in &blocks {
        apply_block(&vb.cbor, registry, &mut state.boundary, &mut scratch)?;
        scratch.clear(); // rows were inserted when the block was first seen
        sealed = Some((vb.slot, vb.hash.clone()));
    }
    if let Some((slot, hash)) = sealed {
        ledger.checkpoint(&state.boundary, venues, slot, &hash)?;
        ledger.delete_volatile_upto(slot)?;
        state.boundary_slot = slot;
    }
    Ok(())
}

/// Chainsync told us our chain past `point` is orphaned: truncate events +
/// volatile blocks past it and rebuild the live buffer from the boundary
/// checkpoint + a replay of the surviving window.
fn rollback(
    point: &Point,
    registry: &VenueRegistry,
    ledger: &mut Ledger,
    state: &mut FollowState,
) -> Result<()> {
    let target = match point {
        Point::Origin => bail!("peer rolled back to origin — refusing; re-walk from a snapshot"),
        Point::Specific(slot, _) => *slot,
    };
    if target < state.boundary_slot {
        bail!(
            "rollback to slot {target} is beyond the volatile window (boundary \
             {}) — deeper than k blocks should be impossible; re-walk from a \
             fresh snapshot",
            state.boundary_slot
        );
    }

    let dropped_events = ledger.delete_events_after(target)?;
    let dropped_blocks = ledger.delete_volatile_after(target)?;
    state.live = state.boundary.clone();
    let mut rows: Vec<MarketEventRow> = Vec::new();
    let survivors = ledger.volatile_after(state.boundary_slot)?;
    for vb in &survivors {
        apply_block(&vb.cbor, registry, &mut state.live, &mut rows)?;
        rows.clear();
    }
    tracing::warn!(
        target,
        dropped_events,
        dropped_blocks,
        replayed = survivors.len(),
        open_book = state.live.len(),
        "follow: rolled back"
    );
    Ok(())
}

/// Parse `<slot>:<block_hash_hex>` (same shape as walk's `--from-point`).
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Ledger;
    use std::path::Path;

    /// The real mainnet block fixture the decode tests use — gives the
    /// volatile plumbing a genuine block to chew on (no watched venues in it,
    /// so zero events; the buffer/window mechanics are what's under test).
    const FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../crates/mitos-platform/tests/fixtures/186000000.block.cbor"
    );

    fn fixture_block() -> Vec<u8> {
        std::fs::read(Path::new(FIXTURE)).expect("block fixture")
    }

    #[test]
    fn volatile_window_roundtrip_and_sweeps() {
        let mut led = Ledger::open(Path::new(":memory:")).unwrap();
        let cbor = fixture_block();
        for (slot, hash_byte) in [(100u64, 1u8), (110, 2), (120, 3)] {
            led.insert_volatile(slot, &[hash_byte; 32], Some(slot / 10), &cbor)
                .unwrap();
        }
        assert_eq!(led.volatile_count().unwrap(), 3);

        let after = led.volatile_after(100).unwrap();
        assert_eq!(
            after.iter().map(|v| v.slot).collect::<Vec<_>>(),
            vec![110, 120]
        );
        assert_eq!(after[0].cbor, cbor);

        // volatile_oldest(limit) returns the seal batch, slot ASC.
        let batch = led.volatile_oldest(2).unwrap();
        assert_eq!(
            batch
                .iter()
                .map(|v| (v.slot, v.hash[0]))
                .collect::<Vec<_>>(),
            vec![(100, 1), (110, 2)]
        );

        // Boundary advance sweep (<=) and rollback truncate (>).
        assert_eq!(led.delete_volatile_upto(100).unwrap(), 1);
        assert_eq!(led.delete_volatile_after(110).unwrap(), 1);
        assert_eq!(led.volatile_count().unwrap(), 1);
        assert_eq!(led.volatile_oldest(1).unwrap()[0].slot, 110);
    }

    #[test]
    fn events_truncate_past_rollback_point() {
        let mut led = Ledger::open(Path::new(":memory:")).unwrap();
        let mk = |tx: &str, slot: u64| crate::row::MarketEventRow {
            tx_hash: tx.into(),
            policy_id: "p".into(),
            asset_name_hex: "n".into(),
            fingerprint: None,
            kind: "sold".into(),
            price_lovelace: Some(1),
            buyer_price_lovelace: Some(1),
            seller_stake: None,
            buyer_stake: None,
            marketplace: "wayup".into(),
            bundle_size: None,
            output_index: None,
            fee_waived: false,
            slot,
            block_height: None,
            block_time: slot,
            venue: "wayup".into(),
        };
        led.insert_events(&[mk("a", 5), mk("b", 10), mk("c", 15)])
            .unwrap();
        assert_eq!(led.delete_events_after(10).unwrap(), 1);
        assert_eq!(led.delete_events_after(10).unwrap(), 0);
    }

    #[test]
    fn cursor_point_skips_empty_hashes_and_takes_min() {
        let mut led = Ledger::open(Path::new(":memory:")).unwrap();
        let buf = OutrefBuffer::default();
        // Old-style final checkpoint with an empty hash — unusable for
        // intersect, must be skipped.
        led.checkpoint(&buf, &["jpg"], 500, &[]).unwrap();
        assert_eq!(led.cursor_point(&["jpg"]).unwrap(), None);

        led.checkpoint(&buf, &["jpg"], 700, &[7u8; 32]).unwrap();
        led.checkpoint(&buf, &["wayup"], 600, &[6u8; 32]).unwrap();
        let (slot, hash) = led.cursor_point(&["jpg", "wayup"]).unwrap().unwrap();
        assert_eq!((slot, hash[0]), (600, 6));
    }

    #[test]
    fn apply_block_processes_real_fixture() {
        let registry = VenueRegistry::load(
            Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/venues.toml")),
            &[],
        )
        .unwrap();
        let mut buffer = OutrefBuffer::default();
        let mut rows = Vec::new();
        let height = apply_block(&fixture_block(), &registry, &mut buffer, &mut rows).unwrap();
        assert!(height > 0);
        // Fixture block touches no watched venue — no events, empty book.
        assert!(rows.is_empty());
        assert!(buffer.is_empty());
    }

    #[test]
    fn parse_point_shape() {
        let (slot, hash) = parse_point(&format!("123:{}", hex::encode([9u8; 32]))).unwrap();
        assert_eq!((slot, hash[0]), (123, 9));
        assert!(parse_point("123").is_err());
        assert!(parse_point("123:abcd").is_err());
    }
}
