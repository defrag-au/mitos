//! The chain-tail spool — raw block CBOR from the last COMPLETE chunk to the
//! tip, kept in a small sqlite so the sieve can scan chunks + tail with one
//! code path and freshness becomes minutes, not the Mithril cadence.
//!
//! A self-contained N2N chainsync + blockfetch follower (market-ledger's
//! follow.rs shape — deliberately NOT a mitos companion, and deliberately not
//! market-ledger's own buffer, whose boundary checkpoint drops the CBOR we
//! need). NO extraction at ingest, ever: the spool is bytes, the sieve is
//! the reader — the no-indexer property holds through the tail.
//!
//! Lifecycle: intersect at the newest spool block (or the last block of the
//! last complete chunk on a cold/stale start), append `(slot, hash, cbor)`
//! per RollForward, delete past the point on RollBackward, and prune
//! everything the chunk store has come to cover after each Mithril refresh
//! (~20-25 MB/day of chain, so the spool stays page-cache warm). Scans only
//! read blocks at least [`SAFETY_DEPTH_SLOTS`] behind the spool tip, so a
//! shallow rollback can never leave phantom rows in wallet caches; a rollback
//! deeper than that is logged loudly — affected wallets need a re-excavation.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use mitos_chain_walk::mithril::CHUNK_SLOTS;
use mitos_chain_walk::open_blocks;
use pallas_network::facades::PeerClient;
use pallas_network::miniprotocols::Point;
use pallas_network::miniprotocols::chainsync::{HeaderContent, N2NClient, NextResponse};
use pallas_traverse::{MultiEraBlock, MultiEraHeader};
use rusqlite::{Connection, OpenFlags, params};

/// Spool blocks younger than this many slots below the spool tip are not
/// scanned — shallow-rollback protection for the wallet caches.
pub const SAFETY_DEPTH_SLOTS: u64 = 300;

pub fn open_rw(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS tail_blocks (
             slot INTEGER PRIMARY KEY,
             hash BLOB NOT NULL,
             cbor BLOB NOT NULL
         );",
    )?;
    Ok(conn)
}

pub fn open_ro(path: &Path) -> Result<Connection> {
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("opening {} read-only", path.display()))
}

/// A spool block: (slot, raw CBOR).
pub type SpoolBlock = (u64, Vec<u8>);

/// Scannable spool window: blocks in `(after_slot, tip - SAFETY_DEPTH]`,
/// slot-ascending. Returns `(blocks, high_water)` where `high_water` is the
/// newest slot INCLUDED (callers persist it as the wallet cursor).
pub fn scannable_blocks(
    conn: &Connection,
    after_slot: u64,
) -> Result<(Vec<SpoolBlock>, Option<u64>)> {
    let tip: Option<u64> = conn.query_row("SELECT MAX(slot) FROM tail_blocks", [], |r| r.get(0))?;
    let Some(tip) = tip else {
        return Ok((Vec::new(), None));
    };
    let horizon = tip.saturating_sub(SAFETY_DEPTH_SLOTS);
    let mut stmt = conn.prepare(
        "SELECT slot, cbor FROM tail_blocks WHERE slot > ?1 AND slot <= ?2 ORDER BY slot",
    )?;
    let mut rows = stmt.query(params![after_slot, horizon])?;
    let mut out = Vec::new();
    while let Some(r) = rows.next()? {
        out.push((r.get(0)?, r.get(1)?));
    }
    let high = out.last().map(|(s, _)| *s);
    Ok((out, high))
}

/// Spawn the follower thread. Never returns errors to the caller — it
/// reconnects with backoff forever (the spool is best-effort freshness; the
/// chunk store remains the ground truth).
pub fn spawn(immutable: PathBuf, tail_db: PathBuf, peer: String, magic: u64) -> Result<()> {
    std::thread::Builder::new()
        .name("chain-tail".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    tracing::error!("chain-tail: runtime build failed: {e}");
                    return;
                }
            };
            loop {
                if let Err(e) = rt.block_on(follow_once(&immutable, &tail_db, &peer, magic)) {
                    tracing::warn!("chain-tail: follower error, reconnecting in 10s: {e:#}");
                }
                std::thread::sleep(std::time::Duration::from_secs(10));
            }
        })
        .context("spawning chain-tail thread")?;
    Ok(())
}

/// The last block of the last COMPLETE chunk — the cold-start intersect.
fn chunk_frontier(immutable: &Path) -> Result<(u64, Vec<u8>)> {
    let chunks = crate::scan::list_chunks(immutable, 0)?;
    let Some(&last) = chunks.last() else {
        bail!("no complete chunks");
    };
    let start = last * CHUNK_SLOTS;
    let end = (last + 1) * CHUNK_SLOTS;
    let mut frontier = None;
    for raw in open_blocks(immutable, Some((start, Vec::new())))? {
        let raw = raw.map_err(|e| anyhow::anyhow!("reading block: {e:?}"))?;
        let block = MultiEraBlock::decode(&raw)
            .map_err(|e| anyhow::anyhow!("decoding chunk frontier: {e:?}"))?;
        if block.slot() >= end {
            break;
        }
        frontier = Some((block.slot(), block.hash().as_ref().to_vec()));
    }
    frontier.context("last chunk yielded no blocks")
}

async fn follow_once(immutable: &Path, tail_db: &Path, peer: &str, magic: u64) -> Result<()> {
    let conn = open_rw(tail_db)?;

    // Prune what the chunk store now covers (runs each (re)connect — i.e.
    // shortly after every Mithril refresh restart-or-reconnect).
    let chunk_end = {
        let chunks = crate::scan::list_chunks(immutable, 0)?;
        let last = chunks.last().copied().context("no complete chunks")?;
        (last + 1) * CHUNK_SLOTS - 1
    };
    let pruned = conn.execute(
        "DELETE FROM tail_blocks WHERE slot <= ?1",
        params![chunk_end],
    )?;
    if pruned > 0 {
        tracing::info!(pruned, chunk_end, "chain-tail: pruned chunk-covered spool");
    }

    // Intersect: newest spool block, else the chunk frontier. A stale spool
    // point the peer no longer knows → wipe and fall back.
    let spool_tip: Option<(u64, Vec<u8>)> = conn
        .query_row(
            "SELECT slot, hash FROM tail_blocks ORDER BY slot DESC LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .ok();
    let mut intersect = match spool_tip {
        Some(p) => p,
        None => chunk_frontier(immutable)?,
    };

    let mut client = PeerClient::connect(peer, magic)
        .await
        .with_context(|| format!("connecting to {peer}"))?;
    let (point, _tip) = match client
        .chainsync()
        .find_intersect(vec![Point::Specific(intersect.0, intersect.1.clone())])
        .await
        .context("find_intersect")?
    {
        (Some(p), tip) => (p, tip),
        (None, _) => {
            tracing::warn!("chain-tail: spool point unknown to peer — wiping spool");
            conn.execute("DELETE FROM tail_blocks", [])?;
            intersect = chunk_frontier(immutable)?;
            let (p, tip) = client
                .chainsync()
                .find_intersect(vec![Point::Specific(intersect.0, intersect.1.clone())])
                .await
                .context("find_intersect (chunk frontier)")?;
            (
                p.context("peer cannot intersect even the chunk frontier")?,
                tip,
            )
        }
    };
    tracing::info!(slot = point.slot_or_default(), "chain-tail: following");

    let mut appended: u64 = 0;
    loop {
        match next_or_await(client.chainsync()).await? {
            NextResponse::RollForward(content, _tip) => {
                let header = MultiEraHeader::decode(
                    content.variant,
                    content.byron_prefix.map(|p| p.0),
                    &content.cbor,
                )
                .map_err(|e| anyhow::anyhow!("decoding header: {e:?}"))?;
                let slot = header.slot();
                let hash = header.hash();
                let body = client
                    .blockfetch()
                    .fetch_single(Point::Specific(slot, hash.as_ref().to_vec()))
                    .await
                    .with_context(|| format!("blockfetch at slot {slot}"))?;
                conn.execute(
                    "INSERT OR REPLACE INTO tail_blocks (slot, hash, cbor) VALUES (?1, ?2, ?3)",
                    params![slot, hash.as_ref(), body],
                )?;
                appended += 1;
                if appended.is_multiple_of(500) {
                    tracing::info!(slot, appended, "chain-tail: catching up");
                }
            }
            NextResponse::RollBackward(point, _tip) => {
                // Chainsync always opens with a rollback to the intersect —
                // only one that actually removes blocks is news. Scans stay
                // SAFETY_DEPTH_SLOTS behind tip, so shallow rollbacks never
                // reach wallet caches; a deep one would need re-excavation.
                let slot = point.slot_or_default();
                let removed =
                    conn.execute("DELETE FROM tail_blocks WHERE slot > ?1", params![slot])?;
                if removed > 0 {
                    tracing::warn!(slot, removed, "chain-tail: rolled back");
                }
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
