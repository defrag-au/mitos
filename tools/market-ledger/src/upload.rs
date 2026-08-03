//! `upload` — POST the ledger's hot window to the worker's `/admin/ledger-ingest`
//! endpoint in batches. The prod-backfill path (replacing the sharded-Koios
//! firehose): `INSERT OR IGNORE` on the worker side makes it idempotent and
//! re-runnable, a no-op for rows the live feed already landed.
//!
//! Only the six D1 event kinds are uploaded; the walker's extra offer-book kinds
//! (`offer_created`/`_cancelled`/`_updated`) stay local. The window is anchored
//! on the ledger's newest event, not wall-clock, so it's stable across re-runs.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};

#[derive(clap::Args, Debug)]
pub struct UploadArgs {
    /// Ledger sqlite path.
    #[arg(long, default_value = "market-ledger.db")]
    db: PathBuf,

    /// Worker ingest endpoint, e.g. `https://ownership.dev.cnft.dev/admin/ledger-ingest`.
    #[arg(long)]
    endpoint: String,

    /// Admin token (sent as `X-Debug-Token`).
    #[arg(long, env = "DEBUG_TOKEN")]
    token: String,

    /// Hot-window size in days, anchored on the ledger's newest event.
    #[arg(long, default_value_t = 90)]
    window_days: u64,

    /// Rows per POST (the worker chunks at 500 internally too).
    #[arg(long, default_value_t = 500)]
    batch: usize,

    /// Read + report the window without POSTing.
    #[arg(long)]
    dry_run: bool,
}

/// The six D1 event kinds (the walker's offer-book kinds stay local).
const D1_KINDS: &str =
    "'sold','listed','price_change','delisted','offer_accepted','collection_offer_accepted'";

#[derive(Serialize)]
struct IngestRow {
    tx_hash: String,
    policy_id: String,
    asset_name_hex: String,
    fingerprint: Option<String>,
    kind: String,
    price_lovelace: Option<u64>,
    buyer_price_lovelace: Option<u64>,
    seller_stake: Option<String>,
    buyer_stake: Option<String>,
    marketplace: String,
    bundle_size: Option<u32>,
    output_index: Option<u32>,
    fee_waived: bool,
    slot: u64,
    block_height: Option<u64>,
    block_time: u64,
}

#[derive(Deserialize)]
struct IngestResponse {
    #[allow(dead_code)]
    received: usize,
    inserted: usize,
}

pub fn run(args: UploadArgs) -> Result<()> {
    let conn = Connection::open_with_flags(&args.db, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("opening ledger {} read-only", args.db.display()))?;

    let max_bt: Option<i64> = conn
        .query_row("SELECT MAX(block_time) FROM market_events", [], |r| {
            r.get(0)
        })
        .optional()?
        .flatten();
    let Some(max_bt) = max_bt else {
        tracing::info!("upload: ledger is empty, nothing to send");
        return Ok(());
    };
    let floor_bt = max_bt - (args.window_days as i64 * 86_400);

    let sql = format!(
        "SELECT tx_hash, policy_id, asset_name_hex, fingerprint, kind,
                price_lovelace, buyer_price_lovelace, seller_stake, buyer_stake,
                marketplace, bundle_size, output_index, fee_waived,
                slot, block_height, block_time
         FROM market_events
         WHERE block_time >= ?1 AND kind IN ({D1_KINDS})
         ORDER BY slot"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows: Vec<IngestRow> = stmt
        .query_map([floor_bt], |r| {
            Ok(IngestRow {
                tx_hash: r.get(0)?,
                policy_id: r.get(1)?,
                asset_name_hex: r.get(2)?,
                fingerprint: r.get(3)?,
                kind: r.get(4)?,
                price_lovelace: r.get::<_, Option<i64>>(5)?.map(|v| v as u64),
                buyer_price_lovelace: r.get::<_, Option<i64>>(6)?.map(|v| v as u64),
                seller_stake: r.get(7)?,
                buyer_stake: r.get(8)?,
                marketplace: r.get(9)?,
                bundle_size: r.get::<_, Option<i64>>(10)?.map(|v| v as u32),
                output_index: r.get::<_, Option<i64>>(11)?.map(|v| v as u32),
                fee_waived: r.get::<_, i64>(12)? != 0,
                slot: r.get::<_, i64>(13)? as u64,
                block_height: r.get::<_, Option<i64>>(14)?.map(|v| v as u64),
                block_time: r.get::<_, i64>(15)? as u64,
            })
        })?
        .collect::<rusqlite::Result<_>>()?;

    tracing::info!(
        total = rows.len(),
        window_days = args.window_days,
        floor_block_time = floor_bt,
        "upload: rows in window"
    );
    if args.dry_run {
        tracing::info!("upload: --dry-run, not posting");
        return Ok(());
    }
    if rows.is_empty() {
        return Ok(());
    }

    let client = reqwest::blocking::Client::new();
    let mut sent = 0usize;
    let mut inserted = 0usize;
    for chunk in rows.chunks(args.batch) {
        let resp = client
            .post(&args.endpoint)
            .header("X-Debug-Token", &args.token)
            .json(chunk)
            .send()
            .with_context(|| format!("POST {}", args.endpoint))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().unwrap_or_default();
            bail!("ingest endpoint returned {status}: {body}");
        }
        let body: IngestResponse = resp.json().context("decoding ingest response")?;
        sent += chunk.len();
        inserted += body.inserted;
        tracing::info!(sent, inserted, "upload: batch ok");
    }

    tracing::info!(sent, inserted, "upload: complete");
    Ok(())
}
