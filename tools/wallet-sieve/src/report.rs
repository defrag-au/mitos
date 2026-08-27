//! Output — JSONL rows (typed structs, one per timeline tx) + a stderr
//! summary with the pass timings the spike exists to measure.

use std::collections::HashMap;

use mitos_chain_walk::slot_to_unix;
use serde::{Deserialize, Serialize};

use crate::classify::TimelineTx;

#[derive(Serialize, Deserialize)]
pub struct Row {
    pub kind: String,
    pub slot: u64,
    pub time: String,
    pub tx: String,
    pub lovelace_in: u64,
    pub lovelace_out: u64,
    pub net_lovelace: i64,
    pub assets_in: u32,
    pub assets_out: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub senders: Option<Vec<Sender>>,
    /// Destinations of a send (address + lovelace, largest first).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recipients: Option<Vec<Sender>>,
    /// Which assets moved, netted (positive arrived, negative left).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assets: Option<Vec<AssetEntry>>,
    /// What market-ledger says this transaction was, when it knows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub market: Option<crate::market::MarketEvent>,
}

/// One asset movement on a row. Names stay hex on the wire — decoding (and
/// CIP-67 label stripping) is display work, and `cardano-assets::AssetId`
/// already owns it.
#[derive(Serialize, Deserialize, Clone)]
pub struct AssetEntry {
    pub policy: String,
    pub name_hex: String,
    pub quantity: i64,
    /// Created in this transaction — read from the mint field, so a consumer
    /// can say "mint" as a fact rather than inferring it from a payment
    /// followed by a delivery, which is a shape that purchases share.
    ///
    /// Defaulted so a cache written before this field existed still
    /// deserializes; those rows simply report neither.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub minted: bool,
    /// Destroyed in this transaction.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub burned: bool,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Sender {
    pub address: String,
    pub lovelace: u64,
}

/// A timeline tx + the resolved source map → one output row.
pub fn row_for(tx: &TimelineTx, sources: &HashMap<([u8; 32], u32), (String, u64)>) -> Row {
    let senders: Vec<Sender> = tx
        .foreign_inputs
        .iter()
        .filter_map(|oref| sources.get(oref))
        .map(|(address, lovelace)| Sender {
            address: address.clone(),
            lovelace: *lovelace,
        })
        .collect();
    Row {
        kind: tx.kind.to_string(),
        slot: tx.slot,
        time: fmt_unix(slot_to_unix(tx.slot)),
        tx: hex::encode(tx.hash),
        lovelace_in: tx.lovelace_in,
        lovelace_out: tx.lovelace_out,
        net_lovelace: tx.lovelace_in as i64 - tx.lovelace_out as i64,
        assets_in: tx.assets_in,
        assets_out: tx.assets_out,
        senders: if senders.is_empty() {
            None
        } else {
            Some(senders)
        },
        // Enrichment is stitched in by the store layer, which is where the
        // market lookup happens; a bare row_for() call carries none.
        market: None,
        assets: if tx.asset_moves.is_empty() {
            None
        } else {
            Some(
                tx.asset_moves
                    .iter()
                    .map(|m| AssetEntry {
                        policy: m.policy.clone(),
                        name_hex: m.name_hex.clone(),
                        quantity: m.quantity,
                        minted: m.minted,
                        burned: m.burned,
                    })
                    .collect(),
            )
        },
        recipients: if tx.recipients.is_empty() {
            None
        } else {
            Some(
                tx.recipients
                    .iter()
                    .map(|(address, lovelace)| Sender {
                        address: address.clone(),
                        lovelace: *lovelace,
                    })
                    .collect(),
            )
        },
    }
}

/// Unix seconds → `YYYY-MM-DD HH:MM` UTC (civil-from-days, Hinnant's
/// algorithm) — enough for a human timeline without a chrono dependency.
pub fn fmt_unix(unix: u64) -> String {
    let days = (unix / 86_400) as i64;
    let secs = unix % 86_400;
    let (h, m) = (secs / 3600, (secs % 3600) / 60);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mth = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if mth <= 2 { y + 1 } else { y };
    format!("{y:04}-{mth:02}-{d:02} {h:02}:{m:02}")
}
