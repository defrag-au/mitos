//! Marketplace semantics — what a transaction *was*, from market-ledger.
//!
//! The sieve can say a wallet paid 9.57 ₳ and received two NFTs; only
//! market-ledger knows that was a **Wayup purchase at 2.43 ₳ each**, because
//! recognising a venue's datum shapes is its whole job and duplicating that
//! here would be a second implementation of the thing memory keeps warning
//! about.
//!
//! ## Why a read-only sqlite open, not the HTTP surface
//!
//! market-ledger's contract is "consumers PULL" and its hosted `/events` is
//! how off-box consumers do that. This consumer is **on the same box**, and
//! its query — "what do you know about these 500 tx hashes" — is a filter
//! `/events` does not offer, so the alternative was changing a live service
//! that other consumers depend on. A read-only handle against a WAL database
//! cannot block its writer, and `tx_hash` leads the primary key so this is an
//! index seek.
//!
//! The coupling that buys is one query over five columns, and it is
//! **fail-soft by construction**: a missing file, a locked db, or a renamed
//! column yields an empty map and rows that simply lack their venue label —
//! never a failed excavation.

use std::collections::HashMap;
use std::path::Path;

use rusqlite::{Connection, OpenFlags};

/// Targets kept per transaction. A cart with more says so via `target_total`.
const MAX_TARGETS: usize = 12;

/// What market-ledger knows about one transaction.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MarketEvent {
    /// Ledger event kind — `sold`, `listed`, `offer_filled`, …
    pub kind: String,
    /// Venue slug (`jpg`, `wayup`, …).
    pub venue: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price_lovelace: Option<u64>,
    /// How many ledger rows this tx produced — a bundle sale is one tx and
    /// many rows, and the count is the difference between "sold an NFT" and
    /// "cleared out a collection".
    pub rows: u32,
    /// What the event was ABOUT. An offer moves ADA into a contract and names
    /// the thing it wants; without this a card can only say "you spent 1.2 ₳
    /// on an offer" and never *for what*. A Wayup cart is one transaction
    /// with many of these — seen up to 25 distinct collections in a single
    /// tx — so it is a list, capped, with the true count beside it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub targets: Vec<MarketTarget>,
    /// Distinct targets before the cap.
    #[serde(default)]
    pub target_total: u32,
}

/// One thing an event was about. An empty `name_hex` means the whole policy —
/// a collection offer names a collection, not an asset.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct MarketTarget {
    pub policy: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name_hex: String,
}

/// Look up market events for `hashes` (hex). Returns the subset that matched;
/// any failure degrades to an empty map with a warning.
pub fn lookup(db: &Path, hashes: &[String]) -> HashMap<String, MarketEvent> {
    if hashes.is_empty() || !db.exists() {
        return HashMap::new();
    }
    match try_lookup(db, hashes) {
        Ok(found) => found,
        Err(e) => {
            tracing::warn!("market enrichment unavailable: {e:#}");
            HashMap::new()
        }
    }
}

fn try_lookup(db: &Path, hashes: &[String]) -> anyhow::Result<HashMap<String, MarketEvent>> {
    let conn = Connection::open_with_flags(
        db,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )?;
    let mut out: HashMap<String, MarketEvent> = HashMap::new();
    // Chunked so the bind-variable count stays sane on a long history.
    for batch in hashes.chunks(400) {
        let placeholders = std::iter::repeat_n("?", batch.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT tx_hash, kind, venue, price_lovelace, policy_id, asset_name_hex
             FROM market_events WHERE tx_hash IN ({placeholders})"
        );
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query(rusqlite::params_from_iter(batch.iter()))?;
        let mut seen: HashMap<String, std::collections::HashSet<MarketTarget>> = HashMap::new();
        while let Some(r) = rows.next()? {
            let tx: String = r.get(0)?;
            let kind: String = r.get(1)?;
            let venue: String = r.get(2)?;
            let price: Option<u64> = r.get(3)?;
            let target = MarketTarget {
                policy: r.get(4)?,
                name_hex: r.get(5)?,
            };
            let fresh = seen.entry(tx.clone()).or_default().insert(target.clone());
            let entry = out.entry(tx).or_insert_with(|| MarketEvent {
                kind: kind.clone(),
                venue: venue.clone(),
                price_lovelace: None,
                rows: 0,
                targets: Vec::new(),
                target_total: 0,
            });
            if fresh {
                entry.target_total += 1;
                if entry.targets.len() < MAX_TARGETS {
                    entry.targets.push(target);
                }
            }
            entry.rows += 1;
            // A settled sale outranks the listing that led to it when one tx
            // carries both — the money event is the one worth naming.
            if is_settlement(&kind) && !is_settlement(&entry.kind) {
                entry.kind = kind;
                entry.venue = venue;
                entry.price_lovelace = None;
            }
            // Bundle rows repeat the bundle price; sum only distinct legs.
            if let Some(p) = price.filter(|p| *p > 0) {
                entry.price_lovelace = Some(entry.price_lovelace.unwrap_or(0).max(p));
            }
        }
    }
    Ok(out)
}

/// Kinds where money actually moved.
fn is_settlement(kind: &str) -> bool {
    matches!(kind, "sold" | "offer_filled" | "bought" | "sale")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settlement_outranks_listing() {
        assert!(is_settlement("sold"));
        assert!(is_settlement("offer_filled"));
        assert!(!is_settlement("listed"));
        assert!(!is_settlement("delisted"));
    }

    #[test]
    fn missing_database_is_not_an_error() {
        let found = lookup(Path::new("/nonexistent/market.db"), &["abc".into()]);
        assert!(found.is_empty());
    }
}
