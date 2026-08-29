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
///
/// **A transaction is not one event.** A batched Wayup submission routinely
/// does several unrelated things at once — the case that forced this shape
/// made three collection offers at 5 ₳ each AND listed three NFTs at 49/147/147.
/// Collapsing that to a single `kind` plus the largest price described it as
/// "an offer created, 147 ₳", which is wrong twice: it never mentions the 343 ₳
/// of listings, and 147 ₳ is one listing's price rather than anything about
/// the transaction.
///
/// So the kinds are kept apart, each with its own subjects and its own money.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MarketEvent {
    /// The kind that best NAMES the transaction — a settlement if there is
    /// one, else whichever leg has the most rows. A headline, not a summary:
    /// read [`MarketEvent::legs`] for what actually happened.
    pub kind: String,
    /// Venue slug (`jpg`, `wayup`, …).
    pub venue: String,
    /// Every distinct kind this transaction produced.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub legs: Vec<MarketLeg>,
    /// Ledger rows across all legs — a bundle sale is one tx and many rows,
    /// and the count is the difference between "sold an NFT" and "cleared out
    /// a collection".
    pub rows: u32,
}

/// One kind of thing a transaction did, with its own subjects and money.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MarketLeg {
    /// Ledger event kind — `sold`, `listed`, `offer_created`, …
    pub kind: String,
    /// What this leg was worth.
    ///
    /// Per-item kinds sum across distinct subjects: three listings at
    /// 49 + 147 + 147 is 343 ₳ asked. A BUNDLE repeats one price across its
    /// rows, so summing would multiply it by the bundle size — those count
    /// once. [`MarketLeg::bundled`] says which rule applied.
    pub total_lovelace: u64,
    /// The leg's rows carried a bundle price rather than per-item prices.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub bundled: bool,
    /// What this leg was ABOUT. An offer names the thing it wants; a listing
    /// names the thing being sold. Without it a card can say "you spent 1.2 ₳"
    /// and never *on what*. Capped, with the true count beside it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub targets: Vec<MarketTarget>,
    /// Distinct targets before the cap.
    #[serde(default)]
    pub target_total: u32,
    pub rows: u32,
}

/// One thing an event was about. An empty `name_hex` means the whole policy —
/// a collection offer names a collection, not an asset.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MarketTarget {
    pub policy: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name_hex: String,
    /// What this specific subject was priced at — the asking price of THIS
    /// listing, not the transaction's. A per-target price is the only way a
    /// reader can see that one NFT was listed at 49 ₳ and another at 147 ₳.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_lovelace: Option<u64>,
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
            "SELECT tx_hash, kind, venue, price_lovelace, policy_id, asset_name_hex,
                    COALESCE(bundle_size, 0)
             FROM market_events WHERE tx_hash IN ({placeholders})"
        );
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query(rusqlite::params_from_iter(batch.iter()))?;

        // (tx, kind) → the leg being built. Grouping by KIND is the whole
        // point: one transaction's listings and its offers are different
        // facts with different money, and merging them loses both.
        let mut legs: HashMap<(String, String), LegAccum> = HashMap::new();
        let mut venues: HashMap<String, String> = HashMap::new();

        while let Some(r) = rows.next()? {
            let tx: String = r.get(0)?;
            let kind: String = r.get(1)?;
            let venue: String = r.get(2)?;
            let price: Option<u64> = r.get(3)?;
            let policy: String = r.get(4)?;
            let name_hex: String = r.get(5)?;
            let bundle_size: i64 = r.get(6)?;

            venues.entry(tx.clone()).or_insert(venue);
            let leg = legs.entry((tx, kind)).or_default();
            leg.rows += 1;
            leg.bundled |= bundle_size > 1;
            // Dedup on identity only — the same subject can appear twice in a
            // leg, and it should not be counted or priced twice.
            if leg.seen.insert((policy.clone(), name_hex.clone())) {
                leg.target_total += 1;
                if let Some(p) = price.filter(|p| *p > 0) {
                    leg.prices.push(p);
                }
                if leg.targets.len() < MAX_TARGETS {
                    leg.targets.push(MarketTarget {
                        policy,
                        name_hex,
                        price_lovelace: price.filter(|p| *p > 0),
                    });
                }
            }
        }

        for ((tx, kind), acc) in legs {
            let entry = out.entry(tx).or_insert_with(|| MarketEvent {
                kind: String::new(),
                venue: venues.get("").cloned().unwrap_or_default(),
                legs: Vec::new(),
                rows: 0,
            });
            entry.rows += acc.rows;
            entry.legs.push(MarketLeg {
                kind,
                // A bundle repeats one price across its rows, so counting it
                // once is the honest total; per-item kinds genuinely add up.
                total_lovelace: if acc.bundled {
                    acc.prices.iter().copied().max().unwrap_or(0)
                } else {
                    acc.prices.iter().sum()
                },
                bundled: acc.bundled,
                targets: acc.targets,
                target_total: acc.target_total,
                rows: acc.rows,
            });
        }
    }

    for (tx, ev) in out.iter_mut() {
        ev.venue = venue_of(&conn, tx).unwrap_or_default();
        // Deterministic order, and a headline that names the transaction: a
        // settlement outranks the listing that led to it — the money event is
        // the one worth putting on a card — otherwise the busiest leg wins.
        ev.legs.sort_by(|a, b| {
            is_settlement(&b.kind)
                .cmp(&is_settlement(&a.kind))
                .then(b.rows.cmp(&a.rows))
                .then(a.kind.cmp(&b.kind))
        });
        ev.kind = ev.legs.first().map(|l| l.kind.clone()).unwrap_or_default();
    }
    Ok(out)
}

/// One leg under construction.
#[derive(Default)]
struct LegAccum {
    targets: Vec<MarketTarget>,
    seen: std::collections::HashSet<(String, String)>,
    prices: Vec<u64>,
    target_total: u32,
    rows: u32,
    bundled: bool,
}

/// The venue recorded against a transaction. One per tx in practice; taking
/// the first is fine and avoids threading it through the leg accumulator.
fn venue_of(conn: &Connection, tx: &str) -> anyhow::Result<String> {
    let mut stmt = conn.prepare("SELECT venue FROM market_events WHERE tx_hash = ?1 LIMIT 1")?;
    let mut rows = stmt.query([tx])?;
    Ok(match rows.next()? {
        Some(r) => r.get(0)?,
        None => String::new(),
    })
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

    /// Build a ledger holding just the rows a test cares about, and read one
    /// transaction back through the real `lookup` path.
    ///
    /// A temp FILE rather than `:memory:` — `lookup` opens the database
    /// itself, read-only, which is the behaviour worth exercising.
    /// One `market_events` row as a test writes it:
    /// `(tx_hash, kind, price_lovelace, policy_id, asset_name_hex, bundle_size)`.
    type Row<'a> = (&'a str, &'a str, Option<u64>, &'a str, &'a str, i64);

    fn events(tx: &str, rows: &[Row<'_>]) -> MarketEvent {
        let path = std::env::temp_dir().join(format!("wallet-sieve-market-{tx}.db"));
        let _ = std::fs::remove_file(&path);
        {
            let conn = Connection::open(&path).expect("open");
            conn.execute_batch(
                "CREATE TABLE market_events (
                     tx_hash TEXT, kind TEXT, venue TEXT, price_lovelace INTEGER,
                     policy_id TEXT, asset_name_hex TEXT, bundle_size INTEGER);",
            )
            .expect("schema");
            for (t, kind, price, policy, name, bundle) in rows {
                conn.execute(
                    "INSERT INTO market_events
                         (tx_hash, kind, venue, price_lovelace, policy_id,
                          asset_name_hex, bundle_size)
                     VALUES (?1, ?2, 'wayup', ?3, ?4, ?5, ?6)",
                    rusqlite::params![t, kind, price, policy, name, bundle],
                )
                .expect("insert");
            }
        }
        let found = lookup(&path, &[tx.to_string()]);
        let _ = std::fs::remove_file(&path);
        found.get(tx).cloned().expect("event for tx")
    }

    /// The transaction that forced this shape: one batched Wayup submission
    /// that made three collection offers AND listed three NFTs.
    ///
    /// The old collapse reported it as `offer_created` at 147 ₳ — a kind that
    /// omits the listings entirely, and a price that is one listing's asking
    /// price rather than anything about the transaction.
    #[test]
    fn one_transaction_can_be_several_kinds_at_once() {
        let ev = events(
            "tx1",
            &[
                ("tx1", "offer_created", Some(5_000_000), "col_a", "", 0),
                ("tx1", "offer_created", Some(5_000_000), "col_b", "", 0),
                ("tx1", "offer_created", Some(5_000_000), "col_c", "", 0),
                ("tx1", "listed", Some(49_000_000), "perp", "0531", 0),
                ("tx1", "listed", Some(147_000_000), "perp", "3122", 0),
                ("tx1", "listed", Some(147_000_000), "perp", "4852", 0),
            ],
        );

        assert_eq!(ev.legs.len(), 2, "offers and listings are separate facts");
        assert_eq!(ev.rows, 6);

        let listed = ev.legs.iter().find(|l| l.kind == "listed").expect("listed");
        assert_eq!(
            listed.total_lovelace, 343_000_000,
            "three listings ASKED 49 + 147 + 147, not the largest of them"
        );
        assert_eq!(listed.target_total, 3);

        let offers = ev
            .legs
            .iter()
            .find(|l| l.kind == "offer_created")
            .expect("offers");
        assert_eq!(offers.total_lovelace, 15_000_000, "3 offers at 5 ₳");
        assert_eq!(offers.target_total, 3);
    }

    /// Per-target prices survive, so a reader can see 49 ₳ against one NFT and
    /// 147 ₳ against another rather than a single number for the transaction.
    #[test]
    fn each_subject_keeps_its_own_price() {
        let ev = events(
            "tx2",
            &[
                ("tx2", "listed", Some(49_000_000), "perp", "0531", 0),
                ("tx2", "listed", Some(147_000_000), "perp", "3122", 0),
            ],
        );
        let leg = &ev.legs[0];
        let mut prices: Vec<u64> = leg
            .targets
            .iter()
            .filter_map(|t| t.price_lovelace)
            .collect();
        prices.sort_unstable();
        assert_eq!(prices, [49_000_000, 147_000_000]);
    }

    /// A bundle repeats ONE price across its rows. Summing would multiply it
    /// by the bundle size — the reason the old code used `max` throughout.
    #[test]
    fn a_bundle_price_is_counted_once() {
        let ev = events(
            "tx3",
            &[
                ("tx3", "sold", Some(200_000_000), "col", "a", 3),
                ("tx3", "sold", Some(200_000_000), "col", "b", 3),
                ("tx3", "sold", Some(200_000_000), "col", "c", 3),
            ],
        );
        let leg = &ev.legs[0];
        assert!(leg.bundled);
        assert_eq!(leg.total_lovelace, 200_000_000, "not 600 ₳");
    }

    /// The headline still names the money event when a tx both lists and
    /// settles — a card has room for one word, and that word is the sale.
    #[test]
    fn the_headline_prefers_a_settlement() {
        let ev = events(
            "tx4",
            &[
                ("tx4", "listed", Some(10_000_000), "col", "a", 0),
                ("tx4", "sold", Some(10_000_000), "col", "a", 0),
            ],
        );
        assert_eq!(ev.kind, "sold");
        assert_eq!(ev.legs.len(), 2, "the listing is still reported");
    }

    #[test]
    fn a_repeated_subject_is_counted_once() {
        let ev = events(
            "tx5",
            &[
                ("tx5", "listed", Some(10_000_000), "col", "a", 0),
                ("tx5", "listed", Some(10_000_000), "col", "a", 0),
            ],
        );
        assert_eq!(ev.legs[0].target_total, 1);
        assert_eq!(ev.legs[0].total_lovelace, 10_000_000, "priced once");
    }
}
