//! Event query building + keyset pagination.
//!
//! Order is `(slot ASC, rowid ASC)`. Slot alone isn't unique (bundles, busy
//! blocks); rowid is, and sqlite stores rowid as the implicit trailing
//! column of every secondary index, so this ORDER BY is served directly by
//! an index scan for fixed-prefix filters — no sort step. The walker is a
//! single append-only writer (`INSERT OR IGNORE`, no deletes), so a
//! `(slot, rowid)` position never moves.
//!
//! Caveat (by design): rowid order is global *insert* order, so a later
//! venue backfill can insert rows at slots an in-flight pagination already
//! passed. Pagination is a stream over the corpus as-of-passage; re-query
//! from `from_slot` for an authoritative read.

use anyhow::{Context, Result};
use rusqlite::Connection;
use rusqlite::types::Value;
use serde::Serialize;

/// Opaque-to-clients continuation point, encoded `"<slot>:<rowid>"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    pub slot: u64,
    pub rowid: i64,
}

impl Cursor {
    pub fn encode(&self) -> String {
        format!("{}:{}", self.slot, self.rowid)
    }

    pub fn parse(s: &str) -> Option<Self> {
        let (slot, rowid) = s.split_once(':')?;
        let slot: u64 = slot.parse().ok()?;
        let rowid: i64 = rowid.parse().ok()?;
        if rowid < 0 {
            return None;
        }
        Some(Cursor { slot, rowid })
    }
}

/// Validated `/events` filter (parsing/validation happens in `handlers.rs`).
#[derive(Debug, Default)]
pub struct EventFilter {
    pub venue: Option<String>,
    pub policy: Option<String>,
    pub fingerprint: Option<String>,
    pub asset_name_hex: Option<String>,
    /// db-form kind strings (validated via `EventKind::from_db_str`).
    pub kinds: Vec<&'static str>,
    pub from_slot: Option<u64>,
    pub to_slot: Option<u64>,
    pub cursor: Option<Cursor>,
    /// Page size (already clamped).
    pub limit: u32,
}

/// Text-form ledger row, as stored. Serialized directly for the
/// `?format=json` debug path.
#[derive(Debug, Clone, Serialize)]
pub struct LedgerRow {
    pub rowid: i64,
    pub tx_hash: String,
    pub policy_id: String,
    pub asset_name_hex: String,
    pub kind: String,
    pub price_lovelace: Option<u64>,
    pub buyer_price_lovelace: Option<u64>,
    pub seller_stake: Option<String>,
    pub buyer_stake: Option<String>,
    pub marketplace: String,
    pub bundle_size: Option<u32>,
    pub output_index: Option<u32>,
    pub fee_waived: bool,
    pub slot: u64,
    pub block_height: Option<u64>,
    pub block_time: i64,
    pub venue: String,
}

fn events_sql(f: &EventFilter) -> (String, Vec<Value>) {
    let mut sql = String::from(
        "SELECT rowid, tx_hash, policy_id, asset_name_hex, kind, price_lovelace,
                buyer_price_lovelace, seller_stake, buyer_stake, marketplace,
                bundle_size, output_index, fee_waived, slot, block_height, block_time, venue
         FROM market_events WHERE 1=1",
    );
    let mut params: Vec<Value> = Vec::new();

    if let Some(v) = &f.venue {
        sql.push_str(" AND venue = ?");
        params.push(v.clone().into());
    }
    if let Some(p) = &f.policy {
        sql.push_str(" AND policy_id = ?");
        params.push(p.clone().into());
    }
    if !f.kinds.is_empty() {
        let marks = vec!["?"; f.kinds.len()].join(",");
        sql.push_str(&format!(" AND kind IN ({marks})"));
        for k in &f.kinds {
            params.push(k.to_string().into());
        }
    }
    if let Some(fp) = &f.fingerprint {
        sql.push_str(" AND fingerprint = ?");
        params.push(fp.clone().into());
    }
    if let Some(n) = &f.asset_name_hex {
        sql.push_str(" AND asset_name_hex = ?");
        params.push(n.clone().into());
    }
    if let Some(s) = f.from_slot {
        sql.push_str(" AND slot >= ?");
        params.push((s as i64).into());
    }
    if let Some(s) = f.to_slot {
        sql.push_str(" AND slot <= ?");
        params.push((s as i64).into());
    }
    if let Some(c) = f.cursor {
        // Row-value keyset comparison (sqlite ≥ 3.15; the bundled build is
        // far newer).
        sql.push_str(" AND (slot, rowid) > (?, ?)");
        params.push((c.slot as i64).into());
        params.push(c.rowid.into());
    }

    sql.push_str(" ORDER BY slot, rowid LIMIT ?");
    // limit+1: the extra row detects whether a next page exists.
    params.push((i64::from(f.limit) + 1).into());
    (sql, params)
}

/// Run the filter; returns one page plus the cursor for the next (`None` =
/// end of results).
pub fn fetch_events(
    conn: &Connection,
    f: &EventFilter,
) -> Result<(Vec<LedgerRow>, Option<Cursor>)> {
    let (sql, params) = events_sql(f);
    let mut stmt = conn.prepare(&sql).context("preparing events query")?;
    let mapped = stmt.query_map(rusqlite::params_from_iter(params), |r| {
        Ok(LedgerRow {
            rowid: r.get(0)?,
            tx_hash: r.get(1)?,
            policy_id: r.get(2)?,
            asset_name_hex: r.get(3)?,
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
            block_time: r.get(15)?,
            venue: r.get(16)?,
        })
    })?;

    let mut rows: Vec<LedgerRow> = Vec::new();
    for row in mapped {
        rows.push(row?);
    }

    let next = if rows.len() > f.limit as usize {
        rows.pop();
        rows.last().map(|last| Cursor {
            slot: last.slot,
            rowid: last.rowid,
        })
    } else {
        None
    };
    Ok((rows, next))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store;

    /// In-memory ledger with the real schema and a small fixture corpus:
    /// policy A (jpg) has 4 events incl. two rows sharing slot 100 (bundle),
    /// policy B (wayup) has 2.
    fn fixture_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(store::SCHEMA).unwrap();
        let rows: &[(&str, &str, &str, &str, u64, &str)] = &[
            // (tx_hash, policy, name_hex, kind, slot, venue)
            ("t1", "aaa", "01", "listed", 90, "jpg"),
            ("t2", "aaa", "01", "sold", 100, "jpg"),
            ("t2", "aaa", "02", "sold", 100, "jpg"), // same slot, same tx (bundle)
            ("t3", "aaa", "01", "delisted", 120, "jpg"),
            ("t4", "bbb", "01", "listed", 95, "wayup"),
            ("t5", "bbb", "01", "sold", 130, "wayup"),
        ];
        for (tx, policy, name, kind, slot, venue) in rows {
            conn.execute(
                "INSERT INTO market_events (tx_hash, policy_id, asset_name_hex, fingerprint,
                    kind, marketplace, fee_waived, slot, block_time, source, venue)
                 VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, 'walk', ?)",
                rusqlite::params![
                    tx,
                    policy,
                    name,
                    format!("asset1{policy}{name}"),
                    kind,
                    "jpg.store",
                    *slot as i64,
                    *slot as i64 * 10,
                    venue
                ],
            )
            .unwrap();
        }
        conn
    }

    fn all(filter: EventFilter, conn: &Connection) -> Vec<String> {
        let (rows, next) = fetch_events(conn, &filter).unwrap();
        assert!(next.is_none());
        rows.iter()
            .map(|r| format!("{}/{}/{}", r.tx_hash, r.asset_name_hex, r.kind))
            .collect()
    }

    #[test]
    fn cursor_round_trip_and_rejects() {
        let c = Cursor {
            slot: 142_837_465,
            rowid: 9_912_034,
        };
        assert_eq!(Cursor::parse(&c.encode()), Some(c));
        for bad in ["", "abc", "1:2:3", "1:-2", ":", "5:", ":5"] {
            assert_eq!(Cursor::parse(bad), None, "should reject {bad:?}");
        }
    }

    #[test]
    fn filters_hit_expected_rows_in_order() {
        let conn = fixture_conn();
        let base = || EventFilter {
            limit: 100,
            ..Default::default()
        };

        // Policy + single kind.
        let f = EventFilter {
            policy: Some("aaa".into()),
            kinds: vec!["sold"],
            ..base()
        };
        assert_eq!(all(f, &conn), ["t2/01/sold", "t2/02/sold"]);

        // Venue filter.
        let f = EventFilter {
            venue: Some("wayup".into()),
            ..base()
        };
        assert_eq!(all(f, &conn), ["t4/01/listed", "t5/01/sold"]);

        // Multi-kind + slot window.
        let f = EventFilter {
            kinds: vec!["listed", "sold"],
            from_slot: Some(95),
            to_slot: Some(120),
            ..base()
        };
        assert_eq!(all(f, &conn), ["t4/01/listed", "t2/01/sold", "t2/02/sold"]);

        // Fingerprint addressing.
        let f = EventFilter {
            fingerprint: Some("asset1aaa02".into()),
            ..base()
        };
        assert_eq!(all(f, &conn), ["t2/02/sold"]);

        // Policy + name addressing.
        let f = EventFilter {
            policy: Some("bbb".into()),
            asset_name_hex: Some("01".into()),
            ..base()
        };
        assert_eq!(all(f, &conn), ["t4/01/listed", "t5/01/sold"]);
    }

    #[test]
    fn pagination_yields_every_row_exactly_once() {
        let conn = fixture_conn();
        let mut seen = Vec::new();
        let mut cursor = None;
        loop {
            let f = EventFilter {
                limit: 2,
                cursor,
                ..Default::default()
            };
            let (rows, next) = fetch_events(&conn, &f).unwrap();
            assert!(rows.len() <= 2);
            seen.extend(
                rows.iter()
                    .map(|r| format!("{}/{}/{}", r.tx_hash, r.asset_name_hex, r.kind)),
            );
            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        // All 6 rows, slot order, bundle rows split across pages without loss.
        assert_eq!(
            seen,
            [
                "t1/01/listed",
                "t4/01/listed",
                "t2/01/sold",
                "t2/02/sold",
                "t3/01/delisted",
                "t5/01/sold"
            ]
        );
    }

    #[test]
    fn exact_page_boundary_has_no_phantom_next() {
        let conn = fixture_conn();
        // Exactly 2 sold rows for policy aaa with limit 2 → next must be None.
        let f = EventFilter {
            policy: Some("aaa".into()),
            kinds: vec!["sold"],
            limit: 2,
            ..Default::default()
        };
        let (rows, next) = fetch_events(&conn, &f).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(next.is_none());
    }

    /// Guards against regressions to a full table scan: the policy+kind
    /// query must ride idx_me_policy_kind_slot.
    #[test]
    fn policy_kind_query_uses_index() {
        let conn = fixture_conn();
        let f = EventFilter {
            policy: Some("aaa".into()),
            kinds: vec!["sold"],
            limit: 10,
            ..Default::default()
        };
        let (sql, params) = events_sql(&f);
        let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        let plan: Vec<String> = stmt
            .query_map(rusqlite::params_from_iter(params), |r| {
                r.get::<_, String>(3)
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        let plan = plan.join(" | ");
        assert!(
            plan.contains("idx_me_policy_kind_slot"),
            "expected index scan, got: {plan}"
        );
    }
}
