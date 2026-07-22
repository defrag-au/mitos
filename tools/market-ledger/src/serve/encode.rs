//! Ledger rows → wire envelope.
//!
//! Interns policy / stake / marketplace / venue strings into the page's side
//! tables and converts hex text to raw bytes. Malformed hex or an unknown
//! kind here means corpus corruption (the walker wrote it), so it surfaces
//! as an error (→ 500), never a silent skip.

use std::collections::HashMap;

use anyhow::{Context, Result};
use market_ledger_wire::{EventKind, EventRow, EventsPage};

use super::query::{Cursor, LedgerRow};

fn intern(table: &mut Vec<String>, index: &mut HashMap<String, u32>, value: &str) -> u32 {
    if let Some(&i) = index.get(value) {
        return i;
    }
    let i = table.len() as u32;
    table.push(value.to_string());
    index.insert(value.to_string(), i);
    i
}

pub fn build_page(rows: Vec<LedgerRow>, next: Option<Cursor>) -> Result<EventsPage> {
    let mut page = EventsPage::empty();
    let mut policy_index: HashMap<String, u32> = HashMap::new();
    let mut stake_index: HashMap<String, u32> = HashMap::new();
    let mut marketplace_index: HashMap<String, u32> = HashMap::new();
    let mut venue_index: HashMap<String, u32> = HashMap::new();

    for row in &rows {
        let policy = match policy_index.get(&row.policy_id) {
            Some(&i) => i,
            None => {
                let bytes: [u8; 28] = hex::decode(&row.policy_id)
                    .ok()
                    .and_then(|b| b.try_into().ok())
                    .with_context(|| format!("policy_id not 28-byte hex: {}", row.policy_id))?;
                let i = page.policies.len() as u32;
                page.policies.push(bytes);
                policy_index.insert(row.policy_id.clone(), i);
                i
            }
        };

        let tx_hash: [u8; 32] = hex::decode(&row.tx_hash)
            .ok()
            .and_then(|b| b.try_into().ok())
            .with_context(|| format!("tx_hash not 32-byte hex: {}", row.tx_hash))?;
        let asset_name = hex::decode(&row.asset_name_hex)
            .with_context(|| format!("asset_name_hex not hex: {}", row.asset_name_hex))?;
        let kind = EventKind::from_db_str(&row.kind)
            .with_context(|| format!("unknown event kind: {}", row.kind))?;

        page.events.push(EventRow {
            tx_hash,
            policy,
            asset_name,
            kind,
            price_lovelace: row.price_lovelace,
            buyer_price_lovelace: row.buyer_price_lovelace,
            seller_stake: row
                .seller_stake
                .as_deref()
                .map(|s| intern(&mut page.stakes, &mut stake_index, s)),
            buyer_stake: row
                .buyer_stake
                .as_deref()
                .map(|s| intern(&mut page.stakes, &mut stake_index, s)),
            marketplace: intern(
                &mut page.marketplaces,
                &mut marketplace_index,
                &row.marketplace,
            ),
            venue: intern(&mut page.venues, &mut venue_index, &row.venue),
            bundle_size: row.bundle_size,
            output_index: row.output_index,
            fee_waived: row.fee_waived,
            slot: row.slot,
            block_height: row.block_height,
            block_time: row.block_time,
        });
    }

    page.next_cursor = next.map(|c| c.encode());
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(tx: u8, policy: u8, stake: &str) -> LedgerRow {
        LedgerRow {
            rowid: tx as i64,
            tx_hash: hex::encode([tx; 32]),
            policy_id: hex::encode([policy; 28]),
            asset_name_hex: "42756431".into(), // "Bud1"
            kind: "sold".into(),
            price_lovelace: Some(980_000_000),
            buyer_price_lovelace: Some(1_000_000_000),
            seller_stake: Some(stake.into()),
            buyer_stake: Some("stake1buyer".into()),
            marketplace: "wayup".into(),
            bundle_size: None,
            output_index: Some(0),
            fee_waived: false,
            slot: 100 + tx as u64,
            block_height: Some(50),
            block_time: 1_750_000_000,
            venue: "wayup".into(),
        }
    }

    #[test]
    fn interns_shared_strings_once() {
        let rows = vec![
            row(1, 9, "stake1seller"),
            row(2, 9, "stake1seller"),
            row(3, 9, "stake1other"),
        ];
        let page = build_page(
            rows,
            Some(Cursor {
                slot: 103,
                rowid: 3,
            }),
        )
        .unwrap();

        assert_eq!(page.version, market_ledger_wire::WIRE_VERSION);
        assert_eq!(page.policies, vec![[9u8; 28]]);
        // seller × 2 interned once + other seller + shared buyer.
        assert_eq!(
            page.stakes,
            vec!["stake1seller", "stake1buyer", "stake1other"]
        );
        assert_eq!(page.marketplaces, vec!["wayup"]);
        assert_eq!(page.venues, vec!["wayup"]);
        assert_eq!(page.events.len(), 3);
        assert_eq!(page.events[0].tx_hash, [1u8; 32]);
        assert_eq!(page.events[0].asset_name, b"Bud1");
        assert_eq!(page.events[1].seller_stake, Some(0));
        assert_eq!(page.events[2].seller_stake, Some(2));
        assert_eq!(page.events[2].buyer_stake, Some(1));
        assert_eq!(page.next_cursor.as_deref(), Some("103:3"));

        // And the whole page survives the wire.
        let bytes = market_ledger_wire::encode_events_page(&page).unwrap();
        assert_eq!(
            market_ledger_wire::decode_events_page(&bytes).unwrap(),
            page
        );
    }

    #[test]
    fn malformed_corpus_is_an_error() {
        let mut bad = row(1, 9, "stake1seller");
        bad.policy_id = "zz".into();
        assert!(build_page(vec![bad], None).is_err());

        let mut bad = row(1, 9, "stake1seller");
        bad.kind = "bogus".into();
        assert!(build_page(vec![bad], None).is_err());
    }
}
