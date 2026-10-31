//! `events` — the ledger read as `chain-events`.
//!
//! The marketplace axis of the achievements store has to answer "ever did X" over a
//! collection's **whole life**, so it needs history, not a subscription. This ledger is the
//! only source that has it: `walk` reads the venues from the certified immutable DB, and
//! `follow` keeps the same ledger tip-current. This mode is the last step — rows → one
//! [`ChainEvent`] each — and it reuses both halves of the existing path: `serve`'s
//! [`build_page`] to intern the rows into a wire page, and
//! [`chain_events_cardano::market::Collection::from_market_page`] to translate it. A row
//! exported here and the same row fetched over HTTP are therefore the same page, which is
//! what stops the two readers of one ledger from drifting.
//!
//! # What it does not carry
//!
//! **The transaction's position in its block.** Neither the ledger schema nor the postcard
//! wire stores `tx_index`, so [`TxRef::index`](chain_events::TxRef) is `0` — the same
//! "the source does not say" convention the OpenSea adapter uses. It is a coordinate and is
//! not hashed into `EventId`, so a future producer that does know the position can fill it
//! in without rewriting an id.
//!
//! # Coverage
//!
//! `coverage` on every event is the operator's statement about the walk, because the ledger
//! cannot prove it: `complete` says the walk began at or before the collection's first
//! market fact, `--partial` says it did not. The tool prints the first and last slot it
//! wrote so the claim can be checked against the collection's mint slot.

use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use chain_events::{ChainEvent, ChainRef, Coverage};
use chain_events_cardano::Collection;
use rusqlite::Connection;

use crate::serve::db::Db;
use crate::serve::encode::build_page;
use crate::serve::query::{EventFilter, fetch_events};

#[derive(clap::Args, Debug)]
pub struct EventsArgs {
    /// Ledger sqlite path (opened read-only; a concurrent walk or follow is fine).
    #[arg(long, default_value = "market-ledger.db")]
    db: PathBuf,
    /// Policy id, 56 hex characters. Repeat for several collections.
    #[arg(long = "policy", value_name = "HEX", required = true)]
    policies: Vec<String>,
    /// Directory to write `<policy>.ndjson` into. Omit to write stdout.
    #[arg(long)]
    out: Option<PathBuf>,
    /// Rows per ledger page. Bounds memory; it does not change what is written.
    #[arg(long, default_value_t = 5_000)]
    page: u32,
    /// The `recorded_at` every event carries, as unix seconds. Defaults to now.
    #[arg(long)]
    recorded_at: Option<u64>,
    /// Say the walk began after this collection's first market fact.
    #[arg(long)]
    partial: bool,
}

/// What one policy's export wrote.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Written {
    pub events: u64,
    pub unrepresentable: u64,
    pub malformed: u64,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
}

pub fn run(args: EventsArgs) -> Result<()> {
    let recorded_at = args.recorded_at.unwrap_or_else(now);
    let coverage = if args.partial {
        Coverage::Partial
    } else {
        Coverage::Complete
    };
    let conn = Db::new(args.db.clone()).open_ro()?;

    for policy in &args.policies {
        let collection = Collection::new(ChainRef::cardano_mainnet(), policy, coverage)
            .with_context(|| format!("policy {policy} is not a 56-hex Cardano policy id"))?;
        announce(policy, coverage, recorded_at);

        let written = match &args.out {
            None => {
                let stdout = std::io::stdout();
                let mut out = BufWriter::new(stdout.lock());
                let written = stream(&conn, &collection, args.page, recorded_at, &mut out)?;
                out.flush().context("flushing the event stream")?;
                written
            }
            Some(dir) => {
                std::fs::create_dir_all(dir)
                    .with_context(|| format!("creating {}", dir.display()))?;
                let path = dir.join(format!("{policy}.ndjson"));
                let mut out = BufWriter::new(
                    std::fs::File::create(&path)
                        .with_context(|| format!("creating {}", path.display()))?,
                );
                let written = stream(&conn, &collection, args.page, recorded_at, &mut out)?;
                out.flush()
                    .with_context(|| format!("flushing {}", path.display()))?;
                eprintln!("# wrote {}", path.display());
                written
            }
        };
        report(&written);
    }
    Ok(())
}

/// Paged read → page → translation → NDJSON.
///
/// Takes a `&Connection` rather than a path so the test drives a real schema over an
/// in-memory database, which is the only way to pin that the SQL and the wire agree.
pub(crate) fn stream(
    conn: &Connection,
    collection: &Collection,
    page: u32,
    recorded_at: u64,
    out: &mut dyn Write,
) -> Result<Written> {
    let mut written = Written::default();
    let mut cursor = None;
    loop {
        let filter = EventFilter {
            policy: Some(collection.policy().to_owned()),
            cursor,
            limit: page.max(1),
            ..Default::default()
        };
        let (rows, next) = fetch_events(conn, &filter)?;
        if rows.is_empty() {
            break;
        }

        let day = build_page(rows, next)?;
        let translated = collection.from_market_page(&day, recorded_at);
        written.unrepresentable += translated.unrepresentable.len() as u64;
        written.malformed += translated.malformed as u64;

        for event in &translated.events {
            write_event(out, event)?;
            written.events += 1;
            if let Some(coord) = event.coord {
                written.first_slot.get_or_insert(coord.height);
                written.last_slot = Some(coord.height);
            }
        }

        match next {
            Some(at) => cursor = Some(at),
            None => break,
        }
    }
    Ok(written)
}

fn write_event(out: &mut dyn Write, event: &ChainEvent) -> Result<()> {
    serde_json::to_writer(&mut *out, event).context("serialising a chain event")?;
    out.write_all(b"\n").context("writing a chain event")?;
    Ok(())
}

fn announce(policy: &str, coverage: Coverage, recorded_at: u64) {
    eprintln!(
        "# chain-events · {} · {policy}\n\
         # family: market · coverage: {}\n\
         # source: market-ledger walk, replayed · recorded_at {recorded_at} · \
         tx position is absent from the ledger and is written as 0",
        ChainRef::cardano_mainnet(),
        match coverage {
            Coverage::Complete => "complete",
            Coverage::Partial => "partial",
        }
    );
}

fn report(written: &Written) {
    match (written.first_slot, written.last_slot) {
        (Some(first), Some(last)) => eprintln!(
            "# market events: {} · {} unrepresentable · {} malformed · slots {first}..{last}",
            written.events, written.unrepresentable, written.malformed
        ),
        _ => eprintln!(
            "# market events: 0 · {} unrepresentable · {} malformed · no rows for this policy",
            written.unrepresentable, written.malformed
        ),
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::SCHEMA;

    const POLICY: &str = "e6ba9c0ff27be029442c32533c6efd956a60d15ecb976acbb64c4de0";
    const ASSET: &str = "5065727030303031";

    /// A bech32 string over `bytes`, with a placeholder checksum. Nothing here verifies one;
    /// the point is that `account_key` can read it.
    fn bech32(hrp: &str, bytes: &[u8]) -> String {
        const CHARSET: &[u8] = b"qpzry9x8gf2tvdw0s3jn54khce6mua7l";
        let (mut acc, mut bits, mut out) = (0u32, 0u32, String::from(hrp));
        for &b in bytes {
            acc = (acc << 8) | u32::from(b);
            bits += 8;
            while bits >= 5 {
                bits -= 5;
                out.push(CHARSET[((acc >> bits) & 31) as usize] as char);
            }
        }
        if bits > 0 {
            out.push(CHARSET[((acc << (5 - bits)) & 31) as usize] as char);
        }
        out + "qqqqqq"
    }

    fn stake(credential: u8) -> String {
        let mut bytes = vec![0xe1];
        bytes.extend([credential; 28]);
        bech32("stake1", &bytes)
    }

    fn ledger() -> Connection {
        let conn = Connection::open_in_memory().expect("in-memory ledger");
        conn.execute_batch(SCHEMA).expect("the ledger schema");
        conn
    }

    #[allow(clippy::too_many_arguments)]
    fn insert(
        conn: &Connection,
        tx: &str,
        kind: &str,
        price: Option<i64>,
        seller: Option<&str>,
        buyer: Option<&str>,
        output_index: Option<i64>,
        slot: i64,
    ) {
        conn.execute(
            "INSERT INTO market_events
               (tx_hash, policy_id, asset_name_hex, kind, price_lovelace, buyer_price_lovelace,
                seller_stake, buyer_stake, marketplace, bundle_size, output_index, slot,
                block_time, source, venue)
             VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?6, ?7, 'jpg.store', NULL, ?8, ?9, ?10, 'walk',
                     'jpg.store')",
            rusqlite::params![
                tx,
                POLICY,
                ASSET,
                kind,
                price,
                seller,
                buyer,
                output_index,
                slot,
                slot,
            ],
        )
        .expect("inserting a ledger row");
    }

    fn collection() -> Collection {
        Collection::new(ChainRef::cardano_mainnet(), POLICY, Coverage::Complete).unwrap()
    }

    fn events(conn: &Connection, page: u32) -> Vec<ChainEvent> {
        let mut buffer = Vec::new();
        let written = stream(conn, &collection(), page, 1_700_000_000, &mut buffer).unwrap();
        assert!(written.malformed == 0, "no row may be malformed");
        buffer
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).expect("each line is a chain event"))
            .collect()
    }

    #[test]
    fn a_sale_and_a_listing_become_two_transitions_in_slot_order() {
        let conn = ledger();
        // Inserted newest-first: the order of the export comes from the ledger, not from
        // the order a caller happened to write rows in.
        insert(
            &conn,
            &"22".repeat(32),
            "listed",
            Some(1_200_000_000),
            Some(&stake(0xaa)),
            None,
            Some(3),
            2_000,
        );
        insert(
            &conn,
            &"11".repeat(32),
            "sold",
            Some(980_000_000),
            Some(&stake(0xaa)),
            Some(&stake(0xbb)),
            Some(0),
            1_000,
        );

        let events = events(&conn, 5_000);
        assert_eq!(events.len(), 2);
        assert!(
            events
                .iter()
                .all(|e| e.body.family() == chain_events::Family::Market)
        );
        assert_eq!(events[0].coord.unwrap().height, 1_000);
        assert_eq!(events[1].coord.unwrap().height, 2_000);

        let chain_events::EventBody::Market(chain_events::MarketEvent::Filled {
            side,
            price,
            parties,
            ..
        }) = &events[0].body
        else {
            panic!("a `sold` row is a fill, got {:?}", events[0].body);
        };
        assert_eq!(*side, chain_events::Side::Ask);
        assert_eq!(price.as_ref().map(|p| p.amount), Some(980_000_000));
        assert_eq!(parties.len(), 2);
        // Keyed by the stake credential, the same way a holder movement is.
        assert_eq!(parties[0].account.address, "aa".repeat(28));
        assert_eq!(parties[1].account.address, "bb".repeat(28));

        let chain_events::EventBody::Market(chain_events::MarketEvent::Placed {
            order, price, ..
        }) = &events[1].body
        else {
            panic!("a `listed` row is a placement, got {:?}", events[1].body);
        };
        // A listing carries its own output in its own transaction, so `tx:index` is a real
        // UTxO — the one order reference Cardano can state.
        assert_eq!(order.id, format!("{}:3", "22".repeat(32)));
        assert_eq!(price.amount, 1_200_000_000);
    }

    #[test]
    fn the_transaction_is_named_and_its_position_is_stated_as_absent() {
        let conn = ledger();
        insert(
            &conn,
            &"11".repeat(32),
            "sold",
            Some(1),
            Some(&stake(0xaa)),
            Some(&stake(0xbb)),
            Some(0),
            1_000,
        );
        let events = events(&conn, 5_000);
        let tx = events[0]
            .tx
            .as_ref()
            .expect("a market row names its transaction");
        assert_eq!(tx.hash, "11".repeat(32));
        assert_eq!(tx.index, 0, "the ledger does not store the position");
    }

    #[test]
    fn paging_does_not_change_what_is_written() {
        let conn = ledger();
        for (index, slot) in [1_000, 2_000, 3_000].into_iter().enumerate() {
            insert(
                &conn,
                &format!("{:02x}", index + 1).repeat(32),
                "delisted",
                None,
                Some(&stake(0xaa)),
                None,
                Some(0),
                slot,
            );
        }
        let one_page = events(&conn, 5_000);
        let per_row = events(&conn, 1);
        assert_eq!(one_page.len(), 3);
        assert_eq!(
            one_page, per_row,
            "a page boundary must not change an event or its order"
        );
    }

    #[test]
    fn another_policys_rows_are_not_exported() {
        let conn = ledger();
        insert(
            &conn,
            &"11".repeat(32),
            "sold",
            Some(1),
            Some(&stake(0xaa)),
            Some(&stake(0xbb)),
            Some(0),
            1_000,
        );
        conn.execute(
            "INSERT INTO market_events
               (tx_hash, policy_id, asset_name_hex, kind, price_lovelace, seller_stake,
                marketplace, output_index, slot, block_time, source, venue)
             VALUES (?1, ?2, ?3, 'listed', 5, ?4, 'wayup', 0, 900, 900, 'walk', 'wayup')",
            rusqlite::params!["33".repeat(32), "aa".repeat(28), ASSET, stake(0xcc)],
        )
        .unwrap();

        let events = events(&conn, 5_000);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].coord.unwrap().height, 1_000);
    }

    #[test]
    fn a_policy_with_no_market_history_writes_nothing_and_says_so() {
        let conn = ledger();
        let mut buffer = Vec::new();
        let written = stream(&conn, &collection(), 5_000, 1, &mut buffer).unwrap();
        assert!(buffer.is_empty());
        assert_eq!(
            written,
            Written {
                events: 0,
                unrepresentable: 0,
                malformed: 0,
                first_slot: None,
                last_slot: None,
            }
        );
    }
}
