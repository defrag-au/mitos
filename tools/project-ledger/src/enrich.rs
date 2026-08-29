//! `enrich` — copy the policy's venue sales in from market-ledger.
//!
//! ## The gap this closes
//!
//! The walk can see that an asset moved and what a wallet's net was. It cannot
//! see that the movement was a SALE, because a sale has no distinguishing shape
//! on chain: an asset leaves, some lovelace arrives, and so does change. Worse,
//! in a marketplace sale the seller supplies the asset UTxO and therefore FUNDS
//! the transaction, so `book_unit_flows` correctly declines to book the
//! proceeds — an output back to a funder is change, and that rule is what stops
//! every wallet from appearing to pay itself. The result read as an asset
//! handed over for nothing, which is the exact inverse of what happened.
//!
//! market-ledger already decodes the venues. Joining it in by `tx_hash` turns
//! "an asset left and 57.2 ₳ arrived" into "sold for 68 ₳ on wayup", which is
//! the difference between a suspicious give-away and an ordinary trade.
//!
//! ## What it CANNOT tell you
//!
//! **Only venue sales exist here.** A peer-to-peer sale — two wallets agreeing
//! directly — leaves no marketplace event, so it is absent from this table and
//! stays indistinguishable from a gift. Enrichment narrows the unknown; it does
//! not close it, and a reader must not read "no sale row" as "not sold".
//!
//! `seller_stake` is also frequently EMPTY on collection offers, because the
//! venue does not record it. That is not a defect to paper over: the walk knows
//! who sent the asset from `asset_event.from_party`, so the two sources are
//! complementary and the gap is left visible rather than guessed at.
//!
//! ## Why a separate pass
//!
//! `secondary_sale` is interpretation, not observation, and the design has
//! always kept those apart: it is recomputable without a re-walk. A `reset`
//! clears it along with everything else derived, so re-run this after a walk —
//! it costs seconds against a walk's quarter hour.

use std::path::PathBuf;

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};

use crate::store::Ledger;

/// Marketplace event kinds that transfer an asset for value.
///
/// Listings, delistings, offers and price changes are all NON-events for this
/// purpose — an asset that was listed and never sold has moved nothing. Only
/// these three settle.
const SALE_KINDS: [&str; 3] = ["sold", "offer_accepted", "collection_offer_accepted"];

#[derive(clap::Args, Debug)]
pub struct EnrichArgs {
    #[arg(long, default_value = "project-ledger.db")]
    pub db: PathBuf,

    /// market-ledger's sqlite database.
    #[arg(long)]
    pub market: PathBuf,
}

struct Sale {
    tx_hash: String,
    asset_name: String,
    venue: String,
    price: Option<i64>,
    seller: Option<String>,
    buyer: Option<String>,
    slot: i64,
    policy_id: String,
}

pub fn run(args: &EnrichArgs) -> Result<()> {
    let mut ledger = Ledger::open(&args.db)?;
    let policy = ledger
        .meta_get("policy_id")?
        .context("ledger has no policy_id — seed it first")?;

    // READ-ONLY, and its own connection. market-ledger.db is written live by
    // `market-ledger-follow`; this pass must not be able to take a write lock
    // on a running service's database.
    let market = Connection::open_with_flags(
        &args.market,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .with_context(|| format!("opening market ledger {}", args.market.display()))?;

    let placeholders = SALE_KINDS.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT tx_hash, asset_name_hex, venue, price_lovelace, seller_stake, buyer_stake, slot,
                policy_id
         FROM market_events
         WHERE policy_id = ? AND kind IN ({placeholders})"
    );
    let mut stmt = market.prepare(&sql).context("preparing the market query")?;
    let mut binds: Vec<&str> = vec![policy.as_str()];
    binds.extend(SALE_KINDS);
    let rows = stmt.query_map(rusqlite::params_from_iter(binds), read_sale)?;
    let sales: Vec<Sale> = rows.collect::<rusqlite::Result<Vec<_>>>()?;

    if sales.is_empty() {
        tracing::warn!(
            policy,
            market = %args.market.display(),
            "enrich: the market ledger holds no sales for this policy — wrong database, \
             or it has not walked this collection"
        );
        return Ok(());
    }

    let written = ledger.put_secondary_sales(&sales_rows(&sales))?;

    // SAY WHAT IS STILL UNKNOWN. A count of sales found reads as a count of
    // sales that HAPPENED, and the difference is every peer-to-peer trade.
    let no_seller = sales.iter().filter(|s| s.seller.is_none()).count();
    let no_price = sales.iter().filter(|s| s.price.is_none()).count();
    tracing::info!(
        found = sales.len(),
        written,
        without_seller = no_seller,
        without_price = no_price,
        "enrich: venue sales copied in"
    );
    tracing::warn!(
        "enrich: VENUE SALES ONLY. A peer-to-peer sale leaves no marketplace event and is \
         absent here — 'no sale row' does NOT mean 'not sold'."
    );

    foreign_sales(&mut ledger, &market, &policy)?;
    Ok(())
}

/// The watched parties' venue trades in OTHER collections.
///
/// A watched wallet buying an unrelated NFT is ordinary shopping the app hides
/// by default — but it is recorded rather than dropped, because a marketplace
/// hop between two colluding wallets is also a serviceable way to move funds
/// while looking like shopping, so the rows must stay surfaceable.
///
/// One full scan of the market ledger's sale rows with the party test done in
/// Rust: the party set is a few hundred keys, and a scan beats teaching SQLite
/// an `OR (buyer IN … OR seller IN …)` it cannot index anyway.
fn foreign_sales(ledger: &mut Ledger, market: &Connection, case_policy: &str) -> Result<()> {
    let parties: std::collections::HashSet<String> = ledger.party_keys()?.into_iter().collect();

    let placeholders = SALE_KINDS.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT tx_hash, asset_name_hex, venue, price_lovelace, seller_stake, buyer_stake, slot,
                policy_id
         FROM market_events
         WHERE policy_id <> ? AND kind IN ({placeholders})"
    );
    let mut stmt = market
        .prepare(&sql)
        .context("preparing the foreign-sale scan")?;
    let mut binds: Vec<&str> = vec![case_policy];
    binds.extend(SALE_KINDS);
    let rows = stmt.query_map(rusqlite::params_from_iter(binds), read_sale)?;

    let mut foreign: Vec<Sale> = Vec::new();
    for row in rows {
        let sale = row?;
        let ours = |p: &Option<String>| p.as_deref().is_some_and(|p| parties.contains(p));
        if ours(&sale.buyer) || ours(&sale.seller) {
            foreign.push(sale);
        }
    }

    let written = ledger.put_secondary_sales(&sales_rows(&foreign))?;
    tracing::info!(
        found = foreign.len(),
        written,
        "enrich: foreign-collection venue trades involving watched parties — the app \
         hides these by default; toggle them on to check the marketplace-as-transfer angle"
    );
    Ok(())
}

fn read_sale(r: &rusqlite::Row<'_>) -> rusqlite::Result<Sale> {
    Ok(Sale {
        tx_hash: r.get(0)?,
        asset_name: r.get(1)?,
        venue: r.get(2)?,
        price: r.get(3)?,
        // The venue does not always record a seller — an empty string is
        // absence, not a party whose key happens to be "".
        seller: r.get::<_, Option<String>>(4)?.filter(|s| !s.is_empty()),
        buyer: r.get::<_, Option<String>>(5)?.filter(|s| !s.is_empty()),
        slot: r.get(6)?,
        policy_id: r.get(7)?,
    })
}

fn sales_rows(sales: &[Sale]) -> Vec<crate::store::SecondarySaleRow> {
    sales
        .iter()
        .map(|s| crate::store::SecondarySaleRow {
            tx_hash: s.tx_hash.clone(),
            asset_name: s.asset_name.clone(),
            venue: s.venue.clone(),
            price_lovelace: s.price,
            seller: s.seller.clone(),
            buyer: s.buyer.clone(),
            slot: s.slot,
            policy_id: Some(s.policy_id.clone()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Listings and offers are not sales. Counting them would inflate "was it
    /// paid for" with assets that were merely advertised.
    #[test]
    fn only_settling_kinds_count_as_sales() {
        assert!(SALE_KINDS.contains(&"sold"));
        assert!(SALE_KINDS.contains(&"offer_accepted"));
        assert!(SALE_KINDS.contains(&"collection_offer_accepted"));
        for non_sale in ["listed", "delisted", "offer_created", "price_change"] {
            assert!(!SALE_KINDS.contains(&non_sale), "{non_sale} is not a sale");
        }
    }

    #[test]
    fn an_empty_venue_seller_becomes_absence_not_a_party() {
        let s = Sale {
            tx_hash: "tx".into(),
            asset_name: "a".into(),
            venue: "wayup".into(),
            price: Some(68_000_000),
            seller: None,
            buyer: Some("stake1buyer".into()),
            slot: 1,
            policy_id: "policyaaaa".into(),
        };
        let rows = sales_rows(&[s]);
        assert_eq!(rows[0].seller, None);
        assert_eq!(rows[0].price_lovelace, Some(68_000_000));
        // The policy always rides along — "which collection sold" is what
        // separates case evidence from a watched wallet's ordinary shopping.
        assert_eq!(rows[0].policy_id.as_deref(), Some("policyaaaa"));
    }
}
