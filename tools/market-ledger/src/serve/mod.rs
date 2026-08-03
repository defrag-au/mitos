//! `serve` — the hosted read surface over the ledger (phase 2).
//!
//! An axum HTTP service answering market-history queries in the compact
//! postcard wire format (`market-ledger-wire`); consumers PULL from it
//! rather than the walker pushing rows elsewhere. Raw-first per the design
//! doc: `/events` returns ledger rows; aggregates (DuckDB union over sealed
//! Parquet + live sqlite) come later behind the same `Db` seam.
//!
//! The tokio runtime lives entirely inside [`run`] — the rest of the binary
//! stays sync. The db is opened read-only per request (`db.rs`), so a
//! concurrent walk on the same ledger is safe (WAL, single writer).
//!
//! Surface: `GET /health` (open, JSON) · `GET /events` (bearer-gated via
//! `MARKET_LEDGER_TOKEN`, binary; `?format=json` debug escape hatch). Gzip
//! via `Accept-Encoding`; permissive CORS so browser/WASM consumers work
//! through a CF tunnel without a proxy hop.

mod auth;
mod db;
mod encode;
mod handlers;
mod query;

use std::path::PathBuf;

use anyhow::{Context, Result};
use axum::Router;
use axum::http::{Method, header};
use axum::routing::get;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{Any, CorsLayer};

#[derive(clap::Args, Debug)]
pub struct ServeArgs {
    /// Ledger sqlite path (opened read-only; a concurrent walk is fine).
    #[arg(long, default_value = "market-ledger.db")]
    db: PathBuf,
    /// Listen address. Keep it loopback; external access goes through a CF
    /// tunnel (mitos.defrag.cc pattern).
    #[arg(long, env = "MARKET_LEDGER_LISTEN", default_value = "127.0.0.1:8183")]
    listen: String,
    /// Sealed-parquet root — reserved for the DuckDB union view (unused in v1).
    #[arg(long, default_value = "parquet")]
    parquet_dir: PathBuf,
    /// Page size when ?limit is absent.
    #[arg(long, default_value_t = 1000)]
    default_limit: u32,
    /// Hard cap on ?limit.
    #[arg(long, default_value_t = 10_000)]
    max_limit: u32,
}

#[derive(Clone)]
pub struct AppState {
    db: db::Db,
    default_limit: u32,
    max_limit: u32,
}

fn router(state: AppState, token: auth::AuthToken) -> Router {
    // Auth layers the gated sub-router only, BEFORE the merge, so /health
    // stays open (the mitos-platform admin pattern).
    let gated = Router::new()
        .route("/events", get(handlers::events))
        .route("/count", get(handlers::count))
        .route("/listings", get(handlers::listings))
        .layer(axum::middleware::from_fn_with_state(
            token,
            auth::require_auth,
        ));
    Router::new()
        .route("/health", get(handlers::health))
        .merge(gated)
        .layer(CompressionLayer::new())
        .layer(
            // Wildcard origin is fine: bearer-in-header, no cookies, so no
            // credentialed requests.
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([Method::GET])
                .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE]),
        )
        .with_state(state)
}

pub fn run(args: ServeArgs) -> Result<()> {
    let token = auth::AuthToken::from_env();
    let state = AppState {
        db: db::Db::new(args.db.clone()),
        default_limit: args.default_limit,
        max_limit: args.max_limit,
    };
    let rt = tokio::runtime::Runtime::new().context("building tokio runtime")?;
    rt.block_on(async move {
        let app = router(state, token);
        let listener = tokio::net::TcpListener::bind(&args.listen)
            .await
            .with_context(|| format!("binding {}", args.listen))?;
        tracing::info!(
            listen = %args.listen,
            db = %args.db.display(),
            parquet_dir = %args.parquet_dir.display(),
            "market-ledger serve up"
        );
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = tokio::signal::ctrl_c().await;
                tracing::info!("shutting down");
            })
            .await
            .context("server error")
    })
}

#[cfg(test)]
mod tests {
    use std::future::IntoFuture;
    use std::net::SocketAddr;

    use market_ledger_wire::{EventKind, decode_events_page};

    use super::*;
    use crate::row::MarketEventRow;
    use crate::store::Ledger;

    const TOKEN: &str = "test-token";

    fn fixture_row(tx: u8, name: &str, kind: &str, slot: u64) -> MarketEventRow {
        MarketEventRow {
            tx_hash: hex::encode([tx; 32]),
            policy_id: hex::encode([0xaa; 28]),
            asset_name_hex: hex::encode(name.as_bytes()),
            fingerprint: Some(format!("asset1fixture{name}")),
            kind: kind.into(),
            price_lovelace: Some(980_000_000),
            buyer_price_lovelace: Some(1_000_000_000),
            seller_stake: Some("stake1seller".into()),
            buyer_stake: Some("stake1buyer".into()),
            marketplace: "wayup".into(),
            bundle_size: None,
            output_index: Some(0),
            fee_waived: false,
            slot,
            block_height: Some(slot / 20),
            block_time: slot + 1_596_059_091,
            venue: "wayup".into(),
        }
    }

    /// Spin the real server on an ephemeral port against a temp ledger and
    /// exercise the whole surface with a blocking client (which must run on
    /// this non-runtime thread).
    #[test]
    fn end_to_end_smoke() {
        let dir = std::env::temp_dir().join(format!("market-ledger-serve-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("ledger.db");
        let _ = std::fs::remove_file(&db_path);

        let mut ledger = Ledger::open(&db_path).unwrap();
        ledger
            .insert_events(&[
                fixture_row(1, "Bud1", "listed", 100),
                fixture_row(2, "Bud1", "sold", 200),
                fixture_row(3, "Bud2", "sold", 300),
            ])
            .unwrap();
        let policy_hex = hex::encode([0xaa; 28]);
        let listing = |name: &str, price: Option<u64>| crate::store::Listing {
            policy_id: policy_hex.clone(),
            asset_name_hex: hex::encode(name.as_bytes()),
            venue: "wayup".into(),
            price_lovelace: price,
            seller_stake: Some("stake1seller".into()),
            tx_hash: hex::encode([0x11; 32]),
            output_index: 0,
            listed_slot: 400,
            listed_time: 1_700_000_000,
        };
        ledger
            .apply_listing_ops(&[
                crate::store::ListingOp::Upsert(listing("Cheap", Some(20_000_000))),
                crate::store::ListingOp::Upsert(listing("Pricey", Some(90_000_000))),
                crate::store::ListingOp::Upsert(listing("Unpriced", None)),
            ])
            .unwrap();
        drop(ledger);

        let state = AppState {
            db: db::Db::new(db_path.clone()),
            default_limit: 1000,
            max_limit: 10_000,
        };
        let app = router(state, auth::AuthToken(Some(TOKEN.into())));

        let rt = tokio::runtime::Runtime::new().unwrap();
        let addr: SocketAddr = rt.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            rt.spawn(axum::serve(listener, app).into_future());
            addr
        });
        let base = format!("http://{addr}");
        let client = reqwest::blocking::Client::new();

        // /health is open and reflects the corpus.
        let health: serde_json::Value = client
            .get(format!("{base}/health"))
            .send()
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .unwrap();
        assert_eq!(health["rows"], 3);
        assert_eq!(health["slot_min"], 100);
        assert_eq!(health["slot_max"], 300);

        // /events without / with a bad bearer → 401.
        assert_eq!(
            client
                .get(format!("{base}/events"))
                .send()
                .unwrap()
                .status(),
            401
        );
        assert_eq!(
            client
                .get(format!("{base}/events"))
                .bearer_auth("wrong")
                .send()
                .unwrap()
                .status(),
            401
        );

        // Bad kind → 400 with a JSON error body.
        let resp = client
            .get(format!("{base}/events?kind=bogus"))
            .bearer_auth(TOKEN)
            .send()
            .unwrap();
        assert_eq!(resp.status(), 400);
        let err: serde_json::Value = resp.json().unwrap();
        assert!(err["error"].as_str().unwrap().contains("unknown kind"));

        // Binary path: filter to sold, page size 1, walk the cursor to
        // exhaustion, decoding each page.
        let policy = hex::encode([0xaa; 28]);
        let mut cursor: Option<String> = None;
        let mut sold_names: Vec<String> = Vec::new();
        loop {
            let mut url = format!("{base}/events?policy={policy}&kind=sold&limit=1");
            if let Some(c) = &cursor {
                url.push_str(&format!("&cursor={c}"));
            }
            let resp = client.get(url).bearer_auth(TOKEN).send().unwrap();
            assert_eq!(resp.status(), 200);
            assert_eq!(
                resp.headers()[reqwest::header::CONTENT_TYPE],
                "application/octet-stream"
            );
            let bytes = resp.bytes().unwrap();
            assert_eq!(bytes[0], market_ledger_wire::WIRE_VERSION);
            let page = decode_events_page(&bytes).unwrap();
            assert_eq!(page.policies, vec![[0xaa; 28]]);
            for e in &page.events {
                assert_eq!(e.kind, EventKind::Sold);
                sold_names.push(String::from_utf8(e.asset_name.clone()).unwrap());
            }
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        assert_eq!(sold_names, ["Bud1", "Bud2"]);

        // JSON debug escape hatch returns the text-form rows.
        let json: serde_json::Value = client
            .get(format!("{base}/events?kind=listed&format=json"))
            .bearer_auth(TOKEN)
            .send()
            .unwrap()
            .json()
            .unwrap();
        assert_eq!(json["events"].as_array().unwrap().len(), 1);
        assert_eq!(json["events"][0]["kind"], "listed");
        assert_eq!(json["next_cursor"], serde_json::Value::Null);

        // /listings — binary ListingsPage: cheapest-first, floor + count header.
        let resp = client
            .get(format!("{base}/listings?policy={policy_hex}"))
            .bearer_auth(TOKEN)
            .send()
            .unwrap();
        assert_eq!(resp.status(), 200);
        let page = market_ledger_wire::decode_listings_page(&resp.bytes().unwrap()).unwrap();
        assert_eq!(page.count, 3);
        assert_eq!(page.floor_lovelace, Some(20_000_000));
        assert_eq!(page.policies, vec![[0xaa; 28]]);
        let names: Vec<String> = page
            .listings
            .iter()
            .map(|l| String::from_utf8(l.asset_name.clone()).unwrap())
            .collect();
        assert_eq!(names, ["Cheap", "Pricey", "Unpriced"]); // priced ASC, unpriced last
        assert_eq!(page.listings[2].price_lovelace, None);
        // policy is required.
        assert_eq!(
            client
                .get(format!("{base}/listings"))
                .bearer_auth(TOKEN)
                .send()
                .unwrap()
                .status(),
            400
        );

        drop(rt); // shuts the server down
        let _ = std::fs::remove_dir_all(&dir);
    }
}
