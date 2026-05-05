//! `mitos-tail`: observability CLI for the CF replication
//! protocol.
//!
//! Connects to mitos's `/replicate/{indexer}` test surface as a
//! synthetic CBOR client, sends a Subscribe envelope, and prints
//! every record it receives. Useful for:
//!
//! - Local debugging during indexer development.
//! - Reorg validation: tail an indexer through a known historical
//!   reorg slot range and confirm the Apply/Undo sequence is
//!   well-formed (every Undo references a slot we've previously
//!   Applied, subsequent Apply records re-establish state).
//! - Cost validation: count records per minute as a baseline for
//!   the CF DO billing math in `CF_REPLICATION.md`.
//! - Brand-typed payload validation (Phase 6): connect with
//!   `--decode protocol-event` against the `marketplace` indexer
//!   and confirm `Marketplace::OfferCancel(OfferCancelPayload::*)`
//!   variants decode cleanly with the right brand identity.
//!
//! Output is human-readable by default; pass `--json` for one
//! JSON object per record (suitable for piping to `jq`).
//!
//! ## Scope JSON shape per indexer (post Phase 4)
//!
//! - `marketplace` and `collection-ownership` use
//!   `Scope = Vec<Interest>`. Example:
//!
//!   ```json
//!   [
//!     {
//!       "asset": { "Policy": "b3dab69f7e6100849434fb1781e34bd12a916557f6231b8d2629b6f6" },
//!       "domain": "Any",
//!       "value": "Any"
//!     }
//!   ]
//!   ```
//!
//!   "Match everything" is `[{"asset":"Any","domain":"Any","value":"Any"}]`.
//!
//! - `jpg-co` uses `Scope = ()`. Pass `--scope-json null` (or
//!   omit `--scope-json`).

use std::time::Instant;

use clap::{Parser, ValueEnum};
use futures_util::{SinkExt, StreamExt};
use mitos_core::{ClientMessage, ServerMessage, decode_server, encode_client};
use mitos_protocol::{
    ChainPoint, Domain, Marketplace, OfferCancelPayload, ProtocolEvent, SubscribeReply,
};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tracing::{error, info, warn};
use url::Url;

#[derive(Parser, Debug)]
#[command(
    version,
    about = "tail mitos's replication channel for an indexer/scope"
)]
struct Args {
    /// Mitos base URL (the bundle's listen address).
    #[arg(long, env = "MITOS_URL", default_value = "http://127.0.0.1:8080")]
    mitos: String,

    /// Indexer name, e.g. `collection-ownership` or `jpg-co`.
    #[arg(long)]
    indexer: String,

    /// Scope payload as JSON, encoded to CBOR before sending. The
    /// JSON shape must match the indexer's `Scope` type. For
    /// collection-ownership: `{"policy_id": "<hex>"}`.
    /// For indexers with `Scope = ()`: omit (sent as empty CBOR).
    #[arg(long)]
    scope_json: Option<String>,

    /// Resume cursor. `origin` for cold subscribe, otherwise
    /// `slot:hash_hex`.
    #[arg(long, default_value = "origin")]
    cursor: String,

    /// Bearer token for the auth header. Reads from
    /// `MITOS_AUTH_TOKEN` env if unset.
    #[arg(long, env = "MITOS_AUTH_TOKEN")]
    token: Option<String>,

    /// Emit one JSON object per record instead of human-readable
    /// log lines.
    #[arg(long)]
    json: bool,

    /// Stop after this many records. Useful for one-shot probes.
    #[arg(long)]
    max_records: Option<usize>,

    /// Validate the Apply/Undo invariants live: track every Apply's
    /// cursor, and warn if an Undo references a cursor we haven't
    /// seen Applied. Off by default (the CLI is also useful for raw
    /// observation).
    #[arg(long)]
    validate: bool,

    /// How to interpret each Apply's `change` bytes. `raw` (default)
    /// dumps the CBOR as hex — useful when an indexer's Change type
    /// isn't statically known to this tool. `protocol-event` decodes
    /// against `mitos_protocol::ProtocolEvent`, which is what the
    /// `marketplace` indexer (and any future `dex`/`lending`
    /// indexer) emits — surfaces brand + kind + brand-specific
    /// payload variant names.
    #[arg(long, value_enum, default_value_t = DecodeMode::Raw)]
    decode: DecodeMode,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum DecodeMode {
    Raw,
    ProtocolEvent,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .compact()
        .init();

    let args = Args::parse();
    let cursor = parse_cursor(&args.cursor)?;
    let scope_cbor = encode_scope(args.scope_json.as_deref())?;
    let url = build_ws_url(&args.mitos, &args.indexer)?;

    info!(
        url = %url,
        indexer = %args.indexer,
        scope_bytes = scope_cbor.len(),
        cursor = %args.cursor,
        validate = args.validate,
        "connecting"
    );

    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|e| anyhow::anyhow!("build request: {e}"))?;
    if let Some(token) = &args.token {
        request.headers_mut().insert(
            AUTHORIZATION,
            format!("Bearer {token}")
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid token (must be ASCII): {e}"))?,
        );
    }

    let (mut socket, _resp) = connect_async(request)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;

    let subscribe = ClientMessage::Subscribe {
        scope: scope_cbor,
        cursor,
    };
    let bytes = encode_client(&subscribe).map_err(|e| anyhow::anyhow!("encode: {e}"))?;
    socket
        .send(Message::Binary(bytes.into()))
        .await
        .map_err(|e| anyhow::anyhow!("send: {e}"))?;

    let mut state = TailState::new(args.json, args.validate, args.decode);
    let started = Instant::now();
    while let Some(frame) = socket.next().await {
        let msg = match frame {
            Ok(Message::Binary(b)) => match decode_server(&b) {
                Ok(m) => m,
                Err(e) => {
                    error!(error = %e, "decode failed; skipping frame");
                    continue;
                }
            },
            Ok(Message::Close(_)) => {
                info!("server closed connection");
                break;
            }
            Ok(_) => continue,
            Err(e) => {
                error!(error = %e, "ws error");
                break;
            }
        };

        state.observe(started.elapsed().as_secs_f64(), msg);
        if state.is_done(&args.max_records) {
            break;
        }
    }

    state.report();
    Ok(())
}

fn build_ws_url(base: &str, indexer: &str) -> anyhow::Result<Url> {
    let mut base = Url::parse(base)?;
    let scheme: String = match base.scheme() {
        "http" => "ws".into(),
        "https" => "wss".into(),
        s => s.into(),
    };
    base.set_scheme(&scheme)
        .map_err(|_| anyhow::anyhow!("set scheme to ws"))?;
    base.set_path(&format!("/replicate/{indexer}"));
    Ok(base)
}

fn parse_cursor(s: &str) -> anyhow::Result<ChainPoint> {
    if s == "origin" {
        return Ok(ChainPoint::Origin);
    }
    if let Some((slot, hash_hex)) = s.split_once(':') {
        let slot: u64 = slot.parse().map_err(|e| anyhow::anyhow!("bad slot: {e}"))?;
        let hash_bytes = hex::decode(hash_hex).map_err(|e| anyhow::anyhow!("bad hash hex: {e}"))?;
        if hash_bytes.len() != 32 {
            anyhow::bail!("hash must be 32 bytes; got {}", hash_bytes.len());
        }
        return Ok(ChainPoint::Specific(slot, hash_hex.to_string()));
    }
    let slot: u64 = s.parse().map_err(|e| anyhow::anyhow!("bad slot: {e}"))?;
    Ok(ChainPoint::Slot(slot))
}

fn encode_scope(scope_json: Option<&str>) -> anyhow::Result<Vec<u8>> {
    let value: serde_json::Value = match scope_json {
        Some(s) => serde_json::from_str(s).map_err(|e| anyhow::anyhow!("scope JSON parse: {e}"))?,
        None => serde_json::Value::Null,
    };
    let mut buf = Vec::with_capacity(64);
    ciborium::into_writer(&value, &mut buf).map_err(|e| anyhow::anyhow!("CBOR encode: {e}"))?;
    Ok(buf)
}

struct TailState {
    json: bool,
    validate: bool,
    decode: DecodeMode,
    seen_apply_cursors: std::collections::HashSet<String>,
    counts: Counts,
}

#[derive(Default)]
struct Counts {
    subscribe_reply: usize,
    apply: usize,
    undo: usize,
    mark: usize,
    error: usize,
    undo_without_prior_apply: usize,
}

impl TailState {
    fn new(json: bool, validate: bool, decode: DecodeMode) -> Self {
        Self {
            json,
            validate,
            decode,
            seen_apply_cursors: Default::default(),
            counts: Counts::default(),
        }
    }

    fn observe(&mut self, t: f64, msg: ServerMessage) {
        match msg {
            ServerMessage::SubscribeReply(reply) => {
                self.counts.subscribe_reply += 1;
                if self.json {
                    self.print_json("subscribe_reply", &SubscribeReplyView(&reply));
                } else {
                    info!(t = format_args!("{t:.3}"), ?reply, "subscribe reply");
                }
            }
            ServerMessage::Connected { last_emission_id } => {
                info!(t = format_args!("{t:.3}"), last_emission_id, "connected");
            }
            ServerMessage::Apply {
                emission_id: _,
                cursor,
                change,
            } => {
                self.counts.apply += 1;
                let key = cursor_key(&cursor);
                if self.validate {
                    self.seen_apply_cursors.insert(key.clone());
                }
                self.print_apply(t, &cursor, &key, &change);
            }
            ServerMessage::Undo { cursor } => {
                self.counts.undo += 1;
                let key = cursor_key(&cursor);
                if self.validate && !self.seen_apply_cursors.contains(&key) {
                    self.counts.undo_without_prior_apply += 1;
                    warn!(cursor = %key, "Undo at cursor we never Applied — protocol invariant violation?");
                }
                if self.json {
                    self.print_json(
                        "undo",
                        &serde_json::json!({ "cursor": cursor_summary(&cursor) }),
                    );
                } else {
                    warn!(t = format_args!("{t:.3}"), cursor = %key, "UNDO");
                }
            }
            ServerMessage::Mark { cursor } => {
                self.counts.mark += 1;
                if self.json {
                    self.print_json(
                        "mark",
                        &serde_json::json!({ "cursor": cursor_summary(&cursor) }),
                    );
                } else {
                    tracing::debug!(t = format_args!("{t:.3}"), cursor = %cursor_key(&cursor), "mark");
                }
            }
            ServerMessage::Error { code, message } => {
                self.counts.error += 1;
                error!(code = %code, message = %message, "server error");
                if self.json {
                    self.print_json(
                        "error",
                        &serde_json::json!({ "code": code, "message": message }),
                    );
                }
            }
        }
    }

    fn print_apply(&self, t: f64, cursor: &ChainPoint, key: &str, change: &[u8]) {
        match self.decode {
            DecodeMode::Raw => {
                if self.json {
                    self.print_json(
                        "apply",
                        &serde_json::json!({
                            "cursor": cursor_summary(cursor),
                            "change_bytes_hex": hex::encode(change),
                        }),
                    );
                } else {
                    info!(
                        t = format_args!("{t:.3}"),
                        cursor = %key,
                        change_bytes = change.len(),
                        "apply"
                    );
                }
            }
            DecodeMode::ProtocolEvent => match decode_protocol_event(change) {
                Ok(event) => {
                    let summary = protocol_event_summary(&event);
                    if self.json {
                        self.print_json(
                            "apply",
                            &serde_json::json!({
                                "cursor": cursor_summary(cursor),
                                "event": &event,
                                "summary": summary,
                            }),
                        );
                    } else {
                        info!(
                            t = format_args!("{t:.3}"),
                            cursor = %key,
                            policy = %event.policy_id.as_str(),
                            asset = event.asset_name_hex.as_deref().unwrap_or("<policy-wide>"),
                            event = %summary,
                            "apply"
                        );
                    }
                }
                Err(e) => {
                    error!(
                        cursor = %key,
                        error = %e,
                        bytes = change.len(),
                        "decode ProtocolEvent failed; falling back to raw"
                    );
                    if self.json {
                        self.print_json(
                            "apply",
                            &serde_json::json!({
                                "cursor": cursor_summary(cursor),
                                "decode_error": e.to_string(),
                                "change_bytes_hex": hex::encode(change),
                            }),
                        );
                    }
                }
            },
        }
    }

    fn is_done(&self, max: &Option<usize>) -> bool {
        if let Some(n) = max {
            self.counts.apply + self.counts.undo + self.counts.mark >= *n
        } else {
            false
        }
    }

    fn print_json<T: serde::Serialize>(&self, kind: &str, payload: &T) {
        let line = serde_json::json!({ "kind": kind, "payload": payload });
        println!("{line}");
    }

    fn report(&self) {
        info!(
            apply = self.counts.apply,
            undo = self.counts.undo,
            mark = self.counts.mark,
            error = self.counts.error,
            undo_without_prior_apply = self.counts.undo_without_prior_apply,
            "tail summary"
        );
    }
}

fn cursor_key(c: &ChainPoint) -> String {
    match c {
        ChainPoint::Origin => "origin".into(),
        ChainPoint::Slot(s) => format!("{s}"),
        ChainPoint::Specific(s, h) => format!("{s}:{h}"),
    }
}

fn cursor_summary(c: &ChainPoint) -> serde_json::Value {
    match c {
        ChainPoint::Origin => serde_json::json!({ "kind": "origin" }),
        ChainPoint::Slot(s) => serde_json::json!({ "kind": "slot", "slot": s }),
        ChainPoint::Specific(s, h) => serde_json::json!({
            "kind": "specific",
            "slot": s,
            "hash_hex": h,
        }),
    }
}

impl serde::Serialize for SubscribeReplyView<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            SubscribeReply::Resume { cursor } => {
                let mut m = serde_json::Map::new();
                m.insert("kind".into(), "resume".into());
                m.insert("cursor".into(), cursor_summary(cursor));
                m.serialize(s)
            }
            SubscribeReply::SnapshotRedirect {
                snapshot_url,
                snapshot_cursor,
            } => {
                let mut m = serde_json::Map::new();
                m.insert("kind".into(), "snapshot_redirect".into());
                m.insert("snapshot_url".into(), snapshot_url.clone().into());
                m.insert("snapshot_cursor".into(), cursor_summary(snapshot_cursor));
                m.serialize(s)
            }
            SubscribeReply::Fork { common_ancestor } => {
                let mut m = serde_json::Map::new();
                m.insert("kind".into(), "fork".into());
                m.insert("common_ancestor".into(), cursor_summary(common_ancestor));
                m.serialize(s)
            }
        }
    }
}

struct SubscribeReplyView<'a>(&'a SubscribeReply);

fn decode_protocol_event(bytes: &[u8]) -> anyhow::Result<ProtocolEvent> {
    ciborium::from_reader(bytes).map_err(|e| anyhow::anyhow!("decode ProtocolEvent: {e}"))
}

/// One-line human summary of a `ProtocolEvent`. Surfaces the
/// brand + kind for marketplace events, plus the divergent payload
/// arm name for `OfferCancel` so JpgStore/Wayup/Unknown round-trip
/// is observable in the log line.
fn protocol_event_summary(event: &ProtocolEvent) -> String {
    match &event.domain {
        Domain::Marketplace(m) => {
            let kind = format!("{:?}", m.kind());
            let brand = format!("{:?}", m.brand());
            match m {
                Marketplace::OfferCancel(c) => {
                    let arm = match c {
                        OfferCancelPayload::JpgStore {
                            redeemer,
                            script_ref,
                            ..
                        } => format!(
                            "JpgStore(redeemer={}, script_ref={})",
                            redeemer.as_ref().map_or("None", |_| "Some"),
                            script_ref.as_ref().map_or("None", |_| "Some"),
                        ),
                        OfferCancelPayload::Wayup { redeemer, .. } => format!(
                            "Wayup(redeemer={})",
                            redeemer.as_ref().map_or("None", |_| "Some"),
                        ),
                        OfferCancelPayload::Unknown { brand_script, .. } => {
                            format!("Unknown(script={brand_script})")
                        }
                    };
                    format!("Marketplace::OfferCancel/{brand} -> {arm}")
                }
                _ => format!("Marketplace::{kind}/{brand}"),
            }
        }
        Domain::Dex(d) => format!("Dex::{:?}/{:?}", d.kind(), d.brand()),
        Domain::Lending(l) => format!("Lending::{:?}/{:?}", l.kind(), l.brand()),
    }
}

#[cfg(test)]
mod tests {
    //! Phase 6 round-trip smoke tests. Synthesise the kinds of
    //! `ProtocolEvent` mitos emits, encode them through the same
    //! ciborium pipeline the bundle's emitter uses, decode via
    //! `decode_protocol_event` (the consumer-side path), and assert
    //! the summary surfaces brand identity intact. Catches any
    //! mismatch between the indexer and consumer wire shapes
    //! without requiring a live mitos instance.

    use super::*;
    use cardano_assets::{AssetId, PolicyId};
    use mitos_protocol::{
        Marketplace, MarketplaceBrand, OfferCancelPayload, PlutusBytes, ProtocolEvent, SalePayload,
    };
    use pipeline_types::PricedAsset;

    const POLICY: &str = "b3dab69f7e6100849434fb1781e34bd12a916557f6231b8d2629b6f6";
    const ASSET_NAME: &str = "426c61636b466c6167303031";

    fn encode(event: &ProtocolEvent) -> Vec<u8> {
        let mut buf = Vec::new();
        ciborium::into_writer(event, &mut buf).unwrap();
        buf
    }

    #[test]
    fn jpgstore_offer_cancel_round_trip_keeps_brand_arm() {
        let event = ProtocolEvent {
            policy_id: PolicyId::new(POLICY).unwrap(),
            asset_name_hex: Some(ASSET_NAME.into()),
            tx_hash: "ab".repeat(32),
            slot: 100,
            domain: Domain::Marketplace(Marketplace::OfferCancel(OfferCancelPayload::JpgStore {
                policy_id: PolicyId::new(POLICY).unwrap(),
                asset_name_hex: Some(ASSET_NAME.into()),
                bidder: "addr1bidder".into(),
                redeemer: None,
                script_ref: None,
            })),
        };
        let bytes = encode(&event);
        let decoded = decode_protocol_event(&bytes).expect("decode");
        let summary = protocol_event_summary(&decoded);
        assert!(
            summary.contains("OfferCancel") && summary.contains("JpgStore"),
            "summary {summary:?} should mention both OfferCancel and JpgStore"
        );
    }

    #[test]
    fn wayup_offer_cancel_round_trip_surfaces_redeemer_presence() {
        let event = ProtocolEvent {
            policy_id: PolicyId::new(POLICY).unwrap(),
            asset_name_hex: None,
            tx_hash: "cd".repeat(32),
            slot: 200,
            domain: Domain::Marketplace(Marketplace::OfferCancel(OfferCancelPayload::Wayup {
                policy_id: PolicyId::new(POLICY).unwrap(),
                asset_name_hex: None,
                bidder: "addr1bidder".into(),
                redeemer: Some(PlutusBytes::new(vec![0x01, 0x02, 0x03])),
            })),
        };
        let bytes = encode(&event);
        let decoded = decode_protocol_event(&bytes).expect("decode");
        let summary = protocol_event_summary(&decoded);
        assert!(
            summary.contains("Wayup") && summary.contains("redeemer=Some"),
            "summary {summary:?} should mention Wayup with redeemer=Some"
        );
    }

    #[test]
    fn unknown_marketplace_offer_cancel_preserves_script_string() {
        let event = ProtocolEvent {
            policy_id: PolicyId::new(POLICY).unwrap(),
            asset_name_hex: None,
            tx_hash: "ef".repeat(32),
            slot: 300,
            domain: Domain::Marketplace(Marketplace::OfferCancel(OfferCancelPayload::Unknown {
                brand_script: "addr1unknown_marketplace_test".into(),
                policy_id: PolicyId::new(POLICY).unwrap(),
                raw: None,
            })),
        };
        let bytes = encode(&event);
        let decoded = decode_protocol_event(&bytes).expect("decode");
        let summary = protocol_event_summary(&decoded);
        assert!(
            summary.contains("Unknown") && summary.contains("addr1unknown_marketplace_test"),
            "summary {summary:?} should preserve the unknown script string for triage"
        );
    }

    #[test]
    fn sale_event_summary_carries_brand() {
        let asset = AssetId::new(POLICY.into(), ASSET_NAME.into()).unwrap();
        let event = ProtocolEvent {
            policy_id: PolicyId::new(POLICY).unwrap(),
            asset_name_hex: Some(ASSET_NAME.into()),
            tx_hash: "01".repeat(32),
            slot: 400,
            domain: Domain::Marketplace(Marketplace::Sale(SalePayload {
                brand: MarketplaceBrand::JpgStore,
                asset: PricedAsset::with_price(asset, 50_000_000),
                seller: "addr1seller".into(),
                buyer: "addr1buyer".into(),
                royalty_lovelace: None,
            })),
        };
        let bytes = encode(&event);
        let decoded = decode_protocol_event(&bytes).expect("decode");
        let summary = protocol_event_summary(&decoded);
        assert!(
            summary.contains("Sale") && summary.contains("JpgStore"),
            "summary {summary:?} should surface Sale + JpgStore"
        );
    }
}
