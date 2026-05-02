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
//!
//! Output is human-readable by default; pass `--json` for one
//! JSON object per record (suitable for piping to `jq`).

use std::time::Instant;

use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use mitos_core::{
    ChainPoint, ClientMessage, ServerMessage, SubscribeReply, decode_server, encode_client,
};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tracing::{error, info, warn};
use url::Url;

#[derive(Parser, Debug)]
#[command(version, about = "tail mitos's replication channel for an indexer/scope")]
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

    let mut state = TailState::new(args.json, args.validate);
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
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&hash_bytes);
        let hash = mitos_core::BlockHash::from(arr);
        return Ok(ChainPoint::Specific(slot, hash));
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
    fn new(json: bool, validate: bool) -> Self {
        Self {
            json,
            validate,
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
            ServerMessage::Apply { cursor, change } => {
                self.counts.apply += 1;
                let key = cursor_key(&cursor);
                if self.validate {
                    self.seen_apply_cursors.insert(key.clone());
                }
                if self.json {
                    self.print_json(
                        "apply",
                        &serde_json::json!({
                            "cursor": cursor_summary(&cursor),
                            "change_bytes_hex": hex::encode(&change),
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
            ServerMessage::Undo { cursor } => {
                self.counts.undo += 1;
                let key = cursor_key(&cursor);
                if self.validate && !self.seen_apply_cursors.contains(&key) {
                    self.counts.undo_without_prior_apply += 1;
                    warn!(cursor = %key, "Undo at cursor we never Applied — protocol invariant violation?");
                }
                if self.json {
                    self.print_json("undo", &serde_json::json!({ "cursor": cursor_summary(&cursor) }));
                } else {
                    warn!(t = format_args!("{t:.3}"), cursor = %key, "UNDO");
                }
            }
            ServerMessage::Mark { cursor } => {
                self.counts.mark += 1;
                if self.json {
                    self.print_json("mark", &serde_json::json!({ "cursor": cursor_summary(&cursor) }));
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
        ChainPoint::Specific(s, h) => format!("{s}:{}", hex::encode(h)),
    }
}

fn cursor_summary(c: &ChainPoint) -> serde_json::Value {
    match c {
        ChainPoint::Origin => serde_json::json!({ "kind": "origin" }),
        ChainPoint::Slot(s) => serde_json::json!({ "kind": "slot", "slot": s }),
        ChainPoint::Specific(s, h) => serde_json::json!({
            "kind": "specific",
            "slot": s,
            "hash_hex": hex::encode(h),
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
