//! `MitosCompanionRuntime<C>` — the runtime DO struct.
//!
//! Plain generic struct (no `#[durable_object]`, no `DurableObject`
//! impl). The dApp writes a non-generic `#[durable_object]` wrapper
//! per Companion type and forwards each DO method into the runtime
//! via `self.runtime.fetch(req).await` etc. See the design doc's
//! "Runtime DO shape" section for the canonical pattern + the
//! "Why composition, not a generic `#[durable_object]`" rationale.

use std::cell::Cell;

use worker::durable::State;
use worker::{Env, Headers, Method, Request, Response, WebSocket, WebSocketIncomingMessage};

use crate::ctx::Ctx;
use crate::error::{CompanionError, Result};
use crate::meta::{self, ensure_schema, migrate_split_row_cursor, write_cursor};
use crate::subscribe::SubscribeRequest;
use crate::traits::{MitosChannel, MitosChannelDyn, MitosCompanion};
use crate::wire::{ChainPoint, ClientMessage, ServerMessage, decode_server, encode_client};

/// Default WS Hibernation tag when an upgrade lands on
/// `/_internal/replicate` without a channel suffix. Single-channel
/// companions use this.
const DEFAULT_CHANNEL_TAG: &str = "default";

/// Companion runtime.
///
/// Embedded inside the dApp's `#[durable_object]` wrapper struct;
/// receives forwarded `fetch` / `websocket_message` / `websocket_close`
/// / `websocket_error` calls.
pub struct MitosCompanionRuntime<C: MitosCompanion> {
    state: State,
    env: Env,
    inner: C,
    /// Channel set, materialised eagerly from `inner.channels()` in
    /// `new()`. Owned `Vec<Box<dyn MitosChannelDyn>>`; never mutated
    /// after construction so dispatch can borrow it without lifetime
    /// gymnastics.
    channels: Vec<Box<dyn MitosChannelDyn>>,
    /// Whether the runtime has run its one-time schema setup. Cheap
    /// flag to avoid re-running `ensure_schema` on every request.
    schema_ready: Cell<bool>,
}

impl<C: MitosCompanion> MitosCompanionRuntime<C> {
    /// Construct a new runtime. Called by the dApp's
    /// `#[durable_object]` wrapper inside its `new(state, env)`
    /// constructor.
    pub fn new(state: State, env: Env, inner: C) -> Self {
        let channels = inner.channels();
        Self {
            state,
            env,
            inner,
            channels,
            schema_ready: Cell::new(false),
        }
    }

    /// Borrow the dApp's environment. Useful for the wrapper if it
    /// needs to override or extend a method.
    pub fn env(&self) -> &Env {
        &self.env
    }

    /// Borrow the inner companion. Useful for tests + RPC handler
    /// dispatch (PR 6 work).
    pub fn inner(&self) -> &C {
        &self.inner
    }

    // ========================================================================
    // DO method forwards
    // ========================================================================

    /// Forwarded from `DurableObject::fetch`. Routes by URL path.
    pub async fn fetch(&self, req: Request) -> worker::Result<Response> {
        self.ensure_runtime_ready()?;

        let url = req.url()?;
        let path = url.path().to_string();

        match (req.method(), path.as_str()) {
            (Method::Get, "/_internal/replicate") => self.handle_replicate_upgrade(&req, None),
            (Method::Get, p) if p.starts_with("/_internal/replicate-") => {
                let channel = p.trim_start_matches("/_internal/replicate-").to_string();
                self.handle_replicate_upgrade(&req, Some(channel))
            }
            (Method::Post, "/_internal/wake") => self.handle_wake().await,
            (Method::Get, "/api/_health") => self.handle_health(),
            (Method::Get, "/api/_meta") => self.handle_meta(),
            _ => Response::error("not found", 404),
        }
    }

    /// Forwarded from `DurableObject::websocket_message`. The
    /// load-bearing dispatch path: decode → route to channel →
    /// run `apply_event` → synchronous cursor advance → Ack/Nack.
    pub async fn websocket_message(
        &self,
        ws: WebSocket,
        msg: WebSocketIncomingMessage,
    ) -> worker::Result<()> {
        self.ensure_runtime_ready()?;

        let bytes = match msg {
            WebSocketIncomingMessage::Binary(b) => b,
            WebSocketIncomingMessage::String(s) => s.into_bytes(),
        };

        let server_msg = match decode_server(&bytes) {
            Ok(m) => m,
            Err(e) => {
                tracing::error!(error = %e, "wire decode failed; ignoring frame");
                return Ok(());
            }
        };

        if let Err(e) = self.dispatch_server_message(&ws, server_msg).await {
            tracing::error!(error = %e, "server message dispatch failed");
        }
        Ok(())
    }

    /// Forwarded from `DurableObject::websocket_close`. Logs the
    /// disconnect; mitos owns reconnect (Pattern X — see design doc
    /// "Wake-up: mitos drives all dial-ups").
    pub async fn websocket_close(
        &self,
        _ws: WebSocket,
        code: usize,
        reason: String,
        was_clean: bool,
    ) -> worker::Result<()> {
        tracing::info!(
            code,
            reason = %reason,
            was_clean,
            "WS closed; mitos will redial"
        );
        Ok(())
    }

    /// Forwarded from `DurableObject::websocket_error`. Logs.
    pub async fn websocket_error(&self, _ws: WebSocket, err: worker::Error) -> worker::Result<()> {
        tracing::warn!(error = %err, "WS error");
        Ok(())
    }

    // ========================================================================
    // Internal dispatch
    // ========================================================================

    /// One-time setup: schema creation, split-row cursor migration.
    /// Idempotent + cheap on subsequent calls (Cell flag short-circuits).
    fn ensure_runtime_ready(&self) -> Result<()> {
        if !self.schema_ready.get() {
            let sql = self.state.storage().sql();
            ensure_schema(&sql)?;
            migrate_split_row_cursor(&sql)?;
            self.schema_ready.set(true);
        }
        Ok(())
    }

    /// Dispatch a decoded `ServerMessage`. Errors here are
    /// logged-only — the runtime should keep streaming.
    async fn dispatch_server_message(&self, ws: &WebSocket, msg: ServerMessage) -> Result<()> {
        match msg {
            ServerMessage::Connected { last_emission_id } => {
                tracing::info!(last_emission_id, "mitos connected");
                Ok(())
            }
            ServerMessage::Apply {
                emission_id,
                cursor,
                change,
            } => {
                let channel = self
                    .state
                    .get_tags(ws)
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| DEFAULT_CHANNEL_TAG.to_string());
                self.dispatch_apply(ws, emission_id, cursor, channel, change)
                    .await
            }
            ServerMessage::Undo { cursor } => self.dispatch_undo(cursor).await,
            ServerMessage::Mark { cursor } => {
                let sql = self.state.storage().sql();
                write_cursor(&sql, &cursor)?;
                Ok(())
            }
            ServerMessage::SubscribeReply(reply) => {
                tracing::info!(?reply, "subscribe reply received");
                Ok(())
            }
            ServerMessage::Error { code, message } => {
                tracing::warn!(code = %code, message = %message, "host-side error frame");
                Ok(())
            }
        }
    }

    /// Apply-frame dispatch — the load-bearing happy path.
    ///
    /// Per Q3 of the design doc:
    /// 1. dApp's `apply_event` does any `.await` IO first, then
    ///    synchronous SQL writes.
    /// 2. Runtime synchronously appends a cursor advance — output
    ///    gate wraps both writes atomically.
    /// 3. Runtime sends Ack (success) or Nack (error) upstream
    ///    fire-and-forget after the gate flush.
    async fn dispatch_apply(
        &self,
        ws: &WebSocket,
        emission_id: u64,
        cursor: ChainPoint,
        channel: String,
        change: Vec<u8>,
    ) -> Result<()> {
        let channel_name = channel.clone();
        let channel_handler = self.lookup_channel(&channel_name)?;
        let sql = self.state.storage().sql();
        let ctx = Ctx::new(cursor.clone(), channel_name.clone(), sql.clone());

        let apply_result = channel_handler.apply_bytes(&ctx, &change).await;

        // Synchronous cursor advance (output gate wraps it with the
        // dApp's writes). Runs whether apply succeeded or not — per
        // Q5/Q7 design: cursor always advances to keep streaming.
        write_cursor(&sql, &cursor)?;

        let frame = match apply_result {
            Ok(()) => ClientMessage::Ack { emission_id },
            Err(e) => {
                tracing::warn!(
                    channel = %channel_name,
                    emission_id,
                    error = %e,
                    "apply_event failed; sending Nack"
                );
                ClientMessage::Nack {
                    emission_id,
                    error: e.to_string(),
                }
            }
        };
        let bytes = encode_client(&frame)?;
        ws.send_with_bytes(&bytes)
            .map_err(|e| CompanionError::Wire(format!("send ack/nack: {e}")))?;
        Ok(())
    }

    async fn dispatch_undo(&self, cursor: ChainPoint) -> Result<()> {
        // Default semantics: log + advance cursor. Channels' undo
        // hooks fire when reorg-aware dApps opt in. v1 fans the undo
        // call to all channels (broadcast) since the wire frame
        // doesn't carry a channel name; channels' default impl just
        // logs. Refine in PR 4 (multi-channel) if needed.
        let sql = self.state.storage().sql();
        for ch in &self.channels {
            let ctx = Ctx::new(cursor.clone(), ch.name().to_string(), sql.clone());
            if let Err(e) = ch.undo(&ctx, cursor.clone()).await {
                tracing::warn!(error = %e, channel = ch.name(), "undo handler failed");
            }
        }
        write_cursor(&sql, &cursor)?;
        Ok(())
    }

    fn lookup_channel(&self, name: &str) -> Result<&dyn MitosChannelDyn> {
        self.channels
            .iter()
            .find(|c| c.name() == name)
            .map(|c| c.as_ref())
            .ok_or_else(|| CompanionError::UnknownChannel(name.to_string()))
    }

    // ========================================================================
    // Route handlers
    // ========================================================================

    /// WS upgrade handler — accepts the inbound WS via the
    /// Hibernation API, tagged with the channel name so multi-channel
    /// companions can dispatch correctly on each `websocket_message`.
    fn handle_replicate_upgrade(
        &self,
        req: &Request,
        channel: Option<String>,
    ) -> worker::Result<Response> {
        if !is_websocket_upgrade(req) {
            return Response::error("expected WebSocket upgrade", 426);
        }
        let pair = worker::WebSocketPair::new()?;
        let tag = channel.as_deref().unwrap_or(DEFAULT_CHANNEL_TAG);
        self.state.accept_websocket_with_tags(&pair.server, &[tag]);
        Response::from_websocket(pair.client)
    }

    /// `/_internal/wake` — triggered by the dApp Worker during
    /// onboarding to materialise the DO and run the HTTPS subscribe
    /// call against mitos. Reads the persisted cursor from DO SQLite
    /// (Q4) and the cached interest set from `mitos_companion_interest`
    /// (PR 2 wires the dynamic-interest helpers; for PR 1 the interest
    /// set is empty), POSTs `SubscribeRequest` to the mitos host, and
    /// caches the result so subsequent wakes can short-circuit.
    ///
    /// dApp Worker pattern (per design doc "Bootstrapping" subsection):
    ///
    /// ```ignore
    /// let stub = env.OWNERSHIP_DO.id_from_name(&customer_id)?.get_stub()?;
    /// stub.fetch_with_str("/_internal/wake").await?;
    /// ```
    async fn handle_wake(&self) -> worker::Result<Response> {
        // Determine the companion key. PR 1 uses the DO's own name as
        // the Companion key — the dApp Worker creates the DO with
        // `id_from_name(companion_key)`, so `state.id().name()` is the
        // round-trip. (PR 5 will introduce a more flexible mechanism
        // when collections-mitos consolidates from per-policy to
        // per-customer keys.)
        let companion_key = self
            .state
            .id()
            .name()
            .map(|s| s.to_string())
            .unwrap_or_default();

        let sql = self.state.storage().sql();
        let resume_from = meta::read_cursor(&sql)?;

        let request = SubscribeRequest {
            module_name: C::NAME.to_string(),
            companion_key,
            resume_from,
            interests: Vec::new(), // PR 2 reads the dynamic interest table
            dial_back: None,
        };

        match subscribe_via_env(&self.env, &request).await {
            Ok(resp) => {
                tracing::info!(
                    next_emission_id = resp.next_emission_id,
                    "subscribe call succeeded"
                );
                Response::ok(format!(
                    "{{\"status\":\"{}\",\"next_emission_id\":{}}}",
                    resp.status, resp.next_emission_id
                ))
            }
            Err(e) => {
                tracing::error!(error = %e, "subscribe call failed");
                Response::error(format!("subscribe failed: {e}"), 502)
            }
        }
    }

    /// `/api/_health` — runtime-owned. Reports cursor + schema
    /// version so operators can probe liveness.
    fn handle_health(&self) -> worker::Result<Response> {
        let sql = self.state.storage().sql();
        let cursor_repr = match meta::read_cursor(&sql) {
            Ok(Some(p)) => format!("{p:?}"),
            Ok(None) => "none".into(),
            Err(e) => format!("error: {e}"),
        };
        let body = format!(
            "{{\"runtime_schema_version\":{},\"cursor\":\"{}\",\"companion\":\"{}\"}}",
            meta::RUNTIME_SCHEMA_VERSION,
            cursor_repr.replace('"', "'"),
            C::NAME
        );
        Response::ok(body).map(|r| {
            let headers = Headers::new();
            let _ = headers.set("content-type", "application/json");
            r.with_headers(headers)
        })
    }

    /// `/api/_meta` — runtime-owned. Reports companion name +
    /// channels.
    fn handle_meta(&self) -> worker::Result<Response> {
        let channels_json = self
            .channels
            .iter()
            .map(|c| format!("\"{}\"", c.name()))
            .collect::<Vec<_>>()
            .join(",");
        let body = format!(
            "{{\"companion\":\"{}\",\"channels\":[{}]}}",
            C::NAME,
            channels_json
        );
        Response::ok(body).map(|r| {
            let headers = Headers::new();
            let _ = headers.set("content-type", "application/json");
            r.with_headers(headers)
        })
    }
}

/// Compile-time witness that the `MitosChannel` trait stays usable
/// from this module. (Pulls the trait import in the hot path.)
#[allow(dead_code)]
fn _channel_trait_witness<T: MitosChannel>() {}

/// Wrap the wasm-only `subscribe::post_subscribe` so the runtime can
/// call it without `cfg` decoration at every call site. On non-wasm
/// targets this returns a "subscribe disabled in non-wasm builds"
/// error — those builds are tests-only and don't actually dial mitos.
#[cfg(target_arch = "wasm32")]
async fn subscribe_via_env(
    env: &Env,
    request: &SubscribeRequest,
) -> Result<crate::subscribe::SubscribeResponse> {
    crate::subscribe::post_subscribe(env, request).await
}

#[cfg(not(target_arch = "wasm32"))]
async fn subscribe_via_env(
    _env: &Env,
    _request: &SubscribeRequest,
) -> Result<crate::subscribe::SubscribeResponse> {
    Err(CompanionError::Wire(
        "subscribe_via_env: not supported on non-wasm targets".into(),
    ))
}

fn is_websocket_upgrade(req: &Request) -> bool {
    req.headers()
        .get("Upgrade")
        .ok()
        .flatten()
        .map(|v| v.eq_ignore_ascii_case("websocket"))
        .unwrap_or(false)
}
