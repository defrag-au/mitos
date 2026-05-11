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
use crate::wire::{
    ChainPoint, ClientMessage, InterestOp, ServerMessage, decode_server, encode_client,
};

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
    /// the constructor. Owned `Vec<Box<dyn MitosChannelDyn>>`;
    /// never mutated after construction so dispatch can borrow it
    /// without lifetime gymnastics.
    channels: Vec<Box<dyn MitosChannelDyn>>,
    /// Whether the runtime has run its one-time schema setup. Cheap
    /// flag to avoid re-running `ensure_schema` on every request.
    schema_ready: Cell<bool>,
}

impl<C: MitosCompanion> MitosCompanionRuntime<C> {
    /// Construct a runtime. The companion declares its subscribe
    /// targets via `MitosCompanion::subscribe_targets()` — default
    /// is one `SubscribeTarget::Module` with name = `C::NAME`, so
    /// classic single-wasm-module companions Just Work without
    /// overriding.
    ///
    /// Override `subscribe_targets()` on the companion to declare
    /// indexer-target or multi-target subscriptions. See
    /// `docs/design/UNIFIED_SUBSCRIBE.md`.
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

    /// Deprecated alias for `new`. Kept for backward compat with
    /// dApps that adopted the original step-2 API; remove on next
    /// breaking-change wave. New code should use `new(...)` and
    /// — if the default `SubscribeTarget::Module` isn't what they
    /// want — override `MitosCompanion::subscribe_targets()`.
    #[deprecated(
        since = "0.0.2",
        note = "use `MitosCompanionRuntime::new` and override `MitosCompanion::subscribe_targets()` if needed"
    )]
    pub fn module(state: State, env: Env, inner: C) -> Self {
        Self::new(state, env, inner)
    }

    /// Deprecated. Companions targeting in-tree indexers should
    /// `new(...)` and override `MitosCompanion::subscribe_targets()`
    /// to return `vec![SubscribeTarget::Indexer { name: ... }]`.
    /// This constructor doesn't actually enforce indexer-target
    /// behaviour — that's the companion's declaration.
    #[deprecated(
        since = "0.0.2",
        note = "use `MitosCompanionRuntime::new` and override `MitosCompanion::subscribe_targets()` to declare `Indexer` target"
    )]
    pub fn indexer(state: State, env: Env, inner: C) -> Self {
        Self::new(state, env, inner)
    }

    /// Borrow the dApp's environment. Useful for the wrapper if it
    /// needs to override or extend a method.
    pub fn env(&self) -> &Env {
        &self.env
    }

    /// Borrow the DO `State`. Used by the dApp's `#[durable_object]`
    /// wrapper for direct SQL access in read-API handlers (the
    /// runtime owns its own meta tables, but the dApp's read API
    /// queries its own application tables and needs the same
    /// `state.storage().sql()` handle the runtime is using).
    pub fn state(&self) -> &State {
        &self.state
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
            (Method::Get, "/api/_interest") => self.handle_interest_list(),
            (Method::Post, "/api/_interest/subscribe") => self.handle_interest_subscribe(req).await,
            (Method::Post, "/api/_interest/unsubscribe") => {
                self.handle_interest_unsubscribe(req).await
            }
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

    /// Look up a channel handler by tag.
    ///
    /// Tags come from `state.get_tags(ws)` set during WS upgrade
    /// — see `handle_replicate_upgrade`. Naming layers below the
    /// runtime don't always agree:
    ///
    /// - The legacy `IndexerHandle` path tags the WS with the
    ///   indexer name (e.g. `"collection-ownership"`).
    /// - The new emit-interception path stringifies the WIT u32
    ///   channel id (e.g. `"0"`) — the host doesn't know the
    ///   dApp's channel names, only their indices.
    /// - The default upgrade route uses `DEFAULT_CHANNEL_TAG`
    ///   (`"default"`).
    ///
    /// None of those match the dApp's `MitosChannel::NAME`
    /// values directly. v1 fallback: when the tag isn't a
    /// match, dispatch to the **first registered channel**.
    /// That's correct for single-channel dApps + matches the
    /// v1 emit-interception model where every event from
    /// channel 0 goes through the primary channel handler.
    /// Multi-channel routing where each channel gets only its
    /// own events is a v2 problem (will require the wasm module
    /// to declare its channel-name → u32 mapping at init time
    /// so the host can tag the dial-back URL appropriately).
    fn lookup_channel(&self, name: &str) -> Result<&dyn MitosChannelDyn> {
        if let Some(c) = self.channels.iter().find(|c| c.name() == name) {
            return Ok(c.as_ref());
        }
        if let Some(first) = self.channels.first() {
            tracing::debug!(
                requested = %name,
                fallback = %first.name(),
                "channel tag did not match any registered channel; falling back to first",
            );
            return Ok(first.as_ref());
        }
        Err(CompanionError::UnknownChannel(name.to_string()))
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

        // Pull the canonical interest set from DO SQLite. Sent to
        // the host as `Vec<mitos_protocol::Interest>` in the
        // subscribe payload — host persists this as the
        // companion's registration. (PR 3 wires the host's
        // dial-back to deliver matching emissions; until then,
        // this is the canonical-source-of-record handshake.)
        let interest_rows = crate::interest::list_interests(&sql)?;
        let interests = crate::interest::rows_to_interests(&interest_rows);

        // Pull the dial-back URL from wrangler env so mitos
        // knows where to open its outbound WS. Required for
        // emission delivery — without it the host persists
        // the registration but the dial loop logs and exits.
        let dial_back = self
            .env
            .var(crate::subscribe::MITOS_REPLICATE_URL_ENV)
            .ok()
            .map(|v| v.to_string())
            .map(|url| crate::subscribe::DialBackOverride {
                url: Some(url),
                auth_header: None,
                auth_value: None,
            });

        // Targets are declared by the companion via
        // `MitosCompanion::subscribe_targets()`. Default is one
        // `Module { name: C::NAME }` for backward compat with
        // single-wasm-module companions. Multi-target (e.g. one
        // wasm module + one in-tree indexer for the same DO) is
        // expressed by overriding the trait method.
        let targets = self.inner.subscribe_targets();
        let request = SubscribeRequest {
            targets,
            companion_key,
            resume_from,
            interests,
            dial_back,
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

    /// `GET /api/_interest` — list the companion's canonical
    /// interest set as JSON.
    fn handle_interest_list(&self) -> worker::Result<Response> {
        let sql = self.state.storage().sql();
        let rows = crate::interest::list_interests(&sql)?;
        Response::from_json(&crate::interest::InterestListResponse { interests: rows })
    }

    /// `POST /api/_interest/subscribe` — add a single interest
    /// row. Body: `InterestMutateRequest`. On success, emits a
    /// `ClientMessage::Interest { op: Add, items: [Interest] }`
    /// frame over the held WS so the host's running module picks
    /// up the new filter immediately. (Host-side WS-receive-loop
    /// wiring lands in PR 3 alongside the dial-back path; PR 2
    /// validates the companion-side surface end-to-end via
    /// the SQL row + WS-frame send.)
    async fn handle_interest_subscribe(&self, mut req: Request) -> worker::Result<Response> {
        let payload: crate::interest::InterestMutateRequest = req.json().await?;
        let channel = payload.channel.clone().unwrap_or_default();
        let added_at = current_rfc3339();

        let sql = self.state.storage().sql();
        crate::interest::add_interest(&sql, &payload.kind, &payload.value, &channel, &added_at)?;

        // Translate just-added row to wire `Interest` and emit
        // `ClientMessage::Interest { op: Add, items: [..] }` over
        // the held WS. Best-effort — if no WS is currently held
        // (DO not yet wakened with a live mitos connection), the
        // canonical SQL row stays committed and the next
        // reconnect-time `Replace` rehydrates the host.
        let row = crate::interest::InterestRow {
            kind: payload.kind.clone(),
            value: payload.value.clone(),
            channel: channel.clone(),
            added_at: added_at.clone(),
        };
        self.broadcast_interest_frame(InterestOp::Add, &row);

        Response::from_json(&crate::interest::InterestMutateResponse {
            op_result: "added".into(),
            kind: payload.kind,
            value: payload.value,
            channel,
        })
    }

    /// `POST /api/_interest/unsubscribe` — symmetric to subscribe.
    /// Body: `InterestMutateRequest`. Emits a
    /// `ClientMessage::Interest { op: Remove, items: [..] }`
    /// frame on success.
    async fn handle_interest_unsubscribe(&self, mut req: Request) -> worker::Result<Response> {
        let payload: crate::interest::InterestMutateRequest = req.json().await?;
        let channel = payload.channel.clone().unwrap_or_default();

        let sql = self.state.storage().sql();
        crate::interest::remove_interest(&sql, &payload.kind, &payload.value, &channel)?;

        let row = crate::interest::InterestRow {
            kind: payload.kind.clone(),
            value: payload.value.clone(),
            channel: channel.clone(),
            added_at: String::new(), // unused by rows_to_interests
        };
        self.broadcast_interest_frame(InterestOp::Remove, &row);

        Response::from_json(&crate::interest::InterestMutateResponse {
            op_result: "removed".into(),
            kind: payload.kind,
            value: payload.value,
            channel,
        })
    }

    /// Send `ClientMessage::Interest { op, items }` frames to the
    /// companion's held WS connections, filtering items per
    /// channel so multi-channel companions (PR 4) don't bleed
    /// ownership interests into a marketplace WS, etc.
    ///
    /// Routing rules:
    ///
    /// - **Channel-scoped row** (`row.channel == "ownership"`,
    ///   say): sent only to WSs tagged `ownership`.
    /// - **Empty channel** (`row.channel == NO_CHANNEL`): sent
    ///   to every held WS regardless of tag.
    ///
    /// On encode/send failure, logs and continues — the SQL row
    /// is the source of truth and the next reconnect-time
    /// `Replace` rehydrates the host.
    fn broadcast_interest_frame(&self, op: InterestOp, row: &crate::interest::InterestRow) {
        let rows = std::slice::from_ref(row);
        if row.channel.is_empty() {
            // Broadcast to every WS we hold. Single-channel
            // companions land here; so do explicit
            // cross-channel interests (NO_CHANNEL marker).
            self.send_interest_to(op, rows, &self.state.get_websockets());
            return;
        }
        // Channel-scoped: send only to WSs whose Hibernation tag
        // matches.
        let targeted = self.state.get_websockets_with_tag(&row.channel);
        if targeted.is_empty() {
            tracing::debug!(
                channel = %row.channel,
                "no WS held with this channel tag; companion will rehydrate on reconnect"
            );
            return;
        }
        self.send_interest_to(op, rows, &targeted);
    }

    fn send_interest_to(
        &self,
        op: InterestOp,
        rows: &[crate::interest::InterestRow],
        sockets: &[WebSocket],
    ) {
        let items = crate::interest::rows_to_interests(rows);
        if items.is_empty() {
            return;
        }
        let frame = ClientMessage::Interest { op, items };
        let bytes = match encode_client(&frame) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "encode Interest frame failed");
                return;
            }
        };
        for ws in sockets {
            if let Err(e) = ws.send_with_bytes(&bytes) {
                tracing::warn!(error = %e, "send Interest frame failed");
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

/// Current time as an RFC 3339 / ISO 8601 string. JavaScript's
/// `Date.toISOString()` returns the canonical RFC 3339 shape
/// (`2026-05-05T12:34:56.789Z`); we use that directly rather
/// than pulling chrono into a wasm32 build.
#[cfg(target_arch = "wasm32")]
fn current_rfc3339() -> String {
    let date = js_sys::Date::new_0();
    date.to_iso_string().as_string().unwrap_or_default()
}

/// Native fallback for non-wasm builds (tests run host-side).
/// Uses `SystemTime::now()` and a tiny formatter.
#[cfg(not(target_arch = "wasm32"))]
fn current_rfc3339() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let secs = (nanos / 1_000_000_000) as i64;
    let nsec = (nanos % 1_000_000_000) as u32;
    format_rfc3339_secs(secs, nsec)
}

#[cfg(not(target_arch = "wasm32"))]
fn format_rfc3339_secs(secs: i64, nsec: u32) -> String {
    // Minimal RFC 3339 formatter — sufficient for diagnostic
    // timestamps. Only used in native test builds; production
    // wasm path goes through the JS Date.toISOString() above.
    let days_per_400y: i64 = 365 * 400 + 97;
    let days_per_100y: i64 = 365 * 100 + 24;
    let days_per_4y: i64 = 365 * 4 + 1;
    let mut secs = secs;
    let mut days = secs / 86_400;
    secs -= days * 86_400;
    if secs < 0 {
        secs += 86_400;
        days -= 1;
    }
    let h = secs / 3600;
    let m = (secs / 60) % 60;
    let s = secs % 60;

    days += 11_017; // shift epoch to 2000-03-01
    let qc_cycles = days / days_per_400y;
    days %= days_per_400y;
    let mut c_cycles = days / days_per_100y;
    if c_cycles == 4 {
        c_cycles = 3;
    }
    days -= c_cycles * days_per_100y;
    let mut q_cycles = days / days_per_4y;
    if q_cycles == 25 {
        q_cycles = 24;
    }
    days -= q_cycles * days_per_4y;
    let mut remyears = days / 365;
    if remyears == 4 {
        remyears = 3;
    }
    days -= remyears * 365;

    let year = 2000 + remyears + 4 * q_cycles + 100 * c_cycles + 400 * qc_cycles;
    let months = [31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 31, 29];
    let mut mon = 0;
    let mut d = days;
    while mon < 12 && d >= months[mon] {
        d -= months[mon];
        mon += 1;
    }
    let (year, mon) = if mon >= 10 {
        (year + 1, mon - 9)
    } else {
        (year, mon + 3)
    };
    let day = (d + 1) as u32;

    format!(
        "{year:04}-{mon:02}-{day:02}T{h:02}:{m:02}:{s:02}.{nsec_ms:03}Z",
        nsec_ms = nsec / 1_000_000
    )
}

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
