//! `MitosCompanionRuntime<C>` — the runtime DO struct.
//!
//! Plain generic struct (no `#[durable_object]`, no `DurableObject`
//! impl). The dApp writes a non-generic `#[durable_object]` wrapper
//! per Companion type and forwards `fetch` calls into the runtime
//! via `self.runtime.fetch(req).await`. See the design doc's
//! "Runtime DO shape" section for the canonical pattern.
//!
//! ## HTTP delivery (post-WS migration)
//!
//! The runtime exposes two HTTP POST endpoints for mitos's outbound
//! delivery:
//!
//! - `POST /_internal/apply-<channel>?key=<companion_key>` — one
//!   emission per request. Body is a CBOR-encoded
//!   [`mitos_protocol::ApplyBody`]. The runtime decodes, dispatches
//!   to the matching `MitosChannel::apply_event` via `apply_bytes`,
//!   advances the persisted cursor, and returns:
//!   - 200 OK with empty body on success (Ack)
//!   - 422 Unprocessable with the error text on `apply_event` Err
//!     (Nack — won't succeed on naive retry)
//!   - 5xx for transport/runtime errors (dialer retries)
//! - `POST /_internal/recapture-<channel>?key=<companion_key>` —
//!   triggers the dApp's `on_recapture` hook. Body is a CBOR-encoded
//!   [`mitos_protocol::RecaptureBody`]. Returns 200 once the dApp's
//!   cleanup completes (= RecaptureReady) or 500 if the hook errors.
//!
//! The wrapper DO only needs to forward `fetch`. WS-related methods
//! (`websocket_message`, `websocket_close`, `websocket_error`) are
//! no longer part of the surface — workers should remove their
//! overrides.

use std::cell::Cell;

use worker::durable::State;
use worker::{Env, Headers, Method, Request, Response};

use crate::ctx::Ctx;
use crate::error::{CompanionError, Result};
use crate::meta::{self, ensure_schema, migrate_split_row_cursor, write_cursor};
use crate::subscribe::SubscribeRequest;
use crate::traits::{MitosChannel, MitosChannelDyn, MitosCompanion};
use mitos_protocol::{
    ApplyBody, ApplyBulkResponse, BulkEmissionResult, ChainPoint, Interest, InterestOp,
    RecaptureBody, UndoBody, decode_apply, decode_apply_bulk, decode_recapture, decode_undo,
    encode_apply_bulk_response,
};

/// Companion runtime.
///
/// Embedded inside the dApp's `#[durable_object]` wrapper struct
/// and receives forwarded `fetch` calls. WS lifecycle methods are
/// no longer part of the surface — see the module-level docs.
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

    /// Borrow the inner companion. Useful for tests + dApp-level
    /// dispatch where the runtime needs to reach into the trait
    /// impl directly (e.g. RPC handlers the dApp owns).
    pub fn inner(&self) -> &C {
        &self.inner
    }

    // ========================================================================
    // DO method forwards
    // ========================================================================

    /// Forwarded from `DurableObject::fetch`. Routes by URL path.
    ///
    /// Owns:
    /// - `POST /_internal/apply-<channel>?key=...` — emission delivery
    /// - `POST /_internal/recapture-<channel>?key=...` — recapture trigger
    /// - `POST /_internal/wake` — operator-triggered re-subscribe
    /// - `GET /api/_health`, `GET /api/_meta` — introspection
    /// - `GET|POST /api/_interest[/subscribe|/unsubscribe]` — interest mutation
    pub async fn fetch(&self, req: Request) -> worker::Result<Response> {
        self.ensure_runtime_ready()?;

        let url = req.url()?;
        let path = url.path().to_string();

        match (req.method(), path.as_str()) {
            // Bulk arm must precede the generic apply arm — a bulk
            // URL also starts with `/_internal/apply-`. Strip both the
            // prefix and the `-bulk` suffix to recover the channel.
            (Method::Post, p) if p.starts_with("/_internal/apply-") && p.ends_with("-bulk") => {
                let channel = p
                    .trim_start_matches("/_internal/apply-")
                    .trim_end_matches("-bulk")
                    .to_string();
                self.handle_apply_bulk_post(req, channel).await
            }
            (Method::Post, p) if p.starts_with("/_internal/apply-") => {
                let channel = p.trim_start_matches("/_internal/apply-").to_string();
                self.handle_apply_post(req, channel).await
            }
            (Method::Post, p) if p.starts_with("/_internal/recapture-") => {
                let channel = p.trim_start_matches("/_internal/recapture-").to_string();
                self.handle_recapture_post(req, channel).await
            }
            (Method::Post, p) if p.starts_with("/_internal/undo-") => {
                let channel = p.trim_start_matches("/_internal/undo-").to_string();
                self.handle_undo_post(req, channel).await
            }
            (Method::Post, "/_internal/wake") => self.handle_wake().await,
            (Method::Post, "/_internal/teardown") => self.handle_interest_teardown().await,
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

    /// Apply HTTP handler — the load-bearing happy path.
    ///
    /// Decodes the CBOR body, dispatches to the channel handler,
    /// advances the persisted cursor, and translates the result
    /// to an HTTP status:
    /// - `Ok(())` → 200 OK with empty body
    /// - `Err(e)` → 422 Unprocessable Entity with the error text
    ///   (semantic Nack — dApp's `apply_event` errored)
    ///
    /// Cursor always advances regardless of apply outcome — per
    /// Q5/Q7 of the design doc (keep streaming; Nacked rows are
    /// surfaced via the host's emissions log for operator review).
    async fn handle_apply_post(
        &self,
        mut req: Request,
        channel: String,
    ) -> worker::Result<Response> {
        let bytes = req
            .bytes()
            .await
            .map_err(|e| worker::Error::RustError(format!("read apply body: {e}")))?;
        let body = match decode_apply(&bytes) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(channel = %channel, error = %e, "decode ApplyBody failed");
                return Response::error(format!("decode: {e}"), 400);
            }
        };

        let ApplyBody {
            emission_id,
            cursor,
            change,
        } = body;

        let channel_handler = match self.lookup_channel(&channel) {
            Ok(h) => h,
            Err(e) => {
                tracing::error!(channel = %channel, error = %e, "no handler for channel");
                return Response::error(format!("unknown channel: {e}"), 404);
            }
        };
        let sql = self.state.storage().sql();
        let ctx = Ctx::new(cursor.clone(), channel.clone(), sql.clone());

        let apply_result = channel_handler.apply_bytes(&ctx, &change).await;

        // Cursor advance happens regardless of apply outcome.
        if let Err(e) = write_cursor(&sql, &cursor) {
            tracing::error!(
                channel = %channel,
                emission_id,
                error = %e,
                "cursor advance failed; reporting 5xx so dialer retries"
            );
            return Response::error(format!("cursor advance: {e}"), 500);
        }

        match apply_result {
            Ok(()) => Response::empty(),
            Err(e) => {
                tracing::warn!(
                    channel = %channel,
                    emission_id,
                    error = %e,
                    "apply_event failed; returning 422 (Nack equivalent)"
                );
                Response::error(format!("{e}"), 422)
            }
        }
    }

    /// Bulk apply HTTP handler — the batched analogue of
    /// [`Self::handle_apply_post`]. Decodes an `ApplyBulkRequest`,
    /// applies each emission **in slice order** via the same
    /// `apply_bytes` path, and returns 200 with one
    /// `BulkEmissionResult` per emission.
    ///
    /// Semantics mirror the per-row path exactly:
    /// - A per-emission apply error → `applied: false` + error text
    ///   (the bulk analogue of a 422 Nack); the loop keeps going.
    /// - The persisted cursor advances **once**, to the last
    ///   emission's cursor, regardless of individual outcomes
    ///   (same "always advance" policy as the single path).
    /// - A decode failure or empty batch → 400.
    /// - An unknown channel → 404 (lets the host's capability probe
    ///   distinguish "no bulk route" — which is a path 404 from
    ///   `fetch` — from "bulk route exists, unknown channel").
    /// - A cursor-write failure → 500 (whole batch retried).
    ///
    /// Idempotency: none here. Applies are chain-point-idempotent at
    /// the dApp handler layer (a v1 runtime contract), so a retry of
    /// an already-applied batch converges. See
    /// `docs/design/DIALER_BULK_APPLY.md`.
    async fn handle_apply_bulk_post(
        &self,
        mut req: Request,
        channel: String,
    ) -> worker::Result<Response> {
        let bytes = req
            .bytes()
            .await
            .map_err(|e| worker::Error::RustError(format!("read apply-bulk body: {e}")))?;
        let batch = match decode_apply_bulk(&bytes) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(channel = %channel, error = %e, "decode ApplyBulkRequest failed");
                return Response::error(format!("decode: {e}"), 400);
            }
        };
        if batch.emissions.is_empty() {
            return Response::error("empty bulk batch", 400);
        }

        let channel_handler = match self.lookup_channel(&channel) {
            Ok(h) => h,
            Err(e) => {
                tracing::error!(channel = %channel, error = %e, "no handler for channel");
                return Response::error(format!("unknown channel: {e}"), 404);
            }
        };
        let sql = self.state.storage().sql();

        let mut results = Vec::with_capacity(batch.emissions.len());
        let mut last_cursor: Option<ChainPoint> = None;
        let mut applied = 0usize;
        let mut rejected = 0usize;
        for emission in &batch.emissions {
            let ctx = Ctx::new(emission.cursor.clone(), channel.clone(), sql.clone());
            match channel_handler.apply_bytes(&ctx, &emission.change).await {
                Ok(()) => {
                    applied += 1;
                    results.push(BulkEmissionResult {
                        emission_id: emission.emission_id,
                        applied: true,
                        error: None,
                    });
                }
                Err(e) => {
                    rejected += 1;
                    results.push(BulkEmissionResult {
                        emission_id: emission.emission_id,
                        applied: false,
                        error: Some(format!("{e}")),
                    });
                }
            }
            last_cursor = Some(emission.cursor.clone());
        }

        // Advance the persisted cursor once, to the final emission's
        // chain point — monotonic + correct regardless of per-row
        // outcomes (Nacked rows are surfaced via the host's emissions
        // log, same as the single path). A write failure means we
        // can't safely report progress, so 5xx → host retries the
        // whole batch (idempotent re-apply converges).
        if let Some(cursor) = last_cursor
            && let Err(e) = write_cursor(&sql, &cursor)
        {
            tracing::error!(
                channel = %channel,
                error = %e,
                "bulk cursor advance failed; reporting 5xx so dialer retries"
            );
            return Response::error(format!("cursor advance: {e}"), 500);
        }

        tracing::info!(
            channel = %channel,
            applied,
            rejected,
            total = batch.emissions.len(),
            "apply-bulk processed"
        );

        let body = match encode_apply_bulk_response(&ApplyBulkResponse { results }) {
            Ok(b) => b,
            Err(e) => return Response::error(format!("encode bulk response: {e}"), 500),
        };
        let resp = Response::from_bytes(body)?;
        let headers = Headers::new();
        let _ = headers.set("content-type", mitos_protocol::HTTP_DELIVERY_MIME);
        Ok(resp.with_headers(headers))
    }

    /// Recapture HTTP handler. Runs the dApp's `on_recapture`
    /// synchronously; 200 OK ack-equivalent means the dApp's
    /// table is clean and ready for refill.
    ///
    /// Error semantics: if `on_recapture` returns `Err`, the
    /// handler returns 500. The host's admin endpoint surfaces
    /// the error to the operator who can investigate and retry.
    /// 200 without a clean dApp body would risk ghost rows
    /// post-refill — the "fail loud" path is safer.
    ///
    /// Cursor handling: the runtime does NOT reset the persisted
    /// cursor here. Per-module recapture must leave other module
    /// subscriptions undisturbed; rewinding the cursor would
    /// affect all of them. The refill Apply requests advance the
    /// cursor naturally.
    async fn handle_recapture_post(
        &self,
        mut req: Request,
        channel: String,
    ) -> worker::Result<Response> {
        let bytes = req
            .bytes()
            .await
            .map_err(|e| worker::Error::RustError(format!("read recapture body: {e}")))?;
        let body = match decode_recapture(&bytes) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(channel = %channel, error = %e, "decode RecaptureBody failed");
                return Response::error(format!("decode: {e}"), 400);
            }
        };

        let RecaptureBody { module, reason } = body;
        let sql = self.state.storage().sql();
        let current_cursor = meta::read_cursor(&sql)
            .ok()
            .flatten()
            .unwrap_or(ChainPoint::Origin);
        let ctx = Ctx::new(current_cursor, module.clone(), sql.clone());

        tracing::info!(
            channel = %channel,
            module = %module,
            reason = ?reason,
            "Recapture HTTP request received; invoking on_recapture"
        );
        match self
            .inner
            .on_recapture(&ctx, &module, reason.as_deref())
            .await
        {
            Ok(()) => {
                tracing::info!(
                    module = %module,
                    "on_recapture complete; returning 200 (RecaptureReady)"
                );
                Response::empty()
            }
            Err(e) => {
                tracing::error!(
                    module = %module,
                    reason = ?reason,
                    error = %e,
                    "on_recapture failed; returning 500 so admin endpoint surfaces the error"
                );
                Response::error(format!("on_recapture failed: {e}"), 500)
            }
        }
    }

    /// Undo HTTP handler — signals a chain rollback (reorg).
    ///
    /// Decodes the [`UndoBody`], dispatches to the channel handler's
    /// `undo` hook with the rollback `ChainPoint`, and translates the
    /// result to an HTTP status:
    /// - `Ok(())` → 200 OK (revert applied / no-op)
    /// - `Err(e)` → 500 so the dialer retries (an undo that errored
    ///   left latching state wrongly set — fail loud)
    ///
    /// Unlike apply, undo does **not** advance the persisted cursor:
    /// the undo row is interleaved in the emission log before the new
    /// chain's applies, which advance the cursor as they arrive. A
    /// re-derivable companion's default no-op `undo` simply 200s.
    async fn handle_undo_post(
        &self,
        mut req: Request,
        channel: String,
    ) -> worker::Result<Response> {
        let bytes = req
            .bytes()
            .await
            .map_err(|e| worker::Error::RustError(format!("read undo body: {e}")))?;
        let body = match decode_undo(&bytes) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(channel = %channel, error = %e, "decode UndoBody failed");
                return Response::error(format!("decode: {e}"), 400);
            }
        };

        let UndoBody { cursor } = body;

        let channel_handler = match self.lookup_channel(&channel) {
            Ok(h) => h,
            Err(e) => {
                tracing::error!(channel = %channel, error = %e, "no handler for channel");
                return Response::error(format!("unknown channel: {e}"), 404);
            }
        };

        let sql = self.state.storage().sql();
        let ctx = Ctx::new(cursor.clone(), channel.clone(), sql.clone());

        match channel_handler.undo(&ctx, cursor.clone()).await {
            Ok(()) => {
                // `debug`, not `info`: a chain rollback is fanned out to EVERY
                // subscribed companion, but for most the `undo` hook is a no-op
                // (nothing confirmed at/after the slot). A handler that actually
                // reverts work logs that itself (with the affected rows), so this
                // generic line is just per-rollback × per-companion noise at `info`.
                tracing::debug!(channel = %channel, cursor = ?cursor, "undo applied");
                Response::empty()
            }
            Err(e) => {
                tracing::error!(
                    channel = %channel,
                    cursor = ?cursor,
                    error = %e,
                    "undo hook failed; returning 500 so the dialer retries"
                );
                Response::error(format!("undo failed: {e}"), 500)
            }
        }
    }

    /// Look up a channel handler by name.
    ///
    /// The channel comes from the request URL path
    /// (`/_internal/apply-<channel>`). Single-channel companions
    /// can fall through to the first registered channel if the
    /// URL name doesn't match — preserves the legacy "dispatch
    /// channel 0 to the primary handler" semantics for callers
    /// that don't yet plumb the channel name through.
    fn lookup_channel(&self, name: &str) -> Result<&dyn MitosChannelDyn> {
        if let Some(c) = self.channels.iter().find(|c| c.name() == name) {
            return Ok(c.as_ref());
        }
        if let Some(first) = self.channels.first() {
            tracing::debug!(
                requested = %name,
                fallback = %first.name(),
                "channel name did not match any registered channel; falling back to first",
            );
            return Ok(first.as_ref());
        }
        Err(CompanionError::UnknownChannel(name.to_string()))
    }

    // ========================================================================
    // Route handlers
    // ========================================================================

    /// `/_internal/wake` — triggered by the dApp Worker during
    /// onboarding to materialise the DO and run the HTTPS subscribe
    /// call against mitos. Reads the persisted cursor from DO SQLite
    /// and the cached interest set from `mitos_companion_interest`
    /// (populated via `/api/_interest/subscribe`), POSTs
    /// `SubscribeRequest` to the mitos host, and caches the result so
    /// subsequent wakes can short-circuit.
    async fn handle_wake(&self) -> worker::Result<Response> {
        // The companion key is the DO's own name — the dApp Worker
        // creates the DO with `id_from_name(companion_key)`, so
        // `state.id().name()` round-trips. Multi-tenant dApps that
        // want richer key derivation (e.g. per-customer) override
        // by passing the name explicitly through their wrapper.
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
        // companion's registration; the dialer uses it for the
        // filter applied to outbound Apply requests.
        let interest_rows = crate::interest::list_interests(&sql)?;
        let mut interests = crate::interest::rows_to_interests(&interest_rows);
        // Append the companion's programmatic initial-interest
        // declarations — used for filter shapes the SQL table
        // doesn't support. See `MitosCompanion::initial_interests`.
        interests.extend(self.inner.initial_interests());

        // Pull the dial-back URL from wrangler env so mitos knows
        // where to POST. The template carries `{key}` and
        // `{target}` substitutions (and now also `{op}` for the
        // apply/recapture path discriminator).
        let dial_back_url = self
            .env
            .var(crate::subscribe::MITOS_REPLICATE_URL_ENV)
            .ok()
            .map(|v| v.to_string());
        let dial_back = dial_back_url
            .clone()
            .map(|url| crate::subscribe::DialBackOverride {
                url: Some(url),
                auth_header: None,
                auth_value: None,
            });

        // Resolve the client_id. Precedence:
        // 1. Companion's `client_id()` trait override.
        // 2. Host portion of MITOS_REPLICATE_URL.
        // No fallback — if neither yields a non-empty value, the
        // subscribe fails loudly. See
        // `docs/design/MULTI_CLIENT_COMPANIONS.md` for the
        // "panic on no identity" rationale.
        let client_id = match self
            .inner
            .client_id()
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                dial_back_url
                    .as_deref()
                    .and_then(crate::subscribe::host_of_url)
                    .map(|s| s.to_string())
                    .filter(|s| !s.is_empty())
            }) {
            Some(id) => id,
            None => {
                let msg = "subscribe: no client_id available — \
                           neither MitosCompanion::client_id() nor \
                           MITOS_REPLICATE_URL host portion yielded \
                           a non-empty value. See \
                           docs/design/MULTI_CLIENT_COMPANIONS.md.";
                tracing::error!("{msg}");
                return Response::error(msg, 500);
            }
        };

        // Targets are declared by the companion via
        // `MitosCompanion::subscribe_targets()`. Default is one
        // `Module { name: C::NAME }` for backward compat with
        // single-wasm-module companions.
        let targets = self.inner.subscribe_targets();
        let request = SubscribeRequest {
            targets,
            companion_key,
            client_id,
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
    /// row. Body: `InterestMutateRequest`. Writes to the
    /// companion's local SQL (source of truth) then POSTs a
    /// targeted Add to mitos's `/api/companions/<key>/interest`
    /// endpoint. The targeted mutation updates the host's
    /// persisted CBOR + the running follower's live filter
    /// without a respawn.
    ///
    /// On mutation-endpoint failure, the SQL row stays committed
    /// and the next `/_internal/wake` (or full subscribe call)
    /// rehydrates the host. The endpoint is best-effort from
    /// this handler's perspective — the SQL is canonical.
    async fn handle_interest_subscribe(&self, mut req: Request) -> worker::Result<Response> {
        let payload: crate::interest::InterestMutateRequest = req.json().await?;
        let channel = payload.channel.clone().unwrap_or_default();
        let added_at = current_rfc3339();

        let sql = self.state.storage().sql();
        crate::interest::add_interest(&sql, &payload.kind, &payload.value, &channel, &added_at)?;

        if let Err(e) = self
            .push_interest_mutation(InterestOp::Add, &payload.kind, &payload.value, &channel)
            .await
        {
            tracing::warn!(
                error = %e,
                "push interest add to mitos failed; row persisted, host will resync on next subscribe"
            );
        }

        Response::from_json(&crate::interest::InterestMutateResponse {
            op_result: "added".into(),
            kind: payload.kind,
            value: payload.value,
            channel,
        })
    }

    /// `POST /api/_interest/unsubscribe` — symmetric to subscribe.
    /// Removes the SQL row then POSTs a targeted Remove to the
    /// mitos mutation endpoint.
    async fn handle_interest_unsubscribe(&self, mut req: Request) -> worker::Result<Response> {
        let payload: crate::interest::InterestMutateRequest = req.json().await?;
        let channel = payload.channel.clone().unwrap_or_default();

        let sql = self.state.storage().sql();
        crate::interest::remove_interest(&sql, &payload.kind, &payload.value, &channel)?;

        if let Err(e) = self
            .push_interest_mutation(InterestOp::Remove, &payload.kind, &payload.value, &channel)
            .await
        {
            tracing::warn!(
                error = %e,
                "push interest remove to mitos failed; row persisted, host will resync on next subscribe"
            );
        }

        Response::from_json(&crate::interest::InterestMutateResponse {
            op_result: "removed".into(),
            kind: payload.kind,
            value: payload.value,
            channel,
        })
    }

    /// `POST /_internal/teardown` — full companion teardown, called
    /// from the dApp's DO-reset path when a collection is removed.
    /// Removes EVERY local interest row and pushes a `Remove` to the
    /// host for each, so the host drops the policy from each subscribed
    /// module's scan-interest (and its shards) and — once a
    /// registration's interest set goes empty — deletes the companion
    /// record. Without this, a removed collection's host record
    /// survives and the host's startup reconcile re-routes it into the
    /// scan-interest, resurrecting (and re-cold-starting) a collection
    /// the dApp already dropped. Idempotent: a second call finds no
    /// rows and no-ops. Host pushes are best-effort — local SQL is
    /// cleared regardless, and the host converges on the now-empty
    /// record at its next reconcile.
    async fn handle_interest_teardown(&self) -> worker::Result<Response> {
        let sql = self.state.storage().sql();
        let rows = crate::interest::list_interests(&sql)?;
        let count = rows.len();
        for row in &rows {
            if let Err(e) = self
                .push_interest_mutation(InterestOp::Remove, &row.kind, &row.value, &row.channel)
                .await
            {
                tracing::warn!(
                    kind = %row.kind,
                    value = %row.value,
                    error = %e,
                    "teardown: host interest-remove push failed; clearing local row anyway",
                );
            }
            crate::interest::remove_interest(&sql, &row.kind, &row.value, &row.channel)?;
        }
        tracing::info!(removed = count, "companion teardown complete");
        Response::ok(format!("{{\"torn_down\":{count}}}"))
    }

    /// Translate one `(kind, value, channel)` interest mutation
    /// row into the wire `Interest` shape and POST it to mitos.
    /// Encapsulates the SQL-to-wire translation + the auth /
    /// endpoint plumbing so both handlers share one path.
    async fn push_interest_mutation(
        &self,
        op: InterestOp,
        kind: &str,
        value: &str,
        channel: &str,
    ) -> Result<()> {
        let companion_key = self
            .state
            .id()
            .name()
            .map(|s| s.to_string())
            .unwrap_or_default();
        let row = crate::interest::InterestRow {
            kind: kind.to_string(),
            value: value.to_string(),
            channel: channel.to_string(),
            added_at: String::new(),
        };
        let items: Vec<Interest> = crate::interest::rows_to_interests(std::slice::from_ref(&row));
        if items.is_empty() {
            // Translation didn't yield a wire-shape Interest
            // (kind/value combination not supported). Soft no-op
            // — SQL still persists, next full subscribe carries
            // the row via `rows_to_interests` if it's
            // serialisable then.
            return Ok(());
        }
        push_interest_mutation_via_env(&self.env, &companion_key, op, items).await
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

// ============================================================================
// Helpers
// ============================================================================

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
        "subscribe_via_env called outside wasm target".into(),
    ))
}

#[cfg(target_arch = "wasm32")]
async fn push_interest_mutation_via_env(
    env: &Env,
    companion_key: &str,
    op: InterestOp,
    items: Vec<Interest>,
) -> Result<()> {
    crate::subscribe::post_interest_mutation(env, companion_key, op, items).await
}

#[cfg(not(target_arch = "wasm32"))]
async fn push_interest_mutation_via_env(
    _env: &Env,
    _companion_key: &str,
    _op: InterestOp,
    _items: Vec<Interest>,
) -> Result<()> {
    Err(CompanionError::Wire(
        "push_interest_mutation_via_env called outside wasm target".into(),
    ))
}

fn current_rfc3339() -> String {
    // Diagnostic-only timestamp; consumers treat as opaque string.
    // Worker-rs's `Date::now().as_millis()` returns the JS epoch
    // milliseconds without pulling `std::time` (which has no
    // monotonic clock in wasm32 + would panic at runtime).
    let millis = worker::Date::now().as_millis();
    let secs = millis / 1000;
    format!("unix:{secs}")
}
