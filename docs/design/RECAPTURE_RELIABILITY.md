# Recapture reliability & scoped per-policy refresh

Status: **proposed** (2026-07-05, analysis complete — see evidence anchors
throughout). Companion to `HOLDER_DISTRIBUTION_MULTI_DEX.md` (which owns the
LP-decomposition *correctness* roadmap; this doc owns the *reliability*
architecture around rebootstrap/recapture).

## Problem

holder-distribution was down 2026-06-21 → 2026-07-05 (all 17 companions
frozen, new tokens empty) because four defects stacked:

1. `POST /_admin/modules/{id}/recapture` runs the whole recapture **inline in
   the axum handler** (`admin.rs:1906`). The CF tunnel drops the connection at
   ~100s → the handler future is cancelled → the `InFlightGuard` releases (no
   409 next attempt) and cancellation lands inside `start(id, true)`.
2. `start()` is **stop-first with no recovery** (`host_v2.rs:381` stops the old
   follower before anything else; any later failure/cancellation returns with
   no slot re-inserted). Result: module left STOPPED, silently.
3. A stopped module **accepts subscribes silently** — the companion registers,
   the interest mutation is dropped (`companions.rs:544-558`,
   `host_v2.rs:1589`), the caller sees success.
4. Nothing alerted. `/metrics` carried `mitos_last_rebootstrap_age_seconds` >
   1.1M the whole time; nothing scrapes it.

The recapture was being hammered 96×/day: prod holder-map cron (`0 * * * *`,
intentional) **plus an orphaned `*/15 * * * *` schedule on the deployed
`holder-map-dev` script** (set 2026-03-01, pre-cutover; the repo removed dev's
`[triggers]` but `wrangler deploy` does not clear an omitted schedule).

Why a cron hammers recapture at all: per-wallet **LP/vest attribution is a
snapshot-time transform** — the live delta path emits `lp_amount: 0`
(`holder_distribution.rs:998-1001`), so staked-LP-only wallets (e.g.
`$aliens.vault`) drop off the map until a rebuild. The hourly whole-module
recapture exists solely to bound that staleness.

## Facts the design rests on (verified in code)

- **`ledger:` shards are live-maintained.** `flush_buffer` read-modify-writes
  the module's own per-holder shards on every transfer
  (`holder_distribution.rs:966-992`). Only the LP/vest *attribution* is
  snapshot-time.
- **Per-policy snapshot re-emit is companion-safe.** All emits are keyed by
  policy; SnapshotBegin/Chunk/End = full replace *of that policy only*, and the
  worker DO has an explicit policy fence (`feed_do_mitos.rs:113-138`).
- **Scoped rebootstrap already exists mechanically**: `seed_onboard_scope([X])`
  + `rebootstrap(Onboard)` scans→decomps→emits exactly policy X, riding the
  disposable backfill plane, with **no dialer wipe-coordination** (the ack gate
  exists only because whole-module recapture wipes companions; a scoped
  refresh is additive). Missing pieces are host-side triggers only.
- **Bootstrap-done flags are written per-predicate** but only clearable
  whole-module (`bootstrap_v2.rs:74` vs `:795`).
- **Decomp is the fuel-fragile phase and is not trap-resumable**: progress
  lives in resident state (`DECOMP_STATE`), the durable cursor is a one-byte
  marker, so any over-budget decomp unit → trap → restart-from-zero → livelock
  (the HOSKY failure). Materialize also does one `state_kv::set_value` **per
  holder** (`holder_distribution.rs:593`) — 1000 discrete writes per step.
- **A decode-failed pool still pays full materialize**: "pool datum did not
  decode — skipping LP" (`:1173-1178`) leaves `decomposed=true` and copies
  every ledger shard into `decomp:` for zero attribution benefit.
- `meta:` (pool_ref + vesting refs) is wiped at `emit_end` (`:1535`) — the
  reason a *decomp-only* op can't run standalone today.
- `list_running()` exists but a dead follower's slot **stays in the map**
  (nothing polls the JoinHandle) — stale liveness for any supervisor/status.

## Decision — three layers

### Layer 1: mechanism hardening (mitos platform; small, ship first)

1. **Detach recapture from the HTTP request.** Admin handler keeps validation
   + `RecaptureStarted` synchronous, `tokio::spawn`s the existing
   `match host.recapture_module(...)` block (which already records
   Completed/Failed on the ring), returns **202** immediately. The
   `InFlightGuard` moves with the spawned future so duplicate POSTs still 409.
   Completion is observed via `/_admin/events` + gauges. (~15 lines, no host
   change.)
2. **`start()` recover-on-failure.** On any `Err` after the old follower was
   stopped, attempt `self.start(id, false)` (old config) before returning, and
   record a ring event either way. Build-new-then-swap is the nicer end-state
   but a bigger refactor — recover-on-failure closes the stranding hole now.
3. **Truthful liveness + exposure.** Expose follower health (poll
   `JoinHandle::is_finished` or clear the slot on follower exit), then:
   `running: bool` on `/_admin/status` modules + `mitos_module_running` gauge
   (template: `mitos_recapture_in_progress`, `admin.rs:1049`).
4. **Auto-start on subscribe.** In the subscribe handler, if the target module
   is not running (and not recapture/bootstrap-in-flight), `start(id, false)`
   before routing the interest mutation. A subscribe to a stopped module is
   intent to run it.
5. **Supervisor-lite.** Periodic task (template: `compaction::spawn`,
   `bundle.rs:564`): module has companions, isn't running, isn't in-flight →
   `start(id, false)`. Guards: skip in-flight ids, delay first tick past
   `auto_resume`, use the truthful liveness from (3).

### Layer 2: right-size the refresh (kill the whole-module hammer)

> **SUPERSEDED (2026-07-05)** by `NATIVE_HOLDER_SNAPSHOT_PLANE.md`:
> snapshot building moves host-side (native), which delivers scoped
> per-policy refresh without the wasm fuel fixes below — items 6–8
> are implemented there instead. Layer 1 (shipped) and Layer 3
> (shipped) stand.

6. **Per-policy scoped refresh** (`POST /_admin/modules/{id}/refresh-policy`,
   body `{policy}`): host clears that predicate's bootstrap flag (new
   `clear_bootstrap_flag_for_predicate`, key fn already exists), seeds the
   onboard scope with X (handle the REPLACE-semantics clobber: merge or reject
   if an onboard is pending), routes the rescope through the follower's
   interest channel → Onboard pump → per-policy snapshot re-emit. Additive; no
   companion wipe; no ack gate; blast radius = one policy.
7. **Rewire the triggers**: holder-map's B1 DexFeedDO debounce (already
   committed + deployed) calls the scoped refresh for the affected project's
   policy instead of whole-module recapture; prod cron relaxes to a **daily**
   whole-module backstop; the orphaned `holder-map-dev` `*/15` schedule is
   deleted (ops action, CF API/dashboard — `wrangler deploy` won't clear it).
8. **Decomp fuel fixes** (module, targeted): batch `write_decomp_holder` via
   `set_many`; on pool-datum decode failure downgrade to the fast path
   (skip materialize) instead of paying it for nothing; make decomp
   trap-resumable with a durable sub-cursor (lp_after / mat_after) so a trap
   resumes instead of restarting from zero. These de-fang HOSKY-class
   policies everywhere (onboard, scoped refresh, recapture).

### Layer 3: alerting (cnft.dev-workers)

9. **`mitos-watch` worker**: 5-min cron scrapes `/_admin/status` (readonly
   token) + `/metrics` (open). Rules: host unreachable; tip lag (slot vs
   wall-clock); `companions>0 && !running`; `last_rebootstrap_age` beyond
   backstop interval; all-companion `last_drain_age` stale;
   `queued>0 && companions==0` (already live again on jpg-store-listing/sale).
   Alert dedup in KV; egress via the existing `DISPATCH` queue →
   `notification-dispatcher` → Discord (template: `workers/notifier`
   cron + producer binding). Sentry-mediated reporting can layer on later per
   `OBSERVABILITY_AND_INCIDENTS.md`; a direct DISPATCH ping is the correct
   first slice.

## Rejected alternatives

- **Decomp-only refresh op now** (persist `meta:` past `emit_end` + new WIT
  mode): saves a per-policy holder rescan vs (6), but requires
  meta-lifecycle + ABI surface changes, inherits decomp's trap-fragility
  (fixed by 8 anyway), and the multi-DEX slice is about to restructure decomp
  (`pools: Vec<PoolRef>`, per-pool cursors). Fold "persist meta" into that
  slice if scoped-refresh cost ever bites; don't build a parallel surface now.
- **Live LP attribution**: reserve drift is continuous (every swap) —
  event-driven scoped refresh (7) captures the discrete events that actually
  strand wallets (stake/unstake/add/remove) at a fraction of the complexity.
- **Build-new-then-swap `start()`**: correct end-state, deferred — bigger
  refactor of slot/instance lifecycle for marginal gain over recover-on-failure
  once recapture is detached (the cancellation source is gone).
- **Per-companion recapture (wipe-based)**: the dialer machinery is already
  per-companion-capable, but scoped *policy* refresh (6) covers the actual use
  cases (onboarding repair, LP refresh, drift) without wipes. Whole-module
  recapture remains as disaster recovery only.

## Phasing / deploy order

- **Phase 0 (recover + stop the bleeding):** Layer 1 items 1–4 → one mitos
  deploy (restart itself performs the in-process rebootstrap and revives
  holder-distribution) → delete the orphaned dev schedule → verify $DIS +
  frozen tokens flow. Supervisor-lite (5) rides along if liveness (3) lands
  cleanly, else fast-follows.
- **Phase 1:** Layer 3 alert worker (independent; can land same day).
- **Phase 2:** Layer 2 scoped refresh + trigger rewire + decomp fuel fixes.
- **Phase 3 (existing roadmap):** multi-DEX decomposition + `utxos_by_asset`
  (removes the shared-LP-policy overscan and the 100K truncation risk).

Deploy notes: all mitos changes are host-side except (8) (module rebuild —
`deploy.sh` rebuilds community modules on the box). No wire-format changes; no
worker rev-bump needed for Phase 0. HOSKY stays subscribed — post-(8) it is
survivable; an add-time admission guard (size/age) remains a worker-side
follow-up.
