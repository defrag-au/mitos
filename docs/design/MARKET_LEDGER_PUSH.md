# market-ledger — push / webhook delivery (Phase 4)

**Status:** DESIGN, agreed 2026-07-22. Not built. Companion to
[`MARKET_LEDGER.md`](./MARKET_LEDGER.md) (the authoritative market-ledger spec)
and [[project_market_ledger]] / [[market-ledger-direction]] in memory.

## Why push (and why only for some consumers)

market-ledger is a **pull-first** hosted read service — analytical consumers
(the pricing engine's 6h-cached demand model, frontend history views) query
`/events` on demand and that's the right model for them. But **reactive**
consumers need to *know when something happened* promptly, not poll:

- **Quests** (`QuestDO`) verify actions (sold / offer-accepted / listed) as they
  occur. Today they poll a mitos-fed per-policy DO event feed. As part of
  retiring the mitos marketplace subscriptions (the live-book experiment —
  listings move back to Anvil), quests move onto market-ledger. Polling would
  work but is wasteful and laggy; the user's call is that market-ledger should
  grow **companion-like push mechanics, mirroring mitos**, so reactive
  consumers subscribe and get events delivered.
- Future reactive consumers (alerting, notifications) reuse the same surface.

So the split is deliberate: **pull for analytical reads, push for reactive
triggers.** `/events` and `/health` are unchanged; push is additive.

## Key insight: delivery is an internal poll of our OWN ledger

`follow` already inserts every tip event into the sqlite ledger in real time.
Push delivery does **not** need to hook into block processing — it is just a
loop that reads new ledger rows (the same `(slot, rowid)` cursor query `/events`
uses) and POSTs them to subscribers. This decouples delivery from chainsync:

```
chainsync tip → follow inserts events → (ledger) → delivery loop reads > cursor → POST to subscriber → 200 advances cursor
```

Delivery reuses the existing event-query + cursor machinery (`serve/query.rs`).
`follow` needs no changes beyond signalling "new events landed" (or delivery
just polls every few seconds).

## Topology

Recommended (keeps the public read surface's availability independent of the
writer, and avoids two sqlite writers):

- **`serve`** (RO public, `:8183`, tunnelled `market-ledger.defrag.cc`) —
  unchanged: `/events`, `/health`.
- **`follow`** (the single writer) gains, in its existing tokio runtime:
  1. the **subscription registry** (a new sqlite table — follow is the only
     writer, so no contention);
  2. the **delivery loop** (reads new events per subscription, pushes, advances
     cursors);
  3. a small **admin HTTP** surface (axum, loopback-bound, tunnelled on its own
     route/host, admin-token-gated) for subscription CRUD.

Rationale for putting subscription writes in `follow`, not `serve`: sqlite is
single-writer (WAL); `serve` is deliberately read-only. Routing the rare
subscription writes through the existing writer avoids `SQLITE_BUSY` contention
and a second-writer footgun. Alternative considered — merge `serve` into the
`follow` process (one daemon) — is cleaner for a single writer but couples the
public read surface's uptime to chainsync restarts; rejected for that reason.
Keep them as two systemd units (see infra `services/market-ledger/`).

## Data model — `subscriptions` table (market-ledger sqlite)

```sql
CREATE TABLE IF NOT EXISTS subscriptions (
    id            TEXT PRIMARY KEY,   -- subscriber name, e.g. "cnft-quests"
    target_url    TEXT NOT NULL,      -- where to POST (the consumer's receiver)
    token         TEXT NOT NULL,      -- bearer secret the push carries; the
                                      -- consumer verifies it
    kinds         TEXT NOT NULL,      -- CSV of EventKind db-strings, or "" = all
    policies      TEXT,               -- JSON array of policy_id allowlist;
                                      -- NULL = all policies
    cursor_slot   INTEGER NOT NULL,   -- last-delivered (slot, rowid) — advanced
    cursor_rowid  INTEGER NOT NULL,   -- only on a 200 ACK
    active        INTEGER NOT NULL DEFAULT 1,
    fail_count    INTEGER NOT NULL DEFAULT 0,  -- consecutive delivery failures
    next_attempt  INTEGER NOT NULL DEFAULT 0,  -- unix secs; backoff gate
    created_at    INTEGER NOT NULL
);
```

The cursor is the same keyset `(slot, rowid)` `/events` pagination uses — so
"events for this subscription since its cursor" is the existing indexed query
plus the kind/policy filter. Persisted, so a market-ledger restart resumes
delivery with zero loss.

## Subscription API (follow admin HTTP, admin-token-gated)

- `POST /subscriptions` — register `{ id, target_url, token, kinds?, policies?,
  start }`. `start` ∈ `"tip"` (deliver only events from the current tip forward
  — the default for quests) | `"origin"` (backfill everything) |
  `<slot>:<rowid>` (explicit). Sets the initial cursor accordingly.
- `PUT /subscriptions/:id` — update `target_url` / `token` / `kinds` /
  `policies` / `active`. **Quests use this to keep the policy allowlist in sync
  as quests are added/removed.**
- `DELETE /subscriptions/:id` — remove.
- `GET /subscriptions` — list (redact token).

Auth: the admin bearer (a dedicated `MARKET_LEDGER_ADMIN_TOKEN`, separate from
the now-open `/events` — only trusted services register subscriptions).

## Delivery semantics

- **Per-subscription loop:** for each `active` subscription whose `next_attempt
  <= now`, query the ledger for up to `BATCH` (e.g. 500) events with
  `(slot, rowid) > cursor` matching `kinds` + `policies`, oldest first. If none,
  idle. Else POST them.
- **Payload:** the same postcard `EventsPage` wire format `/events` returns (the
  consumer already has `market-ledger-wire`). Header `Authorization: Bearer
  <subscription.token>`. The `next_cursor` in the page is the batch's high-water
  mark.
- **ACK = HTTP 200.** On 200: advance `cursor` to the batch's last
  `(slot, rowid)`, reset `fail_count`/`next_attempt`, immediately try the next
  batch (drain to tip). On non-200/timeout: leave cursor untouched, increment
  `fail_count`, set `next_attempt = now + backoff(fail_count)` (exponential,
  capped, e.g. 2^n secs → max 5 min). **Cursor never advances past an
  un-ACKed batch → at-least-once, no loss.**
- **Idempotent receiver required.** At-least-once means a consumer may see a
  batch twice (processed, but the 200 was lost, so we retry). Events carry a
  natural key `(tx_hash, policy_id, asset_name_hex, kind)`; the consumer must
  dedup on it (quests' apply is already `INSERT OR IGNORE`-shaped).
- **Cadence:** delivery wakes on a `follow` "new events" signal (tokio notify)
  or polls every ~2–5 s. At tip that's near-real-time; during a market-ledger
  catch-up, batching bounds the push rate.
- **Degraded subscriptions:** after N consecutive failures (e.g. 20 ≈ hours of
  backoff), log/emit a warning but keep the subscription and its cursor — a
  consumer that comes back gets everything it missed. Never silently drop.

## Consumer side (cnft.dev-workers CO worker)

- **Receiver:** a new authed endpoint, e.g. `POST /_internal/market-ledger-push`
  (bearer = the subscription token, verified via `check_*_auth`). Body = postcard
  `EventsPage` → `market_ledger_wire::decode_events_page`. Returns 200 to ACK
  after durably staging/applying (so a crash mid-apply just gets a redelivery).
- **Quests:** `QuestDO` consumes the pushed events instead of polling the
  mitos-fed DO `/market-events` feed. Its quest-verb logic (sold / offer_accepted
  / listed) and `quest_market_events` dedup table are reused; only the source
  changes. The worker registers/updates its subscription (kinds =
  `sold,offer_accepted,listed`, policies = active quest policies) via the
  subscription API on startup and whenever the quest-policy set changes.
- Registration secret + market-ledger admin token live in `wrangler.toml`
  secrets, mirroring the `MITOS_AUTH_TOKEN` pattern.

## Failure modes / reliability summary

| Scenario | Behaviour |
|---|---|
| Consumer briefly down | Retries with backoff; cursor holds; catches up on recovery. No loss. |
| market-ledger restart | Subscriptions + cursors persisted in sqlite; delivery resumes from cursor. No loss. |
| Duplicate delivery (lost ACK) | Consumer dedups on the event natural key. No double-count. |
| market-ledger rollback (follow) | Rolled-back events were `slot > point`; a subscription whose cursor already passed them delivered them — consumer must tolerate a later "correction". **OPEN Q below.** |
| Consumer permanently gone | Subscription goes degraded (logged); admin `DELETE`s it. |

## Open questions

1. **Rollback vs already-delivered events.** follow can roll back the volatile
   window (≤ k blocks) and delete events past a fork point. If delivery already
   pushed an event that's later rolled back, the consumer acted on a
   now-orphaned event. Options: (a) delay delivery until events are k-deep
   (past the volatile window = final) — adds latency (~k blocks) but never
   delivers a reversible event; (b) deliver at tip and push a "retraction"
   message on rollback (consumer must handle). **Lean (a)** — deliver from the
   *boundary* (settled) cursor, not the live tip. Quest latency of a few
   minutes is fine, and it sidesteps retractions entirely. Revisit if a
   consumer needs sub-finality latency.
2. **Backfill guard.** A `start=origin` subscription would push millions of
   historical events. Cap/rate-limit backfill, or restrict `origin` to admin +
   document it's a firehose.
3. Per-subscription vs shared delivery loop concurrency (fine to start
   single-threaded — tip event volume is low).

## Implementation slices

1. **Schema + registry**: `subscriptions` table, `store.rs` CRUD helpers.
2. **Admin HTTP on follow**: axum loopback server, subscription CRUD, admin
   auth; infra tunnel route + `MARKET_LEDGER_ADMIN_TOKEN`.
3. **Delivery loop**: per-subscription query (reuse `serve/query.rs` filter +
   cursor) from the **boundary** cursor, POST postcard batch, ACK→advance,
   backoff on failure. Unit-test cursor/backoff/idempotence.
4. **Consumer receiver**: CO worker `POST /_internal/market-ledger-push`,
   decode, idempotent stage/apply, 200 ACK.
5. **Quests repoint**: `QuestDO` off the DO feed onto pushed events; worker
   registers/maintains its subscription.
6. Then the mitos live-book removal (`MarketIngressDO` + DO market tables +
   market-stats/listings endpoints + wrangler DO-class deletion migration) —
   quests now have a home, listings are on Anvil.
