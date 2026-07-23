# market-ledger — deep marketplace history & pricing data service

Status: **design, decisions settled** (2026-07-22). Home: `tools/market-ledger/`
(graduates to `crates/` when serve mode becomes a resident service).

## Why

The CNFT valuation work in `cnft.dev-workers` is grounded in realized sales
(global D1 `market_events`, firehose-backfilled ~90 days via Koios). Three
pressures push past that design:

1. **Koios is marginal for bulk history on hot addresses.** The jpg.store
   scripts are among the most active addresses on Cardano; per-page
   `/address_txs` cost + 504 retry storms made the 90-day walk take hours.
   Deeper history (jpg opened 2021) is effectively unreachable that way.
2. **D1 is the wrong home for the deep corpus.** ~13k rows (90d) is a fine
   D1 tenant; full-history all-collections data is millions of rows — D1
   bloat risk (asset-db hit the 10GB cap once) and a large one-time write
   bill on a platform where writes are the expensive operation.
3. **The pricing north star wants a research corpus.** Auto-detecting
   demand axes from sales (`pricing-core::SalesDemandModel` and beyond)
   improves with depth, and listing lifecycle unlocks time-on-market and
   ask-history signals. The corpus should be a *dataset* (loadable in
   polars/notebooks), not rows trapped behind an API.

`market-ledger` walks certified chain history locally, decodes with the same
SoT crate the live modules use, keeps the deep corpus on the box, and pushes
the hot window to D1. It is designed to become the **authoritative CNFT
marketplace ledger**: full-depth, tip-current (phase 3), venue-pluggable.

## Non-goals / hard constraints

- **NOT a second follower *store*.** (REVISED 2026-07-22, user decision —
  was "NOT a second chain follower".) The original constraint guarded
  against a second dolos-style follower: its own state store, redb locks,
  WAN churn. A raw chainsync *tail* is none of that and is now the plan:
  phase-3 `follow` is a **self-contained pallas-network chainsync client**
  (see "Follow mode"), NOT a mitos companion subscriber. Rationale: the
  companion path would put the tip hot-path through the WASM module
  pipeline and substitute module-emitted events for the tool's own
  SoT-crate decode over raw blocks — the basis of its whole validation
  story — buying only rollback handling, which is modest native work. The
  tool keeps zero follower state beyond its existing sqlite (cursor,
  buffer, small volatile window).
- **Decode stays in `mitos-marketplace-decode`.** The walker maps chain
  data into `DecodeTx` and consumes the crate's outputs; no parallel decode
  logic. (Standing single-source-of-truth decision.)
- **Raw first.** The serve API exposes raw events before any precomputed
  pricing; aggregates layer on via DuckDB later and never hide the fills.
- **No live-service coupling.** The walk reads Mithril immutable-db chunk
  files (plain files, no locks). It must not open the running dolos/mitos
  redb stores (single-writer lock model) nor degrade their tip-follow
  (nice/ionice the walk).
- D1 remains the hot-window edge cache **for now** — but per the 2026-07-22
  decision, if this service proves out, worker reads may cut over to
  market-ledger directly and the global D1 store becomes optional. Design
  the serve surface as if it will be leaned on externally.

## Settled decisions (2026-07-22)

1. **Listing lifecycle is in from v1** — time-on-market matters (real-estate
   analogy). Prerequisite: extract listing/offer lifecycle decode into the
   SoT crate (see "Crate prerequisite").
2. **Dev D1 is left alone** — no buyer-price correction pass; the pre-fix
   skew in dev is accepted, and dev may eventually read from this service.
3. **Name: `market-ledger`.**
4. **Full chain history**, and the walker must be **re-runnable per venue**
   as new marketplaces are discovered and plugged in.

## Architecture

One binary, three modes sharing one storage layout:

```
market-ledger walk --venue jpg,wayup [--from-slot …]   # snapshot walk (resumable, per-venue)
market-ledger serve                                     # HTTP read surface (long-running)
market-ledger follow                                    # chainsync tip tail (phase 3)
```

### Venue registry (the pluggability seam)

Venues are **declarative configs**, not code paths scattered through the
walker:

```toml
[venue.jpg]
sale_addrs   = ["addr1zxgx…", "addr1x8rjw…", "addr1w8rjw…", "addr1w999…"]  # V1–V4
offer_addrs  = ["addr1xxgx…"]                                              # CO V2
earliest_slot = 33_000_000        # fast-skip floor (first contract activity)
decoder       = "jpg"             # dispatch key into the SoT crate

[venue.wayup]
sale_creds   = ["a76f0fb8…"]
offer_creds  = ["27d46ecb…"]
earliest_slot = …
decoder       = "wayup"
```

- The walker's watched-set, buffer scope, and decoder dispatch all derive
  from the enabled venue set.
- **Per-venue walk cursors** (`walk_cursor` keyed by venue): adding a new
  venue later = add its config, run `walk --venue newvenue` over full
  history; existing venues are untouched. PK dedup makes any overlap safe.
- A venue's `earliest_slot` lets a full-genesis walk skip dead chunks
  cheaply; "whole chain history" is the default posture, the floor is just
  an optimization.
- Plugging in a genuinely new marketplace still requires its decoder in
  `mitos-marketplace-decode` (datum/redeemer semantics are venue-specific
  and golden-tested there) — the registry makes the *walker* need zero
  changes, not the crate.

### Data flow (walk)

```
mithril-client CLI ──► immutable-db chunks (plain files)
        │
pallas-hardano chunk reader ──► MultiEraBlock (pallas-traverse)
        │
   [venue-aware slot fast-skip]
        │
outref buffer (see below) ──► DecodeTx per marketplace-touching tx
        │
mitos-marketplace-decode  (sales, offer accepts, listing lifecycle —
        │                  fees, bundles, waivers, on-top fees all in-crate)
        ▼
sqlite ledger  ──► Parquet partitions (venue/year/month, sealed)
        │
        └──► --upload: hot window → CF worker ingest (batched, idempotent)
```

### The outref buffer (the core correctness piece)

Block bodies reference inputs as `(tx_hash, index)` — they don't carry the
consumed output's address/value/datum. The walker therefore replays the
mitos buffer semantics over history:

- Walking forward, every tx output **created at a watched credential** (per
  the enabled venue set) is stored keyed by outref:
  `(address, lovelace, assets, inline_datum | datum_hash)`.
- When a later tx spends a buffered outref, build a `DecodeTx` from
  - the buffered entries (consumed listings/offers, fully resolved),
  - the spending tx's own outputs (buyer output, fee outputs — this is
    what makes the jpg on-top-fee decode work),
  - redeemers from the spending tx's witness set, matched to inputs,
  - **hash-datum resolution from the spending tx's witness set**: jpg's
    hash-only listing datums are revealed as `plutus_data` witnesses in
    the buy tx — a per-tx `hash → bytes` map (the local equivalent of the
    firehose's Koios `/datum_info` call).
- Listing lifecycle needs the buffer too: a *create* is a watched output
  appearing; an *update* is a watched output consumed with a new watched
  output produced in the same tx; a *delist* is a cancel-redeemer consume
  with no sale. The lifecycle classification mirrors the live listing
  modules' semantics and must come from the crate (below), not be
  reimplemented walker-side.
- Spent entries are evicted. Buffer cardinality ≈ the open book across all
  enabled venues — tens of thousands, trivially in memory, but **persisted
  to sqlite at checkpoint** with the cursor so walks are resumable (a
  resume without the buffer silently drops every event whose listing
  predates the resume point).

Rollbacks don't exist here: the immutable db is final. The walk ends
~k blocks (≈12–36h) behind tip; that gap is covered by the live mitos→
worker path and later by companion mode.

### Crate prerequisite: listing/offer lifecycle decode

Sales + offer-accepts already live in `mitos-marketplace-decode`. Listing
lifecycle (`listed` / `price_change` / `delisted`) and offer lifecycle
(create/cancel) are currently decoded inside the live modules
(`{jpg,wayup}-store-{listing,offer}`). **Phase 1 starts by extracting that
logic into the crate** — the same move already made for sales and
offer-accepts, with the same acceptance bar: the modules become thin
wrappers and the golden suite passes byte-identical. This benefits the
live platform too (one decode surface for everything).

### Event scope (v1)

All kinds, one schema: `listed`, `price_change`, `delisted`, `sold`,
`offer_accepted`, `collection_offer_accepted` (+ offer create/cancel if the
crate extraction surfaces them cheaply — decide during extraction). Row
shape mirrors D1 `0005-market-events.sql` (same PK
`(tx_hash, policy_id, asset_name_hex, kind)`, same buyer-price semantics
including wayup `/49` + waiver and jpg `on_top_fee`). One schema across
D1 / sqlite / Parquet — deliberately.

Time-on-market is **derived at query time** (lifetime of a listing outref
from create to sale/delist), not stored — storing raw events keeps the
ledger append-only and lets the derivation improve without migration.

### Storage

- **sqlite = operational ledger.** Tables: `market_events` (≈ D1 schema
  + `venue` column), `walk_cursor` (per venue), `outref_buffer`
  (checkpoint snapshot), `partition_manifest`. Single writer (this
  binary), WAL mode. Ingest point for all modes; holds unsealed months.
- **Parquet = the deep corpus**, partitioned
  `events/venue=<v>/year=<y>/month=<m>.parquet`, written by DuckDB
  (`ATTACH sqlite; COPY … (FORMAT PARQUET)`) — no direct arrow dep.
  **Per-venue partitioning is what makes venue re-runs safe**: a new
  venue's historical backfill creates its own partition tree; sealed
  partitions of other venues are never rewritten. Sealed = immutable.
- **DuckDB = the query engine.** In-process in serve mode; a union view
  over sealed Parquet (hive-partitioned) + the sqlite-attached current
  months presents one continuous corpus.
- **ClickHouse = documented graduation path, not a dependency.** Ingests
  the Parquet partitions natively if scale ever demands it (100M+ rows).

### Serve mode (phase 2)

Token-gated (own bearer, `MITOS_AUTH_TOKEN` shape), `127.0.0.1` first; CF
tunnel (the `mitos.defrag.cc` pattern) when workers or external consumers
should reach it — and per decision 2, design as if they will. Raw first:

- `GET /health` — per-venue cursors, corpus extent, partition manifest.
- `GET /events?venue=&policy=&asset=&kind=&from_slot=&to_slot=&limit=&cursor=`
  — raw rows, slot-cursor pagination.
- `GET /export?window=90d` — the D1-upload feed; doubles as an audit feed.

Aggregates later via DuckDB (floors, windowed volume, realized ranges,
time-on-market distributions). **Honest boundary:** this service knows
assets, not traits — trait bitmaps/vocab live in the CF DOs. Trait-level
analytics stays worker-side, or a later phase ingests per-policy trait
maps as a Parquet sidecar via the worker admin API. Don't let trait logic
leak in ad hoc.

### D1 upload

`--upload --window 90d`: batched POSTs (≤500 rows) to a small token-gated
worker admin endpoint wrapping the existing
`data::market_events::insert_events` (INSERT OR IGNORE — idempotent,
re-runnable, no-op for rows the live feed already landed). This is the
**prod backfill path**, replacing the sharded-Koios plan. Dev D1 is left
as-is (decision 2).

### Follow mode (phase 3) — self-contained chainsync

(REDESIGNED 2026-07-22, user decision — supersedes the companion-subscriber
plan. The superseded shape: subscribe to the marketplace modules on
localhost `:8181` via the companion WS protocol. Rejected because it routes
the tip hot-path through the WASM module pipeline and replaces SoT-crate
decode-at-source with module-emitted events; its only real win was
rollback handling.)

`follow` is a **pallas-network chainsync + blockfetch client** feeding the
exact same `process_tx` path as `walk`:

- **Intersect at the walk cursor.** Walk a fresh snapshot to near-tip,
  then `follow` intersects chainsync at the persisted `(slot, hash)`
  cursor — the outref buffer in sqlite is warm and exactly right. The
  buffer IS the inputs cache; no external resolution needed (same
  local-first rule as walk).
- **Peer is configurable** (`--peer host:port`). Preferred: the box's own
  dolos, if/when the mitos bundle exposes dolos's o7s (N2N serve)
  listener — standard protocol against localhost, mitos stays a black
  box, no second WAN connection. NOTE (checked 2026-07-22): the bundle
  currently wires only the optional `minibf` HTTP bridge, NOT the o7s
  serve stack — enabling it is a small config-gated bundle addition.
  Until then: a public relay (IOG backbone) — one TCP connection at tip
  rate, at most a few hundred MB/day.
- **Rollback handling (the one new piece):** a volatile window. Keep raw
  block CBOR for the last k (≤2160) blocks in a small sqlite table;
  checkpoint the buffer at the immutable boundary as it advances. On
  `RollBackward(point)`: delete `market_events` with `slot > point`,
  reload the boundary buffer checkpoint, replay surviving volatile blocks
  through `process_tx` (sub-second at the walk's measured ~10k blocks/s).
  `INSERT OR IGNORE` makes replay idempotent. Practical rollbacks are 1-2
  blocks; k is a safety ceiling, not a working size.
- **Topology unchanged:** `follow` merges into `serve` (one process = the
  single writer); PK dedup keeps the walk/follow seam safe in both
  directions.

## Validation

- **Golden cross-check (acceptance test):** walk the slot range D1 already
  covers and row-diff against D1 exports. Every divergence is a walk bug
  or a known Koios-era gap — both worth finding. Named fixture: tx
  `1abb0f60…` (SpaceBud #9668) must land
  `price=980, buyer_price=1000, on_top_fee=20`.
- **Crate goldens** (39 scenarios + fee tests, extended by the lifecycle
  extraction) cover decode; walker tests cover what's new: outref buffer
  lifecycle (create → update → spend/delist → evict; bundle multi-asset
  listings), witness datum resolution, and **resume equivalence**
  (walk A→C ≡ walk A→B + checkpoint + resume B→C).
- **Corpus sanity queries** post-walk: fills/month vs known market
  history, venue split, buyer-price vs settlement distributions
  (wayup ≈ +2%, jpg-era ≈ 0, wayup-settled-jpg ≈ +2%), listing
  create/delist balance vs open-book size.

## Deployment / ops

- cardano-infra (Netcup RS 4000 G12), source-on-box build like
  `tools/capture-block`. Disk measured 2026-07-22: **784G free**; snapshot
  transient ≈ 250–280G. Since venue re-runs are expected (decision 4),
  default to **keeping the unpacked immutable db** between runs and
  refreshing the snapshot when a re-run needs newer history; document the
  keep/refresh toggle in `infra/docs/mitos-operations.md` on deploy.
- `mithril-client` **CLI** (not the SDK) for snapshot download+verify.
- Walk under `nice`/`ionice`; the co-tenant chain services' tip-follow is
  the thing to protect.
- Serve mode gets a systemd unit (mirroring the mitos units) in phase 2;
  walk mode stays operator-invoked.

## Phasing & delegation map

**[F] = needs careful design-holding (Fable). [O] = mechanical against
this doc (Opus).**

**Phase 0 — crate lifecycle extraction (prerequisite):**
- [F] Extract listing + offer lifecycle decode from the four live modules
  into `mitos-marketplace-decode`; modules become thin wrappers; goldens
  byte-identical. (Same shape as the earlier sales/offer-accept
  extraction — the risk is subtle semantic drift, e.g. jpg's
  hash-only-create no-fallback boot-stall behaviour must be preserved.)

**Phase 1 — walk + store + upload:**
- [F] Outref buffer + `DecodeTx` assembly: witness-datum resolution,
  redeemer↔input matching, multi-listing txs, bundle listings, lifecycle
  classification via the crate, checkpoint/resume semantics. This is
  where silent data corruption would live.
- [F] Validation harness (D1 cross-check differ + resume-equivalence).
- [O] Venue registry config loading + watched-set derivation.
- [O] mithril-client bootstrap scripting; chunk-iteration scaffold
  (pallas-hardano + pallas-traverse); venue-aware slot fast-skip.
- [O] sqlite schema/CRUD, per-venue cursors, CLI surface (clap),
  progress/logging.
- [O] DuckDB Parquet sealing job + per-venue partition manifest.
- [O] Worker-side ingest endpoint (token-gated, wraps `insert_events`) +
  batched uploader.

**Phase 2 — serve:**
- [O] axum read surface, token auth, slot-cursor pagination, health.
- [F] The union-view seam (Parquet + live sqlite months) and the aggregate
  endpoint contracts (incl. time-on-market derivation), once raw is proven.

**Phase 3 — chainsync follow (redesigned 2026-07-22, see "Follow mode"):**
- [F] Rollback correctness: volatile window (raw block CBOR table),
  boundary buffer checkpoint, RollBackward → truncate + replay.
- [O] pallas-network chainsync/blockfetch client, `--peer` config,
  intersect-at-cursor, merge into the serve process.
- [O] (optional, separate) config-gated o7s serve listener in the mitos
  bundle so follow can peer against localhost dolos.

**Phase 4 — push / webhook delivery (see [`MARKET_LEDGER_PUSH.md`](./MARKET_LEDGER_PUSH.md)):**
- Companion-like subscription + push for REACTIVE consumers (quests first;
  alerts/notifications later). Pull stays for analytical reads. `follow` (the
  writer) owns a `subscriptions` table + a delivery loop that reads new ledger
  rows per subscription and POSTs postcard `EventsPage` batches with a
  per-subscriber `(slot, rowid)` cursor + retry (at-least-once, idempotent
  receiver). Delivered from the SETTLED boundary cursor, not the live tip, so a
  volatile-window rollback never delivers a reversible event. This is the
  vehicle for retiring the mitos marketplace subscriptions: quests repoint from
  the mitos-fed DO feed to market-ledger push, then the live-book DO
  (`MarketIngressDO` + DO market tables + market-stats/listings endpoints) is
  removed (listings revert to Anvil).
