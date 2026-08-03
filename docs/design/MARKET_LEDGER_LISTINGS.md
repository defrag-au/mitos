# market-ledger — current-listings serving (Phase 5)

**Status:** DESIGN, agreed 2026-07-22. **Validation gate PASSED 2026-07-22** (Anvil
cross-check, below). Not built. Companion to [`MARKET_LEDGER.md`](./MARKET_LEDGER.md)
and [`MARKET_LEDGER_PUSH.md`](./MARKET_LEDGER_PUSH.md).

## Cross-check results (2026-07-22 — PASS, with one build directive)

Cross-checked market-ledger's current listings against Anvil (fully paginated)
for 6 policies spanning wayup-live, jpg, and SpaceBudz. Findings:

- **The OPEN BOOK matches Anvil EXACTLY.** Where the two differ from the event
  fold, Anvil agrees with the open book: 86ec26a9 → openbook 726 == Anvil 726;
  SpaceBudz → openbook 5 == Anvil 5. Clean wayup policies → fold==openbook==Anvil
  (b558ea5e 432/432, 8da763ce 499/499, 793aca91 277/277, all `fold_only=0`).
- **market-ledger is a complete SUPERSET — `anvil_only=0` on every policy.** It
  never misses a listing Anvil has.
- **The event FOLD carries ghosts** (spent listings whose delist/sale decode was
  missed): 482 on 86ec26a9, 593 on SpaceBudz, 4 on e96b7329, 0 on clean wayup
  ones. **⇒ BUILD DIRECTIVE: the `listings` table MUST be sourced from the open
  book (evict-on-spend, matches Anvil), NOT the event fold.** This is the single
  most important result — building from the fold would ship ghosts.
- **Wayup prices reconcile with Anvil exactly via the fee.** anvil/fold ratio
  clusters at **1.02041 = 50/49** (the wayup fee) + royalty variants — Anvil
  shows the buyer-total, market-ledger stores the payout-sum ask. Same
  settle-vs-buyer relation validated in the sales cross-check. **⇒ serve
  buyer-price (fold the fee, as the sales path does) for marketplace parity.**
- **jpg prices ARE recoverable (correction 2026-07-22).** Earlier "jpg
  unpriceable" was WRONG. The on-box data: **86% of jpg buffer entries
  (187,905 / 219,326) already carry the listing datum** (jpg publishes the
  preimage in the create tx's witness set / metadata; the walker resolves it
  into `outref_buffer.datum_bytes`). A pulled sample decodes to a clean price
  (payouts 2.5+1.0+46.5 = 50 ADA). The fold shows `price=0` only because the
  walker's create path is **payload-only** — it feeds the decoder the *inline*
  datum (empty for hash-only jpg) and never the resolved `datum_bytes`. So the
  price is dropped at DECODE time, not absent from chain. **Fix: decode the
  price from the buffer's `datum_bytes`** (mitos `decode_listing_datum`, plus
  the per-version jpg schemas from shared-crates `pipeline/datum-parsing` —
  `jpg-store-v{1,2,3,4}`, pure-pallas & portable — if jpg's field layout
  differs from wayup's). The remaining ~14% (no `datum_bytes`) are genuinely
  hash-only-unresolvable at create; price them retroactively when the listing
  is first spent (the preimage lands in the spend tx's witnesses), or leave
  null. Net: listings can be priced for ~all wayup + ~86% of jpg.
- **Venue coverage: no real gap.** Anvil returned only jpgstore + wayup across
  all samples — even for SpaceBudz, 0 spacebudz-venue listings. The spacebudz
  registry gap is nil in practice; revisit only if that changes.

## Motivation — market-ledger already holds the live book

market-ledger doesn't just serve history; its **open book** (the outref buffer)
is the exact set of currently un-spent watched outputs — i.e. the **live
listings**. Measured on the box 2026-07-22: **~240k open entries** (219k jpg +
20k wayup), the overwhelming majority carrying their datum, so prices are
decodable. Its event stream also has the full listing lifecycle (4.4M `listed`,
1.48M `delisted`, 405k `price_change`). It has everything needed to serve
current listings + floor — it just lacks a listings *query*.

This **reverses** the earlier "listings stay on Anvil" call. The mitos live-book
experiment failed on its *foundation* (flaky marketplace-subscription
streaming); market-ledger's foundation is the opposite — a deep validated walk,
crash-safe follow, byte-identical decode, and a persistent open book. Same
robustness that made the pricing/history cutover land cleanly. So this is the
right-ground version of the same idea, not a re-tread.

**The open book is authoritative, not approximate.** A listed asset is escrowed
in the listing script — the owner cannot move it without spending that UTxO,
which *is* a delist or a sale. So an un-spent listing UTxO genuinely holds the
asset: there is no "stale listing" failure mode. The set of un-spent listing
outputs in the open book == the set of real, transactable listings.

> **Terminology:** "venue" is market-ledger's word for a **marketplace**
> (jpg.store, wayup) — the entries in the pluggable venue registry
> (`venues.toml`). The `marketplace` field on events is the same thing under
> the worker's name for it.

**Validation gate (before cutting Anvil):** cross-check market-ledger's current
listings against Anvil for a sample of policies (count / price / asset set), and
audit **venue coverage** — market-ledger watches jpg + wayup; any marketplace
Anvil aggregates that isn't in the venue registry is a blind spot until added
(the registry is pluggable). Don't cut Anvil until both check out.

## Design principle — history and listings are SEPARATE endpoints

(User directive 2026-07-22.) `/events` (historical, immutable) and `/listings`
(current, mutable live book) are **distinct endpoints with distinct wire
types**, so a consumer that needs both — e.g. a collection detail page showing
sales history *and* the current floor/listings — fires the two requests
**concurrently** rather than serially. They're different data, different query
shapes (paginated append-only log vs a snapshot of the current book), and the
parallelism is a real page-load win.

## Data model — a materialized `listings` projection

A new sqlite table, the query-ready projection of the open book with the price
**decoded** (the buffer stores raw datums; decoding per-request would be too
slow):

```sql
CREATE TABLE IF NOT EXISTS listings (
    policy_id       TEXT    NOT NULL,
    asset_name_hex  TEXT    NOT NULL,
    venue           TEXT    NOT NULL,   -- which marketplace it's on (attribute)
    price_lovelace  INTEGER NOT NULL,
    seller_stake    TEXT,
    tx_hash         TEXT    NOT NULL,   -- the current listing UTxO
    output_index    INTEGER NOT NULL,
    listed_slot     INTEGER NOT NULL,   -- created / last repriced
    listed_time     INTEGER NOT NULL,
    PRIMARY KEY (policy_id, asset_name_hex)
);
CREATE INDEX IF NOT EXISTS idx_listings_policy ON listings(policy_id, price_lovelace);
```

**One active listing per `(policy, asset)`** — the asset is escrowed in the
listing script, so it can be in at most one listing UTxO at a time (on one
venue). `venue` is therefore an attribute, not part of the key; a reprice is a
clean in-place UPDATE.

**Source of truth = the open book, NOT the event fold** (proven by the
cross-check: the fold accumulates ghost listings when a spend isn't decoded into
a `delisted`/`sold` event, e.g. 482 ghosts on one policy; the `outref_buffer`
evicts on *any* spend and matched Anvil exactly). So the `listings` table is a
**decoded projection of the buffer's listing subset**, maintained by the
buffer's own operations, not by the lifecycle events:

- **buffer INSERT** of a listing-channel output → decode its datum → upsert
  `(policy, asset)` with price/seller/oref/slot. (A reprice is a spend+produce,
  i.e. an evict of the old UTxO + insert of the new — nets to an in-place
  update.)
- **buffer EVICT** (`take`, on any spend) of a listing output → delete the row.
- Price is `decode_listing_datum(datum_bytes)` payout-sum. **Fold the venue fee**
  to serve buyer-price (wayup `+price/49`, matching the sales path). This decodes
  ~all wayup + ~86% of jpg (whose `datum_bytes` the walker already resolved from
  the create tx). jpg may need the per-version schemas from shared-crates
  `pipeline/datum-parsing` (`jpg-store-v{1,2,3,4}`, portable pallas) if its field
  layout differs from wayup's — validate the decoder against jpg datums. The
  ~14% of jpg listings with no `datum_bytes` (hash-only, unresolvable at create)
  get a null price now, backfilled when first spent (preimage in the spend tx).

**Seed** at walk-end / follow-start by decoding every listing-channel entry
already in the buffer (complete back to 2022 from the deep walk). The lifecycle
events are NOT trusted for removal. A periodic reconcile (table vs buffer
listing-set) is still cheap insurance.

Offers are excluded — the buffer holds offer UTxOs too; filter to the **Sale**
channel via the venue registry (`venue.rs` `Channel::Sale`; jpg = fixed listing
address, wayup = payment-cred match).

## Endpoint — `GET /listings`

Bearer-open like `/events` (the whole surface is open per the 2026-07-22 call).

```
GET /listings?policy=<56hex>[&venue=][&limit=][&cursor=]
```

Response: `application/octet-stream`, a postcard **`ListingsPage`** (new wire
type in `market-ledger-wire`, interned like `EventsPage`):

```rust
pub struct ListingsPage {
    pub version: u8,                 // byte 0 — same versioning contract
    pub policies: Vec<[u8; 28]>,
    pub sellers: Vec<String>,        // interned bech32 stakes
    pub venues: Vec<String>,
    pub listings: Vec<Listing>,      // ordered by price ASC (floor first)
    pub floor_lovelace: Option<u64>, // MIN(price) over the filtered set
    pub count: u32,                  // total listings for the policy
    pub next_cursor: Option<String>,
}
pub struct Listing {
    pub policy: u32,                 // idx into policies
    pub asset_name: Vec<u8>,
    pub price_lovelace: u64,
    pub seller: Option<u32>,         // idx into sellers
    pub venue: u32,                  // idx into venues
    pub tx_hash: [u8; 32],
    pub output_index: u32,
    pub listed_slot: u64,
    pub listed_time: i64,
}
```

`floor_lovelace` + `count` ride in the page header so a floor/summary needs no
second call. Ordered price-ASC so the floor is `listings[0]` and "cheapest N" is
a truncated read. `?format=json` debug hatch as on `/events`.

## Consumers

- The sell-analysis surfaces (the worker's `/api/market-listings` — the "Anvil
  retirement path") and the frontend listing/floor views repoint from Anvil to
  market-ledger `/listings`.
- Detail-page pattern: fire `/events` (history, for the sales timeline) and
  `/listings` (current book + floor) **in parallel**; render each as it lands.
- Wire type decoded via `market-ledger-wire` (already a workspace dep in
  cnft.dev-workers after the D1 cutover).

## Relationship to the mitos live-book removal (S4)

This revises S4: the mitos live-book (`MarketIngressDO` + DO market tables +
`/api/market-stats` / `/api/market-listings`) still gets removed — but its
replacement is **market-ledger `/listings`**, not Anvil. So the end state is
market-ledger-authoritative for history *and* listings, Anvil retired, mitos
live-book gone.

## Implementation slices

0. **Anvil cross-check** — DONE 2026-07-22 (PASS; see top). Validation gate
   cleared.
1. **Listing-datum price decode.** Validate mitos `decode_listing_datum` against
   real jpg datums (fixture = a pulled buffer datum, e.g. the 50-ADA sample); if
   jpg's field layout (owner-first) isn't handled, port the per-version schemas
   from shared-crates `pipeline/datum-parsing` (`jpg-store-v{1,2,3,4}`, portable
   pallas, price = Σ payouts). Output: `datum_bytes → Option<price_lovelace>`
   for jpg + wayup.
2. `listings` table + `store.rs` upsert/delete/query helpers.
3. **Maintenance (buffer-driven, NOT event-fold — cross-check proved the fold
   ghosts).** Hook the outref buffer: on INSERT of a Sale-channel output →
   decode its `datum_bytes` (slice 1) → upsert the listing (fee-folded
   buyer-price); on EVICT (`take`, any spend) → delete. Seed the table from the
   existing buffer at follow-start (decode every Sale-channel entry). Null price
   for the ~14% jpg with no `datum_bytes`; backfill at spend later.
4. `ListingsPage` wire type in `market-ledger-wire` + encode helpers.
5. `GET /listings` handler (filter, price-ASC order, floor/count header,
   cursor) reusing the serve query/encode scaffolding.
6. Repoint consumers (worker `/api/market-listings` + frontend) to
   `/listings`; parallelize the detail-page fetches.

## Resolved during design (2026-07-22)

- An asset is escrowed in the listing script, so it can be listed at most once at
  a time — the table PK is `(policy, asset)` and a reprice is an in-place UPDATE.
- No stale-listing failure mode: the asset can't leave the script without a
  delist/sale spending the UTxO, so the open book is authoritative.
- Floor is a single collection-wide `MIN(price)` — **no per-venue floor.**
