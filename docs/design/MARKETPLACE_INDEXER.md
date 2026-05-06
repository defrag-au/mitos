# Marketplace indexer

> **Scope: this doc describes the legacy in-tree static-crate
> shape** at `crates/marketplace-indexer/`, including the
> per-policy CF fan-out topology, the SQL schema, and the HTTP
> routes the consumer worker would expose. The crate has the
> decode logic but is currently flagged "legacy, slated for
> retirement, no live subscribers" (`crates/marketplace-indexer/src/lib.rs`).
>
> **For new marketplace-event consumers**, the canonical shape
> is a companion-module pair in the consumer's repo — see
> `../strategy/MITOS_COMPANION_PATTERN.md`, `../HOWTO_FIRST_MODULE.md`,
> and `MITOS_BUILD.md`. The decode logic in this doc is portable
> (event taxonomy, datum shapes, brand resolution); the
> deployment topology (per-policy router DO, mounted HTTP routes
> on the bundle) is the legacy shape.
>
> The schema, routes, and fan-out diagrams below are recorded as
> a transitional spec — useful when porting the decode logic
> into a wasm module + companion DO pair, less useful as a
> blueprint for "where does this code live."

A second indexer for mitos that ports the existing classifier's
decode logic (sales, listings, offers, collection offers) into the
chain-projection model.

This doc captures the design before any code lands. Refer back when
the work is actually picked up — it'll save re-deriving decisions.

Cross-references:
- `CF_REPLICATION.md` — the protocol this indexer rides on
- `INDEXER_TRAIT.md` — the `Indexer<D>` contract every indexer
  implements
- `ROADMAP.md` step 8 — the planned `mitos-protocol` extraction;
  marketplace event types live there too
- `cnft.dev-workers/pipeline/classifier/` — the existing decode
  logic this indexer ports
- `cnft.dev-workers/workers/collection-ownership/` — the existing
  per-policy DO whose shape the consumer worker mirrors

## Why this fits

The existing classifier worker does a lot in one place: receives a
TX from `captain-hook`, walks every output and input, identifies
marketplace contracts (jpg.store, dropspot, wayup, etc.), decodes
datums to determine event type, extracts seller/buyer/price, and
routes to downstream queues + DOs. The decode logic — the "what is
this transaction in human terms" judgment — is the genuinely
valuable part. The routing is just plumbing.

Mitos already has the chain locally and runs typed indexers. A
`MarketplaceIndexer` does exactly what the classifier's decode does,
without paying provider API costs and without coupling decode to
routing. Each consumer subscribes to whatever event types it cares
about; routing logic lives entirely in the consumer.

This is the same pattern `OwnershipIndexer` already establishes —
chain-derived state in mitos, side-effects in CF — applied to
events instead of state.

## Scope

In:
- **Sales** — token-for-ADA on a marketplace contract.
- **Listings** — asset placed at a marketplace contract awaiting a
  buyer. Distinct from the bare on-chain transfer that
  `OwnershipIndexer` would also see — but more meaningful as a
  first-class event.
- **Unlistings** — listing cancelled, asset returned to seller.
- **Per-asset offers** — `OfferCreate`, `OfferAccept`, `OfferCancel`.
- **Collection offers** — `CollectionOfferCreate`,
  `CollectionOfferAccept`, `CollectionOfferCancel`.
- **Marketplace contracts watched**: jpg.store v1/v2/v3, dropspot,
  wayup, plus any others currently decoded by the existing
  classifier. The watch list is fixed at indexer-init time, not
  per-consumer (consumers can scope to a policy, not to a
  marketplace).

Out (deliberately):
- **Alert rule routing.** Stays in CF. A consumer worker subscribes
  to the marketplace feed, applies user rules, fires Discord
  notifications via the relay. No alert logic in mitos.
- **Asset-refresh queue triggering.** Same — that's a CF-side
  side-effect, driven by mitos events as input.
- **System wallet detection.** Worker-side; mitos doesn't know
  about cnft.dev's operational wallets.
- **Cross-marketplace fungibility/joining.** The MarketplaceIndexer
  emits events tagged with the originating marketplace; consumers
  decide whether to merge across them.

Out (Phase 5+):
- **Sale price normalisation across multi-asset bundles.** The
  classifier today has some heuristics for bundle pricing; we'd
  port them as a follow-up once the single-asset path is solid.
- **Royalty resolution.** The fee distribution is recoverable from
  the same TX outputs; emit alongside the Sale event when present.

## Event taxonomy

Variants live in `mitos_protocol::Marketplace` — the
kind-as-outer / brand-as-data layout from
`SUBSCRIPTION_MECHANICS.md`. The crate is shared between mitos
and CF workers, eliminating wire-format drift. The event taxonomy
below is illustrative of the *Phase 0* shape used in the existing
classifier worker; Phase 2 of the subscription-mechanics rollout
replaces it with `mitos_protocol::Marketplace`.

```rust
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum MarketplaceEvent {
    Sale {
        policy_id: String,
        asset_name: String,        // hex
        asset_fingerprint: String, // CIP-14 (asset1...); see CF_REPLICATION.md
        seller_stake: Option<String>,
        buyer_stake: Option<String>,
        seller_address: String,    // full bech32, granular
        buyer_address: String,
        price_lovelace: u64,
        royalties_lovelace: Option<u64>,
        marketplace: Marketplace,
        tx_hash: String,
    },
    Listing {
        policy_id: String,
        asset_name: String,
        asset_fingerprint: String,
        lister_stake: Option<String>,
        lister_address: String,
        price_lovelace: u64,
        marketplace: Marketplace,
        listing_tx_hash: String,   // identifies the listing UTxO
        listing_output_index: u32,
    },
    Unlisting {
        policy_id: String,
        asset_name: String,
        lister_stake: Option<String>,
        marketplace: Marketplace,
        listing_tx_hash: String,   // points back to the prior Listing
        unlist_tx_hash: String,
    },
    OfferCreate {
        policy_id: String,
        asset_name: String,        // specific asset target
        offerer_stake: Option<String>,
        offerer_address: String,
        price_lovelace: u64,
        marketplace: Marketplace,
        offer_tx_hash: String,
        offer_output_index: u32,
    },
    OfferAccept { /* mirrors Sale shape; cross-linked via offer_tx_hash */ },
    OfferCancel { /* mirrors Unlisting shape */ },
    CollectionOfferCreate {
        policy_id: String,         // policy-wide target, no asset_name
        offerer_stake: Option<String>,
        offerer_address: String,
        price_lovelace: u64,
        target_traits: Option<Vec<TraitFilter>>,
        marketplace: Marketplace,
        offer_tx_hash: String,
        offer_output_index: u32,
    },
    CollectionOfferAccept { /* … */ },
    CollectionOfferCancel { /* … */ },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Marketplace {
    JpgStoreV1,
    JpgStoreV2,
    JpgStoreV3,
    Dropspot,
    Wayup,
    // grow as new marketplaces are added
}
```

Notes on shape:
- Always emit both `*_stake` and `*_address` per the
  `OwnershipIndexer` precedent — consumer chooses which to dedupe
  on, mitos stays neutral.
- Stake fields are `Option` because not every wallet has a stake key
  (enterprise, byron). Same handling as ownership.
- `tx_hash` + `output_index` on listing/offer create events lets
  consumers correlate Accept/Cancel events back to the original
  listing/offer.
- `Marketplace` enum is closed (not a string) so consumers can
  match exhaustively.

### Subscription scope: none

```rust
type Scope = ();
```

**Marketplace events are emitted as a single global feed**, not
scoped per-policy. The CF worker subscribes once and receives
every marketplace tx on chain. Per-policy fan-out happens on the
CF side (see "Per-DO multi-feed pattern" below).

This is the meaningful architectural divergence from
`OwnershipIndexer`. Ownership state is *naturally per-policy*
(asset X belongs to policy Y, mutating Y's DO is well-defined).
Marketplace events are *naturally global*: thousands of policies
trade through the same handful of marketplace contracts every
day. Pre-registering each one as a separate subscription would be
absurd — and missing one means missing the corresponding sales,
which is silent data loss.

The shape that fits: emit everything from mitos, route on receipt
in CF. Policies the CF worker doesn't care about: drop the event
(or route to a never-created DO that 404s on read — both fine).
"What policies are we watching" is a CF-side concern, not a
mitos-side concern.

## CF-side topology

Two indexers feed the CF worker, but their connection patterns
differ because their semantics differ:

**`collection-ownership`** is per-policy. One WebSocket per
watched policy_id; mitos's `Replicator` opens N connections to
N per-policy DOs, each one scoped via `OwnershipScope { policy_id }`.

**`marketplace`** is global. **One WebSocket total.** A single
"router DO" (or just the worker's `fetch` handler) receives the
full feed, extracts `policy_id` from each event, and forwards to
the relevant per-policy DO via `idFromName(policy_id).get_stub()`.

```
                    ┌───────────────────────────┐
   mitos box ──────►│ ownership-mitos.cnft.dev/ │
                    │  _internal/replicate/     │
                    │   collection-ownership    │  one WS per
                    │   ?policy_id=abc...       │  watched policy
                    └─────────────┬─────────────┘
                                  ▼
                        ┌─────────────────┐
                        │ DO[policy=abc]  │  ◄── ownership state
                        └─────────────────┘

                    ┌───────────────────────────┐
   mitos box ──────►│ marketplace-mitos.cnft.dev│
                    │  /_internal/replicate/    │  one WS, period
                    │   marketplace             │
                    └─────────────┬─────────────┘
                                  ▼
                       ┌─────────────────────┐
                       │  Router (the worker │
                       │   fetch handler or  │
                       │   a single DO)      │
                       └────┬───┬───┬────────┘
                            │   │   │
                  policy=abc▼   │   ▼ policy=ghi
                  (watched)     │   (unwatched, drop)
                                ▼
                          policy=def
                          (watched)
                       ┌─────────────────────┐
                       │ DO[policy=abc]      │ ◄── sales/listings/offers
                       │ DO[policy=def]      │ ◄── sales/listings/offers
                       └─────────────────────┘
```

A "watched" policy is one some downstream user has expressed
interest in tracking — represented in CF KV or D1 as a small
allowlist (`marketplace_watched_policies` keyed by policy_id).
The router checks membership before forwarding; if absent, the
event is dropped. Adding/removing watched policies is an admin
operation that doesn't require touching the mitos side.

Same DO instance still holds both ownership AND marketplace state
for a watched policy (the DO is keyed only on `policy_id`), so
queries like "current owner + recent sales for asset X" join
naturally. The two feeds reach the same DO via different paths
— one direct (ownership's per-policy WS), one mediated by the
router (marketplace's global feed).

The DO's writes for marketplace events use the Hibernation API
the same way ownership does — but only on the *router's*
WebSocket. Per-policy DOs receive marketplace updates as
ordinary fetch invocations (RPC-style) from the router, not via
WebSockets of their own. Cost-wise that's a slightly more active
DO than pure-hibernation, but negligible.

The previous design — two WSs per policy, tagged via
`accept_web_socket_with_tags` — applied to both feeds. Marketplace's
global feed sidesteps that entirely; only ownership uses the
WebSocket-per-policy pattern. The tag-attachment design is
recorded below as a fallback if we later decide to scope
marketplace per-policy after all (e.g. for a high-volume policy
where the router becomes a bottleneck).

```rust
fn handle_replicate_upgrade(&self, indexer: &str, policy_id: &str) -> Result<Response> {
    let pair = WebSocketPair::new()?;
    let server = pair.server;
    self.state.accept_web_socket_with_tags(&server, &[indexer]);
    // ... send Subscribe message with the policy_id-shaped scope ...
    Response::from_websocket(pair.client)
}

async fn websocket_message(&self, ws: WebSocket, msg: WebSocketIncomingMessage) {
    let tags = ws.tags(); // exact API name TBC vs the workers-rs version
    let indexer = tags.first().map(String::as_str).unwrap_or("");
    let bytes = match msg { /* ... */ };
    match indexer {
        "collection-ownership" => self.apply_ownership_change(bytes).await,
        "collection-marketplace" => self.apply_marketplace_event(bytes).await,
        _ => warn!(?tags, "unknown WS tag"),
    }
}
```

The protocol envelope (`ServerMessage::Apply { cursor, change: bytes }`)
is identical regardless of indexer; the tag tells the DO how to
interpret the `change` bytes (which `mitos-protocol` change type to
deserialise into).

## DO schema (post-marketplace)

The DO grows from "ownership only" to "everything we know about
this policy". Existing tables stay, new ones added:

```sql
-- existing (gains asset_fingerprint as part of this work)
CREATE TABLE ownership (
    asset_name_hex TEXT PRIMARY KEY,
    asset_fingerprint TEXT NOT NULL,    -- CIP-14, indexed for lookup
    owner_address TEXT NOT NULL,
    owner_stake TEXT
);
CREATE INDEX idx_ownership_fingerprint ON ownership(asset_fingerprint);
CREATE INDEX idx_ownership_stake ON ownership(owner_stake);
CREATE INDEX idx_ownership_address ON ownership(owner_address);

-- new

-- One row per completed sale. Append-only with periodic prune
-- (e.g. 30 days) to bound storage.
CREATE TABLE sales (
    tx_hash TEXT PRIMARY KEY,
    asset_name_hex TEXT NOT NULL,
    asset_fingerprint TEXT NOT NULL,
    seller_stake TEXT,
    seller_address TEXT NOT NULL,
    buyer_stake TEXT,
    buyer_address TEXT NOT NULL,
    price_lovelace INTEGER NOT NULL,
    royalties_lovelace INTEGER,
    marketplace TEXT NOT NULL,
    slot INTEGER NOT NULL,
    timestamp INTEGER NOT NULL DEFAULT (unixepoch())
);
CREATE INDEX idx_sales_asset ON sales(asset_name_hex);
CREATE INDEX idx_sales_fingerprint ON sales(asset_fingerprint);
CREATE INDEX idx_sales_buyer_stake ON sales(buyer_stake);
CREATE INDEX idx_sales_seller_stake ON sales(seller_stake);
CREATE INDEX idx_sales_slot ON sales(slot);

-- Current state of listings — removed on Unlisting or Sale,
-- never accumulates. The composite primary key handles relisting:
-- a single asset can be listed at multiple marketplaces over time
-- but only one listing-per-marketplace at a time.
CREATE TABLE listings (
    asset_name_hex TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    lister_stake TEXT,
    lister_address TEXT NOT NULL,
    price_lovelace INTEGER NOT NULL,
    listing_tx_hash TEXT NOT NULL,
    listing_output_index INTEGER NOT NULL,
    listed_at_slot INTEGER NOT NULL,
    PRIMARY KEY (asset_name_hex, marketplace)
);
CREATE INDEX idx_listings_price ON listings(price_lovelace);

-- Current state of offers — both per-asset and collection-wide.
-- Removed on Accept/Cancel.
CREATE TABLE offers (
    offer_tx_hash TEXT NOT NULL,
    offer_output_index INTEGER NOT NULL,
    asset_target TEXT,        -- NULL for collection offers
    offerer_stake TEXT,
    offerer_address TEXT NOT NULL,
    price_lovelace INTEGER NOT NULL,
    target_traits_json TEXT,  -- NULL for non-trait-filtered
    marketplace TEXT NOT NULL,
    created_at_slot INTEGER NOT NULL,
    PRIMARY KEY (offer_tx_hash, offer_output_index)
);
CREATE INDEX idx_offers_asset ON offers(asset_target);
CREATE INDEX idx_offers_offerer_stake ON offers(offerer_stake);

-- Append-only audit of every marketplace event. Lets a consumer
-- reconstruct an arbitrary historical state without an op-log on
-- the indexer side. Pruned after retention window.
CREATE TABLE marketplace_events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    slot INTEGER NOT NULL,
    tx_hash TEXT NOT NULL,
    event_type TEXT NOT NULL,
    asset_name_hex TEXT,
    marketplace TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    timestamp INTEGER NOT NULL DEFAULT (unixepoch())
);
CREATE INDEX idx_events_slot ON marketplace_events(slot);
CREATE INDEX idx_events_asset ON marketplace_events(asset_name_hex);
```

## Read API additions

Worker-level (each routes to its per-policy DO):

- `GET /api/sales/:policy?since=<slot>&limit=<N>` — recent sales,
  optionally filtered by stake (buyer or seller via `?stake=`) or
  by `?fingerprint=asset1...` for a single asset's sale history.
- `GET /api/listings/:policy[?marketplace=...]` — current listings.
- `GET /api/floor/:policy[?marketplace=...]` — derived
  `MIN(price_lovelace)` over listings.
- `GET /api/offers/:policy?stake=<offerer>&kind=<asset|collection>` —
  current offers, filtered.
- `GET /api/asset-history/:policy?asset=<name_hex>` — joined view
  across `sales`, `marketplace_events`, `ownership_changes` for one
  asset. Also accepts `?fingerprint=asset1...` as an alternate
  primary key.
- `GET /api/by-fingerprint/:fingerprint` — owner + listing + offer
  state for one asset, addressed only by CIP-14 fingerprint. The
  worker resolves the policy_id from the fingerprint via the
  `policy_id` column on whatever indexed table contains it (or the
  caller supplies `?policy=...` as a hint to skip the resolve).
  Useful for "the user pasted a jpg.store URL into a search bar"
  flows.

These mirror what the existing `cnft.dev` ecosystem already exposes
through other code paths, presented from a single per-policy DO.

## Worker rename

`collections-mitos` becomes a misnomer once it consumes
non-ownership feeds. Suggested rename: **`collections-mitos`**.
Routes move from `ownership-mitos.cnft.dev/api/...` to
`collections-mitos.cnft.dev/api/...` (or the existing
`collections.cnft.dev` if available — naming convention TBD).

The DO class also renames: `MitosOwnershipDO` →
`MitosCollectionDO`. Existing data needs migration; the
`/_admin/reset` path doubles as the migration tool.

## Reusing `cardano-assets` from shared-crates

The shared-crates `cardano-assets` crate already provides most of
what mitos needs — `AssetId` (typed `(policy_id, asset_name_hex)`
wrapper with validation + multi-format `Display`/`FromStr`),
`TxHash` newtype, CIP-14 fingerprint computation behind the
`cip14` feature, `TokenType` for CIP-25/CIP-68 classification,
asset-name decoding helpers, and a `Resolver` for policy metadata
lookups.

**Mitos depends on it directly** rather than re-implementing.

### Honest gaps to close upstream first

`cardano-assets` is "partially typed". `AssetId` is a real newtype
but its `policy_id: String` field is a bare string, and
`AssetId::fingerprint()` returns `Result<String, AssetIdError>`.
The same for asset names — bare hex strings throughout. To use
this safely from indexer code, three small newtypes are warranted:

```rust
// Add to cardano-assets:
pub struct PolicyId(String);          // 56-char lowercase hex, validated
pub struct Fingerprint(String);       // CIP-14 "asset1..." bech32, validated
pub struct AssetNameHex(String);      // variable-length hex (or relax to String — style call)
```

`AssetId.policy_id` field type changes from `String` to
`PolicyId`. `AssetId::fingerprint()` returns
`Result<Fingerprint, AssetIdError>`. The change is breaking on the
crate surface — every existing call site that takes/returns a bare
string for these concepts updates — but the validation lives in
one place from then on, and downstream code can't accidentally
mix up "this 56-char hex is a policy id" with "this 64-char hex is
a tx hash".

### Development workflow: patch overrides

Upstreaming this normally requires a coordinated release —
shared-crates publishes, then mitos and cnft.dev-workers update
their version pin. We don't need that overhead during prototype
iteration.

Both downstreams (mitos workspace, cnft.dev-workers workspace) add
a `[patch]` section pointing at a local shared-crates checkout
for the duration of the work:

```toml
# in both mitos/Cargo.toml and cnft.dev-workers/Cargo.toml
[patch."https://github.com/defrag-au/shared-crates"]
cardano-assets = { path = "../shared-crates/cardano-assets" }
```

(Path adjusted to wherever the user has shared-crates checked out
relative to each workspace; convention is sibling repos under
`~/code/defrag/`.)

That lets a single shared-crates branch power both downstreams
during the migration. Land the upstream changes, validate both
downstreams build + test cleanly, then remove the patch overrides
and bump the published version.

Other shared-crates that benefit from the same approach:
`cardano-tx::dex::*` (DEX decode), parts of `tx-classifier`
(marketplace decode), shared address-handling helpers — audit
during the marketplace indexer's Phase A and decide case-by-case.

### Required prerequisite: shared-crates devenv

Shared-crates doesn't currently have a `flake.nix` of its own.
Mirror what mitos and cnft.dev-workers do — `defrag-nix`'s
`rust-worker-stack` shell — so anyone working on shared-crates
gets the same toolchain without piggybacking on an adjacent repo.
Same content as mitos's `flake.nix`, copy `flake.lock` for
deterministic resolution.

Lands as a small standalone PR before the typed-newtypes work
starts.

### Public-vs-private dep direction

Mitos is going public; shared-crates is private. **End-state:
cardano-assets becomes a public crate** (extracted to its own repo
or published to crates.io). Both shared-crates and mitos depend on
it. The patch-override workflow is the bridge during development;
the public extraction is the bridge before mitos's public release.

The clean migration sequence:
1. Add `flake.nix` to shared-crates.
2. Land typed `PolicyId` / `Fingerprint` (+ optionally
   `AssetNameHex`) in cardano-assets via patch overrides.
3. Validate both mitos and cnft.dev-workers build + tests pass.
4. Bump shared-crates version, remove patch overrides.
5. (Future) Extract cardano-assets to its own public location;
   both shared-crates and mitos depend on it from there.

Step 5 is independent of the prototype work; it's a release-
engineering concern that lands when mitos actually goes public.

## Required protocol additions

These are small (~hours each) and unblock the marketplace work
without touching `OwnershipIndexer`. Land them as a prep step
before the marketplace indexer itself.

1. **URL-path-tagged replicate route**:
   `/_internal/replicate/{indexer}?policy_id=X` instead of just
   `/_internal/replicate?policy_id=X`. Worker's `route_to_do` reads
   the `{indexer}` segment and forwards as `X-Mitos-Indexer`
   header. The DO uses it to call
   `accept_web_socket_with_tags(&server, &[indexer])`. Backwards
   compatible: keep the un-tagged route working for existing
   ownership subscribers during the cutover.

2. **DO tag attachment + read** in the worker. Verify the
   `worker = "0.7"` crate supports `accept_web_socket_with_tags`
   and a `ws.tags()` lookup; port if it's behind a feature flag or
   missing. If missing, use `ws.serialize_attachment(&{indexer:
   "..."})` as a fallback (each WS gets a piece of attached state).

3. **Per-indexer scoped reset**:
   `POST /_admin/reset/:policy?indexer=collection-marketplace` wipes
   only that indexer's tables. The all-tables wipe (no `?indexer`)
   stays as the schema-migration escape hatch.

4. **`Replicator::add` ergonomic addition**: a registration UX that
   takes a policy_id and registers all "applicable" indexer
   subscriptions in one call, so ops doesn't have to remember to
   add both ownership and marketplace separately. Optional, sugar
   not architecture.

## Migration plan

The classifier's current decode logic is the bulk of the work. Most
of it ports directly into mitos, but the data-shape transformation
is real.

### Phase A: Survey what's already in shared-crates

User flagged that classifier logic has been moving into
`shared-crates`. Before any new code, audit:

- What's already in `cardano-tx::dex::*` and similar — likely the
  most reusable bits.
- What's in `tx-classifier` (the workspace dep used by the existing
  classifier and ownership workers).
- What's still in the classifier worker itself (decode + routing
  intermingled).

The clean cut is: anything pure-decode (TX → typed event) belongs
in shared-crates and is reusable. Routing/queueing is classifier's
own.

### Phase B: Build `MarketplaceIndexer::handle_event`

For each block:
1. `pallas::ledger::traverse::MultiEraBlock::decode(&block)`.
2. For each TX, walk inputs (resolve via `domain.state().get_utxos`)
   and outputs.
3. Identify marketplace-contract interactions by output script
   address (table of known marketplace addresses).
4. Decode the relevant datum via shared decoders.
5. Emit one `MarketplaceEvent` per logical event.
6. For `Accept` and `Cancel` events, the input UTxO references the
   prior listing/offer — emit with the cross-link.

Bulk: ~1-2 weeks of focused porting. Edge cases: bundle sales
(multiple assets in one TX), cross-marketplace migrations, datum
schema versions per marketplace contract revision.

Validation: parallel-run against the existing classifier's output
for a known historical block range. Both should emit equivalent
events for the same blocks.

### Phase C: Build the DO write path

Mirror `apply_change` in the DO for `MarketplaceEvent`:

```rust
async fn apply_marketplace_event(&self, bytes: Vec<u8>) -> Result<()> {
    let event: MarketplaceEvent = decode(bytes)?;
    match event {
        MarketplaceEvent::Sale { ... } => self.apply_sale(...).await,
        MarketplaceEvent::Listing { ... } => self.apply_listing(...).await,
        // ...
    }
}
```

Each variant maps to a SQL statement set: insert into the relevant
table, remove from `listings` on Sale or Unlisting, append to
`marketplace_events`.

Per the protocol design, inserts must be idempotent — the DO will
sometimes see the same record twice on reconnect. Use upserts and
check existence-before-write where the schema doesn't naturally
deduplicate (sales table's `tx_hash` PK handles dedup naturally;
events use `(slot, tx_hash, event_index)` for the same).

### Phase D: Read APIs

Mechanical — straight SQL queries with json output. Each API mirrors
the existing classifier-fed worker shape where possible (so a
diff-harness can verify parity against the production classifier).

### Phase E: Cutover

Same shape as the ownership migration: parallel-run the new
`collections-mitos` worker alongside the existing classifier-fed
infrastructure for 30+ days, diff-harness comparing event-by-event
emissions. When divergence stays at zero across reorgs and edge
cases, retire the classifier path for migrated policies.

## Open questions, parked

- **Cross-marketplace event correlation**: a single TX might hit
  multiple marketplaces (rare). Today the classifier emits one
  event per match; mitos should do the same. Verify this is
  consistent across all ports.

- **Historical replay correctness**: when we backfill a freshly
  subscribed consumer, do we emit historical sales/listings? The
  ownership indexer answers this with current-state UTxO
  enumeration. Marketplace events are inherently historical —
  there's no "current state" of a sale. Two options:
  - Cold subscribes get *only* live tail; historical events
    require a separate batch endpoint or snapshot.
  - Backfill enumerates the chain since some configurable
    cutoff, decoding marketplace events.
  Lean toward (1) — marketplace events are notifications, not
  state. Consumers wanting historical pull from a separate
  archive. Listings/offers are state — backfill *should* enumerate
  current open listings/offers from current marketplace contract
  UTxOs, same as ownership does.

- **Datum schema evolution per marketplace**: when a marketplace
  upgrades its contract (jpg.store v2 → v3), datum decoding has to
  support both. Shared decoders should be versioned. Currently the
  classifier handles this via best-effort + fallback; we should
  carry that forward but consider whether a stricter "fail loudly
  on unknown datum version" is a better default for mitos.

- **Reorg handling for marketplace events**: a sale at slot N that
  reorgs out is a fake notification if already delivered. The
  framework's `Undo` records reach the DO; the DO's
  marketplace-event tables would need to handle revert. Sale
  rollback = `DELETE FROM sales WHERE tx_hash = ?` (works because
  sales primary-key is tx_hash). Listing rollback is harder
  (Unlisting at slot N reorgs out → listing should reappear, but
  the DO has already deleted the row). Op-log pattern in
  `marketplace_events` lets us recover.

## Phasing summary

| Phase | Scope | Effort | Unblocks |
|---|---|---|---|
| Pre-A | Protocol additions (URL tagging, DO tags, scoped reset) | ~1 day | Multi-feed-per-DO model |
| A | Audit shared-crates for reusable decode | ~1-2 days | Phase B scope |
| B | `MarketplaceIndexer` decode logic | ~1-2 weeks | The actual feed |
| C | DO write path + schema | ~3-5 days | Reads work |
| D | Read APIs | ~2-3 days | Consumer integration |
| E | Parallel-run + cutover | 30+ days calendar (light coding) | Ship |

Total: roughly 3-4 weeks of focused work, plus calendar time for
the parallel-run validation.
