# Domain refactor: Mint, Burn, AssetMovement

Adds three top-tier `Domain` arms to `mitos-protocol` and retires
the bespoke `OwnershipChange` event type. Per-asset state
transitions become first-class events on the protocol stream;
ownership becomes a *projection* over those events on the consumer
side.

This doc supersedes the open question in
`SUBSCRIPTION_MECHANICS.md` ("Domain::None for state-only
indexers") — the answer is "they emit `Mint` / `Burn` /
`AssetMovement` like every other domain".

## Motivation

Three forces converging:

1. **Unified asset-lifecycle taxonomy.** Today, ownership changes
   travel on a separate `OwnershipChange` channel parallel to
   `ProtocolEvent`. Two event taxonomies for what's essentially
   "what happened to this asset" creates encoder/decoder drift
   risk and forces consumers to subscribe to two channels for
   the same logical concern.
2. **Mints as a first-class concern.** Open-mint monitoring,
   collection-discovery automation, and supply tracking all want
   "every mint on chain" as a subscription, not a per-policy
   filter on an ownership channel. Today that's not expressible.
3. **Residual classification.** When the marketplace indexer
   sees an asset transfer through an unrecognised script, it
   emits `Marketplace::Sale { brand: Unknown }` — but it can't
   actually know it was a sale (the redeemer wasn't decoded).
   The honest emission is "asset moved, we don't know why".
   That bucket needs a home.

## Implementation phasing

This doc captures the full design. **v1 ships the mechanical
structure only**, with consumer migrations deferred. The split:

**v1 (this refactor):**

1. New `Domain` arms: `Mint(MintPayload)`, `Burn(BurnPayload)`,
   `AssetMovement(AssetMovementPayload)`, plus selectors
2. `Indexer::handle_event` returns `Vec<MovementClaim>` (trait
   change touches every existing indexer; non-marketplace impls
   just return empty Vec)
3. **`none_match-indexer`** as the dispatcher's residual tail-
   step (reserved name, convention-over-configuration — the
   dispatcher recognises it structurally, not via config)
4. `mint-burn-indexer` (in-tree, walks `tx.mint`)
5. Marketplace indexer adapts to claim its movements (Sale,
   Listing, Unlisting)

After v1, mitos emits the new domains alongside the existing
ones. `OwnershipChange::Transfer` keeps flowing for current
consumers; nothing breaks.

**Deferred to later iterations:**

- Collections-mitos migration off `OwnershipChange` (consumer-
  side projection across multiple domains)
- `MarketplaceBrand::Unknown` removal + marketplace indexer
  flipping its `Unknown` path to abstain
- `OwnershipChange` retirement and `collection-ownership-indexer`
  removal
- `finalize.rs` companion-runtime hook (separate feature,
  pursued independently when a cross-event atomicity use case
  surfaces)

The minimal v1 scope above lets the **mint-watcher PoC** ship as
the first real consumer of the new domains, validating the design
end-to-end against real on-chain mint events.

## Design

### `Domain` gains three arms

```rust
pub enum Domain {
    Mint(MintPayload),                   // top-tier: asset created
    Burn(BurnPayload),                   // top-tier: asset destroyed
    AssetMovement(AssetMovementPayload), // residual: A→B, no domain claimed it
    Marketplace(Marketplace),            // unchanged
    Dex(Dex),                            // unchanged
    Lending(Lending),                    // unchanged
}
```

Mint and Burn are top-tier because they're conceptually distinct
from movement — a mint has no source address, a burn has no
destination. Forcing them under `AssetMovement` would require
either optional fields or sub-variants, both worse than just
having three siblings.

`AssetMovement` is the **residual** — what's left when no specific
domain claimed a transfer. It is NOT a parent type for
asset-centric marketplace events. Sales, listings, unlistings,
offer-accepts stay in `Marketplace`; they have rich brand-specific
context that a residual classification can't carry.

### Payload shapes

```rust
pub struct MintPayload {
    pub minter: String,    // first holder (typically the recipient address)
    pub amount: u64,       // 1 for NFTs; N for fungibles/RFTs
}

pub struct BurnPayload {
    pub previous_owner: String,
    pub amount: u64,
}

pub struct AssetMovementPayload {
    pub previous_owner: String,
    pub new_owner: String,
    pub amount: u64,
    // Future: `via_script: Option<Address>` to distinguish
    // wallet-to-wallet from movement-through-unrecognised-script.
    // Defer until a consumer needs it.
}
```

`policy_id`, `asset_name_hex`, `tx_hash`, `slot` already live on
the `ProtocolEvent` envelope — payloads only carry the fields
unique to the kind.

### Quantity semantics

`amount` is the quantity transferred *in this specific event*,
not the total holding. A tx that splits 100 tokens (30 to Bob,
70 back to self as change) emits one `AssetMovement` event with
`amount: 30` (the change leg is suppressed — see next section).
Same convention applies to `Mint` (per-output mint quantity) and
`Burn` (quantity reduced from input).

NFTs always have `amount: 1` — degenerate case of the same field,
not a special case.

### Same-wallet suppression (`AssetMovement` only)

`AssetMovement` is suppressed at indexer emission time when
`previous_owner` and `new_owner` resolve to the same wallet.
Sending change back to yourself is UTxO bookkeeping, not a
movement — emitting it creates noise every consumer would have
to filter.

**"Same wallet" definition:**

- **Both sides have a stake credential** → compare stake
  credentials. Equal stake creds = same wallet (HD-wallet change
  to a different payment address but the same stake key counts as
  same-wallet).
- **Either side lacks a stake credential** (enterprise addresses,
  Byron, script addresses) → fall back to bech32 address equality.

This covers ~99% of mainnet usage cleanly. The residual edge
cases (enterprise-only wallets, contract self-references) get
the conservative address-equality treatment.

**Useful invariant for downstream:** consumers can rely on
`previous_owner != new_owner` (at the wallet level). Per-policy
ownership projections never see self-transfers.

**Other variants:**

- **Mint** — no `previous_owner`, suppression rule doesn't apply.
  Self-mints (creator mints to their own wallet) are real events
  and emit normally.
- **Burn** — no `new_owner`, same. Always emits.
- **Marketplace::Sale / Listing / Unlisting / OfferAccept** —
  seller/buyer roles already distinguish parties. Self-sales
  through a marketplace are odd but real (price-discovery
  signal); leave them emitting and let consumers filter if they
  care.

## OwnershipChange retires

Under this taxonomy there is no `Domain::Ownership` arm —
ownership is the *projection* of multiple domain events over a
policy. Crucially, "ownership" and "status" are two independent
dimensions:

- `(asset → owner)` — persistent, who actually controls the asset
- `(asset → status)` — transient, currently held / listed / etc.

Listing an asset on a marketplace doesn't change its ownership;
the seller still owns it. It changes the asset's status until the
listing resolves (sold, unlisted, expired). The chain-level move
to the marketplace script is bookkeeping the indexer absorbs;
consumers see a status change, not an ownership change.

A per-policy projection consumer subscribes across the relevant
event types and folds them into the two tables:

| Event | `(asset → owner)` | `(asset → status)` |
|---|---|---|
| `Mint` | insert `asset → minter` | `held` |
| `Burn` | delete `asset` | (deleted) |
| `AssetMovement` | update `asset → new_owner` | `held` |
| `Marketplace::Sale` | update `asset → buyer` | `held` |
| `Marketplace::OfferAccept` | update `asset → bidder` | `held` |
| `Marketplace::ListingCreate` | (no change — seller still owns) | `listed` |
| `Marketplace::Unlisting` | (no change — never left) | `held` |

A holders-table consumer projects only the left column. A
"what's listed" consumer projects only the right column. A
frontend showing "my assets including listed" projects both and
joins.

The `OwnershipChange` type in
`cnft.dev-workers/types/collections-mitos-events` deprecates
entirely. Same for the dedicated `collection-ownership-indexer`
crate — its functionality folds into the mint/burn indexer +
fallthrough indexer described below.

## `MarketplaceBrand::Unknown` retires

Today's marketplace indexer emits `Marketplace::Sale { brand:
Unknown, ... }` when it sees a sale-shaped tx through an
unrecognised script. Under the new design that's wrong:
"unrecognised script" implies "we couldn't decode the redeemer",
which means we can't know the kind (Sale? Unlisting?
Offer-accept?) — only that an asset moved. Those route to
`Domain::AssetMovement` instead (per the dispatch mechanism
above). `Marketplace::*` becomes "we recognised both the brand
and the kind".

Consequence: `MarketplaceBrand::Unknown` is removed entirely.
The variant has no path to emission, so keeping it as a selector
member is misleading (consumers think they're filtering for
unrecognised marketplace events; nothing matches because those
events emit as `AssetMovement`). Same applies to
`OfferCancelPayload::Unknown { brand_script, policy_id, raw }` —
unrecognised cancel scripts move lovelace, not assets, and
lovelace is out of scope; nothing emits for them.

A consumer wanting "everything that looked marketplace-shaped
including stuff we couldn't classify" subscribes to both
`Domain::Marketplace(Any)` and `Domain::AssetMovement(Any)` and
unions. The "this specifically looked like a sale but we
couldn't pin the brand" signal is rarely actionable; preserving
it isn't worth the type-system noise. If a real consumer need
emerges, adding a `brand_script: Option<String>` soft-signal
field on relevant payloads is forward-compatible.

**Migration cost.** Consumers storing historical `ProtocolEvent`
data with `brand: Unknown` need either (a) re-fetch from mitos's
cursor replay, or (b) serde-tolerant deserialization to drop
unknown variants. Documented as a `mitos-protocol` major bump.
`SUBSCRIPTION_MECHANICS.md` examples that reference
`MarketplaceBrand::Unknown` get cleaned up in the same PR.

## Indexer architecture

The indexers reorganise around domains. Today there's
`collection-ownership-indexer` and `marketplace-indexer`. Under
the new shape:

- **`mint-burn-indexer`** (new). Watches each tx's `mint` field
  directly — Cardano transactions carry explicit per-asset mint
  quantities, positive for mints, negative for burns. No
  script-recognition needed. Emits `Domain::Mint` and
  `Domain::Burn`.
- **`marketplace-indexer`** (existing, scope tightens). Only
  emits `Domain::Marketplace(...)` when it recognises a
  brand+kind. Unrecognised marketplace-shaped txs no longer
  emit; they fall through.
- **`none_match-indexer`** (new — residual tail-step, reserved
  name). Runs *after* the specific-domain indexers and emits
  `Domain::AssetMovement` for any asset transfer not claimed by
  an earlier indexer. This is the home for P2P trades, transfers
  through unrecognised scripts, and movements-via-dApps that
  mitos doesn't yet have a domain for. **The reserved name is a
  convention** — the dispatcher recognises an indexer named
  `none_match` and runs it at the synchronisation point after
  all specific-domain indexers, rather than as a parallel peer
  task. Adding a new specific indexer requires no config change;
  removing residual handling is dropping the crate.
- **`collection-ownership-indexer`** (existing) **retires**. Its
  ownership-tracking responsibility was already a projection of
  what mint/burn/movement/marketplace events do; making the
  projection consumer-side is the structural fix.

### Dispatch mechanism

Indexers return both events and movement claims. The dispatcher
runs them, accumulates both, then emits residual `AssetMovement`
events for unclaimed movements.

**Architectural note:** today's mitos dispatcher runs each
indexer in its own task with no cross-indexer coordination
(verified via the agent-research pass over `mitos-core`). The
new dispatch mechanism introduces a **synchronisation point** —
specific-domain indexers run (order between them is unspecified
since claims are commutative), then the residual phase runs once
the prior indexers have processed the tx. The residual phase is
not a peer indexer with its own task; it's a dispatcher tail-
step that reads accumulated claims. This is a new architectural
concept relative to today's parallel-task model.

```rust
pub trait Indexer {
    fn classify(&self, tx: &Tx) -> ClassifyResult;
}

pub struct ClassifyResult {
    pub events: Vec<ProtocolEvent>,
    pub claimed: Vec<MovementClaim>,
}

pub struct MovementClaim {
    pub asset: AssetId,
    pub from: Address,
    pub to: Address,
}
```

The "fallthrough indexer" is not a peer indexer — it's a
dispatcher tail step. Single residual logic, owns the
same-wallet suppression rule alongside the claim subtraction.

**Pipeline:**

1. **Diff computation** builds the set of `(asset, from, to,
   amount)` movements from input vs output amounts per asset.
   `tx.mint` quantities are netted out so freshly-minted UTxOs
   and burns don't surface as movements.
2. **Mint/burn indexer** emits `Mint`/`Burn` events from the
   `tx.mint` field directly. Claims nothing (the diff already
   excluded these).
3. **Specific-domain indexers** (marketplace, dex, lending, ...)
   run and return their events + claimed triplets. Order between
   them is unspecified for v1 — claims are commutative for the
   residual computation. (See "Open questions" — revisit if a
   genuinely cross-domain tx surfaces.)
4. **Residual pass** emits `AssetMovement` for every movement
   from step 1 that is neither claimed nor suppressed by the
   same-wallet rule.

**Asset scope:** lovelace / ADA flow is excluded throughout. The
taxonomy is for native assets only.

**Listing claims:** `Marketplace::ListingCreate` claims `(asset,
seller, script_addr)`; `Marketplace::Unlisting` claims `(asset,
script_addr, seller)`. The residual sees them as covered and
skips emission. Consumer projections treat them as status
changes (per the projection table above), so the absence of an
`AssetMovement` here is consistent with "no ownership change".

**Overlap behaviour** (two indexers claiming the same triplet —
a classification bug; production indexers should be mutually
exclusive by script recognition):

- **Production:** log a warning. First claim wins for the
  residual pass (the duplicate claim is discarded; the residual
  treats the triplet as covered exactly once). Both indexers'
  events still emit on the wire — the protocol layer doesn't
  silently drop classifications. Consumers may see duplicate
  events; the log surfaces the misclassification for fixing.
- **Dev / test:** hard error. Mis-classification surfaces
  immediately rather than accumulating silent log warnings.

### Implementation notes (v1)

Decisions locked in for the v1 implementation, derived from a
deep read of `mitos-core/src/{indexer,dispatcher,handle,
emitter,bundle}.rs`. These resolve the implementation-level
risks the architecture review surfaced.

**Claim state lifecycle — ephemeral per Apply event.**

The claim coordinator (sketch: a `TxClaimCoordinator` struct
holding the claim set) is constructed per `TipEvent::Apply` and
discarded once the Apply finishes processing. Claims are not
persisted, not carried across events, not durable.
Implications:

- `TipEvent::Undo` and `TipEvent::Mark` need no claim handling
  — there's nothing to roll back, the coordinator from the
  prior Apply is already gone
- `handle_event` stays idempotent to re-delivery as today (a
  re-applied tx gets a fresh coordinator with fresh claims)
- Multi-block reorgs work without special handling — each
  Apply event is self-contained for claim purposes

This must be documented explicitly in `INDEXER_TRAIT.md` so
indexer authors don't accidentally rely on cross-Apply claim
state.

**Concurrent claim collection — `DashMap`, not `Mutex<HashSet>`.**

Specific-domain indexers run in parallel within a single Apply
(per the dispatch mechanism above). The claim collection needs
to support concurrent insertion from multiple async tasks.
Use `dashmap::DashMap` rather than `Arc<Mutex<HashSet<...>>>`
to avoid lock contention on busy blocks (open-mint launches,
marketplace volume spikes). Standard tradeoff: marginally more
memory footprint for lock-free throughput. The dependency is
already in the workspace.

**Broadcast channel capacity — bump from 4096 to 16384.**

Today's `BROADCAST_CAPACITY = 4096`
(`mitos-core/src/handle.rs:42`) is sized for a single indexer's
emission rate. With mint-burn-indexer + marketplace +
none_match-indexer all feeding the same per-consumer broadcast
channel, busy blocks risk overflow → `RecvError::Lagged` →
consumer drop + reconnect noise.

Bump to 16384 in v1 as a flat increase (no per-indexer tuning).
If production metrics surface persistent lag, per-indexer
per-channel capacity tuning becomes a post-v1 follow-up.

**Same-wallet suppression helper.**

Lives at `mitos-core/src/helpers.rs::same_wallet(addr_a,
addr_b)` (new file). Uses `pallas-addresses` (already in the
workspace) to:

- Both Shelley addresses with key-stake-credentials → compare
  the stake key bytes
- Both Shelley with script-stake-credentials → compare the
  stake script bytes
- Mixed key/script, or any non-Shelley → fall back to bech32
  string equality

Called by `none_match-indexer` before emitting each
`AssetMovement` event; suppresses emissions where the rule
matches.

## Subscription mechanics impact

`SUBSCRIPTION_MECHANICS.md` updates:

1. **New domain arms.** `DomainSelector` gains `Mint(MintSelector)`,
   `Burn(BurnSelector)`, `AssetMovement(AssetMovementSelector)`.
   Each is single-variant (`Any` only) for v1 — the asset axis
   on `Interest` already handles policy/asset/fingerprint
   targeting for every domain, so a sub-axis filter isn't
   needed up front. Wrapping each in its own enum (rather than
   making them unit variants of `DomainSelector`) keeps the
   shape consistent with `MarketplaceSelector` and reserves
   space for future `Filter { ... }` variants without breaking
   the wire format.

   ```rust
   pub enum MintSelector { Any }
   pub enum BurnSelector { Any }
   pub enum AssetMovementSelector { Any }
   ```

   Plausible v2 expansions (not in this refactor):
   - `MintSelector::Filter { min_amount, recipient_addresses }`
     — large-mint watch or recipient targeting
   - `AssetMovementSelector::Filter { via_script, address_pairs }`
     — gated on the future `via_script` payload field
2. **"Domain::None for state-only indexers" open question
   resolves.** Ownership projections subscribe across multiple
   real domains rather than synthesising a state arm.
3. **Mint-watcher use case becomes idiomatic.**
   `Interest { asset: Any, domain: Mint(Any), value: Any }`.
4. **Per-policy targeting is via the asset axis, not the domain
   axis.** "All mints for policy p" is `Interest { asset:
   Policy(p), domain: Mint(Any), value: Any }`. Multi-policy is
   `Vec<Interest>` per the existing subscription model.

## Migration

Stepwise so no consumer breaks mid-migration. **Steps 1–3 are
v1 (this refactor); steps 4–6 are deferred to later iterations.**

### v1

1. **Add the new types alongside the old.**
   - `mitos-protocol`: add `Mint` / `Burn` / `AssetMovement` arms
     to `Domain`; existing `Marketplace` etc. unchanged.
   - `OwnershipChange` in `cnft.dev-workers` stays.
   - `Indexer::handle_event` returns `Vec<MovementClaim>` (trait
     change; existing impls return empty Vec apart from
     marketplace).
   - No consumer changes required.
2. **Build the new indexers + residual mechanism.**
   - `mint-burn-indexer` ships and emits.
   - `none_match-indexer` ships as the dispatcher's residual
     tail-step. Marketplace indexer adapts to claim its
     movements (Sale, Listing, Unlisting). Unrecognised
     marketplace-shaped txs *still* emit `Marketplace::Sale {
     brand: Unknown }` and claim — the `Unknown`-path flip is
     deferred to step 5 so existing consumers keep working.
3. **First consumer (mint-watcher PoC).** Subscribes to
   `Domain::Mint(Any)` + `AssetSelector::Any`. Validates the
   firehose path and the `mint-burn-indexer` end-to-end. This
   is the v1 acceptance gate.

### Deferred

4. **Migrate `collections-mitos`.** `OwnershipChannel` (currently
   consumes `OwnershipChange::Transfer`) replaces with a multi-
   domain subscription folding mint/burn/movement/marketplace
   into the same `ownership` SQL table. `MarketplaceChannel`
   stays as-is during this step.
5. **Flip marketplace indexer's `Unknown` path.** Unrecognised
   marketplace-shaped txs route to `AssetMovement` instead of
   `Marketplace::*` with brand=Unknown. Consumers that filter
   on `MarketplaceBrand::Unknown` need updating first.
6. **Retire `OwnershipChange` and the
   `collection-ownership-indexer` crate.** Bump
   `mitos-protocol`'s schema version; consumers that haven't
   migrated stop receiving ownership events. Audit before flip.

Each step is reversible until step 6.

## Out of scope

- **Concrete `via_script` attribution on `AssetMovementPayload`.**
  Defer until a consumer needs to distinguish wallet-to-wallet
  from movement-through-unrecognised-script. Adding the field
  later is forward-compatible (Option<...>).
- **DEX and Lending domains.** They already exist; this refactor
  doesn't touch them.
- **Multi-asset bundle attribution.** A Sale of a 5-asset bundle
  emits one `Marketplace::Sale` event with `assets: Vec<...>`;
  the equivalent wallet-to-wallet bundle transfer would emit
  five `AssetMovement` events (one per asset). Asymmetry is fine
  — bundle-as-tx is a marketplace concept, not a chain concept.
- **Consumer SDK helpers.** Folding multiple events into an
  ownership projection is a real piece of code; whether it lives
  as a helper in `mitos-companion` or stays consumer-side is a
  separate decision.
- **Trait-based selection.** `AssetSelector::Trait` is reserved
  on the asset axis (per `SUBSCRIPTION_MECHANICS.md`) and
  composes orthogonally with every domain — the new `Mint` /
  `Burn` / `AssetMovement` arms inherit trait filtering
  automatically when the asset-axis side ships. Not important
  for this refactor; gated on the indexer-side metadata index
  landing. Cross-policy trait selection ("legendary across any
  collection") would need a separate `TraitAnyPolicy` variant or
  a global trait index — design pass when that feature lands.

## Open questions

1. ~~**`MarketplaceBrand::Unknown` final disposition.**~~
   **Resolved:** dropped entirely from `MarketplaceBrand` and
   from `OfferCancelPayload`. Unrecognised marketplace-shaped
   txs route to `Domain::AssetMovement`. Documented in the
   "MarketplaceBrand::Unknown retires" section above.
2. ~~**Listing-as-ownership-change.**~~ **Resolved:** listing
   is a status change, not an ownership change. Seller retains
   ownership while the asset is at the marketplace script.
   Documented in the projection table above. The marketplace
   indexer claims the listing/unlisting asset transfers so the
   fallthrough indexer doesn't double-emit them as
   `AssetMovement` (related to question 4).
3. ~~**`MintSelector` / `BurnSelector` / `AssetMovementSelector`
   shapes.**~~ **Resolved:** v1 ships each as a single-variant
   enum with only `Any`. Per-policy/asset targeting is handled
   by the asset axis on `Interest`. Future `Filter { ... }`
   variants can be added without wire breakage. Documented in
   the "Subscription mechanics impact" section above.
4. ~~**Indexer dispatch ordering.**~~ **Resolved:** indexers
   return explicit `MovementClaim` triplets alongside events;
   the dispatcher's residual pass emits `AssetMovement` for
   unclaimed movements. Lovelace excluded. Overlapping claims
   log+first-wins in production, hard error in dev. Order
   between specific-domain indexers deferred. Documented in the
   "Dispatch mechanism" section above.

## Cross-references

- `SUBSCRIPTION_MECHANICS.md` — selector design, will need
  updating after this lands
- `INDEXER_TRAIT.md` — indexer contract, may need a
  "fallthrough/post-pass" concept
- `cnft.dev-workers/docs/COLLECTION_OWNERSHIP_MITOS_INTEGRATION.md` —
  primary consumer of this work; phase plan there assumes the
  new domain shape lands so the per-policy DO can subscribe via
  multi-domain projection
