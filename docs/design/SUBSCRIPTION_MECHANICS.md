# Subscription mechanics

How a CF-side consumer expresses *what events it wants* and how
mitos matches that expression against the events it produces.

This doc is the type-system design for the subscription surface. It
does **not** cover the wire protocol (see `CF_REPLICATION.md`),
indexer trait contracts (see `INDEXER_TRAIT.md`), or per-indexer
classification logic (see `MARKETPLACE_INDEXER.md`). It covers only
the shapes that flow between the two sides at subscribe time and
the matching algorithm that runs per event.

Cross-references:
- `INDEXER_TRAIT.md` — `Indexer::Scope` is being generalised here
- `MARKETPLACE_INDEXER.md` — first consumer of this model; current
  `Scope = ()` becomes a richer `Interest` type
- `CF_REPLICATION.md` — wire protocol carrying `Subscribe` /
  `Unsubscribe` / events
- `ROADMAP.md` step 11 — adoption migration from per-indexer scope
  to unified `Interest`

## Goals

A consumer worker — `collection-ownership-mitos`, a future DEX
worker, an alert-rule evaluator — needs to say:

- *which assets* it cares about (one policy, one fingerprint,
  multiple policies, everything)
- *which protocols / brands / events* it cares about (all
  marketplace events, only JpgStore + Wayup sales, every DEX swap)
- *(future)* threshold filters on transaction value

Each consumer expresses its interest **once**, at subscribe time, in
a single typed structure that survives serialisation to the wire and
back. Mitos uses the same structure server-side to filter the
emission stream. There is no in-band string DSL, no per-event regex,
and no JSON pattern matcher. Selectors are Rust enums all the way
down.

A consumer that wants more than one disjoint interest sends a
`Vec<Interest>`; the subscription matches if any one entry matches.
This keeps each `Interest` a single descent path and makes the OR
explicit.

## The three dimensions

```rust
pub struct Interest {
    pub asset:  AssetSelector,
    pub domain: DomainSelector,
    pub value:  ValueFilter,
}
```

Three orthogonal axes, ANDed at match time. Each axis has a
canonical "no constraint" form (`Any`) so broad interests collapse
to short literals.

### Asset

```rust
pub enum AssetSelector {
    Any,
    Policy(PolicyId),
    Asset { policy: PolicyId, name_hex: String },
    Fingerprint(Fingerprint),
    Trait { policy: PolicyId, key: String, value: TraitValue }, // future
}
```

`Trait` is reserved — declared in the type but matched as `false`
(or returned as an error) until a metadata-fingerprint index exists
on the indexer side. Keeping it in the enum now means consumers can
encode forward-looking subscriptions and the wire shape stays
stable when the feature lands.

A single emitted event references exactly one `AssetId`. (TXs that
touch N assets across N policies emit N records — one per
`(asset, domain_event)` pair.) Matching is therefore equality on
the asset identity at whichever specificity the selector chose:
`Policy` matches by `policy_id`, `Fingerprint` matches the CIP-14
hash, `Asset` matches both fields exactly.

### Domain (protocol → brand → event-kind)

This is the dimension where the asymmetry between events and
selectors matters. Events nest fully (Domain → Brand → BrandEvent
with brand-specific payloads); selectors flatten at the brand level
to orthogonal `(brands, kinds)` axes within each domain.

#### Event payloads (what mitos emits)

The outer dimension within a domain is **kind**, not brand. Brand
is carried inside each variant — as a struct field for kinds whose
shape is shared across brands, or as an inner sum-type variant for
kinds where brand semantics genuinely differ. This collapses the
common case (most marketplaces share most event shapes) and reserves
the typed-divergence machinery for the cases that need it.

```rust
pub enum Domain {
    Marketplace(Marketplace),
    Dex(Dex),
    Lending(Lending),
}

#[derive(strum::EnumDiscriminants)]
#[strum_discriminants(name(MarketplaceEventKind))]
#[strum_discriminants(derive(enumset::EnumSetType))]
pub enum Marketplace {
    Sale(SalePayload),
    ListingCreate(ListingPayload),
    ListingUpdate(ListingPayload),
    Unlisting(UnlistingPayload),
    OfferCreate(OfferCreatePayload),
    OfferAccept(OfferAcceptPayload),
    OfferUpdate(OfferUpdatePayload),
    OfferCancel(OfferCancelPayload),
}

// Shared shape: brand is data, one variant covers every marketplace.
pub struct SalePayload {
    pub brand: MarketplaceBrand,
    pub asset: PricedAsset,
    pub seller: Address,
    pub buyer: Address,
    pub royalty: Option<Lovelace>,
}

// Divergent shape: brand is a variant because the redeemer/script
// data differs per marketplace. The selector engine still sees one
// kind (OfferCancel) and one brand (extracted via brand()).
pub enum OfferCancelPayload {
    JpgStore { policy_id: PolicyId, redeemer: PlutusData, script_ref: TxoRef },
    Wayup    { policy_id: PolicyId, redeemer: PlutusData },
    Unknown  { brand_script: String, raw: PlutusData },
}
```

The `EnumDiscriminants` derive auto-generates `MarketplaceEventKind`
directly from `Marketplace`'s variants. There is no separate
hand-written kind enum to keep in sync — the kind taxonomy *is* the
outer enum's variant set.

`brand()` is a single method on `Marketplace` that dispatches by
variant: a struct-field read for shared-shape variants, an inner
match for divergent-shape variants. Inlinable, allocation-free.

```rust
impl Marketplace {
    pub fn brand(&self) -> MarketplaceBrand {
        match self {
            Marketplace::Sale(p)          => p.brand,
            Marketplace::ListingCreate(p) => p.brand,
            Marketplace::ListingUpdate(p) => p.brand,
            Marketplace::Unlisting(p)     => p.brand,
            Marketplace::OfferCreate(p)   => p.brand,
            Marketplace::OfferAccept(p)   => p.brand,
            Marketplace::OfferUpdate(p)   => p.brand,
            Marketplace::OfferCancel(c)   => match c {
                OfferCancelPayload::JpgStore { .. }     => MarketplaceBrand::JpgStore,
                OfferCancelPayload::Wayup { .. }        => MarketplaceBrand::Wayup,
                OfferCancelPayload::Unknown { .. }      => MarketplaceBrand::Unknown,
            },
        }
    }
}
```

**Adding a brand whose Sale shape matches the existing
`SalePayload`** requires only:

1. Extend `MarketplaceBrand` with the new variant
2. Update the classifier to populate `brand: NewBrand` for that script
3. Done — no new event variant, no new consumer match arm

**Adding a kind** is one new variant on `Marketplace`, one new
payload struct (or enum, if shapes diverge), and `EnumDiscriminants`
regenerates `MarketplaceEventKind` automatically.

The `Unknown` placement reflects the asymmetry: shared-shape
variants carry it as `MarketplaceBrand::Unknown` in the brand field;
divergent-shape variants need a dedicated `Unknown { brand_script,
raw }` arm because there's no shared payload to fall back on. Either
way, unrecognised contracts still emit a usable event — lossless.

#### Selectors (what consumers send)

```rust
pub enum DomainSelector {
    Any,                              // every protocol event
    Marketplace(MarketplaceSelector),
    Dex(DexSelector),
    Lending(LendingSelector),
}

pub enum MarketplaceSelector {
    Any,                              // every marketplace event
    Filter {
        brands: BrandSet,             // EnumSet<MarketplaceBrand>
        kinds:  KindSet,              // EnumSet<MarketplaceEventKind>
    },
}

pub type BrandSet = enumset::EnumSet<MarketplaceBrand>;
pub type KindSet  = enumset::EnumSet<MarketplaceEventKind>;
```

`enumset::EnumSet<T>` is the workhorse: const construction
(`enum_set!(Sale | OfferCancel)`), efficient set ops, auto-widening
to `[u64; N]` when variant counts grow past 64. `BrandSet::all()` is
"any brand"; `KindSet::all()` is "any kind"; both are the natural
broad-interest forms.

The selector hierarchy flattens at the brand level because the
common consumer query is "discrete event-kinds across multiple
brands" — exactly the case that became verbose under fully-nested
selectors. `Filter { brands, kinds }` turns "Sale + OfferCancel
across JpgStore + Wayup" into one record.

`MarketplaceSelector::Any` is sugar for
`Filter { brands: EnumSet::all(), kinds: EnumSet::all() }`. Kept as
a separate variant for cheap construction and clear wire encoding.

The same shape repeats per domain:

```rust
pub enum DexSelector {
    Any,
    Filter { brands: EnumSet<DexBrand>, kinds: EnumSet<DexEventKind> },
}
pub enum DexEventKind { Swap, AddLiquidity, RemoveLiquidity, PoolCreate }
pub enum DexBrand     { Splash, Cswap, Minswap, MinswapV2, Sundae, Unknown(String) }
```

(The `Unknown(String)` variant on `DexBrand` is for the **event**
payload's tagged-union; selectors use a non-string discriminant set
that excludes `Unknown`. A consumer who wants events from
unrecognised brands subscribes via `MarketplaceSelector::Any` and
filters at the consumer side.)

### Value

```rust
pub enum ValueFilter {
    Any,
    // Future: Min { lovelace: u64 }, Range { min, max }, etc.
}
```

Placeholder. CF-side post-receive filtering is sufficient until a
concrete query motivates pushing this server-side. The type exists
so the wire shape doesn't change when the feature lands.

## Matching algorithm

Per emitted `ProtocolEvent`, against an `Interest`:

```
matches(interest, event) =
    asset_matches(interest.asset, event.asset) &&
    domain_matches(interest.domain, event.domain) &&
    value_matches(interest.value, event.value)
```

`asset_matches`: equality at the selector's specificity level (Any,
policy, policy+name, fingerprint).

`domain_matches`: descend by Domain arm.
- `DomainSelector::Any` → match.
- `DomainSelector::Marketplace(sel)` against
  `Domain::Marketplace(ev)`:
  - `MarketplaceSelector::Any` → match.
  - `Filter { brands, kinds }` →
    `brands.contains(ev.brand()) && kinds.contains(ev.kind())`.
- Domain mismatch (e.g. selector picks Marketplace, event is Dex) →
  no match.

`value_matches`: today, `ValueFilter::Any` always matches.

A subscription is `Vec<Interest>`; an event matches the
subscription if it matches any single `Interest`. Matching is
short-circuit OR over the vec.

## Selector overlap and precedence

A subscription with multiple `Interest`s may have overlapping
matches (event matched by more than one entry). The default
behaviour is **deduplicate by event identity** —
`(tx_hash, asset, domain_event_kind)` — and emit once. Consumers
never see duplicates due to selector overlap.

For future asymmetric interests ("everything in Marketplace **except
JpgStore Sale**"), the precedence rule borrowed from
`tracing-subscriber`'s `Directive::Ord` is
**longest-prefix-wins**: more specific selectors override less
specific ones. Specificity is ordered by selector depth
(Domain::Any < Marketplace::Any < Marketplace::Filter < ...).
Polarity (include vs. exclude) is a future extension; today, all
selectors are inclusive and overlap simply unions.

The `tracing` precedent is referenced verbatim because it's been
production-tested at scale; rolling our own ordering is a net loss.

## Wire format

`Interest` and its sub-enums derive `Serialize` + `Deserialize`.
Subscriptions ride the existing `CF_REPLICATION` WebSocket protocol:

```
{ "type": "subscribe",
  "indexer": "marketplace",
  "interests": [ <Interest>, <Interest>, ... ] }
```

Concrete shape: enum variants serialise as externally-tagged JSON
(serde default), `EnumSet<T>` serialises as a JSON array of variant
names (its `serde` feature), opaque newtypes (`PolicyId`,
`Fingerprint`) serialise as their canonical strings (hex / bech32).
A consumer that only reads ownership data subscribes once with one
`Interest`; an alert-rule evaluator may send dozens.

The wire format is stable once shipped. Adding new variants to
`MarketplaceEventKind`, new `Domain` arms, or new `Brand`s is
forward-compatible (consumers using `Any` keep working; consumers
on old `Filter`s simply don't match the new variants).

Removing variants is breaking — versioned via the indexer's
`schema_version` field on subscribe (returned in the ack).

## Indexer trait change

Today's `Indexer<D>` has:

```rust
trait Indexer<D: Domain> {
    type Scope: ...;
    type Change: ...;
    fn change_matches_scope(&self, change: &Self::Change, scope: &Self::Scope) -> bool;
    // ...
}
```

The migration: `Scope` becomes `Interest` for indexers that opt in.
Per-indexer scope (e.g., ownership's `PolicyId`) is replaced by the
shared `Interest` type, with the indexer matching on whichever
dimensions it produces. Ownership matches on `asset` only (its
`Domain` is `None` semantically — events are not protocol events,
they're state changes). A future state-vs-event split in the trait
is plausible but deferred; for now, ownership populates a
synthesised `Domain::State` arm or simply ignores the `domain` axis
of the `Interest` (every event matches `Domain::Any`).

The marketplace indexer flips `Scope = ()` → `Scope = Interest` and
implements `change_matches_scope` via the matching algorithm above.

## What this enables

- **`collection-ownership-mitos`** subscribes to: `Interest { asset:
  Policy(p), domain: Any, value: Any }` per policy it tracks.
  Receives ownership state changes (today's behaviour) plus, on
  opt-in, marketplace events for that policy.
- **A future DEX worker** subscribes to: `Interest { asset: Any,
  domain: Dex(Filter { brands: all, kinds: only(Swap) }), value:
  Any }`. Receives every swap from every DEX brand without
  per-pool registration.
- **An offer-cancel automation** subscribes to: `Interest { asset:
  Policy(p), domain: Marketplace(Filter { brands: EnumSet::all(),
  kinds: enum_set!(OfferCancel) }), value: Any }`. Receives
  cross-brand cancellation events; on the consumer side, one outer
  match arm (`Marketplace::OfferCancel(c)`) catches all brands, with
  a nested match on `OfferCancelPayload` for brand-specific
  redeemer/script-ref handling.
- **An alert-rule evaluator** subscribes to many `Interest`s,
  generated from user-authored rules. A future stage compiles
  CEL-style rule expressions down to a `Vec<Interest>` plus
  consumer-side post-filter; the subscription model itself stays
  typed.

## Crate dependencies

- `strum` (0.28+) with `derive` feature — `EnumDiscriminants`
  derives `MarketplaceEventKind` directly from `Marketplace`'s
  variants
- `enumset` (1.1+) with `serde` feature — `BrandSet` / `KindSet`
- `serde` — wire serialisation
- (Future) `subenum` (1.2+) — kept on the radar for cross-domain
  variant subsets (e.g., a "cancel event from marketplace OR
  lending" projection). Inside one domain it's redundant under the
  kind-as-outer layout — `Marketplace::OfferCancel(_)` is already
  the cross-brand projection.
- (Future) `cel-interpreter` — only when user-authored rules become
  a product surface; not a current dependency

## Out of scope

- **Push-down filtering optimisation.** Today the indexer emits
  every event and the framework filters per-subscription
  post-emission. Push-down (compiling many subscriptions into a
  fused server-side filter) is a future optimisation when CPU
  shows up in profiling.
- **Trait-based asset selection.** Reserved as
  `AssetSelector::Trait` but inert until a metadata index exists.
- **Stateful filters** (rate limits, dedupe windows beyond
  per-event identity, sampling). All consumer-side concerns.
- **Multi-indexer subscriptions.** A subscription is per-indexer
  today (`Subscribe { indexer: "marketplace", interests: [...] }`).
  A unified-indexer subscription is plausible but introduces
  cross-indexer schema versioning concerns; defer.

## Open questions

- **Domain::None for state-only indexers.** Ownership produces
  state changes, not protocol events; how it expresses its
  `Domain` axis (synthetic arm, separate trait, or just ignored)
  is unresolved. Cleanest answer is probably a separate
  `StateIndexer` trait whose subscription type is `AssetSelector`
  alone — but that doubles the trait surface. Park until a second
  state indexer exists.
- **`MarketplaceBrand::Unknown` in selectors.** Under kind-as-outer,
  `MarketplaceBrand` is a flat C-style enum (no string tail —
  divergent-shape kinds carry the `brand_script` string inside
  their `Unknown` payload variant instead). `MarketplaceBrand::Unknown`
  is therefore a normal `EnumSet` member and can be selected on
  directly. Long term: catalogue brands aggressively so the
  `Unknown` carrier shrinks toward empty.
- **Migration timing.** Existing `OwnershipIndexer` uses
  `Scope = PolicyId`. Migrating to `Interest` is non-trivial (wire
  format change, redb registry schema bump). Defer until the
  marketplace indexer has a real consumer; that's the forcing
  function.
