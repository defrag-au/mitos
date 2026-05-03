# Data plane query API

A typed query API for *current* UTxO chain state, implemented
in-process at zero serialisation cost, but designed schema-first
so the same surface can ship later as wasm host functions, IPC,
or utxorpc-compatible gRPC for cross-machine consumers.

**Nothing here is built yet.** This is a thought experiment to
capture design direction so it's recoverable when the work is
actually picked up. Sister doc to
`MITOS_ISOLATION_ROADMAP.md` — the two threads converge: this
trait is the foundation that the wasm host-function surface in
the isolation roadmap's Phase C sits on.

Cross-references:
- `MITOS_ISOLATION_ROADMAP.md` — host fn surface for wasm modules
  is one transport for this API
- `SUBSCRIPTION_MECHANICS.md` — `Interest` is the *event filter*
  consumers express; this trait is for *snapshot queries* over
  current state. Different concerns, complementary
- `INDEXER_TRAIT.md` — current Domain access pattern indexers
  use; this API is a higher-level layer on top

## Scope

**In scope:**

- Snapshot queries over current chain state (UTxO set, parameters,
  stake info, etc.)
- Caller-blind datum resolution (inline vs hash-referenced is
  plane-side concern, never surfaced)
- Lightweight, declarative expression of intent: predicates
  composed of address / asset / output-ref patterns
- Field-mask-style decode opt-in: caller specifies which fields
  to decode, plane skips work for the rest
- Multiple transport implementations behind a single Rust trait

**Out of scope:**

- Chain follow / streaming / pub-sub. The existing replication
  pipeline (`Indexer` trait + dispatcher + replicator) handles
  bootstrap + updates. This API is purely "what does the chain
  look like right now"
- Historical / archival queries. Spent UTxOs are gone. If a
  caller needs "what UTxOs existed at slot N", that's a different
  problem (and a much larger storage commitment) — not here
- Transaction submission. Mitos is a read-only consumer of chain
  data; tx submission goes elsewhere
- Cross-chain abstractions. UTxO-shaped, Cardano-flavoured. If
  another UTxO chain ever matters, the schema is portable, but
  designing for that prematurely is wasted

## Goals

- **One schema, many transports.** A typed Rust trait is the
  canonical contract. Multiple impls ship the same surface over
  different mechanisms; consumers pick the impl that matches their
  hosting model.
- **Zero-cost in-process.** When caller and plane are in the same
  process (the common case for indexers), no serialisation happens
  — direct trait calls return typed Rust values.
- **Caller-blind decode.** When a caller asks "what's in this
  output's datum", they get the plutus payload regardless of
  whether it was inline or hash-referenced via witness set. The
  plane resolves transparently.
- **Lightweight expression of intent.** Composable predicate
  algebra (and / or / not) over address / asset / output-ref
  patterns. The caller declares what matches; the plane figures
  out how to find it.
- **Decode-on-demand.** Field-mask request semantics — caller
  asks for `["parsed.address", "parsed.assets"]` and the plane
  skips PlutusData decode for outputs that weren't asked. Heavy
  decode work happens only when needed.

## Prior art (May 2026)

The schema design space has been thoroughly explored. The headline
finding is that **utxorpc's `QueryService` is the canonical shape**
and **Balius already implements the same schema as a wasm WIT
interface** — txpipe has effectively built the "single schema,
multiple transports" pattern, they just haven't unified the IDL.
This API is what we get if we extend that pattern with an
in-process Rust transport and a typed-Address discriminated union.

| Reference | What we borrow | What we reject |
|---|---|---|
| `utxorpc` (txpipe; protobuf + gRPC) | Predicate algebra (`and`/`or`/`not`/`all_of`/`any_of`), `FieldMask` decode opt-in, cursor pagination, `Datum { hash, payload, original_cbor }` struct, no acquire/release semantics | `bytes`-typed addresses (lossy; replace with typed discriminated union) |
| `Balius` (txpipe; WIT host fns) | Schema shape; `read-utxos` / `search-utxos` / `read-params` verbs; CBOR-as-bytes pass-through where the caller wants raw | None notable — Balius's WIT is a sound implementation of the same schema utxorpc proto serves |
| `Kupo` (txpipe-adjacent; HTTP + DSL) | Textual pattern grammar: `<payment>/<delegation>`, `<policyid>.<asset>`, `<txid>@<ix>`, `*` for wildcard | Slot-range pagination — wrong shape for current-state queries |
| `Ogmios` (LocalStateQuery JSON-RPC) | Verb naming (`queryLedgerState/utxo`), dual key shapes per query (UTxO by `addresses` OR `outputReferences`) | Acquire/release state machine — leaks ledger semantics into the API; hide it |
| `pallas-network` LocalStateQuery | Set of useful query verbs as an existence proof of "what queries do indexers actually need" | Era-tagged everything, no predicate composition, raw CBOR throughout — too low-level |
| `Maestro`/`Blockfrost`/`Koios` (REST) | Confirmation that the verb set ("UTxOs by address", "asset holders", "params") is the right surface | Datum resolution as a caller-controlled flag — should be unconditional plane-side |
| `cardano-graphql` | Field selection as a concept (built into GraphQL for free) | Sprawling schema, db-sync coupling, performance reputation — `FieldMask` gives us the same field-selection win without GraphQL's complexity |

The predicate algebra + field mask + cursor pagination + caller-
blind datum struct combination is essentially the consensus design
across the projects that thought about this carefully. We can lift
it almost wholesale.

## The trait

Sketch — names and shapes will be iterated. Purpose here is to
anchor the discussion, not commit.

```rust
pub trait ChainDataPlane: Send + Sync {
    // ===== Point lookups =====

    fn read_utxo(
        &self,
        oref: &OutputRef,
        mask: FieldMask,
    ) -> Result<Option<TypedOutput>>;

    fn read_utxos(
        &self,
        orefs: &[OutputRef],
        mask: FieldMask,
    ) -> Result<Vec<(OutputRef, TypedOutput)>>;

    // ===== Predicate-driven search =====

    fn search_utxos(
        &self,
        predicate: &UtxoPredicate,
        mask: FieldMask,
        page: PageRequest,
    ) -> Result<Page<(OutputRef, TypedOutput)>>;

    // ===== Datum / script resolution side-doors =====
    // For callers that have a hash but don't have the
    // containing UTxO context (rare).

    fn read_datum(&self, hash: &DatumHash) -> Result<Option<PlutusData>>;
    fn read_script(&self, hash: &ScriptHash) -> Result<Option<Script>>;

    // ===== Aggregates / stats (server-side, not row trips) =====

    fn total_supply(
        &self,
        policy: &PolicyId,
        asset_name: &AssetName,
    ) -> Result<u64>;

    fn holder_count(&self, policy: &PolicyId) -> Result<u64>;

    // ===== Chain metadata =====

    fn tip(&self) -> Result<ChainPoint>;
    fn era_at(&self, slot: Slot) -> Result<Era>;
    fn protocol_params(&self) -> Result<ProtocolParameters>;
    fn era_summaries(&self) -> Result<Vec<EraSummary>>;

    // ===== (Optional, Cardano-specific extensions) =====

    fn stake_pool_info(&self, pool: &PoolId) -> Result<Option<PoolInfo>>;
    fn delegation(&self, stake: &StakeAddress) -> Result<Option<DelegationState>>;
    fn drep_state(&self, drep: &DRepId) -> Result<Option<DRepState>>;
}
```

### Key types

```rust
/// Composable predicate over UTxO outputs. Algebra mirrors
/// utxorpc's `UtxoPredicate` directly.
pub enum UtxoPredicate {
    Match(UtxoPattern),
    Not(Box<UtxoPredicate>),
    AnyOf(Vec<UtxoPredicate>),
    AllOf(Vec<UtxoPredicate>),
}

pub struct UtxoPattern {
    pub address: Option<AddressPattern>,
    pub asset: Option<AssetPattern>,
    pub output_ref: Option<OutputRefPattern>,
}

/// Address pattern — discriminated union, NOT raw bytes.
/// Improves on utxorpc's parallel `bytes` fields.
pub enum AddressPattern {
    Exact(Address),
    /// Match by payment credential only — any delegation.
    Payment(PaymentCredential),
    /// Match by stake credential only — any payment.
    Stake(StakeCredential),
    /// Both halves required to match.
    PaymentAndStake(PaymentCredential, StakeCredential),
    /// Wildcard: match every Shelley address.
    AnyShelley,
    /// Wildcard: match every Byron bootstrap address.
    AnyByron,
}

pub enum AssetPattern {
    /// Any asset under this policy.
    Policy(PolicyId),
    /// Specific asset.
    PolicyAndName(PolicyId, AssetName),
    /// Match by CIP-14 fingerprint (server resolves).
    Fingerprint(Fingerprint),
}

pub struct OutputRefPattern {
    pub tx_hash: Option<TxHash>,
    pub index: Option<u32>,
}

/// Decode-opt-in selector. Mirrors utxorpc's `FieldMask` but as
/// typed Rust enum so the compiler enforces field validity.
/// Plane skips decode work for fields the caller didn't ask for.
pub struct FieldMask {
    pub address: bool,
    pub assets: bool,
    pub datum: DatumDecode,
    pub script: ScriptDecode,
    pub raw_cbor: bool,
}

pub enum DatumDecode {
    /// Don't include datum at all.
    Skip,
    /// Include hash + raw CBOR; don't decode PlutusData.
    Reference,
    /// Include everything: hash, decoded PlutusData, raw CBOR.
    /// If hash-referenced, plane resolves via witness-set
    /// transparently before returning.
    Full,
}

/// Server-resolved datum struct. Hash always present (when any
/// datum exists); payload populated whenever decoded; original
/// CBOR available for callers that want to skip the codec hop.
pub struct TypedDatum {
    pub hash: DatumHash,
    pub payload: Option<PlutusData>,
    pub original_cbor: Option<Vec<u8>>,
}

/// What `read_utxo` / `read_utxos` / `search_utxos` return —
/// fields conditional on what `FieldMask` requested.
pub struct TypedOutput {
    pub address: Option<Address>,
    pub assets: Option<Vec<AssetMovement>>,
    pub datum: Option<TypedDatum>,
    pub script: Option<TypedScript>,
    pub raw_cbor: Option<Vec<u8>>,
}

/// Cursor-based pagination. Server returns opaque
/// `next_token`; client passes back unchanged. No
/// limit/offset — order isn't promised stable.
pub struct PageRequest {
    pub max_items: u32,
    pub start_token: Option<String>,
}

pub struct Page<T> {
    pub items: Vec<T>,
    pub next_token: Option<String>,
    /// Tip cursor at the time of query — caller can detect
    /// drift if they paginate across mutations.
    pub tip: ChainPoint,
}
```

### Textual pattern grammar (front-end)

For CLI / config / logs, expose Kupo's grammar as a parse layer
on top of the typed `UtxoPattern` — no reason to reinvent the
canonical Cardano DSL.

| Surface form | Compiles to |
|---|---|
| `*` | `Match(any)` |
| `*/*` | `Match(address: AnyShelley)` |
| `addr1...` | `Match(address: Exact(...))` |
| `<paymentcred>/*` | `Match(address: Payment(...))` |
| `*/<stakecred>` | `Match(address: Stake(...))` |
| `<paymentcred>/<stakecred>` | `Match(address: PaymentAndStake(...))` |
| `<txid>` | `Match(output_ref: OutputRefPattern { tx_hash, .. })` |
| `<txid>@<ix>` | `Match(output_ref: OutputRefPattern { tx_hash, index })` |
| `<policyid>.*` | `Match(asset: Policy(...))` |
| `<policyid>.<assetname>` | `Match(asset: PolicyAndName(...))` |
| `asset1...` | `Match(asset: Fingerprint(...))` |

Compose via mitos-side syntax (`a OR b`, `a AND NOT c`, etc.) or
just construct typed predicates directly in code. The grammar is
ergonomic; the typed enum is the source of truth.

## Transport implementations

Each implements the same trait. Caller depends on the trait, not
the impl. Runtime config picks one.

### `LocalDataPlane` (in-process, zero serde)

Direct calls into `dolos_core::Domain` + mitos's own indexes.
Returns typed Rust values via `Result<T>`. No CBOR encode/decode
on the hot path; plane operates on pre-decoded `MultiEraOutput`s
via pallas-traverse internally.

This is what indexers in-process use. The
`OwnershipIndexer::backfill_for_policy` you see in the codebase
today already does this kind of work ad-hoc — `LocalDataPlane`
formalises it into a trait-driven API.

### `WasmDataPlane` (host functions for wasm modules)

Maps the trait to a WIT interface, mirroring Balius's existing
`ledger` shape. Wasm module stubs call host functions; arguments
move through linear memory; results copy back. Used by the
isolation roadmap's Phase C indexer modules.

```wit
interface chain-data-plane {
  type field-mask = list<u8>;     // bitmap of decode opts
  type cbor = list<u8>;
  type oref = record { tx-hash: list<u8>, index: u32 };
  type page-request = record { max-items: u32, start-token: option<string> };
  type page = record {
    items: list<typed-output>,
    next-token: option<string>,
    tip: chain-point
  };

  read-utxo: func(oref: oref, mask: field-mask)
    -> result<option<typed-output>, plane-error>;
  search-utxos: func(predicate: cbor, mask: field-mask, page: page-request)
    -> result<page, plane-error>;
  read-datum: func(hash: list<u8>)
    -> result<option<plutus-data>, plane-error>;
  // ... rest of the trait, mapped 1:1
}
```

Predicate is passed as CBOR rather than translated through WIT —
the algebra is recursive and WIT has weak support for recursive
types. That's the only place we stop being "structurally typed
all the way down"; manageable.

### `IpcDataPlane` (Unix socket, neighbour processes on the same box)

For separate-process indexers / consumers on the same machine.
Wire format: CBOR over a Unix domain socket. Server is hosted in
mitos; client is a stub crate consumers link. Same trait
implemented by both server and client (via boilerplate — server
delegates to `LocalDataPlane`, client serialises and waits).

Avoids gRPC's protobuf overhead for the local-only case. Roughly
the cost of a redb transaction + a memcpy.

### `GrpcDataPlane` (utxorpc-compatible)

For cross-machine consumers. Translates the trait to utxorpc's
`QueryService` proto. This is the "compat" transport — the schema
shape is close enough to utxorpc that translation is mechanical.
Consumers using utxorpc-compatible clients in any language can
hit mitos this way.

Order of priority: this is the *least* important impl. Worth
having for ecosystem-compat but only when there's a real
cross-machine consumer that needs it.

## Where the schema lives

A new crate `mitos-data-plane` (sibling of `mitos-protocol`).
Holds:

- The `ChainDataPlane` trait
- The typed query / pattern / mask / response types
- `LocalDataPlane` impl (depends on dolos)
- (Future) `WasmDataPlane` host-side helpers + WIT
- (Future) `IpcDataPlane` server + client stub
- (Future) `GrpcDataPlane` impl

Wasm module SDK and IPC client stub may live in further sub-crates
(`mitos-data-plane-client`, `mitos-data-plane-wasm`) to keep the
host-side impl detail out of consumer dep trees.

## Open design questions

1. **Schema-to-IDL: hand-mirror or generate?** Rust trait is
   canonical. WIT and proto are derived shapes. Hand-mirror is
   lower-risk but means two-sided edits when the trait changes.
   Generate (e.g. via a build.rs producing WIT/proto from Rust
   trait metadata) is more elegant but adds tooling complexity.
   Lean: hand-mirror until churn becomes annoying.

2. **Pagination cursor opacity.** `next_token: String` is opaque
   to the caller. What does it actually encode? Probably a
   compact CBOR `(predicate_hash, last_seen_oref)` so the server
   can resume without storing per-cursor state. Needs design —
   utxorpc's spec doesn't dictate, leaves it to the server.

3. **Cardinality of `search_utxos` results.** Some predicates
   (e.g. "all UTxOs at a popular address" or "all UTxOs under
   policy X" for a 12000-asset collection) return many rows.
   Without pagination defaults the API is a foot-gun. Consider:
   max `max_items` cap on the server, paginate-by-default.

4. **Snapshot consistency across calls.** A caller doing
   `search_utxos(predicate)` followed by `read_datum(hash)`
   spans two redb read txns. If the chain advanced between, the
   datum may not exist. Worth defining: each call is its own
   snapshot; tip is included in every response so callers can
   detect drift; for true cross-call consistency, expose a
   transaction-handle API later (`open_snapshot() -> Snapshot`,
   methods on `Snapshot`). Defer until needed.

5. **Stake / governance / pool queries — first-class or
   extension?** They're Cardano-specific. The trait could
   declare them at the top level (commits to Cardano) or move
   them to a `CardanoExtensions` sub-trait that
   `ChainDataPlane` implementations can opt into. Probably
   sub-trait — keeps the core surface portable and opt-in for
   the chain-specific bits.

6. **Reactive vs polled.** Strictly polled in scope; if a caller
   wants "tell me when a UTxO at this address appears", that's
   the `Indexer` + `Interest` pipeline, not this. But the `tip`
   field on every response means callers can poll and detect
   change cheaply. Worth documenting the distinction explicitly.

7. **Error model.** `PlaneError` enum or `anyhow::Error`?
   Probably typed enum — different transports surface different
   failure classes (network for IPC, sandbox-trap for wasm,
   deadline-exceeded for gRPC). Caller handles each shape.

8. **What about CIP-25 metadata?** Caller often wants "the NFT's
   metadata JSON" alongside the UTxO. Currently spread across
   the asset's mint TX metadata, the CIP-68 reference NFT's
   datum, etc. A `read_metadata(asset_id)` query would be
   genuinely useful but means the plane needs metadata indexes
   we don't have today. Defer; out of MVP scope. Worth flagging
   as the obvious phase-2 extension.

## Migration path

Like the isolation roadmap, this isn't a single PR. Three phases.

### Phase A — Trait + `LocalDataPlane`
*(Trigger: when wasm host-fn API design starts in earnest, OR
when a second indexer hits the same `dolos_core::Domain` access
patterns and we want to factor them out)*

- Sketch + iterate the `ChainDataPlane` trait shape against a
  small set of worked examples (ownership indexer queries,
  marketplace input resolution, hypothetical alert-evaluator
  queries).
- Implement `LocalDataPlane` — wraps `dolos_core::Domain` plus
  any mitos-side indexes the trait needs (e.g. holder counting
  via existing watch-state or new index).
- Port `OwnershipIndexer` to use `LocalDataPlane` instead of
  raw `Domain` access. See where the abstraction creaks.
- Port `MarketplaceIndexer`'s input resolution. This is the
  real test — the plane needs to handle "give me resolved
  inputs for a TX" cleanly without exposing dolos's
  state-applied-before-dispatch quirk.

If both indexers feel natural against the trait, the abstraction
is sound. If they feel forced, iterate the trait before going
further.

### Phase B — `WasmDataPlane`
*(Trigger: isolation roadmap's Phase C activates)*

- Define WIT mirroring the Rust trait. Hand-mirror; document the
  drift expectations.
- Wasm module SDK — small Rust crate that wasm modules link;
  exposes a `ChainDataPlane` impl that compiles to WIT-stub
  calls. Module authors code against the same trait as
  in-process indexers.
- Host-side: implement the WIT host fns by delegating to
  `LocalDataPlane`. Transparent translation.
- Integration with the isolation roadmap's `Watch` /
  `DecodeRequest` pre-decode kit — host pre-fills inputs
  according to module's declared decode needs, so most modules
  rarely call the data plane at all on the hot path.

### Phase C — `IpcDataPlane` / `GrpcDataPlane`
*(Trigger: when there's a second-process consumer on the same
box, or a cross-machine client respectively)*

- Lower priority. Implement when there's a concrete consumer
  asking for it. Not interesting until then — `LocalDataPlane` +
  `WasmDataPlane` covers all in-process and sandboxed-module
  callers.

## Trigger conditions

Don't start this work until at least one of:

1. **Wasm host-fn design starts** (isolation roadmap Phase C).
   This trait is the foundation of that design; co-developing
   them makes sense.
2. **A third indexer arrives** that needs the same kind of state
   queries. Refactoring two ad-hoc data accesses into a shared
   trait is over-engineering; refactoring three is the natural
   move.
3. **A consumer needs UTxO state queries that aren't event-shaped.**
   The current `Indexer` / `Interest` model handles "tell me when
   X happens"; this trait would handle "what does the chain look
   like for X right now". The first time someone asks "what's
   the current floor price across all jpg.store listings for
   this policy", that's a snapshot query, not an event filter,
   and we'd want this trait.

If none of these are true, the existing `dolos_core::Domain`
access pattern + indexer-internal state is fine. Don't build
abstractions ahead of demand.

## Known gaps the trait doesn't yet address

### Marketplace input resolution — needs a separate primitive

The MarketplaceIndexer's `classify_tx` needs to resolve a tx's
*consumed inputs* into typed `MultiEraOutput` form so the
classifier can build a `RawTxData` for rule evaluation. This
case **cannot be served by `ChainDataPlane` as currently
specified** because:

- The plane is a snapshot-of-current-state API. By the time
  `Indexer::handle_event(Apply, block)` fires, dolos has
  already applied the block to state — consumed UTxOs are
  gone from the snapshot.
- `state.get_utxos(refs)` returns `None`/empty for spent
  refs at this point; the plane's `read_utxos()` wrapping
  the same call has the same failure mode.
- This is empirically observable: the marketplace indexer
  is currently silent in production despite chain activity,
  because every tx's input resolution fails this way.

The right shape for a fix is a **block-context resolution
primitive**, not a data plane query:

```rust
/// Resolve the consumed inputs of a single tx within the
/// current block being applied. Implementation detail varies
/// by host: dolos's archive may retain spent UTxOs for a
/// short window; otherwise the host has to capture inputs
/// pre-apply via a different sync hook.
trait BlockContext {
    fn resolve_consumed_inputs(
        &self,
        tx: &MultiEraTx<'_>,
    ) -> Result<HashMap<OutputRef, MultiEraOutput<'_>>, _>;
}
```

This is a different contract from `ChainDataPlane`:
- Tied to a specific block / tx context, not the open chain
- Returns pallas-shaped values (the classifier consumes those
  directly), not the data plane's projected `TypedOutput`
- Available only inside `handle_event` hooks, not from
  arbitrary callers

Until this primitive exists:
- MarketplaceIndexer keeps its current `domain.state().get_utxos()`
  call shape (silently fails for spent inputs — known)
- Refactoring it to `LocalDataPlane.read_utxos()` is cosmetic,
  doesn't change behaviour, not a meaningful win
- The data plane proceeds with snapshot-only queries; the
  block-context primitive lives in `mitos-core` (closer to
  dolos's chainsync) once designed

### Datum / script witness-set resolution

Already noted under "Phase A scope explicitly deferred" but
worth flagging here too. `DecodeLevel::WithDatum` and `Full`
are honoured at the API surface (caller can request them, the
`decoded_at` field reflects the request) but the
`LocalDataPlane` impl always returns `datum: None` /
`script_ref: None` because the witness-set lookup primitive
isn't built. Affects any consumer that needs decoded datum
content on outputs — not used by Phase A's ownership backfill,
will be needed for tx-template construction in the framework
context.

## Phase A implementation notes

Implementation landed 2026-05-03 (see `crates/mitos-data-plane/`).
Decisions made during the build, captured for posterity:

**Object-safety vs generics — chose generics.** Trait is
`async_trait` macro with `Send + Sync` bounds, designed for
generic consumer use (`<DP: ChainDataPlane>`). Object-safe via
the macro's BoxFuture trick if a future caller needs runtime
dispatch, but the default consumer pattern is generic + zero-cost
dispatch. Aligns with the existing `Indexer<D: Domain>` pattern.

**Lifetime-parameterised `LocalDataPlane`.** Construct with `&D`,
not `Arc<D>`. Plane is short-lived — typically built ad-hoc inside
an indexer's `subscribe` callback that already has `&domain`.
Saves the framework from threading `Arc<D>` through the indexer
trait surface. Future longer-lived plane usage (e.g. plane
embedded in a long-running consumer) can hold `&'static D` or
wrap an `Arc` and dereference.

**Address as bech32 string, not pallas `Address`.** `pallas_addresses::Address`
doesn't implement Serialize/Deserialize and the custom-serde
plumbing was non-trivial. Storing addresses as bech32 `String`
in `TypedOutput.address` and `AddressPattern::Exact(String)`
matches the convention `cardano_assets::PolicyId` already uses,
serialises cleanly, and keeps the public API lighter. Callers
needing the typed `pallas_addresses::Address` can `Address::from_bech32(&output.address)`
cheaply at the call site.

**`DecodeLevel` simpler than `FieldMask` was the right call.**
Three named tiers (`Lean` / `WithDatum` / `Full`) cover the
meaningful axes server-side resolution can vary on. utxorpc's
`FieldMask` is a protobuf-shaped 5-axis Option-stuffing surface
that doesn't earn its keep when the hot path is in-process Rust
(zero serialisation savings) — captured in the doc + earlier
discussion. `TypedOutput.decoded_at` carries the level the plane
actually performed for transparency.

**`Option<T>` on `TypedOutput` means "absent on chain", never
"caller didn't ask".** The latter is encoded in `DecodeLevel`,
not in field optionality. Separation kept the response struct's
type contract honest.

**Predicate algebra is sound; the matcher isn't.** `UtxoPredicate`
+ `UtxoPattern` + `AddressPattern` + `AssetPattern` + `OutputRefPattern`
all defined with the full algebra (and / or / not / nested
composition). `LocalDataPlane.search_utxos` only handles the
`Match(UtxoPattern { asset: Some(Policy(_)), ... })` case in Phase A
because that's what the OwnershipIndexer backfill spike needs.
Other predicate shapes return `DataPlaneError::NotYetImplemented`.
The grammar is committed; the index-driven query planning is
incremental work — each new predicate shape gets its own
plumbing pass.

**The `dolos_cardano::indexes::CardanoIndexExt` blanket impl
worked transparently.** `LocalDataPlane` is generic over `D: Domain`
and calls `domain.indexes().utxos_by_policy(...)` without an
explicit `where <D as Domain>::Indexes: CardanoIndexExt` bound;
the blanket impl on dolos's index types makes the call resolve.
Worth knowing for future Phase B+ indexes (e.g. `utxos_by_address`).

**Spike: `backfill_for_policy_via_data_plane`.** A side-by-side
reference impl in `collection-ownership-indexer` that does the
same job as the existing `backfill_for_policy` via `LocalDataPlane`.
Currently `#[allow(dead_code)]` — the live `subscribe` path still
calls the original. Kept around for review + eventual replacement
once the trait shape is validated against more consumers.
Concretely shorter than the original (no manual era / output
decode, no manual asset-multiset filter to the policy — the plane
handles it via the predicate-driven search).

**Phase A scope explicitly deferred:**
- `read_datum` / `read_script` (need hash-keyed indexes mitos
  doesn't currently maintain)
- `total_supply` (needs per-policy mint aggregation index)
- `holder_count` is implemented but slowly (enumerate UTxOs +
  count distinct addresses); production-scale needs a real
  aggregation index
- `protocol_params` (era-state walking)
- Pagination cursor encoding (Phase A uses single-page no-token
  semantics; works fine for ~10K-output collections, fails for
  millions)
- Predicate matcher beyond `Match(UtxoPattern { asset: Policy(_) })`
- Witness-set datum resolution at `DecodeLevel::WithDatum` (the
  level is honoured by `decoded_at`, but the actual resolution
  isn't built — `datum: None` always)

## Lessons banked

These come from the research pass and our own implementation
experience; informed the design above.

- **Single schema, multiple transports — the design pattern
  exists, the unified IDL doesn't.** utxorpc + Balius span gRPC
  and wasm with the same conceptual schema, hand-mirrored. Our
  Rust-trait-as-source approach is consistent with that pattern;
  generating WIT/proto from a single Rust source would be a
  small but real ecosystem contribution.
- **Caller-blind datum resolution is the hard part to get
  right.** Most APIs (Maestro, Blockfrost, Koios, Kupo) leak the
  inline-vs-hash distinction to the caller via flags or
  side-doors. utxorpc's `Datum { hash, payload, original_cbor }`
  fused struct is the cleanest; resolution happens server-side
  unconditionally.
- **Predicate algebra > query-per-key endpoint set.** A handful
  of compact predicates (`AND`, `OR`, `NOT`, primitive
  patterns) covers the surface that LocalStateQuery's ~35
  query verbs need. Easier to extend, easier to compose.
- **Field masks > separate decode requests.** I'd previously
  sketched a `DecodeRequest` separate from queries. utxorpc's
  approach of folding decode opt-in into the request via
  `FieldMask` is more ergonomic and matches "lightweight
  declarative expression of need".
- **No acquire/release in the API surface.** Cardano's
  `LocalStateQuery` mini-protocol exposes acquire/release
  semantics; that's a leaky abstraction at the API layer.
  Hide it; each call is a fresh snapshot read; surface tip
  in the response envelope so callers can detect drift if
  they care.
