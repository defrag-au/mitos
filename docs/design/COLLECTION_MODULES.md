# Collection Modules — Holders & Metadata

> **Status: Phase 1 shipped (2026-05-20). Phases 2–5 pending.** Proposes two peer community modules — `collection-holders` and `collection-metadata` — that together replace Maestro as the bootstrap data source for `cnft.dev-workers`'s `collection-ownership` worker and provide forward-looking primitives for a future trading-card-game (TCG) consumer. Companion to [`COLLECTION_OWNERSHIP_MITOS_INTEGRATION.md`](../../../cnft.dev-workers/docs/design/COLLECTION_OWNERSHIP_MITOS_INTEGRATION.md), which scopes the worker-side migration; this doc scopes the module-side primitives. **Target dispatch model: platform v2** ([`MITOS_PLATFORM_V2.md`](../strategy/MITOS_PLATFORM_V2.md)) — both modules consume interest-filtered `produced` / `consumed` events per TX, not block-iteration. Bootstrap, backfill, and tip dispatch flow through the same path on v2.
>
> **Phase 1 shipped at `community-modules/collection-holders/`** (event types in `mitos-community-events::collection_holders`), with four golden fixtures under `tests/fixtures/`: `cip68-pair-mint-mekka`, `cold-start-mixed-holders`, `cip25-single-mint-perp`, `cip68-datum-update-noop`. Mitos-run gained a `by_policy` index in the fixture data plane to support cold-start tests for holder-shaped modules. **Outstanding Phase 1 follow-ups:** `HolderRef::Script.label` lookup via `address-registry` (deferred per Resolved Decision #3); FT-classified policy hard-reject; 48-hour live acceptance test against a deployed mitos host.

## Goal

Make mitos the authoritative source for *current holders* and *current metadata* of any policy whose tokens behave as **discrete collectible identities** — i.e. each `asset_name_hex` represents a distinct thing (an artwork, a card, a slot, a membership), and quantity-per-`asset_name_hex` is small and meaningful.

Two collection-shaped primitives:

| Module | Subscribe → emits |
|---|---|
| `collection-holders` | `Snapshot{Begin,Chunk,End}` (all `(asset_name, holder, qty)` tuples at a cursor) + `CollectionDelta` events on asset movement |
| `collection-metadata` | `MetadataSnapshot` (all `(asset_name, metadata)` pairs at a cursor) + `MetadataUpdate` events for CIP-68 datum rotations |

A consumer subscribing to both for the same `companion_key` gets the entire "what does this collection currently look like, and how does it change" picture in two parallel typed event streams. CIP-25 and CIP-68 are presented uniformly — see [The CIP-68 Facade](#the-cip-68-facade-for-cip-25).

This retires Maestro pagination as the bootstrap path for `collection-ownership`, including for assets held in marketplace script addresses — which Maestro's `/policy/{id}/accounts` endpoint silently omits because it groups by stake credential. The Dolos `BY_POLICY` index is UTxO-set-complete.

## Why two modules instead of one

The temptation to fold both into a single `collection` module is real — one subscription, one stream, simpler worker wiring. We resist it because:

1. **Different chain primitives.** Holder data derives from current UTxO set (`utxos_by_policy`). Metadata derives from TX auxiliary data (CIP-25) or ref-token datums (CIP-68). The host-fn surface and update cadence are different enough that combining them produces a discriminated union at every layer.
2. **Different scaling profiles.** Holder snapshot grows with `(supply × avg_holders_per_asset)`. Metadata snapshot grows with `asset_name_count` only — once per distinct identity, regardless of how many copies exist. For RFTs especially, these scale very differently.
3. **Different rollout risk.** Metadata projection is more involved (CIP-25 ↔ CIP-68 normalisation, historical bootstrap via Maestro fallback). Holders is simpler. Shipping in two phases is safer than one combined module that has to land both.
4. **Composition at the subscription layer is already supported.** `SubscribeRequest.targets` is `Vec<SubscribeTarget>` — the consumer subscribes to both modules in one HTTP call for the same `companion_key`. The "flat interest expression" feel is preserved without forcing the modules together.

Why not reuse `holder-distribution`? That module is CNT-shaped (`holder → total qty` projection, holder-count-dominant snapshot, gini/concentration semantics, DEX/vesting awareness). Collection-shaped tokens want `(asset_name, holder, qty)` projection, supply-dominant snapshot, marketplace-aware script-address surfacing. The signatures are different enough that one module serving both ends up worse at each. See [Comparison to `holder-distribution`](#comparison-to-holder-distribution).

## Scope

**In scope:**
- Policies where every `asset_name_hex` is a distinct collectible identity
- NFTs (qty=1 per asset_name) and RFTs (qty>1 per asset_name, bounded conceptually by edition design, may grow over time as new copies mint)
- CIP-25 (metadata in mint TX) and CIP-68 (metadata in ref-token datum) — uniformly
- Snapshot-at-cursor + incremental delta stream, with consistent semantics across the two

**Out of scope:**
- CNT distribution analysis — stays on [`holder-distribution`](./HOLDER_DISTRIBUTION_MODULE.md)
- DEX pool LP positions — stays on per-brand DEX modules
- Vesting / lockup tracking — separate concern
- Marketplace listing attribution back to seller — consumer-side (e.g. `jpg-store-mirror`) re-attributes via marketplace knowledge; the module surfaces `Script(addr, label)` as raw data
- Burn/mint event channels — already covered by `cip-25-mint`, `cip-68-mint`, `standard-burn`, `burn-address`. `collection-holders` deltas reflect the *quantity outcome* of mints/burns (production from null, consumption to null) but doesn't try to be the canonical mint/burn event surface
- Historical analytics (top-N over time, holder churn) — consumer-side projections

## Architecture

```
              ┌──────────────────────────────────────────────────┐
              │  collection-holders wasm module                  │
              │                                                  │
              │  Interest (dynamic): holds_policy(X)             │
              │  State (kv):  holder_ledger:<policy_hex>         │
              │                                                  │
              │  Inputs:                                         │
              │    chain_data::utxos_by_policy(X)  ─ cold-start  │
              │    Produced/Consumed deltas        ─ live tail   │
              │                                                  │
              │  Outputs:                                        │
              │    Snapshot{Begin,Chunk,End}                            │
              │    CollectionDelta                               │
              └──────────────────────────────────────────────────┘

              ┌──────────────────────────────────────────────────┐
              │  collection-metadata wasm module                 │
              │                                                  │
              │  Interest (dynamic): policy(X)                   │
              │  State (kv):  metadata_ledger:<policy_hex>       │
              │                                                  │
              │  Inputs:                                         │
              │    Mint events (CIP-25 tx.metadata)              │
              │    Mint events (CIP-68 ref-token Produced)       │
              │    Ref-token datum updates (Consumed+Produced    │
              │      of the same ref-token UTxO, new datum)      │
              │    Maestro fallback for historical CIP-25 ──┐    │
              │                                              │    │
              │  Outputs:                                    │    │
              │    MetadataSnapshot                          │    │
              │    MetadataUpdate                            │    │
              └──────────────────────────────────────────────┼────┘
                                                             │
                                                             │
              ┌──────────────────────────────────────────────▼────┐
              │  MaestroFallbackPlane (existing infrastructure)   │
              │  - tx aux_data fetch + cache (aux_data.redb)      │
              │  - per-process semaphore (MAESTRO_MAX_INFLIGHT)   │
              │  - in-flight coalescing, 429-aware backoff        │
              └───────────────────────────────────────────────────┘
```

Both modules share the dynamic-interest pattern from `holder-distribution`: empty static interest, per-policy registration at subscription time. State is keyed by policy hex.

## `collection-holders` module

### Event surface

The snapshot is emitted as a **chunked sequence** (`SnapshotBegin` → `SnapshotChunk` × N → `SnapshotEnd`), not as a single event. Building the whole holdings list as one CBOR payload traps for large policies under the per-call WASM fuel budget — same pattern + same reason as `holder-distribution` (see `WASM_BUDGET_CHUNKING.md`). Consumer semantics: on `SnapshotBegin`, wipe the policy's projection; the sequence is an authoritative replacement.

```rust
#[derive(Serialize, Deserialize, Debug)]
pub struct Holding {
    pub asset_name_hex: String,
    pub holder: HolderRef,
    pub quantity: u64,         // always 1 for NFTs; meaningful for RFTs
}

/// Where supply currently lives. Surfacing script addresses as data
/// (not filtering them) is the key fix for the per-account-cap and
/// stake-credential-grouping limitations of asset-by-policy APIs.
#[derive(Serialize, Deserialize, Debug)]
pub enum HolderRef {
    Stake(String),                                       // 56-char hex stake credential
    Payment(String),                                     // bech32 payment_addr (no stake credential)
    Script { addr: String, label: Option<String> },      // script addr + known-marketplace tag
}

/// Opens a chunked snapshot for one policy.
#[derive(Serialize, Deserialize, Debug)]
pub struct SnapshotBegin {
    pub policy: String,                  // 56-char hex
    pub cursor_slot: u64,
    pub cursor_hash_hex: String,         // empty when host doesn't surface a block hash
}

/// One bounded slice of the snapshot's holdings list.
#[derive(Serialize, Deserialize, Debug)]
pub struct SnapshotChunk {
    pub policy: String,
    pub holdings: Vec<Holding>,          // ≤ SNAPSHOT_CHUNK_HOLDINGS per chunk
}

/// Closes a chunked snapshot. Consumer marks the projection authoritative.
#[derive(Serialize, Deserialize, Debug)]
pub struct SnapshotEnd {
    pub policy: String,
    pub holding_count: u64,              // sanity check across the sequence
}

/// Emitted for each TX touching the policy that produces a non-empty
/// movement list. Same-holder zero-delta movements (change outputs) are
/// netted out before emission.
#[derive(Serialize, Deserialize, Debug)]
pub struct CollectionDelta {
    pub policy: String,
    pub tx_hash: String,                 // 64-char hex
    pub slot: u64,
    pub movements: Vec<Movement>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Movement {
    pub asset_name_hex: String,
    pub from: Option<HolderRef>,         // None on mint
    pub to: Option<HolderRef>,           // None on burn
    pub quantity: u64,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CollectionEvent {
    SnapshotBegin(SnapshotBegin),
    SnapshotChunk(SnapshotChunk),
    SnapshotEnd(SnapshotEnd),
    Delta(CollectionDelta),
}
```

`Movement.from = None` represents a mint (asset materialised into the holder). `Movement.to = None` represents a burn. This matches `asset-transfer`'s existing semantics and gives the consumer a uniform delta surface that handles all qty-affecting events without needing additional channel subscriptions.

### Algorithm

**Cold-start (on first `Add` interest for a policy):**

1. Call `chain_data::utxos_by_policy(X)` — returns `Vec<OutputRef>` from the Dolos `BY_POLICY` index.
2. Call `chain_data::read_utxos(refs)` — resolves to `Vec<TypedOutput>` (address + assets + datum hash). For UTxOs whose ancestor TX is past the archive horizon, `MaestroFallbackPlane` resolves transparently.
3. For each typed output:
   - Decode address → `HolderRef::Stake | Payment | Script`
   - For each asset in the output's value where `policy == X`, emit a `Holding`
4. Aggregate into the state ledger, persist under `holder_ledger:<policy_hex>`.
5. Emit a `SnapshotBegin` → `SnapshotChunk` × N → `SnapshotEnd` sequence at the current cursor.

**Live updates (per block):**

For each TX touching policy `X`:
1. Walk Consumed inputs → for each policy-`X` asset, decrement holder's qty (or remove from ledger if qty=0)
2. Walk Produced outputs → for each policy-`X` asset, increment holder's qty (insert if missing)
3. Net same-holder zero-delta movements (change outputs)
4. Emit `CollectionDelta` with non-zero movements only

**Rollback:**

Platform v2 contract: the host re-feeds events from the rollback cursor forward, and the chain-point-keyed dApp `apply_event` handler re-applies idempotently (per `MITOS_COMPANION_RUNTIME_V1.md` Q3). The module logs the rollback for operator visibility but maintains no per-cursor undo log on the module side — convergence comes from re-apply, not rewind.

### State management

Storage in mitos kv-state:

| Key | Value |
|---|---|
| `tracked-policies` | CBOR `Vec<String>` of 56-char policy-id hexes (the active interest set; restored on `init`) |
| `ledger:<policy_hex>` | CBOR `PolicyLedger { holdings: BTreeMap<HolderKey, BTreeMap<Vec<u8>, u64>> }` — nest-by-holder, keyed on the in-memory `HolderKey` (no `Script.label`, that's a presentation concern resolved at emit time) |
| `rebootstrap-cursor` | 8-byte BE `predicate_idx` into the sorted tracked-policy list — drives the chunked re-entrant rebootstrap. Single global key, not per-policy: the round walks policies in sorted order and the cursor advances only when a policy's emit closes |

Size estimates:
- 10k-supply NFT collection, ~3k distinct holders: ~10k entries × ~80 bytes = **~800KB**
- 200-card RFT collection, 50 avg copies, 3 avg holders per asset: ~10k entries = **~800KB**
- 5k-supply NFT collection with ~30% in marketplace scripts: ~5k entries (marketplaces show as `HolderRef::Script` entries) = **~400KB**

WASM-budget chunking (per [WASM Budget Chunking](./WASM_BUDGET_CHUNKING.md)) handles snapshot emission for ledgers exceeding the per-emission budget. Per-delta payloads are small (TX-bounded) and don't need chunking.

### Edge cases

- **NFT in marketplace escrow.** Asset moves to `Script(addr, Some("jpg.store v3"))`. Consumer (e.g. `collection-ownership`) decides whether to re-attribute to the lister via marketplace knowledge or surface "in marketplace" state.
- **RFT partial listing.** Player owns 5 copies, lists 2 → snapshot has two `Holding` entries for that asset (3 at stake, 2 at script). Movements emit on each listing/delisting/sale.
- **Treasury holdings.** Show as regular `Stake` entries. Consumers exclude or include based on what "circulating supply" means in their domain.
- **Mints contributing to growing RFT supply.** Each booster-mint TX produces qty into the buyer's wallet, emits a Movement with `from = None`. Snapshot at any cursor reflects current supply; supply at cursor C+1 may exceed cursor C.
- **Bulk burns.** Game mechanics (sacrifice-to-summon) burn qty from holder, Movement emits with `to = None`. Holder qty decrements; entry removed when qty=0.
- **Address with no stake credential.** Enterprise addresses, multi-sig addresses, etc. → `HolderRef::Payment(addr)`. Less common but valid.
- **Asset held at script address with stake credential.** Rare but exists (some marketplace contracts). Classified as `Script` based on payment-credential type, not presence of stake.
- **Policy classified as FT during pre-flight.** Reject subscription with explicit error. Surfaces the same FT-classification check that exists in `MaestroOneShot` today. Avoids accidental mass-subscription against a CNT.

### Interest model

```
Interest::Holds(Policy(X))
```

Identical to `holder-distribution`'s shape. Module is dynamic-interest only — no static config. Adding/removing interest on the fly is supported via the standard mutation endpoint.

A subscription's `Add` triggers cold-start scan + chunked snapshot emission. `Remove` clears the ledger from state immediately (per Resolved Decisions #1 — no TTL, no refcount; rebuild on next subscribe).

## `collection-metadata` module

### The CIP-68 facade for CIP-25

CIP-25 and CIP-68 ultimately answer the same consumer question: *what's the current metadata for this asset?* They differ in storage and updatability:

- **CIP-25**: Metadata lives in `tx.metadata` of the mint TX (label `721`). Immutable after mint. No update mechanism.
- **CIP-68**: Metadata lives in the datum of a reference-token UTxO (`100`-label prefix). Mutable via spend-and-recreate of the ref token by an authorised party.

The unifying observation:

> *CIP-25 is CIP-68 with a single immutable update at mint time, broadcast via TX metadata instead of via a ref-token datum.*

The module presents both as a uniform `MetadataUpdate` event stream. CIP-25 collections emit one `Initial` event per asset and then go silent. CIP-68 collections emit `Initial` + zero-or-more `Updated`. Consumer code handles them identically.

### Event surface

```rust
#[derive(Serialize, Deserialize, Debug)]
pub struct MetadataSnapshot {
    pub policy: PolicyId,
    pub cursor: ChainPoint,
    pub entries: Vec<MetadataEntry>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MetadataEntry {
    pub asset_name_hex: String,
    pub payload: CanonicalMetadata,
    pub standard: MetadataStandard,
    pub version: u64,                  // CIP-25: always 1; CIP-68: ref-token datum version
    pub immutable: bool,               // true for CIP-25; false for CIP-68
    pub source_tx: TxHash,             // TX that produced this version
}

/// Normalised across CIP-25 (tx.metadata.721) and CIP-68 (ref-token
/// datum constructor 0 field 0). Preserves the metadata map verbatim;
/// projections (name, image, attributes) are consumer-side concerns.
#[derive(Serialize, Deserialize, Debug)]
pub struct CanonicalMetadata {
    pub fields: BTreeMap<String, MetadataValue>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum MetadataValue {
    Str(String),
    Int(i64),
    Bytes(Vec<u8>),
    Array(Vec<MetadataValue>),
    Map(BTreeMap<String, MetadataValue>),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum MetadataStandard {
    Cip25,
    Cip68V1,    // datum constructor 0, fields [metadata, version]
    Cip68V2,    // datum constructor 0, fields [metadata, version, extra]
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MetadataEvent {
    Snapshot(MetadataSnapshot),
    Initial { policy: PolicyId, cursor: ChainPoint, entry: MetadataEntry },
    Updated { policy: PolicyId, cursor: ChainPoint, entry: MetadataEntry, prior_version: u64 },
    Burned  { policy: PolicyId, cursor: ChainPoint, asset_name_hex: String, source_tx: TxHash },
}
```

`Burned` fires only for CIP-68 (when the ref token UTxO is spent without re-production). CIP-25 has no equivalent — a CIP-25 user-token burn doesn't invalidate the metadata, it just leaves the metadata orphaned. Consumers can decide whether to drop those entries (cross-reference with `collection-holders` to see if any copies remain).

### Algorithm

**Cold-start — CIP-68 path (current state, no archive horizon problem):**

1. Call `chain_data::utxos_by_policy(X)` → resolve refs
2. For each output whose value contains an asset with the `000643b0` (label 100) prefix:
   - Decode the datum (inline or via hash lookup) as CIP-68 constructor 0
   - Extract `(asset_name_hex, metadata, version)` (strip the `000643b0` prefix to get the user-token-shared name suffix)
3. Build `MetadataEntry` per discovered ref token
4. Emit `MetadataSnapshot`

**Cold-start — CIP-25 path (historical, may require Maestro fallback):**

1. Call `chain_data::utxos_by_policy(X)` → derive the set of currently-held `asset_name_hex` values from the typed outputs. This is the set of assets whose metadata we need.
2. Resolve `asset_name → mint_tx_hash` via the module-internal Maestro path (`/policy/{id}/assets` catalog + per-asset `/assets/{id}/txs?type=mint`). The data plane's existing `MaestroClient` is the call site; rate-limit envelope is shared.
3. For each `(asset_name, mint_tx_hash)`:
   - Call `chain_data::tx_metadata(tx_hash)` — tiered: Dolos archive first, `MaestroFallbackPlane` via `fetch_aux_data` for TXs past the horizon
   - Decode the CBOR aux-data payload, extract `[721][policy_hex][asset_name]` → `CanonicalMetadata`
4. Emit `MetadataSnapshot`

Both `tx_metadata` (data plane API at `lib.rs:241`) and `MaestroFallbackPlane` (UTxO fallback at `crates/mitos-platform/src/maestro_fallback_plane.rs`) are already in place. The only platform-side work for the CIP-25 path is verifying that `MaestroClient::fetch_aux_data` returns aux-data CBOR including the CIP-25 (`721`) label across all known Maestro response shapes.

**Cold-start — Hybrid (policy uses both, common during CIP-25→CIP-68 migrations):**

Try CIP-68 path first; for asset_names not covered by a ref token, fall back to CIP-25 path. Each entry carries its `standard` field so consumers know the provenance.

**Live updates:**

- **New mint (CIP-25)**: `cip-25-mint` channel surfaces tx metadata → emit `Initial`
- **New mint (CIP-68)**: ref-token Produced → emit `Initial`
- **Datum update (CIP-68)**: ref-token UTxO Consumed and same asset_name Produced in same TX with new datum → emit `Updated` (carry `prior_version`)
- **Burn (CIP-68)**: ref-token Consumed without matching Produced → emit `Burned`

Mints are fanned in via subscriptions to the existing `cip-25-mint` and `cip-68-mint` channels (module-as-consumer pattern) rather than re-implementing the parse. This keeps the metadata module focused on **projection**, not chain recognition.

### State management

| Key | Value |
|---|---|
| `metadata_ledger:<policy_hex>` | CBOR-encoded `HashMap<asset_name_hex, MetadataEntry>` |
| `metadata_cursor:<policy_hex>` | Last applied `ChainPoint` |

Size estimates:
- 10k-supply CIP-25 NFT collection, ~2KB avg metadata per asset: **~20MB raw**, ~5MB CBOR
- 200-card CIP-68 RFT collection (TCG), ~3KB avg datum payload: **~600KB**
- 10k-supply CIP-68 NFT collection: similar to CIP-25 — same data, different storage

For large CIP-25 collections, **snapshot emission is the dominant cost**. Chunking is essential — and the chunker (re-entrant since the WASM budget work) handles this. Per-update payloads are TX-bounded and small.

### Edge cases

- **Historical CIP-25 collection past archive horizon.** Mint TXs are pruned from Dolos. Maestro fallback resolves via `fetch_aux_data`. Bootstrap cost: 1 Maestro call per asset_name (N+1 against a 10k collection = real cost, but bounded and one-time per policy). Subsequent state lives in `metadata_ledger`; no re-fetch needed.
- **Mint TX metadata is malformed.** Some CIP-25 mints have non-spec-compliant metadata. Module emits `Initial` with whatever can be parsed; logs a warning. Consumer decides whether to drop or accept partials.
- **CIP-68 ref token rotated to new policy.** Rare. Treated as Burn under old policy + Initial under new — symmetric with mint/burn semantics.
- **CIP-68 datum points at off-chain data (URI in metadata).** Module captures the URI verbatim. Following the URI is consumer-side (e.g. archivist preservation workflow).
- **Asset minted before policy was tracked by mitos.** Cold-start covers via the history scan. No "missed mint" gap.
- **Multiple mint TXs for same asset_name (RFT supply growth).** Each mint emits a `MetadataInitial` if it carries new metadata; subsequent re-mints of the same asset_name with same metadata are silently deduped by version comparison. (Edge case — most RFTs mint identical metadata across all copies.)

### Data plane surface

What's already available (verified 2026-05-21 against `crates/mitos-data-plane/src/lib.rs`):

| Host-fn | Used by | Status |
|---|---|---|
| `chain_data::utxos_by_policy(policy_id) -> Vec<OutputRef>` | both | `lib.rs:129` — exists (`holder-distribution` uses it) |
| `chain_data::read_utxos(refs) -> Vec<(OutputRef, TypedOutput)>` | both | `lib.rs:81` — exists with Maestro fallback via `MaestroFallbackPlane` |
| `chain_data::tx_metadata(tx_hash) -> Option<Vec<u8>>` | `collection-metadata` | `lib.rs:241` — **already shipped**, returns CBOR aux-data payload (CIP-25 metadata at label 721 is decoded by the module) |

What still needs to land for the CIP-25 historical bootstrap path:

**Mint-TX resolution for currently-held assets.** Given a `policy_id` and the set of currently-held `asset_name_hex` values (derivable from `utxos_by_policy` + `read_utxos`), we need to find each asset's mint TX hash so we can call `tx_metadata` against it. Two implementation paths, neither requiring a new host-fn:

1. **Module-internal Maestro enumeration.** The module calls Maestro's `/policy/{id}/assets` (catalog) and `/assets/{id}/txs?type=mint` (or equivalent) via the data plane's existing Maestro client, builds the `asset_name → mint_tx_hash` map, then runs `tx_metadata` per mint TX. The Maestro client and its rate-limit envelope (`MAESTRO_MAX_INFLIGHT`) are already in place; this just calls them.
2. **Native Dolos mint-by-policy index.** Add a `mint_history_by_policy` host-fn backed by a Dolos index. Defer until cold-start performance dictates; module-internal path works fine for tens-of-thousands-of-asset collections.

Option 1 is the right Phase 3 default. No new host-fn proposal. The module-internal pattern is consistent with how `holder-distribution` derives ledger state from `utxos_by_policy` results without needing the platform to surface a "holder ledger" primitive.

### Interest model

```
Interest::Policy(X)
```

Same dynamic shape as `collection-holders`. Adding interest triggers cold-start scan + `MetadataSnapshot`. Worth noting that a consumer wanting **only** metadata (no holders) is a valid subscription pattern — e.g. a discovery/explorer worker that doesn't need ownership data but does want trait info.

## Maestro fallback strategy

A core principle of this design: **workers do not call Maestro directly.** Mitos owns the chain-data resolution layer, including the fallback to Maestro for data past the Dolos archive horizon. This is the same pattern that `MaestroFallbackPlane` already implements for UTxO ancestry resolution.

### What gets fallback-resolved

| Data | Primary source | Fallback (Maestro) |
|---|---|---|
| Current UTxO set for policy | `chain_data::utxos_by_policy` (Dolos `BY_POLICY` index) | None — always current |
| UTxO content (address, value, datum hash) | `chain_data::read_utxos` (Dolos) | `/transactions/{tx}/outputs/{idx}/txo` |
| CIP-25 mint metadata (recent) | `chain_data::tx_metadata` from Dolos aux-data archive | None needed |
| CIP-25 mint metadata (historical, past archive horizon) | — | `/transactions/{tx}/metadata` via `MaestroClient::fetch_aux_data` |
| CIP-68 ref-token datum | `chain_data::read_utxos` (current UTxO state) | None — always current |
| Asset catalog for policy (`mint_history_by_policy`) | (proposed) Dolos mint index or aux fallback | `/policy/{id}/assets` (phase-1 fallback) |

### Why centralise the fallback

- **Single rate-limit envelope.** Today's `collection-ownership` worker hits Maestro from its own DO, with its own retry logic and no coordination with other workers. Centralising in mitos means one process-wide semaphore (`MAESTRO_MAX_INFLIGHT`) governs all fallback traffic across all consumers.
- **One cache.** `aux_data.redb` already caches resolved aux data. Pulling CIP-25 metadata via the platform means the cache amortises across all metadata consumers, not per-worker.
- **One classification path.** FT-detection, malformed-metadata handling, ref-token parsing — all live in the module, applied uniformly. Workers consume typed events.
- **Easier to swap.** When the Dolos archive horizon extends, or when Maestro is replaced by a different fallback source (a self-hosted Blockfrost-shape, a different indexer), only the platform changes.

### Cost shape

The cost concern with Maestro-mediated bootstrap is the per-asset metadata fetch for historical CIP-25 collections. Order-of-magnitude:

- 10k-supply collection past archive horizon → up to 10k Maestro aux-data fetches at cold-start
- With `MAESTRO_MAX_INFLIGHT=4` and ~200ms per call → ~8 minutes wall time, fully sequenced
- One-time per policy. Cached forever in `aux_data.redb` after.

This is comparable to the current `cnft.tools` paginated bootstrap that `collection-ownership` runs today (minutes for active policies). It's acceptable, and the cache means subsequent subscriptions for the same policy are free.

For fresh policies (post-mitos-deployment), there is no historical-fetch cost — mints flow through `cip-25-mint` in real time and the metadata is cached on first observation.

## Composition: how a consumer uses both

A worker subscribing to both modules for the same policy makes one CBOR subscription request:

```rust
SubscribeRequest {
    targets: vec![
        SubscribeTarget { module_id: "collection-holders".into(), ... },
        SubscribeTarget { module_id: "collection-metadata".into(), ... },
    ],
    companion_key: policy_id.into(),
    client_id: "ownership-prod".into(),
    interests: vec![Interest::Policy(policy_id.clone())],
    resume_from: None,
    dial_back: None,
}
```

Both modules independently dial back to the worker's `/_internal/apply-<channel>` endpoints. Subscribed companions see two separate event streams, demuxed by channel name. Snapshot semantics are independent — `Snapshot{Begin,Chunk,End}` and `MetadataSnapshot` may arrive at slightly different cursors, and the worker reconciles them when both have caught up. (Versioning by cursor makes this safe — see [Open Questions](#open-questions) for cross-stream cursor coordination.)

### `collection-ownership` worker integration

The worker's current `handle_configure` path (per [`COLLECTION_OWNERSHIP_MITOS_INTEGRATION.md`](../../../cnft.dev-workers/docs/design/COLLECTION_OWNERSHIP_MITOS_INTEGRATION.md)) already embeds the mitos companion runtime. Wiring in collection-holders + collection-metadata is additive:

1. On policy onboarding, the worker subscribes to both targets in one call.
2. `Snapshot{Begin,Chunk,End}` apply → `ownership` table populated. Same SQL path as today's `reconcile_full_with_traits`, but with explicit `HolderRef::Script` handling (either include with script address as owner, or filter — per-policy config).
3. `MetadataSnapshot` apply → `asset_traits` table populated. Same trait-bitmap construction as today's D1 trait-reconcile path, but sourced from typed events rather than Maestro/cnft.tools fetches.
4. `CollectionDelta` → existing transfer SQL path (already runs from `apply_transfer`).
5. `MetadataUpdate::Initial` → trait bitmap insert.
6. `MetadataUpdate::Updated` → trait bitmap rewrite (CIP-68 only; rare).
7. Maestro pagination on `/admin/policies/.../sync` becomes a no-op or is replaced with a `Recapture` request to mitos.

The Maestro client in the worker is retained as a defensive fallback (the [`COLLECTION_OWNERSHIP_MITOS_INTEGRATION.md`](../../../cnft.dev-workers/docs/design/COLLECTION_OWNERSHIP_MITOS_INTEGRATION.md) doc explicitly preserves it for `/admin/policies/.../sync`). With mitos-side fallback in place, this becomes truly defensive — invoked only on mitos host outage.

### TCG consumer integration (forward-looking)

For a future TCG worker, the same subscription pattern applies. Differences:

- The `MetadataSnapshot` is the card definition catalogue (200 cards × ~3KB datum = ~600KB)
- The `Snapshot{Begin,Chunk,End}` is per-player inventory (10k user tokens × ~80 bytes = ~800KB)
- `MetadataUpdate::Updated` fires when card art / stats are rotated (e.g. seasonal balance changes via datum updates)
- `Movement` events drive UI: card purchased, card listed, card transferred, booster opened
- `Movement.from = None` events specifically surface as "new card minted to your wallet" UX

No special TCG-aware code paths needed in mitos. The TCG worker is just another consumer of the same primitives.

## Comparison to `holder-distribution`

| | `holder-distribution` | `collection-holders` |
|---|---|---|
| Target token shape | CNT | NFT + RFT |
| Wire shape | `holder → total_qty` | `(asset_name, holder, qty)` tuples |
| Snapshot size driver | holder count | supply |
| Script-address policy | filter (DEX pools = noise) | surface (marketplaces = data) |
| Mint pattern | continuous (mint/burn always live) | mint-once or bounded edition with edition growth |
| Reorg semantics | balance arithmetic | asset movement |
| FT classification | required | hard-reject |
| Companion use cases | distribution analysis, top-N tracking, gini | per-asset ownership, marketplace presence, supply tracking |

The two modules are explicitly *peers*, not alternatives. A policy that has both an NFT collectible side and a fungible-token side (rare but possible) could be subscribed to both, projecting different views of the same chain data.

## Phased delivery

### Phase 1 — `collection-holders` module ships ✅

**Shipped 2026-05-20.** Implementation at `community-modules/collection-holders/collection_holders.rs`. All six v2 Guest exports implemented including the chunked re-entrant `rebootstrap`. Wire types at `crates/mitos-community-events/src/collection_holders.rs`. Four golden fixtures land alongside.

**Acceptance work outstanding:**
- 48-hour live test against a known policy (suggest islanova_apex_legends or aliens) — requires deployed mitos host.
- `HolderRef::Script.label` registry wire-up (per Resolved Decision #3).
- FT-classified policy hard-reject on subscribe (defensive guard against accidental CNT subscriptions).

### Phase 2 — `collection-metadata` (CIP-68 path only)

- Module implementation in `community-modules/collection-metadata/`
- Cold-start via `utxos_by_policy` + ref-token datum decode
- Live tail via ref-token Produced/Consumed
- Snapshot + Update + Burned events
- Acceptance: subscribed against a known CIP-68 collection, emits canonical metadata for every asset, datum updates surface as `MetadataUpdate::Updated`

Estimated: 3–4 days. CIP-25 path explicitly deferred.

### Phase 3 — `collection-metadata` CIP-25 facade

- Module-internal Maestro enumeration (`/policy/{id}/assets` + per-asset mint TX lookup) using the data plane's existing Maestro client
- Use existing `chain_data::tx_metadata` host-fn (`crates/mitos-data-plane/src/lib.rs:241`) with its existing fallback for aux-data resolution
- CIP-25 cold-start path: derive asset_name set from `utxos_by_policy`, resolve mint TX per asset via Maestro, fetch metadata via `tx_metadata`
- CanonicalMetadata normalisation across CIP-25 and CIP-68
- Verify `MaestroClient::fetch_aux_data` surfaces label-721 metadata across Maestro response shapes
- Acceptance: subscribed against islanova_apex_legends (CIP-25, historical), receives full metadata snapshot including script-locked supply's metadata. Maestro call count is bounded by collection size and one-time per policy.

Estimated: 3–4 days. No new host-fn work — data plane surface is sufficient as-is.

### Phase 4 — `collection-ownership` worker cuts over to mitos bootstrap

- `handle_configure` subscribes to both `collection-holders` + `collection-metadata`
- Maestro pagination paths in `sync.rs` become no-ops (kept for defensive `/admin/policies/.../sync` only)
- Verify: subscribe a previously Maestro-bootstrapped policy via mitos, confirm asset count includes script-locked supply that was previously missing
- Acceptance: `seen_assets` after mitos bootstrap ≥ `seen_assets` from Maestro bootstrap for any policy, with the delta being script-locked supply

Estimated: 3–5 days.

### Phase 5 — `mint_history_by_policy` native host-fn (optional, deferred)

- New `chain_data::mint_history_by_policy(policy_id) -> Vec<(asset_name_hex, tx_hash)>` host-fn backed by a Dolos mint index
- Replaces the module-internal Maestro enumeration in Phase 3 with a single host-fn call
- Only needed if phase-3 cold-start cost is material in practice

Estimated: 5–10 days. Deferred until phase 4 telemetry justifies the work.

## Resolved decisions

These were open during initial drafting and have since been closed out. Captured here so the design's reasoning stays auditable.

1. **GC on `Remove` interest.** Clear `holder_ledger:<policy_hex>` and `metadata_ledger:<policy_hex>` immediately when the last companion drops interest. Rationale: without an active consumer, the ledger stops receiving deltas and goes stale fast; retaining it is worse than rebuilding on the next subscription. Cold-start cost is the same whether the data was retained-and-stale or absent.

2. **Cross-module cursor coordination.** Consumer reconciles. `Snapshot{Begin,Chunk,End}` and `MetadataSnapshot` may arrive at different cursors; the consumer applies deltas from each stream forward independently. Matches the existing emission-id ordering semantics; no module-side coordination needed.

3. **`HolderRef::Script` label registry.** Use the existing `shared-crates/address-registry` crate. It already has rich typed labelling for marketplaces (`JpgStoreV1`–`V4`, `Wayup`), DEXes (Splash, DexHunter, Minswap, CSWAP, SaturnSwap), and vesting contracts (CrowdLock). The `collection-holders` module looks up `HolderRef::Script.label` via this registry at emission time. Avoids duplicating known-script knowledge; the registry becomes the single source of truth across mitos modules and consumer workers.

4. **Catalog completeness on RFT policies.** Ref tokens are tracked from the moment they hit chain, regardless of whether any user tokens exist yet. For a TCG, this means card definitions appear in `MetadataSnapshot` as soon as the treasury mints the ref tokens (typically all at once at project launch). Treasury ref-token holdings appear in `Snapshot{Begin,Chunk,End}` as regular `HolderRef::Stake` entries against the treasury stake address. Consumers filter via standard projection logic.

5. **Metadata version semantics.** Use the CIP-68 datum-version field directly (per spec). CIP-25 entries get `version = 1` always. Consumers detect updates by hash-comparing `payload` across `MetadataEntry` revisions; the module doesn't maintain a separate monotonic counter.

6. **`mint_history_by_policy` pagination during Phase 3 cold-start.** Maestro paginates the catalog endpoint at ~100 assets per call. Pagination happens **inside** the data-plane implementation of the host-fn; the module sees one blocking call returning the complete `Vec<(asset_name_hex, tx_hash)>`. One-time per policy, acceptable. The chunked snapshot emission that follows is delivered to the consumer via the standard dialback pattern, with bulk-apply throughput once [`DIALER_BULK_APPLY.md`](./DIALER_BULK_APPLY.md) ships (see [Snapshot delivery throughput](#snapshot-delivery-throughput) below).

7. **TCG card-state vs metadata-update typing.** Consumer-side. Module emits `MetadataUpdate::Updated` with the full new payload; consumer projects "art rotated" vs "stats rebalanced" vs other kinds via whatever schema makes sense for that domain. Keeps the protocol surface narrow.

## Snapshot delivery throughput

For large policies, snapshot delivery cost is dominated by the per-emission dialback round-trip. The partition-keyed dialer pool (shipped 2026-05-14 at `lanes=8`) sets the current ceiling at ~50 events/sec. For a 10k-supply collection chunked into ~100 emissions, that's ~2 seconds wall time at the dialer side; for a recapture against an actively-traded collection it's tens of seconds.

[`DIALER_BULK_APPLY.md`](./DIALER_BULK_APPLY.md) (design draft, 2026-05-16) batches up to M emissions per POST with per-emission status in the response. At M=50 and the same 8 lanes, throughput goes to ~2,600 events/sec — well past the WS-transport-era number. For collection-modules snapshots, bulk apply turns cold-start delivery from "noticeable" into "imperceptible."

This is a soft dependency: collection-holders + collection-metadata work without bulk apply, just less efficiently for large snapshots. Phase 1 doesn't block on bulk apply landing. But the design assumes it lands soon, and consumer companion implementations (collection-ownership worker, future TCG worker) should advertise bulk support per the handshake described in `DIALER_BULK_APPLY.md`.

## Non-goals

- **Custom indexers per consumer.** The module IS the indexer for the bounded-collectible domain. Workers don't get their own.
- **Discovery / classification.** "Is this policy a collectible or a CNT?" — answered upstream (config or upstream detection module). `collection-holders` rejects FT-classified policies; it doesn't classify them.
- **Cross-policy aggregation.** "Show me all CNFT-shaped holdings for stake X" — that's a consumer-side rollup across multiple per-policy subscriptions.
- **Marketplace attribution.** Re-projecting `HolderRef::Script` back to the actual lister via marketplace knowledge — `jpg-store-mirror` and friends do this. Module surfaces raw script holdings only.
- **Real-time floor-price tracking.** Different module entirely.
- **CNT distribution.** [`holder-distribution`](./HOLDER_DISTRIBUTION_MODULE.md) owns this.

## References

- [`MITOS_PLATFORM_V2.md`](../strategy/MITOS_PLATFORM_V2.md) — **target dispatch model**: eUTXO-event-filtered-by-interest, not block-iteration. Both modules implement against this surface.
- [`MULTI_CLIENT_COMPANIONS.md`](./MULTI_CLIENT_COMPANIONS.md) — `(module_id, client_id, companion_key)` triple identity (now required, not optional)
- [`WASM_BUDGET_CHUNKING.md`](./WASM_BUDGET_CHUNKING.md) — snapshot chunking for large emissions (Phases 1–5 shipped 2026-05-19)
- [`EVENT_DELIVERY_RESILIENCE.md`](./EVENT_DELIVERY_RESILIENCE.md) — at-least-once delivery, recapture semantics, Maestro fallback context
- [`DIALER_CONCURRENCY.md`](./DIALER_CONCURRENCY.md) — partition-keyed pool underpinning per-policy lanes (shipped 2026-05-14 at lanes=8)
- [`DIALER_BULK_APPLY.md`](./DIALER_BULK_APPLY.md) — bulk-apply throughput design; soft dependency for efficient snapshot delivery (design draft)
- [`HOLDER_DISTRIBUTION_MODULE.md`](./HOLDER_DISTRIBUTION_MODULE.md) — sibling CNT module; superseded in specifics but pattern reference holds
- [`DOMAIN_REFACTOR.md`](./DOMAIN_REFACTOR.md) — superseded as implementation vehicle, but the `Mint` / `Burn` / `AssetMovement` taxonomy remains canonical and underpins these modules' event shapes
- [`COLLECTION_OWNERSHIP_MITOS_INTEGRATION.md`](../../../cnft.dev-workers/docs/design/COLLECTION_OWNERSHIP_MITOS_INTEGRATION.md) — consumer-side worker migration plan
- `crates/mitos-data-plane/src/lib.rs:81,129,241` — host-fns these modules use (`read_utxos`, `utxos_by_policy`, `tx_metadata` all already shipped)
- `crates/mitos-platform/src/maestro_fallback_plane.rs` — existing Maestro fallback implementation
- `crates/mitos-platform/src/maestro.rs` — Maestro client + `aux_data.redb` cache
- `community-modules/holder-distribution/holder_distribution.rs` — implementation reference for module structure
- `~/code/defrag/shared-crates/address-registry/src/registry.rs` — typed marketplace / DEX / vesting script labels used for `HolderRef::Script.label`
- CIP-25 spec: <https://cips.cardano.org/cip/CIP-25>
- CIP-68 spec: <https://cips.cardano.org/cip/CIP-68>
