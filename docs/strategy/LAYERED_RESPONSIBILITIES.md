# Layered responsibilities — where logic lives

> **Status: layered model still canonical; in-tree-indexer layer
> mostly retired (2026-05).** The three-layer model (in-tree
> indexer, wasm module, companion DO) and the
> community-modules-first heuristic that fell out of this
> analysis remain authoritative. The three legacy in-tree
> indexers used as examples below (`collection-ownership-indexer`,
> `marketplace-indexer`, `mint-burn-indexer`) **retired** —
> their chain-recognition surface now lives in community
> modules (`asset-transfer`, `jpg-store-{listing,sale,offer}`,
> `cip-25-mint`, `cip-68-mint`, `standard-burn`,
> `burn-address`, `asset-metadata-update`). Only the
> residual-pass `none-match-indexer` remains in-tree.
>
> The body below uses past-tense framing for the three retired
> indexers but the layering reasoning is unchanged.

Mitos's three-layer model: in-tree indexer, wasm module, companion
DO. This doc names what belongs in each layer, the decision
heuristic for new work, and the patterns that fall out.

## The forcing observation

`jpg-store-mirror`'s wasm module (`modules/jpg_co.rs`) decodes
jpg.store CO datums, walks the V2/V3 script addresses, and emits
`Created` / `Spent` events. The host's `marketplace-indexer`
already does the **same script-address recognition and datum
decode**, emitting `Marketplace::OfferCreate` / `OfferAccept` /
`OfferCancel`.

This is duplicated work. The wasm module exists because:

- jpg-store-mirror needed a richer event shape than
  `marketplace-indexer`'s payloads expose (specifically: metadata-
  resolved datum bytes for reconstructing the CO datum hash).
- The companion-runtime path was wasm-module-only until
  `UNIFIED_SUBSCRIBE.md` landed — there was no way to subscribe a
  companion to in-tree indexer events.

Both reasons are now removable. The unified-subscribe path lets
companions consume in-tree indexer events directly. Extending
`marketplace-indexer`'s payload shapes is a one-time cost that
benefits every future consumer.

That's the immediate forcing function. The general question
underneath it: **when should new logic live in the host vs a
wasm module vs the companion DO?**

## The three layers

```
   ┌─────────────────────────────────────────────────────────┐
   │ Companion DO                                            │
   │  - state projection                                     │
   │  - ui-flow / admin / RPC surfaces                       │
   │  - business workflow                                    │
   │  - app database                                         │
   └──────────────────┬──────────────────────────────────────┘
                      │  WS / subscribe
   ┌──────────────────┴──────────────────────────────────────┐
   │ Mitos host                                              │
   │                                                         │
   │  ┌──────────────────────────┐    ┌──────────────────┐   │
   │  │ Wasm module              │    │ In-tree indexer  │   │
   │  │ (community or per-dApp)  │    │ (framework)      │   │
   │  │                          │    │                  │   │
   │  │ - chain-recognition:     │    │ - chain follower │   │
   │  │   datum decode, script   │    │ - dispatch +     │   │
   │  │   classification, deep   │    │   claim coord    │   │
   │  │   brand-specific logic   │    │ - residual pass  │   │
   │  │ - dApp-specific          │    │ - host primitives│   │
   │  │   transformations,       │    │   nobody could   │   │
   │  │   aggregations, ML       │    │   reasonably do  │   │
   │  │   features               │    │   from a sandbox │   │
   │  └──────────────────────────┘    └──────────────────┘   │
   └─────────────────────────────────────────────────────────┘
```

### Wasm module

**Default for chain-recognition.** Anything where two dApps
would otherwise rewrite the same code — marketplace classification,
mint/burn detection, DEX swap recognition, lending operations,
per-contract datum decode — lives in a wasm module. **Community
modules** (`mitos/community-modules/<name>/`) are auto-loaded
and addressable by name from any companion; **private modules**
ship in the dApp's own repo for one-offs that aren't worth
sharing.

**Sandboxed.** Wasm runs in mitos-platform's host_v2; can't
crash the host; resource budgeted; can be hot-swapped without a
mitos restart. Trade-off vs in-tree Rust: ~5–20% per-event
overhead on the dispatch path. Acceptable for the composition
gains.

**One canonical event shape per module.** A community module
owns one typed payload definition in `mitos-community-events::<name>`.
Every consumer decodes against the same definition; no per-dApp
events crates that drift.

**See:** `COMMUNITY_MODULES.md` for the directory convention,
auto-load mechanism, and the migration playbook for promoting
a private module to community.

### In-tree indexer

**Framework-level concerns.** The bones of the platform:
chain follower, event dispatch, claim coordination, residual
pass, the indexer trait surface itself. Things a sandboxed wasm
module genuinely can't do (e.g. cross-indexer claim
synchronisation for the residual `none-match` pass).

**Not the destination for popular wasm modules.** The platform
doesn't accumulate brand-specific decode logic in-tree — that
work belongs in community modules even when widely used.
Existing in-tree indexers (`marketplace-indexer`, `mint-burn-indexer`,
`collection-ownership-indexer`) shipped and work; they stay until
there's a concrete reason to migrate them out. New chain-
recognition work doesn't add to the in-tree pile.

### Companion DO

**Consumer-side state machine.** Subscribes to events (from
in-tree indexers and/or wasm modules), projects them into the
app's SQL store, exposes RPC + ui-flow surfaces, handles admin
endpoints + business workflow.

**Where most app code lives.** A typical dApp is mostly its
companion DO + frontend; the wasm-module surface is empty.

## The decision heuristic

For any new piece of logic, ask:

```
1. Is this chain-recognition logic? (e.g. "decode this datum,
   recognize this script, classify this transaction")
   → wasm module
     - community module if more than one dApp wants it
     - private module if it's a one-off

2. Is this dApp-specific transformation? (e.g. "compute a
   semantic fingerprint from collection metadata", "extract ML
   features from a price stream")
   → wasm module (private)

3. Is this state projection + application logic? (e.g. "fold
   marketplace events into an offers table", "expose a
   /my-offers endpoint")
   → companion DO

4. Is this framework plumbing? (e.g. chain follower, dispatch,
   claim coordination, the indexer trait itself)
   → in-tree
```

The rule of thumb: **community wasm module first; in-tree only
for framework plumbing.** Blockchain dApp logic is a boundless
sea — trying to anticipate the right primitive set in-tree
leads to confusion and overlap. Composition over primitives.

## Examples — running through the heuristic

| Logic | Layer | Why |
|---|---|---|
| jpg.store CO datum decode | Community module (`jpg-co`) | Brand-specific decode; multiple dApps benefit |
| wayup CO datum decode | Community module (`wayup-co`) | Same pattern, different brand — parallel module |
| Marketplace sale classification across brands | Community module | Per-brand decode + classification ships per brand |
| "Mints over 10k tokens to addresses on the alert list" | Companion DO | Filter is application policy |
| Semantic fingerprint of collection metadata | Wasm module (private) | dApp-specific transformation, no other consumer |
| Ownership projection (mint + burn + transfer fold) | Companion DO | App-owned database schema |
| `/my-offers` HTTP endpoint | Companion DO | Application API |
| ui-flow snapshot/delta for the frontend | Companion DO | Application UX |
| Backfill walking unspent UTxOs at a script address | Community module's bootstrap | Per-module concern — `mitos-platform` runs the bootstrap pass against the module's declared `[interest]` |
| Residual `none-match` pass for unclaimed asset movement | In-tree | Cross-indexer synchronisation; can't run from sandbox |
| Chain-sync pipeline (block ingest, rollback handling) | In-tree | Framework plumbing |

## Implication for the jpg-store-mirror

Under this model, the right end state for `jpg-store-mirror`:

- **Promote `modules/jpg_co.rs` to a community module** — it
  moves from the dApp's repo to
  `mitos/community-modules/jpg-co/`. Any future CO-tracking
  dApp subscribes by name without copying the source. See
  `COMMUNITY_MODULES.md`.
- **Companion DO subscribes by name** to the auto-loaded
  jpg-co community module. The current additive subscription
  to the in-tree `marketplace-indexer` for typed
  offer-lifecycle events stays for now (Phase 4–5 of the
  relayering plan wires it up for DELETE handling) — it works
  and ships; the in-tree indexer is grandfathered. The
  long-term direction is brand-specific community modules
  handle full lifecycle.
- **Event types live in `mitos-community-events`** — a shared
  crate with one submodule per community module. Consumers
  decode against the same definitions; no `types/<module>-
  events` per-dApp duplication.

The migration path also unlocks **wayup CO support** (and any
future marketplace's COs) as a parallel community module —
`wayup-co` sits next to `jpg-co` in the same directory and
ships alongside. Workers subscribe to whichever combination
they need.

## Configuration vs code

Configuration sits inside a module, not inside the framework.
Each community module declares its own config shape (TOML →
CBOR at build time, deserialised by the module's `init`).
Examples:

- A new marketplace contract address is config on the relevant
  brand's community module (e.g. add an address to
  `jpg-co.toml`'s watched set, rebuild, redeploy).
- A new fee threshold for "high-value sale" filtering is a
  consumer-side `Interest` filter, or a future
  `ValueFilter::Min { lovelace }` on subscribe.
- A new event shape is a new community module — not an
  extension of an existing one's payload.

## What this means for the platform

Over time:

- **Community modules expand.** Each chain primitive a dApp
  cares about (per-brand marketplace decode, mint detection,
  DEX swap recognition, lending operations, governance,
  ...) ships as a community module. Anyone can subscribe by
  name.
- **In-tree work stays small.** Framework plumbing — chain
  follower, dispatch, claim coordination, the indexer trait
  itself — that's all. Existing in-tree indexers from the
  pre-community-module era stay; new chain-recognition work
  doesn't add to them.
- **Companion DOs do most of the application work.** State
  projection, business workflow, RPC, ui-flow. Where the dApp
  actually lives.
- **`mitos-community-events` is the typed wire contract.** One
  submodule per community module; consumers decode against the
  same definitions across every dApp.

## Open questions

1. **Promotion path: private wasm → community.** A dApp ships
   a private module (`workers/<dapp>/modules/<name>.rs`); a
   second dApp wants the same logic. The mechanical migration
   is in `COMMUNITY_MODULES.md`'s playbook. Outstanding:
   discovery + signalling — how does the second dApp find the
   first's module and ask to share?
2. **Companion subscribing to many community modules.** A
   dApp wanting CO support across jpg.store + wayup +
   dropspot subscribes to three community modules. The
   companion-runtime handles this via multiple `MitosChannel`
   impls. Need a clean idiom for the common pattern.
3. **Shared-utility crate for module authors.** Recurring
   transformation patterns (windowing, deduplication, rate
   limiting, datum-hash matching against TX metadata, …)
   could live in a `mitos-module-helpers` crate. Worth doing
   once the second or third community module wants the same
   helper.
4. ~~**Existing in-tree indexer end-state.**~~ Resolved (2026-05):
   `marketplace-indexer`, `mint-burn-indexer`, and
   `collection-ownership-indexer` all retired in favour of
   community modules — see the status banner at the top of this
   doc. `none-match-indexer` remains as the residual-pass
   coordinator; new in-tree indexers aren't anticipated.

## Cross-references

- `COMMUNITY_MODULES.md` — concrete operationalisation of the
  community-modules layer: directory convention, single
  `mitos-community-events` crate, auto-load on host startup,
  migration playbook for promoting private wasm to community
- `MITOS_COMPANION_PATTERN.md` — the paired-deployable thesis
  (companion DO ↔ wasm module pairing)
- `MITOS_PLATFORM_V2.md` — wasm-module hosting architecture
- `docs/design/UNIFIED_SUBSCRIBE.md` — the bridge that lets
  companions subscribe to either community modules or in-tree
  indexers via the same handshake
- `docs/design/DOMAIN_REFACTOR.md` — event taxonomy
  (Mint / Burn / AssetMovement) shared between in-tree
  indexers and community modules
- `cnft.dev-workers/docs/COLLECTION_OWNERSHIP_MITOS_INTEGRATION.md`
  — applies the layering to the collection-ownership refactor
