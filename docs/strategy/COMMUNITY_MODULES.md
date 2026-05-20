# Community modules

**The default home for chain-recognition logic.** Wasm modules
that ship with mitos, auto-load on host startup, and are
addressable by name from any companion. The canonical example is
`jpg-store-offer` — contract-specific datum decode for jpg.store
collection offers that *any* dApp tracking COs benefits from.
Twelve community modules ship today (`asset-metadata-update`,
`asset-transfer`, `burn-address`, `cip-25-mint`, `cip-68-mint`,
`cswap-dex`, `holder-distribution`, `jpg-store-listing`,
`jpg-store-offer`, `jpg-store-sale`, `splash-dex`,
`standard-burn`, `vesting-tracker`).

Per `LAYERED_RESPONSIBILITIES.md`'s layering:

| Where it lives | When | This doc covers |
|---|---|---|
| `workers/<dapp>/modules/` (private wasm) | One-off dApp logic with no other consumer | Not here |
| **`mitos/community-modules/<name>/` (community wasm)** | **Anything two or more dApps would reasonably want** | **Yes — the default** |
| `mitos/crates/<name>-indexer/` (in-tree) | Framework plumbing the wasm sandbox can't do (chain follower, dispatch, residual pass, …) | Not here. Existing brand-decode indexers shipped pre-community-modules and stay; new chain-recognition work doesn't go here. |

## Goals

- **Composition, not reinvention.** Workers subscribe to
  community modules by name; they don't ship copies of
  chain-decoding logic other workers also need. Existing
  in-tree indexers stay as peers in the same subscribe
  handshake, but they're not where new decode work lands.
- **Shared event types.** One typed wire format per community
  module, owned by the module's source-of-truth. Consumers
  decode against the same definition.
- **Auto-load on host.** Operators don't manually upload
  community modules — mitos preloads from
  `community-modules/` at startup, same effect as
  `mitos-admin upload-module` but operator-free.

## Directory convention

```
mitos/
├── community-modules/
│   ├── jpg-store-offer/
│   │   ├── jpg_store_offer.rs # single-file module source
│   │   ├── jpg_store_offer.toml # manifest (V2/V3 addresses, deps)
│   │   └── fixtures/          # test fixtures
│   ├── jpg-store-listing/
│   │   └── ...
│   ├── asset-transfer/
│   │   └── ...
│   ├── holder-distribution/
│   │   └── ...
│   └── ...                    # 12 modules total
└── crates/
    └── mitos-community-events/
        ├── Cargo.toml
        └── src/
            ├── lib.rs         # `pub mod jpg_store_offer; pub mod asset_transfer; ...`
            ├── jpg_store_offer.rs  # `pub enum JpgStoreOffer { Create, Cancel, Accept, Update }`
            ├── asset_transfer.rs
            ├── holder_distribution.rs
            └── ...
```

Note the events crate is **single** with one submodule per
community module — not one crate per module. Keeps shared types
discoverable in one place; saves on Cargo workspace overhead;
each community module's events form a submodule that consumers
can `use mitos_community_events::jpg_store_offer::JpgStoreOffer;`.

## Auto-load mechanism

On host startup, before `host.auto_resume()`:

1. Walk `community-modules/` for `<name>/<name>.{rs,toml}` pairs.
2. For each, check if the artifact is already present in
   `<modules_dir>/<name>/current.wasm`. If yes + sha matches the
   expected hash from the manifest, skip (already loaded).
3. Otherwise, invoke `mitos-build` to produce the artifact, then
   activate it via the same path `mitos-admin upload-module` uses
   (`ModuleStorage::activate`).
4. After all community modules are activated, `auto_resume`
   proceeds as today — follower tasks start.

**Build artifacts in release distribution:** for production
deploys where building on the host is expensive, mitos releases
include pre-built `.wasm` + manifest pairs for each community
module. The startup auto-load checks for these first; falls
back to building from source only in dev.

**Opt-out:** v1 has none — all modules under
`community-modules/` auto-load. Operators wanting fewer modules
patch their mitos checkout to remove the unused directories.
A config-driven `[community_modules] enabled = ["jpg-store-offer"]`
allowlist is a future refinement when the catalogue grows.

## Consumer pattern

A worker's companion DO subscribes to community modules by name
exactly as it would to any wasm module:

```rust
// In the dApp's worker:
use mitos_community_events::jpg_store_offer::JpgStoreOffer;
use mitos_protocol::SubscribeTarget;

impl MitosCompanion for JpgStoreMirror {
    const NAME: &'static str = "jpg-store-mirror";

    fn subscribe_targets(&self) -> Vec<SubscribeTarget> {
        vec![
            // Community module — provided by mitos's auto-load.
            SubscribeTarget::Module { name: "jpg-store-offer".into() },
            // Other targets (in-tree indexers, other community
            // modules) as needed.
        ]
    }

    fn channels(&self) -> Vec<Box<dyn MitosChannelDyn>> {
        vec![Box::new(JpgStoreOfferChannel { /* ... */ })]
    }
}

struct JpgStoreOfferChannel { /* ... */ }
impl MitosChannel for JpgStoreOfferChannel {
    const NAME: &'static str = "jpg-store-offer";
    type Event = JpgStoreOffer;  // ← from mitos-community-events
    // ...
}
```

The dApp owns **zero chain-decoding code**. The wasm module
(jpg-store-offer) lives in mitos. The event types are in
`mitos-community-events`. The dApp's repo holds only the
projection logic — what the dApp does with the events.

## Cargo dependency from cnft.dev-workers

Same git-ref pinning pattern as other shared mitos crates:

```toml
# cnft.dev-workers/Cargo.toml
[workspace.dependencies]
mitos-community-events = {
    git = "https://github.com/defrag-au/mitos",
    rev = "..." }
```

Or via the local-dev patch override at the bottom of the file:

```toml
[patch."https://github.com/defrag-au/mitos"]
mitos-community-events = { path = "../mitos/crates/mitos-community-events" }
```

## Migration playbook — promoting a private module to community

> **Historical reference.** The `jpg-store-offer` migration below
> shipped — the module now lives at
> `mitos/community-modules/jpg-store-offer/` and the events crate
> at `mitos/crates/mitos-community-events/`. It was originally
> named `jpg-co` in this doc; the rename happened during the move.
> Retained as a worked example for future promotions.

Concrete steps using jpg-store-offer as the worked example. Generalises
to any per-dApp wasm module that earns wider relevance.

### Source moves

1. `cnft.dev-workers/workers/jpg-store-mirror/modules/jpg_store_offer.rs`
   → `mitos/community-modules/jpg-store-offer/jpg_store_offer.rs`
2. `cnft.dev-workers/workers/jpg-store-mirror/modules/jpg_store_offer.toml`
   → `mitos/community-modules/jpg-store-offer/jpg_store_offer.toml`
3. `cnft.dev-workers/workers/jpg-store-mirror/modules/fixtures/`
   → `mitos/community-modules/jpg-store-offer/fixtures/`

### Events crate

1. Create `mitos/crates/mitos-community-events/` with
   `lib.rs` re-exporting one submodule per community module.
2. Move `cnft.dev-workers/types/jpg-store-offer-events/src/lib.rs` →
   `mitos/crates/mitos-community-events/src/jpg_store_offer.rs`.
3. Delete `cnft.dev-workers/types/jpg-store-offer-events/`.

### Build / manifest reference updates

1. `jpg_store_offer.toml` `[deps]` section: previously referenced
   `jpg-store-offer-events = { path = "../../../types/jpg-store-offer-events" }`.
   Now: `jpg-store-offer-events = { path = "../../crates/mitos-community-events" }`
   or — once the crate lives in mitos workspace — simply
   `mitos-community-events = { workspace = true }`.

### cnft.dev-workers updates

1. `workers/jpg-store-mirror/Cargo.toml`: drop
   `jpg-store-offer-events = { path = "../../types/jpg-store-offer-events" }`; add
   `mitos-community-events = { workspace = true }`.
2. `do_state.rs`: change
   `use jpg_store_offer_events::JpgStoreOffer;` →
   `use mitos_community_events::jpg_store_offer::JpgStoreOffer;`.
3. Delete `workers/jpg-store-mirror/modules/` entirely.

### Auto-load setup

1. Add the community-module preload pass to `Bundle::run`'s
   startup sequence (before `host.auto_resume()`).
2. Operators upgrading to this mitos version pick up jpg-store-offer
   automatically on next deploy.

### Verification

- After deploy: `mitos-admin list-modules` shows `jpg-store-offer` as
  registered (auto-loaded, not operator-uploaded).
- jpg-store-mirror worker's companion DO subscribes by name
  exactly as today; events flow identically.
- `co-stats` total stable; no regression in observed CO
  population.

## What this means for the existing in-tree indexers

Three of the four legacy in-tree indexers
(`collection-ownership-indexer`, `marketplace-indexer`,
`mint-burn-indexer`) **retired in 2026-05** once their consumers
cut over to platform-v2 community modules. The
community-modules-first preference shaped new chain-recognition
work for a year + then drove the retirement of the in-tree path
once parity was reached.

Only `none-match-indexer` remains — it stays as the dispatcher's
residual-pass coordinator (emits `Domain::AssetMovement` for
asset transfers no specific-domain emitter claimed; required by
the synchronised dispatcher, not legacy).

The retirement playbook each indexer followed:

1. Stand up a community module covering the chain-recognition
   surface (e.g. `cip-25-mint` for mint events).
2. Wait for the consumer worker(s) to migrate subscriptions.
3. Verify zero remaining replicator subscribers via
   `/_admin/subscriptions` (now removed).
4. Drop the indexer crate + bundle wiring in one PR.

New brand decode work continues to go to community modules
(e.g. a hypothetical wayup CO module would land as
`wayup-store-offer` next to `jpg-store-offer`), and `Replicator`
itself retired alongside
its last consumer.

The forcing function for any future retirement is the same as
the one that produced this doc: real second consumers reveal
where the duplication actually lives.

## Operational concern: shared community module efficiency

When multiple workers subscribe to the same community module
(e.g. jpg-store-offer serving five different dApps), the host processes
events once per chain block, fans events out to N subscribers.
Per-event compute is bounded; the broadcast channel handles
fan-out without re-running the wasm classification logic per
subscriber.

The wasm module itself runs **once per host**, regardless of
subscriber count. The expensive work (decode jpg.store CO
datum, hash-match metadata) happens once; outputs flow to all
subscribers via the broadcast channel.

That's the theory. **In practice**, watch:

- Broadcast channel buffer size when N subscribers consume at
  different rates (slow subscribers shouldn't slow the wasm
  module's input stream)
- WS connection count per host (one outbound dial per
  subscriber per target — five dApps each subscribing to
  jpg-store-offer = five concurrent WSes per host)
- The wasm module's per-tx cost itself if it does expensive
  state-kv lookups

Optimisation pass when actually needed; the model's correct
without it. **Worth surfacing as a future profile-and-tune
task; not blocking initial adoption.**

## Open questions

1. **Versioning.** Community modules evolve; consumers depend on
   specific event shapes. The events crate gives semver-style
   guarantees. The wasm module's behaviour can change without
   breaking events; ABI version on the manifest handles
   hard-incompatible changes.
2. **Where do third-party-contributed community modules land?**
   v1: same repo (PR to mitos). Future: external registry +
   signing + per-tier promotion path. Out of scope until a
   non-defrag-au community module exists.
3. **Per-module config.** Today `jpg_store_offer.toml` carries V2/V3
   addresses. If those addresses are operator-specific (per
   testnet vs mainnet), config-by-environment becomes a need.
   Not urgent.
4. **Module discovery + introspection.** Operators run
   `mitos-admin list-modules`; that's tier-2 visibility. Future:
   surface module-level metadata (events emitted, contracts
   watched, intended consumers) so dApp authors can discover
   what's available.

## Cross-references

- `LAYERED_RESPONSIBILITIES.md` — the layering this doc
  operationalises; sets the community-modules-first preference
  for chain-recognition work
- `MODULE_COMPOSITION.md` — future module-to-module dependency
  declarations (orthogonal but composes well: community
  modules upstream, dApp-specific modules downstream)
- `MITOS_PLATFORM_V2.md` — wasm hosting architecture
- `cnft.dev-workers/docs/JPG_STORE_MIRROR_RELAYERING.md` —
  in-progress migration that this doc reframes (the wasm module
  doesn't get retired; it gets *promoted* into
  `community-modules/`)
