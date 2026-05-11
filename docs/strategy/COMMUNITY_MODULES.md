# Community modules

**The default home for chain-recognition logic.** Wasm modules
that ship with mitos, auto-load on host startup, and are
addressable by name from any companion. The canonical example is
`jpg-co` — contract-specific datum decode for jpg.store
collection offers that *any* dApp tracking COs benefits from.

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
│   ├── jpg-co/
│   │   ├── jpg_co.rs          # single-file module source
│   │   ├── jpg_co.toml        # manifest (V2/V3 addresses, deps)
│   │   └── fixtures/          # test fixtures
│   ├── wayup-co/              # future
│   │   └── ...
│   └── ...
└── crates/
    └── mitos-community-events/
        ├── Cargo.toml
        └── src/
            ├── lib.rs         # `pub mod jpg_co; pub mod wayup_co; ...`
            ├── jpg_co.rs      # `pub enum CoChange { Created { ... }, Spent { ... } }`
            ├── wayup_co.rs    # future
            └── ...
```

Note the events crate is **single** with one submodule per
community module — not one crate per module. Keeps shared types
discoverable in one place; saves on Cargo workspace overhead;
each community module's events form a submodule that consumers
can `use mitos_community_events::jpg_co::CoChange;`.

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
A config-driven `[community_modules] enabled = ["jpg-co"]`
allowlist is a future refinement when the catalogue grows.

## Consumer pattern

A worker's companion DO subscribes to community modules by name
exactly as it would to any wasm module:

```rust
// In the dApp's worker:
use mitos_community_events::jpg_co::CoChange;
use mitos_protocol::SubscribeTarget;

impl MitosCompanion for JpgStoreMirror {
    const NAME: &'static str = "jpg-store-mirror";

    fn subscribe_targets(&self) -> Vec<SubscribeTarget> {
        vec![
            // Community module — provided by mitos's auto-load.
            SubscribeTarget::Module { name: "jpg-co".into() },
            // Other targets (in-tree indexers, other community
            // modules) as needed.
        ]
    }

    fn channels(&self) -> Vec<Box<dyn MitosChannelDyn>> {
        vec![Box::new(JpgCoChannel { /* ... */ })]
    }
}

struct JpgCoChannel { /* ... */ }
impl MitosChannel for JpgCoChannel {
    const NAME: &'static str = "jpg-co";
    type Event = CoChange;  // ← from mitos-community-events
    // ...
}
```

The dApp owns **zero chain-decoding code**. The wasm module
(jpg-co) lives in mitos. The event types are in
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

Concrete steps using jpg-co as the worked example. Generalises
to any per-dApp wasm module that earns wider relevance.

### Source moves

1. `cnft.dev-workers/workers/jpg-store-mirror/modules/jpg_co.rs`
   → `mitos/community-modules/jpg-co/jpg_co.rs`
2. `cnft.dev-workers/workers/jpg-store-mirror/modules/jpg_co.toml`
   → `mitos/community-modules/jpg-co/jpg_co.toml`
3. `cnft.dev-workers/workers/jpg-store-mirror/modules/fixtures/`
   → `mitos/community-modules/jpg-co/fixtures/`

### Events crate

1. Create `mitos/crates/mitos-community-events/` with
   `lib.rs` re-exporting one submodule per community module.
2. Move `cnft.dev-workers/types/jpg-co-events/src/lib.rs` →
   `mitos/crates/mitos-community-events/src/jpg_co.rs`.
3. Delete `cnft.dev-workers/types/jpg-co-events/`.

### Build / manifest reference updates

1. `jpg_co.toml` `[deps]` section: previously referenced
   `jpg-co-events = { path = "../../../types/jpg-co-events" }`.
   Now: `jpg-co-events = { path = "../../crates/mitos-community-events" }`
   or — once the crate lives in mitos workspace — simply
   `mitos-community-events = { workspace = true }`.

### cnft.dev-workers updates

1. `workers/jpg-store-mirror/Cargo.toml`: drop
   `jpg-co-events = { path = "../../types/jpg-co-events" }`; add
   `mitos-community-events = { workspace = true }`.
2. `do_state.rs`: change
   `use jpg_co_events::CoChange;` →
   `use mitos_community_events::jpg_co::CoChange;`.
3. Delete `workers/jpg-store-mirror/modules/` entirely.

### Auto-load setup

1. Add the community-module preload pass to `Bundle::run`'s
   startup sequence (before `host.auto_resume()`).
2. Operators upgrading to this mitos version pick up jpg-co
   automatically on next deploy.

### Verification

- After deploy: `mitos-admin list-modules` shows `jpg-co` as
  registered (auto-loaded, not operator-uploaded).
- jpg-store-mirror worker's companion DO subscribes by name
  exactly as today; events flow identically.
- `co-stats` total stable; no regression in observed CO
  population.

## What this means for the existing in-tree indexers

`marketplace-indexer`, `mint-burn-indexer`,
`collection-ownership-indexer`, `none-match-indexer` all
shipped pre-community-modules. They work, they have consumers,
they stay. The community-modules-first preference shapes **new**
chain-recognition work — it doesn't mandate retroactive
demolition.

Concretely:

- **No rip-and-replace.** Workers that subscribe to in-tree
  indexers today (jpg-store-mirror's marketplace channel,
  collections-mitos's ownership channel) keep doing so. The
  unified-subscribe path treats community modules and in-tree
  indexers as peers — companions don't notice the distinction.
- **New brand decode work goes to a community module.** When
  wayup's CO support lands, it's `wayup-co` next to `jpg-co` —
  not a payload extension to `marketplace-indexer`. Likewise
  any new marketplace, DEX, or lending brand.
- **Retirement is a per-indexer call, not a strategy
  decision.** If `marketplace-indexer` becomes maintenance
  dead-weight once `jpg-co` + `wayup-co` + future brand
  modules cover the same ground, it gets retired then — driven
  by concrete pressure (a contract change nobody wants to port
  twice, an indexer payload that's been unused for months),
  not by principle.

The forcing function for any future retirement is the same as
the one that produced this doc: real second consumers reveal
where the duplication actually lives.

## Operational concern: shared community module efficiency

When multiple workers subscribe to the same community module
(e.g. jpg-co serving five different dApps), the host processes
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
  jpg-co = five concurrent WSes per host)
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
3. **Per-module config.** Today `jpg_co.toml` carries V2/V3
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
