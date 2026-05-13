# Holder Distribution Community Module

## Goal

A mitos community module that exposes the **current set of
holders for any tracked policy** as a typed event stream. One
subscription per policy gives the consumer:

1. A `HolderSnapshot` event on registration — the full
   per-stake-credential balance ledger for that policy at the
   subscription's starting cursor.
2. Subsequent `HolderDelta` events as on-chain transfers move
   tokens between wallets.

This retires the Maestro-pagination cron pattern that downstream
consumers (notably `cnft.dev-workers`'s holder-map worker) use to
maintain per-token holder maps today. Cold-start moves from
"paginate Maestro N pages, classify, store" (minutes for active
policies, billed per call) to "one dolos-indexed lookup + replay"
(sub-second).

Same per-brand-per-domain principle as `<brand>-dex` modules:
chain recognition + state derivation lives in the platform, projections
live in consumer workers. Consumers stay thin.

## Why this is a mitos module rather than a consumer's responsibility

Consumers wanting per-policy holders could in principle:
- Call Maestro pagination on cold-start (current pattern; slow, billed)
- Open a `holds_policy(X)` interest on a stub module, accept the
  bootstrap dispatch stream, build the ledger themselves
- Use a hypothetical `utxos_by_policy` host-fn from a custom
  module + maintain their own state

The dedicated module is preferable because:

1. **One place for classification-adjacent edge cases.** Splitting
   a UTxO's value into (stake_cred → quantity) is non-trivial when
   a single UTxO holds multiple distinct asset names under one
   policy (NFT collections), or when a wallet's funds are spread
   across many UTxOs. Putting this in the module means every
   consumer benefits from the same correct accounting.
2. **Per-policy state lives in mitos, not duplicated across N
   consumers.** Three consumer workers each maintaining their own
   holder ledger for Aliens means three sources of truth that can
   drift. One mitos module owns the canonical ledger; consumers
   project from it.
3. **Cold-start scan is amortised across consumers.** Two
   consumers tracking the same policy share the snapshot work.
4. **Symmetric with `<brand>-dex` pattern.** Easier to reason
   about: every per-domain mitos module owns its derived state
   and exposes it via typed events.

## Architecture

```
  ┌─────────────────────────────────────────────────────────┐
  │  holder-distribution wasm module                         │
  │                                                          │
  │  Interest (dynamic):                                     │
  │    [holds_policy(X), holds_policy(Y), …]                 │
  │    ← registered per-policy via update_interest           │
  │      from consumer subscriptions                         │
  │                                                          │
  │  Per-policy state (kv-state, keyed by policy_id):        │
  │    HolderLedger {                                        │
  │      ledger: BTreeMap<StakeCred, AssetBalances>,         │
  │      bootstrap_cursor: Option<ChainPoint>,               │
  │    }                                                     │
  │                                                          │
  │  init():                                                 │
  │    - No upfront work; per-policy hydration happens       │
  │      lazily on first interest registration.              │
  │                                                          │
  │  update_interest(Add, holds_policy(X)):                  │
  │    - Call chain_data::utxos_by_policy(X)                 │
  │      ← NEW HOST-FN; see "Mitos host-fn additions" below  │
  │    - Read typed outputs via read_utxos()                 │
  │    - Build initial ledger per stake_cred                 │
  │    - Persist HolderLedger to kv-state                    │
  │    - Emit HolderSnapshot { policy, ledger, cursor }      │
  │                                                          │
  │  handle_events(events):                                  │
  │    For each Produced/Consumed event matching one of the  │
  │    registered policies:                                  │
  │      - Update affected stake_cred balances               │
  │      - Persist updated ledger                            │
  │      - Emit HolderDelta { policy, changes, cursor }      │
  │                                                          │
  │  Emission channel:                                       │
  │    0  →  HolderEvent (Snapshot | Delta)                  │
  └─────────────────────────────────────────────────────────┘
```

## Event surface

`mitos_community_events::holder_distribution`:

```rust
pub struct AssetBalance {
    /// Lowercase hex asset name. Empty for the policy's
    /// dominant asset (in fungible-token convention) or
    /// distinct per-NFT for collection policies.
    pub asset_name_hex: String,
    pub quantity: u64,
}

pub struct HolderEntry {
    /// 28-byte stake credential. None for enterprise (no-stake)
    /// addresses — those holders are aggregated under a single
    /// synthetic "enterprise" entry per policy so they're not
    /// lost from the ledger.
    pub stake_cred_hex: Option<String>,
    /// Per-asset-name balances under this policy held by this
    /// stake credential. For pure fungible tokens this is
    /// length-1.
    pub assets: Vec<AssetBalance>,
}

pub struct HolderSnapshot {
    /// 56-char hex policy id this snapshot is for.
    pub policy: String,
    /// Chain point at which this snapshot is valid.
    pub cursor_slot: u64,
    pub cursor_hash_hex: String,
    /// All holders at the cursor. Sorted by total quantity
    /// descending for deterministic test output and to make
    /// top-N consumer queries cheap to slice.
    pub holders: Vec<HolderEntry>,
}

pub struct HolderDelta {
    pub policy: String,
    pub tx_hash: String,
    pub slot: u64,
    /// Stake credentials whose balances changed in this TX,
    /// with their new balance state. A holder whose balance
    /// drops to zero appears here with `assets: []` (consumer
    /// removes the entry).
    pub changed: Vec<HolderEntry>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HolderEvent {
    Snapshot(HolderSnapshot),
    Delta(HolderDelta),
}
```

**Why `HolderEntry::stake_cred_hex` and not full bech32**: the
module operates on cred-bytes from `TypedOutput`'s payment+stake
parsing without needing to construct bech32 (avoids pulling a
bech32 encoder into wasm). Consumers convert to bech32 on the
projection side using their own brand of the formatter.

**Why per-asset balances** (`Vec<AssetBalance>` per holder
rather than `quantity: u64`): a policy can house multiple
asset names. For NFT collections this matters (every NFT is a
distinct asset name); for fungibles the vec is length-1. Single
schema covers both.

**Enterprise addresses** (key payment, no stake): aggregated
under a `stake_cred_hex: None` entry rather than dropped.
Consumer-side classification can show them as "loose" if needed.

## Mitos host-fn additions

The module needs one new function in mitos's `chain-data` WIT
interface:

```wit
/// Enumerate the current unspent set holding a given policy id.
/// Sub-second even for very active policies (dolos has a
/// first-class `BY_POLICY` index — see dolos's `crates/redb3/
/// src/indexes/mod.rs` and `crates/fjall/src/index/state_tags.rs`).
utxos-by-policy: func(policy: list<u8>) -> list<output-ref>;
```

Implementation: one-line pass-through to dolos's existing
`utxos_by_policy()` query (`dolos/crates/cardano/src/indexes/
ext.rs:43-45`). Same shape as the existing `utxos_by_address`
host-fn that ships in v2 today.

This is independently useful — any future module needing
per-policy cold-start (vesting trackers, NFT-collection
indexers, etc.) gets it for free.

## Algorithm

### Cold-start (per policy)

1. Consumer registers interest in policy `X` (via mitos
   replication WS → `update_interest(Add, holds_policy(X))`)
2. Module callback fires:
   - `refs = chain_data::utxos_by_policy(X)` — one host call
   - `outputs = chain_data::read_utxos(refs)` — batch resolve
   - For each output:
     - Parse address → `(payment_cred, stake_cred)`
     - For each asset entry with `policy == X`:
       - Add `(stake_cred, asset_name) → quantity` to ledger
   - Persist `HolderLedger` to kv-state keyed by policy hex
3. Emit `HolderSnapshot { policy: X, holders: …, cursor: now }`

### Live updates

The platform delivers `Produced` and `Consumed` events for any
TX touching the policy (because `holds_policy(X)` is in the
module's interest). For each event:

- **Consumed**: subtract the prior output's policy-X assets
  from the consumed wallet's stake_cred balance
- **Produced**: add the new output's policy-X assets to the
  produced wallet's stake_cred balance
- Aggregate per-TX changes (a TX can touch many wallets)
- Persist updated ledger
- Emit `HolderDelta { policy: X, changed: …, tx: …, slot: … }`

### Rollback

The platform delivers `Rollback` markers. Module's options:

(a) Maintain a small change-log per policy + per-slot, replay
    in reverse on rollback
(b) On rollback, re-run cold-start (call `utxos_by_policy`
    again) and emit a fresh snapshot

Lean (b) for simplicity. Rollbacks are rare; the cold-start
operation is fast enough that re-emitting the snapshot is
acceptable. Consumers treat a `HolderSnapshot` as
"authoritative replacement of prior state".

## Interest model

`holder-distribution` is unusual among community modules in
that its static `[interest]` in `holder_distribution.toml` is
*empty*. All interest is dynamic — consumers register the
policies they care about via the standard `update_interest`
mechanism. The module starts with no policies tracked; the
first consumer's subscription is what triggers its first
cold-start scan.

This is intentional: tracking every policy on mainnet (~2M+)
isn't feasible, and most policies have no DEX-shaped use case.
Per-consumer dynamic registration keeps state scoped to what's
actually being consumed.

**Caveat**: dynamic interest is per-module-installation, not
per-subscription. If two consumers both want policy X tracked,
the module sees one `holds_policy(X)` interest entry (refcounted
on the platform side). When the last consumer drops, the
platform removes the interest and the module can drop the
policy's ledger from kv-state — but only if it knows to.
**Open question** (see below): how does the module learn that a
policy's last consumer has disconnected? For phase 1 we don't
GC dropped policies; ledger state stays even after consumer
disconnect (extra storage, not correctness).

## State management

Per-policy kv-state keyed by `policy_hex`:

```
holder_ledger:<policy_hex>  →  CBOR-encoded HolderLedger {
                                  ledger: BTreeMap<StakeCred, AssetBalances>,
                                  cursor: ChainPoint,
                              }
```

**Size estimate**: for an active policy with ~10K distinct
holders × average 32 bytes per (stake_cred, asset_name,
quantity) tuple = ~320KB per policy. Mitos's kv-state backend
(redb in current platform v2) handles this comfortably. For a
worker watching 100 policies, total module state is on the
order of 30MB — well within budget.

**Per-event cost**: each `handle_events` call updates a small
slice of the ledger (only the stake_creds involved in the
TX). Read + mutate + write is single-digit-millisecond. Hot
TXs (large batcher fills affecting 20+ wallets) are still
sub-100ms.

## Edge cases

1. **Multi-asset-name policies** (NFT collections like Aliens
   where each unit is a distinct asset name under one policy).
   The `assets: Vec<AssetBalance>` shape handles this. Most
   per-token-per-holder rows will have length-1 assets;
   collection holders will have N. Consumers slice per
   asset_name if they need NFT-grain detail.

2. **Smart-contract held tokens** (LP pools, vesting contracts,
   farms). These show up as holders with the *script's* stake
   credential (or `None` for enterprise scripts). Consumers
   classify them via per-project address maps (the same way
   holder-map does today — burn addresses, LP staking sources,
   etc.). Classification stays in the consumer; the module
   emits raw balances.

3. **Brand-new policy (no current holders)**. Cold-start
   `utxos_by_policy(X)` returns an empty set. Module emits a
   `HolderSnapshot` with `holders: []` and waits for the first
   mint event to populate via the live path.

4. **Retired / fully-burnt policy**. Same as above — empty
   ledger, no further deltas until something changes. Module
   doesn't distinguish "never existed" from "burnt out".

5. **Massive mint TX** (one TX produces 10K outputs at the
   mint). The module sees 10K `Produced` events in one
   `handle_events` batch, updates 10K stake_cred entries,
   emits one `HolderDelta` with all changes. Atomic per TX.

## Phased delivery

**Phase 1: Module ships with cold-start + delta emission**

- `utxos_by_policy` host-fn added to mitos chain-data WIT
- `holder-distribution` module: dynamic interest, cold-start
  scan, live deltas, kv-state persistence
- Snapshot + Delta event types in `mitos_community_events`
- Golden fixtures: cold-start for a known policy + a few
  live transfer TXs validating deltas line up against
  Maestro-derived ground truth

**Phase 2: Consumer integration**

- Holder-map worker subscribes via `MITOS_SUBSCRIBER` DO
- Parallel-run against the legacy Maestro-cron path; verify
  ledger equivalence
- Retire Maestro holder cron once parallel-run is clean

**Phase 3: Operational hardening**

- Per-policy state GC on consumer disconnect (see Interest
  model open question)
- Rollback handling: snapshot re-emission on rollback markers
- Metrics: kv-state size per policy, cold-start latency, delta
  emission rate

## Open questions

1. **GC on dynamic-interest drop**. When the last consumer of
   policy X disconnects, should the module remove its ledger
   from kv-state? Phase 1 says no (keep it; extra storage is
   cheap). Phase 3 might add a TTL or explicit "drop policy"
   admin endpoint.

2. **Single-event delta granularity**. Do we emit one
   `HolderDelta` per TX, or batch a slot's worth of deltas
   into one event? Phase 1 says per-TX — simpler downstream
   consumption. Revisit if event volume becomes an issue.

3. **Snapshot re-emission cadence**. Should the module
   periodically (every N hours) re-emit a snapshot for active
   policies as a checkpoint, so consumers can resync without
   replaying all deltas from origin? Lean yes, with a tunable
   interval (default daily).

4. **Multi-asset reduction**. For policies where consumers
   only care about total under-policy quantity (most fungible
   tokens), the per-asset vec is overkill. Should we surface
   a separate `HolderSummary` view with `total_quantity: u64`
   per holder for these cases? Or trust consumers to sum the
   vec? Lean trust consumers; one type of event keeps the wire
   simple.

5. **Cross-policy correlation**. A single TX often touches
   multiple tracked policies (e.g., a DEX swap involves the
   ADA-policy and the token-policy). Currently each policy
   gets its own `HolderDelta`; do we want a combined
   `MultiPolicyDelta` so consumers see all related changes at
   once? Phase 1 says no — per-policy is cleaner per the
   per-domain modularity principle. Cross-policy correlation
   is a consumer-side concern.

## Non-goals

- **Wallet classification** (Wallet / LP / Burn / Vesting).
  Stays on the consumer side; the module emits raw
  `(stake_cred → balance)` pairs. Per-project classification
  config doesn't belong in a chain-recognition module.
- **Per-asset metadata** (display name, decimals, image, etc.).
  That's metadata, not chain state — separate concern.
- **Historical holder lists** (e.g., "who held this at slot
  N"). The module always emits current state + forward deltas.
  Historical reconstruction requires consumer-side persistence
  of delta history.
- **ADA holder tracking**. The module is built for native
  assets. ADA balances are computable from any consumer with
  UTxO visibility; not in scope here.

## References

- Dolos policy-keyed index (sub-second `utxos_by_policy`):
  `~/code/github/dolos/crates/redb3/src/indexes/mod.rs:86-87`
  `~/code/github/dolos/crates/fjall/src/index/state_tags.rs`
  `~/code/github/dolos/crates/cardano/src/indexes/ext.rs:43-45`
- Mitos v2 chain-data WIT interface:
  `~/code/defrag/mitos/crates/mitos-platform/wit-v2/world.wit`
- Mitos community events crate (where the new submodule lands):
  `~/code/defrag/mitos/crates/mitos-community-events/src/`
- Consumer-side integration:
  `~/code/defrag/cnft.dev-workers/docs/design/HOLDER_MAP_MITOS_INTEGRATION.md`
- DEX modules (sibling per-brand example):
  `~/code/defrag/mitos/docs/design/DEX_COMMUNITY_MODULES.md`
