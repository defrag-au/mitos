# Watched-UTxO index (live interest set)

Status: **implemented, shadow-ready — not yet deployed** (2026-06-05).
Builds clean (`cargo check -p mitos`), 120 platform lib tests green,
clippy clean. Ships defaulting to `MITOS_FALLBACK_GATE=off` (pre-index
behaviour); operator enables `shadow` then `on`.
Owner: TBD
Related: `crates/mitos-platform/src/maestro_fallback_plane.rs`,
`crates/mitos-data-plane/src/dispatch.rs`,
`crates/mitos-platform/src/bootstrap_v2.rs`,
`infra/docs/mitos-operations.md`

## Summary

The core primitive introduced here is a **per-module persisted
index of the live UTxOs that match a module's interest** — recorded
on the way in (when produced), dropped on the way out (when
consumed), and seeded at bootstrap from the live UTxO set. mitos
already computes "does this output match interest" for every
`Produced` event; the index simply *remembers the answer* so it
doesn't have to be re-derived later.

The first consumer of the index — and the motivation for building
it now — is **scoping the Maestro prior-output fallback** (below).
But the index is a general primitive: any future feature that needs
"is this old UTxO one we care about?" without a round-trip
(replication scoping, selective archival, etc.) can read it.

## Problem (first consumer: the Maestro fallback)

The follower's per-block dispatch resolves the prior output of
**every input + reference-input of every TX in the block**
(`dispatch.rs:53-64`) *before* the interest filter runs — it has
to, because deciding whether a `Consumed` event matches an
address/policy predicate requires the consumed output's address
and assets, which is exactly what resolution produces.

For any prior output older than dolos's 7-day archive horizon
(`dolos.toml`: `max_history = 604800` slots = 7 days), the local
plane misses and `MaestroFallbackPlane` calls Maestro's
`GET /transactions/{tx}/outputs/{idx}/txo`. Because resolution is
**interest-blind**, this fires for every chain-wide spend of a
>7-day-old UTxO — almost all of which match no module's interest
and are discarded immediately after resolution.

### Measured on mainnet (2026-06-05, at tip, 16 modules, 10-day uptime)

| Signal | Value |
|---|---|
| `prior output resolved via fallback provider` | **1,209 / hour ≈ 29K/day** |
| `fallback provider: utxo not found` / `lookup failed` | **0 / 0** |
| duplicate `(tx,idx)` resolutions within an hour | **0** (cache + cross-module dedup already perfect) |
| emitted module events (all 16 modules, 12h) | **~250** |
| sampled prior-output ages | all >7d (pruned from dolos) |

The cache is doing its job: 0 duplicates, and it collapses the
16× module fan-out (without it ≈ 460K/day). The residual ~29K/day
is **irreducibly-unique one-time consumed-input lookups** — a UTxO
is spent once, so there is no cross-block reuse for the cache to
capture. The ~14.5K-resolutions-to-~250-emits ratio over 12h shows
the overwhelming majority of these resolutions are wasted.

**Conclusion: the lever is not the cache. It is to stop resolving
prior outputs that cannot match any interest set.**

## Core insight: matching is symmetric

`event_matches` (`dispatch.rs:263`) uses the *same* `matches_output`
for `Produced` (`&e.output`) and `Consumed` (`&e.prior_output`).
A UTxO's content (address, assets) is immutable between creation
and spend. Therefore:

> If a consumed UTxO matches an interest predicate now, the
> identical UTxO matched the identical predicate **when it was
> produced** — and mitos saw that `Produced` event (or enumerated
> it at bootstrap).

So mitos never needs Maestro to *discover* whether an old consumed
output is interesting. It already knew when the output was created.
It only needs to **remember** the set of live UTxOs that match
interest, and gate the Maestro fallback on membership.

Only `Consumed` and `Referenced` events need prior-output
resolution. `Minted` comes from `tx.mints`, `Produced` from
`tx.outputs`, `TxContext` needs nothing — so the mint/burn modules
(`cip-25-mint`, `cip-68-mint`, `standard-burn`, `asset-metadata-update`)
should contribute **zero** fallback load.

## Design

### 1. `WatchedRefIndex` (new, `mitos-platform`)

A per-module, persisted set of `OutputRef`s = "live UTxOs that
match this module's interest."

```rust
/// Persistent set of live output refs matching a module's interest.
/// Backed by a redb table in the module's existing `kv.redb`.
/// Key = tx_hash(32) ++ index.to_be_bytes()(4) = 36 bytes; value = ().
pub struct WatchedRefIndex {
    db: Arc<redb::Database>,            // module's kv.redb
    mem: RwLock<HashSet<OutputRef>>,    // hot in-memory mirror for contains()
}

impl WatchedRefIndex {
    pub fn open(db: Arc<redb::Database>) -> Result<Self>;   // loads mem from table
    pub fn contains(&self, oref: &OutputRef) -> bool;       // hot path, mem only
    pub fn apply(&self, insert: &[OutputRef], remove: &[OutputRef]) -> Result<()>; // 1 redb txn + mem update
    pub fn seed(&self, refs: impl IntoIterator<Item = OutputRef>) -> Result<()>;   // bootstrap
    pub fn len(&self) -> usize;
}
```

Bounded by the count of live watched UTxOs (thousands), not the
chain. `contains()` never touches disk.

### 2. Gate the fallback (`maestro_fallback_plane.rs`)

`MaestroFallbackPlane` gains `watched: Option<Arc<WatchedRefIndex>>`
plus a `shadow: bool` (rollout, below). In `read_utxos`, the only
change is the gap loop:

```rust
for oref in orefs {
    if found.contains(&(oref.tx_hash, oref.index)) { continue; }

    let in_scope = self.watched.as_ref().map_or(true, |w| w.contains(oref));

    if !in_scope {
        if self.shadow {
            metrics::counter!("mitos_fallback_would_skip").increment(1);
            // fall through and resolve anyway (shadow = observe only)
        } else {
            continue; // GATED: provably non-matching, skip Maestro
        }
    } else if self.shadow {
        metrics::counter!("mitos_fallback_would_resolve").increment(1);
    }

    if let Some(typed) = fetch_dedup(provider.as_ref(), oref, decode).await {
        // SHADOW SAFETY NET: if a "would_skip" ref actually matches
        // interest, the index is incomplete — a correctness bug.
        if self.shadow && !in_scope && self.interest.matches_output(&typed) {
            tracing::error!(%oref, "INDEX GAP: skipped ref matches interest");
            metrics::counter!("mitos_fallback_index_gap").increment(1);
        }
        out.push((*oref, typed));
    }
}
```

`watched = None` is the current fail-open behaviour (unchanged), so
the wrapper is safe before the index is populated.

### 3. Populate the index (`driver_v2.rs::apply_block`)

After `build_event_batches` returns, walk the matched batches and
diff the index (the interest is already in hand at line 210):

```rust
let mut insert = Vec::new();
let mut remove = Vec::new();
for batch in &batches {
    for e in &batch.events {
        match e {
            UtxoEvent::Produced(p) if interest.matches_output(&p.output) => insert.push(p.oref),
            UtxoEvent::Consumed(c) => remove.push(c.oref),
            _ => {}
        }
    }
}
self.watched.apply(&insert, &remove)?;   // before dispatch_batch loop
```

Every matching produced output is in a matched batch (a produced
match makes the TX relevant), and every consumed watched ref is in
a matched batch (a consumed match — or another event — makes the TX
relevant). Same-block produce→consume needs no Maestro (the local
archive serves same-block outputs), so ordering within the block is
irrelevant to correctness.

### 4. Seed the index (`bootstrap_v2.rs` + recapture pump)

`scan_one_address` / `scan_one_payment_cred` / `scan_one_policy`
already enumerate the live UTxO set matching each predicate
(via `utxos_by_address`, `utxos_by_payment_cred`,
`search_utxos(holds_policy)`). Insert every enumerated ref into the
index during the scan — that makes the index complete for all
pre-existing UTxOs the moment the feature is enabled, and refills it
whenever a new predicate is added (the recapture/rebootstrap pump,
`follower_v2.rs:355`, runs the same scans).

## Correctness

The index always contains every live UTxO matching the module's
interest, because: (1) bootstrap/recapture seeds all pre-existing
matches; (2) `apply_block` inserts every new matching produced
output; (3) `apply_block` removes every consumed ref. Gating Maestro
on `contains()` therefore never skips a watched UTxO. Skipped refs
are provably non-matching by the symmetry argument, so output is
**identical** to today. The shadow-mode safety net (`index_gap`
counter) empirically proves this before the gate is flipped.

## Rollout (de-risked, also measures exact win)

1. **Ship index + population + seeding + plane in `shadow: true`.**
   Resolves everything as today (zero behaviour change) but emits
   `would_skip` / `would_resolve` / `index_gap` counters. Run
   several days across a full archive-horizon window.
2. **Verify:** `index_gap == 0` and `would_skip` ≈ 90%+ of calls.
   This is the precise win measurement.
3. **Flip `shadow: false`.** Gate active; Maestro drops to the
   `would_resolve` rate (expected: low hundreds/day).

Wire `shadow` to an env var (`MITOS_FALLBACK_GATE=shadow|on|off`)
so the flip is a restart, not a redeploy.

## Files touched

- `crates/mitos-platform/src/watched_ref_index.rs` — new
- `crates/mitos-platform/src/maestro_fallback_plane.rs` — gate + shadow + carry `InterestSet`
- `crates/mitos-platform/src/driver_v2.rs` — populate in `apply_block`
- `crates/mitos-platform/src/bootstrap_v2.rs` — seed in the three scans
- `crates/mitos-platform/src/host_v2.rs:593` — construct index per module, pass to plane
- `crates/mitos-platform/src/follower_v2.rs` — seed on recapture/interest-add

## Tests

- Unit: `WatchedRefIndex` insert/remove/contains/seed round-trips through redb.
- Dispatch: produce-at-watched-addr inserts; consume removes (extend `dispatch.rs` tests).
- Gate: gap ref NOT in index → no `fetch_output` call (stub provider asserts 0 calls);
  gap ref IN index → exactly 1 call.
- Regression: the jpg.store archive-horizon Cancel case (the reason the fallback
  exists) — seed the offer ref, confirm the >7d consume still resolves + emits.
- Shadow safety net: a watched ref deliberately omitted from the index trips `index_gap`.

## Non-goals / follow-ups

- Negative caching in `fetch_dedup` (cache `Ok(None)`): 0 impact today (no misses);
  do it for hygiene separately.
- Widening dolos `max_history`: blocked by disk (292GB archive, 680/1000GB used).
- Pruning index entries when a predicate is unsubscribed: stale entries only cause
  wasted (not incorrect) resolution; lazy cleanup is fine.
