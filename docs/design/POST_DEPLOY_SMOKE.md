# Post-deploy smoke verification (functional, against live state)

**Status: spec / future sprint.** Drafted 2026-05-23 alongside the
CO3 fix (CIP-68 hash-only datum resolution) and its offline golden
(`community-modules/collection-metadata/tests/fixtures/cip68-hash-only-datum`).
This doc covers the *other half* of that test story: a new class of
checks that run against the **deployed** mitos host + consuming
worker, catching the failure modes an offline golden fundamentally
cannot.

## Why — the gap offline goldens can't close

`scripts/run-golden-tests.sh` is deterministic, offline, and
fixture-driven. It proves module logic and host wiring. It is
blind, by construction, to everything that only exists at runtime:

- **dolos state** — `DATUM_NS` snapshot gaps, archive horizon
  pruning. A datum that resolves in a fixture may be absent on the
  live node.
- **Maestro config** — key present, budget, rate limits, the
  fallback tiers actually reachable.
- **the cross-process pipeline** — mitos emit → dialer POST →
  worker `/_internal/apply-*` → reconcile → trait bitmaps. Every
  hop is outside the golden harness.

**CO3 is the canonical example.** Every golden passed, yet
Nikeverse showed `traits = 0` on the live box because the hash-only
ref datums weren't in the live `DATUM_NS`. The bug lived in the
*seam* between offline-correct code and live state. We've since
added an offline golden (A) that guards the hash-only *wiring* — but
the live-state class still has no automated check. That's what this
sprint adds.

## The decisive pattern: recapture-then-assert

A recapture (`POST /_admin/modules/{id}/recapture {"companion":"*"}`)
is:

- **fast** — ~3.6s to re-derive ~7k entries across companions
  (bulk-apply path),
- **pure-mitos** — the re-derivation runs entirely through the
  module → dialer → worker pipeline, with no legacy `cnft_tools`
  sync in the loop, and
- **idempotent** — projected state is re-derivable; safe to repeat
  on dev.

So the check is:

> **recapture → poll worker `/api/trait-schema/{policy}` → assert
> non-empty + known traits present.**

This is an unconfounded, end-to-end proof that the live pipeline
works on real dolos state. **Recapture-first is the load-bearing
detail:** a bare "trait-schema is non-empty" could be green only
because the legacy `cnft_tools` sync filled it, masking a broken
mitos pipeline. Recapture wipes and re-derives purely from mitos in
a few seconds; asserting immediately after attributes the result to
the pipeline under test. (This is exactly how CO3 was verified by
hand — recapture, then confirm the schema held with real CIP-68
trait keys.)

## Reference-collection manifest

A small, checked-in manifest of canonical collections and their
invariants drives the suite:

```toml
[[collection]]
policy   = "de79250af8caffc7a64645d86939159f665d4107c3f198562007bf32"  # Nikeverse
label    = "Nikeverse"
module   = "collection-metadata"
min_assets              = 5000
trait_schema_min_bitmap = 40
must_have_traits        = ["head:Satoshi", "body:Midnike"]
```

Pick collections that are **mitos-only** (legacy `cnft_tools` sync
disabled) where possible; otherwise rely on recapture-first. Grow
the manifest over time: a CIP-25 reference once that path is
covered, and a `collection-holders` reference asserting
`holder_count` / distribution invariants.

## Shape and where it lives

- **Start: `scripts/post-deploy-smoke.sh`** (mitos repo). Reads the
  manifest; for each collection: optionally POST recapture, poll
  the worker API, assert invariants. Exit non-zero on any miss.
  Mirrors `run-golden-tests.sh` ergonomics (per-line ✓/✗, summary,
  `--no-recapture` for read-only mode).
- **Graduate: a `mitos-admin verify-collections` subcommand** — the
  "wrangler for mitos" framing (see
  [`MITOS_OBSERVABILITY_API.md`](./MITOS_OBSERVABILITY_API.md)).
  Could chain off `deploy.sh`'s verify step, but **opt-in only**,
  since recapture mutates.
- **Auth.** The recapture POST reuses `MITOS_AUTH_TOKEN` from the
  env (per the observability spec — token-in-env, no SSH). The
  worker assertion endpoints (`/api/trait-schema`, `/api/stats`)
  are public reads.

## Assertions (LLM-decodable, decision-oriented)

- **trait-schema** — `bitmap_size >= N`, `schema_version > 0`,
  `must_have_traits ⊆ traits.keys()`.
- **stats** — `asset_count` within `[min, max]`.
- **(later) holders** — `holder_count > 0`; distribution buckets
  sum to supply.
- **(later) recapture metrics** — `events_emitted >= min`,
  `duration_ms` under budget, `0` traps (once
  `/_admin/status` / `/_admin/events` from the observability spec
  expose them).

Keep responses bounded and field-oriented (counts, booleans, set
membership) so an agent can decide pass/fail without parsing prose.

## Confounds and caveats

- **Legacy `cnft_tools` sync** can mask a broken pipeline — mitigate
  with mitos-only reference collections or always recapture-first.
- **Recapture mutates** (wipes + re-derives). This is a deploy-time
  / on-demand check, **not** a read-only health probe — keep it out
  of any always-on health path, and run it against dev (or a
  known-safe reference set) only.
- **Maestro budget** — the first recapture after a cold
  `indexer_data.redb` may fetch many datums from Maestro; subsequent
  runs are served from the persistent cache. Be budget-aware when
  choosing how many reference collections to recapture.

## Relationship to other docs

- **Offline counterpart** — `scripts/run-golden-tests.sh` + the
  `cip68-hash-only-datum` golden (deterministic logic/wiring).
- **Observability** —
  [`MITOS_OBSERVABILITY_API.md`](./MITOS_OBSERVABILITY_API.md)
  (status / tail / events). This suite is the *functional
  verification* layer that sits on top of those status endpoints.

## Phasing

1. `scripts/post-deploy-smoke.sh` + manifest with Nikeverse
   (recapture-then-assert on `trait-schema`).
2. Add `collection-holders` + CIP-25 reference collections.
3. `mitos-admin verify-collections`; optional opt-in `deploy.sh`
   hook.
4. Assert on recapture metrics once `/_admin/status` /
   `/_admin/events` land.

## Non-goals

- **Replacing goldens** — offline logic correctness stays in the
  golden suite; this asserts live coverage + invariants, not
  per-asset metadata exactness.
- **A continuous monitor** — this is deploy-time / on-demand and it
  mutates; uptime/liveness belongs to `/health` + the observability
  status endpoints.
