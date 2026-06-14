# Metadata Obliteration Defense — holder-protective metadata pinning

> **Status: DESIGN, not built (2026-06-14).** Captures a defense against a
> malicious collection maintainer destroying/blanking on-chain NFT metadata
> (the "Omen scenario" — a project abandoning Cardano and obliterating
> metadata to force migration). Companion to
> [`COLLECTION_MODULES.md`](./COLLECTION_MODULES.md) (the `collection-metadata`
> module + CIP-25 facade / reveal handling this builds on). No immediate build
> — recorded now so it's ready to fire when needed.

## TL;DR

A reveal and an obliteration use the **same on-chain mechanism**; they're
structurally indistinguishable. Our metadata resolvers correctly follow
metadata *forward* (so reveal collections work), which means a later blanking
tx wins and wipes good traits. Defense is three complementary controls:

1. **Degradation circuit-breaker** (worker) — auto-detect a finalise that
   would zero-out traits across many owned assets; quarantine + alert instead
   of committing. The *live* tripwire.
2. **`lock_metadata_at_slot`** (platform decision → mitos mechanism) — pin a
   collection's metadata at a known-good slot; resolve "latest metadata ≤ N",
   ignore anything after. The durable anchor; survives reingestion.
3. **Per-asset metadata history in the module** (mitos) — so "≤ N" is
   answerable natively (and, for CIP-68, content-addressed and burn-proof).

**Architecture rule:** the *decision* (which collection, what slot, custody
judgment) is platform/curation; the *mechanism* (resolve ≤ N) is the mitos
data plane — same category as the reveal fix. The worker only owns the
decision, the detector, and an optional tactical recovery shortcut.

---

## 1. Threat model

An author re-publishes metadata to **destroy** rather than improve it. Both
NFT standards are exposed; the mechanics differ.

### CIP-25 (metadata in mint-tx aux-data, label 721)

- Metadata is immutable *per tx*, but indexers honor "latest 721 wins", so an
  author with the minting key re-mints (burn + re-mint, or a metadata-only
  republish) with blank/garbage 721. The latest republish becomes canonical.
- This is a **republish hack** — out of spec, but universally honored.

### CIP-68 (metadata in the reference-token inline datum)

- Updating metadata is the **sanctioned spec mechanism**: spend the
  `000643b0…` ref token, re-produce it with a new datum, bump the `version`
  field. Or **burn** the ref token outright (consume, no re-produce).
- So obliteration needs no hack — it's the normal update path abused. Easier
  than CIP-25.

### Custody is the real threat variable (CIP-68)

Who holds the ref tokens decides whether mass-rug is possible:
- **Project-retained** ref tokens (held in a script/wallet — common) →
  maintainer can mass-obliterate. **This is the Omen risk.**
- **Holder-distributed** ref tokens → each holder controls only their own; no
  mass rug, and a per-asset lock would be *wrong* (blanking your own is the
  holder's prerogative). Inherently resistant.

⇒ Locking is a **collection-level, project-custody judgment**. Record custody
as part of the decision; don't lock holder-custodied CIP-68 collections.

---

## 2. The hard constraint

dolos's `AssetState` keeps only `metadata_tx` (latest metadata-bearing tx) +
`prev_metadata_tx` (one step back, for rollback) + `version` — **no
slot-indexed metadata history** (dolos-cardano `roll/assets.rs`; surfaced via
`mitos-data-plane` `types/asset_state.rs::AssetMintState`). So "metadata as of
slot N" is **not** directly answerable from chain state. Any ≤-N resolution
needs one of: `prev_metadata_tx` (one step), a Maestro history walk, or a
metadata history we keep ourselves (control C).

This is *why* the defense can't be "detect bad metadata at reingest" — a cold
reingest has no good baseline and faithfully captures whatever's latest (the
poison). The protection must capture/anchor the good state and bind
resolution to it.

---

## 3. Control A — degradation circuit-breaker (worker, the live tripwire)

**Where:** `cnft.dev-workers` `workers/collection-ownership/src/ownership_do/`
`traits.rs::reconcile_traits` (it already loads `existing` bitmaps at step 5
and computes a diff).

**Obliteration signature:** owned assets transition from rich bitmaps →
**empty bitmaps** (they're blanked, not burned, so they're *not* orphans —
they get overwritten to zero-traits). The schema never shrinks (`get_or_assign`
only adds), so detection must be on **set-bit totals**, not schema size.

**Logic:** before committing writes, compare set-bit count (new vs existing)
across assets present in both. If a finalise would drive a large fraction of
owned assets from has-traits → no-traits (e.g. **> 25 % of owned assets lose
all traits in one finalise**, tunable per-collection), then:
1. **Abort** the write (don't reconcile away the good bitmaps — `existing`
   stays intact).
2. Set collection state `metadata_quarantined`.
3. **Alert** (via the notifier worker) with the offending slot/tx.

**Quarantine, don't reject:** a rare legitimate mass-restructure must not be
silently lost. The operator reviews → release (legit) or lock (malicious).
The circuit-breaker's real job is to **hand the operator the slot N** and the
last-good moment, so control B/C can anchor before any reingest.

**CIP-68 bonus signal:** the `collection-metadata` module can flag a
`version`-up / content-down datum update (version bumps while attributes
vanish) — a precise fingerprint CIP-25 can't produce. Cheap to add in
`handle_produced`/`flush_buffer`.

**Limit:** A is useless on a cold reingest (no `existing` baseline). It is the
*live* detector that triggers B/C; it is not itself reingest-safe.

---

## 4. Control B — `lock_metadata_at_slot` (the durable anchor)

The load-bearing piece. One declarative value per collection — human-meaningful
("the collection as it stood at slot N, before the rug"), minimal to store, and
**re-derivable** (survives any reingest because it re-drives resolution, not a
stored copy).

### Decision vs mechanism

- **Decision (platform):** `collection_metadata_lock` row in **D1**:
  `{ policy, lock_slot, reason, custody, set_by, set_at }`. Durable, survives
  DO reset. The operator (or an accepted quarantine) sets it.
- **Mechanism (mitos facade):** given a policy's `lock_slot`, resolve each
  asset's metadata as the **latest metadata-bearing tx ≤ N**, not the latest.
  Lives in `crates/mitos-platform/src/host_fns/mod.rs` alongside
  `cip25_metadata` / `cip25_metadata_batch` and the `cip25_source_tx` reveal
  helper — same resolution layer.

### Resolution ≤ N, per standard

| | CIP-25 | CIP-68 |
|---|---|---|
| Canonical "latest" | latest 721 tx | current ref-token datum |
| Resolve ≤ N | latest 721 tx ≤ N → decode aux 721 | ref-token datum live at N |
| Most-robust pin | good `source_tx` | **good `datum_hash`** (content-addressed; survives spend **and burn**) |
| Recovery source | Maestro `/transactions/{tx}/cbor` | Maestro ref-token tx history + `/datums/{hash}` |

Resolution order (cheapest → most-general):
1. **`prev_metadata_tx`** (dolos, one step) — covers single-step
   reveal→obliterate; if prev's slot ≤ N and content non-degenerate, use it.
2. **Module metadata history** (control C) — answers ≤ N / ≤ version natively.
3. **Maestro history walk** — enumerate the asset's metadata txs (CIP-25) or
   the ref token's update txs (CIP-68), take the latest ≤ N, resolve its
   721 / datum. Handles **multi-step** blanking (reveal → junk1 → junk2),
   which `prev_metadata_tx` alone cannot.

### CIP-68's decisive advantage: content-addressed datums

A CIP-68 datum, once seen by an indexer, stays resolvable **by hash** from the
datum store **even after the ref-token UTxO is spent or burned** (mitos
`resolve_datum_bytes` / `chain_data::read_output_datums`; Maestro
`/datums/{hash}`). So the most robust CIP-68 pin is the **good datum hash** —
capture it at lock time, resolve from the store forever, regardless of the
current ref-token datum. CIP-25 has no equivalent; a burned CIP-68 ref token's
metadata is still recoverable, a CIP-25 holder-token burn's is not relevant
(metadata lives in the immutable mint tx).

`version` is a CIP-68-native alternative anchor ("lock at version ≤ V"), but
**slot stays the universal primitive** so one knob covers both standards;
capture `version` as informative metadata alongside.

---

## 5. Control C — per-asset metadata history in the module (resilient backing)

The `collection-metadata` module already decodes every metadata event it sees
live (`community-modules/collection-metadata/collection_metadata.rs` —
`handle_produced`/`flush_buffer` for CIP-68 datum motion; `produced_plain` for
CIP-25 mints; `decode_page` for cold-start). It can persist a per-asset history
in its state-kv:

```
asset_suffix → [ { slot, version, source_tx, datum_hash?, has_attributes } ]
```

Then "≤ N" / "≤ version V" is answerable from mitos's own state — no Maestro in
the hot path — and CIP-68 resolution is by **datum hash → store** (burn-proof).
Maestro stays the fallback only for *recovering* an obliteration that predates
the module's history (the "caught napping" case where the good reveal is older
than what mitos has indexed).

This is the truly resilient long-term form and generalizes beyond rug-recovery
to arbitrary point-in-time metadata queries (useful to any consumer).

---

## 6. Recovery procedure — the "caught napping" (already-happened) case

The good metadata never left the chain (CIP-25: older reveal tx; CIP-68: older
datum, resolvable by hash). Recovery = **re-resolve at a capped slot, capture,
lock.**

1. **Find N.** Inspect the asset's tx history (Maestro), find the obliteration
   tx, set N = slot just before it. (If control A had been live it would have
   handed you N automatically — that's its value.) A "degenerate-content scan"
   can auto-propose N: walk back from latest to the first tx whose metadata
   actually has attributes.
2. **Set `lock_slot = N`** in D1 (+ custody judgment).
3. **Capped recovery pass:** re-ingest with metadata resolved ≤ N
   (`prev_metadata_tx` fast-path → module history → Maestro general path) →
   stage → finalise → good traits restored.
4. **Freeze:** the lock makes future on-chain updates *and* future reingests
   resolve ≤ N — the obliteration can never win again, and a cold reingest
   reproduces the good state deterministically.

---

## 7. Reingestion coverage (the property A/earlier-locking can't give alone)

A cold reingest (DO reset → mitos cold-start) starts with no baseline, so
controls A and a worker-only "ignore updates" lock both fall down — the worker
faithfully captures whatever mitos resolves as latest (the poison). Only
`lock_slot` enforced at the **mitos facade** survives this: the cold-start *is*
the reingest, and if the facade resolves ≤ N, every reingest is protected with
zero worker involvement. This is the decisive reason the mechanism belongs in
mitos, not the worker.

---

## 8. Edge cases

- **Assets minted after N** (legit new mints post-lock) are excluded by a hard
  cap — fine for a rug/migration recovery (no legit new mints expected). Keep
  the cap per-collection so healthy collections are untouched.
- **CIP-68 holder-custodied ref tokens** — do *not* lock; blanking is the
  holder's right, not a rug. Custody gate on the decision.
- **Multi-step obliteration** (reveal → junk1 → junk2) — `prev_metadata_tx`
  reaches only junk1; needs the Maestro/module-history ≤-N path.
- **Datum pruning (CIP-68)** — a datum never seen by any indexer before its
  spend may be unresolvable by hash. The module sees ref-token production live
  so it records datums it observes; cold recovery of a pre-history,
  long-pruned datum is the one genuinely unrecoverable case (rare).
- **False-positive on A** — a real mass-restructure trips the breaker;
  mitigated by quarantine-not-reject + operator review.

---

## 9. Layering summary

| Piece | Lives in | Role |
|---|---|---|
| Degradation circuit-breaker | worker `reconcile_traits` | live tripwire → quarantine + alert |
| `version`-up/content-down flag | `collection-metadata` module | CIP-68 live fingerprint |
| Lock decision + config | platform / D1 | which collection, slot N, custody |
| `resolve ≤ N` mechanism | mitos `host_fns` facade | the actual ≤-slot resolution |
| Per-asset metadata history | `collection-metadata` module state-kv | native ≤-N / datum-hash backing |
| Tactical one-time recovery | worker (Maestro-driven) | incident speed before mitos change ships |

## 10. Phased build plan

1. **Control A** (worker `reconcile_traits` guard + notifier alert) — highest
   value, lowest cost, no chain-data change. Ships the live tripwire.
2. **Control B mechanism in the mitos facade** (`resolve ≤ lock_slot` via
   `prev_metadata_tx` + Maestro) + D1 decision config + a worker tactical
   recovery endpoint for incident speed.
3. **Control C** (module per-asset metadata history incl. `datum_hash` +
   `version`) — makes ≤-N native + burn-proof and retires the Maestro hot path.

## 11. Open decisions

- Circuit-breaker threshold: fraction-of-assets-zeroed (lean) vs absolute
  set-bit drop; per-collection tunable default.
- Lock granularity: collection-level only, or per-asset override (for partial
  obliteration).
- Where `lock_slot` config reaches mitos: per-policy subscribe parameter vs a
  config the module reads vs worker-driven resolution. (Subscribe-parameter
  keeps policy in the platform, mechanism in mitos.)
- Whether to also expose generic point-in-time metadata queries (`as_of_slot`)
  once control C exists — likely yes; broadly useful.
