# tx-index as a mitos datasource

Status: **requirements** (2026-09-13). No code yet. Scoped deliberately
*around* the Maestro/Koios cutover — nothing here is on the Sept-18
critical path, and none of it should be allowed to drift onto it.

Owner: TBD
Related: `crates/tx-index/src/reader.rs`,
`crates/mitos-platform/src/local_tx_index.rs`,
`crates/mitos-platform/src/maestro_fallback_plane.rs`,
`crates/mitos-platform/src/host_fns/mod.rs`,
`docs/design/FALLBACK_PROVIDER_AND_BATCH_PREFETCH.md`,
`docs/design/WATCHED_UTXO_INDEX.md`,
`infra/docs/mitos-operations.md`

## Summary

mitos already carries a local tx-index tier, but it answers exactly one
question — auxiliary data — and it is almost certainly not switched on in
production. The prior-output path, which is where the remote provider is
actually being paid per call, has no local tier at all.

The capability to close that gap is already built and simply not called:
`tx_index::Index::resolve(hash, index)` (`reader.rs:243`) returns a
verified output for an outref, with the same three-state answer that makes
the aux-data tier safe.

This document states what tx-index has to guarantee before it can stand in
front of a remote provider as a general mitos datasource, and — just as
importantly — where the boundary is that it must never be pushed past.

## Where we are today (measured)

**The aux-data tier exists and is wired.** `local_tx_index.rs` wraps
`IndexHandle` and exposes `tx_metadata()`; `host_v2.rs:452` passes it into
`CachingDataPlane`, giving a 4-tier path (`host_fns/mod.rs:680-740`):
cache → dolos (7-day) → tx-index → remote.

**It is live on mainnet, and it is working.** Verified on the box
2026-09-13:

```
local tx-index tier enabled for aux-data lookups
  index_dir=/opt/tx-index/mainnet
  cov=Coverage { base_first_chunk: Some(0), base_last_chunk: Some(9141),
                 base_entries: 123673450, tail_segments: 3,
                 tail_entries: 15254, newest_chunk: Some(9144) }
```

It also re-maps itself in practice — four `re-mapped after on-disk change`
events between Sep 9 and Sep 12, so the index tracks the refresh roughly
daily rather than weekly, and R3.2's seam is narrower than the weekly
cadence implies.

⚠️ **The repo had drifted from the box.** `MITOS_TX_INDEX_DIR` /
`MITOS_TX_INDEX_IMMUTABLE` were set in `/etc/default/mitos-mainnet` but
appeared in **neither** env template nor the ops doc, so a rebuild from the
templates would have silently dropped the tier — and with both vars unset
`shared()` returns `None` silently (`local_tx_index.rs:150-190`), which is
indistinguishable from a working tier. Templates and ops doc corrected
2026-09-13; R1 is now about keeping them in sync, not about turning the
tier on.

**The output path has no local tier.** `MaestroFallbackPlane` is built at
`host_v2.rs:620-628` with `fallback_opt` and no `local_index`. Its
`read_utxos` (`maestro_fallback_plane.rs:334-361`) is 2-tier: inner plane,
then one remote call per gap.

**This is where the entire remote bill now sits.** Measured on mainnet over
24h to 2026-09-13:

| Path | Remote calls / 24h |
|---|---|
| aux-data (`tx_metadata`, has a local tier) | **0** |
| prior outputs (`read_utxos`, has no local tier) | **8,581** |

The tier with a local answer costs nothing remotely. The tier without one
is the whole load.

**Throttling: real but currently harmless — do not oversell it.** Those
8,581 calls drew **33 Koios 429s, all tagged `utxo_info`** — 0.38%, in
short bursts rather than sustained. The client's backoff absorbed every
one: retry attempts reached 1 (×24), 2 (×6), 3 (×2), 4 (×1) against a
`MAX_ATTEMPTS` of 5, with **zero** retry exhaustions, **zero** hard
failures and **zero** unresolved prior outputs in the same window.
Preprod: 0 429s. The API key is present and correct on both instances
(tier 2, same key as the workspace `.env`), so this is burst concurrency,
not quota exhaustion or a missing credential.

⇒ **R2 is still an efficiency argument, not an incident.** The honest case
is that it takes 8,581 calls/day to ~0 and removes a retry ladder that has
been one attempt from its ceiling; it is not repairing something currently
broken.

Caveat on the aux-data `0`: it proves the remote is never *needed* there,
not that tx-index rather than cache/dolos answered. R6's counters separate
those.

**`DecodeLevel::Full` is already not served.** Maestro's `/txo` doesn't
surface reference scripts, so `typed_output_from_maestro` hardcodes
`script_ref: None` (`maestro.rs:402-406`). Any local tier that also returns
`None` is at parity with today, not a regression.

---

## R1 — Configuration and operability

The tier is live on mainnet (see above). R1 is therefore about keeping the
repo honest about that, not about switching it on. **Done 2026-09-13**
except where noted.

- **R1.1 ✅** `MITOS_TX_INDEX_DIR` and `MITOS_TX_INDEX_IMMUTABLE` are set in
  the mainnet env template and the `mitos-operations.md` env block
  alongside `MITOS_FALLBACK_PROVIDER`; preprod records their *deliberate*
  absence rather than leaving it to be guessed. The general rule this
  drift proved: **a var that exists only on the box is a var the next
  rebuild loses.**
- **R1.2** Silent absence is not acceptable for a tier that is supposed to
  be load-bearing. Startup must make the state legible: the tier logs
  `Coverage` when it opens (`local_tx_index.rs:161`), so the requirement is
  that this is *checked* — and that a configured-but-unusable index is
  distinguishable from an unconfigured one in the logs (it already is,
  `:169-177`). Post-deploy smoke (`POST_DEPLOY_SMOKE.md`) should assert the
  tier is up, the same way it currently asserts provider config.
- **R1.3** Absence must remain non-fatal. The tier is an optimisation;
  losing it means paying the remote, not failing. This property already
  holds and must survive the output work.

## R2 — Output resolution

### R2.1 Tier placement

- **R2.1.1** The local index must sit inside `MaestroFallbackPlane::read_utxos`
  as a **tier**, not behind `FallbackProvider`. Same argument as aux-data
  (`local_tx_index.rs:19-32`): the trait returns `Option<TypedOutput>` and
  collapses "no such output" with "I can't say". `Resolution` already keeps
  them apart (`reader.rs:78-91`) and that distinction must not be discarded
  at the boundary.
- **R2.1.2** Order within `read_utxos`: `OUTPUT_CACHE` → local index →
  remote via `fetch_dedup`. The cache goes first because it is free.
- **R2.1.3** The local tier must **not** be wrapped in the single-flight
  `INFLIGHT` machinery (`maestro_fallback_plane.rs:170-200`). That exists to
  stop 13 modules making 13 identical HTTP calls; a sub-millisecond `pread`
  does not need coalescing and would only inherit the lock traffic.
- **R2.1.4** **The gate must not apply to the local tier.** `gate_allows`
  is currently checked before the remote fetch (`:353`) and exists purely to
  reduce remote spend (`WATCHED_UTXO_INDEX.md`). Gating a free local lookup
  is pure loss — it would skip refs the index could have answered for
  nothing and leave them unresolved. The gate must continue to scope only
  the remote tier.

### R2.2 `ResolvedOutput` → `TypedOutput` mapping

`ResolvedOutput` (`wire.rs:16-28`) vs `TypedOutput` (`types/output.rs:163`):

| `TypedOutput` | Source | Requirement |
|---|---|---|
| `address` | `.address` | `decode.rs:58-61` substitutes the sentinel `"<unparsable>"` when pallas refuses the address. This must map to a **local miss** (fall through), never propagate — an empty-ish address silently fails `InterestSet::matches_output`, which is the exact class of bug the unresolved-placeholder work was built to stop. |
| `lovelace` | `.lovelace` | Direct. |
| `assets` | `.assets` | `{policy hex, name hex, quantity}` → `AssetEntry { PolicyId::new(policy)?, asset_name_hex, quantity }`. A policy that fails to parse must be treated as a decode failure, not silently dropped. |
| `datum` | `.datum_hash` / `.inline_datum` | See R2.3. |
| `script_ref` | — | Not surfaced; `ResolvedOutput` carries only `has_script_ref: bool`. See R2.4. |
| `original_cbor` | `.cbor` | **`.cbor` is re-encoded** (`wire.rs:26-27`, `decode.rs` `o.encode()`), not the original bytes. `TypedOutput::original_cbor` is documented for "re-serialising into tx constructions without re-encoding". Either leave it `None` on the local path, or prove byte-identical round-trip first. Defaulting to `None` matches the remote path today (`maestro.rs:406`). |
| `decoded_at` | request level | Mirror of the requested `DecodeLevel`. |
| `resolution` | — | `Resolution::Resolved`. |

- **R2.2.1** Every field that cannot be faithfully produced must make the
  output a miss or be left genuinely absent. No placeholder values.

### R2.3 `DecodeLevel` semantics

- **R2.3.1 `Lean`** — fully served locally. Address, lovelace, assets, no
  datum work. This is the default (`types/decode.rs`) and should be the
  overwhelming majority of the win.
- **R2.3.2 `WithDatum`, inline** — `ResolvedOutput.inline_datum` carries
  the CBOR. `TypedDatum.hash` is a required `Hash<32>`, so the tier must
  **compute** blake2b-256 over the datum bytes rather than inventing one.
  `payload` decodes from the same bytes; `original_cbor: Some`.
- **R2.3.3 `WithDatum`, hash-only** — this is **a complete local answer,
  not a miss.** `TypedDatum` already models it natively: `payload: None,
  original_cbor: None` with the hash populated is exactly what the type
  documents (`types/output.rs:56-64`), and `fill_from_witness`
  (`types/output.rs:79`) backfills the bytes from the *consuming* tx's
  witness set downstream — which is precisely the jpg.store
  metadata-encoded-datum case. Treating hash-only as a miss would send the
  majority of listing lookups to the remote and give back most of the win.
- **R2.3.4** **This is parity with the current remote, not a new gap.**
  Koios `/utxo_info` returns `datum_hash` with no preimage —
  `UtxoInfoEntry` has no bytes field for hash-attached datums, and
  `datum_from_koios` returns `payload: None, original_cbor: None`
  (`koios.rs:602-612`). Maestro *did* fill them eagerly (`TxoDatum.bytes`,
  `maestro.rs:115-121`), so preimage resolution went from eager to lazy at
  the Koios cutover (2026-07-11), by design: the Koios comment points at
  `read_datum` → `/datum_info` as the explicit path, and
  `backfill_prior_datum` (`dispatch.rs:248-261`) covers the consuming-tx
  case. The local tier must behave exactly as Koios does here. Whether it
  should *additionally* consult the remote for the preimage is a cost
  decision, not a correctness one — default: do not, and see the open
  question, which is a live prod question independent of this work.
- **R2.3.5 `Full`** — see R2.4. Until then, `Full` returns `script_ref:
  None`, at parity with the remote path.

### R2.4 Reference scripts

- **R2.4.1** `DecodeLevel::Full` is not served by anything today. The local
  tier is the first thing that *could* serve it — the script ref is present
  in the output CBOR that `decode::convert` already holds and reduces to a
  boolean (`decode.rs:87`).
- **R2.4.2** If `Full` is wanted, extend `ResolvedOutput` to carry the
  script (bytes + language) and map to `TypedScript { hash, language,
  original_cbor }`. This is an enhancement over the remote, so it must be
  sequenced separately and must not block R2.1-R2.3.

### R2.5 Batching

- **R2.5.1** `read_utxos` takes `&[OutputRef]`. `resolve` reads the body and
  decodes **every** output before discarding all but one (`reader.rs:243-255`).
  Multiple orefs against the same tx must be **grouped by `tx_hash`**: one
  `tx()` and one `outputs()` per tx, then index into the result. A bootstrap
  re-walk hits this constantly; per-oref resolution would re-read and
  re-decode the same body N times.
- **R2.5.2** Lookups must stay off the async executor — `spawn_blocking`
  around the `pread` + decode, as the aux-data tier already does
  (`local_tx_index.rs:85`).

## R3 — Coverage and freshness

- **R3.1** The index covers **completed Mithril chunks**; dolos covers the
  trailing 7 days. These are complementary, which is what makes the tier
  worth building — the fallback plane's entire job is the >7-day gap.
- **R3.2 The seam is the risk.** The index refreshes **weekly** off the
  market-ledger snapshot timer; dolos holds 7 days. Any slip opens a window
  neither tier covers, and every ref in it goes remote. **Refresh cadence
  must be strictly tighter than the dolos archive window**, with headroom.
- **R3.3** `Coverage` (`reader.rs:92-99`) exposes `newest_chunk`. The age of
  the newest chunk must be observable and alertable, so a stalled refresh
  surfaces as an alert rather than as a quiet rise in remote spend.
- **R3.4** `reload_if_changed` is checked at most once per 60s on the lookup
  path (`local_tx_index.rs:51`, `:110-130`). This is correct for a weekly
  refresh — it is about noticing eventually, not promptly — and must not be
  turned into a background task.

## R4 — Correctness invariants

These are the properties that let a local tier answer *before* the remote
without changing what mitos believes.

- **R4.1 Verification is mandatory.** The index stores an 8-byte hash prefix
  (`format.rs:147`), so a lookup can land on the wrong tx. `tx()` re-hashes
  every candidate and moves on if it mismatches (`reader.rs:195-220`); a
  prefix collision therefore costs a wasted read, never a wrong answer.
  `resolve` goes through `tx()` and inherits this. **No path may read a span
  directly and serve it as output data** — the same argument `tx_metadata`
  makes at `reader.rs:225-231`.
- **R4.2 Three-state discipline.** `Resolution::NoSuchOutput` is a **final
  negative** — the tx is indexed and has fewer outputs than the index asked
  for, so the oref is not resolvable by anyone — and must **not** fall
  through to the remote. `UnknownTx` means "this tier cannot say" and
  **must**. Collapsing these either burns remote budget on unanswerable
  refs or hides real gaps.
- **R4.3 Failures are `UnknownTx`, not errors.** An I/O or decode failure
  means "cannot say", which is what makes the caller fall through. It must
  be logged so a broken index shows up as noise rather than as silently
  slow lookups — the rule already applied at `local_tx_index.rs:76-80`.
- **R4.4 Era skew must fail safe.** `decode::outputs` returns an error for
  an era newer than the build (`decode.rs:51-53`). Per R4.3 that becomes
  `UnknownTx` and falls through — correct, but it means a stale binary
  degrades into full remote traffic at the next hard fork. It must alert,
  not just fall through. Byron through Conway are handled
  (`decode.rs:17-50`).
- **R4.5 Span width.** `Location.len` is `u16` (`format.rs`), capping a span
  at 65,535 bytes. Cardano's `maxTxSize` is well inside that, so this holds
  — but it is an assumption the format bakes in and should be asserted at
  index-build time rather than discovered as a truncated body.

## R5 — Hard boundaries: what tx-index must never be asked

This is the section that keeps the tier safe. tx-index is an archive of
*what was created*, keyed by tx hash. It is not a ledger state.

- **R5.1 No UTxO-set queries.** tx-index has **no spend tracking**. It knows
  an output was created; it cannot know whether it has since been spent. It
  may therefore only serve `read_utxos` in the prior-output sense — the
  caller already holds the oref and wants its *content*. It must never be
  wired into `search_utxos`, `utxos_by_address`, `utxos_by_policy`, or
  `utxos_by_payment_cred`, all of which currently pass through to the inner
  plane (`maestro_fallback_plane.rs:363-382`) and must continue to.
- **R5.2 No datum-by-hash.** `fetch_datum(datum_hash)` is keyed wrongly:
  datum preimages live in witness sets, and the index is keyed by tx hash.
  Serving it would need a separate datum-hash index. This stays on the
  remote provider permanently.
- **R5.3 Nothing at the volatile tip.** Only completed chunks are indexed.
  Tip data is dolos's job.
- **R5.4** Any future consumer that wants one of the above must build the
  index for it, not stretch this one.

## R6 — Observability

- **R6.1** Counters per outcome — `local_found`, `local_no_such_output`,
  `local_unknown`, `remote_calls` — so the collapse in remote call rate is
  measurable rather than asserted. `record_gate_decision`
  (`maestro_fallback_plane.rs:94`) is the existing pattern to follow.
- **R6.2** A coverage-age gauge per R3.3.
- **R6.3** Local hit rate and remote call rate must be visible in the same
  place, so a regression (stalled refresh, era skew, config lost on a
  rebuild) shows up as traffic moving between tiers.

## R7 — Performance targets and non-goals

- **R7.1** Lookup p50 well under a millisecond, per the existing aux-data
  tier's basis (`local_tx_index.rs:15-17`). The benchmark to beat is the
  measured ~4 lookups/second remote bootstrap rate.
- **R7.2** The win is concentrated in bootstrap/recapture re-walks of old
  transactions, not at the tip, where archive-horizon misses are already
  rare (`maestro_fallback_plane.rs:30-34`).
- **R7.3 Non-goal:** replacing dolos for anything inside the archive
  horizon. The inner plane stays tier 1.
- **R7.4 Non-goal:** removing the remote provider. R5 guarantees a residual
  remote need (datum-by-hash, tip, coverage seam). The goal is to make the
  remote the rare path, not to delete it.

## Phasing

0. **R1 — ✅ done 2026-09-13.** Templates and ops doc now match the box.
   The tier was already live; the risk was that only the box knew.
1. **R2.1-R2.3, R2.5 — output resolution at `Lean` + `WithDatum`.** Behind
   the existing `Option<Arc<LocalTxIndex>>`, so absence is the current
   behaviour. Fixture/unit coverage of the tier's own branches is part of
   this step, not a follow-up — preprod will not exercise them (see
   Decided).
2. **R6 + soak.** Confirm the hit rate and the remote-call collapse before
   trusting it.
3. **R2.4 — reference scripts / `DecodeLevel::Full`,** only if a caller
   genuinely needs it.
4. **Revisit the gate.** If the local tier serves most gaps for free, the
   trade `GateMode::On` makes — skipping refs to save remote spend —
   largely evaporates, and the honest outcome may be retiring the gate
   rather than graduating it out of `shadow`.

## Decided

**Preprod runs without the tier** (2026-09-13). `/opt/tx-index/mainnet` is
mainnet-only and stays that way; preprod degrades to the Koios fallback
permanently. Preprod Koios is already configured and keyed
(`KOIOS_NETWORK=preprod`, same `KOIOS_API_KEY` as mainnet), and preprod
volume makes rate limits a non-issue. A Mithril ingestion process on
preprod may happen in time, at which point this is revisited — nothing here
forecloses it.

Two consequences, one good and one that creates an obligation:

- **Good:** preprod becomes a permanent live exercise of the Koios path, so
  the fallback can't silently rot after the cutover. That is worth having.
- **Obligation:** preprod therefore stops being a rehearsal for mainnet's
  resolution path. Prod would serve most orefs locally while preprod serves
  all of them remotely, so the tier's own logic — the
  `NoSuchOutput`/`UnknownTx` split (R4.2), the mapping (R2.2), the
  `<unparsable>` miss (R2.2), era skew (R4.4) — would be exercised **in
  production only**. **Those paths must be covered by fixtures and unit
  tests rather than by a preprod soak.** Treat this as a requirement of
  R2, not an afterthought.

## The prior-output path is not tx-index's (measured 2026-09-23)

**A local tier for the output path is worth having, and it should not be this
one.** The argument is a shape mismatch, and it is written out in full in
`DELTA_CHAIN.md` §9; the short version:

- `tx-index` holds **`tx hash prefix → Location { chunk, offset, len }`**. A spend
  needs the **output bytes**, so `Index::resolve` re-derives them: a `pread` of the
  transaction's span, a **blake2b over the whole body** to confirm an 8-byte prefix,
  and a decode of **every output in that transaction** before the one asked for can
  be picked out (`reader.rs`). A transaction with 20 outputs costs 20 decodes to
  answer about 1.
- The outputs a tip-follower cannot resolve are exactly the ones created since the
  published book's boundary — and **the walk that produced them had each one in
  hand, decoded, before it was needed.** A back-pointer throws that away and
  re-derives it, per query, from disk.

Measured on the ARM box over chunks 9161..9181: a walk that emits the created
outputs as it passes them resolves **100%** of the inputs the book cannot, at
**~240 ns a probe** against `book_utxo`'s 496 ns, costing **3.3 MiB and +31% of the
walk per chunk**. That is `LIVE_BOOK.md` §B7's "inverse of tx-index" made
concrete: *a forwarding index answers "where did this go"; a resolver needs "what
was this"*.

⚠️ **One half of that comparison is measured and the other is not.** The ~240 ns is;
a like-for-like `Index::resolve` benchmark is not, because it needs a 123.6 M-entry
index and `/opt/tx-index` is not populated on that box. `DELTA_CHAIN.md` §9 carries
the derived estimate and says so.

Two questions that look like one, and do not have the same answer:

| the question | who answers it |
|---|---|
| a transaction **just landed** — what were its inputs? | **book + delta.** This is the half that closed |
| a re-walk of a **2022** transaction — what were its inputs? | those outputs are long spent, so a snapshot of the live set does not hold them and no delta reaches back past its boundary. This is tx-index's remaining job, and it is R7.2's archive-horizon case |

What follows for this document:

- **R2 is still worth doing**, and this does not change its cost/benefit — but its
  justification is now narrow and nameable: the archive-horizon case (R7.2), which
  is where R7.2 says the 8,581 remote calls a day originate. That R7.2 claim is
  itself an assertion about the *mix* of those calls rather than a measurement of
  it, and R6's counters are what would settle it.
- **R7.3's non-goal stands**, and is now the whole boundary: tx-index answers
  history, the book plus its delta answers the current set.
- The two artifacts come off **the same walk** (§B7), so this is a question of
  which sink a walk is given, not of which index exists.

## Open questions

Both are measurement-driven, and neither blocks starting.

- **Coverage seam width — largely answered, worth confirming properly.**
  The tier is live on mainnet and the aux-data path made **0** remote calls
  in 24h, so whatever the seam is, it is not costing anything on the path
  that has a local tier. Both tiers ask the same underlying question ("is
  this tx in a completed chunk"), so that is a strong prior for the output
  path too. What is still missing is the *decomposition*: `0` proves the
  remote was never needed, not that tx-index rather than cache/dolos
  answered. R6's `Found`/`NoMetadata`/`UnknownTx` counters settle it, and
  they are cheap. Worth doing before R2 so the expected hit rate is a
  number rather than an inference — but no longer a blocker.
- **Hash-only datum preimages.** Reframed by the R2.3.4 finding: this is
  **not a tx-index question at all.** Since 2026-07-11, hash-attached prior
  datums have arrived from Koios with no preimage, recoverable only lazily
  via `fill_from_witness` or an explicit `read_datum`. That was deliberate,
  but whether the two lazy paths cover every case in practice has never
  been measured. Instrument the existing path: count prior outputs arriving
  hash-only at `WithDatum`, and of those, how many still reach a module
  with `payload: None, original_cbor: None`. ~0 ⇒ settled, and R2.3.3
  stands. Non-trivial ⇒ it is a live prod issue to fix on the Koios path,
  independently of and ahead of this work.
Not open, but worth stating so it stops being re-derived: `Location.chunk`
is `u16`, and mainnet chunks are 21,600 slots (6h) each, so the ~9.1k
chunks today overflow the field in the 2060s. Not a constraint on anything
here.
