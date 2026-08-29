# Backlog

Improvement notes that aren't scheduled work. One heading per item;
delete on completion or when obsoleted.

## Batched datum prefetch for bootstrap / recapture

**Context (2026-07-17):** the `jpg-store-listing` bootstrap over the
post-shutdown (stranded) jpg book stalled host startup for 45+ minutes:
each hash-only create datum missed the local archive and fell through
to the fallback provider — one sequential Koios HTTP call per listing
(~10/min), because `chain_data::datum_by_hash` is a synchronous
guest→host call made demand-driven mid-execution, and bootstrap
dispatches batches strictly in order (deliberately — replay/trap
determinism).

Mitigated at the module level: jpg-store-listing creates are now
payload-only (no `datum_by_hash` fallback on the produced path),
matching live semantics where jpg reveals datums only at spend.

**The general fix, if a future venue ships hash-only-at-create escrow
with a large book:** a bootstrap/recapture *prefetch pass*. The
platform already holds every scanned output's `datum_hash` before
dispatching — collect the misses, resolve them in bulk (Koios
`datum_info` accepts arrays; one call per few-hundred hashes), warm the
datum cache, then dispatch in chain order as today. Concurrency lives
entirely host-side before module execution starts, so ordering and
replay determinism are untouched.

Sizing: turns O(book × RTT) sequential into O(book / batch) — the jpg
incident's hours into seconds.

## `/api/tx/submit` falsely rejects txs using pre-Conway reference scripts

**Context (2026-08-08):** every jpg.store / Wayup collection-offer
cancel submitted through `POST /api/tx/submit` came back `400` with
`phase-1 script rejected the transaction: script witness is missing`,
while the same CBOR evaluated cleanly on a real cardano-node (all
validators ran, budgets matched the tx's ex-units exactly). The tx was
valid; we were rejecting it.

**Cause — upstream, in `pallas-validate`.** `phase1/conway.rs`
resolves reference scripts with:

```rust
fn get_script_hash_from_reference_input(ref_input, utxos) -> Option<PolicyId> {
    match utxos.get(...).and_then(MultiEraOutput::as_conway) { ... }
}
```

and `MultiEraOutput::as_conway()` returns `None` for
`MultiEraOutput::Babbage`. `crates/cardano/src/validate.rs` decodes
resolved UTxOs at their **stored** era (`MultiEraOutput::try_from(eracbor)`,
over `tx.requires()` = inputs + reference_inputs + collateral), which is
correct — so any reference script published before Conway is invisible
to the validator. `check_input_scripts` then finds the spent script
input uncovered and returns `PostAlonzo(ScriptWitnessMissing)`.

`get_script_hash_from_input` has the same flaw in the permissive
direction: a Babbage-era script UTxO being spent isn't recognised as
needing a script at all.

**Blast radius.** Babbage ran to epoch 506, so this hits most
established mainnet dApps — not an edge case. jpg.store's CO script ref
is epoch 366, Wayup's epoch 475. It stays rare in the wild only because
`pallas-validate` runs for dolos submitters, and dolos is mostly
deployed read-only.

**Fix.** `pallas-traverse` already exposes era-agnostic accessors that
handle Babbage correctly — `MultiEraOutput::script_ref()` and
`::address()`. Both functions should use those instead of `as_conway()`.
Two functions, no new logic. Upstreamable to txpipe/pallas; until then
it needs a `[patch.crates-io]` here.

Not fixed by a version bump: `pallas-validate` 1.1.1 is the latest
release and its `phase1/conway.rs` is byte-identical to 1.0.0, and
`main` still calls `as_conway()`. No matching upstream issue found.

**Workaround in place meanwhile:** `cnft.dev-workers`'
`services/tx-submit` now submits Koios-first with mitos as the
fallback. Ordering is what matters — `submit_with_fallback` treats
`Rejected` as "invalid everywhere" and short-circuits, so a mitos false
negative is fatal when mitos leads. Revert that ordering once this is
fixed.
