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
