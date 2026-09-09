# mitos-chain-walk

⚠️ **A library, not a binary** — it lives under `tools/` for historical
reasons. `cargo run -p mitos-chain-walk` does nothing.

The plumbing every Mithril-snapshot walker shares. Extracted from
`market-ledger` (2026-08-16) so `project-ledger` could reuse it without
becoming a mode of a tool whose contract is a static venue set.

Family B (see the root README). Consumers: `market-ledger`, `project-ledger`,
`token-ledger`, and — via `decode` — anything else reading chunk files.

## Four pieces, all venue-agnostic

| module | what |
|---|---|
| `mithril` | shell out to `mithril-client` to download + verify a certified immutable DB, optionally a partial immutable-file range |
| `decode` | bare-pallas tx decode into the parts a walker needs: outputs with datums, inputs with **canonical-order** redeemers, witness datums, aux data |
| `checkpoint` | the crash-visible JSON mirror of a walker's last committed position |
| `CHUNK_SLOTS` | 21,600 — the slots per immutable file, which is the grain a great deal of this workspace is quantised to |

## Why `decode` is bare-pallas

Walkers need the transaction's *parts*, not a `MultiEraTx` — building one
clones the witness set alongside, and at 123.6M transactions that cost is the
walk. `decode` takes the borrowed slices instead.

`Asset` carries an **optional** quantity, with `Asset::nft()` for the
single-quantity case: `unmeasured` and `zero` are different claims, and a
`quantity: 1` default silently shipped 1 of 454,140 tokens once.

## ⚠️ Redeemer order is canonical, not positional

Inputs are sorted canonically before redeemer indices are assigned. A walker
that pairs redeemer `i` with the `i`-th input *as written* will mis-attribute
on any transaction whose inputs were not already sorted — which is most of
them.
