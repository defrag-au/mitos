# dex-ledger — deliberately NOT built (2026-08-20)

Status: **decision recorded, no code.** Revisit when the trigger below fires.

## The question

project-ledger needed to stop counting DEX swap returns as project income.
Should that be a `dex-ledger` tool — a third walker beside `market-ledger` and
`project-ledger`, sharing `mitos-chain-walk` — scaffolded now and filled in
later?

## The answer: no, and specifically not scaffolded

**There is nothing to scaffold.** The reusable units already exist and are
already shared:

- `tools/mitos-chain-walk` — mithril bootstrap, block decode, checkpoint, chain
  iteration. Extracted from market-ledger, now used by project-ledger too.
- `crates/mitos-dex-decode` — CSwap + Splash pool/staking datum and script
  address decode. Pure, no WIT coupling. Its own header states the principle:
  *"the decode library is the genuine reuse unit; composition over
  coordination."*

A `dex-ledger` would be a thin tool assembling two crates that exist. The shell
is not the work; the decoding is.

**The immediate problem did not need swap decoding at all.** To stop a return
leg reading as income you only need to know *that a counterparty is a DEX*, not
what was swapped. That is an address lookup — `project-ledger classify`, using
`address-registry`, ~200 lines. Measured on Mekka it moved the treasury's
"unexplained" inbound from 132,590 ADA to 74,352 by identifying **58,150 ADA of
round trips**.

**The extraction pattern here has always been extract-from-working-code.**
`mitos-chain-walk` came out of market-ledger *after* market-ledger worked, which
is why it fitted project-ledger cleanly — it had earned its shape. A scaffold
written in advance of a consumer tends to be the wrong shape by the time one
arrives, and rots quietly in the meantime.

## The trigger to revisit

Build `dex-ledger` when something needs swap **amounts, directions and rates**,
not merely counterparty identity. Two known candidates:

1. **project-ledger price context** — converting flows to a stable unit needs
   the rate that was actually transacted at, not a daily average.
2. **the price oracle** (`project_price_oracle`, ~70%) — already a mitos DEX
   consumer.

Two consumers is the same threshold that justified extracting
`mitos-chain-walk`. Until then, one consumer plus a registry lookup is cheaper
and reversible.

## Worth doing NOW, regardless

**`mitos-dex-decode` has no Minswap.** Minswap dominates the flows this project
touches — 54,580 ADA of the treasury's swap returns — and it is the one gap
that is not speculative. Adding it to the existing decode crate is useful
whether or not a `dex-ledger` is ever written.

## The known weakness of the cheap path

An address registry **fails silently**. A new DEX, a new pool version, an
aggregator nobody catalogued — each reverts to counting a round trip as income,
and the ledger still looks fine. `classify` therefore reports what it could NOT
name, ranked by value, in the same spirit as the unresolved-payer count and the
`no sale row does not mean not sold` warning.

That report is already earning its place. First run on Mekka named 231 of
35,718 counterparties and immediately surfaced:

- `stake1u83ekh6q42…` — **403,747 flows, 242,346,360 ADA**, unnamed
- `addr1w8qnfkpe5e99m7u…` — 115M ADA gross, unnamed, and holding 20,046 ADA of
  what is currently booked as external inflow to the treasury

Those two are the next registry entries, and naming them will move the numbers
again.

Related: `MARKET_LEDGER.md`, `HOLDER_DISTRIBUTION_LP_DECOMPOSITION.md`,
`cnft.dev-workers/docs/design/PROJECT_LEDGER_IMPORTER.md`.
