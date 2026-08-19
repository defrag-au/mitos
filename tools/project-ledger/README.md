# project-ledger

Pure ingestion of one NFT project's two-sided mint view — capital in (mint
payments + royalties), assets out (the holder base forming), capital out
(deployment) — from a certified Mithril snapshot into one slot-keyed ledger.
Never a follower. Design: `cnft.dev-workers/docs/design/PROJECT_LEDGER_IMPORTER.md`.

**Standalone package for now** (own workspace root; see `Cargo.toml` header for
why). Build from this directory: `nix develop ../.. -c cargo build --release`
→ `../../target/release/project-ledger`.

```
project-ledger seed  --registry registry.toml --db mekka.db          # floor (Koios-seeded) + declared wallets
project-ledger walk  --data-dir <market-ledger's dir> --db mekka.db  # decode from the floor; Koios for the input ladder
project-ledger stats --db mekka.db
project-ledger reset --db mekka.db --yes
```

`seed --offline --floor <slot>` avoids the indexer entirely (floor recorded as
`declared`). `walk --remote offline` never calls out — unresolved inputs are
counted on the rows (`unresolved_inputs`), never guessed.

## What the walk does per tx

1. every output → party (stake key, else the address itself, stakeless =
   terminal by shape) + a global receipt counter per staking credential;
2. a mint of the policy seeds the **signer** credential (native-script `sig`)
   and the **CIP-27 royalty** address (label 777) into the frontier, and records
   the ceiling (`before` slot);
3. asset events (`mint | transfer | burn`) from a holder map — no input
   resolution;
4. if the tx touches a watched party: resolve ALL inputs (buffer → `outref_cache`
   → Koios `/utxo_info`, write-through), build a `TxView`, take net deltas +
   pro-rata movements (`chain-ledger`), feed the **frontier** (any receipt from
   an expanding member promotes the receiver; stakeless / declared /
   custodial-scale members are frozen, never expanded), write `tx_delta` +
   `value_event` rows, buffer the outputs now held by members.

Checkpoint (cursor + frontier + buffer + activity + holders, one transaction)
every `--checkpoint-every` in-range blocks. The floor test at the end reconciles
distinct minted assets against Koios's list: equal → `floor_basis = observed`.

## Resolving inputs LOCALLY — do this, or receipts are unreliable

**A walk that cannot resolve its inputs cannot tell an incoming payment from the
wallet's own change.** Change is recognised in `book_unit_flows` by asking
whether an output goes back to a wallet that FUNDED the tx; with no resolved
inputs there are no known funders, so change is booked as a receipt from an
unnamed payer. That turns gross throughput into apparent income — on the first
Mekka walk, 415,508 outputs, and one script address reading 115M ₳ "received"
against a net of 305k.

The `outref_buffer` rung only holds **watched parties' own** outputs, so it
resolves a tracked wallet spending something the walk already saw and nothing
else. Every genuine inbound payment comes from a stranger and falls through it.

**The snapshot already has every one of those outputs** — an input references an
output in an earlier block of the same immutable DB. So the fix is local, and
runs as three steps:

```sh
# 1. walk — records what it could not resolve into `wanted_outref`
project-ledger walk --db mekka.db --data-dir /opt/market-ledger/snapshot-full …

# 2. read those refs straight out of the snapshot into `outref_cache`
#    (omit --from-slot to scan from genesis, which is the only setting that can
#    close the list completely; set it to measure cost on a bounded range first)
project-ledger resolve-local --db mekka.db \
  --data-dir /opt/market-ledger/snapshot-full --from-slot 150000000

# 3. re-walk — the ladder now finds them and the change rule works
project-ledger reset --db mekka.db --yes && project-ledger seed … && walk …
```

Step 2 reports `closed = have/wanted` and WARNS when refs remain: those point at
outputs created before the scanned range, and until they resolve a walk still
cannot classify those receipts. `outref_cache` is append-only, so the cost is
paid once and survives every later walk and rebootstrap — no indexer involved.

**Step 3's `reset` keeps the ledger FILE and clears only what a walk derives**,
because the cache lives in that same file: deleting it would discard the
snapshot scan immediately before the walk that exists to spend it, and the only
symptom would be change booked as income all over again. `--purge-cache` opts
into the old delete-the-file behaviour for a genuinely clean start.

Sizing: an input points at an output made earlier by an unknown margin (a wallet
may hold a UTxO for years), so no start slot short of genesis is provably far
enough back. `--from-slot` is a cost/coverage dial, not a correctness one.

## Not yet built

`classify` (kind projection: mint_payment / royalty / internal / deployment),
`enrich` (copy the policy's secondary sales in from market-ledger), `export`
(Parquet + manifest + SHA256SUMS via the duckdb CLI — the artifact),
`backfill`.
