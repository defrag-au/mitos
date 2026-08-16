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

## Not yet built

`classify` (kind projection: mint_payment / royalty / internal / deployment),
`enrich` (copy the policy's secondary sales in from market-ledger), `export`
(Parquet + manifest + SHA256SUMS via the duckdb CLI — the artifact),
`backfill`.
