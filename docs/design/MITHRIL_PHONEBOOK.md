# Mitos as a phonebook of the Mithril log

**Status: SKETCH** (2026-09-22). Not a work order. Nothing here is scheduled
and nothing here should start on the ARM box that is currently proving the
dolos-shaped bootstrap path.

Owner: TBD
Related:
- `docs/design/FOOTPRINT_AND_MINIMUM_SPEC.md` — §7 “how far can dolos be
  pruned?”, §8 unified walker constraints
- `docs/design/INSTALLABLE_MITOS.md` — §4 import is the slow part; I5 ships
  an already-folded book
- `docs/design/TX_INDEX_DATASOURCE.md`, `crates/tx-index/` — the projection
  pattern this note generalises
- `docs/work-orders/WO-F5-mitos-bootstraps-itself.md` — today’s importer
  *is* dolos `import_blocks`

## 1. Forcing observation

Mithril’s cardano-db snapshot is a **log**: sealed `NNNNN.chunk` files,
slot order, a sidecar that maps slot → block offset, and nothing keyed by
tx hash, address, or policy. Walking it is cheap and parallel — `tx-index
build --threads 8` is decode-bound, tens of minutes for all of mainnet.

What we actually serve — to modules, to Cloudflare workers, to ourselves —
is a **phonebook plus a change feed**: given a name (tx hash, output ref,
policy, address), where is it, is it still live, and what just changed.

Dolos import is a different job that happens to use the same bytes as
input. It reconstructs a **node-shaped store** (full ledger, archive copy
of the log, dolos indexes, WAL) so the process can validate and submit.
We have been treating that store as the phonebook. Most of it is not.

The live UTxO set is invaluable and stays. Being a Cardano node does not.

## 2. What the 145 G is

Production dolos data dir, from FOOTPRINT:

| Piece | Size | Role |
|---|---|---|
| **state** | ~4 G | UTxO + entities. The book. |
| archive | ~93 G | a second copy of the log we already keep as ~217 G of chunks |
| index | ~39 G | dolos’s own address/tx indexes |
| wal | ~11 G | follow undo log (grows after import) |

Per block, `import_blocks` does not just spend/produce outputs. It crawls
accounts, assets, datums, DReps, epochs, pools, proposals, tx logs, writes
archive + index + state, then at epoch boundaries walks accounts in 32
**sequential** shards (`ACCOUNT_SHARDS`). Those shards are crash-resume
slices, not a thread pool. `cache = 500` in `dolos.toml` is **500 MB** of
fjall cache over a 4 G state keyspace — random `load_utxos` / `commit`
against an eighth of the working set.

Submit does not justify this. `services/tx-submit` is already Koios-first,
mitos fallback, because local phase-1/2 false-rejects Babbage script refs.
A submitter is a TxSubmission client talking to a relay, not a 147 G ledger.

## 3. Two ceilings

**Sequential apply is inherent for a live UTxO set.** Block N+1 depends on
N. A UTxO folder will never look like `tx-index --threads 8`. You cannot
outsmart consensus order.

**Dolos is close to the ceiling for a full ledger. It is not close to the
ceiling for this phonebook.** A purpose-built folder:

- reads the log sequentially (the chunks stay the source of truth);
- holds the live set in memory for the *build* (naive
  `HashMap<TxoRef, EraCbor>` is 2–3× the 4 G packed-on-disk figure — fine
  on 32 G if that is *all* you hold, not fine next to archive/index
  writers);
- checkpoints at epoch grain (that is already when dolos moves the cursor);
- dumps a packed mmap file and serves from page cache, same as tx-index —
  RAM is for the fold, not a permanent 8–10 G anonymous heap;
- follows the tip as a small delta.

That is still hours of one-core apply, not tens of minutes. The only way
the fold becomes an hour is **not folding**: copy an already-folded book
(INSTALLABLE_MITOS I5, or a ledger-state snapshot).

The other phonebook volumes (tx-index, policy-index, ledgers, walker
extractors) *can* skip order. They stay projections of the log. FOOTPRINT
§8 already constrains the unified walker: mmap-friendly, bounded RSS,
`--threads`, Nice/idle, resumable per chunk.

Workers never open a chunk. They ask the book and they subscribe to diffs
(catalogue → R2/KV is already that pattern).

## 4. Keep / drop

| Keep | Drop or shrink |
|---|---|
| Chunk store + Mithril refresh (the log) | dolos archive (~93 G rewrite of the log) |
| Live UTxO set (~4 G, or a compact mmap of it) | Full CardanoLogic entity graph, 32-shard epoch walks |
| tx-index / policy-index / ledger walks | Local mempool, N2C, minibf-as-node |
| Auth, modules, worker-facing surface | Modules that *must* subscribe to dolos `TipEvent` — those become walker extractors |
| Thin tip follow (immutable lags by *k*) | Import-as-live-follow-engine (`import_blocks` under `write_chain`) |

`lean` / `standard` / `full` in FOOTPRINT stay: they are how much of the
*log* you hold locally, not whether you materialise a node next to it.

## 5. Evidence, not this box

2026-09-21, ARM (`v2202604354294454929`, 12× aarch64, 24 G, Debian 13):
musl `mitos bootstrap --fetch never` against the existing 217 G store.
Import is one core (~120% CPU) for the whole run; RSS walked off 24 G and
onto 32 G of swap by ~25% of tip (~50 M / 198 M slots) after ~9 hours.
Disk was never the constraint. This SKU is a fine disk host for chunks and
parallel extractors. It is a bad host for a node-shaped fold.

Do not start a UTxO-only importer on that box, and do not kill the running
dolos import to make room for one. That run is a proof of the *node*
install path. This note is the other path.

## 6. Open

- Compact on-disk / mmap layout for the live set (tx-index is the template;
  full `EraCbor` per output is the lazy default).
- What of dolos state besides UTxO do we actually query (pparams, accounts,
  pools)? Measure before keeping entity visitors.
- Tip window: headers-only follow vs a small dolos vs someone else’s API,
  for “did this land in the last *k* blocks”.
- Submit: keep Koios/Maestro, or a tiny TxSubmission process; not a reason
  to keep `Domain::receive_tx`.
- I5 vs a from-chunks in-memory folder: I5 wins wall clock; the folder
  wins “we do not have to trust a published state dir”.
