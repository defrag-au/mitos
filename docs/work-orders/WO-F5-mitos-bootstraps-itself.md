# WO-F5 — `mitos bootstrap`: one Mithril fetch, one chunk store, no second binary

**Design refs:** `docs/design/FOOTPRINT_AND_MINIMUM_SPEC.md` §10 ("a first-run
path that does not need our operational knowledge"), §7 (disk is what prices
people out), §8 (resumable per chunk).
**Lifecycle crate:** nothing. No lifecycle decision is made here.

**Depends on:** nothing. **Blocks:** nothing — a FAST FOLLOW. The ARM box is
being brought up on the old path (a standalone `dolos` binary); this replaces
that path, it does not gate it.

## Why

`crates/mitos-core/src/domain.rs` says it plainly: *"Mitos doesn't run the
bootstrap itself yet — invoke `dolos bootstrap mithril ...` first."* So a
fresh host today needs:

1. a standalone **`dolos`** binary, at a version matching the one mitos embeds,
   used ONCE — to download a Mithril snapshot into `/opt/mitos/<net>/snapshot`,
   import it, and then **delete it**; and
2. a standalone **`mithril-client`** CLI (0.13.9), used by
   `infra/services/market-ledger/snapshot-refresh.sh` to maintain
   `/opt/market-ledger/snapshot-full/db/immutable` — the ~217 G chunk store
   `tx-index`, `token-ledger` and `market-ledger` all read. That script is
   INCREMENTAL ONLY ("no existing chunks — run a full bootstrap first"); the
   full first fetch is not scripted anywhere.

That is the same ~217 G downloaded twice, by two binaries from two other
projects, one copy thrown away, and one step that exists only in somebody's
head. Every part of it is operational knowledge a self-hoster does not have.

## What this order does, and where it deliberately stops

`mitos bootstrap` fetches a Mithril snapshot ONCE into the shared chunk store,
verifies it, imports it into mitos's embedded-dolos data dir, and leaves the
store where every other tool already expects it. No `dolos` binary.

It stops at the FIRST fetch. The weekly incremental refresh keeps using
`snapshot-refresh.sh` and the `mithril-client` CLI: the crate version already
in the lockfile (`mithril-client 0.12.2`, via dolos) may not expose a ranged
download, and finding out is part of this order's report, not its scope.

## What is already there — read before writing anything

- The reference implementation: dolos v1.2.0,
  `src/bin/dolos/bootstrap/mithril.rs` (~300 lines; it lives in dolos's
  BINARY, which is why mitos cannot simply call it). Read all of it.
- The IMPORT half of it uses only library APIs mitos already links:
  `pallas::interop::hardano::storage::immutable::{get_tip,
  read_blocks_from_point}`, `dolos_core::ImportExt::import_blocks`, and a
  `setup_domain` that `mitos-core/src/domain.rs` already replicates. It is
  RESUMABLE: it starts from the state store's cursor, and skips the block the
  cursor is standing on.
- `mithril-client` is ALREADY in `Cargo.lock` at 0.12.2 (dolos's default
  features include `mithril`). This order adds a direct dependency on a crate
  the workspace already compiles — on aarch64 too, since the v3 Appendix B
  benchmark built there. Pin the SAME version; do not let cargo pick a second.
- `RootConfig.mithril` (`MithrilConfig { aggregator, genesis_key,
  ancillary_key }`) is already in every `dolos.toml` under
  `infra/services/mitos/`.

**Do not copy upstream's validation bug.** Upstream reads:

```rust
if !args.skip_validation {
    warn!("skipping validation, assuming snapshot is already validated");
    return Ok(());
}
```

The condition is inverted: by default it verifies the certificate CHAIN and
then returns before checking the downloaded FILES against the certificate
(`compute_snapshot_message` / `match_message`). Ours validates by default, and
`assert!` on a mismatch becomes a typed error.

## Files

- `crates/mitos-core/src/bootstrap.rs` — NEW: the plan, the fetch, the import.
- `crates/mitos-core/Cargo.toml` (+ workspace) — `mithril-client`, pinned.
- `bundles/default/src/main.rs` — the subcommand.
- `crates/mitos-core/src/domain.rs` — the doc comment that says mitos cannot.
- `docs/operations/…` or wherever first-run is documented in THIS repo, and
  `docs/design/FOOTPRINT_AND_MINIMUM_SPEC.md` §10.

## The work

### 1. The CLI — and `mitos` with no subcommand must keep meaning "run"

Every installed unit's `ExecStart` is the bare binary with environment
variables. So: an OPTIONAL subcommand. `mitos` = run, exactly as today, with
the same flags and env names. `mitos bootstrap` = this order. A test pins
that the no-subcommand parse is unchanged.

```
mitos bootstrap --config <dolos.toml> --chunk-store <dir>
                [--fetch if-empty|never] [--chunk-size 500]
```

`--chunk-store` (env `MITOS_CHUNK_STORE`) is the directory that CONTAINS
`immutable/` — i.e. `/opt/market-ledger/snapshot-full/db` on our boxes. There
is no default: a bootstrap that guesses where 217 G goes is not a feature.

### 2. The plan is a pure function, decided before anything is touched

```rust
pub enum Store { Empty, HasChunks { newest: u32 } }
pub enum DataDir { Fresh, PartlyImported { cursor_slot: u64 }, AlreadyBootstrapped }
pub enum Fetch { IfEmpty, Never }

pub enum Step { FetchAndVerify, Import { from: ImportFrom }, Nothing }
pub enum Refused { StoreEmptyAndFetchForbidden, /* … */ }

pub fn plan(store: Store, data: DataDir, fetch: Fetch) -> Result<Vec<Step>, Refused>;
```

- `Empty` + `IfEmpty` ⇒ fetch, verify, import. `Empty` + `Never` ⇒ refused.
- `HasChunks` ⇒ import only — whoever put the chunks there verified them, and
  this is the path our boxes take once the store exists. NEVER re-download
  over an existing store.
- `PartlyImported` ⇒ resume the import from the cursor (a crash, or a small
  box that ran out of time — footprint §8).
- `AlreadyBootstrapped` (cursor at or past the store's tip) ⇒ `Nothing`, exit 0.
  Bootstrapping twice is not an error; it is an idempotent script.
- Named enums throughout; a whole-table test.

### 3. The fetch

- `mithril-client` refuses to unpack into a non-empty directory (the refresh
  script's comment records this for 0.13.9; confirm for 0.12.2). So fetch
  into a scratch directory ON THE SAME FILESYSTEM as the store, verify, then
  RENAME into place — the README's rule: "destination exists" must mean
  "destination is complete". A scratch directory left by a crash is deleted
  at the start of the next run, loudly.
- Verify the certificate chain AND the downloaded files against the
  certificate, by default.
- Check free space BEFORE starting: the aggregator reports the snapshot's
  size; refuse with both numbers if the filesystem cannot hold it plus
  headroom. A bootstrap that dies at 95% on a cheap VPS is the first
  impression we are trying not to make.
- Progress to `tracing` at a sane cadence (this runs under `nohup` and
  systemd, not a TTY): bytes and percent every ~30 s, not a progress bar.

### 4. The import

Port `do_import` / `import_hardano_into_domain`. Keep: batches of
`--chunk-size`, `Arc`-wrapped blocks, and `domain.shutdown()` on EVERY path
out. Log slot and percent-of-tip every ~30 s.

**It never deletes the store.** Upstream deletes its download directory
unless `--retain-snapshot` is passed; there is no such flag here because
there is no such behaviour. There is a test whose whole job is to fail if
anything under `--chunk-store` is removed.

A running mitos on the same data dir holds the redb lock, so the import fails
to open the stores. Turn that into one clear line — "is mitos-<net> running?"
— rather than a redb error.

### 5. Docs

- `domain.rs`'s comment, and the `--config` help text in `main.rs`, stop
  saying to run dolos first.
- Footprint doc §10: the first-run path is now one binary; record what is
  still NOT (the incremental refresh, and whatever you found about
  `mithril-client 0.12.2` and ranged downloads).

## Out of scope — do not do these

- Replacing `snapshot-refresh.sh` or the `mithril-client` CLI. Report whether
  0.12.2 can do a ranged (`--start N`) download; do not build it.
- Bumping `mithril-client`, or dolos.
- A remote / pruned chunk store (footprint §7's `lean` profile).
- Anything in `infra`. The deploy note says what changes there.
- Preprod-specific handling beyond "it works with preprod's `dolos.toml`".

## Tests that encode the old behaviour

None.

## Acceptance

1. `nix develop -c cargo test -p mitos-core -p mitos`.
2. clippy `--workspace --all-targets -D warnings` 0;
   `cargo check --workspace --all-targets` 0; `cargo fmt --check` clean.
   `Cargo.lock` still has exactly ONE `mithril-client`.
3. `nix develop -c env NIX_HARDENING_ENABLE="" cargo check -p mitos-protocol -p mitos-companion --target wasm32-unknown-unknown`
   still exits 0 — a new dependency in `mitos-core` must not leak into the
   crates workers compile.
4. `bootstrap.rs`:
   - `the_whole_plan_table`.
   - `the_no_subcommand_invocation_parses_exactly_as_before` (in the bundle).
   - `an_existing_store_is_imported_from_and_never_fetched_over`.
   - `nothing_under_the_chunk_store_is_ever_removed` — snapshot the tree
     before and after an import (and after a FAILED import).
   - `a_scratch_directory_left_by_a_crash_is_cleared_not_trusted`.
   - `an_import_resumes_from_the_cursor_and_does_not_import_a_block_twice`.
   - `bootstrapping_twice_is_a_no_op`.
   - `a_data_dir_that_is_in_use_says_so_in_plain_words`.
   - `too_little_disk_is_refused_before_a_byte_is_fetched`.
   The fetch sits behind a trait (as WO-05's `ArtifactFetcher` does) so none
   of these touches the network; the import tests use a small immutable-db
   fixture — if the repo has none, say so and propose one rather than
   inventing a format.
5. Goldens 69/69.
6. NOT RUN is an acceptable result for "a real mainnet bootstrap" — say so
   plainly. That happens on the ARM box, by the operator.

## Deploy note (for the operator — NOT part of this order)

After this lands, `infra/services/mitos/bootstrap.sh` becomes
`mitos bootstrap --config dolos.toml --chunk-store /opt/market-ledger/snapshot-full/db`,
and `services/dolos/provision.sh` is no longer needed on a mitos-only box.

**Until it lands, the same saving is available with no code at all.** dolos
already takes the flags:

```sh
dolos bootstrap mithril --config dolos.toml \
  --download-dir /opt/market-ledger/snapshot-full/db \
  --skip-download --retain-snapshot
```

— fetch the chunk store ONCE (the `mithril-client` CLI, full download), then
import from it. **`--retain-snapshot` is not optional there: without it dolos
DELETES `--download-dir` when the import finishes — which would be the shared
chunk store.**
