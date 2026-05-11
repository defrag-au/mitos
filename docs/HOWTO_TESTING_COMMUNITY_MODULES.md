# HOWTO: testing community modules against real chain data

End-to-end loop for proving a community module's chain-recognition
behaviour against real mainnet blocks, before deploying. Surfaces
wire-shape bugs the moment they happen rather than hours later in
production logs.

The full pipeline:

1. Pick a TX that exercises the module's path
2. Pull the containing block from production mitos
3. Author a fixture (interest predicates + scenario dir)
4. Run `mitos-run` once with `UPDATE_GOLDEN=1` to capture expected emissions
5. Commit the fixture + expected JSON
6. From then on, `scripts/run-golden-tests.sh` re-asserts on every change

## Fixture layout

One scenario per directory:

```
community-modules/<name>/tests/fixtures/<scenario>/
  fixture.toml          interest predicates + (optional) hand-authored UTxOs / tx_metadata
  expected.json         JSON array of decoded emissions in emission order
  <slot>.block.cbor     one or more blocks; filenames are slot numbers,
                        sorted = chain order
  README.md             (optional) scenario provenance — TX hash, why
                        this scenario was picked, what it proves
```

Scenario directories isolate each fixture so blocks for one
scenario don't bleed into another. Single-scenario modules have
one subdir; multi-scenario modules (e.g. `cip-25-mint` with the
single-asset perp + 4-asset UG scenarios) have one subdir per
scenario.

## Step 1 — find a TX

Choose by what the module catches:

| Module | Pick a TX with… |
|---|---|
| `cip-25-mint` | positive `tx.mint` entry + label-721 metadata for that asset |
| `cip-68-mint` | paired `_100` + `_222`/`_333`/`_444` mints under one policy |
| `standard-burn` | negative `tx.mint` entry on the policy you care about |
| `burn-address` | output sending an asset to a known burn-sink address |
| `asset-metadata-update` (Cip68) | TX with **zero mints** that consumes a `_100` reference UTxO and produces a new one with a different datum |
| `asset-metadata-update` (Cip25) | TX with negative or net-zero mint + label-721 metadata (the burn-with-metadata hack — rare post-CIP-68) |

Maestro's `/policy/{policy}/transactions?order=desc` returns the
recent TX list per policy; pair with `/transactions/{hash}` to
inspect mints. For burn-address you'll need a TX that sent an asset
to your watched bech32 — fish via cardanoscan or your wallet history.

## Step 2 — pull the block

Production mitos exposes `GET /_admin/blocks/by-tx/{tx_hash}`. The
response carries the slot in `X-Mitos-Block-Slot`:

```bash
TOKEN=$(ssh root@159.195.57.187 'grep ^MITOS_AUTH_TOKEN= /etc/default/mitos-mainnet | cut -d= -f2')

ssh root@159.195.57.187 "curl -sS -D /tmp/h.txt \
  -H 'Authorization: Bearer $TOKEN' \
  http://127.0.0.1:8181/_admin/blocks/by-tx/<tx_hash> -o /tmp/block.cbor \
  && grep -i x-mitos-block-slot /tmp/h.txt"

mkdir -p community-modules/<name>/tests/fixtures/<scenario>
scp root@159.195.57.187:/tmp/block.cbor \
    community-modules/<name>/tests/fixtures/<scenario>/<slot>.block.cbor
```

**Archive horizon caveat**: the production dolos archive only
retains the most recent year or so of blocks. Older TXs return a
404 with `tx hash resolved to slot N but archive has no block at
that slot`. See `reference_mitos_archive_horizon.md` in the
project memory for details. For scenarios needing older blocks
you'll need Blockfrost's `/blocks/{hash}/cbor` or similar.

## Step 3 — author the fixture TOML

Minimal example for a `holds_policy` interest:

```toml
# community-modules/<name>/tests/fixtures/<scenario>/fixture.toml
version = 1

[[interest]]
kind = "holds_policy"
policy = "<56-char hex>"
```

For `burn-address`:

```toml
[[interest]]
kind = "at_address"
address = "addr1w..."
```

You can also hand-author `[[utxo]]` rows (for outputs the
dispatcher needs to resolve but aren't in any loaded block) and
`[[tx_metadata]]` entries (for TXs whose aux-data lives outside
the dispatched blocks). In practice, `mitos-run` auto-harvests
both from `--block` arguments, so you only need explicit fixtures
for adversarial / synthetic test cases.

## Step 4 — capture expected emissions

```bash
UPDATE_GOLDEN=1 ./target/release/mitos-run \
  --artifact community-modules/<name>/target/mitos/<name> \
  --fixture community-modules/<name>/tests/fixtures/<scenario>/fixture.toml \
  --block community-modules/<name>/tests/fixtures/<scenario>/<slot>.block.cbor \
  --expected community-modules/<name>/tests/fixtures/<scenario>/expected.json
```

The first run with `UPDATE_GOLDEN=1` writes the file with the
actual emissions. Inspect it. If it's correct, commit it. If it's
wrong, fix the module and regenerate.

## Step 5 — assert from now on

Run the full suite from the repo root:

```bash
./scripts/run-golden-tests.sh
```

Walks every `community-modules/*/tests/fixtures/*/` and asserts
each scenario's emissions match its `expected.json`. Exit code
non-zero on any failure. CI-friendly.

To accept an intentional change (e.g. you renamed an event field):

```bash
UPDATE_GOLDEN=1 ./scripts/run-golden-tests.sh
git diff community-modules/*/tests/fixtures/*/expected.json
# Review carefully, then commit
```

## What this catches

Every bug surfaced during the initial build-out of the mint/burn
module family was caught by this loop, **before** any module ever
ran against live chain events:

1. `parse_cip67` reading the wrong bits — the CIP-67 label is
   nibble-aligned across bytes 0-3, not stored in bytes 1-2.
2. `chain_data::datum_by_hash` host fn hardcoded to return `None`
   with a TODO comment.
3. CBOR tag-259 wrapper handling missing from CIP-25 metadata
   extraction (Alonzo+ aux-data wraps the metadata map in a tag,
   not the Shelley-era array form).
4. `burn-address` assuming the platform filters per-output for
   `at_address` interest. It filters per-TX — the module has to
   filter per-output itself.
5. Non-deterministic emission order — `cip-68-mint` iterated a
   `HashMap` to flush pairs, so emission order varied between
   runs. Fixed by switching to `BTreeMap`.

Plus 4 mitos-run test-infra unlocks (aux-data harvest, witness-
datum harvest, UTxO harvest, `update_interest` invocation).

## Auto-deployment

When you commit a new fixture, run `./scripts/deploy.sh` to push
the host changes to production. The deploy:

1. Rsyncs source (excluding `target/`, `.git/`, `node_modules/`)
2. Builds mitos + mitos-build
3. Walks every `community-modules/<name>/` and runs `mitos-build`
   for each (skipping modules whose wasm is newer than source)
4. Restarts `mitos-mainnet`
5. Polls `/health` with a 30s window

Auto-load picks up the new artifacts via sha check (idempotent) —
no manual `mitos-admin upload-module` needed. Modules that fail to
build are logged + skipped; the rest activate normally.

## Related

- `docs/design/MINT_BURN_MODULES.md` — module family design
- `docs/strategy/COMMUNITY_MODULES.md` — auto-load mechanism
- `crates/mitos-platform/src/admin.rs` — block-fetch admin route
- `tools/mitos-run/src/main.rs` — local test runner + golden assertion path
- `scripts/run-golden-tests.sh` — assertion driver
- `scripts/deploy.sh` — host + community-module deploy pipeline
