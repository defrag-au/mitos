# HOWTO: debug a wasm module trap

Modules trap. They run out of fuel, they hit a panic in a CBOR
parser, they index past the end of an array, they pull on real
on-chain data the test fixtures didn't anticipate. Without
tooling, the symptom is unhelpful: the module disappears from
the host's running set and re-uploads keep failing with cryptic
wasm backtraces.

This doc walks through the **fault-finding process** the
platform supports today: capture the failing data plane state
on the host, fetch it as a fixture, replay locally with full
debug symbols, then iterate without touching production. It is
the path to use when a deployed module won't `init` or its
`handle_event` traps on a specific block — every step below was
exercised end-to-end on the jpg-co bootstrap trap of 2026-05.

The process is designed to need no input from a senior
operator: a fresh context can follow this top-to-bottom and get
to a local repro in minutes. The fix at the end is yours; the
infrastructure to *find* the fix is checked in.

Cross-references:
- `crates/mitos-platform/src/trap_context.rs` — the in-host
  data-plane logger that captures fixtures
- `tools/mitos-run/` — the local replayer
- `strategy/MITOS_PLATFORM_V1.md` — what's being instantiated
- `HOWTO_FIRST_MODULE.md` — the shape of a module before it
  goes wrong

## Prerequisites

- A mitos host that's running and reachable (`/health` returns
  `200`). You don't need SSH for trap diagnosis — only for
  redeploying fixes.
- `MITOS_AUTH_TOKEN` for the host's admin endpoints (the same
  one `mitos-build` and `mitos-admin upload-module` use).
- The same artifact you uploaded: the `<module-id>.wasm` and
  `manifest.toml` from `mitos-build`'s output directory.
  Symbols must be intact — modern `mitos-build` ships
  `debug = "line-tables-only"` in the release profile so
  function names appear in trap backtraces.
- A local checkout of the mitos repo (this repo). `mitos-run`
  is the workspace member that does the replay.

## Step 1 — Confirm the trap

Hit the restart endpoint to surface the error inline. The host
returns the wasm error including the backtrace:

```bash
curl -sS -X POST \
    -H "Authorization: Bearer $MITOS_AUTH_TOKEN" \
    https://mitos.defrag.cc/_admin/modules/<id>/restart \
    | jq .
```

A trapping module returns:

```json
{
  "error": "wasmtime: host.replace: wasmtime: error while executing at wasm backtrace:\n    0:   0x69c6 - jpg_co_module.wasm!jpg_co_module::scan_address::hd6cfeb7dc2e04b10\n    1:  0x14537 - jpg_co_module.wasm!init\n",
  "code": "wasm_invalid"
}
```

This tells you the trap is live and where it surfaces, but not
*why* — you need the fixture for that.

## Step 2 — Pull the trap fixture

Every time `init()` (and, in a follow-up, `handle_event()`)
returns a wasm error, the host's `TrapContextLogger` snapshots
every host-fn call the module made and serialises it to
`<modules-dir>/<id>/last-trap.toml`. Pull it:

```bash
curl -sS -H "Authorization: Bearer $MITOS_AUTH_TOKEN" \
    https://mitos.defrag.cc/_admin/modules/<id>/last-trap \
    -o /tmp/<id>-last-trap.toml
```

The file is `mitos-run --fixture` compatible TOML:

```toml
version = 1

[[utxo]]
tx_hash = "..."
index = 0
address = "addr1..."
lovelace = 75000000
datum_hash = "..."
datum_payload_hex = "..."  # absent if host couldn't resolve

# ... one entry per UTxO the host returned to the module ...

[[tx_metadata]]
tx_hash = "..."
aux_cbor_hex = "..."

# ... one entry per tx the module asked aux-data for ...
```

A 7 MB file with thousands of entries is normal for a
bootstrap trap — the logger captures everything seen up to the
trap. If the file is much smaller than you expected, the trap
fired earlier than the host got to populate the surface you
care about.

## Step 3 — Replay locally

Build `mitos-run` once (incremental from there):

```bash
cargo build --release -p mitos-run
```

Run the captured fixture against the artifact you uploaded:

```bash
cargo run --release -p mitos-run -- \
    --artifact path/to/<your-worker>/modules/target/mitos/<id> \
    --fixture /tmp/<id>-last-trap.toml
```

Output is verbose by default — every `logging::log` call from
the module surfaces, plus emissions, plus the full wasm
backtrace on trap. Filter with `RUST_LOG=info` env var if it's
too much.

A trapping module produces:

```
✗ init() failed:
error while executing at wasm backtrace:
    0:  0x1bd91 - jpg_co_module.wasm!minicbor::decode::decoder::Decoder::read_slice
    1:   0xa787 - jpg_co_module.wasm!minicbor::decode::decoder::Decoder::str
    2:   0x7791 - jpg_co_module.wasm!jpg_co_module::parse_metadata_datums
    3:   0x68d5 - jpg_co_module.wasm!jpg_co_module::scan_address
    4:  0x14537 - jpg_co_module.wasm!init

Caused by:
    wasm trap: all fuel consumed by WebAssembly
```

This is qualitatively different from production output. The
`Caused by:` line is the actual trap kind — fuel exhaustion,
unreachable instruction, out-of-bounds index, etc. The
backtrace names every frame down to the source crate function.
This is the diagnostic you couldn't get from production.

## Step 4 — Read the trap correctly

Wasm traps with `panic = "abort"` (mitos-build's default) lose
the panic message. The trap *kind* is what `wasmtime` reports;
the *cause* you infer from the backtrace + your knowledge of
the module.

Common kinds and how to recognise them:

- **`all fuel consumed by WebAssembly`** — the module ran out
  of compute budget. Almost always means the module is doing
  too much work in one host call, not that the work is
  inherently expensive. Solution is usually chunking
  (process N items per call, persist progress in `state-kv`,
  resume on the next call), not raising fuel limits.
- **`wasm trap: unreachable`** — a Rust panic compiled to a
  wasm `unreachable` instruction. The backtrace points at the
  `unwrap`/`expect`/index-out-of-range that fired. With
  release builds you don't get the panic message; the location
  is the lead.
- **`wasm trap: integer overflow`** / **`out of bounds memory
  access`** — pointer-arithmetic class issues, normally in
  `unsafe` blocks or generated code. Rare for high-level
  module code.

The single most common diagnostic mistake is reading the trap
*leaf* as the bug. A fuel trap shows you wherever the module
happened to be running when fuel hit zero — moving a defensive
validation upstream just moves the trap deeper into the next
host fn. Always confirm the trap *kind* (the `Caused by:` line)
before reasoning about logic.

## Step 5 — Iterate locally

The fixture is a static replay — same input bytes every time.
Once you've reproduced the trap:

1. Edit the module source.
2. `mitos-build` it (or use the `scripts/build-module.sh` from
   the consumer repo if you have one).
3. Re-run `mitos-run --fixture`.

No production redeploy until you've got it green locally. The
fixture *is* the regression test: commit it to the consumer
repo's `modules/fixtures/` once you've fixed the issue, so a
future bug in the same shape doesn't ship past CI.

When the local run reports `✓ init() returned cleanly`,
push the module artifact upstream the normal way (`mitos-admin
upload-module --artifact <dir>` from your laptop, or the
consumer repo's deploy script if it wraps that).

## What this doesn't cover yet

- **`handle_event` traps**: the trap-context logger captures
  data-plane calls during dispatch too, but the per-block CBOR
  capture and `mitos-run --block` replay are follow-up work.
  Today, dispatch traps surface as wasmtime errors in the
  follower's logs without an automatic fixture dump. When this
  lands, the same workflow applies — fetch fixture, run with
  `--block`, repro locally.
- **Cross-block lookups**: the fixture only captures host-fn
  calls the module made in the trapping pass. A module that
  calls `read_utxo` for a tx the host didn't surface in the
  bootstrap won't have that data in the fixture. For now,
  add the missing data to the fixture by hand or capture from
  Maestro and append.
- **Native panics in the host**: this workflow debugs *guest*
  traps. Host-side panics show up in the systemd journal —
  use `ssh root@<box> 'journalctl -u mitos-mainnet -n 200'`.

## Reference: full reproduction of the jpg-co fuel trap

Concrete worked example you can mirror for a fresh trap:

```bash
# 1. Confirm trap
curl -sS -X POST -H "Authorization: Bearer $MITOS_AUTH_TOKEN" \
    https://mitos.defrag.cc/_admin/modules/jpg-co/restart

# 2. Pull fixture (~7 MB on a bootstrap trap)
curl -sS -H "Authorization: Bearer $MITOS_AUTH_TOKEN" \
    https://mitos.defrag.cc/_admin/modules/jpg-co/last-trap \
    -o /tmp/jpg-co-last-trap.toml

# 3. Replay with the artifact you uploaded
cd ~/code/defrag/mitos
cargo run --release -p mitos-run -- \
    --artifact ~/code/defrag/cnft.dev-workers/workers/jpg-store-mirror/modules/target/mitos/jpg-co \
    --fixture /tmp/jpg-co-last-trap.toml

# 4. Read the trap kind from the `Caused by:` line.
#    For jpg-co this was: "all fuel consumed by WebAssembly"
#    after 1582 emitted events — fuel exhaustion mid-bootstrap,
#    not a logic bug. Fix is chunked-bootstrap on the module
#    side, not bumping `init_fuel`.
```

Total time from "module trapping in production" to
"reproducing locally with full backtrace": under five minutes.
That's the bar; if you find yourself spending longer, file
something into `tools/mitos-run` to make the next round faster.
