# Platform v3 work orders

Each file here is ONE session's worth of work on
`docs/strategy/MITOS_PLATFORM_V3.md`. Read this page first, every session —
a work order assumes everything below and does not repeat it.

## What you are doing, in one paragraph

Every lifecycle DECISION in v3 is already made, implemented and exhaustively
tested in `crates/mitos-lifecycle` — pure reducers of the shape
`step(state, event, ctx) → Transition { next, effects }`. **You are not
designing anything.** A work order asks you to make the HOST
(`crates/mitos-platform`, `crates/mitos-core`) do one of two things: translate
something that happened into an `Event`, or EXECUTE an `Effect`. If you find
yourself writing an `if` that decides *whether* an instance should stop, a
client should move or a lease should expire, stop — that decision belongs in
`mitos-lifecycle`, and the right move is to report it (see "When to stop").

## Read before writing code

1. This page.
2. The work order, top to bottom, including "Out of scope".
3. `crates/mitos-lifecycle/src/lib.rs` (the pattern) and the module(s) the work
   order names. Read the module docs AND the tests — the tests are the spec.
4. The host files the work order names. Read them; do not grep-and-guess.

## Rules

- **Git is the user's.** Never `git commit`, `push`, `merge`, `stash`, `reset`
  or `checkout` a path. Leave everything uncommitted. Read-only git is fine.
- **Edit with the Read/Edit/Write tools.** Never `sed`, `awk`, `perl`, a
  heredoc or a python script to modify a file — not even for "just one line",
  not even for ten identical edits. A scripted replace silently does nothing
  when the anchor text has moved; that has shipped a real bug here.
- **Tests are the specification.** Never change an existing assertion to make
  it pass. If a test fails after your change, your change is wrong. The ONE
  exception is an assertion the work order explicitly lists under "Tests that
  encode the old behaviour", and then only in the way it says.
- **No behaviour change beyond what the work order states.** Each order is
  sliced so it can be deployed on its own. "While I was in there" is how a
  storage move becomes an outage.
- **Named decisions, not booleans.** No `bool` parameters, no `is_x()`
  predicates standing in for a decision — a named enum. No `serde_json::json!`
  — typed structs with `#[derive(Serialize)]`. Inline format args
  (`format!("{name}")`), everywhere.
- **Match the surrounding code**: its comment density, naming and idiom. This
  codebase comments WHY, at length, where a choice was non-obvious. Do the
  same; do not narrate WHAT.
- Let `cargo fmt` and `cargo clippy --fix` do the mechanical work.

## Commands (all through nix — `cargo` is not on the bare PATH)

```sh
nix develop -c cargo fmt
nix develop -c cargo clippy --fix --allow-dirty --all-targets --all-features -- -D warnings
nix develop -c cargo test -p <crate>
```

## Traps that have each cost a wrong conclusion here — do not rediscover them

- **Host tests call the real network by default.** `MITOS_FALLBACK_PROVIDER`
  defaults to koios. Construct hosts with `FallbackSource::Disabled` in tests,
  or a failure looks exactly like a regression in your change.
- **`nix develop -c` prints a banner to STDOUT.** Anything you capture or
  count from a nix-wrapped command includes `rust-worker-stack shell ready`.
  Filter it, or a grep over the output matches the banner.
- **`grep "^error"` misses cargo's coloured errors.** Pass `--color never`.
- **`grep` in the agent shell is a function with different semantics** and has
  returned EMPTY for strings that exist. Use `rg`. An empty search result
  proves nothing until you have confirmed the search could have matched.
- **A native `cargo check` is blind to `#[cfg(target_arch = "wasm32")]`
  modules.** Not an issue inside `mitos-platform`, but it is for anything you
  touch in `mitos-companion` — check those with
  `--target wasm32-unknown-unknown`.
- **The golden harness goes stale two ways.** A stale module `.wasm` fails
  loudly; a stale `mitos-run` PASSES SILENTLY (serde drops unknown fields).
  If a work order says "goldens must stay 69/69", rebuild `mitos-run` AND the
  modules before believing the number:
  `cargo build --release --bin mitos-run`, then `mitos-build` each module,
  then `./scripts/run-golden-tests.sh`.
- **redb is single-writer per process and per file.** Every redb open for a
  module-scoped file goes through `ModuleStorage` (clippy's
  `disallowed_methods` enforces it). Never call `redb::Database::create`
  elsewhere, and never move or delete a redb file while a handle is cached —
  close it through `ModuleStorage` first.
- **zsh does not word-split `$VAR`.** `cmd $ARGS` passes ONE argument. Use
  `${=ARGS}` or an array.
- **Verify the WORKSPACE, not a list of `-p` targets.** A signature change in
  `mitos-platform` can break a tool crate that no `-p` list names. Finish with
  `nix develop -c cargo check --workspace --all-targets`.

## Writing anything that changes files on disk in more than one step

Both bugs found reviewing WO-01 were of this one kind, in code that was
otherwise careful, so it gets its own section.

- **Enumerate the crash point between EVERY adjacent pair of writes**, not just
  the one the design talks about. WO-01's spec named "crash between the rename
  and the pointer write" and the implementation handled it perfectly — and
  there was a second window, between the rename and the marker write, that
  nobody had listed, which left a module unable to start. For N writes there
  are N−1 windows. Write them down, and write a test for each.
- **"Destination exists" must mean "destination is complete."** Any step that
  skips work because its output is already there is trusting that. So every
  such output is written to a sibling temp name and RENAMED into place — never
  written in place — or a crash mid-write is silently accepted next time.
- **A path that does not hold the WRITE lock must never write.** A recovery or
  "adopt it if it's missing" step that "just fixes it up" on the way to
  answering a READ races with every locked read-modify-write beside it, and
  the loser's transition is silently discarded. Derive the answer on read;
  persist it under the lock. This has now been found TWICE in otherwise careful
  code — WO-01's crash repair (under a shared read lock) and WO-03's
  `load_or_adopt` (called by the watchdog with no lock at all) — so treat any
  function named `load_or_*`, `get_or_create` or `ensure_*` as suspect: ask who
  calls it, and whether each of them holds the lock.
- **A state that REJECTS its own tick needs a retry path.** A terminal state
  whose cleanup effect can fail is never revisited by the normal event loop —
  the machine refuses the event. WO-03's half-finished `Reap` waited for a
  host restart. Give the machine a `pending_cleanup()`-style list of what is
  safe to re-issue on every tick, and keep it SEPARATE from boot recovery,
  which usually also restarts things.
- **Anything skipped or failed gets its own bucket in the summary.** A report
  of `migrated = 19` reads as healthy whether or not a twentieth was dropped.

## Executing an effect

Found reviewing WO-05, in code that got everything in the section above right.

- **Effects run under the state's lock, so an effect may never WAIT ON THE
  WORLD.** Bookkeeping, or START the work on its own task and have it report
  back later as an event, taking the lock only then. WO-05's executor awaited
  a download inline: a subscribe handler waited on it, the row lock was held
  throughout, and `boot` queued behind every unreachable worker. The tell is a
  test that can observe the work's OUTCOME the moment the request returns.
  Test it against a fake that is HELD, under a timeout, so a regression fails
  instead of hanging.
- **A cache hit skips only the checks about the BYTES.** "Already stored, so
  already verified" is true of digests and validity, and false of anything the
  CALLER just claimed about the artifact (its revision, which module it is
  for). Split the two kinds of check and run the second kind on both paths.
- **Recover from EVERY persisted stage, through the serialised form.** The
  pure crate's recovery test started each case from a late stage, so the one
  stage whose re-issued effect was missing a field was never recovered from.
  An effect must carry everything its executor needs; what it carries must be
  on the persisted state; and the test that proves it round-trips through disk.

## When to stop and report instead of continuing

- The work order's description of the code does not match what you find.
- Doing the work needs a DECISION the lifecycle crate does not already make.
- An existing test would have to change in a way the work order did not list.
- Anything would touch the production box, a deploy script's behaviour, the
  WIT (`wit-v2/world.wit`), or the wire types in `mitos-protocol` beyond what
  the order specifies.

Stopping with a precise question is a successful outcome. Guessing is not.

## Definition of done (every work order)

1. Everything under the order's "Acceptance" passes, and you RAN it — quote
   the command and the result line in your report.
2. `cargo fmt` clean; `cargo clippy … -- -D warnings` exits 0 for the crates
   you touched.
3. `nix develop -c cargo test -p mitos-lifecycle` still passes (you should not
   have needed to touch it).
4. No file outside the order's "Files" list changed, or each extra one is
   justified in the report.

## Report format

```
## WO-NN report
Done / Partially done / Blocked
### What changed          — file by file, one line each
### Acceptance            — each item: command + result line
### Deviations            — anything you did that the order did not say
### Found but not fixed   — adjacent problems, with file:line
### Questions             — only if blocked or unsure
```

## Index

| # | Title | Depends on | Status |
|---|---|---|---|
| 01 | Instance-keyed storage + content-addressed artifacts | — | **done**, reviewed 2026-09-19 — see its "Review" section |
| 02 | Namespaces: a token resolves to a principal | 01 | **done**, reviewed 2026-09-19 — no defects; `/replicate` settled as `Attempt::TailIndexer`. Needs `/security-review` before mainnet |
| 03 | Instance lifecycle wired — a persisted `LifecycleRecord` per instance; `LifecycleManager` replaces `auto_resume` and drives the watchdog; operator stop/start/pin/evict. Fixes an operator's stop being undone by a restart, by the watchdog, and by an upload. (Stays on the storage facade — host slots are still keyed by module id until two instances can exist.) | 02 | **done**, reviewed 2026-09-19 — 3 review fixes (a `Reap` that never retried; an unlocked read that could write; a loud diagnostic for under-counted adoption) |
| 04 | The manifest's `[lifecycle]` section (`revision`, `state_schema`, `[[lifecycle.upgrade]]`) — ONE table, stripped from `config.cbor` because the config's sha is instance identity; `mitos-build` emits `built-module.json` | — (parallel) | **done**, reviewed 2026-09-19 — no implementation defects; the wasm-sha question settled by construction; 2 review fixes (typos in `[lifecycle]` were silent — a gap in the pure types; a failed build left a stale `built-module.json`) |
| 05 | A worker announces its build on subscribe (digests only — NO url: the host derives the fetch location from the client id, and only for a client the operator named); persisted client rows; pull-by-digest into the CAS, incl. a manifest CAS keyed by MANIFEST digest. `client_move` runs to `Queued` and stops — observable, not behavioural | 03, 04 | **done**, reviewed 2026-09-19 — the reported blocker was a gap in the PURE crate (`Move` dropped the manifest pin; now persisted, and `url` removed from the pure types entirely) + 2 review fixes (a subscribe awaited the download under the row lock; a stored artifact skipped the checks about the announcement ⇒ rollback bypass) |
| 06 | The DATA plane is instance-addressed — storage handle caches, host slots, the follower's cursor, and the emission fan-out (`routing::fan_out`). A second instance of a module CAN run; nothing starts one. One instance ⇒ byte-identical behaviour. Shaped around `activate` RENAMING a live instance directory on every upload | 01, 03, 05 | **implemented 2026-09-20, awaiting review** — all acceptance met (goldens 69/69; the fan-out baseline passed BEFORE the change and still does). One behaviour change a test caught: a fan-out `Skip` is not "no subscriber", so the instance that does not serve a client must not sentinel its event. See its "Implemented" section |
| 07 | The CONTROL plane is instance-addressed — `LifecycleManager` + `lifecycle.toml` per instance (tick over instances, not modules), one dialer drain per instance, instance-aware evict; CLIENT-scoped recapture (see WO-02 on why it is whole-module today) | 06 | not written |
| 08 | An instance is CREATED — the catch-up slot queue (`next_catch_up`), `EnsureInstance`, `state_prep` executed (copy with the parent paused / rebuild), instance-scoped upgrade actions; a row whose `current` names a renamed-away instance is re-adopted before a move is planned. `client_move` runs to `CatchingUp` | 07 | not written |
| 09 | The switch — `FanOut::Hold` + the advisory diff, pause both at a boundary, drain-or-purge, `CommitSwitch` (THE atomic write) + `ResolveCommit`, attach/detach ownership moves from `reconcile` to rows, scoped refill, the Apply frame carries the instance digest | 08 | not written |
| 10 | Client lease + probe | 05 | not written |
| 11 | GC — CAS refcounts, `orphaned-v2/`, retained instances; delete `auto_resume` | 03 | not written |
| — | D2 semver ABI gate; D11 reporting + MCP | independent | not written — the WIT split is designed by hand, not delegated |

## Writing a work order (for whoever writes the next one)

Learned the hard way on 01 and 02, where the FIRST draft of each contained a
claim that was wrong and would have shipped a bug or an outage:

- **Verify every "this is safe" / "nothing else uses this" sentence against
  the code before handing the order over.** WO-01 said a live directory rename
  was safe; seven implicit `create_dir_all` calls said otherwise. WO-02 mapped
  `/minibf` to the admin API; a worker that is not even a subscriber calls it
  with the shared secret.
- **Take a census of CALLERS, not just of the code being changed** — across
  every repo, and in config files as well as source. WO-02's first pass over
  `.rs` files found 8 holders of the mitos token and missed 6 that hold it
  only through a `wrangler.toml` binding.
- **Put the decision in `mitos-lifecycle` first.** If the order needs a rule
  ("may a dev token recapture?"), write it as a pure function with a decision
  table BEFORE writing the order. The order then maps routes and nothing else.
- **Pin every example with a test.** `WO-02-namespaces.example.toml` is parsed
  by a test in the lifecycle crate, so it cannot drift from the types.
