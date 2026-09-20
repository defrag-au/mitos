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
- **Whatever STARTS or STOPS a follower does it under the module's lifecycle
  lock, and decides there — not from a snapshot.** WO-06 made `upload` hold a
  follower stopped while it renames the directory. The watchdog read "who is
  running" and "who should be" with no lock and then started followers
  itself; a sweep landing in that gap started the old instance in the middle
  of its own upload, and the upload's start of the new one then failed on
  redb "already open" (WO-07's review). An unlocked READ is fine, and is how
  `peek` works. An unlocked ACT on what was read is not: take the lock, ask
  again, then act. When you add a deliberate gap to one path, list every
  OTHER path that can act inside it.
- **If an effect must wait, BOUND the wait and fail into the retry.** "Never
  wait on the world" has a second answer besides spawning: a machine whose
  cleanup is re-issued every tick can afford a short bounded wait that gives
  up with a typed "busy". WO-07's reap waited, under the module lock, for a
  drain that lets go only between ticks — as long as a hung companion's
  timeout. Three seconds, `DrainBusy`, retried by `pending_cleanup`; the
  request stays queued, so the retry finds it done.
- **"One bad X must not stop the rest" — grep for every loop over X.** Asked
  for on boot and tick in WO-07; also needed in reconcile and in the drain's
  store scan, both of which returned on the first error because with ONE
  instance that was the only thing to do.
- **An effect issued by a transition that CLEARS the state which issued it
  cannot be recovered — so something must SWEEP for it.** `recover()`
  re-issues effects from what is persisted. `ReleaseStaging` comes from
  abandoning a move, which leaves the row with no pending move: crash between
  the row write and the effect and nothing will ever ask again (WO-08: a
  staging follower that ran for ever, for nobody). For every such effect, put
  its rule on a tick — "anything in state X that nothing wants" — and have the
  tick and the effect call ONE function. And never write "the tick will retry
  it" in a log line without naming the line of the tick that does: that
  sentence has now been false twice (WO-03, WO-08).
- **"Carry on past a failed stop" is right for a RENAME and wrong for a
  COPY.** `upload` tolerates a quiesce that errors, because a rename moves
  files whole. WO-08 borrowed the pattern for a byte copy of redb files,
  where a writer still running means a torn `kv` beside a `cursor` from
  another block — silent, and found weeks later. Before reusing a tolerance,
  ask what made it tolerable.
- **Long synchronous work goes on `spawn_blocking`.** A gigabyte of
  `std::fs::copy` inside an `async fn` stalls a runtime worker that every
  follower on the host shares.
- **A value that HOLDS A LOCK must never be parked across a step that can
  fail.** WO-09's order had `quiesce_pair` return a guard holding the module
  lock, stored it in a map, and fed the machine a `Progress` — every path of
  which takes the guard back out. Every path the MACHINE can take. When the
  row write failed the machine took none, the guard stayed in the map with
  two followers stopped, and the next tick blocked on that lock for ever while
  holding a row lock. If you must hold a lock across steps: the step's
  failure branch releases it, the holder's `Drop` releases it, and there is a
  test whose assertion is a timeout.
- **A precondition enforced at ONE entry point is not enforced.** A row is
  rebased before an ANNOUNCEMENT is stepped (WO-08) — but `boot` and the tick
  advance moves too, and neither asked. When you add "X must hold before a
  move proceeds", list every function that makes a move proceed.
- **When a test goes red "by design", suspect its FIXTURE before its
  assertion.** WO-09's one red test asserted something true; its fixture
  built a state (`current: None` with a move pending) that the system had
  stopped producing two orders earlier. A fixture nobody updated is a test of
  a world that no longer exists — and here it was the only thing exercising a
  path that turned out to be reachable for real.

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
| 06 | The DATA plane is instance-addressed — storage handle caches, host slots, the follower's cursor, and the emission fan-out (`routing::fan_out`). A second instance of a module CAN run; nothing starts one. One instance ⇒ byte-identical behaviour. Shaped around `activate` RENAMING a live instance directory on every upload | 01, 03, 05 | **done**, reviewed 2026-09-20 — a SPEC error of the order's (a follower running across the rename lost a block's emissions ⇒ `upload` now quiesces first) + 2 review fixes (the new follower reused the old one's redb handles after every upload; a cached `&str` read could return "no cursor" mid-upload). Implementer's own catch: a fan-out `Skip` is not "no subscriber" |
| 07 | The CONTROL plane is instance-addressed — a lifecycle record, watchdog, compaction and reap per INSTANCE; `attached` derived by `routing::attached_to` (the SAME rule as delivery); the manager stops using the host's `&str` API. The dialer stays ONE drain per MODULE reading every instance's store, leftovers before serving (`routing::drain_pass`) — per-instance drains would POST to one companion concurrently. A non-current instance with no record is `Unmanaged`, never adopted | 06 | **done**, reviewed 2026-09-20 — strongest implementation of the series (two deviations FIXED the order: `CompanionWork`; a `FakeHost` that really removes the directory). Review: a RACE the order did not see (the unlocked watchdog could start a follower inside an upload's quiesce ⇒ `LifecycleManager::revive`, and `restart` ⇒ `cycle_follower`); the reap's wait on the drain is now BOUNDED (`DrainBusy` → retried); reconcile and the drain's store scan no longer let one bad instance stop its siblings |
| 08 | An instance is CREATED — `Event::Rebase` before every announcement (an upload renames the current instance under every row); the host-wide `CatchUpSlot`; `EnsureInstance` ⇒ `LifecycleManager::create_staging` under the MODULE lock: plan from pure fns, build under `.partial` and rename, parent stopped for the copy and ALWAYS restarted, `reflink_or_copy`; `ReleaseStaging`; `AttachTo`/`DetachFrom` ⇒ reconcile. `client_move` runs to `CatchingUp` and stops — the staging instance is silent | 05, 07 | **done**, reviewed 2026-09-20 — implementer's catch: a copy from a parent directory that was GONE produced an empty instance silently (`ParentGone`). Review: a FALSE "the tick will retry it" — a lost `ReleaseStaging` leaked a staging follower for ever ⇒ the tick now sweeps unwanted staging instances; a parent that would not stop was copied anyway (`ParentWouldNotStop`); a failed build left its `.partial`; the copy ran on an async worker (`spawn_blocking`). ⚠️ The slot is held until WO-09 can feed `CaughtUp` — the mainnet gate |
| 09 | The switch — NO aligned pause: quiesce both with `stop_instance`, require staging ≤ live, commit behind a per-client WATERMARK on the staging instance (`mitos_lifecycle::handoff`; exactly-once is a test). `LifecycleManager::quiesce_pair` returns HOLDING the module lock; the row write is the commit; `routing::succession` moves `current.toml` when a move empties the current instance; every phase has a tick sweep. Moves carrying a REFILL wait at `CatchingUp`; the diff stays a no-op | 08 | **done**, reviewed 2026-09-20 — implementer's catches, both defects in the ORDER: a stranded quiesce (row write fails after the stops) deadlocked the whole row tick on the module lock; "no quiesce in hand ⇒ `CommitLost`" is an endless loop for a first subscribe. The open question (a red, unlisted test) was the FIXTURE — a row at `current: None`, unreal since WO-08's rebase — standing on a real gap: `boot` and the tick advanced STALE rows (a served client cut over as a "first subscribe"; a move stuck for ever after an upload renamed its instance, holding the slot) ⇒ both now rebase first. No assertion changed |
| 10 | The refill — make the debt durable on the row (`RunRefill` is issued once by the transition that clears the move, so a crash loses a `recapture`) and pay it CLIENT-scoped: per-companion wipe frame + `InterestOp::Resync`, both of which already exist per companion | 09 | not written |
| F4 | Per-instance resource ceilings, WITH MEASURED NUMBERS — `ResourceProfile::max_memory_bytes` is NOT enforced (`ResourceBudget` is fuel only); redb cache DEFAULTS chosen from the attribution table, plus a host-wide cache budget divided across live instances so forks cannot multiply it. **Gate: no worker announces on MAINNET before this and 10 land** — forks are what grow memory in v3 | F3 | not written |
| 11 | Client lease + probe — ALSO bounds `WaitingForDrain`: a client whose companion is down keeps its move, and the host's ONE catch-up slot, until it recovers | 05 | not written |
| — | Small, unowned: an upload whose key collides with a staging instance (`InstanceExists`); `grant_slot` releasing on a transient error; a poison row in a departed instance's leftovers; the advisory diff (D7) | 09 | not written |
| 12 | GC — CAS refcounts, `orphaned-v2/`, retained instances; delete `auto_resume` | 03 | not written |
| — | D2 semver ABI gate; D11 reporting + MCP | independent | not written — the WIT split is designed by hand, not delegated |

### The footprint track — `docs/design/FOOTPRINT_AND_MINIMUM_SPEC.md`

Runs IN PARALLEL with the numbered orders; it depends on none of them. It is
not a side project: F4 is half of the mainnet gate, and the ARM box doubles as
the isolated mainnet soak for WO-06…09, whose deploy notes all say "should
change nothing" — better proved there than on production.

| # | Title | Depends on | Status |
|---|---|---|---|
| F1 | (in `infra`) Rebuild the ARM box by script — a typed `netcup` SCP client (`plan` sends nothing; `rebuild` is OPERATOR-ONLY), small ext4 root + XFS `reflink=1` data volume, aarch64 provisioning, slices with NO baked-in limits, no production credentials, `/etc/hosts` blackhole as the second lock on shadow delivery, the `smaps` sampler | — | **ready** — `infra/docs/work-orders/WO-F1-arm-mainnet-box.md` |
| F2 | Every redb cache is a named knob (kv asks for **10 GB** per module today; four other stores take redb's 1 GiB default) — defaults UNCHANGED; `MITOS_DELIVERY=shadow`: an isolated box has no companions, so its modules do no work and every number is fiction — it needs production's registrations, and must then be UNABLE to speak to production's DOs (four outbound paths, incl. the recapture WIPE and the indexer WebSocket) | — | **ready** — `WO-F2-cache-knobs-and-shadow-delivery.md` |
| F3 | ARM mainnet at v3 HEAD + F2 to tip; registrations synced box-to-box and checked; the attribution table (footprint §5a); the §6 squeeze ladder AS A SCRIPT | F1, F2 | not written |

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
- **A window you declare TOLERABLE must be walked through every rule the order
  ADDS, not just the ones it inherits — and its length measured, not
  assumed.** WO-06 told the implementer a follower may run across `activate`'s
  rename: cursor write refused for "milliseconds", replay ≤ 1 block, do not
  reorder `upload`. True of the cursor. But the same order introduced
  `routing::fan_out`, under which that follower is no longer the module's
  current instance and writes no emission rows; and the window is a block's
  DISPATCH time, because `stop` awaits the follower. Implemented exactly as
  written, every upload could drop a block's emissions. An instruction of the
  form "do NOT fix this" deserves more scrutiny than any other line in an
  order: it switches off the implementer's judgement at exactly that spot.
- **When you write "the bug is X" in a review comment, the comment is a
  claim — check it like one.** WO-06's review first explained a real defect
  with a mechanism the code made impossible (a mutex held across the open). It
  surfaced only because the explanation had to be written next to the code it
  described. Mutation-check the fix: a test that passes both with and without
  it has explained nothing.
