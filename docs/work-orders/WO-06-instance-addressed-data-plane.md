# WO-06 — The data plane is instance-addressed: a second instance of a module can run

**Design refs:** v3 §3 (model), D4, Wave 2 item 1 ("instance re-keying — most
invasive; alone").
**Lifecycle crate — read docs AND tests:**
- `routing` (written for this order) — `fan_out`, `unclaimed`. Read its module
  doc twice; "The trap this is shaped around" is the reason this order exists
  in the shape it does.
- `ids::InstanceKey`.

**Depends on:** WO-01, 03, 05. **Blocks:** WO-07 (control plane), and through
it the move.

## Implemented 2026-09-20 — notes for review

Every acceptance item passes.

| Acceptance | Result |
|---|---|
| 1. `cargo test -p mitos-platform -p mitos-core -p mitos-lifecycle` | `mitos-core` 28, `mitos-lifecycle` 93, `mitos-platform` lib 257, `bootstrap_by_policy_v2` 1, `community_module_interest` 2, `dispatch_v2` 1, `dynamic_interest_v2` 1, `lifecycle_v2` 2, `two_instances_v2` 5 — **0 failed** |
| 1. integration files carry ONLY the sanctioned edit | `git diff --stat crates/mitos-platform/tests` = 4 files, 10 insertions / 5 deletions — the five `KvFactory` closures and their `use`. No assertion, fixture or helper touched; `tests/common/mod.rs` unmodified |
| 2. clippy `--workspace --all-targets --all-features -D warnings` | 0 |
| 2. `cargo check --workspace --all-targets` | 0 |
| 2. `cargo fmt --check` | 0 diffs |
| 3. storage (six named tests) | all present and passing |
| 4. host (`tests/two_instances_v2.rs`, five named tests) | 5 passed |
| 5. fan-out (seven named tests) | 7 passed — see below on the pinned baseline |
| 6. admin | `the_instances_route_lists_both_and_marks_the_current_one`, `reading_instances_writes_nothing`, and `the_route_lists_cover_the_whole_router` all pass |
| 7. goldens | **69/69**, after `cargo build --release --bin mitos-run` AND `mitos-build` over all 19 community modules |

### The baseline passed BEFORE the change

`with_one_instance_and_no_rows_the_fan_out_is_unchanged` was written against
`drain_one` / `fan_out_undo` as they were and run before any instance
addressing went in:

```
test host_v2::fan_out_tests::with_one_instance_and_no_rows_the_fan_out_is_unchanged ... ok
test result: ok. 1 passed; 0 failed
```

It still passes unmodified. That, plus the five untouched integration files,
is the whole of "with one instance the behaviour is byte-identical".

### Found by a test, fixed: a `Skip` is not "no subscriber"

`a_client_whose_row_names_the_second_instance_is_served_by_it_alone` failed
the first time it ran, and the reason was worth the test.

The first draft did what the order's bullet says literally — `Skip` ⇒
`continue` past the client. But `drain_one` writes a SENTINEL row when it
walked the companions directory and saw no companion at all, so that the
first-ever subscriber can reclaim events emitted before it arrived. Skipping
the client directory outright left `saw_companion` false, so the instance
that did NOT serve the only client wrote a sentinel for an event that had in
fact been delivered — and a later, unrelated subscriber would have reclaimed
it.

A `Skip` means "another instance is writing this client's row", not "nobody
is subscribed". The walk now descends far enough to set `saw_companion` and
withholds only the `append` (`host_v2.rs`, the `serves` binding). `fan_out_undo`
has no sentinel, so its early `continue` is correct and is left alone.

### Deviations, all small

1. **`recapture_module` asks `refuse_if_forked` BEFORE the dialer lookup.**
   It was after, so a forked module with no dialer wired reported "dialer not
   wired" — the true reason masked by unrelated wiring. Production always has
   a dialer, so nothing changes there; the test reads the real reason.
2. **The admin enum is `InstanceRole`, not `Role`.** Variants are `Current` /
   `Other` as specified; the prefix matches this file's `RowSummary` /
   `RowPlacement` / `RowStage`. Its sibling `SlotState::{Running, Stopped}` is
   an enum for the same reason, though the order only asked for one.
3. **The second instance in the host tests is config-keyed, not wasm-keyed.**
   The order suggests `put_artifact(&manifest, &wasm)` + `write_instance_marker`,
   but there is one test wasm and `put_artifact` of the same bytes is a no-op
   — the same bytes give the same key, so that recipe yields one instance, not
   two. `put_config` + `write_instance_marker` is the other half of the same
   public surface and does give a distinct instance. Config bytes are empty,
   which the test-indexer's `init` treats exactly as no config, so the second
   instance differs in identity and nothing else.
4. **`tests/two_instances_v2.rs` defines its own `BroadcastSub`.** `common`'s
   `OneShotSub` shares ONE receiver across every subscription it hands out, so
   a tip event reaches whichever follower wakes first. Harmless with one
   instance, fatal here: "both cursors advanced" would depend on the
   scheduler. `common/mod.rs` is a shared fixture and changing it is not
   sanctioned, so the new subscription lives in the new file.
5. **`emissions_path` / `kv_path` / `watched_path` are now `#[cfg(test)]`.**
   Production reaches all three through the instance-addressed stores. They
   are kept because `opening_state_for_a_missing_instance_is_an_error_not_a_mkdir`
   proves the no-mkdir refusal per PATH, which is stronger than proving it per
   store. No assertion changed.

### Found, NOT fixed

- **`interest_filter_tests::POLICY_B` is 57 hex characters** (`host_v2.rs`), one
  over. It has never surfaced because that module only passes it as a raw
  `&str`; my `fan_out_tests` module was the first to put a constant through
  `PolicyId::new`, which rejected it. I used a correct 56-char constant in the
  new module and left theirs alone — changing it is a test edit this order
  does not sanction, and it asserts nothing about length. One line, someone
  else's call.

### Questions for the reviewer

1. **The sentinel across a fork is still only approximately right.** With two
   instances and NO companions at all, only the current instance keeps the
   unclaimed row — that is `routing::unclaimed` and it is correct. But the
   fix above means a client served by instance B also suppresses instance A's
   sentinel, which is right for that client and says nothing about a *future*
   client. Two instances, one client on B, then a brand-new client subscribes:
   it reclaims A's sentinel rows from before the fork and nothing from after.
   I believe that is acceptable (sentinel reclaim is best-effort and the
   window is "before the first subscriber"), but it is a behaviour that only
   exists once a module can fork, and WO-09's `Hold` may want to name it.
2. **`ModuleHostHandle::running_instances` returns `Vec::new()` on `FakeHost`.**
   `FakeHost` is module-addressed and the lifecycle watchdog it serves stays
   module-addressed until WO-07. So the admin instances route reports every
   instance as `Stopped` under `FakeHost` — which is what the admin tests
   assert. If WO-07 gives `FakeHost` instances, that assertion changes with it.

## What this order does, and where it deliberately stops

WO-01 made the DISK layout instance-keyed. Everything above the disk is still
keyed by module-id string: `ModuleStorage`'s four handle caches, the host's
`slots` and `interest_senders`, the follower's cursor writes, the emission
fan-out. So today a second instance of a module cannot run — it would share
the first one's `kv.redb` handle and overwrite its cursor.

After this order it can. **And nothing starts one.** No route, no effect, no
timer creates or starts a second instance; only tests do. With one instance
and the `&str` API, every observable behaviour is byte-identical to today —
that is the acceptance bar, and the integration tests are how it is held.

Out of this order entirely, because they are the CONTROL plane and come next:
`LifecycleManager` (stays keyed by module id), the dialer's drains (stay one
per module, reading the module's CURRENT instance's emissions), recapture,
the admin mutation routes.

## You are wiring, not deciding

There is one decision in this order and it is already made:

| The host is about to… | …and asks |
|---|---|
| write an emission row for a companion of client C, from instance X | `routing::fan_out(row_of_C, X, module_current, dir_of)` — `Deliver` ⇒ write, `Skip` ⇒ don't |
| fan an UNDO out to a companion of client C, from instance X | the same call |
| keep an emission nobody is subscribed for (the sentinel row), from instance X | `routing::unclaimed(X, module_current)` — `Keep` ⇒ today's sentinel logic, `Drop` ⇒ nothing |

`match` on the result — never `== FanOut::Deliver`. A `Hold` variant is
coming, and it must arrive as a compile error at your call sites.

## Files

- `crates/mitos-platform/src/storage.rs` — instance-addressed handles.
- `crates/mitos-platform/src/host_v2.rs` — slots, senders, start/stop, fan-out.
- `crates/mitos-platform/src/follower_v2.rs`, `bootstrap_v2.rs`,
  `backfill_v2.rs` — carry an `InstanceKey`, not a module-id string, to
  anything that opens a store or writes a cursor.
- `crates/mitos-core/src/bundle.rs` — the `KvFactory` closure.
- `crates/mitos-platform/src/admin.rs` — one new read route.
- The four integration test files named under "Tests that encode the old
  behaviour" — one sanctioned edit each, nothing else.

## The work

### 1. Storage — handles are per INSTANCE

- A private cache key:

  ```rust
  enum StateAddr {
      Instance(InstanceKey),
      /// Never activated: no `current.toml`, state lives in the module
      /// directory, exactly where v2 put it. Tests and a few tools write a
      /// cursor to such a module; it must keep working.
      Unactivated(String),
  }
  ```

  All four caches (`cursor_stores`, `emissions_stores`, `kv_stores`,
  `watched_stores`) are keyed by it.
- For each of `kv_store`, `watched_index`, `emissions_store`, `read_cursor`,
  `write_cursor`, `close_kv`, `close_watched`, `close_emissions`,
  `close_cursor`, `last_trap_path`: add a `*_at(&InstanceKey, …)` twin.
  (Config needs none: it is already in the CAS under `key.config` —
  `read_config_bytes`.) The existing `&str` method RESOLVES the current instance and calls the
  twin (or takes the `Unactivated` path). Not one `&str` caller changes.
- **A `*_at` method NEVER creates an instance directory.** If
  `instance_dir(key)` is not a directory it returns
  `StorageError::NoSuchInstance`. WO-01's review is why: an implicit
  `create_dir_all` behind a rename resurrects the old directory, and a
  follower then resumes on an empty cursor in it. Instance directories are
  created in exactly the places they are today. Resolve-and-build stays under
  ONE `repoint` read guard, as `state_dir` does now.
- **`repoint_locked` re-keys the caches.** It renames a LIVE instance
  directory `K1 → K2` with the follower running (read its comments), and it
  has TWO callers: `activate` (a new wasm) and `write_config` (a new config —
  config is half of instance identity). An upload with a config therefore
  renames TWICE, `K1 → K2 → K3`. Put the re-key in `repoint_locked` so neither
  caller can forget it. The
  open redb handles survive that — they hold fds — but a cache keyed by
  `InstanceKey` would now have them under a key whose directory is gone, and
  the next `kv_store_at(K2)` would open the same file a second time: redb
  refuses, and the module cannot start. So, inside the same `repoint` WRITE
  guard and after the rename: move every cache entry `Instance(K1)` →
  `Instance(K2)`. The host's `facade_slots` (§2) is NOT re-keyed — it records
  what was started, and that is what `replace` must stop.
  - Crash windows: none new. The re-key is in memory only.
  - The window you must know about: between the rename and the upload
    handler's `replace`, the running follower still holds `K1`, and its
    `write_cursor_at(K1)` now returns `NoSuchInstance`. `follower_v2.rs`'s
    `flush_cursor` already logs a failed write at `warn` and carries on
    ("best-effort"); the follower is stopped milliseconds later and
    the restart replays at most one block, which is the at-least-once the
    cursor has always had. Do NOT "fix" this by creating the directory, by
    falling back to the module's current instance, or by reordering `upload`.

### 2. Host — a slot is an instance

- `slots` and `interest_senders` are keyed by `InstanceKey`.
- `start_instance(&InstanceKey, rebootstrap) -> PlatformResult<u64>` is
  today's `start` body with every store opened through a `*_at` twin, the
  manifest and wasm read from the CAS for `key.wasm`
  (`read_artifact_manifest`, `artifact_path`), and the config from
  `key.config`. `stop_instance(&InstanceKey)` likewise.
- The `&str` API (`start`, `stop`, `replace`, and the `ModuleHostHandle`
  trait, UNCHANGED in signature) resolves the module's current instance and
  delegates. One subtlety, from §1: `replace(id)` runs AFTER `activate` has
  renamed `K1 → K2`, so "stop the current instance" would look for `K2` and
  find nothing running. The host therefore records, per module id, which key
  the `&str` API last STARTED (`facade_slots: HashMap<String, InstanceKey>`),
  and `stop(id)` stops THAT. `list_running` keeps returning module ids
  (deduplicated).
- `KvFactory` becomes `Arc<dyn Fn(&InstanceKey) -> ModuleKv + Send + Sync>`;
  the bundle's closure calls `kv_store_at`. Every internal caller passes the
  instance it is running as — `start`'s bootstrap KV, `follower_v2`'s
  dynamic-interest seed KV, `backfill_v2`, the recapture path.
- `recapture_in_flight`, `bootstrap_in_flight`, `last_results` stay keyed by
  module id: they back `/_admin/status`, and recapture is whole-module until
  WO-02's note about it is resolved.
- `route_interest(module, …)` sends to EVERY running instance of the module.
  Interest belongs to a companion, at module level; an instance that is not
  serving that companion watching a few extra refs costs a little and breaks
  nothing, while one that IS serving it and missed the update is silent loss.
- `recapture_module` and `evict_module`: if the module has MORE THAN ONE
  instance directory, refuse with a new typed
  `PlatformError::MultipleInstances { module, found }`. Both are whole-module
  operations written for one instance; making them instance-aware is WO-07's.
  With one instance — every module in production — nothing changes.

### 3. The emission fan-out

`run_emit_drain` / `drain_one` / `fan_out_undo` take the `InstanceKey` they
run for and open `emissions_store_at`. Inside the per-companion loop, after
the interest filter and before the row is written, ask `routing::fan_out`.

- The client's row: `storage.read_row(module, client_id)`. `None` on a read
  or parse error, logged at `warn` once per client — a row that cannot be
  read must not stop delivery. Cache it exactly as `InterestCache` caches
  interest files: by path, refreshed on mtime change. This loop runs per
  emitted event.
- `module_current`: `storage.current_instance(module)`. Resolve it once per
  EVENT, not once per drain task — an upload changes it under a running task.
- `dir_of`: `instance_dir(k).is_dir()` → `Present` / `Gone`.
- A `client_id` directory name that does not parse as a `ClientId` has no
  row: pass `None`.
- The sentinel branch (`!saw_companion`) asks `routing::unclaimed` first.

### 4. Admin — see them

`GET /_admin/modules/{id}/instances`, typed, read-only: for each instance
directory — `dir_name`, the full key, `current: bool`-SHAPED AS AN ENUM
(`Role::{Current, Other}`), whether a slot is running, the cursor. Add it to
`READ_ROUTES`. It reads; it never adopts, repairs or writes.

## Out of scope — do not do these

- `LifecycleManager`, `lifecycle.toml`, the watchdog: still module-addressed.
- The dialer: one drain per module, on the module's current instance.
- Creating an instance, copying state, `EnsureInstance`, slots, the move.
- Any route or timer that starts a second instance.
- `FanOut::Hold`, held emissions, the diff.
- Reordering `upload`, or making `activate` copy instead of rename.
- Rewriting rows when `activate` renames. `routing` is built so that nothing
  has to: read its module doc.

## Tests that encode the old behaviour

`KvFactory`'s parameter type changes, and five closures in integration tests
name it: `tests/lifecycle_v2.rs` (two), `tests/dispatch_v2.rs`,
`tests/dynamic_interest_v2.rs`, `tests/bootstrap_by_policy_v2.rs` — each
`|_id: &str| state_kv::ModuleKv::new_in_memory()`. Changing `&str` to
`&InstanceKey` in those five closures (plus the `use`) is sanctioned. No
assertion, fixture or helper changes. If any integration test needs more than
that, stop and report.

## Acceptance

1. `nix develop -c cargo test -p mitos-platform -p mitos-core -p mitos-lifecycle`
   — the integration files pass with ONLY the sanctioned edit
   (`git diff --stat crates/mitos-platform/tests` in the report).
2. clippy `--workspace --all-targets -D warnings` 0;
   `cargo check --workspace --all-targets` 0; `cargo fmt --check` clean.
3. Storage:
   - `two_instances_of_one_module_have_separate_handles` — write a KV value
     and a cursor through each; neither sees the other's.
   - `an_at_call_on_a_missing_instance_is_an_error_and_creates_nothing` — for
     EVERY `*_at` method; assert the module directory listing is unchanged.
   - `activate_carries_open_handles_to_the_new_key` — open all four stores at
     `K1`, `activate` to `K2`, then `*_at(K2)` returns the SAME handles (a
     value written before is readable after) and does not hit redb's
     "already open".
   - `an_upload_with_a_config_carries_them_across_both_renames` — `activate`
     then `write_config`, `K1 → K2 → K3`.
   - `a_cursor_write_to_the_old_key_after_a_rename_fails_and_resurrects_nothing`
     — use the existing `set_repoint_hook` idiom if you need the window held.
   - `an_unactivated_module_still_takes_a_cursor` (the `Unactivated` path).
4. Host (`FakeHost` does not cover this — use the real `ModuleHostV2` the way
   `tests/lifecycle_v2.rs` builds one, in a NEW `tests/two_instances_v2.rs`).
   A test makes a second instance with what is already public:
   `put_artifact(&manifest, &wasm)` then `write_instance_marker(&key)` — the
   one place an instance directory is created outside `activate`. Do not add a
   production helper for it; creating instances is WO-08's.
   - `a_second_instance_follows_beside_the_first` — both slots running, each
     advancing its OWN cursor, each with its own KV.
   - `stopping_one_instance_leaves_the_other_running`.
   - `replace_after_an_upload_stops_the_renamed_instance_and_starts_the_new_key`
     — the `facade_slots` case; assert exactly one slot is running after.
   - `an_interest_update_reaches_every_running_instance`.
   - `recapture_and_evict_refuse_a_module_with_two_instances`.
5. Fan-out. **`drain_one` and `fan_out_undo` have NO tests today** — the tests
   at the bottom of `host_v2.rs` cover `InterestCache` only. Both are plain
   synchronous functions over a `ModuleStorage` and an `EmissionsStore`, so
   they need no host. Write the FIRST test against the code as it is BEFORE
   you change it, so it pins today's behaviour rather than yours:
   - `with_one_instance_and_no_rows_the_fan_out_is_unchanged` — two clients,
     three companions, one bounded interest that filters one of them out, and
     the no-companion sentinel case. Quote in your report that it passed
     before the change.
   - `a_second_instance_writes_no_rows_and_no_sentinel`.
   - `a_client_whose_row_names_the_second_instance_is_served_by_it_alone` —
     one emitted event through both instances' drains ⇒ exactly ONE row, in
     the second instance's store.
   - `a_row_naming_a_renamed_away_instance_is_still_delivered` — write a row
     at `K1`, `activate` to `K2`, emit ⇒ delivered from `K2`.
   - `an_unreadable_row_does_not_stop_delivery`.
   - `an_undo_follows_the_same_rule`.
   - `an_upload_mid_task_moves_module_current` — `module_current` is resolved
     per event.
6. Admin: `the_instances_route_lists_both_and_marks_the_current_one`;
   `reading_instances_writes_nothing`; `the_route_lists_cover_the_whole_router`
   still passes.
7. Goldens 69/69 (rebuild `mitos-run` AND the modules first — README).

## Deploy note (for the operator — NOT part of this order)

This is the riskiest deploy in the series precisely because it is meant to
change nothing: every module's follower, handle cache and fan-out is touched.
Deploy to preprod first and compare, per module, before/after: cursor advance
rate, emission row counts per companion over the same hour, and that a
`deploy.sh` upload still resumes on the old cursor. Then mainnet.
