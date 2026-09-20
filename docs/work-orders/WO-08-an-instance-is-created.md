# WO-08 — An instance is CREATED: the slot, `EnsureInstance`, state preparation

**Design refs:** v3 D4 steps 2–4, D5 (state), D6 (instance-scoped actions).
**Lifecycle crate — read docs AND tests:**
- `client_move` — `Event::Rebase` and `ClientRow::rebase`; `CatchUpSlot`,
  `Grant`, `QueuedMove`, `next_catch_up` (all written for this order); the
  `Queued → Preparing → CatchingUp` arms of `progress`; `abandon`; `recover`.
- `state_prep` — `plan`, `copy_set`, `CopySet`, `Carry` (the last three written
  for this order), and the three rules at the top of the module.
- `upgrade::resolve_instance`, `InstancePlan`, `WatchedIndex`.
- `instance::Instance::staging`, and what `StagingReleased` does.

**Depends on:** WO-05, 07. **Blocks:** WO-09 (the switch).

## Implemented 2026-09-20 — notes for review

Every acceptance item passes.

| Acceptance | Result |
|---|---|
| 1. `cargo test -p mitos-platform -p mitos-core -p mitos-lifecycle` | `mitos-core` 28, `mitos-lifecycle` 102, `mitos-platform` lib **318**, + 15 integration — **0 failed** |
| 1. `git diff --stat crates/mitos-platform/tests` | `two_instances_v2.rs \| 149 ++++` — **149 insertions, 0 deletions**, and no other file in the directory |
| 2. clippy `--workspace --all-targets --all-features -D warnings` | 0 |
| 2. `cargo check --workspace --all-targets` | 0 |
| 2. `cargo fmt --check` | 0 diffs |
| 3. Rebase (3 named) | pass |
| 4. Slot (6 named) | pass |
| 5. `create_staging` (12 named, incl. one per crash window) | pass, plus `a_parent_that_is_not_on_disk_is_refused_rather_than_copied_as_nothing` — see "Found and fixed" |
| 6. `ReleaseStaging` (2 named) | pass |
| 7. End to end | `an_announcement_creates_a_staging_instance_that_speaks_to_nobody` |
| 8. Real host | `a_staging_instance_copied_from_a_running_parent_resumes_from_its_cursor` |
| 9. Goldens | **69/69**, after `cargo build --release --bin mitos-run` AND `mitos-build` over all 19 community modules |

Both sanctioned test edits were needed and nothing else was:
`a_move_parks_at_queued_and_no_slot_is_ever_granted` →
`a_move_parks_at_catching_up_and_nothing_feeds_caught_up` (phase `Queued` →
`CatchingUp`; the order says its "the client's `current` has NOT moved"
assertion stays as it is — there was not one, so it is ADDED, and it is the
bar for this whole order), and `boot_refetches_a_row_that_was_fetching`
relaxed to `Planned { .. }`.

### The crash windows, one test each

- **(a) partial built, parent still stopped** and **(b) partial complete, not
  renamed** — `a_crash_mid_build_leaves_a_partial_that_boot_sweeps`. One
  partial with only a state file, one with a full record: boot deletes both
  and restarts the parent from its untouched record. They share a test
  because they share a code path — a COMPLETE partial is still a partial, and
  the only thing that makes one an instance is the rename.
- **(c) renamed, follower not started** —
  `a_crash_after_the_rename_starts_the_instance_it_finds`: `create_staging`
  step 1, which is also what the move's `recover()` re-drives.
- And the invariant under all three:
  `nothing_sees_a_half_built_instance` walks `list_instances` and
  `LifecycleManager::instances` after each file written into the partial.

**The parent's downtime** is logged at `info` with bytes and milliseconds and
recorded as `EventKind::StagingCopy { instance, parent, bytes, millis }`.

### Deviations

1. **`create_staging(key, parent)` takes no `governing`.** §3 names the
   parameter and then says the record written is `Instance::staging(key)`'s
   — which is `governing: None` by construction, so the argument would be
   unused. Setting it instead would be the host deciding something
   `instance.rs` decides: the profile is a high-water mark folded over
   clients that have ATTACHED, and none has.
2. **The parent is restarted only if its record says `StartFollower`.**
   "ALWAYS restarted" is implemented as "on every path out of the copy,
   success or failure" — which is what the sentence is guarding against. But
   restarting an instance an OPERATOR had stopped is the v2 bug WO-03 exists
   to fix, so the restart goes through the same `on_boot` check `upload`
   makes. A stopped parent was never running, so the stop was a no-op and
   there is nothing to put back.
3. **`the_parent_is_stopped_for_the_copy_and_always_restarted` injects the
   failure on the SOURCE file (`chmod 000`), not by making "the partial's
   parent read-only".** A read-only instances directory fails
   `begin_instance` — BEFORE the parent is ever stopped — so the test would
   assert nothing at all. `#[cfg(unix)]`.
4. **§8 proves "both followers advancing" with a ROLLBACK event, not a
   second `Apply`.** `DriverV2` takes its cursor from the block it decoded
   (`cursor_after = decoded.cursor`), and there is exactly one block fixture
   — so re-applying it could not move either cursor and would have proved
   nothing. `TipEvent::Undo` sets the cursor to the point it carries, which
   each follower then flushes to its own file. (Found by writing the obvious
   version first and watching the PARENT fail to advance.)
5. **`StateFile` enum in `storage.rs`**, used by the copy AND by the four
   existing store-open sites. "The files a `CopySet` can carry" and "the
   files the stores open" must be the same set, and four string literals in
   four functions is how they stop being.
6. **`FakeHost::stop_instance` now closes the three `*_at` handles the real
   one closes.** Without it every copy test would be copying a file whose
   writer is still open — precisely what `state_prep`'s rule 1 forbids — and
   the fake could not fail on it.
7. **Signature changes the order implies but does not spell out:**
   `MoveManager::new` takes a `Clock` (§2: "inject one, as `LifecycleManager`
   does"), and `admin_router_with_host` takes the `MoveManager` (§5: the rows
   route shows the slot, which lives in memory on that manager). Both call
   sites are in `bundle.rs`.
8. **`LifecycleManager::start_follower` extracted** from `execute`'s
   `StartFollower` arm, so `create_staging` can report `StartFailed` while
   `execute` keeps the "log it and let the watchdog close the gap" posture
   every effect has. That IS §3.4's "the way the manager starts anything" —
   one function, two callers, and the `dialer.reconcile_module_interest` is
   inside it rather than beside it.
9. **Files outside the list:** `events.rs` (the `StagingCopy` variant — §3
   requires the downtime on the event ring and no existing variant could
   carry it), `host_v2.rs` (deviation 6, plus a `RecordingRouter` so "the
   scan-interest was re-asserted" is a fact and not a hope), and
   `mitos-core/src/bundle.rs` (deviation 7, two lines).

### Found and fixed

**A `Carry::Copy` from a parent whose directory is GONE produced an empty
instance, silently.** `PartialInstance::carry` returns `Ok(0)` for a file
that is not there — which is right for `watched.redb`, often absent, and
wrong for the whole parent directory. An un-rebased row naming an instance an
upload renamed away would therefore have been given a REBUILD nobody planned,
presented as a copy, with no error anywhere. `copy_from_parent` now refuses
with `StagingError::ParentGone`; §1's rebase is the first lock on that door
and this is the second.

It also showed the rebase test was weak. It originally inferred "the parent
was K2" from the staging instance merely EXISTING — which, before the fix,
was true either way. It now writes a marker into K2's KV and asserts the
staging instance's KV has it.

### Questions for the reviewer

1. **`ReleaseStaging` and `AttachTo`/`DetachFrom` take the MODULE lock from
   under a ROW lock.** §2's table specifies both inline, and the ordering is
   consistent everywhere (row then module; nothing takes a row lock while
   holding a module one), so there is no deadlock. But the module lock is
   held by `create_staging` for the length of a copy — 1.3 GB on the largest
   module today — so an announcement for ANOTHER client of the same module
   blocks for that long while holding its own row lock, and a subscribe waits
   on it. That is the shape WO-07's review bounded with `DrainBusy`.
   `reconcile` is a DIFF and the tick re-runs it, so a bounded attempt that
   gives up would be safe by construction — but choosing that is a design
   call, not an implementation one.
2. **The slot is held from `SlotGranted` until a switch nothing in this order
   performs.** The first announcement on a host takes the host-wide catch-up
   slot and never gives it back, so every later move queues behind it for
   ever. The deploy note says exactly this and gates mainnet on WO-09;
   restating it because after this order it is real rather than hypothetical,
   and `GET /_admin/catch-up` is now the place it is visible.

## Review 2026-09-20 — a false "the tick will retry it", and three fixes to the copy

Acceptance re-run independently and matches (core 28, lifecycle 102, platform
lib 318, `two_instances_v2` 8; clippy `--workspace -D warnings` 0). Scope
matches "Files" plus `events.rs`, `host_v2.rs`, `bundle.rs`, each justified;
`mitos-lifecycle` untouched; no `#[ignore]`; `tests/` shows additions only.
After review: platform lib **321**.

All nine deviations agreed. Three are better than the order: (2) the parent is
restarted only if its record WANTS a follower — "ALWAYS" as I wrote it would
have resurrected an operator's stop, the v2 bug WO-03 exists to fix; (4)
proving "both followers advance" with a rollback, found by watching the
obvious test fail to prove it; (6) a `FakeHost` whose `stop_instance` closes
the handles, without which every copy test would have been copying a file
under its writer. And "found and fixed" is the catch of the order: a
`Carry::Copy` from a parent whose directory was GONE produced an empty
instance SILENTLY — a rebuild nobody planned, reported as a copy — and the
rebase test had been passing either way. Both now closed.

**1. FALSE CLAIM — "releasing a staging instance failed; the tick will retry
it".** Nothing on any tick called `release_staging`. And the hole was wider
than a failed release: `ReleaseStaging` is issued ONCE, by a transition whose
row is persisted BEFORE the effect runs and which leaves that row with no
pending move — so a crash between the two leaves `recover()` with nothing to
re-issue, and the staging instance follows the chain for ever, for nobody, on
a box whose binding constraint is memory. (Same family as WO-03's "the next
tick retries the reap".) Fix: `LifecycleManager::tick` now releases every
`Staging` instance no move wants, through the same
`release_staging_locked` the effect uses — the rule is the effect's own, so
re-applying it is executing, not deciding; and a staging instance is never
unwanted by accident, because its row is written before `EnsureInstance`
creates it. Test: `the_tick_releases_a_staging_instance_whose_release_was_lost`
(and that a tick leaves a WANTED one, and the parent, alone).

**2. A parent that would not stop was copied anyway.** `copy_from_parent`
logged a failed `stop_instance` and carried on, as `upload` does. `upload` can:
a rename moves the files whole. A byte copy of redb files under a live writer
is a torn `kv` beside a `cursor` from another block — `state_prep`'s rule 1 —
and it does not fail at the copy but weeks later, as state that is quietly
wrong. Now `StagingError::ParentWouldNotStop`, no copy, and no restart
(nothing was stopped). Test: `a_parent_that_will_not_stop_is_never_copied`
(`FakeHost::fail_stop_instance`).

**3. A failed build left its `.partial` on disk** — up to a parent's worth,
1.3 GB for the largest module — until the next boot's sweep, with the host
still up. `PartialInstance::discard`, called on every failure of the build.
Test: `a_build_that_fails_leaves_no_partial_behind`.

**4. The copy ran on an async worker thread.** Synchronous `std::fs` work,
seconds of it without reflink, on the runtime every follower dispatches on.
Now `spawn_blocking`.

**Q1 — leave it inline.** The row-then-module order is consistent, and the
wait it adds is one the subscribe path ALREADY has: `subscribe_handler` calls
`lifecycle.reconcile(module)` itself, before any announcement, so a subscribe
to a module mid-copy waits on the module lock either way, and has since
WO-06's `upload` began holding it across a quiesce. Bounding the second wait
while the first stands would buy nothing. What bounds it in practice is the
copy, which is logged with its duration; if that number is ever not small,
the answer is reflink (the next box), not a timeout.
**Q2 — yes, and it is the deploy gate.** Agreed as written.

Noted for WO-09, not changed: `grant_slot` releases the slot on ANY error
feeding `SlotGranted`, including a transient row-write failure — the move is
then `Queued` with no place in the queue until a restart, because a
re-announcement of the same target is (rightly) quiet.

## What this order does, and where it deliberately stops

Today an announcement for a new build ends at `Queued` (WO-05) and nothing
ever grants it a slot. After this order it gets one: the host creates the
target instance — state copied from the parent the client is on, or rebuilt —
starts it, and the move reaches **`CatchingUp`. And stops.** Nothing feeds
`CaughtUp`; nothing pauses, commits or switches. The client keeps being
served by the instance it was on, exactly as today: the staging instance
follows the chain beside it and speaks to nobody (`routing::fan_out` → `Skip`).

So this is the first order where a SECOND instance exists in production —
and only when a worker announces, which none does unless an operator makes it.

## You are wiring, not deciding

| The host observes… | …and feeds |
|---|---|
| (under the row lock, before stepping an announcement) the row's `current` is not where the client is being served | `Event::Rebase { onto }` |
| the slot was granted to this row | `Progress::SlotGranted` |
| the target instance exists and its follower was started | `Progress::InstanceReady` |
| creating it failed | `Progress::Failed(MoveFailure::StagingFailed)` |

| Effect | In THIS order |
|---|---|
| `RequestCatchUpSlot` | `CatchUpSlot::request` → on a `Grant`, feed `SlotGranted` to THAT row |
| `ReleaseCatchUpSlot` | `CatchUpSlot::release` → likewise |
| `EnsureInstance { key, parent }` | §3 — on its own task; never under the row lock |
| `ReleaseStaging(key)` | §4 |
| `AttachTo(_)` / `DetachFrom { .. }` | `lifecycle.reconcile(module)`. WO-07 derives attachment from rows, so the row write IS the attach; these say "the row changed, recount". That replaces WO-05's "ignore `DetachFrom`" special case |
| `HoldAndDiff { .. }` | nothing, with a comment: the staging instance already speaks to nobody. The hold and the diff are WO-09 |
| `PauseAtBoundary`, `ResumeFollowers`, `PurgeClientEmissions`, `CommitSwitch`, `ResolveCommit`, `RunRefill`, `FinishDiff` | unreachable (nothing feeds `CaughtUp`): keep WO-05's loud `error!`, never a panic. `ResolveCommit` from `recover()` cannot occur for the same reason |

## Files

- `crates/mitos-platform/src/moves.rs` — rebase, the slot, the effects.
- `crates/mitos-platform/src/lifecycle.rs` — `create_staging`, `release_staging`.
- `crates/mitos-platform/src/storage.rs` — building an instance directory.
- `crates/mitos-platform/Cargo.toml` (+ workspace) — `reflink-copy`.
- `crates/mitos-platform/src/admin.rs` — the rows route shows the slot.

## The work

### 1. Rebase, before every announcement

In `announce_locked`, after the row is loaded (or adopted) and BEFORE
`step(Announce)`: work out where the client is actually served —
`row.current` if that instance directory exists, otherwise the module's
current instance (its revision from that instance's manifest, as
`current_placement` already reads it). If that differs from `row.current`,
`step(Event::Rebase { onto })`, persist, execute. Read `ClientRow::rebase`'s
doc comment for the two ways a row goes stale; an operator upload does it to
every row of the module, every deploy.

`Rejected::CommitInFlight` cannot happen yet. Handle it anyway: the
announcement is `Refused` with that reason, nothing written.

`current_placement` today returns `None` for a client not attached to the
current instance. By the time an announcement is stepped the subscribe that
carried it has persisted the registration and reconciled, so this is "the
module has no current instance" — which WO-05 already refuses upstream.

### 2. The catch-up slot

One `CatchUpSlot` on the `MoveManager`, behind a `std::sync::Mutex` (no
`.await` is ever held across it). It is HOST-wide, not per module.

- `QueuedMove.tier` from the client's namespace profile; `queued_at` from the
  manager's clock — inject one, as `LifecycleManager` does.
- A `Grant` names ANOTHER row more often than not. Feeding it means taking
  THAT row's lock, so it happens on a spawned task, never inline under the
  lock of the row whose effect produced it (two rows releasing to each other
  would deadlock).
- **If `SlotGranted` is `Rejected`** (the move was abandoned between being
  queued and being granted) the slot has a holder that will never release
  it. `release` on that row's behalf at once, and feed whatever `Grant` that
  produces. There is a test for exactly this; a leaked slot stops every move
  on the host.
- In memory only — read `CatchUpSlot`'s doc. `boot()` rebuilds it from
  `recover()`'s re-issued `RequestCatchUpSlot`s; do not persist it.

### 3. `EnsureInstance` → `LifecycleManager::create_staging`

The effect is executed by spawning a task (like `spawn_fetch`; track it in
the same `settle()` list) that calls the manager and then, under the row
lock, feeds `InstanceReady` or `Failed(StagingFailed)`. A late answer for an
abandoned move is `Rejected::Stale` and dropped quietly, as WO-05 does.

**Everything below runs under the MODULE's lifecycle lock, in
`LifecycleManager`** — it stops and starts a follower, and WO-07's review is
what happens when something does that from outside.

`create_staging(key, parent, governing) -> Result<(), StagingError>`:

1. **Already there** (`instance_dir(key)` exists with a `lifecycle.toml`):
   if its record expects a follower and none is running, `start_instance`.
   Done. This is the recovery path AND the "B already exists" path of D4.
   Unmanaged (directory, no record): refuse — `StagingError::Unmanaged`.
2. **The plan**, from pure functions only:
   - `state_prep::plan(parent's manifest state_schema, target's, MigrateExport::Absent)`
     — `Absent` always (WO-04: there is no `migrate` export in the WIT yet).
     Manifests come from the CAS by each key's wasm digest. `parent: None`
     ⇒ pass `None`. **`CopyThenMigrate` is therefore unreachable; if it
     appears, fail with `StagingError::MigrateUnsupported` — do not copy and
     hope.**
   - `upgrade::resolve_instance(target's upgrade entries, parent's revision, target's revision)`.
   - `state_prep::copy_set(plan, instance_plan.watched)`.
3. **Build the directory under a temp name and RENAME it into place** —
   `<dir_name>.partial`, beside where it will live. (`list_instances` skips a
   directory with no `instance.toml`, so write that file LAST inside the
   partial: until then nothing on the host can see it.)
   - For each file `copy_set` says `Copy`: the parent's follower must be
     stopped at a block boundary so `kv`, `cursor` and `watched` describe the
     same block. `stop_instance(parent)` does exactly that — it awaits the
     block in flight and flushes the cursor. Then copy with
     `reflink_copy::reflink_or_copy` (NOT `std::fs::copy`: identical on
     today's ext4, a metadata clone on the next box), then
     **`start_instance(parent)` — ALWAYS, on every path out of the copy,
     success or failure.** The parent's record is untouched: this is not a
     lifecycle event, exactly as in `upload`.
   - `emissions.redb` is never copied.
   - Then `lifecycle.toml` = `Instance::staging(key)`'s record (it has no
     governing profile yet — that arrives with its first `Attach`, in WO-09);
     then `instance.toml`; then the rename.
4. Start it THE WAY THE MANAGER STARTS ANYTHING — `execute(&key,
   Effect::StartFollower)`, not a bare `start_instance`. That path also calls
   `dialer.reconcile_module_interest`, and here it is load-bearing: a
   REBUILT instance has an empty KV, so the module's scan-interest (persisted
   in the KV by `update-interest`) is gone, and without the re-assert it
   would bootstrap to tip having scanned for nothing. A failure here leaves a complete, managed,
   `Staging` instance that the watchdog will retry — report it as
   `StagingError::StartFailed` and let the move fail; `ReleaseStaging` (from
   the failed move) is what cleans it up.

**Crash windows — enumerate them in your report, one test each:**
(a) partial built, parent still stopped — boot restarts the parent from its
untouched record and `boot` deletes every `*.partial` it finds;
(b) partial complete, not renamed — same;
(c) renamed, follower not started — `create_staging` step 1, via `recover()`
re-issuing the move.

**The parent's downtime is the copy.** Log it (`info`, with bytes and
milliseconds) and put it on the event ring: the largest `kv.redb` today is
1.3G, and "a dev worker's announcement paused prod's follower for N seconds"
is a thing an operator must be able to see after the fact.

### 4. `ReleaseStaging(key)`

`LifecycleManager::release_staging(key)`, under the module lock: if ANY row
of the module still has a pending move targeting `key`, do nothing (two
clients can be moving to one build). Otherwise `apply_at(key,
Event::StagingReleased)` — the pure machine decides what follows (a `Staging`
instance nobody wants is stopped and reaped; one that has since become
`Active` refuses the event, which is fine — `debug`, not `warn`).

### 5. Admin

`GET /_admin/modules/{id}/rows` gains, per row: whether its move holds the
slot, is waiting (and its position), and — for a move in `CatchingUp` — the
staging and live cursors, so an operator can watch the gap close. A new
`GET /_admin/catch-up` (read-only; add to `READ_ROUTES`): the holder and the
queue.

## Out of scope — do not do these

- Feeding `CaughtUp`; any pause, commit, purge, drain, refill, diff. WO-09.
- `FanOut::Hold`. The staging instance is silent.
- A per-instance memory ceiling (`ResourceProfile::max_memory_bytes` is not
  enforced by the host — `ResourceBudget` is fuel only). Its own order; see
  the deploy note.
- The `migrate` export; `CopyThenMigrate`.
- Pulling a module the host has never had.
- An upload whose new key COLLIDES with an existing staging instance (the
  operator deploys the very build a worker announced): `repoint_locked`
  already refuses it with `InstanceExists` and WO-06's `upload` restarts the
  old follower. Correct, if unhelpful; WO-09 owns making it a no-op.
- Any change to `mitos-lifecycle`. If you need one, stop and report.

## Tests that encode the old behaviour

`a_move_parks_at_queued_and_no_slot_is_ever_granted` (`moves.rs`, WO-05) is
now false by design. Sanctioned: rename it
`a_move_parks_at_catching_up_and_nothing_feeds_caught_up` and change its
expected phase from `Queued` to `CatchingUp`; its assertion that the client's
`current` has NOT moved stays exactly as it is.

`boot_refetches_a_row_that_was_fetching` asserts the refetched row is
`Planned { phase: Queued }`. After this order a refetched move is granted the
slot and carries on, so — sanctioned — relax that ONE pattern to
`Planned { .. }`; its other two assertions (the artifact is in the CAS; no
failures) stay. If any OTHER test must change, stop and report: every other
WO-05 assertion is about fetching and refusing, which this order does not
touch.

## Acceptance

1. `nix develop -c cargo test -p mitos-platform -p mitos-core -p mitos-lifecycle`;
   `git diff --stat crates/mitos-platform/tests` shows additions only.
2. clippy `--workspace --all-targets -D warnings` 0;
   `cargo check --workspace --all-targets` 0; `cargo fmt --check` clean.
3. Rebase (`moves.rs`):
   - `an_upload_between_announcements_rebases_the_row_before_the_next_move` —
     adopt at `K1`, upload to `K2`, announce `K3` ⇒ the staging instance's
     parent is `K2`, and the row on disk says `K2`.
   - `an_unplaced_row_is_rebased_so_its_first_move_is_a_hand_off`.
   - `a_rebase_abandons_the_pending_move_and_its_staging_instance_is_released`.
4. Slot:
   - `only_one_move_catches_up_at_a_time_across_modules`.
   - `production_is_granted_ahead_of_development`.
   - `a_grant_that_is_rejected_releases_the_slot_to_the_next_move`.
   - `an_abandoned_holder_frees_the_slot`.
   - `boot_rebuilds_the_queue_from_the_rows`.
   - `granting_never_deadlocks_two_rows` — two rows whose releases grant each
     other, under a timeout.
5. `create_staging` (`lifecycle.rs`, `FakeHost`):
   - `a_compatible_schema_copies_kv_cursor_and_watched_but_never_emissions`.
   - `reset_watched_drops_only_the_watched_index`.
   - `a_schema_bump_rebuilds_from_empty_and_never_stops_the_parent`.
   - `a_rebuilt_instance_has_the_modules_scan_interest_reasserted`.
   - `the_parent_is_stopped_for_the_copy_and_ALWAYS_restarted` — including
     when the copy fails half-way (make the partial's parent read-only, or
     inject the failure); assert `StopInstance(parent)` then
     `StartInstance(parent)` in `FakeHost::calls()`, and that the parent's
     `lifecycle.toml` is byte-identical before and after.
   - `the_copy_is_taken_with_the_parent_stopped` — `FakeHost::on_stop`
     records a marker the parent's KV is then given; the copy must contain it.
   - `an_existing_staging_instance_is_found_not_rebuilt` — its cursor survives.
   - `an_unmanaged_directory_is_refused`.
   - One test per crash window (a)–(c) above.
   - `nothing_sees_a_half_built_instance` — between the first file and the
     rename: `list_instances`, the instances route, the drain's store
     reconcile and `boot` all behave as if it were not there.
   - `a_watchdog_revive_cannot_land_inside_a_copy` — the same shape as WO-07's
     upload test: `revive(parent)` during the copy is `NotNeeded`… and the
     parent IS running afterwards.
6. `ReleaseStaging`:
   - `a_staging_instance_two_clients_are_moving_to_survives_one_giving_up`.
   - `a_staging_instance_nobody_wants_is_stopped_and_reaped`.
7. End to end (`moves.rs`): an announcement ⇒ fetch ⇒ slot ⇒ staging instance
   created and started ⇒ row at `CatchingUp`; the client's `current`
   unchanged; `routing::fan_out` for the staging instance is `Skip`; a second
   client's move for another module waits in the queue.
8. Real host, `tests/two_instances_v2.rs`:
   `a_staging_instance_copied_from_a_running_parent_resumes_from_its_cursor`
   — the real `ModuleHostV2`, a real copy, and both followers advancing after.
9. Goldens 69/69.

## Deploy note (for the operator — NOT part of this order)

Deploying this changes nothing until a worker announces. **Do not let a
worker announce on MAINNET yet**, for two reasons this order does not close:
the slot is held until WO-09 can feed `CaughtUp`, so one announcement stops
every other move on the host; and there is no per-instance memory ceiling, on
a box whose binding constraint is memory (v3 "Box hardening": the `dev`
ceiling ships WITH forks, never after). Preprod is the place: announce one
dev worker, watch `GET /_admin/catch-up` and the rows route, confirm the
parent's follower paused only for the logged copy and both cursors advance.
To clear it afterwards, announce the build the client is already on — that
abandons the move and `ReleaseStaging` reaps the instance.
