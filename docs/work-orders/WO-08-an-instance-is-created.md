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
