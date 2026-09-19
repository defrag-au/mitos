# WO-07 — The control plane is instance-addressed: a second instance is a full citizen

**Design refs:** v3 D10 (instance lifecycle), D4.
**Lifecycle crate — read docs AND tests:**
- `routing` — `attached_to`, `Subscriber`, `drain_pass`, `DrainPass` (written
  for this order), and `fan_out`, which they are built from.
- `instance` — `Instance::staging`, `reconcile`, `on_boot`, `recover`,
  `pending_cleanup`. Nothing in it changes.

**Depends on:** WO-06. **Blocks:** WO-08 (an instance is created).

## What this order does, and where it deliberately stops

WO-06 let a second instance of a module RUN. Nothing can yet MANAGE one:
`LifecycleManager` keeps one record per module id and drives the host through
the `&str` API; the watchdog, the compactor and the dialer's drain each know
only "the module", meaning its current instance. A second instance today
would have no lifecycle, no watchdog, never be compacted, and any row written
to its emissions store would sit there undelivered.

After this order every instance directory has its own lifecycle, is started,
watched, compacted and reaped on its own, and its emissions are delivered.
**And still nothing creates one** — WO-08 does. With one instance per module,
behaviour is identical to today; that is the acceptance bar again.

## You are wiring, not deciding

| The host is about to… | …and asks |
|---|---|
| reconcile instance X's `attached` set | `routing::attached_to(X, module_current, subscribers, dir_of)` — hand the result to `Instance::reconcile`. NOT "every client on disk" |
| send a companion's queued rows found in instance X's store | `routing::drain_pass(row, X, module_current, dir_of)` — all `Leftover` sends complete before any `Serving` send starts |
| start / leave stopped / reap instance X | `Instance::on_boot`, `recover`, `pending_cleanup` — as today, per instance |

`attached_to` and `fan_out` are the same rule on purpose: read `attached_to`'s
doc comment. Do not compute attachment any other way.

## Files

- `crates/mitos-platform/src/lifecycle.rs` — the manager.
- `crates/mitos-platform/src/storage.rs` — `read_lifecycle_at` /
  `write_lifecycle_at`, `remove_instance_dir`.
- `crates/mitos-platform/src/host_v2.rs` — `ModuleHostHandle` gains the
  instance calls; `FakeHost` gains instances.
- `crates/mitos-platform/src/watchdog.rs`, `compaction.rs` — per instance.
- `crates/mitos-platform/src/dialer.rs` — the drain reads every instance.
- `crates/mitos-platform/src/admin.rs` — the instances route reports lifecycle.
- `crates/mitos-platform/src/moves.rs` — one call site (`current_placement`).

## The work

### 1. `ModuleHostHandle` — instance calls, and the manager stops using `&str`

Add to the trait: `start_instance(&InstanceKey)`, `stop_instance(&InstanceKey)`,
`evict_instance(&InstanceKey) -> PlatformResult<Vec<String>>`. `ModuleHostV2`
already has the first two.

**The `LifecycleManager` then never calls `replace(id)` / `stop(id)` /
`evict_module(id)` again.** It always knows the key it means, and mixing the
two addressings is how WO-06's `facade_slots` came to exist. In particular
`upload` — read it, and its comment, first — becomes: resolve `K1` →
`stop_instance(K1)` → `activate` / `write_config` → resolve `K2` →
`start_instance(K2)`; on failure, `start_instance` whatever `current.toml`
names. The quiesce-FIRST order is not negotiable (WO-06's review).

`evict_instance`: stop the slot; ask the dialer to release that instance's
store (§4) and AWAIT it; count the rows still `Queued` in its emissions store
and, if there are any, say so at `warn` and on the event ring WITH the count
— they are about to be deleted, and until WO-09 owns "what a departing
instance still owes" this line is the only trace; close all four handles
`*_at`; `remove_instance_dir`.
Then, if NO instance directory remains, do what `evict_module` does today for
the module level (dialer `unregister_module`, `remove_module_dir`) — so
reaping a module's only instance is exactly today's evict.

**Reaping the module's CURRENT instance while siblings exist is refused**:
`PlatformError::CurrentInstanceHasSiblings`, surfaced through the existing
`effect_failed` path, retried by `pending_cleanup` like any failed reap.
Choosing a successor for `current.toml` is part of the switch (WO-09); until
then the case cannot arise outside a test, and guessing would repoint every
`&str` tool at an instance nobody chose.

`FakeHost`: tracks running INSTANCES; `HostCall` gains
`StartInstance(InstanceKey)` / `StopInstance(InstanceKey)` /
`EvictInstance(InstanceKey)`. Its helpers `started(module)`,
`stopped(module)`, `evicted(module)`, `follower_died(module)` keep their
signatures and mean "any instance of that module", so the ~26 assertions
written against them stand untouched. `running_instances` returns what it
tracks (WO-06's Q2).

### 2. `LifecycleManager` — one record per instance

- Storage: `read_lifecycle_at` / `write_lifecycle_at(&InstanceKey)`, through
  `locate_held` like every other `*_at` (never creates a directory). The
  `&str` pair stays as the current-instance facade for other callers.
- Every internal function takes an `&InstanceKey`. The public `&str` methods
  (`apply`, `reconcile`, `instance`, `expects_follower`, the operator
  commands' entry points) stay, and mean the module's current instance —
  EXCEPT `reconcile(module)`, which reconciles EVERY instance of the module:
  one registration change can alter two instances' attached sets. Add
  `apply_at`, `instance_at`, `expects_follower_at`, `instances(module)`.
- The lock stays ONE PER MODULE. Instances of a module share `companions/`,
  `rows/` and `current.toml`; two locks would let two reconciles read one
  listing and write two records that disagree about a client.
- `reconcile`: build the module's `Subscriber`s ONCE (today's
  `on_disk_clients`, plus `storage.read_row` for each — a row that fails to
  read is `None`, with a `warn`), then for each instance
  `attached_to(..)` → `Instance::reconcile(&attached)` → `apply_at`.
- **Adoption is for the module's CURRENT instance only.** It exists so the
  first v3 boot of a v2 tree is a no-op. A NON-current instance directory
  with no `lifecycle.toml` is not adopted, not started and not reaped: it is
  `Unmanaged` — listed by the admin route, warned about once at boot. Adopting
  it would make it `Active` + `Pinned` (no clients ⇒ pinned) and it would
  follow the chain for ever. WO-08 writes `Instance::staging(key)`'s record
  when it creates one; tests do the same.
- `boot`, `tick`, `recover`, `pending_cleanup`: iterate instances, not
  modules. One bad instance must not stop the rest (there is a test for the
  module-level version of that; keep it passing and add the instance one).
- `peek` stays the unlocked read and still never writes.

### 3. Watchdog and compactor

- `watchdog.rs`: for each instance with a record, `expects_follower_at` vs
  `host.running_instances(module)`; revive per instance. The
  `reconcile_module_interest` call after a revive stays ONCE PER MODULE per
  sweep, however many of its instances were revived — it routes the union to
  every running instance already (WO-06), and it is the call whose
  per-companion repetition the dialer's own comment warns re-triggers a
  module's cold-start machinery.
- `compaction.rs`: compact `emissions_store_at` for EVERY instance directory
  of each module it visits, not only the current one.

### 4. The dialer — one drain per MODULE, reading every instance's store

**Not one drain per instance.** Read `DrainPass`'s doc comment for why.

- `run_module_drain` no longer opens one store up front. Each tick:
  `list_instances(module)`, and for each `emissions_store_at(key)` (cached —
  cheap). Keep a `HashMap<InstanceKey, StatusWriterHandle>`; create lazily —
  and on FIRST sight of a store in this process run today's
  `requeue_all_pending` for it — and shut down + drop the entry for any key
  no longer listed (an upload RENAMES the directory: `K1` disappears, `K2`
  appears, same file, same cached handle). Reconcile that map at the START of
  a tick and nowhere else: every send a tick spawns holds a clone of its
  store's status sender, and the tick joins them all before it returns, so
  between ticks is the one moment no send can be holding a writer you are
  about to shut down. And in THIS order: shut down the departed keys'
  writers first and AWAIT them (`shutdown` drains the queue), only then
  requeue for newly-seen keys. After a rename, `K2`'s store IS `K1`'s file;
  requeue it while `K1`'s writer still holds an unwritten `Delivered` and
  that row goes `Pending → Queued`, is listed by this very tick, and is sent
  twice. (`POLL_INTERVAL` is 1s; a directory listing per module
  per second is nothing.)
- Build the tick's work from every store's
  `list_queued_grouped_by_companion`, tagging each companion's batch with
  `drain_pass(..)`. Rows come from `RowCache` — lift it out of `host_v2.rs`
  to somewhere both can use it rather than writing a second one. Resolve
  `module_current` once per tick; on error, treat every batch as `Serving`
  and `warn` (fail open, as the fan-out does).
- Run the `Leftover` work list to completion (today's bounded-concurrency
  join loop, unchanged), update `retry` from its results, THEN build and run
  the `Serving` list — so a companion whose leftovers just failed is already
  backing off when its serving batch would have been sent.
- New `ModuleControl::ReleaseInstance { key, done: oneshot::Sender<()> }`:
  shut down that instance's status writer, drop it, reply. Expose
  `CompanionDialer::release_instance(&InstanceKey)` which sends it and awaits
  the reply (and returns at once if the module has no drain). §1's
  `evict_instance` calls it before closing handles — a status writer holding
  a store clone is what would make the directory removal fail.
- Recapture is untouched: whole-module, and refused for a module with more
  than one instance (WO-06). Client-scoped recapture arrives with the refill
  that needs it (WO-09).

### 5. Admin and `moves.rs`

- `GET /_admin/modules/{id}/instances` adds, per instance: lifecycle phase,
  desired state, attached clients — or `Unmanaged`. Still read-only.
- The operator routes (`stop`, `start`, `pin`, `unpin`, `evict`) keep acting
  on the CURRENT instance. Instance-addressed operator commands are not in
  this order.
- `moves.rs::current_placement` asks `lifecycle.instance(module)`; unchanged
  in meaning (the current instance), just confirm it still compiles against
  whatever you rename.

## Out of scope — do not do these

- Creating an instance; the slot queue; `EnsureInstance`; state copy. WO-08.
- Repointing `current.toml` at a successor; `FanOut::Hold`; the commit. WO-09.
- Client-scoped or instance-scoped recapture.
- Instance-addressed operator routes.
- Removing `facade_slots` or the host's `&str` API — tests and tools use it.
- Any change to `mitos-lifecycle`. If you need one, stop and report.

## Tests that encode the old behaviour

Two tests compare `HostCall` literals and WILL need their expected values
changed, because the manager now makes instance calls:
`an_upload_stops_the_follower_before_it_renames_anything` and
`a_failed_upload_puts_the_follower_back` (both in `lifecycle.rs`). Sanctioned:
replace `HostCall::Stop(MODULE)` / `HostCall::Replace(MODULE)` with the
`StopInstance(k)` / `StartInstance(k)` the new flow makes — for the first
test that is `StopInstance(before)` then `StartInstance(after)`, which is a
STRONGER assertion than the one it replaces; keep the `seen_at_stop` check
exactly as it is. Nothing else. Every helper-based assertion
(`h.host.started(MODULE)` and friends) must pass unedited.

## Acceptance

1. `nix develop -c cargo test -p mitos-platform -p mitos-core -p mitos-lifecycle`;
   `git diff --stat crates/mitos-platform/tests` shows nothing but additions
   to `two_instances_v2.rs`.
2. clippy `--workspace --all-targets -D warnings` 0;
   `cargo check --workspace --all-targets` 0; `cargo fmt --check` clean.
3. Lifecycle (unit, `FakeHost`; a second instance = `write_instance_marker`
   + `write_lifecycle_at(&key, &Instance::staging(key).record())`, or an
   `Active` record where the test needs one):
   - `each_instance_has_its_own_record` — stop one; the other's record and
     follower are untouched.
   - `a_client_is_attached_to_exactly_one_instance` — two instances, three
     clients (no row / row on A / row on B): assert BOTH attached sets.
   - `a_registration_change_reconciles_every_instance`.
   - `a_row_naming_a_renamed_away_instance_stays_attached_to_the_current_one`
     — row at `K1`, upload to `K2`, reconcile ⇒ attached to `K2`, and the
     instance is NOT ejected as abandoned.
   - `an_unmanaged_instance_is_not_adopted_started_or_reaped`.
   - `boot_starts_each_instance_its_record_expects` and
     `one_bad_instance_does_not_stop_boot`.
   - `the_watchdog_revives_a_dead_second_instance_and_leaves_a_stopped_one`.
   - `reaping_a_second_instance_leaves_the_module_and_its_sibling` — the
     sibling's directory, record, slot and `companions/` all intact.
   - `reaping_the_only_instance_removes_the_module_as_evict_always_did`.
   - `reaping_the_current_instance_with_a_sibling_is_refused_and_retried`.
   - `an_upload_never_uses_the_str_host_api` — assert no `Replace` / `Stop`
     in `FakeHost::calls()` across boot, upload, operator stop/start, reap.
4. Dialer. **There is NO delivery test today** — `dialer.rs`'s tests cover
   `group_by_partition` and URL splicing, and nothing in the crate stands up
   an HTTP receiver. So:
   - Split the tick's PLANNING out of `run_module_tick` as a synchronous
     function (stores + registry snapshot + rows + `module_current` in, the
     two ordered work lists out) and test the decisions there, with no
     network: `leftover_batches_are_planned_ahead_of_serving_ones`,
     `a_companion_backing_off_is_in_neither_list`,
     `an_unresolvable_current_instance_plans_everything_as_serving`.
   - Build ONE small receiver for the rest: an `axum` router on
     `127.0.0.1:0` (both already dependencies) that records, in arrival
     order, `(path, the emission ids in the body)`, and can be told to answer
     500 for a given companion. `#[cfg(test)]`, in `dialer.rs`.
   - `rows_in_a_second_instances_store_are_delivered`.
   - `leftovers_are_sent_before_the_serving_instance_speaks` — assert the
     ORDER the receiver saw, not just the set.
   - `a_companion_whose_leftovers_failed_is_not_sent_its_serving_batch_that_tick`.
   - Write `with_one_instance_the_drain_delivers_as_it_always_did` FIRST,
     against the code BEFORE you change it, and quote that it passed — the
     same discipline as WO-06's fan-out baseline, for the same reason.
   - `an_upload_mid_drain_keeps_delivering` — rename `K1 → K2` between ticks;
     assert every row arrived EXACTLY once.
   - `releasing_an_instance_lets_its_directory_be_removed` — and the drain
     keeps serving the sibling.
   - `pending_rows_are_requeued_once_per_store_not_once_per_tick`.
5. Compaction: `every_instances_store_is_compacted`.
6. Real host, in `tests/two_instances_v2.rs`:
   `a_second_instance_is_started_by_boot_and_revived_by_the_watchdog` —
   through `LifecycleManager`, not `host.start_instance`.
7. Admin: the instances route shows phase / desired / attached, and
   `Unmanaged` for a record-less sibling; `reading_instances_writes_nothing`
   still passes.
8. Goldens 69/69.

## Deploy note (for the operator — NOT part of this order)

Same bar as WO-06: nothing should change. Compare before/after per module —
the lifecycle record (`lifecycle.toml` must be untouched by the deploy),
follower running, and per-companion delivery rate. Then do one `deploy.sh`
upload of a module with a busy companion and confirm no emission gap across
it: that exercises the quiesce-first path end to end on the real host.
