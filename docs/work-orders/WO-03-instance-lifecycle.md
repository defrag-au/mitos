# WO-03 — The instance lifecycle, wired

**Design refs:** v3 D10 (Instance lifecycle).
**Lifecycle crate:** `mitos_lifecycle::instance` — read the module docs and ALL
of its tests. Four things in it were written FOR this order: `Instance::adopt`,
`Instance::reconcile`, `Instance::recover`, and `LifecycleRecord`. Their doc
comments carry the reasoning; this order does not repeat it.
**Depends on:** WO-01 (instance dirs), WO-02 (`Registry::client_key`, profiles).
**Blocks:** WO-05, WO-06, WO-08.

## Implemented 2026-09-19 — open items for review

Acceptance all passes: `mitos-platform` 204 + the four integration files
UNMODIFIED, `mitos-core` 28, `mitos-lifecycle` 70, clippy `--workspace
--all-targets -D warnings` exit 0, `cargo check --workspace --all-targets`
exit 0, `cargo fmt --check` 0 diffs, goldens 69/69 (rebuilt `mitos-run`).
Every existing test passes unchanged — there was no sanctioned exception in
this order and none was needed.

Two new files' worth of surface to review: `crates/mitos-platform/src/lifecycle.rs`
(the manager, plus 17 tests) and `host_v2::testing::FakeHost`, the one
recording `ModuleHostHandle` — which is also the first test coverage the
watchdog and the admin evict path have ever had.

### Deviations

**1 and 5 are judgement calls — please scrutinise those two rather than
accepting them.** Both change WHERE or WHETHER something the order specified
happens. 2, 3, 4 and 6 are mechanical.

1. **The flat-companion migration runs INSIDE `manager.boot()`**, as its first
   action per module, rather than as a separate step 3 in `bundle.rs`. Same
   ordering, but `migrate_flat_companions_for_module` is `pub(crate)` and
   `bundle.rs` is another crate — and putting it next to adoption makes the
   invariant ("adoption must never see flat files") structural instead of
   dependent on someone preserving a call order in the bundle. Still
   idempotent; `dialer.start_all()` still runs it again.
2. **A second `EventKind`.** The order says events.rs gains one
   (`InstanceAlert`) but also requires "Record the failure on the event ring"
   for a failed effect. That is a different thing from an alert, so it is
   `LifecycleEffectFailed { effect, error }`.
3. **`InstanceAlert` gained `Serialize`/`Deserialize` + `rename_all` in
   `mitos-lifecycle`.** The event ring is a typed `#[derive(Serialize)]` enum
   and the alert reaches an operator through `GET /_admin/events`, so the
   alternative was a stringly-typed `alert: String` in the host. `Phase`,
   `DesiredState` and `Tier` in the same crate already derive it for the same
   reason. No logic changed.
4. **`HandlerError::NotRunnable`** for "recapture on a stopped module ⇒ 409".
   The first draft reused `Rejected::StagingIsNotOperable`, which was a lie —
   it is not a staging instance.
5. **`Reap` does not remove the directory if `host.evict_module` failed.** The
   order says the effect is today's evict-handler body, which removes
   unconditionally. Removing files under redb handles the host failed to close
   is how a follower ends up writing to an unlinked inode; the failure is
   recorded and the next tick retries the whole effect.
6. `companion_router` and `admin_router_with_host` take the manager, so their
   signatures changed. `ModuleSummary` gained
   `lifecycle: Option<StatusLifecycle>` (skipped when absent) rather than a
   new wrapper type, so an upload response is unchanged.

### Found but not fixed

- `crates/mitos-platform/src/admin.rs` — `evict` reports the lifecycle's
  ATTACHED set where v2 reported `client_id:companion_key` pairs from a
  directory walk. Same field name (`cancelled_companions`), different
  granularity: one entry per client, not per registration. No consumer in this
  repo reads it, but `mitos-admin`'s evict output will look different.
- `crates/mitos-platform/src/watchdog.rs:130` — the sweep now calls
  `expects_follower` per module, which reads `lifecycle.toml` from disk each
  tick. Fine at ~15 modules and a 60s cadence; it is a per-module file read
  where v2 did a directory count.
- `ModuleHostV2::auto_resume` is now dead in production — only
  `tests/lifecycle_v2.rs` calls it. Left in place deliberately (that file must
  not change), but it is a second, now-divergent answer to "what should be
  running" sitting in the codebase. WO-08 or a follow-up should delete it once
  that test moves to the manager.

### Questions

None blocking. One thing worth a reviewer's eye: **`reconcile` is called
before the evict authorization check**, so `POST …/evict` on a module whose
registrations were removed while the host was down succeeds instead of 409ing
on a stale attached set. That is what the order asks for ("`evict`:
`reconcile` first"), and it is right, but it does mean the safety check is
evaluated against the disk rather than against what the record said a moment
earlier.

## Why

v2 has no notion of what an operator WANTS. `DELETE /_admin/modules/{id}` only
stops the follower — and two things then undo it:

1. **`auto_resume`** starts every module with a manifest on the next restart.
2. **The watchdog** (`watchdog.rs`) restarts any module that has registered
   companions and no live follower — so on a SUBSCRIBED module, an operator's
   stop is undone within ONE TICK, not merely on the next restart.

There is also no lifecycle at all: nothing records who is attached to a
module, and nothing ever retires one. v3's client move (WO-06) needs both.

## What this order does, in one line

Every module instance gets a persisted `LifecycleRecord`; ONE component, the
`LifecycleManager`, feeds events to `Instance::step` and executes the effects;
and boot, the watchdog, and the admin stop/start/evict paths all go through it.

**Behaviour on mainnet must not change on deploy.** Every existing module is
ADOPTED: with clients ⇒ `Active`; with none ⇒ `Active` + `Pinned`. Every
mainnet instance is shared by a prod client or has none, so it is governed by
`AlertOnly` or pinned — nothing stops, nothing is reaped, no timer fires. The
ONE intended behaviour change is the bug fix: an operator's stop now sticks.

## You are executing effects, not deciding

If you write an `if` that decides whether an instance should be running,
draining, stopped or reaped — stop. `Instance::step`, `on_boot`, `recover` and
`reconcile` decide all of it. The host's whole job is below.

| The host observes… | …and feeds |
|---|---|
| registrations on disk changed (or might have) | `instance.reconcile(&on_disk)` → each event it returns |
| 60s passed | `reconcile`, then `Event::Tick` |
| `DELETE /_admin/modules/{id}` | `Operator(Stop)` |
| `POST …/restart` | `Operator(Start)`; if that produced NO `StartFollower` it was already running — then `host.replace(id)` as today |
| `POST …/start`, `…/pin`, `…/unpin` (NEW routes) | `Operator(Start \| Pin \| Unpin)` |
| `POST …/evict[?force=true]` | `Operator(Evict(RefuseIfAttached \| Force))` |
| boot | load-or-adopt, `reconcile`, then execute `instance.recover()` |

| Effect | Execute as | Idempotent because |
|---|---|---|
| `StartFollower` | `host.replace(id)`, then — if the dialer is set — `dialer.reconcile_module_interest(id)`, exactly as the watchdog does after a revive (subscribes that landed while it was down dropped their interest mutations) | `replace` stops-then-starts |
| `StopFollower` | `host.stop(id)` | a no-op on a module that is not running |
| `Reap` | today's evict handler body: `host.evict_module(id)` then `storage.remove_module_dir(id)`. Move it; do not copy it | both tolerate "already gone" |
| `Alert(kind)` | `tracing::error!` + a new `EventKind::InstanceAlert { alert }` on the ring | a repeated alert is noise, not damage |

**Persist, THEN execute.** `apply` = lock the module → load → `step` → write
the new record → execute effects in order. A crash between the write and the
effects is what `recover()` is for. An effect that FAILS does not roll the
record back: the record is what SHOULD be true, and the watchdog (below) is
what closes the gap. Record the failure on the event ring.

## Files

- `crates/mitos-platform/src/lifecycle.rs` — NEW. `LifecycleManager`.
- `crates/mitos-platform/src/lib.rs` — `pub mod lifecycle;`
- `crates/mitos-platform/src/storage.rs` — `read_lifecycle` / `write_lifecycle`.
- `crates/mitos-platform/src/admin.rs` — handlers route through the manager;
  three new routes; status + module listings gain lifecycle fields.
- `crates/mitos-platform/src/companions.rs` — call `manager.reconcile(module)`
  after a registration is written or removed.
- `crates/mitos-platform/src/watchdog.rs` — "should be running" comes from the
  lifecycle.
- `crates/mitos-platform/src/events.rs` — one new `EventKind`.
- `crates/mitos-core/src/bundle.rs` — boot order; construct + wire the manager.

## The work

### 1. Storage

`lifecycle.toml` in the instance directory, holding a `LifecycleRecord` — NOT
an `Instance`. The record deliberately has no key: an upload RENAMES the
instance directory to a new key, the record moves with it untouched, and the
key is read from `instance.toml`. Do not add the key "for convenience".

- `read_lifecycle(module_id: &str) -> Result<Option<LifecycleRecord>>`
- `write_lifecycle(module_id: &str, &LifecycleRecord) -> Result<()>`

Both go through the `&str` facade and resolve + build the path under the
`repoint` READ guard, like every other instance-scoped path. Write to a
sibling temp name and RENAME into place (README: "destination exists must mean
destination is complete"). Do not `create_dir_all` the instance dir — a
missing one is `NoSuchInstance`, per WO-01.

### 2. `LifecycleManager`

Holds `ModuleStorage`, `Arc<dyn ModuleHostHandle>`, `Arc<Authenticator>` (for
`registry()`), the `EventRing`, a dialer slot set after construction (as
`host.set_dialer` is), a per-module `tokio::sync::Mutex`, and a CLOCK —
`Arc<dyn Fn() -> UnixSecs + Send + Sync>`. The pure crate never reads a clock
and neither may a test of this: tests pass their own.

- `load_or_adopt(module)` — record on disk ⇒ `Instance::from_record(key, r)`;
  none ⇒ `Instance::adopt(key, &on_disk_clients)` and WRITE it. `key` is
  `storage.current_instance(module)`.
- `on_disk_clients(module) -> BTreeMap<ClientKey, NamespaceProfile>` — every
  directory under `<module>/companions/` that holds at least one `.cbor`,
  skipping dot-directories (`.unreachable`), mapped through
  `registry.client_key` + `registry.namespace_of(..).profile`. A client NO
  namespace owns is skipped — it has no `ClientKey`, so it cannot attach. (It
  is already reported as `unowned_clients`, WO-02.)
- `apply(module, Event) -> Result<Vec<Effect>, LifecycleError>` — as above.
  `Rejected` is a typed error the admin layer maps to **409**.
- `reconcile(module)` — `instance.reconcile(&on_disk)`, `apply` each.
- `expects_follower(module) -> BootAction` — `load_or_adopt(..).on_boot()`.
- `upload(manifest, wasm, config)` — under the module lock: the existing
  `activate` (+ `write_config`), then `host.replace` ONLY IF
  `on_boot() == StartFollower`. Today's handler calls `replace`
  unconditionally, which is a third way an operator's stop gets undone.
- `boot()` and `tick()` — below.

One lock per module, held across load→step→write→effects, so two admin calls
and a tick cannot interleave on one instance.

### 3. Boot — REPLACES the `host.auto_resume()` call in `bundle.rs`

Order, and the reason for each position:

1. `migrate_v2::run` — unchanged, first.
2. community-module `auto_load` — unchanged.
3. **`migrate_flat_companions_for_module` for EVERY module.** Today this runs
   inside `dialer.start_all()`, i.e. AFTER `auto_resume`. Adoption counts
   client directories; a module whose registrations were still flat files
   would look client-less, be adopted as `Pinned`, and then NEVER retire when
   its clients later move — one zombie follower per deploy, for ever. It is
   idempotent, so calling it here and leaving the call in `start_all` is fine.
4. `manager.boot()` — for each module: `load_or_adopt`, `reconcile`, then
   execute `instance.recover()`. One bad module is logged and skipped; it must
   not stop the others (as `auto_resume` does today).
5. dialer `start_all`, THEN `manager.set_dialer(..)`. At boot the dialer is
   not set, so `StartFollower` skips the interest reconcile — `start_all` does
   that for every module, as it does today.
6. watchdog, compaction, and a NEW 60s lifecycle tick task, cancelled on
   shutdown like the other two.

Leave `ModuleHostV2::auto_resume` in place: `tests/lifecycle_v2.rs` calls it
and that file must not change. The bundle just stops calling it.

### 4. Watchdog

Replace the `count_companions(&id) == 0 ⇒ skip` test with the lifecycle:
revive iff `manager.expects_follower(id) == StartFollower` and it is not
running and not busy. Keep everything else — the busy guards, the delayed first
tick, the `WatchdogRestart` event, the interest reconcile after a revive.

Two consequences, both intended: an operator-stopped module is no longer
revived (THE fix), and a pinned module with no companions that crashes IS now
revived (v2 treated that as "a valid idle state").

### 5. Admin

- The three new routes are `POST` under `/_admin/modules/{id}/` and sit on the
  `authed` sub-router, so they are `Admin(Mutate)` with no auth work from you.
  **Add them to `MUTATING_ROUTES`** — `the_route_lists_cover_the_whole_router`
  fails until you do, which is what it is for.
- `evict`: `reconcile` first, then `Operator(Evict(mode))`.
  `Rejected::StillAttached` ⇒ the existing 409 conflict body. The old
  file-count check goes away — the lifecycle's attached set replaces it; do
  not keep both.
- `recapture` on an instance that does not expect a follower ⇒ **409**, not a
  recapture that starts it.
- `GET /_admin/status`, `GET /_admin/modules`, `GET /_admin/modules/{id}`: add
  per module, as TYPED structs — `phase`, `desired`, `attached` (client keys
  as `namespace/client`), `governing_tier`. No `serde_json::json!`.

### 6. Registrations

After a registration is written (`subscribe`) or removed (interest mutation
emptying a record; admin `delete_companion`), call `manager.reconcile(module)`.
Best-effort: log a failure at `warn` and carry on — the 60s tick reconciles
again. Do NOT try to work out which client attached or detached yourself;
`reconcile` diffs the disk against the record, and being a diff is why a path
you miss today is corrected a minute later instead of never.

## Out of scope — do not do these

- Creating an instance any way other than adoption. `Instance::staging` is for
  WO-06; nothing here calls it.
- A second instance of a module. Host slots stay keyed by module id.
- Applying `NamespaceProfile.resources` to a `ResourceBudget`.
- Leases (`DetachCause::LeaseExpired`), client rows, `ModuleRef`, GC.
- An alert WEBHOOK. `Alert` is a log line and a ring event here; D11 adds the
  transport.
- Relaxing `Rejected::StagingIsNotOperable` or anything else in
  `mitos-lifecycle`. If the machine seems to need a change, STOP and report.

## Tests that encode the old behaviour

**None.** Checked 2026-09-19: `watchdog.rs` has no tests, and neither does the
admin evict handler. So there is no sanctioned exception in this order — every
existing test passes UNCHANGED, including `tests/lifecycle_v2.rs` and the other
three integration files.

It also means the acceptance tests below are the FIRST coverage either path
has ever had. `watchdog::sweep` already takes `&dyn ModuleHostHandle`, so once
it also takes the manager it is testable with the same fake host.

**There is no fake `ModuleHostHandle` today** — `ModuleHostV2` is the trait's
only implementor, and it needs a wasmtime engine and a data plane. Write ONE
fake, in a `#[cfg(test)] pub(crate) mod testing` beside the trait (the shape
`auth::testing` already uses), that records `replace` / `stop` /
`evict_module` calls and can be told to fail. Every other trait method returns
an empty / `Ok` default. One shared fake, not one per test module: three
copies is three chances for one to stop matching the trait.

## Acceptance

Against a real `LifecycleManager` over a tempdir with a FAKE `ModuleHostHandle`
that records `replace` / `stop` / `evict_module` calls, and a test clock.

1. `nix develop -c cargo test -p mitos-platform -p mitos-core -p mitos-lifecycle`
2. New tests:
   - `an_operator_stop_survives_a_restart` — stop, build a NEW manager over
     the same dir, `boot()`; assert `replace` was never called for it.
   - `an_operator_stop_is_not_undone_by_the_watchdog` — the module HAS
     companions; run a sweep; assert no `replace`. This is the v2 bug.
   - `an_operator_stop_is_not_undone_by_an_upload`
   - `an_operator_stop_is_not_undone_by_a_subscribe`
   - `restart_starts_a_stopped_module_and_replaces_a_running_one`
   - `adoption_changes_nothing` — a v2-shaped tree with a subscribed module
     and an unsubscribed one; `boot()`; assert both are started, the first is
     `Active`/`Running` with its clients attached, the second `Pinned`; then
     tick a YEAR forward and assert no `stop`, no `evict`, no alert.
   - `flat_registrations_are_migrated_before_adoption` — a module whose only
     registration is a legacy flat file must be adopted WITH that client, not
     `Pinned`. This is the zombie-per-deploy bug; assert on `desired`.
   - `a_subscribe_attaches_and_the_last_delete_detaches` — through the real
     routers; assert on the persisted record.
   - `a_path_that_forgot_to_reconcile_is_corrected_by_the_tick` — remove a
     registration file BEHIND the manager's back, `tick()`, assert detached.
   - `an_abandoned_dev_only_instance_drains_stops_and_is_reaped` — dev
     registry, test clock: +599s nothing; +600s `stop`; +24h `evict_module`
     and the module directory is gone.
   - `a_prod_shared_instance_alerts_and_keeps_running` — the ring holds ONE
     `InstanceAlert`; a year of ticks adds none and calls no `stop`.
   - `evict_refuses_while_attached_and_force_overrides` — 409, then 200.
   - `recapture_on_a_stopped_module_is_409`
   - `a_crash_between_persist_and_effects_is_recovered` — write a record whose
     phase says `Reaped` while the directory still exists; `boot()`; assert it
     is reaped. And one that says `Active`; assert it is started.
   - `a_failed_effect_does_not_roll_back_the_record` — the fake host fails
     `replace`; assert the record still says `Active` and the watchdog retries.
   - `the_record_survives_an_upload` — stop, upload a new sha (the directory
     is renamed), assert the record is still there, still `Stopped`, and
     `load_or_adopt` rebuilds it with the NEW key.
   - `one_bad_module_does_not_stop_boot`
3. clippy `-D warnings` exit 0; `cargo check --workspace --all-targets` exit 0;
   `nix develop -c cargo test -p mitos-lifecycle` untouched and green (70).

## Deploy note (for the operator — NOT part of this order)

First boot ADOPTS every module and writes a `lifecycle.toml` beside each
`instance.toml`. Afterwards, `GET /_admin/status` should show every subscribed
module `Active`/`Running` and every unsubscribed one `Pinned` — if a
SUBSCRIBED module shows `Pinned`, its registrations were not where adoption
looked; do not deploy further until that is understood. The four retired
modules (`burn-address`, `credit-address`, `standard-burn`, `vesting-tracker`)
will be the pinned ones unless already evicted; `POST …/evict` them then.
