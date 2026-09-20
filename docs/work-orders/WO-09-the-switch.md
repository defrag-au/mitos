# WO-09 — The switch: a client changes instance

**Design refs:** v3 D4 step 5 (REWRITTEN for this order — read it again), D10.
**Lifecycle crate — read docs AND tests:**
- `handoff` (written for this order) — the module doc is the design of this
  whole order. `Handoffs`, `Admit`, `align`, `Alignment`, `catch_up`.
- `routing::succession` (written for this order), and `fan_out`,
  `attached_to`, `drain_pass`.
- `client_move` — the arms of `progress` from `CatchingUp` to the commit;
  `abandon`; `recover`. Note `CommitLost` now RESUMES first.

**Depends on:** WO-08. **Blocks:** WO-10 (the refill), and with it the mainnet
gate.

## What this order does, and where it deliberately stops

WO-08 parks every move at `CatchingUp`, holding the host's one catch-up slot
for ever. This order carries a move to its end: the staging instance is seen
to have caught up, both followers are quiesced, the row is committed, the
client is served by its new instance, and the slot is released.

**There is no "pause both at the same block boundary", and you will not build
one.** Read `handoff`'s module doc: the switch is `stop_instance` on each
follower (the quiesce `upload` and `create_staging` already use), a check that
staging is not AHEAD of live, and a per-client WATERMARK on the staging
instance that withholds the blocks its predecessor already delivered.

**It stops short of two things, on purpose:**
- **A move whose plan carries a refill (`plan.refill != Refill::None`) is NOT
  advanced past `CatchingUp`.** `RunRefill` is issued once, by the transition
  that clears the pending move — the shape WO-08's review showed cannot be
  recovered after a crash — and a lost `recapture` is a client whose state
  silently never got rebuilt. WO-10 makes the debt durable and pays it. Until
  then such a move waits, visibly (§1).
- The advisory emission diff (D7). `HoldAndDiff` and `FinishDiff` stay no-ops;
  nothing about the switch's correctness needs them.

## You are wiring, not deciding

| The host observes… | …and feeds |
|---|---|
| (moves tick) `handoff::catch_up(live cursor, staging cursor)` is `CaughtUp`, for a move in `CatchingUp` | `Progress::CaughtUp` |
| both followers quiesced and `handoff::align` says `Commit` | `Progress::Quiesced { queue }` |
| the watermark is written (the `CommitSwitch` effect ran) | `Progress::SwitchCommitted` |
| (moves tick) a move in `WaitingForDrain` whose client has nothing queued on the live instance | `Progress::QueueDrained` |
| `ResolveCommit` (only ever from `recover()`) | `Progress::CommitLost` — always. The commit IS the row write that clears the pending move; a row that still says `Committing` is a commit that did not land |

| Effect | In THIS order |
|---|---|
| `PauseAtBoundary { live, staging }` | §2 |
| `ResumeFollowers { .. }` | release the quiesce (§2). Idempotent: nothing held ⇒ nothing to do |
| `PurgeClientEmissions { on }` | delete that client's `Queued` rows from `on`'s emissions store — with its follower stopped, which it is |
| `CommitSwitch { from, to }` | §3 |
| `AttachTo` / `DetachFrom` | reconcile, as in WO-08 — then §4 |
| `ReleaseCatchUpSlot` | as in WO-08 |
| `HoldAndDiff`, `FinishDiff` | nothing, with a comment |
| `RunRefill(_)` | unreachable (§1 stops those moves). Keep the loud `error!` |

## Files

- `crates/mitos-platform/src/moves.rs` — the tick, the effects.
- `crates/mitos-platform/src/lifecycle.rs` — `quiesce_pair`, succession.
- `crates/mitos-platform/src/storage.rs` — `handoffs.toml` per instance.
- `crates/mitos-platform/src/host_v2.rs` — the fan-out asks `Handoffs`.
- `crates/mitos-platform/src/rows.rs` — `RowCache` freshness.
- `crates/mitos-platform/src/admin.rs` — what the rows route shows.
- `docs/HOWTO_CONSUMING_A_COMMUNITY_MODULE.md` — what a worker sees.

## The work

### 1. The moves tick — and EVERY phase gets a sweep

`MoveManager::tick` already runs. For each row with a pending move, under the
row lock:

- `CatchingUp`: if `plan.refill != Refill::None`, do nothing, and say why ONCE
  per move (`info`, and as a `blocked` reason on the rows route — not per
  tick). Otherwise read both cursors (`read_cursor_at`); `catch_up` ⇒
  `CaughtUp` ⇒ feed it. A cursor that will not read, or an instance with no
  cursor yet, is "still behind".
- `Quiescing`: **re-execute `PauseAtBoundary`.** The effect is issued once and
  §2 deliberately gives up when staging is ahead — so the retry is this tick,
  by name. (README, "Executing an effect": never write "the tick will retry
  it" without the line of the tick that does. This is that line.)
  **But look before you stop anything.** Every attempt stops the LIVE
  follower — a production client's — for as long as its block in flight
  takes. So first read both cursors as they stand, unlocked, and only
  attempt the quiesce when `handoff::align` on THOSE already says `Commit`.
  It is a hint, not the decision (§2 asks again with both stopped), but
  without it a staging instance that sits a block ahead would pause prod's
  follower once a minute for as long as it stayed there. The same pre-check
  guards the first attempt, straight after `CaughtUp`.
- `WaitingForDrain`: count the client's `Queued` + `Pending` rows in the LIVE
  instance's store; zero ⇒ `QueueDrained`.
- `Committing`: re-execute `CommitSwitch` (§3 is idempotent).

### 2. `PauseAtBoundary` ⇒ `LifecycleManager::quiesce_pair`

Stopping a follower is the manager's, under the module lock (WO-07's review).

`quiesce_pair(live, staging) -> Result<Quiesced, QuiesceError>` takes the
module lock and RETURNS HOLDING IT — `Quiesced` owns a
`tokio::sync::OwnedMutexGuard`, the two keys and the two cursors. While it is
alive no upload, revive, reap or `create_staging` can touch the module, which
is the point: from here to the commit both followers must stay stopped, and
the watchdog's `revive` is exactly the thing that would restart one.

1. `stop_instance(staging)`, then `stop_instance(live)` — staging FIRST, so
   live has every chance to be at or past it. If EITHER stop fails: restart
   whatever was stopped (per its record's `on_boot`, as `copy_from_parent`
   does) and return the error. A commit over a follower that may still be
   running is WO-06's bug.
2. Read both cursors. `handoff::align(live, staging)`:
   - `StagingAhead` ⇒ resume both, drop the guard, return
     `QuiesceError::StagingAhead`. The move stays `Quiescing`; §1's tick tries
     again. Do NOT wait for live to catch up with two followers stopped.
   - `Commit { after }` ⇒ return the `Quiesced`.
3. `Quiesced::resume(self)`: start each follower its record wants running,
   then drop the guard. **`Drop` must do the same** (spawned — `Drop` cannot
   await), because a panic or an early `?` between quiesce and resume would
   otherwise leave a production follower stopped until the watchdog's next
   sweep. There is a test.

The `MoveManager` holds the `Quiesced` in a field keyed by `(module, client)`
between effects — `PauseAtBoundary` stores it, `CommitSwitch` and
`PurgeClientEmissions` use it, `ResumeFollowers` takes and resumes it. All of
these run back-to-back on one task inside one `execute_all`; the field exists
because they are separate effects, not because time passes.

`ClientQueue` for `Quiesced { queue }`: `Pending` iff the client has any
`Queued` or `Pending` row in the LIVE store.

**What is accepted, and logged:** both followers are stopped from the quiesce
to the resume — milliseconds of work, plus however long each takes to finish
its block in flight. Log the span (`info`, millis) and put it on the event
ring, like `StagingCopy`.

### 3. `CommitSwitch { from, to }`

With the `Quiesced` in hand (no `Quiesced` ⇒ this is a re-issue after a crash
⇒ feed `CommitLost`, which resumes and catches up again):

1. `Handoffs` for the `to` instance: load `<instance>/handoffs.toml` (absent ⇒
   default), `begin(client, after)`, write-then-rename. `from: None` (a first
   subscribe) writes no watermark.
2. Feed `Progress::SwitchCommitted`. **Persisting the row that transition
   returns is THE commit** — `fan_out` reads rows, so the row write is the
   routing change, atomically, for every companion of the client.

Two writes, one window: watermark written, row not. Harmless — a watermark
for a client the row does not place here is never consulted, and the next
commit overwrites it. Say so in a comment and test it.

**Who writes `handoffs.toml`:** the manager, ONLY while that instance's
follower is stopped (here); and that instance's own emission drain, ONLY while
it runs (§5). `stop_instance` awaits the drain task, so the two never overlap.
Nothing else, ever — in particular not an admin route.

### 4. Succession, after the reconcile

`SwitchCommitted` issues `AttachTo(to)` / `DetachFrom(from)`, which reconcile.
Then, under the module lock: `routing::succession(module_current,
attached_to(current…), to)`. `MovesTo(k)` ⇒ `storage.set_current(&k)`. Put
this in `LifecycleManager` next to `reconcile`, and ALSO run it from the
lifecycle tick for every module with more than one instance — the effect that
triggers it is issued once, by a transition that clears its own state.

Without it the emptied current instance is refused its reap
(`CurrentInstanceHasSiblings`) on every tick for ever.

### 5. The fan-out honours the watermark

`run_emit_drain` loads the instance's `Handoffs` when it starts and owns it.

- Forward emission: after `routing::fan_out` says `Deliver`, ask
  `handoffs.admits(client, slot)`. `Withheld` ⇒ no row — but like a `Skip`,
  the companion still counts as SEEN (`saw_companion`), or the sentinel logic
  will keep an event that was in fact delivered (WO-06's catch).
- Undo: `handoffs.rolled_back(to)` BEFORE fanning the undo out; persist on
  `Changed::Yes`. The undo itself is never withheld.
- `handoffs.settle(slot)` on each event; persist on `Changed::Yes`.
- `match` on `Admit` and on `Changed`, never `==`.
- A `handoffs.toml` that will not parse: `error!`, and treat the instance as
  having NO watermarks. That can deliver a block twice, which a companion
  tolerates ("applies are chain-point-idempotent"); refusing to start would
  lose every block instead.

### 6. `RowCache` must see the commit

It trusts mtime, and `Committing` → committed can be two row writes inside one
timestamp tick. The FOLLOWERS' caches are safe by construction — both were
stopped across the commit and rebuild theirs on start; say so in a comment.
The dialer's is not: key freshness on `(mtime, len)`. A committed row has lost
its `pending` table, so its length always changes.

### 7. Admin

The rows route: a `blocked` reason (§1), and for a committed client its
watermark on the instance serving it. `GET /_admin/modules/{id}/instances`:
each instance's watermarks. Read-only.

## Out of scope — do not do these

- Any refill; `RunRefill`. WO-10.
- `FanOut::Hold`, held rows, the diff.
- A follower pause primitive, or anything that waits for two cursors to meet.
- A timeout on `WaitingForDrain`. A client whose companion is down keeps its
  move — and the host's slot — until it recovers or re-announces. That is
  the client lease's job; it is recorded against that order in the index.
- An upload whose key collides with a staging instance (still `InstanceExists`).
- `grant_slot` releasing on a transient error (WO-08's review note).
- Any change to `mitos-lifecycle`. If you need one, stop and report.

## Tests that encode the old behaviour

`a_move_parks_at_catching_up_and_nothing_feeds_caught_up` (`moves.rs`) is now
false by design. Sanctioned: rename it
`a_move_with_a_refill_waits_at_catching_up_until_the_refill_order_lands`, give
its announced artifact an upgrade entry carrying `recapture`, and keep BOTH
its assertions (phase `CatchingUp`; the client's `current` has not moved).
WO-08's end-to-end test may assert `CatchingUp` after `settle()` and is
unaffected — nothing in `settle()` runs the tick. If any other test must
change, stop and report.

## Acceptance

1. `nix develop -c cargo test -p mitos-platform -p mitos-core -p mitos-lifecycle`;
   `git diff --stat crates/mitos-platform/tests` shows additions only.
2. clippy `--workspace --all-targets -D warnings` 0;
   `cargo check --workspace --all-targets` 0; `cargo fmt --check` clean.
3. `quiesce_pair` (`lifecycle.rs`, `FakeHost`):
   - `staging_is_stopped_before_live`.
   - `staging_ahead_resumes_both_and_holds_nothing`.
   - `a_follower_that_will_not_stop_aborts_the_quiesce_and_restarts_the_other`.
   - `nothing_else_can_touch_the_module_while_it_is_quiesced` — `revive`,
     `upload` and `create_staging` all wait; under a timeout.
   - `a_dropped_quiesce_restarts_both_followers`.
   - `an_operator_stopped_instance_is_not_started_by_a_resume`.
4. The switch (`moves.rs`):
   - `a_caught_up_move_commits_and_the_client_is_served_by_the_new_instance` —
     row `current` = target, `pending` none, slot released, the next queued
     move granted.
   - `a_staging_instance_still_behind_is_left_alone`.
   - `staging_ahead_leaves_the_move_quiescing_and_the_next_tick_retries_it`.
   - `a_quiesce_is_not_attempted_while_the_running_cursors_say_staging_is_ahead`
     — assert NO `StopInstance` in `FakeHost::calls()` across three ticks.
   - `a_client_with_rows_still_queued_drains_before_it_switches` and
     `…_or_has_them_purged_when_the_plan_says_so`.
   - `a_crash_between_the_watermark_and_the_row_commits_nothing` — then boot,
     and the move catches up and commits again.
   - `a_crash_after_the_row_leaves_both_followers_restarted_by_boot`.
   - `a_newer_announcement_mid_quiesce_resumes_both_followers`.
   - `two_clients_moving_to_one_instance_commit_one_after_the_other`.
5. Fan-out (`host_v2.rs`, beside WO-06's):
   - `blocks_at_or_before_the_watermark_are_withheld_and_later_ones_delivered`.
   - `a_withheld_block_is_not_kept_as_unclaimed`.
   - `a_rollback_below_the_watermark_lowers_it_and_is_itself_delivered` —
     assert the file on disk.
   - `an_unreadable_watermark_file_delivers_rather_than_refusing_to_start`.
6. **The property, end to end, real host** (`tests/two_instances_v2.rs`):
   `every_block_reaches_a_moving_client_exactly_once` — live and staging
   followers, a switch in the middle, and the union of rows written for the
   client across both stores covers every block once. Use WO-08's rollback
   trick if one fixture block is not enough to tell.
7. Succession (`lifecycle.rs`):
   - `the_pointer_follows_the_last_client_out_and_the_old_instance_is_reaped`.
   - `a_dev_client_forking_away_does_not_move_the_pointer`.
   - `the_tick_repoints_a_module_whose_succession_effect_was_lost`.
8. Dialer: `a_row_committed_within_one_timestamp_tick_is_seen`.
9. Goldens 69/69.

## Deploy note (for the operator — NOT part of this order)

After this a dev worker's announcement on PREPROD completes: watch the rows
route go `queued → preparing → catching-up → (committed)`, the instances route
show the watermark, and the old instance drain, stop and — after its retain
window — go. Compare the companion's applied blocks across the switch for a
gap or a repeat. **Mainnet stays gated** on WO-10 (a module whose upgrade
needs a `recapture` cannot move yet) and on per-instance memory ceilings.
