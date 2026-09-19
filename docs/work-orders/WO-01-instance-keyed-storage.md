# WO-01 — Instance-keyed storage + content-addressed artifacts

**Design refs:** v3 §3 (Model), D8 (CAS), D10 (GC — NOT this order).
**Lifecycle crate:** `mitos_lifecycle::ids` only (`InstanceKey`, `ModuleId`,
`Sha256Hex`). No state machine is wired in this order.
**Depends on:** nothing. **Blocks:** everything else.

## Why this is first, and why it is shaped the way it is

v3's unit of execution is the INSTANCE — `(module, wasm sha, config sha)` —
and today every byte on disk is keyed by the bare `module_id`. Until storage
can hold two instances of one module side by side, nothing else in v3 can
exist.

This is also the most dangerous order in the series: it moves production
state (a 2.1G `emissions.redb`, a 1.3G `kv.redb`) and touches a type with
**~105 call sites across 12 files**. So it is deliberately split:

> **WO-01 changes the LAYOUT and nothing else.** Every one of those 105 call
> sites keeps compiling and keeps behaving exactly as it does today.

That works because of two facts about `storage.rs`:

1. Every path is built through ONE private helper, `module_dir(id)`. The
   layout can change underneath without any caller noticing.
2. v2 has exactly one running instance per module. So `&str module_id` can be
   resolved to "that module's CURRENT instance" by a pointer file, and the
   existing `&str` API becomes a compatibility facade over the new layout.

Later orders migrate callers off the facade one subsystem at a time, as each
becomes instance-aware. Do NOT start that here, and do NOT mark the facade
`#[deprecated]` yet — there is nothing to migrate TO until WO-03.

## The new layout

```text
<root>/
  indexer_data.redb                       unchanged — platform-wide
  _cas/                                   `_` cannot begin a module id, so no collision
    wasm/<sha256>.wasm                    immutable artifact
    wasm/<sha256>.manifest.toml           its manifest, beside it
    config/<sha256>.cbor                  immutable config bytes
    orphaned-v2/<module_id>/<sha>.wasm    see "Migration" step 3
  <module_id>/
    .upload.lock                          unchanged
    current.toml                          the facade's pointer: a serialised InstanceKey
    companions/<client_id>/<key>.cbor     UNCHANGED — subscriptions belong to the
                                          module, not to one instance of it
    instances/<InstanceKey::dir_name()>/
      instance.toml                       the full InstanceKey (dir names are truncated)
      kv.redb  cursor.redb  watched.redb  emissions.redb  last-trap.toml
```

What lives where, and why:

| Data | Scope | Reason |
|---|---|---|
| wasm + manifest | artifact (CAS) | identical bytes are shared by every instance and namespace that runs them; fork and promote copy nothing |
| config | config (CAS) | it is part of instance IDENTITY, so it must be immutable once referenced |
| kv, cursor, watched, emissions, last-trap | instance | this is what "two versions side by side" means |
| companions/ | module | a subscription follows its client row between instances; it is not one instance's property |

Emissions being per-instance is safe: a companion's resume cursor is a
`ChainPoint` (`mitos-companion/src/meta.rs`), and emission ids only correlate
Ack/Nack within a delivery. Independent id sequences per instance break
nothing. (Checked 2026-09-19 — if you find a consumer that persists an
emission id across subscribes, STOP and report.)

## Files

- `crates/mitos-platform/Cargo.toml` — add `mitos-lifecycle = { path = "../mitos-lifecycle" }`
- `crates/mitos-platform/src/storage.rs` — the work
- `crates/mitos-platform/src/storage/migrate_v2.rs` — NEW (make `storage` a
  directory module if you prefer; keep `storage.rs`'s public path unchanged)
- `crates/mitos-platform/src/emissions.rs` (~line 152) and
  `crates/mitos-platform/src/trap_context.rs` (~line 495) — remove the
  implicit `create_dir_all` of the instance dir; see step 4
- `crates/mitos-core/src/bundle.rs` — one call, at the boot site (~line 320)
- `crates/mitos-platform/src/companions.rs` — ONE test fixture (~line 1482)
  that hand-writes `manifest.toml` into a module dir; make it call `activate`
- tests as listed under Acceptance

## The work

### 1. Artifact + config store

Add to `ModuleStorage` (names are a suggestion; the behaviour is not):

- `put_artifact(&Manifest, &[u8]) -> Result<Sha256Hex>` — verify
  `sha256_hex(bytes) == manifest.module.sha256` (REFUSE on mismatch, typed
  error), write `_cas/wasm/<sha>.wasm` and `.manifest.toml`, each as
  write-tmp-then-rename. If both already exist, do nothing — content-addressed
  means a second put of the same sha is a no-op, not an overwrite.
- `put_config(&[u8]) -> Result<Sha256Hex>` — same, under `_cas/config/`.
- `artifact_path(&Sha256Hex)`, `read_artifact_manifest(&Sha256Hex)`,
  `read_config_bytes(&Sha256Hex)`.

Use `manifest::sha256_hex` — do not add a second hasher.

### 2. Instance-keyed paths

- `instance_dir(&InstanceKey) -> PathBuf` =
  `<root>/<module>/instances/<key.dir_name()>`.
- Re-point the private path helpers (`kv_path`, `cursor_path`,
  `watched_path`, `emissions_path`, `last_trap_path`) at the instance dir.
- `write_instance_marker(&InstanceKey)` writes `instance.toml`. `dir_name()`
  TRUNCATES both digests to 16 hex chars, so the directory name alone cannot
  reconstruct the key; the marker is the source of truth and `list_instances`
  reads it.
- `list_instances(&ModuleId) -> Vec<InstanceKey>`.

### 3. The compatibility facade

- `current_instance(module_id: &str) -> Result<Option<InstanceKey>>` reads
  `<module>/current.toml`.
- `set_current(&InstanceKey)` writes it, write-tmp-then-rename.
- EVERY existing `&str`-taking public method keeps its exact signature and
  resolves through `current_instance`. A module with no `current.toml` behaves
  as an unregistered module does today (`read_manifest` → `Ok(None)`, etc.).
- The handle caches (`cursor_stores`, `kv_stores`, …) stay keyed by module id
  in this order. They hold open fds; see step 4 for why that matters.
- `list_modules()` = directories that have a `current.toml`.
- `module_dir_for_companions` is UNCHANGED — still `<root>/<module_id>`.

### 4. `activate` and `write_config` — preserving v2's replace semantics

Today, uploading a new sha swaps a symlink and the new code RESUMES ON THE OLD
STATE AND CURSOR. v3 will replace that with a proper client move (WO-04), but
in this order the behaviour must be identical. In the new layout a new sha is
a new `InstanceKey`, hence a new directory name — so:

- `activate(manifest, wasm)`:
  1. `put_artifact`.
  2. `new_key` = (module, new sha, the CURRENT instance's config sha, or
     `None` if the module is new).
  3. If a current instance exists and `new_key != current`: **rename** its
     instance directory to `new_key.dir_name()`, rewrite `instance.toml`, then
     `set_current(new_key)`. If none exists: create the directory, write the
     marker, `set_current`.
- `write_config(id, bytes)`: `put_config`, then the same rename-and-repoint
  with the new config sha. (An upload WITHOUT a config field leaves the config
  as it was — that is today's behaviour; keep it.)

**The rename runs while the module's follower is LIVE** (the upload handler
calls `activate` and only then `host.replace()`). Two things make that safe,
and BOTH are part of this order:

1. *Open handles survive it.* Renaming a directory whose redb files are open
   is fine on unix — the fds stay valid and redb never reopens by path. Say so
   in a comment at the rename: it looks wrong and is not.
2. *Nothing may re-create the old path.* Today it can. There are SEVEN implicit
   `create_dir_all` calls on the module dir — `storage.rs` (`kv_store`,
   `watched_index`, `cursor_store`, `acquire_upload_lock`, `activate`,
   `write_config`), `emissions.rs:152`, and `trap_context.rs:495`, which
   writes `last-trap.toml`. A trap firing between the rename and the repoint
   would resurrect the OLD instance directory, leaving two, and the
   crash-repair rule below would (correctly) refuse to guess. So:
   - **An instance directory is created in exactly ONE place** — the
     `activate` path (and the migration). Every other open REQUIRES it to
     exist and returns a typed `StorageError::NoSuchInstance` otherwise.
     Remove the implicit creates from the instance-scoped opens; keep
     `create_dir_all` only for the MODULE dir in `acquire_upload_lock` (the
     lock file lives there and must work for a brand-new module).
   - **Resolution and repoint exclude each other.** One `RwLock<()>` on
     `ModuleStorage`: every `&str → InstanceKey` resolution holds it for READ
     across resolving AND building the path; rename-and-repoint holds it for
     WRITE. One lock for the whole store, not one per module — a repoint is
     rare and takes microseconds, and a single lock cannot deadlock.

Ordering inside the rename matters for crash safety: rename the directory
FIRST, `set_current` LAST. A crash in between leaves `current.toml` naming a
directory that no longer exists; make `current_instance` detect exactly that
case — pointer names a missing dir, and exactly one instance dir exists for
the module — and repair the pointer, logging at `warn`. Any other mismatch is
an error, not something to guess at.

### 5. One-shot migration of the v2 layout — `migrate_v2.rs`

Called ONCE from `bundle.rs`, immediately after `ModuleStorage::new` and
BEFORE anything opens a redb file, runs auto-load or calls `auto_resume`.

A module directory is v2 if it has a top-level `manifest.toml` and no
`current.toml`. For each, in this order:

1. Parse `manifest.toml` → wasm sha. Hash `config.cbor` if present → config
   sha. Build the `InstanceKey`.
2. COPY the current `<sha>.wasm` and `manifest.toml` into `_cas/wasm/`; COPY
   `config.cbor` into `_cas/config/`. Copy, not move — the originals are the
   fallback until step 6.
3. MOVE every OTHER `<sha>.wasm` in the directory to
   `_cas/orphaned-v2/<module_id>/`. v2 kept ~20 old shas per module as
   "rollback targets" but only ever kept the CURRENT manifest, so they cannot
   be validated or started. They are not deleted here — a migration moves
   data, it does not judge it. GC is a later order.
4. Create `instances/<dir_name>/` and MOVE `kv.redb`, `cursor.redb`,
   `watched.redb`, `emissions.redb`, `last-trap.toml` into it (whichever
   exist). Same filesystem ⇒ `rename`, instant even for the 2.1G file. If a
   rename fails with `EXDEV`, STOP with an error — do not fall back to copy;
   a half-copied redb is worse than an unmigrated one.
5. Write `instance.toml`, then `current.toml`. **`current.toml` is the commit
   point** — it is what makes the directory stop being "v2".
6. Only now remove the originals: `current.wasm` (symlink), the current
   `<sha>.wasm`, `manifest.toml`, `config.cbor`.

Requirements:

- **Idempotent and resumable.** Every step tolerates "already done" (source
  missing AND destination present ⇒ skip). A crash at ANY point followed by a
  restart must converge on the same final layout. There is a test for this.
- **`MITOS_STORAGE_MIGRATION`** env, a named enum not a flag:
  `apply` (default) | `plan`. `plan` logs every move it WOULD make at `info`
  and changes nothing, then the host exits non-zero with a clear message. The
  operator runs `plan` on the box before the real deploy.
- Returns a typed `MigrationReport { migrated, already_v3, orphaned_artifacts }`
  which `bundle.rs` logs as ONE structured line. A module that fails to
  migrate is logged at `error` and SKIPPED — it must not stop the others, and
  it must not be half-migrated (see idempotency).

## Out of scope — do not do these

- No change to `host_v2.rs` slots, the dialer, the admin API's shapes or
  routes, `SubscribeTarget`, or anything in `mitos-protocol`.
- No second instance of any module is ever created by this order.
- No GC of artifacts or of `orphaned-v2/`.
- No wiring of `mitos_lifecycle::instance` / `client_move` / `lease`.
- No `#[deprecated]` on the facade.
- No `reflink` — nothing is copied between instances yet (that is WO-04).
- Do not touch `scripts/deploy.sh`.

## Tests that encode the old behaviour

These assert the v2 LAYOUT and must be rewritten to assert the v3 layout —
same intent, new paths. This is the one sanctioned exception to "never change
an assertion", and it covers ONLY these:

- `storage.rs::activate_writes_artifact_and_symlink` → asserts the artifact is
  in `_cas/wasm/`, the marker and `current.toml` exist. Rename the test.
- `storage.rs::activate_replaces_symlink_atomically` → asserts that activating
  a second sha leaves ONE instance dir, renamed, with its state files intact
  and `current.toml` naming the new key. Rename the test.
- `companions.rs` fixture (~line 1482) — stop hand-writing `manifest.toml`;
  call `activate`. Its assertions do not change.

Every other existing test must pass UNCHANGED. If one does not, your change is
wrong.

## Acceptance

Run each; quote the result line in the report.

1. `nix develop -c cargo test -p mitos-platform` — all pass, including the
   integration tests `lifecycle_v2`, `dispatch_v2`, `dynamic_interest_v2`,
   `bootstrap_by_policy_v2` **unmodified**. They are the proof the facade
   preserved behaviour: `start_replace_stop_roundtrip_v2` in particular
   exercises activate → start → replace → stop end to end.
2. `nix develop -c cargo test -p mitos-core` — all pass.
3. New tests in `storage.rs` / `migrate_v2.rs`:
   - `put_artifact_refuses_bytes_that_do_not_match_the_manifest`
   - `put_artifact_twice_is_a_no_op` (assert mtime or content unchanged)
   - `two_configs_of_one_wasm_are_two_instance_dirs`
   - `activating_a_new_sha_carries_state_forward` — write a cursor, activate a
     second sha, read the cursor back through the `&str` facade.
   - `a_crash_between_rename_and_repoint_is_repaired_on_read`
   - `opening_state_for_a_missing_instance_is_an_error_not_a_mkdir` — for
     each of kv / cursor / watched / emissions / last-trap: resolve against a
     module whose instance dir was removed; assert `NoSuchInstance` AND that
     no directory was created.
   - `a_trap_written_during_activate_lands_in_the_new_directory` — hold the
     write lock's critical section open with a test hook, write a last-trap
     from another thread, release; assert exactly ONE instance dir exists and
     it contains the trap file.
   - `migrates_a_v2_directory` — build a realistic v2 dir in a tempdir
     (current sha + 3 old shas + all four redb files + `config.cbor` +
     `companions/<client>/<key>.cbor` + `last-trap.toml`); assert the final
     layout file by file, that `companions/` did not move, that the old shas
     are under `orphaned-v2/`, and that the cursor reads back identically.
   - `migration_is_resumable_from_every_step` — for EACH of steps 1–6, run the
     migration with a fault injected after that step, then run it again clean,
     and assert the same final layout as an uninterrupted run. Inject the
     fault through a test-only hook, not by copying the function.
   - `migration_is_idempotent` — run twice; second report is all `already_v3`.
   - `plan_mode_changes_nothing` — snapshot the tree (paths + sizes), run in
     `plan`, assert the tree is identical.
   - `a_module_that_fails_to_migrate_does_not_stop_the_others`
4. `nix develop -c cargo clippy -p mitos-platform -p mitos-core --all-targets -- -D warnings` exits 0.
5. `rg -n '"(kv|cursor|emissions|watched)\.redb"|"current\.wasm"' --type rust crates/mitos-platform/src crates/mitos-core/src | rg -v 'storage'`
   returns only test-local tempfile paths (`watched_ref_index.rs:250`,
   `emissions.rs:824`) — i.e. nothing outside `storage` builds a layout path
   by hand. Confirm the search CAN match first (it matches inside `storage`).
6. Goldens stay **69/69** (see the README for the two staleness traps).
   Storage does not touch decode, so a change here means something else moved.

## Deploy note (for the operator — NOT part of this order)

The first boot on the new binary moves production state. Before it:
`cp -a /var/lib/mitos/modules /var/lib/mitos/modules.pre-v3` (6.8G, ext4 so no
reflink), then start once with `MITOS_STORAGE_MIGRATION=plan` and read the log.
Preprod first — it is the host-binary staging environment.
