# WO-05 — A worker announces its module; the host pulls it

**Design refs:** v3 D4 (client move), D8 (pull by digest).
**Lifecycle crate — read ALL of these, docs and tests:**
- `client_move` — `ClientRow`, `ClientRow::adopt` (written for this order),
  `announce`, `recover`, the `Effect` and `Progress` enums.
- `artifact` — `ArtifactSource` and `ModuleRef::announced_by` (written for this
  order), `BuiltModule`.
- `upgrade::validate`.
- `access` — `Registry::may_announce` (written for this order) and its two
  tests. It is NOT `namespace_of`; the difference is the security property.

**Depends on:** WO-01, 02, 03, 04. **Blocks:** WO-06.

## Implemented 2026-09-19 — open items for review

Acceptance passes: `mitos-protocol` 41, `mitos-companion` 31,
`mitos-platform` 238, `mitos-core` 28, `mitos-lifecycle` 87, the four
integration files UNMODIFIED except the one sanctioned field;
`cargo check -p mitos-protocol -p mitos-companion --target wasm32-unknown-unknown`
exit 0; clippy `--workspace --all-targets -D warnings` 0; workspace check 0;
`cargo fmt --check` 0 diffs; goldens 69/69.

### ⚠️ BLOCKING — the manifest digest does not survive a restart

**The three lines that matter**, all in `crates/mitos-lifecycle/src/client_move.rs`:

```rust
// :123 — the PERSISTED part of a row. No manifest digest.
pub struct Move { target, url, intent, revision_policy, stage }

// :474 — announce() builds it from the ModuleRef, which HAS
// `manifest: Sha256Hex` (:74), and drops it on the floor.
next.pending = Some(Move { target, url: module_ref.url, intent, .. });

// :369 — so recover() can only re-issue this, and the executor has
// nothing to run verification step 2 against.
Stage::Fetching => vec![Effect::EnsureArtifact { url, wasm }]
```

Re-fetching anyway would mean accepting whatever manifest the worker serves at
that moment — exactly what pinning the manifest exists to prevent (a manifest
with `recapture` quietly removed). So I did NOT weaken the check. Instead a
re-issued `EnsureArtifact` with no digest in hand fails the move with a named
reason and the client's next announcement re-drives it, which costs nothing
operationally: every one of a client's companions re-announces on every DO
wake. See `MoveManager::cannot_verify` in `crates/mitos-platform/src/moves.rs`.

**This means acceptance 7's `boot_refetches_a_row_that_was_fetching` is not
satisfiable as written.** The test is there as
`boot_cannot_verify_a_row_that_was_fetching_and_says_so`, asserting the
behaviour above and pointing here. It is the one acceptance item this order
does not meet.

Two ways to close it, both above my remit:

1. **Put the digest on `Move` and on the effect.** Roughly:
   - `Move` gains `pub manifest: Sha256Hex` (`:123`), set from
     `module_ref.manifest` at `:474`;
   - `Effect::EnsureArtifact` gains `manifest: Sha256Hex` (`:223`), so the
     effect carries everything its executor needs and `recover()` at `:369`
     re-issues a complete one;
   - host side: `Pinned` in `moves.rs` disappears, `cannot_verify` disappears,
     `ensure_artifact` reads the digest off the row, and the test reverts to
     the order's `boot_refetches_a_row_that_was_fetching`.

   It is additive to a serialised struct, so an existing row on disk would
   fail to parse — but no row exists anywhere yet, so that costs nothing
   today and will not be true after this ships.
2. **Persist it host-side beside the row.** A second file and a second crash
   window per row — the README's own warning about multi-step writes — so I
   did not.

I think (1) is right, and doing it BEFORE this ships is what makes the
migration question moot. But it changes the pure crate's persisted shape, so
the order says report rather than guess.

### Deviations

1. **`AnnouncedModule::from_built` lives in `mitos-protocol`, not
   `mitos-companion`.** The order puts it under §2 (the worker's side), but
   `AnnouncedModule` is the wire type and `mitos-companion` would otherwise
   need `mitos-lifecycle` just to name `BuiltModule`. `mitos-protocol` gained
   `sha2` + `hex` for the digest check; both are pure Rust and the wasm32
   check passes.
2. **`validate_with_wasmtime` moved to `inspect.rs` as
   `validate_component`**, with its own `ComponentRefused` error, as the order
   asked. `inspect.rs` already owned wasmtime-side validation for both
   callers.
3. **`HttpFetcher` uses `Response::chunk()`, not `bytes_stream()`.** The
   latter needs reqwest's `stream` feature, which is a workspace-wide
   change for no extra guarantee — `chunk()` gives the same
   stop-at-`max`-while-reading behaviour.
4. **`Effect::EnsureArtifact`'s `url` is ignored**; the puller re-derives it
   from `ArtifactSource::for_client`. That keeps "the host derives the URL" a
   single code path rather than two that must agree.
5. `RowStage` is a host-side enum rather than a rendering of the lifecycle
   crate's `Stage`, which carries the resolved plan an operator does not need.

### Found but not fixed

- `crates/mitos-platform/src/moves.rs` — `MoveError` is declared and never
  constructed; every refusal is a `String` reason on `AnnounceResult`. Left
  because the order's contract is "an announcement never fails the
  subscribe", so there is no error to return. Worth deleting or using in
  WO-06.
- `Effect::ReleaseCatchUpSlot` and `RequestCatchUpSlot` are no-ops here, so
  nothing records WHO is waiting. The order says "add the row to an in-memory
  queue"; since nothing drains it and a restart empties it, I left the queue
  out rather than build one that only ever grows. WO-06 needs a real one.

### Questions

The blocking one is above. One smaller: `announce` returns `Accepted` as soon
as the move is recorded, so a client whose artifact then turns out to be
unreachable still sees `Accepted` in its subscribe response, with the refusal
only on the event ring. That is what "an announcement never fails the
subscribe" implies, and the alternative (await the fetch, report its outcome)
would make a subscribe wait on a download — but it is worth confirming the
response semantics are what you want.

## Review 2026-09-19 — the blocker closed, two defects fixed

Acceptance re-run independently and matches. Commit scope matches "Files"
(plus `inspect.rs`, `host_v2.rs`, `lifecycle.rs`, each justified); no
`#[ignore]`; `tests/common/mod.rs` has exactly the sanctioned field. Row and
manifest writes are write-then-rename; the size cap stops the READ; the
half-stored-artifact window was found and tested without being asked for.
After review: lifecycle 88, platform 242, clippy `--workspace -D warnings` 0,
workspace check 0.

**1. The blocker — a gap in the PURE crate, and correctly reported rather
than worked around.** `Move` dropped the manifest digest, so the pin held only
as long as the process did. Closed as option (1), and further: `url` is gone
from `ModuleRef`, `Move` and `Effect::EnsureArtifact` altogether (deviation 4
had already made it dead weight — one derivation, nothing stored that could
disagree with it), and `manifest` takes its place on `Move` and on the effect.
Host side, `Pinned` and `cannot_verify` are deleted. Tests:
`a_fetch_interrupted_by_a_restart_is_reissued_with_both_pins` (pure, THROUGH
toml), `boot_refetches_a_row_that_was_fetching` (as the order named it), and
`a_refetch_after_a_restart_still_holds_the_manifest_to_its_pin` — the worker
swaps its manifest between crash and restart, which is the attack the pin
exists for. How it got through: my recovery test started every case from
`Planned`. `Fetching` was the one stage it never recovered from.

**2. DEFECT — a subscribe waited on the download, under the row lock.**
`announce` awaited `execute_all`, which awaited the pull: up to two 30-second
fetches inside a subscribe handler with a Durable Object on the other end, so
"an announcement never fails the subscribe" was false by timeout; the row lock
was held throughout; and `boot` awaited every unreachable worker in turn
before the host finished starting. The report's closing question ("`Accepted`
as soon as the move is recorded") described the intent, not the code —
`an_announcement_never_fails_the_subscribe` could only assert the failure was
on the ring when the response returned BECAUSE the fetch was inline. Now
`spawn_fetch`: the pull runs on its own task holding no lock, and takes the
row lock only to report. `MoveManager::new` returns `Arc<Self>`
(`new_cyclic`); `settle()` waits for running fetches (tests, shutdown). Tests:
`an_announcement_does_not_wait_for_the_download` and
`boot_does_not_wait_on_an_unreachable_worker`, both against a HELD fetcher and
both under a timeout so a regression fails instead of hanging. That answers
the question too: `Accepted` means the move was recorded; the artifact's fate
is on the ring and the row.

**3. DEFECT — a stored artifact skipped the checks about the ANNOUNCEMENT.**
The CAS short-circuit returned the stored manifest untested. The digests and
the ABI gate are properties of the bytes, settled when stored; module, wasm
and REVISION agreement are properties of what this client just claimed. The
rollback rule runs on the announced revision before any fetch, so a client
could name the digests of a stored revision-3 build, announce revision 9, pass
the rule, and carry the 3. Now `agrees_with()` runs on both paths. Test:
`a_stored_artifact_is_still_held_to_the_announced_revision`.

Agreed and left: deviations 1–5; `MoveError` unused (delete in WO-06); no slot
queue (right — one that only grows is worse than none).

## What this order does, and where it deliberately stops

A worker says, on subscribe, "this is the module I was BUILT against". The host
records that in a persisted CLIENT ROW, fetches the artifact into its content
store, verifies it, and works out what the move would involve.

**And then it stops.** `client_move` runs `Announce → Fetching → Queued`, and no
further: nothing here grants a catch-up slot, creates an instance, or switches
a client. That is WO-06. So after this order a worker that announces keeps
being served by exactly the instance it is on today, and what changes is
observable only: the artifact is in the CAS, and `GET …/rows` shows a pending
move parked at `queued`.

That makes it safe to deploy ahead of any worker using it — a worker that
announces nothing gets a byte-identical subscribe.

## You are executing effects, not deciding

| The host observes… | …and feeds `ClientRow::step` |
|---|---|
| a subscribe carrying an announcement for a module it targets | `Event::Announce { module_ref, intent: Announced, context }` |
| a fetch finished and verified | `Progress::ArtifactReady { upgrades }` — the manifest's `lifecycle.upgrade` |
| a fetch failed, or verification refused it | `Progress::Failed(MoveFailure::ArtifactRefused)` |
| a client no longer has any registration for the module (checked on a tick) | `Event::Withdraw { cause: DetachCause::Unsubscribed }` (`DetachCause` lives in `instance`) |
| boot | `row.recover()` — execute what it returns |

| Effect | In THIS order |
|---|---|
| `EnsureArtifact { url, wasm }` | spawn the fetch (§4). Already in the CAS ⇒ answer at once, no network. |
| `RequestCatchUpSlot` | add the row to an in-memory queue. **Never grant it.** |
| `ReleaseCatchUpSlot` | remove it from that queue |
| `Alert(MoveAlert::Failed(_))` | `tracing::error!` + a new `EventKind::MoveFailed { client_id, reason }` |
| `DetachFrom { .. }` (only from `Withdraw`) | **ignore, with a comment.** WO-03's `reconcile` already detached the client from the instance when its registrations vanished. Who owns attach/detach once rows drive moves is WO-06's question; do not answer it here. |
| anything else | unreachable before a slot is granted. Log at `error` naming the effect — never `panic!`, never `unreachable!()`. |

**Persist, THEN execute**, exactly as `LifecycleManager::apply` does: lock →
load → `step` → write the row → execute effects. `Rejected` is a typed outcome,
not an HTTP error (§5).

## Files

- `crates/mitos-protocol/Cargo.toml` + `src/subscribe.rs` — the wire.
- `crates/mitos-companion/src/traits.rs` + `src/runtime.rs` — the worker's side.
- `crates/mitos-platform/src/moves.rs` — NEW. `MoveManager`.
- `crates/mitos-platform/src/fetch.rs` — NEW. `ArtifactFetcher` + verification.
- `crates/mitos-platform/src/storage.rs` — manifest CAS; row read/write/list.
- `crates/mitos-platform/src/companions.rs` — the subscribe handler calls it.
- `crates/mitos-platform/src/admin.rs` — one new read route.
- `crates/mitos-platform/src/events.rs` — one new `EventKind`.
- `crates/mitos-core/src/bundle.rs` — construct, wire, `boot()`, tick.
- `docs/HOWTO_CONSUMING_A_COMMUNITY_MODULE.md` — "Announcing your build".

## The work

### 1. The wire — `mitos-protocol`

`mitos-protocol` gains a path dependency on `mitos-lifecycle` (pure, `serde` +
`thiserror` only, so it stays wasm-safe).

```rust
pub struct AnnouncedModule {
    pub module: ModuleId,
    pub wasm: Sha256Hex,
    pub manifest: Sha256Hex,
    pub revision: Revision,
    /// The module's `config.cbor`, INLINE. `None` = the module has no config,
    /// which is not the same as an empty one. At most 64 KiB.
    #[serde(default, with = "serde_bytes")]
    pub config: Option<Vec<u8>>,
}
```

- On `SubscribeRequest`:
  `#[serde(default, skip_serializing_if = "Vec::is_empty")] pub announced: Vec<AnnouncedModule>`.
- **The field goes on the REQUEST, not on `SubscribeTarget::Module`.** That
  variant is built by literal 17 times across five workers in
  `cnft.dev-workers`; changing it would break every one at its next rev bump.
  `SubscribeRequest` is never built by literal outside this repo.
- `skip_serializing_if` is load-bearing: a request that announces nothing must
  encode BYTE-IDENTICALLY to today's. There is a test for that.
- **There is no URL.** Not an omission — see `ArtifactSource`. Do not add one.
- The ids are the validated newtypes, so a malformed digest or module id is a
  DECODE error (the existing 400), not something to check by hand.
- On `SubscribeResponse`:
  `#[serde(default, skip_serializing_if = "Vec::is_empty")] pub announced: Vec<AnnounceOutcome>`
  where

  ```rust
  pub struct AnnounceOutcome { pub module: ModuleId, pub result: AnnounceResult }
  #[serde(rename_all = "kebab-case", tag = "result")]
  pub enum AnnounceResult {
      AlreadyCurrent,
      Accepted,                    // a move is pending
      Refused { reason: String },  // the Display of whatever refused it
  }
  ```

About twenty `SubscribeRequest { .. }` literals in this repo (ten of them in
`companions.rs`'s tests) gain
`announced: Vec::new()` — let the compiler enumerate them. One is in
`crates/mitos-platform/tests/common/mod.rs`: adding that ONE field to that ONE
literal is sanctioned, as in WO-04. Nothing else under `tests/` changes.

### 2. The worker's side — `mitos-companion`

- `MitosCompanion` gains
  `fn announced_modules(&self) -> Vec<AnnouncedModule> { Vec::new() }`.
  Defaulted, so no existing companion changes.
- `runtime.rs` puts it on the request it already builds.
- `AnnouncedModule::from_built(&BuiltModule, config: Option<Vec<u8>>)` so a
  worker goes from its embedded `built-module.json` to the wire in one call.
  It must REFUSE (typed error) when the sha of `config` does not equal
  `built.config` — a worker that embeds one build's digests and another's
  bytes should find out at startup, not as a refused announcement.

This crate compiles for wasm32 in the workers. CHECK IT THERE (acceptance 3);
a native check is blind to it.

### 3. Storage

- **A manifest CAS keyed by the MANIFEST's own digest:**
  `_cas/manifest/<manifest-sha>.toml`. WO-01 stored a manifest beside the wasm
  as `_cas/wasm/<wasm-sha>.manifest.toml`, keyed by the WASM's sha; that stays,
  and the facade keeps reading it. But a manifest's bytes change on every
  build (`build_id`) while the wasm's may not, so two announcements can
  legitimately pin two different manifests over ONE wasm — and those must not
  overwrite each other.
  `put_manifest(bytes) -> Sha256Hex`, `read_manifest_by_digest(&Sha256Hex)`.
  Write-tmp-then-rename; a second put of the same digest is a no-op.
- **Rows:** `<module>/rows/<client_id>.toml`, holding a `ClientRow`. A client
  id belongs to at most one namespace (WO-02), so it is a sufficient file name.
  `read_row`, `write_row` (write-tmp-then-rename), `list_rows(module)`,
  `remove_row`. `rows/` is a MODULE-level directory like `companions/` — a row
  outlives any one instance; that is its whole point.

### 4. `fetch.rs`

`ArtifactFetcher` is a TRAIT (`async fn fetch(&self, url: &str, max: usize)
-> Result<Vec<u8>, FetchError>`) with a `reqwest` implementation and a fake for
tests. No test in this order touches the network.

The real one: `redirect::Policy::none()`, a 30s total timeout, and the size cap
enforced WHILE STREAMING — stop reading at `max + 1` bytes and refuse. Checking
`Content-Length`, or the length after buffering, is not a cap. Caps: wasm
16 MiB, manifest 256 KiB.

Fetch BOTH urls from `ArtifactSource::for_client` — do not build a URL by hand
anywhere. Then `verify`, in this order, each failure a distinct typed
`Refusal` whose Display becomes the alert's `reason`:

1. sha256(wasm bytes) == announced `wasm`.
2. sha256(manifest bytes) == announced `manifest`. **Raw bytes as fetched.**
3. The manifest parses, and `manifest.module.id` == the announced module.
4. `manifest.module.sha256` == announced `wasm` (the manifest is ABOUT this wasm).
5. `manifest.lifecycle.revision` == the announced `revision`. The rollback
   check already ran against the announced number; an artifact that disagrees
   with it would have been checked against a lie.
6. `manifest.validate_against_host(..)` — ABI, WIT sha, trap policy, ledger.
7. wasmtime accepts the component (the existing `validate_with_wasmtime`).

Only then `put_artifact(&manifest, &wasm)` + `put_manifest(raw bytes)`.
(`put_artifact` re-checks the wasm sha and writes its OWN re-serialised copy of
the manifest beside the wasm; that copy's bytes are not the fetched bytes, which
is precisely why the digest-keyed one stores the raw file.) Nothing unverified
is ever written to the CAS. `validate_with_wasmtime` is private to `admin.rs`
and returns a `HandlerError` — move it somewhere both can call and give it its
own error type; do not make `fetch.rs` depend on an HTTP handler's error. In-flight fetches are de-duplicated per wasm sha: two clients
announcing one build cause ONE download.

### 5. `MoveManager`

Holds storage, the fetcher, `Arc<Authenticator>`, `Arc<LifecycleManager>`, the
event ring, and one `tokio::sync::Mutex` per `(module, client)`.

- `announce(client, module, AnnouncedModule) -> AnnounceResult`, under the row
  lock:
  0. `authenticator.registry().may_announce(&client)` — `Err` ⇒ `Refused`
     with its Display. Its `Ok` is the `NamespaceDef` whose `profile` step 4
     needs. This is FIRST because everything after it writes or fetches.
  1. `put_config` the inline bytes if any ⇒ the config sha. Over 64 KiB ⇒
     `Refused`.
  2. `ModuleRef::announced_by(..)` — `SourceRefused` ⇒ `Refused`.
  3. Load the row, or create it — **see "Adopting a row" below.**
  4. Build the `AnnounceContext`: `revision_policy` from the client's
     namespace profile; `target` = `Exists` iff `list_instances(module)`
     contains the target key; `live_instances` = that list's length;
     `max_instances` = a `const MAX_INSTANCES_PER_MODULE: usize = 4`.
  5. `step(Announce)`. `Err(Rejected)` ⇒ `Refused { reason }` and NOTHING is
     written. `Ok` with no pending move ⇒ `AlreadyCurrent`; otherwise
     `Accepted`. Persist, then execute.
- **An announcement never fails the subscribe.** A refused rollback, a fork
  limit, an unreachable artifact — the companion is still registered and still
  served by the instance it is on. The outcome goes in the response, on the
  event ring, and on the row.
- **Do not write your own "is this mode safe" check.** `may_announce` is the
  whole rule: only a client the operator NAMED in a `clients = [...]` list.
  That refuses legacy mode and open mode without mentioning either — in both,
  a client id is whatever the caller typed, and the host would be fetching
  from a hostname of the caller's choosing. Consequence, which is intended:
  on a host with no `MITOS_NAMESPACES_FILE`, every announcement is `Refused`.
- `boot()` — for every row: `recover()`, persist, execute. A `Fetching` row
  re-issues its fetch; everything else re-queues.
- `tick()` — a row whose client has no registration left for the module ⇒
  `Withdraw`; if that leaves the row with neither `current` nor `pending`,
  `remove_row`.

**Adopting a row.** No row on disk, and the client IS attached to the module's
current instance (`LifecycleManager::instance(module)` ⇒ `Instance.attached`,
keyed by `ClientKey` — `registry.client_key(&client)` builds one) ⇒
`ClientRow::adopt(client, module, Placement { instance: <current key>,
revision: <that instance's manifest lifecycle.revision> })`. Otherwise
`ClientRow::new`. Read `ClientRow::adopt`'s doc comment for why `None` would
be wrong — it is the reason this paragraph exists.

**The README's rule applies to you directly:** anything named `load_or_*` must
only ever be called by a holder of the row lock. The admin read route (§6) and
the tick's scan must READ rows, never adopt-and-write them.

### 6. The subscribe handler, and admin

- In `subscribe_handler`, AFTER the existing flow has succeeded (registration
  persisted, `reconcile` called): for each `announced` entry whose module is
  ALSO one of this request's `Module` targets, call `announce` and collect the
  outcome. An entry for a module the request does not target ⇒
  `Refused { reason }`. Everything before that point is unchanged, including
  `UnknownModule` for a module the host has never heard of — pulling a
  brand-new module needs instance creation, which is WO-06.
- `GET /_admin/modules/{id}/rows` — typed: per client, `current`
  (`dir_name` + revision), `pending` (target `dir_name`, revision, stage,
  phase), and the last refusal if any. It is a `GET` on the `authed`
  sub-router, so it is `Admin(Read)` with no auth work. **Add it to
  `READ_ROUTES`** — `the_route_lists_cover_the_whole_router` fails until you do.

## Out of scope — do not do these

- Granting a slot; `EnsureInstance`; staging; catch-up; the switch. WO-06.
- Pulling a module the host has no instance of.
- Any change to who attaches or detaches a client (`LifecycleManager` owns it).
- The Apply frame carrying an instance digest (D8's self-healing) — WO-06.
- GC of `_cas/` or of rows.
- Making ANY worker in `cnft.dev-workers` announce. That is the operator's.
- A URL anywhere on the wire, or an origin allowlist.

## Tests that encode the old behaviour

None. No assertion changes. The one sanctioned edit under `tests/` is the
single-field addition described in §1.

## Acceptance

1. `nix develop -c cargo test -p mitos-protocol -p mitos-companion -p mitos-platform -p mitos-core -p mitos-lifecycle`
2. clippy `--workspace --all-targets -D warnings` 0;
   `cargo check --workspace --all-targets` 0; `cargo fmt --check` clean.
3. **`nix develop -c env NIX_HARDENING_ENABLE="" cargo check -p mitos-protocol -p mitos-companion --target wasm32-unknown-unknown`** —
   exit 0. (The env var: nix's cc-wrapper injects a hardening flag wasm cannot
   take, and it surfaces as a broken `blst` build, not as a toolchain problem.)
4. Wire, in `mitos-protocol`:
   - `a_request_that_announces_nothing_encodes_as_it_always_did` — compare
     against bytes captured from the CURRENT encoder before your change.
   - `an_old_host_ignores_an_announcement` — decode an announcing request into
     a struct shaped like today's `SubscribeRequest`.
   - `an_old_worker_gets_no_outcomes` — decode today's response shape.
   - `a_malformed_digest_is_a_decode_error`.
5. Companion: `from_built_refuses_config_bytes_that_do_not_match_the_build`.
6. `fetch.rs`, with the fake fetcher — one test per refusal 1–7 above, each
   asserting NOTHING was written to `_cas/`; plus
   `the_size_cap_stops_the_read_not_the_buffer`,
   `two_announcements_of_one_build_fetch_once`, and
   `an_artifact_already_in_the_cas_touches_no_network`.
7. `moves.rs`, fake fetcher + the WO-03 `FakeHost`:
   - `an_announcement_never_fails_the_subscribe` — through the real router: a
     refused announcement, and the registration is still written.
   - `an_attached_client_is_adopted_onto_its_instance_not_started_at_none` —
     assert the persisted row's `current`.
   - `the_91st_companion_of_a_client_is_a_no_op` — 91 subscribes, ONE fetch.
   - `a_rollback_is_refused_in_prod_and_the_row_is_untouched`.
   - `a_fork_past_the_limit_is_refused`.
   - `a_newer_announcement_abandons_the_pending_one` — the first fetch's late
     answer is `Rejected::Stale` and must be dropped quietly, not alerted.
   - `a_refused_artifact_alerts_and_leaves_the_client_where_it_was`.
   - `a_move_parks_at_queued_and_no_slot_is_ever_granted`.
   - `boot_refetches_a_row_that_was_fetching`.
   - `a_host_without_a_namespaces_file_refuses_every_announcement` — and
     assert the fake fetcher was never called and `_cas/config/` is empty.
   - `a_dev_token_cannot_announce_for_a_prod_client` — WO-02 already returns
     403 for the whole subscribe; assert no row and no fetch happened.
   - `reading_rows_never_writes_them`.
   - `a_client_that_unsubscribed_loses_its_pending_move_and_then_its_row`.
8. Goldens 69/69.

## Deploy note (for the operator — NOT part of this order)

Deploying this changes nothing until a worker announces. To try it on ONE dev
worker: serve the two files from `BuiltModule::served_paths()` as static assets
under the worker's own hostname, return the `AnnouncedModule` from
`announced_modules()`, deploy, and watch `GET /_admin/modules/{id}/rows` — the
row should reach `queued` and stay there. Because module builds are not
reproducible across machines, the digest a worker announces will differ from
what `deploy.sh` built on the box even for identical source, so EVERY first
announcement produces a pending move. That is expected; WO-06 is what acts on
it.
