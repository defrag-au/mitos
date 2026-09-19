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
