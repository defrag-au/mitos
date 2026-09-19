# WO-04 — The manifest's `[lifecycle]` section, and what a build emits

**Design refs:** v3 D5 (state schema), D6 (upgrade actions), D8 (pull by digest).
**Lifecycle crate:** `mitos_lifecycle::artifact` — written FOR this order. Read
its module docs and tests: `LifecycleSection`, `BuiltModule`, `LIFECYCLE_TABLE`.
Also `mitos_lifecycle::upgrade` for what `[[lifecycle.upgrade]]` means.
**Depends on:** nothing — independent of WO-03. **Blocks:** WO-05.

## Implemented 2026-09-19 — open items for review

Acceptance passes: `mitos-platform` 211, `mitos-core` 28, `mitos-lifecycle` 79,
`mitos-build` 9 (its first tests), the four integration files UNMODIFIED except
the one sanctioned field, clippy `--workspace --all-targets -D warnings` 0,
`cargo check --workspace --all-targets` 0, `cargo fmt --check` 0 diffs,
goldens 69/69.

`lifecycle_is_stripped_from_the_config` passes, and acceptance 4 confirms it on
the real modules: all 19 community modules rebuilt with the new builder produce
**byte-identical `config.cbor`**. That is the property that matters — if it
broke, adding a revision would silently re-key a module's instance directory.

### ⚠️ One acceptance check I could not run as written

Acceptance 4 says "build every community module BEFORE and after your change".
I only built AFTER: the baseline would have needed the pre-change `mitos-build`,
and getting one means reverting or a second worktree — both git writes, which
the rules forbid. So my "before" was the artifacts already on disk, built at
some earlier unknown time.

Against that baseline every `config.cbor` matched and **every wasm sha
changed**. The evidence that this is stale build output rather than my doing:

- `community-modules/**/target/` is gitignored, so those artifacts are
  untracked output of whenever the modules were last built on this machine.
- The guest crate's dependencies are `wit-bindgen`, `serde`, `ciborium`, `hex`
  and `mitos-protocol`; `mitos-module-kit` has none at all. I touched none of
  them — `mitos-protocol`'s last commit is `c4d1cac`, 2026-07-08.
- The build IS deterministic here: building `burn-address` twice with the same
  binary gives the same sha, so this is not per-run nondeterminism.
- Goldens are 69/69 against the rebuilt modules, so behaviour is unchanged.

I believe the wasm deltas are pre-existing staleness, but I did not PROVE it.
Flagging rather than asserting — the WO-03 review's lesson was exactly this.

**To settle it**, from a checkout at `4136b7b` (before this work):

```sh
nix develop -c cargo build --release --bin mitos-build
nix develop -c ./target/release/mitos-build --module community-modules/burn-address/burn_address.rs
shasum -a 256 community-modules/burn-address/target/mitos/burn-address/burn-address.wasm
```

then the same three lines with this work applied. Equal ⇒ the on-disk
artifacts were stale and nothing here moved the wasm. Unequal ⇒ something in
this change reaches the guest tree, which would be a real defect and I have
not found the path by which it could.

### Deviations

**Only 1 is a judgement call** — it goes against the order's explicit text and
is the one to scrutinise. 2, 3 and 4 are forced: 2 by an existing signature, 3
by the order's own sanctioned exception, 4 by TOML's grammar.

1. **`ModuleSummary.revision` / `.state_schema` are `Revision` and
   `StateSchema`, not `u32`.** The order says `u32`. Both newtypes serialise
   transparently as bare numbers, so the JSON is identical, and the typed form
   matches how WO-02 put `Tier` and `OperatorTokenAsClient` on `/_admin/status`.
   Trivially revertible if you want the literal `u32`.
2. **`read_lifecycle` is split in two.** The order specifies
   `read_lifecycle(toml_str)`; `build_manifest` has a `config_path`, so there
   is a thin `read_lifecycle_section(Option<&Path>)` wrapper around the pure
   `read_lifecycle(&str)` that the tests exercise.
3. **The sanctioned `tests/common/mod.rs` edit**, as the order allows: one
   `lifecycle: Default::default()` line in the `Manifest` literal, with a
   comment. No assertion, fixture or helper signature changed.
4. `lifecycle` is the LAST field of `Manifest`. It ends in an array of tables
   (`[[lifecycle.upgrade]]`), which in TOML swallows any top-level key written
   after it — `interest` would have ended up inside the last upgrade entry.

### Found but not fixed

- `tools/mitos-build/src/main.rs` — `read_interest_section` still pokes at
  `toml::Value` while `read_lifecycle` uses typed serde. The order only asked
  for the new one to be typed, and converting `[interest]` would change what a
  malformed section does (silently empty today), so it is left alone.
- `built-module.json` is emitted for every build including `--dry-run`'s
  sibling paths — harmless, but nothing prunes it, so a stale one survives a
  failed build. It is regenerated on every successful run.

### Questions

None blocking. The wasm-sha item above is the only thing I would want a second
pair of eyes on.

## Why

WO-05 will let a worker announce, on subscribe, the artifact it was BUILT
against. Two things have to exist first:

1. A module must be able to SAY its revision, its state schema and its upgrade
   actions — and the host must read and validate them.
2. A build must EMIT something a worker can embed: the digests of what it built.

Nothing in this order changes what any module does at runtime. It is plumbing,
deployable on its own, and a module that declares nothing is exactly what it
was yesterday.

## The trap this order is shaped around — read this before anything else

`mitos-build` ships the module's ENTIRE `.toml`, CBOR-encoded, to the module as
`config.cbor` (`tools/mitos-build/src/main.rs`, ~line 272 — it strips `[deps]`
and nothing else). And since WO-01, **the config's sha is part of instance
identity** (`InstanceKey { module, wasm, config }`).

So build metadata written into the module `.toml` the obvious way would:

- reach the module's own `Config` deserialiser as unknown keys, and
- make bumping `revision` CHANGE THE CONFIG SHA — i.e. fork a new instance
  whose code and config are byte-identical to the old one.

Therefore the new keys live under ONE table, `[lifecycle]`
(`artifact::LIFECYCLE_TABLE`), and `mitos-build` strips it from `config.cbor`
exactly as it strips `[deps]`. One table means one `remove`, and a module stays
free to have a config key of its own called `revision`.

**The acceptance test that matters most:** a module `.toml` WITH a
`[lifecycle]` table and the same `.toml` WITHOUT it produce BYTE-IDENTICAL
`config.cbor`. If that ever fails, adding a revision to a module silently
re-keys its instance directory on the next deploy.

## The authoring shape

```toml
abi_version = 2

[lifecycle]
revision = 5
state_schema = 2

[[lifecycle.upgrade]]
since = 3
actions = ["recapture"]

[interest]
addresses = []
```

Every field defaults. The default — revision 0, schema 0, no upgrades — IS what
every artifact built before this order is, so all ~16 existing module `.toml`
files and every manifest already in a CAS remain valid, unedited. (The v3 doc's
D6 shows `[module] revision` + top-level `[[upgrade]]`; `[lifecycle]` supersedes
it, for the reason above. Update the doc's example.)

## Files

- `crates/mitos-platform/src/manifest.rs` — `Manifest` gains the section;
  validation; one new `ManifestError` variant.
- `crates/mitos-platform/src/admin.rs` — `ModuleSummary` reports it.
- `tools/mitos-build/src/main.rs` — read, validate, copy, strip, emit. Plus the
  first tests this binary has ever had.
- `tools/mitos-build/Cargo.toml` — `mitos-lifecycle` path dep.
- `docs/HOWTO_FIRST_MODULE.md` — a short "Declaring a revision" section.
- `docs/strategy/MITOS_PLATFORM_V3.md` — D6's example only.

## The work

### 1. `Manifest`

```rust
#[serde(default, skip_serializing_if = "LifecycleSection::is_default")]
pub lifecycle: mitos_lifecycle::artifact::LifecycleSection,
```

- `skip_serializing_if` is load-bearing: a manifest for a module that declares
  nothing must serialise EXACTLY as it does today (apart from `build_id`).
- `validate_against_host` calls `lifecycle.validate()`; failure is a new
  `ManifestError::Lifecycle(#[from] LedgerError)` ⇒ the module is REFUSED, on
  upload and on auto-load alike. An unknown action never reaches `validate` —
  it is already a TOML parse error, which is the intent ("unverifiable is not
  fine"). Do not add a lenient fallback.
- There is no `deny_unknown_fields` on `Manifest` (checked 2026-09-19), so an
  OLDER host reading a manifest that has `[lifecycle]` ignores it. That is what
  makes a host rollback safe. Do not add `deny_unknown_fields`.
- Every `mitos_platform::manifest::Manifest { .. }` literal gains
  `lifecycle: Default::default()`. There are about six (`manifest.rs`,
  `storage.rs`, `migrate_v2.rs`, `companions.rs`,
  `mitos-core/src/community_modules.rs`, and the one below) — let the COMPILER
  enumerate them rather than trusting this list. `policy-archive` has an
  unrelated type of the same name; leave it alone.
- **One of them is in `crates/mitos-platform/tests/common/mod.rs`**, the
  integration tests' shared helper. Every order says those tests pass
  "unmodified", and this is the narrow exception: adding that ONE field to that
  ONE literal is sanctioned, because the struct gained a field and the file
  will not compile otherwise. Nothing else in `tests/` may change — no
  assertion, no fixture, no helper signature. Say so under "Deviations".

### 2. `mitos-build`

Extract the logic out of `main()` into functions so it can be tested — the
binary has no tests today:

- `read_lifecycle(toml_str) -> anyhow::Result<LifecycleSection>` — typed serde,
  NOT `toml::Value` poking as `read_interest_section` does. Call `validate()`;
  a bad ledger FAILS THE BUILD with the `LedgerError` text. A refused artifact
  should never leave the builder.
- `encode_config(toml_str) -> anyhow::Result<Vec<u8>>` — the existing encoding,
  with BOTH `deps` and `artifact::LIFECYCLE_TABLE` removed. Use the constant.
- Write the section into the manifest.
- Emit **`built-module.json`** beside `manifest.toml`: a
  `mitos_lifecycle::artifact::BuiltModule` via `serde_json` (typed struct — no
  `json!`). Its fields:
  - `wasm` = the manifest's `module.sha256`.
  - `manifest` = SHA-256 of **the `manifest.toml` file's bytes exactly as
    written to disk**. Compute it from the bytes you wrote, after writing them.
  - `config` = SHA-256 of the `config.cbor` bytes, or `null` when no
    `config.cbor` is produced. Use `mitos_platform::manifest::sha256_hex` — the
    same function `ModuleStorage::put_config` uses, so the builder's digest and
    the host's are the same by construction.
  - `revision` = `lifecycle.revision`.

**Why the manifest digest is over raw bytes, not a canonical form.** A
canonical form (say, the manifest minus `[build]`) would make the digest depend
on the builder's and the host's TOML serialisers agreeing, across versions and
across repos. Raw bytes have no such dependency: the host hashes what it
fetched. The cost is that `build_id` makes every build's manifest digest
different — and that is FINE: the manifest is not part of instance identity, so
a no-change rebuild announces a new manifest digest for the SAME `InstanceKey`,
which `client_move` already treats as "already there". Do not "fix" this by
stripping `build_id`.

### 3. Admin

`ModuleSummary` gains `revision: u32` and `state_schema: u32`, read from the
manifest. Typed; always present (0 for a pre-v3 artifact, which is
informative). Both appear wherever `ModuleSummary` already does.

### 4. Docs

`HOWTO_FIRST_MODULE.md`: when to bump `revision` (any change a consumer could
observe), when to bump `state_schema` (the kv layout changed — today that means
REBUILD, since no `migrate` export exists yet), and the four actions with one
line each, taken from the doc comments on `UpgradeAction`. Say plainly that
`revision` is NOT identity and that bumping it alone moves nobody.

## Out of scope — do not do these

- **The WIT.** No `migrate` export. Touching `wit-v2/world.wit` changes
  `host_wit_sha` and refuses every module until it is rebuilt; that belongs to
  the semver ABI work. The host therefore always has `MigrateExport::Absent`
  and a `state_schema` bump means rebuild. This order only RECORDS the schema.
- USING any of it. Nothing reads `revision` to make a decision yet — WO-05.
- Adding `[lifecycle]` to any existing module `.toml`.
- `ModuleRef` on the wire, fetching, a manifest CAS keyed by manifest digest.
- Making the wasm build reproducible (see the deploy note).
- `mitos-modules.lock` for cross-repo consumers.

## Tests that encode the old behaviour

None — no ASSERTION changes anywhere. The four integration test files and
`tests/community_module_interest.rs` are not touched. The single sanctioned
edit under `tests/` is the one-field addition to the `Manifest` literal in
`tests/common/mod.rs` described in step 1, which changes no behaviour.

## Acceptance

1. `nix develop -c cargo test -p mitos-platform -p mitos-core -p mitos-lifecycle -p mitos-build`
2. New tests in `mitos-build`:
   - `lifecycle_is_stripped_from_the_config` — **the one that matters.** The
     same `.toml` with and without `[lifecycle]` (and with `[[lifecycle.upgrade]]`
     entries) ⇒ `encode_config` returns byte-identical output.
   - `deps_are_still_stripped` — the existing behaviour, now pinned.
   - `a_module_toml_with_a_key_called_revision_keeps_it` — a TOP-LEVEL
     `revision = 9` is the module's own config and must survive into the CBOR.
   - `a_bad_ledger_fails_the_build` — `since` ahead of `revision`; assert the
     error names it.
   - `an_unknown_action_fails_the_build`
   - `built_module_json_is_pinned` — assert the exact JSON text for a fixed
     input: field names, order, `null` config. It is read by workers in another
     repo; a round-trip test would pass however the fields were renamed.
   - `the_manifest_digest_is_over_the_bytes_written` — write a manifest, read
     the file back, hash it, compare with `built-module.json`.
   - `the_config_digest_matches_the_hosts` — `put_config` the same bytes into a
     tempdir `ModuleStorage` and assert the sha it returns equals `config`.
3. New tests in `manifest.rs`:
   - `a_manifest_without_lifecycle_parses_as_pre_v3`
   - `a_default_lifecycle_is_not_serialised` — `to_toml()` of a manifest with
     the default section does not contain the string `lifecycle`.
   - `a_bad_ledger_refuses_the_module` — through `validate_against_host`.
   - `an_older_host_would_ignore_the_section` — parse a manifest that HAS
     `[lifecycle]` into a struct shaped like the pre-WO-04 `Manifest` (a local
     test struct); it must parse. This is the rollback guarantee, as a test.
4. Build every community module before and after your change and confirm each
   `config.cbor` and each wasm sha is UNCHANGED. None of their `.toml` files
   has a `[lifecycle]`, so anything that moved was moved by you.
5. Goldens **69/69** (README: rebuild `mitos-run` AND the modules first).
6. clippy `--workspace --all-targets -D warnings` 0;
   `cargo check --workspace --all-targets` 0; `cargo fmt --check` clean.

## Deploy note (for the operator — NOT part of this order)

Two facts about digests, established while writing this order, that shape how
WO-05 gets rolled out:

- **The wasm build is not reproducible across machines.** `mitos-build` compiles
  with `debug = "line-tables-only"` and no `--remap-path-prefix`, so absolute
  source paths end up in the artifact: the same source built in
  `/opt/mitos/src` and in a CI runner's checkout yields two DIFFERENT wasm shas.
  Under instance-per-digest that is two instances, for ever, of identical code.
  So a module consumed by more than one worker (`jpg-store-*` is consumed by
  both `jpgsm` and `ownership`) must be BUILT ONCE and its `built-module.json`
  shared — never rebuilt per worker. Making the build reproducible would remove
  the constraint and is worth its own piece of work.
- **The manifest digest changes on every build** (`build_id`). Harmless, by
  design — see above.
