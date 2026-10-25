<!-- BEGIN agent-playbook (project: mitos, target: zed) — generated, do not edit -->

# Agent rules

Generated from `.playbook/project.conf` + `models/zed`.
Each rule's source file is named in the comment above its heading.
To change a rule, change the rule — not this block.

## Non-negotiable

The lead of each rule below, repeated so it is read first. Full text follows in place.

- **Read code and history with the agent tools, not with the shell** — Reading code with `rg`, `grep`, `sed`, `cat`, `head` or `wc` → use `at-peek`. Reading installed crate source or build-script output with `grep`, `sed` or `find` under `~/.cargo` or `target/…/build` → use `at-crate`. Reading history or working-tree state with `git status`, `git diff` or `git log` → use `at-recall`. All three are read-only, bounded by construction, and say what they did not show. `git show` and `git blame` have no `at-recall` verb yet — say so rather than reaching for them silently.

## Do not report success you have not observed
<!-- rule: rules/core/verify-before-claiming -->

- Claim only what you ran and saw this session. "Tests pass" = the command ran and its
  output showed them passing · "it builds" = a build ran (a successful edit is not one) ·
  "it renders" = it was rendered and looked at (see `org/defrag/look-at-what-you-built`) ·
  "fixed" = the symptom was reproduced, then observed gone.
- Validation not run → say so and say why (no toolchain reachable, needs a device, needs
  credentials). An unearned "done" moves the discovery of the failure to me.
- Also not evidence: a partial result reported as complete (three of four done → say which
  three) · a step silently skipped (name it, do not drop it) · a green exit code from a
  command that did nothing — a `str.replace` matching no anchor, a test filter matching no
  tests, check the output says what you think it says · a narrower run than claimed (a
  single-crate build is not a workspace build).


## Tests are the specification — never edit an assertion to make it pass
<!-- rule: rules/core/test-preservation -->

**Never modify a test assertion without explicit permission.** When behaviour is reported as
wrong, the fix goes in the code; if the fix makes the test fail, the fix is wrong.

1. Fix the code to produce correct behaviour.
2. Leave the existing assertions alone.
3. If tests still fail, the fix is still wrong.
4. Tests are the specification, not an obstacle.

Never: change an assertion to match current behaviour · weaken a test (loosen a bound, widen
a tolerance, add an early return) · remove specificity (`assert_eq!(sales, vec![expected_sale])`
→ `assert!(!sales.iter().any(is_false_positive))` throws away the thing being tested) ·
delete, `#[ignore]` or comment out a failing test · optimise for a green suite over a correct
system · assert what the code currently does and call it the specification.

Allowed without asking: **adding** a test for behaviour that had none · fixing a test that
cannot compile because a signature legitimately changed, provided its assertion survives ·
renaming a test to describe what it checks.

A test that is genuinely wrong → say so, explain why the assertion is incorrect, and get
sign-off before changing it.


## Working first — fix problems, don't hide them
<!-- rule: rules/core/working-first -->

- Deliver working functionality before optimising architecture. Fix the cause; never comment
  out, disable, or route around a problem to get a build green.
- Facing a compile error, a lifetime fight or a design question → ask **"what is the simplest
  thing that makes this actually work?"**, not "what is the correct architecture?".
- Stop and reconsider if you are about to:
  - add a `// TODO:` to disable functionality that was supposed to work
  - comment out code to silence a compilation error
  - fight type/lifetime/async issues before the plain logic is proven
  - rewrite a signature to make an error go away rather than understanding it
  - say "we'll implement that later" about a core feature rather than an edge
  - spend longer on the shape of the code than on whether the feature works
- Deviate only when continuing would break something that works, introduce a security hole,
  or risk data corruption — and then fix it properly, do not disable it.

**Make it work, make it right, make it fast — in that order.**


## Never invent data to satisfy an interface
<!-- rule: rules/core/never-make-things-up -->

- Never fabricate a value to make a function, fixture or screen look functional. Before
  writing a placeholder, find where the data actually comes from — a config file, a table, an
  API response, a sibling implementation, an env var. A real search turning up nothing → ask,
  do not invent.
- Same rule for: a plausible address, hash or timestamp invented for a fixture (it hides real
  parsing bugs, because invented data is already in the format the code expects) ·
  `unwrap_or(0)`, `Default::default()` or a hard-coded fallback on a value that should have
  been sourced · a struct written from memory of what an API "should" return (cite the actual
  response or the docs) · a test asserting what the code currently does · a confident
  explanation of a failure you have not verified.
- Choosing a sensible default and **stating it** is fine. Inventing a value and presenting it
  as data is not. Test: if someone asks "where does this number come from?", is there an
  answer? "Nowhere yet" → say so and ask.


## Build what was asked, then stop
<!-- rule: rules/core/dont-rush-new-features -->

- Asked for X → build X and stop. Do not add Y and Z because they seem wanted: no config
  option "for flexibility", no trait "for testability", no abstraction "for later" — unless
  the task named it.
- Plan each logical step of implementation together, one at a time.
- Three exceptions only: the task cannot be completed without it (a caller that must be
  updated to keep the workspace compiling, a type the requested feature needs to typecheck) ·
  not doing it would break something that currently works · I asked you to use your judgement.
- Everything else → mention it and let me decide. A one-line "this would also allow X if you
  want it next" is welcome. Building X uninvited is not.
- Applies to cleanup too: renames, import reordering, tidying a neighbouring function, bumping
  a dependency "while I'm here". If a nearby thing is genuinely broken, say so in the final
  message rather than fixing it silently.


## Ask before changing dependencies or architecture
<!-- rule: rules/core/conservative-package-changes -->

Stop and ask before a change that alters the *approach* rather than the *implementation*.
Present the problem, two or three specific options with their trade-offs, and wait.

Ask first for:

- editing `Cargo.toml`, `package.json`, `flake.nix` or any other manifest
- adding, removing or bumping a dependency
- swapping one library for another (`smlang` → `rust-fsm`, `reqwest` → `ureq`)
- replacing a whole module or implementation rather than fixing it
- choosing a pattern — state machine style, database access layer, error strategy, where a
  type lives
- anything that changes how the project is built, deployed or configured

No need to ask for: a bug fix inside the existing approach · a direct instruction ("change X
to Y") · formatting, lint fixes, or renames local to one function.

Options must be **specific** — name the crates, say what each costs. "We could use a library
or write it ourselves" is not a set of options.


## Git state is mine — do not commit, push, merge or stage
<!-- rule: rules/core/git-is-the-users-domain -->

- Do not run `git add`, `git reset`, `git restore --staged`, `git stash`, `git commit`, `push`,
  `merge`, `rebase`, `tag` or anything else that changes the index, history or a remote unless I
  specifically ask. The same prohibition covers discarding working-tree changes — `git restore
  --source`, `git checkout -- <paths>`, `git apply --reverse`, `git clean` — because the working
  tree is mine to review too. Leave the index exactly as you found it; do not stage or unstage
  anything.
- For reads, `at-recall` and `at-peek` are the default, not the alternative. An agent shell may
  not inherit the devshell; if `which at-recall` is empty, reach it as `direnv exec . at-recall …`
  (and the same for `at-peek`). A bare `git status`, `git diff` or `git log` is a last resort:
  only after that invocation fails, and say which one you tried. `git show` and `git blame` have
  no `at-recall` verb yet — say so rather than widening the habit silently. Use `--no-pager` for
  any git read that remains.
- Branching for work is fine when it helps. Creating a branch rewrites nothing I have to
  unpick.
- If you change the index by accident, undo exactly that and say so.
- Also: no `[skip ci]`, `--no-verify` or bypassing hooks without asking · no amend or
  force-push · do not commit artefacts `.gitignore` excludes, even when they are up to date ·
  do not write a commit message and leave it staged in a way that suggests it was committed.


## Edit files with the editor tools, never with a shell script
<!-- rule: rules/core/edit-via-editor-tools -->

- Edit files with the read/edit/write tools. Never `python`, `sed`, `awk`, `perl`, `truncate`
  or a heredoc — not for a big change, and not for "just one small change". This applies to
  every file: source, config, docs, rules, memory.
- A change that touches ten places is ten edit calls, not one script.
- The editor tools verify the file was read first, fail loudly on a stale or ambiguous
  match, and show a reviewable diff. A script's `str.replace` **silently does nothing** when
  the anchor text has moved.
- Still fine: generating a file's *content* with a script when the content is genuinely
  computed (a catalogue from source headers, a table derived from data) — writing it to disk
  still goes through the write tool. And reads through the shell (`cat`, `grep`, `find`) to
  gather information, before editing with the editor tools.


## Kill only the processes you started — never by name pattern
<!-- rule: rules/core/kill-only-what-you-started -->

- Never clean up with `pkill -f "<app>"`, `killall <app>`, or any pattern that matches a
  binary by name — it also matches the copy the user is running interactively. Kill only the
  instances you started, by an identifier unique to them: a PID you captured, a flag you
  passed, or a port only your process bound.
- A long-running helper you spawn — a headless browser, a dev server, a watcher — gets a
  unique handle on its own command line: `--user-data-dir=.tmp/<task>-profile`, a `--port` you
  chose, an output path under `.tmp/`. Clean up with *that string*, not the app's name.
- The handle must be one only your process carries. `pkill -f "Brave Browser"` is the failure;
  `pkill -f "brave-profile-<task>"` is the fix, because the user's own Brave has no such flag.
- Cannot give it a unique handle → kill nothing. Say what you left running instead.
- Put scratch profiles and artefacts under `.tmp/` in the repo, so the handle and the cleanup
  target are unambiguous.


## Typed structs only — no serde_json::Value, no json! macro
<!-- rule: rules/core/typed-json-only -->

- Never `serde_json::Value` or `json!()` for data structures, API responses, configuration,
  queue messages, SSE payloads, or anything persisted. Define concrete types with
  `#[derive(Serialize, Deserialize)]`.
- Banned: `Value` · `json!()` · `Map<String, Value>` · `Vec<Value>` · any composition of
  `Value` · `#[serde(untagged)]` used to avoid deciding on a shape.
- Required: structs and enums with serde derives · `#[serde(tag = "type")]` for tagged unions
  rather than a stringly-typed discriminant field · `Option<T>` for nullable fields and
  newtypes for ids and timestamps · `#[serde(flatten)]` where a shape genuinely composes.

```rust
// no
let event = json!({ "type": "progress", "message": msg, "pct": pct });

// yes
#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum CatchupEvent {
    Progress { message: String, pct: u8 },
    Complete { assets: u32 },
}
```

Exceptions, allowed only with the justification written in the code: consuming a genuinely
dynamic external API whose schema cannot be known · temporary debugging code that will be
removed · a low-level JSON utility whose whole purpose is handling arbitrary JSON.


## Plan in reasoning, not in the response
<!-- rule: rules/core/planning-stays-in-thinking -->

- I see the answer and the diff, not the plan for getting there. Do not narrate what you are
  about to look up — look it up.
- Not: "I'll start by reading X, then check Y" as a preamble to reading X and checking Y ·
  restating the request before acting on it · announcing each step ("Now let me look at the
  config…") · a plan-shaped response where a tool call was the answer.
- Still fine: a genuine plan when the task is large enough that I should agree to the approach
  first (a decision I need to make, not narration) · one sentence before a group of related
  tool calls · reasoning in the final message when it affects whether I trust the result.
- A nudge to "first privately list what you need next" means list it *in reasoning*, then
  batch the independent tool calls into one response. Never begin a response with
  "Privately," or any variant.


## Never commit a secret
<!-- rule: rules/core/no-committed-secrets -->

- No credential, token, private key, connection string or API key in the repository — not in
  source, a test fixture, a config file, a commit message, or a comment explaining what the
  value used to be.
- A change that needs a secret gets it from the environment or a secrets manager. The
  repository holds the variable *name* and an `.env.example` with an obvious placeholder:

```
# .env.example — committed
KOIOS_API_KEY=your-key-here

# .env — gitignored
KOIOS_API_KEY=<real value>
```

- Need one to test something → say so and stop. Do not invent a placeholder that looks real,
  and do not reach for a real key already in the environment; a test that only passes with a
  live credential is not a test, it is a scheduled failure.
- Find one already committed → **report it**, do not quietly delete the line. It is in the
  history, so deleting it from the working tree does not revoke it. The secret has to be
  rotated, and that is my decision rather than a cleanup you perform silently.
- A secret in a code comment or a `// TODO: replace before merge` is the same leak with an
  expiry date that nobody enforces.


## Prefer ownership over shared mutable state
<!-- rule: rules/core/ownership-over-shared-state -->

Prefer an ownership design to shared mutable state. Do not reach for `Rc<RefCell<T>>`,
`Arc<Mutex<T>>` or a `static` lock to get around a borrow, lifetime or callback problem —
restructure the value instead, and say what the restructure is.

Three shapes that usually replace it:

- **Split the value.** The thing being mutated and the things reading it become separate
  objects, wired together by the caller that owns both.
- **Move the state to the caller.** The count, the cache, the cursor is owned by whoever
  drives the loop and passed in, so there is one writer by construction.
- **Replace the check, not the state.** If the shared state existed only to catch a misuse, a
  type parameter, a consumed token, or a `Result` from the one place that can see the misuse
  costs less and cannot be reached around.

If shared mutable state is genuinely the answer, name what breaks without it and ask before
building on it. Do not offer it as the low-effort option in a list of designs either.

Narrow existing uses are not this rule: a counter or a memo already behind `&self` inside one
type is an implementation detail, not a design, and it does not need a decision from me.


## Rust tooling lives behind the Nix devshell
<!-- rule: rules/rust/devshell-first -->

`cargo`, `rustc`, `clippy`, `rustfmt`, `trunk`, `wrangler`, `node` and the wasm targets are
**not on `PATH`** — they come only from the devshell defined in `flake.nix`. Wrap every
command:

```sh
nix develop -c cargo build --workspace
nix develop -c cargo test -p <crate>
nix develop -c cargo clippy --workspace --all-targets --all-features -- -D warnings
nix develop -c cargo fmt
```

`nix develop --command <cmd>` is equivalent.

**In a sandboxed shell, `nix develop` cannot reach the daemon socket**
(`/nix/var/nix/daemon-socket/socket`) — `cannot connect to socket … Operation not
permitted`. Use direnv instead, which reads the already-realised devshell out of `.direnv/`
and needs no daemon:

```sh
direnv exec . cargo build -p <crate>
```

If `.direnv/` is cold, run `direnv allow .` once from an unsandboxed shell.

Never: install a separate toolchain, source `~/.bash_profile` to find cargo, or reach for the
network. Add `--offline` if cargo tries to fetch a crate the devshell cache already holds.


## Build often — the compiler is the best information you have
<!-- rule: rules/rust/build-often -->

- A change that could plausibly break a type → compile it. Do not read your way to confidence
  through three files when one `cargo check` answers the question in seconds.
- `nix develop -c cargo check` while iterating · `cargo build` when you need artefacts · `-p
  <crate>` to scope a large workspace.
- `cargo clippy` for the lints the compiler does not carry — see
  `rust-tooling-handles-grunt-work`.
- Build fails → read the whole error before editing. Rust errors cascade; fixing a consequence
  first produces a second round of errors that look new.
- Build already established as expensive and the change cannot reach it (editing a markdown
  file does not need a compile) → do not build.


## Inline format arguments everywhere
<!-- rule: rules/rust/inline-format-args -->

Inline format arguments everywhere — clippy warns on the alternative and the lint command
treats warnings as errors.

```rust
// no
format!("Hello {}", name)
println!("wrote {} rows to {}", count, path)

// yes
format!("Hello {name}")
println!("wrote {count} rows to {path}")
```

- Every formatting macro: `format!`, `println!`, `eprintln!`, `write!`, `writeln!`, `panic!`,
  `assert!`, `assert_eq!`, `debug_assert!`, `tracing` macros.
- New code → inline args from the first draft.
- Editing existing code → convert the lines you are already touching · do not sweep the file.
  Broadly non-compliant file → `clippy --fix` as its own change — see
  `rust-tooling-handles-grunt-work`.


## Let fmt and clippy do the mechanical work
<!-- rule: rules/rust/tooling-handles-grunt-work -->

Formatting and mechanical lint fixes are tool work. Do not hand-align code or type out a fix
the tool will apply itself.

```sh
nix develop -c cargo fmt
nix develop -c cargo clippy --fix --allow-dirty --all-targets --all-features -- -D warnings
```

- Run them, then read what changed and check the remaining diagnostics manually. `--fix` first,
  then review — the pass changes the lines you were about to read.
- A warning is a failure, not a note. This codebase does not accumulate "known warnings".
- `--fix` applies only mechanically safe suggestions — it will not choose between two valid
  shapes · know a `disallowed_methods` match is a false positive on a blessed call · fix
  anything needing a semantic decision.
- A rule clippy cannot enforce → add a source scan in `cargo test`, in the same spirit as
  `cargo fmt --check`. Add the test with the rule.


## Where tests live, and what they may touch
<!-- rule: rules/rust/test-layout -->

- Unit tests → a `mod tests` block in the same file. They can reach private items, which is
  usually the point.
- Integration tests → `tests/`, one file per scenario, exercising the public API only.
- Runnable examples → `examples/` when they clarify the API. `cargo run -p <crate> --example
  <name>` is the test that a consumer can actually use the thing.
- WASM tests → `wasm-bindgen-test` and a WASM-capable runner. They do not run under plain
  `cargo test`, so a green suite is not evidence that a WASM test passed.
- Tests are deterministic. No network calls unless they are feature-gated or mocked.
- A fixture a second crate needs → capture it in the shared `data/` crate rather than copying
  it. Shared serialization fixtures live in `test_datum_serialization.rs`.


## Read code and history with the agent tools, not with the shell
<!-- rule: rules/org/defrag/agent-tools -->

- Reading code with `rg`, `grep`, `sed`, `cat`, `head` or `wc` → use `at-peek`. Reading installed
  crate source or build-script output with `grep`, `sed` or `find` under `~/.cargo` or
  `target/…/build` → use `at-crate`. Reading history or working-tree state with `git status`,
  `git diff` or `git log` → use `at-recall`. All three are read-only, bounded by construction, and
  say what they did not show. `git show` and `git blame` have no `at-recall` verb yet — say so
  rather than reaching for them silently.

| Instead of | Use |
| --- | --- |
| `rg`, `grep` | `at-peek search <pattern> [path…]` |
| `sed -n '40,60p' <file>`, `head`, `cat` | `at-peek slice <file>:40-60` |
| `wc -l`, `ls -l`, `stat` | `at-peek stat <path>…` |
| `grep -rn … ~/.cargo/registry/src`, `sed -n … <crate>/src/…`, `find ~/.cargo/registry` | `at-crate search <pattern> <crate>` · `at-crate slice <crate>/<path>:40-60` · `at-crate where <crate>` |
| `cargo info <crate>`, `ls ~/.cargo/registry/src/…` | `at-crate where <crate>` · `at-crate installed [<crate>]` |
| `head target/…/build/<crate>-*/out/…`, `grep … target/…/build/…/out/…` | `at-crate out <crate>` · `at-crate slice <crate>/<path> --build` · `at-crate search <pattern> <crate> --build` |
| `git status`, `git status --porcelain` | `at-recall state` |
| `git diff`, `git diff --stat` | `at-recall diff [<rev>] [<path>…]` — add `--patch` for the hunks |
| `git log`, `git log --oneline` | `at-recall log [<rev>] [<path>…]` |
| the archaeology before a PR description: branch, base, `merge-base`, `log --oneline base..HEAD`, `diff --stat` | `at-recall pr [--base <rev>]` — the branch, its commits and its diffstat in one read |
| anything not listed | `at-describe` — the catalogue, one screen |

- The exact lines · every mention · how many · which files → `at-peek slice <file>:40-60` ·
  `at-peek search <pat>` · `at-peek search <pat> --count` · `at-peek search <pat> --files-only`.
  For installed crate source the same three are `at-crate slice <crate>/<path>:40-60`,
  `at-crate search <pat> <crate>` and `--count`.
- `which at-peek` empty → `direnv exec . at-peek <verb>` (and the same for `at-recall`, `at-crate`
  and `at-describe`). An agent's spawned shell does not inherit the devshell environment; an
  interactive one does.
- `at-crate` resolves a crate through the nearest `Cargo.lock`, not through memory or "latest". A
  crate named twice in the lock, or a file present in more than one build-script `out`, is an error
  naming the flag that chooses — never a guess. `--version` and `--build-dir` are those flags.
- Output that was cut says so, and so does anything that would make it wrong. Repeat both when
  you report the finding: `# 50 of 143` is the difference between a fact and a guess.
- A `# next:` line is the next question, already spelled as a command. Run it as printed rather
  than composing your own — it is the read you just made, widened or deepened.
- Several questions in one turn → ask for the frame (`--summary`: the counts, bounds and exits,
  without the rows) and put several targets in one invocation (`diff <path> <path>`, `slice
  <path>:40-60 <path>:1-20`) rather than a loop. Never `| tail -3`: a tail is a bound you did not
  read, and the bound is the part that makes the answer reportable.
- The tools are for understanding, not for preparing an edit. Read the file with the editor tools
  before editing it.
- `at-peek` and `at-crate` run nothing at all; `at-recall` runs `git` and nothing else. That
  difference is why they are separate binaries, and why they are separate approvals if I have
  tiered them.
- A **recipe** is one verb that composes the reads its sibling verbs use, so a count cannot disagree
  with the listing beside it: `at-recall pr` answers "write me a PR description" with the base, the
  branch's commits and the diffstat by kind, and a caveat naming tracked files that have changed
  since HEAD — those are in no commit, and a description written from the branch alone leaves them
  out. Ask for the recipe rather than composing the sequence — `pr --with commits,diffstat,areas`
  selects sections, and its `# next:` lines name the primitives (`log`, `diff`) when you want the
  detail behind it. It prints the facts a description is written from and does not write the
  description: why a change exists is not in the repository.
- `at-recall` answers `state`, `log`, `diff` and the `pr` recipe. `blame`, `show` and `churn`, and the
  `review` and `release` recipes, are designed and not written — ask for the one you want rather than
  reaching for `git`, and name the question, because that is what turns it into a verb. `at-crate`
  answers `where`, `installed`, `files`, `slice`, `search` and `out`.


## Toggle local [patch] blocks as a unit
<!-- rule: rules/org/defrag/patch-blocks -->

Engaging a local `[patch]` override block in `Cargo.toml` — to consume a sibling working tree
instead of the pinned git rev → uncomment the **entire `[patch."…"]` table as a unit**.

- Never selectively un-comment individual entries.
- Re-commenting before commit → re-comment the whole block. Never leave a partial active block
  behind.

Selective edits cause two failures:

- A line ends up in both the active and the still-commented copy → duplicate-key error
- A transitively-required crate is left commented → unrelated workers break


## Keep logic pure — runtime bridges come in pairs
<!-- rule: rules/org/defrag/runtime-pairs -->

**wasm-bindgen code can never run under macroquad/miniquad** — the miniquad `gl.js` runtime has no
wasm-bindgen glue; browser interop goes through the miniquad plugin protocol (`sapp_jsutils`)
instead. Permanent, not a version issue.

- Runtime-specific bridges come in pairs: `wallet-core` for wasm-bindgen frontends,
  `wallet-miniquad` for macroquad games. egui-widgets and macroquad-widgets do not interchange.
- Keep logic in small pure crates — no wasm-bindgen, no I/O, no runtime deps — so the same crate
  can be consumed from a macroquad game, a wasm-bindgen frontend (Leptos) **and** a Cloudflare
  worker. Push runtime bindings to thin edge crates.
- Macroquad target → check a dependency's transitive tree for wasm-bindgen before adding it. A
  native `cargo tree` is blind to this; the target flag is required:

```sh
nix develop -c cargo tree --target wasm32-unknown-unknown -p <crate>
```

- WASM build → minimise the features enabled on a wasm32 target. Default features are chosen for a
  native build, so a wasm consumer inherits capabilities it cannot use; a feature that pulls in a
  system library fails at link time, not at the crate that introduced it. Check the transitive
  tree, not the direct dependency.


## Bulk uploads to R2 run on the arm box, not the workstation
<!-- rule: rules/org/defrag/bulk-uploads-run-on-the-arm-box -->

**Build and verify on the workstation; run the transfer on the arm box.** The workstation's
uplink sets the ceiling for a GB-scale write to R2, so a large upload from here is a validation
step and never the production one.

| step | where |
| --- | --- |
| build the artifact · dry-run the publish · check the bytes | workstation |
| the `rclone`/`publish` run that moves GB to R2 | arm box |

- `--run`-gated publishers (`mithril-snapshot publish`, `tx-index publish`) print their exact
  `rclone` invocation without `--run` · copy that line to the box rather than re-running it here.
- The box already holds the chunk store, so an artifact derived from chunks is **rebuilt** there,
  never copied to it — copying it spends the same uplink twice.
- Native build on the box, not `aarch64-unknown-linux-musl`: the musl build measured 2.2× slower
  on one thread and *slower with eight than with one*.


## Commit and PR conventions for this ecosystem
<!-- rule: rules/org/defrag/commit-conventions -->

Applies only when asked to prepare a commit or draft a PR — see
`core/git-is-the-users-domain` for the standing rule
that you do not commit uninvited.

#### Commits

Prefix with a type, then the change, then the PR number:

```
feature: indexer retries (#164)
fix: wrap the stake address in the compact breakpoint (#171)
chore: bump pallas to 0.31 (#168)
```

- Prefixes are `feature:`, `chore:`, `fix:` · never `feat:`/`refactor:`/`perf:` — match the
  existing log, not conventional-commits defaults.
- Append the PR number as `(#164)`.
- One concern per commit — never bundle a formatting sweep with a behaviour change.
- Imperative, present tense: "wrap the address", not "wrapped the address".

#### Pull requests

- Include a concise summary, the linked issue, test coverage for the change, and example output or
  screenshots where they apply.
- Note any feature flags involved (`native`, `wasm`).
- `[skip ci]` → needs a clear justification in the description, and any manual deployment or data
  task named (CI cannot catch what CI did not run).


## Prefer rustls over OpenSSL
<!-- rule: rules/org/defrag/rustls-over-openssl -->

Use `rustls` rather than OpenSSL for TLS in any new dependency or HTTP client, and when choosing
between two crates that differ only in which they pull in.

- Check a crate's default features for `native-tls` or `openssl-sys`; prefer a `rustls` feature
  where one exists:

```toml
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls", "json"] }
```

- Check the transitive tree, not the crate you typed — OpenSSL usually arrives because a crate's
  *default* features pulled it in:

```sh
nix develop -c cargo tree --target wasm32-unknown-unknown -p <crate> | grep -e openssl -e native-tls
```

## Zed mechanics

### Which instruction file is read

Zed uses **the first matching file** from this ordered list, at the worktree root:

```
.rules
.cursorrules
.windsurfrules
.clinerules
.github/copilot-instructions.md
AGENT.md
AGENTS.md
CLAUDE.md
GEMINI.md
```

Source: `https://zed.dev/docs/ai/instructions`, checked 2026-09-21.

Two consequences that matter here:

- **`AGENTS.md` outranks `CLAUDE.md`**, so a repo with both gets the `AGENTS.md` block and
  Zed never reads the `CLAUDE.md` one. That is the intended arrangement: `AGENTS.md` is
  canonical for Zed, `CLAUDE.md` is for Claude Code.
- **Anything above `AGENTS.md` in that list silently disables it.** A stray `.cursorrules`,
  `.rules` or `AGENT.md` (note: singular) and Zed reads that instead, with no error and no
  warning. `playbook install` and `playbook check` warn when they find one — see
  `instruction_files:` in `models/zed/overlay.conf`.

**In a multi-root workspace the choice is per worktree, not global.** Each root is its own
worktree and contributes its own first match, so one root can be read from `AGENTS.md` while
another is read from `CLAUDE.md`. (Observed 2026-09-21: `archivist`, which has only
`CLAUDE.md`, was read through it while three sibling roots were read through `AGENTS.md`.)

The consequence for installation is that a repo with only a lower-ranked file is read
through **whichever target generated that file** — so a `CLAUDE.md`-only repo gives Zed the
`claude-code` target's harness addenda, which name Claude Code's tools. Install both targets.
See `models/README.md`.

### Nested instruction files are not supported

Zed loads **one file, at the worktree root**. A nested `AGENTS.md` in a subdirectory is
ignored, regardless of which file is active.

This is not a configuration problem, and it is not something to work around:
`crates/agent/src/agent.rs` resolves the file with `entry_for_path(name)` against the
worktree root and takes only the first hit, and the field is typed
`WorktreeContext.rules_file: Option<RulesFileContext>` — one file by construction. The
behaviour is tracked as zed#60215 (July 2026, still open as of September 2026), where the
reporter notes that Codex CLI, Claude Code, Cursor and OpenCode all walk up from the active
file and concatenate, and Zed does not.

**So there is no path- or directory-scoped rule mechanism on Zed.** Area-of-focus guidance
has exactly one route: **skills**, which are loaded when the model invokes them or when I
invoke them by name. That means a domain rule either sits in the always-on block, or it
lives in a skill and is retrieved on a judgement call — there is no third option that gets
loaded automatically for a subtree.

Do not build on nested files, and do not write a rule that assumes one will be read.

### Personal instructions

`~/.config/zed/AGENTS.md` (Windows: `%APPDATA%\Zed\AGENTS.md`) is loaded as personal
instructions for **every** project, alongside the project file. The docs say project
instructions override personal `AGENTS.md` when they conflict.

**Verified 2026-09-21.** Both load. A marker written into the personal file was visible to a
fresh agent thread alongside the project blocks for all four workspace roots — and it also
appeared in an *already-running* thread on its next turn, so Zed re-reads the personal file
per prompt rather than only at thread start. A change there takes effect without restarting
anything.

**What "override when they conflict" actually means.** There is no structured merge. Both
files arrive as text, and "project wins" is a reading instruction to the model rather than a
mechanical per-rule resolution. So do not rely on precedence to settle a contradiction —
**avoid the overlap**. Keep personal instructions to rules that are true of every repository
and let project files carry the rest.

That is still a *duplication* win rather than a token win: personal and project instructions
both load, so a session pays for both. Use it so a core rule is maintained in one place
rather than reinstalled in five.

### Rendering

- **Mermaid renders.** ` ```mermaid ` blocks are drawn as diagrams. Supported: flowchart,
  sequence, class, state, ER, gantt, pie, gitgraph, mindmap, timeline, quadrant, xy, journey.
  Anything else shows as a code block.
- **Inline HTML inside mermaid is not rendered** — it is better to skip the formatting than
  to write `<b>` and have it appear literally.
- **Images render from remote URLs, absolute paths, and paths relative to a workspace root.**
  This is what makes the screenshot workflow usable — write the screenshot into `.tmp/` in the
  repo and reference it.
- Do not include `%%{init}%%` directives or custom `classDef` in mermaid; the theme is
  supplied.

### Editing

Prefer the editor tools for edits, shell commands for reads and builds. Zed shows the diff
for an editor edit and does not for a `sed` — see
`core/edit-via-editor-tools`.

## The sandboxed shell

Zed's agent terminal runs sandboxed by default. The restrictions are not errors to work
around — they are boundaries to recognise early, because each one presents as something else.

### No shell substitution in permission-protected commands

`$VAR`, `${VAR}`, `$(...)`, backticks, `$((...))`, `<(...)` and `>(...)` are rejected in any
command that needs approval. A loop like `for f in ~/x/*/SKILL.md; do cat "$f"; done` will be
refused outright. Resolve the values first, or pass the files as literal arguments:
`cat ~/x/a.md ~/x/b.md`.

### Git metadata is never writable

`.git` is read-only while sandboxed — no `commit`, no `branch`, no `stash`. That is consistent
with `rules/core/git-is-the-users-domain`:
history is the user's. Use `git --no-optional-locks status` and prefer read-only git flags
that avoid optional metadata writes.

### The nix daemon socket is refused

`nix develop` fails with `cannot connect to socket … Operation not permitted`, which reads
like a permissions problem and is actually the sandbox. Use `direnv exec . <cmd>` instead —
it reads the already-realised devshell out of `.direnv/` and needs no daemon. See
`rules/rust/devshell-first`.

### A spawned shell does not inherit the devshell

`direnv` loads the devshell on `cd`, which only happens in an interactive shell. Commands an agent
spawns run without it, so a tool that is "on `PATH` in the repo" is not on `PATH` for the agent —
`which <tool>` returns nothing, and the failure reads as "not installed". Reach for it explicitly
with `direnv exec . <tool> …` (which needs no daemon), or install it into the nix profile so it is
on `PATH` unconditionally.

### Network access is per-host

Outbound network is blocked by default. Access is granted per host through an HTTP/HTTPS
proxy, so `https://` URLs work where `git@`/`ssh://` do not. Request the specific hosts a
command needs rather than reaching for blanket access — the user sees and approves the list.

### Writes outside the project

Writable by default: the project roots and a per-thread temp directory (`$TMPDIR`). Anything
else needs an explicit grant. Prefer enumerating the paths over requesting blanket write
access.

<!-- END agent-playbook -->
