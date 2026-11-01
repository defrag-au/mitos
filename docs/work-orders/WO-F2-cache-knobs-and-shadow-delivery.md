# WO-F2 — Every redb cache is a named knob; a host can run in SHADOW

**Design refs:** `docs/design/FOOTPRINT_AND_MINIMUM_SPEC.md` §4 (leads 1, 2),
§5 (attribution experiment 2), §9 lever 1. This is that doc's first code.
**Lifecycle crate:** nothing. This order makes no lifecycle decision.

**Depends on:** nothing. **Blocks:** WO-F3 (the ARM bring-up and the squeeze
ladder), which cannot start without BOTH halves of this.

## What this order does, and where it deliberately stops

Two things the footprint work needs before a single number from the ARM box
can be believed:

1. **The redb caches become configuration.** Today the module KV store asks
   redb for a **10 GB** cache per module (`vendored/balius/kv.rs:55`,
   `cache_size.unwrap_or(10_000)`, and every production caller passes `None`),
   and four other stores take redb 2.6.3's default, which is **1 GiB each**
   (`db.rs:1140`). redb's cache is user-space ANONYMOUS memory: the kernel
   cannot evict it, so under a cgroup limit it OOMs instead of shrinking. The
   attribution experiment needs to clamp these and watch what moves.
2. **A host can be told it is a SHADOW.** The ARM box runs mainnet in
   isolation — so no companion ever subscribes to it, every dynamic-interest
   module scans for nothing, and the RSS, CPU and emissions growth we measure
   would all be flattering fictions. The fix is to give it production's
   persisted companion registrations. **And a host holding production's
   registrations must never, under any configuration mistake, speak to
   production's Durable Objects** — least of all a recapture WIPE.

**It changes NO default.** The knobs default to exactly today's effective
values (10 000 MB and 1 024 MB), and delivery defaults to `Live`. The footprint
doc is explicit that no optimisation lands before the attribution table
exists; choosing better defaults is WO-F4, with evidence. Deployed to the
production box with no new configuration, this order does nothing.

## Files

- `crates/mitos-platform/src/cache_budget.rs` — NEW.
- `crates/mitos-platform/src/storage.rs` — carries the budget to every open.
- `crates/mitos-platform/src/vendored/balius/kv.rs`, `emissions.rs`,
  `watched_ref_index.rs`, `indexer_data_cache.rs` — take a size.
- `crates/mitos-platform/src/delivery.rs` — NEW. `Delivery`.
- `crates/mitos-platform/src/dialer.rs`, `dialer/pool.rs` — honour it.
- `crates/mitos-core/src/indexer_bridge.rs` — honour it.
- `crates/mitos-platform/src/admin.rs` — status + `/metrics`.
- `crates/mitos-core/src/bundle.rs` — read the environment, ONCE, here.
- `docs/design/FOOTPRINT_AND_MINIMUM_SPEC.md` — §4 leads 1–2 become facts.

## The work

### 1. `CacheBudget`

```rust
pub enum StoreClass { Kv, Emissions, Watched, Cursor, IndexerData }

pub struct CacheBudget { /* one size per class, in bytes */ }

impl CacheBudget {
    /// Exactly what the host does today. NOT a recommendation.
    pub fn legacy() -> Self;          // Kv 10_000 MiB, every other class 1 GiB
    pub fn bytes(&self, class: StoreClass) -> usize;
    pub fn parse(vars: impl Fn(&str) -> Option<String>) -> Result<Self, CacheBudgetError>;
}
```

- `parse` starts from `legacy()` and overrides per class from
  `MITOS_CACHE_KV_MB`, `MITOS_CACHE_EMISSIONS_MB`, `MITOS_CACHE_WATCHED_MB`,
  `MITOS_CACHE_CURSOR_MB`, `MITOS_CACHE_INDEXER_DATA_MB`. It takes a LOOKUP
  function, not the process environment: no test depends on process-global
  env (the rule `Authenticator::from_env` already follows). `bundle.rs` passes
  `|k| std::env::var(k).ok()`.
- A value that does not parse, or is `0`, is a STARTUP ERROR naming the
  variable. A typo that silently meant "10 GB" is how this got here.
- `ModuleStorage::new(root)` keeps its signature and means `legacy()` — it has
  dozens of callers in tests. Add `ModuleStorage::with_cache_budget(root,
  budget)`; `bundle.rs` uses it.
- Every open site calls `.set_cache_size(budget.bytes(class))` EXPLICITLY —
  including the four that took the library default, so that upgrading redb
  can never silently move a number. `RedbKv::try_new`'s `cache_size:
  Option<usize>` parameter becomes a required `usize`; delete `unwrap_or`.
- `ModuleStorage::kv_store(id, cache_size)` / `kv_store_at(key, cache_size)`
  LOSE their `cache_size: Option<usize>` parameter. It has ~26 callers and
  every one of them passes `None` — a knob nobody has ever turned, sitting
  beside the one that now works, is how a future caller sizes ONE store by
  hand and defeats the budget. Let the compiler enumerate them.
- Enable redb's `cache_metrics` feature on the workspace dependency. It is
  what makes `Database::cache_stats()` count anything, and eviction counts
  are the signal the squeeze ladder turns on: a clamp that is too tight shows
  up as evictions climbing long before it shows up as tip lag.
- `into_ephemeral` (the in-memory KV) is not a file and is not in scope.

### 2. `Delivery` — and ONE choke point

```rust
pub enum Delivery {
    /// Dial companions. Production.
    Live,
    /// NEVER open a connection to a companion. Queue, then mark delivered.
    Shadow,
}
```

Read from `MITOS_DELIVERY` (`live` | `shadow`; absent ⇒ `Live`; anything else
is a startup error) in `bundle.rs`, and handed to the `CompanionDialer` and
the indexer bridge at construction.

There are FOUR places the host opens a connection to a companion. Find them
yourself and confirm this list is complete — if you find a fifth, stop and
report it, because that is the one that would have wiped a production DO:

| Site | What it sends |
|---|---|
| `dialer/pool.rs` — per-row send | an `apply`, or an `undo` |
| `dialer/pool.rs` — bulk send | an `apply-bulk` batch |
| `dialer.rs` — `post_recapture` | **a recapture WIPE**, and `recapture-ready` |
| `mitos-core/src/indexer_bridge.rs` — `run_indexer_dial` | a WebSocket to `dial_back.url` |

In `Shadow`:
- The two `pool.rs` sends return what a healthy companion would have — the
  per-row path as a 2xx, the bulk path as "every emission applied" — WITHOUT
  building a request. So the emission is written, scanned, planned, passed
  through the lanes and the status writer, marked `Acked`, and compacted:
  everything that costs memory and disk still runs. Only the socket does not.
- `post_recapture` returns success without sending. A recapture on a shadow
  host still wipes and rebuilds the MODULE's state, which is what costs.
- `run_indexer_dial` logs once and returns; no socket.
- **The `reqwest::Client` is not built at all** (`build_http_client`). A
  shadow dialer that HAS no client cannot send by accident through a path
  nobody listed. Make the client an `Option` held by whatever owns it, so the
  compiler makes every send site say what it does without one.

The other outbound traffic is NOT companions and is not touched: the
chain-data fallback (koios), and WO-05's artifact fetcher. A shadow host still
needs the first, and nothing announces to it so the second never runs.

### 3. Say so, everywhere an operator looks

- Boot: one `warn!` line in `Shadow` naming what is suppressed; one `info!`
  line with the five cache sizes in MiB, always.
- `GET /_admin/status`: `delivery` and the cache budget, typed (no `json!`).
- `GET /metrics`:
  - `mitos_delivery_shadow` (0/1).
  - `mitos_redb_cache_budget_bytes{class}`.
  - `mitos_redb_cache_evictions_total{class}` — summed over every open store
    of that class (`cache_stats().evictions()`). Read the handles the storage
    already caches; do not open a store to count it.
  - `mitos_process_resident_bytes` and `mitos_process_anonymous_bytes`, from
    `/proc/self/smaps_rollup` (Linux only; absent elsewhere — do not fail the
    scrape). This is the anon/file split the attribution in §5 turns on.
  - `mitos_shadow_suppressed_total{kind}` — `apply`, `bulk`, `undo`,
    `recapture`, `indexer`. If this is not climbing on a shadow host, its
    modules are not emitting, and the footprint numbers are fiction again.

### 4. The doc

`FOOTPRINT_AND_MINIMUM_SPEC.md` §4: leads 1 and 2 are no longer hypotheses.
State the redb version, its default (1 GiB, with the file:line), and the five
variables. Add the co-tenancy note to §5: an isolated box needs `Shadow` plus
production's `companions/` directories, or its modules do no work.

## Out of scope — do not do these

- **Changing any default.** Not the 10 GB, tempting as it is. WO-F4.
- A host-wide budget divided across live instances. WO-F4.
- A global allocator, `MALLOC_ARENA_MAX`, jemalloc. Footprint doc lever 2.
- dolos's own cache and retention (`dolos.toml`: `cache = 500`,
  `max_history = 604800`). Infra, not this repo.
- Copying registrations between boxes. WO-F1 / F3, in `infra`.
- Anything in `mitos-lifecycle`.

## Tests that encode the old behaviour

None. `ModuleStorage::new` keeps its meaning. Dropping `kv_store`'s unused
parameter touches one integration test — `tests/two_instances_v2.rs:149`,
`storage.kv_store_at(key, None)` → `storage.kv_store_at(key)`. That one
argument is sanctioned; no assertion, fixture or helper changes.

## Acceptance

1. `nix develop -c cargo test -p mitos-platform -p mitos-core`.
2. clippy `--workspace --all-targets -D warnings` 0;
   `cargo check --workspace --all-targets` 0; `cargo fmt --check` clean.
3. `cache_budget.rs`:
   - `legacy_is_exactly_what_the_host_did_before` — 10 000 MiB and 1 GiB,
     written as literals in the test, NOT derived from the code under test.
   - `each_variable_overrides_only_its_own_class`.
   - `a_value_that_does_not_parse_refuses_to_start_and_names_the_variable`,
     and the same for `0`.
   - `no_open_site_relies_on_the_librarys_default` — a test per store class
     that opens it under a budget of 1 MiB and asserts, through redb's
     `cache_stats().evictions()` after writing a few MiB, that the clamp
     took. (A size that is passed but ignored is the failure this catches.)
4. `Delivery`:
   - `a_shadow_host_never_opens_a_connection` — the receiver from WO-07's
     dialer tests, registered companions with queued rows, a recapture and an
     undo in flight: the receiver saw NOTHING, under a timeout.
   - `a_shadow_host_still_marks_rows_delivered_and_compacts_them`.
   - `a_shadow_recapture_reports_ready_without_wiping_anybody`.
   - `a_shadow_indexer_dial_opens_no_socket`.
   - `a_shadow_dialer_has_no_http_client_to_send_with`.
   - `an_unknown_delivery_mode_refuses_to_start`.
   - `with_no_configuration_delivery_is_live` — and the existing dialer tests
     pass untouched.
5. Admin: status and `/metrics` carry the new fields; the scrape succeeds on
   macOS with the `smaps` gauges simply absent.
6. Goldens 69/69.

## Deploy note (for the operator — NOT part of this order)

Safe to deploy to the production box with no configuration change: it is
`Live` and `legacy()`. On the ARM box the unit file sets `MITOS_DELIVERY=shadow`
— and WO-F1 makes that box unable to reach the companions anyway, as a second
lock: a firewall rule is cheap, and a wiped production DO is not.
