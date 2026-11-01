//! `holders-from-fold` — a published holder table, served as a
//! `collection-holders` snapshot without touching the chain.
//!
//! The fold plane builds a holder table offline (`fold-probe pilot` →
//! `holders.tsv` + `artifact.meta`) and a host serves the directory through
//! the `fold-data` interface (`MITOS_FOLD_ARTIFACT`). This module is the
//! reader: it names the file, pages it, parses it with the table's own rows
//! codec, and emits the same `SnapshotBegin` → `SnapshotChunk`* →
//! `SnapshotEnd` sequence `collection-holders` emits — from a file rather
//! than from a policy scan.
//!
//! ## Why this exists
//!
//! `collection-holders` cold-starts by walking `utxos_by_policy(X)`, which
//! costs one scan per policy, pages against a fuel budget, and shards the
//! result into state-kv so no call materialises the ledger. When the answer
//! was already computed for a base slot, all of that is redundant work: the
//! artifact *is* the aggregate, and the module is a reader. That is the fold
//! plane's claim — *an aggregate is a definition on a shared substrate, not
//! another pipeline* — executed rather than described
//! (`docs/design/FOLD_PLANE.md` §7).
//!
//! ## What it deliberately does not do
//!
//! - **No interest.** The artifact names its subject in extension tag 1
//!   (`HolderTable::POLICY_EXTENSION`), so there is nothing to subscribe to
//!   and no scan to scope. `update-interest` accepts and ignores.
//! - **No deltas.** The table is a *position* at its base slot; everything
//!   after it belongs to the flow plane. `handle-events` is a no-op, and a
//!   companion that wants live movement keeps `collection-holders` beside
//!   this one.
//! - **No stamp check.** The base is reported in `SnapshotBegin` and is not
//!   compared with the followed chain, because a host may serve an artifact
//!   built from a different network than it follows — the local stack does
//!   exactly that. A consumer that cares pairs the two itself.
//!
//! ## Why `rebootstrap` and not `init`
//!
//! `init` is **light** by the v2 contract: no bootstrap work, no state
//! hydration. This module's entire state is a bootstrap, so it emits from
//! `rebootstrap`, which the host drives to completion on a disposable
//! instance (`start(module, rebootstrap = true)` at startup, and again on
//! operator recapture). `init` only reads and validates the manifest.
//!
//! ## Work per call
//!
//! Three phases, one bounded unit each, held in thread-local state because
//! nothing here is worth a durable cursor:
//!
//! 1. **Read** — one `fold-data::read` page per call. The host caps a page
//!    at 256 KiB and refuses more rather than clamping, so `READ_PAGE` is
//!    that cap. The pilot's rows file fits one page; the multi-page loop is
//!    exercised by any artifact above it, and a boundary mistake there fails
//!    loudly — `SnapshotBegin` with no `SnapshotEnd` — rather than silently.
//! 2. **Parse** — one call: `HolderTable::from_rows` over the assembled
//!    file, then flatten and sort to the wire order
//!    `(asset_name_hex, holder)`.
//! 3. **Emit** — one `SnapshotChunk` per call, opening with
//!    `SnapshotBegin` and closing with `SnapshotEnd`.
//!
//! Phase 2 is the only unbounded step, and the artifact bounds it: the
//! pilot's rows file is 52,603 bytes over 294 holders. It is also the step
//! that disappears when the row encoding moves off TSV — a binary sorted
//! array is read by index rather than assembled and split — so the follow-up
//! is named in `FOLD_PLANE.md` §7 rather than worked around here.
//!
//! A trap mid-sequence loses the thread-local phase, and the host restarts
//! the round from page 0. That is safe rather than clever: `SnapshotBegin`
//! is an authoritative replacement on the consumer side, so a re-emitted
//! sequence converges instead of doubling.

use std::cell::RefCell;

use mitos_cohort::is_script_address;
use mitos_community_events::collection_holders::{
    CollectionEvent, HolderRef, Holding, SnapshotBegin, SnapshotChunk, SnapshotEnd,
};
use mitos_fold::{Artifact, SchemaId};
use mitos_holder_table::{HolderKey, HolderRow, HolderTable};

use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::fold_data;
use crate::mitos::platform_v2::logging::{self, LogLevel};

const LOG_TARGET: &str = "holders-from-fold";

/// The schema this module reads.
///
/// Named here because **the reader names the schema** — `Artifact::decode`
/// refuses a manifest that declares another, by name, rather than handing
/// back rows this build does not know how to parse.
const SCHEMA: SchemaId = SchemaId::new("holder-table", 1);

/// Holdings per `SnapshotChunk`. The same bound `collection-holders` uses,
/// so a consumer sees one chunk shape regardless of which module produced
/// it (`WASM_BUDGET_CHUNKING.md`).
const SNAPSHOT_CHUNK_HOLDINGS: usize = 1_000;

/// Bytes asked for per `fold-data::read` call — the host's own cap
/// (`mitos_data_plane::MAX_PAGE`).
const READ_PAGE: u32 = 256 * 1024;

/// What this host serves, reduced to what the module needs.
#[derive(Clone)]
struct Served {
    policy: [u8; 28],
    base_slot: u64,
    base_hash_hex: String,
    rows_file: String,
    rows_bytes: u64,
}

/// Where the current round has got to.
///
/// `Idle` is both "not started" and "finished": a second `rebootstrap` after
/// `done` starts a fresh round and re-emits, which is the recapture contract.
enum Round {
    /// No round in flight; the next `rebootstrap` starts one.
    Idle,
    /// Reading the rows file, one host page per call.
    Reading { offset: u64, bytes: Vec<u8> },
    /// Emitting the parsed holdings, one chunk per call.
    Emitting {
        holdings: Vec<Holding>,
        emitted: usize,
    },
}

thread_local! {
    /// The artifact this instance serves, resolved once at `init`.
    static SERVED: RefCell<Option<Served>> = const { RefCell::new(None) };
    /// The in-flight round. Volatile on purpose — see the module docs.
    static ROUND: RefCell<Round> = const { RefCell::new(Round::Idle) };
}

// ============================================================
// The artifact
// ============================================================

/// Read and validate the manifest this host serves.
///
/// `None` for every reason the module cannot proceed, each logged with its
/// own message: a host that serves no artifact is a configuration state, not
/// a module bug, and the distinction has to survive into the log.
fn load() -> Option<Served> {
    let Some(manifest) = fold_data::meta() else {
        logging::log(
            LogLevel::Warn,
            LOG_TARGET,
            "this host serves no fold artifact (MITOS_FOLD_ARTIFACT is unset or does not open); \
             the module is inert",
        );
        return None;
    };

    let artifact = match Artifact::decode(&manifest, SCHEMA) {
        Ok(artifact) => artifact,
        Err(error) => {
            logging::log(
                LogLevel::Error,
                LOG_TARGET,
                &format!("the served artifact is not a holder table: {error}"),
            );
            return None;
        }
    };

    let Some(policy) = HolderTable::policy_of(&artifact.extensions) else {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            "the served artifact names no policy (extension tag 1 of 28 bytes); \
             a holder table without its subject cannot be emitted",
        );
        return None;
    };

    let Some(rows) = artifact
        .files
        .iter()
        .find(|file| file.name == HolderTable::ROWS_FILE)
    else {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!(
                "the served artifact lists no `{}`; files: {:?}",
                HolderTable::ROWS_FILE,
                artifact
                    .files
                    .iter()
                    .map(|file| file.name.as_str())
                    .collect::<Vec<_>>(),
            ),
        );
        return None;
    };

    logging::log(
        LogLevel::Info,
        LOG_TARGET,
        &format!(
            "serving {} · policy {} · base slot {} · {} bytes",
            SCHEMA,
            hex::encode(policy),
            artifact.stamp.base_slot,
            rows.bytes,
        ),
    );

    Some(Served {
        policy,
        base_slot: artifact.stamp.base_slot,
        base_hash_hex: artifact
            .stamp
            .base_hash
            .map(hex::encode)
            .unwrap_or_default(),
        rows_file: rows.name.clone(),
        rows_bytes: rows.bytes,
    })
}

// ============================================================
// Rows → wire holdings
// ============================================================

/// The wire identity of a holder the table already keyed.
///
/// `HolderKey::Stake` is the stake credential — Key and Script delegation
/// collapse there, exactly as `HolderRef::Stake` documents. An enterprise
/// address has no credential to collapse to, so it stays a full bech32 and
/// the payment kind decides the variant. `is_script_address` reads that
/// through pallas rather than from the cohort: a script-payment address can
/// be *classified* `vesting` or `pool` while still being a script, and the
/// wire shape is about the address, not about what we know of the contract.
fn holder_ref(row: &HolderRow) -> HolderRef {
    match &row.holder {
        HolderKey::Stake(credential) => HolderRef::Stake(hex::encode(credential)),
        HolderKey::Enterprise(address) => {
            if is_script_address(address) {
                HolderRef::Script {
                    addr: address.clone(),
                    // The `address-registry` lookup `collection-holders` also
                    // leaves for a follow-up; the variant exists so consumers
                    // can branch, and `None` is the honest value here.
                    label: None,
                }
            } else {
                HolderRef::Payment(address.clone())
            }
        }
    }
}

/// The whole table, flattened to `(asset_name, holder)` and sorted the way
/// `SnapshotChunk::holdings` documents.
fn holdings_from(table: &HolderTable) -> Vec<Holding> {
    let mut holdings: Vec<Holding> = Vec::with_capacity(table.balances());
    for row in &table.holders {
        let holder = holder_ref(row);
        for asset in &row.assets {
            holdings.push(Holding {
                asset_name_hex: hex::encode(&asset.name),
                holder: holder.clone(),
                quantity: asset.quantity,
            });
        }
    }
    holdings.sort_by(|a, b| (&a.asset_name_hex, &a.holder).cmp(&(&b.asset_name_hex, &b.holder)));
    holdings
}

// ============================================================
// Emission
// ============================================================

fn emit_event(event: &CollectionEvent) {
    let mut buf = Vec::with_capacity(512);
    if let Err(error) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode CollectionEvent failed: {error}"),
        );
        return;
    }
    // Partition key = policy hex, the same key `collection-holders` uses: the
    // dialer serialises same-policy events into one lane (snapshot ordering)
    // and the host's fan-out filters each emission to companions interested
    // in that policy.
    emit::emit_event_keyed(0, event_policy(event).as_bytes(), &buf);
}

/// The policy every `CollectionEvent` carries.
fn event_policy(event: &CollectionEvent) -> &str {
    match event {
        CollectionEvent::SnapshotBegin(begin) => &begin.policy,
        CollectionEvent::SnapshotChunk(chunk) => &chunk.policy,
        CollectionEvent::SnapshotEnd(end) => &end.policy,
        CollectionEvent::Delta(delta) => &delta.policy,
    }
}

fn emit_begin(served: &Served) {
    emit_event(&CollectionEvent::SnapshotBegin(SnapshotBegin {
        policy: hex::encode(served.policy),
        cursor_slot: served.base_slot,
        cursor_hash_hex: served.base_hash_hex.clone(),
    }));
}

fn emit_chunk(policy: [u8; 28], holdings: &[Holding]) {
    emit_event(&CollectionEvent::SnapshotChunk(SnapshotChunk {
        policy: hex::encode(policy),
        holdings: holdings.to_vec(),
    }));
}

fn emit_end(policy: [u8; 28], holding_count: u64) {
    emit_event(&CollectionEvent::SnapshotEnd(SnapshotEnd {
        policy: hex::encode(policy),
        holding_count,
    }));
}

// ============================================================
// The round
// ============================================================

/// Assemble the file, one page at a time, and hand back its text.
///
/// Returned from the reading phase so the caller owns the buffer and can
/// parse it without a copy. An invalid UTF-8 file is refused rather than
/// lossily converted: the rows codec's columns are hex and bech32, so a byte
/// outside ASCII means the artifact is not the one this reader knows.
fn assembled(bytes: Vec<u8>) -> Result<String, String> {
    String::from_utf8(bytes).map_err(|error| {
        format!(
            "the rows file is not UTF-8 at byte {}",
            error.utf8_error().valid_up_to()
        )
    })
}

fn step(round: Round, served: &Served) -> (Round, Result<RebootstrapStep, String>) {
    let nothing = Ok(RebootstrapStep {
        done: false,
        ingested: 0,
    });

    match round {
        Round::Idle => (
            Round::Reading {
                offset: 0,
                bytes: Vec::new(),
            },
            nothing,
        ),

        Round::Reading { offset, mut bytes } => {
            if offset >= served.rows_bytes {
                // The file is whole — parse it.
                let text = match assembled(bytes) {
                    Ok(text) => text,
                    Err(reason) => return (Round::Idle, Err(reason)),
                };
                let table = match HolderTable::from_rows(served.policy, &text) {
                    Ok(table) => table,
                    Err(error) => {
                        return (
                            Round::Idle,
                            Err(format!("{} did not read back: {error}", served.rows_file)),
                        );
                    }
                };
                logging::log(
                    LogLevel::Info,
                    LOG_TARGET,
                    &format!(
                        "parsed {} · {} holders · {} balances",
                        served.rows_file,
                        table.distinct_holders(),
                        table.balances(),
                    ),
                );
                (
                    Round::Emitting {
                        holdings: holdings_from(&table),
                        emitted: 0,
                    },
                    nothing,
                )
            } else {
                let Some(page) = fold_data::read(&served.rows_file, offset, READ_PAGE) else {
                    return (
                        Round::Idle,
                        Err(format!(
                            "fold-data refused `{}` at offset {offset} of {}",
                            served.rows_file, served.rows_bytes,
                        )),
                    );
                };
                if page.is_empty() {
                    return (
                        Round::Idle,
                        Err(format!(
                            "fold-data returned no bytes for `{}` at offset {offset} of {}",
                            served.rows_file, served.rows_bytes,
                        )),
                    );
                }
                bytes.extend_from_slice(&page);
                let offset = offset + page.len() as u64;
                (
                    Round::Reading { offset, bytes },
                    Ok(RebootstrapStep {
                        done: false,
                        ingested: 0,
                    }),
                )
            }
        }

        Round::Emitting { holdings, emitted } => {
            if emitted == 0 {
                emit_begin(served);
            }
            let end = (emitted + SNAPSHOT_CHUNK_HOLDINGS).min(holdings.len());
            if end > emitted {
                emit_chunk(served.policy, &holdings[emitted..end]);
            }
            let ingested = (end - emitted) as u64;

            if end == holdings.len() {
                emit_end(served.policy, holdings.len() as u64);
                logging::log(
                    LogLevel::Info,
                    LOG_TARGET,
                    &format!("emitted snapshot · {} holding(s)", holdings.len()),
                );
                (
                    Round::Idle,
                    Ok(RebootstrapStep {
                        done: true,
                        ingested,
                    }),
                )
            } else {
                (
                    Round::Emitting {
                        holdings,
                        emitted: end,
                    },
                    Ok(RebootstrapStep {
                        done: false,
                        ingested,
                    }),
                )
            }
        }
    }
}

// ============================================================
// v2 Guest impl
// ============================================================

struct Module;

impl Guest for Module {
    fn module_version() -> (u32, u32) {
        (2, 0)
    }

    fn trap_policy() -> (TrapStrategy, RetryPolicy) {
        (
            TrapStrategy::Replay,
            RetryPolicy {
                max_retries: 3,
                backoff_cap_ms: 1_000,
            },
        )
    }

    /// Read the manifest and nothing else.
    ///
    /// The v2 contract calls `init` light, and this module keeps to it: the
    /// work is one host call and a text parse of the envelope, both bounded
    /// by the manifest's size. `config` is the module's own `.toml` as CBOR,
    /// and it is accepted rather than read — the artifact names its own
    /// subject, so there is nothing for an operator to configure, and an
    /// operator-set "which policy" could only disagree with the rows.
    fn init(config: Vec<u8>) {
        let served = load();
        logging::log(
            LogLevel::Info,
            LOG_TARGET,
            &format!(
                "init: config {} byte(s) (ignored) · {}",
                config.len(),
                if served.is_some() {
                    "artifact loaded"
                } else {
                    "no artifact — inert"
                },
            ),
        );
        SERVED.with(|slot| *slot.borrow_mut() = served);
    }

    /// Nothing: a position at a base slot has no events to apply.
    fn handle_events(_events: Vec<DispatchEvent>) {}

    /// Nothing to add or remove: the artifact's subject is fixed.
    ///
    /// Returning `Ok` rather than an error is deliberate — a companion that
    /// re-asserts interest after a reconnect must not see a failure for a
    /// module that simply has no interest set.
    fn update_interest(_op: InterestOp, _items_cbor: Vec<u8>) -> Result<(), String> {
        Ok(())
    }

    /// Re-emit the artifact's snapshot, one bounded unit of work per call.
    ///
    /// `mode` is accepted and ignored: there is no scan to scope, so `Onboard`
    /// and `Full` are the same round. Idempotent by construction — a second
    /// round re-emits the same `Begin` → `Chunk`* → `End`, which the consumer
    /// treats as an authoritative replacement.
    fn rebootstrap(_mode: RebootstrapMode) -> Result<RebootstrapStep, String> {
        let served = SERVED.with(|slot| slot.borrow().clone());
        let Some(served) = served else {
            return Ok(RebootstrapStep {
                done: true,
                ingested: 0,
            });
        };

        ROUND.with(|cell| {
            let mut slot = cell.borrow_mut();
            let round = std::mem::replace(&mut *slot, Round::Idle);
            let (next, result) = step(round, &served);
            *slot = next;
            result
        })
    }
}

export!(Module);
