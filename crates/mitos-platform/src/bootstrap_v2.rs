//! v2 bootstrap orchestrator — synthesise events for the
//! current unspent set at watched addresses, dispatch them
//! through `DriverV2` so the module sees historical state as
//! a stream of typed events.
//!
//! Per-module state-kv flags track per-address completion:
//! `__platform/bootstrap/<addr>` records that the address has
//! been hydrated. Subsequent runs skip already-completed
//! addresses; an operator who wants to re-hydrate (after a
//! companion-DO schema change, say) deletes the flag and the
//! orchestrator re-runs.
//!
//! Idempotence is the contract: companion DOs must accept
//! duplicate `Created` events and converge. The orchestrator
//! re-emits whenever the per-address flag is missing.

use std::collections::BTreeMap;

use mitos_data_plane::{
    block_events::datum_from_draft, ChainDataPlane, ChainPoint, InterestPredicate, InterestSet,
    OutputRef, ProducedEvent, TxContextEvent, TxEventBatch, TypedOutput, UtxoEvent,
    ValidityInterval,
};
use pallas_primitives::Hash;

use crate::driver_v2::DriverV2;
use crate::host_fns::state_kv::ModuleKv;
use crate::vendored::balius::kv::KvError;

/// Result of one bootstrap pass over an interest set.
#[derive(Debug, Default, Clone, Copy)]
pub struct BootstrapStats {
    /// Number of `at_address` predicates seen.
    pub addresses_seen: usize,
    /// Number of addresses we actually scanned (ones missing
    /// the completion flag).
    pub addresses_scanned: usize,
    /// Total UTxOs synthesised into events across all scanned
    /// addresses.
    pub utxos_dispatched: usize,
    /// Number of synthetic `handle-events` calls made.
    pub batches_dispatched: usize,
}

/// Reserved state-kv key prefix the platform uses for per-
/// address bootstrap completion flags. Modules MUST NOT touch
/// keys under this prefix — `__platform/...` is platform-
/// reserved namespace.
fn bootstrap_flag_key(address: &str) -> String {
    format!("__platform/bootstrap/{address}")
}

/// Run bootstrap for one module. Walks the interest set,
/// scans each not-yet-bootstrapped `at_address` predicate,
/// synthesises events, dispatches batches through the driver.
/// Returns when all addresses have been hydrated.
///
/// `module_id` is needed to namespace state-kv accesses (the
/// underlying `ModuleKv` keys by `(module_id, key)`).
pub async fn run_bootstrap<P: ChainDataPlane + Sync>(
    driver: &mut DriverV2,
    module_id: &str,
    kv: &mut ModuleKv,
    interest: &InterestSet,
    plane: &P,
) -> anyhow::Result<BootstrapStats> {
    let mut stats = BootstrapStats::default();
    let addresses: Vec<&str> = interest.watched_addresses().collect();
    stats.addresses_seen = addresses.len();

    for address in addresses {
        let key = bootstrap_flag_key(address);
        if has_flag(kv, module_id, &key)? {
            tracing::debug!(
                module = %module_id,
                address,
                "bootstrap: skipping; already complete",
            );
            continue;
        }
        tracing::info!(
            module = %module_id,
            address,
            "bootstrap: scanning current unspent set",
        );
        let scanned = scan_one_address(driver, address, plane).await?;
        stats.addresses_scanned += 1;
        stats.utxos_dispatched += scanned.utxos;
        stats.batches_dispatched += scanned.batches;

        // Persist completion. Failure here surfaces — we can't
        // safely advance without recording or we'd re-emit on
        // restart and slow the next deploy.
        set_flag(kv, module_id, &key)?;
        tracing::info!(
            module = %module_id,
            address,
            utxos = scanned.utxos,
            batches = scanned.batches,
            "bootstrap: address complete",
        );
    }

    Ok(stats)
}

#[derive(Debug, Default)]
struct AddressScanResult {
    utxos: usize,
    batches: usize,
}

/// Scan one address: enumerate refs, bulk-resolve outputs +
/// datums, group by producing TX, dispatch one batch per
/// producing-TX through the driver.
async fn scan_one_address<P: ChainDataPlane + Sync>(
    driver: &mut DriverV2,
    address: &str,
    plane: &P,
) -> anyhow::Result<AddressScanResult> {
    let refs = plane
        .utxos_by_address(address)
        .await
        .map_err(|e| anyhow::anyhow!("utxos_by_address({address}): {e}"))?;
    if refs.is_empty() {
        return Ok(AddressScanResult::default());
    }

    let outputs = plane
        .read_utxos(&refs, mitos_data_plane::DecodeLevel::Lean)
        .await
        .map_err(|e| anyhow::anyhow!("read_utxos: {e}"))?;
    let datums = plane
        .read_output_datums(&refs)
        .await
        .map_err(|e| anyhow::anyhow!("read_output_datums: {e}"))?;

    // Index outputs + datums by ref so we can pair with the
    // ordered ref list. `read_utxos` may omit refs the host
    // couldn't resolve (spent mid-scan); the iteration handles
    // that by skipping missing entries.
    let mut output_by_ref: std::collections::HashMap<(Hash<32>, u32), TypedOutput> =
        std::collections::HashMap::new();
    for (oref, out) in outputs {
        output_by_ref.insert((oref.tx_hash, oref.index), out);
    }
    // `read_output_datums` is parallel-to-input; pair against
    // the requested refs in order.
    let mut datum_by_ref: std::collections::HashMap<
        (Hash<32>, u32),
        mitos_data_plane::TypedDatum,
    > = std::collections::HashMap::new();
    for (oref, datum_opt) in refs.iter().zip(datums.into_iter()) {
        if let Some(td) = datum_opt {
            datum_by_ref.insert((oref.tx_hash, oref.index), td);
        }
    }

    // Group refs by producing tx_hash. BTreeMap keeps a stable
    // order for deterministic dispatch.
    let mut by_tx: BTreeMap<Hash<32>, Vec<OutputRef>> = BTreeMap::new();
    for r in refs {
        by_tx.entry(r.tx_hash).or_default().push(r);
    }

    let mut result = AddressScanResult::default();
    for (tx_hash, group_refs) in by_tx {
        // Synthesise tx-context (no validity interval / signers
        // — we don't refetch the producing TX; modules that need
        // those call `chain_data::tx_metadata` lazily).
        // Cursor: slot-only chain point keyed off the host's
        // current tip, since we don't have the producing
        // block's hash at this layer. v2 events emit before
        // any real block dispatch so the cursor ordering is
        // {bootstrap events, then live chain events}.
        let cursor = ChainPoint::Origin;

        let mut batch = TxEventBatch::new(0);
        batch.push(UtxoEvent::TxContext(TxContextEvent {
            cursor: cursor.clone(),
            tx_hash,
            tx_idx: 0,
            validity_interval: ValidityInterval::default(),
            required_signers: Vec::new(),
        }));

        let mut group_utxos = 0;
        for r in group_refs {
            let key = (r.tx_hash, r.index);
            let Some(output) = output_by_ref.remove(&key) else {
                continue; // host couldn't resolve; skip
            };
            // Pair with the bootstrap-side TypedDatum if we
            // have one. The dispatch composer uses
            // `datum_from_draft` to project from drafts; here
            // we bypass that and use the data-plane-resolved
            // form directly.
            let datum = datum_by_ref.remove(&key);
            let _ = datum_from_draft; // keep import alive

            batch.push(UtxoEvent::Produced(ProducedEvent {
                cursor: cursor.clone(),
                tx_hash,
                tx_idx: 0,
                oref: r,
                output,
                datum,
            }));
            group_utxos += 1;
        }

        // Skip empty groups (every ref unresolved).
        if group_utxos == 0 {
            continue;
        }

        driver
            .dispatch_synthetic_batch(batch, cursor)
            .await
            .map_err(|e| anyhow::anyhow!("dispatch_synthetic_batch: {e}"))?;
        result.utxos += group_utxos;
        result.batches += 1;
    }

    Ok(result)
}

// ----- state-kv flag helpers --------------------------------

fn has_flag(kv: &ModuleKv, module_id: &str, key: &str) -> anyhow::Result<bool> {
    match kv {
        ModuleKv::InMemory(map) => Ok(map.contains_key(key)),
        ModuleKv::Redb(redb) => match redb.get_value(module_id, key) {
            Ok(_) => Ok(true),
            Err(KvError::NotFound(_)) => Ok(false),
            Err(e) => Err(anyhow::anyhow!("kv.get: {e}")),
        },
    }
}

fn set_flag(kv: &mut ModuleKv, module_id: &str, key: &str) -> anyhow::Result<()> {
    match kv {
        ModuleKv::InMemory(map) => {
            map.insert(key.to_owned(), b"1".to_vec());
            Ok(())
        }
        ModuleKv::Redb(redb) => redb
            .set_value(module_id, key, b"1".to_vec())
            .map_err(|e| anyhow::anyhow!("kv.set: {e}")),
    }
}

/// Convenience: build an InterestSet from a list of bech32
/// addresses. Used by `mitos-run` and (eventually) the
/// manifest-driven auto-bootstrap path.
pub fn interest_from_addresses(addresses: &[String]) -> InterestSet {
    let mut set = InterestSet::default();
    for a in addresses {
        set.add(InterestPredicate::AtAddress(a.clone()));
    }
    set
}
