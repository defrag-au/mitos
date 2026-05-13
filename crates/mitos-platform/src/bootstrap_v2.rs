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

use cardano_assets::PolicyId;
use mitos_data_plane::{
    AssetPattern, ChainDataPlane, ChainPoint, DecodeLevel, InterestPredicate, InterestSet,
    OutputRef, PageRequest, ProducedEvent, TxContextEvent, TxEventBatch, TypedOutput, UtxoEvent,
    UtxoPattern, UtxoPredicate, ValidityInterval, block_events::datum_from_draft,
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
    /// Number of `holds_policy` predicates seen.
    pub policies_seen: usize,
    /// Number of policies we actually scanned (ones missing
    /// the completion flag).
    pub policies_scanned: usize,
    /// Number of `at_payment_cred` predicates seen.
    pub payment_creds_seen: usize,
    /// Number of payment creds we actually scanned (ones missing
    /// the completion flag).
    pub payment_creds_scanned: usize,
    /// Total UTxOs synthesised into events across all scanned
    /// addresses + policies + payment creds.
    pub utxos_dispatched: usize,
    /// Number of synthetic `handle-events` calls made.
    pub batches_dispatched: usize,
}

/// Reserved state-kv key prefix for per-address bootstrap
/// completion flags. Modules MUST NOT touch keys under
/// `__platform/...` — that's platform-reserved namespace.
/// Existing deploys' flag keys live at this exact path, so
/// the format stays stable across the bootstrap-by-policy
/// addition (otherwise a redeploy would re-bootstrap every
/// already-hydrated address).
fn bootstrap_flag_key_for_address(address: &str) -> String {
    format!("__platform/bootstrap/{address}")
}

/// Reserved state-kv key prefix for per-policy bootstrap
/// completion flags. Separate namespace from the address flags
/// — addresses are bech32 (`addr1...`), policies are bare hex,
/// so the values wouldn't collide in practice, but the explicit
/// `policy/` segment keeps the two paths visually distinct in
/// state-kv dumps.
fn bootstrap_flag_key_for_policy(policy: &PolicyId) -> String {
    format!("__platform/bootstrap/policy/{policy}")
}

/// Reserved state-kv key prefix for per-payment-cred bootstrap
/// completion flags. Hex-encodes the 28-byte cred so the key is
/// printable.
fn bootstrap_flag_key_for_payment_cred(cred: &[u8; 28]) -> String {
    format!("__platform/bootstrap/payment-cred/{}", hex::encode(cred))
}

/// Run bootstrap for one module. Walks the interest set,
/// scans each not-yet-bootstrapped `at_address` and
/// `holds_policy` predicate, synthesises events, dispatches
/// batches through the driver. Returns when every watched
/// scope has been hydrated.
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

    // ----- address scans (current-state hydration via
    // `utxos_by_address`).
    let addresses: Vec<String> = interest.watched_addresses().map(|s| s.to_owned()).collect();
    stats.addresses_seen = addresses.len();
    for address in addresses {
        let key = bootstrap_flag_key_for_address(&address);
        if has_flag(kv, module_id, &key)? {
            tracing::debug!(
                module = %module_id,
                address = %address,
                "bootstrap: skipping address; already complete",
            );
            continue;
        }
        tracing::info!(
            module = %module_id,
            address = %address,
            "bootstrap: scanning current unspent set at address",
        );
        let scanned = scan_one_address(driver, &address, plane).await?;
        stats.addresses_scanned += 1;
        stats.utxos_dispatched += scanned.utxos;
        stats.batches_dispatched += scanned.batches;
        set_flag(kv, module_id, &key)?;
        tracing::info!(
            module = %module_id,
            address = %address,
            utxos = scanned.utxos,
            batches = scanned.batches,
            "bootstrap: address complete",
        );
    }

    // ----- payment-cred scans (current-state hydration via
    // `utxos_by_payment_cred`). Used by contract sweeps where
    // the payment part is fixed (script hash) but staking part
    // varies per UTxO — e.g. CrowdLock vesting.
    let payment_creds: Vec<[u8; 28]> = interest.watched_payment_creds().copied().collect();
    stats.payment_creds_seen = payment_creds.len();
    for cred in payment_creds {
        let key = bootstrap_flag_key_for_payment_cred(&cred);
        if has_flag(kv, module_id, &key)? {
            tracing::debug!(
                module = %module_id,
                payment_cred = %hex::encode(cred),
                "bootstrap: skipping payment cred; already complete",
            );
            continue;
        }
        tracing::info!(
            module = %module_id,
            payment_cred = %hex::encode(cred),
            "bootstrap: scanning current unspent set at payment cred",
        );
        let scanned = scan_one_payment_cred(driver, &cred, plane).await?;
        stats.payment_creds_scanned += 1;
        stats.utxos_dispatched += scanned.utxos;
        stats.batches_dispatched += scanned.batches;
        set_flag(kv, module_id, &key)?;
        tracing::info!(
            module = %module_id,
            payment_cred = %hex::encode(cred),
            utxos = scanned.utxos,
            batches = scanned.batches,
            "bootstrap: payment cred complete",
        );
    }

    // ----- policy scans (current-state hydration via
    // `search_utxos(holds_policy)`). Same idempotence pattern
    // as the address path: a per-policy state-kv flag records
    // completion. An operator who wants to re-hydrate clears
    // the flag and the orchestrator re-runs.
    let policies: Vec<PolicyId> = interest.watched_policies().cloned().collect();
    stats.policies_seen = policies.len();
    for policy in policies {
        let key = bootstrap_flag_key_for_policy(&policy);
        if has_flag(kv, module_id, &key)? {
            tracing::debug!(
                module = %module_id,
                policy = %policy,
                "bootstrap: skipping policy; already complete",
            );
            continue;
        }
        tracing::info!(
            module = %module_id,
            policy = %policy,
            "bootstrap: scanning current unspent set under policy",
        );
        let scanned = scan_one_policy(driver, &policy, plane).await?;
        stats.policies_scanned += 1;
        stats.utxos_dispatched += scanned.utxos;
        stats.batches_dispatched += scanned.batches;
        set_flag(kv, module_id, &key)?;
        tracing::info!(
            module = %module_id,
            policy = %policy,
            utxos = scanned.utxos,
            batches = scanned.batches,
            "bootstrap: policy complete",
        );
    }

    Ok(stats)
}

/// Run bootstrap for a single newly-added predicate (address or
/// policy). Used by the dynamic-interest path: when a companion
/// adds a new policy at runtime via `update-interest(Add, ...)`,
/// the follower calls this to hydrate current state for just
/// that scope rather than re-scanning everything in the set.
///
/// Idempotent — the per-scope state-kv flag prevents repeated
/// scans on subsequent dynamic adds of the same predicate.
pub async fn bootstrap_one_predicate<P: ChainDataPlane + Sync>(
    driver: &mut DriverV2,
    module_id: &str,
    kv: &mut ModuleKv,
    predicate: &InterestPredicate,
    plane: &P,
) -> anyhow::Result<BootstrapStats> {
    let mut stats = BootstrapStats::default();
    match predicate {
        InterestPredicate::AtAddress(address) => {
            stats.addresses_seen = 1;
            let key = bootstrap_flag_key_for_address(address);
            if has_flag(kv, module_id, &key)? {
                return Ok(stats);
            }
            let scanned = scan_one_address(driver, address, plane).await?;
            stats.addresses_scanned = 1;
            stats.utxos_dispatched = scanned.utxos;
            stats.batches_dispatched = scanned.batches;
            set_flag(kv, module_id, &key)?;
        }
        InterestPredicate::HoldsPolicy(policy) => {
            stats.policies_seen = 1;
            let key = bootstrap_flag_key_for_policy(policy);
            if has_flag(kv, module_id, &key)? {
                return Ok(stats);
            }
            let scanned = scan_one_policy(driver, policy, plane).await?;
            stats.policies_scanned = 1;
            stats.utxos_dispatched = scanned.utxos;
            stats.batches_dispatched = scanned.batches;
            set_flag(kv, module_id, &key)?;
        }
        InterestPredicate::AtPaymentCred(cred) => {
            stats.payment_creds_seen = 1;
            let key = bootstrap_flag_key_for_payment_cred(cred);
            if has_flag(kv, module_id, &key)? {
                return Ok(stats);
            }
            let scanned = scan_one_payment_cred(driver, cred, plane).await?;
            stats.payment_creds_scanned = 1;
            stats.utxos_dispatched = scanned.utxos;
            stats.batches_dispatched = scanned.batches;
            set_flag(kv, module_id, &key)?;
        }
        // `AtStakeCred` would need a stake-cred → addresses
        // resolution at the data plane (not implemented Phase A).
        // `HoldsAsset` is single-asset specific; bootstrap-by-
        // asset is uncommon in practice (most callers either
        // care about a whole policy or already know the UTxO).
        // `TickEvery` has no current-state to hydrate.
        InterestPredicate::AtStakeCred(_)
        | InterestPredicate::HoldsAsset { .. }
        | InterestPredicate::TickEvery(_) => {
            tracing::debug!(
                module = %module_id,
                predicate = ?predicate,
                "bootstrap: predicate has no current-state hydration path; skipped",
            );
        }
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
    let mut datum_by_ref: std::collections::HashMap<(Hash<32>, u32), mitos_data_plane::TypedDatum> =
        std::collections::HashMap::new();
    for (oref, datum_opt) in refs.iter().zip(datums) {
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

/// Scan one payment credential: enumerate refs via
/// `utxos_by_payment_cred`, bulk-resolve outputs + datums,
/// group by producing TX, dispatch one batch per producing-TX
/// through the driver. Mirrors `scan_one_address` but works
/// against the data plane's payment-cred index.
async fn scan_one_payment_cred<P: ChainDataPlane + Sync>(
    driver: &mut DriverV2,
    cred: &[u8; 28],
    plane: &P,
) -> anyhow::Result<AddressScanResult> {
    let refs = plane
        .utxos_by_payment_cred(cred)
        .await
        .map_err(|e| anyhow::anyhow!("utxos_by_payment_cred({}): {e}", hex::encode(cred)))?;
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

    let mut output_by_ref: std::collections::HashMap<(Hash<32>, u32), TypedOutput> =
        std::collections::HashMap::new();
    for (oref, out) in outputs {
        output_by_ref.insert((oref.tx_hash, oref.index), out);
    }
    let mut datum_by_ref: std::collections::HashMap<(Hash<32>, u32), mitos_data_plane::TypedDatum> =
        std::collections::HashMap::new();
    for (oref, datum_opt) in refs.iter().zip(datums) {
        if let Some(td) = datum_opt {
            datum_by_ref.insert((oref.tx_hash, oref.index), td);
        }
    }

    let mut by_tx: BTreeMap<Hash<32>, Vec<OutputRef>> = BTreeMap::new();
    for r in refs {
        by_tx.entry(r.tx_hash).or_default().push(r);
    }

    let mut result = AddressScanResult::default();
    for (tx_hash, group_refs) in by_tx {
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
                continue;
            };
            let datum = datum_by_ref.remove(&key);
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

/// Scan one policy: page through `search_utxos(holds_policy)`,
/// group by producing-tx, dispatch one synthetic batch per tx
/// through the driver. Mirrors `scan_one_address` but works
/// against the data plane's policy index instead of the
/// address index.
async fn scan_one_policy<P: ChainDataPlane + Sync>(
    driver: &mut DriverV2,
    policy: &PolicyId,
    plane: &P,
) -> anyhow::Result<AddressScanResult> {
    let predicate = UtxoPredicate::matching(UtxoPattern {
        asset: Some(AssetPattern::Policy(policy.clone())),
        ..Default::default()
    });

    // Page through the entire matching set. The data plane's
    // current cap is per-page (Phase A: 1000); keep walking
    // until `next_token` is `None`.
    let mut all_outputs: Vec<(OutputRef, TypedOutput)> = Vec::new();
    let mut page_token: Option<String> = None;
    loop {
        let page = plane
            .search_utxos(
                &predicate,
                DecodeLevel::Lean,
                PageRequest {
                    start_token: page_token.clone(),
                    max_items: 1000,
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("search_utxos(holds_policy({policy})): {e}"))?;
        all_outputs.extend(page.items);
        match page.next_token {
            Some(t) => page_token = Some(t),
            None => break,
        }
    }

    if all_outputs.is_empty() {
        return Ok(AddressScanResult::default());
    }

    // Bulk-resolve datums for everything we got back. Mirrors
    // the address-side flow.
    let refs: Vec<OutputRef> = all_outputs.iter().map(|(r, _)| *r).collect();
    let datums = plane
        .read_output_datums(&refs)
        .await
        .map_err(|e| anyhow::anyhow!("read_output_datums: {e}"))?;

    let mut output_by_ref: std::collections::HashMap<(Hash<32>, u32), TypedOutput> =
        std::collections::HashMap::new();
    for (oref, out) in all_outputs {
        output_by_ref.insert((oref.tx_hash, oref.index), out);
    }
    let mut datum_by_ref: std::collections::HashMap<(Hash<32>, u32), mitos_data_plane::TypedDatum> =
        std::collections::HashMap::new();
    for (oref, datum_opt) in refs.iter().zip(datums) {
        if let Some(td) = datum_opt {
            datum_by_ref.insert((oref.tx_hash, oref.index), td);
        }
    }

    // Group refs by producing tx_hash. BTreeMap → deterministic
    // dispatch order across runs.
    let mut by_tx: BTreeMap<Hash<32>, Vec<OutputRef>> = BTreeMap::new();
    for r in refs {
        by_tx.entry(r.tx_hash).or_default().push(r);
    }

    let mut result = AddressScanResult::default();
    for (tx_hash, group_refs) in by_tx {
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
                continue;
            };
            let datum = datum_by_ref.remove(&key);
            let _ = datum_from_draft;

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

/// Reserved prefix every bootstrap flag (address-scoped + policy-scoped)
/// is filed under. Centralised here so the recapture path can clear
/// the full set with one call rather than enumerating from the
/// interest set. See `docs/design/RECAPTURE.md`.
const BOOTSTRAP_FLAG_PREFIX: &str = "__platform/bootstrap/";

/// Clear every bootstrap-done flag for this module. Used by the
/// recapture flow to force a re-walk of the manifest's
/// `[interest]` addresses + policies on the next bootstrap pass.
///
/// Returns the number of flags removed.
///
/// Implementation note: rather than enumerate the current interest
/// set and delete each key individually, we range-scan under
/// `__platform/bootstrap/` and delete everything we find. That makes
/// the call robust against interest-set drift between runs (e.g. an
/// address that was once watched, isn't anymore, but still has a
/// stale flag).
pub fn clear_bootstrap_flags(kv: &mut ModuleKv, module_id: &str) -> anyhow::Result<usize> {
    match kv {
        ModuleKv::InMemory(map) => {
            let to_remove: Vec<String> = map
                .keys()
                .filter(|k| k.starts_with(BOOTSTRAP_FLAG_PREFIX))
                .cloned()
                .collect();
            let n = to_remove.len();
            for key in to_remove {
                map.remove(&key);
            }
            Ok(n)
        }
        ModuleKv::Redb(redb) => {
            // `list_values` returns full module-scoped keys
            // (`<module_id>-<key>`); strip the prefix for the
            // `delete_value` round-trip which re-applies it.
            let full_keys = redb
                .list_values(module_id, BOOTSTRAP_FLAG_PREFIX)
                .map_err(|e| anyhow::anyhow!("kv.list: {e}"))?;
            let mod_prefix = format!("{module_id}-");
            let mut n = 0usize;
            for full in full_keys {
                let key = full.strip_prefix(&mod_prefix).unwrap_or(&full);
                redb.delete_value(module_id, key)
                    .map_err(|e| anyhow::anyhow!("kv.delete {key}: {e}"))?;
                n += 1;
            }
            Ok(n)
        }
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

/// Build an `InterestSet` from manifest-declared addresses +
/// policies. Both predicate kinds participate in the bootstrap
/// scan: addresses hydrate via `utxos_by_address` and policies
/// hydrate via `search_utxos(holds_policy)`. Idempotent — the
/// per-scope state-kv flag prevents repeated scans across
/// restarts.
pub fn interest_from_manifest(addresses: &[String], policy_hexes: &[String]) -> InterestSet {
    let mut set = interest_from_addresses(addresses);
    for hex in policy_hexes {
        match cardano_assets::PolicyId::new(hex.clone()) {
            Ok(p) => set.add(InterestPredicate::HoldsPolicy(p)),
            Err(e) => {
                tracing::warn!(
                    policy = %hex,
                    error = %e,
                    "interest_from_manifest: skipping invalid policy id",
                );
            }
        }
    }
    set
}
