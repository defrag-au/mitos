//! Asset-transfer community module — surfaces net asset
//! movements between addresses as `Transfer` events.
//!
//! ## Algorithm
//!
//! Per dispatched TX:
//!
//! 1. Walk `Consumed` events. For each asset on the prior
//!    output: `deltas[(address, policy, name)] -= quantity`.
//! 2. Walk `Produced` events. For each asset on the new
//!    output: `deltas[(address, policy, name)] += quantity`.
//! 3. Drop zero-delta entries (change outputs — same address
//!    appears on both sides with offsetting deltas net to zero).
//! 4. Group remaining entries by `(policy, asset_name)`.
//! 5. For each asset group, partition into senders (delta < 0)
//!    and receivers (delta > 0). If either side is empty the
//!    movement is a mint or burn — skip (handled by other
//!    modules).
//! 6. Pick the primary sender as the address with the largest
//!    `|delta|`. Ties broken by address lex sort — deterministic
//!    and correct-by-construction for NFTs (qty = 1, supply = 1
//!    ⇒ exactly one negative delta).
//! 7. Emit one `Transfer` per receiver, attributed to the
//!    primary sender.
//!
//! `referenced` events are ignored — CIP-31 read-only inputs
//! don't move asset ownership.
//!
//! ## Interest
//!
//! Module ships with empty static interest. Filter scope is
//! consumer-driven: the companion calls
//! `/api/_interest/asset-transfer/subscribe` with
//! `kind = "policy"` for each policy it cares about. The
//! platform's dispatch composer filters Produced/Consumed
//! events host-side; this module trusts that everything it
//! sees is in-scope.
//!
//! Per-policy is the only supported predicate today.
//! Per-asset-name targeting is intentionally absent — too
//! granular for the surface, and consumers can post-filter
//! cheaply by inspecting `asset_name_hex` on emitted events.

use std::collections::BTreeMap;

use mitos_community_events::asset_transfer::{AssetTransfer, Transfer};

use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::types::{ChainPoint, ConsumedEvent, ProducedEvent, UtxoEvent};

const LOG_TARGET: &str = "asset-transfer-module";

/// Key for the per-TX net-delta map.
///
/// Stored as `(address, policy_bytes, asset_name_bytes)`. Hex
/// encoding is deferred to emission time so the comparison +
/// hashing stay byte-equal regardless of upstream casing.
type DeltaKey = (String, Vec<u8>, Vec<u8>);

#[derive(Default)]
struct TxBuffer {
    deltas: BTreeMap<DeltaKey, i64>,
    tx_hash: Option<Vec<u8>>,
    slot: u64,
}

fn cursor_slot(cursor: &ChainPoint) -> u64 {
    match cursor {
        ChainPoint::Origin => 0,
        ChainPoint::SlotOnly(s) => *s,
        ChainPoint::Specific(p) => p.slot,
    }
}

fn handle_consumed(c: &ConsumedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(c.consuming_tx_hash.clone());
        buf.slot = cursor_slot(&c.cursor);
    }
    for asset in &c.prior_output.assets {
        let key = (
            c.prior_output.address.clone(),
            asset.asset.policy.clone(),
            asset.asset.name.clone(),
        );
        *buf.deltas.entry(key).or_insert(0) -= asset.quantity as i64;
    }
}

fn handle_produced(p: &ProducedEvent, buf: &mut TxBuffer) {
    if buf.tx_hash.is_none() {
        buf.tx_hash = Some(p.tx_hash.clone());
        buf.slot = cursor_slot(&p.cursor);
    }
    for asset in &p.output.assets {
        let key = (
            p.output.address.clone(),
            asset.asset.policy.clone(),
            asset.asset.name.clone(),
        );
        *buf.deltas.entry(key).or_insert(0) += asset.quantity as i64;
    }
}

fn flush_buffer(buf: TxBuffer) {
    let Some(tx_hash) = buf.tx_hash else {
        return;
    };
    let tx_hash_hex = hex::encode(&tx_hash);
    let slot = buf.slot;

    // Group non-zero entries by (policy, asset_name).
    let mut by_asset: BTreeMap<(Vec<u8>, Vec<u8>), Vec<(String, i64)>> = BTreeMap::new();
    for ((address, policy, name), delta) in buf.deltas {
        if delta == 0 {
            continue; // change-output residual
        }
        by_asset
            .entry((policy, name))
            .or_default()
            .push((address, delta));
    }

    for ((policy, name), entries) in by_asset {
        // Partition into senders (negative) + receivers (positive).
        let mut senders: Vec<(String, i64)> = entries.iter().filter(|(_, d)| *d < 0).cloned().collect();
        let receivers: Vec<(String, u64)> = entries
            .iter()
            .filter(|(_, d)| *d > 0)
            .map(|(a, d)| (a.clone(), *d as u64))
            .collect();

        // Mint or burn — handled by dedicated modules.
        if senders.is_empty() || receivers.is_empty() {
            continue;
        }

        // Primary sender = largest |delta|, ties broken by
        // address lex sort. For NFTs there's exactly one
        // negative delta so this is by-construction
        // unambiguous; for FTs it's a deterministic heuristic.
        senders.sort_by(|a, b| b.1.abs().cmp(&a.1.abs()).then_with(|| a.0.cmp(&b.0)));
        let primary_sender = senders[0].0.clone();

        let policy_hex = hex::encode(&policy);
        let name_hex = hex::encode(&name);
        for (recv_addr, qty) in receivers {
            emit_transfer(&AssetTransfer::Transfer(Transfer {
                policy: policy_hex.clone(),
                asset_name_hex: name_hex.clone(),
                quantity: qty,
                from_address: primary_sender.clone(),
                to_address: recv_addr,
                tx_hash: tx_hash_hex.clone(),
                slot,
            }));
        }
    }
}

fn emit_transfer(event: &AssetTransfer) {
    let mut buf = Vec::with_capacity(256);
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode transfer event failed: {e}"),
        );
        return;
    }
    emit::emit_event(0, &buf);
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
        // Idempotent — emissions are pure functions of the TX's
        // dispatched events. Replay-safe.
        (
            TrapStrategy::Replay,
            RetryPolicy {
                max_retries: 3,
                backoff_cap_ms: 1_000,
            },
        )
    }

    fn init(config: Vec<u8>) {
        logging::log(
            LogLevel::Info,
            LOG_TARGET,
            &format!("init: enter (config_bytes={})", config.len()),
        );
        // No persistent config — interest is platform-managed
        // via the consumer companion's subscribe call.
    }

    fn handle_events(events: Vec<DispatchEvent>) {
        let mut buf = TxBuffer::default();
        for event in events {
            match event {
                DispatchEvent::Utxo(UtxoEvent::Consumed(c)) => handle_consumed(&c, &mut buf),
                DispatchEvent::Utxo(UtxoEvent::Produced(p)) => handle_produced(&p, &mut buf),
                // Referenced inputs (CIP-31 read-only) don't
                // move asset ownership — ignored.
                // Minted events are someone else's concern.
                // TxContext carries no asset deltas.
                _ => {}
            }
        }
        flush_buffer(buf);
    }

    fn update_interest(_op: InterestOp, _items_cbor: Vec<u8>) -> Result<(), String> {
        // Interest filter is host-managed; the module is a
        // pure shape-transform over delivered events and
        // doesn't key any internal state by the interest set.
        Ok(())
    }

    /// No-op: event-driven modules are refilled host-side by
    /// `run_bootstrap` over the manifest `[interest]`. See the
    /// `rebootstrap` export in `wit-v2/world.wit`.
    fn rebootstrap() -> Result<u64, String> {
        Ok(0)
    }
}

export!(Module);
