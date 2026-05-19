//! Vesting-tracker community module — watches Shield Vest /
//! CrowdLock contract addresses + payment credentials and emits
//! lock/unlock events as UTxOs at them appear and get spent.
//!
//! ## Two interest predicates
//!
//! - `AtAddress(bech32)` — project-vest contracts at a single
//!   known address. Cold-start uses `utxos_by_address`.
//! - `AtPaymentCred(28b_hex)` — CrowdLock-style sweeps where the
//!   payment part is fixed (script hash) but the staking part
//!   varies per UTxO. Cold-start uses `utxos_by_payment_cred`.
//!
//! Both register through `update_interest`; both feed the same
//! datum decoder + emission path. The wire shape carries
//! `interest_kind` so consumers know which scope a snapshot
//! replaces.
//!
//! ## Shield datum
//!
//! Both Shield project vests and CrowdLock user vests use:
//!
//! ```text
//! Constructor 0 [
//!   Int(unlock_ts_ms),
//!   List[ Bytes(owner_payment_key_hash_28b) ]
//! ]
//! ```
//!
//! The chain doesn't carry a VestStyle discriminator; the module
//! derives it from the locking TX's metadata key 674 message
//! ("Shield Vest - Crowd Lock" → CrowdLock, "Shield Vest" →
//! Shield, anything else → Unknown).
//!
//! ## Owner stake-cred resolution
//!
//! The datum carries the owner's *payment* PKH. We resolve it
//! to a stake credential via the chain-data
//! `resolve_stake_for_payment_pkh` host-fn (one redb scan over
//! the by-payment-cred index). `None` for enterprise-only
//! owners; consumers should keep the lock visible.
//!
//! ## Emission shape
//!
//! - `Snapshot` on registration / rollback — full set of current
//!   locks under one interest scope.
//! - `Locked` per `(produced UTxO, policy, asset_name)` matching
//!   a tracked address/cred at handle-events time.
//! - `Unlocked` per `consumed UTxO` matching a tracked
//!   address/cred. Identifies by `lock_ref` only — the consumer
//!   deletes the row keyed on the UTxO ref regardless of
//!   whether we'd previously surfaced it.

use std::cell::RefCell;
use std::collections::HashSet;

use mitos_community_events::vesting_tracker::{
    InterestKind, LockEntry, LockRef, VestStyle, VestingEvent, VestingLock, VestingSnapshot,
    VestingUnlock,
};
use pallas_addresses::{Address, ShelleyPaymentPart};
use pallas_codec::minicbor::data::Type as CborType;
use pallas_primitives::PlutusData;
use serde::{Deserialize, Serialize};

use crate::mitos::platform_v2::chain_data;
use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
use crate::mitos::platform_v2::state_kv;
use crate::mitos::platform_v2::types::{
    AssetEntry as WitAssetEntry, ChainPoint, ConsumedEvent, OutputRef as WitOutputRef,
    ProducedEvent, StakeCred as WitStakeCred, TypedDatum, UtxoEvent,
};

const LOG_TARGET: &str = "vesting-tracker-module";

const HASH_BYTES: usize = 28;

/// state-kv key under which we persist the set of currently-
/// tracked interest predicates (CBOR
/// `(Vec<String>, Vec<[u8; 28]>)`). The host-side bootstrap flag
/// is separate; this is the module's view of which scopes the
/// companion has subscribed it to.
const KV_TRACKED_INTERESTS: &str = "tracked-interests";

/// Read-cap matching the host. Reaching this means the lock
/// scope has too many active UTxOs to fit in one shot; the
/// emitted snapshot is suppressed and live deltas apply against
/// an empty consumer state.
const COLD_START_CAP: usize = 100_000;

thread_local! {
    /// Watched lock-contract addresses (bech32). Mirrors the
    /// host-side interest set; persisted via state-kv so a host
    /// restart without an attached companion keeps the module
    /// pre-armed.
    static TRACKED_ADDRESSES: RefCell<HashSet<String>> = RefCell::new(HashSet::new());

    /// Watched payment credentials (28-byte hash). Same purpose
    /// as `TRACKED_ADDRESSES` for the CrowdLock-style sweep.
    static TRACKED_PAYMENT_CREDS: RefCell<HashSet<[u8; HASH_BYTES]>> = RefCell::new(HashSet::new());
}

// ============================================================
// Wire-side interest predicate mirror
// ============================================================

/// Local mirror of the on-wire `InterestPredicate` enum — same
/// pattern as holder-distribution. We capture `AtAddress` +
/// `AtPaymentCred` and step over the others.
#[derive(Debug, Deserialize)]
enum InterestPredicateWire {
    AtAddress(String),
    /// CBOR array of u8 (serde default for `[u8; 28]`); we
    /// length-validate to 28 at use.
    AtPaymentCred(Vec<u8>),
    AtStakeCred(serde::de::IgnoredAny),
    HoldsPolicy(serde::de::IgnoredAny),
    HoldsAsset {
        #[allow(dead_code)]
        policy: serde::de::IgnoredAny,
        #[allow(dead_code)]
        asset_name: serde::de::IgnoredAny,
    },
    TickEvery(#[allow(dead_code)] u32),
}

// ============================================================
// Persisted tracked-interests blob
// ============================================================

#[derive(Default, Serialize, Deserialize)]
struct PersistedInterests {
    addresses: Vec<String>,
    payment_creds: Vec<String>, // hex-encoded 28-byte
}

fn persist_tracked_interests() {
    let mut blob = PersistedInterests::default();
    TRACKED_ADDRESSES.with(|set| {
        blob.addresses = set.borrow().iter().cloned().collect();
    });
    TRACKED_PAYMENT_CREDS.with(|set| {
        blob.payment_creds = set.borrow().iter().map(hex::encode).collect();
    });
    let mut buf = Vec::with_capacity(256);
    if ciborium::ser::into_writer(&blob, &mut buf).is_ok() {
        state_kv::set_value(KV_TRACKED_INTERESTS, &buf);
    }
}

fn restore_tracked_interests() {
    let Some(bytes) = state_kv::get_value(KV_TRACKED_INTERESTS) else {
        return;
    };
    let blob: PersistedInterests = match ciborium::de::from_reader(bytes.as_slice()) {
        Ok(v) => v,
        Err(_) => return,
    };
    let mut addr_count = 0usize;
    TRACKED_ADDRESSES.with(|set| {
        let mut set = set.borrow_mut();
        set.clear();
        for a in blob.addresses {
            set.insert(a);
            addr_count += 1;
        }
    });
    let mut cred_count = 0usize;
    TRACKED_PAYMENT_CREDS.with(|set| {
        let mut set = set.borrow_mut();
        set.clear();
        for h in blob.payment_creds {
            if let Ok(bytes) = hex::decode(&h) {
                if bytes.len() == HASH_BYTES {
                    let mut arr = [0u8; HASH_BYTES];
                    arr.copy_from_slice(&bytes);
                    set.insert(arr);
                    cred_count += 1;
                }
            }
        }
    });
    logging::log(
        LogLevel::Info,
        LOG_TARGET,
        &format!("restored {addr_count} address(es) + {cred_count} payment-cred(s) from state-kv"),
    );
}

// ============================================================
// Datum decode — Shield / CrowdLock unified shape
// ============================================================

struct DecodedDatum {
    unlock_ts_ms: u64,
    owner_pkh_hex: String,
}

/// Decode a Shield datum:
/// `Constructor 0 [ Int(unlock_ts_ms), List[ Bytes(owner_pkh) ] ]`.
fn decode_shield_datum(cbor: &[u8]) -> Option<DecodedDatum> {
    let pd: PlutusData = pallas_codec::minicbor::decode(cbor).ok()?;
    let outer = match pd {
        PlutusData::Constr(c) => c,
        _ => return None,
    };
    // Field 0: unlock_ts_ms as positive int
    let fields: Vec<PlutusData> = outer.fields.into();
    if fields.len() < 2 {
        return None;
    }
    let unlock_ts_ms = match &fields[0] {
        PlutusData::BigInt(i) => bigint_to_u64(i)?,
        _ => return None,
    };
    // Field 1: list with one entry = owner PKH bytes
    let owner_pkh = match &fields[1] {
        PlutusData::Array(items) => match items.first()? {
            PlutusData::BoundedBytes(b) => {
                let raw: &[u8] = &**b;
                if raw.len() != HASH_BYTES {
                    return None;
                }
                hex::encode(raw)
            }
            _ => return None,
        },
        _ => return None,
    };
    Some(DecodedDatum {
        unlock_ts_ms,
        owner_pkh_hex: owner_pkh,
    })
}

fn bigint_to_u64(i: &pallas_primitives::BigInt) -> Option<u64> {
    match i {
        pallas_primitives::BigInt::Int(n) => {
            let v = i128::from(*n);
            if v < 0 {
                None
            } else {
                u64::try_from(v).ok()
            }
        }
        pallas_primitives::BigInt::BigUInt(b) => {
            let bytes: &[u8] = &**b;
            if bytes.len() > 8 {
                return None;
            }
            let mut buf = [0u8; 8];
            buf[8 - bytes.len()..].copy_from_slice(bytes);
            Some(u64::from_be_bytes(buf))
        }
        pallas_primitives::BigInt::BigNInt(_) => None,
    }
}

/// Resolve a `TypedDatum` to its raw CBOR bytes. Inline datums
/// carry `payload` directly; hash-only datums fall back to
/// `chain_data::datum_by_hash`.
fn resolve_datum_bytes(d: Option<&TypedDatum>) -> Option<Vec<u8>> {
    let d = d?;
    if !d.payload.is_empty() {
        return Some(d.payload.clone());
    }
    chain_data::datum_by_hash(&d.hash)
}

// ============================================================
// VestStyle from TX metadata 674
// ============================================================

/// Look up metadata key 674's `msg` array and classify the
/// vest style. Returns `VestStyle::Unknown` when:
/// - the TX has no aux data
/// - aux data has no label 674
/// - the value at 674 isn't `{ "msg": [<string>...] }`
/// - the msg strings don't match either prefix
fn vest_style_from_tx(tx_hash: &[u8]) -> VestStyle {
    let Some(aux) = chain_data::tx_metadata(&tx_hash.to_vec()) else {
        return VestStyle::Unknown;
    };
    classify_vest_style_msg(&aux).unwrap_or(VestStyle::Unknown)
}

/// Inner: walks aux-data CBOR (handling both the Alonzo+ tagged
/// map form and the pre-Alonzo array form) to find key 674's
/// `msg` array, then matches the first string. Returns `None`
/// on any structural mismatch.
fn classify_vest_style_msg(aux: &[u8]) -> Option<VestStyle> {
    let mut d = pallas_codec::minicbor::Decoder::new(aux);

    // Optional outer tag (Alonzo+ `#6.259`); strip it.
    let was_tagged = matches!(d.datatype().ok()?, CborType::Tag);
    if was_tagged {
        d.tag().ok()?;
    }

    // Two top-level shapes: tagged → `{ 0 => metadata_map, 1 => ... }`,
    // pre-Alonzo → `[metadata_map, native_scripts]`, or bare
    // metadata map.
    let dt = d.datatype().ok()?;
    if dt == CborType::Array || dt == CborType::ArrayIndef {
        let _arr = d.array().ok()?;
        if d.datatype().ok()? == CborType::Null {
            return None;
        }
        // cursor now on metadata_map
    } else if was_tagged {
        // Outer map keyed by ints; walk to key 0.
        let len = d.map().ok()?;
        let mut i = 0u64;
        let mut found = false;
        loop {
            if let Some(n) = len {
                if i >= n {
                    break;
                }
            }
            if len.is_none() && d.datatype().ok()? == CborType::Break {
                d.skip().ok()?;
                break;
            }
            let key: u64 = d.u64().ok()?;
            if key == 0 {
                found = true;
                break;
            }
            d.skip().ok()?;
            i += 1;
        }
        if !found {
            return None;
        }
    }
    // Else bare-map shape; cursor already on metadata_map.

    // Walk metadata_map to find label 674.
    let len = d.map().ok()?;
    let mut i = 0u64;
    let mut found_674 = false;
    loop {
        if let Some(n) = len {
            if i >= n {
                break;
            }
        }
        if len.is_none() && d.datatype().ok()? == CborType::Break {
            d.skip().ok()?;
            break;
        }
        let key: u64 = d.u64().ok()?;
        if key == 674 {
            found_674 = true;
            break;
        }
        d.skip().ok()?;
        i += 1;
    }
    if !found_674 {
        return None;
    }

    // Value at 674: `{ "msg": [<string>...] }`. Walk to find
    // the `msg` key, then iterate the array's first string.
    let value_len = d.map().ok()?;
    let mut i = 0u64;
    let mut msg_str: Option<String> = None;
    loop {
        if let Some(n) = value_len {
            if i >= n {
                break;
            }
        }
        if value_len.is_none() && d.datatype().ok()? == CborType::Break {
            d.skip().ok()?;
            break;
        }
        // CIP-25/674 keys are strings, but accept bytes too as a
        // belt-and-braces measure.
        let key_match = match d.datatype().ok()? {
            CborType::String => d.str().ok()? == "msg",
            CborType::Bytes => d.bytes().ok()? == b"msg",
            _ => {
                d.skip().ok()?;
                d.skip().ok()?;
                i += 1;
                continue;
            }
        };
        if key_match {
            // Value should be a list of strings; concatenate
            // them and look for "Crowd Lock" / "Shield".
            if matches!(d.datatype().ok()?, CborType::Array | CborType::ArrayIndef) {
                let arr_len = d.array().ok()?;
                let mut joined = String::new();
                let mut j = 0u64;
                loop {
                    if let Some(n) = arr_len {
                        if j >= n {
                            break;
                        }
                    }
                    if arr_len.is_none() && d.datatype().ok()? == CborType::Break {
                        d.skip().ok()?;
                        break;
                    }
                    match d.datatype().ok()? {
                        CborType::String => {
                            let s = d.str().ok()?;
                            joined.push_str(s);
                            joined.push(' ');
                        }
                        _ => {
                            d.skip().ok()?;
                        }
                    }
                    j += 1;
                }
                msg_str = Some(joined);
            } else if matches!(d.datatype().ok()?, CborType::String) {
                msg_str = Some(d.str().ok()?.to_owned());
            }
            break;
        }
        d.skip().ok()?;
        i += 1;
    }
    let msg = msg_str?;
    if msg.contains("Crowd Lock") {
        Some(VestStyle::CrowdLock)
    } else if msg.contains("Shield") {
        Some(VestStyle::Shield)
    } else {
        None
    }
}

// ============================================================
// Address → payment-cred extraction (for AtPaymentCred matching)
// ============================================================

/// Return the 28-byte payment credential of a bech32 Cardano
/// address, regardless of key vs script.
fn payment_cred_bytes(address: &str) -> Option<[u8; HASH_BYTES]> {
    let addr = Address::from_bech32(address).ok()?;
    let shelley = match addr {
        Address::Shelley(s) => s,
        _ => return None,
    };
    match shelley.payment() {
        ShelleyPaymentPart::Key(h) => Some((**h).into()),
        ShelleyPaymentPart::Script(h) => Some((**h).into()),
    }
}

// ============================================================
// Match output against tracked interests
// ============================================================

/// Decide whether an output matches any tracked interest, and
/// which (interest_kind, interest_value) it matched. Returns the
/// FIRST matching scope — if the same physical address is
/// watched both directly (AtAddress) AND via its payment-cred
/// (AtPaymentCred), AtAddress wins (more specific).
fn match_interest(address: &str) -> Option<(InterestKind, String)> {
    let addr_match = TRACKED_ADDRESSES.with(|set| set.borrow().contains(address));
    if addr_match {
        return Some((InterestKind::Address, address.to_owned()));
    }
    let cred = payment_cred_bytes(address)?;
    let cred_match = TRACKED_PAYMENT_CREDS.with(|set| set.borrow().contains(&cred));
    if cred_match {
        return Some((InterestKind::PaymentCred, hex::encode(cred)));
    }
    None
}

// ============================================================
// Build LockEntry from a UTxO
// ============================================================

/// Decode a UTxO's datum + metadata into one `LockEntry` per
/// non-lovelace asset. Returns empty Vec when the datum can't be
/// decoded or the UTxO carries no non-lovelace assets.
fn build_lock_entries(
    address: &str,
    oref_tx_hash: &[u8],
    oref_index: u32,
    assets: &[WitAssetEntry],
    datum: Option<&TypedDatum>,
    vest_style: VestStyle,
) -> Vec<LockEntry> {
    let Some(datum_bytes) = resolve_datum_bytes(datum) else {
        return Vec::new();
    };
    let Some(decoded) = decode_shield_datum(&datum_bytes) else {
        return Vec::new();
    };

    let owner_stake_cred_hex = resolve_owner_stake(&decoded.owner_pkh_hex);

    let tx_hash_hex = hex::encode(oref_tx_hash);
    let mut entries = Vec::new();
    for asset in assets {
        // Skip lovelace — assets list has only non-lovelace
        // entries already in the WIT shape, but defensive in
        // case of future shape drift.
        if asset.asset.policy.is_empty() {
            continue;
        }
        entries.push(LockEntry {
            utxo_ref: LockRef {
                tx_hash: tx_hash_hex.clone(),
                index: oref_index,
            },
            lock_address: address.to_owned(),
            policy: hex::encode(&asset.asset.policy),
            asset_name_hex: hex::encode(&asset.asset.name),
            amount: asset.quantity,
            owner_pkh: decoded.owner_pkh_hex.clone(),
            owner_stake_cred_hex: owner_stake_cred_hex.clone(),
            unlock_ts_ms: decoded.unlock_ts_ms,
            vest_style,
            locked_at_tx: tx_hash_hex.clone(),
        });
    }
    entries
}

/// Resolve a payment PKH → stake-cred hex via the host-fn.
/// Returns `None` for enterprise-only owners.
fn resolve_owner_stake(owner_pkh_hex: &str) -> Option<String> {
    let pkh = hex::decode(owner_pkh_hex).ok()?;
    let resolved = chain_data::resolve_stake_for_payment_pkh(&pkh)?;
    let bytes = match resolved {
        WitStakeCred::KeyHash(b) | WitStakeCred::ScriptHash(b) => b,
    };
    Some(hex::encode(bytes))
}

// ============================================================
// Cold-start
// ============================================================

/// Snapshot scan for a single newly-added address interest.
fn cold_start_address(address: &str) {
    let refs = chain_data::utxos_by_address(&address.to_string());
    if refs.len() >= COLD_START_CAP {
        logging::log(
            LogLevel::Warn,
            LOG_TARGET,
            &format!(
                "cold-start address={address}: hit COLD_START_CAP={COLD_START_CAP}; snapshot suppressed"
            ),
        );
        return;
    }
    let locks = build_snapshot_locks(&refs);
    emit_snapshot(InterestKind::Address, address.to_owned(), locks, refs.len());
}

/// Snapshot scan for a single newly-added payment-cred interest.
fn cold_start_payment_cred(cred: &[u8; HASH_BYTES]) {
    let refs = chain_data::utxos_by_payment_cred(&cred.to_vec());
    if refs.len() >= COLD_START_CAP {
        logging::log(
            LogLevel::Warn,
            LOG_TARGET,
            &format!(
                "cold-start payment_cred={}: hit COLD_START_CAP={COLD_START_CAP}; snapshot suppressed",
                hex::encode(cred)
            ),
        );
        return;
    }
    let locks = build_snapshot_locks(&refs);
    emit_snapshot(InterestKind::PaymentCred, hex::encode(cred), locks, refs.len());
}

/// Common cold-start body: bulk-resolve each ref's output +
/// datum, derive VestStyle from the producing TX's metadata,
/// build per-asset LockEntries.
fn build_snapshot_locks(refs: &[WitOutputRef]) -> Vec<LockEntry> {
    if refs.is_empty() {
        return Vec::new();
    }
    let mut all_locks = Vec::with_capacity(refs.len());
    for chunk in refs.chunks(1024) {
        let outputs = chain_data::read_utxos(&chunk.to_vec());
        let datums = chain_data::read_output_datums(&chunk.to_vec());
        for ((oref, out), datum) in chunk.iter().zip(outputs.iter()).zip(datums.iter()) {
            let style = vest_style_from_tx(&oref.tx_hash);
            let entries = build_lock_entries(
                &out.address,
                &oref.tx_hash,
                oref.index,
                &out.assets,
                datum.as_ref(),
                style,
            );
            all_locks.extend(entries);
        }
    }
    all_locks
}

fn emit_snapshot(
    interest_kind: InterestKind,
    interest_value: String,
    mut locks: Vec<LockEntry>,
    refs_scanned: usize,
) {
    // Deterministic ordering for stable goldens.
    locks.sort_by(|a, b| {
        a.utxo_ref
            .tx_hash
            .cmp(&b.utxo_ref.tx_hash)
            .then_with(|| a.utxo_ref.index.cmp(&b.utxo_ref.index))
            .then_with(|| a.asset_name_hex.cmp(&b.asset_name_hex))
    });
    let snap = VestingSnapshot {
        interest_kind,
        interest_value: interest_value.clone(),
        cursor_slot: 0,
        cursor_hash_hex: String::new(),
        locks,
    };
    logging::log(
        LogLevel::Info,
        LOG_TARGET,
        &format!(
            "cold-start {kind:?}={interest_value}: {refs_scanned} UTxO(s) → {n} lock(s)",
            kind = snap.interest_kind,
            n = snap.locks.len()
        ),
    );
    emit_event(&VestingEvent::Snapshot(snap));
}

// ============================================================
// Live event handling
// ============================================================

fn handle_produced(p: &ProducedEvent) {
    let Some((interest_kind, interest_value)) = match_interest(&p.output.address) else {
        return;
    };
    let style = vest_style_from_tx(&p.tx_hash);
    let entries = build_lock_entries(
        &p.output.address,
        &p.tx_hash,
        p.oref.index,
        &p.output.assets,
        p.datum.as_ref(),
        style,
    );
    for lock in entries {
        emit_event(&VestingEvent::Locked(VestingLock {
            interest_kind,
            interest_value: interest_value.clone(),
            lock,
        }));
    }
}

fn handle_consumed(c: &ConsumedEvent) {
    let Some((interest_kind, interest_value)) = match_interest(&c.prior_output.address) else {
        return;
    };
    let slot = match &c.cursor {
        ChainPoint::Specific(sp) => sp.slot,
        ChainPoint::SlotOnly(s) => *s,
        ChainPoint::Origin => 0,
    };
    let lock_ref = LockRef {
        tx_hash: hex::encode(&c.oref.tx_hash),
        index: c.oref.index,
    };
    emit_event(&VestingEvent::Unlocked(VestingUnlock {
        interest_kind,
        interest_value,
        lock_ref,
        consuming_tx_hash: hex::encode(&c.consuming_tx_hash),
        slot,
    }));
}

// ============================================================
// Interest management
// ============================================================

fn apply_interest_update(op: InterestOp, items_cbor: &[u8]) {
    let predicates: Vec<InterestPredicateWire> = match ciborium::de::from_reader(items_cbor) {
        Ok(v) => v,
        Err(e) => {
            logging::log(
                LogLevel::Error,
                LOG_TARGET,
                &format!("decode interest predicates failed: {e}"),
            );
            return;
        }
    };

    let mut new_addresses: Vec<String> = Vec::new();
    let mut new_creds: Vec<[u8; HASH_BYTES]> = Vec::new();
    for pred in predicates {
        match pred {
            InterestPredicateWire::AtAddress(s) => new_addresses.push(s),
            InterestPredicateWire::AtPaymentCred(b) => {
                if b.len() == HASH_BYTES {
                    let mut arr = [0u8; HASH_BYTES];
                    arr.copy_from_slice(&b);
                    new_creds.push(arr);
                }
            }
            _ => {}
        }
    }

    let mut added_addresses: Vec<String> = Vec::new();
    let mut added_creds: Vec<[u8; HASH_BYTES]> = Vec::new();

    match op {
        InterestOp::Replace => {
            TRACKED_ADDRESSES.with(|set| {
                let mut set = set.borrow_mut();
                let prev = std::mem::take(&mut *set);
                for a in &new_addresses {
                    set.insert(a.clone());
                    if !prev.contains(a) {
                        added_addresses.push(a.clone());
                    }
                }
            });
            TRACKED_PAYMENT_CREDS.with(|set| {
                let mut set = set.borrow_mut();
                let prev = std::mem::take(&mut *set);
                for c in &new_creds {
                    set.insert(*c);
                    if !prev.contains(c) {
                        added_creds.push(*c);
                    }
                }
            });
        }
        InterestOp::Add => {
            TRACKED_ADDRESSES.with(|set| {
                let mut set = set.borrow_mut();
                for a in &new_addresses {
                    if set.insert(a.clone()) {
                        added_addresses.push(a.clone());
                    }
                }
            });
            TRACKED_PAYMENT_CREDS.with(|set| {
                let mut set = set.borrow_mut();
                for c in &new_creds {
                    if set.insert(*c) {
                        added_creds.push(*c);
                    }
                }
            });
        }
        InterestOp::Remove => {
            TRACKED_ADDRESSES.with(|set| {
                let mut set = set.borrow_mut();
                for a in &new_addresses {
                    set.remove(a);
                }
            });
            TRACKED_PAYMENT_CREDS.with(|set| {
                let mut set = set.borrow_mut();
                for c in &new_creds {
                    set.remove(c);
                }
            });
        }
    }

    persist_tracked_interests();

    for a in added_addresses {
        cold_start_address(&a);
    }
    for c in added_creds {
        cold_start_payment_cred(&c);
    }
}

// ============================================================
// Emit
// ============================================================

fn emit_event(event: &VestingEvent) {
    let mut buf = Vec::with_capacity(2048);
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Error,
            LOG_TARGET,
            &format!("encode VestingEvent failed: {e}"),
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
        restore_tracked_interests();
    }

    fn handle_events(events: Vec<DispatchEvent>) {
        let any_tracked = TRACKED_ADDRESSES.with(|s| !s.borrow().is_empty())
            || TRACKED_PAYMENT_CREDS.with(|s| !s.borrow().is_empty());
        if !any_tracked {
            return;
        }
        for event in events {
            match event {
                DispatchEvent::Utxo(UtxoEvent::Produced(p)) => handle_produced(&p),
                DispatchEvent::Utxo(UtxoEvent::Consumed(c)) => handle_consumed(&c),
                _ => {}
            }
        }
    }

    fn update_interest(op: InterestOp, items_cbor: Vec<u8>) -> Result<(), String> {
        apply_interest_update(op, &items_cbor);
        Ok(())
    }

    /// Re-emit lock snapshots for every tracked address + payment
    /// credential. `init` restores the tracked sets from
    /// `state-kv`, so the module already knows what it watches;
    /// the host's recapture flow calls this after `start()` to
    /// refill companions whose projected state was just dropped.
    /// Each `cold_start_*` re-scans the chain and emits a fresh
    /// `Snapshot` — idempotent.
    fn rebootstrap() -> Result<(), String> {
        let addresses: Vec<String> =
            TRACKED_ADDRESSES.with(|set| set.borrow().iter().cloned().collect());
        let creds: Vec<[u8; HASH_BYTES]> =
            TRACKED_PAYMENT_CREDS.with(|set| set.borrow().iter().copied().collect());
        logging::log(
            LogLevel::Info,
            LOG_TARGET,
            &format!(
                "rebootstrap: re-scanning {} address(es) + {} payment cred(s)",
                addresses.len(),
                creds.len()
            ),
        );
        for addr in &addresses {
            cold_start_address(addr);
        }
        for cred in &creds {
            cold_start_payment_cred(cred);
        }
        Ok(())
    }
}

export!(Module);
