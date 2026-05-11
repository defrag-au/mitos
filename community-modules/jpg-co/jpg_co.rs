//! jpg.store collection-offer indexer module — v2 platform.
//!
//! v2 model: the platform delivers a typed `handle-events`
//! stream filtered against this module's declared interest
//! (the V2 + V3 script addresses, registered via
//! `update-interest` from the companion DO). Modules see
//! `Produced` and `Consumed` events for matching outputs;
//! bootstrap (current-state hydration) is platform-managed
//! and arrives through the same `handle-events` channel as
//! synthesised events keyed by the original producing-TX
//! hash.
//!
//! What this module does:
//! - On `Produced(addr ∈ {V2, V3})` → emit `JpgCoChange::Created`.
//!   Datum bytes come on the event payload when the host could
//!   resolve (inline / witness-set); for jpg.store's
//!   metadata-encoded datums the payload is empty and we fall
//!   back to `chain_data::tx_metadata(tx_hash)` + hash-match.
//! - On `Consumed(prior_addr ∈ {V2, V3})` → emit
//!   `JpgCoChange::Spent` keyed by the prior `(tx_hash, idx)`.
//!   Cancel/accept TXs always carry the datum in the witness
//!   set, so the prior_datum payload is always populated.
//! - All other event variants (TxContext, Referenced, Minted,
//!   Tick, Rollback) are ignored — jpg-co is purely about
//!   produced/consumed for the watched script addresses.
//!
//! Architecture: see `cnft.dev-workers/docs/design/JPG_CO_MODULE.md`.
//! v2 platform docs: `mitos/docs/strategy/MITOS_PLATFORM_V2.md`.

use std::cell::RefCell;

use mitos_community_events::jpg_co::{JpgCoChange, JpgCoVersion};
use pallas_crypto::hash::Hasher;
use pallas_primitives::{Constr, PlutusData};
use serde::Deserialize;

use crate::mitos::platform_v2::chain_data;
use crate::mitos::platform_v2::emit;
use crate::mitos::platform_v2::logging::{self, LogLevel};
// `DispatchEvent`, `TrapStrategy`, `RetryPolicy`, `InterestOp`,
// and the `Guest` trait come from the world-level `use ...`
// clauses and are addressable at the top of this module's scope
// without explicit imports.
use crate::mitos::platform_v2::types::{ConsumedEvent, ProducedEvent, UtxoEvent};

const LOG_TARGET: &str = "jpg-co-module";

/// CBOR'd typed config from `jpg_co.toml`.
#[derive(Debug, Clone, Default, Deserialize)]
struct Config {
    v2_address: String,
    v3_address: String,
}

thread_local! {
    static V2_ADDRESS: RefCell<String> = const { RefCell::new(String::new()) };
    static V3_ADDRESS: RefCell<String> = const { RefCell::new(String::new()) };
}

fn watched_version(addr: &str) -> Option<JpgCoVersion> {
    if V2_ADDRESS.with(|a| a.borrow().as_str() == addr) {
        return Some(JpgCoVersion::V2);
    }
    if V3_ADDRESS.with(|a| a.borrow().as_str() == addr) {
        return Some(JpgCoVersion::V3);
    }
    None
}

/// Decoded fields of interest from a jpg.store CO datum.
#[derive(Debug)]
struct DecodedCoDatum {
    offerer_pkh: String,
    target_policy: Option<String>,
    /// Asset names (lowercase hex) the offer is constrained to.
    /// Empty for a collection-wide offer; non-empty when the
    /// payout's inner Map carries asset entries (e.g. an offer on
    /// a specific ADA Handle rather than the Handle collection).
    target_asset_names: Vec<String>,
}

/// Decode the CO datum's CBOR. Same shape as v1 — the wire
/// format hasn't changed, only the dispatch path that delivers
/// the bytes.
fn decode_co_datum(cbor: &[u8]) -> Option<DecodedCoDatum> {
    let pd: PlutusData = pallas_codec::minicbor::decode(cbor).ok()?;
    let constr = match pd {
        PlutusData::Constr(c) => c,
        _ => return None,
    };
    if !is_constructor_zero(&constr) {
        return None;
    }
    let fields: Vec<PlutusData> = constr.fields.into();
    if fields.len() != 2 {
        return None;
    }
    let offerer_pkh = match &fields[0] {
        PlutusData::BoundedBytes(b) if b.len() == 28 => hex::encode(&**b),
        _ => return None,
    };
    let (target_policy, target_asset_names) = match &fields[1] {
        PlutusData::Array(payouts) => payouts
            .last()
            .map(extract_target_policy_and_assets)
            .unwrap_or_default(),
        _ => Default::default(),
    };
    Some(DecodedCoDatum {
        offerer_pkh,
        target_policy,
        target_asset_names,
    })
}

fn is_constructor_zero(c: &Constr<PlutusData>) -> bool {
    c.tag == 121 && c.any_constructor.is_none()
}

/// Parse the NFT-payout entry's inner `Map { policy_id_bytes:
/// (1, Map { asset_name_bytes → unit }) }` shape.
///
/// Returns `(target_policy, target_asset_names)`:
/// - `target_policy` — first 28-byte key found at the outer Map
///   level, hex-encoded. `None` when no policy-shaped key exists.
/// - `target_asset_names` — hex-encoded asset names from the inner
///   Map's keys when the value is `Constr(1, Map { ... })`. Empty
///   when the inner Map is empty (collection-wide offer) or when
///   the value's structure doesn't match.
fn extract_target_policy_and_assets(payout: &PlutusData) -> (Option<String>, Vec<String>) {
    let constr = match payout {
        PlutusData::Constr(c) => c,
        _ => return Default::default(),
    };
    if !is_constructor_zero(constr) || constr.fields.len() != 2 {
        return Default::default();
    }
    let pairs = match &constr.fields[1] {
        PlutusData::Map(m) => m,
        _ => return Default::default(),
    };
    for (k, v) in pairs.iter() {
        let PlutusData::BoundedBytes(policy_bytes) = k else {
            continue;
        };
        if policy_bytes.len() != 28 {
            continue;
        }
        let policy_hex = hex::encode(&**policy_bytes);
        let asset_names = extract_asset_names(v);
        return (Some(policy_hex), asset_names);
    }
    Default::default()
}

/// The value side of the `policy → ...` entry. Expected shape:
/// `Constr(any_tag, [_, Map { asset_name_bytes → _ }])`. The tag
/// and first field's exact form aren't load-bearing for our
/// purposes; we only care about the Map's keys (asset names).
fn extract_asset_names(v: &PlutusData) -> Vec<String> {
    let PlutusData::Constr(constr) = v else {
        return Vec::new();
    };
    let inner_map = constr.fields.iter().find_map(|f| match f {
        PlutusData::Map(m) => Some(m),
        _ => None,
    });
    let Some(pairs) = inner_map else {
        return Vec::new();
    };
    pairs
        .iter()
        .filter_map(|(k, _)| match k {
            PlutusData::BoundedBytes(b) => Some(hex::encode(&**b)),
            _ => None,
        })
        .collect()
}

fn emit_co_change(event: &JpgCoChange) {
    let mut buf = Vec::new();
    if let Err(e) = ciborium::ser::into_writer(event, &mut buf) {
        logging::log(
            LogLevel::Warn,
            LOG_TARGET,
            &format!("emit serialize failed: {e}"),
        );
        return;
    }
    emit::emit_event(0, &buf);
}

/// Resolve datum bytes for an event. Host populates `payload`
/// when it could resolve (inline / witness-set); when empty,
/// we fall back to jpg.store's metadata-encoded-datum
/// convention via `chain_data::tx_metadata`.
fn resolve_datum_bytes(
    tx_hash: &[u8],
    datum_hash: &[u8],
    payload: &[u8],
) -> Option<Vec<u8>> {
    if !payload.is_empty() {
        return Some(payload.to_vec());
    }
    // Host couldn't resolve. Pull the producing TX's aux-data
    // and hash-match candidates.
    let aux = chain_data::tx_metadata(tx_hash)?;
    for candidate in parse_metadata_datums(&aux) {
        if candidate.len() % 2 != 0
            || !candidate.bytes().all(|b| b.is_ascii_hexdigit())
        {
            continue;
        }
        let bytes = hex::decode(&candidate).ok()?;
        let h = Hasher::<256>::hash(&bytes);
        if h.as_ref() == datum_hash {
            return Some(bytes);
        }
    }
    None
}

/// Walk metadata for jpg.store's labels-50+ chunked-hex
/// convention. Same shape as v1; only the call site moved
/// (event payload, not block-context resource).
fn parse_metadata_datums(aux_cbor: &[u8]) -> Vec<String> {
    let mut entries: Vec<(u64, String)> = Vec::new();
    if extract_metadata_entries(aux_cbor, &mut entries).is_err() {
        return Vec::new();
    }
    entries.sort_by_key(|(k, _)| *k);

    let mut datums = Vec::new();
    let mut current = String::new();
    for (label, val) in entries {
        if label < 50 {
            continue;
        }
        if val.contains("::") {
            continue;
        }
        if let Some((prefix, _)) = val.split_once(',') {
            if !prefix.is_empty() {
                current.push_str(prefix);
            }
            if !current.is_empty() {
                datums.push(std::mem::take(&mut current));
            }
        } else {
            current.push_str(&val);
        }
    }
    if !current.is_empty() {
        datums.push(current);
    }
    datums
}

fn extract_metadata_entries(
    aux_cbor: &[u8],
    out: &mut Vec<(u64, String)>,
) -> Result<(), pallas_codec::minicbor::decode::Error> {
    use pallas_codec::minicbor::data::Type;
    let mut d = pallas_codec::minicbor::Decoder::new(aux_cbor);

    if d.datatype()? == Type::Tag {
        let _tag = d.tag()?;
        let outer_len = d.map()?;
        let mut found = false;
        let mut i = 0u64;
        loop {
            if let Some(n) = outer_len
                && i >= n
            {
                break;
            }
            if outer_len.is_none() && d.datatype()? == Type::Break {
                d.skip()?;
                break;
            }
            let key: u64 = d.u64()?;
            if key == 0 {
                found = true;
                break;
            }
            d.skip()?;
            i += 1;
        }
        if !found {
            return Ok(());
        }
    }

    let map_len = d.map()?;
    let mut i = 0u64;
    loop {
        if let Some(n) = map_len
            && i >= n
        {
            break;
        }
        if map_len.is_none() && d.datatype()? == Type::Break {
            d.skip()?;
            break;
        }
        let label: u64 = d.u64()?;
        match d.datatype()? {
            Type::String => {
                let s: &str = d.str()?;
                out.push((label, s.to_owned()));
            }
            _ => {
                d.skip()?;
            }
        }
        i += 1;
    }
    Ok(())
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
        if config.is_empty() {
            return;
        }
        let cfg: Config = match ciborium::de::from_reader(config.as_slice()) {
            Ok(c) => c,
            Err(e) => {
                logging::log(
                    LogLevel::Error,
                    LOG_TARGET,
                    &format!("init: decode config failed: {e}"),
                );
                return;
            }
        };
        V2_ADDRESS.with(|a| *a.borrow_mut() = cfg.v2_address);
        V3_ADDRESS.with(|a| *a.borrow_mut() = cfg.v3_address);
        logging::log(LogLevel::Info, LOG_TARGET, "init: addresses stored");
    }

    fn handle_events(events: Vec<DispatchEvent>) {
        for event in events {
            match event {
                DispatchEvent::Utxo(UtxoEvent::Produced(p)) => handle_produced(&p),
                DispatchEvent::Utxo(UtxoEvent::Consumed(c)) => handle_consumed(&c),
                // Other variants ignored — jpg-co only cares
                // about produced/consumed at watched addresses.
                _ => {}
            }
        }
    }

    fn update_interest(_op: InterestOp, _items_cbor: Vec<u8>) -> Result<(), String> {
        // Companion declares interest via the fixture / runtime
        // config; the platform handles filter application.
        // V1 contract was an empty no-op too.
        Ok(())
    }
}

fn handle_produced(p: &ProducedEvent) {
    let Some(version) = watched_version(&p.output.address) else {
        return;
    };
    let Some(typed_datum) = p.datum.as_ref() else {
        return;
    };
    let datum_bytes =
        match resolve_datum_bytes(&p.tx_hash, &typed_datum.hash, &typed_datum.payload) {
            Some(b) => b,
            None => return,
        };
    let Some(decoded) = decode_co_datum(&datum_bytes) else {
        return;
    };
    emit_co_change(&JpgCoChange::Created {
        offerer_pkh: decoded.offerer_pkh,
        tx_hash: hex::encode(&p.tx_hash),
        output_index: p.oref.index,
        lovelace: p.output.lovelace,
        datum_cbor: datum_bytes,
        co_version: version,
        target_policy: decoded.target_policy,
        target_asset_names: decoded.target_asset_names,
    });
}

fn handle_consumed(c: &ConsumedEvent) {
    let oref_str = format!("{}#{}", hex::encode(&c.oref.tx_hash), c.oref.index);
    let Some(version) = watched_version(&c.prior_output.address) else {
        // Not a watched address — silent skip, expected for most consumed events.
        return;
    };
    logging::log(
        LogLevel::Info,
        LOG_TARGET,
        &format!("consumed at watched address: oref={oref_str} version={version:?}"),
    );
    let Some(prior_datum) = c.prior_datum.as_ref() else {
        logging::log(
            LogLevel::Warn,
            LOG_TARGET,
            &format!("consumed: prior_datum missing — cannot emit Spent for {oref_str}"),
        );
        return;
    };
    if prior_datum.payload.is_empty() {
        logging::log(
            LogLevel::Warn,
            LOG_TARGET,
            &format!(
                "consumed: prior_datum.payload empty (hash={}) — cannot emit Spent for {oref_str}",
                hex::encode(&prior_datum.hash)
            ),
        );
        return;
    }
    let Some(decoded) = decode_co_datum(&prior_datum.payload) else {
        logging::log(
            LogLevel::Warn,
            LOG_TARGET,
            &format!(
                "consumed: decode_co_datum failed (payload_bytes={}) for {oref_str}",
                prior_datum.payload.len()
            ),
        );
        return;
    };
    logging::log(
        LogLevel::Info,
        LOG_TARGET,
        &format!("consumed: emitting Spent oref={oref_str} pkh={}", decoded.offerer_pkh),
    );
    emit_co_change(&JpgCoChange::Spent {
        tx_hash: hex::encode(&c.oref.tx_hash),
        output_index: c.oref.index,
        offerer_pkh: decoded.offerer_pkh,
    });
}

export!(Module);
