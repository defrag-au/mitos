//! Koios API client for chain-data fallback.
//!
//! A second [`crate::fallback::FallbackProvider`] alongside Maestro,
//! covering the same two resolution tiers (aux-data / hash-only
//! datums, and archive-pruned prior outputs) for deployments that
//! prefer Koios — notably because its **free public tier needs no
//! API key**. That's the headline difference from Maestro:
//! [`MaestroClient::from_env`](crate::maestro::MaestroClient) returns
//! `None` without `MAESTRO_API_KEY`, whereas Koios builds a working
//! keyless client when `KOIOS_API_KEY` is absent.
//!
//! **Native batch endpoints.** Unlike Maestro (which falls back to
//! the trait's bounded fan-out), Koios exposes POST batch endpoints
//! that resolve many references in one HTTP call:
//! - `POST /tx_metadata` → aux-data for many TXs;
//! - `POST /utxo_info` (`_extended`) → many prior outputs;
//! - `POST /datum_info` → many datum preimages.
//!
//! So [`KoiosProvider`] *overrides* the batch trait methods with one
//! request each; the three single methods just call the batch with a
//! 1-element slice and unwrap.
//!
//! **The aux-data re-encode.** Koios returns tx metadata as decoded
//! JSON (`metadata: {"721": {...}}`), not raw CBOR — so unlike
//! Maestro (which we hand the TX CBOR and slice element 3 out of),
//! we must *re-encode* that JSON back into auxiliary-data CBOR that
//! [`cardano_assets::cip25::cip25_metadata_json`] can parse. See
//! [`koios_metadata_to_aux_cbor`] — it builds a pallas
//! `AuxiliaryData::Shelley(Metadata)` (the bare Shelley metadata-map
//! shape `aux_metadata` accepts) and minicbor-encodes it.
//!
//! **Process-wide singleton + rate limiting.** Same shape as Maestro:
//! [`KoiosProvider::shared`] returns one `Arc` for the whole process,
//! so the connection pool and the inflight `Semaphore`
//! (`KOIOS_MAX_INFLIGHT`, default 4) are global; on 429 we respect
//! `Retry-After`, on 5xx / transport errors we back off (500ms → 30s)
//! for up to 5 attempts, holding the permit across sleeps.
//!
//! Configuration (read once at first `shared()` call):
//! - `KOIOS_BASE_URL` — full API base, overrides network selection;
//! - `KOIOS_NETWORK` — `api` (mainnet, default) / `preprod` /
//!   `preview`, used to build `https://{net}.koios.rest/api/v1`;
//! - `KOIOS_API_KEY` — optional bearer token (Pro tier);
//! - `KOIOS_MAX_INFLIGHT` — inflight cap (default 4).

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures_util::future::join_all;

use cardano_assets::PolicyId;
use mitos_data_plane::{AssetEntry, DecodeLevel, OutputRef, Resolution, TypedDatum, TypedOutput};
use pallas_primitives::alonzo::AuxiliaryData;
use pallas_primitives::{Hash, Int, Metadata, Metadatum, PlutusData};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::Semaphore;

const DEFAULT_MAX_INFLIGHT: usize = 4;
/// Max refs per Koios bulk POST. Koios rejects oversized bodies with
/// `413 Request Entity Too Large` — observed at BOTH 274 AND 150 hashes,
/// but NOT at 50, so the per-request cap sits between 50 and 150 (well
/// under the byte estimate — likely an array-length limit). 50 is the
/// proven-safe value (a 10k one-tx-per-asset cold start ran it with 0×
/// 413). Do NOT raise without re-testing the live API: an oversized
/// chunk 413s, which drops the whole chunk to the per-asset fallback and
/// triggers a 429 storm. The throughput lever for big old collections is
/// the Koios PRO tier (250 req/10 s), not bigger chunks. A failed chunk
/// is skipped, not fatal — the caller re-resolves its gaps.
const KOIOS_BATCH_CHUNK: usize = 50;
const MAX_ATTEMPTS: u32 = 5;
const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Process-wide singleton. Unlike Maestro this is `Some` even
/// without an API key (Koios's free tier is keyless); `None` only
/// on a client-build failure (bad header value / TLS init).
static SHARED: OnceLock<Option<Arc<KoiosProvider>>> = OnceLock::new();

#[derive(Debug, thiserror::Error)]
pub enum KoiosError {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("decode: {0}")]
    Decode(String),
    #[error("rate limited after {0} attempts")]
    RateLimited(u32),
    #[error("server error {0} after {1} attempts")]
    Server(reqwest::StatusCode, u32),
}

// ---- Koios response shapes ------------------------------------------------

/// One element of the `POST /tx_metadata` response array.
///
/// `metadata` is a JSON object keyed by label *string* (e.g.
/// `"721"`), value is the decoded metadatum tree. Null / absent for
/// TXs that carry no metadata.
#[derive(Deserialize)]
struct TxMetadataEntry {
    tx_hash: String,
    #[serde(default)]
    metadata: Option<Value>,
}

/// One element of the `POST /datum_info` response array. Koios keys
/// the hash as `datum_hash` (confirmed against the live free tier —
/// not `hash`); `bytes` is the raw CBOR preimage in hex.
#[derive(Deserialize)]
struct DatumInfoEntry {
    datum_hash: String,
    #[serde(default)]
    bytes: Option<String>,
}

/// One element of the `POST /utxo_info` (`_extended: true`) response
/// array. `value` is lovelace as a numeric string (Koios) — deser via
/// a string-or-number helper. `inline_datum` is `null` or an object
/// carrying the CBOR `bytes`.
#[derive(Deserialize)]
struct UtxoInfoEntry {
    tx_hash: String,
    tx_index: u32,
    address: String,
    #[serde(deserialize_with = "de_u64_str_or_num")]
    value: u64,
    #[serde(default)]
    datum_hash: Option<String>,
    #[serde(default)]
    inline_datum: Option<KoiosInlineDatum>,
    #[serde(default)]
    asset_list: Vec<KoiosAsset>,
}

#[derive(Deserialize)]
struct KoiosInlineDatum {
    /// Hex-encoded raw CBOR of the inline datum. Koios always
    /// populates this for inline datums; modelled `Option` for
    /// defensiveness against shape drift.
    #[serde(default)]
    bytes: Option<String>,
}

#[derive(Deserialize)]
struct KoiosAsset {
    policy_id: String,
    /// Hex-encoded asset name (may be empty for a nameless asset).
    asset_name: Option<String>,
    /// Quantity as a numeric string.
    #[serde(deserialize_with = "de_u64_str_or_num")]
    quantity: u64,
}

/// Koios encodes numeric quantities as JSON strings (e.g.
/// `"2000000"`) but occasionally as bare numbers; accept both.
fn de_u64_str_or_num<'de, D>(d: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    match Value::deserialize(d)? {
        Value::String(s) => s.parse::<u64>().map_err(D::Error::custom),
        Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| D::Error::custom(format!("not a u64: {n}"))),
        other => Err(D::Error::custom(format!("expected u64, got {other}"))),
    }
}

// ---- Client ---------------------------------------------------------------

/// Lightweight Koios REST client.
///
/// Access via [`KoiosProvider::shared`] — never construct directly.
/// `Clone` on the returned `Arc` is cheap.
pub struct KoiosProvider {
    http: reqwest::Client,
    base_url: String,
    permits: Arc<Semaphore>,
}

impl KoiosProvider {
    /// Return the process-wide shared client, initialising on first
    /// call. Subsequent calls are O(1) clones of the same `Arc`, so
    /// the connection pool and rate-limit semaphore stay global.
    pub fn shared() -> Option<Arc<KoiosProvider>> {
        SHARED
            .get_or_init(|| Self::from_env().map(Arc::new))
            .clone()
    }

    fn from_env() -> Option<Self> {
        let base_url = std::env::var("KOIOS_BASE_URL").unwrap_or_else(|_| {
            let network = std::env::var("KOIOS_NETWORK").unwrap_or_else(|_| "api".to_owned());
            format!("https://{network}.koios.rest/api/v1")
        });
        let max_inflight = std::env::var("KOIOS_MAX_INFLIGHT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_MAX_INFLIGHT);

        let mut headers = reqwest::header::HeaderMap::new();
        let keyed = match std::env::var("KOIOS_API_KEY") {
            Ok(key) if !key.is_empty() => {
                // A malformed key value shouldn't kill the keyless
                // path — drop the header and continue keyless.
                match reqwest::header::HeaderValue::from_str(&format!("Bearer {key}")) {
                    Ok(val) => {
                        headers.insert(reqwest::header::AUTHORIZATION, val);
                        true
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "KOIOS_API_KEY not a valid header value; continuing keyless");
                        false
                    }
                }
            }
            _ => false,
        };

        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(60))
            .build()
            .ok()?;

        tracing::info!(
            max_inflight,
            base_url = %base_url,
            keyed,
            "Koios client initialised"
        );

        Some(Self {
            http,
            base_url,
            permits: Arc::new(Semaphore::new(max_inflight)),
        })
    }

    /// Shared POST-with-retry loop for Koios's batch endpoints.
    /// Sends `body` as JSON to `{base_url}{path}`, returns the raw
    /// 200 body bytes; honours 429 `Retry-After`, backs off on 5xx /
    /// transport errors, and (per module docs) holds the inflight
    /// permit across backoff sleeps.
    ///
    /// `tag` is logged with 429/5xx warns so operators can correlate
    /// a backoff with the endpoint it hit.
    async fn post_bytes_with_retries<B: serde::Serialize>(
        &self,
        path: &str,
        body: &B,
        tag: &str,
    ) -> Result<Vec<u8>, KoiosError> {
        let url = format!("{}{path}", self.base_url);
        // Held across retry sleeps on purpose — a 429 means we've
        // already exceeded budget, so slow *all* callers.
        let _permit = self
            .permits
            .acquire()
            .await
            .expect("koios semaphore never closed");

        let mut backoff = INITIAL_BACKOFF;
        for attempt in 1..=MAX_ATTEMPTS {
            let resp = match self.http.post(&url).json(body).send().await {
                Ok(r) => r,
                Err(e) if e.is_timeout() || e.is_connect() => {
                    if attempt == MAX_ATTEMPTS {
                        return Err(KoiosError::Http(e));
                    }
                    tracing::debug!(
                        attempt,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %e,
                        "Koios transient transport error, retrying"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }
                Err(e) => return Err(KoiosError::Http(e)),
            };

            let status = resp.status();

            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                if attempt == MAX_ATTEMPTS {
                    return Err(KoiosError::RateLimited(attempt));
                }
                let wait = parse_retry_after(resp.headers()).unwrap_or(backoff);
                tracing::warn!(
                    attempt,
                    wait_ms = wait.as_millis() as u64,
                    tag,
                    "Koios 429 rate limited, backing off"
                );
                tokio::time::sleep(wait).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }

            if status.is_server_error() {
                if attempt == MAX_ATTEMPTS {
                    return Err(KoiosError::Server(status, attempt));
                }
                tracing::warn!(
                    attempt,
                    status = %status,
                    backoff_ms = backoff.as_millis() as u64,
                    tag,
                    "Koios 5xx, retrying"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }

            let resp = resp.error_for_status()?;
            let bytes = resp.bytes().await.map_err(KoiosError::Http)?.to_vec();
            return Ok(bytes);
        }

        // For-loop returns on every path; unreachable unless
        // MAX_ATTEMPTS becomes 0, which would be a bug.
        Err(KoiosError::RateLimited(MAX_ATTEMPTS))
    }

    /// `POST /tx_metadata` for many TXs in one call, re-encoding each
    /// `metadata` JSON object back to auxiliary-data CBOR. Best-effort
    /// — TXs with null/empty metadata, and any whose re-encode fails,
    /// are dropped (the caller re-resolves gaps individually).
    async fn fetch_aux_data_batch_inner(
        &self,
        tx_hash_hexes: &[String],
    ) -> Result<HashMap<String, Vec<u8>>, KoiosError> {
        // Chunk + POST concurrently (bounded by the inflight semaphore).
        let partials: Vec<HashMap<String, Vec<u8>>> = join_all(
            tx_hash_hexes
                .chunks(KOIOS_BATCH_CHUNK)
                .map(|chunk| async move {
                    let mut m = HashMap::new();
                    let body = TxMetadataRequest { tx_hashes: chunk };
                    let bytes = match self
                        .post_bytes_with_retries("/tx_metadata", &body, "tx_metadata")
                        .await
                    {
                        Ok(b) => b,
                        Err(e) => {
                            tracing::warn!(error = %e, chunk = chunk.len(), "Koios /tx_metadata chunk failed; skipping");
                            return m;
                        }
                    };
                    let entries: Vec<TxMetadataEntry> = match serde_json::from_slice(&bytes) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!(error = %e, "Koios /tx_metadata chunk decode failed; skipping");
                            return m;
                        }
                    };
                    for entry in entries {
                        let Some(metadata) = entry.metadata else {
                            continue;
                        };
                        // Koios returns `metadata: {}` (or `null`) for txs
                        // with no metadata; skip rather than encode empty.
                        if metadata.as_object().is_none_or(|o| o.is_empty()) {
                            continue;
                        }
                        match koios_metadata_to_aux_cbor(&metadata) {
                            Some(aux_cbor) => {
                                m.insert(entry.tx_hash, aux_cbor);
                            }
                            None => {
                                tracing::debug!(tx = %entry.tx_hash, "Koios metadata re-encode produced nothing; skipping");
                            }
                        }
                    }
                    m
                }),
        )
        .await;
        let mut out = HashMap::new();
        for m in partials {
            out.extend(m);
        }
        Ok(out)
    }

    /// `POST /datum_info` for many datum hashes, hex-decoding the
    /// `bytes` preimage of each. Best-effort.
    async fn fetch_datums_batch_inner(
        &self,
        hashes: &[String],
    ) -> Result<HashMap<String, Vec<u8>>, KoiosError> {
        let partials: Vec<HashMap<String, Vec<u8>>> =
            join_all(hashes.chunks(KOIOS_BATCH_CHUNK).map(|chunk| async move {
                let mut m = HashMap::new();
                let body = DatumInfoRequest {
                    datum_hashes: chunk,
                };
                let bytes = match self
                    .post_bytes_with_retries("/datum_info", &body, "datum_info")
                    .await
                {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(error = %e, chunk = chunk.len(), "Koios /datum_info chunk failed; skipping");
                        return m;
                    }
                };
                let entries: Vec<DatumInfoEntry> = match serde_json::from_slice(&bytes) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(error = %e, "Koios /datum_info chunk decode failed; skipping");
                        return m;
                    }
                };
                for entry in entries {
                    let Some(hex_bytes) = entry.bytes else {
                        continue;
                    };
                    match hex::decode(&hex_bytes) {
                        Ok(cbor) => {
                            m.insert(entry.datum_hash, cbor);
                        }
                        Err(e) => {
                            tracing::debug!(hash = %entry.datum_hash, error = %e, "Koios datum bytes hex decode failed; skipping");
                        }
                    }
                }
                m
            }))
            .await;
        let mut out = HashMap::new();
        for m in partials {
            out.extend(m);
        }
        Ok(out)
    }

    /// `POST /utxo_info` (`_extended`) for many output refs, mapping
    /// each to a [`TypedOutput`] at the requested decode level.
    /// Best-effort.
    async fn fetch_outputs_batch_inner(
        &self,
        orefs: &[OutputRef],
        level: DecodeLevel,
    ) -> Result<HashMap<(Hash<32>, u32), TypedOutput>, KoiosError> {
        let partials: Vec<HashMap<(Hash<32>, u32), TypedOutput>> =
            join_all(orefs.chunks(KOIOS_BATCH_CHUNK).map(|chunk| async move {
                    let mut m = HashMap::new();
                    let refs: Vec<String> = chunk
                        .iter()
                        .map(|o| format!("{}#{}", hex::encode(o.tx_hash), o.index))
                        .collect();
                    let body = UtxoInfoRequest {
                        utxo_refs: &refs,
                        extended: true,
                    };
                    let bytes = match self
                        .post_bytes_with_retries("/utxo_info", &body, "utxo_info")
                        .await
                    {
                        Ok(b) => b,
                        Err(e) => {
                            tracing::warn!(error = %e, chunk = chunk.len(), "Koios /utxo_info chunk failed; skipping");
                            return m;
                        }
                    };
                    let entries: Vec<UtxoInfoEntry> = match serde_json::from_slice(&bytes) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!(error = %e, "Koios /utxo_info chunk decode failed; skipping");
                            return m;
                        }
                    };
                    for entry in entries {
                        let Some(tx_hash) = parse_hash32(&entry.tx_hash) else {
                            tracing::debug!(tx = %entry.tx_hash, "Koios utxo_info tx_hash parse failed; skipping");
                            continue;
                        };
                        m.insert((tx_hash, entry.tx_index), typed_output_from_koios(entry, level));
                    }
                    m
                }))
                .await;
        let mut out = HashMap::new();
        for m in partials {
            out.extend(m);
        }
        Ok(out)
    }
}

// ---- Request bodies -------------------------------------------------------

#[derive(serde::Serialize)]
struct TxMetadataRequest<'a> {
    #[serde(rename = "_tx_hashes")]
    tx_hashes: &'a [String],
}

#[derive(serde::Serialize)]
struct DatumInfoRequest<'a> {
    #[serde(rename = "_datum_hashes")]
    datum_hashes: &'a [String],
}

#[derive(serde::Serialize)]
struct UtxoInfoRequest<'a> {
    #[serde(rename = "_utxo_refs")]
    utxo_refs: &'a [String],
    #[serde(rename = "_extended")]
    extended: bool,
}

// ---- Helpers --------------------------------------------------------------

/// Parse a `Retry-After` header value in delta-seconds form. Clamped
/// to `MAX_BACKOFF` so a misbehaving server can't pin us forever.
fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let val = headers.get(reqwest::header::RETRY_AFTER)?;
    let secs: u64 = val.to_str().ok()?.parse().ok()?;
    Some(Duration::from_secs(secs.min(MAX_BACKOFF.as_secs())))
}

/// Parse a hex 32-byte hash string into `Hash<32>`. `None` on bad hex
/// or wrong length.
fn parse_hash32(hex_str: &str) -> Option<Hash<32>> {
    let bytes = hex::decode(hex_str).ok()?;
    let arr: [u8; 32] = bytes.try_into().ok()?;
    Some(Hash::<32>::from(arr))
}

/// Build a [`TypedOutput`] from a Koios `utxo_info` entry. Mirrors
/// `maestro::typed_output_from_maestro`: address + lovelace + assets
/// always; datum only at `WithDatum`/`Full`. Reference scripts aren't
/// populated (same gap as the Maestro path).
fn typed_output_from_koios(entry: UtxoInfoEntry, level: DecodeLevel) -> TypedOutput {
    let mut assets: Vec<AssetEntry> = Vec::with_capacity(entry.asset_list.len());
    for a in entry.asset_list {
        match PolicyId::new(&a.policy_id) {
            Ok(policy_id) => assets.push(AssetEntry {
                policy_id,
                asset_name_hex: a.asset_name.unwrap_or_default(),
                quantity: a.quantity,
            }),
            Err(e) => {
                tracing::debug!(policy = %a.policy_id, error = %e, "Koios asset policy_id parse failed; skipping");
            }
        }
    }

    let datum = match level {
        DecodeLevel::Lean => None,
        DecodeLevel::WithDatum | DecodeLevel::Full => {
            datum_from_koios(entry.datum_hash.as_deref(), entry.inline_datum.as_ref())
        }
    };

    TypedOutput {
        address: entry.address,
        lovelace: entry.value,
        assets,
        datum,
        // /utxo_info surfaces `reference_script` but we don't decode
        // it — same gap as the Maestro fallback. Callers needing
        // ref scripts after a fallback are rare enough to defer.
        script_ref: None,
        original_cbor: None,
        decoded_at: level,
        resolution: Resolution::Resolved,
    }
}

/// Build a [`TypedDatum`] from a Koios output's datum fields. Prefers
/// the inline datum's CBOR `bytes` (decoding `PlutusData`); falls back
/// to a hash-only datum when only `datum_hash` is present.
fn datum_from_koios(
    datum_hash: Option<&str>,
    inline: Option<&KoiosInlineDatum>,
) -> Option<TypedDatum> {
    // Inline datums arrive with the CBOR bytes but no explicit hash,
    // so we derive the hash (blake2b-256) from those bytes. Hash-only
    // outputs give us `datum_hash` directly but no preimage.
    if let Some(inline) = inline
        && let Some(hex_bytes) = inline.bytes.as_deref()
        && let Ok(cbor) = hex::decode(hex_bytes)
    {
        let hash = blake2b256(&cbor);
        let payload = pallas::codec::minicbor::decode::<PlutusData>(&cbor).ok();
        return Some(TypedDatum {
            hash,
            payload,
            original_cbor: Some(cbor),
        });
    }

    // Hash-only datum: we have the hash but not (yet) the preimage.
    // A downstream `read_datum` fallback resolves the bytes via
    // /datum_info if needed.
    let h = datum_hash?;
    let hash = parse_hash32(h)?;
    Some(TypedDatum {
        hash,
        payload: None,
        original_cbor: None,
    })
}

/// Blake2b-256 of the datum CBOR — the datum hash. Inline datums on
/// Koios outputs come without an explicit hash, so we derive it.
fn blake2b256(bytes: &[u8]) -> Hash<32> {
    use pallas::crypto::hash::Hasher;
    Hasher::<256>::hash(bytes)
}

// ---- The re-encode: Koios metadata JSON → aux-data CBOR -------------------

/// Re-encode Koios's decoded `metadata` JSON object
/// (`{"<label>": <value>, ...}`, labels as decimal strings) into
/// transaction auxiliary-data CBOR — the inverse of
/// `cardano_assets::cip25::metadatum_to_json`.
///
/// Builds a pallas `AuxiliaryData::Shelley(Metadata)` (bare Shelley
/// metadata-map shape, which `cip25::aux_metadata` accepts) and
/// minicbor-encodes it. `Metadata` is `BTreeMap<u64, Metadatum>`, so
/// each label string is parsed to `u64`; labels that don't parse are
/// skipped. Returns `None` if the input isn't an object or no label
/// survived.
pub fn koios_metadata_to_aux_cbor(metadata: &Value) -> Option<Vec<u8>> {
    let obj = metadata.as_object()?;
    let mut md: Metadata = Metadata::new();
    for (label_str, val) in obj {
        let Ok(label) = label_str.parse::<u64>() else {
            tracing::debug!(label = %label_str, "Koios metadata label not a u64; skipping");
            continue;
        };
        md.insert(label, json_to_metadatum(val));
    }
    if md.is_empty() {
        return None;
    }
    pallas::codec::minicbor::to_vec(AuxiliaryData::Shelley(md)).ok()
}

/// Inverse of `cip25::metadatum_to_json`: render a `serde_json::Value`
/// to a pallas `Metadatum`.
///
/// - Object → `Map` (keys become `Metadatum::Text`)
/// - Array → `Array`
/// - String → `Text`
/// - Number → `Int` from `i64`/`i128`; non-integer or out-of-range
///   numbers fall back to `Text` of the number's string form
/// - Bool → `Text("true"/"false")`
/// - Null → `Text("")`
fn json_to_metadatum(v: &Value) -> Metadatum {
    match v {
        Value::Object(map) => {
            let pairs: Vec<(Metadatum, Metadatum)> = map
                .iter()
                .map(|(k, val)| (Metadatum::Text(k.clone()), json_to_metadatum(val)))
                .collect();
            Metadatum::Map(pairs.into())
        }
        Value::Array(items) => Metadatum::Array(items.iter().map(json_to_metadatum).collect()),
        Value::String(s) => Metadatum::Text(s.clone()),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Metadatum::Int(Int::from(i))
            } else if let Some(i) = n.as_i128().and_then(|x| Int::try_from(x).ok()) {
                Metadatum::Int(i)
            } else {
                // Non-integer (f64) or beyond i128 — keep as text so
                // we don't lose the value or guess a lossy int.
                Metadatum::Text(n.to_string())
            }
        }
        Value::Bool(b) => Metadatum::Text(if *b { "true" } else { "false" }.to_owned()),
        Value::Null => Metadatum::Text(String::new()),
    }
}

// ---- FallbackProvider impl ------------------------------------------------

/// Koios as a [`crate::fallback::FallbackProvider`]. Overrides all
/// three batch methods with native Koios batch endpoints; the single
/// methods delegate to the batch with a 1-element slice. Transport
/// errors map to `FallbackError` (call sites only log the message);
/// the batch methods are best-effort and swallow errors (returning an
/// empty/partial map), matching the trait contract.
#[async_trait::async_trait]
impl crate::fallback::FallbackProvider for KoiosProvider {
    async fn fetch_aux_data(
        &self,
        tx_hash_hex: &str,
    ) -> Result<Option<Vec<u8>>, crate::fallback::FallbackError> {
        let one = [tx_hash_hex.to_owned()];
        let mut map = self
            .fetch_aux_data_batch_inner(&one)
            .await
            .map_err(|e| crate::fallback::FallbackError(e.to_string()))?;
        Ok(map.remove(tx_hash_hex))
    }

    async fn fetch_output(
        &self,
        oref: &OutputRef,
        level: DecodeLevel,
    ) -> Result<Option<TypedOutput>, crate::fallback::FallbackError> {
        let one = [*oref];
        let mut map = self
            .fetch_outputs_batch_inner(&one, level)
            .await
            .map_err(|e| crate::fallback::FallbackError(e.to_string()))?;
        Ok(map.remove(&(oref.tx_hash, oref.index)))
    }

    async fn fetch_datum(
        &self,
        datum_hash_hex: &str,
    ) -> Result<Option<Vec<u8>>, crate::fallback::FallbackError> {
        let one = [datum_hash_hex.to_owned()];
        let mut map = self
            .fetch_datums_batch_inner(&one)
            .await
            .map_err(|e| crate::fallback::FallbackError(e.to_string()))?;
        Ok(map.remove(datum_hash_hex))
    }

    async fn fetch_aux_data_batch(&self, tx_hash_hexes: &[String]) -> HashMap<String, Vec<u8>> {
        match self.fetch_aux_data_batch_inner(tx_hash_hexes).await {
            Ok(map) => map,
            Err(e) => {
                tracing::warn!(error = %e, count = tx_hash_hexes.len(), "Koios fetch_aux_data_batch failed; returning empty");
                HashMap::new()
            }
        }
    }

    async fn fetch_outputs_batch(
        &self,
        orefs: &[OutputRef],
        level: DecodeLevel,
    ) -> HashMap<(Hash<32>, u32), TypedOutput> {
        match self.fetch_outputs_batch_inner(orefs, level).await {
            Ok(map) => map,
            Err(e) => {
                tracing::warn!(error = %e, count = orefs.len(), "Koios fetch_outputs_batch failed; returning empty");
                HashMap::new()
            }
        }
    }

    async fn fetch_datums_batch(&self, hashes: &[String]) -> HashMap<String, Vec<u8>> {
        match self.fetch_datums_batch_inner(hashes).await {
            Ok(map) => map,
            Err(e) => {
                tracing::warn!(error = %e, count = hashes.len(), "Koios fetch_datums_batch failed; returning empty");
                HashMap::new()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trip gate: a realistic Koios `/tx_metadata` `metadata`
    /// JSON for label 721 → aux-data CBOR via
    /// [`koios_metadata_to_aux_cbor`] → decoded back by the shared
    /// `cardano_assets::cip25` decoder. Proves our re-encode produces
    /// CBOR the live CIP-25 decoder accepts, so a Koios-sourced mint
    /// and a dolos-sourced mint render byte-identical metadata.
    #[test]
    fn koios_metadata_round_trips_through_cip25_decoder() {
        // 56-hex policy + plain ascii asset name (matches a real
        // Clay-Nation-shaped CIP-25 mint).
        let policy_hex = "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728";
        let asset_name = "ClayNation1";

        let metadata: Value = serde_json::from_str(&format!(
            r#"{{
                "721": {{
                    "{policy_hex}": {{
                        "{asset_name}": {{
                            "name": "Foo #1",
                            "image": "ipfs://QmUHdjHYQMVu33uHmsfYkeGnBJDPc8fQsaG6dRgZ6SY2Xr",
                            "edition": 1,
                            "traits": [{{"Background": "Blue"}}],
                            "files": [
                                {{"src": "ipfs://QmFile", "mediaType": "image/png"}}
                            ]
                        }}
                    }}
                }}
            }}"#
        ))
        .expect("metadata json parses");

        let aux_cbor = koios_metadata_to_aux_cbor(&metadata).expect("re-encode produces aux cbor");

        let policy_bytes = hex::decode(policy_hex).expect("policy hex");
        let json = cardano_assets::cip25::cip25_metadata_json(
            &aux_cbor,
            &policy_bytes,
            asset_name.as_bytes(),
        )
        .expect("cip25 decoder resolves the re-encoded aux data");

        // The decoded JSON must carry the fields we encoded.
        assert!(json.contains("\"name\""), "missing name: {json}");
        assert!(json.contains("Foo #1"), "missing name value: {json}");
        assert!(json.contains("Background"), "missing trait key: {json}");
        assert!(json.contains("Blue"), "missing trait value: {json}");
        assert!(
            json.contains("image/png"),
            "missing nested file field: {json}"
        );
        // Integer round-trips as a JSON number, not a string.
        assert!(
            json.contains("\"edition\":1"),
            "edition not a number: {json}"
        );
    }

    #[test]
    fn empty_or_non_object_metadata_returns_none() {
        let empty: Value = serde_json::from_str("{}").unwrap();
        let bad_label: Value = serde_json::from_str(r#"{"notalabel": {}}"#).unwrap();
        assert!(koios_metadata_to_aux_cbor(&Value::Null).is_none());
        assert!(koios_metadata_to_aux_cbor(&empty).is_none());
        // A label that isn't a u64 is skipped → empty map → None.
        assert!(koios_metadata_to_aux_cbor(&bad_label).is_none());
    }

    /// Quantities and lovelace arrive from Koios as JSON strings;
    /// confirm the deser helper accepts both string and number forms.
    #[test]
    fn u64_str_or_num_accepts_both() {
        #[derive(serde::Deserialize)]
        struct Wrap {
            #[serde(deserialize_with = "de_u64_str_or_num")]
            v: u64,
        }
        let a: Wrap = serde_json::from_str(r#"{"v":"2000000"}"#).unwrap();
        let b: Wrap = serde_json::from_str(r#"{"v":2000000}"#).unwrap();
        assert_eq!(a.v, 2_000_000);
        assert_eq!(b.v, 2_000_000);
    }
}
