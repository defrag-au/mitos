//! Maestro API client for chain-data fallback.
//!
//! Used by two resolution tiers in the platform:
//! - `CachingDataPlane::tx_metadata` → `fetch_aux_data` for aux-data
//!   CBOR on TXs older than the 7-day dolos archive horizon.
//! - `MaestroFallbackPlane::read_utxos` → `fetch_output` for prior
//!   outputs whose create-TX has been pruned past the archive
//!   horizon. The follower hits this any time it dispatches a TX
//!   that consumes a UTxO older than ~7 days (cancel of a long-
//!   standing offer, accept of an old listing, etc.).
//!
//! **Process-wide singleton.** `MaestroClient::shared()` returns
//! the same `Arc<MaestroClient>` for every module in the process,
//! so the connection pool *and* the rate-limit semaphore are
//! global. This is what keeps us a polite Maestro citizen — a
//! 429 from one module's call slows the whole platform, not just
//! that module.
//!
//! **Rate limiting.** Inflight requests are capped by a tokio
//! `Semaphore` (`MAESTRO_MAX_INFLIGHT`, default 4). On 429 we
//! respect `Retry-After` and back off; on 5xx / transport errors
//! we exponentially back off (500ms → 30s cap) for up to 5
//! attempts. The semaphore permit is held across backoff sleeps
//! — that's intentional, since hitting 429 means we already
//! exceeded our rate budget and the right move is to slow *all*
//! callers, not just the one that lost the lottery.
//!
//! Configuration: read from environment at first `shared()` call.
//! Missing `MAESTRO_API_KEY` → returns `None` forever →
//! `CachingDataPlane` / `MaestroFallbackPlane` skip their
//! Maestro tier gracefully.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use cardano_assets::PolicyId;
use mitos_data_plane::{AssetEntry, DecodeLevel, OutputRef, Resolution, TypedDatum, TypedOutput};
use pallas_primitives::{Hash, PlutusData};
use serde::Deserialize;
use tokio::sync::Semaphore;

const DEFAULT_MAX_INFLIGHT: usize = 4;
const MAX_ATTEMPTS: u32 = 5;
const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Process-wide singleton. `None` means `MAESTRO_API_KEY` was
/// unset when first queried; we don't retry env reads.
static SHARED: OnceLock<Option<Arc<MaestroClient>>> = OnceLock::new();

#[derive(Debug, thiserror::Error)]
pub enum MaestroError {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("not found")]
    NotFound,
    #[error("decode: {0}")]
    Decode(String),
    #[error("rate limited after {0} attempts")]
    RateLimited(u32),
    #[error("server error {0} after {1} attempts")]
    Server(reqwest::StatusCode, u32),
}

/// `GET /transactions/{tx_hash}/cbor` response. Maestro wraps
/// the hex payload in a `data` envelope.
#[derive(Deserialize)]
struct TxCborResponse {
    data: String,
}

/// `GET /datums/{datum_hash}` response. Maestro wraps the datum
/// in a `data` envelope carrying the raw CBOR `bytes` (hex) and a
/// decoded `json` view; we only need the bytes.
#[derive(Deserialize)]
struct DatumResponse {
    data: DatumData,
}

#[derive(Deserialize)]
struct DatumData {
    /// Hex-encoded raw CBOR of the datum.
    bytes: String,
}

/// `GET /transactions/{tx_hash}/outputs/{index}/txo` response.
/// Maestro wraps the output in a `data` envelope.
#[derive(Deserialize)]
struct TxoResponse {
    data: TxoData,
}

#[derive(Deserialize)]
struct TxoData {
    address: String,
    assets: Vec<TxoAsset>,
    #[serde(default)]
    datum: Option<TxoDatum>,
}

#[derive(Deserialize)]
struct TxoAsset {
    /// `"lovelace"` for the ADA component, otherwise
    /// `"{policy_hex}{asset_name_hex}"` (56 + variable hex).
    unit: String,
    amount: u64,
}

#[derive(Deserialize)]
struct TxoDatum {
    /// `"hash"` or `"inline"`.
    #[serde(rename = "type")]
    _kind: String,
    hash: String,
    /// Hex-encoded raw CBOR of the datum payload. Always present
    /// for inline datums; present for hash-attached datums when
    /// the indexer that built this Maestro response could resolve
    /// the bytes (which it usually can — Maestro stores witness-
    /// set datums indefinitely).
    #[serde(default)]
    bytes: Option<String>,
}

/// Lightweight Maestro REST client.
///
/// Access via `MaestroClient::shared()` — never construct
/// directly. `Clone` on the returned `Arc` is cheap.
pub struct MaestroClient {
    http: reqwest::Client,
    base_url: String,
    permits: Arc<Semaphore>,
}

impl MaestroClient {
    /// Return the process-wide shared client, initialising on
    /// first call. Subsequent calls are O(1) and return clones of
    /// the same `Arc`, so every caller shares the connection
    /// pool and the rate-limit semaphore.
    pub fn shared() -> Option<Arc<MaestroClient>> {
        SHARED
            .get_or_init(|| Self::from_env().map(Arc::new))
            .clone()
    }

    fn from_env() -> Option<Self> {
        let api_key = std::env::var("MAESTRO_API_KEY").ok()?;
        let network = std::env::var("MAESTRO_NETWORK").unwrap_or_else(|_| "mainnet".to_owned());
        let max_inflight = std::env::var("MAESTRO_MAX_INFLIGHT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_MAX_INFLIGHT);

        let mut headers = reqwest::header::HeaderMap::new();
        let key_val = reqwest::header::HeaderValue::from_str(&api_key).ok()?;
        headers.insert("api-key", key_val);

        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(30))
            .build()
            .ok()?;

        tracing::info!(
            max_inflight,
            network = %network,
            "Maestro client initialised"
        );

        Some(Self {
            http,
            base_url: format!("{network}.gomaestro-api.org/v1"),
            permits: Arc::new(Semaphore::new(max_inflight)),
        })
    }

    /// Fetch auxiliary-data CBOR for a TX by its hex hash.
    ///
    /// Returns:
    /// - `Ok(Some(bytes))` — TX found and has aux_data
    /// - `Ok(None)` — TX found but no aux_data, or 404
    /// - `Err(...)` — exhausted retries on rate-limit or 5xx, or
    ///   a non-retryable transport / decode error
    pub async fn fetch_aux_data(&self, tx_hash_hex: &str) -> Result<Option<Vec<u8>>, MaestroError> {
        let url = format!("https://{}/transactions/{tx_hash_hex}/cbor", self.base_url);
        let Some(body_bytes) = self.get_bytes_with_retries(&url, tx_hash_hex).await? else {
            return Ok(None);
        };
        let body: TxCborResponse = serde_json::from_slice(&body_bytes)
            .map_err(|e| MaestroError::Decode(format!("json: {e}")))?;
        let tx_cbor =
            hex::decode(&body.data).map_err(|e| MaestroError::Decode(format!("hex: {e}")))?;
        Ok(aux_from_tx_cbor(&tx_cbor))
    }

    /// Fetch a single output by reference. Returns `None` when
    /// Maestro doesn't have the output (404) or the response
    /// can't be parsed into the typed shape.
    ///
    /// Used as the third tier in `read_utxos` resolution —
    /// after dolos current-state and dolos archive both miss.
    /// Decode level governs what gets populated:
    /// - `Lean` — address + lovelace + assets only, no datum
    /// - `WithDatum` / `Full` — same, plus datum (with payload
    ///   when Maestro returns the bytes)
    ///
    /// `Full` does *not* populate `script_ref` — this endpoint
    /// doesn't surface reference scripts. Callers needing
    /// reference scripts after a Maestro fallback are rare
    /// enough that a separate lookup can be added if they
    /// appear.
    pub async fn fetch_output(
        &self,
        oref: &OutputRef,
        level: DecodeLevel,
    ) -> Result<Option<TypedOutput>, MaestroError> {
        let tx_hex = hex::encode(oref.tx_hash);
        let url = format!(
            "https://{}/transactions/{tx_hex}/outputs/{}/txo",
            self.base_url, oref.index
        );
        let Some(body_bytes) = self.get_bytes_with_retries(&url, &tx_hex).await? else {
            return Ok(None);
        };
        let body: TxoResponse = serde_json::from_slice(&body_bytes)
            .map_err(|e| MaestroError::Decode(format!("json: {e}")))?;
        Ok(Some(typed_output_from_maestro(body.data, level)))
    }

    /// Fetch a datum's raw CBOR by its hash via
    /// `GET /datums/{datum_hash}`. Returns:
    /// - `Ok(Some(cbor))` — Maestro has the datum preimage
    /// - `Ok(None)` — 404 (Maestro doesn't have it)
    /// - `Err(...)` — retry exhaustion / transport / decode error
    ///
    /// Used as the datum-resolution fallback in
    /// `MaestroFallbackPlane::read_datum` when dolos's `DATUM_NS`
    /// misses — e.g. hash-only CIP-68 reference datums whose
    /// witnessing block predates the dolos sync/archive coverage
    /// (the metadata bytes aren't on the current ref UTxO, only the
    /// hash is). Maestro indexes all witnessed datums, so it can
    /// resolve them regardless of horizon.
    pub async fn fetch_datum(&self, datum_hash_hex: &str) -> Result<Option<Vec<u8>>, MaestroError> {
        let url = format!("https://{}/datums/{datum_hash_hex}", self.base_url);
        let Some(body_bytes) = self.get_bytes_with_retries(&url, datum_hash_hex).await? else {
            return Ok(None);
        };
        let body: DatumResponse = serde_json::from_slice(&body_bytes)
            .map_err(|e| MaestroError::Decode(format!("json: {e}")))?;
        let cbor =
            hex::decode(&body.data.bytes).map_err(|e| MaestroError::Decode(format!("hex: {e}")))?;
        Ok(Some(cbor))
    }

    /// Shared retry loop for any `GET` endpoint that wraps its
    /// payload in Maestro's `{ "data": ... }` envelope. Returns
    /// `Ok(None)` on 404, `Ok(Some(body))` on 200, propagates
    /// retry-exhaustion as `MaestroError::{RateLimited, Server}`.
    ///
    /// The `tag` is included in 429/5xx warn logs so operators
    /// can correlate a backoff event with the TX hash or oref
    /// it pertains to.
    async fn get_bytes_with_retries(
        &self,
        url: &str,
        tag: &str,
    ) -> Result<Option<Vec<u8>>, MaestroError> {
        // Held across retry sleeps on purpose — see module docs.
        let _permit = self
            .permits
            .acquire()
            .await
            .expect("maestro semaphore never closed");

        let mut backoff = INITIAL_BACKOFF;
        for attempt in 1..=MAX_ATTEMPTS {
            let resp = match self.http.get(url).send().await {
                Ok(r) => r,
                Err(e) if e.is_timeout() || e.is_connect() => {
                    if attempt == MAX_ATTEMPTS {
                        return Err(MaestroError::Http(e));
                    }
                    tracing::debug!(
                        attempt,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %e,
                        "Maestro transient transport error, retrying"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }
                Err(e) => return Err(MaestroError::Http(e)),
            };

            let status = resp.status();

            if status == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }

            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                if attempt == MAX_ATTEMPTS {
                    return Err(MaestroError::RateLimited(attempt));
                }
                let wait = parse_retry_after(resp.headers()).unwrap_or(backoff);
                tracing::warn!(
                    attempt,
                    wait_ms = wait.as_millis() as u64,
                    tag,
                    "Maestro 429 rate limited, backing off"
                );
                tokio::time::sleep(wait).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }

            if status.is_server_error() {
                if attempt == MAX_ATTEMPTS {
                    return Err(MaestroError::Server(status, attempt));
                }
                tracing::warn!(
                    attempt,
                    status = %status,
                    backoff_ms = backoff.as_millis() as u64,
                    tag,
                    "Maestro 5xx, retrying"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }

            let resp = resp.error_for_status()?;
            let bytes = resp.bytes().await.map_err(MaestroError::Http)?.to_vec();
            return Ok(Some(bytes));
        }

        // For-loop returns on every path; unreachable unless
        // MAX_ATTEMPTS becomes 0, which would be a bug.
        Err(MaestroError::RateLimited(MAX_ATTEMPTS))
    }
}

/// Parse a `Retry-After` header value in delta-seconds form.
/// HTTP-date form is technically allowed by RFC 7231 but
/// Maestro emits seconds, so we don't bother parsing dates.
/// Clamped to `MAX_BACKOFF` so a misbehaving server can't pin
/// us indefinitely.
fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let val = headers.get(reqwest::header::RETRY_AFTER)?;
    let secs: u64 = val.to_str().ok()?.parse().ok()?;
    Some(Duration::from_secs(secs.min(MAX_BACKOFF.as_secs())))
}

/// Build a `TypedOutput` from Maestro's TXO response. Caller-blind
/// w.r.t. era — Maestro emits the post-decode JSON view, so no
/// CBOR/era guessing is needed.
///
/// Asset units in Maestro's format are `"lovelace"` for ADA, or
/// `"{policy_hex}{asset_name_hex}"` for native assets. Malformed
/// units are silently skipped — the resulting `TypedOutput` is
/// still useful for address-keyed interest matching even if a
/// stray bad asset entry is dropped.
fn typed_output_from_maestro(data: TxoData, level: DecodeLevel) -> TypedOutput {
    let mut lovelace: u64 = 0;
    let mut assets: Vec<AssetEntry> = Vec::new();
    for a in data.assets {
        if a.unit == "lovelace" {
            lovelace = a.amount;
            continue;
        }
        // unit = policy_hex (56 chars) + asset_name_hex (rest)
        if a.unit.len() < 56 {
            tracing::debug!(unit = %a.unit, "Maestro asset unit shorter than 56 chars; skipping");
            continue;
        }
        let policy_hex = &a.unit[..56];
        let asset_name_hex = a.unit[56..].to_owned();
        match PolicyId::new(policy_hex) {
            Ok(policy_id) => assets.push(AssetEntry {
                policy_id,
                asset_name_hex,
                quantity: a.amount,
            }),
            Err(e) => {
                tracing::debug!(unit = %a.unit, error = %e, "Maestro asset unit policy_id parse failed; skipping");
            }
        }
    }

    let datum = match level {
        DecodeLevel::Lean => None,
        DecodeLevel::WithDatum | DecodeLevel::Full => data.datum.and_then(datum_from_maestro),
    };

    TypedOutput {
        address: data.address,
        lovelace,
        assets,
        datum,
        // Reference scripts aren't surfaced by /txo. Leave as
        // None — `DecodeLevel::Full` callers that need them
        // would need a follow-up lookup we haven't built yet.
        script_ref: None,
        original_cbor: None,
        decoded_at: level,
        resolution: Resolution::Resolved,
    }
}

fn datum_from_maestro(d: TxoDatum) -> Option<TypedDatum> {
    let hash_bytes = hex::decode(&d.hash).ok()?;
    if hash_bytes.len() != 32 {
        return None;
    }
    let mut hash_arr = [0u8; 32];
    hash_arr.copy_from_slice(&hash_bytes);
    let hash = Hash::<32>::from(hash_arr);

    let (payload, original_cbor) = if let Some(hex_bytes) = d.bytes {
        match hex::decode(&hex_bytes) {
            Ok(cbor) => {
                let payload = pallas::codec::minicbor::decode::<PlutusData>(&cbor).ok();
                (payload, Some(cbor))
            }
            Err(e) => {
                tracing::debug!(error = %e, "Maestro datum bytes hex decode failed");
                (None, None)
            }
        }
    } else {
        (None, None)
    };
    Some(TypedDatum {
        hash,
        payload,
        original_cbor,
    })
}

/// Extract the auxiliary-data sub-object from a raw Conway/Babbage
/// TX CBOR byte string.
///
/// Cardano TX CBOR layout (Alonzo+):
/// `[tx_body, tx_witness_set, bool is_valid, aux_data / null]`
///
/// The aux_data element at index 3 is extracted and re-serialised
/// with ciborium. Re-encoding is safe for this use case: the
/// module reads metadata by label number (not by raw byte position)
/// and hashes the reconstructed datum bytes, not the aux_cbor
/// itself.
fn aux_from_tx_cbor(cbor: &[u8]) -> Option<Vec<u8>> {
    let decoded: ciborium::Value = ciborium::de::from_reader(cbor).ok()?;
    let ciborium::Value::Array(arr) = decoded else {
        return None;
    };
    let aux = arr.into_iter().nth(3)?;
    match aux {
        ciborium::Value::Null => None,
        other => {
            let mut buf = Vec::new();
            ciborium::ser::into_writer(&other, &mut buf).ok()?;
            if buf.is_empty() { None } else { Some(buf) }
        }
    }
}
