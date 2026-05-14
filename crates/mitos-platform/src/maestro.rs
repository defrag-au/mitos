//! Maestro API client for aux-data fallback.
//!
//! Third resolution tier in `CachingDataPlane::tx_metadata`:
//! called only after both the local cache and the dolos archive
//! return `None` (TX is older than the 7-day archive window and
//! was not cached proactively during `apply_block`).
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
//! `CachingDataPlane` skips tier 3 gracefully.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

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

#[derive(Deserialize)]
struct TxCborResponse {
    data: String,
}

/// Lightweight Maestro REST client used solely for
/// `GET /transactions/{tx_hash}/cbor` aux-data fallback.
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
        let network =
            std::env::var("MAESTRO_NETWORK").unwrap_or_else(|_| "mainnet".to_owned());
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
    pub async fn fetch_aux_data(
        &self,
        tx_hash_hex: &str,
    ) -> Result<Option<Vec<u8>>, MaestroError> {
        let url = format!("https://{}/transactions/{tx_hash_hex}/cbor", self.base_url);

        // Held across retry sleeps on purpose — see module docs.
        let _permit = self
            .permits
            .acquire()
            .await
            .expect("maestro semaphore never closed");

        let mut backoff = INITIAL_BACKOFF;
        for attempt in 1..=MAX_ATTEMPTS {
            let resp = match self.http.get(&url).send().await {
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
                    tx_hash = tx_hash_hex,
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
                    "Maestro 5xx, retrying"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }

            let resp = resp.error_for_status()?;
            let body: TxCborResponse = resp
                .json()
                .await
                .map_err(|e| MaestroError::Decode(format!("json: {e}")))?;

            let tx_cbor = hex::decode(&body.data)
                .map_err(|e| MaestroError::Decode(format!("hex: {e}")))?;

            return Ok(aux_from_tx_cbor(&tx_cbor));
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
