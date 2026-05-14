//! Maestro API client for aux-data fallback.
//!
//! Third resolution tier in `CachingDataPlane::tx_metadata`:
//! called only after both the local cache and the dolos archive
//! return `None` (TX is older than the 7-day archive window and
//! was not cached proactively during `apply_block`).
//!
//! Concurrency: single `reqwest::Client` (connection-pool inside).
//! No parallel Maestro calls are expected — bootstrap walks one
//! module at a time and `tx_metadata` calls within that walk are
//! sequential. The `reqwest` client handles connection reuse.
//!
//! Configuration: read from environment variables at startup.
//! Missing key → `MaestroClient::from_env()` returns `None` →
//! `CachingDataPlane` skips tier 3 gracefully.

use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum MaestroError {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("not found")]
    NotFound,
    #[error("decode: {0}")]
    Decode(String),
}

#[derive(Deserialize)]
struct TxCborResponse {
    data: String,
}

/// Lightweight Maestro REST client used solely for
/// `GET /transactions/{tx_hash}/cbor` aux-data fallback.
pub struct MaestroClient {
    http: reqwest::Client,
    base_url: String,
}

impl MaestroClient {
    /// Build from environment. Returns `None` when `MAESTRO_API_KEY`
    /// is not set — platform starts without the Maestro tier rather
    /// than failing at startup.
    pub fn from_env() -> Option<Self> {
        let api_key = std::env::var("MAESTRO_API_KEY").ok()?;
        let network = std::env::var("MAESTRO_NETWORK")
            .unwrap_or_else(|_| "mainnet".to_owned());

        let mut headers = reqwest::header::HeaderMap::new();
        let key_val = reqwest::header::HeaderValue::from_str(&api_key).ok()?;
        headers.insert("api-key", key_val);

        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .ok()?;

        Some(Self {
            http,
            base_url: format!("{network}.gomaestro-api.org/v1"),
        })
    }

    /// Fetch auxiliary-data CBOR for a TX by its hex hash.
    ///
    /// Returns:
    /// - `Ok(Some(bytes))` — TX found and has aux_data
    /// - `Ok(None)` — TX found but no aux_data, or 404
    /// - `Err(...)` — network / parse failure
    pub async fn fetch_aux_data(&self, tx_hash_hex: &str) -> Result<Option<Vec<u8>>, MaestroError> {
        let url = format!("https://{}/transactions/{tx_hash_hex}/cbor", self.base_url);
        let resp = self.http.get(&url).send().await?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let resp = resp.error_for_status()?;
        let body: TxCborResponse = resp
            .json()
            .await
            .map_err(|e| MaestroError::Decode(format!("json: {e}")))?;

        let tx_cbor = hex::decode(&body.data)
            .map_err(|e| MaestroError::Decode(format!("hex: {e}")))?;

        Ok(aux_from_tx_cbor(&tx_cbor))
    }
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
