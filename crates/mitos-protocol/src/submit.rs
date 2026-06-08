//! Wire types for consumer → host **transaction submission**.
//!
//! A consumer (the minting engine) POSTs a signed tx to the mitos host's
//! `POST /api/tx/submit`; the host validates it (dolos phase-1/2) and pushes it
//! into dolos's mempool, which diffuses it to the chain via the node-to-node
//! TxSubmission stage of the sync pipeline. CBOR-encoded (`SUBMIT_MIME`), same
//! codec as `subscribe` — see the host's `submit_handler` (mitos-core) and the
//! engine's `MitosSubmitProvider` (consumer).
//!
//! Success is a 2xx carrying [`SubmitTxResponse`]; failures bypass it and return
//! a text body with a status the consumer classifies on:
//! - `400` — the chain REJECTED the tx (phase-1/2 invalid, bad inputs, …): permanent.
//! - `409` — DUPLICATE (already in the mempool / on chain): idempotent success.
//! - `5xx` — host/runtime unavailable: transient, the consumer may fall back / retry.

use serde::{Deserialize, Serialize};

/// Wire MIME type for both `SubmitTxRequest` and `SubmitTxResponse`.
pub const SUBMIT_MIME: &str = "application/cbor";

/// Request body for `POST /api/tx/submit` — the raw signed transaction CBOR.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitTxRequest {
    /// Signed transaction, CBOR-encoded (the exact bytes a node accepts).
    #[serde(with = "serde_bytes")]
    pub tx_cbor: Vec<u8>,
}

/// Response body for a 2xx `POST /api/tx/submit` — the accepted tx's hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitTxResponse {
    /// The submitted transaction's hash (32 bytes).
    #[serde(with = "serde_bytes")]
    pub tx_hash: Vec<u8>,
}

/// Encode/decode failure for the submit wire types.
#[derive(Debug, thiserror::Error)]
pub enum SubmitWireError {
    #[error("encode: {0}")]
    Encode(String),
    #[error("decode: {0}")]
    Decode(String),
}

impl SubmitTxRequest {
    /// CBOR-encode for the wire (the engine's `MitosSubmitProvider` request body).
    pub fn encode(&self) -> Result<Vec<u8>, SubmitWireError> {
        let mut buf = Vec::with_capacity(self.tx_cbor.len() + 16);
        ciborium::ser::into_writer(self, &mut buf)
            .map_err(|e| SubmitWireError::Encode(e.to_string()))?;
        Ok(buf)
    }
    /// CBOR-decode (the host's `submit_handler`).
    pub fn decode(bytes: &[u8]) -> Result<Self, SubmitWireError> {
        ciborium::de::from_reader(bytes).map_err(|e| SubmitWireError::Decode(e.to_string()))
    }
}

impl SubmitTxResponse {
    /// CBOR-encode (the host's `submit_handler` success body).
    pub fn encode(&self) -> Result<Vec<u8>, SubmitWireError> {
        let mut buf = Vec::with_capacity(48);
        ciborium::ser::into_writer(self, &mut buf)
            .map_err(|e| SubmitWireError::Encode(e.to_string()))?;
        Ok(buf)
    }
    /// CBOR-decode (the engine's `MitosSubmitProvider` response).
    pub fn decode(bytes: &[u8]) -> Result<Self, SubmitWireError> {
        ciborium::de::from_reader(bytes).map_err(|e| SubmitWireError::Decode(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_round_trips() {
        let req = SubmitTxRequest {
            tx_cbor: vec![1, 2, 3, 4, 5],
        };
        let bytes = req.encode().unwrap();
        let back = SubmitTxRequest::decode(&bytes).unwrap();
        assert_eq!(back.tx_cbor, req.tx_cbor);
    }

    #[test]
    fn response_round_trips() {
        let resp = SubmitTxResponse {
            tx_hash: vec![0xab; 32],
        };
        let bytes = resp.encode().unwrap();
        let back = SubmitTxResponse::decode(&bytes).unwrap();
        assert_eq!(back.tx_hash, resp.tx_hash);
    }
}
