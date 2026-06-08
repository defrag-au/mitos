//! Consumer → host **transaction submission** route.
//!
//! `POST /api/tx/submit` takes a signed tx (CBOR [`SubmitTxRequest`]), validates +
//! enqueues it into dolos's mempool via [`Domain::receive_tx`], which the sync
//! pipeline's `submit::Stage` then diffuses to the chain over node-to-node
//! TxSubmission. This lets a consumer (the minting engine) submit through mitos —
//! which already holds the node connection — instead of a flaky third-party API.
//!
//! Mirrors dolos's own minibf `/tx/submit` route (same `receive_tx` call + error
//! taxonomy); the difference is the typed CBOR wrapper ([`mitos_protocol::submit`])
//! and that it's served on the mitos host alongside `/api/companions/*`, gated by
//! the same `MITOS_AUTH_TOKEN`. Status codes are the consumer's classification
//! signal: 400 = chain rejected (permanent), 409 = duplicate (idempotent success),
//! 5xx = host unavailable (transient → the engine's fallback may try Maestro).

use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use dolos::adapters::DomainAdapter;
use dolos_core::{ChainError, Domain, DomainError, MempoolError, SubmitExt};
use mitos_protocol::submit::{SUBMIT_MIME, SubmitTxRequest, SubmitTxResponse};

use crate::auth::{AuthToken, require_auth};

/// Build the tx-submission router, gated by the shared-secret auth token. Merged
/// into the host `app` in [`crate::bundle::Bundle::run`].
pub fn tx_router(domain: DomainAdapter, auth: AuthToken) -> Router {
    Router::new()
        .route("/api/tx/submit", post(submit_handler))
        .with_state(domain)
        .layer(axum::middleware::from_fn_with_state(auth, require_auth))
}

async fn submit_handler(State(domain): State<DomainAdapter>, body: Bytes) -> Response {
    let req = match SubmitTxRequest::decode(&body) {
        Ok(r) => r,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("decode request: {e}")).into_response(),
    };

    // Validate (dolos phase-1/2) + enqueue into the mempool — the same call the
    // dolos minibf route makes. The sync `submit::Stage` diffuses it to the chain.
    let chain = domain.read_chain();
    match domain.receive_tx("mitos", &chain, &req.tx_cbor) {
        Ok(hash) => {
            let resp = SubmitTxResponse {
                tx_hash: hash.as_ref().to_vec(),
            };
            match resp.encode() {
                Ok(bytes) => ([(header::CONTENT_TYPE, SUBMIT_MIME)], bytes).into_response(),
                Err(e) => {
                    (StatusCode::INTERNAL_SERVER_ERROR, format!("encode: {e}")).into_response()
                }
            }
        }
        Err(e) => map_submit_error(&e).into_response(),
    }
}

/// Map dolos's submit error to the HTTP status the consumer classifies on.
/// Mirrors `dolos minibf routes/tx/submit`: chain-validation faults → 400,
/// duplicate → 409 (idempotent success), runtime/state faults → 500.
fn map_submit_error(e: &DomainError) -> (StatusCode, String) {
    let status = match e {
        DomainError::ChainError(
            ChainError::BrokenInvariant(_)
            | ChainError::DecodingError(_)
            | ChainError::CborDecodingError(_)
            | ChainError::AddressDecoding(_)
            | ChainError::Phase1ValidationRejected(_)
            | ChainError::Phase2ValidationRejected(_),
        ) => StatusCode::BAD_REQUEST,
        DomainError::MempoolError(x) => match x {
            MempoolError::TraverseError(_)
            | MempoolError::InvalidTx(_)
            | MempoolError::DecodeError(_)
            | MempoolError::PlutusNotSupported => StatusCode::BAD_REQUEST,
            MempoolError::DuplicateTx => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        },
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, e.to_string())
}
