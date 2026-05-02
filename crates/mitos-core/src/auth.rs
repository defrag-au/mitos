//! Shared-secret bearer-token auth for replication + admin
//! endpoints.
//!
//! Three places use this:
//!
//! - `/replicate/{indexer}` (test/synthetic-client surface) — checks
//!   the upgrade request's `Authorization: Bearer <token>` header.
//! - `/_admin/subscriptions` — same.
//! - `Replicator` outbound dials — adds the header to its
//!   tungstenite request so the CF DO can verify the connection
//!   came from mitos, not an attacker who guessed the URL.
//!
//! Token comes from `MITOS_AUTH_TOKEN`. If unset, all of the above
//! run in open mode and a warning is logged at startup. Suitable
//! for `wrangler dev` and synthetic tests; production should
//! always set the env.

use axum::extract::{FromRequestParts, OptionalFromRequestParts, Request};
use axum::http::{StatusCode, header, request::Parts};
use axum::middleware::Next;
use axum::response::Response;

/// Token loaded once at startup. `None` ⇒ open mode (no auth).
#[derive(Clone, Debug)]
pub struct AuthToken(pub Option<String>);

impl AuthToken {
    pub fn from_env() -> Self {
        match std::env::var("MITOS_AUTH_TOKEN") {
            Ok(t) if !t.is_empty() => {
                tracing::info!("auth token loaded; replicate + admin endpoints require it");
                Self(Some(t))
            }
            _ => {
                tracing::warn!(
                    "MITOS_AUTH_TOKEN not set — running in open mode (no auth on \
                     /replicate or /_admin). Set the env var in production."
                );
                Self(None)
            }
        }
    }

    pub fn as_deref(&self) -> Option<&str> {
        self.0.as_deref()
    }

    pub fn is_open(&self) -> bool {
        self.0.is_none()
    }
}

/// axum middleware that rejects requests missing or mismatching the
/// configured token. Open mode lets every request through.
pub async fn require_auth(
    axum::extract::State(token): axum::extract::State<AuthToken>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if let Some(expected) = token.as_deref() {
        let provided = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.strip_prefix("Bearer "));
        match provided {
            Some(p) if constant_time_eq(p.as_bytes(), expected.as_bytes()) => {}
            _ => return Err(StatusCode::UNAUTHORIZED),
        }
    }
    Ok(next.run(req).await)
}

/// Constant-time string compare so an attacker can't leak the token
/// via response-time side channels.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

// FromRequestParts so handlers that want the token directly (e.g.
// the Replicator's spawn point) can pull it from request state.
// Currently unused; left as scaffolding for endpoints that do their
// own per-route auth instead of going through the middleware.
impl<S: Send + Sync> FromRequestParts<S> for AuthToken
where
    AuthToken: axum::extract::FromRef<S>,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(_parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Ok(<AuthToken as axum::extract::FromRef<S>>::from_ref(state))
    }
}

impl<S: Send + Sync> OptionalFromRequestParts<S> for AuthToken
where
    AuthToken: axum::extract::FromRef<S>,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        _parts: &mut Parts,
        state: &S,
    ) -> Result<Option<Self>, Self::Rejection> {
        Ok(Some(
            <AuthToken as axum::extract::FromRef<S>>::from_ref(state),
        ))
    }
}
