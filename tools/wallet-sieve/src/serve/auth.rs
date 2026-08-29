//! Bearer-token auth — the market-ledger trimmed copy of mitos-core's
//! `AuthToken` / `require_auth` / `constant_time_eq`, with its own secret so
//! the sieve trust domain rotates independently. Unset ⇒ open mode with a
//! startup warning (fine on loopback, never expose that way).

use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;

pub const TOKEN_ENV: &str = "WALLET_SIEVE_TOKEN";

/// Token loaded once at startup. `None` ⇒ open mode (no auth).
#[derive(Clone, Debug)]
pub struct AuthToken(pub Option<String>);

impl AuthToken {
    pub fn from_env() -> Self {
        match std::env::var(TOKEN_ENV) {
            Ok(t) if !t.is_empty() => {
                tracing::info!("auth token loaded; data routes require it");
                Self(Some(t))
            }
            _ => {
                tracing::warn!(
                    "{TOKEN_ENV} not set — serving in open mode (no auth). \
                     Set the env var before exposing this beyond localhost."
                );
                Self(None)
            }
        }
    }

    pub fn as_deref(&self) -> Option<&str> {
        self.0.as_deref()
    }
}

/// axum middleware rejecting requests missing or mismatching the token.
pub async fn require_auth(
    State(token): State<AuthToken>,
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

/// Constant-time compare so an attacker can't leak the token via
/// response-time side channels.
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
