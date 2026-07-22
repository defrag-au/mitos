//! Bearer-token auth for the read surface.
//!
//! A trimmed copy of `mitos-core/src/auth.rs` (`AuthToken` / `require_auth` /
//! `constant_time_eq`) — copied rather than depended on so the walker binary
//! doesn't drag the platform dependency tree in for fifty lines. Same
//! semantics, different secret: the token comes from `MARKET_LEDGER_TOKEN`
//! so the market-history trust domain rotates independently of mitos. Unset
//! ⇒ open mode with a startup warning.

use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;

pub const TOKEN_ENV: &str = "MARKET_LEDGER_TOKEN";

/// Token loaded once at startup. `None` ⇒ open mode (no auth).
#[derive(Clone, Debug)]
pub struct AuthToken(pub Option<String>);

impl AuthToken {
    pub fn from_env() -> Self {
        match std::env::var(TOKEN_ENV) {
            Ok(t) if !t.is_empty() => {
                tracing::info!("auth token loaded; /events requires it");
                Self(Some(t))
            }
            _ => {
                tracing::warn!(
                    "{TOKEN_ENV} not set — serving in open mode (no auth on /events). \
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

/// axum middleware that rejects requests missing or mismatching the
/// configured token. Open mode lets every request through.
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
