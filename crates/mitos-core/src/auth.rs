//! Bearer-token auth for the bundle's own routes.
//!
//! The authenticator itself lives in [`mitos_platform::auth`] — one
//! implementation for all three routers, resolving a token to a
//! `mitos_lifecycle::access::Principal` and asking `access::Registry::authorize`
//! who may do what. This module is the mitos-core-side adapter: it re-exports
//! that middleware under the names the bundle and its binaries already use, and
//! keeps [`AuthToken`] for the one job that is genuinely a shared secret rather
//! than an identity — presenting a credential on an OUTBOUND dial.
//!
//! Token comes from `MITOS_AUTH_TOKEN`. If unset, the host runs in open mode
//! and a warning is logged at startup. Suitable for `wrangler dev` and
//! synthetic tests; production should always set the env.

use axum::extract::{FromRequestParts, OptionalFromRequestParts};
use axum::http::request::Parts;

pub use mitos_platform::auth::{
    Authenticator, Resolution, authenticate, require_read_chain, require_submit_tx,
};

/// A shared secret, as an OUTBOUND credential.
///
/// Inbound gating is [`Authenticator`]'s job. What is left is the dial-back
/// side: the `Replicator` and the in-tree indexer bridge add this token to
/// their requests so the far end can tell the connection came from mitos and
/// not from somebody who guessed the URL.
#[derive(Clone, Debug)]
pub struct AuthToken(pub Option<String>);

impl AuthToken {
    pub fn from_env() -> Self {
        match std::env::var("MITOS_AUTH_TOKEN") {
            Ok(t) if !t.is_empty() => Self(Some(t)),
            _ => Self(None),
        }
    }

    pub fn as_deref(&self) -> Option<&str> {
        self.0.as_deref()
    }

    pub fn is_open(&self) -> bool {
        self.0.is_none()
    }
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
        Ok(Some(<AuthToken as axum::extract::FromRef<S>>::from_ref(
            state,
        )))
    }
}
