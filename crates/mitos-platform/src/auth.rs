//! The host's one authenticator — D9's boundary, wired.
//!
//! v2 had three separate copies of the same bearer-token middleware
//! (`mitos-core::auth`, `admin::require_auth`, `companions::require_auth`),
//! each checking ONE shared secret. Every deployed worker holds that secret, so
//! every worker holds the token that evicts modules; and `client_id` on a
//! subscribe is self-asserted, so any holder can subscribe as anybody.
//!
//! This module does three things and no more:
//!
//! 1. **Authenticate.** Turn a bearer token into a
//!    [`Principal`](access::Principal) — constant-time, against every
//!    configured token.
//! 2. **Carry.** One axum middleware inserts the principal into request
//!    extensions; a missing or unknown token is a 401 right there.
//! 3. **Authorize.** Each route builds an [`Attempt`](access::Attempt) and asks
//!    [`access::Registry::authorize`]. A refusal is a 403.
//!
//! **The policy is NOT here.** Who may do what lives in
//! `mitos_lifecycle::access`, as one pure decision with a decision-table test.
//! If you find yourself writing `if namespace == …` in this crate, that is the
//! second copy, and the two will drift.
//!
//! # 401 versus 403
//!
//! 401 means "who are you"; 403 means "not you". A worker holding a stale
//! token and a worker holding the WRONG NAMESPACE's token are different
//! incidents, and the status code is how an operator tells them apart without
//! reading the host's logs.
//!
//! # Legacy mode
//!
//! With `MITOS_NAMESPACES_FILE` unset the host builds
//! [`access::Registry::legacy`]: one implicit namespace owning every client,
//! reachable with the operator token alone. That is byte-for-byte v2, and the
//! proof is that no pre-existing test changed.

use std::collections::BTreeSet;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use mitos_lifecycle::access::{
    AdminAccess, Attempt, ClientSet, Denied, Granted, NamespaceDef, OperatorScope,
    OperatorTokenAsClient, Principal, Registry, RegistryError,
};
use mitos_lifecycle::ids::{ClientId, IdError, Namespace};
use mitos_lifecycle::policy::{
    EjectPolicy, NamespaceProfile, ResourceProfile, RevisionPolicy, Tier,
};
use serde::{Deserialize, Serialize};

/// Shortest namespace token the host will accept. `openssl rand -hex 32` is
/// 64 characters; 32 bytes is the floor below which a token is a password
/// somebody typed.
const MIN_NAMESPACE_TOKEN_LEN: usize = 32;

/// Which posture the host is running in. Reported on `/_admin/status` so an
/// operator can confirm a deploy took effect without reading the unit file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthMode {
    /// No `MITOS_NAMESPACES_FILE` — exactly v2.
    Legacy,
    Namespaces,
}

/// Why the host refused to start.
///
/// Every variant is a MISCONFIGURATION of a security boundary, so each is
/// fatal rather than a warning: a host that declares a boundary and then does
/// not enforce it is worse than one that never declared it.
#[derive(Debug, thiserror::Error)]
pub enum AuthSetupError {
    #[error("reading {path}: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("parsing {path}: {source}")]
    Parse {
        path: String,
        source: toml::de::Error,
    },
    #[error("{0}")]
    Registry(#[from] RegistryError),
    #[error("id: {0}")]
    Id(#[from] IdError),
    #[error("namespace `{namespace}` names `{var}` as its token env var, and it is unset or empty")]
    TokenEnvUnset { namespace: String, var: String },
    #[error(
        "namespace `{namespace}`'s token (from {var}) is {len} characters; at least \
         {MIN_NAMESPACE_TOKEN_LEN} are required — generate one with `openssl rand -hex 32`"
    )]
    TokenTooShort {
        namespace: String,
        var: String,
        len: usize,
    },
    #[error(
        "{first} and {second} have the same value; a token must resolve to exactly one principal"
    )]
    DuplicateToken { first: String, second: String },
    #[error(
        "MITOS_NAMESPACES_FILE is set but MITOS_AUTH_TOKEN is not. Open mode does not combine \
         with namespaces: a host that declares a security boundary and then admits every \
         request unauthenticated is a misconfiguration, not a posture."
    )]
    NamespacesWithoutOperatorToken,
}

/// The on-disk shape of `MITOS_NAMESPACES_FILE`.
///
/// Separate from [`NamespaceDef`] because it carries `token_env` — the NAME of
/// an environment variable. The secret itself never enters the file; systemd's
/// `EnvironmentFile` carries it, exactly as it carries `MITOS_AUTH_TOKEN`
/// today. See `docs/work-orders/WO-02-namespaces.example.toml`, which a test in
/// the lifecycle crate parses so it cannot drift from these types.
#[derive(Debug, Deserialize)]
struct RegistryFile {
    operator_token_as_client: OperatorTokenAsClient,
    #[serde(default, rename = "namespace")]
    namespaces: Vec<NamespaceFileEntry>,
}

#[derive(Debug, Deserialize)]
struct NamespaceFileEntry {
    name: Namespace,
    token_env: String,
    clients: BTreeSet<ClientId>,
    profile: NamespaceProfile,
}

/// A bearer token's identity, or the absence of one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    Principal(Principal),
    /// Legacy mode with no `MITOS_AUTH_TOKEN`: every request is allowed, as it
    /// was in v2. Treated downstream as the full operator.
    OpenMode,
    Unauthenticated,
}

/// Turns bearer tokens into principals and answers authorization questions.
///
/// Built once at boot and shared (`Arc`) by all three routers. Constructed
/// from explicit values in tests and from the environment in the binary — no
/// test depends on process-global env.
pub struct Authenticator {
    registry: Registry,
    mode: AuthMode,
    /// `None` only in legacy mode, and only then does open mode exist.
    operator_full: Option<String>,
    operator_readonly: Option<String>,
    /// `(namespace, token)`. A Vec rather than a map because resolution
    /// compares against EVERY entry regardless — see [`Self::resolve`].
    namespace_tokens: Vec<(Namespace, String)>,
}

/// Hand-written, and it must stay that way: four of this struct's fields are
/// secrets, and a derived `Debug` would put them in the first log line that
/// ever formatted an `Authenticator` — including a panic message. Only
/// whether a token is configured is ever printed, never a token.
impl std::fmt::Debug for Authenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Authenticator")
            .field("mode", &self.mode)
            .field("namespaces", &self.registry.namespaces().len())
            .field("operator_full", &self.operator_full.is_some())
            .field("operator_readonly", &self.operator_readonly.is_some())
            .field("namespace_tokens", &self.namespace_tokens.len())
            .finish()
    }
}

impl Authenticator {
    /// A host with no namespace config. `full: None` is open mode.
    pub fn legacy(operator_full: Option<String>, operator_readonly: Option<String>) -> Self {
        Self {
            registry: Registry::legacy(legacy_namespace(), legacy_profile()),
            mode: AuthMode::Legacy,
            operator_full,
            operator_readonly,
            namespace_tokens: Vec::new(),
        }
    }

    /// A host with namespaces. `namespace_tokens` must cover every namespace
    /// in `registry`; the env-reading half of that lives in [`Self::from_env`].
    pub fn configured(
        registry: Registry,
        namespace_tokens: Vec<(Namespace, String)>,
        operator_full: String,
        operator_readonly: Option<String>,
    ) -> Result<Self, AuthSetupError> {
        // A token must resolve to exactly ONE principal. Two principals
        // sharing a token is not an ambiguity to break by ordering — it is a
        // boundary that silently is not one.
        let mut seen: Vec<(String, &str)> = vec![("the operator token".to_owned(), &operator_full)];
        if let Some(ro) = &operator_readonly {
            seen.push(("the read-only token".to_owned(), ro));
        }
        for (namespace, token) in &namespace_tokens {
            seen.push((format!("namespace `{namespace}`'s token"), token));
        }
        for i in 0..seen.len() {
            for j in (i + 1)..seen.len() {
                if seen[i].1 == seen[j].1 {
                    return Err(AuthSetupError::DuplicateToken {
                        first: seen[i].0.clone(),
                        second: seen[j].0.clone(),
                    });
                }
            }
        }

        Ok(Self {
            registry,
            mode: AuthMode::Namespaces,
            operator_full: Some(operator_full),
            operator_readonly,
            namespace_tokens,
        })
    }

    /// The production constructor. `MITOS_NAMESPACES_FILE` unset ⇒ legacy.
    pub fn from_env() -> Result<Self, AuthSetupError> {
        let Some(path) = non_empty_env("MITOS_NAMESPACES_FILE") else {
            let operator_full = non_empty_env("MITOS_AUTH_TOKEN");
            if operator_full.is_none() {
                tracing::warn!(
                    "MITOS_AUTH_TOKEN not set — running in open mode (no auth on /_admin, \
                     /api/companions or /api/tx). Set it in production."
                );
            }
            return Ok(Self::legacy(
                operator_full,
                non_empty_env("MITOS_READONLY_TOKEN"),
            ));
        };
        let raw = std::fs::read_to_string(&path).map_err(|source| AuthSetupError::Read {
            path: path.clone(),
            source,
        })?;
        Self::from_registry_file(&path, &raw, &non_empty_env)
    }

    /// The env-free half of [`Self::from_env`], so every startup refusal has a
    /// test that does not race the process environment. `lookup` resolves an
    /// env var NAME — a `token_env`, or one of the two operator tokens — to
    /// its value.
    pub fn from_registry_file(
        path: &str,
        raw: &str,
        lookup: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Self, AuthSetupError> {
        // Checked before the file is even parsed: it is the one refusal that
        // is about posture rather than content, and saying so first gives the
        // operator the actionable message.
        let Some(operator_full) = lookup("MITOS_AUTH_TOKEN") else {
            return Err(AuthSetupError::NamespacesWithoutOperatorToken);
        };

        let file: RegistryFile = toml::from_str(raw).map_err(|source| AuthSetupError::Parse {
            path: path.to_owned(),
            source,
        })?;

        let mut defs = Vec::with_capacity(file.namespaces.len());
        let mut tokens = Vec::with_capacity(file.namespaces.len());
        for entry in file.namespaces {
            let token = lookup(&entry.token_env).ok_or_else(|| AuthSetupError::TokenEnvUnset {
                namespace: entry.name.to_string(),
                var: entry.token_env.clone(),
            })?;
            if token.len() < MIN_NAMESPACE_TOKEN_LEN {
                return Err(AuthSetupError::TokenTooShort {
                    namespace: entry.name.to_string(),
                    var: entry.token_env.clone(),
                    len: token.len(),
                });
            }
            tokens.push((entry.name.clone(), token));
            defs.push(NamespaceDef {
                name: entry.name,
                clients: ClientSet::Only(entry.clients),
                // Parsed and STORED, never applied: nothing in this order
                // changes a budget, a timer or a follower. WO-03 reads it.
                profile: entry.profile,
            });
        }

        let registry = Registry::configured(defs, file.operator_token_as_client)?;
        tracing::info!(
            namespaces = registry.namespaces().len(),
            operator_token_as_client = ?file.operator_token_as_client,
            file = %path,
            "namespace registry loaded",
        );
        Self::configured(
            registry,
            tokens,
            operator_full,
            lookup("MITOS_READONLY_TOKEN"),
        )
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    pub fn mode(&self) -> AuthMode {
        self.mode
    }

    /// Which principal presented `bearer`.
    ///
    /// Compares against EVERY configured token and does not return early on a
    /// match: which token matched must not be observable from response timing.
    pub fn resolve(&self, bearer: Option<&str>) -> Resolution {
        // Open mode short-circuits before the bearer is even looked at, which
        // is what v2 did — a host with no token configured allowed every
        // request, with or without an `Authorization` header.
        let Some(full) = self.operator_full.as_deref() else {
            return Resolution::OpenMode;
        };
        let Some(bearer) = bearer else {
            return Resolution::Unauthenticated;
        };

        let mut found: Option<Principal> = None;
        if constant_time_eq(bearer.as_bytes(), full.as_bytes()) {
            found = Some(Principal::Operator(OperatorScope::Full));
        }
        if let Some(readonly) = self.operator_readonly.as_deref()
            && constant_time_eq(bearer.as_bytes(), readonly.as_bytes())
        {
            found = found.or(Some(Principal::Operator(OperatorScope::ReadOnly)));
        }
        for (namespace, token) in &self.namespace_tokens {
            if constant_time_eq(bearer.as_bytes(), token.as_bytes()) {
                found = found.or(Some(Principal::Namespace(namespace.clone())));
            }
        }
        match found {
            Some(principal) => Resolution::Principal(principal),
            None => Resolution::Unauthenticated,
        }
    }

    pub fn authorize(
        &self,
        principal: &Principal,
        attempt: Attempt<'_>,
    ) -> Result<Granted, Denied> {
        self.registry.authorize(principal, attempt)
    }

    /// Client ids with a registration on disk that no namespace owns.
    ///
    /// Nothing is removed and nothing stops being dialled — this order takes
    /// nothing away. The list is a rollout aid: their next subscribe is a 403
    /// (`UnknownClient`), which is the signal to add them to a namespace.
    pub fn unowned_clients(&self, storage: &crate::storage::ModuleStorage) -> Vec<String> {
        let mut out = BTreeSet::new();
        for module in storage.list_modules().unwrap_or_default() {
            let root = storage.module_dir_for_companions(&module);
            let Ok(entries) = std::fs::read_dir(&root) else {
                continue;
            };
            for entry in entries.flatten() {
                if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().into_owned();
                // `.unreachable/` and friends are dialer metadata, not clients.
                if name.starts_with('.') {
                    continue;
                }
                match ClientId::parse(&name) {
                    Ok(client) if self.registry.client_key(&client).is_some() => {}
                    _ => {
                        out.insert(name);
                    }
                }
            }
        }
        out.into_iter().collect()
    }
}

fn non_empty_env(var: &str) -> Option<String> {
    match std::env::var(var) {
        Ok(v) if !v.is_empty() => Some(v),
        _ => None,
    }
}

/// The implicit namespace of a host with no namespace config.
fn legacy_namespace() -> Namespace {
    Namespace::parse("prod").expect("`prod` is a valid namespace name")
}

/// A production-shaped profile for the implicit legacy namespace. It is
/// carried, never applied — legacy mode changes no behaviour — but it must be
/// the cautious row rather than the permissive one, because WO-03 will read
/// whatever is here and a host with no config is a production host.
fn legacy_profile() -> NamespaceProfile {
    NamespaceProfile {
        tier: Tier::Production,
        eject: EjectPolicy::AlertOnly,
        revision: RevisionPolicy::ExplicitRollbackOnly,
        retain_superseded_secs: 72 * 3600,
        resources: ResourceProfile {
            fuel_per_call: 100_000_000,
            max_memory_bytes: None,
            backfill_concurrency: 4,
        },
    }
}

/// Constant-time compare so an attacker cannot leak a token through
/// response-time side channels. The ONE copy in this workspace's crates.
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

// -----------------------------------------------------------------------------
// Middleware
// -----------------------------------------------------------------------------

/// Resolve the bearer token and insert the [`Principal`] into request
/// extensions. The ONE authentication middleware; all three routers use it.
///
/// A missing or unknown token is a 401 HERE, before any handler runs — the
/// handlers only ever see a principal, so a route cannot forget to check that
/// there was one.
pub async fn authenticate(
    State(auth): State<Arc<Authenticator>>,
    mut req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let bearer = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "));
    let principal = match auth.resolve(bearer) {
        Resolution::Principal(principal) => principal,
        // v2's open mode, preserved: no token configured ⇒ full operator.
        Resolution::OpenMode => Principal::Operator(OperatorScope::Full),
        Resolution::Unauthenticated => return Err(StatusCode::UNAUTHORIZED),
    };
    req.extensions_mut().insert(principal);
    Ok(next.run(req).await)
}

/// The principal [`authenticate`] inserted. Absent only if a router forgot the
/// middleware, which is a wiring bug rather than a request the host should
/// serve — hence the refusal rather than a default.
pub fn principal_of(req: &Request) -> Option<&Principal> {
    req.extensions().get::<Principal>()
}

/// Turn a [`Denied`] into a 403 carrying its own explanation. A refusal names
/// itself: `ClientNotInNamespace` and `OperatorTokenRefused` are different
/// operator actions, and the body is where the operator reads which.
pub fn denied_response(denied: &Denied) -> Response {
    (StatusCode::FORBIDDEN, denied.to_string()).into_response()
}

/// Shared body of the authorization middlewares below.
///
/// Synchronous on purpose: a `&Request` may not be held across an `.await`,
/// because `axum::body::Body` is `Send` but not `Sync`, which would make every
/// middleware's future non-`Send` and fail axum's `Service` bound.
fn guard(auth: &Authenticator, req: &Request, attempt: Attempt<'_>) -> Result<(), Box<Response>> {
    let Some(principal) = principal_of(req) else {
        return Err(Box::new(StatusCode::UNAUTHORIZED.into_response()));
    };
    match auth.authorize(principal, attempt) {
        Ok(_) => Ok(()),
        Err(denied) => {
            tracing::warn!(
                ?principal,
                ?attempt,
                path = %req.uri().path(),
                denied = %denied,
                "request refused",
            );
            Err(Box::new(denied_response(&denied)))
        }
    }
}

/// `Admin(Read)` for `GET`, `Admin(Mutate)` for anything else — decided ONCE,
/// as a layer on the admin router, rather than in each of its twenty handlers.
///
/// Every read endpoint on that router is a `GET` and every mutation is a
/// `POST`/`DELETE`, so the method is a sound proxy for "read-only-safe". That
/// was already the rule v2's `authorize` applied to the read-only token; it is
/// stated here once instead of inside a token comparison.
pub async fn require_admin(
    State(auth): State<Arc<Authenticator>>,
    req: Request,
    next: Next,
) -> Response {
    let access = if req.method() == Method::GET {
        AdminAccess::Read
    } else {
        AdminAccess::Mutate
    };
    match guard(&auth, &req, Attempt::Admin(access)) {
        Ok(()) => next.run(req).await,
        Err(response) => *response,
    }
}

/// A middleware for an attempt that names no client and needs no request
/// inspection: `SubmitTx`, `ReadChain`, `RecaptureModule`.
macro_rules! fixed_attempt_middleware {
    ($(#[$meta:meta])* $name:ident, $attempt:expr) => {
        $(#[$meta])*
        pub async fn $name(
            State(auth): State<Arc<Authenticator>>,
            req: Request,
            next: Next,
        ) -> Response {
            match guard(&auth, &req, $attempt) {
                Ok(()) => next.run(req).await,
                Err(response) => *response,
            }
        }
    };
}

fixed_attempt_middleware!(
    /// `POST /api/tx/submit`.
    require_submit_tx,
    Attempt::SubmitTx
);

fixed_attempt_middleware!(
    /// `/minibf/*` — a read API over PUBLIC chain data, open to any
    /// authenticated principal. NOT `Admin(Read)`: `wallet-viewer` calls it
    /// with the same secret the companions use and is not a subscriber at all,
    /// so folding it into the admin API would 403 that worker the day its
    /// secret is rotated to a namespace token.
    require_read_chain,
    Attempt::ReadChain
);

fixed_attempt_middleware!(
    /// `POST /_admin/modules/{id}/recapture` — its own attempt because a
    /// WORKER does this: `holder-map`'s production cron recaptures
    /// `holder-distribution` with the secret it subscribes with. Whether a
    /// given namespace may is `authorize`'s decision, not this route's.
    require_recapture,
    Attempt::RecaptureModule
);

fixed_attempt_middleware!(
    /// `Admin(Mutate)` on a route that is a `GET`, used by `/replicate/{indexer}`.
    ///
    /// That route is the synthetic-client WebSocket surface `mitos-tail`
    /// consumes, and it is the one route WO-02's attempt table does not list.
    /// Today only the full operator token opens it, so mapping it to
    /// `Admin(Read)` — which [`require_admin`] would pick for a `GET` — would
    /// hand it to the read-only dashboard token as a side effect of this
    /// change. `Admin(Mutate)` admits exactly who it admits now.
    ///
    /// This is deliberately the CONSERVATIVE placeholder, not an opinion: it
    /// widens nothing at rollout, so whichever attempt `/replicate` should
    /// really carry can be decided in `access.rs` later without an outage in
    /// between.
    require_admin_mutate,
    Attempt::Admin(AdminAccess::Mutate)
);

/// The two-namespace host every auth test in this crate runs against: `prod`
/// owning `ownership.cnft.dev`, `dev` owning `ownership.dev.cnft.dev` — the
/// shape of the real deployment, shrunk.
///
/// Lives outside `mod tests` so the admin and companion router tests share
/// exactly one definition of "a configured host": three copies of a fixture
/// is three chances for one of them to quietly stop matching production.
#[cfg(test)]
pub(crate) mod testing {
    use super::*;

    pub(crate) const FULL: &str = "full-operator-token-0000000000000000000000000000";
    pub(crate) const READONLY: &str = "readonly-operator-token-000000000000000000000000";
    pub(crate) const PROD: &str = "prod-namespace-token-000000000000000000000000000";
    pub(crate) const DEV: &str = "dev-namespace-token-0000000000000000000000000000";

    pub(crate) const PROD_CLIENT: &str = "ownership.cnft.dev";
    pub(crate) const DEV_CLIENT: &str = "ownership.dev.cnft.dev";

    pub(crate) fn ns(name: &str) -> Namespace {
        Namespace::parse(name).unwrap()
    }

    pub(crate) fn client(id: &str) -> ClientId {
        ClientId::parse(id).unwrap()
    }

    fn def(name: &str, clients: &[&str], profile: NamespaceProfile) -> NamespaceDef {
        NamespaceDef {
            name: ns(name),
            clients: ClientSet::Only(clients.iter().map(|c| client(c)).collect()),
            profile,
        }
    }

    pub(crate) fn dev_profile() -> NamespaceProfile {
        NamespaceProfile {
            tier: Tier::Development,
            eject: EjectPolicy::Automatic {
                grace_secs: 600,
                retain_secs: 24 * 3600,
            },
            revision: RevisionPolicy::Allow,
            retain_superseded_secs: 24 * 3600,
            resources: ResourceProfile {
                fuel_per_call: 25_000_000,
                max_memory_bytes: Some(512 * 1024 * 1024),
                backfill_concurrency: 1,
            },
        }
    }

    pub(crate) fn two_namespace_registry(mode: OperatorTokenAsClient) -> Registry {
        Registry::configured(
            vec![
                def("prod", &[PROD_CLIENT], legacy_profile()),
                def("dev", &[DEV_CLIENT], dev_profile()),
            ],
            mode,
        )
        .unwrap()
    }

    pub(crate) fn configured_authenticator(mode: OperatorTokenAsClient) -> Authenticator {
        Authenticator::configured(
            two_namespace_registry(mode),
            vec![(ns("prod"), PROD.to_owned()), (ns("dev"), DEV.to_owned())],
            FULL.to_owned(),
            Some(READONLY.to_owned()),
        )
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::testing::*;
    use super::*;

    // ---------------------------------------------------------------------
    // The four `admin::authorize` unit tests, ported. Same intent, same
    // assertions — the function they tested is gone, the rule is not.
    // ---------------------------------------------------------------------

    #[test]
    fn full_token_authorizes_any_method() {
        let auth = Authenticator::legacy(Some(FULL.to_owned()), Some(READONLY.to_owned()));
        let Resolution::Principal(principal) = auth.resolve(Some(FULL)) else {
            panic!("the full token must resolve to a principal");
        };
        for access in [AdminAccess::Read, AdminAccess::Mutate] {
            assert!(auth.authorize(&principal, Attempt::Admin(access)).is_ok());
        }
    }

    #[test]
    fn readonly_token_authorizes_only_get() {
        let auth = Authenticator::legacy(Some(FULL.to_owned()), Some(READONLY.to_owned()));
        let Resolution::Principal(principal) = auth.resolve(Some(READONLY)) else {
            panic!("the read-only token must resolve to a principal");
        };
        assert!(
            auth.authorize(&principal, Attempt::Admin(AdminAccess::Read))
                .is_ok()
        );
        assert_eq!(
            auth.authorize(&principal, Attempt::Admin(AdminAccess::Mutate)),
            Err(Denied::ReadOnlyToken)
        );
    }

    #[test]
    fn wrong_or_missing_token_rejected() {
        let auth = Authenticator::legacy(Some(FULL.to_owned()), Some(READONLY.to_owned()));
        assert_eq!(auth.resolve(None), Resolution::Unauthenticated);
        assert_eq!(auth.resolve(Some("nope")), Resolution::Unauthenticated);
        assert_eq!(
            auth.resolve(Some(&FULL[..FULL.len() - 1])),
            Resolution::Unauthenticated,
            "a prefix of the token is not the token",
        );
    }

    #[test]
    fn readonly_unset_means_only_full_works() {
        let auth = Authenticator::legacy(Some(FULL.to_owned()), None);
        assert!(matches!(auth.resolve(Some(FULL)), Resolution::Principal(_)));
        assert_eq!(auth.resolve(Some(READONLY)), Resolution::Unauthenticated);
    }

    // ---------------------------------------------------------------------

    #[test]
    fn open_mode_allows_everything_as_the_full_operator() {
        let auth = Authenticator::legacy(None, None);
        assert_eq!(auth.resolve(None), Resolution::OpenMode);
        assert_eq!(auth.resolve(Some("anything")), Resolution::OpenMode);
    }

    #[test]
    fn a_namespace_token_resolves_to_its_namespace() {
        let auth = configured_authenticator(OperatorTokenAsClient::AllowWithWarning);
        assert_eq!(
            auth.resolve(Some(PROD)),
            Resolution::Principal(Principal::Namespace(ns("prod")))
        );
        assert_eq!(
            auth.resolve(Some(DEV)),
            Resolution::Principal(Principal::Namespace(ns("dev")))
        );
        assert_eq!(
            auth.resolve(Some(FULL)),
            Resolution::Principal(Principal::Operator(OperatorScope::Full))
        );
        assert_eq!(auth.resolve(Some("stranger")), Resolution::Unauthenticated);
    }

    #[test]
    fn two_principals_may_not_share_a_token() {
        let err = Authenticator::configured(
            two_namespace_registry(OperatorTokenAsClient::AllowWithWarning),
            vec![(ns("prod"), FULL.to_owned()), (ns("dev"), DEV.to_owned())],
            FULL.to_owned(),
            None,
        )
        .unwrap_err();
        assert!(
            matches!(err, AuthSetupError::DuplicateToken { .. }),
            "{err}"
        );

        let err = Authenticator::configured(
            two_namespace_registry(OperatorTokenAsClient::AllowWithWarning),
            vec![(ns("prod"), PROD.to_owned()), (ns("dev"), PROD.to_owned())],
            FULL.to_owned(),
            None,
        )
        .unwrap_err();
        assert!(
            matches!(err, AuthSetupError::DuplicateToken { .. }),
            "{err}"
        );
    }

    // ---------------------------------------------------------------------
    // Startup refusals. A misconfigured boundary is fatal, never a warning —
    // so each of these is a host that does not come up.
    // ---------------------------------------------------------------------

    /// The `WO-02-namespaces.example.toml` shape, minus whatever a test needs
    /// to break.
    fn example_file() -> String {
        r#"
operator_token_as_client = "allow-with-warning"

[[namespace]]
name = "prod"
token_env = "MITOS_TOKEN_PROD"
clients = ["ownership.cnft.dev"]
[namespace.profile]
tier = "production"
revision = "explicit-rollback-only"
retain_superseded_secs = 259200
[namespace.profile.eject]
kind = "alert-only"
[namespace.profile.resources]
fuel_per_call = 100000000
backfill_concurrency = 4

[[namespace]]
name = "dev"
token_env = "MITOS_TOKEN_DEV"
clients = ["ownership.dev.cnft.dev"]
[namespace.profile]
tier = "development"
revision = "allow"
retain_superseded_secs = 86400
[namespace.profile.eject]
kind = "automatic"
grace_secs = 600
retain_secs = 86400
[namespace.profile.resources]
fuel_per_call = 25000000
max_memory_bytes = 536870912
backfill_concurrency = 1
"#
        .to_owned()
    }

    /// A lookup over an explicit table, so no test touches process env.
    fn lookup_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let owned: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |var: &str| owned.iter().find(|(k, _)| k == var).map(|(_, v)| v.clone())
    }

    fn full_env() -> Vec<(&'static str, &'static str)> {
        vec![
            ("MITOS_AUTH_TOKEN", FULL),
            ("MITOS_TOKEN_PROD", PROD),
            ("MITOS_TOKEN_DEV", DEV),
        ]
    }

    #[test]
    fn the_example_registry_file_starts_the_host() {
        let auth =
            Authenticator::from_registry_file("ns.toml", &example_file(), &lookup_of(&full_env()))
                .expect("the documented shape must start a host");
        assert_eq!(auth.mode(), AuthMode::Namespaces);
        assert_eq!(auth.registry().namespaces().len(), 2);
        assert_eq!(
            auth.resolve(Some(PROD)),
            Resolution::Principal(Principal::Namespace(ns("prod")))
        );
    }

    #[test]
    fn an_unparseable_registry_file_refuses_to_start() {
        let err = Authenticator::from_registry_file(
            "ns.toml",
            "this is not = = toml",
            &lookup_of(&full_env()),
        )
        .unwrap_err();
        assert!(matches!(err, AuthSetupError::Parse { .. }), "{err}");
    }

    #[test]
    fn a_client_in_two_namespaces_refuses_to_start() {
        let doubled = example_file().replace(
            r#"clients = ["ownership.dev.cnft.dev"]"#,
            r#"clients = ["ownership.cnft.dev"]"#,
        );
        let err = Authenticator::from_registry_file("ns.toml", &doubled, &lookup_of(&full_env()))
            .unwrap_err();
        assert!(
            matches!(
                err,
                AuthSetupError::Registry(RegistryError::ClientInTwoNamespaces { .. })
            ),
            "{err}"
        );
    }

    #[test]
    fn an_unset_token_env_refuses_to_start() {
        let err = Authenticator::from_registry_file(
            "ns.toml",
            &example_file(),
            &lookup_of(&[("MITOS_AUTH_TOKEN", FULL), ("MITOS_TOKEN_PROD", PROD)]),
        )
        .unwrap_err();
        match err {
            AuthSetupError::TokenEnvUnset { namespace, var } => {
                assert_eq!(namespace, "dev");
                assert_eq!(var, "MITOS_TOKEN_DEV");
            }
            other => panic!("expected TokenEnvUnset, got {other}"),
        }
    }

    #[test]
    fn a_short_namespace_token_refuses_to_start() {
        let err = Authenticator::from_registry_file(
            "ns.toml",
            &example_file(),
            &lookup_of(&[
                ("MITOS_AUTH_TOKEN", FULL),
                ("MITOS_TOKEN_PROD", "hunter2"),
                ("MITOS_TOKEN_DEV", DEV),
            ]),
        )
        .unwrap_err();
        assert!(matches!(err, AuthSetupError::TokenTooShort { .. }), "{err}");
    }

    #[test]
    fn two_equal_tokens_refuse_to_start() {
        let err = Authenticator::from_registry_file(
            "ns.toml",
            &example_file(),
            &lookup_of(&[
                ("MITOS_AUTH_TOKEN", FULL),
                ("MITOS_TOKEN_PROD", PROD),
                ("MITOS_TOKEN_DEV", PROD),
            ]),
        )
        .unwrap_err();
        assert!(
            matches!(err, AuthSetupError::DuplicateToken { .. }),
            "{err}"
        );
    }

    #[test]
    fn namespaces_without_an_operator_token_refuse_to_start() {
        let err = Authenticator::from_registry_file(
            "ns.toml",
            &example_file(),
            &lookup_of(&[("MITOS_TOKEN_PROD", PROD), ("MITOS_TOKEN_DEV", DEV)]),
        )
        .unwrap_err();
        assert!(
            matches!(err, AuthSetupError::NamespacesWithoutOperatorToken),
            "{err}"
        );
    }

    // ---------------------------------------------------------------------
    // The two middlewares whose routers live in other crates (`/minibf` in
    // `bundles/default`, `/api/tx/submit` in `mitos-core`). Exercised here
    // over a stub router so the ATTEMPT each maps to is pinned in the crate
    // that defines it, rather than in whichever binary happens to mount it.
    // ---------------------------------------------------------------------

    async fn probe<F, Fut>(middleware: F, token: Option<&str>) -> axum::http::StatusCode
    where
        F: Clone + Send + Sync + 'static + FnMut(State<Arc<Authenticator>>, Request, Next) -> Fut,
        Fut: std::future::Future<Output = Response> + Send + 'static,
    {
        use tower::ServiceExt;

        let auth = Arc::new(configured_authenticator(OperatorTokenAsClient::Refuse));
        let router = axum::Router::new()
            .route("/probe", axum::routing::get(|| async { "ok" }))
            .layer(axum::middleware::from_fn_with_state(
                auth.clone(),
                middleware,
            ))
            .layer(axum::middleware::from_fn_with_state(auth, authenticate));

        let mut req = axum::http::Request::builder().uri("/probe");
        if let Some(token) = token {
            req = req.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        router
            .oneshot(req.body(axum::body::Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
    }

    /// `wallet-viewer` reads `/minibf` with the same secret the companions
    /// use and subscribes to nothing, so it is in no namespace's client list.
    /// It must keep working once that secret holds a namespace token.
    #[tokio::test]
    async fn minibf_accepts_any_authenticated_principal_and_no_other() {
        use axum::http::StatusCode;

        for token in [PROD, DEV, READONLY, FULL] {
            assert_eq!(
                probe(require_read_chain, Some(token)).await,
                StatusCode::OK,
                "chain data is public to any authenticated principal",
            );
        }
        assert_eq!(
            probe(require_read_chain, None).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            probe(require_read_chain, Some("stranger")).await,
            StatusCode::UNAUTHORIZED,
        );
    }

    /// The six `tx-submit` consumers hold `MITOS_AUTH_TOKEN` through a
    /// wrangler binding; each keeps working once that secret is a namespace
    /// token. The read-only token must not be able to spend.
    #[tokio::test]
    async fn a_namespace_token_may_submit_a_tx_and_a_readonly_token_may_not() {
        use axum::http::StatusCode;

        for token in [PROD, DEV, FULL] {
            assert_eq!(probe(require_submit_tx, Some(token)).await, StatusCode::OK);
        }
        assert_eq!(
            probe(require_submit_tx, Some(READONLY)).await,
            StatusCode::FORBIDDEN,
        );
        assert_eq!(
            probe(require_submit_tx, None).await,
            StatusCode::UNAUTHORIZED
        );
    }

    /// `/replicate/{indexer}` keeps exactly the access it has today: the full
    /// operator token and nobody else. See `require_admin_mutate`.
    #[tokio::test]
    async fn the_replicate_surface_stays_operator_only() {
        use axum::http::StatusCode;

        assert_eq!(
            probe(require_admin_mutate, Some(FULL)).await,
            StatusCode::OK
        );
        for token in [PROD, DEV, READONLY] {
            assert_eq!(
                probe(require_admin_mutate, Some(token)).await,
                StatusCode::FORBIDDEN,
            );
        }
    }

    #[test]
    fn legacy_mode_carries_a_production_shaped_profile() {
        // It is never applied in this order, but a host with no config is a
        // production host and WO-03 will read whatever is here.
        let auth = Authenticator::legacy(Some(FULL.to_owned()), None);
        let profile = auth.registry().namespaces()[0].profile;
        assert_eq!(profile.tier, Tier::Production);
        assert_eq!(profile.eject, EjectPolicy::AlertOnly);
        assert_eq!(profile.revision, RevisionPolicy::ExplicitRollbackOnly);
    }
}
