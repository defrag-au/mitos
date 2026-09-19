//! Who may do what — D9's security boundary, as one pure decision.
//!
//! v2 has ONE shared `MITOS_AUTH_TOKEN`. Workers hold it, so every worker holds
//! the token that evicts modules; and `client_id` on a subscribe is
//! self-asserted, so any holder can subscribe AS `ownership.cnft.dev`. The
//! interest-mutation route is looser still — it names no client at all and
//! applies to every registration under a `companion_key`, and `dev` and `prod`
//! workers share companion keys (the policy-id DO name). None of that was
//! exploitable while every token holder was us. v3 lets a subscribe create an
//! instance and move a client, so the boundary has to become real first.
//!
//! The rule: **a token resolves to a [`Principal`]; a namespace principal may
//! act ONLY as the clients its namespace lists.** Everything else follows.
//!
//! The host owns the secrets — this crate never sees a token. It maps a bearer
//! token to a `Principal` (constant-time, against every configured token) and
//! asks [`Registry::authorize`] the rest.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::ids::{ClientId, ClientKey, Namespace};
use crate::policy::NamespaceProfile;

/// Which clients a namespace owns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ClientSet {
    /// Exactly these. The only form a CONFIGURED registry may use.
    Only(BTreeSet<ClientId>),
    /// Every client id. Exists for [`Registry::legacy`] alone: a host with no
    /// namespace config behaves as v2 did, with one implicit namespace. It is
    /// refused in a configured registry because an unlisted `dev` client
    /// would silently become a `prod` one.
    Any,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceDef {
    pub name: Namespace,
    pub clients: ClientSet,
    pub profile: NamespaceProfile,
}

/// What to do when a CLIENT action arrives bearing the OPERATOR's full token.
///
/// Every deployed worker holds that token today, so refusing it on day one
/// would take every companion offline at once. The rollout is: deploy with
/// `AllowWithWarning` → rotate each worker's secret to its namespace token →
/// flip to `Refuse`. The warning is what tells you who is left.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OperatorTokenAsClient {
    AllowWithWarning,
    Refuse,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RegistryError {
    #[error("namespace `{0}` is defined twice")]
    DuplicateNamespace(Namespace),
    #[error("client `{client}` is listed under both `{first}` and `{second}`")]
    ClientInTwoNamespaces {
        client: ClientId,
        first: Namespace,
        second: Namespace,
    },
    #[error(
        "namespace `{0}` claims every client; that is only valid as the single implicit \
         namespace of a host with no namespace config"
    )]
    AnyInConfiguredRegistry(Namespace),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registry {
    namespaces: Vec<NamespaceDef>,
    operator_token_as_client: OperatorTokenAsClient,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorScope {
    Full,
    /// `MITOS_READONLY_TOKEN` — dashboards and agents that observe.
    ReadOnly,
}

/// Who a bearer token turned out to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Principal {
    Operator(OperatorScope),
    Namespace(Namespace),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminAccess {
    Read,
    Mutate,
}

/// What is being attempted. Deliberately coarse — a finer split would only be
/// finer in the places it is wrong. `ReadChain` and `RecaptureModule` are NOT
/// refinements of `Admin`: each exists because a census of every worker that
/// holds a mitos token (2026-09-19) found one doing exactly that, with the same
/// secret it subscribes with. A case earns its place here the same way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Attempt<'a> {
    /// Anything under `/_admin` (and, later, `/mcp`).
    Admin(AdminAccess),
    /// Anything done in a client's name: subscribe, mutate its interest.
    ActAs(&'a ClientId),
    /// `/api/tx/submit` — names no client; needs a principal that may write.
    SubmitTx,
    /// `/minibf/*` — a read API over PUBLIC chain data. Its own case rather
    /// than `Admin(Read)` because it is consumed by workers that are not
    /// subscribers at all: `wallet-viewer` calls it with the same
    /// `MITOS_AUTH_TOKEN` secret the companions use. Folding it into the admin
    /// API would mean either 403-ing that worker the day its secret is rotated
    /// to a namespace token, or leaving it holding the operator token — the
    /// one that evicts modules — for ever.
    ReadChain,
    /// `POST /_admin/modules/{id}/recapture`. Its own case because a WORKER
    /// does this: `holder-map`'s production cron recaptures
    /// `holder-distribution` with the same secret it subscribes with.
    ///
    /// Recapture is a WHOLE-MODULE operation today — it wipes the module's
    /// bootstrap flags and restarts it, so every client of the module is
    /// disturbed, in every namespace. A principal may therefore request it
    /// only if no namespace is more protected than its own: see
    /// [`Denied::WholeModuleOpFromLowerTier`]. When recapture becomes
    /// client-scoped (it must, for `Refill::Recapture`) this case narrows to
    /// "its own clients" and the tier rule goes away.
    RecaptureModule,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Granted {
    Plain,
    /// Granted, and the host must log it at `warn` with the client id: the
    /// operator token is being used as a client credential.
    OperatorTokenUsedAsClient,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Denied {
    #[error("the read-only token may only read the admin API")]
    ReadOnlyToken,
    #[error("a namespace token has no access to the admin API")]
    NamespaceTokenOnAdmin,
    #[error("client `{client}` does not belong to namespace `{namespace}`")]
    ClientNotInNamespace {
        client: ClientId,
        namespace: Namespace,
    },
    #[error("client `{0}` is not listed under any namespace")]
    UnknownClient(ClientId),
    #[error("the operator token is no longer accepted as a client credential")]
    OperatorTokenRefused,
    #[error(
        "namespace `{0}` may not run a whole-module operation: it would disturb clients of a \
         more protected namespace"
    )]
    WholeModuleOpFromLowerTier(Namespace),
}

impl Registry {
    pub fn configured(
        namespaces: Vec<NamespaceDef>,
        operator_token_as_client: OperatorTokenAsClient,
    ) -> Result<Self, RegistryError> {
        let mut names = BTreeSet::new();
        let mut owners: Vec<(&ClientId, &Namespace)> = Vec::new();
        for def in &namespaces {
            if !names.insert(&def.name) {
                return Err(RegistryError::DuplicateNamespace(def.name.clone()));
            }
            let ClientSet::Only(clients) = &def.clients else {
                return Err(RegistryError::AnyInConfiguredRegistry(def.name.clone()));
            };
            for client in clients {
                if let Some((_, first)) = owners.iter().find(|(c, _)| *c == client) {
                    return Err(RegistryError::ClientInTwoNamespaces {
                        client: client.clone(),
                        first: (*first).clone(),
                        second: def.name.clone(),
                    });
                }
                owners.push((client, &def.name));
            }
        }
        Ok(Self {
            namespaces,
            operator_token_as_client,
        })
    }

    /// A host with no namespace config: one implicit namespace owning every
    /// client, reachable only with the operator token — exactly v2.
    pub fn legacy(implicit: Namespace, profile: NamespaceProfile) -> Self {
        Self {
            namespaces: vec![NamespaceDef {
                name: implicit,
                clients: ClientSet::Any,
                profile,
            }],
            // There are no namespace tokens to rotate TO, so refusing the
            // operator token here would refuse every client there is.
            operator_token_as_client: OperatorTokenAsClient::AllowWithWarning,
        }
    }

    pub fn namespaces(&self) -> &[NamespaceDef] {
        &self.namespaces
    }

    /// Where the rollout has got to. Read by the host purely to REPORT it on
    /// `/_admin/status` — the decision it drives stays in [`Self::authorize`],
    /// so an operator watching the rollout and the rule being enforced can
    /// never disagree.
    pub fn operator_token_as_client(&self) -> OperatorTokenAsClient {
        self.operator_token_as_client
    }

    /// The namespace that owns `client`. A client id belongs to AT MOST one
    /// namespace ([`RegistryError::ClientInTwoNamespaces`]), which is what
    /// lets the on-disk `companions/<client_id>/` layout stay as it is: the
    /// namespace is derivable, so it never needs to be in a path.
    pub fn namespace_of(&self, client: &ClientId) -> Option<&NamespaceDef> {
        self.namespaces.iter().find(|def| match &def.clients {
            ClientSet::Only(clients) => clients.contains(client),
            ClientSet::Any => true,
        })
    }

    pub fn client_key(&self, client: &ClientId) -> Option<ClientKey> {
        self.namespace_of(client).map(|def| ClientKey {
            namespace: def.name.clone(),
            client: client.clone(),
        })
    }

    fn is_legacy(&self) -> bool {
        matches!(
            self.namespaces.as_slice(),
            [NamespaceDef {
                clients: ClientSet::Any,
                ..
            }]
        )
    }

    pub fn authorize(
        &self,
        principal: &Principal,
        attempt: Attempt<'_>,
    ) -> Result<Granted, Denied> {
        match (principal, attempt) {
            // Chain data is public; any authenticated principal may read it.
            // First, so the read-only arm below cannot shadow it.
            (_, Attempt::ReadChain) => Ok(Granted::Plain),

            (
                Principal::Operator(OperatorScope::Full),
                Attempt::Admin(_) | Attempt::SubmitTx | Attempt::RecaptureModule,
            ) => Ok(Granted::Plain),
            (Principal::Operator(OperatorScope::ReadOnly), Attempt::Admin(AdminAccess::Read)) => {
                Ok(Granted::Plain)
            }
            (Principal::Operator(OperatorScope::ReadOnly), _) => Err(Denied::ReadOnlyToken),

            (Principal::Operator(OperatorScope::Full), Attempt::ActAs(client)) => {
                if self.namespace_of(client).is_none() {
                    return Err(Denied::UnknownClient(client.clone()));
                }
                match self.operator_token_as_client {
                    // In legacy mode this IS the client credential, so there
                    // is nothing to warn about.
                    OperatorTokenAsClient::AllowWithWarning if self.is_legacy() => {
                        Ok(Granted::Plain)
                    }
                    OperatorTokenAsClient::AllowWithWarning => {
                        Ok(Granted::OperatorTokenUsedAsClient)
                    }
                    OperatorTokenAsClient::Refuse => Err(Denied::OperatorTokenRefused),
                }
            }

            (Principal::Namespace(_), Attempt::Admin(_)) => Err(Denied::NamespaceTokenOnAdmin),
            (Principal::Namespace(namespace), Attempt::RecaptureModule) => {
                // `Tier` orders by protection, most protected first. A
                // namespace that is not AT the top would be disturbing
                // someone above it.
                let top = self.namespaces.iter().map(|d| d.profile.tier).min();
                let own = self
                    .namespaces
                    .iter()
                    .find(|d| d.name == *namespace)
                    .map(|d| d.profile.tier);
                match (own, top) {
                    (Some(own), Some(top)) if own == top => Ok(Granted::Plain),
                    _ => Err(Denied::WholeModuleOpFromLowerTier(namespace.clone())),
                }
            }
            (Principal::Namespace(_), Attempt::SubmitTx) => Ok(Granted::Plain),
            (Principal::Namespace(namespace), Attempt::ActAs(client)) => {
                match self.namespace_of(client) {
                    Some(owner) if owner.name == *namespace => Ok(Granted::Plain),
                    Some(_) => Err(Denied::ClientNotInNamespace {
                        client: client.clone(),
                        namespace: namespace.clone(),
                    }),
                    None => Err(Denied::UnknownClient(client.clone())),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::fixtures::{dev, prod};

    fn ns(name: &str) -> Namespace {
        Namespace::parse(name).unwrap()
    }

    fn client(id: &str) -> ClientId {
        ClientId::parse(id).unwrap()
    }

    fn def(name: &str, clients: &[&str], profile: NamespaceProfile) -> NamespaceDef {
        NamespaceDef {
            name: ns(name),
            clients: ClientSet::Only(clients.iter().map(|c| client(c)).collect()),
            profile,
        }
    }

    /// The registry the mainnet host would run, from the subscriber census
    /// taken on the box 2026-09-19.
    fn mainnet(mode: OperatorTokenAsClient) -> Registry {
        Registry::configured(
            vec![
                def(
                    "prod",
                    &[
                        "ownership.cnft.dev",
                        "jpgsm.cnft.dev",
                        "hooks.epochify.space",
                    ],
                    prod(),
                ),
                def(
                    "dev",
                    &["ownership.dev.cnft.dev", "hooks.dev.epochify.space"],
                    dev(),
                ),
            ],
            mode,
        )
        .unwrap()
    }

    const WARN: OperatorTokenAsClient = OperatorTokenAsClient::AllowWithWarning;

    /// THE property: a dev token cannot act as a prod client — not to
    /// subscribe as it, and not to rewrite its interest set.
    #[test]
    fn a_dev_token_cannot_act_as_a_prod_client() {
        let registry = mainnet(WARN);
        let dev_token = Principal::Namespace(ns("dev"));
        let prod_client = client("ownership.cnft.dev");
        assert_eq!(
            registry.authorize(&dev_token, Attempt::ActAs(&prod_client)),
            Err(Denied::ClientNotInNamespace {
                client: prod_client,
                namespace: ns("dev"),
            })
        );
        assert_eq!(
            registry.authorize(
                &dev_token,
                Attempt::ActAs(&client("ownership.dev.cnft.dev"))
            ),
            Ok(Granted::Plain)
        );
    }

    #[test]
    fn a_namespace_token_never_reaches_the_admin_api() {
        let registry = mainnet(WARN);
        for namespace in ["prod", "dev"] {
            for access in [AdminAccess::Read, AdminAccess::Mutate] {
                assert_eq!(
                    registry
                        .authorize(&Principal::Namespace(ns(namespace)), Attempt::Admin(access)),
                    Err(Denied::NamespaceTokenOnAdmin),
                    "even a prod worker's token must not be able to evict a module"
                );
            }
        }
    }

    #[test]
    fn an_unlisted_client_is_refused_not_adopted() {
        let registry = mainnet(WARN);
        let stranger = client("preview-1234.cnft.dev");
        for principal in [
            Principal::Namespace(ns("dev")),
            Principal::Namespace(ns("prod")),
            Principal::Operator(OperatorScope::Full),
        ] {
            assert_eq!(
                registry.authorize(&principal, Attempt::ActAs(&stranger)),
                Err(Denied::UnknownClient(stranger.clone()))
            );
        }
    }

    #[test]
    fn the_operator_token_as_a_client_credential_warns_then_is_refused() {
        let full = Principal::Operator(OperatorScope::Full);
        let c = client("ownership.cnft.dev");
        assert_eq!(
            mainnet(WARN).authorize(&full, Attempt::ActAs(&c)),
            Ok(Granted::OperatorTokenUsedAsClient)
        );
        assert_eq!(
            mainnet(OperatorTokenAsClient::Refuse).authorize(&full, Attempt::ActAs(&c)),
            Err(Denied::OperatorTokenRefused)
        );
        // Refusing it as a CLIENT credential never touches the admin API.
        assert_eq!(
            mainnet(OperatorTokenAsClient::Refuse)
                .authorize(&full, Attempt::Admin(AdminAccess::Mutate)),
            Ok(Granted::Plain)
        );
    }

    #[test]
    fn the_read_only_token_reads_the_admin_api_and_nothing_else() {
        let registry = mainnet(WARN);
        let ro = Principal::Operator(OperatorScope::ReadOnly);
        assert_eq!(
            registry.authorize(&ro, Attempt::Admin(AdminAccess::Read)),
            Ok(Granted::Plain)
        );
        for attempt in [
            Attempt::Admin(AdminAccess::Mutate),
            Attempt::SubmitTx,
            Attempt::ActAs(&client("ownership.cnft.dev")),
        ] {
            assert_eq!(registry.authorize(&ro, attempt), Err(Denied::ReadOnlyToken));
        }
    }

    /// A host with no namespace config must behave EXACTLY as v2: one token,
    /// any client id, no warnings.
    #[test]
    fn legacy_mode_is_v2() {
        let registry = Registry::legacy(ns("prod"), prod());
        let full = Principal::Operator(OperatorScope::Full);
        for id in ["ownership.cnft.dev", "anything-at-all", "preview-99"] {
            assert_eq!(
                registry.authorize(&full, Attempt::ActAs(&client(id))),
                Ok(Granted::Plain)
            );
            assert_eq!(
                registry.client_key(&client(id)).unwrap().namespace,
                ns("prod")
            );
        }
        assert_eq!(
            registry.authorize(&full, Attempt::Admin(AdminAccess::Mutate)),
            Ok(Granted::Plain)
        );
    }

    /// `wallet-viewer` is not a subscriber — it is in no namespace's client
    /// list — and reads `/minibf` with the same secret the companions use. It
    /// must keep working once that secret holds a namespace token.
    #[test]
    fn a_worker_that_only_reads_chain_data_needs_no_client_identity() {
        let registry = mainnet(OperatorTokenAsClient::Refuse);
        for principal in [
            Principal::Namespace(ns("prod")),
            Principal::Namespace(ns("dev")),
            Principal::Operator(OperatorScope::ReadOnly),
            Principal::Operator(OperatorScope::Full),
        ] {
            assert_eq!(
                registry.authorize(&principal, Attempt::ReadChain),
                Ok(Granted::Plain),
                "{principal:?}"
            );
        }
    }

    /// `holder-map`'s production cron recaptures `holder-distribution` with
    /// the secret it subscribes with, so a prod namespace token must be able
    /// to. A dev token must NOT: recapture is whole-module, and it would make
    /// production's companions drop their state.
    #[test]
    fn a_whole_module_recapture_needs_the_most_protected_tier() {
        let registry = mainnet(OperatorTokenAsClient::Refuse);
        assert_eq!(
            registry.authorize(&Principal::Namespace(ns("prod")), Attempt::RecaptureModule),
            Ok(Granted::Plain)
        );
        assert_eq!(
            registry.authorize(&Principal::Namespace(ns("dev")), Attempt::RecaptureModule),
            Err(Denied::WholeModuleOpFromLowerTier(ns("dev")))
        );
        // A host with ONLY development namespaces has nobody above dev to
        // disturb, so dev is the top tier there.
        let dev_only = Registry::configured(vec![def("dev", &["d"], dev())], WARN).unwrap();
        assert_eq!(
            dev_only.authorize(&Principal::Namespace(ns("dev")), Attempt::RecaptureModule),
            Ok(Granted::Plain)
        );
        // A token for a namespace the registry does not define is no tier.
        assert_eq!(
            registry.authorize(&Principal::Namespace(ns("ghost")), Attempt::RecaptureModule),
            Err(Denied::WholeModuleOpFromLowerTier(ns("ghost")))
        );
    }

    #[test]
    fn a_client_id_belongs_to_at_most_one_namespace() {
        let err = Registry::configured(
            vec![
                def("prod", &["shared.cnft.dev"], prod()),
                def("dev", &["shared.cnft.dev"], dev()),
            ],
            WARN,
        );
        assert_eq!(
            err,
            Err(RegistryError::ClientInTwoNamespaces {
                client: client("shared.cnft.dev"),
                first: ns("prod"),
                second: ns("dev"),
            })
        );
    }

    #[test]
    fn a_catch_all_namespace_is_refused_in_a_configured_registry() {
        let catch_all = NamespaceDef {
            name: ns("prod"),
            clients: ClientSet::Any,
            profile: prod(),
        };
        assert_eq!(
            Registry::configured(vec![catch_all, def("dev", &["d"], dev())], WARN),
            Err(RegistryError::AnyInConfiguredRegistry(ns("prod")))
        );
        assert_eq!(
            Registry::configured(
                vec![def("prod", &["a"], prod()), def("prod", &["b"], prod())],
                WARN
            ),
            Err(RegistryError::DuplicateNamespace(ns("prod")))
        );
    }

    /// Every (principal, attempt) pair, so a new variant of either cannot be
    /// added without deciding it. The decision table IS the specification.
    #[test]
    fn the_whole_decision_table() {
        use AdminAccess::{Mutate, Read};
        let registry = mainnet(WARN);
        let own = client("ownership.dev.cnft.dev");
        let other = client("ownership.cnft.dev");
        let full = Principal::Operator(OperatorScope::Full);
        let ro = Principal::Operator(OperatorScope::ReadOnly);
        let dev_ns = Principal::Namespace(ns("dev"));

        let granted = |p: &Principal, a: Attempt<'_>| registry.authorize(p, a).is_ok();
        let table = [
            (&full, Attempt::Admin(Read), true),
            (&full, Attempt::Admin(Mutate), true),
            (&full, Attempt::SubmitTx, true),
            (&full, Attempt::ReadChain, true),
            (&full, Attempt::ActAs(&own), true),
            (&ro, Attempt::Admin(Read), true),
            (&ro, Attempt::Admin(Mutate), false),
            (&ro, Attempt::SubmitTx, false),
            (&ro, Attempt::ReadChain, true),
            (&ro, Attempt::ActAs(&own), false),
            (&dev_ns, Attempt::Admin(Read), false),
            (&dev_ns, Attempt::Admin(Mutate), false),
            (&dev_ns, Attempt::SubmitTx, true),
            (&dev_ns, Attempt::ReadChain, true),
            (&dev_ns, Attempt::ActAs(&own), true),
            (&dev_ns, Attempt::ActAs(&other), false),
            (&full, Attempt::RecaptureModule, true),
            (&ro, Attempt::RecaptureModule, false),
            (&dev_ns, Attempt::RecaptureModule, false),
            (
                &Principal::Namespace(ns("prod")),
                Attempt::RecaptureModule,
                true,
            ),
        ];
        for (principal, attempt, expected) in table {
            assert_eq!(
                granted(principal, attempt),
                expected,
                "{principal:?} attempting {attempt:?}"
            );
        }
    }

    /// The example file a work order hands to its implementer, and the file an
    /// operator will copy. If it stops parsing into these types, this fails —
    /// so the documentation cannot drift from the code.
    #[test]
    fn the_documented_example_parses() {
        /// The host-side shape: it carries `token_env`, which the pure types
        /// deliberately do not.
        #[derive(Deserialize)]
        struct File {
            operator_token_as_client: OperatorTokenAsClient,
            namespace: Vec<Entry>,
        }
        #[derive(Deserialize)]
        struct Entry {
            name: Namespace,
            token_env: String,
            clients: BTreeSet<ClientId>,
            profile: NamespaceProfile,
        }

        let file: File = toml::from_str(include_str!(
            "../../../docs/work-orders/WO-02-namespaces.example.toml"
        ))
        .expect("the documented example must parse");

        assert_eq!(file.operator_token_as_client, WARN);
        assert!(file.namespace.iter().all(|e| !e.token_env.is_empty()));
        let registry = Registry::configured(
            file.namespace
                .into_iter()
                .map(|e| NamespaceDef {
                    name: e.name,
                    clients: ClientSet::Only(e.clients),
                    profile: e.profile,
                })
                .collect(),
            file.operator_token_as_client,
        )
        .expect("the documented example must be a valid registry");

        // The example's profiles ARE the D9 table.
        let of = |name: &str| {
            registry
                .namespaces()
                .iter()
                .find(|d| d.name == ns(name))
                .unwrap()
                .profile
        };
        assert_eq!(of("prod"), prod());
        assert_eq!(of("dev"), dev());
    }
}
