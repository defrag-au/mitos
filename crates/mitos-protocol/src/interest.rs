//! Subscription interest — what a CF consumer says it wants.
//!
//! See `docs/design/SUBSCRIPTION_MECHANICS.md` for the full design.
//! Three orthogonal axes (asset / domain / value) ANDed at match
//! time, with each axis having a canonical "no constraint" form
//! (`Any`) so broad interests collapse to short literals.
//!
//! A subscription is `Vec<Interest>`; an event matches if it
//! matches any one entry. Matching is short-circuit OR over the
//! vec.

use cardano_assets::{AssetId, Fingerprint, PolicyId};
use enumset::EnumSet;
use serde::{Deserialize, Serialize};

use crate::protocol::{
    AssetMovementPayload, AssetRole, BurnPayload, Dex, DexBrand, DexEventKind, Domain, Lending,
    LendingBrand, LendingEventKind, Marketplace, MarketplaceBrand, MarketplaceEventKind,
    MintPayload, ProtocolEvent,
};

/// `EnumSet::all()` as a serde default for the `roles` axis.
/// Kept as a free function so `#[serde(default = ...)]` can name
/// it; absent the explicit default, serde would substitute
/// `EnumSet::default()` which is **empty** — the wrong semantic
/// (would silently match nothing). Old payloads on the wire that
/// predate the `roles` field deserialise as "all roles".
fn all_asset_roles() -> EnumSet<AssetRole> {
    EnumSet::all()
}

/// One descent path of consumer interest. Four axes, ANDed at
/// match time. To express disjoint interests, send a `Vec<Interest>`
/// — entries are ORed at match time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Interest {
    pub asset: AssetSelector,
    /// NFT-centric role filter. `EnumSet::all()` (the serde
    /// default) keeps every role through; a narrower set like
    /// `enum_set!(AssetRole::NativeUser | AssetRole::Cip68User)`
    /// drops reference NFTs and minting-script sentinels server-
    /// side at the indexer.
    ///
    /// For events with no specific asset (collection-wide offers,
    /// multi-asset bundle listings — `event.asset_name_hex ==
    /// None`) the role is undefined and the filter is bypassed:
    /// such events match regardless of `roles`.
    #[serde(default = "all_asset_roles")]
    pub roles: EnumSet<AssetRole>,
    pub domain: DomainSelector,
    pub value: ValueFilter,
}

impl Interest {
    /// Short-hand for "match everything."
    pub fn any() -> Self {
        Self {
            asset: AssetSelector::Any,
            roles: EnumSet::all(),
            domain: DomainSelector::Any,
            value: ValueFilter::Any,
        }
    }

    /// Per-event match. AND across the four axes.
    /// `roles` is bypassed for collection-wide events (no
    /// specific asset → no derivable role).
    pub fn matches(&self, event: &ProtocolEvent) -> bool {
        self.asset
            .matches(&event.policy_id, event.asset_name_hex.as_deref())
            && self.role_matches(event.asset_name_hex.as_deref())
            && self.domain.matches(&event.domain)
            && self.value.matches(event)
    }

    /// Match the asset axis only, plus the role axis. For
    /// state-only indexers (ownership, future tokenisation) whose
    /// Change is not a `ProtocolEvent`. The indexer derives the
    /// role from the asset's name once and passes it in.
    pub fn matches_asset_role(
        &self,
        policy_id: &PolicyId,
        asset_name_hex: Option<&str>,
        role: AssetRole,
    ) -> bool {
        self.asset.matches(policy_id, asset_name_hex) && self.roles.contains(role)
    }

    /// Match the asset axis only (no role check). Retained for
    /// callers that don't have a role projected.
    pub fn matches_asset(&self, policy_id: &PolicyId, asset_name_hex: Option<&str>) -> bool {
        self.asset.matches(policy_id, asset_name_hex)
    }

    fn role_matches(&self, asset_name_hex: Option<&str>) -> bool {
        match asset_name_hex {
            Some(name) => self.roles.contains(AssetRole::from_asset_name_hex(name)),
            None => true,
        }
    }
}

/// OR-fold over a `Vec<Interest>` against a fully-typed event.
/// The canonical "subscription matches" check for indexers whose
/// `Change = ProtocolEvent`.
pub fn any_interest_matches_event(interests: &[Interest], event: &ProtocolEvent) -> bool {
    interests.iter().any(|i| i.matches(event))
}

/// OR-fold for asset-only matching, no role projected. Retained
/// for transitional callers.
pub fn any_interest_matches_asset(
    interests: &[Interest],
    policy_id: &PolicyId,
    asset_name_hex: Option<&str>,
) -> bool {
    interests
        .iter()
        .any(|i| i.matches_asset(policy_id, asset_name_hex))
}

/// OR-fold for state-only indexers that surface the asset's
/// `AssetRole`. The canonical match check for ownership-style
/// changes once role-aware emission lands.
pub fn any_interest_matches_asset_role(
    interests: &[Interest],
    policy_id: &PolicyId,
    asset_name_hex: Option<&str>,
    role: AssetRole,
) -> bool {
    interests
        .iter()
        .any(|i| i.matches_asset_role(policy_id, asset_name_hex, role))
}

/// Project a slice of `Interest`s down to the set of policy_ids
/// they reference, for indexers that maintain a watch-set
/// optimisation (e.g. ownership indexer skipping blocks that touch
/// no watched policy).
///
/// Returns `None` if any interest in the slice is unbounded by
/// policy (`AssetSelector::Any`) — meaning every block must be
/// processed, no watch-set optimisation possible. Otherwise
/// returns `Some(set)` of distinct policies referenced by `Policy`,
/// `Asset`, or `Trait` selectors.
///
/// `Fingerprint` selectors don't constrain by policy
/// (a fingerprint is policy+name combined and hashed), so they
/// also force `None` — the indexer must scan everything and
/// post-filter.
pub fn watched_policies(interests: &[Interest]) -> Option<std::collections::HashSet<PolicyId>> {
    let mut out = std::collections::HashSet::new();
    for i in interests {
        match &i.asset {
            AssetSelector::Any | AssetSelector::Fingerprint(_) => return None,
            AssetSelector::Policy(p) | AssetSelector::Trait { policy: p, .. } => {
                out.insert(p.clone());
            }
            AssetSelector::Asset { policy, .. } => {
                out.insert(policy.clone());
            }
        }
    }
    Some(out)
}

/// Asset-axis selector — equality at the chosen specificity level.
/// `Trait` is reserved for a future metadata index; today it never
/// matches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AssetSelector {
    Any,
    Policy(PolicyId),
    Asset {
        policy: PolicyId,
        name_hex: String,
    },
    Fingerprint(Fingerprint),
    /// Future: trait-level filtering once a metadata-fingerprint
    /// index exists. Today this arm is inert (always `false`).
    Trait {
        policy: PolicyId,
        key: String,
        value: String,
    },
}

impl AssetSelector {
    /// Match against an event's `(policy_id, asset_name_hex)` pair.
    /// `asset_name_hex == None` means the event is policy-wide
    /// (e.g. a collection-wide offer or a multi-asset bundle
    /// listing): `Policy` selectors match it, `Asset` /
    /// `Fingerprint` selectors do not.
    pub fn matches(&self, policy_id: &PolicyId, asset_name_hex: Option<&str>) -> bool {
        match self {
            AssetSelector::Any => true,
            AssetSelector::Policy(p) => p == policy_id,
            AssetSelector::Asset { policy, name_hex } => {
                policy == policy_id && asset_name_hex == Some(name_hex.as_str())
            }
            AssetSelector::Fingerprint(fp) => match asset_name_hex {
                Some(name) => match AssetId::new(policy_id.as_str().to_string(), name.to_string())
                    .ok()
                    .and_then(|a| a.fingerprint_typed().ok())
                {
                    Some(actual) => &actual == fp,
                    None => false,
                },
                None => false,
            },
            AssetSelector::Trait { .. } => false,
        }
    }
}

/// Domain-axis selector — descend into the domain that matters,
/// `Any` at any level matches anything underneath.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DomainSelector {
    Any,
    Mint(MintSelector),
    Burn(BurnSelector),
    AssetMovement(AssetMovementSelector),
    Marketplace(MarketplaceSelector),
    Dex(DexSelector),
    Lending(LendingSelector),
}

impl DomainSelector {
    pub fn matches(&self, domain: &Domain) -> bool {
        match (self, domain) {
            (DomainSelector::Any, _) => true,
            (DomainSelector::Mint(s), Domain::Mint(p)) => s.matches(p),
            (DomainSelector::Burn(s), Domain::Burn(p)) => s.matches(p),
            (DomainSelector::AssetMovement(s), Domain::AssetMovement(p)) => s.matches(p),
            (DomainSelector::Marketplace(s), Domain::Marketplace(m)) => s.matches(m),
            (DomainSelector::Dex(s), Domain::Dex(d)) => s.matches(d),
            (DomainSelector::Lending(s), Domain::Lending(l)) => s.matches(l),
            _ => false,
        }
    }
}

/// Mint-domain selector. v1 ships `Any` only — per-policy and
/// per-asset targeting is handled by the asset axis on `Interest`.
/// Future `Filter { ... }` variants can add sub-axes
/// (`min_amount`, recipient address filters, ...) without
/// breaking the wire format.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MintSelector {
    Any,
}

impl MintSelector {
    pub fn matches(&self, _payload: &MintPayload) -> bool {
        match self {
            MintSelector::Any => true,
        }
    }
}

/// Burn-domain selector. Same shape as `MintSelector`; v1 is
/// `Any` only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BurnSelector {
    Any,
}

impl BurnSelector {
    pub fn matches(&self, _payload: &BurnPayload) -> bool {
        match self {
            BurnSelector::Any => true,
        }
    }
}

/// AssetMovement-domain selector. Same shape; v1 is `Any` only.
/// Future `Filter { ... }` variants could expose `via_script`
/// targeting (when that payload field lands) or address-pair
/// filtering for specific P2P-monitoring use cases.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AssetMovementSelector {
    Any,
}

impl AssetMovementSelector {
    pub fn matches(&self, _payload: &AssetMovementPayload) -> bool {
        match self {
            AssetMovementSelector::Any => true,
        }
    }
}

/// Per-domain selector pattern: either `Any` or an orthogonal
/// `(brands, kinds)` filter. Same shape repeats for every domain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketplaceSelector {
    Any,
    Filter {
        brands: EnumSet<MarketplaceBrand>,
        kinds: EnumSet<MarketplaceEventKind>,
    },
}

impl MarketplaceSelector {
    pub fn matches(&self, ev: &Marketplace) -> bool {
        match self {
            MarketplaceSelector::Any => true,
            MarketplaceSelector::Filter { brands, kinds } => {
                brands.contains(ev.brand()) && kinds.contains(ev.kind())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DexSelector {
    Any,
    Filter {
        brands: EnumSet<DexBrand>,
        kinds: EnumSet<DexEventKind>,
    },
}

impl DexSelector {
    pub fn matches(&self, ev: &Dex) -> bool {
        match self {
            DexSelector::Any => true,
            DexSelector::Filter { brands, kinds } => {
                brands.contains(ev.brand()) && kinds.contains(ev.kind())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LendingSelector {
    Any,
    Filter {
        brands: EnumSet<LendingBrand>,
        kinds: EnumSet<LendingEventKind>,
    },
}

impl LendingSelector {
    pub fn matches(&self, ev: &Lending) -> bool {
        match self {
            LendingSelector::Any => true,
            LendingSelector::Filter { brands, kinds } => {
                brands.contains(ev.brand()) && kinds.contains(ev.kind())
            }
        }
    }
}

/// Value-axis selector — placeholder. Today only `Any` is
/// implemented; CF-side post-receive filtering covers any current
/// need. The type exists so the wire shape doesn't change when a
/// concrete query motivates pushing this server-side.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueFilter {
    Any,
}

impl ValueFilter {
    pub fn matches(&self, _event: &ProtocolEvent) -> bool {
        match self {
            ValueFilter::Any => true,
        }
    }
}
