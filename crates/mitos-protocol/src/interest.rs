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
    Dex, DexBrand, DexEventKind, Domain, Lending, LendingBrand, LendingEventKind, Marketplace,
    MarketplaceBrand, MarketplaceEventKind, ProtocolEvent,
};

/// One descent path of consumer interest. Three axes, ANDed at
/// match time. To express disjoint interests, send a `Vec<Interest>`
/// — entries are ORed at match time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Interest {
    pub asset: AssetSelector,
    pub domain: DomainSelector,
    pub value: ValueFilter,
}

impl Interest {
    /// Short-hand for "match everything."
    pub fn any() -> Self {
        Self {
            asset: AssetSelector::Any,
            domain: DomainSelector::Any,
            value: ValueFilter::Any,
        }
    }

    /// Per-event match. AND across the three axes.
    pub fn matches(&self, event: &ProtocolEvent) -> bool {
        self.asset
            .matches(&event.policy_id, event.asset_name_hex.as_deref())
            && self.domain.matches(&event.domain)
            && self.value.matches(event)
    }
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
    Marketplace(MarketplaceSelector),
    Dex(DexSelector),
    Lending(LendingSelector),
}

impl DomainSelector {
    pub fn matches(&self, domain: &Domain) -> bool {
        match (self, domain) {
            (DomainSelector::Any, _) => true,
            (DomainSelector::Marketplace(s), Domain::Marketplace(m)) => s.matches(m),
            (DomainSelector::Dex(s), Domain::Dex(d)) => s.matches(d),
            (DomainSelector::Lending(s), Domain::Lending(l)) => s.matches(l),
            _ => false,
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
