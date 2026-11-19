//! The address registry as mitos reads it.
//!
//! Two tables, one loader: what a script **is** (`resolved/behaviour.bin`, keyed
//! by payment credential) and who runs an ordinary wallet
//! (`resolved/stake-services.bin`, keyed by stake credential). Both are authored
//! in `theyexist/address-registry`; this crate only parses and indexes them.
//!
//! No I/O. A native tool reads or fetches the two files and hands the bytes here;
//! a wasm module embeds them. That split is what lets one set of lookups serve a
//! walk, a worker and a game without either reimplementing the registry.
//!
//! The index is built once, at load: every address any use names (its own and
//! every `also_at` form) maps to that use, so "what is this address" is a lookup
//! rather than a scan. A credential may carry more than one use — one contract
//! can do more than one job — and the type says so.

pub use address_registry_schema;
pub use address_registry_schema::{
    Behaviour, Category, Contract, DerivedFrom, MarketplacePurpose, Shape, StakeLabel, StakeLabels,
    StakeServiceKind,
};
use address_registry_schema::{BehaviourError, StakeTableError};
use std::collections::BTreeMap;
use std::fmt;

/// Why the bytes are not a registry.
#[derive(Debug)]
pub enum RegistryError {
    Behaviour(BehaviourError),
    Stakes(StakeTableError),
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RegistryError::Behaviour(error) => write!(f, "behaviour table: {error}"),
            RegistryError::Stakes(error) => write!(f, "stake-service table: {error}"),
        }
    }
}

impl std::error::Error for RegistryError {}

impl From<BehaviourError> for RegistryError {
    fn from(error: BehaviourError) -> Self {
        RegistryError::Behaviour(error)
    }
}

impl From<StakeTableError> for RegistryError {
    fn from(error: StakeTableError) -> Self {
        RegistryError::Stakes(error)
    }
}

/// The registry, indexed for the questions a consumer asks.
#[derive(Debug)]
pub struct Registry {
    behaviour: Behaviour,
    stakes: StakeLabels,
    /// Every address a use is registered at -> (its credential, its index in
    /// that credential's uses).
    by_address: BTreeMap<String, (String, usize)>,
}

impl Default for Registry {
    fn default() -> Self {
        Self::empty()
    }
}

impl Registry {
    /// The two artefacts, parsed. A wrong magic or an unknown format is an
    /// error, never a partial registry: a consumer that half-reads a table is
    /// answering from a registry nobody published.
    pub fn from_bytes(behaviour: &[u8], stakes: &[u8]) -> Result<Self, RegistryError> {
        Ok(Self::from_tables(
            Behaviour::from_bytes(behaviour)?,
            StakeLabels::from_bytes(stakes)?,
        ))
    }

    /// The registry embedded at build time from `data/`.
    ///
    /// For a consumer that cannot fetch — the wasm module — and for tests that
    /// must be offline. The bytes are a pinned copy of the registry repository's
    /// `resolved/`; `data/README.md` records the rev, both digests, and how to
    /// refresh them.
    #[cfg(feature = "pinned")]
    pub fn pinned() -> Self {
        match Self::from_bytes(
            include_bytes!("../data/behaviour.bin"),
            include_bytes!("../data/stake-services.bin"),
        ) {
            Ok(registry) => registry,
            Err(error) => panic!("the pinned registry artefacts do not decode: {error}"),
        }
    }

    /// Tables a caller already holds — a test, or a consumer that decoded them
    /// elsewhere.
    pub fn from_tables(behaviour: Behaviour, stakes: StakeLabels) -> Self {
        let mut by_address = BTreeMap::new();
        for (credential, uses) in &behaviour.contracts {
            for (index, use_of) in uses.iter().enumerate() {
                for address in use_of.addresses() {
                    by_address
                        .entry(address.to_string())
                        .or_insert_with(|| (credential.clone(), index));
                }
            }
        }
        Self {
            behaviour,
            stakes,
            by_address,
        }
    }

    /// A consumer that holds only the behaviour file — the common case — and
    /// has no stake table to give it.
    pub fn from_behaviour(behaviour: Behaviour) -> Self {
        Self::from_tables(
            behaviour,
            StakeLabels {
                entries: Vec::new(),
            },
        )
    }

    /// A registry with nothing in it. Honest for a caller with no table — every
    /// lookup misses, and `is_empty` says so — rather than an error at every
    /// call site.
    pub fn empty() -> Self {
        Self {
            behaviour: Behaviour {
                format: address_registry_schema::FORMAT,
                generated_at: String::new(),
                contracts: BTreeMap::new(),
            },
            stakes: StakeLabels {
                entries: Vec::new(),
            },
            by_address: BTreeMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.behaviour.contracts.is_empty() && self.stakes.entries.is_empty()
    }

    pub fn behaviour(&self) -> &Behaviour {
        &self.behaviour
    }

    pub fn stakes(&self) -> &StakeLabels {
        &self.stakes
    }

    /// Every use authored for a payment credential. Empty when nobody stated
    /// one.
    pub fn uses(&self, credential: &str) -> &[Contract] {
        self.behaviour.get(credential)
    }

    /// The use registered at exactly this address, with the credential it
    /// belongs to. Exact, not by payment credential: jpg.store's V1 sale and
    /// collection-offer contracts are one script under two delegation forms, and
    /// only the address tells them apart.
    pub fn at_address(&self, address: &str) -> Option<(&str, &Contract)> {
        let (credential, index) = self.by_address.get(address)?;
        let use_of = self.uses(credential).get(*index)?;
        Some((credential.as_str(), use_of))
    }

    /// The service a stake key belongs to, by its bech32 address.
    pub fn stake(&self, stake: &str) -> Option<&StakeLabel> {
        self.stakes.get(stake)
    }

    /// The same lookup for a consumer already holding the 29 bytes.
    pub fn stake_bytes(&self, stake: &[u8; 29]) -> Option<&StakeLabel> {
        self.stakes.get_bytes(stake)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use address_registry_schema::{MarketplacePurpose, StakeServiceKind};

    const JPG_V1: &str = "9068a7a3f008803edac87af1619860f2cdcde40c26987325ace138ad";
    const SALE: &str = "addr1zxgx3far7qygq0k6epa0zcvcvrevmn0ypsnfsue94nsn3tvpw288a4x0xf8pxgcntelxmyclq83s0ykeehchz2wtspks905plm";
    const OFFER: &str = "addr1xxgx3far7qygq0k6epa0zcvcvrevmn0ypsnfsue94nsn3tfvjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8eks2utwdd";
    const ANVIL: &str = "stake1uy50zl7a9k9c74v66c0gn833at5sh83qnjldk8hg4rrv05g3mmskr";

    fn lock() -> Contract {
        Contract {
            category: Category::Lock,
            purpose: None,
            market: None,
            shape: None,
            datum_example: None,
            label: Some("TSURU Lock".to_string()),
            project: Some("TSURU".to_string()),
            source: "test".to_string(),
            derived_from: DerivedFrom::Address(
                "addr1w8r6kmlsm7gfwnaqrlxhjufnv7gjqlz82vj0rv5gukqguhcs9tj6j".to_string(),
            ),
            also_at: Vec::new(),
        }
    }

    fn marketplace(purpose: MarketplacePurpose, address: &str) -> Contract {
        Contract {
            category: Category::Marketplace,
            purpose: Some(purpose),
            market: Some("jpg.store".to_string()),
            shape: None,
            datum_example: None,
            label: Some("JPG.store".to_string()),
            project: None,
            source: "test".to_string(),
            derived_from: DerivedFrom::Address(address.to_string()),
            also_at: Vec::new(),
        }
    }

    fn registry() -> Registry {
        let behaviour = Behaviour {
            format: address_registry_schema::FORMAT,
            generated_at: "2026-11-19T00:00:00Z".to_string(),
            contracts: BTreeMap::from([
                (
                    JPG_V1.to_string(),
                    vec![
                        marketplace(MarketplacePurpose::Sale, SALE),
                        marketplace(MarketplacePurpose::Offer, OFFER),
                    ],
                ),
                (
                    "c7ab6ff0df90974fa01fcd7971336791207c475324f1b288e5808e5f".to_string(),
                    vec![lock()],
                ),
            ]),
        };
        let stakes = StakeLabels {
            entries: vec![StakeLabel {
                stake: address_registry_schema::stake::stake_bytes(ANVIL).expect("a stake address"),
                kind: StakeServiceKind::MintingProvider,
                label: "Anvil".to_string(),
            }],
        };
        Registry::from_tables(behaviour, stakes)
    }

    #[test]
    fn an_address_finds_the_use_it_is_registered_at() {
        let registry = registry();
        let (credential, use_of) = registry
            .at_address(OFFER)
            .expect("the offer form is indexed");
        assert_eq!(credential, JPG_V1);
        assert_eq!(use_of.purpose, Some(MarketplacePurpose::Offer));

        // The other delegation form of the SAME credential is a different use.
        let (_, sale) = registry.at_address(SALE).expect("the sale form is indexed");
        assert_eq!(sale.purpose, Some(MarketplacePurpose::Sale));
    }

    #[test]
    fn an_unregistered_address_finds_nothing() {
        let registry = registry();
        assert!(
            registry
                .at_address("addr1w8n8kq3j96v03a3znqqy9f54prt8uf6s4lyj7nuf2cvg2ucnwhs68")
                .is_none()
        );
    }

    #[test]
    fn a_credential_keeps_every_use_it_has() {
        let registry = registry();
        assert_eq!(registry.uses(JPG_V1).len(), 2);
        assert_eq!(
            registry.uses("002059fc75b038a85b33ba4fdfaced4bed1247fbad24c538dd45245a"),
            &[]
        );
    }

    #[test]
    fn a_stake_key_finds_its_service() {
        let registry = registry();
        let service = registry.stake(ANVIL).expect("Anvil is registered");
        assert_eq!(service.label, "Anvil");
        assert_eq!(service.kind, StakeServiceKind::MintingProvider);
        assert!(
            registry
                .stake("stake1u8jdj77t835q3gl7pmpjxtl6286awmdvn24ljfddnrf0g6gdvtddv")
                .is_none()
        );
    }

    #[test]
    fn an_empty_registry_misses_every_lookup() {
        let registry = Registry::empty();
        assert!(registry.is_empty());
        assert!(registry.at_address(SALE).is_none());
        assert!(registry.stake(ANVIL).is_none());
        assert_eq!(registry.uses(JPG_V1), &[]);
    }

    #[test]
    fn a_wrong_magic_is_refused_rather_than_half_read() {
        let result = Registry::from_bytes(b"NOPE", b"NOPE");
        assert!(result.is_err(), "garbage is not a registry");
    }

    #[cfg(feature = "pinned")]
    #[test]
    fn the_pinned_artefacts_decode() {
        let registry = Registry::pinned();
        assert!(!registry.is_empty());
        // The burn sink the compiled table used to carry is in the hosted file.
        assert!(
            registry
                .uses("c1b35bb893529376effc4083dc0a0ed90a1c07fe09550885b37aa27f")
                .iter()
                .any(|use_of| use_of.category == Category::Burn)
        );
        // And the stake layer came with it.
        assert!(
            registry
                .stake("stake1uy50zl7a9k9c74v66c0gn833at5sh83qnjldk8hg4rrv05g3mmskr")
                .is_some()
        );
    }
}
