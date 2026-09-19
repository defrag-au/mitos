//! What a module DECLARES about its own lifecycle, and what a build EMITS for
//! a worker to announce — D6 and D8, the artifact's half.
//!
//! Two types, used at three points:
//!
//! - [`LifecycleSection`] is written by the AUTHOR in the module's `.toml`,
//!   copied verbatim by `mitos-build` into the manifest, and read by the HOST.
//!   One type for all three, so they cannot drift.
//! - [`BuiltModule`] is emitted by `mitos-build` beside the artifact. A worker
//!   embeds it and turns it into the [`ModuleRef`] it announces on subscribe.
//!
//! # Why `[lifecycle]` is ONE table
//!
//! `mitos-build` ships the module's whole `.toml` to the module as
//! `config.cbor`, and since instance-keyed storage the config's sha is part of
//! INSTANCE IDENTITY. Build metadata left in there would (a) reach the
//! module's own `Config` deserialiser, and (b) make bumping `revision` change
//! the config sha — forking a new instance whose code and config are
//! byte-identical. So `mitos-build` strips this table, exactly as it strips
//! `[deps]`. Grouping the keys under one name makes that a single `remove`, and
//! means a module is free to have a config key of its own called `revision`.

use serde::{Deserialize, Serialize};

use crate::client_move::ModuleRef;
use crate::ids::{ModuleId, Revision, Sha256Hex};
use crate::state_prep::StateSchema;
use crate::upgrade::{self, LedgerError, UpgradeEntry};

/// The table `mitos-build` strips from `config.cbor`. A constant so the
/// builder, the host and the tests cannot disagree on the spelling.
pub const LIFECYCLE_TABLE: &str = "lifecycle";

/// `[lifecycle]` in a module's `.toml` and in its manifest.
///
/// ```toml
/// [lifecycle]
/// revision = 5
/// state_schema = 2
///
/// [[lifecycle.upgrade]]
/// since = 3
/// actions = ["recapture"]
/// ```
///
/// Every field defaults, and the default — revision 0, schema 0, no upgrades —
/// is what every artifact built before this existed IS. A move between two
/// revision-0 artifacts crosses nothing, so it runs no actions: exactly v2's
/// behaviour, which is what makes this safe to deploy ahead of the modules
/// that use it.
///
/// **Unknown keys are REFUSED**, here and in [`UpgradeEntry`]. Because every
/// field defaults, a typo would otherwise be silent in the worst way:
/// `revison = 5` parses as revision 0, the author believes they bumped it, and
/// no client ever runs their upgrade actions. Same rule as an unknown action —
/// unverifiable is not fine. It also means a host that predates some FUTURE
/// key refuses a manifest that uses it rather than quietly ignoring semantics
/// it does not understand. (A host that predates this whole section is
/// unaffected: `Manifest` itself does not deny unknown fields, so it never
/// looks inside `[lifecycle]` at all — that is what keeps a host rollback
/// safe.)
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleSection {
    #[serde(default)]
    pub revision: Revision,
    #[serde(default)]
    pub state_schema: StateSchema,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub upgrade: Vec<UpgradeEntry>,
}

impl LifecycleSection {
    /// True for an artifact that declares nothing — so a manifest written for
    /// one serialises exactly as it did before this section existed.
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }

    /// Manifest-load validation. A section that fails is a REFUSED module, at
    /// build time (`mitos-build`) and again at the host — an artifact is not
    /// trusted because a builder once accepted it.
    pub fn validate(&self) -> Result<(), LedgerError> {
        upgrade::validate(&self.upgrade, self.revision)
    }
}

/// What `mitos-build` writes beside an artifact, as `built-module.json`.
///
/// It has no URL: the builder does not know where the worker will serve the
/// artifact from. The worker supplies that — see [`BuiltModule::module_ref`].
///
/// `manifest` is here because the MANIFEST must be pinned too. The wasm's sha
/// pins the code, but the manifest carries the `[[lifecycle.upgrade]]` actions
/// and the `[interest]` set, and a host that fetched an unpinned manifest
/// could be handed one with `recapture` quietly removed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuiltModule {
    pub module: ModuleId,
    pub wasm: Sha256Hex,
    pub manifest: Sha256Hex,
    /// `None` when the module has no config — which the host distinguishes
    /// from an EMPTY config, and which is part of instance identity.
    pub config: Option<Sha256Hex>,
    pub revision: Revision,
}

impl BuiltModule {
    /// The pointer a worker announces, given the base URL it serves its own
    /// artifacts from. Layout mirrors the host's CAS: the wasm and its
    /// manifest sit side by side, named by the wasm's sha.
    ///
    /// ```text
    /// <base>/.well-known/mitos/<module>/<wasm-sha>.wasm
    /// <base>/.well-known/mitos/<module>/<wasm-sha>.manifest.toml
    /// ```
    pub fn module_ref(&self, base_url: &str) -> ModuleRef {
        ModuleRef {
            url: self.artifact_url(base_url),
            wasm: self.wasm.clone(),
            manifest: self.manifest.clone(),
            config: self.config.clone(),
            revision: self.revision,
        }
    }

    pub fn artifact_url(&self, base_url: &str) -> String {
        format!("{}.wasm", self.stem(base_url))
    }

    pub fn manifest_url(&self, base_url: &str) -> String {
        format!("{}.manifest.toml", self.stem(base_url))
    }

    fn stem(&self, base_url: &str) -> String {
        format!(
            "{}/.well-known/mitos/{}/{}",
            base_url.trim_end_matches('/'),
            self.module,
            self.wasm,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upgrade::UpgradeAction;

    #[derive(Deserialize)]
    struct ModuleToml {
        #[serde(default)]
        lifecycle: LifecycleSection,
    }

    fn parse(text: &str) -> LifecycleSection {
        toml::from_str::<ModuleToml>(text)
            .expect("module toml parses")
            .lifecycle
    }

    #[test]
    fn the_documented_shape_parses() {
        let section = parse(
            r#"
            abi_version = 2

            [lifecycle]
            revision = 5
            state_schema = 2

            [[lifecycle.upgrade]]
            since = 3
            actions = ["recapture"]

            [[lifecycle.upgrade]]
            since = 5
            actions = ["purge-emissions", "reset-watched"]

            [interest]
            addresses = []
            "#,
        );
        assert_eq!(section.revision, Revision(5));
        assert_eq!(section.state_schema, StateSchema(2));
        assert_eq!(section.upgrade.len(), 2);
        assert!(
            section.upgrade[1]
                .actions
                .contains(&UpgradeAction::ResetWatched)
        );
        assert_eq!(section.validate(), Ok(()));
    }

    /// Every one of the ~16 module `.toml` files that exist today, and every
    /// manifest already in a CAS, has no `[lifecycle]` at all.
    #[test]
    fn a_module_that_declares_nothing_is_a_valid_pre_v3_artifact() {
        let section = parse("abi_version = 2\n[interest]\naddresses = []\n");
        assert!(section.is_default());
        assert_eq!(section.revision, Revision(0));
        assert_eq!(section.state_schema, StateSchema(0));
        assert_eq!(section.validate(), Ok(()));
    }

    #[test]
    fn a_partial_section_defaults_the_rest() {
        let section = parse("[lifecycle]\nrevision = 3\n");
        assert_eq!(section.revision, Revision(3));
        assert_eq!(section.state_schema, StateSchema(0));
        assert!(section.upgrade.is_empty());
    }

    #[test]
    fn an_upgrade_the_artifact_has_not_reached_refuses_it() {
        let section = parse(
            "[lifecycle]\nrevision = 2\n[[lifecycle.upgrade]]\nsince = 3\nactions = [\"resync\"]\n",
        );
        assert_eq!(
            section.validate(),
            Err(LedgerError::SinceAheadOfRevision {
                since: 3,
                revision: 2
            })
        );
        // …which is also why declaring an upgrade WITHOUT a revision fails:
        // the default is 0, and nothing can be "since" before 1.
        let no_revision = parse("[[lifecycle.upgrade]]\nsince = 1\nactions = [\"resync\"]\n");
        assert!(no_revision.validate().is_err());
    }

    #[test]
    fn an_unknown_action_is_a_parse_error_not_a_skipped_entry() {
        let parsed = toml::from_str::<ModuleToml>(
            "[lifecycle]\nrevision = 1\n[[lifecycle.upgrade]]\nsince = 1\nactions = [\"sing\"]\n",
        );
        assert!(parsed.is_err());
    }

    /// Every field defaults, so without `deny_unknown_fields` a typo is silent
    /// in the worst way: the author believes they bumped the revision, the
    /// section parses as revision 0, and nobody ever runs their actions.
    #[test]
    fn a_typo_in_the_section_is_an_error_not_a_silent_default() {
        let typo = toml::from_str::<ModuleToml>("[lifecycle]\nrevison = 5\n");
        assert!(typo.is_err(), "`revison` parsed as a default revision 0");

        let singular = toml::from_str::<ModuleToml>(
            "[lifecycle]\nrevision = 1\n[[lifecycle.upgrade]]\nsince = 1\nactions = []\naction = [\"recapture\"]\n",
        );
        assert!(
            singular.is_err(),
            "`action` beside `actions` was ignored, leaving an entry that does nothing"
        );
    }

    /// The strictness is INSIDE `[lifecycle]` only. A module's own config keys
    /// live beside it at the top level and are none of this type's business.
    #[test]
    fn a_modules_own_config_keys_are_untouched_by_that_strictness() {
        let section = parse(
            "abi_version = 2\nrevision = 9\nv2_address = \"addr1…\"\n[lifecycle]\nrevision = 2\n",
        );
        assert_eq!(section.revision, Revision(2));
    }

    /// A manifest for a module that declares nothing must serialise exactly as
    /// it did before — the host skips the section when it is the default, and
    /// this is the predicate it skips on.
    #[test]
    fn a_declared_section_round_trips_and_a_default_one_is_recognised() {
        let declared = parse("[lifecycle]\nrevision = 4\nstate_schema = 1\n");
        assert!(!declared.is_default());
        let text = toml::to_string(&declared).unwrap();
        assert_eq!(toml::from_str::<LifecycleSection>(&text).unwrap(), declared);
        assert!(LifecycleSection::default().is_default());
    }

    fn built() -> BuiltModule {
        BuiltModule {
            module: ModuleId::parse("wayup-store-offer").unwrap(),
            wasm: Sha256Hex::parse(&"ab".repeat(32)).unwrap(),
            manifest: Sha256Hex::parse(&"cd".repeat(32)).unwrap(),
            config: Some(Sha256Hex::parse(&"ef".repeat(32)).unwrap()),
            revision: Revision(5),
        }
    }

    #[test]
    fn a_worker_turns_its_built_module_into_the_ref_it_announces() {
        let b = built();
        let wasm = "ab".repeat(32);
        let expected =
            format!("https://ownership.cnft.dev/.well-known/mitos/wayup-store-offer/{wasm}");
        // A trailing slash on the base is the commonest way to get `//`.
        for base in ["https://ownership.cnft.dev", "https://ownership.cnft.dev/"] {
            let module_ref = b.module_ref(base);
            assert_eq!(module_ref.url, format!("{expected}.wasm"));
            assert_eq!(b.manifest_url(base), format!("{expected}.manifest.toml"));
            assert_eq!(module_ref.wasm, b.wasm);
            assert_eq!(module_ref.manifest, b.manifest);
            assert_eq!(module_ref.config, b.config);
            assert_eq!(module_ref.revision, Revision(5));
        }
    }

    /// `built-module.json` is read by a WORKER, possibly in another repo, so
    /// its field names and order are a contract. Pinned as TEXT, not by round
    /// trip — a round trip passes however the fields are renamed. (TOML here
    /// because this crate has no JSON dependency; serde gives both formats the
    /// same names, and `mitos-build` pins the JSON rendering itself.)
    #[test]
    fn the_emitted_field_names_are_pinned() {
        let expected = format!(
            "module = \"wayup-store-offer\"\nwasm = \"{}\"\nmanifest = \"{}\"\nconfig = \"{}\"\nrevision = 5\n",
            "ab".repeat(32),
            "cd".repeat(32),
            "ef".repeat(32),
        );
        assert_eq!(toml::to_string(&built()).unwrap(), expected);
    }
}
