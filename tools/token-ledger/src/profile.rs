//! A collection profile — WHAT to stream, and where its chain comes from.
//!
//! `token-ledger walk --profile <file>` reads one of these, and
//! `event-firehose --profile <file>` runs that walk. One file, so the viewer does not grow
//! a second copy of the walk's flags and the two cannot drift apart.
//!
//! The `[[collection]]` keys mirror the achievement catalogue
//! (`cnft.dev-workers/config/achievements/perps.toml`): `id`, `kind`, `policy`,
//! `asset-name-hex`. Nothing here scores anything — a profile says what the collections
//! ARE, never what holding them is worth.
//!
//! # One policy per collection, and one collection may be a token
//!
//! A policy id is the only handle the chain gives for "this project". Perps is two of them
//! — the 6,000 NFTs and $PERP — so a profile carries a list, and each collection walks into
//! its own ledger, named after its `id`.
//!
//! # A path in a TOML file is not read by a shell
//!
//! `~/data/mithril` is literal to the parser, so a leading `~/` is expanded here. Anything
//! else is used as given, relative to the working directory the walk runs in.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

/// The only profile shape this binary reads. A number rather than "whatever parses",
/// because a profile is a file a person edits and a later version has to say so.
pub const SCHEMA: u32 = 1;

#[derive(Debug, Deserialize)]
pub struct Profile {
    #[serde(default)]
    schema: u32,
    #[serde(default)]
    walk: Walk,
    #[serde(default)]
    collection: Vec<Collection>,
}

#[derive(Debug, Default, Deserialize)]
struct Walk {
    /// The snapshot root — `<data-dir>/immutable` is what the walk opens.
    #[serde(rename = "data-dir", default)]
    data_dir: Option<PathBuf>,
    /// Each collection's ledger is `<db-dir>/<id>.db`.
    #[serde(rename = "db-dir", default)]
    db_dir: Option<PathBuf>,
    /// The first slot worth decoding. One for the whole profile: the collections of one
    /// project start together, and a collection that minted later simply matches nothing
    /// until it launches.
    #[serde(rename = "from-slot", default)]
    from_slot: Option<u64>,
    /// Cap on in-range blocks, for a smoke run.
    #[serde(rename = "max-blocks", default)]
    max_blocks: Option<u64>,
    /// Memmem each raw block for the policy id and decode only on a hit.
    #[serde(default)]
    sieve: bool,
}

#[derive(Debug, Deserialize)]
pub struct Collection {
    /// Also the ledger's filename, so it is held to one path segment.
    pub id: String,
    pub kind: String,
    pub policy: String,
    /// The policy's first mint, from `~/data/mithril/policies/<policy>.json`.
    ///
    /// It is BOTH where the walk starts and what makes its coverage complete, because
    /// the walk's `floor_slot` is exactly this fact. A collection that states it and starts
    /// there produces `coverage: complete`; one that does not is indistinguishable from a
    /// shallow walk of an unregistered policy, and every event it emits says `partial` no
    /// matter where it started.
    ///
    /// The two collections of one project rarely share it — $PERP minted ten months before
    /// the Perps NFTs — so it belongs here and not on `[walk]`.
    #[serde(rename = "first-mint-slot", default)]
    pub first_mint_slot: Option<u64>,
    /// Which asset under the policy a token collection means. Unused by the walk, which
    /// follows the whole policy; it is the catalogue's identity and the viewer's label.
    #[serde(rename = "asset-name-hex", default)]
    pub asset_name_hex: Option<String>,
}

impl Profile {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading the profile {}", path.display()))?;
        let profile: Self = toml::from_str(&text)
            .with_context(|| format!("parsing the profile {}", path.display()))?;
        profile.check(path)?;
        Ok(profile)
    }

    fn check(&self, path: &Path) -> Result<()> {
        let at = path.display();
        if self.schema != SCHEMA {
            bail!("{at}: schema {} is not {SCHEMA}", self.schema);
        }
        if self.collection.is_empty() {
            bail!("{at}: no [[collection]] blocks — there is nothing to walk");
        }

        let mut seen = BTreeSet::new();
        for collection in &self.collection {
            let id = &collection.id;
            if id.is_empty() || id.contains(['/', '\\']) || id == "." || id == ".." {
                bail!("{at}: collection id {id:?} cannot name a ledger file");
            }
            if !seen.insert(id.as_str()) {
                bail!("{at}: collection id {id:?} appears twice — one ledger each");
            }
            if collection.kind != "nft" && collection.kind != "token" {
                bail!(
                    "{at}: collection {id:?} has kind {:?} — expected `nft` or `token`",
                    collection.kind
                );
            }
            if collection.policy.len() != 56
                || !collection.policy.chars().all(|c| c.is_ascii_hexdigit())
            {
                bail!(
                    "{at}: collection {id:?} policy {:?} is not a 56-hex policy id",
                    collection.policy
                );
            }
            let bad_name = collection.asset_name_hex.as_deref().is_some_and(|name| {
                name.len() % 2 != 0 || !name.chars().all(|c| c.is_ascii_hexdigit())
            });
            if bad_name {
                bail!(
                    "{at}: collection {id:?} asset-name-hex {:?} is not an even-length \
                     hex string",
                    collection.asset_name_hex
                );
            }
        }
        Ok(())
    }

    pub fn collections(&self) -> &[Collection] {
        &self.collection
    }

    pub fn data_dir(&self) -> Option<PathBuf> {
        self.walk.data_dir.as_deref().map(expand)
    }

    pub fn db_dir(&self) -> PathBuf {
        self.walk
            .db_dir
            .as_deref()
            .map(expand)
            .unwrap_or_else(|| PathBuf::from("."))
    }

    pub fn db_path(&self, collection: &Collection) -> PathBuf {
        self.db_dir().join(format!("{}.db", collection.id))
    }

    /// The floor one collection starts at: its own first mint, else the file's floor.
    pub fn floor_of(&self, collection: &Collection) -> Option<u64> {
        collection.first_mint_slot.or(self.walk.from_slot)
    }

    pub fn max_blocks(&self) -> Option<u64> {
        self.walk.max_blocks
    }

    pub fn sieve(&self) -> bool {
        self.walk.sieve
    }
}

/// `~/x` → `$HOME/x`. A TOML file is not read by a shell, so a tilde in one is literal.
fn expand(path: &Path) -> PathBuf {
    let text = path.to_string_lossy();
    let Some(rest) = text.strip_prefix("~/") else {
        return path.to_path_buf();
    };
    match std::env::var_os("HOME") {
        Some(home) => PathBuf::from(home).join(rest),
        None => path.to_path_buf(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PERPS: &str = "e6ba9c0ff27be029442c32533c6efd956a60d15ecb976acbb64c4de0";
    const PERP: &str = "d9d59e71eb99e653a493956a6826a406060706bd8c9a2427b3564f64";

    fn parse(text: &str) -> Result<Profile> {
        let profile: Profile = toml::from_str(text).context("parse")?;
        profile.check(Path::new("profile.toml"))?;
        Ok(profile)
    }

    fn collection(id: &str, policy: &str) -> String {
        format!("[[collection]]\nid = \"{id}\"\nkind = \"nft\"\npolicy = \"{policy}\"\n")
    }

    #[test]
    fn a_profile_carries_its_collections_and_where_to_read_them() {
        let text = format!(
            "schema = 1\n\n[walk]\ndata-dir = \"/snapshot\"\ndb-dir = \"/db\"\n\
             from-slot = 170445182\nsieve = true\n\n{}",
            collection("perps", PERPS)
        );
        let profile = parse(&text).unwrap();

        assert_eq!(profile.collections().len(), 1);
        assert_eq!(profile.data_dir().unwrap(), PathBuf::from("/snapshot"));
        assert_eq!(
            profile.floor_of(&profile.collections()[0]),
            Some(170_445_182)
        );
        assert!(profile.sieve());
        assert_eq!(
            profile.db_path(&profile.collections()[0]),
            PathBuf::from("/db/perps.db")
        );
    }

    #[test]
    fn several_collections_get_their_own_ledgers() {
        let text = format!(
            "schema = 1\n[walk]\ndb-dir = \"/db\"\n{}{}",
            collection("perps", PERPS),
            collection("perp-coin", PERP)
        );
        let profile = parse(&text).unwrap();

        assert_eq!(profile.collections().len(), 2);
        let paths: Vec<_> = profile
            .collections()
            .iter()
            .map(|c| profile.db_path(c))
            .collect();
        assert_eq!(
            paths,
            vec![
                PathBuf::from("/db/perps.db"),
                PathBuf::from("/db/perp-coin.db")
            ]
        );
    }

    #[test]
    fn a_schema_this_binary_does_not_read_is_refused() {
        let text = format!("schema = 2\n{}", collection("perps", PERPS));
        assert!(parse(&text).unwrap_err().to_string().contains("schema 2"));
    }

    #[test]
    fn a_profile_with_no_collection_is_refused() {
        assert!(
            parse("schema = 1\n")
                .unwrap_err()
                .to_string()
                .contains("nothing to walk")
        );
    }

    #[test]
    fn two_collections_may_not_share_a_ledger() {
        let text = format!(
            "schema = 1\n{}{}",
            collection("perps", PERPS),
            collection("perps", PERP)
        );
        assert!(
            parse(&text)
                .unwrap_err()
                .to_string()
                .contains("appears twice")
        );
    }

    #[test]
    fn a_policy_that_is_not_56_hex_is_refused() {
        let text = format!("schema = 1\n{}", collection("perps", "e6ba"));
        assert!(
            parse(&text)
                .unwrap_err()
                .to_string()
                .contains("56-hex policy id")
        );
    }

    #[test]
    fn an_id_cannot_escape_the_ledger_directory() {
        let text = format!("schema = 1\n{}", collection("../perps", PERPS));
        assert!(
            parse(&text)
                .unwrap_err()
                .to_string()
                .contains("cannot name a ledger file")
        );
    }

    #[test]
    fn a_collection_may_set_its_own_first_mint() {
        // The real case: $PERP minted ten months before the Perps NFTs, so the file's
        // floor cannot serve both.
        let text = format!(
            "schema = 1\n[walk]\nfrom-slot = 170445182\n{}\n[[collection]]\nid = \"perp-coin\"\n\
             kind = \"token\"\npolicy = \"{PERP}\"\nfirst-mint-slot = 145246220\n",
            collection("perps", PERPS)
        );
        let profile = parse(&text).unwrap();

        let nft = &profile.collections()[0];
        let token = &profile.collections()[1];
        assert_eq!(
            profile.floor_of(nft),
            Some(170_445_182),
            "inherits the file's"
        );
        assert_eq!(profile.floor_of(token), Some(145_246_220), "its own wins");
        assert_eq!(
            token.first_mint_slot,
            Some(145_246_220),
            "the walk is given it as its floor_slot"
        );
    }

    #[test]
    fn a_leading_tilde_is_the_home_directory_not_a_literal_one() {
        // Only the shape is asserted: the test machine's HOME is whatever it is.
        let expanded = expand(Path::new("~/data/mithril"));
        assert!(!expanded.to_string_lossy().starts_with('~'));
        assert!(expanded.ends_with("data/mithril"));
    }
}
