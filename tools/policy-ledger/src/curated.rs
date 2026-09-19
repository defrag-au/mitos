//! The two inputs chain cannot supply: jpg.store's curated names and the set
//! of policies `collection-ownership` already tracks.
//!
//! ⚠️ **Both are OPTIONAL, deliberately.** Without them the catalogue is
//! purely chain-derived — reproducible from the immutable chunks alone, which
//! is what makes the daily build a certified snapshot rather than a join
//! against whatever a Cloudflare D1 happened to say that morning. With them it
//! gains the 2,551 collections only jpg names (ADA Handle among them) and the
//! `TRACKED` bit.
//!
//! ⚠️ **jpg's own names are 62% PLACEHOLDER.** MEASURED over its 38,992 rows:
//! 20,303 `Unnamed`, 3,987 `None`, and one row carrying the leaked Python repr
//! `"{'name': '', 'family': ''}"`. Loading them unfiltered would put 24,000
//! entries called "Unnamed" into a search box and flag every one of them
//! `JPG_VERIFIED`. They are dropped here, once, rather than at every use.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result};

use crate::classify::Overlay;

/// Names jpg.store stores instead of a name. Compared case-insensitively
/// after trimming.
const PLACEHOLDERS: [&str; 4] = ["unnamed", "none", "null", "n/a"];

/// The leaked Python repr in jpg's export — one row, but it would sort into
/// the palette under `{`.
fn is_leaked_repr(name: &str) -> bool {
    name.starts_with('{') && name.contains("'name'")
}

fn is_placeholder(name: &str) -> bool {
    let t = name.trim();
    t.is_empty() || PLACEHOLDERS.contains(&t.to_lowercase().as_str()) || is_leaked_repr(t)
}

fn policy_bytes(hex_id: &str) -> Option<[u8; 28]> {
    let raw = hex::decode(hex_id.trim()).ok()?;
    raw.try_into().ok()
}

/// How a load went — reported, because a silently empty overlay looks exactly
/// like a successful one that changed nothing.
#[derive(Debug, Default)]
pub struct OverlayStats {
    pub curated_rows: usize,
    pub curated_kept: usize,
    pub curated_placeholder: usize,
    pub curated_unparsed: usize,
    pub tracked: usize,
}

/// jpg.store's export: `{ "<56-hex policy>": "<display name>" }`.
pub fn load_curated(path: &Path, into: &mut Overlay, stats: &mut OverlayStats) -> Result<()> {
    let raw = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let rows: HashMap<String, String> = serde_json::from_slice(&raw)
        .with_context(|| format!("parsing {} as a policy → name map", path.display()))?;
    stats.curated_rows = rows.len();
    for (id, name) in rows {
        let Some(policy) = policy_bytes(&id) else {
            stats.curated_unparsed += 1;
            continue;
        };
        if is_placeholder(&name) {
            stats.curated_placeholder += 1;
            continue;
        }
        stats.curated_kept += 1;
        into.curated.insert(policy, name.trim().to_string());
    }
    Ok(())
}

/// Policies `collection-ownership` already tracks: one 56-hex id per line,
/// `#` comments and blanks ignored.
pub fn load_tracked(path: &Path, into: &mut Overlay, stats: &mut OverlayStats) -> Result<()> {
    let raw =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let ids: HashSet<[u8; 28]> = raw
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .filter_map(policy_bytes)
        .collect();
    stats.tracked = ids.len();
    into.tracked.extend(ids);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ⚠️ 62% of jpg's names are not names. Loading them unfiltered puts
    /// 24,000 entries called "Unnamed" into a search box.
    #[test]
    fn jpg_placeholders_are_not_names() {
        for p in ["Unnamed", "None", "none", " NONE ", "", "   ", "null"] {
            assert!(is_placeholder(p), "{p:?} should be dropped");
        }
        assert!(
            is_placeholder("{'name': '', 'family': ''}"),
            "the leaked Python repr would sort into the palette under '{{'"
        );
        for real in ["Clay Nation", "ADA Handle", "None Of The Above"] {
            assert!(!is_placeholder(real), "{real:?} is a real name");
        }
    }

    #[test]
    fn a_curated_export_loads_and_reports_what_it_dropped() {
        let dir =
            std::env::temp_dir().join(format!("policy-ledger-curated-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("curated.json");
        let good = "11".repeat(28);
        let bad = "22".repeat(28);
        std::fs::write(
            &path,
            format!(r#"{{"{good}":"Clay Nation","{bad}":"Unnamed","not-hex":"Whatever"}}"#),
        )
        .unwrap();

        let mut overlay = Overlay::default();
        let mut stats = OverlayStats::default();
        load_curated(&path, &mut overlay, &mut stats).unwrap();

        assert_eq!(stats.curated_rows, 3);
        assert_eq!(stats.curated_kept, 1);
        assert_eq!(stats.curated_placeholder, 1);
        assert_eq!(stats.curated_unparsed, 1);
        assert_eq!(
            overlay.curated.get(&[0x11; 28]).map(String::as_str),
            Some("Clay Nation")
        );
        assert!(!overlay.curated.contains_key(&[0x22; 28]));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A 27-byte id is not a policy, and quietly padding one would key an
    /// entry nothing can ever look up.
    #[test]
    fn only_a_full_28_byte_policy_id_parses() {
        assert_eq!(policy_bytes(&"ab".repeat(28)), Some([0xab; 28]));
        assert_eq!(policy_bytes(&"ab".repeat(27)), None);
        assert_eq!(policy_bytes(&"ab".repeat(32)), None);
        assert_eq!(policy_bytes("zz"), None);
        assert_eq!(
            policy_bytes(&format!("  {}  ", "ab".repeat(28))),
            Some([0xab; 28])
        );
    }

    #[test]
    fn a_tracked_list_ignores_comments_and_blanks() {
        let dir =
            std::env::temp_dir().join(format!("policy-ledger-tracked-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("tracked.txt");
        std::fs::write(
            &path,
            format!(
                "# ownership_policies\n{}\n\n{}\n",
                "11".repeat(28),
                "11".repeat(28)
            ),
        )
        .unwrap();

        let mut overlay = Overlay::default();
        let mut stats = OverlayStats::default();
        load_tracked(&path, &mut overlay, &mut stats).unwrap();
        assert_eq!(stats.tracked, 1, "deduped");
        assert!(overlay.tracked.contains(&[0x11; 28]));
        std::fs::remove_dir_all(&dir).ok();
    }
}
