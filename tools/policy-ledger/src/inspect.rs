//! `policy-ledger catalogue inspect` — read a built artifact back.
//!
//! The artifact is opaque bytes; this is how a build gets checked without
//! deploying a frontend to look at it. It decodes through the SAME
//! `collection_catalogue::decode` a consumer uses, version byte included, so
//! "inspect works" is evidence about the published format and not about a
//! private reader.

use std::path::PathBuf;

use anyhow::{Context, Result};
use collection_catalogue::{Catalogue, EntryFlags, NameOrigin};

#[derive(clap::Args, Debug)]
pub struct InspectArgs {
    #[arg(long, default_value = "catalogue.bin")]
    pub artifact: PathBuf,
    /// Print entries whose name contains this, case-insensitively — the same
    /// substring match a palette would start from.
    #[arg(long)]
    pub find: Option<String>,
    /// Print the N largest collections.
    #[arg(long, default_value_t = 0)]
    pub top: usize,
}

pub fn run(args: InspectArgs) -> Result<()> {
    let raw = std::fs::read(&args.artifact)
        .with_context(|| format!("reading {}", args.artifact.display()))?;
    let catalogue = collection_catalogue::decode(&raw)
        .map_err(|e| anyhow::anyhow!("{} is not a catalogue: {e}", args.artifact.display()))?;

    summarise(&catalogue, raw.len());

    if let Some(needle) = &args.find {
        let needle = needle.to_lowercase();
        let mut hits = 0usize;
        for e in &catalogue.entries {
            if e.names
                .iter()
                .any(|a| a.name.to_lowercase().contains(&needle))
            {
                println!(
                    "  {} {:>7} assets {} {}",
                    e.policy_hex(),
                    e.assets,
                    flag_letters(e.flags),
                    e.names
                        .iter()
                        .map(|a| format!("{} [{}]", a.name, origin_label(a.origin)))
                        .collect::<Vec<_>>()
                        .join(" · ")
                );
                hits += 1;
            }
        }
        println!("{hits} match(es) for {needle:?}");
    }

    if args.top > 0 {
        let mut by_size: Vec<_> = catalogue.entries.iter().collect();
        by_size.sort_unstable_by_key(|e| std::cmp::Reverse(e.assets));
        println!("largest {}:", args.top);
        for e in by_size.into_iter().take(args.top) {
            println!(
                "  {:>7} {} {}",
                e.assets,
                flag_letters(e.flags),
                e.best_name().unwrap_or("<unnamed>")
            );
        }
    }
    Ok(())
}

fn summarise(c: &Catalogue, stored: usize) {
    let named = c.named();
    let aliases: usize = c.entries.iter().map(|e| e.names.len()).sum();
    let unique: std::collections::HashSet<&str> = c
        .entries
        .iter()
        .flat_map(|e| e.names.iter().map(|a| a.name.as_str()))
        .collect();
    let count = |f: EntryFlags| c.entries.iter().filter(|e| e.flags.contains(f)).count();
    let by_origin = |o: NameOrigin| {
        c.entries
            .iter()
            .flat_map(|e| e.names.iter())
            .filter(|a| a.origin == o)
            .count()
    };

    println!(
        "v{} through chunk {} built_at {}",
        c.version, c.built_through_chunk, c.built_at
    );
    println!(
        "{} entries, {} named ({:.1}%), {} unnamed",
        c.entries.len(),
        named,
        100.0 * named as f64 / c.entries.len().max(1) as f64,
        c.entries.len() - named,
    );
    println!(
        "{aliases} aliases, {} unique ({:.0}% — 🔑 the reason this is not interned)",
        unique.len(),
        100.0 * unique.len() as f64 / aliases.max(1) as f64,
    );
    for o in NameOrigin::ALL {
        let n = by_origin(o);
        if n > 0 {
            println!("  {:<22} {n}", origin_label(o));
        }
    }
    println!(
        "flags: hazard {}, tracked {}, cip68 {}, jpg {}",
        count(EntryFlags::HAZARD),
        count(EntryFlags::TRACKED),
        count(EntryFlags::CIP68),
        count(EntryFlags::JPG_VERIFIED),
    );
    let with_ext = c.entries.iter().filter(|e| !e.ext.is_empty()).count();
    println!(
        "extensions: {with_ext} entries carry one, {} top-level",
        c.ext.len()
    );
    println!("{:.1} KiB raw", stored as f64 / 1024.0);
}

fn origin_label(o: NameOrigin) -> &'static str {
    match o {
        NameOrigin::Declared => "declared",
        NameOrigin::DerivedFromMetadata => "derived:metadata",
        NameOrigin::DerivedFromAssetNames => "derived:asset-names",
        NameOrigin::CuratedJpg => "curated:jpg",
        NameOrigin::CuratedLocal => "curated:local",
        NameOrigin::RegistryDeclared => "registry:declared",
    }
}

/// Compact flag column — `H---` reads at a glance where four booleans do not.
fn flag_letters(f: EntryFlags) -> String {
    let bit = |set: bool, c: char| if set { c } else { '-' };
    [
        bit(f.contains(EntryFlags::HAZARD), 'H'),
        bit(f.contains(EntryFlags::TRACKED), 'T'),
        bit(f.contains(EntryFlags::CIP68), '6'),
        bit(f.contains(EntryFlags::JPG_VERIFIED), 'J'),
    ]
    .iter()
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_read_as_a_column() {
        assert_eq!(flag_letters(EntryFlags::default()), "----");
        assert_eq!(flag_letters(EntryFlags::HAZARD), "H---");
        let mut all = EntryFlags::default();
        for f in [
            EntryFlags::HAZARD,
            EntryFlags::TRACKED,
            EntryFlags::CIP68,
            EntryFlags::JPG_VERIFIED,
        ] {
            all.insert(f);
        }
        assert_eq!(flag_letters(all), "HT6J");
    }

    /// Every origin has a label — an unlabelled one reaching a listing means
    /// a variant was added without being given words.
    #[test]
    fn every_origin_has_words() {
        for o in NameOrigin::ALL {
            assert!(!origin_label(o).is_empty());
        }
    }
}
