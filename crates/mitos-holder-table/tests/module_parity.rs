//! The pilot's gate: the fold plane's holder table against the module's snapshot.
//!
//! This runs only when both artifacts exist. Generate them with:
//!
//! ```text
//! fold-probe pilot --immutable ~/data/mithril/immutable --start 0 --chunks 2000 \
//!   --policy <56-hex> --book .tmp/pilot/book-2000 --out .tmp/pilot/full
//!
//! MITOS_SHADOW_BOOK_DIR=... MITOS_SHADOW_DOLOS_CONFIG=... \
//! MITOS_SHADOW_POLICY=<56-hex> MITOS_SHADOW_MODULE=holder-distribution \
//! MITOS_SHADOW_DUMP=.tmp/pilot/module-events \
//!   cargo test -p mitos-core --test shadow_planes -- --nocapture
//!
//! cargo test -p mitos-holder-table --test module_parity -- --nocapture
//! ```
//!
//! The comparison is the raw table: holder identity and liquid asset quantities. The
//! module's `lp_amount` and `vests` are snapshot-time transforms the raw table does not
//! perform, so they are reported rather than asserted. A policy with neither should match
//! exactly, which is what `407ca4…` is for.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use mitos_community_events::holder_distribution::{HolderEvent, HolderId};
use mitos_holder_table::HolderKey;

type Assets = BTreeMap<Vec<u8>, u64>;

fn path_from_env(name: &str, fallback: &str) -> Option<PathBuf> {
    if let Some(path) = std::env::var_os(name) {
        return Some(PathBuf::from(path));
    }
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(fallback);
    path.exists().then_some(path)
}

fn key_label(key: &HolderKey) -> String {
    match key {
        HolderKey::Stake(credential) => hex::encode(credential),
        HolderKey::Enterprise(address) => address.clone(),
    }
}

/// Read a holder table keyed by column name, so the same reader takes the pilot's
/// `holders.tsv` and the book projection's narrower file. Both carry `holder`,
/// `asset_name_hex` and `quantity`; only the pilot carries `address` and `cohort`.
fn read_pilot(path: &Path) -> BTreeMap<HolderKey, Assets> {
    let text = std::fs::read_to_string(path).expect("read the pilot table");
    let mut table: BTreeMap<HolderKey, Assets> = BTreeMap::new();

    let mut lines = text.lines();
    let header: Vec<&str> = lines.next().expect("a header line").split('\t').collect();
    let column = |name: &str| {
        header
            .iter()
            .position(|field| *field == name)
            .unwrap_or_else(|| panic!("the table has no `{name}` column: {header:?}"))
    };
    let holder_at = column("holder");
    let name_at = column("asset_name_hex");
    let quantity_at = column("quantity");

    for line in lines {
        if line.is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split('\t').collect();
        let holder = fields[holder_at];
        let asset_hex = fields[name_at];
        let quantity: u64 = fields[quantity_at].parse().expect("quantity");

        let key = match hex::decode(holder) {
            Ok(bytes) if bytes.len() == 28 => {
                let credential: [u8; 28] = bytes.try_into().expect("28 bytes");
                HolderKey::Stake(credential)
            }
            _ => HolderKey::Enterprise(holder.to_owned()),
        };
        let name = hex::decode(asset_hex).expect("asset name hex");
        let total = table.entry(key).or_default().entry(name).or_insert(0);
        *total += quantity;
    }
    table
}

fn read_module(dir: &Path) -> BTreeMap<HolderKey, (Assets, u64, usize)> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("read the module event dump")
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            let name = path.file_name()?.to_string_lossy().into_owned();
            (name.starts_with("book-") && name.ends_with(".bin")).then_some(path)
        })
        .collect();
    files.sort();

    let mut holders: BTreeMap<HolderKey, (Assets, u64, usize)> = BTreeMap::new();
    for file in files {
        let bytes = std::fs::read(&file).expect("read a module event");
        let event: HolderEvent = ciborium::de::from_reader(bytes.as_slice()).expect("decode event");
        let HolderEvent::SnapshotChunk(chunk) = event else {
            continue;
        };
        for entry in chunk.holders {
            let key = match entry.id {
                HolderId::Stake(hex) => {
                    let bytes = hex::decode(hex).expect("stake credential hex");
                    HolderKey::Stake(bytes.try_into().expect("28-byte stake credential"))
                }
                HolderId::Enterprise(address) => HolderKey::Enterprise(address),
            };
            let (assets, lp_amount, vesting) = holders.entry(key).or_default();
            *lp_amount += entry.lp_amount;
            *vesting += entry.vests.len();
            for asset in entry.assets {
                let name = hex::decode(asset.asset_name_hex).expect("asset name hex");
                *assets.entry(name).or_insert(0) += asset.quantity;
            }
        }
    }
    holders
}

#[test]
fn the_pilot_table_matches_the_module_snapshot() {
    let Some(pilot_path) = path_from_env("MITOS_PILOT_TABLE", ".tmp/pilot/full/holders.tsv") else {
        eprintln!("skipping: no pilot table (run the pilot, or set MITOS_PILOT_TABLE)");
        return;
    };
    let Some(module_dir) = path_from_env("MITOS_MODULE_EVENTS", ".tmp/pilot/module-events") else {
        eprintln!("skipping: no module events (run the shadow dump, or set MITOS_MODULE_EVENTS)");
        return;
    };

    let pilot = read_pilot(&pilot_path);
    let module = read_module(&module_dir);

    let mut problems: Vec<String> = Vec::new();
    for (key, (module_assets, lp_amount, vesting)) in &module {
        match pilot.get(key) {
            None => problems.push(format!("module-only holder {}", key_label(key))),
            Some(pilot_assets) if pilot_assets != module_assets => problems.push(format!(
                "asset mismatch for {}: module {:?} vs pilot {:?}",
                key_label(key),
                module_assets,
                pilot_assets
            )),
            Some(_) => {}
        }
        if *lp_amount > 0 || *vesting > 0 {
            println!(
                "note: {} carries lp_amount={lp_amount} and {vesting} vest(s); \
                 the raw table does not decompose either",
                key_label(key)
            );
        }
    }
    for key in pilot.keys().filter(|key| !module.contains_key(*key)) {
        problems.push(format!("pilot-only holder {}", key_label(key)));
    }

    let pilot_total: u128 = pilot
        .values()
        .flat_map(|assets| assets.values())
        .map(|quantity| u128::from(*quantity))
        .sum();
    let module_total: u128 = module
        .values()
        .flat_map(|(assets, _, _)| assets.values())
        .map(|quantity| u128::from(*quantity))
        .sum();

    println!("pilot:  {} holders, {pilot_total} quantity", pilot.len());
    println!("module: {} holders, {module_total} quantity", module.len());

    assert!(
        problems.is_empty(),
        "the pilot and the module disagree:\n{}",
        problems.join("\n")
    );
    assert_eq!(pilot.len(), module.len());
    assert_eq!(pilot_total, module_total);
    println!("OK          holder identity and liquid quantities match");
}
