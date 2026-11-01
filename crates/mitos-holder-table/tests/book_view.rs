//! The published artifact against the book it was built from.
//!
//! `pilot` asserts the two shapes agree while it runs — the walk's live set and the book's
//! are the same set. This is the other half, from the artifacts alone: the manifest is
//! readable, every file is the length it claims, and projecting the book again reproduces
//! the rows byte for byte. A changed row encoder, a book that drifted from its artifact, or
//! a manifest that outlived the file it describes all fail here without re-walking two
//! thousand chunks.
//!
//! It runs only when the pilot has produced an artifact. Generate one with:
//!
//! ```text
//! fold-probe pilot --immutable ~/data/mithril/immutable --start 0 --chunks 2000 \
//!   --policy <56-hex> --book .tmp/pilot/book-2000 --out .tmp/pilot/full
//!
//! cargo test -p mitos-holder-table --test book_view -- --nocapture
//! ```
//!
//! `MITOS_PILOT_DIR` and `MITOS_PILOT_BOOK` override the two paths.

use std::path::PathBuf;

use mitos_fold::{Artifact, SchemaId, View};
use mitos_holder_table::{HolderTable, HolderTableView};

fn path_from_env(name: &str, fallback: &str) -> Option<PathBuf> {
    if let Some(path) = std::env::var_os(name) {
        return Some(PathBuf::from(path));
    }
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(fallback);
    path.exists().then_some(path)
}

#[test]
fn the_book_reproduces_the_published_artifact() {
    let Some(dir) = path_from_env("MITOS_PILOT_DIR", ".tmp/pilot/full") else {
        eprintln!("skipping: no pilot artifact (run the pilot)");
        return;
    };
    let Some(book_dir) = path_from_env("MITOS_PILOT_BOOK", ".tmp/pilot/book-2000") else {
        eprintln!("skipping: no pilot book (run the pilot)");
        return;
    };

    let manifest = dir.join(Artifact::FILE);
    let Ok(text) = std::fs::read_to_string(&manifest) else {
        eprintln!("skipping: no manifest at {}", manifest.display());
        return;
    };
    let artifact = Artifact::decode(&text, SchemaId::new("holder-table", 1))
        .expect("the manifest is one this build writes");

    // Every file the manifest names must be the length it names. This is what catches a
    // rows file that changed under a manifest that survived — the case the writer's
    // remove-then-write ordering cannot cover on its own.
    for file in &artifact.files {
        let path = dir.join(&file.name);
        let length = std::fs::metadata(&path)
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()))
            .len();
        assert_eq!(
            length, file.bytes,
            "{} is {length} bytes; the manifest says {}",
            file.name, file.bytes
        );
    }

    let Some(policy) = HolderTable::policy_of(&artifact.extensions) else {
        panic!(
            "the artifact names no policy — tag {} is missing",
            HolderTable::POLICY_EXTENSION
        );
    };

    let published = std::fs::read_to_string(dir.join(HolderTable::ROWS_FILE))
        .expect("the manifest names a rows file");

    let book = live_book::Base::open(&book_dir).expect("open the pilot book");
    let mut view = HolderTableView::new(policy, Vec::new(), Vec::new());
    view.view(&book).expect("the view reads the book");
    let refs = view.refs();
    let dropped = view.dropped();
    let table = view.table().expect("the view produces a table");
    let rebuilt = table.to_rows();

    println!(
        "book:   {} holders, {} balances ({refs} refs, {dropped} without a holder identity)",
        table.distinct_holders(),
        table.balances()
    );
    println!(
        "artifact: {} rows, {} entries, {} bytes at slot {}",
        artifact.counts.rows,
        artifact.counts.entries,
        artifact.counts.bytes,
        artifact.stamp.base_slot
    );

    assert_eq!(
        artifact.counts.rows,
        table.distinct_holders() as u64,
        "the manifest's row count is not the table's holder count"
    );
    assert_eq!(
        artifact.counts.entries,
        table.balances() as u64,
        "the manifest's entry count is not the table's balance count"
    );
    assert_eq!(
        rebuilt.len() as u64,
        artifact.counts.bytes,
        "the rebuilt rows are not the length the manifest claims"
    );
    assert_eq!(
        rebuilt, published,
        "the book does not reproduce the published rows"
    );

    // The stamp is the coordinate a reader pairs a delta against, so it must survive the
    // manifest's text form exactly.
    artifact
        .require_stamp(artifact.stamp)
        .expect("the artifact's own stamp is the stamp it requires");

    println!("OK          the artifact is reproducible from its book");
}
