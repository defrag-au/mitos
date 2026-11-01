//! The book view against the walk's own answer.
//!
//! `pilot` asserts the two shapes agree while it runs; this is the same assertion made
//! afterwards, from the artifacts alone, so a later change to either shape is caught without
//! re-walking two thousand chunks.
//!
//! It runs only when the pilot has produced a summary and a book. Generate them with:
//!
//! ```text
//! fold-probe pilot --immutable ~/data/mithril/immutable --start 0 --chunks 2000 \
//!   --policy <56-hex> --book .tmp/pilot/book-2000 --out .tmp/pilot/full
//!
//! cargo test -p mitos-holder-table --test book_view -- --nocapture
//! ```
//!
//! `MITOS_PILOT_DIR` and `MITOS_PILOT_BOOK` override the two paths.

use std::collections::BTreeMap;
use std::path::PathBuf;

use mitos_fold::View;
use mitos_holder_table::HolderTableView;

fn path_from_env(name: &str, fallback: &str) -> Option<PathBuf> {
    if let Some(path) = std::env::var_os(name) {
        return Some(PathBuf::from(path));
    }
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(fallback);
    path.exists().then_some(path)
}

/// `summary.tsv` as key → value. Written whole by the pilot, so a missing key is a summary
/// this test does not understand rather than a blank to invent.
fn read_summary(path: &std::path::Path) -> BTreeMap<String, String> {
    std::fs::read_to_string(path)
        .expect("read the pilot summary")
        .lines()
        .filter_map(|line| {
            let (key, value) = line.split_once('\t')?;
            Some((key.to_owned(), value.to_owned()))
        })
        .collect()
}

fn number(summary: &BTreeMap<String, String>, key: &str) -> Option<u128> {
    summary.get(key)?.parse().ok()
}

#[test]
fn the_book_projects_to_the_walk_s_answer() {
    let Some(dir) = path_from_env("MITOS_PILOT_DIR", ".tmp/pilot/full") else {
        eprintln!("skipping: no pilot summary (run the pilot)");
        return;
    };
    let Some(book_dir) = path_from_env("MITOS_PILOT_BOOK", ".tmp/pilot/book-2000") else {
        eprintln!("skipping: no pilot book (run the pilot)");
        return;
    };
    let summary = read_summary(&dir.join("summary.tsv"));

    let Some(policy_hex) = summary.get("policy") else {
        eprintln!("skipping: the summary names no policy");
        return;
    };
    let policy_bytes = hex::decode(policy_hex).expect("the summary's policy is hex");
    let policy: [u8; 28] = policy_bytes.try_into().expect("a 28-byte policy");

    let (Some(walked_holders), Some(walked_quantity)) = (
        number(&summary, "stream_holders"),
        number(&summary, "stream_total_quantity"),
    ) else {
        // A summary written before the view existed has no stream line. That is a stale
        // artifact, not a disagreement, and saying so is more useful than failing.
        eprintln!("skipping: the summary predates the two-shape cross-check — re-run the pilot");
        return;
    };

    let book = live_book::Base::open(&book_dir).expect("open the pilot book");
    let mut view = HolderTableView::new(policy, Vec::new(), Vec::new());
    view.view(&book).expect("the view reads the book");
    let refs = view.refs();
    let dropped = view.dropped();
    let table = view.table().expect("the view produces a table");

    println!(
        "book:   {} holders, {} quantity ({refs} refs, {dropped} without a holder identity)",
        table.distinct_holders(),
        table.total_quantity()
    );
    println!("walk:   {walked_holders} holders, {walked_quantity} quantity");

    assert_eq!(
        u128::from(table.distinct_holders() as u64),
        walked_holders,
        "the book and the walk disagree about how many holders the policy has"
    );
    assert_eq!(
        table.total_quantity(),
        walked_quantity,
        "the book and the walk disagree about how much of the policy is held"
    );
    println!("OK          the two shapes agree");
}
