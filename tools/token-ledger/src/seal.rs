//! `seal` — write a ledger's movements to Parquet, month by month.
//!
//! The sqlite ledger is the mutable working set: incremental walks, resume
//! cursors, the outref buffer, and point lookups by transaction hash. Parquet is
//! the sealed archive — immutable once written, columnar, and queryable in place
//! from R2 over HTTP range requests.
//!
//! # Why in-process rather than shelling `duckdb`
//!
//! market-ledger's `seal` shells the CLI, declining the **`duckdb` crate**
//! because it embeds a whole query engine. The **`parquet` crate** is a
//! different proposition — the file format alone, no engine, no SQL — so it
//! costs one build dependency and no runtime one. That matters concretely:
//! `duckdb` is not installed on cardano-infra, so the CLI route cannot seal
//! anything until somebody installs a system package on a box running six live
//! services.
//!
//! It also keeps the artifact honest about its own provenance. The writer is the
//! same code that knows the ledger's [`Completeness`], rather than a SQL string
//! that would have to be told separately — and a sealed partition that forgets
//! it was built from a PARTIAL walk is a partial history that looks
//! authoritative.
//!
//! # What gets sealed
//!
//! One row per `(transaction, party, unit)` movement — the `delta` join,
//! flattened. That is the 12% of the ledger that cannot be recomputed from the
//! Mithril snapshot: transactions and addresses are already on chain, but the
//! ATTRIBUTION is not, because a transaction's inputs are bare outrefs carrying
//! no content.
//!
//! Four of the six columns are integers sorted by `tx_ord`, which is the ideal
//! case for columnar encoding — dictionary on the repeating ids, delta/RLE on
//! the sorted ones. Row-major sqlite pages can exploit none of it.

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use rusqlite::{Connection, OptionalExtension};

use crate::store::Completeness;

#[derive(clap::Args, Debug)]
pub struct SealArgs {
    /// Ledger sqlite path.
    #[arg(long)]
    pub db: PathBuf,

    /// Root for the partition tree
    /// (`<out-dir>/movements/policy=<hex>/year=<y>/month=<m>.parquet`).
    #[arg(long, default_value = "parquet")]
    pub out_dir: PathBuf,

    /// Seal months the ledger's coverage does not fully contain.
    ///
    /// Off by default, and the default is the important one: a partially
    /// covered month sealed now is recorded as done and never revisited, so the
    /// archive keeps a month with its tail quietly missing.
    #[arg(long)]
    pub all: bool,

    /// Re-seal months already in the manifest.
    #[arg(long)]
    pub force: bool,
}

/// One sealed partition, as the manifest records it.
#[derive(Debug, PartialEq, Eq)]
pub struct Partition {
    pub year: i64,
    pub month: i64,
    pub rows: i64,
    /// Highest slot in the partition — what a consumer reads to know how far
    /// this file reaches without opening it.
    pub max_slot: i64,
}

/// Movement rows, flattened. The Parquet message type is spelled out rather
/// than derived so the on-disk column names are a deliberate contract with the
/// frontend, not a by-product of a Rust struct's field order.
const MESSAGE_TYPE: &str = "
    message movement {
        REQUIRED INT64 slot;
        REQUIRED INT64 block_time;
        REQUIRED BYTE_ARRAY tx_hash;
        REQUIRED BYTE_ARRAY unit_name;
        REQUIRED BYTE_ARRAY address;
        REQUIRED INT64 amount;
    }
";

pub fn run(args: SealArgs) -> Result<()> {
    let conn = Connection::open(&args.db)
        .with_context(|| format!("opening ledger {}", args.db.display()))?;
    ensure_manifest(&conn)?;

    // WHAT THIS LEDGER IS, and how far it reaches. Both travel into the
    // artifact path and the manifest — a sealed file that cannot say which
    // policy it describes, or whether that description is complete, is a
    // liability rather than an archive.
    let policy: Vec<u8> = conn
        .query_row("SELECT policy FROM meta WHERE k = 'asset'", [], |r| {
            r.get(0)
        })
        .context("ledger has no `meta` row — run `walk` or `reverse` first")?;
    let policy_hex = hex::encode(&policy);
    let completeness = crate::store::Completeness::from_code(
        conn.query_row(
            "SELECT slot FROM cursor WHERE k = ?1",
            rusqlite::params![crate::store::cursor_key::COVERAGE_COMPLETE],
            |r| r.get::<_, i64>(0),
        )
        .ok(),
    );

    let months = months_in(&conn)?;
    if months.is_empty() {
        tracing::info!("seal: ledger holds no transactions, nothing to seal");
        return Ok(());
    }
    let coverage = coverage_window(&conn)?;

    let mut sealed = 0usize;
    let mut skipped = 0usize;
    for m in months {
        let (year, month) = (m.year, m.month);
        if !args.all && !covers(coverage, &m) {
            // Not fully walked. See `covers` — which end is incomplete depends
            // on which direction built the ledger, so the test is containment
            // rather than position.
            skipped += 1;
            continue;
        }
        if !args.force && already_sealed(&conn, year, month)? {
            continue;
        }

        let dir = args
            .out_dir
            .join("movements")
            .join(format!("policy={policy_hex}"))
            .join(format!("year={year}"));
        std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
        let path = dir.join(format!("month={month:02}.parquet"));

        let part = write_month(&conn, &path, year, month)?;
        record(&conn, &part, completeness)?;
        sealed += 1;
        tracing::info!(
            year, month, rows = part.rows, max_slot = part.max_slot,
            path = %path.display(), "seal: wrote partition"
        );
    }

    println!(
        "sealed {sealed} partition(s) under {}",
        args.out_dir.display()
    );
    if skipped > 0 {
        println!(
            "skipped {skipped} month(s) the coverage does not fully contain — \
             walk further before sealing them, or force with --all"
        );
    }
    if completeness != Completeness::Complete {
        println!(
            "NOTE: this ledger is {} — every partition is stamped with that, so a \
             consumer can tell an archive from a window.",
            match completeness {
                Completeness::Partial => "PARTIAL",
                _ => "of UNRECORDED coverage",
            }
        );
    }
    Ok(())
}

/// One calendar month present in the ledger, with its own bounds.
///
/// The bounds are the MONTH's, not the data's — a month whose rows happen to
/// start on the 9th is still a month that begins on the 1st, and sealing it
/// while coverage starts on the 5th would file a partial month as whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Month {
    pub year: i64,
    pub month: i64,
    /// Unix seconds of the first instant of the month.
    pub start_unix: i64,
    /// Unix seconds of the last instant of the month.
    pub end_unix: i64,
}

/// The window a ledger actually covers, as unix seconds.
///
/// `None` at either end means unrecorded, which [`covers`] treats as covering
/// nothing — a ledger that cannot state its bounds must not have months sealed
/// off the back of a guess.
type Window = (Option<i64>, Option<i64>);

/// Distinct months present in the ledger, ascending, with calendar bounds.
fn months_in(conn: &Connection) -> Result<Vec<Month>> {
    let mut stmt = conn.prepare(
        "SELECT DISTINCT
             CAST(strftime('%Y', datetime(block_time,'unixepoch')) AS INTEGER) AS y,
             CAST(strftime('%m', datetime(block_time,'unixepoch')) AS INTEGER) AS m,
             CAST(strftime('%s', printf('%04d-%02d-01',
                 CAST(strftime('%Y', datetime(block_time,'unixepoch')) AS INTEGER),
                 CAST(strftime('%m', datetime(block_time,'unixepoch')) AS INTEGER))) AS INTEGER),
             CAST(strftime('%s', datetime(printf('%04d-%02d-01',
                 CAST(strftime('%Y', datetime(block_time,'unixepoch')) AS INTEGER),
                 CAST(strftime('%m', datetime(block_time,'unixepoch')) AS INTEGER)),
                 '+1 month')) AS INTEGER) - 1
         FROM tx ORDER BY y, m",
    )?;
    Ok(stmt
        .query_map([], |r| {
            Ok(Month {
                year: r.get(0)?,
                month: r.get(1)?,
                start_unix: r.get(2)?,
                end_unix: r.get(3)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?)
}

/// What the ledger covers, in unix seconds.
fn coverage_window(conn: &Connection) -> Result<Window> {
    // Keys come from `store::cursor_key`, never spelled here. An earlier draft
    // wrote `walked_from` where the store says `walk_from`, read `None`, and
    // quietly sealed nothing.
    let read = |k: &str| -> Result<Option<i64>> {
        Ok(conn
            .query_row(
                "SELECT slot FROM cursor WHERE k = ?1",
                rusqlite::params![k],
                |r| r.get::<_, i64>(0),
            )
            .optional()?
            .map(|s| mitos_chain_walk::slot_to_unix(s as u64) as i64))
    };
    Ok((
        read(crate::store::cursor_key::WALK_FROM)?,
        read(crate::store::cursor_key::WALK_TO)?,
    ))
}

/// Is this month FULLY inside the ledger's coverage?
///
/// # Why containment, and not "skip the newest"
///
/// The first version skipped the newest month as "still growing". That is a
/// FORWARD-walk assumption and it is exactly inverted for a reverse walk, which
/// starts at the tip and works down: its newest month is the first one finished,
/// and the month containing the floor is the incomplete one.
///
/// Measured on the live SpaceBudz ledger — floor 193,212,000, months 2026-07
/// and 2026-08 — the old rule skipped the complete month and sealed the partial
/// one. Containment against `[walked_from, walked_to]` is direction-agnostic:
/// a forward walk's upper bound is where it stopped, a reverse walk's is the
/// ceiling it started from, and neither needs the sealer to know which ran.
fn covers(window: Window, m: &Month) -> bool {
    let (Some(lo), Some(hi)) = window else {
        // Unrecorded coverage. Sealing on a guess is how a month with a missing
        // tail becomes a permanent archive entry.
        return false;
    };
    lo <= m.start_unix && m.end_unix <= hi
}

fn already_sealed(conn: &Connection, year: i64, month: i64) -> Result<bool> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM partition_manifest WHERE year=?1 AND month=?2",
            rusqlite::params![year, month],
            |_| Ok(()),
        )
        .is_ok())
}

/// Write one month's movements.
///
/// **Materialises the month before writing it.** Parquet is columnar, so each
/// column is handed over as a contiguous batch — the row-major sqlite cursor has
/// to be pivoted, and pivoting means holding the rows.
///
/// That is a real ceiling: a mint month of a large collection is hundreds of
/// thousands of movements, held six columns wide. Acceptable for now because a
/// month is the partition unit and the largest observed is far short of
/// trouble — but if a policy ever seals a month that will not fit, the fix is
/// to write several row groups per file, batching by row count, rather than to
/// make the partitions smaller.
fn write_month(conn: &Connection, path: &PathBuf, year: i64, month: i64) -> Result<Partition> {
    let schema = Arc::new(parse_message_type(MESSAGE_TYPE).context("parquet schema")?);
    let props = WriterProperties::builder()
        // SNAPPY, and the choice is the READER's, not the writer's.
        //
        // Whatever codec is written here, every consumer must compile in to
        // decompress — including the wasm frontend, where the whole reader
        // currently costs 0.15 MB. `zstd` is a C library through `zstd-sys`;
        // snappy is pure Rust and small. Spending hundreds of kilobytes of
        // browser bundle to shrink a file the browser fetches once is the wrong
        // way round.
        //
        // The loss is smaller than it looks: four of six columns are sorted
        // integers, so dictionary and RLE encoding do most of the work before
        // the codec sees anything.
        //
        // Note this MUST match an enabled cargo feature — the crate accepts an
        // unsupported codec at compile time and panics on first write.
        .set_compression(Compression::SNAPPY)
        .build();
    let file = File::create(path).with_context(|| format!("creating {}", path.display()))?;
    let mut writer = SerializedFileWriter::new(file, schema, Arc::new(props))?;

    let mut stmt = conn.prepare(
        "SELECT t.slot, t.block_time, t.tx_hash, u.name, p.address, d.amount
         FROM delta d
         JOIN tx t     ON t.tx_ord = d.tx_ord
         JOIN party p  ON p.party_id = d.party_id
         JOIN unit u   ON u.unit_id = d.unit_id
         WHERE CAST(strftime('%Y', datetime(t.block_time,'unixepoch')) AS INTEGER) = ?1
           AND CAST(strftime('%m', datetime(t.block_time,'unixepoch')) AS INTEGER) = ?2
         ORDER BY t.tx_ord",
    )?;
    let rows: Vec<(i64, i64, Vec<u8>, Vec<u8>, String, i64)> = stmt
        .query_map(rusqlite::params![year, month], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;

    let max_slot = rows.iter().map(|r| r.0).max().unwrap_or(0);
    let count = rows.len() as i64;

    {
        use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
        let mut group = writer.next_row_group()?;
        let mut col = 0usize;
        while let Some(mut w) = group.next_column()? {
            match col {
                0 => {
                    let v: Vec<i64> = rows.iter().map(|r| r.0).collect();
                    w.typed::<Int64Type>().write_batch(&v, None, None)?;
                }
                1 => {
                    let v: Vec<i64> = rows.iter().map(|r| r.1).collect();
                    w.typed::<Int64Type>().write_batch(&v, None, None)?;
                }
                2 => {
                    let v: Vec<ByteArray> =
                        rows.iter().map(|r| ByteArray::from(r.2.clone())).collect();
                    w.typed::<ByteArrayType>().write_batch(&v, None, None)?;
                }
                3 => {
                    let v: Vec<ByteArray> =
                        rows.iter().map(|r| ByteArray::from(r.3.clone())).collect();
                    w.typed::<ByteArrayType>().write_batch(&v, None, None)?;
                }
                4 => {
                    let v: Vec<ByteArray> = rows
                        .iter()
                        .map(|r| ByteArray::from(r.4.as_bytes().to_vec()))
                        .collect();
                    w.typed::<ByteArrayType>().write_batch(&v, None, None)?;
                }
                _ => {
                    let v: Vec<i64> = rows.iter().map(|r| r.5).collect();
                    w.typed::<Int64Type>().write_batch(&v, None, None)?;
                }
            }
            w.close()?;
            col += 1;
        }
        group.close()?;
    }
    writer.close()?;

    Ok(Partition {
        year,
        month,
        rows: count,
        max_slot,
    })
}

/// The manifest — which months are sealed, and under what coverage.
///
/// `completeness` is stored PER PARTITION rather than once for the ledger
/// because it is a property of the walk that produced the file, and a ledger
/// deepened later will seal newer partitions under a different verdict than the
/// ones already written.
fn ensure_manifest(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS partition_manifest (
             year         INTEGER NOT NULL,
             month        INTEGER NOT NULL,
             rows         INTEGER NOT NULL,
             max_slot     INTEGER NOT NULL,
             completeness TEXT    NOT NULL,
             sealed_unix  INTEGER NOT NULL,
             PRIMARY KEY (year, month)
         );",
    )?;
    Ok(())
}

fn record(conn: &Connection, p: &Partition, completeness: Completeness) -> Result<()> {
    conn.execute(
        "INSERT INTO partition_manifest
             (year, month, rows, max_slot, completeness, sealed_unix)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(year, month) DO UPDATE SET
             rows = excluded.rows,
             max_slot = excluded.max_slot,
             completeness = excluded.completeness,
             sealed_unix = excluded.sealed_unix",
        rusqlite::params![
            p.year,
            p.month,
            p.rows,
            p.max_slot,
            completeness.as_wire(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0)
        ],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The column names are a CONTRACT with the frontend, so the schema has to
    /// parse and has to say what we think it says. A typo here is a file the
    /// reader cannot address by name.
    #[test]
    fn the_message_type_parses_and_names_its_columns() {
        let schema = parse_message_type(MESSAGE_TYPE).expect("schema parses");
        let names: Vec<&str> = schema.get_fields().iter().map(|f| f.name()).collect();
        assert_eq!(
            names,
            vec![
                "slot",
                "block_time",
                "tx_hash",
                "unit_name",
                "address",
                "amount"
            ]
        );
    }

    /// A month, by its calendar bounds. July 2026 in unix seconds.
    fn july() -> Month {
        Month {
            year: 2026,
            month: 7,
            start_unix: 1_782_950_400,
            end_unix: 1_785_628_799,
        }
    }

    #[test]
    fn a_month_fully_inside_the_window_seals() {
        let m = july();
        let w = (Some(m.start_unix - 1), Some(m.end_unix + 1));
        assert!(covers(w, &m));
        // Exact fit still counts — the bounds are inclusive.
        assert!(covers((Some(m.start_unix), Some(m.end_unix)), &m));
    }

    /// THE FORWARD CASE: coverage stops mid-month, so the month's TAIL is
    /// missing. Sealing it files a short month as complete.
    #[test]
    fn a_month_whose_tail_is_unwalked_does_not_seal() {
        let m = july();
        let w = (Some(m.start_unix), Some(m.end_unix - 1));
        assert!(!covers(w, &m));
    }

    /// THE REVERSE CASE, and the one the first version got backwards. A reverse
    /// walk starts at the tip, so its newest month is COMPLETE and the month
    /// containing the floor has its HEAD missing. The old rule skipped the
    /// newest month and sealed this one.
    #[test]
    fn a_month_whose_head_is_unwalked_does_not_seal() {
        let m = july();
        let w = (Some(m.start_unix + 1), Some(m.end_unix));
        assert!(
            !covers(w, &m),
            "the floor sits inside this month — its first days were never walked"
        );
    }

    /// The newest month is not special. On a reverse-built ledger it is the
    /// FIRST one finished, and refusing to seal it would strand the data a
    /// reader most wants.
    #[test]
    fn position_does_not_decide_sealability_coverage_does() {
        let older = july();
        let newer = Month {
            year: 2026,
            month: 8,
            start_unix: older.end_unix + 1,
            end_unix: older.end_unix + 2_678_400,
        };
        // Reverse walk: floor landed inside July, so August is whole and July
        // is not — the exact shape of the live SpaceBudz ledger.
        let w = (Some(older.start_unix + 1), Some(newer.end_unix));
        assert!(!covers(w, &older), "July is the partial one here");
        assert!(covers(w, &newer), "August is complete and must seal");
    }

    /// Unrecorded coverage seals NOTHING. A ledger that cannot state its bounds
    /// must not have months filed permanently on a guess.
    #[test]
    fn unknown_coverage_seals_nothing() {
        let m = july();
        assert!(!covers((None, None), &m));
        assert!(!covers((Some(m.start_unix), None), &m));
        assert!(!covers((None, Some(m.end_unix)), &m));
    }

    /// The manifest records the ledger's coverage verdict, spelled the same way
    /// it crosses the wire — so a consumer reading a partition and a consumer
    /// reading the feed are told the same thing in the same words.
    #[test]
    fn every_completeness_has_a_manifest_spelling() {
        for state in Completeness::ALL {
            assert!(
                !state.as_wire().is_empty(),
                "{state:?} must be recordable in the manifest"
            );
        }
    }
}
