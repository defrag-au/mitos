//! The two workers: resolve a policy through Koios, then fetch the range.
//!
//! Both are blocking functions a thread can own. Resolution is two HTTP calls
//! (Koios and the aggregator); the download is `mithril-client` as a library,
//! with its own progress events feeding the screen.

use std::fs;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use mithril_snapshot::aggregator::{Aggregator, MAINNET_AGGREGATOR, MAINNET_GENESIS_VKEY};
use mithril_snapshot::ingest::{self, FeedbackReceiver, MithrilEvent, MithrilEventCardanoDatabase};
use mithril_snapshot::{Gap, Inventory, Reconciliation};
use mitos_koios::Koios;
use tokio::sync::oneshot;

use crate::app::{Event, FetchProgress, Outcome};
use crate::fmt;
use crate::manifest::{self, FetchedRange};
use crate::plan::{PolicyFloor, PolicyId, RangePlan, Tip};

/// How much of a gap one verified transfer covers.
///
/// A cancelled or failed batch costs at most this many chunks of re-download,
/// and resume is exact because only verified, lifted files count as present.
/// 200 files is ~5 GB at the measured ~25 MB per file.
pub const BATCH_FILES: u64 = 200;

pub type Observer = Arc<dyn Fn(Event) + Send + Sync>;

/// Ask Koios for the policy's assets, take the earliest, and place it against
/// the aggregator's newest signed snapshot.
pub fn resolve_blocking(
    policy: &PolicyId,
    generation: u64,
    observer: &Observer,
) -> Result<(PolicyFloor, Tip)> {
    observer(Event::Status {
        generation,
        message: "asking Koios for the policy's assets…".to_string(),
    });

    let koios =
        Koios::new(None, std::env::var("KOIOS_TOKEN").ok()).context("building the Koios client")?;
    let assets = koios
        .policy_asset_info(&policy.to_hex())
        .context("Koios policy_asset_info")?;
    let floor = crate::plan::floor_from_assets(&assets)?;

    observer(Event::Status {
        generation,
        message: format!(
            "Koios knows {} asset(s); reading the aggregator's signed snapshots…",
            fmt::count(floor.assets as u64)
        ),
    });

    let aggregator = Aggregator::mainnet().context("building the aggregator client")?;
    let snapshots = aggregator
        .snapshots()
        .context("listing the aggregator's Cardano database snapshots")?;
    let latest = snapshots
        .into_iter()
        .max_by_key(|snapshot| snapshot.beacon.immutable_file_number)
        .context("the aggregator listed no Cardano database snapshots")?;

    let tip = Tip {
        beacon: latest.beacon.immutable_file_number,
        epoch: latest.beacon.epoch,
        created_at: latest.created_at,
        total_bytes: latest.total_db_size_uncompressed,
    };

    Ok((floor, tip))
}

/// Fetch everything in `plan` that the store does not already hold.
///
/// Every range is proved against the snapshot certificate and checked against
/// the aggregator's digest map before it is moved in, so the store only ever
/// gains bytes Mithril signed. Running this again after a cancellation resumes
/// at the first missing immutable file.
pub fn fetch_blocking(
    plan: &RangePlan,
    generation: u64,
    observer: Observer,
    cancel: oneshot::Receiver<()>,
) -> Result<Outcome> {
    let destination = &plan.destination;
    let immutable = destination.join("immutable");
    fs::create_dir_all(&immutable).with_context(|| format!("creating {}", immutable.display()))?;

    let staging = destination.join(".mithril-fetch-staging");
    if staging.exists() {
        fs::remove_dir_all(&staging)
            .with_context(|| format!("clearing the old staging directory {}", staging.display()))?;
    }

    observer(Event::Status {
        generation,
        message: "reading the aggregator's digest map…".to_string(),
    });
    let aggregator = Aggregator::mainnet().context("building the aggregator client")?;
    let digests = aggregator
        .digests()
        .context("reading the aggregator's digest map")?;

    let inventory = Inventory::scan(&immutable)?;
    let reconciliation = Reconciliation::build(&inventory, &digests);

    // `fetchable` names numbers absent entirely. A crash between two renames
    // can leave one part of a triple, and that shape is named separately —
    // refusing is better than downloading a file `lift` will not overwrite.
    let incomplete: Vec<u64> = reconciliation
        .incomplete_published
        .iter()
        .map(|incomplete| incomplete.number)
        .filter(|number| *number >= plan.from && *number <= plan.to)
        .collect();
    if !incomplete.is_empty() {
        bail!(
            "the store has incomplete immutable triples in this range: {}. Remove those parts \
             (or run `mithril-snapshot validate {}`) and retry",
            incomplete
                .iter()
                .map(|number| format!("{number:05}"))
                .collect::<Vec<_>>()
                .join(", "),
            immutable.display()
        );
    }

    let runs: Vec<Gap> = reconciliation
        .fetch_runs(plan.to)
        .into_iter()
        .filter_map(|gap| {
            let from = gap.from.max(plan.from);
            let to = gap.to.min(plan.to);
            (from <= to).then_some(Gap { from, to })
        })
        .collect();

    let missing: u64 = runs.iter().map(|gap| gap.to - gap.from + 1).sum();
    let already_present = plan.files().saturating_sub(missing);

    if runs.is_empty() {
        observer(Event::Status {
            generation,
            message: "the store already holds every chunk in this range".to_string(),
        });
        return Ok(Outcome {
            already_present,
            ..Outcome::default()
        });
    }

    let batches: Vec<(u64, u64)> = runs
        .iter()
        .flat_map(|gap| batches(gap, BATCH_FILES))
        .collect();

    observer(Event::Status {
        generation,
        message: format!(
            "fetching {} immutable file(s) in {} verified batch(es)",
            fmt::count(missing),
            batches.len()
        ),
    });

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("starting a tokio runtime")?;
    let jobs = std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(4);
    let progress = Arc::new(ProgressSink {
        observer: observer.clone(),
        generation,
    });

    let started = std::time::Instant::now();
    let mut outcome = Outcome {
        already_present,
        ..Outcome::default()
    };

    runtime.block_on(async {
        tokio::select! {
            result = async {
                for (from, to) in &batches {
                    observer(Event::Status {
                        generation,
                        message: format!("fetching {from:05}–{to:05}"),
                    });
                    if staging.exists() {
                        fs::remove_dir_all(&staging).with_context(|| {
                            format!("clearing the staging directory {}", staging.display())
                        })?;
                    }

                    let ingested = ingest::ingest_range(
                        &ingest::IngestTarget {
                            endpoint: MAINNET_AGGREGATOR,
                            genesis_key: MAINNET_GENESIS_VKEY,
                            staging: &staging,
                            db_immutable: &immutable,
                            digests: &digests,
                            jobs,
                        },
                        *from,
                        *to,
                        progress.clone(),
                    )
                    .await?;

                    let range = FetchedRange {
                        from: *from,
                        to: *to,
                        snapshot_hash: ingested.snapshot_hash.clone(),
                        certificate_hash: ingested.certificate_hash.clone(),
                        beacon: ingested.beacon,
                        files: to - from + 1,
                        bytes: ingested.bytes,
                        at_unix: fmt::now_unix(),
                    };
                    manifest::record(plan, range.clone())?;

                    outcome.files += range.files;
                    outcome.bytes += range.bytes;
                    observer(Event::RangeDone {
                        generation,
                        range: range.clone(),
                        files: range.files,
                        bytes: range.bytes,
                    });
                    outcome.ranges.push(range);
                }
                Ok::<(), anyhow::Error>(())
            } => result,
            _ = cancel => Err(anyhow!("cancelled")),
        }
    })?;

    outcome.elapsed_secs = started.elapsed().as_secs();
    Ok(outcome)
}

/// Split an inclusive gap into batches of at most `size` files.
fn batches(gap: &Gap, size: u64) -> Vec<(u64, u64)> {
    let size = size.max(1);
    let mut out = Vec::new();
    let mut from = gap.from;
    while from <= gap.to {
        let to = from.saturating_add(size - 1).min(gap.to);
        out.push((from, to));
        match to.checked_add(1) {
            Some(next) => from = next,
            None => break,
        }
    }
    out
}

/// `mithril-client`'s feedback, projected onto the screen's vocabulary.
struct ProgressSink {
    observer: Observer,
    generation: u64,
}

#[async_trait]
impl FeedbackReceiver for ProgressSink {
    async fn handle_event(&self, event: MithrilEvent) {
        let progress = match event {
            MithrilEvent::CardanoDatabase(MithrilEventCardanoDatabase::Started {
                total_immutable_files,
                ..
            }) => FetchProgress::Started {
                total: total_immutable_files,
            },
            MithrilEvent::CardanoDatabase(
                MithrilEventCardanoDatabase::ImmutableDownloadStarted {
                    immutable_file_number,
                    size,
                    ..
                },
            ) => FetchProgress::FileStarted {
                number: immutable_file_number,
                size,
            },
            MithrilEvent::CardanoDatabase(
                MithrilEventCardanoDatabase::ImmutableDownloadProgress {
                    immutable_file_number,
                    downloaded_bytes,
                    ..
                },
            ) => FetchProgress::FileProgress {
                number: immutable_file_number,
                downloaded: downloaded_bytes,
            },
            MithrilEvent::CardanoDatabase(
                MithrilEventCardanoDatabase::ImmutableDownloadCompleted {
                    immutable_file_number,
                    ..
                },
            ) => FetchProgress::FileCompleted {
                number: immutable_file_number,
            },
            MithrilEvent::CardanoDatabase(MithrilEventCardanoDatabase::Completed { .. }) => {
                FetchProgress::Completed
            }
            _ => return,
        };

        (self.observer)(Event::Progress {
            generation: self.generation,
            progress,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_gap_is_split_into_contiguous_batches() {
        assert_eq!(
            batches(&Gap { from: 10, to: 14 }, 2),
            vec![(10, 11), (12, 13), (14, 14)]
        );
        assert_eq!(batches(&Gap { from: 10, to: 10 }, 200), vec![(10, 10)]);
        assert_eq!(batches(&Gap { from: 10, to: 20 }, 200), vec![(10, 20)]);
    }

    #[test]
    fn a_batch_never_wraps_past_u64_max() {
        assert_eq!(
            batches(
                &Gap {
                    from: u64::MAX - 1,
                    to: u64::MAX
                },
                200
            ),
            vec![(u64::MAX - 1, u64::MAX)]
        );
    }
}
