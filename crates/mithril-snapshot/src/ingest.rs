//! Ingesting immutable files from Mithril, certificate-verified.
//!
//! This is the sequence `mithril-client-cli` runs for `cardano-db download`, with one
//! step deliberately absent. In order:
//!
//! 1. `ClientBuilder::aggregator(endpoint, genesis_verification_key)` — the genesis key
//!    is what makes the chain check mean anything.
//! 2. `cardano_database_v2().list()`, then the highest beacon in it.
//! 3. `certificate().verify_chain(certificate_hash)` — the chain back to genesis.
//! 4. `download_unpack(&snapshot, &range, target, options)` — the bytes.
//! 5. `compute_merkle_proof(&certificate, &snapshot, &range, target)` — a proof over
//!    exactly the files that were just written.
//! 6. `compute_cardano_database_message` + `match_message` — that proof checked against
//!    the chain-verified certificate.
//!
//! **Missing: the CLI's failure path.** `mithril-client-cli`'s `shared_steps.rs` calls
//! `remove_dir_all(db_dir)` when the message does not match, which is a way to lose a
//! whole database to one bad transfer. Here a mismatch is an error, and the directory
//! it was written into is the caller's staging area, so the cost is the transfer.
//!
//! Nothing in this module is on the critical path for a reader: it is the ingest half,
//! and `lib.rs`'s `Reconciliation` is what the publish half reasons about.

use std::collections::BTreeMap;
use std::io::{IsTerminal, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use mithril_client::cardano_database_client::{DownloadUnpackOptions, ImmutableFileRange};
use mithril_client::feedback::{FeedbackReceiver, MithrilEvent, MithrilEventCardanoDatabase};
use mithril_client::{ClientBuilder, MessageBuilder};

/// What was fetched, and which signed snapshot and certificate it was checked against.
#[derive(Debug, Clone)]
pub struct Ingested {
    /// Hash of the snapshot the files came from.
    pub snapshot_hash: String,
    /// The beacon that snapshot is signed at. The range must fit inside it.
    pub beacon: u64,
    /// Hash of the certificate the files were verified against.
    pub certificate_hash: String,
}

/// Fetch immutable files `from..=to` from the newest snapshot into `target`, verified.
///
/// `target` is a *database* directory: the library creates `immutable/` inside it, and
/// the docs require the directory itself to exist first. Options are left at their
/// defaults, so `allow_override` is false and the library refuses to write over
/// anything — which is the intent, because the caller stages and the database proper is
/// never a directory this can write into.
pub async fn fetch(
    endpoint: &str,
    genesis_key: &str,
    target: &Path,
    from: u64,
    to: u64,
) -> Result<Ingested> {
    let client = ClientBuilder::aggregator(endpoint, genesis_key)
        .add_feedback_receiver(Arc::new(Progress::new()))
        .build()
        .map_err(|error| anyhow!("building the Mithril client: {error}"))?;
    let api = client.cardano_database_v2();

    let listed = api
        .list()
        .await
        .map_err(|error| anyhow!("listing Cardano database snapshots: {error}"))?;
    let newest = listed
        .iter()
        .max_by_key(|item| item.beacon.immutable_file_number)
        .ok_or_else(|| anyhow!("the aggregator listed no Cardano database snapshots"))?;

    let beacon = newest.beacon.immutable_file_number;
    if beacon < to {
        bail!(
            "the newest snapshot is signed at {beacon:05}, which does not reach {to:05} — \
             this aggregator cannot serve that range"
        );
    }

    // `list` yields list items; the download and the proof want the full snapshot, so
    // resolving the hash we just picked takes one more call.
    let snapshot = api
        .get(&newest.hash)
        .await
        .map_err(|error| anyhow!("getting snapshot {}: {error}", newest.hash))?
        .ok_or_else(|| {
            anyhow!(
                "the aggregator listed {} but does not serve it",
                newest.hash
            )
        })?;

    let certificate = client
        .certificate()
        .verify_chain(&snapshot.certificate_hash)
        .await
        .map_err(|error| {
            anyhow!(
                "verifying the certificate chain for {}: {error}",
                snapshot.certificate_hash
            )
        })?;

    let range = ImmutableFileRange::Range(from, to);
    api.download_unpack(&snapshot, &range, target, DownloadUnpackOptions::default())
        .await
        .map_err(|error| anyhow!("downloading immutable files {from}–{to}: {error}"))?;

    let proof = api
        .compute_merkle_proof(&certificate, &snapshot, &range, target)
        .await
        .map_err(|error| anyhow!("computing the Merkle proof for {from}–{to}: {error}"))?;
    let message = MessageBuilder::new()
        .compute_cardano_database_message(&certificate, &proof)
        .await
        .map_err(|error| anyhow!("computing the certificate message: {error}"))?;

    if !certificate.match_message(&message) {
        bail!(
            "{from:05}–{to:05} did not verify against certificate {} — these are not the files \
             the chain signed",
            snapshot.certificate_hash
        );
    }

    Ok(Ingested {
        snapshot_hash: snapshot.hash.clone(),
        beacon,
        certificate_hash: snapshot.certificate_hash.clone(),
    })
}

/// Renders `mithril-client`'s own progress events as one live line.
///
/// Nothing here guesses at a download's state: `add_feedback_receiver` is how the
/// library exposes what its downloader is doing, and these are the same events
/// `mithril-client-cli` renders. Standard library only — a drawn bar would want
/// `indicatif`, which buys a shape rather than information.
///
/// Events arrive from as many tasks as there are parallel downloads
/// (`DownloadUnpackOptions::max_parallel_downloads`, 100 by default), so the state
/// lives behind a mutex and the repaint is rate-limited; without that, every chunk of
/// every archive is a repaint.
struct Progress {
    tty: bool,
    state: Mutex<State>,
}

/// How often the live line may be repainted.
const REPAINT: Duration = Duration::from_millis(100);

#[derive(Default)]
struct State {
    /// Immutable files expected, as the library reported them.
    total: u64,
    /// Archive size per immutable file.
    sizes: BTreeMap<u64, u64>,
    /// Bytes received so far, per immutable file still downloading.
    inflight: BTreeMap<u64, u64>,
    /// Immutable files that have finished.
    completed: u64,
    /// Length of the line currently on screen, so it can be overwritten cleanly.
    drawn: usize,
    last_draw: Option<Instant>,
}

impl Progress {
    fn new() -> Self {
        Self::with_tty(std::io::stdout().is_terminal())
    }

    fn with_tty(tty: bool) -> Self {
        Progress {
            tty,
            state: Mutex::new(State::default()),
        }
    }

    /// Repaint, unless a repaint just happened and `force` is unset.
    ///
    /// Not a terminal → no control characters, one line per milestone instead.
    fn draw(&self, state: &mut State, force: bool) {
        let recent = state.last_draw.is_some_and(|last| last.elapsed() < REPAINT);
        if recent && !force {
            return;
        }
        state.last_draw = Some(Instant::now());

        let line = summary(state);
        if self.tty {
            let padding = state.drawn.saturating_sub(line.len());
            print!("\r{line}{}", " ".repeat(padding));
            let _ = std::io::stdout().flush();
            state.drawn = line.len();
        } else if force {
            println!("{line}");
        }
    }

    /// Terminate the live line and report what arrived.
    fn finish(&self, state: &mut State) {
        if self.tty && state.drawn > 0 {
            println!();
            state.drawn = 0;
        }
        println!(
            "downloaded  {}/{} immutable files · {} received",
            state.completed,
            state.total,
            human_bytes(received(state))
        );
    }
}

/// Bytes received so far: finished archives at full size, plus what is in flight.
fn received(state: &State) -> u64 {
    state
        .sizes
        .iter()
        .map(|(number, size)| match state.inflight.get(number) {
            Some(downloaded) => *downloaded,
            None => *size,
        })
        .sum()
}

/// The live line — the one thing here worth reading, so it is a pure function.
fn summary(state: &State) -> String {
    format!(
        "downloading  {}/{} immutable files · {} received",
        state.completed,
        state.total,
        human_bytes(received(state))
    )
}

/// Bytes in binary units, because this is a download, not a disk.
fn human_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;

    let bytes = bytes as f64;
    if bytes >= GIB {
        format!("{:.1} GiB", bytes / GIB)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes / MIB)
    } else {
        format!("{:.0} KiB", bytes / KIB)
    }
}

#[async_trait]
impl FeedbackReceiver for Progress {
    async fn handle_event(&self, event: MithrilEvent) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            // A poisoned lock means a repaint tripped over a closed pipe. Progress
            // reporting is not worth failing a download over.
            Err(_) => return,
        };

        match event {
            MithrilEvent::CertificateChainValidationStarted { .. } => {
                println!("verifying   the certificate chain…");
            }
            MithrilEvent::CertificateChainValidated { .. } => {
                println!("verified    the certificate chain");
            }
            MithrilEvent::CardanoDatabase(MithrilEventCardanoDatabase::Started {
                total_immutable_files,
                ..
            }) => {
                state.total = total_immutable_files;
                self.draw(&mut state, true);
            }
            MithrilEvent::CardanoDatabase(
                MithrilEventCardanoDatabase::ImmutableDownloadStarted {
                    immutable_file_number,
                    size,
                    ..
                },
            ) => {
                state.sizes.insert(immutable_file_number, size);
                state.inflight.insert(immutable_file_number, 0);
                self.draw(&mut state, false);
            }
            MithrilEvent::CardanoDatabase(
                MithrilEventCardanoDatabase::ImmutableDownloadProgress {
                    immutable_file_number,
                    downloaded_bytes,
                    ..
                },
            ) => {
                state
                    .inflight
                    .insert(immutable_file_number, downloaded_bytes);
                self.draw(&mut state, false);
            }
            MithrilEvent::CardanoDatabase(
                MithrilEventCardanoDatabase::ImmutableDownloadCompleted {
                    immutable_file_number,
                    ..
                },
            ) => {
                state.inflight.remove(&immutable_file_number);
                state.completed += 1;
                self.draw(&mut state, true);
            }
            MithrilEvent::CardanoDatabase(MithrilEventCardanoDatabase::Completed { .. }) => {
                self.finish(&mut state);
            }
            // Snapshot (v1) and ancillary events: this tool asks for neither.
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn started(total: u64) -> MithrilEvent {
        MithrilEvent::CardanoDatabase(MithrilEventCardanoDatabase::Started {
            download_id: "download".to_string(),
            total_immutable_files: total,
            include_ancillary: false,
        })
    }

    fn file_started(number: u64, size: u64) -> MithrilEvent {
        MithrilEvent::CardanoDatabase(MithrilEventCardanoDatabase::ImmutableDownloadStarted {
            immutable_file_number: number,
            download_id: "download".to_string(),
            size,
        })
    }

    fn file_progress(number: u64, downloaded_bytes: u64, size: u64) -> MithrilEvent {
        MithrilEvent::CardanoDatabase(MithrilEventCardanoDatabase::ImmutableDownloadProgress {
            immutable_file_number: number,
            download_id: "download".to_string(),
            downloaded_bytes,
            size,
        })
    }

    fn file_completed(number: u64) -> MithrilEvent {
        MithrilEvent::CardanoDatabase(MithrilEventCardanoDatabase::ImmutableDownloadCompleted {
            immutable_file_number: number,
            download_id: "download".to_string(),
        })
    }

    /// The denominator is the library's, not a count of events we happened to see.
    #[tokio::test]
    async fn a_started_event_sets_the_total_and_a_finished_file_is_counted() {
        let progress = Progress::with_tty(false);
        progress.handle_event(started(2)).await;
        progress.handle_event(file_started(0, 1_048_576)).await;
        progress.handle_event(file_started(1, 2_097_152)).await;
        progress
            .handle_event(file_progress(0, 524_288, 1_048_576))
            .await;
        progress.handle_event(file_completed(0)).await;

        let state = progress.state.lock().unwrap();
        assert_eq!(state.total, 2);
        assert_eq!(state.completed, 1);
        // The finished archive counts at its full size; the other has not ticked.
        assert_eq!(received(&state), 1_048_576);
        assert_eq!(
            summary(&state),
            "downloading  1/2 immutable files · 1.0 MiB received"
        );
    }

    #[tokio::test]
    async fn in_flight_bytes_come_from_the_progress_events() {
        let progress = Progress::with_tty(false);
        progress.handle_event(started(1)).await;
        progress.handle_event(file_started(7, 4_194_304)).await;
        progress
            .handle_event(file_progress(7, 2_097_152, 4_194_304))
            .await;

        let state = progress.state.lock().unwrap();
        assert_eq!(state.completed, 0);
        assert_eq!(received(&state), 2_097_152);
    }

    #[test]
    fn bytes_are_rendered_in_binary_units() {
        assert_eq!(human_bytes(0), "0 KiB");
        assert_eq!(human_bytes(4096), "4 KiB");
        assert_eq!(human_bytes(1_048_576), "1.0 MiB");
        assert_eq!(human_bytes(3 * 1024 * 1024 * 1024), "3.0 GiB");
    }
}
