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

use std::path::Path;

use anyhow::{Result, anyhow, bail};
use mithril_client::cardano_database_client::{DownloadUnpackOptions, ImmutableFileRange};
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
