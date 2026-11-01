//! The Mithril aggregator, read for the two facts an immutable directory cannot give.
//!
//! Both routes are read-only and cheap, and both are documented in Mithril's
//! `docs/website/openapi.current.yaml` under `/artifact/cardano-database*`:
//!
//! | endpoint | gives |
//! |---|---|
//! | `GET /artifact/cardano-database` | the recent snapshots, newest first, each with its beacon |
//! | `GET /artifact/cardano-database/digests` | the aggregator's whole digest map |
//!
//! The digest map is the one worth understanding, because it is broader than its
//! name suggests. `get_cardano_database_digest_list_message`
//! (`mithril-aggregator/src/services/message.rs`) returns
//! `get_all_immutable_file_digest()` — every immutable file part the aggregator has
//! ever digested, not the contents of one snapshot. Measured 2026-09-26 it held
//! 27,609 entries covering numbers 00000–09202 continuously, against a beacon of
//! 09202: the full range, not a window.
//!
//! That breadth is what makes it usable here. Sealed bytes never change, so a digest
//! published today still describes a file written years ago — which means an oracle
//! exists for a snapshot whose own listing expired long ago, and which no longer
//! appears in [`Aggregator::snapshots`] at all.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::Digests;

/// The release-mainnet aggregator, and the endpoint `tools/mitos-chain-walk` hands to
/// `mithril-client` for a download. Duplicated rather than shared because that crate
/// is a binary and cannot be depended on.
pub const MAINNET_AGGREGATOR: &str =
    "https://aggregator.release-mainnet.api.mithril.network/aggregator";

/// The release-mainnet genesis verification key, without which `mithril-client`
/// cannot verify the certificate chain and so will not download anything.
///
/// Public and fixed at genesis — the same value `tools/mitos-chain-walk/src/mithril.rs`
/// carries, duplicated for the same reason `MAINNET_AGGREGATOR` is: that crate is a
/// binary. Two copies of a constant that cannot change is the cheapest version of this.
pub const MAINNET_GENESIS_VKEY: &str = "5b3139312c36362c3134302c3138352c3133382c31312c3233372c3230372c3235302c3134342c32372c322c3138382c33302c31322c38312c3135352c3230342c31302c3137392c37352c32332c3133382c3139362c3231372c352c31342c32302c35372c37392c33392c3137365d";

/// A point on the Cardano chain at which a snapshot was signed.
#[derive(Debug, Clone, Deserialize)]
pub struct Beacon {
    /// Cardano chain epoch number.
    pub epoch: u64,
    /// Number of the last immutable file included in the snapshot, and the number
    /// the certificate's digest covers — inclusive at the top.
    pub immutable_file_number: u64,
}

/// One recent snapshot, as listed by `GET /artifact/cardano-database`.
///
/// Named after `mithril-common/src/messages/cardano_database_list.rs`, which is the
/// definition of the wire shape.
#[derive(Debug, Clone, Deserialize)]
pub struct Snapshot {
    /// Hash of the snapshot, as the aggregator identifies it.
    pub hash: String,
    /// The signed boundary: this snapshot holds `0..=beacon.immutable_file_number`.
    pub beacon: Beacon,
    /// Size of the uncompressed database, immutables **plus ancillary**.
    ///
    /// Not a check on an immutables-only tree: `total_db_size_uncompressed` is
    /// `ancillary_upload.size + immutables_upload.total_size`
    /// (`mithril-aggregator/src/artifact_builder/cardano_database.rs`), it sums
    /// numbers `1..=N` so it omits `00000` (`compressed_archive_snapshotter.rs`), and
    /// the ledger state it counts is present whether or not a download asked for it
    /// (`include_ancillary` defaults to false). Measured 2026-09-26: our
    /// immutables-only tree is 232,439,984,455 bytes against 234,969,539,140 for the
    /// same beacon, and two entries at beacon 9184 disagree by 15,641,525 bytes.
    pub total_db_size_uncompressed: u64,
    /// Version of the Cardano node that produced the snapshot.
    pub cardano_node_version: String,
    /// When the snapshot was created, RFC 3339.
    pub created_at: String,
}

/// One published digest, as listed by `GET /artifact/cardano-database/digests`.
#[derive(Debug, Clone, Deserialize)]
pub struct DigestEntry {
    /// Immutable file name, extension included — `09193.chunk`.
    pub immutable_file_name: String,
    /// SHA-256 of that file's contents, hex-encoded.
    pub digest: String,
}

/// A blocking client for the aggregator's read-only artifact routes.
pub struct Aggregator {
    http: reqwest::blocking::Client,
    base: String,
}

impl Aggregator {
    /// A client reading `base`.
    pub fn new(base: impl Into<String>) -> Result<Self> {
        let http = reqwest::blocking::Client::builder()
            .user_agent(concat!("mithril-snapshot/", env!("CARGO_PKG_VERSION")))
            .timeout(Duration::from_secs(120))
            .build()
            .context("building the HTTP client")?;

        Ok(Aggregator {
            http,
            base: base.into(),
        })
    }

    /// A client reading [`MAINNET_AGGREGATOR`].
    pub fn mainnet() -> Result<Self> {
        Self::new(MAINNET_AGGREGATOR)
    }

    /// The endpoint this client reads.
    pub fn base(&self) -> &str {
        &self.base
    }

    /// The most recent snapshots, newest first.
    pub fn snapshots(&self) -> Result<Vec<Snapshot>> {
        self.get("artifact/cardano-database")
    }

    /// The aggregator's whole immutable-file digest map.
    pub fn digests(&self) -> Result<Digests> {
        let entries: Vec<DigestEntry> = self.get("artifact/cardano-database/digests")?;

        Ok(Digests::from_parts(
            entries
                .into_iter()
                .map(|entry| (entry.immutable_file_name, entry.digest)),
        ))
    }

    fn get<T: serde::de::DeserializeOwned>(&self, route: &str) -> Result<T> {
        let url = format!("{}/{route}", self.base.trim_end_matches('/'));
        let response = self
            .http
            .get(&url)
            .send()
            .with_context(|| format!("GET {url}"))?;

        let status = response.status();
        if !status.is_success() {
            bail!("GET {url} returned {status}");
        }

        response
            .json::<T>()
            .with_context(|| format!("parsing the response from {url}"))
    }
}
