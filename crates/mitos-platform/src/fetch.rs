//! Pulling an announced artifact into the content store — D8's fetch half.
//!
//! A worker announces DIGESTS, never a URL. The host derives where to fetch
//! from out of the client's authenticated identity
//! (`mitos_lifecycle::artifact::ArtifactSource`), so the question "may I
//! fetch this URL" never arises — a caller can only ever make the host pull a
//! fixed path from a hostname an operator wrote into the namespace registry.
//!
//! # Nothing unverified reaches the CAS
//!
//! [`ArtifactFetcher`] brings bytes back; [`verify_and_store`] decides whether
//! they are what was announced. Seven checks, in order, each its own
//! [`Refusal`] — and `put_artifact` / `put_manifest` run only after the last
//! of them. A refused artifact leaves `_cas/` exactly as it was, which is what
//! the tests assert rather than trusting the ordering to stay as written.
//!
//! # The size cap is enforced while STREAMING
//!
//! Not by checking `Content-Length` (a header the far end chooses) and not by
//! buffering and then measuring (which is the memory the cap exists to
//! bound). The reader stops at `max + 1` bytes and refuses.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use mitos_lifecycle::artifact::ArtifactSource;
use mitos_lifecycle::ids::{ModuleId, Revision, Sha256Hex};

use crate::manifest::{Manifest, sha256_hex};
use crate::storage::{ModuleStorage, StorageError};

/// A wasm artifact. 16 MiB is roughly four times the largest module built
/// here; a module past it is a mistake rather than a deployment.
pub const MAX_WASM_BYTES: usize = 16 * 1024 * 1024;
/// A manifest. Generous — the biggest in this repo is under 2 KiB — but a
/// manifest is TOML the host parses, so the cap is what stops a hostile one
/// being a memory bomb.
pub const MAX_MANIFEST_BYTES: usize = 256 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("http: {0}")]
    Http(String),
    #[error("status {0}")]
    Status(u16),
    #[error("body exceeds {max} bytes")]
    TooLarge { max: usize },
}

/// Brings bytes back from a URL. A trait so every test in this order runs
/// against a fake — no test here touches the network.
#[async_trait::async_trait]
pub trait ArtifactFetcher: Send + Sync {
    /// Fetch `url`, refusing at `max + 1` bytes read.
    async fn fetch(&self, url: &str, max: usize) -> Result<Vec<u8>, FetchError>;
}

/// Why an announced artifact was refused. Each variant's `Display` becomes
/// the reason on the row, the event ring and the subscribe response, so they
/// are written to be read by an operator who has only that line.
#[derive(Debug, thiserror::Error)]
pub enum Refusal {
    #[error("fetching {what}: {source}")]
    Fetch {
        what: &'static str,
        #[source]
        source: FetchError,
    },
    #[error("announced wasm {announced} but the bytes served hash to {actual}")]
    WasmDigest {
        announced: Sha256Hex,
        actual: String,
    },
    #[error("announced manifest {announced} but the bytes served hash to {actual}")]
    ManifestDigest {
        announced: Sha256Hex,
        actual: String,
    },
    #[error("the served manifest does not parse: {0}")]
    ManifestParse(String),
    #[error("the served manifest is for module `{found}`, not `{announced}`")]
    ManifestModule { announced: ModuleId, found: String },
    #[error("the served manifest describes wasm {found}, not the announced {announced}")]
    ManifestWasm { announced: Sha256Hex, found: String },
    #[error(
        "announced revision {announced} but the served manifest declares {found} — the rollback \
         check ran against the announced number, so this artifact was checked against a lie"
    )]
    RevisionMismatch { announced: u32, found: u32 },
    #[error("the served manifest is refused by this host: {0}")]
    ManifestRefused(#[from] crate::manifest::ManifestError),
    #[error("wasmtime refused the component: {0}")]
    NotAComponent(#[from] crate::inspect::ComponentRefused),
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
}

/// What an announcement pins, once the host has derived where to get it.
#[derive(Debug, Clone)]
pub struct Announced {
    pub module: ModuleId,
    pub wasm: Sha256Hex,
    pub manifest: Sha256Hex,
    pub revision: Revision,
}

/// Fetches, verifies and stores announced artifacts, de-duplicating
/// concurrent fetches of one build.
pub struct ArtifactPuller {
    storage: ModuleStorage,
    fetcher: Arc<dyn ArtifactFetcher>,
    /// One `Mutex` per wasm digest. Two clients announcing the same build —
    /// or one client's 91 companions — cause ONE download: the others wait
    /// here and then find the artifact already in the CAS.
    in_flight: Mutex<HashMap<Sha256Hex, Arc<tokio::sync::Mutex<()>>>>,
}

impl ArtifactPuller {
    pub fn new(storage: ModuleStorage, fetcher: Arc<dyn ArtifactFetcher>) -> Self {
        Self {
            storage,
            fetcher,
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    fn gate(&self, wasm: &Sha256Hex) -> Arc<tokio::sync::Mutex<()>> {
        let mut map = self.in_flight.lock().expect("in-flight mutex");
        map.entry(wasm.clone()).or_default().clone()
    }

    /// Make the announced artifact present in the CAS.
    ///
    /// Already there ⇒ returns at once, having touched no network. The
    /// manifest's upgrade ledger is what the caller feeds back into the move.
    pub async fn ensure(
        &self,
        client: &mitos_lifecycle::ids::ClientId,
        announced: &Announced,
    ) -> Result<Manifest, Refusal> {
        let gate = self.gate(&announced.wasm);
        let _held = gate.lock().await;

        // Both halves must be present: an earlier run could have stored the
        // wasm and died before the manifest.
        if let Some(manifest) = self.storage.read_manifest_by_digest(&announced.manifest)?
            && self.storage.artifact_path(&announced.wasm).exists()
        {
            // Stored ⇒ the BYTES were verified. What this client claims
            // about them has not been.
            agrees_with(&manifest, announced)?;
            return Ok(manifest);
        }

        // The ONE place a URL is built. `for_client` refuses a client id that
        // is not a hostname; the caller has already turned that into a
        // refusal, so reaching here with one is a bug rather than input.
        let source = ArtifactSource::for_client(client, &announced.module, &announced.wasm)
            .map_err(|e| Refusal::ManifestParse(e.to_string()))?;

        let wasm_bytes = self
            .fetcher
            .fetch(&source.wasm_url, MAX_WASM_BYTES)
            .await
            .map_err(|source| Refusal::Fetch {
                what: "wasm",
                source,
            })?;
        let manifest_bytes = self
            .fetcher
            .fetch(&source.manifest_url, MAX_MANIFEST_BYTES)
            .await
            .map_err(|source| Refusal::Fetch {
                what: "manifest",
                source,
            })?;

        verify_and_store(&self.storage, announced, &wasm_bytes, &manifest_bytes)
    }
}

/// The checks that are about THIS ANNOUNCEMENT rather than about the bytes:
/// the manifest describes the announced module, the announced wasm, at the
/// announced revision.
///
/// Separate because they must run on the CAS short-circuit too. The digests
/// and the host's ABI gate are properties of the artifact, settled once when
/// it was stored; these are properties of what a CLIENT just claimed about
/// it. Skip them for a stored artifact and a client could announce revision 9
/// beside the digests of a stored revision-3 build — the rollback rule would
/// pass the 9, and the move would carry the 3.
fn agrees_with(manifest: &Manifest, announced: &Announced) -> Result<(), Refusal> {
    // 3b. It is about the module that was announced.
    if manifest.module.id != announced.module.as_str() {
        return Err(Refusal::ManifestModule {
            announced: announced.module.clone(),
            found: manifest.module.id.clone(),
        });
    }

    // 4. …and about THIS wasm. Without this a manifest could be a valid,
    //    correctly-digested description of an entirely different artifact.
    if manifest.module.sha256 != announced.wasm.as_str() {
        return Err(Refusal::ManifestWasm {
            announced: announced.wasm.clone(),
            found: manifest.module.sha256.clone(),
        });
    }

    // 5. The revision agrees. The rollback check already ran against the
    //    ANNOUNCED number, so an artifact that disagrees with it was checked
    //    against a lie.
    if manifest.lifecycle.revision != announced.revision {
        return Err(Refusal::RevisionMismatch {
            announced: announced.revision.0,
            found: manifest.lifecycle.revision.0,
        });
    }
    Ok(())
}

/// The seven checks, in order, then the two writes.
///
/// Split out from the fetch so every refusal has a test that needs no
/// fetcher at all.
pub fn verify_and_store(
    storage: &ModuleStorage,
    announced: &Announced,
    wasm_bytes: &[u8],
    manifest_bytes: &[u8],
) -> Result<Manifest, Refusal> {
    // 1. The wasm is the wasm that was announced.
    let wasm_sha = sha256_hex(wasm_bytes);
    if wasm_sha != announced.wasm.as_str() {
        return Err(Refusal::WasmDigest {
            announced: announced.wasm.clone(),
            actual: wasm_sha,
        });
    }

    // 2. …and the manifest is the manifest that was announced. Over the RAW
    //    bytes as fetched: re-serialising first would hash the host's
    //    rendering rather than the file, and the two need not agree.
    let manifest_sha = sha256_hex(manifest_bytes);
    if manifest_sha != announced.manifest.as_str() {
        return Err(Refusal::ManifestDigest {
            announced: announced.manifest.clone(),
            actual: manifest_sha,
        });
    }

    // 3. It parses, and it is about the module that was announced.
    let manifest_text =
        std::str::from_utf8(manifest_bytes).map_err(|e| Refusal::ManifestParse(e.to_string()))?;
    let manifest =
        Manifest::parse(manifest_text).map_err(|e| Refusal::ManifestParse(e.to_string()))?;
    // 3b, 4, 5.
    agrees_with(&manifest, announced)?;

    // 6. This host will run it: ABI major, WIT revision, trap policy, the
    //    wasm's own sha and size, and the upgrade ledger.
    manifest.validate_against_host(
        wasm_bytes,
        &[(
            crate::registry_v2::HOST_ABI_MAJOR_V2,
            "mitos:platform-v2/mitos-module-v2",
        )],
    )?;

    // 7. …and wasmtime agrees it is a component.
    crate::inspect::validate_component(wasm_bytes)?;

    // Only now. `put_artifact` re-checks the wasm sha and writes its own
    // re-serialised copy of the manifest beside the wasm; `put_manifest`
    // keeps the RAW file under its own digest, which is why both exist.
    storage.put_artifact(&manifest, wasm_bytes)?;
    storage.put_manifest(manifest_bytes)?;
    Ok(manifest)
}

/// The production fetcher.
///
/// `redirect::Policy::none()` because the URL is derived from an
/// operator-named hostname and a redirect is how that guarantee would be
/// escaped. The 30s timeout is total, not per-read.
pub struct HttpFetcher {
    client: reqwest::Client,
}

impl HttpFetcher {
    pub fn new() -> Result<Self, FetchError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| FetchError::Http(e.to_string()))?;
        Ok(Self { client })
    }
}

#[async_trait::async_trait]
impl ArtifactFetcher for HttpFetcher {
    async fn fetch(&self, url: &str, max: usize) -> Result<Vec<u8>, FetchError> {
        let mut response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| FetchError::Http(e.to_string()))?;
        if !response.status().is_success() {
            return Err(FetchError::Status(response.status().as_u16()));
        }

        // Read chunk by chunk and stop the moment the cap is passed, so the
        // cap bounds what is ever HELD. `chunk()` rather than `bytes_stream()`
        // because the latter needs reqwest's `stream` feature, which would be
        // a workspace-wide change for no extra guarantee.
        //
        // Neither `Content-Length` (a number the far end chooses) nor
        // `body.len()` after `bytes()` (the memory is already spent) is a cap.
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| FetchError::Http(e.to_string()))?
        {
            if body.len() + chunk.len() > max {
                return Err(FetchError::TooLarge { max });
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}

#[cfg(test)]
mod tests {
    use super::testing::FakeFetcher;
    use super::*;
    use mitos_lifecycle::ids::ClientId;

    const MODULE: &str = "ownership-indexer";
    const CLIENT: &str = "ownership.cnft.dev";

    struct Fixture {
        _tmp: tempfile::TempDir,
        storage: ModuleStorage,
        wasm: Vec<u8>,
        manifest_bytes: String,
        announced: Announced,
    }

    /// A build that WOULD pass every check, so each test breaks exactly one
    /// thing and nothing else can be the reason it was refused.
    fn good() -> Fixture {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());
        let wasm = real_component();
        let manifest = crate::companions::testing::manifest_for(MODULE, &wasm);
        let manifest_bytes = manifest.to_toml().unwrap();
        let announced = Announced {
            module: ModuleId::parse(MODULE).unwrap(),
            wasm: Sha256Hex::parse(&sha256_hex(&wasm)).unwrap(),
            manifest: Sha256Hex::parse(&sha256_hex(manifest_bytes.as_bytes())).unwrap(),
            revision: manifest.lifecycle.revision,
        };
        Fixture {
            _tmp: tmp,
            storage,
            wasm,
            manifest_bytes,
            announced,
        }
    }

    /// A minimal real wasm component — `validate_component` is check 7 and a
    /// fake byte string would fail it before the check under test.
    fn real_component() -> Vec<u8> {
        // The canonical empty component: the wasm preamble with the
        // component-model layer (0x0d 0x00 0x01 0x00) rather than a module's.
        vec![0x00, 0x61, 0x73, 0x6d, 0x0d, 0x00, 0x01, 0x00]
    }

    /// Nothing may reach the content store unless every check passed.
    fn cas_is_empty(storage: &ModuleStorage) -> bool {
        !storage.root().join("_cas").exists()
    }

    #[test]
    fn a_good_artifact_is_stored() {
        let f = good();
        verify_and_store(
            &f.storage,
            &f.announced,
            &f.wasm,
            f.manifest_bytes.as_bytes(),
        )
        .expect("a build that passes every check");
        assert!(f.storage.artifact_path(&f.announced.wasm).exists());
        assert!(
            f.storage
                .read_manifest_by_digest(&f.announced.manifest)
                .unwrap()
                .is_some(),
            "the RAW manifest must be stored under its own digest",
        );
    }

    #[test]
    fn refusal_1_wasm_digest() {
        let f = good();
        let err = verify_and_store(
            &f.storage,
            &f.announced,
            b"not the wasm that was announced",
            f.manifest_bytes.as_bytes(),
        )
        .unwrap_err();
        assert!(matches!(err, Refusal::WasmDigest { .. }), "{err}");
        assert!(cas_is_empty(&f.storage));
    }

    #[test]
    fn refusal_2_manifest_digest() {
        let f = good();
        let err = verify_and_store(&f.storage, &f.announced, &f.wasm, b"# a different manifest")
            .unwrap_err();
        assert!(matches!(err, Refusal::ManifestDigest { .. }), "{err}");
        assert!(cas_is_empty(&f.storage));
    }

    #[test]
    fn refusal_3_manifest_is_for_another_module() {
        let mut f = good();
        let other = crate::companions::testing::manifest_for("collection-holders", &f.wasm);
        f.manifest_bytes = other.to_toml().unwrap();
        f.announced.manifest = Sha256Hex::parse(&sha256_hex(f.manifest_bytes.as_bytes())).unwrap();

        let err = verify_and_store(
            &f.storage,
            &f.announced,
            &f.wasm,
            f.manifest_bytes.as_bytes(),
        )
        .unwrap_err();
        assert!(matches!(err, Refusal::ManifestModule { .. }), "{err}");
        assert!(cas_is_empty(&f.storage));
    }

    /// A manifest can be well-formed, correctly digested, for the right
    /// module — and describe an entirely different artifact.
    #[test]
    fn refusal_4_manifest_is_about_another_wasm() {
        let mut f = good();
        let other = crate::companions::testing::manifest_for(MODULE, b"some other wasm");
        f.manifest_bytes = other.to_toml().unwrap();
        f.announced.manifest = Sha256Hex::parse(&sha256_hex(f.manifest_bytes.as_bytes())).unwrap();

        let err = verify_and_store(
            &f.storage,
            &f.announced,
            &f.wasm,
            f.manifest_bytes.as_bytes(),
        )
        .unwrap_err();
        assert!(matches!(err, Refusal::ManifestWasm { .. }), "{err}");
        assert!(cas_is_empty(&f.storage));
    }

    /// The rollback check ran against the ANNOUNCED revision, so an artifact
    /// declaring a different one was checked against a lie.
    #[test]
    fn refusal_5_revision_disagrees_with_the_announcement() {
        let mut f = good();
        f.announced.revision = Revision(9);
        let err = verify_and_store(
            &f.storage,
            &f.announced,
            &f.wasm,
            f.manifest_bytes.as_bytes(),
        )
        .unwrap_err();
        match err {
            Refusal::RevisionMismatch { announced, found } => {
                assert_eq!((announced, found), (9, 0));
            }
            other => panic!("expected RevisionMismatch, got {other}"),
        }
        assert!(cas_is_empty(&f.storage));
    }

    #[test]
    fn refusal_6_the_host_refuses_the_manifest() {
        let mut f = good();
        let mut manifest = crate::companions::testing::manifest_for(MODULE, &f.wasm);
        // A WIT revision this host does not serve.
        manifest.abi.wit_sha = Some("00".repeat(32));
        f.manifest_bytes = manifest.to_toml().unwrap();
        f.announced.manifest = Sha256Hex::parse(&sha256_hex(f.manifest_bytes.as_bytes())).unwrap();

        let err = verify_and_store(
            &f.storage,
            &f.announced,
            &f.wasm,
            f.manifest_bytes.as_bytes(),
        )
        .unwrap_err();
        assert!(matches!(err, Refusal::ManifestRefused(_)), "{err}");
        assert!(cas_is_empty(&f.storage));
    }

    #[test]
    fn refusal_7_wasmtime_refuses_the_component() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());
        // Well-formed everything, but not a component.
        let wasm = b"this is not a wasm component at all".to_vec();
        let manifest = crate::companions::testing::manifest_for(MODULE, &wasm);
        let manifest_bytes = manifest.to_toml().unwrap();
        let announced = Announced {
            module: ModuleId::parse(MODULE).unwrap(),
            wasm: Sha256Hex::parse(&sha256_hex(&wasm)).unwrap(),
            manifest: Sha256Hex::parse(&sha256_hex(manifest_bytes.as_bytes())).unwrap(),
            revision: Revision(0),
        };

        let err =
            verify_and_store(&storage, &announced, &wasm, manifest_bytes.as_bytes()).unwrap_err();
        assert!(matches!(err, Refusal::NotAComponent(_)), "{err}");
        assert!(cas_is_empty(&storage));
    }

    fn puller(f: &Fixture, fetcher: Arc<FakeFetcher>) -> ArtifactPuller {
        ArtifactPuller::new(f.storage.clone(), fetcher)
    }

    fn urls(f: &Fixture) -> ArtifactSource {
        ArtifactSource::for_client(
            &ClientId::parse(CLIENT).unwrap(),
            &f.announced.module,
            &f.announced.wasm,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn the_size_cap_stops_the_read_not_the_buffer() {
        let f = good();
        let fetcher = Arc::new(FakeFetcher::new());
        let source = urls(&f);
        // Bigger than the wasm cap. The fake enforces the SAME `max` the
        // real one streams against, so this pins the contract rather than
        // the implementation.
        fetcher.serve(&source.wasm_url, vec![0u8; MAX_WASM_BYTES + 1]);
        fetcher.serve(&source.manifest_url, f.manifest_bytes.clone().into_bytes());

        let err = puller(&f, fetcher)
            .ensure(&ClientId::parse(CLIENT).unwrap(), &f.announced)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                Refusal::Fetch {
                    source: FetchError::TooLarge { .. },
                    ..
                }
            ),
            "{err}",
        );
        assert!(cas_is_empty(&f.storage));
    }

    /// Two clients announcing one build — or one client's 91 companions —
    /// cause ONE download.
    #[tokio::test]
    async fn two_announcements_of_one_build_fetch_once() {
        let f = good();
        let fetcher = Arc::new(FakeFetcher::new());
        let source = urls(&f);
        fetcher.serve(&source.wasm_url, f.wasm.clone());
        fetcher.serve(&source.manifest_url, f.manifest_bytes.clone().into_bytes());
        let puller = puller(&f, fetcher.clone());
        let client = ClientId::parse(CLIENT).unwrap();

        puller.ensure(&client, &f.announced).await.unwrap();
        let after_first = fetcher.calls();
        assert_eq!(after_first, 2, "one wasm + one manifest");

        for _ in 0..10 {
            puller.ensure(&client, &f.announced).await.unwrap();
        }
        assert_eq!(
            fetcher.calls(),
            after_first,
            "an artifact already in the CAS must touch no network",
        );
    }

    #[tokio::test]
    async fn an_artifact_already_in_the_cas_touches_no_network() {
        let f = good();
        // Put it there by the front door first.
        verify_and_store(
            &f.storage,
            &f.announced,
            &f.wasm,
            f.manifest_bytes.as_bytes(),
        )
        .unwrap();

        // A fetcher that serves NOTHING: any call is a 404 and would fail.
        let fetcher = Arc::new(FakeFetcher::new());
        puller(&f, fetcher.clone())
            .ensure(&ClientId::parse(CLIENT).unwrap(), &f.announced)
            .await
            .expect("already present");
        assert_eq!(fetcher.calls(), 0);
    }

    /// The rollback rule runs on the ANNOUNCED revision, before any fetch. So
    /// a stored artifact must still be held to it: otherwise a client names
    /// the digests of an old build already in the CAS, claims a revision high
    /// enough to pass, and the move carries the old build.
    #[tokio::test]
    async fn a_stored_artifact_is_still_held_to_the_announced_revision() {
        let f = good();
        verify_and_store(
            &f.storage,
            &f.announced,
            &f.wasm,
            f.manifest_bytes.as_bytes(),
        )
        .unwrap();

        let lying = Announced {
            revision: mitos_lifecycle::ids::Revision(f.announced.revision.0 + 6),
            ..f.announced.clone()
        };
        let err = puller(&f, Arc::new(FakeFetcher::new()))
            .ensure(&ClientId::parse(CLIENT).unwrap(), &lying)
            .await
            .unwrap_err();
        assert!(matches!(err, Refusal::RevisionMismatch { .. }), "{err}");
    }

    /// Half-stored is not stored: an earlier run could have written the wasm
    /// and died before the manifest.
    #[tokio::test]
    async fn a_half_stored_artifact_is_fetched_again() {
        let f = good();
        let manifest = crate::companions::testing::manifest_for(MODULE, &f.wasm);
        f.storage.put_artifact(&manifest, &f.wasm).unwrap();

        let fetcher = Arc::new(FakeFetcher::new());
        let source = urls(&f);
        fetcher.serve(&source.wasm_url, f.wasm.clone());
        fetcher.serve(&source.manifest_url, f.manifest_bytes.clone().into_bytes());

        puller(&f, fetcher.clone())
            .ensure(&ClientId::parse(CLIENT).unwrap(), &f.announced)
            .await
            .unwrap();
        assert_eq!(fetcher.calls(), 2, "the missing manifest must be fetched");
    }

    #[tokio::test]
    async fn an_unreachable_artifact_is_a_refusal_not_a_panic() {
        let f = good();
        let fetcher = Arc::new(FakeFetcher::new());
        let err = puller(&f, fetcher)
            .ensure(&ClientId::parse(CLIENT).unwrap(), &f.announced)
            .await
            .unwrap_err();
        assert!(matches!(err, Refusal::Fetch { .. }), "{err}");
        assert!(cas_is_empty(&f.storage));
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Serves canned bytes and counts calls, so a test can assert a fetch
    /// happened ONCE or not at all.
    #[derive(Default)]
    pub(crate) struct FakeFetcher {
        responses: Mutex<HashMap<String, Result<Vec<u8>, String>>>,
        calls: AtomicUsize,
        gate: Mutex<Option<Arc<tokio::sync::Semaphore>>>,
    }

    impl FakeFetcher {
        pub(crate) fn new() -> Self {
            Self::default()
        }

        pub(crate) fn serve(&self, url: &str, bytes: Vec<u8>) {
            self.responses
                .lock()
                .expect("fake fetcher")
                .insert(url.to_owned(), Ok(bytes));
        }

        pub(crate) fn fail(&self, url: &str, why: &str) {
            self.responses
                .lock()
                .expect("fake fetcher")
                .insert(url.to_owned(), Err(why.to_owned()));
        }

        /// Every fetch from now on waits until permits are added to the
        /// returned semaphore. A semaphore rather than a `Notify`: a release
        /// that arrives BEFORE the fetch starts waiting must not be lost.
        pub(crate) fn hold(&self) -> Arc<tokio::sync::Semaphore> {
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            *self.gate.lock().expect("fake fetcher") = Some(gate.clone());
            gate
        }

        pub(crate) fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
    }

    #[async_trait::async_trait]
    impl ArtifactFetcher for FakeFetcher {
        async fn fetch(&self, url: &str, max: usize) -> Result<Vec<u8>, FetchError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let gate = self.gate.lock().expect("fake fetcher").clone();
            if let Some(gate) = gate {
                let _permit = gate.acquire().await.expect("gate is never closed");
            }
            match self.responses.lock().expect("fake fetcher").get(url) {
                Some(Ok(bytes)) if bytes.len() > max => Err(FetchError::TooLarge { max }),
                Some(Ok(bytes)) => Ok(bytes.clone()),
                Some(Err(why)) => Err(FetchError::Http(why.clone())),
                None => Err(FetchError::Status(404)),
            }
        }
    }
}
