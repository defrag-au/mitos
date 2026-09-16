//! The collection catalogue — what collections exist on Cardano, what they
//! are called, and how big they are.
//!
//! Built daily on cardano-infra by `policy-ledger catalogue build`, from the
//! policy index and the Mithril chunks it points at; published to R2 + KV;
//! fetched once at startup by a command palette that then ranks locally per
//! keystroke.
//!
//! # Encoding contract
//!
//! Postcard is positional — there are no field names or tags on the wire, so
//! the struct definitions here ARE the format:
//!
//! - never reorder, remove, or insert fields; never change a field's type or
//!   `Option`-ness;
//! - no `#[serde(skip_serializing_if)]` / `default` — every field is always
//!   present;
//! - enum variants are append-only (discriminant = declaration order);
//! - entries live inside `Vec<Entry>`, so even *appending* a field to
//!   [`Entry`] is a breaking change.
//!
//! Any change bumps [`CATALOGUE_VERSION`]. `version` is the first field of
//! [`Catalogue`] and a plain `u8`, so it encodes as byte 0 of every payload —
//! a reader peeks it before decoding the rest and fails loudly instead of
//! decoding garbage.
//!
//! # Extension slots — additive change WITHOUT a version bump
//!
//! Postcard has no reserved bytes and no schema evolution: an appended typed
//! field misaligns every older reader. What a positional format CAN carry
//! forward is a length-prefixed opaque field — an older reader decodes the
//! length and the bytes and simply never looks inside. So [`Entry::ext`] and
//! [`Catalogue::ext`] are `Vec<Extension>`: empty today (one varint-zero byte
//! each), and the place every ADDITIVE feature lands from v2 on — provenance
//! first (cnft.dev-workers `docs/design/COLLECTION_PROVENANCE.md` §8.3a).
//!
//! - an extension `tag` is assigned once and never reused;
//! - its `body` is itself a postcard struct, frozen per tag — a new shape is a
//!   NEW tag, never an edit;
//! - a reader IGNORES tags it does not know; that is the whole point;
//! - ⚠️ enum VARIANTS are not covered: an unknown postcard discriminant fails
//!   the decode outright. A new variant is still a version bump, which is why
//!   [`NameOrigin::RegistryDeclared`] was reserved in the v2 bump rather than
//!   when it is first written.
//!
//! Version history: **1** (2026-09-15, first publish) · **2** (2026-09-15, the
//! two extension slots and `NameOrigin::RegistryDeclared`).
//!
//! # ⚠️ Why postcard and not an interned layout
//!
//! MEASURED 2026-09-15 over 27,207 named entries: postcard 1,652.6 KiB, an
//! interned string table + front-coded names + fixed stride **1,662.8 KiB**.
//! Front-coding saves 198.6 KiB and dedup 17.3%, but the indirection costs
//! 394.8 KiB of offsets and refs to recover ~200 KiB of text.
//!
//! 🔑 **The repetition ratio decides it.** `hodlcroft/viewer` interns because
//! one 10K-token collection repeats a ~200-value trait vocabulary thousands of
//! times. Here 42,668 of 51,592 name slots are UNIQUE (85%). There is nothing
//! to intern. The technique is right for its problem and wrong for this one.
//!
//! The floor is entropy, not encoding: policy ids are 45% of the payload and
//! are blake2b-224 hashes — 743.9 KiB that gzip *grows*. Truncating them to 8
//! bytes would save 531 KiB and was REJECTED, because every worker route is
//! keyed by the full 56-hex id and the saving buys a resolution round trip at
//! the exact moment the reader presses Enter.

use serde::{Deserialize, Serialize};

/// Bump on ANY field change. Positional, append-only-is-still-breaking — the
/// same contract as `market-ledger-wire`.
pub const CATALOGUE_VERSION: u8 = 2;

/// A policy at or above this many distinct assets is flagged
/// [`EntryFlags::HAZARD`].
///
/// ⚠️ **Advisory metadata, NOT an exclusion.** MEASURED: 206 policies (0.09%),
/// led by `HOSKY C(ash grab)NFT` (420,420) and `ADA Handle` (318,136). They
/// are real collections and people search for them, so they are in the
/// catalogue and in the palette. What refuses them is INGESTION —
/// `collection-ownership` will not onboard one — and those are different
/// systems answering different questions. The flag exists so a consumer can
/// explain itself ("too large to index yet") rather than silently return
/// nothing.
///
/// The limit is expected to MOVE. When these become ingestible the flag's
/// meaning narrows without the artifact changing shape.
pub const HAZARD_ASSETS: u32 = 10_000;

/// Where a name CAME FROM, not just how good we think it is.
///
/// ⚠️ A bare `Vec<String>` would make consumers read the ORDERING as an
/// implicit ranking, which then cannot change without breaking them, and
/// leaves nowhere for a curated override to say it outranks a derivation. One
/// byte now; a format break later.
///
/// Discriminants are postcard varint tags in declaration order — append-only,
/// never reorder (pinned by a test below).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum NameOrigin {
    /// A `collection` / `Collection` / `project` key in CIP-25. MEASURED the
    /// best signal: 61.3% exact where present, on 32% of policies.
    Declared = 0,
    /// Longest common prefix of CIP-25 `name` values. 21.6% exact.
    DerivedFromMetadata = 1,
    /// The same over on-chain asset names.
    DerivedFromAssetNames = 2,
    /// jpg.store's curated `display_name`. Frozen April 2026 — still the ONLY
    /// name for 2,551 collections, including ADA Handle, whose handles share
    /// no prefix to derive from.
    CuratedJpg = 3,
    /// Reserved for the PHASE 2 admin override. **Never written by the
    /// builder** — the artifact is rebuilt whole from chain each refresh and
    /// would overwrite it. Applied by a read-time overlay keyed by policy,
    /// with the catalogue supplying the fallback.
    CuratedLocal = 4,
    /// RESERVED (v2) for a name in a creator-bound on-chain provenance
    /// declaration. **Never written by the builder yet.** Reserved ahead of
    /// use because a postcard enum cannot gain a variant without a version
    /// bump — the extension slots do not cover variants.
    RegistryDeclared = 5,
}

impl NameOrigin {
    /// All origins, in wire-discriminant order.
    pub const ALL: [NameOrigin; 6] = [
        NameOrigin::Declared,
        NameOrigin::DerivedFromMetadata,
        NameOrigin::DerivedFromAssetNames,
        NameOrigin::CuratedJpg,
        NameOrigin::CuratedLocal,
        NameOrigin::RegistryDeclared,
    ];

    /// How much this origin is trusted, LOWEST IS BEST — the order
    /// [`Entry::names`] is sorted into.
    ///
    /// ⚠️ **Deliberately not the wire discriminant.** The discriminant is
    /// append-only and frozen; trust is a judgement that moves as sources are
    /// re-scored. A consumer that wants "the best name" reads `names[0]`; a
    /// consumer that cares WHICH source said it reads `origin`. Conflating the
    /// two is exactly what this enum exists to prevent.
    ///
    /// `CuratedLocal` outranks everything because a human typed it.
    /// `CuratedJpg` sits above the prefix derivations and below `Declared`:
    /// MEASURED, a declared `collection` field is 61.3% exact against jpg's
    /// own names, while 62% of jpg's names are placeholders (`Unnamed`,
    /// `None`) that never reach the artifact at all.
    ///
    /// `RegistryDeclared` sits directly below `CuratedLocal`: like `Declared`
    /// it is authored under the policy's keys, but it is an explicit
    /// collection name that amendment can correct, where a CIP-25 field is
    /// frozen in its mint.
    pub fn rank(&self) -> u8 {
        match self {
            NameOrigin::CuratedLocal => 0,
            NameOrigin::RegistryDeclared => 1,
            NameOrigin::Declared => 2,
            NameOrigin::CuratedJpg => 3,
            NameOrigin::DerivedFromMetadata => 4,
            NameOrigin::DerivedFromAssetNames => 5,
        }
    }

    /// Was this asserted by a person, rather than derived from chain?
    ///
    /// The curation view needs the distinction: a derived name is a claim
    /// ABOUT chain that a rebuild may revise, a curated one is a claim we
    /// made and must reconcile against.
    ///
    /// `RegistryDeclared` is NOT curated: it is the creator's on-chain claim,
    /// not one we made.
    pub fn is_curated(&self) -> bool {
        matches!(self, NameOrigin::CuratedJpg | NameOrigin::CuratedLocal)
    }
}

/// One name for a collection, and where it came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Alias {
    pub name: String,
    pub origin: NameOrigin,
}

impl Alias {
    pub fn new(name: impl Into<String>, origin: NameOrigin) -> Self {
        Alias {
            name: name.into(),
            origin,
        }
    }
}

/// Advisory facts about an entry, as a bitfield.
///
/// A `u8` newtype rather than a `bitflags` dependency: five bits over a wasm
/// wire format is not worth a crate, and postcard encodes it as one byte.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntryFlags(pub u8);

impl EntryFlags {
    /// ≥ [`HAZARD_ASSETS`] distinct assets. ⚠️ Advisory — see that constant.
    pub const HAZARD: EntryFlags = EntryFlags(1 << 0);
    /// Already onboarded in `ownership_policies` — collection-ownership has
    /// live holder data for it.
    pub const TRACKED: EntryFlags = EntryFlags(1 << 1);
    /// Mints CIP-68 labelled assets (a (100) reference or (222) NFT label).
    pub const CIP68: EntryFlags = EntryFlags(1 << 2);
    /// jpg.store lists it with a real curated name.
    pub const JPG_VERIFIED: EntryFlags = EntryFlags(1 << 3);

    pub fn contains(self, other: EntryFlags) -> bool {
        self.0 & other.0 == other.0
    }

    pub fn insert(&mut self, other: EntryFlags) {
        self.0 |= other.0;
    }

    /// Set or clear, from a condition — `flags.set(HAZARD, assets >= N)`.
    pub fn set(&mut self, other: EntryFlags, on: bool) {
        match on {
            true => self.0 |= other.0,
            false => self.0 &= !other.0,
        }
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }
}

/// One forward-compatible extension: an opaque, length-prefixed body that a
/// reader predating its `tag` decodes past without looking inside.
///
/// ⚠️ **`tag` is a `u8`, deliberately not an enum.** A postcard enum with an
/// unknown discriminant fails the WHOLE decode — the exact failure this type
/// exists to avoid. Known tags are the constants in [`entry_ext`] and
/// [`catalogue_ext`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Extension {
    pub tag: u8,
    /// A postcard-encoded struct whose shape is frozen for this tag.
    pub body: Vec<u8>,
}

/// Extension tags on [`Entry::ext`]. Assigned once, never reused.
pub mod entry_ext {
    /// RESERVED — a collection's provenance summary (cnft.dev-workers
    /// `COLLECTION_PROVENANCE.md` §8.3). Not written by any builder yet.
    pub const PROVENANCE: u8 = 1;
}

/// Extension tags on [`Catalogue::ext`]. Assigned once, never reused.
pub mod catalogue_ext {
    /// RESERVED — the interned attester table provenance summaries index
    /// into. Not written by any builder yet.
    pub const ATTESTERS: u8 = 1;
}

fn find_extension(ext: &[Extension], tag: u8) -> Option<&[u8]> {
    ext.iter().find(|x| x.tag == tag).map(|x| x.body.as_slice())
}

/// One collection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// Raw 28 bytes, NOT 56-hex. Halving this is the one size win available
    /// and it costs nothing.
    pub policy: [u8; 28],
    /// Distinct assets, from the index's `PolicyRun` — the size guard and the
    /// onboarding progress DENOMINATOR in one field.
    ///
    /// 🔑 Known WITHOUT reading a chunk or decoding any metadata, which is
    /// what makes the hazard guard free.
    pub assets: u32,
    /// Every known alias, deduped case-insensitively, best first.
    ///
    /// ⚠️ **Best first is a convenience, not the contract.** Read
    /// [`Alias::origin`] if you care which source said it; the ordering is
    /// free to change as sources are re-scored.
    ///
    /// May be EMPTY: a classified collection whose name could not be derived
    /// is still a collection that exists, and dropping it would make the
    /// catalogue lie about the corpus in exactly the way the hazard tier is
    /// not allowed to.
    pub names: Vec<Alias>,
    pub flags: EntryFlags,
    /// Additive per-entry data, by tag (see module docs). Empty today.
    pub ext: Vec<Extension>,
}

impl Entry {
    /// The 56-hex policy id every worker route is keyed by.
    pub fn policy_hex(&self) -> String {
        let mut s = String::with_capacity(56);
        for b in self.policy {
            s.push(char::from_digit((b >> 4) as u32, 16).expect("nibble"));
            s.push(char::from_digit((b & 0xf) as u32, 16).expect("nibble"));
        }
        s
    }

    /// The name to show, or `None` for an entry chain could not name.
    pub fn best_name(&self) -> Option<&str> {
        self.names.first().map(|a| a.name.as_str())
    }

    pub fn is_hazard(&self) -> bool {
        self.flags.contains(EntryFlags::HAZARD)
    }

    /// The body of this entry's extension `tag`, if it carries one.
    pub fn extension(&self, tag: u8) -> Option<&[u8]> {
        find_extension(&self.ext, tag)
    }
}

/// The published artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Catalogue {
    /// MUST stay the first field — encodes as byte 0 (see module docs).
    pub version: u8,
    /// Chunk the index covered when this was built — the freshness stamp.
    pub built_through_chunk: u16,
    /// Unix seconds at build.
    pub built_at: u64,
    /// Sorted by `policy` ascending, so two builds of the same corpus produce
    /// the same bytes. ⚠️ Load-bearing: change detection is a hash of these.
    pub entries: Vec<Entry>,
    /// Additive top-level data — tables entries index into — by tag. Empty
    /// today. ⚠️ Part of [`Self::content_bytes`]: it sits outside `entries`.
    pub ext: Vec<Extension>,
}

impl Catalogue {
    pub fn empty() -> Self {
        Catalogue {
            version: CATALOGUE_VERSION,
            built_through_chunk: 0,
            built_at: 0,
            entries: Vec::new(),
            ext: Vec::new(),
        }
    }

    /// Entries carrying at least one name — what a search box can reach.
    pub fn named(&self) -> usize {
        self.entries.iter().filter(|e| !e.names.is_empty()).count()
    }

    /// The body of the top-level extension `tag`, if present.
    pub fn extension(&self, tag: u8) -> Option<&[u8]> {
        find_extension(&self.ext, tag)
    }

    /// The bytes that DEFINE this catalogue's content, for change detection.
    ///
    /// # ⚠️ Why this is not just the encoded artifact
    ///
    /// The doc's rule is *hash the ARTIFACT, not the inputs* — `base.pidx`
    /// changes every day because chunks were appended, and a day with no new
    /// collections must publish nothing.
    ///
    /// But [`Self::built_through_chunk`] and [`Self::built_at`] move on every
    /// single build BY CONSTRUCTION — they are the freshness stamp. Hashing
    /// the whole encoded artifact would therefore make every day look changed
    /// and defeat the rule it was meant to implement. So the content hash
    /// covers exactly what the catalogue SAYS — the version and the entries —
    /// and excludes when it was said.
    ///
    /// The freshness a skipped publish would otherwise lose is carried by the
    /// pointer object, which is flipped on every run whether or not the blob
    /// changed.
    ///
    /// ⚠️ **Every content field joins, including [`Self::ext`].** A top-level
    /// table left out here would change the blob and never publish.
    pub fn content_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_allocvec(&Content {
            version: self.version,
            entries: &self.entries,
            ext: &self.ext,
        })
    }
}

/// The view [`Catalogue::content_bytes`] hashes. Private: it is an internal
/// canonicalisation, never something on the wire.
#[derive(Serialize)]
struct Content<'a> {
    version: u8,
    entries: &'a [Entry],
    ext: &'a [Extension],
}

/// Decode-side errors.
#[derive(Debug, PartialEq, Eq)]
pub enum CatalogueError {
    /// Payload is empty — not even a version byte.
    Empty,
    /// Byte 0 didn't match [`CATALOGUE_VERSION`]. A version-locked consumer
    /// should surface this as "update me".
    VersionMismatch { got: u8 },
    /// Postcard decode failure after the version check.
    Decode(postcard::Error),
    /// The stored object looked gzipped and would not inflate.
    Inflate(String),
}

impl core::fmt::Display for CatalogueError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            CatalogueError::Empty => write!(f, "empty payload"),
            CatalogueError::VersionMismatch { got } => write!(
                f,
                "catalogue version mismatch: got {got}, expected {CATALOGUE_VERSION}"
            ),
            CatalogueError::Decode(e) => write!(f, "postcard decode failed: {e}"),
            CatalogueError::Inflate(e) => write!(f, "gzip inflate failed: {e}"),
        }
    }
}

impl std::error::Error for CatalogueError {}

/// Encode the catalogue to its published bytes.
pub fn encode(catalogue: &Catalogue) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(catalogue)
}

/// Decode published bytes, checking the version byte first.
pub fn decode(bytes: &[u8]) -> Result<Catalogue, CatalogueError> {
    match bytes.first() {
        None => Err(CatalogueError::Empty),
        Some(&v) if v != CATALOGUE_VERSION => Err(CatalogueError::VersionMismatch { got: v }),
        Some(_) => postcard::from_bytes(bytes).map_err(CatalogueError::Decode),
    }
}

/// The first two bytes of a gzip member (RFC 1952 §2.3.1).
pub const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

/// Decode the catalogue AS STORED — gzipped or not — checking the version.
///
/// Sniffed rather than told, and unambiguously: byte 0 of an uncompressed
/// catalogue is [`CATALOGUE_VERSION`], which is never `0x1f` (a test pins it).
/// So a reader needs no side channel for the encoding — which is what lets a
/// Worker hand the stored bytes over untouched. workers-rs 0.7.4 exposes no
/// `encodeBody: "manual"`, so declaring `Content-Encoding: gzip` on a Worker
/// response is not something to rely on; an opaque body the browser inflates
/// is.
#[cfg(feature = "gzip")]
pub fn decode_stored(bytes: &[u8]) -> Result<Catalogue, CatalogueError> {
    if !bytes.starts_with(&GZIP_MAGIC) {
        return decode(bytes);
    }
    use std::io::Read as _;
    let mut raw = Vec::new();
    flate2::read::GzDecoder::new(bytes)
        .read_to_end(&mut raw)
        .map_err(|e| CatalogueError::Inflate(e.to_string()))?;
    decode(&raw)
}

/// Prefix shared by the KV key and the bucket keys.
pub const KV_PREFIX: &str = "collection-catalogue";

/// The KV key the catalogue is stored under: `collection-catalogue:<network>`.
///
/// Part of the contract, not the publisher's private business — a Worker reads
/// the key the publisher writes, so the spelling lives beside the format.
pub fn kv_key(network: &str) -> String {
    format!("{KV_PREFIX}:{network}")
}

/// What `latest.json` says, and the metadata on the KV entry.
///
/// ⚠️ A typed struct, never `serde_json::json!` — this is an API contract with
/// a Worker and a frontend, which is why it lives here and not in the
/// publisher. JSON, not postcard: KV metadata is JSON, and a reader must be
/// able to refuse on `version` before spending a 1.4 MiB read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pointer {
    /// [`CATALOGUE_VERSION`] of the blob this names, so a reader can refuse
    /// before spending a 1.7 MiB GET.
    pub version: u8,
    /// Full key of the blob in the bucket.
    pub key: String,
    /// blake2b-256 of the catalogue's CONTENT bytes, hex. The build identity.
    pub content_hash: String,
    /// Size of the STORED object — gzipped when that measurably paid.
    pub bytes: u64,
    /// `Some("gzip")` when the stored object is compressed. An ABSENT
    /// encoding is the identity encoding.
    pub content_encoding: Option<String>,
    pub entries: u32,
    pub named: u32,
    pub built_through_chunk: u16,
    pub built_at: u64,
}

#[cfg(all(test, feature = "gzip"))]
mod stored_tests {
    use super::*;
    use std::io::Write as _;

    fn tiny() -> Catalogue {
        Catalogue {
            version: CATALOGUE_VERSION,
            built_through_chunk: 1,
            built_at: 2,
            entries: vec![Entry {
                policy: [7; 28],
                assets: 12,
                names: vec![Alias::new("Tiny", NameOrigin::Declared)],
                flags: EntryFlags::default(),
                ext: Vec::new(),
            }],
            ext: Vec::new(),
        }
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(bytes).expect("in-memory write");
        enc.finish().expect("in-memory finish")
    }

    #[test]
    fn stored_bytes_decode_whether_gzipped_or_not() {
        let c = tiny();
        let raw = encode(&c).expect("encode");
        assert_eq!(decode_stored(&raw).expect("plain"), c);
        assert_eq!(decode_stored(&gzip(&raw)).expect("gzipped"), c);
    }

    /// The sniff is only sound while no version can look like gzip.
    #[test]
    fn a_version_byte_is_never_mistaken_for_gzip() {
        assert_ne!(CATALOGUE_VERSION, GZIP_MAGIC[0]);
    }

    #[test]
    fn a_truncated_gzip_member_is_an_inflate_error_not_garbage() {
        let g = gzip(&encode(&tiny()).expect("encode"));
        assert!(matches!(
            decode_stored(&g[..g.len() / 2]),
            Err(CatalogueError::Inflate(_))
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(policy: u8, assets: u32, names: Vec<Alias>, flags: EntryFlags) -> Entry {
        Entry {
            policy: [policy; 28],
            assets,
            names,
            flags,
            ext: Vec::new(),
        }
    }

    fn sample() -> Catalogue {
        Catalogue {
            version: CATALOGUE_VERSION,
            built_through_chunk: 9153,
            built_at: 1_757_900_000,
            entries: vec![
                entry(
                    0x1f,
                    10_000,
                    vec![
                        Alias::new("Clay Nation", NameOrigin::Declared),
                        Alias::new("Clay Nation by Clay Mates", NameOrigin::CuratedJpg),
                    ],
                    EntryFlags(EntryFlags::HAZARD.0 | EntryFlags::JPG_VERIFIED.0),
                ),
                // A classified collection chain could not name.
                entry(0x2a, 331, Vec::new(), EntryFlags::default()),
            ],
            ext: Vec::new(),
        }
    }

    #[test]
    fn round_trip() {
        let c = sample();
        let bytes = encode(&c).unwrap();
        assert_eq!(bytes[0], CATALOGUE_VERSION);
        assert_eq!(decode(&bytes).unwrap(), c);
    }

    #[test]
    fn version_mismatch_is_loud() {
        // The v1 blobs published before the extension slots are refused, loudly.
        let mut bytes = encode(&Catalogue::empty()).unwrap();
        bytes[0] = 1;
        assert_eq!(
            decode(&bytes),
            Err(CatalogueError::VersionMismatch { got: 1 })
        );
        assert_eq!(decode(&[]), Err(CatalogueError::Empty));
    }

    /// 🔑 **The whole change-detection story.** Two builds of the same corpus
    /// on different days differ in their freshness stamp and in nothing else,
    /// and must hash the same — otherwise every daily refresh republishes
    /// ~1.7 MiB to say nothing new.
    #[test]
    fn a_later_build_of_the_same_corpus_has_the_same_content() {
        let today = sample();
        let mut tomorrow = today.clone();
        tomorrow.built_through_chunk += 7;
        tomorrow.built_at += 86_400;

        assert_ne!(
            encode(&today).unwrap(),
            encode(&tomorrow).unwrap(),
            "the artifacts differ — the stamp moved"
        );
        assert_eq!(
            today.content_bytes().unwrap(),
            tomorrow.content_bytes().unwrap(),
            "but the catalogue SAYS the same thing, so nothing should publish"
        );
    }

    /// And the hash has to be able to notice a real change, or it is just a
    /// constant that suppresses every publish.
    #[test]
    fn a_new_collection_changes_the_content() {
        let before = sample();
        let mut after = before.clone();
        after.entries.push(entry(
            0x33,
            5,
            vec![Alias::new("Skullys", NameOrigin::DerivedFromMetadata)],
            EntryFlags::default(),
        ));
        assert_ne!(
            before.content_bytes().unwrap(),
            after.content_bytes().unwrap()
        );

        // So does a name, a size and a flag — each on its own.
        let mut renamed = before.clone();
        renamed.entries[0].names[0].name = "Clay Nation ".into();
        assert_ne!(
            before.content_bytes().unwrap(),
            renamed.content_bytes().unwrap()
        );
        let mut grown = before.clone();
        grown.entries[1].assets += 1;
        assert_ne!(
            before.content_bytes().unwrap(),
            grown.content_bytes().unwrap()
        );
        let mut flagged = before.clone();
        flagged.entries[1].flags.insert(EntryFlags::TRACKED);
        assert_ne!(
            before.content_bytes().unwrap(),
            flagged.content_bytes().unwrap()
        );
    }

    /// ⚠️ A name's ORIGIN is part of the content. A collection that jpg named
    /// and chain later learns to derive is a real change to what we assert,
    /// even when the text is identical.
    #[test]
    fn an_origin_change_alone_changes_the_content() {
        let before = sample();
        let mut after = before.clone();
        after.entries[0].names[0].origin = NameOrigin::DerivedFromMetadata;
        assert_ne!(
            before.content_bytes().unwrap(),
            after.content_bytes().unwrap()
        );
    }

    /// Freezes the wire contract: each origin's postcard tag is its
    /// declaration position. If this fails, the format broke.
    #[test]
    fn name_origin_discriminants_pinned() {
        for (expected, origin) in NameOrigin::ALL.iter().enumerate() {
            let bytes = postcard::to_allocvec(origin).unwrap();
            assert_eq!(
                bytes,
                vec![expected as u8],
                "discriminant drift for {origin:?}"
            );
        }
    }

    /// ⚠️ Trust order is NOT the wire order, and the two must be free to
    /// disagree — that is the point of carrying an origin at all.
    #[test]
    fn trust_rank_is_independent_of_the_wire_discriminant() {
        let mut by_rank = NameOrigin::ALL;
        by_rank.sort_by_key(|o| o.rank());
        assert_eq!(
            by_rank,
            [
                NameOrigin::CuratedLocal,
                NameOrigin::RegistryDeclared,
                NameOrigin::Declared,
                NameOrigin::CuratedJpg,
                NameOrigin::DerivedFromMetadata,
                NameOrigin::DerivedFromAssetNames,
            ]
        );
        assert_ne!(
            by_rank,
            NameOrigin::ALL,
            "if these ever coincide the test stops proving anything"
        );
        // Every rank is distinct, so "best first" is a total order.
        let mut ranks: Vec<u8> = NameOrigin::ALL.iter().map(|o| o.rank()).collect();
        ranks.sort_unstable();
        ranks.dedup();
        assert_eq!(ranks.len(), NameOrigin::ALL.len());
    }

    #[test]
    fn flags_are_independent_bits() {
        let mut f = EntryFlags::default();
        assert!(f.is_empty());
        f.insert(EntryFlags::HAZARD);
        f.insert(EntryFlags::CIP68);
        assert!(f.contains(EntryFlags::HAZARD) && f.contains(EntryFlags::CIP68));
        assert!(!f.contains(EntryFlags::TRACKED) && !f.contains(EntryFlags::JPG_VERIFIED));
        f.set(EntryFlags::HAZARD, false);
        assert!(!f.contains(EntryFlags::HAZARD) && f.contains(EntryFlags::CIP68));
    }

    #[test]
    fn a_policy_reads_back_as_the_hex_a_worker_route_wants() {
        let e = entry(0xab, 1, Vec::new(), EntryFlags::default());
        let hex = e.policy_hex();
        assert_eq!(hex.len(), 56);
        assert_eq!(hex, "ab".repeat(28));
        assert_eq!(e.best_name(), None, "unnamed, and honest about it");
    }

    /// 🔑 **What the v2 bump buys.** An entry or catalogue carrying a tag no
    /// reader knows yet decodes intact, and the tag is simply not found.
    #[test]
    fn an_unknown_extension_is_carried_not_rejected() {
        let mut c = sample();
        c.entries[0].ext.push(Extension {
            tag: 200,
            body: vec![0xde, 0xad, 0xbe, 0xef],
        });
        c.ext.push(Extension {
            tag: 201,
            body: b"a future table".to_vec(),
        });

        let back = decode(&encode(&c).unwrap()).unwrap();
        assert_eq!(back, c);
        assert_eq!(
            back.entries[0].extension(200),
            Some(&[0xde, 0xad, 0xbe, 0xef][..])
        );
        assert_eq!(back.entries[0].extension(entry_ext::PROVENANCE), None);
        assert_eq!(
            back.entries[1].extension(200),
            None,
            "extensions are per entry"
        );
        assert_eq!(back.extension(201), Some(&b"a future table"[..]));
    }

    /// The price of the slots, pinned against the v1 layout: ONE byte per
    /// entry plus one for the catalogue, when empty.
    #[test]
    fn empty_extension_slots_cost_one_byte_each() {
        #[derive(Serialize)]
        struct EntryV1<'a> {
            policy: [u8; 28],
            assets: u32,
            names: &'a [Alias],
            flags: EntryFlags,
        }
        #[derive(Serialize)]
        struct CatalogueV1<'a> {
            version: u8,
            built_through_chunk: u16,
            built_at: u64,
            entries: Vec<EntryV1<'a>>,
        }

        let c = sample();
        let v1 = CatalogueV1 {
            version: 1,
            built_through_chunk: c.built_through_chunk,
            built_at: c.built_at,
            entries: c
                .entries
                .iter()
                .map(|e| EntryV1 {
                    policy: e.policy,
                    assets: e.assets,
                    names: &e.names,
                    flags: e.flags,
                })
                .collect(),
        };
        let v1_len = postcard::to_allocvec(&v1).unwrap().len();
        let v2_len = encode(&c).unwrap().len();
        assert_eq!(v2_len, v1_len + c.entries.len() + 1);
    }

    /// ⚠️ The change-detection trap the slots create: a TOP-LEVEL extension
    /// sits outside `entries`, so unless `Content` names it, a table-only
    /// change alters the blob and never publishes.
    #[test]
    fn extensions_change_the_content() {
        let before = sample();

        let mut top = before.clone();
        top.ext.push(Extension {
            tag: catalogue_ext::ATTESTERS,
            body: vec![0],
        });
        assert_ne!(
            before.content_bytes().unwrap(),
            top.content_bytes().unwrap()
        );

        let mut per_entry = before.clone();
        per_entry.entries[1].ext.push(Extension {
            tag: entry_ext::PROVENANCE,
            body: vec![0],
        });
        assert_ne!(
            before.content_bytes().unwrap(),
            per_entry.content_bytes().unwrap()
        );
    }
}
