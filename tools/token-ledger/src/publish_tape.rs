//! Publishing a pack — the files a replay reads — to R2, for the arena.
//!
//! # Layout
//!
//! Under `token-tape/<policy>/`:
//!
//! - `pages/<first_day>-<digest>.bin`: every page, named by its own bytes
//!   (`token_tape::page_digest`), shared by every build that has it. A key
//!   never names different bytes, so a page is uploaded once and cached for
//!   a year, and a republish sends only the pages that changed: the open
//!   page, mostly, as the closed ones are byte-identical from pack to pack.
//! - `<build>/pages.bin` and `<build>/meta.bin`: the book. The index names
//!   each page's digest, so it says which bytes this build reads, and a
//!   reader can never put a page of one pack against the meta of another.
//!   A build is named by the book's bytes.
//! - `current`: the build a reader should open, `no-cache`.
//!
//! # Order
//!
//! 1. Every page the index names that R2 does not hold yet.
//! 2. The book, under the build.
//! 3. The pointer, once everything it names exists.
//! 4. Prune what neither this build nor the one the pointer named before
//!    reads: a reader that read the old pointer a moment ago can still finish
//!    with it.
//!
//! Only what a replay fetches goes up. The holder tables and the per-day
//! chunks stay on the box.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use cf_publish::{Client, Targets, immutable_attributes, pointer_attributes};
use object_store::path::Path as ObjPath;

/// Key prefix in the bucket.
pub const PREFIX: &str = "token-tape";
/// The pointer's name under a policy's prefix.
pub const POINTER: &str = "current";
/// The book's files, under a build.
const BOOK: [&str; 2] = [token_tape::PAGES_INDEX_PATH, "meta.bin"];

#[derive(clap::Args, Debug)]
pub struct PublishTapeArgs {
    /// A pack's output root (`<out>/<policy_hex>/pages.bin`), as `pack
    /// --out` was given it.
    #[arg(long)]
    pub out: PathBuf,
    /// 56-hex policy id.
    #[arg(long)]
    pub policy: String,
}

/// What a publish did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Published {
    pub build: String,
    /// The build the pointer named before, if any.
    pub before: Option<String>,
    pub pages: usize,
    pub pages_sent: usize,
    pub bytes_sent: usize,
    pub pruned: usize,
}

impl Published {
    pub fn summary(&self) -> String {
        let flip = match self.before.as_deref() {
            Some(b) if b == self.build => "unchanged".to_string(),
            Some(b) => format!("{b} -> {}", self.build),
            None => format!("(none) -> {}", self.build),
        };
        format!(
            "{flip}: {} of {} pages sent, {} bytes; {} stale objects pruned",
            self.pages_sent, self.pages, self.bytes_sent, self.pruned
        )
    }
}

/// The book's bytes and the pages its index names, read from a pack.
struct Pack {
    book: [Vec<u8>; 2],
    pages: Vec<String>,
}

fn read_pack(dir: &Path) -> Result<Pack> {
    let read = |rel: &str| {
        std::fs::read(dir.join(rel)).with_context(|| format!("reading {}", dir.join(rel).display()))
    };
    let book = [read(BOOK[0])?, read(BOOK[1])?];
    let index =
        token_tape::Index::decode(&book[0]).map_err(|e| anyhow::anyhow!("{}: {e}", BOOK[0]))?;
    if index.pages.is_empty() {
        bail!("{} names no pages: pack it first", dir.display());
    }
    let pages = index.pages.iter().map(token_tape::PageInfo::path).collect();
    Ok(Pack { book, pages })
}

/// FNV-1a over the book: a build's name. Not a security property, only
/// "these bytes"; the index inside names every page's digest, so the book's
/// bytes are the build's.
fn build_name(book: &[Vec<u8>; 2]) -> String {
    let h = book
        .iter()
        .flatten()
        .fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
            (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3)
        });
    format!("v{}-{h:016x}", token_tape::FORMAT)
}

pub fn run(args: PublishTapeArgs) -> Result<()> {
    let policy = args.policy.to_lowercase();
    let targets = Targets::from_env();
    println!("targets: {}", targets.describe());
    let Some(client) = Client::new(targets)? else {
        bail!(
            "R2 is not configured (R2_ENDPOINT / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_BUCKET)"
        );
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let started = std::time::Instant::now();
    let done = runtime.block_on(publish(&client, &args.out.join(&policy), &policy))?;
    println!(
        "{}; in {:.1}s",
        done.summary(),
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

/// Publish the pack at `dir` (`<out>/<policy>`): see the module.
pub async fn publish(client: &Client, dir: &Path, policy: &str) -> Result<Published> {
    let pack = read_pack(dir)?;
    let build = build_name(&pack.book);
    let root = format!("{PREFIX}/{policy}");
    let pointer = ObjPath::from(format!("{root}/{POINTER}"));
    let before = match client.store().get(&pointer).await {
        Ok(r) => Some(
            String::from_utf8_lossy(&r.bytes().await?)
                .trim()
                .to_string(),
        ),
        Err(object_store::Error::NotFound { .. }) => None,
        Err(e) => return Err(e).context("reading the pointer"),
    };
    // 1. The pages R2 does not hold yet.
    let there = client
        .sizes_under(&ObjPath::from(format!("{root}/pages")))
        .await?;
    let (mut pages_sent, mut bytes_sent) = (0usize, 0usize);
    for page in &pack.pages {
        let key = ObjPath::from(format!("{root}/{page}"));
        if there.contains_key(&key) {
            continue;
        }
        let body = std::fs::read(dir.join(page)).with_context(|| format!("reading {page}"))?;
        bytes_sent += body.len();
        client
            .put(&key, body, immutable_attributes())
            .await
            .with_context(|| format!("uploading {page}"))?;
        pages_sent += 1;
    }
    // 2. The book.
    for (rel, body) in BOOK.iter().zip(&pack.book) {
        let key = ObjPath::from(format!("{root}/{build}/{rel}"));
        bytes_sent += body.len();
        client
            .put(&key, body.clone(), immutable_attributes())
            .await
            .with_context(|| format!("uploading {rel}"))?;
    }
    // 3. The pointer, once every file it names is there.
    if before.as_deref() != Some(build.as_str()) {
        client
            .put(
                &pointer,
                build.as_bytes().to_vec(),
                pointer_attributes("text/plain"),
            )
            .await
            .context("flipping the pointer")?;
    }
    // 4. What neither this build nor the one before reads.
    let mut keep: BTreeSet<ObjPath> = BTreeSet::from([pointer]);
    for b in std::iter::once(build.as_str()).chain(before.as_deref()) {
        for rel in BOOK {
            keep.insert(ObjPath::from(format!("{root}/{b}/{rel}")));
        }
    }
    keep.extend(
        pack.pages
            .iter()
            .map(|p| ObjPath::from(format!("{root}/{p}"))),
    );
    // The pages the previous build reads, from its own index. A previous
    // build this reader cannot decode (an older format) keeps its book, and
    // its pages go: nothing current reads them.
    if let Some(b) = before.as_deref().filter(|b| *b != build) {
        let key = ObjPath::from(format!("{root}/{b}/{}", BOOK[0]));
        if let Ok(r) = client.store().get(&key).await
            && let Ok(bytes) = r.bytes().await
            && let Ok(index) = token_tape::Index::decode(&bytes)
        {
            keep.extend(
                index
                    .pages
                    .iter()
                    .map(|p| ObjPath::from(format!("{root}/{}", p.path()))),
            );
        }
    }
    let pruned = client.prune(&ObjPath::from(root), &keep).await?;
    Ok(Published {
        build,
        before,
        pages: pack.pages.len(),
        pages_sent,
        bytes_sent,
        pruned,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A build is named by its book, and the book names the pages: the same
    /// pack the same build, a changed page (so a changed index) another.
    #[test]
    fn a_build_is_named_by_its_book() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let page = |digest| token_tape::PageInfo {
            first_day: 20_000,
            last_day: 20_001,
            events: 1,
            bytes: 4,
            digest,
        };
        let index = |digest| {
            token_tape::Index {
                parties: 1,
                chunks: Vec::new(),
                pages: vec![page(digest)],
            }
            .encode()
        };
        std::fs::write(dir.join("pages.bin"), index(7)).unwrap();
        std::fs::write(dir.join("meta.bin"), b"meta").unwrap();
        let pack = read_pack(dir).unwrap();
        assert_eq!(pack.pages, [page(7).path()]);
        let a = build_name(&pack.book);
        assert_eq!(a, build_name(&read_pack(dir).unwrap().book));
        assert!(a.starts_with(&format!("v{}-", token_tape::FORMAT)));
        std::fs::write(dir.join("pages.bin"), index(8)).unwrap();
        assert_ne!(a, build_name(&read_pack(dir).unwrap().book));
    }
}
