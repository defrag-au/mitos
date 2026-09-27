//! `publish` — put the base, its directory, its head and its entry shards where a reader can
//! range them.
//!
//! Everything goes to a build-addressed path and is cached forever; a tiny pointer names the
//! current build and is the only object that ever changes. The order is load-bearing — the
//! base and the shards first, so the pointer never names something that is not there.
//!
//! ⚠️ **The entry shards duplicate the base's entry bytes, on purpose.** The base is one
//! 3 GB object, so Cloudflare answers `cf-cache-status: BYPASS` at every offset and every
//! lookup pays an R2 origin fetch. The shards are the same bytes cut into objects the edge
//! will keep, and a browser reader uses those instead. That roughly doubles what a publish
//! uploads; the box's uplink is ~74 MiB/s measured, so it is about a minute.
//!
//! `--run` gates every write, as it does everywhere else here: without it this prints the exact
//! `rclone` invocations and touches nothing. Per
//! `rules/org/defrag/bulk-uploads-run-on-the-arm-box.md`, the run itself belongs on the arm box
//! — the base is GB-scale and rclone's `copyto` restarts an interrupted multipart upload rather
//! than resuming it.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use tx_index::base::{BASE_FILE, BaseFile, base_path};
use tx_index::format::BaseHeader;
use tx_index::hosted::{self, DIR_FILE, HEAD_FILE, POINTER_FILE, Pointer};
use tx_index::locate::{self, FENCE_BYTES};

/// Where the pointer is written locally, so what was published is inspectable afterwards.
const POINTER_LOG: &str = "publish.json";

#[derive(clap::Args, Debug)]
pub struct PublishArgs {
    /// Index directory (`segments/` + `base.idx`).
    #[arg(long)]
    pub index_dir: PathBuf,

    /// Bucket and prefix, e.g. `r2:mithril-snapshots/index/tx/v1`.
    #[arg(long)]
    pub dest: String,

    /// Where the shards are cut to before they are uploaded.
    ///
    /// ⚠️ **Required, and it needs room for the whole entry region** — ~3 GB on
    /// mainnet. `--index-dir` is usually on the root filesystem beside the base, which
    /// does not have it, so this is not defaulted to somewhere plausible and quietly
    /// wrong. The build's own subdirectory is written, uploaded and then removed, so a
    /// successful publish leaves nothing behind.
    #[arg(long)]
    pub shard_dir: PathBuf,

    /// Transfer threads for the base, and the parallelism of the shard upload.
    #[arg(long, default_value_t = 8)]
    pub transfers: usize,

    /// Execute. Without it, print the plan and touch nothing.
    #[arg(long)]
    pub run: bool,
}

pub fn run(a: PublishArgs) -> Result<()> {
    let dest = a.dest.trim_end_matches('/').to_string();
    let base = base_path(&a.index_dir);
    if !base.is_file() {
        bail!(
            "no {BASE_FILE} in {} — run `tx-index build --compact always` first",
            a.index_dir.display()
        );
    }

    let header = BaseFile::open(&base)
        .with_context(|| format!("opening {}", base.display()))?
        .header;
    let bytes = std::fs::metadata(&base)
        .with_context(|| format!("sizing {}", base.display()))?
        .len();
    // The directory ends where the entry array begins, so the header already carries
    // its length — no new field, and the slice cannot disagree with the layout.
    let dir_len = header.entries_off;
    // The head is the base's own first bytes, so it too is derived rather than declared.
    let head_len = hosted::head_bytes(&header);
    let shard_count = hosted::shard_count(&header);
    let shards_len = locate::shards_bytes(header.n_buckets() as u64, header.count);
    let digest = digest_of(&base)?;
    let (remote, bucket, prefix) = split_dest(&dest)?;
    let pointer = Pointer::new(&prefix, &header, &digest, bytes);
    let pointer_json = serde_json::to_string_pretty(&pointer)?;
    let pointer_file = a.index_dir.join(POINTER_LOG);
    // ⚠️ The build's OWN subdirectory, never the staging root directly. A stale shard
    // left by a failed run of a DIFFERENT build would otherwise be uploaded under this
    // build's prefix, where `--ignore-existing` would then keep it forever.
    let staging = a.shard_dir.join(&pointer.build);

    println!("base        {} · {bytes} bytes", base.display());
    println!("sha256      {}", hex::encode(digest));
    println!(
        "covers      chunks {:05}..={:05} · {} entries",
        pointer.first_chunk, pointer.last_chunk, pointer.entries
    );
    println!("build       {}", pointer.build);
    println!("dest        {remote}:{bucket}");
    println!("writes      {}", pointer.base_key);
    println!(
        "            {} · {dir_len} bytes (the directory, whole)",
        hosted::dir_key(&prefix, &pointer.build)
    );
    println!(
        "            {} · {head_len} bytes (the header and the era table)",
        hosted::head_key(&prefix, &pointer.build)
    );
    println!(
        "            {shard_count} entry shards of ~{} KiB · {} bytes",
        locate::shard_bytes(header.count.div_ceil(shard_count) as u32) / 1024,
        shards_len
    );
    println!("            {prefix}/{POINTER_FILE}");
    println!("pointer     {}", serde_json::to_string(&pointer)?);
    println!();

    let base_argv = rclone_base(&dest, &base, &pointer.build, a.transfers);
    let dir_argv = rclone_dir(&dest, &a.index_dir.join(DIR_FILE), &pointer.build);
    let head_argv = rclone_head(&dest, &staging.join(HEAD_FILE), &pointer.build);
    let entries_argv = rclone_entries(&dest, &staging.join("entries"), &pointer.build, a.transfers);
    let pointer_argv = rclone_pointer(&dest, &pointer_file);
    println!("{}", shell_line(&base_argv));
    println!("{}", shell_line(&dir_argv));
    println!("{}", shell_line(&head_argv));
    println!("{}", shell_line(&entries_argv));
    println!("{}", shell_line(&pointer_argv));
    println!();
    println!("note  the base is {bytes} bytes, past rclone's 200 MiB upload_cutoff, so it is a");
    println!(
        "      multipart upload: `copyto` RESTARTS one rather than resuming it, and its parts"
    );
    println!(
        "      are billed until it completes or is aborted. To abort one by hand, note that both"
    );
    println!(
        "      `rclone cleanup` and `rclone backend cleanup` SILENTLY IGNORE an upload younger"
    );
    println!("      than 24 h; the working form is");
    println!("        rclone backend cleanup -o max-age=0 <remote>:<bucket>");
    println!("      Run this on the arm box, not the workstation.");
    println!();
    println!(
        "staging     {} · needs room for {shards_len} bytes, removed after a verified upload",
        staging.display()
    );

    if !a.run {
        println!();
        println!("(re-run with --run to execute)");
        return Ok(());
    }

    // The base FIRST, then everything a reader needs to resolve a hash, then the pointer: a
    // reader that finds the pointer must find everything it names.
    split_directory(&base, &a.index_dir.join(DIR_FILE), dir_len)?;
    let cut = cut_shards(&base, &staging, &header)?;
    if cut.count != shard_count || cut.bytes != shards_len {
        bail!(
            "cut {} shards of {} bytes, expected {shard_count} of {shards_len}",
            cut.count,
            cut.bytes
        );
    }
    std::fs::write(&pointer_file, format!("{pointer_json}\n"))
        .with_context(|| format!("writing {}", pointer_file.display()))?;
    for argv in [
        &base_argv,
        &dir_argv,
        &head_argv,
        &entries_argv,
        &pointer_argv,
    ] {
        run_rclone(argv)?;
    }

    // A publish that does not know whether it published is the gap the plan already flags, and
    // the base is the object large enough for a silent partial to be possible. So read back what
    // landed and compare it to what was built.
    let base_dest = format!("{dest}/{}/{BASE_FILE}", pointer.build);
    let dir_dest = format!("{dest}/{}/{DIR_FILE}", pointer.build);
    let head_dest = format!("{dest}/{}/{HEAD_FILE}", pointer.build);
    let entries_dest = format!("{dest}/{}/entries", pointer.build);
    let pointer_dest = format!("{dest}/{POINTER_FILE}");
    let landed = stat_size(&base_dest)?;
    if landed != bytes {
        bail!(
            "{base_dest} is {landed} bytes on the mirror, not {bytes} — the upload did not land whole"
        );
    }
    let landed_dir = stat_size(&dir_dest)?;
    if landed_dir != dir_len {
        bail!(
            "{dir_dest} is {landed_dir} bytes on the mirror, not {dir_len} — a directory that is \
             short reads as a lookup that found nothing"
        );
    }
    let landed_head = stat_size(&head_dest)?;
    if landed_head != head_len {
        bail!("{head_dest} is {landed_head} bytes on the mirror, not {head_len}");
    }
    // ⚠️ The shards are verified by COUNT and TOTAL BYTES rather than one object at a time:
    // there are tens of thousands of them, and a truncated PUT lands as a smaller object. Both
    // halves matter — a count alone would pass a set with one object the wrong size, and a
    // byte total alone would pass one missing and one duplicated.
    let (landed_shards, landed_shard_bytes) = prefix_size(&entries_dest)?;
    if landed_shards != shard_count || landed_shard_bytes != shards_len {
        bail!(
            "{entries_dest} holds {landed_shards} objects of {landed_shard_bytes} bytes, \
             expected {shard_count} of {shards_len}"
        );
    }
    let landed_pointer = stat_size(&pointer_dest)?;
    let want_pointer = std::fs::metadata(&pointer_file)
        .with_context(|| format!("sizing {}", pointer_file.display()))?
        .len();
    if landed_pointer != want_pointer {
        bail!("{pointer_dest} is {landed_pointer} bytes on the mirror, not {want_pointer}");
    }

    // Only now, with every object read back and agreed: the shards are reproducible from the
    // base, so keeping them would just fill the disk the next build needs.
    std::fs::remove_dir_all(&staging)
        .with_context(|| format!("clearing {}", staging.display()))?;

    println!();
    println!("published   {base_dest} · {landed} bytes");
    println!("            {dir_dest} · {landed_dir} bytes");
    println!("            {head_dest} · {landed_head} bytes");
    println!("            {entries_dest}/ · {landed_shards} shards of {landed_shard_bytes} bytes");
    println!("            {pointer_dest} · {landed_pointer} bytes");
    println!();
    println!("readers     fetch `{prefix}/{POINTER_FILE}`, then its `head_key` and shards");
    Ok(())
}

/// What [`cut_shards`] wrote.
#[derive(Debug, PartialEq, Eq)]
struct Cut {
    count: u64,
    bytes: u64,
}

/// Cut the entry region into one object per shard, each self-contained.
///
/// A shard is `[fence table][entries]`, where the table holds the shard's own
/// `SHARD_BUCKETS + 1` boundary values rebased on the shard's first. So a reader addressing a
/// bucket inside a shard gets the pair that bounds it and the run it bounds from ONE object,
/// and never has to warm the 64 MiB directory to find them.
///
/// ⚠️ The fence table is read from the base ONCE, whole, and the entries are read forwards
/// from `entries_off` into the staging directory. Both halves of the base are read exactly
/// once and in order, which is what keeps a 3 GB cut to a couple of minutes on the box.
fn cut_shards(base: &Path, staging: &Path, header: &BaseHeader) -> Result<Cut> {
    let n_buckets = header.n_buckets() as u64;
    let shard_count = locate::shard_count(n_buckets);
    let entries_dir = staging.join("entries");
    std::fs::create_dir_all(&entries_dir)
        .with_context(|| format!("creating {}", entries_dir.display()))?;

    // The whole fence array, 64 MiB at mainnet's 2²⁴ buckets, plus the sentinel that bounds
    // the last bucket.
    let mut fences = vec![0u8; ((n_buckets + 1) * FENCE_BYTES) as usize];
    let mut src = File::open(base).with_context(|| format!("opening {}", base.display()))?;
    src.seek(SeekFrom::Start(header.dir_off))
        .with_context(|| format!("seeking {}, directory", base.display()))?;
    src.read_exact(&mut fences)
        .with_context(|| format!("reading the fence array of {}", base.display()))?;
    let fence = |bucket: u64| -> u32 {
        let at = (bucket * FENCE_BYTES) as usize;
        u32::from_le_bytes(fences[at..at + 4].try_into().expect("4 bytes"))
    };

    // The head, from the same bytes the base begins with.
    let mut head = vec![0u8; usize::try_from(header.dir_off).context("head length")?];
    src.seek(SeekFrom::Start(0))
        .with_context(|| format!("seeking {}, head", base.display()))?;
    src.read_exact(&mut head)
        .with_context(|| format!("reading the head of {}", base.display()))?;
    std::fs::write(staging.join(HEAD_FILE), &head)
        .with_context(|| format!("writing {}", staging.join(HEAD_FILE).display()))?;

    // The entries, forwards, one shard at a time.
    src.seek(SeekFrom::Start(header.entries_off))
        .with_context(|| format!("seeking {}, entries", base.display()))?;
    let mut count = 0u64;
    let mut bytes = 0u64;
    for shard in 0..shard_count {
        let first = locate::shard_first_bucket(shard);
        // The last shard of a directory that is not a whole number of shards: the boundaries
        // past the end repeat the final count, so every bucket that exists is bounded and no
        // bucket that does not is invented.
        let mut global = Vec::with_capacity(locate::SHARD_BUCKETS as usize + 1);
        for j in 0..=locate::SHARD_BUCKETS {
            global.push(fence((first + j).min(n_buckets)));
        }
        let local = locate::rebase_fences(&global)?;
        let total = local[locate::SHARD_BUCKETS as usize];

        let mut out = vec![0u8; usize::try_from(locate::shard_bytes(total)).context("shard")?];
        for (j, value) in local.iter().enumerate() {
            let at = j * FENCE_BYTES as usize;
            out[at..at + 4].copy_from_slice(&value.to_le_bytes());
        }
        let entries_at = locate::SHARD_FENCE_BYTES as usize;
        src.read_exact(&mut out[entries_at..])
            .with_context(|| format!("reading shard {shard} of {}", base.display()))?;

        let path = entries_dir.join(format!("{shard:05}.bin"));
        let mut file = File::create(&path).with_context(|| format!("creating {}", path.display()))?;
        file.write_all(&out)
            .with_context(|| format!("writing {}", path.display()))?;
        count += 1;
        bytes += out.len() as u64;
    }
    Ok(Cut { count, bytes })
}

/// `r2:mithril-snapshots/index/tx/v1` → (`r2`, `mithril-snapshots`, `index/tx/v1`).
///
/// The pointer carries a key *relative to the bucket*, so the destination has to say where the
/// bucket ends. Every R2 target in this org is `remote:bucket/prefix`, so that is the shape
/// required — and the bucket name cannot be guessed from a path alone.
fn split_dest(dest: &str) -> Result<(String, String, String)> {
    let (remote, path) = dest
        .split_once(':')
        .with_context(|| format!("dest {dest:?} must be `remote:bucket/prefix`"))?;
    let (bucket, prefix) = path
        .split_once('/')
        .with_context(|| format!("dest {dest:?} has no prefix under the bucket"))?;
    if remote.is_empty() || bucket.is_empty() || prefix.trim_matches('/').is_empty() {
        bail!("dest {dest:?} must be `remote:bucket/prefix` with all three non-empty");
    }
    Ok((
        remote.to_string(),
        bucket.to_string(),
        prefix.trim_matches('/').to_string(),
    ))
}

/// SHA-256 of the base, streamed. The build id is derived from this, so it is not optional.
fn digest_of(path: &Path) -> Result<[u8; 32]> {
    let mut file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 8 << 20];
    loop {
        let n = file
            .read(&mut buf)
            .with_context(|| format!("reading {}", path.display()))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let mut digest = [0u8; 32];
    digest.copy_from_slice(&hasher.finalize());
    Ok(digest)
}

fn run_rclone(argv: &[String]) -> Result<()> {
    let status = Command::new(&argv[0])
        .args(&argv[1..])
        .status()
        .with_context(|| format!("running {}", argv[0]))?;
    if !status.success() {
        bail!("rclone exited with {status}");
    }
    Ok(())
}

/// The object count and total bytes under a prefix, from `rclone size`.
///
/// ⚠️ Both halves, for the shards. A count alone passes a set with one object the wrong
/// size; a byte total alone passes one object missing and another duplicated.
fn prefix_size(path: &str) -> Result<(u64, u64)> {
    let out = Command::new("rclone")
        .args(["size", "--json", &format!("{path}/")])
        .output()
        .with_context(|| format!("running rclone size {path}/"))?;
    if !out.status.success() {
        bail!(
            "rclone size {path}/ failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    let size: PrefixSize = serde_json::from_slice(&out.stdout)
        .with_context(|| format!("parsing rclone size --json {path}/"))?;
    Ok((size.count, size.bytes))
}

/// `rclone size --json`'s object, as far as this needs it.
#[derive(serde::Deserialize)]
struct PrefixSize {
    #[serde(rename = "count")]
    count: u64,
    #[serde(rename = "bytes")]
    bytes: u64,
}

/// The object's size on the mirror, from `rclone lsjson --stat`.
fn stat_size(path: &str) -> Result<u64> {
    let out = Command::new("rclone")
        .args(["lsjson", "--stat", path])
        .output()
        .with_context(|| format!("running rclone lsjson --stat {path}"))?;
    if !out.status.success() {
        bail!(
            "rclone lsjson --stat {path} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    let stat: ObjectStat = serde_json::from_slice(&out.stdout)
        .with_context(|| format!("parsing rclone lsjson --stat {path}"))?;
    Ok(stat.size)
}

/// `rclone lsjson --stat`'s object, as far as this needs it. Only the size is read: a publish
/// that landed a different number of bytes than it built is the failure this catches.
#[derive(serde::Deserialize)]
struct ObjectStat {
    #[serde(rename = "Size")]
    size: u64,
}

/// Cut the directory out of the base into its own file.
///
/// ⚠️ **A copy, not a move.** The base still begins with these bytes and every existing
/// reader still ranges them out of it, so this adds an object rather than changing one —
/// nothing that reads the base can be broken by publishing this.
///
/// The point of it is arithmetic: inside a 2.83 GiB base the directory is part of an
/// object the CDN will not cache, so every lookup pays an origin fetch. 64 MiB on its
/// own is under the 512 MB cap, immutable for the build, and cached for every reader
/// after the first.
fn split_directory(base: &Path, out: &Path, dir_len: u64) -> Result<()> {
    let mut src = File::open(base).with_context(|| format!("opening {}", base.display()))?;
    let mut buf = vec![0u8; usize::try_from(dir_len).context("directory length exceeds usize")?];
    src.read_exact(&mut buf)
        .with_context(|| format!("reading {dir_len} bytes of {}", base.display()))?;
    // Temp + rename, the discipline the base itself is built with: a reader must never
    // find a half-written directory, and a torn one would answer lookups wrongly rather
    // than failing.
    let tmp = out.with_extension("tmp");
    std::fs::write(&tmp, &buf).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, out).with_context(|| format!("renaming into {}", out.display()))?;
    Ok(())
}

/// `head.idx` — the header and the era table, 9,280 bytes at mainnet's coverage.
///
/// No `--s3-chunk-size` and no `--transfers`: it is one PUT far under rclone's 200 MiB
/// cutoff. Immutable for the build, like the base and the directory beside it.
fn rclone_head(dest: &str, file: &Path, build: &str) -> Vec<String> {
    let mut argv = vec![
        "rclone".to_string(),
        "copyto".to_string(),
        file.display().to_string(),
        format!("{dest}/{build}/{HEAD_FILE}"),
    ];
    for arg in [
        "--header-upload",
        "Content-Type: application/octet-stream",
        "--header-upload",
        "Cache-Control: public, max-age=31536000, immutable",
        "--s3-no-check-bucket",
        "--ignore-existing",
        "--retries",
        "5",
    ] {
        argv.push(arg.to_string());
    }
    argv
}

/// The entry shards — one `copy` of the staging directory.
///
/// ⚠️ `copy`, not `copyto`, and 30,000-odd objects rather than one. Each is ~93 KiB, which
/// is the whole point: under Cloudflare's 512 MB cacheable cap, so the edge keeps it, and
/// small enough that a cold read of one costs a fraction of a second to warm.
///
/// ⚠️ **`--transfers` is raised to at least 16 here** even when the base upload is told to
/// use fewer. These are latency-bound PUTs, not bandwidth-bound ones: 30,000 objects
/// at 8 in flight is hours, and at 16 it is minutes. Nothing else about the invocation
/// changes, so the credential handling and the cache headers stay identical to the base's.
fn rclone_entries(dest: &str, dir: &Path, build: &str, transfers: usize) -> Vec<String> {
    let mut argv = vec![
        "rclone".to_string(),
        "copy".to_string(),
        dir.display().to_string(),
        format!("{dest}/{build}/entries/"),
    ];
    for arg in [
        "--header-upload",
        "Content-Type: application/octet-stream",
        "--header-upload",
        "Cache-Control: public, max-age=31536000, immutable",
        "--s3-no-check-bucket",
        "--ignore-existing",
        "--retries",
        "5",
    ] {
        argv.push(arg.to_string());
    }
    argv.push("--transfers".to_string());
    argv.push(transfers.max(16).to_string());
    argv.push("--checkers".to_string());
    argv.push(transfers.max(16).to_string());
    argv
}

/// `copyto`, not `copy`: the base is ONE object at an exact path, and a trailing-slash slip with
/// `copy` would place it somewhere else and still exit zero.
fn rclone_base(dest: &str, base: &Path, build: &str, transfers: usize) -> Vec<String> {
    let mut argv = vec![
        "rclone".to_string(),
        "copyto".to_string(),
        base.display().to_string(),
        format!("{dest}/{build}/{BASE_FILE}"),
    ];
    for arg in [
        "--header-upload",
        "Content-Type: application/octet-stream",
        "--header-upload",
        "Cache-Control: public, max-age=31536000, immutable",
        "--s3-no-check-bucket",
        "--s3-chunk-size",
        "16M",
        // The path is build-addressed and the id carries a content digest, so an object already
        // there IS this build. Skipping by name makes a re-run cheap and safe, and it needs no
        // HEAD — which R2's version-qualified form does not implement.
        "--ignore-existing",
        "--retries",
        "5",
        "--progress",
    ] {
        argv.push(arg.to_string());
    }
    argv.push("--transfers".to_string());
    argv.push(transfers.to_string());
    argv
}

/// `dir.idx` — the directory alone, at 64 MiB on mainnet.
///
/// No `--s3-chunk-size` and no `--transfers`: it is one PUT under rclone's 200 MiB
/// cutoff, which is also why it will not be a multipart object. Same immutable
/// directive as the base, because it is immutable for the build too.
fn rclone_dir(dest: &str, file: &Path, build: &str) -> Vec<String> {
    let mut argv = vec![
        "rclone".to_string(),
        "copyto".to_string(),
        file.display().to_string(),
        format!("{dest}/{build}/{DIR_FILE}"),
    ];
    for arg in [
        "--header-upload",
        "Content-Type: application/octet-stream",
        "--header-upload",
        "Cache-Control: public, max-age=31536000, immutable",
        "--s3-no-check-bucket",
        "--ignore-existing",
        "--retries",
        "5",
    ] {
        argv.push(arg.to_string());
    }
    argv
}

/// The pointer is the one mutable object, so it has no `--ignore-existing` and a TTL measured in
/// seconds rather than a year.
fn rclone_pointer(dest: &str, file: &Path) -> Vec<String> {
    let mut argv = vec![
        "rclone".to_string(),
        "copyto".to_string(),
        file.display().to_string(),
        format!("{dest}/{POINTER_FILE}"),
    ];
    for arg in [
        "--header-upload",
        "Content-Type: application/json",
        "--header-upload",
        "Cache-Control: public, max-age=60",
        "--s3-no-check-bucket",
        "--retries",
        "5",
    ] {
        argv.push(arg.to_string());
    }
    argv
}

/// Quote only what needs it, so an argument holding a space (`Cache-Control: …`) survives a
/// copy-paste into a shell.
fn shell_line(argv: &[String]) -> String {
    argv.iter()
        .map(|arg| {
            if arg.contains(' ') {
                format!("\"{arg}\"")
            } else {
                arg.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dest_splits_into_remote_bucket_and_prefix() {
        assert_eq!(
            split_dest("r2:mithril-snapshots/index/tx/v1").unwrap(),
            (
                "r2".to_string(),
                "mithril-snapshots".to_string(),
                "index/tx/v1".to_string()
            )
        );
        assert_eq!(
            split_dest("r2:mithril-snapshots/index/tx/v1/").unwrap(),
            (
                "r2".to_string(),
                "mithril-snapshots".to_string(),
                "index/tx/v1".to_string()
            )
        );
        for bad in [
            "r2:mithril-snapshots",
            "mithril-snapshots/index",
            "r2:",
            ":bucket/x",
        ] {
            assert!(split_dest(bad).is_err(), "{bad} should not split");
        }
    }

    #[test]
    fn the_base_goes_to_the_build_path_and_is_cached_forever() {
        let argv = rclone_base(
            "r2:mithril-snapshots/index/tx/v1",
            Path::new("/i/base.idx"),
            "09201-ab12cd34ef56",
            8,
        );
        assert_eq!(argv[0], "rclone");
        assert_eq!(argv[1], "copyto");
        assert_eq!(argv[2], "/i/base.idx");
        assert_eq!(
            argv[3],
            "r2:mithril-snapshots/index/tx/v1/09201-ab12cd34ef56/base.idx"
        );
        let joined = argv.join(" ");
        assert!(joined.contains("Cache-Control: public, max-age=31536000, immutable"));
        assert!(joined.contains("Content-Type: application/octet-stream"));
        assert!(
            joined.contains("--ignore-existing"),
            "a re-run of the same build is a no-op"
        );
        assert!(joined.contains("--transfers 8"));
    }

    #[test]
    fn the_directory_is_one_immutable_put_beside_the_base() {
        let argv = rclone_dir(
            "r2:mithril-snapshots/index/tx/v1",
            Path::new("/i/dir.idx"),
            "09201-ab12cd34ef56",
        );
        assert_eq!(
            argv[3],
            "r2:mithril-snapshots/index/tx/v1/09201-ab12cd34ef56/dir.idx"
        );
        let joined = argv.join(" ");
        assert!(joined.contains("Cache-Control: public, max-age=31536000, immutable"));
        assert!(joined.contains("--ignore-existing"));
        assert!(
            !joined.contains("--s3-chunk-size"),
            "64 MiB is one PUT under rclone's cutoff, so it is not a multipart object"
        );
    }

    /// The split is a COPY. If it ever became a move, the base would stop beginning with
    /// its own header and every existing reader would break at once.
    #[test]
    fn the_directory_is_the_first_entries_off_bytes_of_the_base() {
        let dir = std::env::temp_dir().join("tx-index-split-test");
        std::fs::create_dir_all(&dir).unwrap();
        let base = dir.join("base.idx");
        let out = dir.join("dir.idx");
        let bytes: Vec<u8> = (0..=255u8).cycle().take(1_000).collect();
        std::fs::write(&base, &bytes).unwrap();

        split_directory(&base, &out, 600).unwrap();

        assert_eq!(std::fs::read(&out).unwrap(), &bytes[..600]);
        assert_eq!(
            std::fs::read(&base).unwrap(),
            bytes,
            "the base is untouched"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_pointer_goes_to_a_fixed_key_with_a_short_ttl() {
        let argv = rclone_pointer(
            "r2:mithril-snapshots/index/tx/v1",
            Path::new("/i/publish.json"),
        );
        assert_eq!(argv[3], "r2:mithril-snapshots/index/tx/v1/current");
        let joined = argv.join(" ");
        assert!(joined.contains("Cache-Control: public, max-age=60"));
        assert!(joined.contains("Content-Type: application/json"));
        assert!(
            !joined.contains("--ignore-existing"),
            "the pointer MUST be replaced on every publish"
        );
    }

    #[test]
    fn a_shell_line_quotes_only_what_holds_a_space() {
        let argv = vec![
            "rclone".to_string(),
            "copyto".to_string(),
            "--header-upload".to_string(),
            "Cache-Control: public".to_string(),
        ];
        assert_eq!(
            shell_line(&argv),
            "rclone copyto --header-upload \"Cache-Control: public\""
        );
    }

    #[test]
    fn the_head_goes_beside_the_base_and_is_immutable() {
        let argv = rclone_head(
            "r2:mithril-snapshots/index/tx/v1",
            Path::new("/s/head.idx"),
            "09201-ab12cd34ef56",
        );
        assert_eq!(
            argv[3],
            "r2:mithril-snapshots/index/tx/v1/09201-ab12cd34ef56/head.idx"
        );
        let joined = argv.join(" ");
        assert!(joined.contains("Cache-Control: public, max-age=31536000, immutable"));
        assert!(joined.contains("--ignore-existing"));
    }

    /// ⚠️ The shards are `copy`, not `copyto` — 30,000 objects, not one — and the
    /// parallelism is raised because these PUTs are latency-bound. A publish that quietly
    /// used the base's 8 would take hours.
    #[test]
    fn the_shards_are_one_copy_of_the_staging_directory() {
        let argv = rclone_entries(
            "r2:mithril-snapshots/index/tx/v1",
            Path::new("/s/entries"),
            "09201-ab12cd34ef56",
            8,
        );
        assert_eq!(argv[1], "copy");
        assert_eq!(argv[2], "/s/entries");
        assert_eq!(
            argv[3],
            "r2:mithril-snapshots/index/tx/v1/09201-ab12cd34ef56/entries/",
            "the trailing slash is what makes this a directory copy"
        );
        let joined = argv.join(" ");
        assert!(joined.contains("Cache-Control: public, max-age=31536000, immutable"));
        assert!(joined.contains("--ignore-existing"));
        assert!(joined.contains("--transfers 16"), "raised from the base's 8");
        assert!(
            !joined.contains("--s3-chunk-size"),
            "93 KiB is one PUT per object; multipart would be per-part billing for nothing"
        );
    }

    /// The whole point of a shard: a reader that has ONE of them can go from a hash to the
    /// body's location without the base, the directory or anything else.
    ///
    /// ⚠️ Bucket 700 is in the SECOND shard, so it is also the test that the fence table
    /// was rebased on that shard's own first value rather than on the base's zero. A publisher
    /// that got that wrong produces shards that read as every bucket starting at entry 0 —
    /// plausible bytes, wrong answer, no failure anywhere.
    #[test]
    fn a_cut_shard_is_one_a_reader_can_locate_through() {
        use tx_index::format::{Entry, Location};

        // 2^10 buckets, so exactly two shards of 512: the smallest base with a shard
        // boundary that is not also the directory's own end.
        let header = BaseHeader::layout(10, 0, 3, 3);
        let n_buckets = 1024usize;
        let mut base = vec![0u8; header.file_len() as usize];
        header.write(&mut base[..64]);

        // Bucket 700 holds one entry, 701 holds two, everything else is empty. The fence
        // array is therefore flat except where it steps: 0 up to 700, 1 at 701, 3 after.
        let entries = [
            Entry {
                prefix: 0xAB00_0000_0000_0000,
                loc: Location {
                    chunk: 7,
                    offset: 100,
                    len: 20,
                },
                aux: None,
            },
            Entry {
                prefix: 0xAB01_0000_0000_0000,
                loc: Location {
                    chunk: 7,
                    offset: 200,
                    len: 30,
                },
                aux: None,
            },
            Entry {
                prefix: 0xAB02_0000_0000_0000,
                loc: Location {
                    chunk: 8,
                    offset: 300,
                    len: 40,
                },
                aux: None,
            },
        ];
        for (i, e) in entries.iter().enumerate() {
            let at = header.entries_off as usize + i * 24;
            e.write(&mut base[at..at + 24]);
        }
        let fence_at = |b: usize| header.dir_off as usize + b * 4;
        for b in 0..=n_buckets {
            let value: u32 = match b {
                0..=700 => 0,
                701 => 1,
                _ => 3,
            };
            base[fence_at(b)..fence_at(b) + 4].copy_from_slice(&value.to_le_bytes());
        }

        let dir = std::env::temp_dir().join("tx-index-cut-test");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("base.idx");
        std::fs::write(&path, &base).unwrap();
        let staging = dir.join("staging");
        std::fs::create_dir_all(&staging).unwrap();

        let cut = cut_shards(&path, &staging, &header).unwrap();
        assert_eq!(cut.count, 2);
        assert_eq!(
            cut.bytes,
            locate::shard_bytes(3) + locate::shard_bytes(0),
            "one shard's fences and entries, and one shard that is only its fences"
        );
        assert_eq!(
            std::fs::read(staging.join(HEAD_FILE)).unwrap(),
            &base[..header.dir_off as usize],
            "the head is the base's own first bytes, not a second copy of the layout"
        );

        // An empty shard is still an object: a reader that indexes a hash into it must find
        // a fence table saying the bucket is empty, not a 404.
        assert_eq!(
            std::fs::read(staging.join("entries/00000.bin"))
                .unwrap()
                .len() as u64,
            locate::SHARD_FENCE_BYTES
        );

        let shard = std::fs::read(staging.join("entries/00001.bin")).unwrap();
        // Bucket 700 is the 189th boundary pair of shard 1, at a shard-local offset.
        assert_eq!(
            locate::shard_fence_span(700),
            locate::Span {
                offset: 752,
                len: 8
            }
        );
        let (lo, hi) = locate::fence_pair(&shard[752..760]).unwrap();
        assert_eq!((lo, hi), (0, 1));
        let run = locate::shard_entries_span(lo, hi);
        let found = locate::search(
            entries[0].prefix,
            lo,
            hi,
            &shard[run.offset as usize..][..run.len as usize],
        )
        .unwrap();
        assert_eq!(found, vec![entries[0]]);

        // The neighbour, in the same shard and one boundary along.
        let (lo, hi) = locate::fence_pair(&shard[756..764]).unwrap();
        assert_eq!((lo, hi), (1, 3));
        let run = locate::shard_entries_span(lo, hi);
        assert_eq!(run.len, 48, "two entries, and the run is fetched at its true length");
        let found = locate::search(
            entries[1].prefix,
            lo,
            hi,
            &shard[run.offset as usize..][..run.len as usize],
        )
        .unwrap();
        assert_eq!(found, vec![entries[1]]);
        let found = locate::search(
            entries[2].prefix,
            lo,
            hi,
            &shard[run.offset as usize..][..run.len as usize],
        )
        .unwrap();
        assert_eq!(found, vec![entries[2]]);

        std::fs::remove_dir_all(&dir).ok();
    }
}
