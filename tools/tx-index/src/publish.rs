//! `publish` — put the base and its pointer where a reader can range them.
//!
//! Two objects, deliberately. The base goes to a build-addressed path and is cached forever;
//! a tiny pointer names the current build and is the only object that ever changes. The order
//! is load-bearing — the base first, so the pointer never names something that is not there.
//!
//! `--run` gates every write, as it does everywhere else here: without it this prints the exact
//! `rclone` invocations and touches nothing. Per
//! `rules/org/defrag/bulk-uploads-run-on-the-arm-box.md`, the run itself belongs on the arm box
//! — the base is GB-scale and rclone's `copyto` restarts an interrupted multipart upload rather
//! than resuming it.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use tx_index::base::{BASE_FILE, BaseFile, base_path};
use tx_index::hosted::{POINTER_FILE, Pointer};

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

    /// Transfer threads for the base.
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
    let digest = digest_of(&base)?;
    let (remote, bucket, prefix) = split_dest(&dest)?;
    let pointer = Pointer::new(&prefix, &header, &digest, bytes);
    let pointer_json = serde_json::to_string_pretty(&pointer)?;
    let pointer_file = a.index_dir.join(POINTER_LOG);

    println!("base        {} · {bytes} bytes", base.display());
    println!("sha256      {}", hex::encode(digest));
    println!(
        "covers      chunks {:05}..={:05} · {} entries",
        pointer.first_chunk, pointer.last_chunk, pointer.entries
    );
    println!("build       {}", pointer.build);
    println!("dest        {remote}:{bucket}");
    println!("writes      {}", pointer.base_key);
    println!("            {prefix}/{POINTER_FILE}");
    println!("pointer     {}", serde_json::to_string(&pointer)?);
    println!();

    let base_argv = rclone_base(&dest, &base, &pointer.build, a.transfers);
    let pointer_argv = rclone_pointer(&dest, &pointer_file);
    println!("{}", shell_line(&base_argv));
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

    if !a.run {
        println!();
        println!("(re-run with --run to execute)");
        return Ok(());
    }

    std::fs::write(&pointer_file, format!("{pointer_json}\n"))
        .with_context(|| format!("writing {}", pointer_file.display()))?;
    for argv in [&base_argv, &pointer_argv] {
        run_rclone(argv)?;
    }

    // A publish that does not know whether it published is the gap the plan already flags, and
    // the base is the object large enough for a silent partial to be possible. So read back what
    // landed and compare it to what was built.
    let base_dest = format!("{dest}/{}/{}", pointer.build, BASE_FILE);
    let pointer_dest = format!("{dest}/{POINTER_FILE}");
    let landed = stat_size(&base_dest)?;
    if landed != bytes {
        bail!(
            "{base_dest} is {landed} bytes on the mirror, not {bytes} — the upload did not land whole"
        );
    }
    let landed_pointer = stat_size(&pointer_dest)?;
    let want_pointer = std::fs::metadata(&pointer_file)
        .with_context(|| format!("sizing {}", pointer_file.display()))?
        .len();
    if landed_pointer != want_pointer {
        bail!("{pointer_dest} is {landed_pointer} bytes on the mirror, not {want_pointer}");
    }

    println!();
    println!("published   {base_dest} · {landed} bytes");
    println!("            {pointer_dest} · {landed_pointer} bytes");
    println!();
    println!("readers     fetch `{prefix}/{POINTER_FILE}`, then its `base_key`");
    Ok(())
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
}
