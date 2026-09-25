//! Laying a book out for a remote reader: uniform parts, and an attestation.
//!
//! # No credentials, no network
//!
//! This writes a directory. Uploading it is somebody else's job — `rclone`,
//! `wrangler`, `cf-publish` — and keeping the two apart means the layout is
//! inspectable before it is anywhere, and that nothing here can leak a key. It
//! also means the same output serves R2, a second region, or a file server.
//!
//! # Why parts at all, and why 1 MiB
//!
//! Measured 2026-09-25 against never-read objects
//! (`cnft.dev-workers/docs/design/EDGE_PROBE_PLAN.md`): a cold read through
//! Cloudflare costs a **~650 ms floor plus ~35 ms per MiB of depth** to the byte
//! asked for, and **concurrent reads are close to free**. So the cost of a read is
//! set by how deep into an object it reaches, and splitting a 1.69 GiB arena into
//! small objects caps that depth.
//!
//! [`PART`] is **1 MiB** and **uniform**, which matters for two separate reasons:
//!
//! - 1 MiB is the knee. 4 MiB costs ~105 ms more per cold read; below 1 MiB the
//!   floor dominates (650 of 685 ms) and the object count doubles for ~18 ms.
//! - Uniform makes `offset -> (part, offset within part)` arithmetic. A mixed
//!   layout would need a table consulted on every read.
//!
//! ⚠️ **A run can straddle a boundary** — a 332 KB clustered read does so about a
//! third of the time at 1 MiB — so a reader must be able to stitch two parts.
//! Concurrency being free makes that an extra request rather than extra latency.
//!
//! # The attestation, and what it is not
//!
//! `Base::open` validates that each directory partitions its entry array: two
//! passes over 2^21 buckets plus a probe per non-empty bucket. That is 73 ms over
//! a mapping and roughly 2.1 M round trips over a network — not slow, impossible.
//! So the publisher runs the check and records the result, and a remote reader
//! trusts it.
//!
//! ⚠️ That is a real transfer of trust and it is narrow on purpose: the
//! attestation carries `book.meta`'s fields, each file's length and part count,
//! and the validator's verdict. It is **not** a content hash of the bytes — see
//! `ARTIFACT_SHARDING.md` §8 for why integrity is a separate mechanism — and a
//! reader still confirms every hit against the row it lands on, exactly as the
//! local reader does. The narrow-key-plus-confirm discipline is untouched.

use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::format::{
    self, BaseFingerprint, DIRECTORY_ADDR, DIRECTORY_OUTREF, DIRECTORY_POLICY, META_FILE, Meta,
    POLICY_FILE, ROWS_FILE, TABLE_ADDR, TABLE_OUTREF, TABLE_POLICY,
};

/// The part size every reader assumes. See the module header for why 1 MiB.
pub const PART: u64 = 1 << 20;

/// The files a published book is made of, in the order a reader needs them.
///
/// `book.meta` is not here: it is carried inside the attestation instead, so a
/// reader learns the artifact's shape and its parts from one read rather than two.
pub const PARTED: [&str; 8] = [
    ROWS_FILE,
    TABLE_OUTREF,
    TABLE_ADDR,
    TABLE_POLICY,
    DIRECTORY_OUTREF,
    DIRECTORY_ADDR,
    DIRECTORY_POLICY,
    POLICY_FILE,
];

/// One file's layout in the published form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartedFile {
    pub name: String,
    /// Total logical length. A reader checks this against what `book.meta` implies
    /// before trusting any offset arithmetic.
    pub len: u64,
    /// `ceil(len / PART)`. The last part is short unless `len` divides exactly.
    pub parts: u64,
}

/// What a remote reader is asked to take on trust, and nothing more.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attestation {
    pub fingerprint: BaseFingerprint,
    pub part_size: u64,
    pub files: Vec<PartedFile>,
    /// `book.meta`'s own text, carried verbatim rather than re-rendered — the
    /// publisher must not reword what the builder wrote.
    pub meta: String,
    /// Whether the publisher ran the directory-partition check and it passed.
    /// A reader that finds `false` here should refuse rather than proceed.
    pub validated: bool,
}

/// The version [`Attestation::render`] writes and [`Attestation::parse`] accepts.
///
/// Separate from `LAYOUT_VERSION`: this describes the *manifest*, and the manifest
/// can gain a field without a single byte of the artifact changing.
pub const ATTESTATION_VERSION: u32 = 1;

impl Attestation {
    /// Read back what [`Attestation::render`] wrote.
    ///
    /// The remote reader's **first** read, and the only one it makes before it
    /// knows the artifact's shape — so every failure here has to be loud. An
    /// unparseable manifest means the reader cannot do offset arithmetic at all,
    /// which is a refusal rather than a degraded mode.
    ///
    /// ⚠️ `validated` is parsed, not enforced. Whether `false` is fatal is the
    /// *reader's* call — this function reports what the file says.
    pub fn parse(text: &str) -> Result<Attestation> {
        let mut version = None;
        let mut fingerprint = None;
        let mut part_size = None;
        let mut validated = None;
        let mut files = Vec::new();
        let mut meta = String::new();
        let mut in_meta = false;
        let mut meta_closed = false;

        for (n, line) in text.lines().enumerate() {
            let lineno = n + 1;
            if in_meta {
                if line == "meta_end" {
                    in_meta = false;
                    meta_closed = true;
                    continue;
                }
                // `render` indents by exactly two spaces. Strip that and nothing
                // else: a meta line's own leading whitespace would be its content.
                meta.push_str(line.strip_prefix("  ").unwrap_or(line));
                meta.push('\n');
                continue;
            }
            let Some((key, rest)) = line.split_once(' ').or(Some((line, ""))) else {
                continue;
            };
            match key {
                "" => continue,
                "attestation_version" => {
                    version = Some(
                        rest.parse::<u32>()
                            .with_context(|| format!("line {lineno}: attestation_version"))?,
                    )
                }
                "fingerprint" => {
                    fingerprint = Some(
                        rest.parse::<BaseFingerprint>()
                            .with_context(|| format!("line {lineno}: fingerprint"))?,
                    )
                }
                "part_size" => {
                    part_size = Some(
                        rest.parse::<u64>()
                            .with_context(|| format!("line {lineno}: part_size"))?,
                    )
                }
                "validated" => match rest {
                    "true" => validated = Some(true),
                    "false" => validated = Some(false),
                    other => bail!("line {lineno}: validated is {other:?}, not true or false"),
                },
                "file" => {
                    let mut it = rest.split(' ');
                    let (Some(name), Some(len), Some(parts), None) =
                        (it.next(), it.next(), it.next(), it.next())
                    else {
                        bail!("line {lineno}: expected `file <name> <len> <parts>`, got {rest:?}");
                    };
                    let len: u64 = len
                        .parse()
                        .with_context(|| format!("line {lineno}: {name} length"))?;
                    let parts: u64 = parts
                        .parse()
                        .with_context(|| format!("line {lineno}: {name} part count"))?;
                    files.push(PartedFile {
                        name: name.to_owned(),
                        len,
                        parts,
                    });
                }
                "meta_begin" => in_meta = true,
                "meta_end" => bail!("line {lineno}: meta_end without meta_begin"),
                other => bail!("line {lineno}: unknown key {other:?}"),
            }
        }

        if in_meta {
            bail!("meta_begin was never closed by meta_end");
        }
        let version = version.context("attestation_version is missing")?;
        if version != ATTESTATION_VERSION {
            bail!("attestation is version {version}; this build reads {ATTESTATION_VERSION}");
        }
        let part_size = part_size.context("part_size is missing")?;
        if files.is_empty() {
            bail!("attestation lists no files");
        }
        if !meta_closed {
            bail!("attestation carries no meta block");
        }

        // The part count is what every offset calculation depends on, so check it
        // against the length rather than trusting two numbers that can disagree.
        // A manifest written by a publisher with a different `PART` is the failure
        // this catches, and it would otherwise read as scrambled rows.
        for f in &files {
            let want = f.len.div_ceil(part_size);
            if f.parts != want {
                bail!(
                    "{}: {} B at {part_size} B implies {want} parts, manifest says {}",
                    f.name,
                    f.len,
                    f.parts
                );
            }
        }

        Ok(Attestation {
            fingerprint: fingerprint.context("fingerprint is missing")?,
            part_size,
            files,
            meta,
            validated: validated.context("validated is missing")?,
        })
    }

    /// One file's entry, or `None` if the manifest does not carry it.
    pub fn file(&self, name: &str) -> Option<&PartedFile> {
        self.files.iter().find(|f| f.name == name)
    }

    /// Render as the line-keyed text `book.meta` already uses, because a format a
    /// person can diff is worth more here than a compact one — this is the file
    /// someone reads when a reader and a publisher disagree.
    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("attestation_version {ATTESTATION_VERSION}\n"));
        out.push_str(&format!("fingerprint {}\n", self.fingerprint));
        out.push_str(&format!("part_size {}\n", self.part_size));
        out.push_str(&format!("validated {}\n", self.validated));
        for f in &self.files {
            out.push_str(&format!("file {} {} {}\n", f.name, f.len, f.parts));
        }
        out.push_str("meta_begin\n");
        for line in self.meta.lines() {
            out.push_str("  ");
            out.push_str(line);
            out.push('\n');
        }
        out.push_str("meta_end\n");
        out
    }
}

/// How many parts a file of `len` bytes becomes.
pub fn part_count(len: u64) -> u64 {
    len.div_ceil(PART)
}

/// The part a logical offset falls in, and where it sits inside that part.
///
/// The arithmetic uniform parts exist for. A reader resolving a row offset calls
/// this and fetches one object; if the row runs past the part's end it stitches
/// the next one (see the module header's straddling note).
pub fn locate(offset: u64) -> (u64, u64) {
    (offset / PART, offset % PART)
}

/// The object key for one part — zero-padded so a lexical listing is also the
/// numeric order, which is what makes a prune or a diff legible.
pub fn part_key(name: &str, part: u64) -> String {
    format!("{name}/{part:05}")
}

/// Split one file into `PART`-sized objects under `out/<name>/NNNNN`.
fn split_file(src: &Path, out: &Path, name: &str) -> Result<PartedFile> {
    let mut f = File::open(src).with_context(|| format!("opening {}", src.display()))?;
    let len = f
        .metadata()
        .with_context(|| format!("statting {}", src.display()))?
        .len();
    let dir = out.join(name);
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;

    let mut buf = vec![0u8; PART as usize];
    let mut part = 0u64;
    let mut written = 0u64;
    while written < len {
        let want = (len - written).min(PART) as usize;
        f.read_exact(&mut buf[..want])
            .with_context(|| format!("reading {} at {written}", src.display()))?;
        let key = dir.join(format!("{part:05}"));
        let mut w = BufWriter::new(
            File::create(&key).with_context(|| format!("creating {}", key.display()))?,
        );
        w.write_all(&buf[..want])?;
        w.flush()?;
        written += want as u64;
        part += 1;
    }

    // Belt and braces: the reader's offset arithmetic depends on this identity, so
    // assert it here rather than discover a short file over a network.
    if part != part_count(len) {
        bail!(
            "{name} split into {part} parts; {len} B at {PART} B implies {}",
            part_count(len)
        );
    }
    Ok(PartedFile {
        name: name.to_owned(),
        len,
        parts: part,
    })
}

/// Lay a book out for upload: `out/<file>/NNNNN` plus `out/attestation`.
///
/// `validated` should be the result of actually opening the book locally — the
/// caller does that, because opening it is what proves the directories partition
/// their arrays and this module must not claim a check it did not run.
pub fn publish(book: &Path, out: &Path, validated: bool) -> Result<Attestation> {
    let meta_path = book.join(META_FILE);
    let meta_text = std::fs::read_to_string(&meta_path)
        .with_context(|| format!("reading {}", meta_path.display()))?;
    let meta = Meta::parse(&meta_text)?;
    if meta.layout_version != format::LAYOUT_VERSION {
        bail!(
            "book.meta is layout_version {}; this build publishes {}",
            meta.layout_version,
            format::LAYOUT_VERSION
        );
    }

    std::fs::create_dir_all(out).with_context(|| format!("creating {}", out.display()))?;
    let mut files = Vec::with_capacity(PARTED.len());
    for name in PARTED {
        files.push(split_file(&book.join(name), out, name)?);
    }

    // The one cross-check worth making here: the row arena's published length must
    // match what the builder recorded, or every offset in every index is wrong.
    let rows = files
        .iter()
        .find(|f| f.name == ROWS_FILE)
        .expect("ROWS_FILE is in PARTED");
    if rows.len != meta.rows_bytes {
        bail!(
            "{ROWS_FILE} is {} B but book.meta claims {}",
            rows.len,
            meta.rows_bytes
        );
    }

    let att = Attestation {
        fingerprint: meta.fingerprint(),
        part_size: PART,
        files,
        meta: meta_text,
        validated,
    };
    let path = out.join("attestation");
    std::fs::write(&path, att.render()).with_context(|| format!("writing {}", path.display()))?;
    Ok(att)
}

/// Every object key a published book consists of, for an uploader to walk.
pub fn keys(att: &Attestation) -> Vec<PathBuf> {
    let mut out = vec![PathBuf::from("attestation")];
    for f in &att.files {
        for p in 0..f.parts {
            out.push(PathBuf::from(part_key(&f.name, p)));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locate_is_the_inverse_of_part_layout() {
        assert_eq!(locate(0), (0, 0));
        assert_eq!(locate(PART - 1), (0, PART - 1));
        assert_eq!(locate(PART), (1, 0));
        assert_eq!(locate(PART * 3 + 17), (3, 17));
    }

    #[test]
    fn part_count_is_ceiling_not_floor() {
        // A file one byte over a boundary needs another part; getting this wrong
        // truncates the last row of the arena.
        assert_eq!(part_count(0), 0);
        assert_eq!(part_count(1), 1);
        assert_eq!(part_count(PART), 1);
        assert_eq!(part_count(PART + 1), 2);
    }

    #[test]
    fn keys_are_zero_padded_so_lexical_order_is_numeric() {
        let mut ks = vec![part_key("rows.bin", 10), part_key("rows.bin", 9)];
        ks.sort();
        assert_eq!(ks, vec!["rows.bin/00009", "rows.bin/00010"]);
    }

    #[test]
    fn a_rendered_attestation_is_diffable_and_carries_meta_verbatim() {
        let att = Attestation {
            fingerprint: Meta::default().fingerprint(),
            part_size: PART,
            files: vec![PartedFile {
                name: "rows.bin".into(),
                len: PART + 10,
                parts: 2,
            }],
            meta: "layout_version 3\nrows 7\n".into(),
            validated: true,
        };
        let text = att.render();
        assert!(text.contains("file rows.bin 1048586 2"));
        assert!(text.contains("validated true"));
        // Indented rather than reworded: a publisher must not paraphrase what the
        // builder wrote, because the two disagreeing is the thing this file exists
        // to make visible.
        assert!(text.contains("  layout_version 3"));
        assert!(text.contains("  rows 7"));
    }

    /// The real manifest shape, as `fold-probe publish` writes it.
    fn sample() -> Attestation {
        Attestation {
            fingerprint: "59c0554061419b7e00f607cb77c69f80"
                .parse()
                .expect("32 hex digits"),
            part_size: PART,
            files: vec![
                PartedFile {
                    name: ROWS_FILE.into(),
                    len: 21_924_039,
                    parts: 21,
                },
                PartedFile {
                    name: POLICY_FILE.into(),
                    len: 372_664,
                    parts: 1,
                },
            ],
            meta: "layout_version 3\nrows 128567\nbase_slot 198590382\n".into(),
            validated: true,
        }
    }

    #[test]
    fn render_and_parse_are_inverses() {
        let att = sample();
        let back = Attestation::parse(&att.render()).expect("round trip");
        assert_eq!(back, att);
        // The meta block specifically: a parser that kept the two-space indent
        // would hand `Meta::parse` lines it rejects.
        assert!(back.meta.starts_with("layout_version 3\n"));
    }

    #[test]
    fn the_fingerprint_survives_the_text_form() {
        let att = sample();
        let back = Attestation::parse(&att.render()).expect("round trip");
        assert_eq!(
            back.fingerprint.to_string(),
            "59c0554061419b7e00f607cb77c69f80"
        );
        assert_eq!(back.fingerprint, att.fingerprint);
    }

    #[test]
    fn a_part_count_that_contradicts_the_length_is_refused() {
        // The failure this check exists for: a manifest written by a publisher
        // using a different PART. Every offset would be wrong, and the rows would
        // read as scrambled rather than as an error.
        let mut att = sample();
        att.files[0].parts = 6;
        let err = Attestation::parse(&att.render()).expect_err("must refuse");
        assert!(
            err.to_string().contains("implies 21 parts"),
            "unhelpful error: {err}"
        );
    }

    #[test]
    fn a_truncated_or_unknown_manifest_is_refused_rather_than_guessed() {
        // Each of these is a manifest a reader cannot do arithmetic from, so each
        // has to be a refusal rather than a default.
        for (what, text) in [
            ("no version", "part_size 1048576\nvalidated true\n"),
            (
                "no files",
                "attestation_version 1\nfingerprint 59c0554061419b7e00f607cb77c69f80\n\
                 part_size 1048576\nvalidated true\nmeta_begin\nmeta_end\n",
            ),
            (
                "no meta block",
                "attestation_version 1\nfingerprint 59c0554061419b7e00f607cb77c69f80\n\
                 part_size 1048576\nvalidated true\nfile rows.bin 10 1\n",
            ),
            (
                "unclosed meta",
                "attestation_version 1\nfingerprint 59c0554061419b7e00f607cb77c69f80\n\
                 part_size 1048576\nvalidated true\nfile rows.bin 10 1\nmeta_begin\n  rows 1\n",
            ),
            (
                "a key from a future version",
                "attestation_version 1\nfingerprint 59c0554061419b7e00f607cb77c69f80\n\
                 part_size 1048576\nvalidated true\nfile rows.bin 10 1\ncontent_hash abc\n\
                 meta_begin\nmeta_end\n",
            ),
            (
                "a short fingerprint",
                "attestation_version 1\nfingerprint 59c0\npart_size 1048576\n\
                 validated true\nfile rows.bin 10 1\nmeta_begin\nmeta_end\n",
            ),
        ] {
            assert!(
                Attestation::parse(text).is_err(),
                "{what} must be refused, not guessed past"
            );
        }
    }

    #[test]
    fn validated_false_parses_and_is_left_for_the_reader_to_judge() {
        // Reported, not enforced: whether an unvalidated book is usable is the
        // reader's decision, and a parser that refused here would hide it.
        let mut att = sample();
        att.validated = false;
        let back = Attestation::parse(&att.render()).expect("parses");
        assert!(!back.validated);
    }
}
