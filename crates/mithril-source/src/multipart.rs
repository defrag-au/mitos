//! `multipart/byteranges` — the body Cloudflare answers 2–300 ranges with.
//!
//! One range comes back as a plain `206` carrying the bytes. 2–300 **ascending,
//! non-overlapping** ranges come back as `multipart/byteranges`. Above 300
//! Cloudflare ignores `Range` entirely and returns the whole object as `200`, so a
//! batcher caps below that — see [`crate::source::MAX_BATCH_RANGES`].
//!
//! Measured against the live mirror 2026-09-27: `bytes=0-9,9461-9470,5000000-5000009`
//! of `09161.chunk` returned `content-type: multipart/byteranges;
//! boundary=79e43b2626461876`, `content-length: 366`, and:
//!
//! ```text
//! \r\n--79e43b2626461876\r\n
//! Content-Type: application/octet-stream\r\n
//! Content-Range: bytes 0-9/5134154\r\n
//! \r\n
//! <10 bytes>
//! \r\n--79e43b2626461876\r\n
//!   … twice more …
//! \r\n--79e43b2626461876--\r\n
//! ```
//!
//! # The one thing a parser must not do
//!
//! **Do not find the next part by searching for the boundary.** A block's CBOR is
//! arbitrary bytes, so a payload can itself contain `\r\n--<boundary>`; a
//! search-based parser then truncates that part or splits it in two. The
//! `Content-Range` header already states exactly how long each part is, so this
//! parser takes the length from the header and resumes at the byte after the data,
//! then checks that what follows really is the delimiter. Length from the header,
//! position from arithmetic, and the delimiter-checked — never searched for.
//!
//! `parse` is also the place the mirror is called on its lying: the declared length
//! has to match the header's range, and every part has to agree on the object's
//! total size.

use anyhow::{Context, Result, bail};

/// One part of a `multipart/byteranges` body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Part {
    /// First byte of the range, inclusive.
    pub start: u64,
    /// Last byte of the range, inclusive.
    pub end: u64,
    /// The object's total length, as the part's `Content-Range` reported it.
    pub total: u64,
    /// Exactly `end - start + 1` bytes.
    pub data: Vec<u8>,
}

/// The boundary parameter of a `multipart/byteranges` content type.
pub fn boundary_of(content_type: &str) -> Result<&str> {
    let (kind, params) = content_type
        .split_once(';')
        .with_context(|| format!("content-type {content_type:?} has no boundary parameter"))?;
    let kind = kind.trim();
    if kind != "multipart/byteranges" {
        bail!("content-type {kind:?} is not multipart/byteranges");
    }
    params
        .split(';')
        .filter_map(|param| param.split_once('='))
        .find(|(key, _)| key.trim().eq_ignore_ascii_case("boundary"))
        .map(|(_, value)| value.trim().trim_matches('"'))
        .with_context(|| format!("content-type {content_type:?} has no boundary parameter"))
}

/// Parse a `multipart/byteranges` body into its parts, in body order.
pub fn parse(body: &[u8], boundary: &str) -> Result<Vec<Part>> {
    if boundary.is_empty() {
        bail!("an empty boundary cannot be delimited");
    }
    let delimiter: Vec<u8> = [b"--".as_slice(), boundary.as_bytes()].concat();

    // The first delimiter may follow a preamble, which is not ours to interpret.
    let first = find(body, &delimiter)
        .with_context(|| format!("no `--{boundary}` in a {}-byte body", body.len()))?;
    let mut rest = &body[first + delimiter.len()..];

    let mut parts = Vec::new();
    loop {
        if rest.starts_with(b"--") {
            break; // the closing delimiter
        }
        let after = rest.strip_prefix(b"\r\n").unwrap_or(rest);
        let (headers, body_after_headers) = split_headers(after)?;
        let (start, end, total) = content_range(headers)?;
        let want = usize::try_from(end - start + 1).context("part length exceeds usize")?;
        let data = body_after_headers.get(..want).with_context(|| {
            format!(
                "part {start}-{end} declares {want} bytes but only {} follow",
                body_after_headers.len()
            )
        })?;
        parts.push(Part {
            start,
            end,
            total,
            data: data.to_vec(),
        });

        // Resume from the byte after the data — never from a search — and require
        // the delimiter to be exactly there.
        let tail = body_after_headers
            .get(want..)
            .context("part data runs past the end of the body")?;
        let tail = tail.strip_prefix(b"\r\n").unwrap_or(tail);
        if !tail.starts_with(&delimiter) {
            // Covers both directions of a lying length: too long and the slice runs
            // into the framing, too short and it stops inside the payload. Either way
            // the delimiter is not where the header said the part ends.
            let found = String::from_utf8_lossy(&tail[..tail.len().min(24)]);
            bail!(
                "part {start}-{end} declares {want} bytes but they are not followed by \
                 `--{boundary}`; found {found:?}"
            );
        }
        rest = &tail[delimiter.len()..];
    }

    if parts.is_empty() {
        bail!("a multipart/byteranges body with no parts");
    }
    let total = parts[0].total;
    if let Some(odd) = parts.iter().find(|p| p.total != total) {
        bail!(
            "parts disagree on the object's size: {} then {}",
            total,
            odd.total
        );
    }
    Ok(parts)
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|w| w[0] == needle[0] && w == needle)
}

fn split_headers(bytes: &[u8]) -> Result<(&str, &[u8])> {
    let at = find(bytes, b"\r\n\r\n").context("a part has no blank line after its headers")?;
    let headers = std::str::from_utf8(&bytes[..at]).context("part headers are not UTF-8")?;
    Ok((headers, &bytes[at + 4..]))
}

fn content_range(headers: &str) -> Result<(u64, u64, u64)> {
    let value = headers
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(key, _)| key.trim().eq_ignore_ascii_case("content-range"))
        .map(|(_, value)| value.trim())
        .context("a part has no content-range header")?;
    parse_content_range(value)
}

/// `bytes 0-9/5134154`
fn parse_content_range(value: &str) -> Result<(u64, u64, u64)> {
    let rest = value
        .strip_prefix("bytes ")
        .with_context(|| format!("content-range {value:?} is not `bytes …`"))?;
    let (range, total) = rest
        .split_once('/')
        .with_context(|| format!("content-range {value:?} has no `/<total>`"))?;
    let (start, end) = range
        .split_once('-')
        .with_context(|| format!("content-range {value:?} has no `<start>-<end>`"))?;
    let parse = |s: &str| -> Result<u64> {
        s.trim()
            .parse()
            .with_context(|| format!("content-range {value:?} is not numeric"))
    };
    let (start, end, total) = (parse(start)?, parse(end)?, parse(total)?);
    if end < start {
        bail!("content-range {value:?} ends before it starts");
    }
    Ok((start, end, total))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The observed framing, assembled: an empty preamble, then each part as
    /// `--boundary` CRLF headers CRLF CRLF data, then the closing delimiter.
    fn body(boundary: &str, parts: &[(u64, u64, u64, &[u8])]) -> Vec<u8> {
        let mut out = Vec::new();
        for (start, end, total, data) in parts {
            out.extend_from_slice(
                format!(
                    "\r\n--{boundary}\r\nContent-Type: application/octet-stream\r\n\
                     Content-Range: bytes {start}-{end}/{total}\r\n\r\n"
                )
                .as_bytes(),
            );
            out.extend_from_slice(data);
        }
        out.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        out
    }

    #[test]
    fn the_boundary_is_read_out_of_the_content_type() {
        assert_eq!(
            boundary_of("multipart/byteranges; boundary=79e43b2626461876").unwrap(),
            "79e43b2626461876"
        );
        assert_eq!(
            boundary_of("multipart/byteranges; charset=utf-8; boundary=\"a b\"").unwrap(),
            "a b"
        );
        assert!(boundary_of("multipart/byteranges").is_err());
        assert!(boundary_of("application/octet-stream").is_err());
    }

    #[test]
    fn parts_come_back_with_their_ranges_and_bytes() {
        let b = "BOUND";
        let parsed = parse(
            &body(
                b,
                &[
                    (0, 9, 100, b"0123456789"),
                    (40, 42, 100, b"xyz"),
                    (90, 99, 100, b"abcdefghij"),
                ],
            ),
            b,
        )
        .unwrap();

        assert_eq!(parsed.len(), 3);
        assert_eq!((parsed[0].start, parsed[0].end), (0, 9));
        assert_eq!(parsed[0].data, b"0123456789");
        assert_eq!((parsed[1].start, parsed[1].end), (40, 42));
        assert_eq!(parsed[1].data, b"xyz");
        assert_eq!(parsed[2].data, b"abcdefghij");
        assert!(parsed.iter().all(|p| p.total == 100));
    }

    #[test]
    fn a_payload_containing_the_delimiter_does_not_split_the_part() {
        // The whole reason the length comes from the header: these bytes are a
        // legitimate block, and a search-based parser would cut the part here.
        let b = "BOUND";
        let payload: &[u8] = b"AAA\r\n--BOUND\r\nContent-Range: bytes 0-0/1\r\n\r\nBBB";
        let parsed = parse(
            &body(
                b,
                &[
                    (0, payload.len() as u64 - 1, 99, payload),
                    (50, 51, 99, b"hi"),
                ],
            ),
            b,
        )
        .unwrap();

        assert_eq!(parsed.len(), 2);
        assert_eq!(
            parsed[0].data, payload,
            "the delimiter inside the payload is data"
        );
        assert_eq!(parsed[0].data.len(), payload.len());
        assert_eq!(parsed[1].data, b"hi");
    }

    #[test]
    fn a_declared_length_longer_than_the_framing_is_refused() {
        // Says 10 bytes, the builder supplied 3 — the delimiter is not where the
        // header claimed the part ends.
        let b = "BOUND";
        let broken = body(b, &[(0, 9, 100, b"abc")]);
        let err = parse(&broken, b).unwrap_err().to_string();
        assert!(err.contains("declares 10 bytes"), "{err}");
    }

    #[test]
    fn a_declared_length_shorter_than_the_payload_is_refused() {
        // Says 3 bytes, the builder supplied 6: the slice would stop inside the
        // payload, which is exactly the failure a too-large header produces.
        let b = "BOUND";
        let broken = body(b, &[(0, 2, 100, b"abcdef")]);
        let err = parse(&broken, b).unwrap_err().to_string();
        assert!(err.contains("declares 3 bytes"), "{err}");
    }

    #[test]
    fn parts_that_disagree_on_the_object_size_are_refused() {
        let b = "BOUND";
        let broken = body(b, &[(0, 1, 100, b"ab"), (10, 11, 999, b"cd")]);
        let err = parse(&broken, b).unwrap_err().to_string();
        assert!(err.contains("disagree on the object's size"), "{err}");
    }

    #[test]
    fn a_missing_content_range_is_refused() {
        let b = "BOUND";
        let mut broken = Vec::new();
        broken
            .extend_from_slice(format!("\r\n--{b}\r\nContent-Type: text/plain\r\n\r\n").as_bytes());
        broken.extend_from_slice(b"xy");
        broken.extend_from_slice(format!("\r\n--{b}--\r\n").as_bytes());
        let err = parse(&broken, b).unwrap_err().to_string();
        assert!(err.contains("no content-range"), "{err}");
    }

    #[test]
    fn a_body_with_no_delimiter_is_refused() {
        let err = parse(b"not a multipart body", "BOUND")
            .unwrap_err()
            .to_string();
        assert!(err.contains("no `--BOUND`"), "{err}");
    }

    #[test]
    fn content_range_parses_the_shape_rfc_7233_uses() {
        assert_eq!(
            parse_content_range("bytes 0-9/5134154").unwrap(),
            (0, 9, 5134154)
        );
        assert_eq!(
            parse_content_range("bytes 5000000-5000009/5134154").unwrap(),
            (5000000, 5000009, 5134154)
        );
        assert!(parse_content_range("items 0-9/10").is_err());
        assert!(parse_content_range("bytes 9-0/10").is_err());
        assert!(parse_content_range("bytes 0-9").is_err());
    }
}
