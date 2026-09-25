//! Turning "I need these 1,055 scattered rows" into the fewest HTTP requests that
//! will fetch them — and knowing where each row ended up afterwards.
//!
//! # Why this is a crate and not a function in a worker
//!
//! The same plan is wanted in three places and must agree in all of them: a
//! Cloudflare Worker fetching through the CDN, a browser fetching directly (which
//! `MITOS_AT_THE_EDGE.md` §10.2 argues is where it *should* happen, because a
//! browser on HTTP/2 out-fans a Worker by a wide margin), and a test. It is pure:
//! offsets in, request shapes out, no I/O and no runtime types.
//!
//! # What it has to get right
//!
//! Multi-range against Cloudflare is unforgiving, and each of these was measured
//! rather than assumed:
//!
//! - **At most [`MAX_SPANS`] = 299 ranges per request.** ⚠️ **300 returns `200`
//!   with the whole object** — at 64 MiB parts that is a 64 MiB download where a
//!   206 would have been a kilobyte. The cap is a correctness bound, not a tuning.
//! - **Ascending and non-overlapping.** Out-of-order or overlapping ranges also
//!   degrade to the whole object.
//! - **One object per request.** Multi-range cannot span objects, which is the
//!   entire reason part size decides how well this works: at 1 MiB a wallet's rows
//!   land ~one per part and there is nothing to coalesce; at 64 MiB they land tens
//!   per part.
//! - **A want can straddle a part boundary**, and then it is two requests' worth
//!   of bytes that have to be stitched back together in order.
//!
//! # Coalescing
//!
//! Two rows 40 bytes apart are cheaper to fetch as one 40-byte-longer range than as
//! two ranges: a multipart part header costs ~80 bytes on the wire, and — far more
//! scarce — **a range costs one of the 299**. So spans closer than
//! [`COALESCE_GAP`] are merged, trading a few dead bytes for request count.
//!
//! # The part nobody gets right first time
//!
//! Producing the requests is the easy half. [`Plan::located`] is the other half:
//! for each input, which request, which span, and what offset inside it. Get that
//! wrong and every row decodes into a plausible neighbour — which is why
//! `round_trip_reassembles_every_want` simulates execution over a synthetic file
//! and checks the bytes, rather than checking the shape of the plan.

use std::ops::Range;

/// The most ranges one request may carry.
///
/// ⚠️ **299, not 300.** Cloudflare documents "between two and 300" and in fact
/// serves 300 as `200 OK` with the complete object. Measured twice, on a 4 MiB
/// fixture and on a real 64 MiB book part.
pub const MAX_SPANS: usize = 299;

/// Spans closer than this are merged. See the module header.
///
/// 256 B is a judgement, not a measurement: a multipart part header is ~80 B, so
/// anything under that is free, and beyond it the scarce resource is the 299 cap
/// rather than bandwidth. A caller that knows its access pattern can pass its own.
pub const COALESCE_GAP: u64 = 256;

/// One thing a caller wants: `len` bytes at a logical offset in one file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Want {
    pub offset: u64,
    pub len: u64,
}

/// One byte range inside one part, **part-local** and **inclusive** at both ends,
/// which is what HTTP's `Range` header means.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Span {
    pub start: u64,
    pub end: u64,
}

impl Span {
    pub fn len(&self) -> u64 {
        self.end - self.start + 1
    }

    pub fn is_empty(&self) -> bool {
        false // an inclusive span always covers at least one byte
    }
}

/// One HTTP request: some ranges of one part.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Batch {
    pub part: u64,
    pub spans: Vec<Span>,
}

impl Batch {
    /// The `Range` header value. Ascending and non-overlapping by construction.
    pub fn range_header(&self) -> String {
        let mut s = String::from("bytes=");
        for (i, sp) in self.spans.iter().enumerate() {
            if i > 0 {
                s.push_str(", ");
            }
            s.push_str(&format!("{}-{}", sp.start, sp.end));
        }
        s
    }

    /// The object key this request goes to, through the one definition of it.
    pub fn key(&self, file: &str) -> String {
        crate::publish::part_key(file, self.part)
    }

    /// Bytes this request will return, excluding multipart framing.
    pub fn bytes(&self) -> u64 {
        self.spans.iter().map(|s| s.len()).sum()
    }
}

/// Where one span's bytes live once a batch has been fetched.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Segment {
    /// Index into [`Plan::batches`].
    pub batch: usize,
    /// Index into that batch's `spans`.
    pub span: usize,
    /// Offset of this segment's bytes within that span.
    pub within: u64,
    pub len: u64,
}

/// Where one [`Want`]'s bytes live — usually one segment, two when it straddled a
/// part boundary. **In order**, so concatenating them reconstructs the want.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Located {
    pub segments: Vec<Segment>,
}

/// The requests to make, and how to find each want in the results.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Plan {
    pub batches: Vec<Batch>,
    /// One entry per input want, **in the order they were given**, however much
    /// the plan reordered the fetching.
    pub located: Vec<Located>,
}

impl Plan {
    /// Total bytes the plan will move, excluding multipart framing.
    pub fn bytes(&self) -> u64 {
        self.batches.iter().map(|b| b.bytes()).sum()
    }

    pub fn requests(&self) -> usize {
        self.batches.len()
    }
}

/// A want split at part boundaries: which part, and the part-local range.
struct Piece {
    want: usize,
    /// Order of this piece within its want, so segments reassemble in order.
    seq: usize,
    part: u64,
    range: Range<u64>,
}

/// Plan the fetches for `wants` over a file laid out in `part_size` parts.
///
/// `file_len` clamps the last part, so a want running past the end of the file is
/// shortened rather than producing a range the server will reject.
///
/// ⚠️ Wants may be given in any order and may overlap; the plan is always emitted
/// ascending and non-overlapping per request, because anything else degrades to the
/// whole object. [`Plan::located`] restores the caller's original order.
pub fn plan(wants: &[Want], part_size: u64, file_len: u64, gap: u64) -> Plan {
    // 1. Cut every want at the part boundaries it crosses.
    let mut pieces: Vec<Piece> = Vec::with_capacity(wants.len());
    for (i, w) in wants.iter().enumerate() {
        let start = w.offset.min(file_len);
        let end = (w.offset + w.len).min(file_len); // exclusive
        let mut at = start;
        let mut seq = 0;
        while at < end {
            let part = at / part_size;
            let part_end = (part + 1) * part_size;
            let stop = end.min(part_end);
            pieces.push(Piece {
                want: i,
                seq,
                part,
                range: (at - part * part_size)..(stop - part * part_size),
            });
            seq += 1;
            at = stop;
        }
    }

    // 2. Group by part, then sort within it. Ascending order is a requirement of
    //    the protocol, not a preference.
    pieces.sort_by_key(|p| (p.part, p.range.start, p.range.end));

    let mut batches: Vec<Batch> = Vec::new();
    // Segments carry the sequence number they were cut in while the plan is being
    // built, because batches are emitted part by part and a straddling want's two
    // halves are produced in that order only by accident. Stripped at the end.
    let mut keyed: Vec<Vec<(usize, Segment)>> = vec![Vec::new(); wants.len()];

    let mut i = 0;
    while i < pieces.len() {
        let part = pieces[i].part;
        let j = pieces[i..].partition_point(|p| p.part == part) + i;
        let group = &pieces[i..j];

        // 3. Coalesce within the part: merge spans separated by less than `gap`.
        //    `spans[k]` covers `members[k]`, the pieces that fell inside it.
        let mut spans: Vec<Span> = Vec::new();
        let mut members: Vec<Vec<usize>> = Vec::new();
        for (k, p) in group.iter().enumerate() {
            let (s, e) = (p.range.start, p.range.end - 1); // inclusive
            match spans.last_mut() {
                // `saturating_sub`: an overlapping piece has `s <= last.end`, and
                // the gap is then zero rather than a wrapped enormous number.
                Some(last) if s.saturating_sub(last.end) <= gap => {
                    last.end = last.end.max(e);
                    members.last_mut().expect("a span has members").push(k);
                }
                _ => {
                    spans.push(Span { start: s, end: e });
                    members.push(vec![k]);
                }
            }
        }

        // 4. Cut into requests of at most MAX_SPANS. See the module header for what
        //    exceeding it costs.
        for chunk_start in (0..spans.len()).step_by(MAX_SPANS) {
            let chunk_end = (chunk_start + MAX_SPANS).min(spans.len());
            let batch = batches.len();
            batches.push(Batch {
                part,
                spans: spans[chunk_start..chunk_end].to_vec(),
            });
            for (local, span_ix) in (chunk_start..chunk_end).enumerate() {
                for &k in &members[span_ix] {
                    let p = &group[k];
                    let span = spans[span_ix];
                    keyed[p.want].push((
                        p.seq,
                        Segment {
                            batch,
                            span: local,
                            within: p.range.start - span.start,
                            len: p.range.end - p.range.start,
                        },
                    ));
                }
            }
        }
        i = j;
    }

    // 5. A want's segments must concatenate in the order the want was CUT, not the
    //    order the batches happened to be emitted in — a want straddling a boundary
    //    has its halves in different parts, and parts are emitted ascending, so
    //    this is usually already right and silently wrong when it is not.
    Plan {
        batches,
        located: keyed
            .into_iter()
            .map(|mut segs| {
                segs.sort_by_key(|(seq, _)| *seq);
                Located {
                    segments: segs.into_iter().map(|(_, s)| s).collect(),
                }
            })
            .collect(),
    }
}

/// Convenience: plan with [`COALESCE_GAP`].
pub fn plan_default(wants: &[Want], part_size: u64, file_len: u64) -> Plan {
    plan(wants, part_size, file_len, COALESCE_GAP)
}

/// One part of a `multipart/byteranges` response: what the server says it is, and
/// its bytes borrowed from the body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Received<'a> {
    /// From the part's own `Content-Range`, **not** from what was asked for.
    pub start: u64,
    pub end: u64,
    pub bytes: &'a [u8],
}

/// The `boundary=` value out of a `content-type` header, if it is multipart.
///
/// ⚠️ `None` for a `206` that is **not** multipart — which is what a server
/// returns when it decided to satisfy a multi-range request with a single range.
/// Treating that as "no parts" rather than as an error is the caller's job.
pub fn boundary_of(content_type: &str) -> Option<&str> {
    if !content_type
        .trim_start()
        .starts_with("multipart/byteranges")
    {
        return None;
    }
    let at = content_type.find("boundary=")? + "boundary=".len();
    let rest = &content_type[at..];
    let end = rest
        .find(|c: char| c == ';' || c.is_whitespace())
        .unwrap_or(rest.len());
    Some(rest[..end].trim_matches('"'))
}

/// Split a `multipart/byteranges` body into its parts.
///
/// ⚠️ **Operates on bytes throughout.** The payload is arbitrary binary; decoding
/// it to a string to split on the boundary corrupts every part that happens to
/// contain invalid UTF-8, which for a row arena is most of them.
pub fn parse_multipart<'a>(body: &'a [u8], boundary: &str) -> Result<Vec<Received<'a>>, String> {
    let dash: Vec<u8> = [b"--", boundary.as_bytes()].concat();
    let mut parts = Vec::new();
    let Some(mut at) = find(body, &dash, 0) else {
        return Err("no boundary in the body".to_owned());
    };
    loop {
        let after = at + dash.len();
        // The closing delimiter is `--boundary--`.
        if body.get(after) == Some(&b'-') && body.get(after + 1) == Some(&b'-') {
            break;
        }
        let Some(head_end) = find(body, b"\r\n\r\n", after) else {
            return Err("a part has no header terminator".to_owned());
        };
        let head = std::str::from_utf8(&body[after..head_end])
            .map_err(|_| "a part header is not UTF-8".to_owned())?;
        let (start, end) = content_range(head)
            .ok_or_else(|| format!("a part has no usable Content-Range: {head:?}"))?;
        let body_at = head_end + 4;
        let next = find(body, &dash, body_at);
        // A part's body is followed by CRLF before the next delimiter.
        let body_end = next.unwrap_or(body.len()).saturating_sub(2);
        if body_end < body_at {
            return Err("a part's body runs backwards".to_owned());
        }
        let bytes = &body[body_at..body_end];
        if bytes.len() as u64 != end - start + 1 {
            return Err(format!(
                "a part claims bytes {start}-{end} ({} B) but carries {} B",
                end - start + 1,
                bytes.len()
            ));
        }
        parts.push(Received { start, end, bytes });
        match next {
            Some(n) => at = n,
            None => break,
        }
    }
    Ok(parts)
}

impl Batch {
    /// Check a response against what this batch asked for.
    ///
    /// ⚠️ **The confirm, and it is not optional.** A response with the right number
    /// of parts in the wrong order, or satisfying ranges that were coalesced
    /// differently, yields plausible bytes at every offset — rows that parse into
    /// other rows. Nothing downstream can detect that, so it is checked here
    /// against the spans that were actually requested.
    pub fn check(&self, received: &[Received<'_>]) -> Result<(), String> {
        if received.len() != self.spans.len() {
            return Err(format!(
                "asked for {} ranges of part {} and got {} back",
                self.spans.len(),
                self.part,
                received.len()
            ));
        }
        for (i, (want, got)) in self.spans.iter().zip(received).enumerate() {
            if want.start != got.start || want.end != got.end {
                return Err(format!(
                    "part {} range {i}: asked {}-{}, got {}-{}",
                    self.part, want.start, want.end, got.start, got.end
                ));
            }
        }
        Ok(())
    }
}

/// Parse a `Content-Range` **header value** — `bytes 100-199/1048576`.
///
/// ⚠️ Needed because **a one-range request is not answered with multipart.** A
/// server given a single range replies with a plain `206` and the bytes, which is
/// correct HTTP and which a reader that only knows how to parse multipart will
/// reject. Coalescing makes single-span batches common, not exotic: at 64 MiB parts
/// a part holding one cluster of rows collapses to exactly one span.
pub fn single_range(value: &str) -> Option<(u64, u64)> {
    let spec = value.trim().strip_prefix("bytes")?.trim();
    let (range, _total) = spec.split_once('/')?;
    let (a, b) = range.trim().split_once('-')?;
    Some((a.trim().parse().ok()?, b.trim().parse().ok()?))
}

fn content_range(head: &str) -> Option<(u64, u64)> {
    let line = head
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("content-range:"))?;
    single_range(line.split_once(':')?.1)
}

fn find(hay: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    (from..=hay.len() - needle.len()).find(|&i| &hay[i..i + needle.len()] == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    const P: u64 = 1 << 20;

    fn w(offset: u64, len: u64) -> Want {
        Want { offset, len }
    }

    /// Every span a plan emits must be ascending and non-overlapping within its
    /// request, and no request may exceed the cap. Anything else silently returns
    /// the whole object.
    fn assert_protocol_safe(p: &Plan) {
        for b in &p.batches {
            assert!(!b.spans.is_empty(), "an empty request is not a request");
            assert!(
                b.spans.len() <= MAX_SPANS,
                "{} spans exceeds the {MAX_SPANS} cap — this returns the WHOLE OBJECT",
                b.spans.len()
            );
            for pair in b.spans.windows(2) {
                assert!(
                    pair[0].end < pair[1].start,
                    "spans must ascend and not overlap: {:?} then {:?}",
                    pair[0],
                    pair[1]
                );
            }
        }
    }

    #[test]
    fn one_want_is_one_request_of_one_span() {
        let p = plan_default(&[w(100, 50)], P, 10 * P);
        assert_eq!(p.requests(), 1);
        assert_eq!(p.batches[0].part, 0);
        assert_eq!(
            p.batches[0].spans,
            vec![Span {
                start: 100,
                end: 149
            }]
        );
        assert_eq!(p.located[0].segments.len(), 1);
        assert_protocol_safe(&p);
    }

    #[test]
    fn near_wants_coalesce_into_one_span() {
        // 40 bytes apart: cheaper as one range than as two, and it saves one of
        // the 299.
        let p = plan_default(&[w(100, 50), w(190, 50)], P, 10 * P);
        assert_eq!(p.requests(), 1);
        assert_eq!(
            p.batches[0].spans,
            vec![Span {
                start: 100,
                end: 239
            }]
        );
        assert_protocol_safe(&p);
    }

    #[test]
    fn distant_wants_stay_separate_spans_in_one_request() {
        let p = plan_default(&[w(100, 50), w(900_000, 50)], P, 10 * P);
        assert_eq!(p.requests(), 1, "same part ⇒ one request");
        assert_eq!(p.batches[0].spans.len(), 2);
        assert_protocol_safe(&p);
    }

    #[test]
    fn different_parts_are_different_requests() {
        // Multi-range cannot span objects. This is the property that makes part
        // size decide how well the whole scheme works.
        let p = plan_default(&[w(100, 50), w(P + 100, 50)], P, 10 * P);
        assert_eq!(p.requests(), 2);
        assert_eq!(p.batches[0].part, 0);
        assert_eq!(p.batches[1].part, 1);
        assert_protocol_safe(&p);
    }

    #[test]
    fn a_want_straddling_a_boundary_becomes_two_ordered_segments() {
        let p = plan_default(&[w(P - 10, 20)], P, 10 * P);
        assert_eq!(p.requests(), 2);
        let segs = &p.located[0].segments;
        assert_eq!(segs.len(), 2);
        assert_eq!(segs[0].len, 10, "the head is in the earlier part");
        assert_eq!(segs[1].len, 10);
        assert_eq!(segs[0].batch, 0);
        assert_eq!(segs[1].batch, 1);
        assert_protocol_safe(&p);
    }

    #[test]
    fn more_than_the_cap_splits_into_several_requests() {
        // ⚠️ The assertion that matters is `assert_protocol_safe`: one request of
        // 300 spans would return the whole object, which at 64 MiB parts is a
        // 64 MiB download that looks like success.
        let wants: Vec<Want> = (0..700).map(|i| w(i * 1000, 10)).collect();
        let p = plan_default(&wants, P, 10 * P);
        assert!(p.requests() >= 3, "700 spans cannot fit in two requests");
        assert_protocol_safe(&p);
    }

    #[test]
    fn a_want_past_the_end_of_the_file_is_clamped_not_rejected() {
        // The speculative over-read for a row near the end of the arena asks for
        // more than exists; the server would reject an unsatisfiable range.
        let p = plan_default(&[w(10 * P - 5, 8192)], P, 10 * P);
        assert_protocol_safe(&p);
        assert_eq!(p.bytes(), 5);
    }

    #[test]
    fn overlapping_wants_do_not_produce_overlapping_spans() {
        // Two rows pointing at the same place — a prefix collision resolving to one
        // row twice. Overlapping ranges degrade to the whole object.
        let p = plan_default(&[w(500, 100), w(520, 100), w(500, 100)], P, 10 * P);
        assert_protocol_safe(&p);
        assert_eq!(p.requests(), 1);
    }

    #[test]
    fn unsorted_wants_are_planned_ascending_but_located_in_input_order() {
        let wants = [w(900_000, 10), w(100, 10), w(500_000, 10)];
        let p = plan_default(&wants, P, 10 * P);
        assert_protocol_safe(&p);
        // Located is in the caller's order, whatever the fetch order was.
        let b = &p.batches[0];
        assert_eq!(
            b.spans[p.located[1].segments[0].span].start, 100,
            "want[1] was the lowest offset and must map to the first span"
        );
    }

    /// ⚠️ **The test the rest are scaffolding for.** Simulate executing the plan
    /// over a synthetic file, reassemble every want from its segments, and check
    /// the bytes. A plan with the right *shape* and wrong offsets decodes every row
    /// into a plausible neighbour, which no structural assertion catches.
    #[test]
    fn round_trip_reassembles_every_want() {
        let file_len = 3 * P + 12_345;
        let byte = |i: u64| (i.wrapping_mul(2_654_435_761) >> 13) as u8;

        let wants: Vec<Want> = (0..400)
            .map(|i| {
                let offset = (i * 7919) % (file_len - 200);
                w(offset, 30 + (i % 170))
            })
            .collect();

        let p = plan_default(&wants, P, file_len);
        assert_protocol_safe(&p);

        // "Fetch": each span's bytes, straight out of the synthetic file.
        let fetched: Vec<Vec<Vec<u8>>> = p
            .batches
            .iter()
            .map(|b| {
                b.spans
                    .iter()
                    .map(|s| {
                        let base = b.part * P;
                        (s.start..=s.end).map(|o| byte(base + o)).collect()
                    })
                    .collect()
            })
            .collect();

        for (i, want) in wants.iter().enumerate() {
            let mut got = Vec::new();
            for seg in &p.located[i].segments {
                let span = &fetched[seg.batch][seg.span];
                got.extend_from_slice(&span[seg.within as usize..(seg.within + seg.len) as usize]);
            }
            let expect: Vec<u8> = (want.offset..(want.offset + want.len).min(file_len))
                .map(byte)
                .collect();
            assert_eq!(
                got, expect,
                "want {i} at {} reassembled wrongly",
                want.offset
            );
        }
    }

    #[test]
    fn a_real_wallet_shape_collapses_into_a_handful_of_requests() {
        // The measurement this exists for: 1,055 rows scattered over a 1.69 GiB
        // arena. At 1 MiB parts that is ~1,000 requests; at 64 MiB it should be
        // tens. Rows are ~8 KiB speculative reads.
        let arena = 1_804_637_928u64;
        let wants: Vec<Want> = (0..1055)
            .map(|i| w((i * 1_710_557) % (arena - 8192), 8192))
            .collect();

        let small = plan_default(&wants, 1 << 20, arena);
        let large = plan_default(&wants, 64 << 20, arena);
        assert_protocol_safe(&small);
        assert_protocol_safe(&large);

        assert!(
            large.requests() * 8 < small.requests(),
            "64 MiB parts should cut requests by an order of magnitude: \
             {} vs {}",
            large.requests(),
            small.requests()
        );
        assert!(
            large.requests() <= 60,
            "1,055 rows over 27 parts should be tens of requests, got {}",
            large.requests()
        );
        // Printed because the ratio is the argument for the part size, and a
        // reader of this test should not have to re-derive it.
        eprintln!(
            "1,055 rows over {arena} B:\n   1 MiB parts → {} requests, {} B\n  64 MiB parts → {} requests, {} B",
            small.requests(),
            small.bytes(),
            large.requests(),
            large.bytes(),
        );
    }

    #[test]
    fn the_range_header_is_what_cloudflare_expects() {
        let p = plan_default(&[w(10, 5), w(1000, 5)], P, 10 * P);
        assert_eq!(p.batches[0].range_header(), "bytes=10-14, 1000-1004");
    }

    // ── multipart ────────────────────────────────────────────────────────────

    fn multipart(boundary: &str, parts: &[(u64, u64, &[u8])]) -> Vec<u8> {
        let mut out = Vec::new();
        for (s, e, body) in parts {
            out.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
            out.extend_from_slice(b"Content-Type: application/octet-stream\r\n");
            out.extend_from_slice(
                format!("Content-Range: bytes {s}-{e}/1048576\r\n\r\n").as_bytes(),
            );
            out.extend_from_slice(body);
            out.extend_from_slice(b"\r\n");
        }
        out.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        out
    }

    #[test]
    fn a_multipart_body_splits_into_its_parts() {
        let body = multipart("abc", &[(10, 12, b"xyz"), (100, 101, b"hi")]);
        let got = parse_multipart(&body, "abc").expect("parses");
        assert_eq!(got.len(), 2);
        assert_eq!(
            (got[0].start, got[0].end, got[0].bytes),
            (10, 12, &b"xyz"[..])
        );
        assert_eq!(
            (got[1].start, got[1].end, got[1].bytes),
            (100, 101, &b"hi"[..])
        );
    }

    #[test]
    fn binary_payloads_survive_including_the_boundary_bytes_and_invalid_utf8() {
        // The reason this parser works on bytes. A row arena is full of sequences
        // that are not valid UTF-8, and a string-based split corrupts them.
        let payload: Vec<u8> = vec![0xff, 0x00, 0x0d, 0x0a, 0x2d, 0x2d, 0xfe, 0x80];
        let body = multipart("zz", &[(0, 7, &payload)]);
        let got = parse_multipart(&body, "zz").expect("parses");
        assert_eq!(got[0].bytes, &payload[..]);
    }

    #[test]
    fn a_part_whose_length_contradicts_its_content_range_is_refused() {
        // Silent truncation would hand the caller short bytes that still parse as
        // *something*. Loud is the only safe option.
        let mut body = multipart("q", &[(0, 9, b"0123456789")]);
        // Chop two bytes out of the payload without fixing the header.
        let at = find(&body, b"\r\n\r\n", 0).unwrap() + 4;
        body.drain(at..at + 2);
        assert!(parse_multipart(&body, "q").is_err());
    }

    #[test]
    fn a_single_range_response_is_read_from_its_content_range_header() {
        // The case a multipart-only reader rejects: one range in, a bare 206 out.
        assert_eq!(single_range("bytes 100-199/1048576"), Some((100, 199)));
        assert_eq!(single_range(" bytes 0-7/8 "), Some((0, 7)));
        assert_eq!(single_range("bytes */1048576"), None, "unsatisfiable");
        assert_eq!(single_range("items 0-1/2"), None, "not a byte range");
    }

    #[test]
    fn boundary_is_read_only_from_a_multipart_content_type() {
        assert_eq!(
            boundary_of("multipart/byteranges; boundary=c79494251400ec62"),
            Some("c79494251400ec62")
        );
        // A 206 that is NOT multipart: the server satisfied the request with a
        // single range. Not an error here, but not parts either.
        assert_eq!(boundary_of("application/octet-stream"), None);
    }

    #[test]
    fn check_refuses_a_response_that_does_not_match_what_was_asked() {
        let p = plan_default(&[w(10, 5), w(1000, 5)], P, 10 * P);
        let b = &p.batches[0];
        let good = [
            Received {
                start: 10,
                end: 14,
                bytes: &[0; 5],
            },
            Received {
                start: 1000,
                end: 1004,
                bytes: &[0; 5],
            },
        ];
        assert!(b.check(&good).is_ok());

        // Right count, wrong order — plausible bytes at every offset.
        let swapped = [good[1], good[0]];
        assert!(b.check(&swapped).is_err(), "order must be checked");
        assert!(b.check(&good[..1]).is_err(), "count must be checked");
    }

    /// End to end over the pure pieces: plan, "serve" the batches as multipart,
    /// parse, check, reassemble — and compare the bytes.
    #[test]
    fn plan_then_multipart_then_reassemble_round_trips() {
        let file_len = 2 * P + 999;
        let byte = |i: u64| (i.wrapping_mul(0x9E3779B1) >> 11) as u8;
        let wants: Vec<Want> = (0..120)
            .map(|i| w((i * 15_013) % (file_len - 300), 40 + (i % 200)))
            .collect();
        let p = plan_default(&wants, P, file_len);

        let bodies: Vec<Vec<u8>> = p
            .batches
            .iter()
            .map(|b| {
                let parts: Vec<(u64, u64, Vec<u8>)> = b
                    .spans
                    .iter()
                    .map(|s| {
                        let base = b.part * P;
                        (
                            s.start,
                            s.end,
                            (s.start..=s.end).map(|o| byte(base + o)).collect(),
                        )
                    })
                    .collect();
                let refs: Vec<(u64, u64, &[u8])> =
                    parts.iter().map(|(s, e, v)| (*s, *e, &v[..])).collect();
                multipart("bnd", &refs)
            })
            .collect();

        let parsed: Vec<Vec<Received<'_>>> = bodies
            .iter()
            .zip(&p.batches)
            .map(|(body, batch)| {
                let got = parse_multipart(body, "bnd").expect("parses");
                batch.check(&got).expect("matches what was asked");
                got
            })
            .collect();

        for (i, want) in wants.iter().enumerate() {
            let mut got = Vec::new();
            for seg in &p.located[i].segments {
                let part = parsed[seg.batch][seg.span].bytes;
                got.extend_from_slice(&part[seg.within as usize..(seg.within + seg.len) as usize]);
            }
            let expect: Vec<u8> = (want.offset..want.offset + want.len).map(byte).collect();
            assert_eq!(got, expect, "want {i} at {}", want.offset);
        }
    }
}
