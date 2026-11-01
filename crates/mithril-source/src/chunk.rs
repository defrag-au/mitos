//! A chunk addressed by range: the entry table, a streaming block walk, and a
//! `Read + Seek` handle.
//!
//! # The two reads, and why there are two
//!
//! - **Sequential** — a walker decodes every block in order. Blocks are contiguous
//!   by construction (a block runs to the next entry's offset, or to EOF), so the
//!   cost is one request per *window*, not per block: [`Blocks`] keeps a MiB-aligned
//!   read-ahead window and refills it only when a block falls outside.
//! - **Random** — an index resolves one location and wants those bytes.
//!   [`Chunk::read_span`] asks for exactly the span, because over-fetching around a
//!   2 KB transaction body is work nobody asked for. The edge fetches its whole
//!   1 MiB cache line either way, which is why the span does not need to align.
//!
//! RSS is bounded by the window, never the chunk: a 128 MiB chunk costs 4 MiB of
//! buffer, and a block larger than the window is the one case that grows it (the
//! window is `max(block, 4 MiB)`, so it still fits in one request).

use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use anyhow::{Context, Result, bail};

use crate::sidecar::{Entry, parse_primary, read_entries};
use crate::source::RangeSource;

/// How far ahead a sequential reader fetches. One request covers ~500 mainnet
/// blocks at the measured ~4.5 KB average, against one request per block.
const READAHEAD: u64 = 4 * 1024 * 1024;

/// What a window's start is rounded down to, so a read shares a cache line with
/// its neighbours. Cloudflare's Origin Range Requests fetch whole 1 MiB lines from
/// R2 regardless, so a rounded-down start costs nothing and reuses more.
const ALIGN: u64 = 1024 * 1024;

/// One chunk of a snapshot: its blocks' places, and the bytes on demand.
pub struct Chunk {
    source: Arc<dyn RangeSource>,
    number: u16,
    name: String,
    len: u64,
    entries: Vec<Entry>,
}

impl Chunk {
    /// Read `NNNNN.primary` and `NNNNN.secondary`, build the entry table, then size
    /// `NNNNN.chunk`.
    ///
    /// Two requests for the sidecars (~86 KB and ~1.2 MB mainnet) and one for the
    /// length; no chunk bytes move until a block is asked for.
    pub fn open(source: Arc<dyn RangeSource>, number: u16) -> Result<Chunk> {
        let name = format!("{number:05}.chunk");
        let primary_name = format!("{number:05}.primary");
        let secondary_name = format!("{number:05}.secondary");

        let primary_bytes = source
            .read_all(&primary_name)
            .with_context(|| format!("reading {primary_name}"))?;
        let primary =
            parse_primary(&primary_bytes).with_context(|| format!("parsing {primary_name}"))?;
        let secondary = source
            .read_all(&secondary_name)
            .with_context(|| format!("reading {secondary_name}"))?;
        let entries = read_entries(&primary, &secondary)
            .with_context(|| format!("reading {secondary_name} entries"))?;
        if entries.is_empty() {
            bail!("{name} has no blocks");
        }

        let len = source
            .len(&name)
            .with_context(|| format!("sizing {name}"))?;
        let last = entries.last().expect("checked non-empty");
        if last.block_offset >= len {
            bail!(
                "{name} is {len} bytes but its last block is said to start at {}",
                last.block_offset
            );
        }

        Ok(Chunk {
            source,
            number,
            name,
            len,
            entries,
        })
    }

    pub fn number(&self) -> u16 {
        self.number
    }

    /// The chunk file's length in bytes.
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Every block, in slot order.
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    pub fn block_count(&self) -> usize {
        self.entries.len()
    }

    /// A block's byte length: up to the next block's offset, or to EOF for the last.
    pub fn block_len(&self, i: usize) -> Result<u64> {
        let e = self
            .entries
            .get(i)
            .with_context(|| format!("{} has no block {i}", self.name))?;
        let end = self.entries.get(i + 1).map_or(self.len, |n| n.block_offset);
        Ok(end - e.block_offset)
    }

    /// Exactly `len` bytes of the chunk at `offset`, in one request.
    pub fn read_span(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        let end = offset.checked_add(len).context("span end overflow")?;
        if end > self.len {
            bail!(
                "span {offset}..{end} runs past the {}-byte {}",
                self.len,
                self.name
            );
        }
        self.source
            .read_range(&self.name, offset, len)
            .with_context(|| format!("reading {} bytes at {offset}", len))
    }

    /// Fetch many spans at once, in as few requests as the range limit allows.
    ///
    /// **Overlapping and repeated spans are MERGED, not rejected.** Resolving a
    /// batch of inputs routinely reaches the same transaction twice — two inputs
    /// from one transaction is ordinary — and that is the case a batcher exists to
    /// make cheaper, not to refuse. Results come back in the order asked for, one
    /// per span, so a caller can zip them against what it asked.
    ///
    /// The merge is also what makes the request legal: multi-range needs ascending,
    /// non-overlapping spans, and a caller's order and repetitions are neither.
    pub fn read_spans(&self, spans: &[Span]) -> Result<Vec<Vec<u8>>> {
        if spans.is_empty() {
            return Ok(Vec::new());
        }
        for span in spans {
            if span.end() > self.len {
                bail!(
                    "span {}..{} runs past the {}-byte {}",
                    span.offset,
                    span.end(),
                    self.len,
                    self.name
                );
            }
        }

        // Ascending by offset, remembering which merged range each input landed in.
        let mut order: Vec<usize> = (0..spans.len()).collect();
        order.sort_unstable_by_key(|i| (spans[*i].offset, spans[*i].len));

        let mut merged: Vec<Span> = Vec::new();
        let mut home = vec![0usize; spans.len()];
        for &i in &order {
            let span = spans[i];
            if merged
                .last()
                .is_some_and(|m| span.offset <= m.offset + m.len)
            {
                let landing = merged.len() - 1;
                let m = merged.last_mut().expect("just checked");
                m.len = span.end().max(m.offset + m.len) - m.offset;
                home[i] = landing;
            } else {
                home[i] = merged.len();
                merged.push(span);
            }
        }

        let pairs: Vec<(u64, u64)> = merged.iter().map(|s| (s.offset, s.len)).collect();
        let fetched = self
            .source
            .read_ranges(&self.name, &pairs)
            .with_context(|| format!("reading {} span(s) of {}", merged.len(), self.name))?;
        if fetched.len() != merged.len() {
            bail!(
                "{}: asked for {} spans and got {} back",
                self.name,
                merged.len(),
                fetched.len()
            );
        }

        let mut out = Vec::with_capacity(spans.len());
        for (i, span) in spans.iter().enumerate() {
            let buffer = &fetched[home[i]];
            let base = merged[home[i]].offset;
            let start = usize::try_from(span.offset - base).context("span offset exceeds usize")?;
            let len = usize::try_from(span.len).context("span length exceeds usize")?;
            let slice = buffer.get(start..start + len).with_context(|| {
                format!(
                    "{}: {} bytes at {} is outside the {} fetched from {base}",
                    self.name,
                    span.len,
                    span.offset,
                    buffer.len()
                )
            })?;
            out.push(slice.to_vec());
        }
        Ok(out)
    }

    /// Stream the blocks, one read-ahead window at a time.
    pub fn blocks(&self) -> Blocks<'_> {
        Blocks {
            chunk: self,
            next: 0,
            window: Window::new(self.name.clone(), self.len),
        }
    }

    /// A `Read + Seek` view of the same bytes, for a consumer that wants a file.
    ///
    /// Windowed, so a sequential `read_exact` chain costs one request per window
    /// rather than one per call. A seek drops the window, so random reads pay a
    /// request each — which is what a random read costs.
    pub fn file(&self) -> ChunkFile {
        ChunkFile {
            source: Arc::clone(&self.source),
            len: self.len,
            pos: 0,
            window: Window::new(self.name.clone(), self.len),
        }
    }
}

/// One block, with the slot the sidecar claims for it.
#[derive(Clone, Debug)]
pub struct Block {
    /// Slot within the chunk; add the chunk's slot base for an absolute slot.
    pub slot: u32,
    /// Byte offset within the chunk, as the `.secondary` recorded it.
    pub offset: u64,
    pub bytes: Vec<u8>,
}

/// A byte span of a chunk: an offset and a length.
///
/// The unit an index stores and a batched read takes — `tx-index`'s
/// location is this plus the chunk number.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Span {
    pub offset: u64,
    pub len: u64,
}

impl Span {
    pub fn new(offset: u64, len: u64) -> Span {
        Span { offset, len }
    }

    /// The byte after the last one, or [`u64::MAX`] if the length overflows.
    pub fn end(&self) -> u64 {
        self.offset.saturating_add(self.len)
    }
}

/// A streaming walk of a chunk's blocks.
pub struct Blocks<'a> {
    chunk: &'a Chunk,
    next: usize,
    window: Window,
}

impl Blocks<'_> {
    fn fetch(&mut self, offset: u64, len: u64) -> Result<Vec<u8>> {
        self.window
            .ensure(&*self.chunk.source, offset, len, READAHEAD)?;
        Ok(self.window.slice(offset, len).to_vec())
    }
}

impl Iterator for Blocks<'_> {
    type Item = Result<Block>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.chunk.entries.len() {
            return None;
        }
        let i = self.next;
        let entry = self.chunk.entries[i];

        // Any failure ends the walk: the window's position is what makes the next
        // block cheap, and continuing past a bad offset would return plausible
        // bytes from the wrong place.
        let len = match self.chunk.block_len(i) {
            Ok(l) => l,
            Err(e) => {
                self.next = usize::MAX;
                return Some(Err(e));
            }
        };
        let bytes = match self.fetch(entry.block_offset, len) {
            Ok(b) => b,
            Err(e) => {
                self.next = usize::MAX;
                return Some(Err(e));
            }
        };

        self.next += 1;
        Some(Ok(Block {
            slot: entry.slot,
            offset: entry.block_offset,
            bytes,
        }))
    }
}

/// `Read + Seek` over a chunk, windowed.
pub struct ChunkFile {
    source: Arc<dyn RangeSource>,
    len: u64,
    pos: u64,
    window: Window,
}

impl ChunkFile {
    pub fn len_bytes(&self) -> u64 {
        self.len
    }
}

impl Read for ChunkFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() || self.pos >= self.len {
            return Ok(0);
        }
        let want = (buf.len() as u64).min(self.len - self.pos);
        self.window
            .ensure(&*self.source, self.pos, want, READAHEAD)
            .map_err(|e| std::io::Error::other(format!("{e:#}")))?;
        let src = self.window.slice(self.pos, want);
        buf[..want as usize].copy_from_slice(src);
        self.pos += want;
        Ok(want as usize)
    }
}

impl Seek for ChunkFile {
    fn seek(&mut self, from: SeekFrom) -> std::io::Result<u64> {
        let target = match from {
            SeekFrom::Start(n) => i64::try_from(n).unwrap_or(i64::MAX),
            SeekFrom::End(n) => i64::try_from(self.len)
                .unwrap_or(i64::MAX)
                .saturating_add(n),
            SeekFrom::Current(n) => i64::try_from(self.pos)
                .unwrap_or(i64::MAX)
                .saturating_add(n),
        };
        if target < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "seek before the start of the chunk",
            ));
        }
        self.pos = target as u64;
        Ok(self.pos)
    }
}

/// A MiB-aligned read-ahead buffer over one immutable file.
struct Window {
    name: String,
    len: u64,
    base: u64,
    buf: Vec<u8>,
}

impl Window {
    fn new(name: String, len: u64) -> Window {
        Window {
            name,
            len,
            base: 0,
            buf: Vec::new(),
        }
    }

    fn holds(&self, offset: u64, len: u64) -> bool {
        offset >= self.base && offset + len <= self.base + self.buf.len() as u64
    }

    /// Make `[offset, offset + len)` readable, refilling if it is not.
    fn ensure(
        &mut self,
        source: &dyn RangeSource,
        offset: u64,
        len: u64,
        readahead: u64,
    ) -> Result<()> {
        if self.holds(offset, len) {
            return Ok(());
        }
        if offset.checked_add(len).is_none_or(|end| end > self.len) {
            bail!(
                "{}: {len} bytes at {offset} run past the {}-byte file",
                self.name,
                self.len
            );
        }
        let base = offset / ALIGN * ALIGN;
        let want = ((offset - base) + len.max(readahead)).min(self.len - base);
        self.buf = source
            .read_range(&self.name, base, want)
            .with_context(|| format!("{}: reading {want} bytes at {base}", self.name))?;
        self.base = base;
        if !self.holds(offset, len) {
            bail!(
                "{}: asked for {want} bytes at {base} and got {} — not enough for {len} at {offset}",
                self.name,
                self.buf.len()
            );
        }
        Ok(())
    }

    fn slice(&self, offset: u64, len: u64) -> &[u8] {
        let start = (offset - self.base) as usize;
        &self.buf[start..start + len as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::LocalDir;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// A source that records what was asked for, so the request *shape* can be
    /// asserted and not just the bytes.
    #[derive(Default)]
    struct Counting {
        files: HashMap<String, Vec<u8>>,
        reads: Mutex<Vec<(String, u64, u64)>>,
        /// One entry per `read_ranges` call: the spans that went in together.
        batches: Mutex<Vec<Vec<(u64, u64)>>>,
    }

    impl Counting {
        fn with(name: &str, bytes: Vec<u8>) -> Self {
            let mut files = HashMap::new();
            files.insert(name.to_string(), bytes);
            Self {
                files,
                ..Self::default()
            }
        }

        fn reads(&self) -> Vec<(String, u64, u64)> {
            self.reads.lock().expect("reads lock").clone()
        }

        fn batches(&self) -> Vec<Vec<(u64, u64)>> {
            self.batches.lock().expect("batches lock").clone()
        }
    }

    impl RangeSource for Counting {
        fn len(&self, name: &str) -> Result<u64> {
            Ok(self
                .files
                .get(name)
                .with_context(|| format!("no {name}"))?
                .len() as u64)
        }

        fn read_range(&self, name: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
            self.reads
                .lock()
                .expect("reads lock")
                .push((name.to_string(), offset, len));
            let f = self.files.get(name).with_context(|| format!("no {name}"))?;
            let start = usize::try_from(offset)?;
            let end = start + usize::try_from(len)?;
            f.get(start..end)
                .map(<[u8]>::to_vec)
                .with_context(|| format!("{name}: {start}..{end} past the end"))
        }

        // Recorded rather than looped, so a test can assert the batching — the
        // trait's default would make one call per span and prove nothing.
        fn read_ranges(&self, name: &str, spans: &[(u64, u64)]) -> Result<Vec<Vec<u8>>> {
            self.batches
                .lock()
                .expect("batches lock")
                .push(spans.to_vec());
            spans
                .iter()
                .map(|(offset, len)| self.read_range(name, *offset, *len))
                .collect()
        }
    }

    /// A 3-block chunk: entry table and chunk bytes together, so a test can slice
    /// the same bytes the reader will.
    fn fixture() -> (Arc<Counting>, u16, Vec<Entry>) {
        // Blocks are 1000 bytes each; block offsets 0, 1000, 2000.
        let chunk: Vec<u8> = (0..3000u32).map(|i| (i % 251) as u8).collect();
        let entries: Vec<Entry> = [0u64, 1000, 2000]
            .into_iter()
            .enumerate()
            .map(|(i, off)| Entry {
                // `primary_for` below marks slots 0, 1, 2 occupied in order, so these
                // are the slots the reader must report back.
                slot: u32::try_from(i).expect("small"),
                block_offset: off,
                header_offset: 3,
                header_size: 80,
                checksum: crc32fast::hash(&chunk[off as usize..(off + 1000) as usize]),
                header_hash: [0; 32],
                block_or_ebb: [0; 8],
            })
            .collect();

        let mut source = Counting::with("00007.chunk", chunk);
        source
            .files
            .insert("00007.primary".into(), primary_for(&entries));
        source
            .files
            .insert("00007.secondary".into(), secondary_for(&entries));
        (Arc::new(source), 7, entries)
    }

    /// Coerce the concrete test source to the trait object the reader takes.
    ///
    /// A function rather than a cast: the unsized coercion has to happen at a
    /// return position, or type inference resolves `Arc::clone` against the
    /// annotation and asks for `&Arc<dyn RangeSource>`.
    fn dyn_source(source: &Arc<Counting>) -> Arc<dyn RangeSource> {
        let owned: Arc<Counting> = Arc::clone(source);
        owned
    }

    /// `Chunk::open` without a panic, so a test can assert the error.
    fn try_open(source: &Arc<Counting>, number: u16) -> Result<Chunk> {
        Chunk::open(dyn_source(source), number)
    }

    fn open(source: &Arc<Counting>, number: u16) -> Chunk {
        try_open(source, number).unwrap()
    }

    fn primary_for(entries: &[Entry]) -> Vec<u8> {
        // One occupied slot per entry, then a trailing sentinel.
        let mut out = vec![crate::sidecar::PRIMARY_VERSION];
        for (i, _) in entries.iter().enumerate() {
            out.extend_from_slice(&u32::try_from(i * 56).expect("small").to_be_bytes());
        }
        out.extend_from_slice(
            &u32::try_from(entries.len() * 56)
                .expect("small")
                .to_be_bytes(),
        );
        out
    }

    fn secondary_for(entries: &[Entry]) -> Vec<u8> {
        let mut out = Vec::new();
        for e in entries {
            out.extend_from_slice(&e.block_offset.to_be_bytes());
            out.extend_from_slice(&e.header_offset.to_be_bytes());
            out.extend_from_slice(&e.header_size.to_be_bytes());
            out.extend_from_slice(&e.checksum.to_be_bytes());
            out.extend_from_slice(&e.header_hash);
            out.extend_from_slice(&e.block_or_ebb);
        }
        out
    }

    #[test]
    fn a_walk_yields_every_block_in_order_and_its_declared_bytes() {
        let (source, number, entries) = fixture();
        let chunk = open(&source, number);
        assert_eq!(chunk.block_count(), 3);
        assert_eq!(chunk.len(), 3000);

        let blocks: Vec<Block> = chunk.blocks().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(blocks.len(), 3);
        for (b, e) in blocks.iter().zip(&entries) {
            assert_eq!(b.offset, e.block_offset);
            assert_eq!(b.slot, e.slot);
            assert_eq!(b.bytes.len(), 1000);
        }
        // The blocks tile the file exactly — no gap, no overlap.
        assert_eq!(
            blocks.iter().map(|b| b.bytes.len() as u64).sum::<u64>(),
            chunk.len()
        );
        for i in 0..3 {
            assert_eq!(
                blocks[i].bytes,
                chunk.read_span(entries[i].block_offset, 1000).unwrap()
            );
        }
    }

    #[test]
    fn a_sequential_walk_fetches_one_window_not_one_block() {
        let (source, number, _) = fixture();
        let chunk = open(&source, number);
        assert_eq!(chunk.blocks().count(), 3);

        let reads = source.reads();
        let chunk_reads: Vec<_> = reads
            .iter()
            .filter(|(name, _, _)| name == "00007.chunk")
            .collect();
        assert_eq!(
            chunk_reads.len(),
            1,
            "three contiguous blocks must cost one window, got {reads:?}"
        );
        // A window is not the whole file: the request is bounded by the readahead.
        assert_eq!(chunk_reads[0].1, 0);
        assert_eq!(chunk_reads[0].2, 3000);
    }

    #[test]
    fn a_span_read_is_exactly_the_span() {
        let (source, number, _) = fixture();
        let chunk = open(&source, number);
        let got = chunk.read_span(1234, 10).unwrap();
        assert_eq!(got.len(), 10);
        assert_eq!(got, chunk.read_span(1234, 10).unwrap());
        assert_ne!(got, chunk.read_span(1235, 10).unwrap());
        assert!(
            chunk.read_span(2995, 10).is_err(),
            "span past EOF must refuse"
        );
    }

    #[test]
    fn a_seek_drops_the_window_and_a_read_resumes_from_it() {
        let (source, number, _) = fixture();
        let chunk = open(&source, number);
        let mut f = chunk.file();

        let mut head = [0u8; 16];
        f.read_exact(&mut head).unwrap();
        assert_eq!(&head[..], chunk.read_span(0, 16).unwrap().as_slice());

        f.seek(SeekFrom::Start(2500)).unwrap();
        let mut tail = [0u8; 8];
        f.read_exact(&mut tail).unwrap();
        assert_eq!(&tail[..], chunk.read_span(2500, 8).unwrap().as_slice());

        // Reading at EOF is a clean zero, not an error.
        f.seek(SeekFrom::Start(3000)).unwrap();
        assert_eq!(f.read(&mut tail).unwrap(), 0);
        assert!(f.seek(SeekFrom::Current(-9999)).is_err());
    }

    #[test]
    fn a_block_larger_than_the_readahead_still_reads_in_one_request() {
        // 5 MiB block against a 4 MiB readahead: the window must grow to fit it.
        let chunk_bytes: Vec<u8> = (0..5 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
        let entry = Entry {
            slot: 0,
            block_offset: 0,
            header_offset: 3,
            header_size: 80,
            checksum: 0,
            header_hash: [0; 32],
            block_or_ebb: [0; 8],
        };
        let mut source = Counting::with("00003.chunk", chunk_bytes.clone());
        source.files.insert(
            "00003.primary".into(),
            primary_for(std::slice::from_ref(&entry)),
        );
        source.files.insert(
            "00003.secondary".into(),
            secondary_for(std::slice::from_ref(&entry)),
        );
        let source = Arc::new(source);

        let chunk = open(&source, 3);
        let blocks: Vec<Block> = chunk.blocks().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(blocks[0].bytes.len(), chunk_bytes.len());
        assert_eq!(
            source.reads().len(),
            3,
            "one read for the primary, one for the secondary, one for the window"
        );
    }

    #[test]
    fn a_missing_chunk_is_an_error_not_an_empty_walk() {
        let entry = Entry {
            slot: 0,
            block_offset: 0,
            header_offset: 3,
            header_size: 80,
            checksum: 0,
            header_hash: [0; 32],
            block_or_ebb: [0; 8],
        };
        let mut source = Counting::default();
        source.files.insert(
            "00009.primary".into(),
            primary_for(std::slice::from_ref(&entry)),
        );
        source.files.insert(
            "00009.secondary".into(),
            secondary_for(std::slice::from_ref(&entry)),
        );
        let err = match try_open(&Arc::new(source), 9) {
            Ok(_) => panic!("a chunk with no chunk file must not open"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("00009.chunk"), "{err}");
    }

    #[test]
    fn many_scattered_spans_cost_one_batched_call_and_come_back_in_order() {
        let (source, number, _) = fixture();
        let chunk = open(&source, number);

        // Blocks are 1000 bytes. Ask for three heads out of order, and one of them
        // twice — the shape of resolving a batch of inputs.
        let asked = [
            Span::new(2000, 16),
            Span::new(0, 16),
            Span::new(1000, 16),
            Span::new(0, 16),
        ];
        let got = chunk.read_spans(&asked).unwrap();

        assert_eq!(got.len(), asked.len(), "one result per span asked for");
        for (span, bytes) in asked.iter().zip(&got) {
            assert_eq!(
                bytes.as_slice(),
                chunk.read_span(span.offset, span.len).unwrap().as_slice(),
                "span {span:?} must be its own bytes"
            );
        }

        let batches = source.batches();
        assert_eq!(
            batches.len(),
            1,
            "one call for the whole set, got {batches:?}"
        );
        assert_eq!(
            batches[0],
            vec![(0, 16), (1000, 16), (2000, 16)],
            "ascending, with the repeat merged away"
        );
    }

    #[test]
    fn overlapping_spans_merge_and_each_still_gets_its_own_bytes() {
        let (source, number, _) = fixture();
        let chunk = open(&source, number);

        // Contained, partial, and adjacent overlap in one set.
        let asked = [
            Span::new(0, 32),
            Span::new(16, 16),
            Span::new(32, 8),
            Span::new(900, 4),
        ];
        let got = chunk.read_spans(&asked).unwrap();
        assert_eq!(got.len(), 4);
        for (span, bytes) in asked.iter().zip(&got) {
            let expected = chunk.read_span(span.offset, span.len).unwrap();
            assert_eq!(bytes, &expected, "span {span:?}");
        }

        let batches = source.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(
            batches[0],
            vec![(0, 40), (900, 4)],
            "32 and 16 and 8 merge into one 40-byte range"
        );
    }

    #[test]
    fn a_span_past_the_end_is_refused_before_anything_is_fetched() {
        let (source, number, _) = fixture();
        let chunk = open(&source, number);
        let before = source.batches().len();
        let err = chunk
            .read_spans(&[Span::new(0, 8), Span::new(2996, 8)])
            .unwrap_err()
            .to_string();
        assert!(err.contains("runs past"), "{err}");
        assert_eq!(source.batches().len(), before, "no request for a bad set");
        assert!(chunk.read_spans(&[]).unwrap().is_empty());
    }

    #[test]
    fn a_local_directory_is_a_source() {
        let dir = std::env::temp_dir().join("mithril-source-localdir-test");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("00000.chunk"), b"0123456789").unwrap();
        let s = LocalDir::new(&dir);
        assert_eq!(s.len("00000.chunk").unwrap(), 10);
        assert_eq!(s.read_range("00000.chunk", 3, 4).unwrap(), b"3456");
        assert_eq!(s.read_all("00000.chunk").unwrap(), b"0123456789");
        assert!(s.read_range("00000.chunk", 8, 5).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }
}
