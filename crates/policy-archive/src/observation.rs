//! Observations — what a script output HELD, recorded whether or not anything
//! could decode it.
//!
//! # Why this is a second file kind and not more movement columns
//!
//! A movement is conserved: the walk knows every input carrying the asset was
//! seen as an output first, and the per-transaction sum is what makes the log
//! self-verifying. An observation is not conserved and must never pretend to
//! be — it is a reading taken from one output, at one slot, with no claim that
//! its sources were resolved. Mixing the two would put an unverified row inside
//! the invariant that makes the verified ones trustworthy.
//!
//! It is also far smaller. $PERP has 29,545 movement rows against ~5,000 pool
//! states, so the two have different shapes, different cardinalities and
//! different readers.
//!
//! # The raw output IS the observation; the decode is an annotation
//!
//! Every column from `address` through `datum` is REQUIRED and comes straight
//! off the chain. Everything after it is OPTIONAL and is whatever a decoder
//! made of it at walk time.
//!
//! That split is the whole point, and it exists to protect a property the
//! movement log already has. Cohort classification is a pure function of the
//! address, so `classify` re-derives all history in about a second and a
//! reclassification never means a re-walk. A decoded observation has no such
//! property: it was produced by whatever decoders existed when the pass ran, so
//! adding a Sundae decoder next year would leave every Sundae pool in the
//! archive undecoded — and recovering them would mean walking certified chunks
//! again.
//!
//! Recording the RAW output alongside the annotation removes that. A decoder
//! added later re-derives its history **from the archive**, because the archive
//! kept the bytes it would have needed. The cost is datum bytes; the thing
//! bought is that the archive can re-interpret itself.
//!
//! Measured precedent for why this matters: $Dong's pooled share went from
//! 1.48% to 11.32% purely from recognising a second Splash contract, and
//! nothing errored in between.

use std::io::Write;
use std::sync::Arc;

use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::Type;

use crate::Result;
use crate::schema::Stamp;

/// The file a pass writes its observations to, beside `movements.parquet`.
pub const OBSERVATIONS: &str = "observations.parquet";

/// Bumped only when a reader of the previous version could MISREAD a file of
/// this one. A new trailing optional column does not bump it.
pub const OBSERVATION_FORMAT: u32 = 1;

/// Spelled out rather than derived, for the same reason the movement schema is:
/// the column names are a contract with every reader, not a by-product of field
/// order.
pub const MESSAGE_TYPE: &str = "
    message observation {
        REQUIRED INT64 slot;
        REQUIRED INT64 block_time;
        REQUIRED BYTE_ARRAY tx_hash;
        REQUIRED BYTE_ARRAY address;
        REQUIRED INT64 lovelace;
        REQUIRED BYTE_ARRAY unit_name;
        REQUIRED INT64 unit_amount;
        OPTIONAL BYTE_ARRAY datum;
        OPTIONAL BYTE_ARRAY venue;
        OPTIONAL BYTE_ARRAY key_policy;
        OPTIONAL BYTE_ARRAY key_name;
        OPTIONAL BYTE_ARRAY key_basis;
        OPTIONAL INT64 base_reserve;
        OPTIONAL BYTE_ARRAY quote_policy;
        OPTIONAL BYTE_ARRAY quote_name;
        OPTIONAL INT64 quote_reserve;
        OPTIONAL INT64 fee_bps;
        OPTIONAL INT64 total_lp;
        OPTIONAL BYTE_ARRAY reserve_source;
    }
";

/// What a decoder made of an output. Absent on a candidate nothing claimed.
///
/// `quote_*` is `Option` inside an already-optional decode because the two
/// unknowns are different questions: no `Decoded` at all means nothing
/// recognised this output; a `Decoded` with no `quote_policy` means a venue
/// recognised it and could not name what it pairs with.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Decoded {
    pub venue: String,
    pub key_policy: Vec<u8>,
    pub key_name: Vec<u8>,
    pub key_basis: String,
    pub base_reserve: i64,
    pub quote_policy: Option<Vec<u8>>,
    pub quote_name: Option<Vec<u8>>,
    /// `None` when the pair is known but its amount was not decodable from
    /// this output — see `mitos-pool-observe`'s `Side::reserve`.
    pub quote_reserve: Option<i64>,
    pub fee_bps: Option<i64>,
    pub total_lp: Option<i64>,
    pub reserve_source: String,
}

/// One script output holding the watched policy's units, at one transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observation {
    pub slot: u64,
    pub block_time: u64,
    pub tx_hash: Vec<u8>,
    /// The script address. Bech32, as the movement rows spell it.
    pub address: String,
    pub lovelace: i64,
    pub unit_name: Vec<u8>,
    /// How much of the watched policy's unit this output holds.
    pub unit_amount: i64,
    /// Inline, or resolved from the creating transaction's witness set.
    /// `None` when the output carries none — which is itself a finding: a pool
    /// with no datum is not a pool anybody can read.
    pub datum: Option<Vec<u8>>,
    pub decoded: Option<Decoded>,
}

impl Observation {
    /// An output nothing recognised — kept precisely so a decoder added later
    /// can recognise it without a re-walk.
    pub fn is_candidate(&self) -> bool {
        self.decoded.is_none()
    }
}

fn schema() -> Result<Arc<Type>> {
    Ok(Arc::new(parse_message_type(MESSAGE_TYPE)?))
}

/// Streams observations into a stamped file.
///
/// No [`crate::groups::Grouper`]: observations do not drive the density
/// histogram, and one row group per file is right at these cardinalities. Rows
/// must still arrive in slot order, so a reader can seek by the footer's
/// statistics.
pub struct ObservationWriter<W: Write + Send> {
    inner: SerializedFileWriter<W>,
    buf: Vec<Observation>,
    rows: u64,
    min_slot: Option<u64>,
    max_slot: Option<u64>,
}

impl<W: Write + Send> ObservationWriter<W> {
    pub fn new(sink: W, stamp: &Stamp) -> Result<Self> {
        let props = WriterProperties::builder()
            // SNAPPY, because every reader links the codec — including wasm,
            // where zstd is a C library. Same constraint as the movement file.
            .set_compression(Compression::SNAPPY)
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_key_value_metadata(Some(stamp.to_key_values()))
            .build();
        Ok(ObservationWriter {
            inner: SerializedFileWriter::new(sink, schema()?, Arc::new(props))?,
            buf: Vec::new(),
            rows: 0,
            min_slot: None,
            max_slot: None,
        })
    }

    pub fn push(&mut self, o: Observation) {
        self.min_slot = Some(self.min_slot.map_or(o.slot, |m| m.min(o.slot)));
        self.max_slot = Some(self.max_slot.map_or(o.slot, |m| m.max(o.slot)));
        self.rows += 1;
        self.buf.push(o);
    }

    pub fn close(mut self) -> Result<Written> {
        if !self.buf.is_empty() {
            let rows = std::mem::take(&mut self.buf);
            write_group(&mut self.inner, &rows)?;
        }
        self.inner.close()?;
        Ok(Written {
            rows: self.rows,
            min_slot: self.min_slot,
            max_slot: self.max_slot,
        })
    }
}

/// What a finished observation file holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Written {
    pub rows: u64,
    pub min_slot: Option<u64>,
    pub max_slot: Option<u64>,
}

fn write_group<W: Write + Send>(
    w: &mut SerializedFileWriter<W>,
    rows: &[Observation],
) -> Result<()> {
    let mut rg = w.next_row_group()?;
    let mut col = 0usize;

    macro_rules! req_i64 {
        ($f:expr) => {{
            let vals: Vec<i64> = rows.iter().map($f).collect();
            let mut c = rg.next_column()?.expect("column count matches schema");
            c.typed::<Int64Type>().write_batch(&vals, None, None)?;
            c.close()?;
            col += 1;
        }};
    }
    macro_rules! req_bytes {
        ($f:expr) => {{
            let vals: Vec<ByteArray> = rows.iter().map($f).collect();
            let mut c = rg.next_column()?.expect("column count matches schema");
            c.typed::<ByteArrayType>().write_batch(&vals, None, None)?;
            c.close()?;
            col += 1;
        }};
    }
    // An OPTIONAL column carries a definition level per row: 1 present, 0 null.
    // Values are written ONLY for the present rows — writing a placeholder for
    // a null is how an optional column silently becomes a column of zeros.
    macro_rules! opt_i64 {
        ($f:expr) => {{
            let mut vals: Vec<i64> = Vec::new();
            let mut defs: Vec<i16> = Vec::with_capacity(rows.len());
            for r in rows {
                match $f(r) {
                    Some(v) => {
                        vals.push(v);
                        defs.push(1);
                    }
                    None => defs.push(0),
                }
            }
            let mut c = rg.next_column()?.expect("column count matches schema");
            c.typed::<Int64Type>()
                .write_batch(&vals, Some(&defs), None)?;
            c.close()?;
            col += 1;
        }};
    }
    macro_rules! opt_bytes {
        ($f:expr) => {{
            let mut vals: Vec<ByteArray> = Vec::new();
            let mut defs: Vec<i16> = Vec::with_capacity(rows.len());
            for r in rows {
                match $f(r) {
                    Some(v) => {
                        vals.push(v);
                        defs.push(1);
                    }
                    None => defs.push(0),
                }
            }
            let mut c = rg.next_column()?.expect("column count matches schema");
            c.typed::<ByteArrayType>()
                .write_batch(&vals, Some(&defs), None)?;
            c.close()?;
            col += 1;
        }};
    }

    req_i64!(|r: &Observation| r.slot as i64);
    req_i64!(|r: &Observation| r.block_time as i64);
    req_bytes!(|r: &Observation| ByteArray::from(r.tx_hash.as_slice()));
    req_bytes!(|r: &Observation| ByteArray::from(r.address.as_bytes()));
    req_i64!(|r: &Observation| r.lovelace);
    req_bytes!(|r: &Observation| ByteArray::from(r.unit_name.as_slice()));
    req_i64!(|r: &Observation| r.unit_amount);
    opt_bytes!(|r: &Observation| r.datum.as_deref().map(ByteArray::from));
    opt_bytes!(|r: &Observation| r
        .decoded
        .as_ref()
        .map(|d| ByteArray::from(d.venue.as_bytes())));
    opt_bytes!(|r: &Observation| r
        .decoded
        .as_ref()
        .map(|d| ByteArray::from(d.key_policy.as_slice())));
    opt_bytes!(|r: &Observation| r
        .decoded
        .as_ref()
        .map(|d| ByteArray::from(d.key_name.as_slice())));
    opt_bytes!(|r: &Observation| r
        .decoded
        .as_ref()
        .map(|d| ByteArray::from(d.key_basis.as_bytes())));
    opt_i64!(|r: &Observation| r.decoded.as_ref().map(|d| d.base_reserve));
    opt_bytes!(|r: &Observation| r
        .decoded
        .as_ref()
        .and_then(|d| d.quote_policy.as_deref())
        .map(ByteArray::from));
    opt_bytes!(|r: &Observation| r
        .decoded
        .as_ref()
        .and_then(|d| d.quote_name.as_deref())
        .map(ByteArray::from));
    opt_i64!(|r: &Observation| r.decoded.as_ref().and_then(|d| d.quote_reserve));
    opt_i64!(|r: &Observation| r.decoded.as_ref().and_then(|d| d.fee_bps));
    opt_i64!(|r: &Observation| r.decoded.as_ref().and_then(|d| d.total_lp));
    opt_bytes!(|r: &Observation| r
        .decoded
        .as_ref()
        .map(|d| ByteArray::from(d.reserve_source.as_bytes())));

    debug_assert_eq!(col, 19, "every schema column must be written");
    rg.close()?;
    Ok(())
}

/// Read every observation back. Whole-file, because these are small and every
/// consumer so far wants the curve rather than a page of it.
pub fn read_all(bytes: &[u8]) -> Result<Vec<Observation>> {
    let reader = SerializedFileReader::new(bytes::Bytes::copy_from_slice(bytes))?;
    let mut out = Vec::new();
    for row in reader.get_row_iter(None)? {
        let row = row?;
        let mut o = Observation {
            slot: 0,
            block_time: 0,
            tx_hash: Vec::new(),
            address: String::new(),
            lovelace: 0,
            unit_name: Vec::new(),
            unit_amount: 0,
            datum: None,
            decoded: None,
        };
        let mut d = Decoded::default();
        let mut any_decode = false;
        for (name, field) in row.get_column_iter() {
            use parquet::record::Field;
            match (name.as_str(), field) {
                ("slot", Field::Long(v)) => o.slot = *v as u64,
                ("block_time", Field::Long(v)) => o.block_time = *v as u64,
                ("tx_hash", Field::Bytes(b)) => o.tx_hash = b.data().to_vec(),
                ("address", Field::Bytes(b)) => {
                    o.address = String::from_utf8_lossy(b.data()).into_owned()
                }
                ("lovelace", Field::Long(v)) => o.lovelace = *v,
                ("unit_name", Field::Bytes(b)) => o.unit_name = b.data().to_vec(),
                ("unit_amount", Field::Long(v)) => o.unit_amount = *v,
                ("datum", Field::Bytes(b)) => o.datum = Some(b.data().to_vec()),
                ("venue", Field::Bytes(b)) => {
                    d.venue = String::from_utf8_lossy(b.data()).into_owned();
                    any_decode = true;
                }
                ("key_policy", Field::Bytes(b)) => d.key_policy = b.data().to_vec(),
                ("key_name", Field::Bytes(b)) => d.key_name = b.data().to_vec(),
                ("key_basis", Field::Bytes(b)) => {
                    d.key_basis = String::from_utf8_lossy(b.data()).into_owned()
                }
                ("base_reserve", Field::Long(v)) => d.base_reserve = *v,
                ("quote_policy", Field::Bytes(b)) => d.quote_policy = Some(b.data().to_vec()),
                ("quote_name", Field::Bytes(b)) => d.quote_name = Some(b.data().to_vec()),
                ("quote_reserve", Field::Long(v)) => d.quote_reserve = Some(*v),
                ("fee_bps", Field::Long(v)) => d.fee_bps = Some(*v),
                ("total_lp", Field::Long(v)) => d.total_lp = Some(*v),
                ("reserve_source", Field::Bytes(b)) => {
                    d.reserve_source = String::from_utf8_lossy(b.data()).into_owned()
                }
                _ => {}
            }
        }
        // `venue` is the presence marker: it is written for every decode and
        // never for a bare candidate, so it cannot disagree with the rest.
        o.decoded = any_decode.then_some(d);
        out.push(o);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Completeness;

    fn stamp() -> Stamp {
        Stamp {
            policy_hex: "ab".repeat(28),
            completeness: Completeness::Complete,
            walk_from: Some(1),
            walk_to: Some(9),
            covered_from: 1,
            covered_to: 9,
            sealed_unix: 1_788_000_000,
        }
    }

    fn candidate(slot: u64) -> Observation {
        Observation {
            slot,
            block_time: 1_700_000_000 + slot,
            tx_hash: vec![7; 32],
            address: "addr1xcandidate".into(),
            lovelace: 2_000_000,
            unit_name: b"TOK".to_vec(),
            unit_amount: 500,
            datum: Some(vec![0xd8, 0x79, 0x80]),
            decoded: None,
        }
    }

    fn decoded(slot: u64) -> Observation {
        Observation {
            decoded: Some(Decoded {
                venue: "splash".into(),
                key_policy: vec![1; 28],
                key_name: b"nft".to_vec(),
                key_basis: "datum".into(),
                base_reserve: 1_000,
                quote_policy: Some(Vec::new()),
                quote_name: Some(Vec::new()),
                quote_reserve: Some(9_000_000),
                fee_bps: Some(30),
                total_lp: Some(12_345),
                reserve_source: "value".into(),
            }),
            ..candidate(slot)
        }
    }

    fn round_trip(rows: Vec<Observation>) -> Vec<Observation> {
        let mut buf = Vec::new();
        let mut w = ObservationWriter::new(&mut buf, &stamp()).unwrap();
        for r in rows {
            w.push(r);
        }
        w.close().unwrap();
        read_all(&buf).unwrap()
    }

    #[test]
    fn a_decoded_observation_round_trips_whole() {
        let got = round_trip(vec![decoded(5)]);
        assert_eq!(got, vec![decoded(5)]);
    }

    /// The point of the file: an output nothing recognised is KEPT, with the
    /// bytes a later decoder would need. If this ever regresses, adding a
    /// decoder means re-walking certified chunks instead of re-reading the
    /// archive.
    #[test]
    fn an_undecoded_candidate_keeps_its_datum() {
        let got = round_trip(vec![candidate(5)]);
        assert_eq!(got, vec![candidate(5)]);
        assert!(got[0].is_candidate());
        assert_eq!(got[0].datum, Some(vec![0xd8, 0x79, 0x80]));
    }

    /// Mixed nullability in one row group is where an optional column turns
    /// into a column of zeros — the definition levels and the value vector
    /// have different lengths, and getting that wrong reads back as "every row
    /// decoded, all with reserve 0".
    #[test]
    fn candidates_and_decodes_interleave_without_bleeding() {
        let rows = vec![candidate(1), decoded(2), candidate(3), decoded(4)];
        let got = round_trip(rows.clone());
        assert_eq!(got, rows);
        assert!(got[0].is_candidate() && got[2].is_candidate());
        assert_eq!(got[1].decoded.as_ref().unwrap().base_reserve, 1_000);
        assert_eq!(got[3].decoded.as_ref().unwrap().fee_bps, Some(30));
    }

    /// A pool with no datum at all is a real finding, not a missing value.
    #[test]
    fn an_output_with_no_datum_says_so() {
        let mut c = candidate(5);
        c.datum = None;
        let got = round_trip(vec![c.clone()]);
        assert_eq!(got, vec![c]);
        assert_eq!(got[0].datum, None);
    }

    #[test]
    fn an_empty_file_is_legal_and_carries_its_stamp() {
        let mut buf = Vec::new();
        let w = ObservationWriter::new(&mut buf, &stamp()).unwrap();
        let written = w.close().unwrap();
        assert_eq!(written.rows, 0);
        assert_eq!(written.min_slot, None);
        assert!(read_all(&buf).unwrap().is_empty());
    }
}
