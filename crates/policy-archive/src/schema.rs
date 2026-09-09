//! The on-disk contract: columns, and the stamp in the footer.

use parquet::format::KeyValue;

use crate::{Error, Result};

/// Bumped when a reader of the previous version could misread a file of this
/// one. Additive changes (a new footer key, a new trailing column) do not bump
/// it; a renamed column or a changed row-group rule does.
pub const FORMAT_VERSION: u32 = 1;

/// Spelled out rather than derived from a struct so the column names are a
/// deliberate contract with every reader, not a by-product of field order.
///
/// `net_mint` is the transaction's net mint OF THIS ROW'S UNIT — the same
/// figure the feed's `UnitMove.net_mint` carries — so direction can be derived
/// from the archive alone with the same rule the live feed uses. A mint whose
/// unit reached no party we could attribute (a burn from below the walk floor
/// is the common case) is still a transaction the ledger knows, and it is kept
/// as a row with an empty address and a zero amount rather than dropped, so
/// the archive holds every transaction the ledger does.
pub const MESSAGE_TYPE: &str = "
    message movement {
        REQUIRED INT64 slot;
        REQUIRED INT64 block_time;
        REQUIRED BYTE_ARRAY tx_hash;
        REQUIRED BYTE_ARRAY unit_name;
        REQUIRED BYTE_ARRAY address;
        REQUIRED INT64 amount;
        REQUIRED INT64 net_mint;
    }
";

/// The columns, in file order. `index()` is what a reader addresses a column
/// chunk by; `name()` is what the schema says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Column {
    Slot,
    BlockTime,
    TxHash,
    UnitName,
    Address,
    Amount,
    NetMint,
}

impl Column {
    pub const ALL: [Column; 7] = [
        Column::Slot,
        Column::BlockTime,
        Column::TxHash,
        Column::UnitName,
        Column::Address,
        Column::Amount,
        Column::NetMint,
    ];

    pub fn index(self) -> usize {
        match self {
            Column::Slot => 0,
            Column::BlockTime => 1,
            Column::TxHash => 2,
            Column::UnitName => 3,
            Column::Address => 4,
            Column::Amount => 5,
            Column::NetMint => 6,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Column::Slot => "slot",
            Column::BlockTime => "block_time",
            Column::TxHash => "tx_hash",
            Column::UnitName => "unit_name",
            Column::Address => "address",
            Column::Amount => "amount",
            Column::NetMint => "net_mint",
        }
    }
}

/// One row: a party's signed movement of one unit in one transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Movement {
    pub slot: u64,
    /// Unix seconds of the containing block. Row groups align on this.
    pub block_time: u64,
    pub tx_hash: Vec<u8>,
    /// On-chain asset-name bytes.
    pub unit_name: Vec<u8>,
    /// Bech32 address. Empty for an unattributed mint/burn placeholder.
    pub address: String,
    pub amount: i64,
    /// This transaction's net mint of this unit: 0 transfer, + mint, − burn.
    pub net_mint: i64,
}

impl Movement {
    /// A row standing in for a `(transaction, unit)` the ledger knows about but
    /// attributed to nobody — see [`MESSAGE_TYPE`].
    pub fn is_placeholder(&self) -> bool {
        self.address.is_empty() && self.amount == 0
    }
}

/// How far the walk that produced a file reached, in the three words the feed
/// already uses. Spelled identically to token-ledger's `store::Completeness`
/// and `shared_types::policy_feed::Completeness`; a fourth spelling would be a
/// reader that silently reads every archive as unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completeness {
    Complete,
    Partial,
    Unrecorded,
}

impl Completeness {
    pub const ALL: [Completeness; 3] = [
        Completeness::Complete,
        Completeness::Partial,
        Completeness::Unrecorded,
    ];

    pub fn as_wire(self) -> &'static str {
        match self {
            Completeness::Complete => "complete",
            Completeness::Partial => "partial",
            Completeness::Unrecorded => "unrecorded",
        }
    }

    pub fn from_wire(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.as_wire() == s)
    }
}

/// What a file is, written into its own footer.
///
/// Two ranges, and they are different claims:
///
/// - `covered_from..=covered_to` is what THIS FILE covers — the intersection
///   of the ledger's coverage with the partition's calendar range. A month the
///   walk only reached halfway into is honestly a file covering half a month,
///   not a refusal and not a month quietly missing its tail.
/// - `walk_from..=walk_to` is what the LEDGER covered when the file was
///   written. A reverse walk corrects rows above its floor as it deepens, so a
///   reader comparing two files of the same partition knows which was written
///   from the deeper walk, and a sealer knows a file is stale the moment the
///   ledger's coverage differs from the one stamped here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stamp {
    pub policy_hex: String,
    pub completeness: Completeness,
    /// Ledger floor at seal time, as a slot. `None` when the ledger had not
    /// recorded one.
    pub walk_from: Option<u64>,
    /// Ledger ceiling at seal time, as a slot.
    pub walk_to: Option<u64>,
    /// This file's covered range, inclusive, as slots.
    pub covered_from: u64,
    pub covered_to: u64,
    pub sealed_unix: u64,
}

/// Footer key-value keys. Namespaced so a file that passed through another
/// tool keeps ours distinguishable from whatever it added.
pub mod kv {
    pub const FORMAT: &str = "policy_archive.format";
    pub const POLICY: &str = "policy_archive.policy";
    pub const COMPLETENESS: &str = "policy_archive.completeness";
    pub const WALK_FROM: &str = "policy_archive.walk_from";
    pub const WALK_TO: &str = "policy_archive.walk_to";
    pub const COVERED_FROM: &str = "policy_archive.covered_from";
    pub const COVERED_TO: &str = "policy_archive.covered_to";
    pub const SEALED_UNIX: &str = "policy_archive.sealed_unix";
    /// Histogram resolution and row-group alignment, seconds.
    pub const BUCKET_SECS: &str = "policy_archive.bucket_secs";
    /// Side tables — comma-separated decimals. One entry per row group:
    pub const GROUP_BUCKETS: &str = "policy_archive.groups.buckets";
    /// …and one entry per bucket, flat across groups in order. See
    /// `groups::GroupSummary::encode`.
    pub const BUCKET_FROM: &str = "policy_archive.buckets.from";
    pub const BUCKET_ROWS: &str = "policy_archive.buckets.rows";
    pub const BUCKET_PLACEHOLDERS: &str = "policy_archive.buckets.placeholders";
    pub const BUCKET_TXS: &str = "policy_archive.buckets.txs";
    pub const BUCKET_MINTS: &str = "policy_archive.buckets.mints";
    pub const BUCKET_BURNS: &str = "policy_archive.buckets.burns";
}

impl Stamp {
    pub fn to_key_values(&self) -> Vec<KeyValue> {
        let mut out = vec![
            KeyValue::new(kv::FORMAT.to_string(), FORMAT_VERSION.to_string()),
            KeyValue::new(kv::POLICY.to_string(), self.policy_hex.clone()),
            KeyValue::new(
                kv::COMPLETENESS.to_string(),
                self.completeness.as_wire().to_string(),
            ),
            KeyValue::new(kv::COVERED_FROM.to_string(), self.covered_from.to_string()),
            KeyValue::new(kv::COVERED_TO.to_string(), self.covered_to.to_string()),
            KeyValue::new(kv::SEALED_UNIX.to_string(), self.sealed_unix.to_string()),
        ];
        // Absent rather than a sentinel: `None` means the ledger never recorded
        // a bound, and `0` would read as genesis.
        if let Some(from) = self.walk_from {
            out.push(KeyValue::new(kv::WALK_FROM.to_string(), from.to_string()));
        }
        if let Some(to) = self.walk_to {
            out.push(KeyValue::new(kv::WALK_TO.to_string(), to.to_string()));
        }
        out
    }

    pub fn from_key_values(kvs: &[KeyValue]) -> Result<Self> {
        let format: u32 = required(kvs, kv::FORMAT)?;
        if format > FORMAT_VERSION {
            return Err(Error::BadStamp {
                key: kv::FORMAT,
                detail: format!("version {format} is newer than this reader ({FORMAT_VERSION})"),
            });
        }
        let completeness_word: String = required(kvs, kv::COMPLETENESS)?;
        let completeness =
            Completeness::from_wire(&completeness_word).ok_or_else(|| Error::BadStamp {
                key: kv::COMPLETENESS,
                detail: format!("unknown word `{completeness_word}`"),
            })?;
        Ok(Stamp {
            policy_hex: required(kvs, kv::POLICY)?,
            completeness,
            walk_from: optional(kvs, kv::WALK_FROM)?,
            walk_to: optional(kvs, kv::WALK_TO)?,
            covered_from: required(kvs, kv::COVERED_FROM)?,
            covered_to: required(kvs, kv::COVERED_TO)?,
            sealed_unix: required(kvs, kv::SEALED_UNIX)?,
        })
    }
}

/// One footer entry by key.
pub fn lookup<'a>(kvs: &'a [KeyValue], key: &str) -> Option<&'a str> {
    kvs.iter()
        .find(|kv| kv.key == key)
        .and_then(|kv| kv.value.as_deref())
}

pub(crate) fn required<T: std::str::FromStr>(kvs: &[KeyValue], key: &'static str) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    let raw = lookup(kvs, key).ok_or(Error::MissingStamp(key))?;
    raw.parse::<T>().map_err(|e| Error::BadStamp {
        key,
        detail: e.to_string(),
    })
}

pub(crate) fn optional<T: std::str::FromStr>(
    kvs: &[KeyValue],
    key: &'static str,
) -> Result<Option<T>>
where
    T::Err: std::fmt::Display,
{
    match lookup(kvs, key) {
        None => Ok(None),
        Some(raw) => raw.parse::<T>().map(Some).map_err(|e| Error::BadStamp {
            key,
            detail: e.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::schema::parser::parse_message_type;

    /// The column names are a contract with every reader. A typo here is a
    /// file nobody can address by name, and the enum has to agree with the
    /// schema string or `index()` reads the wrong column.
    #[test]
    fn the_schema_parses_and_the_enum_matches_it() {
        let schema = parse_message_type(MESSAGE_TYPE).expect("schema parses");
        let names: Vec<&str> = schema.get_fields().iter().map(|f| f.name()).collect();
        let expected: Vec<&str> = Column::ALL.iter().map(|c| c.name()).collect();
        assert_eq!(names, expected);
        for (i, c) in Column::ALL.iter().enumerate() {
            assert_eq!(c.index(), i, "{c:?}");
        }
    }

    fn stamp() -> Stamp {
        Stamp {
            policy_hex: "ab".repeat(28),
            completeness: Completeness::Partial,
            walk_from: Some(190_000_000),
            walk_to: Some(196_000_000),
            covered_from: 191_000_000,
            covered_to: 193_000_000,
            sealed_unix: 1_788_000_000,
        }
    }

    #[test]
    fn the_stamp_round_trips_through_the_footer() {
        let s = stamp();
        assert_eq!(Stamp::from_key_values(&s.to_key_values()).unwrap(), s);
    }

    /// `None` is the ABSENCE of a key, never a zero — zero is genesis.
    #[test]
    fn an_unrecorded_bound_is_absent_not_zero() {
        let mut s = stamp();
        s.walk_from = None;
        let kvs = s.to_key_values();
        assert!(lookup(&kvs, kv::WALK_FROM).is_none());
        assert_eq!(Stamp::from_key_values(&kvs).unwrap().walk_from, None);
    }

    /// Every completeness has exactly one spelling and it is the feed's.
    #[test]
    fn completeness_spellings_are_the_feeds() {
        let words: Vec<&str> = Completeness::ALL.iter().map(|c| c.as_wire()).collect();
        assert_eq!(words, ["complete", "partial", "unrecorded"]);
        for c in Completeness::ALL {
            assert_eq!(Completeness::from_wire(c.as_wire()), Some(c));
        }
    }

    /// A file from a NEWER format is refused rather than misread.
    #[test]
    fn a_newer_format_is_refused() {
        let mut kvs = stamp().to_key_values();
        for kv in &mut kvs {
            if kv.key == kv::FORMAT {
                kv.value = Some((FORMAT_VERSION + 1).to_string());
            }
        }
        assert!(matches!(
            Stamp::from_key_values(&kvs),
            Err(Error::BadStamp { key, .. }) if key == kv::FORMAT
        ));
    }

    #[test]
    fn a_file_without_a_stamp_says_so() {
        assert!(matches!(
            Stamp::from_key_values(&[]),
            Err(Error::MissingStamp(kv::FORMAT))
        ));
    }
}
