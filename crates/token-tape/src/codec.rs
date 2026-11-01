//! The byte-level reader and writer every file here shares.

use crate::FORMAT;

/// Why a file was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Not this kind of file: the magic bytes differ.
    NotThisFile,
    /// A format this reader does not know.
    Format(u16),
    /// It ends before it should.
    Short,
    /// Something in it cannot be right.
    Invalid(&'static str),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotThisFile => write!(f, "not this kind of token-tape file"),
            Error::Format(n) => write!(f, "token-tape format {n}; this reader knows {FORMAT}"),
            Error::Short => write!(f, "token-tape file ends early"),
            Error::Invalid(why) => write!(f, "token-tape file invalid: {why}"),
        }
    }
}

impl std::error::Error for Error {}

pub(crate) struct Writer {
    out: Vec<u8>,
}

impl Writer {
    pub(crate) fn new(magic: &[u8; 4]) -> Self {
        let mut out = Vec::with_capacity(1024);
        out.extend_from_slice(magic);
        out.extend_from_slice(&FORMAT.to_le_bytes());
        Self { out }
    }

    pub(crate) fn byte(&mut self, b: u8) {
        self.out.push(b);
    }

    pub(crate) fn raw(&mut self, b: &[u8]) {
        self.out.extend_from_slice(b);
    }

    pub(crate) fn uint(&mut self, mut n: u64) {
        loop {
            let low = (n & 0x7f) as u8;
            n >>= 7;
            if n == 0 {
                self.out.push(low);
                return;
            }
            self.out.push(low | 0x80);
        }
    }

    pub(crate) fn bytes(&mut self, b: &[u8]) {
        self.uint(b.len() as u64);
        self.raw(b);
    }

    pub(crate) fn finish(self) -> Vec<u8> {
        self.out
    }
}

pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    /// Past the magic and the format, or why not.
    pub(crate) fn open(bytes: &'a [u8], magic: &[u8; 4]) -> Result<Self, Error> {
        let mut r = Self { bytes, at: 0 };
        if r.take(4)? != magic {
            return Err(Error::NotThisFile);
        }
        let format = u16::from_le_bytes([r.byte()?, r.byte()?]);
        if format != FORMAT {
            return Err(Error::Format(format));
        }
        Ok(r)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let end = self
            .at
            .checked_add(n)
            .filter(|e| *e <= self.bytes.len())
            .ok_or(Error::Short)?;
        let out = &self.bytes[self.at..end];
        self.at = end;
        Ok(out)
    }

    /// The next `n` bytes as they are.
    pub(crate) fn take_raw(&mut self, n: usize) -> Result<&'a [u8], Error> {
        self.take(n)
    }

    pub(crate) fn byte(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn uint(&mut self) -> Result<u64, Error> {
        let mut n: u64 = 0;
        for shift in (0..64).step_by(7) {
            let b = self.byte()?;
            n |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Ok(n);
            }
        }
        Err(Error::Invalid("a varint past 64 bits"))
    }

    pub(crate) fn u32(&mut self) -> Result<u32, Error> {
        u32::try_from(self.uint()?).map_err(|_| Error::Invalid("a number past 32 bits"))
    }

    /// A count, bounded by what is left: no count can promise more entries
    /// than there are bytes.
    pub(crate) fn len(&mut self) -> Result<usize, Error> {
        let n = self.uint()?;
        match usize::try_from(n) {
            Ok(n) if n <= self.bytes.len() - self.at => Ok(n),
            _ => Err(Error::Invalid("a count past the end of the file")),
        }
    }

    pub(crate) fn bytes(&mut self) -> Result<&'a [u8], Error> {
        let n = self.len()?;
        self.take(n)
    }

    pub(crate) fn string(&mut self) -> Result<String, Error> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| Error::Invalid("a string not UTF-8"))
    }

    /// Nothing may follow the last field.
    pub(crate) fn end(&self) -> Result<(), Error> {
        match self.at == self.bytes.len() {
            true => Ok(()),
            false => Err(Error::Invalid("bytes after the end")),
        }
    }
}
