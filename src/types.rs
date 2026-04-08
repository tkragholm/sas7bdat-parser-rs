use std::fmt;
use std::ops::{Add, AddAssign, Sub, SubAssign};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct PageIndex(pub u64);

impl fmt::Display for PageIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for PageIndex {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<PageIndex> for u64 {
    fn from(v: PageIndex) -> Self {
        v.0
    }
}

impl PageIndex {
    #[must_use]
    pub const fn saturating_sub(self, rhs: u64) -> Self {
        Self(self.0.saturating_sub(rhs))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct RowIndex(pub u64);

impl fmt::Display for RowIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for RowIndex {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<RowIndex> for u64 {
    fn from(v: RowIndex) -> Self {
        v.0
    }
}

impl Add<u64> for RowIndex {
    type Output = Self;
    fn add(self, rhs: u64) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl AddAssign<u64> for RowIndex {
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
    }
}

impl Sub<u64> for RowIndex {
    type Output = Self;
    fn sub(self, rhs: u64) -> Self::Output {
        Self(self.0 - rhs)
    }
}

impl SubAssign<u64> for RowIndex {
    fn sub_assign(&mut self, rhs: u64) {
        self.0 -= rhs;
    }
}

impl RowIndex {
    #[must_use]
    pub const fn saturating_sub(self, rhs: u64) -> Self {
        Self(self.0.saturating_sub(rhs))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct ColumnIndex(pub usize);

impl fmt::Display for ColumnIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<usize> for ColumnIndex {
    fn from(v: usize) -> Self {
        Self(v)
    }
}

impl From<ColumnIndex> for usize {
    fn from(v: ColumnIndex) -> Self {
        v.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct RowLength(pub u32);

impl fmt::Display for RowLength {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for RowLength {
    fn from(v: u32) -> Self {
        Self(v)
    }
}

impl From<RowLength> for u32 {
    fn from(v: RowLength) -> Self {
        v.0
    }
}

impl From<RowLength> for u64 {
    fn from(v: RowLength) -> Self {
        Self::from(v.0)
    }
}

impl From<RowLength> for usize {
    fn from(v: RowLength) -> Self {
        v.0 as Self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct PageSize(pub u32);

impl fmt::Display for PageSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for PageSize {
    fn from(v: u32) -> Self {
        Self(v)
    }
}

impl From<PageSize> for u32 {
    fn from(v: PageSize) -> Self {
        v.0
    }
}

impl From<PageSize> for u64 {
    fn from(v: PageSize) -> Self {
        Self::from(v.0)
    }
}

impl From<PageSize> for usize {
    fn from(v: PageSize) -> Self {
        v.0 as Self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct ByteOffset(pub u32);

impl fmt::Display for ByteOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for ByteOffset {
    fn from(v: u32) -> Self {
        Self(v)
    }
}

impl From<ByteOffset> for u32 {
    fn from(v: ByteOffset) -> Self {
        v.0
    }
}

impl From<ByteOffset> for usize {
    fn from(v: ByteOffset) -> Self {
        v.0 as Self
    }
}

impl Add<u32> for ByteOffset {
    type Output = Self;
    fn add(self, rhs: u32) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl Sub<u32> for ByteOffset {
    type Output = Self;
    fn sub(self, rhs: u32) -> Self::Output {
        Self(self.0 - rhs)
    }
}

/// A safe wrapper around raw page bytes.
pub struct PageSlice<'a> {
    pub bytes: &'a [u8],
}

impl<'a> PageSlice<'a> {
    #[must_use]
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    #[must_use]
    pub fn get_u16(&self, offset: ByteOffset, endianness: crate::metadata::Endianness) -> Option<u16> {
        let start = usize::from(offset);
        let end = start + 2;
        let bytes = self.bytes.get(start..end)?;
        Some(match endianness {
            crate::metadata::Endianness::Little => u16::from_le_bytes([bytes[0], bytes[1]]),
            crate::metadata::Endianness::Big => u16::from_be_bytes([bytes[0], bytes[1]]),
        })
    }

    #[must_use]
    pub fn get_u32(&self, offset: ByteOffset, endianness: crate::metadata::Endianness) -> Option<u32> {
        let start = usize::from(offset);
        let end = start + 4;
        let bytes = self.bytes.get(start..end)?;
        let mut buf = [0u8; 4];
        buf.copy_from_slice(bytes);
        Some(match endianness {
            crate::metadata::Endianness::Little => u32::from_le_bytes(buf),
            crate::metadata::Endianness::Big => u32::from_be_bytes(buf),
        })
    }
}

/// A safe wrapper around raw row bytes.
pub struct RowSlice<'a> {
    pub bytes: &'a [u8],
}

impl<'a> RowSlice<'a> {
    #[must_use]
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    #[must_use]
    pub fn column_slice(&self, offset: ByteOffset, width: u32) -> Option<&'a [u8]> {
        let start = usize::from(offset);
        let end = start + width as usize;
        self.bytes.get(start..end)
    }
}
