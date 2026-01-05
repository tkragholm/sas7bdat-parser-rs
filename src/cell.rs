use std::borrow::Cow;

use time::{Duration, OffsetDateTime};

use crate::dataset::{MissingLiteral, TaggedMissing};

/// Represents a single cell value produced by the SAS reader.
#[derive(Debug, Clone, PartialEq)]
pub enum CellValue<'a> {
    /// 64-bit floating point number.
    Float(f64),
    /// 32-bit signed integer.
    Int32(i32),
    /// 64-bit signed integer.
    Int64(i64),
    /// Arbitrary precision numeric represented as string to preserve formatting.
    NumericString(Cow<'a, str>),
    /// UTF-8 string converted from the source encoding.
    Str(Cow<'a, str>),
    /// Raw bytes when decoding into UTF-8 is deferred.
    Bytes(Cow<'a, [u8]>),
    /// SAS datetime value mapped to `OffsetDateTime`.
    DateTime(OffsetDateTime),
    /// SAS date value mapped to midnight UTC.
    Date(OffsetDateTime),
    /// SAS time value represented as duration since midnight.
    Time(Duration),
    /// Missing value with additional context.
    Missing(MissingValue),
}

impl CellValue<'_> {
    #[must_use]
    pub fn into_owned(self) -> CellValue<'static> {
        match self {
            CellValue::Float(v) => CellValue::Float(v),
            CellValue::Int32(v) => CellValue::Int32(v),
            CellValue::Int64(v) => CellValue::Int64(v),
            CellValue::NumericString(s) => CellValue::NumericString(Cow::Owned(s.into_owned())),
            CellValue::Str(s) => CellValue::Str(Cow::Owned(s.into_owned())),
            CellValue::Bytes(bytes) => CellValue::Bytes(Cow::Owned(bytes.into_owned())),
            CellValue::DateTime(dt) => CellValue::DateTime(dt),
            CellValue::Date(dt) => CellValue::Date(dt),
            CellValue::Time(duration) => CellValue::Time(duration),
            CellValue::Missing(missing) => CellValue::Missing(missing),
        }
    }
}

/// Variants of missing values encountered in SAS datasets.
#[derive(Debug, Clone, PartialEq)]
pub enum MissingValue {
    /// System missing represented by `.` in SAS.
    System,
    /// Missing value tagged with a letter (`.A`-`.Z`) or other sentinel.
    Tagged(TaggedMissing),
    /// Missing due to explicit range definition.
    Range {
        lower: MissingLiteral,
        upper: MissingLiteral,
    },
}

impl MissingValue {
    #[must_use]
    pub const fn system() -> Self {
        Self::System
    }
}
