use std::borrow::Cow;

use time::{Duration, OffsetDateTime};

use crate::metadata::{MissingLiteral, TaggedMissing};

/// Represents a single cell value produced by the SAS reader.
#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
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

impl Value<'_> {
    #[must_use]
    pub fn into_owned(self) -> Value<'static> {
        match self {
            Value::Float(v) => Value::Float(v),
            Value::Int32(v) => Value::Int32(v),
            Value::Int64(v) => Value::Int64(v),
            Value::NumericString(s) => Value::NumericString(Cow::Owned(s.into_owned())),
            Value::Str(s) => Value::Str(Cow::Owned(s.into_owned())),
            Value::Bytes(bytes) => Value::Bytes(Cow::Owned(bytes.into_owned())),
            Value::DateTime(dt) => Value::DateTime(dt),
            Value::Date(dt) => Value::Date(dt),
            Value::Time(duration) => Value::Time(duration),
            Value::Missing(missing) => Value::Missing(missing),
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
