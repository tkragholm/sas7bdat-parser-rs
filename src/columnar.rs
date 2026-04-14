use crate::metadata::{SasDate, SasDateTime, SasTime};
pub const BLANK_ID: u32 = 0;

/// Bit-packed validity slice: each `u64` word holds 64 row-validity bits (LSB = first row).
/// Bit `i % 64` of word `i / 64` is 1 if row `i` is valid, 0 if null.
/// Unused bits in the last word (when row count is not a multiple of 64) are 0.
pub type BitSlice<'a> = &'a [u64];

#[derive(Debug, Clone, Copy)]
pub struct PrimitiveBuffer<'a, T> {
    pub values: &'a [T],
    pub valid: Option<BitSlice<'a>>,
}

#[derive(Debug, Clone, Copy)]
pub struct Utf8Dictionary<'a> {
    pub values: &'a [&'a str],
}

#[derive(Debug, Clone, Copy)]
pub struct Utf8Buffer<'a> {
    pub offsets: &'a [u32],
    pub data: &'a [u8],
    pub valid: Option<BitSlice<'a>>,
    pub dictionary_ids: Option<&'a [u32]>,
    pub dictionary: Option<&'a Utf8Dictionary<'a>>,
}

#[derive(Debug, Clone, Copy)]
pub struct BytesBuffer<'a> {
    pub offsets: &'a [u32],
    pub data: &'a [u8],
    pub valid: Option<BitSlice<'a>>,
}

#[derive(Debug, Clone, Copy)]
pub enum ColumnBuffer<'a> {
    I32(PrimitiveBuffer<'a, i32>),
    I64(PrimitiveBuffer<'a, i64>),
    F64(PrimitiveBuffer<'a, f64>),
    Date(PrimitiveBuffer<'a, SasDate>),
    DateTime(PrimitiveBuffer<'a, SasDateTime>),
    Time(PrimitiveBuffer<'a, SasTime>),
    Utf8(Utf8Buffer<'a>),
    RawBytes(BytesBuffer<'a>),
}

#[macro_export]
macro_rules! define_owned_column_enum {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $($extra_variant:ident { $($extra_field:ident : $extra_type:ty),* $(,)? }),* $(,)?
        }
    ) => {
        $(#[$meta])*
        $vis enum $name {
            I32 {
                values: Vec<i32>,
                valid: Option<Vec<u64>>,
            },
            I64 {
                values: Vec<i64>,
                valid: Option<Vec<u64>>,
            },
            F64 {
                values: Vec<f64>,
                valid: Option<Vec<u64>>,
            },
            Date {
                values: Vec<$crate::metadata::SasDate>,
                valid: Option<Vec<u64>>,
            },
            DateTime {
                values: Vec<$crate::metadata::SasDateTime>,
                valid: Option<Vec<u64>>,
            },
            Time {
                values: Vec<$crate::metadata::SasTime>,
                valid: Option<Vec<u64>>,
            },
            Utf8 {
                offsets: Vec<u32>,
                data: Vec<u8>,
                valid: Option<Vec<u64>>,
                dictionary_ids: Option<Vec<u32>>,
            },
            RawBytes {
                offsets: Vec<u32>,
                data: Vec<u8>,
                valid: Option<Vec<u64>>,
            },
            $($extra_variant { $($extra_field : $extra_type),* }),*
        }
    };
}

define_owned_column_enum! {
    #[derive(Debug, Clone)]
    pub enum OwnedColumnBuffer {}
}

impl OwnedColumnBuffer {
    #[must_use]
    pub fn as_borrowed(&self) -> ColumnBuffer<'_> {
        match self {
            Self::I32 { values, valid } => ColumnBuffer::I32(PrimitiveBuffer {
                values,
                valid: valid.as_deref(),
            }),
            Self::I64 { values, valid } => ColumnBuffer::I64(PrimitiveBuffer {
                values,
                valid: valid.as_deref(),
            }),
            Self::F64 { values, valid } => ColumnBuffer::F64(PrimitiveBuffer {
                values,
                valid: valid.as_deref(),
            }),
            Self::Date { values, valid } => ColumnBuffer::Date(PrimitiveBuffer {
                values,
                valid: valid.as_deref(),
            }),
            Self::DateTime { values, valid } => ColumnBuffer::DateTime(PrimitiveBuffer {
                values,
                valid: valid.as_deref(),
            }),
            Self::Time { values, valid } => ColumnBuffer::Time(PrimitiveBuffer {
                values,
                valid: valid.as_deref(),
            }),
            Self::Utf8 {
                offsets,
                data,
                valid,
                dictionary_ids,
            } => ColumnBuffer::Utf8(Utf8Buffer {
                offsets,
                data,
                valid: valid.as_deref(),
                dictionary_ids: dictionary_ids.as_deref(),
                dictionary: None,
            }),
            Self::RawBytes {
                offsets,
                data,
                valid,
            } => ColumnBuffer::RawBytes(BytesBuffer {
                offsets,
                data,
                valid: valid.as_deref(),
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnarBatch<'a> {
    pub row_base: crate::types::RowIndex,
    pub row_count: usize,
    pub columns: &'a [ColumnBuffer<'a>],
}

#[derive(Debug, Clone, Default)]
pub struct OwnedColumnarBatch {
    pub row_base: crate::types::RowIndex,
    pub row_count: usize,
    pub columns: Vec<OwnedColumnBuffer>,
}

impl OwnedColumnarBatch {
    #[must_use]
    pub fn borrowed_columns(&self) -> Vec<ColumnBuffer<'_>> {
        self.columns
            .iter()
            .map(OwnedColumnBuffer::as_borrowed)
            .collect()
    }
}
