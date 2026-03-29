use crate::metadata::{SasDate, SasDateTime, SasTime};

pub type BitSlice<'a> = &'a [u8];

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

#[derive(Debug, Clone)]
pub enum OwnedColumnBuffer {
    I32 {
        values: Vec<i32>,
        valid: Option<Vec<u8>>,
    },
    I64 {
        values: Vec<i64>,
        valid: Option<Vec<u8>>,
    },
    F64 {
        values: Vec<f64>,
        valid: Option<Vec<u8>>,
    },
    Date {
        values: Vec<SasDate>,
        valid: Option<Vec<u8>>,
    },
    DateTime {
        values: Vec<SasDateTime>,
        valid: Option<Vec<u8>>,
    },
    Time {
        values: Vec<SasTime>,
        valid: Option<Vec<u8>>,
    },
    Utf8 {
        offsets: Vec<u32>,
        data: Vec<u8>,
        valid: Option<Vec<u8>>,
    },
    RawBytes {
        offsets: Vec<u32>,
        data: Vec<u8>,
        valid: Option<Vec<u8>>,
    },
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
            } => ColumnBuffer::Utf8(Utf8Buffer {
                offsets,
                data,
                valid: valid.as_deref(),
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
    pub row_base: u64,
    pub row_count: usize,
    pub columns: &'a [ColumnBuffer<'a>],
}

#[derive(Debug, Clone, Default)]
pub struct OwnedColumnarBatch {
    pub row_base: u64,
    pub row_count: usize,
    pub columns: Vec<OwnedColumnBuffer>,
}
