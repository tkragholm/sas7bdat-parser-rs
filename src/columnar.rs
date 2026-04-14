#[cfg(feature = "arrow")]
use crate::error::{Error, Result};
use crate::metadata::{SasDate, SasDateTime, SasTime};
#[cfg(feature = "arrow")]
use arrow_array::{
    ArrayRef, RecordBatch,
    builder::{BinaryBuilder, PrimitiveBuilder, StringBuilder},
    types::{
        ArrowPrimitiveType, Date32Type, Float64Type, Int32Type, Int64Type, Time32SecondType,
        TimestampSecondType,
    },
};
#[cfg(feature = "arrow")]
use arrow_schema::SchemaRef;
#[cfg(feature = "arrow")]
use std::sync::Arc;
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

    #[cfg(feature = "arrow")]
    pub fn into_arrow_array(self) -> Result<ArrayRef> {
        match self {
            Self::I32 { values, valid } => build_primitive_array::<Int32Type>(values, valid),
            Self::I64 { values, valid } => build_primitive_array::<Int64Type>(values, valid),
            Self::F64 { values, valid } => build_primitive_array::<Float64Type>(values, valid),
            Self::Date { values, valid } => build_primitive_array::<Date32Type>(
                values
                    .into_iter()
                    .map(|value| value.days_since_sas_epoch)
                    .collect(),
                valid,
            ),
            Self::DateTime { values, valid } => build_primitive_array::<TimestampSecondType>(
                values
                    .into_iter()
                    .map(|value| value.seconds_since_sas_epoch)
                    .collect(),
                valid,
            ),
            Self::Time { values, valid } => build_primitive_array::<Time32SecondType>(
                values
                    .into_iter()
                    .map(|value| {
                        i32::try_from(value.seconds_since_midnight)
                            .map_err(|_| Error::arrow("SAS time value exceeds Arrow Time32 range"))
                    })
                    .collect::<Result<Vec<_>>>()?,
                valid,
            ),
            Self::Utf8 {
                offsets,
                data,
                valid,
                dictionary_ids: _,
            } => build_utf8_array(offsets, data, valid),
            Self::RawBytes {
                offsets,
                data,
                valid,
            } => build_binary_array(offsets, data, valid),
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

    #[cfg(feature = "arrow")]
    pub fn into_arrow_record_batch(self, schema: SchemaRef) -> Result<RecordBatch> {
        let arrays = self
            .columns
            .into_iter()
            .map(OwnedColumnBuffer::into_arrow_array)
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(schema, arrays).map_err(|err| Error::arrow(err.to_string()))
    }
}

#[cfg(feature = "arrow")]
fn build_primitive_array<T>(values: Vec<T::Native>, valid: Option<Vec<u64>>) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: Copy,
{
    let mut builder = PrimitiveBuilder::<T>::new();
    for (idx, value) in values.into_iter().enumerate() {
        if row_is_valid(valid.as_deref(), idx) {
            builder.append_value(value);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(feature = "arrow")]
fn build_utf8_array(offsets: Vec<u32>, data: Vec<u8>, valid: Option<Vec<u64>>) -> Result<ArrayRef> {
    let mut builder = StringBuilder::new();
    let row_count = offsets.len().saturating_sub(1);
    for idx in 0..row_count {
        if row_is_valid(valid.as_deref(), idx) {
            let start = usize::try_from(offsets[idx])
                .map_err(|_| Error::arrow("utf8 offset exceeds platform usize"))?;
            let end = usize::try_from(offsets[idx + 1])
                .map_err(|_| Error::arrow("utf8 offset exceeds platform usize"))?;
            let slice = data
                .get(start..end)
                .ok_or_else(|| Error::arrow("utf8 slice exceeds buffer bounds"))?;
            let value = std::str::from_utf8(slice).map_err(|err| Error::arrow(err.to_string()))?;
            builder.append_value(value);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(feature = "arrow")]
fn build_binary_array(
    offsets: Vec<u32>,
    data: Vec<u8>,
    valid: Option<Vec<u64>>,
) -> Result<ArrayRef> {
    let mut builder = BinaryBuilder::new();
    let row_count = offsets.len().saturating_sub(1);
    for idx in 0..row_count {
        if row_is_valid(valid.as_deref(), idx) {
            let start = usize::try_from(offsets[idx])
                .map_err(|_| Error::arrow("binary offset exceeds platform usize"))?;
            let end = usize::try_from(offsets[idx + 1])
                .map_err(|_| Error::arrow("binary offset exceeds platform usize"))?;
            let slice = data
                .get(start..end)
                .ok_or_else(|| Error::arrow("binary slice exceeds buffer bounds"))?;
            builder.append_value(slice);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(feature = "arrow")]
fn row_is_valid(valid: Option<&[u64]>, idx: usize) -> bool {
    match valid {
        None => true,
        Some(words) => {
            let word = idx / 64;
            let bit = idx % 64;
            words
                .get(word)
                .is_some_and(|bits| (bits & (1u64 << bit)) != 0)
        }
    }
}
