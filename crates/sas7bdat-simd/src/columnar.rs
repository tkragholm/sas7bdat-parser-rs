use crate::error::Error;
#[cfg(feature = "arrow")]
use crate::error::Result;
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
#[cfg_attr(not(feature = "arrow"), allow(dead_code))]
pub type BitSlice<'a> = &'a [u64];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustedOffsets {
    offsets: Vec<i64>,
}

impl Default for TrustedOffsets {
    fn default() -> Self {
        Self::with_capacity_for_rows(0)
    }
}

impl TrustedOffsets {
    #[must_use]
    pub fn with_capacity_for_rows(target_rows: usize) -> Self {
        let mut offsets = Vec::with_capacity(target_rows.saturating_add(1));
        offsets.push(0);
        Self { offsets }
    }

    #[must_use]
    pub fn as_slice(&self) -> &[i64] {
        &self.offsets
    }

    pub fn clear_for_reuse(&mut self) {
        self.offsets.clear();
        self.offsets.push(0);
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.offsets.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Validates that the offsets start at zero, never decrease, and end at `data_len`.
    ///
    /// # Errors
    ///
    /// Returns an error if the offsets are missing the required initial zero offset, decrease at
    /// any point, end at a different value than `data_len`, or if `data_len` exceeds the `i64`
    /// range required by Arrow-style large offsets.
    pub fn validate_for_values_len(&self, data_len: usize) -> crate::error::Result<()> {
        let expected_end = i64::try_from(data_len)
            .map_err(|_| Error::unsupported("columnar variable buffer exceeds i64 offset range"))?;
        let Some(&first) = self.offsets.first() else {
            return Err(Error::unsupported(
                "trusted offsets must contain the initial zero offset",
            ));
        };
        if first != 0 {
            return Err(Error::unsupported("trusted offsets must start at zero"));
        }

        let mut previous = first;
        for &offset in self.offsets.iter().skip(1) {
            if offset < previous {
                return Err(Error::unsupported(
                    "trusted offsets must be monotonically non-decreasing",
                ));
            }
            previous = offset;
        }

        if previous != expected_end {
            return Err(Error::unsupported(
                "trusted offsets final offset must match values length",
            ));
        }

        Ok(())
    }

    /// Debug-validates the offset invariants against the provided values length.
    ///
    /// # Panics
    ///
    /// Panics in debug builds if `Self::validate_for_values_len` fails.
    pub fn debug_assert_valid_for_values_len(&self, data_len: usize) {
        #[cfg(debug_assertions)]
        if let Err(err) = self.validate_for_values_len(data_len) {
            panic!("TrustedOffsets invariant violated: {err}");
        }

        #[cfg(not(debug_assertions))]
        let _ = data_len;
    }

    /// Appends the current variable-width data length as the next offset.
    ///
    /// # Errors
    ///
    /// Returns an error if `data_len` exceeds the `i64` range required by Arrow-style large
    /// offsets.
    pub fn push_current_data_len(&mut self, data_len: usize) -> crate::error::Result<()> {
        let next_offset = i64::try_from(data_len)
            .map_err(|_| Error::unsupported("columnar variable buffer exceeds i64 offset range"))?;
        debug_assert!(self.offsets.last().is_some_and(|last| *last <= next_offset));
        self.offsets.push(next_offset);
        Ok(())
    }

    /// Repeats the last offset value, preserving the current variable-width data length.
    ///
    /// # Panics
    ///
    /// Panics if the invariant that offsets always contain the initial zero value has been broken.
    pub fn push_repeat_last(&mut self) {
        let last = *self
            .offsets
            .last()
            .expect("trusted offsets always contain an initial zero");
        self.offsets.push(last);
    }

    #[must_use]
    /// Returns the current trailing offset.
    ///
    /// # Panics
    ///
    /// Panics if the invariant that offsets always contain the initial zero value has been broken.
    pub fn last(&self) -> i64 {
        *self
            .offsets
            .last()
            .expect("trusted offsets always contain an initial zero")
    }

    #[must_use]
    pub fn into_inner(self) -> Vec<i64> {
        self.offsets
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PrimitiveBuffer<'a, T> {
    pub values: &'a [T],
    /// Bit-packed validity: each `u64` holds 64 row-validity bits (LSB = first row).
    pub valid: Option<&'a [u64]>,
}

/// The string vocabulary for a dictionary-encoded column.
///
/// Paired with [`Utf8Buffer::dictionary_ids`] when dictionary staging is active.
/// Not yet populated in the current scan path — reserved for future dictionary-value exposure
/// (e.g. Arrow `DictionaryArray` or Polars categoricals).
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub struct Utf8Dictionary<'a> {
    pub(crate) values: &'a [&'a str],
}

#[derive(Debug, Clone, Copy)]
pub struct Utf8Buffer<'a> {
    pub offsets: &'a [i64],
    pub data: &'a [u8],
    /// Bit-packed validity: each `u64` holds 64 row-validity bits (LSB = first row).
    pub valid: Option<&'a [u64]>,
    // IDs into the decoder's internal dictionary staging table; only populated when
    // dictionary staging is active. Exposed for tests and diagnostic tooling.
    #[allow(dead_code)]
    pub(crate) dictionary_ids: Option<&'a [u32]>,
    // String vocabulary paired with dictionary_ids; not yet populated.
    #[allow(dead_code)]
    pub(crate) dictionary: Option<&'a Utf8Dictionary<'a>>,
}

#[derive(Debug, Clone, Copy)]
pub struct BytesBuffer<'a> {
    pub offsets: &'a [i64],
    pub data: &'a [u8],
    /// Bit-packed validity: each `u64` holds 64 row-validity bits (LSB = first row).
    pub valid: Option<&'a [u64]>,
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

impl<'a> ColumnBuffer<'a> {
    #[must_use]
    pub const fn is_nullable(&self) -> bool {
        match self {
            Self::I32(b) => b.valid.is_some(),
            Self::I64(b) => b.valid.is_some(),
            Self::F64(b) => b.valid.is_some(),
            Self::Date(b) => b.valid.is_some(),
            Self::DateTime(b) => b.valid.is_some(),
            Self::Time(b) => b.valid.is_some(),
            Self::Utf8(b) => b.valid.is_some(),
            Self::RawBytes(b) => b.valid.is_some(),
        }
    }

    #[must_use]
    pub const fn as_f64_slice(&self) -> Option<&'a [f64]> {
        if let Self::F64(b) = self {
            Some(b.values)
        } else {
            None
        }
    }

    #[must_use]
    pub const fn as_i32_slice(&self) -> Option<&'a [i32]> {
        if let Self::I32(b) = self {
            Some(b.values)
        } else {
            None
        }
    }

    #[must_use]
    pub const fn as_i64_slice(&self) -> Option<&'a [i64]> {
        if let Self::I64(b) = self {
            Some(b.values)
        } else {
            None
        }
    }

    /// Iterate over string values. Yields `None` for null entries when the column is nullable.
    /// For non-nullable columns every entry is `Some`.
    ///
    /// # Panics
    ///
    /// Panics if the internal offset buffer contains a negative value, which violates the
    /// [`TrustedOffsets`] invariant and indicates a corrupt batch.
    #[must_use]
    pub fn as_str_iter(&self) -> Option<impl Iterator<Item = Option<&str>>> {
        if let Self::Utf8(b) = self {
            let offsets = b.offsets;
            let data = b.data;
            let valid = b.valid;
            let len = offsets.len().saturating_sub(1);
            Some((0..len).map(move |i| {
                let is_valid = valid.is_none_or(|v| {
                    let word = i / 64;
                    let bit = i % 64;
                    word < v.len() && (v[word] >> bit) & 1 == 1
                });
                if !is_valid {
                    return None;
                }
                let start = usize::try_from(offsets[i]).expect("trusted offset is non-negative");
                let end = usize::try_from(offsets[i + 1]).expect("trusted offset is non-negative");
                std::str::from_utf8(data.get(start..end)?).ok()
            }))
        } else {
            None
        }
    }
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
                offsets: $crate::columnar::TrustedOffsets,
                data: Vec<u8>,
                valid: Option<Vec<u64>>,
                dictionary_ids: Option<Vec<u32>>,
            },
            RawBytes {
                offsets: $crate::columnar::TrustedOffsets,
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
                offsets: offsets.as_slice(),
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
                offsets: offsets.as_slice(),
                data,
                valid: valid.as_deref(),
            }),
        }
    }

    #[cfg(feature = "arrow")]
    /// Convert the owned column buffer into an Arrow array.
    ///
    /// # Errors
    ///
    /// Returns an error if the data cannot be encoded as the selected Arrow
    /// array type.
    pub fn into_arrow_array(self) -> Result<ArrayRef> {
        self.as_borrowed().into_arrow_array()
    }
}

#[cfg(feature = "arrow")]
impl ColumnBuffer<'_> {
    /// Convert the borrowed column buffer into an Arrow array.
    ///
    /// # Errors
    ///
    /// Returns an error if the data cannot be encoded as the selected Arrow
    /// array type.
    pub fn into_arrow_array(self) -> Result<ArrayRef> {
        match self {
            Self::I32(PrimitiveBuffer { values, valid }) => {
                build_primitive_array::<Int32Type, _, _>(values.iter().copied(), valid, |value| {
                    Ok(value)
                })
            }
            Self::I64(PrimitiveBuffer { values, valid }) => {
                build_primitive_array::<Int64Type, _, _>(values.iter().copied(), valid, |value| {
                    Ok(value)
                })
            }
            Self::F64(PrimitiveBuffer { values, valid }) => {
                build_primitive_array::<Float64Type, _, _>(values.iter().copied(), valid, |value| {
                    Ok(value)
                })
            }
            Self::Date(PrimitiveBuffer { values, valid }) => {
                build_primitive_array::<Date32Type, _, _>(values.iter().copied(), valid, |value| {
                    Ok(value.days_since_sas_epoch)
                })
            }
            Self::DateTime(PrimitiveBuffer { values, valid }) => {
                build_primitive_array::<TimestampSecondType, _, _>(
                    values.iter().copied(),
                    valid,
                    |value| Ok(value.seconds_since_sas_epoch),
                )
            }
            Self::Time(PrimitiveBuffer { values, valid }) => {
                build_primitive_array::<Time32SecondType, _, _>(
                    values.iter().copied(),
                    valid,
                    |value| Ok(value.seconds_since_midnight),
                )
            }
            Self::Utf8(Utf8Buffer {
                offsets,
                data,
                valid,
                dictionary_ids: _,
                dictionary: _,
            }) => build_utf8_array(offsets, data, valid),
            Self::RawBytes(BytesBuffer {
                offsets,
                data,
                valid,
            }) => build_binary_array(offsets, data, valid),
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
    /// Convert the owned batch into an Arrow record batch.
    ///
    /// # Errors
    ///
    /// Returns an error if one of the columns cannot be converted or if the
    /// resulting arrays do not match the provided schema.
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
impl ColumnarBatch<'_> {
    /// Convert the borrowed batch into an Arrow record batch.
    ///
    /// # Errors
    ///
    /// Returns an error if one of the columns cannot be converted or if the
    /// resulting arrays do not match the provided schema.
    pub fn into_arrow_record_batch(&self, schema: SchemaRef) -> Result<RecordBatch> {
        let arrays = self
            .columns
            .iter()
            .copied()
            .map(ColumnBuffer::into_arrow_array)
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(schema, arrays).map_err(|err| Error::arrow(err.to_string()))
    }
}

#[cfg(feature = "arrow")]
fn build_primitive_array<T, I, F>(
    values: I,
    valid: Option<BitSlice<'_>>,
    mut map: F,
) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: Copy,
    I: IntoIterator,
    F: FnMut(I::Item) -> Result<T::Native>,
{
    let mut builder = PrimitiveBuilder::<T>::new();
    for (idx, value) in values.into_iter().enumerate() {
        if row_is_valid(valid, idx) {
            builder.append_value(map(value)?);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(feature = "arrow")]
fn build_utf8_array(offsets: &[i64], data: &[u8], valid: Option<BitSlice<'_>>) -> Result<ArrayRef> {
    let mut builder = StringBuilder::new();
    let row_count = offsets.len().saturating_sub(1);
    for idx in 0..row_count {
        if row_is_valid(valid, idx) {
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
    offsets: &[i64],
    data: &[u8],
    valid: Option<BitSlice<'_>>,
) -> Result<ArrayRef> {
    let mut builder = BinaryBuilder::new();
    let row_count = offsets.len().saturating_sub(1);
    for idx in 0..row_count {
        if row_is_valid(valid, idx) {
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
    valid.is_none_or(|words| {
        let word = idx / 64;
        let bit = idx % 64;
        words
            .get(word)
            .is_some_and(|bits| (bits & (1u64 << bit)) != 0)
    })
}

#[cfg(test)]
mod tests {
    use super::TrustedOffsets;

    #[test]
    fn trusted_offsets_default_has_initial_zero() {
        let offsets = TrustedOffsets::default();
        assert_eq!(offsets.as_slice(), &[0]);
        offsets
            .validate_for_values_len(0)
            .expect("default trusted offsets should be valid");
    }

    #[test]
    fn trusted_offsets_validate_rejects_non_zero_start() {
        let offsets = TrustedOffsets {
            offsets: vec![1, 3],
        };
        assert!(offsets.validate_for_values_len(3).is_err());
    }

    #[test]
    fn trusted_offsets_validate_rejects_non_monotonic_offsets() {
        let offsets = TrustedOffsets {
            offsets: vec![0, 4, 2],
        };
        assert!(offsets.validate_for_values_len(2).is_err());
    }
}
