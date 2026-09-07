use crate::error::Error;
#[cfg(feature = "arrow")]
use crate::error::Result;
use crate::metadata::{SasDate, SasDateTime, SasTime};
#[cfg(feature = "arrow")]
use arrow_array::{
    ArrayRef, BinaryArray, Date32Array, DurationNanosecondArray, Float64Array, Int32Array,
    Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    builder::{BinaryBuilder, PrimitiveBuilder, StringBuilder},
    types::{
        ArrowPrimitiveType, Date32Type, DurationNanosecondType, Float64Type, Int32Type, Int64Type,
        TimestampMicrosecondType,
    },
};
#[cfg(feature = "arrow")]
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
#[cfg(feature = "arrow")]
use arrow_schema::{DataType, SchemaRef, TimeUnit};
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
            return Err(Error::internal("trusted offsets must start at zero"));
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
    /// Bytes this column holds on the heap. See [`OwnedColumnarBatch::heap_bytes`].
    #[must_use]
    pub fn heap_bytes(&self) -> usize {
        fn validity(valid: Option<&Vec<u64>>) -> usize {
            valid.map_or(0, |bits| bits.len() * size_of::<u64>())
        }
        match self {
            Self::I32 { values, valid } => {
                values.len() * size_of::<i32>() + validity(valid.as_ref())
            }
            Self::I64 { values, valid } => {
                values.len() * size_of::<i64>() + validity(valid.as_ref())
            }
            Self::F64 { values, valid } => {
                values.len() * size_of::<f64>() + validity(valid.as_ref())
            }
            Self::Date { values, valid } => {
                values.len() * size_of::<crate::metadata::SasDate>() + validity(valid.as_ref())
            }
            Self::DateTime { values, valid } => {
                values.len() * size_of::<crate::metadata::SasDateTime>() + validity(valid.as_ref())
            }
            Self::Time { values, valid } => {
                values.len() * size_of::<crate::metadata::SasTime>() + validity(valid.as_ref())
            }
            Self::Utf8 {
                offsets,
                data,
                valid,
                dictionary_ids,
            } => {
                offsets.len() * size_of::<i64>()
                    + data.len()
                    + validity(valid.as_ref())
                    + dictionary_ids
                        .as_ref()
                        .map_or(0, |ids| ids.len() * size_of::<u32>())
            }
            Self::RawBytes {
                offsets,
                data,
                valid,
            } => offsets.len() * size_of::<i64>() + data.len() + validity(valid.as_ref()),
        }
    }

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
    /// Move the owned column buffer into an Arrow array.
    ///
    /// No value is copied. A primitive column's `Vec` becomes the array's
    /// buffer, the validity words become its null buffer as they are (Arrow
    /// packs validity the same way, least-significant bit first), a string or
    /// bytes column moves its data, and the only per-value work is the epoch
    /// shift on dates and datetimes, done in place. The offsets of a string
    /// column are narrowed from the scanner's `i64` to Arrow's `Utf8` `i32`,
    /// which is why a single batch of more than 2 GB of string data is an error
    /// rather than a `LargeUtf8` array: the declared schema says `Utf8`.
    ///
    /// The borrowed conversion, [`ColumnBuffer::into_arrow_array`], builds
    /// every array through a builder one value at a time, which a batch that
    /// is only borrowed cannot avoid. Measured on a 1.7-million-row register
    /// file, moving the buffers took a quarter of the time.
    ///
    /// # Errors
    ///
    /// Returns an error if the data cannot be encoded as the selected Arrow
    /// array type: a string that is not UTF-8, offsets that are not monotone,
    /// or a string batch too large for `i32` offsets.
    pub fn into_arrow_array(self) -> Result<ArrayRef> {
        Ok(match self {
            Self::I32 { values, valid } => {
                let rows = values.len();
                Arc::new(Int32Array::new(
                    ScalarBuffer::from(values),
                    null_buffer(valid, rows),
                ))
            }
            Self::I64 { values, valid } => {
                let rows = values.len();
                Arc::new(Int64Array::new(
                    ScalarBuffer::from(values),
                    null_buffer(valid, rows),
                ))
            }
            Self::F64 { values, valid } => {
                let rows = values.len();
                Arc::new(Float64Array::new(
                    ScalarBuffer::from(values),
                    null_buffer(valid, rows),
                ))
            }
            Self::Date { values, valid } => {
                // Arrow Date32 counts days from the Unix epoch (1970), not the SAS
                // epoch (1960): shift in place on the moved buffer.
                let mut days: Vec<i32> = bytemuck::cast_vec(values);
                for day in &mut days {
                    *day -= SasDate::DAYS_SAS_TO_UNIX;
                }
                let rows = days.len();
                Arc::new(Date32Array::new(
                    ScalarBuffer::from(days),
                    null_buffer(valid, rows),
                ))
            }
            Self::DateTime { values, valid } => {
                // Whole seconds from 1960 to microseconds from 1970, in place; a
                // sub-second column arrives as `F64` and is handled by the schema-aware
                // conversion.
                let mut micros: Vec<i64> = bytemuck::cast_vec(values);
                for value in &mut micros {
                    *value = (*value - SasDateTime::SECONDS_SAS_TO_UNIX).saturating_mul(1_000_000);
                }
                let rows = micros.len();
                Arc::new(TimestampMicrosecondArray::new(
                    ScalarBuffer::from(micros),
                    null_buffer(valid, rows),
                ))
            }
            Self::Time { values, valid } => {
                // Duration, not Time64: see `scan::plan::arrow_data_type`.
                let nanos: Vec<i64> = values
                    .iter()
                    .map(|time| {
                        i64::from(time.seconds_since_midnight).saturating_mul(1_000_000_000)
                    })
                    .collect();
                let rows = nanos.len();
                Arc::new(DurationNanosecondArray::new(
                    ScalarBuffer::from(nanos),
                    null_buffer(valid, rows),
                ))
            }
            Self::Utf8 {
                offsets,
                mut data,
                valid,
                dictionary_ids: _,
            } => {
                let rows = offsets.len().saturating_sub(1);
                // The scanner sizes a string buffer for the column's full width
                // and trimmed values fill less of it. Consumers that keep the
                // array keep the whole allocation, so it is cut to its contents.
                data.truncate(usize::try_from(offsets.last()).unwrap_or(data.len()));
                data.shrink_to_fit();
                let offsets = narrow_offsets(offsets.into_inner(), data.len())?;
                // Validates the whole buffer as UTF-8 and every offset as a
                // character boundary, in one pass.
                let array =
                    StringArray::try_new(offsets, Buffer::from_vec(data), null_buffer(valid, rows))
                        .map_err(|err| Error::arrow(err.to_string()))?;
                Arc::new(array)
            }
            Self::RawBytes {
                offsets,
                mut data,
                valid,
            } => {
                let rows = offsets.len().saturating_sub(1);
                data.truncate(usize::try_from(offsets.last()).unwrap_or(data.len()));
                data.shrink_to_fit();
                let offsets = narrow_offsets(offsets.into_inner(), data.len())?;
                let array =
                    BinaryArray::try_new(offsets, Buffer::from_vec(data), null_buffer(valid, rows))
                        .map_err(|err| Error::arrow(err.to_string()))?;
                Arc::new(array)
            }
        })
    }
}

/// The scanner's validity words as an Arrow null buffer. `None` means every
/// row is valid, which Arrow also spells as no buffer.
#[cfg(feature = "arrow")]
fn null_buffer(valid: Option<Vec<u64>>, rows: usize) -> Option<NullBuffer> {
    valid.map(|words| NullBuffer::new(BooleanBuffer::new(Buffer::from_vec(words), 0, rows)))
}

/// The scanner's `i64` offsets as the `i32` offsets an Arrow `Utf8` or
/// `Binary` array takes, checked to be monotone and within `data_len`, so
/// that the checked array constructors after this never panic.
#[cfg(feature = "arrow")]
fn narrow_offsets(offsets: Vec<i64>, data_len: usize) -> Result<OffsetBuffer<i32>> {
    let mut narrowed = Vec::with_capacity(offsets.len());
    let mut previous = 0i64;
    for (index, offset) in offsets.into_iter().enumerate() {
        if offset < previous || (index == 0 && offset != 0) {
            return Err(Error::arrow("string offsets are not monotone from zero"));
        }
        if usize::try_from(offset).is_ok_and(|end| end > data_len) {
            return Err(Error::arrow("string offsets run past their data"));
        }
        narrowed.push(i32::try_from(offset).map_err(|_| {
            Error::arrow(
                "a string batch over 2 GB does not fit i32 offsets; ask for a smaller batch",
            )
        })?);
        previous = offset;
    }
    if narrowed.is_empty() {
        narrowed.push(0);
    }
    Ok(OffsetBuffer::new(ScalarBuffer::from(narrowed)))
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
                // Arrow Date32 counts days from the Unix epoch (1970), not the SAS epoch (1960).
                build_primitive_array::<Date32Type, _, _>(values.iter().copied(), valid, |value| {
                    Ok(value.unix_days())
                })
            }
            Self::DateTime(PrimitiveBuffer { values, valid }) => {
                // Arrow timestamps count from the Unix epoch (1970), not the SAS epoch (1960).
                // A DateTime buffer holds whole seconds (sub-second values widen to F64 and are
                // handled in `column_buffer_to_arrow`); scale to microseconds.
                build_primitive_array::<TimestampMicrosecondType, _, _>(
                    values.iter().copied(),
                    valid,
                    |value| Ok(value.unix_seconds().saturating_mul(1_000_000)),
                )
            }
            Self::Time(PrimitiveBuffer { values, valid }) => {
                // Duration, not Time64: SAS TIME is a signed count of seconds since midnight
                // and is not confined to `[0, 24h)`. See `scan::plan::arrow_data_type`.
                build_primitive_array::<DurationNanosecondType, _, _>(
                    values.iter().copied(),
                    valid,
                    |value| {
                        Ok(i64::from(value.seconds_since_midnight).saturating_mul(1_000_000_000))
                    },
                )
            }
            Self::Utf8(Utf8Buffer {
                offsets,
                data,
                valid,
                dictionary_ids: _,
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

    /// Bytes this batch holds on the heap.
    ///
    /// For consumers that budget memory over batches they have not converted yet — the
    /// Parquet writer sizes row groups this way, so that it can decide when a row group is
    /// full without first turning every batch into Arrow arrays.
    ///
    /// Counts buffer contents, not capacity, and ignores the enum's own inline size. It is
    /// a budgeting figure, not an allocator accounting.
    #[must_use]
    pub fn heap_bytes(&self) -> usize {
        self.columns.iter().map(OwnedColumnBuffer::heap_bytes).sum()
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
            .zip(schema.fields())
            .map(|(column, field)| owned_column_to_arrow(column, field.data_type()))
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(schema, arrays).map_err(|err| Error::arrow(err.to_string()))
    }
}

/// Move an owned column buffer into an Arrow array of the schema's declared type.
///
/// The owned twin of [`column_buffer_to_arrow`]: the same rule for a temporal
/// column widened to `F64`, and [`OwnedColumnBuffer::into_arrow_array`], which
/// moves rather than copies, for everything else.
#[cfg(feature = "arrow")]
fn owned_column_to_arrow(buffer: OwnedColumnBuffer, field_type: &DataType) -> Result<ArrayRef> {
    buffer.into_arrow_array_as(field_type)
}

#[cfg(feature = "arrow")]
impl OwnedColumnBuffer {
    /// Move the buffer into an Arrow array of `field_type`, the type the scan's
    /// Arrow schema declares for its column.
    ///
    /// The one thing the buffer cannot decide for itself: a temporal column
    /// whose values did not all fit a whole integer arrives as `F64` in raw SAS
    /// units, and `field_type` says which temporal type it must come out as.
    /// Every other buffer goes through [`Self::into_arrow_array`].
    ///
    /// # Errors
    ///
    /// As [`Self::into_arrow_array`].
    pub fn into_arrow_array_as(self, field_type: &DataType) -> Result<ArrayRef> {
        owned_column_to_arrow_as(self, field_type)
    }
}

#[cfg(feature = "arrow")]
fn owned_column_to_arrow_as(buffer: OwnedColumnBuffer, field_type: &DataType) -> Result<ArrayRef> {
    match (field_type, buffer) {
        (
            DataType::Timestamp(TimeUnit::Microsecond, _),
            OwnedColumnBuffer::F64 { values, valid },
        ) => {
            #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
            let micros: Vec<i64> = values
                .iter()
                .map(|&raw| {
                    ((raw - SasDateTime::SECONDS_SAS_TO_UNIX as f64) * 1_000_000.0).round() as i64
                })
                .collect();
            let rows = micros.len();
            Ok(Arc::new(TimestampMicrosecondArray::new(
                ScalarBuffer::from(micros),
                null_buffer(valid, rows),
            )))
        }
        (DataType::Duration(TimeUnit::Nanosecond), OwnedColumnBuffer::F64 { values, valid }) => {
            #[allow(clippy::cast_possible_truncation)]
            let nanos: Vec<i64> = values
                .iter()
                .map(|&raw| (raw * 1_000_000_000.0).round() as i64)
                .collect();
            let rows = nanos.len();
            Ok(Arc::new(DurationNanosecondArray::new(
                ScalarBuffer::from(nanos),
                null_buffer(valid, rows),
            )))
        }
        (DataType::Date32, OwnedColumnBuffer::F64 { values, valid }) => {
            #[allow(clippy::cast_possible_truncation)]
            let days: Vec<i32> = values
                .iter()
                .map(|&raw| raw.round() as i32 - SasDate::DAYS_SAS_TO_UNIX)
                .collect();
            let rows = days.len();
            Ok(Arc::new(Date32Array::new(
                ScalarBuffer::from(days),
                null_buffer(valid, rows),
            )))
        }
        (_, buffer) => buffer.into_arrow_array(),
    }
}

/// Convert a column buffer to an Arrow array of the schema's declared type.
///
/// Temporal columns whose values didn't all fit a whole integer are widened to an `F64`
/// buffer holding RAW SAS-epoch units; this reinterprets such a buffer as the declared
/// temporal type (preserving sub-second precision) instead of a bare `Float64`. Every
/// other buffer goes through its plain [`ColumnBuffer::into_arrow_array`] conversion.
#[cfg(feature = "arrow")]
fn column_buffer_to_arrow(buffer: ColumnBuffer<'_>, field_type: &DataType) -> Result<ArrayRef> {
    match (field_type, &buffer) {
        (DataType::Timestamp(TimeUnit::Microsecond, _), ColumnBuffer::F64(buf)) => {
            let PrimitiveBuffer { values, valid } = buf;
            build_primitive_array::<TimestampMicrosecondType, _, _>(
                values.iter().copied(),
                *valid,
                |raw| {
                    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
                    Ok(
                        ((raw - SasDateTime::SECONDS_SAS_TO_UNIX as f64) * 1_000_000.0).round()
                            as i64,
                    )
                },
            )
        }
        (DataType::Duration(TimeUnit::Nanosecond), ColumnBuffer::F64(buf)) => {
            let PrimitiveBuffer { values, valid } = buf;
            build_primitive_array::<DurationNanosecondType, _, _>(
                values.iter().copied(),
                *valid,
                |raw| {
                    // A float->int cast saturates in Rust, so a value beyond i64 nanoseconds
                    // (~292 years) pins to the bound rather than wrapping to nonsense.
                    #[allow(clippy::cast_possible_truncation)]
                    Ok((raw * 1_000_000_000.0).round() as i64)
                },
            )
        }
        (DataType::Date32, ColumnBuffer::F64(buf)) => {
            let PrimitiveBuffer { values, valid } = buf;
            build_primitive_array::<Date32Type, _, _>(values.iter().copied(), *valid, |raw| {
                #[allow(clippy::cast_possible_truncation)]
                Ok(raw.round() as i32 - SasDate::DAYS_SAS_TO_UNIX)
            })
        }
        _ => buffer.into_arrow_array(),
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
            .zip(schema.fields())
            .map(|(column, field)| column_buffer_to_arrow(column, field.data_type()))
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

#[cfg(all(test, feature = "arrow"))]
mod arrow_tests {
    //! The moved conversion must produce exactly what the copying one does.
    use super::{OwnedColumnBuffer, TrustedOffsets, column_buffer_to_arrow, owned_column_to_arrow};
    use crate::metadata::{SasDate, SasDateTime, SasTime};
    use arrow_schema::{DataType, TimeUnit};

    fn same(buffer: OwnedColumnBuffer, declared: &DataType) {
        let copied = column_buffer_to_arrow(buffer.as_borrowed(), declared).expect("borrowed");
        let moved = owned_column_to_arrow(buffer, declared).expect("owned");
        assert_eq!(copied.data_type(), moved.data_type());
        assert_eq!(&*copied, &*moved);
    }

    fn trusted(lengths: &[usize]) -> (TrustedOffsets, usize) {
        let mut offsets = TrustedOffsets::with_capacity_for_rows(lengths.len());
        let mut total = 0;
        for length in lengths {
            total += length;
            offsets.push_current_data_len(total).expect("offset");
        }
        (offsets, total)
    }

    // Rows 0, 2 and 4 valid; 1 and 3 null.
    const VALID: u64 = 0b10101;

    #[test]
    fn primitives_move_with_their_validity() {
        same(
            OwnedColumnBuffer::I32 {
                values: vec![1, 2, 3, 4, 5],
                valid: Some(vec![VALID]),
            },
            &DataType::Int32,
        );
        same(
            OwnedColumnBuffer::I64 {
                values: vec![1, 2, 3, 4, 5],
                valid: None,
            },
            &DataType::Int64,
        );
        same(
            OwnedColumnBuffer::F64 {
                values: vec![1.5, 2.5, 3.5, 4.5, 5.5],
                valid: Some(vec![VALID]),
            },
            &DataType::Float64,
        );
    }

    #[test]
    fn temporals_shift_epochs_in_place() {
        let dates = (0..5)
            .map(|day| SasDate {
                days_since_sas_epoch: day * 400,
            })
            .collect();
        same(
            OwnedColumnBuffer::Date {
                values: dates,
                valid: Some(vec![VALID]),
            },
            &DataType::Date32,
        );
        let datetimes = (0..5)
            .map(|s| SasDateTime {
                seconds_since_sas_epoch: s * 86_400 + 1,
            })
            .collect();
        same(
            OwnedColumnBuffer::DateTime {
                values: datetimes,
                valid: None,
            },
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        );
        let times = (0..5)
            .map(|s| SasTime {
                seconds_since_midnight: s * 3_600 - 7_200,
            })
            .collect();
        same(
            OwnedColumnBuffer::Time {
                values: times,
                valid: Some(vec![VALID]),
            },
            &DataType::Duration(TimeUnit::Nanosecond),
        );
    }

    #[test]
    fn widened_temporals_follow_the_declared_type() {
        let raw = vec![0.5, 1.25, 86_400.75, 3.0, 4.0];
        for declared in [
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Duration(TimeUnit::Nanosecond),
            DataType::Date32,
        ] {
            same(
                OwnedColumnBuffer::F64 {
                    values: raw.clone(),
                    valid: Some(vec![VALID]),
                },
                &declared,
            );
        }
    }

    #[test]
    fn strings_and_bytes_move_their_offsets_and_data() {
        let (offsets, _) = trusted(&[2, 0, 3, 0, 4]);
        let data = b"ABCDEFGHI".to_vec();
        same(
            OwnedColumnBuffer::Utf8 {
                offsets,
                data: data.clone(),
                valid: Some(vec![VALID]),
                dictionary_ids: None,
            },
            &DataType::Utf8,
        );
        let (offsets, _) = trusted(&[2, 0, 3, 0, 4]);
        same(
            OwnedColumnBuffer::RawBytes {
                offsets,
                data,
                valid: None,
            },
            &DataType::Binary,
        );
    }

    #[test]
    fn an_empty_column_moves_too() {
        same(
            OwnedColumnBuffer::F64 {
                values: vec![],
                valid: None,
            },
            &DataType::Float64,
        );
        let (offsets, _) = trusted(&[]);
        same(
            OwnedColumnBuffer::Utf8 {
                offsets,
                data: vec![],
                valid: None,
                dictionary_ids: None,
            },
            &DataType::Utf8,
        );
    }
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

    /// Regression: a SAS TIME is a signed count of seconds since midnight and is not confined
    /// to a clock day. Under the old `Time64(Nanosecond)` declaration these integers were
    /// written faithfully but fell outside the type's `[0, 24h)` domain, so arrow-rs, pyarrow,
    /// `DuckDB` and Polars all handed back null. `Duration(Nanosecond)` carries the same i64
    /// payload over the whole SAS range.
    #[cfg(feature = "arrow")]
    #[test]
    fn out_of_range_times_convert_to_durations_without_nulling() {
        use super::{ColumnBuffer, PrimitiveBuffer};
        use crate::metadata::SasTime;
        use arrow_array::{Array, DurationNanosecondArray};
        use arrow_schema::{DataType, TimeUnit};

        let time = |seconds| SasTime {
            seconds_since_midnight: seconds,
        };
        // 99h48m (the value that motivated this), exactly 24h, a negative offset, and an
        // ordinary clock time that must be unaffected.
        let values = [time(359_280), time(86_400), time(-77), time(69_507)];
        let array = ColumnBuffer::Time(PrimitiveBuffer {
            values: &values,
            valid: None,
        })
        .into_arrow_array()
        .expect("time column converts");

        assert_eq!(array.data_type(), &DataType::Duration(TimeUnit::Nanosecond));
        let column = array
            .as_any()
            .downcast_ref::<DurationNanosecondArray>()
            .expect("Duration(ns) column");
        assert_eq!(
            column.null_count(),
            0,
            "the declared type must not null any in-domain SAS value"
        );
        assert_eq!(column.value(0), 359_280 * 1_000_000_000);
        assert_eq!(column.value(1), 86_400 * 1_000_000_000);
        assert_eq!(column.value(2), -77 * 1_000_000_000);
        assert_eq!(column.value(3), 69_507 * 1_000_000_000);
    }

    /// The same, for a TIME column that widened to `F64` because its values carried a
    /// fractional part. It reaches Arrow through the schema-aware path instead.
    #[cfg(feature = "arrow")]
    #[test]
    fn widened_fractional_times_convert_to_durations() {
        use super::{ColumnBuffer, PrimitiveBuffer, column_buffer_to_arrow};
        use arrow_array::{Array, DurationNanosecondArray};
        use arrow_schema::{DataType, TimeUnit};

        let values = [359_960.4_f64, -77.001];
        let array = column_buffer_to_arrow(
            ColumnBuffer::F64(PrimitiveBuffer {
                values: &values,
                valid: None,
            }),
            &DataType::Duration(TimeUnit::Nanosecond),
        )
        .expect("widened time column converts");

        let column = array
            .as_any()
            .downcast_ref::<DurationNanosecondArray>()
            .expect("Duration(ns) column");
        assert_eq!(column.null_count(), 0);
        assert_eq!(column.value(0), 359_960_400_000_000);
        assert_eq!(column.value(1), -77_001_000_000);
    }
}
