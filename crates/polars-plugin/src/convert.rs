//! The scanner's buffers as Arrow arrays, without copying them.
//!
//! An owned column buffer is a `Vec` of values and an optional `Vec<u64>`
//! validity bitmask, in exactly the layout Arrow wants: little-endian words,
//! least-significant bit first, one bit per row. So a primitive column moves
//! into an Arrow buffer as it is, a string column moves its offsets and its
//! bytes, and the only per-value work left is the epoch shift on dates and
//! datetimes, done in place on the moved buffer. The core's own
//! `into_arrow_array` builds every array through a builder, one value at a
//! time, which is the difference between this and a read twice as slow.

use arrow_array::builder::make_view;
use arrow_array::{
    ArrayRef, Date32Array, DurationNanosecondArray, Float64Array, Int32Array, Int64Array,
    LargeBinaryArray, StringViewArray, TimestampMicrosecondArray,
};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, TimeUnit};
use sas7bdat::{Error, OwnedColumnBuffer, Result as SasResult, SasDate, SasDateTime};
use std::sync::Arc;

/// The scanner's validity words as an Arrow null buffer. `None` means every
/// row is valid, which Arrow also spells as no buffer.
fn nulls(valid: Option<Vec<u64>>, rows: usize) -> Option<NullBuffer> {
    valid.map(|words| NullBuffer::new(BooleanBuffer::new(Buffer::from_vec(words), 0, rows)))
}

/// `column` as the Arrow array `declared` names, where `declared` is the core's
/// schema type for the column. That type decides one thing the buffer cannot:
/// a temporal column whose values did not all fit a whole integer arrives as
/// `F64` in raw SAS units and must still come out as the declared temporal
/// type.
pub fn column_to_arrow(
    column: OwnedColumnBuffer,
    declared: &DataType,
    rows: usize,
) -> SasResult<ArrayRef> {
    Ok(match column {
        OwnedColumnBuffer::I32 { values, valid } => Arc::new(Int32Array::new(
            ScalarBuffer::from(values),
            nulls(valid, rows),
        )),
        OwnedColumnBuffer::I64 { values, valid } => Arc::new(Int64Array::new(
            ScalarBuffer::from(values),
            nulls(valid, rows),
        )),
        OwnedColumnBuffer::F64 { values, valid } => widened_f64(values, valid, declared, rows),
        OwnedColumnBuffer::Date { values, valid } => {
            // Arrow counts days from 1970, SAS from 1960: shift in place.
            let mut days: Vec<i32> = bytemuck::cast_vec(values);
            for day in &mut days {
                *day -= SasDate::DAYS_SAS_TO_UNIX;
            }
            Arc::new(Date32Array::new(
                ScalarBuffer::from(days),
                nulls(valid, rows),
            ))
        }
        OwnedColumnBuffer::DateTime { values, valid } => {
            // Whole seconds from 1960 to microseconds from 1970, in place.
            let mut micros: Vec<i64> = bytemuck::cast_vec(values);
            for value in &mut micros {
                *value = (*value - SasDateTime::SECONDS_SAS_TO_UNIX).saturating_mul(1_000_000);
            }
            Arc::new(TimestampMicrosecondArray::new(
                ScalarBuffer::from(micros),
                nulls(valid, rows),
            ))
        }
        OwnedColumnBuffer::Time { values, valid } => {
            // Duration, not Time64: SAS TIME is a signed count of seconds since
            // midnight and is not confined to a day. The core declares it so.
            let nanos: Vec<i64> = values
                .iter()
                .map(|time| i64::from(time.seconds_since_midnight).saturating_mul(1_000_000_000))
                .collect();
            Arc::new(DurationNanosecondArray::new(
                ScalarBuffer::from(nanos),
                nulls(valid, rows),
            ))
        }
        OwnedColumnBuffer::Utf8 {
            offsets,
            data,
            valid,
            dictionary_ids: _,
        } => Arc::new(string_view_array(&offsets.into_inner(), data, valid, rows)?),
        OwnedColumnBuffer::RawBytes {
            offsets,
            data,
            valid,
        } => {
            let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets.into_inner()));
            let array =
                LargeBinaryArray::try_new(offsets, Buffer::from_vec(data), nulls(valid, rows))
                    .map_err(|err| Error::arrow(err.to_string()))?;
            Arc::new(array)
        }
    })
}

/// The scanner's strings as a `StringViewArray`, polars' own string layout:
/// one 16-byte view per row, a string of twelve bytes or fewer inlined in it
/// and a longer one pointing into the moved buffer.
///
/// Why views and not `LargeUtf8`: polars converts an imported `LargeUtf8`
/// column into views anyway, and for a column with any string over twelve
/// bytes it keeps the whole imported allocation alive behind the result, the
/// offsets, the validity and the buffer's unused capacity included. Measured on
/// a 1.7-million-row register file that was 134 MB retained on 44 MB of
/// strings. A view array imports as it is, so only the views and a buffer
/// shrunk to its contents remain.
///
/// Why unchecked: `StringViewArray::try_new` validates UTF-8 one string at a
/// time, which measured a third of a full read. The buffer is validated as a
/// whole instead, one pass, plus a check that every offset sits on a character
/// boundary, which together are the same guarantee.
fn string_view_array(
    offsets: &[i64],
    mut data: Vec<u8>,
    valid: Option<Vec<u64>>,
    rows: usize,
) -> SasResult<StringViewArray> {
    let text = std::str::from_utf8(&data).map_err(|err| Error::arrow(err.to_string()))?;
    if offsets.len() != rows + 1 {
        return Err(Error::arrow("string offsets do not match the row count"));
    }
    let end = usize::try_from(offsets[rows]).map_err(|_| Error::arrow("negative offset"))?;
    if end > data.len() {
        return Err(Error::arrow("string offsets run past their data"));
    }
    u32::try_from(end).map_err(|_| {
        Error::unsupported("a string batch over 4 GB; ask for a smaller batch_size")
    })?;
    let mut views: Vec<u128> = Vec::with_capacity(rows);
    let mut previous = 0usize;
    for &offset in &offsets[1..] {
        let offset = usize::try_from(offset).map_err(|_| Error::arrow("negative offset"))?;
        if offset < previous || !text.is_char_boundary(offset) {
            return Err(Error::arrow(
                "string offsets are not monotone character boundaries",
            ));
        }
        #[allow(clippy::cast_possible_truncation)]
        views.push(make_view(&data[previous..offset], 0, previous as u32));
        previous = offset;
    }
    // The buffer keeps its contents and loses its slack; a consumer that keeps
    // the array keeps this and nothing else.
    data.truncate(end);
    data.shrink_to_fit();
    // SAFETY: every view was built by `make_view` from a slice of `data` at the
    // offset it records, within `end <= data.len()`; `data` is valid UTF-8 as a
    // whole and every string starts and ends on a character boundary, so every
    // string is valid UTF-8; buffer index 0 is the one buffer passed.
    #[allow(unsafe_code)]
    let array = unsafe {
        StringViewArray::new_unchecked(
            ScalarBuffer::from(views),
            vec![Buffer::from_vec(data)],
            nulls(valid, rows),
        )
    };
    Ok(array)
}

/// An `F64` buffer as the array `declared` names. A genuine float column moves
/// as it is; a temporal column widened to `F64` holds raw SAS units and is
/// converted to the declared temporal type, keeping its sub-second precision.
fn widened_f64(
    values: Vec<f64>,
    valid: Option<Vec<u64>>,
    declared: &DataType,
    rows: usize,
) -> ArrayRef {
    match declared {
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
            let micros: Vec<i64> = values
                .iter()
                .map(|&seconds| {
                    ((seconds - SasDateTime::SECONDS_SAS_TO_UNIX as f64) * 1_000_000.0).round()
                        as i64
                })
                .collect();
            Arc::new(TimestampMicrosecondArray::new(
                ScalarBuffer::from(micros),
                nulls(valid, rows),
            ))
        }
        DataType::Date32 => {
            #[allow(clippy::cast_possible_truncation)]
            let days: Vec<i32> = values
                .iter()
                .map(|&days| days.round() as i32 - SasDate::DAYS_SAS_TO_UNIX)
                .collect();
            Arc::new(Date32Array::new(
                ScalarBuffer::from(days),
                nulls(valid, rows),
            ))
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            // A float-to-int cast saturates, so a value beyond i64 nanoseconds
            // pins to the bound rather than wrapping.
            #[allow(clippy::cast_possible_truncation)]
            let nanos: Vec<i64> = values
                .iter()
                .map(|&seconds| (seconds * 1_000_000_000.0).round() as i64)
                .collect();
            Arc::new(DurationNanosecondArray::new(
                ScalarBuffer::from(nanos),
                nulls(valid, rows),
            ))
        }
        _ => Arc::new(Float64Array::new(
            ScalarBuffer::from(values),
            nulls(valid, rows),
        )),
    }
}

/// The type a stream declares for a core schema type: strings as views, bytes
/// with 64-bit offsets, which is what the arrays above hold.
pub fn stream_type(declared: &DataType) -> DataType {
    match declared {
        DataType::Utf8 => DataType::Utf8View,
        DataType::Binary => DataType::LargeBinary,
        other => other.clone(),
    }
}
