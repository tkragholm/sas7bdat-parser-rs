#![allow(clippy::redundant_pub_crate)]

#[cfg(feature = "arrow")]
use arrow_schema::{DataType as ArrowSchemaDataType, TimeUnit as ArrowSchemaTimeUnit};
#[cfg(feature = "arrow")]
use polars::frame::DataFrame;
#[cfg(feature = "arrow")]
use polars_arrow::{
    array::{Array, BinaryArray, PrimitiveArray, Utf8Array},
    bitmap::MutableBitmap,
    datatypes::{ArrowDataType, ArrowSchema, Field, TimeUnit as PlTimeUnit},
    offset::{Offsets, OffsetsBuffer},
    record_batch::RecordBatch as PolarsRecordBatch,
};
#[cfg(feature = "arrow")]
use pyo3::{exceptions::PyValueError, prelude::*, types::PyModule};
#[cfg(feature = "arrow")]
use sas7bdat_simd::{Error, OwnedColumnBuffer, Result as SasResult, TrustedOffsets};
#[cfg(feature = "arrow")]
use std::sync::Arc;

#[cfg(feature = "arrow")]
pub(super) fn build_polars_schema(arrow_schema: &arrow_schema::Schema) -> SasResult<ArrowSchema> {
    let fields: Vec<Field> = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let dtype = arrow_dt_to_polars_arrow(field.data_type()).map_err(Error::arrow)?;
            Ok(Field::new(field.name().as_str().into(), dtype, true))
        })
        .collect::<SasResult<Vec<_>>>()?;
    ArrowSchema::from_iter_check_duplicates(fields).map_err(|err| Error::arrow(err.to_string()))
}

#[cfg(feature = "arrow")]
pub(super) fn arrow_dt_to_polars_arrow(dt: &ArrowSchemaDataType) -> Result<ArrowDataType, String> {
    Ok(match dt {
        ArrowSchemaDataType::Int32 => ArrowDataType::Int32,
        ArrowSchemaDataType::Int64 => ArrowDataType::Int64,
        ArrowSchemaDataType::Float64 => ArrowDataType::Float64,
        ArrowSchemaDataType::Utf8 | ArrowSchemaDataType::LargeUtf8 => ArrowDataType::LargeUtf8,
        ArrowSchemaDataType::Binary | ArrowSchemaDataType::LargeBinary => {
            ArrowDataType::LargeBinary
        }
        ArrowSchemaDataType::Date32 => ArrowDataType::Date32,
        ArrowSchemaDataType::Time32(ArrowSchemaTimeUnit::Second) => {
            ArrowDataType::Time64(PlTimeUnit::Nanosecond)
        }
        ArrowSchemaDataType::Time64(ArrowSchemaTimeUnit::Nanosecond) => {
            ArrowDataType::Time64(PlTimeUnit::Nanosecond)
        }
        ArrowSchemaDataType::Timestamp(ArrowSchemaTimeUnit::Second, None) => {
            ArrowDataType::Timestamp(PlTimeUnit::Second, None)
        }
        other => return Err(format!("unsupported Arrow type: {other:?}")),
    })
}

#[cfg(feature = "arrow")]
pub(super) fn owned_batch_to_dataframe(
    batch: sas7bdat_simd::OwnedColumnarBatch,
    schema: Arc<ArrowSchema>,
) -> SasResult<DataFrame> {
    let row_count = batch.row_count;
    let mut arrays: Vec<Box<dyn Array>> = Vec::with_capacity(batch.columns.len());

    for col in batch.columns {
        let array: Box<dyn Array> = match col {
            OwnedColumnBuffer::I32 { values, valid } => {
                Box::new(primitive_array_with_optional_validity(
                    ArrowDataType::Int32,
                    values,
                    valid,
                    row_count,
                ))
            }
            OwnedColumnBuffer::I64 { values, valid } => {
                Box::new(primitive_array_with_optional_validity(
                    ArrowDataType::Int64,
                    values,
                    valid,
                    row_count,
                ))
            }
            OwnedColumnBuffer::F64 { values, valid } => {
                Box::new(primitive_array_with_optional_validity(
                    ArrowDataType::Float64,
                    values,
                    valid,
                    row_count,
                ))
            }
            OwnedColumnBuffer::Date { values, valid } => {
                let i32s: Vec<i32> = bytemuck::cast_vec(values);
                Box::new(primitive_array_with_optional_validity(
                    ArrowDataType::Date32,
                    i32s,
                    valid,
                    row_count,
                ))
            }
            OwnedColumnBuffer::DateTime { values, valid } => {
                let i64s: Vec<i64> = bytemuck::cast_vec(values);
                let dtype = ArrowDataType::Timestamp(PlTimeUnit::Second, None);
                Box::new(primitive_array_with_optional_validity(
                    dtype, i64s, valid, row_count,
                ))
            }
            OwnedColumnBuffer::Time { values, valid } => {
                let nanos = values
                    .into_iter()
                    .map(|value| i64::from(value.seconds_since_midnight) * 1_000_000_000)
                    .collect();
                let dtype = ArrowDataType::Time64(PlTimeUnit::Nanosecond);
                Box::new(primitive_array_with_optional_validity(
                    dtype, nanos, valid, row_count,
                ))
            }
            OwnedColumnBuffer::Utf8 {
                offsets,
                data,
                valid,
                ..
            } => Box::new(trusted_large_utf8_array(offsets, data, valid, row_count)),
            OwnedColumnBuffer::RawBytes {
                offsets,
                data,
                valid,
            } => Box::new(trusted_large_binary_array(offsets, data, valid, row_count)),
        };
        arrays.push(array);
    }

    let rec = PolarsRecordBatch::try_new(row_count, schema, arrays)
        .map_err(|err| Error::arrow(err.to_string()))?;
    Ok(DataFrame::from(rec))
}

#[cfg(feature = "arrow")]
fn trusted_large_offsets_buffer(offsets: TrustedOffsets, data_len: usize) -> OffsetsBuffer<i64> {
    offsets.debug_assert_valid_for_values_len(data_len);

    // SAFETY:
    // `TrustedOffsets` is only produced by the scanner's variable-width builders in
    // `sas7bdat-simd`. Those builders:
    // - initialize offsets with a single `0`
    // - append the current data length after each pushed value
    // - repeat the previous offset for nulls
    // This guarantees non-negative, monotonically non-decreasing Arrow offsets.
    // Debug builds re-check the final offset matches `data_len` at this boundary.
    unsafe { Offsets::new_unchecked(offsets.into_inner()).into() }
}

#[cfg(feature = "arrow")]
fn trusted_large_utf8_array(
    offsets: TrustedOffsets,
    data: Vec<u8>,
    valid: Option<Vec<u64>>,
    row_count: usize,
) -> Utf8Array<i64> {
    let offs_buf = trusted_large_offsets_buffer(offsets, data.len());
    let bitmap = valid.map(|v| bits_to_bitmap(&v, row_count));

    #[cfg(debug_assertions)]
    {
        debug_assert!(
            std::str::from_utf8(&data).is_ok(),
            "scanner-produced Utf8 column must contain valid UTF-8 bytes"
        );
    }

    // SAFETY:
    // `TrustedOffsets` guarantees Arrow offset invariants for this `data` buffer.
    // Scanner-produced `Utf8` buffers are built only from:
    // - ASCII slices
    // - source slices that passed `from_utf8`
    // - decoded/encoded scratch strings from the configured encoding path
    // - explicit scalar-to-UTF-8 encoding for windows-1252 single-byte decoding
    // Therefore the concatenated `data` buffer is valid UTF-8, and debug builds
    // assert that directly here.
    unsafe {
        Utf8Array::<i64>::new_unchecked(ArrowDataType::LargeUtf8, offs_buf, data.into(), bitmap)
    }
}

#[cfg(feature = "arrow")]
fn trusted_large_binary_array(
    offsets: TrustedOffsets,
    data: Vec<u8>,
    valid: Option<Vec<u64>>,
    row_count: usize,
) -> BinaryArray<i64> {
    let offs_buf = trusted_large_offsets_buffer(offsets, data.len());
    let bitmap = valid.map(|v| bits_to_bitmap(&v, row_count));

    // SAFETY:
    // `TrustedOffsets` guarantees Arrow offset invariants for this `data` buffer,
    // and debug builds already checked the final offset against `data.len()`.
    unsafe {
        BinaryArray::<i64>::new_unchecked(ArrowDataType::LargeBinary, offs_buf, data.into(), bitmap)
    }
}

#[cfg(feature = "arrow")]
fn primitive_array_with_optional_validity<T: polars_arrow::types::NativeType>(
    dtype: ArrowDataType,
    values: Vec<T>,
    valid: Option<Vec<u64>>,
    row_count: usize,
) -> PrimitiveArray<T> {
    match valid {
        Some(valid) => PrimitiveArray::new(
            dtype,
            values.into(),
            Some(bits_to_bitmap(&valid, row_count)),
        ),
        None => PrimitiveArray::new(dtype, values.into(), None),
    }
}

#[cfg(feature = "arrow")]
pub(super) fn bits_to_bitmap(bits: &[u64], len: usize) -> polars_arrow::bitmap::Bitmap {
    #[cfg(target_endian = "little")]
    let bytes = bytemuck::cast_slice(bits).to_vec();

    #[cfg(target_endian = "big")]
    let mut bytes = Vec::with_capacity(bits.len() * 8);

    #[cfg(target_endian = "big")]
    for word in bits {
        bytes.extend_from_slice(&word.to_le_bytes());
    }

    MutableBitmap::from_vec(bytes, len).into()
}

#[cfg(feature = "arrow")]
pub(super) fn polars_dtype(
    polars: &Bound<'_, PyModule>,
    data_type: &ArrowSchemaDataType,
) -> PyResult<Py<PyAny>> {
    let dtype = match data_type {
        ArrowSchemaDataType::Int32 => polars.getattr("Int32")?.unbind(),
        ArrowSchemaDataType::Int64 => polars.getattr("Int64")?.unbind(),
        ArrowSchemaDataType::Float64 => polars.getattr("Float64")?.unbind(),
        ArrowSchemaDataType::Utf8 => polars.getattr("Utf8")?.unbind(),
        ArrowSchemaDataType::Binary => polars.getattr("Binary")?.unbind(),
        ArrowSchemaDataType::Date32 => polars.getattr("Date")?.unbind(),
        ArrowSchemaDataType::Time32(ArrowSchemaTimeUnit::Second)
        | ArrowSchemaDataType::Time64(ArrowSchemaTimeUnit::Nanosecond) => {
            polars.getattr("Time")?.unbind()
        }
        ArrowSchemaDataType::Timestamp(ArrowSchemaTimeUnit::Second, None) => {
            polars.getattr("Datetime")?.call1(("us",))?.unbind()
        }
        other => {
            return Err(PyValueError::new_err(format!(
                "unsupported Arrow type for Polars schema: {other:?}"
            )));
        }
    };
    Ok(dtype)
}

#[cfg(feature = "arrow")]
pub(super) fn py_err(err: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_common_arrow_types() {
        assert_eq!(
            arrow_dt_to_polars_arrow(&ArrowSchemaDataType::Int32).expect("int32"),
            ArrowDataType::Int32
        );
        assert_eq!(
            arrow_dt_to_polars_arrow(&ArrowSchemaDataType::Utf8).expect("utf8"),
            ArrowDataType::LargeUtf8
        );
        assert_eq!(
            arrow_dt_to_polars_arrow(&ArrowSchemaDataType::Timestamp(
                ArrowSchemaTimeUnit::Second,
                None
            ))
            .expect("timestamp"),
            ArrowDataType::Timestamp(PlTimeUnit::Second, None)
        );
    }
}
