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
use sas7bdat::{
    Error, LabelSet, OwnedColumnBuffer, Result as SasResult, SasDate, SasDateTime, TrustedOffsets,
};
#[cfg(feature = "arrow")]
use std::io::Write;
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
        ArrowSchemaDataType::Time32(ArrowSchemaTimeUnit::Second)
        | ArrowSchemaDataType::Time64(ArrowSchemaTimeUnit::Nanosecond) => {
            ArrowDataType::Time64(PlTimeUnit::Nanosecond)
        }
        ArrowSchemaDataType::Timestamp(ArrowSchemaTimeUnit::Microsecond, None) => {
            ArrowDataType::Timestamp(PlTimeUnit::Microsecond, None)
        }
        other => return Err(format!("unsupported Arrow type: {other:?}")),
    })
}

#[cfg(feature = "arrow")]
pub(super) fn owned_batch_to_dataframe(
    batch: sas7bdat::OwnedColumnarBatch,
    schema: Arc<ArrowSchema>,
    label_mapping: &[Option<LabelSet>],
) -> SasResult<DataFrame> {
    let row_count = batch.row_count;
    let mut arrays: Vec<Box<dyn Array>> = Vec::with_capacity(batch.columns.len());
    // Target dtype per column, in column order. A temporal column whose values do
    // not all fit a whole i64 (e.g. sub-second datetimes) is widened to an F64
    // buffer of RAW SAS-epoch units; we must still emit it as the declared
    // temporal dtype (below) rather than a bare Float64, or Polars reinterprets
    // the raw seconds/days as the timestamp's i64 payload and yields garbage.
    let field_dtypes: Vec<ArrowDataType> =
        schema.iter_values().map(|field| field.dtype.clone()).collect();

    for (col_idx, col) in batch.columns.into_iter().enumerate() {
        let label_set = label_mapping.get(col_idx).and_then(Option::as_ref);
        if let Some(label_set) = label_set {
            arrays.push(Box::new(owned_column_to_labelled_utf8(col, label_set, row_count)));
            continue;
        }
        let target_dtype = field_dtypes.get(col_idx);
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
                // A genuine Float column stays Float64. A temporal column that was
                // widened to F64 (its values didn't all fit a whole i64) carries RAW
                // SAS-epoch units as floats and must be converted to its declared
                // temporal dtype, preserving sub-second precision.
                match target_dtype {
                    Some(ArrowDataType::Timestamp(PlTimeUnit::Microsecond, tz)) => {
                        let tz = tz.clone();
                        #[allow(
                            clippy::cast_possible_truncation,
                            clippy::cast_precision_loss
                        )]
                        let micros: Vec<i64> = values
                            .iter()
                            .map(|&seconds_since_sas| {
                                ((seconds_since_sas
                                    - SasDateTime::SECONDS_SAS_TO_UNIX as f64)
                                    * 1_000_000.0)
                                    .round() as i64
                            })
                            .collect();
                        Box::new(primitive_array_with_optional_validity(
                            ArrowDataType::Timestamp(PlTimeUnit::Microsecond, tz),
                            micros,
                            valid,
                            row_count,
                        ))
                    }
                    Some(ArrowDataType::Date32) => {
                        #[allow(clippy::cast_possible_truncation)]
                        let days: Vec<i32> = values
                            .iter()
                            .map(|&days_since_sas| {
                                days_since_sas.round() as i32 - SasDate::DAYS_SAS_TO_UNIX
                            })
                            .collect();
                        Box::new(primitive_array_with_optional_validity(
                            ArrowDataType::Date32,
                            days,
                            valid,
                            row_count,
                        ))
                    }
                    Some(ArrowDataType::Time64(PlTimeUnit::Nanosecond)) => {
                        #[allow(
                            clippy::cast_possible_truncation,
                            clippy::cast_precision_loss
                        )]
                        let nanos: Vec<i64> = values
                            .iter()
                            .map(|&seconds_since_midnight| {
                                (seconds_since_midnight * 1_000_000_000.0).round() as i64
                            })
                            .collect();
                        Box::new(primitive_array_with_optional_validity(
                            ArrowDataType::Time64(PlTimeUnit::Nanosecond),
                            nanos,
                            valid,
                            row_count,
                        ))
                    }
                    _ => Box::new(primitive_array_with_optional_validity(
                        ArrowDataType::Float64,
                        values,
                        valid,
                        row_count,
                    )),
                }
            }
            OwnedColumnBuffer::Date { values, valid } => {
                // Arrow Date32 counts days from the Unix epoch (1970), not the SAS epoch (1960).
                let mut i32s: Vec<i32> = bytemuck::cast_vec(values);
                for v in &mut i32s {
                    *v -= SasDate::DAYS_SAS_TO_UNIX;
                }
                Box::new(primitive_array_with_optional_validity(
                    ArrowDataType::Date32,
                    i32s,
                    valid,
                    row_count,
                ))
            }
            OwnedColumnBuffer::DateTime { values, valid } => {
                // Arrow timestamps count from the Unix epoch (1970), not the SAS epoch (1960).
                // Emit MICROSECONDS (not seconds): Polars has no Second time unit, so a
                // Timestamp(Second) array materializes as Datetime('ms') and clashes with the
                // declared Datetime('us') schema when batches are stacked. Microseconds keep
                // the declared schema and the materialized batches identical.
                let i64s: Vec<i64> = bytemuck::cast_vec::<_, i64>(values)
                    .into_iter()
                    .map(|seconds| {
                        (seconds - SasDateTime::SECONDS_SAS_TO_UNIX) * 1_000_000
                    })
                    .collect();
                let dtype = ArrowDataType::Timestamp(PlTimeUnit::Microsecond, None);
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
fn is_valid_bit(valid: Option<&[u64]>, i: usize) -> bool {
    valid.is_none_or(|bits| {
        bits.get(i / 64)
            .is_some_and(|w| (w >> (i % 64)) & 1 == 1)
    })
}

#[cfg(feature = "arrow")]
fn owned_column_to_labelled_utf8(
    col: OwnedColumnBuffer,
    label_set: &LabelSet,
    row_count: usize,
) -> Utf8Array<i64> {
    let mut offsets: Vec<i64> = Vec::with_capacity(row_count + 1);
    // Labels are short category names; pre-size to avoid repeated reallocation as
    // `data` grows. A modest per-row estimate covers the common case.
    let mut data: Vec<u8> = Vec::with_capacity(row_count.saturating_mul(16));
    let mut validity = MutableBitmap::with_capacity(row_count);
    offsets.push(0);

    // Emits one row. For valid rows `$emit` appends the label bytes to `data`
    // (a borrowed `&str` for a label hit, or a formatted value for a miss) — no
    // per-cell `String` is allocated. For null rows the label is not computed at
    // all and the previous offset is repeated.
    macro_rules! push_cell {
        ($is_valid:expr, $emit:block) => {{
            if $is_valid {
                $emit
                offsets.push(i64::try_from(data.len()).expect("string data exceeds i64::MAX"));
                validity.push(true);
            } else {
                offsets.push(*offsets.last().unwrap_or(&0));
                validity.push(false);
            }
        }};
    }

    match col {
        OwnedColumnBuffer::F64 { values, valid } => {
            for (i, v) in values.iter().enumerate() {
                push_cell!(is_valid_bit(valid.as_deref(), i), {
                    match label_set.lookup_numeric(*v) {
                        Some(label) => data.extend_from_slice(label.as_bytes()),
                        None => {
                            let _ = write!(data, "{v}");
                        }
                    }
                });
            }
        }
        OwnedColumnBuffer::I64 { values, valid } => {
            for (i, v) in values.iter().enumerate() {
                push_cell!(is_valid_bit(valid.as_deref(), i), {
                    // SAS categorical codes are small integers; i64→f64 is exact for |v| ≤ 2^53.
                    #[allow(clippy::cast_precision_loss)]
                    let hit = label_set.lookup_numeric(*v as f64);
                    match hit {
                        Some(label) => data.extend_from_slice(label.as_bytes()),
                        None => {
                            let _ = write!(data, "{v}");
                        }
                    }
                });
            }
        }
        OwnedColumnBuffer::I32 { values, valid } => {
            for (i, v) in values.iter().enumerate() {
                push_cell!(is_valid_bit(valid.as_deref(), i), {
                    match label_set.lookup_numeric(f64::from(*v)) {
                        Some(label) => data.extend_from_slice(label.as_bytes()),
                        None => {
                            let _ = write!(data, "{v}");
                        }
                    }
                });
            }
        }
        OwnedColumnBuffer::Utf8 {
            offsets: src_offsets,
            data: src_data,
            valid,
            ..
        } => {
            let src_offs = src_offsets.into_inner();
            for i in 0..row_count {
                push_cell!(is_valid_bit(valid.as_deref(), i), {
                    // Arrow Offsets<i64> guarantees non-negative, monotonically-increasing values.
                    let start = usize::try_from(src_offs[i]).expect("Arrow offset must be non-negative");
                    let end = usize::try_from(src_offs[i + 1]).expect("Arrow offset must be non-negative");
                    let s = std::str::from_utf8(&src_data[start..end]).unwrap_or("");
                    let label = label_set.lookup_string(s).unwrap_or(s);
                    data.extend_from_slice(label.as_bytes());
                });
            }
        }
        _ => {
            for _ in 0..row_count {
                offsets.push(*offsets.last().unwrap_or(&0));
                validity.push(false);
            }
        }
    }

    let bitmap = validity.into();
    // SAFETY: offsets are built by appending data.len() after each extend (monotonically
    // non-decreasing) and zero-copying for null rows, so Arrow offset invariants hold.
    let offs_buf = unsafe { Offsets::new_unchecked(offsets).into() };
    // SAFETY: all label strings come from &str literals or format!() — valid UTF-8.
    unsafe { Utf8Array::<i64>::new_unchecked(ArrowDataType::LargeUtf8, offs_buf, data.into(), Some(bitmap)) }
}

#[cfg(feature = "arrow")]
fn trusted_large_offsets_buffer(offsets: TrustedOffsets, data_len: usize) -> OffsetsBuffer<i64> {
    offsets.debug_assert_valid_for_values_len(data_len);

    // SAFETY:
    // `TrustedOffsets` is only produced by the scanner's variable-width builders in
    // `sas7bdat`. Those builders:
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
        ArrowSchemaDataType::Timestamp(ArrowSchemaTimeUnit::Microsecond, None) => {
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
                ArrowSchemaTimeUnit::Microsecond,
                None
            ))
            .expect("timestamp"),
            ArrowDataType::Timestamp(PlTimeUnit::Microsecond, None)
        );
    }

    // §2.3 — Catalog + Polars label conversion tests.
    // These test owned_batch_to_dataframe with a label_mapping at the Rust level,
    // without requiring a Python interpreter.

    fn make_f64_batch(values: Vec<f64>, valid: Option<Vec<u64>>) -> sas7bdat::OwnedColumnarBatch {
        let row_count = values.len();
        sas7bdat::OwnedColumnarBatch {
            row_base: sas7bdat::RowIndex(0),
            row_count,
            columns: vec![sas7bdat::OwnedColumnBuffer::F64 { values, valid }],
        }
    }

    fn make_single_col_schema(name: &str, dtype: ArrowDataType) -> Arc<ArrowSchema> {
        Arc::new(
            ArrowSchema::from_iter_check_duplicates(vec![Field::new(
                name.into(),
                dtype,
                true,
            )])
            .expect("schema"),
        )
    }

    fn make_label_set(pairs: &[(f64, &str)]) -> LabelSet {
        let mut ls = sas7bdat::LabelSet::new(
            "TEST".to_owned(),
            sas7bdat::ValueType::Numeric,
        );
        for (k, v) in pairs {
            ls.labels.push(sas7bdat::ValueLabel {
                key: sas7bdat::ValueKey::Numeric(*k),
                label: (*v).to_owned(),
            });
        }
        ls
    }

    #[test]
    fn labelled_f64_column_becomes_utf8_with_correct_strings() {
        let label_set = make_label_set(&[(1.0, "Male"), (2.0, "Female")]);
        let batch = make_f64_batch(vec![1.0, 2.0], None);
        let schema = make_single_col_schema("gender", ArrowDataType::LargeUtf8);

        let df = owned_batch_to_dataframe(batch, schema, &[Some(label_set)])
            .expect("batch to dataframe");

        assert_eq!(df.height(), 2);
        let series = df.column("gender").expect("gender column");
        assert!(
            matches!(series.dtype(), polars::prelude::DataType::String),
            "expected String dtype, got {:?}",
            series.dtype()
        );
        let strs: Vec<Option<&str>> = series.str().expect("str chunked").into_iter().collect();
        assert_eq!(strs, vec![Some("Male"), Some("Female")]);
    }

    #[test]
    fn unlabelled_value_falls_back_to_stringified_number() {
        let label_set = make_label_set(&[(1.0, "Male")]);
        let batch = make_f64_batch(vec![1.0, 99.0], None);
        let schema = make_single_col_schema("code", ArrowDataType::LargeUtf8);

        let df = owned_batch_to_dataframe(batch, schema, &[Some(label_set)])
            .expect("batch to dataframe");

        let strs: Vec<Option<&str>> = df
            .column("code")
            .expect("code")
            .str()
            .expect("str")
            .into_iter()
            .collect();
        assert_eq!(strs[0], Some("Male"));
        assert!(strs[1].is_some(), "unlabelled value should be non-null");
        assert!(
            strs[1].unwrap().contains("99"),
            "unlabelled value should contain the raw number"
        );
    }

    #[test]
    fn null_row_stays_null_after_label_mapping() {
        let label_set = make_label_set(&[(1.0, "Male"), (2.0, "Female")]);
        // row 1 (index 1) is null: valid bit 0 for bit-1
        let valid: Vec<u64> = vec![0b0000_0101]; // bits 0 and 2 set → rows 0 and 2 valid
        let batch = make_f64_batch(vec![1.0, 0.0, 2.0], Some(valid));
        let schema = make_single_col_schema("gender", ArrowDataType::LargeUtf8);

        let df = owned_batch_to_dataframe(batch, schema, &[Some(label_set)])
            .expect("batch to dataframe");

        let strs: Vec<Option<&str>> = df
            .column("gender")
            .expect("gender")
            .str()
            .expect("str")
            .into_iter()
            .collect();
        assert_eq!(strs[0], Some("Male"));
        assert_eq!(strs[1], None, "null row should remain null");
        assert_eq!(strs[2], Some("Female"));
    }

    #[test]
    fn empty_label_mapping_passes_numeric_column_through_unchanged() {
        let batch = make_f64_batch(vec![1.0, 2.0], None);
        let schema = make_single_col_schema("x", ArrowDataType::Float64);

        let df = owned_batch_to_dataframe(batch, schema, &[])
            .expect("batch to dataframe");

        assert!(
            matches!(df.column("x").expect("x").dtype(), polars::prelude::DataType::Float64),
            "unlabelled F64 column should remain Float64"
        );
    }
}
