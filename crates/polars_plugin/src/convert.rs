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
    offset::OffsetsBuffer,
    record_batch::RecordBatch as PolarsRecordBatch,
};
#[cfg(feature = "arrow")]
use pyo3::{exceptions::PyValueError, prelude::*, types::PyModule};
#[cfg(feature = "arrow")]
use sas7bdat_simd::{Error, OwnedColumnBuffer, Result as SasResult};
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
            ArrowDataType::Time32(PlTimeUnit::Second)
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
                let bitmap = valid.map(|v| bits_to_bitmap(&v, row_count));
                Box::new(PrimitiveArray::new(
                    ArrowDataType::Int32,
                    values.into(),
                    bitmap,
                ))
            }
            OwnedColumnBuffer::I64 { values, valid } => {
                let bitmap = valid.map(|v| bits_to_bitmap(&v, row_count));
                Box::new(PrimitiveArray::new(
                    ArrowDataType::Int64,
                    values.into(),
                    bitmap,
                ))
            }
            OwnedColumnBuffer::F64 { values, valid } => {
                let bitmap = valid.map(|v| bits_to_bitmap(&v, row_count));
                Box::new(PrimitiveArray::new(
                    ArrowDataType::Float64,
                    values.into(),
                    bitmap,
                ))
            }
            OwnedColumnBuffer::Date { values, valid } => {
                let i32s: Vec<i32> = values.into_iter().map(|d| d.days_since_sas_epoch).collect();
                let bitmap = valid.map(|v| bits_to_bitmap(&v, row_count));
                Box::new(PrimitiveArray::new(
                    ArrowDataType::Date32,
                    i32s.into(),
                    bitmap,
                ))
            }
            OwnedColumnBuffer::DateTime { values, valid } => {
                let i64s: Vec<i64> = values
                    .into_iter()
                    .map(|d| d.seconds_since_sas_epoch)
                    .collect();
                let bitmap = valid.map(|v| bits_to_bitmap(&v, row_count));
                let dtype = ArrowDataType::Timestamp(PlTimeUnit::Second, None);
                Box::new(PrimitiveArray::new(dtype, i64s.into(), bitmap))
            }
            OwnedColumnBuffer::Time { values, valid } => {
                let i32s: Vec<i32> = values
                    .into_iter()
                    .map(|t| i32::try_from(t.seconds_since_midnight).unwrap_or(i32::MAX))
                    .collect();
                let bitmap = valid.map(|v| bits_to_bitmap(&v, row_count));
                let dtype = ArrowDataType::Time32(PlTimeUnit::Second);
                Box::new(PrimitiveArray::new(dtype, i32s.into(), bitmap))
            }
            OwnedColumnBuffer::Utf8 {
                offsets,
                data,
                valid,
                ..
            } => {
                let i64_offs: Vec<i64> = offsets.into_iter().map(i64::from).collect();
                let offs_buf = unsafe { OffsetsBuffer::new_unchecked(i64_offs.into()) };
                let bitmap = valid.map(|v| bits_to_bitmap(&v, row_count));
                Box::new(Utf8Array::<i64>::new(
                    ArrowDataType::LargeUtf8,
                    offs_buf,
                    data.into(),
                    bitmap,
                ))
            }
            OwnedColumnBuffer::RawBytes {
                offsets,
                data,
                valid,
            } => {
                let i64_offs: Vec<i64> = offsets.into_iter().map(i64::from).collect();
                let offs_buf = unsafe { OffsetsBuffer::new_unchecked(i64_offs.into()) };
                let bitmap = valid.map(|v| bits_to_bitmap(&v, row_count));
                Box::new(BinaryArray::<i64>::new(
                    ArrowDataType::LargeBinary,
                    offs_buf,
                    data.into(),
                    bitmap,
                ))
            }
        };
        arrays.push(array);
    }

    let rec = PolarsRecordBatch::try_new(row_count, schema, arrays)
        .map_err(|err| Error::arrow(err.to_string()))?;
    Ok(DataFrame::from(rec))
}

#[cfg(feature = "arrow")]
pub(super) fn bits_to_bitmap(bits: &[u64], len: usize) -> polars_arrow::bitmap::Bitmap {
    let bytes: Vec<u8> = bits.iter().flat_map(|word| word.to_le_bytes()).collect();
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
        ArrowSchemaDataType::Time32(ArrowSchemaTimeUnit::Second) => {
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
