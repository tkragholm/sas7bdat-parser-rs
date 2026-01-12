use std::borrow::Cow;

use parquet::data_type::{ByteArrayType, DataType, DoubleType, Int32Type, Int64Type};
use parquet::file::writer::SerializedColumnWriter;

use crate::error::{Error, Result};
use crate::parser::{
    ColumnarColumn, MaterializedUtf8Column, StagedUtf8Value, sas_days_to_datetime,
    sas_seconds_to_datetime, sas_seconds_to_time,
};

use super::constants::SECONDS_PER_DAY;
use super::plan::{ColumnPlan, ColumnValueEncoder, ColumnValues};
use super::stream::{
    StreamNumericCtx, expand_bitmap_to_def_levels, prepare_def_bitmap, stream_numeric,
};

fn measure_encoder<F>(_: &str, block: F) -> Result<()>
where
    F: FnOnce() -> Result<()>,
{
    block()
}

fn stream_numeric_typed<T: DataType, S: NumericColumnSource>(
    def_levels: &mut Vec<i16>,
    def_bitmap: &mut Vec<u8>,
    column_writer: &mut SerializedColumnWriter<'_>,
    column: &S,
    values: &mut Vec<T::T>,
    chunk: usize,
    convert: impl FnMut(u64) -> Result<T::T>,
) -> Result<()> {
    let writer = column_writer.typed::<T>();
    let mut ctx = StreamNumericCtx {
        def_levels,
        def_bitmap,
        values,
        chunk,
    };
    stream_numeric(
        &mut ctx,
        column.len(),
        |start, len| column.iter_numeric_bits_range(start, len),
        convert,
        |vals, defs| writer.write_batch(vals, Some(defs), None),
    )
}

// Helper functions for data type conversions
fn convert_date(bits: u64, column_name: &str) -> Result<i32> {
    let days = f64::from_bits(bits);
    let datetime = sas_days_to_datetime(days).ok_or_else(|| Error::InvalidMetadata {
        details: Cow::Owned(format!(
            "column '{column_name}' contains date outside supported range"
        )),
    })?;
    let seconds = datetime.unix_timestamp();
    let day = seconds.div_euclid(SECONDS_PER_DAY);
    i32::try_from(day).map_err(|_| Error::InvalidMetadata {
        details: Cow::Owned(format!(
            "column '{column_name}' contains date outside Parquet range"
        )),
    })
}

fn convert_datetime(bits: u64, column_name: &str) -> Result<i64> {
    let seconds = f64::from_bits(bits);
    let datetime = sas_seconds_to_datetime(seconds).ok_or_else(|| Error::InvalidMetadata {
        details: Cow::Owned(format!(
            "column '{column_name}' contains timestamp outside supported range"
        )),
    })?;
    let micros = datetime.unix_timestamp_nanos().div_euclid(1_000);
    i64::try_from(micros).map_err(|_| Error::InvalidMetadata {
        details: Cow::Owned(format!(
            "column '{column_name}' contains timestamp outside Parquet range"
        )),
    })
}

fn convert_time(bits: u64, column_name: &str) -> Result<i64> {
    let seconds = f64::from_bits(bits);
    let duration = sas_seconds_to_time(seconds).ok_or_else(|| Error::InvalidMetadata {
        details: Cow::Owned(format!(
            "column '{column_name}' contains time outside supported range"
        )),
    })?;
    let micros = duration.whole_microseconds();
    i64::try_from(micros).map_err(|_| Error::InvalidMetadata {
        details: Cow::Owned(format!(
            "column '{column_name}' contains time outside Parquet range"
        )),
    })
}

// Trait to abstract over different column types
trait NumericColumnSource {
    fn len(&self) -> usize;
    fn iter_numeric_bits_range(
        &self,
        start: usize,
        len: usize,
    ) -> Box<dyn Iterator<Item = Option<u64>> + '_>;
}

impl NumericColumnSource for ColumnarColumn<'_, '_> {
    fn len(&self) -> usize {
        self.len()
    }

    fn iter_numeric_bits_range(
        &self,
        start: usize,
        len: usize,
    ) -> Box<dyn Iterator<Item = Option<u64>> + '_> {
        Box::new(self.iter_numeric_bits_range(start, len))
    }
}

impl ColumnPlan {
    // Generic streaming function for numeric types
    fn stream_numeric_column<S: NumericColumnSource>(
        &mut self,
        mut column_writer: SerializedColumnWriter<'_>,
        column: &S,
        chunk: usize,
        encoder_name: &str,
    ) -> Result<()> {
        let column_name = self.name.clone();
        let def_levels = &mut self.def_levels;
        let def_bitmap = &mut self.def_bitmap;

        let result = match (&mut self.values, self.encoder) {
            (ColumnValues::Double(values), ColumnValueEncoder::Double) => {
                measure_encoder(encoder_name, || {
                    stream_numeric_typed::<DoubleType, _>(
                        def_levels,
                        def_bitmap,
                        &mut column_writer,
                        column,
                        values,
                        chunk,
                        |bits| Ok(f64::from_bits(bits)),
                    )
                })
            }
            (ColumnValues::Int32(values), ColumnValueEncoder::Date) => {
                measure_encoder(encoder_name, || {
                    stream_numeric_typed::<Int32Type, _>(
                        def_levels,
                        def_bitmap,
                        &mut column_writer,
                        column,
                        values,
                        chunk,
                        |bits| convert_date(bits, &column_name),
                    )
                })
            }
            (ColumnValues::Int64(values), ColumnValueEncoder::DateTime) => {
                measure_encoder(encoder_name, || {
                    stream_numeric_typed::<Int64Type, _>(
                        def_levels,
                        def_bitmap,
                        &mut column_writer,
                        column,
                        values,
                        chunk,
                        |bits| convert_datetime(bits, &column_name),
                    )
                })
            }
            (ColumnValues::Int64(values), ColumnValueEncoder::Time) => {
                measure_encoder(encoder_name, || {
                    stream_numeric_typed::<Int64Type, _>(
                        def_levels,
                        def_bitmap,
                        &mut column_writer,
                        column,
                        values,
                        chunk,
                        |bits| convert_time(bits, &column_name),
                    )
                })
            }
            _ => Err(Error::Parquet {
                details: Cow::from("unsupported column encoding during streaming"),
            }),
        };
        result?;
        column_writer.close()?;
        Ok(())
    }

    pub(super) fn extend_columnar(&mut self, column: &ColumnarColumn<'_, '_>) -> Result<()> {
        self.def_levels.reserve(column.len());
        match (&mut self.values, self.encoder) {
            (ColumnValues::Double(values), ColumnValueEncoder::Double) => {
                values.reserve(column.len());
                for maybe_bits in column.iter_numeric_bits() {
                    if let Some(bits) = maybe_bits {
                        self.def_levels.push(1);
                        values.push(f64::from_bits(bits));
                    } else {
                        self.def_levels.push(0);
                    }
                }
            }
            (ColumnValues::Int32(values), ColumnValueEncoder::Date) => {
                values.reserve(column.len());
                for maybe_bits in column.iter_numeric_bits() {
                    if let Some(bits) = maybe_bits {
                        let day = convert_date(bits, &self.name)?;
                        self.def_levels.push(1);
                        values.push(day);
                    } else {
                        self.def_levels.push(0);
                    }
                }
            }
            (ColumnValues::Int64(values), ColumnValueEncoder::DateTime) => {
                values.reserve(column.len());
                for maybe_bits in column.iter_numeric_bits() {
                    if let Some(bits) = maybe_bits {
                        let micros = convert_datetime(bits, &self.name)?;
                        self.def_levels.push(1);
                        values.push(micros);
                    } else {
                        self.def_levels.push(0);
                    }
                }
            }
            (ColumnValues::Int64(values), ColumnValueEncoder::Time) => {
                values.reserve(column.len());
                for maybe_bits in column.iter_numeric_bits() {
                    if let Some(bits) = maybe_bits {
                        let micros = convert_time(bits, &self.name)?;
                        self.def_levels.push(1);
                        values.push(micros);
                    } else {
                        self.def_levels.push(0);
                    }
                }
            }
            (ColumnValues::ByteArray(values), ColumnValueEncoder::Utf8) => {
                values.reserve(column.len());
                for maybe_text in column.iter_strings() {
                    if let Some(text) = maybe_text {
                        self.def_levels.push(1);
                        values.push(parquet::data_type::ByteArray::from(text.as_ref()));
                    } else {
                        self.def_levels.push(0);
                    }
                }
            }
            _ => unreachable!("column value encoder mismatch"),
        }
        Ok(())
    }

    pub(super) fn stream_columnar(
        &mut self,
        mut column_writer: SerializedColumnWriter<'_>,
        column: &ColumnarColumn<'_, '_>,
        chunk_rows: usize,
    ) -> Result<()> {
        let chunk = chunk_rows.max(1);

        // Handle UTF-8 separately as it has different logic
        if matches!(
            (&self.values, self.encoder),
            (ColumnValues::ByteArray(_), ColumnValueEncoder::Utf8)
        ) {
            measure_encoder("parquet::stream_columnar::utf8", || {
                let writer = column_writer.typed::<ByteArrayType>();
                let total = column.len();
                let mut processed = 0;
                let scratch = self
                    .utf8_scratch
                    .as_mut()
                    .expect("utf8 scratch missing for UTF-8 encoder");

                if let ColumnValues::ByteArray(values) = &mut self.values {
                    while processed < total {
                        let take = (total - processed).min(chunk);
                        prepare_def_bitmap(&mut self.def_bitmap, take);
                        values.clear();
                        for (idx, maybe_text) in
                            column.iter_strings_range(processed, take).enumerate()
                        {
                            if let Some(text) = maybe_text {
                                let byte = idx >> 3;
                                let bit = idx & 7;
                                self.def_bitmap[byte] |= 1 << bit;
                                values.push(scratch.intern_str(text.as_ref()));
                            }
                        }
                        expand_bitmap_to_def_levels(&mut self.def_levels, &self.def_bitmap, take);
                        writer.write_batch(values, Some(&self.def_levels), None)?;
                        processed += take;
                    }
                }
                Ok(())
            })?;
            column_writer.close()?;
            return Ok(());
        }

        // Handle numeric types using generic function
        self.stream_numeric_column(column_writer, column, chunk, "parquet::stream_columnar")?;
        Ok(())
    }

    pub(super) fn stream_columnar_materialized_utf8(
        &mut self,
        mut column_writer: SerializedColumnWriter<'_>,
        materialized: &MaterializedUtf8Column,
    ) -> Result<()> {
        match (&mut self.values, self.encoder) {
            (ColumnValues::ByteArray(values), ColumnValueEncoder::Utf8) => {
                let scratch = self
                    .utf8_scratch
                    .as_mut()
                    .expect("utf8 scratch missing for UTF-8 encoder");
                let mut dictionary_handles = Vec::with_capacity(materialized.dictionary().len());
                for entry in materialized.dictionary() {
                    dictionary_handles.push(scratch.intern_slice(entry));
                }

                self.def_levels.clear();
                self.def_levels.extend_from_slice(materialized.def_levels());
                values.clear();
                self.utf8_inlines.clear();
                values.reserve(materialized.values().len());
                self.utf8_inlines.reserve(materialized.values().len());

                for value in materialized.values() {
                    match value {
                        StagedUtf8Value::Dictionary(id) => {
                            let handle = dictionary_handles.get(*id as usize).ok_or_else(|| {
                                Error::InvalidMetadata {
                                    details: Cow::Owned(format!(
                                        "dictionary index {id} exceeds staged dictionary for column '{}'",
                                        self.name
                                    )),
                                }
                            })?;
                            values.push(handle.clone());
                        }
                        StagedUtf8Value::Inline(bytes) => {
                            let interned = scratch.intern_slice(bytes);
                            self.utf8_inlines.push(interned.clone());
                            values.push(interned);
                        }
                    }
                }

                measure_encoder("parquet::stream_columnar::utf8_staged", || {
                    let writer = column_writer.typed::<ByteArrayType>();
                    writer.write_batch(values, Some(&self.def_levels), None)?;
                    Ok(())
                })?;

                column_writer.close()?;
                Ok(())
            }
            _ => Err(Error::InvalidMetadata {
                details: Cow::from("materialized UTF-8 column type mismatch"),
            }),
        }
    }
}
