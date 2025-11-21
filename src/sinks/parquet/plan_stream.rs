use std::borrow::Cow;

use parquet::data_type::{ByteArrayType, DoubleType, Int32Type, Int64Type};
use parquet::file::writer::SerializedColumnWriter;

use crate::error::{Error, Result};
use crate::parser::{
    ColumnMajorColumnView, ColumnarColumn, MaterializedUtf8Column, StagedUtf8Value, sas_days_to_datetime,
    sas_seconds_to_datetime, sas_seconds_to_time,
};

use super::constants::SECONDS_PER_DAY;
use super::plan::{ColumnPlan, ColumnValueEncoder, ColumnValues};
use super::stream::stream_numeric;

fn measure_encoder<F>(_: &str, block: F) -> Result<()>
where
    F: FnOnce() -> Result<()>,
{
    block()
}

impl ColumnPlan {
    #[allow(clippy::too_many_lines)]
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
                        let days = f64::from_bits(bits);
                        let datetime =
                            sas_days_to_datetime(days).ok_or_else(|| Error::InvalidMetadata {
                                details: Cow::Owned(format!(
                                    "column '{}' contains date outside supported range",
                                    self.name
                                )),
                            })?;
                        let seconds = datetime.unix_timestamp();
                        let day = seconds.div_euclid(SECONDS_PER_DAY);
                        let day = i32::try_from(day).map_err(|_| Error::InvalidMetadata {
                            details: Cow::Owned(format!(
                                "column '{}' contains date outside Parquet range",
                                self.name
                            )),
                        })?;
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
                        let seconds = f64::from_bits(bits);
                        let datetime = sas_seconds_to_datetime(seconds).ok_or_else(|| {
                            Error::InvalidMetadata {
                                details: Cow::Owned(format!(
                                    "column '{}' contains timestamp outside supported range",
                                    self.name
                                )),
                            }
                        })?;
                        let micros = datetime.unix_timestamp_nanos().div_euclid(1_000);
                        let micros = i64::try_from(micros).map_err(|_| Error::InvalidMetadata {
                            details: Cow::Owned(format!(
                                "column '{}' contains timestamp outside Parquet range",
                                self.name
                            )),
                        })?;
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
                        let seconds = f64::from_bits(bits);
                        let duration =
                            sas_seconds_to_time(seconds).ok_or_else(|| Error::InvalidMetadata {
                                details: Cow::Owned(format!(
                                    "column '{}' contains time outside supported range",
                                    self.name
                                )),
                            })?;
                        let micros = duration.whole_microseconds();
                        let micros = i64::try_from(micros).map_err(|_| Error::InvalidMetadata {
                            details: Cow::Owned(format!(
                                "column '{}' contains time outside Parquet range",
                                self.name
                            )),
                        })?;
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

    #[allow(clippy::too_many_lines)]
    pub(super) fn stream_columnar(
        &mut self,
        mut column_writer: SerializedColumnWriter<'_>,
        column: &ColumnarColumn<'_, '_>,
        chunk_rows: usize,
    ) -> Result<()> {
        let chunk = chunk_rows.max(1);
        match (&mut self.values, self.encoder) {
            (ColumnValues::Double(values), ColumnValueEncoder::Double) => {
                measure_encoder("parquet::stream_columnar::double", || {
                    let writer = column_writer.typed::<DoubleType>();
                    stream_numeric(
                        &mut self.def_levels,
                        column.len(),
                        |start, len| column.iter_numeric_bits_range(start, len),
                        chunk,
                        values,
                        |bits| Ok(f64::from_bits(bits)),
                        |vals, defs| writer.write_batch(vals, Some(defs), None),
                    )?;
                    Ok(())
                })?;
            }
            (ColumnValues::Int32(values), ColumnValueEncoder::Date) => {
                measure_encoder("parquet::stream_columnar::date", || {
                    let writer = column_writer.typed::<Int32Type>();
                    let column_name = self.name.clone();
                    stream_numeric(
                        &mut self.def_levels,
                        column.len(),
                        |start, len| column.iter_numeric_bits_range(start, len),
                        chunk,
                        values,
                        |bits| {
                            let days = f64::from_bits(bits);
                            let datetime =
                                sas_days_to_datetime(days).ok_or_else(|| Error::InvalidMetadata {
                                    details: Cow::Owned(format!(
                                        "column '{column_name}' contains date outside supported range"
                                    )),
                                })?;
                            let seconds = datetime.unix_timestamp();
                            let day = seconds.div_euclid(SECONDS_PER_DAY);
                            let day = i32::try_from(day).map_err(|_| Error::InvalidMetadata {
                                details: Cow::Owned(format!(
                                    "column '{column_name}' contains date outside Parquet range"
                                )),
                            })?;
                            Ok(day)
                        },
                        |vals, defs| writer.write_batch(vals, Some(defs), None),
                    )?;
                    Ok(())
                })?;
            }
            (ColumnValues::Int64(values), ColumnValueEncoder::DateTime) => {
                measure_encoder("parquet::stream_columnar::datetime", || {
                    let writer = column_writer.typed::<Int64Type>();
                    let column_name = self.name.clone();
                    stream_numeric(
                        &mut self.def_levels,
                        column.len(),
                        |start, len| column.iter_numeric_bits_range(start, len),
                        chunk,
                        values,
                        |bits| {
                            let seconds = f64::from_bits(bits);
                            let datetime = sas_seconds_to_datetime(seconds).ok_or_else(|| {
                                Error::InvalidMetadata {
                                    details: Cow::Owned(format!(
                                        "column '{column_name}' contains timestamp outside supported range"
                                    )),
                                }
                            })?;
                            let micros = datetime.unix_timestamp_nanos().div_euclid(1_000);
                            let micros =
                                i64::try_from(micros).map_err(|_| Error::InvalidMetadata {
                                    details: Cow::Owned(format!(
                                        "column '{column_name}' contains timestamp outside Parquet range"
                                    )),
                                })?;
                            Ok(micros)
                        },
                        |vals, defs| writer.write_batch(vals, Some(defs), None),
                    )?;
                    Ok(())
                })?;
            }
            (ColumnValues::Int64(values), ColumnValueEncoder::Time) => {
                measure_encoder("parquet::stream_columnar::time", || {
                    let writer = column_writer.typed::<Int64Type>();
                    let column_name = self.name.clone();
                    stream_numeric(
                        &mut self.def_levels,
                        column.len(),
                        |start, len| column.iter_numeric_bits_range(start, len),
                        chunk,
                        values,
                        |bits| {
                            let seconds = f64::from_bits(bits);
                            let duration =
                                sas_seconds_to_time(seconds).ok_or_else(|| Error::InvalidMetadata {
                                    details: Cow::Owned(format!(
                                        "column '{column_name}' contains time outside supported range"
                                    )),
                                })?;
                            let micros = duration.whole_microseconds();
                            let micros =
                                i64::try_from(micros).map_err(|_| Error::InvalidMetadata {
                                    details: Cow::Owned(format!(
                                        "column '{column_name}' contains time outside Parquet range"
                                    )),
                                })?;
                            Ok(micros)
                        },
                        |vals, defs| writer.write_batch(vals, Some(defs), None),
                    )?;
                    Ok(())
                })?;
            }
            (ColumnValues::ByteArray(values), ColumnValueEncoder::Utf8) => {
                measure_encoder("parquet::stream_columnar::utf8", || {
                    let writer = column_writer.typed::<ByteArrayType>();
                    let total = column.len();
                    let mut processed = 0;
                    let scratch = self
                        .utf8_scratch
                        .as_mut()
                        .expect("utf8 scratch missing for UTF-8 encoder");
                    while processed < total {
                        let take = (total - processed).min(chunk);
                        self.def_levels.clear();
                        values.clear();
                        for maybe_text in column.iter_strings_range(processed, take) {
                            if let Some(text) = maybe_text {
                                self.def_levels.push(1);
                                values.push(scratch.intern_str(text.as_ref()));
                            } else {
                                self.def_levels.push(0);
                            }
                        }
                        writer.write_batch(values, Some(&self.def_levels), None)?;
                        processed += take;
                    }
                    Ok(())
                })?;
            }
            _ => {
                return Err(Error::Parquet {
                    details: Cow::from("unsupported column encoding during streaming"),
                });
            }
        }
        column_writer.close()?;
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
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

    #[allow(clippy::too_many_lines)]
    pub(super) fn stream_column_major(
        &mut self,
        column: &ColumnMajorColumnView<'_>,
        chunk_rows: usize,
        mut column_writer: SerializedColumnWriter<'_>,
    ) -> Result<()> {
        let chunk = chunk_rows.max(1);
        match (&mut self.values, self.encoder) {
            (ColumnValues::Double(values), ColumnValueEncoder::Double) => {
                let writer = column_writer.typed::<DoubleType>();
                stream_numeric(
                    &mut self.def_levels,
                    column.len(),
                    |start, len| column.iter_numeric_bits_range(start, len),
                    chunk,
                    values,
                    |bits| Ok(f64::from_bits(bits)),
                    |vals, defs| writer.write_batch(vals, Some(defs), None),
                )?;
            }
            (ColumnValues::Int32(values), ColumnValueEncoder::Date) => {
                let writer = column_writer.typed::<Int32Type>();
                let column_name = self.name.clone();
                stream_numeric(
                    &mut self.def_levels,
                    column.len(),
                    |start, len| column.iter_numeric_bits_range(start, len),
                    chunk,
                    values,
                    |bits| {
                        let days = f64::from_bits(bits);
                        let datetime =
                            sas_days_to_datetime(days).ok_or_else(|| Error::InvalidMetadata {
                                details: Cow::Owned(format!(
                                    "column '{column_name}' contains date outside supported range"
                                )),
                            })?;
                        let seconds = datetime.unix_timestamp();
                        let day = seconds.div_euclid(SECONDS_PER_DAY);
                        let day = i32::try_from(day).map_err(|_| Error::InvalidMetadata {
                            details: Cow::Owned(format!(
                                "column '{column_name}' contains date outside Parquet range"
                            )),
                        })?;
                        Ok(day)
                    },
                    |vals, defs| writer.write_batch(vals, Some(defs), None),
                )?;
            }
            (ColumnValues::Int64(values), ColumnValueEncoder::DateTime) => {
                let writer = column_writer.typed::<Int64Type>();
                let column_name = self.name.clone();
                stream_numeric(
                    &mut self.def_levels,
                    column.len(),
                    |start, len| column.iter_numeric_bits_range(start, len),
                    chunk,
                    values,
                    |bits| {
                        let seconds = f64::from_bits(bits);
                        let datetime = sas_seconds_to_datetime(seconds).ok_or_else(|| Error::InvalidMetadata {
                            details: Cow::Owned(format!("column '{column_name}' contains timestamp outside supported range")),
                        })?;
                        let micros = datetime.unix_timestamp_nanos().div_euclid(1_000);
                        let micros = i64::try_from(micros).map_err(|_| Error::InvalidMetadata {
                            details: Cow::Owned(format!(
                                "column '{column_name}' contains timestamp outside Parquet range"
                            )),
                        })?;
                        Ok(micros)
                    },
                    |vals, defs| writer.write_batch(vals, Some(defs), None),
                )?;
            }
            (ColumnValues::Int64(values), ColumnValueEncoder::Time) => {
                let writer = column_writer.typed::<Int64Type>();
                let column_name = self.name.clone();
                stream_numeric(
                    &mut self.def_levels,
                    column.len(),
                    |start, len| column.iter_numeric_bits_range(start, len),
                    chunk,
                    values,
                    |bits| {
                        let seconds = f64::from_bits(bits);
                        let duration =
                            sas_seconds_to_time(seconds).ok_or_else(|| Error::InvalidMetadata {
                                details: Cow::Owned(format!(
                                    "column '{column_name}' contains time outside supported range"
                                )),
                            })?;
                        let micros = duration.whole_microseconds();
                        let micros = i64::try_from(micros).map_err(|_| Error::InvalidMetadata {
                            details: Cow::Owned(format!(
                                "column '{column_name}' contains time outside Parquet range"
                            )),
                        })?;
                        Ok(micros)
                    },
                    |vals, defs| writer.write_batch(vals, Some(defs), None),
                )?;
            }
            (ColumnValues::ByteArray(values), ColumnValueEncoder::Utf8) => {
                let writer = column_writer.typed::<ByteArrayType>();
                let total = column.len();
                let mut processed = 0;
                while processed < total {
                    let take = (total - processed).min(chunk);
                    self.def_levels.clear();
                    values.clear();
                    values.reserve(take);
                    for maybe_text in column.iter_strings_range(processed, take) {
                        if let Some(text) = maybe_text {
                            self.def_levels.push(1);
                            values.push(parquet::data_type::ByteArray::from(text.as_ref().as_bytes()));
                        } else {
                            self.def_levels.push(0);
                        }
                    }
                    writer.write_batch(values, Some(&self.def_levels), None)?;
                    processed += take;
                }
            }
            _ => {
                return Err(Error::Parquet {
                    details: Cow::from("unsupported column encoding during streaming"),
                });
            }
        }
        column_writer.close()?;
        Ok(())
    }
}
