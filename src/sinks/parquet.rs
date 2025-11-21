use std::borrow::Cow;
use std::io::Write;
use std::sync::Arc;

use bytes::Bytes;
use hashbrown::{HashMap, hash_map::RawEntryMut};
use itoa::Buffer as ItoaBuffer;
use parquet::basic::{LogicalType, Repetition, TimeUnit, Type as PhysicalType};
use parquet::data_type::{ByteArray, ByteArrayType, DoubleType, Int32Type, Int64Type};
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::{SerializedColumnWriter, SerializedFileWriter};
use parquet::schema::types::{Type, TypePtr};
use ryu::Buffer as RyuBuffer;

use crate::error::{Error, Result};
use crate::metadata::Variable;
use crate::parser::{
    ColumnInfo, ColumnKind, ColumnMajorBatch, ColumnMajorColumnView, ColumnarBatch, ColumnarColumn,
    MaterializedUtf8Column, NumericKind, StagedUtf8Value, sas_days_to_datetime,
    sas_seconds_to_datetime, sas_seconds_to_time,
};
use crate::sinks::{ColumnarSink, RowSink, SinkContext};
use crate::value::Value;

const SECONDS_PER_DAY: i64 = 86_400;

const DEFAULT_ROW_GROUP_SIZE: usize = 8_192;
const DEFAULT_TARGET_ROW_GROUP_BYTES: usize = 512 * 1024 * 1024;
const MIN_AUTO_ROW_GROUP_ROWS: usize = 1_024;
const MAX_AUTO_ROW_GROUP_ROWS: usize = 262_144;
const UTF8_DICTIONARY_LIMIT: usize = 4_096;

macro_rules! measure_encoder {
    ($name:expr, $block:block) => {{
        let result: Result<()> = $block;
        result
    }};
}

/// Writes decoded SAS rows into a Parquet file.
pub struct ParquetSink<W: Write + Send> {
    output: Option<W>,
    writer: Option<SerializedFileWriter<W>>,
    row_group_size: usize,
    columns: Vec<ColumnPlan>,
    rows_buffered: usize,
    auto_row_group_size: bool,
    target_row_group_bytes: usize,
    streaming_columnar: bool,
}

impl<W: Write + Send> ParquetSink<W> {
    /// Creates a new sink that writes to the supplied writer.
    #[must_use]
    pub const fn new(writer: W) -> Self {
        Self {
            output: Some(writer),
            writer: None,
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            columns: Vec::new(),
            rows_buffered: 0,
            auto_row_group_size: true,
            target_row_group_bytes: DEFAULT_TARGET_ROW_GROUP_BYTES,
            streaming_columnar: false,
        }
    }

    /// Configures the number of rows buffered per Parquet row group.
    #[must_use]
    pub const fn with_row_group_size(mut self, size: usize) -> Self {
        self.row_group_size = size;
        self.auto_row_group_size = false;
        self
    }

    /// Sets the target byte size used when automatically estimating row group size.
    #[must_use]
    pub const fn with_target_row_group_bytes(mut self, bytes: usize) -> Self {
        self.target_row_group_bytes = if bytes == 0 { 1 } else { bytes };
        self.auto_row_group_size = true;
        self
    }

    /// Toggles streaming columnar mode where batches map directly to row groups.
    #[must_use]
    pub const fn with_streaming_columnar(mut self, enabled: bool) -> Self {
        self.streaming_columnar = enabled;
        self
    }

    fn estimate_row_group_size(&self, context: &SinkContext<'_>) -> usize {
        let mut approx_row_bytes = context
            .columns
            .iter()
            .map(|column| usize::try_from(column.offsets.width).unwrap_or(0))
            .sum::<usize>();
        if approx_row_bytes == 0 {
            approx_row_bytes = 1;
        }

        let mut rows = self.target_row_group_bytes.saturating_div(approx_row_bytes);
        if rows == 0 {
            rows = 1;
        }

        if rows > MAX_AUTO_ROW_GROUP_ROWS {
            rows = MAX_AUTO_ROW_GROUP_ROWS;
        } else if rows < MIN_AUTO_ROW_GROUP_ROWS {
            rows = rows.max(1);
        }

        if context.metadata.row_count > 0
            && let Ok(total_rows) = usize::try_from(context.metadata.row_count)
        {
            rows = rows.min(total_rows.max(1));
        }

        rows
    }

    /// Returns the underlying writer once the sink has been finalised.
    ///
    /// # Errors
    ///
    /// Returns an error if the writer has not been finished or if the
    /// internal output has already been taken.
    pub fn into_inner(mut self) -> Result<W> {
        if self.writer.is_some() {
            return Err(Error::Unsupported {
                feature: Cow::from("attempted to take Parquet writer before sink was finished"),
            });
        }
        self.output.take().ok_or_else(|| Error::InvalidMetadata {
            details: Cow::from("Parquet sink output already consumed"),
        })
    }

    fn flush(&mut self) -> Result<()> {
        if self.rows_buffered == 0 {
            return Ok(());
        }

        let writer = self.writer.as_mut().ok_or_else(|| Error::InvalidMetadata {
            details: Cow::from("Parquet sink has not been initialised"),
        })?;
        let mut row_group = writer.next_row_group()?;

        for plan in &mut self.columns {
            let column_writer = row_group.next_column()?.ok_or_else(|| Error::Parquet {
                details: Cow::from("writer returned fewer columns than metadata described"),
            })?;
            plan.flush(column_writer)?;
        }

        // Ensure the row group writer has no dangling columns.
        if row_group.next_column()?.is_some() {
            return Err(Error::Parquet {
                details: Cow::from("writer returned more columns than metadata described"),
            });
        }

        row_group.close()?;
        self.rows_buffered = 0;
        Ok(())
    }
}

impl<W: Write + Send> RowSink for ParquetSink<W> {
    fn begin(&mut self, context: SinkContext<'_>) -> Result<()> {
        if self.writer.is_some() {
            return Err(Error::Unsupported {
                feature: Cow::from("Parquet sink cannot be reused without finishing"),
            });
        }

        if context.metadata.variables.len() != context.columns.len() {
            return Err(Error::InvalidMetadata {
                details: Cow::from("column metadata length mismatch"),
            });
        }

        let mut plans = Vec::with_capacity(context.columns.len());
        let mut fields: Vec<TypePtr> = Vec::with_capacity(context.columns.len());

        if self.auto_row_group_size {
            self.row_group_size = self.estimate_row_group_size(&context);
            self.auto_row_group_size = false;
        }

        for (variable, column) in context
            .metadata
            .variables
            .iter()
            .zip(context.columns.iter())
        {
            let (plan, field) = ColumnPlan::new(variable, column)?;
            fields.push(field);
            plans.push(plan);
        }
        for plan in &mut plans {
            plan.reserve_capacity(self.row_group_size);
        }

        let schema = Type::group_type_builder("schema")
            .with_fields(fields)
            .build()?;
        let schema = Arc::new(schema);

        let props = WriterProperties::builder().build();
        let output = self.output.take().ok_or_else(|| Error::InvalidMetadata {
            details: Cow::from("Parquet sink output already taken"),
        })?;
        let writer = SerializedFileWriter::new(output, schema, props.into())?;

        self.columns = plans;
        self.writer = Some(writer);
        self.rows_buffered = 0;
        Ok(())
    }

    fn write_row(&mut self, row: &[Value<'_>]) -> Result<()> {
        if self.writer.is_none() {
            return Err(Error::Unsupported {
                feature: Cow::from("rows written before Parquet sink initialised"),
            });
        }

        if row.len() != self.columns.len() {
            return Err(Error::InvalidMetadata {
                details: Cow::Owned(format!(
                    "row length {} does not match column count {}",
                    row.len(),
                    self.columns.len()
                )),
            });
        }

        for (value, plan) in row.iter().zip(self.columns.iter_mut()) {
            plan.push(value)?;
        }

        self.rows_buffered = self.rows_buffered.saturating_add(1);

        if self.row_group_size > 0 && self.rows_buffered >= self.row_group_size {
            self.flush()?;
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if self.writer.is_none() {
            return Ok(());
        }

        if self.rows_buffered > 0 {
            self.flush()?;
        }

        if let Some(writer) = self.writer.take() {
            let output = writer.into_inner()?;
            self.output = Some(output);
        }
        self.columns.clear();
        self.rows_buffered = 0;
        Ok(())
    }
}

impl<W: Write + Send> ColumnarSink for ParquetSink<W> {
    fn write_columnar_batch(
        &mut self,
        batch: &ColumnarBatch<'_>,
        selection: &[usize],
    ) -> Result<()> {
        if self.writer.is_none() {
            return Err(Error::Unsupported {
                feature: Cow::from("rows written before Parquet sink initialised"),
            });
        }
        if selection.len() != self.columns.len() {
            return Err(Error::InvalidMetadata {
                details: Cow::from("column selection length does not match sink columns"),
            });
        }

        if self.streaming_columnar {
            self.write_columnar_batch_streaming(batch, selection)?;
            return Ok(());
        }

        for (plan, &source_idx) in self.columns.iter_mut().zip(selection.iter()) {
            let column = batch
                .column(source_idx)
                .ok_or_else(|| Error::InvalidMetadata {
                    details: Cow::Owned(format!(
                        "column selection index {source_idx} exceeds available columns"
                    )),
                })?;
            plan.extend_columnar(&column)?;
        }

        self.rows_buffered = self.rows_buffered.saturating_add(batch.row_count);
        if self.row_group_size > 0 && self.rows_buffered >= self.row_group_size {
            self.flush()?;
        }
        Ok(())
    }

    fn write_column_major_batch(
        &mut self,
        batch: &ColumnMajorBatch<'_>,
        selection: &[usize],
    ) -> Result<()> {
        if self.writer.is_none() {
            return Err(Error::Unsupported {
                feature: Cow::from("rows written before Parquet sink initialised"),
            });
        }
        if !self.streaming_columnar {
            return Err(Error::Unsupported {
                feature: Cow::from("column-major batches require streaming mode"),
            });
        }
        if selection.len() != self.columns.len() {
            return Err(Error::InvalidMetadata {
                details: Cow::from("column selection length does not match sink columns"),
            });
        }

        let chunk_rows = self.streaming_chunk_rows().max(1);
        let writer = self.writer.as_mut().ok_or_else(|| Error::InvalidMetadata {
            details: Cow::from("Parquet sink has not been initialised"),
        })?;

        let mut row_group = writer.next_row_group()?;
        for (plan, &source_idx) in self.columns.iter_mut().zip(selection.iter()) {
            let column = batch
                .column(source_idx)
                .ok_or_else(|| Error::InvalidMetadata {
                    details: Cow::Owned(format!(
                        "column selection index {source_idx} exceeds available columns"
                    )),
                })?;
            let column_writer = row_group.next_column()?.ok_or_else(|| Error::Parquet {
                details: Cow::from("writer returned fewer columns than metadata described"),
            })?;
            plan.stream_column_major(&column, chunk_rows, column_writer)?;
        }

        if row_group.next_column()?.is_some() {
            return Err(Error::Parquet {
                details: Cow::from("writer returned more columns than metadata described"),
            });
        }

        row_group.close()?;
        self.rows_buffered = 0;
        Ok(())
    }
}

impl<W: Write + Send> ParquetSink<W> {
    #[inline]
    const fn streaming_chunk_rows(&self) -> usize {
        // Align streaming chunking to the configured row group size to reduce write_batch calls.
        let rows = self.row_group_size;
        if rows == 0 { 1 } else { rows }
    }

    fn write_columnar_batch_streaming(
        &mut self,
        batch: &ColumnarBatch<'_>,
        selection: &[usize],
    ) -> Result<()> {
        let chunk_rows = self.streaming_chunk_rows().max(1);
        let writer = self.writer.as_mut().ok_or_else(|| Error::InvalidMetadata {
            details: Cow::from("Parquet sink has not been initialised"),
        })?;

        let mut row_group = writer.next_row_group()?;
        for (plan, &source_idx) in self.columns.iter_mut().zip(selection.iter()) {
            let column = batch
                .column(source_idx)
                .ok_or_else(|| Error::InvalidMetadata {
                    details: Cow::Owned(format!(
                        "column selection index {source_idx} exceeds available columns"
                    )),
                })?;
            let column_writer = row_group.next_column()?.ok_or_else(|| Error::Parquet {
                details: Cow::from("writer returned fewer columns than metadata described"),
            })?;
            match plan.encoder {
                ColumnValueEncoder::Utf8 => {
                    if let Some(materialized) = batch.materialize_utf8(source_idx)? {
                        plan.stream_columnar_materialized_utf8(column_writer, &materialized)?;
                    } else {
                        plan.stream_columnar(column_writer, &column, chunk_rows)?;
                    }
                }
                _ => {
                    plan.stream_columnar(column_writer, &column, chunk_rows)?;
                }
            }
        }

        if row_group.next_column()?.is_some() {
            return Err(Error::Parquet {
                details: Cow::from("writer returned more columns than metadata described"),
            });
        }

        row_group.close()?;
        self.rows_buffered = 0;
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum ColumnValueEncoder {
    Double,
    Date,
    DateTime,
    Time,
    Utf8,
}

enum ColumnValues {
    Double(Vec<f64>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    ByteArray(Vec<ByteArray>),
}

struct ColumnPlan {
    name: String,
    encoder: ColumnValueEncoder,
    def_levels: Vec<i16>,
    values: ColumnValues,
    utf8_scratch: Option<Utf8Scratch>,
    utf8_inlines: Vec<ByteArray>,
}

struct Utf8Scratch {
    ryu: RyuBuffer,
    itoa: ItoaBuffer,
    dictionary: HashMap<Vec<u8>, ByteArray>,
    dictionary_enabled: bool,
}

impl Utf8Scratch {
    fn new() -> Self {
        Self {
            ryu: RyuBuffer::new(),
            itoa: ItoaBuffer::new(),
            dictionary: HashMap::new(),
            dictionary_enabled: true,
        }
    }

    fn intern_slice(&mut self, data: &[u8]) -> ByteArray {
        if self.dictionary_enabled && self.dictionary.len() >= UTF8_DICTIONARY_LIMIT {
            self.dictionary.clear();
            self.dictionary_enabled = false;
        }
        if !self.dictionary_enabled {
            return ByteArray::from(Bytes::copy_from_slice(data));
        }
        match self.dictionary.raw_entry_mut().from_key(data) {
            RawEntryMut::Occupied(entry) => entry.get().clone(),
            RawEntryMut::Vacant(vacant) => {
                let stored = ByteArray::from(Bytes::copy_from_slice(data));
                vacant.insert(data.to_vec(), stored.clone());
                stored
            }
        }
    }

    fn intern_str(&mut self, text: &str) -> ByteArray {
        self.intern_slice(text.as_bytes())
    }
}

impl ColumnPlan {
    fn new(variable: &Variable, column: &ColumnInfo) -> Result<(Self, TypePtr)> {
        let (encoder, physical_type, logical_type) = match column.kind {
            ColumnKind::Character => (
                ColumnValueEncoder::Utf8,
                PhysicalType::BYTE_ARRAY,
                Some(LogicalType::String),
            ),
            ColumnKind::Numeric(NumericKind::Double) => {
                (ColumnValueEncoder::Double, PhysicalType::DOUBLE, None)
            }
            ColumnKind::Numeric(NumericKind::Date) => (
                ColumnValueEncoder::Date,
                PhysicalType::INT32,
                Some(LogicalType::Date),
            ),
            ColumnKind::Numeric(NumericKind::DateTime) => (
                ColumnValueEncoder::DateTime,
                PhysicalType::INT64,
                Some(LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: true,
                    unit: TimeUnit::MICROS,
                }),
            ),
            ColumnKind::Numeric(NumericKind::Time) => (
                ColumnValueEncoder::Time,
                PhysicalType::INT64,
                Some(LogicalType::Time {
                    is_adjusted_to_u_t_c: true,
                    unit: TimeUnit::MICROS,
                }),
            ),
        };

        let field = Type::primitive_type_builder(&variable.name, physical_type)
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(logical_type)
            .build()?;

        let plan = Self {
            name: variable.name.clone(),
            encoder,
            def_levels: Vec::new(),
            values: match encoder {
                ColumnValueEncoder::Double => ColumnValues::Double(Vec::new()),
                ColumnValueEncoder::Date => ColumnValues::Int32(Vec::new()),
                ColumnValueEncoder::DateTime | ColumnValueEncoder::Time => {
                    ColumnValues::Int64(Vec::new())
                }
                ColumnValueEncoder::Utf8 => ColumnValues::ByteArray(Vec::new()),
            },
            utf8_scratch: match encoder {
                ColumnValueEncoder::Utf8 => Some(Utf8Scratch::new()),
                _ => None,
            },
            utf8_inlines: Vec::new(),
        };
        Ok((plan, Arc::new(field)))
    }

    fn reserve_capacity(&mut self, capacity: usize) {
        self.def_levels.reserve(capacity);
        match &mut self.values {
            ColumnValues::Double(values) => values.reserve(capacity),
            ColumnValues::Int32(values) => values.reserve(capacity),
            ColumnValues::Int64(values) => values.reserve(capacity),
            ColumnValues::ByteArray(values) => values.reserve(capacity),
        }
    }

    fn push(&mut self, value: &Value<'_>) -> Result<()> {
        match self.encoder {
            ColumnValueEncoder::Double => {
                let coerced = self.coerce_numeric(value)?;
                match &mut self.values {
                    ColumnValues::Double(values) => {
                        Self::push_optional(&mut self.def_levels, values, coerced);
                    }
                    _ => unreachable!("column value encoder mismatch"),
                }
            }
            ColumnValueEncoder::Date => {
                let coerced = self.coerce_date(value)?;
                match &mut self.values {
                    ColumnValues::Int32(values) => {
                        Self::push_optional(&mut self.def_levels, values, coerced);
                    }
                    _ => unreachable!("column value encoder mismatch"),
                }
            }
            ColumnValueEncoder::DateTime => {
                let coerced = self.coerce_timestamp(value)?;
                match &mut self.values {
                    ColumnValues::Int64(values) => {
                        Self::push_optional(&mut self.def_levels, values, coerced);
                    }
                    _ => unreachable!("column value encoder mismatch"),
                }
            }
            ColumnValueEncoder::Time => {
                let coerced = self.coerce_time(value)?;
                match &mut self.values {
                    ColumnValues::Int64(values) => {
                        Self::push_optional(&mut self.def_levels, values, coerced);
                    }
                    _ => unreachable!("column value encoder mismatch"),
                }
            }
            ColumnValueEncoder::Utf8 => {
                let coerced = self.coerce_utf8(value);
                match &mut self.values {
                    ColumnValues::ByteArray(values) => {
                        Self::push_optional(&mut self.def_levels, values, coerced);
                    }
                    _ => unreachable!("column value encoder mismatch"),
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn extend_columnar(&mut self, column: &ColumnarColumn<'_, '_>) -> Result<()> {
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
                        values.push(ByteArray::from(text.as_ref()));
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
    fn stream_columnar(
        &mut self,
        mut column_writer: SerializedColumnWriter<'_>,
        column: &ColumnarColumn<'_, '_>,
        chunk_rows: usize,
    ) -> Result<()> {
        let chunk = chunk_rows.max(1);
        match (&mut self.values, self.encoder) {
            (ColumnValues::Double(values), ColumnValueEncoder::Double) => {
                measure_encoder!("parquet::stream_columnar::double", {
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
                measure_encoder!("parquet::stream_columnar::date", {
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
                measure_encoder!("parquet::stream_columnar::datetime", {
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
                measure_encoder!("parquet::stream_columnar::time", {
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
                measure_encoder!("parquet::stream_columnar::utf8", {
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
    fn stream_columnar_materialized_utf8(
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

                measure_encoder!("parquet::stream_columnar::utf8_staged", {
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

    fn stream_column_major(
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
                            values.push(ByteArray::from(text.as_ref().as_bytes()));
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

    #[inline]
    fn push_optional<T>(def_levels: &mut Vec<i16>, values: &mut Vec<T>, value: Option<T>) {
        match value {
            Some(v) => {
                def_levels.push(1);
                values.push(v);
            }
            None => def_levels.push(0),
        }
    }

    fn flush(&mut self, mut column_writer: SerializedColumnWriter<'_>) -> Result<()> {
        match (&mut self.values, self.encoder) {
            (ColumnValues::Double(values), ColumnValueEncoder::Double) => {
                let writer = column_writer.typed::<DoubleType>();
                writer.write_batch(values, Some(&self.def_levels), None)?;
                values.clear();
            }
            (ColumnValues::Int32(values), ColumnValueEncoder::Date) => {
                let writer = column_writer.typed::<Int32Type>();
                writer.write_batch(values, Some(&self.def_levels), None)?;
                values.clear();
            }
            (
                ColumnValues::Int64(values),
                ColumnValueEncoder::DateTime | ColumnValueEncoder::Time,
            ) => {
                let writer = column_writer.typed::<Int64Type>();
                writer.write_batch(values, Some(&self.def_levels), None)?;
                values.clear();
            }
            (ColumnValues::ByteArray(values), ColumnValueEncoder::Utf8) => {
                let writer = column_writer.typed::<ByteArrayType>();
                writer.write_batch(values, Some(&self.def_levels), None)?;
                values.clear();
            }
            _ => {
                return Err(Error::Parquet {
                    details: Cow::from("unsupported column encoding during flush"),
                });
            }
        }
        self.def_levels.clear();
        column_writer.close()?;
        Ok(())
    }

    fn coerce_numeric(&self, value: &Value<'_>) -> Result<Option<f64>> {
        match value {
            Value::Missing(_) => Ok(None),
            Value::Float(v) => Ok(Some(*v)),
            Value::Int32(v) => Ok(Some(f64::from(*v))),
            Value::Int64(v) => {
                // Only accept integers that are exactly representable as f64 (|v| <= 2^53).
                const MAX_SAFE: i64 = 9_007_199_254_740_992; // 2^53
                const MIN_SAFE: i64 = -9_007_199_254_740_992;
                if *v < MIN_SAFE || *v > MAX_SAFE {
                    return Err(Error::InvalidMetadata {
                        details: Cow::Owned(format!(
                            "column '{}' int64 value {} cannot be represented exactly as f64",
                            self.name, v
                        )),
                    });
                }
                let text = v.to_string();
                let parsed = text.parse::<f64>().map_err(|_| Error::InvalidMetadata {
                    details: Cow::Owned(format!(
                        "column '{}' int64 value '{text}' cannot be parsed as f64",
                        self.name
                    )),
                })?;
                Ok(Some(parsed))
            }
            Value::NumericString(text) | Value::Str(text) => self.parse_f64(text.as_ref()),
            Value::Bytes(bytes) => {
                let text =
                    std::str::from_utf8(bytes.as_ref()).map_err(|_| Error::InvalidMetadata {
                        details: Cow::Owned(format!(
                            "column '{}' received non-UTF8 bytes for numeric sink",
                            self.name
                        )),
                    })?;
                self.parse_f64(text)
            }
            other => Err(self.type_mismatch_error("numeric", other)),
        }
    }

    fn coerce_date(&self, value: &Value<'_>) -> Result<Option<i32>> {
        match value {
            Value::Missing(_) => Ok(None),
            Value::Date(datetime) => {
                let seconds = datetime.unix_timestamp();
                let days = seconds.div_euclid(SECONDS_PER_DAY);
                let days = i32::try_from(days).map_err(|_| Error::InvalidMetadata {
                    details: Cow::Owned(format!(
                        "column '{}' contains date outside Parquet range",
                        self.name
                    )),
                })?;
                Ok(Some(days))
            }
            other => Err(self.type_mismatch_error("date", other)),
        }
    }

    fn coerce_timestamp(&self, value: &Value<'_>) -> Result<Option<i64>> {
        match value {
            Value::Missing(_) => Ok(None),
            Value::DateTime(datetime) => {
                let nanos = datetime.unix_timestamp_nanos();
                let micros = nanos.div_euclid(1_000);
                let micros = i64::try_from(micros).map_err(|_| Error::InvalidMetadata {
                    details: Cow::Owned(format!(
                        "column '{}' contains timestamp outside Parquet range",
                        self.name
                    )),
                })?;
                Ok(Some(micros))
            }
            other => Err(self.type_mismatch_error("timestamp", other)),
        }
    }

    fn coerce_time(&self, value: &Value<'_>) -> Result<Option<i64>> {
        match value {
            Value::Missing(_) => Ok(None),
            Value::Time(duration) => {
                let micros = duration.whole_microseconds();
                let micros = i64::try_from(micros).map_err(|_| Error::InvalidMetadata {
                    details: Cow::Owned(format!(
                        "column '{}' contains time outside Parquet range",
                        self.name
                    )),
                })?;
                Ok(Some(micros))
            }
            other => Err(self.type_mismatch_error("time", other)),
        }
    }

    fn coerce_utf8(&mut self, value: &Value<'_>) -> Option<ByteArray> {
        match value {
            Value::Missing(_) => None,
            Value::Str(text) | Value::NumericString(text) => {
                let scratch = self
                    .utf8_scratch
                    .as_mut()
                    .expect("utf8 scratch missing for UTF-8 encoder");
                Some(scratch.intern_str(text.as_ref()))
            }
            Value::Bytes(bytes) => {
                let scratch = self
                    .utf8_scratch
                    .as_mut()
                    .expect("utf8 scratch missing for UTF-8 encoder");
                Some(scratch.intern_slice(bytes.as_ref()))
            }
            Value::Float(v) => {
                let scratch = self
                    .utf8_scratch
                    .as_mut()
                    .expect("utf8 scratch missing for UTF-8 encoder");
                let owned = scratch.ryu.format(*v).to_owned();
                Some(scratch.intern_slice(owned.as_bytes()))
            }
            Value::Int32(v) => {
                let scratch = self
                    .utf8_scratch
                    .as_mut()
                    .expect("utf8 scratch missing for UTF-8 encoder");
                let owned = scratch.itoa.format(*v).to_owned();
                Some(scratch.intern_slice(owned.as_bytes()))
            }
            Value::Int64(v) => {
                let scratch = self
                    .utf8_scratch
                    .as_mut()
                    .expect("utf8 scratch missing for UTF-8 encoder");
                let owned = scratch.itoa.format(*v).to_owned();
                Some(scratch.intern_slice(owned.as_bytes()))
            }
            Value::DateTime(datetime) => {
                let scratch = self
                    .utf8_scratch
                    .as_mut()
                    .expect("utf8 scratch missing for UTF-8 encoder");
                let text = datetime.to_string();
                Some(scratch.intern_str(&text))
            }
            Value::Date(datetime) => {
                let scratch = self
                    .utf8_scratch
                    .as_mut()
                    .expect("utf8 scratch missing for UTF-8 encoder");
                let text = datetime.date().to_string();
                Some(scratch.intern_str(&text))
            }
            Value::Time(duration) => {
                let scratch = self
                    .utf8_scratch
                    .as_mut()
                    .expect("utf8 scratch missing for UTF-8 encoder");
                let text = duration.to_string();
                Some(scratch.intern_str(&text))
            }
        }
    }

    fn parse_f64(&self, text: &str) -> Result<Option<f64>> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        trimmed
            .parse::<f64>()
            .map(Some)
            .map_err(|_| Error::InvalidMetadata {
                details: Cow::Owned(format!(
                    "column '{}' value '{trimmed}' cannot be parsed as f64",
                    self.name
                )),
            })
    }

    fn type_mismatch_error(&self, expected: &str, value: &Value<'_>) -> Error {
        Error::InvalidMetadata {
            details: Cow::Owned(format!(
                "column '{}' expected {expected} value but received {value:?}",
                self.name
            )),
        }
    }
}

fn stream_numeric<T, F, W, P, I>(
    def_levels: &mut Vec<i16>,
    total_len: usize,
    mut iter_provider: P,
    chunk: usize,
    values: &mut Vec<T>,
    mut map_value: F,
    mut write_chunk: W,
) -> Result<()>
where
    P: FnMut(usize, usize) -> I,
    I: Iterator<Item = Option<u64>>,
    F: FnMut(u64) -> Result<T>,
    W: FnMut(&[T], &[i16]) -> std::result::Result<usize, ParquetError>,
{
    let total = total_len;
    let mut processed = 0;
    while processed < total {
        let take = (total - processed).min(chunk);
        def_levels.clear();
        values.clear();
        values.reserve(take);
        for maybe_bits in iter_provider(processed, take) {
            if let Some(bits) = maybe_bits {
                def_levels.push(1);
                let value = map_value(bits)?;
                values.push(value);
            } else {
                def_levels.push(0);
            }
        }
        write_chunk(values, def_levels).map_err(Error::from)?;
        processed += take;
    }
    Ok(())
}
