use std::borrow::Cow;
use std::io::Write;
use std::sync::Arc;

use itoa::Buffer as ItoaBuffer;
use parquet::basic::{LogicalType, Repetition, TimeUnit, Type as PhysicalType};
use parquet::data_type::{ByteArray, ByteArrayType, DoubleType, Int32Type, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::{SerializedColumnWriter, SerializedFileWriter};
use parquet::schema::types::{Type, TypePtr};
use ryu::Buffer as RyuBuffer;

use crate::error::{Error, Result};
use crate::metadata::Variable;
use crate::parser::{ColumnInfo, ColumnKind, NumericKind};
use crate::sinks::{RowSink, SinkContext};
use crate::value::Value;

const SECONDS_PER_DAY: i64 = 86_400;

const DEFAULT_ROW_GROUP_SIZE: usize = 8_192;
const DEFAULT_TARGET_ROW_GROUP_BYTES: usize = 256 * 1024 * 1024;
const MIN_AUTO_ROW_GROUP_ROWS: usize = 1_024;
const MAX_AUTO_ROW_GROUP_ROWS: usize = 131_072;

/// Writes decoded SAS rows into a Parquet file.
pub struct ParquetSink<W: Write + Send> {
    output: Option<W>,
    writer: Option<SerializedFileWriter<W>>,
    row_group_size: usize,
    columns: Vec<ColumnPlan>,
    rows_buffered: usize,
    auto_row_group_size: bool,
    target_row_group_bytes: usize,
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
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
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

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
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
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
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

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
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

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
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
}

struct Utf8Scratch {
    ryu: RyuBuffer,
    itoa: ItoaBuffer,
}

impl Utf8Scratch {
    fn new() -> Self {
        Self {
            ryu: RyuBuffer::new(),
            itoa: ItoaBuffer::new(),
        }
    }
}

impl ColumnPlan {
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
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

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn push(&mut self, value: &Value<'_>) -> Result<()> {
        match self.encoder {
            ColumnValueEncoder::Double => {
                let coerced = self.coerce_numeric(value)?;
                match (&mut self.values, coerced) {
                    (ColumnValues::Double(values), Some(v)) => {
                        self.def_levels.push(1);
                        values.push(v);
                    }
                    (ColumnValues::Double(_), None) => {
                        self.def_levels.push(0);
                    }
                    _ => unreachable!("column value encoder mismatch"),
                }
            }
            ColumnValueEncoder::Date => {
                let coerced = self.coerce_date(value)?;
                match (&mut self.values, coerced) {
                    (ColumnValues::Int32(values), Some(v)) => {
                        self.def_levels.push(1);
                        values.push(v);
                    }
                    (ColumnValues::Int32(_), None) => {
                        self.def_levels.push(0);
                    }
                    _ => unreachable!("column value encoder mismatch"),
                }
            }
            ColumnValueEncoder::DateTime => {
                let coerced = self.coerce_timestamp(value)?;
                match (&mut self.values, coerced) {
                    (ColumnValues::Int64(values), Some(v)) => {
                        self.def_levels.push(1);
                        values.push(v);
                    }
                    (ColumnValues::Int64(_), None) => {
                        self.def_levels.push(0);
                    }
                    _ => unreachable!("column value encoder mismatch"),
                }
            }
            ColumnValueEncoder::Time => {
                let coerced = self.coerce_time(value)?;
                match (&mut self.values, coerced) {
                    (ColumnValues::Int64(values), Some(v)) => {
                        self.def_levels.push(1);
                        values.push(v);
                    }
                    (ColumnValues::Int64(_), None) => {
                        self.def_levels.push(0);
                    }
                    _ => unreachable!("column value encoder mismatch"),
                }
            }
            ColumnValueEncoder::Utf8 => {
                let coerced = self.coerce_utf8(value);
                match (&mut self.values, coerced) {
                    (ColumnValues::ByteArray(values), Some(bytes)) => {
                        self.def_levels.push(1);
                        values.push(bytes);
                    }
                    (ColumnValues::ByteArray(_), None) => {
                        self.def_levels.push(0);
                    }
                    _ => unreachable!("column value encoder mismatch"),
                }
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
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
            Value::Str(text) | Value::NumericString(text) => Some(ByteArray::from(text.as_ref())),
            Value::Bytes(bytes) => Some(ByteArray::from(bytes.as_ref())),
            Value::Float(v) => {
                let scratch = self
                    .utf8_scratch
                    .as_mut()
                    .expect("utf8 scratch missing for UTF-8 encoder");
                let text = scratch.ryu.format(*v);
                Some(ByteArray::from(text.as_bytes()))
            }
            Value::Int32(v) => {
                let scratch = self
                    .utf8_scratch
                    .as_mut()
                    .expect("utf8 scratch missing for UTF-8 encoder");
                let text = scratch.itoa.format(*v);
                Some(ByteArray::from(text.as_bytes()))
            }
            Value::Int64(v) => {
                let scratch = self
                    .utf8_scratch
                    .as_mut()
                    .expect("utf8 scratch missing for UTF-8 encoder");
                let text = scratch.itoa.format(*v);
                Some(ByteArray::from(text.as_bytes()))
            }
            Value::DateTime(datetime) => {
                let text = datetime.to_string();
                Some(ByteArray::from(text.as_str()))
            }
            Value::Date(datetime) => {
                let text = datetime.date().to_string();
                Some(ByteArray::from(text.as_str()))
            }
            Value::Time(duration) => {
                let text = duration.to_string();
                Some(ByteArray::from(text.as_str()))
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
