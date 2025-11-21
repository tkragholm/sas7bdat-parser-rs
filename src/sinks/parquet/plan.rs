use std::borrow::Cow;
use std::sync::Arc;

use parquet::basic::{LogicalType, Repetition, TimeUnit, Type as PhysicalType};
use parquet::data_type::ByteArray;
use parquet::schema::types::{Type, TypePtr};

use crate::error::{Error, Result};
use crate::metadata::Variable;
use crate::parser::{ColumnInfo, ColumnKind, NumericKind};
use crate::value::Value;

use super::constants::SECONDS_PER_DAY;
use super::utf8::Utf8Scratch;

#[derive(Clone, Copy)]
pub(super) enum ColumnValueEncoder {
    Double,
    Date,
    DateTime,
    Time,
    Utf8,
}

pub(super) enum ColumnValues {
    Double(Vec<f64>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    ByteArray(Vec<ByteArray>),
}

pub(super) struct ColumnPlan {
    pub name: String,
    pub encoder: ColumnValueEncoder,
    pub def_levels: Vec<i16>,
    pub def_bitmap: Vec<u8>,
    pub values: ColumnValues,
    pub utf8_scratch: Option<Utf8Scratch>,
    pub utf8_inlines: Vec<ByteArray>,
}

impl ColumnPlan {
    pub(super) fn new(variable: &Variable, column: &ColumnInfo) -> Result<(Self, TypePtr)> {
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
            def_bitmap: Vec::new(),
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

    pub(super) fn reserve_capacity(&mut self, capacity: usize) {
        self.def_levels.reserve(capacity);
        match &mut self.values {
            ColumnValues::Double(values) => values.reserve(capacity),
            ColumnValues::Int32(values) => values.reserve(capacity),
            ColumnValues::Int64(values) => values.reserve(capacity),
            ColumnValues::ByteArray(values) => values.reserve(capacity),
        }
    }

    pub(super) fn push(&mut self, value: &Value<'_>) -> Result<()> {
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

    pub(super) fn flush(
        &mut self,
        mut column_writer: parquet::file::writer::SerializedColumnWriter<'_>,
    ) -> Result<()> {
        match (&mut self.values, self.encoder) {
            (ColumnValues::Double(values), ColumnValueEncoder::Double) => {
                let writer = column_writer.typed::<parquet::data_type::DoubleType>();
                writer.write_batch(values, Some(&self.def_levels), None)?;
                values.clear();
            }
            (ColumnValues::Int32(values), ColumnValueEncoder::Date) => {
                let writer = column_writer.typed::<parquet::data_type::Int32Type>();
                writer.write_batch(values, Some(&self.def_levels), None)?;
                values.clear();
            }
            (
                ColumnValues::Int64(values),
                ColumnValueEncoder::DateTime | ColumnValueEncoder::Time,
            ) => {
                let writer = column_writer.typed::<parquet::data_type::Int64Type>();
                writer.write_batch(values, Some(&self.def_levels), None)?;
                values.clear();
            }
            (ColumnValues::ByteArray(values), ColumnValueEncoder::Utf8) => {
                let writer = column_writer.typed::<parquet::data_type::ByteArrayType>();
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

    fn push_optional<T>(def_levels: &mut Vec<i16>, values: &mut Vec<T>, value: Option<T>) {
        match value {
            Some(v) => {
                def_levels.push(1);
                values.push(v);
            }
            None => def_levels.push(0),
        }
    }

    fn coerce_numeric(&self, value: &Value<'_>) -> Result<Option<f64>> {
        match value {
            Value::Missing(_) => Ok(None),
            Value::Float(v) => Ok(Some(*v)),
            Value::Int32(v) => Ok(Some(f64::from(*v))),
            Value::Int64(v) => {
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
