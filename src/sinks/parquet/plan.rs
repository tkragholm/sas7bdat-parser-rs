use std::borrow::Cow;
use std::sync::Arc;

use parquet::basic::{LogicalType, Repetition, TimeUnit, Type as PhysicalType};
use parquet::data_type::ByteArray;
use parquet::schema::types::{Type, TypePtr};
use time::{Date, Duration, Month, OffsetDateTime, PrimitiveDateTime, Time};

use crate::error::{Error, Result};
use crate::logger::log_warn;
use crate::metadata::Variable;
use crate::parser::{ColumnInfo, ColumnKind, NumericKind};
use crate::value::Value;

use super::constants::SECONDS_PER_DAY;
use super::utf8::Utf8Scratch;
use crate::parser::{sas_days_to_datetime, sas_seconds_to_datetime};

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
    lenient_dates: bool,
    warned_invalid_value: bool,
    source_path: Option<String>,
}

impl ColumnPlan {
    pub(super) fn new(
        variable: &Variable,
        column: &ColumnInfo,
        lenient_dates: bool,
        source_path: Option<&str>,
    ) -> Result<(Self, TypePtr)> {
        let effective_kind = column.kind;

        let (encoder, physical_type, logical_type) = match effective_kind {
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
            lenient_dates,
            warned_invalid_value: false,
            source_path: source_path.map(str::to_owned),
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
            ColumnValueEncoder::Date => self.push_date(value)?,
            ColumnValueEncoder::DateTime => self.push_datetime(value)?,
            ColumnValueEncoder::Time => self.push_time(value)?,
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

    fn push_date(&mut self, value: &Value<'_>) -> Result<()> {
        self.push_temporal_i32(value, "date", Self::coerce_date)
    }

    fn push_datetime(&mut self, value: &Value<'_>) -> Result<()> {
        self.push_temporal_i64(value, "datetime", Self::coerce_timestamp)
    }

    fn push_time(&mut self, value: &Value<'_>) -> Result<()> {
        self.push_temporal_i64(value, "time", Self::coerce_time)
    }

    fn push_temporal_i32(
        &mut self,
        value: &Value<'_>,
        kind: &str,
        coerce: fn(&Self, &Value<'_>) -> Result<Option<i32>>,
    ) -> Result<()> {
        self.push_temporal(value, kind, coerce, |this, coerced| match &mut this.values {
            ColumnValues::Int32(values) => {
                Self::push_optional(&mut this.def_levels, values, coerced);
            }
            _ => unreachable!("column value encoder mismatch"),
        })
    }

    fn push_temporal_i64(
        &mut self,
        value: &Value<'_>,
        kind: &str,
        coerce: fn(&Self, &Value<'_>) -> Result<Option<i64>>,
    ) -> Result<()> {
        self.push_temporal(value, kind, coerce, |this, coerced| match &mut this.values {
            ColumnValues::Int64(values) => {
                Self::push_optional(&mut this.def_levels, values, coerced);
            }
            _ => unreachable!("column value encoder mismatch"),
        })
    }

    fn push_temporal<T>(
        &mut self,
        value: &Value<'_>,
        kind: &str,
        coerce: fn(&Self, &Value<'_>) -> Result<Option<T>>,
        mut push: impl FnMut(&mut Self, Option<T>),
    ) -> Result<()> {
        match coerce(self, value) {
            Ok(coerced) => {
                push(self, coerced);
            }
            Err(_err) if self.lenient_dates => {
                push(self, None);
                self.warn_invalid(kind);
            }
            Err(err) => return Err(err),
        }
        Ok(())
    }

    fn warn_invalid(&mut self, kind: &str) {
        if self.warned_invalid_value {
            return;
        }
        let prefix = self
            .source_path
            .as_deref()
            .map(|p| format!("{p}: "))
            .unwrap_or_default();
        log_warn(&format!(
            "{prefix}column '{}' contains non-{kind} value; written as null (use --strict-dates to fail)",
            self.name
        ));
        self.warned_invalid_value = true;
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
            Value::DateTime(dt) => Ok(Some(datetime_to_sas_seconds(dt))),
            Value::Date(dt) => Ok(Some(datetime_to_sas_days(dt))),
            Value::Time(duration) => Ok(Some(time_to_sas_seconds(duration))),
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
            Value::Float(days) => Self::float_days_to_i32(self.name.as_str(), *days),
            Value::Int32(days) => Ok(Some(*days)),
            Value::Int64(days) => i32::try_from(*days)
                .map(Some)
                .map_err(|_| self.type_mismatch_error("date", value)),
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
            other => self.coerce_seconds_to_micros(other, "timestamp"),
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
            other => self.coerce_seconds_to_micros(other, "time"),
        }
    }

    fn coerce_seconds_to_micros(
        &self,
        value: &Value<'_>,
        kind: &str,
    ) -> Result<Option<i64>> {
        match value {
            Value::Float(seconds) => Self::float_seconds_to_micros(self.name.as_str(), *seconds),
            Value::Int32(seconds) => Ok(Some(i64::from(*seconds) * 1_000_000)),
            Value::Int64(seconds) => Ok(Some(*seconds * 1_000_000)),
            other => Err(self.type_mismatch_error(kind, other)),
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
            Value::Int32(v) => Some(self.format_int_to_utf8(i64::from(*v))),
            Value::Int64(v) => Some(self.format_int_to_utf8(*v)),
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

    fn format_int_to_utf8(&mut self, value: i64) -> ByteArray {
        let scratch = self
            .utf8_scratch
            .as_mut()
            .expect("utf8 scratch missing for UTF-8 encoder");
        let owned = scratch.itoa.format(value).to_owned();
        scratch.intern_slice(owned.as_bytes())
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

    fn float_days_to_i32(column_name: &str, days: f64) -> Result<Option<i32>> {
        if !days.is_finite() {
            return Ok(None);
        }
        let rounded = days.trunc();
        let dt = sas_days_to_datetime(rounded).ok_or_else(|| Error::InvalidMetadata {
            details: Cow::Owned(format!(
                "column '{column_name}' contains date outside supported range"
            )),
        })?;
        let seconds = dt.unix_timestamp();
        let day = seconds.div_euclid(SECONDS_PER_DAY);
        i32::try_from(day)
            .map(Some)
            .map_err(|_| Error::InvalidMetadata {
                details: Cow::Owned(format!(
                    "column '{column_name}' contains date outside Parquet range"
                )),
            })
    }

    fn float_seconds_to_micros(column_name: &str, seconds: f64) -> Result<Option<i64>> {
        if !seconds.is_finite() {
            return Ok(None);
        }
        let dt = sas_seconds_to_datetime(seconds).ok_or_else(|| Error::InvalidMetadata {
            details: Cow::Owned(format!(
                "column '{column_name}' contains timestamp outside supported range"
            )),
        })?;
        let micros = dt.unix_timestamp_nanos().div_euclid(1_000);
        i64::try_from(micros)
            .map(Some)
            .map_err(|_| Error::InvalidMetadata {
                details: Cow::Owned(format!(
                    "column '{column_name}' contains timestamp outside Parquet range"
                )),
            })
    }
}

fn sas_epoch() -> OffsetDateTime {
    PrimitiveDateTime::new(
        Date::from_calendar_date(1960, Month::January, 1).expect("valid SAS epoch"),
        Time::MIDNIGHT,
    )
    .assume_utc()
}

fn datetime_to_sas_seconds(datetime: &OffsetDateTime) -> f64 {
    (*datetime - sas_epoch()).as_seconds_f64()
}

fn datetime_to_sas_days(datetime: &OffsetDateTime) -> f64 {
    const SECONDS_PER_DAY_F64: f64 = 86_400.0;
    datetime_to_sas_seconds(datetime) / SECONDS_PER_DAY_F64
}

#[allow(clippy::cast_precision_loss)]
fn time_to_sas_seconds(duration: &Duration) -> f64 {
    duration.as_seconds_f64()
}
