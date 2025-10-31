use std::borrow::Cow;
use std::io::Write;

use csv::{ByteRecord, Writer, WriterBuilder};
use itoa::Buffer as ItoaBuffer;
use ryu::Buffer as RyuBuffer;
use time::{Duration, OffsetDateTime};

use crate::error::{Error, Result};
use crate::parser::{ColumnKind, NumericKind};
use crate::sinks::{RowSink, SinkContext};
use crate::value::Value;

/// Writes decoded rows into a delimited text file (CSV/TSV).
pub struct CsvSink<W: Write + Send> {
    output: Option<W>,
    writer: Option<Writer<W>>, 
    delimiter: u8,
    write_headers: bool,
    column_count: usize,
    record: ByteRecord,
    scratch: Vec<Vec<u8>>, // one scratch buffer per column
}

impl<W: Write + Send> CsvSink<W> {
    #[must_use]
    pub fn new(writer: W) -> Self {
        Self {
            output: Some(writer),
            writer: None,
            delimiter: b',',
            write_headers: true,
            column_count: 0,
            record: ByteRecord::new(),
            scratch: Vec::new(),
        }
    }

    #[must_use]
    pub const fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    #[must_use]
    pub const fn with_headers(mut self, headers: bool) -> Self {
        self.write_headers = headers;
        self
    }

    fn build_writer(&mut self) -> Result<()> {
        let output = self.output.take().ok_or_else(|| Error::InvalidMetadata {
            details: Cow::from("CSV sink output already taken"),
        })?;
        let mut builder = WriterBuilder::new();
        builder.delimiter(self.delimiter);
        // Leave other settings to csv defaults.
        let writer = builder.from_writer(output);
        self.writer = Some(writer);
        Ok(())
    }

    fn write_headers(&mut self, context: &SinkContext<'_>) -> Result<()> {
        if !self.write_headers {
            return Ok(());
        }
        // Build a one-off header record of variable names, trimming trailing spaces.
        let mut header = ByteRecord::new();
        for (variable, _column) in context
            .metadata
            .variables
            .iter()
            .zip(context.columns.iter())
        {
            header.push_field(variable.name.trim_end().as_bytes());
        }
        let writer = self.writer.as_mut().expect("csv writer must be present");
        writer
            .write_byte_record(&header)
            .map_err(|e| Error::InvalidMetadata {
                details: Cow::Owned(format!("csv header write failed: {e}")),
            })?;
        Ok(())
    }

    fn encode_value(
        value: &Value<'_>,
        out: &mut Vec<u8>,
        ryu: &mut RyuBuffer,
        itoa: &mut ItoaBuffer,
    ) -> Result<()> {
        out.clear();
        match value {
            Value::Missing(_) => {
                // empty field
            }
            Value::Float(v) => {
                let s = ryu.format(*v);
                out.extend_from_slice(s.as_bytes());
            }
            Value::Int32(v) => {
                let s = itoa.format(*v);
                out.extend_from_slice(s.as_bytes());
            }
            Value::Int64(v) => {
                let s = itoa.format(*v);
                out.extend_from_slice(s.as_bytes());
            }
            Value::NumericString(s) | Value::Str(s) => {
                out.extend_from_slice(s.as_bytes());
            }
            Value::Bytes(bytes) => {
                out.extend_from_slice(bytes);
            }
            Value::DateTime(dt) => {
                write_datetime(dt, out);
            }
            Value::Date(dt) => {
                write_date(dt, out);
            }
            Value::Time(dur) => {
                write_time(dur, out)?;
            }
        }
        Ok(())
    }
}

impl<W: Write + Send> RowSink for CsvSink<W> {
    fn begin(&mut self, context: SinkContext<'_>) -> Result<()> {
        if self.writer.is_some() {
            return Err(Error::Unsupported {
                feature: Cow::from("CSV sink cannot be reused without finishing"),
            });
        }
        if context.metadata.variables.len() != context.columns.len() {
            return Err(Error::InvalidMetadata {
                details: Cow::from("column metadata length mismatch"),
            });
        }

        // Defensive: ensure no unsupported/unknown column kinds sneak in (parity with Parquet sink checks)
        for (var, col) in context
            .metadata
            .variables
            .iter()
            .zip(context.columns.iter())
        {
            match col.kind {
                ColumnKind::Character => {
                    let _ = var; // ok
                }
                ColumnKind::Numeric(NumericKind::Double | NumericKind::Date |
NumericKind::DateTime | NumericKind::Time) => {}
            }
        }

        self.build_writer()?;
        self.column_count = context.columns.len();
        self.record = ByteRecord::with_capacity(self.column_count, 0);
        self.scratch = (0..self.column_count).map(|_| Vec::with_capacity(64)).collect();

        self.write_headers(&context)?;
        Ok(())
    }

    fn write_row(&mut self, row: &[Value<'_>]) -> Result<()> {
        if row.len() != self.column_count {
            return Err(Error::InvalidMetadata {
                details: Cow::Owned(format!(
                    "row length {} does not match expected {}",
                    row.len(), self.column_count
                )),
            });
        }
        self.record.clear();
        // Local number formatting buffers to avoid borrowing self while
        // holding a mutable borrow of a scratch column buffer.
        let mut ryu = RyuBuffer::new();
        let mut itoa = ItoaBuffer::new();

        for (idx, val) in row.iter().enumerate() {
            let buf = &mut self.scratch[idx];
            Self::encode_value(val, buf, &mut ryu, &mut itoa)?;
            self.record.push_field(buf);
        }
        let writer = self.writer.as_mut().expect("csv writer must be present");
        writer
            .write_byte_record(&self.record)
            .map_err(|e| Error::InvalidMetadata {
                details: Cow::Owned(format!("csv write failed: {e}")),
            })?;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.flush()?;
            let out = writer.into_inner().map_err(|e| Error::InvalidMetadata {
                details: Cow::Owned(format!("csv into_inner failed: {e}")),
            })?; // returns W
            self.output = Some(out);
        }
        self.column_count = 0;
        self.scratch.clear();
        self.record.clear();
        Ok(())
    }
}

fn write_date(dt: &OffsetDateTime, out: &mut Vec<u8>) {
    let date = dt.date().to_string();
    out.extend_from_slice(date.as_bytes());
}

fn write_datetime(dt: &OffsetDateTime, out: &mut Vec<u8>) {
    // Round to milliseconds like the integration fixtures and render
    // "YYYY-MM-DD HH:MM:SS[.mmm]"
    let rounded = round_to_millisecond(dt);
    let date = rounded.date();
    let time = rounded.time();
    // YYYY-MM-DD
    out.extend_from_slice(date.to_string().as_bytes());
    out.extend_from_slice(b" ");
    // HH:MM:SS
    write_two(time.hour(), out);
    out.push(b':');
    write_two(time.minute(), out);
    out.push(b':');
    write_two(time.second(), out);
    let nanos = time.nanosecond();
    if nanos != 0 {
        out.push(b'.');
        let millis = nanos / 1_000_000;
        let millis_u16 = u16::try_from(millis).unwrap_or(0);
        write_three(millis_u16, out);
    }
}

fn write_time(dur: &Duration, out: &mut Vec<u8>) -> Result<()> {
    // Render HH:MM:SS[.mmm]
    let mut total_seconds = dur.whole_seconds();
    let nanos_total = dur.whole_nanoseconds();
    let nanos = nanos_total - i128::from(total_seconds) * 1_000_000_000;
    // Round fractional part to nearest millisecond and carry to seconds if needed.
    let mut millis = i64::try_from((nanos.abs() + 500_000) / 1_000_000).map_err(|_| {
        Error::InvalidMetadata {
            details: Cow::from("time millisecond rounding overflow"),
        }
    })?;
    if millis >= 1000 {
        millis = 0;
        total_seconds += if nanos_total >= 0 { 1 } else { -1 };
    }

    let mut remaining = total_seconds;
    let hours = remaining.div_euclid(3600);
    remaining -= hours * 3600;
    let minutes = remaining.div_euclid(60);
    remaining -= minutes * 60;
    let seconds = remaining;

    let hours_u8 = u8::try_from(hours).map_err(|_| Error::InvalidMetadata {
        details: Cow::from("time hours component out of range for CSV formatting"),
    })?;
    let minutes_u8 = u8::try_from(minutes).map_err(|_| Error::InvalidMetadata {
        details: Cow::from("time minutes component out of range for CSV formatting"),
    })?;
    let seconds_u8 = u8::try_from(seconds).map_err(|_| Error::InvalidMetadata {
        details: Cow::from("time seconds component out of range for CSV formatting"),
    })?;

    write_two(hours_u8, out);
    out.push(b':');
    write_two(minutes_u8, out);
    out.push(b':');
    write_two(seconds_u8, out);

    if millis != 0 {
        out.push(b'.');
        let millis_u16 = u16::try_from(millis).map_err(|_| Error::InvalidMetadata {
            details: Cow::from("time milliseconds component out of range for CSV formatting"),
        })?;
        write_three(millis_u16, out);
    }
    Ok(())
}

fn round_to_millisecond(dt: &OffsetDateTime) -> OffsetDateTime {
    use time::Duration as TDuration;
    let nanos = u64::from(dt.time().nanosecond());
    let mut millis = (nanos + 500_000) / 1_000_000; // round to nearest ms
    let mut adjusted = *dt;
    if millis == 1_000 {
        millis = 0;
        if let Some(next) = adjusted.checked_add(TDuration::seconds(1)) {
            adjusted = next;
        } else {
            return *dt;
        }
    }
    let new_nanos = u32::try_from(millis * 1_000_000).unwrap_or(0);
    adjusted.replace_nanosecond(new_nanos).unwrap_or(*dt)
}

#[inline]
fn write_two(v: u8, out: &mut Vec<u8>) {
    out.push(b'0' + (v / 10));
    out.push(b'0' + (v % 10));
}

#[inline]
fn write_three(v: u16, out: &mut Vec<u8>) {
    let hundreds = v / 100;
    let tens = (v / 10) % 10;
    let ones = v % 10;
    let hundreds_u8 = u8::try_from(hundreds).unwrap_or(0);
    let tens_u8 = u8::try_from(tens).unwrap_or(0);
    let ones_u8 = u8::try_from(ones).unwrap_or(0);
    out.push(b'0' + hundreds_u8);
    out.push(b'0' + tens_u8);
    out.push(b'0' + ones_u8);
}
