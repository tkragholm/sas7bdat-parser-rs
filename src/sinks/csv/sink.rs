use std::borrow::Cow;
use std::io::Write;

use csv::{ByteRecord, Writer, WriterBuilder};
use itoa::Buffer as ItoaBuffer;
use ryu::Buffer as RyuBuffer;

use crate::error::{Error, Result};
use crate::parser::{ColumnKind, NumericKind, StreamingRow};
use crate::sinks::{RowSink, SinkContext};
use crate::value::Value;

use super::constants::{DEFAULT_DELIMITER, DEFAULT_SCRATCH_CAPACITY, DEFAULT_WRITE_HEADERS};
use super::encode::{encode_value, flush_record};

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
            delimiter: DEFAULT_DELIMITER,
            write_headers: DEFAULT_WRITE_HEADERS,
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
        let writer = builder.from_writer(output);
        self.writer = Some(writer);
        Ok(())
    }

    fn write_headers(&mut self, context: &SinkContext<'_>) -> Result<()> {
        if !self.write_headers {
            return Ok(());
        }
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

    fn ensure_row_len(&self, len: usize) -> Result<()> {
        if len != self.column_count {
            return Err(Error::InvalidMetadata {
                details: Cow::Owned(format!(
                    "row length {len} does not match expected {}",
                    self.column_count
                )),
            });
        }
        Ok(())
    }

    fn prepare_row_buffers(&mut self, len: usize) -> Result<(RyuBuffer, ItoaBuffer)> {
        self.ensure_row_len(len)?;
        self.record.clear();
        Ok((RyuBuffer::new(), ItoaBuffer::new()))
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

        for (var, col) in context
            .metadata
            .variables
            .iter()
            .zip(context.columns.iter())
        {
            match col.kind {
                ColumnKind::Character => {
                    let _ = var;
                }
                ColumnKind::Numeric(
                    NumericKind::Double
                    | NumericKind::Date
                    | NumericKind::DateTime
                    | NumericKind::Time,
                ) => {}
            }
        }

        self.build_writer()?;
        self.column_count = context.columns.len();
        self.record = ByteRecord::with_capacity(self.column_count, 0);
        self.scratch = (0..self.column_count)
            .map(|_| Vec::with_capacity(DEFAULT_SCRATCH_CAPACITY))
            .collect();

        self.write_headers(&context)?;
        Ok(())
    }

    fn write_row(&mut self, row: &[Value<'_>]) -> Result<()> {
        let (mut ryu, mut itoa) = self.prepare_row_buffers(row.len())?;

        for (idx, val) in row.iter().enumerate() {
            let buf = &mut self.scratch[idx];
            encode_value(val, buf, &mut ryu, &mut itoa)?;
            self.record.push_field(buf);
        }
        let writer = self.writer.as_mut().expect("csv writer must be present");
        flush_record(writer, &self.record)
    }

    fn write_streaming_row(&mut self, row: StreamingRow<'_, '_>) -> Result<()> {
        let (mut ryu, mut itoa) = self.prepare_row_buffers(row.len())?;

        for (idx, cell_result) in row.iter().enumerate() {
            let cell = cell_result?;
            let value = cell.decode_value()?;
            let buf = &mut self.scratch[idx];
            encode_value(&value, buf, &mut ryu, &mut itoa)?;
            self.record.push_field(buf);
        }

        let writer = self.writer.as_mut().expect("csv writer must be present");
        flush_record(writer, &self.record)
    }

    fn finish(&mut self) -> Result<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.flush()?;
            let out = writer.into_inner().map_err(|e| Error::InvalidMetadata {
                details: Cow::Owned(format!("csv into_inner failed: {e}")),
            })?;
            self.output = Some(out);
        }
        self.column_count = 0;
        self.scratch.clear();
        self.record.clear();
        Ok(())
    }
}
