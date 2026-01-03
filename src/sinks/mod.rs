mod csv;
mod parquet;

use std::borrow::Cow;

use crate::error::{Error, Result};
use crate::metadata::DatasetMetadata;
use crate::parser::{ColumnInfo, ColumnarBatch, ParsedMetadata, StreamingRow};
use crate::value::Value;

pub use csv::CsvSink;
pub use parquet::ParquetSink;

/// Provides high-level dataset information to sinks during initialisation.
pub struct SinkContext<'a> {
    pub metadata: &'a DatasetMetadata,
    pub columns: &'a [ColumnInfo],
    pub source_path: Option<String>,
}

impl<'a> SinkContext<'a> {
    #[must_use]
    pub fn new(parsed: &'a ParsedMetadata) -> Self {
        Self {
            metadata: &parsed.header.metadata,
            columns: &parsed.columns,
            source_path: None,
        }
    }
}

/// Trait implemented by row sinks that consume decoded SAS rows.
pub trait RowSink {
    /// Called before any rows are written to allow the sink to initialise internal state.
    ///
    /// # Errors
    ///
    /// Returns an error if the sink cannot be initialised for the provided metadata.
    fn begin(&mut self, context: SinkContext<'_>) -> Result<()>;

    /// Invoked for every decoded row produced by the parser.
    ///
    /// # Errors
    ///
    /// Returns an error if the row cannot be encoded or written to the underlying output.
    fn write_row(&mut self, row: &[Value<'_>]) -> Result<()>;

    /// Invoked for every decoded row when using the zero-copy streaming pipeline.
    ///
    /// The default implementation materialises the row into a temporary `Vec<Value>`
    /// before delegating to [`write_row`](RowSink::write_row).
    ///
    /// # Errors
    ///
    /// Propagates errors from materialisation or `write_row`.
    fn write_streaming_row(&mut self, row: StreamingRow<'_, '_>) -> Result<()> {
        let values = row.materialize()?;
        self.write_row(&values)
    }

    /// Called once all rows have been forwarded to the sink.
    ///
    /// # Errors
    ///
    /// Returns an error if finalising the sink or flushing the underlying output fails.
    fn finish(&mut self) -> Result<()>;
}

/// Trait implemented by sinks that can consume columnar batches directly.
pub trait ColumnarSink: RowSink {
    /// Writes a batch of rows that may be filtered via `selection`, which maps sink columns
    /// to their corresponding source column indices.
    ///
    /// # Errors
    ///
    /// Returns an error if decoding fails or if the column selection is invalid.
    fn write_columnar_batch(
        &mut self,
        batch: &ColumnarBatch<'_>,
        selection: &[usize],
    ) -> Result<()>;
}

pub(crate) fn validate_sink_begin(
    context: &SinkContext<'_>,
    writer_present: bool,
    sink_name: &str,
) -> Result<()> {
    if writer_present {
        return Err(Error::Unsupported {
            feature: Cow::Owned(format!(
                "{sink_name} sink cannot be reused without finishing"
            )),
        });
    }
    if context.metadata.variables.len() != context.columns.len() {
        return Err(Error::InvalidMetadata {
            details: Cow::from("column metadata length mismatch"),
        });
    }
    Ok(())
}
