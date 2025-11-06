mod csv;
mod parquet;

use crate::error::Result;
use crate::metadata::DatasetMetadata;
use crate::parser::{ColumnInfo, ParsedMetadata, StreamingRow};
use crate::value::Value;

pub use csv::CsvSink;
pub use parquet::ParquetSink;

/// Provides high-level dataset information to sinks during initialisation.
pub struct SinkContext<'a> {
    pub metadata: &'a DatasetMetadata,
    pub columns: &'a [ColumnInfo],
}

impl<'a> SinkContext<'a> {
    #[must_use]
    pub fn new(parsed: &'a ParsedMetadata) -> Self {
        Self {
            metadata: &parsed.header.metadata,
            columns: &parsed.columns,
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
