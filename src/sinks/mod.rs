mod parquet;
mod csv;

use crate::error::Result;
use crate::metadata::DatasetMetadata;
use crate::parser::{ColumnInfo, ParsedMetadata};
use crate::value::Value;

pub use parquet::ParquetSink;
pub use csv::CsvSink;

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

    /// Called once all rows have been forwarded to the sink.
    ///
    /// # Errors
    ///
    /// Returns an error if finalising the sink or flushing the underlying output fails.
    fn finish(&mut self) -> Result<()>;
}
