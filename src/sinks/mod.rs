mod parquet;

use crate::error::Result;
use crate::metadata::DatasetMetadata;
use crate::parser::{ColumnInfo, ParsedMetadata};
use crate::value::Value;

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
    fn begin(&mut self, context: SinkContext<'_>) -> Result<()>;

    /// Invoked for every decoded row produced by the parser.
    fn write_row(&mut self, row: &[Value<'_>]) -> Result<()>;

    /// Called once all rows have been forwarded to the sink.
    fn finish(&mut self) -> Result<()>;
}
