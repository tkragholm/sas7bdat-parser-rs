pub mod api;
pub mod error;
pub mod metadata;
pub mod parser;
pub mod sinks;
pub mod value;
pub mod logger;
pub use crate::error::{Error, Result};
pub use api::{ReadOptions, SasFile};
pub use sinks::{ColumnarSink, CsvSink, ParquetSink, RowSink, SinkContext};

/// Parses SAS metadata and returns the decoded layout information.
///
/// This preserves the old API name to maintain compatibility with existing
/// tests and callers; it simply forwards to `parser::parse_metadata`.
/// Parses SAS metadata and returns the decoded layout information.
///
/// # Errors
///
/// Returns an error if the metadata pages cannot be decoded.
pub fn parse_layout<R: std::io::Read + std::io::Seek>(
    reader: &mut R,
) -> Result<parser::ParsedMetadata> {
    parser::parse_metadata(reader)
}
