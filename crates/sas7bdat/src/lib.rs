pub mod cell;
pub mod dataset;
pub mod error;
mod iter_utils;
pub mod logger;
pub mod parser;
pub mod reader;
pub mod sinks;
pub use crate::error::{Error, Result};
pub use cell::{CellValue, MissingValue};
pub use reader::{
    Row, RowIter, RowLookup, RowSelection, RowValue, RowView, RowViewIter, SasReader,
};
pub use parser::{MetadataIoMode, MetadataReadOptions};
#[cfg(feature = "csv")]
pub use sinks::CsvSink;
#[cfg(feature = "parquet")]
pub use sinks::ParquetSink;
pub use sinks::{ColumnarSink, RowSink, SinkContext};
#[cfg(feature = "time")]
pub use time::OffsetDateTime;

/// Parses SAS metadata and returns the decoded layout information.
///
/// # Errors
///
/// Returns an error if the metadata pages cannot be decoded.
pub fn decode_layout<R: std::io::Read + std::io::Seek>(
    reader: &mut R,
) -> Result<parser::DatasetLayout> {
    parser::parse_metadata(reader)
}

/// Parses SAS metadata with custom metadata read options.
///
/// # Errors
///
/// Returns an error if the metadata pages cannot be decoded.
pub fn decode_layout_with_options<R: std::io::Read + std::io::Seek>(
    reader: &mut R,
    options: MetadataReadOptions,
) -> Result<parser::DatasetLayout> {
    parser::parse_metadata_with_options(reader, options)
}
