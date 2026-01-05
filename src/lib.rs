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
pub use reader::{RowSelection, SasReader};
pub use sinks::{ColumnarSink, CsvSink, ParquetSink, RowSink, SinkContext};

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
