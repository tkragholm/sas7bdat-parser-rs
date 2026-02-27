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
pub use parser::{
    ColumnarBatchMode, DecodePolicy, MetadataIoMode, MetadataReadOptions, MojibakeFixPolicy,
    ParallelScanConfig, RawRowBatch, RawScanStats, StringTrimPolicy, TemporalDecodePolicy,
    scan_file_projected_rows_with_decode_policy,
    scan_file_projected_rows_with_decode_policy_unordered, scan_file_raw_rows,
    scan_file_raw_rows_unordered, scan_file_raw_rows_unordered_batched_with_stats,
    scan_file_raw_rows_unordered_with_stats, scan_file_rows_with_decode_policy,
    scan_file_rows_with_decode_policy_unordered,
};
pub use reader::{
    BinaryCol, CatalogScanPolicy, FrameBatch, FrameColumn, FrameColumnType, FrameSchema,
    FrameSchemaField, MissingSummary, OrderingMode, PrimitiveCol, Query, QueryPlan, QueryStream,
    Row, RowIter, RowLookup, RowSelection, RowValue, RowView, RowViewIter, SasReader, Shape,
    SourceKind, Utf8Col,
};
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
