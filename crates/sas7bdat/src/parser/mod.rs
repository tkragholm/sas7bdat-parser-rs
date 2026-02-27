mod catalog;
pub mod core;
mod header;
pub mod metadata;
mod rows;

pub use catalog::{CatalogLayout, parse_catalog};
pub use core::byteorder::{read_i16, read_u16, read_u32, read_u64, read_u64_be};
pub use header::{SasHeader, parse_header};
pub use metadata::{
    ColumnInfo, ColumnKind, ColumnMetadataBuilder, ColumnOffsets, DatasetLayout, MetadataIoMode,
    MetadataReadOptions, NumericKind, RowInfo, TextRef, TextStore, parse_metadata,
    parse_metadata_with_options,
};
pub(crate) use rows::CompiledRuntimeColumnRef;
pub use rows::{
    ColumnarBatch, ColumnarBatchMode, ColumnarColumn, DecodePolicy, MaterializedUtf8Column,
    MojibakeFixPolicy, NumericRuntimeColumnRef, OwnedRowIterator, ParallelScanConfig, RawRowBatch,
    RawScanStats, RowIterator, RowIteratorCore, RuntimeColumnRef, StagedUtf8Value, StreamingCell,
    StreamingRow, StringTrimPolicy, TemporalDecodePolicy, TypedNumericColumn, row_iterator,
    scan_file_projected_rows_with_decode_policy,
    scan_file_projected_rows_with_decode_policy_unordered, scan_file_raw_rows,
    scan_file_raw_rows_unordered, scan_file_raw_rows_unordered_batched_with_stats,
    scan_file_raw_rows_unordered_with_stats, scan_file_rows_with_decode_policy,
    scan_file_rows_with_decode_policy_unordered,
};
#[cfg(feature = "parquet")]
pub(crate) use rows::{sas_days_to_datetime, sas_seconds_to_datetime, sas_seconds_to_time};
