#![feature(portable_simd)]

mod columnar;
mod compression;
mod dataset;
mod encoding;
mod error;
mod fixture_catalog;
mod internal;
mod layout;
mod metadata;
mod options;
mod pages;
mod probe;
mod projection;
mod row;
mod scan;

#[cfg(test)]
pub(crate) mod test_utils;

pub use columnar::{
    BytesBuffer, ColumnBuffer, ColumnarBatch, OwnedColumnBuffer, OwnedColumnarBatch,
    PrimitiveBuffer, TrustedOffsets, Utf8Buffer,
};
pub use types::{ColumnIndex, RowIndex};
pub(crate) mod types;

pub use dataset::{Dataset, OpenBreakdown};
pub use error::{
    ArrowError, CompressionError, CorruptionError, DecodeError, Error, HeaderError, IoError,
    MetadataError, ProjectionError, Result, UnsupportedError,
};
pub use fixture_catalog::{
    FixtureCatalog, FixtureEntry, FixtureProfile, FixtureStatus, LogicalTypeCounts, NamedCount,
    ProjectionPreset, SampleSummary, ScanStatsSummary, TemporalFormatSummary, WidthSummary,
    build_catalog, build_projection, discover_fixture_paths, profile_dataset_with_sample,
    profile_fixture, summarize_scan_stats,
};
pub use metadata::{
    ColumnMeta, CompressionKind, DatasetMetadata, Endianness, LogicalType, SasDate, SasDateTime,
    SasTime, Timestamp,
};
pub use options::{
    BatchHint, DecodeMode, DictionaryStaging, IoBackendPreference, MojibakePolicy, OpenOptions,
    OpenOptionsBuilder, OrderingMode, PageCachePolicy, Parallelism, PrefetchPolicy, RowSelection,
    StringDecodeOptions, StringDecodeOptionsBuilder, TemporalDecodeOptions,
    TemporalDecodeOptionsBuilder, TrimMode, Utf8ValidationMode, ValidationMode,
};
pub use pages::DescriptorBreakdown;
pub use projection::{ProjectedColumnMeta, Projection, ProjectionBuilder};
pub use row::{CellValue, OwnedCellValue, OwnedRow, RawRow, RowView};
pub use scan::{
    BatchSink, OwnedBatchScanBreakdown, RawRowSink, RowSink, ScanBuilder, ScanProgress,
};
