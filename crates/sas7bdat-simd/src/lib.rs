#![feature(portable_simd)]

mod columnar;
mod compression;
mod dataset;
mod encoding;
mod error;
#[doc(hidden)]
pub mod fixture_catalog;
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
pub use metadata::{
    ColumnMeta, CompressionKind, DatasetMetadata, Endianness, LogicalType, SasDate, SasDateTime,
    SasTime, Timestamp,
};
pub use options::{
    BatchHint, DecodeMode, DictionaryStaging, IoBackendPreference, MojibakePolicy, OpenOptions,
    OrderingMode, PageCachePolicy, Parallelism, PrefetchPolicy, RowSelection, StringDecodeOptions,
    TemporalDecodeOptions, TrimMode, Utf8ValidationMode, ValidationMode,
};
pub use pages::DescriptorBreakdown;
pub use projection::{ProjectedColumnMeta, Projection, ProjectionBuilder};
pub use row::{CellValue, OwnedCellValue, OwnedRow, RawRow, RowView};
pub use scan::{
    BatchSink, OwnedBatchScanBreakdown, RawRowSink, RowSink, ScanBuilder, ScanProgress, ScanStats,
};
