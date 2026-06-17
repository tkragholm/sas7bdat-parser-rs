//! A high-performance SAS7BDAT parser built around SIMD-accelerated page decoding.
//!
//! # Quick start
//!
//! ```no_run
//! use sas7bdat::{Dataset, Result};
//! use std::ops::ControlFlow;
//!
//! fn main() -> Result<()> {
//!     let ds = Dataset::open("data.sas7bdat")?;
//!     ds.scan().visit_rows(|row| {
//!         println!("{row:?}");
//!         Ok(ControlFlow::Continue(()))
//!     })?;
//!     Ok(())
//! }
//! ```
//!
//! # Feature flags
//!
//! - **`arrow`** — enables [`ScanBuilder::visit_arrow_batches`] and [`Dataset::arrow_schema`].
//!   When active, the crate pulls in `arrow-schema` and `polars-arrow` and exposes Arrow
//!   `RecordBatch` conversion helpers. When absent, those APIs do not exist and the rest of
//!   the crate is unchanged.

#![feature(portable_simd)]

pub mod catalog;
#[cfg(feature = "dictionary")]
pub mod dictionary;
mod columnar;
mod compression;
mod dataset;
mod encoding;
mod error;
mod fixture_catalog;
mod internal;
mod labels;
mod layout;
mod metadata;
mod options;
mod pages;
mod probe;
mod projection;
mod row;
mod scan;

// Exposed publicly only for in-crate tests and the `internal-bench` benchmarks (which
// synthesize datasets via `MockDatasetBuilder`). Absent from normal builds.
#[cfg(any(test, feature = "internal-bench"))]
pub mod test_utils;

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
    ProjectionPreset, SampleSummary, TemporalFormatSummary, WidthSummary, build_catalog,
    build_projection, discover_fixture_paths, profile_dataset_with_sample, profile_fixture,
    summarize_scan_stats,
};
pub use labels::{LabelSet, ValueKey, ValueLabel, ValueType};
pub use metadata::{
    ColumnMeta, CompressionKind, DatasetMetadata, Endianness, LogicalType, SasDate, SasDateTime,
    SasTime, Timestamp,
};
pub use options::{
    BatchHint, ColumnMajorDecode, DecodeMode, DictionaryStaging, IoBackendPreference,
    MojibakePolicy, OpenOptions, OpenOptionsBuilder, OrderingMode, PageCachePolicy, Parallelism,
    PrefetchPolicy, RowSelection, StringDecodeOptions, StringDecodeOptionsBuilder,
    TemporalDecodeOptions, TemporalDecodeOptionsBuilder, TrimMode, Utf8ValidationMode,
    ValidationMode,
};
pub use pages::DescriptorBreakdown;
pub use projection::{ProjectedColumnMeta, Projection, ProjectionBuilder};
pub use row::{CellValue, OwnedCellValue, OwnedRow, RawRow, RowView};
pub use scan::{
    BatchSink, OwnedBatchScanBreakdown, RawRowSink, RowSink, ScanBuilder, ScanProgress,
    ScanStatsSummary,
};
