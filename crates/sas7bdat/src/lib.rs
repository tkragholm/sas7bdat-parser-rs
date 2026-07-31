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
//! - **`arrow`** — enables [`ScanBuilder::visit_arrow_batches`] and
//!   [`ScanBuilder::arrow_schema`]. When active, the crate pulls in `arrow-array` and
//!   `arrow-schema` and exposes Arrow `RecordBatch` conversion helpers. When absent,
//!   those APIs do not exist and the rest of the crate is unchanged.
//! - **`dictionary`** — enables the [`dictionary`] module: string columns are probed for
//!   cardinality and, when they look like category codes rather than free text, interned
//!   into `u32` codes plus a value dictionary. That feeds an Arrow `DictionaryArray<u32>`,
//!   and through it Polars `Categorical` and R `factor`. Adds `ahash`, `lasso2` and
//!   `cardinality-estimator`.
//!
//! The remaining features exist for this repository's own tooling and carry no API
//! stability guarantees: `hotpath-profile` (profiling instrumentation),
//! `internal-bench` (exposes `test_utils` to the `wide_table` benchmark) and
//! `fixture-catalog` (corpus profiling used by the benchmark harness and the
//! `sas7bdat-cli` dev-tools binaries).

#![feature(portable_simd)]

pub mod catalog;
mod columnar;
mod compression;
mod dataset;
#[cfg(feature = "dictionary")]
pub mod dictionary;
mod encoding;
mod error;
// In-repo tooling for the benchmark harness and the dev-tools CLI binaries, not
// reader API — consumers have no fixture corpus to point it at. Gated the same way
// `test_utils` is; the self dev-dependency in Cargo.toml turns it on for benches.
#[cfg(any(test, feature = "fixture-catalog"))]
mod fixture_catalog;
mod internal;
mod labels;
mod layout;
mod metadata;
mod netpath;
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
    ArrowError, CompressionError, CorruptionError, DecodeError, Error, HeaderError, InternalError,
    IoError, MetadataError, ProjectionError, Result, UnsupportedError,
};
#[cfg(any(test, feature = "fixture-catalog"))]
pub use fixture_catalog::{
    FixtureCatalog, FixtureEntry, FixtureProfile, FixtureStatus, LogicalTypeCounts, NamedCount,
    ProfileMode, ProjectionPreset, SampleSummary, TemporalFormatSummary, WidthSummary,
    build_catalog, build_projection, discover_fixture_paths, profile_dataset_with_sample,
    profile_fixture, summarize_scan_stats,
};
pub use labels::{LabelSet, ValueKey, ValueLabel, ValueType};
pub use metadata::{
    ColumnMeta, CompressionKind, DatasetMetadata, Endianness, LogicalType, SasDate, SasDateTime,
    SasTime, Timestamp,
};
pub use options::{
    BatchHint, ColumnMajorDecode, DecodeMode, DictionaryStaging, IoBackendPreference,
    MojibakePolicy, OpenOptions, OpenOptionsBuilder, OrderingMode, Parallelism, RowSelection,
    StringDecodeOptions, StringDecodeOptionsBuilder, TemporalDecodeOptions,
    TemporalDecodeOptionsBuilder, TrimMode, Utf8ValidationMode,
};
pub use pages::DescriptorBreakdown;
pub use projection::{ProjectedColumnMeta, Projection, ProjectionBuilder};
pub use row::{CellValue, OwnedCellValue, OwnedRow, RawRow, RowView};
#[cfg(feature = "arrow")]
pub use scan::SAS_LOGICAL_TYPE_KEY;
pub use scan::{
    OwnedBatchScanBreakdown, ScanBuilder, ScanProgress, ScanProgressObserver, ScanStatsSummary,
};
