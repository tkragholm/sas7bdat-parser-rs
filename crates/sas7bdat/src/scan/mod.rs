//! Scanning: turning a dataset's pages into rows or batches.
//!
//! # The pipelines, and why there is more than one
//!
//! There are several decode pipelines here, and **which one runs depends on the method you
//! called as much as on the file**. They share almost everything below the point where they
//! diverge, so a profile of one looks like a profile of another.
//!
//! ```text
//!                                              page source          fill
//!   visit_raw_rows ───────────────────────────► row-stream          n/a
//!   visit_rows ───────────────────────────────► row-stream          n/a
//!   visit_batches ────────────────────────────► borrowed-stream     row-major
//!                                                (its own pipeline; never reaches the
//!                                                 tiled fill, whatever the plan looks like)
//!   collect_batches ─┬─ buffered source? ─────► one-pass         ┐
//!                    ├─ >1 worker, >1 page? ──► two-pass-parallel │ tiled *or* row-major,
//!                    └─ otherwise ────────────► two-pass-serial   ┘ decided per page
//! ```
//!
//! The two axes are independent, which is why they are reported separately. `two-pass-serial`
//! with a tiled fill and `two-pass-serial` with a row-major one are two of the four things
//! that used to be called "different pipelines"; splitting the axes makes it visible that
//! they differ only in the fill, which is the argument for merging them.
//!
//! The first three of those four are tried in order and each declines by returning `None`,
//! so the conditions are spread across modules rather than written in one place:
//!
//! | condition | where |
//! |---|---|
//! | `ColumnMajorDecode` requested | [`crate::ScanBuilder::with_column_major_decode`] |
//! | whole-file window, in-memory source | `column_major_file_bytes` in [`builder`] |
//! | file source is a path, no cached descriptors, page geometry | [`fused::try_stream_batches_fused`] |
//! | worker count and ordering | `try_stream_batches_parallel` in [`builder`] |
//! | plan has a staged-numeric family | [`batch::BatchDecodePlan`] flags |
//! | page is `FusedContiguousUncompressed` | the descriptor's `exec_class` |
//!
//! Below that split, `stream_descriptors_into_batches` chooses per page between the tiled
//! column-major fill and `emit_rows_from_page`.
//!
//! # Before you benchmark
//!
//! Ask [`crate::ScanBuilder::predict_path`] which pipeline your scan will take, and label
//! the measurement with it. [`ScanStatsSummary::path`] reports the same thing after the
//! fact. An optimisation to the tiled fill, benchmarked through `visit_batches`, will
//! measure as exactly zero, and nothing in the profile will say why.

use crate::{
    columnar::{ColumnBuffer, ColumnarBatch, OwnedColumnBuffer, OwnedColumnarBatch},
    compression::decompress_row,
    dataset::Dataset,
    encoding::resolve_encoding,
    error::{Error, Result},
    internal::{FileSource, PageDescriptor, ProjectedColumnPlan, RowSpan, RowSpanKind},
    metadata::{ColumnMeta, Endianness, LogicalType, SasDate, SasDateTime, SasTime},
    options::{
        BatchHint, ColumnMajorDecode, DecodeMode, MojibakePolicy, OrderingMode, Parallelism,
        RowSelection, StringDecodeOptions, TemporalDecodeOptions, TrimMode, Utf8ValidationMode,
    },
    projection::Projection,
    row::{OwnedRow, RawRow, RowView},
};
use encoding_rs::{Encoding, UTF_8};
use serde::Serialize;
use std::{
    fs::File,
    io::{Cursor, Seek, SeekFrom},
    ops::ControlFlow,
    sync::Arc,
};

mod batch;
mod builder;
mod extent;
mod fused;
mod numeric;
mod plan;
mod raw;
mod row_decode;
mod string;

pub use builder::{ScanBuilder, SourceDeclined};
#[cfg(feature = "arrow")]
pub use plan::SAS_LOGICAL_TYPE_KEY;

/// How a scan read and planned the file's pages.
///
/// One of the two axes of [`ScanPath`]. Independent of [`FillStrategy`]: any source can be
/// paired with either fill.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
pub enum PageSource {
    /// Not recorded. For a completed scan this is a bug in the recording, not a source.
    #[default]
    Unrecorded,
    /// Descriptors compiled and rows decoded from the same reads, in one pass over the file.
    /// Chosen only for a path source, which is the buffered backend, which is the
    /// network-share case: one pass halves the bytes moved and that is the whole cost there.
    OnePass,
    /// Descriptors compiled first, then decoded across worker threads.
    TwoPassParallel,
    /// Descriptors compiled first, then decoded on the calling thread. The parallel runner
    /// declines below two workers or two pages, and this is what picks those up.
    TwoPassSerial,
    /// Descriptors compiled first, then batches that borrow the page rather than owning
    /// their bytes. A pipeline of its own: it does not go through
    /// `stream_descriptors_into_batches`, so it can never reach the tiled fill.
    BorrowedStream,
    /// Rows streamed without batching: `visit_rows` and `visit_raw_rows`.
    RowStream,
}

impl PageSource {
    /// A short stable name, for labelling a benchmark or a profile.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unrecorded => "unrecorded",
            Self::OnePass => "one-pass",
            Self::TwoPassParallel => "two-pass-parallel",
            Self::TwoPassSerial => "two-pass-serial",
            Self::BorrowedStream => "borrowed-stream",
            Self::RowStream => "row-stream",
        }
    }
}

/// How a scan turned page bytes into columns.
///
/// The other axis of [`ScanPath`], and the one that matters when timing the tiled fill.
/// `ColumnMajorDecode::Off` changes this and leaves [`PageSource`] alone, which is why the
/// two are reported separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
pub enum FillStrategy {
    /// Not recorded, or the scan produced no batches to fill.
    #[default]
    Unrecorded,
    /// Rows are not materialised into columns at all: the row and raw-row entries.
    NotApplicable,
    /// One cell at a time, in row order. Every cell pays a column lookup, a match on the
    /// builder variant, and branches on values fixed for the whole column.
    RowMajor,
    /// At least one page filled column-major, a tile at a time, with that dispatch hoisted
    /// out of the inner loop.
    Tiled,
}

impl FillStrategy {
    /// A short stable name, for labelling a benchmark or a profile.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unrecorded => "unrecorded",
            Self::NotApplicable => "n/a",
            Self::RowMajor => "row-major",
            Self::Tiled => "tiled",
        }
    }
}

/// Which decode pipeline a scan ran through, on both axes.
///
/// This crate has several, chosen by a chain of conditions spread over the builder, the
/// batch plan and the page descriptors, and **the symbols below the split are shared**, so a
/// flat profile of one is indistinguishable from a flat profile of another. That has already
/// cost one wrong conclusion: an optimisation was written for the tiled fill, measured
/// through `visit_batches`, and appeared to do nothing because `visit_batches` never reaches
/// it.
///
/// The two axes are deliberately separate. Reporting a single name conflated them, so
/// `ColumnMajorDecode::Off` was indistinguishable from `On`: both changed the fill and
/// neither changed the source.
///
/// Every scan records this in [`ScanStatsSummary::path`], and
/// [`crate::ScanBuilder::predict_path`] answers the same question without a full scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
pub struct ScanPath {
    /// How pages were read and planned.
    pub source: PageSource,
    /// How page bytes became columns.
    pub fill: FillStrategy,
}

impl ScanPath {
    pub(crate) const fn borrowed_batches() -> Self {
        Self {
            source: PageSource::BorrowedStream,
            fill: FillStrategy::RowMajor,
        }
    }

    pub(crate) const fn rows() -> Self {
        Self {
            source: PageSource::RowStream,
            fill: FillStrategy::NotApplicable,
        }
    }

    pub(crate) const fn with_source(self, source: PageSource) -> Self {
        Self { source, ..self }
    }

    /// Whether the tiled column-major fill actually ran. **Ask this before trusting a
    /// benchmark of that fill**: a scan can be on a source that permits tiling and still
    /// fill row-major, because the plan or the page class declined.
    #[must_use]
    pub const fn used_tiled_fill(self) -> bool {
        matches!(self.fill, FillStrategy::Tiled)
    }

    /// `source/fill`, for a benchmark id or a profile label.
    #[must_use]
    pub fn label(self) -> String {
        format!("{}/{}", self.source.as_str(), self.fill.as_str())
    }
}

/// Which scan method a caller intends to use, for [`crate::ScanBuilder::predict_path`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanEntry {
    /// [`crate::ScanBuilder::collect_batches`] and friends: owned columnar batches.
    Batches,
    /// [`crate::ScanBuilder::visit_batches`]: batches borrowing the page.
    BorrowedBatches,
    /// [`crate::ScanBuilder::visit_rows`].
    Rows,
    /// [`crate::ScanBuilder::visit_raw_rows`].
    RawRows,
}

/// User-facing scan statistics returned by all public scan methods.
///
/// `fused_pages` and `indexed_pages` describe SAS7BDAT page layout classes
/// (contiguous uncompressed vs. indexed pointer rows) and are meaningful to
/// users benchmarking on varied corpora. Internal decode-pipeline counters are
/// not exposed here.
#[derive(Debug, Clone, Copy, Serialize, Default)]
pub struct ScanStatsSummary {
    pub rows_seen: u64,
    pub rows_emitted: u64,
    pub pages_seen: u64,
    /// Contiguous uncompressed pages — reflects SAS file structure.
    pub fused_pages: u64,
    /// Indexed pointer pages — reflects SAS file structure.
    pub indexed_pages: u64,
    pub compressed_pages: u64,
    pub raw_bytes_read: u64,
    pub row_bytes_materialized: u64,
    pub decode_batches: u64,
    /// Which pipeline decoded this scan. See [`ScanPath`].
    pub path: ScanPath,
}

use batch::{BatchAccumulator, BatchDecodePlan, unexpected_batch_cell};
use numeric::{
    DateNumericValue, DateTimeNumericValue, SAS_NUMERIC_MISSING_SENTINEL, TimeNumericValue,
    TypedNumericValue, classify_date_numeric_value, classify_datetime_numeric_value,
    classify_time_numeric_value, classify_typed_numeric_value, decode_numeric_cell,
    f64_is_i64_representable, materialize_staged_numeric_column, numeric_bits,
    numeric_bits_is_missing, staged_numeric_raw_bits_from_planned_cell,
};
use plan::{
    ColumnMaterializationKind, CompiledColumnPlan, CompiledDecodeKernel, NumericTileMode,
    OwnedCellMaterializationKind, compile_column_plan, compile_compiled_projection_column_plan,
    compile_owned_materialization_kind, compile_string_decode_kernel,
};
use row_decode::{
    DecodedUtf8BatchValue, PlannedCell, RowDecodePlan, StringDecodeKernel, TrimmedString,
    materialize_planned_cells,
};
#[cfg(test)]
use string::trim_and_classify_ascii;
use string::{is_blank_after_trim_mode, mojibake_repaired, trim_and_classify_for_mode};

#[derive(Debug, Clone, Default)]
struct ScanStats {
    pub rows_seen: u64,
    pub rows_emitted: u64,
    pub pages_seen: u64,
    pub fused_pages: u64,
    pub indexed_pages: u64,
    pub compressed_pages: u64,
    pub raw_bytes_read: u64,
    pub row_bytes_materialized: u64,
    pub decode_batches: u64,
    pub batch_staged_numeric_cells: u64,
    pub batch_direct_raw_bytes_cells: u64,
    pub batch_direct_utf8_single_byte_cells: u64,
    pub batch_direct_utf8_borrowed_cells: u64,
    pub batch_direct_utf8_owned_cells: u64,
    pub batch_direct_utf8_owned_interned_hits: u64,
    pub batch_direct_utf8_owned_seen_once_promotions: u64,
    pub batch_fallback_cells: u64,
    pub path: ScanPath,
}

impl ScanStats {
    #[must_use]
    pub(crate) const fn summary(&self) -> crate::ScanStatsSummary {
        crate::ScanStatsSummary {
            rows_seen: self.rows_seen,
            rows_emitted: self.rows_emitted,
            pages_seen: self.pages_seen,
            fused_pages: self.fused_pages,
            indexed_pages: self.indexed_pages,
            compressed_pages: self.compressed_pages,
            raw_bytes_read: self.raw_bytes_read,
            row_bytes_materialized: self.row_bytes_materialized,
            decode_batches: self.decode_batches,
            path: self.path,
        }
    }
}

/// Timing breakdown for owned batch scans.
///
/// Used by profiling and backend comparison tools to inspect the scan pipeline
/// without exposing the internal accumulator. Not part of the general-purpose
/// scan API.
#[derive(Debug, Clone, Default)]
pub struct OwnedBatchScanBreakdown {
    pub total_ns: u128,
    pub plan_ns: u128,
    pub scan_row_bytes_ns: u128,
    pub push_row_ns: u128,
    pub take_batch_ns: u128,
    pub reset_after_flush_ns: u128,
    pub batches_emitted: u64,
    pub stats: crate::ScanStatsSummary,
}

/// A progress snapshot delivered to the observer registered with [`ScanBuilder::with_progress`].
#[derive(Debug, Clone, Copy, Default)]
pub struct ScanProgress {
    pub pages_seen: u64,
    pub total_pages: u64,
    pub raw_bytes_read: u64,
    pub estimated_total_bytes: u64,
    pub compressed_pages: u64,
    pub rows_seen: u64,
    pub rows_emitted: u64,
}

/// A boxed progress callback registered via [`ScanBuilder::with_progress`].
pub type ScanProgressObserver = Arc<dyn Fn(ScanProgress) + Send + Sync + 'static>;

#[cfg(test)]
mod tests;
