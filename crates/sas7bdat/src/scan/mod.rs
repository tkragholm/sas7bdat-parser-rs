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
    simd::{Simd, cmp::SimdPartialEq, num::SimdUint},
    sync::Arc,
};

mod batch;
mod builder;
mod numeric;
mod plan;
mod raw;
mod row_decode;
mod string;

pub use builder::ScanBuilder;

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
}

use batch::{BatchAccumulator, BatchDecodePlan, unexpected_batch_cell};
use numeric::{
    DateNumericValue, DateTimeNumericValue, SAS_NUMERIC_MISSING_SENTINEL, TimeNumericValue,
    TypedNumericValue, classify_date_numeric_value, classify_datetime_numeric_value,
    classify_time_numeric_value, classify_typed_numeric_value, decode_numeric_cell,
    f64_is_i64_representable, materialize_staged_numeric_column, numeric_bits,
    numeric_bits_is_missing, staged_numeric_raw_bits_from_planned_cell, NUMERIC_EXP_MASK,
    NUMERIC_FRACTION_MASK,
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
use string::{
    is_blank_after_trim_mode, maybe_fix_mojibake, mojibake_fix_maybe_needed_for_encoded_bytes,
    trim_and_classify_for_mode,
};

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
    pub batch_direct_numeric_cells: u64,
    pub batch_direct_raw_bytes_cells: u64,
    pub batch_direct_utf8_single_byte_cells: u64,
    pub batch_direct_utf8_borrowed_cells: u64,
    pub batch_direct_utf8_owned_cells: u64,
    pub batch_direct_utf8_owned_interned_hits: u64,
    pub batch_direct_utf8_owned_seen_once_promotions: u64,
    pub batch_fallback_cells: u64,
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

/// Push-based sink for raw (undecoded) rows.
///
/// Implement this trait to receive each row as its raw byte slice. Useful when
/// you want to pass rows directly to another parser or write them verbatim.
/// Use [`ScanBuilder::write_raw_rows`] to drive the scan.
pub trait RawRowSink {
    /// # Errors
    ///
    /// Returns an error if the sink cannot accept the raw row.
    fn push(&mut self, row: RawRow<'_>) -> Result<ControlFlow<()>>;
}

/// Push-based sink for decoded rows.
///
/// Implement this trait to receive each row as a [`RowView`]. This is the
/// push-based alternative to the `visit_rows` callback. Use
/// [`ScanBuilder::write_rows`] to drive the scan.
pub trait RowSink {
    /// # Errors
    ///
    /// Returns an error if the sink cannot accept the decoded row.
    fn push(&mut self, row: RowView<'_>) -> Result<ControlFlow<()>>;
}

/// Push-based sink for decoded columnar batches.
///
/// Implement this trait to receive each batch as a [`ColumnarBatch`]. This is
/// the push-based alternative to the `visit_batches` callback. Use
/// [`ScanBuilder::write_batches`] to drive the scan.
pub trait BatchSink {
    /// # Errors
    ///
    /// Returns an error if the sink cannot accept the decoded batch.
    fn push(&mut self, batch: ColumnarBatch<'_>) -> Result<ControlFlow<()>>;
}

#[cfg(test)]
mod tests;
