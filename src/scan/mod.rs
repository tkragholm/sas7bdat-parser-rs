use crate::{
    columnar::{ColumnBuffer, ColumnarBatch, OwnedColumnBuffer, OwnedColumnarBatch},
    compression::decompress_row,
    dataset::Dataset,
    encoding::resolve_encoding,
    error::{Error, Result},
    internal::{FileSource, PageDescriptor, ProjectedColumnPlan, RowSpan, RowSpanKind},
    metadata::{ColumnMeta, Endianness, LogicalType, SasDate, SasDateTime, SasTime},
    options::{
        BatchHint, DecodeMode, MojibakePolicy, OrderingMode, Parallelism, RowSelection,
        StringDecodeOptions, TemporalDecodeOptions, Utf8ValidationMode,
    },
    projection::Projection,
    row::{OwnedRow, RawRow, RowView},
};
use encoding_rs::{Encoding, UTF_8};
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

use batch::{BatchAccumulator, BatchDecodePlan, borrow_column_buffers, unexpected_batch_cell};
use numeric::{
    DateNumericValue, DateTimeNumericValue, SAS_NUMERIC_MISSING_SENTINEL, TimeNumericValue,
    TypedNumericValue, classify_date_numeric_value, classify_datetime_numeric_value,
    classify_time_numeric_value, classify_typed_numeric_value, decode_numeric_cell,
    decode_numeric_raw_bits_or_missing, materialize_staged_numeric_column, numeric_bits,
    numeric_bits_is_missing, staged_numeric_raw_bits_from_planned_cell,
};
use plan::{
    ColumnMaterializationKind, CompiledColumnPlan, CompiledDecodeKernel, NumericTileMode,
    OwnedCellMaterializationKind, compile_column_plan, compile_compiled_projection_column_plan,
    compile_owned_materialization_kind, compile_string_decode_kernel,
    effective_scan_row_capacity_hint, resolve_batch_row_capacity,
};
use raw::{scan_raw_rows, scan_row_bytes};
use row_decode::{
    DecodedUtf8BatchValue, PlannedCell, RowDecodePlan, StringDecodeKernel, TrimmedString,
    materialize_planned_cells,
};
use string::{
    maybe_fix_mojibake, mojibake_fix_maybe_needed_for_encoded_bytes, trim_and_classify_ascii,
};

#[derive(Debug, Clone, Default)]
pub struct ScanStats {
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
    pub batch_fallback_cells: u64,
}

#[doc(hidden)]
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

pub type ScanProgressObserver = Arc<dyn Fn(ScanProgress) + Send + Sync + 'static>;

pub trait RawRowSink {
    /// # Errors
    ///
    /// Returns an error if the sink cannot accept the raw row.
    fn push(&mut self, row: RawRow<'_>) -> Result<ControlFlow<()>>;
}

pub trait RowSink {
    /// # Errors
    ///
    /// Returns an error if the sink cannot accept the decoded row.
    fn push(&mut self, row: RowView<'_>) -> Result<ControlFlow<()>>;
}

pub trait BatchSink {
    /// # Errors
    ///
    /// Returns an error if the sink cannot accept the decoded batch.
    fn push(&mut self, batch: ColumnarBatch<'_>) -> Result<ControlFlow<()>>;
}

#[cfg(test)]
mod tests;
