use super::{
    ColumnMaterializationKind, CompiledColumnPlan, CompiledDecodeKernel, Endianness, Error,
    NumericTileMode, OwnedColumnBuffer, OwnedColumnarBatch, PlannedCell, Result, RowDecodePlan,
    SAS_NUMERIC_MISSING_SENTINEL, SasDate, SasDateTime, SasTime, ScanBuilder, StringDecodeKernel,
    TrimMode, TrimmedString, TypedNumericValue, classify_typed_numeric_value,
    f64_is_i64_representable, materialize_staged_numeric_column, numeric_bits,
    numeric_bits_is_missing, staged_numeric_raw_bits_from_planned_cell,
};
use crate::columnar::{ColumnBuffer, ColumnarBatch, TrustedOffsets};
use crate::define_owned_column_enum;
use crate::simd::gather_missing;
use rayon::prelude::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use simdutf8::basic::from_utf8 as simd_from_utf8;
use std::ops::ControlFlow;

mod dict;
mod families;

pub(super) use families::{BatchDecodePlan, BatchPlanFlags, DirectUtf8OwnedMode};
mod strings;

use strings::{
    DirectUtf8OwnedBreakdown, append_direct_raw_bytes_batch_column,
    append_direct_utf8_borrowed_batch_column, append_direct_utf8_owned_batch_column,
    append_direct_utf8_single_byte_batch_column, push_utf8_bytes_fast,
};

use dict::{
    DICT_ID_NONE, MAX_STAGED_STRING_WIDTH, StageLookupHit, StagedStringLookup, push_dictionary_id,
    staged_entry_to_dictionary_id,
};

/// Ceiling on one column's pre-allocated variable-width buffer.
///
/// Advisory, like the row hint in `scan::plan`: the buffer grows on demand, so this
/// changes nothing except how much a pathological width claim pre-allocates. 64 MiB is
/// far above any real batch — a 65,536-row batch of 8-byte numerics is 512 KiB.
const MAX_PREALLOC_BYTES_PER_COLUMN: usize = 64 << 20;

#[derive(Debug)]
pub(super) struct BatchAccumulator {
    plan: BatchDecodePlan,
    target_rows: usize,
    capacity_hint_rows: usize,
    row_base: Option<u64>,
    row_count: usize,
    columns: Vec<OwnedBatchColumnBuilder>,
    owned_strings: Vec<String>,
    utf8_decode_scratch: String,
    staged_string_lookups: Vec<Option<StagedStringLookup>>,
    counters: BatchFamilyCounters,
    materialize_threads: usize,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct BatchFamilyCounters {
    pub(super) staged_numeric: u64,
    pub(super) direct_raw_bytes: u64,
    pub(super) direct_utf8_single_byte: u64,
    pub(super) direct_utf8_borrowed: u64,
    pub(super) direct_utf8_owned: u64,
    pub(super) direct_utf8_owned_interned_hits: u64,
    pub(super) direct_utf8_owned_seen_once_promotions: u64,
    pub(super) fallback: u64,
}

define_owned_column_enum! {
    #[derive(Debug)]
    pub(super) enum OwnedBatchColumnBuilder {
        StagedNumeric {
            raw_bits: Vec<u64>,
            mode: NumericTileMode,
            has_missing: bool,
        },
    }
}

impl BatchAccumulator {
    pub(super) fn new(
        plan: BatchDecodePlan,
        target_rows: usize,
        capacity_hint_rows: usize,
    ) -> Self {
        let mut staged_string_lookups = Vec::with_capacity(plan.row_plan.columns.len());
        staged_string_lookups.resize_with(plan.row_plan.columns.len(), || None);
        for &idx in &plan.staged_string_lookup_indices {
            staged_string_lookups[idx] = Some(StagedStringLookup::new());
        }
        let mut columns: Vec<OwnedBatchColumnBuilder> = plan
            .row_plan
            .columns
            .iter()
            .zip(plan.column_kinds.iter().copied())
            .map(|(column, kind)| {
                OwnedBatchColumnBuilder::with_capacity_hint(
                    kind,
                    capacity_hint_rows,
                    column.width,
                    column.numeric_tile,
                    if matches!(column.kernel, CompiledDecodeKernel::Utf8)
                        && matches!(
                            plan.row_plan.string_kernel,
                            StringDecodeKernel::EncodedStrict | StringDecodeKernel::EncodedLenient
                        )
                    {
                        2
                    } else {
                        1
                    },
                )
            })
            .collect();
        for &idx in &plan.families.direct_utf8_owned {
            if staged_string_lookups.get(idx).is_some_and(Option::is_some)
                && let Some(OwnedBatchColumnBuilder::Utf8 { dictionary_ids, .. }) =
                    columns.get_mut(idx)
            {
                dictionary_ids.replace(Vec::with_capacity(capacity_hint_rows.max(1)));
            }
        }
        Self {
            plan,
            target_rows: target_rows.max(1),
            capacity_hint_rows: capacity_hint_rows.max(1),
            row_base: None,
            row_count: 0,
            columns,
            owned_strings: Vec::new(),
            utf8_decode_scratch: String::new(),
            staged_string_lookups,
            counters: BatchFamilyCounters::default(),
            materialize_threads: 1,
        }
    }

    pub(super) fn with_materialize_threads(mut self, threads: usize) -> Self {
        self.materialize_threads = threads.max(1);
        self
    }

    pub(super) const fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    pub(super) const fn is_full(&self) -> bool {
        self.row_count >= self.target_rows
    }

    pub(super) const fn row_count(&self) -> usize {
        self.row_count
    }

    pub(super) const fn target_rows(&self) -> usize {
        self.target_rows
    }

    /// Whether every column in the plan is a staged numeric tile — the precondition
    /// for the column-major contiguous fill path.
    pub(super) const fn plan_is_all_columns_staged_numeric(&self) -> bool {
        self.plan
            .flags
            .has(BatchPlanFlags::ALL_COLUMNS_STAGED_NUMERIC)
    }

    /// Whether a contiguous span can be filled column-major. The condition itself lives on
    /// the plan, so the accumulator and the plan cannot answer this differently.
    pub(super) const fn plan_can_fill_span_column_major(&self) -> bool {
        self.plan.can_fill_span_column_major()
    }

    /// Column-major decode of a fixed-stride, contiguous row span into the staged-numeric
    /// builders. Valid only when the plan is all-staged-numeric (every column is a numeric
    /// tile). Fills `span_len` rows starting at page row `span_first_row`.
    ///
    /// This is the transpose of [`Self::push_staged_numeric_family`]: instead of touching
    /// every column's `raw_bits` tail once per row (per-cell column lookup + builder enum
    /// dispatch), it hoists the dispatch out of the inner loop and fills one column at a time
    /// (one hot write tail, a tight vectorizable gather). The rows are processed in cache-sized
    /// tiles ([`contiguous_tile_rows`]) so the strided reads of a wide row stay L2-resident
    /// instead of re-streaming the whole page once per column. The caller guarantees
    /// `span_len <= target_rows - row_count` so the span fits the current batch.
    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    pub(super) fn push_contiguous_span_all_staged_numeric(
        &mut self,
        page_row_base: u64,
        page: &[u8],
        span_first_row: usize,
        span_len: usize,
        data_start: usize,
        row_len: usize,
    ) -> Result<()> {
        debug_assert!(self.plan_is_all_columns_staged_numeric());
        if span_len == 0 {
            return Ok(());
        }
        self.fill_staged_numeric_span(
            page_row_base,
            page,
            span_first_row,
            span_len,
            data_start,
            row_len,
        )?;
        self.row_count += span_len;
        Ok(())
    }

    /// Column-major fill of a span for a plan whose columns are *not* all staged numeric:
    /// the numerics take the same tiled fill, then the remaining families are filled row by
    /// row over the same span. Without this a single string column drops every numeric
    /// column to the row-major path.
    pub(super) fn push_contiguous_span_mixed(
        &mut self,
        page_row_base: u64,
        page: &[u8],
        span_first_row: usize,
        span_len: usize,
        data_start: usize,
        row_len: usize,
    ) -> Result<()> {
        if span_len == 0 {
            return Ok(());
        }
        if self.plan.flags.has(BatchPlanFlags::HAS_STAGED_NUMERIC) {
            self.fill_staged_numeric_span(
                page_row_base,
                page,
                span_first_row,
                span_len,
                data_start,
                row_len,
            )?;
        } else if self.row_base.is_none() {
            self.row_base = Some(page_row_base.saturating_add(span_first_row as u64));
        }

        let span_base = data_start + span_first_row * row_len;
        for offset in 0..span_len {
            let start = span_base + offset * row_len;
            let row = page
                .get(start..start + row_len)
                .ok_or_else(|| Error::corruption("contiguous span row out of page bounds"))?;
            self.push_row_families_except_staged_numeric(row)?;
        }

        self.row_count += span_len;
        Ok(())
    }

    /// Everything [`Self::push_row`] does apart from the staged-numeric family and the row
    /// count, for use when the numerics were already filled column-major.
    fn push_row_families_except_staged_numeric(&mut self, row: &[u8]) -> Result<()> {
        if self
            .plan
            .flags
            .has(BatchPlanFlags::NEEDS_OWNED_STRING_SCRATCH)
        {
            self.owned_strings.clear();
        }
        if self.plan.flags.has(BatchPlanFlags::HAS_DIRECT_RAW_BYTES) {
            self.push_direct_raw_bytes_family(row)?;
        }
        if self
            .plan
            .flags
            .has(BatchPlanFlags::HAS_DIRECT_UTF8_SINGLE_BYTE)
        {
            self.push_direct_utf8_single_byte_family(row)?;
        }
        if self
            .plan
            .flags
            .has(BatchPlanFlags::HAS_DIRECT_UTF8_BORROWED)
        {
            self.push_direct_utf8_borrowed_family(row)?;
        }
        if self.plan.flags.has(BatchPlanFlags::HAS_DIRECT_UTF8_OWNED) {
            self.push_direct_utf8_owned_family(row)?;
        }
        if self.plan.flags.has(BatchPlanFlags::HAS_FALLBACK) {
            self.push_fallback_family(row)?;
        }
        Ok(())
    }

    /// The tiled column-major fill itself, shared by the all-numeric and mixed entry points.
    /// Does not touch `row_count`: the caller owns that, because the mixed case has other
    /// families to fill over the same span first.
    fn fill_staged_numeric_span(
        &mut self,
        page_row_base: u64,
        page: &[u8],
        span_first_row: usize,
        span_len: usize,
        data_start: usize,
        row_len: usize,
    ) -> Result<()> {
        if span_len == 0 {
            return Ok(());
        }

        // The whole span, and every column field within each row's stride, must lie in `page`.
        let max_end = self.plan.row_plan.max_end;
        let span_end_byte = span_first_row
            .checked_add(span_len)
            .and_then(|rows| rows.checked_mul(row_len))
            .and_then(|span_bytes| span_bytes.checked_add(data_start))
            .ok_or_else(|| Error::corruption("contiguous span byte range overflow"))?;
        if row_len < max_end || span_end_byte > page.len() {
            return Err(Error::corruption("contiguous span exceeds page bounds"));
        }

        if self.row_base.is_none() {
            self.row_base = Some(page_row_base.saturating_add(span_first_row as u64));
        }

        let endianness = self.plan.row_plan.endianness;
        let span_base = data_start + span_first_row * row_len;

        // Reserve each column's tail for the whole span up front so the tiled pushes below
        // never reallocate mid-tile.
        for op in &self.plan.staged_numeric_ops {
            let OwnedBatchColumnBuilder::StagedNumeric { raw_bits, .. } = &mut self.columns[op.idx]
            else {
                return Err(Error::unsupported(
                    "column-major staged-numeric plan did not match column builder",
                ));
            };
            raw_bits.reserve(span_len);
        }

        // Tile the rows so each tile's *read* footprint (`tile_rows * row_len`) stays
        // cache-resident, then transpose within the tile (column-outer, row-inner). The
        // non-tiled gather streams the whole span once per column: for wide rows (row_len ≫
        // cache line) every column re-reads the entire page region from memory, so the win
        // collapses as width grows. Blocking bounds the re-read to one L2-resident tile —
        // the page is read from memory ~once while keeping the per-column dispatch hoisted
        // out of the inner loop and one hot write tail at a time.
        let tile_rows = contiguous_tile_rows(row_len);
        let mut tile_start = 0usize;
        while tile_start < span_len {
            let tile_len = tile_rows.min(span_len - tile_start);
            let tile_base = span_base + tile_start * row_len;
            for op in &self.plan.staged_numeric_ops {
                let OwnedBatchColumnBuilder::StagedNumeric {
                    raw_bits,
                    has_missing,
                    ..
                } = &mut self.columns[op.idx]
                else {
                    return Err(Error::unsupported(
                        "column-major staged-numeric plan did not match column builder",
                    ));
                };
                let field_len = op.end - op.start;
                if field_len == 0 {
                    raw_bits.extend(std::iter::repeat_n(SAS_NUMERIC_MISSING_SENTINEL, tile_len));
                    *has_missing = true;
                    continue;
                }

                let mut off = tile_base + op.start;
                let mut missing = false;
                if field_len == 8 && matches!(endianness, Endianness::Little) {
                    // Dominant case: full-width 8-byte native-endian numeric. SIMD-assisted
                    // strided gather (vectorized missing-test + bulk store).
                    missing |= gather_staged_8byte_le(page, off, row_len, tile_len, raw_bits);
                } else if field_len == 8 {
                    for _ in 0..tile_len {
                        let bytes: [u8; 8] = page[off..off + 8].try_into().expect("8-byte field");
                        let raw = u64::from_be_bytes(bytes);
                        missing |= numeric_bits_is_missing(raw);
                        raw_bits.push(raw);
                        off += row_len;
                    }
                } else {
                    for _ in 0..tile_len {
                        let raw = numeric_bits(&page[off..off + field_len], endianness);
                        missing |= numeric_bits_is_missing(raw);
                        raw_bits.push(raw);
                        off += row_len;
                    }
                }
                *has_missing |= missing;
            }
            tile_start += tile_len;
        }

        self.counters.staged_numeric +=
            (span_len as u64).saturating_mul(self.plan.staged_numeric_cells_per_row);
        Ok(())
    }

    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    fn push_all_staged_numeric(&mut self, row: &[u8]) -> Result<()> {
        self.push_staged_numeric_family(row)
    }

    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    fn push_staged_numeric_family(&mut self, row: &[u8]) -> Result<()> {
        self.counters.staged_numeric += self.plan.staged_numeric_cells_per_row;
        for op in &self.plan.staged_numeric_ops {
            let idx = op.idx;
            let batch_column = &mut self.columns[idx];
            let raw = if op.start == op.end {
                SAS_NUMERIC_MISSING_SENTINEL
            } else {
                numeric_bits(&row[op.start..op.end], self.plan.row_plan.endianness)
            };
            let appended = batch_column.append_staged_numeric_bits_fast(raw);
            debug_assert!(appended, "compiled staged numeric batch must match builder");
            if !appended {
                return Err(Error::unsupported(
                    "compiled staged numeric batch plan did not match column builder",
                ));
            }
        }
        Ok(())
    }
    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    fn push_direct_raw_bytes_family(&mut self, row: &[u8]) -> Result<()> {
        self.counters.direct_raw_bytes += self.plan.direct_raw_bytes_cells_per_row;
        for &idx in &self.plan.families.direct_raw_bytes {
            let batch_column = &mut self.columns[idx];
            let column = &self.plan.row_plan.columns[idx];
            let appended = append_direct_raw_bytes_batch_column(
                &self.plan.row_plan,
                batch_column,
                column,
                row,
            )?;
            debug_assert!(appended, "compiled raw-bytes batch must match builder");
            if !appended {
                return Err(Error::unsupported(
                    "compiled raw-bytes batch plan did not match column builder",
                ));
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    fn push_direct_utf8_single_byte_family(&mut self, row: &[u8]) -> Result<()> {
        if self
            .plan
            .flags
            .has(BatchPlanFlags::USE_SINGLE_BYTE_UTF8_FAMILY)
        {
            self.counters.direct_utf8_single_byte +=
                self.plan.direct_utf8_single_byte_cells_per_row;
            for &idx in &self.plan.families.direct_utf8_single_byte {
                let batch_column = &mut self.columns[idx];
                let column = &self.plan.row_plan.columns[idx];
                let appended = append_direct_utf8_single_byte_batch_column(
                    &self.plan.row_plan,
                    batch_column,
                    column,
                    row,
                    &mut self.utf8_decode_scratch,
                )?;
                debug_assert!(
                    appended,
                    "compiled single-byte utf8 batch must match builder"
                );
                if !appended {
                    return Err(Error::unsupported(
                        "compiled single-byte utf8 batch plan did not match column builder",
                    ));
                }
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    fn push_direct_utf8_borrowed_family(&mut self, row: &[u8]) -> Result<()> {
        self.counters.direct_utf8_borrowed += self.plan.direct_utf8_borrowed_cells_per_row;
        for &idx in &self.plan.families.direct_utf8_borrowed {
            let batch_column = &mut self.columns[idx];
            let column = &self.plan.row_plan.columns[idx];
            let appended = append_direct_utf8_borrowed_batch_column(
                &self.plan.row_plan,
                batch_column,
                column,
                row,
            )?;
            debug_assert!(appended, "compiled borrowed utf8 batch must match builder");
            if !appended {
                return Err(Error::unsupported(
                    "compiled borrowed utf8 batch plan did not match column builder",
                ));
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    fn push_direct_utf8_owned_family(&mut self, row: &[u8]) -> Result<()> {
        if let Some(mode) = self.plan.direct_utf8_owned_mode {
            self.counters.direct_utf8_owned += self.plan.direct_utf8_owned_cells_per_row;
            let mut breakdown = DirectUtf8OwnedBreakdown::default();
            for &idx in &self.plan.families.direct_utf8_owned {
                let batch_column = &mut self.columns[idx];
                let column = &self.plan.row_plan.columns[idx];
                let staged_lookup = self.staged_string_lookups[idx].as_mut();
                let appended = match mode {
                    DirectUtf8OwnedMode::Utf8Lenient => append_direct_utf8_owned_batch_column(
                        (&self.plan.row_plan, column, row),
                        batch_column,
                        &mut self.utf8_decode_scratch,
                        staged_lookup,
                        |p, t, s| Ok(p.decode_utf8_lenient_trimmed_bytes_for_batch_direct(t, s)),
                        true,
                        &mut breakdown,
                    )?,
                    DirectUtf8OwnedMode::EncodedStrict => append_direct_utf8_owned_batch_column(
                        (&self.plan.row_plan, column, row),
                        batch_column,
                        &mut self.utf8_decode_scratch,
                        staged_lookup,
                        RowDecodePlan::decode_encoded_strict_trimmed_bytes_for_batch_direct,
                        false,
                        &mut breakdown,
                    )?,
                    DirectUtf8OwnedMode::EncodedLenient => append_direct_utf8_owned_batch_column(
                        (&self.plan.row_plan, column, row),
                        batch_column,
                        &mut self.utf8_decode_scratch,
                        staged_lookup,
                        |p, t, s| Ok(p.decode_encoded_lenient_trimmed_bytes_for_batch_direct(t, s)),
                        false,
                        &mut breakdown,
                    )?,
                };
                debug_assert!(appended, "compiled utf8 batch must match builder");
                if !appended {
                    return Err(Error::unsupported(
                        "compiled owned utf8 batch plan did not match column builder",
                    ));
                }
            }
            self.counters.direct_utf8_owned_interned_hits = self
                .counters
                .direct_utf8_owned_interned_hits
                .saturating_add(breakdown.interned_hits);
            self.counters.direct_utf8_owned_seen_once_promotions = self
                .counters
                .direct_utf8_owned_seen_once_promotions
                .saturating_add(breakdown.seen_once_promotions);
        }
        Ok(())
    }

    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    fn push_fallback_family(&mut self, row: &[u8]) -> Result<()> {
        self.counters.fallback += self.plan.fallback_cells_per_row;
        for &idx in &self.plan.families.fallback {
            let batch_column = &mut self.columns[idx];
            let column = &self.plan.row_plan.columns[idx];
            let cell =
                self.plan
                    .row_plan
                    .plan_cell_in_bounds(row, column, &mut self.owned_strings)?;
            batch_column.append(cell, &self.owned_strings)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    pub(super) fn push_row(&mut self, row_index: u64, row: &[u8]) -> Result<()> {
        if self.row_base.is_none() {
            self.row_base = Some(row_index);
        }

        self.plan.row_plan.validate_row_bounds(row)?;
        if self
            .plan
            .flags
            .has(BatchPlanFlags::ALL_COLUMNS_STAGED_NUMERIC)
        {
            self.push_all_staged_numeric(row)?;
            self.row_count += 1;
            return Ok(());
        }

        if self
            .plan
            .flags
            .has(BatchPlanFlags::HAS_FAST_PATH_STAGED_PLUS_UTF8_OWNED)
        {
            self.push_staged_numeric_family(row)?;
            self.push_direct_utf8_owned_family(row)?;
            self.row_count += 1;
            return Ok(());
        }

        if self
            .plan
            .flags
            .has(BatchPlanFlags::NEEDS_OWNED_STRING_SCRATCH)
        {
            self.owned_strings.clear();
        }

        if self.plan.flags.has(BatchPlanFlags::HAS_STAGED_NUMERIC) {
            self.push_staged_numeric_family(row)?;
        }
        if self.plan.flags.has(BatchPlanFlags::HAS_DIRECT_RAW_BYTES) {
            self.push_direct_raw_bytes_family(row)?;
        }
        if self
            .plan
            .flags
            .has(BatchPlanFlags::HAS_DIRECT_UTF8_SINGLE_BYTE)
        {
            self.push_direct_utf8_single_byte_family(row)?;
        }
        if self
            .plan
            .flags
            .has(BatchPlanFlags::HAS_DIRECT_UTF8_BORROWED)
        {
            self.push_direct_utf8_borrowed_family(row)?;
        }
        if self.plan.flags.has(BatchPlanFlags::HAS_DIRECT_UTF8_OWNED) {
            self.push_direct_utf8_owned_family(row)?;
        }
        if self.plan.flags.has(BatchPlanFlags::HAS_FALLBACK) {
            self.push_fallback_family(row)?;
        }

        self.row_count += 1;
        Ok(())
    }

    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    pub(super) fn take_batch(&mut self) -> Result<OwnedColumnarBatch> {
        let row_base = self.row_base.unwrap_or(0);
        let row_count = self.row_count;
        let columns =
            finish_columns_ordered(std::mem::take(&mut self.columns), self.materialize_threads);
        self.check_integer_contract(&columns, row_base)?;
        self.row_base = None;
        self.row_count = 0;
        Ok(OwnedColumnarBatch {
            row_base: crate::types::RowIndex(row_base),
            row_count,
            columns,
        })
    }

    /// Error if a column planned as Integer (only possible via a schema override)
    /// materialized as F64 because a value was non-integral or out of i64 range.
    /// `columns` must be indexed like `plan.row_plan.columns` (full batch order).
    fn check_integer_contract(&self, columns: &[OwnedColumnBuffer], row_base: u64) -> Result<()> {
        for &idx in &self.plan.families.staged_numeric {
            if matches!(
                self.plan.row_plan.columns[idx].numeric_tile,
                Some(NumericTileMode::IntegerWidth8)
            ) && matches!(columns[idx], OwnedColumnBuffer::F64 { .. })
            {
                return Err(integer_override_violation(
                    &self.plan.row_plan.names[idx],
                    row_base,
                    &columns[idx],
                ));
            }
        }
        Ok(())
    }

    pub(super) fn reset_after_flush(&mut self) {
        self.columns = self
            .plan
            .row_plan
            .columns
            .iter()
            .zip(self.plan.column_kinds.iter().copied())
            .map(|(column, kind)| {
                OwnedBatchColumnBuilder::with_capacity_hint(
                    kind,
                    self.capacity_hint_rows,
                    column.width,
                    column.numeric_tile,
                    if matches!(column.kernel, CompiledDecodeKernel::Utf8)
                        && matches!(
                            self.plan.row_plan.string_kernel,
                            StringDecodeKernel::EncodedStrict | StringDecodeKernel::EncodedLenient
                        )
                    {
                        2
                    } else {
                        1
                    },
                )
            })
            .collect();
        for &idx in &self.plan.families.direct_utf8_owned {
            if self
                .staged_string_lookups
                .get(idx)
                .is_some_and(Option::is_some)
                && let Some(OwnedBatchColumnBuilder::Utf8 { dictionary_ids, .. }) =
                    self.columns.get_mut(idx)
            {
                dictionary_ids.replace(Vec::with_capacity(self.capacity_hint_rows.max(1)));
            }
        }
        self.owned_strings.clear();
        self.utf8_decode_scratch.clear();
    }

    pub(super) const fn staged_numeric_count(&self) -> usize {
        self.plan.families.staged_numeric.len()
    }

    /// Reset column buffers for reuse, preserving allocated capacity where possible.
    /// Builders that have widened (e.g. I32 → F64) are recreated from the original plan.
    pub(super) fn reset_for_reuse(&mut self) {
        for (idx, col) in self.columns.iter_mut().enumerate() {
            let planned_kind = self.plan.column_kinds[idx];
            if col.matches_planned_kind(planned_kind) {
                col.clear_for_reuse();
            } else {
                let column = &self.plan.row_plan.columns[idx];
                let utf8_mult = if matches!(column.kernel, CompiledDecodeKernel::Utf8)
                    && matches!(
                        self.plan.row_plan.string_kernel,
                        StringDecodeKernel::EncodedStrict | StringDecodeKernel::EncodedLenient
                    ) {
                    2
                } else {
                    1
                };
                *col = OwnedBatchColumnBuilder::with_capacity_hint(
                    planned_kind,
                    self.capacity_hint_rows,
                    column.width,
                    column.numeric_tile,
                    utf8_mult,
                );
            }
        }
        for &idx in &self.plan.families.direct_utf8_owned {
            if self
                .staged_string_lookups
                .get(idx)
                .is_some_and(Option::is_some)
                && let Some(OwnedBatchColumnBuilder::Utf8 { dictionary_ids, .. }) =
                    self.columns.get_mut(idx)
            {
                match dictionary_ids {
                    None => {
                        dictionary_ids.replace(Vec::with_capacity(self.capacity_hint_rows.max(1)));
                    }
                    Some(ids) => ids.clear(),
                }
            }
        }
        self.owned_strings.clear();
        self.utf8_decode_scratch.clear();
        self.row_base = None;
        self.row_count = 0;
    }

    /// Materialize a borrowed `ColumnarBatch<'_>` view, invoke `f`, then reset for reuse.
    /// `StagedNumeric` columns are materialized into `staged_scratch` first.
    /// `staged_scratch` must have capacity for `self.staged_numeric_count()` entries.
    pub(super) fn flush_borrowed_and_reset<F>(
        &mut self,
        staged_scratch: &mut Vec<OwnedColumnBuffer>,
        f: &mut F,
    ) -> Result<ControlFlow<()>>
    where
        F: FnMut(ColumnarBatch<'_>) -> Result<ControlFlow<()>>,
    {
        staged_scratch.clear();
        for &idx in &self.plan.families.staged_numeric {
            if let OwnedBatchColumnBuilder::StagedNumeric {
                raw_bits,
                mode,
                has_missing,
            } = &self.columns[idx]
            {
                let materialized = materialize_staged_numeric_column(raw_bits, *mode, *has_missing);
                if matches!(*mode, NumericTileMode::IntegerWidth8)
                    && matches!(materialized, OwnedColumnBuffer::F64 { .. })
                {
                    return Err(integer_override_violation(
                        &self.plan.row_plan.names[idx],
                        self.row_base.unwrap_or(0),
                        &materialized,
                    ));
                }
                staged_scratch.push(materialized);
            }
        }

        let mut views: Vec<ColumnBuffer<'_>> = Vec::with_capacity(self.columns.len());
        let mut staged_idx = 0usize;
        for col in &self.columns {
            if let Some(view) = col.borrow_view() {
                views.push(view);
            } else {
                views.push(staged_scratch[staged_idx].as_borrowed());
                staged_idx += 1;
            }
        }

        let row_base = crate::types::RowIndex(self.row_base.unwrap_or(0));
        let row_count = self.row_count;
        let result = f(ColumnarBatch {
            row_base,
            row_count,
            columns: &views,
        })?;

        drop(views);
        self.reset_for_reuse();
        Ok(result)
    }

    pub(super) const fn counters(&self) -> BatchFamilyCounters {
        self.counters
    }

    #[cfg(test)]
    pub(super) fn has_staged_string_lookup_for(&self, idx: usize) -> bool {
        self.staged_string_lookups
            .get(idx)
            .is_some_and(Option::is_some)
    }
}

/// Build the error for a column whose schema override declared it Integer but whose
/// staged values could not all be materialized as i64.
///
/// `LogicalType::Integer` is only ever assigned through an explicit schema override
/// (inference never produces it), so an Integer-planned column that materialized as
/// F64 means the file violates the caller's declared contract. Erroring here — rather
/// than silently emitting Float64 — keeps the declared schema and the materialized
/// batches identical, which downstream consumers (Polars batch stacking, multi-file
/// concat) rely on.
fn integer_override_violation(name: &str, row_base: u64, buffer: &OwnedColumnBuffer) -> Error {
    let detail = if let OwnedColumnBuffer::F64 { values, valid } = buffer {
        values
            .iter()
            .enumerate()
            .find(|&(i, &value)| {
                let is_valid = valid.as_deref().is_none_or(|bits| {
                    bits.get(i / 64)
                        .is_some_and(|word| (word >> (i % 64)) & 1 == 1)
                });
                is_valid && !f64_is_i64_representable(value)
            })
            .map(|(i, value)| format!(" (row {}: value {value})", row_base + i as u64))
            .unwrap_or_default()
    } else {
        String::new()
    };
    Error::decode(format!(
        "column '{name}' is declared Integer by a schema override, but the file \
         contains a non-integral or out-of-i64-range value{detail}; remove the \
         override for this column or fix the source data"
    ))
}

fn finish_columns_ordered(
    columns: Vec<OwnedBatchColumnBuilder>,
    materialize_threads: usize,
) -> Vec<OwnedColumnBuffer> {
    const PARALLEL_MIN_COLUMNS: usize = 16;

    let column_count = columns.len();
    if materialize_threads <= 1 || column_count < PARALLEL_MIN_COLUMNS {
        return columns
            .into_iter()
            .map(OwnedBatchColumnBuilder::finish)
            .collect();
    }

    let workers = materialize_threads.min(column_count);
    let mut merged = columns
        .into_iter()
        .enumerate()
        .collect::<Vec<_>>()
        .into_par_iter()
        .with_max_len(column_count.div_ceil(workers))
        .map(|(idx, column)| (idx, column.finish()))
        .collect::<Vec<_>>();

    merged.sort_unstable_by_key(|(idx, _)| *idx);
    merged.into_iter().map(|(_, column)| column).collect()
}

impl OwnedBatchColumnBuilder {
    pub(super) fn with_capacity_hint(
        kind: ColumnMaterializationKind,
        target_rows: usize,
        width_hint: u32,
        numeric_tile: Option<NumericTileMode>,
        utf8_data_capacity_multiplier: usize,
    ) -> Self {
        // Clamped independently of the row hint `ScanPlan` already bounds, because
        // `width_hint` is a `u32` column width and the UTF-8 multiplier scales on top of
        // it — so the byte product can overshoot even when the row count is sane. Like
        // the row hint this is advisory: both buffers grow on demand.
        let base_variable_capacity = target_rows
            .saturating_mul(usize::try_from(width_hint).unwrap_or(0))
            .min(MAX_PREALLOC_BYTES_PER_COLUMN);
        let utf8_variable_capacity = base_variable_capacity
            .saturating_mul(utf8_data_capacity_multiplier.max(1))
            .min(MAX_PREALLOC_BYTES_PER_COLUMN);
        match kind {
            ColumnMaterializationKind::I32 => Self::I32 {
                values: Vec::with_capacity(target_rows),
                valid: None,
            },
            ColumnMaterializationKind::I64 => Self::I64 {
                values: Vec::with_capacity(target_rows),
                valid: None,
            },
            ColumnMaterializationKind::F64 => Self::F64 {
                values: Vec::with_capacity(target_rows),
                valid: None,
            },
            ColumnMaterializationKind::Date => Self::Date {
                values: Vec::with_capacity(target_rows),
                valid: None,
            },
            ColumnMaterializationKind::DateTime => Self::DateTime {
                values: Vec::with_capacity(target_rows),
                valid: None,
            },
            ColumnMaterializationKind::Time => Self::Time {
                values: Vec::with_capacity(target_rows),
                valid: None,
            },
            ColumnMaterializationKind::Utf8 => Self::Utf8 {
                offsets: TrustedOffsets::with_capacity_for_rows(target_rows),
                data: Vec::with_capacity(utf8_variable_capacity),
                valid: None,
                dictionary_ids: None,
            },
            ColumnMaterializationKind::RawBytes => Self::RawBytes {
                offsets: TrustedOffsets::with_capacity_for_rows(target_rows),
                data: Vec::with_capacity(base_variable_capacity),
                valid: None,
            },
        }
        .with_numeric_tile(target_rows, numeric_tile)
    }

    pub(super) fn with_numeric_tile(
        self,
        target_rows: usize,
        numeric_tile: Option<NumericTileMode>,
    ) -> Self {
        match (self, numeric_tile) {
            (_, Some(mode)) => Self::StagedNumeric {
                raw_bits: Vec::with_capacity(target_rows),
                mode,
                has_missing: false,
            },
            (builder, _) => builder,
        }
    }

    #[inline]
    pub(super) fn append_staged_numeric_bits_fast(&mut self, raw: u64) -> bool {
        match self {
            Self::StagedNumeric {
                raw_bits,
                has_missing,
                ..
            } => {
                *has_missing |= numeric_bits_is_missing(raw);
                raw_bits.push(raw);
                true
            }
            _ => false,
        }
    }

    fn append_with_temporal_widening(
        &mut self,
        cell: PlannedCell,
        owned_strings: &[String],
    ) -> Result<()> {
        self.widen_temporal_to_f64();
        self.append(cell, owned_strings)
    }

    #[allow(clippy::too_many_lines)]
    pub(super) fn append(&mut self, cell: PlannedCell, owned_strings: &[String]) -> Result<()> {
        match self {
            Self::I32 { values, valid } => {
                match cell {
                    PlannedCell::Null => push_primitive_null(values, valid, 0),
                    PlannedCell::Int32(value) => push_primitive_valid(values, valid, value),
                    PlannedCell::Int64(value) => {
                        if let Ok(value32) = i32::try_from(value) {
                            push_primitive_valid(values, valid, value32);
                        } else {
                            self.widen_integer_to_f64();
                            return self.append(PlannedCell::Int64(value), owned_strings);
                        }
                    }
                    PlannedCell::Float64(value) => {
                        match classify_typed_numeric_value(Some(value), true) {
                            TypedNumericValue::Int32(value32) => {
                                push_primitive_valid(values, valid, value32);
                            }
                            TypedNumericValue::Float64(_) | TypedNumericValue::Int64(_) => {
                                self.widen_integer_to_f64();
                                return self.append(PlannedCell::Float64(value), owned_strings);
                            }
                            TypedNumericValue::Null => push_primitive_null(values, valid, 0),
                        }
                    }
                    other => return Err(unexpected_batch_cell("i32", other)),
                }
                Ok(())
            }
            Self::I64 { values, valid } => {
                match cell {
                    PlannedCell::Null => push_primitive_null(values, valid, 0),
                    PlannedCell::Int32(value) => {
                        push_primitive_valid(values, valid, i64::from(value));
                    }
                    PlannedCell::Int64(value) => push_primitive_valid(values, valid, value),
                    PlannedCell::Float64(value) => {
                        match classify_typed_numeric_value(Some(value), false) {
                            TypedNumericValue::Int32(value32) => {
                                push_primitive_valid(values, valid, i64::from(value32));
                            }
                            TypedNumericValue::Int64(value64) => {
                                push_primitive_valid(values, valid, value64);
                            }
                            TypedNumericValue::Float64(_) => {
                                self.widen_integer_to_f64();
                                return self.append(PlannedCell::Float64(value), owned_strings);
                            }
                            TypedNumericValue::Null => push_primitive_null(values, valid, 0),
                        }
                    }
                    other => return Err(unexpected_batch_cell("i64", other)),
                }
                Ok(())
            }
            Self::F64 { values, valid } => {
                match cell {
                    PlannedCell::Null => push_primitive_null(values, valid, 0.0),
                    PlannedCell::Int32(value) => {
                        push_primitive_valid(values, valid, f64::from(value));
                    }
                    PlannedCell::Int64(value) => {
                        #[allow(clippy::cast_precision_loss)]
                        let value_f64 = value as f64;
                        push_primitive_valid(values, valid, value_f64);
                    }
                    PlannedCell::Float64(value) => push_primitive_valid(values, valid, value),
                    other => return Err(unexpected_batch_cell("f64", other)),
                }
                Ok(())
            }
            Self::StagedNumeric { raw_bits, .. } => {
                raw_bits.push(staged_numeric_raw_bits_from_planned_cell(cell)?);
                Ok(())
            }
            Self::Date { values, valid } => match cell {
                PlannedCell::Null => {
                    push_primitive_null(
                        values,
                        valid,
                        SasDate {
                            days_since_sas_epoch: 0,
                        },
                    );
                    Ok(())
                }
                PlannedCell::Date(value) => {
                    push_primitive_valid(values, valid, value);
                    Ok(())
                }
                PlannedCell::Int32(_) | PlannedCell::Int64(_) | PlannedCell::Float64(_) => {
                    self.append_with_temporal_widening(cell, owned_strings)
                }
                other => Err(unexpected_batch_cell("date", other)),
            },
            Self::DateTime { values, valid } => match cell {
                PlannedCell::Null => {
                    push_primitive_null(
                        values,
                        valid,
                        SasDateTime {
                            seconds_since_sas_epoch: 0,
                        },
                    );
                    Ok(())
                }
                PlannedCell::DateTime(value) => {
                    push_primitive_valid(values, valid, value);
                    Ok(())
                }
                PlannedCell::Int32(_) | PlannedCell::Int64(_) | PlannedCell::Float64(_) => {
                    self.append_with_temporal_widening(cell, owned_strings)
                }
                other => Err(unexpected_batch_cell("datetime", other)),
            },
            Self::Time { values, valid } => match cell {
                PlannedCell::Null => {
                    push_primitive_null(
                        values,
                        valid,
                        SasTime {
                            seconds_since_midnight: 0,
                        },
                    );
                    Ok(())
                }
                PlannedCell::Time(value) => {
                    push_primitive_valid(values, valid, value);
                    Ok(())
                }
                PlannedCell::Int32(_) | PlannedCell::Int64(_) | PlannedCell::Float64(_) => {
                    self.append_with_temporal_widening(cell, owned_strings)
                }
                other => Err(unexpected_batch_cell("time", other)),
            },
            Self::Utf8 {
                offsets,
                data,
                valid,
                dictionary_ids,
            } => match cell {
                PlannedCell::Null => {
                    push_variable_null(offsets, data, valid);
                    push_dictionary_id(dictionary_ids, DICT_ID_NONE);
                    Ok(())
                }
                PlannedCell::StrBorrowed(value) => {
                    push_utf8_bytes_fast(offsets, data, valid, value.as_bytes())?;
                    push_dictionary_id(dictionary_ids, DICT_ID_NONE);
                    Ok(())
                }
                PlannedCell::StrOwned(index) => {
                    push_utf8_bytes_fast(
                        offsets,
                        data,
                        valid,
                        owned_strings
                            .get(index)
                            .ok_or_else(|| Error::internal("owned string index out of range"))?
                            .as_bytes(),
                    )?;
                    push_dictionary_id(dictionary_ids, DICT_ID_NONE);
                    Ok(())
                }
                other => Err(unexpected_batch_cell("utf8", other)),
            },
            Self::RawBytes {
                offsets,
                data,
                valid,
            } => match cell {
                PlannedCell::Null => {
                    push_variable_null(offsets, data, valid);
                    Ok(())
                }
                PlannedCell::Bytes(value) => {
                    push_variable_valid(offsets, data, valid, value)?;
                    Ok(())
                }
                other => Err(unexpected_batch_cell("raw-bytes", other)),
            },
        }
    }

    pub(super) fn widen_temporal_to_f64(&mut self) {
        let widened = match std::mem::replace(
            self,
            Self::F64 {
                values: Vec::new(),
                valid: None,
            },
        ) {
            Self::Date { values, valid } => Self::F64 {
                values: values
                    .into_iter()
                    .map(|value| f64::from(value.days_since_sas_epoch))
                    .collect(),
                valid,
            },
            Self::DateTime { values, valid } => Self::F64 {
                values: values
                    .into_iter()
                    .map(|value| {
                        #[allow(clippy::cast_precision_loss)]
                        let v = value.seconds_since_sas_epoch as f64;
                        v
                    })
                    .collect(),
                valid,
            },
            Self::Time { values, valid } => Self::F64 {
                values: values
                    .into_iter()
                    .map(|value| f64::from(value.seconds_since_midnight))
                    .collect(),
                valid,
            },
            other => other,
        };
        *self = widened;
    }

    pub(super) fn widen_integer_to_f64(&mut self) {
        let widened = match std::mem::replace(
            self,
            Self::F64 {
                values: Vec::new(),
                valid: None,
            },
        ) {
            Self::I32 { values, valid } => Self::F64 {
                values: values.into_iter().map(f64::from).collect(),
                valid,
            },
            Self::I64 { values, valid } => Self::F64 {
                values: values
                    .into_iter()
                    .map(|value| {
                        #[allow(clippy::cast_precision_loss)]
                        let v = value as f64;
                        v
                    })
                    .collect(),
                valid,
            },
            other => other,
        };
        *self = widened;
    }

    /// Borrow a `ColumnBuffer<'_>` view without consuming the builder.
    /// Returns `None` for `StagedNumeric`, which requires external materialization first.
    pub(super) fn borrow_view(&self) -> Option<ColumnBuffer<'_>> {
        use crate::columnar::{BytesBuffer, PrimitiveBuffer, Utf8Buffer};
        match self {
            Self::I32 { values, valid } => Some(ColumnBuffer::I32(PrimitiveBuffer {
                values,
                valid: valid.as_deref(),
            })),
            Self::I64 { values, valid } => Some(ColumnBuffer::I64(PrimitiveBuffer {
                values,
                valid: valid.as_deref(),
            })),
            Self::F64 { values, valid } => Some(ColumnBuffer::F64(PrimitiveBuffer {
                values,
                valid: valid.as_deref(),
            })),
            Self::Date { values, valid } => Some(ColumnBuffer::Date(PrimitiveBuffer {
                values,
                valid: valid.as_deref(),
            })),
            Self::DateTime { values, valid } => Some(ColumnBuffer::DateTime(PrimitiveBuffer {
                values,
                valid: valid.as_deref(),
            })),
            Self::Time { values, valid } => Some(ColumnBuffer::Time(PrimitiveBuffer {
                values,
                valid: valid.as_deref(),
            })),
            Self::Utf8 {
                offsets,
                data,
                valid,
                dictionary_ids,
            } => Some(ColumnBuffer::Utf8(Utf8Buffer {
                offsets: offsets.as_slice(),
                data,
                valid: valid.as_deref(),
                dictionary_ids: dictionary_ids.as_deref(),
            })),
            Self::RawBytes {
                offsets,
                data,
                valid,
            } => Some(ColumnBuffer::RawBytes(BytesBuffer {
                offsets: offsets.as_slice(),
                data,
                valid: valid.as_deref(),
            })),
            Self::StagedNumeric { .. } => None,
        }
    }

    /// Returns whether the builder's current discriminant matches the planned initial kind.
    /// `StagedNumeric` always returns `true` — it cannot widen.
    pub(super) const fn matches_planned_kind(&self, kind: ColumnMaterializationKind) -> bool {
        match self {
            Self::StagedNumeric { .. } => true,
            Self::I32 { .. } => matches!(kind, ColumnMaterializationKind::I32),
            Self::I64 { .. } => matches!(kind, ColumnMaterializationKind::I64),
            Self::F64 { .. } => matches!(kind, ColumnMaterializationKind::F64),
            Self::Date { .. } => matches!(kind, ColumnMaterializationKind::Date),
            Self::DateTime { .. } => matches!(kind, ColumnMaterializationKind::DateTime),
            Self::Time { .. } => matches!(kind, ColumnMaterializationKind::Time),
            Self::Utf8 { .. } => matches!(kind, ColumnMaterializationKind::Utf8),
            Self::RawBytes { .. } => matches!(kind, ColumnMaterializationKind::RawBytes),
        }
    }

    /// Clear accumulated data while preserving allocated capacity for reuse.
    pub(super) fn clear_for_reuse(&mut self) {
        match self {
            Self::I32 { values, valid } => {
                values.clear();
                *valid = None;
            }
            Self::I64 { values, valid } => {
                values.clear();
                *valid = None;
            }
            Self::F64 { values, valid } => {
                values.clear();
                *valid = None;
            }
            Self::Date { values, valid } => {
                values.clear();
                *valid = None;
            }
            Self::DateTime { values, valid } => {
                values.clear();
                *valid = None;
            }
            Self::Time { values, valid } => {
                values.clear();
                *valid = None;
            }
            Self::Utf8 {
                offsets,
                data,
                valid,
                dictionary_ids,
            } => {
                offsets.clear_for_reuse();
                data.clear();
                *valid = None;
                if let Some(ids) = dictionary_ids {
                    ids.clear();
                }
            }
            Self::RawBytes {
                offsets,
                data,
                valid,
            } => {
                offsets.clear_for_reuse();
                data.clear();
                *valid = None;
            }
            Self::StagedNumeric {
                raw_bits,
                has_missing,
                ..
            } => {
                raw_bits.clear();
                *has_missing = false;
            }
        }
    }

    pub(super) fn finish(self) -> OwnedColumnBuffer {
        match self {
            Self::I32 { values, valid } => OwnedColumnBuffer::I32 { values, valid },
            Self::I64 { values, valid } => OwnedColumnBuffer::I64 { values, valid },
            Self::F64 { values, valid } => OwnedColumnBuffer::F64 { values, valid },
            Self::StagedNumeric {
                raw_bits,
                mode,
                has_missing,
            } => materialize_staged_numeric_column(&raw_bits, mode, has_missing),
            Self::Date { values, valid } => OwnedColumnBuffer::Date { values, valid },
            Self::DateTime { values, valid } => OwnedColumnBuffer::DateTime { values, valid },
            Self::Time { values, valid } => OwnedColumnBuffer::Time { values, valid },
            Self::Utf8 {
                offsets,
                data,
                valid,
                dictionary_ids,
            } => OwnedColumnBuffer::Utf8 {
                offsets,
                data,
                valid,
                dictionary_ids,
            },
            Self::RawBytes {
                offsets,
                data,
                valid,
            } => OwnedColumnBuffer::RawBytes {
                offsets,
                data,
                valid,
            },
        }
    }
}

/// Set bit `pos` in the bit-packed validity word array (1 = valid).
#[inline]
fn set_valid_bit(bits: &mut Vec<u64>, pos: usize) {
    let word = pos / 64;
    let bit = pos % 64;
    if bits.len() <= word {
        bits.resize(word + 1, 0);
    }
    bits[word] |= 1u64 << bit;
}

/// Initialize a bit-packed validity vector for the first null at row `first_null_pos`.
/// Rows `0..first_null_pos` are set valid (1); row `first_null_pos` is null (0).
#[inline]
fn init_validity_first_null(first_null_pos: usize) -> Vec<u64> {
    let full_words = first_null_pos / 64;
    let bit_offset = first_null_pos % 64;
    let mut bits = Vec::with_capacity(full_words + 1);
    bits.extend(std::iter::repeat_n(u64::MAX, full_words));
    // Partial word: bits 0..bit_offset-1 set to 1, bit bit_offset stays 0.
    bits.push(if bit_offset == 0 {
        0u64
    } else {
        (1u64 << bit_offset) - 1
    });
    bits
}

#[inline]
pub(super) fn push_primitive_valid<T>(values: &mut Vec<T>, valid: &mut Option<Vec<u64>>, value: T) {
    let pos = values.len();
    values.push(value);
    if let Some(bits) = valid {
        set_valid_bit(bits, pos);
    }
}

#[inline]
pub(super) fn push_primitive_null<T: Copy>(
    values: &mut Vec<T>,
    valid: &mut Option<Vec<u64>>,
    default: T,
) {
    let pos = values.len();
    if valid.is_none() {
        *valid = Some(init_validity_first_null(pos));
    } else {
        let bits = valid.as_mut().expect("validity initialized");
        let word = pos / 64;
        if bits.len() <= word {
            bits.resize(word + 1, 0);
        }
        // bit at pos stays 0 (null)
    }
    values.push(default);
}

#[inline]
pub(super) fn push_variable_valid(
    offsets: &mut TrustedOffsets,
    data: &mut Vec<u8>,
    valid: &mut Option<Vec<u64>>,
    value: &[u8],
) -> Result<()> {
    let pos = offsets.len().saturating_sub(1);
    data.extend_from_slice(value);
    offsets.push_current_data_len(data.len())?;
    if let Some(bits) = valid {
        set_valid_bit(bits, pos);
    }
    Ok(())
}

/// The widest cell this can over-copy: one fixed 16-byte move.
pub(super) const OVERCOPY_WIDTH: usize = 16;

/// Append a cell by copying a fixed 16 bytes and then cutting back to `keep`.
///
/// `Vec::extend_from_slice` of a runtime-length slice is a `memcpy` call, and register cells
/// carry about five bytes of content, where the call costs more than the copy. Extending by a
/// fixed-size array instead lets the length fold into the instruction stream, and `truncate`
/// on a `Vec<u8>` is a length assignment because `u8` has no drop glue. Measured at 1.34x
/// over the variable-length form on a simulated register batch.
///
/// Reading 16 bytes runs past the cell into whatever follows it in the same row, which is
/// sound and discarded: trimming only ever removes a suffix, so the bytes worth keeping are
/// a prefix, and `keep` cuts the rest away. Rows shorter than `start + 16`, which is the last
/// column of a short row, take the ordinary path.
#[inline]
pub(super) fn push_variable_valid_overcopy(
    offsets: &mut TrustedOffsets,
    data: &mut Vec<u8>,
    valid: &mut Option<Vec<u64>>,
    row: &[u8],
    start: usize,
    keep: usize,
) -> Result<()> {
    let pos = offsets.len().saturating_sub(1);
    let before = data.len();
    match row.get(start..start + OVERCOPY_WIDTH) {
        Some(chunk) if keep <= OVERCOPY_WIDTH => {
            let fixed: [u8; OVERCOPY_WIDTH] = chunk.try_into().expect("checked width");
            data.extend_from_slice(&fixed);
            data.truncate(before + keep);
        }
        _ => data.extend_from_slice(&row[start..start + keep]),
    }
    offsets.push_current_data_len(data.len())?;
    if let Some(bits) = valid {
        set_valid_bit(bits, pos);
    }
    Ok(())
}

#[inline]
pub(super) fn push_variable_valid_without_validity(
    offsets: &mut TrustedOffsets,
    data: &mut Vec<u8>,
    value: &[u8],
) -> Result<()> {
    data.extend_from_slice(value);
    offsets.push_current_data_len(data.len())?;
    Ok(())
}

#[inline]
pub(super) fn push_variable_null(
    offsets: &mut TrustedOffsets,
    _data: &mut Vec<u8>,
    valid: &mut Option<Vec<u64>>,
) {
    let pos = offsets.len().saturating_sub(1);
    if valid.is_none() {
        *valid = Some(init_validity_first_null(pos));
    } else {
        let bits = valid.as_mut().expect("validity initialized");
        let word = pos / 64;
        if bits.len() <= word {
            bits.resize(word + 1, 0);
        }
        // bit at pos stays 0 (null)
    }
    offsets.push_repeat_last();
}

/// Target *read* footprint of one transpose tile, in bytes. The column-outer/row-inner
/// transpose re-reads the tile's page region once per column, so the region must stay
/// cache-resident for the wide-row case to win. 512 KiB sits within a typical per-core L2 yet
/// is large enough to amortise the per-column/per-tile setup over enough rows; a sweep of
/// 256 KiB–1 MiB across 16–1024 columns put 512 KiB at or near the best for every width.
/// Narrow rows collapse to a single large tile (the whole batch), matching the untiled path.
const CONTIGUOUS_TILE_BYTES: usize = 512 * 1024;

/// Strided gather of `len` full-width (8-byte) little-endian numerics into
/// `raw_bits`, with the SAS-missing test vectorized. The loop itself lives in
/// [`crate::simd`] so a runtime-dispatching backend selects its implementation once
/// for the whole run rather than once per 8 rows.
#[inline]
fn gather_staged_8byte_le(
    page: &[u8],
    base: usize,
    stride: usize,
    len: usize,
    raw_bits: &mut Vec<u64>,
) -> bool {
    gather_missing(page, base, stride, len, raw_bits)
}

/// Rows per transpose tile for a given fixed row stride, so
/// `tile_rows * row_len ≈ CONTIGUOUS_TILE_BYTES`. Always at least one row.
const fn contiguous_tile_rows(row_len: usize) -> usize {
    let row_len = if row_len == 0 { 1 } else { row_len };
    let rows = CONTIGUOUS_TILE_BYTES / row_len;
    if rows == 0 { 1 } else { rows }
}

pub(super) fn unexpected_batch_cell(expected: &str, actual: PlannedCell<'_>) -> Error {
    Error::Decode(crate::error::DecodeError {
        message: format!("columnar decode expected {expected} cell but saw {actual:?}"),
    })
}
