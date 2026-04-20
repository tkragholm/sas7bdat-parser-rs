use super::plan::ScanPlan;
use super::raw::{scan_raw_rows_with_plan, scan_row_bytes_with_plan};
use super::{
    BatchAccumulator, BatchDecodePlan, BatchHint, BatchSink, ColumnBuffer, ColumnarBatch,
    ControlFlow, Dataset, DecodeMode, Error, FileSource, OrderingMode, OwnedBatchScanBreakdown,
    OwnedColumnarBatch, OwnedRow, Parallelism, Projection, RawRow, RawRowSink, Result,
    RowSelection, RowSink, RowView, ScanProgress, ScanProgressObserver, ScanStats,
    StringDecodeOptions, TemporalDecodeOptions, materialize_planned_cells,
};
#[cfg(feature = "arrow")]
use arrow_array::RecordBatch;
#[cfg(feature = "arrow")]
use arrow_schema::SchemaRef;
use rayon::prelude::{ParallelIterator, ParallelSlice};
use std::time::Instant;

fn resolved_batch_materialize_threads(parallelism: Parallelism) -> usize {
    match parallelism {
        Parallelism::Threads(n) => n.max(1),
        Parallelism::None | Parallelism::Auto => 1,
    }
}

fn resolved_parallel_workers(parallelism: Parallelism, work_items: usize) -> usize {
    match parallelism {
        Parallelism::Threads(n) => n.max(1).min(work_items.max(1)),
        Parallelism::None | Parallelism::Auto => 1,
    }
}

pub struct ScanBuilder<'a> {
    pub(crate) ds: &'a Dataset,
    pub(crate) projection: Option<&'a Projection>,
    pub(crate) decode: DecodeMode,
    pub(crate) string_options: StringDecodeOptions,
    pub(crate) temporal_options: TemporalDecodeOptions,
    pub(crate) ordering: OrderingMode,
    pub(crate) parallelism: Parallelism,
    pub(crate) batch_hint: BatchHint,
    pub(crate) row_limit: Option<u64>,
    pub(crate) row_selection: RowSelection,
    pub(crate) progress: Option<ScanProgressObserver>,
}

impl<'a> ScanBuilder<'a> {
    pub(crate) fn new(ds: &'a Dataset) -> Self {
        Self {
            ds,
            projection: None,
            decode: DecodeMode::Typed,
            string_options: StringDecodeOptions::default(),
            temporal_options: TemporalDecodeOptions::default(),
            ordering: OrderingMode::Stable,
            parallelism: Parallelism::Auto,
            batch_hint: BatchHint::Auto,
            row_limit: None,
            row_selection: RowSelection::All,
            progress: None,
        }
    }

    #[must_use]
    pub const fn with_projection(mut self, projection: &'a Projection) -> Self {
        self.projection = Some(projection);
        self
    }

    #[must_use]
    pub const fn with_decode_mode(mut self, mode: DecodeMode) -> Self {
        self.decode = mode;
        self
    }

    #[must_use]
    pub const fn with_string_options(mut self, options: StringDecodeOptions) -> Self {
        self.string_options = options;
        self
    }

    #[must_use]
    pub const fn with_temporal_options(mut self, options: TemporalDecodeOptions) -> Self {
        self.temporal_options = options;
        self
    }

    #[must_use]
    pub const fn with_ordering(mut self, mode: OrderingMode) -> Self {
        self.ordering = mode;
        self
    }

    #[must_use]
    pub const fn with_parallelism(mut self, parallelism: Parallelism) -> Self {
        self.parallelism = parallelism;
        self
    }

    #[must_use]
    pub const fn with_batch_hint(mut self, hint: BatchHint) -> Self {
        self.batch_hint = hint;
        self
    }

    #[must_use]
    pub const fn limit(mut self, rows: u64) -> Self {
        self.row_limit = Some(rows);
        self
    }

    #[must_use]
    pub const fn select(mut self, selection: RowSelection) -> Self {
        self.row_selection = selection;
        self
    }

    /// Register a progress callback for long-running scans.
    ///
    /// The observer is called after each page is processed with a [`ScanProgress`] snapshot.
    /// Useful for reporting progress on large files. The callback must be `Send + Sync` because
    /// it may be invoked from a scan thread.
    #[must_use]
    pub fn with_progress<F>(mut self, observer: F) -> Self
    where
        F: Fn(ScanProgress) + Send + Sync + 'static,
    {
        self.progress = Some(std::sync::Arc::new(observer));
        self
    }

    /// # Errors
    ///
    /// Returns an error if the scan fails, such as due to I/O or decompression errors.
    pub fn visit_raw_rows<F>(&self, mut f: F) -> Result<crate::ScanStatsSummary>
    where
        F: FnMut(RawRow<'_>) -> Result<ControlFlow<()>>,
    {
        self.scan_raw_rows(&mut f).map(|stats| stats.summary())
    }

    /// Scans raw rows and calls `tap(row_offset, raw_page_bytes)` before each row decode.
    /// Intended for profiling and corpus analysis tools that need access to the raw page
    /// bytes alongside each row's position. Not part of the general-purpose scan API.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails.
    pub fn visit_raw_rows_with_tap<F, T>(
        &self,
        mut f: F,
        tap: &mut T,
    ) -> Result<crate::ScanStatsSummary>
    where
        F: FnMut(RawRow<'_>) -> Result<ControlFlow<()>>,
        T: FnMut(u64, &[u8]),
    {
        self.scan_raw_rows_with_tap(&mut f, tap)
            .map(|stats| stats.summary())
    }

    /// # Errors
    ///
    /// Returns an error if the scan or row decoding fails.
    pub fn visit_rows<F>(&self, mut f: F) -> Result<crate::ScanStatsSummary>
    where
        F: FnMut(RowView<'_>) -> Result<ControlFlow<()>>,
    {
        self.scan_rows(&mut f).map(|stats| stats.summary())
    }

    /// Scans decoded rows and calls `tap(row_offset, raw_page_bytes)` before each row decode.
    /// Intended for profiling and corpus analysis tools. Not part of the general-purpose scan API.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan or row decoding fails.
    pub fn visit_rows_with_tap<F, T>(
        &self,
        mut f: F,
        tap: &mut T,
    ) -> Result<crate::ScanStatsSummary>
    where
        F: FnMut(RowView<'_>) -> Result<ControlFlow<()>>,
        T: FnMut(u64, &[u8]),
    {
        self.scan_rows_with_tap(&mut f, tap)
            .map(|stats| stats.summary())
    }

    /// # Errors
    ///
    /// Returns an error if the scan or batch decoding fails.
    pub fn visit_batches<F>(&self, mut f: F) -> Result<crate::ScanStatsSummary>
    where
        F: FnMut(ColumnarBatch<'_>) -> Result<ControlFlow<()>>,
    {
        self.scan_batches(&mut |batch| {
            let columns = batch.borrowed_columns();
            let batch = ColumnarBatch {
                row_base: batch.row_base,
                row_count: batch.row_count,
                columns: &columns,
            };
            f(batch)
        })
        .map(|stats| stats.summary())
    }

    /// # Errors
    ///
    /// Returns an error if the scan or Arrow conversion fails.
    #[cfg(feature = "arrow")]
    pub fn visit_arrow_batches<F>(&self, mut f: F) -> Result<crate::ScanStatsSummary>
    where
        F: FnMut(RecordBatch) -> Result<ControlFlow<()>>,
    {
        let schema = self.arrow_schema()?;
        self.visit_batches(|batch| {
            let record_batch = batch.into_arrow_record_batch(schema.clone())?;
            f(record_batch)
        })
    }

    /// Yields each decoded batch as an [`OwnedColumnarBatch`] without any intermediate
    /// Arrow conversion, giving callers full ownership of the raw column buffers.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan or batch decoding fails.
    pub fn visit_owned_batches<F>(&self, mut f: F) -> Result<crate::ScanStatsSummary>
    where
        F: FnMut(OwnedColumnarBatch) -> Result<ControlFlow<()>>,
    {
        self.scan_batches(&mut f).map(|stats| stats.summary())
    }

    /// Scans decoded batches and calls `tap(row_offset, raw_page_bytes)` before each batch.
    /// Intended for profiling and corpus analysis tools. Not part of the general-purpose scan API.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan or batch decoding fails.
    pub fn visit_batches_with_tap<F, T>(
        &self,
        mut f: F,
        tap: &mut T,
    ) -> Result<crate::ScanStatsSummary>
    where
        F: FnMut(ColumnarBatch<'_>) -> Result<ControlFlow<()>>,
        T: FnMut(u64, &[u8]),
    {
        self.scan_batches_with_tap(
            &mut |batch| {
                let columns = batch.borrowed_columns();
                let batch = ColumnarBatch {
                    row_base: batch.row_base,
                    row_count: batch.row_count,
                    columns: &columns,
                };
                f(batch)
            },
            tap,
        )
        .map(|stats| stats.summary())
    }

    /// # Errors
    ///
    /// Returns an error if scan planning fails.
    #[cfg(feature = "arrow")]
    pub fn arrow_schema(&self) -> Result<SchemaRef> {
        let plan = ScanPlan::new(self)?;
        Ok(super::plan::arrow_schema_for_plan(&plan))
    }

    /// # Errors
    ///
    /// Returns an error if scan planning, batch materialization, or Arrow conversion fails.
    #[cfg(feature = "arrow")]
    pub fn collect_arrow_batches(&self) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        self.visit_arrow_batches(|batch| {
            batches.push(batch);
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(batches)
    }

    /// # Errors
    ///
    /// Returns an error if the scan or row decoding fails.
    pub fn collect_rows(&self) -> Result<Vec<OwnedRow>> {
        if matches!(self.decode, DecodeMode::Raw) {
            return Err(Error::unsupported(
                "collect_rows does not support DecodeMode::Raw; use visit_raw_rows instead",
            ));
        }

        let plan = ScanPlan::new(self)?;
        let mut rows = Vec::with_capacity(usize::try_from(self.ds.metadata.row_count).unwrap_or(0));
        match plan.row.decode_mode {
            DecodeMode::Typed | DecodeMode::TypedLossless => {
                scan_row_bytes_with_plan(self, &plan.raw, &mut |row_index, bytes| {
                    plan.row.validate_row_bounds(bytes)?;
                    let mut cells = Vec::with_capacity(plan.row.columns.len());
                    for (column, kind) in plan.row.columns.iter().zip(&plan.row.owned_kinds) {
                        cells.push(plan.row.materialize_owned_cell_fast(bytes, column, *kind)?);
                    }
                    rows.push(OwnedRow { row_index, cells });
                    Ok(ControlFlow::Continue(()))
                })?;
            }
            DecodeMode::Raw => {
                return Err(Error::unsupported(
                    "collect_rows does not support DecodeMode::Raw; use visit_raw_rows instead",
                ));
            }
        }
        Ok(rows)
    }

    /// # Errors
    ///
    /// Returns an error if the scan or batch decoding fails.
    pub fn collect_batches(&self) -> Result<Vec<OwnedColumnarBatch>> {
        if matches!(self.decode, DecodeMode::Raw) {
            return Err(Error::unsupported(
                "collect_batches does not support DecodeMode::Raw",
            ));
        }

        let plan = ScanPlan::new(self)?;
        if let Some(batches) = self.try_collect_batches_parallel(&plan)? {
            return Ok(batches);
        }
        let target_rows_u64 = u64::try_from(plan.batch_row_capacity)
            .unwrap_or(u64::MAX)
            .max(1);
        let estimated_batches = self.ds.metadata.row_count.div_ceil(target_rows_u64);
        let mut batches = Vec::with_capacity(usize::try_from(estimated_batches).unwrap_or(0));
        let mut batch_accumulator = BatchAccumulator::new(
            plan.batch.clone(),
            plan.batch_row_capacity,
            plan.capacity_hint_rows,
        )
        .with_materialize_threads(resolved_batch_materialize_threads(self.parallelism));

        let _stats = scan_row_bytes_with_plan(self, &plan.raw, &mut |row_index, bytes| {
            batch_accumulator.push_row(row_index.into(), bytes)?;
            if batch_accumulator.is_full() {
                batches.push(batch_accumulator.take_batch());
                batch_accumulator.reset_after_flush();
            }
            Ok(ControlFlow::Continue(()))
        })?;

        if !batch_accumulator.is_empty() {
            batches.push(batch_accumulator.take_batch());
        }

        Ok(batches)
    }

    /// # Errors
    ///
    /// Returns an error if the scan or sink fails.
    pub fn write_raw_rows(&self, sink: &mut impl RawRowSink) -> Result<crate::ScanStatsSummary> {
        self.visit_raw_rows(|row| sink.push(row))
    }

    /// # Errors
    ///
    /// Returns an error if the scan or sink fails.
    pub fn write_rows(&self, sink: &mut impl RowSink) -> Result<crate::ScanStatsSummary> {
        self.visit_rows(|row| sink.push(row))
    }

    /// # Errors
    ///
    /// Returns an error if the scan or sink fails.
    pub fn write_batches(&self, sink: &mut impl BatchSink) -> Result<crate::ScanStatsSummary> {
        self.visit_batches(|batch| sink.push(batch))
    }
}

impl ScanBuilder<'_> {
    fn try_collect_batches_parallel(
        &self,
        plan: &ScanPlan,
    ) -> Result<Option<Vec<OwnedColumnarBatch>>> {
        let descriptors = self.ds.descriptors()?;
        let page_count = descriptors.pages.len();
        let workers = resolved_parallel_workers(self.parallelism, page_count);
        if workers <= 1 || page_count <= 1 || self.row_limit.is_some() {
            return Ok(None);
        }

        let file_bytes: &[u8] = match &self.ds.file.source {
            FileSource::Bytes(bytes) => bytes.as_ref(),
            FileSource::Mmap(mmap) => &mmap[..],
            FileSource::Path(_) => return Ok(None),
        };

        let worker_capacity_hint = plan.capacity_hint_rows.div_ceil(workers).max(1);
        super::raw::RawScanPlan::validate_builder(self)?;
        if usize::from(self.ds.layout.row_len) == 0 {
            return Ok(Some(Vec::new()));
        }

        let chunk_size = page_count.div_ceil(workers).max(1);
        let context = DescriptorChunkContext {
            descriptor_table: descriptors.as_ref(),
            raw_plan: &plan.raw,
            batch_plan: &plan.batch,
            row_count: self.ds.metadata.row_count,
            target_rows: plan.batch_row_capacity,
            capacity_hint_rows: worker_capacity_hint,
        };
        let results = descriptors
            .pages
            .par_chunks(chunk_size)
            .map(|chunk| collect_batches_for_descriptor_chunk(file_bytes, chunk, &context))
            .collect::<Vec<_>>();

        let mut batches = Vec::new();
        for result in results {
            let mut chunk_batches = result?;
            batches.append(&mut chunk_batches);
        }
        if matches!(self.ordering, OrderingMode::Stable) {
            batches.sort_unstable_by_key(|batch| batch.row_base);
        }
        Ok(Some(batches))
    }
}

struct DescriptorChunkContext<'a> {
    descriptor_table: &'a crate::internal::PageDescriptorTable,
    raw_plan: &'a super::raw::RawScanPlan,
    batch_plan: &'a BatchDecodePlan,
    row_count: u64,
    target_rows: usize,
    capacity_hint_rows: usize,
}

fn collect_batches_for_descriptor_chunk(
    file_bytes: &[u8],
    descriptor_chunk: &[crate::internal::PageDescriptor],
    context: &DescriptorChunkContext<'_>,
) -> Result<Vec<OwnedColumnarBatch>> {
    let target_rows_u64 = u64::try_from(context.target_rows)
        .unwrap_or(u64::MAX)
        .max(1);
    let estimated_batches = context.row_count.div_ceil(target_rows_u64);
    let mut batches = Vec::with_capacity(usize::try_from(estimated_batches).unwrap_or(0));
    let mut batch_accumulator = BatchAccumulator::new(
        context.batch_plan.clone(),
        context.target_rows,
        context.capacity_hint_rows,
    )
    .with_materialize_threads(1);
    let mut stats = ScanStats::default();
    let mut decompressed_row = Vec::new();

    for &descriptor in descriptor_chunk {
        let page = super::raw::page_slice(file_bytes, context.raw_plan, descriptor)?;
        stats.pages_seen = stats.pages_seen.saturating_add(1);
        stats.raw_bytes_read = stats
            .raw_bytes_read
            .saturating_add(u64::try_from(page.len()).unwrap_or(u64::MAX));
        super::raw::emit_rows_from_page(
            context.raw_plan,
            context.descriptor_table,
            descriptor,
            page,
            &mut decompressed_row,
            &mut stats,
            &mut |row_index: crate::types::RowIndex, bytes| {
                batch_accumulator.push_row(row_index.into(), bytes)?;
                if batch_accumulator.is_full() {
                    batches.push(batch_accumulator.take_batch());
                    batch_accumulator.reset_after_flush();
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;
    }

    if !batch_accumulator.is_empty() {
        batches.push(batch_accumulator.take_batch());
    }

    Ok(batches)
}

const fn _keep_type_imports_alive<'a>(_columns: &'a [ColumnBuffer<'a>], _dataset: &'a Dataset) {}

impl ScanBuilder<'_> {
    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    fn scan_raw_rows<F>(&self, f: &mut F) -> Result<ScanStats>
    where
        F: FnMut(RawRow<'_>) -> Result<ControlFlow<()>>,
    {
        let plan = ScanPlan::new(self)?;
        scan_raw_rows_with_plan(self, &plan.raw, f)
    }

    fn scan_raw_rows_with_tap<F, T>(&self, f: &mut F, tap: &mut T) -> Result<ScanStats>
    where
        F: FnMut(RawRow<'_>) -> Result<ControlFlow<()>>,
        T: FnMut(u64, &[u8]),
    {
        let plan = ScanPlan::new(self)?;
        scan_raw_rows_with_plan(self, &plan.raw, &mut |raw| {
            tap(raw.row_index.into(), raw.bytes);
            f(raw)
        })
    }

    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    fn scan_rows<F>(&self, f: &mut F) -> Result<ScanStats>
    where
        F: FnMut(RowView<'_>) -> Result<ControlFlow<()>>,
    {
        if matches!(self.decode, DecodeMode::Raw) {
            return Err(Error::unsupported(
                "visit_rows does not support DecodeMode::Raw; use visit_raw_rows instead",
            ));
        }

        let plan = ScanPlan::new(self)?;
        let mut owned_strings = Vec::new();
        scan_raw_rows_with_plan(self, &plan.raw, &mut |raw| {
            let planned = plan.row.plan_cells(raw.bytes, &mut owned_strings)?;
            let cells = materialize_planned_cells(&planned, &owned_strings)?;
            let row = RowView {
                row_index: raw.row_index,
                names: plan.row.names.as_ref(),
                cells: &cells,
            };
            f(row)
        })
    }

    fn scan_rows_with_tap<F, T>(&self, f: &mut F, tap: &mut T) -> Result<ScanStats>
    where
        F: FnMut(RowView<'_>) -> Result<ControlFlow<()>>,
        T: FnMut(u64, &[u8]),
    {
        if matches!(self.decode, DecodeMode::Raw) {
            return Err(Error::unsupported(
                "visit_rows does not support DecodeMode::Raw; use visit_raw_rows instead",
            ));
        }

        let plan = ScanPlan::new(self)?;
        let mut owned_strings = Vec::new();
        scan_raw_rows_with_plan(self, &plan.raw, &mut |raw| {
            tap(raw.row_index.into(), raw.bytes);
            let planned = plan.row.plan_cells(raw.bytes, &mut owned_strings)?;
            let cells = materialize_planned_cells(&planned, &owned_strings)?;
            let row = RowView {
                row_index: raw.row_index,
                names: plan.row.names.as_ref(),
                cells: &cells,
            };
            f(row)
        })
    }

    fn scan_batches<F>(&self, f: &mut F) -> Result<ScanStats>
    where
        F: FnMut(OwnedColumnarBatch) -> Result<ControlFlow<()>>,
    {
        let plan = ScanPlan::new(self)?;
        if let Some(batches) = self.try_collect_batches_parallel(&plan)? {
            let mut stats = ScanStats {
                decode_batches: u64::try_from(batches.len()).unwrap_or(u64::MAX),
                ..ScanStats::default()
            };
            for batch in batches {
                stats.rows_emitted = stats
                    .rows_emitted
                    .saturating_add(u64::try_from(batch.row_count).unwrap_or(u64::MAX));
                match f(batch)? {
                    ControlFlow::Continue(()) => {}
                    ControlFlow::Break(()) => break,
                }
            }
            return Ok(stats);
        }
        self.scan_batches_with_tap(f, &mut |_, _| {})
    }

    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    fn scan_batches_with_tap<F, T>(&self, f: &mut F, tap: &mut T) -> Result<ScanStats>
    where
        F: FnMut(OwnedColumnarBatch) -> Result<ControlFlow<()>>,
        T: FnMut(u64, &[u8]),
    {
        if matches!(self.decode, DecodeMode::Raw) {
            return Err(Error::unsupported(
                "visit_batches does not support DecodeMode::Raw",
            ));
        }

        let plan = ScanPlan::new(self)?;
        let mut batcher = BatchAccumulator::new(
            plan.batch.clone(),
            plan.batch_row_capacity,
            plan.capacity_hint_rows,
        )
        .with_materialize_threads(resolved_batch_materialize_threads(self.parallelism));
        let mut decode_batches = 0u64;
        let mut stop_after_current_batch = false;

        let mut stats = scan_row_bytes_with_plan(self, &plan.raw, &mut |row_index, bytes| {
            tap(row_index.into(), bytes);
            batcher.push_row(row_index.into(), bytes)?;
            if batcher.is_full() {
                let batch = batcher.take_batch();
                match f(batch)? {
                    ControlFlow::Continue(()) => {
                        decode_batches = decode_batches.saturating_add(1);
                        batcher.reset_after_flush();
                    }
                    ControlFlow::Break(()) => {
                        decode_batches = decode_batches.saturating_add(1);
                        stop_after_current_batch = true;
                        return Ok(ControlFlow::Break(()));
                    }
                }
            }
            Ok(ControlFlow::Continue(()))
        })?;

        if !stop_after_current_batch && !batcher.is_empty() {
            let batch = batcher.take_batch();
            decode_batches = decode_batches.saturating_add(1);
            let _ = f(batch)?;
        }

        let counters = batcher.counters();
        stats.decode_batches = decode_batches;
        stats.batch_staged_numeric_cells = counters.staged_numeric;
        stats.batch_direct_numeric_cells = counters.direct_numeric;
        stats.batch_direct_raw_bytes_cells = counters.direct_raw_bytes;
        stats.batch_direct_utf8_single_byte_cells = counters.direct_utf8_single_byte;
        stats.batch_direct_utf8_borrowed_cells = counters.direct_utf8_borrowed;
        stats.batch_direct_utf8_owned_cells = counters.direct_utf8_owned;
        stats.batch_direct_utf8_owned_interned_hits = counters.direct_utf8_owned_interned_hits;
        stats.batch_direct_utf8_owned_seen_once_promotions =
            counters.direct_utf8_owned_seen_once_promotions;
        stats.batch_fallback_cells = counters.fallback;
        Ok(stats)
    }

    #[doc(hidden)]
    /// # Errors
    ///
    /// Returns an error if the owned-batch scan fails.
    pub fn owned_batch_scan_breakdown(&self) -> Result<OwnedBatchScanBreakdown> {
        if matches!(self.decode, DecodeMode::Raw) {
            return Err(Error::unsupported(
                "visit_batches does not support DecodeMode::Raw",
            ));
        }

        let total_start = Instant::now();
        let plan_start = Instant::now();
        let plan = ScanPlan::new(self)?;
        let plan_ns = plan_start.elapsed().as_nanos();

        let mut batcher = BatchAccumulator::new(
            plan.batch.clone(),
            plan.batch_row_capacity,
            plan.capacity_hint_rows,
        )
        .with_materialize_threads(resolved_batch_materialize_threads(self.parallelism));
        let mut decode_batches = 0u64;
        let mut push_row_ns = 0u128;
        let mut take_batch_ns = 0u128;
        let mut reset_after_flush_ns = 0u128;

        let scan_start = Instant::now();
        let mut stats = scan_row_bytes_with_plan(self, &plan.raw, &mut |row_index, bytes| {
            let push_start = Instant::now();
            batcher.push_row(row_index.into(), bytes)?;
            push_row_ns += push_start.elapsed().as_nanos();
            if batcher.is_full() {
                let take_start = Instant::now();
                let _batch = batcher.take_batch();
                take_batch_ns += take_start.elapsed().as_nanos();
                decode_batches = decode_batches.saturating_add(1);

                let reset_start = Instant::now();
                batcher.reset_after_flush();
                reset_after_flush_ns += reset_start.elapsed().as_nanos();
            }
            Ok(ControlFlow::Continue(()))
        })?;
        let scan_row_bytes_ns = scan_start.elapsed().as_nanos();

        if !batcher.is_empty() {
            let take_start = Instant::now();
            let _batch = batcher.take_batch();
            take_batch_ns += take_start.elapsed().as_nanos();
            decode_batches = decode_batches.saturating_add(1);
        }

        let counters = batcher.counters();
        stats.decode_batches = decode_batches;
        stats.batch_staged_numeric_cells = counters.staged_numeric;
        stats.batch_direct_numeric_cells = counters.direct_numeric;
        stats.batch_direct_raw_bytes_cells = counters.direct_raw_bytes;
        stats.batch_direct_utf8_single_byte_cells = counters.direct_utf8_single_byte;
        stats.batch_direct_utf8_borrowed_cells = counters.direct_utf8_borrowed;
        stats.batch_direct_utf8_owned_cells = counters.direct_utf8_owned;
        stats.batch_direct_utf8_owned_interned_hits = counters.direct_utf8_owned_interned_hits;
        stats.batch_direct_utf8_owned_seen_once_promotions =
            counters.direct_utf8_owned_seen_once_promotions;
        stats.batch_fallback_cells = counters.fallback;

        Ok(OwnedBatchScanBreakdown {
            total_ns: total_start.elapsed().as_nanos(),
            plan_ns,
            scan_row_bytes_ns,
            push_row_ns,
            take_batch_ns,
            reset_after_flush_ns,
            batches_emitted: decode_batches,
            stats: stats.summary(),
        })
    }
}
