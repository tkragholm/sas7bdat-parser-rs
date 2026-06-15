use super::plan::ScanPlan;
use super::raw::{scan_raw_rows_with_plan, scan_row_bytes_with_plan};
use super::{
    BatchAccumulator, BatchDecodePlan, BatchHint, BatchSink, ColumnBuffer, ColumnMajorDecode,
    ColumnarBatch, ControlFlow, Dataset, DecodeMode, Error, FileSource, OrderingMode,
    OwnedBatchScanBreakdown, OwnedColumnarBatch, OwnedRow, Parallelism, Projection, RawRow,
    RawRowSink, Result, RowSelection, RowSink, RowView, ScanProgress, ScanProgressObserver,
    ScanStats, StringDecodeOptions, TemporalDecodeOptions, materialize_planned_cells,
};
#[cfg(feature = "arrow")]
use arrow_array::RecordBatch;
#[cfg(feature = "arrow")]
use arrow_schema::SchemaRef;
use rayon::prelude::{ParallelIterator, ParallelSlice};
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{SyncSender, sync_channel},
    },
    time::Instant,
};

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

/// Chunks cut per worker. Cutting several chunks per worker (instead of exactly one) gives the
/// scheduler slack to steal work and balance uneven chunks — compressed pages, partial last
/// pages, and varying page fill all make a static `pages / workers` split tail-bound on the
/// slowest chunk.
const CHUNK_OVERSUBSCRIBE: usize = 4;

/// Pages per parallel chunk. Targets ~[`CHUNK_OVERSUBSCRIBE`] chunks per worker for stealing
/// slack, but never smaller than the pages needed to fill one target batch — so finer chunking
/// doesn't shred the file into a swarm of undersized tail batches or per-chunk accumulator churn.
fn balanced_chunk_size(
    page_count: usize,
    workers: usize,
    rows_per_page: u64,
    target_rows: usize,
) -> usize {
    let target_chunks = workers.saturating_mul(CHUNK_OVERSUBSCRIBE).max(1);
    let by_oversubscribe = page_count.div_ceil(target_chunks).max(1);
    let pages_per_batch = usize::try_from(
        u64::try_from(target_rows)
            .unwrap_or(u64::MAX)
            .div_ceil(rows_per_page.max(1)),
    )
    .unwrap_or(1)
    .max(1);
    by_oversubscribe.max(pages_per_batch)
}

/// Delivers buffered batches to `f` in strict chunk order, starting at
/// `*next_chunk` and advancing past any chunk that is both finished and
/// drained. Returns `Break` if `f` requested a stop.
///
/// This must run after a new batch is buffered *and* after a chunk finishes:
/// a `Finished` message can make `*next_chunk` eligible to advance onto a
/// chunk whose batches already arrived out of order, and those buffered
/// batches must be flushed here rather than waiting for a later batch to
/// drive delivery (the last chunk may have no later batch, stranding it).
fn drain_ordered_batches<F>(
    buffered: &mut [VecDeque<OwnedColumnarBatch>],
    finished: &[bool],
    next_chunk: &mut usize,
    chunk_count: usize,
    delivered_batches: &mut u64,
    delivered_rows: &mut u64,
    f: &mut F,
) -> Result<ControlFlow<()>>
where
    F: FnMut(OwnedColumnarBatch) -> Result<ControlFlow<()>>,
{
    while *next_chunk < chunk_count {
        if let Some(batch) = buffered[*next_chunk].pop_front() {
            *delivered_batches = delivered_batches.saturating_add(1);
            *delivered_rows =
                delivered_rows.saturating_add(u64::try_from(batch.row_count).unwrap_or(u64::MAX));
            if f(batch)?.is_break() {
                return Ok(ControlFlow::Break(()));
            }
        } else if finished[*next_chunk] {
            *next_chunk += 1;
        } else {
            break;
        }
    }
    Ok(ControlFlow::Continue(()))
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
    pub(crate) column_major: ColumnMajorDecode,
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
            // On by default: the column-major fill is byte-identical to row-major (extensive
            // parity tests + readstat cross-checks) and several× faster for wide numeric tables,
            // and it falls back to row-major automatically whenever its preconditions don't hold.
            column_major: ColumnMajorDecode::On,
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

    /// Select the owned-batch decode strategy. See [`ColumnMajorDecode`]. Defaults to
    /// [`ColumnMajorDecode::Off`] (row-major). Only [`Self::collect_batches`] honors this;
    /// it falls back to row-major automatically when the column-major preconditions don't hold.
    #[must_use]
    pub const fn with_column_major_decode(mut self, mode: ColumnMajorDecode) -> Self {
        self.column_major = mode;
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
        self.scan_batches_borrowed_with_tap(&mut f, &mut |_, _| {})
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
        self.scan_batches_borrowed_with_tap(&mut f, tap)
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
        self.collect_batches_inner(matches!(self.column_major, ColumnMajorDecode::On))
    }

    /// Force the column-major decode path on (ignoring the builder's [`ColumnMajorDecode`]
    /// setting) when its preconditions hold, else fall back to row-major exactly like
    /// [`Self::collect_batches`]. Exposed (hidden) for benchmarking the two paths head-to-head;
    /// not part of the stable API.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan or batch decoding fails.
    #[doc(hidden)]
    pub fn collect_batches_columnar(&self) -> Result<Vec<OwnedColumnarBatch>> {
        self.collect_batches_inner(true)
    }

    fn collect_batches_inner(&self, column_major: bool) -> Result<Vec<OwnedColumnarBatch>> {
        if matches!(self.decode, DecodeMode::Raw) {
            return Err(Error::unsupported(
                "collect_batches does not support DecodeMode::Raw",
            ));
        }

        let plan = ScanPlan::new(self)?;
        if let Some(batches) = self.try_collect_batches_parallel(&plan, column_major)? {
            return Ok(batches);
        }

        let target_rows_u64 = u64::try_from(plan.batch_row_capacity)
            .unwrap_or(u64::MAX)
            .max(1);
        let estimated_batches = self.ds.metadata.row_count.div_ceil(target_rows_u64);
        let mut batches = Vec::with_capacity(usize::try_from(estimated_batches).unwrap_or(0));
        let mut acc = BatchAccumulator::new(
            plan.batch.clone(),
            plan.batch_row_capacity,
            plan.capacity_hint_rows,
        )
        .with_materialize_threads(resolved_batch_materialize_threads(self.parallelism));

        // The column-major fast path needs an in-memory source, a whole-file scan, and an
        // all-staged-numeric plan; otherwise (and for any non-fused page) decode stays
        // row-major and produces identical output.
        let columnar_file_bytes = column_major_file_bytes(self, column_major)
            .filter(|_| acc.plan_is_all_columns_staged_numeric());

        if let Some(file_bytes) = columnar_file_bytes {
            super::raw::RawScanPlan::validate_builder(self)?;
            let row_len = usize::from(self.ds.layout.row_len);
            if row_len == 0 {
                return Ok(Vec::new());
            }
            let descriptors = self.ds.descriptors()?;
            let ctx = ColumnarDecodeCtx {
                descriptors: descriptors.as_ref(),
                raw_plan: &plan.raw,
                file_bytes,
                row_len,
                columnar: true,
            };
            decode_descriptors_into_batches(&mut acc, &ctx, &descriptors.pages, &mut batches)?;
        } else {
            let _stats = scan_row_bytes_with_plan(self, &plan.raw, &mut |row_index, bytes| {
                acc.push_row(row_index.into(), bytes)?;
                if acc.is_full() {
                    batches.push(acc.take_batch()?);
                    acc.reset_after_flush();
                }
                Ok(ControlFlow::Continue(()))
            })?;
        }

        if !acc.is_empty() {
            batches.push(acc.take_batch()?);
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
        column_major: bool,
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
        let row_len = usize::from(self.ds.layout.row_len);
        if row_len == 0 {
            return Ok(Some(Vec::new()));
        }

        // The column-major fill ignores row selection, so it only applies to whole-file scans.
        let columnar = column_major && matches!(self.row_selection, RowSelection::All);
        // Oversubscribe chunks so rayon's global pool can steal and balance uneven chunks.
        let chunk_size = balanced_chunk_size(
            page_count,
            workers,
            self.ds.layout.rows_per_page,
            plan.batch_row_capacity,
        );
        let context = DescriptorChunkContext {
            descriptor_table: descriptors.as_ref(),
            raw_plan: &plan.raw,
            batch_plan: &plan.batch,
            row_count: self.ds.metadata.row_count,
            target_rows: plan.batch_row_capacity,
            capacity_hint_rows: worker_capacity_hint,
            row_len,
            columnar,
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

/// In-memory file bytes for the column-major path, or `None` if it must not engage: the path
/// only handles whole-file (`RowSelection::All`, no limit) scans of an in-memory source. The
/// caller additionally requires an all-staged-numeric plan.
fn column_major_file_bytes<'a>(
    builder: &'a ScanBuilder<'_>,
    column_major: bool,
) -> Option<&'a [u8]> {
    if !column_major
        || !matches!(builder.row_selection, RowSelection::All)
        || builder.row_limit.is_some()
    {
        return None;
    }
    match &builder.ds.file.source {
        FileSource::Bytes(bytes) => Some(bytes.as_ref()),
        FileSource::Mmap(mmap) => Some(&mmap[..]),
        FileSource::Path(_) => None,
    }
}

/// Inputs shared across a run of page-descriptor decodes (the descriptor table for span
/// lookups, the raw plan, the in-memory file bytes, the fixed row stride, and whether the
/// column-major fill is permitted).
struct ColumnarDecodeCtx<'a> {
    descriptors: &'a crate::internal::PageDescriptorTable,
    raw_plan: &'a super::raw::RawScanPlan,
    file_bytes: &'a [u8],
    row_len: usize,
    columnar: bool,
}

/// Decode a run of page descriptors, invoking `consume` on each *full* batch (the caller is
/// responsible for flushing the final partial batch left in `acc`). When `ctx.columnar` is set,
/// `FusedContiguousUncompressed` pages take the column-major tiled fill
/// ([`BatchAccumulator::push_contiguous_span_all_staged_numeric`]); every other page (and the
/// whole run when `columnar` is false) uses the row-major push. Returns `Break` if `consume`
/// asked to stop. Shared by the serial/parallel `collect_batches` and the serial/parallel
/// owned-batch streaming paths. `stats` accrues page/row counters; callers that don't need them
/// pass a throwaway.
fn stream_descriptors_into_batches<C>(
    acc: &mut BatchAccumulator,
    ctx: &ColumnarDecodeCtx<'_>,
    page_descriptors: &[crate::internal::PageDescriptor],
    stats: &mut ScanStats,
    consume: &mut C,
) -> Result<ControlFlow<()>>
where
    C: FnMut(OwnedColumnarBatch) -> Result<ControlFlow<()>>,
{
    let target_rows = acc.target_rows();
    let mut decompressed_row = Vec::new();

    for &descriptor in page_descriptors {
        let page = super::raw::page_slice(ctx.file_bytes, ctx.raw_plan, descriptor)?;
        stats.pages_seen = stats.pages_seen.saturating_add(1);
        stats.raw_bytes_read = stats
            .raw_bytes_read
            .saturating_add(u64::try_from(page.len()).unwrap_or(u64::MAX));

        if ctx.columnar
            && matches!(
                descriptor.exec_class,
                crate::internal::PageExecClass::FusedContiguousUncompressed
            )
        {
            let data_start = usize::from(descriptor.data_start);
            let row_count = usize::try_from(descriptor.row_count).unwrap_or(usize::MAX);
            let page_row_base: u64 = descriptor.row_base.into();
            let row_count_u64 = u64::try_from(row_count).unwrap_or(u64::MAX);
            stats.fused_pages = stats.fused_pages.saturating_add(1);
            // Whole-file scan (the column-major precondition), so every row is seen and emitted.
            stats.rows_seen = stats.rows_seen.saturating_add(row_count_u64);
            stats.rows_emitted = stats.rows_emitted.saturating_add(row_count_u64);
            let mut row_off = 0usize;
            while row_off < row_count {
                if acc.is_full() {
                    let batch = acc.take_batch()?;
                    if consume(batch)?.is_break() {
                        return Ok(ControlFlow::Break(()));
                    }
                    acc.reset_after_flush();
                }
                let span = (target_rows - acc.row_count()).min(row_count - row_off);
                acc.push_contiguous_span_all_staged_numeric(
                    page_row_base,
                    page,
                    row_off,
                    span,
                    data_start,
                    ctx.row_len,
                )?;
                row_off += span;
            }
        } else {
            let stopped = super::raw::emit_rows_from_page(
                ctx.raw_plan,
                ctx.descriptors,
                descriptor,
                page,
                &mut decompressed_row,
                stats,
                &mut |row_index: crate::types::RowIndex, bytes| {
                    acc.push_row(row_index.into(), bytes)?;
                    if acc.is_full() {
                        let batch = acc.take_batch()?;
                        if consume(batch)?.is_break() {
                            return Ok(ControlFlow::Break(()));
                        }
                        acc.reset_after_flush();
                    }
                    Ok(ControlFlow::Continue(()))
                },
            )?;
            if stopped {
                return Ok(ControlFlow::Break(()));
            }
        }
    }
    Ok(ControlFlow::Continue(()))
}

/// Collect a run of page descriptors into owned batches (full batches appended to `batches`;
/// the caller flushes the final partial). Thin wrapper over [`stream_descriptors_into_batches`]
/// whose `consume` never stops.
fn decode_descriptors_into_batches(
    acc: &mut BatchAccumulator,
    ctx: &ColumnarDecodeCtx<'_>,
    page_descriptors: &[crate::internal::PageDescriptor],
    batches: &mut Vec<OwnedColumnarBatch>,
) -> Result<()> {
    let mut stats = ScanStats::default();
    let flow = stream_descriptors_into_batches(acc, ctx, page_descriptors, &mut stats, &mut |b| {
        batches.push(b);
        Ok(ControlFlow::Continue(()))
    })?;
    debug_assert!(flow.is_continue(), "collect consume never breaks");
    Ok(())
}

/// Copy the accumulator's per-family cell counters into `stats`.
fn store_batch_counters(stats: &mut ScanStats, counters: super::batch::BatchFamilyCounters) {
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
}

struct DescriptorChunkContext<'a> {
    descriptor_table: &'a crate::internal::PageDescriptorTable,
    raw_plan: &'a super::raw::RawScanPlan,
    batch_plan: &'a BatchDecodePlan,
    row_count: u64,
    target_rows: usize,
    capacity_hint_rows: usize,
    row_len: usize,
    columnar: bool,
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
    let mut acc = BatchAccumulator::new(
        context.batch_plan.clone(),
        context.target_rows,
        context.capacity_hint_rows,
    )
    .with_materialize_threads(1);

    let ctx = ColumnarDecodeCtx {
        descriptors: context.descriptor_table,
        raw_plan: context.raw_plan,
        file_bytes,
        row_len: context.row_len,
        columnar: context.columnar && acc.plan_is_all_columns_staged_numeric(),
    };
    decode_descriptors_into_batches(&mut acc, &ctx, descriptor_chunk, &mut batches)?;

    if !acc.is_empty() {
        batches.push(acc.take_batch()?);
    }

    Ok(batches)
}

enum StreamedBatchMessage {
    Batch {
        chunk_idx: usize,
        batch: OwnedColumnarBatch,
    },
    Finished {
        chunk_idx: usize,
    },
    Error(Error),
}

fn stream_batches_for_descriptor_chunk(
    file_bytes: &[u8],
    descriptor_chunk: &[crate::internal::PageDescriptor],
    chunk_idx: usize,
    context: &DescriptorChunkContext<'_>,
    tx: &SyncSender<StreamedBatchMessage>,
    stop: &AtomicBool,
) -> ScanStats {
    let mut acc = BatchAccumulator::new(
        context.batch_plan.clone(),
        context.target_rows,
        context.capacity_hint_rows,
    )
    .with_materialize_threads(1);
    let mut stats = ScanStats::default();
    let mut decode_batches = 0u64;

    let ctx = ColumnarDecodeCtx {
        descriptors: context.descriptor_table,
        raw_plan: context.raw_plan,
        file_bytes,
        row_len: context.row_len,
        columnar: context.columnar && acc.plan_is_all_columns_staged_numeric(),
    };

    // The shared routine flushes full batches through `consume`, which streams each to the
    // collector and stops the worker when the channel closes or the consumer requested a stop.
    let result = stream_descriptors_into_batches(
        &mut acc,
        &ctx,
        descriptor_chunk,
        &mut stats,
        &mut |batch| {
            decode_batches = decode_batches.saturating_add(1);
            if tx
                .send(StreamedBatchMessage::Batch { chunk_idx, batch })
                .is_err()
                || stop.load(Ordering::Relaxed)
            {
                return Ok(ControlFlow::Break(()));
            }
            Ok(ControlFlow::Continue(()))
        },
    );

    match result {
        Ok(flow) => {
            if flow.is_continue() && !stop.load(Ordering::Relaxed) && !acc.is_empty() {
                match acc.take_batch() {
                    Ok(batch) => {
                        decode_batches = decode_batches.saturating_add(1);
                        let _ = tx.send(StreamedBatchMessage::Batch { chunk_idx, batch });
                    }
                    Err(e) => {
                        let _ = tx.send(StreamedBatchMessage::Error(e));
                        return stats;
                    }
                }
            }
        }
        Err(e) => {
            let _ = tx.send(StreamedBatchMessage::Error(e));
            return stats;
        }
    }

    stats.decode_batches = decode_batches;
    store_batch_counters(&mut stats, acc.counters());
    let _ = tx.send(StreamedBatchMessage::Finished { chunk_idx });
    stats
}

const fn merge_scan_stats(into: &mut ScanStats, from: &ScanStats) {
    into.rows_seen = into.rows_seen.saturating_add(from.rows_seen);
    into.rows_emitted = into.rows_emitted.saturating_add(from.rows_emitted);
    into.pages_seen = into.pages_seen.saturating_add(from.pages_seen);
    into.fused_pages = into.fused_pages.saturating_add(from.fused_pages);
    into.indexed_pages = into.indexed_pages.saturating_add(from.indexed_pages);
    into.compressed_pages = into.compressed_pages.saturating_add(from.compressed_pages);
    into.raw_bytes_read = into.raw_bytes_read.saturating_add(from.raw_bytes_read);
    into.row_bytes_materialized = into
        .row_bytes_materialized
        .saturating_add(from.row_bytes_materialized);
    into.decode_batches = into.decode_batches.saturating_add(from.decode_batches);
    into.batch_staged_numeric_cells = into
        .batch_staged_numeric_cells
        .saturating_add(from.batch_staged_numeric_cells);
    into.batch_direct_numeric_cells = into
        .batch_direct_numeric_cells
        .saturating_add(from.batch_direct_numeric_cells);
    into.batch_direct_raw_bytes_cells = into
        .batch_direct_raw_bytes_cells
        .saturating_add(from.batch_direct_raw_bytes_cells);
    into.batch_direct_utf8_single_byte_cells = into
        .batch_direct_utf8_single_byte_cells
        .saturating_add(from.batch_direct_utf8_single_byte_cells);
    into.batch_direct_utf8_borrowed_cells = into
        .batch_direct_utf8_borrowed_cells
        .saturating_add(from.batch_direct_utf8_borrowed_cells);
    into.batch_direct_utf8_owned_cells = into
        .batch_direct_utf8_owned_cells
        .saturating_add(from.batch_direct_utf8_owned_cells);
    into.batch_direct_utf8_owned_interned_hits = into
        .batch_direct_utf8_owned_interned_hits
        .saturating_add(from.batch_direct_utf8_owned_interned_hits);
    into.batch_direct_utf8_owned_seen_once_promotions = into
        .batch_direct_utf8_owned_seen_once_promotions
        .saturating_add(from.batch_direct_utf8_owned_seen_once_promotions);
    into.batch_fallback_cells = into
        .batch_fallback_cells
        .saturating_add(from.batch_fallback_cells);
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
        let column_major = matches!(self.column_major, ColumnMajorDecode::On);
        if let Some(stats) = self.try_stream_batches_parallel(&plan, column_major, f)? {
            return Ok(stats);
        }
        if column_major
            && plan.batch.all_columns_staged_numeric()
            && let Some(file_bytes) = column_major_file_bytes(self, true)
        {
            return self.stream_batches_columnar_serial(&plan, file_bytes, f);
        }
        self.scan_batches_with_tap(f, &mut |_, _| {})
    }

    /// Serial owned-batch streaming via the column-major fill. Preconditions (in-memory source,
    /// whole-file scan, all-staged-numeric plan) are checked by the caller; non-fused pages
    /// still stream row-major inside [`stream_descriptors_into_batches`].
    fn stream_batches_columnar_serial<F>(
        &self,
        plan: &ScanPlan,
        file_bytes: &[u8],
        f: &mut F,
    ) -> Result<ScanStats>
    where
        F: FnMut(OwnedColumnarBatch) -> Result<ControlFlow<()>>,
    {
        super::raw::RawScanPlan::validate_builder(self)?;
        let row_len = usize::from(self.ds.layout.row_len);
        if row_len == 0 {
            return Ok(ScanStats::default());
        }
        let descriptors = self.ds.descriptors()?;
        let mut acc = BatchAccumulator::new(
            plan.batch.clone(),
            plan.batch_row_capacity,
            plan.capacity_hint_rows,
        )
        .with_materialize_threads(resolved_batch_materialize_threads(self.parallelism));
        let ctx = ColumnarDecodeCtx {
            descriptors: descriptors.as_ref(),
            raw_plan: &plan.raw,
            file_bytes,
            row_len,
            columnar: true,
        };

        let mut stats = ScanStats::default();
        let mut decode_batches = 0u64;
        let flow = stream_descriptors_into_batches(
            &mut acc,
            &ctx,
            &descriptors.pages,
            &mut stats,
            &mut |batch| {
                decode_batches = decode_batches.saturating_add(1);
                f(batch)
            },
        )?;
        if flow.is_continue() && !acc.is_empty() {
            let batch = acc.take_batch()?;
            decode_batches = decode_batches.saturating_add(1);
            let _ = f(batch)?;
        }

        stats.decode_batches = decode_batches;
        store_batch_counters(&mut stats, acc.counters());
        Ok(stats)
    }

    #[allow(clippy::too_many_lines)]
    fn try_stream_batches_parallel<F>(
        &self,
        plan: &ScanPlan,
        column_major: bool,
        f: &mut F,
    ) -> Result<Option<ScanStats>>
    where
        F: FnMut(OwnedColumnarBatch) -> Result<ControlFlow<()>>,
    {
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
            return Ok(Some(ScanStats::default()));
        }

        let chunk_size = balanced_chunk_size(
            page_count,
            workers,
            self.ds.layout.rows_per_page,
            plan.batch_row_capacity,
        );
        let chunks: Vec<&[crate::internal::PageDescriptor]> =
            descriptors.pages.chunks(chunk_size).collect();
        let chunk_count = chunks.len();
        // Declared outside `thread::scope` so the shared reference satisfies the `'scope` bound.
        let next_chunk_idx = AtomicUsize::new(0);
        let context = DescriptorChunkContext {
            descriptor_table: descriptors.as_ref(),
            raw_plan: &plan.raw,
            batch_plan: &plan.batch,
            row_count: self.ds.metadata.row_count,
            target_rows: plan.batch_row_capacity,
            capacity_hint_rows: worker_capacity_hint,
            row_len: usize::from(self.ds.layout.row_len),
            // The column-major fill ignores row selection, so it only applies to whole-file
            // scans; each worker additionally requires an all-staged-numeric plan.
            columnar: column_major && matches!(self.row_selection, RowSelection::All),
        };

        let total_stats = std::thread::scope(|scope| -> Result<ScanStats> {
            let channel_bound = workers.saturating_mul(2).max(1);
            let (tx, rx) = sync_channel(channel_bound);
            let stop = Arc::new(AtomicBool::new(false));
            let next_chunk_idx = &next_chunk_idx;
            let chunks = &chunks;
            let mut handles = Vec::with_capacity(workers);

            // A fixed pool of `workers` threads pulls chunk indices from a shared counter, so
            // `chunk_count` can exceed `workers` (giving stealing slack to balance uneven
            // chunks) without spawning more threads than the resolved core budget. Batches stay
            // tagged with their chunk index, so the consumer's ordered delivery is unaffected.
            for _ in 0..workers {
                let tx = tx.clone();
                let stop = stop.clone();
                let context = &context;
                handles.push(scope.spawn(move || {
                    let mut worker_stats = ScanStats::default();
                    loop {
                        let idx = next_chunk_idx.fetch_add(1, Ordering::Relaxed);
                        if idx >= chunk_count {
                            break;
                        }
                        let chunk_stats = stream_batches_for_descriptor_chunk(
                            file_bytes,
                            chunks[idx],
                            idx,
                            context,
                            &tx,
                            stop.as_ref(),
                        );
                        merge_scan_stats(&mut worker_stats, &chunk_stats);
                    }
                    worker_stats
                }));
            }
            drop(tx);

            let mut buffered = (0..chunk_count)
                .map(|_| VecDeque::<OwnedColumnarBatch>::new())
                .collect::<Vec<_>>();
            let mut finished = vec![false; chunk_count];
            let mut next_chunk = 0usize;
            let mut delivered_batches = 0u64;
            let mut delivered_rows = 0u64;

            while let Ok(message) = rx.recv() {
                match message {
                    StreamedBatchMessage::Batch { chunk_idx, batch } => {
                        if matches!(self.ordering, OrderingMode::Unordered) {
                            delivered_batches = delivered_batches.saturating_add(1);
                            delivered_rows = delivered_rows
                                .saturating_add(u64::try_from(batch.row_count).unwrap_or(u64::MAX));
                            if f(batch)?.is_break() {
                                stop.store(true, Ordering::Relaxed);
                                break;
                            }
                        } else {
                            buffered[chunk_idx].push_back(batch);
                            if drain_ordered_batches(
                                &mut buffered,
                                &finished,
                                &mut next_chunk,
                                chunk_count,
                                &mut delivered_batches,
                                &mut delivered_rows,
                                f,
                            )?
                            .is_break()
                            {
                                stop.store(true, Ordering::Relaxed);
                                break;
                            }
                        }
                    }
                    StreamedBatchMessage::Finished { chunk_idx } => {
                        finished[chunk_idx] = true;
                        if matches!(self.ordering, OrderingMode::Stable)
                            && drain_ordered_batches(
                                &mut buffered,
                                &finished,
                                &mut next_chunk,
                                chunk_count,
                                &mut delivered_batches,
                                &mut delivered_rows,
                                f,
                            )?
                            .is_break()
                        {
                            stop.store(true, Ordering::Relaxed);
                            break;
                        }
                    }
                    StreamedBatchMessage::Error(err) => {
                        stop.store(true, Ordering::Relaxed);
                        drop(rx);
                        for handle in handles {
                            let _ = handle.join();
                        }
                        return Err(err);
                    }
                }
            }

            drop(rx);
            let mut total = ScanStats::default();
            for handle in handles {
                let worker_stats = handle
                    .join()
                    .map_err(|_| Error::unsupported("parallel batch worker panicked"))?;
                merge_scan_stats(&mut total, &worker_stats);
            }
            total.decode_batches = delivered_batches;
            total.rows_emitted = delivered_rows;
            Ok(total)
        })?;
        Ok(Some(total_stats))
    }

    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    fn scan_batches_borrowed_with_tap<F, T>(&self, f: &mut F, tap: &mut T) -> Result<ScanStats>
    where
        F: FnMut(ColumnarBatch<'_>) -> Result<ControlFlow<()>>,
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
        let mut staged_scratch: Vec<crate::OwnedColumnBuffer> =
            Vec::with_capacity(batcher.staged_numeric_count());
        let mut decode_batches = 0u64;
        let mut stop_after_current_batch = false;

        let mut stats = scan_row_bytes_with_plan(self, &plan.raw, &mut |row_index, bytes| {
            tap(row_index.into(), bytes);
            batcher.push_row(row_index.into(), bytes)?;
            if batcher.is_full() {
                match batcher.flush_borrowed_and_reset(&mut staged_scratch, f)? {
                    ControlFlow::Continue(()) => {
                        decode_batches = decode_batches.saturating_add(1);
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
            decode_batches = decode_batches.saturating_add(1);
            let _ = batcher.flush_borrowed_and_reset(&mut staged_scratch, f)?;
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
                let batch = batcher.take_batch()?;
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
            let batch = batcher.take_batch()?;
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
                let _batch = batcher.take_batch()?;
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
            let _batch = batcher.take_batch()?;
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

#[cfg(test)]
mod drain_tests {
    use super::drain_ordered_batches;
    use crate::columnar::OwnedColumnarBatch;
    use crate::types::RowIndex;
    use std::collections::VecDeque;
    use std::ops::ControlFlow;

    fn batch(row_base: u64, row_count: usize) -> OwnedColumnarBatch {
        OwnedColumnarBatch {
            row_base: RowIndex(row_base),
            row_count,
            columns: Vec::new(),
        }
    }

    fn drain(
        buffered: &mut [VecDeque<OwnedColumnarBatch>],
        finished: &[bool],
        next_chunk: &mut usize,
    ) -> (Vec<u64>, u64, u64) {
        let chunk_count = buffered.len();
        let mut delivered_batches = 0u64;
        let mut delivered_rows = 0u64;
        let mut seen = Vec::new();
        let flow = drain_ordered_batches(
            buffered,
            finished,
            next_chunk,
            chunk_count,
            &mut delivered_batches,
            &mut delivered_rows,
            &mut |b| {
                seen.push(b.row_base.0);
                Ok(ControlFlow::Continue(()))
            },
        )
        .expect("drain");
        assert!(flow.is_continue());
        (seen, delivered_batches, delivered_rows)
    }

    // Regression: a batch buffered for a later chunk must be flushed once the
    // earlier chunks are finished and drained. Previously delivery only ran
    // when a new batch arrived, so a `Finished` event that advanced past empty
    // chunks would strand an already-buffered later batch (it had no following
    // batch to drive delivery), making the parallel stream drop rows.
    #[test]
    fn flushes_buffered_later_chunk_when_earlier_chunks_finish() {
        let mut buffered = vec![VecDeque::new(), VecDeque::new()];
        buffered[1].push_back(batch(2, 2)); // chunk 1's batch arrived first
        let mut next_chunk = 0;
        let (seen, delivered_batches, delivered_rows) =
            drain(&mut buffered, &[true, true], &mut next_chunk);

        assert_eq!(seen, vec![2]); // the otherwise-stranded batch is delivered
        assert_eq!(next_chunk, 2);
        assert_eq!(delivered_batches, 1);
        assert_eq!(delivered_rows, 2);
    }

    // Delivers in strict chunk order and blocks at an unfinished gap so a later
    // chunk that arrived early is not delivered ahead of its predecessor.
    #[test]
    fn delivers_in_order_and_blocks_on_unfinished_gap() {
        let mut buffered = vec![VecDeque::new(), VecDeque::new(), VecDeque::new()];
        buffered[0].push_back(batch(0, 2));
        buffered[2].push_back(batch(4, 2));
        let mut next_chunk = 0;
        let (seen, delivered_batches, _) =
            drain(&mut buffered, &[true, false, true], &mut next_chunk);

        assert_eq!(seen, vec![0]); // chunk 1 unfinished blocks chunk 2
        assert_eq!(next_chunk, 1);
        assert_eq!(delivered_batches, 1);
    }
}
