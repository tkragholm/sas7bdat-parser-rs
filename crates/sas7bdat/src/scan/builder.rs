use super::plan::ScanPlan;
use super::raw::{scan_raw_rows_with_plan, scan_row_bytes_with_plan};
use super::{
    BatchAccumulator, BatchDecodePlan, BatchHint, ColumnBuffer, ColumnMajorDecode, ColumnarBatch,
    ControlFlow, Dataset, DecodeMode, Error, FileSource, OrderingMode, OwnedBatchScanBreakdown,
    OwnedColumnarBatch, OwnedRow, Parallelism, Projection, RawRow, Result, RowSelection, RowView,
    ScanProgress, ScanProgressObserver, ScanStats, StringDecodeOptions, TemporalDecodeOptions,
    materialize_planned_cells,
};
#[cfg(feature = "arrow")]
use arrow_array::RecordBatch;
#[cfg(feature = "arrow")]
use arrow_schema::SchemaRef;
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{SyncSender, sync_channel},
    },
    time::Instant,
};

/// Thread budget for [`Parallelism::Auto`]: every logical core.
///
/// No fixed ceiling, since a ceiling would leave most of a many-core host idle on the large
/// files this crate targets. `resolved_parallel_workers` clamps to the available work, so
/// small files do not over-spawn.
///
/// These are `std::thread::scope` threads rather than a shared pool, so concurrent scans
/// multiply the budget; those callers should set [`Parallelism::Threads`].
fn auto_thread_budget() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

fn resolved_batch_materialize_threads(parallelism: Parallelism) -> usize {
    match parallelism {
        Parallelism::Threads(n) => n.max(1),
        Parallelism::Auto => auto_thread_budget(),
        Parallelism::None => 1,
    }
}

pub(super) fn resolved_parallel_workers(parallelism: Parallelism, work_items: usize) -> usize {
    match parallelism {
        Parallelism::Threads(n) => n.max(1).min(work_items.max(1)),
        Parallelism::Auto => auto_thread_budget().min(work_items.max(1)),
        Parallelism::None => 1,
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
    by_oversubscribe.max(pages_per_batch(rows_per_page, target_rows))
}

/// Pages needed to fill one target batch. Each chunk decodes with its own accumulator and
/// flushes at its end, so chunk size sets batch size. Sizing a chunk by bytes alone gives
/// 136-row batches on a table with 4 rows per page, where per-batch overhead exceeds what
/// the larger reads save.
pub(super) fn pages_per_batch(rows_per_page: u64, target_rows: usize) -> usize {
    usize::try_from(
        u64::try_from(target_rows)
            .unwrap_or(u64::MAX)
            .div_ceil(rows_per_page.max(1)),
    )
    .unwrap_or(1)
    .max(1)
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

/// Counters the parallel paths bump as each chunk finishes.
///
/// The serial scan reports progress from the loop that reads the pages. A parallel scan has no
/// such loop — the workers only surrender their statistics when they are joined, which is after
/// the scan is over. These are updated per chunk instead, so the collector can report while the
/// work is still running.
#[derive(Default)]
pub(super) struct ProgressCounters {
    pages: std::sync::atomic::AtomicU64,
    compressed_pages: std::sync::atomic::AtomicU64,
    bytes: std::sync::atomic::AtomicU64,
    rows_seen: std::sync::atomic::AtomicU64,
}

impl ProgressCounters {
    pub(super) fn add(&self, stats: &ScanStats) {
        self.pages.fetch_add(stats.pages_seen, Ordering::Relaxed);
        self.compressed_pages
            .fetch_add(stats.compressed_pages, Ordering::Relaxed);
        self.bytes
            .fetch_add(stats.raw_bytes_read, Ordering::Relaxed);
        self.rows_seen.fetch_add(stats.rows_seen, Ordering::Relaxed);
    }
}

/// What the collector needs to report progress: where to send it, the running counters, and
/// the totals to measure them against.
#[derive(Clone, Copy)]
pub(super) struct ProgressReport<'a> {
    pub observer: Option<&'a ScanProgressObserver>,
    pub counters: &'a ProgressCounters,
    pub total_pages: u64,
    pub estimated_total_bytes: u64,
}

impl ProgressReport<'_> {
    /// Snapshot of a scan in flight. Called once per delivered batch, which for a large file
    /// is a few thousand times over many minutes.
    fn emit(self, rows_emitted: u64) {
        let Some(observer) = self.observer else {
            return;
        };
        observer(ScanProgress {
            pages_seen: self.counters.pages.load(Ordering::Relaxed),
            total_pages: self.total_pages,
            raw_bytes_read: self.counters.bytes.load(Ordering::Relaxed),
            estimated_total_bytes: self.estimated_total_bytes,
            compressed_pages: self.counters.compressed_pages.load(Ordering::Relaxed),
            // A chunk only reports once it is fully decoded, so the running count can lag the
            // rows already handed over.
            rows_seen: self
                .counters
                .rows_seen
                .load(Ordering::Relaxed)
                .max(rows_emitted),
            rows_emitted,
        });
    }
}

/// Deliver decoded batches to `f`, in chunk order unless the scan is
/// [`OrderingMode::Unordered`]. Returns the delivered batch and row counts.
///
/// Every chunk index below `chunk_count` must eventually produce a `Finished`, including any
/// the producer skipped — ordered delivery cannot advance past a chunk that never reports in.
///
/// Takes `rx` by value and drops it on the way out, so workers parked on a full channel are
/// released before the caller joins them.
pub(super) fn collect_streamed_batches<F>(
    rx: std::sync::mpsc::Receiver<StreamedBatchMessage>,
    ordering: OrderingMode,
    chunk_count: usize,
    stop: &AtomicBool,
    progress: ProgressReport<'_>,
    f: &mut F,
) -> Result<(u64, u64)>
where
    F: FnMut(OwnedColumnarBatch) -> Result<ControlFlow<()>>,
{
    let outcome = deliver_streamed_batches(rx, ordering, chunk_count, stop, progress, f);
    // Any exit that isn't the end of the stream leaves producers still working. They see the
    // dropped receiver soon enough, but not while parked inside a blocking send.
    if outcome.is_err() {
        stop.store(true, Ordering::Relaxed);
    }
    outcome
}

fn deliver_streamed_batches<F>(
    rx: std::sync::mpsc::Receiver<StreamedBatchMessage>,
    ordering: OrderingMode,
    chunk_count: usize,
    stop: &AtomicBool,
    progress: ProgressReport<'_>,
    f: &mut F,
) -> Result<(u64, u64)>
where
    F: FnMut(OwnedColumnarBatch) -> Result<ControlFlow<()>>,
{
    let mut buffered = (0..chunk_count)
        .map(|_| VecDeque::<OwnedColumnarBatch>::new())
        .collect::<Vec<_>>();
    let mut finished = vec![false; chunk_count];
    let mut next_chunk = 0usize;
    let mut delivered_batches = 0u64;
    let mut delivered_rows = 0u64;

    for message in rx {
        match message {
            StreamedBatchMessage::Batch { chunk_idx, batch } => {
                if matches!(ordering, OrderingMode::Unordered) {
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
                progress.emit(delivered_rows);
            }
            StreamedBatchMessage::Finished { chunk_idx } => {
                finished[chunk_idx] = true;
                if matches!(ordering, OrderingMode::Stable)
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
                // A chunk finishing can release batches that arrived out of order, including
                // the last ones in the file, so this is not only a batch-message concern.
                progress.emit(delivered_rows);
            }
            StreamedBatchMessage::Error(err) => {
                stop.store(true, Ordering::Relaxed);
                return Err(err);
            }
        }
    }

    Ok((delivered_batches, delivered_rows))
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

    /// The absolute row range this scan emits, resolving [`Self::select`] and [`Self::limit`]
    /// into one bound. Every path that needs to know which rows are wanted goes through here,
    /// so the two knobs cannot drift apart in cost or meaning again.
    pub(super) fn row_window(&self) -> super::raw::RowWindow {
        super::raw::RowWindow::resolve(self.row_selection, self.row_limit)
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
    /// The observer receives a [`ScanProgress`] snapshot as the scan runs: after each page on
    /// the serial path, and after each delivered batch on the parallel ones. The callback must
    /// be `Send + Sync` because it may be invoked from a scan thread.
    #[must_use]
    pub fn with_progress<F>(self, observer: F) -> Self
    where
        F: Fn(ScanProgress) + Send + Sync + 'static,
    {
        self.with_progress_observer(std::sync::Arc::new(observer))
    }

    /// [`Self::with_progress`] for an observer that is already shared, so one callback can be
    /// reused across scans without being boxed again.
    #[must_use]
    pub fn with_progress_observer(mut self, observer: ScanProgressObserver) -> Self {
        self.progress = Some(observer);
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

    /// Which decode pipeline this scan will take, for the given entry point.
    ///
    /// The pipeline depends on the method you call as much as on the file: `visit_batches`
    /// and `collect_batches` decode the same data through different code, and only the
    /// latter can reach the tiled column-major fill. Label a benchmark with this, or it may
    /// be measuring a path the change under test cannot reach.
    ///
    /// # Errors
    ///
    /// Returns an error if opening or planning the scan fails.
    pub fn predict_path(&self, entry: crate::ScanEntry) -> Result<crate::ScanPath> {
        // Deliberately a truncated real scan rather than a re-statement of the conditions.
        // The selection chain is long (an in-memory source, a whole-file window, cached
        // descriptors, page geometry, the batch plan's families, a parallel-worker decision)
        // and spread across three modules. A predicate that restated it would be correct
        // only until one of them moved. Running it and stopping at the first item cannot
        // drift, at the cost of decoding one batch or row.
        let stats = match entry {
            crate::ScanEntry::Batches => self.scan_batches_with_column_major(
                matches!(self.column_major, ColumnMajorDecode::On),
                &mut |_| Ok(ControlFlow::Break(())),
            )?,
            crate::ScanEntry::BorrowedBatches => {
                let mut stats = self.scan_batches_borrowed_with_tap(
                    &mut |_| Ok(ControlFlow::Break(())),
                    &mut |_, _| {},
                )?;
                stats.path = crate::ScanPath::borrowed_batches();
                stats
            }
            crate::ScanEntry::Rows => self.scan_rows(&mut |_| Ok(ControlFlow::Break(())))?,
            crate::ScanEntry::RawRows => self.scan_raw_rows(&mut |_| Ok(ControlFlow::Break(())))?,
        };
        Ok(stats.path)
    }

    /// # Errors
    ///
    /// Returns an error if the scan or batch decoding fails.
    pub fn visit_batches<F>(&self, mut f: F) -> Result<crate::ScanStatsSummary>
    where
        F: FnMut(ColumnarBatch<'_>) -> Result<ControlFlow<()>>,
    {
        self.scan_batches_borrowed_with_tap(&mut f, &mut |_, _| {})
            .map(|mut stats| {
                stats.path = crate::ScanPath::borrowed_batches();
                stats.summary()
            })
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

    /// Collect the streamed owned-batch scan into a `Vec`.
    ///
    /// Deliberately a thin wrapper over [`Self::visit_owned_batches`] rather than its own
    /// dispatch tree. The streaming driver already chooses between the fused single-pass scan,
    /// the parallel mapped and extent-streamed readers, the column-major fill and the row-major
    /// loop; a second copy of that choice lived here and had drifted from it — the collect side
    /// never grew the fused or extent-streamed branches, so a path source collected serially in
    /// two passes while the identical scan streamed in one.
    fn collect_batches_inner(&self, column_major: bool) -> Result<Vec<OwnedColumnarBatch>> {
        // Pre-size from the batch hint alone. `resolve_batch_row_capacity` reads the builder,
        // so the estimate costs no plan compilation of its own.
        let target_rows = u64::try_from(super::plan::resolve_batch_row_capacity(self)?)
            .unwrap_or(u64::MAX)
            .max(1);
        let estimated = self.ds.metadata.row_count.div_ceil(target_rows);
        let mut batches = Vec::with_capacity(usize::try_from(estimated).unwrap_or(0));
        self.scan_batches_with_column_major(column_major, &mut |batch| {
            batches.push(batch);
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(batches)
    }
}

/// In-memory file bytes for the column-major path, or `None` if it must not engage: the path
/// only handles whole-file scans of an in-memory source, because its fill walks a page's rows
/// by position rather than by row index. The caller additionally requires an all-staged-numeric
/// plan.
fn column_major_file_bytes<'a>(
    builder: &'a ScanBuilder<'_>,
    column_major: bool,
) -> Option<&'a [u8]> {
    if !column_major || !builder.row_window().is_whole_file() {
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
    window: super::raw::PageWindow<'a>,
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
        let page = super::raw::page_slice(ctx.window, ctx.raw_plan, descriptor)?;
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
            // Observed, not assumed: a source that permits tiling can still fill row-major
            // when the plan or the page class declines, and that difference is exactly what
            // a benchmark of the tiled fill needs to know.
            stats.path.fill = crate::FillStrategy::Tiled;
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
                if acc.plan_is_all_columns_staged_numeric() {
                    acc.push_contiguous_span_all_staged_numeric(
                        page_row_base,
                        page,
                        row_off,
                        span,
                        data_start,
                        ctx.row_len,
                    )?;
                } else {
                    acc.push_contiguous_span_mixed(
                        page_row_base,
                        page,
                        row_off,
                        span,
                        data_start,
                        ctx.row_len,
                    )?;
                }
                row_off += span;
            }
        } else {
            if matches!(stats.path.fill, crate::FillStrategy::Unrecorded) {
                stats.path.fill = crate::FillStrategy::RowMajor;
            }
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

/// Copy the accumulator's per-family cell counters into `stats`.
const fn store_batch_counters(stats: &mut ScanStats, counters: super::batch::BatchFamilyCounters) {
    stats.batch_staged_numeric_cells = counters.staged_numeric;
    stats.batch_direct_raw_bytes_cells = counters.direct_raw_bytes;
    stats.batch_direct_utf8_single_byte_cells = counters.direct_utf8_single_byte;
    stats.batch_direct_utf8_borrowed_cells = counters.direct_utf8_borrowed;
    stats.batch_direct_utf8_owned_cells = counters.direct_utf8_owned;
    stats.batch_direct_utf8_owned_interned_hits = counters.direct_utf8_owned_interned_hits;
    stats.batch_direct_utf8_owned_seen_once_promotions =
        counters.direct_utf8_owned_seen_once_promotions;
    stats.batch_fallback_cells = counters.fallback;
}

/// A parallel streamed scan, fully decided before any thread starts.
///
/// Exists to keep [`ScanBuilder::plan_parallel_stream`] and the `thread::scope` that consumes
/// it readable apart: one picks chunk sizes, read spans and worker counts from geometry, the
/// other only spawns and collects.
struct ParallelStreamPlan<'a> {
    /// Page descriptors cut into per-chunk slices. A chunk is both the unit of work and the
    /// batch boundary.
    chunks: Vec<&'a [crate::internal::PageDescriptor]>,
    /// One contiguous `(offset, len)` read per chunk. Empty for mapped sources, which decode
    /// straight out of the existing buffer.
    spans: Vec<(u64, usize)>,
    mapped: Option<&'a [u8]>,
    source_path: Option<&'a std::path::Path>,
    workers: usize,
    total_pages: u64,
    estimated_total_bytes: u64,
    context: DescriptorChunkContext<'a>,
}

/// What planning concluded about a candidate parallel scan.
enum ParallelStreamDecision<'a> {
    /// This scan is not a fit — too few pages or workers. The caller falls back.
    NotApplicable,
    /// The rows are zero bytes wide, so there is nothing to decode. Distinct from
    /// `NotApplicable`: falling back would just rediscover the same emptiness.
    NothingToScan,
    Run(ParallelStreamPlan<'a>),
}

/// Inputs every chunk decode shares. The descriptor table is passed separately because the
/// fused scan builds one per extent rather than one for the whole file.
pub(super) struct DescriptorChunkContext<'a> {
    pub raw_plan: &'a super::raw::RawScanPlan,
    pub batch_plan: &'a BatchDecodePlan,
    pub target_rows: usize,
    pub capacity_hint_rows: usize,
    pub row_len: usize,
    pub columnar: bool,
}

pub(super) enum StreamedBatchMessage {
    Batch {
        chunk_idx: usize,
        batch: OwnedColumnarBatch,
    },
    Finished {
        chunk_idx: usize,
    },
    Error(Error),
}

pub(super) fn stream_batches_for_descriptor_chunk(
    window: super::raw::PageWindow<'_>,
    descriptor_table: &crate::internal::PageDescriptorTable,
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
        descriptors: descriptor_table,
        raw_plan: context.raw_plan,
        window,
        row_len: context.row_len,
        columnar: context.columnar && acc.plan_can_fill_span_column_major(),
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

pub(super) const fn merge_scan_stats(into: &mut ScanStats, from: &ScanStats) {
    into.rows_seen = into.rows_seen.saturating_add(from.rows_seen);
    into.rows_emitted = into.rows_emitted.saturating_add(from.rows_emitted);
    into.pages_seen = into.pages_seen.saturating_add(from.pages_seen);
    // A worker that tiled anything makes the whole scan tiled: the question the caller is
    // asking is whether the fill ran at all, not on what fraction of pages.
    if matches!(from.path.fill, crate::FillStrategy::Tiled) {
        into.path.fill = crate::FillStrategy::Tiled;
    } else if matches!(into.path.fill, crate::FillStrategy::Unrecorded) {
        into.path.fill = from.path.fill;
    }
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
        let mut stats = scan_raw_rows_with_plan(self, &plan.raw, f)?;
        stats.path = crate::ScanPath::rows();
        Ok(stats)
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
        let mut stats = scan_raw_rows_with_plan(self, &plan.raw, &mut |raw| {
            let planned = plan.row.plan_cells(raw.bytes, &mut owned_strings)?;
            let cells = materialize_planned_cells(&planned, &owned_strings)?;
            let row = RowView {
                row_index: raw.row_index,
                names: plan.row.names.as_ref(),
                cells: &cells,
            };
            f(row)
        })?;
        stats.path = crate::ScanPath::rows();
        Ok(stats)
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
        self.scan_batches_with_column_major(matches!(self.column_major, ColumnMajorDecode::On), f)
    }

    /// The owned-batch driver, with the column-major choice passed in rather than read from the
    /// builder — `collect_batches_columnar` forces it on to benchmark the two fills head-to-head.
    fn scan_batches_with_column_major<F>(&self, column_major: bool, f: &mut F) -> Result<ScanStats>
    where
        F: FnMut(OwnedColumnarBatch) -> Result<ControlFlow<()>>,
    {
        // Checked here rather than in the row-major fallback alone, so every owned-batch entry
        // point rejects `Raw` the same way instead of only the scans that reach that branch.
        if matches!(self.decode, DecodeMode::Raw) {
            return Err(Error::unsupported(
                "owned batch scans do not support DecodeMode::Raw; use visit_raw_rows instead",
            ));
        }
        let plan = ScanPlan::new(self)?;
        // One pass where it applies: compiling descriptors and decoding rows from the same
        // reads halves the bytes moved, which is the whole cost on a network share.
        if let Some(mut stats) =
            super::fused::try_stream_batches_fused(self, &plan, column_major, f)?
        {
            stats.path = stats.path.with_source(crate::PageSource::OnePass);
            return Ok(stats);
        }
        if let Some(mut stats) = self.try_stream_batches_parallel(&plan, column_major, f)? {
            stats.path = stats.path.with_source(crate::PageSource::TwoPassParallel);
            return Ok(stats);
        }
        if column_major
            && plan.batch.can_fill_span_column_major()
            && let Some(file_bytes) = column_major_file_bytes(self, true)
        {
            let mut stats = self.stream_batches_columnar_serial(&plan, file_bytes, f)?;
            stats.path = stats.path.with_source(crate::PageSource::TwoPassSerial);
            return Ok(stats);
        }
        let mut stats = self.scan_batches_with_tap(f, &mut |_, _| {})?;
        stats.path = stats.path.with_source(crate::PageSource::TwoPassSerial);
        Ok(stats)
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
            window: super::raw::PageWindow::whole_file(file_bytes),
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

    /// Plan a parallel streamed scan, or `None` when this scan should take another path.
    ///
    /// Split from the run so the two can be read separately: everything here is decided before
    /// a thread exists, and nothing here touches file bytes.
    fn plan_parallel_stream<'p>(
        &'p self,
        plan: &'p ScanPlan,
        descriptors: &'p crate::internal::PageDescriptorTable,
        column_major: bool,
    ) -> Result<ParallelStreamDecision<'p>> {
        // Chunk only the pages the window wants. A bounded scan is parallel now that the bound
        // is an absolute row range rather than a per-worker emitted-row count, so this is what
        // keeps it from also reading the rest of the file. Worker count follows the windowed
        // page count, so a small limit does not spawn a thread per idle core.
        let window_pages = plan.raw.window().page_range(&descriptors.pages);
        let page_count = window_pages.len();
        let workers = resolved_parallel_workers(self.parallelism, page_count);
        if workers <= 1 || page_count <= 1 {
            return Ok(ParallelStreamDecision::NotApplicable);
        }

        // Mapped and in-memory sources decode straight out of one contiguous buffer. A path
        // source has no buffer to decode from, so pages arrive as extents streamed by a small
        // pool of concurrent positional reads — see `super::extent`.
        let mapped: Option<&[u8]> = match &self.ds.file.source {
            FileSource::Bytes(bytes) => Some(bytes.as_ref()),
            FileSource::Mmap(mmap) => Some(&mmap[..]),
            FileSource::Path(_) => None,
        };
        let source_path: Option<&std::path::Path> = match &self.ds.file.source {
            FileSource::Path(path) => Some(path.as_path()),
            _ => None,
        };

        super::raw::RawScanPlan::validate_builder(self)?;
        let row_len = usize::from(self.ds.layout.row_len);
        if row_len == 0 {
            return Ok(ParallelStreamDecision::NothingToScan);
        }

        // Extent-sized chunks for streamed sources: the chunk IS the unit of I/O there, so it
        // is sized by the measured best read size rather than by worker count.
        let chunk_size = if mapped.is_some() {
            balanced_chunk_size(
                page_count,
                workers,
                self.ds.layout.rows_per_page,
                plan.batch_row_capacity,
            )
        } else {
            // Extent size is an I/O choice, but the chunk is also the batch boundary, so it
            // must still cover one batch — otherwise wide tables get tiny batches.
            super::extent::pages_per_extent(plan.raw.page_size()).max(pages_per_batch(
                self.ds.layout.rows_per_page,
                plan.batch_row_capacity,
            ))
        };
        let chunks: Vec<&[crate::internal::PageDescriptor]> =
            descriptors.pages[window_pages].chunks(chunk_size).collect();
        // Byte span per chunk, precomputed so the reader threads do no descriptor work: each
        // is one contiguous read covering that chunk's pages. Empty for mapped sources.
        let spans: Vec<(u64, usize)> = if mapped.is_some() {
            Vec::new()
        } else {
            let raw = &plan.raw;
            chunks
                .iter()
                .map(|chunk| {
                    super::extent::chunk_span(
                        chunk,
                        |page_index| raw.page_offset(page_index),
                        raw.page_size(),
                    )
                    .ok_or_else(|| Error::corruption("page span overflow while planning reads"))
                })
                .collect::<Result<Vec<_>>>()?
        };

        Ok(ParallelStreamDecision::Run(ParallelStreamPlan {
            chunks,
            spans,
            mapped,
            source_path,
            workers,
            total_pages: u64::try_from(page_count).unwrap_or(u64::MAX),
            estimated_total_bytes: u64::try_from(page_count)
                .unwrap_or(u64::MAX)
                .saturating_mul(u64::try_from(plan.raw.page_size()).unwrap_or(0)),
            context: DescriptorChunkContext {
                raw_plan: &plan.raw,
                batch_plan: &plan.batch,
                target_rows: plan.batch_row_capacity,
                capacity_hint_rows: plan.capacity_hint_rows.div_ceil(workers).max(1),
                row_len,
                // The column-major fill ignores row selection, so it only applies to whole-file
                // scans; each worker additionally requires an all-staged-numeric plan.
                columnar: column_major && self.row_window().is_whole_file(),
            },
        }))
    }

    /// Run a planned parallel stream: spawn the workers, collect batches in order, join.
    ///
    /// Still long, and deliberately not split further. What remains is one `thread::scope`
    /// whose pieces all borrow the same scope-local channels, stop flag and counters; lifting
    /// either spawn branch out would mean a helper taking a dozen `&'scope` parameters, which
    /// reads worse than the loop it replaced. The planning that *could* be separated already is.
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
        let stream = match self.plan_parallel_stream(plan, descriptors.as_ref(), column_major)? {
            ParallelStreamDecision::NotApplicable => return Ok(None),
            ParallelStreamDecision::NothingToScan => return Ok(Some(ScanStats::default())),
            ParallelStreamDecision::Run(stream) => stream,
        };

        let ParallelStreamPlan {
            chunks,
            spans,
            mapped,
            source_path,
            workers,
            total_pages,
            estimated_total_bytes,
            context,
        } = stream;
        let chunk_count = chunks.len();
        let spans = &spans;
        // Declared outside `thread::scope` so the shared references satisfy the `'scope` bound.
        let next_chunk_idx = AtomicUsize::new(0);
        let descriptor_table = descriptors.as_ref();
        let counters = ProgressCounters::default();
        let counters = &counters;
        let progress = ProgressReport {
            observer: self.progress.as_ref(),
            counters,
            total_pages,
            estimated_total_bytes,
        };

        let total_stats = std::thread::scope(|scope| -> Result<ScanStats> {
            let channel_bound = workers.saturating_mul(2).max(1);
            let (tx, rx) = sync_channel(channel_bound);
            let stop = Arc::new(AtomicBool::new(false));
            let next_chunk_idx = &next_chunk_idx;
            let chunks = &chunks;
            let mut handles = Vec::with_capacity(workers);

            let (io_err_tx, io_err_rx) = sync_channel::<Error>(workers.max(1));

            if let Some(file_bytes) = mapped {
                // A fixed pool of `workers` threads pulls chunk indices from a shared counter,
                // so `chunk_count` can exceed `workers` (giving stealing slack to balance
                // uneven chunks) without spawning more threads than the resolved core budget.
                // Batches stay tagged with their chunk index, so ordered delivery is unaffected.
                let window = super::raw::PageWindow::whole_file(file_bytes);
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
                                window,
                                descriptor_table,
                                chunks[idx],
                                idx,
                                context,
                                &tx,
                                stop.as_ref(),
                            );
                            counters.add(&chunk_stats);
                            merge_scan_stats(&mut worker_stats, &chunk_stats);
                        }
                        worker_stats
                    }));
                }
            } else if let Some(path) = source_path {
                // Streamed source: readers keep large reads in flight while decode threads
                // consume whole extents. The pools are sized independently, since read
                // throughput peaks at a few concurrent requests while decode scales with
                // cores.
                let stream = super::extent::spawn_readers(
                    scope,
                    super::extent::ReadPlan {
                        path,
                        spans,
                        next_chunk_idx,
                    },
                    &stop,
                    workers,
                    &io_err_tx,
                );
                let extents = Arc::new(std::sync::Mutex::new(stream.extents));
                let recycle = stream.recycle;
                for _ in 0..workers {
                    let tx = tx.clone();
                    let stop = stop.clone();
                    let context = &context;
                    let extents = Arc::clone(&extents);
                    let recycle = recycle.clone();
                    handles.push(scope.spawn(move || {
                        let mut worker_stats = ScanStats::default();
                        loop {
                            if stop.load(Ordering::Relaxed) {
                                break;
                            }
                            let Some(extent) = extents.lock().ok().and_then(|rx| rx.recv().ok())
                            else {
                                break;
                            };
                            let window = super::raw::PageWindow {
                                bytes: &extent.bytes,
                                base_offset: extent.base_offset,
                            };
                            let chunk_stats = stream_batches_for_descriptor_chunk(
                                window,
                                descriptor_table,
                                chunks[extent.chunk_idx],
                                extent.chunk_idx,
                                context,
                                &tx,
                                stop.as_ref(),
                            );
                            counters.add(&chunk_stats);
                            merge_scan_stats(&mut worker_stats, &chunk_stats);
                            let _ = recycle.send(extent.bytes);
                        }
                        worker_stats
                    }));
                }
                // The workers hold the only handles that matter now. Keeping these alive here
                // would leave a reader parked on a send no one will ever receive, or on a
                // recycled buffer no one will ever return, once the consumer stops early.
                drop(extents);
                drop(recycle);
            }
            drop(tx);
            drop(io_err_tx);

            let (delivered_batches, delivered_rows) = match collect_streamed_batches(
                rx,
                self.ordering,
                chunk_count,
                stop.as_ref(),
                progress,
                f,
            ) {
                Ok(counts) => counts,
                Err(err) => {
                    for handle in handles {
                        let _ = handle.join();
                    }
                    return Err(err);
                }
            };

            let mut total = ScanStats::default();
            for handle in handles {
                let worker_stats = handle
                    .join()
                    .map_err(|_| Error::internal("parallel batch worker panicked"))?;
                merge_scan_stats(&mut total, &worker_stats);
            }
            // A failed read closes its extent channel, which decoders see as end of work,
            // so the scan would otherwise return fewer rows and report success.
            if let Ok(err) = io_err_rx.try_recv() {
                return Err(err);
            }
            total.decode_batches = delivered_batches;
            total.rows_emitted = delivered_rows;
            Ok(total)
        })?;
        Ok(Some(total_stats))
    }

    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    #[allow(clippy::doc_markdown)]
    /// Batches that borrow the page. **A different pipeline from the owned batch path**: it
    /// does not go through `stream_descriptors_into_batches`, so the tiled column-major fill
    /// never applies here however the plan is shaped.
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

        stats.decode_batches = decode_batches;
        store_batch_counters(&mut stats, batcher.counters());
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

        stats.decode_batches = decode_batches;
        store_batch_counters(&mut stats, batcher.counters());
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

        stats.decode_batches = decode_batches;
        store_batch_counters(&mut stats, batcher.counters());

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
