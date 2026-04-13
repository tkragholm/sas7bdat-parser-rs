use super::{
    BatchAccumulator, BatchDecodePlan, BatchHint, BatchSink, ColumnBuffer, ColumnarBatch,
    ControlFlow, Dataset, DecodeMode, Error, FileSource, OrderingMode, OwnedColumnarBatch,
    OwnedRow, Parallelism, Projection, RawRow, RawRowSink, Result, RowDecodePlan, RowSelection,
    RowSink, RowView, ScanProgress, ScanProgressObserver, ScanStats, StringDecodeOptions,
    TemporalDecodeOptions, borrow_column_buffers, effective_scan_row_capacity_hint,
    materialize_planned_cells, resolve_batch_row_capacity, scan_raw_rows, scan_row_bytes,
};
use rayon::prelude::{ParallelIterator, ParallelSlice};

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

    #[doc(hidden)]
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
    pub fn visit_raw_rows<F>(&self, mut f: F) -> Result<ScanStats>
    where
        F: FnMut(RawRow<'_>) -> Result<ControlFlow<()>>,
    {
        self.scan_raw_rows(&mut f)
    }

    #[doc(hidden)]
    /// # Errors
    ///
    /// Returns an error if the scan fails.
    pub fn visit_raw_rows_with_tap<F, T>(&self, mut f: F, tap: &mut T) -> Result<ScanStats>
    where
        F: FnMut(RawRow<'_>) -> Result<ControlFlow<()>>,
        T: FnMut(u64, &[u8]),
    {
        self.scan_raw_rows_with_tap(&mut f, tap)
    }

    /// # Errors
    ///
    /// Returns an error if the scan or row decoding fails.
    pub fn visit_rows<F>(&self, mut f: F) -> Result<ScanStats>
    where
        F: FnMut(RowView<'_>) -> Result<ControlFlow<()>>,
    {
        self.scan_rows(&mut f)
    }

    #[doc(hidden)]
    /// # Errors
    ///
    /// Returns an error if the scan or row decoding fails.
    pub fn visit_rows_with_tap<F, T>(&self, mut f: F, tap: &mut T) -> Result<ScanStats>
    where
        F: FnMut(RowView<'_>) -> Result<ControlFlow<()>>,
        T: FnMut(u64, &[u8]),
    {
        self.scan_rows_with_tap(&mut f, tap)
    }

    /// # Errors
    ///
    /// Returns an error if the scan or batch decoding fails.
    pub fn visit_batches<F>(&self, mut f: F) -> Result<ScanStats>
    where
        F: FnMut(ColumnarBatch<'_>) -> Result<ControlFlow<()>>,
    {
        self.scan_batches(&mut |batch| {
            let columns = borrow_column_buffers(&batch.columns);
            let batch = ColumnarBatch {
                row_base: batch.row_base,
                row_count: batch.row_count,
                columns: &columns,
            };
            f(batch)
        })
    }

    #[doc(hidden)]
    /// # Errors
    ///
    /// Returns an error if the scan or batch decoding fails.
    pub fn visit_batches_with_tap<F, T>(&self, mut f: F, tap: &mut T) -> Result<ScanStats>
    where
        F: FnMut(ColumnarBatch<'_>) -> Result<ControlFlow<()>>,
        T: FnMut(u64, &[u8]),
    {
        self.scan_batches_with_tap(
            &mut |batch| {
                let columns = borrow_column_buffers(&batch.columns);
                let batch = ColumnarBatch {
                    row_base: batch.row_base,
                    row_count: batch.row_count,
                    columns: &columns,
                };
                f(batch)
            },
            tap,
        )
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

        let plan = RowDecodePlan::new(self)?;
        let mut rows = Vec::with_capacity(usize::try_from(self.ds.metadata.row_count).unwrap_or(0));
        match plan.decode_mode {
            DecodeMode::Typed | DecodeMode::TypedLossless => {
                scan_row_bytes(self, &mut |row_index, bytes| {
                    plan.validate_row_bounds(bytes)?;
                    let mut cells = Vec::with_capacity(plan.columns.len());
                    for (column, kind) in plan.columns.iter().zip(&plan.owned_kinds) {
                        cells.push(plan.materialize_owned_cell_fast(bytes, column, *kind)?);
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

        let target_rows = resolve_batch_row_capacity(self)?;
        let capacity_hint_rows = effective_scan_row_capacity_hint(self).min(target_rows);
        if let Some(batches) = self.try_collect_batches_parallel(target_rows, capacity_hint_rows)? {
            return Ok(batches);
        }
        let target_rows_u64 = u64::try_from(target_rows).unwrap_or(u64::MAX).max(1);
        let estimated_batches = self.ds.metadata.row_count.div_ceil(target_rows_u64);
        let mut batches = Vec::with_capacity(usize::try_from(estimated_batches).unwrap_or(0));
        let mut batch_accumulator =
            BatchAccumulator::new(BatchDecodePlan::new(self)?, target_rows, capacity_hint_rows)
                .with_materialize_threads(resolved_batch_materialize_threads(self.parallelism));

        let _stats = scan_row_bytes(self, &mut |row_index, bytes| {
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
    pub fn write_raw_rows(&self, sink: &mut impl RawRowSink) -> Result<ScanStats> {
        self.visit_raw_rows(|row| sink.push(row))
    }

    /// # Errors
    ///
    /// Returns an error if the scan or sink fails.
    pub fn write_rows(&self, sink: &mut impl RowSink) -> Result<ScanStats> {
        self.visit_rows(|row| sink.push(row))
    }

    /// # Errors
    ///
    /// Returns an error if the scan or sink fails.
    pub fn write_batches(&self, sink: &mut impl BatchSink) -> Result<ScanStats> {
        self.visit_batches(|batch| sink.push(batch))
    }
}

impl ScanBuilder<'_> {
    fn try_collect_batches_parallel(
        &self,
        target_rows: usize,
        capacity_hint_rows: usize,
    ) -> Result<Option<Vec<OwnedColumnarBatch>>> {
        let page_count = self.ds.descriptors.pages.len();
        let workers = resolved_parallel_workers(self.parallelism, page_count);
        if workers <= 1 || page_count <= 1 || self.row_limit.is_some() {
            return Ok(None);
        }

        let file_bytes: &[u8] = match &self.ds.file.source {
            FileSource::Bytes(bytes) => bytes.as_ref(),
            FileSource::Mmap(mmap) => &mmap[..],
            FileSource::Path(_) => return Ok(None),
        };

        let worker_capacity_hint = capacity_hint_rows.div_ceil(workers).max(1);
        let plan = super::raw::RawScanPlan::compile(self);
        super::raw::RawScanPlan::validate_builder(self)?;
        if usize::from(self.ds.layout.row_len) == 0 {
            return Ok(Some(Vec::new()));
        }

        let chunk_size = page_count.div_ceil(workers).max(1);
        let results = self
            .ds
            .descriptors
            .pages
            .par_chunks(chunk_size)
            .map(|chunk| {
                collect_batches_for_descriptor_chunk(
                    self,
                    file_bytes,
                    chunk,
                    &plan,
                    target_rows,
                    worker_capacity_hint,
                )
            })
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

fn collect_batches_for_descriptor_chunk(
    builder: &ScanBuilder<'_>,
    file_bytes: &[u8],
    descriptors: &[crate::internal::PageDescriptor],
    plan: &super::raw::RawScanPlan,
    target_rows: usize,
    capacity_hint_rows: usize,
) -> Result<Vec<OwnedColumnarBatch>> {
    let target_rows_u64 = u64::try_from(target_rows).unwrap_or(u64::MAX).max(1);
    let estimated_batches = builder.ds.metadata.row_count.div_ceil(target_rows_u64);
    let mut batches = Vec::with_capacity(usize::try_from(estimated_batches).unwrap_or(0));
    let mut batch_accumulator = BatchAccumulator::new(
        BatchDecodePlan::new(builder)?,
        target_rows,
        capacity_hint_rows,
    )
    .with_materialize_threads(1);
    let mut stats = ScanStats::default();
    let mut decompressed_row = Vec::new();

    for &descriptor in descriptors {
        let page = super::raw::page_slice(file_bytes, plan, descriptor)?;
        stats.pages_seen = stats.pages_seen.saturating_add(1);
        stats.raw_bytes_read = stats
            .raw_bytes_read
            .saturating_add(u64::try_from(page.len()).unwrap_or(u64::MAX));
        super::raw::emit_rows_from_page(
            builder,
            plan,
            descriptor,
            page,
            &mut decompressed_row,
            &mut stats,
            &mut |row_index, bytes| {
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
        scan_raw_rows(self, f)
    }

    fn scan_raw_rows_with_tap<F, T>(&self, f: &mut F, tap: &mut T) -> Result<ScanStats>
    where
        F: FnMut(RawRow<'_>) -> Result<ControlFlow<()>>,
        T: FnMut(u64, &[u8]),
    {
        scan_raw_rows(self, &mut |raw| {
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

        let plan = RowDecodePlan::new(self)?;
        let mut owned_strings = Vec::new();
        self.scan_raw_rows(&mut |raw| {
            let planned = plan.plan_cells(raw.bytes, &mut owned_strings)?;
            let cells = materialize_planned_cells(&planned, &owned_strings)?;
            let row = RowView {
                row_index: raw.row_index,
                names: plan.names.as_ref(),
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

        let plan = RowDecodePlan::new(self)?;
        let mut owned_strings = Vec::new();
        self.scan_raw_rows_with_tap(
            &mut |raw| {
                let planned = plan.plan_cells(raw.bytes, &mut owned_strings)?;
                let cells = materialize_planned_cells(&planned, &owned_strings)?;
                let row = RowView {
                    row_index: raw.row_index,
                    names: plan.names.as_ref(),
                    cells: &cells,
                };
                f(row)
            },
            tap,
        )
    }

    fn scan_batches<F>(&self, f: &mut F) -> Result<ScanStats>
    where
        F: FnMut(OwnedColumnarBatch) -> Result<ControlFlow<()>>,
    {
        if let Some(mut batches) = self.try_collect_batches_parallel(
            resolve_batch_row_capacity(self)?,
            effective_scan_row_capacity_hint(self).min(resolve_batch_row_capacity(self)?),
        )? {
            let mut stats = ScanStats {
                decode_batches: u64::try_from(batches.len()).unwrap_or(u64::MAX),
                ..ScanStats::default()
            };
            for batch in batches.drain(..) {
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

        let target_rows = resolve_batch_row_capacity(self)?;
        let capacity_hint_rows = effective_scan_row_capacity_hint(self).min(target_rows);
        let mut batcher =
            BatchAccumulator::new(BatchDecodePlan::new(self)?, target_rows, capacity_hint_rows)
                .with_materialize_threads(resolved_batch_materialize_threads(self.parallelism));
        let mut decode_batches = 0u64;
        let mut stop_after_current_batch = false;

        let mut stats = scan_row_bytes(self, &mut |row_index, bytes| {
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
}
