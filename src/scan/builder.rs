use super::{
    BatchAccumulator, BatchDecodePlan, BatchHint, BatchSink, ColumnBuffer, ColumnarBatch,
    ControlFlow, Dataset, DecodeMode, Error, OrderingMode, OwnedColumnarBatch, OwnedRow,
    Parallelism, Projection, RawRow, RawRowSink, Result, RowDecodePlan, RowSelection, RowSink,
    RowView, ScanProgress, ScanProgressObserver, ScanStats, StringDecodeOptions,
    TemporalDecodeOptions, borrow_column_buffers, effective_scan_row_capacity_hint,
    materialize_planned_cells, resolve_batch_row_capacity, scan_raw_rows, scan_row_bytes,
};
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
            DecodeMode::Typed => {
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
            DecodeMode::TypedLossless => {
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
        let target_rows_u64 = u64::try_from(target_rows).unwrap_or(u64::MAX).max(1);
        let estimated_batches = self.ds.metadata.row_count.div_ceil(target_rows_u64);
        let mut batches = Vec::with_capacity(usize::try_from(estimated_batches).unwrap_or(0));
        let mut batch_accumulator = BatchAccumulator::new(
            BatchDecodePlan::new(self)?,
            target_rows,
            capacity_hint_rows,
        );

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

const fn _keep_type_imports_alive<'a>(_columns: &'a [ColumnBuffer<'a>], _dataset: &'a Dataset) {}

impl ScanBuilder<'_> {
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
        if matches!(self.decode, DecodeMode::Raw) {
            return Err(Error::unsupported(
                "visit_batches does not support DecodeMode::Raw",
            ));
        }

        let target_rows = resolve_batch_row_capacity(self)?;
        let capacity_hint_rows = effective_scan_row_capacity_hint(self).min(target_rows);
        let mut batcher = BatchAccumulator::new(
            BatchDecodePlan::new(self)?,
            target_rows,
            capacity_hint_rows,
        );
        let mut decode_batches = 0u64;
        let mut stop_after_current_batch = false;

        let mut stats = scan_row_bytes(self, &mut |row_index, bytes| {
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

        stats.decode_batches = decode_batches;
        Ok(stats)
    }

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
        let mut batcher = BatchAccumulator::new(
            BatchDecodePlan::new(self)?,
            target_rows,
            capacity_hint_rows,
        );
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

        stats.decode_batches = decode_batches;
        Ok(stats)
    }
}
