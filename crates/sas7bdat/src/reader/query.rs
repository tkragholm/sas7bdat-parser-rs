use super::{
    ProjectedRowIter, ProjectedRowWindow, RowLookup, RowSelection, RowViewIter, RowWindow,
    SasReader, frame::FrameBlueprint, row::RowProjection,
};
use crate::{
    cell::CellValue,
    error::{Error, Result},
    logger::log_warn,
    parser::{
        DecodePolicy, ParallelScanConfig, RawRowBatch, RawScanStats, RowIterator,
        scan_file_projected_rows_with_decode_policy,
        scan_file_projected_rows_with_decode_policy_unordered, scan_file_raw_rows,
        scan_file_raw_rows_unordered_batched_with_stats, scan_file_raw_rows_unordered_with_stats,
        scan_file_rows_with_decode_policy, scan_file_rows_with_decode_policy_unordered,
    },
    reader::frame::FrameBatch,
};
use std::{
    borrow::Cow,
    io::{Read, Seek, SeekFrom},
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Shape {
    Raw,
    #[default]
    Rows,
    Projection,
    Numeric,
    Frame,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderingMode {
    Ordered,
    Unordered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    FileBacked,
    StreamBacked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPlan {
    pub shape: Shape,
    pub ordering: OrderingMode,
    pub parallelism: usize,
    pub decode_policy: DecodePolicy,
    pub projection: Option<Vec<usize>>,
    pub skip_rows: u64,
    pub max_rows: Option<u64>,
    pub batch_rows: Option<usize>,
    pub source_kind: SourceKind,
}

pub enum QueryStream<'a, R: Read + Seek> {
    Rows(RowIterator<'a, R>),
    Projection(ProjectedRowIter<'a, R>),
    RowWindow(RowWindow<'a, R>),
    ProjectionWindow(ProjectedRowWindow<'a, R>),
}

impl<R: Read + Seek> QueryStream<'_, R> {
    /// Returns the next row as owned cell values.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails.
    pub fn try_next(&mut self) -> Result<Option<Vec<CellValue<'static>>>> {
        match self {
            Self::Rows(iterator) => iterator.try_next_owned(),
            Self::Projection(iterator) => iterator.try_next(),
            Self::RowWindow(window) => window
                .try_next()
                .map(|row| row.map(|row| row.into_iter().map(CellValue::into_owned).collect())),
            Self::ProjectionWindow(window) => window.try_next(),
        }
    }
}

impl<R: Read + Seek> Iterator for QueryStream<'_, R> {
    type Item = Result<Vec<CellValue<'static>>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.try_next() {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}

pub struct Query<'a, R: Read + Seek> {
    reader: &'a mut SasReader<R>,
    shape: Shape,
    ordering: Option<OrderingMode>,
    projection: Option<Vec<usize>>,
    decode_policy: DecodePolicy,
    parallelism: usize,
    skip_rows: u64,
    max_rows: Option<u64>,
    batch_rows: Option<usize>,
}

impl<'a, R: Read + Seek> Query<'a, R> {
    pub(crate) fn new(reader: &'a mut SasReader<R>) -> Self {
        Self {
            reader,
            shape: Shape::Rows,
            ordering: None,
            projection: None,
            decode_policy: DecodePolicy::default(),
            parallelism: 1,
            skip_rows: 0,
            max_rows: None,
            batch_rows: None,
        }
    }

    #[must_use]
    pub const fn shape(mut self, shape: Shape) -> Self {
        self.shape = shape;
        self
    }

    #[must_use]
    pub fn projection(mut self, indices: &[usize]) -> Self {
        self.projection = Some(indices.to_vec());
        self
    }

    /// Resolves and applies projection by column names.
    ///
    /// # Errors
    ///
    /// Returns an error if any name cannot be resolved.
    pub fn columns_by_name(mut self, names: &[&str]) -> Result<Self> {
        let selection = RowSelection::new().columns(names);
        let metadata = self.reader.metadata();
        let indices =
            selection
                .resolve_projection(metadata)?
                .ok_or_else(|| Error::InvalidConfiguration {
                    details: "column projection not specified".into(),
                })?;
        self.projection = Some(indices);
        Ok(self)
    }

    #[must_use]
    pub const fn decode(mut self, policy: DecodePolicy) -> Self {
        self.decode_policy = policy;
        self
    }

    #[must_use]
    pub fn parallel(mut self, parse_threads: usize) -> Self {
        self.parallelism = parse_threads.max(1);
        self
    }

    #[must_use]
    pub const fn ordering(mut self, mode: OrderingMode) -> Self {
        self.ordering = Some(mode);
        self
    }

    #[must_use]
    pub fn batch_rows(mut self, rows: usize) -> Self {
        self.batch_rows = Some(rows.max(1));
        self
    }

    #[must_use]
    pub const fn skip_rows(mut self, rows: u64) -> Self {
        self.skip_rows = rows;
        self
    }

    #[must_use]
    pub const fn max_rows(mut self, rows: u64) -> Self {
        self.max_rows = Some(rows);
        self
    }

    #[must_use]
    pub const fn window(mut self, skip_rows: u64, max_rows: u64) -> Self {
        self.skip_rows = skip_rows;
        self.max_rows = Some(max_rows);
        self
    }

    #[must_use]
    pub fn explain(&self) -> QueryPlan {
        QueryPlan {
            shape: self.shape,
            ordering: self.resolved_ordering(),
            parallelism: self.effective_parallelism(),
            decode_policy: self.resolved_decode_policy(),
            projection: self.projection.clone(),
            skip_rows: self.skip_rows,
            max_rows: self.max_rows,
            batch_rows: self.batch_rows,
            source_kind: self.source_kind(),
        }
    }

    /// Scans decoded rows in ordered mode and invokes the callback with owned values.
    ///
    /// # Errors
    ///
    /// Returns an error if query options are invalid, row decoding fails, or the callback errors.
    pub fn scan_ordered<F>(&mut self, f: F) -> Result<u64>
    where
        F: FnMut(&[CellValue<'static>]) -> Result<()>,
    {
        self.validate_batch_rows_compat("scan_ordered")?;
        if self.ordering == Some(OrderingMode::Unordered) {
            return Err(invalid_configuration(
                "scan_ordered cannot be used with OrderingMode::Unordered",
            ));
        }
        self.validate_decoded_shape("scan_ordered")?;
        let projection = self.resolved_projection_for_decoded()?;
        self.scan_decoded_ordered_internal(projection.as_deref(), self.resolved_decode_policy(), f)
    }

    /// Scans decoded rows in unordered mode and invokes the callback with borrowed values.
    ///
    /// # Errors
    ///
    /// Returns an error if query options are invalid, row decoding fails, or the callback errors.
    pub fn scan_unordered<F>(&mut self, f: F) -> Result<u64>
    where
        F: for<'row> Fn(&[CellValue<'row>]) -> Result<()> + Send + Sync,
    {
        self.validate_batch_rows_compat("scan_unordered")?;
        self.validate_window_ordering_compat("scan_unordered")?;
        if self.ordering == Some(OrderingMode::Ordered) {
            return Err(invalid_configuration(
                "scan_unordered cannot be used with OrderingMode::Ordered",
            ));
        }
        self.validate_decoded_shape("scan_unordered")?;
        let projection = self.resolved_projection_for_decoded()?;
        self.scan_decoded_unordered_internal(
            projection.as_deref(),
            self.resolved_decode_policy(),
            f,
        )
    }

    /// Scans numeric projection as `Option<f64>` values (missing => `None`).
    ///
    /// # Errors
    ///
    /// Returns an error if the shape is not numeric, projection is invalid,
    /// parallelism exceeds one, or scanning fails.
    pub fn scan_numeric<F>(&mut self, f: F) -> Result<u64>
    where
        F: FnMut(&[Option<f64>]) -> Result<()>,
    {
        self.validate_batch_rows_compat("scan_numeric")?;
        if self.shape != Shape::Numeric {
            return Err(invalid_configuration(
                "scan_numeric requires Shape::Numeric",
            ));
        }
        if self.ordering == Some(OrderingMode::Unordered) {
            return Err(invalid_configuration(
                "Shape::Numeric does not support OrderingMode::Unordered",
            ));
        }
        if self.effective_parallelism() > 1 {
            return Err(invalid_configuration(
                "Shape::Numeric only supports single-thread execution",
            ));
        }
        let projection = self.required_projection()?;
        self.reader.scan_numeric_columns(&projection, f)
    }

    /// Streams rows in order.
    ///
    /// # Errors
    ///
    /// Returns an error if query options are invalid or iterator creation fails.
    pub fn stream_ordered(&mut self) -> Result<QueryStream<'_, R>> {
        self.validate_batch_rows_compat("stream_ordered")?;
        if self.ordering == Some(OrderingMode::Unordered) {
            return Err(invalid_configuration(
                "stream_ordered cannot be used with OrderingMode::Unordered",
            ));
        }
        self.validate_decoded_shape("stream_ordered")?;
        let decode_policy = self.resolved_decode_policy();
        let projection = self.resolved_projection_for_decoded()?;
        if self.has_window() {
            let skip_rows = self.skip_rows;
            let max_rows = self.max_rows;
            if let Some(indices) = projection {
                let iterator = self
                    .reader
                    .select_columns_with_decode_policy(&indices, decode_policy)?;
                return Ok(QueryStream::ProjectionWindow(ProjectedRowWindow::new(
                    iterator, skip_rows, max_rows,
                )));
            }
            let iterator = self.ordered_row_iterator(decode_policy)?;
            return Ok(QueryStream::RowWindow(RowWindow::new(
                iterator, skip_rows, max_rows,
            )));
        }
        if let Some(indices) = projection {
            let iterator = self
                .reader
                .select_columns_with_decode_policy(&indices, decode_policy)?;
            Ok(QueryStream::Projection(iterator))
        } else {
            let iterator = self.ordered_row_iterator(decode_policy)?;
            Ok(QueryStream::Rows(iterator))
        }
    }

    /// Streams borrowed row views in order for zero-copy access.
    ///
    /// This endpoint avoids per-row owned allocation and is intended for
    /// high-throughput parser-to-memory ingestion paths.
    ///
    /// # Errors
    ///
    /// Returns an error if query options are invalid or iterator creation fails.
    pub fn stream_ordered_view(&mut self) -> Result<RowViewIter<'_, R>> {
        self.validate_batch_rows_compat("stream_ordered_view")?;
        if self.ordering == Some(OrderingMode::Unordered) {
            return Err(invalid_configuration(
                "stream_ordered_view cannot be used with OrderingMode::Unordered",
            ));
        }
        if self.has_window() {
            return Err(invalid_configuration(
                "stream_ordered_view does not support row windows",
            ));
        }
        self.validate_decoded_shape("stream_ordered_view")?;

        let decode_policy = self.resolved_decode_policy();
        let projection = self.resolved_projection_for_decoded()?;
        let lookup = Arc::new(RowLookup::from_metadata(self.reader.metadata()));
        let row_projection = if let Some(indices) = projection {
            let column_count = usize::try_from(self.reader.metadata().column_count).unwrap_or(0);
            Some(RowProjection::new(&indices, column_count))
        } else {
            None
        };
        let iterator = self.ordered_row_iterator(decode_policy)?;
        Ok(RowViewIter::new(iterator, lookup, row_projection))
    }

    /// Scans raw row bytes in ordered mode.
    ///
    /// # Errors
    ///
    /// Returns an error if query options are invalid, raw scan fails, or the callback errors.
    pub fn scan_raw_ordered<F>(&mut self, f: F) -> Result<u64>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        self.validate_batch_rows_compat("scan_raw_ordered")?;
        if self.ordering == Some(OrderingMode::Unordered) {
            return Err(invalid_configuration(
                "scan_raw_ordered cannot be used with OrderingMode::Unordered",
            ));
        }
        self.validate_raw_shape("scan_raw_ordered")?;
        self.scan_raw_ordered_internal(f)
    }

    /// Scans raw row bytes in unordered mode and returns row/byte counters.
    ///
    /// # Errors
    ///
    /// Returns an error if query options are invalid, raw scan fails, or the callback errors.
    pub fn scan_raw_unordered<F>(&mut self, f: F) -> Result<RawScanStats>
    where
        F: Fn(&[u8]) -> Result<()> + Send + Sync,
    {
        self.validate_batch_rows_compat("scan_raw_unordered")?;
        self.validate_window_ordering_compat("scan_raw_unordered")?;
        if self.ordering == Some(OrderingMode::Ordered) {
            return Err(invalid_configuration(
                "scan_raw_unordered cannot be used with OrderingMode::Ordered",
            ));
        }
        self.validate_raw_shape("scan_raw_unordered")?;
        self.scan_raw_unordered_internal(f)
    }

    /// Collects raw row bytes into contiguous owned batches.
    ///
    /// The ordering follows the configured ordering mode. Unordered mode is
    /// throughput-oriented and may return batches in non-row order.
    ///
    /// # Errors
    ///
    /// Returns an error if query options are invalid or raw scanning fails.
    pub fn collect_raw_batches(&mut self, batch_rows: usize) -> Result<Vec<RawRowBatch>> {
        if batch_rows == 0 {
            return Err(invalid_configuration(
                "collect_raw_batches requires batch_rows > 0",
            ));
        }
        self.validate_window_ordering_compat("collect_raw_batches")?;
        self.validate_raw_shape("collect_raw_batches")?;
        let mut batches = Vec::new();

        if self.resolved_ordering() == OrderingMode::Ordered {
            let row_length = usize::try_from(self.reader.layout.row_info.row_length).unwrap_or(0);
            let mut current =
                RawRowBatch::with_capacity(batch_rows, row_length.saturating_mul(batch_rows));
            self.scan_raw_ordered_internal(|row| {
                current.push_row(row);
                if current.row_count() >= batch_rows {
                    let mut next = RawRowBatch::with_capacity(
                        batch_rows,
                        row_length.saturating_mul(batch_rows),
                    );
                    std::mem::swap(&mut current, &mut next);
                    batches.push(next);
                }
                Ok(())
            })?;
            if !current.is_empty() {
                batches.push(current);
            }
            return Ok(batches);
        }

        let parse_threads = self.effective_parallelism_with_warning("collect raw batches");
        if parse_threads > 1
            && let Some(path) = self.reader.source_path.as_deref()
        {
            let unordered_batches: Mutex<Vec<RawRowBatch>> = Mutex::new(Vec::new());
            scan_file_raw_rows_unordered_batched_with_stats(
                path,
                &self.reader.layout,
                parallel_scan_config(parse_threads),
                batch_rows,
                |batch| {
                    unordered_batches
                        .lock()
                        .map_err(|_| Error::InvalidConfiguration {
                            details: "raw batch accumulator mutex poisoned".into(),
                        })?
                        .push(batch.clone());
                    Ok(())
                },
            )?;
            return unordered_batches
                .into_inner()
                .map_err(|_| Error::InvalidConfiguration {
                    details: "raw batch accumulator mutex poisoned".into(),
                });
        }

        self.reader.ensure_missing_policies_fresh()?;
        self.reader.reader.seek(SeekFrom::Start(0))?;
        let mut iterator = self.reader.layout.row_iterator(&mut self.reader.reader)?;

        let row_length = usize::try_from(self.reader.layout.row_info.row_length).unwrap_or(0);
        let mut current =
            RawRowBatch::with_capacity(batch_rows, row_length.saturating_mul(batch_rows));
        let result = (|| -> Result<()> {
            while let Some(row) = iterator.try_next_raw_row()? {
                current.push_row(row);
                if current.row_count() >= batch_rows {
                    let mut next = RawRowBatch::with_capacity(
                        batch_rows,
                        row_length.saturating_mul(batch_rows),
                    );
                    std::mem::swap(&mut current, &mut next);
                    batches.push(next);
                }
            }
            Ok(())
        })();
        self.reader.reader.seek(SeekFrom::Start(0))?;
        result?;
        if !current.is_empty() {
            batches.push(current);
        }
        Ok(batches)
    }

    /// Collects all rows into a single frame batch.
    ///
    /// # Errors
    ///
    /// Returns an error if query options are invalid or frame materialization fails.
    pub fn collect_frame(&mut self) -> Result<FrameBatch> {
        self.validate_batch_rows_compat("collect_frame")?;
        if self.ordering == Some(OrderingMode::Unordered) {
            return Err(invalid_configuration(
                "collect_frame cannot be used with OrderingMode::Unordered",
            ));
        }
        if self.shape != Shape::Frame {
            return Err(invalid_configuration("collect_frame requires Shape::Frame"));
        }

        let decode_policy = self.resolved_decode_policy();
        let projection = self.frame_projection()?;
        let blueprint = FrameBlueprint::from_metadata(self.reader.metadata(), &projection)?;
        let use_projection = self.projection.is_some();
        let expected_rows = usize::try_from(self.reader.metadata().row_count).unwrap_or(0);

        let mut collector = blueprint.collector_with_capacity(expected_rows);
        if use_projection {
            self.scan_decoded_ordered_internal(Some(&projection), decode_policy, |row| {
                collector.push_row(row)
            })?;
        } else {
            self.scan_decoded_ordered_internal(None, decode_policy, |row| collector.push_row(row))?;
        }
        Ok(collector.finish())
    }

    /// Collects rows into frame batches with at most `batch_rows` rows each.
    ///
    /// # Errors
    ///
    /// Returns an error if query options are invalid or frame materialization fails.
    pub fn collect_frame_batches(&mut self, batch_rows: usize) -> Result<Vec<FrameBatch>> {
        self.validate_window_ordering_compat("collect_frame_batches")?;
        if self.ordering == Some(OrderingMode::Unordered) {
            return Err(invalid_configuration(
                "collect_frame_batches cannot be used with OrderingMode::Unordered",
            ));
        }
        if self.shape != Shape::Frame {
            return Err(invalid_configuration(
                "collect_frame_batches requires Shape::Frame",
            ));
        }
        if batch_rows == 0 {
            return Err(invalid_configuration(
                "batch_rows must be greater than zero",
            ));
        }

        let decode_policy = self.resolved_decode_policy();
        let projection = self.frame_projection()?;
        let use_projection = self.projection.is_some();
        let blueprint = FrameBlueprint::from_metadata(self.reader.metadata(), &projection)?;
        let total_rows = usize::try_from(self.reader.metadata().row_count).unwrap_or(0);

        let mut batches = if total_rows > 0 {
            Vec::with_capacity(total_rows.div_ceil(batch_rows))
        } else {
            Vec::new()
        };
        let mut collector = blueprint.collector_with_capacity(batch_rows);
        let mut push_row = |row: &[CellValue<'static>]| -> Result<()> {
            collector.push_row(row)?;
            if collector.row_count() >= batch_rows {
                let next = blueprint.collector_with_capacity(batch_rows);
                let finished = std::mem::replace(&mut collector, next).finish();
                batches.push(finished);
            }
            Ok(())
        };

        if use_projection {
            self.scan_decoded_ordered_internal(Some(&projection), decode_policy, &mut push_row)?;
        } else {
            self.scan_decoded_ordered_internal(None, decode_policy, &mut push_row)?;
        }

        if collector.row_count() > 0 {
            batches.push(collector.finish());
        }

        Ok(batches)
    }

    fn validate_decoded_shape(&self, method: &str) -> Result<()> {
        match self.shape {
            Shape::Rows | Shape::Projection => Ok(()),
            Shape::Raw => Err(invalid_configuration(&format!(
                "{method} does not support Shape::Raw"
            ))),
            Shape::Numeric => Err(invalid_configuration(&format!(
                "{method} does not support Shape::Numeric; use scan_numeric"
            ))),
            Shape::Frame => Err(invalid_configuration(&format!(
                "{method} does not support Shape::Frame; use collect_frame"
            ))),
        }
    }

    fn validate_raw_shape(&self, method: &str) -> Result<()> {
        if self.shape != Shape::Raw {
            return Err(invalid_configuration(&format!(
                "{method} requires Shape::Raw"
            )));
        }
        Ok(())
    }

    fn validate_batch_rows_compat(&self, method: &str) -> Result<()> {
        if self.batch_rows.is_some() && !matches!(self.shape, Shape::Raw | Shape::Frame) {
            return Err(invalid_configuration(&format!(
                "{method} does not support batch_rows for shape {:?}",
                self.shape
            )));
        }
        Ok(())
    }

    fn validate_window_ordering_compat(&self, method: &str) -> Result<()> {
        if self.has_window() && self.resolved_ordering() == OrderingMode::Unordered {
            return Err(invalid_configuration(&format!(
                "{method} does not support row window with OrderingMode::Unordered"
            )));
        }
        Ok(())
    }

    const fn source_kind(&self) -> SourceKind {
        if self.reader.source_path.is_some() {
            SourceKind::FileBacked
        } else {
            SourceKind::StreamBacked
        }
    }

    fn resolved_ordering(&self) -> OrderingMode {
        self.ordering.unwrap_or(match self.shape {
            Shape::Frame | Shape::Numeric => OrderingMode::Ordered,
            Shape::Raw | Shape::Rows | Shape::Projection => OrderingMode::Unordered,
        })
    }

    fn resolved_decode_policy(&self) -> DecodePolicy {
        if self.shape == Shape::Numeric {
            DecodePolicy::FAST_SCAN
        } else {
            self.decode_policy
        }
    }

    const fn effective_parallelism(&self) -> usize {
        if self.parallelism <= 1 {
            return 1;
        }
        if self.reader.source_path.is_some() {
            self.parallelism
        } else {
            1
        }
    }

    fn effective_parallelism_with_warning(&self, action: &str) -> usize {
        let effective = self.effective_parallelism();
        if self.parallelism > 1 && effective == 1 {
            log_warn(&format!(
                "{action}: parallelism downgraded to 1 for non-file-backed reader"
            ));
        }
        effective
    }

    fn required_projection(&self) -> Result<Vec<usize>> {
        let Some(indices) = self.projection.as_ref() else {
            return Err(invalid_configuration(
                "projection is required for this query shape",
            ));
        };
        self.reader.normalize_projection(indices)
    }

    fn resolved_projection_for_decoded(&self) -> Result<Option<Vec<usize>>> {
        match self.shape {
            Shape::Projection => self.required_projection().map(Some),
            _ => Ok(None),
        }
    }

    fn frame_projection(&self) -> Result<Vec<usize>> {
        if let Some(indices) = self.projection.as_ref() {
            return self.reader.normalize_projection(indices);
        }
        let column_count = usize::try_from(self.reader.metadata().column_count).unwrap_or(0);
        Ok((0..column_count).collect())
    }

    fn ordered_row_iterator(&mut self, decode_policy: DecodePolicy) -> Result<RowIterator<'_, R>> {
        self.reader.ensure_missing_policies_fresh()?;
        self.reader.reader.seek(SeekFrom::Start(0))?;
        let mut iterator = self.reader.layout.row_iterator(&mut self.reader.reader)?;
        iterator.set_decode_policy(decode_policy);
        Ok(iterator)
    }

    const fn has_window(&self) -> bool {
        self.skip_rows > 0 || self.max_rows.is_some()
    }

    fn scan_decoded_ordered_internal<F>(
        &mut self,
        projection: Option<&[usize]>,
        decode_policy: DecodePolicy,
        mut f: F,
    ) -> Result<u64>
    where
        F: FnMut(&[CellValue<'static>]) -> Result<()>,
    {
        let mut window = QueryWindowState::new(self.skip_rows, self.max_rows);
        let parse_threads = self.effective_parallelism_with_warning("ordered scan");
        if parse_threads > 1
            && let Some(path) = self.reader.source_path.as_deref()
        {
            let config = parallel_scan_config(parse_threads);
            let mut rows = 0u64;
            let mut visit = |row: &[CellValue<'static>]| -> Result<()> {
                if !window.include_row() {
                    return Ok(());
                }
                f(row)?;
                rows = rows.saturating_add(1);
                Ok(())
            };
            if let Some(indices) = projection {
                scan_file_projected_rows_with_decode_policy(
                    path,
                    &self.reader.layout,
                    indices,
                    decode_policy,
                    config,
                    projection_legacy_mode(),
                    &mut visit,
                )?;
                return Ok(rows);
            }
            scan_file_rows_with_decode_policy(
                path,
                &self.reader.layout,
                decode_policy,
                config,
                &mut visit,
            )?;
            return Ok(rows);
        }

        self.reader.ensure_missing_policies_fresh()?;
        self.reader.reader.seek(SeekFrom::Start(0))?;
        let mut iterator = self.reader.layout.row_iterator(&mut self.reader.reader)?;
        iterator.set_decode_policy(decode_policy);

        let result = (|| -> Result<u64> {
            let mut rows = 0u64;
            if let Some(indices) = projection {
                while let Some(row) = iterator.try_next_projected(indices)? {
                    if !window.include_row() {
                        if window.exhausted() {
                            break;
                        }
                        continue;
                    }
                    let owned: Vec<CellValue<'static>> =
                        row.into_iter().map(CellValue::into_owned).collect();
                    f(&owned)?;
                    rows = rows.saturating_add(1);
                }
            } else {
                while let Some(row) = iterator.try_next_owned()? {
                    if !window.include_row() {
                        if window.exhausted() {
                            break;
                        }
                        continue;
                    }
                    f(&row)?;
                    rows = rows.saturating_add(1);
                }
            }
            Ok(rows)
        })();

        self.reader.reader.seek(SeekFrom::Start(0))?;
        result
    }

    fn scan_decoded_unordered_internal<F>(
        &mut self,
        projection: Option<&[usize]>,
        decode_policy: DecodePolicy,
        f: F,
    ) -> Result<u64>
    where
        F: for<'row> Fn(&[CellValue<'row>]) -> Result<()> + Send + Sync,
    {
        let parse_threads = self.effective_parallelism_with_warning("unordered scan");
        if parse_threads > 1
            && let Some(path) = self.reader.source_path.as_deref()
        {
            let config = parallel_scan_config(parse_threads);
            if let Some(indices) = projection {
                return scan_file_projected_rows_with_decode_policy_unordered(
                    path,
                    &self.reader.layout,
                    indices,
                    decode_policy,
                    config,
                    projection_legacy_mode(),
                    f,
                );
            }
            return scan_file_rows_with_decode_policy_unordered(
                path,
                &self.reader.layout,
                decode_policy,
                config,
                f,
            );
        }

        self.reader.ensure_missing_policies_fresh()?;
        self.reader.reader.seek(SeekFrom::Start(0))?;
        let mut iterator = self.reader.layout.row_iterator(&mut self.reader.reader)?;
        iterator.set_decode_policy(decode_policy);

        let result = (|| -> Result<u64> {
            let mut rows = 0u64;
            if let Some(indices) = projection {
                while let Some(row) = iterator.try_next_projected(indices)? {
                    f(&row)?;
                    rows = rows.saturating_add(1);
                }
            } else {
                while let Some(row) = iterator.try_next()? {
                    f(&row)?;
                    rows = rows.saturating_add(1);
                }
            }
            Ok(rows)
        })();

        self.reader.reader.seek(SeekFrom::Start(0))?;
        result
    }

    fn scan_raw_ordered_internal<F>(&mut self, mut f: F) -> Result<u64>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        let mut window = QueryWindowState::new(self.skip_rows, self.max_rows);
        let parse_threads = self.effective_parallelism_with_warning("ordered raw scan");
        if parse_threads > 1
            && let Some(path) = self.reader.source_path.as_deref()
        {
            let mut rows = 0u64;
            let mut visit = |row: &[u8]| -> Result<()> {
                if !window.include_row() {
                    return Ok(());
                }
                f(row)?;
                rows = rows.saturating_add(1);
                Ok(())
            };
            scan_file_raw_rows(
                path,
                &self.reader.layout,
                parallel_scan_config(parse_threads),
                &mut visit,
            )?;
            return Ok(rows);
        }

        self.reader.ensure_missing_policies_fresh()?;
        self.reader.reader.seek(SeekFrom::Start(0))?;
        let mut iterator = self.reader.layout.row_iterator(&mut self.reader.reader)?;

        let result = (|| -> Result<u64> {
            let mut rows = 0u64;
            while iterator
                .try_next_raw_row_visit(&mut |row| {
                    if !window.include_row() {
                        return Ok(());
                    }
                    f(row)?;
                    rows = rows.saturating_add(1);
                    Ok(())
                })?
                .is_some()
            {
                if window.exhausted() {
                    break;
                }
            }
            Ok(rows)
        })();

        self.reader.reader.seek(SeekFrom::Start(0))?;
        result
    }

    fn scan_raw_unordered_internal<F>(&mut self, f: F) -> Result<RawScanStats>
    where
        F: Fn(&[u8]) -> Result<()> + Send + Sync,
    {
        let parse_threads = self.effective_parallelism_with_warning("unordered raw scan");
        if parse_threads > 1
            && let Some(path) = self.reader.source_path.as_deref()
        {
            let config = parallel_scan_config(parse_threads);
            if let Some(batch_rows) = self.batch_rows.filter(|rows| *rows > 1) {
                return scan_file_raw_rows_unordered_batched_with_stats(
                    path,
                    &self.reader.layout,
                    config,
                    batch_rows,
                    |batch| {
                        for row in batch.rows() {
                            f(row)?;
                        }
                        Ok(())
                    },
                );
            }
            return scan_file_raw_rows_unordered_with_stats(path, &self.reader.layout, config, f);
        }

        self.reader.ensure_missing_policies_fresh()?;
        self.reader.reader.seek(SeekFrom::Start(0))?;
        let mut iterator = self.reader.layout.row_iterator(&mut self.reader.reader)?;

        let result = (|| -> Result<RawScanStats> {
            let mut stats = RawScanStats::default();
            while iterator
                .try_next_raw_row_visit(&mut |row| {
                    f(row)?;
                    stats.rows = stats.rows.saturating_add(1);
                    stats.raw_bytes = stats.raw_bytes.saturating_add(row.len() as u64);
                    Ok(())
                })?
                .is_some()
            {}
            Ok(stats)
        })();

        self.reader.reader.seek(SeekFrom::Start(0))?;
        result
    }
}

#[derive(Debug, Clone, Copy)]
struct QueryWindowState {
    skip_remaining: u64,
    remaining: Option<u64>,
}

impl QueryWindowState {
    const fn new(skip_rows: u64, max_rows: Option<u64>) -> Self {
        Self {
            skip_remaining: skip_rows,
            remaining: max_rows,
        }
    }

    const fn include_row(&mut self) -> bool {
        if self.skip_remaining > 0 {
            self.skip_remaining = self.skip_remaining.saturating_sub(1);
            return false;
        }

        if let Some(remaining) = self.remaining.as_mut() {
            if *remaining == 0 {
                return false;
            }
            *remaining = remaining.saturating_sub(1);
        }
        true
    }

    const fn exhausted(&self) -> bool {
        matches!(self.remaining, Some(0))
    }
}

fn invalid_configuration(details: &str) -> Error {
    Error::InvalidConfiguration {
        details: Cow::Owned(details.to_string()),
    }
}

fn projection_legacy_mode() -> bool {
    std::env::var("SAS7BDAT_PROJECTION_DECODE_PLAN")
        .map(|value| !value.eq_ignore_ascii_case("compiled"))
        .unwrap_or(false)
}

fn parallel_scan_config(parse_threads: usize) -> ParallelScanConfig {
    let mut config = ParallelScanConfig::new(parse_threads.max(1));
    if let Ok(raw) = std::env::var("SAS7BDAT_PARSE_PAGE_CHUNK")
        && let Ok(value) = raw.parse::<u64>()
        && value > 0
    {
        config.page_chunk = value;
    }
    if let Ok(raw) = std::env::var("SAS7BDAT_PARSE_ROW_BATCH_SIZE")
        && let Ok(value) = raw.parse::<usize>()
        && value > 0
    {
        config.row_batch_size = value;
    }
    config
}
