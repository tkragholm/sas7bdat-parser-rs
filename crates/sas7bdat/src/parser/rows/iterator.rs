use super::{
    batch::{AdaptiveBatchState, ColumnarBatchMode, next_columnar_batch_with_mode},
    buffer::RowData,
    decode::{
        DecodePolicy, NumericCell, TemporalDecodePolicy, decode_numeric_cell,
        decode_string_with_policy, decode_value_inner, numeric_value_from_width,
        sas_days_to_datetime, sas_seconds_to_datetime, sas_seconds_to_time,
    },
    runtime_column::{
        CompiledDecodeKind, CompiledRuntimeColumnRef, NumericRuntimeColumnRef, RuntimeColumn,
        RuntimeColumnRef,
    },
    streaming::StreamingRow,
};
use crate::{
    cell::CellValue,
    dataset::Compression,
    error::{Error, Result, Section},
    parser::{
        core::encoding::resolve_encoding,
        metadata::{ColumnKind, DatasetLayout, NumericKind},
    },
};
use encoding_rs::Encoding;
use smallvec::SmallVec;
use std::{
    borrow::Cow,
    cell::Cell,
    convert::TryFrom,
    io::{Read, Seek},
    ops::Deref,
};

#[derive(Clone, Copy)]
struct RowProgress {
    row_index: u16,
    prev_row_in_page: u16,
    prev_emitted: u64,
}

#[derive(Clone, Copy)]
enum NumericProjectionKind {
    Double,
    DateRaw,
    DateTemporal,
    DateTimeRaw,
    DateTimeTemporal,
    TimeRaw,
    TimeTemporal,
}

fn projection_decode_legacy_enabled() -> bool {
    std::env::var("SAS7BDAT_PROJECTION_DECODE_PLAN")
        .map(|value| !value.eq_ignore_ascii_case("compiled"))
        .unwrap_or(true)
}

fn fullrow_decode_compiled_enabled() -> bool {
    std::env::var("SAS7BDAT_FULLROW_DECODE_PLAN")
        .map(|value| !value.eq_ignore_ascii_case("legacy"))
        .unwrap_or(true)
}

pub struct RowIteratorCore<R, L>
where
    R: Read + Seek,
    L: Deref<Target = DatasetLayout>,
{
    pub(crate) reader: R,
    pub(crate) layout: L,
    pub(crate) runtime_columns: Vec<RuntimeColumn>,
    pub(crate) columnar_columns: Vec<RuntimeColumnRef>,
    pub(crate) page_buffer: Vec<u8>,
    pub(crate) current_rows: Vec<RowData>,
    pub(crate) contiguous_base: Option<usize>,
    pub(crate) contiguous_rows: u16,
    pub(crate) reusable_row_buffers: Vec<Vec<u8>>,
    pub(crate) reusable_row_buffer: Vec<u8>,
    pub(crate) columnar_owned_buffer: Vec<u8>,
    pub(crate) page_row_count: Cell<u16>,
    pub(crate) row_in_page: Cell<u16>,
    pub(crate) next_page_index: u64,
    pub(crate) page_range_end: u64,
    pub(crate) emitted_rows: Cell<u64>,
    pub(crate) encoding: &'static Encoding,
    pub(crate) exhausted: Cell<bool>,
    pub(crate) columns_fit_row: bool,
    pub(crate) decode_policy: DecodePolicy,
    pub(crate) projection_decode_legacy: bool,
    pub(crate) fullrow_decode_compiled: bool,
    pub(crate) compiled_fullrow_columns: Vec<CompiledRuntimeColumnRef>,
    pub(crate) adaptive_batch_state: AdaptiveBatchState,
    pub(crate) last_batch_mode: ColumnarBatchMode,
    pub(crate) row_length: usize,
    pub(crate) total_rows: u64,
}

pub type RowIterator<'a, R> = RowIteratorCore<&'a mut R, &'a DatasetLayout>;
pub type OwnedRowIterator<R> = RowIteratorCore<R, Box<DatasetLayout>>;

/// Creates a [`RowIterator`] for the provided reader and layout metadata.
///
/// # Errors
///
/// Returns an error if the iterator cannot be constructed, for example when
/// the dataset uses unsupported compression.
pub fn row_iterator<'a, R: Read + Seek>(
    reader: &'a mut R,
    layout: &'a DatasetLayout,
) -> Result<RowIterator<'a, R>> {
    RowIteratorCore::new(reader, layout)
}

impl<R, L> RowIteratorCore<R, L>
where
    R: Read + Seek,
    L: Deref<Target = DatasetLayout>,
{
    /// Constructs a new row iterator for the provided reader and metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the dataset uses an unsupported compression mode
    /// or the page size cannot be represented on this platform.
    pub fn new(reader: R, layout: L) -> Result<Self> {
        match layout.row_info.compression {
            Compression::None | Compression::Row | Compression::Binary => {}
            Compression::Unknown(code) => {
                return Err(Error::Unsupported {
                    feature: Cow::from(format!(
                        "row iteration for unsupported {code:?} compression"
                    )),
                });
            }
        }

        let encoding = resolve_encoding(layout.header.metadata.file_encoding.as_deref());
        let page_size =
            usize::try_from(layout.header.page_size).map_err(|_| Error::Unsupported {
                feature: Cow::from("page size exceeds platform pointer width"),
            })?;
        let row_length =
            usize::try_from(layout.row_info.row_length).map_err(|_| Error::Unsupported {
                feature: Cow::from("row length exceeds platform pointer width"),
            })?;
        if layout.columns.is_empty() || row_length == 0 {
            return Err(Error::InvalidMetadata {
                details: Cow::from("dataset defines zero columns or row length is zero"),
            });
        }
        let runtime_columns = layout
            .columns
            .iter()
            .map(|column| {
                let offset =
                    usize::try_from(column.offsets.offset).map_err(|_| Error::Unsupported {
                        feature: Cow::from("column offset exceeds platform pointer width"),
                    })?;
                let width =
                    usize::try_from(column.offsets.width).map_err(|_| Error::Unsupported {
                        feature: Cow::from("column width exceeds platform pointer width"),
                    })?;
                let end = offset.saturating_add(width);
                Ok(RuntimeColumn {
                    index: column.index,
                    offset,
                    width,
                    end,
                    raw_width: column.offsets.width,
                    kind: column.kind,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let columnar_columns: Vec<RuntimeColumnRef> =
            runtime_columns.iter().map(RuntimeColumn::as_ref).collect();
        let columns_fit_row = runtime_columns
            .iter()
            .all(|column| column.end <= row_length);

        let total_rows = layout.row_info.total_rows;
        let page_count = layout.header.page_count;
        let decode_policy = DecodePolicy::default();
        let projection_decode_legacy = projection_decode_legacy_enabled();
        let fullrow_decode_compiled = fullrow_decode_compiled_enabled();
        let compiled_fullrow_columns = runtime_columns
            .iter()
            .map(|column| {
                Self::compile_runtime_column_for_policy(
                    column.as_ref(),
                    decode_policy,
                    fullrow_decode_compiled,
                )
            })
            .collect();
        Ok(Self {
            reader,
            layout,
            runtime_columns,
            columnar_columns,
            page_buffer: vec![0u8; page_size],
            current_rows: Vec::new(),
            contiguous_base: None,
            contiguous_rows: 0,
            reusable_row_buffers: Vec::new(),
            reusable_row_buffer: Vec::new(),
            columnar_owned_buffer: Vec::new(),
            page_row_count: Cell::new(0),
            row_in_page: Cell::new(0),
            next_page_index: 0,
            page_range_end: page_count,
            emitted_rows: Cell::new(0),
            encoding,
            exhausted: Cell::new(false),
            columns_fit_row,
            decode_policy,
            projection_decode_legacy,
            fullrow_decode_compiled,
            compiled_fullrow_columns,
            adaptive_batch_state: AdaptiveBatchState::default(),
            last_batch_mode: ColumnarBatchMode::Segmented,
            row_length,
            total_rows,
        })
    }

    #[inline]
    pub(crate) fn ensure_page_ready(&mut self) -> Result<bool> {
        if self.row_in_page.get() >= self.page_row_count.get() {
            if let Err(err) = self.fetch_next_page() {
                self.exhausted.set(true);
                return Err(err);
            }
            if self.page_row_count.get() == 0 {
                self.exhausted.set(true);
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[inline]
    pub(crate) fn revert_row_progress(&self, prev_row_in_page: u16, prev_emitted: u64) {
        self.row_in_page.set(prev_row_in_page);
        self.emitted_rows.set(prev_emitted);
        self.exhausted.set(true);
    }

    #[inline]
    fn reserve_next_row(&mut self) -> Result<Option<RowProgress>> {
        if self.exhausted.get() {
            return Ok(None);
        }
        if self.emitted_rows.get() >= self.total_rows {
            self.exhausted.set(true);
            return Ok(None);
        }

        if !self.ensure_page_ready()? {
            return Ok(None);
        }

        let row_index = self.row_in_page.get();
        let prev_row_in_page = row_index;
        let prev_emitted = self.emitted_rows.get();
        self.row_in_page.set(row_index.saturating_add(1));
        self.emitted_rows.set(prev_emitted.saturating_add(1));

        Ok(Some(RowProgress {
            row_index,
            prev_row_in_page,
            prev_emitted,
        }))
    }

    /// Advances the iterator by one row.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails.
    pub fn try_next(&mut self) -> Result<Option<Vec<CellValue<'_>>>> {
        let Some(progress) = self.reserve_next_row()? else {
            return Ok(None);
        };

        match self.decode_row(progress.row_index) {
            Ok(row) => Ok(Some(row)),
            Err(err) => {
                self.revert_row_progress(progress.prev_row_in_page, progress.prev_emitted);
                Err(err)
            }
        }
    }

    /// Advances the iterator by one row and returns an owned row.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails.
    pub fn try_next_owned(&mut self) -> Result<Option<Vec<CellValue<'static>>>> {
        let Some(progress) = self.reserve_next_row()? else {
            return Ok(None);
        };

        match self.decode_row_owned(progress.row_index) {
            Ok(row) => Ok(Some(row)),
            Err(err) => {
                self.revert_row_progress(progress.prev_row_in_page, progress.prev_emitted);
                Err(err)
            }
        }
    }

    /// Advances the iterator by one row and returns the raw row bytes without decoding.
    ///
    /// The returned slice borrows from internal buffers and is only valid until
    /// the iterator advances.
    ///
    /// # Errors
    ///
    /// Returns an error if row extraction fails.
    pub fn try_next_raw_row(&mut self) -> Result<Option<&[u8]>> {
        let Some(progress) = self.reserve_next_row()? else {
            return Ok(None);
        };

        match self.row_slice(progress.row_index) {
            Ok(row) => Ok(Some(row)),
            Err(err) => {
                self.revert_row_progress(progress.prev_row_in_page, progress.prev_emitted);
                Err(err)
            }
        }
    }

    /// Advances the iterator and invokes the visitor with the raw row bytes.
    ///
    /// Returns `Ok(None)` when no more rows remain or `Ok(Some(()))` when a row
    /// was processed successfully.
    ///
    /// # Errors
    ///
    /// Propagates row extraction failures from the iterator or errors returned by `f`.
    pub fn try_next_raw_row_visit<F>(&mut self, f: &mut F) -> Result<Option<()>>
    where
        F: for<'row> FnMut(&'row [u8]) -> Result<()>,
    {
        let Some(progress) = self.reserve_next_row()? else {
            return Ok(None);
        };

        let row_data = match self.row_slice(progress.row_index) {
            Ok(row) => row,
            Err(err) => {
                self.revert_row_progress(progress.prev_row_in_page, progress.prev_emitted);
                return Err(err);
            }
        };

        if let Err(err) = f(row_data) {
            self.revert_row_progress(progress.prev_row_in_page, progress.prev_emitted);
            return Err(err);
        }
        Ok(Some(()))
    }

    /// Advances the iterator by one row and decodes only the requested columns.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails or any requested column index is invalid.
    pub fn try_next_projected(&mut self, indices: &[usize]) -> Result<Option<Vec<CellValue<'_>>>> {
        let Some(progress) = self.reserve_next_row()? else {
            return Ok(None);
        };

        match self.decode_projected_row(progress.row_index, indices) {
            Ok(row) => Ok(Some(row)),
            Err(err) => {
                self.revert_row_progress(progress.prev_row_in_page, progress.prev_emitted);
                Err(err)
            }
        }
    }

    /// Advances the iterator by one row and decodes a prevalidated projection plan.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails.
    pub fn try_next_projected_columns(
        &mut self,
        columns: &[RuntimeColumnRef],
    ) -> Result<Option<Vec<CellValue<'_>>>> {
        let compiled_columns = self.compile_projection_columns(columns);
        self.try_next_projected_compiled_columns(&compiled_columns)
    }

    /// Advances the iterator by one row and decodes a precompiled projection plan.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails.
    pub(crate) fn try_next_projected_compiled_columns(
        &mut self,
        columns: &[CompiledRuntimeColumnRef],
    ) -> Result<Option<Vec<CellValue<'_>>>> {
        let Some(progress) = self.reserve_next_row()? else {
            return Ok(None);
        };

        match self.decode_projected_row_compiled_columns(progress.row_index, columns) {
            Ok(row) => Ok(Some(row)),
            Err(err) => {
                self.revert_row_progress(progress.prev_row_in_page, progress.prev_emitted);
                Err(err)
            }
        }
    }

    /// Advances the iterator by one row and decodes a prevalidated projection plan into owned values.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails.
    pub fn try_next_projected_columns_owned(
        &mut self,
        columns: &[RuntimeColumnRef],
    ) -> Result<Option<Vec<CellValue<'static>>>> {
        let compiled_columns = self.compile_projection_columns(columns);
        self.try_next_projected_compiled_columns_owned(&compiled_columns)
    }

    /// Advances the iterator by one row and decodes a precompiled projection plan into owned values.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails.
    pub(crate) fn try_next_projected_compiled_columns_owned(
        &mut self,
        columns: &[CompiledRuntimeColumnRef],
    ) -> Result<Option<Vec<CellValue<'static>>>> {
        let Some(progress) = self.reserve_next_row()? else {
            return Ok(None);
        };

        match self.decode_projected_row_compiled_columns_owned(progress.row_index, columns) {
            Ok(row) => Ok(Some(row)),
            Err(err) => {
                self.revert_row_progress(progress.prev_row_in_page, progress.prev_emitted);
                Err(err)
            }
        }
    }

    /// Advances the iterator by one row and visits projected decoded values without owned conversion.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails or the visitor returns an error.
    pub fn try_next_projected_columns_visit<F>(
        &mut self,
        columns: &[RuntimeColumnRef],
        f: &mut F,
    ) -> Result<Option<()>>
    where
        F: for<'row> FnMut(&[CellValue<'row>]) -> Result<()>,
    {
        let compiled_columns = self.compile_projection_columns(columns);
        self.try_next_projected_compiled_columns_visit(&compiled_columns, f)
    }

    /// Advances the iterator by one row and visits projected decoded values using a precompiled plan.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails or the visitor returns an error.
    pub(crate) fn try_next_projected_compiled_columns_visit<F>(
        &mut self,
        columns: &[CompiledRuntimeColumnRef],
        f: &mut F,
    ) -> Result<Option<()>>
    where
        F: for<'row> FnMut(&[CellValue<'row>]) -> Result<()>,
    {
        let Some(progress) = self.reserve_next_row()? else {
            return Ok(None);
        };

        let row_data = match self.row_slice(progress.row_index) {
            Ok(data) => data,
            Err(err) => {
                self.revert_row_progress(progress.prev_row_in_page, progress.prev_emitted);
                return Err(err);
            }
        };
        let values = match self.decode_compiled_columns_from_row_buffer(row_data, columns) {
            Ok(values) => values,
            Err(err) => {
                self.revert_row_progress(progress.prev_row_in_page, progress.prev_emitted);
                return Err(err);
            }
        };
        if let Err(err) = f(values.as_slice()) {
            self.revert_row_progress(progress.prev_row_in_page, progress.prev_emitted);
            return Err(err);
        }
        Ok(Some(()))
    }

    /// Advances the iterator by one row and decodes projected numeric columns into `values`.
    ///
    /// Missing values are represented as `None`; present numeric values are returned as raw SAS
    /// numerics (`f64`) without temporal conversion.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails or any projected column is non-numeric.
    pub fn try_next_numeric_projected(
        &mut self,
        columns: &[RuntimeColumnRef],
        values: &mut Vec<Option<f64>>,
    ) -> Result<Option<()>> {
        let numeric_columns = Self::compile_numeric_columns(columns)?;
        self.try_next_numeric_projected_columns(&numeric_columns, values)
    }

    /// Advances the iterator by one row and decodes prevalidated projected numeric columns.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails.
    pub(crate) fn try_next_numeric_projected_columns(
        &mut self,
        columns: &[NumericRuntimeColumnRef],
        values: &mut Vec<Option<f64>>,
    ) -> Result<Option<()>> {
        let Some(progress) = self.reserve_next_row()? else {
            return Ok(None);
        };

        match self.decode_numeric_projected_row_into(progress.row_index, columns, values) {
            Ok(()) => Ok(Some(())),
            Err(err) => {
                self.revert_row_progress(progress.prev_row_in_page, progress.prev_emitted);
                Err(err)
            }
        }
    }

    #[must_use]
    pub const fn decode_policy(&self) -> DecodePolicy {
        self.decode_policy
    }

    pub fn set_decode_policy(&mut self, policy: DecodePolicy) {
        self.decode_policy = policy;
        if self.fullrow_decode_compiled {
            self.compiled_fullrow_columns = self
                .runtime_columns
                .iter()
                .map(|column| {
                    Self::compile_runtime_column_for_policy(column.as_ref(), policy, true)
                })
                .collect();
        }
    }

    /// Restricts iteration to a half-open page range `[start, end)`.
    ///
    /// Calling this resets current iterator progress so iteration resumes from
    /// `start`.
    ///
    /// # Errors
    ///
    /// Returns an error if the supplied range is invalid for the dataset.
    pub fn set_page_range(&mut self, start: u64, end: u64) -> Result<()> {
        let page_count = self.layout.header.page_count;
        if start > end || end > page_count {
            return Err(Error::InvalidMetadata {
                details: Cow::Owned(format!(
                    "invalid page range [{start}, {end}) for dataset with {page_count} pages"
                )),
            });
        }

        self.recycle_current_rows();
        self.recycle_owned_rows();
        self.page_row_count.set(0);
        self.row_in_page.set(0);
        self.next_page_index = start;
        self.page_range_end = end;
        self.emitted_rows.set(0);
        self.exhausted.set(false);
        Ok(())
    }

    /// Advances the iterator by one row and returns a zero-copy row view.
    ///
    /// The returned row borrows from internal buffers and must not be used after
    /// the iterator advances.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails.
    pub fn try_next_streaming_row(&mut self) -> Result<Option<StreamingRow<'_, '_>>> {
        let Some(progress) = self.reserve_next_row()? else {
            return Ok(None);
        };

        match self.streaming_row(progress.row_index) {
            Ok(row) => Ok(Some(row)),
            Err(err) => {
                self.revert_row_progress(progress.prev_row_in_page, progress.prev_emitted);
                Err(err)
            }
        }
    }

    /// Advances the iterator and invokes the visitor with a zero-copy row view.
    ///
    /// Returns `Ok(None)` when no more rows remain or `Ok(Some(()))` when a row
    /// was processed successfully.
    ///
    /// # Errors
    ///
    /// Propagates decoding failures from the iterator or errors returned by `f`.
    pub fn try_next_streaming<F>(&mut self, f: &mut F) -> Result<Option<()>>
    where
        F: for<'row> FnMut(StreamingRow<'row, '_>) -> Result<()>,
    {
        let Some(progress) = self.reserve_next_row()? else {
            return Ok(None);
        };

        let row_view = match self.streaming_row(progress.row_index) {
            Ok(row) => row,
            Err(err) => {
                self.revert_row_progress(progress.prev_row_in_page, progress.prev_emitted);
                return Err(err);
            }
        };

        if let Err(err) = f(row_view) {
            self.revert_row_progress(progress.prev_row_in_page, progress.prev_emitted);
            return Err(err);
        }

        Ok(Some(()))
    }

    /// Streams all remaining rows into the provided visitor without allocating intermediate vectors.
    ///
    /// # Errors
    ///
    /// Propagates failures reported by the iterator or the visitor closure.
    pub fn stream_all<F>(&mut self, mut f: F) -> Result<()>
    where
        F: for<'row> FnMut(StreamingRow<'row, '_>) -> Result<()>,
    {
        while self.try_next_streaming(&mut f)?.is_some() {}
        self.exhausted.set(true);
        Ok(())
    }

    /// Decodes the next chunk of rows into a column-oriented batch.
    ///
    /// # Errors
    ///
    /// Returns an error when decoding fails.
    pub fn next_columnar_batch(
        &mut self,
        max_rows: usize,
    ) -> Result<Option<super::ColumnarBatch<'_>>> {
        next_columnar_batch_with_mode(self, max_rows, ColumnarBatchMode::Segmented)
    }

    /// Decodes the next chunk of rows into a column-oriented batch using the selected mode.
    ///
    /// # Errors
    ///
    /// Returns an error when decoding fails.
    pub fn next_columnar_batch_with_mode(
        &mut self,
        max_rows: usize,
        mode: ColumnarBatchMode,
    ) -> Result<Option<super::ColumnarBatch<'_>>> {
        next_columnar_batch_with_mode(self, max_rows, mode)
    }

    /// Decodes the next chunk of rows into a column-oriented batch stored contiguously.
    ///
    /// # Errors
    ///
    /// Returns an error when decoding fails.
    pub fn next_columnar_batch_contiguous(
        &mut self,
        max_rows: usize,
    ) -> Result<Option<super::ColumnarBatch<'_>>> {
        next_columnar_batch_with_mode(self, max_rows, ColumnarBatchMode::Contiguous)
    }

    #[must_use]
    pub const fn last_columnar_batch_mode(&self) -> ColumnarBatchMode {
        self.last_batch_mode
    }

    #[must_use]
    pub const fn columnar_batch_mode_observations(&self) -> u64 {
        self.adaptive_batch_state.observations()
    }

    #[must_use]
    pub const fn columnar_batch_mode_switches(&self) -> u64 {
        self.adaptive_batch_state.switches()
    }

    pub(crate) fn streaming_row(&self, row_index: u16) -> Result<StreamingRow<'_, '_>> {
        let data = self.row_slice(row_index)?;

        Ok(StreamingRow::new(
            data,
            &self.runtime_columns,
            self.encoding,
            self.layout.header.endianness,
            self.columns_fit_row,
            self.decode_policy,
        ))
    }

    pub(crate) fn decode_row(&self, row_index: u16) -> Result<Vec<CellValue<'_>>> {
        let row_data = self.row_slice(row_index)?;
        if self.fullrow_decode_compiled {
            return Ok(self
                .decode_compiled_columns_from_row_buffer(row_data, &self.compiled_fullrow_columns)?
                .into_vec());
        }
        self.decode_columns_from_row(row_data, &self.columnar_columns)
    }

    pub(crate) fn decode_row_owned(&self, row_index: u16) -> Result<Vec<CellValue<'static>>> {
        let row_data = self.row_slice(row_index)?;
        if self.fullrow_decode_compiled {
            return self
                .decode_compiled_columns_from_row_owned(row_data, &self.compiled_fullrow_columns);
        }
        self.decode_columns_from_row_owned(row_data, &self.columnar_columns)
    }

    pub(crate) fn decode_projected_row(
        &self,
        row_index: u16,
        indices: &[usize],
    ) -> Result<Vec<CellValue<'_>>> {
        let columns = self.resolve_runtime_columns(indices)?;
        self.decode_projected_row_columns(row_index, &columns)
    }

    pub(crate) fn decode_projected_row_columns(
        &self,
        row_index: u16,
        columns: &[RuntimeColumnRef],
    ) -> Result<Vec<CellValue<'_>>> {
        let compiled_columns = self.compile_projection_columns(columns);
        self.decode_projected_row_compiled_columns(row_index, &compiled_columns)
    }

    pub(crate) fn decode_projected_row_compiled_columns(
        &self,
        row_index: u16,
        columns: &[CompiledRuntimeColumnRef],
    ) -> Result<Vec<CellValue<'_>>> {
        let row_data = self.row_slice(row_index)?;
        Ok(self
            .decode_compiled_columns_from_row_buffer(row_data, columns)?
            .into_vec())
    }

    pub(crate) fn decode_projected_row_compiled_columns_owned(
        &self,
        row_index: u16,
        columns: &[CompiledRuntimeColumnRef],
    ) -> Result<Vec<CellValue<'static>>> {
        let row_data = self.row_slice(row_index)?;
        self.decode_compiled_columns_from_row_owned(row_data, columns)
    }

    pub(crate) fn resolve_runtime_columns(
        &self,
        indices: &[usize],
    ) -> Result<Vec<RuntimeColumnRef>> {
        let mut selected = Vec::with_capacity(indices.len());
        for &column_index in indices {
            let column =
                self.runtime_columns
                    .get(column_index)
                    .ok_or_else(|| Error::InvalidMetadata {
                        details: Cow::Owned(format!(
                            "projected column index {column_index} out of bounds"
                        )),
                    })?;
            selected.push(column.as_ref());
        }
        Ok(selected)
    }

    pub(crate) fn resolve_compiled_runtime_columns(
        &self,
        indices: &[usize],
    ) -> Result<Vec<CompiledRuntimeColumnRef>> {
        let mut selected = Vec::with_capacity(indices.len());
        for &column_index in indices {
            let column =
                self.runtime_columns
                    .get(column_index)
                    .ok_or_else(|| Error::InvalidMetadata {
                        details: Cow::Owned(format!(
                            "projected column index {column_index} out of bounds"
                        )),
                    })?;
            selected.push(self.compile_runtime_column(column.as_ref()));
        }
        Ok(selected)
    }

    pub(crate) fn resolve_numeric_runtime_columns(
        &self,
        indices: &[usize],
    ) -> Result<Vec<NumericRuntimeColumnRef>> {
        let mut selected = Vec::with_capacity(indices.len());
        for &column_index in indices {
            let column =
                self.runtime_columns
                    .get(column_index)
                    .ok_or_else(|| Error::InvalidMetadata {
                        details: Cow::Owned(format!(
                            "projected column index {column_index} out of bounds"
                        )),
                    })?;
            let numeric = column
                .as_numeric_ref()
                .ok_or_else(|| Error::InvalidMetadata {
                    details: Cow::Owned(format!(
                        "typed numeric scan requires numeric columns, but index {} is non-numeric",
                        column.index
                    )),
                })?;
            selected.push(numeric);
        }
        Ok(selected)
    }

    fn compile_numeric_columns(
        columns: &[RuntimeColumnRef],
    ) -> Result<Vec<NumericRuntimeColumnRef>> {
        let mut compiled = Vec::with_capacity(columns.len());
        for column in columns {
            let numeric = column
                .as_numeric_ref()
                .ok_or_else(|| Error::InvalidMetadata {
                    details: Cow::Owned(format!(
                        "typed numeric scan requires numeric columns, but index {} is non-numeric",
                        column.index
                    )),
                })?;
            compiled.push(numeric);
        }
        Ok(compiled)
    }

    fn compile_projection_columns(
        &self,
        columns: &[RuntimeColumnRef],
    ) -> Vec<CompiledRuntimeColumnRef> {
        columns
            .iter()
            .copied()
            .map(|column| self.compile_runtime_column(column))
            .collect()
    }

    fn compile_runtime_column(&self, column: RuntimeColumnRef) -> CompiledRuntimeColumnRef {
        Self::compile_runtime_column_for_policy(
            column,
            self.decode_policy,
            !self.projection_decode_legacy,
        )
    }

    fn compile_runtime_column_for_policy(
        column: RuntimeColumnRef,
        decode_policy: DecodePolicy,
        specialize: bool,
    ) -> CompiledRuntimeColumnRef {
        if !specialize {
            return CompiledRuntimeColumnRef {
                index: column.index,
                offset: column.offset,
                end: column.end,
                decode: CompiledDecodeKind::Generic {
                    kind: column.kind,
                    raw_width: column.raw_width,
                },
            };
        }

        let decode = match column.kind {
            ColumnKind::Character => CompiledDecodeKind::Character,
            ColumnKind::Numeric(NumericKind::Double) => CompiledDecodeKind::NumericDouble {
                raw_width: column.raw_width,
            },
            ColumnKind::Numeric(NumericKind::Date) => {
                if decode_policy.temporal == TemporalDecodePolicy::Yes {
                    CompiledDecodeKind::NumericDateTemporal {
                        raw_width: column.raw_width,
                    }
                } else {
                    CompiledDecodeKind::NumericDateRaw {
                        raw_width: column.raw_width,
                    }
                }
            }
            ColumnKind::Numeric(NumericKind::DateTime) => {
                if decode_policy.temporal == TemporalDecodePolicy::Yes {
                    CompiledDecodeKind::NumericDateTimeTemporal {
                        raw_width: column.raw_width,
                    }
                } else {
                    CompiledDecodeKind::NumericDateTimeRaw {
                        raw_width: column.raw_width,
                    }
                }
            }
            ColumnKind::Numeric(NumericKind::Time) => {
                if decode_policy.temporal == TemporalDecodePolicy::Yes {
                    CompiledDecodeKind::NumericTimeTemporal {
                        raw_width: column.raw_width,
                    }
                } else {
                    CompiledDecodeKind::NumericTimeRaw {
                        raw_width: column.raw_width,
                    }
                }
            }
        };
        CompiledRuntimeColumnRef {
            index: column.index,
            offset: column.offset,
            end: column.end,
            decode,
        }
    }

    fn decode_columns_from_row<'data>(
        &self,
        row_data: &'data [u8],
        columns: &[RuntimeColumnRef],
    ) -> Result<Vec<CellValue<'data>>> {
        Ok(self
            .decode_columns_from_row_buffer(row_data, columns)?
            .into_vec())
    }

    fn decode_columns_from_row_buffer<'data>(
        &self,
        row_data: &'data [u8],
        columns: &[RuntimeColumnRef],
    ) -> Result<SmallVec<[CellValue<'data>; 16]>> {
        let mut values = SmallVec::<[CellValue<'data>; 16]>::with_capacity(columns.len());

        if self.columns_fit_row {
            for column in columns {
                debug_assert!(column.end <= row_data.len());
                let slice = &row_data[column.offset..column.end];
                values.push(decode_value_inner(
                    column.kind,
                    column.raw_width,
                    slice,
                    self.encoding,
                    self.layout.header.endianness,
                    self.decode_policy,
                ));
            }
            return Ok(values);
        }

        for column in columns {
            let slice =
                row_data
                    .get(column.offset..column.end)
                    .ok_or_else(|| Error::Corrupted {
                        section: Section::Column {
                            index: column.index,
                        },
                        details: Cow::from("column slice out of bounds"),
                    })?;
            values.push(decode_value_inner(
                column.kind,
                column.raw_width,
                slice,
                self.encoding,
                self.layout.header.endianness,
                self.decode_policy,
            ));
        }
        Ok(values)
    }

    fn decode_columns_from_row_owned(
        &self,
        row_data: &[u8],
        columns: &[RuntimeColumnRef],
    ) -> Result<Vec<CellValue<'static>>> {
        let mut values = SmallVec::<[CellValue<'static>; 16]>::with_capacity(columns.len());

        if self.columns_fit_row {
            for column in columns {
                debug_assert!(column.end <= row_data.len());
                let slice = &row_data[column.offset..column.end];
                values.push(
                    decode_value_inner(
                        column.kind,
                        column.raw_width,
                        slice,
                        self.encoding,
                        self.layout.header.endianness,
                        self.decode_policy,
                    )
                    .into_owned(),
                );
            }
            return Ok(values.into_vec());
        }

        for column in columns {
            let slice =
                row_data
                    .get(column.offset..column.end)
                    .ok_or_else(|| Error::Corrupted {
                        section: Section::Column {
                            index: column.index,
                        },
                        details: Cow::from("column slice out of bounds"),
                    })?;
            values.push(
                decode_value_inner(
                    column.kind,
                    column.raw_width,
                    slice,
                    self.encoding,
                    self.layout.header.endianness,
                    self.decode_policy,
                )
                .into_owned(),
            );
        }
        Ok(values.into_vec())
    }

    fn decode_compiled_columns_from_row_buffer<'data>(
        &self,
        row_data: &'data [u8],
        columns: &[CompiledRuntimeColumnRef],
    ) -> Result<SmallVec<[CellValue<'data>; 16]>> {
        let mut values = SmallVec::<[CellValue<'data>; 16]>::with_capacity(columns.len());

        if self.columns_fit_row {
            for column in columns {
                debug_assert!(column.end <= row_data.len());
                let slice = &row_data[column.offset..column.end];
                values.push(self.decode_compiled_value(slice, column.decode));
            }
            return Ok(values);
        }

        for column in columns {
            let slice =
                row_data
                    .get(column.offset..column.end)
                    .ok_or_else(|| Error::Corrupted {
                        section: Section::Column {
                            index: column.index,
                        },
                        details: Cow::from("column slice out of bounds"),
                    })?;
            values.push(self.decode_compiled_value(slice, column.decode));
        }

        Ok(values)
    }

    fn decode_compiled_columns_from_row_owned(
        &self,
        row_data: &[u8],
        columns: &[CompiledRuntimeColumnRef],
    ) -> Result<Vec<CellValue<'static>>> {
        let mut values = SmallVec::<[CellValue<'static>; 16]>::with_capacity(columns.len());

        if self.columns_fit_row {
            for column in columns {
                debug_assert!(column.end <= row_data.len());
                let slice = &row_data[column.offset..column.end];
                values.push(
                    self.decode_compiled_value(slice, column.decode)
                        .into_owned(),
                );
            }
            return Ok(values.into_vec());
        }

        for column in columns {
            let slice =
                row_data
                    .get(column.offset..column.end)
                    .ok_or_else(|| Error::Corrupted {
                        section: Section::Column {
                            index: column.index,
                        },
                        details: Cow::from("column slice out of bounds"),
                    })?;
            values.push(
                self.decode_compiled_value(slice, column.decode)
                    .into_owned(),
            );
        }

        Ok(values.into_vec())
    }

    #[inline]
    fn decode_compiled_value<'data>(
        &self,
        slice: &'data [u8],
        decode: CompiledDecodeKind,
    ) -> CellValue<'data> {
        match decode {
            CompiledDecodeKind::Generic { kind, raw_width } => decode_value_inner(
                kind,
                raw_width,
                slice,
                self.encoding,
                self.layout.header.endianness,
                self.decode_policy,
            ),
            CompiledDecodeKind::Character => CellValue::Str(decode_string_with_policy(
                slice,
                self.encoding,
                self.decode_policy,
            )),
            CompiledDecodeKind::NumericDouble { raw_width } => {
                self.decode_compiled_numeric(slice, raw_width, NumericProjectionKind::Double)
            }
            CompiledDecodeKind::NumericDateRaw { raw_width } => {
                self.decode_compiled_numeric(slice, raw_width, NumericProjectionKind::DateRaw)
            }
            CompiledDecodeKind::NumericDateTemporal { raw_width } => {
                self.decode_compiled_numeric(slice, raw_width, NumericProjectionKind::DateTemporal)
            }
            CompiledDecodeKind::NumericDateTimeRaw { raw_width } => {
                self.decode_compiled_numeric(slice, raw_width, NumericProjectionKind::DateTimeRaw)
            }
            CompiledDecodeKind::NumericDateTimeTemporal { raw_width } => self
                .decode_compiled_numeric(slice, raw_width, NumericProjectionKind::DateTimeTemporal),
            CompiledDecodeKind::NumericTimeRaw { raw_width } => {
                self.decode_compiled_numeric(slice, raw_width, NumericProjectionKind::TimeRaw)
            }
            CompiledDecodeKind::NumericTimeTemporal { raw_width } => {
                self.decode_compiled_numeric(slice, raw_width, NumericProjectionKind::TimeTemporal)
            }
        }
    }

    #[inline]
    fn decode_compiled_numeric<'data>(
        &self,
        slice: &[u8],
        raw_width: u32,
        kind: NumericProjectionKind,
    ) -> CellValue<'data> {
        match decode_numeric_cell(slice, self.layout.header.endianness) {
            NumericCell::Missing(missing) => CellValue::Missing(missing),
            NumericCell::Number(number) => match kind {
                NumericProjectionKind::Double
                | NumericProjectionKind::DateRaw
                | NumericProjectionKind::DateTimeRaw
                | NumericProjectionKind::TimeRaw => numeric_value_from_width(number, raw_width),
                NumericProjectionKind::DateTemporal => sas_days_to_datetime(number).map_or_else(
                    || numeric_value_from_width(number, raw_width),
                    CellValue::Date,
                ),
                NumericProjectionKind::DateTimeTemporal => sas_seconds_to_datetime(number)
                    .map_or_else(
                        || numeric_value_from_width(number, raw_width),
                        CellValue::DateTime,
                    ),
                NumericProjectionKind::TimeTemporal => sas_seconds_to_time(number).map_or_else(
                    || numeric_value_from_width(number, raw_width),
                    CellValue::Time,
                ),
            },
        }
    }

    fn decode_numeric_projected_row_into(
        &self,
        row_index: u16,
        columns: &[NumericRuntimeColumnRef],
        values: &mut Vec<Option<f64>>,
    ) -> Result<()> {
        let row_data = self.row_slice(row_index)?;
        self.decode_numeric_columns_from_row(row_data, columns, values)
    }

    fn decode_numeric_columns_from_row(
        &self,
        row_data: &[u8],
        columns: &[NumericRuntimeColumnRef],
        values: &mut Vec<Option<f64>>,
    ) -> Result<()> {
        values.clear();
        values.reserve(columns.len());

        if self.columns_fit_row {
            for column in columns {
                let slice = &row_data[column.offset..column.end];
                values.push(
                    match decode_numeric_cell(slice, self.layout.header.endianness) {
                        NumericCell::Missing(_) => None,
                        NumericCell::Number(number) => Some(number),
                    },
                );
            }
            return Ok(());
        }

        for column in columns {
            let slice =
                row_data
                    .get(column.offset..column.end)
                    .ok_or_else(|| Error::Corrupted {
                        section: Section::Column {
                            index: column.index,
                        },
                        details: Cow::from("column slice out of bounds"),
                    })?;
            values.push(
                match decode_numeric_cell(slice, self.layout.header.endianness) {
                    NumericCell::Missing(_) => None,
                    NumericCell::Number(number) => Some(number),
                },
            );
        }
        Ok(())
    }

    pub(crate) fn row_slice(&self, row_index: u16) -> Result<&[u8]> {
        if let Some(base) = self.contiguous_base {
            let offset = base + (row_index as usize).saturating_mul(self.row_length);
            let end = offset.saturating_add(self.row_length);
            if end > self.page_buffer.len() {
                return Err(Error::Corrupted {
                    section: Section::Row {
                        index: u64::from(row_index),
                    },
                    details: Cow::from("row offset exceeds page bounds"),
                });
            }
            return Ok(&self.page_buffer[offset..end]);
        }

        let row = self
            .current_rows
            .get(row_index as usize)
            .ok_or_else(|| Error::Corrupted {
                section: Section::Row {
                    index: u64::from(row_index),
                },
                details: Cow::from("row index out of bounds for current page"),
            })?;
        row.as_slice(self.row_length, &self.page_buffer, u64::from(row_index))
    }

    pub(crate) fn append_row_to_owned_buffer(&mut self, row_index: u16) -> Result<()> {
        let slice = if let Some(base) = self.contiguous_base {
            let offset = base + (row_index as usize).saturating_mul(self.row_length);
            let end = offset.saturating_add(self.row_length);
            if end > self.page_buffer.len() {
                return Err(Error::Corrupted {
                    section: Section::Row {
                        index: u64::from(row_index),
                    },
                    details: Cow::from("row offset exceeds page bounds"),
                });
            }
            &self.page_buffer[offset..end]
        } else {
            let row =
                self.current_rows
                    .get(row_index as usize)
                    .ok_or_else(|| Error::Corrupted {
                        section: Section::Row {
                            index: u64::from(row_index),
                        },
                        details: Cow::from("row index out of bounds for current page"),
                    })?;
            row.as_slice(self.row_length, &self.page_buffer, u64::from(row_index))?
        };

        self.columnar_owned_buffer.extend_from_slice(slice);
        Ok(())
    }
}

impl<R, L> Iterator for RowIteratorCore<R, L>
where
    R: Read + Seek,
    L: Deref<Target = DatasetLayout>,
{
    type Item = Result<Vec<CellValue<'static>>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.try_next_owned() {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => None,
            Err(err) => {
                self.exhausted.set(true);
                Some(Err(err))
            }
        }
    }
}
