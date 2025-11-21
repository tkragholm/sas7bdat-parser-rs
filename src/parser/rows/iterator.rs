use std::borrow::Cow;
use std::cell::Cell;
use std::convert::TryFrom;
use std::io::{Read, Seek};

use encoding_rs::Encoding;

use crate::error::{Error, Result, Section};
use crate::metadata::Compression;
use crate::parser::core::encoding::resolve_encoding;
use crate::parser::metadata::ParsedMetadata;
use crate::value::Value;

use super::batch::{next_columnar_batch, next_columnar_batch_contiguous};
use super::buffer::RowData;
use super::runtime_column::{RuntimeColumn, RuntimeColumnRef};
use super::streaming::StreamingRow;

#[derive(Clone, Copy)]
struct RowProgress {
    row_index: u16,
    prev_row_in_page: u16,
    prev_emitted: u64,
}

pub struct RowIterator<'a, R: Read + Seek> {
    pub(crate) reader: &'a mut R,
    pub(crate) parsed: &'a ParsedMetadata,
    pub(crate) runtime_columns: Vec<RuntimeColumn>,
    pub(crate) columnar_columns: Vec<RuntimeColumnRef>,
    pub(crate) page_buffer: Vec<u8>,
    pub(crate) current_rows: Vec<RowData>,
    pub(crate) reusable_row_buffers: Vec<Vec<u8>>,
    pub(crate) columnar_owned_buffer: Vec<u8>,
    pub(crate) page_row_count: Cell<u16>,
    pub(crate) row_in_page: Cell<u16>,
    pub(crate) next_page_index: u64,
    pub(crate) emitted_rows: Cell<u64>,
    pub(crate) encoding: &'static Encoding,
    pub(crate) exhausted: Cell<bool>,
    pub(crate) row_length: usize,
    pub(crate) total_rows: u64,
}

/// Creates a [`RowIterator`] for the provided reader and parsed metadata.
///
/// # Errors
///
/// Returns an error if the iterator cannot be constructed, for example when
/// the dataset uses unsupported compression.
pub fn row_iterator<'a, R: Read + Seek>(
    reader: &'a mut R,
    parsed: &'a ParsedMetadata,
) -> Result<RowIterator<'a, R>> {
    RowIterator::new(reader, parsed)
}

impl<'a, R: Read + Seek> RowIterator<'a, R> {
    /// Constructs a new row iterator for the provided reader and metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the dataset uses an unsupported compression mode
    /// or the page size cannot be represented on this platform.
    pub fn new(reader: &'a mut R, parsed: &'a ParsedMetadata) -> Result<Self> {
        match parsed.row_info.compression {
            Compression::None | Compression::Row | Compression::Binary => {}
            Compression::Unknown(code) => {
                return Err(Error::Unsupported {
                    feature: Cow::from(format!(
                        "row iteration for unsupported {code:?} compression"
                    )),
                });
            }
        }

        let encoding = resolve_encoding(parsed.header.metadata.file_encoding.as_deref());
        let page_size =
            usize::try_from(parsed.header.page_size).map_err(|_| Error::Unsupported {
                feature: Cow::from("page size exceeds platform pointer width"),
            })?;
        let row_length =
            usize::try_from(parsed.row_info.row_length).map_err(|_| Error::Unsupported {
                feature: Cow::from("row length exceeds platform pointer width"),
            })?;
        let runtime_columns = parsed
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
                Ok(RuntimeColumn {
                    index: column.index,
                    offset,
                    width,
                    raw_width: column.offsets.width,
                    kind: column.kind,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let columnar_columns: Vec<RuntimeColumnRef> =
            runtime_columns.iter().map(RuntimeColumn::as_ref).collect();

        Ok(Self {
            reader,
            parsed,
            runtime_columns,
            columnar_columns,
            page_buffer: vec![0u8; page_size],
            current_rows: Vec::new(),
            reusable_row_buffers: Vec::new(),
            columnar_owned_buffer: Vec::new(),
            page_row_count: Cell::new(0),
            row_in_page: Cell::new(0),
            next_page_index: 0,
            emitted_rows: Cell::new(0),
            encoding,
            exhausted: Cell::new(false),
            row_length,
            total_rows: parsed.row_info.total_rows,
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
    pub fn try_next(&mut self) -> Result<Option<Vec<Value<'_>>>> {
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
        next_columnar_batch(self, max_rows)
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
        next_columnar_batch_contiguous(self, max_rows)
    }

    pub(crate) fn streaming_row(&self, row_index: u16) -> Result<StreamingRow<'_, '_>> {
        let row = self
            .current_rows
            .get(row_index as usize)
            .ok_or_else(|| Error::Corrupted {
                section: Section::Row {
                    index: u64::from(row_index),
                },
                details: Cow::from("row index out of bounds for current page"),
            })?;
        let data = row.as_slice(self.row_length, &self.page_buffer, u64::from(row_index))?;

        Ok(StreamingRow::new(
            data,
            &self.runtime_columns,
            self.encoding,
            self.parsed.header.endianness,
        ))
    }

    pub(crate) fn decode_row(&self, row_index: u16) -> Result<Vec<Value<'_>>> {
        let row = self.streaming_row(row_index)?;
        row.materialize()
    }
}

impl<R: Read + Seek> Iterator for RowIterator<'_, R> {
    type Item = Result<Vec<Value<'static>>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.try_next() {
            Ok(Some(row)) => {
                let owned = row.into_iter().map(Value::into_owned).collect();
                Some(Ok(owned))
            }
            Ok(None) => None,
            Err(err) => {
                self.exhausted.set(true);
                Some(Err(err))
            }
        }
    }
}
