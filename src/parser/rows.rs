use std::borrow::Cow;
use std::cell::Cell;
use std::convert::{TryFrom, TryInto};
use std::io::{Read, Seek, SeekFrom};

use crate::error::{Error, Result, Section};
use crate::metadata::{Compression, Endianness, MissingLiteral, TaggedMissing, Vendor};
use crate::parser::column::{ColumnKind, NumericKind};
use crate::parser::meta::ParsedMetadata;
use super::encoding::{resolve_encoding, trim_trailing};
use crate::value::{MissingValue, Value};
use encoding_rs::{Encoding, UTF_8};
use rayon::prelude::*;
use simdutf8::basic;
use smallvec::SmallVec;
use time::{Date, Duration, Month, OffsetDateTime, PrimitiveDateTime, Time};

use super::byteorder::{read_u16, read_u32, read_u64};

const PARALLEL_CHUNK_ROWS: usize = 64;
const COLUMNAR_BATCH_ROWS: usize = 256;
const COLUMNAR_INLINE_ROWS: usize = 32;

const SAS_PAGE_TYPE_MASK: u16 = 0x0F00;
const SAS_PAGE_TYPE_DATA: u16 = 0x0100;
const SAS_PAGE_TYPE_MIX: u16 = 0x0200;
const SAS_PAGE_TYPE_COMP: u16 = 0x9000;

const SAS_SUBHEADER_SIGNATURE_COLUMN_TEXT: u32 = 0xFFFF_FFFD;
const SAS_SUBHEADER_SIGNATURE_COLUMN_ATTRS: u32 = 0xFFFF_FFFC;
const SAS_SUBHEADER_SIGNATURE_COLUMN_FORMAT: u32 = 0xFFFF_FBFE;
const SAS_SUBHEADER_SIGNATURE_COLUMN_NAME: u32 = 0xFFFF_FFFF;
const SAS_SUBHEADER_SIGNATURE_COLUMN_SIZE: u32 = 0xF6F6_F6F6;
const SAS_SUBHEADER_SIGNATURE_ROW_SIZE: u32 = 0xF7F7_F7F7;
const SAS_SUBHEADER_SIGNATURE_COUNTS: u32 = 0xFFFF_FC00;
const SAS_SUBHEADER_SIGNATURE_COLUMN_LIST: u32 = 0xFFFF_FFFE;

const SAS_COMPRESSION_NONE: u8 = 0x00;
const SAS_COMPRESSION_TRUNC: u8 = 0x01;
const SAS_COMPRESSION_ROW: u8 = 0x04;
const SUBHEADER_POINTER_OFFSET: usize = 8;

pub struct RowIterator<'a, R: Read + Seek> {
    reader: &'a mut R,
    parsed: &'a ParsedMetadata,
    runtime_columns: Vec<RuntimeColumn>,
    columnar_columns: Vec<RuntimeColumnRef>,
    page_buffer: Vec<u8>,
    current_rows: Vec<RowData>,
    reusable_row_buffers: Vec<Vec<u8>>,
    page_row_count: Cell<u16>,
    row_in_page: Cell<u16>,
    next_page_index: u64,
    emitted_rows: Cell<u64>,
    encoding: &'static Encoding,
    exhausted: Cell<bool>,
    row_length: usize,
    total_rows: u64,
}

enum RowData {
    Borrowed(usize),
    Owned(Vec<u8>),
}

impl RowData {
    fn as_slice<'data>(
        &'data self,
        row_length: usize,
        page_buffer: &'data [u8],
        row_index: u64,
    ) -> Result<&'data [u8]> {
        match self {
            Self::Borrowed(offset) => {
                let start = *offset;
                let end = start.saturating_add(row_length);
                if end > page_buffer.len() {
                    return Err(Error::Corrupted {
                        section: Section::Row { index: row_index },
                        details: Cow::from("row offset exceeds page bounds"),
                    });
                }
                Ok(&page_buffer[start..end])
            }
            Self::Owned(buffer) => Ok(buffer.as_slice()),
        }
    }
}

/// Lightweight view over a row slice with associated metadata for streaming sinks.
pub struct StreamingRow<'data, 'meta> {
    data: &'data [u8],
    columns: &'meta [RuntimeColumn],
    encoding: &'static Encoding,
    endianness: Endianness,
}

/// Lightweight accessor for a single column within a streaming row.
pub struct StreamingCell<'data, 'meta> {
    column: &'meta RuntimeColumn,
    slice: &'data [u8],
    encoding: &'static Encoding,
    endianness: Endianness,
}

impl<'data, 'meta> StreamingRow<'data, 'meta> {
    #[must_use]
    const fn new(
        data: &'data [u8],
        columns: &'meta [RuntimeColumn],
        encoding: &'static Encoding,
        endianness: Endianness,
    ) -> Self {
        Self {
            data,
            columns,
            encoding,
            endianness,
        }
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.columns.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Returns the streaming cell at `index`.
    ///
    /// # Errors
    ///
    /// Returns an error when the index is out of bounds or the column slice
    /// exceeds the row buffer.
    pub fn cell(&self, index: usize) -> Result<StreamingCell<'data, 'meta>> {
        let column = self
            .columns
            .get(index)
            .ok_or_else(|| Error::InvalidMetadata {
                details: Cow::Owned(format!("column index {index} out of bounds")),
            })?;
        self.cell_from_column(column)
    }

    fn cell_from_column(
        &self,
        column: &'meta RuntimeColumn,
    ) -> Result<StreamingCell<'data, 'meta>> {
        if column.offset + column.width > self.data.len() {
            return Err(Error::Corrupted {
                section: Section::Column {
                    index: column.index,
                },
                details: Cow::from("column slice out of bounds"),
            });
        }
        Ok(StreamingCell {
            column,
            slice: &self.data[column.offset..column.offset + column.width],
            encoding: self.encoding,
            endianness: self.endianness,
        })
    }

    #[must_use]
    pub const fn iter(&self) -> StreamingRowIter<'_, 'data, 'meta> {
        StreamingRowIter {
            row: self,
            index: 0,
        }
    }

    /// Materialises the row into an owned vector of values.
    ///
    /// # Errors
    ///
    /// Propagates decoding failures for individual cells.
    pub fn materialize(&self) -> Result<Vec<Value<'data>>> {
        let mut values = SmallVec::<[Value<'data>; 16]>::with_capacity(self.columns.len());
        self.materialize_into(&mut values)?;
        Ok(values.into_vec())
    }

    /// Materialises the row into the provided buffer, reusing its capacity.
    ///
    /// # Errors
    ///
    /// Propagates decoding failures for individual cells.
    pub fn materialize_into(&self, values: &mut SmallVec<[Value<'data>; 16]>) -> Result<()> {
        values.clear();
        values.reserve(self.columns.len());
        for cell in self {
            let cell = cell?;
            values.push(cell.decode_value()?);
        }
        Ok(())
    }
}

impl<'data> StreamingCell<'data, '_> {
    #[must_use]
    pub const fn column_index(&self) -> u32 {
        self.column.index
    }

    #[must_use]
    pub const fn width(&self) -> usize {
        self.column.width
    }

    #[must_use]
    pub const fn kind(&self) -> ColumnKind {
        self.column.kind
    }

    #[must_use]
    pub const fn raw_slice(&self) -> &'data [u8] {
        self.slice
    }

    #[must_use]
    pub fn is_missing(&self) -> bool {
        match self.column.kind {
            ColumnKind::Character => is_blank(self.slice),
            ColumnKind::Numeric(_) => {
                let raw = numeric_bits(self.slice, self.endianness);
                numeric_bits_is_missing(raw)
            }
        }
    }

    /// Decodes the cell into a `Value`.
    ///
    /// # Errors
    ///
    /// Returns an error when decoding fails (e.g. invalid metadata).
    pub fn decode_value(&self) -> Result<Value<'data>> {
        match self.column.kind {
            ColumnKind::Character => Ok(Value::Str(decode_string(self.slice, self.encoding))),
            ColumnKind::Numeric(kind) => match decode_numeric_cell(self.slice, self.endianness) {
                NumericCell::Missing(missing) => Ok(Value::Missing(missing)),
                NumericCell::Number(number) => Ok(match kind {
                    NumericKind::Double => numeric_value_from_width(number, self.column.raw_width),
                    NumericKind::Date => sas_days_to_datetime(number).map_or_else(
                        || numeric_value_from_width(number, self.column.raw_width),
                        Value::Date,
                    ),
                    NumericKind::DateTime => sas_seconds_to_datetime(number).map_or_else(
                        || numeric_value_from_width(number, self.column.raw_width),
                        Value::DateTime,
                    ),
                    NumericKind::Time => sas_seconds_to_time(number).map_or_else(
                        || numeric_value_from_width(number, self.column.raw_width),
                        Value::Time,
                    ),
                }),
            },
        }
    }
}

impl<'row, 'data, 'meta> IntoIterator for &'row StreamingRow<'data, 'meta> {
    type Item = Result<StreamingCell<'data, 'meta>>;
    type IntoIter = StreamingRowIter<'row, 'data, 'meta>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct StreamingRowIter<'row, 'data, 'meta> {
    row: &'row StreamingRow<'data, 'meta>,
    index: usize,
}

impl<'data, 'meta> Iterator for StreamingRowIter<'_, 'data, 'meta> {
    type Item = Result<StreamingCell<'data, 'meta>>;

    fn next(&mut self) -> Option<Self::Item> {
        let column = self.row.columns.get(self.index)?;
        self.index += 1;
        if column.offset + column.width > self.row.data.len() {
            return Some(Err(Error::Corrupted {
                section: Section::Column {
                    index: column.index,
                },
                details: Cow::from("column slice out of bounds"),
            }));
        }
        Some(Ok(StreamingCell {
            column,
            slice: &self.row.data[column.offset..column.offset + column.width],
            encoding: self.row.encoding,
            endianness: self.row.endianness,
        }))
    }
}

enum NumericCell {
    Missing(MissingValue),
    Number(f64),
}

#[derive(Clone, Copy)]
struct RuntimeColumn {
    index: u32,
    offset: usize,
    width: usize,
    raw_width: u32,
    kind: ColumnKind,
}

impl RuntimeColumn {
    const fn as_ref(&self) -> RuntimeColumnRef {
        RuntimeColumnRef {
            index: self.index,
            offset: self.offset,
            width: self.width,
            kind: self.kind,
        }
    }
}

impl<'a, R: Read + Seek> RowIterator<'a, R> {
    /// Constructs a new row iterator for the provided reader and metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the dataset uses an unsupported compression mode
    /// or the page size cannot be represented on this platform.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
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

        let columnar_columns = runtime_columns.iter().map(RuntimeColumn::as_ref).collect();

        Ok(Self {
            reader,
            parsed,
            runtime_columns,
            columnar_columns,
            page_buffer: vec![0u8; page_size],
            current_rows: Vec::new(),
            reusable_row_buffers: Vec::new(),
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
    fn ensure_page_ready(&mut self) -> Result<bool> {
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
    fn revert_row_progress(&self, prev_row_in_page: u16, prev_emitted: u64) {
        self.row_in_page.set(prev_row_in_page);
        self.emitted_rows.set(prev_emitted);
        self.exhausted.set(true);
    }

    /// Advances the iterator by one row.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn try_next(&mut self) -> Result<Option<Vec<Value<'_>>>> {
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

        let row_result = self.decode_row(row_index);
        let row = match row_result {
            Ok(row) => row,
            Err(err) => {
                self.revert_row_progress(prev_row_in_page, prev_emitted);
                return Err(err);
            }
        };
        Ok(Some(row))
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
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

        let row_view = match self.streaming_row(row_index) {
            Ok(row) => row,
            Err(err) => {
                self.revert_row_progress(prev_row_in_page, prev_emitted);
                return Err(err);
            }
        };

        if let Err(err) = f(row_view) {
            self.revert_row_progress(prev_row_in_page, prev_emitted);
            return Err(err);
        }

        Ok(Some(()))
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
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

    /// Prototype: materialises all rows on a page in parallel and invokes `f` with owned values.
    ///
    /// This keeps the existing streaming API untouched while allowing benchmarks to evaluate
    /// Rayon-based decoding throughput.
    ///
    /// # Errors
    ///
    /// Propagates decoding failures, page fetch errors, or any error returned by the visitor `f`.
    pub fn stream_all_parallel_owned<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(Vec<Value<'static>>) -> Result<()>,
    {
        if self.exhausted.get() {
            return Ok(());
        }

        while self.emitted_rows.get() < self.total_rows {
            if self.row_in_page.get() >= self.page_row_count.get() {
                if let Err(err) = self.fetch_next_page() {
                    self.exhausted.set(true);
                    return Err(err);
                }
                if self.page_row_count.get() == 0 {
                    self.exhausted.set(true);
                    return Ok(());
                }
            }

            let page_total = usize::from(self.page_row_count.get());
            let start = usize::from(self.row_in_page.get());
            if start >= page_total {
                continue;
            }

            let processed = {
                let rows_slice = &self.current_rows[start..page_total];
                if rows_slice.is_empty() {
                    0usize
                } else {
                    let page_buffer = &self.page_buffer;
                    let columns = &self.runtime_columns;
                    let encoding = self.encoding;
                    let endianness = self.parsed.header.endianness;
                    let row_length = self.row_length;

                    let chunk_results: Result<Vec<Vec<Vec<Value<'static>>>>> = rows_slice
                        .par_chunks(PARALLEL_CHUNK_ROWS)
                        .enumerate()
                        .map(|(chunk_idx, chunk)| {
                            let mut chunk_output: Vec<Vec<Value<'static>>> =
                                Vec::with_capacity(chunk.len());
                            for (offset, row_data) in chunk.iter().enumerate() {
                                let row_index = start + chunk_idx * PARALLEL_CHUNK_ROWS + offset;
                                let data =
                                    row_data.as_slice(row_length, page_buffer, row_index as u64)?;
                                let view = StreamingRow::new(data, columns, encoding, endianness);
                                let mut owned = Vec::with_capacity(view.len());
                                for cell_result in &view {
                                    let cell = cell_result?;
                                    owned.push(cell.decode_value()?.into_owned());
                                }
                                chunk_output.push(owned);
                            }
                            Ok(chunk_output)
                        })
                        .collect();

                    for chunk in chunk_results? {
                        for row in chunk {
                            f(row)?;
                        }
                    }

                    rows_slice.len()
                }
            };

            let emitted = self.emitted_rows.get();
            self.emitted_rows
                .set(emitted.saturating_add(processed as u64));

            let advanced = start.saturating_add(processed);
            self.row_in_page
                .set(u16::try_from(advanced).unwrap_or(u16::MAX));
        }

        self.exhausted.set(true);
        Ok(())
    }

    /// Decodes the next chunk of rows into a column-oriented batch.
    ///
    /// # Errors
    ///
    /// Returns an error when decoding fails.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn next_columnar_batch(&mut self, max_rows: usize) -> Result<Option<ColumnarBatch<'_>>> {
        if self.exhausted.get() {
            return Ok(None);
        }

        let target = if max_rows == 0 {
            COLUMNAR_BATCH_ROWS
        } else {
            max_rows
        };

        loop {
            if !self.ensure_page_ready()? {
                return Ok(None);
            }

            let page_total = usize::from(self.page_row_count.get());
            let start = usize::from(self.row_in_page.get());
            if start >= page_total {
                continue;
            }

            let available = page_total - start;
            let chunk_len = available.min(target);
            let row_end = start + chunk_len;

            self.row_in_page
                .set(u16::try_from(row_end).unwrap_or(u16::MAX));
            self.emitted_rows
                .set(self.emitted_rows.get().saturating_add(chunk_len as u64));

            if self.emitted_rows.get() >= self.total_rows {
                self.exhausted.set(true);
            }

            let mut row_slices =
                SmallVec::<[&[u8]; COLUMNAR_INLINE_ROWS]>::with_capacity(chunk_len);
            for (offset, row_data) in self.current_rows[start..row_end].iter().enumerate() {
                let row_index = start + offset;
                let slice =
                    row_data.as_slice(self.row_length, &self.page_buffer, row_index as u64)?;
                row_slices.push(slice);
            }

            let batch = ColumnarBatch::new(
                row_slices,
                &self.columnar_columns,
                self.parsed.header.endianness,
            );
            return Ok(Some(batch));
        }
    }

    fn recycle_current_rows(&mut self) {
        for entry in self.current_rows.drain(..) {
            if let RowData::Owned(mut buffer) = entry {
                buffer.clear();
                self.reusable_row_buffers.push(buffer);
            }
        }
    }

    fn take_row_buffer(&mut self) -> Vec<u8> {
        self.reusable_row_buffers.pop().unwrap_or_default()
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    #[allow(clippy::too_many_lines)]
    fn fetch_next_page(&mut self) -> Result<()> {
        let header = &self.parsed.header;
        let row_length = self.row_length;

        while self.next_page_index < header.page_count {
            let offset = header.data_offset + self.next_page_index * u64::from(header.page_size);
            self.reader
                .seek(SeekFrom::Start(offset))
                .map_err(Error::from)?;
            self.reader
                .read_exact(&mut self.page_buffer)
                .map_err(Error::from)?;
            let page_index = self.next_page_index;
            self.next_page_index += 1;

            let page_type = read_u16(
                header.endianness,
                &self.page_buffer[(header.page_header_size as usize) - 8..],
            );
            if (page_type & SAS_PAGE_TYPE_COMP) == SAS_PAGE_TYPE_COMP {
                continue;
            }

            let base_page_type = page_type & SAS_PAGE_TYPE_MASK;

            let mut page_row_count = read_u16(
                header.endianness,
                &self.page_buffer[(header.page_header_size as usize) - 6..],
            );
            let target_rows = if page_row_count == 0 {
                None
            } else {
                Some(page_row_count as usize)
            };

            self.recycle_current_rows();

            let subheader_count_pos = header.page_header_size as usize - 4;
            let subheader_count =
                read_u16(header.endianness, &self.page_buffer[subheader_count_pos..]);

            let pointer_size = header.subheader_pointer_size as usize;
            let mut ptr_cursor = header.page_header_size as usize;

            for _ in 0..subheader_count {
                if ptr_cursor + pointer_size > self.page_buffer.len() {
                    return Err(Error::Corrupted {
                        section: Section::Page { index: page_index },
                        details: Cow::from("subheader pointer exceeds page bounds"),
                    });
                }

                let pointer = &self.page_buffer[ptr_cursor..ptr_cursor + pointer_size];
                ptr_cursor += pointer_size;

                let info = parse_pointer(pointer, header.uses_u64, header.endianness)?;
                if info.length == 0 {
                    continue;
                }
                if info.offset + info.length > self.page_buffer.len() {
                    return Err(Error::Corrupted {
                        section: Section::Page { index: page_index },
                        details: Cow::from("subheader pointer references data beyond page bounds"),
                    });
                }

                let data_start = info.offset;
                let data_end = info.offset + info.length;
                match info.compression {
                    SAS_COMPRESSION_NONE => {
                        let data = &self.page_buffer[data_start..data_end];
                        let signature = read_signature(data, header.endianness, header.uses_u64);
                        if info.is_compressed_data && !signature_is_recognized(signature) {
                            let mut local_offset = info.offset;
                            let mut remaining = info.length;
                            while remaining >= row_length {
                                self.current_rows.push(RowData::Borrowed(local_offset));
                                remaining -= row_length;
                                local_offset += row_length;
                                if let Some(target) = target_rows
                                    && self.current_rows.len() >= target
                                {
                                    break;
                                }
                            }
                            if let Some(target) = target_rows
                                && self.current_rows.len() >= target
                            {
                                break;
                            }
                        }
                    }
                    SAS_COMPRESSION_TRUNC => {
                        // Truncated rows are continuations that reappear in the
                        // next page; skip them to avoid emitting partial data.
                        continue;
                    }
                    SAS_COMPRESSION_ROW => {
                        let mut buffer = self.take_row_buffer();
                        let data = &self.page_buffer[data_start..data_end];
                        match self.parsed.row_info.compression {
                            Compression::Row => decompress_rle(data, row_length, &mut buffer),
                            Compression::Binary => decompress_rdc(data, row_length, &mut buffer),
                            Compression::None => {
                                return Err(Error::Unsupported {
                                    feature: Cow::from(
                                        "row compression pointer seen in uncompressed dataset",
                                    ),
                                });
                            }
                            Compression::Unknown(code) => {
                                return Err(Error::Unsupported {
                                    feature: Cow::from(format!(
                                        "row compression pointer for unsupported mode {code}",
                                    )),
                                });
                            }
                        }
                        .map_err(|msg| Error::Corrupted {
                            section: Section::Page { index: page_index },
                            details: Cow::from(msg),
                        })?;
                        self.current_rows.push(RowData::Owned(buffer));
                        if let Some(target) = target_rows
                            && self.current_rows.len() >= target
                        {
                            break;
                        }
                    }
                    other => {
                        return Err(Error::Unsupported {
                            feature: Cow::from(format!(
                                "unsupported subheader compression mode {other}",
                            )),
                        });
                    }
                }
                if let Some(target) = target_rows
                    && self.current_rows.len() >= target
                {
                    break;
                }
            }

            if self.current_rows.is_empty() {
                if base_page_type != SAS_PAGE_TYPE_DATA && base_page_type != SAS_PAGE_TYPE_MIX {
                    continue;
                }

                let bit_offset = if header.uses_u64 { 32usize } else { 16usize };
                let pointer_section_len = (subheader_count as usize) * pointer_size;
                let base_offset = header.page_header_size as usize + pointer_section_len;
                let alignment_base = bit_offset + SUBHEADER_POINTER_OFFSET + pointer_section_len;
                let align_adjust = if alignment_base.is_multiple_of(8) {
                    0
                } else {
                    8 - (alignment_base % 8)
                };
                let mut data_start = base_offset.saturating_add(align_adjust);

                if base_page_type == SAS_PAGE_TYPE_MIX
                    && (data_start % 8) == 4
                    && data_start + 4 <= self.page_buffer.len()
                {
                    let word = u32::from_le_bytes(
                        self.page_buffer[data_start..data_start + 4]
                            .try_into()
                            .unwrap(),
                    );
                    if word == 0
                        || word == 0x2020_2020
                        || header.metadata.vendor != Vendor::StatTransfer
                    {
                        data_start = data_start.saturating_add(4);
                    }
                }

                if data_start >= self.page_buffer.len() {
                    continue;
                }

                let available = self.page_buffer.len().saturating_sub(data_start);
                let possible_rows = available / row_length;
                if possible_rows == 0 {
                    continue;
                }

                let remaining_rows_u64 = self.total_rows.saturating_sub(self.emitted_rows.get());
                let remaining_rows =
                    usize::try_from(remaining_rows_u64).map_or(usize::MAX, |value| value);

                let mut rows_to_take = if base_page_type == SAS_PAGE_TYPE_MIX {
                    let mix_limit = usize::try_from(self.parsed.row_info.rows_per_page)
                        .map_or(usize::MAX, |value| value);
                    let mix_limit = if mix_limit == 0 {
                        possible_rows
                    } else {
                        mix_limit
                    };
                    mix_limit.min(possible_rows)
                } else {
                    let header_limit = usize::from(page_row_count);
                    let header_limit = if header_limit == 0 {
                        possible_rows
                    } else {
                        header_limit
                    };
                    header_limit.min(possible_rows)
                };

                rows_to_take = rows_to_take.min(remaining_rows);
                rows_to_take = rows_to_take.min(possible_rows);

                if rows_to_take == 0 {
                    continue;
                }

                for idx in 0..rows_to_take {
                    let offset = data_start + idx * row_length;
                    if offset + row_length > self.page_buffer.len() {
                        return Err(Error::Corrupted {
                            section: Section::Page { index: page_index },
                            details: Cow::from("row slice exceeds page bounds"),
                        });
                    }
                    self.current_rows.push(RowData::Borrowed(offset));
                }

                page_row_count = rows_to_take.try_into().unwrap_or(u16::MAX);
            }

            self.page_row_count
                .set(self.current_rows.len().try_into().unwrap_or(u16::MAX));
            self.row_in_page.set(0);
            if self.page_row_count.get() > 0 {
                return Ok(());
            }
        }

        self.page_row_count.set(0);
        Ok(())
    }

    fn streaming_row(&self, row_index: u16) -> Result<StreamingRow<'_, '_>> {
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

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn decode_row(&self, row_index: u16) -> Result<Vec<Value<'_>>> {
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

#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn decode_string<'a>(slice: &'a [u8], encoding: &'static Encoding) -> Cow<'a, str> {
    let trimmed = trim_trailing(slice);
    if trimmed.is_empty() {
        return Cow::Borrowed("");
    }

    if let Ok(text) = basic::from_utf8(trimmed) {
        return maybe_fix_mojibake(Cow::Borrowed(text));
    }

    if encoding == UTF_8 {
        let mut owned = String::from_utf8_lossy(trimmed).into_owned();
        let trimmed_len = owned.trim_end_matches([' ', '\u{0000}']).len();
        if trimmed_len != owned.len() {
            owned.truncate(trimmed_len);
        }
        return maybe_fix_mojibake(Cow::Owned(owned));
    }

    let (decoded, had_errors) = encoding.decode_without_bom_handling(trimmed);
    let mut owned = decoded.into_owned();
    if had_errors && owned.is_empty() {
        owned = String::from_utf8_lossy(trimmed).into_owned();
    }
    let trimmed_len = owned.trim_end_matches([' ', '\u{0000}']).len();
    if trimmed_len != owned.len() {
        owned.truncate(trimmed_len);
    }
    maybe_fix_mojibake(Cow::Owned(owned))
}

fn maybe_fix_mojibake(value: Cow<'_, str>) -> Cow<'_, str> {
    let text = value.as_ref();
    if text.is_ascii() {
        return value;
    }

    let mut bytes = Vec::with_capacity(text.len());
    let mut has_extended = false;

    for ch in text.chars() {
        let code = ch as u32;
        if code > 0xFF {
            return value;
        }
        if code >= 0x80 {
            has_extended = true;
        }
        bytes.push(u8::try_from(code).expect("code <= 0xFF enforced above"));
    }

    if has_extended
        && let Ok(decoded) = std::str::from_utf8(&bytes)
        && decoded != text
    {
        return Cow::Owned(decoded.to_owned());
    }
    value
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn decode_numeric_cell(slice: &[u8], endian: Endianness) -> NumericCell {
    if slice.is_empty() {
        return NumericCell::Missing(MissingValue::system());
    }
    let raw = numeric_bits(slice, endian);
    if numeric_bits_is_missing(raw) {
        NumericCell::Missing(decode_missing_from_bits(raw))
    } else {
        NumericCell::Number(f64::from_bits(raw))
    }
}

#[inline]
fn numeric_bits(slice: &[u8], endian: Endianness) -> u64 {
    debug_assert!(slice.len() <= 8);
    if slice.len() == 8 {
        match endian {
            Endianness::Little => {
                let bytes: [u8; 8] = slice.try_into().expect("len == 8");
                u64::from_le_bytes(bytes)
            }
            Endianness::Big => {
                let bytes: [u8; 8] = slice.try_into().expect("len == 8");
                u64::from_be_bytes(bytes)
            }
        }
    } else {
        let mut buf = [0u8; 8];
        match endian {
            Endianness::Big => {
                let len = slice.len();
                buf[..len].copy_from_slice(slice);
            }
            Endianness::Little => {
                for (idx, &byte) in slice.iter().rev().enumerate() {
                    buf[idx] = byte;
                }
            }
        }
        u64::from_be_bytes(buf)
    }
}

#[inline]
const fn numeric_bits_is_missing(raw: u64) -> bool {
    const EXP_MASK: u64 = 0x7FF0_0000_0000_0000;
    const FRACTION_MASK: u64 = 0x000F_FFFF_FFFF_FFFF;
    (raw & EXP_MASK) == EXP_MASK && (raw & FRACTION_MASK) != 0
}

const fn decode_missing_from_bits(raw: u64) -> MissingValue {
    let upper = (raw >> 40) & 0xFF;
    let tag_byte = !(upper as u8);
    match tag_byte {
        0 => MissingValue::Tagged(TaggedMissing {
            tag: Some('_'),
            literal: MissingLiteral::Numeric(f64::from_bits(raw)),
        }),
        2..=27 => {
            let ch = (b'A' + (tag_byte - 2)) as char;
            MissingValue::Tagged(TaggedMissing {
                tag: Some(ch),
                literal: MissingLiteral::Numeric(f64::from_bits(raw)),
            })
        }
        _ => MissingValue::System,
    }
}

fn numeric_value_from_width<'a>(number: f64, width: u32) -> Value<'a> {
    if let Some(int) = try_i64_from_f64(number) {
        if width <= 4 {
            if let Ok(value32) = i32::try_from(int) {
                return Value::Int32(value32);
            }
            return Value::Int64(int);
        } else if width <= 8 {
            return Value::Int64(int);
        }
    }
    Value::Float(number)
}

fn try_i64_from_f64(value: f64) -> Option<i64> {
    if !value.is_finite() {
        return None;
    }
    if value == 0.0 {
        return Some(0);
    }

    let bits = value.to_bits();
    let sign = (bits >> 63) != 0;
    let exponent_bits = ((bits >> 52) & 0x7FF) as i32;

    if exponent_bits == 0 {
        return None;
    }

    let exponent = exponent_bits - 1023;
    if exponent < 0 {
        return None;
    }

    let mut mantissa = bits & ((1_u64 << 52) - 1);
    mantissa |= 1_u64 << 52;

    let magnitude_bits = if exponent >= 52 {
        let shift = u32::try_from(exponent - 52).ok()?;
        mantissa.checked_shl(shift)?
    } else {
        let shift = u32::try_from(52 - exponent).ok()?;
        let mask = (1_u64 << shift) - 1;
        if mantissa & mask != 0 {
            return None;
        }
        mantissa >> shift
    };

    let magnitude = i128::from(magnitude_bits);
    let signed = if sign { -magnitude } else { magnitude };

    i64::try_from(signed).ok()
}

const fn repeat_byte_usize(byte: u8) -> usize {
    let mut value = 0usize;
    let mut i = 0usize;
    while i < (usize::BITS / 8) as usize {
        value |= (byte as usize) << (i * 8);
        i += 1;
    }
    value
}

const USIZE_BYTES: usize = size_of::<usize>();
const SPACE_MASK_USIZE: usize = repeat_byte_usize(b' ');

#[inline]
fn is_blank(slice: &[u8]) -> bool {
    let mut chunks = slice.chunks_exact(USIZE_BYTES);
    for chunk in chunks.by_ref() {
        let word = usize::from_ne_bytes(chunk.try_into().unwrap());
        if word & !SPACE_MASK_USIZE != 0 {
            return false;
        }
    }
    chunks.remainder().iter().all(|&b| b == 0 || b == b' ')
}

#[derive(Clone, Copy)]
pub struct RuntimeColumnRef {
    pub index: u32,
    pub offset: usize,
    pub width: usize,
    pub kind: ColumnKind,
}

pub struct ColumnarBatch<'rows> {
    pub row_count: usize,
    row_slices: SmallVec<[&'rows [u8]; COLUMNAR_INLINE_ROWS]>,
    columns: &'rows [RuntimeColumnRef],
    endianness: Endianness,
}

impl<'rows> ColumnarBatch<'rows> {
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    #[must_use]
    fn new(
        row_slices: SmallVec<[&'rows [u8]; COLUMNAR_INLINE_ROWS]>,
        columns: &'rows [RuntimeColumnRef],
        endianness: Endianness,
    ) -> Self {
        let row_count = row_slices.len();
        Self {
            row_count,
            row_slices,
            columns,
            endianness,
        }
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    #[must_use]
    pub fn column(&self, index: usize) -> Option<ColumnarColumn<'_, 'rows>> {
        let column = self.columns.get(index)?;
        Some(ColumnarColumn {
            column,
            rows: self.row_slices.as_slice(),
            endianness: self.endianness,
        })
    }

    pub fn columns(&self) -> impl Iterator<Item = ColumnarColumn<'_, 'rows>> {
        self.columns.iter().map(move |column| ColumnarColumn {
            column,
            rows: self.row_slices.as_slice(),
            endianness: self.endianness,
        })
    }

    #[must_use]
    pub fn par_columns(&self) -> impl ParallelIterator<Item = ColumnarColumn<'_, 'rows>> {
        let rows = self.row_slices.as_slice();
        self.columns.par_iter().map(move |column| ColumnarColumn {
            column,
            rows,
            endianness: self.endianness,
        })
    }
}

pub struct ColumnarColumn<'batch, 'rows> {
    column: &'batch RuntimeColumnRef,
    rows: &'batch [&'rows [u8]],
    endianness: Endianness,
}

impl ColumnarColumn<'_, '_> {
    #[must_use]
    pub const fn index(&self) -> u32 {
        self.column.index
    }

    #[must_use]
    pub const fn kind(&self) -> ColumnKind {
        self.column.kind
    }

    fn row_slice(&self, row_index: usize) -> Option<&[u8]> {
        self.rows.get(row_index).copied()
    }

    #[inline]
    fn column_slice<'a>(&self, row: &'a [u8]) -> Option<&'a [u8]> {
        row.get(self.column.offset..self.column.offset + self.column.width)
    }

    pub fn iter_numeric_bits(&self) -> impl Iterator<Item = Option<u64>> + '_ {
        (0..self.rows.len()).map(move |idx| {
            self.row_slice(idx).and_then(|slice| {
                let data = slice.get(self.column.offset..self.column.offset + self.column.width)?;
                let bits = numeric_bits(data, self.endianness);
                if numeric_bits_is_missing(bits) {
                    None
                } else {
                    Some(bits)
                }
            })
        })
    }

    pub fn iter_character_bytes(&self) -> impl Iterator<Item = Option<&[u8]>> + '_ {
        (0..self.rows.len()).map(move |idx| {
            self.row_slice(idx).and_then(|slice| {
                slice
                    .get(self.column.offset..self.column.offset + self.column.width)
                    .filter(|data| !is_blank(data))
            })
        })
    }

    #[must_use]
    pub fn non_null_count(&self) -> u64 {
        match self.column.kind {
            ColumnKind::Numeric(_) => self.numeric_non_null_count(),
            ColumnKind::Character => self.character_non_null_count(),
        }
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn numeric_non_null_count(&self) -> u64 {
        self.rows
            .iter()
            .copied()
            .filter_map(|row| self.column_slice(row))
            .filter(|data| !numeric_bits_is_missing(numeric_bits(data, self.endianness)))
            .count() as u64
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn character_non_null_count(&self) -> u64 {
        self.rows
            .iter()
            .copied()
            .filter_map(|row| self.column_slice(row))
            .filter(|data| !is_blank(data))
            .count() as u64
    }
}

fn read_signature(data: &[u8], endian: Endianness, uses_u64: bool) -> u32 {
    if data.len() < 4 {
        return 0;
    }
    let mut signature = read_u32(endian, &data[0..4]);
    if matches!(endian, Endianness::Big) && signature == u32::MAX && uses_u64 && data.len() >= 8 {
        signature = read_u32(endian, &data[4..8]);
    }
    signature
}

const fn signature_is_recognized(signature: u32) -> bool {
    matches!(
        signature,
        SAS_SUBHEADER_SIGNATURE_COLUMN_TEXT
            | SAS_SUBHEADER_SIGNATURE_COLUMN_ATTRS
            | SAS_SUBHEADER_SIGNATURE_COLUMN_FORMAT
            | SAS_SUBHEADER_SIGNATURE_COLUMN_NAME
            | SAS_SUBHEADER_SIGNATURE_COLUMN_SIZE
            | SAS_SUBHEADER_SIGNATURE_ROW_SIZE
            | SAS_SUBHEADER_SIGNATURE_COUNTS
            | SAS_SUBHEADER_SIGNATURE_COLUMN_LIST
    )
}

#[allow(clippy::too_many_lines)]
fn decompress_rle(
    input: &[u8],
    expected_len: usize,
    output: &mut Vec<u8>,
) -> std::result::Result<(), &'static str> {
    const COMMAND_LENGTHS: [usize; 16] = [1, 1, 0, 0, 2, 1, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0];

    output.clear();
    output.resize(expected_len, 0);
    let buffer = output.as_mut_slice();
    let mut out_pos = 0usize;
    let mut i = 0usize;

    while i < input.len() {
        let control = input[i];
        i += 1;
        let command = (control >> 4) as usize;
        if command >= COMMAND_LENGTHS.len() {
            return Err("unknown RLE command");
        }
        let length_nibble = (control & 0x0F) as usize;
        if i + COMMAND_LENGTHS[command] > input.len() {
            return Err("RLE command exceeds input length");
        }

        let mut copy_len = 0usize;
        let mut insert_len = 0usize;
        let mut insert_byte = 0u8;

        match command {
            0 => {
                let next = input[i] as usize;
                i += 1;
                copy_len = next + 64 + length_nibble * 256;
            }
            1 => {
                let next = input[i] as usize;
                i += 1;
                copy_len = next + 64 + length_nibble * 256 + 4096;
            }
            2 => {
                copy_len = length_nibble + 96;
            }
            4 => {
                let next = input[i] as usize;
                i += 1;
                insert_len = next + 18 + length_nibble * 256;
                insert_byte = input[i];
                i += 1;
            }
            5 => {
                let next = input[i] as usize;
                i += 1;
                insert_len = next + 17 + length_nibble * 256;
                insert_byte = b'@';
            }
            6 => {
                let next = input[i] as usize;
                i += 1;
                insert_len = next + 17 + length_nibble * 256;
                insert_byte = b' ';
            }
            7 => {
                let next = input[i] as usize;
                i += 1;
                insert_len = next + 17 + length_nibble * 256;
                insert_byte = 0;
            }
            8 => {
                copy_len = length_nibble + 1;
            }
            9 => {
                copy_len = length_nibble + 17;
            }
            10 => {
                copy_len = length_nibble + 33;
            }
            11 => {
                copy_len = length_nibble + 49;
            }
            12 => {
                insert_byte = input[i];
                i += 1;
                insert_len = length_nibble + 3;
            }
            13 => {
                insert_len = length_nibble + 2;
                insert_byte = b'@';
            }
            14 => {
                insert_len = length_nibble + 2;
                insert_byte = b' ';
            }
            15 => {
                insert_len = length_nibble + 2;
                insert_byte = 0;
            }
            _ => {}
        }

        if copy_len > 0 {
            if out_pos + copy_len > expected_len {
                return Err("RLE copy exceeds output length");
            }
            if i + copy_len > input.len() {
                return Err("RLE copy exceeds input length");
            }
            buffer[out_pos..out_pos + copy_len].copy_from_slice(&input[i..i + copy_len]);
            i += copy_len;
            out_pos += copy_len;
        }

        if insert_len > 0 {
            if out_pos + insert_len > expected_len {
                return Err("RLE insert exceeds output length");
            }
            buffer[out_pos..out_pos + insert_len].fill(insert_byte);
            out_pos += insert_len;
        }
    }

    if out_pos != expected_len {
        return Err("RLE output length mismatch");
    }

    Ok(())
}

fn decompress_rdc(
    input: &[u8],
    expected_len: usize,
    output: &mut Vec<u8>,
) -> std::result::Result<(), &'static str> {
    output.clear();
    output.resize(expected_len, 0);
    let buffer = output.as_mut_slice();
    let mut out_pos = 0usize;
    let mut i = 0usize;
    while i + 2 <= input.len() {
        let prefix = u16::from_be_bytes([input[i], input[i + 1]]);
        i += 2;
        for bit in 0..16 {
            if (prefix & (1 << (15 - bit))) == 0 {
                if i >= input.len() {
                    break;
                }
                if out_pos >= expected_len {
                    return Err("RDC output overflow");
                }
                buffer[out_pos] = input[i];
                out_pos += 1;
                i += 1;
                continue;
            }

            if i + 2 > input.len() {
                return Err("RDC marker exceeds input");
            }
            let marker = input[i];
            let next = input[i + 1];
            i += 2;

            let mut insert_len = 0usize;
            let mut insert_byte = 0u8;
            let mut copy_len = 0usize;
            let mut back_offset = 0usize;

            if marker <= 0x0F {
                insert_len = 3 + marker as usize;
                insert_byte = next;
            } else if (marker >> 4) == 1 {
                if i >= input.len() {
                    return Err("RDC insert length exceeds input");
                }
                insert_len = 19 + (marker as usize & 0x0F) + (next as usize) * 16;
                insert_byte = input[i];
                i += 1;
            } else if (marker >> 4) == 2 {
                if i >= input.len() {
                    return Err("RDC copy length exceeds input");
                }
                copy_len = 16 + input[i] as usize;
                i += 1;
                back_offset = 3 + (marker as usize & 0x0F) + (next as usize) * 16;
            } else {
                copy_len = (marker >> 4) as usize;
                back_offset = 3 + (marker as usize & 0x0F) + (next as usize) * 16;
            }

            if insert_len > 0 {
                if out_pos + insert_len > expected_len {
                    return Err("RDC insert exceeds output length");
                }
                buffer[out_pos..out_pos + insert_len].fill(insert_byte);
                out_pos += insert_len;
            } else if copy_len > 0 {
                if back_offset == 0
                    || out_pos < back_offset
                    || copy_len > back_offset
                    || out_pos + copy_len > expected_len
                {
                    return Err("RDC copy invalid");
                }
                let start = out_pos - back_offset;
                for j in 0..copy_len {
                    let byte = buffer[start + j];
                    buffer[out_pos + j] = byte;
                }
                out_pos += copy_len;
            }
        }
    }

    if out_pos != expected_len {
        return Err("RDC output length mismatch");
    }
    Ok(())
}

fn sas_epoch() -> PrimitiveDateTime {
    PrimitiveDateTime::new(
        Date::from_calendar_date(1960, Month::January, 1).expect("valid SAS epoch"),
        Time::MIDNIGHT,
    )
}

fn sas_offset_datetime(seconds: f64) -> Option<OffsetDateTime> {
    if !seconds.is_finite() {
        return None;
    }
    let duration = Duration::seconds_f64(seconds.abs());
    if seconds >= 0.0 {
        sas_epoch()
            .checked_add(duration)
            .map(PrimitiveDateTime::assume_utc)
    } else {
        sas_epoch()
            .checked_sub(duration)
            .map(PrimitiveDateTime::assume_utc)
    }
}

fn sas_days_to_datetime(days: f64) -> Option<OffsetDateTime> {
    sas_offset_datetime(days * 86_400.0)
}

fn sas_seconds_to_datetime(seconds: f64) -> Option<OffsetDateTime> {
    sas_offset_datetime(seconds)
}

fn sas_seconds_to_time(seconds: f64) -> Option<Duration> {
    if !seconds.is_finite() {
        return None;
    }
    Some(Duration::seconds_f64(seconds))
}

struct PointerInfo {
    offset: usize,
    length: usize,
    compression: u8,
    is_compressed_data: bool,
}

fn parse_pointer(pointer: &[u8], uses_u64: bool, endian: Endianness) -> Result<PointerInfo> {
    if uses_u64 {
        if pointer.len() < 18 {
            return Err(Error::Corrupted {
                section: Section::Header,
                details: Cow::from("64-bit pointer too short"),
            });
        }
        let offset = usize::try_from(read_u64(endian, &pointer[0..8])).map_err(|_| {
            Error::Unsupported {
                feature: Cow::from("64-bit pointer offset exceeds platform pointer width"),
            }
        })?;
        let length = usize::try_from(read_u64(endian, &pointer[8..16])).map_err(|_| {
            Error::Unsupported {
                feature: Cow::from("64-bit pointer length exceeds platform pointer width"),
            }
        })?;
        Ok(PointerInfo {
            offset,
            length,
            compression: pointer[16],
            is_compressed_data: pointer[17] != 0,
        })
    } else {
        if pointer.len() < 10 {
            return Err(Error::Corrupted {
                section: Section::Header,
                details: Cow::from("32-bit pointer too short"),
            });
        }
        let offset = usize::try_from(read_u32(endian, &pointer[0..4])).map_err(|_| {
            Error::Unsupported {
                feature: Cow::from("32-bit pointer offset exceeds platform pointer width"),
            }
        })?;
        let length = usize::try_from(read_u32(endian, &pointer[4..8])).map_err(|_| {
            Error::Unsupported {
                feature: Cow::from("32-bit pointer length exceeds platform pointer width"),
            }
        })?;
        Ok(PointerInfo {
            offset,
            length,
            compression: pointer[8],
            is_compressed_data: pointer[9] != 0,
        })
    }
}

/// Creates a [`RowIterator`] for the provided reader and parsed metadata.
///
/// # Errors
///
/// Returns an error if the iterator cannot be constructed, for example when
/// the dataset uses unsupported compression.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub fn row_iterator<'a, R: Read + Seek>(
    reader: &'a mut R,
    parsed: &'a ParsedMetadata,
) -> Result<RowIterator<'a, R>> {
    RowIterator::new(reader, parsed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;
    use crate::parser::encoding::resolve_encoding;

    #[test]
    fn decode_respects_encoding_and_trimming() {
        let encoding = Encoding::for_label(b"windows-1252").unwrap();
        let text = decode_string(b"\xC9clair  ", encoding);
        assert_eq!(text, "Éclair");
    }

    #[test]
    fn blank_strings_preserve_empty_text() {
        assert_eq!(decode_string(b"   \0\0", UTF_8), Cow::Borrowed(""));
    }

    #[test]
    fn fixes_mojibake_sequences() {
        let encoding = Encoding::for_label(b"windows-1252").unwrap();
        let repaired = decode_string(b"\xE9\xAB\x98\xE9\x9B\x84\xE5\xB8\x82", encoding);
        assert_eq!(repaired, "高雄市");
    }

    #[test]
    fn resolves_mac_aliases() {
        let encoding = resolve_encoding(Some("MACCYRILLIC"));
        assert_eq!(encoding.name(), "x-mac-cyrillic");
    }
}
