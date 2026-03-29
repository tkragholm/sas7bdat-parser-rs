use crate::{
    columnar::{ColumnBuffer, ColumnarBatch, OwnedColumnBuffer, OwnedColumnarBatch},
    compression::decompress_row,
    dataset::Dataset,
    encoding::resolve_encoding,
    error::{Error, Result},
    internal::{FileSource, PageDescriptor, RowSpan, RowSpanKind},
    metadata::{ColumnMeta, Endianness, LogicalType, SasDate, SasDateTime, SasTime},
    options::{
        BatchHint, DecodeMode, MojibakePolicy, OrderingMode, Parallelism, RowSelection,
        StringDecodeOptions, TemporalDecodeOptions, Utf8ValidationMode,
    },
    projection::Projection,
    row::{OwnedRow, RawRow, RowView},
};
use encoding_rs::{Encoding, UTF_8};
use std::{
    fs::File,
    io::{Cursor, Read, Seek, SeekFrom},
    ops::ControlFlow,
    sync::Arc,
};

#[derive(Debug, Clone, Default)]
pub struct ScanStats {
    pub rows_seen: u64,
    pub rows_emitted: u64,
    pub pages_seen: u64,
    pub fused_pages: u64,
    pub indexed_pages: u64,
    pub compressed_pages: u64,
    pub raw_bytes_read: u64,
    pub row_bytes_materialized: u64,
    pub decode_batches: u64,
}

pub trait RawRowSink {
    fn push(&mut self, row: RawRow<'_>) -> Result<ControlFlow<()>>;
}

pub trait RowSink {
    fn push(&mut self, row: RowView<'_>) -> Result<ControlFlow<()>>;
}

pub trait BatchSink {
    fn push(&mut self, batch: ColumnarBatch<'_>) -> Result<ControlFlow<()>>;
}

#[derive(Debug, Clone)]
pub struct ScanBuilder<'a> {
    #[allow(dead_code)]
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
        }
    }

    #[must_use]
    pub fn with_projection(mut self, projection: &'a Projection) -> Self {
        self.projection = Some(projection);
        self
    }

    #[must_use]
    pub fn with_decode_mode(mut self, mode: DecodeMode) -> Self {
        self.decode = mode;
        self
    }

    #[must_use]
    pub fn with_string_options(mut self, options: StringDecodeOptions) -> Self {
        self.string_options = options;
        self
    }

    #[must_use]
    pub fn with_temporal_options(mut self, options: TemporalDecodeOptions) -> Self {
        self.temporal_options = options;
        self
    }

    #[must_use]
    pub fn with_ordering(mut self, mode: OrderingMode) -> Self {
        self.ordering = mode;
        self
    }

    #[must_use]
    pub fn with_parallelism(mut self, parallelism: Parallelism) -> Self {
        self.parallelism = parallelism;
        self
    }

    #[must_use]
    pub fn with_batch_hint(mut self, hint: BatchHint) -> Self {
        self.batch_hint = hint;
        self
    }

    #[must_use]
    pub fn limit(mut self, rows: u64) -> Self {
        self.row_limit = Some(rows);
        self
    }

    #[must_use]
    pub fn select(mut self, selection: RowSelection) -> Self {
        self.row_selection = selection;
        self
    }

    pub fn visit_raw_rows<F>(self, _f: F) -> Result<ScanStats>
    where
        F: FnMut(RawRow<'_>) -> Result<ControlFlow<()>>,
    {
        let mut f = _f;
        self.scan_raw_rows(&mut f)
    }

    pub fn visit_rows<F>(self, _f: F) -> Result<ScanStats>
    where
        F: FnMut(RowView<'_>) -> Result<ControlFlow<()>>,
    {
        let mut f = _f;
        self.scan_rows(&mut f)
    }

    pub fn visit_batches<F>(self, _f: F) -> Result<ScanStats>
    where
        F: FnMut(ColumnarBatch<'_>) -> Result<ControlFlow<()>>,
    {
        let mut f = _f;
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

    pub fn collect_rows(self) -> Result<Vec<OwnedRow>> {
        if matches!(self.decode, DecodeMode::Raw) {
            return Err(Error::unsupported(
                "collect_rows does not support DecodeMode::Raw; use visit_raw_rows instead",
            ));
        }

        let plan = RowDecodePlan::new(&self)?;
        let mut rows = Vec::new();
        let mut owned_strings = Vec::new();
        self.scan_raw_rows(&mut |raw| {
            let planned = plan.plan_cells(raw.bytes, &mut owned_strings)?;
            let mut cells = Vec::with_capacity(planned.len());
            for cell in &planned {
                cells.push(owned_cell_from_planned(*cell, &owned_strings)?);
            }
            rows.push(OwnedRow {
                row_index: raw.row_index,
                cells,
            });
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(rows)
    }

    pub fn collect_batches(self) -> Result<Vec<OwnedColumnarBatch>> {
        let mut batches = Vec::new();
        self.scan_batches(&mut |batch| {
            batches.push(batch);
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(batches)
    }

    pub fn write_raw_rows(self, _sink: &mut impl RawRowSink) -> Result<ScanStats> {
        self.visit_raw_rows(|row| _sink.push(row))
    }

    pub fn write_rows(self, _sink: &mut impl RowSink) -> Result<ScanStats> {
        self.visit_rows(|row| _sink.push(row))
    }

    pub fn write_batches(self, _sink: &mut impl BatchSink) -> Result<ScanStats> {
        self.visit_batches(|batch| _sink.push(batch))
    }
}

#[allow(dead_code)]
fn _keep_type_imports_alive<'a>(_columns: &'a [ColumnBuffer<'a>], _dataset: &'a Dataset) {}

impl<'a> ScanBuilder<'a> {
    fn scan_raw_rows<F>(self, f: &mut F) -> Result<ScanStats>
    where
        F: FnMut(RawRow<'_>) -> Result<ControlFlow<()>>,
    {
        let mut reader = open_scan_reader(self.ds)?;
        scan_raw_rows_with_reader(self, &mut reader, f)
    }

    fn scan_rows<F>(self, f: &mut F) -> Result<ScanStats>
    where
        F: FnMut(RowView<'_>) -> Result<ControlFlow<()>>,
    {
        if matches!(self.decode, DecodeMode::Raw) {
            return Err(Error::unsupported(
                "visit_rows does not support DecodeMode::Raw; use visit_raw_rows instead",
            ));
        }

        let plan = RowDecodePlan::new(&self)?;
        let mut owned_strings = Vec::new();
        self.scan_raw_rows(&mut |raw| {
            let planned = plan.plan_cells(raw.bytes, &mut owned_strings)?;
            let cells = materialize_planned_cells(&planned, &owned_strings)?;
            let row = RowView {
                row_index: raw.row_index,
                names: &plan.names,
                cells: &cells,
            };
            f(row)
        })
    }

    fn scan_batches<F>(self, f: &mut F) -> Result<ScanStats>
    where
        F: FnMut(OwnedColumnarBatch) -> Result<ControlFlow<()>>,
    {
        if matches!(self.decode, DecodeMode::Raw) {
            return Err(Error::unsupported(
                "visit_batches does not support DecodeMode::Raw",
            ));
        }

        let target_rows = resolve_batch_row_capacity(&self)?;
        let mut batcher = BatchAccumulator::new(BatchDecodePlan::new(&self)?, target_rows);
        let mut decode_batches = 0u64;
        let mut stop_after_current_batch = false;
        let mut reader = open_scan_reader(self.ds)?;

        let mut stats = scan_row_bytes_with_reader(self, &mut reader, &mut |row_index, bytes| {
            batcher.push_row(row_index, bytes)?;
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

fn scan_raw_rows_with_reader<R, F>(
    builder: ScanBuilder<'_>,
    reader: &mut R,
    f: &mut F,
) -> Result<ScanStats>
where
    R: Read + Seek,
    F: FnMut(RawRow<'_>) -> Result<ControlFlow<()>>,
{
    scan_row_bytes_with_reader(builder, reader, &mut |row_index, bytes| {
        f(RawRow { row_index, bytes })
    })
}

fn scan_row_bytes_with_reader<R, F>(
    builder: ScanBuilder<'_>,
    reader: &mut R,
    f: &mut F,
) -> Result<ScanStats>
where
    R: Read + Seek,
    F: FnMut(u64, &[u8]) -> Result<ControlFlow<()>>,
{
    let plan = RawScanPlan::compile(&builder)?;
    if plan.row_len == 0 {
        return Ok(ScanStats::default());
    }

    if builder.ds.layout.compression != crate::metadata::CompressionKind::None
        && builder.ds.metadata.row_count > 0
        && builder.ds.descriptors.total_candidate_rows == 0
    {
        return Err(Error::unsupported(
            "compressed dataset layout compiled no row producers; this compressed page layout is not implemented yet",
        ));
    }

    let mut stats = ScanStats::default();
    let mut page = vec![0u8; plan.page_size];
    let mut decompressed_row = Vec::new();

    for descriptor in builder.ds.descriptors.pages.iter().copied() {
        if plan.should_stop(&stats) {
            break;
        }

        stats.pages_seen = stats.pages_seen.saturating_add(1);
        match descriptor.exec_class {
            crate::internal::PageExecClass::FusedContiguousUncompressed => {
                stats.fused_pages = stats.fused_pages.saturating_add(1);
                load_descriptor_page(reader, &plan, descriptor, &mut page, &mut stats)?;
                if emit_contiguous_rows(&plan, descriptor, &page, &mut stats, f)? {
                    return Ok(stats);
                }
            }
            crate::internal::PageExecClass::MetadataOrEmpty => {}
            crate::internal::PageExecClass::IndexedPointerRows => {
                stats.indexed_pages = stats.indexed_pages.saturating_add(1);
                load_descriptor_page(reader, &plan, descriptor, &mut page, &mut stats)?;
                let spans = descriptor_spans(&builder, descriptor)?;
                if emit_indexed_rows(
                    &plan,
                    descriptor,
                    spans,
                    &page,
                    &mut decompressed_row,
                    &mut stats,
                    f,
                )? {
                    return Ok(stats);
                }
            }
            crate::internal::PageExecClass::IndexedCompressedRows => {
                stats.compressed_pages = stats.compressed_pages.saturating_add(1);
                load_descriptor_page(reader, &plan, descriptor, &mut page, &mut stats)?;
                let spans = descriptor_spans(&builder, descriptor)?;
                if emit_indexed_rows(
                    &plan,
                    descriptor,
                    spans,
                    &page,
                    &mut decompressed_row,
                    &mut stats,
                    f,
                )? {
                    return Ok(stats);
                }
            }
        }
    }

    Ok(stats)
}

#[derive(Debug, Clone, Copy)]
struct RawScanPlan {
    row_len: usize,
    page_size: usize,
    page_stride: u64,
    data_offset: u64,
    compression: crate::metadata::CompressionKind,
    row_limit: Option<u64>,
    row_selection: RowSelection,
}

impl RawScanPlan {
    fn compile(builder: &ScanBuilder<'_>) -> Result<Self> {
        let row_len = usize::try_from(builder.ds.layout.row_len)
            .map_err(|_| Error::unsupported("row length exceeds platform usize"))?;
        let page_size = usize::try_from(builder.ds.layout.header.page_size)
            .map_err(|_| Error::unsupported("page size exceeds platform usize"))?;
        Ok(Self {
            row_len,
            page_size,
            page_stride: u64::from(builder.ds.layout.header.page_size),
            data_offset: builder.ds.layout.header.data_offset,
            compression: builder.ds.layout.compression,
            row_limit: builder.row_limit,
            row_selection: builder.row_selection,
        })
    }

    fn should_stop(self, stats: &ScanStats) -> bool {
        self.row_limit
            .is_some_and(|limit| stats.rows_emitted >= limit)
    }

    fn page_offset(self, page_index: u64) -> u64 {
        self.data_offset + page_index * self.page_stride
    }
}

fn row_selected(selection: RowSelection, row_index: u64) -> bool {
    match selection {
        RowSelection::All => true,
        RowSelection::Range { start, end } => (start..end).contains(&row_index),
    }
}

fn prepare_row_visit(plan: &RawScanPlan, stats: &mut ScanStats, row_index: u64) -> bool {
    stats.rows_seen = stats.rows_seen.saturating_add(1);
    if !row_selected(plan.row_selection, row_index) {
        return false;
    }
    if let Some(limit) = plan.row_limit
        && stats.rows_emitted >= limit
    {
        return false;
    }
    true
}

fn finish_row_visit(stats: &mut ScanStats, flow: ControlFlow<()>) -> bool {
    stats.rows_emitted = stats.rows_emitted.saturating_add(1);
    matches!(flow, ControlFlow::Break(()))
}

fn load_descriptor_page<R: Read + Seek>(
    reader: &mut R,
    plan: &RawScanPlan,
    descriptor: PageDescriptor,
    page: &mut [u8],
    stats: &mut ScanStats,
) -> Result<()> {
    reader
        .seek(SeekFrom::Start(plan.page_offset(descriptor.page_index)))
        .map_err(scan_io_error)?;
    reader.read_exact(page).map_err(scan_io_error)?;
    stats.raw_bytes_read = stats
        .raw_bytes_read
        .saturating_add(u64::try_from(page.len()).unwrap_or(u64::MAX));
    Ok(())
}

fn descriptor_spans<'a>(
    builder: &'a ScanBuilder<'_>,
    descriptor: PageDescriptor,
) -> Result<&'a [RowSpan]> {
    let span_start = usize::try_from(descriptor.row_span_start)
        .map_err(|_| Error::unsupported("row span start exceeds platform usize"))?;
    let span_end = span_start
        .checked_add(
            usize::try_from(descriptor.row_span_count)
                .map_err(|_| Error::unsupported("row span count exceeds platform usize"))?,
        )
        .ok_or_else(|| Error::unsupported("row span range overflow"))?;
    builder
        .ds
        .descriptors
        .row_spans
        .get(span_start..span_end)
        .ok_or_else(|| Error::unsupported("row span range exceeds descriptor table"))
}

fn emit_contiguous_rows<F>(
    plan: &RawScanPlan,
    descriptor: PageDescriptor,
    page: &[u8],
    stats: &mut ScanStats,
    f: &mut F,
) -> Result<bool>
where
    F: FnMut(u64, &[u8]) -> Result<ControlFlow<()>>,
{
    let data_start = usize::try_from(descriptor.data_start)
        .map_err(|_| Error::unsupported("row data start exceeds platform usize"))?;
    for row_offset in 0..descriptor.row_count {
        let row_index = descriptor.row_base + u64::from(row_offset);
        if !prepare_row_visit(plan, stats, row_index) {
            continue;
        }

        let start = data_start
            .checked_add(
                usize::try_from(row_offset)
                    .unwrap_or(usize::MAX)
                    .saturating_mul(plan.row_len),
            )
            .ok_or_else(|| Error::unsupported("row offset overflow"))?;
        let end = start
            .checked_add(plan.row_len)
            .ok_or_else(|| Error::unsupported("row end overflow"))?;
        let Some(bytes) = page.get(start..end) else {
            return Err(Error::unsupported("row slice exceeds page bounds"));
        };

        if finish_row_visit(stats, f(row_index, bytes)?) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn emit_indexed_rows<F>(
    plan: &RawScanPlan,
    descriptor: PageDescriptor,
    spans: &[RowSpan],
    page: &[u8],
    decompressed_row: &mut Vec<u8>,
    stats: &mut ScanStats,
    f: &mut F,
) -> Result<bool>
where
    F: FnMut(u64, &[u8]) -> Result<ControlFlow<()>>,
{
    for (span_index, span) in spans.iter().enumerate() {
        let row_index = descriptor.row_base + u64::try_from(span_index).unwrap_or(u64::MAX);
        if !prepare_row_visit(plan, stats, row_index) {
            continue;
        }

        let start = usize::try_from(span.offset)
            .map_err(|_| Error::unsupported("row span offset exceeds platform usize"))?;
        let len = usize::try_from(span.len)
            .map_err(|_| Error::unsupported("row span length exceeds platform usize"))?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| Error::unsupported("row span end overflow"))?;
        let Some(raw_bytes) = page.get(start..end) else {
            return Err(Error::unsupported("row span exceeds page bounds"));
        };

        let bytes = match span.kind {
            RowSpanKind::Borrowed => raw_bytes,
            RowSpanKind::Compressed => {
                let decoded =
                    decompress_row(plan.compression, raw_bytes, plan.row_len, decompressed_row)?;
                stats.row_bytes_materialized = stats
                    .row_bytes_materialized
                    .saturating_add(u64::try_from(decoded.len()).unwrap_or(u64::MAX));
                decoded
            }
        };

        if finish_row_visit(stats, f(row_index, bytes)?) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn scan_io_error(err: std::io::Error) -> Error {
    Error::Io(crate::error::IoError {
        path: None,
        message: err.to_string(),
    })
}

enum ScanReader {
    File(File),
    Bytes(Cursor<Arc<[u8]>>),
}

impl Read for ScanReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::File(file) => file.read(buf),
            Self::Bytes(cursor) => cursor.read(buf),
        }
    }
}

impl Seek for ScanReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match self {
            Self::File(file) => file.seek(pos),
            Self::Bytes(cursor) => cursor.seek(pos),
        }
    }
}

fn open_scan_reader(ds: &Dataset) -> Result<ScanReader> {
    match &ds.file.source {
        FileSource::Path(path) => File::open(path).map(ScanReader::File).map_err(|err| {
            Error::Io(crate::error::IoError {
                path: Some(path.clone()),
                message: err.to_string(),
            })
        }),
        FileSource::Bytes(bytes) => Ok(ScanReader::Bytes(Cursor::new(Arc::clone(bytes)))),
    }
}

#[derive(Debug)]
struct RowDecodePlan {
    columns: Vec<CompiledColumnPlan>,
    names: Vec<String>,
    encoding: &'static Encoding,
    decode_mode: DecodeMode,
    string_options: StringDecodeOptions,
    temporal_options: TemporalDecodeOptions,
    endianness: Endianness,
}

#[derive(Debug, Clone, Copy)]
enum PlannedCell<'a> {
    Null,
    Int32(i32),
    Int64(i64),
    Float64(f64),
    StrBorrowed(&'a str),
    StrOwned(usize),
    Bytes(&'a [u8]),
    Date(SasDate),
    DateTime(SasDateTime),
    Time(SasTime),
}

#[derive(Debug, Clone, Copy)]
enum CompiledColumnKind {
    String,
    Bytes,
    Date,
    DateTime,
    Time,
    Integer,
    Float,
}

#[derive(Debug, Clone)]
struct CompiledColumnPlan {
    start: usize,
    end: usize,
    width: u32,
    kind: CompiledColumnKind,
}

#[derive(Debug, Clone, Copy)]
enum ColumnMaterializationKind {
    I32,
    I64,
    F64,
    Date,
    DateTime,
    Time,
    Utf8,
    RawBytes,
}

#[derive(Debug)]
struct BatchDecodePlan {
    row_plan: RowDecodePlan,
    column_kinds: Vec<ColumnMaterializationKind>,
}

#[derive(Debug)]
struct BatchAccumulator {
    plan: BatchDecodePlan,
    target_rows: usize,
    row_base: Option<u64>,
    row_count: usize,
    columns: Vec<OwnedBatchColumnBuilder>,
    owned_strings: Vec<String>,
}

#[derive(Debug)]
enum OwnedBatchColumnBuilder {
    I32 {
        values: Vec<i32>,
        valid: Option<Vec<u8>>,
    },
    I64 {
        values: Vec<i64>,
        valid: Option<Vec<u8>>,
    },
    F64 {
        values: Vec<f64>,
        valid: Option<Vec<u8>>,
    },
    Date {
        values: Vec<SasDate>,
        valid: Option<Vec<u8>>,
    },
    DateTime {
        values: Vec<SasDateTime>,
        valid: Option<Vec<u8>>,
    },
    Time {
        values: Vec<SasTime>,
        valid: Option<Vec<u8>>,
    },
    Utf8 {
        offsets: Vec<u32>,
        data: Vec<u8>,
        valid: Option<Vec<u8>>,
    },
    RawBytes {
        offsets: Vec<u32>,
        data: Vec<u8>,
        valid: Option<Vec<u8>>,
    },
}

impl RowDecodePlan {
    fn new(builder: &ScanBuilder<'_>) -> Result<Self> {
        let selected_columns: Vec<&ColumnMeta> = if let Some(projection) = builder.projection {
            projection
                .inner
                .columns
                .iter()
                .map(|index| {
                    builder.ds.layout.columns.get(*index).ok_or_else(|| {
                        Error::Projection(crate::error::ProjectionError {
                            message: format!("projection column index {index} is out of range"),
                        })
                    })
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            builder.ds.layout.columns.iter().collect()
        };

        let names = if let Some(projection) = builder.projection {
            projection.names.iter().cloned().collect()
        } else {
            selected_columns
                .iter()
                .map(|column| column.name.clone())
                .collect()
        };

        let encoding = resolve_encoding(builder.ds.metadata.encoding.as_deref());
        let columns = selected_columns
            .iter()
            .map(|column| compile_column_plan(column))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            columns,
            names,
            encoding,
            decode_mode: builder.decode,
            string_options: builder.string_options,
            temporal_options: builder.temporal_options,
            endianness: builder.ds.layout.header.endianness,
        })
    }

    fn plan_cells<'row>(
        &self,
        row: &'row [u8],
        owned_strings: &mut Vec<String>,
    ) -> Result<Vec<PlannedCell<'row>>> {
        let mut planned = Vec::with_capacity(self.columns.len());
        self.plan_cells_into(row, owned_strings, &mut planned)?;
        Ok(planned)
    }

    fn plan_cells_into<'row>(
        &self,
        row: &'row [u8],
        owned_strings: &mut Vec<String>,
        planned: &mut Vec<PlannedCell<'row>>,
    ) -> Result<()> {
        owned_strings.clear();
        planned.clear();
        if planned.capacity() < self.columns.len() {
            planned.reserve(self.columns.len() - planned.capacity());
        }
        for column in &self.columns {
            planned.push(self.plan_cell(row, column, owned_strings)?);
        }
        Ok(())
    }

    fn plan_cell<'row>(
        &self,
        row: &'row [u8],
        column: &CompiledColumnPlan,
        owned_strings: &mut Vec<String>,
    ) -> Result<PlannedCell<'row>> {
        let slice = row
            .get(column.start..column.end)
            .ok_or_else(|| Error::unsupported("column slice exceeds row bounds"))?;

        match column.kind {
            CompiledColumnKind::String => self.plan_string(slice, owned_strings),
            CompiledColumnKind::Bytes => Ok(PlannedCell::Bytes(slice)),
            CompiledColumnKind::Date => {
                self.plan_numeric_temporal(slice, column.width, TemporalKind::Date)
            }
            CompiledColumnKind::DateTime => {
                self.plan_numeric_temporal(slice, column.width, TemporalKind::DateTime)
            }
            CompiledColumnKind::Time => {
                self.plan_numeric_temporal(slice, column.width, TemporalKind::Time)
            }
            CompiledColumnKind::Integer | CompiledColumnKind::Float => {
                self.plan_numeric_value(slice, column.width)
            }
        }
    }

    fn plan_string<'row>(
        &self,
        slice: &'row [u8],
        owned_strings: &mut Vec<String>,
    ) -> Result<PlannedCell<'row>> {
        if matches!(self.decode_mode, DecodeMode::TypedLossless) {
            return Ok(PlannedCell::Bytes(slice));
        }

        let slice = if self.string_options.trim_fixed_width {
            trim_trailing_space_or_nul(slice)
        } else {
            slice
        };
        if slice.is_empty() {
            return Ok(PlannedCell::StrBorrowed(""));
        }

        if slice.is_ascii() {
            return Ok(PlannedCell::StrBorrowed(
                std::str::from_utf8(slice).expect("ASCII is valid UTF-8"),
            ));
        }

        if self.encoding == UTF_8 {
            match std::str::from_utf8(slice) {
                Ok(value) => return Ok(PlannedCell::StrBorrowed(value)),
                Err(_)
                    if matches!(
                        self.string_options.utf8_validation,
                        Utf8ValidationMode::Strict
                    ) =>
                {
                    return Err(Error::Decode(crate::error::DecodeError {
                        message: "invalid UTF-8 in fixed-width string cell".to_owned(),
                    }));
                }
                Err(_) => {
                    let repaired = maybe_fix_mojibake(
                        String::from_utf8_lossy(slice).into_owned(),
                        self.string_options.mojibake_fix,
                    );
                    owned_strings.push(repaired);
                    return Ok(PlannedCell::StrOwned(owned_strings.len() - 1));
                }
            }
        }

        let (decoded, had_errors) = self.encoding.decode_without_bom_handling(slice);
        if had_errors
            && matches!(
                self.string_options.utf8_validation,
                Utf8ValidationMode::Strict
            )
        {
            return Err(Error::Decode(crate::error::DecodeError {
                message: "string decode failed under strict validation".to_owned(),
            }));
        }

        match decoded {
            std::borrow::Cow::Borrowed(value) => Ok(PlannedCell::StrBorrowed(value)),
            std::borrow::Cow::Owned(value) => {
                owned_strings.push(maybe_fix_mojibake(value, self.string_options.mojibake_fix));
                Ok(PlannedCell::StrOwned(owned_strings.len() - 1))
            }
        }
    }

    fn plan_numeric_temporal<'row>(
        &self,
        slice: &[u8],
        width: u32,
        temporal_kind: TemporalKind,
    ) -> Result<PlannedCell<'row>> {
        match self.decode_mode {
            DecodeMode::Typed => match decode_numeric_cell(slice, self.endianness) {
                None => Ok(PlannedCell::Null),
                Some(number) => match temporal_kind {
                    TemporalKind::Date if self.temporal_options.decode_dates => {
                        if let Some(days) = try_i32_from_f64(number) {
                            Ok(PlannedCell::Date(SasDate {
                                days_since_sas_epoch: days,
                            }))
                        } else {
                            self.plan_numeric_value(slice, width)
                        }
                    }
                    TemporalKind::DateTime if self.temporal_options.decode_datetimes => {
                        if let Some(seconds) = try_i64_from_f64(number) {
                            Ok(PlannedCell::DateTime(SasDateTime {
                                seconds_since_sas_epoch: seconds,
                            }))
                        } else {
                            self.plan_numeric_value(slice, width)
                        }
                    }
                    TemporalKind::Time if self.temporal_options.decode_times => {
                        if let Some(seconds) = try_i64_from_f64(number) {
                            Ok(PlannedCell::Time(SasTime {
                                seconds_since_midnight: seconds,
                            }))
                        } else {
                            self.plan_numeric_value(slice, width)
                        }
                    }
                    _ => self.plan_numeric_value(slice, width),
                },
            },
            DecodeMode::TypedLossless => self.plan_numeric_lossless(slice),
            DecodeMode::Raw => Err(Error::unsupported(
                "visit_rows does not support DecodeMode::Raw; use visit_raw_rows instead",
            )),
        }
    }

    fn plan_numeric_value<'row>(&self, slice: &[u8], width: u32) -> Result<PlannedCell<'row>> {
        match self.decode_mode {
            DecodeMode::Typed => match decode_numeric_cell(slice, self.endianness) {
                None => Ok(PlannedCell::Null),
                Some(number) => {
                    if let Some(value) = try_i64_from_f64(number) {
                        if width <= 4 {
                            if let Ok(value32) = i32::try_from(value) {
                                return Ok(PlannedCell::Int32(value32));
                            }
                        }
                        return Ok(PlannedCell::Int64(value));
                    }
                    Ok(PlannedCell::Float64(number))
                }
            },
            DecodeMode::TypedLossless => self.plan_numeric_lossless(slice),
            DecodeMode::Raw => Err(Error::unsupported(
                "visit_rows does not support DecodeMode::Raw; use visit_raw_rows instead",
            )),
        }
    }

    fn plan_numeric_lossless<'row>(&self, slice: &[u8]) -> Result<PlannedCell<'row>> {
        if slice.is_empty() {
            return Ok(PlannedCell::Null);
        }
        Ok(PlannedCell::Float64(f64::from_bits(numeric_bits(
            slice,
            self.endianness,
        ))))
    }
}

impl BatchDecodePlan {
    fn new(builder: &ScanBuilder<'_>) -> Result<Self> {
        let row_plan = RowDecodePlan::new(builder)?;
        let column_kinds = row_plan
            .columns
            .iter()
            .map(|column| {
                column_materialization_kind(
                    column.kind,
                    column.width,
                    row_plan.temporal_options,
                    row_plan.decode_mode,
                )
            })
            .collect();
        Ok(Self {
            row_plan,
            column_kinds,
        })
    }
}

impl BatchAccumulator {
    fn new(plan: BatchDecodePlan, target_rows: usize) -> Self {
        let columns = plan
            .row_plan
            .columns
            .iter()
            .zip(plan.column_kinds.iter().copied())
            .map(|(column, kind)| {
                OwnedBatchColumnBuilder::with_capacity_hint(kind, target_rows, column.width)
            })
            .collect();
        Self {
            plan,
            target_rows: target_rows.max(1),
            row_base: None,
            row_count: 0,
            columns,
            owned_strings: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    fn is_full(&self) -> bool {
        self.row_count >= self.target_rows
    }

    fn push_row(&mut self, row_index: u64, row: &[u8]) -> Result<()> {
        if self.row_base.is_none() {
            self.row_base = Some(row_index);
        }

        self.owned_strings.clear();
        for (batch_column, column) in self.columns.iter_mut().zip(&self.plan.row_plan.columns) {
            if append_batch_fast_path(&self.plan.row_plan, batch_column, column, row)? {
                continue;
            }

            let cell = self
                .plan
                .row_plan
                .plan_cell(row, column, &mut self.owned_strings)?;
            batch_column.append(cell, &self.owned_strings)?;
        }
        self.row_count += 1;
        Ok(())
    }

    fn take_batch(&mut self) -> OwnedColumnarBatch {
        let row_base = self.row_base.unwrap_or(0);
        let row_count = self.row_count;
        let columns = std::mem::take(&mut self.columns)
            .into_iter()
            .map(OwnedBatchColumnBuilder::finish)
            .collect();
        self.row_base = None;
        self.row_count = 0;
        OwnedColumnarBatch {
            row_base,
            row_count,
            columns,
        }
    }

    fn reset_after_flush(&mut self) {
        self.columns = self
            .plan
            .row_plan
            .columns
            .iter()
            .zip(self.plan.column_kinds.iter().copied())
            .map(|(column, kind)| {
                OwnedBatchColumnBuilder::with_capacity_hint(kind, self.target_rows, column.width)
            })
            .collect();
        self.owned_strings.clear();
    }
}

impl OwnedBatchColumnBuilder {
    fn with_capacity_hint(
        kind: ColumnMaterializationKind,
        target_rows: usize,
        width_hint: u32,
    ) -> Self {
        let variable_capacity =
            target_rows.saturating_mul(usize::try_from(width_hint).unwrap_or(0));
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
                offsets: Vec::with_capacity(target_rows.saturating_add(1)),
                data: Vec::with_capacity(variable_capacity),
                valid: None,
            },
            ColumnMaterializationKind::RawBytes => Self::RawBytes {
                offsets: Vec::with_capacity(target_rows.saturating_add(1)),
                data: Vec::with_capacity(variable_capacity),
                valid: None,
            },
        }
        .with_initial_offset()
    }

    fn with_initial_offset(mut self) -> Self {
        match &mut self {
            Self::Utf8 { offsets, .. } | Self::RawBytes { offsets, .. } => offsets.push(0),
            _ => {}
        }
        self
    }

    fn append_integer_fast(&mut self, number: Option<f64>) -> Result<bool> {
        match self {
            Self::I32 { values, valid } => match number {
                None => {
                    push_primitive_null(values, valid, 0);
                    Ok(true)
                }
                Some(value) => {
                    if let Some(value32) = try_i32_from_f64(value) {
                        push_primitive_valid(values, valid, value32);
                        Ok(true)
                    } else {
                        self.widen_integer_to_f64();
                        self.append_f64_fast(number)
                    }
                }
            },
            Self::I64 { values, valid } => match number {
                None => {
                    push_primitive_null(values, valid, 0);
                    Ok(true)
                }
                Some(value) => {
                    if let Some(value64) = try_i64_from_f64(value) {
                        push_primitive_valid(values, valid, value64);
                        Ok(true)
                    } else {
                        self.widen_integer_to_f64();
                        self.append_f64_fast(number)
                    }
                }
            },
            Self::F64 { .. } => self.append_f64_fast(number),
            _ => Ok(false),
        }
    }

    fn append_f64_fast(&mut self, number: Option<f64>) -> Result<bool> {
        match self {
            Self::F64 { values, valid } => {
                match number {
                    None => push_primitive_null(values, valid, 0.0),
                    Some(value) => push_primitive_valid(values, valid, value),
                }
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn append_date_fast(&mut self, number: Option<f64>) -> Result<bool> {
        match self {
            Self::Date { values, valid } => match number {
                None => {
                    push_primitive_null(
                        values,
                        valid,
                        SasDate {
                            days_since_sas_epoch: 0,
                        },
                    );
                    Ok(true)
                }
                Some(value) => {
                    if let Some(days) = try_i32_from_f64(value) {
                        push_primitive_valid(
                            values,
                            valid,
                            SasDate {
                                days_since_sas_epoch: days,
                            },
                        );
                        Ok(true)
                    } else {
                        self.widen_temporal_to_f64();
                        self.append_f64_fast(number)
                    }
                }
            },
            Self::F64 { .. } => self.append_f64_fast(number),
            _ => Ok(false),
        }
    }

    fn append_datetime_fast(&mut self, number: Option<f64>) -> Result<bool> {
        match self {
            Self::DateTime { values, valid } => match number {
                None => {
                    push_primitive_null(
                        values,
                        valid,
                        SasDateTime {
                            seconds_since_sas_epoch: 0,
                        },
                    );
                    Ok(true)
                }
                Some(value) => {
                    if let Some(seconds) = try_i64_from_f64(value) {
                        push_primitive_valid(
                            values,
                            valid,
                            SasDateTime {
                                seconds_since_sas_epoch: seconds,
                            },
                        );
                        Ok(true)
                    } else {
                        self.widen_temporal_to_f64();
                        self.append_f64_fast(number)
                    }
                }
            },
            Self::F64 { .. } => self.append_f64_fast(number),
            _ => Ok(false),
        }
    }

    fn append_time_fast(&mut self, number: Option<f64>) -> Result<bool> {
        match self {
            Self::Time { values, valid } => match number {
                None => {
                    push_primitive_null(
                        values,
                        valid,
                        SasTime {
                            seconds_since_midnight: 0,
                        },
                    );
                    Ok(true)
                }
                Some(value) => {
                    if let Some(seconds) = try_i64_from_f64(value) {
                        push_primitive_valid(
                            values,
                            valid,
                            SasTime {
                                seconds_since_midnight: seconds,
                            },
                        );
                        Ok(true)
                    } else {
                        self.widen_temporal_to_f64();
                        self.append_f64_fast(number)
                    }
                }
            },
            Self::F64 { .. } => self.append_f64_fast(number),
            _ => Ok(false),
        }
    }

    fn append(&mut self, cell: PlannedCell<'_>, owned_strings: &[String]) -> Result<()> {
        match self {
            Self::I32 { values, valid } => match cell {
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
                    if let Some(value32) = try_i32_from_f64(value) {
                        push_primitive_valid(values, valid, value32);
                    } else {
                        self.widen_integer_to_f64();
                        return self.append(PlannedCell::Float64(value), owned_strings);
                    }
                }
                other => return Err(unexpected_batch_cell("i32", other)),
            },
            Self::I64 { values, valid } => match cell {
                PlannedCell::Null => push_primitive_null(values, valid, 0),
                PlannedCell::Int32(value) => push_primitive_valid(values, valid, i64::from(value)),
                PlannedCell::Int64(value) => push_primitive_valid(values, valid, value),
                PlannedCell::Float64(value) => {
                    if let Some(value64) = try_i64_from_f64(value) {
                        push_primitive_valid(values, valid, value64);
                    } else {
                        self.widen_integer_to_f64();
                        return self.append(PlannedCell::Float64(value), owned_strings);
                    }
                }
                other => return Err(unexpected_batch_cell("i64", other)),
            },
            Self::F64 { values, valid } => match cell {
                PlannedCell::Null => push_primitive_null(values, valid, 0.0),
                PlannedCell::Int32(value) => push_primitive_valid(values, valid, f64::from(value)),
                PlannedCell::Int64(value) => push_primitive_valid(values, valid, value as f64),
                PlannedCell::Float64(value) => push_primitive_valid(values, valid, value),
                other => return Err(unexpected_batch_cell("f64", other)),
            },
            Self::Date { values, valid } => match cell {
                PlannedCell::Null => push_primitive_null(
                    values,
                    valid,
                    SasDate {
                        days_since_sas_epoch: 0,
                    },
                ),
                PlannedCell::Date(value) => push_primitive_valid(values, valid, value),
                PlannedCell::Int32(value) => {
                    self.widen_temporal_to_f64();
                    return self.append(PlannedCell::Int32(value), owned_strings);
                }
                PlannedCell::Int64(value) => {
                    self.widen_temporal_to_f64();
                    return self.append(PlannedCell::Int64(value), owned_strings);
                }
                PlannedCell::Float64(value) => {
                    self.widen_temporal_to_f64();
                    return self.append(PlannedCell::Float64(value), owned_strings);
                }
                other => return Err(unexpected_batch_cell("date", other)),
            },
            Self::DateTime { values, valid } => match cell {
                PlannedCell::Null => push_primitive_null(
                    values,
                    valid,
                    SasDateTime {
                        seconds_since_sas_epoch: 0,
                    },
                ),
                PlannedCell::DateTime(value) => push_primitive_valid(values, valid, value),
                PlannedCell::Int32(value) => {
                    self.widen_temporal_to_f64();
                    return self.append(PlannedCell::Int32(value), owned_strings);
                }
                PlannedCell::Int64(value) => {
                    self.widen_temporal_to_f64();
                    return self.append(PlannedCell::Int64(value), owned_strings);
                }
                PlannedCell::Float64(value) => {
                    self.widen_temporal_to_f64();
                    return self.append(PlannedCell::Float64(value), owned_strings);
                }
                other => return Err(unexpected_batch_cell("datetime", other)),
            },
            Self::Time { values, valid } => match cell {
                PlannedCell::Null => push_primitive_null(
                    values,
                    valid,
                    SasTime {
                        seconds_since_midnight: 0,
                    },
                ),
                PlannedCell::Time(value) => push_primitive_valid(values, valid, value),
                PlannedCell::Int32(value) => {
                    self.widen_temporal_to_f64();
                    return self.append(PlannedCell::Int32(value), owned_strings);
                }
                PlannedCell::Int64(value) => {
                    self.widen_temporal_to_f64();
                    return self.append(PlannedCell::Int64(value), owned_strings);
                }
                PlannedCell::Float64(value) => {
                    self.widen_temporal_to_f64();
                    return self.append(PlannedCell::Float64(value), owned_strings);
                }
                other => return Err(unexpected_batch_cell("time", other)),
            },
            Self::Utf8 {
                offsets,
                data,
                valid,
            } => match cell {
                PlannedCell::Null => push_variable_null(offsets, data, valid),
                PlannedCell::StrBorrowed(value) => {
                    push_variable_valid(offsets, data, valid, value.as_bytes())?
                }
                PlannedCell::StrOwned(index) => push_variable_valid(
                    offsets,
                    data,
                    valid,
                    owned_strings
                        .get(index)
                        .ok_or_else(|| Error::unsupported("owned string index out of range"))?
                        .as_bytes(),
                )?,
                other => return Err(unexpected_batch_cell("utf8", other)),
            },
            Self::RawBytes {
                offsets,
                data,
                valid,
            } => match cell {
                PlannedCell::Null => push_variable_null(offsets, data, valid),
                PlannedCell::Bytes(value) => push_variable_valid(offsets, data, valid, value)?,
                other => return Err(unexpected_batch_cell("raw-bytes", other)),
            },
        }
        Ok(())
    }

    fn widen_temporal_to_f64(&mut self) {
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
                    .map(|value| value.days_since_sas_epoch as f64)
                    .collect(),
                valid,
            },
            Self::DateTime { values, valid } => Self::F64 {
                values: values
                    .into_iter()
                    .map(|value| value.seconds_since_sas_epoch as f64)
                    .collect(),
                valid,
            },
            Self::Time { values, valid } => Self::F64 {
                values: values
                    .into_iter()
                    .map(|value| value.seconds_since_midnight as f64)
                    .collect(),
                valid,
            },
            other => other,
        };
        *self = widened;
    }

    fn widen_integer_to_f64(&mut self) {
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
                values: values.into_iter().map(|value| value as f64).collect(),
                valid,
            },
            other => other,
        };
        *self = widened;
    }

    fn finish(self) -> OwnedColumnBuffer {
        match self {
            Self::I32 { values, valid } => OwnedColumnBuffer::I32 { values, valid },
            Self::I64 { values, valid } => OwnedColumnBuffer::I64 { values, valid },
            Self::F64 { values, valid } => OwnedColumnBuffer::F64 { values, valid },
            Self::Date { values, valid } => OwnedColumnBuffer::Date { values, valid },
            Self::DateTime { values, valid } => OwnedColumnBuffer::DateTime { values, valid },
            Self::Time { values, valid } => OwnedColumnBuffer::Time { values, valid },
            Self::Utf8 {
                offsets,
                data,
                valid,
            } => OwnedColumnBuffer::Utf8 {
                offsets,
                data,
                valid,
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

fn append_batch_fast_path(
    row_plan: &RowDecodePlan,
    batch_column: &mut OwnedBatchColumnBuilder,
    column: &CompiledColumnPlan,
    row: &[u8],
) -> Result<bool> {
    let slice = row
        .get(column.start..column.end)
        .ok_or_else(|| Error::unsupported("column slice exceeds row bounds"))?;

    match (column.kind, batch_column) {
        (CompiledColumnKind::Integer, batch_column) => {
            let number = decode_numeric_cell(slice, row_plan.endianness);
            batch_column.append_integer_fast(number)
        }
        (CompiledColumnKind::Float, batch_column) => {
            let number = decode_numeric_cell(slice, row_plan.endianness);
            batch_column.append_f64_fast(number)
        }
        (CompiledColumnKind::Date, batch_column) => {
            let number = decode_numeric_cell(slice, row_plan.endianness);
            batch_column.append_date_fast(number)
        }
        (CompiledColumnKind::DateTime, batch_column) => {
            let number = decode_numeric_cell(slice, row_plan.endianness);
            batch_column.append_datetime_fast(number)
        }
        (CompiledColumnKind::Time, batch_column) => {
            let number = decode_numeric_cell(slice, row_plan.endianness);
            batch_column.append_time_fast(number)
        }
        (
            CompiledColumnKind::Bytes,
            OwnedBatchColumnBuilder::RawBytes {
                offsets,
                data,
                valid,
            },
        ) => {
            push_variable_valid(offsets, data, valid, slice)?;
            Ok(true)
        }
        (
            CompiledColumnKind::String,
            OwnedBatchColumnBuilder::RawBytes {
                offsets,
                data,
                valid,
            },
        ) if matches!(row_plan.decode_mode, DecodeMode::TypedLossless) => {
            push_variable_valid(offsets, data, valid, slice)?;
            Ok(true)
        }
        (
            CompiledColumnKind::String,
            OwnedBatchColumnBuilder::Utf8 {
                offsets,
                data,
                valid,
            },
        ) => {
            let slice = if row_plan.string_options.trim_fixed_width {
                trim_trailing_space_or_nul(slice)
            } else {
                slice
            };

            if slice.is_ascii() {
                push_variable_valid(offsets, data, valid, slice)?;
                return Ok(true);
            }

            if row_plan.encoding == UTF_8 && std::str::from_utf8(slice).is_ok() {
                push_variable_valid(offsets, data, valid, slice)?;
                return Ok(true);
            }

            Ok(false)
        }
        _ => Ok(false),
    }
}

fn materialize_planned_cells<'a>(
    planned: &[PlannedCell<'a>],
    owned_strings: &'a [String],
) -> Result<Vec<crate::row::CellValue<'a>>> {
    let mut cells = Vec::with_capacity(planned.len());
    for cell in planned {
        cells.push(match *cell {
            PlannedCell::Null => crate::row::CellValue::Null,
            PlannedCell::Int32(value) => crate::row::CellValue::Int32(value),
            PlannedCell::Int64(value) => crate::row::CellValue::Int64(value),
            PlannedCell::Float64(value) => crate::row::CellValue::Float64(value),
            PlannedCell::StrBorrowed(value) => crate::row::CellValue::Str(value),
            PlannedCell::StrOwned(index) => crate::row::CellValue::Str(
                owned_strings
                    .get(index)
                    .ok_or_else(|| Error::unsupported("owned string index out of range"))?
                    .as_str(),
            ),
            PlannedCell::Bytes(value) => crate::row::CellValue::Bytes(value),
            PlannedCell::Date(value) => crate::row::CellValue::Date(value),
            PlannedCell::DateTime(value) => crate::row::CellValue::DateTime(value),
            PlannedCell::Time(value) => crate::row::CellValue::Time(value),
        });
    }
    Ok(cells)
}

fn owned_cell_from_planned(
    cell: PlannedCell<'_>,
    owned_strings: &[String],
) -> Result<crate::row::OwnedCellValue> {
    Ok(match cell {
        PlannedCell::Null => crate::row::OwnedCellValue::Null,
        PlannedCell::Int32(value) => crate::row::OwnedCellValue::Int32(value),
        PlannedCell::Int64(value) => crate::row::OwnedCellValue::Int64(value),
        PlannedCell::Float64(value) => crate::row::OwnedCellValue::Float64(value),
        PlannedCell::StrBorrowed(value) => crate::row::OwnedCellValue::String(value.to_owned()),
        PlannedCell::StrOwned(index) => crate::row::OwnedCellValue::String(
            owned_strings
                .get(index)
                .ok_or_else(|| Error::unsupported("owned string index out of range"))?
                .clone(),
        ),
        PlannedCell::Bytes(value) => crate::row::OwnedCellValue::Bytes(value.to_vec()),
        PlannedCell::Date(value) => crate::row::OwnedCellValue::Date(value),
        PlannedCell::DateTime(value) => crate::row::OwnedCellValue::DateTime(value),
        PlannedCell::Time(value) => crate::row::OwnedCellValue::Time(value),
    })
}

fn column_materialization_kind(
    column_kind: CompiledColumnKind,
    width: u32,
    temporal_options: TemporalDecodeOptions,
    decode_mode: DecodeMode,
) -> ColumnMaterializationKind {
    if matches!(decode_mode, DecodeMode::TypedLossless) {
        return match column_kind {
            CompiledColumnKind::String | CompiledColumnKind::Bytes => {
                ColumnMaterializationKind::RawBytes
            }
            CompiledColumnKind::Integer
            | CompiledColumnKind::Float
            | CompiledColumnKind::Date
            | CompiledColumnKind::DateTime
            | CompiledColumnKind::Time => ColumnMaterializationKind::F64,
        };
    }

    match column_kind {
        CompiledColumnKind::Integer => {
            if width <= 4 {
                ColumnMaterializationKind::I32
            } else {
                ColumnMaterializationKind::I64
            }
        }
        CompiledColumnKind::Float => ColumnMaterializationKind::F64,
        CompiledColumnKind::String => ColumnMaterializationKind::Utf8,
        CompiledColumnKind::Bytes => ColumnMaterializationKind::RawBytes,
        CompiledColumnKind::Date if temporal_options.decode_dates => {
            ColumnMaterializationKind::Date
        }
        CompiledColumnKind::DateTime if temporal_options.decode_datetimes => {
            ColumnMaterializationKind::DateTime
        }
        CompiledColumnKind::Time if temporal_options.decode_times => {
            ColumnMaterializationKind::Time
        }
        CompiledColumnKind::Date | CompiledColumnKind::DateTime | CompiledColumnKind::Time => {
            ColumnMaterializationKind::F64
        }
    }
}

fn compile_column_plan(column: &ColumnMeta) -> Result<CompiledColumnPlan> {
    let start = usize::try_from(column.offset)
        .map_err(|_| Error::unsupported("column offset exceeds platform usize"))?;
    let width = column.physical_width;
    let width_usize = usize::try_from(width)
        .map_err(|_| Error::unsupported("column width exceeds platform usize"))?;
    let end = start
        .checked_add(width_usize)
        .ok_or_else(|| Error::unsupported("column end overflow"))?;
    let kind = match column.logical_type {
        LogicalType::String => CompiledColumnKind::String,
        LogicalType::Bytes => CompiledColumnKind::Bytes,
        LogicalType::Date => CompiledColumnKind::Date,
        LogicalType::DateTime => CompiledColumnKind::DateTime,
        LogicalType::Time => CompiledColumnKind::Time,
        LogicalType::Integer => CompiledColumnKind::Integer,
        LogicalType::Float => CompiledColumnKind::Float,
    };
    Ok(CompiledColumnPlan {
        start,
        end,
        width,
        kind,
    })
}

fn resolve_batch_row_capacity(builder: &ScanBuilder<'_>) -> Result<usize> {
    match builder.batch_hint {
        BatchHint::Rows(rows) => Ok(rows.max(1)),
        BatchHint::Bytes(bytes) => {
            let row_len = usize::try_from(builder.ds.layout.row_len)
                .map_err(|_| Error::unsupported("row length exceeds platform usize"))?;
            Ok((bytes / row_len.max(1)).max(1))
        }
        BatchHint::Auto => {
            let rows_per_page = usize::try_from(builder.ds.layout.rows_per_page)
                .map_err(|_| Error::unsupported("rows per page exceeds platform usize"))?;
            Ok(rows_per_page.max(1))
        }
    }
}

fn borrow_column_buffers(columns: &[OwnedColumnBuffer]) -> Vec<ColumnBuffer<'_>> {
    columns.iter().map(OwnedColumnBuffer::as_borrowed).collect()
}

fn push_primitive_valid<T>(values: &mut Vec<T>, valid: &mut Option<Vec<u8>>, value: T) {
    values.push(value);
    if let Some(valid) = valid {
        valid.push(1);
    }
}

fn push_primitive_null<T: Copy>(values: &mut Vec<T>, valid: &mut Option<Vec<u8>>, default: T) {
    if valid.is_none() {
        *valid = Some(vec![1; values.len()]);
    }
    values.push(default);
    valid.as_mut().expect("validity initialized").push(0);
}

fn push_variable_valid(
    offsets: &mut Vec<u32>,
    data: &mut Vec<u8>,
    valid: &mut Option<Vec<u8>>,
    value: &[u8],
) -> Result<()> {
    data.extend_from_slice(value);
    let next_offset = u32::try_from(data.len())
        .map_err(|_| Error::unsupported("columnar variable buffer exceeds u32 offset range"))?;
    offsets.push(next_offset);
    if let Some(valid) = valid {
        valid.push(1);
    }
    Ok(())
}

fn push_variable_null(offsets: &mut Vec<u32>, _data: &mut Vec<u8>, valid: &mut Option<Vec<u8>>) {
    if valid.is_none() {
        *valid = Some(vec![1; offsets.len().saturating_sub(1)]);
    }
    let last = *offsets.last().unwrap_or(&0);
    offsets.push(last);
    valid.as_mut().expect("validity initialized").push(0);
}

fn unexpected_batch_cell(expected: &str, actual: PlannedCell<'_>) -> Error {
    Error::Decode(crate::error::DecodeError {
        message: format!(
            "columnar decode expected {expected} cell but saw {:?}",
            actual
        ),
    })
}

#[derive(Debug, Clone, Copy)]
enum TemporalKind {
    Date,
    DateTime,
    Time,
}

fn decode_numeric_cell(slice: &[u8], endianness: Endianness) -> Option<f64> {
    if slice.is_empty() {
        return None;
    }
    let raw = numeric_bits(slice, endianness);
    if numeric_bits_is_missing(raw) {
        None
    } else {
        Some(f64::from_bits(raw))
    }
}

fn numeric_bits(slice: &[u8], endianness: Endianness) -> u64 {
    debug_assert!(slice.len() <= 8);
    if slice.len() == 8 {
        let bytes: [u8; 8] = slice.try_into().expect("len == 8");
        match endianness {
            Endianness::Little => u64::from_le_bytes(bytes),
            Endianness::Big => u64::from_be_bytes(bytes),
        }
    } else {
        let mut buf = [0u8; 8];
        match endianness {
            Endianness::Big => {
                buf[..slice.len()].copy_from_slice(slice);
            }
            Endianness::Little => {
                buf[..slice.len()].copy_from_slice(slice);
                buf[..slice.len()].reverse();
            }
        }
        u64::from_be_bytes(buf)
    }
}

const fn numeric_bits_is_missing(raw: u64) -> bool {
    const EXP_MASK: u64 = 0x7FF0_0000_0000_0000;
    const FRACTION_MASK: u64 = 0x000F_FFFF_FFFF_FFFF;
    (raw & EXP_MASK) == EXP_MASK && (raw & FRACTION_MASK) != 0
}

fn try_i64_from_f64(number: f64) -> Option<i64> {
    if !number.is_finite() || number.fract() != 0.0 {
        return None;
    }
    if number < i64::MIN as f64 || number > i64::MAX as f64 {
        return None;
    }
    Some(number as i64)
}

fn try_i32_from_f64(number: f64) -> Option<i32> {
    let value = try_i64_from_f64(number)?;
    i32::try_from(value).ok()
}

fn trim_trailing_space_or_nul(slice: &[u8]) -> &[u8] {
    let mut end = slice.len();
    while end > 0 {
        let byte = slice[end - 1];
        if byte != b' ' && byte != 0 {
            break;
        }
        end -= 1;
    }
    &slice[..end]
}

fn maybe_fix_mojibake(value: String, policy: MojibakePolicy) -> String {
    if !matches!(policy, MojibakePolicy::Auto) || value.is_ascii() {
        return value;
    }
    if !(value.contains("Ã") || value.contains("Â")) {
        return value;
    }
    let mut bytes = Vec::with_capacity(value.len());
    for ch in value.chars() {
        let code = u32::from(ch);
        let Ok(byte) = u8::try_from(code) else {
            return value;
        };
        bytes.push(byte);
    }
    match std::str::from_utf8(&bytes) {
        Ok(decoded) if decoded != value => decoded.to_owned(),
        _ => value,
    }
}

#[cfg(test)]
mod tests {
    use super::ScanBuilder;
    use crate::{
        columnar::OwnedColumnBuffer,
        dataset::Dataset,
        internal::{FileInner, FileSource, HeaderInfo, LayoutPlan},
        metadata::{ColumnMeta, CompressionKind, DatasetMetadata, Endianness, LogicalType},
        options::OpenOptions,
        row::OwnedCellValue,
    };
    use std::{ops::ControlFlow, sync::Arc};

    #[test]
    fn raw_scan_visits_rows_from_fused_pages() {
        let bytes = Arc::<[u8]>::from(make_pages());
        let ds = Dataset {
            file: Arc::new(FileInner {
                source: FileSource::Bytes(Arc::clone(&bytes)),
                options: OpenOptions::default(),
            }),
            metadata: Arc::new(DatasetMetadata {
                row_count: 3,
                row_len: 4,
                compression: CompressionKind::None,
                ..DatasetMetadata::default()
            }),
            layout: Arc::new(LayoutPlan {
                columns: Vec::new(),
                header: HeaderInfo {
                    endianness: Endianness::Little,
                    uses_u64_pointers: false,
                    page_size: 64,
                    page_count: 2,
                    page_header_size: 24,
                    subheader_pointer_size: 12,
                    subheader_signature_size: 4,
                    data_offset: 0,
                    header_size: 0,
                    release: String::new(),
                    is_catalog: false,
                },
                row_len: 4,
                total_rows: 3,
                compression: CompressionKind::None,
                rows_per_page: 1,
            }),
            descriptors: Arc::new(
                crate::pages::compile_page_descriptors(
                    &mut std::io::Cursor::new(bytes.as_ref()),
                    &LayoutPlan {
                        columns: Vec::new(),
                        header: HeaderInfo {
                            endianness: Endianness::Little,
                            uses_u64_pointers: false,
                            page_size: 64,
                            page_count: 2,
                            page_header_size: 24,
                            subheader_pointer_size: 12,
                            subheader_signature_size: 4,
                            data_offset: 0,
                            header_size: 0,
                            release: String::new(),
                            is_catalog: false,
                        },
                        row_len: 4,
                        total_rows: 3,
                        compression: CompressionKind::None,
                        rows_per_page: 1,
                    },
                )
                .expect("descriptors"),
            ),
        };

        let mut rows = Vec::new();
        let stats = ScanBuilder::new(&ds)
            .select(crate::RowSelection::Range { start: 1, end: 3 })
            .visit_raw_rows(|row| {
                rows.push((row.row_index, row.bytes.to_vec()));
                Ok(ControlFlow::Continue(()))
            })
            .expect("scan succeeds");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], (1, b"EFGH".to_vec()));
        assert_eq!(rows[1], (2, b"IJKL".to_vec()));
        assert_eq!(stats.rows_seen, 3);
        assert_eq!(stats.rows_emitted, 2);
        assert_eq!(stats.fused_pages, 2);
    }

    #[test]
    fn raw_scan_decompresses_rle_rows() {
        let bytes = Arc::<[u8]>::from(make_compressed_page(&[0xC1u8, b'A'], 64, 4));
        let layout = LayoutPlan {
            columns: Vec::new(),
            header: HeaderInfo {
                endianness: Endianness::Little,
                uses_u64_pointers: false,
                page_size: 64,
                page_count: 1,
                page_header_size: 24,
                subheader_pointer_size: 12,
                subheader_signature_size: 4,
                data_offset: 0,
                header_size: 0,
                release: String::new(),
                is_catalog: false,
            },
            row_len: 4,
            total_rows: 1,
            compression: CompressionKind::Row,
            rows_per_page: 1,
        };
        let ds = Dataset {
            file: Arc::new(FileInner {
                source: FileSource::Bytes(Arc::clone(&bytes)),
                options: OpenOptions::default(),
            }),
            metadata: Arc::new(DatasetMetadata {
                row_count: 1,
                row_len: 4,
                compression: CompressionKind::Row,
                ..DatasetMetadata::default()
            }),
            layout: Arc::new(layout.clone()),
            descriptors: Arc::new(
                crate::pages::compile_page_descriptors(
                    &mut std::io::Cursor::new(bytes.as_ref()),
                    &layout,
                )
                .expect("descriptors"),
            ),
        };

        let mut rows = Vec::new();
        let stats = ScanBuilder::new(&ds)
            .visit_raw_rows(|row| {
                rows.push((row.row_index, row.bytes.to_vec()));
                Ok(ControlFlow::Continue(()))
            })
            .expect("compressed raw scan");
        assert_eq!(rows, vec![(0, b"AAAA".to_vec())]);
        assert_eq!(stats.compressed_pages, 1);
        assert_eq!(stats.row_bytes_materialized, 4);
    }

    #[test]
    fn raw_scan_visits_rows_from_indexed_pointer_pages() {
        let bytes = Arc::<[u8]>::from(make_pointer_page(&[b"ABCD", b"EFGH"], 64));
        let layout = LayoutPlan {
            columns: Vec::new(),
            header: HeaderInfo {
                endianness: Endianness::Little,
                uses_u64_pointers: false,
                page_size: 64,
                page_count: 1,
                page_header_size: 24,
                subheader_pointer_size: 12,
                subheader_signature_size: 4,
                data_offset: 0,
                header_size: 0,
                release: String::new(),
                is_catalog: false,
            },
            row_len: 4,
            total_rows: 2,
            compression: CompressionKind::None,
            rows_per_page: 2,
        };
        let ds = Dataset {
            file: Arc::new(FileInner {
                source: FileSource::Bytes(Arc::clone(&bytes)),
                options: OpenOptions::default(),
            }),
            metadata: Arc::new(DatasetMetadata {
                row_count: 2,
                row_len: 4,
                compression: CompressionKind::None,
                ..DatasetMetadata::default()
            }),
            layout: Arc::new(layout.clone()),
            descriptors: Arc::new(
                crate::pages::compile_page_descriptors(
                    &mut std::io::Cursor::new(bytes.as_ref()),
                    &layout,
                )
                .expect("descriptors"),
            ),
        };

        let mut rows = Vec::new();
        let stats = ScanBuilder::new(&ds)
            .visit_raw_rows(|row| {
                rows.push((row.row_index, row.bytes.to_vec()));
                Ok(ControlFlow::Continue(()))
            })
            .expect("scan succeeds");

        assert_eq!(rows, vec![(0, b"ABCD".to_vec()), (1, b"EFGH".to_vec())]);
        assert_eq!(stats.indexed_pages, 1);
        assert_eq!(stats.rows_emitted, 2);
    }

    #[test]
    fn raw_scan_visits_rows_from_mixed_pointer_and_contiguous_page() {
        let bytes = Arc::<[u8]>::from(make_mixed_pointer_page(b"WXYZ", b"ABCD", 64));
        let layout = LayoutPlan {
            columns: Vec::new(),
            header: HeaderInfo {
                endianness: Endianness::Little,
                uses_u64_pointers: false,
                page_size: 64,
                page_count: 1,
                page_header_size: 24,
                subheader_pointer_size: 12,
                subheader_signature_size: 4,
                data_offset: 0,
                header_size: 0,
                release: String::new(),
                is_catalog: false,
            },
            row_len: 4,
            total_rows: 2,
            compression: CompressionKind::None,
            rows_per_page: 2,
        };
        let ds = Dataset {
            file: Arc::new(FileInner {
                source: FileSource::Bytes(Arc::clone(&bytes)),
                options: OpenOptions::default(),
            }),
            metadata: Arc::new(DatasetMetadata {
                row_count: 2,
                row_len: 4,
                compression: CompressionKind::None,
                ..DatasetMetadata::default()
            }),
            layout: Arc::new(layout.clone()),
            descriptors: Arc::new(
                crate::pages::compile_page_descriptors(
                    &mut std::io::Cursor::new(bytes.as_ref()),
                    &layout,
                )
                .expect("descriptors"),
            ),
        };

        let mut rows = Vec::new();
        let stats = ScanBuilder::new(&ds)
            .visit_raw_rows(|row| {
                rows.push((row.row_index, row.bytes.to_vec()));
                Ok(ControlFlow::Continue(()))
            })
            .expect("scan succeeds");

        assert_eq!(rows, vec![(0, b"WXYZ".to_vec()), (1, b"ABCD".to_vec())]);
        assert_eq!(stats.indexed_pages, 1);
        assert_eq!(stats.rows_emitted, 2);
    }

    #[test]
    fn typed_row_scan_decodes_projected_cells() {
        let bytes = Arc::<[u8]>::from(make_page(
            0x0100,
            1,
            0,
            &[&make_numeric_text_row(42.0, b"ABCD")],
            64,
        ));
        let layout = LayoutPlan {
            columns: vec![
                ColumnMeta {
                    index: 0,
                    name: "num".to_owned(),
                    logical_type: LogicalType::Float,
                    physical_width: 8,
                    offset: 0,
                    label: None,
                    format: None,
                },
                ColumnMeta {
                    index: 1,
                    name: "txt".to_owned(),
                    logical_type: LogicalType::String,
                    physical_width: 4,
                    offset: 8,
                    label: None,
                    format: None,
                },
            ],
            header: HeaderInfo {
                endianness: Endianness::Little,
                uses_u64_pointers: false,
                page_size: 64,
                page_count: 1,
                page_header_size: 24,
                subheader_pointer_size: 12,
                subheader_signature_size: 4,
                data_offset: 0,
                header_size: 0,
                release: String::new(),
                is_catalog: false,
            },
            row_len: 12,
            total_rows: 1,
            compression: CompressionKind::None,
            rows_per_page: 1,
        };
        let ds = Dataset {
            file: Arc::new(FileInner {
                source: FileSource::Bytes(Arc::clone(&bytes)),
                options: OpenOptions::default(),
            }),
            metadata: Arc::new(DatasetMetadata {
                row_count: 1,
                row_len: 12,
                compression: CompressionKind::None,
                encoding: Some("UTF-8".to_owned()),
                ..DatasetMetadata::default()
            }),
            layout: Arc::new(layout.clone()),
            descriptors: Arc::new(
                crate::pages::compile_page_descriptors(
                    &mut std::io::Cursor::new(bytes.as_ref()),
                    &layout,
                )
                .expect("descriptors"),
            ),
        };
        let projection = ds
            .projection()
            .column("txt")
            .column("num")
            .build()
            .expect("projection");

        let mut seen = Vec::new();
        let stats = ScanBuilder::new(&ds)
            .with_projection(&projection)
            .visit_rows(|row| {
                seen.push((
                    row.row_index(),
                    row.get(0).expect("txt").to_owned_value(),
                    row.get(1).expect("num").to_owned_value(),
                ));
                Ok(ControlFlow::Continue(()))
            })
            .expect("typed scan");

        assert_eq!(stats.rows_emitted, 1);
        assert_eq!(seen.len(), 1);
        assert!(matches!(seen[0].1, OwnedCellValue::String(ref value) if value == "ABCD"));
        assert!(matches!(seen[0].2, OwnedCellValue::Int64(42)));
    }

    #[test]
    fn typed_lossless_rows_preserve_numeric_bits_and_string_bytes() {
        let mut row = Vec::with_capacity(12);
        let missing_bits = 0x7FF0_0000_0000_0001u64;
        row.extend_from_slice(&missing_bits.to_le_bytes());
        row.extend_from_slice(b"A  ");
        let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[&row], 64));
        let layout = LayoutPlan {
            columns: vec![
                ColumnMeta {
                    index: 0,
                    name: "num".to_owned(),
                    logical_type: LogicalType::Float,
                    physical_width: 8,
                    offset: 0,
                    label: None,
                    format: None,
                },
                ColumnMeta {
                    index: 1,
                    name: "txt".to_owned(),
                    logical_type: LogicalType::String,
                    physical_width: 3,
                    offset: 8,
                    label: None,
                    format: None,
                },
            ],
            header: HeaderInfo {
                endianness: Endianness::Little,
                uses_u64_pointers: false,
                page_size: 64,
                page_count: 1,
                page_header_size: 24,
                subheader_pointer_size: 12,
                subheader_signature_size: 4,
                data_offset: 0,
                header_size: 0,
                release: String::new(),
                is_catalog: false,
            },
            row_len: 11,
            total_rows: 1,
            compression: CompressionKind::None,
            rows_per_page: 1,
        };
        let ds = Dataset {
            file: Arc::new(FileInner {
                source: FileSource::Bytes(Arc::clone(&bytes)),
                options: OpenOptions::default(),
            }),
            metadata: Arc::new(DatasetMetadata {
                row_count: 1,
                row_len: 11,
                compression: CompressionKind::None,
                encoding: Some("UTF-8".to_owned()),
                ..DatasetMetadata::default()
            }),
            layout: Arc::new(layout.clone()),
            descriptors: Arc::new(
                crate::pages::compile_page_descriptors(
                    &mut std::io::Cursor::new(bytes.as_ref()),
                    &layout,
                )
                .expect("descriptors"),
            ),
        };

        let rows = ScanBuilder::new(&ds)
            .with_decode_mode(crate::DecodeMode::TypedLossless)
            .collect_rows()
            .expect("lossless rows");
        assert_eq!(rows.len(), 1);
        assert!(matches!(
            rows[0].cells[0],
            crate::row::OwnedCellValue::Float64(value) if value.to_bits() == missing_bits
        ));
        assert!(matches!(
            rows[0].cells[1],
            crate::row::OwnedCellValue::Bytes(ref value) if value == b"A  "
        ));
    }

    #[test]
    fn collect_rows_materializes_owned_values() {
        let row = make_numeric_text_row(7.0, b"ZX  ");
        let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[&row], 64));
        let layout = LayoutPlan {
            columns: vec![
                ColumnMeta {
                    index: 0,
                    name: "num".to_owned(),
                    logical_type: LogicalType::Float,
                    physical_width: 8,
                    offset: 0,
                    label: None,
                    format: None,
                },
                ColumnMeta {
                    index: 1,
                    name: "txt".to_owned(),
                    logical_type: LogicalType::String,
                    physical_width: 4,
                    offset: 8,
                    label: None,
                    format: None,
                },
            ],
            header: HeaderInfo {
                endianness: Endianness::Little,
                uses_u64_pointers: false,
                page_size: 64,
                page_count: 1,
                page_header_size: 24,
                subheader_pointer_size: 12,
                subheader_signature_size: 4,
                data_offset: 0,
                header_size: 0,
                release: String::new(),
                is_catalog: false,
            },
            row_len: 12,
            total_rows: 1,
            compression: CompressionKind::None,
            rows_per_page: 1,
        };
        let ds = Dataset {
            file: Arc::new(FileInner {
                source: FileSource::Bytes(Arc::clone(&bytes)),
                options: OpenOptions::default(),
            }),
            metadata: Arc::new(DatasetMetadata {
                row_count: 1,
                row_len: 12,
                compression: CompressionKind::None,
                encoding: Some("UTF-8".to_owned()),
                ..DatasetMetadata::default()
            }),
            layout: Arc::new(layout.clone()),
            descriptors: Arc::new(
                crate::pages::compile_page_descriptors(
                    &mut std::io::Cursor::new(bytes.as_ref()),
                    &layout,
                )
                .expect("descriptors"),
            ),
        };

        let rows = ScanBuilder::new(&ds).collect_rows().expect("owned rows");
        assert_eq!(rows.len(), 1);
        assert!(matches!(
            rows[0].cells[0],
            crate::row::OwnedCellValue::Int64(7)
        ));
        assert!(matches!(
            rows[0].cells[1],
            crate::row::OwnedCellValue::String(ref value) if value == "ZX"
        ));
    }

    #[test]
    fn collect_batches_materializes_columnar_values() {
        let row_a = make_numeric_text_row(1.5, b"AA  ");
        let row_b = make_numeric_text_row(2.0, b"BBBB");
        let bytes = Arc::<[u8]>::from(make_page(0x0100, 2, 0, &[&row_a, &row_b], 64));
        let layout = LayoutPlan {
            columns: vec![
                ColumnMeta {
                    index: 0,
                    name: "num".to_owned(),
                    logical_type: LogicalType::Float,
                    physical_width: 8,
                    offset: 0,
                    label: None,
                    format: None,
                },
                ColumnMeta {
                    index: 1,
                    name: "txt".to_owned(),
                    logical_type: LogicalType::String,
                    physical_width: 4,
                    offset: 8,
                    label: None,
                    format: None,
                },
            ],
            header: HeaderInfo {
                endianness: Endianness::Little,
                uses_u64_pointers: false,
                page_size: 64,
                page_count: 1,
                page_header_size: 24,
                subheader_pointer_size: 12,
                subheader_signature_size: 4,
                data_offset: 0,
                header_size: 0,
                release: String::new(),
                is_catalog: false,
            },
            row_len: 12,
            total_rows: 2,
            compression: CompressionKind::None,
            rows_per_page: 2,
        };
        let ds = Dataset {
            file: Arc::new(FileInner {
                source: FileSource::Bytes(Arc::clone(&bytes)),
                options: OpenOptions::default(),
            }),
            metadata: Arc::new(DatasetMetadata {
                row_count: 2,
                row_len: 12,
                compression: CompressionKind::None,
                encoding: Some("UTF-8".to_owned()),
                ..DatasetMetadata::default()
            }),
            layout: Arc::new(layout.clone()),
            descriptors: Arc::new(
                crate::pages::compile_page_descriptors(
                    &mut std::io::Cursor::new(bytes.as_ref()),
                    &layout,
                )
                .expect("descriptors"),
            ),
        };

        let batches = ScanBuilder::new(&ds)
            .with_batch_hint(crate::BatchHint::Rows(2))
            .collect_batches()
            .expect("columnar batches");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].row_base, 0);
        assert_eq!(batches[0].row_count, 2);

        match &batches[0].columns[0] {
            OwnedColumnBuffer::F64 { values, valid } => {
                assert_eq!(values, &vec![1.5, 2.0]);
                assert!(valid.is_none());
            }
            other => panic!("unexpected numeric batch column: {other:?}"),
        }
        match &batches[0].columns[1] {
            OwnedColumnBuffer::Utf8 {
                offsets,
                data,
                valid,
            } => {
                assert_eq!(offsets, &vec![0, 2, 6]);
                assert_eq!(data, b"AABBBB");
                assert!(valid.is_none());
            }
            other => panic!("unexpected utf8 batch column: {other:?}"),
        }
    }

    #[test]
    fn typed_rows_decode_ascii_strings_without_utf8_encoding() {
        let row = make_numeric_text_row(1.0, b"pear");
        let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[&row], 64));
        let layout = LayoutPlan {
            columns: vec![
                ColumnMeta {
                    index: 0,
                    name: "num".to_owned(),
                    logical_type: LogicalType::Float,
                    physical_width: 8,
                    offset: 0,
                    label: None,
                    format: None,
                },
                ColumnMeta {
                    index: 1,
                    name: "txt".to_owned(),
                    logical_type: LogicalType::String,
                    physical_width: 4,
                    offset: 8,
                    label: None,
                    format: None,
                },
            ],
            header: HeaderInfo {
                endianness: Endianness::Little,
                uses_u64_pointers: false,
                page_size: 64,
                page_count: 1,
                page_header_size: 24,
                subheader_pointer_size: 12,
                subheader_signature_size: 4,
                data_offset: 0,
                header_size: 0,
                release: String::new(),
                is_catalog: false,
            },
            row_len: 12,
            total_rows: 1,
            compression: CompressionKind::None,
            rows_per_page: 1,
        };
        let ds = Dataset {
            file: Arc::new(FileInner {
                source: FileSource::Bytes(Arc::clone(&bytes)),
                options: OpenOptions::default(),
            }),
            metadata: Arc::new(DatasetMetadata {
                row_count: 1,
                row_len: 12,
                compression: CompressionKind::None,
                encoding: Some("WINDOWS-1252".to_owned()),
                ..DatasetMetadata::default()
            }),
            layout: Arc::new(layout.clone()),
            descriptors: Arc::new(
                crate::pages::compile_page_descriptors(
                    &mut std::io::Cursor::new(bytes.as_ref()),
                    &layout,
                )
                .expect("descriptors"),
            ),
        };

        let rows = ScanBuilder::new(&ds).collect_rows().expect("rows");
        assert!(matches!(
            rows[0].cells[1],
            OwnedCellValue::String(ref value) if value == "pear"
        ));
    }

    #[test]
    fn collect_batches_decode_ascii_strings_without_utf8_encoding() {
        let row = make_numeric_text_row(1.0, b"pear");
        let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[&row], 64));
        let layout = LayoutPlan {
            columns: vec![
                ColumnMeta {
                    index: 0,
                    name: "num".to_owned(),
                    logical_type: LogicalType::Float,
                    physical_width: 8,
                    offset: 0,
                    label: None,
                    format: None,
                },
                ColumnMeta {
                    index: 1,
                    name: "txt".to_owned(),
                    logical_type: LogicalType::String,
                    physical_width: 4,
                    offset: 8,
                    label: None,
                    format: None,
                },
            ],
            header: HeaderInfo {
                endianness: Endianness::Little,
                uses_u64_pointers: false,
                page_size: 64,
                page_count: 1,
                page_header_size: 24,
                subheader_pointer_size: 12,
                subheader_signature_size: 4,
                data_offset: 0,
                header_size: 0,
                release: String::new(),
                is_catalog: false,
            },
            row_len: 12,
            total_rows: 1,
            compression: CompressionKind::None,
            rows_per_page: 1,
        };
        let ds = Dataset {
            file: Arc::new(FileInner {
                source: FileSource::Bytes(Arc::clone(&bytes)),
                options: OpenOptions::default(),
            }),
            metadata: Arc::new(DatasetMetadata {
                row_count: 1,
                row_len: 12,
                compression: CompressionKind::None,
                encoding: Some("WINDOWS-1252".to_owned()),
                ..DatasetMetadata::default()
            }),
            layout: Arc::new(layout.clone()),
            descriptors: Arc::new(
                crate::pages::compile_page_descriptors(
                    &mut std::io::Cursor::new(bytes.as_ref()),
                    &layout,
                )
                .expect("descriptors"),
            ),
        };

        let batches = ScanBuilder::new(&ds)
            .with_batch_hint(crate::BatchHint::Rows(1))
            .collect_batches()
            .expect("batches");
        assert_eq!(batches.len(), 1);
        match &batches[0].columns[1] {
            OwnedColumnBuffer::Utf8 {
                offsets,
                data,
                valid,
            } => {
                assert_eq!(offsets, &vec![0, 4]);
                assert_eq!(data, b"pear");
                assert!(valid.is_none());
            }
            other => panic!("unexpected utf8 batch column: {other:?}"),
        }
    }

    #[test]
    fn collect_batches_typed_integer_widens_to_f64_for_fractional_values() {
        let row = make_numeric_text_row(1.5, b"INT ");
        let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[&row], 64));
        let layout = LayoutPlan {
            columns: vec![
                ColumnMeta {
                    index: 0,
                    name: "num".to_owned(),
                    logical_type: LogicalType::Integer,
                    physical_width: 8,
                    offset: 0,
                    label: None,
                    format: None,
                },
                ColumnMeta {
                    index: 1,
                    name: "txt".to_owned(),
                    logical_type: LogicalType::String,
                    physical_width: 4,
                    offset: 8,
                    label: None,
                    format: None,
                },
            ],
            header: HeaderInfo {
                endianness: Endianness::Little,
                uses_u64_pointers: false,
                page_size: 64,
                page_count: 1,
                page_header_size: 24,
                subheader_pointer_size: 12,
                subheader_signature_size: 4,
                data_offset: 0,
                header_size: 0,
                release: String::new(),
                is_catalog: false,
            },
            row_len: 12,
            total_rows: 1,
            compression: CompressionKind::None,
            rows_per_page: 1,
        };
        let ds = Dataset {
            file: Arc::new(FileInner {
                source: FileSource::Bytes(Arc::clone(&bytes)),
                options: OpenOptions::default(),
            }),
            metadata: Arc::new(DatasetMetadata {
                row_count: 1,
                row_len: 12,
                compression: CompressionKind::None,
                encoding: Some("UTF-8".to_owned()),
                ..DatasetMetadata::default()
            }),
            layout: Arc::new(layout.clone()),
            descriptors: Arc::new(
                crate::pages::compile_page_descriptors(
                    &mut std::io::Cursor::new(bytes.as_ref()),
                    &layout,
                )
                .expect("descriptors"),
            ),
        };

        let rows = ScanBuilder::new(&ds).collect_rows().expect("rows");
        assert!(matches!(rows[0].cells[0], OwnedCellValue::Float64(value) if value == 1.5));

        let batches = ScanBuilder::new(&ds)
            .with_batch_hint(crate::BatchHint::Rows(1))
            .collect_batches()
            .expect("batches");
        assert_eq!(batches.len(), 1);
        match &batches[0].columns[0] {
            OwnedColumnBuffer::F64 { values, valid } => {
                assert_eq!(values, &vec![1.5]);
                assert!(valid.is_none());
            }
            other => panic!("unexpected widened integer batch column: {other:?}"),
        }
    }

    #[test]
    fn collect_batches_typed_lossless_uses_f64_and_raw_bytes() {
        let row = make_numeric_text_row(42.0, b"ZX  ");
        let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[&row], 64));
        let layout = LayoutPlan {
            columns: vec![
                ColumnMeta {
                    index: 0,
                    name: "num".to_owned(),
                    logical_type: LogicalType::Integer,
                    physical_width: 8,
                    offset: 0,
                    label: None,
                    format: None,
                },
                ColumnMeta {
                    index: 1,
                    name: "txt".to_owned(),
                    logical_type: LogicalType::String,
                    physical_width: 4,
                    offset: 8,
                    label: None,
                    format: None,
                },
            ],
            header: HeaderInfo {
                endianness: Endianness::Little,
                uses_u64_pointers: false,
                page_size: 64,
                page_count: 1,
                page_header_size: 24,
                subheader_pointer_size: 12,
                subheader_signature_size: 4,
                data_offset: 0,
                header_size: 0,
                release: String::new(),
                is_catalog: false,
            },
            row_len: 12,
            total_rows: 1,
            compression: CompressionKind::None,
            rows_per_page: 1,
        };
        let ds = Dataset {
            file: Arc::new(FileInner {
                source: FileSource::Bytes(Arc::clone(&bytes)),
                options: OpenOptions::default(),
            }),
            metadata: Arc::new(DatasetMetadata {
                row_count: 1,
                row_len: 12,
                compression: CompressionKind::None,
                encoding: Some("UTF-8".to_owned()),
                ..DatasetMetadata::default()
            }),
            layout: Arc::new(layout.clone()),
            descriptors: Arc::new(
                crate::pages::compile_page_descriptors(
                    &mut std::io::Cursor::new(bytes.as_ref()),
                    &layout,
                )
                .expect("descriptors"),
            ),
        };

        let batches = ScanBuilder::new(&ds)
            .with_decode_mode(crate::DecodeMode::TypedLossless)
            .collect_batches()
            .expect("lossless batches");
        assert_eq!(batches.len(), 1);
        match &batches[0].columns[0] {
            OwnedColumnBuffer::F64 { values, valid } => {
                assert_eq!(values, &vec![42.0]);
                assert!(valid.is_none());
            }
            other => panic!("unexpected lossless numeric batch column: {other:?}"),
        }
        match &batches[0].columns[1] {
            OwnedColumnBuffer::RawBytes {
                offsets,
                data,
                valid,
            } => {
                assert_eq!(offsets, &vec![0, 4]);
                assert_eq!(data, b"ZX  ");
                assert!(valid.is_none());
            }
            other => panic!("unexpected lossless raw-bytes batch column: {other:?}"),
        }
    }

    #[test]
    fn visit_batches_streams_projected_columnar_views() {
        let row_a = make_numeric_text_row(10.0, b"ABCD");
        let row_b = make_numeric_text_row(20.0, b"EF  ");
        let bytes = Arc::<[u8]>::from(make_pointer_page(&[&row_a, &row_b], 64));
        let layout = LayoutPlan {
            columns: vec![
                ColumnMeta {
                    index: 0,
                    name: "num".to_owned(),
                    logical_type: LogicalType::Float,
                    physical_width: 8,
                    offset: 0,
                    label: None,
                    format: None,
                },
                ColumnMeta {
                    index: 1,
                    name: "txt".to_owned(),
                    logical_type: LogicalType::String,
                    physical_width: 4,
                    offset: 8,
                    label: None,
                    format: None,
                },
            ],
            header: HeaderInfo {
                endianness: Endianness::Little,
                uses_u64_pointers: false,
                page_size: 64,
                page_count: 1,
                page_header_size: 24,
                subheader_pointer_size: 12,
                subheader_signature_size: 4,
                data_offset: 0,
                header_size: 0,
                release: String::new(),
                is_catalog: false,
            },
            row_len: 12,
            total_rows: 2,
            compression: CompressionKind::None,
            rows_per_page: 2,
        };
        let ds = Dataset {
            file: Arc::new(FileInner {
                source: FileSource::Bytes(Arc::clone(&bytes)),
                options: OpenOptions::default(),
            }),
            metadata: Arc::new(DatasetMetadata {
                row_count: 2,
                row_len: 12,
                compression: CompressionKind::None,
                encoding: Some("UTF-8".to_owned()),
                ..DatasetMetadata::default()
            }),
            layout: Arc::new(layout.clone()),
            descriptors: Arc::new(
                crate::pages::compile_page_descriptors(
                    &mut std::io::Cursor::new(bytes.as_ref()),
                    &layout,
                )
                .expect("descriptors"),
            ),
        };
        let projection = ds.projection().column("txt").build().expect("projection");

        let mut seen = Vec::new();
        let stats = ScanBuilder::new(&ds)
            .with_projection(&projection)
            .with_batch_hint(crate::BatchHint::Rows(1))
            .visit_batches(|batch| {
                seen.push((
                    batch.row_base,
                    batch.row_count,
                    read_utf8_column(&batch.columns[0]),
                ));
                Ok(ControlFlow::Continue(()))
            })
            .expect("batch scan");

        assert_eq!(
            seen,
            vec![
                (0, 1, vec!["ABCD".to_owned()]),
                (1, 1, vec!["EF".to_owned()]),
            ]
        );
        assert_eq!(stats.decode_batches, 2);
        assert_eq!(stats.pages_seen, 1);
    }

    #[test]
    fn collect_rows_decodes_compressed_string_rows() {
        let bytes = Arc::<[u8]>::from(make_compressed_page(&[0xC1u8, b'Z'], 64, 4));
        let layout = LayoutPlan {
            columns: vec![ColumnMeta {
                index: 0,
                name: "txt".to_owned(),
                logical_type: LogicalType::String,
                physical_width: 4,
                offset: 0,
                label: None,
                format: None,
            }],
            header: HeaderInfo {
                endianness: Endianness::Little,
                uses_u64_pointers: false,
                page_size: 64,
                page_count: 1,
                page_header_size: 24,
                subheader_pointer_size: 12,
                subheader_signature_size: 4,
                data_offset: 0,
                header_size: 0,
                release: String::new(),
                is_catalog: false,
            },
            row_len: 4,
            total_rows: 1,
            compression: CompressionKind::Row,
            rows_per_page: 1,
        };
        let ds = Dataset {
            file: Arc::new(FileInner {
                source: FileSource::Bytes(Arc::clone(&bytes)),
                options: OpenOptions::default(),
            }),
            metadata: Arc::new(DatasetMetadata {
                row_count: 1,
                row_len: 4,
                compression: CompressionKind::Row,
                encoding: Some("UTF-8".to_owned()),
                ..DatasetMetadata::default()
            }),
            layout: Arc::new(layout.clone()),
            descriptors: Arc::new(
                crate::pages::compile_page_descriptors(
                    &mut std::io::Cursor::new(bytes.as_ref()),
                    &layout,
                )
                .expect("descriptors"),
            ),
        };

        let rows = ScanBuilder::new(&ds)
            .collect_rows()
            .expect("typed compressed rows");
        assert_eq!(rows.len(), 1);
        assert!(matches!(
            rows[0].cells[0],
            crate::row::OwnedCellValue::String(ref value) if value == "ZZZZ"
        ));
    }

    fn make_pages() -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(make_page(0x0100, 2, 0, &[b"ABCD", b"EFGH"], 64));
        bytes.extend(make_page(0x0200, 0, 0, &[b"IJKL"], 64));
        bytes
    }

    fn make_page(
        page_type: u16,
        row_count: u16,
        pointer_count: u16,
        rows: &[&[u8]],
        page_size: usize,
    ) -> Vec<u8> {
        let mut page = vec![0u8; page_size];
        page[(24 - 8)..(24 - 6)].copy_from_slice(&page_type.to_le_bytes());
        page[(24 - 6)..(24 - 4)].copy_from_slice(&row_count.to_le_bytes());
        page[(24 - 4)..(24 - 2)].copy_from_slice(&pointer_count.to_le_bytes());

        let mut offset = 24usize;
        for row in rows {
            page[offset..offset + row.len()].copy_from_slice(row);
            offset += row.len();
        }
        page
    }

    fn make_pointer_page(rows: &[&[u8]], page_size: usize) -> Vec<u8> {
        let mut page = vec![0u8; page_size];
        page[(24 - 8)..(24 - 6)].copy_from_slice(&0x0100u16.to_le_bytes());
        page[(24 - 6)..(24 - 4)].copy_from_slice(&(rows.len() as u16).to_le_bytes());
        page[(24 - 4)..(24 - 2)].copy_from_slice(&1u16.to_le_bytes());

        let data_offset = 40u32;
        let data_len = u32::try_from(rows.len() * 4).unwrap_or(u32::MAX);
        page[24..28].copy_from_slice(&data_offset.to_le_bytes());
        page[28..32].copy_from_slice(&data_len.to_le_bytes());
        page[32] = 0;
        page[33] = 1;

        let mut offset = data_offset as usize;
        for row in rows {
            page[offset..offset + row.len()].copy_from_slice(row);
            offset += row.len();
        }
        page
    }

    fn make_mixed_pointer_page(
        pointer_row: &[u8; 4],
        contiguous_row: &[u8; 4],
        page_size: usize,
    ) -> Vec<u8> {
        let mut page = vec![0u8; page_size];
        page[(24 - 8)..(24 - 6)].copy_from_slice(&0x0100u16.to_le_bytes());
        page[(24 - 6)..(24 - 4)].copy_from_slice(&2u16.to_le_bytes());
        page[(24 - 4)..(24 - 2)].copy_from_slice(&1u16.to_le_bytes());

        let pointer_data_offset = 48u32;
        page[24..28].copy_from_slice(&pointer_data_offset.to_le_bytes());
        page[28..32].copy_from_slice(&4u32.to_le_bytes());
        page[32] = 0;
        page[33] = 1;

        page[40..44].copy_from_slice(contiguous_row);
        let start = usize::try_from(pointer_data_offset).unwrap_or(0);
        page[start..start + 4].copy_from_slice(pointer_row);
        page
    }

    fn make_numeric_text_row(number: f64, text: &[u8; 4]) -> Vec<u8> {
        let mut row = Vec::with_capacity(12);
        row.extend_from_slice(&number.to_le_bytes());
        row.extend_from_slice(text);
        row
    }

    fn make_compressed_page(compressed: &[u8], page_size: usize, compression_flag: u8) -> Vec<u8> {
        let mut page = vec![0u8; page_size];
        page[(24 - 8)..(24 - 6)].copy_from_slice(&0x0100u16.to_le_bytes());
        page[(24 - 6)..(24 - 4)].copy_from_slice(&1u16.to_le_bytes());
        page[(24 - 4)..(24 - 2)].copy_from_slice(&1u16.to_le_bytes());

        let data_offset = 40u32;
        let data_len = u32::try_from(compressed.len()).unwrap_or(u32::MAX);
        page[24..28].copy_from_slice(&data_offset.to_le_bytes());
        page[28..32].copy_from_slice(&data_len.to_le_bytes());
        page[32] = compression_flag;
        page[33] = 1;

        let start = usize::try_from(data_offset).unwrap_or(0);
        let end = start + compressed.len();
        page[start..end].copy_from_slice(compressed);
        page
    }

    fn read_utf8_column(column: &crate::ColumnBuffer<'_>) -> Vec<String> {
        let crate::ColumnBuffer::Utf8(buffer) = column else {
            panic!("expected utf8 column, got {column:?}");
        };
        buffer
            .offsets
            .windows(2)
            .map(|window| {
                let start = usize::try_from(window[0]).expect("utf8 start");
                let end = usize::try_from(window[1]).expect("utf8 end");
                String::from_utf8(buffer.data[start..end].to_vec()).expect("utf8 cell")
            })
            .collect()
    }
}
