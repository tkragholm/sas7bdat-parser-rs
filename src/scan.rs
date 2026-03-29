use crate::{
    columnar::{ColumnBuffer, ColumnarBatch, OwnedColumnarBatch},
    dataset::Dataset,
    error::{Error, Result},
    internal::FileSource,
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
        Err(Error::unsupported(
            "columnar scanning is not implemented yet",
        ))
    }

    pub fn collect_rows(self) -> Result<Vec<OwnedRow>> {
        let mut rows = Vec::new();
        self.visit_rows(|row| {
            rows.push(OwnedRow {
                row_index: row.row_index(),
                cells: row
                    .iter()
                    .map(crate::row::CellValue::to_owned_value)
                    .collect(),
            });
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(rows)
    }

    pub fn collect_batches(self) -> Result<Vec<OwnedColumnarBatch>> {
        Err(Error::unsupported(
            "batch collection is not implemented yet",
        ))
    }

    pub fn write_raw_rows(self, _sink: &mut impl RawRowSink) -> Result<ScanStats> {
        self.visit_raw_rows(|row| _sink.push(row))
    }

    pub fn write_rows(self, _sink: &mut impl RowSink) -> Result<ScanStats> {
        self.visit_rows(|row| _sink.push(row))
    }

    pub fn write_batches(self, _sink: &mut impl BatchSink) -> Result<ScanStats> {
        Err(Error::unsupported(
            "batch sink writing is not implemented yet",
        ))
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
        self.scan_raw_rows(&mut |raw| {
            let mut owned_strings = Vec::new();
            let planned = plan.plan_cells(raw.bytes, &mut owned_strings)?;
            let cells = materialize_planned_cells(planned, &owned_strings)?;
            let row = RowView {
                row_index: raw.row_index,
                names: &plan.names,
                cells: &cells,
            };
            f(row)
        })
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
    let row_len = usize::try_from(builder.ds.layout.row_len)
        .map_err(|_| Error::unsupported("row length exceeds platform usize"))?;
    if row_len == 0 {
        return Ok(ScanStats::default());
    }

    let page_size = usize::try_from(builder.ds.layout.header.page_size)
        .map_err(|_| Error::unsupported("page size exceeds platform usize"))?;
    let mut stats = ScanStats::default();
    let mut page = vec![0u8; page_size];

    for descriptor in builder.ds.descriptors.pages.iter().copied() {
        if let Some(limit) = builder.row_limit
            && stats.rows_emitted >= limit
        {
            break;
        }

        stats.pages_seen = stats.pages_seen.saturating_add(1);
        match descriptor.exec_class {
            crate::internal::PageExecClass::FusedContiguousUncompressed => {
                stats.fused_pages = stats.fused_pages.saturating_add(1);
                let page_offset = builder.ds.layout.header.data_offset
                    + descriptor.page_index * u64::from(builder.ds.layout.header.page_size);
                reader
                    .seek(SeekFrom::Start(page_offset))
                    .map_err(scan_io_error)?;
                reader.read_exact(&mut page).map_err(scan_io_error)?;
                stats.raw_bytes_read = stats
                    .raw_bytes_read
                    .saturating_add(u64::try_from(page.len()).unwrap_or(u64::MAX));

                let data_start = usize::try_from(descriptor.data_start)
                    .map_err(|_| Error::unsupported("row data start exceeds platform usize"))?;
                for row_offset in 0..descriptor.row_count {
                    let row_index = descriptor.row_base + u64::from(row_offset);
                    stats.rows_seen = stats.rows_seen.saturating_add(1);
                    if !row_selected(builder.row_selection, row_index) {
                        continue;
                    }
                    if let Some(limit) = builder.row_limit
                        && stats.rows_emitted >= limit
                    {
                        break;
                    }

                    let start = data_start
                        .checked_add(
                            usize::try_from(row_offset)
                                .unwrap_or(usize::MAX)
                                .saturating_mul(row_len),
                        )
                        .ok_or_else(|| Error::unsupported("row offset overflow"))?;
                    let end = start
                        .checked_add(row_len)
                        .ok_or_else(|| Error::unsupported("row end overflow"))?;
                    let Some(bytes) = page.get(start..end) else {
                        return Err(Error::unsupported("row slice exceeds page bounds"));
                    };

                    match f(RawRow { row_index, bytes })? {
                        ControlFlow::Continue(()) => {
                            stats.rows_emitted = stats.rows_emitted.saturating_add(1);
                        }
                        ControlFlow::Break(()) => {
                            stats.rows_emitted = stats.rows_emitted.saturating_add(1);
                            return Ok(stats);
                        }
                    }
                }
            }
            crate::internal::PageExecClass::MetadataOrEmpty => {}
            crate::internal::PageExecClass::IndexedPointerRows => {
                stats.indexed_pages = stats.indexed_pages.saturating_add(1);
                let page_offset = builder.ds.layout.header.data_offset
                    + descriptor.page_index * u64::from(builder.ds.layout.header.page_size);
                reader
                    .seek(SeekFrom::Start(page_offset))
                    .map_err(scan_io_error)?;
                reader.read_exact(&mut page).map_err(scan_io_error)?;
                stats.raw_bytes_read = stats
                    .raw_bytes_read
                    .saturating_add(u64::try_from(page.len()).unwrap_or(u64::MAX));

                let span_start = usize::try_from(descriptor.row_span_start)
                    .map_err(|_| Error::unsupported("row span start exceeds platform usize"))?;
                let span_end =
                    span_start
                        .checked_add(usize::try_from(descriptor.row_span_count).map_err(|_| {
                            Error::unsupported("row span count exceeds platform usize")
                        })?)
                        .ok_or_else(|| Error::unsupported("row span range overflow"))?;
                let Some(spans) = builder.ds.descriptors.row_spans.get(span_start..span_end) else {
                    return Err(Error::unsupported(
                        "row span range exceeds descriptor table",
                    ));
                };

                for (span_index, span) in spans.iter().enumerate() {
                    let row_index =
                        descriptor.row_base + u64::try_from(span_index).unwrap_or(u64::MAX);
                    stats.rows_seen = stats.rows_seen.saturating_add(1);
                    if !row_selected(builder.row_selection, row_index) {
                        continue;
                    }
                    if let Some(limit) = builder.row_limit
                        && stats.rows_emitted >= limit
                    {
                        break;
                    }

                    let start = usize::try_from(span.offset).map_err(|_| {
                        Error::unsupported("row span offset exceeds platform usize")
                    })?;
                    let len = usize::try_from(span.len).map_err(|_| {
                        Error::unsupported("row span length exceeds platform usize")
                    })?;
                    let end = start
                        .checked_add(len)
                        .ok_or_else(|| Error::unsupported("row span end overflow"))?;
                    let Some(bytes) = page.get(start..end) else {
                        return Err(Error::unsupported("row span exceeds page bounds"));
                    };
                    match f(RawRow { row_index, bytes })? {
                        ControlFlow::Continue(()) => {
                            stats.rows_emitted = stats.rows_emitted.saturating_add(1);
                        }
                        ControlFlow::Break(()) => {
                            stats.rows_emitted = stats.rows_emitted.saturating_add(1);
                            return Ok(stats);
                        }
                    }
                }
            }
            crate::internal::PageExecClass::IndexedCompressedRows => {
                return Err(Error::unsupported(
                    "raw row scanning for compressed pages is not implemented yet",
                ));
            }
        }
    }

    Ok(stats)
}

fn row_selected(selection: RowSelection, row_index: u64) -> bool {
    match selection {
        RowSelection::All => true,
        RowSelection::Range { start, end } => (start..end).contains(&row_index),
    }
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
struct RowDecodePlan<'a> {
    columns: Vec<&'a ColumnMeta>,
    names: Vec<String>,
    encoding: &'static Encoding,
    decode_mode: DecodeMode,
    string_options: StringDecodeOptions,
    temporal_options: TemporalDecodeOptions,
    endianness: Endianness,
}

#[derive(Debug)]
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

impl<'a> RowDecodePlan<'a> {
    fn new(builder: &ScanBuilder<'a>) -> Result<Self> {
        let columns: Vec<&ColumnMeta> = if let Some(projection) = builder.projection {
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
            columns.iter().map(|column| column.name.clone()).collect()
        };

        let encoding = builder
            .ds
            .metadata
            .encoding
            .as_deref()
            .and_then(|label| Encoding::for_label(label.as_bytes()))
            .unwrap_or(UTF_8);

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
        for column in &self.columns {
            planned.push(self.plan_cell(row, column, owned_strings)?);
        }
        Ok(planned)
    }

    fn plan_cell<'row>(
        &self,
        row: &'row [u8],
        column: &ColumnMeta,
        owned_strings: &mut Vec<String>,
    ) -> Result<PlannedCell<'row>> {
        let start = usize::try_from(column.offset)
            .map_err(|_| Error::unsupported("column offset exceeds platform usize"))?;
        let width = usize::try_from(column.physical_width)
            .map_err(|_| Error::unsupported("column width exceeds platform usize"))?;
        let end = start
            .checked_add(width)
            .ok_or_else(|| Error::unsupported("column end overflow"))?;
        let slice = row
            .get(start..end)
            .ok_or_else(|| Error::unsupported("column slice exceeds row bounds"))?;

        match column.logical_type {
            LogicalType::String => self.plan_string(slice, owned_strings),
            LogicalType::Bytes => Ok(PlannedCell::Bytes(slice)),
            LogicalType::Date => {
                self.plan_numeric_temporal(slice, width as u32, TemporalKind::Date)
            }
            LogicalType::DateTime => {
                self.plan_numeric_temporal(slice, width as u32, TemporalKind::DateTime)
            }
            LogicalType::Time => {
                self.plan_numeric_temporal(slice, width as u32, TemporalKind::Time)
            }
            LogicalType::Integer | LogicalType::Float => {
                self.plan_numeric_value(slice, width as u32)
            }
        }
    }

    fn plan_string<'row>(
        &self,
        slice: &'row [u8],
        owned_strings: &mut Vec<String>,
    ) -> Result<PlannedCell<'row>> {
        let slice = if self.string_options.trim_fixed_width {
            trim_trailing_space_or_nul(slice)
        } else {
            slice
        };
        if slice.is_empty() {
            return Ok(PlannedCell::StrBorrowed(""));
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
            DecodeMode::Typed | DecodeMode::TypedLossless => {
                match decode_numeric_cell(slice, self.endianness) {
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
                }
            }
            DecodeMode::Raw => Err(Error::unsupported(
                "visit_rows does not support DecodeMode::Raw; use visit_raw_rows instead",
            )),
        }
    }

    fn plan_numeric_value<'row>(&self, slice: &[u8], width: u32) -> Result<PlannedCell<'row>> {
        match decode_numeric_cell(slice, self.endianness) {
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
        }
    }
}

fn materialize_planned_cells<'a>(
    planned: Vec<PlannedCell<'a>>,
    owned_strings: &'a [String],
) -> Result<Vec<crate::row::CellValue<'a>>> {
    let mut cells = Vec::with_capacity(planned.len());
    for cell in planned {
        cells.push(match cell {
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
    fn raw_scan_rejects_indexed_pages() {
        let mut page = vec![0u8; 64];
        page[(24 - 8)..(24 - 6)].copy_from_slice(&0x9000u16.to_le_bytes());
        page[(24 - 6)..(24 - 4)].copy_from_slice(&1u16.to_le_bytes());
        page[(24 - 4)..(24 - 2)].copy_from_slice(&0u16.to_le_bytes());
        let bytes = Arc::<[u8]>::from(page);
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

        let err = ScanBuilder::new(&ds)
            .visit_raw_rows(|_| Ok(ControlFlow::Continue(())))
            .expect_err("compressed pages are not supported yet");
        assert!(err.to_string().contains("compressed pages"));
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

    fn make_numeric_text_row(number: f64, text: &[u8; 4]) -> Vec<u8> {
        let mut row = Vec::with_capacity(12);
        row.extend_from_slice(&number.to_le_bytes());
        row.extend_from_slice(text);
        row
    }
}
