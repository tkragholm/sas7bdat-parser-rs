use super::{
    Arc, ControlFlow, Cursor, Dataset, Error, File, FileSource, PageDescriptor, RawRow, Result,
    RowSelection, RowSpan, RowSpanKind, ScanBuilder, ScanStats, Seek, SeekFrom, decompress_row,
};
use crate::types::{PageIndex, RowIndex};
use std::io::Read;

pub(super) fn scan_raw_rows_with_plan<F>(
    builder: &ScanBuilder<'_>,
    plan: &RawScanPlan,
    f: &mut F,
) -> Result<ScanStats>
where
    F: FnMut(RawRow<'_>) -> Result<ControlFlow<()>>,
{
    scan_row_bytes_with_plan(builder, plan, &mut |row_index, bytes| {
        f(RawRow { row_index, bytes })
    })
}

pub(super) fn scan_row_bytes_with_plan<F>(
    builder: &ScanBuilder<'_>,
    plan: &RawScanPlan,
    f: &mut F,
) -> Result<ScanStats>
where
    F: FnMut(RowIndex, &[u8]) -> Result<ControlFlow<()>>,
{
    let descriptors = builder.ds.descriptors()?;
    match &builder.ds.file.source {
        FileSource::Bytes(bytes) => {
            scan_row_bytes_in_memory(builder, plan, bytes.as_ref(), descriptors.as_ref(), f)
        }
        FileSource::Mmap(mmap) => {
            scan_row_bytes_in_memory(builder, plan, &mmap[..], descriptors.as_ref(), f)
        }
        FileSource::Path(_) => {
            let mut reader = open_scan_reader(builder.ds)?;
            scan_row_bytes_with_reader(builder, plan, descriptors.as_ref(), &mut reader, f)
        }
    }
}

struct ScanLoopContext {
    stats: ScanStats,
    page: Vec<u8>,
    decompressed_row: Vec<u8>,
    total_pages: u64,
    estimated_total_bytes: u64,
}

impl ScanLoopContext {
    fn new(plan: &RawScanPlan, total_pages: u64) -> Self {
        let stats = ScanStats::default();
        let page = vec![0u8; plan.page_size];
        let decompressed_row = Vec::new();
        let estimated_total_bytes = u64::try_from(plan.page_size)
            .unwrap_or(u64::MAX)
            .saturating_mul(total_pages);
        Self {
            stats,
            page,
            decompressed_row,
            total_pages,
            estimated_total_bytes,
        }
    }
}

pub(super) fn scan_row_bytes_with_reader<R, F>(
    builder: &ScanBuilder<'_>,
    plan: &RawScanPlan,
    descriptors: &crate::internal::PageDescriptorTable,
    reader: &mut R,
    f: &mut F,
) -> Result<ScanStats>
where
    R: Read + Seek,
    F: FnMut(RowIndex, &[u8]) -> Result<ControlFlow<()>>,
{
    if plan.row_len == 0 {
        return Ok(ScanStats::default());
    }
    RawScanPlan::validate_builder(builder)?;

    let mut ctx = ScanLoopContext::new(
        plan,
        u64::try_from(descriptors.pages.len()).unwrap_or(u64::MAX),
    );

    for descriptor in descriptors.pages.iter().copied() {
        if plan.should_stop(&ctx.stats) {
            break;
        }

        ctx.stats.pages_seen = ctx.stats.pages_seen.saturating_add(1);
        load_descriptor_page(reader, plan, descriptor, &mut ctx.page, &mut ctx.stats)?;
        if emit_rows_from_page(
            plan,
            descriptors,
            descriptor,
            &ctx.page,
            &mut ctx.decompressed_row,
            &mut ctx.stats,
            f,
        )? {
            return Ok(ctx.stats);
        }

        emit_progress(
            builder,
            &ctx.stats,
            ctx.total_pages,
            ctx.estimated_total_bytes,
        );
    }

    Ok(ctx.stats)
}

pub(super) fn scan_row_bytes_in_memory<F>(
    builder: &ScanBuilder<'_>,
    plan: &RawScanPlan,
    file_bytes: &[u8],
    descriptors: &crate::internal::PageDescriptorTable,
    f: &mut F,
) -> Result<ScanStats>
where
    F: FnMut(RowIndex, &[u8]) -> Result<ControlFlow<()>>,
{
    if plan.row_len == 0 {
        return Ok(ScanStats::default());
    }
    RawScanPlan::validate_builder(builder)?;

    let mut ctx = ScanLoopContext::new(
        plan,
        u64::try_from(descriptors.pages.len()).unwrap_or(u64::MAX),
    );

    for descriptor in descriptors.pages.iter().copied() {
        if plan.should_stop(&ctx.stats) {
            break;
        }

        let page = page_slice(file_bytes, plan, descriptor)?;
        ctx.stats.pages_seen = ctx.stats.pages_seen.saturating_add(1);
        ctx.stats.raw_bytes_read = ctx
            .stats
            .raw_bytes_read
            .saturating_add(u64::try_from(page.len()).unwrap_or(u64::MAX));

        if emit_rows_from_page(
            plan,
            descriptors,
            descriptor,
            page,
            &mut ctx.decompressed_row,
            &mut ctx.stats,
            f,
        )? {
            return Ok(ctx.stats);
        }

        emit_progress(
            builder,
            &ctx.stats,
            ctx.total_pages,
            ctx.estimated_total_bytes,
        );
    }

    Ok(ctx.stats)
}

fn emit_progress(
    builder: &ScanBuilder<'_>,
    stats: &ScanStats,
    total_pages: u64,
    estimated_total_bytes: u64,
) {
    if let Some(observer) = &builder.progress {
        observer(super::ScanProgress {
            pages_seen: stats.pages_seen,
            total_pages,
            raw_bytes_read: stats.raw_bytes_read,
            estimated_total_bytes,
            compressed_pages: stats.compressed_pages,
            rows_seen: stats.rows_seen,
            rows_emitted: stats.rows_emitted,
        });
    }
}

pub(super) fn emit_rows_from_page<F>(
    plan: &RawScanPlan,
    descriptors: &crate::internal::PageDescriptorTable,
    descriptor: PageDescriptor,
    page: &[u8],
    decompressed_row: &mut Vec<u8>,
    stats: &mut ScanStats,
    f: &mut F,
) -> Result<bool>
where
    F: FnMut(RowIndex, &[u8]) -> Result<ControlFlow<()>>,
{
    match descriptor.exec_class {
        crate::internal::PageExecClass::FusedContiguousUncompressed => {
            stats.fused_pages = stats.fused_pages.saturating_add(1);
            if emit_contiguous_rows(plan, descriptor, page, stats, f)? {
                return Ok(true);
            }
        }
        crate::internal::PageExecClass::MetadataOrEmpty => {}
        crate::internal::PageExecClass::IndexedPointerRows => {
            stats.indexed_pages = stats.indexed_pages.saturating_add(1);
            let spans = descriptor_spans(descriptors, descriptor)?;
            if emit_indexed_rows(plan, descriptor, spans, page, decompressed_row, stats, f)? {
                return Ok(true);
            }
        }
        crate::internal::PageExecClass::IndexedCompressedRows => {
            stats.compressed_pages = stats.compressed_pages.saturating_add(1);
            let spans = descriptor_spans(descriptors, descriptor)?;
            if emit_indexed_rows(plan, descriptor, spans, page, decompressed_row, stats, f)? {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub(super) fn page_slice<'a>(
    file_bytes: &'a [u8],
    plan: &RawScanPlan,
    descriptor: PageDescriptor,
) -> Result<&'a [u8]> {
    let start = usize::try_from(plan.page_offset(descriptor.page_index))
        .map_err(|_| Error::unsupported("page offset exceeds platform usize"))?;
    let end = start
        .checked_add(plan.page_size)
        .ok_or_else(|| Error::unsupported("page end overflow"))?;
    file_bytes
        .get(start..end)
        .ok_or_else(|| Error::unsupported("page slice exceeds source bounds"))
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RawScanPlan {
    row_len: usize,
    page_size: usize,
    page_stride: u64,
    data_offset: u64,
    compression: crate::metadata::CompressionKind,
    row_limit: Option<u64>,
    row_selection: RowSelection,
}

impl RawScanPlan {
    pub(super) fn validate_builder(builder: &ScanBuilder<'_>) -> Result<()> {
        let descriptors = builder.ds.descriptors()?;
        if builder.ds.layout.compression != crate::metadata::CompressionKind::None
            && builder.ds.metadata.row_count > 0
            && descriptors.total_candidate_rows == 0
        {
            return Err(Error::unsupported(
                "compressed dataset layout compiled no row producers; this compressed page layout is not implemented yet",
            ));
        }
        Ok(())
    }

    pub(super) fn compile(builder: &ScanBuilder<'_>) -> Self {
        let row_len = usize::from(builder.ds.layout.row_len);
        let page_size = usize::from(builder.ds.layout.header.page_size);
        Self {
            row_len,
            page_size,
            page_stride: u64::from(builder.ds.layout.header.page_size),
            data_offset: builder.ds.layout.header.data_offset,
            compression: builder.ds.layout.compression,
            row_limit: builder.row_limit,
            row_selection: builder.row_selection,
        }
    }

    fn should_stop(self, stats: &ScanStats) -> bool {
        self.row_limit
            .is_some_and(|limit| stats.rows_emitted >= limit)
    }

    const fn page_offset(self, page_index: PageIndex) -> u64 {
        self.data_offset + page_index.0 * self.page_stride
    }
}

pub(super) fn row_selected(selection: RowSelection, row_index: RowIndex) -> bool {
    match selection {
        RowSelection::All => true,
        RowSelection::Range { start, end } => (start..end).contains(&row_index),
        RowSelection::First(n) => row_index.0 < n,
    }
}

pub(super) fn prepare_row_visit(
    plan: &RawScanPlan,
    stats: &mut ScanStats,
    row_index: RowIndex,
) -> bool {
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

pub(super) const fn finish_row_visit(stats: &mut ScanStats, flow: ControlFlow<()>) -> bool {
    stats.rows_emitted = stats.rows_emitted.saturating_add(1);
    matches!(flow, ControlFlow::Break(()))
}

pub(super) fn load_descriptor_page<R: Read + Seek>(
    reader: &mut R,
    plan: &RawScanPlan,
    descriptor: PageDescriptor,
    page: &mut [u8],
    stats: &mut ScanStats,
) -> Result<()> {
    reader
        .seek(SeekFrom::Start(plan.page_offset(descriptor.page_index)))
        .map_err(|e| Error::io_error(&e))?;
    reader.read_exact(page).map_err(|e| Error::io_error(&e))?;
    stats.raw_bytes_read = stats
        .raw_bytes_read
        .saturating_add(u64::try_from(page.len()).unwrap_or(u64::MAX));
    Ok(())
}

pub(super) fn descriptor_spans(
    descriptors: &crate::internal::PageDescriptorTable,
    descriptor: PageDescriptor,
) -> Result<&[RowSpan]> {
    let span_start = usize::try_from(descriptor.row_span_start)
        .map_err(|_| Error::unsupported("row span start exceeds platform usize"))?;
    let span_end = span_start
        .checked_add(
            usize::try_from(descriptor.row_span_count)
                .map_err(|_| Error::unsupported("row span count exceeds platform usize"))?,
        )
        .ok_or_else(|| Error::unsupported("row span range overflow"))?;
    descriptors
        .row_spans
        .get(span_start..span_end)
        .ok_or_else(|| Error::unsupported("row span range exceeds descriptor table"))
}

pub(super) fn emit_contiguous_rows<F>(
    plan: &RawScanPlan,
    descriptor: PageDescriptor,
    page: &[u8],
    stats: &mut ScanStats,
    f: &mut F,
) -> Result<bool>
where
    F: FnMut(RowIndex, &[u8]) -> Result<ControlFlow<()>>,
{
    let data_start = usize::from(descriptor.data_start);
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

pub(super) fn emit_indexed_rows<F>(
    plan: &RawScanPlan,
    descriptor: PageDescriptor,
    spans: &[RowSpan],
    page: &[u8],
    decompressed_row: &mut Vec<u8>,
    stats: &mut ScanStats,
    f: &mut F,
) -> Result<bool>
where
    F: FnMut(RowIndex, &[u8]) -> Result<ControlFlow<()>>,
{
    for (span_index, span) in spans.iter().enumerate() {
        let row_index = descriptor.row_base + u64::try_from(span_index).unwrap_or(u64::MAX);
        if !prepare_row_visit(plan, stats, row_index) {
            continue;
        }

        let start = usize::from(span.offset);
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

pub(super) enum ScanReader {
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

pub(super) fn open_scan_reader(ds: &Dataset) -> Result<ScanReader> {
    match &ds.file.source {
        FileSource::Path(path) => File::open(path)
            .map(ScanReader::File)
            .map_err(|err| Error::io_error_with_path(path, &err)),
        FileSource::Bytes(bytes) => Ok(ScanReader::Bytes(Cursor::new(Arc::clone(bytes)))),
        FileSource::Mmap(_) => unreachable!("mapped files use the in-memory scan path"),
    }
}
