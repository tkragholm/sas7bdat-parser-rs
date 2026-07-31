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

    // Only the descriptors whose rows the window actually wants. Slicing here means a bounded
    // scan neither reads nor faults in the pages outside it, and it costs a whole-file scan
    // nothing — `page_range` returns the full slice for an unbounded window.
    let page_range = plan.window.page_range(&descriptors.pages);
    for descriptor in descriptors.pages[page_range].iter().copied() {
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

    // Only the descriptors whose rows the window actually wants. Slicing here means a bounded
    // scan neither reads nor faults in the pages outside it, and it costs a whole-file scan
    // nothing — `page_range` returns the full slice for an unbounded window.
    let page_range = plan.window.page_range(&descriptors.pages);
    for descriptor in descriptors.pages[page_range].iter().copied() {
        let page = page_slice(PageWindow::whole_file(file_bytes), plan, descriptor)?;
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

/// A window of file bytes the decoder can slice pages out of.
///
/// `base_offset` is where `bytes[0]` sits in the file. Whole-file sources (mmap, in-memory)
/// pass `0`; a chunked reader passes one extent at a time with its own offset, so both reach
/// the same decode routine.
#[derive(Clone, Copy)]
pub(super) struct PageWindow<'a> {
    pub bytes: &'a [u8],
    pub base_offset: u64,
}

impl<'a> PageWindow<'a> {
    pub(super) const fn whole_file(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            base_offset: 0,
        }
    }
}

pub(super) fn page_slice<'a>(
    window: PageWindow<'a>,
    plan: &RawScanPlan,
    descriptor: PageDescriptor,
) -> Result<&'a [u8]> {
    let absolute = plan.page_offset(descriptor.page_index);
    let relative = absolute
        .checked_sub(window.base_offset)
        .ok_or_else(|| Error::unsupported("page precedes the current read window"))?;
    let start = usize::try_from(relative)
        .map_err(|_| Error::unsupported("page offset exceeds platform usize"))?;
    let end = start
        .checked_add(plan.page_size)
        .ok_or_else(|| Error::unsupported("page end overflow"))?;
    window
        .bytes
        .get(start..end)
        .ok_or_else(|| Error::unsupported("page slice exceeds source bounds"))
}

/// The absolute row range a scan emits, resolved from [`RowSelection`] and
/// [`ScanBuilder::limit`](super::ScanBuilder::limit) together.
///
/// The two used to be enforced by separate mechanisms with opposite costs: the selection was
/// a per-row predicate that kept parallelism but never stopped the scan, while the limit
/// stopped early but disabled every parallel path — it counted *emitted* rows, which is a
/// per-worker quantity and therefore meaningless once decode is split across threads. Resolving
/// both to one absolute `[start, end)` in row indices makes the bound worker-independent, so a
/// bounded scan can be parallel and stop early at the same time.
///
/// `end` is deliberately not clamped to the declared row count: the count comes from the file
/// header and this reader already handles files where it is wrong, so clamping would truncate
/// a scan that the unclamped predicate would have completed.
#[derive(Debug, Clone, Copy)]
pub(super) struct RowWindow {
    start: u64,
    end: u64,
}

impl RowWindow {
    pub(super) fn resolve(selection: RowSelection, limit: Option<u64>) -> Self {
        let (start, end) = match selection {
            RowSelection::All => (0, u64::MAX),
            RowSelection::Range { start, end } => (start.0, end.0),
            RowSelection::First(n) => (0, n),
        };
        let start = start.min(end);
        // A limit counts rows *within* the selection, so it bounds the window's own end.
        let end = limit.map_or(end, |limit| end.min(start.saturating_add(limit)));
        Self { start, end }
    }

    const fn contains(self, row: RowIndex) -> bool {
        row.0 >= self.start && row.0 < self.end
    }

    #[cfg(test)]
    pub(super) const fn start_for_test(self) -> u64 {
        self.start
    }

    #[cfg(test)]
    pub(super) const fn end_for_test(self) -> u64 {
        self.end
    }

    /// Whether this window covers every row, which is what lets the fused and column-major
    /// paths engage — both plan their reads from file geometry rather than from row indices.
    pub(super) const fn is_whole_file(self) -> bool {
        self.start == 0 && self.end == u64::MAX
    }

    /// Rows this window spans, saturating for the unbounded case.
    pub(super) const fn len(self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    /// First row index past `descriptor`'s rows.
    fn descriptor_end(descriptor: &PageDescriptor) -> u64 {
        descriptor
            .row_base
            .0
            .saturating_add(u64::from(descriptor.row_count))
    }

    /// The contiguous descriptor range covering this window.
    ///
    /// Sound because `row_base` is a running total over pages in file order, so it is
    /// non-decreasing and a row range maps to one slice rather than a scattered set.
    pub(super) fn page_range(self, pages: &[PageDescriptor]) -> std::ops::Range<usize> {
        if self.is_whole_file() {
            return 0..pages.len();
        }
        let first = pages.partition_point(|page| Self::descriptor_end(page) <= self.start);
        let last = pages.partition_point(|page| page.row_base.0 < self.end);
        first..last.max(first)
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RawScanPlan {
    row_len: usize,
    page_size: usize,
    page_stride: u64,
    data_offset: u64,
    compression: crate::metadata::CompressionKind,
    window: RowWindow,
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
            window: builder.row_window(),
        }
    }

    pub(super) const fn window(self) -> RowWindow {
        self.window
    }

    pub(super) const fn page_offset(self, page_index: PageIndex) -> u64 {
        self.data_offset + page_index.0 * self.page_stride
    }

    pub(super) const fn page_size(self) -> usize {
        self.page_size
    }
}

pub(super) fn prepare_row_visit(
    plan: &RawScanPlan,
    stats: &mut ScanStats,
    row_index: RowIndex,
) -> bool {
    stats.rows_seen = stats.rows_seen.saturating_add(1);
    plan.window.contains(row_index)
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
