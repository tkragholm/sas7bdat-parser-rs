use crate::{
    error::{Error, Result},
    internal::{
        LayoutPlan, PageDescriptor, PageDescriptorTable, PageExecClass, PageKind, RowSpan,
        RowSpanKind, SAS_PAGE_TYPE_COMP, classify_page, parse_subheader_signature, read_u32,
        read_u64,
    },
    types::{ByteOffset, PageIndex, PageSlice, RowIndex},
};
use std::{
    io::{Read, Seek, SeekFrom},
    ops::ControlFlow,
    time::Instant,
};

const SUBHEADER_POINTER_OFFSET: usize = 8;
const SIG_ROW_SIZE: u32 = 0xF7F7_F7F7;
const SIG_COLUMN_SIZE: u32 = 0xF6F6_F6F6;
const SIG_COLUMN_TEXT: u32 = 0xFFFF_FFFD;
const SIG_COLUMN_NAME: u32 = 0xFFFF_FFFF;
const SIG_COLUMN_ATTRS: u32 = 0xFFFF_FFFC;
const SIG_COLUMN_FORMAT: u32 = 0xFFFF_FBFE;
const SIG_COUNTS: u32 = 0xFFFF_FC00;
const SIG_COLUMN_LIST: u32 = 0xFFFF_FFFE;

pub fn walk_pages<R, F>(
    reader: &mut R,
    header: &crate::internal::HeaderInfo,
    mut visit: F,
) -> Result<()>
where
    R: Read + Seek,
    F: FnMut(u64, &[u8]) -> Result<ControlFlow<()>>,
{
    let mut page = vec![0u8; usize::from(header.page_size)];
    for page_index in 0..header.page_count {
        let page_offset = header.data_offset + page_index * u64::from(header.page_size);
        reader
            .seek(SeekFrom::Start(page_offset))
            .map_err(|e| page_io_error(&e))?;
        reader
            .read_exact(&mut page)
            .map_err(|e| page_io_error(&e))?;
        if visit(page_index, &page)? == ControlFlow::Break(()) {
            break;
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct DescriptorInputs {
    page_index: u64,
    page_type: u16,
    page_row_count: u64,
    subheader_count: u64,
    row_base: u64,
}

#[derive(Clone, Copy)]
struct IndexedDescriptorInputs {
    page_index: u64,
    kind: PageKind,
    page_row_count: u64,
    subheader_count: u64,
    row_base: u64,
    remaining_rows: u64,
}

#[derive(Debug, Clone, Default)]
pub struct DescriptorBreakdown {
    pub page_read_ns: u128,
    pub header_read_ns: u128,
    pub classify_ns: u128,
    pub total_ns: u128,
    pub pages_seen: u64,
    pub descriptors_emitted: usize,
    pub row_spans_emitted: usize,
    pub total_candidate_rows: u64,
}

pub fn compile_page_descriptors<R: Read + Seek>(
    reader: &mut R,
    layout: &LayoutPlan,
) -> Result<PageDescriptorTable> {
    let (table, _) = compile_page_descriptors_inner::<R, false>(reader, layout)?;
    Ok(table)
}

pub fn compile_page_descriptors_in_memory(
    file_bytes: &[u8],
    layout: &LayoutPlan,
) -> Result<PageDescriptorTable> {
    let (table, _) = compile_page_descriptors_inner_in_memory::<false>(file_bytes, layout)?;
    Ok(table)
}

pub fn compile_page_descriptors_breakdown<R: Read + Seek>(
    reader: &mut R,
    layout: &LayoutPlan,
) -> Result<DescriptorBreakdown> {
    let (_, breakdown) = compile_page_descriptors_inner::<R, true>(reader, layout)?;
    Ok(breakdown)
}

pub fn compile_page_descriptors_breakdown_in_memory(
    file_bytes: &[u8],
    layout: &LayoutPlan,
) -> Result<DescriptorBreakdown> {
    let (_, breakdown) = compile_page_descriptors_inner_in_memory::<true>(file_bytes, layout)?;
    Ok(breakdown)
}

/// Reads the wall clock only when `PROFILE` is set. On the default open path
/// (`PROFILE == false`) this is a compile-time `None` and the call is elided,
/// so descriptor compilation pays no per-page clock cost.
// `inline(always)` is deliberate: it guarantees the `PROFILE == false` branch
// folds away so the non-profiling path has zero overhead.
#[allow(clippy::inline_always)]
#[inline(always)]
fn profile_now<const PROFILE: bool>() -> Option<Instant> {
    PROFILE.then(Instant::now)
}

#[allow(clippy::inline_always)]
#[inline(always)]
fn accumulate_ns(acc: &mut u128, start: Option<Instant>) {
    if let Some(start) = start {
        *acc += start.elapsed().as_nanos();
    }
}

/// Bytes read per syscall while walking page headers. Matches the scan reader's extent
/// size, which measured highest on network storage.
const DESCRIPTOR_BLOCK_BYTES: usize = 4 * 1024 * 1024;

fn compile_page_descriptors_inner<R: Read + Seek, const PROFILE: bool>(
    reader: &mut R,
    layout: &LayoutPlan,
) -> Result<(PageDescriptorTable, DescriptorBreakdown)> {
    let header = &layout.header;
    if header.page_header_size > u32::from(header.page_size) {
        return Err(page_corruption(
            "page header size exceeds configured page size",
        ));
    }
    if u32::from(layout.row_len) == 0 {
        return Ok((
            PageDescriptorTable::default(),
            DescriptorBreakdown::default(),
        ));
    }

    let total_start = profile_now::<PROFILE>();
    let mut descriptors = Vec::with_capacity(
        usize::try_from(header.page_count)
            .map_err(|_| page_corruption("page count exceeds usize"))?,
    );
    let mut row_spans = Vec::new();
    let mut row_base = 0u64;
    let mut breakdown = DescriptorBreakdown::default();
    // Pages are contiguous and walked in order, so they are read in blocks rather than one
    // syscall each. This pass runs before any row is decoded, and a 100 GB file holds on the
    // order of a million pages, so per-page round-trips add directly to wall-clock.
    let page_size = usize::from(header.page_size);
    let pages_per_block = (DESCRIPTOR_BLOCK_BYTES / page_size.max(1)).max(1);
    let mut block = vec![0u8; pages_per_block * page_size];
    let mut block_first_page = 0u64;
    let mut block_pages = 0usize;

    for page_index in 0..header.page_count {
        let read_start = profile_now::<PROFILE>();
        if block_pages == 0 || page_index >= block_first_page + block_pages as u64 {
            let remaining = header.page_count - page_index;
            let want = usize::try_from(remaining.min(pages_per_block as u64)).unwrap_or(1);
            let offset = header.data_offset + page_index * u64::from(header.page_size);
            reader
                .seek(SeekFrom::Start(offset))
                .map_err(|e| page_io_error(&e))?;
            reader
                .read_exact(&mut block[..want * page_size])
                .map_err(|e| page_io_error(&e))?;
            block_first_page = page_index;
            block_pages = want;
        }
        let page_start = usize::try_from(page_index - block_first_page).unwrap_or(0) * page_size;
        let page = &block[page_start..page_start + page_size];
        accumulate_ns(&mut breakdown.page_read_ns, read_start);
        breakdown.pages_seen += 1;

        let header_start = profile_now::<PROFILE>();
        let page_type = read_header_u16(page, header.page_header_size as usize - 8, layout)?;
        let page_row_count = u64::from(read_header_u16(
            page,
            header.page_header_size as usize - 6,
            layout,
        )?);
        let subheader_count = u64::from(read_header_u16(
            page,
            header.page_header_size as usize - 4,
            layout,
        )?);
        accumulate_ns(&mut breakdown.header_read_ns, header_start);

        let classify_start = profile_now::<PROFILE>();
        let descriptor = classify_descriptor(
            layout,
            page,
            DescriptorInputs {
                page_index,
                page_type,
                page_row_count,
                subheader_count,
                row_base,
            },
            &mut row_spans,
        )?;
        accumulate_ns(&mut breakdown.classify_ns, classify_start);

        row_base = row_base.saturating_add(u64::from(descriptor.row_count));
        descriptors.push(descriptor);
        if row_base >= layout.total_rows {
            break;
        }
    }

    accumulate_ns(&mut breakdown.total_ns, total_start);
    breakdown.descriptors_emitted = descriptors.len();
    breakdown.row_spans_emitted = row_spans.len();
    breakdown.total_candidate_rows = row_base;

    Ok((
        PageDescriptorTable {
            pages: descriptors.into_boxed_slice(),
            row_spans: row_spans.into_boxed_slice(),
            total_candidate_rows: row_base,
        },
        breakdown,
    ))
}

fn compile_page_descriptors_inner_in_memory<const PROFILE: bool>(
    file_bytes: &[u8],
    layout: &LayoutPlan,
) -> Result<(PageDescriptorTable, DescriptorBreakdown)> {
    let header = &layout.header;
    if header.page_header_size > u32::from(header.page_size) {
        return Err(page_corruption(
            "page header size exceeds configured page size",
        ));
    }
    if u32::from(layout.row_len) == 0 {
        return Ok((
            PageDescriptorTable::default(),
            DescriptorBreakdown::default(),
        ));
    }

    let total_start = profile_now::<PROFILE>();
    let mut descriptors = Vec::with_capacity(
        usize::try_from(header.page_count)
            .map_err(|_| page_corruption("page count exceeds usize"))?,
    );
    let mut row_spans = Vec::new();
    let mut row_base = 0u64;
    let mut breakdown = DescriptorBreakdown::default();

    for page_index in 0..header.page_count {
        let read_start = profile_now::<PROFILE>();
        let page = page_slice(file_bytes, header, page_index)?;
        accumulate_ns(&mut breakdown.page_read_ns, read_start);
        breakdown.pages_seen += 1;

        let header_start = profile_now::<PROFILE>();
        let page_type = read_header_u16(page, header.page_header_size as usize - 8, layout)?;
        let page_row_count = u64::from(read_header_u16(
            page,
            header.page_header_size as usize - 6,
            layout,
        )?);
        let subheader_count = u64::from(read_header_u16(
            page,
            header.page_header_size as usize - 4,
            layout,
        )?);
        accumulate_ns(&mut breakdown.header_read_ns, header_start);

        let classify_start = profile_now::<PROFILE>();
        let descriptor = classify_descriptor(
            layout,
            page,
            DescriptorInputs {
                page_index,
                page_type,
                page_row_count,
                subheader_count,
                row_base,
            },
            &mut row_spans,
        )?;
        accumulate_ns(&mut breakdown.classify_ns, classify_start);

        row_base = row_base.saturating_add(u64::from(descriptor.row_count));
        descriptors.push(descriptor);
        if row_base >= layout.total_rows {
            break;
        }
    }

    accumulate_ns(&mut breakdown.total_ns, total_start);
    breakdown.descriptors_emitted = descriptors.len();
    breakdown.row_spans_emitted = row_spans.len();
    breakdown.total_candidate_rows = row_base;

    Ok((
        PageDescriptorTable {
            pages: descriptors.into_boxed_slice(),
            row_spans: row_spans.into_boxed_slice(),
            total_candidate_rows: row_base,
        },
        breakdown,
    ))
}

fn page_slice<'a>(
    file_bytes: &'a [u8],
    header: &crate::internal::HeaderInfo,
    page_index: u64,
) -> Result<&'a [u8]> {
    let page_offset = header.data_offset + page_index * u64::from(header.page_size);
    let start =
        usize::try_from(page_offset).map_err(|_| page_corruption("page offset exceeds usize"))?;
    let end = start
        .checked_add(usize::from(header.page_size))
        .ok_or_else(|| page_corruption("page end exceeds usize"))?;
    file_bytes
        .get(start..end)
        .ok_or_else(|| page_corruption("page extends beyond backing buffer"))
}

fn empty_page_descriptor(
    page_index: u64,
    row_base: u64,
    exec_class: PageExecClass,
) -> PageDescriptor {
    PageDescriptor {
        page_index: PageIndex::from(page_index),
        row_base: RowIndex::from(row_base),
        row_count: 0,
        data_start: ByteOffset::from(0),
        row_span_start: 0,
        row_span_count: 0,
        exec_class,
    }
}

fn calculate_rows_to_take(
    layout: &LayoutPlan,
    kind: PageKind,
    possible_rows: u64,
    page_row_count: u64,
    remaining_rows: u64,
) -> u64 {
    match kind {
        PageKind::Mix => {
            let mix_limit = if layout.rows_per_page == 0 {
                possible_rows
            } else {
                layout.rows_per_page
            };
            mix_limit.min(possible_rows)
        }
        PageKind::Data => {
            let header_limit = if page_row_count == 0 {
                possible_rows
            } else {
                page_row_count
            };
            header_limit.min(possible_rows)
        }
        _ => 0,
    }
    .min(remaining_rows)
}

#[allow(clippy::too_many_lines)]
fn classify_descriptor(
    layout: &LayoutPlan,
    page: &[u8],
    inputs: DescriptorInputs,
    row_spans: &mut Vec<RowSpan>,
) -> Result<PageDescriptor> {
    let DescriptorInputs {
        page_index,
        page_type,
        page_row_count,
        subheader_count,
        row_base,
    } = inputs;
    if (page_type & SAS_PAGE_TYPE_COMP) != 0 {
        return Ok(empty_page_descriptor(
            page_index,
            row_base,
            PageExecClass::MetadataOrEmpty,
        ));
    }
    let kind = classify_page(page_type);
    let data_like = matches!(
        kind,
        PageKind::Data | PageKind::Mix | PageKind::Amd | PageKind::Comp
    ) || (layout.compression != crate::metadata::CompressionKind::None
        && matches!(kind, PageKind::Meta | PageKind::Meta2));
    if !data_like {
        let exec_class = match kind {
            PageKind::Comp | PageKind::CompTable => PageExecClass::IndexedCompressedRows,
            _ => PageExecClass::MetadataOrEmpty,
        };
        return Ok(empty_page_descriptor(page_index, row_base, exec_class));
    }

    if layout.compression != crate::metadata::CompressionKind::None
        && subheader_count == 0
        && !matches!(kind, PageKind::Data)
    {
        return Ok(empty_page_descriptor(
            page_index,
            row_base,
            PageExecClass::IndexedCompressedRows,
        ));
    }

    let remaining_rows = layout.total_rows.saturating_sub(row_base);
    if remaining_rows == 0 {
        return Ok(empty_page_descriptor(
            page_index,
            row_base,
            PageExecClass::MetadataOrEmpty,
        ));
    }

    if subheader_count != 0 && !matches!(kind, PageKind::Data) {
        return classify_indexed_descriptor(
            layout,
            page,
            IndexedDescriptorInputs {
                page_index,
                kind,
                page_row_count,
                subheader_count,
                row_base,
                remaining_rows,
            },
            row_spans,
        );
    }

    let data_start = if matches!(kind, PageKind::Data) {
        layout.header.page_header_size
    } else {
        contiguous_data_start(layout, page, kind, subheader_count)?
    };
    let page_len = u64::try_from(page.len()).unwrap_or(u64::MAX);
    if u64::from(data_start) >= page_len {
        return Ok(empty_page_descriptor(
            page_index,
            row_base,
            PageExecClass::MetadataOrEmpty,
        ));
    }

    let row_len = u64::from(layout.row_len);
    if row_len == 0 {
        return Ok(empty_page_descriptor(
            page_index,
            row_base,
            PageExecClass::MetadataOrEmpty,
        ));
    }

    let available = page_len.saturating_sub(u64::from(data_start));
    let possible_rows = available / row_len;
    if possible_rows == 0 {
        return Ok(empty_page_descriptor(
            page_index,
            row_base,
            PageExecClass::MetadataOrEmpty,
        ));
    }

    let rows_to_take =
        calculate_rows_to_take(layout, kind, possible_rows, page_row_count, remaining_rows);

    let row_count = u32::try_from(rows_to_take).unwrap_or(u32::MAX);
    if row_count == 0 {
        return Ok(empty_page_descriptor(
            page_index,
            row_base,
            PageExecClass::MetadataOrEmpty,
        ));
    }

    Ok(PageDescriptor {
        page_index: PageIndex::from(page_index),
        row_base: RowIndex::from(row_base),
        row_count,
        data_start: ByteOffset::from(data_start),
        row_span_start: 0,
        row_span_count: 0,
        exec_class: PageExecClass::FusedContiguousUncompressed,
    })
}

fn parse_subheader_pointers(
    layout: &LayoutPlan,
    page: &[u8],
    subheader_count: usize,
    target_rows: Option<u64>,
    row_span_start: u32,
    row_spans: &mut Vec<RowSpan>,
    remaining_rows: u64,
) -> Result<()> {
    let header = &layout.header;
    let pointer_size = usize::try_from(header.subheader_pointer_size)
        .map_err(|_| page_corruption("pointer size exceeds usize"))?;
    let min_data_offset = header.page_header_size as usize + subheader_count * pointer_size;
    let mut pointer_cursor = header.page_header_size as usize;

    for _ in 0..subheader_count {
        if target_rows.is_some_and(|target| {
            u64::from(
                u32::try_from(row_spans.len())
                    .unwrap_or(u32::MAX)
                    .saturating_sub(row_span_start),
            ) >= target
        }) {
            break;
        }

        let pointer_end = pointer_cursor.saturating_add(pointer_size);
        let Some(pointer_bytes) = page.get(pointer_cursor..pointer_end) else {
            break;
        };
        pointer_cursor = pointer_end;

        let pointer = parse_pointer(pointer_bytes, header)?;
        if pointer.offset < min_data_offset || pointer.length == 0 {
            continue;
        }
        let data_end = pointer.offset.saturating_add(pointer.length);
        if data_end > page.len() {
            continue;
        }

        match pointer.compression {
            0 => {
                let data = &page[pointer.offset..data_end];
                if pointer.is_compressed_data
                    && !signature_is_recognized(
                        parse_subheader_signature(header, data).unwrap_or(0),
                    )
                {
                    let row_len = usize::from(layout.row_len);
                    let mut local_offset = pointer.offset;
                    let mut remaining = pointer.length;
                    while remaining >= row_len {
                        let produced = u64::from(
                            u32::try_from(row_spans.len())
                                .unwrap_or(u32::MAX)
                                .saturating_sub(row_span_start),
                        );
                        if produced >= remaining_rows
                            || target_rows.is_some_and(|target| produced >= target)
                        {
                            break;
                        }
                        row_spans.push(RowSpan {
                            offset: ByteOffset::from(
                                u32::try_from(local_offset).map_err(|_| {
                                    page_corruption("borrowed row offset exceeds u32")
                                })?,
                            ),
                            len: u32::from(layout.row_len),
                            kind: RowSpanKind::Borrowed,
                        });
                        local_offset = local_offset.saturating_add(row_len);
                        remaining = remaining.saturating_sub(row_len);
                    }
                }
            }
            1 => {}
            4 => {
                let produced = u64::from(
                    u32::try_from(row_spans.len())
                        .unwrap_or(u32::MAX)
                        .saturating_sub(row_span_start),
                );
                if produced < remaining_rows && target_rows.is_none_or(|target| produced < target) {
                    row_spans.push(RowSpan {
                        offset: ByteOffset::from(
                            u32::try_from(pointer.offset).map_err(|_| {
                                page_corruption("compressed row offset exceeds u32")
                            })?,
                        ),
                        len: u32::try_from(pointer.length)
                            .map_err(|_| page_corruption("compressed row length exceeds u32"))?,
                        kind: RowSpanKind::Compressed,
                    });
                }
            }
            _ => {
                return Err(page_corruption(format!(
                    "unsupported subheader compression mode {}",
                    pointer.compression
                )));
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn classify_indexed_descriptor(
    layout: &LayoutPlan,
    page: &[u8],
    inputs: IndexedDescriptorInputs,
    row_spans: &mut Vec<RowSpan>,
) -> Result<PageDescriptor> {
    let IndexedDescriptorInputs {
        page_index,
        kind,
        page_row_count,
        subheader_count,
        row_base,
        remaining_rows,
    } = inputs;

    let header = &layout.header;
    let pointer_size = usize::try_from(header.subheader_pointer_size)
        .map_err(|_| page_corruption("pointer size exceeds usize"))?;
    let max_subheaders = page.len().saturating_sub(header.page_header_size as usize) / pointer_size;
    let subheader_count = usize::try_from(subheader_count)
        .unwrap_or(usize::MAX)
        .min(max_subheaders);
    let target_rows = if page_row_count == 0 {
        None
    } else {
        Some(page_row_count.min(remaining_rows))
    };

    let row_span_start = u32::try_from(row_spans.len()).unwrap_or(u32::MAX);

    parse_subheader_pointers(
        layout,
        page,
        subheader_count,
        target_rows,
        row_span_start,
        row_spans,
        remaining_rows,
    )?;

    let row_span_count = u32::try_from(row_spans.len())
        .unwrap_or(u32::MAX)
        .saturating_sub(row_span_start);
    let data_start = contiguous_data_start(layout, page, kind, subheader_count as u64)?;
    let page_len = u64::try_from(page.len()).unwrap_or(u64::MAX);
    let available = page_len.saturating_sub(u64::from(data_start));
    let row_len = u64::from(layout.row_len);
    let possible_rows = available.checked_div(row_len).unwrap_or(0);
    let rows_to_take =
        calculate_rows_to_take(layout, kind, possible_rows, page_row_count, remaining_rows);

    let produced_rows = u64::from(row_span_count);
    if row_span_count > 0 && produced_rows < rows_to_take {
        push_contiguous_row_spans(
            row_spans,
            data_start,
            u32::from(layout.row_len),
            rows_to_take - produced_rows,
        )?;
    }

    let row_span_count = u32::try_from(row_spans.len())
        .unwrap_or(u32::MAX)
        .saturating_sub(row_span_start);
    if row_span_count > 0 {
        let span_start = usize::try_from(row_span_start).unwrap_or(usize::MAX);
        let span_end = span_start.saturating_add(usize::try_from(row_span_count).unwrap_or(0));
        let has_compressed_spans = row_spans
            .get(span_start..span_end)
            .into_iter()
            .flatten()
            .any(|span| span.kind == RowSpanKind::Compressed);
        return Ok(PageDescriptor {
            page_index: PageIndex::from(page_index),
            row_base: RowIndex::from(row_base),
            row_count: row_span_count,
            data_start: ByteOffset::from(0),
            row_span_start,
            row_span_count,
            exec_class: if has_compressed_spans {
                PageExecClass::IndexedCompressedRows
            } else {
                PageExecClass::IndexedPointerRows
            },
        });
    }

    if row_len == 0 || available < row_len {
        return Ok(empty_page_descriptor(
            page_index,
            row_base,
            PageExecClass::MetadataOrEmpty,
        ));
    }
    let possible_rows = available / row_len;
    let rows_to_take =
        calculate_rows_to_take(layout, kind, possible_rows, page_row_count, remaining_rows);
    let row_count = u32::try_from(rows_to_take).unwrap_or(u32::MAX);
    let exec_class = if row_count == 0 {
        PageExecClass::MetadataOrEmpty
    } else {
        PageExecClass::FusedContiguousUncompressed
    };
    Ok(PageDescriptor {
        page_index: PageIndex::from(page_index),
        row_base: RowIndex::from(row_base),
        row_count,
        data_start: if row_count == 0 {
            ByteOffset::from(0)
        } else {
            ByteOffset::from(data_start)
        },
        row_span_start: 0,
        row_span_count: 0,
        exec_class,
    })
}

fn push_contiguous_row_spans(
    row_spans: &mut Vec<RowSpan>,
    data_start: u32,
    row_len: u32,
    row_count: u64,
) -> Result<()> {
    let row_len_usize =
        usize::try_from(row_len).map_err(|_| page_corruption("row length exceeds usize"))?;
    let mut offset = usize::try_from(data_start)
        .map_err(|_| page_corruption("contiguous row offset exceeds usize"))?;
    for _ in 0..row_count {
        row_spans.push(RowSpan {
            offset: ByteOffset::from(
                u32::try_from(offset)
                    .map_err(|_| page_corruption("contiguous row offset exceeds u32"))?,
            ),
            len: row_len,
            kind: RowSpanKind::Borrowed,
        });
        offset = offset.saturating_add(row_len_usize);
    }
    Ok(())
}

fn contiguous_data_start(
    layout: &LayoutPlan,
    page: &[u8],
    page_kind: PageKind,
    subheader_count: u64,
) -> Result<u32> {
    let header = &layout.header;
    let pointer_size = u64::from(header.subheader_pointer_size);
    let pointer_section_len = subheader_count.saturating_mul(pointer_size);
    let base_offset = u64::from(header.page_header_size).saturating_add(pointer_section_len);

    let bit_offset = if header.uses_u64_pointers {
        32_u64
    } else {
        16_u64
    };
    let alignment_base =
        bit_offset + u64::try_from(SUBHEADER_POINTER_OFFSET).unwrap_or(0) + pointer_section_len;
    let align_adjust = if alignment_base % 8 == 0 {
        0
    } else {
        8 - (alignment_base % 8)
    };
    let mut data_start = base_offset.saturating_add(align_adjust);

    // StatTransfer-written files (release "X.0000M0") place row data immediately after the
    // subheader-pointer array instead of padding the start up to an 8-byte boundary the way
    // SAS does. When `align_adjust` padded us forward but the bytes at `base_offset` are real
    // data (not zero/space padding), undo the pad — otherwise every column is shifted by the
    // pad width. Observed on a 32-bit StatTransfer file whose data mix page has an odd
    // subheader count (base_offset % 8 == 4), which `readstat` decodes at `base_offset`.
    if matches!(page_kind, PageKind::Mix)
        && align_adjust != 0
        && is_stattransfer_release(&header.release)
    {
        let base = usize::try_from(base_offset).unwrap_or(0);
        let looks_padded = page.get(base..base.saturating_add(4)).is_some_and(|bytes| {
            let word = u32::from_le_bytes(bytes.try_into().unwrap_or([0; 4]));
            word == 0 || word == 0x2020_2020
        });
        if !looks_padded {
            data_start = base_offset;
        }
    }

    u32::try_from(data_start).map_err(|_| page_corruption("page data start exceeds u32"))
}

fn is_stattransfer_release(release: &str) -> bool {
    let bytes = release.as_bytes();
    if bytes.len() != 8 || bytes[1] != b'.' || (bytes[6] != b'M' && bytes[6] != b'J') {
        return false;
    }

    let major = match bytes[0] {
        b'1'..=b'9' => u16::from(bytes[0] - b'0'),
        b'V' => 9,
        _ => return false,
    };

    let Some(minor) = std::str::from_utf8(&bytes[2..6])
        .ok()
        .and_then(|digits| digits.parse::<u16>().ok())
    else {
        return false;
    };

    let revision = match bytes[7] {
        b'0'..=b'9' => u16::from(bytes[7] - b'0'),
        _ => return false,
    };

    (major == 8 || major == 9) && minor == 0 && revision == 0
}

fn read_header_u16(page: &[u8], start: usize, layout: &LayoutPlan) -> Result<u16> {
    let Ok(start) = u32::try_from(start) else {
        return Err(page_corruption("page header field exceeds page bounds"));
    };
    PageSlice::new(page)
        .get_u16(ByteOffset::from(start), layout.header.endianness)
        .ok_or_else(|| page_corruption("page header field exceeds page bounds"))
}

#[derive(Debug, Clone, Copy)]
struct PointerInfo {
    offset: usize,
    length: usize,
    compression: u8,
    is_compressed_data: bool,
}

fn parse_pointer(pointer: &[u8], header: &crate::internal::HeaderInfo) -> Result<PointerInfo> {
    if header.uses_u64_pointers {
        if pointer.len() < 18 {
            return Err(page_corruption("64-bit pointer too short"));
        }
        Ok(PointerInfo {
            offset: usize::try_from(read_u64(header.endianness, &pointer[0..8]))
                .map_err(|_| page_corruption("pointer offset exceeds usize"))?,
            length: usize::try_from(read_u64(header.endianness, &pointer[8..16]))
                .map_err(|_| page_corruption("pointer length exceeds usize"))?,
            compression: pointer[16],
            is_compressed_data: pointer[17] != 0,
        })
    } else {
        if pointer.len() < 10 {
            return Err(page_corruption("32-bit pointer too short"));
        }
        Ok(PointerInfo {
            offset: usize::try_from(read_u32(header.endianness, &pointer[0..4]))
                .map_err(|_| page_corruption("pointer offset exceeds usize"))?,
            length: usize::try_from(read_u32(header.endianness, &pointer[4..8]))
                .map_err(|_| page_corruption("pointer length exceeds usize"))?,
            compression: pointer[8],
            is_compressed_data: pointer[9] != 0,
        })
    }
}

const fn signature_is_recognized(signature: u32) -> bool {
    matches!(
        signature,
        SIG_COLUMN_TEXT
            | SIG_COLUMN_ATTRS
            | SIG_COLUMN_FORMAT
            | SIG_COLUMN_NAME
            | SIG_COLUMN_SIZE
            | SIG_ROW_SIZE
            | SIG_COUNTS
            | SIG_COLUMN_LIST
    )
}

fn page_corruption(message: impl Into<String>) -> Error {
    Error::page_corruption(message)
}

fn page_io_error(err: &std::io::Error) -> Error {
    page_corruption(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        internal::{PageExecClass, RowSpanKind, SAS_PAGE_TYPE_DATA, SAS_PAGE_TYPE_MIX},
        metadata::CompressionKind,
        test_utils::*,
    };
    use std::io::Cursor;

    #[test]
    fn compiles_fused_descriptors_for_simple_data_pages() {
        let mut bytes = Vec::new();
        bytes.extend(make_page(SAS_PAGE_TYPE_DATA, 2, 0, &[b"ABCD", b"EFGH"], 64));
        bytes.extend(make_page(SAS_PAGE_TYPE_MIX, 0, 0, &[b"IJKL"], 64));

        let layout = simple_layout(64, 2, 4, 3);
        let mut cursor = Cursor::new(bytes);
        let descriptors = compile_page_descriptors(&mut cursor, &layout).expect("descriptors");

        assert_eq!(descriptors.pages.len(), 2);
        assert_eq!(descriptors.total_candidate_rows, 3);

        let first = descriptors.pages[0];
        assert_eq!(first.exec_class, PageExecClass::FusedContiguousUncompressed);
        assert_eq!(first.row_base, crate::types::RowIndex(0));
        assert_eq!(first.row_count, 2);
        assert_eq!(first.data_start, crate::types::ByteOffset(24));
        assert_eq!(first.row_span_count, 0);

        let second = descriptors.pages[1];
        assert_eq!(
            second.exec_class,
            PageExecClass::FusedContiguousUncompressed
        );
        assert_eq!(second.row_base, crate::types::RowIndex(2));
        assert_eq!(second.row_count, 1);
        assert_eq!(second.data_start, crate::types::ByteOffset(24));
        assert_eq!(second.row_span_count, 0);
    }

    #[test]
    fn stattransfer_mix_data_start_is_not_padded_when_rows_follow_pointers() {
        // 32-bit, 17 subheaders → base_offset = 24 + 17*12 = 228 (% 8 == 4), which SAS would
        // pad to 232. A StatTransfer file ("9.0000M0") instead places rows at 228; padding
        // would shift every column by 4 (regression: NYYTS_2000_2020_PublicUse).
        let mut layout = simple_layout(512, 1, 16, 1);
        layout.header.release = "9.0000M0".to_owned();

        // Real (non-padding) data at offset 228.
        let mut page = vec![0u8; 512];
        page[228..232].copy_from_slice(&[0x00, 0x40, 0x9f, 0x40]);
        assert_eq!(
            contiguous_data_start(&layout, &page, PageKind::Mix, 17).expect("data start"),
            228,
            "StatTransfer rows sit at base_offset, not the 8-padded offset"
        );

        // If those 4 bytes are zero/space padding, the pad is real → keep the aligned 232.
        let padded = vec![0u8; 512];
        assert_eq!(
            contiguous_data_start(&layout, &padded, PageKind::Mix, 17).expect("data start"),
            232,
            "zero padding at base_offset means the row data really is 8-aligned"
        );

        // A non-StatTransfer release always pads to 8, regardless of the bytes.
        layout.header.release = "9.0401M3".to_owned();
        assert_eq!(
            contiguous_data_start(&layout, &page, PageKind::Mix, 17).expect("data start"),
            232,
            "non-StatTransfer files keep the 8-byte alignment"
        );
    }

    #[test]
    fn marks_pointer_pages_as_indexed() {
        let bytes = make_pointer_page(&[b"ABCD"], 64);
        let layout = simple_layout(64, 1, 4, 1);
        let mut cursor = Cursor::new(bytes);
        let descriptors = compile_page_descriptors(&mut cursor, &layout).expect("descriptors");

        assert_eq!(descriptors.pages.len(), 1);
        assert_eq!(
            descriptors.pages[0].exec_class,
            PageExecClass::IndexedPointerRows
        );
        assert_eq!(descriptors.pages[0].row_count, 1);
        assert_eq!(descriptors.pages[0].row_span_count, 1);
        assert_eq!(descriptors.row_spans.len(), 1);
        assert_eq!(
            descriptors.row_spans[0].offset,
            crate::types::ByteOffset(40)
        );
        assert_eq!(descriptors.row_spans[0].len, 4);
        assert_eq!(descriptors.row_spans[0].kind, RowSpanKind::Borrowed);
    }

    fn assert_compressed_page_descriptors(descriptors: &crate::internal::PageDescriptorTable) {
        assert_eq!(descriptors.pages.len(), 1);
        assert_eq!(
            descriptors.pages[0].exec_class,
            PageExecClass::IndexedCompressedRows
        );
        assert_eq!(descriptors.pages[0].row_count, 1);
        assert_eq!(descriptors.pages[0].row_span_count, 1);
        assert_eq!(descriptors.row_spans[0].kind, RowSpanKind::Compressed);
    }

    #[test]
    fn marks_compressed_pointer_pages_as_compressed() {
        let bytes = make_compressed_page(&[0xC1u8, b'A'], 64, 4);
        let mut layout = simple_layout(64, 1, 4, 1);
        layout.compression = CompressionKind::Row;
        let mut cursor = Cursor::new(bytes);
        let descriptors = compile_page_descriptors(&mut cursor, &layout).expect("descriptors");

        assert_compressed_page_descriptors(&descriptors);
    }

    #[test]
    fn compressed_meta_pages_compile_row_spans() {
        let bytes = make_compressed_meta_page(&[0xC1u8, b'A'], 64);
        let mut layout = simple_layout(64, 1, 4, 1);
        layout.compression = CompressionKind::Row;
        let mut cursor = Cursor::new(bytes);
        let descriptors = compile_page_descriptors(&mut cursor, &layout).expect("descriptors");

        assert_compressed_page_descriptors(&descriptors);
    }

    #[test]
    fn skips_pages_with_comp_mask_bit_set_before_pointer_decode() {
        let mut page = vec![0u8; 64];
        let page_type = 0x1000u16;
        write_page_header(&mut page, page_type, 1, 1);
        page[24..28].copy_from_slice(&40u32.to_le_bytes());
        page[28..32].copy_from_slice(&4u32.to_le_bytes());
        page[32] = 88;
        page[33] = 1;

        let layout = simple_layout(64, 1, 4, 1);
        let mut cursor = Cursor::new(page);
        let descriptors = compile_page_descriptors(&mut cursor, &layout).expect("descriptors");

        assert_eq!(descriptors.pages.len(), 1);
        assert_eq!(
            descriptors.pages[0].exec_class,
            PageExecClass::MetadataOrEmpty
        );
        assert_eq!(descriptors.pages[0].row_count, 0);
        assert!(descriptors.row_spans.is_empty());
    }

    #[test]
    fn mix_page_skips_hidden_padding_for_non_stattransfer_release() {
        let mut page = vec![0u8; 64];
        page[36..40].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
        let mut layout = simple_layout(64, 1, 4, 1);
        layout.header.release = "9.0401M3".to_owned();

        let data_start =
            contiguous_data_start(&layout, &page, super::PageKind::Mix, 1).expect("data start");

        assert_eq!(data_start, 40);
    }

    #[test]
    fn stattransfer_release_detection_matches_reference_rule() {
        assert!(is_stattransfer_release("V.0000M0"));
        assert!(is_stattransfer_release("9.0000M0"));
        assert!(!is_stattransfer_release("9.0401M3"));
    }
}
