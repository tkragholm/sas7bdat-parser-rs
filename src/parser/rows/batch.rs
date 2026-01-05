use std::convert::TryFrom;
use std::io::{Read, Seek};

use smallvec::SmallVec;

use crate::error::Result;

use super::columnar::{COLUMNAR_BATCH_ROWS, COLUMNAR_INLINE_ROWS, ColumnarBatch};
use super::iterator::RowIterator;

// Cap columnar staging to avoid enormous allocations when row_length is very large.
const MAX_COLUMNAR_BUFFER_BYTES: usize = 512 * 1024 * 1024;

struct PageChunk {
    start: usize,
    row_end: usize,
    chunk_len: usize,
}

#[inline]
const fn resolve_target(iter_exhausted: &std::cell::Cell<bool>, max_rows: usize) -> Option<usize> {
    if iter_exhausted.get() {
        return None;
    }
    Some(if max_rows == 0 {
        COLUMNAR_BATCH_ROWS
    } else {
        max_rows
    })
}

fn resolve_target_with_remaining<R: Read + Seek>(
    iter: &RowIterator<'_, R>,
    max_rows: usize,
) -> Option<(usize, usize)> {
    let target = resolve_target(&iter.exhausted, max_rows)?;
    let remaining_rows = usize::try_from(
        iter.total_rows
            .saturating_sub(iter.emitted_rows.get())
            .min(usize::MAX as u64),
    )
    .unwrap_or(usize::MAX);
    Some((target, remaining_rows))
}

fn next_page_chunk<R: Read + Seek>(
    iter: &mut RowIterator<'_, R>,
    target: usize,
) -> Result<Option<PageChunk>> {
    loop {
        if !iter.ensure_page_ready()? {
            return Ok(None);
        }

        let page_total = usize::from(iter.page_row_count.get());
        let start = usize::from(iter.row_in_page.get());
        if start >= page_total {
            continue;
        }

        let available = page_total - start;
        let chunk_len = available.min(target);
        let row_end = start + chunk_len;

        iter.row_in_page
            .set(u16::try_from(row_end).unwrap_or(u16::MAX));
        iter.emitted_rows
            .set(iter.emitted_rows.get().saturating_add(chunk_len as u64));

        if iter.emitted_rows.get() >= iter.total_rows {
            iter.exhausted.set(true);
        }

        return Ok(Some(PageChunk {
            start,
            row_end,
            chunk_len,
        }));
    }
}

pub fn next_columnar_batch<'iter, R: Read + Seek>(
    iter: &'iter mut RowIterator<'_, R>,
    max_rows: usize,
) -> Result<Option<ColumnarBatch<'iter>>> {
    let Some((target, _)) = resolve_target_with_remaining(iter, max_rows) else {
        return Ok(None);
    };

    let Some(chunk) = next_page_chunk(iter, target)? else {
        return Ok(None);
    };

    let mut row_slices = SmallVec::<[&[u8]; COLUMNAR_INLINE_ROWS]>::with_capacity(chunk.chunk_len);
    for (offset, row_data) in iter.current_rows[chunk.start..chunk.row_end]
        .iter()
        .enumerate()
    {
        let row_index = chunk.start + offset;
        let slice = row_data.as_slice(iter.row_length, &iter.page_buffer, row_index as u64)?;
        row_slices.push(slice);
    }

    let batch = ColumnarBatch::new(
        row_slices,
        &iter.columnar_columns,
        iter.layout.header.endianness,
        iter.encoding,
        false,
    );
    Ok(Some(batch))
}

pub fn next_columnar_batch_contiguous<'iter, R: Read + Seek>(
    iter: &'iter mut RowIterator<'_, R>,
    max_rows: usize,
) -> Result<Option<ColumnarBatch<'iter>>> {
    let Some((target, remaining_rows)) = resolve_target_with_remaining(iter, max_rows) else {
        return Ok(None);
    };

    let mut effective_target = target.min(remaining_rows);
    if effective_target == 0 {
        return Ok(None);
    }

    // Avoid preallocating more than the cap; clamp rows to fit within the budget.
    if iter.row_length > 0
        && effective_target.saturating_mul(iter.row_length) > MAX_COLUMNAR_BUFFER_BYTES
    {
        effective_target = MAX_COLUMNAR_BUFFER_BYTES.max(iter.row_length) / iter.row_length;
        effective_target = effective_target.max(1);
    }

    iter.recycle_owned_rows();
    let target_bytes = effective_target.saturating_mul(iter.row_length);
    if iter.columnar_owned_buffer.capacity() < target_bytes {
        iter.columnar_owned_buffer
            .reserve(target_bytes - iter.columnar_owned_buffer.capacity());
    }

    let mut copied_rows = 0usize;
    while copied_rows < effective_target {
        let remaining = target - copied_rows;
        let Some(chunk) = next_page_chunk(iter, remaining)? else {
            break;
        };

        for row_index in chunk.start..chunk.row_end {
            let row_data = iter.current_rows[row_index].as_slice(
                iter.row_length,
                &iter.page_buffer,
                row_index as u64,
            )?;
            iter.columnar_owned_buffer.extend_from_slice(row_data);
        }

        copied_rows += chunk.chunk_len;

        if iter.exhausted.get() {
            break;
        }
    }

    if copied_rows == 0 {
        return Ok(None);
    }

    let mut row_slices = SmallVec::<[&[u8]; COLUMNAR_INLINE_ROWS]>::with_capacity(
        copied_rows.min(COLUMNAR_INLINE_ROWS),
    );
    let mut offset = 0usize;
    for _ in 0..copied_rows {
        let end = offset + iter.row_length;
        row_slices.push(&iter.columnar_owned_buffer[offset..end]);
        offset = end;
    }

    let batch = ColumnarBatch::new(
        row_slices,
        &iter.columnar_columns,
        iter.layout.header.endianness,
        iter.encoding,
        true,
    );
    Ok(Some(batch))
}
