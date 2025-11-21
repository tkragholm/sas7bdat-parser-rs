use std::convert::TryFrom;
use std::io::{Read, Seek};

use smallvec::SmallVec;

use crate::error::Result;

use super::columnar::{COLUMNAR_BATCH_ROWS, COLUMNAR_INLINE_ROWS, ColumnMajorBatch, ColumnarBatch};
use super::iterator::RowIterator;

pub fn next_columnar_batch<'iter, R: Read + Seek>(
    iter: &'iter mut RowIterator<'_, R>,
    max_rows: usize,
) -> Result<Option<ColumnarBatch<'iter>>> {
    if iter.exhausted.get() {
        return Ok(None);
    }

    let target = if max_rows == 0 {
        COLUMNAR_BATCH_ROWS
    } else {
        max_rows
    };

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

        let mut row_slices = SmallVec::<[&[u8]; COLUMNAR_INLINE_ROWS]>::with_capacity(chunk_len);
        for (offset, row_data) in iter.current_rows[start..row_end].iter().enumerate() {
            let row_index = start + offset;
            let slice = row_data.as_slice(iter.row_length, &iter.page_buffer, row_index as u64)?;
            row_slices.push(slice);
        }

        let batch = ColumnarBatch::new(
            row_slices,
            &iter.columnar_columns,
            iter.parsed.header.endianness,
            iter.encoding,
            false,
        );
        return Ok(Some(batch));
    }
}

pub fn next_column_major_batch<'iter, R: Read + Seek>(
    iter: &'iter mut RowIterator<'_, R>,
    max_rows: usize,
) -> Result<Option<ColumnMajorBatch<'iter>>> {
    if iter.exhausted.get() {
        return Ok(None);
    }

    let target = if max_rows == 0 {
        COLUMNAR_BATCH_ROWS
    } else {
        max_rows
    };

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

        for column in &mut iter.column_major_columns {
            column.prepare_rows(chunk_len);
        }

        for (offset, row_data) in iter.current_rows[start..row_end].iter().enumerate() {
            let row_slice =
                row_data.as_slice(iter.row_length, &iter.page_buffer, (start + offset) as u64)?;
            for column in &mut iter.column_major_columns {
                let runtime = &column.column;
                let cell = row_slice
                    .get(runtime.offset..runtime.offset + runtime.width)
                    .ok_or_else(|| crate::error::Error::Corrupted {
                        section: crate::error::Section::Column {
                            index: runtime.index,
                        },
                        details: std::borrow::Cow::from("column slice out of bounds"),
                    })?;
                column.write_cell(offset, cell);
            }
        }

        iter.row_in_page
            .set(u16::try_from(row_end).unwrap_or(u16::MAX));
        iter.emitted_rows
            .set(iter.emitted_rows.get().saturating_add(chunk_len as u64));

        if iter.emitted_rows.get() >= iter.total_rows {
            iter.exhausted.set(true);
        }

        let batch = ColumnMajorBatch::new(
            &iter.column_major_columns,
            chunk_len,
            iter.parsed.header.endianness,
            iter.encoding,
        );
        return Ok(Some(batch));
    }
}

pub fn next_columnar_batch_contiguous<'iter, R: Read + Seek>(
    iter: &'iter mut RowIterator<'_, R>,
    max_rows: usize,
) -> Result<Option<ColumnarBatch<'iter>>> {
    if iter.exhausted.get() {
        return Ok(None);
    }

    let target = if max_rows == 0 {
        COLUMNAR_BATCH_ROWS
    } else {
        max_rows
    };

    iter.recycle_owned_rows();
    if target > 0 {
        let target_bytes = target.saturating_mul(iter.row_length);
        if iter.columnar_owned_buffer.capacity() < target_bytes {
            iter.columnar_owned_buffer
                .reserve(target_bytes - iter.columnar_owned_buffer.capacity());
        }
    }

    let mut copied_rows = 0usize;
    while copied_rows < target {
        if !iter.ensure_page_ready()? {
            break;
        }

        let page_total = usize::from(iter.page_row_count.get());
        let start = usize::from(iter.row_in_page.get());
        if start >= page_total {
            continue;
        }

        let available = page_total - start;
        let remaining = target - copied_rows;
        let chunk_len = available.min(remaining);
        let row_end = start + chunk_len;

        for row_index in start..row_end {
            let row_data = iter.current_rows[row_index].as_slice(
                iter.row_length,
                &iter.page_buffer,
                row_index as u64,
            )?;
            iter.columnar_owned_buffer.extend_from_slice(row_data);
        }

        copied_rows += chunk_len;

        iter.row_in_page
            .set(u16::try_from(row_end).unwrap_or(u16::MAX));
        iter.emitted_rows
            .set(iter.emitted_rows.get().saturating_add(chunk_len as u64));

        if iter.emitted_rows.get() >= iter.total_rows {
            iter.exhausted.set(true);
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
        iter.parsed.header.endianness,
        iter.encoding,
        true,
    );
    Ok(Some(batch))
}
