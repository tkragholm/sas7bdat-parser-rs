use super::{
    buffer::RowData,
    columnar::{COLUMNAR_BATCH_ROWS, COLUMNAR_INLINE_ROWS, ColumnarBatch, RowSegment},
    iterator::RowIteratorCore,
};
use crate::{
    dataset::Compression,
    error::{Error, Result, Section},
    parser::metadata::DatasetLayout,
};
use bytes::Bytes;
use smallvec::SmallVec;
use std::{
    borrow::Cow,
    convert::TryFrom,
    io::{Read, Seek},
    ops::Deref,
};

// Cap columnar staging to avoid enormous allocations when row_length is very large.
const MAX_COLUMNAR_BUFFER_BYTES: usize = 512 * 1024 * 1024;
const ADAPTIVE_MIN_BATCHES_BEFORE_SWITCH: usize = 3;
const ADAPTIVE_WIDE_ROW_BYTES: usize = 256;
const ADAPTIVE_MAX_SEGMENTED_ROWS: usize = 8192;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnarBatchMode {
    Segmented,
    Contiguous,
    Adaptive,
}

#[derive(Debug, Clone, Copy)]
pub struct AdaptiveBatchState {
    initialized: bool,
    active_mode: ColumnarBatchMode,
    pending_mode: Option<ColumnarBatchMode>,
    pending_batches: usize,
    observations: u64,
    switches: u64,
}

impl Default for AdaptiveBatchState {
    fn default() -> Self {
        Self {
            initialized: false,
            active_mode: ColumnarBatchMode::Segmented,
            pending_mode: None,
            pending_batches: 0,
            observations: 0,
            switches: 0,
        }
    }
}

impl AdaptiveBatchState {
    pub(crate) const fn observations(&self) -> u64 {
        self.observations
    }

    pub(crate) const fn switches(&self) -> u64 {
        self.switches
    }

    pub(crate) fn select_mode(&mut self, desired_mode: ColumnarBatchMode) -> ColumnarBatchMode {
        self.observations = self.observations.saturating_add(1);

        if !self.initialized {
            self.initialized = true;
            self.active_mode = desired_mode;
            self.pending_mode = None;
            self.pending_batches = 0;
            return self.active_mode;
        }

        if desired_mode == self.active_mode {
            self.pending_mode = None;
            self.pending_batches = 0;
            return self.active_mode;
        }

        if self.pending_mode == Some(desired_mode) {
            self.pending_batches = self.pending_batches.saturating_add(1);
        } else {
            self.pending_mode = Some(desired_mode);
            self.pending_batches = 1;
        }

        if self.pending_batches >= ADAPTIVE_MIN_BATCHES_BEFORE_SWITCH {
            if self.active_mode != desired_mode {
                self.switches = self.switches.saturating_add(1);
            }
            self.active_mode = desired_mode;
            self.pending_mode = None;
            self.pending_batches = 0;
        }

        self.active_mode
    }
}

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

fn resolve_target_with_remaining<R, L>(
    iter: &RowIteratorCore<R, L>,
    max_rows: usize,
) -> Option<(usize, usize)>
where
    R: Read + Seek,
    L: Deref<Target = DatasetLayout>,
{
    let target = resolve_target(&iter.exhausted, max_rows)?;
    let remaining_rows = usize::try_from(
        iter.total_rows
            .saturating_sub(iter.emitted_rows.get())
            .min(usize::MAX as u64),
    )
    .unwrap_or(usize::MAX);
    Some((target, remaining_rows))
}

fn next_page_chunk<R, L>(
    iter: &mut RowIteratorCore<R, L>,
    target: usize,
) -> Result<Option<PageChunk>>
where
    R: Read + Seek,
    L: Deref<Target = DatasetLayout>,
{
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

#[inline]
fn page_snapshot(page_snapshot: &mut Option<Bytes>, page_buffer: &[u8]) -> Bytes {
    page_snapshot
        .get_or_insert_with(|| Bytes::copy_from_slice(page_buffer))
        .clone()
}

fn push_segment_row<R, L>(
    iter: &RowIteratorCore<R, L>,
    row_index: usize,
    page_snapshot_cache: &mut Option<Bytes>,
    row_segments: &mut SmallVec<[RowSegment; COLUMNAR_INLINE_ROWS]>,
) -> Result<()>
where
    R: Read + Seek,
    L: Deref<Target = DatasetLayout>,
{
    if let Some(base) = iter.contiguous_base {
        let offset = base + row_index.saturating_mul(iter.row_length);
        let end = offset.saturating_add(iter.row_length);
        if end > iter.page_buffer.len() {
            return Err(Error::Corrupted {
                section: Section::Row {
                    index: row_index as u64,
                },
                details: Cow::from("row offset exceeds page bounds"),
            });
        }
        let page = page_snapshot(page_snapshot_cache, &iter.page_buffer);
        row_segments.push(RowSegment::paged(page, offset, iter.row_length));
        return Ok(());
    }

    let row = iter
        .current_rows
        .get(row_index)
        .ok_or_else(|| Error::Corrupted {
            section: Section::Row {
                index: row_index as u64,
            },
            details: Cow::from("row index out of bounds for current page"),
        })?;

    match row {
        RowData::Borrowed(offset) => {
            let end = offset.saturating_add(iter.row_length);
            if end > iter.page_buffer.len() {
                return Err(Error::Corrupted {
                    section: Section::Row {
                        index: row_index as u64,
                    },
                    details: Cow::from("row offset exceeds page bounds"),
                });
            }
            let page = page_snapshot(page_snapshot_cache, &iter.page_buffer);
            row_segments.push(RowSegment::paged(page, *offset, iter.row_length));
        }
        RowData::Owned(buffer) => {
            // Compressed rows are already materialized into owned buffers per row.
            row_segments.push(RowSegment::owned(Bytes::copy_from_slice(buffer.as_slice())));
        }
    }

    Ok(())
}

fn desired_adaptive_mode<R, L>(
    iter: &RowIteratorCore<R, L>,
    target_rows: usize,
) -> ColumnarBatchMode
where
    R: Read + Seek,
    L: Deref<Target = DatasetLayout>,
{
    if matches!(
        iter.layout.row_info.compression,
        Compression::Row | Compression::Binary
    ) {
        return ColumnarBatchMode::Contiguous;
    }

    if iter.row_length >= ADAPTIVE_WIDE_ROW_BYTES && target_rows <= ADAPTIVE_MAX_SEGMENTED_ROWS {
        return ColumnarBatchMode::Segmented;
    }

    ColumnarBatchMode::Contiguous
}

fn effective_batch_mode<R, L>(
    iter: &mut RowIteratorCore<R, L>,
    max_rows: usize,
) -> ColumnarBatchMode
where
    R: Read + Seek,
    L: Deref<Target = DatasetLayout>,
{
    let target_rows = if max_rows == 0 {
        COLUMNAR_BATCH_ROWS
    } else {
        max_rows
    };
    let desired = desired_adaptive_mode(iter, target_rows);
    iter.adaptive_batch_state.select_mode(desired)
}

pub fn next_columnar_batch_with_mode<R, L>(
    iter: &mut RowIteratorCore<R, L>,
    max_rows: usize,
    mode: ColumnarBatchMode,
) -> Result<Option<ColumnarBatch<'_>>>
where
    R: Read + Seek,
    L: Deref<Target = DatasetLayout>,
{
    let effective_mode = match mode {
        ColumnarBatchMode::Segmented => ColumnarBatchMode::Segmented,
        ColumnarBatchMode::Contiguous => ColumnarBatchMode::Contiguous,
        ColumnarBatchMode::Adaptive => effective_batch_mode(iter, max_rows),
    };
    iter.last_batch_mode = effective_mode;

    match effective_mode {
        ColumnarBatchMode::Segmented => next_columnar_batch(iter, max_rows),
        ColumnarBatchMode::Contiguous => next_columnar_batch_contiguous(iter, max_rows),
        ColumnarBatchMode::Adaptive => unreachable!("adaptive mode resolves to a concrete mode"),
    }
}

pub fn next_columnar_batch<R, L>(
    iter: &mut RowIteratorCore<R, L>,
    max_rows: usize,
) -> Result<Option<ColumnarBatch<'_>>>
where
    R: Read + Seek,
    L: Deref<Target = DatasetLayout>,
{
    let Some((target, remaining_rows)) = resolve_target_with_remaining(iter, max_rows) else {
        return Ok(None);
    };

    let effective_target = target.min(remaining_rows);
    if effective_target == 0 {
        return Ok(None);
    }

    let mut row_segments =
        SmallVec::<[RowSegment; COLUMNAR_INLINE_ROWS]>::with_capacity(effective_target);

    while row_segments.len() < effective_target {
        let remaining = effective_target - row_segments.len();
        let Some(chunk) = next_page_chunk(iter, remaining)? else {
            break;
        };

        let mut page_snapshot_cache = None;
        for row_index in chunk.start..chunk.row_end {
            push_segment_row(iter, row_index, &mut page_snapshot_cache, &mut row_segments)?;
        }

        if iter.exhausted.get() {
            break;
        }
    }

    if row_segments.is_empty() {
        return Ok(None);
    }

    let batch = ColumnarBatch::new(
        row_segments,
        &iter.columnar_columns,
        iter.layout.header.endianness,
        iter.encoding,
        true,
    );
    Ok(Some(batch))
}

pub fn next_columnar_batch_contiguous<R, L>(
    iter: &mut RowIteratorCore<R, L>,
    max_rows: usize,
) -> Result<Option<ColumnarBatch<'_>>>
where
    R: Read + Seek,
    L: Deref<Target = DatasetLayout>,
{
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
        let remaining = effective_target - copied_rows;
        let Some(chunk) = next_page_chunk(iter, remaining)? else {
            break;
        };

        for row_index in chunk.start..chunk.row_end {
            iter.append_row_to_owned_buffer(u16::try_from(row_index).unwrap_or(u16::MAX))?;
        }

        copied_rows += chunk.chunk_len;

        if iter.exhausted.get() {
            break;
        }
    }

    if copied_rows == 0 {
        return Ok(None);
    }

    let mut row_segments = SmallVec::<[RowSegment; COLUMNAR_INLINE_ROWS]>::with_capacity(
        copied_rows.min(COLUMNAR_INLINE_ROWS),
    );
    let contiguous = Bytes::copy_from_slice(
        &iter.columnar_owned_buffer[..copied_rows.saturating_mul(iter.row_length)],
    );
    let mut offset = 0usize;
    for _ in 0..copied_rows {
        row_segments.push(RowSegment::paged(
            contiguous.clone(),
            offset,
            iter.row_length,
        ));
        offset = offset.saturating_add(iter.row_length);
    }

    let batch = ColumnarBatch::new(
        row_segments,
        &iter.columnar_columns,
        iter.layout.header.endianness,
        iter.encoding,
        true,
    );
    Ok(Some(batch))
}

#[cfg(test)]
mod tests {
    use super::{AdaptiveBatchState, ColumnarBatchMode};

    #[test]
    fn adaptive_state_switches_after_hysteresis_window() {
        let mut state = AdaptiveBatchState::default();
        assert_eq!(state.active_mode, ColumnarBatchMode::Segmented);

        assert_eq!(
            state.select_mode(ColumnarBatchMode::Contiguous),
            ColumnarBatchMode::Contiguous
        );
        assert_eq!(
            state.select_mode(ColumnarBatchMode::Segmented),
            ColumnarBatchMode::Contiguous
        );
        assert_eq!(
            state.select_mode(ColumnarBatchMode::Segmented),
            ColumnarBatchMode::Contiguous
        );
        assert_eq!(
            state.select_mode(ColumnarBatchMode::Segmented),
            ColumnarBatchMode::Segmented
        );
        assert_eq!(state.active_mode, ColumnarBatchMode::Segmented);
        assert_eq!(state.observations(), 4);
        assert_eq!(state.switches(), 1);
    }

    #[test]
    fn adaptive_state_does_not_flap_under_alternating_desires() {
        let mut state = AdaptiveBatchState::default();
        assert_eq!(
            state.select_mode(ColumnarBatchMode::Contiguous),
            ColumnarBatchMode::Contiguous
        );

        for i in 0..12 {
            let desired = if i % 2 == 0 {
                ColumnarBatchMode::Segmented
            } else {
                ColumnarBatchMode::Contiguous
            };
            let _ = state.select_mode(desired);
        }

        assert_eq!(state.active_mode, ColumnarBatchMode::Contiguous);
        assert_eq!(state.switches(), 0);
        assert_eq!(state.observations(), 13);
    }
}
