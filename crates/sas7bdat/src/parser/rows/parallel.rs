use super::{DecodePolicy, row_iterator};
use crate::{
    cell::CellValue,
    error::{Error, Result},
    parser::DatasetLayout,
};
use std::{
    collections::HashMap,
    fs::File,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
};

const DEFAULT_PAGE_CHUNK: u64 = 8;
const DEFAULT_ROW_BATCH_SIZE: usize = 1024;
const DEFAULT_QUEUE_FACTOR: usize = 2;
const DEFAULT_PAGE_CHUNK_FULL_UNORDERED: u64 = 16;
const DEFAULT_PAGE_CHUNK_PROJECTED_UNORDERED: u64 = 4;
const DEFAULT_PAGE_CHUNK_RAW_UNORDERED: u64 = 64;
const TARGET_CHUNKS_PER_WORKER: u64 = 16;
const ROW_CAPACITY_MAX_PAGE_FACTOR: usize = 16;
const CANCEL_POLL_EVERY: usize = 64;
const CANCEL_POLL_MASK: usize = CANCEL_POLL_EVERY - 1;

#[derive(Debug, Clone, Copy)]
pub struct ParallelScanConfig {
    pub threads: usize,
    pub page_chunk: u64,
    pub row_batch_size: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RawScanStats {
    pub rows: u64,
    pub raw_bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RawRowBatch {
    row_end_offsets: Vec<usize>,
    bytes: Vec<u8>,
}

impl RawRowBatch {
    #[must_use]
    pub fn with_capacity(row_capacity: usize, byte_capacity: usize) -> Self {
        Self {
            row_end_offsets: Vec::with_capacity(row_capacity),
            bytes: Vec::with_capacity(byte_capacity),
        }
    }

    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.row_end_offsets.len()
    }

    #[must_use]
    pub const fn raw_bytes(&self) -> usize {
        self.bytes.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.row_end_offsets.is_empty()
    }

    #[must_use]
    pub fn row(&self, index: usize) -> Option<&[u8]> {
        if index >= self.row_end_offsets.len() {
            return None;
        }
        let start = if index == 0 {
            0
        } else {
            self.row_end_offsets[index - 1]
        };
        let end = self.row_end_offsets[index];
        self.bytes.get(start..end)
    }

    #[must_use]
    pub const fn rows(&self) -> RawRowBatchRows<'_> {
        RawRowBatchRows {
            batch: self,
            index: 0,
            start: 0,
        }
    }

    pub(crate) fn push_row(&mut self, row: &[u8]) {
        self.bytes.extend_from_slice(row);
        self.row_end_offsets.push(self.bytes.len());
    }

    pub(crate) fn clear(&mut self) {
        self.row_end_offsets.clear();
        self.bytes.clear();
    }
}

pub struct RawRowBatchRows<'a> {
    batch: &'a RawRowBatch,
    index: usize,
    start: usize,
}

impl<'a> Iterator for RawRowBatchRows<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let end = *self.batch.row_end_offsets.get(self.index)?;
        let row = self.batch.bytes.get(self.start..end)?;
        self.index = self.index.saturating_add(1);
        self.start = end;
        Some(row)
    }
}

impl ParallelScanConfig {
    #[must_use]
    pub const fn new(threads: usize) -> Self {
        Self {
            threads,
            page_chunk: DEFAULT_PAGE_CHUNK,
            row_batch_size: DEFAULT_ROW_BATCH_SIZE,
        }
    }
}

impl Default for ParallelScanConfig {
    fn default() -> Self {
        Self::new(1)
    }
}

/// Scans all rows from a file path and invokes a callback with decoded owned rows.
///
/// # Errors
///
/// Returns an error if file IO, row decoding, or callback execution fails.
pub fn scan_file_rows_with_decode_policy<F>(
    path: &Path,
    layout: &DatasetLayout,
    decode_policy: DecodePolicy,
    config: ParallelScanConfig,
    mut f: F,
) -> Result<u64>
where
    F: FnMut(&[CellValue<'static>]) -> Result<()>,
{
    scan_file_rows_internal(
        path,
        layout,
        decode_policy,
        config,
        RowScanMode::Full {
            projection_legacy: false,
        },
        &mut f,
    )
}

/// Scans projected columns from a file path and invokes a callback with decoded owned rows.
///
/// # Errors
///
/// Returns an error if file IO, projection validation, row decoding, or callback execution fails.
pub fn scan_file_projected_rows_with_decode_policy<F>(
    path: &Path,
    layout: &DatasetLayout,
    indices: &[usize],
    decode_policy: DecodePolicy,
    config: ParallelScanConfig,
    projection_legacy: bool,
    mut f: F,
) -> Result<u64>
where
    F: FnMut(&[CellValue<'static>]) -> Result<()>,
{
    scan_file_rows_internal(
        path,
        layout,
        decode_policy,
        config,
        RowScanMode::Projected {
            projection: indices.to_vec(),
            projection_legacy,
        },
        &mut f,
    )
}

/// Scans raw row bytes from a file path and invokes a callback for each row.
///
/// # Errors
///
/// Returns an error if file IO, raw row iteration, or callback execution fails.
#[allow(clippy::too_many_lines)]
pub fn scan_file_raw_rows<F>(
    path: &Path,
    layout: &DatasetLayout,
    config: ParallelScanConfig,
    mut f: F,
) -> Result<u64>
where
    F: FnMut(&[u8]) -> Result<()>,
{
    enum WorkerChunk {
        Rows { chunk_id: u64, rows: Vec<Vec<u8>> },
        Done,
    }

    let page_count = layout.header.page_count;
    let worker_count = normalized_worker_count(page_count, config.threads);
    if worker_count <= 1 || page_count <= 1 {
        let mut file = File::open(path)?;
        let mut it = row_iterator(&mut file, layout)?;
        let mut rows = 0u64;
        while it
            .try_next_raw_row_visit(&mut |row| {
                f(row)?;
                rows = rows.saturating_add(1);
                Ok(())
            })?
            .is_some()
        {}
        return Ok(rows);
    }

    let cancel = Arc::new(AtomicBool::new(false));
    let next_chunk_id = Arc::new(AtomicU64::new(0));
    let chunk_pages = normalized_chunk_pages_ordered(page_count, worker_count, config.page_chunk);
    let row_batch_size = config.row_batch_size.max(1);
    let queue_capacity = normalized_queue_capacity(worker_count, page_count, chunk_pages);
    let chunk_row_capacity_hint = normalized_chunk_row_capacity_hint(row_batch_size, chunk_pages);

    thread::scope(|scope| -> Result<u64> {
        let (tx, rx) = mpsc::sync_channel::<Result<WorkerChunk>>(queue_capacity);
        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let tx = tx.clone();
            let cancel = Arc::clone(&cancel);
            let next_chunk_id = Arc::clone(&next_chunk_id);
            let input = path.to_path_buf();
            handles.push(scope.spawn(move || {
                let work = || -> Result<()> {
                    let mut file = File::open(&input)?;
                    let mut it = row_iterator(&mut file, layout)?;

                    loop {
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        let chunk_id = next_chunk_id.fetch_add(1, Ordering::Relaxed);
                        let chunk_start = chunk_id.saturating_mul(chunk_pages);
                        if chunk_start >= page_count {
                            break;
                        }
                        let chunk_end = chunk_start.saturating_add(chunk_pages).min(page_count);
                        it.set_page_range(chunk_start, chunk_end)?;

                        let mut rows = Vec::with_capacity(chunk_row_capacity_hint);
                        let mut cancel_poll = 0usize;
                        loop {
                            if should_cancel(cancel.as_ref(), &mut cancel_poll) {
                                break;
                            }
                            let Some(row) = it.try_next_raw_row()? else {
                                break;
                            };
                            rows.push(row.to_vec());
                        }

                        if tx.send(Ok(WorkerChunk::Rows { chunk_id, rows })).is_err() {
                            return Ok(());
                        }
                    }

                    let _ = tx.send(Ok(WorkerChunk::Done));
                    Ok(())
                };

                if let Err(err) = work() {
                    let _ = tx.send(Err(err));
                }
            }));
        }
        drop(tx);

        let mut done_workers = 0usize;
        let mut rows_seen = 0u64;
        let mut next_expected_chunk_id = 0u64;
        let mut pending = HashMap::<u64, Vec<Vec<u8>>>::with_capacity(queue_capacity);
        let mut first_err = None;

        while done_workers < worker_count {
            match rx.recv() {
                Ok(Ok(WorkerChunk::Done)) => {
                    done_workers += 1;
                }
                Ok(Ok(WorkerChunk::Rows { chunk_id, rows })) => {
                    pending.insert(chunk_id, rows);
                }
                Ok(Err(err)) => {
                    first_err = Some(err);
                    cancel.store(true, Ordering::Relaxed);
                    break;
                }
                Err(_) => break,
            }

            while let Some(rows) = pending.remove(&next_expected_chunk_id) {
                for row in rows {
                    f(&row)?;
                    rows_seen = rows_seen.saturating_add(1);
                }
                next_expected_chunk_id = next_expected_chunk_id.saturating_add(1);
                if next_expected_chunk_id.saturating_mul(chunk_pages) >= page_count {
                    break;
                }
            }
        }

        cancel.store(true, Ordering::Relaxed);
        drop(rx);
        for handle in handles {
            if handle.join().is_err() {
                return Err(Error::Unsupported {
                    feature: "parallel raw scan worker panicked".into(),
                });
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(rows_seen)
    })
}

/// Scans all rows from a file path in unordered parallel mode.
///
/// # Errors
///
/// Returns an error if file IO, row decoding, or callback execution fails.
pub fn scan_file_rows_with_decode_policy_unordered<F>(
    path: &Path,
    layout: &DatasetLayout,
    decode_policy: DecodePolicy,
    config: ParallelScanConfig,
    f: F,
) -> Result<u64>
where
    F: for<'row> Fn(&[CellValue<'row>]) -> Result<()> + Send + Sync,
{
    scan_file_rows_unordered_internal(
        path,
        layout,
        decode_policy,
        config,
        RowScanMode::Full {
            projection_legacy: false,
        },
        f,
    )
}

/// Scans projected columns from a file path in unordered parallel mode.
///
/// # Errors
///
/// Returns an error if file IO, projection validation, row decoding, or callback execution fails.
pub fn scan_file_projected_rows_with_decode_policy_unordered<F>(
    path: &Path,
    layout: &DatasetLayout,
    indices: &[usize],
    decode_policy: DecodePolicy,
    config: ParallelScanConfig,
    projection_legacy: bool,
    f: F,
) -> Result<u64>
where
    F: for<'row> Fn(&[CellValue<'row>]) -> Result<()> + Send + Sync,
{
    scan_file_rows_unordered_internal(
        path,
        layout,
        decode_policy,
        config,
        RowScanMode::Projected {
            projection: indices.to_vec(),
            projection_legacy,
        },
        f,
    )
}

/// Scans raw row bytes from a file path in unordered parallel mode.
///
/// # Errors
///
/// Returns an error if file IO, raw row iteration, or callback execution fails.
pub fn scan_file_raw_rows_unordered<F>(
    path: &Path,
    layout: &DatasetLayout,
    config: ParallelScanConfig,
    f: F,
) -> Result<u64>
where
    F: Fn(&[u8]) -> Result<()> + Send + Sync,
{
    Ok(scan_file_raw_rows_unordered_with_stats(path, layout, config, f)?.rows)
}

/// Scans raw row bytes from a file path in unordered parallel mode and returns row/byte stats.
///
/// # Errors
///
/// Returns an error if file IO, raw row iteration, or callback execution fails.
pub fn scan_file_raw_rows_unordered_with_stats<F>(
    path: &Path,
    layout: &DatasetLayout,
    config: ParallelScanConfig,
    f: F,
) -> Result<RawScanStats>
where
    F: Fn(&[u8]) -> Result<()> + Send + Sync,
{
    let page_count = layout.header.page_count;
    let worker_count = normalized_worker_count(page_count, config.threads);
    if worker_count <= 1 || page_count <= 1 {
        let mut file = File::open(path)?;
        let mut it = row_iterator(&mut file, layout)?;
        let mut stats = RawScanStats::default();
        while it
            .try_next_raw_row_visit(&mut |row| {
                f(row)?;
                stats.rows = stats.rows.saturating_add(1);
                stats.raw_bytes = stats.raw_bytes.saturating_add(row.len() as u64);
                Ok(())
            })?
            .is_some()
        {}
        return Ok(stats);
    }

    let cancel = Arc::new(AtomicBool::new(false));
    let next_chunk_id = Arc::new(AtomicU64::new(0));
    let chunk_pages = normalized_chunk_pages_unordered(
        page_count,
        effective_unordered_page_chunk(config.page_chunk, UnorderedChunkProfile::Raw),
    );

    thread::scope(|scope| -> Result<RawScanStats> {
        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let cancel = Arc::clone(&cancel);
            let next_chunk_id = Arc::clone(&next_chunk_id);
            let input = path.to_path_buf();
            let visit = &f;
            handles.push(scope.spawn(move || -> Result<RawScanStats> {
                let work = || -> Result<RawScanStats> {
                    let mut file = File::open(&input)?;
                    let mut it = row_iterator(&mut file, layout)?;
                    let mut stats = RawScanStats::default();

                    loop {
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        let chunk_id = next_chunk_id.fetch_add(1, Ordering::Relaxed);
                        let chunk_start = chunk_id.saturating_mul(chunk_pages);
                        if chunk_start >= page_count {
                            break;
                        }
                        let chunk_end = chunk_start.saturating_add(chunk_pages).min(page_count);
                        it.set_page_range(chunk_start, chunk_end)?;

                        let mut cancel_poll = 0usize;
                        loop {
                            if should_cancel(cancel.as_ref(), &mut cancel_poll) {
                                break;
                            }
                            let Some(row) = it.try_next_raw_row()? else {
                                break;
                            };
                            visit(row)?;
                            stats.rows = stats.rows.saturating_add(1);
                            stats.raw_bytes = stats.raw_bytes.saturating_add(row.len() as u64);
                        }
                    }

                    Ok(stats)
                };

                let result = work();
                if result.is_err() {
                    cancel.store(true, Ordering::Relaxed);
                }
                result
            }));
        }

        let mut stats = RawScanStats::default();
        let mut first_err = None;
        for handle in handles {
            match handle.join() {
                Ok(Ok(worker_stats)) => {
                    stats.rows = stats.rows.saturating_add(worker_stats.rows);
                    stats.raw_bytes = stats.raw_bytes.saturating_add(worker_stats.raw_bytes);
                }
                Ok(Err(err)) => {
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
                Err(_) => {
                    if first_err.is_none() {
                        first_err = Some(Error::Unsupported {
                            feature: "unordered parallel raw scan worker panicked".into(),
                        });
                    }
                }
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(stats)
    })
}

/// Scans raw row bytes from a file path in unordered parallel mode using owned row batches.
///
/// # Errors
///
/// Returns an error if file IO, raw row iteration, batch callback execution, or synchronization fails.
#[allow(clippy::too_many_lines)]
pub fn scan_file_raw_rows_unordered_batched_with_stats<F>(
    path: &Path,
    layout: &DatasetLayout,
    config: ParallelScanConfig,
    batch_rows: usize,
    f: F,
) -> Result<RawScanStats>
where
    F: Fn(&RawRowBatch) -> Result<()> + Send + Sync,
{
    let batch_rows = batch_rows.max(1);
    let batch_byte_capacity = raw_batch_byte_capacity(layout, batch_rows);

    let page_count = layout.header.page_count;
    let worker_count = normalized_worker_count(page_count, config.threads);
    if worker_count <= 1 || page_count <= 1 {
        let mut file = File::open(path)?;
        let mut it = row_iterator(&mut file, layout)?;
        let mut stats = RawScanStats::default();
        let mut batch = RawRowBatch::with_capacity(batch_rows, batch_byte_capacity);
        while let Some(row) = it.try_next_raw_row()? {
            batch.push_row(row);
            stats.rows = stats.rows.saturating_add(1);
            stats.raw_bytes = stats.raw_bytes.saturating_add(row.len() as u64);
            if batch.row_count() >= batch_rows {
                flush_raw_batch(&mut batch, &f)?;
            }
        }
        flush_raw_batch(&mut batch, &f)?;
        return Ok(stats);
    }

    let cancel = Arc::new(AtomicBool::new(false));
    let next_chunk_id = Arc::new(AtomicU64::new(0));
    let chunk_pages = normalized_chunk_pages_unordered(
        page_count,
        effective_unordered_page_chunk(config.page_chunk, UnorderedChunkProfile::Raw),
    );

    thread::scope(|scope| -> Result<RawScanStats> {
        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let cancel = Arc::clone(&cancel);
            let next_chunk_id = Arc::clone(&next_chunk_id);
            let input = path.to_path_buf();
            let visit = &f;
            handles.push(scope.spawn(move || -> Result<RawScanStats> {
                let work = || -> Result<RawScanStats> {
                    let mut file = File::open(&input)?;
                    let mut it = row_iterator(&mut file, layout)?;
                    let mut stats = RawScanStats::default();
                    let mut batch = RawRowBatch::with_capacity(batch_rows, batch_byte_capacity);

                    loop {
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        let chunk_id = next_chunk_id.fetch_add(1, Ordering::Relaxed);
                        let chunk_start = chunk_id.saturating_mul(chunk_pages);
                        if chunk_start >= page_count {
                            break;
                        }
                        let chunk_end = chunk_start.saturating_add(chunk_pages).min(page_count);
                        it.set_page_range(chunk_start, chunk_end)?;

                        let mut cancel_poll = 0usize;
                        loop {
                            if should_cancel(cancel.as_ref(), &mut cancel_poll) {
                                break;
                            }
                            let Some(row) = it.try_next_raw_row()? else {
                                break;
                            };
                            batch.push_row(row);
                            stats.rows = stats.rows.saturating_add(1);
                            stats.raw_bytes = stats.raw_bytes.saturating_add(row.len() as u64);
                            if batch.row_count() >= batch_rows {
                                flush_raw_batch(&mut batch, visit)?;
                            }
                        }
                    }
                    flush_raw_batch(&mut batch, visit)?;
                    Ok(stats)
                };

                let result = work();
                if result.is_err() {
                    cancel.store(true, Ordering::Relaxed);
                }
                result
            }));
        }

        let mut stats = RawScanStats::default();
        let mut first_err = None;
        for handle in handles {
            match handle.join() {
                Ok(Ok(worker_stats)) => {
                    stats.rows = stats.rows.saturating_add(worker_stats.rows);
                    stats.raw_bytes = stats.raw_bytes.saturating_add(worker_stats.raw_bytes);
                }
                Ok(Err(err)) => {
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
                Err(_) => {
                    if first_err.is_none() {
                        first_err = Some(Error::Unsupported {
                            feature: "unordered parallel raw batched scan worker panicked".into(),
                        });
                    }
                }
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(stats)
    })
}

enum RowScanMode {
    Full {
        projection_legacy: bool,
    },
    Projected {
        projection: Vec<usize>,
        projection_legacy: bool,
    },
}

#[allow(clippy::too_many_lines)]
fn scan_file_rows_internal<F>(
    path: &Path,
    layout: &DatasetLayout,
    decode_policy: DecodePolicy,
    config: ParallelScanConfig,
    mode: RowScanMode,
    f: &mut F,
) -> Result<u64>
where
    F: FnMut(&[CellValue<'static>]) -> Result<()>,
{
    enum WorkerChunk {
        Rows {
            chunk_id: u64,
            rows: Vec<Vec<CellValue<'static>>>,
        },
        Done,
    }

    let page_count = layout.header.page_count;
    let worker_count = normalized_worker_count(page_count, config.threads);
    if worker_count <= 1 || page_count <= 1 {
        let mut file = File::open(path)?;
        let mut it = row_iterator(&mut file, layout)?;
        it.set_decode_policy(decode_policy);
        let mut rows_seen = 0u64;
        match mode {
            RowScanMode::Full { .. } => {
                while let Some(row) = it.try_next_owned()? {
                    f(&row)?;
                    rows_seen = rows_seen.saturating_add(1);
                }
            }
            RowScanMode::Projected {
                projection,
                projection_legacy,
            } => {
                if projection_legacy {
                    while let Some(row) = it.try_next()? {
                        let mut projected = Vec::with_capacity(projection.len());
                        for &idx in &projection {
                            projected.push(row[idx].clone().into_owned());
                        }
                        f(&projected)?;
                        rows_seen = rows_seen.saturating_add(1);
                    }
                } else {
                    while let Some(row) = it.try_next_projected(&projection)? {
                        let owned: Vec<CellValue<'static>> =
                            row.into_iter().map(CellValue::into_owned).collect();
                        f(&owned)?;
                        rows_seen = rows_seen.saturating_add(1);
                    }
                }
            }
        }
        return Ok(rows_seen);
    }

    let cancel = Arc::new(AtomicBool::new(false));
    let next_chunk_id = Arc::new(AtomicU64::new(0));
    let chunk_pages = normalized_chunk_pages_ordered(page_count, worker_count, config.page_chunk);
    let row_batch_size = config.row_batch_size.max(1);
    let queue_capacity = normalized_queue_capacity(worker_count, page_count, chunk_pages);
    let chunk_row_capacity_hint = normalized_chunk_row_capacity_hint(row_batch_size, chunk_pages);

    thread::scope(|scope| -> Result<u64> {
        let (tx, rx) = mpsc::sync_channel::<Result<WorkerChunk>>(queue_capacity);
        let mut handles = Vec::with_capacity(worker_count);
        let projection = match &mode {
            RowScanMode::Full { .. } => Vec::new(),
            RowScanMode::Projected { projection, .. } => projection.clone(),
        };
        let projection_legacy = match mode {
            RowScanMode::Full {
                projection_legacy, ..
            }
            | RowScanMode::Projected {
                projection_legacy, ..
            } => projection_legacy,
        };
        let full_mode = matches!(mode, RowScanMode::Full { .. });

        for _ in 0..worker_count {
            let tx = tx.clone();
            let cancel = Arc::clone(&cancel);
            let next_chunk_id = Arc::clone(&next_chunk_id);
            let input = path.to_path_buf();
            let projection = projection.clone();
            handles.push(scope.spawn(move || {
                let work = || -> Result<()> {
                    let mut file = File::open(&input)?;
                    let mut it = row_iterator(&mut file, layout)?;
                    it.set_decode_policy(decode_policy);

                    loop {
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        let chunk_id = next_chunk_id.fetch_add(1, Ordering::Relaxed);
                        let chunk_start = chunk_id.saturating_mul(chunk_pages);
                        if chunk_start >= page_count {
                            break;
                        }
                        let chunk_end = chunk_start.saturating_add(chunk_pages).min(page_count);
                        it.set_page_range(chunk_start, chunk_end)?;

                        let mut rows = Vec::with_capacity(chunk_row_capacity_hint);
                        let mut cancel_poll = 0usize;
                        if full_mode {
                            loop {
                                if should_cancel(cancel.as_ref(), &mut cancel_poll) {
                                    break;
                                }
                                let Some(row) = it.try_next_owned()? else {
                                    break;
                                };
                                rows.push(row);
                            }
                        } else if projection_legacy {
                            loop {
                                if should_cancel(cancel.as_ref(), &mut cancel_poll) {
                                    break;
                                }
                                let Some(row) = it.try_next()? else {
                                    break;
                                };
                                let mut projected = Vec::with_capacity(projection.len());
                                for &idx in &projection {
                                    projected.push(row[idx].clone().into_owned());
                                }
                                rows.push(projected);
                            }
                        } else {
                            loop {
                                if should_cancel(cancel.as_ref(), &mut cancel_poll) {
                                    break;
                                }
                                let Some(row) = it.try_next_projected(&projection)? else {
                                    break;
                                };
                                rows.push(row.into_iter().map(CellValue::into_owned).collect());
                            }
                        }

                        if tx.send(Ok(WorkerChunk::Rows { chunk_id, rows })).is_err() {
                            return Ok(());
                        }
                    }

                    let _ = tx.send(Ok(WorkerChunk::Done));
                    Ok(())
                };

                if let Err(err) = work() {
                    let _ = tx.send(Err(err));
                }
            }));
        }
        drop(tx);

        let mut done_workers = 0usize;
        let mut rows_seen = 0u64;
        let mut next_expected_chunk_id = 0u64;
        let mut pending =
            HashMap::<u64, Vec<Vec<CellValue<'static>>>>::with_capacity(queue_capacity);
        let mut first_err = None;

        while done_workers < worker_count {
            match rx.recv() {
                Ok(Ok(WorkerChunk::Done)) => {
                    done_workers += 1;
                }
                Ok(Ok(WorkerChunk::Rows { chunk_id, rows })) => {
                    pending.insert(chunk_id, rows);
                }
                Ok(Err(err)) => {
                    first_err = Some(err);
                    cancel.store(true, Ordering::Relaxed);
                    break;
                }
                Err(_) => break,
            }

            while let Some(rows) = pending.remove(&next_expected_chunk_id) {
                for row in rows {
                    f(&row)?;
                    rows_seen = rows_seen.saturating_add(1);
                }
                next_expected_chunk_id = next_expected_chunk_id.saturating_add(1);
                if next_expected_chunk_id.saturating_mul(chunk_pages) >= page_count {
                    break;
                }
            }
        }

        cancel.store(true, Ordering::Relaxed);
        drop(rx);
        for handle in handles {
            if handle.join().is_err() {
                return Err(Error::Unsupported {
                    feature: "parallel row scan worker panicked".into(),
                });
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(rows_seen)
    })
}

#[allow(clippy::too_many_lines)]
fn scan_file_rows_unordered_internal<F>(
    path: &Path,
    layout: &DatasetLayout,
    decode_policy: DecodePolicy,
    config: ParallelScanConfig,
    mode: RowScanMode,
    f: F,
) -> Result<u64>
where
    F: for<'row> Fn(&[CellValue<'row>]) -> Result<()> + Send + Sync,
{
    let page_count = layout.header.page_count;
    let worker_count = normalized_worker_count(page_count, config.threads);
    if worker_count <= 1 || page_count <= 1 {
        let mut file = File::open(path)?;
        let mut it = row_iterator(&mut file, layout)?;
        it.set_decode_policy(decode_policy);
        let mut rows_seen = 0u64;
        match mode {
            RowScanMode::Full { .. } => {
                while let Some(row) = it.try_next()? {
                    f(&row)?;
                    rows_seen = rows_seen.saturating_add(1);
                }
            }
            RowScanMode::Projected {
                projection,
                projection_legacy,
            } => {
                if projection_legacy {
                    while let Some(row) = it.try_next()? {
                        let mut projected = Vec::with_capacity(projection.len());
                        for &idx in &projection {
                            projected.push(row[idx].clone());
                        }
                        f(&projected)?;
                        rows_seen = rows_seen.saturating_add(1);
                    }
                } else {
                    while let Some(row) = it.try_next_projected(&projection)? {
                        let owned: Vec<CellValue<'static>> =
                            row.into_iter().map(CellValue::into_owned).collect();
                        f(&owned)?;
                        rows_seen = rows_seen.saturating_add(1);
                    }
                }
            }
        }
        return Ok(rows_seen);
    }

    let cancel = Arc::new(AtomicBool::new(false));
    let next_chunk_id = Arc::new(AtomicU64::new(0));
    let chunk_pages = normalized_chunk_pages_unordered(
        page_count,
        effective_unordered_page_chunk(
            config.page_chunk,
            match mode {
                RowScanMode::Full { .. } => UnorderedChunkProfile::Full,
                RowScanMode::Projected { .. } => UnorderedChunkProfile::Projected,
            },
        ),
    );

    thread::scope(|scope| -> Result<u64> {
        let projection = match &mode {
            RowScanMode::Full { .. } => Vec::new(),
            RowScanMode::Projected { projection, .. } => projection.clone(),
        };
        let projection_legacy = match mode {
            RowScanMode::Full {
                projection_legacy, ..
            }
            | RowScanMode::Projected {
                projection_legacy, ..
            } => projection_legacy,
        };
        let full_mode = matches!(mode, RowScanMode::Full { .. });

        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let cancel = Arc::clone(&cancel);
            let next_chunk_id = Arc::clone(&next_chunk_id);
            let input = path.to_path_buf();
            let projection = projection.clone();
            let visit = &f;
            handles.push(scope.spawn(move || -> Result<u64> {
                let work = || -> Result<u64> {
                    let mut file = File::open(&input)?;
                    let mut it = row_iterator(&mut file, layout)?;
                    it.set_decode_policy(decode_policy);
                    let compiled_projection = if !full_mode && !projection_legacy {
                        Some(it.resolve_compiled_runtime_columns(&projection)?)
                    } else {
                        None
                    };
                    let mut rows_seen = 0u64;

                    loop {
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        let chunk_id = next_chunk_id.fetch_add(1, Ordering::Relaxed);
                        let chunk_start = chunk_id.saturating_mul(chunk_pages);
                        if chunk_start >= page_count {
                            break;
                        }
                        let chunk_end = chunk_start.saturating_add(chunk_pages).min(page_count);
                        it.set_page_range(chunk_start, chunk_end)?;

                        let mut cancel_poll = 0usize;
                        if full_mode {
                            loop {
                                if should_cancel(cancel.as_ref(), &mut cancel_poll) {
                                    break;
                                }
                                let Some(row) = it.try_next()? else {
                                    break;
                                };
                                visit(&row)?;
                                rows_seen = rows_seen.saturating_add(1);
                            }
                        } else if projection_legacy {
                            loop {
                                if should_cancel(cancel.as_ref(), &mut cancel_poll) {
                                    break;
                                }
                                let Some(row) = it.try_next()? else {
                                    break;
                                };
                                let mut projected = Vec::with_capacity(projection.len());
                                for &idx in &projection {
                                    projected.push(row[idx].clone());
                                }
                                visit(&projected)?;
                                rows_seen = rows_seen.saturating_add(1);
                            }
                        } else {
                            let Some(compiled_projection) = compiled_projection.as_deref() else {
                                return Err(Error::Unsupported {
                                    feature: "missing compiled projection in unordered scan".into(),
                                });
                            };
                            let mut visit_projected = |values: &[CellValue<'_>]| -> Result<()> {
                                visit(values)?;
                                rows_seen = rows_seen.saturating_add(1);
                                Ok(())
                            };
                            loop {
                                if should_cancel(cancel.as_ref(), &mut cancel_poll) {
                                    break;
                                }
                                let Some(()) = it.try_next_projected_compiled_columns_visit(
                                    compiled_projection,
                                    &mut visit_projected,
                                )?
                                else {
                                    break;
                                };
                            }
                        }
                    }

                    Ok(rows_seen)
                };

                let result = work();
                if result.is_err() {
                    cancel.store(true, Ordering::Relaxed);
                }
                result
            }));
        }

        let mut rows_seen = 0u64;
        let mut first_err = None;
        for handle in handles {
            match handle.join() {
                Ok(Ok(worker_rows)) => {
                    rows_seen = rows_seen.saturating_add(worker_rows);
                }
                Ok(Err(err)) => {
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
                Err(_) => {
                    if first_err.is_none() {
                        first_err = Some(Error::Unsupported {
                            feature: "unordered parallel row scan worker panicked".into(),
                        });
                    }
                }
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(rows_seen)
    })
}

fn raw_batch_byte_capacity(layout: &DatasetLayout, batch_rows: usize) -> usize {
    let row_length = usize::try_from(layout.row_info.row_length).unwrap_or(0);
    row_length.saturating_mul(batch_rows.max(1))
}

fn flush_raw_batch<F>(batch: &mut RawRowBatch, visit: &F) -> Result<()>
where
    F: Fn(&RawRowBatch) -> Result<()>,
{
    if batch.is_empty() {
        return Ok(());
    }
    visit(batch)?;
    batch.clear();
    Ok(())
}

#[inline]
fn should_cancel(cancel: &AtomicBool, iteration: &mut usize) -> bool {
    let canceled = (*iteration & CANCEL_POLL_MASK) == 0 && cancel.load(Ordering::Relaxed);
    *iteration = iteration.wrapping_add(1);
    canceled
}

fn normalized_worker_count(page_count: u64, threads: usize) -> usize {
    if page_count == 0 {
        return 1;
    }
    threads
        .max(1)
        .min(usize::try_from(page_count).unwrap_or(usize::MAX))
}

fn normalized_chunk_pages_ordered(
    page_count: u64,
    worker_count: usize,
    configured_page_chunk: u64,
) -> u64 {
    let configured = configured_page_chunk.max(1);
    if page_count == 0 {
        return configured;
    }
    let workers_u64 = u64::try_from(worker_count.max(1)).unwrap_or(u64::MAX);
    let target_chunks = workers_u64.saturating_mul(TARGET_CHUNKS_PER_WORKER).max(1);
    let adaptive = ceil_div_u64(page_count, target_chunks).max(1);
    configured.max(adaptive).min(page_count)
}

fn normalized_chunk_pages_unordered(page_count: u64, configured_page_chunk: u64) -> u64 {
    let configured = configured_page_chunk.max(1);
    if page_count == 0 {
        return configured;
    }
    configured.min(page_count)
}

#[derive(Clone, Copy)]
enum UnorderedChunkProfile {
    Full,
    Projected,
    Raw,
}

const fn effective_unordered_page_chunk(
    configured_page_chunk: u64,
    profile: UnorderedChunkProfile,
) -> u64 {
    if configured_page_chunk != DEFAULT_PAGE_CHUNK {
        return configured_page_chunk;
    }
    match profile {
        UnorderedChunkProfile::Full => DEFAULT_PAGE_CHUNK_FULL_UNORDERED,
        UnorderedChunkProfile::Projected => DEFAULT_PAGE_CHUNK_PROJECTED_UNORDERED,
        UnorderedChunkProfile::Raw => DEFAULT_PAGE_CHUNK_RAW_UNORDERED,
    }
}

fn normalized_queue_capacity(worker_count: usize, page_count: u64, chunk_pages: u64) -> usize {
    if page_count == 0 {
        return 1;
    }
    let chunks = ceil_div_u64(page_count, chunk_pages.max(1));
    let max_chunks = usize::try_from(chunks).unwrap_or(usize::MAX);
    let desired = worker_count
        .saturating_mul(DEFAULT_QUEUE_FACTOR)
        .saturating_mul(4)
        .max(1);
    desired.min(max_chunks.max(1)).max(1)
}

fn normalized_chunk_row_capacity_hint(row_batch_size: usize, chunk_pages: u64) -> usize {
    let pages_factor = usize::try_from(chunk_pages)
        .unwrap_or(ROW_CAPACITY_MAX_PAGE_FACTOR)
        .clamp(1, ROW_CAPACITY_MAX_PAGE_FACTOR);
    row_batch_size.max(1).saturating_mul(pages_factor)
}

const fn ceil_div_u64(value: u64, divisor: u64) -> u64 {
    if divisor == 0 {
        return value;
    }
    value.saturating_add(divisor.saturating_sub(1)) / divisor
}

#[cfg(test)]
mod tests {
    use super::{
        RawRowBatch, UnorderedChunkProfile, effective_unordered_page_chunk,
        normalized_chunk_pages_ordered, normalized_chunk_pages_unordered,
        normalized_chunk_row_capacity_hint, normalized_queue_capacity,
    };

    #[test]
    fn normalized_chunk_pages_ordered_scales_for_large_inputs() {
        let chunk_pages = normalized_chunk_pages_ordered(4096, 8, 8);
        assert_eq!(chunk_pages, 32);
    }

    #[test]
    fn normalized_chunk_pages_ordered_keeps_configured_floor() {
        let chunk_pages = normalized_chunk_pages_ordered(64, 8, 8);
        assert_eq!(chunk_pages, 8);
    }

    #[test]
    fn normalized_chunk_pages_unordered_keeps_configured_chunk() {
        let chunk_pages = normalized_chunk_pages_unordered(4096, 8);
        assert_eq!(chunk_pages, 8);
    }

    #[test]
    fn effective_unordered_page_chunk_uses_profile_defaults() {
        assert_eq!(
            effective_unordered_page_chunk(8, UnorderedChunkProfile::Full),
            16
        );
        assert_eq!(
            effective_unordered_page_chunk(8, UnorderedChunkProfile::Projected),
            4
        );
        assert_eq!(
            effective_unordered_page_chunk(8, UnorderedChunkProfile::Raw),
            64
        );
    }

    #[test]
    fn effective_unordered_page_chunk_respects_override() {
        assert_eq!(
            effective_unordered_page_chunk(24, UnorderedChunkProfile::Projected),
            24
        );
    }

    #[test]
    fn normalized_queue_capacity_is_bounded_by_chunk_count() {
        let capacity = normalized_queue_capacity(8, 64, 64);
        assert_eq!(capacity, 1);
    }

    #[test]
    fn normalized_queue_capacity_scales_with_workers() {
        let capacity = normalized_queue_capacity(8, 4096, 32);
        assert_eq!(capacity, 64);
    }

    #[test]
    fn normalized_chunk_row_capacity_hint_clamps_page_factor() {
        assert_eq!(normalized_chunk_row_capacity_hint(1024, 1), 1024);
        assert_eq!(normalized_chunk_row_capacity_hint(1024, 32), 16_384);
    }

    #[test]
    fn raw_row_batch_rows_roundtrip() {
        let mut batch = RawRowBatch::with_capacity(2, 8);
        batch.push_row(b"ab");
        batch.push_row(b"cde");
        assert_eq!(batch.row_count(), 2);
        assert_eq!(batch.raw_bytes(), 5);
        assert_eq!(batch.row(0), Some(&b"ab"[..]));
        assert_eq!(batch.row(1), Some(&b"cde"[..]));
        let rows: Vec<&[u8]> = batch.rows().collect();
        assert_eq!(rows, vec![&b"ab"[..], &b"cde"[..]]);
    }

    #[test]
    fn raw_row_batch_clear_resets_lengths() {
        let mut batch = RawRowBatch::with_capacity(2, 8);
        batch.push_row(b"ab");
        batch.clear();
        assert!(batch.is_empty());
        assert_eq!(batch.row_count(), 0);
        assert_eq!(batch.raw_bytes(), 0);
    }
}
