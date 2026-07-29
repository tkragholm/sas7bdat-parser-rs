//! Reads pages in contiguous extents, for sources that are not memory-mapped.
//!
//! The alternative is one `seek` + `read_exact` per page on a single handle. Pages are
//! 64-256 KB and a 100 GB file holds on the order of a million of them, so on network
//! storage the round-trips cost more than the transfer. An extent covers many pages in one
//! read, and several reads run at once.
//!
//! The constants below come from a sweep against SMB over a LAN; `scripts/probe.py`
//! repeats it on other hosts:
//!
//! ```text
//! readers:   1 -> 176 MB/s    4 -> 323    8 -> 280    16 -> 221
//! extents:   1 MB -> 348      4 MB -> 387    8 MB -> 350    16 MB -> 361    32 MB -> 233
//! ```
//!
//! Throughput falls off past 4 readers and past 16 MB extents.

use crate::error::Error;
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{Receiver, SyncSender, sync_channel},
    },
};

/// Bytes per read. 4 MB measured highest on SMB; 1-16 MB fall within about 10% of it and
/// 32 MB costs about 40%. The low end of that range also keeps the buffer pool small.
pub(super) const EXTENT_BYTES: usize = 4 * 1024 * 1024;

/// Reads in flight. 4 measured highest; 8 gave up 13% and 16 gave up 32%. Decode threads
/// are counted separately and scale with core count.
pub(super) const MAX_READ_CONCURRENCY: usize = 4;

/// One contiguous run of pages, read as a unit.
pub(super) struct Extent {
    /// Index into the chunk list, so decoded batches keep their delivery order.
    pub chunk_idx: usize,
    /// File offset of `bytes[0]`, which the decoder needs to locate pages within it.
    pub base_offset: u64,
    pub bytes: Vec<u8>,
}

/// Returns decoded buffers for reuse, bounding allocations over a long scan.
pub(super) type BufferReturn = SyncSender<Vec<u8>>;

pub(super) struct ExtentStream {
    pub extents: Receiver<Extent>,
    pub recycle: BufferReturn,
}

/// Byte span covering `chunk`, as (offset, length).
pub(super) fn chunk_span(
    chunk: &[crate::internal::PageDescriptor],
    page_offset: impl Fn(crate::types::PageIndex) -> u64,
    page_size: usize,
) -> Option<(u64, usize)> {
    let first = chunk.first()?;
    let last = chunk.last()?;
    let start = page_offset(first.page_index);
    let end = page_offset(last.page_index).checked_add(page_size as u64)?;
    let len = usize::try_from(end.checked_sub(start)?).ok()?;
    Some((start, len))
}

/// Byte spans for a whole-file scan, planned from the header geometry alone: extent `i` covers
/// `pages_per_chunk` pages starting at page `i * pages_per_chunk`, with a shorter final extent.
///
/// The fused scan needs a read plan before any descriptor exists, so it cannot use
/// [`chunk_span`]. Returns `None` if the geometry overflows.
pub(super) fn plan_spans_from_geometry(
    data_offset: u64,
    page_size: usize,
    page_count: u64,
    pages_per_chunk: usize,
) -> Option<Vec<(u64, usize)>> {
    let page_size_u64 = u64::try_from(page_size).ok()?;
    let per_chunk = u64::try_from(pages_per_chunk.max(1)).ok()?;
    let chunk_count = usize::try_from(page_count.div_ceil(per_chunk)).ok()?;
    let mut spans = Vec::with_capacity(chunk_count);
    for chunk_idx in 0..chunk_count {
        let first_page = u64::try_from(chunk_idx).ok()?.checked_mul(per_chunk)?;
        let pages = per_chunk.min(page_count.checked_sub(first_page)?);
        let offset = data_offset.checked_add(first_page.checked_mul(page_size_u64)?)?;
        let len = usize::try_from(pages.checked_mul(page_size_u64)?).ok()?;
        spans.push((offset, len));
    }
    Some(spans)
}

/// Spawn `readers` threads that fill extents for the chunks pulled from `next_chunk_idx`.
///
/// Each reader owns a file handle, so seeks are independent rather than serialized behind a
/// shared cursor. Returns the receiving end and the recycle channel; both close when the
/// readers finish.
#[derive(Clone, Copy)]
pub(super) struct ReadPlan<'scope> {
    pub path: &'scope Path,
    /// One (offset, length) per extent; its length is the extent count.
    pub spans: &'scope [(u64, usize)],
    pub next_chunk_idx: &'scope AtomicUsize,
}

pub(super) fn spawn_readers<'scope>(
    scope: &'scope std::thread::Scope<'scope, '_>,
    plan: ReadPlan<'scope>,
    stop: &Arc<AtomicBool>,
    readers: usize,
    error_slot: &SyncSender<Error>,
) -> ExtentStream {
    let ReadPlan {
        path,
        spans,
        next_chunk_idx,
    } = plan;
    let readers = readers.clamp(1, MAX_READ_CONCURRENCY);
    // One in-flight extent per reader plus one queued each, so a reader never blocks waiting
    // for a decoder that is mid-batch, and memory stays at 2 * readers * EXTENT_BYTES.
    let depth = readers.saturating_mul(2).max(2);
    let (extent_tx, extent_rx) = sync_channel::<Extent>(depth);
    let (recycle_tx, recycle_rx) = sync_channel::<Vec<u8>>(depth + readers);
    for _ in 0..depth {
        let _ = recycle_tx.send(Vec::new());
    }
    let recycle_rx = Arc::new(std::sync::Mutex::new(recycle_rx));

    for _ in 0..readers {
        let extent_tx = extent_tx.clone();
        let recycle_rx = Arc::clone(&recycle_rx);
        let error_slot = error_slot.clone();
        let stop = Arc::clone(stop);
        scope.spawn(move || {
            let mut file = match File::open(path) {
                Ok(file) => file,
                Err(err) => {
                    let _ = error_slot.send(Error::io_error_with_path(path, &err));
                    return;
                }
            };
            loop {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let idx = next_chunk_idx.fetch_add(1, Ordering::Relaxed);
                if idx >= spans.len() {
                    break;
                }
                let (offset, len) = spans[idx];
                let mut bytes = recycle_rx
                    .lock()
                    .ok()
                    .and_then(|rx| rx.recv().ok())
                    .unwrap_or_default();
                // Grow only, then trim: `read_exact` overwrites every byte, so a recycled
                // buffer needs no clearing, and clearing it would memset each extent twice.
                if bytes.len() < len {
                    bytes.resize(len, 0);
                }
                bytes.truncate(len);
                if let Err(err) = file
                    .seek(SeekFrom::Start(offset))
                    .and_then(|_| file.read_exact(&mut bytes))
                {
                    let _ = error_slot.send(Error::io_error_with_path(path, &err));
                    stop.store(true, Ordering::Relaxed);
                    break;
                }
                if extent_tx
                    .send(Extent {
                        chunk_idx: idx,
                        base_offset: offset,
                        bytes,
                    })
                    .is_err()
                {
                    break;
                }
            }
        });
    }
    drop(extent_tx);

    ExtentStream {
        extents: extent_rx,
        recycle: recycle_tx,
    }
}

/// Pages per extent for a given page size, at least one.
pub(super) const fn pages_per_extent(page_size: usize) -> usize {
    if page_size == 0 {
        return 1;
    }
    let pages = EXTENT_BYTES / page_size;
    if pages == 0 { 1 } else { pages }
}

#[cfg(test)]
mod tests {
    use super::{EXTENT_BYTES, MAX_READ_CONCURRENCY, pages_per_extent, plan_spans_from_geometry};

    #[test]
    fn geometry_spans_tile_the_pages_exactly() {
        // 10 pages of 64 KB starting at 0x10000, four pages per extent.
        let spans = plan_spans_from_geometry(65_536, 65_536, 10, 4).expect("spans");
        assert_eq!(
            spans,
            vec![
                (65_536, 4 * 65_536),
                (65_536 + 4 * 65_536, 4 * 65_536),
                // The last extent is short rather than reading past the final page.
                (65_536 + 8 * 65_536, 2 * 65_536),
            ]
        );

        // Every page is covered once: the spans are contiguous and sum to the whole file.
        let total: usize = spans.iter().map(|(_, len)| len).sum();
        assert_eq!(total, 10 * 65_536);
        for pair in spans.windows(2) {
            assert_eq!(pair[0].0 + pair[0].1 as u64, pair[1].0);
        }
    }

    #[test]
    fn geometry_spans_handle_the_degenerate_cases() {
        assert_eq!(
            plan_spans_from_geometry(0, 1024, 1, 8),
            Some(vec![(0, 1024)])
        );
        assert_eq!(plan_spans_from_geometry(0, 1024, 0, 8), Some(Vec::new()));
        // A zero chunk size would divide by zero; it is clamped to one page per extent.
        assert_eq!(
            plan_spans_from_geometry(0, 1024, 2, 0),
            Some(vec![(0, 1024), (1024, 1024)])
        );
    }

    #[test]
    fn extent_holds_whole_pages() {
        assert_eq!(pages_per_extent(64 * 1024), 64);
        assert_eq!(pages_per_extent(256 * 1024), 16);
        // A page larger than one extent still yields a single page, never zero.
        assert_eq!(pages_per_extent(EXTENT_BYTES * 2), 1);
        assert_eq!(pages_per_extent(0), 1);
    }

    #[test]
    fn concurrency_stays_within_the_measured_peak() {
        assert!((1..=8).contains(&MAX_READ_CONCURRENCY));
    }
}
