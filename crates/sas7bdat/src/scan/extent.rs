//! Streams a file's pages as large positional reads, for sources that cannot be mapped.
//!
//! Memory-mapping is unavailable or ruinous on network storage, and the fallback — one
//! `seek` + `read_exact` per page on a single handle — is latency-bound: a 100 GB file holds
//! on the order of a million pages, and one round-trip each dwarfs the transfer time. This
//! module reads *extents* instead: contiguous runs of pages, several megabytes at a time,
//! with a few reads in flight at once.
//!
//! The two constants below are measured, not guessed, against SMB storage over a LAN
//! (see `scripts/io_probe.py`, which reproduces the sweep on any host):
//!
//! ```text
//! readers:   1 -> 176 MB/s    4 -> 323    8 -> 280    16 -> 221
//! extents:   1 MB -> 348      4 MB -> 387    8 MB -> 350    16 MB -> 361    32 MB -> 233
//! ```
//!
//! Both curves fall off past their peak, so more is actively worse: concurrency beyond ~4
//! and extents beyond ~16 MB lose throughput rather than gaining it.

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

/// Bytes per positional read. 4 MB measured best on SMB; the 1–16 MB range is within noise
/// of it, while 32 MB costs ~40%. Staying at the low end of the plateau also keeps the
/// in-flight buffer pool small.
pub(super) const EXTENT_BYTES: usize = 4 * 1024 * 1024;

/// Reads in flight. Network storage rewards a few outstanding requests and punishes many:
/// 4 was the peak, 8 gave up 13%, 16 gave up 32%. Decode threads are counted separately —
/// they scale with cores, this does not.
pub(super) const MAX_READ_CONCURRENCY: usize = 4;

/// One contiguous run of pages, read as a unit.
pub(super) struct Extent {
    /// Index into the chunk list, so decoded batches keep their delivery order.
    pub chunk_idx: usize,
    /// File offset of `bytes[0]`, which the decoder needs to locate pages within it.
    pub base_offset: u64,
    pub bytes: Vec<u8>,
}

/// Hands decoded buffers back for reuse, so a long scan allocates a bounded number of them
/// rather than one per extent.
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

/// Spawn `readers` threads that fill extents for the chunks pulled from `next_chunk_idx`.
///
/// Each reader owns its own file handle, so the seeks are independent — that is what makes
/// the requests concurrent at the storage layer rather than serialized behind one cursor.
/// Returns the receiving end plus the recycle channel; both close when the readers finish.
#[derive(Clone, Copy)]
pub(super) struct ReadPlan<'scope> {
    pub path: &'scope Path,
    pub chunks: &'scope [&'scope [crate::internal::PageDescriptor]],
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
        chunks,
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
                if idx >= chunks.len() {
                    break;
                }
                let (offset, len) = spans[idx];
                let mut bytes = recycle_rx
                    .lock()
                    .ok()
                    .and_then(|rx| rx.recv().ok())
                    .unwrap_or_default();
                bytes.clear();
                bytes.resize(len, 0);
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
    use super::{EXTENT_BYTES, MAX_READ_CONCURRENCY, pages_per_extent};

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
