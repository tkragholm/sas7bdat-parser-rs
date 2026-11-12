//! Delivery order, the callback's thread, and stopping, across every parallel pipeline.
//!
//! One synthetic file is served three ways so the same rows reach each pipeline: from memory
//! and from a memory map (two-pass, mapped), and by path (fused single-pass for a whole-file
//! scan, two-pass buffered for a row window). Read extents are cut to one page, so the
//! buffered pipelines split this small file into as many chunks as the mapped one. Every row holds its own index, so a missing,
//! repeated or misplaced row shows up as a wrong value. Each scan runs under a watchdog, so a
//! hang fails the test instead of stalling the suite.

use crate::internal::FileSource;
use crate::test_utils::{MockDatasetBuilder, make_numeric_text_row, make_page};
use crate::{
    BatchHint, Dataset, Error, LogicalType, OrderingMode, OwnedColumnBuffer, OwnedColumnarBatch,
    PageSource, Parallelism, RowSelection,
};
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

const PAGES: usize = 500;
const ROWS_PER_PAGE: usize = 2;
const ROWS: usize = PAGES * ROWS_PER_PAGE;
const PAGE_SIZE: usize = 64;
const WATCHDOG: Duration = Duration::from_secs(20);

fn text_for(row: usize) -> [u8; 4] {
    let s = format!("{:04}", row % 10_000);
    s.as_bytes().try_into().expect("four digits")
}

fn file_bytes() -> Arc<[u8]> {
    let mut bytes = Vec::with_capacity(PAGES * PAGE_SIZE);
    for page in 0..PAGES {
        let rows: Vec<Vec<u8>> = (0..ROWS_PER_PAGE)
            .map(|i| {
                let row = page * ROWS_PER_PAGE + i;
                #[allow(clippy::cast_precision_loss)]
                make_numeric_text_row(row as f64, text_for(row))
            })
            .collect();
        let refs: Vec<&[u8]> = rows.iter().map(Vec::as_slice).collect();
        bytes.extend_from_slice(&make_page(0x0100, 2, 0, &refs, PAGE_SIZE));
    }
    Arc::from(bytes)
}

/// The synthetic file on disk, removed when dropped.
struct TempFile(PathBuf);

impl TempFile {
    fn new(bytes: &[u8]) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("sas7bdat-ordering-{}-{n}.bin", std::process::id()));
        std::fs::write(&path, bytes).expect("write temp file");
        Self(path)
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[derive(Clone, Copy, Debug)]
enum Source {
    Memory,
    Mmap,
    Path,
}

fn dataset(bytes: &Arc<[u8]>, file: &TempFile, source: Source) -> Dataset {
    let builder = MockDatasetBuilder::new(Arc::clone(bytes))
        .with_column("num", LogicalType::Float, 8, 0)
        .with_column("txt", LogicalType::String, 4, 8)
        .with_row_len(12)
        .with_total_rows(ROWS as u64)
        .with_rows_per_page(ROWS_PER_PAGE as u64);
    let builder = match source {
        Source::Memory => builder,
        Source::Path => builder.with_source(FileSource::Path(file.0.clone())),
        Source::Mmap => {
            let handle = std::fs::File::open(&file.0).expect("open temp file");
            // SAFETY: the file is written once before mapping and not modified while the
            // map is alive; the test owns it for the whole scan.
            #[allow(unsafe_code)]
            let map = unsafe { memmap2::Mmap::map(&handle) }.expect("map temp file");
            builder.with_source(FileSource::Mmap(Arc::new(map)))
        }
    };
    builder.build()
}

/// What one scan saw: per batch, its row base and the row indices its two columns decode to.
#[derive(Debug, Default)]
struct Seen {
    batches: Vec<(u64, Vec<usize>)>,
    source: Option<PageSource>,
    /// Most batches the ordered collector held at once.
    reorder_peak: u64,
}

fn rows_of(batch: &OwnedColumnarBatch) -> Vec<usize> {
    let nums = match &batch.columns[0] {
        OwnedColumnBuffer::F64 { values, .. } => values.clone(),
        other => panic!("num column decoded as {other:?}"),
    };
    let texts: Vec<[u8; 4]> = match &batch.columns[1] {
        OwnedColumnBuffer::Utf8 { offsets, data, .. } => offsets
            .as_slice()
            .windows(2)
            .map(|w| {
                let (s, e) = (
                    usize::try_from(w[0]).unwrap(),
                    usize::try_from(w[1]).unwrap(),
                );
                data[s..e].try_into().expect("four-byte text")
            })
            .collect(),
        other => panic!("txt column decoded as {other:?}"),
    };
    assert_eq!(nums.len(), batch.row_count);
    assert_eq!(texts.len(), batch.row_count);
    nums.iter()
        .zip(&texts)
        .map(|(n, t)| {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let row = *n as usize;
            assert_eq!(*t, text_for(row), "text and number disagree for row {row}");
            row
        })
        .collect()
}

#[derive(Clone, Copy, Debug)]
struct Case {
    source: Source,
    window: Option<(u64, u64)>,
    threads: usize,
    batch_rows: usize,
    ordering: OrderingMode,
    /// Stop after this many batches, by `Break` or by an error from the callback.
    stop_after: Option<(usize, bool)>,
    /// Hold this chunk back before decoding it.
    slow_chunk: Option<(usize, Duration)>,
}

/// Run one scan under the watchdog, checking the callback contract as it goes.
fn run(bytes: &Arc<[u8]>, file: &Arc<TempFile>, case: Case) -> (Seen, crate::Result<()>) {
    let bytes = Arc::clone(bytes);
    let file = Arc::clone(file);
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let ds = dataset(&bytes, &file, case.source);
        let caller = std::thread::current().id();
        let in_callback = AtomicBool::new(false);
        let mut seen = Seen::default();
        let mut scan = ds
            .scan()
            .with_parallelism(Parallelism::Threads(case.threads))
            .with_batch_hint(BatchHint::Rows(case.batch_rows))
            .with_ordering(case.ordering);
        if let Some((start, end)) = case.window {
            scan = scan.select(RowSelection::range(start, end));
        }
        scan.extent_pages = Some(1);
        scan.slow_chunk = case.slow_chunk;
        let outcome = scan.scan_batches(&mut |batch: OwnedColumnarBatch| {
            assert_eq!(
                std::thread::current().id(),
                caller,
                "callback ran off the calling thread"
            );
            assert!(
                !in_callback.swap(true, Ordering::SeqCst),
                "two callbacks ran at once"
            );
            let rows = rows_of(&batch);
            seen.batches.push((batch.row_base.0, rows));
            let result = match case.stop_after {
                Some((n, as_error)) if seen.batches.len() >= n => {
                    if as_error {
                        Err(Error::unsupported("stop requested by test"))
                    } else {
                        Ok(ControlFlow::Break(()))
                    }
                }
                _ => Ok(ControlFlow::Continue(())),
            };
            in_callback.store(false, Ordering::SeqCst);
            result
        });
        let outcome = outcome.map(|stats| {
            seen.source = Some(stats.path.source);
            seen.reorder_peak = stats.reorder_peak_batches;
        });
        let _ = tx.send((seen, outcome));
    });
    rx.recv_timeout(WATCHDOG)
        .unwrap_or_else(|_| panic!("scan did not return within {WATCHDOG:?}: {case:?}"))
}

fn expected_rows(window: Option<(u64, u64)>) -> std::ops::Range<usize> {
    match window {
        None => 0..ROWS,
        Some((s, e)) => usize::try_from(s).unwrap()..usize::try_from(e).unwrap().min(ROWS),
    }
}

/// Every row exactly once; in order and contiguous when ordering is stable.
fn check_complete(case: Case, seen: &Seen) {
    let expected = expected_rows(case.window);
    let mut batches = seen.batches.clone();
    if matches!(case.ordering, OrderingMode::Unordered) {
        batches.sort_by_key(|(base, _)| *base);
    }
    let mut next = expected.start;
    for (base, rows) in &batches {
        assert_eq!(
            usize::try_from(*base).unwrap(),
            next,
            "batch out of place in {case:?}"
        );
        for (j, row) in rows.iter().enumerate() {
            assert_eq!(*row, next + j, "wrong row inside a batch in {case:?}");
        }
        next += rows.len();
    }
    assert_eq!(next, expected.end, "rows missing at the end in {case:?}");
}

fn fixture() -> (Arc<[u8]>, Arc<TempFile>) {
    let bytes = file_bytes();
    let file = Arc::new(TempFile::new(&bytes));
    (bytes, file)
}

const SOURCES: [Source; 3] = [Source::Memory, Source::Mmap, Source::Path];
const WINDOWS: [Option<(u64, u64)>; 2] = [None, Some((37, 911))];
const THREADS: [usize; 4] = [2, 3, 8, 16];
const BATCH_ROWS: [usize; 4] = [1, 2, 3, 7];
const ORDERINGS: [OrderingMode; 2] = [OrderingMode::Stable, OrderingMode::Unordered];

#[test]
fn every_pipeline_delivers_every_row_once_in_order_when_asked() {
    let (bytes, file) = fixture();
    for _round in 0..3 {
        for source in SOURCES {
            for window in WINDOWS {
                for threads in THREADS {
                    for batch_rows in BATCH_ROWS {
                        for ordering in ORDERINGS {
                            let case = Case {
                                source,
                                window,
                                threads,
                                batch_rows,
                                ordering,
                                stop_after: None,
                                slow_chunk: None,
                            };
                            let (seen, outcome) = run(&bytes, &file, case);
                            outcome.unwrap_or_else(|e| panic!("{case:?} failed: {e}"));
                            check_complete(case, &seen);
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn each_source_reaches_the_pipeline_it_is_meant_to_test() {
    let (bytes, file) = fixture();
    for (source, window, expected) in [
        (Source::Memory, None, PageSource::TwoPassParallel),
        (Source::Mmap, None, PageSource::TwoPassParallel),
        (Source::Path, None, PageSource::OnePass),
        (Source::Path, Some((37, 911)), PageSource::TwoPassParallel),
    ] {
        let case = Case {
            source,
            window,
            threads: 4,
            batch_rows: 3,
            ordering: OrderingMode::Stable,
            stop_after: None,
            slow_chunk: None,
        };
        let (seen, outcome) = run(&bytes, &file, case);
        outcome.expect("scan");
        assert_eq!(
            seen.source,
            Some(expected),
            "{case:?} took another pipeline"
        );
    }
}

#[test]
fn a_break_or_an_error_from_the_callback_stops_every_pipeline_promptly() {
    let (bytes, file) = fixture();
    for source in SOURCES {
        for window in WINDOWS {
            for threads in [2, 16] {
                for ordering in ORDERINGS {
                    for stop_at in [1, 5, 40] {
                        for as_error in [false, true] {
                            let case = Case {
                                source,
                                window,
                                threads,
                                batch_rows: 2,
                                ordering,
                                stop_after: Some((stop_at, as_error)),
                                slow_chunk: None,
                            };
                            let (seen, outcome) = run(&bytes, &file, case);
                            assert_eq!(
                                seen.batches.len(),
                                stop_at,
                                "callback ran again after asking to stop: {case:?}"
                            );
                            assert_eq!(outcome.is_err(), as_error, "{case:?}");
                            if matches!(ordering, OrderingMode::Stable) {
                                let start = expected_rows(window).start;
                                let mut next = start;
                                for (base, rows) in &seen.batches {
                                    assert_eq!(usize::try_from(*base).unwrap(), next, "{case:?}");
                                    next += rows.len();
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// A slow first chunk lets every other worker finish ahead of it. Ordered delivery must still
/// hand rows over in order, and the reorder buffer must stay within the claim window instead
/// of filling with everything behind the slow chunk. With one-page extents and one-page
/// batches every chunk is one batch, so the window bounds batches directly.
#[test]
fn a_slow_chunk_holds_the_reorder_buffer_within_the_claim_window() {
    let (bytes, file) = fixture();
    for source in SOURCES {
        for window in WINDOWS {
            for threads in [2, 8] {
                for ordering in ORDERINGS {
                    let case = Case {
                        source,
                        window,
                        threads,
                        batch_rows: 2,
                        ordering,
                        stop_after: None,
                        slow_chunk: Some((0, Duration::from_millis(200))),
                    };
                    let (seen, outcome) = run(&bytes, &file, case);
                    outcome.unwrap_or_else(|e| panic!("{case:?} failed: {e}"));
                    check_complete(case, &seen);
                    let bound = super::gate::ordered_window(threads) as u64;
                    match ordering {
                        OrderingMode::Stable => assert!(
                            seen.reorder_peak <= bound,
                            "reorder buffer held {} batches, window {bound}: {case:?}",
                            seen.reorder_peak
                        ),
                        OrderingMode::Unordered => {
                            assert_eq!(seen.reorder_peak, 0, "unordered buffered: {case:?}");
                        }
                    }
                }
            }
        }
    }
}

/// Stopping while a slow chunk keeps the window full must not leave a claimer waiting.
#[test]
fn a_stop_while_the_window_is_full_returns_promptly() {
    let (bytes, file) = fixture();
    for source in SOURCES {
        for as_error in [false, true] {
            let case = Case {
                source,
                window: None,
                threads: 8,
                batch_rows: 2,
                ordering: OrderingMode::Stable,
                stop_after: Some((1, as_error)),
                slow_chunk: Some((0, Duration::from_millis(200))),
            };
            let started = std::time::Instant::now();
            let (seen, outcome) = run(&bytes, &file, case);
            assert_eq!(seen.batches.len(), 1, "{case:?}");
            assert_eq!(outcome.is_err(), as_error, "{case:?}");
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "stop took {:?}: {case:?}",
                started.elapsed()
            );
        }
    }
}

/// A scan whose callback, or one of whose workers, panics must end: the panic reaches the
/// caller, or the scan returns an error. It must never hang, and never return success.
#[test]
fn a_panic_in_the_callback_or_a_worker_never_hangs_the_scan() {
    let (bytes, file) = fixture();
    for source in SOURCES {
        for ordering in ORDERINGS {
            for in_worker in [false, true] {
                let bytes = Arc::clone(&bytes);
                let file = Arc::clone(&file);
                let (tx, rx) = mpsc::channel();
                std::thread::spawn(move || {
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let ds = dataset(&bytes, &file, source);
                        let mut scan = ds
                            .scan()
                            .with_parallelism(Parallelism::Threads(4))
                            .with_batch_hint(BatchHint::Rows(2))
                            .with_ordering(ordering);
                        scan.extent_pages = Some(1);
                        if in_worker {
                            scan.panic_chunk = Some(3);
                        }
                        let mut n = 0;
                        scan.scan_batches(&mut |_batch: OwnedColumnarBatch| {
                            n += 1;
                            assert!(in_worker || n < 3, "test: callback panics");
                            Ok(ControlFlow::Continue(()))
                        })
                        .map(|_| ())
                    }));
                    let _ = tx.send(match outcome {
                        Err(_) => "panicked",
                        Ok(Err(_)) => "error",
                        Ok(Ok(())) => "success",
                    });
                });
                let ended = rx.recv_timeout(WATCHDOG).unwrap_or_else(|_| {
                    panic!("scan hung after a panic: {source:?} {ordering:?} worker={in_worker}")
                });
                assert_ne!(
                    ended, "success",
                    "a panic was swallowed: {source:?} {ordering:?} worker={in_worker}"
                );
            }
        }
    }
}

/// A chunk that stalls mid-file, not only the first one, is held to the window too.
#[test]
fn a_slow_chunk_mid_file_is_bounded_and_ordered() {
    let (bytes, file) = fixture();
    for source in SOURCES {
        let case = Case {
            source,
            window: None,
            threads: 8,
            batch_rows: 2,
            ordering: OrderingMode::Stable,
            stop_after: None,
            slow_chunk: Some((250, Duration::from_millis(200))),
        };
        let (seen, outcome) = run(&bytes, &file, case);
        outcome.unwrap_or_else(|e| panic!("{case:?} failed: {e}"));
        check_complete(case, &seen);
        let bound = super::gate::ordered_window(8) as u64;
        assert!(
            seen.reorder_peak <= bound,
            "held {}: {case:?}",
            seen.reorder_peak
        );
    }
}

/// A scan after a single-threaded one reuses the page descriptors that scan cached, which
/// sends a by-path scan through the two-pass buffered pipeline instead of the fused one.
#[test]
fn a_second_scan_with_cached_descriptors_still_delivers_in_order() {
    let (bytes, file) = fixture();
    let ds = dataset(&bytes, &file, Source::Path);
    for round in 0..2 {
        // The first scan runs on one thread, through the two-pass pipeline, which caches the
        // descriptor table; the fused pipeline builds one per extent and caches none.
        let threads = if round == 0 {
            Parallelism::None
        } else {
            Parallelism::Threads(8)
        };
        let mut scan = ds
            .scan()
            .with_parallelism(threads)
            .with_batch_hint(BatchHint::Rows(3));
        scan.extent_pages = Some(1);
        let mut next = 0usize;
        let stats = scan
            .scan_batches(&mut |batch: OwnedColumnarBatch| {
                assert_eq!(
                    usize::try_from(batch.row_base.0).unwrap(),
                    next,
                    "round {round}"
                );
                for (j, row) in rows_of(&batch).iter().enumerate() {
                    assert_eq!(*row, next + j, "round {round}");
                }
                next += batch.row_count;
                Ok(ControlFlow::Continue(()))
            })
            .expect("scan");
        assert_eq!(next, ROWS, "round {round}");
        if round == 1 {
            assert_eq!(
                stats.path.source,
                PageSource::TwoPassParallel,
                "second scan"
            );
        }
    }
}
