//! Single-pass scan: page descriptors are compiled from the same extents that feed decode.
//!
//! The two-pass scan walks the whole file once to build the descriptor table and again to
//! decode it. On a source whose cost is transfer rather than CPU — a network share — that
//! doubles wall-clock. It also holds the table in memory for the whole scan, and a compressed
//! file carries one row span per row, so a few hundred million rows cost several GB.
//!
//! Fusing the two needs a sequential stage. `row_base` is a running total: the first row index
//! of page N depends on every page before it, and the classifier uses it to cap the last page
//! at the dataset's declared row count. Readers deliver extents out of order, so that stage
//! keeps a small reorder buffer, parses each extent's page headers in file order, and hands the
//! extent plus its descriptors to the decode pool.
//!
//! ```text
//! readers (4, concurrent)  ->  parse (1, in order)  ->  decoders (all cores)  ->  collector
//! ```
//!
//! Parsing headers is cheap next to decoding rows, so one thread keeps up: it touches ~20 bytes
//! per page for a contiguous page, and for an indexed page walks the subheader pointers it
//! would otherwise walk in the separate pass.

use super::{
    ControlFlow, Error, FileSource, OwnedColumnarBatch, Result, ScanBuilder, ScanStats,
    builder::{
        DescriptorChunkContext, ProgressCounters, ProgressReport, StreamedBatchMessage,
        collect_streamed_batches, merge_scan_stats, pages_per_batch, resolved_parallel_workers,
        stream_batches_for_descriptor_chunk,
    },
    extent::{Extent, ExtentStream, ReadPlan, pages_per_extent, plan_spans_from_geometry},
    plan::ScanPlan,
    raw::PageWindow,
};
use crate::internal::{LayoutPlan, PageDescriptor, PageDescriptorTable, RowSpan};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{Receiver, SyncSender, sync_channel},
    },
};

/// One extent paired with the descriptors compiled from it.
///
/// The table's `row_span_start` indices are extent-local, since each extent gets a fresh span
/// vector — the decoder only ever looks up spans through this same table.
struct FusedExtent {
    extent: Extent,
    table: PageDescriptorTable,
}

/// Everything the parse stage needs that isn't carried by an individual extent.
struct ParseContext<'a> {
    layout: &'a LayoutPlan,
    page_size: usize,
    chunk_count: usize,
}

/// A planned fused scan: the read plan, the two stage contexts, and the pool size.
struct FusedRun<'a> {
    path: &'a std::path::Path,
    spans: &'a [(u64, usize)],
    workers: usize,
    ordering: super::OrderingMode,
    observer: Option<&'a super::ScanProgressObserver>,
    total_pages: u64,
    decode: DescriptorChunkContext<'a>,
    parse: ParseContext<'a>,
}

/// Stream owned batches by compiling descriptors and decoding rows in one pass over the file.
///
/// Returns `Ok(None)` when the scan doesn't qualify, leaving the caller to take the two-pass
/// path. It qualifies when the source is an unmapped path (so reads dominate), the descriptor
/// table isn't already cached, and the scan covers every row (no limit, no row selection) —
/// fusion plans its reads from header geometry, which only describes the whole file.
///
/// # Errors
///
/// Returns an error if reading, descriptor compilation, or decoding fails.
pub(super) fn try_stream_batches_fused<F>(
    builder: &ScanBuilder<'_>,
    plan: &ScanPlan,
    column_major: bool,
    f: &mut F,
) -> Result<Option<ScanStats>>
where
    F: FnMut(OwnedColumnarBatch) -> Result<ControlFlow<()>>,
{
    let FileSource::Path(path) = &builder.ds.file.source else {
        return Ok(None);
    };
    if !builder.row_window().is_whole_file() {
        return Ok(None);
    }
    if builder.ds.has_cached_descriptors() {
        return Ok(None);
    }

    let layout = builder.ds.layout.as_ref();
    let header = &layout.header;
    let page_size = usize::from(header.page_size);
    let page_count = header.page_count;
    // Geometry the classifier assumes: a page must hold its own header, and a zero-length row
    // means nothing to decode. Both are the two-pass path's errors to report, not this one's.
    if page_size == 0 || usize::from(layout.row_len) == 0 {
        return Ok(None);
    }
    // A page has to hold its own header, and the header has to hold the three trailing fields
    // the classifier reads. Anything else is the two-pass path's error to report, not this
    // one's, so decline rather than diagnose.
    if header.page_header_size > u32::from(header.page_size) || header.page_header_size < 8 {
        return Ok(None);
    }

    let workers = resolved_parallel_workers(
        builder.parallelism,
        usize::try_from(page_count).unwrap_or(usize::MAX),
    );
    if workers <= 1 || page_count <= 1 {
        return Ok(None);
    }

    // The extent is both the read unit and the batch boundary, so it is sized by the measured
    // best read size but never below one batch's worth of pages.
    let pages_per_chunk = pages_per_extent(page_size).max(pages_per_batch(
        layout.rows_per_page,
        plan.batch_row_capacity,
    ));
    let Some(spans) =
        plan_spans_from_geometry(header.data_offset, page_size, page_count, pages_per_chunk)
    else {
        return Ok(None);
    };
    let chunk_count = spans.len();
    if chunk_count == 0 {
        return Ok(None);
    }

    let run = FusedRun {
        path,
        spans: &spans,
        workers,
        ordering: builder.ordering,
        observer: builder.progress.as_ref(),
        total_pages: page_count,
        decode: DescriptorChunkContext {
            raw_plan: &plan.raw,
            batch_plan: &plan.batch,
            target_rows: plan.batch_row_capacity,
            capacity_hint_rows: plan.capacity_hint_rows.div_ceil(workers).max(1),
            row_len: usize::from(layout.row_len),
            // Whole-file scan is a precondition above, so the column-major fill is free to
            // ignore row selection; each worker additionally requires an all-staged-numeric
            // plan.
            columnar: column_major,
        },
        parse: ParseContext {
            layout,
            page_size,
            chunk_count,
        },
    };

    let (total_stats, candidate_rows) = run_fused_pipeline(&run, f)?;

    // The two-pass path checks this against the finished table before decoding anything. Here
    // the count only exists once the scan is over, so the check moves to the end; either way a
    // layout that produces no rows at all is reported as unsupported rather than as empty.
    if layout.compression != crate::metadata::CompressionKind::None
        && builder.ds.metadata.row_count > 0
        && candidate_rows == 0
    {
        return Err(Error::unsupported(
            "compressed dataset layout compiled no row producers; this compressed page layout is not implemented yet",
        ));
    }

    Ok(Some(total_stats))
}

/// Run the reader, parse, and decode stages, delivering batches to `f`.
///
/// Returns the merged scan statistics and the candidate row count the parse stage arrived at.
fn run_fused_pipeline<F>(run: &FusedRun<'_>, f: &mut F) -> Result<(ScanStats, u64)>
where
    F: FnMut(OwnedColumnarBatch) -> Result<ControlFlow<()>>,
{
    let workers = run.workers;
    // Declared outside `thread::scope` so the shared references satisfy the `'scope` bound.
    let next_chunk_idx = AtomicUsize::new(0);
    let counters = ProgressCounters::default();
    let counters = &counters;
    let progress = ProgressReport {
        observer: run.observer,
        counters,
        total_pages: run.total_pages,
        estimated_total_bytes: run
            .total_pages
            .saturating_mul(u64::try_from(run.parse.page_size).unwrap_or(0)),
    };

    std::thread::scope(|scope| -> Result<(ScanStats, u64)> {
        let (tx, rx) = sync_channel::<StreamedBatchMessage>(workers.saturating_mul(2).max(1));
        let stop = Arc::new(AtomicBool::new(false));
        // Readers get their own stop flag. `stop` means the consumer asked to quit, which
        // decode workers honour mid-chunk; the readers must also stop once the parse stage
        // reaches the last row, and that must not truncate a chunk already being decoded.
        let read_stop = Arc::new(AtomicBool::new(false));
        let (io_err_tx, io_err_rx) = sync_channel::<Error>(workers.max(1));

        let ExtentStream { extents, recycle } = super::extent::spawn_readers(
            scope,
            ReadPlan {
                path: run.path,
                spans: run.spans,
                next_chunk_idx: &next_chunk_idx,
            },
            &read_stop,
            workers,
            &io_err_tx,
        );

        // One parsed extent per decode worker, plus slack, so the parse stage stays ahead
        // without holding more extents in memory than the readers already do.
        let (parsed_tx, parsed_rx) = sync_channel::<FusedExtent>(workers.max(2));
        let parse_handle = {
            let tx = tx.clone();
            let stop = Arc::clone(&stop);
            let parse = &run.parse;
            scope.spawn(move || parse_extents_in_order(parse, &extents, &parsed_tx, &tx, &stop))
        };

        let parsed_rx = Arc::new(Mutex::new(parsed_rx));
        let mut handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            let tx = tx.clone();
            let stop = Arc::clone(&stop);
            let parsed_rx = Arc::clone(&parsed_rx);
            let recycle = recycle.clone();
            let decode = &run.decode;
            handles.push(scope.spawn(move || {
                let mut worker_stats = ScanStats::default();
                loop {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let Some(fused) = parsed_rx.lock().ok().and_then(|rx| rx.recv().ok()) else {
                        break;
                    };
                    let window = PageWindow {
                        bytes: &fused.extent.bytes,
                        base_offset: fused.extent.base_offset,
                    };
                    let chunk_stats = stream_batches_for_descriptor_chunk(
                        window,
                        &fused.table,
                        &fused.table.pages,
                        fused.extent.chunk_idx,
                        decode,
                        &tx,
                        stop.as_ref(),
                    );
                    counters.add(&chunk_stats);
                    merge_scan_stats(&mut worker_stats, &chunk_stats);
                    let _ = recycle.send(fused.extent.bytes);
                }
                worker_stats
            }));
        }
        drop(tx);
        drop(io_err_tx);
        // The workers hold the only handles that matter now. Keeping these alive here would
        // leave the parse stage parked on a send no one will ever receive, or a reader parked
        // on a recycled buffer no one will ever return, once the consumer stops early.
        drop(recycle);
        drop(parsed_rx);

        let collected = collect_streamed_batches(
            rx,
            run.ordering,
            run.parse.chunk_count,
            stop.as_ref(),
            progress,
            f,
        );
        join_stages(collected, parse_handle, handles, &io_err_rx)
    })
}

/// Wind the pipeline down and merge what every stage reported.
///
/// The parse stage is joined first: the decode workers only see their channel close once it
/// drops the sending end.
fn join_stages(
    collected: Result<(u64, u64)>,
    parse_handle: std::thread::ScopedJoinHandle<'_, u64>,
    handles: Vec<std::thread::ScopedJoinHandle<'_, ScanStats>>,
    io_err_rx: &Receiver<Error>,
) -> Result<(ScanStats, u64)> {
    let candidate_rows = parse_handle
        .join()
        .map_err(|_| Error::internal("fused descriptor stage panicked"))?;
    let (delivered_batches, delivered_rows) = match collected {
        Ok(counts) => counts,
        Err(err) => {
            for handle in handles {
                let _ = handle.join();
            }
            return Err(err);
        }
    };

    let mut total = ScanStats::default();
    for handle in handles {
        let worker_stats = handle
            .join()
            .map_err(|_| Error::internal("parallel batch worker panicked"))?;
        merge_scan_stats(&mut total, &worker_stats);
    }
    // A failed read closes its extent channel, which the parse stage sees as end of work, so
    // the scan would otherwise return fewer rows and report success.
    if let Ok(err) = io_err_rx.try_recv() {
        return Err(err);
    }
    total.decode_batches = delivered_batches;
    total.rows_emitted = delivered_rows;
    Ok((total, candidate_rows))
}

/// Compile descriptors for extents in file order, forwarding each to the decode pool.
///
/// Returns the number of candidate rows the descriptors account for, which the caller uses in
/// place of the descriptor table's `total_candidate_rows`.
///
/// Extents arrive out of order, so unwanted ones are parked in `pending` rather than waited on:
/// blocking on the next index while refusing to receive would stall the readers holding it.
fn parse_extents_in_order(
    ctx: &ParseContext<'_>,
    extents: &Receiver<Extent>,
    parsed_tx: &SyncSender<FusedExtent>,
    tx: &SyncSender<StreamedBatchMessage>,
    stop: &AtomicBool,
) -> u64 {
    let mut pending: HashMap<usize, Extent> = HashMap::new();
    let mut next_idx = 0usize;
    let mut row_base = 0u64;

    while next_idx < ctx.chunk_count && !stop.load(Ordering::Relaxed) {
        let extent = match pending.remove(&next_idx) {
            Some(extent) => extent,
            None => match extents.recv() {
                Ok(extent) if extent.chunk_idx == next_idx => extent,
                Ok(extent) => {
                    pending.insert(extent.chunk_idx, extent);
                    continue;
                }
                Err(_) => break,
            },
        };

        // Taken from the extent's own offset rather than recomputed from the chunk index, so
        // the page numbering cannot drift from the read plan that produced it.
        let first_page = extent
            .base_offset
            .saturating_sub(ctx.layout.header.data_offset)
            / ctx.page_size as u64;
        let (table, reached_last_row) =
            match compile_extent_descriptors(ctx, &extent.bytes, first_page, row_base) {
                Ok(compiled) => compiled,
                Err(err) => {
                    let _ = tx.send(StreamedBatchMessage::Error(err));
                    break;
                }
            };
        row_base = table.total_candidate_rows;

        if parsed_tx.send(FusedExtent { extent, table }).is_err() {
            break;
        }
        next_idx += 1;
        if reached_last_row {
            break;
        }
    }

    // Ordered delivery cannot advance past a chunk that never reports in, so every extent this
    // stage skipped — because the last row landed early, or a read failed — is closed out here.
    for chunk_idx in next_idx..ctx.chunk_count {
        if tx
            .send(StreamedBatchMessage::Finished { chunk_idx })
            .is_err()
        {
            break;
        }
    }
    row_base
}

/// Compile descriptors for every page in one extent, carrying `row_base` forward.
///
/// The returned flag is set when the descriptors reach the dataset's declared row count, which
/// ends the scan — the two-pass compiler stops on the same condition.
fn compile_extent_descriptors(
    ctx: &ParseContext<'_>,
    bytes: &[u8],
    first_page: u64,
    mut row_base: u64,
) -> Result<(PageDescriptorTable, bool)> {
    let page_count = bytes.len() / ctx.page_size;
    let mut descriptors: Vec<PageDescriptor> = Vec::with_capacity(page_count);
    let mut row_spans: Vec<RowSpan> = Vec::new();
    let mut reached_last_row = false;

    for page_offset in 0..page_count {
        let start = page_offset * ctx.page_size;
        let page = bytes
            .get(start..start + ctx.page_size)
            .ok_or_else(|| Error::corruption("page slice exceeds extent bounds"))?;
        let descriptor = crate::pages::compile_page_descriptor(
            ctx.layout,
            page,
            first_page + page_offset as u64,
            row_base,
            &mut row_spans,
        )?;
        row_base = row_base.saturating_add(u64::from(descriptor.row_count));
        descriptors.push(descriptor);
        if row_base >= ctx.layout.total_rows {
            reached_last_row = true;
            break;
        }
    }

    Ok((
        PageDescriptorTable {
            pages: descriptors.into_boxed_slice(),
            row_spans: row_spans.into_boxed_slice(),
            total_candidate_rows: row_base,
        },
        reached_last_row,
    ))
}
