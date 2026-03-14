use crate::{
    AnyError,
    cli::{ColumnarBatchModeArg, ConvertArgs, SinkKind},
};
use rayon::prelude::*;
use sas7bdat::{
    CellValue, ColumnInfoJson, ColumnarBatchMode, ColumnarSink, CsvSink, ParquetSink, RowSink,
    SasReader, TableInfoJson,
    dataset::{DatasetMetadata, VariableKind},
    logger::{log_error, log_warn, set_log_file, set_log_prefix},
    parser::{ColumnInfo, DatasetLayout},
};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Seek},
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
};

const DEFAULT_COLUMNAR_BATCH_ROWS: usize = 4096;
const COLUMNAR_ROW_GROUP_MULTIPLIER: usize = 16;
const PARSE_WORKER_BATCH_ROWS: usize = 1024;
const PARSE_WORKER_PAGE_CHUNK: u64 = 8;
const PARSE_WORKER_QUEUE_FACTOR: usize = 2;

fn csv_projection_legacy_enabled() -> bool {
    static LEGACY: OnceLock<bool> = OnceLock::new();
    *LEGACY.get_or_init(|| {
        std::env::var("SAS7BDAT_CSV_PROJECTION_LEGACY")
            .map(|value| {
                value == "1"
                    || value.eq_ignore_ascii_case("true")
                    || value.eq_ignore_ascii_case("legacy")
            })
            .unwrap_or(false)
    })
}

const fn map_batch_mode(mode: ColumnarBatchModeArg) -> ColumnarBatchMode {
    match mode {
        ColumnarBatchModeArg::Segmented => ColumnarBatchMode::Segmented,
        ColumnarBatchModeArg::Contiguous => ColumnarBatchMode::Contiguous,
        ColumnarBatchModeArg::Adaptive => ColumnarBatchMode::Adaptive,
    }
}

pub fn run_convert(args: &ConvertArgs) -> Result<(), AnyError> {
    if let Some(path) = &args.logging.log_file {
        set_log_file(path)?;
    }
    if let Some(jobs) = args.execution.jobs {
        // Best-effort: configure global rayon pool once. Ignore error if already set.
        let _ = rayon::ThreadPoolBuilder::new()
            .num_threads(jobs)
            .build_global();
    }
    if matches!(args.execution.parse_threads, Some(0)) {
        return Err("--parse-threads must be greater than 0".into());
    }

    let files = crate::paths::discover_inputs(&args.inputs);

    if args.output.out.is_some() && files.len() != 1 {
        return Err("--out requires a single input".into());
    }

    let mut tasks: Vec<(PathBuf, PathBuf, PathBuf)> = Vec::with_capacity(files.len());
    if let Some(ref out) = args.output.out {
        let (root, file) = &files[0];
        tasks.push((root.clone(), file.clone(), out.clone()));
    } else {
        for (root, input) in files {
            let output = crate::paths::compute_output_path_unchecked(&root, &input, args);
            tasks.push((root, input, output));
        }
    }

    if args.execution.fail_fast {
        tasks
            .into_par_iter()
            .map(|(_root, input, output)| {
                convert_one(&input, &output, args)
                    .map_err(|e| format!("{}: {e}", input.display()).into())
            })
            .collect::<Result<Vec<()>, AnyError>>()?;
    } else {
        let results = tasks
            .into_par_iter()
            .map(|(_root, input, output)| {
                let res: Result<(), AnyError> = convert_one(&input, &output, args)
                    .map_err(|e| format!("{}: {e}", input.display()).into());
                if let Err(ref e) = res {
                    log_error(&e.to_string());
                }
                res
            })
            .collect::<Vec<_>>();
        let failures = results
            .iter()
            .filter(|r: &&Result<_, _>| r.is_err())
            .count();
        if failures > 0 {
            eprintln!("completed with {failures} failures");
        }
    }

    Ok(())
}

fn convert_one(input: &Path, output: &Path, args: &ConvertArgs) -> Result<(), AnyError> {
    let _log_prefix = set_log_prefix(input.to_string_lossy());
    // Prepare reader and metadata
    let mut sas = SasReader::open(input)?;
    if let Some(cat) = &args.catalog {
        let _ = sas.attach_catalog(cat);
    }
    let (mut reader, parsed) = sas.into_parts();

    // Resolve projection
    let (indices, selection, meta_filtered, cols_filtered) =
        crate::projection::resolve_projection(&parsed.header.metadata, &parsed.columns, args)?;

    // Build sink
    let sink_kind = args.output.sink;
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let columnar_batch_rows = DEFAULT_COLUMNAR_BATCH_ROWS.max(1);
    let derived_row_group_rows = columnar_batch_rows
        .saturating_mul(COLUMNAR_ROW_GROUP_MULTIPLIER)
        .max(columnar_batch_rows);
    let options = StreamOptions {
        indices: indices.as_deref(),
        skip: args.skip,
        max_rows: args.max_rows,
    };
    let parse_threads = args.execution.parse_threads.unwrap_or(1).max(1);
    match sink_kind {
        SinkKind::Parquet => {
            if parse_threads > 1 {
                log_warn(
                    "Parquet --parse-threads row path regressed in benchmarks; using single-thread columnar path",
                );
            }
            let file = File::create(output)?;
            let mut sink = ParquetSink::new(file).with_lenient_dates(!args.validation.strict_dates);
            let columnar_row_group_rows = if let Some(rows) = args.output.parquet_row_group_size {
                sink = sink.with_row_group_size(rows);
                Some(rows)
            } else {
                sink = sink.with_row_group_size(derived_row_group_rows);
                Some(derived_row_group_rows)
            };
            if let Some(bytes) = args.output.parquet_target_bytes {
                sink = sink.with_target_row_group_bytes(bytes);
            }
            sink = sink.with_streaming_columnar(true);
            if args.output.parquet_metadata {
                let columns = meta_filtered
                    .variables
                    .iter()
                    .map(|v| ColumnInfoJson {
                        index: v.index,
                        name: v.name.clone(),
                        label: v.label.clone(),
                        kind: match v.kind {
                            VariableKind::Numeric => "numeric".to_string(),
                            VariableKind::Character => "character".to_string(),
                        },
                        format: v.format.as_ref().map(|f| f.name.clone()),
                        width: v.storage_width,
                    })
                    .collect();
                let payload = TableInfoJson {
                    table_name: meta_filtered.table_name.clone(),
                    file_label: meta_filtered.file_label.clone(),
                    row_count: meta_filtered.row_count,
                    column_count: meta_filtered.column_count,
                    columns,
                };
                if let Ok(json) = serde_json::to_string(&payload) {
                    sink = sink.with_key_value_metadata(vec![parquet::file::metadata::KeyValue {
                        key: "sas7bdat.metadata".to_string(),
                        value: Some(json),
                    }]);
                }
            }
            let batch_rows = columnar_row_group_rows.unwrap_or(columnar_batch_rows);
            let col_opts = ColumnarOptions {
                selection: &selection,
                batch_rows,
                batch_mode: map_batch_mode(args.output.columnar_batch_mode),
                source_path: Some(input.to_string_lossy().to_string()),
                skip: args.skip,
                max_rows: args.max_rows,
            };
            stream_columnar_into_sink(
                &mut reader,
                &parsed,
                &meta_filtered,
                &cols_filtered,
                &col_opts,
                &mut sink,
            )?;
            let _ = sink.into_inner()?;
        }
        SinkKind::Csv | SinkKind::Tsv => {
            let file = File::create(output)?;
            let mut sink = CsvSink::new(file)
                .with_headers(args.output.headers)
                .with_delimiter(match (sink_kind, args.output.delimiter) {
                    (SinkKind::Tsv, None) => b'\t',
                    (_, Some(ch)) => ch as u8,
                    _ => b',',
                });
            stream_into_sink_with_threads(
                &mut reader,
                input,
                &parsed,
                &meta_filtered,
                &cols_filtered,
                &options,
                Some(input.to_string_lossy().to_string()),
                parse_threads,
                &mut sink,
            )?;
        }
    }

    println!("{} -> {}", input.display(), output.display());

    Ok(())
}

#[derive(Copy, Clone)]
struct StreamOptions<'a> {
    indices: Option<&'a [usize]>,
    skip: Option<u64>,
    max_rows: Option<u64>,
}

#[derive(Clone)]
struct ColumnarOptions<'a> {
    selection: &'a [usize],
    batch_rows: usize,
    batch_mode: ColumnarBatchMode,
    source_path: Option<String>,
    skip: Option<u64>,
    max_rows: Option<u64>,
}

fn stream_into_sink<W: std::io::Read + std::io::Seek, S: RowSink>(
    reader: &mut W,
    parsed: &sas7bdat::parser::DatasetLayout,
    meta_filtered: &DatasetMetadata,
    cols_filtered: &[ColumnInfo],
    options: &StreamOptions<'_>,
    source_path: Option<String>,
    sink: &mut S,
) -> Result<(), AnyError> {
    // Begin sink with filtered context
    let context = sas7bdat::sinks::SinkContext {
        metadata: meta_filtered,
        columns: cols_filtered,
        source_path,
    };
    sink.begin(context)?;

    let mut it = parsed.row_iterator(reader)?;
    let mut skipped = 0u64;
    let to_skip = options.skip.unwrap_or(0);
    let mut remaining = options.max_rows;
    let projection_legacy = csv_projection_legacy_enabled();

    if let Some(indices) = options.indices
        && !projection_legacy
    {
        while let Some(row) = it.try_next_projected(indices)? {
            if skipped < to_skip {
                skipped += 1;
                continue;
            }
            sink.write_row(&row)?;

            if let Some(rem) = remaining.as_mut() {
                if *rem == 0 {
                    break;
                }
                *rem -= 1;
                if *rem == 0 {
                    break;
                }
            }
        }
    } else if let Some(indices) = options.indices {
        let mut projected: Vec<CellValue<'static>> = Vec::new();
        while let Some(row) = it.try_next()? {
            if skipped < to_skip {
                skipped += 1;
                continue;
            }
            projected.clear();
            projected.reserve(indices.len());
            for &idx in indices {
                projected.push(row[idx].clone().into_owned());
            }
            sink.write_row(&projected)?;

            if let Some(rem) = remaining.as_mut() {
                if *rem == 0 {
                    break;
                }
                *rem -= 1;
                if *rem == 0 {
                    break;
                }
            }
        }
    } else {
        while let Some(row) = it.try_next()? {
            if skipped < to_skip {
                skipped += 1;
                continue;
            }
            sink.write_row(&row)?;

            if let Some(rem) = remaining.as_mut() {
                if *rem == 0 {
                    break;
                }
                *rem -= 1;
                if *rem == 0 {
                    break;
                }
            }
        }
    }
    sink.finish()?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn stream_into_sink_with_threads<W: Read + Seek, S: RowSink>(
    reader: &mut W,
    input: &Path,
    parsed: &DatasetLayout,
    meta_filtered: &DatasetMetadata,
    cols_filtered: &[ColumnInfo],
    options: &StreamOptions<'_>,
    source_path: Option<String>,
    parse_threads: usize,
    sink: &mut S,
) -> Result<(), AnyError> {
    if parse_threads <= 1 || parsed.header.page_count <= 1 {
        return stream_into_sink(
            reader,
            parsed,
            meta_filtered,
            cols_filtered,
            options,
            source_path,
            sink,
        );
    }
    stream_into_sink_parallel(
        input,
        parsed,
        meta_filtered,
        cols_filtered,
        options,
        source_path,
        parse_threads,
        sink,
    )
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn stream_into_sink_parallel<S: RowSink>(
    input: &Path,
    parsed: &DatasetLayout,
    meta_filtered: &DatasetMetadata,
    cols_filtered: &[ColumnInfo],
    options: &StreamOptions<'_>,
    source_path: Option<String>,
    parse_threads: usize,
    sink: &mut S,
) -> Result<(), AnyError> {
    enum WorkerChunk {
        ChunkRows {
            chunk_start: u64,
            rows: Vec<Vec<CellValue<'static>>>,
        },
        Done,
    }

    let context = sas7bdat::sinks::SinkContext {
        metadata: meta_filtered,
        columns: cols_filtered,
        source_path,
    };
    sink.begin(context)?;

    let page_count = parsed.header.page_count;
    let worker_count = parse_threads
        .max(1)
        .min(usize::try_from(page_count).unwrap_or(usize::MAX));
    if worker_count <= 1 || page_count <= 1 {
        let mut reader = File::open(input)?;
        let mut it = parsed.row_iterator(&mut reader)?;
        let mut skipped = 0u64;
        let to_skip = options.skip.unwrap_or(0);
        let mut remaining = options.max_rows;
        let projection_legacy = csv_projection_legacy_enabled();
        if let Some(indices) = options.indices
            && !projection_legacy
        {
            while let Some(row) = it.try_next_projected(indices)? {
                if skipped < to_skip {
                    skipped += 1;
                    continue;
                }
                let owned: Vec<CellValue<'static>> =
                    row.into_iter().map(CellValue::into_owned).collect();
                sink.write_row(&owned)?;
                if let Some(rem) = remaining.as_mut() {
                    if *rem == 0 {
                        break;
                    }
                    *rem -= 1;
                    if *rem == 0 {
                        break;
                    }
                }
            }
        } else if let Some(indices) = options.indices {
            while let Some(row) = it.try_next()? {
                if skipped < to_skip {
                    skipped += 1;
                    continue;
                }
                let mut projected: Vec<CellValue<'static>> = Vec::with_capacity(indices.len());
                for &idx in indices {
                    projected.push(row[idx].clone().into_owned());
                }
                sink.write_row(&projected)?;
                if let Some(rem) = remaining.as_mut() {
                    if *rem == 0 {
                        break;
                    }
                    *rem -= 1;
                    if *rem == 0 {
                        break;
                    }
                }
            }
        } else {
            while let Some(row) = it.try_next_owned()? {
                if skipped < to_skip {
                    skipped += 1;
                    continue;
                }
                sink.write_row(&row)?;
                if let Some(rem) = remaining.as_mut() {
                    if *rem == 0 {
                        break;
                    }
                    *rem -= 1;
                    if *rem == 0 {
                        break;
                    }
                }
            }
        }
        sink.finish()?;
        return Ok(());
    }

    let cancel = Arc::new(AtomicBool::new(false));
    let next_chunk = Arc::new(AtomicU64::new(0));
    let projection_legacy = csv_projection_legacy_enabled();
    let mut skipped = 0u64;
    let to_skip = options.skip.unwrap_or(0);
    let mut remaining = options.max_rows;
    let chunk_pages = PARSE_WORKER_PAGE_CHUNK.max(1);

    thread::scope(|scope| -> Result<(), AnyError> {
        let (tx, rx) = mpsc::sync_channel::<Result<WorkerChunk, AnyError>>(
            worker_count
                .saturating_mul(PARSE_WORKER_QUEUE_FACTOR)
                .max(1),
        );
        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let tx = tx.clone();
            let cancel = Arc::clone(&cancel);
            let next_chunk = Arc::clone(&next_chunk);
            let input = input.to_path_buf();
            let selection = options.indices.map(ToOwned::to_owned);
            let layout = parsed;
            handles.push(scope.spawn(move || {
                let work = || -> Result<(), AnyError> {
                    let mut file = File::open(&input)?;
                    let mut it = layout.row_iterator(&mut file)?;
                    loop {
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        let chunk_start = next_chunk.fetch_add(chunk_pages, Ordering::Relaxed);
                        if chunk_start >= page_count {
                            break;
                        }
                        let chunk_end = chunk_start.saturating_add(chunk_pages).min(page_count);
                        it.set_page_range(chunk_start, chunk_end)?;
                        let mut rows: Vec<Vec<CellValue<'static>>> =
                            Vec::with_capacity(PARSE_WORKER_BATCH_ROWS);

                        if let Some(indices) = selection.as_deref()
                            && !projection_legacy
                        {
                            while !cancel.load(Ordering::Relaxed) {
                                let Some(row) = it.try_next_projected(indices)? else {
                                    break;
                                };
                                rows.push(row.into_iter().map(CellValue::into_owned).collect());
                            }
                        } else if let Some(indices) = selection.as_deref() {
                            while !cancel.load(Ordering::Relaxed) {
                                let Some(row) = it.try_next()? else {
                                    break;
                                };
                                let mut projected = Vec::with_capacity(indices.len());
                                for &idx in indices {
                                    projected.push(row[idx].clone().into_owned());
                                }
                                rows.push(projected);
                            }
                        } else {
                            while !cancel.load(Ordering::Relaxed) {
                                let Some(row) = it.try_next_owned()? else {
                                    break;
                                };
                                rows.push(row);
                            }
                        }

                        if tx
                            .send(Ok(WorkerChunk::ChunkRows { chunk_start, rows }))
                            .is_err()
                        {
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
        let mut next_expected_chunk = 0u64;
        let mut pending = BTreeMap::<u64, Vec<Vec<CellValue<'static>>>>::new();
        let mut reached_limit = false;

        while done_workers < worker_count && !reached_limit {
            match rx.recv() {
                Ok(Ok(WorkerChunk::Done)) => {
                    done_workers += 1;
                }
                Ok(Ok(WorkerChunk::ChunkRows { chunk_start, rows })) => {
                    pending.insert(chunk_start, rows);
                }
                Ok(Err(err)) => {
                    cancel.store(true, Ordering::Relaxed);
                    drop(rx);
                    for handle in handles {
                        if handle.join().is_err() {
                            return Err("parser worker thread panicked".into());
                        }
                    }
                    return Err(err);
                }
                Err(_) => break,
            }

            while let Some(rows) = pending.remove(&next_expected_chunk) {
                for row in rows {
                    if skipped < to_skip {
                        skipped += 1;
                        continue;
                    }
                    sink.write_row(&row)?;
                    if let Some(rem) = remaining.as_mut() {
                        if *rem == 0 {
                            cancel.store(true, Ordering::Relaxed);
                            reached_limit = true;
                            break;
                        }
                        *rem -= 1;
                        if *rem == 0 {
                            cancel.store(true, Ordering::Relaxed);
                            reached_limit = true;
                            break;
                        }
                    }
                }
                if reached_limit {
                    break;
                }

                next_expected_chunk = next_expected_chunk.saturating_add(chunk_pages);
                if next_expected_chunk >= page_count {
                    break;
                }
            }

            if next_expected_chunk >= page_count {
                if done_workers >= worker_count {
                    break;
                }
                while done_workers < worker_count {
                    match rx.recv() {
                        Ok(Ok(WorkerChunk::Done)) => done_workers += 1,
                        Ok(Ok(WorkerChunk::ChunkRows { .. })) => {}
                        Ok(Err(err)) => {
                            cancel.store(true, Ordering::Relaxed);
                            drop(rx);
                            for handle in handles {
                                if handle.join().is_err() {
                                    return Err("parser worker thread panicked".into());
                                }
                            }
                            return Err(err);
                        }
                        Err(_) => break,
                    }
                }
            }
        }
        cancel.store(true, Ordering::Relaxed);
        drop(rx);

        for handle in handles {
            if handle.join().is_err() {
                return Err("parser worker thread panicked".into());
            }
        }
        Ok(())
    })?;

    sink.finish()?;
    Ok(())
}

fn stream_columnar_into_sink<W: std::io::Read + std::io::Seek, S: ColumnarSink>(
    reader: &mut W,
    parsed: &sas7bdat::parser::DatasetLayout,
    meta_filtered: &DatasetMetadata,
    cols_filtered: &[ColumnInfo],
    options: &ColumnarOptions<'_>,
    sink: &mut S,
) -> Result<(), AnyError> {
    if options.selection.len() != cols_filtered.len() {
        return Err("column selection length mismatch".into());
    }

    let context = sas7bdat::sinks::SinkContext {
        metadata: meta_filtered,
        columns: cols_filtered,
        source_path: options.source_path.clone(),
    };
    sink.begin(context)?;

    let mut it = parsed.row_iterator(reader)?;
    let mut skipped = 0u64;
    let mut remaining = options.max_rows;
    while let Some(mut batch) =
        it.next_columnar_batch_with_mode(options.batch_rows, options.batch_mode)?
    {
        // Apply skip/max_rows on top of the batch.
        if let Some(skip) = options.skip
            && skipped < skip
        {
            let to_drop = usize::try_from(skip.saturating_sub(skipped)).unwrap_or(usize::MAX);
            if to_drop >= batch.row_count {
                skipped = skipped.saturating_add(batch.row_count as u64);
                continue;
            }
            batch.truncate_front(to_drop);
            skipped = skip;
        }
        if let Some(rem) = remaining.as_mut() {
            if *rem == 0 {
                break;
            }
            if batch.row_count as u64 > *rem {
                let limit = usize::try_from(*rem).unwrap_or(usize::MAX);
                batch.truncate(limit);
            }
            *rem = rem.saturating_sub(batch.row_count as u64);
        }
        if batch.row_count == 0 {
            break;
        }
        sink.write_columnar_batch(&batch, options.selection)?;
    }

    sink.finish()?;
    Ok(())
}
