use super::{
    CorpusScanCsvRow, Dataset, FileProgressReporter, FixtureCatalog, FixtureEntry, FixtureStatus,
    FullContentAccumulator, OpenOptions, ProjectedScanShape, ScanCsvContext, ScanProfileResult,
    ScanRankedFile, ScanRunOptions, ScanSummary, apply_scan_stats, build_projection,
    bytes_to_megabytes, compression_name, content_class_from_counts, encoding_class_from_name,
    join_named_counts, logical_type_counts_for_scan, profile_dataset_with_sample,
    projected_scan_shape, round_metric, run_scan, size_class_from_page, source_group,
    structural_companion_csv_path, summarize_catalog, summarize_scan_stats, summary_txt_path,
    temporal_format_summary_for_scan, top_scan_ranked, width_class_from_shape,
    width_summary_for_scan,
};
use csv::Writer;
use std::{
    fs,
    path::{Path, PathBuf},
    time::Instant,
};

pub fn write_scan_profile(
    paths: &[PathBuf],
    roots: &[String],
    out: Option<PathBuf>,
    options: ScanRunOptions,
) -> std::result::Result<(), String> {
    let context = ScanCsvContext {
        roots: roots.join("|"),
        mode: options.mode.as_str().to_owned(),
        projection: options.projection.as_str().to_owned(),
        io_backend: options.io_backend.as_str().to_owned(),
        batch_rows: options.batch_rows,
        limit: options
            .limit
            .map_or_else(String::new, |value| value.to_string()),
    };

    let results = super::collect_scan_rows(paths, &context, options);
    let rows: Vec<CorpusScanCsvRow> = results
        .iter()
        .map(|result| result.scan_row.clone())
        .collect();
    let mut summary = ScanSummary {
        discovered_files: paths.len(),
        ..ScanSummary::default()
    };

    for row in &rows {
        if row.status == "profiled" {
            summary.profiled_files += 1;
            summary.total_elapsed_ns = summary.total_elapsed_ns.saturating_add(row.elapsed_ns);
            summary.total_rows_emitted =
                summary.total_rows_emitted.saturating_add(row.rows_emitted);
            summary.total_raw_bytes_read = summary
                .total_raw_bytes_read
                .saturating_add(row.raw_bytes_read);
            summary.total_row_bytes_materialized = summary
                .total_row_bytes_materialized
                .saturating_add(row.row_bytes_materialized);
            summary.total_pages_seen = summary.total_pages_seen.saturating_add(row.pages_seen);
            summary.total_compressed_pages = summary
                .total_compressed_pages
                .saturating_add(row.compressed_pages);
            summary.slowest_by_elapsed.push(ScanRankedFile {
                path: row.path.clone(),
                file_name: row.file_name.clone(),
                value: row.elapsed_ns,
            });
            summary.largest_by_raw_bytes.push(ScanRankedFile {
                path: row.path.clone(),
                file_name: row.file_name.clone(),
                value: u128::from(row.raw_bytes_read),
            });
        } else {
            summary.failed_files += 1;
        }
    }

    summary.slowest_by_elapsed = top_scan_ranked(summary.slowest_by_elapsed, 20);
    summary.largest_by_raw_bytes = top_scan_ranked(summary.largest_by_raw_bytes, 20);

    if let Some(path) = out {
        let mut writer = Writer::from_path(&path).map_err(|err| err.to_string())?;
        for row in &rows {
            writer.serialize(row).map_err(|err| err.to_string())?;
        }
        writer.flush().map_err(|err| err.to_string())?;
        let summary_path = summary_txt_path(&path);
        fs::write(
            summary_path,
            super::corpus_render::render_scan_summary_txt(&summary, roots, &context),
        )
        .map_err(|err| err.to_string())?;

        let structural_path = structural_companion_csv_path(&path);
        let structural_fixtures: Vec<FixtureEntry> = results
            .into_iter()
            .map(|result| result.structural_entry)
            .collect();
        let catalog = FixtureCatalog {
            roots: roots.to_vec(),
            sample_rows: 0,
            fixtures: structural_fixtures,
        };
        let structural_summary = summarize_catalog(&catalog);
        super::write_csv(
            &catalog,
            roots,
            0,
            &structural_summary,
            false,
            Some(structural_path),
        )
    } else {
        let stdout = std::io::stdout();
        let mut writer = Writer::from_writer(stdout.lock());
        for row in &rows {
            writer.serialize(row).map_err(|err| err.to_string())?;
        }
        writer.flush().map_err(|err| err.to_string())
    }
}

#[allow(clippy::cast_precision_loss)]
pub fn build_scan_csv_row(
    path: &Path,
    context: &ScanCsvContext,
    options: ScanRunOptions,
    file_progress: Option<&FileProgressReporter>,
) -> ScanProfileResult {
    let path_string = path.display().to_string();
    let file_name = path
        .file_name()
        .map_or_else(String::new, |value| value.to_string_lossy().into_owned());
    let source_group = source_group(path);
    let size_bytes = fs::metadata(path).map_or(0, |meta| meta.len());
    let mut row = new_scan_row(context, path_string, file_name, source_group, size_bytes);

    let ds = match Dataset::open_with(
        path,
        OpenOptions::builder()
            .io_backend(options.io_backend)
            .build(),
    ) {
        Ok(ds) => ds,
        Err(err) => {
            if let Some(file_progress) = file_progress {
                file_progress.finish();
            }
            return scan_error_result(row, path, size_bytes, err.to_string());
        }
    };

    fill_scan_metadata(&mut row, &ds);
    let projection_obj = build_projection(&ds, options.projection);
    let projected_shape = projected_scan_shape(&ds, projection_obj.as_ref());
    fill_projected_shape(&mut row, projected_shape);

    let mut full_content = FullContentAccumulator::new(&ds);
    let start = Instant::now();
    let stats = match run_scan(
        &ds,
        options.mode,
        projection_obj.as_ref(),
        options.batch_rows,
        options.limit,
        file_progress.cloned(),
        Some(&mut |_, bytes| full_content.observe_row(bytes)),
    ) {
        Ok(stats) => stats,
        Err(err) => {
            if let Some(file_progress) = file_progress {
                file_progress.finish();
            }
            return scan_error_result(row, path, size_bytes, err.to_string());
        }
    };

    if let Some(file_progress) = file_progress {
        file_progress.finish();
    }

    row.status.clear();
    row.status.push_str("profiled");
    row.elapsed_ns = start.elapsed().as_nanos();
    apply_scan_stats(&mut row, summarize_scan_stats(&stats));
    if row.elapsed_ns > 0 {
        let seconds = row.elapsed_ns as f64 / 1_000_000_000.0;
        row.rows_per_second = round_metric(row.rows_emitted as f64 / seconds);
        row.bytes_per_second = round_metric(row.raw_bytes_read as f64 / seconds);
        row.pages_per_second = round_metric(row.pages_seen as f64 / seconds);
    }
    if row.pages_seen > 0 {
        row.rows_per_page = round_metric(row.rows_emitted as f64 / row.pages_seen as f64);
    }
    if row.rows_emitted > 0 {
        row.raw_bytes_per_row = round_metric(row.raw_bytes_read as f64 / row.rows_emitted as f64);
        row.materialized_bytes_per_row =
            round_metric(row.row_bytes_materialized as f64 / row.rows_emitted as f64);
    }
    if row.raw_bytes_read > 0 {
        row.materialization_ratio =
            round_metric(row.row_bytes_materialized as f64 / row.raw_bytes_read as f64);
    }

    let exact_profile = profile_dataset_with_sample(&ds, full_content.into_sample());
    let structural_entry = FixtureEntry {
        path: path.display().to_string(),
        file_name: row.file_name.clone(),
        source_group: row.source_group.clone(),
        size_bytes,
        status: FixtureStatus::Profiled(Box::new(exact_profile)),
    };

    ScanProfileResult {
        scan_row: row,
        structural_entry,
    }
}

fn new_scan_row(
    context: &ScanCsvContext,
    path: String,
    file_name: String,
    source_group: String,
    size_bytes: u64,
) -> CorpusScanCsvRow {
    CorpusScanCsvRow {
        corpus_roots: context.roots.clone(),
        corpus_mode: context.mode.clone(),
        corpus_projection: context.projection.clone(),
        corpus_io_backend: context.io_backend.clone(),
        corpus_batch_rows: context.batch_rows,
        corpus_limit: context.limit.clone(),
        path,
        file_name,
        source_group,
        status: "error".to_owned(),
        size_megabytes: bytes_to_megabytes(size_bytes),
        compression_class: "unknown".to_owned(),
        encoding_class: "unknown".to_owned(),
        size_class: "unknown".to_owned(),
        width_class: "unknown".to_owned(),
        content_class: "unknown".to_owned(),
        ..CorpusScanCsvRow::default()
    }
}

fn scan_error_result(
    mut row: CorpusScanCsvRow,
    path: &Path,
    size_bytes: u64,
    error: String,
) -> ScanProfileResult {
    row.error.clone_from(&error);
    let file_name = row.file_name.clone();
    let source_group = row.source_group.clone();
    ScanProfileResult {
        scan_row: row,
        structural_entry: FixtureEntry {
            path: path.display().to_string(),
            file_name,
            source_group,
            size_bytes,
            status: FixtureStatus::Error { error },
        },
    }
}

fn fill_scan_metadata(row: &mut CorpusScanCsvRow, ds: &Dataset) {
    row.table_name = ds.metadata().table_name.clone().unwrap_or_default();
    row.encoding = ds.metadata().encoding.clone().unwrap_or_default();
    row.compression.clear();
    row.compression
        .push_str(compression_name(ds.metadata().compression));
    row.row_count = ds.metadata().row_count;
    row.column_count = ds.columns().len();
    row.row_len = ds.metadata().row_len;
    row.page_size = ds.metadata().page_size;
    row.page_count = ds.metadata().page_count;

    let logical_types = logical_type_counts_for_scan(ds);
    let widths = width_summary_for_scan(ds);
    let temporal_formats = temporal_format_summary_for_scan(ds);
    row.string_columns = logical_types.string;
    row.integer_columns = logical_types.integer;
    row.float_columns = logical_types.float;
    row.date_columns = logical_types.date;
    row.datetime_columns = logical_types.datetime;
    row.time_columns = logical_types.time;
    row.bytes_columns = logical_types.bytes;
    row.numeric_like_columns = logical_types.integer
        + logical_types.float
        + logical_types.date
        + logical_types.datetime
        + logical_types.time;
    row.string_width_sum = widths.string_width_sum;
    row.string_width_max = widths.string_width_max;
    row.numeric_width_sum = widths.numeric_width_sum;
    row.numeric_width_max = widths.numeric_width_max;
    row.date_format_columns = temporal_formats.date_format_columns;
    row.datetime_format_columns = temporal_formats.datetime_format_columns;
    row.time_format_columns = temporal_formats.time_format_columns;
    row.date_formats = join_named_counts(&temporal_formats.date_formats);
    row.datetime_formats = join_named_counts(&temporal_formats.datetime_formats);
    row.time_formats = join_named_counts(&temporal_formats.time_formats);
    row.compression_class.clone_from(&row.compression);
    row.encoding_class = encoding_class_from_name(&row.encoding);
    row.size_class = size_class_from_page(row.page_size, row.page_count);
    row.width_class = width_class_from_shape(row.column_count, row.row_len);
    row.content_class = content_class_from_counts(
        logical_types.string + logical_types.bytes,
        row.numeric_like_columns,
        row.column_count,
    );
}

const fn fill_projected_shape(row: &mut CorpusScanCsvRow, shape: ProjectedScanShape) {
    row.projected_columns = shape.projected_columns;
    row.projected_string_columns = shape.projected_string_columns;
    row.projected_numeric_like_columns = shape.projected_numeric_like_columns;
    row.projected_physical_width_sum = shape.projected_physical_width_sum;
    row.projected_string_width_sum = shape.projected_string_width_sum;
    row.projected_numeric_width_sum = shape.projected_numeric_width_sum;
}
