use csv::{ReaderBuilder, Writer};
use rayon::prelude::*;
use sas7bdat_profiler::init_profiler_runtime;
use sas7bdat_simd::{
    BatchHint, Dataset, DecodeMode, Endianness, IoBackendPreference, LogicalType, OpenOptions,
    Projection, ScanProgress,
    fixture_catalog::{
        FixtureCatalog, FixtureEntry, FixtureProfile, FixtureStatus, LogicalTypeCounts, NamedCount,
        ProjectionPreset, SampleSummary, ScanStatsSummary, WidthSummary, build_projection,
        discover_fixture_paths, profile_dataset_with_sample, profile_fixture, summarize_scan_stats,
    },
};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Path, PathBuf},
    process::ExitCode,
    sync::{
        Arc, Once,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};
use tracing::Span;
use tracing_indicatif::{IndicatifLayer, span_ext::IndicatifSpanExt, style::ProgressStyle};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputFormat {
    Json,
    Csv,
}

impl OutputFormat {
    fn parse(value: &str) -> std::result::Result<Self, String> {
        match value {
            "json" => Ok(Self::Json),
            "csv" => Ok(Self::Csv),
            other => Err(format!("unsupported --format value: {other}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProfileMode {
    RawRows,
    TypedRows,
    TypedLosslessRows,
    TypedBatches,
    TypedLosslessBatches,
}

impl ProfileMode {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "raw_rows" => Some(Self::RawRows),
            "typed_rows" => Some(Self::TypedRows),
            "typed_lossless_rows" => Some(Self::TypedLosslessRows),
            "typed_batches" => Some(Self::TypedBatches),
            "typed_lossless_batches" => Some(Self::TypedLosslessBatches),
            _ => None,
        }
    }

    const fn decode_mode(self) -> DecodeMode {
        match self {
            Self::RawRows | Self::TypedRows | Self::TypedBatches => DecodeMode::Typed,
            Self::TypedLosslessRows | Self::TypedLosslessBatches => DecodeMode::TypedLossless,
        }
    }

    const fn is_batch(self) -> bool {
        matches!(self, Self::TypedBatches | Self::TypedLosslessBatches)
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::RawRows => "raw_rows",
            Self::TypedRows => "typed_rows",
            Self::TypedLosslessRows => "typed_lossless_rows",
            Self::TypedBatches => "typed_batches",
            Self::TypedLosslessBatches => "typed_lossless_batches",
        }
    }
}

#[derive(Debug, Serialize)]
struct CorpusProfileOutput {
    roots: Vec<String>,
    sample_rows: usize,
    summary: CorpusSummary,
    fixtures: Vec<FixtureEntry>,
}

#[derive(Debug, Serialize, Default)]
struct CorpusSummary {
    discovered_files: usize,
    profiled_files: usize,
    failed_files: usize,
    total_size_bytes: u64,
    total_rows: u64,
    total_columns: u64,
    total_string_columns: u64,
    total_numeric_like_columns: u64,
    total_sampled_string_cells: u64,
    total_sampled_empty_string_cells: u64,
    total_sampled_ascii_string_cells: u64,
    compression_counts: BTreeMap<String, u64>,
    encoding_counts: BTreeMap<String, u64>,
    tag_counts: BTreeMap<String, u64>,
    top_by_size_bytes: Vec<RankedFile>,
    top_by_row_count: Vec<RankedFile>,
    top_by_column_count: Vec<RankedFile>,
    top_by_string_columns: Vec<RankedFile>,
}

#[derive(Debug, Clone, Serialize)]
struct RankedFile {
    path: String,
    file_name: String,
    value: u64,
}

#[derive(Debug, Serialize)]
struct CorpusCsvRow {
    corpus_roots: String,
    corpus_sample_rows: usize,
    path: String,
    file_name: String,
    source_group: String,
    status: String,
    error: String,
    size_megabytes: f64,
    table_name: String,
    encoding: String,
    compression: String,
    row_count: u64,
    column_count: usize,
    row_len: u32,
    page_size: u32,
    page_count: u64,
    string_columns: usize,
    integer_columns: usize,
    float_columns: usize,
    date_columns: usize,
    datetime_columns: usize,
    time_columns: usize,
    bytes_columns: usize,
    numeric_like_columns: usize,
    string_width_sum: u64,
    string_width_max: u32,
    numeric_width_sum: u64,
    numeric_width_max: u32,
    date_format_columns: usize,
    datetime_format_columns: usize,
    time_format_columns: usize,
    date_formats: String,
    datetime_formats: String,
    time_formats: String,
    rows_sampled: u64,
    string_cells: u64,
    empty_string_cells: u64,
    empty_string_ratio: f64,
    ascii_string_cells: u64,
    ascii_ratio: f64,
    non_ascii_string_cells: u64,
    avg_trimmed_string_len: f64,
    max_trimmed_string_len: u64,
    numeric_like_cells: u64,
    null_numeric_like_cells: u64,
    missing_numeric_ratio: f64,
    compression_class: String,
    encoding_class: String,
    size_class: String,
    width_class: String,
    content_class: String,
    categorical_heavy: bool,
}

#[derive(Debug, Clone)]
struct CorpusCsvContext {
    roots: String,
    sample_rows: usize,
}

#[derive(Debug, Clone)]
struct ScanCsvContext {
    roots: String,
    mode: String,
    projection: String,
    io_backend: String,
    batch_rows: usize,
    limit: String,
}

#[derive(Debug, Clone, Copy)]
struct ScanRunOptions {
    mode: ProfileMode,
    projection: ProjectionPreset,
    batch_rows: usize,
    io_backend: IoBackendPreference,
    limit: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct CorpusScanCsvRow {
    corpus_roots: String,
    corpus_mode: String,
    corpus_projection: String,
    corpus_io_backend: String,
    corpus_batch_rows: usize,
    corpus_limit: String,
    path: String,
    file_name: String,
    source_group: String,
    status: String,
    error: String,
    size_megabytes: f64,
    table_name: String,
    encoding: String,
    compression: String,
    row_count: u64,
    column_count: usize,
    row_len: u32,
    page_size: u32,
    page_count: u64,
    string_columns: usize,
    integer_columns: usize,
    float_columns: usize,
    date_columns: usize,
    datetime_columns: usize,
    time_columns: usize,
    bytes_columns: usize,
    numeric_like_columns: usize,
    string_width_sum: u64,
    string_width_max: u32,
    numeric_width_sum: u64,
    numeric_width_max: u32,
    date_format_columns: usize,
    datetime_format_columns: usize,
    time_format_columns: usize,
    date_formats: String,
    datetime_formats: String,
    time_formats: String,
    projected_columns: usize,
    projected_string_columns: usize,
    projected_numeric_like_columns: usize,
    projected_physical_width_sum: u64,
    projected_string_width_sum: u64,
    projected_numeric_width_sum: u64,
    compression_class: String,
    encoding_class: String,
    size_class: String,
    width_class: String,
    content_class: String,
    elapsed_ns: u128,
    rows_per_second: f64,
    bytes_per_second: f64,
    rows_seen: u64,
    rows_emitted: u64,
    pages_seen: u64,
    fused_pages: u64,
    indexed_pages: u64,
    compressed_pages: u64,
    raw_bytes_read: u64,
    row_bytes_materialized: u64,
    decode_batches: u64,
    pages_per_second: f64,
    rows_per_page: f64,
    raw_bytes_per_row: f64,
    materialized_bytes_per_row: f64,
    materialization_ratio: f64,
}

#[derive(Debug, Clone)]
struct ScanRankedFile {
    path: String,
    file_name: String,
    value: u128,
}

#[derive(Debug, Clone)]
struct ScanProfileResult {
    scan_row: CorpusScanCsvRow,
    structural_entry: FixtureEntry,
}

#[derive(Debug, Clone, Default)]
struct ScanSummary {
    discovered_files: usize,
    profiled_files: usize,
    failed_files: usize,
    total_elapsed_ns: u128,
    total_rows_emitted: u64,
    total_raw_bytes_read: u64,
    total_row_bytes_materialized: u64,
    total_pages_seen: u64,
    total_compressed_pages: u64,
    slowest_by_elapsed: Vec<ScanRankedFile>,
    largest_by_raw_bytes: Vec<ScanRankedFile>,
}

#[derive(Debug, Clone, Copy, Default)]
struct ProjectedScanShape {
    projected_columns: usize,
    projected_string_columns: usize,
    projected_numeric_like_columns: usize,
    projected_physical_width_sum: u64,
    projected_string_width_sum: u64,
    projected_numeric_width_sum: u64,
}

#[derive(Debug, Clone, Copy)]
struct ContentColumn {
    logical_type: LogicalType,
    start: usize,
    end: usize,
}

#[derive(Debug, Clone)]
struct FullContentAccumulator {
    columns: Vec<ContentColumn>,
    endianness: Endianness,
    sample: SampleSummary,
}

#[derive(Debug, Clone)]
struct WeightedPath {
    path: PathBuf,
    size_bytes: u64,
    work_units: u64,
}

#[derive(Debug, Clone)]
struct ProgressReporter {
    span: Span,
    total_files: usize,
    total_bytes: u64,
    completed_files: Arc<AtomicUsize>,
    completed_bytes: Arc<AtomicU64>,
    active_files: Arc<AtomicUsize>,
}

#[derive(Debug, Clone)]
struct FileProgressReporter {
    progress: ProgressReporter,
    label: &'static str,
    work_units: u64,
    forwarded_units: Arc<AtomicU64>,
    report_granularity_units: u64,
}

impl ProgressReporter {
    fn new(message: &str, total_files: usize, total_bytes: u64) -> Self {
        init_progress_subscriber();
        let span = tracing::info_span!("corpus_profile");
        span.pb_set_style(&progress_style());
        span.pb_set_length(total_bytes.max(1));
        span.pb_set_message(&format_progress_message(
            message,
            0,
            total_files,
            0,
            total_bytes,
            0,
        ));
        span.pb_set_finish_message("done");
        span.pb_start();
        Self {
            span,
            total_files,
            total_bytes,
            completed_files: Arc::new(AtomicUsize::new(0)),
            completed_bytes: Arc::new(AtomicU64::new(0)),
            active_files: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn start_file(&self, label: &str) {
        let active_files = self.active_files.fetch_add(1, Ordering::Relaxed) + 1;
        let files_done = self.completed_files.load(Ordering::Relaxed);
        let bytes_done = self.completed_bytes.load(Ordering::Relaxed);
        self.span.pb_set_message(&format_progress_message(
            label,
            files_done,
            self.total_files,
            bytes_done,
            self.total_bytes,
            active_files,
        ));
    }

    fn finish_file(&self, label: &str, work_units: u64) {
        let files_done = self.completed_files.fetch_add(1, Ordering::Relaxed) + 1;
        let bytes_done = self
            .completed_bytes
            .fetch_add(work_units, Ordering::Relaxed)
            .saturating_add(work_units)
            .min(self.total_bytes);
        let active_files = self
            .active_files
            .fetch_sub(1, Ordering::Relaxed)
            .saturating_sub(1);
        self.span.pb_set_position(bytes_done);
        self.span.pb_set_message(&format_progress_message(
            label,
            files_done,
            self.total_files,
            bytes_done,
            self.total_bytes,
            active_files,
        ));
    }

    fn advance_work(&self, label: &str, delta: u64) {
        if delta == 0 {
            return;
        }
        let bytes_done = self
            .completed_bytes
            .fetch_add(delta, Ordering::Relaxed)
            .saturating_add(delta)
            .min(self.total_bytes);
        let files_done = self.completed_files.load(Ordering::Relaxed);
        let active_files = self.active_files.load(Ordering::Relaxed);
        self.span.pb_set_position(bytes_done);
        self.span.pb_set_message(&format_progress_message(
            label,
            files_done,
            self.total_files,
            bytes_done,
            self.total_bytes,
            active_files,
        ));
    }

    fn finish(&self, message: &str) {
        self.span.pb_set_position(self.total_bytes.max(1));
        self.span.pb_set_finish_message(message);
    }
}

impl FileProgressReporter {
    fn new(progress: ProgressReporter, label: &'static str, work_units: u64) -> Self {
        Self {
            progress,
            label,
            work_units,
            forwarded_units: Arc::new(AtomicU64::new(0)),
            report_granularity_units: report_granularity_units(work_units),
        }
    }

    fn on_scan_progress(&self, progress: ScanProgress) {
        let current_units = scaled_work_units(
            progress.raw_bytes_read,
            progress.estimated_total_bytes,
            self.work_units,
        );
        let forwarded_units = self.forwarded_units.load(Ordering::Relaxed);
        let should_forward = current_units >= self.work_units
            || current_units.saturating_sub(forwarded_units) >= self.report_granularity_units;
        if should_forward {
            let previous_units = self.forwarded_units.swap(current_units, Ordering::Relaxed);
            if current_units > previous_units {
                self.progress
                    .advance_work(self.label, current_units - previous_units);
            }
        }
    }

    fn finish(&self) {
        let forwarded_units = self.forwarded_units.load(Ordering::Relaxed);
        let residual_units = self.work_units.saturating_sub(forwarded_units);
        self.progress.finish_file(self.label, residual_units);
    }
}

fn init_progress_subscriber() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let indicatif_layer = IndicatifLayer::new();
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(indicatif_layer.get_stderr_writer())
                    .without_time()
                    .with_target(false)
                    .with_level(false),
            )
            .with(indicatif_layer)
            .init();
    });
}

fn progress_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{spinner:.green} {msg} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {percent:>3}%",
    )
    .expect("progress template")
}

fn weighted_paths(paths: &[PathBuf]) -> Vec<WeightedPath> {
    let mut weighted: Vec<WeightedPath> = paths
        .iter()
        .map(|path| {
            let size_bytes = fs::metadata(path).map_or(0, |meta| meta.len());
            WeightedPath {
                path: path.clone(),
                size_bytes,
                work_units: size_bytes.max(1),
            }
        })
        .collect();
    weighted.sort_by(|left, right| {
        right
            .size_bytes
            .cmp(&left.size_bytes)
            .then_with(|| left.path.cmp(&right.path))
    });
    weighted
}

fn total_work_units(paths: &[WeightedPath]) -> u64 {
    paths.iter().map(|path| path.work_units).sum()
}

fn format_progress_message(
    label: &str,
    files_done: usize,
    total_files: usize,
    bytes_done: u64,
    total_bytes: u64,
    active_files: usize,
) -> String {
    format!(
        "{label} {files_done}/{total_files} files, active {active_files}, {}/{}",
        format_bytes(bytes_done),
        format_bytes(total_bytes)
    )
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn scaled_work_units(raw_bytes_read: u64, estimated_total_bytes: u64, work_units: u64) -> u64 {
    if work_units == 0 || estimated_total_bytes == 0 {
        return 0;
    }
    let scaled = (u128::from(raw_bytes_read) * u128::from(work_units))
        / u128::from(estimated_total_bytes.max(1));
    u64::try_from(scaled).unwrap_or(u64::MAX).min(work_units)
}

fn report_granularity_units(work_units: u64) -> u64 {
    const MIN_GRANULARITY: u64 = 4 * 1024 * 1024;
    const MAX_GRANULARITY: u64 = 64 * 1024 * 1024;

    if work_units <= MIN_GRANULARITY {
        return work_units.max(1);
    }

    (work_units / 256).clamp(MIN_GRANULARITY, MAX_GRANULARITY).min(work_units)
}

impl FullContentAccumulator {
    fn new(ds: &Dataset) -> Self {
        let columns = ds
            .columns()
            .iter()
            .map(|column| {
                let start = usize::try_from(column.offset).unwrap_or(usize::MAX);
                let width = usize::try_from(column.physical_width).unwrap_or(usize::MAX);
                let end = start.saturating_add(width);
                ContentColumn {
                    logical_type: column.logical_type,
                    start,
                    end,
                }
            })
            .collect();
        Self {
            columns,
            endianness: ds.metadata().endianness,
            sample: SampleSummary::default(),
        }
    }

    fn observe_row(&mut self, bytes: &[u8]) {
        self.sample.rows_sampled = self.sample.rows_sampled.saturating_add(1);
        for column in &self.columns {
            let slice = bytes.get(column.start..column.end).unwrap_or(&[]);
            match column.logical_type {
                LogicalType::String | LogicalType::Bytes => {
                    let trimmed = trim_trailing_space_or_nul(slice);
                    let trimmed_len = u64::try_from(trimmed.len()).unwrap_or(u64::MAX);
                    self.sample.string_cells = self.sample.string_cells.saturating_add(1);
                    self.sample.total_trimmed_string_len = self
                        .sample
                        .total_trimmed_string_len
                        .saturating_add(trimmed_len);
                    self.sample.max_trimmed_string_len =
                        self.sample.max_trimmed_string_len.max(trimmed_len);
                    if trimmed.is_empty() {
                        self.sample.empty_string_cells =
                            self.sample.empty_string_cells.saturating_add(1);
                    }
                    if trimmed.is_ascii() {
                        self.sample.ascii_string_cells =
                            self.sample.ascii_string_cells.saturating_add(1);
                    } else {
                        self.sample.non_ascii_string_cells =
                            self.sample.non_ascii_string_cells.saturating_add(1);
                    }
                }
                LogicalType::Integer
                | LogicalType::Float
                | LogicalType::Date
                | LogicalType::DateTime
                | LogicalType::Time => {
                    self.sample.numeric_like_cells =
                        self.sample.numeric_like_cells.saturating_add(1);
                    if numeric_slice_is_missing(slice, self.endianness) {
                        self.sample.null_numeric_like_cells =
                            self.sample.null_numeric_like_cells.saturating_add(1);
                    }
                }
            }
        }
    }

    fn into_sample(self) -> SampleSummary {
        self.sample
    }
}

fn trim_trailing_space_or_nul(slice: &[u8]) -> &[u8] {
    let mut end = slice.len();
    while end > 0 {
        let byte = slice[end - 1];
        if byte != b' ' && byte != 0 {
            break;
        }
        end -= 1;
    }
    &slice[..end]
}

fn numeric_slice_is_missing(slice: &[u8], endianness: Endianness) -> bool {
    numeric_bits_is_missing(numeric_bits(slice, endianness))
}

fn numeric_bits(slice: &[u8], endianness: Endianness) -> u64 {
    if slice.is_empty() {
        return SAS_NUMERIC_MISSING_SENTINEL;
    }
    let mut buf = [0u8; 8];
    if slice.len() >= 8 {
        match endianness {
            Endianness::Big => buf.copy_from_slice(&slice[..8]),
            Endianness::Little => {
                buf.copy_from_slice(&slice[..8]);
                buf.reverse();
            }
        }
        return u64::from_be_bytes(buf);
    }

    match endianness {
        Endianness::Big => {
            buf[..slice.len()].copy_from_slice(slice);
        }
        Endianness::Little => {
            buf[..slice.len()].copy_from_slice(slice);
            buf[..slice.len()].reverse();
        }
    }
    u64::from_be_bytes(buf)
}

const NUMERIC_EXP_MASK: u64 = 0x7FF0_0000_0000_0000;
const NUMERIC_FRACTION_MASK: u64 = 0x000F_FFFF_FFFF_FFFF;
const SAS_NUMERIC_MISSING_SENTINEL: u64 = 0x7FF0_0000_0000_0001;

const fn numeric_bits_is_missing(raw: u64) -> bool {
    (raw & NUMERIC_EXP_MASK) == NUMERIC_EXP_MASK && (raw & NUMERIC_FRACTION_MASK) != 0
}

fn collect_fixture_entries(paths: &[PathBuf], sample_rows: usize) -> Vec<FixtureEntry> {
    let weighted = weighted_paths(paths);
    let progress = ProgressReporter::new(
        "profiling corpus structure",
        weighted.len(),
        total_work_units(&weighted),
    );
    let fixtures: Vec<FixtureEntry> = weighted
        .par_iter()
        .map(|item| {
            progress.start_file("profiling corpus structure");
            let entry = profile_fixture(&item.path, sample_rows);
            progress.finish_file("profiling corpus structure", item.work_units);
            entry
        })
        .collect();

    progress.finish("structure profiling complete");
    fixtures
}

fn collect_scan_rows(
    paths: &[PathBuf],
    context: &ScanCsvContext,
    options: ScanRunOptions,
) -> Vec<ScanProfileResult> {
    let weighted = weighted_paths(paths);
    let progress = ProgressReporter::new(
        "profiling corpus scans",
        weighted.len(),
        total_work_units(&weighted),
    );
    let rows: Vec<ScanProfileResult> = weighted
        .par_iter()
        .map(|item| {
            progress.start_file("profiling corpus scans");
            build_scan_csv_row(
                &item.path,
                context,
                options,
                Some(FileProgressReporter::new(
                    progress.clone(),
                    "profiling corpus scans",
                    item.work_units,
                )),
            )
        })
        .collect();

    progress.finish("scan profiling complete");
    rows
}

fn main() -> ExitCode {
    init_profiler_runtime();
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> std::result::Result<(), String> {
    let mut args = env::args_os().skip(1);
    let mut sample_rows = 256usize;
    let mut out: Option<PathBuf> = None;
    let mut failed_from: Option<PathBuf> = None;
    let mut summary_only = false;
    let mut format = OutputFormat::Json;
    let mut format_explicit = false;
    let mut scan_mode: Option<ProfileMode> = None;
    let mut scan_projection = ProjectionPreset::Full;
    let mut scan_batch_rows = 256usize;
    let mut scan_io_backend = IoBackendPreference::Auto;
    let mut scan_limit: Option<u64> = None;
    let mut inputs = Vec::new();

    while let Some(arg) = args.next() {
        match arg.to_string_lossy().as_ref() {
            "--sample-rows" => {
                let Some(value) = args.next() else {
                    return Err("missing value after --sample-rows".to_owned());
                };
                sample_rows = value
                    .to_string_lossy()
                    .parse()
                    .map_err(|_| "invalid --sample-rows value".to_owned())?;
            }
            "--out" => {
                let Some(value) = args.next() else {
                    return Err("missing value after --out".to_owned());
                };
                out = Some(PathBuf::from(value));
            }
            "--failed-from" => {
                let Some(value) = args.next() else {
                    return Err("missing value after --failed-from".to_owned());
                };
                failed_from = Some(PathBuf::from(value));
            }
            "--format" => {
                let Some(value) = args.next() else {
                    return Err("missing value after --format".to_owned());
                };
                format = OutputFormat::parse(&value.to_string_lossy())?;
                format_explicit = true;
            }
            "--scan-mode" | "--mode" => {
                let Some(value) = args.next() else {
                    return Err(format!("missing value after {}", arg.to_string_lossy()));
                };
                scan_mode = Some(
                    ProfileMode::parse(&value.to_string_lossy())
                        .ok_or_else(|| format!("invalid {} value", arg.to_string_lossy()))?,
                );
            }
            "--scan-projection" | "--projection" => {
                let Some(value) = args.next() else {
                    return Err(format!("missing value after {}", arg.to_string_lossy()));
                };
                scan_projection = ProjectionPreset::parse(&value.to_string_lossy())
                    .ok_or_else(|| format!("invalid {} value", arg.to_string_lossy()))?;
            }
            "--scan-batch-rows" | "--batch-rows" => {
                let Some(value) = args.next() else {
                    return Err(format!("missing value after {}", arg.to_string_lossy()));
                };
                scan_batch_rows = value
                    .to_string_lossy()
                    .parse()
                    .map_err(|_| format!("invalid {} value", arg.to_string_lossy()))?;
            }
            "--scan-io-backend" | "--io-backend" => {
                let Some(value) = args.next() else {
                    return Err(format!("missing value after {}", arg.to_string_lossy()));
                };
                scan_io_backend = parse_io_backend(&value.to_string_lossy())
                    .ok_or_else(|| format!("invalid {} value", arg.to_string_lossy()))?;
            }
            "--scan-limit" | "--limit" => {
                let Some(value) = args.next() else {
                    return Err(format!("missing value after {}", arg.to_string_lossy()));
                };
                let parsed: u64 = value
                    .to_string_lossy()
                    .parse()
                    .map_err(|_| format!("invalid {} value", arg.to_string_lossy()))?;
                scan_limit = (parsed != 0).then_some(parsed);
            }
            "--summary-only" => {
                summary_only = true;
            }
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            value if value.starts_with("--") => {
                return Err(format!("unexpected argument: {value}"));
            }
            value => inputs.push(PathBuf::from(value)),
        }
    }

    if inputs.is_empty() && failed_from.is_none() {
        return Err("usage requires at least one input path".to_owned());
    }

    let roots = display_roots(&inputs, failed_from.as_deref());
    let paths = if let Some(failed_from) = failed_from.as_deref() {
        load_failed_paths(failed_from, &inputs)?
    } else {
        discover_fixture_paths(&inputs).map_err(|err| err.to_string())?
    };
    if paths.is_empty() {
        return Err("no files selected for profiling".to_owned());
    }

    if let Some(mode) = scan_mode {
        if format_explicit && format != OutputFormat::Csv {
            return Err("scan profiling currently supports only --format csv".to_owned());
        }
        if summary_only {
            return Err("--summary-only is not supported with --scan-mode".to_owned());
        }
        return write_scan_profile(
            &paths,
            &roots,
            out,
            ScanRunOptions {
                mode,
                projection: scan_projection,
                batch_rows: scan_batch_rows,
                io_backend: scan_io_backend,
                limit: scan_limit,
            },
        );
    }

    let catalog = FixtureCatalog {
        roots: roots.clone(),
        sample_rows,
        fixtures: collect_fixture_entries(&paths, sample_rows),
    };

    let summary = summarize_catalog(&catalog);
    match format {
        OutputFormat::Json => write_json(catalog, roots, sample_rows, summary, summary_only, out),
        OutputFormat::Csv => write_csv(catalog, roots, sample_rows, summary, summary_only, out),
    }
}

fn write_json(
    catalog: FixtureCatalog,
    roots: Vec<String>,
    sample_rows: usize,
    summary: CorpusSummary,
    summary_only: bool,
    out: Option<PathBuf>,
) -> std::result::Result<(), String> {
    let output = CorpusProfileOutput {
        roots,
        sample_rows,
        summary,
        fixtures: if summary_only {
            Vec::new()
        } else {
            catalog.fixtures
        },
    };

    let json = serde_json::to_string_pretty(&output).map_err(|err| err.to_string())?;
    if let Some(path) = out {
        fs::write(path, json).map_err(|err| err.to_string())?;
    } else {
        println!("{json}");
    }
    Ok(())
}

fn write_csv(
    catalog: FixtureCatalog,
    roots: Vec<String>,
    sample_rows: usize,
    summary: CorpusSummary,
    summary_only: bool,
    out: Option<PathBuf>,
) -> std::result::Result<(), String> {
    let context = CorpusCsvContext {
        roots: roots.join("|"),
        sample_rows,
    };
    let rows = build_csv_rows(&catalog.fixtures, &context, summary_only);

    if let Some(path) = out {
        let mut writer = Writer::from_path(&path).map_err(|err| err.to_string())?;
        for row in rows {
            writer.serialize(row).map_err(|err| err.to_string())?;
        }
        writer.flush().map_err(|err| err.to_string())?;
        let summary_path = summary_txt_path(&path);
        fs::write(
            summary_path,
            render_summary_txt(&summary, &roots, sample_rows),
        )
        .map_err(|err| err.to_string())
    } else {
        let stdout = std::io::stdout();
        let mut writer = Writer::from_writer(stdout.lock());
        for row in rows {
            writer.serialize(row).map_err(|err| err.to_string())?;
        }
        writer.flush().map_err(|err| err.to_string())
    }
}

fn summarize_catalog(catalog: &FixtureCatalog) -> CorpusSummary {
    let mut summary = CorpusSummary {
        discovered_files: catalog.fixtures.len(),
        ..CorpusSummary::default()
    };

    let mut size_ranked = Vec::new();
    let mut row_ranked = Vec::new();
    let mut column_ranked = Vec::new();
    let mut string_ranked = Vec::new();

    for fixture in &catalog.fixtures {
        summary.total_size_bytes = summary.total_size_bytes.saturating_add(fixture.size_bytes);
        size_ranked.push(RankedFile {
            path: fixture.path.clone(),
            file_name: fixture.file_name.clone(),
            value: fixture.size_bytes,
        });

        match &fixture.status {
            FixtureStatus::Profiled(profile) => {
                summary.profiled_files += 1;
                accumulate_profile(&mut summary, fixture, profile);
                row_ranked.push(RankedFile {
                    path: fixture.path.clone(),
                    file_name: fixture.file_name.clone(),
                    value: profile.row_count,
                });
                column_ranked.push(RankedFile {
                    path: fixture.path.clone(),
                    file_name: fixture.file_name.clone(),
                    value: profile.column_count as u64,
                });
                string_ranked.push(RankedFile {
                    path: fixture.path.clone(),
                    file_name: fixture.file_name.clone(),
                    value: profile.logical_types.string as u64,
                });
            }
            FixtureStatus::Error { .. } => {
                summary.failed_files += 1;
            }
        }
    }

    summary.top_by_size_bytes = top_n(size_ranked, 10);
    summary.top_by_row_count = top_n(row_ranked, 10);
    summary.top_by_column_count = top_n(column_ranked, 10);
    summary.top_by_string_columns = top_n(string_ranked, 10);
    summary
}

fn accumulate_profile(
    summary: &mut CorpusSummary,
    fixture: &FixtureEntry,
    profile: &FixtureProfile,
) {
    summary.total_rows = summary.total_rows.saturating_add(profile.row_count);
    summary.total_columns = summary
        .total_columns
        .saturating_add(profile.column_count as u64);
    summary.total_string_columns = summary
        .total_string_columns
        .saturating_add(profile.logical_types.string as u64);
    let numeric_like = profile.logical_types.integer
        + profile.logical_types.float
        + profile.logical_types.date
        + profile.logical_types.datetime
        + profile.logical_types.time;
    summary.total_numeric_like_columns = summary
        .total_numeric_like_columns
        .saturating_add(numeric_like as u64);
    summary.total_sampled_string_cells = summary
        .total_sampled_string_cells
        .saturating_add(profile.sample.string_cells);
    summary.total_sampled_empty_string_cells = summary
        .total_sampled_empty_string_cells
        .saturating_add(profile.sample.empty_string_cells);
    summary.total_sampled_ascii_string_cells = summary
        .total_sampled_ascii_string_cells
        .saturating_add(profile.sample.ascii_string_cells);

    *summary
        .compression_counts
        .entry(profile.compression.clone())
        .or_default() += 1;
    let encoding = profile
        .encoding
        .clone()
        .unwrap_or_else(|| "unknown".to_owned());
    *summary.encoding_counts.entry(encoding).or_default() += 1;
    for tag in &profile.tags {
        *summary.tag_counts.entry(tag.clone()).or_default() += 1;
    }

    if let Some(source_group) = fixture.path.split('/').nth_back(1) {
        let key = format!("source:{source_group}");
        *summary.tag_counts.entry(key).or_default() += 1;
    }
}

fn build_csv_rows(
    fixtures: &[FixtureEntry],
    context: &CorpusCsvContext,
    summary_only: bool,
) -> Vec<CorpusCsvRow> {
    if summary_only {
        return vec![build_csv_row(None, context)];
    }
    fixtures
        .iter()
        .map(|fixture| build_csv_row(Some(fixture), context))
        .collect()
}

fn build_csv_row(fixture: Option<&FixtureEntry>, context: &CorpusCsvContext) -> CorpusCsvRow {
    let mut row = CorpusCsvRow {
        corpus_roots: context.roots.clone(),
        corpus_sample_rows: context.sample_rows,
        path: String::new(),
        file_name: String::new(),
        source_group: String::new(),
        status: String::new(),
        error: String::new(),
        size_megabytes: 0.0,
        table_name: String::new(),
        encoding: String::new(),
        compression: String::new(),
        row_count: 0,
        column_count: 0,
        row_len: 0,
        page_size: 0,
        page_count: 0,
        string_columns: 0,
        integer_columns: 0,
        float_columns: 0,
        date_columns: 0,
        datetime_columns: 0,
        time_columns: 0,
        bytes_columns: 0,
        numeric_like_columns: 0,
        string_width_sum: 0,
        string_width_max: 0,
        numeric_width_sum: 0,
        numeric_width_max: 0,
        date_format_columns: 0,
        datetime_format_columns: 0,
        time_format_columns: 0,
        date_formats: String::new(),
        datetime_formats: String::new(),
        time_formats: String::new(),
        rows_sampled: 0,
        string_cells: 0,
        empty_string_cells: 0,
        empty_string_ratio: 0.0,
        ascii_string_cells: 0,
        ascii_ratio: 0.0,
        non_ascii_string_cells: 0,
        avg_trimmed_string_len: 0.0,
        max_trimmed_string_len: 0,
        numeric_like_cells: 0,
        null_numeric_like_cells: 0,
        missing_numeric_ratio: 0.0,
        compression_class: String::new(),
        encoding_class: String::new(),
        size_class: String::new(),
        width_class: String::new(),
        content_class: String::new(),
        categorical_heavy: false,
    };

    let Some(fixture) = fixture else {
        return row;
    };

    row.path = fixture.path.clone();
    row.file_name = fixture.file_name.clone();
    row.source_group = fixture.source_group.clone();
    row.size_megabytes = bytes_to_megabytes(fixture.size_bytes);

    match &fixture.status {
        FixtureStatus::Profiled(profile) => {
            row.status = "profiled".to_owned();
            row.table_name = profile.table_name.clone().unwrap_or_default();
            row.encoding = profile.encoding.clone().unwrap_or_default();
            row.compression = profile.compression.clone();
            row.row_count = profile.row_count;
            row.column_count = profile.column_count;
            row.row_len = profile.row_len;
            row.page_size = profile.page_size;
            row.page_count = profile.page_count;
            row.string_columns = profile.logical_types.string;
            row.integer_columns = profile.logical_types.integer;
            row.float_columns = profile.logical_types.float;
            row.date_columns = profile.logical_types.date;
            row.datetime_columns = profile.logical_types.datetime;
            row.time_columns = profile.logical_types.time;
            row.bytes_columns = profile.logical_types.bytes;
            row.numeric_like_columns = profile.logical_types.integer
                + profile.logical_types.float
                + profile.logical_types.date
                + profile.logical_types.datetime
                + profile.logical_types.time;
            row.string_width_sum = profile.widths.string_width_sum;
            row.string_width_max = profile.widths.string_width_max;
            row.numeric_width_sum = profile.widths.numeric_width_sum;
            row.numeric_width_max = profile.widths.numeric_width_max;
            row.date_format_columns = profile.temporal_formats.date_format_columns;
            row.datetime_format_columns = profile.temporal_formats.datetime_format_columns;
            row.time_format_columns = profile.temporal_formats.time_format_columns;
            row.date_formats = join_named_counts(&profile.temporal_formats.date_formats);
            row.datetime_formats = join_named_counts(&profile.temporal_formats.datetime_formats);
            row.time_formats = join_named_counts(&profile.temporal_formats.time_formats);
            row.rows_sampled = profile.sample.rows_sampled;
            row.string_cells = profile.sample.string_cells;
            row.empty_string_cells = profile.sample.empty_string_cells;
            row.empty_string_ratio = round_metric(profile.sample.empty_string_ratio());
            row.ascii_string_cells = profile.sample.ascii_string_cells;
            row.ascii_ratio = round_metric(profile.sample.ascii_ratio());
            row.non_ascii_string_cells = profile.sample.non_ascii_string_cells;
            row.avg_trimmed_string_len = round_metric(profile.sample.avg_trimmed_string_len());
            row.max_trimmed_string_len = profile.sample.max_trimmed_string_len;
            row.numeric_like_cells = profile.sample.numeric_like_cells;
            row.null_numeric_like_cells = profile.sample.null_numeric_like_cells;
            row.missing_numeric_ratio = round_metric(profile.sample.missing_numeric_ratio());
            row.compression_class = profile.compression.clone();
            row.encoding_class = encoding_class(profile);
            row.size_class = size_class(profile);
            row.width_class = width_class(profile);
            row.content_class = content_class(profile);
            row.categorical_heavy = profile.tags.iter().any(|tag| tag == "categorical-heavy");
        }
        FixtureStatus::Error { error } => {
            row.status = "error".to_owned();
            row.error = error.clone();
            row.compression_class = "unknown".to_owned();
            row.encoding_class = "unknown".to_owned();
            row.size_class = "unknown".to_owned();
            row.width_class = "unknown".to_owned();
            row.content_class = "unknown".to_owned();
        }
    }

    row
}

fn join_map(values: &BTreeMap<String, u64>) -> String {
    values
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("|")
}

fn join_ranked_files(files: &[RankedFile]) -> String {
    files
        .iter()
        .map(|file| format!("{}:{}@{}", file.file_name, file.value, file.path))
        .collect::<Vec<_>>()
        .join("|")
}

fn join_ranked_files_megabytes(files: &[RankedFile]) -> String {
    files
        .iter()
        .map(|file| {
            format!(
                "{}:{}@{}",
                file.file_name,
                bytes_to_megabytes(file.value),
                file.path
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn join_named_counts(values: &[NamedCount]) -> String {
    values
        .iter()
        .map(|value| format!("{}={}", value.name, value.count))
        .collect::<Vec<_>>()
        .join("|")
}

fn encoding_class(profile: &FixtureProfile) -> String {
    match profile.encoding.as_deref() {
        Some("UTF-8") => "utf8".to_owned(),
        Some(_) => "legacy".to_owned(),
        None => "unknown".to_owned(),
    }
}

fn normalize_root_display(path: &Path) -> String {
    let raw = path.display().to_string();
    if raw.len() <= 1 {
        return raw;
    }
    raw.trim_end_matches(['/', '\\']).to_owned()
}

fn display_roots(inputs: &[PathBuf], failed_from: Option<&Path>) -> Vec<String> {
    let mut roots: Vec<String> = inputs
        .iter()
        .map(|path| normalize_root_display(path))
        .collect();
    if let Some(failed_from) = failed_from {
        roots.push(format!(
            "failed-from:{}",
            normalize_root_display(failed_from)
        ));
    }
    roots
}

fn load_failed_paths(
    failed_from: &Path,
    inputs: &[PathBuf],
) -> std::result::Result<Vec<PathBuf>, String> {
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_path(failed_from)
        .map_err(|err| format!("failed to read {}: {err}", failed_from.display()))?;
    let headers = reader
        .headers()
        .map_err(|err| {
            format!(
                "failed to read CSV headers from {}: {err}",
                failed_from.display()
            )
        })?
        .clone();
    let status_index = headers
        .iter()
        .position(|header| header == "status")
        .ok_or_else(|| format!("{} does not contain a status column", failed_from.display()))?;
    let path_index = headers
        .iter()
        .position(|header| header == "path")
        .ok_or_else(|| format!("{} does not contain a path column", failed_from.display()))?;

    let mut failed_paths = BTreeSet::new();
    for record in reader.records() {
        let record = record.map_err(|err| {
            format!(
                "failed to read CSV row from {}: {err}",
                failed_from.display()
            )
        })?;
        if record.get(status_index) != Some("error") {
            continue;
        }
        let Some(path) = record.get(path_index) else {
            continue;
        };
        if path.is_empty() {
            continue;
        }
        let candidate = PathBuf::from(path);
        if !inputs.is_empty() && !inputs.iter().any(|root| candidate.starts_with(root)) {
            continue;
        }
        failed_paths.insert(candidate);
    }

    Ok(failed_paths.into_iter().collect())
}

fn source_group(path: &Path) -> String {
    let mut iter = path.components().map(std::path::Component::as_os_str);
    while let Some(component) = iter.next() {
        if component == "raw_data" {
            return iter.next().map_or_else(
                || "raw_data".to_owned(),
                |part| part.to_string_lossy().into_owned(),
            );
        }
    }
    path.parent().and_then(Path::file_name).map_or_else(
        || "root".to_owned(),
        |name| name.to_string_lossy().into_owned(),
    )
}

fn round_metric(value: f64) -> f64 {
    (value * 1_000_000.0).round() / 1_000_000.0
}

fn bytes_to_megabytes(bytes: u64) -> f64 {
    round_metric(bytes as f64 / 1_000_000.0)
}

fn size_class(profile: &FixtureProfile) -> String {
    let page_bytes = u64::from(profile.page_size).saturating_mul(profile.page_count);
    match page_bytes {
        0..=1_048_575 => "tiny".to_owned(),
        1_048_576..=16_777_215 => "small".to_owned(),
        16_777_216..=268_435_455 => "medium".to_owned(),
        268_435_456..=1_073_741_823 => "large".to_owned(),
        _ => "huge".to_owned(),
    }
}

fn width_class(profile: &FixtureProfile) -> String {
    if profile.column_count > 1024 || profile.row_len > 4096 {
        "ultra-wide".to_owned()
    } else if profile.column_count > 64 || profile.row_len > 256 {
        "wide".to_owned()
    } else if profile.column_count > 16 || profile.row_len > 64 {
        "medium".to_owned()
    } else {
        "narrow".to_owned()
    }
}

fn content_class(profile: &FixtureProfile) -> String {
    let total = profile.column_count;
    if total == 0 {
        return "empty".to_owned();
    }
    let string_like = profile.logical_types.string + profile.logical_types.bytes;
    let numeric_like = profile.logical_types.integer
        + profile.logical_types.float
        + profile.logical_types.date
        + profile.logical_types.datetime
        + profile.logical_types.time;
    if string_like as f64 / total as f64 >= 0.7 {
        "string-heavy".to_owned()
    } else if numeric_like as f64 / total as f64 >= 0.7 {
        "numeric-heavy".to_owned()
    } else {
        "mixed".to_owned()
    }
}

fn summary_txt_path(csv_path: &Path) -> PathBuf {
    let stem = csv_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("corpus_profile");
    let summary_name = format!("{stem}_summary.txt");
    csv_path.with_file_name(summary_name)
}

fn structural_companion_csv_path(scan_csv_path: &Path) -> PathBuf {
    let stem = scan_csv_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("corpus_scan_profile");
    let ext = scan_csv_path.extension().and_then(|value| value.to_str());
    let companion_stem = if let Some(prefix) = stem.strip_suffix("_scan_profile") {
        format!("{prefix}_profile")
    } else {
        format!("{stem}_structural")
    };
    let file_name = if let Some(ext) = ext {
        format!("{companion_stem}.{ext}")
    } else {
        companion_stem
    };
    scan_csv_path.with_file_name(file_name)
}

fn render_summary_txt(summary: &CorpusSummary, roots: &[String], sample_rows: usize) -> String {
    let mut out = String::new();
    out.push_str("Corpus Profile Summary\n");
    out.push_str("======================\n\n");
    out.push_str(&format!("roots: {}\n", roots.join(", ")));
    out.push_str(&format!("sample_rows: {sample_rows}\n"));
    out.push_str(&format!("discovered_files: {}\n", summary.discovered_files));
    out.push_str(&format!("profiled_files: {}\n", summary.profiled_files));
    out.push_str(&format!("failed_files: {}\n", summary.failed_files));
    out.push_str(&format!(
        "total_size_megabytes: {}\n",
        bytes_to_megabytes(summary.total_size_bytes)
    ));
    out.push_str(&format!("total_rows: {}\n", summary.total_rows));
    out.push_str(&format!("total_columns: {}\n", summary.total_columns));
    out.push_str(&format!(
        "total_string_columns: {}\n",
        summary.total_string_columns
    ));
    out.push_str(&format!(
        "total_numeric_like_columns: {}\n",
        summary.total_numeric_like_columns
    ));
    out.push_str(&format!(
        "total_sampled_string_cells: {}\n",
        summary.total_sampled_string_cells
    ));
    out.push_str(&format!(
        "total_sampled_empty_string_cells: {}\n",
        summary.total_sampled_empty_string_cells
    ));
    out.push_str(&format!(
        "total_sampled_ascii_string_cells: {}\n\n",
        summary.total_sampled_ascii_string_cells
    ));
    out.push_str(&format!(
        "compression_counts: {}\n",
        join_map(&summary.compression_counts)
    ));
    out.push_str(&format!(
        "encoding_counts: {}\n",
        join_map(&summary.encoding_counts)
    ));
    out.push_str(&format!(
        "tag_counts: {}\n\n",
        join_map(&summary.tag_counts)
    ));
    out.push_str(&format!(
        "top_by_size_megabytes: {}\n",
        join_ranked_files_megabytes(&summary.top_by_size_bytes)
    ));
    out.push_str(&format!(
        "top_by_row_count: {}\n",
        join_ranked_files(&summary.top_by_row_count)
    ));
    out.push_str(&format!(
        "top_by_column_count: {}\n",
        join_ranked_files(&summary.top_by_column_count)
    ));
    out.push_str(&format!(
        "top_by_string_columns: {}\n",
        join_ranked_files(&summary.top_by_string_columns)
    ));
    out
}

fn top_n(mut ranked: Vec<RankedFile>, n: usize) -> Vec<RankedFile> {
    ranked.sort_by(|left, right| {
        right
            .value
            .cmp(&left.value)
            .then_with(|| left.path.cmp(&right.path))
    });
    ranked.truncate(n);
    ranked
}

fn write_scan_profile(
    paths: &[PathBuf],
    roots: &[String],
    out: Option<PathBuf>,
    options: ScanRunOptions,
) -> std::result::Result<(), String> {
    let context = ScanCsvContext {
        roots: roots.join("|"),
        mode: options.mode.as_str().to_owned(),
        projection: projection_name(options.projection).to_owned(),
        io_backend: io_backend_name(options.io_backend).to_owned(),
        batch_rows: options.batch_rows,
        limit: options
            .limit
            .map_or_else(String::new, |value| value.to_string()),
    };

    let results = collect_scan_rows(paths, &context, options);
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
            render_scan_summary_txt(&summary, roots, &context),
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
        write_csv(
            catalog,
            roots.to_vec(),
            0,
            structural_summary,
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

fn build_scan_csv_row(
    path: &Path,
    context: &ScanCsvContext,
    options: ScanRunOptions,
    file_progress: Option<FileProgressReporter>,
) -> ScanProfileResult {
    let path_string = path.display().to_string();
    let file_name = path
        .file_name()
        .map_or_else(String::new, |value| value.to_string_lossy().into_owned());
    let source_group = source_group(path);
    let size_bytes = fs::metadata(path).map_or(0, |meta| meta.len());

    let mut row = CorpusScanCsvRow {
        corpus_roots: context.roots.clone(),
        corpus_mode: context.mode.clone(),
        corpus_projection: context.projection.clone(),
        corpus_io_backend: context.io_backend.clone(),
        corpus_batch_rows: context.batch_rows,
        corpus_limit: context.limit.clone(),
        path: path_string,
        file_name,
        source_group,
        status: "error".to_owned(),
        error: String::new(),
        size_megabytes: bytes_to_megabytes(size_bytes),
        table_name: String::new(),
        encoding: String::new(),
        compression: String::new(),
        row_count: 0,
        column_count: 0,
        row_len: 0,
        page_size: 0,
        page_count: 0,
        string_columns: 0,
        integer_columns: 0,
        float_columns: 0,
        date_columns: 0,
        datetime_columns: 0,
        time_columns: 0,
        bytes_columns: 0,
        numeric_like_columns: 0,
        string_width_sum: 0,
        string_width_max: 0,
        numeric_width_sum: 0,
        numeric_width_max: 0,
        date_format_columns: 0,
        datetime_format_columns: 0,
        time_format_columns: 0,
        date_formats: String::new(),
        datetime_formats: String::new(),
        time_formats: String::new(),
        projected_columns: 0,
        projected_string_columns: 0,
        projected_numeric_like_columns: 0,
        projected_physical_width_sum: 0,
        projected_string_width_sum: 0,
        projected_numeric_width_sum: 0,
        compression_class: "unknown".to_owned(),
        encoding_class: "unknown".to_owned(),
        size_class: "unknown".to_owned(),
        width_class: "unknown".to_owned(),
        content_class: "unknown".to_owned(),
        elapsed_ns: 0,
        rows_per_second: 0.0,
        bytes_per_second: 0.0,
        rows_seen: 0,
        rows_emitted: 0,
        pages_seen: 0,
        fused_pages: 0,
        indexed_pages: 0,
        compressed_pages: 0,
        raw_bytes_read: 0,
        row_bytes_materialized: 0,
        decode_batches: 0,
        pages_per_second: 0.0,
        rows_per_page: 0.0,
        raw_bytes_per_row: 0.0,
        materialized_bytes_per_row: 0.0,
        materialization_ratio: 0.0,
    };

    let ds = match Dataset::open_with(
        path,
        OpenOptions {
            io_backend: options.io_backend,
            ..OpenOptions::default()
        },
    ) {
        Ok(ds) => ds,
        Err(err) => {
            row.error = err.to_string();
            let error = row.error.clone();
            let file_name = row.file_name.clone();
            let source_group = row.source_group.clone();
            return ScanProfileResult {
                scan_row: row,
                structural_entry: FixtureEntry {
                    path: path.display().to_string(),
                    file_name,
                    source_group,
                    size_bytes,
                    status: FixtureStatus::Error { error },
                },
            };
        }
    };

    row.status = "profiled".to_owned();
    row.table_name = ds.metadata().table_name.clone().unwrap_or_default();
    row.encoding = ds.metadata().encoding.clone().unwrap_or_default();
    row.compression = compression_name(ds.metadata().compression).to_owned();
    row.row_count = ds.metadata().row_count;
    row.column_count = ds.columns().len();
    row.row_len = ds.metadata().row_len;
    row.page_size = ds.metadata().page_size;
    row.page_count = ds.metadata().page_count;

    let logical_types = logical_type_counts_for_scan(&ds);
    let widths = width_summary_for_scan(&ds);
    let temporal_formats = temporal_format_summary_for_scan(&ds);
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
    row.compression_class = row.compression.clone();
    row.encoding_class = encoding_class_from_name(&row.encoding);
    row.size_class = size_class_from_page(row.page_size, row.page_count);
    row.width_class = width_class_from_shape(row.column_count, row.row_len);
    row.content_class = content_class_from_counts(
        logical_types.string + logical_types.bytes,
        row.numeric_like_columns,
        row.column_count,
    );

    let projection_obj = build_projection(&ds, options.projection);
    let projected_shape = projected_scan_shape(&ds, projection_obj.as_ref());
    row.projected_columns = projected_shape.projected_columns;
    row.projected_string_columns = projected_shape.projected_string_columns;
    row.projected_numeric_like_columns = projected_shape.projected_numeric_like_columns;
    row.projected_physical_width_sum = projected_shape.projected_physical_width_sum;
    row.projected_string_width_sum = projected_shape.projected_string_width_sum;
    row.projected_numeric_width_sum = projected_shape.projected_numeric_width_sum;
    let mut full_content = FullContentAccumulator::new(&ds);
    let start = Instant::now();
    let stats = match run_scan(
        &ds,
        options.mode,
        projection_obj.as_ref(),
        options.batch_rows,
        options.limit,
        file_progress.clone(),
        Some(&mut |_, bytes| full_content.observe_row(bytes)),
    ) {
        Ok(stats) => stats,
        Err(err) => {
            row.status = "error".to_owned();
            row.error = err.to_string();
            row.compression_class = "unknown".to_owned();
            row.encoding_class = "unknown".to_owned();
            row.size_class = "unknown".to_owned();
            row.width_class = "unknown".to_owned();
            row.content_class = "unknown".to_owned();
            if let Some(file_progress) = &file_progress {
                file_progress.finish();
            }
            let error = row.error.clone();
            let file_name = row.file_name.clone();
            let source_group = row.source_group.clone();
            return ScanProfileResult {
                scan_row: row,
                structural_entry: FixtureEntry {
                    path: path.display().to_string(),
                    file_name,
                    source_group,
                    size_bytes,
                    status: FixtureStatus::Error { error },
                },
            };
        }
    };
    if let Some(file_progress) = &file_progress {
        file_progress.finish();
    }
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

type RowTap<'a> = Option<&'a mut dyn FnMut(u64, &[u8])>;

fn run_scan(
    ds: &Dataset,
    mode: ProfileMode,
    projection: Option<&Projection>,
    batch_rows: usize,
    limit: Option<u64>,
    file_progress: Option<FileProgressReporter>,
    mut row_tap: RowTap<'_>,
) -> sas7bdat_simd::Result<sas7bdat_simd::ScanStats> {
    let mut scan = ds.scan().with_decode_mode(mode.decode_mode());
    if let Some(projection) = projection {
        scan = scan.with_projection(projection);
    }
    if let Some(limit) = limit {
        scan = scan.limit(limit);
    }
    if mode.is_batch() {
        scan = scan.with_batch_hint(BatchHint::Rows(batch_rows));
    }
    if let Some(file_progress) = file_progress {
        scan = scan.with_progress(move |progress| file_progress.on_scan_progress(progress));
    }

    match (mode, row_tap.as_mut()) {
        (ProfileMode::RawRows, Some(tap)) => scan
            .with_decode_mode(DecodeMode::Raw)
            .visit_raw_rows_with_tap(|_| Ok(std::ops::ControlFlow::Continue(())), tap),
        (ProfileMode::RawRows, None) => scan
            .with_decode_mode(DecodeMode::Raw)
            .visit_raw_rows(|_| Ok(std::ops::ControlFlow::Continue(()))),
        (ProfileMode::TypedRows | ProfileMode::TypedLosslessRows, Some(tap)) => {
            scan.visit_rows_with_tap(|_| Ok(std::ops::ControlFlow::Continue(())), tap)
        }
        (ProfileMode::TypedRows | ProfileMode::TypedLosslessRows, None) => {
            scan.visit_rows(|_| Ok(std::ops::ControlFlow::Continue(())))
        }
        (ProfileMode::TypedBatches | ProfileMode::TypedLosslessBatches, Some(tap)) => {
            scan.visit_batches_with_tap(|_| Ok(std::ops::ControlFlow::Continue(())), tap)
        }
        (ProfileMode::TypedBatches | ProfileMode::TypedLosslessBatches, None) => {
            scan.visit_batches(|_| Ok(std::ops::ControlFlow::Continue(())))
        }
    }
}

const fn apply_scan_stats(row: &mut CorpusScanCsvRow, stats: ScanStatsSummary) {
    row.rows_seen = stats.rows_seen;
    row.rows_emitted = stats.rows_emitted;
    row.pages_seen = stats.pages_seen;
    row.fused_pages = stats.fused_pages;
    row.indexed_pages = stats.indexed_pages;
    row.compressed_pages = stats.compressed_pages;
    row.raw_bytes_read = stats.raw_bytes_read;
    row.row_bytes_materialized = stats.row_bytes_materialized;
    row.decode_batches = stats.decode_batches;
}

fn render_scan_summary_txt(
    summary: &ScanSummary,
    roots: &[String],
    context: &ScanCsvContext,
) -> String {
    let mut out = String::new();
    out.push_str("Corpus Scan Profile Summary\n");
    out.push_str("==========================\n\n");
    out.push_str(&format!("roots: {}\n", roots.join(", ")));
    out.push_str(&format!("mode: {}\n", context.mode));
    out.push_str(&format!("projection: {}\n", context.projection));
    out.push_str(&format!("io_backend: {}\n", context.io_backend));
    out.push_str(&format!("batch_rows: {}\n", context.batch_rows));
    out.push_str(&format!(
        "limit: {}\n\n",
        if context.limit.is_empty() {
            "all"
        } else {
            &context.limit
        }
    ));
    out.push_str(&format!("discovered_files: {}\n", summary.discovered_files));
    out.push_str(&format!("profiled_files: {}\n", summary.profiled_files));
    out.push_str(&format!("failed_files: {}\n", summary.failed_files));
    out.push_str(&format!("total_elapsed_ns: {}\n", summary.total_elapsed_ns));
    out.push_str(&format!(
        "total_rows_emitted: {}\n",
        summary.total_rows_emitted
    ));
    out.push_str(&format!(
        "total_raw_bytes_read: {}\n",
        summary.total_raw_bytes_read
    ));
    out.push_str(&format!(
        "total_row_bytes_materialized: {}\n",
        summary.total_row_bytes_materialized
    ));
    out.push_str(&format!("total_pages_seen: {}\n", summary.total_pages_seen));
    out.push_str(&format!(
        "total_compressed_pages: {}\n\n",
        summary.total_compressed_pages
    ));
    out.push_str(&format!(
        "slowest_by_elapsed: {}\n",
        join_scan_ranked_files(&summary.slowest_by_elapsed)
    ));
    out.push_str(&format!(
        "largest_by_raw_bytes: {}\n",
        join_scan_ranked_files(&summary.largest_by_raw_bytes)
    ));
    out
}

fn join_scan_ranked_files(files: &[ScanRankedFile]) -> String {
    files
        .iter()
        .map(|file| format!("{}:{}@{}", file.file_name, file.value, file.path))
        .collect::<Vec<_>>()
        .join("|")
}

fn top_scan_ranked(mut ranked: Vec<ScanRankedFile>, n: usize) -> Vec<ScanRankedFile> {
    ranked.sort_by(|left, right| {
        right
            .value
            .cmp(&left.value)
            .then_with(|| left.path.cmp(&right.path))
    });
    ranked.truncate(n);
    ranked
}

fn logical_type_counts_for_scan(ds: &Dataset) -> LogicalTypeCounts {
    let mut counts = LogicalTypeCounts::default();
    for column in ds.columns() {
        match column.logical_type {
            sas7bdat_simd::LogicalType::String => counts.string += 1,
            sas7bdat_simd::LogicalType::Integer => counts.integer += 1,
            sas7bdat_simd::LogicalType::Float => counts.float += 1,
            sas7bdat_simd::LogicalType::Date => counts.date += 1,
            sas7bdat_simd::LogicalType::DateTime => counts.datetime += 1,
            sas7bdat_simd::LogicalType::Time => counts.time += 1,
            sas7bdat_simd::LogicalType::Bytes => counts.bytes += 1,
        }
    }
    counts
}

fn width_summary_for_scan(ds: &Dataset) -> WidthSummary {
    let mut widths = WidthSummary::default();
    for column in ds.columns() {
        match column.logical_type {
            sas7bdat_simd::LogicalType::String | sas7bdat_simd::LogicalType::Bytes => {
                widths.string_width_sum += u64::from(column.physical_width);
                widths.string_width_max = widths.string_width_max.max(column.physical_width);
            }
            sas7bdat_simd::LogicalType::Integer
            | sas7bdat_simd::LogicalType::Float
            | sas7bdat_simd::LogicalType::Date
            | sas7bdat_simd::LogicalType::DateTime
            | sas7bdat_simd::LogicalType::Time => {
                widths.numeric_width_sum += u64::from(column.physical_width);
                widths.numeric_width_max = widths.numeric_width_max.max(column.physical_width);
            }
        }
    }
    widths
}

fn projected_scan_shape(ds: &Dataset, projection: Option<&Projection>) -> ProjectedScanShape {
    let mut shape = ProjectedScanShape::default();
    match projection {
        Some(projection) => {
            for projected in projection.columns() {
                let column = &ds.columns()[projected.index];
                accumulate_projected_column(&mut shape, column.logical_type, column.physical_width);
            }
        }
        None => {
            for column in ds.columns() {
                accumulate_projected_column(&mut shape, column.logical_type, column.physical_width);
            }
        }
    }
    shape
}

fn accumulate_projected_column(
    shape: &mut ProjectedScanShape,
    logical_type: sas7bdat_simd::LogicalType,
    physical_width: u32,
) {
    shape.projected_columns += 1;
    shape.projected_physical_width_sum += u64::from(physical_width);
    match logical_type {
        sas7bdat_simd::LogicalType::String | sas7bdat_simd::LogicalType::Bytes => {
            shape.projected_string_columns += 1;
            shape.projected_string_width_sum += u64::from(physical_width);
        }
        sas7bdat_simd::LogicalType::Integer
        | sas7bdat_simd::LogicalType::Float
        | sas7bdat_simd::LogicalType::Date
        | sas7bdat_simd::LogicalType::DateTime
        | sas7bdat_simd::LogicalType::Time => {
            shape.projected_numeric_like_columns += 1;
            shape.projected_numeric_width_sum += u64::from(physical_width);
        }
    }
}

fn temporal_format_summary_for_scan(
    ds: &Dataset,
) -> sas7bdat_simd::fixture_catalog::TemporalFormatSummary {
    let mut summary = sas7bdat_simd::fixture_catalog::TemporalFormatSummary::default();
    let mut date_formats = BTreeMap::<String, usize>::new();
    let mut datetime_formats = BTreeMap::<String, usize>::new();
    let mut time_formats = BTreeMap::<String, usize>::new();

    for column in ds.columns() {
        let Some(format) = column.format.as_deref() else {
            continue;
        };
        let cleaned = format.trim();
        if cleaned.is_empty() {
            continue;
        }
        match column.logical_type {
            sas7bdat_simd::LogicalType::Date => {
                summary.date_format_columns += 1;
                *date_formats.entry(cleaned.to_owned()).or_default() += 1;
            }
            sas7bdat_simd::LogicalType::DateTime => {
                summary.datetime_format_columns += 1;
                *datetime_formats.entry(cleaned.to_owned()).or_default() += 1;
            }
            sas7bdat_simd::LogicalType::Time => {
                summary.time_format_columns += 1;
                *time_formats.entry(cleaned.to_owned()).or_default() += 1;
            }
            _ => {}
        }
    }

    summary.date_formats = named_counts(date_formats);
    summary.datetime_formats = named_counts(datetime_formats);
    summary.time_formats = named_counts(time_formats);
    summary
}

fn named_counts(values: BTreeMap<String, usize>) -> Vec<NamedCount> {
    let mut values: Vec<NamedCount> = values
        .into_iter()
        .map(|(name, count)| NamedCount { name, count })
        .collect();
    values.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.name.cmp(&right.name))
    });
    values
}

const fn compression_name(value: sas7bdat_simd::CompressionKind) -> &'static str {
    match value {
        sas7bdat_simd::CompressionKind::None => "uncompressed",
        sas7bdat_simd::CompressionKind::Row => "compressed",
        sas7bdat_simd::CompressionKind::Binary => "compressed-binary",
        sas7bdat_simd::CompressionKind::Unknown => "compressed-unknown",
    }
}

fn encoding_class_from_name(encoding: &str) -> String {
    if encoding.is_empty() {
        "unknown".to_owned()
    } else if encoding == "UTF-8" {
        "utf8".to_owned()
    } else {
        "legacy".to_owned()
    }
}

fn size_class_from_page(page_size: u32, page_count: u64) -> String {
    let page_bytes = u64::from(page_size).saturating_mul(page_count);
    match page_bytes {
        0..=1_048_575 => "tiny".to_owned(),
        1_048_576..=16_777_215 => "small".to_owned(),
        16_777_216..=268_435_455 => "medium".to_owned(),
        268_435_456..=1_073_741_823 => "large".to_owned(),
        _ => "huge".to_owned(),
    }
}

fn width_class_from_shape(column_count: usize, row_len: u32) -> String {
    if column_count > 1024 || row_len > 4096 {
        "ultra-wide".to_owned()
    } else if column_count > 64 || row_len > 256 {
        "wide".to_owned()
    } else if column_count > 16 || row_len > 64 {
        "medium".to_owned()
    } else {
        "narrow".to_owned()
    }
}

fn content_class_from_counts(
    string_like: usize,
    numeric_like: usize,
    total_columns: usize,
) -> String {
    if total_columns == 0 {
        return "empty".to_owned();
    }
    if string_like as f64 / total_columns as f64 >= 0.7 {
        "string-heavy".to_owned()
    } else if numeric_like as f64 / total_columns as f64 >= 0.7 {
        "numeric-heavy".to_owned()
    } else {
        "mixed".to_owned()
    }
}

fn parse_io_backend(value: &str) -> Option<IoBackendPreference> {
    match value {
        "auto" => Some(IoBackendPreference::Auto),
        "mmap-preferred" => Some(IoBackendPreference::MmapPreferred),
        "buffered-preferred" => Some(IoBackendPreference::BufferedPreferred),
        "buffered-only" => Some(IoBackendPreference::BufferedOnly),
        _ => None,
    }
}

const fn io_backend_name(value: IoBackendPreference) -> &'static str {
    match value {
        IoBackendPreference::Auto => "auto",
        IoBackendPreference::MmapPreferred => "mmap-preferred",
        IoBackendPreference::BufferedPreferred => "buffered-preferred",
        IoBackendPreference::BufferedOnly => "buffered-only",
    }
}

const fn projection_name(value: ProjectionPreset) -> &'static str {
    match value {
        ProjectionPreset::Full => "full",
        ProjectionPreset::Numeric => "numeric",
        ProjectionPreset::Strings => "strings",
        ProjectionPreset::Mixed => "mixed",
    }
}

fn print_usage() {
    eprintln!(
        "usage: cargo run -p sas7bdat-profiler --bin corpus_profile -- INPUT [INPUT ...] [--failed-from PATH] [--sample-rows N] [--format json|csv] [--summary-only] [--out PATH] [--scan-mode raw_rows|typed_rows|typed_lossless_rows|typed_batches|typed_lossless_batches] [--scan-projection full|numeric|strings|mixed] [--scan-batch-rows N] [--scan-io-backend auto|mmap-preferred|buffered-preferred|buffered-only] [--scan-limit N]"
    );
}
