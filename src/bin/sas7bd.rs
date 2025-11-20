#[cfg(feature = "hotpath")]
use std::cmp::Ordering;
#[cfg(feature = "hotpath")]
use std::fs;
use std::fs::File;
#[cfg(feature = "hotpath")]
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};
use rayon::prelude::*;
use walkdir::WalkDir;

use sas7bdat_parser_rs::metadata::DatasetMetadata;
use sas7bdat_parser_rs::parser::ColumnInfo;
use sas7bdat_parser_rs::value::Value;
use sas7bdat_parser_rs::{ColumnarSink, CsvSink, ParquetSink, RowSink, SasFile};

#[cfg(feature = "hotpath")]
use hotpath::{
    GuardBuilder, MetricType, MetricsJson, MetricsProvider, Reporter, shorten_function_name,
};
#[cfg(feature = "hotpath")]
use time::{OffsetDateTime, macros::format_description};

#[cfg(feature = "hotpath")]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum HotpathOutputFormat {
    Table,
    Json,
    #[value(name = "json-pretty")]
    JsonPretty,
    Csv,
}

#[cfg(feature = "hotpath")]
impl HotpathOutputFormat {
    fn label(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Json => "json",
            Self::JsonPretty => "json-pretty",
            Self::Csv => "csv",
        }
    }

    fn extension(self) -> &'static str {
        match self {
            Self::Table => "txt",
            Self::Json | Self::JsonPretty => "json",
            Self::Csv => "csv",
        }
    }

    fn persistable(&self) -> bool {
        !matches!(self, Self::Table)
    }
}

#[cfg(feature = "hotpath")]
#[derive(Args, Clone, Debug)]
struct HotpathArgs {
    /// Directory to write profiling runs (creates timestamped files).
    #[arg(long, env = "HOTPATH_OUT")]
    hotpath_out: Option<PathBuf>,

    /// Persist profiling output (comma-separated: json,csv,json-pretty).
    #[arg(long, env = "HOTPATH_SAVE", value_enum, value_delimiter = ',')]
    hotpath_save: Vec<HotpathOutputFormat>,

    /// Format to print to stdout (table,json,json-pretty,csv).
    #[arg(
        long,
        env = "HOTPATH_STDOUT",
        value_enum,
        default_value_t = HotpathOutputFormat::Table
    )]
    hotpath_stdout: HotpathOutputFormat,

    /// Optional tag appended to output filenames (e.g., dataset or change note).
    #[arg(long, env = "HOTPATH_TAG")]
    hotpath_tag: Option<String>,
}

#[derive(Parser)]
#[command(
    name = "sas7bd",
    version,
    about = "Batch convert SAS7BDAT to Parquet/CSV/TSV"
)]
struct Cli {
    #[cfg(feature = "hotpath")]
    #[command(flatten)]
    hotpath: HotpathArgs,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Convert one or more inputs to a chosen sink format.
    Convert(Box<ConvertArgs>),
    /// Inspect dataset metadata and print a summary.
    Inspect(InspectArgs),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum SinkKind {
    Parquet,
    Csv,
    Tsv,
}

#[derive(Parser, Clone)]
#[allow(clippy::struct_excessive_bools)]
struct ConvertArgs {
    /// Input files or directories (recurses directories).
    #[arg(required = true)]
    inputs: Vec<PathBuf>,

    /// Output directory (computed file names).
    #[arg(long, conflicts_with = "out")]
    out_dir: Option<PathBuf>,

    /// Output file (only valid with a single input).
    #[arg(long, conflicts_with = "out_dir")]
    out: Option<PathBuf>,

    /// Sink kind: parquet, csv, or tsv.
    #[arg(long, value_enum, default_value_t = SinkKind::Parquet)]
    sink: SinkKind,

    /// CSV/TSV delimiter. Defaults to ',' for csv and '\t' for tsv.
    #[arg(long)]
    delimiter: Option<char>,

    /// Write header row (CSV/TSV only).
    #[arg(long = "headers", action = ArgAction::SetTrue, default_value_t = true)]
    headers: bool,
    /// Disable header row (CSV/TSV only).
    #[arg(long = "no-headers", action = ArgAction::SetFalse, overrides_with = "headers")]
    _no_headers: bool,

    /// Skip leading N rows.
    #[arg(long)]
    skip: Option<u64>,

    /// Limit to at most N rows.
    #[arg(long = "max-rows")]
    max_rows: Option<u64>,

    /// Project a subset of columns by name (comma-separated).
    #[arg(long = "columns", value_delimiter = ',')]
    columns: Option<Vec<String>>,

    /// Project a subset of columns by zero-based indices (comma-separated).
    #[arg(long = "column-indices", value_delimiter = ',')]
    column_indices: Option<Vec<usize>>,

    /// Optional value-label catalog (.sas7bcat) to load.
    #[arg(long)]
    catalog: Option<PathBuf>,

    /// Parquet row group size (rows). If unset, uses the library's heuristic.
    #[arg(long)]
    parquet_row_group_size: Option<usize>,

    /// Parquet target row group size (bytes) for automatic estimation.
    #[arg(long)]
    parquet_target_bytes: Option<usize>,

    /// Number of concurrent worker threads.
    #[arg(long)]
    jobs: Option<usize>,

    /// Stop on first error.
    #[arg(long)]
    fail_fast: bool,

    /// Use the columnar decoding pipeline (Parquet sink only).
    #[arg(long)]
    columnar: bool,

    /// Override the columnar batch size (rows) when `--columnar` is set.
    #[arg(long, requires = "columnar")]
    columnar_batch_rows: Option<usize>,

    /// Deprecated: columnar mode always uses contiguous staging now.
    #[arg(long, requires = "columnar", hide = true)]
    columnar_staging: bool,

    /// Experimental: decode SAS pages into column-major buffers directly.
    #[arg(long, requires = "columnar")]
    columnar_column_major: bool,
}

const DEFAULT_COLUMNAR_BATCH_ROWS: usize = 4096;
const COLUMNAR_ROW_GROUP_MULTIPLIER: usize = 16;

#[derive(Parser, Clone)]
struct InspectArgs {
    input: PathBuf,
    /// Emit JSON instead of human readable output.
    #[arg(long)]
    json: bool,
}

type AnyError = Box<dyn std::error::Error + Send + Sync>;
type ProjectionResult = (
    Option<Vec<usize>>,
    Vec<usize>,
    DatasetMetadata,
    Vec<ColumnInfo>,
);

#[cfg(feature = "hotpath")]
#[derive(Clone, Debug)]
struct HotpathRuntimeConfig {
    stdout_format: HotpathOutputFormat,
    save_formats: Vec<HotpathOutputFormat>,
    out_dir: Option<PathBuf>,
    tag: Option<String>,
}

#[cfg(feature = "hotpath")]
impl HotpathRuntimeConfig {
    fn from_args(args: &HotpathArgs) -> Self {
        let mut save_formats = args.hotpath_save.clone();
        if save_formats.is_empty() && args.hotpath_out.is_some() {
            save_formats = vec![HotpathOutputFormat::Json, HotpathOutputFormat::Csv];
        }
        save_formats.retain(HotpathOutputFormat::persistable);
        save_formats.sort_unstable();
        save_formats.dedup();

        Self {
            stdout_format: args.hotpath_stdout,
            save_formats,
            out_dir: args.hotpath_out.clone(),
            tag: sanitize_tag(args.hotpath_tag.as_deref()),
        }
    }

    fn base_filename(&self, caller_name: &'static str) -> String {
        let timestamp = OffsetDateTime::now_utc();
        let fmt = format_description!("[year][month][day]-[hour][minute][second]");
        let ts = timestamp
            .format(&fmt)
            .unwrap_or_else(|_| "unknown-time".to_string());

        if let Some(tag) = &self.tag {
            format!("hotpath-{caller_name}-{ts}-{tag}")
        } else {
            format!("hotpath-{caller_name}-{ts}")
        }
    }

    fn build_reporter(&self, caller_name: &'static str) -> Result<Box<dyn Reporter>, AnyError> {
        let mut reporters: Vec<Box<dyn Reporter>> = vec![Box::new(StdoutReporter {
            format: self.stdout_format,
        })];

        if let Some(out_dir) = &self.out_dir {
            if self.save_formats.is_empty() {
                eprintln!(
                    "HOTPATH_OUT set to {} but no HOTPATH_SAVE formats selected; skipping file output.",
                    out_dir.display()
                );
            } else {
                fs::create_dir_all(out_dir)?;
                let base = self.base_filename(caller_name);
                for format in &self.save_formats {
                    let path =
                        out_dir.join(format!("{base}-{}.{}", format.label(), format.extension()));
                    match format {
                        HotpathOutputFormat::Json => reporters.push(Box::new(JsonFileReporter {
                            path,
                            pretty: false,
                        })),
                        HotpathOutputFormat::JsonPretty => {
                            reporters.push(Box::new(JsonFileReporter { path, pretty: true }))
                        }
                        HotpathOutputFormat::Csv => {
                            reporters.push(Box::new(CsvFileReporter { path }))
                        }
                        HotpathOutputFormat::Table => {}
                    }
                }
            }
        }

        Ok(if reporters.len() == 1 {
            reporters.remove(0)
        } else {
            Box::new(CompositeReporter { reporters })
        })
    }
}

#[cfg(feature = "hotpath")]
fn sanitize_tag(tag: Option<&str>) -> Option<String> {
    let tag = tag?;
    let filtered: String = tag
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '-' || *ch == '_')
        .collect();
    if filtered.is_empty() {
        None
    } else {
        Some(filtered)
    }
}

#[cfg(feature = "hotpath")]
fn build_hotpath_guard(cli: &Cli) -> Result<hotpath::HotPath, AnyError> {
    let config = HotpathRuntimeConfig::from_args(&cli.hotpath);
    let reporter = config.build_reporter("sas7bd")?;

    Ok(GuardBuilder::new("sas7bd")
        .limit(20)
        .reporter(reporter)
        .build())
}

#[cfg(feature = "hotpath")]
struct CompositeReporter {
    reporters: Vec<Box<dyn Reporter>>,
}

#[cfg(feature = "hotpath")]
impl Reporter for CompositeReporter {
    fn report(
        &self,
        metrics_provider: &dyn MetricsProvider<'_>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for reporter in &self.reporters {
            reporter.report(metrics_provider)?;
        }
        Ok(())
    }
}

#[cfg(feature = "hotpath")]
struct JsonFileReporter {
    path: PathBuf,
    pretty: bool,
}

#[cfg(feature = "hotpath")]
impl Reporter for JsonFileReporter {
    fn report(
        &self,
        metrics_provider: &dyn MetricsProvider<'_>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let metrics = MetricsJson::from(metrics_provider);
        let json = if self.pretty {
            serde_json::to_string_pretty(&metrics)?
        } else {
            serde_json::to_string(&metrics)?
        };
        fs::write(&self.path, json)?;
        Ok(())
    }
}

#[cfg(feature = "hotpath")]
struct CsvFileReporter {
    path: PathBuf,
}

#[cfg(feature = "hotpath")]
impl Reporter for CsvFileReporter {
    fn report(
        &self,
        metrics_provider: &dyn MetricsProvider<'_>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::create(&self.path)?;
        write_csv_report(metrics_provider, file)?;
        Ok(())
    }
}

#[cfg(feature = "hotpath")]
struct StdoutReporter {
    format: HotpathOutputFormat,
}

#[cfg(feature = "hotpath")]
impl Reporter for StdoutReporter {
    fn report(
        &self,
        metrics_provider: &dyn MetricsProvider<'_>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self.format {
            HotpathOutputFormat::Table => print_table_report(metrics_provider)?,
            HotpathOutputFormat::Json => {
                let metrics = MetricsJson::from(metrics_provider);
                println!("{}", serde_json::to_string(&metrics)?);
            }
            HotpathOutputFormat::JsonPretty => {
                let metrics = MetricsJson::from(metrics_provider);
                println!("{}", serde_json::to_string_pretty(&metrics)?);
            }
            HotpathOutputFormat::Csv => {
                let stdout = io::stdout();
                let handle = stdout.lock();
                write_csv_report(metrics_provider, handle)?;
            }
        }
        Ok(())
    }
}

#[cfg(feature = "hotpath")]
fn header_key(header: &str) -> String {
    header
        .to_lowercase()
        .replace(' ', "_")
        .replace('%', "percent")
}

#[cfg(feature = "hotpath")]
fn metric_to_cell(metric: &MetricType) -> String {
    match metric {
        MetricType::CallsCount(value) => value.to_string(),
        MetricType::DurationNs(ns) => ns.to_string(),
        MetricType::AllocBytes(bytes) => bytes.to_string(),
        MetricType::AllocCount(count) => count.to_string(),
        MetricType::Percentage(basis_points) => basis_points.to_string(),
        MetricType::Unsupported => String::new(),
    }
}

#[cfg(feature = "hotpath")]
fn sorted_entries(metrics_provider: &dyn MetricsProvider<'_>) -> Vec<(String, Vec<MetricType>)> {
    let mut entries: Vec<(String, Vec<MetricType>)> =
        metrics_provider.metric_data().into_iter().collect();
    entries.sort_by(|(_, left_metrics), (_, right_metrics)| {
        metrics_provider
            .sort_key(right_metrics)
            .partial_cmp(&metrics_provider.sort_key(left_metrics))
            .unwrap_or(Ordering::Equal)
    });
    entries
}

#[cfg(feature = "hotpath")]
fn write_csv_report(
    metrics_provider: &dyn MetricsProvider<'_>,
    writer: impl Write,
) -> Result<(), Box<dyn std::error::Error>> {
    let headers = metrics_provider.headers();
    let mut csv_writer = csv::Writer::from_writer(writer);
    csv_writer.write_record(
        headers
            .iter()
            .map(|header| header_key(header))
            .collect::<Vec<_>>(),
    )?;

    for (function, metrics) in sorted_entries(metrics_provider) {
        let mut row = Vec::with_capacity(headers.len());
        row.push(function);
        for metric in metrics {
            row.push(metric_to_cell(&metric));
        }
        csv_writer.write_record(row)?;
    }
    csv_writer.flush()?;
    Ok(())
}

#[cfg(feature = "hotpath")]
fn print_separator(widths: &[usize]) {
    let mut line = String::from('+');
    for width in widths {
        line.push_str(&"-".repeat(*width + 2));
        line.push('+');
    }
    println!("{line}");
}

#[cfg(feature = "hotpath")]
fn print_row(cells: &[String], widths: &[usize]) {
    let mut line = String::from('|');
    for (cell, width) in cells.iter().zip(widths) {
        line.push(' ');
        line.push_str(&format!("{cell:<width$}", cell = cell, width = *width));
        line.push(' ');
        line.push('|');
    }
    println!("{line}");
}

#[cfg(feature = "hotpath")]
fn print_table_report(
    metrics_provider: &dyn MetricsProvider<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut entries = sorted_entries(metrics_provider);
    if entries.is_empty() {
        println!(
            "[hotpath] No measurements recorded from {} (enable #[hotpath::measure]).",
            metrics_provider.caller_name()
        );
        return Ok(());
    }

    let headers = metrics_provider.headers();
    let mut widths: Vec<usize> = headers.iter().map(|header| header.len()).collect();
    let mut rows: Vec<Vec<String>> = Vec::with_capacity(entries.len());

    for (function, metrics) in entries.drain(..) {
        let mut row = Vec::with_capacity(headers.len());
        let display_name = shorten_function_name(&function);
        widths[0] = widths[0].max(display_name.len());
        row.push(display_name);

        for (idx, metric) in metrics.iter().enumerate() {
            let cell = metric.to_string();
            widths[idx + 1] = widths[idx + 1].max(cell.len());
            row.push(cell);
        }

        rows.push(row);
    }

    println!(
        "[hotpath] {} - {}",
        metrics_provider.profiling_mode(),
        metrics_provider.description()
    );

    let (displayed, total) = metrics_provider.entry_counts();
    if displayed < total {
        println!(
            "{}: {:.2?} ({}/{})",
            metrics_provider.caller_name(),
            std::time::Duration::from_nanos(metrics_provider.total_elapsed()),
            displayed,
            total
        );
    } else {
        println!(
            "{}: {:.2?}",
            metrics_provider.caller_name(),
            std::time::Duration::from_nanos(metrics_provider.total_elapsed())
        );
    }

    print_separator(&widths);
    print_row(&headers, &widths);
    print_separator(&widths);
    for row in rows {
        print_row(&row, &widths);
    }
    print_separator(&widths);

    Ok(())
}

fn main() -> Result<(), AnyError> {
    let cli = Cli::parse();

    #[cfg(feature = "hotpath")]
    let _hotpath = build_hotpath_guard(&cli)?;

    match cli.command {
        Command::Convert(args) => run_convert(&args),
        Command::Inspect(args) => run_inspect(&args),
    }
}

fn run_convert(args: &ConvertArgs) -> Result<(), AnyError> {
    if let Some(jobs) = args.jobs {
        // Best-effort: configure global rayon pool once. Ignore error if already set.
        let _ = rayon::ThreadPoolBuilder::new()
            .num_threads(jobs)
            .build_global();
    }

    if args.columnar && args.sink != SinkKind::Parquet {
        return Err("--columnar is only supported for Parquet conversions".into());
    }
    if args.columnar && (args.skip.is_some() || args.max_rows.is_some()) {
        return Err("--columnar cannot be combined with --skip or --max-rows".into());
    }

    let files = discover_inputs(&args.inputs);

    if args.out.is_some() && files.len() != 1 {
        return Err("--out requires a single input".into());
    }

    let mut tasks: Vec<(PathBuf, PathBuf)> = Vec::with_capacity(files.len());
    if let Some(ref out) = args.out {
        tasks.push((files[0].clone(), out.clone()));
    } else {
        for input in files {
            let output = compute_output_path_unchecked(&input, args);
            tasks.push((input, output));
        }
    }

    // If single input with explicit --out, keep the order (just one task).
    // Otherwise, process in parallel.
    let process = |(input, output): (PathBuf, PathBuf)| -> Result<(), AnyError> {
        convert_one(&input, &output, args)
    };

    if args.fail_fast {
        tasks
            .into_par_iter()
            .map(process)
            .collect::<Result<Vec<_>, _>>()?;
    } else {
        let results = tasks
            .into_par_iter()
            .map(|t| {
                let res = process(t);
                if let Err(ref e) = res {
                    eprintln!("error: {e}");
                }
                res
            })
            .collect::<Vec<_>>();
        let failures = results.iter().filter(|r| r.is_err()).count();
        if failures > 0 {
            eprintln!("completed with {failures} failures");
        }
    }

    Ok(())
}

fn run_inspect(args: &InspectArgs) -> Result<(), AnyError> {
    let sas = SasFile::open(&args.input)?;
    let meta = sas.metadata().clone();
    if args.json {
        #[derive(serde::Serialize)]
        struct ColumnInfoJson {
            index: u32,
            name: String,
            label: Option<String>,
            kind: &'static str,
            format: Option<String>,
            width: usize,
        }
        #[derive(serde::Serialize)]
        struct InspectJson {
            row_count: u64,
            column_count: u32,
            columns: Vec<ColumnInfoJson>,
        }
        let columns = meta
            .variables
            .iter()
            .map(|v| ColumnInfoJson {
                index: v.index,
                name: v.name.clone(),
                label: v.label.clone(),
                kind: match v.kind {
                    sas7bdat_parser_rs::metadata::VariableKind::Numeric => "numeric",
                    sas7bdat_parser_rs::metadata::VariableKind::Character => "character",
                },
                format: v.format.as_ref().map(|f| f.name.clone()),
                width: v.storage_width,
            })
            .collect();
        let payload = InspectJson {
            row_count: meta.row_count,
            column_count: meta.column_count,
            columns,
        };
        serde_json::to_writer_pretty(std::io::stdout(), &payload)?;
        println!();
    } else {
        println!(
            "Rows: {}  Columns: {}  Table: {}",
            meta.row_count,
            meta.column_count,
            meta.table_name.as_deref().unwrap_or("")
        );
        for v in &meta.variables {
            let kind = match v.kind {
                sas7bdat_parser_rs::metadata::VariableKind::Numeric => "numeric",
                sas7bdat_parser_rs::metadata::VariableKind::Character => "character",
            };
            let fmt = v
                .format
                .as_ref()
                .map(|f| f.name.trim().to_owned())
                .unwrap_or_default();
            println!(
                "[{idx:>3}] {name:<24}  {kind:<9}  width={w:<4}  fmt={fmt}",
                idx = v.index,
                name = v.name,
                kind = kind,
                w = v.storage_width,
                fmt = fmt
            );
        }
    }
    Ok(())
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn convert_one(input: &Path, output: &Path, args: &ConvertArgs) -> Result<(), AnyError> {
    // Prepare reader and metadata
    let mut sas = SasFile::open(input)?;
    if let Some(cat) = &args.catalog {
        let _ = sas.load_catalog(cat);
    }
    let (mut reader, parsed) = sas.into_parts();

    // Resolve projection
    let (indices, selection, meta_filtered, cols_filtered) =
        resolve_projection(&parsed.header.metadata, &parsed.columns, args)?;

    // Build sink
    let sink_kind = args.sink;
    let columnar_batch_rows = args
        .columnar_batch_rows
        .unwrap_or(DEFAULT_COLUMNAR_BATCH_ROWS)
        .max(1);
    let derived_row_group_rows = columnar_batch_rows
        .saturating_mul(COLUMNAR_ROW_GROUP_MULTIPLIER)
        .max(columnar_batch_rows);
    let options = StreamOptions {
        indices: indices.as_deref(),
        skip: args.skip,
        max_rows: args.max_rows,
    };
    match sink_kind {
        SinkKind::Parquet => {
            let file = File::create(output)?;
            let mut sink = ParquetSink::new(file);
            let mut columnar_row_group_rows = None;
            if let Some(rows) = args.parquet_row_group_size {
                sink = sink.with_row_group_size(rows);
                columnar_row_group_rows = Some(rows);
            } else if args.columnar {
                sink = sink.with_row_group_size(derived_row_group_rows);
                columnar_row_group_rows = Some(derived_row_group_rows);
            }
            if let Some(bytes) = args.parquet_target_bytes {
                sink = sink.with_target_row_group_bytes(bytes);
            }
            if args.columnar {
                sink = sink.with_streaming_columnar(true);
            }
            if args.columnar {
                let mode = if args.columnar_column_major {
                    ColumnarStreamMode::ColumnMajor {
                        batch_rows: columnar_row_group_rows.unwrap_or(columnar_batch_rows),
                    }
                } else {
                    ColumnarStreamMode::Contiguous {
                        batch_rows: columnar_row_group_rows.unwrap_or(columnar_batch_rows),
                    }
                };
                stream_columnar_into_sink(
                    &mut reader,
                    &parsed,
                    &meta_filtered,
                    &cols_filtered,
                    &selection,
                    &mode,
                    &mut sink,
                )?;
            } else {
                stream_into_sink(
                    &mut reader,
                    &parsed,
                    &meta_filtered,
                    &cols_filtered,
                    &options,
                    &mut sink,
                )?;
            }
            let _ = sink.into_inner()?;
        }
        SinkKind::Csv | SinkKind::Tsv => {
            let file = File::create(output)?;
            let mut sink = CsvSink::new(file)
                .with_headers(args.headers)
                .with_delimiter(match (sink_kind, args.delimiter) {
                    (SinkKind::Tsv, None) => b'\t',
                    (_, Some(ch)) => ch as u8,
                    _ => b',',
                });
            if args.columnar {
                return Err("columnar mode is only supported for Parquet sinks".into());
            }
            stream_into_sink(
                &mut reader,
                &parsed,
                &meta_filtered,
                &cols_filtered,
                &options,
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

#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn stream_into_sink<W: std::io::Read + std::io::Seek, S: RowSink>(
    reader: &mut W,
    parsed: &sas7bdat_parser_rs::parser::ParsedMetadata,
    meta_filtered: &DatasetMetadata,
    cols_filtered: &[ColumnInfo],
    options: &StreamOptions<'_>,
    sink: &mut S,
) -> Result<(), AnyError> {
    // Begin sink with filtered context
    let context = sas7bdat_parser_rs::sinks::SinkContext {
        metadata: meta_filtered,
        columns: cols_filtered,
    };
    sink.begin(context)?;

    let mut it = parsed.row_iterator(reader)?;
    let mut skipped = 0u64;
    let to_skip = options.skip.unwrap_or(0);
    let mut remaining = options.max_rows;
    let mut projected: Vec<Value<'static>> = Vec::new();

    loop {
        if options.indices.is_some() {
            projected.clear();
        }
        let Some(row) = it.try_next()? else { break };
        if skipped < to_skip {
            skipped += 1;
            continue;
        }

        if let Some(idxs) = options.indices {
            projected.reserve(idxs.len());
            for &idx in idxs {
                projected.push(row[idx].clone().into_owned());
            }
            sink.write_row(&projected)?;
        } else {
            sink.write_row(&row)?;
        }

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
    sink.finish()?;
    Ok(())
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn stream_columnar_into_sink<W: std::io::Read + std::io::Seek, S: ColumnarSink>(
    reader: &mut W,
    parsed: &sas7bdat_parser_rs::parser::ParsedMetadata,
    meta_filtered: &DatasetMetadata,
    cols_filtered: &[ColumnInfo],
    selection: &[usize],
    mode: &ColumnarStreamMode,
    sink: &mut S,
) -> Result<(), AnyError> {
    if selection.len() != cols_filtered.len() {
        return Err("column selection length mismatch".into());
    }

    let context = sas7bdat_parser_rs::sinks::SinkContext {
        metadata: meta_filtered,
        columns: cols_filtered,
    };
    sink.begin(context)?;

    let mut it = parsed.row_iterator(reader)?;
    match mode {
        ColumnarStreamMode::Contiguous { batch_rows } => {
            while let Some(batch) = it.next_columnar_batch_contiguous(*batch_rows)? {
                sink.write_columnar_batch(&batch, selection)?;
            }
        }
        ColumnarStreamMode::ColumnMajor { batch_rows } => {
            while let Some(batch) = it.next_column_major_batch(*batch_rows)? {
                sink.write_column_major_batch(&batch, selection)?;
            }
        }
    }

    sink.finish()?;
    Ok(())
}

fn resolve_projection(
    meta: &DatasetMetadata,
    cols: &[ColumnInfo],
    args: &ConvertArgs,
) -> Result<ProjectionResult, AnyError> {
    let column_count = meta.column_count as usize;
    let mut indices: Option<Vec<usize>> = None;
    if let Some(ref idxs) = args.column_indices {
        let mut seen = std::collections::HashSet::with_capacity(idxs.len());
        for &i in idxs {
            if i >= column_count {
                return Err(
                    format!("column index {i} out of range ({column_count} columns)").into(),
                );
            }
            if !seen.insert(i) {
                return Err(format!("duplicate column index {i}").into());
            }
        }
        indices = Some(idxs.clone());
    } else if let Some(ref names) = args.columns {
        // Build lookup allowing trailing-space-insensitive matching
        let mut map = std::collections::HashMap::with_capacity(meta.variables.len());
        for v in &meta.variables {
            map.entry(v.name.clone()).or_insert(v.index as usize);
            map.entry(v.name.trim_end().to_owned())
                .or_insert(v.index as usize);
        }
        let mut resolved = Vec::with_capacity(names.len());
        let mut seen = std::collections::HashSet::with_capacity(names.len());
        for name in names {
            let key = if let Some(&idx) = map.get(name) {
                idx
            } else if let Some(&idx) = map.get(name.trim_end()) {
                idx
            } else {
                return Err(format!("column '{name}' not found").into());
            };
            if !seen.insert(key) {
                return Err(format!("duplicate column '{name}' (index {key})").into());
            }
            resolved.push(key);
        }
        if resolved.is_empty() {
            return Err("projection resolved to empty set".into());
        }
        indices = Some(resolved);
    }

    let selected: Vec<usize> = indices
        .clone()
        .unwrap_or_else(|| (0..column_count).collect());

    // Filter metadata clone and columns to match the projection
    let mut filtered = meta.clone();
    filtered.column_count = u32::try_from(selected.len())
        .map_err(|_| "projected column count exceeds u32 range".to_string())?;
    let mut new_vars = Vec::with_capacity(selected.len());
    for (new_idx, &old_idx) in selected.iter().enumerate() {
        let mut v = meta.variables[old_idx].clone();
        v.index = u32::try_from(new_idx)
            .map_err(|_| "projected column index exceeds u32 range".to_string())?;
        new_vars.push(v);
    }
    filtered.variables = new_vars;
    // column_list not needed for sinks; keep as-is

    let filtered_cols: Vec<ColumnInfo> = selected.iter().map(|&i| cols[i].clone()).collect();

    Ok((indices, selected, filtered, filtered_cols))
}

fn discover_inputs(inputs: &[PathBuf]) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for input in inputs {
        if input.is_dir() {
            for entry in WalkDir::new(input)
                .follow_links(false)
                .into_iter()
                .filter_map(Result::ok)
            {
                let path = entry.path();
                if path.is_file() && is_sas7bdat(path) {
                    files.push(path.to_path_buf());
                }
            }
        } else if input.is_file() {
            if is_sas7bdat(input) {
                files.push(input.clone());
            }
        } else {
            // Non-existent paths are ignored; shell globbing typically expands patterns.
        }
    }
    files.sort();
    files.dedup();
    files
}

fn is_sas7bdat(path: &Path) -> bool {
    path.extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("sas7bdat"))
}

fn compute_output_path_unchecked(input: &Path, args: &ConvertArgs) -> PathBuf {
    use std::ffi::OsStr;
    let new_ext = match args.sink {
        SinkKind::Parquet => "parquet",
        SinkKind::Csv => "csv",
        SinkKind::Tsv => "tsv",
    };
    args.out_dir.as_ref().map_or_else(
        || input.with_extension(new_ext),
        |dir| {
            let fname = input.file_name().unwrap_or_else(|| OsStr::new("output"));
            let renamed = PathBuf::from(fname).with_extension(new_ext);
            dir.join(renamed)
        },
    )
}
enum ColumnarStreamMode {
    Contiguous { batch_rows: usize },
    ColumnMajor { batch_rows: usize },
}
