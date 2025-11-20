use std::fs::File;

use std::path::{Path, PathBuf};

use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use rayon::prelude::*;
use walkdir::WalkDir;

use sas7bdat_parser_rs::metadata::DatasetMetadata;
use sas7bdat_parser_rs::parser::ColumnInfo;
use sas7bdat_parser_rs::value::Value;
use sas7bdat_parser_rs::{ColumnarSink, CsvSink, ParquetSink, RowSink, SasFile};

#[derive(Parser)]
#[command(
    name = "sas7bd",
    version,
    about = "Batch convert SAS7BDAT to Parquet/CSV/TSV"
)]
struct Cli {
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

fn main() -> Result<(), AnyError> {
    let cli = Cli::parse();

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
