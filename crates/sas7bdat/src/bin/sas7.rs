use clap::{ArgAction, Parser, ValueEnum};
use rayon::prelude::*;
use sas7bdat::{
    CellValue, ColumnarSink, CsvSink, ParquetSink, RowSink, SasReader,
    dataset::DatasetMetadata,
    logger::{log_error, set_log_file, set_log_prefix},
    parser::ColumnInfo,
};
use std::{
    fs::File,
    path::{Path, PathBuf},
};
use walkdir::WalkDir;

#[derive(Parser)]
#[command(
    name = "sas7",
    version,
    about = "Batch convert SAS7BDAT to Parquet/CSV/TSV"
)]
struct Cli {
    /// Conversion options (default mode).
    #[command(flatten)]
    convert: ConvertArgs,

    /// Inspect dataset metadata and print a summary.
    #[arg(long, value_name = "FILE", help_heading = "Inspect")]
    inspect: Option<PathBuf>,

    /// Emit JSON instead of human readable output (inspect only).
    #[arg(long, requires = "inspect", help_heading = "Inspect")]
    inspect_json: bool,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum SinkKind {
    Parquet,
    Csv,
    Tsv,
}

#[derive(Parser, Clone)]
struct ConvertArgs {
    /// Input files or directories (recurses directories).
    #[arg(
        required_unless_present = "inspect",
        value_name = "PATH",
        help_heading = "Input"
    )]
    inputs: Vec<PathBuf>,

    #[command(flatten)]
    output: OutputOptions,

    #[command(flatten)]
    execution: ExecutionOptions,

    #[command(flatten)]
    validation: ValidationOptions,

    #[command(flatten)]
    logging: LoggingOptions,

    /// Skip leading N rows.
    #[arg(long, help_heading = "Row Limits")]
    skip: Option<u64>,

    /// Limit to at most N rows.
    #[arg(long = "max-rows", help_heading = "Row Limits")]
    max_rows: Option<u64>,

    /// Project a subset of columns by name (comma-separated).
    #[arg(
        long = "columns",
        value_delimiter = ',',
        help_heading = "Projection",
        value_name = "NAME[,NAME]"
    )]
    columns: Option<Vec<String>>,

    /// Project a subset of columns by zero-based indices (comma-separated).
    #[arg(
        long = "column-indices",
        value_delimiter = ',',
        help_heading = "Projection",
        value_name = "IDX[,IDX]"
    )]
    column_indices: Option<Vec<usize>>,

    /// Optional value-label catalog (.sas7bcat) to load.
    #[arg(long, value_name = "FILE", help_heading = "Input")]
    catalog: Option<PathBuf>,
}

#[derive(Parser, Clone)]
struct OutputOptions {
    /// Output directory (computed file names).
    #[arg(
        long,
        conflicts_with = "out",
        value_name = "DIR",
        help_heading = "Output"
    )]
    out_dir: Option<PathBuf>,

    /// Output file (only valid with a single input).
    #[arg(
        long,
        conflicts_with = "out_dir",
        value_name = "FILE",
        help_heading = "Output"
    )]
    out: Option<PathBuf>,

    /// Sink kind: parquet, csv, or tsv.
    #[arg(long, value_enum, default_value_t = SinkKind::Parquet, help_heading = "Output")]
    sink: SinkKind,

    /// CSV/TSV delimiter. Defaults to ',' for csv and '\t' for tsv.
    #[arg(long, help_heading = "Output")]
    delimiter: Option<char>,

    /// Write header row (CSV/TSV only).
    #[arg(
        long = "headers",
        action = ArgAction::SetTrue,
        default_value_t = true,
        help_heading = "Output"
    )]
    headers: bool,
    /// Disable header row (CSV/TSV only).
    #[arg(
        long = "no-headers",
        action = ArgAction::SetFalse,
        overrides_with = "headers",
        help_heading = "Output"
    )]
    _no_headers: bool,

    /// Parquet row group size (rows). If unset, uses the library's heuristic.
    #[arg(long, value_name = "ROWS", help_heading = "Parquet")]
    parquet_row_group_size: Option<usize>,

    /// Parquet target row group size (bytes) for automatic estimation.
    #[arg(long, value_name = "BYTES", help_heading = "Parquet")]
    parquet_target_bytes: Option<usize>,

    /// Flatten outputs into a single directory instead of mirroring input tree.
    #[arg(long, help_heading = "Output")]
    flatten: bool,
}

#[derive(Parser, Clone)]
struct ExecutionOptions {
    /// Number of concurrent worker threads (default: Rayon global pool, usually logical CPUs unless `RAYON_NUM_THREADS` is set).
    #[arg(long, help_heading = "Execution")]
    jobs: Option<usize>,

    /// Stop on first error.
    #[arg(long, help_heading = "Execution")]
    fail_fast: bool,
}

#[derive(Parser, Clone)]
struct ValidationOptions {
    /// Enforce strict date/time conversion (fail on out-of-range or malformed values).
    #[arg(long, help_heading = "Validation")]
    strict_dates: bool,
}

#[derive(Parser, Clone)]
struct LoggingOptions {
    /// Write warnings and errors to a log file in addition to stderr.
    #[arg(long, value_name = "FILE", help_heading = "Logging")]
    log_file: Option<PathBuf>,
}

const DEFAULT_COLUMNAR_BATCH_ROWS: usize = 4096;
const COLUMNAR_ROW_GROUP_MULTIPLIER: usize = 16;

#[derive(Parser, Clone)]
struct InspectArgs {
    input: PathBuf,
    /// Emit JSON instead of human readable output.
    #[arg(long, hide = true)]
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

    if let Some(path) = cli.inspect {
        if !cli.convert.inputs.is_empty() {
            return Err("`--inspect` cannot be combined with conversion inputs".into());
        }
        let args = InspectArgs {
            input: path,
            json: cli.inspect_json,
        };
        run_inspect(&args)
    } else {
        run_convert(&cli.convert)
    }
}

fn run_convert(args: &ConvertArgs) -> Result<(), AnyError> {
    if let Some(path) = &args.logging.log_file {
        set_log_file(path)?;
    }
    if let Some(jobs) = args.execution.jobs {
        // Best-effort: configure global rayon pool once. Ignore error if already set.
        let _ = rayon::ThreadPoolBuilder::new()
            .num_threads(jobs)
            .build_global();
    }

    let files = discover_inputs(&args.inputs);

    if args.output.out.is_some() && files.len() != 1 {
        return Err("--out requires a single input".into());
    }

    let mut tasks: Vec<(PathBuf, PathBuf, PathBuf)> = Vec::with_capacity(files.len());
    if let Some(ref out) = args.output.out {
        let (root, file) = &files[0];
        tasks.push((root.clone(), file.clone(), out.clone()));
    } else {
        for (root, input) in files {
            let output = compute_output_path_unchecked(&root, &input, args);
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

fn run_inspect(args: &InspectArgs) -> Result<(), AnyError> {
    let sas = SasReader::open(&args.input)?;
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
                    sas7bdat::dataset::VariableKind::Numeric => "numeric",
                    sas7bdat::dataset::VariableKind::Character => "character",
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
                sas7bdat::dataset::VariableKind::Numeric => "numeric",
                sas7bdat::dataset::VariableKind::Character => "character",
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
    let _log_prefix = set_log_prefix(input.to_string_lossy());
    // Prepare reader and metadata
    let mut sas = SasReader::open(input)?;
    if let Some(cat) = &args.catalog {
        let _ = sas.attach_catalog(cat);
    }
    let (mut reader, parsed) = sas.into_parts();

    // Resolve projection
    let (indices, selection, meta_filtered, cols_filtered) =
        resolve_projection(&parsed.header.metadata, &parsed.columns, args)?;

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
    match sink_kind {
        SinkKind::Parquet => {
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

            let batch_rows = columnar_row_group_rows.unwrap_or(columnar_batch_rows);
            let col_opts = ColumnarOptions {
                selection: &selection,
                batch_rows,
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
            stream_into_sink(
                &mut reader,
                &parsed,
                &meta_filtered,
                &cols_filtered,
                &options,
                Some(input.to_string_lossy().to_string()),
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
    let mut projected: Vec<CellValue<'static>> = Vec::new();

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
    while let Some(mut batch) = it.next_columnar_batch_contiguous(options.batch_rows)? {
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

fn discover_inputs(inputs: &[PathBuf]) -> Vec<(PathBuf, PathBuf)> {
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
                    files.push((input.clone(), path.to_path_buf()));
                }
            }
        } else if input.is_file() {
            if is_sas7bdat(input) {
                let root = input
                    .parent()
                    .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
                files.push((root, input.clone()));
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

fn compute_output_path_unchecked(root: &Path, input: &Path, args: &ConvertArgs) -> PathBuf {
    use std::ffi::OsStr;
    let new_ext = match args.output.sink {
        SinkKind::Parquet => "parquet",
        SinkKind::Csv => "csv",
        SinkKind::Tsv => "tsv",
    };
    args.output.out_dir.as_ref().map_or_else(
        || input.with_extension(new_ext),
        |dir| {
            if args.output.flatten {
                let fname = input.file_name().unwrap_or_else(|| OsStr::new("output"));
                let renamed = PathBuf::from(fname).with_extension(new_ext);
                return dir.join(renamed);
            }

            let rel = input.strip_prefix(root).unwrap_or(input);
            let mut renamed = rel.to_path_buf();
            let file = renamed
                .file_name()
                .map_or_else(|| PathBuf::from("output"), PathBuf::from);
            renamed.set_file_name(file.with_extension(new_ext));
            dir.join(renamed)
        },
    )
}
