use clap::{ArgAction, Parser, ValueEnum};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "sas7",
    version,
    about = "Batch convert SAS7BDAT to Parquet/CSV/TSV"
)]
pub struct Cli {
    /// Conversion options (default mode).
    #[command(flatten)]
    pub(crate) convert: ConvertArgs,

    /// Inspect dataset metadata and print a summary.
    #[arg(long, value_name = "FILE", help_heading = "Inspect")]
    pub(crate) inspect: Option<PathBuf>,

    /// Emit JSON instead of human readable output (inspect only).
    #[arg(long, requires = "inspect", help_heading = "Inspect")]
    pub(crate) inspect_json: bool,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum SinkKind {
    Parquet,
    Csv,
    Tsv,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum ColumnarBatchModeArg {
    Segmented,
    Contiguous,
    Adaptive,
}

#[derive(Parser, Clone)]
pub struct ConvertArgs {
    /// Input files or directories (recurses directories).
    #[arg(
        required_unless_present = "inspect",
        value_name = "PATH",
        help_heading = "Input"
    )]
    pub(crate) inputs: Vec<PathBuf>,

    #[command(flatten)]
    pub(crate) output: OutputOptions,

    #[command(flatten)]
    pub(crate) execution: ExecutionOptions,

    #[command(flatten)]
    pub(crate) validation: ValidationOptions,

    #[command(flatten)]
    pub(crate) logging: LoggingOptions,

    /// Skip leading N rows.
    #[arg(long, help_heading = "Row Limits")]
    pub(crate) skip: Option<u64>,

    /// Limit to at most N rows.
    #[arg(long = "max-rows", help_heading = "Row Limits")]
    pub(crate) max_rows: Option<u64>,

    /// Project a subset of columns by name (comma-separated).
    #[arg(
        long = "columns",
        value_delimiter = ',',
        help_heading = "Projection",
        value_name = "NAME[,NAME]"
    )]
    pub(crate) columns: Option<Vec<String>>,

    /// Project a subset of columns by zero-based indices (comma-separated).
    #[arg(
        long = "column-indices",
        value_delimiter = ',',
        help_heading = "Projection",
        value_name = "IDX[,IDX]"
    )]
    pub(crate) column_indices: Option<Vec<usize>>,

    /// Optional value-label catalog (.sas7bcat) to load.
    #[arg(long, value_name = "FILE", help_heading = "Input")]
    pub(crate) catalog: Option<PathBuf>,
}

#[derive(Parser, Clone)]
pub struct OutputOptions {
    /// Output directory (computed file names).
    #[arg(
        long,
        conflicts_with = "out",
        value_name = "DIR",
        help_heading = "Output"
    )]
    pub(crate) out_dir: Option<PathBuf>,

    /// Output file (only valid with a single input).
    #[arg(
        long,
        conflicts_with = "out_dir",
        value_name = "FILE",
        help_heading = "Output"
    )]
    pub(crate) out: Option<PathBuf>,

    /// Sink kind: parquet, csv, or tsv.
    #[arg(long, value_enum, default_value_t = SinkKind::Parquet, help_heading = "Output")]
    pub(crate) sink: SinkKind,

    /// CSV/TSV delimiter. Defaults to ',' for csv and '\t' for tsv.
    #[arg(long, help_heading = "Output")]
    pub(crate) delimiter: Option<char>,

    /// Write header row (CSV/TSV only).
    #[arg(
        long = "headers",
        action = ArgAction::SetTrue,
        default_value_t = true,
        help_heading = "Output"
    )]
    pub(crate) headers: bool,

    /// Disable header row (CSV/TSV only).
    #[arg(
        long = "no-headers",
        action = ArgAction::SetFalse,
        overrides_with = "headers",
        help_heading = "Output"
    )]
    pub(crate) _no_headers: bool,

    /// Parquet row group size (rows). If unset, uses the library's heuristic.
    #[arg(long, value_name = "ROWS", help_heading = "Parquet")]
    pub(crate) parquet_row_group_size: Option<usize>,

    /// Parquet target row group size (bytes) for automatic estimation.
    #[arg(long, value_name = "BYTES", help_heading = "Parquet")]
    pub(crate) parquet_target_bytes: Option<usize>,

    /// Columnar batch assembly mode for Parquet streaming.
    #[arg(
        long = "columnar-batch-mode",
        value_enum,
        default_value_t = ColumnarBatchModeArg::Adaptive,
        help_heading = "Parquet"
    )]
    pub(crate) columnar_batch_mode: ColumnarBatchModeArg,

    /// Flatten outputs into a single directory instead of mirroring input tree.
    #[arg(long, help_heading = "Output")]
    pub(crate) flatten: bool,
}

#[derive(Parser, Clone)]
pub struct ExecutionOptions {
    /// Number of concurrent worker threads (default: Rayon global pool, usually logical CPUs unless `RAYON_NUM_THREADS` is set).
    #[arg(long, help_heading = "Execution")]
    pub(crate) jobs: Option<usize>,

    /// Parser worker threads per file for row-based conversion paths (default: 1).
    #[arg(long = "parse-threads", help_heading = "Execution")]
    pub(crate) parse_threads: Option<usize>,

    /// Stop on first error.
    #[arg(long, help_heading = "Execution")]
    pub(crate) fail_fast: bool,
}

#[derive(Parser, Clone)]
pub struct ValidationOptions {
    /// Enforce strict date/time conversion (fail on out-of-range or malformed values).
    #[arg(long, help_heading = "Validation")]
    pub(crate) strict_dates: bool,
}

#[derive(Parser, Clone)]
pub struct LoggingOptions {
    /// Write warnings and errors to a log file in addition to stderr.
    #[arg(long, value_name = "FILE", help_heading = "Logging")]
    pub(crate) log_file: Option<PathBuf>,
}
