use clap::{ArgAction, Parser, ValueEnum};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "sas7bdat-convert",
    version,
    about = "Convert SAS7BDAT datasets to parquet, CSV, or TSV"
)]
pub struct ConvertArgs {
    /// Input files or directories. Directories are scanned recursively.
    #[arg(required = true, value_name = "PATH")]
    pub inputs: Vec<PathBuf>,

    /// Directory traversal mode for directory inputs.
    #[arg(
        long,
        short = 'r',
        value_enum,
        default_value_t = RecursionMode::Recursive,
        default_missing_value = "recursive",
        help_heading = "Input"
    )]
    pub recursive: RecursionMode,

    #[command(flatten)]
    pub output: OutputOptions,

    #[command(flatten)]
    pub execution: ExecutionOptions,

    #[command(flatten)]
    pub validation: ValidationOptions,

    #[command(flatten)]
    pub ui: UiOptions,

    /// Skip leading N rows.
    #[arg(long, help_heading = "Row Limits")]
    pub skip: Option<u64>,

    /// Limit to at most N rows.
    #[arg(long = "max-rows", help_heading = "Row Limits")]
    pub max_rows: Option<u64>,

    /// Project a subset of columns by name.
    #[arg(
        long = "columns",
        value_delimiter = ',',
        help_heading = "Projection",
        value_name = "NAME[,NAME]"
    )]
    pub columns: Option<Vec<String>>,

    /// Project a subset of columns by zero-based index.
    #[arg(
        long = "column-indices",
        value_delimiter = ',',
        help_heading = "Projection",
        value_name = "IDX[,IDX]"
    )]
    pub column_indices: Option<Vec<usize>>,

    /// Optional value-label catalog (.sas7bcat) to attach to parquet metadata.
    #[arg(long, value_name = "FILE", help_heading = "Input")]
    pub catalog: Option<PathBuf>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum SinkKind {
    Parquet,
    Csv,
    Tsv,
}

#[derive(Parser, Clone)]
#[allow(clippy::struct_excessive_bools)]
pub struct OutputOptions {
    /// Output directory. Computed file names preserve the input tree unless flattened.
    #[arg(
        long,
        conflicts_with = "out",
        value_name = "DIR",
        help_heading = "Output"
    )]
    pub out_dir: Option<PathBuf>,

    /// Output file. Only valid for a single input.
    #[arg(
        long,
        conflicts_with = "out_dir",
        value_name = "FILE",
        help_heading = "Output"
    )]
    pub out: Option<PathBuf>,

    /// Sink kind.
    #[arg(long, value_enum, default_value_t = SinkKind::Parquet, help_heading = "Output")]
    pub sink: SinkKind,

    /// CSV/TSV delimiter. Defaults to ',' for CSV and '\t' for TSV.
    #[arg(long, help_heading = "Output")]
    pub delimiter: Option<char>,

    /// Write a header row for CSV/TSV.
    #[arg(
        long = "headers",
        action = ArgAction::SetTrue,
        default_value_t = true,
        help_heading = "Output"
    )]
    pub headers: bool,

    /// Disable a header row for CSV/TSV.
    #[arg(
        long = "no-headers",
        action = ArgAction::SetFalse,
        overrides_with = "headers",
        help_heading = "Output"
    )]
    pub no_headers: bool,

    /// Flatten outputs into a single directory instead of mirroring input trees.
    #[arg(long, help_heading = "Output")]
    pub flatten: bool,

    /// Overwrite existing output files.
    #[arg(long, help_heading = "Output")]
    pub overwrite: bool,

    /// Parquet row group size in rows.
    #[arg(long, value_name = "ROWS", help_heading = "Parquet")]
    pub parquet_row_group_size: Option<usize>,

    /// Parquet target row group size in bytes.
    #[arg(long, value_name = "BYTES", help_heading = "Parquet")]
    pub parquet_target_bytes: Option<usize>,
}

#[derive(Parser, Clone)]
pub struct ExecutionOptions {
    /// Number of worker threads for file-level parallelism.
    #[arg(long, help_heading = "Execution")]
    pub jobs: Option<usize>,

    /// Parser worker threads per file.
    #[arg(long = "parse-threads", help_heading = "Execution")]
    pub parse_threads: Option<usize>,

    /// Stop on the first conversion error.
    #[arg(long, help_heading = "Execution")]
    pub fail_fast: bool,
}

#[derive(Parser, Clone)]
pub struct ValidationOptions {
    /// Enforce strict temporal validation.
    #[arg(long, help_heading = "Validation")]
    pub strict_dates: bool,
}

#[derive(Parser, Clone)]
pub struct UiOptions {
    /// Suppress per-file success messages.
    #[arg(long, short = 'q', help_heading = "Output")]
    pub quiet: bool,

    /// Progress display: auto, always, or never.
    #[arg(
        long,
        short = 'p',
        value_enum,
        default_value_t = ProgressMode::Auto,
        default_missing_value = "always",
        help_heading = "Output"
    )]
    pub progress: ProgressMode,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum ProgressMode {
    Auto,
    Always,
    Never,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum RecursionMode {
    Recursive,
    Never,
}

#[derive(Parser, Clone)]
#[command(
    name = "sas7bdat-inspect",
    version,
    about = "Inspect SAS7BDAT dataset metadata and print a summary"
)]
pub struct InspectArgs {
    /// Input SAS7BDAT file.
    #[arg(value_name = "FILE")]
    pub input: PathBuf,

    /// Emit JSON instead of human-readable output.
    #[arg(long, help_heading = "Inspect")]
    pub json: bool,

    /// Limit the human-readable output to the first N columns.
    #[arg(long, value_name = "N", default_value_t = 20, help_heading = "Inspect")]
    pub max_columns: usize,

    /// Print all columns in the human-readable output.
    #[arg(long, help_heading = "Inspect")]
    pub all_columns: bool,

    /// Filter columns by name.
    #[arg(
        long = "columns",
        value_delimiter = ',',
        conflicts_with = "column_indices",
        help_heading = "Inspect",
        value_name = "NAME[,NAME]"
    )]
    pub columns: Option<Vec<String>>,

    /// Filter columns by zero-based index.
    #[arg(
        long = "column-indices",
        value_delimiter = ',',
        conflicts_with = "columns",
        help_heading = "Inspect",
        value_name = "IDX[,IDX]"
    )]
    pub column_indices: Option<Vec<usize>>,
}
