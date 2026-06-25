use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

const EXAMPLES: &str = "\
Examples:
  sas7bdat info data.sas7bdat              Show columns, types, labels, and a sample
  sas7bdat head data.sas7bdat              Preview the first rows as a table
  sas7bdat convert data.sas7bdat           Convert to data.parquet
  sas7bdat convert data.sas7bdat --out d.csv   Convert to CSV (format inferred from .csv)
  sas7bdat convert ./folder --out-dir out  Convert a whole folder, mirroring the tree

Run 'sas7bdat <command> --help' for the full options of a command.";

/// The unified, user-facing CLI: `sas7bdat <command>`.
#[derive(Parser)]
#[command(
    name = "sas7bdat",
    version,
    about = "Read, inspect, and convert SAS7BDAT files",
    propagate_version = true,
    after_help = EXAMPLES
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Convert SAS7BDAT files to Parquet, CSV, or TSV.
    Convert(ConvertArgs),
    /// Show a file's metadata, columns, and a small sample.
    Info(InspectArgs),
    /// Print the first rows of a file as a table.
    Head(HeadArgs),
    /// Generate a shell completion script (bash, zsh, fish, ...).
    Completions(CompletionsArgs),
}

#[derive(Args, Clone)]
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
    #[arg(long, value_name = "N", help_heading = "Rows")]
    pub skip: Option<u64>,

    /// Convert at most N rows.
    #[arg(long = "max-rows", value_name = "N", help_heading = "Rows")]
    pub max_rows: Option<u64>,

    /// Keep only these columns, by name.
    #[arg(
        long = "columns",
        value_delimiter = ',',
        help_heading = "Columns",
        value_name = "NAME[,NAME]"
    )]
    pub columns: Option<Vec<String>>,

    /// Keep only these columns, by zero-based index.
    #[arg(
        long = "column-indices",
        value_delimiter = ',',
        help_heading = "Columns",
        value_name = "IDX[,IDX]"
    )]
    pub column_indices: Option<Vec<usize>>,

    /// Value-label catalog (.sas7bcat) used to attach labels to the output.
    #[arg(long, value_name = "FILE", help_heading = "Input")]
    pub catalog: Option<PathBuf>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum SinkKind {
    Parquet,
    Csv,
    Tsv,
}

/// Parquet compression codec. Defaults to Zstd, which gives a strong size win over
/// uncompressed while staying fast to decode and broadly readable.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Default, ValueEnum)]
pub enum CompressionCodec {
    /// Zstandard, level 3 — best size/speed balance (default).
    #[default]
    Zstd,
    /// LZ4 (`lz4_raw`) — fastest decode, larger than Zstd.
    Lz4,
    /// Snappy — widely compatible, weaker ratio.
    Snappy,
    /// No compression.
    None,
}

#[derive(Args, Clone)]
#[allow(clippy::struct_excessive_bools)]
pub struct OutputOptions {
    /// Output directory. File names mirror the input tree unless --flatten is set.
    #[arg(
        long,
        conflicts_with = "out",
        value_name = "DIR",
        help_heading = "Output"
    )]
    pub out_dir: Option<PathBuf>,

    /// Output file (single input only). The format is inferred from its extension.
    #[arg(
        long,
        conflicts_with = "out_dir",
        value_name = "FILE",
        help_heading = "Output"
    )]
    pub out: Option<PathBuf>,

    /// Output format. Default: inferred from --out's extension, otherwise parquet.
    #[arg(long, value_enum, help_heading = "Output")]
    pub sink: Option<SinkKind>,

    /// Parquet compression codec.
    #[arg(long, value_enum, default_value_t = CompressionCodec::Zstd, help_heading = "Output")]
    pub compression: CompressionCodec,

    /// CSV/TSV delimiter. Defaults to ',' for CSV and a tab for TSV.
    #[arg(long, help_heading = "Output")]
    pub delimiter: Option<char>,

    /// Omit the header row in CSV/TSV output.
    #[arg(long = "no-header", help_heading = "Output")]
    pub no_header: bool,

    /// Put every output in one directory instead of mirroring input trees.
    #[arg(long, help_heading = "Output")]
    pub flatten: bool,

    /// Overwrite existing output files.
    #[arg(long, help_heading = "Output")]
    pub overwrite: bool,

    /// Embed SAS metadata (labels, formats, kinds, widths) into the Parquet file's
    /// key-value metadata under the `sas7bdat.metadata` key.
    #[arg(long, help_heading = "Output")]
    pub parquet_metadata: bool,

    /// Parquet row group size, in rows.
    #[arg(
        long,
        value_name = "ROWS",
        help_heading = "Tuning",
        hide_short_help = true
    )]
    pub parquet_row_group_size: Option<usize>,

    /// Parquet target row group size, in bytes.
    #[arg(
        long,
        value_name = "BYTES",
        help_heading = "Tuning",
        hide_short_help = true
    )]
    pub parquet_target_bytes: Option<usize>,
}

impl OutputOptions {
    /// Resolve the effective output format: an explicit `--sink` wins, otherwise the
    /// extension of `--out` (`.csv`/`.tsv`) is honored, otherwise Parquet.
    #[must_use]
    pub fn effective_sink(&self) -> SinkKind {
        if let Some(sink) = self.sink {
            return sink;
        }
        if let Some(out) = &self.out {
            match out
                .extension()
                .and_then(|ext| ext.to_str())
                .map(str::to_ascii_lowercase)
                .as_deref()
            {
                Some("csv") => return SinkKind::Csv,
                Some("tsv") => return SinkKind::Tsv,
                _ => {}
            }
        }
        SinkKind::Parquet
    }
}

#[derive(Args, Clone)]
pub struct ExecutionOptions {
    /// Worker threads for converting multiple files at once.
    #[arg(long, value_name = "N", help_heading = "Tuning", hide_short_help = true)]
    pub jobs: Option<usize>,

    /// Parser threads per file.
    #[arg(
        long = "parse-threads",
        value_name = "N",
        help_heading = "Tuning",
        hide_short_help = true
    )]
    pub parse_threads: Option<usize>,

    /// Stop at the first file that fails (default: convert the rest and report at the end).
    #[arg(long, help_heading = "Execution")]
    pub fail_fast: bool,
}

#[derive(Args, Clone)]
pub struct ValidationOptions {
    /// Reject out-of-range dates/times instead of passing them through.
    #[arg(long, help_heading = "Execution")]
    pub strict_dates: bool,
}

#[derive(Args, Clone)]
pub struct UiOptions {
    /// Don't print the per-file conversion summary.
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

#[derive(Args, Clone)]
pub struct InspectArgs {
    /// Input SAS7BDAT file.
    #[arg(value_name = "FILE")]
    pub input: PathBuf,

    /// Emit JSON instead of human-readable output.
    #[arg(long, help_heading = "Inspect")]
    pub json: bool,

    /// Limit the column table to the first N columns.
    #[arg(long, value_name = "N", default_value_t = 20, help_heading = "Inspect")]
    pub max_columns: usize,

    /// Print every column in the table.
    #[arg(long, help_heading = "Inspect")]
    pub all_columns: bool,

    /// Don't print the data sample.
    #[arg(long, help_heading = "Inspect")]
    pub no_sample: bool,

    /// Show only these columns, by name.
    #[arg(
        long = "columns",
        value_delimiter = ',',
        conflicts_with = "column_indices",
        help_heading = "Inspect",
        value_name = "NAME[,NAME]"
    )]
    pub columns: Option<Vec<String>>,

    /// Show only these columns, by zero-based index.
    #[arg(
        long = "column-indices",
        value_delimiter = ',',
        conflicts_with = "columns",
        help_heading = "Inspect",
        value_name = "IDX[,IDX]"
    )]
    pub column_indices: Option<Vec<usize>>,
}

#[derive(Args, Clone)]
pub struct HeadArgs {
    /// Input SAS7BDAT file.
    #[arg(value_name = "FILE")]
    pub input: PathBuf,

    /// Number of rows to show.
    #[arg(short = 'n', long = "rows", value_name = "N", default_value_t = 10)]
    pub rows: u64,

    /// Show only these columns, by name.
    #[arg(
        long = "columns",
        value_delimiter = ',',
        conflicts_with = "column_indices",
        value_name = "NAME[,NAME]"
    )]
    pub columns: Option<Vec<String>>,

    /// Show only these columns, by zero-based index.
    #[arg(
        long = "column-indices",
        value_delimiter = ',',
        conflicts_with = "columns",
        value_name = "IDX[,IDX]"
    )]
    pub column_indices: Option<Vec<usize>>,
}

#[derive(Args, Clone)]
pub struct CompletionsArgs {
    /// Shell to generate a completion script for.
    #[arg(value_enum)]
    pub shell: clap_complete::Shell,
}

/// Backward-compatible standalone binary: `sas7bdat-convert`.
#[derive(Parser)]
#[command(
    name = "sas7bdat-convert",
    version,
    about = "Convert SAS7BDAT datasets to parquet, CSV, or TSV"
)]
pub struct ConvertCli {
    #[command(flatten)]
    pub args: ConvertArgs,
}

/// Backward-compatible standalone binary: `sas7bdat-inspect`.
#[derive(Parser)]
#[command(
    name = "sas7bdat-inspect",
    version,
    about = "Inspect SAS7BDAT dataset metadata and print a summary"
)]
pub struct InspectCli {
    #[command(flatten)]
    pub args: InspectArgs,
}

#[cfg(test)]
mod tests {
    use super::{Cli, CompressionCodec, ConvertCli, InspectCli, OutputOptions, SinkKind};
    use clap::CommandFactory;
    use std::path::PathBuf;

    #[test]
    fn command_trees_are_valid() {
        // clap's own assertions catch duplicate flags, bad conflicts, etc.
        Cli::command().debug_assert();
        ConvertCli::command().debug_assert();
        InspectCli::command().debug_assert();
    }

    fn output(sink: Option<SinkKind>, out: Option<&str>) -> OutputOptions {
        OutputOptions {
            out_dir: None,
            out: out.map(PathBuf::from),
            sink,
            compression: CompressionCodec::Zstd,
            delimiter: None,
            no_header: false,
            flatten: false,
            overwrite: false,
            parquet_metadata: false,
            parquet_row_group_size: None,
            parquet_target_bytes: None,
        }
    }

    #[test]
    fn effective_sink_prefers_explicit_flag() {
        // An explicit --sink wins even when --out's extension says otherwise.
        assert_eq!(
            output(Some(SinkKind::Parquet), Some("data.csv")).effective_sink(),
            SinkKind::Parquet
        );
    }

    #[test]
    fn effective_sink_infers_from_out_extension() {
        assert_eq!(output(None, Some("data.csv")).effective_sink(), SinkKind::Csv);
        assert_eq!(output(None, Some("data.TSV")).effective_sink(), SinkKind::Tsv);
        assert_eq!(
            output(None, Some("data.parquet")).effective_sink(),
            SinkKind::Parquet
        );
    }

    #[test]
    fn effective_sink_defaults_to_parquet() {
        assert_eq!(output(None, None).effective_sink(), SinkKind::Parquet);
        // Unknown extension falls back to parquet too.
        assert_eq!(output(None, Some("data.dat")).effective_sink(), SinkKind::Parquet);
    }
}
