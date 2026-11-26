use clap::{Args, Parser, Subcommand, ValueEnum};
use sas7bdat::IoBackendPreference;
use std::path::PathBuf;
use std::sync::LazyLock;

const EXAMPLES: &str = "\
Examples:
  sas7bdat info data.sas7bdat              Show columns, types, labels, and a sample
  sas7bdat head data.sas7bdat              Preview the first rows as a table
  sas7bdat convert data.sas7bdat           Convert to data.parquet
  sas7bdat convert data.sas7bdat --out d.csv   Convert to CSV (format inferred from .csv)
  sas7bdat convert ./folder --out-dir out  Convert a whole folder, mirroring the tree

Run 'sas7bdat <command> --help' for the full options of a command.";

/// `--version` output: this CLI's version, and the `sas7bdat` reader compiled into it.
///
/// The CLI and the reader have separate version lines, so the CLI version does not say which
/// reader is compiled in. `clap::builder::Str` holds a `&'static str`, hence the static
/// rather than a `format!` in the attribute.
static LONG_VERSION: LazyLock<String> = LazyLock::new(|| {
    format!(
        "{} (sas7bdat core {})",
        env!("CARGO_PKG_VERSION"),
        sas7bdat::VERSION
    )
});

/// The unified, user-facing CLI: `sas7bdat <command>`.
#[derive(Parser)]
#[command(
    name = "sas7bdat",
    version,
    // `-V` stays short; `--version` also names the reader compiled in.
    long_version = LONG_VERSION.as_str(),
    about = "Read, inspect, and convert SAS7BDAT files",
    propagate_version = true,
    after_help = EXAMPLES
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

// One of these is parsed per process and then matched on. Boxing the largest variant to
// even them out would cost an allocation and buy nothing.
#[allow(clippy::large_enum_variant)]
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

    #[command(flatten)]
    pub shape: ColumnShapeOptions,

    /// Value-label catalog (.sas7bcat) used to attach labels to the output.
    #[arg(long, value_name = "FILE", help_heading = "Input")]
    pub catalog: Option<PathBuf>,

    /// How to read the input: memory-map, sequential reads, or auto.
    #[arg(long = "io-backend", value_enum, default_value_t = IoBackend::Auto, help_heading = "Input")]
    pub io_backend: IoBackend,
    /// Read the file's text in this encoding instead of the one its header declares,
    /// e.g. utf-8 for UTF-8 data in a file marked latin1. A WHATWG label or a SAS name
    /// such as wlatin1.
    #[arg(long, value_name = "LABEL", help_heading = "Input")]
    pub encoding: Option<String>,

    /// Fail on the first cell that does not decode in the file's encoding, instead of
    /// replacing it.
    #[arg(long, help_heading = "Input")]
    pub strict: bool,
}

/// How the written columns are named and typed.
#[derive(Args, Clone, Default)]
pub struct ColumnShapeOptions {
    /// Case of the written column names. `--rename` overrides it for the columns it names.
    #[arg(long, value_enum, default_value_t = NameCaseArg::AsIs, help_heading = "Columns")]
    pub names: NameCaseArg,

    /// Write column OLD as NEW. OLD matches regardless of case; repeat for more columns.
    #[arg(long, value_name = "OLD=NEW", value_parser = parse_pair, help_heading = "Columns")]
    pub rename: Vec<(String, String)>,

    /// Append a column NAME holding VALUE in every row; repeat for more columns.
    #[arg(long = "add-column", value_name = "NAME=VALUE", value_parser = parse_pair, help_heading = "Columns")]
    pub add_column: Vec<(String, String)>,

    /// Append a column NAME holding the input file's path, as given, in every row.
    #[arg(long = "source-column", value_name = "NAME", help_heading = "Columns")]
    pub source_column: Option<String>,

    /// Arrow type of text columns in Parquet output.
    #[arg(long = "string-type", value_enum, default_value_t = StringTypeArg::String, help_heading = "Columns")]
    pub string_type: StringTypeArg,

    /// Unit of timestamps in Parquet output. Default: microseconds.
    #[arg(
        long = "timestamp-unit",
        value_enum,
        value_name = "UNIT",
        help_heading = "Columns"
    )]
    pub timestamp_unit: Option<TimeUnitArg>,

    /// How SAS times are written in Parquet output. `time-of-day` is Parquet's TIME,
    /// which every reader shows as a time, and fails a file holding a value outside one
    /// day. `duration` keeps elapsed times, including those past 24 hours or below zero,
    /// but reads as an integer outside Arrow.
    #[arg(long = "time-type", value_enum, default_value_t = TimeTypeArg::TimeOfDay, help_heading = "Columns")]
    pub time_type: TimeTypeArg,

    /// Unit for --time-type. Default: us for time-of-day, ns for duration.
    #[arg(
        long = "time-unit",
        value_enum,
        value_name = "UNIT",
        help_heading = "Columns"
    )]
    pub time_unit: Option<TimeUnitArg>,
}

fn parse_pair(text: &str) -> Result<(String, String), String> {
    match text.split_once('=') {
        Some((key, value)) if !key.is_empty() => Ok((key.to_string(), value.to_string())),
        _ => Err(format!("expected NAME=VALUE, got `{text}`")),
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum NameCaseArg {
    /// As stored in the file.
    #[default]
    AsIs,
    Lower,
    Upper,
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum StringTypeArg {
    /// `string`, 32-bit offsets.
    #[default]
    String,
    /// `large_string`, 64-bit offsets.
    LargeString,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum TimeUnitArg {
    S,
    Ms,
    Us,
    Ns,
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum TimeTypeArg {
    #[default]
    TimeOfDay,
    Duration,
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

    /// Stage outputs in this directory, then move them to the destination.
    ///
    /// Every output is written to a temporary file and moved into place when it is complete,
    /// so a run that fails or is interrupted leaves nothing behind. By default that temporary
    /// sits beside the destination; point this at a local disk to keep the write off a network
    /// link until there is a finished file to send.
    #[arg(long, value_name = "DIR", help_heading = "Output")]
    pub tmp_dir: Option<PathBuf>,

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
    #[arg(
        long,
        value_name = "N",
        help_heading = "Tuning",
        hide_short_help = true
    )]
    pub jobs: Option<usize>,

    /// Threads per file, for both decoding and Parquet encoding. Defaults to every core.
    #[arg(
        long = "parse-threads",
        value_name = "N",
        help_heading = "Tuning",
        hide_short_help = true
    )]
    pub parse_threads: Option<usize>,

    /// Rows per scan batch. Defaults to one read extent's worth.
    ///
    /// Independent of `--parquet-row-group-size`: the writer gathers scan batches into row
    /// groups. Each batch costs serial work on the collector thread, so a larger value takes
    /// that stage off the critical path — at the cost of stretching the read extents, since a
    /// batch cannot span one.
    #[arg(
        long = "batch-rows",
        value_name = "ROWS",
        help_heading = "Tuning",
        hide_short_help = true
    )]
    pub batch_rows: Option<usize>,

    /// Decoded bytes that may sit in row groups being encoded but not yet written.
    ///
    /// This is what decides how many row groups encode at once, and so how the work splits
    /// between the row-group and column axes. The column axis tops out at the column count, so
    /// on a narrow table this is what fills a large host: raise it when cores sit idle at a
    /// large `--parquet-row-group-size`. Defaults to 512 MiB.
    #[arg(
        long = "encode-in-flight-bytes",
        value_name = "BYTES",
        help_heading = "Tuning",
        hide_short_help = true
    )]
    pub encode_in_flight_bytes: Option<usize>,

    /// Stop at the first file that fails (default: convert the rest and report at the end).
    #[arg(long, help_heading = "Execution")]
    pub fail_fast: bool,
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

/// How the reader gets at the file's bytes.
///
/// Mirrors [`IoBackendPreference`] one-for-one. It exists only because `ValueEnum` cannot be
/// derived for a type from another crate, and deriving it in the core would put clap in the
/// library's dependency tree.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Default, ValueEnum)]
pub enum IoBackend {
    /// Memory-map local files; read network shares sequentially (default).
    #[default]
    Auto,
    /// Always memory-map. Fastest on local storage; slow over a network share, where each
    /// access becomes a round-trip.
    Mmap,
    /// Always read sequentially. The safe choice for network storage.
    Buffered,
}

impl IoBackend {
    #[must_use]
    pub const fn preference(self) -> IoBackendPreference {
        match self {
            Self::Auto => IoBackendPreference::Auto,
            Self::Mmap => IoBackendPreference::Mmap,
            Self::Buffered => IoBackendPreference::Buffered,
        }
    }
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

    /// How to read the input: memory-map, sequential reads, or auto.
    #[arg(long = "io-backend", value_enum, default_value_t = IoBackend::Auto, help_heading = "Input")]
    pub io_backend: IoBackend,
    /// Read the file's text in this encoding instead of the one its header declares,
    /// e.g. utf-8 for UTF-8 data in a file marked latin1. A WHATWG label or a SAS name
    /// such as wlatin1.
    #[arg(long, value_name = "LABEL", help_heading = "Input")]
    pub encoding: Option<String>,
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

    /// How to read the input: memory-map, sequential reads, or auto.
    #[arg(long = "io-backend", value_enum, default_value_t = IoBackend::Auto, help_heading = "Input")]
    pub io_backend: IoBackend,
    /// Read the file's text in this encoding instead of the one its header declares,
    /// e.g. utf-8 for UTF-8 data in a file marked latin1. A WHATWG label or a SAS name
    /// such as wlatin1.
    #[arg(long, value_name = "LABEL", help_heading = "Input")]
    pub encoding: Option<String>,

    /// Fail on the first cell that does not decode in the file's encoding, instead of
    /// replacing it.
    #[arg(long, help_heading = "Input")]
    pub strict: bool,
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

// ── clap enums -> library enums ──────────────────────────────────────────────
//
// `sas7bdat-convert` deliberately carries no clap dependency, so its equivalents
// are plain enums. These conversions are the single boundary between the two, and
// being exhaustive `match`es they fail to compile if either side gains a variant.

impl From<SinkKind> for sas7bdat_convert::SinkKind {
    fn from(value: SinkKind) -> Self {
        match value {
            SinkKind::Parquet => Self::Parquet,
            SinkKind::Csv => Self::Csv,
            SinkKind::Tsv => Self::Tsv,
        }
    }
}

impl From<RecursionMode> for sas7bdat_convert::RecursionMode {
    fn from(value: RecursionMode) -> Self {
        match value {
            RecursionMode::Recursive => Self::Recursive,
            RecursionMode::Never => Self::Never,
        }
    }
}

impl From<CompressionCodec> for sas7bdat_convert::CompressionCodec {
    fn from(value: CompressionCodec) -> Self {
        match value {
            CompressionCodec::Zstd => Self::Zstd,
            CompressionCodec::Lz4 => Self::Lz4,
            CompressionCodec::Snappy => Self::Snappy,
            CompressionCodec::None => Self::None,
        }
    }
}

impl From<TimeUnitArg> for arrow_schema::TimeUnit {
    fn from(unit: TimeUnitArg) -> Self {
        match unit {
            TimeUnitArg::S => Self::Second,
            TimeUnitArg::Ms => Self::Millisecond,
            TimeUnitArg::Us => Self::Microsecond,
            TimeUnitArg::Ns => Self::Nanosecond,
        }
    }
}

impl ColumnShapeOptions {
    /// These flags as the conversion library's column plan.
    #[must_use]
    pub fn column_plan(&self) -> sas7bdat_convert::columns::ColumnPlan {
        use arrow_schema::TimeUnit;
        use sas7bdat_convert::columns::{
            ColumnPlan, ConstantValue, NameCase, StringType, TimeType,
        };
        let mut constants: Vec<(String, ConstantValue)> = self
            .add_column
            .iter()
            .map(|(name, value)| (name.clone(), ConstantValue::Text(value.clone())))
            .collect();
        if let Some(name) = &self.source_column {
            constants.push((name.clone(), ConstantValue::InputPath));
        }
        ColumnPlan {
            case: match self.names {
                NameCaseArg::AsIs => NameCase::AsIs,
                NameCaseArg::Lower => NameCase::Lower,
                NameCaseArg::Upper => NameCase::Upper,
            },
            rename: self.rename.clone(),
            constants,
            strings: match self.string_type {
                StringTypeArg::String => StringType::Utf8,
                StringTypeArg::LargeString => StringType::LargeUtf8,
            },
            timestamps: self.timestamp_unit.map(Into::into),
            times: match self.time_type {
                TimeTypeArg::Duration => {
                    TimeType::Duration(self.time_unit.map_or(TimeUnit::Nanosecond, Into::into))
                }
                TimeTypeArg::TimeOfDay => {
                    TimeType::TimeOfDay(self.time_unit.map_or(TimeUnit::Microsecond, Into::into))
                }
            },
        }
    }
}

impl ConvertArgs {
    /// The output layout this invocation asks for, as the conversion library sees it.
    #[must_use]
    pub fn output_layout(&self) -> sas7bdat_convert::OutputLayout {
        sas7bdat_convert::OutputLayout {
            out_dir: self.output.out_dir.clone(),
            flatten: self.output.flatten,
            sink: self.output.effective_sink().into(),
        }
    }
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
            tmp_dir: None,
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
        assert_eq!(
            output(None, Some("data.csv")).effective_sink(),
            SinkKind::Csv
        );
        assert_eq!(
            output(None, Some("data.TSV")).effective_sink(),
            SinkKind::Tsv
        );
        assert_eq!(
            output(None, Some("data.parquet")).effective_sink(),
            SinkKind::Parquet
        );
    }

    #[test]
    fn effective_sink_defaults_to_parquet() {
        assert_eq!(output(None, None).effective_sink(), SinkKind::Parquet);
        // Unknown extension falls back to parquet too.
        assert_eq!(
            output(None, Some("data.dat")).effective_sink(),
            SinkKind::Parquet
        );
    }
}
