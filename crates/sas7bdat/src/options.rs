/// How the reader gets at a file's bytes.
///
/// Three variants because `Dataset::open_with` makes exactly one decision from this — whether
/// to attempt a memory map. An earlier `BufferedPreferred` sat beside `BufferedOnly` and took
/// the same branch as it, so the pair named a preference the opener could not act on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IoBackendPreference {
    /// Memory-map local files; read network paths sequentially. The default.
    #[default]
    Auto,
    /// Always attempt a memory map, network path or not. Falls back to buffered reads if the
    /// map itself fails.
    Mmap,
    /// Never map; read the file sequentially. The safe choice for network storage, where each
    /// page fault becomes a round trip.
    Buffered,
}

impl IoBackendPreference {
    /// The canonical lowercase spelling, and the inverse of [`FromStr`].
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Mmap => "mmap",
            Self::Buffered => "buffered",
        }
    }
}

impl std::fmt::Display for IoBackendPreference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for IoBackendPreference {
    type Err = crate::error::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        // The hyphenated spellings are the names the profiling tools accepted before the
        // variants collapsed; kept so existing invocations and scripts keep working.
        match value.to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "mmap" | "mmap-preferred" => Ok(Self::Mmap),
            "buffered" | "buffered-only" | "buffered-preferred" => Ok(Self::Buffered),
            other => Err(crate::error::Error::unsupported(format!(
                "unknown io backend {other:?}; expected auto, mmap, or buffered"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenOptions {
    pub(crate) io_backend: IoBackendPreference,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            io_backend: IoBackendPreference::Auto,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenOptionsBuilder {
    io_backend: IoBackendPreference,
}

impl OpenOptions {
    #[must_use]
    pub const fn builder() -> OpenOptionsBuilder {
        OpenOptionsBuilder::new()
    }
}

impl OpenOptionsBuilder {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            io_backend: IoBackendPreference::Auto,
        }
    }

    #[must_use]
    pub const fn io_backend(mut self, io_backend: IoBackendPreference) -> Self {
        self.io_backend = io_backend;
        self
    }

    #[must_use]
    pub const fn build(self) -> OpenOptions {
        OpenOptions {
            io_backend: self.io_backend,
        }
    }
}

impl Default for OpenOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeMode {
    Raw,
    Typed,
    TypedLossless,
}

/// What a scan does with bytes that are invalid in the file's declared encoding.
///
/// Two variants because there are two behaviours. An earlier `Off` never differed from the
/// lenient path — every decision site tests for `Strict` and treats everything else as lossy —
/// so it promised a third behaviour the decoder does not have.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Utf8ValidationMode {
    /// Replace what will not decode and carry on, optionally repairing mojibake
    /// (see [`MojibakePolicy`]). The default, and what the `*Lenient` decode kernels implement.
    #[default]
    Lenient,
    /// Fail the scan on the first cell that does not decode cleanly.
    Strict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MojibakePolicy {
    Auto,
    Off,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DictionaryStaging {
    Auto,
    Off,
    On,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrimMode {
    Preserve,
    RTrim,
    Strip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StringDecodeOptions {
    pub(crate) trim_mode: TrimMode,
    pub(crate) utf8_validation: Utf8ValidationMode,
    pub(crate) mojibake_fix: MojibakePolicy,
    pub(crate) dictionary_staging: DictionaryStaging,
}

impl Default for StringDecodeOptions {
    fn default() -> Self {
        Self {
            trim_mode: TrimMode::RTrim,
            utf8_validation: Utf8ValidationMode::Lenient,
            mojibake_fix: MojibakePolicy::Auto,
            dictionary_staging: DictionaryStaging::Auto,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StringDecodeOptionsBuilder {
    trim_mode: TrimMode,
    utf8_validation: Utf8ValidationMode,
    mojibake_fix: MojibakePolicy,
    dictionary_staging: DictionaryStaging,
}

impl StringDecodeOptions {
    #[must_use]
    pub const fn builder() -> StringDecodeOptionsBuilder {
        StringDecodeOptionsBuilder::new()
    }
}

impl StringDecodeOptionsBuilder {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            trim_mode: TrimMode::RTrim,
            utf8_validation: Utf8ValidationMode::Lenient,
            mojibake_fix: MojibakePolicy::Auto,
            dictionary_staging: DictionaryStaging::Auto,
        }
    }

    #[must_use]
    pub const fn trim_mode(mut self, trim_mode: TrimMode) -> Self {
        self.trim_mode = trim_mode;
        self
    }

    #[must_use]
    pub const fn utf8_validation(mut self, utf8_validation: Utf8ValidationMode) -> Self {
        self.utf8_validation = utf8_validation;
        self
    }

    #[must_use]
    pub const fn mojibake_fix(mut self, mojibake_fix: MojibakePolicy) -> Self {
        self.mojibake_fix = mojibake_fix;
        self
    }

    #[must_use]
    pub const fn dictionary_staging(mut self, dictionary_staging: DictionaryStaging) -> Self {
        self.dictionary_staging = dictionary_staging;
        self
    }

    #[must_use]
    pub const fn build(self) -> StringDecodeOptions {
        StringDecodeOptions {
            trim_mode: self.trim_mode,
            utf8_validation: self.utf8_validation,
            mojibake_fix: self.mojibake_fix,
            dictionary_staging: self.dictionary_staging,
        }
    }
}

impl Default for StringDecodeOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemporalDecodeOptions {
    pub(crate) decode_dates: bool,
    pub(crate) decode_datetimes: bool,
    pub(crate) decode_times: bool,
}

impl Default for TemporalDecodeOptions {
    fn default() -> Self {
        Self {
            decode_dates: true,
            decode_datetimes: true,
            decode_times: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemporalDecodeOptionsBuilder {
    decode_dates: bool,
    decode_datetimes: bool,
    decode_times: bool,
}

impl TemporalDecodeOptions {
    #[must_use]
    pub const fn builder() -> TemporalDecodeOptionsBuilder {
        TemporalDecodeOptionsBuilder::new()
    }
}

impl TemporalDecodeOptionsBuilder {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            decode_dates: true,
            decode_datetimes: true,
            decode_times: true,
        }
    }

    #[must_use]
    pub const fn decode_dates(mut self, decode_dates: bool) -> Self {
        self.decode_dates = decode_dates;
        self
    }

    #[must_use]
    pub const fn decode_datetimes(mut self, decode_datetimes: bool) -> Self {
        self.decode_datetimes = decode_datetimes;
        self
    }

    #[must_use]
    pub const fn decode_times(mut self, decode_times: bool) -> Self {
        self.decode_times = decode_times;
        self
    }

    #[must_use]
    pub const fn build(self) -> TemporalDecodeOptions {
        TemporalDecodeOptions {
            decode_dates: self.decode_dates,
            decode_datetimes: self.decode_datetimes,
            decode_times: self.decode_times,
        }
    }
}

impl Default for TemporalDecodeOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderingMode {
    Stable,
    Unordered,
}

/// How many threads a scan may use to decode pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Parallelism {
    /// Every logical core (`available_parallelism`), **clamped to the work the file holds**.
    ///
    /// The clamp is the point. Spawning threads costs more than it saves on a small file:
    /// timed across the corpus, twelve threads decode `cars` 2.70x *slower* than one, and
    /// `date_format_time_loop` 1.75x slower, while `homimp` runs 3.86x faster. So `Auto`
    /// resolves to a single worker until there are enough rows and pages to hand each thread
    /// a real share, and a single worker is not a lesser path: it runs the same chunk decode
    /// inline on the calling thread and still reaches the tiled fill.
    ///
    /// The thresholds and the evidence behind them are on `resolved_parallel_workers` in
    /// `scan::builder`, including one file this policy is known to get wrong.
    ///
    /// A caller running several scans at once should divide the machine between them with
    /// [`Parallelism::Threads`] instead.
    Auto,
    /// One thread. Decode runs on the calling thread.
    None,
    /// Exactly this many threads, clamped only to the pages that exist — for callers that
    /// know their own workload. No grain check: naming a number says you know better.
    Threads(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchHint {
    Auto,
    Rows(usize),
    Bytes(usize),
}

/// Whether owned-batch scans may use the column-major (transposed) decode path.
///
/// The default row-major path decodes one row at a time. When every projected column is a
/// numeric tile and the source is in-memory, the column-major path instead decodes each
/// `FusedContiguousUncompressed` page column-by-column in cache-sized tiles, which hoists
/// per-cell dispatch out of the inner loop and is markedly faster for wide tables. It only
/// affects [`crate::ScanBuilder::collect_batches`]; it falls back to row-major automatically
/// whenever its preconditions don't hold, so output is identical either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnMajorDecode {
    /// Never use the column-major path.
    Off,
    /// Use the column-major path whenever its preconditions hold; row-major otherwise.
    ///
    /// This is the default — see `ScanBuilder::new`. Reading it as opt-in understates what
    /// the scanner already does: the repository's own `wide_table` benchmark puts this
    /// path 1.4x to 2.1x ahead of row-major from 16 to 1024 columns, and
    /// `examples/verify_columnar.rs` decodes every corpus fixture both ways to confirm
    /// the results are byte-identical.
    On,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowSelection {
    All,
    Range {
        start: crate::types::RowIndex,
        end: crate::types::RowIndex,
    },
    /// Read only the first `n` rows. Equivalent to `.scan().limit(n)` but
    /// expressible without the builder for the common "read a sample" pattern.
    First(u64),
}

impl RowSelection {
    /// Construct a row range from plain `u64` bounds without needing the
    /// [`crate::RowIndex`] newtype.
    #[must_use]
    pub const fn range(start: u64, end: u64) -> Self {
        Self::Range {
            start: crate::types::RowIndex(start),
            end: crate::types::RowIndex(end),
        }
    }
}
