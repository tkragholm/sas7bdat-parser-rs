/// How the reader gets at a file's bytes.
///
/// Three variants because `Dataset::open_with` makes exactly one decision from this: whether
/// to attempt a memory map.
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
    /// The canonical lowercase spelling, and the inverse of [`FromStr`](std::str::FromStr).
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
        // The hyphenated spellings are accepted for compatibility with existing scripts.
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

/// Options for opening a file. Build one with [`OpenOptions::builder`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenOptions {
    pub(crate) io_backend: IoBackendPreference,
    /// Replaces the encoding the file declares, for every text in it.
    pub(crate) encoding: Option<&'static encoding_rs::Encoding>,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            io_backend: IoBackendPreference::Auto,
            encoding: None,
        }
    }
}

/// Builder for [`OpenOptions`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenOptionsBuilder {
    io_backend: IoBackendPreference,
    encoding: Option<&'static encoding_rs::Encoding>,
}

impl OpenOptions {
    /// Starts a builder with the default options.
    #[must_use]
    pub const fn builder() -> OpenOptionsBuilder {
        OpenOptionsBuilder::new()
    }
}

impl OpenOptionsBuilder {
    /// Starts a builder with the default options.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            io_backend: IoBackendPreference::Auto,
            encoding: None,
        }
    }

    /// Reads the file's text in `label`'s encoding instead of the one the file declares:
    /// cell values, column names, labels and format names alike. For files whose header
    /// names the wrong encoding, a common case being UTF-8 data in a file marked latin1.
    /// [`DatasetMetadata::encoding`](crate::DatasetMetadata::encoding) then reports the
    /// encoding used.
    ///
    /// `label` is a WHATWG label (`utf-8`, `windows-1252`, `latin1`), one of SAS's names
    /// for the Windows code pages (`wlatin1`, `wlatin2`, ...), or a Mac encoding.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Unsupported`](crate::Error::Unsupported) for a label not recognised.
    pub fn encoding(mut self, label: &str) -> crate::error::Result<Self> {
        let encoding = crate::encoding::encoding_for_label(label).ok_or_else(|| {
            crate::error::Error::unsupported(format!("unknown encoding {label:?}"))
        })?;
        self.encoding = Some(encoding);
        Ok(self)
    }

    /// Sets how the file's bytes are read. The default is [`IoBackendPreference::Auto`].
    #[must_use]
    pub const fn io_backend(mut self, io_backend: IoBackendPreference) -> Self {
        self.io_backend = io_backend;
        self
    }

    /// Builds the options.
    #[must_use]
    pub const fn build(self) -> OpenOptions {
        OpenOptions {
            io_backend: self.io_backend,
            encoding: self.encoding,
        }
    }
}

impl Default for OpenOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// How cells are turned into values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeMode {
    /// Cells are not decoded. Only [`ScanBuilder::visit_raw_rows`](crate::ScanBuilder::visit_raw_rows) accepts this mode.
    Raw,
    /// Numbers, dates and strings are decoded to typed values. The default.
    Typed,
    /// Numeric columns come out as the stored `f64`, whatever their logical type, and character columns as bytes. Nothing is trimmed, converted or reinterpreted.
    TypedLossless,
}

/// What a scan does with bytes that are invalid in the file's declared encoding.
///
/// Two variants because there are two behaviours. Every decision site tests for `Strict` and
/// treats everything else as lossy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Utf8ValidationMode {
    /// Replace what will not decode and carry on, optionally repairing mojibake
    /// (see [`MojibakePolicy`]). The default, and what the `*Lenient` decode kernels implement.
    #[default]
    Lenient,
    /// Fail the scan on the first cell that does not decode cleanly.
    Strict,
}

/// Whether text that was written as UTF-8 into a file declared in a single-byte encoding is repaired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MojibakePolicy {
    /// A cell in a single-byte encoded file whose bytes are valid UTF-8 is read as UTF-8. The default.
    Auto,
    /// Cells are always decoded with the file's declared encoding.
    Off,
}

/// Whether repeated string values are staged through a lookup of values already seen while a batch is decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DictionaryStaging {
    /// The reader decides, from the string decode path and the column width. The default.
    Auto,
    /// Never stage.
    Off,
    /// Stage every string column that has a nonzero width.
    On,
}

/// How whitespace around string values is handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrimMode {
    /// Keep the bytes as stored. A string of only spaces is read as an empty string.
    Preserve,
    /// Remove trailing spaces and NUL bytes. The default.
    RTrim,
    /// Remove spaces and NUL bytes from both ends.
    Strip,
}

/// Options for decoding string columns. Build one with [`StringDecodeOptions::builder`]. The defaults are right-trimmed strings, lenient UTF-8 handling, mojibake repair and automatic dictionary staging.
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

/// Builder for [`StringDecodeOptions`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StringDecodeOptionsBuilder {
    trim_mode: TrimMode,
    utf8_validation: Utf8ValidationMode,
    mojibake_fix: MojibakePolicy,
    dictionary_staging: DictionaryStaging,
}

impl StringDecodeOptions {
    /// Starts a builder with the default options.
    #[must_use]
    pub const fn builder() -> StringDecodeOptionsBuilder {
        StringDecodeOptionsBuilder::new()
    }
}

impl StringDecodeOptionsBuilder {
    /// Starts a builder with the default options.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            trim_mode: TrimMode::RTrim,
            utf8_validation: Utf8ValidationMode::Lenient,
            mojibake_fix: MojibakePolicy::Auto,
            dictionary_staging: DictionaryStaging::Auto,
        }
    }

    /// Sets how whitespace around strings is handled.
    #[must_use]
    pub const fn trim_mode(mut self, trim_mode: TrimMode) -> Self {
        self.trim_mode = trim_mode;
        self
    }

    /// Sets what happens to bytes that are invalid in the file's encoding.
    #[must_use]
    pub const fn utf8_validation(mut self, utf8_validation: Utf8ValidationMode) -> Self {
        self.utf8_validation = utf8_validation;
        self
    }

    /// Sets whether UTF-8 text in single-byte encoded files is repaired.
    #[must_use]
    pub const fn mojibake_fix(mut self, mojibake_fix: MojibakePolicy) -> Self {
        self.mojibake_fix = mojibake_fix;
        self
    }

    /// Sets whether repeated strings are staged through a lookup.
    #[must_use]
    pub const fn dictionary_staging(mut self, dictionary_staging: DictionaryStaging) -> Self {
        self.dictionary_staging = dictionary_staging;
        self
    }

    /// Builds the options.
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

/// Whether date, datetime and time columns are decoded to [`SasDate`](crate::SasDate), [`SasDateTime`](crate::SasDateTime) and [`SasTime`](crate::SasTime), or left as the stored number. All three default to `true`. Build one with [`TemporalDecodeOptions::builder`].
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

/// Builder for [`TemporalDecodeOptions`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemporalDecodeOptionsBuilder {
    decode_dates: bool,
    decode_datetimes: bool,
    decode_times: bool,
}

impl TemporalDecodeOptions {
    /// Starts a builder with the default options.
    #[must_use]
    pub const fn builder() -> TemporalDecodeOptionsBuilder {
        TemporalDecodeOptionsBuilder::new()
    }
}

impl TemporalDecodeOptionsBuilder {
    /// Starts a builder with the default options.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            decode_dates: true,
            decode_datetimes: true,
            decode_times: true,
        }
    }

    /// Sets whether date columns are decoded to dates.
    #[must_use]
    pub const fn decode_dates(mut self, decode_dates: bool) -> Self {
        self.decode_dates = decode_dates;
        self
    }

    /// Sets whether datetime columns are decoded to datetimes.
    #[must_use]
    pub const fn decode_datetimes(mut self, decode_datetimes: bool) -> Self {
        self.decode_datetimes = decode_datetimes;
        self
    }

    /// Sets whether time columns are decoded to times.
    #[must_use]
    pub const fn decode_times(mut self, decode_times: bool) -> Self {
        self.decode_times = decode_times;
        self
    }

    /// Builds the options.
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

/// Whether batches are delivered in file order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderingMode {
    /// Batches arrive in file order. The default.
    Stable,
    /// Batches may arrive in any order, as the worker threads finish.
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
    /// `scan::builder`.
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

/// The target size of a batch in a batch scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchHint {
    /// The reader chooses. It aims for at least 4096 rows and caps the decoded size of a batch. The default.
    Auto,
    /// At most this many rows per batch. 0 is treated as 1.
    Rows(usize),
    /// About this many decoded bytes per batch, estimated from the row length on disk. At least one row.
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

/// Which rows a scan covers. Row numbers are 0-based.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowSelection {
    /// Every row.
    All,
    /// The rows from `start` up to but not including `end`.
    Range {
        /// The first row.
        start: crate::types::RowIndex,
        /// The row after the last one.
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
