#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoBackendPreference {
    Auto,
    MmapPreferred,
    BufferedPreferred,
    BufferedOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefetchPolicy {
    Auto,
    Off,
    Sequential,
    Aggressive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageCachePolicy {
    Auto,
    None,
    Bounded { pages: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationMode {
    Strict,
    Permissive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenOptions {
    pub(crate) io_backend: IoBackendPreference,
    pub(crate) prefetch: PrefetchPolicy,
    pub(crate) page_cache: PageCachePolicy,
    pub(crate) validation: ValidationMode,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            io_backend: IoBackendPreference::Auto,
            prefetch: PrefetchPolicy::Auto,
            page_cache: PageCachePolicy::Auto,
            validation: ValidationMode::Strict,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenOptionsBuilder {
    io_backend: IoBackendPreference,
    prefetch: PrefetchPolicy,
    page_cache: PageCachePolicy,
    validation: ValidationMode,
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
            prefetch: PrefetchPolicy::Auto,
            page_cache: PageCachePolicy::Auto,
            validation: ValidationMode::Strict,
        }
    }

    #[must_use]
    pub const fn io_backend(mut self, io_backend: IoBackendPreference) -> Self {
        self.io_backend = io_backend;
        self
    }

    #[must_use]
    pub const fn prefetch(mut self, prefetch: PrefetchPolicy) -> Self {
        self.prefetch = prefetch;
        self
    }

    #[must_use]
    pub const fn page_cache(mut self, page_cache: PageCachePolicy) -> Self {
        self.page_cache = page_cache;
        self
    }

    #[must_use]
    pub const fn validation(mut self, validation: ValidationMode) -> Self {
        self.validation = validation;
        self
    }

    #[must_use]
    pub const fn build(self) -> OpenOptions {
        OpenOptions {
            io_backend: self.io_backend,
            prefetch: self.prefetch,
            page_cache: self.page_cache,
            validation: self.validation,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Utf8ValidationMode {
    Auto,
    Strict,
    Off,
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
            utf8_validation: Utf8ValidationMode::Auto,
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
            utf8_validation: Utf8ValidationMode::Auto,
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
    /// Use every logical core (`available_parallelism`), clamped to the available work.
    /// A caller running several scans at once should divide the machine between them with
    /// [`Parallelism::Threads`].
    Auto,
    /// One thread. Decode runs on the calling thread.
    None,
    /// Exactly this many threads, uncapped — for callers that know their own workload.
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
    /// Never use the column-major path (default).
    Off,
    /// Use the column-major path whenever its preconditions hold; row-major otherwise.
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
