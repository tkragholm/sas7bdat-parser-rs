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
    pub io_backend: IoBackendPreference,
    pub prefetch: PrefetchPolicy,
    pub page_cache: PageCachePolicy,
    pub validation: ValidationMode,
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
pub struct StringDecodeOptions {
    pub trim_fixed_width: bool,
    pub utf8_validation: Utf8ValidationMode,
    pub mojibake_fix: MojibakePolicy,
    pub dictionary_staging: DictionaryStaging,
}

impl Default for StringDecodeOptions {
    fn default() -> Self {
        Self {
            trim_fixed_width: true,
            utf8_validation: Utf8ValidationMode::Auto,
            mojibake_fix: MojibakePolicy::Auto,
            dictionary_staging: DictionaryStaging::Auto,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemporalDecodeOptions {
    pub decode_dates: bool,
    pub decode_datetimes: bool,
    pub decode_times: bool,
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
pub enum OrderingMode {
    Stable,
    Unordered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Parallelism {
    Auto,
    None,
    Threads(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchHint {
    Auto,
    Rows(usize),
    Bytes(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowSelection {
    All,
    Range { start: u64, end: u64 },
}
