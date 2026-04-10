use super::{
    ColumnBuffer, ColumnMaterializationKind, CompiledColumnPlan, CompiledDecodeKernel,
    DateNumericValue, DateTimeNumericValue, DecodedUtf8BatchValue, Error, NumericTileMode,
    OwnedColumnBuffer, OwnedColumnarBatch, PlannedCell, Result, RowDecodePlan,
    SAS_NUMERIC_MISSING_SENTINEL, SasDate, SasDateTime, SasTime, ScanBuilder, StringDecodeKernel,
    TimeNumericValue, TrimMode, TrimmedString, TypedNumericValue, classify_date_numeric_value,
    classify_datetime_numeric_value, classify_time_numeric_value, classify_typed_numeric_value,
    decode_numeric_cell, materialize_staged_numeric_column, numeric_bits,
    numeric_bits_is_missing, staged_numeric_raw_bits_from_planned_cell, trim_and_classify_for_mode,
};
use crate::define_owned_column_enum;
use crate::{BLANK_ID, DictionaryStaging};
use encoding_rs::WINDOWS_1252;

#[derive(Debug)]
pub(super) struct BatchDecodePlan {
    pub(super) row_plan: RowDecodePlan,
    pub(super) column_kinds: Vec<ColumnMaterializationKind>,
    pub(super) families: BatchColumnFamilies,
    pub(super) staged_numeric_ops: Vec<StagedNumericOp>,
    pub(super) direct_numeric_integer: Vec<usize>,
    pub(super) direct_numeric_floatlike: Vec<usize>,
    pub(super) direct_numeric_date: Vec<usize>,
    pub(super) direct_numeric_datetime: Vec<usize>,
    pub(super) direct_numeric_time: Vec<usize>,
    pub(super) direct_utf8_owned_mode: Option<DirectUtf8OwnedMode>,
    pub(super) flags: BatchPlanFlags,
    pub(super) staged_numeric_cells_per_row: u64,
    pub(super) direct_numeric_cells_per_row: u64,
    pub(super) direct_raw_bytes_cells_per_row: u64,
    pub(super) direct_utf8_single_byte_cells_per_row: u64,
    pub(super) direct_utf8_borrowed_cells_per_row: u64,
    pub(super) direct_utf8_owned_cells_per_row: u64,
    pub(super) fallback_cells_per_row: u64,
}

#[derive(Debug, Clone, Default)]
pub(super) struct BatchColumnFamilies {
    pub(super) staged_numeric: Vec<usize>,
    pub(super) direct_numeric: Vec<usize>,
    pub(super) direct_raw_bytes: Vec<usize>,
    pub(super) direct_utf8_single_byte: Vec<usize>,
    pub(super) direct_utf8_borrowed: Vec<usize>,
    pub(super) direct_utf8_owned: Vec<usize>,
    pub(super) fallback: Vec<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DirectUtf8OwnedMode {
    Utf8Lenient,
    EncodedStrict,
    EncodedLenient,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct BatchPlanFlags(u16);

impl BatchPlanFlags {
    const USE_SINGLE_BYTE_UTF8_FAMILY: u16 = 1 << 0;
    const ALL_COLUMNS_STAGED_NUMERIC: u16 = 1 << 1;
    const NEEDS_OWNED_STRING_SCRATCH: u16 = 1 << 2;
    const HAS_STAGED_NUMERIC: u16 = 1 << 3;
    const HAS_DIRECT_NUMERIC: u16 = 1 << 4;
    const HAS_DIRECT_RAW_BYTES: u16 = 1 << 5;
    const HAS_DIRECT_UTF8_SINGLE_BYTE: u16 = 1 << 6;
    const HAS_DIRECT_UTF8_BORROWED: u16 = 1 << 7;
    const HAS_DIRECT_UTF8_OWNED: u16 = 1 << 8;
    const HAS_FALLBACK: u16 = 1 << 9;
    const HAS_FAST_PATH_STAGED_PLUS_UTF8_OWNED: u16 = 1 << 10;

    const fn has(self, bit: u16) -> bool {
        (self.0 & bit) != 0
    }

    const fn set(&mut self, bit: u16, enabled: bool) {
        if enabled {
            self.0 |= bit;
        } else {
            self.0 &= !bit;
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct StagedNumericOp {
    pub(super) idx: usize,
    pub(super) start: usize,
    pub(super) end: usize,
}

const DICT_CAPACITY: usize = 512;
const DICT_SLOT_MASK: usize = DICT_CAPACITY - 1;
const MAX_DICT_ENTRIES: usize = 200;
const MAX_STAGED_STRING_WIDTH: u32 = 20;
const INLINE_DICT_KEY_BYTES: usize = 16;
const DICT_ID_NONE: u32 = u32::MAX;
const DICT_DISABLE_MIN_LOOKUPS: u32 = 512;
const DICT_DISABLE_MIN_LOOKUPS_ZERO_PROMOTIONS: u32 = 256;
const DICT_DISABLE_MIN_PROMOTION_BPS: u32 = 800; // 8%

#[derive(Debug, Clone, Copy)]
struct DictSlot {
    fingerprint: u16, // 0 = empty
    entry_idx: u16,
}

impl DictSlot {
    const EMPTY: Self = Self {
        fingerprint: 0,
        entry_idx: 0,
    };
}

#[derive(Debug, Clone, Copy)]
enum DictEntryState {
    SeenOnce,
    Interned,
}

#[derive(Debug, Clone)]
struct DictEntry {
    raw_start: u32,
    raw_end: u32,
    inline_len: u8,
    inline_key: [u8; INLINE_DICT_KEY_BYTES],
    state: DictEntryState,
    utf8_is_raw: bool,
    utf8_start: u32,
    utf8_end: u32,
}

#[derive(Debug, Clone, Copy)]
enum StageLookupHit {
    SeenOnce(u16),
    Interned(u16),
}

#[derive(Debug, Clone, Copy)]
enum TrimmedCellClass<'a> {
    Blank,
    Ascii(&'a [u8]),
    NonAscii(TrimmedString<'a>),
}

/// Per-column open-address hash table mapping semantic string bytes -> staged entry.
/// Entries are first staged as `SeenOnce`, then promoted to `Interned` on second sighting.
#[derive(Debug)]
struct StagedStringLookup {
    slots: Vec<DictSlot>,
    entries: Vec<DictEntry>,
    /// Concatenated raw bytes for staged dictionary keys.
    raw_data: Vec<u8>,
    /// Concatenated UTF-8 bytes for each unique entry.
    utf8_data: Vec<u8>,
    seen: u32,
    promoted: u32,
    disabled: bool,
    recent_hashes: [u32; 4],
    recent_entries: [u16; 4],
    recent_valid: [u8; 4],
}

impl StagedStringLookup {
    fn new() -> Self {
        Self {
            slots: vec![DictSlot::EMPTY; DICT_CAPACITY],
            entries: Vec::new(),
            raw_data: Vec::new(),
            utf8_data: Vec::new(),
            seen: 0,
            promoted: 0,
            disabled: false,
            recent_hashes: [0; 4],
            recent_entries: [0; 4],
            recent_valid: [0; 4],
        }
    }

    #[inline]
    const fn should_use(&self) -> bool {
        !self.disabled
    }

    #[inline]
    const fn observe_lookup(&mut self) {
        self.seen = self.seen.saturating_add(1);
        if self.promoted == 0 && self.seen >= DICT_DISABLE_MIN_LOOKUPS_ZERO_PROMOTIONS {
            self.disabled = true;
            return;
        }
        if self.seen >= DICT_DISABLE_MIN_LOOKUPS
            && self.promoted.saturating_mul(10_000)
                < self.seen.saturating_mul(DICT_DISABLE_MIN_PROMOTION_BPS)
        {
            self.disabled = true;
        }
    }

    #[inline]
    fn fnv1a(bytes: &[u8]) -> u32 {
        let mut h = 0x811c_9dc5_u32;
        for &b in bytes {
            h ^= u32::from(b);
            h = h.wrapping_mul(0x0100_0193);
        }
        h
    }

    #[inline]
    fn key_hash_and_fingerprint(key: &[u8]) -> (u32, u16) {
        let h = Self::fnv1a(key);
        let fp = (h >> 16) as u16 | 1; // force non-zero
        (h, fp)
    }

    #[inline]
    const fn remember_recent(&mut self, hash: u32, entry_idx: u16) {
        let slot = (hash as usize) & 3;
        self.recent_hashes[slot] = hash;
        self.recent_entries[slot] = entry_idx;
        self.recent_valid[slot] = 1;
    }

    /// Returns staged state for `key` if present.
    #[inline]
    fn lookup(&mut self, key: &[u8]) -> Option<StageLookupHit> {
        if self.disabled {
            return None;
        }
        let (h, fp) = Self::key_hash_and_fingerprint(key);
        let recent_slot = (h as usize) & 3;
        if self.recent_valid[recent_slot] != 0 && self.recent_hashes[recent_slot] == h {
            let entry_idx = self.recent_entries[recent_slot];
            let (matched, state) = {
                let entry = &self.entries[entry_idx as usize];
                (self.entry_key_matches(entry, key), entry.state)
            };
            if matched {
                return match state {
                    DictEntryState::SeenOnce => Some(StageLookupHit::SeenOnce(entry_idx)),
                    DictEntryState::Interned => Some(StageLookupHit::Interned(entry_idx)),
                };
            }
        }

        let mut slot = (h as usize) & DICT_SLOT_MASK;
        loop {
            let s = self.slots[slot];
            if s.fingerprint == 0 {
                return None; // empty slot -> not in table
            }
            if s.fingerprint == fp {
                let (matched, state) = {
                    let entry = &self.entries[s.entry_idx as usize];
                    (self.entry_key_matches(entry, key), entry.state)
                };
                if matched {
                    self.remember_recent(h, s.entry_idx);
                    return match state {
                        DictEntryState::SeenOnce => Some(StageLookupHit::SeenOnce(s.entry_idx)),
                        DictEntryState::Interned => Some(StageLookupHit::Interned(s.entry_idx)),
                    };
                }
            }
            slot = (slot + 1) & DICT_SLOT_MASK;
        }
    }

    #[inline]
    fn entry_raw_bytes<'a>(&'a self, entry: &DictEntry) -> &'a [u8] {
        let start = entry.raw_start as usize;
        let end = entry.raw_end as usize;
        &self.raw_data[start..end]
    }

    #[inline]
    fn entry_key_matches(&self, entry: &DictEntry, key: &[u8]) -> bool {
        if key.len() <= INLINE_DICT_KEY_BYTES && usize::from(entry.inline_len) == key.len() {
            return entry.inline_key[..key.len()] == *key;
        }
        self.entry_raw_bytes(entry) == key
    }

    fn interned_utf8(&self, entry_idx: u16) -> &[u8] {
        let entry = &self.entries[entry_idx as usize];
        if entry.utf8_is_raw {
            return self.entry_raw_bytes(entry);
        }
        let start = entry.utf8_start as usize;
        let end = entry.utf8_end as usize;
        &self.utf8_data[start..end]
    }

    fn insert_seen_once(&mut self, key: &[u8]) -> Option<u16> {
        if self.disabled {
            return None;
        }
        if key.len() > MAX_STAGED_STRING_WIDTH as usize {
            return None;
        }
        if self.entries.len() >= MAX_DICT_ENTRIES {
            return None;
        }
        let (h, fp) = Self::key_hash_and_fingerprint(key);
        let mut slot = (h as usize) & DICT_SLOT_MASK;
        loop {
            if self.slots[slot].fingerprint == 0 {
                break;
            }
            slot = (slot + 1) & DICT_SLOT_MASK;
        }
        let entry_idx = u16::try_from(self.entries.len()).ok()?;
        let raw_start = u32::try_from(self.raw_data.len()).ok()?;
        self.raw_data.extend_from_slice(key);
        let raw_end = u32::try_from(self.raw_data.len()).ok()?;
        let mut inline_key = [0_u8; INLINE_DICT_KEY_BYTES];
        let inline_len = if key.len() <= INLINE_DICT_KEY_BYTES {
            inline_key[..key.len()].copy_from_slice(key);
            u8::try_from(key.len()).expect("inline key length is capped")
        } else {
            0
        };
        self.entries.push(DictEntry {
            raw_start,
            raw_end,
            inline_len,
            inline_key,
            state: DictEntryState::SeenOnce,
            utf8_is_raw: false,
            utf8_start: 0,
            utf8_end: 0,
        });
        self.slots[slot] = DictSlot {
            fingerprint: fp,
            entry_idx,
        };
        self.remember_recent(h, entry_idx);
        Some(entry_idx)
    }

    #[inline]
    fn promote_interned(&mut self, entry_idx: u16, utf8: &[u8], utf8_is_raw: bool) {
        let (start, end) = if utf8_is_raw {
            (0, 0)
        } else {
            let start = u32::try_from(self.utf8_data.len()).expect("dict utf8 data within u32");
            self.utf8_data.extend_from_slice(utf8);
            let end = u32::try_from(self.utf8_data.len()).expect("dict utf8 data within u32");
            (start, end)
        };
        let entry = &mut self.entries[entry_idx as usize];
        entry.state = DictEntryState::Interned;
        entry.utf8_is_raw = utf8_is_raw;
        entry.utf8_start = start;
        entry.utf8_end = end;
        self.promoted = self.promoted.saturating_add(1);
    }
}

#[derive(Debug)]
pub(super) struct BatchAccumulator {
    plan: BatchDecodePlan,
    target_rows: usize,
    capacity_hint_rows: usize,
    row_base: Option<u64>,
    row_count: usize,
    columns: Vec<OwnedBatchColumnBuilder>,
    owned_strings: Vec<String>,
    utf8_decode_scratch: String,
    staged_string_lookups: Vec<Option<StagedStringLookup>>,
    counters: BatchFamilyCounters,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct BatchFamilyCounters {
    pub(super) staged_numeric: u64,
    pub(super) direct_numeric: u64,
    pub(super) direct_raw_bytes: u64,
    pub(super) direct_utf8_single_byte: u64,
    pub(super) direct_utf8_borrowed: u64,
    pub(super) direct_utf8_owned: u64,
    pub(super) direct_utf8_owned_interned_hits: u64,
    pub(super) direct_utf8_owned_seen_once_promotions: u64,
    pub(super) fallback: u64,
}

define_owned_column_enum! {
    #[derive(Debug)]
    pub(super) enum OwnedBatchColumnBuilder {
        StagedNumeric {
            raw_bits: Vec<u64>,
            mode: NumericTileMode,
            has_missing: bool,
        },
    }
}

impl BatchDecodePlan {
    pub(super) fn new(builder: &ScanBuilder<'_>) -> Result<Self> {
        let row_plan = RowDecodePlan::new(builder)?;
        let column_kinds = row_plan
            .columns
            .iter()
            .map(|column| column_materialization_kind(column.kernel, column.width))
            .collect::<Vec<_>>();
        let enable_single_byte_utf8 = !matches!(
            builder.ds.metadata().compression,
            crate::CompressionKind::None
        );
        let families = compile_batch_column_families(
            &row_plan.columns,
            &column_kinds,
            row_plan.string_kernel,
            enable_single_byte_utf8,
        );
        let direct_utf8_owned_mode =
            resolve_direct_utf8_owned_mode(&families, row_plan.string_kernel)?;
        let staged_numeric_ops = families
            .staged_numeric
            .iter()
            .copied()
            .map(|idx| {
                let column = &row_plan.columns[idx];
                StagedNumericOp {
                    idx,
                    start: column.start,
                    end: column.end,
                }
            })
            .collect::<Vec<_>>();
        let DirectNumericGroups {
            integer: direct_numeric_integer,
            floatlike: direct_numeric_floatlike,
            date: direct_numeric_date,
            datetime: direct_numeric_datetime,
            time: direct_numeric_time,
        } = compile_direct_numeric_groups(&families, &row_plan);

        let staged_numeric_cells_per_row =
            u64::try_from(staged_numeric_ops.len()).unwrap_or(u64::MAX);
        let direct_numeric_cells_per_row =
            u64::try_from(families.direct_numeric.len()).unwrap_or(u64::MAX);
        let direct_raw_bytes_cells_per_row =
            u64::try_from(families.direct_raw_bytes.len()).unwrap_or(u64::MAX);
        let direct_utf8_single_byte_cells_per_row =
            u64::try_from(families.direct_utf8_single_byte.len()).unwrap_or(u64::MAX);
        let direct_utf8_borrowed_cells_per_row =
            u64::try_from(families.direct_utf8_borrowed.len()).unwrap_or(u64::MAX);
        let direct_utf8_owned_cells_per_row =
            u64::try_from(families.direct_utf8_owned.len()).unwrap_or(u64::MAX);
        let fallback_cells_per_row = u64::try_from(families.fallback.len()).unwrap_or(u64::MAX);
        let flags = compile_plan_flags(&families, &row_plan);

        Ok(Self {
            row_plan,
            column_kinds,
            families,
            staged_numeric_ops,
            direct_numeric_integer,
            direct_numeric_floatlike,
            direct_numeric_date,
            direct_numeric_datetime,
            direct_numeric_time,
            direct_utf8_owned_mode,
            flags,
            staged_numeric_cells_per_row,
            direct_numeric_cells_per_row,
            direct_raw_bytes_cells_per_row,
            direct_utf8_single_byte_cells_per_row,
            direct_utf8_borrowed_cells_per_row,
            direct_utf8_owned_cells_per_row,
            fallback_cells_per_row,
        })
    }

    #[cfg(test)]
    pub(super) const fn all_columns_staged_numeric(&self) -> bool {
        self.flags.has(BatchPlanFlags::ALL_COLUMNS_STAGED_NUMERIC)
    }

    #[cfg(test)]
    pub(super) const fn needs_owned_string_scratch(&self) -> bool {
        self.flags.has(BatchPlanFlags::NEEDS_OWNED_STRING_SCRATCH)
    }
}

#[derive(Debug, Default)]
struct DirectNumericGroups {
    integer: Vec<usize>,
    floatlike: Vec<usize>,
    date: Vec<usize>,
    datetime: Vec<usize>,
    time: Vec<usize>,
}

fn resolve_direct_utf8_owned_mode(
    families: &BatchColumnFamilies,
    string_kernel: StringDecodeKernel,
) -> Result<Option<DirectUtf8OwnedMode>> {
    if families.direct_utf8_owned.is_empty() {
        return Ok(None);
    }
    let mode = match string_kernel {
        StringDecodeKernel::Utf8Strict => {
            return Err(Error::unsupported(
                "strict UTF-8 must not compile owned utf8 batch family",
            ));
        }
        StringDecodeKernel::Utf8Lenient => DirectUtf8OwnedMode::Utf8Lenient,
        StringDecodeKernel::EncodedStrict => DirectUtf8OwnedMode::EncodedStrict,
        StringDecodeKernel::EncodedLenient => DirectUtf8OwnedMode::EncodedLenient,
    };
    Ok(Some(mode))
}

fn compile_direct_numeric_groups(
    families: &BatchColumnFamilies,
    row_plan: &RowDecodePlan,
) -> DirectNumericGroups {
    let mut groups = DirectNumericGroups::default();
    for &idx in &families.direct_numeric {
        let column = &row_plan.columns[idx];
        match column.kernel {
            CompiledDecodeKernel::Integer => groups.integer.push(idx),
            CompiledDecodeKernel::Float
            | CompiledDecodeKernel::NumericLossless
            | CompiledDecodeKernel::DateAsNumeric
            | CompiledDecodeKernel::DateTimeAsNumeric
            | CompiledDecodeKernel::TimeAsNumeric => groups.floatlike.push(idx),
            CompiledDecodeKernel::Date => groups.date.push(idx),
            CompiledDecodeKernel::DateTime => groups.datetime.push(idx),
            CompiledDecodeKernel::Time => groups.time.push(idx),
            _ => unreachable!("non-numeric kernel in direct numeric family"),
        }
    }
    groups
}

fn compile_plan_flags(families: &BatchColumnFamilies, row_plan: &RowDecodePlan) -> BatchPlanFlags {
    let has_staged_numeric = !families.staged_numeric.is_empty();
    let has_direct_numeric = !families.direct_numeric.is_empty();
    let has_direct_raw_bytes = !families.direct_raw_bytes.is_empty();
    let has_direct_utf8_single_byte = !families.direct_utf8_single_byte.is_empty();
    let has_direct_utf8_borrowed = !families.direct_utf8_borrowed.is_empty();
    let has_direct_utf8_owned = !families.direct_utf8_owned.is_empty();
    let has_fallback = !families.fallback.is_empty();
    let use_single_byte_utf8_family = has_direct_utf8_single_byte;
    let all_columns_staged_numeric =
        !row_plan.columns.is_empty() && families.staged_numeric.len() == row_plan.columns.len();
    let needs_owned_string_scratch = families
        .fallback
        .iter()
        .any(|&idx| matches!(row_plan.columns[idx].kernel, CompiledDecodeKernel::Utf8));
    let has_fast_path_staged_plus_utf8_owned = has_staged_numeric
        && has_direct_utf8_owned
        && !has_direct_numeric
        && !has_direct_raw_bytes
        && !has_direct_utf8_single_byte
        && !has_direct_utf8_borrowed
        && !has_fallback
        && !needs_owned_string_scratch;

    let mut flags = BatchPlanFlags::default();
    flags.set(
        BatchPlanFlags::USE_SINGLE_BYTE_UTF8_FAMILY,
        use_single_byte_utf8_family,
    );
    flags.set(
        BatchPlanFlags::ALL_COLUMNS_STAGED_NUMERIC,
        all_columns_staged_numeric,
    );
    flags.set(
        BatchPlanFlags::NEEDS_OWNED_STRING_SCRATCH,
        needs_owned_string_scratch,
    );
    flags.set(BatchPlanFlags::HAS_STAGED_NUMERIC, has_staged_numeric);
    flags.set(BatchPlanFlags::HAS_DIRECT_NUMERIC, has_direct_numeric);
    flags.set(BatchPlanFlags::HAS_DIRECT_RAW_BYTES, has_direct_raw_bytes);
    flags.set(
        BatchPlanFlags::HAS_DIRECT_UTF8_SINGLE_BYTE,
        has_direct_utf8_single_byte,
    );
    flags.set(
        BatchPlanFlags::HAS_DIRECT_UTF8_BORROWED,
        has_direct_utf8_borrowed,
    );
    flags.set(BatchPlanFlags::HAS_DIRECT_UTF8_OWNED, has_direct_utf8_owned);
    flags.set(BatchPlanFlags::HAS_FALLBACK, has_fallback);
    flags.set(
        BatchPlanFlags::HAS_FAST_PATH_STAGED_PLUS_UTF8_OWNED,
        has_fast_path_staged_plus_utf8_owned,
    );
    flags
}

fn build_staged_string_lookups(plan: &BatchDecodePlan) -> Vec<Option<StagedStringLookup>> {
    let mut lookups = Vec::with_capacity(plan.row_plan.columns.len());
    let allow_staging = match plan.row_plan.string_options.dictionary_staging {
        DictionaryStaging::Off => false,
        DictionaryStaging::On => true,
        DictionaryStaging::Auto => {
            !matches!(
                plan.direct_utf8_owned_mode,
                Some(DirectUtf8OwnedMode::Utf8Lenient)
            ) && !matches!(plan.row_plan.string_options.trim_mode, TrimMode::Preserve)
        }
    };
    for (idx, column) in plan.row_plan.columns.iter().enumerate() {
        let width_ok = match plan.row_plan.string_options.dictionary_staging {
            DictionaryStaging::On => column.width > 0,
            DictionaryStaging::Auto | DictionaryStaging::Off => {
                column.width > 0 && column.width <= MAX_STAGED_STRING_WIDTH
            }
        };
        let should_stage = allow_staging
            && plan.families.direct_utf8_owned.contains(&idx)
            && width_ok
            && matches!(column.kernel, CompiledDecodeKernel::Utf8);
        if should_stage {
            lookups.push(Some(StagedStringLookup::new()));
        } else {
            lookups.push(None);
        }
    }
    lookups
}

impl BatchAccumulator {
    pub(super) fn new(
        plan: BatchDecodePlan,
        target_rows: usize,
        capacity_hint_rows: usize,
    ) -> Self {
        let staged_string_lookups = build_staged_string_lookups(&plan);
        let mut columns: Vec<OwnedBatchColumnBuilder> = plan
            .row_plan
            .columns
            .iter()
            .zip(plan.column_kinds.iter().copied())
            .map(|(column, kind)| {
                OwnedBatchColumnBuilder::with_capacity_hint(
                    kind,
                    capacity_hint_rows,
                    column.width,
                    column.numeric_tile,
                    if matches!(column.kernel, CompiledDecodeKernel::Utf8)
                        && matches!(
                            plan.row_plan.string_kernel,
                            StringDecodeKernel::EncodedStrict | StringDecodeKernel::EncodedLenient
                        )
                    {
                        2
                    } else {
                        1
                    },
                )
            })
            .collect();
        for &idx in &plan.families.direct_utf8_owned {
            if staged_string_lookups.get(idx).is_some_and(Option::is_some)
                && let Some(OwnedBatchColumnBuilder::Utf8 { dictionary_ids, .. }) =
                    columns.get_mut(idx)
            {
                dictionary_ids.replace(Vec::with_capacity(capacity_hint_rows.max(1)));
            }
        }
        Self {
            plan,
            target_rows: target_rows.max(1),
            capacity_hint_rows: capacity_hint_rows.max(1),
            row_base: None,
            row_count: 0,
            columns,
            owned_strings: Vec::new(),
            utf8_decode_scratch: String::new(),
            staged_string_lookups,
            counters: BatchFamilyCounters::default(),
        }
    }

    pub(super) const fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    pub(super) const fn is_full(&self) -> bool {
        self.row_count >= self.target_rows
    }

    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    fn push_all_staged_numeric(&mut self, row: &[u8]) -> Result<()> {
        self.push_staged_numeric_family(row)
    }

    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    fn push_staged_numeric_family(&mut self, row: &[u8]) -> Result<()> {
        self.counters.staged_numeric += self.plan.staged_numeric_cells_per_row;
        for op in &self.plan.staged_numeric_ops {
            let idx = op.idx;
            let batch_column = &mut self.columns[idx];
            let raw = if op.start == op.end {
                SAS_NUMERIC_MISSING_SENTINEL
            } else {
                numeric_bits(&row[op.start..op.end], self.plan.row_plan.endianness)
            };
            let appended = batch_column.append_staged_numeric_bits_fast(raw);
            debug_assert!(appended, "compiled staged numeric batch must match builder");
            if !appended {
                return Err(Error::unsupported(
                    "compiled staged numeric batch plan did not match column builder",
                ));
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    fn push_direct_numeric_family(&mut self, row: &[u8]) -> Result<()> {
        self.counters.direct_numeric += self.plan.direct_numeric_cells_per_row;
        for &idx in &self.plan.direct_numeric_integer {
            let batch_column = &mut self.columns[idx];
            let column = &self.plan.row_plan.columns[idx];
            let slice = RowDecodePlan::slice_in_bounds(row, column);
            let number = decode_numeric_cell(slice, self.plan.row_plan.endianness);
            let appended = batch_column.append_integer_fast(number);
            debug_assert!(appended, "compiled direct numeric batch must match builder");
            if !appended {
                return Err(Error::unsupported(
                    "compiled direct numeric batch plan did not match column builder",
                ));
            }
        }
        for &idx in &self.plan.direct_numeric_floatlike {
            let batch_column = &mut self.columns[idx];
            let column = &self.plan.row_plan.columns[idx];
            let slice = RowDecodePlan::slice_in_bounds(row, column);
            let number = decode_numeric_cell(slice, self.plan.row_plan.endianness);
            let appended = batch_column.append_f64_fast(number);
            debug_assert!(appended, "compiled direct numeric batch must match builder");
            if !appended {
                return Err(Error::unsupported(
                    "compiled direct numeric batch plan did not match column builder",
                ));
            }
        }
        for &idx in &self.plan.direct_numeric_date {
            let batch_column = &mut self.columns[idx];
            let column = &self.plan.row_plan.columns[idx];
            let slice = RowDecodePlan::slice_in_bounds(row, column);
            let number = decode_numeric_cell(slice, self.plan.row_plan.endianness);
            let appended = batch_column.append_date_fast(number);
            debug_assert!(appended, "compiled direct numeric batch must match builder");
            if !appended {
                return Err(Error::unsupported(
                    "compiled direct numeric batch plan did not match column builder",
                ));
            }
        }
        for &idx in &self.plan.direct_numeric_datetime {
            let batch_column = &mut self.columns[idx];
            let column = &self.plan.row_plan.columns[idx];
            let slice = RowDecodePlan::slice_in_bounds(row, column);
            let number = decode_numeric_cell(slice, self.plan.row_plan.endianness);
            let appended = batch_column.append_datetime_fast(number);
            debug_assert!(appended, "compiled direct numeric batch must match builder");
            if !appended {
                return Err(Error::unsupported(
                    "compiled direct numeric batch plan did not match column builder",
                ));
            }
        }
        for &idx in &self.plan.direct_numeric_time {
            let batch_column = &mut self.columns[idx];
            let column = &self.plan.row_plan.columns[idx];
            let slice = RowDecodePlan::slice_in_bounds(row, column);
            let number = decode_numeric_cell(slice, self.plan.row_plan.endianness);
            let appended = batch_column.append_time_fast(number);
            debug_assert!(appended, "compiled direct numeric batch must match builder");
            if !appended {
                return Err(Error::unsupported(
                    "compiled direct numeric batch plan did not match column builder",
                ));
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    fn push_direct_raw_bytes_family(&mut self, row: &[u8]) -> Result<()> {
        self.counters.direct_raw_bytes += self.plan.direct_raw_bytes_cells_per_row;
        for &idx in &self.plan.families.direct_raw_bytes {
            let batch_column = &mut self.columns[idx];
            let column = &self.plan.row_plan.columns[idx];
            let appended = append_direct_raw_bytes_batch_column(
                &self.plan.row_plan,
                batch_column,
                column,
                row,
            )?;
            debug_assert!(appended, "compiled raw-bytes batch must match builder");
            if !appended {
                return Err(Error::unsupported(
                    "compiled raw-bytes batch plan did not match column builder",
                ));
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    fn push_direct_utf8_single_byte_family(&mut self, row: &[u8]) -> Result<()> {
        if self
            .plan
            .flags
            .has(BatchPlanFlags::USE_SINGLE_BYTE_UTF8_FAMILY)
        {
            self.counters.direct_utf8_single_byte +=
                self.plan.direct_utf8_single_byte_cells_per_row;
            for &idx in &self.plan.families.direct_utf8_single_byte {
                let batch_column = &mut self.columns[idx];
                let column = &self.plan.row_plan.columns[idx];
                let appended = append_direct_utf8_single_byte_batch_column(
                    &self.plan.row_plan,
                    batch_column,
                    column,
                    row,
                    &mut self.utf8_decode_scratch,
                )?;
                debug_assert!(
                    appended,
                    "compiled single-byte utf8 batch must match builder"
                );
                if !appended {
                    return Err(Error::unsupported(
                        "compiled single-byte utf8 batch plan did not match column builder",
                    ));
                }
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    fn push_direct_utf8_borrowed_family(&mut self, row: &[u8]) -> Result<()> {
        self.counters.direct_utf8_borrowed += self.plan.direct_utf8_borrowed_cells_per_row;
        for &idx in &self.plan.families.direct_utf8_borrowed {
            let batch_column = &mut self.columns[idx];
            let column = &self.plan.row_plan.columns[idx];
            let appended = append_direct_utf8_borrowed_batch_column(
                &self.plan.row_plan,
                batch_column,
                column,
                row,
            )?;
            debug_assert!(appended, "compiled borrowed utf8 batch must match builder");
            if !appended {
                return Err(Error::unsupported(
                    "compiled borrowed utf8 batch plan did not match column builder",
                ));
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    fn push_direct_utf8_owned_family(&mut self, row: &[u8]) -> Result<()> {
        if let Some(mode) = self.plan.direct_utf8_owned_mode {
            self.counters.direct_utf8_owned += self.plan.direct_utf8_owned_cells_per_row;
            let mut breakdown = DirectUtf8OwnedBreakdown::default();
            for &idx in &self.plan.families.direct_utf8_owned {
                let batch_column = &mut self.columns[idx];
                let column = &self.plan.row_plan.columns[idx];
                let staged_lookup = self.staged_string_lookups[idx].as_mut();
                let appended = match mode {
                    DirectUtf8OwnedMode::Utf8Lenient => append_direct_utf8_owned_batch_column(
                        (&self.plan.row_plan, column, row),
                        batch_column,
                        &mut self.utf8_decode_scratch,
                        staged_lookup,
                        |p, t, s| Ok(p.decode_utf8_lenient_trimmed_bytes_for_batch_direct(t, s)),
                        true,
                        &mut breakdown,
                    )?,
                    DirectUtf8OwnedMode::EncodedStrict => append_direct_utf8_owned_batch_column(
                        (&self.plan.row_plan, column, row),
                        batch_column,
                        &mut self.utf8_decode_scratch,
                        staged_lookup,
                        RowDecodePlan::decode_encoded_strict_trimmed_bytes_for_batch_direct,
                        false,
                        &mut breakdown,
                    )?,
                    DirectUtf8OwnedMode::EncodedLenient => append_direct_utf8_owned_batch_column(
                        (&self.plan.row_plan, column, row),
                        batch_column,
                        &mut self.utf8_decode_scratch,
                        staged_lookup,
                        |p, t, s| Ok(p.decode_encoded_lenient_trimmed_bytes_for_batch_direct(t, s)),
                        false,
                        &mut breakdown,
                    )?,
                };
                debug_assert!(appended, "compiled utf8 batch must match builder");
                if !appended {
                    return Err(Error::unsupported(
                        "compiled owned utf8 batch plan did not match column builder",
                    ));
                }
            }
            self.counters.direct_utf8_owned_interned_hits = self
                .counters
                .direct_utf8_owned_interned_hits
                .saturating_add(breakdown.interned_hits);
            self.counters.direct_utf8_owned_seen_once_promotions = self
                .counters
                .direct_utf8_owned_seen_once_promotions
                .saturating_add(breakdown.seen_once_promotions);
        }
        Ok(())
    }

    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    fn push_fallback_family(&mut self, row: &[u8]) -> Result<()> {
        self.counters.fallback += self.plan.fallback_cells_per_row;
        for &idx in &self.plan.families.fallback {
            let batch_column = &mut self.columns[idx];
            let column = &self.plan.row_plan.columns[idx];
            let cell =
                self.plan
                    .row_plan
                    .plan_cell_in_bounds(row, column, &mut self.owned_strings)?;
            batch_column.append(cell, &self.owned_strings)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    pub(super) fn push_row(&mut self, row_index: u64, row: &[u8]) -> Result<()> {
        if self.row_base.is_none() {
            self.row_base = Some(row_index);
        }

        self.plan.row_plan.validate_row_bounds(row)?;
        if self
            .plan
            .flags
            .has(BatchPlanFlags::ALL_COLUMNS_STAGED_NUMERIC)
        {
            self.push_all_staged_numeric(row)?;
            self.row_count += 1;
            return Ok(());
        }

        if self
            .plan
            .flags
            .has(BatchPlanFlags::HAS_FAST_PATH_STAGED_PLUS_UTF8_OWNED)
        {
            self.push_staged_numeric_family(row)?;
            self.push_direct_utf8_owned_family(row)?;
            self.row_count += 1;
            return Ok(());
        }

        if self
            .plan
            .flags
            .has(BatchPlanFlags::NEEDS_OWNED_STRING_SCRATCH)
        {
            self.owned_strings.clear();
        }

        if self.plan.flags.has(BatchPlanFlags::HAS_STAGED_NUMERIC) {
            self.push_staged_numeric_family(row)?;
        }
        if self.plan.flags.has(BatchPlanFlags::HAS_DIRECT_NUMERIC) {
            self.push_direct_numeric_family(row)?;
        }
        if self.plan.flags.has(BatchPlanFlags::HAS_DIRECT_RAW_BYTES) {
            self.push_direct_raw_bytes_family(row)?;
        }
        if self
            .plan
            .flags
            .has(BatchPlanFlags::HAS_DIRECT_UTF8_SINGLE_BYTE)
        {
            self.push_direct_utf8_single_byte_family(row)?;
        }
        if self
            .plan
            .flags
            .has(BatchPlanFlags::HAS_DIRECT_UTF8_BORROWED)
        {
            self.push_direct_utf8_borrowed_family(row)?;
        }
        if self.plan.flags.has(BatchPlanFlags::HAS_DIRECT_UTF8_OWNED) {
            self.push_direct_utf8_owned_family(row)?;
        }
        if self.plan.flags.has(BatchPlanFlags::HAS_FALLBACK) {
            self.push_fallback_family(row)?;
        }

        self.row_count += 1;
        Ok(())
    }

    #[cfg_attr(feature = "hotpath-profile", hotpath::measure)]
    pub(super) fn take_batch(&mut self) -> OwnedColumnarBatch {
        let row_base = self.row_base.unwrap_or(0);
        let row_count = self.row_count;
        let columns = std::mem::take(&mut self.columns)
            .into_iter()
            .map(OwnedBatchColumnBuilder::finish)
            .collect();
        self.row_base = None;
        self.row_count = 0;
        OwnedColumnarBatch {
            row_base: crate::types::RowIndex(row_base),
            row_count,
            columns,
        }
    }

    pub(super) fn reset_after_flush(&mut self) {
        self.columns = self
            .plan
            .row_plan
            .columns
            .iter()
            .zip(self.plan.column_kinds.iter().copied())
            .map(|(column, kind)| {
                OwnedBatchColumnBuilder::with_capacity_hint(
                    kind,
                    self.capacity_hint_rows,
                    column.width,
                    column.numeric_tile,
                    if matches!(column.kernel, CompiledDecodeKernel::Utf8)
                        && matches!(
                            self.plan.row_plan.string_kernel,
                            StringDecodeKernel::EncodedStrict | StringDecodeKernel::EncodedLenient
                        )
                    {
                        2
                    } else {
                        1
                    },
                )
            })
            .collect();
        for &idx in &self.plan.families.direct_utf8_owned {
            if self.staged_string_lookups.get(idx).is_some_and(Option::is_some)
                && let Some(OwnedBatchColumnBuilder::Utf8 { dictionary_ids, .. }) =
                    self.columns.get_mut(idx)
            {
                dictionary_ids.replace(Vec::with_capacity(self.capacity_hint_rows.max(1)));
            }
        }
        self.owned_strings.clear();
        self.utf8_decode_scratch.clear();
    }

    pub(super) const fn counters(&self) -> BatchFamilyCounters {
        self.counters
    }

    #[cfg(test)]
    pub(super) fn has_staged_string_lookup_for(&self, idx: usize) -> bool {
        self.staged_string_lookups
            .get(idx)
            .is_some_and(Option::is_some)
    }
}

impl OwnedBatchColumnBuilder {
    pub(super) fn with_capacity_hint(
        kind: ColumnMaterializationKind,
        target_rows: usize,
        width_hint: u32,
        numeric_tile: Option<NumericTileMode>,
        utf8_data_capacity_multiplier: usize,
    ) -> Self {
        let base_variable_capacity =
            target_rows.saturating_mul(usize::try_from(width_hint).unwrap_or(0));
        let utf8_variable_capacity =
            base_variable_capacity.saturating_mul(utf8_data_capacity_multiplier.max(1));
        match kind {
            ColumnMaterializationKind::I32 => Self::I32 {
                values: Vec::with_capacity(target_rows),
                valid: None,
            },
            ColumnMaterializationKind::I64 => Self::I64 {
                values: Vec::with_capacity(target_rows),
                valid: None,
            },
            ColumnMaterializationKind::F64 => Self::F64 {
                values: Vec::with_capacity(target_rows),
                valid: None,
            },
            ColumnMaterializationKind::Date => Self::Date {
                values: Vec::with_capacity(target_rows),
                valid: None,
            },
            ColumnMaterializationKind::DateTime => Self::DateTime {
                values: Vec::with_capacity(target_rows),
                valid: None,
            },
            ColumnMaterializationKind::Time => Self::Time {
                values: Vec::with_capacity(target_rows),
                valid: None,
            },
            ColumnMaterializationKind::Utf8 => Self::Utf8 {
                offsets: Vec::with_capacity(target_rows.saturating_add(1)),
                data: Vec::with_capacity(utf8_variable_capacity),
                valid: None,
                dictionary_ids: None,
            },
            ColumnMaterializationKind::RawBytes => Self::RawBytes {
                offsets: Vec::with_capacity(target_rows.saturating_add(1)),
                data: Vec::with_capacity(base_variable_capacity),
                valid: None,
            },
        }
        .with_numeric_tile(target_rows, numeric_tile)
        .with_initial_offset()
    }

    pub(super) fn with_numeric_tile(
        self,
        target_rows: usize,
        numeric_tile: Option<NumericTileMode>,
    ) -> Self {
        match (self, numeric_tile) {
            (_, Some(mode)) => Self::StagedNumeric {
                raw_bits: Vec::with_capacity(target_rows),
                mode,
                has_missing: false,
            },
            (builder, _) => builder,
        }
    }

    pub(super) fn with_initial_offset(mut self) -> Self {
        match &mut self {
            Self::Utf8 { offsets, .. } | Self::RawBytes { offsets, .. } => offsets.push(0),
            _ => {}
        }
        self
    }

    pub(super) fn append_integer_fast(&mut self, number: Option<f64>) -> bool {
        match self {
            Self::I32 { values, valid } => match classify_typed_numeric_value(number, true) {
                TypedNumericValue::Null => {
                    push_primitive_null(values, valid, 0);
                    true
                }
                TypedNumericValue::Int32(value32) => {
                    push_primitive_valid(values, valid, value32);
                    true
                }
                TypedNumericValue::Int64(_) | TypedNumericValue::Float64(_) => {
                    self.widen_integer_to_f64();
                    self.append_f64_fast(number)
                }
            },
            Self::I64 { values, valid } => match classify_typed_numeric_value(number, false) {
                TypedNumericValue::Null => {
                    push_primitive_null(values, valid, 0);
                    true
                }
                TypedNumericValue::Int32(value32) => {
                    push_primitive_valid(values, valid, i64::from(value32));
                    true
                }
                TypedNumericValue::Int64(value64) => {
                    push_primitive_valid(values, valid, value64);
                    true
                }
                TypedNumericValue::Float64(_) => {
                    self.widen_integer_to_f64();
                    self.append_f64_fast(number)
                }
            },
            Self::F64 { .. } => self.append_f64_fast(number),
            _ => false,
        }
    }

    pub(super) fn append_f64_fast(&mut self, number: Option<f64>) -> bool {
        match self {
            Self::F64 { values, valid } => {
                match number {
                    None => push_primitive_null(values, valid, 0.0),
                    Some(value) => push_primitive_valid(values, valid, value),
                }
                true
            }
            Self::StagedNumeric {
                raw_bits,
                mode: NumericTileMode::F64RawBits,
                has_missing,
            } => {
                let raw = number.map_or(SAS_NUMERIC_MISSING_SENTINEL, f64::to_bits);
                *has_missing |= numeric_bits_is_missing(raw);
                raw_bits.push(raw);
                true
            }
            _ => false,
        }
    }

    #[inline]
    pub(super) fn append_staged_numeric_bits_fast(&mut self, raw: u64) -> bool {
        match self {
            Self::StagedNumeric {
                raw_bits,
                has_missing,
                ..
            } => {
                *has_missing |= numeric_bits_is_missing(raw);
                raw_bits.push(raw);
                true
            }
            _ => false,
        }
    }

    pub(super) fn append_date_fast(&mut self, number: Option<f64>) -> bool {
        match self {
            Self::Date { values, valid } => match classify_date_numeric_value(number) {
                DateNumericValue::Null => {
                    push_primitive_null(
                        values,
                        valid,
                        SasDate {
                            days_since_sas_epoch: 0,
                        },
                    );
                    true
                }
                DateNumericValue::Date(value) => {
                    push_primitive_valid(values, valid, value);
                    true
                }
                DateNumericValue::Float64(_) => {
                    self.widen_temporal_to_f64();
                    self.append_f64_fast(number)
                }
            },
            Self::F64 { .. } => self.append_f64_fast(number),
            _ => false,
        }
    }

    pub(super) fn append_datetime_fast(&mut self, number: Option<f64>) -> bool {
        match self {
            Self::DateTime { values, valid } => match classify_datetime_numeric_value(number) {
                DateTimeNumericValue::Null => {
                    push_primitive_null(
                        values,
                        valid,
                        SasDateTime {
                            seconds_since_sas_epoch: 0,
                        },
                    );
                    true
                }
                DateTimeNumericValue::DateTime(value) => {
                    push_primitive_valid(values, valid, value);
                    true
                }
                DateTimeNumericValue::Float64(_) => {
                    self.widen_temporal_to_f64();
                    self.append_f64_fast(number)
                }
            },
            Self::F64 { .. } => self.append_f64_fast(number),
            _ => false,
        }
    }

    pub(super) fn append_time_fast(&mut self, number: Option<f64>) -> bool {
        match self {
            Self::Time { values, valid } => match classify_time_numeric_value(number) {
                TimeNumericValue::Null => {
                    push_primitive_null(
                        values,
                        valid,
                        SasTime {
                            seconds_since_midnight: 0,
                        },
                    );
                    true
                }
                TimeNumericValue::Time(value) => {
                    push_primitive_valid(values, valid, value);
                    true
                }
                TimeNumericValue::Float64(_) => {
                    self.widen_temporal_to_f64();
                    self.append_f64_fast(number)
                }
            },
            Self::F64 { .. } => self.append_f64_fast(number),
            _ => false,
        }
    }

    fn append_with_temporal_widening(
        &mut self,
        cell: PlannedCell,
        owned_strings: &[String],
    ) -> Result<()> {
        self.widen_temporal_to_f64();
        self.append(cell, owned_strings)
    }

    #[allow(clippy::too_many_lines)]
    pub(super) fn append(&mut self, cell: PlannedCell, owned_strings: &[String]) -> Result<()> {
        match self {
            Self::I32 { values, valid } => {
                match cell {
                    PlannedCell::Null => push_primitive_null(values, valid, 0),
                    PlannedCell::Int32(value) => push_primitive_valid(values, valid, value),
                    PlannedCell::Int64(value) => {
                        if let Ok(value32) = i32::try_from(value) {
                            push_primitive_valid(values, valid, value32);
                        } else {
                            self.widen_integer_to_f64();
                            return self.append(PlannedCell::Int64(value), owned_strings);
                        }
                    }
                    PlannedCell::Float64(value) => {
                        match classify_typed_numeric_value(Some(value), true) {
                            TypedNumericValue::Int32(value32) => {
                                push_primitive_valid(values, valid, value32);
                            }
                            TypedNumericValue::Float64(_) | TypedNumericValue::Int64(_) => {
                                self.widen_integer_to_f64();
                                return self.append(PlannedCell::Float64(value), owned_strings);
                            }
                            TypedNumericValue::Null => push_primitive_null(values, valid, 0),
                        }
                    }
                    other => return Err(unexpected_batch_cell("i32", other)),
                }
                Ok(())
            }
            Self::I64 { values, valid } => {
                match cell {
                    PlannedCell::Null => push_primitive_null(values, valid, 0),
                    PlannedCell::Int32(value) => {
                        push_primitive_valid(values, valid, i64::from(value));
                    }
                    PlannedCell::Int64(value) => push_primitive_valid(values, valid, value),
                    PlannedCell::Float64(value) => {
                        match classify_typed_numeric_value(Some(value), false) {
                            TypedNumericValue::Int32(value32) => {
                                push_primitive_valid(values, valid, i64::from(value32));
                            }
                            TypedNumericValue::Int64(value64) => {
                                push_primitive_valid(values, valid, value64);
                            }
                            TypedNumericValue::Float64(_) => {
                                self.widen_integer_to_f64();
                                return self.append(PlannedCell::Float64(value), owned_strings);
                            }
                            TypedNumericValue::Null => push_primitive_null(values, valid, 0),
                        }
                    }
                    other => return Err(unexpected_batch_cell("i64", other)),
                }
                Ok(())
            }
            Self::F64 { values, valid } => {
                match cell {
                    PlannedCell::Null => push_primitive_null(values, valid, 0.0),
                    PlannedCell::Int32(value) => {
                        push_primitive_valid(values, valid, f64::from(value));
                    }
                    PlannedCell::Int64(value) => {
                        #[allow(clippy::cast_precision_loss)]
                        let value_f64 = value as f64;
                        push_primitive_valid(values, valid, value_f64);
                    }
                    PlannedCell::Float64(value) => push_primitive_valid(values, valid, value),
                    other => return Err(unexpected_batch_cell("f64", other)),
                }
                Ok(())
            }
            Self::StagedNumeric { raw_bits, .. } => {
                raw_bits.push(staged_numeric_raw_bits_from_planned_cell(cell)?);
                Ok(())
            }
            Self::Date { values, valid } => match cell {
                PlannedCell::Null => {
                    push_primitive_null(
                        values,
                        valid,
                        SasDate {
                            days_since_sas_epoch: 0,
                        },
                    );
                    Ok(())
                }
                PlannedCell::Date(value) => {
                    push_primitive_valid(values, valid, value);
                    Ok(())
                }
                PlannedCell::Int32(_) | PlannedCell::Int64(_) | PlannedCell::Float64(_) => {
                    self.append_with_temporal_widening(cell, owned_strings)
                }
                other => Err(unexpected_batch_cell("date", other)),
            },
            Self::DateTime { values, valid } => match cell {
                PlannedCell::Null => {
                    push_primitive_null(
                        values,
                        valid,
                        SasDateTime {
                            seconds_since_sas_epoch: 0,
                        },
                    );
                    Ok(())
                }
                PlannedCell::DateTime(value) => {
                    push_primitive_valid(values, valid, value);
                    Ok(())
                }
                PlannedCell::Int32(_) | PlannedCell::Int64(_) | PlannedCell::Float64(_) => {
                    self.append_with_temporal_widening(cell, owned_strings)
                }
                other => Err(unexpected_batch_cell("datetime", other)),
            },
            Self::Time { values, valid } => match cell {
                PlannedCell::Null => {
                    push_primitive_null(
                        values,
                        valid,
                        SasTime {
                            seconds_since_midnight: 0,
                        },
                    );
                    Ok(())
                }
                PlannedCell::Time(value) => {
                    push_primitive_valid(values, valid, value);
                    Ok(())
                }
                PlannedCell::Int32(_) | PlannedCell::Int64(_) | PlannedCell::Float64(_) => {
                    self.append_with_temporal_widening(cell, owned_strings)
                }
                other => Err(unexpected_batch_cell("time", other)),
            },
            Self::Utf8 {
                offsets,
                data,
                valid,
                dictionary_ids,
            } => match cell {
                PlannedCell::Null => {
                    push_variable_null(offsets, data, valid);
                    push_dictionary_id(dictionary_ids, DICT_ID_NONE);
                    Ok(())
                }
                PlannedCell::StrBorrowed(value) => {
                    push_utf8_bytes_fast(offsets, data, valid, value.as_bytes())?;
                    push_dictionary_id(dictionary_ids, DICT_ID_NONE);
                    Ok(())
                }
                PlannedCell::StrOwned(index) => {
                    push_utf8_bytes_fast(
                        offsets,
                        data,
                        valid,
                        owned_strings
                            .get(index)
                            .ok_or_else(|| Error::unsupported("owned string index out of range"))?
                            .as_bytes(),
                    )?;
                    push_dictionary_id(dictionary_ids, DICT_ID_NONE);
                    Ok(())
                }
                other => Err(unexpected_batch_cell("utf8", other)),
            },
            Self::RawBytes {
                offsets,
                data,
                valid,
            } => match cell {
                PlannedCell::Null => {
                    push_variable_null(offsets, data, valid);
                    Ok(())
                }
                PlannedCell::Bytes(value) => {
                    push_variable_valid(offsets, data, valid, value)?;
                    Ok(())
                }
                other => Err(unexpected_batch_cell("raw-bytes", other)),
            },
        }
    }

    pub(super) fn widen_temporal_to_f64(&mut self) {
        let widened = match std::mem::replace(
            self,
            Self::F64 {
                values: Vec::new(),
                valid: None,
            },
        ) {
            Self::Date { values, valid } => Self::F64 {
                values: values
                    .into_iter()
                    .map(|value| f64::from(value.days_since_sas_epoch))
                    .collect(),
                valid,
            },
            Self::DateTime { values, valid } => Self::F64 {
                values: values
                    .into_iter()
                    .map(|value| {
                        #[allow(clippy::cast_precision_loss)]
                        let v = value.seconds_since_sas_epoch as f64;
                        v
                    })
                    .collect(),
                valid,
            },
            Self::Time { values, valid } => Self::F64 {
                values: values
                    .into_iter()
                    .map(|value| {
                        #[allow(clippy::cast_precision_loss)]
                        let v = value.seconds_since_midnight as f64;
                        v
                    })
                    .collect(),
                valid,
            },
            other => other,
        };
        *self = widened;
    }

    pub(super) fn widen_integer_to_f64(&mut self) {
        let widened = match std::mem::replace(
            self,
            Self::F64 {
                values: Vec::new(),
                valid: None,
            },
        ) {
            Self::I32 { values, valid } => Self::F64 {
                values: values.into_iter().map(f64::from).collect(),
                valid,
            },
            Self::I64 { values, valid } => Self::F64 {
                values: values
                    .into_iter()
                    .map(|value| {
                        #[allow(clippy::cast_precision_loss)]
                        let v = value as f64;
                        v
                    })
                    .collect(),
                valid,
            },
            other => other,
        };
        *self = widened;
    }

    pub(super) fn finish(self) -> OwnedColumnBuffer {
        match self {
            Self::I32 { values, valid } => OwnedColumnBuffer::I32 { values, valid },
            Self::I64 { values, valid } => OwnedColumnBuffer::I64 { values, valid },
            Self::F64 { values, valid } => OwnedColumnBuffer::F64 { values, valid },
            Self::StagedNumeric {
                raw_bits,
                mode,
                has_missing,
            } => materialize_staged_numeric_column(raw_bits, mode, has_missing),
            Self::Date { values, valid } => OwnedColumnBuffer::Date { values, valid },
            Self::DateTime { values, valid } => OwnedColumnBuffer::DateTime { values, valid },
            Self::Time { values, valid } => OwnedColumnBuffer::Time { values, valid },
            Self::Utf8 {
                offsets,
                data,
                valid,
                dictionary_ids,
            } => OwnedColumnBuffer::Utf8 {
                offsets,
                data,
                valid,
                dictionary_ids,
            },
            Self::RawBytes {
                offsets,
                data,
                valid,
            } => OwnedColumnBuffer::RawBytes {
                offsets,
                data,
                valid,
            },
        }
    }
}

#[inline]
fn push_utf8_bytes_fast(
    offsets: &mut Vec<u32>,
    data: &mut Vec<u8>,
    valid: &mut Option<Vec<u64>>,
    value: &[u8],
) -> Result<()> {
    if valid.is_none() {
        push_variable_valid_without_validity(offsets, data, value)
    } else {
        push_variable_valid(offsets, data, valid, value)
    }
}

#[inline]
fn push_dictionary_id(dictionary_ids: &mut Option<Vec<u32>>, dictionary_id: u32) {
    if let Some(ids) = dictionary_ids.as_mut() {
        ids.push(dictionary_id);
    }
}

#[inline]
fn staged_entry_to_dictionary_id(entry_idx: u16) -> u32 {
    u32::from(entry_idx).saturating_add(1)
}

#[inline]
fn classify_trimmed_cell(slice: &[u8], mode: TrimMode) -> TrimmedCellClass<'_> {
    let trimmed = trim_and_classify_for_mode(slice, mode);
    let bytes = trimmed.bytes;
    let is_blank = bytes.is_empty()
        || (matches!(mode, TrimMode::Preserve) && bytes.iter().all(|&byte| byte == b' '));
    if is_blank {
        TrimmedCellClass::Blank
    } else if trimmed.is_ascii {
        TrimmedCellClass::Ascii(bytes)
    } else {
        TrimmedCellClass::NonAscii(trimmed)
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct DirectUtf8OwnedBreakdown {
    interned_hits: u64,
    seen_once_promotions: u64,
}

#[inline]
pub(super) fn append_direct_raw_bytes_batch_column(
    _row_plan: &RowDecodePlan,
    batch_column: &mut OwnedBatchColumnBuilder,
    column: &CompiledColumnPlan,
    row: &[u8],
) -> Result<bool> {
    let slice = RowDecodePlan::slice_in_bounds(row, column);
    match batch_column {
        OwnedBatchColumnBuilder::RawBytes {
            offsets,
            data,
            valid,
        } if matches!(column.kernel, CompiledDecodeKernel::RawBytes) => {
            push_variable_valid(offsets, data, valid, slice)?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

#[inline]
pub(super) fn append_direct_utf8_single_byte_batch_column(
    row_plan: &RowDecodePlan,
    batch_column: &mut OwnedBatchColumnBuilder,
    column: &CompiledColumnPlan,
    row: &[u8],
    utf8_decode_scratch: &mut String,
) -> Result<bool> {
    match (column.kernel, batch_column) {
        (
            CompiledDecodeKernel::Utf8,
            OwnedBatchColumnBuilder::Utf8 {
                offsets,
                data,
                valid,
                dictionary_ids,
            },
        ) if column.width == 1 => {
            let byte = row[column.start];
            let trim_space = match row_plan.string_options.trim_mode {
                TrimMode::Preserve => false,
                TrimMode::RTrim | TrimMode::Strip => byte == b' ' || byte == 0,
            };
            let slice = if trim_space {
                &[][..]
            } else {
                &row[column.start..column.end]
            };

            if slice.is_empty() || byte.is_ascii() {
                push_variable_valid(offsets, data, valid, slice)?;
                push_dictionary_id(
                    dictionary_ids,
                    if slice.is_empty() {
                        BLANK_ID
                    } else {
                        DICT_ID_NONE
                    },
                );
                return Ok(true);
            }

            if row_plan.encoding == WINDOWS_1252
                && matches!(
                    row_plan.string_kernel,
                    StringDecodeKernel::EncodedStrict | StringDecodeKernel::EncodedLenient
                )
            {
                append_windows_1252_single_byte_utf8(
                    offsets,
                    data,
                    valid,
                    byte,
                    matches!(row_plan.string_kernel, StringDecodeKernel::EncodedStrict),
                )?;
                push_dictionary_id(dictionary_ids, DICT_ID_NONE);
                return Ok(true);
            }

            append_non_ascii_single_byte_utf8(
                row_plan,
                offsets,
                data,
                valid,
                dictionary_ids,
                slice,
                utf8_decode_scratch,
            )?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

#[inline]
fn append_non_ascii_single_byte_utf8(
    row_plan: &RowDecodePlan,
    offsets: &mut Vec<u32>,
    data: &mut Vec<u8>,
    valid: &mut Option<Vec<u64>>,
    dictionary_ids: &mut Option<Vec<u32>>,
    slice: &[u8],
    utf8_decode_scratch: &mut String,
) -> Result<()> {
    let trimmed = TrimmedString {
        bytes: slice,
        is_ascii: false,
    };
    match row_plan.string_kernel {
        StringDecodeKernel::Utf8Strict => {
            Err(Error::Decode(crate::error::DecodeError {
                message: "invalid UTF-8 in fixed-width string cell".to_owned(),
            }))
        }
        StringDecodeKernel::Utf8Lenient => {
            match row_plan.decode_utf8_lenient_trimmed_bytes_for_batch_direct(trimmed, utf8_decode_scratch)
            {
                DecodedUtf8BatchValue::Borrowed(bytes) => {
                    push_variable_valid(offsets, data, valid, bytes)?;
                }
                DecodedUtf8BatchValue::Scratch => {
                    push_variable_valid(offsets, data, valid, utf8_decode_scratch.as_bytes())?;
                }
            }
            push_dictionary_id(dictionary_ids, DICT_ID_NONE);
            Ok(())
        }
        StringDecodeKernel::EncodedStrict => {
            match row_plan.decode_encoded_strict_trimmed_bytes_for_batch_direct(trimmed, utf8_decode_scratch)?
            {
                DecodedUtf8BatchValue::Borrowed(bytes) => {
                    push_variable_valid(offsets, data, valid, bytes)?;
                }
                DecodedUtf8BatchValue::Scratch => {
                    push_variable_valid(offsets, data, valid, utf8_decode_scratch.as_bytes())?;
                }
            }
            push_dictionary_id(dictionary_ids, DICT_ID_NONE);
            Ok(())
        }
        StringDecodeKernel::EncodedLenient => {
            match row_plan.decode_encoded_lenient_trimmed_bytes_for_batch_direct(trimmed, utf8_decode_scratch)
            {
                DecodedUtf8BatchValue::Borrowed(bytes) => {
                    push_variable_valid(offsets, data, valid, bytes)?;
                }
                DecodedUtf8BatchValue::Scratch => {
                    push_variable_valid(offsets, data, valid, utf8_decode_scratch.as_bytes())?;
                }
            }
            push_dictionary_id(dictionary_ids, DICT_ID_NONE);
            Ok(())
        }
    }
}

#[inline]
fn append_windows_1252_single_byte_utf8(
    offsets: &mut Vec<u32>,
    data: &mut Vec<u8>,
    valid: &mut Option<Vec<u64>>,
    byte: u8,
    strict: bool,
) -> Result<()> {
    let codepoint =
        windows_1252_single_byte_to_codepoint(byte).unwrap_or(if strict { 0 } else { 0xFFFD });
    if strict && codepoint == 0 {
        return Err(Error::Decode(crate::error::DecodeError {
            message: "string decode failed under strict validation".to_owned(),
        }));
    }

    let scalar = char::from_u32(codepoint).ok_or_else(|| {
        Error::unsupported("windows-1252 single-byte decode produced invalid scalar")
    })?;
    let mut scratch = [0_u8; 4];
    let encoded = scalar.encode_utf8(&mut scratch);
    push_variable_valid(offsets, data, valid, encoded.as_bytes())
}

#[inline]
const fn windows_1252_single_byte_to_codepoint(byte: u8) -> Option<u32> {
    match byte {
        0x80 => Some(0x20AC),
        0x81 | 0x8D | 0x8F | 0x90 | 0x9D => None,
        0x82 => Some(0x201A),
        0x83 => Some(0x0192),
        0x84 => Some(0x201E),
        0x85 => Some(0x2026),
        0x86 => Some(0x2020),
        0x87 => Some(0x2021),
        0x88 => Some(0x02C6),
        0x89 => Some(0x2030),
        0x8A => Some(0x0160),
        0x8B => Some(0x2039),
        0x8C => Some(0x0152),
        0x8E => Some(0x017D),
        0x91 => Some(0x2018),
        0x92 => Some(0x2019),
        0x93 => Some(0x201C),
        0x94 => Some(0x201D),
        0x95 => Some(0x2022),
        0x96 => Some(0x2013),
        0x97 => Some(0x2014),
        0x98 => Some(0x02DC),
        0x99 => Some(0x2122),
        0x9A => Some(0x0161),
        0x9B => Some(0x203A),
        0x9C => Some(0x0153),
        0x9E => Some(0x017E),
        0x9F => Some(0x0178),
        _ => Some(byte as u32),
    }
}

#[inline]
pub(super) fn append_direct_utf8_borrowed_batch_column(
    row_plan: &RowDecodePlan,
    batch_column: &mut OwnedBatchColumnBuilder,
    column: &CompiledColumnPlan,
    row: &[u8],
) -> Result<bool> {
    let slice = RowDecodePlan::slice_in_bounds(row, column);
    match (column.kernel, batch_column) {
        (
            CompiledDecodeKernel::Utf8,
            OwnedBatchColumnBuilder::Utf8 {
                offsets,
                data,
                valid,
                dictionary_ids,
            },
        ) => {
            let bytes = row_plan.decode_string_bytes_for_batch_borrowed(slice)?;
            push_variable_valid(offsets, data, valid, bytes)?;
            push_dictionary_id(dictionary_ids, DICT_ID_NONE);
            Ok(true)
        }
        _ => Ok(false),
    }
}

#[inline]
fn append_direct_utf8_owned_batch_column(
    source: (&RowDecodePlan, &CompiledColumnPlan, &[u8]),
    batch_column: &mut OwnedBatchColumnBuilder,
    utf8_decode_scratch: &mut String,
    mut staged_lookup: Option<&mut StagedStringLookup>,
    decode_owned: impl for<'a> Fn(
        &'a RowDecodePlan,
        TrimmedString<'a>,
        &'a mut String,
    ) -> Result<DecodedUtf8BatchValue<'a>>,
    fast_valid_utf8_non_ascii: bool,
    breakdown: &mut DirectUtf8OwnedBreakdown,
) -> Result<bool> {
    let (row_plan, column, row) = source;
    let slice = RowDecodePlan::slice_in_bounds(row, column);
    match (column.kernel, batch_column) {
        (
            CompiledDecodeKernel::Utf8,
            OwnedBatchColumnBuilder::Utf8 {
                offsets,
                data,
                valid,
                dictionary_ids,
            },
        ) => {
            let (trimmed, slice) =
                match classify_trimmed_cell(slice, row_plan.string_options.trim_mode) {
                    TrimmedCellClass::Blank => {
                        push_variable_valid(offsets, data, valid, &[])?;
                        push_dictionary_id(dictionary_ids, BLANK_ID);
                        return Ok(true);
                    }
                    TrimmedCellClass::Ascii(bytes) => {
                        push_variable_valid(offsets, data, valid, bytes)?;
                        push_dictionary_id(dictionary_ids, DICT_ID_NONE);
                        return Ok(true);
                    }
                    TrimmedCellClass::NonAscii(trimmed) => (trimmed, trimmed.bytes),
                };

            if fast_valid_utf8_non_ascii && std::str::from_utf8(slice).is_ok() {
                push_variable_valid(offsets, data, valid, slice)?;
                push_dictionary_id(dictionary_ids, DICT_ID_NONE);
                return Ok(true);
            }

            if let Some(dict) = staged_lookup.as_deref_mut()
                && dict.should_use()
            {
                dict.observe_lookup();
                if let Some(hit) = dict.lookup(slice) {
                    match hit {
                        StageLookupHit::Interned(entry_idx) => {
                            let interned = dict.interned_utf8(entry_idx);
                            push_variable_valid(offsets, data, valid, interned)?;
                            breakdown.interned_hits = breakdown.interned_hits.saturating_add(1);
                            push_dictionary_id(
                                dictionary_ids,
                                staged_entry_to_dictionary_id(entry_idx),
                            );
                            return Ok(true);
                        }
                        StageLookupHit::SeenOnce(entry_idx) => {
                            breakdown.seen_once_promotions =
                                breakdown.seen_once_promotions.saturating_add(1);
                            match decode_owned(row_plan, trimmed, utf8_decode_scratch)? {
                                DecodedUtf8BatchValue::Borrowed(bytes) => {
                                    // Append once from freshly decoded bytes, then promote for subsequent hits.
                                    push_variable_valid(offsets, data, valid, bytes)?;
                                    dict.promote_interned(entry_idx, bytes, bytes == slice);
                                    push_dictionary_id(
                                        dictionary_ids,
                                        staged_entry_to_dictionary_id(entry_idx),
                                    );
                                }
                                DecodedUtf8BatchValue::Scratch => {
                                    // Avoid reloading from dictionary arena on the promotion row.
                                    let promoted_utf8 = utf8_decode_scratch.as_bytes();
                                    push_variable_valid(offsets, data, valid, promoted_utf8)?;
                                    dict.promote_interned(
                                        entry_idx,
                                        promoted_utf8,
                                        false,
                                    );
                                    push_dictionary_id(
                                        dictionary_ids,
                                        staged_entry_to_dictionary_id(entry_idx),
                                    );
                                }
                            }
                            return Ok(true);
                        }
                    }
                }
            }

            match decode_owned(row_plan, trimmed, utf8_decode_scratch)? {
                DecodedUtf8BatchValue::Borrowed(bytes) => {
                    if let Some(dict) = staged_lookup.as_mut() {
                        let _ = dict.insert_seen_once(slice);
                    }
                    push_variable_valid(offsets, data, valid, bytes)?;
                    push_dictionary_id(dictionary_ids, DICT_ID_NONE);
                }
                DecodedUtf8BatchValue::Scratch => {
                    if let Some(dict) = staged_lookup.as_mut() {
                        let _ = dict.insert_seen_once(slice);
                    }
                    push_variable_valid(offsets, data, valid, utf8_decode_scratch.as_bytes())?;
                    push_dictionary_id(dictionary_ids, DICT_ID_NONE);
                }
            }
            Ok(true)
        }
        _ => Ok(false),
    }
}

pub(super) fn compile_batch_column_families(
    columns: &[CompiledColumnPlan],
    column_kinds: &[ColumnMaterializationKind],
    string_kernel: StringDecodeKernel,
    enable_single_byte_utf8: bool,
) -> BatchColumnFamilies {
    let mut families = BatchColumnFamilies::default();
    for (idx, (column, kind)) in columns.iter().zip(column_kinds.iter().copied()).enumerate() {
        if column.numeric_tile.is_some() {
            families.staged_numeric.push(idx);
            continue;
        }

        match (column.kernel, kind) {
            (CompiledDecodeKernel::Utf8, ColumnMaterializationKind::Utf8)
                if enable_single_byte_utf8 && column.width == 1 =>
            {
                families.direct_utf8_single_byte.push(idx);
            }
            (
                CompiledDecodeKernel::Integer
                | CompiledDecodeKernel::Float
                | CompiledDecodeKernel::Date
                | CompiledDecodeKernel::DateAsNumeric
                | CompiledDecodeKernel::DateTime
                | CompiledDecodeKernel::DateTimeAsNumeric
                | CompiledDecodeKernel::Time
                | CompiledDecodeKernel::TimeAsNumeric
                | CompiledDecodeKernel::NumericLossless,
                ColumnMaterializationKind::I32
                | ColumnMaterializationKind::I64
                | ColumnMaterializationKind::F64
                | ColumnMaterializationKind::Date
                | ColumnMaterializationKind::DateTime
                | ColumnMaterializationKind::Time,
            ) => families.direct_numeric.push(idx),
            (CompiledDecodeKernel::RawBytes, ColumnMaterializationKind::RawBytes) => {
                families.direct_raw_bytes.push(idx);
            }
            (CompiledDecodeKernel::Utf8, ColumnMaterializationKind::Utf8) => match string_kernel {
                StringDecodeKernel::Utf8Strict => families.direct_utf8_borrowed.push(idx),
                StringDecodeKernel::Utf8Lenient
                | StringDecodeKernel::EncodedStrict
                | StringDecodeKernel::EncodedLenient => families.direct_utf8_owned.push(idx),
            },
            _ => families.fallback.push(idx),
        }
    }
    families
}

pub(super) const fn column_materialization_kind(
    kernel: CompiledDecodeKernel,
    width: u32,
) -> ColumnMaterializationKind {
    match kernel {
        CompiledDecodeKernel::Integer => {
            if width <= 4 {
                ColumnMaterializationKind::I32
            } else {
                ColumnMaterializationKind::I64
            }
        }
        CompiledDecodeKernel::Float
        | CompiledDecodeKernel::NumericLossless
        | CompiledDecodeKernel::DateAsNumeric
        | CompiledDecodeKernel::DateTimeAsNumeric
        | CompiledDecodeKernel::TimeAsNumeric => ColumnMaterializationKind::F64,
        CompiledDecodeKernel::Utf8 => ColumnMaterializationKind::Utf8,
        CompiledDecodeKernel::RawBytes => ColumnMaterializationKind::RawBytes,
        CompiledDecodeKernel::Date => ColumnMaterializationKind::Date,
        CompiledDecodeKernel::DateTime => ColumnMaterializationKind::DateTime,
        CompiledDecodeKernel::Time => ColumnMaterializationKind::Time,
    }
}

pub(super) fn borrow_column_buffers(columns: &[OwnedColumnBuffer]) -> Vec<ColumnBuffer<'_>> {
    columns.iter().map(OwnedColumnBuffer::as_borrowed).collect()
}

/// Set bit `pos` in the bit-packed validity word array (1 = valid).
#[inline]
fn set_valid_bit(bits: &mut Vec<u64>, pos: usize) {
    let word = pos / 64;
    let bit = pos % 64;
    if bits.len() <= word {
        bits.resize(word + 1, 0);
    }
    bits[word] |= 1u64 << bit;
}

/// Initialize a bit-packed validity vector for the first null at row `first_null_pos`.
/// Rows `0..first_null_pos` are set valid (1); row `first_null_pos` is null (0).
#[inline]
fn init_validity_first_null(first_null_pos: usize) -> Vec<u64> {
    let full_words = first_null_pos / 64;
    let bit_offset = first_null_pos % 64;
    let mut bits = Vec::with_capacity(full_words + 1);
    bits.extend(std::iter::repeat_n(u64::MAX, full_words));
    // Partial word: bits 0..bit_offset-1 set to 1, bit bit_offset stays 0.
    bits.push(if bit_offset == 0 {
        0u64
    } else {
        (1u64 << bit_offset) - 1
    });
    bits
}

#[inline]
pub(super) fn push_primitive_valid<T>(values: &mut Vec<T>, valid: &mut Option<Vec<u64>>, value: T) {
    let pos = values.len();
    values.push(value);
    if let Some(bits) = valid {
        set_valid_bit(bits, pos);
    }
}

#[inline]
pub(super) fn push_primitive_null<T: Copy>(
    values: &mut Vec<T>,
    valid: &mut Option<Vec<u64>>,
    default: T,
) {
    let pos = values.len();
    if valid.is_none() {
        *valid = Some(init_validity_first_null(pos));
    } else {
        let bits = valid.as_mut().expect("validity initialized");
        let word = pos / 64;
        if bits.len() <= word {
            bits.resize(word + 1, 0);
        }
        // bit at pos stays 0 (null)
    }
    values.push(default);
}

#[inline]
pub(super) fn push_variable_valid(
    offsets: &mut Vec<u32>,
    data: &mut Vec<u8>,
    valid: &mut Option<Vec<u64>>,
    value: &[u8],
) -> Result<()> {
    let pos = offsets.len().saturating_sub(1);
    data.extend_from_slice(value);
    let next_offset = u32::try_from(data.len())
        .map_err(|_| Error::unsupported("columnar variable buffer exceeds u32 offset range"))?;
    offsets.push(next_offset);
    if let Some(bits) = valid {
        set_valid_bit(bits, pos);
    }
    Ok(())
}

#[inline]
pub(super) fn push_variable_valid_without_validity(
    offsets: &mut Vec<u32>,
    data: &mut Vec<u8>,
    value: &[u8],
) -> Result<()> {
    let next_offset = (*offsets.last().unwrap_or(&0) as usize)
        .checked_add(value.len())
        .ok_or_else(|| Error::unsupported("columnar variable buffer exceeds platform usize"))?;
    let next_offset_u32 = u32::try_from(next_offset)
        .map_err(|_| Error::unsupported("columnar variable buffer exceeds u32 offset range"))?;
    data.extend_from_slice(value);
    offsets.push(next_offset_u32);
    Ok(())
}

#[inline]
pub(super) fn push_variable_null(
    offsets: &mut Vec<u32>,
    _data: &mut Vec<u8>,
    valid: &mut Option<Vec<u64>>,
) {
    let pos = offsets.len().saturating_sub(1);
    if valid.is_none() {
        *valid = Some(init_validity_first_null(pos));
    } else {
        let bits = valid.as_mut().expect("validity initialized");
        let word = pos / 64;
        if bits.len() <= word {
            bits.resize(word + 1, 0);
        }
        // bit at pos stays 0 (null)
    }
    let last = *offsets.last().unwrap_or(&0);
    offsets.push(last);
}

pub(super) fn unexpected_batch_cell(expected: &str, actual: PlannedCell<'_>) -> Error {
    Error::Decode(crate::error::DecodeError {
        message: format!("columnar decode expected {expected} cell but saw {actual:?}"),
    })
}
