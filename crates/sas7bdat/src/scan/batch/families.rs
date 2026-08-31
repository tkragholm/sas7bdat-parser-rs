//! Compiling a batch decode plan: which family each projected column belongs to.
//!
//! Everything here runs once per scan, before any file bytes are read. The output is a
//! [`BatchDecodePlan`]: per-column family membership, the flags the row loop branches on, and
//! the per-row cell counts used to size buffers.
//!
//! The families exist so the row loop can dispatch per *column group* rather than per cell.
//! `BatchPlanFlags` is what makes that cheap — one `u16` test per family per row instead of a
//! match on every column.

use super::{
    CompiledColumnPlan, Error, MAX_STAGED_STRING_WIDTH, Result, RowDecodePlan, ScanBuilder,
};
use crate::options::{DictionaryStaging, TrimMode};
use crate::scan::plan::{ColumnMaterializationKind, CompiledDecodeKernel};
use crate::scan::row_decode::StringDecodeKernel;

#[derive(Debug, Clone)]
pub(crate) struct BatchDecodePlan {
    pub(crate) row_plan: RowDecodePlan,
    pub(crate) column_kinds: Vec<ColumnMaterializationKind>,
    pub(crate) families: BatchColumnFamilies,
    pub(crate) staged_numeric_ops: Vec<StagedNumericOp>,
    pub(crate) staged_string_lookup_indices: Vec<usize>,
    pub(crate) direct_utf8_owned_mode: Option<DirectUtf8OwnedMode>,
    pub(crate) flags: BatchPlanFlags,
    pub(crate) staged_numeric_cells_per_row: u64,
    pub(crate) direct_raw_bytes_cells_per_row: u64,
    pub(crate) direct_utf8_single_byte_cells_per_row: u64,
    pub(crate) direct_utf8_borrowed_cells_per_row: u64,
    pub(crate) direct_utf8_owned_cells_per_row: u64,
    pub(crate) fallback_cells_per_row: u64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct BatchColumnFamilies {
    pub(crate) staged_numeric: Vec<usize>,
    pub(crate) direct_raw_bytes: Vec<usize>,
    pub(crate) direct_utf8_single_byte: Vec<usize>,
    pub(crate) direct_utf8_borrowed: Vec<usize>,
    pub(crate) direct_utf8_owned: Vec<usize>,
    pub(crate) fallback: Vec<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DirectUtf8OwnedMode {
    Utf8Lenient,
    EncodedStrict,
    EncodedLenient,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct BatchPlanFlags(u16);

impl BatchPlanFlags {
    pub(crate) const USE_SINGLE_BYTE_UTF8_FAMILY: u16 = 1 << 0;
    pub(crate) const ALL_COLUMNS_STAGED_NUMERIC: u16 = 1 << 1;
    pub(crate) const NEEDS_OWNED_STRING_SCRATCH: u16 = 1 << 2;
    pub(crate) const HAS_STAGED_NUMERIC: u16 = 1 << 3;
    pub(crate) const HAS_DIRECT_RAW_BYTES: u16 = 1 << 4;
    pub(crate) const HAS_DIRECT_UTF8_SINGLE_BYTE: u16 = 1 << 5;
    pub(crate) const HAS_DIRECT_UTF8_BORROWED: u16 = 1 << 6;
    pub(crate) const HAS_DIRECT_UTF8_OWNED: u16 = 1 << 7;
    pub(crate) const HAS_FALLBACK: u16 = 1 << 8;
    pub(crate) const HAS_FAST_PATH_STAGED_PLUS_UTF8_OWNED: u16 = 1 << 9;

    pub(crate) const fn has(self, bit: u16) -> bool {
        (self.0 & bit) != 0
    }

    pub(crate) const fn set(&mut self, bit: u16, enabled: bool) {
        if enabled {
            self.0 |= bit;
        } else {
            self.0 &= !bit;
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct StagedNumericOp {
    pub(crate) idx: usize,
    pub(crate) start: usize,
    pub(crate) end: usize,
}

impl BatchDecodePlan {
    pub(crate) fn new(builder: &ScanBuilder<'_>, row_plan: RowDecodePlan) -> Result<Self> {
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
        let staged_string_lookup_indices =
            compile_staged_string_lookup_indices(&families, &row_plan);

        let staged_numeric_cells_per_row =
            u64::try_from(staged_numeric_ops.len()).unwrap_or(u64::MAX);
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
            staged_string_lookup_indices,
            direct_utf8_owned_mode,
            flags,
            staged_numeric_cells_per_row,
            direct_raw_bytes_cells_per_row,
            direct_utf8_single_byte_cells_per_row,
            direct_utf8_borrowed_cells_per_row,
            direct_utf8_owned_cells_per_row,
            fallback_cells_per_row,
        })
    }

    /// Only the tests ask this now: the column-major gates moved to
    /// [`Self::can_fill_span_column_major`], which is the weaker condition a mixed plan can
    /// also satisfy.
    #[cfg(test)]
    pub(crate) const fn all_columns_staged_numeric(&self) -> bool {
        self.flags.has(BatchPlanFlags::ALL_COLUMNS_STAGED_NUMERIC)
    }

    /// Whether a contiguous span can be filled column-major at all: there is a staged-numeric
    /// family to tile. An all-numeric plan tiles everything; a mixed plan tiles its numerics
    /// and fills the remaining families row by row over the same span.
    pub(crate) const fn can_fill_span_column_major(&self) -> bool {
        self.flags.has(BatchPlanFlags::HAS_STAGED_NUMERIC)
    }

    #[cfg(test)]
    pub(crate) const fn needs_owned_string_scratch(&self) -> bool {
        self.flags.has(BatchPlanFlags::NEEDS_OWNED_STRING_SCRATCH)
    }
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

fn compile_plan_flags(families: &BatchColumnFamilies, row_plan: &RowDecodePlan) -> BatchPlanFlags {
    let has_staged_numeric = !families.staged_numeric.is_empty();
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

fn compile_staged_string_lookup_indices(
    families: &BatchColumnFamilies,
    row_plan: &RowDecodePlan,
) -> Vec<usize> {
    let allow_staging = match row_plan.string_options.dictionary_staging {
        DictionaryStaging::Off => false,
        DictionaryStaging::On => true,
        DictionaryStaging::Auto => {
            !matches!(row_plan.string_kernel, StringDecodeKernel::Utf8Lenient)
                && !matches!(row_plan.string_options.trim_mode, TrimMode::Preserve)
        }
    };
    if !allow_staging {
        return Vec::new();
    }
    let mut indices = Vec::with_capacity(families.direct_utf8_owned.len());
    for &idx in &families.direct_utf8_owned {
        let column = &row_plan.columns[idx];
        let width_ok = match row_plan.string_options.dictionary_staging {
            DictionaryStaging::On => column.width > 0,
            DictionaryStaging::Auto | DictionaryStaging::Off => {
                column.width > 0 && column.width <= MAX_STAGED_STRING_WIDTH
            }
        };
        if matches!(column.kernel, CompiledDecodeKernel::Utf8) && width_ok {
            indices.push(idx);
        }
    }
    indices
}

pub(crate) fn compile_batch_column_families(
    columns: &[CompiledColumnPlan],
    column_kinds: &[ColumnMaterializationKind],
    string_kernel: StringDecodeKernel,
    enable_single_byte_utf8: bool,
) -> BatchColumnFamilies {
    let mut families = BatchColumnFamilies::default();
    for (idx, (column, kind)) in columns.iter().zip(column_kinds.iter().copied()).enumerate() {
        // Every numeric kernel compiles a tile mode (`plan::compile_numeric_tile_mode` returns
        // `None` only for `Utf8` and `RawBytes`), so this claims all of them and the match
        // below only ever sees string and byte columns. Keep the two in step: a numeric kernel
        // that stopped compiling a tile would silently land in `fallback`, which is correct but
        // slow.
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

pub(crate) const fn column_materialization_kind(
    kernel: CompiledDecodeKernel,
    _width: u32,
) -> ColumnMaterializationKind {
    match kernel {
        // Always I64, regardless of on-disk width: truncated numerics (3-7 bytes)
        // still decode to full f64 values, and the staged materializer only ever
        // produces an i64 buffer. Width-dependent I32 would make the declared
        // schema disagree with the materialized batch.
        CompiledDecodeKernel::Integer => ColumnMaterializationKind::I64,
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
