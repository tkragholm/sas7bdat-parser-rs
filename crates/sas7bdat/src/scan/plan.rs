use super::raw::RawScanPlan;
use super::{
    BatchDecodePlan, BatchHint, ColumnMeta, DecodeMode, Encoding, Error, LogicalType,
    ProjectedColumnPlan, Result, RowDecodePlan, ScanBuilder, StringDecodeKernel,
    StringDecodeOptions, UTF_8, Utf8ValidationMode,
};
#[cfg(feature = "arrow")]
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
#[cfg(feature = "arrow")]
use std::collections::HashMap;
#[cfg(feature = "arrow")]
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct ScanPlan {
    pub(super) raw: RawScanPlan,
    pub(super) row: RowDecodePlan,
    pub(super) batch: BatchDecodePlan,
    pub(super) batch_row_capacity: usize,
    pub(super) capacity_hint_rows: usize,
}

impl ScanPlan {
    pub(super) fn new(builder: &ScanBuilder<'_>) -> Result<Self> {
        let raw = RawScanPlan::compile(builder);
        let projection = builder.projection;
        let projection_plan = projection.map(|projection| projection.inner.as_ref());
        let projection_names = projection.map(|projection| projection.names.as_ref());
        let row = RowDecodePlan::new_with_projection(builder, projection_plan, projection_names)?;
        let batch_row_capacity = resolve_batch_row_capacity(builder)?;
        let row_len = usize::from(builder.ds.layout.row_len).max(1);
        let capacity_hint_rows = effective_scan_row_capacity_hint(builder)
            .min(batch_row_capacity)
            .min(MAX_CAPACITY_HINT_ROWS)
            .min(MAX_BATCH_PREALLOC_BYTES / row_len)
            .max(1);
        let batch = BatchDecodePlan::new(builder, row.clone())?;
        Ok(Self {
            raw,
            row,
            batch,
            batch_row_capacity,
            capacity_hint_rows,
        })
    }
}

const AUTO_BATCH_ROWS_MIN: usize = 4096;

/// Ceilings on the batch pre-allocation hint.
///
/// The hint is advisory. It pre-sizes the batch column buffers and every one of them
/// grows on demand, so clamping it can only make a pathological file pre-allocate less
/// — it cannot change how many rows a batch holds or what a scan produces. That is what
/// makes bounding it here safe in a way that rejecting the input would not be.
///
/// It needs bounding because both inputs are file-controlled and neither is checked
/// geometrically: [`effective_scan_row_capacity_hint`] is the declared row count, which
/// this reader already knows can read `u32::MAX`, and `BatchHint::Auto` resolves
/// `batch_row_capacity` from the declared `rows_per_page`. With both large, a fuzz case
/// reached `Vec::with_capacity` for 876 GB.
///
/// Two ceilings, because either factor alone is insufficient. The row cap handles a
/// tiny `row_len`, where a byte budget would permit hundreds of millions of rows. The
/// byte budget handles wide rows, and it scales correctly with column count without
/// needing to know it: every column occupies at least one byte of the row, so
/// `columns * width` is at most `row_len` and the whole batch stays inside the budget.
const MAX_CAPACITY_HINT_ROWS: usize = 1 << 20;
const MAX_BATCH_PREALLOC_BYTES: usize = 256 << 20;
#[derive(Debug, Clone)]
pub(super) struct CompiledColumnPlan {
    pub(super) start: usize,
    pub(super) end: usize,
    pub(super) width: u32,
    pub(super) kernel: CompiledDecodeKernel,
    pub(super) numeric_tile: Option<NumericTileMode>,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum ColumnMaterializationKind {
    // No kernel maps to I32 today (Integer always materializes as I64), but the
    // builder/widening plumbing for it is kept for future width-aware narrowing.
    #[allow(dead_code)]
    I32,
    I64,
    F64,
    Date,
    DateTime,
    Time,
    Utf8,
    RawBytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NumericTileMode {
    F64RawBits,
    IntegerWidth8,
    Date,
    DateTime,
    Time,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum CompiledDecodeKernel {
    Utf8,
    RawBytes,
    Date,
    DateAsNumeric,
    DateTime,
    DateTimeAsNumeric,
    Time,
    TimeAsNumeric,
    Integer,
    Float,
    NumericLossless,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum OwnedCellMaterializationKind {
    Utf8,
    RawBytes,
    Date,
    DateAsNumeric,
    DateTime,
    DateTimeAsNumeric,
    Time,
    TimeAsNumeric,
    NumericTyped,
    NumericLossless,
}

pub(super) fn compile_column_plan(
    builder: &ScanBuilder<'_>,
    column: &ColumnMeta,
) -> Result<CompiledColumnPlan> {
    let start = usize::try_from(column.offset)
        .map_err(|_| Error::unsupported("column offset exceeds platform usize"))?;
    let width = column.physical_width;
    let width_usize = usize::try_from(width)
        .map_err(|_| Error::unsupported("column width exceeds platform usize"))?;
    let end = start
        .checked_add(width_usize)
        .ok_or_else(|| Error::unsupported("column end overflow"))?;
    let kernel = compile_decode_kernel(builder, column.logical_type);
    Ok(CompiledColumnPlan {
        start,
        end,
        width,
        kernel,
        numeric_tile: compile_numeric_tile_mode(kernel, width),
    })
}

pub(super) fn compile_compiled_projection_column_plan(
    builder: &ScanBuilder<'_>,
    column: &ProjectedColumnPlan,
) -> CompiledColumnPlan {
    let kernel = compile_decode_kernel(builder, column.logical_type);
    CompiledColumnPlan {
        start: usize::from(column.offset),
        end: usize::from(column.end),
        width: column.width,
        kernel,
        numeric_tile: compile_numeric_tile_mode(kernel, column.width),
    }
}

pub(super) const fn compile_numeric_tile_mode(
    kernel: CompiledDecodeKernel,
    _width: u32,
) -> Option<NumericTileMode> {
    match kernel {
        CompiledDecodeKernel::Float
        | CompiledDecodeKernel::NumericLossless
        | CompiledDecodeKernel::DateAsNumeric
        | CompiledDecodeKernel::DateTimeAsNumeric
        | CompiledDecodeKernel::TimeAsNumeric => Some(NumericTileMode::F64RawBits),
        CompiledDecodeKernel::Integer => Some(NumericTileMode::IntegerWidth8),
        CompiledDecodeKernel::Date => Some(NumericTileMode::Date),
        CompiledDecodeKernel::DateTime => Some(NumericTileMode::DateTime),
        CompiledDecodeKernel::Time => Some(NumericTileMode::Time),
        _ => None,
    }
}

pub(super) const fn compile_decode_kernel(
    builder: &ScanBuilder<'_>,
    logical_type: LogicalType,
) -> CompiledDecodeKernel {
    match builder.decode {
        DecodeMode::Typed => match logical_type {
            LogicalType::String => CompiledDecodeKernel::Utf8,
            LogicalType::Bytes => CompiledDecodeKernel::RawBytes,
            LogicalType::Date => {
                if builder.temporal_options.decode_dates {
                    CompiledDecodeKernel::Date
                } else {
                    CompiledDecodeKernel::DateAsNumeric
                }
            }
            LogicalType::DateTime => {
                if builder.temporal_options.decode_datetimes {
                    CompiledDecodeKernel::DateTime
                } else {
                    CompiledDecodeKernel::DateTimeAsNumeric
                }
            }
            LogicalType::Time => {
                if builder.temporal_options.decode_times {
                    CompiledDecodeKernel::Time
                } else {
                    CompiledDecodeKernel::TimeAsNumeric
                }
            }
            LogicalType::Integer => CompiledDecodeKernel::Integer,
            LogicalType::Float => CompiledDecodeKernel::Float,
        },
        DecodeMode::TypedLossless => match logical_type {
            LogicalType::String | LogicalType::Bytes => CompiledDecodeKernel::RawBytes,
            LogicalType::Date
            | LogicalType::DateTime
            | LogicalType::Time
            | LogicalType::Integer
            | LogicalType::Float => CompiledDecodeKernel::NumericLossless,
        },
        DecodeMode::Raw => CompiledDecodeKernel::RawBytes,
    }
}

pub(super) const fn compile_owned_materialization_kind(
    kernel: CompiledDecodeKernel,
) -> OwnedCellMaterializationKind {
    match kernel {
        CompiledDecodeKernel::Utf8 => OwnedCellMaterializationKind::Utf8,
        CompiledDecodeKernel::RawBytes => OwnedCellMaterializationKind::RawBytes,
        CompiledDecodeKernel::Date => OwnedCellMaterializationKind::Date,
        CompiledDecodeKernel::DateAsNumeric => OwnedCellMaterializationKind::DateAsNumeric,
        CompiledDecodeKernel::DateTime => OwnedCellMaterializationKind::DateTime,
        CompiledDecodeKernel::DateTimeAsNumeric => OwnedCellMaterializationKind::DateTimeAsNumeric,
        CompiledDecodeKernel::Time => OwnedCellMaterializationKind::Time,
        CompiledDecodeKernel::TimeAsNumeric => OwnedCellMaterializationKind::TimeAsNumeric,
        CompiledDecodeKernel::Integer | CompiledDecodeKernel::Float => {
            OwnedCellMaterializationKind::NumericTyped
        }
        CompiledDecodeKernel::NumericLossless => OwnedCellMaterializationKind::NumericLossless,
    }
}

pub(super) fn compile_string_decode_kernel(
    encoding: &'static Encoding,
    string_options: StringDecodeOptions,
) -> StringDecodeKernel {
    let strict = matches!(string_options.utf8_validation, Utf8ValidationMode::Strict);
    if encoding == UTF_8 {
        if strict {
            StringDecodeKernel::Utf8Strict
        } else {
            StringDecodeKernel::Utf8Lenient
        }
    } else if strict {
        StringDecodeKernel::EncodedStrict
    } else {
        StringDecodeKernel::EncodedLenient
    }
}

pub(super) fn resolve_batch_row_capacity(builder: &ScanBuilder<'_>) -> Result<usize> {
    match builder.batch_hint {
        BatchHint::Rows(rows) => Ok(rows.max(1)),
        BatchHint::Bytes(bytes) => {
            let row_len = usize::from(builder.ds.layout.row_len);
            Ok((bytes / row_len.max(1)).max(1))
        }
        BatchHint::Auto => {
            let rows_per_page = usize::try_from(builder.ds.layout.rows_per_page)
                .map_err(|_| Error::unsupported("rows per page exceeds platform usize"))?;
            Ok(rows_per_page.max(AUTO_BATCH_ROWS_MIN).max(1))
        }
    }
}

pub(super) fn effective_scan_row_capacity_hint(builder: &ScanBuilder<'_>) -> usize {
    // The window is unbounded for a whole-file scan, so the declared row count is what caps
    // the hint there; a bounded window caps it itself.
    let rows = builder
        .row_window()
        .len()
        .min(builder.ds.metadata.row_count);
    usize::try_from(rows).unwrap_or(usize::MAX).max(1)
}

/// Field metadata key marking a column that SAS declared with a TIME format.
///
/// The Arrow type for such a column is `Duration`, not `Time64` (see [`arrow_data_type`]),
/// which loses the *intent* that the number is a time of day even though it loses no data.
/// A consumer that knows its values are in `[0, 24h)` can use this to re-type the column as
/// a clock time — the i64 payload is bit-identical, so that is a free re-label, not a
/// re-encode. Parquet round-trips field metadata through its `ARROW:schema` key, so this
/// survives a `convert`.
#[cfg(feature = "arrow")]
pub const SAS_LOGICAL_TYPE_KEY: &str = "sas.logical_type";

#[cfg(feature = "arrow")]
pub(super) fn arrow_schema_for_plan(plan: &ScanPlan) -> SchemaRef {
    let fields = plan
        .row
        .names
        .iter()
        .zip(plan.batch.column_kinds.iter().copied())
        .map(|(name, kind)| {
            let field = Field::new(name, arrow_data_type(kind), true);
            if matches!(kind, ColumnMaterializationKind::Time) {
                field.with_metadata(HashMap::from([(
                    SAS_LOGICAL_TYPE_KEY.to_owned(),
                    "TIME".to_owned(),
                )]))
            } else {
                field
            }
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}

#[cfg(feature = "arrow")]
const fn arrow_data_type(kind: ColumnMaterializationKind) -> DataType {
    match kind {
        ColumnMaterializationKind::I32 => DataType::Int32,
        ColumnMaterializationKind::I64 => DataType::Int64,
        ColumnMaterializationKind::F64 => DataType::Float64,
        ColumnMaterializationKind::Date => DataType::Date32,
        // Microseconds/nanoseconds, not Seconds: Parquet and Polars have no second-precision
        // temporal type, and sub-second SAS datetimes/times must survive (see the
        // schema-aware conversion in `columnar.rs`).
        ColumnMaterializationKind::DateTime => DataType::Timestamp(TimeUnit::Microsecond, None),
        // Duration, not Time64. SAS stores TIME as a plain numeric count of seconds since
        // midnight, and real files carry values outside `[0, 24h)` — an elapsed duration
        // recorded with a TIME format, or a negative offset. `Time64(Nanosecond)` is defined
        // only over `[0, 24h)`, so such a column is data we can write but no spec-following
        // reader will give back: Arrow-rs, DuckDB and pyarrow all null it on read. The two
        // types have the *same* i64 nanosecond payload, so this costs nothing in size or
        // convertibility and buys the whole SAS domain. `SAS_LOGICAL_TYPE_KEY` records the
        // clock-time intent for consumers that want to narrow it back.
        //
        // Choosing per file (Time64 when everything fits, Duration otherwise) would need a
        // full pre-pass: the schema is fixed before the first batch and Arrow streams cannot
        // change it midway, and no metadata declares a TIME column's range.
        ColumnMaterializationKind::Time => DataType::Duration(TimeUnit::Nanosecond),
        ColumnMaterializationKind::Utf8 => DataType::Utf8,
        ColumnMaterializationKind::RawBytes => DataType::Binary,
    }
}
