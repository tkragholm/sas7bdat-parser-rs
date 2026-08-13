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

/// Ceiling on the decoded size of one [`BatchHint::Auto`] batch.
///
/// `Auto` used to be `rows_per_page` alone: batch size tracked the file's page geometry
/// and knew nothing about how wide a row is. On the 4,041-column AHS fixture that makes a
/// 4,096-row batch materialize ~200 MB, and the parallel scan multiplies that number —
/// it keeps `workers * 2` batches queued plus one in flight per worker. Measured on that
/// file (buffered I/O, so no mmap pages in the figure), peak RSS went 0.30 GB at one
/// worker, 1.72 at two, 3.65 at four and 4.81 at eight, while throughput stopped
/// improving after eight. So the old sizing spent gigabytes to buy nothing, and it spent
/// more of them the more cores the host had — the wrong way round for a 96-core server.
///
/// Sizing by bytes bounds that product no matter how wide the table or how many cores are
/// free, and it splits wide files into more, smaller batches, which also gives the work
/// stealer finer granularity. Narrow tables are unaffected: their rows are small enough
/// that the row-count rule stays the binding constraint.
const AUTO_BATCH_TARGET_BYTES: usize = 32 << 20;

/// Bytes one decoded row occupies once materialized into owned column buffers.
///
/// A string column keeps its bytes plus one offset slot per row; everything else widens to
/// a fixed 8-byte cell whatever its 3..=8 bytes on disk. Deliberately an estimate — it is
/// used to size a batch, not to allocate one — but a close one: it predicts ~48 KB/row for
/// the AHS fixture against a measured ~200 MB per 4,096-row batch.
fn estimated_decoded_row_bytes(builder: &ScanBuilder<'_>) -> usize {
    let columns = builder.ds.columns();
    let cost = |column: &ColumnMeta| match column.logical_type {
        LogicalType::String | LogicalType::Bytes => usize::try_from(column.physical_width)
            .unwrap_or(0)
            .saturating_add(size_of::<u32>()),
        _ => size_of::<f64>(),
    };
    match builder.projection {
        Some(projection) => projection
            .columns()
            .iter()
            .filter_map(|projected| columns.get(projected.index))
            .map(cost)
            .sum(),
        None => columns.iter().map(cost).sum(),
    }
}

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
        .ok_or_else(|| Error::corruption("column end overflow"))?;
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
            let rows = rows_per_page.max(AUTO_BATCH_ROWS_MIN).max(1);
            // Whichever bound binds first. On a narrow table the byte ceiling permits far
            // more rows than the row rule asks for, so `Auto` is unchanged there; on a wide
            // one the byte ceiling takes over. Never below one row.
            let by_bytes = AUTO_BATCH_TARGET_BYTES / estimated_decoded_row_bytes(builder).max(1);
            Ok(rows.min(by_bytes.max(1)))
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

#[cfg(test)]
mod batch_sizing_tests {
    use super::{
        AUTO_BATCH_ROWS_MIN, AUTO_BATCH_TARGET_BYTES, estimated_decoded_row_bytes,
        resolve_batch_row_capacity,
    };
    use crate::test_utils::MockDatasetBuilder;
    use crate::{BatchHint, Dataset, LogicalType};
    use std::sync::Arc;

    fn dataset(numeric_columns: usize, string_columns: usize, string_width: u32) -> Dataset {
        let mut builder = MockDatasetBuilder::new(Arc::from(vec![0u8; 4096].as_slice()));
        let mut offset = 0u32;
        for i in 0..numeric_columns {
            builder = builder.with_column(&format!("n{i}"), LogicalType::Float, 8, offset);
            offset += 8;
        }
        for i in 0..string_columns {
            builder =
                builder.with_column(&format!("s{i}"), LogicalType::String, string_width, offset);
            offset += string_width;
        }
        builder
            .with_row_len(offset as usize)
            .with_rows_per_page(4096)
            .build()
    }

    #[test]
    fn a_narrow_table_is_sized_by_rows_exactly_as_before() {
        let ds = dataset(8, 2, 16);
        let rows = resolve_batch_row_capacity(&ds.scan()).expect("capacity");
        assert_eq!(
            rows, AUTO_BATCH_ROWS_MIN,
            "the byte ceiling must not bind on a table whose rows are small"
        );
    }

    #[test]
    fn a_wide_table_is_sized_so_one_batch_fits_the_byte_ceiling() {
        let ds = dataset(2000, 0, 0);
        let scan = ds.scan();
        let rows = resolve_batch_row_capacity(&scan).expect("capacity");
        let per_row = estimated_decoded_row_bytes(&scan);
        assert!(
            rows < AUTO_BATCH_ROWS_MIN,
            "the byte ceiling should bind here"
        );
        assert!(rows >= 1, "never below a single row");
        assert!(
            rows * per_row <= AUTO_BATCH_TARGET_BYTES,
            "a batch of {rows} x {per_row} B exceeds the {AUTO_BATCH_TARGET_BYTES} B ceiling"
        );
    }

    #[test]
    fn an_explicit_row_hint_still_wins() {
        // The ceiling is a property of `Auto`. A caller who names a row count has
        // said what they want and must keep getting it.
        let ds = dataset(2000, 0, 0);
        let rows = resolve_batch_row_capacity(&ds.scan().with_batch_hint(BatchHint::Rows(50_000)))
            .expect("capacity");
        assert_eq!(rows, 50_000);
    }

    #[test]
    fn strings_cost_their_width_and_numerics_a_fixed_cell() {
        let ds = dataset(3, 2, 20);
        // 3 numerics at 8 B, 2 strings at 20 B of payload plus a 4 B offset slot.
        assert_eq!(
            estimated_decoded_row_bytes(&ds.scan()),
            3 * 8 + 2 * (20 + 4)
        );
    }

    #[test]
    fn a_projection_only_counts_the_columns_it_keeps() {
        let ds = dataset(4, 0, 0);
        let projection = ds
            .projection()
            .columns(["n0", "n1"])
            .build()
            .expect("projection");
        let scan = ds.scan().with_projection(&projection);
        assert_eq!(estimated_decoded_row_bytes(&scan), 2 * 8);
    }
}
