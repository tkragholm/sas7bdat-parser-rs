#[cfg(feature = "arrow")]
use super::Dataset;
#[cfg(feature = "arrow")]
use super::convert::{build_polars_schema, owned_batch_to_dataframe, polars_dtype, py_err};
#[cfg(feature = "arrow")]
use super::predicate::{PredicateExpr, append_unique_columns, filter_dataframe, prepare_predicate};
#[cfg(feature = "arrow")]
use super::{BatchReader, ReaderMessage, SasIoSource};
#[cfg(feature = "arrow")]
use arrow_schema::{
    DataType as ArrowSchemaDataType, Field as ArrowSchemaField, Schema as ArrowSchema,
    TimeUnit as ArrowSchemaTimeUnit,
};
#[cfg(feature = "arrow")]
use polars::frame::DataFrame;
#[cfg(feature = "arrow")]
use pyo3::{
    prelude::*,
    types::{PyDict, PyModule},
};
#[cfg(feature = "arrow")]
use sas7bdat::{
    BatchHint, ColumnMajorDecode, Error, LabelSet, LogicalType, Parallelism, Projection,
    Result as SasResult, catalog::normalize_format_name,
};
#[cfg(feature = "arrow")]
use std::{
    sync::{Arc, Mutex, mpsc},
    thread,
};

#[cfg(feature = "arrow")]
pub struct BatchReaderRequest {
    pub full_schema: Option<Arc<polars_arrow::datatypes::ArrowSchema>>,
    pub with_columns: Option<Vec<String>>,
    pub predicate: Option<Py<PyAny>>,
    pub n_rows: Option<usize>,
    pub batch_size: Option<usize>,
    pub coalesce: bool,
}

#[cfg(feature = "arrow")]
pub fn full_arrow_schema_for_dataset(ds: &Dataset) -> SasResult<Arc<ArrowSchema>> {
    let label_sets = &ds.metadata().label_sets;
    let fields = ds
        .columns()
        .iter()
        .map(|column| {
            let raw_dt = arrow_data_type_for_logical_type(column.logical_type);
            let dt = if label_sets.is_empty() {
                raw_dt
            } else {
                column
                    .format
                    .as_deref()
                    .map(normalize_format_name)
                    .filter(|norm| label_sets.contains_key(norm.as_str()))
                    .map_or(raw_dt, |_| ArrowSchemaDataType::Utf8)
            };
            Ok(ArrowSchemaField::new(column.name.clone(), dt, true))
        })
        .collect::<SasResult<Vec<_>>>()?;
    Ok(Arc::new(ArrowSchema::new(fields)))
}

#[cfg(feature = "arrow")]
pub fn full_polars_schema_for_dataset(
    ds: &Dataset,
) -> SasResult<Arc<polars_arrow::datatypes::ArrowSchema>> {
    let schema = full_arrow_schema_for_dataset(ds)?;
    Ok(Arc::new(build_polars_schema(schema.as_ref())?))
}

#[cfg(feature = "arrow")]
const fn arrow_data_type_for_logical_type(logical_type: LogicalType) -> ArrowSchemaDataType {
    match logical_type {
        LogicalType::Integer => ArrowSchemaDataType::Int64,
        LogicalType::Float => ArrowSchemaDataType::Float64,
        LogicalType::String => ArrowSchemaDataType::Utf8,
        LogicalType::Date => ArrowSchemaDataType::Date32,
        // Microseconds, not Seconds: Polars has no Second time unit, so a
        // Timestamp(Second) batch is materialized as Datetime('ms') while the
        // declared schema says Datetime('us') — the two disagree and Polars
        // refuses to stack the batches (SchemaError: ms != us). Emitting µs
        // keeps the declared schema and the materialized batches identical.
        LogicalType::DateTime => {
            ArrowSchemaDataType::Timestamp(ArrowSchemaTimeUnit::Microsecond, None)
        }
        // Time64 (pl.Time) spans only [0, 24h); SAS time values >= 24h or negative surface as
        // null here by design — see the Time arm in `convert.rs`.
        LogicalType::Time => ArrowSchemaDataType::Time64(ArrowSchemaTimeUnit::Nanosecond),
        LogicalType::Bytes => ArrowSchemaDataType::Binary,
    }
}

#[cfg(feature = "arrow")]
struct ScanRequest {
    full_schema: Option<Arc<polars_arrow::datatypes::ArrowSchema>>,
    with_columns: Option<Vec<String>>,
    n_rows: Option<usize>,
    batch_size: Option<usize>,
    coalesce: bool,
}

#[cfg(feature = "arrow")]
fn build_label_mapping_for_columns(ds: &Dataset, column_names: &[&str]) -> Vec<Option<LabelSet>> {
    let label_sets = &ds.metadata().label_sets;
    if label_sets.is_empty() {
        return Vec::new();
    }
    // Index columns by name once (first occurrence wins, matching the prior
    // `find`) instead of rescanning every column for each requested name.
    let mut by_name: std::collections::HashMap<&str, _> =
        std::collections::HashMap::with_capacity(ds.columns().len());
    for col in ds.columns() {
        by_name.entry(col.name.as_str()).or_insert(col);
    }
    column_names
        .iter()
        .map(|name| {
            by_name
                .get(name)
                .and_then(|col| col.format.as_deref())
                .map(normalize_format_name)
                .and_then(|norm| label_sets.get(&norm))
                .cloned()
        })
        .collect()
}

#[cfg(feature = "arrow")]
pub fn schema_for_dataset(py: Python<'_>, ds: &Dataset) -> PyResult<Py<PyAny>> {
    let schema = full_arrow_schema_for_dataset(ds).map_err(py_err)?;
    schema_from_arrow_schema(py, &schema)
}

#[cfg(feature = "arrow")]
pub fn schema_from_arrow_schema(py: Python<'_>, schema: &ArrowSchema) -> PyResult<Py<PyAny>> {
    let polars = PyModule::import(py, "polars")?;
    let dict = PyDict::new(py);
    for field in schema.fields() {
        dict.set_item(field.name(), polars_dtype(&polars, field.data_type())?)?;
    }
    Ok(polars.getattr("Schema")?.call1((dict,))?.unbind())
}

#[cfg(feature = "arrow")]
pub fn register_io_source(
    py: Python<'_>,
    ds: Arc<Dataset>,
    full_schema: Option<Arc<polars_arrow::datatypes::ArrowSchema>>,
    schema: Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    let full_schema = if let Some(full_schema) = full_schema {
        full_schema
    } else {
        full_polars_schema_for_dataset(ds.as_ref()).map_err(py_err)?
    };
    let io_source = Py::new(py, SasIoSource { ds, full_schema })?;
    let register_io_source =
        PyModule::import(py, "polars.io.plugins")?.getattr("register_io_source")?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("io_source", io_source)?;
    kwargs.set_item("schema", schema)?;
    kwargs.set_item("validate_schema", false)?;
    kwargs.set_item("is_pure", true)?;
    Ok(register_io_source.call((), Some(&kwargs))?.unbind())
}

#[cfg(feature = "arrow")]
pub fn batch_reader_from_dataset(
    py: Python<'_>,
    ds: &Arc<Dataset>,
    request: BatchReaderRequest,
) -> BatchReader {
    let (tx, rx) = mpsc::sync_channel::<ReaderMessage>(4);
    let ds = Arc::clone(ds);
    let (rust_predicate, python_predicate) = prepare_predicate(py, ds.as_ref(), request.predicate);
    let with_columns = match (
        request.with_columns,
        rust_predicate.as_ref(),
        python_predicate.is_some(),
    ) {
        (Some(with_columns), Some(predicate), _) => {
            let mut merged = with_columns;
            let mut predicate_columns = Vec::new();
            predicate.collect_columns(&mut predicate_columns);
            append_unique_columns(&mut merged, &predicate_columns);
            Some(merged)
        }
        (_, None, true) => None,
        (with_columns, _, _) => with_columns,
    };
    let scan_request = ScanRequest {
        full_schema: request.full_schema,
        with_columns,
        n_rows: request.n_rows,
        batch_size: request.batch_size,
        coalesce: request.coalesce,
    };

    thread::spawn(move || {
        let result = run_scan(&ds, &scan_request, rust_predicate.as_ref(), &tx);
        if let Err(err) = result {
            let _ = tx.send(ReaderMessage::Error(err.to_string()));
        }
    });

    BatchReader {
        rx: Mutex::new(rx),
        predicate: python_predicate,
    }
}

// Parallel "grain size": the minimum work one decode worker should be handed before
// the fixed cost of spawning it (rayon task + per-chunk accumulator + the ordered
// page-chunk merge) is worth paying. The grain is in WORK units, not wall-clock, so a
// single conservative default holds across hardware: spawn/merge overhead is ~tens of
// microseconds everywhere, and these grains keep each worker busy for well over a
// millisecond regardless of CPU. This is the standard parallel-cutoff practice — scale
// the worker COUNT to the file (cf. rayon `with_min_len`, TBB partitioner grain)
// instead of tuning a per-machine byte threshold. Both can be overridden via env, but
// the defaults need no tuning.
//
// Each worker should decode ≳ this many uncompressed bytes (row_count × row_len). At
// typical decode throughput that is several ms of work — comfortably amortising setup.
#[cfg(feature = "arrow")]
const DEFAULT_MIN_BYTES_PER_WORKER: u64 = 4 * 1024 * 1024;
// …and span ≳ this many pages, since page chunks are the unit of parallelism (no point
// making 12 one-page chunks out of a 12-page file).
#[cfg(feature = "arrow")]
const DEFAULT_MIN_PAGES_PER_WORKER: u64 = 8;

/// Whether to use the column-major page decode. It is markedly faster for wide all-numeric
/// tables and falls back to row-major automatically when a scan can't use it (string/temporal
/// columns, row limits, non-in-memory sources), so it is safe to leave on. On by default; set
/// `SAS7BDAT_COLUMN_MAJOR=0` (or `off`/`false`) to force the row-major path.
#[cfg(feature = "arrow")]
fn column_major_decode() -> ColumnMajorDecode {
    match std::env::var("SAS7BDAT_COLUMN_MAJOR").ok().as_deref() {
        Some("0" | "off" | "false" | "OFF" | "FALSE") => ColumnMajorDecode::Off,
        _ => ColumnMajorDecode::On,
    }
}

#[cfg(feature = "arrow")]
fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default)
}

/// How many decode threads were requested (env `SAS7BDAT_SCAN_THREADS`, else all
/// logical cores). The inter-file pool sets this per worker to keep the total core
/// budget bounded; standalone scans get every core. This is the CAP — the grain-size
/// rule below may hand out fewer for small files.
#[cfg(feature = "arrow")]
fn requested_scan_threads() -> usize {
    std::env::var("SAS7BDAT_SCAN_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .or_else(|| {
            std::thread::available_parallelism()
                .ok()
                .map(std::num::NonZero::get)
        })
        .unwrap_or(1)
}

/// Resolve the parallelism for one file's page decode.
///
/// The crate's `Parallelism::Auto` resolves to a SINGLE worker (serial), so the plugin
/// must opt in explicitly. But blindly fanning every file across all cores wastes
/// effort on the many small yearly files in a register — the threads just fight over a
/// near-empty file. Instead of a tuned threshold, we derive the worker COUNT from the
/// file using a hardware-stable grain size: each worker must get at least one grain of
/// work in BOTH dimensions (pages and decoded bytes), and the smallest of the three
/// caps (requested threads, pages/grain, bytes/grain) wins. A small file collapses this
/// to <2 → serial (one core, no coordination overhead) and the inter-file pool keeps
/// the cores busy by running several such files at once; a large file scales up to the
/// requested cap. All inputs are header-only metadata — no body decode.
#[cfg(feature = "arrow")]
fn scan_parallelism(ds: &Dataset) -> Parallelism {
    let requested = requested_scan_threads();
    if requested <= 1 {
        return Parallelism::None;
    }
    let meta = ds.metadata();
    let decode_bytes = meta.row_count.saturating_mul(u64::from(meta.row_len));
    let min_bytes = env_u64(
        "SAS7BDAT_SCAN_MIN_BYTES_PER_WORKER",
        DEFAULT_MIN_BYTES_PER_WORKER,
    );
    let min_pages = env_u64(
        "SAS7BDAT_SCAN_MIN_PAGES_PER_WORKER",
        DEFAULT_MIN_PAGES_PER_WORKER,
    );

    // Hand each worker a full grain; the tightest constraint decides the count.
    let workers = (requested as u64)
        .min(meta.page_count / min_pages)
        .min(decode_bytes / min_bytes);
    if workers >= 2 {
        Parallelism::Threads(usize::try_from(workers).unwrap_or(requested))
    } else {
        Parallelism::None
    }
}

#[cfg(feature = "arrow")]
fn run_scan(
    ds: &Dataset,
    request: &ScanRequest,
    predicate: Option<&PredicateExpr>,
    tx: &mpsc::SyncSender<ReaderMessage>,
) -> SasResult<()> {
    let projection = build_projection(ds, request.with_columns.clone())?;
    // Decode pages across threads. The crate defaults to Parallelism::Auto, which
    // resolves to a SINGLE worker (serial) — so without this the scan pegged one core
    // and left large hosts at ~10% CPU. Threads(n) engages the parallel page-streaming
    // path (ScanBuilder::try_stream_batches_parallel). Defaults to all logical cores;
    // override with SAS7BDAT_SCAN_THREADS for tuning.
    let mut scan = ds
        .scan()
        .with_parallelism(scan_parallelism(ds))
        .with_column_major_decode(column_major_decode());
    if let Some(ref projection) = projection {
        scan = scan.with_projection(projection);
    }
    if let Some(n_rows) = request.n_rows {
        scan = scan
            .limit(u64::try_from(n_rows).map_err(|_| Error::unsupported("row limit exceeds u64"))?);
    }
    if let Some(batch_size) = request.batch_size {
        scan = scan.with_batch_hint(BatchHint::Rows(batch_size));
    }

    let pl_schema = resolve_polars_schema(
        request.full_schema.as_ref(),
        request.with_columns.as_deref(),
        &scan,
    )?;

    let projected_names: Vec<&str> = request.with_columns.as_deref().map_or_else(
        || ds.columns().iter().map(|c| c.name.as_str()).collect(),
        |names| names.iter().map(String::as_str).collect(),
    );
    let label_mapping = build_label_mapping_for_columns(ds, &projected_names);

    if request.coalesce {
        let mut combined: Option<DataFrame> = None;
        scan.visit_owned_batches(|batch| {
            let mut df = owned_batch_to_dataframe(batch, Arc::clone(&pl_schema), &label_mapping)
                .map_err(|e| Error::io(e.to_string()))?;
            if let Some(predicate) = predicate {
                df = filter_dataframe(&df, predicate)?;
            }
            if df.height() == 0 {
                return Ok(std::ops::ControlFlow::Continue(()));
            }
            if let Some(existing) = combined.as_mut() {
                existing
                    .vstack_mut(&df)
                    .map_err(|e| Error::io(e.to_string()))?;
            } else {
                combined = Some(df);
            }
            Ok(std::ops::ControlFlow::Continue(()))
        })?;
        if let Some(combined) = combined {
            let _ = tx.send(ReaderMessage::Batch(combined));
        }
    } else {
        scan.visit_owned_batches(|batch| {
            let mut df = owned_batch_to_dataframe(batch, Arc::clone(&pl_schema), &label_mapping)
                .map_err(|e| Error::io(e.to_string()))?;
            if let Some(predicate) = predicate {
                df = filter_dataframe(&df, predicate)?;
            }
            if df.height() == 0 {
                return Ok(std::ops::ControlFlow::Continue(()));
            }
            tx.send(ReaderMessage::Batch(df))
                .map_err(|err| Error::io(err.to_string()))?;
            Ok(std::ops::ControlFlow::Continue(()))
        })?;
    }
    Ok(())
}

#[cfg(feature = "arrow")]
fn resolve_polars_schema(
    full_schema: Option<&Arc<polars_arrow::datatypes::ArrowSchema>>,
    projection_columns: Option<&[String]>,
    scan: &sas7bdat::ScanBuilder<'_>,
) -> SasResult<Arc<polars_arrow::datatypes::ArrowSchema>> {
    match (projection_columns, full_schema) {
        (None, Some(full_schema)) => Ok(Arc::clone(full_schema)),
        (Some(columns), Some(full_schema)) if !columns.is_empty() => {
            let fields = columns
                .iter()
                .map(|name| {
                    full_schema.get(name).cloned().ok_or_else(|| {
                        Error::arrow(format!("missing projected column in cached schema: {name}"))
                    })
                })
                .collect::<SasResult<Vec<_>>>()?;
            Ok(Arc::new(
                polars_arrow::datatypes::ArrowSchema::from_iter_check_duplicates(fields)
                    .map_err(|err| Error::arrow(err.to_string()))?,
            ))
        }
        _ => {
            let arrow_schema = scan.arrow_schema()?;
            Ok(Arc::new(build_polars_schema(&arrow_schema)?))
        }
    }
}

#[cfg(feature = "arrow")]
fn build_projection(
    ds: &Dataset,
    with_columns: Option<Vec<String>>,
) -> SasResult<Option<Projection>> {
    let Some(with_columns) = with_columns else {
        return Ok(None);
    };
    if with_columns.is_empty() {
        return Ok(None);
    }
    ds.projection().columns(with_columns).build().map(Some)
}
