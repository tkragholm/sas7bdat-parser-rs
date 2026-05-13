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
    BatchHint, Error, LabelSet, LogicalType, Projection, Result as SasResult,
    catalog::normalize_format_name,
};
#[cfg(feature = "arrow")]
use std::{
    sync::{Arc, mpsc},
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
        LogicalType::DateTime => ArrowSchemaDataType::Timestamp(ArrowSchemaTimeUnit::Second, None),
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
    column_names
        .iter()
        .map(|name| {
            ds.columns()
                .iter()
                .find(|col| col.name.as_str() == *name)
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
        rx,
        predicate: python_predicate,
    }
}

#[cfg(feature = "arrow")]
fn run_scan(
    ds: &Dataset,
    request: &ScanRequest,
    predicate: Option<&PredicateExpr>,
    tx: &mpsc::SyncSender<ReaderMessage>,
) -> SasResult<()> {
    let projection_columns = request.with_columns.clone();
    let projection = build_projection(ds, request.with_columns.clone())?;
    let mut scan = ds.scan();
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
        projection_columns.as_deref(),
        &scan,
    )?;

    let projected_names: Vec<&str> = projection_columns.as_deref().map_or_else(
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
