#[cfg(feature = "arrow")]
use super::Dataset;
#[cfg(feature = "arrow")]
use super::convert::{build_polars_schema, owned_batch_to_dataframe, polars_dtype, py_err};
#[cfg(feature = "arrow")]
use super::predicate::{PredicateExpr, append_unique_columns, filter_dataframe, prepare_predicate};
#[cfg(feature = "arrow")]
use super::{BatchReader, ReaderMessage, SasIoSource};
#[cfg(feature = "arrow")]
use arrow_schema::Schema as ArrowSchema;
#[cfg(feature = "arrow")]
use polars::frame::DataFrame;
#[cfg(feature = "arrow")]
use pyo3::{
    prelude::*,
    types::{PyDict, PyModule},
};
#[cfg(feature = "arrow")]
use sas7bdat_simd::{BatchHint, Error, Projection, Result as SasResult};
#[cfg(feature = "arrow")]
use std::{
    sync::{Arc, mpsc},
    thread,
};

#[cfg(feature = "arrow")]
pub fn schema_for_dataset(py: Python<'_>, ds: &Dataset) -> PyResult<Py<PyAny>> {
    let schema = ds.scan().arrow_schema().map_err(py_err)?;
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
    let full_schema = match full_schema {
        Some(full_schema) => full_schema,
        None => {
            let arrow_schema = ds.scan().arrow_schema().map_err(py_err)?;
            Arc::new(build_polars_schema(&arrow_schema).map_err(py_err)?)
        }
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
    full_schema: Option<Arc<polars_arrow::datatypes::ArrowSchema>>,
    with_columns: Option<Vec<String>>,
    predicate: Option<Py<PyAny>>,
    n_rows: Option<usize>,
    batch_size: Option<usize>,
    coalesce: bool,
) -> BatchReader {
    let (tx, rx) = mpsc::sync_channel::<ReaderMessage>(4);
    let ds = Arc::clone(ds);
    let (rust_predicate, python_predicate) = prepare_predicate(py, ds.as_ref(), predicate);
    let with_columns = match (
        with_columns,
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

    thread::spawn(move || {
        let result = run_scan(
            &ds,
            full_schema.as_ref(),
            with_columns,
            n_rows,
            batch_size,
            coalesce,
            rust_predicate.as_ref(),
            &tx,
        );
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
pub fn run_scan(
    ds: &Dataset,
    full_schema: Option<&Arc<polars_arrow::datatypes::ArrowSchema>>,
    with_columns: Option<Vec<String>>,
    n_rows: Option<usize>,
    batch_size: Option<usize>,
    coalesce: bool,
    predicate: Option<&PredicateExpr>,
    tx: &mpsc::SyncSender<ReaderMessage>,
) -> SasResult<()> {
    let projection_columns = with_columns.clone();
    let projection = build_projection(ds, with_columns)?;
    let mut scan = ds.scan();
    if let Some(ref projection) = projection {
        scan = scan.with_projection(projection);
    }
    if let Some(n_rows) = n_rows {
        scan = scan
            .limit(u64::try_from(n_rows).map_err(|_| Error::unsupported("row limit exceeds u64"))?);
    }
    if let Some(batch_size) = batch_size {
        scan = scan.with_batch_hint(BatchHint::Rows(batch_size));
    }

    let pl_schema = resolve_polars_schema(full_schema, projection_columns.as_deref(), &scan)?;

    if coalesce {
        let mut combined: Option<DataFrame> = None;
        scan.visit_owned_batches(|batch| {
            let mut df = owned_batch_to_dataframe(batch, Arc::clone(&pl_schema))
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
            let mut df = owned_batch_to_dataframe(batch, Arc::clone(&pl_schema))
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
    scan: &sas7bdat_simd::ScanBuilder<'_>,
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
