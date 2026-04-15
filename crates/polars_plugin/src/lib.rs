mod convert;
mod predicate;
mod scan;

#[cfg(feature = "arrow")]
use std::{
    convert::TryFrom,
    hint::black_box,
    sync::{Arc, mpsc},
    time::Instant,
};

#[cfg(feature = "arrow")]
use arrow_schema::Schema as ArrowSchema;
#[cfg(feature = "arrow")]
use polars::frame::DataFrame;
#[cfg(feature = "arrow")]
use pyo3::{
    IntoPyObjectExt,
    exceptions::{PyRuntimeError, PyStopIteration, PyValueError},
    prelude::*,
    types::{PyDict, PyModule},
};
#[cfg(feature = "arrow")]
use pyo3_polars::types::PyDataFrame;
#[cfg(feature = "arrow")]
use sas7bdat_simd::{BatchHint, Dataset, Error, OwnedColumnarBatch, Projection};

// ─── message types ────────────────────────────────────────────────────────────

#[cfg(feature = "arrow")]
enum ReaderMessage {
    Batch(DataFrame),
    Error(String),
}

#[cfg(feature = "arrow")]
struct PreparedWarmBatches {
    batches: Vec<OwnedColumnarBatch>,
    schema: Arc<polars_arrow::datatypes::ArrowSchema>,
    dataset_open_ns: u128,
    batch_prep_ns: u128,
    rows: usize,
}

#[cfg(feature = "arrow")]
struct BatchBenchmarkStats {
    dataset_open_ns: u128,
    batch_prep_ns: u128,
    priming_ns: u128,
    steady_elapsed_ns_total: u128,
    steady_elapsed_ns_avg: u128,
    steady_projection_ns_avg: u128,
    steady_schema_ns_avg: u128,
    steady_visit_ns_avg: u128,
    rows_last: usize,
    batches_last: usize,
}

#[cfg(feature = "arrow")]
struct ScanToDataframesStats {
    projection_ns: u128,
    schema_ns: u128,
    visit_ns: u128,
    rows: usize,
    batches: usize,
}

// ─── SasDataset ───────────────────────────────────────────────────────────────

/// A pre-opened SAS7BDAT dataset. Opening once lets repeated scans reuse the
/// parsed metadata instead of paying the open cost every time.
#[cfg(feature = "arrow")]
#[pyclass]
struct SasDataset {
    ds: Arc<Dataset>,
    arrow_schema: Arc<ArrowSchema>,
    polars_schema: Arc<polars_arrow::datatypes::ArrowSchema>,
}

#[cfg(feature = "arrow")]
#[pymethods]
impl SasDataset {
    #[new]
    fn open(py: Python<'_>, path: &str) -> PyResult<Self> {
        let ds = py
            .detach(|| Dataset::open(path))
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        let arrow_schema = ds.scan().arrow_schema().map_err(convert::py_err)?;
        let polars_schema =
            Arc::new(convert::build_polars_schema(arrow_schema.as_ref()).map_err(convert::py_err)?);
        Ok(Self {
            ds: Arc::new(ds),
            arrow_schema,
            polars_schema,
        })
    }

    fn schema(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        scan::schema_from_arrow_schema(py, self.arrow_schema.as_ref())
    }

    fn batch_reader(
        &self,
        py: Python<'_>,
        with_columns: Option<Vec<String>>,
        predicate: Option<Py<PyAny>>,
        n_rows: Option<usize>,
        batch_size: Option<usize>,
    ) -> BatchReader {
        scan::batch_reader_from_dataset(
            py,
            &self.ds,
            Some(Arc::clone(&self.polars_schema)),
            with_columns,
            predicate,
            n_rows,
            batch_size,
            false,
        )
    }

    fn scan_sas(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let schema = scan::schema_from_arrow_schema(py, self.arrow_schema.as_ref())?;
        scan::register_io_source(
            py,
            Arc::clone(&self.ds),
            Some(Arc::clone(&self.polars_schema)),
            schema,
        )
    }
}

// ─── SasIoSource ──────────────────────────────────────────────────────────────

#[cfg(feature = "arrow")]
#[pyclass]
struct SasIoSource {
    ds: Arc<Dataset>,
    full_schema: Arc<polars_arrow::datatypes::ArrowSchema>,
}

#[cfg(feature = "arrow")]
#[pymethods]
impl SasIoSource {
    fn __call__(
        &self,
        py: Python<'_>,
        with_columns: Option<Vec<String>>,
        predicate: Option<Py<PyAny>>,
        n_rows: Option<usize>,
        batch_size: Option<usize>,
    ) -> BatchReader {
        scan::batch_reader_from_dataset(
            py,
            &self.ds,
            Some(Arc::clone(&self.full_schema)),
            with_columns,
            predicate,
            n_rows,
            batch_size,
            false,
        )
    }
}

// ─── BatchReader ──────────────────────────────────────────────────────────────

#[cfg(feature = "arrow")]
#[pyclass(unsendable)]
struct BatchReader {
    rx: mpsc::Receiver<ReaderMessage>,
    predicate: Option<Py<PyAny>>,
}

#[cfg(feature = "arrow")]
#[pymethods]
impl BatchReader {
    const fn __iter__(slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        loop {
            let rx = &mut slf.rx;
            let message = py
                .detach(move || rx.recv())
                .map_err(|_| PyStopIteration::new_err("end of stream"))?;

            match message {
                ReaderMessage::Batch(df) => {
                    let py_df = PyDataFrame(df).into_py_any(py)?;
                    if let Some(predicate) = &slf.predicate {
                        let filtered = py_df
                            .bind(py)
                            .call_method1("filter", (predicate.bind(py),))?;
                        if filtered.call_method0("is_empty")?.extract::<bool>()? {
                            continue;
                        }
                        return Ok(filtered.unbind());
                    }
                    return Ok(py_df);
                }
                ReaderMessage::Error(message) => return Err(PyRuntimeError::new_err(message)),
            }
        }
    }
}

// ─── public Python functions ──────────────────────────────────────────────────

#[cfg(feature = "arrow")]
#[pyfunction]
fn schema_for_file(py: Python<'_>, path: &str) -> PyResult<Py<PyAny>> {
    let ds = py
        .detach(|| Dataset::open(path))
        .map_err(|err| PyValueError::new_err(err.to_string()))?;
    scan::schema_for_dataset(py, &ds)
}

#[cfg(feature = "arrow")]
#[pyfunction]
fn batch_reader(
    py: Python<'_>,
    path: &str,
    with_columns: Option<Vec<String>>,
    predicate: Option<Py<PyAny>>,
    n_rows: Option<usize>,
    batch_size: Option<usize>,
) -> PyResult<BatchReader> {
    let ds = py
        .detach(|| Dataset::open(path))
        .map_err(|err| PyValueError::new_err(err.to_string()))?;
    let ds = Arc::new(ds);
    Ok(scan::batch_reader_from_dataset(
        py,
        &ds,
        None,
        with_columns,
        predicate,
        n_rows,
        batch_size,
        false,
    ))
}

#[cfg(feature = "arrow")]
#[pyfunction]
fn scan_sas(py: Python<'_>, path: &str) -> PyResult<Py<PyAny>> {
    let ds = py
        .detach(|| Dataset::open(path))
        .map_err(|err| PyValueError::new_err(err.to_string()))?;
    let ds = Arc::new(ds);
    let schema = scan::schema_for_dataset(py, &ds)?;
    scan::register_io_source(py, ds, None, schema)
}

#[cfg(feature = "arrow")]
#[pyfunction]
fn benchmark_batch_to_dataframe(
    py: Python<'_>,
    path: &str,
    with_columns: Option<Vec<String>>,
    n_rows: Option<usize>,
    batch_size: Option<usize>,
    repeat: usize,
) -> PyResult<Py<PyAny>> {
    let prepared = prepare_warm_batches(path, with_columns, n_rows, batch_size)?;
    let dataset_open_ns = prepared.dataset_open_ns;
    let batch_prep_ns = prepared.batch_prep_ns;
    let priming_rows = prepared.rows;
    let batch_count = prepared.batches.len();
    let priming_ns = py.detach({
        let schema = Arc::clone(&prepared.schema);
        let batches = prepared.batches.clone();
        move || -> PyResult<u128> {
            let start = Instant::now();
            for batch in batches {
                let df = convert::owned_batch_to_dataframe(batch, Arc::clone(&schema))
                    .map_err(convert::py_err)?;
                black_box(df.height());
                black_box(df.width());
            }
            Ok(start.elapsed().as_nanos())
        }
    })?;
    let stats = py.detach({
        let schema = Arc::clone(&prepared.schema);
        let batches = prepared.batches.clone();
        move || -> PyResult<BatchBenchmarkStats> {
            let mut elapsed_total = 0u128;
            let mut rows_last = 0usize;
            for _ in 0..repeat {
                let start = Instant::now();
                let mut rows = 0usize;
                for batch in batches.iter().cloned() {
                    let df = convert::owned_batch_to_dataframe(batch, Arc::clone(&schema))
                        .map_err(convert::py_err)?;
                    rows += df.height();
                    black_box(df.width());
                }
                elapsed_total += start.elapsed().as_nanos();
                rows_last = rows;
            }
            Ok(BatchBenchmarkStats {
                dataset_open_ns,
                batch_prep_ns,
                priming_ns,
                steady_elapsed_ns_total: elapsed_total,
                steady_elapsed_ns_avg: elapsed_total / repeat.max(1) as u128,
                steady_projection_ns_avg: 0,
                steady_schema_ns_avg: 0,
                steady_visit_ns_avg: 0,
                rows_last,
                batches_last: batch_count,
            })
        }
    })?;
    batch_benchmark_dict(py, path, repeat, batch_size, n_rows, &stats, priming_rows)
}

#[cfg(feature = "arrow")]
#[pyfunction]
fn benchmark_scan_to_dataframes(
    py: Python<'_>,
    path: &str,
    with_columns: Option<Vec<String>>,
    n_rows: Option<usize>,
    batch_size: Option<usize>,
    repeat: usize,
) -> PyResult<Py<PyAny>> {
    let open_start = Instant::now();
    let ds = Arc::new(Dataset::open(path).map_err(convert::py_err)?);
    let dataset_open_ns = open_start.elapsed().as_nanos();
    let projection_columns = with_columns.clone();
    let projection = build_projection(ds.as_ref(), with_columns)?;
    let full_schema = Arc::new(
        convert::build_polars_schema(ds.scan().arrow_schema().map_err(convert::py_err)?.as_ref())
            .map_err(convert::py_err)?,
    );

    let priming_start = Instant::now();
    let priming_stats = run_scan_to_dataframes_once(
        ds.as_ref(),
        &full_schema,
        projection_columns.as_deref(),
        projection.as_ref(),
        n_rows,
        batch_size,
    )
    .map_err(convert::py_err)?;
    let priming_ns = priming_start.elapsed().as_nanos();

    let stats = py.detach({
        let ds = Arc::clone(&ds);
        let full_schema = Arc::clone(&full_schema);
        move || -> PyResult<BatchBenchmarkStats> {
            let mut elapsed_total = 0u128;
            let mut projection_total = 0u128;
            let mut schema_total = 0u128;
            let mut visit_total = 0u128;
            let mut rows_last = 0usize;
            for _ in 0..repeat {
                let run = run_scan_to_dataframes_once(
                    ds.as_ref(),
                    &full_schema,
                    projection_columns.as_deref(),
                    projection.as_ref(),
                    n_rows,
                    batch_size,
                )
                .map_err(convert::py_err)?;
                elapsed_total += run.projection_ns + run.schema_ns + run.visit_ns;
                projection_total += run.projection_ns;
                schema_total += run.schema_ns;
                visit_total += run.visit_ns;
                rows_last = run.rows;
            }
            Ok(BatchBenchmarkStats {
                dataset_open_ns,
                batch_prep_ns: 0,
                priming_ns,
                steady_elapsed_ns_total: elapsed_total,
                steady_elapsed_ns_avg: elapsed_total / repeat.max(1) as u128,
                steady_projection_ns_avg: projection_total / repeat.max(1) as u128,
                steady_schema_ns_avg: schema_total / repeat.max(1) as u128,
                steady_visit_ns_avg: visit_total / repeat.max(1) as u128,
                rows_last,
                batches_last: priming_stats.batches,
            })
        }
    })?;
    batch_benchmark_dict(
        py,
        path,
        repeat,
        batch_size,
        n_rows,
        &stats,
        priming_stats.rows,
    )
}

#[cfg(feature = "arrow")]
#[pyfunction]
fn benchmark_dataframe_to_python(
    py: Python<'_>,
    path: &str,
    with_columns: Option<Vec<String>>,
    n_rows: Option<usize>,
    batch_size: Option<usize>,
    repeat: usize,
) -> PyResult<Py<PyAny>> {
    let prepared = prepare_warm_batches(path, with_columns, n_rows, batch_size)?;
    let dataset_open_ns = prepared.dataset_open_ns;
    let batch_prep_ns = prepared.batch_prep_ns;
    let priming_rows = prepared.rows;
    let dataframes = py.detach({
        let schema = Arc::clone(&prepared.schema);
        let batches = prepared.batches.clone();
        move || -> PyResult<Vec<DataFrame>> {
            batches
                .into_iter()
                .map(|batch| {
                    convert::owned_batch_to_dataframe(batch, Arc::clone(&schema))
                        .map_err(convert::py_err)
                })
                .collect()
        }
    })?;
    let batch_count = dataframes.len();
    let priming_start = Instant::now();
    for df in &dataframes {
        let py_df = PyDataFrame(df.clone()).into_py_any(py)?;
        black_box(py_df);
    }
    let priming_ns = priming_start.elapsed().as_nanos();

    let mut elapsed_total = 0u128;
    let mut rows_last = 0usize;
    for _ in 0..repeat {
        let start = Instant::now();
        let mut rows = 0usize;
        for df in &dataframes {
            rows += df.height();
            let py_df = PyDataFrame(df.clone()).into_py_any(py)?;
            black_box(py_df);
        }
        elapsed_total += start.elapsed().as_nanos();
        rows_last = rows;
    }
    let stats = BatchBenchmarkStats {
        dataset_open_ns,
        batch_prep_ns,
        priming_ns,
        steady_elapsed_ns_total: elapsed_total,
        steady_elapsed_ns_avg: elapsed_total / repeat.max(1) as u128,
        steady_projection_ns_avg: 0,
        steady_schema_ns_avg: 0,
        steady_visit_ns_avg: 0,
        rows_last,
        batches_last: batch_count,
    };
    batch_benchmark_dict(py, path, repeat, batch_size, n_rows, &stats, priming_rows)
}

#[cfg(feature = "arrow")]
fn prepare_warm_batches(
    path: &str,
    with_columns: Option<Vec<String>>,
    n_rows: Option<usize>,
    batch_size: Option<usize>,
) -> PyResult<PreparedWarmBatches> {
    let open_start = Instant::now();
    let ds = Dataset::open(path).map_err(convert::py_err)?;
    let dataset_open_ns = open_start.elapsed().as_nanos();
    let projection_columns = with_columns.clone();
    let projection = build_projection(&ds, with_columns)?;
    let prep_start = Instant::now();
    let mut scan = ds.scan();
    if let Some(ref projection) = projection {
        scan = scan.with_projection(projection);
    }
    if let Some(n_rows) = n_rows {
        scan = scan
            .limit(u64::try_from(n_rows).map_err(|_| convert::py_err("row limit exceeds u64"))?);
    }
    if let Some(batch_size) = batch_size {
        scan = scan.with_batch_hint(BatchHint::Rows(batch_size));
    }
    let full_schema = Arc::new(
        convert::build_polars_schema(ds.scan().arrow_schema().map_err(convert::py_err)?.as_ref())
            .map_err(convert::py_err)?,
    );
    let schema =
        resolve_polars_schema_from_full(&full_schema, projection_columns.as_deref(), &scan)
            .map_err(convert::py_err)?;
    let batches = scan.collect_batches().map_err(convert::py_err)?;
    let rows = batches.iter().map(|batch| batch.row_count).sum();
    let batch_prep_ns = prep_start.elapsed().as_nanos();
    Ok(PreparedWarmBatches {
        batches,
        schema,
        dataset_open_ns,
        batch_prep_ns,
        rows,
    })
}

#[cfg(feature = "arrow")]
fn build_projection(
    ds: &Dataset,
    with_columns: Option<Vec<String>>,
) -> PyResult<Option<Projection>> {
    match with_columns {
        Some(columns) if !columns.is_empty() => ds
            .projection()
            .columns(columns)
            .build()
            .map(Some)
            .map_err(convert::py_err),
        _ => Ok(None),
    }
}

#[cfg(feature = "arrow")]
fn run_scan_to_dataframes_once(
    ds: &Dataset,
    full_schema: &Arc<polars_arrow::datatypes::ArrowSchema>,
    projection_columns: Option<&[String]>,
    projection: Option<&Projection>,
    n_rows: Option<usize>,
    batch_size: Option<usize>,
) -> Result<ScanToDataframesStats, Error> {
    let projection_start = Instant::now();
    let mut scan = ds.scan();
    if let Some(projection) = projection {
        scan = scan.with_projection(projection);
    }
    if let Some(n_rows) = n_rows {
        scan = scan
            .limit(u64::try_from(n_rows).map_err(|_| Error::unsupported("row limit exceeds u64"))?);
    }
    if let Some(batch_size) = batch_size {
        scan = scan.with_batch_hint(BatchHint::Rows(batch_size));
    }
    let projection_ns = projection_start.elapsed().as_nanos();

    let schema_start = Instant::now();
    let schema = resolve_polars_schema_from_full(full_schema, projection_columns, &scan)?;
    let schema_ns = schema_start.elapsed().as_nanos();

    let visit_start = Instant::now();
    let mut rows = 0usize;
    let mut batches = 0usize;
    scan.visit_owned_batches(|batch| {
        let df = convert::owned_batch_to_dataframe(batch, Arc::clone(&schema))?;
        rows += df.height();
        batches += 1;
        black_box(df.width());
        Ok(std::ops::ControlFlow::Continue(()))
    })?;
    let visit_ns = visit_start.elapsed().as_nanos();
    Ok(ScanToDataframesStats {
        projection_ns,
        schema_ns,
        visit_ns,
        rows,
        batches,
    })
}

#[cfg(feature = "arrow")]
fn resolve_polars_schema_from_full(
    full_schema: &Arc<polars_arrow::datatypes::ArrowSchema>,
    projection_columns: Option<&[String]>,
    scan: &sas7bdat_simd::ScanBuilder<'_>,
) -> Result<Arc<polars_arrow::datatypes::ArrowSchema>, Error> {
    let cached = match projection_columns {
        None => Some(Arc::clone(full_schema)),
        Some(columns) if columns.is_empty() => Some(Arc::clone(full_schema)),
        Some(columns) => {
            let fields = columns
                .iter()
                .map(|name| {
                    full_schema.get(name).cloned().ok_or_else(|| {
                        Error::arrow(format!("missing projected column in cached schema: {name}"))
                    })
                })
                .collect::<Result<Vec<_>, _>>();
            match fields {
                Ok(fields) => Some(Arc::new(
                    polars_arrow::datatypes::ArrowSchema::from_iter_check_duplicates(fields)
                        .map_err(|err| Error::arrow(err.to_string()))?,
                )),
                Err(_) => None,
            }
        }
    };
    match cached {
        Some(schema) => Ok(schema),
        None => Ok(Arc::new(convert::build_polars_schema(
            scan.arrow_schema()?.as_ref(),
        )?)),
    }
}

#[cfg(feature = "arrow")]
fn batch_benchmark_dict(
    py: Python<'_>,
    path: &str,
    repeat: usize,
    batch_size: Option<usize>,
    n_rows: Option<usize>,
    stats: &BatchBenchmarkStats,
    priming_rows: usize,
) -> PyResult<Py<PyAny>> {
    let seconds = stats.steady_elapsed_ns_avg as f64 / 1_000_000_000.0;
    let rows_per_second = if seconds > 0.0 {
        stats.rows_last as f64 / seconds
    } else {
        0.0
    };
    let batches_per_second = if seconds > 0.0 {
        stats.batches_last as f64 / seconds
    } else {
        0.0
    };
    let dict = PyDict::new(py);
    dict.set_item("fixture", path)?;
    dict.set_item("repeat", repeat)?;
    dict.set_item("batch_rows", batch_size)?;
    dict.set_item("limit", n_rows)?;
    dict.set_item("dataset_open_ns", stats.dataset_open_ns)?;
    dict.set_item("batch_prep_ns", stats.batch_prep_ns)?;
    dict.set_item("priming_ns", stats.priming_ns)?;
    dict.set_item("priming_rows", priming_rows)?;
    dict.set_item("steady_elapsed_ns_total", stats.steady_elapsed_ns_total)?;
    dict.set_item("steady_elapsed_ns_avg", stats.steady_elapsed_ns_avg)?;
    dict.set_item("steady_projection_ns_avg", stats.steady_projection_ns_avg)?;
    dict.set_item("steady_schema_ns_avg", stats.steady_schema_ns_avg)?;
    dict.set_item("steady_visit_ns_avg", stats.steady_visit_ns_avg)?;
    dict.set_item("rows_last", stats.rows_last)?;
    dict.set_item("batches_last", stats.batches_last)?;
    dict.set_item("rows_per_second", rows_per_second)?;
    dict.set_item("batches_per_second", batches_per_second)?;
    Ok(dict.unbind().into_any())
}

#[cfg(feature = "arrow")]
#[pymodule]
fn sas7bdat_polars(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<SasDataset>()?;
    m.add_class::<SasIoSource>()?;
    m.add_class::<BatchReader>()?;
    m.add_function(wrap_pyfunction!(schema_for_file, m)?)?;
    m.add_function(wrap_pyfunction!(batch_reader, m)?)?;
    m.add_function(wrap_pyfunction!(scan_sas, m)?)?;
    m.add_function(wrap_pyfunction!(benchmark_batch_to_dataframe, m)?)?;
    m.add_function(wrap_pyfunction!(benchmark_scan_to_dataframes, m)?)?;
    m.add_function(wrap_pyfunction!(benchmark_dataframe_to_python, m)?)?;
    Ok(())
}
