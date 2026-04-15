mod convert;
mod predicate;
mod scan;

#[cfg(feature = "arrow")]
use std::sync::{Arc, Mutex, mpsc};

#[cfg(feature = "arrow")]
use polars::frame::DataFrame;
#[cfg(feature = "arrow")]
use pyo3::{
    IntoPyObjectExt,
    exceptions::{PyRuntimeError, PyStopIteration, PyValueError},
    prelude::*,
    types::PyModule,
};
#[cfg(feature = "arrow")]
use pyo3_polars::types::PyDataFrame;
#[cfg(feature = "arrow")]
use sas7bdat_simd::Dataset;

// ─── message types ────────────────────────────────────────────────────────────

#[cfg(feature = "arrow")]
enum ReaderMessage {
    Batch(DataFrame),
    Error(String),
}

// ─── SasDataset ───────────────────────────────────────────────────────────────

/// A pre-opened SAS7BDAT dataset. Opening once lets repeated scans reuse the
/// parsed metadata instead of paying the open cost every time.
#[cfg(feature = "arrow")]
#[pyclass]
struct SasDataset {
    ds: Arc<Dataset>,
}

#[cfg(feature = "arrow")]
#[pymethods]
impl SasDataset {
    #[new]
    fn open(py: Python<'_>, path: &str) -> PyResult<Self> {
        let ds = py
            .detach(|| Dataset::open(path))
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        Ok(Self { ds: Arc::new(ds) })
    }

    fn schema(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        scan::schema_for_dataset(py, &self.ds)
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
            with_columns,
            predicate,
            n_rows,
            batch_size,
            false,
        )
    }

    fn scan_sas(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let schema = scan::schema_for_dataset(py, &self.ds)?;
        scan::register_io_source(py, Arc::clone(&self.ds), schema)
    }
}

// ─── SasIoSource ──────────────────────────────────────────────────────────────

#[cfg(feature = "arrow")]
#[pyclass]
struct SasIoSource {
    ds: Arc<Dataset>,
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
#[pyclass]
struct BatchReader {
    rx: Mutex<mpsc::Receiver<ReaderMessage>>,
    predicate: Option<Py<PyAny>>,
}

#[cfg(feature = "arrow")]
#[pymethods]
impl BatchReader {
    const fn __iter__(slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        loop {
            let message = py
                .detach(|| self.rx.lock().unwrap().recv())
                .map_err(|_| PyStopIteration::new_err("end of stream"))?;

            match message {
                ReaderMessage::Batch(df) => {
                    let py_df = PyDataFrame(df).into_py_any(py)?;
                    if let Some(predicate) = &self.predicate {
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
    scan::register_io_source(py, ds, schema)
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
    Ok(())
}
