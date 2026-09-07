//! The compiled half of `sas7bdat_polars`: a SAS7BDAT dataset that hands its
//! rows to Python as Arrow C streams.
//!
//! Nothing here knows about polars. A stream object exposes
//! `__arrow_c_stream__`, the Arrow `PyCapsule` Interface, and whatever consumes
//! that (polars, pyarrow, pandas, duckdb) imports the batches through Arrow's C
//! Data Interface with no copy of the primitive buffers. The polars-facing API is
//! the pure-Python layer in `python/sas7bdat_polars/`, built on polars' public
//! constructors, which is what keeps this wheel independent of the polars
//! release installed beside it.

mod scan;

use arrow_array::RecordBatch;
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use pyo3::exceptions::{PyRuntimeError, PyStopIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyDict};
use sas7bdat::{Dataset, LogicalType};
use scan::{ScanReader, ScanSpec};
use std::collections::HashMap;
use std::ffi::CString;
use std::sync::{Arc, Mutex};

/// v3: every batch crosses as an Arrow C stream; `schema_overrides` values are
/// type names; the polars API is the Python layer. v2 added `columns=` and
/// `schema_overrides=` on `scan_sas`, which the layer keeps.
const PLUGIN_CONTRACT_VERSION: &str = "sas7bdat_polars.v3";

/// The capsule name the Arrow `PyCapsule` Interface specifies for a stream.
const STREAM_CAPSULE_NAME: &str = "arrow_array_stream";

fn value_error(err: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(err.to_string())
}

// ─── SasDataset ───────────────────────────────────────────────────────────────

/// An opened SAS7BDAT file: parsed metadata, an optional format catalog, and
/// any schema overrides, reused by every stream taken from it.
#[pyclass(frozen)]
struct SasDataset {
    ds: Arc<Dataset>,
    path: String,
}

#[pymethods]
impl SasDataset {
    #[new]
    #[pyo3(signature = (path, catalog_path=None, schema_overrides=None))]
    fn open(
        py: Python<'_>,
        path: &str,
        catalog_path: Option<&str>,
        schema_overrides: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        let mut ds = py.detach(|| Dataset::open(path)).map_err(value_error)?;
        if let Some(catalog) = catalog_path {
            ds.attach_catalog(catalog).map_err(value_error)?;
        }
        if let Some(overrides) = schema_overrides {
            let parsed = overrides
                .into_iter()
                .map(|(name, type_name)| Ok((name, logical_type_from_name(&type_name)?)))
                .collect::<PyResult<Vec<_>>>()?;
            ds.apply_schema_overrides(parsed).map_err(value_error)?;
        }
        Ok(Self {
            ds: Arc::new(ds),
            path: path.to_owned(),
        })
    }

    /// The source column names, in file order.
    #[getter]
    fn column_names(&self) -> Vec<String> {
        self.ds
            .columns()
            .iter()
            .map(|column| column.name.clone())
            .collect()
    }

    /// Header-level facts about the file: row and column counts, encoding,
    /// compression, page layout. No rows are decoded.
    fn info<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        info_dict(py, &self.ds, &self.path)
    }

    /// A stream of the file's rows, as `columns` in that order (or every column),
    /// at most `n_rows` of them, in batches of about `batch_size` rows.
    ///
    /// The stream decodes on its own thread from the moment a consumer asks for
    /// it, and stops if the consumer lets go of it early.
    #[pyo3(signature = (columns=None, n_rows=None, batch_size=None))]
    fn stream(
        &self,
        columns: Option<Vec<String>>,
        n_rows: Option<usize>,
        batch_size: Option<usize>,
    ) -> PyResult<ArrowStream> {
        let spec = ScanSpec {
            columns,
            n_rows,
            batch_size,
        };
        // Resolve the schema now so an unknown column name fails here, at the
        // call, rather than inside the consumer's import.
        scan::stream_schema(&self.ds, &spec).map_err(value_error)?;
        Ok(ArrowStream::new(StreamSource::Scan {
            ds: Arc::clone(&self.ds),
            spec,
        }))
    }

    /// A stream with the file's schema and no rows: how a consumer learns the
    /// schema without a decode.
    #[pyo3(signature = (columns=None))]
    fn schema_stream(&self, columns: Option<Vec<String>>) -> PyResult<ArrowStream> {
        let spec = ScanSpec {
            columns,
            ..ScanSpec::default()
        };
        let schema = scan::stream_schema(&self.ds, &spec).map_err(value_error)?;
        Ok(ArrowStream::new(StreamSource::Schema(schema)))
    }

    /// The same rows as `stream`, one batch at a time, each batch its own
    /// single-batch stream. For a consumer that wants to work per batch.
    #[pyo3(signature = (columns=None, n_rows=None, batch_size=None))]
    fn batches(
        &self,
        columns: Option<Vec<String>>,
        n_rows: Option<usize>,
        batch_size: Option<usize>,
    ) -> PyResult<BatchIterator> {
        let spec = ScanSpec {
            columns,
            n_rows,
            batch_size,
        };
        let reader = ScanReader::start(Arc::clone(&self.ds), spec).map_err(value_error)?;
        Ok(BatchIterator {
            reader: Mutex::new(reader),
        })
    }
}

fn logical_type_from_name(name: &str) -> PyResult<LogicalType> {
    Ok(match name.trim().to_ascii_lowercase().as_str() {
        "int64" | "integer" | "int" => LogicalType::Integer,
        "float64" | "float" | "double" => LogicalType::Float,
        "date" => LogicalType::Date,
        "datetime" | "timestamp" => LogicalType::DateTime,
        "time" | "duration" => LogicalType::Time,
        "string" | "utf8" | "str" => LogicalType::String,
        "binary" | "bytes" => LogicalType::Bytes,
        other => {
            return Err(PyValueError::new_err(format!(
                "unsupported schema override type {other:?}; one of int64, float64, date, \
                 datetime, time, string, binary"
            )));
        }
    })
}

fn info_dict<'py>(py: Python<'py>, ds: &Dataset, path: &str) -> PyResult<Bound<'py, PyDict>> {
    let meta = ds.metadata();
    let info = PyDict::new(py);
    info.set_item("path", path)?;
    info.set_item("n_rows", meta.row_count)?;
    info.set_item("n_columns", ds.columns().len())?;
    info.set_item("row_length_bytes", meta.row_len)?;
    info.set_item("page_count", meta.page_count)?;
    info.set_item("encoding", meta.encoding.clone())?;
    info.set_item("compression", format!("{:?}", meta.compression))?;
    // The ROW_SIZE subheader's own rows-per-page, which some writers leave at 0.
    // Reporting it is how a delivery can be checked for the shape without
    // decoding anything.
    info.set_item("rows_per_page", ds.declared_rows_per_page())?;
    if let Ok(fs_meta) = std::fs::metadata(path) {
        info.set_item("size_bytes", fs_meta.len())?;
    }
    Ok(info)
}

// ─── ArrowStream ──────────────────────────────────────────────────────────────

enum StreamSource {
    Scan { ds: Arc<Dataset>, spec: ScanSpec },
    Schema(arrow_schema::SchemaRef),
    Batch(RecordBatch),
}

/// An object a consumer imports through `__arrow_c_stream__`. Consumable once:
/// the decode behind a scan cannot be rewound, and the interface says a second
/// call may fail.
#[pyclass(frozen)]
struct ArrowStream {
    source: Mutex<Option<StreamSource>>,
}

impl ArrowStream {
    fn new(source: StreamSource) -> Self {
        Self {
            source: Mutex::new(Some(source)),
        }
    }
}

/// `FFI_ArrowArrayStream` holds raw pointers and so is not `Send`, and a capsule's
/// payload must be. The pointers are to the boxed `ScanReader` this crate made,
/// which is `Send`, and to the C callbacks arrow-array installed; nothing in it
/// is tied to the thread that built it.
// The field is read through the capsule's pointer by the consumer's C code, which
// rustc cannot see.
#[allow(dead_code)]
struct SendStream(FFI_ArrowArrayStream);

// SAFETY: see the type's doc comment. The private data is a `Box<dyn
// RecordBatchReader + Send>` created by `FFI_ArrowArrayStream::new`, and the
// callbacks are arrow-array's, so moving the struct between threads moves
// nothing thread-affine.
#[allow(unsafe_code)]
unsafe impl Send for SendStream {}

#[pymethods]
impl ArrowStream {
    /// The Arrow `PyCapsule` Interface entry point. `requested_schema` is accepted
    /// and not applied: the interface allows that, and projection is chosen when
    /// the stream is made.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let source = self
            .source
            .lock()
            .map_err(|_| PyRuntimeError::new_err("stream state poisoned"))?
            .take()
            .ok_or_else(|| {
                PyRuntimeError::new_err(
                    "this stream was already consumed; take a new one from the dataset",
                )
            })?;
        let reader = match source {
            StreamSource::Scan { ds, spec } => ScanReader::start(ds, spec).map_err(value_error)?,
            StreamSource::Schema(schema) => ScanReader::empty(schema),
            StreamSource::Batch(batch) => ScanReader::single(batch),
        };
        let stream = FFI_ArrowArrayStream::new(Box::new(reader));
        let name = CString::new(STREAM_CAPSULE_NAME).expect("capsule name has no NUL");
        PyCapsule::new(py, SendStream(stream), Some(name))
    }
}

// ─── BatchIterator ────────────────────────────────────────────────────────────

/// Iterates a scan batch by batch, yielding each as a single-batch stream. The
/// receiver is `Send` but not `Sync`, and pyo3 wants both, hence the mutex; it
/// is only ever reached through `&mut self`.
#[pyclass]
struct BatchIterator {
    reader: Mutex<ScanReader>,
}

#[pymethods]
impl BatchIterator {
    const fn __iter__(slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<ArrowStream> {
        let reader = slf
            .reader
            .get_mut()
            .map_err(|_| PyRuntimeError::new_err("batch iterator poisoned"))?;
        // The decode thread does not need the GIL, and the consumer may be
        // another Python thread that does.
        let next = py.detach(move || reader.next());
        match next {
            None => Err(PyStopIteration::new_err("end of stream")),
            Some(Err(err)) => Err(PyRuntimeError::new_err(err.to_string())),
            Some(Ok(batch)) => Ok(ArrowStream::new(StreamSource::Batch(batch))),
        }
    }
}

// ─── module ───────────────────────────────────────────────────────────────────

/// Header-level facts about a file, without opening a dataset object.
#[pyfunction]
#[pyo3(signature = (path, catalog_path=None))]
fn sas_info<'py>(
    py: Python<'py>,
    path: &str,
    catalog_path: Option<&str>,
) -> PyResult<Bound<'py, PyDict>> {
    let mut ds = py.detach(|| Dataset::open(path)).map_err(value_error)?;
    if let Some(catalog) = catalog_path {
        ds.attach_catalog(catalog).map_err(value_error)?;
    }
    info_dict(py, &ds, path)
}

#[pymodule]
fn sas7bdat_polars(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    // The wheel's version and the reader's are separate lines that look alike;
    // this is the only way a caller can tell which core it has.
    m.add("__core_version__", sas7bdat::VERSION)?;
    m.add("PLUGIN_CONTRACT_VERSION", PLUGIN_CONTRACT_VERSION)?;
    m.add_class::<SasDataset>()?;
    m.add_class::<ArrowStream>()?;
    m.add_class::<BatchIterator>()?;
    m.add_function(wrap_pyfunction!(sas_info, m)?)?;
    Ok(())
}
