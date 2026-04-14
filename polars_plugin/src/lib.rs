#![allow(clippy::module_name_repetitions)]

#[cfg(feature = "arrow")]
use std::{
    sync::{mpsc, Arc, Mutex},
    thread,
};

#[cfg(feature = "arrow")]
use arrow_schema::{DataType, TimeUnit};
#[cfg(feature = "arrow")]
use polars::prelude::DataFrame;
#[cfg(feature = "arrow")]
use polars_arrow::{
    array::{Array, BinaryArray, PrimitiveArray, Utf8Array},
    bitmap::MutableBitmap,
    datatypes::{ArrowDataType, ArrowSchema, Field, TimeUnit as PlTimeUnit},
    offset::OffsetsBuffer,
    record_batch::RecordBatch as PolarsRecordBatch,
};
#[cfg(feature = "arrow")]
use pyo3::{
    exceptions::{PyRuntimeError, PyStopIteration, PyValueError},
    prelude::*,
    types::{PyDict, PyModule},
    IntoPyObjectExt,
};
#[cfg(feature = "arrow")]
use pyo3_polars::types::PyDataFrame;
#[cfg(feature = "arrow")]
use sas7bdat_simd::{BatchHint, Dataset, Error, OwnedColumnBuffer, Projection, Result as SasResult};

// ─── message types ────────────────────────────────────────────────────────────

#[cfg(feature = "arrow")]
enum ReaderMessage {
    Batch(DataFrame),
    Error(String),
}

// ─── SasDataset ───────────────────────────────────────────────────────────────

/// A pre-opened SAS7BDAT dataset.  Opening a 2 GB file parses the full page
/// index and can take hundreds of milliseconds; wrapping the result in this
/// object lets you pay that cost once and reuse the metadata for many scans.
///
/// ```python
/// ds = sp.SasDataset("big.sas7bdat")   # pay the parse cost here
/// df1 = ds.scan_sas().select(cols1).collect()   # fast
/// df2 = ds.scan_sas().filter(expr).collect()    # fast
/// ```
#[cfg(feature = "arrow")]
#[pyclass]
struct SasDataset {
    ds: Arc<Dataset>,
}

#[cfg(feature = "arrow")]
#[pymethods]
impl SasDataset {
    /// Open a SAS7BDAT file and parse its metadata.  The GIL is released
    /// while the file is parsed so other Python threads can run.
    #[new]
    fn open(py: Python<'_>, path: &str) -> PyResult<Self> {
        #[allow(deprecated)]
        let ds = py
            .allow_threads(|| Dataset::open(path))
            .map_err(py_err)?;
        Ok(Self { ds: Arc::new(ds) })
    }

    /// Return the Polars `Schema` for this dataset.
    fn schema(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        schema_for_dataset(py, &self.ds)
    }

    /// Return a streaming `BatchReader` backed by this pre-opened dataset.
    fn batch_reader(
        &self,
        py: Python<'_>,
        with_columns: Option<Vec<String>>,
        predicate: Option<Py<PyAny>>,
        n_rows: Option<usize>,
        batch_size: Option<usize>,
    ) -> BatchReader {
        batch_reader_from_dataset(py, &self.ds, with_columns, predicate, n_rows, batch_size, false)
    }

    /// Register this dataset as a Polars IO source and return a `LazyFrame`.
    fn scan_sas(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let schema = schema_for_dataset(py, &self.ds)?;
        register_io_source(py, Arc::clone(&self.ds), schema)
    }
}

// ─── SasIoSource (internal Polars IO plugin callback) ─────────────────────────

/// Registered with `polars.io.plugins.register_io_source`; called by the
/// Polars query planner with pushdown information.  Holds an `Arc<Dataset>`
/// so it can be cheaply called multiple times without re-opening the file.
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
        // Coalesce all batches into one DataFrame inside the Rust background
        // thread — eliminates N-1 GIL round-trips for the scan_sas path.
        batch_reader_from_dataset(py, &self.ds, with_columns, predicate, n_rows, batch_size, true)
    }
}

// ─── BatchReader ──────────────────────────────────────────────────────────────

/// A streaming batch iterator returned to Python.
///
/// `rx` is behind a `Mutex` (not `RefCell`) so the type is `Send + Sync`,
/// which lets `__next__` release the GIL while waiting for the next batch.
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
            // Release the GIL while waiting for the background scan thread.
            // allow_threads is deprecated in pyo3 0.27 (replacement: detach)
            // but the new API is not yet stable; suppress until pyo3 0.28.
            #[allow(deprecated)]
            let message = py
                .allow_threads(|| self.rx.lock().unwrap().recv())
                .map_err(|_| PyStopIteration::new_err("end of stream"))?;

            match message {
                ReaderMessage::Batch(df) => {
                    let py_df = PyDataFrame(df).into_py_any(py)?;
                    if let Some(predicate) = &self.predicate {
                        // DataFrame.filter() is cheaper than lazy().filter().collect()
                        // because it avoids creating an intermediate LazyFrame and
                        // running the full query planner per batch.
                        let filtered =
                            py_df.bind(py).call_method1("filter", (predicate.bind(py),))?;
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
    #[allow(deprecated)]
    let ds = py.allow_threads(|| Dataset::open(path)).map_err(py_err)?;
    schema_for_dataset(py, &ds)
}

/// Convenience entry-point: opens the file then returns a streaming
/// `BatchReader`.  For repeated scans of the same file use `SasDataset`
/// instead to avoid paying the open cost on every call.
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
    #[allow(deprecated)]
    let ds = py.allow_threads(|| Dataset::open(path)).map_err(py_err)?;
    let ds = Arc::new(ds);
    Ok(batch_reader_from_dataset(
        py,
        &ds,
        with_columns,
        predicate,
        n_rows,
        batch_size,
        false,
    ))
}

/// Register this path as a Polars IO source and return a `LazyFrame`.
/// For repeated scans use `SasDataset.scan_sas()` to avoid re-opening.
#[cfg(feature = "arrow")]
#[pyfunction]
fn scan_sas(py: Python<'_>, path: &str) -> PyResult<Py<PyAny>> {
    #[allow(deprecated)]
    let ds = py.allow_threads(|| Dataset::open(path)).map_err(py_err)?;
    let ds = Arc::new(ds);
    let schema = schema_for_dataset(py, &ds)?;
    register_io_source(py, ds, schema)
}

// ─── internal helpers ─────────────────────────────────────────────────────────

#[cfg(feature = "arrow")]
fn register_io_source(
    py: Python<'_>,
    ds: Arc<Dataset>,
    schema: Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    let io_source = Py::new(py, SasIoSource { ds })?;
    let register_io_source =
        PyModule::import(py, "polars.io.plugins")?.getattr("register_io_source")?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("io_source", io_source)?;
    kwargs.set_item("schema", schema)?;
    kwargs.set_item("validate_schema", true)?;
    kwargs.set_item("is_pure", true)?;
    Ok(register_io_source.call((), Some(&kwargs))?.unbind())
}

#[cfg(feature = "arrow")]
fn schema_for_dataset(py: Python<'_>, ds: &Dataset) -> PyResult<Py<PyAny>> {
    let schema = ds.scan().arrow_schema().map_err(py_err)?;
    // Import the polars module once and reuse for every column — avoids N
    // redundant dict-lookup + refcount round-trips when building the schema.
    let polars = PyModule::import(py, "polars")?;
    let dict = PyDict::new(py);
    for field in schema.fields() {
        dict.set_item(field.name(), polars_dtype(&polars, field.data_type())?)?;
    }
    Ok(polars.getattr("Schema")?.call1((dict,))?.unbind())
}

#[cfg(feature = "arrow")]
fn batch_reader_from_dataset(
    _py: Python<'_>,
    ds: &Arc<Dataset>,
    with_columns: Option<Vec<String>>,
    predicate: Option<Py<PyAny>>,
    n_rows: Option<usize>,
    batch_size: Option<usize>,
    coalesce: bool,
) -> BatchReader {
    let (tx, rx) = mpsc::channel::<ReaderMessage>();
    let ds = Arc::clone(ds);

    thread::spawn(move || {
        let result = run_scan(&ds, with_columns, n_rows, batch_size, coalesce, &tx);
        if let Err(err) = result {
            let _ = tx.send(ReaderMessage::Error(err.to_string()));
        }
    });

    BatchReader {
        rx: Mutex::new(rx),
        predicate,
    }
}

// ─── scan pipeline ────────────────────────────────────────────────────────────

/// Scan the dataset in a background thread.  Each decoded [`OwnedColumnarBatch`]
/// is converted directly to a Polars [`DataFrame`] using polars-arrow types —
/// no arrow-rs FFI round-trip involved — and sent to the Python thread.
///
/// When `coalesce` is true all batches are merged in the Rust thread into a
/// single [`DataFrame`] before sending, reducing GIL round-trips from N to 1.
#[cfg(feature = "arrow")]
fn run_scan(
    ds: &Dataset,
    with_columns: Option<Vec<String>>,
    n_rows: Option<usize>,
    batch_size: Option<usize>,
    coalesce: bool,
    tx: &mpsc::Sender<ReaderMessage>,
) -> SasResult<()> {
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

    // Build the polars-arrow schema once from metadata; clone the Arc per batch
    // to avoid per-batch field-vec allocation and duplicate-name checks.
    let arrow_schema = scan.arrow_schema()?;
    let field_names: Vec<String> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().to_owned())
        .collect();
    let pl_schema = Arc::new(build_polars_schema(&arrow_schema, &field_names)?);

    if coalesce {
        // Accumulate every OwnedColumnarBatch → DataFrame in Rust; vstack into
        // one contiguous frame before handing control back to Python.
        let mut frames: Vec<DataFrame> = Vec::new();
        scan.visit_owned_batches(|batch| {
            let df = owned_batch_to_dataframe(batch, Arc::clone(&pl_schema))
                .map_err(|e| Error::io(e.to_string()))?;
            frames.push(df);
            Ok(std::ops::ControlFlow::Continue(()))
        })?;
        if !frames.is_empty() {
            let combined = vstack_frames(frames).map_err(|e| Error::io(e.to_string()))?;
            let _ = tx.send(ReaderMessage::Batch(combined));
        }
    } else {
        scan.visit_owned_batches(|batch| {
            let df = owned_batch_to_dataframe(batch, Arc::clone(&pl_schema))
                .map_err(|e| Error::io(e.to_string()))?;
            tx.send(ReaderMessage::Batch(df))
                .map_err(|err| Error::io(err.to_string()))?;
            Ok(std::ops::ControlFlow::Continue(()))
        })?;
    }
    Ok(())
}

/// Build a polars-arrow `ArrowSchema` from an arrow-rs schema, applying the
/// same dtype widening that `owned_batch_to_dataframe` uses (Utf8 → `LargeUtf8`,
/// Binary → `LargeBinary`).
#[cfg(feature = "arrow")]
fn build_polars_schema(
    arrow_schema: &arrow_schema::Schema,
    field_names: &[String],
) -> SasResult<ArrowSchema> {
    let fields: Vec<Field> = arrow_schema
        .fields()
        .iter()
        .zip(field_names.iter())
        .map(|(f, name)| {
            let dtype = arrow_dt_to_polars_arrow(f.data_type())
                .map_err(Error::arrow)?;
            Ok(Field::new(name.as_str().into(), dtype, true))
        })
        .collect::<SasResult<Vec<_>>>()?;
    ArrowSchema::from_iter_check_duplicates(fields).map_err(|e| Error::arrow(e.to_string()))
}

#[cfg(feature = "arrow")]
fn arrow_dt_to_polars_arrow(dt: &DataType) -> Result<ArrowDataType, String> {
    Ok(match dt {
        DataType::Int32 => ArrowDataType::Int32,
        DataType::Int64 => ArrowDataType::Int64,
        DataType::Float64 => ArrowDataType::Float64,
        // sas7bdat-simd emits Utf8/Binary; widen to 64-bit offsets to match
        // the arrays we build in owned_batch_to_dataframe.
        DataType::Utf8 | DataType::LargeUtf8 => ArrowDataType::LargeUtf8,
        DataType::Binary | DataType::LargeBinary => ArrowDataType::LargeBinary,
        DataType::Date32 => ArrowDataType::Date32,
        DataType::Time32(TimeUnit::Second) => ArrowDataType::Time32(PlTimeUnit::Second),
        DataType::Timestamp(TimeUnit::Second, None) => {
            ArrowDataType::Timestamp(PlTimeUnit::Second, None)
        }
        other => return Err(format!("unsupported Arrow type: {other:?}")),
    })
}

/// Vstack a non-empty list of `DataFrames` into one.  When there is only a
/// single frame (most common for small files with one batch) no copy occurs.
#[cfg(feature = "arrow")]
fn vstack_frames(mut frames: Vec<DataFrame>) -> Result<DataFrame, polars::prelude::PolarsError> {
    if frames.len() == 1 {
        return Ok(frames.remove(0));
    }
    let mut out = frames.remove(0);
    for frame in frames {
        out.vstack_mut(&frame)?;
    }
    Ok(out)
}

/// Convert an [`OwnedColumnarBatch`] directly to a Polars [`DataFrame`] using
/// polars-arrow types.  The conversion hot-path for each column type is:
///
/// - **I32 / I64 / F64**: `Vec<T>` moves zero-copy into a `Buffer<T>`.
/// - **Date / `DateTime` / Time**: one flat `Vec` copy to extract the inner
///   scalar, then a zero-copy move into a `Buffer<T>`.
/// - **Utf8 / `RawBytes`**: one `Vec<u32> → Vec<i64>` offset-widening copy; the
///   data `Vec<u8>` moves zero-copy into a `Buffer<u8>`.
/// - **Nulls**: built into a `Bitmap` via `bits_to_bitmap` (only when nulls
///   are actually present; the fast path returns `None`).
///
/// The schema `Arc` is pre-built once per scan and cloned here — no per-batch
/// field-vec allocation or duplicate-name check.
#[cfg(feature = "arrow")]
fn owned_batch_to_dataframe(
    batch: sas7bdat_simd::OwnedColumnarBatch,
    schema: Arc<ArrowSchema>,
) -> SasResult<DataFrame> {
    let row_count = batch.row_count;
    let mut arrays: Vec<Box<dyn Array>> = Vec::with_capacity(batch.columns.len());

    for col in batch.columns {
        let array: Box<dyn Array> = match col {
            OwnedColumnBuffer::I32 { values, valid } => {
                let bitmap = valid.map(|v| bits_to_bitmap(&v, row_count));
                Box::new(PrimitiveArray::new(ArrowDataType::Int32, values.into(), bitmap))
            }
            OwnedColumnBuffer::I64 { values, valid } => {
                let bitmap = valid.map(|v| bits_to_bitmap(&v, row_count));
                Box::new(PrimitiveArray::new(ArrowDataType::Int64, values.into(), bitmap))
            }
            OwnedColumnBuffer::F64 { values, valid } => {
                let bitmap = valid.map(|v| bits_to_bitmap(&v, row_count));
                Box::new(PrimitiveArray::new(ArrowDataType::Float64, values.into(), bitmap))
            }
            OwnedColumnBuffer::Date { values, valid } => {
                // SasDate wraps i32 day-since-epoch; extract with one flat copy.
                let i32s: Vec<i32> =
                    values.into_iter().map(|d| d.days_since_sas_epoch).collect();
                let bitmap = valid.map(|v| bits_to_bitmap(&v, row_count));
                Box::new(PrimitiveArray::new(ArrowDataType::Date32, i32s.into(), bitmap))
            }
            OwnedColumnBuffer::DateTime { values, valid } => {
                // SasDateTime wraps i64 seconds; extract with one flat copy.
                let i64s: Vec<i64> =
                    values.into_iter().map(|d| d.seconds_since_sas_epoch).collect();
                let bitmap = valid.map(|v| bits_to_bitmap(&v, row_count));
                let dtype = ArrowDataType::Timestamp(PlTimeUnit::Second, None);
                Box::new(PrimitiveArray::new(dtype, i64s.into(), bitmap))
            }
            OwnedColumnBuffer::Time { values, valid } => {
                // SasTime stores i64 seconds-since-midnight; Arrow Time32 is i32.
                let i32s: Vec<i32> = values
                    .into_iter()
                    .map(|t| i32::try_from(t.seconds_since_midnight).unwrap_or(i32::MAX))
                    .collect();
                let bitmap = valid.map(|v| bits_to_bitmap(&v, row_count));
                let dtype = ArrowDataType::Time32(PlTimeUnit::Second);
                Box::new(PrimitiveArray::new(dtype, i32s.into(), bitmap))
            }
            OwnedColumnBuffer::Utf8 {
                offsets,
                data,
                valid,
                ..
            } => {
                // Widen u32 → i64 offsets (one copy); data Vec<u8> moves zero-copy.
                let i64_offs: Vec<i64> = offsets.into_iter().map(i64::from).collect();
                // SAFETY: our offsets satisfy OffsetsBuffer invariants:
                // monotonically non-decreasing, first element == 0.
                let offs_buf = unsafe { OffsetsBuffer::new_unchecked(i64_offs.into()) };
                let bitmap = valid.map(|v| bits_to_bitmap(&v, row_count));
                Box::new(Utf8Array::<i64>::new(
                    ArrowDataType::LargeUtf8,
                    offs_buf,
                    data.into(),
                    bitmap,
                ))
            }
            OwnedColumnBuffer::RawBytes {
                offsets,
                data,
                valid,
            } => {
                let i64_offs: Vec<i64> = offsets.into_iter().map(i64::from).collect();
                // SAFETY: same offset invariant as Utf8 above.
                let offs_buf = unsafe { OffsetsBuffer::new_unchecked(i64_offs.into()) };
                let bitmap = valid.map(|v| bits_to_bitmap(&v, row_count));
                Box::new(BinaryArray::<i64>::new(
                    ArrowDataType::LargeBinary,
                    offs_buf,
                    data.into(),
                    bitmap,
                ))
            }
        };
        arrays.push(array);
    }

    let rec = PolarsRecordBatch::try_new(row_count, schema, arrays)
        .map_err(|e| Error::arrow(e.to_string()))?;
    Ok(DataFrame::from(rec))
}

/// Convert a bit-packed validity `Vec<u64>` (LSB-first, same layout as Arrow
/// bitmaps on little-endian hardware) into a polars-arrow [`Bitmap`].
///
/// On little-endian hardware (all Apple Silicon / x86-64) our u64 validity
/// words have exactly the same byte layout as an Arrow bitmap, so we only
/// need to reinterpret the raw bytes rather than push bits one by one.
/// This reduces bitmap construction from O(n) individual push calls to a
/// single O(n/8) byte copy.
#[cfg(feature = "arrow")]
fn bits_to_bitmap(bits: &[u64], len: usize) -> polars_arrow::bitmap::Bitmap {
    let bytes: Vec<u8> = bits.iter().flat_map(|w| w.to_le_bytes()).collect();
    // from_vec: length must be <= bytes.len() * 8, which holds because we
    // allocate ceil(len/64) words = ceil(len/8) bytes for len bits.
    MutableBitmap::from_vec(bytes, len).into()
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

/// Map an arrow-rs [`DataType`] to the equivalent Polars Python dtype object.
/// Takes a pre-imported `polars` module to avoid re-importing it per column.
#[cfg(feature = "arrow")]
fn polars_dtype(polars: &Bound<'_, PyModule>, data_type: &DataType) -> PyResult<Py<PyAny>> {
    let dtype = match data_type {
        DataType::Int32 => polars.getattr("Int32")?.unbind(),
        DataType::Int64 => polars.getattr("Int64")?.unbind(),
        DataType::Float64 => polars.getattr("Float64")?.unbind(),
        DataType::Utf8 => polars.getattr("Utf8")?.unbind(),
        DataType::Binary => polars.getattr("Binary")?.unbind(),
        DataType::Date32 => polars.getattr("Date")?.unbind(),
        DataType::Time32(TimeUnit::Second) => polars.getattr("Time")?.unbind(),
        DataType::Timestamp(TimeUnit::Second, None) => {
            polars.getattr("Datetime")?.call1(("us",))?.unbind()
        }
        other => {
            return Err(PyValueError::new_err(format!(
                "unsupported Arrow type for Polars schema: {other:?}"
            )));
        }
    };
    Ok(dtype)
}

#[cfg(feature = "arrow")]
fn py_err(err: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(err.to_string())
}

// ─── module registration ──────────────────────────────────────────────────────

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
