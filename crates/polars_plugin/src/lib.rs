#![allow(clippy::module_name_repetitions)]

#[cfg(feature = "arrow")]
use std::{
    sync::{Arc, Mutex, mpsc},
    thread,
};

#[cfg(feature = "arrow")]
use arrow_schema::{DataType, TimeUnit};
#[cfg(feature = "arrow")]
use polars::prelude::{
    AnyValue, BooleanChunked, ChunkCompareEq, ChunkCompareIneq, ChunkFull, Column, DataFrame,
    DataType as PlDataType, PlSmallStr, Scalar,
};
#[cfg(feature = "arrow")]
use polars_arrow::{
    array::{Array, BinaryArray, PrimitiveArray, Utf8Array},
    bitmap::MutableBitmap,
    datatypes::{ArrowDataType, ArrowSchema, Field, TimeUnit as PlTimeUnit},
    offset::OffsetsBuffer,
    record_batch::RecordBatch as PolarsRecordBatch,
};
#[cfg(feature = "arrow")]
use polars_plan::prelude::{
    BooleanFunction, DataTypeExpr, Expr, FunctionExpr, LiteralValue, Operator,
};
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
use rmp_serde::from_slice;
#[cfg(feature = "arrow")]
use sas7bdat_simd::{
    BatchHint, Dataset, Error, OwnedColumnBuffer, Projection, Result as SasResult,
};

// ─── message types ────────────────────────────────────────────────────────────

#[cfg(feature = "arrow")]
enum ReaderMessage {
    Batch(DataFrame),
    Error(String),
}

#[cfg(feature = "arrow")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompareOp {
    Eq,
    EqValidity,
    Ne,
    NeValidity,
    Gt,
    GtEq,
    Lt,
    LtEq,
}

#[cfg(feature = "arrow")]
#[derive(Debug, Clone)]
enum PredicateExpr {
    Const(bool),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Not(Box<Self>),
    Compare {
        left: PredicateOperand,
        op: CompareOp,
        right: PredicateOperand,
    },
    IsNull(PredicateOperand),
    IsNotNull(PredicateOperand),
    IsFinite(PredicateOperand),
    IsInfinite(PredicateOperand),
    IsNan(PredicateOperand),
    IsNotNan(PredicateOperand),
}

#[cfg(feature = "arrow")]
#[derive(Debug, Clone)]
enum PredicateOperand {
    Column {
        name: String,
        cast: Option<PlDataType>,
    },
    Scalar {
        value: Scalar,
        cast: Option<PlDataType>,
    },
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
        let ds = py.allow_threads(|| Dataset::open(path)).map_err(py_err)?;
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
        batch_reader_from_dataset(
            py,
            &self.ds,
            with_columns,
            predicate,
            n_rows,
            batch_size,
            false,
        )
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
        batch_reader_from_dataset(
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
fn register_io_source(py: Python<'_>, ds: Arc<Dataset>, schema: Py<PyAny>) -> PyResult<Py<PyAny>> {
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
    py: Python<'_>,
    ds: &Arc<Dataset>,
    with_columns: Option<Vec<String>>,
    predicate: Option<Py<PyAny>>,
    n_rows: Option<usize>,
    batch_size: Option<usize>,
    coalesce: bool,
) -> BatchReader {
    let (tx, rx) = mpsc::channel::<ReaderMessage>();
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
        rx: Mutex::new(rx),
        predicate: python_predicate,
    }
}

#[cfg(feature = "arrow")]
fn prepare_predicate(
    py: Python<'_>,
    ds: &Dataset,
    predicate: Option<Py<PyAny>>,
) -> (Option<PredicateExpr>, Option<Py<PyAny>>) {
    let Some(predicate) = predicate else {
        return (None, None);
    };

    predicate_from_python(py, ds, &predicate).map_or_else(
        || (None, Some(predicate)),
        |predicate| (Some(predicate), None),
    )
}

#[cfg(feature = "arrow")]
fn predicate_from_python(
    py: Python<'_>,
    ds: &Dataset,
    predicate: &Py<PyAny>,
) -> Option<PredicateExpr> {
    let predicate = predicate.bind(py);
    let meta = predicate.getattr("meta").ok()?;
    let serialized = meta.call_method0("serialize").ok()?;
    let serialized: Vec<u8> = serialized.extract().ok()?;
    let expr: Expr = from_slice(&serialized).ok()?;
    parse_predicate_expr(ds, &expr)
}

#[cfg(feature = "arrow")]
fn append_unique_columns(base: &mut Vec<String>, extra: &[String]) {
    for column in extra {
        if !base.iter().any(|existing| existing == column) {
            base.push(column.clone());
        }
    }
}

#[cfg(feature = "arrow")]
fn parse_predicate_expr(ds: &Dataset, expr: &Expr) -> Option<PredicateExpr> {
    use BooleanFunction as B;
    use Operator as O;

    match expr {
        Expr::Literal(literal) => predicate_const(literal),
        Expr::Alias(inner, _) | Expr::KeepName(inner) | Expr::RenameAlias { expr: inner, .. } => {
            parse_predicate_expr(ds, inner)
        }
        Expr::BinaryExpr { left, op, right } if op.is_comparison() => {
            Some(PredicateExpr::Compare {
                left: parse_predicate_operand(ds, left.as_ref())?,
                op: compare_op(*op)?,
                right: parse_predicate_operand(ds, right.as_ref())?,
            })
        }
        Expr::BinaryExpr {
            left,
            op: O::And | O::LogicalAnd,
            right,
        } => Some(PredicateExpr::And(
            Box::new(parse_predicate_expr(ds, left.as_ref())?),
            Box::new(parse_predicate_expr(ds, right.as_ref())?),
        )),
        Expr::BinaryExpr {
            left,
            op: O::Or | O::LogicalOr,
            right,
        } => Some(PredicateExpr::Or(
            Box::new(parse_predicate_expr(ds, left.as_ref())?),
            Box::new(parse_predicate_expr(ds, right.as_ref())?),
        )),
        Expr::Function { input, function } if input.len() == 1 => match function {
            FunctionExpr::Boolean(B::Not) => Some(PredicateExpr::Not(Box::new(
                parse_predicate_expr(ds, &input[0])?,
            ))),
            FunctionExpr::Boolean(B::IsNull) => Some(PredicateExpr::IsNull(
                parse_predicate_operand(ds, &input[0])?,
            )),
            FunctionExpr::Boolean(B::IsNotNull) => Some(PredicateExpr::IsNotNull(
                parse_predicate_operand(ds, &input[0])?,
            )),
            FunctionExpr::Boolean(B::IsFinite) => Some(PredicateExpr::IsFinite(
                parse_predicate_operand(ds, &input[0])?,
            )),
            FunctionExpr::Boolean(B::IsInfinite) => Some(PredicateExpr::IsInfinite(
                parse_predicate_operand(ds, &input[0])?,
            )),
            FunctionExpr::Boolean(B::IsNan) => Some(PredicateExpr::IsNan(parse_predicate_operand(
                ds, &input[0],
            )?)),
            FunctionExpr::Boolean(B::IsNotNan) => Some(PredicateExpr::IsNotNan(
                parse_predicate_operand(ds, &input[0])?,
            )),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(feature = "arrow")]
fn parse_predicate_operand(ds: &Dataset, expr: &Expr) -> Option<PredicateOperand> {
    match expr {
        Expr::Column(name) => {
            ds.column(name.as_str())?;
            Some(PredicateOperand::Column {
                name: name.to_string(),
                cast: None,
            })
        }
        Expr::Literal(literal) => {
            literal_to_scalar(literal).map(|value| PredicateOperand::Scalar { value, cast: None })
        }
        Expr::Alias(inner, _) | Expr::KeepName(inner) | Expr::RenameAlias { expr: inner, .. } => {
            parse_predicate_operand(ds, inner)
        }
        Expr::Cast {
            expr: inner, dtype, ..
        } => {
            let cast = match dtype {
                DataTypeExpr::Literal(dtype) => Some(dtype.clone()),
                _ => None,
            }?;
            let mut operand = parse_predicate_operand(ds, inner.as_ref())?;
            operand.set_cast(cast);
            Some(operand)
        }
        _ => None,
    }
}

#[cfg(feature = "arrow")]
fn predicate_const(literal: &LiteralValue) -> Option<PredicateExpr> {
    let LiteralValue::Scalar(scalar) = literal.clone().materialize() else {
        return None;
    };
    match scalar.as_any_value() {
        AnyValue::Boolean(value) => Some(PredicateExpr::Const(value)),
        _ => None,
    }
}

#[cfg(feature = "arrow")]
fn literal_to_scalar(literal: &LiteralValue) -> Option<Scalar> {
    match literal.clone().materialize() {
        LiteralValue::Scalar(scalar) => Some(scalar),
        LiteralValue::Series(series) if series.len() == 1 => {
            let value = series.get(0).ok()?;
            Some(Scalar::new(value.dtype(), value.into_static()))
        }
        _ => None,
    }
}

#[cfg(feature = "arrow")]
const fn compare_op(op: Operator) -> Option<CompareOp> {
    Some(match op {
        Operator::Eq => CompareOp::Eq,
        Operator::EqValidity => CompareOp::EqValidity,
        Operator::NotEq => CompareOp::Ne,
        Operator::NotEqValidity => CompareOp::NeValidity,
        Operator::Gt => CompareOp::Gt,
        Operator::GtEq => CompareOp::GtEq,
        Operator::Lt => CompareOp::Lt,
        Operator::LtEq => CompareOp::LtEq,
        _ => return None,
    })
}

#[cfg(feature = "arrow")]
impl PredicateExpr {
    fn collect_columns(&self, columns: &mut Vec<String>) {
        match self {
            Self::Const(_) => (),
            Self::And(left, right) | Self::Or(left, right) => {
                left.collect_columns(columns);
                right.collect_columns(columns);
            }
            Self::Not(inner) => inner.collect_columns(columns),
            Self::Compare { left, right, .. } => {
                left.collect_columns(columns);
                right.collect_columns(columns);
            }
            Self::IsNull(operand)
            | Self::IsNotNull(operand)
            | Self::IsFinite(operand)
            | Self::IsInfinite(operand)
            | Self::IsNan(operand)
            | Self::IsNotNan(operand) => operand.collect_columns(columns),
        }
    }
}

#[cfg(feature = "arrow")]
impl PredicateOperand {
    fn collect_columns(&self, columns: &mut Vec<String>) {
        if let Self::Column { name, .. } = self
            && !columns.iter().any(|existing| existing == name)
        {
            columns.push(name.clone());
        }
    }

    fn set_cast(&mut self, cast: PlDataType) {
        match self {
            Self::Column { cast: slot, .. } | Self::Scalar { cast: slot, .. } => {
                *slot = Some(cast);
            }
        }
    }
}

#[cfg(feature = "arrow")]
fn filter_dataframe(df: &DataFrame, predicate: &PredicateExpr) -> SasResult<DataFrame> {
    let mask = evaluate_predicate(df, predicate)?;
    df.filter(&mask).map_err(|err| Error::io(err.to_string()))
}

#[cfg(feature = "arrow")]
fn evaluate_predicate(df: &DataFrame, predicate: &PredicateExpr) -> SasResult<BooleanChunked> {
    match predicate {
        PredicateExpr::Const(value) => {
            Ok(BooleanChunked::full(PlSmallStr::EMPTY, *value, df.height()))
        }
        PredicateExpr::And(left, right) => {
            let left = evaluate_predicate(df, left)?;
            let right = evaluate_predicate(df, right)?;
            Ok(&left & &right)
        }
        PredicateExpr::Or(left, right) => {
            let left = evaluate_predicate(df, left)?;
            let right = evaluate_predicate(df, right)?;
            Ok(&left | &right)
        }
        PredicateExpr::Not(inner) => Ok(!evaluate_predicate(df, inner)?),
        PredicateExpr::Compare { left, op, right } => {
            let left = resolve_operand(df, left)?;
            let right = resolve_operand(df, right)?;
            let mask = match op {
                CompareOp::Eq => left.equal(&right),
                CompareOp::EqValidity => left.equal_missing(&right),
                CompareOp::Ne => left.not_equal(&right),
                CompareOp::NeValidity => left.not_equal_missing(&right),
                CompareOp::Gt => left.gt(&right),
                CompareOp::GtEq => left.gt_eq(&right),
                CompareOp::Lt => left.lt(&right),
                CompareOp::LtEq => left.lt_eq(&right),
            }
            .map_err(|err| Error::io(err.to_string()))?;
            Ok(mask)
        }
        PredicateExpr::IsNull(operand) => Ok(resolve_operand(df, operand)?.is_null()),
        PredicateExpr::IsNotNull(operand) => Ok(resolve_operand(df, operand)?.is_not_null()),
        PredicateExpr::IsFinite(operand) => resolve_operand(df, operand)?
            .is_finite()
            .map_err(|err| Error::io(err.to_string())),
        PredicateExpr::IsInfinite(operand) => resolve_operand(df, operand)?
            .is_infinite()
            .map_err(|err| Error::io(err.to_string())),
        PredicateExpr::IsNan(operand) => resolve_operand(df, operand)?
            .is_nan()
            .map_err(|err| Error::io(err.to_string())),
        PredicateExpr::IsNotNan(operand) => Ok(!resolve_operand(df, operand)?
            .is_nan()
            .map_err(|err| Error::io(err.to_string()))?),
    }
}

#[cfg(feature = "arrow")]
fn resolve_operand(df: &DataFrame, operand: &PredicateOperand) -> SasResult<Column> {
    match operand {
        PredicateOperand::Column { name, cast } => {
            let mut column = df
                .column(name)
                .map_err(|err| Error::io(err.to_string()))?
                .clone();
            if let Some(dtype) = cast {
                column = column
                    .cast(dtype)
                    .map_err(|err| Error::io(err.to_string()))?;
            }
            Ok(column)
        }
        PredicateOperand::Scalar { value, cast } => {
            let mut column = Column::new_scalar(PlSmallStr::EMPTY, value.clone(), df.height());
            if let Some(dtype) = cast {
                column = column
                    .cast(dtype)
                    .map_err(|err| Error::io(err.to_string()))?;
            }
            Ok(column)
        }
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
    predicate: Option<&PredicateExpr>,
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

    // Build the polars-arrow schema once from metadata and reuse it for each
    // batch conversion.
    let arrow_schema = scan.arrow_schema()?;
    let pl_schema = Arc::new(build_polars_schema(&arrow_schema)?);

    if coalesce {
        // Accumulate every OwnedColumnarBatch → DataFrame in Rust; vstack into
        // one contiguous frame before handing control back to Python.
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

/// Build a polars-arrow `ArrowSchema` from an arrow-rs schema, applying the
/// same dtype widening that `owned_batch_to_dataframe` uses (Utf8 → `LargeUtf8`,
/// Binary → `LargeBinary`).
#[cfg(feature = "arrow")]
fn build_polars_schema(arrow_schema: &arrow_schema::Schema) -> SasResult<ArrowSchema> {
    let fields: Vec<Field> = arrow_schema
        .fields()
        .iter()
        .map(|f| {
            let dtype = arrow_dt_to_polars_arrow(f.data_type()).map_err(Error::arrow)?;
            Ok(Field::new(f.name().as_str().into(), dtype, true))
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
                Box::new(PrimitiveArray::new(
                    ArrowDataType::Int32,
                    values.into(),
                    bitmap,
                ))
            }
            OwnedColumnBuffer::I64 { values, valid } => {
                let bitmap = valid.map(|v| bits_to_bitmap(&v, row_count));
                Box::new(PrimitiveArray::new(
                    ArrowDataType::Int64,
                    values.into(),
                    bitmap,
                ))
            }
            OwnedColumnBuffer::F64 { values, valid } => {
                let bitmap = valid.map(|v| bits_to_bitmap(&v, row_count));
                Box::new(PrimitiveArray::new(
                    ArrowDataType::Float64,
                    values.into(),
                    bitmap,
                ))
            }
            OwnedColumnBuffer::Date { values, valid } => {
                // SasDate wraps i32 day-since-epoch; extract with one flat copy.
                let i32s: Vec<i32> = values.into_iter().map(|d| d.days_since_sas_epoch).collect();
                let bitmap = valid.map(|v| bits_to_bitmap(&v, row_count));
                Box::new(PrimitiveArray::new(
                    ArrowDataType::Date32,
                    i32s.into(),
                    bitmap,
                ))
            }
            OwnedColumnBuffer::DateTime { values, valid } => {
                // SasDateTime wraps i64 seconds; extract with one flat copy.
                let i64s: Vec<i64> = values
                    .into_iter()
                    .map(|d| d.seconds_since_sas_epoch)
                    .collect();
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
