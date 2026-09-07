//! One scan, one decode thread, one Arrow record-batch reader.
//!
//! The core decodes pages across its own thread pool and delivers batches to a
//! visitor; a C stream is pulled by its consumer. The two meet at a bounded
//! channel: the visitor sends, the reader receives, and back-pressure from a slow
//! consumer stalls the decode rather than buffering the file. A consumer that
//! drops the stream early closes the channel, and the next send tells the scan to
//! stop.

use arrow_array::builder::StringBuilder;
use arrow_array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, RecordBatchReader,
    StringArray,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use sas7bdat::{
    BatchHint, ColumnMajorDecode, Dataset, Error, LabelSet, Parallelism, Result as SasResult,
    catalog::normalize_format_name,
};
use std::ops::ControlFlow;
use std::sync::{Arc, mpsc};
use std::thread;

/// What one stream decodes.
#[derive(Clone, Debug, Default)]
pub struct ScanSpec {
    /// Source column names to decode, in the order they should come out. `None`
    /// is every column.
    pub columns: Option<Vec<String>>,
    pub n_rows: Option<usize>,
    pub batch_size: Option<usize>,
}

/// The Arrow schema a stream declares for `spec`: the core's schema for the
/// projected columns, with every column that carries a value-label format
/// re-typed to `Utf8`, since that is what its batches hold.
pub fn stream_schema(ds: &Dataset, spec: &ScanSpec) -> SasResult<SchemaRef> {
    let core = core_schema(ds, spec)?;
    let labels = label_sets(ds, &core);
    if labels.iter().all(Option::is_none) {
        return Ok(core);
    }
    let fields = core
        .fields()
        .iter()
        .zip(&labels)
        .map(|(field, label)| {
            if label.is_some() {
                Arc::new(
                    Field::new(field.name(), DataType::Utf8, true)
                        .with_metadata(field.metadata().clone()),
                )
            } else {
                Arc::clone(field)
            }
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new(fields)))
}

fn core_schema(ds: &Dataset, spec: &ScanSpec) -> SasResult<SchemaRef> {
    let projection = projection(ds, spec)?;
    let mut scan = ds.scan();
    if let Some(ref projection) = projection {
        scan = scan.with_projection(projection);
    }
    scan.arrow_schema()
}

fn projection(ds: &Dataset, spec: &ScanSpec) -> SasResult<Option<sas7bdat::Projection>> {
    match spec.columns.as_deref() {
        None | Some([]) => Ok(None),
        Some(columns) => ds.projection().columns(columns.to_vec()).build().map(Some),
    }
}

/// The value-label set behind each field of `schema`, by the SAS format the
/// column carries, where the dataset has a catalog attached.
fn label_sets(ds: &Dataset, schema: &Schema) -> Vec<Option<LabelSet>> {
    let label_sets = &ds.metadata().label_sets;
    if label_sets.is_empty() {
        return vec![None; schema.fields().len()];
    }
    let mut by_name = std::collections::HashMap::with_capacity(ds.columns().len());
    for column in ds.columns() {
        by_name.entry(column.name.as_str()).or_insert(column);
    }
    schema
        .fields()
        .iter()
        .map(|field| {
            by_name
                .get(field.name().as_str())
                .and_then(|column| column.format.as_deref())
                .map(normalize_format_name)
                .and_then(|format| label_sets.get(&format))
                .cloned()
        })
        .collect()
}

/// Whether to use the column-major page decode. Markedly faster for wide
/// all-numeric tables and falls back to row-major on its own when a scan cannot
/// use it, so it is safe to leave on. `SAS7BDAT_COLUMN_MAJOR=0` forces row-major.
fn column_major_decode() -> ColumnMajorDecode {
    match std::env::var("SAS7BDAT_COLUMN_MAJOR").ok().as_deref() {
        Some("0" | "off" | "false" | "OFF" | "FALSE") => ColumnMajorDecode::Off,
        _ => ColumnMajorDecode::On,
    }
}

/// The decode parallelism for one file. `Parallelism::Auto` carries the core's
/// calibrated gate; `SAS7BDAT_SCAN_THREADS` overrides it, which is how a caller
/// reading many files at once bounds the total core budget.
fn scan_parallelism() -> Parallelism {
    match std::env::var("SAS7BDAT_SCAN_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|threads| *threads > 0)
    {
        Some(1) => Parallelism::None,
        Some(threads) => Parallelism::Threads(threads),
        None => Parallelism::Auto,
    }
}

/// A record-batch reader fed by a decode thread. Implements what an Arrow C
/// stream needs: a schema, and batches until `None`.
pub struct ScanReader {
    schema: SchemaRef,
    rx: mpsc::Receiver<Result<RecordBatch, String>>,
}

impl ScanReader {
    /// A reader with a schema and no batches: what a schema-only stream is.
    pub fn empty(schema: SchemaRef) -> Self {
        let (_tx, rx) = mpsc::sync_channel(1);
        Self { schema, rx }
    }

    /// A reader over exactly one batch.
    pub fn single(batch: RecordBatch) -> Self {
        let schema = batch.schema();
        let (tx, rx) = mpsc::sync_channel(1);
        let _ = tx.send(Ok(batch));
        Self { schema, rx }
    }

    /// Start the decode of `spec` on its own thread and return the reading end.
    pub fn start(ds: Arc<Dataset>, spec: ScanSpec) -> SasResult<Self> {
        let schema = stream_schema(&ds, &spec)?;
        let labels = label_sets(&ds, &schema);
        // Four batches of slack: enough that the decoder is not stalled by a
        // consumer doing a little work per batch, small enough that a stalled
        // consumer holds a handful of batches and not the file.
        let (tx, rx) = mpsc::sync_channel::<Result<RecordBatch, String>>(4);
        let stream_schema = Arc::clone(&schema);
        thread::spawn(move || {
            if let Err(err) = run_scan(&ds, &spec, &stream_schema, &labels, &tx) {
                let _ = tx.send(Err(err.to_string()));
            }
        });
        Ok(Self { schema, rx })
    }
}

impl Iterator for ScanReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        // A closed channel is the end of the stream, whether the decode finished
        // or its thread is gone.
        let message = self.rx.recv().ok()?;
        Some(message.map_err(|text| ArrowError::ExternalError(text.into())))
    }
}

impl RecordBatchReader for ScanReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

fn run_scan(
    ds: &Dataset,
    spec: &ScanSpec,
    stream_schema: &SchemaRef,
    labels: &[Option<LabelSet>],
    tx: &mpsc::SyncSender<Result<RecordBatch, String>>,
) -> SasResult<()> {
    let projection = projection(ds, spec)?;
    let mut scan = ds
        .scan()
        .with_parallelism(scan_parallelism())
        .with_column_major_decode(column_major_decode());
    if let Some(ref projection) = projection {
        scan = scan.with_projection(projection);
    }
    if let Some(n_rows) = spec.n_rows {
        scan = scan
            .limit(u64::try_from(n_rows).map_err(|_| Error::unsupported("row limit exceeds u64"))?);
    }
    if let Some(batch_size) = spec.batch_size {
        scan = scan.with_batch_hint(BatchHint::Rows(batch_size));
    }
    let core_schema = scan.arrow_schema()?;
    scan.visit_owned_batches(|batch| {
        let batch = batch.into_arrow_record_batch(Arc::clone(&core_schema))?;
        let batch = apply_labels(batch, stream_schema, labels)?;
        // The receiver is gone: the consumer released the stream. Stop decoding.
        if tx.send(Ok(batch)).is_err() {
            return Ok(ControlFlow::Break(()));
        }
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(())
}

/// Replace every labelled column's raw values with their labels, as `Utf8`.
fn apply_labels(
    batch: RecordBatch,
    stream_schema: &SchemaRef,
    labels: &[Option<LabelSet>],
) -> SasResult<RecordBatch> {
    if labels.iter().all(Option::is_none) {
        return Ok(batch);
    }
    let columns = batch
        .columns()
        .iter()
        .zip(labels)
        .map(|(column, label)| match label {
            Some(label) => labelled(column, label),
            None => Ok(Arc::clone(column)),
        })
        .collect::<SasResult<Vec<_>>>()?;
    RecordBatch::try_new(Arc::clone(stream_schema), columns)
        .map_err(|err| Error::arrow(err.to_string()))
}

/// A `Utf8` array of the labels for `column`'s values. A value the label set
/// does not know is written as itself, which is what SAS prints for it.
fn labelled(column: &ArrayRef, labels: &LabelSet) -> SasResult<ArrayRef> {
    let rows = column.len();
    let mut out = StringBuilder::with_capacity(rows, rows.saturating_mul(16));
    if let Some(values) = column.as_any().downcast_ref::<Float64Array>() {
        for row in 0..rows {
            if values.is_null(row) {
                out.append_null();
            } else {
                let value = values.value(row);
                match labels.lookup_numeric(value) {
                    Some(label) => out.append_value(label),
                    None => out.append_value(value.to_string()),
                }
            }
        }
    } else if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
        for row in 0..rows {
            if values.is_null(row) {
                out.append_null();
            } else {
                let value = values.value(row);
                // SAS categorical codes are small integers; exact as f64 for |v| <= 2^53.
                #[allow(clippy::cast_precision_loss)]
                match labels.lookup_numeric(value as f64) {
                    Some(label) => out.append_value(label),
                    None => out.append_value(value.to_string()),
                }
            }
        }
    } else if let Some(values) = column.as_any().downcast_ref::<Int32Array>() {
        for row in 0..rows {
            if values.is_null(row) {
                out.append_null();
            } else {
                let value = values.value(row);
                match labels.lookup_numeric(f64::from(value)) {
                    Some(label) => out.append_value(label),
                    None => out.append_value(value.to_string()),
                }
            }
        }
    } else if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        for row in 0..rows {
            if values.is_null(row) {
                out.append_null();
            } else {
                let value = values.value(row);
                out.append_value(labels.lookup_string(value).unwrap_or(value));
            }
        }
    } else {
        return Err(Error::unsupported(format!(
            "a value-label format on a {} column",
            column.data_type()
        )));
    }
    Ok(Arc::new(out.finish()))
}
