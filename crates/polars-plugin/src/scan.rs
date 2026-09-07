//! One scan, one decode thread, one Arrow record-batch reader.
//!
//! The core decodes pages across its own thread pool and delivers batches to a
//! visitor; a C stream is pulled by its consumer. The two meet at a bounded
//! channel: the visitor sends, the reader receives, and back-pressure from a slow
//! consumer stalls the decode rather than buffering the file. A consumer that
//! drops the stream early closes the channel, and the next send tells the scan to
//! stop.

use arrow_array::builder::{StringViewBuilder, make_view};
use arrow_array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, RecordBatchReader,
    StringArray, StringViewArray,
};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use sas7bdat::OwnedColumnBuffer;
use sas7bdat::{
    BatchHint, ColumnMajorDecode, Dataset, Error, LabelSet, Parallelism, Result as SasResult,
    catalog::normalize_format_name,
};
use std::ops::ControlFlow;
use std::panic::{AssertUnwindSafe, catch_unwind};
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
/// projected columns, with strings as views (see [`string_views`]) and every
/// column that carries a value-label format re-typed to a string, since that
/// is what its batches hold.
pub fn stream_schema(ds: &Dataset, spec: &ScanSpec) -> SasResult<SchemaRef> {
    let core = core_schema(ds, spec)?;
    Ok(stream_schema_from(&core, &label_sets(ds, &core)))
}

fn stream_schema_from(core: &Schema, labels: &[Option<LabelSet>]) -> SchemaRef {
    let fields = core
        .fields()
        .iter()
        .zip(labels)
        .map(|(field, label)| {
            let data_type = match (label, field.data_type()) {
                (Some(_), _) | (None, DataType::Utf8) => DataType::Utf8View,
                (None, other) => other.clone(),
            };
            Arc::new(
                Field::new(field.name(), data_type, true).with_metadata(field.metadata().clone()),
            )
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
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

/// The decode workers a scan will use: the override, else every core.
fn scan_workers() -> usize {
    match scan_parallelism() {
        Parallelism::None => 1,
        Parallelism::Threads(threads) => threads,
        Parallelism::Auto => thread::available_parallelism().map_or(1, usize::from),
    }
}

/// Rows per batch when the caller names none.
///
/// Three costs, measured on 7 September 2026 on a twelve-core machine. Every
/// batch crossing into Python pays a fixed import price, about 70
/// microseconds, so a narrow 1.7-million-row register file at the core's own
/// 4,096-row default made 430 batches and spent a third of a full read on
/// them. The core decodes batches in parallel, one per worker, so a wide
/// 84,000-row survey file at 65,536 rows a batch used three cores and read
/// three times slower. And each worker holds the batch it is decoding, so
/// memory in flight is workers times batch bytes: at 8 MB a batch the
/// twelve workers held 114 MB over a single one, and the analysis server has
/// ninety-six.
///
/// So: a budget of 64 MB of decoded rows in flight, shared out per worker and
/// never below 1 MB a batch; no fewer than two batches per core; and between
/// 4,096 and 32,768 rows. On twelve cores that is the size every file
/// measured was within a few percent of its best at; on ninety-six it is
/// smaller batches and a bounded footprint.
fn default_batch_rows(ds: &Dataset, spec: &ScanSpec) -> usize {
    const IN_FLIGHT_BYTES: usize = 64 << 20;
    const MIN_BATCH_BYTES: usize = 1 << 20;
    let meta = ds.metadata();
    let columns = spec
        .columns
        .as_ref()
        .map_or(ds.columns().len(), Vec::len)
        .max(1);
    // Decoded width: the on-disk row where strings dominate, eight bytes a
    // column where numerics do.
    let width = usize::try_from(meta.row_len)
        .unwrap_or(usize::MAX)
        .max(columns * 8)
        .max(1);
    let workers = scan_workers().max(1);
    let by_bytes = (IN_FLIGHT_BYTES / workers).max(MIN_BATCH_BYTES) / width;
    let rows = usize::try_from(meta.row_count).unwrap_or(usize::MAX);
    let by_parallelism = rows / (2 * workers);
    by_bytes.min(by_parallelism).clamp(1 << 12, 1 << 15)
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
        let core = core_schema(&ds, &spec)?;
        let labels = label_sets(&ds, &core);
        let schema = stream_schema_from(&core, &labels);
        // Two batches of slack: enough that the decoder is not stalled by a
        // consumer doing a little work per batch, and the smallest number that
        // measured no slower than four.
        let (tx, rx) = mpsc::sync_channel::<Result<RecordBatch, String>>(2);
        let stream_schema = Arc::clone(&schema);
        thread::spawn(move || {
            // A panic on this thread would close the channel and end the stream
            // early, which a consumer cannot tell from a short file. Turn it into
            // an error the consumer sees.
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                run_scan(&ds, &spec, &core, &stream_schema, &labels, &tx)
            }));
            let message = match outcome {
                Ok(Ok(())) => return,
                Ok(Err(err)) => err.to_string(),
                Err(panic) => format!(
                    "the decode thread panicked: {}",
                    panic
                        .downcast_ref::<String>()
                        .map(String::as_str)
                        .or_else(|| panic.downcast_ref::<&str>().copied())
                        .unwrap_or("no message")
                ),
            };
            let _ = tx.send(Err(message));
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
    core: &SchemaRef,
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
    scan = scan.with_batch_hint(BatchHint::Rows(
        spec.batch_size
            .unwrap_or_else(|| default_batch_rows(ds, spec)),
    ));
    scan.visit_owned_batches(|batch| {
        // The core moves a numeric or temporal buffer into its array; a string
        // buffer becomes views here, in one pass over its bytes; a labelled
        // column is rebuilt from its values to their labels.
        let rows = batch.row_count;
        let columns = batch
            .columns
            .into_iter()
            .zip(core.fields())
            .zip(labels)
            .map(|((column, field), label)| match (label, column) {
                (
                    None,
                    OwnedColumnBuffer::Utf8 {
                        offsets,
                        data,
                        valid,
                        ..
                    },
                ) => string_views(&offsets.into_inner(), data, valid, rows),
                (None, column) => column.into_arrow_array_as(field.data_type()),
                (Some(label), column) => {
                    labelled(&column.into_arrow_array_as(field.data_type())?, label)
                }
            })
            .collect::<SasResult<Vec<_>>>()?;
        let batch = RecordBatch::try_new(Arc::clone(stream_schema), columns)
            .map_err(|err| Error::arrow(err.to_string()))?;
        // The receiver is gone: the consumer released the stream. Stop decoding.
        if tx.send(Ok(batch)).is_err() {
            return Ok(ControlFlow::Break(()));
        }
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(())
}

/// The scanner's strings as a `StringViewArray`, polars' own string layout:
/// one 16-byte view per row, a string of twelve bytes or fewer inlined in it
/// and a longer one pointing into the moved buffer.
///
/// Why views: polars converts an imported `Utf8` array into views anyway, and
/// for a column with any string over twelve bytes it keeps the whole imported
/// allocation alive behind the result, offsets and validity included: measured
/// 7 September 2026, 74 MB more resident on a 1.7-million-row register file
/// and a full read a quarter slower. Why from the scanner's buffer rather than
/// from the core's `Utf8` array: that array's construction narrows and checks
/// the offsets and validates the bytes, and views need one pass, not three.
///
/// The buffer is validated as UTF-8 as a whole and every offset checked to be
/// a character boundary, which together are what a per-string check would
/// establish; the array is then built unchecked, since arrow's checked
/// constructor would repeat the per-string check.
fn string_views(
    offsets: &[i64],
    mut data: Vec<u8>,
    valid: Option<Vec<u64>>,
    rows: usize,
) -> SasResult<ArrayRef> {
    if offsets.len() != rows + 1 {
        return Err(Error::arrow("string offsets do not match the row count"));
    }
    let end = usize::try_from(offsets[rows]).map_err(|_| Error::arrow("negative offset"))?;
    if end > data.len() {
        return Err(Error::arrow("string offsets run past their data"));
    }
    u32::try_from(end).map_err(|_| {
        Error::unsupported("a string batch over 4 GB; ask for a smaller batch_size")
    })?;
    // Cut to contents: the scanner sized the buffer for the column's full width.
    data.truncate(end);
    data.shrink_to_fit();
    let text = std::str::from_utf8(&data).map_err(|err| Error::arrow(err.to_string()))?;
    let mut views: Vec<u128> = Vec::with_capacity(rows);
    let mut previous = 0usize;
    for &offset in &offsets[1..] {
        let offset = usize::try_from(offset).map_err(|_| Error::arrow("negative offset"))?;
        if offset < previous || !text.is_char_boundary(offset) {
            return Err(Error::arrow(
                "string offsets are not monotone character boundaries",
            ));
        }
        #[allow(clippy::cast_possible_truncation)]
        views.push(make_view(&data[previous..offset], 0, previous as u32));
        previous = offset;
    }
    let nulls =
        valid.map(|words| NullBuffer::new(BooleanBuffer::new(Buffer::from_vec(words), 0, rows)));
    // SAFETY: every view was built by `make_view` from a slice of `data` at the
    // offset it records, within `end <= data.len()`; `data` is valid UTF-8 as a
    // whole and every string starts and ends on a character boundary, so every
    // string is valid UTF-8; buffer index 0 is the one buffer passed.
    #[allow(unsafe_code)]
    let array = unsafe {
        StringViewArray::new_unchecked(
            ScalarBuffer::from(views),
            vec![Buffer::from_vec(data)],
            nulls,
        )
    };
    Ok(Arc::new(array))
}

/// A string array of the labels for `column`'s values. A value the label set
/// does not know is written as itself, which is what SAS prints for it.
fn labelled(column: &ArrayRef, labels: &LabelSet) -> SasResult<ArrayRef> {
    let rows = column.len();
    let mut out = StringViewBuilder::with_capacity(rows);
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
