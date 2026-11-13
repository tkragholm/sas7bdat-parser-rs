//! One scan, one decode thread, one Arrow record-batch reader.
//!
//! The core decodes pages across its own thread pool and delivers batches to a
//! visitor; a C stream is pulled by its consumer. The two meet at a bounded
//! channel: the visitor sends, the reader receives, and back-pressure from a slow
//! consumer stalls the decode rather than buffering the file. A consumer that
//! drops the stream early closes the channel, and the next send tells the scan to
//! stop.

use arrow_array::builder::{StringViewBuilder, make_view};
use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Float64Array, Int32Array, Int64Array, RecordBatch,
    RecordBatchReader, StringArray, StringViewArray,
};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use rayon::prelude::*;
use sas7bdat::OwnedColumnBuffer;
use sas7bdat::{
    BatchHint, ColumnMajorDecode, Dataset, Error, LabelSet, Parallelism, Result as SasResult,
    catalog::normalize_format_name,
};
use std::collections::HashMap;
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
    /// Rows to skip first, counting the rows a read returns.
    pub row_offset: usize,
    /// Fail on the first cell that does not decode in the file's encoding.
    pub strict: bool,
    /// Leave labelled columns as their codes even when a catalog is attached.
    pub keep_codes: bool,
    pub batch_size: Option<usize>,
    /// String columns to hand over dictionary-encoded, which polars reads as
    /// `Categorical`: a code per row and the distinct values once per batch.
    pub categorical: Vec<String>,
}

/// The Arrow schema a stream declares for `spec`: the core's schema for the
/// projected columns, with strings as views (see [`string_views`]) and every
/// column that carries a value-label format re-typed to a string, since that
/// is what its batches hold.
pub fn stream_schema(ds: &Dataset, spec: &ScanSpec) -> SasResult<SchemaRef> {
    let core = core_schema(ds, spec)?;
    let labels = label_sets(ds, &core, spec.keep_codes);
    stream_schema_from(&core, &labels, &spec.categorical)
}

/// Arrow's spelling of what polars reads as `Categorical`.
fn dictionary_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}

fn stream_schema_from(
    core: &Schema,
    labels: &[Option<LabelSet>],
    categorical: &[String],
) -> SasResult<SchemaRef> {
    for name in categorical {
        let Some((index, _)) = core.column_with_name(name) else {
            return Err(Error::unsupported(format!(
                "categorical names a column the scan does not have: {name}"
            )));
        };
        if labels[index].is_none() && core.field(index).data_type() != &DataType::Utf8 {
            return Err(Error::unsupported(format!(
                "categorical on {name}, a {} column; only string and labelled columns",
                core.field(index).data_type()
            )));
        }
    }
    let fields = core
        .fields()
        .iter()
        .zip(labels)
        .map(|(field, label)| {
            let data_type = match (label, field.data_type()) {
                _ if categorical.contains(field.name()) => dictionary_type(),
                (Some(_), _) | (None, DataType::Utf8) => DataType::Utf8View,
                (None, other) => other.clone(),
            };
            Arc::new(
                Field::new(field.name(), data_type, true).with_metadata(field.metadata().clone()),
            )
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
/// column carries, where the dataset has a catalog attached. None at all when
/// `keep_codes` asks for the codes.
fn label_sets(ds: &Dataset, schema: &Schema, keep_codes: bool) -> Vec<Option<LabelSet>> {
    let label_sets = &ds.metadata().label_sets;
    if label_sets.is_empty() || keep_codes {
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
/// Three costs, measured on a twelve-core machine. Every batch crossing into
/// Python pays a fixed import price, about 70 microseconds, so a narrow
/// 1.7-million-row file at the core's own
/// 4,096-row default made 430 batches and spent a third of a full read on
/// them. The core decodes batches in parallel, one per worker, so a wide
/// 84,000-row survey file at 65,536 rows a batch used three cores and read
/// three times slower. And each worker holds the batch it is decoding, so
/// memory in flight is workers times batch bytes: at 8 MB a batch the
/// twelve workers held 114 MB over a single one, and a large server may have
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
        let labels = label_sets(&ds, &core, spec.keep_codes);
        let schema = stream_schema_from(&core, &labels, &spec.categorical)?;
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

/// One column of a batch to convert: its buffer and field, and its value labels
/// and interner if it has them.
type ColumnWork<'a> = (
    (OwnedColumnBuffer, &'a arrow_schema::FieldRef),
    (&'a Option<LabelSet>, &'a mut Option<Interner>),
);

/// `scan` restricted to the rows `spec` asks for: a row limit alone, or a range from
/// `row_offset`. Both count the rows a read returns.
fn with_rows<'a>(
    scan: sas7bdat::ScanBuilder<'a>,
    spec: &ScanSpec,
) -> SasResult<sas7bdat::ScanBuilder<'a>> {
    let to_u64 =
        |n: usize| u64::try_from(n).map_err(|_| Error::unsupported("row count exceeds u64"));
    Ok(match (spec.row_offset, spec.n_rows) {
        (0, None) => scan,
        (0, Some(n_rows)) => scan.limit(to_u64(n_rows)?),
        (offset, n_rows) => {
            let start = to_u64(offset)?;
            let end =
                n_rows.map_or(Ok(u64::MAX), |n| to_u64(n).map(|n| start.saturating_add(n)))?;
            scan.select(sas7bdat::RowSelection::range(start, end))
        }
    })
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
    scan = with_rows(scan, spec)?;
    if spec.strict {
        scan = scan.with_string_options(
            sas7bdat::StringDecodeOptions::builder()
                .utf8_validation(sas7bdat::Utf8ValidationMode::Strict)
                .build(),
        );
    }
    scan = scan.with_batch_hint(BatchHint::Rows(
        spec.batch_size
            .unwrap_or_else(|| default_batch_rows(ds, spec)),
    ));
    // One interner per categorical column for the whole scan, so a value keeps
    // its code from batch to batch and each batch's dictionary extends the last.
    // Held by column position, so each column's conversion owns its own.
    let mut interners: Vec<Option<Interner>> = core
        .fields()
        .iter()
        .map(|field| {
            spec.categorical
                .iter()
                .any(|name| name == field.name())
                .then(Interner::default)
        })
        .collect();
    // Batches arrive on one thread, and converting a wide batch's columns there
    // took longer than decoding them on all the others: 0.5 s of the 0.95 s read
    // of the 4,041-column AHS file. The columns are independent, so they convert
    // in parallel, on as many threads as the scan decodes with.
    let workers = scan_workers();
    let pool = (workers > 1)
        .then(|| rayon::ThreadPoolBuilder::new().num_threads(workers).build())
        .transpose()
        .map_err(|err| Error::internal(format!("conversion thread pool: {err}")))?;
    scan.visit_owned_batches(|batch| {
        // The core moves a numeric or temporal buffer into its array; a string
        // buffer becomes views here, in one pass over its bytes; a labelled
        // column is rebuilt from its values to their labels; a categorical
        // column becomes codes against its interner's dictionary.
        let rows = batch.row_count;
        let convert =
            |((column, field), (label, interner)): ColumnWork<'_>| -> SasResult<ArrayRef> {
                let interner = interner.as_mut();
                match (label, column, interner) {
                    (
                        None,
                        OwnedColumnBuffer::Utf8 {
                            offsets,
                            data,
                            valid,
                            ..
                        },
                        Some(interner),
                    ) => interner.encode_buffer(&offsets.into_inner(), &data, valid, rows),
                    (
                        None,
                        OwnedColumnBuffer::Utf8 {
                            offsets,
                            data,
                            valid,
                            ..
                        },
                        None,
                    ) => string_views(&offsets.into_inner(), data, valid, rows),
                    (None, column, _) => column.into_arrow_array_as(field.data_type()),
                    (Some(label), column, interner) => {
                        let labels =
                            labelled(&column.into_arrow_array_as(field.data_type())?, label)?;
                        match interner {
                            Some(interner) => interner.encode_views(
                                labels
                                    .as_any()
                                    .downcast_ref::<StringViewArray>()
                                    .ok_or_else(|| Error::arrow("labels are not a string array"))?,
                            ),
                            None => Ok(labels),
                        }
                    }
                }
            };
        let work = batch
            .columns
            .into_iter()
            .zip(core.fields().iter())
            .zip(labels.iter().zip(interners.iter_mut()));
        let columns = match &pool {
            Some(pool) => pool.install(|| {
                work.collect::<Vec<_>>()
                    .into_par_iter()
                    .map(convert)
                    .collect::<SasResult<Vec<_>>>()
            }),
            None => work.map(convert).collect::<SasResult<Vec<_>>>(),
        }?;
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
/// allocation alive behind the result, offsets and validity included: 74 MB
/// more resident on a 1.7-million-row file, and a full read a quarter slower.
///
/// Why from the scanner's buffer rather than from the core's `Utf8` array: that
/// array's construction narrows and checks the offsets and validates the bytes,
/// and views need one pass, not three.
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
    data.truncate(end);
    let text = std::str::from_utf8(&data).map_err(|err| Error::arrow(err.to_string()))?;
    let mut views: Vec<u128> = Vec::with_capacity(rows);
    let mut previous = 0usize;
    let mut longest = 0usize;
    for &offset in &offsets[1..] {
        let offset = usize::try_from(offset).map_err(|_| Error::arrow("negative offset"))?;
        if offset < previous || !text.is_char_boundary(offset) {
            return Err(Error::arrow(
                "string offsets are not monotone character boundaries",
            ));
        }
        longest = longest.max(offset - previous);
        #[allow(clippy::cast_possible_truncation)]
        views.push(make_view(&data[previous..offset], 0, previous as u32));
        previous = offset;
    }
    // A view holds a string of up to twelve bytes itself. When every string in
    // the batch is that short, which is most SAS category columns, no view points
    // into the buffer and it is dropped. Otherwise it is cut to its contents,
    // since the scanner sized it for the column's full width, unless that would
    // free little: shrinking copies the buffer.
    let buffers: Arc<[Buffer]> = if longest <= 12 {
        Arc::from([])
    } else {
        if data.capacity() > end + end / 4 {
            data.shrink_to_fit();
        }
        Arc::from([Buffer::from_vec(data)])
    };
    let nulls =
        valid.map(|words| NullBuffer::new(BooleanBuffer::new(Buffer::from_vec(words), 0, rows)));
    // SAFETY: every view was built by `make_view` from a slice of `data` at the
    // offset it records, within `end <= data.len()`; `data` is valid UTF-8 as a
    // whole and every string starts and ends on a character boundary, so every
    // string is valid UTF-8. A view of more than twelve bytes refers to buffer
    // index 0, which is `data`, passed whenever such a view exists; shorter views
    // hold their bytes and refer to no buffer.
    #[allow(unsafe_code)]
    let array =
        unsafe { StringViewArray::new_unchecked(ScalarBuffer::from(views), buffers, nulls) };
    Ok(Arc::new(array))
}

/// The dictionary behind one categorical column, kept for the whole scan.
///
/// A value gets a code the first time it is seen and keeps it, so every
/// batch's dictionary is the previous one extended and polars has nothing to
/// reconcile between batches. SAS category columns are often one byte wide, and
/// for those a 257-entry table stands in for the hash map: no hashing at all on
/// the common case, which is where arrow's generic dictionary builder spent
/// ten nanoseconds a row.
#[derive(Default)]
struct Interner {
    /// Code per one-byte value, index 256 for the empty string; `-1` unseen.
    byte_codes: Option<Box<[i32; 257]>>,
    longer: HashMap<Box<[u8]>, i32, ahash::RandomState>,
    /// Every distinct value once, in code order.
    values: Vec<String>,
}

impl Interner {
    fn code(&mut self, bytes: &[u8]) -> SasResult<i32> {
        if bytes.len() <= 1 {
            let slot = bytes.first().map_or(256, |byte| usize::from(*byte));
            if let Some(table) = &self.byte_codes
                && table[slot] >= 0
            {
                return Ok(table[slot]);
            }
            let code = self.push(bytes)?;
            self.byte_codes.get_or_insert_with(|| Box::new([-1; 257]))[slot] = code;
            return Ok(code);
        }
        if let Some(&code) = self.longer.get(bytes) {
            return Ok(code);
        }
        let code = self.push(bytes)?;
        self.longer.insert(bytes.into(), code);
        Ok(code)
    }

    fn push(&mut self, bytes: &[u8]) -> SasResult<i32> {
        let code = i32::try_from(self.values.len()).map_err(|_| {
            Error::unsupported("a categorical column with over 2^31 distinct values")
        })?;
        let text = std::str::from_utf8(bytes).map_err(|err| Error::arrow(err.to_string()))?;
        self.values.push(text.to_owned());
        Ok(code)
    }

    fn finish(&self, keys: Vec<i32>, nulls: Option<NullBuffer>) -> SasResult<ArrayRef> {
        let keys = Int32Array::new(ScalarBuffer::from(keys), nulls);
        let values = StringArray::from_iter_values(self.values.iter().map(String::as_str));
        DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values))
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(|err| Error::arrow(err.to_string()))
    }

    /// Codes for a scanner string buffer.
    fn encode_buffer(
        &mut self,
        offsets: &[i64],
        data: &[u8],
        valid: Option<Vec<u64>>,
        rows: usize,
    ) -> SasResult<ArrayRef> {
        if offsets.len() != rows + 1 {
            return Err(Error::arrow("string offsets do not match the row count"));
        }
        let nulls = valid
            .map(|words| NullBuffer::new(BooleanBuffer::new(Buffer::from_vec(words), 0, rows)));
        let mut keys = Vec::with_capacity(rows);
        let mut previous = 0usize;
        for (row, &offset) in offsets[1..].iter().enumerate() {
            let offset = usize::try_from(offset).map_err(|_| Error::arrow("negative offset"))?;
            let bytes = data
                .get(previous..offset)
                .ok_or_else(|| Error::arrow("string offsets run past their data"))?;
            let valid = nulls.as_ref().is_none_or(|nulls| nulls.is_valid(row));
            keys.push(if valid { self.code(bytes)? } else { 0 });
            previous = offset;
        }
        self.finish(keys, nulls)
    }

    /// Codes for an already-built string array, which is what a labelled
    /// column is by the time it is categorical.
    fn encode_views(&mut self, strings: &StringViewArray) -> SasResult<ArrayRef> {
        let mut keys = Vec::with_capacity(strings.len());
        for value in strings {
            keys.push(match value {
                Some(text) => self.code(text.as_bytes())?,
                None => 0,
            });
        }
        self.finish(keys, strings.nulls().cloned())
    }
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
