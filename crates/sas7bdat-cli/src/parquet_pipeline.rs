//! Encodes Parquet row groups across cores instead of on the thread that reads them.
//!
//! `ArrowWriter` encodes wherever it is called, so a conversion builds dictionaries, encodes
//! pages, gathers statistics and compresses on a single core. Measured on this repo's fixtures
//! with the input cached — so decode and I/O are as cheap as they get — that one thread is
//! 80-90% of a conversion's wall clock.
//!
//! There are two ways to split the work, and which one pays depends on the table:
//!
//! - **By column.** The axis parquet-rs documents. It tops out at the column count, which for
//!   the SAS files this tool converts is often 10 and sometimes 3.
//! - **By row group.** Several row groups encode at once and are appended in file order. This
//!   scales with rows rather than columns, so it is the one that fills a large host on a narrow
//!   table.
//!
//! Both are used. Row groups are dispatched as they fill and their columns are encoded in
//! parallel, so the pool sees `row groups in flight × columns` independent tasks. The split
//! between the two settles on its own: [`IN_FLIGHT_INPUT_BUDGET`] caps the decoded rows held by
//! row groups that are still encoding, so a narrow table (a few MB per row group) keeps many in
//! flight, while a table with thousands of columns keeps one or two — which is all it needs,
//! since one such row group already has thousands of column tasks in it.
//!
//! Row groups reach the file in the order they were filled. Encoding finishes out of order, so
//! completed ones wait in a small reorder buffer.

use anyhow::{Result, anyhow};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema, SchemaRef};
use parquet::arrow::arrow_writer::{
    ArrowColumnChunk, ArrowColumnWriter, ArrowRowGroupWriterFactory, compute_leaves,
};
use parquet::errors::ParquetError;
use parquet::file::writer::SerializedFileWriter;
use rayon::prelude::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use std::collections::HashMap;
use std::io::Write;
use std::sync::OnceLock;
use std::sync::mpsc::{Receiver, Sender, channel};

/// Decoded rows a single row group may buffer before it is closed early, whatever the row
/// target says. Without it a table with thousands of columns would hold gigabytes of Arrow
/// data waiting for a row count it reaches slowly.
const ROW_GROUP_INPUT_BYTE_CAP: usize = 256 * 1024 * 1024;

/// Decoded rows held by row groups that are encoding but not yet written. This is what decides
/// how many run at once, and so how the work splits between row groups and columns.
const IN_FLIGHT_INPUT_BUDGET: usize = 1024 * 1024 * 1024;

/// Parquet records a row group's ordinal in an `i16`, so a file cannot hold more than this
/// many however large it is.
const MAX_ROW_GROUPS: usize = 32_767;

/// Row groups to write before the target size doubles, and how that budget shrinks.
///
/// A fixed row target puts a hard ceiling on a file — at 65,536 rows it is about 2.1 billion,
/// which real SAS files exceed. Doubling the target each time a stage is spent, while halving
/// the stage, keeps the total under [`MAX_ROW_GROUPS`] no matter how many rows arrive: each
/// stage covers as many rows as the one before it, so fifteen or so stages span more rows than
/// any file will hold. It needs no row count, which matters because the header's is not always
/// to be believed.
const FIRST_STAGE_ROW_GROUPS: usize = MAX_ROW_GROUPS / 2;

/// Ceiling on the grown byte cap. Both triggers have to grow together — a row group closed by
/// bytes ignores the row target entirely, so raising only the target would leave the row group
/// count free to run past the limit anyway. Matching [`IN_FLIGHT_INPUT_BUDGET`] means the worst
/// case is one row group encoding at a time.
const MAX_ROW_GROUP_INPUT_BYTES: usize = IN_FLIGHT_INPUT_BUDGET;

/// Grows both row group triggers so a file cannot run out of row groups.
struct RowGroupSizer {
    target: usize,
    bytes: usize,
    stage: usize,
    left: usize,
}

impl RowGroupSizer {
    fn new(target: usize) -> Self {
        Self {
            target: target.max(1),
            bytes: ROW_GROUP_INPUT_BYTE_CAP,
            stage: FIRST_STAGE_ROW_GROUPS,
            left: FIRST_STAGE_ROW_GROUPS,
        }
    }

    const fn target(&self) -> usize {
        self.target
    }

    const fn byte_cap(&self) -> usize {
        self.bytes
    }

    /// Account for one dispatched row group, doubling both triggers when a stage runs out.
    fn dispatched(&mut self) {
        self.left = self.left.saturating_sub(1);
        if self.left == 0 {
            self.target = self.target.saturating_mul(2);
            self.bytes = self.bytes.saturating_mul(2).min(MAX_ROW_GROUP_INPUT_BYTES);
            self.stage = (self.stage / 2).max(1);
            self.left = self.stage;
        }
    }
}

/// The shared encoding pool.
///
/// One pool for the process, not one per file: `--jobs` converts several files at once, and a
/// pool each would put `jobs × threads` threads on the machine. Sized by the first caller,
/// since every file in a run shares the one thread budget.
fn encode_pool(threads: Option<usize>) -> Option<&'static rayon::ThreadPool> {
    static POOL: OnceLock<Option<rayon::ThreadPool>> = OnceLock::new();
    POOL.get_or_init(|| {
        let threads = threads.unwrap_or_else(|| {
            std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
        });
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads.max(1))
            .thread_name(|index| format!("parquet-encode-{index}"))
            .build()
            .ok()
    })
    .as_ref()
}

/// Whether every column maps to exactly one Parquet leaf.
///
/// The parallel path pairs writer `i` with field `i`. That holds for flat schemas, which is all
/// this converter produces — a nested type would need writers distributed across fields by leaf
/// count, so it takes the serial path instead of being decoded wrongly.
#[must_use]
pub fn schema_is_flat(schema: &Schema) -> bool {
    schema.fields().iter().all(|field| {
        !matches!(
            field.data_type(),
            DataType::List(_)
                | DataType::LargeList(_)
                | DataType::ListView(_)
                | DataType::LargeListView(_)
                | DataType::FixedSizeList(_, _)
                | DataType::Struct(_)
                | DataType::Map(_, _)
                | DataType::Union(_, _)
                | DataType::RunEndEncoded(_, _)
        )
    })
}

/// Whether the parallel writer can be used at all: a flat schema and a working pool.
#[must_use]
pub fn is_available(schema: &Schema, threads: Option<usize>) -> bool {
    schema_is_flat(schema)
        && encode_pool(threads).is_some_and(|pool| pool.current_num_threads() > 1)
}

/// A row group that finished encoding, tagged with the position it must occupy in the file.
struct Encoded {
    index: usize,
    input_bytes: usize,
    chunks: Result<Vec<ArrowColumnChunk>, ParquetError>,
}

/// Accepts record batches, encodes row groups across the pool, and writes them in file order.
pub struct RowGroupPipeline<W: Write + Send> {
    writer: SerializedFileWriter<W>,
    factory: ArrowRowGroupWriterFactory,
    pool: &'static rayon::ThreadPool,
    schema: SchemaRef,
    sizer: RowGroupSizer,

    /// Batches for the row group currently being filled.
    pending: Vec<RecordBatch>,
    pending_rows: usize,
    pending_bytes: usize,

    /// Next row group to hand to the pool, and next to write.
    next_dispatch: usize,
    next_write: usize,
    in_flight: usize,
    in_flight_bytes: usize,
    /// Row groups that finished ahead of their turn.
    reordered: HashMap<usize, Vec<ArrowColumnChunk>>,

    tx: Sender<Encoded>,
    rx: Receiver<Encoded>,
}

impl<W: Write + Send> RowGroupPipeline<W> {
    /// # Errors
    ///
    /// Returns an error if the encoding pool cannot be created.
    pub fn new(
        writer: SerializedFileWriter<W>,
        factory: ArrowRowGroupWriterFactory,
        schema: SchemaRef,
        target_rows: usize,
        threads: Option<usize>,
    ) -> Result<Self> {
        let pool =
            encode_pool(threads).ok_or_else(|| anyhow!("could not start the encoding pool"))?;
        let (tx, rx) = channel();
        Ok(Self {
            writer,
            factory,
            pool,
            schema,
            sizer: RowGroupSizer::new(target_rows),
            pending: Vec::new(),
            pending_rows: 0,
            pending_bytes: 0,
            next_dispatch: 0,
            next_write: 0,
            in_flight: 0,
            in_flight_bytes: 0,
            reordered: HashMap::new(),
            tx,
            rx,
        })
    }

    /// # Errors
    ///
    /// Returns an error if a row group fails to encode or the file cannot be written.
    pub fn push(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.pending_rows += batch.num_rows();
        self.pending_bytes += batch
            .columns()
            .iter()
            .map(|column| column.get_array_memory_size())
            .sum::<usize>();
        self.pending.push(batch);

        if self.pending_rows >= self.sizer.target() || self.pending_bytes >= self.sizer.byte_cap() {
            self.dispatch()?;
        }
        Ok(())
    }

    /// # Errors
    ///
    /// Returns an error if a row group fails to encode or the file cannot be finalized.
    pub fn finish(mut self) -> Result<()> {
        if !self.pending.is_empty() {
            self.dispatch()?;
        }
        while self.in_flight > 0 {
            self.collect_one()?;
        }
        self.write_ready()?;
        debug_assert!(self.reordered.is_empty(), "every row group was written");
        self.writer.close()?;
        Ok(())
    }

    /// Hand the buffered batches to the pool, waiting first if too much decoded data is already
    /// in flight.
    fn dispatch(&mut self) -> Result<()> {
        let batches = std::mem::take(&mut self.pending);
        let input_bytes = self.pending_bytes;
        self.pending_rows = 0;
        self.pending_bytes = 0;
        if batches.is_empty() {
            return Ok(());
        }

        // Always allow one, so a row group larger than the whole budget still makes progress.
        while self.in_flight > 0 && !self.has_room_for(input_bytes) {
            self.collect_one()?;
        }

        let index = self.next_dispatch;
        self.next_dispatch += 1;
        self.sizer.dispatched();
        let writers = self.factory.create_column_writers(index)?;
        if writers.len() != self.schema.fields().len() {
            return Err(anyhow!(
                "parquet schema has {} leaves for {} columns; expected one each",
                writers.len(),
                self.schema.fields().len()
            ));
        }

        let schema = SchemaRef::clone(&self.schema);
        let tx = self.tx.clone();
        self.pool.spawn(move || {
            let chunks = encode_row_group(&schema, writers, &batches);
            // The receiver outlives every task it dispatched, so this only fails if the
            // pipeline was already torn down by an earlier error.
            let _ = tx.send(Encoded {
                index,
                input_bytes,
                chunks,
            });
        });
        self.in_flight += 1;
        self.in_flight_bytes += input_bytes;

        self.drain_finished()?;
        self.write_ready()
    }

    fn has_room_for(&self, input_bytes: usize) -> bool {
        self.in_flight < self.pool.current_num_threads()
            && self.in_flight_bytes + input_bytes <= IN_FLIGHT_INPUT_BUDGET
    }

    /// Wait for one row group to finish encoding.
    fn collect_one(&mut self) -> Result<()> {
        let Ok(encoded) = self.rx.recv() else {
            return Err(anyhow!("a parquet encoding task disappeared"));
        };
        self.accept(encoded)?;
        self.drain_finished()?;
        self.write_ready()
    }

    /// Take whatever has already finished, without waiting.
    fn drain_finished(&mut self) -> Result<()> {
        while let Ok(encoded) = self.rx.try_recv() {
            self.accept(encoded)?;
        }
        Ok(())
    }

    fn accept(&mut self, encoded: Encoded) -> Result<()> {
        self.in_flight -= 1;
        self.in_flight_bytes = self.in_flight_bytes.saturating_sub(encoded.input_bytes);
        let chunks = encoded.chunks?;
        self.reordered.insert(encoded.index, chunks);
        Ok(())
    }

    /// Write every row group whose turn has come.
    fn write_ready(&mut self) -> Result<()> {
        while let Some(chunks) = self.reordered.remove(&self.next_write) {
            let mut row_group = self.writer.next_row_group()?;
            for chunk in chunks {
                chunk.append_to_row_group(&mut row_group)?;
            }
            row_group.close()?;
            self.next_write += 1;
        }
        Ok(())
    }
}

/// Encode one row group's columns in parallel.
///
/// Each column reads its own slice of every batch, so the columns share nothing and the tasks
/// are independent. On a narrow table this is only a handful of tasks — the row groups running
/// alongside it are what fill the pool.
fn encode_row_group(
    schema: &Schema,
    writers: Vec<ArrowColumnWriter>,
    batches: &[RecordBatch],
) -> Result<Vec<ArrowColumnChunk>, ParquetError> {
    writers
        .into_par_iter()
        .enumerate()
        .map(|(column_index, mut writer)| {
            let field = schema.field(column_index);
            for batch in batches {
                for leaf in compute_leaves(field, batch.column(column_index))? {
                    writer.write(&leaf)?;
                }
            }
            writer.close()
        })
        .collect()
}

#[cfg(test)]
// Row indices in these fixtures are small enough that the f64 casts below are exact.
#[allow(clippy::cast_precision_loss)]
mod tests {
    use super::{RowGroupPipeline, schema_is_flat};
    use arrow_array::{
        ArrayRef, Float64Array, Int32Array, RecordBatch, StringArray,
        cast::AsArray,
        types::{Float64Type, Int32Type},
    };
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterProperties;
    use std::fs::File;
    use std::sync::Arc;

    const ROWS_PER_BATCH: usize = 4096;
    const ROW_GROUP_ROWS: usize = 16_384;

    /// Every value is a function of its file-wide row index, so reading the file back proves
    /// both that no row was lost and that the row groups landed in the order they were filled.
    fn batch(schema: &SchemaRef, first_row: usize, columns: usize) -> RecordBatch {
        let rows = first_row..first_row + ROWS_PER_BATCH;
        let arrays: Vec<ArrayRef> = (0..columns)
            .map(|column| match column % 3 {
                0 => Arc::new(Int32Array::from_iter_values(
                    rows.clone().map(|row| i32::try_from(row % 1000).unwrap()),
                )) as ArrayRef,
                1 => Arc::new(Float64Array::from_iter_values(
                    rows.clone().map(|row| row as f64 * 0.5),
                )) as ArrayRef,
                _ => Arc::new(StringArray::from_iter_values(
                    rows.clone().map(|row| format!("r{row}")),
                )) as ArrayRef,
            })
            .collect();
        RecordBatch::try_new(SchemaRef::clone(schema), arrays).expect("batch")
    }

    fn schema_of(columns: usize) -> SchemaRef {
        Arc::new(Schema::new(
            (0..columns)
                .map(|column| {
                    let data_type = match column % 3 {
                        0 => DataType::Int32,
                        1 => DataType::Float64,
                        _ => DataType::Utf8,
                    };
                    Field::new(format!("c{column}"), data_type, false)
                })
                .collect::<Vec<_>>(),
        ))
    }

    fn properties() -> WriterProperties {
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
            .build()
    }

    fn temp_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "sas7bdat-cli-{name}-{}.parquet",
            std::process::id()
        ))
    }

    /// Read the file back and assert every row still carries the value its index implies.
    fn assert_rows_in_order(path: &std::path::Path, rows: usize, columns: usize) {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).expect("open"))
            .expect("reader")
            .build()
            .expect("build");

        let mut seen = 0usize;
        for batch in reader {
            let batch = batch.expect("batch");
            for row in 0..batch.num_rows() {
                let expected = seen + row;
                for column in 0..columns {
                    let array = batch.column(column);
                    match column % 3 {
                        0 => assert_eq!(
                            array.as_primitive::<Int32Type>().value(row),
                            i32::try_from(expected % 1000).unwrap(),
                            "column {column}, row {expected}"
                        ),
                        1 => assert!(
                            (array.as_primitive::<Float64Type>().value(row)
                                - expected as f64 * 0.5)
                                .abs()
                                < f64::EPSILON,
                            "column {column}, row {expected}"
                        ),
                        _ => assert_eq!(
                            array.as_string::<i32>().value(row),
                            format!("r{expected}"),
                            "column {column}, row {expected}"
                        ),
                    }
                }
            }
            seen += batch.num_rows();
        }
        assert_eq!(seen, rows, "every row was written");
    }

    fn write_in_parallel(path: &std::path::Path, columns: usize, batches: usize) {
        let schema = schema_of(columns);
        let file = File::create(path).expect("create");
        let writer = ArrowWriter::try_new(file, SchemaRef::clone(&schema), Some(properties()))
            .expect("writer");
        let (file_writer, factory) = writer.into_serialized_writer().expect("serialized writer");
        let mut pipeline = RowGroupPipeline::new(
            file_writer,
            factory,
            SchemaRef::clone(&schema),
            ROW_GROUP_ROWS,
            None,
        )
        .expect("pipeline");
        for index in 0..batches {
            pipeline
                .push(batch(&schema, index * ROWS_PER_BATCH, columns))
                .expect("push");
        }
        pipeline.finish().expect("finish");
    }

    /// Enough row groups that encoding finishes out of order and the reorder buffer is used.
    #[test]
    fn writes_row_groups_in_file_order() {
        let path = temp_path("order");
        let batches = 60;
        write_in_parallel(&path, 3, batches);
        assert_rows_in_order(&path, batches * ROWS_PER_BATCH, 3);
        let _ = std::fs::remove_file(&path);
    }

    /// A single column gives the column axis nothing to split, so this is the case that only
    /// works because row groups are encoded in parallel too.
    #[test]
    fn handles_a_single_column() {
        let path = temp_path("one-column");
        let batches = 20;
        write_in_parallel(&path, 1, batches);
        assert_rows_in_order(&path, batches * ROWS_PER_BATCH, 1);
        let _ = std::fs::remove_file(&path);
    }

    /// A trailing partial row group must still be written.
    #[test]
    fn writes_a_final_partial_row_group() {
        let path = temp_path("partial");
        // 5 batches of 4096 = 20,480 rows: one full 16,384-row group and a 4,096-row remainder.
        write_in_parallel(&path, 4, 5);
        assert_rows_in_order(&path, 5 * ROWS_PER_BATCH, 4);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn matches_the_serial_writer() {
        let columns = 4;
        let batches = 30;
        let schema = schema_of(columns);

        let serial_path = temp_path("serial");
        let mut writer = ArrowWriter::try_new(
            File::create(&serial_path).expect("create"),
            SchemaRef::clone(&schema),
            Some(properties()),
        )
        .expect("writer");
        for index in 0..batches {
            writer
                .write(&batch(&schema, index * ROWS_PER_BATCH, columns))
                .expect("write");
        }
        writer.close().expect("close");

        let parallel_path = temp_path("parallel");
        write_in_parallel(&parallel_path, columns, batches);

        let serial = std::fs::read(&serial_path).expect("read serial");
        let parallel = std::fs::read(&parallel_path).expect("read parallel");
        assert_eq!(
            serial, parallel,
            "same input, same properties, same row group boundaries: the files should match byte for byte"
        );

        let _ = std::fs::remove_file(&serial_path);
        let _ = std::fs::remove_file(&parallel_path);
    }

    /// The failure this guards against: a fixed 65,536-row target caps a file at about 2.1
    /// billion rows, and a 234 GiB SAS file went past it.
    #[test]
    fn row_groups_stay_within_the_parquet_limit() {
        let mut sizer = super::RowGroupSizer::new(65_536);
        let mut groups = 0usize;
        let mut rows = 0u64;
        // Fill every row group the format allows and count the rows they cover.
        while groups < super::MAX_ROW_GROUPS {
            rows += sizer.target() as u64;
            sizer.dispatched();
            groups += 1;
        }
        assert!(
            rows > 10_000_000_000,
            "the row groups a file can hold should cover more rows than any SAS file will, got {rows}"
        );
        assert!(sizer.target() > 65_536, "the target grew");
    }

    /// A row group closed by bytes ignores the row target, so the byte cap has to grow too or
    /// the count runs past the limit regardless of how large the target got.
    #[test]
    fn the_byte_cap_grows_with_the_row_target() {
        let mut sizer = super::RowGroupSizer::new(65_536);
        let start = sizer.byte_cap();
        for _ in 0..super::MAX_ROW_GROUPS {
            sizer.dispatched();
        }
        assert!(sizer.byte_cap() > start, "the byte cap grew");
        assert!(
            sizer.byte_cap() <= super::MAX_ROW_GROUP_INPUT_BYTES,
            "and stayed bounded"
        );
    }

    /// Row groups must not shrink back, or the count would run away again.
    #[test]
    fn the_row_target_only_grows() {
        let mut sizer = super::RowGroupSizer::new(1024);
        let mut previous = sizer.target();
        for _ in 0..super::MAX_ROW_GROUPS {
            assert!(sizer.target() >= previous);
            previous = sizer.target();
            sizer.dispatched();
        }
    }

    #[test]
    fn nested_columns_are_not_flat() {
        let flat = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        assert!(schema_is_flat(&flat));

        let nested = Schema::new(vec![Field::new(
            "a",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            false,
        )]);
        assert!(
            !schema_is_flat(&nested),
            "a nested column maps to more than one leaf, so the parallel path must decline"
        );
    }
}
