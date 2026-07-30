#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]
// Times the parquet writer alone, on a synthetic table shaped like production data:
// few columns, many rows. Usage: writer_bench <cols> <total_rows>
//
// Both arms start from the scanner's own columnar batch and pay the Arrow conversion, which
// is what each really does: the serial path converts on the calling thread before `write`,
// the pipeline converts inside its encode tasks. Handing the pipeline a pre-built
// `RecordBatch` would have hidden exactly the work this comparison is about.
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use sas7bdat::{OwnedColumnBuffer, OwnedColumnarBatch, TrustedOffsets};
use sas7bdat_cli::parquet_pipeline::RowGroupPipeline;
use std::sync::Arc;
use std::time::Instant;

const ROWS_PER_BATCH: usize = 8192;
const ROW_GROUP_ROWS: usize = 65_536;

fn make(cols: usize) -> (SchemaRef, OwnedColumnarBatch) {
    let mut fields = Vec::new();
    let mut columns: Vec<OwnedColumnBuffer> = Vec::new();
    for c in 0..cols {
        match c % 4 {
            0 => {
                fields.push(Field::new(format!("f{c}"), DataType::Float64, true));
                columns.push(OwnedColumnBuffer::F64 {
                    values: (0..ROWS_PER_BATCH)
                        .map(|r| (r as f64) * 1.5 + c as f64)
                        .collect(),
                    valid: None,
                });
            }
            1 => {
                fields.push(Field::new(format!("i{c}"), DataType::Int32, true));
                columns.push(OwnedColumnBuffer::I32 {
                    values: (0..ROWS_PER_BATCH).map(|r| (r % 977) as i32).collect(),
                    valid: None,
                });
            }
            _ => {
                fields.push(Field::new(format!("s{c}"), DataType::Utf8, true));
                let mut offsets = TrustedOffsets::with_capacity_for_rows(ROWS_PER_BATCH);
                let mut data = Vec::new();
                for r in 0..ROWS_PER_BATCH {
                    data.extend_from_slice(format!("value-{}-{}", c, r % 5000).as_bytes());
                    offsets.push_current_data_len(data.len()).unwrap();
                }
                columns.push(OwnedColumnBuffer::Utf8 {
                    offsets,
                    data,
                    valid: None,
                    dictionary_ids: None,
                });
            }
        }
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = OwnedColumnarBatch {
        row_base: 0u64.into(),
        row_count: ROWS_PER_BATCH,
        columns,
    };
    (schema, batch)
}

fn props() -> WriterProperties {
    WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build()
}

fn main() {
    let mut args = std::env::args().skip(1);
    let cols: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(10);
    let total_rows: usize = args
        .next()
        .and_then(|a| a.parse().ok())
        .unwrap_or(4_000_000);
    let repeats = total_rows / ROWS_PER_BATCH;
    let (schema, batch) = make(cols);
    let bytes: usize = batch.heap_bytes();

    let start = Instant::now();
    let mut w = ArrowWriter::try_new(Vec::new(), Arc::clone(&schema), Some(props())).unwrap();
    for _ in 0..repeats {
        let record = batch
            .clone()
            .into_arrow_record_batch(Arc::clone(&schema))
            .unwrap();
        w.write(&record).unwrap();
    }
    let serial_out = w.into_inner().unwrap();
    let serial = start.elapsed();

    let start = Instant::now();
    let w = ArrowWriter::try_new(Vec::new(), Arc::clone(&schema), Some(props())).unwrap();
    let (fw, factory) = w.into_serialized_writer().unwrap();
    let mut pipeline =
        RowGroupPipeline::new(fw, factory, Arc::clone(&schema), ROW_GROUP_ROWS, None).unwrap();
    for _ in 0..repeats {
        pipeline.push(batch.clone()).unwrap();
    }
    pipeline.finish().unwrap();
    let parallel = start.elapsed();

    println!(
        "{cols:>5} cols  {:>10} rows  {:>6} MB in   serial {:>9.3?}  parallel {:>9.3?}  {:.2}x   (serial out {} MB)",
        repeats * ROWS_PER_BATCH,
        repeats * bytes / 1_048_576,
        serial,
        parallel,
        serial.as_secs_f64() / parallel.as_secs_f64(),
        serial_out.len() / 1_048_576,
    );
}
