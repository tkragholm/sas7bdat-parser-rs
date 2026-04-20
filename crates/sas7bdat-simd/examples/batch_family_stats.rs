use sas7bdat_simd::{BatchHint, Dataset};
use std::{env, ops::ControlFlow, path::PathBuf};

fn main() {
    let path = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .expect("usage: cargo run --example batch_family_stats -- <path-to-sas7bdat>");
    let batch_rows = env::var("BATCH_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(256);

    let ds = Dataset::open(&path).expect("open fixture");
    let stats = ds
        .scan()
        .with_batch_hint(BatchHint::Rows(batch_rows))
        .visit_batches(|_| Ok(ControlFlow::Continue(())))
        .expect("scan typed batches");

    println!("path={}", path.display());
    println!("row_count={}", ds.metadata().row_count);
    println!("column_count={}", ds.columns().len());
    println!("decode_batches={}", stats.decode_batches);
    println!("batch_rows={batch_rows}");
    println!("rows_seen={}", stats.rows_seen);
    println!("rows_emitted={}", stats.rows_emitted);
    println!("pages_seen={}", stats.pages_seen);
    println!("compressed_pages={}", stats.compressed_pages);
    println!("raw_bytes_read={}", stats.raw_bytes_read);
    println!("row_bytes_materialized={}", stats.row_bytes_materialized);
}
