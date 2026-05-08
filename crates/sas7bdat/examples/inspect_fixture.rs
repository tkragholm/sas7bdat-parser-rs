use sas7bdat::{BatchHint, Dataset};
use std::{env, ops::ControlFlow, path::PathBuf};

fn main() {
    let path = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .expect("usage: cargo run --example inspect_fixture -- <path>");

    let ds = Dataset::open(&path).expect("open fixture");
    println!("path={}", path.display());
    println!("table_name={:?}", ds.metadata().table_name);
    println!("encoding={:?}", ds.metadata().encoding);
    println!("page_size={}", ds.metadata().page_size);
    println!("page_count={}", ds.metadata().page_count);
    println!("row_count={}", ds.metadata().row_count);
    println!("row_len={}", ds.metadata().row_len);
    println!("compression={:?}", ds.metadata().compression);
    println!("column_count={}", ds.columns().len());
    for column in ds.columns() {
        println!(
            "column idx={} name={} type={:?} width={} offset={}",
            column.index, column.name, column.logical_type, column.physical_width, column.offset
        );
    }

    let rows = ds.scan().limit(3).collect_rows().expect("collect rows");
    for row in &rows {
        println!("row {}", row.row_index);
        for (idx, cell) in row.cells.iter().enumerate() {
            println!("  cell[{idx}]={cell:?}");
        }
    }

    let batches = ds
        .scan()
        .limit(3)
        .with_batch_hint(BatchHint::Rows(3))
        .collect_batches()
        .expect("collect batches");
    println!("batch_count={}", batches.len());
    for batch in &batches {
        println!(
            "batch row_base={} row_count={}",
            batch.row_base, batch.row_count
        );
        for (idx, column) in batch.columns.iter().enumerate() {
            println!("  column[{idx}]={column:?}");
        }
    }

    let mut raw_rows = 0u64;
    ds.scan()
        .limit(3)
        .visit_raw_rows(|_| {
            raw_rows += 1;
            Ok(ControlFlow::Continue(()))
        })
        .expect("raw scan");
    println!("raw_rows={raw_rows}");
}
