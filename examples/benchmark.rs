use std::env;
use std::time::Instant;

use sas7bdat_parser_rs::SasFile;

#[cfg(feature = "parallel-rows")]
use sas7bdat_parser_rs::value::Value;

#[cfg(feature = "hotpath")]
use hotpath::{Format, GuardBuilder};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "hotpath")]
    let _hotpath = GuardBuilder::new("benchmark")
        .format(Format::Table) // optional: pretty output
        .limit(20) // optional: how many rows to show
        .build();

    let path = env::args()
        .nth(1)
        .expect("usage: cargo run --release --example benchmark -- <file.sas7bdat>");

    let absolute = std::fs::canonicalize(&path)?;
    let start = Instant::now();

    let mut file = SasFile::open(&absolute)?;
    let column_count = file.metadata().column_count;

    let mut rows = file.rows()?;
    let mut row_count: u64 = 0;
    let mut non_null_cells: u64 = 0;

    let use_parallel = std::env::var("BENCH_PARALLEL_ROWS").is_ok();

    #[cfg(feature = "parallel-rows")]
    if use_parallel {
        rows.stream_all_parallel_owned(|values| {
            row_count += 1;
            for value in &values {
                let is_missing = match value {
                    Value::Missing(_) => true,
                    Value::Str(text) => text.is_empty(),
                    _ => false,
                };
                if !is_missing {
                    non_null_cells += 1;
                }
            }
            Ok(())
        })?;
    } else {
        rows.stream_all(|row| {
            row_count += 1;
            for cell in row.iter() {
                let cell = cell?;
                if !cell.is_missing() {
                    non_null_cells += 1;
                }
            }
            Ok(())
        })?;
    }

    #[cfg(not(feature = "parallel-rows"))]
    {
        if use_parallel {
            eprintln!(
                "BENCH_PARALLEL_ROWS set but the parallel-rows feature is disabled; \
                 running sequential benchmark instead"
            );
        }
        rows.stream_all(|row| {
            row_count += 1;
            for cell in row.iter() {
                let cell = cell?;
                if !cell.is_missing() {
                    non_null_cells += 1;
                }
            }
            Ok(())
        })?;
    }

    let elapsed = start.elapsed();

    println!("File            : {}", absolute.display());
    println!("Rows processed  : {}", row_count);
    println!("Columns         : {}", column_count);
    println!("Non-null cells  : {}", non_null_cells);
    println!("Elapsed (ms)    : {:.2}", elapsed.as_secs_f64() * 1_000.0);

    Ok(())
}
