use std::env;
use std::time::Instant;

#[cfg(feature = "parallel-rows")]
use rayon::prelude::*;
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
    let use_columnar = std::env::var("BENCH_COLUMNAR").is_ok();

    #[cfg(feature = "parallel-rows")]
    if use_columnar {
        let use_columnar_par = std::env::var("BENCH_COLUMNAR_PAR").is_ok();
        const COLUMNAR_CHUNK: usize = 1024;
        while let Some(batch) = rows.next_columnar_batch(COLUMNAR_CHUNK)? {
            row_count += u64::try_from(batch.row_count).unwrap_or(0);
            let batch_non_null = if use_columnar_par {
                batch
                    .par_columns()
                    .map(|column| column.non_null_count())
                    .sum::<u64>()
            } else {
                batch
                    .columns()
                    .map(|column| column.non_null_count())
                    .sum::<u64>()
            };
            non_null_cells += batch_non_null;
        }
    } else if use_parallel {
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
        if use_parallel || use_columnar {
            eprintln!(
                "BENCH_PARALLEL_ROWS or BENCH_COLUMNAR set but the parallel-rows feature is disabled; \
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
