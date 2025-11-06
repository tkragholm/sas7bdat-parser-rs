use std::env;
use std::time::Instant;

use sas7bdat_parser_rs::SasFile;
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

    while let Some(row) = rows.try_next()? {
        row_count += 1;
        non_null_cells += row
            .iter()
            .filter(|value| !matches!(value, Value::Missing(_)))
            .count() as u64;
    }

    let elapsed = start.elapsed();

    println!("File            : {}", absolute.display());
    println!("Rows processed  : {}", row_count);
    println!("Columns         : {}", column_count);
    println!("Non-null cells  : {}", non_null_cells);
    println!("Elapsed (ms)    : {:.2}", elapsed.as_secs_f64() * 1_000.0);

    Ok(())
}
