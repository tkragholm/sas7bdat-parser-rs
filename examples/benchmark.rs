use std::env;
use std::time::Instant;

use sas7bdat_parser_rs::SasFile;

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

    let elapsed = start.elapsed();

    println!("File            : {}", absolute.display());
    println!("Rows processed  : {}", row_count);
    println!("Columns         : {}", column_count);
    println!("Non-null cells  : {}", non_null_cells);
    println!("Elapsed (ms)    : {:.2}", elapsed.as_secs_f64() * 1_000.0);

    Ok(())
}
