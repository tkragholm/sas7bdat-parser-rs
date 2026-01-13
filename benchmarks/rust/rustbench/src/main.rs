use sas7bdat::SasReader;
use std::{fs::File, time::Instant};

fn main() {
    let path = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: sas7bdat-rustbench <path-to-sas7bdat>");
        std::process::exit(1);
    });

    let file = File::open(&path).unwrap_or_else(|err| {
        eprintln!("Input file not found: {} ({})", path, err);
        std::process::exit(1);
    });

    let mut reader = SasReader::from_reader(file).unwrap_or_else(|err| {
        eprintln!("sas7bdat error: {}", err);
        std::process::exit(1);
    });

    let column_count = usize::try_from(reader.metadata().column_count).unwrap_or_else(|_| {
        eprintln!("column count exceeds usize");
        std::process::exit(1);
    });

    let start = Instant::now();
    let mut row_count = 0usize;
    let mut rows = reader.rows().unwrap_or_else(|err| {
        eprintln!("sas7bdat error: {}", err);
        std::process::exit(1);
    });
    while let Some(row) = rows.try_next().unwrap_or_else(|err| {
        eprintln!("sas7bdat error: {}", err);
        std::process::exit(1);
    }) {
        let _row_len = row.len();
        row_count += 1;
    }
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("File           : {}", path);
    println!("Rows processed : {}", row_count);
    println!("Columns        : {}", column_count);
    println!("Elapsed (ms)   : {:.2}", elapsed_ms);
}
