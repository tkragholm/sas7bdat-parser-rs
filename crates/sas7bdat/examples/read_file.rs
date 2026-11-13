//! Open a SAS7BDAT file, print its columns, and print every row.
//!
//! ```sh
//! cargo run -p sas7bdat --example read_file
//! cargo run -p sas7bdat --example read_file -- path/to/file.sas7bdat
//! ```
//!
//! With no argument it reads the five-row example file that ships with the R
//! package, `crates/r-plugin/inst/extdata/people.sas7bdat`.

use sas7bdat::{Dataset, Result};
use std::ops::ControlFlow;
use std::path::PathBuf;

fn main() -> Result<()> {
    let path = std::env::args_os().nth(1).map_or_else(
        || {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../r-plugin/inst/extdata/people.sas7bdat")
        },
        PathBuf::from,
    );
    let ds = Dataset::open(&path)?;

    let meta = ds.metadata();
    println!(
        "{}: {} rows, {} columns, encoding {}",
        path.display(),
        meta.row_count - meta.deleted_row_count,
        ds.columns().len(),
        meta.encoding.as_deref().unwrap_or("not declared"),
    );
    for column in ds.columns() {
        println!("  {:<8} {:?}", column.name, column.logical_type);
    }

    ds.scan().visit_rows(|row| {
        let cells: Vec<String> = row.iter().map(|cell| format!("{cell:?}")).collect();
        println!("{}", cells.join("  "));
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(())
}
