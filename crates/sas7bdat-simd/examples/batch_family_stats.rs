use sas7bdat_simd::{BatchHint, Dataset};
use std::{env, ops::ControlFlow, path::PathBuf};

#[allow(clippy::cast_precision_loss)]
fn pct(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        (part as f64 * 100.0) / total as f64
    }
}

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

    let total_cells = stats
        .batch_staged_numeric_cells
        .saturating_add(stats.batch_direct_numeric_cells)
        .saturating_add(stats.batch_direct_raw_bytes_cells)
        .saturating_add(stats.batch_direct_utf8_single_byte_cells)
        .saturating_add(stats.batch_direct_utf8_borrowed_cells)
        .saturating_add(stats.batch_direct_utf8_owned_cells)
        .saturating_add(stats.batch_fallback_cells);

    println!("path={}", path.display());
    println!("row_count={}", ds.metadata().row_count);
    println!("column_count={}", ds.columns().len());
    println!("decode_batches={}", stats.decode_batches);
    println!("batch_rows={batch_rows}");
    println!("total_routed_cells={total_cells}");
    println!(
        "staged_numeric_cells={} ({:.2}%)",
        stats.batch_staged_numeric_cells,
        pct(stats.batch_staged_numeric_cells, total_cells)
    );
    println!(
        "direct_numeric_cells={} ({:.2}%)",
        stats.batch_direct_numeric_cells,
        pct(stats.batch_direct_numeric_cells, total_cells)
    );
    println!(
        "direct_raw_bytes_cells={} ({:.2}%)",
        stats.batch_direct_raw_bytes_cells,
        pct(stats.batch_direct_raw_bytes_cells, total_cells)
    );
    println!(
        "direct_utf8_single_byte_cells={} ({:.2}%)",
        stats.batch_direct_utf8_single_byte_cells,
        pct(stats.batch_direct_utf8_single_byte_cells, total_cells)
    );
    println!(
        "direct_utf8_borrowed_cells={} ({:.2}%)",
        stats.batch_direct_utf8_borrowed_cells,
        pct(stats.batch_direct_utf8_borrowed_cells, total_cells)
    );
    println!(
        "direct_utf8_owned_cells={} ({:.2}%)",
        stats.batch_direct_utf8_owned_cells,
        pct(stats.batch_direct_utf8_owned_cells, total_cells)
    );
    println!(
        "direct_utf8_owned_interned_hits={}",
        stats.batch_direct_utf8_owned_interned_hits
    );
    println!(
        "direct_utf8_owned_seen_once_promotions={}",
        stats.batch_direct_utf8_owned_seen_once_promotions
    );
    println!(
        "fallback_cells={} ({:.2}%)",
        stats.batch_fallback_cells,
        pct(stats.batch_fallback_cells, total_cells)
    );
}
