use sas7bdat_simd::{Dataset, OpenOptions};
use std::{path::PathBuf, time::Instant};

fn main() -> Result<(), String> {
    let fixture = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or_else(|| "usage: cargo run --manifest-path crates/sas7bdat-simd/Cargo.toml --release --example open_breakdown -- <fixture>".to_owned())?;

    let breakdown =
        Dataset::open_breakdown(&fixture, OpenOptions::default()).map_err(|err| err.to_string())?;
    let dataset_open_start = Instant::now();
    let dataset = Dataset::open(&fixture).map_err(|err| err.to_string())?;
    let dataset_open_ns = dataset_open_start.elapsed().as_nanos();

    println!(
        "{{\
\"fixture\":\"{}\",\
\"metadata_ns\":{},\
\"file_open_ns\":{},\
\"mmap_ns\":{},\
\"probe_header_ns\":{},\
\"rewind_ns\":{},\
\"parse_layout_ns\":{},\
\"manual_total_ns\":{},\
\"dataset_open_ns\":{},\
\"used_mmap\":{},\
\"page_size\":{},\
\"page_count\":{},\
\"column_count\":{},\
\"row_count\":{}\
}}",
        fixture.display(),
        breakdown.metadata_ns,
        breakdown.file_open_ns,
        breakdown.mmap_ns.unwrap_or(0),
        breakdown.probe_header_ns,
        breakdown.rewind_ns,
        breakdown.parse_layout_ns,
        breakdown.total_ns,
        dataset_open_ns,
        breakdown.used_mmap,
        dataset.metadata().page_size,
        dataset.metadata().page_count,
        dataset.columns().len(),
        dataset.metadata().row_count,
    );

    Ok(())
}
