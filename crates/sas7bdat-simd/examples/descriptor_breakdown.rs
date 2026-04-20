use sas7bdat_simd::Dataset;
use std::path::PathBuf;

fn main() -> Result<(), String> {
    let fixture = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or_else(|| "usage: cargo run --manifest-path crates/sas7bdat-simd/Cargo.toml --release --example descriptor_breakdown -- <fixture>".to_owned())?;

    let dataset = Dataset::open(&fixture).map_err(|err| err.to_string())?;
    let breakdown = dataset
        .descriptor_breakdown()
        .map_err(|err| err.to_string())?;

    println!(
        "{{\
\"fixture\":\"{}\",\
\"page_read_ns\":{},\
\"header_read_ns\":{},\
\"classify_ns\":{},\
\"total_ns\":{},\
\"pages_seen\":{},\
\"descriptors_emitted\":{},\
\"row_spans_emitted\":{},\
\"total_candidate_rows\":{},\
\"page_size\":{},\
\"page_count\":{},\
\"row_count\":{}\
}}",
        fixture.display(),
        breakdown.page_read_ns,
        breakdown.header_read_ns,
        breakdown.classify_ns,
        breakdown.total_ns,
        breakdown.pages_seen,
        breakdown.descriptors_emitted,
        breakdown.row_spans_emitted,
        breakdown.total_candidate_rows,
        dataset.metadata().page_size,
        dataset.metadata().page_count,
        dataset.metadata().row_count,
    );

    Ok(())
}
