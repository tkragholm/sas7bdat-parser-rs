use sas7bdat_simd::{Dataset, OpenOptions};
use std::{path::PathBuf, time::Instant};

#[path = "common/mod.rs"]
mod common;
use common::next_arg;

fn main() -> Result<(), String> {
    let mut args = std::env::args_os().skip(1);
    let help = "usage: cargo run --release --example breakdown -- <open|descriptor> <fixture>";
    let mode = next_arg(&mut args, help)?;
    let fixture = next_arg(&mut args, help).map(PathBuf::from)?;

    match mode.as_str() {
        "open" => run_open_breakdown(&fixture),
        "descriptor" => run_descriptor_breakdown(&fixture),
        _ => Err(help.to_owned()),
    }
}

fn run_open_breakdown(fixture: &std::path::Path) -> Result<(), String> {
    let breakdown =
        Dataset::open_breakdown(fixture, OpenOptions::default()).map_err(|err| err.to_string())?;
    let dataset_open_start = Instant::now();
    let dataset = Dataset::open(fixture).map_err(|err| err.to_string())?;
    let dataset_open_ns = dataset_open_start.elapsed().as_nanos();

    println!(
        "{}",
        serde_json::json!({
            "fixture": fixture.display().to_string(),
            "metadata_ns": breakdown.metadata_ns,
            "file_open_ns": breakdown.file_open_ns,
            "mmap_ns": breakdown.mmap_ns.unwrap_or(0),
            "probe_header_ns": breakdown.probe_header_ns,
            "rewind_ns": breakdown.rewind_ns,
            "parse_layout_ns": breakdown.parse_layout_ns,
            "total_ns": breakdown.total_ns,
            "dataset_open_ns": dataset_open_ns,
            "used_mmap": breakdown.used_mmap,
            "page_size": dataset.metadata().page_size,
            "page_count": dataset.metadata().page_count,
            "column_count": dataset.columns().len(),
            "row_count": dataset.metadata().row_count,
        })
    );
    Ok(())
}

fn run_descriptor_breakdown(fixture: &std::path::Path) -> Result<(), String> {
    let dataset = Dataset::open(fixture).map_err(|err| err.to_string())?;
    let breakdown = dataset
        .descriptor_breakdown()
        .map_err(|err| err.to_string())?;

    println!(
        "{}",
        serde_json::json!({
            "fixture": fixture.display().to_string(),
            "page_read_ns": breakdown.page_read_ns,
            "header_read_ns": breakdown.header_read_ns,
            "classify_ns": breakdown.classify_ns,
            "total_ns": breakdown.total_ns,
            "pages_seen": breakdown.pages_seen,
            "descriptors_emitted": breakdown.descriptors_emitted,
            "row_spans_emitted": breakdown.row_spans_emitted,
            "total_candidate_rows": breakdown.total_candidate_rows,
            "page_size": dataset.metadata().page_size,
            "page_count": dataset.metadata().page_count,
            "row_count": dataset.metadata().row_count,
        })
    );
    Ok(())
}
