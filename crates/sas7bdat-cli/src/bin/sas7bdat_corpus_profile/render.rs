use super::{
    CorpusSummary, ScanCsvContext, ScanSummary, bytes_to_megabytes, join_map, join_ranked_files,
    join_ranked_files_megabytes, join_scan_ranked_files,
};
use std::fmt::Write as _;

pub fn render_summary_txt(summary: &CorpusSummary, roots: &[String], sample_rows: usize) -> String {
    let mut out = String::new();
    writeln!(out, "Corpus Profile Summary").expect("write summary");
    writeln!(out, "======================").expect("write summary");
    writeln!(out).expect("write summary");
    writeln!(out, "roots: {}", roots.join(", ")).expect("write summary");
    writeln!(out, "sample_rows: {sample_rows}").expect("write summary");
    writeln!(out, "discovered_files: {}", summary.discovered_files).expect("write summary");
    writeln!(out, "profiled_files: {}", summary.profiled_files).expect("write summary");
    writeln!(out, "failed_files: {}", summary.failed_files).expect("write summary");
    writeln!(
        out,
        "total_size_megabytes: {}",
        bytes_to_megabytes(summary.total_size_bytes)
    )
    .expect("write summary");
    writeln!(out, "total_rows: {}", summary.total_rows).expect("write summary");
    writeln!(out, "total_columns: {}", summary.total_columns).expect("write summary");
    writeln!(
        out,
        "total_string_columns: {}",
        summary.total_string_columns
    )
    .expect("write summary");
    writeln!(
        out,
        "total_numeric_like_columns: {}",
        summary.total_numeric_like_columns
    )
    .expect("write summary");
    writeln!(
        out,
        "total_sampled_string_cells: {}",
        summary.total_sampled_string_cells
    )
    .expect("write summary");
    writeln!(
        out,
        "total_sampled_empty_string_cells: {}",
        summary.total_sampled_empty_string_cells
    )
    .expect("write summary");
    writeln!(
        out,
        "total_sampled_ascii_string_cells: {}",
        summary.total_sampled_ascii_string_cells
    )
    .expect("write summary");
    writeln!(out).expect("write summary");
    writeln!(
        out,
        "compression_counts: {}",
        join_map(&summary.compression_counts)
    )
    .expect("write summary");
    writeln!(
        out,
        "encoding_counts: {}",
        join_map(&summary.encoding_counts)
    )
    .expect("write summary");
    writeln!(out, "tag_counts: {}", join_map(&summary.tag_counts)).expect("write summary");
    writeln!(out).expect("write summary");
    writeln!(
        out,
        "top_by_size_megabytes: {}",
        join_ranked_files_megabytes(&summary.top_by_size_bytes)
    )
    .expect("write summary");
    writeln!(
        out,
        "top_by_row_count: {}",
        join_ranked_files(&summary.top_by_row_count)
    )
    .expect("write summary");
    writeln!(
        out,
        "top_by_column_count: {}",
        join_ranked_files(&summary.top_by_column_count)
    )
    .expect("write summary");
    writeln!(
        out,
        "top_by_string_columns: {}",
        join_ranked_files(&summary.top_by_string_columns)
    )
    .expect("write summary");
    out
}

pub fn render_scan_summary_txt(
    summary: &ScanSummary,
    roots: &[String],
    context: &ScanCsvContext,
) -> String {
    let mut out = String::new();
    writeln!(out, "Corpus Scan Profile Summary").expect("write summary");
    writeln!(out, "==========================").expect("write summary");
    writeln!(out).expect("write summary");
    writeln!(out, "roots: {}", roots.join(", ")).expect("write summary");
    writeln!(out, "mode: {}", context.mode).expect("write summary");
    writeln!(out, "projection: {}", context.projection).expect("write summary");
    writeln!(out, "io_backend: {}", context.io_backend).expect("write summary");
    writeln!(out, "batch_rows: {}", context.batch_rows).expect("write summary");
    if context.limit.is_empty() {
        writeln!(out, "limit: all").expect("write summary");
    } else {
        writeln!(out, "limit: {}", context.limit).expect("write summary");
    }
    writeln!(out).expect("write summary");
    writeln!(out, "discovered_files: {}", summary.discovered_files).expect("write summary");
    writeln!(out, "profiled_files: {}", summary.profiled_files).expect("write summary");
    writeln!(out, "failed_files: {}", summary.failed_files).expect("write summary");
    writeln!(out, "total_elapsed_ns: {}", summary.total_elapsed_ns).expect("write summary");
    writeln!(out, "total_rows_emitted: {}", summary.total_rows_emitted).expect("write summary");
    writeln!(
        out,
        "total_raw_bytes_read: {}",
        summary.total_raw_bytes_read
    )
    .expect("write summary");
    writeln!(
        out,
        "total_row_bytes_materialized: {}",
        summary.total_row_bytes_materialized
    )
    .expect("write summary");
    writeln!(out, "total_pages_seen: {}", summary.total_pages_seen).expect("write summary");
    writeln!(
        out,
        "total_compressed_pages: {}",
        summary.total_compressed_pages
    )
    .expect("write summary");
    writeln!(out).expect("write summary");
    writeln!(
        out,
        "slowest_by_elapsed: {}",
        join_scan_ranked_files(&summary.slowest_by_elapsed)
    )
    .expect("write summary");
    writeln!(
        out,
        "largest_by_raw_bytes: {}",
        join_scan_ranked_files(&summary.largest_by_raw_bytes)
    )
    .expect("write summary");
    out
}
