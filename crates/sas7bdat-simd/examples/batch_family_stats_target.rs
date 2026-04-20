use sas7bdat_simd::{BatchHint, Dataset};
use std::{
    env, fs,
    path::{Path, PathBuf},
};

const TARGET_MIN_SIZE_BYTES: u64 = 10 * 1024 * 1024;

#[derive(Debug, Default, Clone)]
struct Aggregated {
    files_scanned: usize,
    total_rows: u64,
    total_columns: u64,
    decode_batches: u64,
    pages_seen: u64,
    compressed_pages: u64,
    raw_bytes_read: u64,
    row_bytes_materialized: u64,
}

#[derive(Debug)]
struct FileSummary {
    relative: String,
    size_bytes: u64,
    row_count: u64,
    column_count: usize,
    pages_seen: u64,
    compressed_pages: u64,
    raw_bytes_read: u64,
    row_bytes_materialized: u64,
    decode_batches: u64,
}

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures")
}

fn collect_sas7bdat_files(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_sas7bdat_files(&path, out);
            continue;
        }
        let is_sas = path
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("sas7bdat"));
        if is_sas {
            out.push(path);
        }
    }
}

fn discover_target_paths(min_size_bytes: u64) -> Vec<PathBuf> {
    let fixtures_root = fixture_root();
    let mut roots = Vec::new();
    if let Ok(entries) = fs::read_dir(&fixtures_root) {
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };
            if name == "raw_data" {
                continue;
            }
            roots.push(path);
        }
    }
    roots.sort();

    let mut files = Vec::new();
    for root in roots {
        collect_sas7bdat_files(&root, &mut files);
    }
    files.sort();
    files.retain(|path| fs::metadata(path).is_ok_and(|meta| meta.len() >= min_size_bytes));
    files
}

#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
fn main() {
    let batch_rows = env::var("BATCH_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(256);
    let max_files = env::var("MAX_FILES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(usize::MAX);
    let print_top = env::var("TOP")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10);

    let fixtures_root = fixture_root();
    let mut targets = discover_target_paths(TARGET_MIN_SIZE_BYTES);
    if max_files != usize::MAX {
        targets.truncate(max_files);
    }

    let mut aggregated = Aggregated::default();
    let mut file_summaries = Vec::new();

    for path in targets {
        let Ok(dataset) = Dataset::open(&path) else {
            continue;
        };
        let Ok(stats) = dataset
            .scan()
            .with_batch_hint(BatchHint::Rows(batch_rows))
            .owned_batch_scan_breakdown()
        else {
            continue;
        };

        let relative = path.strip_prefix(&fixtures_root).map_or_else(
            |_| path.display().to_string(),
            |p| p.to_string_lossy().to_string(),
        );
        let size_bytes = fs::metadata(&path).map_or(0, |meta| meta.len());
        let stats_summary = stats.stats;

        file_summaries.push(FileSummary {
            relative,
            size_bytes,
            row_count: dataset.metadata().row_count,
            column_count: dataset.columns().len(),
            pages_seen: stats_summary.pages_seen,
            compressed_pages: stats_summary.compressed_pages,
            raw_bytes_read: stats_summary.raw_bytes_read,
            row_bytes_materialized: stats_summary.row_bytes_materialized,
            decode_batches: stats_summary.decode_batches,
        });

        aggregated.files_scanned += 1;
        aggregated.total_rows = aggregated
            .total_rows
            .saturating_add(dataset.metadata().row_count);
        aggregated.total_columns = aggregated
            .total_columns
            .saturating_add(u64::try_from(dataset.columns().len()).unwrap_or(0));
        aggregated.decode_batches = aggregated
            .decode_batches
            .saturating_add(stats_summary.decode_batches);
        aggregated.pages_seen = aggregated
            .pages_seen
            .saturating_add(stats_summary.pages_seen);
        aggregated.compressed_pages = aggregated
            .compressed_pages
            .saturating_add(stats_summary.compressed_pages);
        aggregated.raw_bytes_read = aggregated
            .raw_bytes_read
            .saturating_add(stats_summary.raw_bytes_read);
        aggregated.row_bytes_materialized = aggregated
            .row_bytes_materialized
            .saturating_add(stats_summary.row_bytes_materialized);
    }

    println!("files_scanned={}", aggregated.files_scanned);
    println!("batch_rows={batch_rows}");
    println!("total_rows={}", aggregated.total_rows);
    println!("decode_batches={}", aggregated.decode_batches);
    println!("pages_seen={}", aggregated.pages_seen);
    println!("compressed_pages={}", aggregated.compressed_pages);
    println!("raw_bytes_read={}", aggregated.raw_bytes_read);
    println!(
        "row_bytes_materialized={}",
        aggregated.row_bytes_materialized
    );

    file_summaries.sort_by(|left, right| {
        right
            .row_bytes_materialized
            .cmp(&left.row_bytes_materialized)
            .then_with(|| right.raw_bytes_read.cmp(&left.raw_bytes_read))
            .then_with(|| right.decode_batches.cmp(&left.decode_batches))
    });

    println!("\nTop files by row materialization / I/O pressure:");
    for summary in file_summaries.iter().take(print_top) {
        println!(
            "{} | size_mb={:.2} rows={} cols={} decode_batches={} row_bytes_materialized={} compressed_pages={} raw_bytes_read={} pages_seen={}",
            summary.relative,
            summary.size_bytes as f64 / (1024.0 * 1024.0),
            summary.row_count,
            summary.column_count,
            summary.decode_batches,
            summary.row_bytes_materialized,
            summary.compressed_pages,
            summary.raw_bytes_read,
            summary.pages_seen,
        );
    }
}
