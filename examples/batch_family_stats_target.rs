use sas7bdat_simd::{BatchHint, Dataset, ScanStats};
use std::{
    env, fs,
    ops::ControlFlow,
    path::{Path, PathBuf},
};

const TARGET_MIN_SIZE_BYTES: u64 = 10 * 1024 * 1024;

#[derive(Debug, Default, Clone)]
struct Aggregated {
    files_scanned: usize,
    total_rows: u64,
    total_columns: u64,
    decode_batches: u64,
    routed_cells: u64,
    staged_numeric_cells: u64,
    direct_numeric_cells: u64,
    direct_raw_bytes_cells: u64,
    direct_utf8_single_byte_cells: u64,
    direct_utf8_borrowed_cells: u64,
    direct_utf8_owned_cells: u64,
    direct_utf8_owned_interned_hits: u64,
    direct_utf8_owned_seen_once_promotions: u64,
    fallback_cells: u64,
}

#[derive(Debug)]
struct FileSummary {
    relative: String,
    size_bytes: u64,
    row_count: u64,
    column_count: usize,
    routed_cells: u64,
    fallback_cells: u64,
    direct_utf8_owned_cells: u64,
    direct_utf8_single_byte_cells: u64,
}

fn pct(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        (part as f64 * 100.0) / total as f64
    }
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
    files.retain(|path| fs::metadata(path).map_or(false, |meta| meta.len() >= min_size_bytes));
    files
}

fn route_total(stats: &ScanStats) -> u64 {
    stats
        .batch_staged_numeric_cells
        .saturating_add(stats.batch_direct_numeric_cells)
        .saturating_add(stats.batch_direct_raw_bytes_cells)
        .saturating_add(stats.batch_direct_utf8_single_byte_cells)
        .saturating_add(stats.batch_direct_utf8_borrowed_cells)
        .saturating_add(stats.batch_direct_utf8_owned_cells)
        .saturating_add(stats.batch_fallback_cells)
}

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
            .visit_batches(|_| Ok(ControlFlow::Continue(())))
        else {
            continue;
        };

        let routed = route_total(&stats);
        let relative = path
            .strip_prefix(&fixtures_root)
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|_| path.display().to_string());
        let size_bytes = fs::metadata(&path).map_or(0, |meta| meta.len());

        file_summaries.push(FileSummary {
            relative,
            size_bytes,
            row_count: dataset.metadata().row_count,
            column_count: dataset.columns().len(),
            routed_cells: routed,
            fallback_cells: stats.batch_fallback_cells,
            direct_utf8_owned_cells: stats.batch_direct_utf8_owned_cells,
            direct_utf8_single_byte_cells: stats.batch_direct_utf8_single_byte_cells,
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
            .saturating_add(stats.decode_batches);
        aggregated.routed_cells = aggregated.routed_cells.saturating_add(routed);
        aggregated.staged_numeric_cells = aggregated
            .staged_numeric_cells
            .saturating_add(stats.batch_staged_numeric_cells);
        aggregated.direct_numeric_cells = aggregated
            .direct_numeric_cells
            .saturating_add(stats.batch_direct_numeric_cells);
        aggregated.direct_raw_bytes_cells = aggregated
            .direct_raw_bytes_cells
            .saturating_add(stats.batch_direct_raw_bytes_cells);
        aggregated.direct_utf8_single_byte_cells = aggregated
            .direct_utf8_single_byte_cells
            .saturating_add(stats.batch_direct_utf8_single_byte_cells);
        aggregated.direct_utf8_borrowed_cells = aggregated
            .direct_utf8_borrowed_cells
            .saturating_add(stats.batch_direct_utf8_borrowed_cells);
        aggregated.direct_utf8_owned_cells = aggregated
            .direct_utf8_owned_cells
            .saturating_add(stats.batch_direct_utf8_owned_cells);
        aggregated.direct_utf8_owned_interned_hits = aggregated
            .direct_utf8_owned_interned_hits
            .saturating_add(stats.batch_direct_utf8_owned_interned_hits);
        aggregated.direct_utf8_owned_seen_once_promotions = aggregated
            .direct_utf8_owned_seen_once_promotions
            .saturating_add(stats.batch_direct_utf8_owned_seen_once_promotions);
        aggregated.fallback_cells = aggregated
            .fallback_cells
            .saturating_add(stats.batch_fallback_cells);
    }

    println!("files_scanned={}", aggregated.files_scanned);
    println!("batch_rows={batch_rows}");
    println!("total_rows={}", aggregated.total_rows);
    println!("decode_batches={}", aggregated.decode_batches);
    println!("total_routed_cells={}", aggregated.routed_cells);
    println!(
        "staged_numeric_cells={} ({:.2}%)",
        aggregated.staged_numeric_cells,
        pct(aggregated.staged_numeric_cells, aggregated.routed_cells)
    );
    println!(
        "direct_numeric_cells={} ({:.2}%)",
        aggregated.direct_numeric_cells,
        pct(aggregated.direct_numeric_cells, aggregated.routed_cells)
    );
    println!(
        "direct_raw_bytes_cells={} ({:.2}%)",
        aggregated.direct_raw_bytes_cells,
        pct(aggregated.direct_raw_bytes_cells, aggregated.routed_cells)
    );
    println!(
        "direct_utf8_single_byte_cells={} ({:.2}%)",
        aggregated.direct_utf8_single_byte_cells,
        pct(
            aggregated.direct_utf8_single_byte_cells,
            aggregated.routed_cells
        )
    );
    println!(
        "direct_utf8_borrowed_cells={} ({:.2}%)",
        aggregated.direct_utf8_borrowed_cells,
        pct(
            aggregated.direct_utf8_borrowed_cells,
            aggregated.routed_cells
        )
    );
    println!(
        "direct_utf8_owned_cells={} ({:.2}%)",
        aggregated.direct_utf8_owned_cells,
        pct(aggregated.direct_utf8_owned_cells, aggregated.routed_cells)
    );
    println!(
        "direct_utf8_owned_interned_hits={}",
        aggregated.direct_utf8_owned_interned_hits
    );
    println!(
        "direct_utf8_owned_seen_once_promotions={}",
        aggregated.direct_utf8_owned_seen_once_promotions
    );
    println!(
        "fallback_cells={} ({:.2}%)",
        aggregated.fallback_cells,
        pct(aggregated.fallback_cells, aggregated.routed_cells)
    );

    file_summaries.sort_by(|left, right| {
        right
            .fallback_cells
            .cmp(&left.fallback_cells)
            .then_with(|| {
                right
                    .direct_utf8_owned_cells
                    .cmp(&left.direct_utf8_owned_cells)
            })
            .then_with(|| right.routed_cells.cmp(&left.routed_cells))
    });

    println!("\nTop files by fallback/owned-utf8 pressure:");
    for summary in file_summaries.iter().take(print_top) {
        println!(
            "{} | size_mb={:.2} rows={} cols={} fallback={} ({:.2}%) utf8_owned={} ({:.2}%) utf8_single_byte={} ({:.2}%)",
            summary.relative,
            summary.size_bytes as f64 / (1024.0 * 1024.0),
            summary.row_count,
            summary.column_count,
            summary.fallback_cells,
            pct(summary.fallback_cells, summary.routed_cells),
            summary.direct_utf8_owned_cells,
            pct(summary.direct_utf8_owned_cells, summary.routed_cells),
            summary.direct_utf8_single_byte_cells,
            pct(summary.direct_utf8_single_byte_cells, summary.routed_cells),
        );
    }
}
