#[cfg(feature = "hotpath-profile")]
use std::{env, fs, path::Path};

#[cfg(feature = "hotpath-profile")]
use std::{hint::black_box, ops::ControlFlow};

#[cfg(feature = "hotpath-profile")]
use sas7bdat_simd::{BatchHint, Dataset};

#[path = "common/mod.rs"]
mod common;
#[cfg(feature = "hotpath-profile")]
use common::{discover_target_paths, fixture_root};

#[allow(dead_code)]
const TARGET_MIN_SIZE_BYTES: u64 = 10 * 1024 * 1024;
#[allow(dead_code)]
const DEFAULT_OUTPUT_PATH: &str = "target/criterion/hotpath/typed_batches_target.json";

#[cfg(feature = "hotpath-profile")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use hotpath::{Format, HotpathGuardBuilder, Section};

    let batch_rows = env::var("BATCH_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(256);
    let max_files = env::var("MAX_FILES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(usize::MAX);
    let output_path =
        env::var("HOTPATH_OUTPUT_PATH").unwrap_or_else(|_| DEFAULT_OUTPUT_PATH.to_string());

    if let Some(parent) = Path::new(&output_path).parent() {
        fs::create_dir_all(parent)?;
    }

    let _guard = HotpathGuardBuilder::new("hotpath_typed_batches_target")
        .format(Format::JsonPretty)
        .output_path(&output_path)
        .functions_limit(200)
        .threads_limit(20)
        .sections(vec![Section::FunctionsTiming, Section::Threads])
        .build();

    let fixtures_root = fixture_root();
    let mut targets = discover_target_paths(TARGET_MIN_SIZE_BYTES);
    targets.truncate(max_files);

    println!("profiling_files={}", targets.len());
    println!("batch_rows={batch_rows}");
    println!("hotpath_output={output_path}");

    let mut files_scanned = 0usize;
    let mut total_rows = 0u64;
    let mut total_batches = 0u64;
    let mut total_cells = 0u64;

    for path in targets {
        let relative = path.strip_prefix(&fixtures_root).map_or_else(
            |_| path.display().to_string(),
            |p| p.to_string_lossy().to_string(),
        );

        let Ok(dataset) = Dataset::open(&path) else {
            eprintln!("skip_open_failed={relative}");
            continue;
        };

        let mut file_batches = 0u64;
        let Ok(stats) = dataset
            .scan()
            .with_batch_hint(BatchHint::Rows(batch_rows))
            .visit_batches(|batch| {
                file_batches = file_batches.saturating_add(1);
                total_cells = total_cells.saturating_add(
                    u64::try_from(batch.row_count)
                        .unwrap_or(0)
                        .saturating_mul(u64::try_from(batch.columns.len()).unwrap_or(0)),
                );
                black_box(batch.row_base);
                black_box(batch.row_count);
                black_box(batch.columns.len());
                Ok(ControlFlow::Continue(()))
            })
        else {
            eprintln!("skip_scan_failed={relative}");
            continue;
        };

        files_scanned += 1;
        total_rows = total_rows.saturating_add(dataset.metadata().row_count);
        total_batches = total_batches.saturating_add(file_batches.max(stats.decode_batches));

        println!(
            "scanned={relative} rows={} cols={} decode_batches={}",
            dataset.metadata().row_count,
            dataset.columns().len(),
            stats.decode_batches
        );
    }

    println!("files_scanned={files_scanned}");
    println!("total_rows={total_rows}");
    println!("total_batches={total_batches}");
    println!("total_cells={total_cells}");
    println!("hotpath_report_written={output_path}");

    Ok(())
}

#[cfg(not(feature = "hotpath-profile"))]
fn main() {
    eprintln!(
        "This example requires --features hotpath-profile.\n\
         Example: cargo run --release --features hotpath-profile --example hotpath_typed_batches_target"
    );
    std::process::exit(1);
}
