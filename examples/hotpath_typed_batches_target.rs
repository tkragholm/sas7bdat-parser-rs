use std::{
    env, fs,
    path::{Path, PathBuf},
};

#[allow(dead_code)]
const TARGET_MIN_SIZE_BYTES: u64 = 10 * 1024 * 1024;
#[allow(dead_code)]
const DEFAULT_OUTPUT_PATH: &str = "target/criterion/hotpath/typed_batches_target.json";

#[allow(dead_code)]
fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures")
}

#[allow(dead_code)]
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

#[allow(dead_code)]
fn discover_target_paths(min_size_bytes: u64, max_files: usize) -> Vec<PathBuf> {
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
    files.truncate(max_files);
    files
}

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
        .with_functions_limit(200)
        .with_threads_limit(20)
        .with_sections(vec![Section::FunctionsTiming, Section::Threads])
        .build();

    let fixtures_root = fixture_root();
    let targets = discover_target_paths(TARGET_MIN_SIZE_BYTES, max_files);
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
