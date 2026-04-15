use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use sas7bdat_simd::{BatchHint, Dataset};
use std::{
    fs,
    hint::black_box,
    path::{Path, PathBuf},
    time::Duration,
};

const TARGET_MIN_SIZE_BYTES: u64 = 10 * 1024 * 1024;
const TYPED_BATCHES_MEASUREMENT_SECONDS: u64 = 30;
const DEFAULT_TYPED_BATCH_ROWS: &[usize] = &[256];

const NON_TARGET_FIXTURES: &[(&str, &str)] = &[
    (
        "compressed_narrow_mixed_54_class",
        "raw_data/csharp/54-class.sas7bdat",
    ),
    (
        "compressed_narrow_temporal_max_sas_date",
        "raw_data/pandas/max_sas_date.sas7bdat",
    ),
    (
        "compressed_wide_mixed_test2",
        "raw_data/pandas/test2.sas7bdat",
    ),
    (
        "compressed_wide_string_topical",
        "raw_data/ahs2013/topical.sas7bdat",
    ),
    (
        "windows1252_local_nls",
        "raw_data/principlesofeco/nls.sas7bdat",
    ),
    (
        "windows1252_local_ces",
        "raw_data/principlesofeco/ces.sas7bdat",
    ),
    (
        "windows1252_local_nels",
        "raw_data/principlesofeco/nels.sas7bdat",
    ),
    (
        "windows1252_local_figurec_3",
        "raw_data/principlesofeco/figurec_3.sas7bdat",
    ),
    (
        "windows1252_local_nls_panel",
        "raw_data/principlesofeco/nls_panel.sas7bdat",
    ),
    (
        "windows1252_local_crime",
        "raw_data/principlesofeco/crime.sas7bdat",
    ),
    (
        "windows1252_local_test_meta2_page",
        "raw_data/pandas/test_meta2_page.sas7bdat",
    ),
];

fn fixture_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(relative)
}

fn load_dataset(relative: &str) -> Option<Dataset> {
    let path = fixture_path(relative);
    let bytes = fs::read(path).ok()?;
    Dataset::from_bytes(bytes).ok()
}

fn bench_case(c: &mut Criterion, name: &str, dataset: &Dataset) {
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(dataset.metadata().row_count));

    group.bench_function(BenchmarkId::new("raw_rows", "all"), |b| {
        b.iter(|| {
            let stats = dataset
                .scan()
                .visit_raw_rows(|row| {
                    black_box(row.row_index);
                    black_box(row.bytes.len());
                    Ok(std::ops::ControlFlow::Continue(()))
                })
                .expect("compressed raw scan");
            black_box(stats.rows_emitted);
        });
    });

    group.measurement_time(Duration::from_secs(TYPED_BATCHES_MEASUREMENT_SECONDS));
    for &batch_rows in DEFAULT_TYPED_BATCH_ROWS {
        group.bench_with_input(
            BenchmarkId::new("typed_batches", batch_rows),
            &batch_rows,
            |b, &rows| {
                b.iter(|| {
                    let batches = dataset
                        .scan()
                        .with_batch_hint(BatchHint::Rows(rows))
                        .collect_batches()
                        .expect("compressed typed batches");
                    black_box(batches.len());
                });
            },
        );
    }

    group.finish();
}

fn sanitize_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    while out.contains("__") {
        out = out.replace("__", "_");
    }
    out.trim_matches('_').to_owned()
}

const fn fnv1a32(bytes: &[u8]) -> u32 {
    let mut hash = 0x811c_9dc5_u32;
    let mut i = 0usize;
    while i < bytes.len() {
        hash ^= bytes[i] as u32;
        hash = hash.wrapping_mul(0x0100_0193);
        i += 1;
    }
    hash
}

fn target_bench_name(relative: &str, dataset: &Dataset) -> String {
    let mut parts = relative.split('/');
    let source = parts.next().unwrap_or("source");
    let file_stem = Path::new(relative)
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or(relative);
    let source = sanitize_name(source);
    let file_stem = sanitize_name(file_stem);
    let encoding = sanitize_name(dataset.metadata().encoding.as_deref().unwrap_or("unknown"));
    let hash = fnv1a32(relative.as_bytes());
    format!("target/{source}/{encoding}/{file_stem}_{hash:08x}")
}

fn discover_target_roots(fixtures_root: &Path) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    let Ok(entries) = fs::read_dir(fixtures_root) else {
        return roots;
    };
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
    roots.sort();
    roots
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

fn discover_target_paths(min_size_bytes: u64) -> Vec<(String, u64)> {
    let fixtures_root = fixture_path("");
    let target_roots = discover_target_roots(&fixtures_root);
    let mut files = Vec::new();

    for root in target_roots {
        collect_sas7bdat_files(&root, &mut files);
    }

    files.sort();

    let mut out = Vec::new();
    for path in files {
        let Ok(meta) = fs::metadata(&path) else {
            continue;
        };
        if meta.len() < min_size_bytes {
            continue;
        }
        let Ok(relative) = path.strip_prefix(&fixtures_root) else {
            continue;
        };
        let relative = relative.to_string_lossy().replace('\\', "/");
        out.push((relative, meta.len()));
    }
    out
}

fn compression_matrix_target(c: &mut Criterion) {
    for (relative, _) in discover_target_paths(TARGET_MIN_SIZE_BYTES) {
        if let Some(dataset) = load_dataset(&relative) {
            let name = target_bench_name(&relative, &dataset);
            bench_case(c, &name, &dataset);
        }
    }
}

fn compression_matrix_top3_target(c: &mut Criterion) {
    let mut targets = discover_target_paths(TARGET_MIN_SIZE_BYTES);
    targets.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    for (relative, _) in targets.into_iter().take(3) {
        if let Some(dataset) = load_dataset(&relative) {
            let base = target_bench_name(&relative, &dataset);
            let scoped = base.replacen("target/", "top3_target/", 1);
            bench_case(c, &scoped, &dataset);
        }
    }
}

fn compression_matrix_non_target(c: &mut Criterion) {
    for (name, relative) in NON_TARGET_FIXTURES {
        if let Some(dataset) = load_dataset(relative) {
            let scoped_name = format!("baseline/{name}");
            bench_case(c, &scoped_name, &dataset);
        }
    }
}

criterion_group!(benches_target, compression_matrix_target);
criterion_group!(benches_top3_target, compression_matrix_top3_target);
criterion_group!(benches_non_target, compression_matrix_non_target);
criterion_main!(benches_target, benches_top3_target, benches_non_target);
