use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use sas7bdat_simd::{BatchHint, Dataset, discover_fixture_paths};
use std::{fs, hint::black_box, time::Duration};

mod common;
use common::{bench_raw_rows, discover_target_roots, fixture_path, load_dataset};

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

fn bench_case(c: &mut Criterion, name: &str, dataset: &Dataset) {
    let mut group = c.benchmark_group(name);

    bench_raw_rows(&mut group, dataset, BenchmarkId::new("raw_rows", "all"));

    group.measurement_time(Duration::from_secs(TYPED_BATCHES_MEASUREMENT_SECONDS));
    for &batch_rows in DEFAULT_TYPED_BATCH_ROWS {
        group.bench_with_input(
            BenchmarkId::new("typed_batches", batch_rows),
            &batch_rows,
            |b, &rows| {
                b.iter(|| {
                    let stats = dataset
                        .scan()
                        .with_batch_hint(BatchHint::Rows(rows))
                        .visit_batches(|batch| {
                            black_box(batch.row_count);
                            Ok(std::ops::ControlFlow::Continue(()))
                        })
                        .expect("compressed typed batch scan");
                    black_box(stats.decode_batches);
                });
            },
        );
    }

    group.finish();
}

fn target_bench_name(relative: &str, dataset: &Dataset) -> String {
    let column_count = dataset.columns().len();
    let row_count = dataset.metadata().row_count;
    format!("target/{relative}/{column_count}cols/{row_count}rows")
}

fn discover_target_paths(min_size_bytes: u64) -> Vec<(String, u64)> {
    let fixtures_root = fixture_path("");
    let target_roots = discover_target_roots(&fixtures_root);

    let Ok(files) = discover_fixture_paths(&target_roots) else {
        return Vec::new();
    };

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
