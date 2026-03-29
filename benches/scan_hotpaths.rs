use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use sas7bdat_simd::{BatchHint, Dataset, RowSelection};
use std::{fs, hint::black_box, path::PathBuf};

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

fn bench_dataset_scans(
    c: &mut Criterion,
    name: &str,
    dataset: Dataset,
    bench_raw: bool,
    bench_typed: bool,
    bench_batches: bool,
) {
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(dataset.metadata().row_count));

    if bench_raw {
        group.bench_function(BenchmarkId::new("raw_rows", "all"), |b| {
            b.iter(|| {
                let stats = dataset
                    .scan()
                    .visit_raw_rows(|row| {
                        black_box(row.row_index);
                        black_box(row.bytes.len());
                        Ok(std::ops::ControlFlow::Continue(()))
                    })
                    .expect("raw scan");
                black_box(stats.rows_emitted);
            });
        });
    }

    if bench_typed {
        group.bench_function(BenchmarkId::new("typed_rows", "all"), |b| {
            b.iter(|| {
                let rows = dataset.scan().collect_rows().expect("typed rows");
                black_box(rows.len());
            });
        });
    }

    if bench_batches {
        group.bench_function(BenchmarkId::new("typed_batches", "all"), |b| {
            b.iter(|| {
                let batches = dataset
                    .scan()
                    .with_batch_hint(BatchHint::Rows(256))
                    .collect_batches()
                    .expect("typed batches");
                black_box(batches.len());
            });
        });
    }

    group.bench_function(BenchmarkId::new("raw_rows", "slice"), |b| {
        b.iter(|| {
            let stats = dataset
                .scan()
                .select(RowSelection::Range { start: 0, end: 8 })
                .visit_raw_rows(|row| {
                    black_box(row.row_index);
                    black_box(row.bytes.len());
                    Ok(std::ops::ControlFlow::Continue(()))
                })
                .expect("sliced raw scan");
            black_box(stats.rows_emitted);
        });
    });

    group.finish();
}

fn scan_hotpaths(c: &mut Criterion) {
    if let Some(dataset) = load_dataset("raw_data/csharp/charset_utf8.sas7bdat") {
        bench_dataset_scans(c, "fixture_charset_utf8", dataset, true, true, true);
    }

    if let Some(dataset) = load_dataset("raw_data/csharp/54-class.sas7bdat") {
        bench_dataset_scans(c, "fixture_54_class", dataset, true, true, true);
    }

    if let Some(dataset) = load_dataset("raw_data/pandas/test2.sas7bdat") {
        bench_dataset_scans(c, "fixture_test2", dataset, true, true, true);
    }

    if let Some(dataset) = load_dataset("raw_data/pandas/max_sas_date.sas7bdat") {
        bench_dataset_scans(c, "fixture_max_sas_date", dataset, true, true, true);
    }

    if let Some(dataset) = load_dataset("raw_data/ahs2013/topical.sas7bdat") {
        bench_dataset_scans(c, "fixture_topical", dataset, true, true, true);
    }

    if let Some(dataset) = load_dataset("raw_data/ahs2013/homimp.sas7bdat") {
        bench_dataset_scans(c, "fixture_homimp", dataset, true, true, true);
    }
}

criterion_group!(benches, scan_hotpaths);
criterion_main!(benches);
