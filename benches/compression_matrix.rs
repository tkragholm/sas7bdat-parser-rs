use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use sas7bdat_simd::{BatchHint, Dataset};
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

    group.bench_function(BenchmarkId::new("typed_rows", "all"), |b| {
        b.iter(|| {
            let rows = dataset
                .scan()
                .collect_rows()
                .expect("compressed typed rows");
            black_box(rows.len());
        });
    });

    group.bench_function(BenchmarkId::new("typed_batches", "all"), |b| {
        b.iter(|| {
            let batches = dataset
                .scan()
                .with_batch_hint(BatchHint::Rows(256))
                .collect_batches()
                .expect("compressed typed batches");
            black_box(batches.len());
        });
    });

    group.finish();
}

fn compression_matrix(c: &mut Criterion) {
    if let Some(dataset) = load_dataset("raw_data/csharp/54-class.sas7bdat") {
        bench_case(c, "compressed_narrow_mixed_54_class", &dataset);
    }

    if let Some(dataset) = load_dataset("raw_data/pandas/max_sas_date.sas7bdat") {
        bench_case(c, "compressed_narrow_temporal_max_sas_date", &dataset);
    }

    if let Some(dataset) = load_dataset("raw_data/pandas/test2.sas7bdat") {
        bench_case(c, "compressed_wide_mixed_test2", &dataset);
    }

    if let Some(dataset) = load_dataset("raw_data/ahs2013/topical.sas7bdat") {
        bench_case(c, "compressed_wide_string_topical", &dataset);
    }
}

criterion_group!(benches, compression_matrix);
criterion_main!(benches);
