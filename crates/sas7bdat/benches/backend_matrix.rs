#![allow(clippy::needless_pass_by_value)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use sas7bdat::{BatchHint, Dataset, IoBackendPreference, Projection};
use std::hint::black_box;

mod common;
use common::{backend_label, bench_raw_rows, open_dataset};

fn bench_string_batches(
    c: &mut Criterion,
    name: &str,
    relative: &str,
    columns: &[&str],
    io_backends: &[IoBackendPreference],
) {
    let Some(reference) = open_dataset(relative, IoBackendPreference::MmapPreferred) else {
        return;
    };
    let row_count = reference.metadata().row_count;
    drop(reference);

    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(row_count));

    for &io_backend in io_backends {
        let Some(dataset) = open_dataset(relative, io_backend) else {
            continue;
        };
        let Ok(projection) = dataset
            .projection()
            .columns(columns.iter().copied())
            .build()
        else {
            continue;
        };
        bench_projected_batches(
            &mut group,
            &dataset,
            projection,
            BenchmarkId::new("typed_batches", backend_label(io_backend)),
        );
    }

    group.finish();
}

fn bench_numeric_batches(
    c: &mut Criterion,
    name: &str,
    relative: &str,
    columns: &[&str],
    io_backends: &[IoBackendPreference],
) {
    let Some(reference) = open_dataset(relative, IoBackendPreference::MmapPreferred) else {
        return;
    };
    let row_count = reference.metadata().row_count;
    drop(reference);

    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(row_count));

    for &io_backend in io_backends {
        let Some(dataset) = open_dataset(relative, io_backend) else {
            continue;
        };
        let Ok(projection) = dataset
            .projection()
            .columns(columns.iter().copied())
            .build()
        else {
            continue;
        };
        bench_projected_batches(
            &mut group,
            &dataset,
            projection,
            BenchmarkId::new("typed_batches", backend_label(io_backend)),
        );
    }

    group.finish();
}

fn bench_backend_raw_rows(
    c: &mut Criterion,
    name: &str,
    relative: &str,
    io_backends: &[IoBackendPreference],
) {
    let Some(reference) = open_dataset(relative, IoBackendPreference::MmapPreferred) else {
        return;
    };
    let row_count = reference.metadata().row_count;
    drop(reference);

    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(row_count));

    for &io_backend in io_backends {
        let Some(dataset) = open_dataset(relative, io_backend) else {
            continue;
        };
        bench_raw_rows(
            &mut group,
            &dataset,
            BenchmarkId::new("raw_rows", backend_label(io_backend)),
        );
    }

    group.finish();
}

fn bench_typed_rows(
    c: &mut Criterion,
    name: &str,
    relative: &str,
    columns: &[&str],
    io_backends: &[IoBackendPreference],
) {
    let Some(reference) = open_dataset(relative, IoBackendPreference::MmapPreferred) else {
        return;
    };
    let row_count = reference.metadata().row_count;
    drop(reference);

    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(row_count));

    for &io_backend in io_backends {
        let Some(dataset) = open_dataset(relative, io_backend) else {
            continue;
        };
        let Ok(projection) = dataset
            .projection()
            .columns(columns.iter().copied())
            .build()
        else {
            continue;
        };
        group.bench_function(
            BenchmarkId::new("typed_rows", backend_label(io_backend)),
            |b| {
                b.iter(|| {
                    let rows = dataset
                        .scan()
                        .with_projection(&projection)
                        .collect_rows()
                        .expect("backend typed rows");
                    black_box(rows.len());
                });
            },
        );
    }

    group.finish();
}

fn bench_projected_batches(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    dataset: &Dataset,
    projection: Projection,
    id: BenchmarkId,
) {
    group.bench_function(id, |b| {
        b.iter(|| {
            let batches = dataset
                .scan()
                .with_projection(&projection)
                .with_batch_hint(BatchHint::Rows(256))
                .collect_batches()
                .expect("backend typed batches");
            black_box(batches.len());
        });
    });
}

fn backend_matrix(c: &mut Criterion) {
    let io_backends = [
        IoBackendPreference::MmapPreferred,
        IoBackendPreference::BufferedOnly,
    ];

    bench_backend_raw_rows(c, "backend_ahs2013n_raw", "ahs2013n.sas7bdat", &io_backends);
    bench_typed_rows(
        c,
        "backend_ahs2013n_strings_rows",
        "ahs2013n.sas7bdat",
        &["CONTROL", "JOB", "RACE", "HHCITSHP"],
        &io_backends,
    );
    bench_numeric_batches(
        c,
        "backend_ahs2013n_numeric_batches",
        "ahs2013n.sas7bdat",
        &["WEIGHT", "ZINC2", "MORTM", "AMTG1", "LOT"],
        &io_backends,
    );
    bench_string_batches(
        c,
        "backend_topical_string_batches",
        "raw_data/ahs2013/topical.sas7bdat",
        &[
            "CONTROL", "EABAN", "EBARCL", "PTBANK", "PTENTMNT", "PTGROCER",
        ],
        &io_backends,
    );
}

criterion_group!(benches, backend_matrix);
criterion_main!(benches);
