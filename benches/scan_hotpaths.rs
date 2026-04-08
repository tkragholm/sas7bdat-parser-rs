#![allow(
    clippy::map_unwrap_or,
    clippy::needless_pass_by_value,
    clippy::uninlined_format_args,
    clippy::unnecessary_debug_formatting
)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use sas7bdat_simd::{
    BatchHint, Dataset, Projection, RowSelection,
    fixture_catalog::{FixtureCatalog, FixtureStatus, ProjectionPreset, build_projection},
};
use std::{
    env, fs,
    hint::black_box,
    path::{Path, PathBuf},
};

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

fn load_dataset_path(path: &Path) -> Option<Dataset> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
    };
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
                .select(RowSelection::Range {
                    start: sas7bdat_simd::types::RowIndex(0),
                    end: sas7bdat_simd::types::RowIndex(8),
                })
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

fn bench_projected_scans(c: &mut Criterion, name: &str, dataset: Dataset, projection: Projection) {
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(dataset.metadata().row_count));

    group.bench_function(BenchmarkId::new("typed_rows", "projected"), |b| {
        b.iter(|| {
            let rows = dataset
                .scan()
                .with_projection(&projection)
                .collect_rows()
                .expect("projected typed rows");
            black_box(rows.len());
        });
    });

    group.bench_function(BenchmarkId::new("typed_batches", "projected"), |b| {
        b.iter(|| {
            let batches = dataset
                .scan()
                .with_projection(&projection)
                .with_batch_hint(BatchHint::Rows(256))
                .collect_batches()
                .expect("projected typed batches");
            black_box(batches.len());
        });
    });

    group.finish();
}

fn maybe_scan_hotpaths_from_catalog(c: &mut Criterion) -> bool {
    let Some(tags) = requested_tags() else {
        return false;
    };
    let catalog_path = env::var("BENCH_CATALOG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| fixture_path("fixture_catalog.local.json"));
    let catalog_bytes = fs::read(&catalog_path).unwrap_or_else(|err| {
        panic!(
            "BENCH_TAGS was set but catalog {:?} could not be read: {}",
            catalog_path, err
        )
    });
    let catalog: FixtureCatalog = serde_json::from_slice(&catalog_bytes).unwrap_or_else(|err| {
        panic!(
            "BENCH_TAGS was set but catalog {:?} could not be parsed: {}",
            catalog_path, err
        )
    });

    let projection = env::var("BENCH_PROJECTION")
        .ok()
        .and_then(|value| ProjectionPreset::parse(&value))
        .unwrap_or(ProjectionPreset::Full);
    let limit = env::var("BENCH_MAX_FIXTURES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(usize::MAX);

    let tag_label = tags.join("+");
    let mut matched = 0usize;
    for entry in catalog.fixtures {
        if matched >= limit {
            break;
        }
        let FixtureStatus::Profiled(profile) = entry.status else {
            continue;
        };
        if !tags
            .iter()
            .all(|tag| profile.tags.iter().any(|fixture_tag| fixture_tag == tag))
        {
            continue;
        }
        let Some(dataset) = load_dataset_path(Path::new(&entry.path)) else {
            continue;
        };
        let fixture_id = sanitize_name(&entry.file_name);
        match projection {
            ProjectionPreset::Full => {
                let name = format!("tag_{tag_label}_{fixture_id}");
                bench_dataset_scans(c, &name, dataset, true, true, true);
            }
            preset => {
                let Some(projection) = build_projection(&dataset, preset) else {
                    continue;
                };
                let name = format!("tag_{tag_label}_{fixture_id}_projected");
                bench_projected_scans(c, &name, dataset, projection);
            }
        }
        matched += 1;
    }

    assert!(
        matched > 0,
        "BENCH_TAGS={:?} matched no profiled fixtures in {:?}",
        tags,
        catalog_path
    );
    true
}

fn requested_tags() -> Option<Vec<String>> {
    let value = env::var("BENCH_TAGS").ok()?;
    let tags: Vec<String> = value
        .split(',')
        .map(str::trim)
        .filter(|tag| !tag.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    (!tags.is_empty()).then_some(tags)
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

fn scan_hotpaths(c: &mut Criterion) {
    if maybe_scan_hotpaths_from_catalog(c) {
        return;
    }

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
        if let Ok(projection) = dataset
            .projection()
            .columns(["CONTROL", "PTCOSTGAS", "SPLTWGT1"])
            .build()
        {
            bench_projected_scans(c, "fixture_topical_projection", dataset.clone(), projection);
        }
        if let Ok(projection) = dataset
            .projection()
            .columns([
                "PTCOSTGAS",
                "PTCOSTINSU",
                "PTCOSTCARP",
                "PTCOSTCARM",
                "PTCOSTPARK",
                "PTCOSTPTR",
                "SPLTWGT1",
                "SPLTWGT2",
            ])
            .build()
        {
            bench_projected_scans(
                c,
                "fixture_topical_projection_numeric",
                dataset.clone(),
                projection,
            );
        }
        if let Ok(projection) = dataset
            .projection()
            .columns([
                "CONTROL", "EABAN", "EBARCL", "PTBANK", "PTENTMNT", "PTGROCER",
            ])
            .build()
        {
            bench_projected_scans(
                c,
                "fixture_topical_projection_strings",
                dataset.clone(),
                projection,
            );
        }
        bench_dataset_scans(c, "fixture_topical", dataset, true, true, true);
    }

    if let Some(dataset) = load_dataset("raw_data/ahs2013/homimp.sas7bdat") {
        if let Ok(projection) = dataset.projection().columns(["CONTROL", "RAD"]).build() {
            bench_projected_scans(c, "fixture_homimp_projection", dataset.clone(), projection);
        }
        bench_dataset_scans(c, "fixture_homimp", dataset, true, true, true);
    }
}

criterion_group!(benches, scan_hotpaths);
criterion_main!(benches);
