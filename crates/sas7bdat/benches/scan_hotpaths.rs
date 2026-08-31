#![allow(
    clippy::map_unwrap_or,
    clippy::needless_pass_by_value,
    clippy::uninlined_format_args,
    clippy::unnecessary_debug_formatting
)]

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use sas7bdat::{
    BatchHint, Dataset, FixtureCatalog, FixtureStatus, Parallelism, Projection, ProjectionPreset,
    RowSelection, ScanEntry, build_projection,
};
use std::{
    env, fs,
    hint::black_box,
    path::{Path, PathBuf},
};

mod common;
use common::{
    bench_raw_rows, bench_visit_rows, fixture_path, labelled_id, labelled_id_with, load_dataset,
    workspace_root,
};

const BATCH_ROWS_SMALL: usize = 256;
const BATCH_ROWS_MEDIUM: usize = 4096;
const BATCH_ROWS_LARGE: usize = 16384;
const PARALLEL_THREADS: usize = 4;

/// Load a dataset named by a catalog entry.
///
/// `sas7bdat-corpus-catalog` records paths relative to the directory it was run from —
/// the workspace root in practice (`fixtures/raw_data/...`), so that is what a relative
/// entry resolves against. Resolving against `CARGO_MANIFEST_DIR` instead pointed every
/// entry at a nonexistent `crates/sas7bdat/fixtures/...`; each load then failed silently
/// and the run ended in the "matched no profiled fixtures" assert.
fn load_dataset_path(path: &Path) -> Option<Dataset> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        workspace_root().join(path)
    };
    let bytes = fs::read(path).ok()?;
    Dataset::from_bytes(bytes).ok()
}

#[allow(clippy::too_many_lines)]
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
        bench_raw_rows(
            &mut group,
            &dataset,
            labelled_id("raw_rows", &dataset, ScanEntry::RawRows, "all"),
        );
    }

    if bench_typed {
        // `collect_rows` materialises every row up front.
        group.bench_function(
            labelled_id("typed_rows_collect", &dataset, ScanEntry::Rows, "all"),
            |b| {
                b.iter(|| {
                    let rows = dataset.scan().collect_rows().expect("typed rows");
                    black_box(rows.len());
                });
            },
        );
        // `visit_rows` streams them, and is what `sas7bdat-convert` calls, so it is the path
        // behind the CLI and the R package. It had no coverage here at all before.
        bench_visit_rows(
            &mut group,
            &dataset,
            labelled_id("typed_rows_visit", &dataset, ScanEntry::Rows, "all"),
        );
    }

    if bench_batches {
        group.bench_function(
            labelled_id(
                "typed_batches_collect",
                &dataset,
                ScanEntry::Batches,
                BATCH_ROWS_SMALL,
            ),
            |b| {
                b.iter(|| {
                    let batches = dataset
                        .scan()
                        .with_batch_hint(BatchHint::Rows(BATCH_ROWS_SMALL))
                        .collect_batches()
                        .expect("typed batches");
                    black_box(batches.len());
                });
            },
        );

        group.bench_function(
            labelled_id_with(
                "typed_batches_collect_threads_4",
                &dataset,
                ScanEntry::Batches,
                BATCH_ROWS_SMALL,
                |scan| scan.with_parallelism(Parallelism::Threads(4)),
            ),
            |b| {
                b.iter(|| {
                    let batches = dataset
                        .scan()
                        .with_parallelism(Parallelism::Threads(PARALLEL_THREADS))
                        .with_batch_hint(BatchHint::Rows(BATCH_ROWS_SMALL))
                        .collect_batches()
                        .expect("typed batches");
                    black_box(batches.len());
                });
            },
        );

        for batch_rows in [BATCH_ROWS_SMALL, BATCH_ROWS_MEDIUM, BATCH_ROWS_LARGE] {
            group.bench_function(
                labelled_id(
                    "typed_batches_visit",
                    &dataset,
                    ScanEntry::BorrowedBatches,
                    batch_rows,
                ),
                |b| {
                    b.iter(|| {
                        let stats = dataset
                            .scan()
                            .with_batch_hint(BatchHint::Rows(batch_rows))
                            .visit_batches(|batch| {
                                black_box(batch.row_base);
                                black_box(batch.row_count);
                                black_box(batch.columns.len());
                                Ok(std::ops::ControlFlow::Continue(()))
                            })
                            .expect("typed batch visit");
                        black_box(stats.decode_batches);
                    });
                },
            );

            group.bench_function(
                labelled_id_with(
                    "typed_batches_visit_threads_4",
                    &dataset,
                    ScanEntry::BorrowedBatches,
                    batch_rows,
                    |scan| scan.with_parallelism(Parallelism::Threads(4)),
                ),
                |b| {
                    b.iter(|| {
                        let stats = dataset
                            .scan()
                            .with_parallelism(Parallelism::Threads(PARALLEL_THREADS))
                            .with_batch_hint(BatchHint::Rows(batch_rows))
                            .visit_batches(|batch| {
                                black_box(batch.row_base);
                                black_box(batch.row_count);
                                black_box(batch.columns.len());
                                Ok(std::ops::ControlFlow::Continue(()))
                            })
                            .expect("typed batch visit");
                        black_box(stats.decode_batches);
                    });
                },
            );
        }
    }

    group.bench_function(
        labelled_id("raw_rows", &dataset, ScanEntry::RawRows, "slice"),
        |b| {
            b.iter(|| {
                let stats = dataset
                    .scan()
                    .select(RowSelection::Range {
                        start: sas7bdat::RowIndex(0),
                        end: sas7bdat::RowIndex(8),
                    })
                    .visit_raw_rows(|row| {
                        black_box(row.row_index);
                        black_box(row.bytes.len());
                        Ok(std::ops::ControlFlow::Continue(()))
                    })
                    .expect("sliced raw scan");
                black_box(stats.rows_emitted);
            });
        },
    );

    group.finish();
}

#[allow(clippy::too_many_lines)]
fn bench_projected_scans(c: &mut Criterion, name: &str, dataset: Dataset, projection: Projection) {
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(dataset.metadata().row_count));

    group.bench_function(
        labelled_id("typed_rows", &dataset, ScanEntry::Rows, "projected"),
        |b| {
            b.iter(|| {
                let rows = dataset
                    .scan()
                    .with_projection(&projection)
                    .collect_rows()
                    .expect("projected typed rows");
                black_box(rows.len());
            });
        },
    );

    group.bench_function(
        labelled_id(
            "typed_batches_collect",
            &dataset,
            ScanEntry::Batches,
            "projected_256",
        ),
        |b| {
            b.iter(|| {
                let batches = dataset
                    .scan()
                    .with_projection(&projection)
                    .with_batch_hint(BatchHint::Rows(BATCH_ROWS_SMALL))
                    .collect_batches()
                    .expect("projected typed batches");
                black_box(batches.len());
            });
        },
    );

    group.bench_function(
        labelled_id_with(
            "typed_batches_collect_threads_4",
            &dataset,
            ScanEntry::Batches,
            "projected_256",
            |scan| scan.with_parallelism(Parallelism::Threads(4)),
        ),
        |b| {
            b.iter(|| {
                let batches = dataset
                    .scan()
                    .with_projection(&projection)
                    .with_parallelism(Parallelism::Threads(PARALLEL_THREADS))
                    .with_batch_hint(BatchHint::Rows(BATCH_ROWS_SMALL))
                    .collect_batches()
                    .expect("projected typed batches");
                black_box(batches.len());
            });
        },
    );

    for batch_rows in [BATCH_ROWS_SMALL, BATCH_ROWS_MEDIUM, BATCH_ROWS_LARGE] {
        group.bench_function(
            labelled_id(
                "typed_batches_visit",
                &dataset,
                ScanEntry::BorrowedBatches,
                batch_rows,
            ),
            |b| {
                b.iter(|| {
                    let stats = dataset
                        .scan()
                        .with_projection(&projection)
                        .with_batch_hint(BatchHint::Rows(batch_rows))
                        .visit_batches(|batch| {
                            black_box(batch.row_base);
                            black_box(batch.row_count);
                            black_box(batch.columns.len());
                            Ok(std::ops::ControlFlow::Continue(()))
                        })
                        .expect("projected typed batch visit");
                    black_box(stats.decode_batches);
                });
            },
        );

        group.bench_function(
            labelled_id_with(
                "typed_batches_visit_threads_4",
                &dataset,
                ScanEntry::BorrowedBatches,
                batch_rows,
                |scan| scan.with_parallelism(Parallelism::Threads(4)),
            ),
            |b| {
                b.iter(|| {
                    let stats = dataset
                        .scan()
                        .with_projection(&projection)
                        .with_parallelism(Parallelism::Threads(PARALLEL_THREADS))
                        .with_batch_hint(BatchHint::Rows(batch_rows))
                        .visit_batches(|batch| {
                            black_box(batch.row_base);
                            black_box(batch.row_count);
                            black_box(batch.columns.len());
                            Ok(std::ops::ControlFlow::Continue(()))
                        })
                        .expect("projected typed batch visit");
                    black_box(stats.decode_batches);
                });
            },
        );
    }

    group.finish();
}

fn bench_numeric_projection(c: &mut Criterion, name: &str, dataset: Dataset) {
    let Some(projection) = build_projection(&dataset, ProjectionPreset::Numeric) else {
        return;
    };
    let projected_name = format!("{name}_numeric_only");
    bench_projected_scans(c, &projected_name, dataset, projection);
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
            "catalog {:?} could not be parsed: {}\n\
             It is generated once and reused, so it may predate the current profile schema — \
             regenerate it with `just catalog`.",
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
                bench_dataset_scans(c, &name, dataset.clone(), true, true, true);
                bench_numeric_projection(c, &name, dataset);
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
        bench_dataset_scans(c, "fixture_charset_utf8", dataset.clone(), true, true, true);
        bench_numeric_projection(c, "fixture_charset_utf8", dataset);
    }

    if let Some(dataset) = load_dataset("raw_data/csharp/54-class.sas7bdat") {
        bench_dataset_scans(c, "fixture_54_class", dataset.clone(), true, true, true);
        bench_numeric_projection(c, "fixture_54_class", dataset);
    }

    if let Some(dataset) = load_dataset("raw_data/pandas/test2.sas7bdat") {
        bench_dataset_scans(c, "fixture_test2", dataset.clone(), true, true, true);
        bench_numeric_projection(c, "fixture_test2", dataset);
    }

    if let Some(dataset) = load_dataset("raw_data/pandas/max_sas_date.sas7bdat") {
        bench_dataset_scans(c, "fixture_max_sas_date", dataset.clone(), true, true, true);
        bench_numeric_projection(c, "fixture_max_sas_date", dataset);
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
        bench_dataset_scans(c, "fixture_topical", dataset.clone(), true, true, true);
        bench_numeric_projection(c, "fixture_topical", dataset);
    }

    if let Some(dataset) = load_dataset("raw_data/ahs2013/homimp.sas7bdat") {
        if let Ok(projection) = dataset.projection().columns(["CONTROL", "RAD"]).build() {
            bench_projected_scans(c, "fixture_homimp_projection", dataset.clone(), projection);
        }
        bench_dataset_scans(c, "fixture_homimp", dataset.clone(), true, true, true);
        bench_numeric_projection(c, "fixture_homimp", dataset);
    }
}

criterion_group!(benches, scan_hotpaths);
criterion_main!(benches);
