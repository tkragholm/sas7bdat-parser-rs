#![allow(dead_code)]

use criterion::{BenchmarkGroup, BenchmarkId, Throughput, measurement::Measurement};
use sas7bdat::{Dataset, IoBackendPreference, OpenOptions, ScanEntry};
use std::{
    fs,
    hint::black_box,
    path::{Path, PathBuf},
};

/// Resolve a fixture path relative to the workspace `fixtures/` directory.
///
/// Fixtures live at the workspace root, not under this crate — joining onto
/// `CARGO_MANIFEST_DIR` alone yields `crates/sas7bdat/fixtures/...`, which does not exist,
/// and every `open_dataset` silently returns `None` so the benches quietly measure nothing.
#[must_use]
pub fn fixture_path(relative: &str) -> PathBuf {
    workspace_root().join("fixtures").join(relative)
}

#[must_use]
pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
}

#[must_use]
pub fn open_dataset(relative: &str, io_backend: IoBackendPreference) -> Option<Dataset> {
    let path = fixture_path(relative);
    Dataset::open_with(&path, OpenOptions::builder().io_backend(io_backend).build()).ok()
}

#[must_use]
pub fn load_dataset(relative: &str) -> Option<Dataset> {
    let path = fixture_path(relative);
    let bytes = fs::read(path).ok()?;
    Dataset::from_bytes(bytes).ok()
}

#[must_use]
pub const fn backend_label(io_backend: IoBackendPreference) -> &'static str {
    io_backend.as_str()
}

#[must_use]
pub fn discover_target_roots(fixtures_root: &Path) -> Vec<PathBuf> {
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

/// # Panics
///
/// Panics if the benchmark scan itself fails.
/// The decode pipeline `entry` will take on `dataset`, as a short label.
///
/// **Put this in the benchmark id.** This crate has several decode pipelines and they share
/// almost every symbol below the point where they diverge, so a benchmark cannot otherwise
/// tell you which one it measured. Two consequences, both of which have already happened
/// here:
///
/// - A change to the tiled column-major fill measured as zero, because the benchmark ran
///   `visit_batches`, which never reaches that fill.
/// - Two of the entry points this suite exercises most, `visit_batches` and
///   `collect_batches`, do not carry the same production consumers, so a number from one
///   says nothing about the other.
///
/// Carrying the path in the id also makes a *change* of path loud: the benchmark is renamed,
/// so Criterion reports it as new rather than silently comparing against a baseline measured
/// on different code.
///
/// `configure` must apply **the same builder settings the benchmark itself uses**. The path
/// depends on them: parallelism, the row window and the column-major flag all move it, so a
/// label taken from a default builder can name a pipeline the benchmark never runs.
#[must_use]
pub fn path_label(
    dataset: &Dataset,
    entry: ScanEntry,
    configure: impl for<'a> Fn(sas7bdat::ScanBuilder<'a>) -> sas7bdat::ScanBuilder<'a>,
) -> String {
    configure(dataset.scan())
        .predict_path(entry)
        .map_or_else(|_| "unknown".to_owned(), |path| path.as_str().to_owned())
}

/// A benchmark id carrying the pipeline it measures: `name[parallel-descriptors]/param`.
///
/// For a benchmark that scans with default settings. Use [`labelled_id_with`] when it does
/// not, or the label will describe a different scan from the one being timed.
#[must_use]
pub fn labelled_id(
    name: &str,
    dataset: &Dataset,
    entry: ScanEntry,
    parameter: impl std::fmt::Display,
) -> BenchmarkId {
    labelled_id_with(name, dataset, entry, parameter, |scan| scan)
}

/// [`labelled_id`] for a benchmark that configures its scan.
#[must_use]
pub fn labelled_id_with(
    name: &str,
    dataset: &Dataset,
    entry: ScanEntry,
    parameter: impl std::fmt::Display,
    configure: impl for<'a> Fn(sas7bdat::ScanBuilder<'a>) -> sas7bdat::ScanBuilder<'a>,
) -> BenchmarkId {
    BenchmarkId::new(
        format!("{name}[{}]", path_label(dataset, entry, configure)),
        parameter,
    )
}

/// Rows through `visit_rows`, the entry `sas7bdat-convert` uses and therefore the one behind
/// the CLI and the R package. It had no benchmark coverage at all until this was added.
///
/// # Panics
///
/// Panics if the scan fails, which in a benchmark is the right response: a fixture that
/// cannot be read has nothing to measure.
pub fn bench_visit_rows<M: Measurement>(
    group: &mut BenchmarkGroup<'_, M>,
    dataset: &Dataset,
    id: BenchmarkId,
) {
    group.throughput(Throughput::Elements(dataset.metadata().row_count));
    group.bench_function(id, |b| {
        b.iter(|| {
            let stats = dataset
                .scan()
                .visit_rows(|row| {
                    black_box(row.len());
                    Ok(std::ops::ControlFlow::Continue(()))
                })
                .expect("row scan");
            black_box(stats.rows_emitted);
        });
    });
}

/// Raw row slices, no decode.
///
/// # Panics
///
/// Panics if the scan fails; a fixture that cannot be read has nothing to measure.
pub fn bench_raw_rows<M: Measurement>(
    group: &mut BenchmarkGroup<'_, M>,
    dataset: &Dataset,
    id: BenchmarkId,
) {
    group.throughput(Throughput::Elements(dataset.metadata().row_count));
    group.bench_function(id, |b| {
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
