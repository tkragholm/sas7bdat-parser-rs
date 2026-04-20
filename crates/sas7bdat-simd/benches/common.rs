#![allow(dead_code)]

use criterion::{BenchmarkGroup, BenchmarkId, Throughput, measurement::Measurement};
use sas7bdat_simd::{Dataset, IoBackendPreference, OpenOptions};
use std::{
    fs,
    hint::black_box,
    path::{Path, PathBuf},
};

#[must_use]
pub fn fixture_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(relative)
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
    match io_backend {
        IoBackendPreference::Auto => "auto",
        IoBackendPreference::MmapPreferred => "mmap_preferred",
        IoBackendPreference::BufferedPreferred => "buffered_preferred",
        IoBackendPreference::BufferedOnly => "buffered_only",
    }
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
