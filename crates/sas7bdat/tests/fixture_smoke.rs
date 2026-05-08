#![allow(
    clippy::float_cmp,
    clippy::needless_pass_by_value,
    clippy::unreadable_literal
)]

use sas7bdat::{BatchHint, Dataset, TrustedOffsets, discover_fixture_paths};
use std::{
    env,
    ops::ControlFlow,
    path::{Path, PathBuf},
};

const EXCLUDED_FIXTURE_NAMES: &[&str] = &["corrupt.sas7bdat", "zero_variables.sas7bdat"];

#[allow(dead_code)]
fn assert_trusted_offsets(offsets: &TrustedOffsets, expected: &[i64], data_len: usize) {
    assert_eq!(offsets.as_slice(), expected);
    offsets
        .validate_for_values_len(data_len)
        .expect("fixture scan should emit valid trusted offsets");
}

fn excluded_fixture(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| EXCLUDED_FIXTURE_NAMES.contains(&name))
}

#[test]
fn local_fixtures_open_and_scan() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures");
    let mut fixtures = discover_fixture_paths(std::slice::from_ref(&root)).unwrap_or_default();
    fixtures.retain(|path| !excluded_fixture(path));
    fixtures.sort();

    if fixtures.is_empty() {
        eprintln!(
            "skipping local fixture smoke test: no .sas7bdat files found under {}",
            root.display()
        );
        return;
    }

    let max_files = env::var("SAS7BDAT_FIXTURE_LIMIT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(8);

    let mut exercised = 0usize;
    for path in fixtures.into_iter().take(max_files) {
        exercised += 1;
        let dataset = Dataset::open(&path)
            .unwrap_or_else(|err| panic!("failed to open fixture {}: {err}", path.display()));

        assert!(dataset.metadata().page_size > 0, "{}", path.display());
        assert!(dataset.metadata().page_count > 0, "{}", path.display());
        assert!(!dataset.columns().is_empty(), "{}", path.display());

        dataset
            .scan()
            .with_batch_hint(BatchHint::Rows(256))
            .visit_batches(|batch| {
                assert!(batch.row_count > 0);
                assert_eq!(batch.columns.len(), dataset.columns().len());
                Ok(ControlFlow::Continue(()))
            })
            .unwrap_or_else(|err| panic!("failed to scan fixture {}: {err}", path.display()));
    }

    assert!(exercised > 0);
}

#[test]
fn local_compressed_fixtures_open_and_scan() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures");
    let mut fixtures = discover_fixture_paths(std::slice::from_ref(&root)).unwrap_or_default();
    fixtures.retain(|path| !excluded_fixture(path));
    fixtures.sort();

    if fixtures.is_empty() {
        eprintln!(
            "skipping local compressed fixture smoke test: no .sas7bdat files found under {}",
            root.display()
        );
        return;
    }

    let max_files = env::var("SAS7BDAT_COMPRESSED_FIXTURE_LIMIT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(64);

    let mut exercised = 0usize;
    for path in fixtures {
        let dataset = Dataset::open(&path)
            .unwrap_or_else(|err| panic!("failed to open fixture {}: {err}", path.display()));

        if dataset.metadata().compression == sas7bdat::CompressionKind::None {
            continue;
        }

        exercised += 1;
        if exercised > max_files {
            break;
        }

        dataset
            .scan()
            .with_batch_hint(BatchHint::Rows(1024))
            .visit_batches(|batch| {
                assert!(batch.row_count > 0);
                Ok(ControlFlow::Continue(()))
            })
            .unwrap_or_else(|err| {
                panic!(
                    "failed to scan compressed fixture {}: {err}",
                    path.display()
                )
            });
    }

    assert!(exercised > 0);
}
