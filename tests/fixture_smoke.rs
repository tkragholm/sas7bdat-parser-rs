#![allow(
    clippy::float_cmp,
    clippy::needless_pass_by_value,
    clippy::unreadable_literal
)]

use sas7bdat_simd::{BatchHint, Dataset, OwnedCellValue, OwnedColumnBuffer};
use std::{
    env,
    ops::ControlFlow,
    path::{Path, PathBuf},
};

const EXCLUDED_FIXTURE_NAMES: &[&str] = &["corrupt.sas7bdat", "zero_variables.sas7bdat"];

#[test]
fn local_fixtures_open_and_scan() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures");
    let mut fixtures = Vec::new();
    collect_fixture_files(&root, &mut fixtures);
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

        let mut raw_rows = 0u64;
        dataset
            .scan()
            .limit(8)
            .visit_raw_rows(|_| {
                raw_rows += 1;
                Ok(ControlFlow::Continue(()))
            })
            .unwrap_or_else(|err| panic!("raw scan failed for {}: {err}", path.display()));

        if dataset.metadata().row_count > 0 {
            assert!(raw_rows > 0, "{}", path.display());
        }

        if !dataset.columns().is_empty() {
            let rows = dataset
                .scan()
                .limit(4)
                .collect_rows()
                .unwrap_or_else(|err| {
                    panic!("typed row scan failed for {}: {err}", path.display())
                });

            let batches = dataset
                .scan()
                .limit(4)
                .with_batch_hint(BatchHint::Rows(4))
                .collect_batches()
                .unwrap_or_else(|err| panic!("batch scan failed for {}: {err}", path.display()));

            if dataset.metadata().row_count > 0 {
                assert!(!rows.is_empty(), "{}", path.display());
                assert!(!batches.is_empty(), "{}", path.display());
            }
        }
    }

    assert!(exercised > 0);
}

#[test]
fn local_compressed_fixtures_open_and_scan() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures");
    let mut fixtures = Vec::new();
    collect_fixture_files(&root, &mut fixtures);
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

        if dataset.metadata().compression == sas7bdat_simd::CompressionKind::None {
            continue;
        }

        exercised += 1;
        if exercised > max_files {
            break;
        }

        let mut raw_rows = 0u64;
        dataset
            .scan()
            .limit(4)
            .visit_raw_rows(|_| {
                raw_rows += 1;
                Ok(ControlFlow::Continue(()))
            })
            .unwrap_or_else(|err| panic!("raw scan failed for {}: {err}", path.display()));

        if dataset.metadata().row_count > 0 {
            assert!(raw_rows > 0, "{}", path.display());
        }

        let rows = dataset
            .scan()
            .limit(2)
            .collect_rows()
            .unwrap_or_else(|err| panic!("typed row scan failed for {}: {err}", path.display()));

        let batches = dataset
            .scan()
            .limit(2)
            .with_batch_hint(BatchHint::Rows(2))
            .collect_batches()
            .unwrap_or_else(|err| panic!("batch scan failed for {}: {err}", path.display()));

        if dataset.metadata().row_count > 0 {
            assert!(!rows.is_empty(), "{}", path.display());
            assert!(!batches.is_empty(), "{}", path.display());
        }
    }

    assert!(exercised > 0);
}

#[test]
fn fixture_54_class_matches_expected_shape() {
    let path = fixture_path("raw_data/csharp/54-class.sas7bdat");
    if !path.exists() {
        eprintln!("skipping fixture-specific test: {}", path.display());
        return;
    }

    let dataset = Dataset::open(&path).expect("open 54-class");
    assert_eq!(dataset.metadata().table_name.as_deref(), Some("CLASS"));
    assert_eq!(dataset.metadata().encoding.as_deref(), Some("ISO-8859-15"));
    assert_eq!(dataset.metadata().row_count, 19);
    assert_eq!(dataset.columns().len(), 5);
    assert_eq!(dataset.columns()[0].name, "Name");
    assert_eq!(dataset.columns()[4].name, "Weight");

    let rows = dataset.scan().limit(3).collect_rows().expect("rows");
    assert_eq!(rows.len(), 3);
    assert!(matches!(rows[0].cells[0], OwnedCellValue::String(ref value) if value == "Alfred"));
    assert!(matches!(rows[0].cells[2], OwnedCellValue::Int64(14)));
    assert!(matches!(rows[1].cells[3], OwnedCellValue::Float64(value) if value == 56.5));
    assert!(matches!(rows[2].cells[4], OwnedCellValue::Int64(98)));

    let batches = dataset
        .scan()
        .limit(3)
        .with_batch_hint(BatchHint::Rows(3))
        .collect_batches()
        .expect("batches");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].row_count, 3);
    match &batches[0].columns[0] {
        OwnedColumnBuffer::Utf8 {
            offsets,
            data,
            valid,
        dictionary_ids: _,
        } => {
            assert_eq!(offsets, &vec![0, 6, 11, 18]);
            assert_eq!(data, b"AlfredAliceBarbara");
            assert!(valid.is_none());
        }
        other => panic!("unexpected class name batch column: {other:?}"),
    }
}

#[test]
fn fixture_test2_matches_expected_shape() {
    let path = fixture_path("raw_data/pandas/test2.sas7bdat");
    if !path.exists() {
        eprintln!("skipping fixture-specific test: {}", path.display());
        return;
    }

    let dataset = Dataset::open(&path).expect("open test2");
    assert_eq!(dataset.metadata().table_name.as_deref(), Some("TEST2"));
    assert_eq!(dataset.metadata().encoding.as_deref(), Some("WINDOWS-1252"));
    assert_eq!(dataset.metadata().row_count, 10);
    assert_eq!(dataset.columns().len(), 100);
    assert_eq!(dataset.columns()[0].name, "Column1");
    assert_eq!(dataset.columns()[1].name, "Column2");

    let rows = dataset.scan().limit(2).collect_rows().expect("rows");
    assert_eq!(rows.len(), 2);
    assert!(matches!(rows[0].cells[0], OwnedCellValue::Float64(value) if value == 0.636));
    assert!(matches!(rows[0].cells[1], OwnedCellValue::String(ref value) if value == "pear"));
    assert!(matches!(rows[1].cells[3], OwnedCellValue::Date(_)));

    let batches = dataset
        .scan()
        .limit(2)
        .with_batch_hint(BatchHint::Rows(2))
        .collect_batches()
        .expect("batches");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].row_count, 2);
    match &batches[0].columns[1] {
        OwnedColumnBuffer::Utf8 {
            offsets,
            data,
            valid,
        dictionary_ids: _,
        } => {
            assert_eq!(offsets, &vec![0, 4, 7]);
            assert_eq!(data, b"peardog");
            assert!(valid.is_none());
        }
        other => panic!("unexpected test2 string batch column: {other:?}"),
    }
}

#[test]
fn fixture_max_sas_date_matches_expected_shape() {
    let path = fixture_path("raw_data/pandas/max_sas_date.sas7bdat");
    if !path.exists() {
        eprintln!("skipping fixture-specific test: {}", path.display());
        return;
    }

    let dataset = Dataset::open(&path).expect("open max_sas_date");
    assert_eq!(
        dataset.metadata().table_name.as_deref(),
        Some("MAX_SAS_DATE")
    );
    assert_eq!(dataset.metadata().encoding.as_deref(), Some("ISO-8859-15"));
    assert_eq!(dataset.metadata().row_count, 2);
    assert_eq!(dataset.columns().len(), 5);
    assert_eq!(dataset.columns()[0].name, "text");
    assert_eq!(dataset.columns()[2].name, "dt_as_dt");

    let rows = dataset.scan().limit(2).collect_rows().expect("rows");
    assert_eq!(rows.len(), 2);
    assert!(matches!(rows[0].cells[0], OwnedCellValue::String(ref value) if value == "max"));
    assert!(
        matches!(rows[0].cells[2], OwnedCellValue::Float64(value) if value == 253717747199.999)
    );
    assert!(matches!(rows[1].cells[4], OwnedCellValue::Date(_)));

    let batches = dataset
        .scan()
        .limit(2)
        .with_batch_hint(BatchHint::Rows(2))
        .collect_batches()
        .expect("batches");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].row_count, 2);

    match &batches[0].columns[0] {
        OwnedColumnBuffer::Utf8 {
            offsets,
            data,
            valid,
        dictionary_ids: _,
        } => {
            assert_eq!(offsets, &vec![0, 3, 9]);
            assert_eq!(data, b"maxnormal");
            assert!(valid.is_none());
        }
        other => panic!("unexpected max_sas_date text batch column: {other:?}"),
    }

    match &batches[0].columns[2] {
        OwnedColumnBuffer::F64 { values, valid } => {
            assert_eq!(values, &vec![253717747199.999, 1880323199.999]);
            assert!(valid.is_none());
        }
        other => panic!("unexpected max_sas_date datetime batch column: {other:?}"),
    }
}

#[test]
fn fixture_charset_utf8_matches_expected_shape() {
    let path = fixture_path("raw_data/csharp/charset_utf8.sas7bdat");
    if !path.exists() {
        eprintln!("skipping fixture-specific test: {}", path.display());
        return;
    }

    let dataset = Dataset::open(&path).expect("open charset_utf8");
    assert_eq!(
        dataset.metadata().table_name.as_deref(),
        Some("CHARSET_UTF8")
    );
    assert_eq!(dataset.metadata().encoding.as_deref(), Some("UTF-8"));
    assert_eq!(dataset.metadata().row_count, 150);
    assert_eq!(dataset.columns().len(), 5);
    assert_eq!(dataset.columns()[4].name, "Species");

    let rows = dataset.scan().limit(3).collect_rows().expect("rows");
    assert_eq!(rows.len(), 3);
    assert!(matches!(rows[0].cells[0], OwnedCellValue::Float64(value) if value == 5.1));
    assert!(matches!(rows[1].cells[1], OwnedCellValue::Int64(3)));
    assert!(
        matches!(rows[2].cells[4], OwnedCellValue::String(ref value) if value == "Iris-setosa")
    );

    let batches = dataset
        .scan()
        .limit(3)
        .with_batch_hint(BatchHint::Rows(3))
        .collect_batches()
        .expect("batches");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].row_count, 3);
    match &batches[0].columns[4] {
        OwnedColumnBuffer::Utf8 {
            offsets,
            data,
            valid,
        dictionary_ids: _,
        } => {
            assert_eq!(offsets, &vec![0, 11, 22, 33]);
            assert_eq!(data, b"Iris-setosaIris-setosaIris-setosa");
            assert!(valid.is_none());
        }
        other => panic!("unexpected species batch column: {other:?}"),
    }
}

#[test]
fn fixture_cookie_matches_expected_shape() {
    let path = fixture_path("raw_data/csharp/54-cookie.sas7bdat");
    if !path.exists() {
        eprintln!("skipping fixture-specific test: {}", path.display());
        return;
    }

    let dataset = Dataset::open(&path).expect("open 54-cookie");
    assert_eq!(dataset.metadata().table_name.as_deref(), Some("COOKIE"));
    assert_eq!(dataset.metadata().encoding.as_deref(), Some("WINDOWS-1252"));
    assert_eq!(dataset.metadata().row_count, 54);
    assert_eq!(dataset.columns().len(), 29);
    assert_eq!(dataset.columns()[0].name, "__CKG");
    assert_eq!(dataset.columns()[25].name, "INSTRON");

    let rows = dataset.scan().limit(3).collect_rows().expect("rows");
    assert_eq!(rows.len(), 3);
    assert!(matches!(rows[0].cells[0], OwnedCellValue::Float64(value) if value == 13.8));
    assert!(matches!(rows[1].cells[25], OwnedCellValue::String(ref value) if value == "15.1"));
    assert!(matches!(rows[2].cells[25], OwnedCellValue::String(ref value) if value == "."));

    let batches = dataset
        .scan()
        .limit(3)
        .with_batch_hint(BatchHint::Rows(3))
        .collect_batches()
        .expect("batches");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].row_count, 3);
    match &batches[0].columns[25] {
        OwnedColumnBuffer::Utf8 {
            offsets,
            data,
            valid,
        dictionary_ids: _,
        } => {
            assert_eq!(offsets, &vec![0, 3, 7, 8]);
            assert_eq!(data, b"9.015.1.");
            assert!(valid.is_none());
        }
        other => panic!("unexpected INSTRON batch column: {other:?}"),
    }
}

fn fixture_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(relative)
}

fn excluded_fixture(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| EXCLUDED_FIXTURE_NAMES.contains(&name))
}

fn collect_fixture_files(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_fixture_files(&path, out);
            continue;
        }
        if path
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("sas7bdat"))
        {
            out.push(path);
        }
    }
}
