use sas7bdat::{CellValue, DecodePolicy, Error, RowSelection, SasReader, dataset::VariableKind};
use sas7bdat_test_support::common;
use std::collections::HashMap;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

#[test]
fn rows_windowed_respects_skip_and_limit() {
    let mut sas = open_datetime_fixture();

    let column_count =
        usize::try_from(sas.metadata().column_count).expect("column count fits in usize");

    let options = RowSelection::new().skip_rows(1).max_rows(2);
    let mut rows = sas
        .rows_windowed(&options)
        .expect("failed to build windowed iterator");

    let mut seen = 0usize;
    while let Some(row) = rows.try_next().expect("row iteration failed") {
        assert_eq!(row.len(), column_count, "row should contain every column");
        seen += 1;
    }

    assert_eq!(seen, 2);
    assert!(
        rows.try_next().expect("final advance failed").is_none(),
        "iterator should end after returning the maximum rows"
    );
}

#[test]
fn select_with_supports_name_projection() {
    let mut sas = open_datetime_fixture();

    let metadata = sas.metadata().clone();
    let column_indices = [0usize, 2usize];
    let column_names: Vec<String> = column_indices
        .iter()
        .map(|&idx| metadata.variables[idx].name.trim_end().to_string())
        .collect();

    let first_full_row: Vec<CellValue<'static>> = {
        let mut iter = sas.rows().expect("failed to build full iterator");
        iter.try_next()
            .expect("row iteration failed")
            .expect("expected at least one row")
            .into_iter()
            .map(CellValue::into_owned)
            .collect()
    };

    let options = RowSelection::new()
        .column_names(column_names.clone())
        .max_rows(1);
    let mut rows = sas
        .select_with(&options)
        .expect("failed to build projected iterator");

    let first = rows
        .try_next()
        .expect("row iteration failed")
        .expect("expected first row");
    assert_eq!(first.len(), column_names.len());
    for (value, (&index, name)) in first
        .iter()
        .zip(column_indices.iter().zip(column_names.iter()))
    {
        assert_eq!(
            value, &first_full_row[index],
            "projected value for column '{name}' did not match reference row"
        );
    }
    assert!(
        rows.try_next().expect("final advance failed").is_none(),
        "iterator should respect max_rows limit"
    );
}

#[test]
fn select_with_rejects_duplicate_names() {
    let mut sas = open_datetime_fixture();

    let options = RowSelection::new().column_names(["DATE1", "DATE1"]);
    let Err(err) = sas.select_with(&options) else {
        panic!("expected duplicate projection to fail");
    };
    match err {
        Error::InvalidMetadata { .. } => {}
        other => panic!("expected InvalidMetadata error, got {other:?}"),
    }
}

#[test]
#[allow(clippy::cast_precision_loss)]
fn scan_numeric_columns_matches_fast_projection_values() {
    let mut sas = open_datetime_fixture();
    let numeric_indices: Vec<usize> = sas
        .metadata()
        .variables
        .iter()
        .enumerate()
        .filter_map(|(idx, variable)| match variable.kind {
            VariableKind::Numeric => Some(idx),
            VariableKind::Character => None,
        })
        .take(3)
        .collect();
    assert!(
        !numeric_indices.is_empty(),
        "fixture must expose at least one numeric column"
    );

    let expected_rows: Vec<Vec<Option<f64>>> = {
        let mut rows = sas
            .select_columns_fast(&numeric_indices)
            .expect("failed to build fast projection");
        let mut values = Vec::new();
        while let Some(row) = rows.try_next().expect("row iteration failed") {
            let projected = row
                .into_iter()
                .map(|cell| match cell {
                    CellValue::Missing(_) => None,
                    CellValue::Int32(v) => Some(f64::from(v)),
                    CellValue::Int64(v) => Some(v as f64),
                    CellValue::Float(v) => Some(v),
                    other => panic!("unexpected non-numeric cell in fast projection: {other:?}"),
                })
                .collect();
            values.push(projected);
        }
        values
    };

    let mut observed_rows: Vec<Vec<Option<f64>>> = Vec::new();
    let scanned = sas
        .scan_numeric_columns(&numeric_indices, |values| {
            observed_rows.push(values.to_vec());
            Ok(())
        })
        .expect("typed numeric scan failed");
    assert_eq!(
        usize::try_from(scanned).expect("row count fits in usize"),
        expected_rows.len()
    );
    assert_eq!(observed_rows, expected_rows);
}

#[test]
fn scan_projected_columns_matches_projection_values() {
    let mut sas = open_datetime_fixture();
    let projection_indices: Vec<usize> = (0..3)
        .take(usize::try_from(sas.metadata().column_count).expect("column count fits in usize"))
        .collect();
    assert!(
        !projection_indices.is_empty(),
        "fixture must expose at least one column"
    );

    let expected_rows: Vec<Vec<CellValue<'static>>> = {
        let mut rows = sas
            .select_columns_with_decode_policy(&projection_indices, DecodePolicy::FAST_SCAN)
            .expect("failed to build fast projection");
        let mut values = Vec::new();
        while let Some(row) = rows.try_next().expect("row iteration failed") {
            values.push(row);
        }
        values
    };

    let mut observed_rows: Vec<Vec<CellValue<'static>>> = Vec::new();
    let scanned = sas
        .scan_projected_columns_fast(&projection_indices, |values| {
            observed_rows.push(values.iter().cloned().map(CellValue::into_owned).collect());
            Ok(())
        })
        .expect("projected scan failed");
    assert_eq!(
        usize::try_from(scanned).expect("row count fits in usize"),
        expected_rows.len()
    );
    assert_eq!(observed_rows, expected_rows);
}

#[test]
fn scan_raw_rows_matches_streaming_raw_bytes() {
    let mut sas = open_datetime_fixture();

    let mut expected_rows = 0u64;
    let mut expected_bytes = 0u64;
    {
        let mut rows = sas
            .stream_rows_fast()
            .expect("failed to build streaming iterator");
        while let Some(row) = rows.try_next().expect("streaming row iteration failed") {
            expected_rows = expected_rows.saturating_add(1);
            for cell in row.streaming_row() {
                let cell = cell.expect("streaming cell decode failed");
                expected_bytes = expected_bytes
                    .saturating_add(u64::try_from(cell.raw_slice().len()).unwrap_or(0));
            }
        }
    }

    let stats = sas
        .scan_raw_rows_with_stats(|_row| Ok(()))
        .expect("raw row scan failed");

    assert_eq!(stats.rows, expected_rows);
    assert_eq!(stats.raw_bytes, expected_bytes);
}

#[test]
fn scan_raw_rows_parallel_matches_single_thread() {
    let mut sas = open_datetime_fixture();
    let mut single_rows = 0u64;
    let mut single_bytes = 0u64;
    sas.scan_raw_rows(|row| {
        single_rows = single_rows.saturating_add(1);
        single_bytes = single_bytes.saturating_add(u64::try_from(row.len()).unwrap_or(0));
        Ok(())
    })
    .expect("single-thread raw scan failed");

    let parallel_stats = sas
        .scan_raw_rows_parallel_with_stats(4, |_row| Ok(()))
        .expect("parallel raw scan failed");

    assert_eq!(parallel_stats.rows, single_rows);
    assert_eq!(parallel_stats.raw_bytes, single_bytes);
}

#[test]
fn scan_raw_rows_batched_matches_single_thread() {
    let mut sas = open_datetime_fixture();
    let mut single_rows = 0u64;
    let mut single_bytes = 0u64;
    sas.scan_raw_rows(|row| {
        single_rows = single_rows.saturating_add(1);
        single_bytes = single_bytes.saturating_add(row.len() as u64);
        Ok(())
    })
    .expect("single-thread raw scan failed");

    let mut batched_rows = 0u64;
    let mut batched_bytes = 0u64;
    let batch_stats = sas
        .scan_raw_rows_batched_with_stats(2, |batch| {
            for row in batch.rows() {
                batched_rows = batched_rows.saturating_add(1);
                batched_bytes = batched_bytes.saturating_add(row.len() as u64);
            }
            Ok(())
        })
        .expect("batched raw scan failed");

    assert_eq!(batch_stats.rows, single_rows);
    assert_eq!(batch_stats.raw_bytes, single_bytes);
    assert_eq!(batched_rows, single_rows);
    assert_eq!(batched_bytes, single_bytes);
}

#[test]
fn scan_projected_columns_parallel_matches_single_thread() {
    let mut sas = open_datetime_fixture();
    let projection_indices: Vec<usize> = (0..3)
        .take(usize::try_from(sas.metadata().column_count).expect("column count fits in usize"))
        .collect();
    assert!(
        !projection_indices.is_empty(),
        "fixture must expose at least one column"
    );

    let mut single_rows: Vec<Vec<CellValue<'static>>> = Vec::new();
    sas.scan_projected_columns_with_decode_policy(
        &projection_indices,
        DecodePolicy::FAST_SCAN,
        |values| {
            single_rows.push(values.iter().cloned().map(CellValue::into_owned).collect());
            Ok(())
        },
    )
    .expect("single-thread projected scan failed");

    let mut parallel_rows: Vec<Vec<CellValue<'static>>> = Vec::new();
    sas.scan_projected_columns_parallel_ordered_with_decode_policy(
        &projection_indices,
        4,
        DecodePolicy::FAST_SCAN,
        |values| {
            parallel_rows.push(values.to_vec());
            Ok(())
        },
    )
    .expect("parallel projected scan failed");

    assert_eq!(parallel_rows, single_rows);
}

#[test]
fn scan_raw_rows_parallel_unordered_matches_single_thread() {
    let mut sas = open_datetime_fixture();
    let mut single_rows = 0u64;
    let mut single_bytes = 0u64;
    sas.scan_raw_rows(|row| {
        single_rows = single_rows.saturating_add(1);
        single_bytes = single_bytes.saturating_add(u64::try_from(row.len()).unwrap_or(0));
        Ok(())
    })
    .expect("single-thread raw scan failed");

    let unordered_rows = AtomicU64::new(0);
    let unordered_bytes = AtomicU64::new(0);
    sas.scan_raw_rows_parallel_unordered(4, |row| {
        unordered_rows.fetch_add(1, Ordering::Relaxed);
        unordered_bytes.fetch_add(u64::try_from(row.len()).unwrap_or(0), Ordering::Relaxed);
        Ok(())
    })
    .expect("unordered parallel raw scan failed");

    assert_eq!(unordered_rows.load(Ordering::Relaxed), single_rows);
    assert_eq!(unordered_bytes.load(Ordering::Relaxed), single_bytes);
}

#[test]
fn scan_raw_rows_parallel_unordered_batched_matches_single_thread() {
    let mut sas = open_datetime_fixture();
    let mut single_rows = 0u64;
    let mut single_bytes = 0u64;
    sas.scan_raw_rows(|row| {
        single_rows = single_rows.saturating_add(1);
        single_bytes = single_bytes.saturating_add(row.len() as u64);
        Ok(())
    })
    .expect("single-thread raw scan failed");

    let unordered_rows = AtomicU64::new(0);
    let unordered_bytes = AtomicU64::new(0);
    let unordered_stats = sas
        .scan_raw_rows_parallel_unordered_batched_with_stats(4, 8, |batch| {
            let mut local_rows = 0u64;
            let mut local_bytes = 0u64;
            for row in batch.rows() {
                local_rows = local_rows.saturating_add(1);
                local_bytes = local_bytes.saturating_add(row.len() as u64);
            }
            unordered_rows.fetch_add(local_rows, Ordering::Relaxed);
            unordered_bytes.fetch_add(local_bytes, Ordering::Relaxed);
            Ok(())
        })
        .expect("unordered parallel batched raw scan failed");

    assert_eq!(unordered_stats.rows, single_rows);
    assert_eq!(unordered_stats.raw_bytes, single_bytes);
    assert_eq!(unordered_rows.load(Ordering::Relaxed), single_rows);
    assert_eq!(unordered_bytes.load(Ordering::Relaxed), single_bytes);
}

#[test]
fn scan_projected_columns_parallel_unordered_matches_single_thread() {
    let mut sas = open_datetime_fixture();
    let projection_indices: Vec<usize> = (0..3)
        .take(usize::try_from(sas.metadata().column_count).expect("column count fits in usize"))
        .collect();
    assert!(
        !projection_indices.is_empty(),
        "fixture must expose at least one column"
    );

    let mut single_rows: HashMap<String, u64> = HashMap::new();
    sas.scan_projected_columns_with_decode_policy(
        &projection_indices,
        DecodePolicy::FAST_SCAN,
        |values| {
            let key = format!("{values:?}");
            *single_rows.entry(key).or_insert(0) += 1;
            Ok(())
        },
    )
    .expect("single-thread projected scan failed");

    let unordered_rows: Mutex<HashMap<String, u64>> = Mutex::new(HashMap::new());
    sas.scan_projected_columns_parallel_unordered_with_decode_policy(
        &projection_indices,
        4,
        DecodePolicy::FAST_SCAN,
        |values| {
            let key = format!("{values:?}");
            {
                let mut rows = unordered_rows.lock().expect("unordered row map poisoned");
                *rows.entry(key).or_insert(0) += 1;
            }
            Ok(())
        },
    )
    .expect("unordered parallel projected scan failed");

    let unordered_rows = unordered_rows
        .into_inner()
        .expect("unordered row map poisoned");
    assert_eq!(unordered_rows, single_rows);
}

#[test]
fn scan_numeric_columns_rejects_non_numeric_projection() {
    let mut sas = open_datetime_fixture();
    let mut non_numeric_index =
        sas.metadata()
            .variables
            .iter()
            .enumerate()
            .find_map(|(idx, variable)| match variable.kind {
                VariableKind::Character => Some(idx),
                VariableKind::Numeric => None,
            });
    if non_numeric_index.is_none() {
        sas = open_utf8_fixture();
        non_numeric_index =
            sas.metadata()
                .variables
                .iter()
                .enumerate()
                .find_map(|(idx, variable)| match variable.kind {
                    VariableKind::Character => Some(idx),
                    VariableKind::Numeric => None,
                });
    }
    let non_numeric_index =
        non_numeric_index.expect("fixture must expose at least one character column");

    let Err(err) = sas.scan_numeric_columns(&[non_numeric_index], |_values| Ok(())) else {
        panic!("expected non-numeric projection to fail");
    };
    match err {
        Error::InvalidMetadata { .. } => {}
        other => panic!("expected InvalidMetadata error, got {other:?}"),
    }
}

fn open_datetime_fixture() -> SasReader<std::fs::File> {
    let path = common::fixture_path("fixtures/raw_data/pandas/datetime.sas7bdat");
    SasReader::open(path).expect("failed to open datetime fixture")
}

fn open_utf8_fixture() -> SasReader<std::fs::File> {
    let path = common::fixture_path("fixtures/raw_data/csharp/charset_utf8.sas7bdat");
    SasReader::open(path).expect("failed to open utf8 fixture")
}
