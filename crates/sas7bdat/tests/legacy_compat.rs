#![allow(deprecated)]

use sas7bdat::{DecodePolicy, OrderingMode, RowSelection, SasReader, Shape};
use sas7bdat_test_support::common;
use std::collections::HashMap;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

#[test]
fn legacy_rows_windowed_matches_query_window() {
    let mut sas = open_datetime_fixture();
    let selection = RowSelection::new().skip_rows(1).max_rows(2);

    let mut legacy_rows = sas
        .rows_windowed(&selection)
        .expect("legacy rows_windowed should succeed");
    let mut legacy_count = 0usize;
    while legacy_rows
        .try_next()
        .expect("legacy rows_windowed iteration failed")
        .is_some()
    {
        legacy_count = legacy_count.saturating_add(1);
    }

    let mut query = sas.query().shape(Shape::Rows).window(1, 2);
    let mut query_rows = query
        .stream_ordered()
        .expect("query stream_ordered should succeed");
    let mut query_count = 0usize;
    while query_rows
        .try_next()
        .expect("query stream_ordered iteration failed")
        .is_some()
    {
        query_count = query_count.saturating_add(1);
    }

    assert_eq!(legacy_count, query_count);
}

#[test]
fn legacy_select_with_matches_query_projection() {
    let mut sas = open_datetime_fixture();
    let metadata = sas.metadata().clone();
    let projection = [0usize, 2usize];
    let names: Vec<String> = projection
        .iter()
        .map(|&idx| metadata.variables[idx].name.trim_end().to_owned())
        .collect();
    let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();

    let legacy_selection = RowSelection::new().column_names(names.clone()).max_rows(1);
    let mut legacy_rows = sas
        .select_with(&legacy_selection)
        .expect("legacy select_with should succeed");
    let legacy_first = legacy_rows
        .try_next()
        .expect("legacy select_with iteration failed")
        .expect("legacy select_with should produce first row");

    let mut query = sas
        .query()
        .shape(Shape::Projection)
        .columns_by_name(&name_refs)
        .expect("query columns_by_name should resolve")
        .window(0, 1);
    let mut query_rows = query
        .stream_ordered()
        .expect("query stream_ordered should succeed");
    let query_first = query_rows
        .try_next()
        .expect("query stream_ordered iteration failed")
        .expect("query projection should produce first row");

    assert_eq!(legacy_first, query_first);
}

#[test]
fn legacy_raw_parallel_unordered_matches_query_stats() {
    let mut sas = open_datetime_fixture();

    let legacy_rows = AtomicU64::new(0);
    let legacy_bytes = AtomicU64::new(0);
    let legacy_stats = sas
        .scan_raw_rows_parallel_unordered_with_stats(4, |row| {
            legacy_rows.fetch_add(1, Ordering::Relaxed);
            legacy_bytes.fetch_add(row.len() as u64, Ordering::Relaxed);
            Ok(())
        })
        .expect("legacy raw unordered scan should succeed");

    let query_rows = AtomicU64::new(0);
    let query_bytes = AtomicU64::new(0);
    let query_stats = sas
        .query()
        .shape(Shape::Raw)
        .parallel(4)
        .ordering(OrderingMode::Unordered)
        .scan_raw_unordered(|row| {
            query_rows.fetch_add(1, Ordering::Relaxed);
            query_bytes.fetch_add(row.len() as u64, Ordering::Relaxed);
            Ok(())
        })
        .expect("query raw unordered scan should succeed");

    assert_eq!(legacy_stats.rows, query_stats.rows);
    assert_eq!(legacy_stats.raw_bytes, query_stats.raw_bytes);
    assert_eq!(
        legacy_rows.load(Ordering::Relaxed),
        query_rows.load(Ordering::Relaxed)
    );
    assert_eq!(
        legacy_bytes.load(Ordering::Relaxed),
        query_bytes.load(Ordering::Relaxed)
    );
}

#[test]
fn legacy_projection_parallel_unordered_matches_query_values() {
    let mut sas = open_datetime_fixture();
    let projection: Vec<usize> = (0..3)
        .take(usize::try_from(sas.metadata().column_count).expect("column count fits in usize"))
        .collect();
    assert!(
        !projection.is_empty(),
        "fixture must expose at least one column"
    );

    let legacy_rows: Mutex<HashMap<String, u64>> = Mutex::new(HashMap::new());
    sas.scan_projected_columns_parallel_unordered_with_decode_policy(
        &projection,
        4,
        DecodePolicy::FAST_SCAN,
        |row| {
            let key = format!("{row:?}");
            {
                let mut rows = legacy_rows.lock().expect("legacy row map mutex poisoned");
                *rows.entry(key).or_insert(0) += 1;
            }
            Ok(())
        },
    )
    .expect("legacy projected unordered scan should succeed");

    let query_rows: Mutex<HashMap<String, u64>> = Mutex::new(HashMap::new());
    sas.query()
        .shape(Shape::Projection)
        .projection(&projection)
        .decode(DecodePolicy::FAST_SCAN)
        .parallel(4)
        .ordering(OrderingMode::Unordered)
        .scan_unordered(|row| {
            let key = format!("{row:?}");
            {
                let mut rows = query_rows.lock().expect("query row map mutex poisoned");
                *rows.entry(key).or_insert(0) += 1;
            }
            Ok(())
        })
        .expect("query projected unordered scan should succeed");

    let legacy_rows = legacy_rows
        .into_inner()
        .expect("legacy row map mutex poisoned");
    let query_rows = query_rows
        .into_inner()
        .expect("query row map mutex poisoned");
    assert_eq!(legacy_rows, query_rows);
}

fn open_datetime_fixture() -> SasReader<std::fs::File> {
    let path = common::fixture_path("fixtures/raw_data/pandas/datetime.sas7bdat");
    SasReader::open(path).expect("failed to open datetime fixture")
}
