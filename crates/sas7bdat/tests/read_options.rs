use sas7bdat::{
    CellValue, DecodePolicy, Error, OrderingMode, SasReader, Shape, dataset::VariableKind,
};
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

    let mut query = sas.query().shape(Shape::Rows).window(1, 2);
    let mut rows = query
        .stream_ordered()
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
        let mut query = sas
            .query()
            .shape(Shape::Rows)
            .ordering(OrderingMode::Ordered);
        let mut iter = query
            .stream_ordered()
            .expect("failed to build full iterator");
        iter.try_next()
            .expect("row iteration failed")
            .expect("expected at least one row")
            .into_iter()
            .map(CellValue::into_owned)
            .collect()
    };

    let name_refs: Vec<&str> = column_names.iter().map(String::as_str).collect();
    let mut query = sas
        .query()
        .shape(Shape::Projection)
        .columns_by_name(&name_refs)
        .expect("projection by name should resolve")
        .window(0, 1);
    let mut rows = query
        .stream_ordered()
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

    let Err(err) = sas
        .query()
        .shape(Shape::Projection)
        .columns_by_name(&["DATE1", "DATE1"])
    else {
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
        let mut query = sas
            .query()
            .shape(Shape::Rows)
            .decode(DecodePolicy::FAST_SCAN)
            .ordering(OrderingMode::Ordered);
        let mut rows = query
            .stream_ordered_view()
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
        .query()
        .shape(Shape::Raw)
        .parallel(4)
        .scan_raw_unordered(|_row| Ok(()))
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
    sas.query()
        .shape(Shape::Projection)
        .projection(&projection_indices)
        .decode(DecodePolicy::FAST_SCAN)
        .parallel(4)
        .ordering(OrderingMode::Ordered)
        .scan_ordered(|values| {
            parallel_rows.push(values.to_vec());
            Ok(())
        })
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
    let stats = sas
        .query()
        .shape(Shape::Raw)
        .parallel(4)
        .ordering(OrderingMode::Unordered)
        .scan_raw_unordered(|row| {
            unordered_rows.fetch_add(1, Ordering::Relaxed);
            unordered_bytes.fetch_add(u64::try_from(row.len()).unwrap_or(0), Ordering::Relaxed);
            Ok(())
        })
        .expect("unordered parallel raw scan failed");

    assert_eq!(stats.rows, single_rows);
    assert_eq!(stats.raw_bytes, single_bytes);
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

    let batches = sas
        .query()
        .shape(Shape::Raw)
        .parallel(4)
        .ordering(OrderingMode::Unordered)
        .collect_raw_batches(8)
        .expect("unordered parallel batched raw scan failed");
    let unordered_rows = batches.iter().fold(0u64, |acc, batch| {
        acc.saturating_add(batch.row_count() as u64)
    });
    let unordered_bytes = batches.iter().fold(0u64, |acc, batch| {
        acc.saturating_add(batch.rows().map(|row| row.len() as u64).sum::<u64>())
    });
    let unordered_stats = sas
        .query()
        .shape(Shape::Raw)
        .parallel(4)
        .ordering(OrderingMode::Unordered)
        .scan_raw_unordered(|_row| Ok(()))
        .expect("unordered parallel raw stats scan failed");

    assert_eq!(unordered_stats.rows, single_rows);
    assert_eq!(unordered_stats.raw_bytes, single_bytes);
    assert_eq!(unordered_rows, single_rows);
    assert_eq!(unordered_bytes, single_bytes);
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
    sas.query()
        .shape(Shape::Projection)
        .projection(&projection_indices)
        .decode(DecodePolicy::FAST_SCAN)
        .parallel(4)
        .ordering(OrderingMode::Unordered)
        .scan_unordered(|values| {
            let key = format!("{values:?}");
            {
                let mut rows = unordered_rows.lock().expect("unordered row map poisoned");
                *rows.entry(key).or_insert(0) += 1;
            }
            Ok(())
        })
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

#[test]
fn query_projection_scan_ordered_matches_scan_projection() {
    let mut sas = open_datetime_fixture();
    let indices = [0usize, 1usize, 2usize];

    let mut expected = Vec::new();
    sas.scan_projected_columns_with_decode_policy(&indices, DecodePolicy::FAST_SCAN, |row| {
        expected.push(
            row.iter()
                .cloned()
                .map(CellValue::into_owned)
                .collect::<Vec<_>>(),
        );
        Ok(())
    })
    .expect("legacy projection scan failed");

    let mut observed = Vec::new();
    sas.query()
        .shape(Shape::Projection)
        .projection(&indices)
        .decode(DecodePolicy::FAST_SCAN)
        .ordering(OrderingMode::Ordered)
        .scan_ordered(|row| {
            observed.push(row.to_vec());
            Ok(())
        })
        .expect("query projection scan failed");

    assert_eq!(observed, expected);
}

#[test]
fn query_collect_frame_matches_metadata_row_count() {
    let mut sas = open_datetime_fixture();
    let frame = sas
        .query()
        .shape(Shape::Frame)
        .collect_frame()
        .expect("collect_frame failed");

    let expected_rows = usize::try_from(sas.metadata().row_count).expect("row count fits in usize");
    let expected_cols =
        usize::try_from(sas.metadata().column_count).expect("column count fits in usize");
    assert_eq!(frame.row_count, expected_rows);
    assert_eq!(frame.columns.len(), expected_cols);
    for column in &frame.columns {
        assert_eq!(column.row_count(), frame.row_count);
    }
}

#[test]
fn query_collect_frame_batches_cover_all_rows() {
    let mut sas = open_datetime_fixture();
    let batches = sas
        .query()
        .shape(Shape::Frame)
        .collect_frame_batches(2)
        .expect("collect_frame_batches failed");

    let total_rows: usize = batches.iter().map(|batch| batch.row_count).sum();
    let expected_rows = usize::try_from(sas.metadata().row_count).expect("row count fits in usize");
    assert_eq!(total_rows, expected_rows);
    assert!(batches.iter().all(|batch| batch.row_count <= 2));
}

#[test]
fn query_stream_ordered_window_matches_expected_row_count() {
    let mut sas = open_datetime_fixture();
    let mut query = sas.query().shape(Shape::Rows).window(1, 2);
    let mut stream = query
        .stream_ordered()
        .expect("stream_ordered with window failed");
    let mut rows = 0usize;
    while let Some(row) = stream.try_next().expect("query stream row decode failed") {
        assert!(!row.is_empty());
        rows = rows.saturating_add(1);
    }
    assert_eq!(rows, 2);
}

#[test]
fn query_stream_ordered_view_projection_supports_name_lookup() {
    let mut sas = open_datetime_fixture();
    let projected_names: Vec<String> = sas
        .metadata()
        .variables
        .iter()
        .take(2)
        .map(|variable| variable.name.clone())
        .collect();
    assert_eq!(
        projected_names.len(),
        2,
        "fixture should have at least two columns"
    );

    let maybe_non_projected = sas
        .metadata()
        .variables
        .get(2)
        .map(|variable| variable.name.clone());
    let name_refs: Vec<&str> = projected_names.iter().map(String::as_str).collect();

    let mut query = sas
        .query()
        .shape(Shape::Projection)
        .columns_by_name(&name_refs)
        .expect("projection by name should resolve")
        .ordering(OrderingMode::Ordered);
    let mut stream = query
        .stream_ordered_view()
        .expect("stream_ordered_view should initialize");

    let mut seen = 0usize;
    while let Some(row) = stream.try_next().expect("row view iteration failed") {
        assert!(
            row.cell(name_refs[0]).is_ok(),
            "projected column should be visible"
        );
        assert!(
            row.cell(name_refs[1]).is_ok(),
            "projected column should be visible"
        );
        if let Some(non_projected) = maybe_non_projected.as_deref() {
            assert!(
                row.cell(non_projected).is_err(),
                "non-projected column should not be visible"
            );
        }
        seen = seen.saturating_add(1);
    }
    assert!(seen > 0, "expected at least one row");
}

#[test]
fn query_collect_frame_window_limits_rows() {
    let mut sas = open_datetime_fixture();
    let frame = sas
        .query()
        .shape(Shape::Frame)
        .window(1, 2)
        .collect_frame()
        .expect("collect_frame with window failed");
    assert_eq!(frame.row_count, 2);
}

#[test]
fn read_frame_aliases_match_collect_variants() {
    let mut sas_collect = open_datetime_fixture();
    let collect_frame = sas_collect.collect_frame().expect("collect_frame failed");
    let collect_batches = sas_collect
        .collect_frame_batches(2)
        .expect("collect_frame_batches failed");

    let mut sas_read = open_datetime_fixture();
    let read_frame = sas_read.read_frame().expect("read_frame failed");
    let read_batches = sas_read
        .read_frame_batches(2)
        .expect("read_frame_batches failed");

    assert_eq!(read_frame.row_count, collect_frame.row_count);
    assert_eq!(read_frame.columns.len(), collect_frame.columns.len());
    assert_eq!(read_batches.len(), collect_batches.len());
    assert_eq!(
        read_batches
            .iter()
            .map(|batch| batch.row_count)
            .sum::<usize>(),
        collect_batches
            .iter()
            .map(|batch| batch.row_count)
            .sum::<usize>()
    );
}

#[test]
fn query_raw_unordered_returns_stats() {
    let mut sas = open_datetime_fixture();
    let stats = sas
        .query()
        .shape(Shape::Raw)
        .parallel(4)
        .scan_raw_unordered(|_row| Ok(()))
        .expect("query raw unordered scan failed");

    assert!(stats.rows > 0);
    assert!(stats.raw_bytes > 0);
}

#[test]
fn query_rejects_numeric_unordered() {
    let mut sas = open_datetime_fixture();
    let numeric_index = sas
        .metadata()
        .variables
        .iter()
        .enumerate()
        .find_map(|(idx, variable)| match variable.kind {
            VariableKind::Numeric => Some(idx),
            VariableKind::Character => None,
        })
        .expect("fixture must expose at least one numeric column");

    let err = sas
        .query()
        .shape(Shape::Numeric)
        .projection(&[numeric_index])
        .ordering(OrderingMode::Unordered)
        .scan_numeric(|_| Ok(()))
        .expect_err("numeric unordered query should fail");
    assert!(matches!(err, Error::InvalidConfiguration { .. }));
}

#[test]
fn query_rejects_batch_rows_for_rows_shape() {
    let mut sas = open_datetime_fixture();
    let err = sas
        .query()
        .shape(Shape::Rows)
        .batch_rows(32)
        .scan_ordered(|_| Ok(()))
        .expect_err("rows shape with batch_rows should fail");
    assert!(matches!(err, Error::InvalidConfiguration { .. }));
}

#[test]
fn query_rejects_window_for_unordered_scans() {
    let mut sas = open_datetime_fixture();
    let err = sas
        .query()
        .shape(Shape::Rows)
        .window(1, 1)
        .scan_unordered(|_| Ok(()))
        .expect_err("unordered scan with window should fail");
    assert!(matches!(err, Error::InvalidConfiguration { .. }));

    let err = sas
        .query()
        .shape(Shape::Raw)
        .window(1, 1)
        .scan_raw_unordered(|_| Ok(()))
        .expect_err("unordered raw scan with window should fail");
    assert!(matches!(err, Error::InvalidConfiguration { .. }));
}

#[test]
fn scan_projected_fast_matches_parallel_projection_scan() {
    let mut sas = open_datetime_fixture();
    let projection = [0usize, 1usize, 2usize];
    let expected: Mutex<Vec<Vec<CellValue<'static>>>> = Mutex::new(Vec::new());
    sas.query()
        .shape(Shape::Projection)
        .projection(&projection)
        .decode(DecodePolicy::FAST_SCAN)
        .parallel(2)
        .ordering(OrderingMode::Unordered)
        .scan_unordered(|row| {
            expected.lock().expect("expected rows mutex poisoned").push(
                row.iter()
                    .cloned()
                    .map(CellValue::into_owned)
                    .collect::<Vec<_>>(),
            );
            Ok(())
        })
        .expect("query projected parallel scan failed");

    let observed: Mutex<Vec<Vec<CellValue<'static>>>> = Mutex::new(Vec::new());
    sas.scan_projected_fast(&projection, 2, |row| {
        observed.lock().expect("observed rows mutex poisoned").push(
            row.iter()
                .cloned()
                .map(CellValue::into_owned)
                .collect::<Vec<_>>(),
        );
        Ok(())
    })
    .expect("scan_projected_fast failed");

    let expected = expected.into_inner().expect("expected rows mutex poisoned");
    let observed = observed.into_inner().expect("observed rows mutex poisoned");
    let mut expected_sorted: Vec<String> = expected.iter().map(|row| format!("{row:?}")).collect();
    let mut observed_sorted: Vec<String> = observed.iter().map(|row| format!("{row:?}")).collect();
    expected_sorted.sort_unstable();
    observed_sorted.sort_unstable();
    assert_eq!(observed_sorted, expected_sorted);
}

#[test]
fn scan_raw_fast_with_stats_returns_non_zero_counters() {
    let mut sas = open_datetime_fixture();
    let stats = sas
        .scan_raw_fast_with_stats(2, |_row| Ok(()))
        .expect("scan_raw_fast_with_stats failed");
    assert!(stats.rows > 0);
    assert!(stats.raw_bytes > 0);
}

#[cfg(feature = "arrow")]
#[test]
fn frame_batch_converts_to_arrow_record_batch() {
    let mut sas = open_datetime_fixture();
    let frame = sas
        .query()
        .shape(Shape::Frame)
        .collect_frame()
        .expect("collect_frame failed");
    let batch = frame
        .to_arrow_record_batch()
        .expect("frame to Arrow conversion failed");
    let consumed_batch = frame
        .clone()
        .into_arrow_record_batch()
        .expect("frame into Arrow conversion failed");

    assert_eq!(batch.num_rows(), frame.row_count);
    assert_eq!(batch.num_columns(), frame.columns.len());
    assert_eq!(consumed_batch.num_rows(), frame.row_count);
    assert_eq!(consumed_batch.num_columns(), frame.columns.len());
}

#[test]
fn frame_batch_schema_and_column_lookup_helpers_work() {
    let mut sas = open_datetime_fixture();
    let frame = sas
        .query()
        .shape(Shape::Frame)
        .collect_frame()
        .expect("collect_frame failed");

    assert_eq!(frame.row_count(), frame.row_count);
    assert_eq!(frame.column_count(), frame.columns.len());
    assert_eq!(frame.columns().len(), frame.columns.len());
    assert_eq!(frame.schema().fields().len(), frame.columns.len());

    let first_field = frame
        .schema()
        .field(0)
        .expect("expected at least one schema field");
    let first_index = frame
        .schema()
        .field_index(&first_field.name)
        .expect("first field should resolve by name");
    assert_eq!(first_index, 0);

    let by_name = frame
        .column_by_name(&first_field.name)
        .expect("column should resolve by field name");
    let by_index = frame.column(0).expect("first column should exist");
    assert_eq!(by_name.physical_type(), by_index.physical_type());

    assert!(frame.column_by_name("__does_not_exist__").is_none());
    assert!(frame.field_by_name("__does_not_exist__").is_none());
}

#[test]
fn frame_column_typed_accessors_and_value_access_work() {
    let mut sas = open_datetime_fixture();
    let frame = sas
        .query()
        .shape(Shape::Frame)
        .collect_frame()
        .expect("collect_frame failed");

    let mut saw_utf8 = false;
    let mut saw_primitive = false;
    for column in frame.columns() {
        match column {
            sas7bdat::FrameColumn::Utf8(col) => {
                saw_utf8 = true;
                assert!(column.as_utf8().is_some());
                assert!(column.as_i64().is_none());
                assert!(column.as_f64().is_none());
                assert!(column.as_binary().is_none());
                assert!(column.as_date32().is_none());
                assert_eq!(col.len(), frame.row_count());
                assert!(
                    col.value(frame.row_count())
                        .expect("utf8 out-of-range read")
                        .is_none()
                );
            }
            sas7bdat::FrameColumn::I64(col)
            | sas7bdat::FrameColumn::DateTime64(col)
            | sas7bdat::FrameColumn::Time64(col) => {
                saw_primitive = true;
                assert!(column.as_i64().is_some());
                assert!(column.as_utf8().is_none());
                assert_eq!(col.len(), frame.row_count());
                assert!(col.value(frame.row_count()).is_none());
                assert!(col.value_copied(frame.row_count()).is_none());
            }
            sas7bdat::FrameColumn::F64(col) => {
                saw_primitive = true;
                assert!(column.as_f64().is_some());
                assert!(column.as_i64().is_none());
                assert_eq!(col.len(), frame.row_count());
                assert!(col.value(frame.row_count()).is_none());
            }
            sas7bdat::FrameColumn::Binary(col) => {
                assert!(column.as_binary().is_some());
                assert_eq!(col.len(), frame.row_count());
                assert!(
                    col.value(frame.row_count())
                        .expect("binary out-of-range read")
                        .is_none()
                );
            }
            sas7bdat::FrameColumn::Date32(col) => {
                saw_primitive = true;
                assert!(column.as_date32().is_some());
                assert_eq!(col.len(), frame.row_count());
                assert!(col.value(frame.row_count()).is_none());
            }
        }
    }

    assert!(saw_primitive, "expected at least one primitive column");

    if !saw_utf8 {
        let mut utf8_sas = open_utf8_fixture();
        let utf8_frame = utf8_sas
            .query()
            .shape(Shape::Frame)
            .collect_frame()
            .expect("collect_frame failed for utf8 fixture");
        assert!(
            utf8_frame
                .columns()
                .iter()
                .any(|column| matches!(column, sas7bdat::FrameColumn::Utf8(_))),
            "expected at least one utf8 column in utf8 fixture"
        );
    }
}

#[test]
fn query_collect_raw_batches_covers_all_rows() {
    let mut sas = open_datetime_fixture();
    let batches = sas
        .query()
        .shape(Shape::Raw)
        .collect_raw_batches(2)
        .expect("collect_raw_batches failed");

    let rows_from_batches: usize = batches.iter().map(sas7bdat::RawRowBatch::row_count).sum();
    let expected_rows = usize::try_from(sas.metadata().row_count).expect("row count fits in usize");
    assert_eq!(rows_from_batches, expected_rows);
    assert!(batches.iter().all(|batch| batch.row_count() <= 2));
}

#[test]
fn collect_raw_batches_fast_covers_all_rows() {
    let mut sas = open_datetime_fixture();
    let batches = sas
        .collect_raw_batches_fast(2, 3)
        .expect("collect_raw_batches_fast failed");

    let rows_from_batches: usize = batches.iter().map(sas7bdat::RawRowBatch::row_count).sum();
    let expected_rows = usize::try_from(sas.metadata().row_count).expect("row count fits in usize");
    assert_eq!(rows_from_batches, expected_rows);
    assert!(batches.iter().all(|batch| batch.row_count() <= 3));
}

fn open_datetime_fixture() -> SasReader<std::fs::File> {
    let path = common::fixture_path("fixtures/raw_data/pandas/datetime.sas7bdat");
    SasReader::open(path).expect("failed to open datetime fixture")
}

fn open_utf8_fixture() -> SasReader<std::fs::File> {
    let path = common::fixture_path("fixtures/raw_data/csharp/charset_utf8.sas7bdat");
    SasReader::open(path).expect("failed to open utf8 fixture")
}
