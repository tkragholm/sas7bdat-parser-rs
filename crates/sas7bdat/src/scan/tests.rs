use super::{
    BatchDecodePlan, SAS_NUMERIC_MISSING_SENTINEL, ScanBuilder,
    batch::{BatchAccumulator, DirectUtf8OwnedMode},
    trim_and_classify_ascii,
};
use crate::{
    columnar::OwnedColumnBuffer,
    metadata::{CompressionKind, LogicalType},
    row::OwnedCellValue,
    test_utils::*,
};
#[cfg(feature = "arrow")]
use arrow_array::{Float64Array, StringArray};
#[cfg(feature = "arrow")]
use arrow_schema::DataType;
use std::{ops::ControlFlow, sync::Arc};

fn make_batch_plan(builder: &ScanBuilder<'_>) -> BatchDecodePlan {
    let row_plan = super::RowDecodePlan::new(builder).expect("row plan");
    BatchDecodePlan::new(builder, row_plan).expect("batch plan")
}

fn assert_trusted_offsets(offsets: &crate::TrustedOffsets, expected: &[i64], data_len: usize) {
    assert_eq!(offsets.as_slice(), expected);
    offsets
        .validate_for_values_len(data_len)
        .expect("scanner should emit valid trusted offsets");
}

#[test]
fn raw_scan_visits_rows_from_fused_pages() {
    let bytes = Arc::<[u8]>::from(make_pages());
    let ds = MockDatasetBuilder::new(bytes)
        .with_row_len(4)
        .with_total_rows(3)
        .with_rows_per_page(1)
        .build();

    let mut rows = Vec::new();
    let stats = ScanBuilder::new(&ds)
        .select(crate::RowSelection::Range {
            start: crate::types::RowIndex(1),
            end: crate::types::RowIndex(3),
        })
        .visit_raw_rows(|row| {
            rows.push((row.row_index, row.bytes.to_vec()));
            Ok(ControlFlow::Continue(()))
        })
        .expect("scan succeeds");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (crate::types::RowIndex(1), b"EFGH".to_vec()));
    assert_eq!(rows[1], (crate::types::RowIndex(2), b"IJKL".to_vec()));
    assert_eq!(stats.rows_seen, 3);
    assert_eq!(stats.rows_emitted, 2);
    assert_eq!(stats.fused_pages, 2);
}

#[test]
fn trim_and_classify_ascii_fast_path_handles_all_space_width_12() {
    let trimmed = trim_and_classify_ascii(b"            ");
    assert!(trimmed.bytes.is_empty());
    assert!(trimmed.is_ascii);
}

#[test]
fn raw_scan_decompresses_rle_rows() {
    let bytes = Arc::<[u8]>::from(make_compressed_page(&[0xC1u8, b'A'], 64, 4));
    let ds = MockDatasetBuilder::new(bytes)
        .with_row_len(4)
        .with_total_rows(1)
        .with_rows_per_page(1)
        .with_compression(CompressionKind::Row)
        .build();

    let mut rows = Vec::new();
    let stats = ScanBuilder::new(&ds)
        .visit_raw_rows(|row| {
            rows.push((row.row_index, row.bytes.to_vec()));
            Ok(ControlFlow::Continue(()))
        })
        .expect("compressed raw scan");
    assert_eq!(rows, vec![(crate::types::RowIndex(0), b"AAAA".to_vec())]);
    assert_eq!(stats.compressed_pages, 1);
    assert_eq!(stats.row_bytes_materialized, 4);
}

fn check_raw_scan(
    ds: &crate::dataset::Dataset,
    expected_rows: &[(crate::types::RowIndex, Vec<u8>)],
) {
    let mut rows = Vec::new();
    let stats = ScanBuilder::new(ds)
        .visit_raw_rows(|row| {
            rows.push((row.row_index, row.bytes.to_vec()));
            Ok(ControlFlow::Continue(()))
        })
        .expect("scan succeeds");

    assert_eq!(rows, expected_rows);
    assert_eq!(stats.indexed_pages, 1);
    assert_eq!(
        stats.rows_emitted,
        u64::try_from(expected_rows.len()).unwrap()
    );
}

#[test]
fn raw_scan_visits_rows_from_indexed_pointer_pages() {
    let bytes = Arc::<[u8]>::from(make_pointer_page(&[b"ABCD", b"EFGH"], 64));
    let ds = MockDatasetBuilder::new(bytes)
        .with_row_len(4)
        .with_total_rows(2)
        .with_rows_per_page(2)
        .build();

    check_raw_scan(
        &ds,
        &[
            (crate::types::RowIndex(0), b"ABCD".to_vec()),
            (crate::types::RowIndex(1), b"EFGH".to_vec()),
        ],
    );
}

#[test]
fn raw_scan_visits_rows_from_mixed_pointer_and_contiguous_page() {
    let bytes = Arc::<[u8]>::from(make_mixed_pointer_page(*b"WXYZ", *b"ABCD", 64));
    let ds = MockDatasetBuilder::new(bytes)
        .with_row_len(4)
        .with_total_rows(2)
        .with_rows_per_page(2)
        .build();

    check_raw_scan(
        &ds,
        &[
            (crate::types::RowIndex(0), b"WXYZ".to_vec()),
            (crate::types::RowIndex(1), b"ABCD".to_vec()),
        ],
    );
}

#[test]
fn typed_row_scan_decodes_projected_cells() {
    let bytes = Arc::<[u8]>::from(make_page(
        0x0100,
        1,
        0,
        &[&make_numeric_text_row(42.0, *b"ABCD")],
        64,
    ));
    let ds = MockDatasetBuilder::new(bytes)
        .with_column("num", LogicalType::Float, 8, 0)
        .with_column("txt", LogicalType::String, 4, 8)
        .with_row_len(12)
        .build();
    let projection = ds
        .projection()
        .column("txt")
        .column("num")
        .build()
        .expect("projection");

    let mut seen = Vec::new();
    let stats = ScanBuilder::new(&ds)
        .with_projection(&projection)
        .visit_rows(|row| {
            seen.push((
                row.row_index(),
                row.get(0).expect("txt").to_owned_value(),
                row.get(1).expect("num").to_owned_value(),
            ));
            Ok(ControlFlow::Continue(()))
        })
        .expect("typed scan");

    assert_eq!(stats.rows_emitted, 1);
    assert_eq!(seen.len(), 1);
    assert!(matches!(seen[0].1, OwnedCellValue::String(ref value) if value == "ABCD"));
    assert!(matches!(seen[0].2, OwnedCellValue::Int64(42)));
}

#[test]
fn typed_lossless_rows_preserve_numeric_bits_and_string_bytes() {
    let mut row = Vec::with_capacity(12);
    let missing_bits = 0x7FF0_0000_0000_0001u64;
    row.extend_from_slice(&missing_bits.to_le_bytes());
    row.extend_from_slice(b"A  ");
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[&row], 64));
    let ds = MockDatasetBuilder::new(bytes)
        .with_column("num", LogicalType::Float, 8, 0)
        .with_column("txt", LogicalType::String, 3, 8)
        .with_row_len(11)
        .build();

    let rows = ScanBuilder::new(&ds)
        .with_decode_mode(crate::DecodeMode::TypedLossless)
        .collect_rows()
        .expect("lossless rows");
    assert_eq!(rows.len(), 1);
    assert!(matches!(
        rows[0].cells[0],
        crate::row::OwnedCellValue::Float64(value) if value.to_bits() == missing_bits
    ));
    assert!(matches!(
        rows[0].cells[1],
        crate::row::OwnedCellValue::Bytes(ref value) if value == b"A  "
    ));
}

fn make_standard_test_dataset(row: &[u8]) -> crate::dataset::Dataset {
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[row], 64));
    MockDatasetBuilder::new(bytes)
        .with_column("num", LogicalType::Float, 8, 0)
        .with_column("txt", LogicalType::String, 4, 8)
        .with_row_len(12)
        .build()
}

fn make_integer_test_dataset(row: &[u8]) -> crate::dataset::Dataset {
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[row], 64));
    MockDatasetBuilder::new(bytes)
        .with_column("num", LogicalType::Integer, 8, 0)
        .with_column("txt", LogicalType::String, 4, 8)
        .with_row_len(12)
        .build()
}

fn make_windows1252_test_dataset(row: &[u8]) -> crate::dataset::Dataset {
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[row], 64));
    MockDatasetBuilder::new(bytes)
        .with_column("num", LogicalType::Float, 8, 0)
        .with_column("txt", LogicalType::String, 4, 8)
        .with_row_len(12)
        .with_encoding(Some("WINDOWS-1252".to_owned()))
        .build()
}

#[test]
fn collect_rows_materializes_owned_values() {
    let row = make_numeric_text_row(7.0, *b"ZX  ");
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[&row], 64));
    let ds = MockDatasetBuilder::new(bytes)
        .with_column("num", LogicalType::Float, 8, 0)
        .with_column("txt", LogicalType::String, 4, 8)
        .with_row_len(12)
        .build();

    let rows = ScanBuilder::new(&ds).collect_rows().expect("owned rows");
    assert_eq!(rows.len(), 1);
    assert!(matches!(
        rows[0].cells[0],
        crate::row::OwnedCellValue::Int64(7)
    ));
    assert!(matches!(
        rows[0].cells[1],
        crate::row::OwnedCellValue::String(ref value) if value == "ZX"
    ));
}

#[test]
fn trim_mode_preserve_keeps_spaces_for_non_blank_values() {
    let row = make_numeric_text_row(7.0, *b" A  ");
    let ds = make_standard_test_dataset(&row);

    let rows = ScanBuilder::new(&ds)
        .with_string_options(crate::StringDecodeOptions {
            trim_mode: crate::TrimMode::Preserve,
            ..crate::StringDecodeOptions::default()
        })
        .collect_rows()
        .expect("owned rows");

    assert!(matches!(
        rows[0].cells[1],
        crate::row::OwnedCellValue::String(ref value) if value == " A  "
    ));
}

#[test]
fn trim_mode_strip_removes_leading_and_trailing_spaces() {
    let row = make_numeric_text_row(7.0, *b" A  ");
    let ds = make_standard_test_dataset(&row);

    let rows = ScanBuilder::new(&ds)
        .with_string_options(crate::StringDecodeOptions {
            trim_mode: crate::TrimMode::Strip,
            ..crate::StringDecodeOptions::default()
        })
        .collect_rows()
        .expect("owned rows");

    assert!(matches!(
        rows[0].cells[1],
        crate::row::OwnedCellValue::String(ref value) if value == "A"
    ));
}

#[test]
fn trim_mode_preserve_blank_still_canonicalizes_to_empty() {
    let row = make_numeric_text_row(7.0, *b"    ");
    let ds = make_standard_test_dataset(&row);

    let rows = ScanBuilder::new(&ds)
        .with_string_options(crate::StringDecodeOptions {
            trim_mode: crate::TrimMode::Preserve,
            ..crate::StringDecodeOptions::default()
        })
        .collect_rows()
        .expect("owned rows");

    assert!(matches!(
        rows[0].cells[1],
        crate::row::OwnedCellValue::String(ref value) if value.is_empty()
    ));
}

#[test]
fn dictionary_staging_policy_controls_lookup_construction() {
    let row = make_numeric_text_row(1.0, *b"A  \xC4");
    let ds = make_windows1252_test_dataset(&row);

    let plan_off = make_batch_plan(&ScanBuilder::new(&ds).with_string_options(
        crate::StringDecodeOptions {
            trim_mode: crate::TrimMode::RTrim,
            utf8_validation: crate::Utf8ValidationMode::Strict,
            mojibake_fix: crate::MojibakePolicy::Auto,
            dictionary_staging: crate::DictionaryStaging::Off,
        },
    ));
    let acc_off = BatchAccumulator::new(plan_off, 1, 1);
    assert!(!acc_off.has_staged_string_lookup_for(1));

    let plan_on = make_batch_plan(&ScanBuilder::new(&ds).with_string_options(
        crate::StringDecodeOptions {
            trim_mode: crate::TrimMode::Preserve,
            utf8_validation: crate::Utf8ValidationMode::Strict,
            mojibake_fix: crate::MojibakePolicy::Auto,
            dictionary_staging: crate::DictionaryStaging::On,
        },
    ));
    let acc_on = BatchAccumulator::new(plan_on, 1, 1);
    assert!(acc_on.has_staged_string_lookup_for(1));

    let plan_auto = make_batch_plan(&ScanBuilder::new(&ds).with_string_options(
        crate::StringDecodeOptions {
            trim_mode: crate::TrimMode::Preserve,
            utf8_validation: crate::Utf8ValidationMode::Strict,
            mojibake_fix: crate::MojibakePolicy::Auto,
            dictionary_staging: crate::DictionaryStaging::Auto,
        },
    ));
    let acc_auto = BatchAccumulator::new(plan_auto, 1, 1);
    assert!(!acc_auto.has_staged_string_lookup_for(1));
}

#[test]
fn collect_batches_materializes_columnar_values() {
    let row_a = make_numeric_text_row(1.5, *b"AA  ");
    let row_b = make_numeric_text_row(2.0, *b"BBBB");
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 2, 0, &[&row_a, &row_b], 64));
    let ds = MockDatasetBuilder::new(bytes)
        .with_column("num", LogicalType::Float, 8, 0)
        .with_column("txt", LogicalType::String, 4, 8)
        .with_row_len(12)
        .build();

    let batches = ScanBuilder::new(&ds)
        .with_batch_hint(crate::BatchHint::Rows(2))
        .collect_batches()
        .expect("columnar batches");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].row_base, crate::types::RowIndex(0));
    assert_eq!(batches[0].row_count, 2);

    match &batches[0].columns[0] {
        OwnedColumnBuffer::F64 { values, valid } => {
            assert_eq!(values, &vec![1.5, 2.0]);
            assert!(valid.is_none());
        }
        other => panic!("unexpected numeric batch column: {other:?}"),
    }
    match &batches[0].columns[1] {
        OwnedColumnBuffer::Utf8 {
            offsets,
            data,
            valid,
            dictionary_ids,
        } => {
            assert_trusted_offsets(offsets, &[0, 2, 6], data.len());
            assert_eq!(data, b"AABBBB");
            assert!(valid.is_none());
            assert!(dictionary_ids.is_none());
        }
        other => panic!("unexpected utf8 batch column: {other:?}"),
    }
}

#[cfg(feature = "arrow")]
#[test]
fn collect_batches_to_arrow_record_batch_round_trips() {
    let row = make_numeric_text_row(7.5, *b"AB  ");
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[&row], 64));
    let ds = MockDatasetBuilder::new(bytes)
        .with_column("num", LogicalType::Float, 8, 0)
        .with_column("txt", LogicalType::String, 4, 8)
        .with_row_len(12)
        .build();

    let builder = ScanBuilder::new(&ds);
    let schema = builder.arrow_schema().expect("arrow schema");
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).data_type(), &DataType::Float64);
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);

    let batch = builder
        .with_batch_hint(crate::BatchHint::Rows(1))
        .collect_batches()
        .expect("columnar batch")
        .into_iter()
        .next()
        .expect("one batch");
    let record_batch = batch
        .into_arrow_record_batch(schema)
        .expect("arrow record batch");

    assert_eq!(record_batch.num_columns(), 2);
    let num = record_batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("float array");
    assert!((num.value(0) - 7.5).abs() < f64::EPSILON);
    let txt = record_batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string array");
    assert_eq!(txt.value(0), "AB");
}

#[cfg(feature = "arrow")]
#[test]
fn collect_arrow_batches_returns_record_batches() {
    let row = make_numeric_text_row(3.25, *b"CD  ");
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[&row], 64));
    let ds = MockDatasetBuilder::new(bytes)
        .with_column("num", LogicalType::Float, 8, 0)
        .with_column("txt", LogicalType::String, 4, 8)
        .with_row_len(12)
        .build();

    let record_batches = ScanBuilder::new(&ds)
        .with_batch_hint(crate::BatchHint::Rows(1))
        .collect_arrow_batches()
        .expect("arrow batches");
    assert_eq!(record_batches.len(), 1);
    let batch = &record_batches[0];
    let num = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("float array");
    assert!((num.value(0) - 3.25).abs() < f64::EPSILON);
}

#[cfg(feature = "arrow")]
#[test]
fn visit_arrow_batches_streams_record_batches() {
    let row_a = make_numeric_text_row(7.0, *b"EF  ");
    let row_b = make_numeric_text_row(9.0, *b"GH  ");
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 2, 0, &[&row_a, &row_b], 64));
    let ds = MockDatasetBuilder::new(bytes)
        .with_column("num", LogicalType::Float, 8, 0)
        .with_column("txt", LogicalType::String, 4, 8)
        .with_row_len(12)
        .build();

    let mut seen = Vec::new();
    let stats = ScanBuilder::new(&ds)
        .with_batch_hint(crate::BatchHint::Rows(1))
        .visit_arrow_batches(|batch| {
            seen.push((batch.num_rows(), batch.num_columns()));
            Ok(ControlFlow::Break(()))
        })
        .expect("arrow batch scan");

    assert_eq!(seen, vec![(1, 2)]);
    assert_eq!(stats.decode_batches, 1);
    assert_eq!(stats.rows_emitted, 1);
}

#[test]
fn collect_batches_stops_at_row_limit() {
    let row_a = make_numeric_text_row(1.0, *b"AA  ");
    let row_b = make_numeric_text_row(2.0, *b"BB  ");
    let row_c = make_numeric_text_row(3.0, *b"CC  ");
    let row_d = make_numeric_text_row(4.0, *b"DD  ");
    let bytes = Arc::<[u8]>::from(make_page(
        0x0100,
        4,
        0,
        &[&row_a, &row_b, &row_c, &row_d],
        96,
    ));
    let ds = MockDatasetBuilder::new(bytes)
        .with_column("num", LogicalType::Float, 8, 0)
        .with_column("txt", LogicalType::String, 4, 8)
        .with_row_len(12)
        .with_total_rows(4)
        .with_rows_per_page(4)
        .build();

    let batches = ScanBuilder::new(&ds)
        .limit(3)
        .with_batch_hint(crate::BatchHint::Rows(2))
        .collect_batches()
        .expect("limited batches");

    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].row_base, crate::types::RowIndex(0));
    assert_eq!(batches[0].row_count, 2);
    assert_eq!(batches[1].row_base, crate::types::RowIndex(2));
    assert_eq!(batches[1].row_count, 1);

    match &batches[1].columns[1] {
        OwnedColumnBuffer::Utf8 { data, .. } => assert_eq!(data, b"CC"),
        other => panic!("unexpected limited utf8 batch column: {other:?}"),
    }
}

#[test]
fn collect_batches_parallel_in_memory_matches_serial_batches() {
    let row_a = make_numeric_text_row(1.0, *b"AA  ");
    let row_b = make_numeric_text_row(2.0, *b"BB  ");
    let row_c = make_numeric_text_row(3.0, *b"CC  ");
    let row_d = make_numeric_text_row(4.0, *b"DD  ");
    let mut bytes = make_page(0x0100, 2, 0, &[&row_a, &row_b], 64);
    bytes.extend_from_slice(&make_page(0x0100, 2, 0, &[&row_c, &row_d], 64));
    let ds = MockDatasetBuilder::new(Arc::<[u8]>::from(bytes))
        .with_column("num", LogicalType::Float, 8, 0)
        .with_column("txt", LogicalType::String, 4, 8)
        .with_row_len(12)
        .with_total_rows(4)
        .with_rows_per_page(2)
        .build();

    let serial = ScanBuilder::new(&ds)
        .with_batch_hint(crate::BatchHint::Rows(2))
        .collect_batches()
        .expect("serial batches");
    let parallel = ScanBuilder::new(&ds)
        .with_batch_hint(crate::BatchHint::Rows(2))
        .with_parallelism(crate::Parallelism::Threads(2))
        .collect_batches()
        .expect("parallel batches");

    assert_eq!(parallel.len(), serial.len());
    for (parallel_batch, serial_batch) in parallel.iter().zip(serial.iter()) {
        assert_eq!(parallel_batch.row_base, serial_batch.row_base);
        assert_eq!(parallel_batch.row_count, serial_batch.row_count);
        match (&parallel_batch.columns[0], &serial_batch.columns[0]) {
            (
                OwnedColumnBuffer::F64 {
                    values: parallel_values,
                    valid: parallel_valid,
                },
                OwnedColumnBuffer::F64 {
                    values: serial_values,
                    valid: serial_valid,
                },
            ) => {
                assert_eq!(parallel_values, serial_values);
                assert_eq!(parallel_valid, serial_valid);
            }
            other => panic!("unexpected numeric batch columns: {other:?}"),
        }
        match (&parallel_batch.columns[1], &serial_batch.columns[1]) {
            (
                OwnedColumnBuffer::Utf8 {
                    offsets: parallel_offsets,
                    data: parallel_data,
                    valid: parallel_valid,
                    dictionary_ids: parallel_dict_ids,
                },
                OwnedColumnBuffer::Utf8 {
                    offsets: serial_offsets,
                    data: serial_data,
                    valid: serial_valid,
                    dictionary_ids: serial_dict_ids,
                },
            ) => {
                assert_eq!(parallel_offsets, serial_offsets);
                assert_eq!(parallel_data, serial_data);
                assert_eq!(parallel_valid, serial_valid);
                assert_eq!(parallel_dict_ids, serial_dict_ids);
            }
            other => panic!("unexpected utf8 batch columns: {other:?}"),
        }
    }
}

#[test]
fn visit_batches_parallel_in_memory_matches_serial_order() {
    let row_a = make_numeric_text_row(1.0, *b"AA  ");
    let row_b = make_numeric_text_row(2.0, *b"BB  ");
    let row_c = make_numeric_text_row(3.0, *b"CC  ");
    let row_d = make_numeric_text_row(4.0, *b"DD  ");
    let mut bytes = make_page(0x0100, 2, 0, &[&row_a, &row_b], 64);
    bytes.extend_from_slice(&make_page(0x0100, 2, 0, &[&row_c, &row_d], 64));
    let ds = MockDatasetBuilder::new(Arc::<[u8]>::from(bytes))
        .with_column("num", LogicalType::Float, 8, 0)
        .with_column("txt", LogicalType::String, 4, 8)
        .with_row_len(12)
        .with_total_rows(4)
        .with_rows_per_page(2)
        .build();

    let mut serial_seen = Vec::new();
    ScanBuilder::new(&ds)
        .with_batch_hint(crate::BatchHint::Rows(2))
        .visit_batches(|batch| {
            serial_seen.push((
                batch.row_base,
                batch.row_count,
                read_utf8_column(&batch.columns[1]),
            ));
            Ok(ControlFlow::Continue(()))
        })
        .expect("serial visit");

    let mut parallel_seen = Vec::new();
    let stats = ScanBuilder::new(&ds)
        .with_batch_hint(crate::BatchHint::Rows(2))
        .with_parallelism(crate::Parallelism::Threads(2))
        .visit_batches(|batch| {
            parallel_seen.push((
                batch.row_base,
                batch.row_count,
                read_utf8_column(&batch.columns[1]),
            ));
            Ok(ControlFlow::Continue(()))
        })
        .expect("parallel visit");

    assert_eq!(parallel_seen, serial_seen);
    assert_eq!(stats.decode_batches, 2);
    assert_eq!(stats.rows_emitted, 4);
}

#[test]
fn visit_owned_batches_parallel_in_memory_matches_serial_order() {
    let row_a = make_numeric_text_row(1.0, *b"AA  ");
    let row_b = make_numeric_text_row(2.0, *b"BB  ");
    let row_c = make_numeric_text_row(3.0, *b"CC  ");
    let row_d = make_numeric_text_row(4.0, *b"DD  ");
    let mut bytes = make_page(0x0100, 2, 0, &[&row_a, &row_b], 64);
    bytes.extend_from_slice(&make_page(0x0100, 2, 0, &[&row_c, &row_d], 64));
    let ds = MockDatasetBuilder::new(Arc::<[u8]>::from(bytes))
        .with_column("num", LogicalType::Float, 8, 0)
        .with_column("txt", LogicalType::String, 4, 8)
        .with_row_len(12)
        .with_total_rows(4)
        .with_rows_per_page(2)
        .build();

    let mut serial_seen = Vec::new();
    ScanBuilder::new(&ds)
        .with_batch_hint(crate::BatchHint::Rows(2))
        .visit_owned_batches(|batch| {
            serial_seen.push((
                batch.row_base,
                batch.row_count,
                match &batch.columns[1] {
                    OwnedColumnBuffer::Utf8 { data, .. } => data.clone(),
                    other => panic!("unexpected utf8 batch column: {other:?}"),
                },
            ));
            Ok(ControlFlow::Continue(()))
        })
        .expect("serial owned visit");

    let mut parallel_seen = Vec::new();
    let stats = ScanBuilder::new(&ds)
        .with_batch_hint(crate::BatchHint::Rows(2))
        .with_parallelism(crate::Parallelism::Threads(2))
        .visit_owned_batches(|batch| {
            parallel_seen.push((
                batch.row_base,
                batch.row_count,
                match &batch.columns[1] {
                    OwnedColumnBuffer::Utf8 { data, .. } => data.clone(),
                    other => panic!("unexpected utf8 batch column: {other:?}"),
                },
            ));
            Ok(ControlFlow::Continue(()))
        })
        .expect("parallel owned visit");

    assert_eq!(parallel_seen, serial_seen);
    assert_eq!(stats.decode_batches, 2);
    assert_eq!(stats.rows_emitted, 4);
}

#[test]
fn batch_decode_plan_compiles_mixed_projected_families() {
    let row = {
        let mut bytes = Vec::with_capacity(16);
        bytes.extend_from_slice(&1.5f64.to_le_bytes());
        bytes.extend_from_slice(b"ABCD");
        bytes.extend_from_slice(&7.0f64.to_le_bytes()[..4]);
        bytes
    };
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[&row], 64));
    let ds = MockDatasetBuilder::new(bytes)
        .with_column("num", LogicalType::Float, 8, 0)
        .with_column("txt", LogicalType::String, 4, 8)
        .with_column("id", LogicalType::Integer, 4, 12)
        .with_row_len(16)
        .build();

    let projection = ds
        .projection()
        .column("num")
        .column("txt")
        .column("id")
        .build()
        .expect("projection");
    let builder = ScanBuilder::new(&ds)
        .with_projection(&projection)
        .with_batch_hint(crate::BatchHint::Rows(1));
    let plan = make_batch_plan(&builder);

    assert_eq!(plan.families.staged_numeric, vec![0, 2]);
    assert!(plan.families.direct_utf8_borrowed.is_empty());
    assert_eq!(plan.families.direct_utf8_owned, vec![1]);
    assert_eq!(
        plan.direct_utf8_owned_mode,
        Some(DirectUtf8OwnedMode::Utf8Lenient)
    );
    assert!(plan.families.direct_numeric.is_empty());
    assert!(plan.families.direct_raw_bytes.is_empty());
    assert!(plan.families.fallback.is_empty());
    assert!(!plan.all_columns_staged_numeric());
    assert!(!plan.needs_owned_string_scratch());
}

#[test]
fn batch_decode_plan_compiles_lossless_raw_bytes_family() {
    let row = make_numeric_text_row(42.0, *b"ZX  ");
    let ds = make_standard_test_dataset(&row);

    let plan = make_batch_plan(
        &ScanBuilder::new(&ds)
            .with_decode_mode(crate::DecodeMode::TypedLossless)
            .with_batch_hint(crate::BatchHint::Rows(1)),
    );

    assert_eq!(plan.families.staged_numeric, vec![0]);
    assert_eq!(plan.families.direct_raw_bytes, vec![1]);
    assert!(plan.families.direct_numeric.is_empty());
    assert!(plan.families.direct_utf8_borrowed.is_empty());
    assert!(plan.families.direct_utf8_owned.is_empty());
    assert!(plan.direct_utf8_owned_mode.is_none());
    assert!(plan.families.fallback.is_empty());
    assert!(!plan.all_columns_staged_numeric());
    assert!(!plan.needs_owned_string_scratch());
}

#[test]
fn batch_decode_plan_compiles_strict_utf8_borrowed_family() {
    let row = make_numeric_text_row(1.0, *b"pear");
    let ds = make_standard_test_dataset(&row);

    let plan = make_batch_plan(
        &ScanBuilder::new(&ds)
            .with_string_options(crate::StringDecodeOptions {
                trim_mode: crate::TrimMode::RTrim,
                utf8_validation: crate::Utf8ValidationMode::Strict,
                mojibake_fix: crate::MojibakePolicy::Auto,
                dictionary_staging: crate::DictionaryStaging::Auto,
            })
            .with_batch_hint(crate::BatchHint::Rows(1)),
    );

    assert_eq!(plan.families.staged_numeric, vec![0]);
    assert_eq!(plan.families.direct_utf8_borrowed, vec![1]);
    assert!(plan.families.direct_utf8_owned.is_empty());
    assert!(plan.direct_utf8_owned_mode.is_none());
    assert!(plan.families.fallback.is_empty());
    assert!(!plan.needs_owned_string_scratch());
}

fn make_single_byte_test_dataset(compression: CompressionKind) -> crate::dataset::Dataset {
    let row = {
        let mut row = [0u8; 9];
        row[..8].copy_from_slice(&1.0f64.to_bits().to_le_bytes());
        row[8] = b'B';
        row
    };
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[&row], 64));
    MockDatasetBuilder::new(bytes)
        .with_column("num", LogicalType::Float, 8, 0)
        .with_column("code", LogicalType::String, 1, 8)
        .with_row_len(9)
        .with_compression(compression)
        .with_encoding(Some("ISO-8859-1".to_owned()))
        .build()
}

#[test]
fn batch_decode_plan_does_not_compile_single_byte_utf8_family_for_uncompressed_scan() {
    let ds = make_single_byte_test_dataset(CompressionKind::None);

    let plan = make_batch_plan(&ScanBuilder::new(&ds).with_batch_hint(crate::BatchHint::Rows(1)));

    assert_eq!(plan.families.staged_numeric, vec![0]);
    assert!(plan.families.direct_utf8_single_byte.is_empty());
    assert!(plan.families.direct_utf8_borrowed.is_empty());
    assert_eq!(plan.families.direct_utf8_owned, vec![1]);
    assert_eq!(
        plan.direct_utf8_owned_mode,
        Some(DirectUtf8OwnedMode::EncodedLenient)
    );
    assert!(plan.families.fallback.is_empty());
}

#[test]
fn batch_decode_plan_compiles_single_byte_utf8_family_for_compressed_scan() {
    let ds = make_single_byte_test_dataset(CompressionKind::Row);

    let plan = make_batch_plan(&ScanBuilder::new(&ds).with_batch_hint(crate::BatchHint::Rows(1)));

    assert_eq!(plan.families.staged_numeric, vec![0]);
    assert_eq!(plan.families.direct_utf8_single_byte, vec![1]);
    assert!(plan.families.direct_utf8_borrowed.is_empty());
    assert!(plan.families.direct_utf8_owned.is_empty());
    assert!(plan.families.fallback.is_empty());
}

fn make_ascii_test_dataset() -> crate::dataset::Dataset {
    let row = make_numeric_text_row(1.0, *b"pear");
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[&row], 64));
    MockDatasetBuilder::new(bytes)
        .with_column("num", LogicalType::Float, 8, 0)
        .with_column("txt", LogicalType::String, 4, 8)
        .with_row_len(12)
        .with_encoding(Some("WINDOWS-1252".to_owned()))
        .build()
}

#[test]
fn typed_rows_decode_ascii_strings_without_utf8_encoding() {
    let ds = make_ascii_test_dataset();

    let rows = ScanBuilder::new(&ds).collect_rows().expect("rows");
    assert!(matches!(
        rows[0].cells[1],
        OwnedCellValue::String(ref value) if value == "pear"
    ));
}

#[test]
fn collect_batches_decode_ascii_strings_without_utf8_encoding() {
    let ds = make_ascii_test_dataset();

    let batches = ScanBuilder::new(&ds)
        .with_batch_hint(crate::BatchHint::Rows(1))
        .collect_batches()
        .expect("batches");
    assert_eq!(batches.len(), 1);
    match &batches[0].columns[1] {
        OwnedColumnBuffer::Utf8 {
            offsets,
            data,
            valid,
            dictionary_ids,
        } => {
            assert_trusted_offsets(offsets, &[0, 4], data.len());
            assert_eq!(data, b"pear");
            assert!(valid.is_none());
            assert!(dictionary_ids.is_none() || dictionary_ids.as_deref() == Some(&[u32::MAX][..]));
        }
        other => panic!("unexpected utf8 batch column: {other:?}"),
    }
}

#[test]
fn collect_batches_typed_integer_errors_for_fractional_values() {
    let row = make_numeric_text_row(1.5, *b"INT ");
    let ds = make_integer_test_dataset(&row);

    // Row-level decode stays value-typed: a fractional cell is simply a Float64.
    let rows = ScanBuilder::new(&ds).collect_rows().expect("rows");
    assert!(
        matches!(rows[0].cells[0], OwnedCellValue::Float64(value) if (value - 1.5).abs() < f64::EPSILON)
    );

    // Batch decode enforces the declared column type: an Integer column (only
    // assignable via a schema override) must error on fractional values rather
    // than silently widening the batch to F64 and flapping the dtype.
    let err = ScanBuilder::new(&ds)
        .with_batch_hint(crate::BatchHint::Rows(1))
        .collect_batches()
        .expect_err("fractional value violates the declared Integer type");
    assert!(err.to_string().contains("declared Integer"));
}

#[test]
fn collect_batches_typed_lossless_uses_f64_and_raw_bytes() {
    let row = make_numeric_text_row(42.0, *b"ZX  ");
    let ds = make_integer_test_dataset(&row);

    let batches = ScanBuilder::new(&ds)
        .with_decode_mode(crate::DecodeMode::TypedLossless)
        .collect_batches()
        .expect("lossless batches");
    assert_eq!(batches.len(), 1);
    match &batches[0].columns[0] {
        OwnedColumnBuffer::F64 { values, valid } => {
            assert_eq!(values, &vec![42.0]);
            assert!(valid.is_none());
        }
        other => panic!("unexpected lossless numeric batch column: {other:?}"),
    }
    match &batches[0].columns[1] {
        OwnedColumnBuffer::RawBytes {
            offsets,
            data,
            valid,
        } => {
            assert_trusted_offsets(offsets, &[0, 4], data.len());
            assert_eq!(data, b"ZX  ");
            assert!(valid.is_none());
        }
        other => panic!("unexpected lossless raw-bytes batch column: {other:?}"),
    }
}

#[test]
fn collect_batches_staged_f64_preserves_missing_validity() {
    let mut row_a = Vec::with_capacity(8);
    row_a.extend_from_slice(&1.25f64.to_le_bytes());
    let mut row_b = Vec::with_capacity(8);
    row_b.extend_from_slice(&SAS_NUMERIC_MISSING_SENTINEL.to_le_bytes());
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 2, 0, &[&row_a, &row_b], 64));
    let ds = MockDatasetBuilder::new(bytes)
        .with_column("num", LogicalType::Float, 8, 0)
        .with_row_len(8)
        .build();

    let batches = ScanBuilder::new(&ds)
        .with_batch_hint(crate::BatchHint::Rows(2))
        .collect_batches()
        .expect("batches");
    assert_eq!(batches.len(), 1);
    match &batches[0].columns[0] {
        OwnedColumnBuffer::F64 { values, valid } => {
            assert_eq!(values, &vec![1.25, 0.0]);
            // Bit-packed validity: row 0 valid (bit 0 = 1), row 1 null (bit 1 = 0) → word = 0b01 = 1.
            assert_eq!(valid.as_deref(), Some(&[1u64][..]));
        }
        other => panic!("unexpected staged f64 batch column: {other:?}"),
    }
}

#[test]
fn visit_batches_streams_projected_columnar_views() {
    let row_a = make_numeric_text_row(10.0, *b"ABCD");
    let row_b = make_numeric_text_row(20.0, *b"EF  ");
    let bytes = Arc::<[u8]>::from(make_pointer_page(&[&row_a, &row_b], 64));
    let ds = MockDatasetBuilder::new(bytes)
        .with_column("num", LogicalType::Float, 8, 0)
        .with_column("txt", LogicalType::String, 4, 8)
        .with_row_len(12)
        .build();
    let projection = ds.projection().column("txt").build().expect("projection");

    let mut seen = Vec::new();
    let stats = ScanBuilder::new(&ds)
        .with_projection(&projection)
        .with_batch_hint(crate::BatchHint::Rows(1))
        .visit_batches(|batch| {
            seen.push((
                batch.row_base,
                batch.row_count,
                read_utf8_column(&batch.columns[0]),
            ));
            Ok(ControlFlow::Continue(()))
        })
        .expect("batch scan");

    assert_eq!(
        seen,
        vec![
            (crate::types::RowIndex(0), 1, vec!["ABCD".to_owned()]),
            (crate::types::RowIndex(1), 1, vec!["EF".to_owned()]),
        ]
    );
    assert_eq!(stats.decode_batches, 2);
    assert_eq!(stats.pages_seen, 1);
}

#[test]
fn collect_rows_decodes_compressed_string_rows() {
    let bytes = Arc::<[u8]>::from(make_compressed_page(&[0xC1u8, b'Z'], 64, 4));
    let ds = MockDatasetBuilder::new(bytes)
        .with_column("txt", LogicalType::String, 4, 0)
        .with_row_len(4)
        .with_total_rows(1)
        .with_rows_per_page(1)
        .with_compression(CompressionKind::Row)
        .build();

    let rows = ScanBuilder::new(&ds)
        .collect_rows()
        .expect("typed compressed rows");
    assert_eq!(rows.len(), 1);
    assert!(matches!(
        rows[0].cells[0],
        crate::row::OwnedCellValue::String(ref value) if value == "ZZZZ"
    ));
}

#[test]
fn collect_batches_decodes_windows1252_single_byte_compressed_row() {
    let bytes = Arc::<[u8]>::from(make_compressed_page(&[0xC1u8, 0x96], 64, 4));
    let ds = MockDatasetBuilder::new(bytes)
        .with_column("txt", LogicalType::String, 1, 0)
        .with_row_len(4)
        .with_total_rows(1)
        .with_rows_per_page(1)
        .with_compression(CompressionKind::Row)
        .with_encoding(Some("WINDOWS-1252".to_owned()))
        .build();

    let batches = ScanBuilder::new(&ds)
        .with_string_options(crate::StringDecodeOptions {
            utf8_validation: crate::Utf8ValidationMode::Strict,
            ..crate::StringDecodeOptions::default()
        })
        .with_batch_hint(crate::BatchHint::Rows(1))
        .collect_batches()
        .expect("typed compressed batches");
    assert_eq!(batches.len(), 1);
    match &batches[0].columns[0] {
        OwnedColumnBuffer::Utf8 {
            offsets,
            data,
            valid,
            dictionary_ids,
        } => {
            assert_trusted_offsets(offsets, &[0, 3], data.len());
            assert_eq!(data, &[0xE2, 0x80, 0x93]);
            assert!(valid.is_none());
            assert!(dictionary_ids.is_none());
        }
        other => panic!("unexpected utf8 batch column: {other:?}"),
    }
}

#[test]
fn collect_batches_windows1252_single_byte_strict_rejects_undefined() {
    let bytes = Arc::<[u8]>::from(make_compressed_page(&[0xC1u8, 0x81], 64, 4));
    let ds = MockDatasetBuilder::new(bytes)
        .with_column("txt", LogicalType::String, 1, 0)
        .with_row_len(4)
        .with_total_rows(1)
        .with_rows_per_page(1)
        .with_compression(CompressionKind::Row)
        .with_encoding(Some("WINDOWS-1252".to_owned()))
        .build();

    let err = ScanBuilder::new(&ds)
        .with_string_options(crate::StringDecodeOptions {
            utf8_validation: crate::Utf8ValidationMode::Strict,
            ..crate::StringDecodeOptions::default()
        })
        .with_batch_hint(crate::BatchHint::Rows(1))
        .collect_batches()
        .expect_err("strict windows-1252 should fail on undefined byte");
    assert!(err.to_string().contains("strict validation"));
}

fn make_pages() -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend(make_page(0x0100, 2, 0, &[b"ABCD", b"EFGH"], 64));
    bytes.extend(make_page(0x0200, 0, 0, &[b"IJKL"], 64));
    bytes
}

// ─── schema overrides: integer-coded numeric columns ────────────────────────

/// One 64-byte data page holding up to 5 little-endian f64 rows.
fn make_f64_page(values: &[f64]) -> Vec<u8> {
    assert!(values.len() <= 5, "64-byte page fits at most 5 f64 rows");
    let rows: Vec<[u8; 8]> = values.iter().map(|value| value.to_le_bytes()).collect();
    let row_refs: Vec<&[u8]> = rows.iter().map(|row| &row[..]).collect();
    make_page(
        0x0100,
        u16::try_from(values.len()).expect("row count"),
        0,
        &row_refs,
        64,
    )
}

fn make_f64_dataset(values: &[f64]) -> crate::Dataset {
    let total_rows = u64::try_from(values.len()).expect("rows");
    MockDatasetBuilder::new(Arc::<[u8]>::from(make_f64_page(values)))
        .with_column("CODE", LogicalType::Float, 8, 0)
        .with_row_len(8)
        .with_total_rows(total_rows)
        .with_rows_per_page(total_rows)
        .build()
}

#[test]
fn integer_override_emits_i64_column() {
    let mut ds = make_f64_dataset(&[1.0, 2.0, 5100.0]);
    ds.apply_schema_overrides([("CODE", LogicalType::Integer)])
        .expect("numeric override applies");
    assert_eq!(ds.columns()[0].logical_type, LogicalType::Integer);

    let batches = ScanBuilder::new(&ds)
        .collect_batches()
        .expect("integral values satisfy the Integer override");
    let all: Vec<i64> = batches
        .iter()
        .flat_map(|batch| match &batch.columns[0] {
            OwnedColumnBuffer::I64 { values, valid } => {
                assert!(valid.is_none(), "no missing values expected");
                values.clone()
            }
            other => panic!("expected I64 buffer under Integer override, got {other:?}"),
        })
        .collect();
    assert_eq!(all, vec![1, 2, 5100]);
}

#[cfg(feature = "arrow")]
#[test]
fn integer_override_declares_int64_in_arrow_schema() {
    let mut ds = make_f64_dataset(&[1.0, 2.0]);
    ds.apply_schema_overrides([("CODE", LogicalType::Integer)])
        .expect("numeric override applies");
    let schema = ScanBuilder::new(&ds).arrow_schema().expect("arrow schema");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
}

#[test]
fn integer_override_errors_on_fractional_value() {
    let mut ds = make_f64_dataset(&[1.0, 64.5, 2.0]);
    ds.apply_schema_overrides([("CODE", LogicalType::Integer)])
        .expect("numeric override applies");

    let err = ScanBuilder::new(&ds)
        .collect_batches()
        .expect_err("fractional value must violate the Integer override");
    let message = err.to_string();
    assert!(
        message.contains("CODE"),
        "error names the column: {message}"
    );
    assert!(message.contains("64.5"), "error shows the value: {message}");
    assert!(
        message.contains("row 1"),
        "error locates the row: {message}"
    );

    // The borrowed-batch path enforces the same contract.
    let err = ScanBuilder::new(&ds)
        .visit_batches(|_| Ok(ControlFlow::Continue(())))
        .expect_err("borrowed path must also enforce the override");
    assert!(err.to_string().contains("CODE"));
}

#[test]
fn integer_override_keeps_missing_values_null() {
    let missing = f64::from_bits(SAS_NUMERIC_MISSING_SENTINEL);
    let mut ds = make_f64_dataset(&[7.0, missing, 9.0]);
    ds.apply_schema_overrides([("CODE", LogicalType::Integer)])
        .expect("numeric override applies");

    let batches = ScanBuilder::new(&ds)
        .collect_batches()
        .expect("missing values are nulls, not violations");
    let batch = &batches[0];
    let OwnedColumnBuffer::I64 { values, valid } = &batch.columns[0] else {
        panic!("expected I64 buffer, got {:?}", batch.columns[0]);
    };
    assert_eq!(values[0], 7);
    assert_eq!(values[2], 9);
    let valid = valid
        .as_ref()
        .expect("missing row produces a validity mask");
    assert_eq!(valid[0] & 0b111, 0b101, "row 1 is null, rows 0 and 2 valid");
}

#[test]
fn schema_overrides_ignore_unknown_columns_and_reject_char_columns() {
    let mut ds = make_f64_dataset(&[1.0]);
    ds.apply_schema_overrides([("NOT_IN_FILE", LogicalType::Integer)])
        .expect("unknown columns are skipped so catalogs can be applied wholesale");
    assert_eq!(ds.columns()[0].logical_type, LogicalType::Float);

    let bytes = Arc::<[u8]>::from(make_pages());
    let mut char_ds = MockDatasetBuilder::new(bytes)
        .with_column("txt", LogicalType::String, 4, 0)
        .with_row_len(4)
        .with_total_rows(3)
        .with_rows_per_page(1)
        .build();
    let err = char_ds
        .apply_schema_overrides([("txt", LogicalType::Integer)])
        .expect_err("char column cannot be re-typed as Integer");
    assert!(err.to_string().contains("txt"));
}
