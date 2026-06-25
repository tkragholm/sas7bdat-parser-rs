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
            assert_eq!(values.len(), 2);
            // Exact bit-pattern check (1.25 is exactly representable); avoids a float_cmp lint.
            assert_eq!(values[0].to_bits(), 1.25_f64.to_bits());
            // Missing cells now preserve their raw SAS bits (validity marks them),
            // so bindings can recover special-missing tags rather than seeing 0.0.
            assert_eq!(values[1].to_bits(), SAS_NUMERIC_MISSING_SENTINEL);
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

// ─── column-major (collect_batches_columnar) parity ─────────────────────────

/// One contiguous data page of two-column little-endian f64 rows.
fn make_2col_f64_page(rows: &[[f64; 2]], page_size: usize) -> Vec<u8> {
    let row_bytes: Vec<[u8; 16]> = rows
        .iter()
        .map(|row| {
            let mut bytes = [0u8; 16];
            bytes[..8].copy_from_slice(&row[0].to_le_bytes());
            bytes[8..].copy_from_slice(&row[1].to_le_bytes());
            bytes
        })
        .collect();
    let refs: Vec<&[u8]> = row_bytes.iter().map(|row| &row[..]).collect();
    make_page(
        0x0100,
        u16::try_from(rows.len()).expect("row count"),
        0,
        &refs,
        page_size,
    )
}

fn make_2col_f64_dataset(pages: &[&[[f64; 2]]]) -> crate::Dataset {
    const PAGE_SIZE: usize = 128;
    let mut bytes = Vec::new();
    let mut total_rows = 0u64;
    for page in pages {
        bytes.extend(make_2col_f64_page(page, PAGE_SIZE));
        total_rows += u64::try_from(page.len()).expect("rows");
    }
    let mut builder = MockDatasetBuilder::new(Arc::<[u8]>::from(bytes))
        .with_column("A", LogicalType::Float, 8, 0)
        .with_column("B", LogicalType::Float, 8, 8)
        .with_row_len(16)
        .with_total_rows(total_rows)
        .with_rows_per_page(5);
    builder.page_size = PAGE_SIZE;
    builder.build()
}

/// Build a multi-page all-`Float` dataset of `num_cols` columns laid out as fused contiguous
/// pages of `rows_per_page` rows. Row `r`, column `c` holds `(r*num_cols+c) as f64 * 0.5`,
/// except every 37th cell which is a SAS missing sentinel.
fn make_wide_f64_dataset(num_cols: usize, total_rows: usize, rows_per_page: usize) -> crate::Dataset {
    let missing = SAS_NUMERIC_MISSING_SENTINEL;
    let row_len = num_cols * 8;
    let page_size = 24 + row_len * rows_per_page;
    let mut bytes = Vec::new();
    let mut row = 0usize;
    while row < total_rows {
        let n = rows_per_page.min(total_rows - row);
        let mut page = vec![0u8; page_size];
        page[16..18].copy_from_slice(&0x0100u16.to_le_bytes());
        page[18..20].copy_from_slice(&u16::try_from(n).expect("rows").to_le_bytes());
        let mut off = 24usize;
        for r in 0..n {
            for c in 0..num_cols {
                let cell = (row + r) * num_cols + c;
                let bits = if cell.is_multiple_of(37) {
                    missing
                } else {
                    #[allow(clippy::cast_precision_loss)]
                    let v = cell as f64 * 0.5;
                    v.to_bits()
                };
                page[off..off + 8].copy_from_slice(&bits.to_le_bytes());
                off += 8;
            }
        }
        bytes.extend_from_slice(&page);
        row += n;
    }
    let mut builder = MockDatasetBuilder::new(Arc::<[u8]>::from(bytes))
        .with_row_len(row_len)
        .with_total_rows(total_rows as u64)
        .with_rows_per_page(rows_per_page as u64);
    builder.page_size = page_size;
    for c in 0..num_cols {
        builder = builder.with_column(
            &format!("c{c}"),
            LogicalType::Float,
            8,
            u32::try_from(c * 8).expect("offset"),
        );
    }
    builder.build()
}

#[test]
fn columnar_matches_row_major_across_tile_boundaries() {
    // 256 columns → row_len 2048 → ~128 rows per transpose tile, so 320 rows spans >2 tiles
    // (and 5 pages of 64 rows). Batch sizes are chosen to interleave with both tile and page
    // boundaries so the tiled fill is exercised against the row-major reference at every split.
    let ds = make_wide_f64_dataset(256, 320, 64);
    for batch_rows in [50usize, 64, 100, 128, 320] {
        let row_major = ScanBuilder::new(&ds)
            .with_batch_hint(crate::BatchHint::Rows(batch_rows))
            .with_column_major_decode(crate::ColumnMajorDecode::Off)
            .collect_batches()
            .expect("row-major batches");
        let column_major = ScanBuilder::new(&ds)
            .with_batch_hint(crate::BatchHint::Rows(batch_rows))
            .collect_batches_columnar()
            .expect("column-major batches");
        assert_collected_batches_eq(&row_major, &column_major);
    }
}

/// Flatten an all-`F64` batch set into per-column `(value_bits, is_valid)` rows in global row
/// order, so two decode paths can be compared regardless of how they split rows into batches
/// (parallel chunk boundaries differ from the serial uniform-batch boundaries).
fn flatten_f64_columns(
    batches: &[crate::OwnedColumnarBatch],
    num_cols: usize,
) -> Vec<Vec<(u64, bool)>> {
    let mut sorted: Vec<&crate::OwnedColumnarBatch> = batches.iter().collect();
    sorted.sort_by_key(|b| b.row_base);
    let mut cols: Vec<Vec<(u64, bool)>> = vec![Vec::new(); num_cols];
    for batch in sorted {
        for (ci, col) in batch.columns.iter().enumerate() {
            let OwnedColumnBuffer::F64 { values, valid } = col else {
                panic!("expected F64 column, got {col:?}");
            };
            for (i, v) in values.iter().enumerate() {
                let is_valid = valid
                    .as_ref()
                    .is_none_or(|bits| (bits[i / 64] >> (i % 64)) & 1 == 1);
                cols[ci].push((v.to_bits(), is_valid));
            }
        }
    }
    cols
}

#[test]
fn columnar_flag_matches_default_serial() {
    // 10 pages of 64 columns; batch splits interleave with page boundaries.
    let ds = make_wide_f64_dataset(64, 500, 50);
    let reference = ScanBuilder::new(&ds)
        .with_batch_hint(crate::BatchHint::Rows(128))
        .with_column_major_decode(crate::ColumnMajorDecode::Off)
        .collect_batches()
        .expect("row-major default");
    let flagged = ScanBuilder::new(&ds)
        .with_batch_hint(crate::BatchHint::Rows(128))
        .with_column_major_decode(crate::ColumnMajorDecode::On)
        .collect_batches()
        .expect("column-major via flag");
    // Same source, same batch hint, serial → identical batch boundaries and contents.
    assert_collected_batches_eq(&reference, &flagged);
}

#[test]
fn columnar_flag_matches_default_parallel() {
    let ds = make_wide_f64_dataset(64, 500, 50);
    let reference = flatten_f64_columns(
        &ScanBuilder::new(&ds)
            .with_batch_hint(crate::BatchHint::Rows(128))
            .with_column_major_decode(crate::ColumnMajorDecode::Off)
            .collect_batches()
            .expect("serial row-major"),
        64,
    );
    // Parallel row-major (flag off) and parallel column-major (flag on) must both reproduce
    // the serial row-major values once flattened to global row order.
    for column_major in [crate::ColumnMajorDecode::Off, crate::ColumnMajorDecode::On] {
        let got = flatten_f64_columns(
            &ScanBuilder::new(&ds)
                .with_batch_hint(crate::BatchHint::Rows(128))
                .with_parallelism(crate::Parallelism::Threads(4))
                .with_column_major_decode(column_major)
                .collect_batches()
                .expect("parallel batches"),
            64,
        );
        assert_eq!(got, reference, "mismatch for {column_major:?}");
    }
}

#[test]
fn columnar_flag_falls_back_for_row_limit_and_range() {
    let ds = make_wide_f64_dataset(32, 400, 50);
    // A row limit and a row range both disable the column-major fill; the flagged result must
    // still equal the row-major reference under the same selection.
    let limited_ref = ScanBuilder::new(&ds)
        .limit(137)
        .with_column_major_decode(crate::ColumnMajorDecode::Off)
        .collect_batches()
        .expect("row-major limited");
    let limited_flagged = ScanBuilder::new(&ds)
        .limit(137)
        .with_column_major_decode(crate::ColumnMajorDecode::On)
        .collect_batches()
        .expect("flagged limited");
    assert_collected_batches_eq(&limited_ref, &limited_flagged);

    let range = crate::RowSelection::range(40, 260);
    let range_ref = ScanBuilder::new(&ds)
        .select(range)
        .with_column_major_decode(crate::ColumnMajorDecode::Off)
        .collect_batches()
        .expect("row-major range");
    let range_flagged = ScanBuilder::new(&ds)
        .select(range)
        .with_column_major_decode(crate::ColumnMajorDecode::On)
        .collect_batches()
        .expect("flagged range");
    assert_collected_batches_eq(&range_ref, &range_flagged);
}

#[test]
fn columnar_flag_streaming_owned_matches_default() {
    // visit_owned_batches is the streaming path the Polars plugin uses. 10 pages so the
    // parallel streaming workers engage.
    let ds = make_wide_f64_dataset(64, 500, 50);
    let mut reference_batches = Vec::new();
    ScanBuilder::new(&ds)
        .with_batch_hint(crate::BatchHint::Rows(128))
        .with_column_major_decode(crate::ColumnMajorDecode::Off)
        .visit_owned_batches(|b| {
            reference_batches.push(b);
            Ok(ControlFlow::Continue(()))
        })
        .expect("row-major stream");
    let reference = flatten_f64_columns(&reference_batches, 64);

    for parallelism in [crate::Parallelism::None, crate::Parallelism::Threads(4)] {
        let mut got_batches = Vec::new();
        ScanBuilder::new(&ds)
            .with_batch_hint(crate::BatchHint::Rows(128))
            .with_parallelism(parallelism)
            .with_column_major_decode(crate::ColumnMajorDecode::On)
            .visit_owned_batches(|b| {
                got_batches.push(b);
                Ok(ControlFlow::Continue(()))
            })
            .expect("column-major stream");
        assert_eq!(
            flatten_f64_columns(&got_batches, 64),
            reference,
            "stream mismatch for {parallelism:?}"
        );
    }
}

#[test]
fn columnar_flag_streaming_owned_early_stop_matches_default() {
    // Stopping after two batches must deliver the same rows under both decode paths (serial,
    // where batch boundaries are deterministic).
    let ds = make_wide_f64_dataset(32, 600, 50);
    let take_first_two = |column_major: crate::ColumnMajorDecode| {
        let mut batches = Vec::new();
        ScanBuilder::new(&ds)
            .with_batch_hint(crate::BatchHint::Rows(128))
            .with_parallelism(crate::Parallelism::None)
            .with_column_major_decode(column_major)
            .visit_owned_batches(|b| {
                batches.push(b);
                if batches.len() == 2 {
                    Ok(ControlFlow::Break(()))
                } else {
                    Ok(ControlFlow::Continue(()))
                }
            })
            .expect("stream");
        batches
    };
    let row_major = take_first_two(crate::ColumnMajorDecode::Off);
    let column_major = take_first_two(crate::ColumnMajorDecode::On);
    assert_eq!(row_major.len(), 2);
    assert_eq!(column_major.len(), 2);
    assert_collected_batches_eq(&row_major, &column_major);
}

fn assert_batches_eq(left: &[OwnedColumnBuffer], right: &[OwnedColumnBuffer]) {
    assert_eq!(left.len(), right.len(), "column count");
    for (lc, rc) in left.iter().zip(right) {
        match (lc, rc) {
            (
                OwnedColumnBuffer::F64 {
                    values: lv,
                    valid: lval,
                },
                OwnedColumnBuffer::F64 {
                    values: rv,
                    valid: rval,
                },
            ) => {
                let lbits: Vec<u64> = lv.iter().map(|v| v.to_bits()).collect();
                let rbits: Vec<u64> = rv.iter().map(|v| v.to_bits()).collect();
                assert_eq!(lbits, rbits, "F64 value bits mismatch");
                assert_eq!(lval, rval, "F64 validity mismatch");
            }
            other => panic!("unexpected column kinds: {other:?}"),
        }
    }
}

fn assert_collected_batches_eq(
    left: &[crate::OwnedColumnarBatch],
    right: &[crate::OwnedColumnarBatch],
) {
    assert_eq!(left.len(), right.len(), "batch count");
    for (l, r) in left.iter().zip(right) {
        assert_eq!(l.row_base, r.row_base, "row_base mismatch");
        assert_eq!(l.row_count, r.row_count, "row_count mismatch");
        assert_batches_eq(&l.columns, &r.columns);
    }
}

#[test]
fn columnar_matches_row_major_multi_page_with_nulls() {
    let missing = f64::from_bits(SAS_NUMERIC_MISSING_SENTINEL);
    let page0: [[f64; 2]; 5] = [
        [1.0, 2.0],
        [3.5, missing],
        [-4.0, 5.25],
        [6.0, 7.0],
        [missing, 8.0],
    ];
    let page1: [[f64; 2]; 4] = [[9.0, 10.0], [11.5, 12.0], [13.0, missing], [14.25, 15.0]];
    let ds = make_2col_f64_dataset(&[&page0, &page1]);

    // Batch sizes that split within a page, across pages, and span the whole file.
    for batch_rows in [2usize, 3, 5, 64] {
        let row_major = ScanBuilder::new(&ds)
            .with_batch_hint(crate::BatchHint::Rows(batch_rows))
            .with_column_major_decode(crate::ColumnMajorDecode::Off)
            .collect_batches()
            .expect("row-major batches");
        let column_major = ScanBuilder::new(&ds)
            .with_batch_hint(crate::BatchHint::Rows(batch_rows))
            .collect_batches_columnar()
            .expect("column-major batches");
        assert_collected_batches_eq(&row_major, &column_major);
    }
}

#[test]
fn columnar_falls_back_for_compressed_pages() {
    // 0xC5,'A' → RLE insert of 8 'A' bytes = one 8-byte f64 row. Compressed pages take the
    // row-major fallback inside collect_batches_columnar, so the two paths must still agree.
    let ds = MockDatasetBuilder::new(Arc::<[u8]>::from(make_compressed_page(&[0xC5u8, b'A'], 64, 4)))
        .with_column("CODE", LogicalType::Float, 8, 0)
        .with_row_len(8)
        .with_total_rows(1)
        .with_rows_per_page(1)
        .with_compression(CompressionKind::Row)
        .build();

    let row_major = ScanBuilder::new(&ds)
        .collect_batches()
        .expect("row-major batches");
    let column_major = ScanBuilder::new(&ds)
        .collect_batches_columnar()
        .expect("column-major batches");
    assert_collected_batches_eq(&row_major, &column_major);
}

#[test]
fn columnar_falls_back_for_string_columns() {
    // A non-numeric plan has no column-major fast path; collect_batches_columnar must defer
    // to the row-major collect_batches and return identical Utf8 output.
    let bytes = Arc::<[u8]>::from(make_pages());
    let ds = MockDatasetBuilder::new(bytes)
        .with_column("txt", LogicalType::String, 4, 0)
        .with_row_len(4)
        .with_total_rows(3)
        .with_rows_per_page(1)
        .build();

    let row_major = ScanBuilder::new(&ds)
        .collect_batches()
        .expect("row-major batches");
    let column_major = ScanBuilder::new(&ds)
        .collect_batches_columnar()
        .expect("column-major batches");
    assert_eq!(row_major.len(), column_major.len());
    for (l, r) in row_major.iter().zip(&column_major) {
        assert_eq!(l.row_count, r.row_count);
        assert_eq!(
            read_utf8_column(&l.columns[0].as_borrowed()),
            read_utf8_column(&r.columns[0].as_borrowed()),
        );
    }
}

/// Direct coverage for [`OwnedBatchColumnBuilder::append`] — the generic fallback
/// path (`push_fallback_family`). Every shipped fixture qualifies for one of the fast
/// paths (direct-fill / staged-numeric / direct-utf8), so the fallback's type-widening
/// and `PlannedCell` promotion branches are otherwise never exercised by the scan tests.
mod fallback_append {
    use super::super::batch::OwnedBatchColumnBuilder;
    use super::super::plan::{ColumnMaterializationKind, NumericTileMode};
    use super::super::row_decode::PlannedCell;
    use crate::columnar::OwnedColumnBuffer;
    use crate::{SasDate, SasDateTime, SasTime};

    /// Build a non-staged builder of `kind`, append `cells`, and finish to a buffer.
    fn run(kind: ColumnMaterializationKind, cells: &[PlannedCell<'_>]) -> OwnedColumnBuffer {
        run_with_owned(kind, None, cells, &[])
    }

    fn run_with_owned(
        kind: ColumnMaterializationKind,
        numeric_tile: Option<NumericTileMode>,
        cells: &[PlannedCell<'_>],
        owned_strings: &[String],
    ) -> OwnedColumnBuffer {
        let mut builder = OwnedBatchColumnBuilder::with_capacity_hint(kind, 8, 8, numeric_tile, 1);
        for &cell in cells {
            builder.append(cell, owned_strings).expect("append should succeed");
        }
        builder.finish()
    }

    /// True iff row `idx` is marked valid (non-null) in a bit-packed validity vector.
    /// A `None` vector means "no nulls seen" — every row is valid.
    fn valid_at(valid: Option<&[u64]>, idx: usize) -> bool {
        valid.is_none_or(|bits| {
            bits.get(idx / 64).is_some_and(|word| (word >> (idx % 64)) & 1 == 1)
        })
    }

    #[test]
    fn i32_accepts_and_narrows_without_widening() {
        // Null, native i32, an i64 that fits, and a float that classifies back to i32.
        let buffer = run(
            ColumnMaterializationKind::I32,
            &[
                PlannedCell::Null,
                PlannedCell::Int32(7),
                PlannedCell::Int64(9),
                PlannedCell::Float64(5.0),
            ],
        );
        let OwnedColumnBuffer::I32 { values, valid } = buffer else {
            panic!("expected i32 buffer, builder widened unexpectedly");
        };
        assert_eq!(values, vec![0, 7, 9, 5]);
        assert!(!valid_at(valid.as_deref(), 0), "row 0 was a null");
        assert!(valid_at(valid.as_deref(), 1) && valid_at(valid.as_deref(), 2) && valid_at(valid.as_deref(), 3));
    }

    #[test]
    fn i32_widens_to_f64_on_int64_overflow() {
        let overflow = i64::from(i32::MAX) + 1;
        let buffer = run(
            ColumnMaterializationKind::I32,
            &[PlannedCell::Int32(1), PlannedCell::Int64(overflow)],
        );
        let OwnedColumnBuffer::F64 { values, .. } = buffer else {
            panic!("i32 builder should widen to f64 on overflow");
        };
        #[allow(clippy::cast_precision_loss)]
        let expected = overflow as f64;
        assert_eq!(values, vec![1.0, expected]);
    }

    #[test]
    fn i32_widens_to_f64_on_non_integral_float() {
        let buffer = run(
            ColumnMaterializationKind::I32,
            &[PlannedCell::Int32(2), PlannedCell::Float64(3.5)],
        );
        let OwnedColumnBuffer::F64 { values, .. } = buffer else {
            panic!("i32 builder should widen to f64 for a fractional value");
        };
        assert_eq!(values, vec![2.0, 3.5]);
    }

    #[test]
    fn i64_accepts_promotions_and_widens_on_fraction() {
        // Integral float promotes to i64; fractional float forces a widen to f64.
        let integral = run(
            ColumnMaterializationKind::I64,
            &[
                PlannedCell::Null,
                PlannedCell::Int32(3),
                PlannedCell::Int64(1_000_000_000_000),
                PlannedCell::Float64(2.0),
            ],
        );
        let OwnedColumnBuffer::I64 { values, valid } = integral else {
            panic!("expected i64 buffer");
        };
        assert_eq!(values, vec![0, 3, 1_000_000_000_000, 2]);
        assert!(!valid_at(valid.as_deref(), 0));

        let fractional = run(
            ColumnMaterializationKind::I64,
            &[PlannedCell::Int64(5), PlannedCell::Float64(2.5)],
        );
        let OwnedColumnBuffer::F64 { values, .. } = fractional else {
            panic!("i64 builder should widen to f64 for a fractional value");
        };
        assert_eq!(values, vec![5.0, 2.5]);
    }

    #[test]
    fn f64_accepts_every_numeric_cell() {
        let buffer = run(
            ColumnMaterializationKind::F64,
            &[
                PlannedCell::Null,
                PlannedCell::Int32(4),
                PlannedCell::Int64(6),
                PlannedCell::Float64(1.5),
            ],
        );
        let OwnedColumnBuffer::F64 { values, valid } = buffer else {
            panic!("expected f64 buffer");
        };
        assert_eq!(values, vec![0.0, 4.0, 6.0, 1.5]);
        assert!(!valid_at(valid.as_deref(), 0));
    }

    #[test]
    fn temporal_builders_take_native_and_widen_on_numeric() {
        // Native temporal cells land in their typed buffer...
        let date = run(
            ColumnMaterializationKind::Date,
            &[
                PlannedCell::Null,
                PlannedCell::Date(SasDate { days_since_sas_epoch: 42 }),
            ],
        );
        let OwnedColumnBuffer::Date { values, valid } = date else {
            panic!("expected date buffer");
        };
        assert_eq!(values, vec![SasDate { days_since_sas_epoch: 0 }, SasDate { days_since_sas_epoch: 42 }]);
        assert!(!valid_at(valid.as_deref(), 0));

        let datetime = run(
            ColumnMaterializationKind::DateTime,
            &[PlannedCell::DateTime(SasDateTime { seconds_since_sas_epoch: 99 })],
        );
        let OwnedColumnBuffer::DateTime { values, .. } = datetime else {
            panic!("expected datetime buffer");
        };
        assert_eq!(values, vec![SasDateTime { seconds_since_sas_epoch: 99 }]);

        let time = run(
            ColumnMaterializationKind::Time,
            &[PlannedCell::Time(SasTime { seconds_since_midnight: 3600 })],
        );
        let OwnedColumnBuffer::Time { values, .. } = time else {
            panic!("expected time buffer");
        };
        assert_eq!(values, vec![SasTime { seconds_since_midnight: 3600 }]);

        // ...but a raw numeric cell forces the temporal column to widen to f64.
        let widened = run(
            ColumnMaterializationKind::Date,
            &[PlannedCell::Date(SasDate { days_since_sas_epoch: 1 }), PlannedCell::Int32(7)],
        );
        let OwnedColumnBuffer::F64 { values, .. } = widened else {
            panic!("date builder should widen to f64 on a numeric cell");
        };
        assert_eq!(values, vec![1.0, 7.0]);
    }

    #[test]
    fn utf8_handles_null_borrowed_and_owned() {
        let owned = vec!["from-pool".to_owned()];
        let buffer = run_with_owned(
            ColumnMaterializationKind::Utf8,
            None,
            &[
                PlannedCell::Null,
                PlannedCell::StrBorrowed("inline"),
                PlannedCell::StrOwned(0),
            ],
            &owned,
        );
        let OwnedColumnBuffer::Utf8 { data, offsets, valid, .. } = buffer else {
            panic!("expected utf8 buffer");
        };
        // Offsets delimit "", "inline", "from-pool" across the shared data buffer.
        assert_eq!(offsets.as_slice(), &[0, 0, 6, 15]);
        assert_eq!(&data, b"inlinefrom-pool");
        assert!(!valid_at(valid.as_deref(), 0));
    }

    #[test]
    fn utf8_owned_index_out_of_range_errors() {
        let mut builder =
            OwnedBatchColumnBuilder::with_capacity_hint(ColumnMaterializationKind::Utf8, 8, 8, None, 1);
        // owned_strings is empty, so index 0 is out of range.
        let err = builder.append(PlannedCell::StrOwned(0), &[]).expect_err("should reject");
        assert!(err.to_string().contains("owned string index out of range"));
    }

    #[test]
    fn raw_bytes_handles_null_and_payload() {
        let buffer = run(
            ColumnMaterializationKind::RawBytes,
            &[PlannedCell::Null, PlannedCell::Bytes(&[1, 2, 3])],
        );
        let OwnedColumnBuffer::RawBytes { data, offsets, valid } = buffer else {
            panic!("expected raw-bytes buffer");
        };
        assert_eq!(offsets.as_slice(), &[0, 0, 3]);
        assert_eq!(&data, &[1, 2, 3]);
        assert!(!valid_at(valid.as_deref(), 0));
    }

    #[test]
    fn staged_numeric_arm_encodes_raw_bits() {
        // The fallback `append` routes cells through the StagedNumeric arm
        // (`staged_numeric_raw_bits_from_planned_cell`) when the builder is tiled.
        // It pushes raw bits without flagging `has_missing`, so we assert only on the
        // round-tripped real values, not on null materialization.
        let buffer = run_with_owned(
            ColumnMaterializationKind::F64,
            Some(NumericTileMode::F64RawBits),
            &[PlannedCell::Float64(2.0), PlannedCell::Int32(3)],
            &[],
        );
        let OwnedColumnBuffer::F64 { values, .. } = buffer else {
            panic!("expected staged numeric to materialize as f64");
        };
        assert_eq!(values, vec![2.0, 3.0]);
    }

    #[test]
    fn type_mismatched_cells_are_rejected() {
        // Each builder rejects a cell kind it cannot represent, rather than silently coercing.
        let mut f64_builder =
            OwnedBatchColumnBuilder::with_capacity_hint(ColumnMaterializationKind::F64, 8, 8, None, 1);
        assert!(f64_builder.append(PlannedCell::Bytes(&[0]), &[]).is_err());

        let mut utf8_builder =
            OwnedBatchColumnBuilder::with_capacity_hint(ColumnMaterializationKind::Utf8, 8, 8, None, 1);
        assert!(utf8_builder.append(PlannedCell::Int32(1), &[]).is_err());

        let mut bytes_builder =
            OwnedBatchColumnBuilder::with_capacity_hint(ColumnMaterializationKind::RawBytes, 8, 8, None, 1);
        assert!(bytes_builder.append(PlannedCell::StrBorrowed("x"), &[]).is_err());

        let mut date_builder =
            OwnedBatchColumnBuilder::with_capacity_hint(ColumnMaterializationKind::Date, 8, 8, None, 1);
        assert!(date_builder.append(PlannedCell::StrBorrowed("x"), &[]).is_err());
    }
}
