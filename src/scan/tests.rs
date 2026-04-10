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
use std::{ops::ControlFlow, sync::Arc};

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

    let plan_off = BatchDecodePlan::new(
        &ScanBuilder::new(&ds).with_string_options(crate::StringDecodeOptions {
            trim_mode: crate::TrimMode::RTrim,
            utf8_validation: crate::Utf8ValidationMode::Strict,
            mojibake_fix: crate::MojibakePolicy::Auto,
            dictionary_staging: crate::DictionaryStaging::Off,
        }),
    )
    .expect("plan off");
    let acc_off = BatchAccumulator::new(plan_off, 1, 1);
    assert!(!acc_off.has_staged_string_lookup_for(1));

    let plan_on = BatchDecodePlan::new(
        &ScanBuilder::new(&ds).with_string_options(crate::StringDecodeOptions {
            trim_mode: crate::TrimMode::Preserve,
            utf8_validation: crate::Utf8ValidationMode::Strict,
            mojibake_fix: crate::MojibakePolicy::Auto,
            dictionary_staging: crate::DictionaryStaging::On,
        }),
    )
    .expect("plan on");
    let acc_on = BatchAccumulator::new(plan_on, 1, 1);
    assert!(acc_on.has_staged_string_lookup_for(1));

    let plan_auto = BatchDecodePlan::new(
        &ScanBuilder::new(&ds).with_string_options(crate::StringDecodeOptions {
            trim_mode: crate::TrimMode::Preserve,
            utf8_validation: crate::Utf8ValidationMode::Strict,
            mojibake_fix: crate::MojibakePolicy::Auto,
            dictionary_staging: crate::DictionaryStaging::Auto,
        }),
    )
    .expect("plan auto");
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
        } => {
            assert_eq!(offsets, &vec![0, 2, 6]);
            assert_eq!(data, b"AABBBB");
            assert!(valid.is_none());
        }
        other => panic!("unexpected utf8 batch column: {other:?}"),
    }
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
    let plan = BatchDecodePlan::new(&builder).expect("batch plan");

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

    let plan = BatchDecodePlan::new(
        &ScanBuilder::new(&ds)
            .with_decode_mode(crate::DecodeMode::TypedLossless)
            .with_batch_hint(crate::BatchHint::Rows(1)),
    )
    .expect("batch plan");

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

    let plan = BatchDecodePlan::new(
        &ScanBuilder::new(&ds)
            .with_string_options(crate::StringDecodeOptions {
                trim_mode: crate::TrimMode::RTrim,
                utf8_validation: crate::Utf8ValidationMode::Strict,
                mojibake_fix: crate::MojibakePolicy::Auto,
                dictionary_staging: crate::DictionaryStaging::Auto,
            })
            .with_batch_hint(crate::BatchHint::Rows(1)),
    )
    .expect("batch plan");

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

    let plan =
        BatchDecodePlan::new(&ScanBuilder::new(&ds).with_batch_hint(crate::BatchHint::Rows(1)))
            .expect("batch plan");

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

    let plan =
        BatchDecodePlan::new(&ScanBuilder::new(&ds).with_batch_hint(crate::BatchHint::Rows(1)))
            .expect("batch plan");

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
        } => {
            assert_eq!(offsets, &vec![0, 4]);
            assert_eq!(data, b"pear");
            assert!(valid.is_none());
        }
        other => panic!("unexpected utf8 batch column: {other:?}"),
    }
}

#[test]
fn collect_batches_typed_integer_widens_to_f64_for_fractional_values() {
    let row = make_numeric_text_row(1.5, *b"INT ");
    let ds = make_integer_test_dataset(&row);

    let rows = ScanBuilder::new(&ds).collect_rows().expect("rows");
    assert!(
        matches!(rows[0].cells[0], OwnedCellValue::Float64(value) if (value - 1.5).abs() < f64::EPSILON)
    );

    let batches = ScanBuilder::new(&ds)
        .with_batch_hint(crate::BatchHint::Rows(1))
        .collect_batches()
        .expect("batches");
    assert_eq!(batches.len(), 1);
    match &batches[0].columns[0] {
        OwnedColumnBuffer::F64 { values, valid } => {
            assert_eq!(values, &vec![1.5]);
            assert!(valid.is_none());
        }
        other => panic!("unexpected widened integer batch column: {other:?}"),
    }
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
            assert_eq!(offsets, &vec![0, 4]);
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
        } => {
            assert_eq!(offsets, &vec![0, 3]);
            assert_eq!(data, &[0xE2, 0x80, 0x93]);
            assert!(valid.is_none());
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
