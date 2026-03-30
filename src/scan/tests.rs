use super::{BatchDecodePlan, DirectUtf8OwnedMode, SAS_NUMERIC_MISSING_SENTINEL, ScanBuilder};
use crate::{
    columnar::OwnedColumnBuffer,
    dataset::Dataset,
    internal::{FileInner, FileSource, HeaderInfo, LayoutPlan},
    metadata::{ColumnMeta, CompressionKind, DatasetMetadata, Endianness, LogicalType},
    options::OpenOptions,
    row::OwnedCellValue,
};
use std::{ops::ControlFlow, sync::Arc};

#[test]
fn raw_scan_visits_rows_from_fused_pages() {
    let bytes = Arc::<[u8]>::from(make_pages());
    let ds = Dataset {
        file: Arc::new(FileInner {
            source: FileSource::Bytes(Arc::clone(&bytes)),
            options: OpenOptions::default(),
        }),
        metadata: Arc::new(DatasetMetadata {
            row_count: 3,
            row_len: 4,
            compression: CompressionKind::None,
            ..DatasetMetadata::default()
        }),
        layout: Arc::new(LayoutPlan {
            columns: Vec::new(),
            header: HeaderInfo {
                endianness: Endianness::Little,
                uses_u64_pointers: false,
                page_size: 64,
                page_count: 2,
                page_header_size: 24,
                subheader_pointer_size: 12,
                subheader_signature_size: 4,
                data_offset: 0,
                header_size: 0,
                release: String::new(),
                is_catalog: false,
            },
            row_len: 4,
            total_rows: 3,
            compression: CompressionKind::None,
            rows_per_page: 1,
        }),
        descriptors: Arc::new(
            crate::pages::compile_page_descriptors(
                &mut std::io::Cursor::new(bytes.as_ref()),
                &LayoutPlan {
                    columns: Vec::new(),
                    header: HeaderInfo {
                        endianness: Endianness::Little,
                        uses_u64_pointers: false,
                        page_size: 64,
                        page_count: 2,
                        page_header_size: 24,
                        subheader_pointer_size: 12,
                        subheader_signature_size: 4,
                        data_offset: 0,
                        header_size: 0,
                        release: String::new(),
                        is_catalog: false,
                    },
                    row_len: 4,
                    total_rows: 3,
                    compression: CompressionKind::None,
                    rows_per_page: 1,
                },
            )
            .expect("descriptors"),
        ),
    };

    let mut rows = Vec::new();
    let stats = ScanBuilder::new(&ds)
        .select(crate::RowSelection::Range { start: 1, end: 3 })
        .visit_raw_rows(|row| {
            rows.push((row.row_index, row.bytes.to_vec()));
            Ok(ControlFlow::Continue(()))
        })
        .expect("scan succeeds");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, b"EFGH".to_vec()));
    assert_eq!(rows[1], (2, b"IJKL".to_vec()));
    assert_eq!(stats.rows_seen, 3);
    assert_eq!(stats.rows_emitted, 2);
    assert_eq!(stats.fused_pages, 2);
}

#[test]
fn raw_scan_decompresses_rle_rows() {
    let bytes = Arc::<[u8]>::from(make_compressed_page(&[0xC1u8, b'A'], 64, 4));
    let layout = LayoutPlan {
        columns: Vec::new(),
        header: HeaderInfo {
            endianness: Endianness::Little,
            uses_u64_pointers: false,
            page_size: 64,
            page_count: 1,
            page_header_size: 24,
            subheader_pointer_size: 12,
            subheader_signature_size: 4,
            data_offset: 0,
            header_size: 0,
            release: String::new(),
            is_catalog: false,
        },
        row_len: 4,
        total_rows: 1,
        compression: CompressionKind::Row,
        rows_per_page: 1,
    };
    let ds = Dataset {
        file: Arc::new(FileInner {
            source: FileSource::Bytes(Arc::clone(&bytes)),
            options: OpenOptions::default(),
        }),
        metadata: Arc::new(DatasetMetadata {
            row_count: 1,
            row_len: 4,
            compression: CompressionKind::Row,
            ..DatasetMetadata::default()
        }),
        layout: Arc::new(layout.clone()),
        descriptors: Arc::new(
            crate::pages::compile_page_descriptors(
                &mut std::io::Cursor::new(bytes.as_ref()),
                &layout,
            )
            .expect("descriptors"),
        ),
    };

    let mut rows = Vec::new();
    let stats = ScanBuilder::new(&ds)
        .visit_raw_rows(|row| {
            rows.push((row.row_index, row.bytes.to_vec()));
            Ok(ControlFlow::Continue(()))
        })
        .expect("compressed raw scan");
    assert_eq!(rows, vec![(0, b"AAAA".to_vec())]);
    assert_eq!(stats.compressed_pages, 1);
    assert_eq!(stats.row_bytes_materialized, 4);
}

#[test]
fn raw_scan_visits_rows_from_indexed_pointer_pages() {
    let bytes = Arc::<[u8]>::from(make_pointer_page(&[b"ABCD", b"EFGH"], 64));
    let layout = LayoutPlan {
        columns: Vec::new(),
        header: HeaderInfo {
            endianness: Endianness::Little,
            uses_u64_pointers: false,
            page_size: 64,
            page_count: 1,
            page_header_size: 24,
            subheader_pointer_size: 12,
            subheader_signature_size: 4,
            data_offset: 0,
            header_size: 0,
            release: String::new(),
            is_catalog: false,
        },
        row_len: 4,
        total_rows: 2,
        compression: CompressionKind::None,
        rows_per_page: 2,
    };
    let ds = Dataset {
        file: Arc::new(FileInner {
            source: FileSource::Bytes(Arc::clone(&bytes)),
            options: OpenOptions::default(),
        }),
        metadata: Arc::new(DatasetMetadata {
            row_count: 2,
            row_len: 4,
            compression: CompressionKind::None,
            ..DatasetMetadata::default()
        }),
        layout: Arc::new(layout.clone()),
        descriptors: Arc::new(
            crate::pages::compile_page_descriptors(
                &mut std::io::Cursor::new(bytes.as_ref()),
                &layout,
            )
            .expect("descriptors"),
        ),
    };

    let mut rows = Vec::new();
    let stats = ScanBuilder::new(&ds)
        .visit_raw_rows(|row| {
            rows.push((row.row_index, row.bytes.to_vec()));
            Ok(ControlFlow::Continue(()))
        })
        .expect("scan succeeds");

    assert_eq!(rows, vec![(0, b"ABCD".to_vec()), (1, b"EFGH".to_vec())]);
    assert_eq!(stats.indexed_pages, 1);
    assert_eq!(stats.rows_emitted, 2);
}

#[test]
fn raw_scan_visits_rows_from_mixed_pointer_and_contiguous_page() {
    let bytes = Arc::<[u8]>::from(make_mixed_pointer_page(b"WXYZ", b"ABCD", 64));
    let layout = LayoutPlan {
        columns: Vec::new(),
        header: HeaderInfo {
            endianness: Endianness::Little,
            uses_u64_pointers: false,
            page_size: 64,
            page_count: 1,
            page_header_size: 24,
            subheader_pointer_size: 12,
            subheader_signature_size: 4,
            data_offset: 0,
            header_size: 0,
            release: String::new(),
            is_catalog: false,
        },
        row_len: 4,
        total_rows: 2,
        compression: CompressionKind::None,
        rows_per_page: 2,
    };
    let ds = Dataset {
        file: Arc::new(FileInner {
            source: FileSource::Bytes(Arc::clone(&bytes)),
            options: OpenOptions::default(),
        }),
        metadata: Arc::new(DatasetMetadata {
            row_count: 2,
            row_len: 4,
            compression: CompressionKind::None,
            ..DatasetMetadata::default()
        }),
        layout: Arc::new(layout.clone()),
        descriptors: Arc::new(
            crate::pages::compile_page_descriptors(
                &mut std::io::Cursor::new(bytes.as_ref()),
                &layout,
            )
            .expect("descriptors"),
        ),
    };

    let mut rows = Vec::new();
    let stats = ScanBuilder::new(&ds)
        .visit_raw_rows(|row| {
            rows.push((row.row_index, row.bytes.to_vec()));
            Ok(ControlFlow::Continue(()))
        })
        .expect("scan succeeds");

    assert_eq!(rows, vec![(0, b"WXYZ".to_vec()), (1, b"ABCD".to_vec())]);
    assert_eq!(stats.indexed_pages, 1);
    assert_eq!(stats.rows_emitted, 2);
}

#[test]
fn typed_row_scan_decodes_projected_cells() {
    let bytes = Arc::<[u8]>::from(make_page(
        0x0100,
        1,
        0,
        &[&make_numeric_text_row(42.0, b"ABCD")],
        64,
    ));
    let layout = LayoutPlan {
        columns: vec![
            ColumnMeta {
                index: 0,
                name: "num".to_owned(),
                logical_type: LogicalType::Float,
                physical_width: 8,
                offset: 0,
                label: None,
                format: None,
            },
            ColumnMeta {
                index: 1,
                name: "txt".to_owned(),
                logical_type: LogicalType::String,
                physical_width: 4,
                offset: 8,
                label: None,
                format: None,
            },
        ],
        header: HeaderInfo {
            endianness: Endianness::Little,
            uses_u64_pointers: false,
            page_size: 64,
            page_count: 1,
            page_header_size: 24,
            subheader_pointer_size: 12,
            subheader_signature_size: 4,
            data_offset: 0,
            header_size: 0,
            release: String::new(),
            is_catalog: false,
        },
        row_len: 12,
        total_rows: 1,
        compression: CompressionKind::None,
        rows_per_page: 1,
    };
    let ds = Dataset {
        file: Arc::new(FileInner {
            source: FileSource::Bytes(Arc::clone(&bytes)),
            options: OpenOptions::default(),
        }),
        metadata: Arc::new(DatasetMetadata {
            row_count: 1,
            row_len: 12,
            compression: CompressionKind::None,
            encoding: Some("UTF-8".to_owned()),
            ..DatasetMetadata::default()
        }),
        layout: Arc::new(layout.clone()),
        descriptors: Arc::new(
            crate::pages::compile_page_descriptors(
                &mut std::io::Cursor::new(bytes.as_ref()),
                &layout,
            )
            .expect("descriptors"),
        ),
    };
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
    let layout = LayoutPlan {
        columns: vec![
            ColumnMeta {
                index: 0,
                name: "num".to_owned(),
                logical_type: LogicalType::Float,
                physical_width: 8,
                offset: 0,
                label: None,
                format: None,
            },
            ColumnMeta {
                index: 1,
                name: "txt".to_owned(),
                logical_type: LogicalType::String,
                physical_width: 3,
                offset: 8,
                label: None,
                format: None,
            },
        ],
        header: HeaderInfo {
            endianness: Endianness::Little,
            uses_u64_pointers: false,
            page_size: 64,
            page_count: 1,
            page_header_size: 24,
            subheader_pointer_size: 12,
            subheader_signature_size: 4,
            data_offset: 0,
            header_size: 0,
            release: String::new(),
            is_catalog: false,
        },
        row_len: 11,
        total_rows: 1,
        compression: CompressionKind::None,
        rows_per_page: 1,
    };
    let ds = Dataset {
        file: Arc::new(FileInner {
            source: FileSource::Bytes(Arc::clone(&bytes)),
            options: OpenOptions::default(),
        }),
        metadata: Arc::new(DatasetMetadata {
            row_count: 1,
            row_len: 11,
            compression: CompressionKind::None,
            encoding: Some("UTF-8".to_owned()),
            ..DatasetMetadata::default()
        }),
        layout: Arc::new(layout.clone()),
        descriptors: Arc::new(
            crate::pages::compile_page_descriptors(
                &mut std::io::Cursor::new(bytes.as_ref()),
                &layout,
            )
            .expect("descriptors"),
        ),
    };

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

#[test]
fn collect_rows_materializes_owned_values() {
    let row = make_numeric_text_row(7.0, b"ZX  ");
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[&row], 64));
    let layout = LayoutPlan {
        columns: vec![
            ColumnMeta {
                index: 0,
                name: "num".to_owned(),
                logical_type: LogicalType::Float,
                physical_width: 8,
                offset: 0,
                label: None,
                format: None,
            },
            ColumnMeta {
                index: 1,
                name: "txt".to_owned(),
                logical_type: LogicalType::String,
                physical_width: 4,
                offset: 8,
                label: None,
                format: None,
            },
        ],
        header: HeaderInfo {
            endianness: Endianness::Little,
            uses_u64_pointers: false,
            page_size: 64,
            page_count: 1,
            page_header_size: 24,
            subheader_pointer_size: 12,
            subheader_signature_size: 4,
            data_offset: 0,
            header_size: 0,
            release: String::new(),
            is_catalog: false,
        },
        row_len: 12,
        total_rows: 1,
        compression: CompressionKind::None,
        rows_per_page: 1,
    };
    let ds = Dataset {
        file: Arc::new(FileInner {
            source: FileSource::Bytes(Arc::clone(&bytes)),
            options: OpenOptions::default(),
        }),
        metadata: Arc::new(DatasetMetadata {
            row_count: 1,
            row_len: 12,
            compression: CompressionKind::None,
            encoding: Some("UTF-8".to_owned()),
            ..DatasetMetadata::default()
        }),
        layout: Arc::new(layout.clone()),
        descriptors: Arc::new(
            crate::pages::compile_page_descriptors(
                &mut std::io::Cursor::new(bytes.as_ref()),
                &layout,
            )
            .expect("descriptors"),
        ),
    };

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
fn collect_batches_materializes_columnar_values() {
    let row_a = make_numeric_text_row(1.5, b"AA  ");
    let row_b = make_numeric_text_row(2.0, b"BBBB");
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 2, 0, &[&row_a, &row_b], 64));
    let layout = LayoutPlan {
        columns: vec![
            ColumnMeta {
                index: 0,
                name: "num".to_owned(),
                logical_type: LogicalType::Float,
                physical_width: 8,
                offset: 0,
                label: None,
                format: None,
            },
            ColumnMeta {
                index: 1,
                name: "txt".to_owned(),
                logical_type: LogicalType::String,
                physical_width: 4,
                offset: 8,
                label: None,
                format: None,
            },
        ],
        header: HeaderInfo {
            endianness: Endianness::Little,
            uses_u64_pointers: false,
            page_size: 64,
            page_count: 1,
            page_header_size: 24,
            subheader_pointer_size: 12,
            subheader_signature_size: 4,
            data_offset: 0,
            header_size: 0,
            release: String::new(),
            is_catalog: false,
        },
        row_len: 12,
        total_rows: 2,
        compression: CompressionKind::None,
        rows_per_page: 2,
    };
    let ds = Dataset {
        file: Arc::new(FileInner {
            source: FileSource::Bytes(Arc::clone(&bytes)),
            options: OpenOptions::default(),
        }),
        metadata: Arc::new(DatasetMetadata {
            row_count: 2,
            row_len: 12,
            compression: CompressionKind::None,
            encoding: Some("UTF-8".to_owned()),
            ..DatasetMetadata::default()
        }),
        layout: Arc::new(layout.clone()),
        descriptors: Arc::new(
            crate::pages::compile_page_descriptors(
                &mut std::io::Cursor::new(bytes.as_ref()),
                &layout,
            )
            .expect("descriptors"),
        ),
    };

    let batches = ScanBuilder::new(&ds)
        .with_batch_hint(crate::BatchHint::Rows(2))
        .collect_batches()
        .expect("columnar batches");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].row_base, 0);
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
    let layout = LayoutPlan {
        columns: vec![
            ColumnMeta {
                index: 0,
                name: "num".to_owned(),
                logical_type: LogicalType::Float,
                physical_width: 8,
                offset: 0,
                label: None,
                format: None,
            },
            ColumnMeta {
                index: 1,
                name: "txt".to_owned(),
                logical_type: LogicalType::String,
                physical_width: 4,
                offset: 8,
                label: None,
                format: None,
            },
            ColumnMeta {
                index: 2,
                name: "id".to_owned(),
                logical_type: LogicalType::Integer,
                physical_width: 4,
                offset: 12,
                label: None,
                format: None,
            },
        ],
        header: HeaderInfo {
            endianness: Endianness::Little,
            uses_u64_pointers: false,
            page_size: 64,
            page_count: 1,
            page_header_size: 24,
            subheader_pointer_size: 12,
            subheader_signature_size: 4,
            data_offset: 0,
            header_size: 0,
            release: String::new(),
            is_catalog: false,
        },
        row_len: 16,
        total_rows: 1,
        compression: CompressionKind::None,
        rows_per_page: 1,
    };
    let ds = Dataset {
        file: Arc::new(FileInner {
            source: FileSource::Bytes(Arc::clone(&bytes)),
            options: OpenOptions::default(),
        }),
        metadata: Arc::new(DatasetMetadata {
            row_count: 1,
            row_len: 16,
            compression: CompressionKind::None,
            encoding: Some("UTF-8".to_owned()),
            ..DatasetMetadata::default()
        }),
        layout: Arc::new(layout.clone()),
        descriptors: Arc::new(
            crate::pages::compile_page_descriptors(
                &mut std::io::Cursor::new(bytes.as_ref()),
                &layout,
            )
            .expect("descriptors"),
        ),
    };

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

    assert_eq!(plan.families.staged_numeric, vec![0]);
    assert!(plan.families.direct_utf8_borrowed.is_empty());
    assert_eq!(plan.families.direct_utf8_owned, vec![1]);
    assert_eq!(
        plan.direct_utf8_owned_mode,
        Some(DirectUtf8OwnedMode::Utf8Lenient)
    );
    assert_eq!(plan.families.direct_numeric, vec![2]);
    assert!(plan.families.direct_raw_bytes.is_empty());
    assert!(plan.families.fallback.is_empty());
    assert!(!plan.all_columns_staged_numeric);
    assert!(!plan.needs_owned_string_scratch);
}

#[test]
fn batch_decode_plan_compiles_lossless_raw_bytes_family() {
    let row = make_numeric_text_row(42.0, b"ZX  ");
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[&row], 64));
    let layout = LayoutPlan {
        columns: vec![
            ColumnMeta {
                index: 0,
                name: "num".to_owned(),
                logical_type: LogicalType::Float,
                physical_width: 8,
                offset: 0,
                label: None,
                format: None,
            },
            ColumnMeta {
                index: 1,
                name: "txt".to_owned(),
                logical_type: LogicalType::String,
                physical_width: 4,
                offset: 8,
                label: None,
                format: None,
            },
        ],
        header: HeaderInfo {
            endianness: Endianness::Little,
            uses_u64_pointers: false,
            page_size: 64,
            page_count: 1,
            page_header_size: 24,
            subheader_pointer_size: 12,
            subheader_signature_size: 4,
            data_offset: 0,
            header_size: 0,
            release: String::new(),
            is_catalog: false,
        },
        row_len: 12,
        total_rows: 1,
        compression: CompressionKind::None,
        rows_per_page: 1,
    };
    let ds = Dataset {
        file: Arc::new(FileInner {
            source: FileSource::Bytes(Arc::clone(&bytes)),
            options: OpenOptions::default(),
        }),
        metadata: Arc::new(DatasetMetadata {
            row_count: 1,
            row_len: 12,
            compression: CompressionKind::None,
            encoding: Some("UTF-8".to_owned()),
            ..DatasetMetadata::default()
        }),
        layout: Arc::new(layout.clone()),
        descriptors: Arc::new(
            crate::pages::compile_page_descriptors(
                &mut std::io::Cursor::new(bytes.as_ref()),
                &layout,
            )
            .expect("descriptors"),
        ),
    };

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
    assert!(!plan.all_columns_staged_numeric);
    assert!(!plan.needs_owned_string_scratch);
}

#[test]
fn batch_decode_plan_compiles_strict_utf8_borrowed_family() {
    let row = make_numeric_text_row(1.0, b"pear");
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[&row], 64));
    let layout = LayoutPlan {
        columns: vec![
            ColumnMeta {
                index: 0,
                name: "num".to_owned(),
                logical_type: LogicalType::Float,
                physical_width: 8,
                offset: 0,
                label: None,
                format: None,
            },
            ColumnMeta {
                index: 1,
                name: "txt".to_owned(),
                logical_type: LogicalType::String,
                physical_width: 4,
                offset: 8,
                label: None,
                format: None,
            },
        ],
        header: HeaderInfo {
            endianness: Endianness::Little,
            uses_u64_pointers: false,
            page_size: 64,
            page_count: 1,
            page_header_size: 24,
            subheader_pointer_size: 12,
            subheader_signature_size: 4,
            data_offset: 0,
            header_size: 0,
            release: String::new(),
            is_catalog: false,
        },
        row_len: 12,
        total_rows: 1,
        compression: CompressionKind::None,
        rows_per_page: 1,
    };
    let ds = Dataset {
        file: Arc::new(FileInner {
            source: FileSource::Bytes(Arc::clone(&bytes)),
            options: OpenOptions::default(),
        }),
        metadata: Arc::new(DatasetMetadata {
            row_count: 1,
            row_len: 12,
            compression: CompressionKind::None,
            encoding: Some("UTF-8".to_owned()),
            ..DatasetMetadata::default()
        }),
        layout: Arc::new(layout.clone()),
        descriptors: Arc::new(
            crate::pages::compile_page_descriptors(
                &mut std::io::Cursor::new(bytes.as_ref()),
                &layout,
            )
            .expect("descriptors"),
        ),
    };

    let plan = BatchDecodePlan::new(
        &ScanBuilder::new(&ds)
            .with_string_options(crate::StringDecodeOptions {
                trim_fixed_width: true,
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
    assert!(!plan.needs_owned_string_scratch);
}

#[test]
fn typed_rows_decode_ascii_strings_without_utf8_encoding() {
    let row = make_numeric_text_row(1.0, b"pear");
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[&row], 64));
    let layout = LayoutPlan {
        columns: vec![
            ColumnMeta {
                index: 0,
                name: "num".to_owned(),
                logical_type: LogicalType::Float,
                physical_width: 8,
                offset: 0,
                label: None,
                format: None,
            },
            ColumnMeta {
                index: 1,
                name: "txt".to_owned(),
                logical_type: LogicalType::String,
                physical_width: 4,
                offset: 8,
                label: None,
                format: None,
            },
        ],
        header: HeaderInfo {
            endianness: Endianness::Little,
            uses_u64_pointers: false,
            page_size: 64,
            page_count: 1,
            page_header_size: 24,
            subheader_pointer_size: 12,
            subheader_signature_size: 4,
            data_offset: 0,
            header_size: 0,
            release: String::new(),
            is_catalog: false,
        },
        row_len: 12,
        total_rows: 1,
        compression: CompressionKind::None,
        rows_per_page: 1,
    };
    let ds = Dataset {
        file: Arc::new(FileInner {
            source: FileSource::Bytes(Arc::clone(&bytes)),
            options: OpenOptions::default(),
        }),
        metadata: Arc::new(DatasetMetadata {
            row_count: 1,
            row_len: 12,
            compression: CompressionKind::None,
            encoding: Some("WINDOWS-1252".to_owned()),
            ..DatasetMetadata::default()
        }),
        layout: Arc::new(layout.clone()),
        descriptors: Arc::new(
            crate::pages::compile_page_descriptors(
                &mut std::io::Cursor::new(bytes.as_ref()),
                &layout,
            )
            .expect("descriptors"),
        ),
    };

    let rows = ScanBuilder::new(&ds).collect_rows().expect("rows");
    assert!(matches!(
        rows[0].cells[1],
        OwnedCellValue::String(ref value) if value == "pear"
    ));
}

#[test]
fn collect_batches_decode_ascii_strings_without_utf8_encoding() {
    let row = make_numeric_text_row(1.0, b"pear");
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[&row], 64));
    let layout = LayoutPlan {
        columns: vec![
            ColumnMeta {
                index: 0,
                name: "num".to_owned(),
                logical_type: LogicalType::Float,
                physical_width: 8,
                offset: 0,
                label: None,
                format: None,
            },
            ColumnMeta {
                index: 1,
                name: "txt".to_owned(),
                logical_type: LogicalType::String,
                physical_width: 4,
                offset: 8,
                label: None,
                format: None,
            },
        ],
        header: HeaderInfo {
            endianness: Endianness::Little,
            uses_u64_pointers: false,
            page_size: 64,
            page_count: 1,
            page_header_size: 24,
            subheader_pointer_size: 12,
            subheader_signature_size: 4,
            data_offset: 0,
            header_size: 0,
            release: String::new(),
            is_catalog: false,
        },
        row_len: 12,
        total_rows: 1,
        compression: CompressionKind::None,
        rows_per_page: 1,
    };
    let ds = Dataset {
        file: Arc::new(FileInner {
            source: FileSource::Bytes(Arc::clone(&bytes)),
            options: OpenOptions::default(),
        }),
        metadata: Arc::new(DatasetMetadata {
            row_count: 1,
            row_len: 12,
            compression: CompressionKind::None,
            encoding: Some("WINDOWS-1252".to_owned()),
            ..DatasetMetadata::default()
        }),
        layout: Arc::new(layout.clone()),
        descriptors: Arc::new(
            crate::pages::compile_page_descriptors(
                &mut std::io::Cursor::new(bytes.as_ref()),
                &layout,
            )
            .expect("descriptors"),
        ),
    };

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
    let row = make_numeric_text_row(1.5, b"INT ");
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[&row], 64));
    let layout = LayoutPlan {
        columns: vec![
            ColumnMeta {
                index: 0,
                name: "num".to_owned(),
                logical_type: LogicalType::Integer,
                physical_width: 8,
                offset: 0,
                label: None,
                format: None,
            },
            ColumnMeta {
                index: 1,
                name: "txt".to_owned(),
                logical_type: LogicalType::String,
                physical_width: 4,
                offset: 8,
                label: None,
                format: None,
            },
        ],
        header: HeaderInfo {
            endianness: Endianness::Little,
            uses_u64_pointers: false,
            page_size: 64,
            page_count: 1,
            page_header_size: 24,
            subheader_pointer_size: 12,
            subheader_signature_size: 4,
            data_offset: 0,
            header_size: 0,
            release: String::new(),
            is_catalog: false,
        },
        row_len: 12,
        total_rows: 1,
        compression: CompressionKind::None,
        rows_per_page: 1,
    };
    let ds = Dataset {
        file: Arc::new(FileInner {
            source: FileSource::Bytes(Arc::clone(&bytes)),
            options: OpenOptions::default(),
        }),
        metadata: Arc::new(DatasetMetadata {
            row_count: 1,
            row_len: 12,
            compression: CompressionKind::None,
            encoding: Some("UTF-8".to_owned()),
            ..DatasetMetadata::default()
        }),
        layout: Arc::new(layout.clone()),
        descriptors: Arc::new(
            crate::pages::compile_page_descriptors(
                &mut std::io::Cursor::new(bytes.as_ref()),
                &layout,
            )
            .expect("descriptors"),
        ),
    };

    let rows = ScanBuilder::new(&ds).collect_rows().expect("rows");
    assert!(matches!(rows[0].cells[0], OwnedCellValue::Float64(value) if value == 1.5));

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
    let row = make_numeric_text_row(42.0, b"ZX  ");
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[&row], 64));
    let layout = LayoutPlan {
        columns: vec![
            ColumnMeta {
                index: 0,
                name: "num".to_owned(),
                logical_type: LogicalType::Integer,
                physical_width: 8,
                offset: 0,
                label: None,
                format: None,
            },
            ColumnMeta {
                index: 1,
                name: "txt".to_owned(),
                logical_type: LogicalType::String,
                physical_width: 4,
                offset: 8,
                label: None,
                format: None,
            },
        ],
        header: HeaderInfo {
            endianness: Endianness::Little,
            uses_u64_pointers: false,
            page_size: 64,
            page_count: 1,
            page_header_size: 24,
            subheader_pointer_size: 12,
            subheader_signature_size: 4,
            data_offset: 0,
            header_size: 0,
            release: String::new(),
            is_catalog: false,
        },
        row_len: 12,
        total_rows: 1,
        compression: CompressionKind::None,
        rows_per_page: 1,
    };
    let ds = Dataset {
        file: Arc::new(FileInner {
            source: FileSource::Bytes(Arc::clone(&bytes)),
            options: OpenOptions::default(),
        }),
        metadata: Arc::new(DatasetMetadata {
            row_count: 1,
            row_len: 12,
            compression: CompressionKind::None,
            encoding: Some("UTF-8".to_owned()),
            ..DatasetMetadata::default()
        }),
        layout: Arc::new(layout.clone()),
        descriptors: Arc::new(
            crate::pages::compile_page_descriptors(
                &mut std::io::Cursor::new(bytes.as_ref()),
                &layout,
            )
            .expect("descriptors"),
        ),
    };

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
    let layout = LayoutPlan {
        columns: vec![ColumnMeta {
            index: 0,
            name: "num".to_owned(),
            logical_type: LogicalType::Float,
            physical_width: 8,
            offset: 0,
            label: None,
            format: None,
        }],
        header: HeaderInfo {
            endianness: Endianness::Little,
            uses_u64_pointers: false,
            page_size: 64,
            page_count: 1,
            page_header_size: 24,
            subheader_pointer_size: 12,
            subheader_signature_size: 4,
            data_offset: 0,
            header_size: 0,
            release: String::new(),
            is_catalog: false,
        },
        row_len: 8,
        total_rows: 2,
        compression: CompressionKind::None,
        rows_per_page: 2,
    };
    let ds = Dataset {
        file: Arc::new(FileInner {
            source: FileSource::Bytes(Arc::clone(&bytes)),
            options: OpenOptions::default(),
        }),
        metadata: Arc::new(DatasetMetadata {
            row_count: 2,
            row_len: 8,
            compression: CompressionKind::None,
            encoding: Some("UTF-8".to_owned()),
            ..DatasetMetadata::default()
        }),
        layout: Arc::new(layout.clone()),
        descriptors: Arc::new(
            crate::pages::compile_page_descriptors(
                &mut std::io::Cursor::new(bytes.as_ref()),
                &layout,
            )
            .expect("descriptors"),
        ),
    };

    let batches = ScanBuilder::new(&ds)
        .with_batch_hint(crate::BatchHint::Rows(2))
        .collect_batches()
        .expect("batches");
    assert_eq!(batches.len(), 1);
    match &batches[0].columns[0] {
        OwnedColumnBuffer::F64 { values, valid } => {
            assert_eq!(values, &vec![1.25, 0.0]);
            assert_eq!(valid.as_deref(), Some(&[1, 0][..]));
        }
        other => panic!("unexpected staged f64 batch column: {other:?}"),
    }
}

#[test]
fn visit_batches_streams_projected_columnar_views() {
    let row_a = make_numeric_text_row(10.0, b"ABCD");
    let row_b = make_numeric_text_row(20.0, b"EF  ");
    let bytes = Arc::<[u8]>::from(make_pointer_page(&[&row_a, &row_b], 64));
    let layout = LayoutPlan {
        columns: vec![
            ColumnMeta {
                index: 0,
                name: "num".to_owned(),
                logical_type: LogicalType::Float,
                physical_width: 8,
                offset: 0,
                label: None,
                format: None,
            },
            ColumnMeta {
                index: 1,
                name: "txt".to_owned(),
                logical_type: LogicalType::String,
                physical_width: 4,
                offset: 8,
                label: None,
                format: None,
            },
        ],
        header: HeaderInfo {
            endianness: Endianness::Little,
            uses_u64_pointers: false,
            page_size: 64,
            page_count: 1,
            page_header_size: 24,
            subheader_pointer_size: 12,
            subheader_signature_size: 4,
            data_offset: 0,
            header_size: 0,
            release: String::new(),
            is_catalog: false,
        },
        row_len: 12,
        total_rows: 2,
        compression: CompressionKind::None,
        rows_per_page: 2,
    };
    let ds = Dataset {
        file: Arc::new(FileInner {
            source: FileSource::Bytes(Arc::clone(&bytes)),
            options: OpenOptions::default(),
        }),
        metadata: Arc::new(DatasetMetadata {
            row_count: 2,
            row_len: 12,
            compression: CompressionKind::None,
            encoding: Some("UTF-8".to_owned()),
            ..DatasetMetadata::default()
        }),
        layout: Arc::new(layout.clone()),
        descriptors: Arc::new(
            crate::pages::compile_page_descriptors(
                &mut std::io::Cursor::new(bytes.as_ref()),
                &layout,
            )
            .expect("descriptors"),
        ),
    };
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
            (0, 1, vec!["ABCD".to_owned()]),
            (1, 1, vec!["EF".to_owned()]),
        ]
    );
    assert_eq!(stats.decode_batches, 2);
    assert_eq!(stats.pages_seen, 1);
}

#[test]
fn collect_rows_decodes_compressed_string_rows() {
    let bytes = Arc::<[u8]>::from(make_compressed_page(&[0xC1u8, b'Z'], 64, 4));
    let layout = LayoutPlan {
        columns: vec![ColumnMeta {
            index: 0,
            name: "txt".to_owned(),
            logical_type: LogicalType::String,
            physical_width: 4,
            offset: 0,
            label: None,
            format: None,
        }],
        header: HeaderInfo {
            endianness: Endianness::Little,
            uses_u64_pointers: false,
            page_size: 64,
            page_count: 1,
            page_header_size: 24,
            subheader_pointer_size: 12,
            subheader_signature_size: 4,
            data_offset: 0,
            header_size: 0,
            release: String::new(),
            is_catalog: false,
        },
        row_len: 4,
        total_rows: 1,
        compression: CompressionKind::Row,
        rows_per_page: 1,
    };
    let ds = Dataset {
        file: Arc::new(FileInner {
            source: FileSource::Bytes(Arc::clone(&bytes)),
            options: OpenOptions::default(),
        }),
        metadata: Arc::new(DatasetMetadata {
            row_count: 1,
            row_len: 4,
            compression: CompressionKind::Row,
            encoding: Some("UTF-8".to_owned()),
            ..DatasetMetadata::default()
        }),
        layout: Arc::new(layout.clone()),
        descriptors: Arc::new(
            crate::pages::compile_page_descriptors(
                &mut std::io::Cursor::new(bytes.as_ref()),
                &layout,
            )
            .expect("descriptors"),
        ),
    };

    let rows = ScanBuilder::new(&ds)
        .collect_rows()
        .expect("typed compressed rows");
    assert_eq!(rows.len(), 1);
    assert!(matches!(
        rows[0].cells[0],
        crate::row::OwnedCellValue::String(ref value) if value == "ZZZZ"
    ));
}

fn make_pages() -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend(make_page(0x0100, 2, 0, &[b"ABCD", b"EFGH"], 64));
    bytes.extend(make_page(0x0200, 0, 0, &[b"IJKL"], 64));
    bytes
}

fn make_page(
    page_type: u16,
    row_count: u16,
    pointer_count: u16,
    rows: &[&[u8]],
    page_size: usize,
) -> Vec<u8> {
    let mut page = vec![0u8; page_size];
    page[(24 - 8)..(24 - 6)].copy_from_slice(&page_type.to_le_bytes());
    page[(24 - 6)..(24 - 4)].copy_from_slice(&row_count.to_le_bytes());
    page[(24 - 4)..(24 - 2)].copy_from_slice(&pointer_count.to_le_bytes());

    let mut offset = 24usize;
    for row in rows {
        page[offset..offset + row.len()].copy_from_slice(row);
        offset += row.len();
    }
    page
}

fn make_pointer_page(rows: &[&[u8]], page_size: usize) -> Vec<u8> {
    let mut page = vec![0u8; page_size];
    page[(24 - 8)..(24 - 6)].copy_from_slice(&0x0100u16.to_le_bytes());
    page[(24 - 6)..(24 - 4)].copy_from_slice(&(rows.len() as u16).to_le_bytes());
    page[(24 - 4)..(24 - 2)].copy_from_slice(&1u16.to_le_bytes());

    let data_offset = 40u32;
    let data_len = u32::try_from(rows.len() * 4).unwrap_or(u32::MAX);
    page[24..28].copy_from_slice(&data_offset.to_le_bytes());
    page[28..32].copy_from_slice(&data_len.to_le_bytes());
    page[32] = 0;
    page[33] = 1;

    let mut offset = data_offset as usize;
    for row in rows {
        page[offset..offset + row.len()].copy_from_slice(row);
        offset += row.len();
    }
    page
}

fn make_mixed_pointer_page(
    pointer_row: &[u8; 4],
    contiguous_row: &[u8; 4],
    page_size: usize,
) -> Vec<u8> {
    let mut page = vec![0u8; page_size];
    page[(24 - 8)..(24 - 6)].copy_from_slice(&0x0100u16.to_le_bytes());
    page[(24 - 6)..(24 - 4)].copy_from_slice(&2u16.to_le_bytes());
    page[(24 - 4)..(24 - 2)].copy_from_slice(&1u16.to_le_bytes());

    let pointer_data_offset = 48u32;
    page[24..28].copy_from_slice(&pointer_data_offset.to_le_bytes());
    page[28..32].copy_from_slice(&4u32.to_le_bytes());
    page[32] = 0;
    page[33] = 1;

    page[40..44].copy_from_slice(contiguous_row);
    let start = usize::try_from(pointer_data_offset).unwrap_or(0);
    page[start..start + 4].copy_from_slice(pointer_row);
    page
}

fn make_numeric_text_row(number: f64, text: &[u8; 4]) -> Vec<u8> {
    let mut row = Vec::with_capacity(12);
    row.extend_from_slice(&number.to_le_bytes());
    row.extend_from_slice(text);
    row
}

fn make_compressed_page(compressed: &[u8], page_size: usize, compression_flag: u8) -> Vec<u8> {
    let mut page = vec![0u8; page_size];
    page[(24 - 8)..(24 - 6)].copy_from_slice(&0x0100u16.to_le_bytes());
    page[(24 - 6)..(24 - 4)].copy_from_slice(&1u16.to_le_bytes());
    page[(24 - 4)..(24 - 2)].copy_from_slice(&1u16.to_le_bytes());

    let data_offset = 40u32;
    let data_len = u32::try_from(compressed.len()).unwrap_or(u32::MAX);
    page[24..28].copy_from_slice(&data_offset.to_le_bytes());
    page[28..32].copy_from_slice(&data_len.to_le_bytes());
    page[32] = compression_flag;
    page[33] = 1;

    let start = usize::try_from(data_offset).unwrap_or(0);
    let end = start + compressed.len();
    page[start..end].copy_from_slice(compressed);
    page
}

fn read_utf8_column(column: &crate::ColumnBuffer<'_>) -> Vec<String> {
    let crate::ColumnBuffer::Utf8(buffer) = column else {
        panic!("expected utf8 column, got {column:?}");
    };
    buffer
        .offsets
        .windows(2)
        .map(|window| {
            let start = usize::try_from(window[0]).expect("utf8 start");
            let end = usize::try_from(window[1]).expect("utf8 end");
            String::from_utf8(buffer.data[start..end].to_vec()).expect("utf8 cell")
        })
        .collect()
}
