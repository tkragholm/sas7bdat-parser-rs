use super::{iterator::RowIterator, row_iterator};
use crate::{
    cell::CellValue,
    dataset::{Alignment, Compression, DatasetMetadata, Endianness, Measure, Vendor},
    parser::{
        core::encoding::resolve_encoding,
        header::SasHeader,
        metadata::{
            ColumnInfo, ColumnKind, ColumnOffsets, DatasetLayout, RowInfo, TextRef, TextStore,
        },
        rows::{
            ColumnarBatchMode,
            columnar::COLUMNAR_BATCH_ROWS,
            compression::{decompress_rdc, decompress_rle},
            constants::SAS_PAGE_TYPE_DATA,
        },
    },
};
use encoding_rs::Encoding;
use std::{
    borrow::Cow,
    io::{Cursor, Read, Seek},
};

fn make_parsed_metadata(
    vendor: Vendor,
    compression: Compression,
    row_length: u32,
    total_rows: u64,
    rows_per_page: u64,
    page_size: u32,
) -> DatasetLayout {
    make_parsed_metadata_with_page_count(
        vendor,
        compression,
        row_length,
        total_rows,
        rows_per_page,
        page_size,
        1,
    )
}

fn make_parsed_metadata_with_page_count(
    vendor: Vendor,
    compression: Compression,
    row_length: u32,
    total_rows: u64,
    rows_per_page: u64,
    page_size: u32,
    page_count: u64,
) -> DatasetLayout {
    let mut metadata = DatasetMetadata::new(1);
    metadata.row_count = total_rows;
    metadata.column_count = 1;
    metadata.endianness = Endianness::Little;
    metadata.compression = compression;
    metadata.vendor = vendor;

    let header = SasHeader {
        metadata,
        endianness: Endianness::Little,
        uses_u64: false,
        page_header_size: 24,
        subheader_pointer_size: 12,
        subheader_signature_size: 4,
        header_size: 0,
        page_size,
        page_count,
        pad_alignment: 0,
        data_offset: 0,
    };

    let column = ColumnInfo {
        index: 0,
        offsets: ColumnOffsets {
            offset: 0,
            width: row_length,
        },
        kind: ColumnKind::Character,
        format_width: None,
        format_decimals: None,
        name_ref: TextRef::EMPTY,
        label_ref: TextRef::EMPTY,
        format_ref: TextRef::EMPTY,
        measure: Measure::Unknown,
        alignment: Alignment::Unknown,
    };

    let row_info = RowInfo {
        row_length,
        total_rows,
        rows_per_page,
        compression,
        file_label: None,
    };

    DatasetLayout {
        header,
        text_store: TextStore::new(resolve_encoding(None)),
        columns: vec![column],
        row_info,
        column_list: None,
    }
}

fn make_data_page(rows: &[&[u8]], row_length: usize, page_size: usize) -> Vec<u8> {
    let mut page = vec![0u8; page_size];
    page[(24 - 8)..(24 - 6)].copy_from_slice(&SAS_PAGE_TYPE_DATA.to_le_bytes());
    let row_count: u16 = rows.len().try_into().expect("row count fits u16");
    page[(24 - 6)..(24 - 4)].copy_from_slice(&row_count.to_le_bytes());
    page[(24 - 4)..(24 - 2)].copy_from_slice(&0u16.to_le_bytes()); // subheader count

    write_rows_to_page(&mut page, rows, row_length);
    page
}

fn make_mix_page(rows: &[&[u8]], row_length: usize, page_size: usize) -> Vec<u8> {
    let mut page = vec![0u8; page_size];
    // MIX page with no subheaders; rows start immediately after header.
    let page_type = super::constants::SAS_PAGE_TYPE_MIX.to_le_bytes();
    page[(24 - 8)..(24 - 6)].copy_from_slice(&page_type);
    page[(24 - 4)..(24 - 2)].copy_from_slice(&0u16.to_le_bytes());

    write_rows_to_page(&mut page, rows, row_length);
    page
}

fn make_compressed_page(
    compressed: &[u8],
    _row_length: usize,
    page_size: usize,
    compression_flag: u8,
) -> Vec<u8> {
    let mut page = vec![0u8; page_size];
    init_data_page_header(&mut page, 1, 1);

    // pointer starts at byte 24.
    let data_offset = 64u32;
    let length = u32::try_from(compressed.len()).expect("length fits");
    let mut pointer = [0u8; 12];
    pointer[0..4].copy_from_slice(&data_offset.to_le_bytes());
    pointer[4..8].copy_from_slice(&length.to_le_bytes());
    pointer[8] = compression_flag; // SAS compression code
    pointer[9] = 1; // is_compressed_data flag
    page[24..36].copy_from_slice(&pointer);

    let start = data_offset as usize;
    let end = start + compressed.len();
    page[start..end].copy_from_slice(compressed);
    page
}

fn write_rows_to_page(page: &mut [u8], rows: &[&[u8]], row_length: usize) {
    let mut offset = 24usize;
    for row in rows {
        let mut buf = vec![0u8; row_length];
        let len = row.len().min(row_length);
        buf[..len].copy_from_slice(&row[..len]);
        page[offset..offset + row_length].copy_from_slice(&buf);
        offset += row_length;
    }
}

fn setup_data_iter(rows: &[&[u8]], row_length: usize) -> (Cursor<Vec<u8>>, DatasetLayout) {
    let page = make_data_page(rows, row_length, 64);
    let parsed = make_parsed_metadata(
        Vendor::Sas,
        Compression::None,
        u32::try_from(row_length).expect("row length fits u32"),
        rows.len() as u64,
        rows.len() as u64,
        64,
    );
    (Cursor::new(page), parsed)
}

fn assert_rows_from_iter<R: Read + Seek>(iter: &mut RowIterator<'_, R>, expected: &[&str]) {
    for (index, expected_row) in expected.iter().enumerate() {
        let row = iter.try_next().expect("row result").expect("row present");
        assert_eq!(
            row,
            vec![CellValue::Str(Cow::Borrowed(*expected_row))],
            "row {}",
            index + 1
        );
    }
    assert!(iter.try_next().expect("end result").is_none());
}

fn assert_rows_from_page(page: Vec<u8>, parsed: &DatasetLayout, expected: &[&str]) {
    let mut cursor = Cursor::new(page);
    let mut iter = row_iterator(&mut cursor, parsed).expect("construct row iterator");
    assert_rows_from_iter(&mut iter, expected);
}

#[test]
fn decompresses_rle_single_run() {
    let input = [0x80u8, b'A']; // command 8, length nibble 0 => copy 1 byte
    let mut output = Vec::new();
    decompress_rle(&input, 1, &mut output).expect("rle decompress succeeds");
    assert_eq!(output, b"A");
}

#[test]
fn decompresses_rdc_literals() {
    let mut input = Vec::new();
    input.extend_from_slice(&0u16.to_be_bytes()); // prefix with all literal bits
    input.extend_from_slice(b"ABCDEFGHIJKLMNOP");
    let mut output = Vec::new();
    decompress_rdc(&input, 16, &mut output).expect("rdc decompress succeeds");
    assert_eq!(output, b"ABCDEFGHIJKLMNOP");
}

#[test]
fn fetches_rows_from_data_page() {
    let row_length = 4usize;
    let rows = [b"AAAA".as_slice(), b"BBBB".as_slice()];
    let (mut cursor, parsed) = setup_data_iter(&rows, row_length);
    let mut iter = row_iterator(&mut cursor, &parsed).expect("construct row iterator");
    assert_rows_from_iter(&mut iter, &["AAAA", "BBBB"]);
}

#[test]
fn page_range_limits_iteration_to_requested_pages() {
    let row_length = 4usize;
    let page_size = 64usize;
    let page_one = make_data_page(
        &[b"A   ".as_slice(), b"B   ".as_slice()],
        row_length,
        page_size,
    );
    let page_two = make_data_page(
        &[b"C   ".as_slice(), b"D   ".as_slice()],
        row_length,
        page_size,
    );
    let mut bytes = Vec::with_capacity(page_one.len() + page_two.len());
    bytes.extend_from_slice(&page_one);
    bytes.extend_from_slice(&page_two);
    let parsed = make_parsed_metadata_with_page_count(
        Vendor::Sas,
        Compression::None,
        u32::try_from(row_length).expect("row length fits in u32"),
        4,
        2,
        u32::try_from(page_size).expect("page size fits in u32"),
        2,
    );

    let mut cursor = Cursor::new(bytes);
    let mut iter = row_iterator(&mut cursor, &parsed).expect("construct row iterator");
    iter.set_page_range(1, 2).expect("set page range");
    assert_rows_from_iter(&mut iter, &["C", "D"]);
}

#[test]
fn page_range_rejects_out_of_bounds_values() {
    let row_length = 4usize;
    let rows = [b"AAAA".as_slice(), b"BBBB".as_slice()];
    let (mut cursor, parsed) = setup_data_iter(&rows, row_length);
    let mut iter = row_iterator(&mut cursor, &parsed).expect("construct row iterator");

    assert!(iter.set_page_range(0, 2).is_err());
    assert!(iter.set_page_range(1, 0).is_err());
}

#[test]
fn columnar_batch_uses_borrowed_rows() {
    let row_length = 4usize;
    let rows = [b"A   ".as_slice(), b"B   ".as_slice()];
    let (mut cursor, parsed) = setup_data_iter(&rows, row_length);
    let mut iter = row_iterator(&mut cursor, &parsed).expect("construct row iterator");

    let batch = iter
        .next_columnar_batch(COLUMNAR_BATCH_ROWS)
        .expect("batch ok")
        .expect("batch present");
    assert_eq!(batch.row_count, 2);

    let col = batch.column(0).expect("column present");
    let texts: Vec<_> = col
        .iter_strings()
        .map(|opt| opt.map(std::borrow::Cow::into_owned))
        .collect();
    assert_eq!(texts, vec![Some("A".to_string()), Some("B".to_string())]);
}

#[test]
fn segmented_batch_supports_utf8_staging() {
    let row_length = 4usize;
    let rows = [b"A   ".as_slice(), b"B   ".as_slice()];
    let (mut cursor, parsed) = setup_data_iter(&rows, row_length);
    let mut iter = row_iterator(&mut cursor, &parsed).expect("construct row iterator");

    let batch = iter
        .next_columnar_batch(COLUMNAR_BATCH_ROWS)
        .expect("batch ok")
        .expect("batch present");
    let staged = batch
        .materialize_utf8(0)
        .expect("utf8 staging should succeed")
        .expect("utf8 staging should be enabled");

    assert_eq!(staged.len(), 2);
    assert_eq!(staged.non_null_count(), 2);
}

#[test]
fn segmented_and_contiguous_batches_are_equivalent_across_pages() {
    let row_length = 4usize;
    let page_size = 64usize;
    let page_one = make_data_page(
        &[b"A   ".as_slice(), b"B   ".as_slice()],
        row_length,
        page_size,
    );
    let page_two = make_data_page(
        &[b"C   ".as_slice(), b"D   ".as_slice()],
        row_length,
        page_size,
    );
    let mut bytes = Vec::with_capacity(page_one.len() + page_two.len());
    bytes.extend_from_slice(&page_one);
    bytes.extend_from_slice(&page_two);

    let parsed = make_parsed_metadata_with_page_count(
        Vendor::Sas,
        Compression::None,
        u32::try_from(row_length).expect("row length fits in u32"),
        4,
        2,
        u32::try_from(page_size).expect("page size fits in u32"),
        2,
    );

    let mut segmented_cursor = Cursor::new(bytes.clone());
    let mut segmented_iter =
        row_iterator(&mut segmented_cursor, &parsed).expect("construct segmented iterator");
    let segmented = segmented_iter
        .next_columnar_batch(4)
        .expect("segmented batch")
        .expect("segmented rows");

    let mut contiguous_cursor = Cursor::new(bytes);
    let mut contiguous_iter =
        row_iterator(&mut contiguous_cursor, &parsed).expect("construct contiguous iterator");
    let contiguous = contiguous_iter
        .next_columnar_batch_contiguous(4)
        .expect("contiguous batch")
        .expect("contiguous rows");

    assert_eq!(segmented.row_count, contiguous.row_count);
    assert_eq!(segmented.row_count, 4);

    let segmented_col = segmented.column(0).expect("segmented column");
    let contiguous_col = contiguous.column(0).expect("contiguous column");

    let segmented_values: Vec<_> = segmented_col
        .iter_strings()
        .map(|value| value.map(Cow::into_owned))
        .collect();
    let contiguous_values: Vec<_> = contiguous_col
        .iter_strings()
        .map(|value| value.map(Cow::into_owned))
        .collect();

    assert_eq!(segmented_values, contiguous_values);
}

#[test]
fn adaptive_mode_matches_contiguous_on_narrow_rows() {
    let row_length = 4usize;
    let page_size = 64usize;
    let page_one = make_data_page(
        &[b"A   ".as_slice(), b"B   ".as_slice()],
        row_length,
        page_size,
    );
    let page_two = make_data_page(
        &[b"C   ".as_slice(), b"D   ".as_slice()],
        row_length,
        page_size,
    );
    let mut bytes = Vec::with_capacity(page_one.len() + page_two.len());
    bytes.extend_from_slice(&page_one);
    bytes.extend_from_slice(&page_two);

    let parsed = make_parsed_metadata_with_page_count(
        Vendor::Sas,
        Compression::None,
        u32::try_from(row_length).expect("row length fits in u32"),
        4,
        2,
        u32::try_from(page_size).expect("page size fits in u32"),
        2,
    );

    let mut adaptive_cursor = Cursor::new(bytes.clone());
    let mut adaptive_iter =
        row_iterator(&mut adaptive_cursor, &parsed).expect("construct adaptive iterator");
    let mut adaptive_modes = Vec::new();
    let mut adaptive_values = Vec::new();
    while let Some(batch) = adaptive_iter
        .next_columnar_batch_with_mode(1, ColumnarBatchMode::Adaptive)
        .expect("adaptive batch")
    {
        let values: Vec<_> = batch
            .column(0)
            .expect("adaptive column")
            .iter_strings()
            .map(|value| value.map(Cow::into_owned))
            .collect();
        adaptive_values.extend(values);
        drop(batch);
        adaptive_modes.push(adaptive_iter.last_columnar_batch_mode());
    }

    assert!(!adaptive_modes.is_empty());
    assert!(
        adaptive_modes
            .iter()
            .all(|mode| *mode == ColumnarBatchMode::Contiguous)
    );
    assert_eq!(adaptive_iter.columnar_batch_mode_observations(), 5);
    assert_eq!(adaptive_iter.columnar_batch_mode_switches(), 0);

    let mut contiguous_cursor = Cursor::new(bytes);
    let mut contiguous_iter =
        row_iterator(&mut contiguous_cursor, &parsed).expect("construct contiguous iterator");
    let mut contiguous_values = Vec::new();
    while let Some(batch) = contiguous_iter
        .next_columnar_batch_contiguous(1)
        .expect("contiguous batch")
    {
        let values: Vec<_> = batch
            .column(0)
            .expect("contiguous column")
            .iter_strings()
            .map(|value| value.map(Cow::into_owned))
            .collect();
        contiguous_values.extend(values);
    }

    assert_eq!(adaptive_values, contiguous_values);
}

#[test]
fn adaptive_mode_matches_segmented_on_wide_rows() {
    let row_length = 256usize;
    let page_size = 1024usize;
    let row_a = vec![b'A'; row_length];
    let row_b = vec![b'B'; row_length];
    let rows = [row_a.as_slice(), row_b.as_slice()];
    let page = make_data_page(&rows, row_length, page_size);

    let parsed = make_parsed_metadata(
        Vendor::Sas,
        Compression::None,
        u32::try_from(row_length).expect("row length fits in u32"),
        2,
        2,
        u32::try_from(page_size).expect("page size fits in u32"),
    );

    let mut adaptive_cursor = Cursor::new(page.clone());
    let mut adaptive_iter =
        row_iterator(&mut adaptive_cursor, &parsed).expect("construct adaptive iterator");
    let adaptive = adaptive_iter
        .next_columnar_batch_with_mode(2, ColumnarBatchMode::Adaptive)
        .expect("adaptive batch")
        .expect("adaptive rows");

    let mut segmented_cursor = Cursor::new(page);
    let mut segmented_iter =
        row_iterator(&mut segmented_cursor, &parsed).expect("construct segmented iterator");
    let segmented = segmented_iter
        .next_columnar_batch(2)
        .expect("segmented batch")
        .expect("segmented rows");

    let adaptive_values: Vec<_> = adaptive
        .column(0)
        .expect("adaptive column")
        .iter_strings()
        .map(|value| value.map(Cow::into_owned))
        .collect();
    drop(adaptive);
    assert_eq!(
        adaptive_iter.last_columnar_batch_mode(),
        ColumnarBatchMode::Segmented
    );
    assert_eq!(adaptive_iter.columnar_batch_mode_observations(), 1);
    assert_eq!(adaptive_iter.columnar_batch_mode_switches(), 0);
    let segmented_values: Vec<_> = segmented
        .column(0)
        .expect("segmented column")
        .iter_strings()
        .map(|value| value.map(Cow::into_owned))
        .collect();

    assert_eq!(adaptive_values, segmented_values);
}

#[test]
fn decompresses_row_compression_page_rle() {
    // Control 0xC1 + 'A' inserts 4 bytes of 'A' (row length 4).
    let compressed = [0xC1u8, b'A'];
    let page = make_compressed_page(&compressed, 4, 96, super::constants::SAS_COMPRESSION_ROW);
    let parsed = make_parsed_metadata(Vendor::Sas, Compression::Row, 4, 1, 1, 96);
    assert_rows_from_page(page, &parsed, &["AAAA"]);
}

#[test]
fn comp_table_pages_are_skipped() {
    let mut page = vec![0u8; 64];
    // High-bit comp-table page type (0x8000).
    page[(24 - 8)..(24 - 6)].copy_from_slice(&0x8000u16.to_le_bytes());
    // Force a nonzero subheader count to catch accidental parsing.
    page[(24 - 4)..(24 - 2)].copy_from_slice(&1u16.to_le_bytes());

    let parsed = make_parsed_metadata(Vendor::Sas, Compression::None, 4, 1, 1, 64);
    let mut cursor = Cursor::new(page);
    let mut iter = row_iterator(&mut cursor, &parsed).expect("construct row iterator");

    assert!(iter.try_next().expect("skip comp-table").is_none());
}

#[test]
fn decompresses_binary_compression_page_rdc() {
    // Prefix of 0x0000 followed by 4 literals "BCDE".
    let mut compressed = Vec::new();
    compressed.extend_from_slice(&0u16.to_be_bytes());
    compressed.extend_from_slice(b"BCDE");
    let page = make_compressed_page(&compressed, 4, 96, super::constants::SAS_COMPRESSION_ROW);
    let parsed = make_parsed_metadata(Vendor::Sas, Compression::Binary, 4, 1, 1, 96);
    assert_rows_from_page(page, &parsed, &["BCDE"]);
}

#[test]
fn invalid_pointer_before_data_section_is_ignored() {
    // A DATA page declaring a pointer that starts inside the pointer table; it should be skipped
    // and rows should be read from the regular data region.
    let row_length = 4usize;
    let mut page = vec![0u8; 96];
    init_data_page_header(&mut page, 1, 1);

    // Pointer claims data at offset 0 (inside header), length 8.
    let mut pointer = [0u8; 12];
    pointer[0..4].copy_from_slice(&0u32.to_le_bytes());
    pointer[4..8].copy_from_slice(&8u32.to_le_bytes());
    page[24..36].copy_from_slice(&pointer);

    let pointer_section_len = 12usize;
    let bit_offset = 16usize;
    let alignment_base =
        bit_offset + super::constants::SUBHEADER_POINTER_OFFSET + pointer_section_len;
    let align_adjust = if alignment_base.is_multiple_of(8) {
        0
    } else {
        8 - (alignment_base % 8)
    };
    let data_start = (24 + pointer_section_len).saturating_add(align_adjust);
    page[data_start..data_start + 4].copy_from_slice(b"GOOD");

    let parsed = make_parsed_metadata(
        Vendor::Sas,
        Compression::None,
        u32::try_from(row_length).expect("row length fits u32"),
        1,
        1,
        96,
    );
    assert_rows_from_page(page, &parsed, &["GOOD"]);
}

fn init_data_page_header(page: &mut [u8], row_count: u16, pointer_count: u16) {
    page[(24 - 8)..(24 - 6)].copy_from_slice(&SAS_PAGE_TYPE_DATA.to_le_bytes());
    page[(24 - 6)..(24 - 4)].copy_from_slice(&row_count.to_le_bytes());
    page[(24 - 4)..(24 - 2)].copy_from_slice(&pointer_count.to_le_bytes());
}

#[test]
fn mix_pages_honor_rows_per_page_limit() {
    let row_length = 4usize;
    let rows = [b"M1  ".as_slice(), b"M2  ".as_slice()];
    let page = make_mix_page(&rows, row_length, 64);
    // rows_per_page = 1 should cap output to the first row even though 2 exist in the page.
    let parsed = make_parsed_metadata(
        Vendor::StatTransfer,
        Compression::None,
        u32::try_from(row_length).expect("row length fits u32"),
        1,
        1,
        64,
    );
    assert_rows_from_page(page, &parsed, &["M1"]);
}

#[test]
fn decode_respects_encoding_and_trimming() {
    let encoding = Encoding::for_label(b"windows-1252").unwrap();
    let text = super::decode::decode_string(b"\xC9clair  ", encoding);
    assert_eq!(text, "Éclair");
}

#[test]
fn blank_strings_preserve_empty_text() {
    assert_eq!(
        super::decode::decode_string(b"   \0\0", Encoding::for_label(b"utf-8").unwrap()),
        Cow::Borrowed("")
    );
}

#[test]
fn fixes_mojibake_sequences() {
    let encoding = Encoding::for_label(b"windows-1252").unwrap();
    let repaired = super::decode::decode_string(b"\xE9\xAB\x98\xE9\x9B\x84\xE5\xB8\x82", encoding);
    assert_eq!(repaired, "高雄市");
}

#[test]
fn resolves_mac_aliases() {
    let encoding = resolve_encoding(Some("MACCYRILLIC"));
    assert_eq!(encoding.name(), "x-mac-cyrillic");
}
