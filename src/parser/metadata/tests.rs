use std::convert::TryFrom;

use super::builder::ColumnMetadataBuilder;
use super::column_info::ColumnKind;
use super::subheaders::{
    parse_column_attrs_subheader, parse_column_format_subheader, parse_column_list_subheader,
    parse_column_name_subheader, parse_column_text_subheader,
};
use crate::metadata::{Alignment, Endianness, Measure};
use encoding_rs::UTF_8;

#[test]
fn column_text_subheader_pushes_blob() {
    let mut builder = ColumnMetadataBuilder::new(UTF_8);
    let signature_len = 4;
    let mut bytes = vec![0u8; signature_len + 2];
    bytes[..4].copy_from_slice(&[0xFD, 0xFF, 0xFF, 0xFF]);
    bytes.extend_from_slice(b"Name\0\0");
    let remainder = u16::try_from(bytes.len() - (4 + 2 * signature_len))
        .expect("remainder fits in u16 for test data");
    bytes[signature_len..signature_len + 2].copy_from_slice(&remainder.to_le_bytes());

    parse_column_text_subheader(&mut builder, &bytes, 4, Endianness::Little).unwrap();

    assert_eq!(builder.text_store().len(), 1);
    let blob = builder.text_store().blob(0).unwrap();
    assert_eq!(blob.len(), bytes.len() - signature_len);
}

#[test]
fn column_name_subheader_sets_text_refs() {
    let mut builder = ColumnMetadataBuilder::new(UTF_8);
    builder
        .text_store_mut()
        .push_blob(&[0, 0, b'C', b'O', b'L', b'1', 0, 0]);

    let signature_len = 4;
    let mut bytes = vec![0u8; signature_len + 8];
    bytes[..4].copy_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);
    bytes.extend_from_slice(&[0x00, 0x00, 0x02, 0x00, 0x04, 0x00, 0x00, 0x00]);
    bytes.extend_from_slice(&[0u8; 8]);
    let remainder = u16::try_from(bytes.len() - (4 + 2 * signature_len))
        .expect("remainder fits in u16 for test data");
    bytes[signature_len..signature_len + 2].copy_from_slice(&remainder.to_le_bytes());
    assert_eq!(bytes.len(), 28);

    parse_column_name_subheader(&mut builder, &bytes, 4, Endianness::Little, false).unwrap();

    assert_eq!(builder.names_seen(), 1);
    let column = builder.column_mut(0);
    assert_eq!(column.name_ref.index, 0);
    assert_eq!(column.name_ref.offset, 2);
    assert_eq!(column.name_ref.length, 4);
}

#[test]
fn column_attrs_subheader_updates_offsets() {
    let mut builder = ColumnMetadataBuilder::new(UTF_8);
    let signature_len = 4;
    let mut bytes = vec![0u8; signature_len + 8];
    bytes[..4].copy_from_slice(&[0xF6, 0xF6, 0xF6, 0xF6]);
    let mut entry = [0u8; 12];
    entry[0..4].copy_from_slice(&4u32.to_le_bytes());
    entry[4..8].copy_from_slice(&8u32.to_le_bytes());
    entry[10] = 0x02;
    bytes.extend_from_slice(&entry);
    bytes.extend_from_slice(&[0u8; 8]);
    let remainder = u16::try_from(bytes.len() - (4 + 2 * signature_len))
        .expect("remainder fits in u16 for test data");
    bytes[signature_len..signature_len + 2].copy_from_slice(&remainder.to_le_bytes());
    assert_eq!(bytes.len(), 32);

    parse_column_attrs_subheader(&mut builder, &bytes, 4, Endianness::Little, false).unwrap();

    assert_eq!(builder.attrs_seen(), 1);
    assert_eq!(builder.max_width(), 8);
    let column = builder.column_mut(0);
    assert_eq!(column.offsets.offset, 4);
    assert_eq!(column.offsets.width, 8);
    assert!(matches!(column.kind, ColumnKind::Character));
}

#[test]
fn column_attrs_subheader_sets_measure_alignment() {
    let mut builder = ColumnMetadataBuilder::new(UTF_8);
    let signature_len = 4;
    let mut bytes = vec![0u8; signature_len + 8];
    bytes[..4].copy_from_slice(&[0xF6, 0xF6, 0xF6, 0xF6]);
    let mut entry = [0u8; 12];
    entry[0..4].copy_from_slice(&16u32.to_le_bytes());
    entry[4..8].copy_from_slice(&32u32.to_le_bytes());
    entry[8] = 0x00;
    entry[9] = 0x32;
    entry[10] = 0x01;
    bytes.extend_from_slice(&entry);
    bytes.extend_from_slice(&[0u8; 8]);
    let remainder = u16::try_from(bytes.len() - (4 + 2 * signature_len))
        .expect("remainder fits in u16 for test data");
    bytes[signature_len..signature_len + 2].copy_from_slice(&remainder.to_le_bytes());

    parse_column_attrs_subheader(
        &mut builder,
        &bytes,
        signature_len,
        Endianness::Little,
        false,
    )
    .unwrap();

    let column = builder.column_mut(0);
    assert_eq!(column.measure, Measure::Ordinal);
    assert_eq!(column.alignment, Alignment::Right);
}

#[test]
fn column_list_subheader_collects_values() {
    let mut builder = ColumnMetadataBuilder::new(UTF_8);
    let bytes: Vec<u8> = vec![
        0xfe, 0xff, 0xff, 0xff, 0x3c, 0x00, 0xdc, 0x7f, 0x00, 0x00, 0x00, 0x00, 0x34, 0x00, 0x00,
        0x00, 0x0d, 0x00, 0x11, 0x00, 0x01, 0x00, 0x0d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xfe, 0xff, 0xf7, 0xff, 0x0d, 0x00, 0x00, 0x00, 0xff, 0xff, 0x0c, 0x00, 0xf8, 0xff, 0x00,
        0x00, 0xfd, 0xff, 0x00, 0x00, 0xfb, 0xff, 0x00, 0x00, 0x0a, 0x00, 0x06, 0x00, 0xfc, 0xff,
        0x0b, 0x00, 0xf9, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    parse_column_list_subheader(&mut builder, &bytes, 4, Endianness::Little, false).unwrap();

    let list = builder.column_list().expect("column list captured");
    assert_eq!(list.len(), 17);
    assert_eq!(list[0], -2);
    assert_eq!(list[2], 13);
    assert_eq!(list[4], -1);
}

#[test]
fn column_format_subheader_sets_refs() {
    let mut builder = ColumnMetadataBuilder::new(UTF_8);
    builder
        .text_store_mut()
        .push_blob(&[0, 0, 0, 0, 0, 0, b'F', b'M', b'T', 0, b'L', b'B', 0, 0]);

    let mut bytes = vec![0u8; 46];
    bytes[0..4].copy_from_slice(&[0xFB, 0xFF, 0xFB, 0xFF]);
    bytes[34..40].copy_from_slice(&[0x00, 0x00, 0x06, 0x00, 0x04, 0x00]);
    bytes[40..46].copy_from_slice(&[0x00, 0x00, 0x0A, 0x00, 0x02, 0x00]);

    parse_column_format_subheader(&mut builder, &bytes, Endianness::Little, false).unwrap();

    assert_eq!(builder.formats_seen(), 1);
    let column = builder.column_mut(0);
    assert_eq!(column.format_ref.offset, 6);
    assert_eq!(column.format_ref.length, 4);
    assert_eq!(column.label_ref.offset, 10);
    assert_eq!(column.label_ref.length, 2);
}
