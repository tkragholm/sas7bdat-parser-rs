use super::{
    builder::ColumnMetadataBuilder,
    column_info::ColumnKind,
    row_info::RowInfoRaw,
    text_store::TextRef,
};
use crate::{
    dataset::{Alignment, Endianness, Measure},
    error::{Error, Result, Section},
    parser::core::byteorder::{read_i16, read_u16, read_u32, read_u64},
};
use std::{borrow::Cow, convert::TryFrom};

const COLUMN_LIST_HEADER_LEN: usize = 30;

pub fn expected_remainder(len: usize, signature_len: usize) -> Option<u16> {
    let base = 4 + 2 * signature_len;
    if len < base {
        return None;
    }
    let remainder = len - base;
    u16::try_from(remainder).ok()
}

#[inline]
const fn subheader_entries(len: usize, uses_u64: bool, chunk_width: usize) -> usize {
    let base = if uses_u64 { 28 } else { 20 };
    len.saturating_sub(base) / chunk_width
}

pub fn parse_text_ref(endian: Endianness, bytes: &[u8]) -> TextRef {
    TextRef {
        index: read_u16(endian, &bytes[0..2]),
        offset: read_u16(endian, &bytes[2..4]),
        length: read_u16(endian, &bytes[4..6]),
    }
}

pub fn parse_column_text_subheader(
    builder: &mut ColumnMetadataBuilder,
    bytes: &[u8],
    signature_len: usize,
    endian: Endianness,
) -> Result<()> {
    let messages = SubheaderValidationMessages {
        too_short: "column text subheader too short",
        length_invalid: "column text subheader length invalid",
        remainder_mismatch: "column text remainder mismatch",
    };
    validate_subheader_lengths(bytes, signature_len, endian, signature_len + 2, messages)?;

    let blob = &bytes[signature_len..];
    builder.text_store_mut().push_blob(blob);
    Ok(())
}

#[derive(Clone, Copy)]
struct SubheaderValidationMessages {
    too_short: &'static str,
    length_invalid: &'static str,
    remainder_mismatch: &'static str,
}

enum ColumnSubheaderKind {
    Name,
    Attributes,
}

impl ColumnSubheaderKind {
    const fn messages(self) -> SubheaderValidationMessages {
        match self {
            Self::Name => SubheaderValidationMessages {
                too_short: "column name subheader too short",
                length_invalid: "column name subheader length invalid",
                remainder_mismatch: "column name remainder mismatch",
            },
            Self::Attributes => SubheaderValidationMessages {
                too_short: "column attributes subheader too short",
                length_invalid: "column attributes subheader length invalid",
                remainder_mismatch: "column attributes remainder mismatch",
            },
        }
    }

    fn validate(
        self,
        bytes: &[u8],
        signature_len: usize,
        endian: Endianness,
        uses_u64: bool,
    ) -> Result<()> {
        let base = if uses_u64 { 28 } else { 20 };
        validate_subheader_lengths(bytes, signature_len, endian, base, self.messages())
    }
}

fn validate_subheader_lengths(
    bytes: &[u8],
    signature_len: usize,
    endian: Endianness,
    base_len: usize,
    messages: SubheaderValidationMessages,
) -> Result<()> {
    if bytes.len() < base_len {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from(messages.too_short),
        });
    }
    let remainder = read_u16(endian, &bytes[signature_len..signature_len + 2]);
    let expected =
        expected_remainder(bytes.len(), signature_len).ok_or_else(|| Error::Corrupted {
            section: Section::Header,
            details: Cow::from(messages.length_invalid),
        })?;
    if remainder != expected {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from(messages.remainder_mismatch),
        });
    }
    Ok(())
}

pub fn parse_column_name_subheader(
    builder: &mut ColumnMetadataBuilder,
    bytes: &[u8],
    signature_len: usize,
    endian: Endianness,
    uses_u64: bool,
) -> Result<()> {
    ColumnSubheaderKind::Name.validate(bytes, signature_len, endian, uses_u64)?;

    let chunk_width = 8;
    let entries = subheader_entries(bytes.len(), uses_u64, chunk_width);

    if entries == 0 {
        return Ok(());
    }

    let expected_len = signature_len + 8 + entries * chunk_width;
    if bytes.len() < expected_len {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("column name subheader truncated"),
        });
    }

    let mut cursor = signature_len + 8;
    let start_index = builder.names_seen();
    for offset in 0..entries {
        let text_ref = parse_text_ref(endian, &bytes[cursor..cursor + 6]);
        let column_index =
            u32::try_from(start_index + offset).map_err(|_| Error::InvalidMetadata {
                details: Cow::from("column index exceeds supported range"),
            })?;
        let column = builder.ensure_column(column_index);
        column.name_ref = text_ref;
        cursor += chunk_width;
    }
    builder.note_names_processed(entries);
    Ok(())
}

pub fn parse_column_attrs_subheader(
    builder: &mut ColumnMetadataBuilder,
    bytes: &[u8],
    signature_len: usize,
    endian: Endianness,
    uses_u64: bool,
) -> Result<()> {
    ColumnSubheaderKind::Attributes.validate(bytes, signature_len, endian, uses_u64)?;

    let row_size = if uses_u64 { 16 } else { 12 };
    let entries = subheader_entries(bytes.len(), uses_u64, row_size);
    if entries == 0 {
        return Ok(());
    }

    let expected_len = signature_len + 8 + entries * row_size;
    if bytes.len() < expected_len {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("column attributes subheader truncated"),
        });
    }

    let mut cursor = signature_len + 8;
    let start_index = builder.attrs_seen();
    for offset in 0..entries {
        let column_index =
            u32::try_from(start_index + offset).map_err(|_| Error::InvalidMetadata {
                details: Cow::from("column index exceeds supported range"),
            })?;
        builder.ensure_column(column_index);
        let entry_start = cursor;
        let (offset_value, width_value, type_pos, next_cursor, measure_byte_pos) = if uses_u64 {
            let offset = read_u64(endian, &bytes[entry_start..entry_start + 8]);
            let width = read_u32(endian, &bytes[entry_start + 8..entry_start + 12]);
            let next_cursor = entry_start + 16;
            let measure_pos = entry_start + 8 + 5;
            let measure_pos = if measure_pos < next_cursor {
                Some(measure_pos)
            } else {
                None
            };
            (offset, width, entry_start + 14, next_cursor, measure_pos)
        } else {
            let offset = u64::from(read_u32(endian, &bytes[entry_start..entry_start + 4]));
            let width = read_u32(endian, &bytes[entry_start + 4..entry_start + 8]);
            let next_cursor = entry_start + 12;
            let measure_pos = entry_start + 4 + 5;
            let measure_pos = if measure_pos < next_cursor {
                Some(measure_pos)
            } else {
                None
            };
            (offset, width, entry_start + 10, next_cursor, measure_pos)
        };

        builder.update_max_width(width_value);
        {
            let column = builder.column_mut(column_index);
            column.offsets.offset = offset_value;
            column.offsets.width = width_value;
        }

        let column_type = bytes[type_pos];
        {
            let column = builder.column_mut(column_index);
            column.kind =
                ColumnKind::from_type_code(column_type).ok_or_else(|| Error::Corrupted {
                    section: Section::Column {
                        index: column_index,
                    },
                    details: Cow::from("unknown column type code"),
                })?;
            if let Some(measure_pos) = measure_byte_pos {
                let measure_algn = bytes[measure_pos];
                column.measure = match measure_algn & 0x0F {
                    1 => Measure::Nominal,
                    2 => Measure::Ordinal,
                    3 => Measure::Scale,
                    _ => Measure::Unknown,
                };
                column.alignment = match (measure_algn >> 4) & 0x0F {
                    1 => Alignment::Left,
                    2 => Alignment::Center,
                    3 => Alignment::Right,
                    _ => Alignment::Unknown,
                };
            }
        }

        cursor = next_cursor;
    }
    builder.note_attrs_processed(entries);
    Ok(())
}

pub fn parse_column_list_subheader(
    builder: &mut ColumnMetadataBuilder,
    bytes: &[u8],
    signature_len: usize,
    endian: Endianness,
    uses_u64: bool,
) -> Result<()> {
    if uses_u64 || signature_len != 4 {
        // Column list parsing for u64 alignment is currently unsupported.
        return Ok(());
    }

    if bytes.len() < COLUMN_LIST_HEADER_LEN {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("column list subheader too short"),
        });
    }

    let signature = read_u32(endian, &bytes[0..4]);
    if signature != 0xFFFF_FFFE {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("unexpected signature for column list subheader"),
        });
    }

    let list_len = usize::from(read_u16(endian, &bytes[18..20]));
    if list_len == 0 {
        return Ok(());
    }
    let values_offset = COLUMN_LIST_HEADER_LEN;
    let required = values_offset + list_len * 2;
    if bytes.len() < required {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("column list subheader truncated"),
        });
    }

    let mut values = Vec::with_capacity(list_len);
    for idx in 0..list_len {
        let pos = values_offset + idx * 2;
        values.push(read_i16(endian, &bytes[pos..pos + 2]));
    }

    // Preserve the first observed column list only; later duplicates may appear when
    // a list is split across pages. When we encounter a second fragment, extend the
    // existing values rather than overwriting them.
    builder.append_column_list(values);
    Ok(())
}

pub fn parse_column_format_subheader(
    builder: &mut ColumnMetadataBuilder,
    bytes: &[u8],
    endian: Endianness,
    uses_u64: bool,
) -> Result<()> {
    let min_len = if uses_u64 { 58 } else { 46 };
    if bytes.len() < min_len {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("column format subheader too short"),
        });
    }

    let column_index =
        u32::try_from(builder.formats_seen()).map_err(|_| Error::InvalidMetadata {
            details: Cow::from("column index exceeds supported range"),
        })?;
    let column = builder.ensure_column(column_index);

    let (format_ref, label_ref) = if uses_u64 {
        (
            parse_text_ref(endian, &bytes[46..52]),
            parse_text_ref(endian, &bytes[52..58]),
        )
    } else {
        (
            parse_text_ref(endian, &bytes[34..40]),
            parse_text_ref(endian, &bytes[40..46]),
        )
    };

    if uses_u64 {
        column.format_width = Some(read_u16(endian, &bytes[24..26]));
        column.format_decimals = Some(read_u16(endian, &bytes[26..28]));
    }
    column.format_ref = format_ref;
    column.label_ref = label_ref;

    builder.note_formats_processed();
    Ok(())
}

pub fn parse_column_size_subheader(
    builder: &mut ColumnMetadataBuilder,
    bytes: &[u8],
    endian: Endianness,
    uses_u64: bool,
) -> Result<u32> {
    let min_len = if uses_u64 { 16 } else { 8 };
    if bytes.len() < min_len {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("column size subheader too short"),
        });
    }

    let raw_count = if uses_u64 {
        read_u64(endian, &bytes[8..16])
    } else {
        u64::from(read_u32(endian, &bytes[4..8]))
    };

    let column_count = u32::try_from(raw_count).map_err(|_| Error::InvalidMetadata {
        details: Cow::from("column count exceeds supported range"),
    })?;

    builder.set_column_count(column_count);
    for idx in 0..column_count {
        builder.ensure_column(idx);
    }

    Ok(column_count)
}

pub fn parse_row_size_subheader(
    bytes: &[u8],
    _signature_len: usize,
    endian: Endianness,
    uses_u64: bool,
) -> Result<RowInfoRaw> {
    let min_len = if uses_u64 { 250 } else { 190 };
    if bytes.len() < min_len {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("row size subheader too short"),
        });
    }

    let row_length_raw = if uses_u64 {
        read_u64(endian, &bytes[40..48])
    } else {
        u64::from(read_u32(endian, &bytes[20..24]))
    };
    let total_rows = if uses_u64 {
        read_u64(endian, &bytes[48..56])
    } else {
        u64::from(read_u32(endian, &bytes[24..28]))
    };
    let rows_per_page = if uses_u64 {
        read_u64(endian, &bytes[120..128])
    } else {
        u64::from(read_u32(endian, &bytes[60..64]))
    };

    let row_length = u32::try_from(row_length_raw).map_err(|_| Error::InvalidMetadata {
        details: Cow::from("row length exceeds supported range"),
    })?;

    let label_ref_offset = bytes
        .len()
        .checked_sub(130)
        .ok_or_else(|| Error::Corrupted {
            section: Section::Header,
            details: Cow::from("row size subheader missing file label reference"),
        })?;
    let compression_ref_offset = bytes
        .len()
        .checked_sub(118)
        .ok_or_else(|| Error::Corrupted {
            section: Section::Header,
            details: Cow::from("row size subheader missing compression reference"),
        })?;

    let label_ref = parse_text_ref(endian, &bytes[label_ref_offset..label_ref_offset + 6]);
    let compression_ref = parse_text_ref(
        endian,
        &bytes[compression_ref_offset..compression_ref_offset + 6],
    );

    Ok(RowInfoRaw {
        row_length,
        total_rows,
        rows_per_page,
        compression_ref,
        label_ref,
    })
}
