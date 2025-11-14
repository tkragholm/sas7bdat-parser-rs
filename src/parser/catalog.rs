use std::borrow::Cow;
use std::cmp::min;
use std::convert::TryFrom;
use std::io::{Read, Seek, SeekFrom};

use encoding_rs::Encoding;

use super::byteorder::{read_u16, read_u32, read_u64, read_u64_be};
use super::encoding::{resolve_encoding, trim_trailing};
use crate::error::{Error, Result, Section};
use crate::metadata::{LabelSet, ValueKey, ValueLabel, ValueType};
use crate::parser::header::{parse_header, SasHeader};

const SAS_CATALOG_FIRST_INDEX_PAGE: u64 = 1;
const SAS_CATALOG_USELESS_PAGES: u64 = 3;

pub struct ParsedCatalog {
    pub header: SasHeader,
    pub label_sets: Vec<LabelSet>,
}

/// Parses a SAS catalog (`.sas7bcat`).
///
/// # Errors
///
/// Returns an error if the catalog metadata cannot be read or decoded.
pub fn parse_catalog<R: Read + Seek>(reader: &mut R) -> Result<ParsedCatalog> {
    let header = parse_header(reader)?;
    let encoding = resolve_encoding(header.metadata.file_encoding.as_deref());

    let index = CatalogueIndex::build(reader, &header)?;
    let label_sets = index.parse_label_sets(reader, &header, encoding)?;

    Ok(ParsedCatalog { header, label_sets })
}

struct CatalogueIndex {
    block_pointers: Vec<u64>,
}

impl CatalogueIndex {
    fn build<R: Read + Seek>(reader: &mut R, header: &SasHeader) -> Result<Self> {
        let mut pointers = Vec::new();
        let cfg = IndexLayout::new(header);
        let mut page = vec![0u8; header.page_size as usize];

        if header.page_count <= SAS_CATALOG_FIRST_INDEX_PAGE {
            return Ok(Self {
                block_pointers: pointers,
            });
        }

        read_page(reader, header, SAS_CATALOG_FIRST_INDEX_PAGE, &mut page)?;
        augment_index(&page[cfg.index_start_offset..], header, &cfg, &mut pointers);

        for index in SAS_CATALOG_USELESS_PAGES..header.page_count {
            read_page(reader, header, index, &mut page)?;
            if page.len() < 16 {
                continue;
            }
            if &page[16..20] == b"XLSR" {
                augment_index(&page[16..], header, &cfg, &mut pointers);
            }
        }

        pointers.sort_unstable();
        pointers.dedup();

        Ok(Self {
            block_pointers: pointers,
        })
    }

    fn parse_label_sets<R: Read + Seek>(
        &self,
        reader: &mut R,
        header: &SasHeader,
        encoding: &'static Encoding,
    ) -> Result<Vec<LabelSet>> {
        let mut label_sets = Vec::new();
        for pointer in &self.block_pointers {
            let block = read_block(reader, header, *pointer)?;
            if let Some(set) = parse_block(&block, header, encoding)? {
                label_sets.push(set);
            }
        }
        Ok(label_sets)
    }
}

struct IndexLayout {
    entry_stride: usize,
    index_start_offset: usize,
    object_marker_offset: usize,
}

impl IndexLayout {
    const fn new(header: &SasHeader) -> Self {
        let pad = header.pad_alignment as usize;
        let mut entry_stride = 212 + pad;
        let mut index_start_offset = 856 + 2 * pad;
        let mut object_marker_offset = 50 + pad;
        if header.uses_u64 {
            entry_stride += 72;
            index_start_offset += 144;
            object_marker_offset += 24;
        }
        Self {
            entry_stride,
            index_start_offset,
            object_marker_offset,
        }
    }
}

fn read_page<R: Read + Seek>(
    reader: &mut R,
    header: &SasHeader,
    index: u64,
    buffer: &mut [u8],
) -> Result<()> {
    let offset = header.data_offset + index * u64::from(header.page_size);
    reader.seek(SeekFrom::Start(offset))?;
    reader.read_exact(buffer)?;
    Ok(())
}

fn augment_index(buffer: &[u8], header: &SasHeader, layout: &IndexLayout, pointers: &mut Vec<u64>) {
    let mut cursor = 0usize;
    while cursor + layout.entry_stride <= buffer.len() {
        let entry = &buffer[cursor..cursor + layout.entry_stride];
        if &entry[0..4] != b"XLSR" {
            cursor += 8;
            continue;
        }
        if entry.len() <= layout.object_marker_offset || entry[layout.object_marker_offset] != b'O'
        {
            cursor += layout.entry_stride;
            continue;
        }

        let (page, pos) = if header.uses_u64 {
            let page = read_u64(header.endianness, &entry[8..16]);
            let pos = read_u16(header.endianness, &entry[16..18]);
            (page, pos)
        } else {
            let page = u64::from(read_u32(header.endianness, &entry[4..8]));
            let pos = read_u16(header.endianness, &entry[8..10]);
            (page, pos)
        };

        if page > 0 && pos > 0 && page <= header.page_count + 1 {
            pointers.push((page << 32) | u64::from(pos));
        }

        cursor += layout.entry_stride;
    }
}

fn read_block<R: Read + Seek>(reader: &mut R, header: &SasHeader, pointer: u64) -> Result<Vec<u8>> {
    let (mut page, mut pos) = decode_pointer(pointer);
    if page == 0 || pos == 0 {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("catalog block pointer references invalid page"),
        });
    }

    let header_len = if header.uses_u64 { 32 } else { 16 };
    let mut total_len = 0usize;
    let mut link_count = 0u64;

    loop {
        if page == 0 || pos == 0 || page > header.page_count || link_count > header.page_count {
            break;
        }
        let mut link_header = vec![0u8; header_len];
        read_chain_segment(reader, header, page, pos, &mut link_header)?;

        let (next_page, next_pos, segment_len) = decode_chain_header(&link_header, header);
        total_len += segment_len as usize;
        if next_page == 0 || next_pos == 0 {
            break;
        }
        page = next_page;
        pos = next_pos;
        link_count += 1;
    }

    if total_len == 0 {
        return Ok(Vec::new());
    }

    let mut buffer = vec![0u8; total_len];
    page = decode_pointer(pointer).0;
    pos = decode_pointer(pointer).1;
    let mut offset = 0usize;
    link_count = 0;

    loop {
        if page == 0 || pos == 0 || page > header.page_count || link_count > header.page_count {
            break;
        }
        let mut link_header = vec![0u8; header_len];
        read_chain_segment(reader, header, page, pos, &mut link_header)?;
        let (next_page, next_pos, segment_len) = decode_chain_header(&link_header, header);
        if segment_len == 0 {
            break;
        }
        if offset + segment_len as usize > buffer.len() {
            return Err(Error::Corrupted {
                section: Section::Page { index: page },
                details: Cow::from("catalog chain exceeds allocated buffer"),
            });
        }
        read_segment_data(
            reader,
            header,
            page,
            pos,
            header_len,
            &mut buffer[offset..offset + segment_len as usize],
        )?;
        offset += segment_len as usize;
        if next_page == 0 || next_pos == 0 {
            break;
        }
        page = next_page;
        pos = next_pos;
        link_count += 1;
    }

    buffer.truncate(offset);
    Ok(buffer)
}

fn parse_block(
    buffer: &[u8],
    header: &SasHeader,
    encoding: &'static Encoding,
) -> Result<Option<LabelSet>> {
    const BASE_PAYLOAD_OFFSET: usize = 106;
    if buffer.len() < BASE_PAYLOAD_OFFSET {
        return Ok(None);
    }

    let flags = read_u16(header.endianness, &buffer[2..4]);
    let mut pad = if flags & 0x08 != 0 { 4 } else { 0 };
    let mut payload_offset = BASE_PAYLOAD_OFFSET;
    let is_string = buffer.get(8).is_some_and(|b| *b == b'$');

    let label_count_capacity: u64;
    let label_count_used: u64;
    if header.uses_u64 {
        label_count_capacity = read_u64(header.endianness, &buffer[42 + pad..50 + pad]);
        label_count_used = read_u64(header.endianness, &buffer[50 + pad..58 + pad]);
        payload_offset += 32;
    } else {
        label_count_capacity = u64::from(read_u32(header.endianness, &buffer[38 + pad..42 + pad]));
        label_count_used = u64::from(read_u32(header.endianness, &buffer[42 + pad..46 + pad]));
    }

    let mut name = decode_text(&buffer[8..16], encoding)?;
    if pad != 0 {
        pad += 16;
    }

    let has_long_name = if header.uses_u64 {
        flags & 0x20 != 0
    } else {
        flags & 0x80 != 0
    };
    if has_long_name {
        let start = payload_offset + pad;
        let end = start + 32;
        if end > buffer.len() {
            return Err(Error::Corrupted {
                section: Section::Header,
                details: Cow::from("catalog long-name block truncated"),
            });
        }
        name = decode_text(&buffer[start..end], encoding)?;
        pad += 32;
    }

    if label_count_used == 0 {
        return Ok(None);
    }

    let value_area = payload_offset + pad;
    if value_area > buffer.len() {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("catalog value block missing payload"),
        });
    }
    let value_bytes = &buffer[value_area..];

    let value_type = if is_string {
        ValueType::String
    } else {
        ValueType::Numeric
    };
    let mut label_set = LabelSet::new(name.trim_end().to_string(), value_type);

    let labels = parse_value_labels(
        value_bytes,
        header,
        encoding,
        label_count_used,
        label_count_capacity,
        value_type,
    )?;
    label_set.labels = labels;

    Ok(Some(label_set))
}

#[allow(clippy::too_many_lines)]
fn parse_value_labels(
    bytes: &[u8],
    header: &SasHeader,
    encoding: &'static Encoding,
    label_count_used: u64,
    label_count_capacity: u64,
    value_type: ValueType,
) -> Result<Vec<ValueLabel>> {
    let label_count = usize::try_from(label_count_used).map_err(|_| Error::Unsupported {
        feature: Cow::from("catalog label count exceeds platform pointer width"),
    })?;
    let capacity = usize::try_from(label_count_capacity).map_err(|_| Error::Unsupported {
        feature: Cow::from("catalog label capacity exceeds platform pointer width"),
    })?;
    let pad = usize::try_from(header.pad_alignment).map_err(|_| Error::Unsupported {
        feature: Cow::from("catalog label padding exceeds platform pointer width"),
    })?;

    let mut offsets = vec![0usize; label_count];
    let mut lbp1 = bytes;

    for i in 0..capacity {
        if lbp1.len() < 6 {
            return Err(Error::Corrupted {
                section: Section::Header,
                details: Cow::from("catalog value entry truncated"),
            });
        }
        let entry_len = usize::from(read_u16(header.endianness, &lbp1[2..4]));
        if 6 + entry_len > lbp1.len() {
            return Err(Error::Corrupted {
                section: Section::Header,
                details: Cow::from("catalog value entry exceeds block"),
            });
        }
        if i < label_count {
            let label_pos_offset = 10 + pad;
            if label_pos_offset + 4 > lbp1.len() {
                return Err(Error::Corrupted {
                    section: Section::Header,
                    details: Cow::from("catalog value entry missing label index"),
                });
            }
            let label_pos = read_u32(
                header.endianness,
                &lbp1[label_pos_offset..label_pos_offset + 4],
            );
            let label_pos = usize::try_from(label_pos).map_err(|_| Error::Corrupted {
                section: Section::Header,
                details: Cow::from("catalog label index out of range"),
            })?;
            if label_pos >= offsets.len() {
                return Err(Error::Corrupted {
                    section: Section::Header,
                    details: Cow::from("catalog label index out of range"),
                });
            }
            offsets[label_pos] = bytes.len() - lbp1.len();
        }
        let consumed = 6 + entry_len;
        if consumed > lbp1.len() {
            lbp1 = &[];
            break;
        }
        lbp1 = &lbp1[consumed..];
        if lbp1.is_empty() {
            break;
        }
    }

    let mut lbp2 = lbp1;
    let mut labels = Vec::with_capacity(label_count);

    for &entry_offset in offsets.iter().take(label_count) {
        if entry_offset + 6 > bytes.len() {
            return Err(Error::Corrupted {
                section: Section::Header,
                details: Cow::from("catalog value entry offset invalid"),
            });
        }
        let entry = &bytes[entry_offset..];
        let entry_len = usize::from(read_u16(header.endianness, &entry[2..4])) + 6;
        if entry_len > entry.len() {
            return Err(Error::Corrupted {
                section: Section::Header,
                details: Cow::from("catalog value entry truncated"),
            });
        }

        let key = match value_type {
            ValueType::String => {
                if entry_len < 16 {
                    return Err(Error::Corrupted {
                        section: Section::Header,
                        details: Cow::from("catalog string value entry too short"),
                    });
                }
                let value_bytes = &entry[entry_len - 16..entry_len];
                ValueKey::String(decode_text(value_bytes, encoding)?)
            }
            ValueType::Numeric => {
                if entry_len < 30 {
                    return Err(Error::Corrupted {
                        section: Section::Header,
                        details: Cow::from("catalog numeric value entry too short"),
                    });
                }
                let raw = read_u64_be(&entry[22..30]);
                decode_numeric_key(raw)
            }
        };

        if lbp2.len() < 10 {
            return Err(Error::Corrupted {
                section: Section::Header,
                details: Cow::from("catalog label entry truncated"),
            });
        }
        let mut label_len = usize::from(read_u16(header.endianness, &lbp2[8..10]));
        let available = lbp2.len().saturating_sub(10);
        label_len = min(label_len, available);
        let label = decode_text(&lbp2[10..10 + label_len], encoding)?;
        labels.push(ValueLabel { key, label });
        let skip = 8 + 2 + label_len + 1;
        if skip > lbp2.len() {
            lbp2 = &[];
        } else {
            lbp2 = &lbp2[skip..];
        }
    }

    Ok(labels)
}

fn read_chain_segment<R: Read + Seek>(
    reader: &mut R,
    header: &SasHeader,
    page: u64,
    pos: u64,
    buffer: &mut [u8],
) -> Result<()> {
    let offset = header.data_offset + (page - 1) * u64::from(header.page_size) + pos;
    reader.seek(SeekFrom::Start(offset))?;
    reader.read_exact(buffer)?;
    Ok(())
}

fn read_segment_data<R: Read + Seek>(
    reader: &mut R,
    header: &SasHeader,
    page: u64,
    pos: u64,
    header_len: usize,
    buffer: &mut [u8],
) -> Result<()> {
    let offset =
        header.data_offset + (page - 1) * u64::from(header.page_size) + pos + header_len as u64;
    reader.seek(SeekFrom::Start(offset))?;
    reader.read_exact(buffer)?;
    Ok(())
}

fn decode_chain_header(chunk: &[u8], header: &SasHeader) -> (u64, u64, u16) {
    let next_page = u64::from(read_u32(header.endianness, &chunk[0..4]));
    if header.uses_u64 {
        let next_pos = u64::from(read_u16(header.endianness, &chunk[8..10]));
        let seg_len = read_u16(header.endianness, &chunk[10..12]);
        (next_page, next_pos, seg_len)
    } else {
        let next_pos = u64::from(read_u16(header.endianness, &chunk[4..6]));
        let seg_len = read_u16(header.endianness, &chunk[6..8]);
        (next_page, next_pos, seg_len)
    }
}

const fn decode_pointer(pointer: u64) -> (u64, u64) {
    let page = pointer >> 32;
    let pos = pointer & 0xFFFF;
    (page, pos)
}

fn try_i32_from_f64(value: f64) -> Option<i32> {
    if !value.is_finite() {
        return None;
    }
    if value == 0.0 {
        return Some(0);
    }

    let bits = value.to_bits();
    let sign = (bits >> 63) != 0;
    let exponent_bits = ((bits >> 52) & 0x7FF) as i32;

    if exponent_bits == 0 {
        // Subnormal numbers cannot represent non-zero integers exactly.
        return None;
    }

    let exponent = exponent_bits - 1023;
    if exponent < 0 {
        return None;
    }

    let mut mantissa = bits & ((1_u64 << 52) - 1);
    mantissa |= 1_u64 << 52;

    let magnitude = if exponent >= 52 {
        let shift = u32::try_from(exponent - 52).ok()?;
        mantissa.checked_shl(shift)?
    } else {
        let shift = u32::try_from(52 - exponent).ok()?;
        mantissa >> shift
    };

    let magnitude = i64::try_from(magnitude).ok()?;
    let signed = if sign { -magnitude } else { magnitude };

    i32::try_from(signed).ok()
}

fn decode_numeric_key(raw: u64) -> ValueKey {
    if (raw | 0xFF00_0000_0000) == 0xFFFF_FFFF_FFFF {
        let tag = decode_missing_tag(u8::try_from((raw >> 40) & 0xFF).unwrap_or_default());
        ValueKey::Tagged(tag)
    } else {
        let mut value = f64::from_bits(raw);
        if value > 0.0 {
            value = f64::from_bits(!raw);
        } else {
            value = -value;
        }
        if value.fract() == 0.0 && value >= f64::from(i32::MIN) && value <= f64::from(i32::MAX) {
            try_i32_from_f64(value).map_or(ValueKey::Numeric(value), ValueKey::Integer)
        } else {
            ValueKey::Numeric(value)
        }
    }
}

const fn decode_missing_tag(tag: u8) -> char {
    match tag {
        0 => '_',
        2..=27 => (b'A' + (tag - 2)) as char,
        _ => '.',
    }
}

fn decode_text(bytes: &[u8], encoding: &'static Encoding) -> Result<String> {
    let trimmed = trim_trailing(bytes);
    if trimmed.is_empty() {
        return Ok(String::new());
    }
    String::from_utf8(trimmed.to_vec()).map_or_else(
        |_| {
            let (decoded, _, had_errors) = encoding.decode(trimmed);
            if had_errors {
                let fallback = String::from_utf8_lossy(trimmed).into_owned();
                Ok(fallback)
            } else {
                Ok(decoded.into_owned())
            }
        },
        |text| Ok(text.trim_end_matches('\u{0000}').to_string()),
    )
}
