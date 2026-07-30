use crate::{
    encoding::resolve_encoding,
    error::{Error, HeaderError, Result},
    internal::HeaderInfo,
    labels::{LabelSet, ValueKey, ValueLabel, ValueType},
    probe::probe_header,
};
use encoding_rs::Encoding;
use std::{
    cmp::min,
    convert::TryFrom,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

#[derive(Debug)]
pub struct CatalogLayout {
    pub label_sets: Vec<LabelSet>,
}

/// Normalizes a format name for use as a `HashMap` key.
///
/// Trims whitespace, removes trailing dots, and uppercases. The leading `$` is
/// preserved because it distinguishes string formats (`$GENDER`) from numeric
/// formats (`GENDER`).
#[must_use]
pub fn normalize_format_name(name: &str) -> String {
    name.trim().trim_end_matches('.').to_ascii_uppercase()
}

/// Opens the given path and parses it as a SAS catalog (`.sas7bcat`).
///
/// # Errors
///
/// Returns an error if the file cannot be opened, the header is invalid, or the
/// catalog structure cannot be decoded.
pub fn parse_catalog_file(path: &Path) -> Result<CatalogLayout> {
    let mut file = File::open(path).map_err(|e| Error::io_error_with_path(path, &e))?;
    parse_catalog(&mut file)
}

/// Parses a SAS catalog from any `Read + Seek` source.
///
/// # Errors
///
/// Returns an error if the source is not a catalog, or if the header or catalog
/// structure cannot be decoded.
pub fn parse_catalog<R: Read + Seek>(reader: &mut R) -> Result<CatalogLayout> {
    let (header, metadata) = probe_header(reader)?;
    // Datasets and catalogs share a header layout, and `probe_header` accepts either magic.
    // Without this check a `.sas7bdat` handed to `--catalog` would decode into an empty
    // catalog and silently attach no labels at all.
    if !header.is_catalog {
        return Err(Error::Header(HeaderError::Other(
            "not a SAS catalog: file carries the sas7bdat dataset magic number".to_owned(),
        )));
    }
    let encoding = resolve_encoding(metadata.encoding.as_deref());

    let index = CatalogueIndex::build(reader, &header)?;
    let label_sets = index.parse_label_sets(reader, &header, encoding)?;

    Ok(CatalogLayout { label_sets })
}

// ─── index ────────────────────────────────────────────────────────────────────

const SAS_CATALOG_FIRST_INDEX_PAGE: u64 = 1;
const SAS_CATALOG_USELESS_PAGES: u64 = 3;

struct CatalogueIndex {
    block_pointers: Vec<u64>,
}

impl CatalogueIndex {
    fn build<R: Read + Seek>(reader: &mut R, header: &HeaderInfo) -> Result<Self> {
        let mut pointers = Vec::new();
        let cfg = IndexLayout::new(header);
        let page_size = header.page_size.0 as usize;
        let mut page = vec![0u8; page_size];

        if header.page_count <= SAS_CATALOG_FIRST_INDEX_PAGE {
            return Ok(Self {
                block_pointers: pointers,
            });
        }

        read_page(reader, header, SAS_CATALOG_FIRST_INDEX_PAGE, &mut page)?;
        augment_index(&page[cfg.index_start_offset..], header, &cfg, &mut pointers);

        for index in SAS_CATALOG_USELESS_PAGES..header.page_count {
            read_page(reader, header, index, &mut page)?;
            if page.len() < 20 {
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
        header: &HeaderInfo,
        encoding: &'static Encoding,
    ) -> Result<Vec<LabelSet>> {
        let mut label_sets = Vec::new();
        for &pointer in &self.block_pointers {
            let block = read_block(reader, header, pointer)?;
            if let Some(set) = parse_block(&block, header, encoding)? {
                label_sets.push(set);
            }
        }
        Ok(label_sets)
    }
}

// ─── index layout ─────────────────────────────────────────────────────────────

struct IndexLayout {
    entry_stride: usize,
    index_start_offset: usize,
    object_marker_offset: usize,
}

impl IndexLayout {
    const fn new(header: &HeaderInfo) -> Self {
        let pad = header.pad_alignment as usize;
        let mut entry_stride = 212 + pad;
        let mut index_start_offset = 856 + 2 * pad;
        let mut object_marker_offset = 50 + pad;
        if header.uses_u64_pointers {
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

// ─── page I/O ─────────────────────────────────────────────────────────────────

fn read_page<R: Read + Seek>(
    reader: &mut R,
    header: &HeaderInfo,
    index: u64,
    buffer: &mut [u8],
) -> Result<()> {
    let offset = header.data_offset + index * u64::from(header.page_size.0);
    reader
        .seek(SeekFrom::Start(offset))
        .map_err(|e| Error::io_error(&e))?;
    reader.read_exact(buffer).map_err(|e| Error::io_error(&e))?;
    Ok(())
}

fn augment_index(
    buffer: &[u8],
    header: &HeaderInfo,
    layout: &IndexLayout,
    pointers: &mut Vec<u64>,
) {
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

        let (page, pos) = if header.uses_u64_pointers {
            let page = read_u64_slice(header, &entry[8..16]);
            let pos = read_u16_slice(header, &entry[16..18]);
            (page, pos)
        } else {
            let page = u64::from(read_u32_slice(header, &entry[4..8]));
            let pos = read_u16_slice(header, &entry[8..10]);
            (page, pos)
        };

        if page > 0 && pos > 0 && page <= header.page_count + 1 {
            pointers.push((page << 32) | u64::from(pos));
        }

        cursor += layout.entry_stride;
    }
}

// ─── block I/O ────────────────────────────────────────────────────────────────

struct ChainSegment {
    page: u64,
    pos: u64,
    len: u16,
}

fn collect_chain_segments<R: Read + Seek>(
    reader: &mut R,
    header: &HeaderInfo,
    mut page: u64,
    mut pos: u64,
    header_len: usize,
) -> Result<Vec<ChainSegment>> {
    let mut segments = Vec::new();
    let mut link_count = 0u64;

    loop {
        if page == 0 || pos == 0 || page > header.page_count || link_count > header.page_count {
            break;
        }
        let mut link_header = vec![0u8; header_len];
        read_chain_segment(reader, header, page, pos, &mut link_header)?;

        let (next_page, next_pos, segment_len) = decode_chain_header(&link_header, header);
        segments.push(ChainSegment {
            page,
            pos,
            len: segment_len,
        });
        if next_page == 0 || next_pos == 0 {
            break;
        }
        page = next_page;
        pos = next_pos;
        link_count += 1;
    }

    Ok(segments)
}

fn read_block<R: Read + Seek>(
    reader: &mut R,
    header: &HeaderInfo,
    pointer: u64,
) -> Result<Vec<u8>> {
    let (page, pos) = decode_pointer(pointer);
    if page == 0 || pos == 0 {
        return Err(Error::page_corruption(
            "catalog block pointer references invalid page",
        ));
    }

    let header_len = if header.uses_u64_pointers { 32 } else { 16 };
    let segments = collect_chain_segments(reader, header, page, pos, header_len)?;
    let total_len: usize = segments.iter().map(|s| s.len as usize).sum();

    if total_len == 0 {
        return Ok(Vec::new());
    }

    let mut buffer = vec![0u8; total_len];
    let mut offset = 0usize;

    for segment in segments {
        if segment.len == 0 {
            break;
        }
        if offset + segment.len as usize > buffer.len() {
            return Err(Error::page_corruption(
                "catalog chain exceeds allocated buffer",
            ));
        }
        read_segment_data(
            reader,
            header,
            segment.page,
            segment.pos,
            header_len,
            &mut buffer[offset..offset + segment.len as usize],
        )?;
        offset += segment.len as usize;
    }

    buffer.truncate(offset);
    Ok(buffer)
}

fn read_chain_segment<R: Read + Seek>(
    reader: &mut R,
    header: &HeaderInfo,
    page: u64,
    pos: u64,
    buffer: &mut [u8],
) -> Result<()> {
    let offset = header.data_offset + (page - 1) * u64::from(header.page_size.0) + pos;
    reader
        .seek(SeekFrom::Start(offset))
        .map_err(|e| Error::io_error(&e))?;
    reader.read_exact(buffer).map_err(|e| Error::io_error(&e))?;
    Ok(())
}

fn read_segment_data<R: Read + Seek>(
    reader: &mut R,
    header: &HeaderInfo,
    page: u64,
    pos: u64,
    header_len: usize,
    buffer: &mut [u8],
) -> Result<()> {
    let offset =
        header.data_offset + (page - 1) * u64::from(header.page_size.0) + pos + header_len as u64;
    reader
        .seek(SeekFrom::Start(offset))
        .map_err(|e| Error::io_error(&e))?;
    reader.read_exact(buffer).map_err(|e| Error::io_error(&e))?;
    Ok(())
}

fn decode_chain_header(chunk: &[u8], header: &HeaderInfo) -> (u64, u64, u16) {
    let next_page = u64::from(read_u32_slice(header, &chunk[0..4]));
    if header.uses_u64_pointers {
        let next_pos = u64::from(read_u16_slice(header, &chunk[8..10]));
        let seg_len = read_u16_slice(header, &chunk[10..12]);
        (next_page, next_pos, seg_len)
    } else {
        let next_pos = u64::from(read_u16_slice(header, &chunk[4..6]));
        let seg_len = read_u16_slice(header, &chunk[6..8]);
        (next_page, next_pos, seg_len)
    }
}

const fn decode_pointer(pointer: u64) -> (u64, u64) {
    let page = pointer >> 32;
    let pos = pointer & 0xFFFF;
    (page, pos)
}

// ─── block parsing ────────────────────────────────────────────────────────────

fn parse_block(
    buffer: &[u8],
    header: &HeaderInfo,
    encoding: &'static Encoding,
) -> Result<Option<LabelSet>> {
    const BASE_PAYLOAD_OFFSET: usize = 106;
    if buffer.len() < BASE_PAYLOAD_OFFSET {
        return Ok(None);
    }

    let flags = read_u16_slice(header, &buffer[2..4]);
    let mut pad = if flags & 0x08 != 0 { 4 } else { 0 };
    let mut payload_offset = BASE_PAYLOAD_OFFSET;
    let is_string = buffer.get(8).is_some_and(|b| *b == b'$');

    let label_count_capacity: u64;
    let label_count_used: u64;
    if header.uses_u64_pointers {
        label_count_capacity = read_u64_slice(header, &buffer[42 + pad..50 + pad]);
        label_count_used = read_u64_slice(header, &buffer[50 + pad..58 + pad]);
        payload_offset += 32;
    } else {
        label_count_capacity = u64::from(read_u32_slice(header, &buffer[38 + pad..42 + pad]));
        label_count_used = u64::from(read_u32_slice(header, &buffer[42 + pad..46 + pad]));
    }

    let mut name = decode_text(&buffer[8..16], encoding);
    if pad != 0 {
        pad += 16;
    }

    let has_long_name = if header.uses_u64_pointers {
        flags & 0x20 != 0
    } else {
        flags & 0x80 != 0
    };
    if has_long_name {
        let start = payload_offset + pad;
        let end = start + 32;
        if end > buffer.len() {
            return Err(Error::header_corruption(
                "catalog long-name block truncated",
            ));
        }
        name = decode_text(&buffer[start..end], encoding);
        pad += 32;
    }

    if label_count_used == 0 {
        return Ok(None);
    }

    let value_area = payload_offset + pad;
    if value_area > buffer.len() {
        return Err(Error::header_corruption(
            "catalog value block missing payload",
        ));
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

fn parse_value_labels(
    bytes: &[u8],
    header: &HeaderInfo,
    encoding: &'static Encoding,
    label_count_used: u64,
    label_count_capacity: u64,
    value_type: ValueType,
) -> Result<Vec<ValueLabel>> {
    let label_count = usize::try_from(label_count_used)
        .map_err(|_| Error::unsupported("catalog label count exceeds platform pointer width"))?;
    let capacity = usize::try_from(label_count_capacity)
        .map_err(|_| Error::unsupported("catalog label capacity exceeds platform pointer width"))?;
    let pad = usize::try_from(header.pad_alignment)
        .map_err(|_| Error::unsupported("catalog label padding exceeds platform pointer width"))?;

    let (offsets, label_blob_offset) =
        parse_value_label_offsets(bytes, header, pad, label_count, capacity)?;
    parse_value_label_entries(
        bytes,
        label_blob_offset,
        &offsets,
        header,
        encoding,
        value_type,
    )
}

/// Smallest a value-label entry can be: the 6-byte entry header with a zero-length
/// payload. The loop below refuses anything shorter, so this is what bounds how many
/// entries a block of a given size can possibly hold.
const MIN_VALUE_ENTRY_BYTES: usize = 6;

fn parse_value_label_offsets(
    bytes: &[u8],
    header: &HeaderInfo,
    pad: usize,
    label_count: usize,
    capacity: usize,
) -> Result<(Vec<usize>, usize)> {
    // `label_count` is `label_count_used`, a u32/u64 lifted straight out of the catalog,
    // and it sizes this table. A fuzz case declaring 1,313,169,229 labels reached
    // `calloc(10505353832)` on a catalog of a few KB. The block cannot hold more entries
    // than its own length allows, so the claim is refutable here rather than trusted.
    //
    // `capacity` needs no equivalent check: the loop errors out as soon as fewer than
    // MIN_VALUE_ENTRY_BYTES remain, so it cannot run more than that many iterations
    // however large the declared capacity is.
    let describable = bytes.len() / MIN_VALUE_ENTRY_BYTES;
    if label_count > describable {
        return Err(Error::page_corruption(format!(
            "catalog declares {label_count} value labels, more than the {} byte block could hold",
            bytes.len()
        )));
    }

    // Fallible: `vec![0usize; n]` aborts the process through `handle_alloc_error`, which
    // the Python and R bindings cannot recover from.
    let mut offsets: Vec<usize> = Vec::new();
    offsets
        .try_reserve(label_count)
        .map_err(|_| Error::page_corruption("cannot allocate a catalog value-label table"))?;
    offsets.resize(label_count, 0usize);
    let mut cursor = 0usize;

    for i in 0..capacity {
        let remaining = bytes.len().saturating_sub(cursor);
        if remaining < 6 {
            return Err(Error::page_corruption("catalog value entry truncated"));
        }
        let entry = &bytes[cursor..];
        let entry_len = usize::from(read_u16_slice(header, &entry[2..4]));
        if 6 + entry_len > entry.len() {
            return Err(Error::page_corruption("catalog value entry exceeds block"));
        }
        if i < label_count {
            let label_pos_offset = 10 + pad;
            if label_pos_offset + 4 > entry.len() {
                return Err(Error::page_corruption(
                    "catalog value entry missing label index",
                ));
            }
            let label_pos = read_u32_slice(header, &entry[label_pos_offset..label_pos_offset + 4]);
            let label_pos = usize::try_from(label_pos)
                .map_err(|_| Error::page_corruption("catalog label index out of range"))?;
            if label_pos >= offsets.len() {
                return Err(Error::page_corruption("catalog label index out of range"));
            }
            offsets[label_pos] = cursor;
        }
        let consumed = 6 + entry_len;
        if consumed > remaining {
            cursor = bytes.len();
            break;
        }
        cursor += consumed;
        if cursor >= bytes.len() {
            break;
        }
    }

    Ok((offsets, cursor))
}

fn parse_value_label_entries(
    bytes: &[u8],
    mut label_cursor: usize,
    offsets: &[usize],
    header: &HeaderInfo,
    encoding: &'static Encoding,
    value_type: ValueType,
) -> Result<Vec<ValueLabel>> {
    let mut labels = Vec::with_capacity(offsets.len());

    for &entry_offset in offsets {
        if entry_offset + 6 > bytes.len() {
            return Err(Error::page_corruption("catalog value entry offset invalid"));
        }
        let entry = &bytes[entry_offset..];
        let entry_len = usize::from(read_u16_slice(header, &entry[2..4])) + 6;
        if entry_len > entry.len() {
            return Err(Error::page_corruption("catalog value entry truncated"));
        }

        let key = parse_value_label_key(entry, entry_len, header, encoding, value_type)?;

        if label_cursor + 10 > bytes.len() {
            return Err(Error::page_corruption("catalog label entry truncated"));
        }
        let lbp2 = &bytes[label_cursor..];
        let mut label_len = usize::from(read_u16_slice(header, &lbp2[8..10]));
        let available = lbp2.len().saturating_sub(10);
        label_len = min(label_len, available);
        let label = decode_text(&lbp2[10..10 + label_len], encoding);
        labels.push(ValueLabel { key, label });
        let skip = 8 + 2 + label_len + 1;
        label_cursor = label_cursor.saturating_add(skip);
        if label_cursor >= bytes.len() {
            label_cursor = bytes.len();
        }
    }

    Ok(labels)
}

fn parse_value_label_key(
    entry: &[u8],
    entry_len: usize,
    _header: &HeaderInfo,
    encoding: &'static Encoding,
    value_type: ValueType,
) -> Result<ValueKey> {
    match value_type {
        ValueType::String => {
            if entry_len < 16 {
                return Err(Error::page_corruption(
                    "catalog string value entry too short",
                ));
            }
            let value_bytes = &entry[entry_len - 16..entry_len];
            Ok(ValueKey::String(decode_text(value_bytes, encoding)))
        }
        ValueType::Numeric => {
            if entry_len < 30 {
                return Err(Error::page_corruption(
                    "catalog numeric value entry too short",
                ));
            }
            let raw = read_u64_be(&entry[22..30]);
            Ok(decode_numeric_key(raw))
        }
    }
}

// ─── numeric key decoding ─────────────────────────────────────────────────────

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
            try_int_from_f64(value).map_or(ValueKey::Numeric(value), ValueKey::Integer)
        } else {
            ValueKey::Numeric(value)
        }
    }
}

fn try_int_from_f64(value: f64) -> Option<i32> {
    if value.fract() != 0.0 {
        return None;
    }
    #[allow(clippy::cast_possible_truncation)]
    let as_i64 = value as i64;
    i32::try_from(as_i64).ok()
}

const fn decode_missing_tag(tag: u8) -> char {
    match tag {
        0 => '_',
        2..=27 => (b'A' + (tag - 2)) as char,
        _ => '.',
    }
}

// ─── text decoding ────────────────────────────────────────────────────────────

fn trim_trailing(bytes: &[u8]) -> &[u8] {
    let end = bytes
        .iter()
        .rposition(|&b| b != 0 && b != b' ')
        .map_or(0, |i| i + 1);
    &bytes[..end]
}

fn decode_text(bytes: &[u8], encoding: &'static Encoding) -> String {
    let trimmed = trim_trailing(bytes);
    if trimmed.is_empty() {
        return String::new();
    }
    // Validate UTF-8 in place; only copy/decode when it isn't valid UTF-8.
    std::str::from_utf8(trimmed).map_or_else(
        |_| {
            let (decoded, _, had_errors) = encoding.decode(trimmed);
            if had_errors {
                String::from_utf8_lossy(trimmed).into_owned()
            } else {
                decoded.into_owned()
            }
        },
        |text| text.trim_end_matches('\u{0000}').to_string(),
    )
}
// ─── slice read helpers ───────────────────────────────────────────────────────

use crate::metadata::Endianness;

fn read_u16_slice(header: &HeaderInfo, bytes: &[u8]) -> u16 {
    debug_assert!(bytes.len() >= 2);
    match header.endianness {
        Endianness::Little => u16::from_le_bytes([bytes[0], bytes[1]]),
        Endianness::Big => u16::from_be_bytes([bytes[0], bytes[1]]),
    }
}

fn read_u32_slice(header: &HeaderInfo, bytes: &[u8]) -> u32 {
    debug_assert!(bytes.len() >= 4);
    match header.endianness {
        Endianness::Little => u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
        Endianness::Big => u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
    }
}

fn read_u64_slice(header: &HeaderInfo, bytes: &[u8]) -> u64 {
    debug_assert!(bytes.len() >= 8);
    match header.endianness {
        Endianness::Little => u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]),
        Endianness::Big => u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]),
    }
}

fn read_u64_be(bytes: &[u8]) -> u64 {
    debug_assert!(bytes.len() >= 8);
    u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ])
}

// ─── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_format_name_uppercases_and_trims() {
        assert_eq!(normalize_format_name("  gender  "), "GENDER");
        assert_eq!(normalize_format_name("$a"), "$A");
        assert_eq!(normalize_format_name("date9."), "DATE9");
        assert_eq!(normalize_format_name("DATE9."), "DATE9");
    }

    #[test]
    fn decode_missing_tag_covers_known_values() {
        assert_eq!(decode_missing_tag(0), '_');
        assert_eq!(decode_missing_tag(2), 'A');
        assert_eq!(decode_missing_tag(27), 'Z');
        assert_eq!(decode_missing_tag(1), '.');
        assert_eq!(decode_missing_tag(28), '.');
    }

    #[test]
    fn try_int_from_f64_round_trips() {
        assert_eq!(try_int_from_f64(1.0), Some(1));
        assert_eq!(try_int_from_f64(-42.0), Some(-42));
        assert_eq!(try_int_from_f64(1.5), None);
        assert_eq!(try_int_from_f64(f64::from(i32::MAX) + 1.0), None);
    }

    #[test]
    fn parse_catalog_file_reads_test_formats_fixture() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/raw_data/readstat/test_formats_win.sas7bcat");
        if !path.exists() {
            return;
        }

        let layout = parse_catalog_file(&path).expect("parse catalog");
        assert!(
            !layout.label_sets.is_empty(),
            "expected at least one label set"
        );

        let names: Vec<&str> = layout.label_sets.iter().map(|s| s.name.as_str()).collect();
        eprintln!("label set names: {names:?}");

        let has_a = layout
            .label_sets
            .iter()
            .any(|s| normalize_format_name(&s.name) == "$A");
        let has_b = layout
            .label_sets
            .iter()
            .any(|s| normalize_format_name(&s.name) == "$B");
        assert!(has_a, "expected label set $A, found: {names:?}");
        assert!(has_b, "expected label set $B, found: {names:?}");

        let label_a = layout
            .label_sets
            .iter()
            .find(|s| normalize_format_name(&s.name) == "$A")
            .expect("label set $A");
        assert_eq!(label_a.value_type, ValueType::String);
        assert!(!label_a.labels.is_empty(), "expected labels in $A");
        assert!(
            label_a.labels.iter().any(|l| l.label == "Male"),
            "expected 'Male' label in $A, found: {:?}",
            label_a.labels
        );
    }
}
