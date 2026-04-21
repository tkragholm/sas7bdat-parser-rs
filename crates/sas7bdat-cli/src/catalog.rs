use anyhow::{Context, Result, bail};
use encoding_rs::Encoding;
use serde::Serialize;
use std::cmp::min;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

const SAS7BCAT_MAGIC_NUMBER: [u8; 32] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC2, 0xEA, 0x81, 0x63,
    0xB3, 0x14, 0x11, 0xCF, 0xBD, 0x92, 0x08, 0x00, 0x09, 0xC7, 0x31, 0x8C, 0x18, 0x1F, 0x10, 0x11,
];

#[derive(Debug, Clone, Serialize)]
pub struct Catalog {
    label_sets: HashMap<String, LabelSet>,
}

impl Catalog {
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be opened or parsed.
    pub fn load(path: &Path) -> Result<Self> {
        let mut file = File::open(path)
            .with_context(|| format!("failed to open catalog {}", path.display()))?;
        parse_catalog(&mut file)
    }

    #[must_use]
    pub fn label_set_for_format(&self, format_name: &str) -> Option<&LabelSet> {
        let normalized = normalize_label_name(format_name);
        self.label_sets.get(&normalized).or_else(|| {
            if normalized.starts_with('$') {
                None
            } else {
                self.label_sets.get(&format!("${normalized}"))
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LabelSet {
    pub name: String,
    pub value_type: ValueType,
    pub labels: Vec<ValueLabel>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ValueLabel {
    pub key: ValueKey,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum ValueKey {
    Numeric(f64),
    Integer(i32),
    Tagged(char),
    String(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ValueType {
    Numeric,
    String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Endianness {
    Little,
    Big,
}

#[derive(Debug, Clone)]
struct SasHeader {
    endianness: Endianness,
    uses_u64: bool,
    page_size: u32,
    page_count: u64,
    pad_alignment: u32,
    data_offset: u64,
    encoding: &'static Encoding,
}

/// # Errors
///
/// Returns an error if the catalog metadata cannot be read or decoded.
pub fn parse_catalog<R: Read + Seek>(reader: &mut R) -> Result<Catalog> {
    let header = parse_header(reader)?;
    let index = CatalogueIndex::build(reader, &header)?;
    let label_sets = index.parse_label_sets(reader, &header, header.encoding)?;
    let label_sets = label_sets
        .into_iter()
        .map(|set| (normalize_label_name(&set.name), set))
        .collect();
    Ok(Catalog { label_sets })
}

struct CatalogueIndex {
    block_pointers: Vec<u64>,
}

impl CatalogueIndex {
    fn build<R: Read + Seek>(reader: &mut R, header: &SasHeader) -> Result<Self> {
        let mut pointers = Vec::new();
        let cfg = IndexLayout::new(header);
        let mut page = vec![0u8; header.page_size as usize];

        if header.page_count <= 1 {
            return Ok(Self {
                block_pointers: pointers,
            });
        }

        read_page(reader, header, 1, &mut page)?;
        augment_index(&page[cfg.index_start_offset..], header, &cfg, &mut pointers);

        for index in 3..header.page_count {
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

fn parse_header<R: Read + Seek>(reader: &mut R) -> Result<SasHeader> {
    let mut start_buf = [0u8; 164];
    reader.read_exact(&mut start_buf)?;
    let start = HeaderStart::from_bytes(start_buf);
    if start.magic != SAS7BCAT_MAGIC_NUMBER {
        bail!("unrecognized SAS catalog magic number");
    }

    let endianness = match start.endian {
        0x00 => Endianness::Big,
        0x01 => Endianness::Little,
        _ => bail!("unsupported endian flag in catalog header"),
    };
    let uses_u64 = start.a2 == 0x33;
    let pad_alignment = if start.a1 == 0x33 { 4 } else { 0 };
    if pad_alignment > 0 {
        reader.seek(SeekFrom::Current(i64::from(pad_alignment)))?;
    }

    read_f64(reader, endianness)?;
    read_f64(reader, endianness)?;
    read_f64(reader, endianness)?;
    read_f64(reader, endianness)?;

    let header_size = read_u32(reader, endianness)?;
    let page_size = read_u32(reader, endianness)?;
    let page_count = if uses_u64 {
        read_u64(reader, endianness)?
    } else {
        u64::from(read_u32(reader, endianness)?)
    };

    reader.seek(SeekFrom::Current(8))?;
    let mut end_buf = [0u8; 120];
    reader.read_exact(&mut end_buf)?;

    reader.seek(SeekFrom::Start(u64::from(header_size)))?;

    Ok(SasHeader {
        endianness,
        uses_u64,
        page_size,
        page_count,
        pad_alignment,
        data_offset: u64::from(header_size),
        encoding: resolve_encoding(lookup_encoding(start.encoding)),
    })
}

struct HeaderStart {
    magic: [u8; 32],
    a2: u8,
    a1: u8,
    endian: u8,
    encoding: u8,
}

impl HeaderStart {
    fn from_bytes(bytes: [u8; 164]) -> Self {
        let mut idx = 0usize;
        let mut take = |len: usize| {
            let start = idx;
            idx += len;
            &bytes[start..start + len]
        };
        let mut magic = [0u8; 32];
        magic.copy_from_slice(take(32));
        let a2 = take(1)[0];
        take(2);
        let a1 = take(1)[0];
        take(1);
        let endian = take(1)[0];
        take(1);
        take(1);
        take(30);
        let encoding = take(1)[0];
        take(13);
        take(8);
        take(32);
        take(8);
        Self {
            magic,
            a2,
            a1,
            endian,
            encoding,
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
            let page = read_u64_from(&entry[8..16], header.endianness);
            let pos = read_u16_from(&entry[16..18], header.endianness);
            (page, pos)
        } else {
            let page = u64::from(read_u32_from(&entry[4..8], header.endianness));
            let pos = read_u16_from(&entry[8..10], header.endianness);
            (page, pos)
        };

        if page > 0 && pos > 0 && page <= header.page_count + 1 {
            pointers.push((page << 32) | u64::from(pos));
        }

        cursor += layout.entry_stride;
    }
}

struct ChainSegment {
    page: u64,
    pos: u64,
    len: u16,
}

fn read_block<R: Read + Seek>(reader: &mut R, header: &SasHeader, pointer: u64) -> Result<Vec<u8>> {
    let (page, pos) = decode_pointer(pointer);
    if page == 0 || pos == 0 {
        bail!("catalog block pointer references invalid page");
    }

    let header_len = if header.uses_u64 { 32 } else { 16 };
    let segments = collect_chain_segments(reader, header, page, pos, header_len)?;
    let total_len: usize = segments.iter().map(|segment| segment.len as usize).sum();
    if total_len == 0 {
        return Ok(Vec::new());
    }

    let mut buffer = vec![0u8; total_len];
    let mut offset = 0usize;
    for segment in segments {
        if segment.len == 0 {
            break;
        }
        let seg_len = segment.len as usize;
        if offset + seg_len > buffer.len() {
            bail!("catalog chain exceeds allocated buffer");
        }
        read_segment_data(
            reader,
            header,
            segment.page,
            segment.pos,
            header_len,
            &mut buffer[offset..offset + seg_len],
        )?;
        offset += seg_len;
    }
    buffer.truncate(offset);
    Ok(buffer)
}

fn collect_chain_segments<R: Read + Seek>(
    reader: &mut R,
    header: &SasHeader,
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

fn parse_block(
    buffer: &[u8],
    header: &SasHeader,
    encoding: &'static Encoding,
) -> Result<Option<LabelSet>> {
    const BASE_PAYLOAD_OFFSET: usize = 106;
    if buffer.len() < BASE_PAYLOAD_OFFSET {
        return Ok(None);
    }

    let flags = read_u16_from(&buffer[2..4], header.endianness);
    let mut pad = if flags & 0x08 != 0 { 4 } else { 0 };
    let mut payload_offset = BASE_PAYLOAD_OFFSET;
    let is_string = buffer.get(8).is_some_and(|b| *b == b'$');

    let label_count_capacity: u64;
    let label_count_used: u64;
    if header.uses_u64 {
        label_count_capacity = read_u64_from(&buffer[42 + pad..50 + pad], header.endianness);
        label_count_used = read_u64_from(&buffer[50 + pad..58 + pad], header.endianness);
        payload_offset += 32;
    } else {
        label_count_capacity = u64::from(read_u32_from(
            &buffer[38 + pad..42 + pad],
            header.endianness,
        ));
        label_count_used = u64::from(read_u32_from(
            &buffer[42 + pad..46 + pad],
            header.endianness,
        ));
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
            bail!("catalog long-name block truncated");
        }
        name = decode_text(&buffer[start..end], encoding)?;
        pad += 32;
    }

    if label_count_used == 0 {
        return Ok(None);
    }

    let value_area = payload_offset + pad;
    if value_area > buffer.len() {
        bail!("catalog value block missing payload");
    }
    let value_bytes = &buffer[value_area..];
    let value_type = if is_string {
        ValueType::String
    } else {
        ValueType::Numeric
    };
    let mut label_set = LabelSet {
        name: name.trim_end().to_string(),
        value_type,
        labels: Vec::new(),
    };
    label_set.labels = parse_value_labels(
        value_bytes,
        header,
        encoding,
        label_count_used,
        label_count_capacity,
        value_type,
    )?;
    Ok(Some(label_set))
}

fn parse_value_labels(
    bytes: &[u8],
    header: &SasHeader,
    encoding: &'static Encoding,
    label_count_used: u64,
    label_count_capacity: u64,
    value_type: ValueType,
) -> Result<Vec<ValueLabel>> {
    let label_count =
        usize::try_from(label_count_used).context("catalog label count exceeds platform width")?;
    let capacity = usize::try_from(label_count_capacity)
        .context("catalog label capacity exceeds platform width")?;
    let pad = usize::try_from(header.pad_alignment)
        .context("catalog label padding exceeds platform width")?;
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

fn parse_value_label_offsets(
    bytes: &[u8],
    header: &SasHeader,
    pad: usize,
    label_count: usize,
    capacity: usize,
) -> Result<(Vec<usize>, usize)> {
    let mut offsets = vec![0usize; label_count];
    let mut cursor = 0usize;

    for i in 0..capacity {
        let remaining = bytes.len().saturating_sub(cursor);
        if remaining < 6 {
            bail!("catalog value entry truncated");
        }
        let entry = &bytes[cursor..];
        let entry_len = usize::from(read_u16_from(&entry[2..4], header.endianness));
        if 6 + entry_len > entry.len() {
            bail!("catalog value entry exceeds block");
        }
        if i < label_count {
            let label_pos_offset = 10 + pad;
            if label_pos_offset + 4 > entry.len() {
                bail!("catalog value entry missing label index");
            }
            let label_pos = usize::try_from(read_u32_from(
                &entry[label_pos_offset..label_pos_offset + 4],
                header.endianness,
            ))
            .context("catalog label index out of range")?;
            if label_pos >= offsets.len() {
                bail!("catalog label index out of range");
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
    header: &SasHeader,
    encoding: &'static Encoding,
    value_type: ValueType,
) -> Result<Vec<ValueLabel>> {
    let mut labels = Vec::with_capacity(offsets.len());

    for &entry_offset in offsets {
        if entry_offset + 6 > bytes.len() {
            bail!("catalog value entry offset invalid");
        }
        let entry = &bytes[entry_offset..];
        let entry_len = usize::from(read_u16_from(&entry[2..4], header.endianness)) + 6;
        if entry_len > entry.len() {
            bail!("catalog value entry truncated");
        }

        let key = parse_value_label_key(entry, entry_len, encoding, value_type)?;

        if label_cursor + 10 > bytes.len() {
            bail!("catalog label entry truncated");
        }
        let lbp2 = &bytes[label_cursor..];
        let mut label_len = usize::from(read_u16_from(&lbp2[8..10], header.endianness));
        let available = lbp2.len().saturating_sub(10);
        label_len = min(label_len, available);
        let label = decode_text(&lbp2[10..10 + label_len], encoding)?;
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
    encoding: &'static Encoding,
    value_type: ValueType,
) -> Result<ValueKey> {
    match value_type {
        ValueType::String => {
            if entry_len < 16 {
                bail!("catalog string value entry too short");
            }
            let value_bytes = &entry[entry_len - 16..entry_len];
            Ok(ValueKey::String(decode_text(value_bytes, encoding)?))
        }
        ValueType::Numeric => {
            if entry_len < 30 {
                bail!("catalog numeric value entry too short");
            }
            let raw = read_u64_be_from(&entry[22..30]);
            Ok(decode_numeric_key(raw))
        }
    }
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
    let next_page = u64::from(read_u32_from(&chunk[0..4], header.endianness));
    if header.uses_u64 {
        let next_pos = u64::from(read_u16_from(&chunk[8..10], header.endianness));
        let seg_len = read_u16_from(&chunk[10..12], header.endianness);
        (next_page, next_pos, seg_len)
    } else {
        let next_pos = u64::from(read_u16_from(&chunk[4..6], header.endianness));
        let seg_len = read_u16_from(&chunk[6..8], header.endianness);
        (next_page, next_pos, seg_len)
    }
}

const fn decode_pointer(pointer: u64) -> (u64, u64) {
    let page = pointer >> 32;
    let pos = pointer & 0xFFFF;
    (page, pos)
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
            num_traits::ToPrimitive::to_i32(&value)
                .map_or(ValueKey::Numeric(value), ValueKey::Integer)
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
    std::str::from_utf8(trimmed).map_or_else(
        |_| {
            let (decoded, _, had_errors) = encoding.decode(trimmed);
            if had_errors {
                Ok(String::from_utf8_lossy(trimmed).into_owned())
            } else {
                Ok(decoded.into_owned())
            }
        },
        |text| Ok(text.trim_end_matches('\u{0000}').to_string()),
    )
}

fn trim_trailing(bytes: &[u8]) -> &[u8] {
    match bytes.iter().rposition(|b| *b != 0 && *b != b' ') {
        Some(idx) => &bytes[..=idx],
        None => &[],
    }
}

fn normalize_label_name(name: &str) -> String {
    name.trim()
        .trim_end_matches('.')
        .trim()
        .to_ascii_uppercase()
}

fn resolve_encoding(label: Option<&str>) -> &'static Encoding {
    if let Some(label) = label {
        if let Some(enc) = Encoding::for_label(label.as_bytes()) {
            return enc;
        }
    }
    Encoding::for_label(b"windows-1252").expect("windows-1252 encoding available")
}

const fn lookup_encoding(code: u8) -> Option<&'static str> {
    match code {
        0 => Some("WINDOWS-1252"),
        20 => Some("UTF-8"),
        28 => Some("US-ASCII"),
        29 => Some("ISO-8859-1"),
        30 => Some("ISO-8859-2"),
        31 => Some("ISO-8859-3"),
        32 => Some("ISO-8859-4"),
        33 => Some("ISO-8859-5"),
        34 => Some("ISO-8859-6"),
        35 => Some("ISO-8859-7"),
        36 => Some("ISO-8859-8"),
        37 => Some("ISO-8859-9"),
        39 => Some("ISO-8859-11"),
        40 => Some("ISO-8859-15"),
        41 => Some("CP437"),
        42 => Some("CP850"),
        43 => Some("CP852"),
        44 => Some("CP857"),
        45 => Some("CP858"),
        46 => Some("CP862"),
        47 => Some("CP864"),
        48 => Some("CP865"),
        49 => Some("CP866"),
        50 => Some("CP869"),
        51 => Some("CP874"),
        _ => None,
    }
}

fn read_u16_from(bytes: &[u8], endian: Endianness) -> u16 {
    let mut buf = [0u8; 2];
    buf.copy_from_slice(bytes);
    match endian {
        Endianness::Little => u16::from_le_bytes(buf),
        Endianness::Big => u16::from_be_bytes(buf),
    }
}

fn read_u32_from(bytes: &[u8], endian: Endianness) -> u32 {
    let mut buf = [0u8; 4];
    buf.copy_from_slice(bytes);
    match endian {
        Endianness::Little => u32::from_le_bytes(buf),
        Endianness::Big => u32::from_be_bytes(buf),
    }
}

fn read_u64_from(bytes: &[u8], endian: Endianness) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(bytes);
    match endian {
        Endianness::Little => u64::from_le_bytes(buf),
        Endianness::Big => u64::from_be_bytes(buf),
    }
}

fn read_u64_be_from(bytes: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(bytes);
    u64::from_be_bytes(buf)
}

fn read_f64<R: Read>(reader: &mut R, endian: Endianness) -> Result<f64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    let bits = match endian {
        Endianness::Little => u64::from_le_bytes(buf),
        Endianness::Big => u64::from_be_bytes(buf),
    };
    Ok(f64::from_bits(bits))
}

fn read_u32<R: Read>(reader: &mut R, endian: Endianness) -> Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(match endian {
        Endianness::Little => u32::from_le_bytes(buf),
        Endianness::Big => u32::from_be_bytes(buf),
    })
}

fn read_u64<R: Read>(reader: &mut R, endian: Endianness) -> Result<u64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(match endian {
        Endianness::Little => u64::from_le_bytes(buf),
        Endianness::Big => u64::from_be_bytes(buf),
    })
}
