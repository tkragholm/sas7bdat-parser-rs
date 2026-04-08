use crate::{
    error::{Error, HeaderError, Result, UnsupportedError},
    internal::HeaderInfo,
    metadata::{CompressionKind, DatasetMetadata, Endianness},
};
use std::{
    io::{Read, Seek, SeekFrom},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const SAS_ALIGNMENT_OFFSET_4: u8 = 0x33;
const SAS_ENDIAN_BIG: u8 = 0x00;
const SAS_ENDIAN_LITTLE: u8 = 0x01;

const SAS_PAGE_HEADER_SIZE_32BIT: u32 = 24;
const SAS_PAGE_HEADER_SIZE_64BIT: u32 = 40;
const SAS_SUBHEADER_POINTER_SIZE_32BIT: u32 = 12;
const SAS_SUBHEADER_POINTER_SIZE_64BIT: u32 = 24;

const SAS_HEADER_START_SIZE: usize = 164;
const SAS_HEADER_END_SIZE: usize = 120;

const SAS_HEADER_MIN_SIZE: u32 = 1024;
const SAS_PAGE_MIN_SIZE: u32 = 1024;
const SAS_MAX_SIZE: u32 = 1 << 24;
const SAS_PAGE_COUNT_MAX: u64 = 1 << 24;

const SAS7BDAT_MAGIC_NUMBER: [u8; 32] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC2, 0xEA, 0x81, 0x60,
    0xB3, 0x14, 0x11, 0xCF, 0xBD, 0x92, 0x08, 0x00, 0x09, 0xC7, 0x31, 0x8C, 0x18, 0x1F, 0x10, 0x11,
];

const SAS7BCAT_MAGIC_NUMBER: [u8; 32] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC2, 0xEA, 0x81, 0x63,
    0xB3, 0x14, 0x11, 0xCF, 0xBD, 0x92, 0x08, 0x00, 0x09, 0xC7, 0x31, 0x8C, 0x18, 0x1F, 0x10, 0x11,
];

pub fn probe_header<R: Read + Seek>(reader: &mut R) -> Result<(HeaderInfo, DatasetMetadata)> {
    let mut start_buf = [0u8; SAS_HEADER_START_SIZE];
    reader.read_exact(&mut start_buf).map_err(|e| io_header_error(&e))?;

    let header_start = HeaderStart::from_bytes(start_buf);
    let is_catalog = header_start.magic == SAS7BCAT_MAGIC_NUMBER;
    if header_start.magic != SAS7BDAT_MAGIC_NUMBER && !is_catalog {
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&header_start.magic[12..16]);
        return Err(Error::Header(HeaderError::InvalidMagic(magic)));
    }

    let endianness = match header_start.endian {
        SAS_ENDIAN_BIG => Endianness::Big,
        SAS_ENDIAN_LITTLE => Endianness::Little,
        _ => return Err(Error::Header(HeaderError::UnexpectedEndianness)),
    };

    let uses_u64_pointers = header_start.a2 == SAS_ALIGNMENT_OFFSET_4;
    let pad_alignment = if header_start.a1 == SAS_ALIGNMENT_OFFSET_4 {
        4_u32
    } else {
        0
    };

    if pad_alignment > 0 {
        reader
            .seek(SeekFrom::Current(i64::from(pad_alignment)))
            .map_err(|e| io_header_error(&e))?;
    }

    let created_at = read_timestamp(reader, endianness)?;
    let modified_at = read_timestamp(reader, endianness)?;
    let created_diff = read_f64(reader, endianness)?;
    let modified_diff = read_f64(reader, endianness)?;
    let created_at = convert_sas_time(created_at, created_diff);
    let modified_at = convert_sas_time(modified_at, modified_diff);

    let (header_size, page_size) = read_sizes(reader, endianness)?;
    let page_count = read_page_count(reader, uses_u64_pointers, endianness)?;

    reader.seek(SeekFrom::Current(8)).map_err(|e| io_header_error(&e))?;

    let mut end_buf = [0u8; SAS_HEADER_END_SIZE];
    reader.read_exact(&mut end_buf).map_err(|e| io_header_error(&e))?;
    let header_end = HeaderEnd::from_bytes(end_buf);
    let release = header_end.release()?;

    let encoding = lookup_encoding(header_start.encoding)
        .map(str::to_owned)
        .ok_or_else(|| {
            Error::Unsupported(UnsupportedError {
                feature: format!("character set code {}", header_start.encoding),
            })
        })?;

    let table_name = decode_padded_string(&header_start.table_name);

    let info = HeaderInfo {
        endianness,
        uses_u64_pointers,
        page_size: crate::types::PageSize(page_size),
        page_count,
        page_header_size: if uses_u64_pointers {
            SAS_PAGE_HEADER_SIZE_64BIT
        } else {
            SAS_PAGE_HEADER_SIZE_32BIT
        },
        subheader_pointer_size: if uses_u64_pointers {
            SAS_SUBHEADER_POINTER_SIZE_64BIT
        } else {
            SAS_SUBHEADER_POINTER_SIZE_32BIT
        },
        subheader_signature_size: if uses_u64_pointers { 8 } else { 4 },
        data_offset: u64::from(header_size),
        header_size,
        release,
        is_catalog,
    };

    let metadata = DatasetMetadata {
        table_name,
        file_label: None,
        encoding: Some(encoding),
        endianness,
        page_size,
        page_count,
        row_count: 0,
        row_len: crate::types::RowLength(0).into(),
        compression: CompressionKind::None,
        created_at,
        modified_at,
    };

    Ok((info, metadata))
}

fn read_sizes<R: Read + Seek>(reader: &mut R, endianness: Endianness) -> Result<(u32, u32)> {
    let header_size_raw = read_u32(reader, endianness)?;
    let header_size = normalize_size(header_size_raw, SAS_HEADER_MIN_SIZE, SAS_MAX_SIZE)
        .ok_or_else(|| header_error("header size outside expected range"))?;

    let page_size_raw = read_u32(reader, endianness)?;
    let page_size = normalize_size(page_size_raw, SAS_PAGE_MIN_SIZE, SAS_MAX_SIZE)
        .ok_or_else(|| header_error("page size outside expected range"))?;

    Ok((header_size, page_size))
}

fn normalize_size(raw: u32, min: u32, max: u32) -> Option<u32> {
    if (min..=max).contains(&raw) {
        return Some(raw);
    }
    let swapped = raw.swap_bytes();
    (min..=max).contains(&swapped).then_some(swapped)
}

fn read_page_count<R: Read + Seek>(
    reader: &mut R,
    uses_u64_pointers: bool,
    endianness: Endianness,
) -> Result<u64> {
    let page_count_raw = if uses_u64_pointers {
        read_u64(reader, endianness)?
    } else {
        u64::from(read_u32(reader, endianness)?)
    };

    if page_count_raw <= SAS_PAGE_COUNT_MAX {
        return Ok(page_count_raw);
    }

    let lower32 = page_count_raw & 0xFFFF_FFFF;
    let swapped = if uses_u64_pointers {
        page_count_raw.swap_bytes()
    } else {
        u32::try_from(lower32)
            .ok()
            .map_or(0, |value| u64::from(value.swap_bytes()))
    };

    if (1..=SAS_PAGE_COUNT_MAX).contains(&lower32) {
        return Ok(lower32);
    }
    if (1..=SAS_PAGE_COUNT_MAX).contains(&swapped) {
        return Ok(swapped);
    }

    Err(header_error("page count outside expected range"))
}

struct HeaderStart {
    magic: [u8; 32],
    a2: u8,
    a1: u8,
    endian: u8,
    encoding: u8,
    table_name: [u8; 32],
}

impl HeaderStart {
    fn from_bytes(bytes: [u8; SAS_HEADER_START_SIZE]) -> Self {
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
        let _file_format = take(1)[0];
        take(30);
        let encoding = take(1)[0];
        take(13);
        take(8);

        let mut table_name = [0u8; 32];
        table_name.copy_from_slice(take(32));

        take(32);
        take(8);

        Self {
            magic,
            a2,
            a1,
            endian,
            encoding,
            table_name,
        }
    }
}

struct HeaderEnd {
    release_raw: [u8; 8],
}

impl HeaderEnd {
    fn from_bytes(bytes: [u8; SAS_HEADER_END_SIZE]) -> Self {
        let mut release_raw = [0u8; 8];
        release_raw.copy_from_slice(&bytes[0..8]);
        Self { release_raw }
    }

    fn release(&self) -> Result<String> {
        decode_padded_string(&self.release_raw)
            .ok_or_else(|| header_error("missing release string"))
    }
}

fn read_timestamp<R: Read>(reader: &mut R, endianness: Endianness) -> Result<f64> {
    read_f64(reader, endianness)
}

fn read_u32<R: Read>(reader: &mut R, endianness: Endianness) -> Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).map_err(|e| io_header_error(&e))?;
    Ok(match endianness {
        Endianness::Little => u32::from_le_bytes(buf),
        Endianness::Big => u32::from_be_bytes(buf),
    })
}

fn read_u64<R: Read>(reader: &mut R, endianness: Endianness) -> Result<u64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf).map_err(|e| io_header_error(&e))?;
    Ok(match endianness {
        Endianness::Little => u64::from_le_bytes(buf),
        Endianness::Big => u64::from_be_bytes(buf),
    })
}

fn read_f64<R: Read>(reader: &mut R, endianness: Endianness) -> Result<f64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf).map_err(|e| io_header_error(&e))?;
    let bits = match endianness {
        Endianness::Little => u64::from_le_bytes(buf),
        Endianness::Big => u64::from_be_bytes(buf),
    };
    Ok(f64::from_bits(bits))
}

fn decode_padded_string(bytes: &[u8]) -> Option<String> {
    let trimmed = bytes
        .iter()
        .rposition(|b| *b != 0 && *b != b' ')
        .map(|idx| &bytes[..=idx])?;
    let lossless = String::from_utf8_lossy(trimmed);
    let candidate = lossless.trim();
    if candidate.is_empty() {
        None
    } else {
        Some(candidate.to_owned())
    }
}

fn convert_sas_time(time: f64, diff: f64) -> Option<SystemTime> {
    const SAS_EPOCH_OFFSET_F64: f64 = 315_532_800.0;
    let unix_seconds = SAS_EPOCH_OFFSET_F64 + (time - diff);
    system_time_from_unix_seconds(unix_seconds)
}

fn system_time_from_unix_seconds(seconds: f64) -> Option<SystemTime> {
    // i64 can exactly represent integers up to 2^63-1. 
    // f64 can represent all integers up to 2^53.
    // For timestamps, we are well within these bounds.
    const I64_MIN_F64: f64 = -9_223_372_036_854_775_808.0;
    const I64_MAX_F64: f64 = 9_223_372_036_854_775_807.0;

    if !seconds.is_finite() {
        return None;
    }

    if !(I64_MIN_F64..=I64_MAX_F64).contains(&seconds) {
        return None;
    }

    #[allow(clippy::cast_possible_truncation)]
    let whole = seconds.trunc() as i64;
    let fract = seconds.fract().abs();
    let nanos_f64 = (fract * 1_000_000_000.0).round();
    let nanos = if nanos_f64 >= 1_000_000_000.0 {
        999_999_999_u32
    } else {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let n = nanos_f64 as u32;
        n
    };
    let duration = Duration::new(whole.unsigned_abs(), nanos);
    if whole >= 0 {
        UNIX_EPOCH.checked_add(duration)
    } else {
        UNIX_EPOCH.checked_sub(duration)
    }
}

fn io_header_error(err: &std::io::Error) -> Error {
    Error::metadata_corruption(err.to_string())
}

fn header_error(message: impl Into<String>) -> Error {
    Error::header_corruption(message)
}

const fn lookup_encoding(code: u8) -> Option<&'static str> {
    static ENCODING_MAP: &[(u8, &str)] = &[
        (0, "WINDOWS-1252"),
        (20, "UTF-8"),
        (28, "US-ASCII"),
        (29, "ISO-8859-1"),
        (30, "ISO-8859-2"),
        (31, "ISO-8859-3"),
        (32, "ISO-8859-4"),
        (33, "ISO-8859-5"),
        (34, "ISO-8859-6"),
        (35, "ISO-8859-7"),
        (36, "ISO-8859-8"),
        (37, "ISO-8859-9"),
        (39, "ISO-8859-11"),
        (40, "ISO-8859-15"),
        (41, "CP437"),
        (42, "CP850"),
        (43, "CP852"),
        (44, "CP857"),
        (45, "CP858"),
        (46, "CP862"),
        (47, "CP864"),
        (48, "CP865"),
        (49, "CP866"),
        (50, "CP869"),
        (51, "CP874"),
        (52, "CP921"),
        (53, "CP922"),
        (54, "CP1129"),
        (55, "CP720"),
        (56, "CP737"),
        (57, "CP775"),
        (58, "CP860"),
        (59, "CP863"),
        (60, "WINDOWS-1250"),
        (61, "WINDOWS-1251"),
        (62, "WINDOWS-1252"),
        (63, "WINDOWS-1253"),
        (64, "WINDOWS-1254"),
        (65, "WINDOWS-1255"),
        (66, "WINDOWS-1256"),
        (67, "WINDOWS-1257"),
        (68, "WINDOWS-1258"),
        (69, "MACROMAN"),
        (70, "MACARABIC"),
        (71, "MACHEBREW"),
        (72, "MACGREEK"),
        (73, "MACTHAI"),
        (75, "MACTURKISH"),
        (76, "MACUKRAINE"),
        (118, "CP950"),
        (119, "EUC-TW"),
        (123, "BIG-5"),
        (125, "GB18030"),
        (126, "WINDOWS-936"),
        (128, "CP1381"),
        (134, "EUC-JP"),
        (136, "CP949"),
        (137, "CP942"),
        (138, "CP932"),
        (140, "EUC-KR"),
        (141, "CP949"),
        (142, "CP949"),
        (163, "MACICELAND"),
        (167, "ISO-2022-JP"),
        (168, "ISO-2022-KR"),
        (169, "ISO-2022-CN"),
        (172, "ISO-2022-CN-EXT"),
        (204, "WINDOWS-1252"),
        (205, "GB18030"),
        (227, "ISO-8859-14"),
        (242, "ISO-8859-13"),
        (245, "MACCROATIAN"),
        (246, "MACCYRILLIC"),
        (247, "MACROMANIA"),
        (248, "SHIFT_JISX0213"),
    ];

    let mut i = 0usize;
    while i < ENCODING_MAP.len() {
        if ENCODING_MAP[i].0 == code {
            return Some(ENCODING_MAP[i].1);
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{SAS_ENDIAN_LITTLE, SAS7BDAT_MAGIC_NUMBER, lookup_encoding, probe_header};
    use crate::{internal::HeaderInfo, metadata::Endianness};
    use std::{io::Cursor, path::Path};

    #[test]
    fn probes_minimal_little_endian_32bit_header() {
        let bytes = minimal_header_bytes();
        let mut cursor = Cursor::new(bytes);
        let (header, metadata) = probe_header(&mut cursor).expect("synthetic header should parse");

        assert_header(&header);
        assert_eq!(metadata.endianness, Endianness::Little);
        assert_eq!(metadata.page_size, u32::from(crate::types::PageSize(4096)));
        assert_eq!(metadata.page_count, 1);
        assert_eq!(metadata.table_name.as_deref(), Some("TEST_TABLE"));
        assert_eq!(metadata.encoding.as_deref(), Some("UTF-8"));
    }

    #[test]
    fn rejects_invalid_magic() {
        let mut bytes = minimal_header_bytes();
        bytes[0] = 0xFF;
        let mut cursor = Cursor::new(bytes);
        let err = probe_header(&mut cursor).expect_err("bad magic should fail");
        let msg = err.to_string();
        assert!(msg.contains("invalid magic"));
    }

    #[test]
    fn supports_wider_sas_encoding_map() {
        assert_eq!(lookup_encoding(40), Some("ISO-8859-15"));
        assert_eq!(lookup_encoding(69), Some("MACROMAN"));
    }

    #[test]
    fn probes_local_54_class_fixture_when_present() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures/raw_data/csharp/54-class.sas7bdat");
        if !path.exists() {
            return;
        }

        let mut file = std::fs::File::open(&path).expect("open fixture");
        let (header, metadata) = probe_header(&mut file).expect("probe fixture header");
        eprintln!(
            "fixture_header data_offset={} page_size={} page_count={} page_header_size={} ptr_size={} endianness={:?} table={:?} encoding={:?}",
            header.data_offset,
            header.page_size,
            header.page_count,
            header.page_header_size,
            header.subheader_pointer_size,
            header.endianness,
            metadata.table_name,
            metadata.encoding
        );
    }

    fn assert_header(header: &HeaderInfo) {
        assert_eq!(header.endianness, Endianness::Little);
        assert!(!header.uses_u64_pointers);
        assert_eq!(header.page_size, crate::types::PageSize(4096));
        assert_eq!(header.page_count, 1);
        assert_eq!(header.page_header_size, 24);
        assert_eq!(header.subheader_pointer_size, 12);
        assert_eq!(header.subheader_signature_size, 4);
        assert_eq!(header.data_offset, 1024);
        assert_eq!(header.header_size, 1024);
        assert_eq!(header.release, "9.0000M0");
        assert!(!header.is_catalog);
    }

    fn minimal_header_bytes() -> Vec<u8> {
        let mut bytes = vec![0u8; 336];

        bytes[0..32].copy_from_slice(&SAS7BDAT_MAGIC_NUMBER);
        bytes[35] = 0x00;
        bytes[37] = SAS_ENDIAN_LITTLE;
        bytes[70] = 20;

        let table_name = b"TEST_TABLE";
        let table_start = 92;
        bytes[table_start..table_start + table_name.len()].copy_from_slice(table_name);

        let sizes_start = 196;
        bytes[sizes_start..sizes_start + 4].copy_from_slice(&1024u32.to_le_bytes());
        bytes[sizes_start + 4..sizes_start + 8].copy_from_slice(&4096u32.to_le_bytes());
        bytes[sizes_start + 8..sizes_start + 12].copy_from_slice(&1u32.to_le_bytes());

        let release_start = 216;
        bytes[release_start..release_start + 8].copy_from_slice(b"9.0000M0");

        bytes
    }
}
