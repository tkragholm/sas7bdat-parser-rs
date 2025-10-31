use std::borrow::Cow;
use std::convert::TryFrom;
use std::io::{Read, Seek, SeekFrom};

use time::{Duration, OffsetDateTime};

use crate::error::{Error, Result, Section};
use crate::metadata::{
    Compression, DatasetMetadata, DatasetTimestamps, Endianness, SasVersion, Vendor,
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

const SAS_EPOCH_OFFSET_SECONDS: i64 = -3653 * 86_400;

const SAS7BDAT_MAGIC_NUMBER: [u8; 32] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC2, 0xEA, 0x81, 0x60,
    0xB3, 0x14, 0x11, 0xCF, 0xBD, 0x92, 0x08, 0x00, 0x09, 0xC7, 0x31, 0x8C, 0x18, 0x1F, 0x10, 0x11,
];

const SAS7BCAT_MAGIC_NUMBER: [u8; 32] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC2, 0xEA, 0x81, 0x63,
    0xB3, 0x14, 0x11, 0xCF, 0xBD, 0x92, 0x08, 0x00, 0x09, 0xC7, 0x31, 0x8C, 0x18, 0x1F, 0x10, 0x11,
];

#[derive(Debug, Clone)]
pub struct SasHeader {
    pub metadata: DatasetMetadata,
    pub endianness: Endianness,
    pub uses_u64: bool,
    pub page_header_size: u32,
    pub subheader_pointer_size: u32,
    pub subheader_signature_size: usize,
    pub header_size: u32,
    pub page_size: u32,
    pub page_count: u64,
    pub pad_alignment: u32,
    pub data_offset: u64,
}

impl SasHeader {
    #[must_use]
    pub fn into_metadata(self) -> DatasetMetadata {
        self.metadata
    }
}

/// Parses the SAS7BDAT file header.
///
/// # Errors
///
/// Returns an error if the header bytes cannot be read or contain unsupported
/// values.
pub fn parse_header<R: Read + Seek>(reader: &mut R) -> Result<SasHeader> {
    let mut start_buf = [0u8; SAS_HEADER_START_SIZE];
    reader.read_exact(&mut start_buf).map_err(Error::from)?;

    let header_start = HeaderStart::from_bytes(start_buf);

    let is_catalog = header_start.magic == SAS7BCAT_MAGIC_NUMBER;
    if header_start.magic != SAS7BDAT_MAGIC_NUMBER && !is_catalog {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("unrecognized SAS magic number"),
        });
    }

    let endianness = match header_start.endian {
        SAS_ENDIAN_BIG => Endianness::Big,
        SAS_ENDIAN_LITTLE => Endianness::Little,
        _ => {
            return Err(Error::Corrupted {
                section: Section::Header,
                details: Cow::from("unsupported endian flag in header"),
            });
        }
    };

    let uses_u64 = header_start.a2 == SAS_ALIGNMENT_OFFSET_4;
    let pad_alignment = if header_start.a1 == SAS_ALIGNMENT_OFFSET_4 {
        4_u32
    } else {
        0
    };

    if pad_alignment > 0 {
        reader
            .seek(SeekFrom::Current(i64::from(pad_alignment)))
            .map_err(Error::from)?;
    }

    let timestamps = read_timestamps(reader, endianness)?;
    let header_size = read_u32(reader, endianness)?;
    let page_size = read_u32(reader, endianness)?;

    if !(SAS_HEADER_MIN_SIZE..=SAS_MAX_SIZE).contains(&header_size) {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("header size outside expected range"),
        });
    }
    if !(SAS_PAGE_MIN_SIZE..=SAS_MAX_SIZE).contains(&page_size) {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("page size outside expected range"),
        });
    }

    let page_count = if uses_u64 {
        read_u64(reader, endianness)?
    } else {
        u64::from(read_u32(reader, endianness)?)
    };

    if page_count > (1 << 24) {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("page count outside expected range"),
        });
    }

    reader.seek(SeekFrom::Current(8)).map_err(Error::from)?;

    let mut end_buf = [0u8; SAS_HEADER_END_SIZE];
    reader.read_exact(&mut end_buf).map_err(Error::from)?;
    let header_end = HeaderEnd::from_bytes(end_buf);

    let release = header_end.release()?;
    let (version, vendor) = parse_release(&release)?;

    let encoding = lookup_encoding(header_start.encoding).ok_or_else(|| Error::Unsupported {
        feature: Cow::from(format!("character set code {}", header_start.encoding)),
    })?;

    let table_name = decode_padded_string(&header_start.table_name);

    reader
        .seek(SeekFrom::Start(u64::from(header_size)))
        .map_err(Error::from)?;

    let mut metadata = DatasetMetadata::new(0);
    metadata.version = version;
    metadata.timestamps = timestamps;
    metadata.table_name = table_name;
    metadata.file_encoding = Some(encoding.to_owned());
    metadata.vendor = vendor;
    metadata.endianness = endianness;
    metadata.compression = Compression::None;

    Ok(SasHeader {
        metadata,
        endianness,
        uses_u64,
        page_header_size: if uses_u64 {
            SAS_PAGE_HEADER_SIZE_64BIT
        } else {
            SAS_PAGE_HEADER_SIZE_32BIT
        },
        subheader_pointer_size: if uses_u64 {
            SAS_SUBHEADER_POINTER_SIZE_64BIT
        } else {
            SAS_SUBHEADER_POINTER_SIZE_32BIT
        },
        subheader_signature_size: if uses_u64 { 8 } else { 4 },
        header_size,
        page_size,
        page_count,
        pad_alignment,
        data_offset: u64::from(header_size),
    })
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
        let mut idx = 0;
        let mut take = |len: usize| {
            let start = idx;
            idx += len;
            &bytes[start..start + len]
        };

        let mut magic = [0u8; 32];
        magic.copy_from_slice(take(32));

        let a2 = take(1)[0];
        let _mystery1 = take(2);
        let a1 = take(1)[0];
        let _mystery2 = take(1);
        let endian = take(1)[0];
        let _mystery3 = take(1);
        let _file_format = take(1)[0];
        let _mystery4 = take(30);
        let encoding = take(1)[0];
        let _mystery5 = take(13);
        let _file_type = take(8);

        let mut table_name = [0u8; 32];
        table_name.copy_from_slice(take(32));

        // skip mystery6 (32 bytes) and file_info (8 bytes)
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
        decode_padded_string(&self.release_raw).ok_or_else(|| Error::Corrupted {
            section: Section::Header,
            details: Cow::from("missing release string"),
        })
    }
}

fn parse_release(release: &str) -> Result<(SasVersion, Vendor)> {
    let release = release.trim();
    let mut chars = release.chars();
    let major_char = chars.next().ok_or_else(|| Error::Corrupted {
        section: Section::Header,
        details: Cow::from("empty release string"),
    })?;

    if chars.next() != Some('.') {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("release string missing '.' separator"),
        });
    }
    let minor_digits: String = chars.by_ref().take(4).collect();
    if minor_digits.len() != 4 || !minor_digits.chars().all(|c| c.is_ascii_digit()) {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("release string missing 4-digit minor version"),
        });
    }
    let revision_tag = chars.next().ok_or_else(|| Error::Corrupted {
        section: Section::Header,
        details: Cow::from("release string missing revision tag"),
    })?;
    if revision_tag != 'M' && revision_tag != 'J' {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from(format!("unexpected revision tag '{revision_tag}'")),
        });
    }
    let revision_char = chars.next().ok_or_else(|| Error::Corrupted {
        section: Section::Header,
        details: Cow::from("release string missing revision number"),
    })?;
    let revision = revision_char.to_digit(10).ok_or_else(|| Error::Corrupted {
        section: Section::Header,
        details: Cow::from("release revision is not numeric"),
    })?;

    let major = match major_char {
        '1'..='9' => major_char as u16 - '0' as u16,
        'V' => 9,
        other => {
            return Err(Error::Corrupted {
                section: Section::Header,
                details: Cow::from(format!("unsupported major version marker '{other}'")),
            });
        }
    };
    let minor = minor_digits.parse::<u16>().map_err(|_| Error::Corrupted {
        section: Section::Header,
        details: Cow::from("failed to parse minor version"),
    })?;

    let vendor = if (major == 8 || major == 9) && minor == 0 && revision == 0 {
        Vendor::StatTransfer
    } else {
        Vendor::Sas
    };

    let revision = u16::try_from(revision).map_err(|_| Error::Corrupted {
        section: Section::Header,
        details: Cow::from("revision value exceeds supported range"),
    })?;

    Ok((
        SasVersion {
            major,
            minor,
            revision,
        },
        vendor,
    ))
}

fn read_timestamps<R: Read>(reader: &mut R, endian: Endianness) -> Result<DatasetTimestamps> {
    let creation_time = read_f64(reader, endian)?;
    let modification_time = read_f64(reader, endian)?;
    let creation_diff = read_f64(reader, endian)?;
    let modification_diff = read_f64(reader, endian)?;

    let created = convert_sas_time(creation_time, creation_diff);
    let modified = convert_sas_time(modification_time, modification_diff);

    Ok(DatasetTimestamps { created, modified })
}

fn convert_sas_time(time: f64, diff: f64) -> Option<OffsetDateTime> {
    let delta = Duration::checked_seconds_f64(time - diff)?;
    let offset = Duration::seconds(SAS_EPOCH_OFFSET_SECONDS);
    let total = offset.checked_add(delta)?;
    OffsetDateTime::UNIX_EPOCH.checked_add(total)
}

fn read_u32<R: Read>(reader: &mut R, endian: Endianness) -> Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).map_err(Error::from)?;
    Ok(match endian {
        Endianness::Little => u32::from_le_bytes(buf),
        Endianness::Big => u32::from_be_bytes(buf),
    })
}

fn read_u64<R: Read>(reader: &mut R, endian: Endianness) -> Result<u64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf).map_err(Error::from)?;
    Ok(match endian {
        Endianness::Little => u64::from_le_bytes(buf),
        Endianness::Big => u64::from_be_bytes(buf),
    })
}

fn read_f64<R: Read>(reader: &mut R, endian: Endianness) -> Result<f64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf).map_err(Error::from)?;
    let bits = match endian {
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
    use super::*;

    #[test]
    fn parse_release_string_with_numeric_major() {
        let (version, vendor) = parse_release("9.0401M3").unwrap();
        assert_eq!(version.major, 9);
        assert_eq!(version.minor, 401);
        assert_eq!(version.revision, 3);
        assert!(matches!(vendor, Vendor::Sas));
    }

    #[test]
    fn parse_release_string_with_visual_forecaster() {
        let (version, vendor) = parse_release("V.0000M0").unwrap();
        assert_eq!(version.major, 9);
        assert_eq!(version.minor, 0);
        assert_eq!(version.revision, 0);
        assert!(matches!(vendor, Vendor::StatTransfer));
    }

    #[test]
    fn decode_string_trims_whitespace() {
        let input = b"TABLE NAME           ";
        let result = decode_padded_string(input).unwrap();
        assert_eq!(result, "TABLE NAME");
    }

    #[test]
    fn lookup_known_encoding() {
        assert_eq!(lookup_encoding(20), Some("UTF-8"));
        assert_eq!(lookup_encoding(0), Some("WINDOWS-1252"));
        assert!(lookup_encoding(255).is_none());
    }

    #[test]
    fn convert_time_handles_nan() {
        assert!(convert_sas_time(f64::NAN, 0.0).is_none());
    }
}
