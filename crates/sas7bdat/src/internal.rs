use crate::{
    metadata::{ColumnMeta, CompressionKind, LogicalType},
    options::OpenOptions,
    types::{ByteOffset, ColumnIndex, PageIndex, PageSize, RowIndex, RowLength},
};
use memmap2::Mmap;
use std::{path::PathBuf, sync::Arc};

#[derive(Debug)]
pub enum FileSource {
    Path(PathBuf),
    Bytes(Arc<[u8]>),
    Mmap(Arc<Mmap>),
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct FileInner {
    pub source: FileSource,
    pub options: OpenOptions,
}

#[derive(Debug, Clone, Default)]
pub struct LayoutPlan {
    pub columns: Vec<ColumnMeta>,
    pub header: HeaderInfo,
    pub row_len: RowLength,
    pub total_rows: u64,
    /// How many of `total_rows` are tombstones. Zero for almost every file, and when it is
    /// zero no page is examined for a deleted-row bitmap at all.
    pub deleted_rows: u64,
    pub compression: CompressionKind,
    pub rows_per_page: u64,
}

#[derive(Debug, Clone, Default)]
pub struct PageDescriptorTable {
    pub pages: Box<[PageDescriptor]>,
    pub row_spans: Box<[RowSpan]>,
    pub total_candidate_rows: u64,
}

impl PageDescriptorTable {
    #[must_use]
    #[allow(dead_code)]
    pub fn has_non_fused_pages(&self) -> bool {
        self.pages
            .iter()
            .any(|page| page.exec_class != PageExecClass::FusedContiguousUncompressed)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PageDescriptor {
    pub page_index: PageIndex,
    pub row_base: RowIndex,
    pub row_count: u32,
    pub data_start: ByteOffset,
    pub row_span_start: u32,
    pub row_span_count: u32,
    pub exec_class: PageExecClass,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RowSpan {
    pub offset: ByteOffset,
    pub len: u32,
    pub kind: RowSpanKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RowSpanKind {
    #[default]
    Borrowed,
    Compressed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PageExecClass {
    #[default]
    FusedContiguousUncompressed,
    IndexedPointerRows,
    IndexedCompressedRows,
    MetadataOrEmpty,
}

#[derive(Debug, Clone, Default)]
pub struct ProjectionPlan {
    pub columns: Box<[ProjectedColumnPlan]>,
    pub max_end: ByteOffset,
}

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub struct ProjectedColumnPlan {
    pub index: ColumnIndex,
    pub offset: ByteOffset,
    pub width: u32,
    pub end: ByteOffset,
    pub logical_type: LogicalType,
}

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct HeaderInfo {
    pub endianness: crate::metadata::Endianness,
    pub uses_u64_pointers: bool,
    pub page_size: PageSize,
    pub page_count: u64,
    pub page_header_size: u32,
    pub subheader_pointer_size: u32,
    pub subheader_signature_size: usize,
    pub data_offset: u64,
    pub header_size: u32,
    pub release: String,
    pub is_catalog: bool,
    pub pad_alignment: u32,
}

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct KernelSet;

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct PageSource;

pub const fn read_u16(endianness: crate::metadata::Endianness, bytes: &[u8]) -> u16 {
    let mut buf = [0u8; 2];
    buf[0] = bytes[0];
    buf[1] = bytes[1];
    match endianness {
        crate::metadata::Endianness::Little => u16::from_le_bytes(buf),
        crate::metadata::Endianness::Big => u16::from_be_bytes(buf),
    }
}

pub const fn read_u32(endianness: crate::metadata::Endianness, bytes: &[u8]) -> u32 {
    let mut buf = [0u8; 4];
    buf[0] = bytes[0];
    buf[1] = bytes[1];
    buf[2] = bytes[2];
    buf[3] = bytes[3];
    match endianness {
        crate::metadata::Endianness::Little => u32::from_le_bytes(buf),
        crate::metadata::Endianness::Big => u32::from_be_bytes(buf),
    }
}

pub const fn read_u64(endianness: crate::metadata::Endianness, bytes: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    buf[0] = bytes[0];
    buf[1] = bytes[1];
    buf[2] = bytes[2];
    buf[3] = bytes[3];
    buf[4] = bytes[4];
    buf[5] = bytes[5];
    buf[6] = bytes[6];
    buf[7] = bytes[7];
    match endianness {
        crate::metadata::Endianness::Little => u64::from_le_bytes(buf),
        crate::metadata::Endianness::Big => u64::from_be_bytes(buf),
    }
}

// Subheader signatures. Defined once here because `layout` and `pages` both classify
// subheaders and previously each kept its own copy of the list.
pub const SIG_ROW_SIZE: u32 = 0xF7F7_F7F7;
pub const SIG_COLUMN_SIZE: u32 = 0xF6F6_F6F6;
pub const SIG_COLUMN_TEXT: u32 = 0xFFFF_FFFD;
pub const SIG_COLUMN_NAME: u32 = 0xFFFF_FFFF;
pub const SIG_COLUMN_ATTRS: u32 = 0xFFFF_FFFC;
pub const SIG_COLUMN_FORMAT: u32 = 0xFFFF_FBFE;
pub const SIG_COUNTS: u32 = 0xFFFF_FC00;
pub const SIG_COLUMN_LIST: u32 = 0xFFFF_FFFE;

/// On a 64-bit file every signature except `ROW_SIZE` and `COLUMN_SIZE` carries an all-ones
/// upper word. That word is the only thing separating a real signature from eight bytes of
/// data that happen to start with the right four.
const SIG_64BIT_UPPER: u64 = 0xFFFF_FFFF_0000_0000;

/// The subheader's signature, or `None` when these bytes are data rather than a signature.
///
/// Both callers treat `None` and an unrecognised value the same way, as "not a subheader we
/// know", which is what makes returning `None` here safe.
///
/// **All eight bytes matter on a little-endian 64-bit file.** Reading only the first four and
/// comparing against the 32-bit constants misreads any `f64` whose low word happens to equal
/// one: an uncompressed row stored in a subheader, whose first numeric column holds such a
/// value, is then taken for a subheader and the row is skipped or fails to parse. That is
/// WizardMac/ReadStat#369, and little-endian 64-bit is what SAS on Linux writes, so it is the
/// common shape rather than an exotic one.
///
/// The two widths lay the eight bytes out differently, and the difference is not cosmetic:
///
/// - **little-endian**: the whole `u64` is the value. `ROW_SIZE` and `COLUMN_SIZE` carry a
///   zero upper word; every other signature carries an all-ones upper word. Anything else is
///   data.
/// - **big-endian**: the significant four bytes come first, followed by padding, *except*
///   for the `0xFFFF_*` family, where an all-ones word comes first and the signature second.
///
/// Upstream's fix for #369 applies the little-endian rule to both, which breaks big-endian
/// 64-bit files: `ReadStat` at `da9fcaa` fails on `raw_data/csharp/54-class.sas7bdat`
/// ("A row in the file was not the expected length", 0 rows) where 1.1.9 read it. That file
/// is big-endian 64-bit, a shape their test suite does not cover. So the big-endian branch
/// here is deliberately the older logic, which is correct for it.
pub fn parse_subheader_signature(header: &HeaderInfo, data: &[u8]) -> Option<u32> {
    if data.len() < header.subheader_signature_size {
        return None;
    }
    if !header.uses_u64_pointers {
        return Some(read_u32(header.endianness, &data[0..4]));
    }
    if data.len() < 8 {
        return None;
    }

    if matches!(header.endianness, crate::metadata::Endianness::Big) {
        let leading = read_u32(header.endianness, &data[0..4]);
        return Some(if leading == u32::MAX {
            read_u32(header.endianness, &data[4..8])
        } else {
            leading
        });
    }

    let wide = read_u64(header.endianness, &data[0..8]);
    if wide == u64::from(SIG_ROW_SIZE) || wide == u64::from(SIG_COLUMN_SIZE) {
        return u32::try_from(wide).ok();
    }
    if wide & SIG_64BIT_UPPER == SIG_64BIT_UPPER {
        return u32::try_from(wide & 0xFFFF_FFFF).ok();
    }
    None
}

pub const SAS_PAGE_TYPE_MASK: u16 = 0x0F00;
pub const SAS_PAGE_TYPE_META: u16 = 0x0000;
pub const SAS_PAGE_TYPE_DATA: u16 = 0x0100;
pub const SAS_PAGE_TYPE_MIX: u16 = 0x0200;
pub const SAS_PAGE_TYPE_META2: u16 = 0x4000;
pub const SAS_PAGE_TYPE_AMD: u16 = 0x0400;
pub const SAS_PAGE_TYPE_COMP: u16 = 0x9000;
pub const SAS_PAGE_TYPE_COMP_TABLE: u16 = 0x8000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageKind {
    Meta,
    Data,
    Mix,
    Amd,
    Meta2,
    Comp,
    CompTable,
    Unknown,
}

pub const fn classify_page(page_type: u16) -> PageKind {
    if (page_type & SAS_PAGE_TYPE_COMP) == SAS_PAGE_TYPE_COMP {
        return PageKind::Comp;
    }
    if (page_type & SAS_PAGE_TYPE_COMP_TABLE) == SAS_PAGE_TYPE_COMP_TABLE {
        return PageKind::CompTable;
    }
    if (page_type & SAS_PAGE_TYPE_META2) == SAS_PAGE_TYPE_META2 {
        return PageKind::Meta2;
    }
    match page_type & SAS_PAGE_TYPE_MASK {
        SAS_PAGE_TYPE_META => PageKind::Meta,
        SAS_PAGE_TYPE_DATA => PageKind::Data,
        SAS_PAGE_TYPE_MIX => PageKind::Mix,
        SAS_PAGE_TYPE_AMD => PageKind::Amd,
        _ => PageKind::Unknown,
    }
}

#[cfg(test)]
mod signature_tests {
    use super::{HeaderInfo, SIG_COLUMN_TEXT, SIG_ROW_SIZE, parse_subheader_signature};
    use crate::metadata::Endianness;
    use crate::types::PageSize;

    fn header(uses_u64_pointers: bool) -> HeaderInfo {
        HeaderInfo {
            endianness: Endianness::Little,
            uses_u64_pointers,
            page_size: PageSize(4096),
            page_count: 1,
            page_header_size: if uses_u64_pointers { 40 } else { 24 },
            subheader_pointer_size: if uses_u64_pointers { 24 } else { 12 },
            subheader_signature_size: if uses_u64_pointers { 8 } else { 4 },
            data_offset: 0,
            header_size: 0,
            release: String::new(),
            is_catalog: false,
            pad_alignment: 0,
        }
    }

    /// The three values `ReadStat`'s own test for `WizardMac/ReadStat#369` uses. Each is an
    /// ordinary `f64` whose low four bytes, read alone, are a valid subheader signature.
    /// On a 64-bit file the upper word is what distinguishes them from the real thing.
    #[test]
    fn doubles_that_collide_in_their_low_word_are_not_signatures() {
        let h = header(true);
        for value in [
            0.001_044_974_633_145_565_9_f64, // F7 F7 F7 F7 ... looks like ROW_SIZE
            -1.317_785_874_549_065_4e-51_f64, // F9 FF FF FF ... looks like a COLUMN_* value
            4.484_192_964_865_350_7e-13_f64, // FD FF FF FF ... looks like COLUMN_TEXT
        ] {
            let bytes = value.to_le_bytes();
            assert_eq!(
                parse_subheader_signature(&h, &bytes),
                None,
                "{value:e} has a data upper word and must not read as a signature"
            );
            // The bug this guards: taking only the low four bytes finds a signature.
            let low = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            assert!(
                low == SIG_ROW_SIZE || low & 0xFFFF_FFF8 == 0xFFFF_FFF8,
                "{value:e} was chosen because its low word collides; it no longer does"
            );
        }
    }

    #[test]
    fn real_64bit_signatures_still_parse() {
        let h = header(true);
        // ROW_SIZE and COLUMN_SIZE carry a zero upper word.
        let mut row_size = [0u8; 8];
        row_size[..4].copy_from_slice(&SIG_ROW_SIZE.to_le_bytes());
        assert_eq!(parse_subheader_signature(&h, &row_size), Some(SIG_ROW_SIZE));

        // Everything else carries an all-ones upper word.
        let mut column_text = [0xFFu8; 8];
        column_text[..4].copy_from_slice(&SIG_COLUMN_TEXT.to_le_bytes());
        assert_eq!(
            parse_subheader_signature(&h, &column_text),
            Some(SIG_COLUMN_TEXT)
        );
    }

    /// A 32-bit file has only four bytes to go on, so its behaviour must not change.
    #[test]
    fn the_32bit_path_reads_four_bytes_as_before() {
        let h = header(false);
        assert_eq!(
            parse_subheader_signature(&h, &SIG_COLUMN_TEXT.to_le_bytes()),
            Some(SIG_COLUMN_TEXT)
        );
        assert_eq!(
            parse_subheader_signature(&h, &0x1234_5678_u32.to_le_bytes()),
            Some(0x1234_5678)
        );
    }
}
