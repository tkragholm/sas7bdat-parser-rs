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

pub fn parse_subheader_signature(header: &HeaderInfo, data: &[u8]) -> Option<u32> {
    if data.len() < header.subheader_signature_size {
        return None;
    }
    let mut signature = read_u32(header.endianness, &data[0..4]);
    if matches!(header.endianness, crate::metadata::Endianness::Big)
        && header.uses_u64_pointers
        && signature == u32::MAX
        && data.len() >= 8
    {
        signature = read_u32(header.endianness, &data[4..8]);
    }
    Some(signature)
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
