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
}

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct KernelSet;

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct PageSource;

#[derive(Debug, Clone)]
pub struct SmallCommandBlock<const N: usize = 16> {
    pub len: u8,
    pub ops: [SmallOp; N],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SmallOp {
    Fill { byte: u8, len: u16 },
    Literal { src_off: u32, len: u16 },
    CopyBackref { back: u16, len: u16 },
}

impl Default for SmallOp {
    fn default() -> Self {
        Self::Fill { byte: 0, len: 0 }
    }
}

impl<const N: usize> Default for SmallCommandBlock<N> {
    fn default() -> Self {
        Self {
            len: 0,
            ops: [SmallOp::default(); N],
        }
    }
}
