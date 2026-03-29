#![allow(dead_code)]

use crate::{
    metadata::{ColumnMeta, CompressionKind},
    options::OpenOptions,
};
use std::{path::PathBuf, sync::Arc};

#[derive(Debug)]
pub(crate) enum FileSource {
    Path(PathBuf),
    Bytes(Arc<[u8]>),
}

#[derive(Debug)]
pub(crate) struct FileInner {
    pub source: FileSource,
    pub options: OpenOptions,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct LayoutPlan {
    pub columns: Vec<ColumnMeta>,
    pub header: HeaderInfo,
    pub row_len: u32,
    pub total_rows: u64,
    pub compression: CompressionKind,
    pub rows_per_page: u64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PageDescriptorTable {
    pub pages: Box<[PageDescriptor]>,
    pub row_spans: Box<[RowSpan]>,
    pub total_candidate_rows: u64,
}

impl PageDescriptorTable {
    #[must_use]
    pub fn has_non_fused_pages(&self) -> bool {
        self.pages
            .iter()
            .any(|page| page.exec_class != PageExecClass::FusedContiguousUncompressed)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PageDescriptor {
    pub page_index: u64,
    pub row_base: u64,
    pub row_count: u32,
    pub data_start: u32,
    pub row_span_start: u32,
    pub row_span_count: u32,
    pub exec_class: PageExecClass,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RowSpan {
    pub offset: u32,
    pub len: u32,
    pub kind: RowSpanKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum RowSpanKind {
    #[default]
    Borrowed,
    Compressed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum PageExecClass {
    #[default]
    FusedContiguousUncompressed,
    IndexedPointerRows,
    IndexedCompressedRows,
    MetadataOrEmpty,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ProjectionPlan {
    pub columns: Vec<usize>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct HeaderInfo {
    pub endianness: crate::metadata::Endianness,
    pub uses_u64_pointers: bool,
    pub page_size: u32,
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
pub(crate) struct KernelSet;

#[derive(Debug, Clone, Default)]
pub(crate) struct PageSource;

#[derive(Debug, Clone)]
pub(crate) struct SmallCommandBlock<const N: usize = 16> {
    pub len: u8,
    pub ops: [SmallOp; N],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SmallOp {
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
