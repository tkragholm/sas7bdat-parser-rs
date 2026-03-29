#![allow(dead_code)]

use crate::{metadata::ColumnMeta, options::OpenOptions};
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
    pub rows_per_page: u64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PageDescriptorTable;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PageExecClass {
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
    pub ops: [Option<SmallOp>; N],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SmallOp {
    Literal { src_off: u32, len: u16 },
    Fill { byte: u8, len: u16 },
    CopyBackref { back: u16, len: u16 },
}

impl<const N: usize> Default for SmallCommandBlock<N> {
    fn default() -> Self {
        Self {
            len: 0,
            ops: [None; N],
        }
    }
}
