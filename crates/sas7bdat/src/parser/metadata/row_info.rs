use super::text_store::TextRef;
use crate::dataset::Compression;

#[derive(Debug, Clone)]
pub struct RowInfo {
    pub row_length: u32,
    pub total_rows: u64,
    pub rows_per_page: u64,
    pub compression: Compression,
    pub file_label: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct RowInfoRaw {
    pub row_length: u32,
    pub total_rows: u64,
    pub rows_per_page: u64,
    pub compression_ref: TextRef,
    pub label_ref: TextRef,
}
