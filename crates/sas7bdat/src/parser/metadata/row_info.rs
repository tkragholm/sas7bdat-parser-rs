use crate::dataset::Compression;

#[derive(Debug, Clone)]
pub struct RowInfo {
    pub row_length: u32,
    pub total_rows: u64,
    pub rows_per_page: u64,
    pub compression: Compression,
    pub file_label: Option<String>,
}
