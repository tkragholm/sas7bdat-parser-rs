use crate::{
    dataset::Dataset,
    internal::{FileInner, FileSource, HeaderInfo, LayoutPlan},
    metadata::{ColumnMeta, CompressionKind, DatasetMetadata, Endianness, LogicalType},
    options::OpenOptions,
    types::{PageSize, RowLength},
};
use std::sync::Arc;

pub struct MockDatasetBuilder {
    pub columns: Vec<ColumnMeta>,
    pub endianness: Endianness,
    pub page_size: usize,
    pub row_len: usize,
    pub total_rows: Option<u64>,
    pub rows_per_page: Option<u64>,
    pub compression: CompressionKind,
    pub encoding: Option<String>,
    pub bytes: Arc<[u8]>,
}

impl MockDatasetBuilder {
    pub fn new(bytes: Arc<[u8]>) -> Self {
        Self {
            columns: Vec::new(),
            endianness: Endianness::Little,
            page_size: 64,
            row_len: 0,
            total_rows: None,
            rows_per_page: None,
            compression: CompressionKind::None,
            encoding: Some("UTF-8".to_owned()),
            bytes,
        }
    }

    pub fn with_column(mut self, name: &str, logical_type: LogicalType, physical_width: u32, offset: u32) -> Self {
        let index = self.columns.len();
        self.columns.push(ColumnMeta {
            index,
            name: name.to_owned(),
            logical_type,
            physical_width,
            offset,
            label: None,
            format: None,
        });
        self
    }

    pub fn with_row_len(mut self, row_len: usize) -> Self {
        self.row_len = row_len;
        self
    }

    pub fn with_total_rows(mut self, total_rows: u64) -> Self {
        self.total_rows = Some(total_rows);
        self
    }

    pub fn with_rows_per_page(mut self, rows_per_page: u64) -> Self {
        self.rows_per_page = Some(rows_per_page);
        self
    }

    pub fn with_compression(mut self, compression: CompressionKind) -> Self {
        self.compression = compression;
        self
    }

    pub fn with_encoding(mut self, encoding: Option<String>) -> Self {
        self.encoding = encoding;
        self
    }

    pub fn build(self) -> Dataset {
        let row_len_u32 = u32::try_from(self.row_len).unwrap_or(0);
        let calculated_total_rows = if row_len_u32 > 0 {
            u64::try_from(self.bytes.len() / (row_len_u32 as usize)).unwrap_or(0)
        } else {
            0
        };
        let total_rows = self.total_rows.unwrap_or(calculated_total_rows);
        let rows_per_page = self.rows_per_page.unwrap_or(total_rows);

        let page_size_u32 = u32::try_from(self.page_size).unwrap();
        let page_count = u64::try_from(self.bytes.len() / (page_size_u32 as usize)).unwrap_or(1);

        let layout = LayoutPlan {
            columns: self.columns,
            header: HeaderInfo {
                endianness: self.endianness,
                uses_u64_pointers: false,
                page_size: PageSize(page_size_u32),
                page_count,
                page_header_size: 24,
                subheader_pointer_size: 12,
                subheader_signature_size: 4,
                data_offset: 0,
                header_size: 0,
                release: String::new(),
                is_catalog: false,
            },
            row_len: RowLength(row_len_u32),
            total_rows,
            compression: self.compression,
            rows_per_page,
        };

        
        Dataset {
            file: Arc::new(FileInner {
                source: FileSource::Bytes(Arc::clone(&self.bytes)),
                options: OpenOptions::default(),
            }),
            metadata: Arc::new(DatasetMetadata {
                row_count: total_rows,
                row_len: u32::try_from(self.row_len).unwrap(),
                compression: self.compression,
                encoding: self.encoding,
                ..DatasetMetadata::default()
            }),
            layout: Arc::new(layout.clone()),
            descriptors: Arc::new(
                crate::pages::compile_page_descriptors(
                    &mut std::io::Cursor::new(self.bytes.as_ref()),
                    &layout,
                )
                .expect("descriptors"),
            ),
        }
    }
}

pub fn write_page_header(
    page: &mut [u8],
    page_type: u16,
    row_count: u16,
    pointer_count: u16,
) {
    page[(24 - 8)..(24 - 6)].copy_from_slice(&page_type.to_le_bytes());
    page[(24 - 6)..(24 - 4)].copy_from_slice(&row_count.to_le_bytes());
    page[(24 - 4)..(24 - 2)].copy_from_slice(&pointer_count.to_le_bytes());
}

pub fn make_page(
    page_type: u16,
    row_count: u16,
    pointer_count: u16,
    rows: &[&[u8]],
    page_size: usize,
) -> Vec<u8> {
    let mut page = vec![0u8; page_size];
    write_page_header(&mut page, page_type, row_count, pointer_count);

    let mut offset = 24usize;
    for row in rows {
        page[offset..offset + row.len()].copy_from_slice(row);
        offset += row.len();
    }
    page
}

pub fn make_pointer_page(rows: &[&[u8]], page_size: usize) -> Vec<u8> {
    let mut page = vec![0u8; page_size];
    page[(24 - 8)..(24 - 6)].copy_from_slice(&0x0200u16.to_le_bytes());
    page[(24 - 6)..(24 - 4)]
        .copy_from_slice(&u16::try_from(rows.len()).expect("rows").to_le_bytes());
    page[(24 - 4)..(24 - 2)].copy_from_slice(&1u16.to_le_bytes());

    let data_offset = 40u32;
    let data_len = u32::try_from(rows.len() * 4).unwrap_or(u32::MAX);
    page[24..28].copy_from_slice(&data_offset.to_le_bytes());
    page[28..32].copy_from_slice(&data_len.to_le_bytes());
    page[32] = 0;
    page[33] = 1;

    let mut offset = data_offset as usize;
    for row in rows {
        page[offset..offset + row.len()].copy_from_slice(row);
        offset += row.len();
    }
    page
}

pub fn make_compressed_page(compressed: &[u8], page_size: usize, compression_flag: u8) -> Vec<u8> {
    make_raw_compressed_page(compressed, page_size, compression_flag, 0x0200)
}

pub fn make_compressed_meta_page(compressed: &[u8], page_size: usize) -> Vec<u8> {
    make_raw_compressed_page(compressed, page_size, 4, 0x0000)
}

fn make_raw_compressed_page(
    compressed: &[u8],
    page_size: usize,
    compression_flag: u8,
    page_type: u16,
) -> Vec<u8> {
    let mut page = vec![0u8; page_size];
    write_page_header(&mut page, page_type, 1, 1);

    let data_offset = 40u32;
    let data_len = u32::try_from(compressed.len()).unwrap_or(u32::MAX);
    page[24..28].copy_from_slice(&data_offset.to_le_bytes());
    page[28..32].copy_from_slice(&data_len.to_le_bytes());
    page[32] = compression_flag;
    page[33] = 1;

    let start = usize::try_from(data_offset).unwrap_or(0);
    let end = start + compressed.len();
    page[start..end].copy_from_slice(compressed);
    page
}

pub fn make_numeric_text_row(number: f64, text: [u8; 4]) -> Vec<u8> {
    let mut row = Vec::with_capacity(12);
    row.extend_from_slice(&number.to_le_bytes());
    row.extend_from_slice(&text);
    row
}

pub fn make_mixed_pointer_page(
    pointer_row: [u8; 4],
    contiguous_row: [u8; 4],
    page_size: usize,
) -> Vec<u8> {
    let mut page = vec![0u8; page_size];
    page[(24 - 8)..(24 - 6)].copy_from_slice(&0x0200u16.to_le_bytes());
    page[(24 - 6)..(24 - 4)].copy_from_slice(&2u16.to_le_bytes());
    page[(24 - 4)..(24 - 2)].copy_from_slice(&1u16.to_le_bytes());

    let pointer_data_offset = 48u32;
    page[24..28].copy_from_slice(&pointer_data_offset.to_le_bytes());
    page[28..32].copy_from_slice(&4u32.to_le_bytes());
    page[32] = 0;
    page[33] = 1;

    page[40..44].copy_from_slice(&contiguous_row);
    let start = usize::try_from(pointer_data_offset).unwrap_or(0);
    page[start..start + 4].copy_from_slice(&pointer_row);
    page
}

pub fn simple_layout(page_size: u32, page_count: u64, row_len: u32, total_rows: u64) -> LayoutPlan {
    LayoutPlan {
        columns: Vec::new(),
        header: HeaderInfo {
            endianness: Endianness::Little,
            uses_u64_pointers: false,
            page_size: PageSize(page_size),
            page_count,
            page_header_size: 24,
            subheader_pointer_size: 12,
            subheader_signature_size: 4,
            data_offset: 0,
            header_size: 0,
            release: String::new(),
            is_catalog: false,
        },
        row_len: RowLength(row_len),
        total_rows,
        compression: CompressionKind::None,
        rows_per_page: 1,
    }
}

pub fn read_utf8_column(column: &crate::ColumnBuffer<'_>) -> Vec<String> {
    let crate::ColumnBuffer::Utf8(buffer) = column else {
        panic!("expected utf8 column, got {column:?}");
    };
    buffer
        .offsets
        .windows(2)
        .map(|window| {
            let start = usize::try_from(window[0]).expect("utf8 start");
            let end = usize::try_from(window[1]).expect("utf8 end");
            String::from_utf8(buffer.data[start..end].to_vec()).expect("utf8 cell")
        })
        .collect()
}
