use crate::{
    dataset::{Compression, Variable},
    error::{Error, Result, Section},
    logger::log_warn,
    parser::{
        core::{
            byteorder::{read_u16, read_u32, read_u64},
            encoding::resolve_encoding,
        },
        header::{SasHeader, parse_header},
    },
};
use std::{
    borrow::Cow,
    convert::TryFrom,
    io::{Read, Seek, SeekFrom},
};
use subheaders::{
    parse_column_attrs_subheader, parse_column_format_subheader, parse_column_list_subheader,
    parse_column_name_subheader, parse_column_size_subheader, parse_column_text_subheader,
    parse_row_size_subheader,
};

mod builder;
mod column_info;
mod row_info;
mod subheaders;
#[cfg(test)]
mod tests;
mod text_store;

pub use builder::ColumnMetadataBuilder;
pub use column_info::{ColumnInfo, ColumnKind, ColumnOffsets, NumericKind};
pub use row_info::RowInfo;
pub use text_store::{TextRef, TextStore};

#[derive(Debug)]
pub struct DatasetLayout {
    pub header: SasHeader,
    pub text_store: TextStore,
    pub columns: Vec<ColumnInfo>,
    pub row_info: RowInfo,
    pub column_list: Option<Vec<i16>>,
}

impl DatasetLayout {
    /// Creates a row iterator for the stored metadata and supplied reader.
    ///
    /// # Errors
    ///
    /// Returns an error if the iterator cannot be constructed.
    pub fn row_iterator<'a, R: Read + Seek>(
        &'a self,
        reader: &'a mut R,
    ) -> Result<crate::parser::rows::RowIterator<'a, R>> {
        crate::parser::rows::row_iterator(reader, self)
    }
}

const SAS_PAGE_TYPE_MASK: u16 = 0x0F00;
const SAS_PAGE_TYPE_META: u16 = 0x0000;
const SAS_PAGE_TYPE_DATA: u16 = 0x0100;
const SAS_PAGE_TYPE_MIX: u16 = 0x0200;
const SAS_PAGE_TYPE_META2: u16 = 0x4000;
const SAS_PAGE_TYPE_AMD: u16 = 0x0400;
const SAS_PAGE_TYPE_COMP: u16 = 0x9000;
const SAS_PAGE_TYPE_COMP_TABLE: u16 = 0x8000; // observed -28672 signed / 36864 unsigned

const SIG_ROW_SIZE: u32 = 0xF7F7_F7F7;
const SIG_COLUMN_SIZE: u32 = 0xF6F6_F6F6;
const SIG_COLUMN_TEXT: u32 = 0xFFFF_FFFD;
const SIG_COLUMN_NAME: u32 = 0xFFFF_FFFF;
const SIG_COLUMN_ATTRS: u32 = 0xFFFF_FFFC;
const SIG_COLUMN_FORMAT: u32 = 0xFFFF_FBFE;
const SIG_COLUMN_LIST: u32 = 0xFFFF_FFFE;

/// Parses dataset metadata from a SAS7BDAT stream.
///
/// # Errors
///
/// Returns an error if the metadata pages cannot be decoded.
pub fn parse_metadata<R: Read + Seek>(reader: &mut R) -> Result<DatasetLayout> {
    let mut header = parse_header(reader)?;
    let encoding = resolve_encoding(header.metadata.file_encoding.as_deref());
    let mut builder = ColumnMetadataBuilder::new(encoding);

    collect_column_text(reader, &header, &mut builder)?;

    let mut state = MetaState::default();
    collect_column_metadata(reader, &header, &mut builder, &mut state)?;

    let column_count = state.column_count.ok_or_else(|| Error::InvalidMetadata {
        details: "column count not found in SAS metadata".into(),
    })?;
    let row_info = state.row_info.ok_or_else(|| Error::InvalidMetadata {
        details: "row size subheader missing from SAS metadata".into(),
    })?;

    let (text_store, mut columns, column_list) = builder.finalize();
    columns.truncate(column_count as usize);

    let mut metadata = header.metadata;
    metadata.column_count = column_count;
    metadata.row_count = row_info.total_rows;
    metadata.compression = match row_info.compression {
        Compression::Binary | Compression::Row => row_info.compression,
        _ => metadata.compression,
    };
    metadata.file_label.clone_from(&row_info.file_label);
    metadata.variables = build_variables(column_count, &columns, &text_store)?;
    metadata.column_list = column_list.clone().unwrap_or_default();

    header.metadata = metadata;

    Ok(DatasetLayout {
        header,
        text_store,
        columns,
        row_info,
        column_list,
    })
}

fn build_variables(
    column_count: u32,
    columns: &[ColumnInfo],
    text_store: &TextStore,
) -> Result<Vec<Variable>> {
    let mut variables = Vec::with_capacity(column_count as usize);
    for index in 0..column_count {
        let info = columns
            .get(index as usize)
            .ok_or_else(|| Error::InvalidMetadata {
                details: format!("missing column info for index {index}").into(),
            })?;
        let mut variable = Variable {
            index,
            ..Variable::default()
        };
        info.apply_to_variable(text_store, &mut variable)?;
        variables.push(variable);
    }
    Ok(variables)
}

fn collect_column_text<R: Read + Seek>(
    reader: &mut R,
    header: &SasHeader,
    builder: &mut ColumnMetadataBuilder,
) -> Result<()> {
    scan_pages(reader, header, |page_type, subheaders| {
        if !is_meta_page(page_type) {
            return Ok(());
        }
        for subheader in subheaders {
            if subheader.signature == SIG_COLUMN_TEXT {
                parse_column_text_subheader(
                    builder,
                    subheader.data,
                    header.subheader_signature_size,
                    header.endianness,
                )?;
            }
        }
        Ok(())
    })
}

fn collect_column_metadata<R: Read + Seek>(
    reader: &mut R,
    header: &SasHeader,
    builder: &mut ColumnMetadataBuilder,
    state: &mut MetaState,
) -> Result<()> {
    scan_pages(reader, header, |page_type, subheaders| {
        if !is_meta_page(page_type) {
            return Ok(());
        }

        for subheader in subheaders {
            match subheader.signature {
                SIG_COLUMN_NAME => parse_column_name_subheader(
                    builder,
                    subheader.data,
                    header.subheader_signature_size,
                    header.endianness,
                    header.uses_u64,
                )?,
                SIG_COLUMN_ATTRS => parse_column_attrs_subheader(
                    builder,
                    subheader.data,
                    header.subheader_signature_size,
                    header.endianness,
                    header.uses_u64,
                )?,
                SIG_COLUMN_FORMAT => parse_column_format_subheader(
                    builder,
                    subheader.data,
                    header.endianness,
                    header.uses_u64,
                )?,
                SIG_COLUMN_LIST => parse_column_list_subheader(
                    builder,
                    subheader.data,
                    header.subheader_signature_size,
                    header.endianness,
                    header.uses_u64,
                )?,
                SIG_COLUMN_SIZE => {
                    let column_count = parse_column_size_subheader(
                        builder,
                        subheader.data,
                        header.endianness,
                        header.uses_u64,
                    )?;
                    state.column_count = Some(column_count);
                }
                SIG_ROW_SIZE => {
                    let row_info = parse_row_size_subheader(
                        builder,
                        subheader.data,
                        header.subheader_signature_size,
                        header.endianness,
                        header.uses_u64,
                    )?;
                    state.row_info = Some(row_info);
                }
                _ => {
                    // counts subheaders and other signatures are ignored.
                }
            }
        }
        Ok(())
    })
}

#[derive(Default)]
struct MetaState {
    column_count: Option<u32>,
    row_info: Option<RowInfo>,
}

struct ParsedSubheader<'a> {
    signature: u32,
    data: &'a [u8],
}

fn scan_pages<R, F>(reader: &mut R, header: &SasHeader, mut f: F) -> Result<()>
where
    R: Read + Seek,
    F: FnMut(u16, Vec<ParsedSubheader<'_>>) -> Result<()>,
{
    let mut buffer = vec![0u8; header.page_size as usize];
    let mut visited = std::collections::HashSet::new();
    let last_examined = scan_forward(reader, header, &mut buffer, &mut visited, &mut f)?;
    if last_examined + 1 < header.page_count {
        // Only run the backward scan if the forward pass bailed early.
        scan_backward(reader, header, &mut buffer, &visited, last_examined, &mut f)?;
    }
    Ok(())
}

fn scan_forward<R, F>(
    reader: &mut R,
    header: &SasHeader,
    buffer: &mut [u8],
    visited: &mut std::collections::HashSet<u64>,
    f: &mut F,
) -> Result<u64>
where
    R: Read + Seek,
    F: FnMut(u16, Vec<ParsedSubheader<'_>>) -> Result<()>,
{
    let mut last_examined = 0u64;
    for page_index in 0..header.page_count {
        let page_type = load_page_type(reader, header, buffer, page_index)?;
        last_examined = page_index;
        let kind = classify_page(page_type);
        if matches!(
            kind,
            PageKind::Comp | PageKind::CompTable | PageKind::Unknown
        ) {
            continue;
        }
        if matches!(kind, PageKind::Data) {
            continue;
        }
        if !matches!(
            kind,
            PageKind::Meta | PageKind::Mix | PageKind::Meta2 | PageKind::Amd
        ) {
            continue;
        }

        visited.insert(page_index);
        if let Some(subheaders) = collect_subheaders(buffer, header, page_index, page_type)? {
            f(page_type, subheaders)?;
        }
    }

    Ok(last_examined)
}

fn scan_backward<R, F>(
    reader: &mut R,
    header: &SasHeader,
    buffer: &mut [u8],
    visited: &std::collections::HashSet<u64>,
    mut page_index: u64,
    f: &mut F,
) -> Result<()>
where
    R: Read + Seek,
    F: FnMut(u16, Vec<ParsedSubheader<'_>>) -> Result<()>,
{
    let mut seen_amd = false;
    while page_index > 0 {
        page_index -= 1;
        if visited.contains(&page_index) {
            continue;
        }
        let page_type = load_page_type(reader, header, buffer, page_index)?;
        let kind = classify_page(page_type);
        if matches!(
            kind,
            PageKind::Comp | PageKind::CompTable | PageKind::Unknown
        ) {
            continue;
        }
        if matches!(kind, PageKind::Data) {
            if seen_amd {
                break;
            }
            continue;
        }
        if !matches!(kind, PageKind::Amd | PageKind::Meta2) {
            continue;
        }
        seen_amd = true;

        if let Some(subheaders) = collect_subheaders(buffer, header, page_index, page_type)? {
            f(page_type, subheaders)?;
        }
    }

    Ok(())
}

fn read_page<R: Read + Seek>(
    reader: &mut R,
    header: &SasHeader,
    buffer: &mut [u8],
    page_index: u64,
) -> Result<()> {
    let offset = header.data_offset + page_index * u64::from(header.page_size);
    reader.seek(SeekFrom::Start(offset)).map_err(Error::from)?;
    reader.read_exact(buffer).map_err(Error::from)?;
    Ok(())
}

fn load_page_type<R: Read + Seek>(
    reader: &mut R,
    header: &SasHeader,
    buffer: &mut [u8],
    page_index: u64,
) -> Result<u16> {
    read_page(reader, header, buffer, page_index)?;
    Ok(read_u16(
        header.endianness,
        &buffer[(header.page_header_size as usize) - 8..],
    ))
}

fn collect_subheaders<'a>(
    buffer: &'a [u8],
    header: &'a SasHeader,
    page_index: u64,
    page_type: u16,
) -> Result<Option<Vec<ParsedSubheader<'a>>>> {
    let subheader_count = peek_subheader_count(buffer, header);
    let subheaders = parse_metadata_page(buffer, header, page_index, page_type, subheader_count)?;
    if subheaders.is_empty() {
        return Ok(None);
    }
    Ok(Some(subheaders))
}

fn parse_metadata_page<'a>(
    buffer: &'a [u8],
    header: &'a SasHeader,
    page_index: u64,
    page_type: u16,
    subheader_count: Option<u16>,
) -> Result<Vec<ParsedSubheader<'a>>> {
    match parse_subheaders(buffer, header) {
        Ok(subheaders) => Ok(subheaders),
        Err(Error::Corrupted {
            section: Section::Header,
            details,
        }) => {
            let detail_str = details.as_ref();
            if detail_str.contains("subheader pointer table exceeds page bounds")
                || detail_str.contains("subheader pointer exceeds page bounds")
                || detail_str.contains("subheader count exceeds page bounds")
            {
                log_warn(&format!(
                    "Skipping metadata page {page_index} (type=0x{page_type:04X}): {details} \
                     [page_size={}, page_header_size={}, pointer_size={}, subheaders={}]",
                    header.page_size,
                    header.page_header_size,
                    header.subheader_pointer_size,
                    subheader_count
                        .map_or_else(|| "unknown".to_string(), |count| count.to_string())
                ));
                return Ok(Vec::new());
            }
            Err(Error::Corrupted {
                section: Section::Header,
                details,
            })
        }
        Err(err) => Err(err),
    }
}

fn peek_subheader_count(page: &[u8], header: &SasHeader) -> Option<u16> {
    let subheader_count_pos = header.page_header_size as usize - 4;
    let count_bytes = page.get(subheader_count_pos..subheader_count_pos + 2)?;
    Some(read_u16(header.endianness, count_bytes))
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
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

#[must_use]
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
    let base = page_type & SAS_PAGE_TYPE_MASK;
    match base {
        SAS_PAGE_TYPE_META => PageKind::Meta,
        SAS_PAGE_TYPE_DATA => PageKind::Data,
        SAS_PAGE_TYPE_MIX => PageKind::Mix,
        SAS_PAGE_TYPE_AMD => PageKind::Amd,
        _ => PageKind::Unknown,
    }
}

fn parse_subheaders<'a>(page: &'a [u8], header: &SasHeader) -> Result<Vec<ParsedSubheader<'a>>> {
    let subheader_count_pos = header.page_header_size as usize - 4;
    let count_bytes = page
        .get(subheader_count_pos..subheader_count_pos + 2)
        .ok_or_else(|| Error::Corrupted {
            section: Section::Header,
            details: Cow::Owned(format!(
                "subheader count exceeds page bounds (pos={}, page_len={})",
                subheader_count_pos,
                page.len()
            )),
        })?;
    let subheader_count = read_u16(header.endianness, count_bytes);

    let mut subheaders = Vec::new();
    let pointer_size = header.subheader_pointer_size as usize;
    let max_subheaders = page.len().saturating_sub(header.page_header_size as usize) / pointer_size;
    let (subheader_count, truncated) = if usize::from(subheader_count) > max_subheaders {
        (u16::try_from(max_subheaders).unwrap_or(0), true)
    } else {
        (subheader_count, false)
    };
    if truncated {
        log_warn(&format!(
            "Clamping subheader count from {} to {} to fit page bounds [page_len={}, header_size={}, pointer_size={}]",
            subheader_count,
            max_subheaders,
            page.len(),
            header.page_header_size,
            pointer_size
        ));
    }
    let mut ptr_cursor = header.page_header_size as usize;
    for _ in 0..subheader_count {
        let pointer_end = ptr_cursor.saturating_add(pointer_size);
        let Some(pointer) = page.get(ptr_cursor..pointer_end) else {
            return Err(Error::Corrupted {
                section: Section::Header,
                details: Cow::Owned(format!(
                    "subheader pointer table exceeds page bounds (cursor={}, pointer_size={}, page_len={})",
                    ptr_cursor,
                    pointer_size,
                    page.len()
                )),
            });
        };
        ptr_cursor = pointer_end;

        let pointer_info = parse_pointer(pointer, header)?;
        if pointer_info.length == 0 {
            continue;
        }
        if pointer_info.compression != 0 {
            continue;
        }
        let Some(end) = pointer_info.offset.checked_add(pointer_info.length) else {
            return Err(Error::Corrupted {
                section: Section::Header,
                details: Cow::Owned(format!(
                    "subheader pointer exceeds page bounds (offset={}, length={}, page_len={}, overflow)",
                    pointer_info.offset,
                    pointer_info.length,
                    page.len()
                )),
            });
        };
        if end > page.len() {
            if pointer_info.is_compressed_data {
                continue;
            }
            return Err(Error::Corrupted {
                section: Section::Header,
                details: Cow::Owned(format!(
                    "subheader pointer exceeds page bounds (offset={}, length={}, page_len={}, compressed_data={})",
                    pointer_info.offset,
                    pointer_info.length,
                    page.len(),
                    pointer_info.is_compressed_data
                )),
            });
        }

        let data = &page[pointer_info.offset..end];
        if data.len() < header.subheader_signature_size {
            continue;
        }

        let mut signature = read_u32(header.endianness, &data[0..4]);
        if !matches!(header.endianness, crate::dataset::Endianness::Little)
            && header.uses_u64
            && signature == u32::MAX
            && data.len() >= 8
        {
            signature = read_u32(header.endianness, &data[4..8]);
        }

        subheaders.push(ParsedSubheader { signature, data });
    }

    Ok(subheaders)
}

struct PointerInfo {
    offset: usize,
    length: usize,
    compression: u8,
    is_compressed_data: bool,
}

fn parse_pointer(pointer: &[u8], header: &SasHeader) -> Result<PointerInfo> {
    if header.uses_u64 {
        if pointer.len() < 18 {
            return Err(Error::Corrupted {
                section: Section::Header,
                details: Cow::from("64-bit subheader pointer too short"),
            });
        }
        let offset =
            usize::try_from(read_u64(header.endianness, &pointer[0..8])).map_err(|_| {
                Error::Unsupported {
                    feature: Cow::from("metadata subheader offset exceeds platform pointer width"),
                }
            })?;
        let length =
            usize::try_from(read_u64(header.endianness, &pointer[8..16])).map_err(|_| {
                Error::Unsupported {
                    feature: Cow::from("metadata subheader length exceeds platform pointer width"),
                }
            })?;
        Ok(PointerInfo {
            offset,
            length,
            compression: pointer[16],
            is_compressed_data: pointer.get(17).copied().unwrap_or_default() != 0,
        })
    } else {
        if pointer.len() < 10 {
            return Err(Error::Corrupted {
                section: Section::Header,
                details: Cow::from("32-bit subheader pointer too short"),
            });
        }
        let offset =
            usize::try_from(read_u32(header.endianness, &pointer[0..4])).map_err(|_| {
                Error::Unsupported {
                    feature: Cow::from("metadata subheader offset exceeds platform pointer width"),
                }
            })?;
        let length =
            usize::try_from(read_u32(header.endianness, &pointer[4..8])).map_err(|_| {
                Error::Unsupported {
                    feature: Cow::from("metadata subheader length exceeds platform pointer width"),
                }
            })?;
        Ok(PointerInfo {
            offset,
            length,
            compression: pointer[8],
            is_compressed_data: pointer.get(9).copied().unwrap_or_default() != 0,
        })
    }
}

const fn is_meta_page(page_type: u16) -> bool {
    let base_type = page_type & SAS_PAGE_TYPE_MASK;
    base_type == SAS_PAGE_TYPE_META
        || base_type == SAS_PAGE_TYPE_MIX
        || base_type == SAS_PAGE_TYPE_AMD
        || (page_type & SAS_PAGE_TYPE_META2) == SAS_PAGE_TYPE_META2
        || page_type == 0
}
