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
use row_info::RowInfoRaw;
pub use text_store::{TextRef, TextStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataIoMode {
    Auto,
    FullPage,
    Streaming,
}

#[derive(Debug, Clone, Copy)]
pub struct MetadataReadOptions {
    pub io_mode: MetadataIoMode,
}

impl Default for MetadataReadOptions {
    fn default() -> Self {
        Self {
            io_mode: MetadataIoMode::Auto,
        }
    }
}

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
    parse_metadata_with_options(reader, MetadataReadOptions::default())
}

/// Parses dataset metadata with configurable IO behavior.
///
/// # Errors
///
/// Returns an error if the metadata pages cannot be decoded.
pub fn parse_metadata_with_options<R: Read + Seek>(
    reader: &mut R,
    options: MetadataReadOptions,
) -> Result<DatasetLayout> {
    let mut header = parse_header(reader)?;
    let encoding = resolve_encoding(header.metadata.file_encoding.as_deref());
    let mut builder = ColumnMetadataBuilder::new(encoding);

    let mut state = MetaState::default();
    collect_metadata(reader, &header, &mut builder, &mut state, options)?;

    let column_count = state.column_count.ok_or_else(|| Error::InvalidMetadata {
        details: "column count not found in SAS metadata".into(),
    })?;
    let row_info_raw = state.row_info.ok_or_else(|| Error::InvalidMetadata {
        details: "row size subheader missing from SAS metadata".into(),
    })?;
    let row_info = resolve_row_info(row_info_raw, builder.text_store())?;

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

fn collect_metadata<R: Read + Seek>(
    reader: &mut R,
    header: &SasHeader,
    builder: &mut ColumnMetadataBuilder,
    state: &mut MetaState,
    options: MetadataReadOptions,
) -> Result<()> {
    scan_pages_with_stop(reader, header, options, |page_type, subheaders| {
        if !is_meta_page(page_type) {
            return Ok(false);
        }
        for subheader in subheaders {
            match subheader.signature {
                SIG_COLUMN_TEXT => parse_column_text_subheader(
                    builder,
                    &subheader.data,
                    header.subheader_signature_size,
                    header.endianness,
                )?,
                SIG_COLUMN_NAME => parse_column_name_subheader(
                    builder,
                    &subheader.data,
                    header.subheader_signature_size,
                    header.endianness,
                    header.uses_u64,
                )?,
                SIG_COLUMN_ATTRS => parse_column_attrs_subheader(
                    builder,
                    &subheader.data,
                    header.subheader_signature_size,
                    header.endianness,
                    header.uses_u64,
                )?,
                SIG_COLUMN_FORMAT => parse_column_format_subheader(
                    builder,
                    &subheader.data,
                    header.endianness,
                    header.uses_u64,
                )?,
                SIG_COLUMN_LIST => parse_column_list_subheader(
                    builder,
                    &subheader.data,
                    header.subheader_signature_size,
                    header.endianness,
                    header.uses_u64,
                )?,
                SIG_COLUMN_SIZE => {
                    let column_count = parse_column_size_subheader(
                        builder,
                        &subheader.data,
                        header.endianness,
                        header.uses_u64,
                    )?;
                    state.column_count = Some(column_count);
                }
                SIG_ROW_SIZE => {
                    let row_info = parse_row_size_subheader(
                        &subheader.data,
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
        Ok(false)
    })
}

#[derive(Default)]
struct MetaState {
    column_count: Option<u32>,
    row_info: Option<RowInfoRaw>,
}

struct ParsedSubheader {
    signature: u32,
    data: Vec<u8>,
}

fn resolve_row_info(raw: RowInfoRaw, text_store: &TextStore) -> Result<RowInfo> {
    let file_label = text_store
        .resolve(raw.label_ref)?
        .map(|value| value.trim_end().to_string())
        .filter(|value| !value.is_empty());

    let compression = text_store
        .resolve(raw.compression_ref)?
        .map_or(Compression::None, |value| match value.trim() {
            "SASYZCR2" => Compression::Binary,
            "SASYZCRL" => Compression::Row,
            _ => Compression::None,
        });

    Ok(RowInfo {
        row_length: raw.row_length,
        total_rows: raw.total_rows,
        rows_per_page: raw.rows_per_page,
        compression,
        file_label,
    })
}

fn scan_pages_with_stop<R, F>(
    reader: &mut R,
    header: &SasHeader,
    options: MetadataReadOptions,
    mut f: F,
) -> Result<()>
where
    R: Read + Seek,
    F: FnMut(u16, Vec<ParsedSubheader>) -> Result<bool>,
{
    let mut header_buf = vec![0u8; header.page_header_size as usize];
    let mut visited = std::collections::HashSet::new();
    let mut last_examined = 0u64;

    for page_index in 0..header.page_count {
        load_page_header(reader, header, &mut header_buf, page_index)?;
        let page_type = page_type_from_header(header, &header_buf)?;
        let subheader_count = subheader_count_from_header(header, &header_buf)?;
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
        let subheaders = collect_subheaders(
            reader,
            header,
            page_index,
            page_type,
            subheader_count,
            options,
        )?;
        if !subheaders.is_empty() && f(page_type, subheaders)? {
            return Ok(());
        }
    }

    if last_examined + 1 < header.page_count {
        scan_backward_with_stop(
            reader,
            header,
            &mut header_buf,
            &visited,
            last_examined,
            options,
            &mut f,
        )?;
    }

    Ok(())
}

fn scan_backward_with_stop<R, F>(
    reader: &mut R,
    header: &SasHeader,
    header_buf: &mut [u8],
    visited: &std::collections::HashSet<u64>,
    mut page_index: u64,
    options: MetadataReadOptions,
    f: &mut F,
) -> Result<()>
where
    R: Read + Seek,
    F: FnMut(u16, Vec<ParsedSubheader>) -> Result<bool>,
{
    let mut seen_amd = false;
    while page_index > 0 {
        page_index -= 1;
        if visited.contains(&page_index) {
            continue;
        }
        load_page_header(reader, header, header_buf, page_index)?;
        let page_type = page_type_from_header(header, header_buf)?;
        let subheader_count = subheader_count_from_header(header, header_buf)?;
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

        let subheaders = collect_subheaders(
            reader,
            header,
            page_index,
            page_type,
            subheader_count,
            options,
        )?;
        if !subheaders.is_empty() && f(page_type, subheaders)? {
            return Ok(());
        }
    }

    Ok(())
}

fn load_page_header<R: Read + Seek>(
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

fn page_type_from_header(header: &SasHeader, buffer: &[u8]) -> Result<u16> {
    let page_header_size = header.page_header_size as usize;
    let start = page_header_size.saturating_sub(8);
    let end = start + 2;
    let slice = buffer.get(start..end).ok_or_else(|| Error::Corrupted {
        section: Section::Header,
        details: Cow::from("page header too short to read page type"),
    })?;
    Ok(read_u16(header.endianness, slice))
}

fn subheader_count_from_header(header: &SasHeader, buffer: &[u8]) -> Result<u16> {
    let page_header_size = header.page_header_size as usize;
    let start = page_header_size.saturating_sub(4);
    let end = start + 2;
    let slice = buffer.get(start..end).ok_or_else(|| Error::Corrupted {
        section: Section::Header,
        details: Cow::from("page header too short to read subheader count"),
    })?;
    Ok(read_u16(header.endianness, slice))
}

fn clamp_subheader_count(header: &SasHeader, subheader_count: u16) -> (u16, usize) {
    let pointer_size = header.subheader_pointer_size as usize;
    let max_subheaders =
        (header.page_size as usize).saturating_sub(header.page_header_size as usize) / pointer_size;
    if usize::from(subheader_count) > max_subheaders {
        (
            u16::try_from(max_subheaders).unwrap_or(0),
            max_subheaders,
        )
    } else {
        (subheader_count, max_subheaders)
    }
}

fn load_pointer_table<R: Read + Seek>(
    reader: &mut R,
    header: &SasHeader,
    page_offset: u64,
    page_index: u64,
    page_type: u16,
    subheader_count: u16,
) -> Result<(u16, Vec<u8>)> {
    if subheader_count == 0 {
        return Ok((0, Vec::new()));
    }

    let pointer_size = header.subheader_pointer_size as usize;
    let original_count = subheader_count;
    let (subheader_count, max_subheaders) = clamp_subheader_count(header, subheader_count);
    if usize::from(original_count) > max_subheaders {
        log_warn(&format!(
            "Clamping subheader count from {} to {} to fit page bounds [page_len={}, header_size={}, pointer_size={}]",
            original_count,
            subheader_count,
            header.page_size,
            header.page_header_size,
            pointer_size
        ));
    }

    let pointer_table_len = usize::from(subheader_count) * pointer_size;
    let pointer_table_end =
        (header.page_header_size as usize)
            .checked_add(pointer_table_len)
            .ok_or_else(|| Error::Corrupted {
                section: Section::Header,
                details: Cow::from("subheader pointer table exceeds page bounds"),
            })?;
    if pointer_table_end > header.page_size as usize {
        log_warn(&format!(
            "Skipping metadata page {page_index} (type=0x{page_type:04X}): subheader pointer table exceeds page bounds \
             [page_size={}, page_header_size={}, pointer_size={}, subheaders={}]",
            header.page_size,
            header.page_header_size,
            header.subheader_pointer_size,
            subheader_count
        ));
        return Ok((0, Vec::new()));
    }

    let mut pointer_table = vec![0u8; pointer_table_len];
    let pointer_offset = page_offset + u64::from(header.page_header_size);
    reader
        .seek(SeekFrom::Start(pointer_offset))
        .map_err(Error::from)?;
    reader.read_exact(&mut pointer_table).map_err(Error::from)?;

    Ok((subheader_count, pointer_table))
}

fn collect_subheaders<R: Read + Seek>(
    reader: &mut R,
    header: &SasHeader,
    page_index: u64,
    page_type: u16,
    subheader_count: u16,
    options: MetadataReadOptions,
) -> Result<Vec<ParsedSubheader>> {
    let page_offset = header.data_offset + page_index * u64::from(header.page_size);
    let (subheader_count, pointer_table) = load_pointer_table(
        reader,
        header,
        page_offset,
        page_index,
        page_type,
        subheader_count,
    )?;
    if subheader_count == 0 {
        return Ok(Vec::new());
    }
    let pointer_size = header.subheader_pointer_size as usize;
    let pointers = parse_pointer_table(&pointer_table, pointer_size, header)?;

    let total_payload: usize = pointers
        .iter()
        .filter(|info| info.length != 0 && info.compression == 0)
        .map(|info| info.length)
        .sum();

    let use_full_page = match options.io_mode {
        MetadataIoMode::FullPage => true,
        MetadataIoMode::Streaming => false,
        MetadataIoMode::Auto => {
            header.page_size <= 32 * 1024
                || total_payload >= (header.page_size as usize / 2)
        }
    };

    if use_full_page {
        return collect_subheaders_full_page(
            reader,
            header,
            page_index,
            page_type,
            page_offset,
            &pointers,
        );
    }

    let mut subheaders = Vec::new();
    for pointer_info in pointers {
        if pointer_info.length == 0 {
            continue;
        }
        if pointer_info.compression != 0 {
            continue;
        }
        let Some(end) = pointer_info.offset.checked_add(pointer_info.length) else {
            if pointer_info.is_compressed_data {
                continue;
            }
            return Err(Error::Corrupted {
                section: Section::Header,
                details: Cow::Owned(format!(
                    "subheader pointer exceeds page bounds (offset={}, length={}, page_len={}, overflow)",
                    pointer_info.offset, pointer_info.length, header.page_size
                )),
            });
        };
        if end > header.page_size as usize {
            if pointer_info.is_compressed_data {
                continue;
            }
            return Err(Error::Corrupted {
                section: Section::Header,
                details: Cow::Owned(format!(
                    "subheader pointer exceeds page bounds (offset={}, length={}, page_len={}, compressed_data={})",
                    pointer_info.offset,
                    pointer_info.length,
                    header.page_size,
                    pointer_info.is_compressed_data
                )),
            });
        }

        let mut data = vec![0u8; pointer_info.length];
        let data_offset = page_offset + pointer_info.offset as u64;
        reader
            .seek(SeekFrom::Start(data_offset))
            .map_err(Error::from)?;
        reader.read_exact(&mut data).map_err(Error::from)?;

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

fn parse_pointer_table(
    pointer_table: &[u8],
    pointer_size: usize,
    header: &SasHeader,
) -> Result<Vec<PointerInfo>> {
    let mut pointers = Vec::new();
    for chunk in pointer_table.chunks(pointer_size) {
        pointers.push(parse_pointer(chunk, header)?);
    }
    Ok(pointers)
}

fn collect_subheaders_full_page<R: Read + Seek>(
    reader: &mut R,
    header: &SasHeader,
    _page_index: u64,
    _page_type: u16,
    page_offset: u64,
    pointers: &[PointerInfo],
) -> Result<Vec<ParsedSubheader>> {
    let mut page = vec![0u8; header.page_size as usize];
    reader
        .seek(SeekFrom::Start(page_offset))
        .map_err(Error::from)?;
    reader.read_exact(&mut page).map_err(Error::from)?;

    let mut subheaders = Vec::new();
    for pointer_info in pointers {
        if pointer_info.length == 0 || pointer_info.compression != 0 {
            continue;
        }
        let Some(end) = pointer_info.offset.checked_add(pointer_info.length) else {
            if pointer_info.is_compressed_data {
                continue;
            }
            return Err(Error::Corrupted {
                section: Section::Header,
                details: Cow::Owned(format!(
                    "subheader pointer exceeds page bounds (offset={}, length={}, page_len={}, overflow)",
                    pointer_info.offset, pointer_info.length, header.page_size
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
                    header.page_size,
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

        subheaders.push(ParsedSubheader {
            signature,
            data: data.to_vec(),
        });
    }

    Ok(subheaders)
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
