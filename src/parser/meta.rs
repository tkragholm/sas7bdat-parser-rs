use std::borrow::Cow;
use std::convert::TryFrom;
use std::io::{Read, Seek, SeekFrom};

use crate::error::{Error, Result, Section};
use crate::metadata::{Compression, Variable};
use crate::parser::column::{
    ColumnInfo, ColumnMetadataBuilder, RowInfo, TextStore, parse_column_attrs_subheader,
    parse_column_format_subheader, parse_column_list_subheader, parse_column_name_subheader,
    parse_column_size_subheader, parse_column_text_subheader, parse_row_size_subheader,
};
use super::byteorder::{read_u16, read_u32, read_u64};
use crate::parser::header::{SasHeader, parse_header};

#[derive(Debug)]
pub struct ParsedMetadata {
    pub header: SasHeader,
    pub text_store: TextStore,
    pub columns: Vec<ColumnInfo>,
    pub row_info: RowInfo,
    pub column_list: Option<Vec<i16>>,
}

impl ParsedMetadata {
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
const SAS_PAGE_TYPE_MIX: u16 = 0x0200;
const SAS_PAGE_TYPE_META2: u16 = 0x4000;
const SAS_PAGE_TYPE_AMD: u16 = 0x0400;
const SAS_PAGE_TYPE_COMP: u16 = 0x9000;

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
pub fn parse_metadata<R: Read + Seek>(reader: &mut R) -> Result<ParsedMetadata> {
    let mut header = parse_header(reader)?;
    let mut builder = ColumnMetadataBuilder::new();

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

    Ok(ParsedMetadata {
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
    for page_index in 0..header.page_count {
        let offset = header.data_offset + page_index * u64::from(header.page_size);
        reader.seek(SeekFrom::Start(offset)).map_err(Error::from)?;
        reader.read_exact(&mut buffer).map_err(Error::from)?;

        let page_type = read_u16(
            header.endianness,
            &buffer[(header.page_header_size as usize) - 8..],
        );
        if (page_type & SAS_PAGE_TYPE_COMP) == SAS_PAGE_TYPE_COMP || !is_meta_page(page_type) {
            continue;
        }

        let subheaders = parse_subheaders(&buffer, header)?;
        f(page_type, subheaders)?;
    }
    Ok(())
}

fn parse_subheaders<'a>(page: &'a [u8], header: &SasHeader) -> Result<Vec<ParsedSubheader<'a>>> {
    let subheader_count_pos = header.page_header_size as usize - 4;
    let subheader_count = read_u16(header.endianness, &page[subheader_count_pos..]);

    let mut subheaders = Vec::new();
    let pointer_size = header.subheader_pointer_size as usize;
    let mut ptr_cursor = header.page_header_size as usize;
    for _ in 0..subheader_count {
        let pointer = &page[ptr_cursor..ptr_cursor + pointer_size];
        ptr_cursor += pointer_size;

        let pointer_info = parse_pointer(pointer, header)?;
        if pointer_info.length == 0 {
            continue;
        }
        if pointer_info.compression != 0 {
            continue;
        }
        let end = pointer_info.offset + pointer_info.length;
        if end > page.len() {
            if pointer_info.is_compressed_data {
                continue;
            }
            return Err(Error::Corrupted {
                section: Section::Header,
                details: Cow::from("subheader pointer exceeds page bounds"),
            });
        }

        let data = &page[pointer_info.offset..end];
        if data.len() < header.subheader_signature_size {
            continue;
        }

        let mut signature = read_u32(header.endianness, &data[0..4]);
        if !matches!(header.endianness, crate::metadata::Endianness::Little)
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
