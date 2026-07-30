use crate::{
    encoding::resolve_encoding,
    error::{Error, Result},
    internal::{
        HeaderInfo, LayoutPlan, PageKind, classify_page, parse_subheader_signature, read_u16,
        read_u32, read_u64,
    },
    metadata::{ColumnMeta, CompressionKind, DatasetMetadata, LogicalType},
    pages::walk_pages,
    types::RowLength,
};
use encoding_rs::{Encoding, UTF_8};
use std::{
    convert::TryFrom,
    io::{Read, Seek},
    ops::ControlFlow,
};

const SIG_ROW_SIZE: u32 = 0xF7F7_F7F7;
const SIG_COLUMN_SIZE: u32 = 0xF6F6_F6F6;
const SIG_COLUMN_TEXT: u32 = 0xFFFF_FFFD;
const SIG_COLUMN_NAME: u32 = 0xFFFF_FFFF;
const SIG_COLUMN_ATTRS: u32 = 0xFFFF_FFFC;
const SIG_COLUMN_FORMAT: u32 = 0xFFFF_FBFE;

/// Absolute ceiling on the column table, as a resource limit rather than a format rule.
///
/// sas7bdat declares no column ceiling: the 32,767-variable figure people quote is
/// a limit on a SAS *data set*, and SAS documents a far larger theoretical maximum,
/// so a format-derived bound would risk rejecting files SAS itself can write. What
/// this bound exists for is that the declared column count is four bytes of the
/// input, and every path that turns it into `Vec` length is an amplifier. At 2^20 it
/// sits ~32x above the data-set limit — high enough that no real file reaches it.
///
/// This is only the backstop. The effective limit is [`column_ceiling`], which is
/// derived per file and is far tighter for everything but a genuinely huge input.
const MAX_COLUMNS: usize = 1 << 20;

/// Smallest metadata footprint a described column can have: one 8-byte entry in a
/// column-name subheader. Attribute and format entries are larger, so the name
/// stride is what bounds how many columns a given number of bytes can describe.
const MIN_BYTES_PER_COLUMN: u64 = 8;

/// How many columns this file could possibly describe, from its own geometry.
///
/// A column is only real once a subheader describes it, and every such description
/// costs at least [`MIN_BYTES_PER_COLUMN`] inside a page. So the file's own size is
/// an honest upper bound on its column count — one that needs no format lore and no
/// guess about what is "reasonable". It turns the declared count from something we
/// allocate against into something we can immediately sanity-check: an 18 KB file
/// claiming 67 million columns is refutable from the geometry alone.
fn column_ceiling(header: &HeaderInfo) -> usize {
    let addressable = u64::from(header.page_size.0).saturating_mul(header.page_count);
    let ceiling = addressable / MIN_BYTES_PER_COLUMN;
    usize::try_from(ceiling)
        .unwrap_or(MAX_COLUMNS)
        .min(MAX_COLUMNS)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct TextRef {
    index: u16,
    offset: u16,
    length: u16,
}

impl TextRef {
    const EMPTY: Self = Self {
        index: 0,
        offset: 0,
        length: 0,
    };
}

#[derive(Debug, Clone)]
struct TextStore {
    blob_end_offsets: Vec<usize>,
    bytes: Vec<u8>,
    encoding: &'static Encoding,
}

impl Default for TextStore {
    fn default() -> Self {
        Self::new(UTF_8)
    }
}

impl TextStore {
    const fn new(encoding: &'static Encoding) -> Self {
        Self {
            blob_end_offsets: Vec::new(),
            bytes: Vec::new(),
            encoding,
        }
    }
    fn push_blob(&mut self, blob: &[u8]) {
        self.bytes.extend_from_slice(blob);
        self.blob_end_offsets.push(self.bytes.len());
    }

    fn blob(&self, index: usize) -> Option<&[u8]> {
        let end = *self.blob_end_offsets.get(index)?;
        let start = if index == 0 {
            0
        } else {
            *self.blob_end_offsets.get(index - 1)?
        };
        self.bytes.get(start..end)
    }

    fn resolve(&self, text_ref: TextRef) -> Option<String> {
        if text_ref.length == 0 {
            return None;
        }
        let blob = self.blob(text_ref.index as usize)?;
        let end = usize::from(text_ref.offset.saturating_add(text_ref.length));
        let start = usize::from(text_ref.offset);
        let bytes = blob.get(start..end)?;
        let (decoded, had_errors) = self.encoding.decode_without_bom_handling(bytes);
        let decoded = if had_errors {
            String::from_utf8_lossy(bytes)
        } else {
            decoded
        };
        // Trim the borrowed `&str` and allocate once, instead of materializing
        // the decoded string and then re-allocating the trimmed copy.
        let trimmed = decoded.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    }

    fn can_resolve(&self, text_ref: TextRef) -> bool {
        if text_ref.length == 0 {
            return true;
        }
        let Some(blob) = self.blob(text_ref.index as usize) else {
            return false;
        };
        let end = usize::from(text_ref.offset.saturating_add(text_ref.length));
        let start = usize::from(text_ref.offset);
        blob.get(start..end).is_some()
    }
}

#[derive(Debug, Clone)]
struct ColumnBuilder {
    index: usize,
    name_ref: TextRef,
    label_ref: TextRef,
    format_ref: TextRef,
    offset: u32,
    width: u32,
    type_code: u8,
}

impl Default for ColumnBuilder {
    fn default() -> Self {
        Self {
            index: 0,
            name_ref: TextRef::EMPTY,
            label_ref: TextRef::EMPTY,
            format_ref: TextRef::EMPTY,
            offset: 0,
            width: 0,
            type_code: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct RowInfoRaw {
    row_length: u32,
    total_rows: u64,
    rows_per_page: u64,
    compression_ref: TextRef,
    label_ref: TextRef,
}

#[derive(Debug, Default)]
struct MetadataState {
    text_store: TextStore,
    columns: Vec<ColumnBuilder>,
    column_count: Option<u32>,
    row_info: Option<RowInfoRaw>,
    names_seen: usize,
    attrs_seen: usize,
    formats_seen: usize,
    /// Upper bound on `columns`, from the file's geometry. See [`column_ceiling`].
    column_ceiling: usize,
}

impl MetadataState {
    /// Reserve capacity for a column count the file *claims*.
    ///
    /// Capacity only — never length. The claim is four bytes of untrusted input, so
    /// letting it set `columns.len()` is what allowed an 18 KB file to ask for 2.5 GiB.
    /// Reserving instead keeps the pre-sizing win for well-formed wide tables while
    /// leaving the table's length a function of columns actually described.
    ///
    /// Clamped to the geometric ceiling, not to `MAX_COLUMNS`: a claim the file is too
    /// small to back is not worth allocating for, so the same 18 KB file reserves for
    /// ~2.3k columns rather than the 1M backstop.
    fn reserve_columns(&mut self, claimed: u32) -> Result<()> {
        let want = (claimed as usize).min(self.column_ceiling);
        let extra = want.saturating_sub(self.columns.len());
        self.columns.try_reserve(extra).map_err(|_| {
            metadata_error("cannot allocate a column table for the declared column count")
        })
    }

    /// Grow the column table to cover `index`, bounded and fallibly.
    ///
    /// `try_reserve` rather than a bare `resize_with`: the infallible path routes
    /// allocation failure through `handle_alloc_error`, which aborts the process.
    /// This crate is loaded into Python and R interpreters, where an abort kills the
    /// host with no recoverable error, so allocation has to be an `Err` instead.
    fn ensure_column(&mut self, index: usize) -> Result<&mut ColumnBuilder> {
        if index >= self.column_ceiling {
            return Err(metadata_error(
                "SAS metadata describes more columns than the file has room to describe",
            ));
        }
        if index >= self.columns.len() {
            let extra = index + 1 - self.columns.len();
            self.columns
                .try_reserve(extra)
                .map_err(|_| metadata_error("cannot allocate a column table"))?;
            self.columns.resize_with(index + 1, ColumnBuilder::default);
        }
        let column = &mut self.columns[index];
        column.index = index;
        Ok(column)
    }

    fn is_complete(&self) -> bool {
        let Some(row_info) = self.row_info else {
            return false;
        };
        let Some(column_count_u32) = self.column_count else {
            return false;
        };
        let Ok(column_count) = usize::try_from(column_count_u32) else {
            return false;
        };
        if self.names_seen < column_count
            || self.attrs_seen < column_count
            || self.formats_seen < column_count
        {
            return false;
        }
        if !self.text_store.can_resolve(row_info.compression_ref)
            || !self.text_store.can_resolve(row_info.label_ref)
        {
            return false;
        }
        self.columns.iter().take(column_count).all(|column| {
            self.text_store.can_resolve(column.name_ref)
                && self.text_store.can_resolve(column.label_ref)
                && self.text_store.can_resolve(column.format_ref)
        })
    }
}

pub fn parse_layout<R: Read + Seek>(
    reader: &mut R,
    header: HeaderInfo,
    mut metadata: DatasetMetadata,
) -> Result<(LayoutPlan, DatasetMetadata)> {
    let encoding = resolve_encoding(metadata.encoding.as_deref());
    let mut state = MetadataState {
        text_store: TextStore::new(encoding),
        column_ceiling: column_ceiling(&header),
        ..MetadataState::default()
    };

    walk_pages(reader, &header, |_page_index, page| {
        let page_type = read_u16(
            header.endianness,
            get_range(
                page,
                header.page_header_size as usize - 8,
                header.page_header_size as usize - 6,
                "page type",
            )?,
        );

        match classify_page(page_type) {
            PageKind::Meta | PageKind::Mix | PageKind::Meta2 | PageKind::Amd => {
                parse_page_subheaders(&header, page, &mut state)?;
            }
            PageKind::Data | PageKind::Comp | PageKind::CompTable | PageKind::Unknown => {}
        }
        if state.is_complete() {
            Ok(ControlFlow::Break(()))
        } else {
            Ok(ControlFlow::Continue(()))
        }
    })?;

    let row_info = state
        .row_info
        .ok_or_else(|| metadata_error("row size subheader missing from SAS metadata"))?;

    let column_count = state
        .column_count
        .unwrap_or_else(|| u32::try_from(state.columns.len()).unwrap_or(u32::MAX));

    // Reconcile the declared count against the columns actually described.
    //
    // Overshoot is normal and stays a truncate: a column-name subheader is sized in
    // whole entries, so the last one routinely has slack past the final real column.
    //
    // A shortfall means the metadata never described every column it declared. That
    // used to be papered over — the count materialised the table up front, so missing
    // columns silently became zero-width entries named `COL{n}`, and the caller could
    // not tell an invented column from a real one. Reject instead: handing back
    // fabricated schema is worse than refusing the file.
    let described = state.columns.len();
    if described < column_count as usize {
        return Err(metadata_error(format!(
            "SAS metadata declares {column_count} columns but describes only {described}"
        )));
    }
    state.columns.truncate(column_count as usize);

    let compression =
        state
            .text_store
            .resolve(row_info.compression_ref)
            .map_or(CompressionKind::None, |value| match value.trim() {
                "SASYZCR2" => CompressionKind::Binary,
                "SASYZCRL" => CompressionKind::Row,
                _ => CompressionKind::None,
            });

    metadata.row_count = row_info.total_rows;
    metadata.row_len = row_info.row_length;
    metadata.compression = compression;
    metadata.file_label = state.text_store.resolve(row_info.label_ref);

    let columns = state
        .columns
        .into_iter()
        .map(|column| {
            let name = state
                .text_store
                .resolve(column.name_ref)
                .unwrap_or_else(|| format!("COL{}", column.index + 1));
            let format = state.text_store.resolve(column.format_ref);
            let label = state.text_store.resolve(column.label_ref);
            let logical_type = infer_logical_type(column.type_code, format.as_deref());

            ColumnMeta {
                index: column.index,
                name,
                logical_type,
                physical_width: column.width,
                offset: column.offset,
                label,
                format,
            }
        })
        .collect();

    Ok((
        LayoutPlan {
            columns,
            header,
            row_len: RowLength::from(row_info.row_length),
            total_rows: row_info.total_rows,
            compression,
            rows_per_page: row_info.rows_per_page,
        },
        metadata,
    ))
}

fn parse_page_subheaders(
    header: &HeaderInfo,
    page: &[u8],
    state: &mut MetadataState,
) -> Result<()> {
    let subheader_count_raw = read_u16(
        header.endianness,
        get_range(
            page,
            header.page_header_size as usize - 4,
            header.page_header_size as usize - 2,
            "subheader count",
        )?,
    );

    let pointer_size = header.subheader_pointer_size as usize;
    let max_subheaders = page.len().saturating_sub(header.page_header_size as usize) / pointer_size;
    let subheader_count = usize::from(subheader_count_raw).min(max_subheaders);

    let mut cursor = header.page_header_size as usize;
    for _ in 0..subheader_count {
        let pointer = get_range(page, cursor, cursor + pointer_size, "subheader pointer")?;
        cursor += pointer_size;

        let info = parse_pointer(pointer, header)?;
        if info.length == 0 || info.compression != 0 {
            continue;
        }

        let end = info
            .offset
            .checked_add(info.length)
            .ok_or_else(|| metadata_error("subheader pointer overflow"))?;
        if end > page.len() {
            continue;
        }
        let data = &page[info.offset..end];
        let Some(signature) = parse_subheader_signature(header, data) else {
            continue;
        };
        match signature {
            SIG_COLUMN_TEXT => state
                .text_store
                .push_blob(&data[header.subheader_signature_size..]),
            SIG_COLUMN_NAME => parse_column_name_subheader(state, data, header)?,
            SIG_COLUMN_ATTRS => parse_column_attrs_subheader(state, data, header)?,
            SIG_COLUMN_FORMAT => parse_column_format_subheader(state, data, header)?,
            SIG_COLUMN_SIZE => parse_column_size_subheader(state, data, header)?,
            SIG_ROW_SIZE => state.row_info = Some(parse_row_size_subheader(data, header)?),
            _ => {}
        }
    }

    Ok(())
}

fn parse_column_name_subheader(
    state: &mut MetadataState,
    bytes: &[u8],
    header: &HeaderInfo,
) -> Result<()> {
    let chunk_width = 8usize;
    let entries = subheader_entries(bytes.len(), header.uses_u64_pointers, chunk_width);
    let mut cursor = header.subheader_signature_size + 8;
    let start_index = state.names_seen;

    for offset in 0..entries {
        let text_ref = parse_text_ref(
            header.endianness,
            get_range(bytes, cursor, cursor + 6, "column name text ref")?,
        );
        let column = state.ensure_column(start_index + offset)?;
        column.name_ref = text_ref;
        cursor += chunk_width;
    }

    state.names_seen += entries;
    Ok(())
}

fn parse_column_attrs_subheader(
    state: &mut MetadataState,
    bytes: &[u8],
    header: &HeaderInfo,
) -> Result<()> {
    let row_size = if header.uses_u64_pointers { 16 } else { 12 };
    let entries = subheader_entries(bytes.len(), header.uses_u64_pointers, row_size);
    let mut cursor = header.subheader_signature_size + 8;
    let start_index = state.attrs_seen;

    for offset in 0..entries {
        let index = start_index + offset;
        let column = state.ensure_column(index)?;
        if header.uses_u64_pointers {
            column.offset = u32::try_from(read_u64(
                header.endianness,
                get_range(bytes, cursor, cursor + 8, "column offset")?,
            ))
            .map_err(|_| metadata_error("column offset exceeds u32"))?;
            column.width = read_u32(
                header.endianness,
                get_range(bytes, cursor + 8, cursor + 12, "column width")?,
            );
            column.type_code = *bytes
                .get(cursor + 14)
                .ok_or_else(|| metadata_error("column attrs type code missing"))?;
            cursor += 16;
        } else {
            column.offset = read_u32(
                header.endianness,
                get_range(bytes, cursor, cursor + 4, "column offset")?,
            );
            column.width = read_u32(
                header.endianness,
                get_range(bytes, cursor + 4, cursor + 8, "column width")?,
            );
            column.type_code = *bytes
                .get(cursor + 10)
                .ok_or_else(|| metadata_error("column attrs type code missing"))?;
            cursor += 12;
        }
    }

    state.attrs_seen += entries;
    Ok(())
}

fn parse_column_format_subheader(
    state: &mut MetadataState,
    bytes: &[u8],
    header: &HeaderInfo,
) -> Result<()> {
    let index = state.formats_seen;
    let column = state.ensure_column(index)?;
    if header.uses_u64_pointers {
        column.format_ref = parse_text_ref(
            header.endianness,
            get_range(bytes, 46, 52, "column format ref")?,
        );
        column.label_ref = parse_text_ref(
            header.endianness,
            get_range(bytes, 52, 58, "column label ref")?,
        );
    } else {
        column.format_ref = parse_text_ref(
            header.endianness,
            get_range(bytes, 34, 40, "column format ref")?,
        );
        column.label_ref = parse_text_ref(
            header.endianness,
            get_range(bytes, 40, 46, "column label ref")?,
        );
    }
    state.formats_seen += 1;
    Ok(())
}

fn parse_column_size_subheader(
    state: &mut MetadataState,
    bytes: &[u8],
    header: &HeaderInfo,
) -> Result<()> {
    let raw_count = if header.uses_u64_pointers {
        read_u64(header.endianness, get_range(bytes, 8, 16, "column count")?)
    } else {
        u64::from(read_u32(
            header.endianness,
            get_range(bytes, 4, 8, "column count")?,
        ))
    };
    let count = u32::try_from(raw_count).map_err(|_| metadata_error("column count exceeds u32"))?;

    // Refute an impossible claim here rather than letting it fail late. The file cannot
    // describe more columns than it has bytes for, so a count past the geometric ceiling
    // is corrupt on its face — and saying so at the source gives a better error than the
    // declared-vs-described mismatch it would otherwise surface as.
    if count as usize > state.column_ceiling {
        return Err(metadata_error(format!(
            "SAS metadata declares {count} columns, more than this {} byte file could describe",
            u64::from(header.page_size.0).saturating_mul(header.page_count)
        )));
    }

    state.column_count = Some(count);
    // Record the claim and reserve against it, but do not materialise it: the column
    // table's length is set by the name/attrs/format subheaders that actually
    // describe columns. `parse_layout` reconciles the two afterwards.
    state.reserve_columns(count)
}

fn parse_row_size_subheader(bytes: &[u8], header: &HeaderInfo) -> Result<RowInfoRaw> {
    let row_length = if header.uses_u64_pointers {
        u32::try_from(read_u64(
            header.endianness,
            get_range(bytes, 40, 48, "row length")?,
        ))
        .map_err(|_| metadata_error("row length exceeds u32"))?
    } else {
        read_u32(header.endianness, get_range(bytes, 20, 24, "row length")?)
    };

    let total_rows = if header.uses_u64_pointers {
        read_u64(header.endianness, get_range(bytes, 48, 56, "total rows")?)
    } else {
        u64::from(read_u32(
            header.endianness,
            get_range(bytes, 24, 28, "total rows")?,
        ))
    };

    let rows_per_page = if header.uses_u64_pointers {
        read_u64(
            header.endianness,
            get_range(bytes, 120, 128, "rows per page")?,
        )
    } else {
        u64::from(read_u32(
            header.endianness,
            get_range(bytes, 60, 64, "rows per page")?,
        ))
    };

    let label_ref_offset = bytes
        .len()
        .checked_sub(130)
        .ok_or_else(|| metadata_error("row size subheader missing label ref"))?;
    let compression_ref_offset = bytes
        .len()
        .checked_sub(118)
        .ok_or_else(|| metadata_error("row size subheader missing compression ref"))?;

    Ok(RowInfoRaw {
        row_length,
        total_rows,
        rows_per_page,
        compression_ref: parse_text_ref(
            header.endianness,
            get_range(
                bytes,
                compression_ref_offset,
                compression_ref_offset + 6,
                "compression ref",
            )?,
        ),
        label_ref: parse_text_ref(
            header.endianness,
            get_range(bytes, label_ref_offset, label_ref_offset + 6, "label ref")?,
        ),
    })
}

#[derive(Debug, Clone, Copy)]
struct PointerInfo {
    offset: usize,
    length: usize,
    compression: u8,
}

fn parse_pointer(pointer: &[u8], header: &HeaderInfo) -> Result<PointerInfo> {
    if header.uses_u64_pointers {
        Ok(PointerInfo {
            offset: usize::try_from(read_u64(
                header.endianness,
                get_range(pointer, 0, 8, "pointer offset")?,
            ))
            .map_err(|_| metadata_error("pointer offset exceeds usize"))?,
            length: usize::try_from(read_u64(
                header.endianness,
                get_range(pointer, 8, 16, "pointer length")?,
            ))
            .map_err(|_| metadata_error("pointer length exceeds usize"))?,
            compression: *pointer
                .get(16)
                .ok_or_else(|| metadata_error("pointer compression byte missing"))?,
        })
    } else {
        Ok(PointerInfo {
            offset: usize::try_from(read_u32(
                header.endianness,
                get_range(pointer, 0, 4, "pointer offset")?,
            ))
            .map_err(|_| metadata_error("pointer offset exceeds usize"))?,
            length: usize::try_from(read_u32(
                header.endianness,
                get_range(pointer, 4, 8, "pointer length")?,
            ))
            .map_err(|_| metadata_error("pointer length exceeds usize"))?,
            compression: *pointer
                .get(8)
                .ok_or_else(|| metadata_error("pointer compression byte missing"))?,
        })
    }
}

fn parse_text_ref(endianness: crate::metadata::Endianness, bytes: &[u8]) -> TextRef {
    TextRef {
        index: read_u16(endianness, &bytes[0..2]),
        offset: read_u16(endianness, &bytes[2..4]),
        length: read_u16(endianness, &bytes[4..6]),
    }
}

const fn subheader_entries(len: usize, uses_u64: bool, chunk_width: usize) -> usize {
    let base = if uses_u64 { 28 } else { 20 };
    len.saturating_sub(base) / chunk_width
}

fn infer_logical_type(type_code: u8, format_name: Option<&str>) -> LogicalType {
    match type_code {
        0x02 => LogicalType::String,
        0x01 => {
            let Some(format_name) = format_name else {
                return LogicalType::Float;
            };
            let cleaned = format_name.trim().trim_matches('.').to_ascii_uppercase();
            if cleaned.contains("DATETIME")
                || cleaned.starts_with("DT")
                || cleaned.ends_with("DT")
                || cleaned.starts_with("E8601DT")
                || cleaned.starts_with("B8601DT")
            {
                LogicalType::DateTime
            } else if cleaned.contains("TIME")
                || cleaned.ends_with("TM")
                || cleaned.starts_with("E8601TM")
                || cleaned.starts_with("HHMM")
            {
                LogicalType::Time
            } else if cleaned.contains("DATE")
                || cleaned.contains("YY")
                || cleaned.contains("MON")
                || cleaned.contains("WEEK")
                || cleaned.contains("YEAR")
                || cleaned.contains("MINGUO")
                || cleaned.ends_with("DA")
                || cleaned.starts_with("E8601DA")
                || cleaned.starts_with("B8601DA")
            {
                LogicalType::Date
            } else {
                LogicalType::Float
            }
        }
        _ => LogicalType::Bytes,
    }
}

fn get_range<'a>(bytes: &'a [u8], start: usize, end: usize, what: &str) -> Result<&'a [u8]> {
    bytes
        .get(start..end)
        .ok_or_else(|| metadata_error(format!("{what} exceeds bounds")))
}

fn metadata_error(message: impl Into<String>) -> Error {
    Error::metadata_corruption(message)
}
