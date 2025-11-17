use std::borrow::Cow;
use std::convert::TryFrom;

use super::byteorder::{read_i16, read_u16, read_u32, read_u64};
use crate::error::{Error, Result, Section};
use crate::metadata::{
    Alignment, Compression, Endianness, Format, Measure, MissingValuePolicy, Variable, VariableKind,
};

/// Reference into the text blob storage used by SAS column metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TextRef {
    pub index: u16,
    pub offset: u16,
    pub length: u16,
}

impl TextRef {
    pub const EMPTY: Self = Self {
        index: 0,
        offset: 0,
        length: 0,
    };

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.length == 0
    }
}

/// Stores decoded text blobs referenced by column metadata subheaders.
#[derive(Debug, Default)]
pub struct TextStore {
    blobs: Vec<Vec<u8>>,
}

impl TextStore {
    #[must_use]
    pub const fn new() -> Self {
        Self { blobs: Vec::new() }
    }

    /// Adds a text blob extracted from a column text subheader.
    pub fn push_blob(&mut self, blob: &[u8]) {
        self.blobs.push(blob.to_vec());
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.blobs.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.blobs.is_empty()
    }

    #[must_use]
    pub fn blob(&self, index: usize) -> Option<&[u8]> {
        self.blobs.get(index).map(Vec::as_slice)
    }

    /// Resolves a `TextRef` into a UTF-8 string if possible.
    ///
    /// # Errors
    ///
    /// Returns an error if the reference points outside the stored blobs or
    /// the bytes cannot be decoded as UTF-8.
    pub fn resolve(&self, text_ref: TextRef) -> Result<Option<Cow<'_, str>>> {
        if text_ref.length == 0 {
            return Ok(None);
        }
        let blob = self
            .blobs
            .get(text_ref.index as usize)
            .ok_or_else(|| Error::Corrupted {
                section: Section::Column {
                    index: u32::from(text_ref.index),
                },
                details: Cow::from("text reference points outside blob storage"),
            })?;
        let end = text_ref
            .offset
            .checked_add(text_ref.length)
            .ok_or_else(|| Error::Corrupted {
                section: Section::Column {
                    index: u32::from(text_ref.index),
                },
                details: Cow::from("text reference overflow"),
            })? as usize;
        let offset = text_ref.offset as usize;
        if end > blob.len() {
            return Err(Error::Corrupted {
                section: Section::Column {
                    index: u32::from(text_ref.index),
                },
                details: Cow::from("text reference exceeds blob length"),
            });
        }
        let bytes = &blob[offset..end];
        let decoded = String::from_utf8(bytes.to_vec()).map_err(|_| Error::Encoding {
            encoding: Cow::from("unknown"),
            details: Cow::from("failed to decode column text blob as UTF-8"),
        })?;
        Ok(Some(Cow::Owned(decoded)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnKind {
    Numeric(NumericKind),
    Character,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericKind {
    Double,
    Date,
    DateTime,
    Time,
}

const COLUMN_LIST_HEADER_LEN: usize = 30;

impl ColumnKind {
    #[must_use]
    pub const fn from_type_code(code: u8) -> Option<Self> {
        match code {
            0x01 => Some(Self::Numeric(NumericKind::Double)),
            0x02 => Some(Self::Character),
            _ => None,
        }
    }
}

/// Tracks column offsets and widths for row parsing.
#[derive(Debug, Clone, Copy)]
pub struct ColumnOffsets {
    pub offset: u64,
    pub width: u32,
}

/// Intermediate column information aggregated from the SAS meta pages.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub index: u32,
    pub offsets: ColumnOffsets,
    pub kind: ColumnKind,
    pub format_width: Option<u16>,
    pub format_decimals: Option<u16>,
    pub name_ref: TextRef,
    pub label_ref: TextRef,
    pub format_ref: TextRef,
    pub measure: Measure,
    pub alignment: Alignment,
}

impl ColumnInfo {
    /// Populates the provided [`Variable`] with the decoded column metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the associated text blobs cannot be resolved.
    pub fn apply_to_variable(&self, text_store: &TextStore, variable: &mut Variable) -> Result<()> {
        variable.index = self.index;
        variable.kind = match self.kind {
            ColumnKind::Numeric(_) => VariableKind::Numeric,
            ColumnKind::Character => VariableKind::Character,
        };
        variable.storage_width =
            usize::try_from(self.offsets.width).map_err(|_| Error::Unsupported {
                feature: Cow::from("column width exceeds platform pointer width"),
            })?;
        variable.missing = MissingValuePolicy::default();
        if matches!(variable.kind, VariableKind::Numeric) {
            variable.missing.system_missing = true;
        }
        variable.measure = self.measure;
        variable.alignment = self.alignment;

        if let Some(name) = text_store.resolve(self.name_ref)? {
            variable.name = name.into_owned();
        }
        if let Some(label) = text_store.resolve(self.label_ref)? {
            variable.label = Some(label.into_owned());
        }
        if let Some(fmt_name) = text_store.resolve(self.format_ref)? {
            let format = Format {
                name: fmt_name.into_owned(),
                width: self.format_width,
                decimals: self.format_decimals,
            };
            variable.format = Some(format);
        }
        Ok(())
    }
}

/// Builder that collects column metadata by interpreting meta-page subheaders.
#[derive(Debug)]
pub struct ColumnMetadataBuilder {
    text_store: TextStore,
    columns: Vec<ColumnInfo>,
    column_count: Option<u32>,
    names_seen: usize,
    attrs_seen: usize,
    formats_seen: usize,
    max_width: u32,
    column_list: Option<Vec<i16>>,
}

impl Default for ColumnMetadataBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ColumnMetadataBuilder {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            text_store: TextStore::new(),
            columns: Vec::new(),
            column_count: None,
            names_seen: 0,
            attrs_seen: 0,
            formats_seen: 0,
            max_width: 0,
            column_list: None,
        }
    }

    #[must_use]
    pub const fn text_store(&self) -> &TextStore {
        &self.text_store
    }

    pub const fn text_store_mut(&mut self) -> &mut TextStore {
        &mut self.text_store
    }

    #[must_use]
    pub const fn column_count(&self) -> Option<u32> {
        self.column_count
    }

    pub const fn set_column_count(&mut self, count: u32) {
        self.column_count = Some(count);
    }

    #[must_use]
    pub const fn max_width(&self) -> u32 {
        self.max_width
    }

    pub fn ensure_column(&mut self, index: u32) -> &mut ColumnInfo {
        let len = self.columns.len();
        if index as usize >= len {
            self.columns.resize_with(index as usize + 1, || ColumnInfo {
                index: 0,
                offsets: ColumnOffsets {
                    offset: 0,
                    width: 0,
                },
                kind: ColumnKind::Numeric(NumericKind::Double),
                format_width: None,
                format_decimals: None,
                name_ref: TextRef::EMPTY,
                label_ref: TextRef::EMPTY,
                format_ref: TextRef::EMPTY,
                measure: Measure::Unknown,
                alignment: Alignment::Unknown,
            });
        }
        let column = &mut self.columns[index as usize];
        column.index = index;
        column
    }

    /// Returns a mutable reference to the column at `index`, creating it if necessary.
    ///
    /// # Panics
    ///
    /// Panics if the column could not be created.
    pub fn column_mut(&mut self, index: u32) -> &mut ColumnInfo {
        let _ = self.ensure_column(index);
        self.columns
            .get_mut(index as usize)
            .expect("column ensured but not present")
    }

    pub const fn note_names_processed(&mut self, count: usize) {
        self.names_seen += count;
    }

    #[must_use]
    pub const fn names_seen(&self) -> usize {
        self.names_seen
    }

    pub const fn note_attrs_processed(&mut self, count: usize) {
        self.attrs_seen += count;
    }

    #[must_use]
    pub const fn attrs_seen(&self) -> usize {
        self.attrs_seen
    }

    pub const fn note_formats_processed(&mut self) {
        self.formats_seen += 1;
    }

    #[must_use]
    pub const fn formats_seen(&self) -> usize {
        self.formats_seen
    }

    pub const fn update_max_width(&mut self, width: u32) {
        if width > self.max_width {
            self.max_width = width;
        }
    }

    pub fn append_column_list(&mut self, values: Vec<i16>) {
        let entry = self.column_list.get_or_insert_with(Vec::new);
        if entry.is_empty() {
            entry.extend(values);
        } else if entry.len() < values.len() {
            entry.extend(values.into_iter().skip(entry.len()));
        }
    }

    #[must_use]
    pub fn column_list(&self) -> Option<&[i16]> {
        self.column_list.as_deref()
    }

    #[must_use]
    pub fn finalize(self) -> (TextStore, Vec<ColumnInfo>, Option<Vec<i16>>) {
        let mut columns = self.columns;
        let mut inferred_formats: Vec<Option<String>> = Vec::with_capacity(columns.len());
        for column in &columns {
            inferred_formats.push(
                self.text_store
                    .resolve(column.format_ref)
                    .ok()
                    .and_then(|opt| opt.map(Cow::into_owned)),
            );
        }

        for (column, format_name) in columns.iter_mut().zip(inferred_formats.into_iter()) {
            if let (ColumnKind::Numeric(kind), Some(format_name)) = (&mut column.kind, format_name)
                && let Some(inferred) = infer_numeric_kind(&format_name)
            {
                *kind = inferred;
            }
        }

        (self.text_store, columns, self.column_list)
    }
}

pub fn expected_remainder(len: usize, signature_len: usize) -> Option<u16> {
    let base = 4 + 2 * signature_len;
    if len < base {
        return None;
    }
    let remainder = len - base;
    u16::try_from(remainder).ok()
}

#[inline]
const fn subheader_entries(len: usize, uses_u64: bool, chunk_width: usize) -> usize {
    let base = if uses_u64 { 28 } else { 20 };
    len.saturating_sub(base) / chunk_width
}

pub fn parse_text_ref(endian: Endianness, bytes: &[u8]) -> TextRef {
    TextRef {
        index: read_u16(endian, &bytes[0..2]),
        offset: read_u16(endian, &bytes[2..4]),
        length: read_u16(endian, &bytes[4..6]),
    }
}

pub fn parse_column_text_subheader(
    builder: &mut ColumnMetadataBuilder,
    bytes: &[u8],
    signature_len: usize,
    endian: Endianness,
) -> Result<()> {
    if bytes.len() < signature_len + 2 {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("column text subheader too short"),
        });
    }

    let remainder = read_u16(endian, &bytes[signature_len..signature_len + 2]);
    let expected =
        expected_remainder(bytes.len(), signature_len).ok_or_else(|| Error::Corrupted {
            section: Section::Header,
            details: Cow::from("column text subheader length invalid"),
        })?;
    if remainder != expected {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("column text remainder mismatch"),
        });
    }

    let blob = &bytes[signature_len..];
    builder.text_store_mut().push_blob(blob);
    Ok(())
}

#[derive(Clone, Copy)]
struct SubheaderValidationMessages {
    too_short: &'static str,
    length_invalid: &'static str,
    remainder_mismatch: &'static str,
}

enum ColumnSubheaderKind {
    Name,
    Attributes,
}

impl ColumnSubheaderKind {
    const fn messages(self) -> SubheaderValidationMessages {
        match self {
            Self::Name => SubheaderValidationMessages {
                too_short: "column name subheader too short",
                length_invalid: "column name subheader length invalid",
                remainder_mismatch: "column name remainder mismatch",
            },
            Self::Attributes => SubheaderValidationMessages {
                too_short: "column attributes subheader too short",
                length_invalid: "column attributes subheader length invalid",
                remainder_mismatch: "column attributes remainder mismatch",
            },
        }
    }

    fn validate(
        self,
        bytes: &[u8],
        signature_len: usize,
        endian: Endianness,
        uses_u64: bool,
    ) -> Result<()> {
        validate_subheader_lengths(
            bytes,
            signature_len,
            endian,
            uses_u64,
            self.messages(),
        )
    }
}

fn validate_subheader_lengths(
    bytes: &[u8],
    signature_len: usize,
    endian: Endianness,
    uses_u64: bool,
    messages: SubheaderValidationMessages,
) -> Result<()> {
    let base = if uses_u64 { 28 } else { 20 };
    if bytes.len() < base {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from(messages.too_short),
        });
    }
    let remainder = read_u16(endian, &bytes[signature_len..signature_len + 2]);
    let expected =
        expected_remainder(bytes.len(), signature_len).ok_or_else(|| Error::Corrupted {
            section: Section::Header,
            details: Cow::from(messages.length_invalid),
        })?;
    if remainder != expected {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from(messages.remainder_mismatch),
        });
    }
    Ok(())
}

pub fn parse_column_name_subheader(
    builder: &mut ColumnMetadataBuilder,
    bytes: &[u8],
    signature_len: usize,
    endian: Endianness,
    uses_u64: bool,
) -> Result<()> {
    ColumnSubheaderKind::Name.validate(bytes, signature_len, endian, uses_u64)?;

    let chunk_width = 8;
    let entries = subheader_entries(bytes.len(), uses_u64, chunk_width);

    if entries == 0 {
        return Ok(());
    }

    let expected_len = signature_len + 8 + entries * chunk_width;
    if bytes.len() < expected_len {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("column name subheader truncated"),
        });
    }

    let mut cursor = signature_len + 8;
    let start_index = builder.names_seen();
    for offset in 0..entries {
        let text_ref = parse_text_ref(endian, &bytes[cursor..cursor + 6]);
        let column_index =
            u32::try_from(start_index + offset).map_err(|_| Error::InvalidMetadata {
                details: Cow::from("column index exceeds supported range"),
            })?;
        let column = builder.ensure_column(column_index);
        column.name_ref = text_ref;
        cursor += chunk_width;
    }
    builder.note_names_processed(entries);
    Ok(())
}

pub fn parse_column_attrs_subheader(
    builder: &mut ColumnMetadataBuilder,
    bytes: &[u8],
    signature_len: usize,
    endian: Endianness,
    uses_u64: bool,
) -> Result<()> {
    ColumnSubheaderKind::Attributes.validate(bytes, signature_len, endian, uses_u64)?;

    let row_size = if uses_u64 { 16 } else { 12 };
    let entries = subheader_entries(bytes.len(), uses_u64, row_size);
    if entries == 0 {
        return Ok(());
    }

    let expected_len = signature_len + 8 + entries * row_size;
    if bytes.len() < expected_len {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("column attributes subheader truncated"),
        });
    }

    let mut cursor = signature_len + 8;
    let start_index = builder.attrs_seen();
    for offset in 0..entries {
        let column_index =
            u32::try_from(start_index + offset).map_err(|_| Error::InvalidMetadata {
                details: Cow::from("column index exceeds supported range"),
            })?;
        builder.ensure_column(column_index);
        let entry_start = cursor;
        let (offset_value, width_value, type_pos, next_cursor, measure_byte_pos) = if uses_u64 {
            let offset = read_u64(endian, &bytes[entry_start..entry_start + 8]);
            let width = read_u32(endian, &bytes[entry_start + 8..entry_start + 12]);
            let next_cursor = entry_start + 16;
            let measure_pos = entry_start + 8 + 5;
            let measure_pos = if measure_pos < next_cursor {
                Some(measure_pos)
            } else {
                None
            };
            (offset, width, entry_start + 14, next_cursor, measure_pos)
        } else {
            let offset = u64::from(read_u32(endian, &bytes[entry_start..entry_start + 4]));
            let width = read_u32(endian, &bytes[entry_start + 4..entry_start + 8]);
            let next_cursor = entry_start + 12;
            let measure_pos = entry_start + 4 + 5;
            let measure_pos = if measure_pos < next_cursor {
                Some(measure_pos)
            } else {
                None
            };
            (offset, width, entry_start + 10, next_cursor, measure_pos)
        };

        builder.update_max_width(width_value);
        {
            let column = builder.column_mut(column_index);
            column.offsets.offset = offset_value;
            column.offsets.width = width_value;
        }

        let column_type = bytes[type_pos];
        {
            let column = builder.column_mut(column_index);
            column.kind =
                ColumnKind::from_type_code(column_type).ok_or_else(|| Error::Corrupted {
                    section: Section::Column {
                        index: column_index,
                    },
                    details: Cow::from("unknown column type code"),
                })?;
            if let Some(measure_pos) = measure_byte_pos {
                let measure_algn = bytes[measure_pos];
                column.measure = match measure_algn & 0x0F {
                    1 => Measure::Nominal,
                    2 => Measure::Ordinal,
                    3 => Measure::Scale,
                    _ => Measure::Unknown,
                };
                column.alignment = match (measure_algn >> 4) & 0x0F {
                    1 => Alignment::Left,
                    2 => Alignment::Center,
                    3 => Alignment::Right,
                    _ => Alignment::Unknown,
                };
            }
        }

        cursor = next_cursor;
    }
    builder.note_attrs_processed(entries);
    Ok(())
}

pub fn parse_column_list_subheader(
    builder: &mut ColumnMetadataBuilder,
    bytes: &[u8],
    signature_len: usize,
    endian: Endianness,
    uses_u64: bool,
) -> Result<()> {
    if uses_u64 || signature_len != 4 {
        // Column list parsing for u64 alignment is currently unsupported.
        return Ok(());
    }

    if bytes.len() < COLUMN_LIST_HEADER_LEN {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("column list subheader too short"),
        });
    }

    let signature = read_u32(endian, &bytes[0..4]);
    if signature != 0xFFFF_FFFE {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("unexpected signature for column list subheader"),
        });
    }

    let list_len = usize::from(read_u16(endian, &bytes[18..20]));
    if list_len == 0 {
        return Ok(());
    }
    let values_offset = COLUMN_LIST_HEADER_LEN;
    let required = values_offset + list_len * 2;
    if bytes.len() < required {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("column list subheader truncated"),
        });
    }

    let mut values = Vec::with_capacity(list_len);
    for idx in 0..list_len {
        let pos = values_offset + idx * 2;
        values.push(read_i16(endian, &bytes[pos..pos + 2]));
    }

    // Preserve the first observed column list only; later duplicates may appear when
    // a list is split across pages. When we encounter a second fragment, extend the
    // existing values rather than overwriting them.
    builder.append_column_list(values);
    Ok(())
}

pub fn parse_column_format_subheader(
    builder: &mut ColumnMetadataBuilder,
    bytes: &[u8],
    endian: Endianness,
    uses_u64: bool,
) -> Result<()> {
    let min_len = if uses_u64 { 58 } else { 46 };
    if bytes.len() < min_len {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("column format subheader too short"),
        });
    }

    let column_index =
        u32::try_from(builder.formats_seen()).map_err(|_| Error::InvalidMetadata {
            details: Cow::from("column index exceeds supported range"),
        })?;
    let column = builder.ensure_column(column_index);

    if uses_u64 {
        column.format_width = Some(read_u16(endian, &bytes[24..26]));
        column.format_decimals = Some(read_u16(endian, &bytes[26..28]));
        column.format_ref = parse_text_ref(endian, &bytes[46..52]);
        column.label_ref = parse_text_ref(endian, &bytes[52..58]);
    } else {
        column.format_ref = parse_text_ref(endian, &bytes[34..40]);
        column.label_ref = parse_text_ref(endian, &bytes[40..46]);
    }

    builder.note_formats_processed();
    Ok(())
}

fn infer_numeric_kind(format_name: &str) -> Option<NumericKind> {
    if format_name.is_empty() {
        return None;
    }
    let cleaned = format_name.trim().trim_matches('.').to_ascii_uppercase();
    if cleaned.is_empty() {
        return None;
    }
    if cleaned.contains("DATETIME")
        || cleaned.ends_with("DT")
        || cleaned.starts_with("E8601DT")
        || cleaned.starts_with("B8601DT")
    {
        return Some(NumericKind::DateTime);
    }
    if cleaned.contains("TIME") || cleaned.ends_with("TM") || cleaned.starts_with("E8601TM") {
        return Some(NumericKind::Time);
    }
    if cleaned.contains("DATE")
        || cleaned.contains("YY")
        || cleaned.contains("MON")
        || cleaned.contains("WEEK")
        || cleaned.contains("YEAR")
        || cleaned.contains("MINGUO")
        || cleaned.ends_with("DA")
        || cleaned.starts_with("E8601DA")
        || cleaned.starts_with("B8601DA")
    {
        return Some(NumericKind::Date);
    }
    None
}

#[derive(Debug, Clone)]
pub struct RowInfo {
    pub row_length: u32,
    pub total_rows: u64,
    pub rows_per_page: u64,
    pub compression: Compression,
    pub file_label: Option<String>,
}

pub fn parse_column_size_subheader(
    builder: &mut ColumnMetadataBuilder,
    bytes: &[u8],
    endian: Endianness,
    uses_u64: bool,
) -> Result<u32> {
    let min_len = if uses_u64 { 16 } else { 8 };
    if bytes.len() < min_len {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("column size subheader too short"),
        });
    }

    let raw_count = if uses_u64 {
        read_u64(endian, &bytes[8..16])
    } else {
        u64::from(read_u32(endian, &bytes[4..8]))
    };

    let column_count = u32::try_from(raw_count).map_err(|_| Error::InvalidMetadata {
        details: Cow::from("column count exceeds supported range"),
    })?;

    builder.set_column_count(column_count);
    for idx in 0..column_count {
        builder.ensure_column(idx);
    }

    Ok(column_count)
}

pub fn parse_row_size_subheader(
    builder: &ColumnMetadataBuilder,
    bytes: &[u8],
    _signature_len: usize,
    endian: Endianness,
    uses_u64: bool,
) -> Result<RowInfo> {
    let min_len = if uses_u64 { 250 } else { 190 };
    if bytes.len() < min_len {
        return Err(Error::Corrupted {
            section: Section::Header,
            details: Cow::from("row size subheader too short"),
        });
    }

    let row_length_raw = if uses_u64 {
        read_u64(endian, &bytes[40..48])
    } else {
        u64::from(read_u32(endian, &bytes[20..24]))
    };
    let total_rows = if uses_u64 {
        read_u64(endian, &bytes[48..56])
    } else {
        u64::from(read_u32(endian, &bytes[24..28]))
    };
    let rows_per_page = if uses_u64 {
        read_u64(endian, &bytes[120..128])
    } else {
        u64::from(read_u32(endian, &bytes[60..64]))
    };

    let row_length = u32::try_from(row_length_raw).map_err(|_| Error::InvalidMetadata {
        details: Cow::from("row length exceeds supported range"),
    })?;

    let text_store = builder.text_store();
    let label_ref_offset = bytes
        .len()
        .checked_sub(130)
        .ok_or_else(|| Error::Corrupted {
            section: Section::Header,
            details: Cow::from("row size subheader missing file label reference"),
        })?;
    let compression_ref_offset = bytes
        .len()
        .checked_sub(118)
        .ok_or_else(|| Error::Corrupted {
            section: Section::Header,
            details: Cow::from("row size subheader missing compression reference"),
        })?;

    let file_label = {
        let text_ref = parse_text_ref(endian, &bytes[label_ref_offset..label_ref_offset + 6]);
        text_store
            .resolve(text_ref)?
            .map(|s| s.trim_end().to_string())
            .filter(|s| !s.is_empty())
    };

    let compression = {
        let text_ref = parse_text_ref(
            endian,
            &bytes[compression_ref_offset..compression_ref_offset + 6],
        );
        text_store
            .resolve(text_ref)?
            .map_or(Compression::None, |value| {
                let text = value.trim();
                match text {
                    "SASYZCR2" => Compression::Binary,
                    "SASYZCRL" => Compression::Row,
                    _ => Compression::None,
                }
            })
    };

    Ok(RowInfo {
        row_length,
        total_rows,
        rows_per_page,
        compression,
        file_label,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_text_subheader_pushes_blob() {
        let mut builder = ColumnMetadataBuilder::new();
        let signature_len = 4;
        let mut bytes = vec![0u8; signature_len + 2];
        bytes[..4].copy_from_slice(&[0xFD, 0xFF, 0xFF, 0xFF]);
        bytes.extend_from_slice(b"Name\0\0");
        let remainder = (bytes.len() - (4 + 2 * signature_len)) as u16;
        bytes[signature_len..signature_len + 2].copy_from_slice(&remainder.to_le_bytes());

        parse_column_text_subheader(&mut builder, &bytes, 4, Endianness::Little).unwrap();

        assert_eq!(builder.text_store().len(), 1);
        let blob = builder.text_store().blob(0).unwrap();
        assert_eq!(blob.len(), bytes.len() - signature_len);
    }

    #[test]
    fn column_name_subheader_sets_text_refs() {
        let mut builder = ColumnMetadataBuilder::new();
        builder
            .text_store_mut()
            .push_blob(&[0, 0, b'C', b'O', b'L', b'1', 0, 0]);

        let signature_len = 4;
        let mut bytes = vec![0u8; signature_len + 8];
        bytes[..4].copy_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);
        bytes.extend_from_slice(&[0x00, 0x00, 0x02, 0x00, 0x04, 0x00, 0x00, 0x00]);
        bytes.extend_from_slice(&[0u8; 8]);
        let remainder = (bytes.len() - (4 + 2 * signature_len)) as u16;
        bytes[signature_len..signature_len + 2].copy_from_slice(&remainder.to_le_bytes());
        assert_eq!(bytes.len(), 28);

        parse_column_name_subheader(&mut builder, &bytes, 4, Endianness::Little, false).unwrap();

        assert_eq!(builder.names_seen(), 1);
        let column = builder.column_mut(0);
        assert_eq!(column.name_ref.index, 0);
        assert_eq!(column.name_ref.offset, 2);
        assert_eq!(column.name_ref.length, 4);
    }

    #[test]
    fn column_attrs_subheader_updates_offsets() {
        let mut builder = ColumnMetadataBuilder::new();
        let signature_len = 4;
        let mut bytes = vec![0u8; signature_len + 8];
        bytes[..4].copy_from_slice(&[0xF6, 0xF6, 0xF6, 0xF6]);
        let mut entry = [0u8; 12];
        entry[0..4].copy_from_slice(&4u32.to_le_bytes());
        entry[4..8].copy_from_slice(&8u32.to_le_bytes());
        entry[10] = 0x02;
        bytes.extend_from_slice(&entry);
        bytes.extend_from_slice(&[0u8; 8]);
        let remainder = (bytes.len() - (4 + 2 * signature_len)) as u16;
        bytes[signature_len..signature_len + 2].copy_from_slice(&remainder.to_le_bytes());
        assert_eq!(bytes.len(), 32);

        parse_column_attrs_subheader(&mut builder, &bytes, 4, Endianness::Little, false).unwrap();

        assert_eq!(builder.attrs_seen(), 1);
        assert_eq!(builder.max_width(), 8);
        let column = builder.column_mut(0);
        assert_eq!(column.offsets.offset, 4);
        assert_eq!(column.offsets.width, 8);
        assert!(matches!(column.kind, ColumnKind::Character));
    }

    #[test]
    fn column_attrs_subheader_sets_measure_alignment() {
        let mut builder = ColumnMetadataBuilder::new();
        let signature_len = 4;
        let mut bytes = vec![0u8; signature_len + 8];
        bytes[..4].copy_from_slice(&[0xF6, 0xF6, 0xF6, 0xF6]);
        let mut entry = [0u8; 12];
        entry[0..4].copy_from_slice(&16u32.to_le_bytes());
        entry[4..8].copy_from_slice(&32u32.to_le_bytes());
        entry[8] = 0x00;
        entry[9] = 0x32;
        entry[10] = 0x01;
        bytes.extend_from_slice(&entry);
        bytes.extend_from_slice(&[0u8; 8]);
        let remainder = (bytes.len() - (4 + 2 * signature_len)) as u16;
        bytes[signature_len..signature_len + 2].copy_from_slice(&remainder.to_le_bytes());

        parse_column_attrs_subheader(
            &mut builder,
            &bytes,
            signature_len,
            Endianness::Little,
            false,
        )
        .unwrap();

        let column = builder.column_mut(0);
        assert_eq!(column.measure, Measure::Ordinal);
        assert_eq!(column.alignment, Alignment::Right);
    }

    #[test]
    fn column_list_subheader_collects_values() {
        let mut builder = ColumnMetadataBuilder::new();
        let bytes: Vec<u8> = vec![
            0xfe, 0xff, 0xff, 0xff, 0x3c, 0x00, 0xdc, 0x7f, 0x00, 0x00, 0x00, 0x00, 0x34, 0x00,
            0x00, 0x00, 0x0d, 0x00, 0x11, 0x00, 0x01, 0x00, 0x0d, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0xfe, 0xff, 0xf7, 0xff, 0x0d, 0x00, 0x00, 0x00, 0xff, 0xff, 0x0c, 0x00,
            0xf8, 0xff, 0x00, 0x00, 0xfd, 0xff, 0x00, 0x00, 0xfb, 0xff, 0x00, 0x00, 0x0a, 0x00,
            0x06, 0x00, 0xfc, 0xff, 0x0b, 0x00, 0xf9, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00,
        ];

        parse_column_list_subheader(&mut builder, &bytes, 4, Endianness::Little, false).unwrap();

        let list = builder.column_list().expect("column list captured");
        assert_eq!(list.len(), 17);
        assert_eq!(list[0], -2);
        assert_eq!(list[2], 13);
        assert_eq!(list[4], -1);
    }

    #[test]
    fn column_format_subheader_sets_refs() {
        let mut builder = ColumnMetadataBuilder::new();
        builder
            .text_store_mut()
            .push_blob(&[0, 0, 0, 0, 0, 0, b'F', b'M', b'T', 0, b'L', b'B', 0, 0]);

        let mut bytes = vec![0u8; 46];
        bytes[0..4].copy_from_slice(&[0xFB, 0xFF, 0xFB, 0xFF]);
        bytes[34..40].copy_from_slice(&[0x00, 0x00, 0x06, 0x00, 0x04, 0x00]);
        bytes[40..46].copy_from_slice(&[0x00, 0x00, 0x0A, 0x00, 0x02, 0x00]);

        parse_column_format_subheader(&mut builder, &bytes, Endianness::Little, false).unwrap();

        assert_eq!(builder.formats_seen(), 1);
        let column = builder.column_mut(0);
        assert_eq!(column.format_ref.offset, 6);
        assert_eq!(column.format_ref.length, 4);
        assert_eq!(column.label_ref.offset, 10);
        assert_eq!(column.label_ref.length, 2);
    }
}
