use crate::{labels::LabelSet, row::CellValue};
use std::{collections::HashMap, time::SystemTime};

pub type Timestamp = SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Endianness {
    #[default]
    Little,
    Big,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionKind {
    #[default]
    None,
    Row,
    Binary,
    Unknown,
}

/// The logical interpretation of a SAS column. 
/// 
/// SAS stores nearly all numeric data as 8-byte floats. `LogicalType` captures 
/// the intent of the column based on its format and internal flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalType {
    /// A numeric column that contains only whole numbers within a safe integer range.
    Integer,
    /// A generic numeric column.
    Float,
    /// A fixed-width, space-padded character column.
    String,
    /// Days since the SAS epoch (1960-01-01).
    Date,
    /// Seconds since the SAS epoch (1960-01-01 00:00:00).
    DateTime,
    /// Seconds since midnight.
    Time,
    /// Uninterpreted binary data.
    Bytes,
}

/// A wrapper for SAS date values (days since 1960-01-01).
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct SasDate {
    pub days_since_sas_epoch: i32,
}

/// A wrapper for SAS datetime values (seconds since 1960-01-01).
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct SasDateTime {
    pub seconds_since_sas_epoch: i64,
}

/// A wrapper for SAS time values (seconds since midnight).
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct SasTime {
    pub seconds_since_midnight: i32,
}

/// Metadata extracted from the SAS7BDAT file header and subheader streams.
#[derive(Debug, Clone, Default)]
pub struct DatasetMetadata {
    /// The name of the table as stored in the header (usually the filename).
    pub table_name: Option<String>,
    /// An optional descriptive label for the dataset.
    pub file_label: Option<String>,
    /// The character encoding (e.g., "UTF-8", "WINDOWS-1252").
    pub encoding: Option<String>,
    /// The byte order of the file (LittleEndian for Windows/Linux, BigEndian for Unix).
    pub endianness: Endianness,
    /// Size of a single data page in bytes.
    pub page_size: u32,
    /// Total number of pages in the file (including metadata).
    pub page_count: u64,
    /// Total number of logical rows in the dataset.
    pub row_count: u64,
    /// Length of a single row in bytes on disk.
    pub row_len: u32,
    /// The compression algorithm used for data pages.
    pub compression: CompressionKind,
    /// File creation timestamp.
    pub created_at: Option<Timestamp>,
    /// File last modification timestamp.
    pub modified_at: Option<Timestamp>,
    /// Value-label sets loaded from a companion `.sas7bcat` file.
    /// Keyed by normalized format name (uppercase, trimmed, `$` prefix preserved).
    pub label_sets: HashMap<String, LabelSet>,
}

/// Metadata for a single column (variable) in the dataset.
#[derive(Debug, Clone)]
pub struct ColumnMeta {
    /// 0-based position of the column in the dataset.
    pub index: usize,
    /// The variable name (e.g., "GENDER").
    pub name: String,
    /// The inferred logical type.
    pub logical_type: LogicalType,
    /// The width of the column in bytes on disk.
    pub physical_width: u32,
    /// The byte offset of the column's data within a row.
    pub offset: u32,
    /// An optional descriptive label (e.g., "Patient Gender").
    pub label: Option<String>,
    /// The SAS format assigned to this column (e.g., "DATE9.").
    pub format: Option<String>,
}

impl ColumnMeta {
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl From<SasDate> for CellValue<'_> {
    fn from(value: SasDate) -> Self {
        Self::Date(value)
    }
}

impl From<SasDateTime> for CellValue<'_> {
    fn from(value: SasDateTime) -> Self {
        Self::DateTime(value)
    }
}

impl From<SasTime> for CellValue<'_> {
    fn from(value: SasTime) -> Self {
        Self::Time(value)
    }
}
