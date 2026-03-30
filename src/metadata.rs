use crate::row::CellValue;
use std::time::SystemTime;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalType {
    Integer,
    Float,
    String,
    Date,
    DateTime,
    Time,
    Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SasDate {
    pub days_since_sas_epoch: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SasDateTime {
    pub seconds_since_sas_epoch: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SasTime {
    pub seconds_since_midnight: i64,
}

#[derive(Debug, Clone, Default)]
pub struct DatasetMetadata {
    pub table_name: Option<String>,
    pub file_label: Option<String>,
    pub encoding: Option<String>,
    pub endianness: Endianness,
    pub page_size: u32,
    pub page_count: u64,
    pub row_count: u64,
    pub row_len: u32,
    pub compression: CompressionKind,
    pub created_at: Option<Timestamp>,
    pub modified_at: Option<Timestamp>,
}

#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub index: usize,
    pub name: String,
    pub logical_type: LogicalType,
    pub physical_width: u32,
    pub offset: u32,
    pub label: Option<String>,
    pub format: Option<String>,
}

impl ColumnMeta {
    #[must_use]
    pub fn borrowed_name(&self) -> &str {
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
