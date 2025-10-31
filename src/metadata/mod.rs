use std::collections::HashMap;

use time::OffsetDateTime;

/// High-level metadata for a SAS dataset.
#[derive(Debug, Clone)]
pub struct DatasetMetadata {
    pub row_count: u64,
    pub column_count: u32,
    pub version: SasVersion,
    pub compression: Compression,
    pub endianness: Endianness,
    pub timestamps: DatasetTimestamps,
    pub table_name: Option<String>,
    pub file_label: Option<String>,
    pub file_encoding: Option<String>,
    pub vendor: Vendor,
    pub variables: Vec<Variable>,
    pub label_sets: HashMap<String, LabelSet>,
    pub column_list: Vec<i16>,
}

impl DatasetMetadata {
    #[must_use]
    pub fn new(column_count: u32) -> Self {
        Self {
            row_count: 0,
            column_count,
            version: SasVersion::default(),
            compression: Compression::None,
            endianness: Endianness::Little,
            timestamps: DatasetTimestamps::default(),
            table_name: None,
            file_label: None,
            file_encoding: None,
            vendor: Vendor::Sas,
            variables: Vec::with_capacity(column_count as usize),
            label_sets: HashMap::new(),
            column_list: Vec::new(),
        }
    }
}

/// Dataset creation and modification times.
#[derive(Debug, Clone, Default)]
pub struct DatasetTimestamps {
    pub created: Option<OffsetDateTime>,
    pub modified: Option<OffsetDateTime>,
}

/// SAS version components extracted from the header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SasVersion {
    pub major: u16,
    pub minor: u16,
    pub revision: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Vendor {
    Sas,
    StatTransfer,
    Other(u16),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Row,
    Binary,
    Unknown(u16),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endianness {
    Little,
    Big,
}

/// Variable metadata mirroring the SAS column descriptor.
#[derive(Debug, Clone)]
pub struct Variable {
    pub index: u32,
    pub name: String,
    pub label: Option<String>,
    pub format: Option<Format>,
    pub kind: VariableKind,
    pub storage_width: usize,
    pub user_width: Option<usize>,
    pub missing: MissingValuePolicy,
    pub measure: Measure,
    pub alignment: Alignment,
    pub display_width: Option<u16>,
    pub decimals: Option<u16>,
    pub value_labels: Option<String>,
}

impl Variable {
    #[must_use]
    pub fn new(index: u32, name: String, kind: VariableKind, storage_width: usize) -> Self {
        Self {
            index,
            name,
            label: None,
            format: None,
            kind,
            storage_width,
            user_width: None,
            missing: MissingValuePolicy::default(),
            measure: Measure::Unknown,
            alignment: Alignment::Unknown,
            display_width: None,
            decimals: None,
            value_labels: None,
        }
    }
}

impl Default for Variable {
    fn default() -> Self {
        Self::new(0, String::new(), VariableKind::Numeric, 0)
    }
}

#[derive(Debug, Clone)]
pub enum VariableKind {
    Numeric,
    Character,
}

#[derive(Debug, Clone)]
pub struct Format {
    pub name: String,
    pub width: Option<u16>,
    pub decimals: Option<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Measure {
    Unknown,
    Nominal,
    Ordinal,
    Scale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Alignment {
    Unknown,
    Left,
    Center,
    Right,
}

#[derive(Debug, Clone, Default)]
pub struct MissingValuePolicy {
    pub system_missing: bool,
    pub tagged_missing: Vec<TaggedMissing>,
    pub ranges: Vec<MissingRange>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaggedMissing {
    pub tag: Option<char>,
    pub literal: MissingLiteral,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MissingRange {
    Numeric { start: f64, end: f64 },
    String { start: String, end: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum MissingLiteral {
    Numeric(f64),
    String(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct LabelSet {
    pub name: String,
    pub value_type: ValueType,
    pub labels: Vec<ValueLabel>,
}

impl LabelSet {
    #[must_use]
    pub const fn new(name: String, value_type: ValueType) -> Self {
        Self {
            name,
            value_type,
            labels: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValueLabel {
    pub key: ValueKey,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValueKey {
    Numeric(f64),
    Integer(i32),
    Tagged(char),
    String(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    Numeric,
    String,
}
