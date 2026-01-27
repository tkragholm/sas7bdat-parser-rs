use super::{labels::LabelSet, variables::Variable};
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

    #[must_use]
    pub fn column_index(&self, name: &str) -> Option<usize> {
        let trimmed = name.trim_end();
        for variable in &self.variables {
            if variable.name == name || variable.name.trim_end() == trimmed {
                return Some(variable.index as usize);
            }
        }
        None
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
