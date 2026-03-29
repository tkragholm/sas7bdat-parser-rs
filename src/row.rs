use crate::metadata::{SasDate, SasDateTime, SasTime};

#[derive(Debug, Clone, PartialEq)]
pub enum CellValue<'a> {
    Null,
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Str(&'a str),
    Bytes(&'a [u8]),
    Date(SasDate),
    DateTime(SasDateTime),
    Time(SasTime),
}

#[derive(Debug, Clone, PartialEq)]
pub enum OwnedCellValue {
    Null,
    Int32(i32),
    Int64(i64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Date(SasDate),
    DateTime(SasDateTime),
    Time(SasTime),
}

#[derive(Debug, Clone, Copy)]
pub struct RawRow<'a> {
    pub row_index: u64,
    pub bytes: &'a [u8],
}

#[derive(Debug, Clone)]
pub struct RowView<'a> {
    pub(crate) row_index: u64,
    pub(crate) names: &'a [String],
    pub(crate) cells: &'a [CellValue<'a>],
}

impl<'a> RowView<'a> {
    #[must_use]
    pub fn row_index(&self) -> u64 {
        self.row_index
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.cells.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    #[must_use]
    pub fn get(&self, idx: usize) -> Option<&CellValue<'a>> {
        self.cells.get(idx)
    }

    #[must_use]
    pub fn get_by_name(&self, name: &str) -> Option<&CellValue<'a>> {
        self.names
            .iter()
            .position(|candidate| candidate == name)
            .and_then(|idx| self.cells.get(idx))
    }

    pub fn iter(&self) -> impl Iterator<Item = &CellValue<'a>> {
        self.cells.iter()
    }
}

#[derive(Debug, Clone, Default)]
pub struct OwnedRow {
    pub row_index: u64,
    pub cells: Vec<OwnedCellValue>,
}

impl<'a> CellValue<'a> {
    #[must_use]
    pub fn to_owned_value(&self) -> OwnedCellValue {
        match self {
            Self::Null => OwnedCellValue::Null,
            Self::Int32(value) => OwnedCellValue::Int32(*value),
            Self::Int64(value) => OwnedCellValue::Int64(*value),
            Self::Float64(value) => OwnedCellValue::Float64(*value),
            Self::Str(value) => OwnedCellValue::String((*value).to_owned()),
            Self::Bytes(value) => OwnedCellValue::Bytes((*value).to_vec()),
            Self::Date(value) => OwnedCellValue::Date(*value),
            Self::DateTime(value) => OwnedCellValue::DateTime(*value),
            Self::Time(value) => OwnedCellValue::Time(*value),
        }
    }
}
