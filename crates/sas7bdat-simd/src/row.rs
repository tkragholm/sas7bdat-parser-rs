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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawRow<'a> {
    pub row_index: crate::types::RowIndex,
    pub bytes: &'a [u8],
}

#[derive(Debug, Clone)]
pub struct RowView<'a> {
    pub(crate) row_index: crate::types::RowIndex,
    pub(crate) names: &'a [String],
    pub(crate) cells: &'a [CellValue<'a>],
}

impl<'a> RowView<'a> {
    #[must_use]
    pub const fn row_index(&self) -> crate::types::RowIndex {
        self.row_index
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.cells.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    #[must_use]
    pub fn get(&self, idx: usize) -> Option<&CellValue<'a>> {
        self.cells.get(idx)
    }

    /// Look up a cell by column name.
    ///
    /// **O(n) in the number of columns.** Calling this inside a `visit_rows` loop on wide
    /// datasets is a silent performance trap. For repeated named access, pre-compute a
    /// column-name index once (e.g. a `HashMap<&str, usize>`) and use positional [`Self::get`].
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

    pub fn iter_named(&self) -> impl Iterator<Item = (&str, &CellValue<'a>)> {
        self.names.iter().map(String::as_str).zip(self.cells.iter())
    }
}

#[derive(Debug, Clone, Default)]
pub struct OwnedRow {
    pub row_index: crate::types::RowIndex,
    pub cells: Vec<OwnedCellValue>,
}

impl OwnedRow {
    /// Look up a cell by column name using the dataset's column list.
    /// The `columns` slice should come from [`crate::Dataset::columns`].
    /// This is O(n) in the number of columns.
    #[must_use]
    pub fn get_by_name<'a>(
        &'a self,
        name: &str,
        columns: &[crate::metadata::ColumnMeta],
    ) -> Option<&'a OwnedCellValue> {
        columns
            .iter()
            .position(|col| col.name() == name)
            .and_then(|idx| self.cells.get(idx))
    }
}

impl<'a> CellValue<'a> {
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    #[must_use]
    pub fn as_str(&self) -> Option<&'a str> {
        if let Self::Str(s) = self { Some(s) } else { None }
    }

    #[must_use]
    pub fn as_f64(&self) -> Option<f64> {
        if let Self::Float64(v) = self { Some(*v) } else { None }
    }

    #[must_use]
    pub fn as_i32(&self) -> Option<i32> {
        if let Self::Int32(v) = self { Some(*v) } else { None }
    }

    #[must_use]
    pub fn as_i64(&self) -> Option<i64> {
        if let Self::Int64(v) = self { Some(*v) } else { None }
    }

    #[must_use]
    pub fn as_bytes(&self) -> Option<&'a [u8]> {
        if let Self::Bytes(b) = self { Some(b) } else { None }
    }

    #[must_use]
    pub fn as_date(&self) -> Option<SasDate> {
        if let Self::Date(d) = self { Some(*d) } else { None }
    }

    #[must_use]
    pub fn as_datetime(&self) -> Option<SasDateTime> {
        if let Self::DateTime(d) = self { Some(*d) } else { None }
    }

    #[must_use]
    pub fn as_time(&self) -> Option<SasTime> {
        if let Self::Time(t) = self { Some(*t) } else { None }
    }

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

impl OwnedCellValue {
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        if let Self::String(s) = self { Some(s) } else { None }
    }

    #[must_use]
    pub fn as_f64(&self) -> Option<f64> {
        if let Self::Float64(v) = self { Some(*v) } else { None }
    }

    #[must_use]
    pub fn as_i32(&self) -> Option<i32> {
        if let Self::Int32(v) = self { Some(*v) } else { None }
    }

    #[must_use]
    pub fn as_i64(&self) -> Option<i64> {
        if let Self::Int64(v) = self { Some(*v) } else { None }
    }

    #[must_use]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        if let Self::Bytes(b) = self { Some(b) } else { None }
    }

    #[must_use]
    pub fn as_date(&self) -> Option<SasDate> {
        if let Self::Date(d) = self { Some(*d) } else { None }
    }

    #[must_use]
    pub fn as_datetime(&self) -> Option<SasDateTime> {
        if let Self::DateTime(d) = self { Some(*d) } else { None }
    }

    #[must_use]
    pub fn as_time(&self) -> Option<SasTime> {
        if let Self::Time(t) = self { Some(*t) } else { None }
    }
}
