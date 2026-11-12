use crate::metadata::{SasDate, SasDateTime, SasTime};

/// One decoded cell of a row, borrowing from the page where it can.
///
/// In the default typed decode, a missing numeric value is [`Null`](Self::Null). A numeric value
/// with no fractional part is an [`Int32`](Self::Int32) or [`Int64`](Self::Int64), and any other
/// number is a [`Float64`](Self::Float64). Which integer type depends on the stored width of the
/// column. Character columns are [`Str`](Self::Str), an empty string when blank. Date, datetime
/// and time columns are [`Date`](Self::Date), [`DateTime`](Self::DateTime) and
/// [`Time`](Self::Time) unless [`TemporalDecodeOptions`](crate::TemporalDecodeOptions) turns that
/// off. The `as_*` methods return `None` for any other variant.
///
/// ```no_run
/// use sas7bdat::{CellValue, Dataset, Result};
/// use std::ops::ControlFlow;
///
/// fn main() -> Result<()> {
///     let ds = Dataset::open("data.sas7bdat")?;
///     let mut total = 0.0;
///     ds.visit_rows(|row| {
///         match row.get_by_name("AGE") {
///             Some(CellValue::Float64(age)) => total += age,
///             Some(CellValue::Int32(age)) => total += f64::from(*age),
///             _ => {}
///         }
///         Ok(ControlFlow::Continue(()))
///     })?;
///     println!("{total}");
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum CellValue<'a> {
    /// A missing value.
    Null,
    /// A whole number that fits in 32 bits.
    Int32(i32),
    /// A whole number in 64 bits.
    Int64(i64),
    /// A number with a fractional part, or one too large for 64-bit integers. Also every numeric
    /// cell in [`DecodeMode::TypedLossless`](crate::DecodeMode::TypedLossless).
    Float64(f64),
    /// A character value, trimmed according to [`TrimMode`](crate::TrimMode).
    Str(&'a str),
    /// Uninterpreted bytes.
    Bytes(&'a [u8]),
    /// A date.
    Date(SasDate),
    /// A datetime.
    DateTime(SasDateTime),
    /// A time of day.
    Time(SasTime),
}

/// A decoded cell that owns its data. The owned form of [`CellValue`], with the same variants.
#[derive(Debug, Clone, PartialEq)]
pub enum OwnedCellValue {
    /// A missing value.
    Null,
    /// A whole number that fits in 32 bits.
    Int32(i32),
    /// A whole number in 64 bits.
    Int64(i64),
    /// A number with a fractional part, or one too large for 64-bit integers.
    Float64(f64),
    /// A character value.
    String(String),
    /// Uninterpreted bytes.
    Bytes(Vec<u8>),
    /// A date.
    Date(SasDate),
    /// A datetime.
    DateTime(SasDateTime),
    /// A time of day.
    Time(SasTime),
}

/// The undecoded bytes of one row, from [`ScanBuilder::visit_raw_rows`](crate::ScanBuilder::visit_raw_rows).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawRow<'a> {
    /// The row's position in the file.
    pub row_index: crate::types::RowIndex,
    /// The row as stored, after decompression. Columns sit at the offsets in
    /// [`ColumnMeta`](crate::ColumnMeta).
    pub bytes: &'a [u8],
}

/// One decoded row, borrowed for the duration of a [`ScanBuilder::visit_rows`](crate::ScanBuilder::visit_rows) callback.
///
/// Cells are in projection order. Use [`get`](Self::get) for fast access by position and
/// [`get_by_name`](Self::get_by_name) for convenience. Call [`CellValue::to_owned_value`] on
/// cells that must outlive the callback.
#[derive(Debug, Clone)]
pub struct RowView<'a> {
    pub(crate) row_index: crate::types::RowIndex,
    pub(crate) names: &'a [String],
    pub(crate) cells: &'a [CellValue<'a>],
}

impl<'a> RowView<'a> {
    /// The row's position in the file.
    #[must_use]
    pub const fn row_index(&self) -> crate::types::RowIndex {
        self.row_index
    }

    /// The number of cells.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.cells.len()
    }

    /// Whether the row has no cells.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// The cell at 0-based position `idx` in the projection, or `None` past the end.
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

    /// The cells in order.
    pub fn iter(&self) -> impl Iterator<Item = &CellValue<'a>> {
        self.cells.iter()
    }

    /// The cells in order, each with its column name.
    pub fn iter_named(&self) -> impl Iterator<Item = (&str, &CellValue<'a>)> {
        self.names.iter().map(String::as_str).zip(self.cells.iter())
    }
}

/// One decoded row that owns its cells, from [`Dataset::collect_rows`](crate::Dataset::collect_rows).
///
/// ```no_run
/// use sas7bdat::{Dataset, Result};
///
/// fn main() -> Result<()> {
///     let ds = Dataset::open("data.sas7bdat")?;
///     let rows = ds.scan().limit(5).collect_rows()?;
///     for row in &rows {
///         let id = row.get_by_name("ID", ds.columns());
///         println!("{}: {id:?}", row.row_index);
///     }
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone, Default)]
pub struct OwnedRow {
    /// The row's position in the file.
    pub row_index: crate::types::RowIndex,
    /// The cells, in projection order.
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
    /// Whether this is [`Null`](Self::Null).
    #[must_use]
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// The string, if this is [`Str`](Self::Str).
    #[must_use]
    pub const fn as_str(&self) -> Option<&'a str> {
        if let Self::Str(s) = self {
            Some(s)
        } else {
            None
        }
    }

    /// The value, if this is [`Float64`](Self::Float64).
    #[must_use]
    pub const fn as_f64(&self) -> Option<f64> {
        if let Self::Float64(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    /// The value, if this is [`Int32`](Self::Int32).
    #[must_use]
    pub const fn as_i32(&self) -> Option<i32> {
        if let Self::Int32(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    /// The value, if this is [`Int64`](Self::Int64).
    #[must_use]
    pub const fn as_i64(&self) -> Option<i64> {
        if let Self::Int64(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    /// The bytes, if this is [`Bytes`](Self::Bytes).
    #[must_use]
    pub const fn as_bytes(&self) -> Option<&'a [u8]> {
        if let Self::Bytes(b) = self {
            Some(b)
        } else {
            None
        }
    }

    /// The date, if this is [`Date`](Self::Date).
    #[must_use]
    pub const fn as_date(&self) -> Option<SasDate> {
        if let Self::Date(d) = self {
            Some(*d)
        } else {
            None
        }
    }

    /// The datetime, if this is [`DateTime`](Self::DateTime).
    #[must_use]
    pub const fn as_datetime(&self) -> Option<SasDateTime> {
        if let Self::DateTime(d) = self {
            Some(*d)
        } else {
            None
        }
    }

    /// The time, if this is [`Time`](Self::Time).
    #[must_use]
    pub const fn as_time(&self) -> Option<SasTime> {
        if let Self::Time(t) = self {
            Some(*t)
        } else {
            None
        }
    }

    /// Copies the cell into an [`OwnedCellValue`].
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
    /// Whether this is [`Null`](Self::Null).
    #[must_use]
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// The string, if this is [`String`](Self::String).
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        if let Self::String(s) = self {
            Some(s)
        } else {
            None
        }
    }

    /// The value, if this is [`Float64`](Self::Float64).
    #[must_use]
    pub const fn as_f64(&self) -> Option<f64> {
        if let Self::Float64(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    /// The value, if this is [`Int32`](Self::Int32).
    #[must_use]
    pub const fn as_i32(&self) -> Option<i32> {
        if let Self::Int32(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    /// The value, if this is [`Int64`](Self::Int64).
    #[must_use]
    pub const fn as_i64(&self) -> Option<i64> {
        if let Self::Int64(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    /// The bytes, if this is [`Bytes`](Self::Bytes).
    #[must_use]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        if let Self::Bytes(b) = self {
            Some(b)
        } else {
            None
        }
    }

    /// The date, if this is [`Date`](Self::Date).
    #[must_use]
    pub const fn as_date(&self) -> Option<SasDate> {
        if let Self::Date(d) = self {
            Some(*d)
        } else {
            None
        }
    }

    /// The datetime, if this is [`DateTime`](Self::DateTime).
    #[must_use]
    pub const fn as_datetime(&self) -> Option<SasDateTime> {
        if let Self::DateTime(d) = self {
            Some(*d)
        } else {
            None
        }
    }

    /// The time, if this is [`Time`](Self::Time).
    #[must_use]
    pub const fn as_time(&self) -> Option<SasTime> {
        if let Self::Time(t) = self {
            Some(*t)
        } else {
            None
        }
    }
}

// The `CellValue` conversions for the temporal newtypes live here, with the enum
// they build, rather than in `metadata` beside the newtypes. Both types are local
// so either module may host them, but putting them in `metadata` meant the module
// that *describes* a dataset had to import the module that *reads* one — the only
// edge in that direction, and enough to make `metadata` and `row` mutually
// dependent for three trivial constructors.
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
