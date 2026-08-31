use crate::labels::LabelSet;
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

impl LogicalType {
    /// Whether the column's bytes are a SAS numeric.
    ///
    /// True for every variant that decodes through the numeric path, temporal types
    /// included: SAS stores dates, datetimes, and times as numbers, and the logical
    /// type only records how to interpret them afterwards. The two false cases are
    /// [`String`](Self::String) and [`Bytes`](Self::Bytes), which are read as raw
    /// character data.
    #[must_use]
    pub const fn is_numeric(self) -> bool {
        match self {
            Self::Integer | Self::Float | Self::Date | Self::DateTime | Self::Time => true,
            Self::String | Self::Bytes => false,
        }
    }
}

/// A wrapper for SAS date values (days since 1960-01-01).
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct SasDate {
    pub days_since_sas_epoch: i32,
}

impl SasDate {
    /// Days between the SAS epoch (1960-01-01) and the Unix epoch (1970-01-01).
    pub const DAYS_SAS_TO_UNIX: i32 = 3653;

    /// Days since the Unix epoch (1970-01-01) — the encoding Arrow `Date32` expects.
    #[must_use]
    pub const fn unix_days(self) -> i32 {
        self.days_since_sas_epoch - Self::DAYS_SAS_TO_UNIX
    }
}

/// A wrapper for SAS datetime values (seconds since 1960-01-01).
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct SasDateTime {
    pub seconds_since_sas_epoch: i64,
}

impl SasDateTime {
    /// Seconds between the SAS epoch (1960-01-01) and the Unix epoch (1970-01-01).
    pub const SECONDS_SAS_TO_UNIX: i64 = 315_619_200;

    /// Seconds since the Unix epoch (1970-01-01) — the encoding Arrow timestamps expect.
    #[must_use]
    pub const fn unix_seconds(self) -> i64 {
        self.seconds_since_sas_epoch - Self::SECONDS_SAS_TO_UNIX
    }
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
    /// The character encoding (e.g., `UTF-8`, `WINDOWS-1252`).
    pub encoding: Option<String>,
    /// The byte order of the file (`LittleEndian` for Windows/Linux, `BigEndian` for Unix).
    pub endianness: Endianness,
    /// Size of a single data page in bytes.
    pub page_size: u32,
    /// Total number of pages in the file (including metadata).
    pub page_count: u64,
    /// Total number of logical rows the header records, tombstones included.
    ///
    /// SAS deletes a row by marking it, not by removing it, so this stays at the value it had
    /// before the deletion and is **not** the number of rows a scan delivers. Subtract
    /// [`Self::deleted_row_count`], or read [`crate::ScanStatsSummary`] after a scan, for that.
    pub row_count: u64,
    /// How many of [`Self::row_count`] are deleted, and so are skipped by every scan.
    ///
    /// Zero for almost every file. Non-zero only where a SAS session deleted rows in place.
    pub deleted_row_count: u64,
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
    /// The *name* of the SAS format assigned to this column, e.g. `DATE` or `DATETIME`.
    ///
    /// Bare, without the width and decimal count. SAS keeps those in two separate fields,
    /// exposed as [`Self::format_width`] and [`Self::format_digits`]; use
    /// [`Self::format_spec`] to get them assembled the way SAS writes a format.
    pub format: Option<String>,
    /// The `w` of a `NAMEw.d` format. Zero when the format carries no width, which is a real
    /// distinction: `DATE` and `DATE9.` are different formats.
    pub format_width: u16,
    /// The `d` of a `NAMEw.d` format. Zero when absent. This is where the sub-second
    /// precision of a `DATETIME23.3` lives.
    pub format_digits: u16,
}

impl ColumnMeta {
    /// The full SAS format, name and width and decimals together, as `DATETIME23.3`.
    ///
    /// `None` only when the column has no format at all. A format with a width but **no
    /// name** is not nothing: SAS writes the plain numeric `w.d` format that way, so a
    /// column stored with width 4 and 2 decimals and no name has the format `4.2`. Reading
    /// the name alone reports such a column as unformatted.
    ///
    /// The width is omitted when it is zero, and the decimals are omitted unless a width
    /// was written, which is the order SAS itself uses: there is no `DATE.3`.
    #[must_use]
    pub fn format_spec(&self) -> Option<String> {
        let name = self.format.as_deref().unwrap_or_default().trim();
        if name.is_empty() && self.format_width == 0 {
            return None;
        }
        if self.format_width == 0 {
            return Some(name.to_owned());
        }
        Some(if self.format_digits == 0 {
            format!("{name}{}", self.format_width)
        } else {
            format!("{name}{}.{}", self.format_width, self.format_digits)
        })
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod epoch_tests {
    use super::{SasDate, SasDateTime};

    #[test]
    fn date_offset_anchors_on_unix_epoch() {
        // 1970-01-01 is SAS day 3653 and must map to Arrow Date32 value 0.
        assert_eq!(
            SasDate {
                days_since_sas_epoch: 3653
            }
            .unix_days(),
            0
        );
        // 2000-01-01 is SAS day 14610 and Unix day 10957.
        assert_eq!(
            SasDate {
                days_since_sas_epoch: 14610
            }
            .unix_days(),
            10957
        );
    }

    #[test]
    fn datetime_offset_anchors_on_unix_epoch() {
        // 1970-01-01T00:00:00 is SAS second 315_619_200 and must map to timestamp 0.
        assert_eq!(
            SasDateTime {
                seconds_since_sas_epoch: 315_619_200
            }
            .unix_seconds(),
            0
        );
        // 2000-01-01T00:00:00 UTC is Unix second 946_684_800.
        assert_eq!(
            SasDateTime {
                seconds_since_sas_epoch: 1_262_304_000
            }
            .unix_seconds(),
            946_684_800
        );
    }
}
