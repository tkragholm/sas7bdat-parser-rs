//! Shared JSON representation of SAS dataset/column metadata.
//!
//! One definition serves two outputs so they never drift:
//! - `inspect --json` prints it to stdout.
//! - `convert --parquet-metadata` embeds it into the Parquet file's key-value metadata
//!   under the [`PARQUET_METADATA_KEY`] key.
//!
//! The shape mirrors what `readstat-rs` exposes (per-column name/label/kind/format/width),
//! so downstream tools can read column labels and SAS formats straight from the Parquet file.

use sas7bdat::{ColumnMeta, Dataset};
use serde::Serialize;

/// Key under which the dataset metadata JSON is stored in Parquet file-level metadata.
pub const PARQUET_METADATA_KEY: &str = "sas7bdat.metadata";

/// JSON view of a single column's metadata.
#[derive(Serialize)]
pub struct ColumnMetaJson {
    /// 0-based position of the column in the source dataset.
    pub index: usize,
    /// Variable name (e.g. `"GENDER"`).
    pub name: String,
    /// Logical type, lowercased (e.g. `"string"`, `"date"`, `"float"`).
    pub kind: String,
    /// On-disk width of the column in bytes.
    pub width: u32,
    /// Optional descriptive label (e.g. `"Patient Gender"`).
    pub label: Option<String>,
    /// Optional SAS format (e.g. `"DATE9."`).
    pub format: Option<String>,
}

impl ColumnMetaJson {
    #[must_use]
    pub fn from_column(column: &ColumnMeta) -> Self {
        Self {
            index: column.index,
            name: column.name.clone(),
            kind: format!("{:?}", column.logical_type).to_lowercase(),
            width: column.physical_width,
            label: column.label.clone(),
            format: column.format.clone(),
        }
    }
}

/// JSON view of a dataset's metadata plus its (written) columns.
#[derive(Serialize)]
pub struct DatasetMetaJson {
    /// Total number of logical rows in the source dataset.
    pub row_count: u64,
    /// Number of columns described here (the written/selected columns, not the file total).
    pub column_count: usize,
    /// Table name from the header (usually the source filename).
    pub table_name: Option<String>,
    /// Optional descriptive label for the dataset.
    pub file_label: Option<String>,
    /// Character encoding (e.g. `"UTF-8"`).
    pub encoding: Option<String>,
    /// Per-column metadata, in output order.
    pub columns: Vec<ColumnMetaJson>,
}

impl DatasetMetaJson {
    /// Build the payload describing `columns` — the columns actually written/selected, in
    /// output order. `column_count` reflects that slice, not the full file width.
    #[must_use]
    pub fn new(dataset: &Dataset, columns: &[&ColumnMeta]) -> Self {
        let meta = dataset.metadata();
        Self {
            row_count: meta.row_count,
            column_count: columns.len(),
            table_name: meta.table_name.clone(),
            file_label: meta.file_label.clone(),
            encoding: meta.encoding.clone(),
            columns: columns
                .iter()
                .copied()
                .map(ColumnMetaJson::from_column)
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ColumnMetaJson;
    use sas7bdat::{ColumnMeta, LogicalType};

    fn column(name: &str, ty: LogicalType, label: Option<&str>, format: Option<&str>) -> ColumnMeta {
        ColumnMeta {
            index: 7,
            name: name.to_owned(),
            logical_type: ty,
            physical_width: 12,
            offset: 0,
            label: label.map(str::to_owned),
            format: format.map(str::to_owned),
        }
    }

    #[test]
    fn maps_fields_and_lowercases_kind() {
        let col = column("GENDER", LogicalType::String, Some("Patient Gender"), Some("$SEX."));
        let json = ColumnMetaJson::from_column(&col);
        assert_eq!(json.index, 7);
        assert_eq!(json.name, "GENDER");
        assert_eq!(json.kind, "string"); // Debug-derived "String" lowercased
        assert_eq!(json.width, 12);
        assert_eq!(json.label.as_deref(), Some("Patient Gender"));
        assert_eq!(json.format.as_deref(), Some("$SEX."));
    }

    #[test]
    fn missing_label_and_format_serialize_as_null() {
        let col = column("DOB", LogicalType::Date, None, None);
        let value = serde_json::to_value(ColumnMetaJson::from_column(&col)).expect("serialize");
        assert_eq!(value["kind"], "date");
        assert!(value["label"].is_null());
        assert!(value["format"].is_null());
    }
}
