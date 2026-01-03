#![allow(dead_code, clippy::pedantic)]
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use csv::ReaderBuilder;
use sas7bdat::SasFile;
use sas7bdat::parser::{ColumnKind, NumericKind, parse_metadata};
use serde_json::json;

use super::reference::{Snapshot, relative_to_manifest};

#[path = "../common/mod.rs"]
mod common;

use common::value_to_json;

pub const SKIP_FIXTURES: &[&str] = &[
    "fixtures/raw_data/pandas/corrupt.sas7bdat",
    "fixtures/raw_data/pandas/zero_variables.sas7bdat",
    "fixtures/raw_data/csharp/54-class.sas7bdat",
    "fixtures/raw_data/csharp/54-cookie.sas7bdat",
    "fixtures/raw_data/csharp/charset_zpce.sas7bdat",
    "fixtures/raw_data/csharp/date_format_dtdate.sas7bdat",
    "fixtures/raw_data/csharp/date_formats.sas7bdat",
    "fixtures/raw_data/ahs2013/topical.sas7bdat",
];

pub fn should_skip(path: &Path) -> bool {
    let normalized = relative_to_manifest(path)
        .iter()
        .map(|component| component.to_string_lossy())
        .collect::<Vec<_>>()
        .join("/");
    SKIP_FIXTURES.contains(&normalized.as_str())
}

pub fn absolute_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        Path::new(env!("CARGO_MANIFEST_DIR")).join(path)
    }
}

pub fn snapshot_name(path: &Path) -> String {
    relative_to_manifest(path)
        .iter()
        .map(|component| component.to_string_lossy())
        .collect::<Vec<_>>()
        .join("__")
        .replace('.', "_")
}

pub fn collect_snapshot(path: &Path) -> Snapshot {
    let display = path.display();
    let mut sas =
        SasFile::open(path).unwrap_or_else(|err| panic!("failed to open {}: {}", display, err));
    let metadata = sas.metadata().clone();
    let columns = metadata
        .variables
        .iter()
        .map(|var| var.name.trim_end().to_string())
        .collect();

    let mut rows_iter = sas
        .rows()
        .unwrap_or_else(|err| panic!("failed to create row iterator for {}: {}", display, err));
    let mut rows = Vec::new();
    while let Some(row) = rows_iter
        .try_next()
        .unwrap_or_else(|err| panic!("error reading row from {}: {}", display, err))
    {
        let json_row = row.iter().map(value_to_json).collect();
        rows.push(json_row);
    }

    Snapshot {
        columns,
        row_count: metadata.row_count as usize,
        rows,
    }
}

static READSTAT_AVAILABLE: OnceLock<bool> = OnceLock::new();
static READSTAT_TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn readstat_available() -> bool {
    *READSTAT_AVAILABLE.get_or_init(|| Command::new("readstat").arg("--version").output().is_ok())
}

pub fn collect_readstat_snapshot(path: &Path) -> Snapshot {
    let display = path.display();
    let display_str = display.to_string();
    let mut file =
        fs::File::open(path).unwrap_or_else(|err| panic!("failed to open {}: {}", display, err));
    let parsed = parse_metadata(&mut file)
        .unwrap_or_else(|err| panic!("failed to parse metadata for {}: {}", display, err));
    let column_kinds: Vec<ColumnKind> = parsed.columns.iter().map(|col| col.kind).collect();

    let temp_path = readstat_temp_path();
    let output = Command::new("readstat")
        .arg(path)
        .arg(&temp_path)
        .output()
        .unwrap_or_else(|err| panic!("failed to run readstat for {}: {}", display, err));
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!(
            "readstat failed for {}: status {:?}\nstdout: {}\nstderr: {}",
            display, output.status, stdout, stderr
        );
    }

    let csv_file = fs::File::open(&temp_path)
        .unwrap_or_else(|err| panic!("failed to read readstat output {}: {}", display, err));
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(csv_file);
    let headers = reader
        .headers()
        .unwrap_or_else(|err| panic!("failed to read headers for {}: {}", display, err))
        .iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();

    let mut rows = Vec::new();
    for record in reader.records() {
        let record =
            record.unwrap_or_else(|err| panic!("failed to read row for {}: {}", display, err));
        let mut row = Vec::with_capacity(record.len());
        for (idx, field) in record.iter().enumerate() {
            let kind = column_kinds
                .get(idx)
                .unwrap_or_else(|| panic!("missing column kind {} for {}", idx, display));
            row.push(readstat_field_to_json(
                field,
                *kind,
                &display_str,
                idx,
            ));
        }
        rows.push(row);
    }

    let _ = fs::remove_file(&temp_path);

    Snapshot {
        columns: headers,
        row_count: rows.len(),
        rows,
    }
}

fn readstat_temp_path() -> PathBuf {
    let count = READSTAT_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut path = std::env::temp_dir();
    path.push(format!(
        "sas7bdat-readstat-{}-{}.csv",
        std::process::id(),
        count
    ));
    path
}

fn readstat_field_to_json(field: &str, kind: ColumnKind, display: &str, index: usize) -> serde_json::Value {
    if field.is_empty() || field == "." {
        return match kind {
            ColumnKind::Character => json!({ "kind": "string", "value": "" }),
            ColumnKind::Numeric(_) => json!({ "kind": "missing", "value": null }),
        };
    }

    match kind {
        ColumnKind::Character => json!({ "kind": "string", "value": field }),
        ColumnKind::Numeric(numeric_kind) => {
            let parsed = field.parse::<f64>().unwrap_or_else(|err| {
                panic!(
                    "failed to parse numeric field '{}' (column {}) for {}: {}",
                    field, index, display, err
                )
            });
            let kind_label = match numeric_kind {
                NumericKind::Double => "number",
                NumericKind::Date => "date",
                NumericKind::DateTime => "datetime",
                NumericKind::Time => "time",
            };
            json!({ "kind": kind_label, "value": parsed })
        }
    }
}
