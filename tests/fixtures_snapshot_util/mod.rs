#![allow(dead_code, clippy::pedantic)]
use std::path::{Path, PathBuf};

use sas7bdat_parser_rs::SasFile;

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
