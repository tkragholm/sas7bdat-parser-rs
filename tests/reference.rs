use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

pub const SNAPSHOT_FLOAT_TOLERANCE: f64 = 1e-4;
pub const SNAPSHOT_DATE_TOLERANCE: f64 = 1e-4;
pub const SNAPSHOT_TIME_TOLERANCE: f64 = 1e-4;
pub const SNAPSHOT_DATETIME_TOLERANCE: f64 = 1.5;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub columns: Vec<String>,
    pub row_count: usize,
    pub rows: Vec<Vec<JsonValue>>,
}

pub fn reference_snapshot_path(parser: &str, sas_path: &Path) -> PathBuf {
    let mut relative = relative_to_manifest(sas_path);
    relative.set_extension("json");
    manifest_dir()
        .join("tests")
        .join("reference")
        .join(parser)
        .join(relative)
}

pub fn load_reference_snapshot(parser: &str, sas_path: &Path) -> io::Result<Option<Snapshot>> {
    let path = reference_snapshot_path(parser, sas_path);
    if !path.exists() {
        return Ok(None);
    }
    let file = fs::File::open(&path)?;
    let snapshot = serde_json::from_reader(file)?;
    Ok(Some(snapshot))
}

pub fn compare_snapshots(parser: &str, sas_path: &Path, actual: &Snapshot, expected: &Snapshot) {
    let relative_key = normalized_relative_path(sas_path);

    assert_eq!(
        actual.columns, expected.columns,
        "column metadata mismatch for {} (parser {})",
        relative_key, parser
    );
    assert_eq!(
        actual.row_count, expected.row_count,
        "row count mismatch for {} (parser {})",
        relative_key, parser
    );
    assert_eq!(
        actual.rows.len(),
        expected.rows.len(),
        "row length mismatch for {} (parser {})",
        relative_key,
        parser
    );

    for (row_index, (actual_row, expected_row)) in
        actual.rows.iter().zip(expected.rows.iter()).enumerate()
    {
        compare_rows(parser, &relative_key, row_index, actual_row, expected_row);
    }
}

fn compare_rows(
    parser: &str,
    relative_key: &str,
    row_index: usize,
    actual: &[JsonValue],
    expected: &[JsonValue],
) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "column count mismatch for row {} in {} (parser {})",
        row_index,
        relative_key,
        parser
    );

    for (column_index, (actual_value, expected_value)) in
        actual.iter().zip(expected.iter()).enumerate()
    {
        compare_cell(
            parser,
            relative_key,
            row_index,
            column_index,
            actual_value,
            expected_value,
        );
    }
}

fn compare_cell(
    parser: &str,
    relative_key: &str,
    row_index: usize,
    column_index: usize,
    actual: &JsonValue,
    expected: &JsonValue,
) {
    let actual_kind = actual
        .get("kind")
        .and_then(JsonValue::as_str)
        .unwrap_or_else(|| {
            panic!(
                "missing kind in actual value at row {} column {} for {} (parser {})",
                row_index, column_index, relative_key, parser
            )
        });
    let expected_kind = expected
        .get("kind")
        .and_then(JsonValue::as_str)
        .unwrap_or_else(|| {
            panic!(
                "missing kind in expected value at row {} column {} for {} (parser {})",
                row_index, column_index, relative_key, parser
            )
        });

    if actual_kind != expected_kind {
        panic!(
            "kind mismatch at row {} column {} for {} (parser {}): actual {} expected {}",
            row_index, column_index, relative_key, parser, actual_kind, expected_kind
        );
    }

    match actual_kind {
        kind @ ("number" | "date" | "datetime" | "time") => {
            let tolerance = match kind {
                "number" => SNAPSHOT_FLOAT_TOLERANCE,
                "date" => SNAPSHOT_DATE_TOLERANCE,
                "time" => SNAPSHOT_TIME_TOLERANCE,
                "datetime" => SNAPSHOT_DATETIME_TOLERANCE,
                _ => unreachable!(),
            };
            let actual_value = actual
                .get("value")
                .and_then(JsonValue::as_f64)
                .unwrap_or_else(|| {
                    panic!(
                        "missing numeric value at row {} column {} for {} (parser {})",
                        row_index, column_index, relative_key, parser
                    )
                });
            let expected_value = expected
                .get("value")
                .and_then(JsonValue::as_f64)
                .unwrap_or_else(|| {
                    panic!(
                        "missing numeric value in expected row {} column {} for {} (parser {})",
                        row_index, column_index, relative_key, parser
                    )
                });
            if (actual_value - expected_value).abs() > tolerance {
                panic!(
                    "numeric mismatch at row {} column {} for {} (parser {}): actual {} expected {} (tolerance {})",
                    row_index,
                    column_index,
                    relative_key,
                    parser,
                    actual_value,
                    expected_value,
                    tolerance
                );
            }
        }
        "string" | "numeric-string" => {
            let actual_value = actual
                .get("value")
                .and_then(JsonValue::as_str)
                .unwrap_or_else(|| {
                    panic!(
                        "missing string value at row {} column {} for {} (parser {})",
                        row_index, column_index, relative_key, parser
                    )
                });
            let expected_value = expected
                .get("value")
                .and_then(JsonValue::as_str)
                .unwrap_or_else(|| {
                    panic!(
                        "missing string value in expected row {} column {} for {} (parser {})",
                        row_index, column_index, relative_key, parser
                    )
                });
            if actual_value != expected_value {
                panic!(
                    "string mismatch at row {} column {} for {} (parser {}): actual {:?} expected {:?}",
                    row_index, column_index, relative_key, parser, actual_value, expected_value
                );
            }
        }
        "bytes" => {
            let actual_value = actual
                .get("value")
                .and_then(JsonValue::as_array)
                .unwrap_or_else(|| {
                    panic!(
                        "missing bytes value at row {} column {} for {} (parser {})",
                        row_index, column_index, relative_key, parser
                    )
                });
            let expected_value = expected
                .get("value")
                .and_then(JsonValue::as_array)
                .unwrap_or_else(|| {
                    panic!(
                        "missing bytes value in expected row {} column {} for {} (parser {})",
                        row_index, column_index, relative_key, parser
                    )
                });
            if actual_value != expected_value {
                panic!(
                    "bytes mismatch at row {} column {} for {} (parser {})",
                    row_index, column_index, relative_key, parser
                );
            }
        }
        "missing" => { /* both sides missing */ }
        other => panic!(
            "unsupported kind {} at row {} column {} for {} (parser {})",
            other, row_index, column_index, relative_key, parser
        ),
    }
}

pub fn relative_to_manifest(path: &Path) -> PathBuf {
    let manifest = manifest_dir();
    if path.is_absolute() {
        path.strip_prefix(manifest)
            .map(PathBuf::from)
            .unwrap_or_else(|_| path.to_path_buf())
    } else {
        path.to_path_buf()
    }
}

pub fn normalized_relative_path(path: &Path) -> String {
    let relative = relative_to_manifest(path);
    path_components_to_string(&relative)
}

fn path_components_to_string(path: &Path) -> String {
    path.iter()
        .map(|component| component.to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}
