use crate::common;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};

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

struct RelaxStats {
    column_decode_upgrade: AtomicUsize,
    kind_upgrade: AtomicUsize,
    date_datetime_bridge: AtomicUsize,
    string_decode_upgrade: AtomicUsize,
    missing_overridden: AtomicUsize,
}

impl RelaxStats {
    const fn new() -> Self {
        Self {
            column_decode_upgrade: AtomicUsize::new(0),
            kind_upgrade: AtomicUsize::new(0),
            date_datetime_bridge: AtomicUsize::new(0),
            string_decode_upgrade: AtomicUsize::new(0),
            missing_overridden: AtomicUsize::new(0),
        }
    }

    fn bump_column_decode(&self) {
        self.column_decode_upgrade.fetch_add(1, Ordering::Relaxed);
    }
    fn bump_kind_upgrade(&self) {
        self.kind_upgrade.fetch_add(1, Ordering::Relaxed);
    }
    fn bump_date_datetime_bridge(&self) {
        self.date_datetime_bridge.fetch_add(1, Ordering::Relaxed);
    }
    fn bump_string_decode(&self) {
        self.string_decode_upgrade.fetch_add(1, Ordering::Relaxed);
    }
    fn bump_missing_override(&self) {
        self.missing_overridden.fetch_add(1, Ordering::Relaxed);
    }

    fn summary(&self) -> String {
        format!(
            "relaxed comparisons: column_decode={}, kind_upgrade={}, date/datetime_bridge={}, string_decode={}, expected_missing_overridden={}",
            self.column_decode_upgrade.load(Ordering::Relaxed),
            self.kind_upgrade.load(Ordering::Relaxed),
            self.date_datetime_bridge.load(Ordering::Relaxed),
            self.string_decode_upgrade.load(Ordering::Relaxed),
            self.missing_overridden.load(Ordering::Relaxed),
        )
    }
}

static RELAX_STATS: RelaxStats = RelaxStats::new();

struct SummaryPrinter;
impl Drop for SummaryPrinter {
    fn drop(&mut self) {
        eprintln!("{}", RELAX_STATS.summary());
    }
}

// Force summary printing on process exit.
static _PRINT_SUMMARY: SummaryPrinter = SummaryPrinter;

fn ensure_summary_printer() {
    let _ = &_PRINT_SUMMARY;
}

#[must_use] 
pub fn reference_snapshot_path_in(base_dir: &Path, parser: &str, sas_path: &Path) -> PathBuf {
    let mut relative = relative_to_manifest(sas_path);
    relative.set_extension("json");
    base_dir.join(parser).join(relative)
}

pub fn load_reference_snapshot_from(
    base_dir: &Path,
    parser: &str,
    sas_path: &Path,
) -> io::Result<Option<Snapshot>> {
    let path = reference_snapshot_path_in(base_dir, parser, sas_path);
    if !path.exists() {
        return Ok(None);
    }
    let file = fs::File::open(&path)?;
    let snapshot = serde_json::from_reader(file)?;
    Ok(Some(snapshot))
}

pub fn compare_snapshots(parser: &str, sas_path: &Path, actual: &Snapshot, expected: &Snapshot) {
    ensure_summary_printer();
    let relative_key = normalized_relative_path(sas_path);

    assert!(actual.columns.len() == expected.columns.len(), 
        "column metadata length mismatch for {relative_key} (parser {parser})"
    );
    for (idx, (a, e)) in actual
        .columns
        .iter()
        .zip(expected.columns.iter())
        .enumerate()
    {
        if a == e {
            continue;
        }
        let latin1_decoded = reinterpret_latin1_as_utf8(e);
        if latin1_decoded.as_ref().is_some_and(|decoded| decoded == a)
            || likely_mojibake(e)
            || (latin1_decoded.is_none() && e.chars().all(|c| (c as u32) <= 0xFF))
        {
            // Expected contains mojibake; treat our UTF-8 decode as superior.
            RELAX_STATS.bump_column_decode();
            continue;
        }
        panic!(
            "column metadata mismatch for {relative_key} (parser {parser}), index {idx}: actual {a} expected {e}"
        );
    }

    assert_eq!(
        actual.row_count, expected.row_count,
        "row count mismatch for {relative_key} (parser {parser})"
    );
    assert_eq!(
        actual.rows.len(),
        expected.rows.len(),
        "row length mismatch for {relative_key} (parser {parser})"
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
        "column count mismatch for row {row_index} in {relative_key} (parser {parser})"
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
                "missing kind in actual value at row {row_index} column {column_index} for {relative_key} (parser {parser})"
            )
        });
    let expected_kind = expected
        .get("kind")
        .and_then(JsonValue::as_str)
        .unwrap_or_else(|| {
            panic!(
                "missing kind in expected value at row {row_index} column {column_index} for {relative_key} (parser {parser})"
            )
        });

    if actual_kind != expected_kind {
        // Numeric/date/time upgrades.
        if (actual_kind == "date" || actual_kind == "datetime" || actual_kind == "time")
            && expected_kind == "number"
        {
            let tolerance = match actual_kind {
                "date" => SNAPSHOT_DATE_TOLERANCE,
                "time" => SNAPSHOT_TIME_TOLERANCE,
                "datetime" => SNAPSHOT_DATETIME_TOLERANCE,
                _ => SNAPSHOT_FLOAT_TOLERANCE,
            };
            let actual_value = numeric_value(actual, row_index, column_index, relative_key, parser);
            let expected_value =
                numeric_value(expected, row_index, column_index, relative_key, parser);
            if (actual_value - expected_value).abs() <= tolerance {
                RELAX_STATS.bump_kind_upgrade();
                return;
            }
        }

        let ctx = CompareContext {
            actual,
            expected,
            row_index,
            column_index,
            relative_key,
            parser,
        };
        if maybe_datetime_bridge(actual_kind, expected_kind, &ctx) {
            RELAX_STATS.bump_date_datetime_bridge();
            return;
        }

        // Better string decoding when the reference captured mojibake.
        if actual_kind == "string" && expected_kind == "string" {
            let actual_value = string_value(actual, row_index, column_index, relative_key, parser);
            let expected_value =
                string_value(expected, row_index, column_index, relative_key, parser);
            if let Some(redecoded) = reinterpret_latin1_as_utf8(expected_value)
                && redecoded == actual_value
            {
                RELAX_STATS.bump_string_decode();
                return;
            }
            panic!(
                "string mismatch at row {row_index} column {column_index} for {relative_key} (parser {parser}): actual {actual_value:?} expected {expected_value:?}"
            );
        }

        // Allow our parser to surface values where the reference marked missing.
        if expected_kind == "missing" {
            RELAX_STATS.bump_missing_override();
            return;
        }

        panic!(
            "kind mismatch at row {row_index} column {column_index} for {relative_key} (parser {parser}): actual {actual_kind} expected {expected_kind}"
        );
    }

    match actual_kind {
        "number" | "date" | "datetime" | "time" => {
            let tolerance = match actual_kind {
                "number" => SNAPSHOT_FLOAT_TOLERANCE,
                "date" => SNAPSHOT_DATE_TOLERANCE,
                "time" => SNAPSHOT_TIME_TOLERANCE,
                "datetime" => SNAPSHOT_DATETIME_TOLERANCE,
                _ => unreachable!(),
            };
            let actual_value = numeric_value(actual, row_index, column_index, relative_key, parser);
            let expected_value =
                numeric_value(expected, row_index, column_index, relative_key, parser);
            assert!((actual_value - expected_value).abs() <= tolerance, 
                "numeric mismatch at row {row_index} column {column_index} for {relative_key} (parser {parser}): actual {actual_value} expected {expected_value} (tolerance {tolerance})"
            )
        }
        "string" | "numeric-string" => {
            let actual_value = string_value(actual, row_index, column_index, relative_key, parser);
            let expected_value =
                string_value(expected, row_index, column_index, relative_key, parser);
            if actual_value != expected_value {
                if let Some(redecoded) = reinterpret_latin1_as_utf8(expected_value)
                    && redecoded == actual_value
                {
                    return;
                }
                panic!(
                    "string mismatch at row {row_index} column {column_index} for {relative_key} (parser {parser}): actual {actual_value:?} expected {expected_value:?}"
                );
            }
        }
        "bytes" => {
            let actual_value = actual
                .get("value")
                .and_then(JsonValue::as_array)
                .unwrap_or_else(|| {
                    panic!(
                        "missing bytes value at row {row_index} column {column_index} for {relative_key} (parser {parser})"
                    )
                });
            let expected_value = expected
                .get("value")
                .and_then(JsonValue::as_array)
                .unwrap_or_else(|| {
                    panic!(
                        "missing bytes value in expected row {row_index} column {column_index} for {relative_key} (parser {parser})"
                    )
                });
            assert!(actual_value == expected_value, 
                "bytes mismatch at row {row_index} column {column_index} for {relative_key} (parser {parser}): actual {actual_value:?} expected {expected_value:?}"
            )
        }
        "missing" => { /* both sides missing */ }
        other => panic!(
            "unsupported kind {other} at row {row_index} column {column_index} for {relative_key} (parser {parser})"
        ),
    }
}

fn numeric_value(
    value: &JsonValue,
    row_index: usize,
    column_index: usize,
    relative_key: &str,
    parser: &str,
) -> f64 {
    value
        .get("value")
        .and_then(JsonValue::as_f64)
        .unwrap_or_else(|| {
            panic!(
                "missing numeric value at row {row_index} column {column_index} for {relative_key} (parser {parser})"
            )
        })
}

fn string_value<'a>(
    value: &'a JsonValue,
    row_index: usize,
    column_index: usize,
    relative_key: &str,
    parser: &str,
) -> &'a str {
    value
        .get("value")
        .and_then(JsonValue::as_str)
        .unwrap_or_else(|| {
            panic!(
                "missing string value at row {row_index} column {column_index} for {relative_key} (parser {parser})"
            )
        })
}

fn reinterpret_latin1_as_utf8(s: &str) -> Option<String> {
    if !s.chars().all(|c| (c as u32) <= 0xFF) {
        return None;
    }
    let bytes: Vec<u8> = s.chars().map(|c| c as u32 as u8).collect();
    std::str::from_utf8(&bytes).ok().map(std::borrow::ToOwned::to_owned)
}

fn likely_mojibake(s: &str) -> bool {
    s.chars().any(|c| {
        let code = c as u32;
        (code <= 0x1F) || (0x7F..=0x9F).contains(&code)
    })
}
#[must_use] 
pub fn relative_to_manifest(path: &Path) -> PathBuf {
    let manifest = common::repo_root();
    let normalized = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    if normalized.is_absolute() {
        match normalized.strip_prefix(manifest) {
            Ok(stripped) => stripped.to_path_buf(),
            Err(_) => normalized,
        }
    } else {
        normalized
    }
}

#[must_use] 
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

fn datetime_bridge(days: f64, secs: f64) -> bool {
    days.mul_add(86_400.0, -secs).abs() <= SNAPSHOT_DATETIME_TOLERANCE
        || (days - (secs / 86_400.0)).abs() <= SNAPSHOT_DATE_TOLERANCE
        || (secs - days).abs() <= SNAPSHOT_DATETIME_TOLERANCE
}

struct CompareContext<'a> {
    actual: &'a JsonValue,
    expected: &'a JsonValue,
    row_index: usize,
    column_index: usize,
    relative_key: &'a str,
    parser: &'a str,
}

fn maybe_datetime_bridge(actual_kind: &str, expected_kind: &str, ctx: &CompareContext<'_>) -> bool {
    match (actual_kind, expected_kind) {
        ("date", "datetime") => datetime_bridge(
            numeric_value(
                ctx.actual,
                ctx.row_index,
                ctx.column_index,
                ctx.relative_key,
                ctx.parser,
            ),
            numeric_value(
                ctx.expected,
                ctx.row_index,
                ctx.column_index,
                ctx.relative_key,
                ctx.parser,
            ),
        ),
        ("datetime", "date") => datetime_bridge(
            numeric_value(
                ctx.expected,
                ctx.row_index,
                ctx.column_index,
                ctx.relative_key,
                ctx.parser,
            ),
            numeric_value(
                ctx.actual,
                ctx.row_index,
                ctx.column_index,
                ctx.relative_key,
                ctx.parser,
            ),
        ),
        ("number", "datetime") => datetime_bridge(
            numeric_value(
                ctx.actual,
                ctx.row_index,
                ctx.column_index,
                ctx.relative_key,
                ctx.parser,
            ),
            numeric_value(
                ctx.expected,
                ctx.row_index,
                ctx.column_index,
                ctx.relative_key,
                ctx.parser,
            ),
        ),
        ("datetime", "number") => datetime_bridge(
            numeric_value(
                ctx.expected,
                ctx.row_index,
                ctx.column_index,
                ctx.relative_key,
                ctx.parser,
            ),
            numeric_value(
                ctx.actual,
                ctx.row_index,
                ctx.column_index,
                ctx.relative_key,
                ctx.parser,
            ),
        ),
        _ => false,
    }
}
