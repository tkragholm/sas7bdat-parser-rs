// Tests for row selection, projection, and batch size edge cases.
// Covers:
//   §2.5 Window / skip+limit
//   §2.6 Duplicate column in projection (documents deduplication behaviour)
//   §2.7 Batch size edge cases

use sas7bdat::{BatchHint, Dataset, RowSelection};
use std::path::Path;

fn fixture(rel: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures")
        .join(rel)
}

// ─── §2.5 Window / skip + limit ──────────────────────────────────────────────

fn check_skip_limit(path: &Path, skip: u64, limit: u64) {
    if !path.exists() {
        return;
    }
    let ds = Dataset::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));

    let total_rows = ds.metadata().row_count;

    // Full scan as reference.
    let all_rows = ds.collect_rows().expect("collect all");

    // Windowed scan.
    let end = (skip + limit).min(total_rows);
    let windowed = ds
        .scan()
        .select(RowSelection::range(skip, end))
        .collect_rows()
        .expect("collect windowed");

    let expected_count = usize::try_from(end.saturating_sub(skip)).unwrap_or(0);
    assert_eq!(
        windowed.len(),
        expected_count,
        "{}: windowed row count wrong (skip={skip}, limit={limit})",
        path.display()
    );

    // First windowed row must match all_rows[skip].
    if expected_count > 0 && skip < total_rows {
        let skip_idx = usize::try_from(skip).unwrap();
        assert_eq!(
            windowed[0].cells,
            all_rows[skip_idx].cells,
            "{}: first windowed row (skip={skip}) doesn't match full scan row {skip_idx}",
            path.display()
        );
    }
}

#[test]
fn airline_skip_then_limit() {
    let path = fixture("raw_data/pandas/airline.sas7bdat");
    check_skip_limit(&path, 5, 10);
}

#[test]
fn airline_skip_past_end_returns_empty() {
    let path = fixture("raw_data/pandas/airline.sas7bdat");
    if !path.exists() {
        return;
    }
    let ds = Dataset::open(&path).expect("open");
    let total = ds.metadata().row_count;
    let windowed = ds
        .scan()
        .select(RowSelection::range(total + 10, total + 20))
        .collect_rows()
        .expect("windowed past end");
    assert!(
        windowed.is_empty(),
        "expected empty result for skip past end, got {} rows",
        windowed.len()
    );
}

#[test]
fn airline_limit_without_skip() {
    let path = fixture("raw_data/pandas/airline.sas7bdat");
    if !path.exists() {
        return;
    }
    let ds = Dataset::open(&path).expect("open");
    let limited = ds.scan().limit(3).collect_rows().expect("limit 3");
    assert_eq!(limited.len(), 3, "limit(3) should return exactly 3 rows");
}

#[test]
fn many_columns_skip_middle() {
    let path = fixture("raw_data/pandas/many_columns.sas7bdat");
    check_skip_limit(&path, 2, 5);
}

#[test]
fn compressed_skip_limit() {
    let path = fixture("raw_data/pandas/0x40controlbyte.sas7bdat");
    check_skip_limit(&path, 1, 3);
}

// ─── §2.6 Duplicate column in projection ─────────────────────────────────────

#[test]
fn duplicate_column_name_is_silently_deduplicated() {
    let path = fixture("raw_data/pandas/airline.sas7bdat");
    if !path.exists() {
        return;
    }
    let ds = Dataset::open(&path).expect("open");

    let first_col = ds.columns()[0].name.clone();

    // Build a projection that requests the same column twice.
    let projection = ds
        .projection()
        .columns([first_col.as_str(), first_col.as_str()])
        .build()
        .expect("build projection with duplicate");

    // The resulting projection should contain the column exactly once.
    assert_eq!(
        projection.columns().len(),
        1,
        "expected deduplication to 1 column, got {:?}",
        projection
            .columns()
            .iter()
            .map(|c| &c.name)
            .collect::<Vec<_>>()
    );

    let rows = ds
        .scan()
        .with_projection(&projection)
        .collect_rows()
        .expect("collect with deduplicated projection");

    // Each row should have exactly one cell.
    for row in &rows {
        assert_eq!(
            row.cells.len(),
            1,
            "expected 1 cell per row after deduplication"
        );
    }
}

// ─── §2.7 Batch size edge cases ──────────────────────────────────────────────

fn check_batch_sizes_invariant(path: &Path) {
    if !path.exists() {
        return;
    }
    let ds = Dataset::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));

    let total_rows = ds.metadata().row_count;

    let batch_hints = [
        BatchHint::Rows(1),
        BatchHint::Rows(usize::from(u16::MAX)),
        BatchHint::Rows(usize::MAX),
    ];

    for hint in batch_hints {
        let batches = ds
            .scan()
            .with_batch_hint(hint)
            .collect_batches()
            .unwrap_or_else(|e| panic!("{}: collect with {hint:?}: {e}", path.display()));

        let scanned_rows: u64 = batches.iter().map(|b| b.row_count as u64).sum();
        assert_eq!(
            scanned_rows,
            total_rows,
            "{}: total rows wrong with {hint:?} (expected {total_rows}, got {scanned_rows})",
            path.display()
        );
    }

    // Batch size 1: every batch must have exactly 1 row (except possibly the last).
    let single_batches = ds
        .scan()
        .with_batch_hint(BatchHint::Rows(1))
        .collect_batches()
        .expect("batch_size=1");
    for (i, batch) in single_batches.iter().enumerate() {
        assert!(
            batch.row_count >= 1,
            "{}: batch {i} is empty with batch_size=1",
            path.display()
        );
    }

    // Huge batch size: should produce a single batch covering all rows.
    let huge_batches = ds
        .scan()
        .with_batch_hint(BatchHint::Rows(usize::MAX))
        .collect_batches()
        .expect("batch_size=MAX");
    assert_eq!(
        huge_batches.len(),
        1,
        "{}: expected 1 batch with batch_size=MAX, got {}",
        path.display(),
        huge_batches.len()
    );
    assert_eq!(
        huge_batches[0].row_count as u64,
        total_rows,
        "{}: single huge batch has wrong row count",
        path.display()
    );
}

#[test]
fn airline_batch_sizes_invariant() {
    check_batch_sizes_invariant(&fixture("raw_data/pandas/airline.sas7bdat"));
}

#[test]
fn controlbyte_batch_sizes_invariant() {
    check_batch_sizes_invariant(&fixture("raw_data/pandas/0x40controlbyte.sas7bdat"));
}

#[test]
fn many_columns_batch_sizes_invariant() {
    check_batch_sizes_invariant(&fixture("raw_data/pandas/many_columns.sas7bdat"));
}
