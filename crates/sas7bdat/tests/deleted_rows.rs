//! SAS deletes a row by marking it, not by removing it. The row stays on the page and stays
//! counted by the header, and a trailing bitmap says which rows are tombstones. A reader that
//! does not consult that bitmap returns deleted rows as live data, silently.
//!
//! Expected counts here were taken from `ReadStat` built at `da9fcaa`, which is after
//! `WizardMac/ReadStat#366` added deleted-row support. `ReadStat` 1.1.9 (what `haven` and
//! `pyreadstat` ship) predates it and over-reports on all three of these files, as did this
//! crate before the bitmap was read.

use sas7bdat::{Dataset, OwnedCellValue};
use std::path::{Path, PathBuf};

fn fixture(rel: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures")
        .join(rel)
}

/// `(fixture, rows the header claims, rows actually deleted)`. Live rows are the difference.
const DELETED_ROW_FIXTURES: &[(&str, u64, u64)] = &[
    ("raw_data/csharp/all_rand_normal.sas7bdat", 37, 0),
    (
        "raw_data/csharp/all_rand_normal_with_deleted.sas7bdat",
        37,
        1,
    ),
    (
        "raw_data/csharp/all_rand_normal_with_deleted2.sas7bdat",
        37,
        5,
    ),
    ("raw_data/csharp/data_page_with_deleted.sas7bdat", 998, 1),
    // Not named for it, and not known to carry deletions until the whole corpus was
    // re-measured. Nine of its rows are tombstones.
    ("raw_data/pandas/load_log.sas7bdat", 2097, 9),
    // RLE-compressed, and the tombstone is the subheader pointer's own compression code
    // (0x05) rather than a page bitmap. Its table name says what it is:
    // `TEST_4_COMP_3RDROW_REMOVED`.
    ("raw_data/csharp/comp_deleted.sas7bdat", 4, 1),
];

#[test]
fn deleted_rows_are_counted_but_not_delivered() {
    for (rel, declared, deleted) in DELETED_ROW_FIXTURES {
        let path = fixture(rel);
        if !path.exists() {
            continue;
        }
        let ds = Dataset::open(&path).expect("open dataset");
        let meta = ds.metadata();

        assert_eq!(meta.row_count, *declared, "{rel}: header row count");
        assert_eq!(meta.deleted_row_count, *deleted, "{rel}: deleted row count");

        let delivered = ds.collect_rows().expect("collect rows").len() as u64;
        assert_eq!(
            delivered,
            declared - deleted,
            "{rel}: a scan must deliver only live rows"
        );
    }
}

/// The rows that survive have to be the *right* rows, not merely the right number of them.
/// A bitmap read with the wrong bit order, or off by a byte, still drops the correct count.
///
/// `x1` in these files is a ladder (0..10, then 20..100 by tens, then 100..1000, then
/// 1000..9000), so the surviving values name the deleted rows exactly. Golden values are
/// `ReadStat` @ `da9fcaa`: one row deleted from the first file (`50`), five from the second
/// (`0`, `9`, `50`, `500`, `3000`).
#[test]
fn the_surviving_rows_are_the_live_ones() {
    const CASES: &[(&str, &[i64])] = &[
        (
            "raw_data/csharp/all_rand_normal_with_deleted.sas7bdat",
            &[
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 60, 70, 80, 90, 100, 200, 300, 400,
                500, 600, 700, 800, 900, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000,
            ],
        ),
        (
            "raw_data/csharp/all_rand_normal_with_deleted2.sas7bdat",
            &[
                1, 2, 3, 4, 5, 6, 7, 8, 10, 20, 30, 40, 60, 70, 80, 90, 100, 200, 300, 400, 600,
                700, 800, 900, 1000, 2000, 4000, 5000, 6000, 7000, 8000, 9000,
            ],
        ),
    ];

    for (rel, expected) in CASES {
        let path = fixture(rel);
        if !path.exists() {
            continue;
        }
        let rows = Dataset::open(&path)
            .expect("open dataset")
            .collect_rows()
            .expect("collect rows");
        let actual: Vec<i64> = rows
            .iter()
            .map(|row| match row.cells.first() {
                Some(OwnedCellValue::Int64(value)) => *value,
                Some(OwnedCellValue::Int32(value)) => i64::from(*value),
                #[allow(clippy::cast_possible_truncation)]
                Some(OwnedCellValue::Float64(value)) => *value as i64,
                other => panic!("{rel}: unexpected first cell {other:?}"),
            })
            .collect();
        assert_eq!(actual, *expected, "{rel}: surviving rows");
    }
}

/// A file with no deletions must not be pushed off the fused decode path, which is where the
/// throughput comes from. The bitmap is gated on the file-level count for exactly this reason.
#[test]
fn a_file_without_deletions_reports_none() {
    let path = fixture("raw_data/csharp/all_rand_normal.sas7bdat");
    if !path.exists() {
        return;
    }
    let ds = Dataset::open(&path).expect("open dataset");
    assert_eq!(ds.metadata().deleted_row_count, 0);
    assert_eq!(
        ds.collect_rows().expect("rows").len() as u64,
        ds.metadata().row_count
    );
}
