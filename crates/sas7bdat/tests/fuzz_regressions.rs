//! Regressions for inputs `cargo fuzz` found (see `crates/sas7bdat/fuzz`).
//!
//! Every file under `tests/fixtures/fuzz/` is an input that once made the reader
//! misbehave. The contract asserted here is deliberately narrow: opening it must
//! *return*, with either `Ok` or `Err`. Aborting, hanging, or exhausting memory are
//! all failures, and none of them are things an `assert!` can catch — the test
//! catches them by the run dying instead of finishing.
//!
//! Drop new artifacts into that directory and they are covered automatically. Add an
//! entry to `EXPECTED` when the artifact's whole point is a specific diagnostic.

use sas7bdat::{Dataset, Parallelism};
use std::{fs, hint::black_box, ops::ControlFlow, path::PathBuf};

/// Enough rows to cross a page boundary and drive the decompressor, few enough that a
/// header claiming billions of rows does not turn the test into a hang.
const SCAN_ROW_CAP: usize = 4096;

/// Artifacts whose error message is itself part of the regression, as
/// `(file stem, substring the message must contain)`.
const EXPECTED: &[(&str, &str)] = &[
    // An 18 KB file declaring 1,085,348,864 columns, and a 32 KB one declaring
    // 2,863,311,531. Both used to reach `Vec::resize_with` and ask the allocator for
    // 2.5 GiB before any row was read; libFuzzer caught them as OOMs within 90
    // seconds of the target first running. The declared count is now refuted against
    // the file's own geometry, so they fail in constant space.
    ("oom_declared_column_count_8k", "columns"),
    ("oom_declared_column_count_32k", "columns"),
    // Found on the very next run, once the OOM above stopped truncating it. A numeric
    // column declared 67 bytes wide reached `numeric_bits`, whose `unreachable!` arm
    // panics — and panics in release too, because the `slice.len() <= 8` guard above it
    // is a `debug_assert!`. A panic out of a library is not a recoverable error for the
    // Python and R bindings, so the width is now rejected at open time.
    ("panic_numeric_width_over_8", "numeric but 67 bytes wide"),
    // A 44 KB file declaring a 4,261,413,064-byte row. `decompress_row` reserves the
    // declared row length per row, so the claim was an allocation primitive:
    // `malloc(4261413064)` on a file of a few KB. Only reachable through a scan, which
    // is why the loop below scans rather than stopping at open.
    ("oom_declared_row_length", "byte row"),
];

fn fuzz_fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fuzz")
}

#[test]
fn fuzz_artifacts_open_without_aborting_or_exhausting_memory() {
    let dir = fuzz_fixture_dir();
    let entries = fs::read_dir(&dir)
        .unwrap_or_else(|err| panic!("cannot read {}: {err}", dir.display()))
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|ext| ext == "sas7bdat")
        })
        .map(|entry| entry.path());

    let mut exercised = 0usize;
    for path in entries {
        let bytes =
            fs::read(&path).unwrap_or_else(|err| panic!("cannot read {}: {err}", path.display()));

        // Open *and* scan. Opening alone is not enough: the row-length artifact was
        // only reachable through decompression, several frames past `from_bytes`, so a
        // test that stops at open would not have exercised the decompressor at all.
        // This mirrors what the fuzz target does, which is what found these.
        //
        // The assertion is that the sequence returns at all. A regression
        // reintroducing an unbounded allocation does not fail here — it takes the
        // whole test process down, which is the signal.
        let outcome = Dataset::from_bytes(bytes).and_then(|dataset| {
            let mut seen = 0usize;
            dataset
                .scan()
                .with_parallelism(Parallelism::None)
                .visit_rows(|row| {
                    black_box(row.len());
                    seen += 1;
                    if seen >= SCAN_ROW_CAP {
                        return Ok(ControlFlow::Break(()));
                    }
                    Ok(ControlFlow::Continue(()))
                })
        });

        let stem = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or_default();
        if let Some((_, needle)) = EXPECTED.iter().find(|(name, _)| *name == stem) {
            let err = outcome.err().unwrap_or_else(|| {
                panic!("{stem} is expected to be rejected, but it parsed and scanned cleanly")
            });
            let message = err.to_string();
            assert!(
                message.contains(needle),
                "{stem}: expected the error to mention {needle:?}, got {message:?}"
            );
        }

        exercised += 1;
    }

    assert_eq!(
        exercised,
        EXPECTED.len(),
        "every entry in EXPECTED should correspond to a fixture on disk, and vice versa"
    );
}

/// The guard is a bound on *plausibility*, not a fixed limit, so it has to stay clear
/// of real files. The widest fixture in the corpus is 4,041 columns; the tightest
/// ratio of ceiling-to-actual measured across 233 fixtures was 26x. This pins the
/// direction of that margin: a normal file must not come near the bound.
#[test]
fn tracked_fixtures_are_unaffected_by_the_column_ceiling() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let dataset = Dataset::open(dir.join("people_nonascii.sas7bdat"))
        .expect("a well-formed fixture must still open with the ceiling in place");
    assert!(
        !dataset.columns().is_empty(),
        "the ceiling must not truncate a valid column table"
    );
}
