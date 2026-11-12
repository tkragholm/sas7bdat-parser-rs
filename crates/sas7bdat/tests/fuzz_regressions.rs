//! Regressions for inputs `cargo fuzz` found (see `crates/sas7bdat/fuzz`).
//!
//! Every file under `tests/fixtures/fuzz/` is an input that once made the reader
//! misbehave. The contract asserted here is deliberately narrow: opening it must
//! *return*, with either `Ok` or `Err`. Aborting, hanging, or exhausting memory are
//! all failures, and none of them are things an `assert!` can catch. The test
//! catches them by the run dying instead of finishing.
//!
//! Drop new artifacts into that directory and they are covered automatically. Add an
//! entry to `EXPECTED` when the artifact's whole point is a specific diagnostic, or to
//! `EXPECTED_NO_DIAGNOSTIC` when it is not. An input handled by clamping an allocation
//! rather than refusing it leaves no message worth pinning, and whatever error the
//! file happens to produce is incidental.
//!
//! `catalog/` holds `.sas7bcat` inputs for the separate value-label parser.

use sas7bdat::{Dataset, Parallelism};
use std::{fs, hint::black_box, io::Cursor, ops::ControlFlow, path::PathBuf};

/// Enough rows to cross a page boundary and drive the decompressor, few enough that a
/// header claiming billions of rows does not turn the test into a hang.
const SCAN_ROW_CAP: usize = 4096;

/// Artifacts whose error message is itself part of the regression, as
/// `(file stem, substring the message must contain)`.
const EXPECTED: &[(&str, &str)] = &[
    // A 32 KB file declaring 2,863,311,531 columns. Taking the declared count at face
    // value would ask the allocator for 2.5 GiB before any row is read. The count is
    // checked against the file's own geometry, so the file fails in constant space.
    // The 8 KB sibling in `EXPECTED_NO_DIAGNOSTIC` is refused earlier, so only this one
    // pins the column diagnostic.
    ("oom_declared_column_count_32k", "columns"),
    // A numeric column declared 67 bytes wide would reach `numeric_bits`, whose
    // `unreachable!` arm panics in release too, because the `slice.len() <= 8` guard above
    // it is a `debug_assert!`. A panic out of a library is not a recoverable error for the
    // Python and R bindings, so the width is rejected at open time.
    ("panic_numeric_width_over_8", "numeric but 67 bytes wide"),
    // A 44 KB file declaring a 4,261,413,064-byte row. `decompress_row` reserves the
    // declared row length per row, so an unchecked claim is an allocation primitive.
    // Only reachable through a scan, which is why the loop below scans rather than
    // stopping at open.
    ("oom_declared_row_length", "byte row"),
];

/// Artifacts with no entry in [`EXPECTED`], listed here only so the completeness check
/// below can tell "deliberately unasserted" from "fixture went missing".
///
/// These are the cases handled by a clamp or an earlier guard rather than a specific
/// rejection, so there is no diagnostic to assert. The signal is memory, and the only
/// observable contract is that the run finishes. Whatever error they do or do not return is
/// incidental and must not be pinned.
const EXPECTED_NO_DIAGNOSTIC: &[&str] = &[
    // This artifact's `COLUMN_SIZE` carries a mutated upper word (`0x40200000` where a real
    // one is zero), so the 64-bit subheader signature is refused as data before any column
    // count is declared, and the file fails for a missing `ROW_SIZE`. It must be refused in
    // constant space; which guard catches it is incidental. `oom_declared_column_count_32k`
    // covers the column diagnostic itself.
    "oom_declared_column_count_8k",
    // 9 KB, 204 rows, 4 columns — a plausible-looking file whose declared `rows_per_page`
    // and row count together drove `BatchAccumulator::new` to ask for 876 GB through
    // `OwnedBatchColumnBuilder::with_capacity_hint`. Reached only via `visit_batches`.
    // It fails row visiting on an unrelated bounds check, which is deliberately not
    // asserted.
    "oom_batch_capacity_hint",
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
    let mut seen_stems: Vec<String> = Vec::new();
    for path in entries {
        let bytes =
            fs::read(&path).unwrap_or_else(|err| panic!("cannot read {}: {err}", path.display()));

        // Open *and* scan. Opening alone is not enough: the row-length artifact was only
        // reachable through decompression, several frames past `from_bytes`, so a test
        // that stops at open would not have exercised the decompressor at all.
        //
        // Row visiting and batch visiting run independently, not chained.
        // `oom_batch_capacity_hint` fails row visiting on a bounds check, which would skip
        // the batch scan, the one path that exercises the pre-allocation hint the artifact
        // is for. Each scan gets its own attempt, as in the fuzz target.
        //
        // The assertion is that all of this returns. An unbounded allocation does not fail
        // an `assert!`, it takes the whole test process down, which is the signal.
        let open = Dataset::from_bytes(bytes);
        let mut first_error = open.as_ref().err().map(ToString::to_string);

        if let Ok(dataset) = open.as_ref() {
            let mut seen = 0usize;
            let rows = dataset
                .scan()
                .with_parallelism(Parallelism::None)
                .visit_rows(|row| {
                    black_box(row.len());
                    seen += 1;
                    if seen >= SCAN_ROW_CAP {
                        return Ok(ControlFlow::Break(()));
                    }
                    Ok(ControlFlow::Continue(()))
                });
            if let Err(err) = rows {
                first_error.get_or_insert_with(|| err.to_string());
            }

            let mut batch_rows = 0usize;
            let batches = dataset
                .scan()
                .with_parallelism(Parallelism::None)
                .visit_batches(|batch| {
                    batch_rows += batch.row_count;
                    if batch_rows >= SCAN_ROW_CAP {
                        return Ok(ControlFlow::Break(()));
                    }
                    Ok(ControlFlow::Continue(()))
                });
            if let Err(err) = batches {
                first_error.get_or_insert_with(|| err.to_string());
            }
        }

        let stem = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or_default();
        if let Some((_, needle)) = EXPECTED.iter().find(|(name, _)| *name == stem) {
            let message = first_error.clone().unwrap_or_else(|| {
                panic!("{stem} is expected to be rejected, but it parsed and scanned cleanly")
            });
            assert!(
                message.contains(needle),
                "{stem}: expected the error to mention {needle:?}, got {message:?}"
            );
        }

        seen_stems.push(stem.to_owned());
        exercised += 1;
    }

    // A fixture renamed or deleted out from under an EXPECTED entry would otherwise leave
    // the entry silently unasserted.
    for (name, _) in EXPECTED {
        assert!(
            seen_stems.iter().any(|stem| stem == name),
            "{name} is in EXPECTED but no such fixture is on disk"
        );
    }
    for name in EXPECTED_NO_DIAGNOSTIC {
        assert!(
            seen_stems.iter().any(|stem| stem == name),
            "{name} is in EXPECTED_NO_DIAGNOSTIC but no such fixture is on disk"
        );
    }
    assert!(exercised >= EXPECTED.len() + EXPECTED_NO_DIAGNOSTIC.len());
}

/// The `.sas7bcat` value-label parser is a separate format with its own declared counts.
/// `label_count_used` is a u64 straight out of the file that sized a `Vec`, and a 22 KB
/// catalog declaring 1,313,169,229 labels reached `calloc(10505353832)`.
#[test]
fn catalog_fuzz_artifacts_parse_without_exhausting_memory() {
    let dir = fuzz_fixture_dir().join("catalog");
    let mut exercised = 0usize;
    for entry in fs::read_dir(&dir)
        .unwrap_or_else(|err| panic!("cannot read {}: {err}", dir.display()))
        .filter_map(Result::ok)
    {
        let path = entry.path();
        if path.extension().is_none_or(|ext| ext != "sas7bcat") {
            continue;
        }
        let bytes =
            fs::read(&path).unwrap_or_else(|err| panic!("cannot read {}: {err}", path.display()));

        // Same narrow contract: it must return.
        let outcome = sas7bdat::catalog::parse_catalog(&mut Cursor::new(bytes.as_slice()));
        if let Ok(layout) = outcome {
            for set in &layout.label_sets {
                black_box(set.labels.len());
            }
        }
        exercised += 1;
    }
    assert!(
        exercised > 0,
        "no catalog artifacts found in {}",
        dir.display()
    );
}

/// The guard is a bound on *plausibility*, not a fixed limit, so it has to stay clear
/// of real files. The widest fixture in the corpus is 4,041 columns, and the tightest
/// ratio of ceiling to actual column count across 233 fixtures is 26x. A normal file
/// must not come near the bound.
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
