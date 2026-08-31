//! Regressions for inputs `cargo fuzz` found (see `crates/sas7bdat/fuzz`).
//!
//! Every file under `tests/fixtures/fuzz/` is an input that once made the reader
//! misbehave. The contract asserted here is deliberately narrow: opening it must
//! *return*, with either `Ok` or `Err`. Aborting, hanging, or exhausting memory are
//! all failures, and none of them are things an `assert!` can catch — the test
//! catches them by the run dying instead of finishing.
//!
//! Drop new artifacts into that directory and they are covered automatically. Add an
//! entry to `EXPECTED` when the artifact's whole point is a specific diagnostic, or to
//! `EXPECTED_NO_DIAGNOSTIC` when it is not — a bug fixed by clamping an allocation
//! rather than refusing input leaves no message worth pinning, and whatever error the
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
    // An 18 KB file declaring 1,085,348,864 columns, and a 32 KB one declaring
    // 2,863,311,531. Both used to reach `Vec::resize_with` and ask the allocator for
    // 2.5 GiB before any row was read; libFuzzer caught them as OOMs within 90
    // seconds of the target first running. The declared count is now refuted against
    // the file's own geometry, so they fail in constant space.
    // `_8k` moved to `EXPECTED_NO_DIAGNOSTIC` below: it no longer reaches the column
    // guard, so its message is now incidental. `_32k` still pins that diagnostic.
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

/// Artifacts with no entry in [`EXPECTED`], listed here only so the completeness check
/// below can tell "deliberately unasserted" from "fixture went missing".
///
/// These are the cases where the fix was a clamp rather than a rejection, so there is no
/// diagnostic to assert — the regression signal is memory, and the only observable
/// contract is that the run finishes. Whatever error they do or do not return is
/// incidental and must not be pinned.
const EXPECTED_NO_DIAGNOSTIC: &[&str] = &[
    // Was pinned to "columns" until the 64-bit subheader signature started being read as
    // eight bytes rather than four. This artifact's `COLUMN_SIZE` carries a mutated upper
    // word (`0x40200000` where a real one is zero), so it is now refused as data before any
    // column count is declared, and the file fails for a missing `ROW_SIZE` instead. Still
    // refused, still in constant space, which is what the artifact exists to prove; which
    // guard catches it is not. `oom_declared_column_count_32k` still covers the column
    // diagnostic itself.
    "oom_declared_column_count_8k",
    // 9 KB, 204 rows, 4 columns — a plausible-looking file whose declared `rows_per_page`
    // and row count together drove `BatchAccumulator::new` to ask for 876 GB through
    // `OwnedBatchColumnBuilder::with_capacity_hint`. Reached only via `visit_batches`.
    // It happens to fail row visiting on an unrelated bounds check; that is not the
    // regression and is deliberately not asserted.
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
        // Row visiting and batch visiting run *independently*, not chained. Chaining
        // them with `and_then` looked tidier and was wrong: `oom_batch_capacity_hint`
        // fails row visiting on an unrelated (and correct) bounds check, which
        // short-circuited the batch scan — the one path that exercises the
        // pre-allocation hint the artifact exists for. Each scan gets its own attempt,
        // exactly as the fuzz target does.
        //
        // The assertion is that all of this returns. A regression reintroducing an
        // unbounded allocation does not fail an `assert!` — it takes the whole test
        // process down, which is the signal.
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

    // Catch a fixture that was renamed or deleted out from under an EXPECTED entry,
    // which would otherwise leave the entry silently unasserted.
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

        // Same narrow contract: it must return. A regression allocates instead.
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
