//! Which pipeline a scan takes, on both axes, and the fact that it depends on the entry
//! point as much as on the file.
//!
//! This crate has several decode pipelines. They share almost every symbol below the point
//! where they diverge, so a flat profile of one is indistinguishable from a flat profile of
//! another. That has already produced one wrong conclusion here: an optimisation was written
//! for the tiled column-major fill, benchmarked through `visit_batches`, and appeared to do
//! nothing, because `visit_batches` never reaches that fill however the plan is shaped.
//!
//! These tests pin the surprising parts so they cannot change quietly.

use sas7bdat::{Dataset, FillStrategy, PageSource, ScanEntry};
use std::path::{Path, PathBuf};

fn fixture(rel: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures")
        .join(rel)
}

fn dataset() -> Option<Dataset> {
    let path = fixture("raw_data/ahs2013/homimp.sas7bdat");
    path.exists().then(|| Dataset::open(&path).expect("open"))
}

/// `visit_batches` does **not** reach the column-major fill. This is the one that cost time:
/// it is not obvious, nothing in a profile says so, and a plan full of numeric columns does
/// not change it.
#[test]
fn borrowed_batches_never_reach_the_tiled_fill() {
    let Some(ds) = dataset() else { return };
    let path = ds
        .scan()
        .predict_path(ScanEntry::BorrowedBatches)
        .expect("predict");
    assert_eq!(path.source, PageSource::BorrowedStream);
    assert!(
        !path.used_tiled_fill(),
        "visit_batches cannot reach the tiled fill; benchmark it through collect_batches"
    );
}

/// The owned batch path is the one that can, and on a fused uncompressed fixture it does.
#[test]
fn owned_batches_reach_the_tiled_fill() {
    let Some(ds) = dataset() else { return };
    let path = ds.scan().predict_path(ScanEntry::Batches).expect("predict");
    assert!(
        path.used_tiled_fill(),
        "collect_batches reported {path:?}; the tiled fill did not run"
    );
}

/// The two axes are independent, which is the whole reason they are reported separately.
/// Turning the fill off must leave the page source alone: a single name could not say this,
/// and reported `On` and `Off` identically.
#[test]
fn turning_the_fill_off_changes_the_fill_and_not_the_source() {
    let Some(ds) = dataset() else { return };
    let on = ds.scan().predict_path(ScanEntry::Batches).expect("predict");
    let off = ds
        .scan()
        .with_column_major_decode(sas7bdat::ColumnMajorDecode::Off)
        .predict_path(ScanEntry::Batches)
        .expect("predict");

    assert_eq!(on.source, off.source, "the page source must not move");
    assert_eq!(on.fill, FillStrategy::Tiled);
    assert_eq!(off.fill, FillStrategy::RowMajor);
}

/// The prediction is a truncated run of the real selection, so it must agree with what a
/// full scan records. If these diverge the prediction has stopped describing the code.
#[test]
fn the_prediction_matches_what_a_full_scan_records() {
    let Some(ds) = dataset() else { return };

    let predicted = ds
        .scan()
        .predict_path(ScanEntry::BorrowedBatches)
        .expect("predict");
    let actual = ds
        .scan()
        .visit_batches(|_| Ok(std::ops::ControlFlow::Continue(())))
        .expect("scan")
        .path;
    assert_eq!(predicted, actual, "borrowed batches");

    let predicted = ds.scan().predict_path(ScanEntry::RawRows).expect("predict");
    let actual = ds
        .scan()
        .visit_raw_rows(|_| Ok(std::ops::ControlFlow::Continue(())))
        .expect("scan")
        .path;
    assert_eq!(predicted, actual, "raw rows");
}

/// Every entry point must record both axes. `Unrecorded` means a pipeline was added without
/// stamping itself, which is how the labelling silently goes stale.
#[test]
fn every_entry_point_records_both_axes() {
    let Some(ds) = dataset() else { return };
    for entry in [
        ScanEntry::Batches,
        ScanEntry::BorrowedBatches,
        ScanEntry::Rows,
        ScanEntry::RawRows,
    ] {
        let path = ds.scan().predict_path(entry).expect("predict");
        assert_ne!(
            path.source,
            PageSource::Unrecorded,
            "{entry:?} recorded no page source"
        );
        assert_ne!(
            path.fill,
            FillStrategy::Unrecorded,
            "{entry:?} recorded no fill strategy"
        );
    }
}

/// The selection is now a value the executor also uses, so asking it must give the same
/// answer as running the scan. If these diverge, the report has stopped describing the code,
/// which is the exact failure the whole mechanism exists to prevent.
#[test]
fn the_free_selection_agrees_with_the_scan_it_describes() {
    let Some(ds) = dataset() else { return };
    for parallelism in [
        sas7bdat::Parallelism::Auto,
        sas7bdat::Parallelism::None,
        sas7bdat::Parallelism::Threads(4),
    ] {
        let predicted = ds
            .scan()
            .with_parallelism(parallelism)
            .predict_path(ScanEntry::Batches)
            .expect("predict");
        let actual_path = ds
            .scan()
            .with_parallelism(parallelism)
            .visit_owned_batches(|_| Ok(std::ops::ControlFlow::Continue(())))
            .expect("scan")
            .path;
        assert_eq!(
            predicted.source, actual_path.source,
            "{parallelism:?}: predicted source differs from the scan's"
        );
    }
}
