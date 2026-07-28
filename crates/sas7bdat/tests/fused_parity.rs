//! The fused single-pass scan must decode exactly what the two-pass scan decodes.
//!
//! A buffered (unmapped) source compiles page descriptors from the same extents it decodes,
//! rather than walking the whole file first. That reorders the work across three thread pools
//! and carries `row_base` through a sequential stage, so the risk is dropped, duplicated, or
//! misplaced rows — none of which a row count alone would catch.
//!
//! Comparison is per row, not per batch: the two paths cut batches at different boundaries, so
//! anything that hashes whole buffers would report a difference that isn't one.

use sas7bdat::{
    Dataset, IoBackendPreference, OpenOptions, OwnedColumnBuffer, OwnedColumnarBatch, Parallelism,
};
use std::{ops::ControlFlow, path::Path};

fn fixture(rel: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures")
        .join(rel)
}

/// Bit-packed validity, LSB first; `None` means every row is valid.
fn valid_at(valid: Option<&Vec<u64>>, row: usize) -> bool {
    valid.is_none_or(|bits| {
        bits.get(row / 64)
            .is_some_and(|word| word >> (row % 64) & 1 == 1)
    })
}

/// Append one row's value to `out`, prefixed by its validity, so column contents can be
/// compared independently of how the rows were grouped into batches.
fn push_row(out: &mut Vec<u8>, column: &OwnedColumnBuffer, row: usize) {
    fn cell<T: bytemuck::Pod>(
        out: &mut Vec<u8>,
        values: &[T],
        valid: Option<&Vec<u64>>,
        row: usize,
    ) {
        out.push(u8::from(valid_at(valid, row)));
        out.extend_from_slice(bytemuck::bytes_of(&values[row]));
    }

    match column {
        OwnedColumnBuffer::I32 { values, valid } => cell(out, values, valid.as_ref(), row),
        OwnedColumnBuffer::I64 { values, valid } => cell(out, values, valid.as_ref(), row),
        OwnedColumnBuffer::F64 { values, valid } => cell(out, values, valid.as_ref(), row),
        OwnedColumnBuffer::Date { values, valid } => cell(out, values, valid.as_ref(), row),
        OwnedColumnBuffer::DateTime { values, valid } => cell(out, values, valid.as_ref(), row),
        OwnedColumnBuffer::Time { values, valid } => cell(out, values, valid.as_ref(), row),
        OwnedColumnBuffer::Utf8 {
            offsets,
            data,
            valid,
            ..
        }
        | OwnedColumnBuffer::RawBytes {
            offsets,
            data,
            valid,
        } => {
            out.push(u8::from(valid_at(valid.as_ref(), row)));
            let offsets = offsets.as_slice();
            let start = usize::try_from(offsets[row]).expect("offset fits usize");
            let end = usize::try_from(offsets[row + 1]).expect("offset fits usize");
            out.extend_from_slice(&data[start..end]);
            // Length-delimit, so "ab" + "c" cannot look like "a" + "bc".
            out.extend_from_slice(&(end - start).to_le_bytes());
        }
    }
}

/// One byte vector per column, holding every row in file order.
fn columns_in_row_order(batches: &[OwnedColumnarBatch]) -> Vec<Vec<u8>> {
    let column_count = batches.first().map_or(0, |batch| batch.columns.len());
    let mut columns = vec![Vec::new(); column_count];
    let mut expected_row_base = 0u64;
    for batch in batches {
        assert_eq!(
            batch.row_base.0, expected_row_base,
            "batches must arrive in row order"
        );
        expected_row_base += batch.row_count as u64;
        for (out, column) in columns.iter_mut().zip(&batch.columns) {
            for row in 0..batch.row_count {
                push_row(out, column, row);
            }
        }
    }
    columns
}

fn scan(path: &Path, backend: IoBackendPreference) -> Vec<OwnedColumnarBatch> {
    let ds = Dataset::open_with(path, OpenOptions::builder().io_backend(backend).build())
        .unwrap_or_else(|err| panic!("open {}: {err}", path.display()));
    let mut batches = Vec::new();
    ds.scan()
        .with_parallelism(Parallelism::Threads(4))
        .visit_owned_batches(|batch| {
            batches.push(batch);
            Ok(ControlFlow::Continue(()))
        })
        .unwrap_or_else(|err| panic!("scan {}: {err}", path.display()));
    batches
}

fn check_fused_matches_two_pass(path: &Path) {
    if !path.exists() {
        return;
    }
    // Mapped sources decline the fused path, so this is the two-pass reference.
    let reference = scan(path, IoBackendPreference::MmapPreferred);
    let fused = scan(path, IoBackendPreference::BufferedOnly);

    let reference_rows: usize = reference.iter().map(|batch| batch.row_count).sum();
    let fused_rows: usize = fused.iter().map(|batch| batch.row_count).sum();
    assert_eq!(
        reference_rows,
        fused_rows,
        "{}: row count differs",
        path.display()
    );

    let reference_columns = columns_in_row_order(&reference);
    let fused_columns = columns_in_row_order(&fused);
    assert_eq!(
        reference_columns.len(),
        fused_columns.len(),
        "{}: column count differs",
        path.display()
    );
    for (index, (expected, actual)) in reference_columns.iter().zip(&fused_columns).enumerate() {
        assert!(
            expected == actual,
            "{}: column {index} differs",
            path.display()
        );
    }
}

#[test]
fn airline_fused_matches_two_pass() {
    check_fused_matches_two_pass(&fixture("raw_data/pandas/airline.sas7bdat"));
}

#[test]
fn controlbyte_fused_matches_two_pass() {
    check_fused_matches_two_pass(&fixture("raw_data/pandas/0x40controlbyte.sas7bdat"));
}

#[test]
fn many_columns_fused_matches_two_pass() {
    check_fused_matches_two_pass(&fixture("raw_data/pandas/many_columns.sas7bdat"));
}

#[test]
fn datetime_fused_matches_two_pass() {
    check_fused_matches_two_pass(&fixture("raw_data/pandas/datetime.sas7bdat"));
}

#[test]
fn productsales_fused_matches_two_pass() {
    check_fused_matches_two_pass(&fixture("raw_data/pandas/productsales.sas7bdat"));
}

/// 37 MB uncompressed: the first fixture large enough to need several extents, so the readers
/// deliver out of order and the sequential parse stage has to put them back in order.
#[test]
fn multi_extent_uncompressed_fused_matches_two_pass() {
    check_fused_matches_two_pass(&fixture(
        "education-edu-demog-and-geog-estimate/GRF14/grf14_lea_blkgrp.sas7bdat",
    ));
}

/// 39 MB of RLE-compressed rows across 1,326 columns: every page is `IndexedCompressedRows`,
/// so the parse stage builds a row span per row and the decoder resolves them extent-locally.
#[test]
fn multi_extent_compressed_fused_matches_two_pass() {
    check_fused_matches_two_pass(&fixture(
        "education-int-assessment-of-adult-comps/original/2014045_sas/prgusams_puf.sas7bdat",
    ));
}
