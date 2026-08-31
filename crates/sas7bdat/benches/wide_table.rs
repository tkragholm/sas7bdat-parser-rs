//! Wide-table decode benchmark: row-major vs column-major batch collection.
//!
//! The default decode loop is row-major — `push_row` touches every column's builder once
//! per row, so an N-column row cycles N distinct `Vec` tails (N cache lines). For wide
//! tables (hundreds–thousands of numeric columns, common in register/family data) that
//! working set spills out of L1/L2 and each append becomes an L2/L3 access.
//!
//! `collect_batches_columnar` decodes `FusedContiguousUncompressed` pages column-by-column
//! instead, filling one column's `raw_bits` to completion before the next (one hot tail,
//! strided reads). This benchmark holds the total cell count roughly constant and sweeps the
//! column count, so the ratio between the two paths isolates the per-column cache effect: the
//! columnar speedup should grow with width.
//!
//! Run with:
//!   `cargo bench -p sas7bdat --features internal-bench --bench wide_table`

#![allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use sas7bdat::{
    BatchHint, ColumnMajorDecode, Dataset, LogicalType, Parallelism, ScanEntry,
    test_utils::MockDatasetBuilder,
};
use std::{hint::black_box, ops::ControlFlow, sync::Arc};

mod common;
use common::{labelled_id, labelled_id_with};

/// SAS numeric "missing" sentinel: exponent all ones, fraction == 1. Mirrors the crate's
/// internal `SAS_NUMERIC_MISSING_SENTINEL`; injecting a few exercises the validity path.
const SAS_MISSING_BITS: u64 = 0x7FF0_0000_0000_0001;

/// Rows packed into each synthetic data page. Constant across configs so page structure
/// (header overhead, rows-per-page) does not confound the comparison.
const ROWS_PER_PAGE: usize = 64;

/// Page header occupies the first 24 bytes (see `test_utils::write_page_header`); data rows
/// start at offset 24.
const PAGE_HEADER_LEN: usize = 24;

/// SAS page type for a contiguous (fused) uncompressed data page.
const PAGE_TYPE_DATA: u16 = 0x0100;

/// Build a synthetic all-`Float` dataset of `num_cols` columns and `total_rows` rows, laid
/// out as fused contiguous uncompressed pages of `ROWS_PER_PAGE` rows each. Every cell is a
/// little-endian f64; roughly 1 in 97 is a SAS missing sentinel to exercise validity.
fn build_wide_dataset(num_cols: usize, total_rows: usize) -> Dataset {
    let row_len = num_cols * 8;
    let page_size = PAGE_HEADER_LEN + row_len * ROWS_PER_PAGE;

    let mut bytes = Vec::with_capacity(total_rows.div_ceil(ROWS_PER_PAGE) * page_size);
    let mut row = 0usize;
    while row < total_rows {
        let n = ROWS_PER_PAGE.min(total_rows - row);
        let mut page = vec![0u8; page_size];
        // Page header: type, row_count, pointer_count (see write_page_header layout).
        page[(PAGE_HEADER_LEN - 8)..(PAGE_HEADER_LEN - 6)]
            .copy_from_slice(&PAGE_TYPE_DATA.to_le_bytes());
        page[(PAGE_HEADER_LEN - 6)..(PAGE_HEADER_LEN - 4)]
            .copy_from_slice(&(n as u16).to_le_bytes());

        let mut off = PAGE_HEADER_LEN;
        for r in 0..n {
            let global_row = row + r;
            for c in 0..num_cols {
                let cell = global_row * num_cols + c;
                let bits = if cell.is_multiple_of(97) {
                    SAS_MISSING_BITS
                } else {
                    // Non-integral so the Float tile stays F64 (no i64 downgrade path).
                    (cell as f64 * 0.5).to_bits()
                };
                page[off..off + 8].copy_from_slice(&bits.to_le_bytes());
                off += 8;
            }
        }
        bytes.extend_from_slice(&page);
        row += n;
    }

    let mut builder = MockDatasetBuilder::new(Arc::<[u8]>::from(bytes))
        .with_row_len(row_len)
        .with_total_rows(total_rows as u64)
        .with_rows_per_page(ROWS_PER_PAGE as u64)
        .with_encoding(Some("UTF-8".to_owned()));
    builder.page_size = page_size;
    for c in 0..num_cols {
        builder = builder.with_column(&format!("c{c}"), LogicalType::Float, 8, (c * 8) as u32);
    }
    builder.build()
}

fn bench_wide_table(c: &mut Criterion) {
    // ~4M cells per config keeps each measurement at a comparable scale while the column
    // count (and thus the row-major cache footprint) varies by 64x.
    const TARGET_CELLS: usize = 4_000_000;
    const BATCH_ROWS: usize = 4096;

    let mut group = c.benchmark_group("wide_table");

    for &num_cols in &[16usize, 64, 256, 1024] {
        let total_rows = (TARGET_CELLS / num_cols).max(ROWS_PER_PAGE);
        let total_cells = (total_rows * num_cols) as u64;
        let dataset = build_wide_dataset(num_cols, total_rows);

        // Sanity: the two paths must agree before timing either.
        debug_assert_columnar_parity(&dataset, BATCH_ROWS);

        group.throughput(Throughput::Elements(total_cells));

        group.bench_function(
            labelled_id("row_major", &dataset, ScanEntry::Batches, num_cols),
            |b| {
                b.iter(|| {
                    let batches = dataset
                        .scan()
                        .with_batch_hint(BatchHint::Rows(BATCH_ROWS))
                        .collect_batches()
                        .expect("row-major batches");
                    black_box(batches.len());
                });
            },
        );

        group.bench_function(
            labelled_id("column_major", &dataset, ScanEntry::Batches, num_cols),
            |b| {
                b.iter(|| {
                    let batches = dataset
                        .scan()
                        .with_batch_hint(BatchHint::Rows(BATCH_ROWS))
                        .collect_batches_columnar()
                        .expect("column-major batches");
                    black_box(batches.len());
                });
            },
        );

        // Parallel column-major collect (work-stealing chunk pool). Confirms the parallel path
        // scales over serial column-major and isn't regressed by the finer chunking.
        group.bench_function(
            labelled_id_with(
                "column_major_threads_4",
                &dataset,
                ScanEntry::Batches,
                num_cols,
                |scan| scan.with_parallelism(Parallelism::Threads(4)),
            ),
            |b| {
                b.iter(|| {
                    let batches = dataset
                        .scan()
                        .with_batch_hint(BatchHint::Rows(BATCH_ROWS))
                        .with_parallelism(Parallelism::Threads(4))
                        .with_column_major_decode(ColumnMajorDecode::On)
                        .collect_batches()
                        .expect("parallel column-major batches");
                    black_box(batches.len());
                });
            },
        );

        // Streaming path (visit_owned_batches) — the one the Polars plugin drives — with the
        // public flag off vs on, to confirm the speedup carries past collect_batches.
        for (label, mode) in [
            ("stream_row_major", ColumnMajorDecode::Off),
            ("stream_column_major", ColumnMajorDecode::On),
        ] {
            group.bench_function(BenchmarkId::new(label, num_cols), |b| {
                b.iter(|| {
                    let mut n = 0u64;
                    dataset
                        .scan()
                        .with_batch_hint(BatchHint::Rows(BATCH_ROWS))
                        .with_column_major_decode(mode)
                        .visit_owned_batches(|batch| {
                            n += batch.row_count as u64;
                            Ok(ControlFlow::Continue(()))
                        })
                        .expect("streamed batches");
                    black_box(n);
                });
            });
        }
    }

    group.finish();
}

/// Assert the column-major path produces byte-identical column buffers to the row-major
/// default. Cheap insurance that the benchmark is comparing two correct decoders.
fn debug_assert_columnar_parity(dataset: &Dataset, batch_rows: usize) {
    use sas7bdat::OwnedColumnBuffer;

    let row_major = dataset
        .scan()
        .with_batch_hint(BatchHint::Rows(batch_rows))
        .collect_batches()
        .expect("row-major parity batches");
    let column_major = dataset
        .scan()
        .with_batch_hint(BatchHint::Rows(batch_rows))
        .collect_batches_columnar()
        .expect("column-major parity batches");

    assert_eq!(
        row_major.len(),
        column_major.len(),
        "batch count mismatch between decode paths"
    );
    for (rb, cb) in row_major.iter().zip(&column_major) {
        assert_eq!(rb.row_base, cb.row_base, "row_base mismatch");
        assert_eq!(rb.row_count, cb.row_count, "row_count mismatch");
        assert_eq!(rb.columns.len(), cb.columns.len(), "column count mismatch");
        for (r, c) in rb.columns.iter().zip(&cb.columns) {
            match (r, c) {
                (
                    OwnedColumnBuffer::F64 {
                        values: rv,
                        valid: rval,
                    },
                    OwnedColumnBuffer::F64 {
                        values: cv,
                        valid: cval,
                    },
                ) => {
                    let rbits: Vec<u64> = rv.iter().map(|v| v.to_bits()).collect();
                    let cbits: Vec<u64> = cv.iter().map(|v| v.to_bits()).collect();
                    assert_eq!(rbits, cbits, "F64 value bits mismatch");
                    assert_eq!(rval, cval, "F64 validity mismatch");
                }
                _ => panic!("expected F64 columns from the all-Float wide dataset"),
            }
        }
    }
}

criterion_group!(benches, bench_wide_table);
criterion_main!(benches);
