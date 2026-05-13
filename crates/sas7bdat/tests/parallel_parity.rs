// Verify parallel and serial scan produce byte-identical data on real fixtures.
//
// The SIMD scanner can scan pages in parallel threads. This test ensures the
// parallel path produces the same batches as the serial path when sorted by
// row_base. Catches dropped rows, off-by-one errors, or data corruption in
// the parallel code path.

use sas7bdat::{Dataset, OwnedColumnBuffer, OwnedColumnarBatch, Parallelism};
use std::path::Path;

fn fixture(rel: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures")
        .join(rel)
}

fn canonical_batch(batch: &OwnedColumnarBatch) -> (u64, Vec<u8>) {
    let mut out = Vec::new();
    out.extend_from_slice(&(batch.row_count as u64).to_le_bytes());
    for col in &batch.columns {
        match col {
            OwnedColumnBuffer::F64 { values, valid } => {
                out.extend_from_slice(bytemuck::cast_slice(values));
                if let Some(v) = valid {
                    out.extend_from_slice(bytemuck::cast_slice(v));
                }
            }
            OwnedColumnBuffer::I32 { values, valid } => {
                out.extend_from_slice(bytemuck::cast_slice(values));
                if let Some(v) = valid {
                    out.extend_from_slice(bytemuck::cast_slice(v));
                }
            }
            OwnedColumnBuffer::I64 { values, valid } => {
                out.extend_from_slice(bytemuck::cast_slice(values));
                if let Some(v) = valid {
                    out.extend_from_slice(bytemuck::cast_slice(v));
                }
            }
            OwnedColumnBuffer::Date { values, valid } => {
                out.extend_from_slice(bytemuck::cast_slice(values));
                if let Some(v) = valid {
                    out.extend_from_slice(bytemuck::cast_slice(v));
                }
            }
            OwnedColumnBuffer::DateTime { values, valid } => {
                out.extend_from_slice(bytemuck::cast_slice(values));
                if let Some(v) = valid {
                    out.extend_from_slice(bytemuck::cast_slice(v));
                }
            }
            OwnedColumnBuffer::Time { values, valid } => {
                out.extend_from_slice(bytemuck::cast_slice(values));
                if let Some(v) = valid {
                    out.extend_from_slice(bytemuck::cast_slice(v));
                }
            }
            OwnedColumnBuffer::Utf8 { data, valid, .. }
            | OwnedColumnBuffer::RawBytes { data, valid, .. } => {
                out.extend_from_slice(data);
                if let Some(v) = valid {
                    out.extend_from_slice(bytemuck::cast_slice(v));
                }
            }
        }
    }
    (batch.row_base.0, out)
}

fn sorted_fingerprints(batches: &[OwnedColumnarBatch]) -> Vec<(u64, Vec<u8>)> {
    let mut fps: Vec<_> = batches.iter().map(canonical_batch).collect();
    fps.sort_by_key(|(base, _)| *base);
    fps
}

fn check_parallel_matches_serial(path: &Path) {
    if !path.exists() {
        return;
    }
    let ds = Dataset::open(path)
        .unwrap_or_else(|e| panic!("open {}: {e}", path.display()));

    let serial = ds
        .scan()
        .with_parallelism(Parallelism::None)
        .collect_batches()
        .expect("serial collect");

    let parallel = ds
        .scan()
        .with_parallelism(Parallelism::Threads(4))
        .collect_batches()
        .expect("parallel collect");

    let serial_total: usize = serial.iter().map(|b| b.row_count).sum();
    let parallel_total: usize = parallel.iter().map(|b| b.row_count).sum();
    assert_eq!(
        serial_total,
        parallel_total,
        "{}: total row count differs",
        path.display()
    );

    let serial_fps = sorted_fingerprints(&serial);
    let parallel_fps = sorted_fingerprints(&parallel);

    for (idx, (sf, pf)) in serial_fps.iter().zip(parallel_fps.iter()).enumerate() {
        assert_eq!(
            sf.0, pf.0,
            "{}: batch row_base mismatch at index {idx}",
            path.display()
        );
        assert_eq!(
            sf.1, pf.1,
            "{}: batch data mismatch at row_base {}",
            path.display(),
            sf.0
        );
    }
}

#[test]
fn airline_parallel_matches_serial() {
    check_parallel_matches_serial(&fixture("raw_data/pandas/airline.sas7bdat"));
}

#[test]
fn controlbyte_parallel_matches_serial() {
    check_parallel_matches_serial(&fixture("raw_data/pandas/0x40controlbyte.sas7bdat"));
}

#[test]
fn many_columns_parallel_matches_serial() {
    check_parallel_matches_serial(&fixture("raw_data/pandas/many_columns.sas7bdat"));
}

#[test]
fn datetime_parallel_matches_serial() {
    check_parallel_matches_serial(&fixture("raw_data/pandas/datetime.sas7bdat"));
}
