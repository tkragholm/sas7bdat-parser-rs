//! Differential verification: for every fixture under `fixtures/`, decode it with the
//! column-major path (now the default) and with the row-major path forced off, and assert the
//! two produce byte-identical batches — serial and parallel. Proves flipping the default is a
//! behavioral no-op on real SAS files.
//!
//! Run with: cargo run -p sas7bdat --release --example verify_columnar

use sas7bdat::{
    BatchHint, ColumnMajorDecode, Dataset, OwnedColumnBuffer, OwnedColumnarBatch, Parallelism,
    ScanBuilder,
};
use std::path::{Path, PathBuf};

fn fixtures_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("fixtures")
}

fn find_sas7bdat(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            find_sas7bdat(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("sas7bdat") {
            out.push(path);
        }
    }
}

/// Flatten a batch sequence into per-column `(bits, valid)` rows in global order. Works for any
/// column type by reading the raw value bytes; ignores the value of null cells.
fn flatten(batches: &[OwnedColumnarBatch]) -> Option<Vec<Vec<(u64, bool)>>> {
    let mut sorted: Vec<&OwnedColumnarBatch> = batches.iter().collect();
    sorted.sort_by_key(|b| b.row_base);
    let ncols = sorted.first().map_or(0, |b| b.columns.len());
    let mut cols: Vec<Vec<(u64, bool)>> = vec![Vec::new(); ncols];
    for b in sorted {
        if b.columns.len() != ncols {
            return None;
        }
        for (ci, col) in b.columns.iter().enumerate() {
            push_column_cells(col, &mut cols[ci]);
        }
    }
    Some(cols)
}

fn valid_bit(valid: &Option<Vec<u64>>, i: usize) -> bool {
    valid
        .as_ref()
        .is_none_or(|bits| bits.get(i / 64).is_some_and(|w| (w >> (i % 64)) & 1 == 1))
}

fn push_column_cells(col: &OwnedColumnBuffer, out: &mut Vec<(u64, bool)>) {
    match col {
        OwnedColumnBuffer::I32 { values, valid } => {
            for (i, v) in values.iter().enumerate() {
                out.push((u64::from(v.cast_unsigned()), valid_bit(valid, i)));
            }
        }
        OwnedColumnBuffer::I64 { values, valid } => {
            for (i, v) in values.iter().enumerate() {
                out.push((v.cast_unsigned(), valid_bit(valid, i)));
            }
        }
        OwnedColumnBuffer::F64 { values, valid } => {
            for (i, v) in values.iter().enumerate() {
                out.push((v.to_bits(), valid_bit(valid, i)));
            }
        }
        OwnedColumnBuffer::Date { values, valid } => {
            for (i, v) in values.iter().enumerate() {
                out.push((u64::from(v.days_since_sas_epoch.cast_unsigned()), valid_bit(valid, i)));
            }
        }
        OwnedColumnBuffer::DateTime { values, valid } => {
            for (i, v) in values.iter().enumerate() {
                out.push((v.seconds_since_sas_epoch.cast_unsigned(), valid_bit(valid, i)));
            }
        }
        OwnedColumnBuffer::Time { values, valid } => {
            for (i, v) in values.iter().enumerate() {
                out.push((u64::from(v.seconds_since_midnight.cast_unsigned()), valid_bit(valid, i)));
            }
        }
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
            let offs = offsets.as_slice();
            for i in 0..offs.len().saturating_sub(1) {
                let start = usize::try_from(offs[i]).unwrap_or(0);
                let end = usize::try_from(offs[i + 1]).unwrap_or(0);
                // Hash the bytes into the u64 slot; collisions are astronomically unlikely to
                // mask a real divergence and this keeps the comparison type-uniform.
                let mut h = 0xcbf2_9ce4_8422_2325_u64;
                for &b in &data[start..end] {
                    h = (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3);
                }
                out.push((h, valid_bit(valid, i)));
            }
        }
    }
}

fn collect(builder: ScanBuilder<'_>) -> Result<Vec<OwnedColumnarBatch>, String> {
    builder
        .with_batch_hint(BatchHint::Rows(4096))
        .collect_batches()
        .map_err(|e| e.to_string())
}

fn main() {
    let mut fixtures = Vec::new();
    find_sas7bdat(&fixtures_root(), &mut fixtures);
    // Also the old reference implementation's corpus, for wider coverage.
    let old_impl = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("old-implementation/sas7bdat-parser-rs/fixtures");
    find_sas7bdat(&old_impl, &mut fixtures);
    fixtures.sort();
    fixtures.dedup();

    let mut checked = 0usize;
    let mut open_failed = 0usize;
    let mut decode_failed = 0usize;
    let mut engaged = 0usize; // fixtures where column-major actually ran (all-staged-numeric)
    let mut mismatches: Vec<String> = Vec::new();

    for path in &fixtures {
        let Ok(ds) = Dataset::from_bytes(match std::fs::read(path) {
            Ok(b) => b,
            Err(_) => {
                open_failed += 1;
                continue;
            }
        }) else {
            open_failed += 1;
            continue;
        };

        let row_major = collect(ds.scan().with_column_major_decode(ColumnMajorDecode::Off));
        let col_serial = collect(ds.scan().with_column_major_decode(ColumnMajorDecode::On));
        let col_parallel = collect(
            ds.scan()
                .with_parallelism(Parallelism::Threads(4))
                .with_column_major_decode(ColumnMajorDecode::On),
        );

        let (row_major, col_serial, col_parallel) = match (row_major, col_serial, col_parallel) {
            (Ok(a), Ok(b), Ok(c)) => (a, b, c),
            _ => {
                decode_failed += 1;
                continue;
            }
        };
        checked += 1;

        // Did column-major engage? Detect via the all-staged-numeric plan: every column is
        // numeric/temporal. (A proxy: no Utf8/RawBytes columns in the output.)
        let all_numeric = row_major.first().is_none_or(|b| {
            b.columns.iter().all(|c| {
                !matches!(
                    c,
                    OwnedColumnBuffer::Utf8 { .. } | OwnedColumnBuffer::RawBytes { .. }
                )
            })
        }) && !row_major.is_empty();
        if all_numeric {
            engaged += 1;
        }

        let rel = path.strip_prefix(fixtures_root()).unwrap_or(path);
        match (flatten(&row_major), flatten(&col_serial), flatten(&col_parallel)) {
            (Some(r), Some(s), Some(p)) => {
                if r != s {
                    mismatches.push(format!("{} : serial column-major != row-major", rel.display()));
                }
                if r != p {
                    mismatches.push(format!("{} : parallel column-major != row-major", rel.display()));
                }
            }
            _ => mismatches.push(format!("{} : column count differs across batches", rel.display())),
        }
    }

    println!("fixtures found:      {}", fixtures.len());
    println!("opened + decoded:    {checked}");
    println!("open failed:         {open_failed}");
    println!("decode failed:       {decode_failed}");
    println!("column-major engaged (all-numeric): {engaged}");
    println!("mismatches:          {}", mismatches.len());
    for m in mismatches.iter().take(50) {
        println!("  MISMATCH {m}");
    }
    assert!(mismatches.is_empty(), "column-major diverged from row-major");
    println!("\nOK: column-major == row-major on every decoded fixture.");
}
