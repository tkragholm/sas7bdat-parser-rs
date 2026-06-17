//! Benchmark string-column dictionary encoding: per file, classify every string
//! column (dictionary vs plain via the HLL gate), and compare memory + encode
//! time of the dictionary representation against plain Utf8.
//!
//! Run: cargo run --release -p sas7bdat --features dictionary --example bench_dict -- <file...>
use sas7bdat::dictionary::{dictionary_encode, DictionaryPolicy};
use sas7bdat::{Dataset, OwnedColumnBuffer};
use std::time::Instant;

/// Plain Utf8 (LargeUtf8) heap footprint of a string column: data + i64 offsets
/// + validity bitmap.
fn utf8_bytes(bufs: &[&OwnedColumnBuffer]) -> usize {
    let mut data = 0usize;
    let mut rows = 0usize;
    for b in bufs {
        if let OwnedColumnBuffer::Utf8 { offsets, data: d, .. } = b {
            data += d.len();
            rows += offsets.as_slice().len().saturating_sub(1);
        }
    }
    data + (rows + 1) * std::mem::size_of::<i64>() + rows / 8
}

fn main() {
    let policy = DictionaryPolicy::default();
    for path in std::env::args().skip(1) {
        let Ok(ds) = Dataset::open(&path) else {
            eprintln!("skip (open failed): {path}");
            continue;
        };
        let batches = match ds.scan().collect_batches() {
            Ok(b) => b,
            Err(e) => {
                eprintln!("skip (scan failed: {e}): {path}");
                continue;
            }
        };
        let ncols = ds.columns().len();

        let mut n_string = 0usize;
        let mut n_dict = 0usize;
        let mut cells = 0usize;
        let mut utf8_total = 0usize;
        let mut encoded_total = 0usize; // dict where chosen, else utf8
        let mut card_sum = 0usize;

        let t = Instant::now();
        for ci in 0..ncols {
            let bufs: Vec<&OwnedColumnBuffer> =
                batches.iter().filter_map(|b| b.columns.get(ci)).collect();
            if !bufs.iter().any(|b| matches!(b, OwnedColumnBuffer::Utf8 { .. })) {
                continue;
            }
            n_string += 1;
            let u = utf8_bytes(&bufs);
            utf8_total += u;
            cells += bufs.iter().map(|b| match b {
                OwnedColumnBuffer::Utf8 { offsets, .. } => offsets.as_slice().len().saturating_sub(1),
                _ => 0,
            }).sum::<usize>();

            match dictionary_encode(&bufs, &policy) {
                Some(dict) => {
                    n_dict += 1;
                    card_sum += dict.dictionary.len();
                    encoded_total += dict.heap_bytes();
                }
                None => encoded_total += u, // high cardinality → keep plain
            }
        }
        let elapsed = t.elapsed();

        let base = std::path::Path::new(&path).file_name().and_then(|s| s.to_str()).unwrap_or(&path);
        println!("== {base} ==");
        println!(
            "  string cols: {n_string}  dictionary-encoded: {n_dict}  plain: {}  cells: {cells}",
            n_string - n_dict
        );
        if n_dict > 0 {
            println!("  avg cardinality (dict cols): {}", card_sum / n_dict);
        }
        println!("  encode time: {:.3}s", elapsed.as_secs_f64());
        println!(
            "  memory  plain-Utf8: {:>7.1} MB   dict-where-possible: {:>7.1} MB   ({:.2}x)",
            utf8_total as f64 / 1e6,
            encoded_total as f64 / 1e6,
            encoded_total as f64 / utf8_total.max(1) as f64,
        );
    }
}
