//! Measure the TRUE added cost of dictionary-encoding string columns *during*
//! the decode stream, vs a decode-only baseline. Also reports the post-decode
//! (cold, separate pass) cost for comparison, and the cardinality classification.
//!
//! Run: cargo run --release -p sas7bdat --features dictionary --example bench_dict -- <file...>
use sas7bdat::dictionary::{dictionary_encode, read_dictionary_columns, DictionaryPolicy};
use sas7bdat::{Dataset, OwnedColumnBuffer};
use std::ops::ControlFlow;
use std::time::Instant;

fn min_time(iters: usize, mut f: impl FnMut()) -> f64 {
    f(); // warmup
    (0..iters)
        .map(|_| {
            let t = Instant::now();
            f();
            t.elapsed().as_secs_f64()
        })
        .fold(f64::INFINITY, f64::min)
}

fn main() {
    let policy = DictionaryPolicy::default();
    let iters = 3;
    for path in std::env::args().skip(1) {
        let Ok(ds) = Dataset::open(&path) else {
            eprintln!("skip (open failed): {path}");
            continue;
        };

        // (1) decode-only baseline: full decode incl. Utf8 build, drop each batch.
        let t_decode = min_time(iters, || {
            ds.scan().visit_owned_batches(|_| Ok(ControlFlow::Continue(()))).expect("scan");
        });

        // (2) decode + dictionary built during the stream (cells cache-hot).
        let t_during = min_time(iters, || {
            let _ = read_dictionary_columns(&ds).expect("dict");
        });

        // (3) decode then dictionary in a separate post-pass (cells cache-cold),
        //     plus the cardinality classification + memory comparison.
        let batches = ds.scan().collect_batches().expect("collect");
        let ncols = ds.columns().len();
        let t_cold_start = Instant::now();
        let mut n_string = 0usize;
        let mut n_dict = 0usize;
        let mut cells = 0usize;
        for ci in 0..ncols {
            let bufs: Vec<&OwnedColumnBuffer> =
                batches.iter().filter_map(|b| b.columns.get(ci)).collect();
            if !bufs.iter().any(|b| matches!(b, OwnedColumnBuffer::Utf8 { .. })) {
                continue;
            }
            n_string += 1;
            cells += bufs.iter().map(|b| match b {
                OwnedColumnBuffer::Utf8 { offsets, .. } => offsets.as_slice().len().saturating_sub(1),
                _ => 0,
            }).sum::<usize>();
            if dictionary_encode(&bufs, &policy).is_some() {
                n_dict += 1;
            }
        }
        let t_cold = t_cold_start.elapsed().as_secs_f64();

        let base = std::path::Path::new(&path).file_name().and_then(|s| s.to_str()).unwrap_or(&path);
        println!("== {base} ==");
        println!(
            "  {n_string} string cols ({n_dict} dict / {} plain), {cells} string cells",
            n_string - n_dict
        );
        println!("  decode only:                 {t_decode:.3}s");
        println!(
            "  decode + dict (during stream): {t_during:.3}s   (+{:.3}s = +{:.0}%)",
            t_during - t_decode,
            (t_during - t_decode) / t_decode * 100.0
        );
        println!("  dict in a separate cold pass:  +{t_cold:.3}s  (for comparison)");
    }
}
