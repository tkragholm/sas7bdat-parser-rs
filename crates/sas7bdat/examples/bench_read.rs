//! Time a full file read into owned columnar batches.
//! Usage: `bench_read <path> <iters> <parallel|serial>`
// Throughput math casts byte/row counts to f64 for MB/s display; precision loss is fine here.
#![allow(clippy::cast_precision_loss)]
use sas7bdat::{Dataset, Parallelism};
use std::time::Instant;

fn read_once(path: &str, serial: bool) -> usize {
    // Parallelism::Auto resolves to a single worker; match the Polars plugin and
    // set Threads(ncpu) explicitly to exercise the parallel page-streaming path.
    let parallelism = if serial {
        Parallelism::None
    } else {
        let n = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        Parallelism::Threads(n)
    };
    let ds = Dataset::open(path).expect("open");
    let batches = ds
        .scan()
        .with_parallelism(parallelism)
        .collect_batches()
        .expect("collect");
    batches.iter().map(|b| b.row_count).sum()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = &args[1];
    let iters: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(5);
    let serial = args.get(3).is_some_and(|s| s == "serial");
    let label = if serial { "rust-core-serial" } else { "rust-core" };

    let rows = read_once(path, serial); // warmup
    let mut times = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        let n = read_once(path, serial);
        times.push(t.elapsed().as_secs_f64());
        std::hint::black_box(n);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let min = times[0];
    let med = times[times.len() / 2];
    let bytes = std::fs::metadata(path).map_or(0, |m| m.len()) as f64;
    let base = std::path::Path::new(path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(path);
    println!(
        "RESULT tool={label} file={base} min={min:.3} med={med:.3} mbps={:.1} rows={rows}",
        bytes / 1e6 / min
    );
}
