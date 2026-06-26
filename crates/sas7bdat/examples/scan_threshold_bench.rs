//! Find the parallel/serial crossover for SAS7BDAT page decode on THIS hardware.
//!
//! For every `.sas7bdat` under the given path, this decodes the whole file at
//! several parallelism levels (serial, then N threads) and reports where threads
//! start to beat a single core. The decode is driven through `visit_owned_batches`
//! — the exact streaming path the Polars plugin uses — so the numbers reflect the
//! real register scan, not a microbenchmark.
//!
//! The page cache is warmed once per file before timing, so this isolates the
//! CPU/coordination crossover (the thing that decides "is there enough decode work
//! to justify spawning threads") from cold-disk I/O variance.
//!
//! Usage:
//! ```text
//! cargo run --release --example scan_threshold_bench -- <dir-or-file> [reps] [threads_csv]
//!
//!   <dir-or-file>  directory searched recursively for *.sas7bdat, or a single file
//!   reps           timed repetitions per config, median reported (default 5)
//!   threads_csv    parallelism levels to try, e.g. "2,4,8,16" (default: 2,4,8 capped
//!                  to logical cores)
//! ```
//!
//! Output: one line per file (size, rows, pages, serial vs best-parallel, speedup,
//! best thread count) sorted by decode size, then the host's observed crossover and
//! a verdict on whether the plugin's generic grain-size default is safe here — i.e.
//! whether it already stays serial below the point where threads start to pay off.
//! This validates the default; it is not required to operate the plugin.

// Timing/size/speedup math casts integer counters to f64 for display; precision loss is fine here.
#![allow(clippy::cast_precision_loss)]

use sas7bdat::{Dataset, Parallelism};
use std::{
    env,
    ops::ControlFlow,
    path::{Path, PathBuf},
    time::Instant,
};

const SPEEDUP_WORTH_IT: f64 = 1.10; // parallel must beat serial by >10% to count

// The plugin's generic grain default: each worker needs ≥ this many decoded bytes, so
// parallelism only engages at ≥ 2× this (the smallest split is 2 workers). Kept in sync
// with DEFAULT_MIN_BYTES_PER_WORKER in the polars-plugin. This benchmark VALIDATES that
// default on the current host — it is not required to operate the plugin.
const GRAIN_MIN_BYTES_PER_WORKER: u64 = 4 * 1024 * 1024;

struct FileResult {
    path: PathBuf,
    decode_bytes: u64,
    rows: u64,
    pages: u64,
    compression: String,
    serial_ns: u128,
    best_parallel_ns: u128,
    best_threads: usize,
}

impl FileResult {
    fn speedup(&self) -> f64 {
        if self.best_parallel_ns == 0 {
            0.0
        } else {
            self.serial_ns as f64 / self.best_parallel_ns as f64
        }
    }
}

fn find_sas_files(root: &Path, out: &mut Vec<PathBuf>) {
    if root.is_file() {
        if root.extension().and_then(|e| e.to_str()) == Some("sas7bdat") {
            out.push(root.to_path_buf());
        }
        return;
    }
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            find_sas_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("sas7bdat") {
            out.push(path);
        }
    }
}

fn time_decode(ds: &Dataset, parallelism: Parallelism) -> Result<(u128, u64), String> {
    let start = Instant::now();
    let mut rows: u64 = 0;
    ds.scan()
        .with_parallelism(parallelism)
        .visit_owned_batches(|batch| {
            rows = rows.saturating_add(batch.row_count as u64);
            Ok(ControlFlow::Continue(()))
        })
        .map_err(|err| err.to_string())?;
    Ok((start.elapsed().as_nanos(), rows))
}

fn median_ns(ds: &Dataset, parallelism: Parallelism, reps: usize) -> Result<u128, String> {
    let mut samples = Vec::with_capacity(reps);
    for _ in 0..reps.max(1) {
        samples.push(time_decode(ds, parallelism)?.0);
    }
    samples.sort_unstable();
    Ok(samples[samples.len() / 2])
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn human_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    let b = bytes as f64;
    if b >= MIB {
        format!("{:.1} MiB", b / MIB)
    } else if b >= KIB {
        format!("{:.1} KiB", b / KIB)
    } else {
        format!("{bytes} B")
    }
}

fn ms(ns: u128) -> f64 {
    ns as f64 / 1_000_000.0
}

fn bench_file(path: &Path, reps: usize, thread_levels: &[usize]) -> Result<FileResult, String> {
    let ds = Dataset::open(path).map_err(|err| err.to_string())?;
    let meta = ds.metadata();
    let decode_bytes = meta.row_count.saturating_mul(u64::from(meta.row_len));
    let compression = format!("{:?}", meta.compression);
    let rows = meta.row_count;
    let pages = meta.page_count;

    // Warm the page cache + JIT-y first-touch effects so timing reflects decode work.
    let _ = time_decode(&ds, Parallelism::None)?;

    let serial_ns = median_ns(&ds, Parallelism::None, reps)?;

    let mut best_parallel_ns = u128::MAX;
    let mut best_threads = 1;
    for &threads in thread_levels {
        if threads <= 1 {
            continue;
        }
        let ns = median_ns(&ds, Parallelism::Threads(threads), reps)?;
        if ns < best_parallel_ns {
            best_parallel_ns = ns;
            best_threads = threads;
        }
    }
    if best_parallel_ns == u128::MAX {
        best_parallel_ns = serial_ns;
    }

    Ok(FileResult {
        path: path.to_path_buf(),
        decode_bytes,
        rows,
        pages,
        compression,
        serial_ns,
        best_parallel_ns,
        best_threads,
    })
}

/// Recommend the floor as the smallest decode size from which the larger files are
/// *reliably* parallel-friendly (≥80% clear the speedup bar). Robust to a single noisy
/// mid-size sample, unlike "largest file that lost". `results` must be sorted ascending
/// by `decode_bytes`. Returns `last + 1` (i.e. "rarely worth it") if no such size holds.
fn recommend_threshold(results: &[FileResult]) -> u64 {
    const TAIL_FRACTION: f64 = 0.80;
    for i in 0..results.len() {
        let tail = &results[i..];
        let cleared = tail
            .iter()
            .filter(|r| r.speedup() >= SPEEDUP_WORTH_IT)
            .count();
        if cleared as f64 / tail.len() as f64 >= TAIL_FRACTION {
            return results[i].decode_bytes;
        }
    }
    results
        .last()
        .map_or(u64::MAX, |r| r.decode_bytes.saturating_add(1))
}

#[allow(clippy::too_many_lines)] // bench driver: argument parsing + sweep + reporting in one place
fn main() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let target = args
        .next()
        .ok_or("usage: scan_threshold_bench <dir-or-file> [reps] [threads_csv]")?;
    let reps = args
        .next()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(5);

    let cores = std::thread::available_parallelism().map_or(8, std::num::NonZeroUsize::get);
    let thread_levels: Vec<usize> = args.next().map_or_else(
        || {
            [2usize, 4, 8]
                .iter()
                .copied()
                .filter(|n| *n <= cores)
                .collect()
        },
        |csv| {
            csv.split(',')
                .filter_map(|s| s.trim().parse::<usize>().ok())
                .filter(|n| *n > 1)
                .collect()
        },
    );
    let thread_levels = if thread_levels.is_empty() {
        vec![2]
    } else {
        thread_levels
    };

    let mut files = Vec::new();
    find_sas_files(Path::new(&target), &mut files);
    files.sort();
    if files.is_empty() {
        return Err(format!("no .sas7bdat files found under {target}"));
    }

    eprintln!(
        "Benchmarking {} file(s), reps={}, thread levels={:?}, cores={}",
        files.len(),
        reps,
        thread_levels,
        cores
    );

    let mut results: Vec<FileResult> = Vec::new();
    for path in &files {
        match bench_file(path, reps, &thread_levels) {
            Ok(r) => {
                eprintln!("  done: {}", path.display());
                results.push(r);
            }
            Err(err) => eprintln!("  SKIP {} ({err})", path.display()),
        }
    }
    if results.is_empty() {
        return Err("no files decoded successfully".to_owned());
    }

    results.sort_by_key(|r| r.decode_bytes);

    println!();
    println!(
        "{:>10}  {:>11}  {:>7}  {:>10}  {:>12}  {:>8}  {:>5}  {:>11}  file",
        "decode_MiB", "rows", "pages", "serial_ms", "best_par_ms", "speedup", "thr", "compression"
    );
    println!("{}", "-".repeat(110));
    for r in &results {
        let flag = if r.speedup() >= SPEEDUP_WORTH_IT {
            ""
        } else {
            "  (serial wins)"
        };
        println!(
            "{:>10.2}  {:>11}  {:>7}  {:>10.2}  {:>12.2}  {:>7.2}x  {:>5}  {:>11}  {}{}",
            mib(r.decode_bytes),
            r.rows,
            r.pages,
            ms(r.serial_ns),
            ms(r.best_parallel_ns),
            r.speedup(),
            r.best_threads,
            r.compression,
            r.path.file_name().and_then(|n| n.to_str()).unwrap_or("?"),
            flag,
        );
    }

    let observed_crossover = recommend_threshold(&results);
    let worth_it = results
        .iter()
        .filter(|r| r.speedup() >= SPEEDUP_WORTH_IT)
        .count();
    // The generic default only parallelises files of ≥ 2 grains (smallest split = 2
    // workers). If the observed crossover sits at or below that floor, the default is
    // safe everywhere on this host with zero tuning.
    let default_floor = GRAIN_MIN_BYTES_PER_WORKER.saturating_mul(2);

    println!();
    println!(
        "{} of {} files cleared the {:.0}% speedup bar.",
        worth_it,
        results.len(),
        (SPEEDUP_WORTH_IT - 1.0) * 100.0
    );
    println!(
        "Observed crossover (≥80% of larger files beat serial): ~{} ({} B).",
        human_bytes(observed_crossover),
        observed_crossover
    );
    println!(
        "Generic default parallelises at ≥ ~{} (2 × {}/worker grain).",
        human_bytes(default_floor),
        human_bytes(GRAIN_MIN_BYTES_PER_WORKER),
    );
    if observed_crossover <= default_floor {
        println!(
            "✓ Defaults are safe here — they stay serial below the floor, which already \
             sits at/above this host's crossover. No tuning needed."
        );
    } else {
        println!(
            "⚠ This host's crossover is above the default floor — the defaults would \
             parallelise some files (~{}–{}) that don't benefit. If that matters, raise \
             SAS7BDAT_SCAN_MIN_BYTES_PER_WORKER toward ~{}.",
            human_bytes(default_floor),
            human_bytes(observed_crossover),
            human_bytes(observed_crossover / 2),
        );
    }

    Ok(())
}
