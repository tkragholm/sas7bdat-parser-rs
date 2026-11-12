//! Read one file once into owned columnar batches and print one JSON line, for
//! `benchmarks/run.py`.
//!
//! Usage: `bench_one <path> [threads]`. Without `threads` the scan uses the
//! default parallelism; `1` reads on one thread.
//!
//! The timed region is opening the file and collecting every batch. With
//! `BENCH_CHECKSUM=1` a checksum is computed afterwards, matching `benchmarks/read_py.py`: numeric columns give their
//! sum and missing count, string columns the total UTF-8 length after removing trailing
//! spaces. Date, datetime and time columns are left out.
#![allow(clippy::cast_precision_loss)]
use sas7bdat::{Dataset, OwnedColumnBuffer, Parallelism};
use std::fmt::Write as _;
use std::time::Instant;

fn is_valid(valid: Option<&Vec<u64>>, i: usize) -> bool {
    valid.is_none_or(|bits| bits.get(i / 64).is_some_and(|w| (w >> (i % 64)) & 1 == 1))
}

enum Sum {
    Num(f64, u64),
    Str(u64),
}

fn add(col: &OwnedColumnBuffer, acc: &mut Option<Sum>) {
    fn num<T: Copy>(
        values: &[T],
        valid: Option<&Vec<u64>>,
        f: impl Fn(T) -> f64,
        acc: &mut Option<Sum>,
    ) {
        let (mut s, mut n) = match acc.take() {
            Some(Sum::Num(s, n)) => (s, n),
            _ => (0.0, 0),
        };
        for (i, v) in values.iter().enumerate() {
            if is_valid(valid, i) {
                s += f(*v);
            } else {
                n += 1;
            }
        }
        *acc = Some(Sum::Num(s, n));
    }
    match col {
        OwnedColumnBuffer::F64 { values, valid } => num(values, valid.as_ref(), |v| v, acc),
        OwnedColumnBuffer::I64 { values, valid } => num(values, valid.as_ref(), |v| v as f64, acc),
        OwnedColumnBuffer::I32 { values, valid } => num(values, valid.as_ref(), f64::from, acc),
        OwnedColumnBuffer::Utf8 {
            offsets,
            data,
            valid,
            ..
        } => {
            let mut total = match acc.take() {
                Some(Sum::Str(t)) => t,
                _ => 0,
            };
            let offs = offsets.as_slice();
            for i in 0..offs.len().saturating_sub(1) {
                if !is_valid(valid.as_ref(), i) {
                    continue;
                }
                let s = usize::try_from(offs[i]).unwrap_or(0);
                let e = usize::try_from(offs[i + 1]).unwrap_or(0);
                let mut cell = &data[s..e];
                while let [rest @ .., b' '] = cell {
                    cell = rest;
                }
                total += cell.len() as u64;
            }
            *acc = Some(Sum::Str(total));
        }
        _ => {}
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = &args[1];
    let parallelism = match args.get(2).and_then(|s| s.parse::<usize>().ok()) {
        Some(1) => Parallelism::None,
        Some(n) => Parallelism::Threads(n),
        None => Parallelism::Auto,
    };

    let start = Instant::now();
    let ds = Dataset::open(path).expect("open");
    let batches = ds
        .scan()
        .with_parallelism(parallelism)
        .collect_batches()
        .expect("collect");
    let seconds = start.elapsed().as_secs_f64();

    let names: Vec<&str> = ds.columns().iter().map(|c| c.name.trim_end()).collect();
    let rows: usize = batches.iter().map(|b| b.row_count).sum();
    let checksum = if std::env::var("BENCH_CHECKSUM").is_ok_and(|v| v == "1") {
        let mut sums: Vec<Option<Sum>> = names.iter().map(|_| None).collect();
        for b in &batches {
            for (col, acc) in b.columns.iter().zip(sums.iter_mut()) {
                add(col, acc);
            }
        }
        let mut entries = String::new();
        for (name, sum) in names.iter().zip(&sums) {
            let entry = match sum {
                Some(Sum::Num(s, n)) => format!("\"{name}\":[\"num\",{s:e},{n}]"),
                Some(Sum::Str(t)) => format!("\"{name}\":[\"str\",{t}]"),
                None => continue,
            };
            if !entries.is_empty() {
                entries.push(',');
            }
            entries.push_str(&entry);
        }
        format!("{{{entries}}}")
    } else {
        "null".to_owned()
    };
    let mut out = String::new();
    let _ = write!(
        out,
        "{{\"tool\":\"sas7bdat-core\",\"seconds\":{seconds:.6},\"rows\":{rows},\"cols\":{},\"checksum\":{checksum}}}",
        names.len()
    );
    println!("{out}");
}
