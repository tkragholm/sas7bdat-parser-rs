//! Times the four SIMD kernel shapes of whichever backend is live.
//!
//! Only one vector backend compiles at a time, so a within-run comparison is not
//! possible. Run it once per configuration and compare across runs — same machine,
//! same bench code, so the numbers are comparable:
//!
//! ```sh
//! cargo bench --bench simd_backends --features internal-bench --no-default-features
//! cargo bench --bench simd_backends --features internal-bench
//! cargo bench --bench simd_backends --features internal-bench,nightly-simd   # nightly
//! ```
//!
//! `scripts/simd-matrix.sh` drives that sweep across `-C target-cpu` levels, which is
//! the axis that matters on x86-64: `std::simd` has no runtime dispatch and lowers to
//! whatever was enabled at compile time, while `fearless_simd` picks at runtime. The
//! interesting question is how much of the gap `target-cpu` closes.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use sas7bdat::simd;
use std::hint::black_box;

const ROWS: usize = 1 << 20;

/// Row stride for the gather benchmark: this column plus two other 8-byte columns.
const STRIDE: usize = 24;

/// Which backend this binary was built with, for the report header.
// Single token, no spaces: it is the first path segment of every criterion
// benchmark id, and scripts/simd-matrix.sh splits on it.
const BACKEND: &str = if cfg!(feature = "nightly-simd") {
    "portable"
} else if cfg!(feature = "simd") {
    "fearless"
} else {
    "scalar"
};

/// Deterministic LCG — identical data on every machine, no `rand` dependency.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }
}

/// Integral f64 bit patterns in i32 range, with `missing_pct` SAS missing sentinels
/// mixed in — the shape the numeric kernels actually see.
fn numeric_bits(missing_pct: u64) -> Vec<u64> {
    let mut rng = Lcg(0x5EED);
    (0..ROWS)
        .map(|_| {
            let raw = rng.next();
            if raw % 100 < missing_pct {
                0x7FF0_0000_0000_0001
            } else {
                #[allow(clippy::cast_possible_truncation)]
                let n = (raw >> 32) as i32;
                f64::from(n / 4).to_bits()
            }
        })
        .collect()
}

/// A fixed-width character column padded with trailing spaces, as SAS stores them.
fn padded(width: usize, content: usize) -> Vec<u8> {
    let mut rng = Lcg(0x00C0_FFEE);
    let mut buf = vec![b' '; width * 4096];
    for row in buf.chunks_mut(width) {
        for slot in row.iter_mut().take(content) {
            #[allow(clippy::cast_possible_truncation)]
            let byte = b'a' + (rng.next() % 26) as u8;
            *slot = byte;
        }
    }
    buf
}

fn numeric(c: &mut Criterion) {
    for pct in [0u64, 2, 25] {
        let data = numeric_bits(pct);
        let mut g = c.benchmark_group(format!("{BACKEND}/classify_missing/{pct}pct"));
        g.throughput(Throughput::Bytes((data.len() * 8) as u64));
        g.bench_function("kernel", |b| {
            b.iter(|| black_box(simd::classify_missing_raw_bits(black_box(&data))));
        });
        g.finish();
    }

    // Full scan with no early exit: the common case, since a column that IS
    // downgradeable to i32 has to be proven so end to end.
    let data = numeric_bits(0);
    let (min, max) = (f64::from(i32::MIN), f64::from(i32::MAX));
    assert_eq!(
        simd::first_non_integral_in_range_index(&data, None, min, max),
        None,
        "bench data should be fully downgradeable, or this measures an early exit"
    );
    let mut g = c.benchmark_group(format!("{BACKEND}/first_non_integral"));
    g.throughput(Throughput::Bytes((data.len() * 8) as u64));
    g.bench_function("kernel", |b| {
        b.iter(|| {
            black_box(simd::first_non_integral_in_range_index(
                black_box(&data),
                None,
                min,
                max,
            ))
        });
    });
    g.finish();

    // The column-level converter, which is what the materializers call. Measured
    // per column rather than per chunk on purpose: a dispatching backend selects
    // once here, and going through the per-chunk kernel instead used to cost a
    // cross-`#[target_feature]` call per 8 rows.
    let valid: Vec<u64> = (0..data.len().div_ceil(64)).map(|i| !(i as u64)).collect();
    let mut g = c.benchmark_group(format!("{BACKEND}/convert_column_i64"));
    g.throughput(Throughput::Bytes((data.len() * 8) as u64));
    g.bench_function("kernel", |b| {
        b.iter(|| {
            let mut out: Vec<i64> = Vec::with_capacity(data.len());
            simd::convert_column_i64(black_box(&data), Some(&valid), &mut out, |x| x);
            black_box(out)
        });
    });
    g.finish();

    // The strided gather, the dominant numeric decode path. A stride of 24 is a row
    // with two other 8-byte columns beside this one.
    let stride = STRIDE;
    let mut page = vec![0u8; data.len() * stride + 8];
    for (i, &bits) in data.iter().enumerate() {
        page[i * stride..i * stride + 8].copy_from_slice(&bits.to_le_bytes());
    }
    let mut g = c.benchmark_group(format!("{BACKEND}/gather_missing"));
    g.throughput(Throughput::Bytes((data.len() * 8) as u64));
    g.bench_function("kernel", |b| {
        b.iter(|| {
            let mut out: Vec<u64> = Vec::with_capacity(data.len());
            let missing = simd::gather_missing(black_box(&page), 0, stride, data.len(), &mut out);
            black_box((out, missing))
        });
    });
    g.finish();
}

fn strings(c: &mut Criterion) {
    // width 256 is a wide free-text column, where the vector path engages.
    //
    // width 16 is the typical SAS char column — and note this measures something
    // production does NOT do: `scan::string` gates on `< 64` and calls the scalar
    // kernel directly, so it never reaches a backend's wide kernel with a narrow
    // slice. What the width-16 row therefore shows is the per-call cost of getting
    // to the fallback — for `fearless` that includes a `dispatch!` — which is worth
    // knowing but must not be read as the cost production pays.
    for (width, content) in [(16usize, 9usize), (256, 140)] {
        let data = padded(width, content);

        let mut g = c.benchmark_group(format!("{BACKEND}/trim_trailing/width_{width}"));
        g.throughput(Throughput::Bytes(data.len() as u64));
        g.bench_function("kernel", |b| {
            b.iter(|| {
                let mut n = 0usize;
                for row in black_box(&data).chunks(width) {
                    n += simd::trim_trailing_space_or_nul_wide(row).len();
                }
                black_box(n)
            });
        });
        g.finish();

        let mut g = c.benchmark_group(format!("{BACKEND}/is_ascii/width_{width}"));
        g.throughput(Throughput::Bytes(data.len() as u64));
        g.bench_function("kernel", |b| {
            b.iter(|| {
                let mut n = 0usize;
                for row in black_box(&data).chunks(width) {
                    n += usize::from(simd::is_ascii_wide(row));
                }
                black_box(n)
            });
        });
        g.finish();
    }
}

criterion_group!(benches, numeric, strings);
criterion_main!(benches);
