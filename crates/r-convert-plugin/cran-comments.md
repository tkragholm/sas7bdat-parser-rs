## Test environments

* macOS 26.6 (arm64), R 4.6.2, rustc 1.97.1, local
* Ubuntu 24.04 (x86_64), R release, GitHub Actions
* Windows Server 2022 (x86_64), R release with Rtools, GitHub Actions
* win-builder, R 4.6.2 and R-devel (2026-11-17 r90926)

## R CMD check results

0 errors | 0 warnings | 1 note

* This is a new submission.

The source tarball is about 6 MB. Most of it is the bundled Rust sources in
`src/rust/vendor.tar.xz`. The package writes Parquet, so the bundle contains
the Apache Arrow and Parquet crates and the zstd codec. Test suites,
benchmarks and example data are removed from the bundle before it is packed.

The installed size is reported as about 6 MB on Linux, almost all of it the
compiled library. It is built with link-time optimisation and one codegen unit,
and local symbols are stripped at link time.

## Rust

The package is implemented in Rust and follows the CRAN guidance on using Rust.
All crates are bundled as source in `src/rust/vendor.tar.xz`, and cargo builds
from them offline, with two jobs and with `CARGO_HOME` inside the build
directory. The rustc and cargo versions are printed before compilation, and the
build tree is removed after linking. The authors and licences of the bundled
crates are listed in `inst/AUTHORS`.

`SystemRequirements` states rustc >= 1.89, the highest minimum version declared
by a bundled crate.
