## Test environments

* macOS 26.6 (arm64), R 4.6.2, rustc 1.97.1, local
* Ubuntu 24.04 (x86_64), R release, GitHub Actions
* Windows Server 2022 (x86_64), R release with Rtools, GitHub Actions
* win-builder, R 4.6.2, R-devel (2026-11-17 r90926) and R 4.5.3

## R CMD check results

0 errors | 0 warnings | 1 note

* This is a new submission.

POSIXct, flagged as possibly misspelled, is the R class of the date-time
columns the package returns.

Win-builder's R-release and R-devel check folders for fastsas still hold a test
file from an earlier upload, tests/testthat/test-nul.R, which fails on Windows.
The package no longer contains it, and the check passes in a fresh folder, as
on win-builder's R 4.5.3.

## Rust

The package is implemented in Rust and follows the CRAN guidance on using Rust.
All crates are bundled as source in `src/rust/vendor.tar.xz`, and cargo builds
from them offline, with two jobs and with `CARGO_HOME` inside the build
directory. The rustc and cargo versions are printed before compilation, and the
build tree is removed after linking. The authors and licences of the bundled
crates are listed in `inst/AUTHORS`.

`SystemRequirements` states rustc >= 1.89, the highest minimum version declared
by a bundled crate.
