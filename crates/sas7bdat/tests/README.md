# Test Suite Notes

The core tests are Rust-only and run by default. Optional cross-tool checks can
be enabled when external tooling is installed.

## Optional external checks

- `SAS7BDAT_VERIFY_READSTAT=1` runs ReadStat CLI comparisons (requires `readstat`).
- `SAS7BDAT_READSTAT_BIN=path` points to a specific ReadStat CLI binary.
- `SAS7BDAT_READSTAT_FAIL_ON_MISMATCH=1` fails the test suite when mismatches are detected.
- `SAS7BDAT_READSTAT_REPORT=path` writes a JSON report of unsupported fixtures/mismatches.
- `SAS7BDAT_VERIFY_CPP=1` runs cppsas7bdat comparisons (requires C++ benchmark build).
- `SAS7BDAT_CPP_FAIL_ON_MISMATCH=1` fails the test suite when mismatches are detected.
- `SAS7BDAT_CPP_REPORT=path` writes a JSON report of unsupported fixtures/mismatches.
- `SAS7BDAT_VERIFY_CSHARP=1` runs Sas7Bdat.Core comparisons (requires .NET SDK).
- `SAS7BDAT_CSHARP_FAIL_ON_MISMATCH=1` fails the test suite when mismatches are detected.
- `SAS7BDAT_CSHARP_REPORT=path` writes a JSON report of unsupported fixtures/mismatches.

Python/R cross-tool comparisons (pandas/pyreadstat/haven) are now expected to
live in the bindings test suites once those bindings are in place.

External snapshots are generated at runtime in a temp directory and are not
committed to the repository.
