# Changelog

All notable changes to the `sas7bdat-cli` Python package. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.9.8] - 2026-11-26

### Added

- `convert` takes `--names`, `--rename OLD=NEW`, `--add-column NAME=VALUE` and `--source-column NAME`, to set the written column names and add constant columns, and `--string-type`, `--timestamp-unit`, `--time-type` and `--time-unit`, to set the Parquet types.

### Changed

- SAS times are written as Parquet `TIME` in microseconds by default, which DuckDB reads as `TIME`. A file holding a time outside one day fails with the column named. `--time-type duration` writes the nanosecond durations of earlier versions.

### Fixed

- Count a sample's distinct values exactly, which 32-bit targets build.

Built with sas7bdat 0.10.4, sas7bdat-convert 0.5.0.

## [0.9.7] - 2026-11-15

### Changed

- The Linux ARM64 wheel is built and tested on ARM.

Built with sas7bdat 0.10.3 and sas7bdat-convert 0.4.2. Conversion is unchanged.
