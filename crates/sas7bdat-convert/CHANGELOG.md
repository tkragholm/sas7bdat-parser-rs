# Changelog

All notable changes to the `sas7bdat-convert` crate. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.5.0] - 2026-11-26

### Added

- `ColumnPlan`, set through `ConvertOptions::column_plan`, says how the written columns are named and typed: a case rule for every name, a rename map that overrides it, constant columns appended after the file's own (one can hold the input's path), and the Arrow types of strings, timestamps and times. Parquet takes all of it. CSV and TSV take the names and constant columns.
- With `parquet_metadata`, the embedded SAS metadata gives a renamed column's written name as `output_name`.

### Changed

- SAS times are written as Parquet `TIME` in microseconds by default, which DuckDB reads as `TIME` and R's arrow as `hms`. A file holding a time outside one day fails with the column named. `TimeType::Duration` writes the nanosecond durations of earlier versions.
- `ConvertOptions`, `WriteOptions` and `DelimitedWriteOptions` each gain a field, and `RowGroupPipeline::new` takes the resolved plan, so code that builds them field by field needs updating.

## [0.4.2] - 2026-11-15

### Fixed

- Requires sas7bdat 0.10.2 or later. The converter uses `ReadAhead`, which 0.10.2 added, but asked only for 0.10, so a project whose lockfile held sas7bdat 0.10.0 or 0.10.1 failed to compile after updating the converter alone.

Earlier versions are described in their [GitHub releases](https://github.com/tkragholm/sas7bdat-parser-rs/releases?q=sas7bdat-convert).
