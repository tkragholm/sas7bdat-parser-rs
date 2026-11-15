# Changelog

All notable changes to the `sas7bdat-convert` crate. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.4.2] - 2026-11-15

### Fixed

- Requires sas7bdat 0.10.2 or later. The converter uses `ReadAhead`, which 0.10.2 added, but asked only for 0.10, so a project whose lockfile held sas7bdat 0.10.0 or 0.10.1 failed to compile after updating the converter alone.

Earlier versions are described in their [GitHub releases](https://github.com/tkragholm/sas7bdat-parser-rs/releases?q=sas7bdat-convert).
