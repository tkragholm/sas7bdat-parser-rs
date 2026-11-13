---
title: 'sas7bdat: a fast reader for SAS7BDAT files in Rust, Python and R'
tags:
  - Rust
  - Python
  - R
  - SAS
  - sas7bdat
  - Apache Arrow
  - Polars
  - data engineering
  - SIMD
authors:
  - name: Tobias Kragholm
    orcid: 0009-0008-4952-5023
    affiliation: 1
affiliations:
  - name: Mary Elizabeth's Hospital, Rigshospitalet, Copenhagen University Hospital, University of Copenhagen, Denmark
    index: 1
date: 6 July 2026
bibliography: paper.bib
---

# Summary

SAS7BDAT is the binary dataset format produced by SAS software. Decades of
government, clinical, and survey data are stored in it: national statistical
agencies distribute register extracts as `.sas7bdat` files, regulatory
submissions have long standardized on SAS formats, and public-use survey data
is routinely published in it. The format is proprietary and undocumented; open
implementations rest on public reverse-engineering work [@shotwell2013].

`sas7bdat` is a SAS7BDAT reader written in Rust. It combines memory-mapped
I/O, SIMD (single instruction, multiple data) string decoding, and parallel
page decoding. On a 12-core laptop it reads a 2.15 GB file at about 3.2 GB/s,
which is 7 to 100 times faster than pandas, pyreadstat, and haven in the
benchmarks below. The core crate streams rows or Apache Arrow [@arrow] record
batches. On top of it, a Polars [@polars] IO plugin (`scan_sas`) exposes lazy,
projection- and slice-pushdown-aware scanning to Python, and an R package
(`fastsas`) returns haven-compatible tibbles. The reader supports both SAS
compression schemes, run-length encoding (RLE) and Ross Data Compression
(RDC); 32- and 64-bit files of either endianness; wide tables with thousands
of columns; the text encodings known to the `encoding_rs` crate; tagged
missing values; and companion `.sas7bcat` catalogs for value labels. It reads
SAS7BDAT only; it does not write files or read SAS transport (XPORT), Stata,
or SPSS files.

# Statement of need

Researchers working with administrative registers, pharmacoepidemiological
databases, and public-use surveys frequently receive data only as SAS7BDAT
files, and increasingly process them in open toolchains rather than in SAS
itself. In Denmark, the two national providers of register data for research
both require SAS data files: Statistics Denmark for the data in its research
project databases [@dstprojektdb], and the Danish Health Data Authority for
extracts on its research platform [@sdsprojektdb]. Statistics Sweden also
delivers research extracts in SAS, among other formats [@scbleverans], while
Statistics Finland has made SAS a paid option on its remote-access system and
removes it from projects that do not ask to keep it [@statfisas]. Dropping the
SAS software does not convert the files already delivered, so they still need
a reader. The format is proprietary and undocumented, so researchers who do
not work in SAS depend on readers built by reverse engineering. In R and
Python, the most widely used routes into these files go through one of two
implementations: ReadStat [@readstat], the single-threaded C library behind
R's haven [@haven] and Python's pyreadstat [@pyreadstat], or the reader built
into pandas [@pandas; @mckinney2010]. Both are mature and widely used, and
neither decodes in parallel. On a 12-core laptop, reading a 2.15 GB American
Housing Survey file takes 67 s with haven and 21 s with pyreadstat. In
national register research, where one project may read hundreds of SAS
extracts, this conversion step sits in front of analysis stages (Polars,
Arrow, DuckDB) that are themselves parallel and vectorized.

The same 2.15 GB file loads in 0.67 s with the Rust core and 1.19 s through
the Polars plugin. The library was written for, and is used in, register
research on Statistics Denmark's secure research servers, where it reads SAS
register extracts directly into Polars pipelines. Deployment there is simple
because the Python package is published on PyPI as prebuilt wheels
(`sas7bdat-polars`, Python 3.12 or later) that install from a PyPI mirror
without a compiler or a system C library. The R package is at present
installed from source and needs a Rust toolchain.

<!-- TODO: if you can, add one measured server timing here (file size, rows,
time with the previous ReadStat-based step, time with this reader). -->

# State of the field

ReadStat [@readstat] is the reference C implementation and the backend of
haven and pyreadstat. pandas ships an independent reader, written in Python
with its decompression in Cython. Parso [@parso] is an independent Java reader
used by Spark connectors, and cppsas7bdat [@cppsas7bdat] is a C++17 reader.
polars-readstat [@polarsreadstat] is a Polars IO plugin for SAS, Stata, and
SPSS files.

ReadStat decodes a file in one pass on one thread and hands each value to a
callback as it is decoded. It can start at a row offset and stop at a row
limit, and pyreadstat's `read_file_multiprocessing` uses this to read row
ranges in separate processes and join the results. Decoding pages in parallel
inside ReadStat would mean reworking the core of a C library that haven,
pyreadstat and other readers share. Speeding up the pandas reader would help
pandas users only. `sas7bdat` began as a translation of ReadStat into Rust,
and the present code base is written around Arrow's columnar format, which
Polars and other Arrow-based tools read without conversion. A second
implementation also lets each reader be checked against the other: the
comparison described below shows where the two differ, in deleted rows, which
ReadStat 1.1.9 returns, and in the mapping of ISO-8859-1 text.

<!-- TODO: compare against polars-readstat on at least one of the benchmark
files, and adjust the next sentence to what the comparison shows. -->

`sas7bdat` differs from these in three ways. It decodes pages in parallel and
uses vectorized string handling throughout. It produces Arrow buffers
directly, so columnar consumers avoid a row-to-column transpose. It has
bindings for both of the ecosystems where SAS data is most used: a Polars
plugin with projection and slice pushdown, and an R package with
haven-compatible types, including `factor` conversion backed by a dictionary
encoder. A few small test files come from the pyreadstat and Parso test suites
and are listed with their licences in the repository. The code's relation to
ReadStat is described under AI usage disclosure.

Correctness is checked against ReadStat as an external oracle. A harness in
the repository (`examples/verify_readstat.rs`) decodes each file in a local
corpus of 329 public SAS files with both readers and compares every cell,
numbers within a relative tolerance of 5 × 10^-7^ plus an absolute tolerance
of 10^-6^, and text after trimming trailing spaces, with this reader's repair
of double-encoded text turned off because ReadStat does not repair it. Against
ReadStat built from its main branch in November 2026, 325 files match on every
cell, 12.8 million cells in all. Four are not compared. ReadStat rejects three
of them: a deliberately corrupt test file, which this reader also rejects; a
file with no columns; and a file in the CP1381 encoding, which ReadStat does
not support. The fourth is left out because ReadStat maps ISO-8859-1 strictly,
where this reader follows the WHATWG mapping to WINDOWS-1252. ReadStat 1.1.9,
the release that haven and pyreadstat ship, also returns rows that SAS has
marked as deleted; this reader excludes them, as ReadStat's main branch now
does [@readstat366]. The Polars plugin is also compared with pyreadstat on the
first 500 rows of each file, and the R package with haven. The corpus is drawn
from the test suites of ReadStat, pandas, and other readers, and from
public-use survey files; it is not redistributed.

# Software design

The library is organized as a small workspace: a core crate that owns all
format logic, plus thin bindings on top, a Polars IO plugin built with PyO3
and an R package built with extendr [@extendr]. Three design decisions drive
the performance profile.

*Columnar decoding.* Although SAS7BDAT stores rows, the decoder materializes
column buffers directly, so downstream Arrow, Polars, and R consumers avoid a
row-to-column transpose and per-row allocation. Row-oriented visitors remain
available for streaming use.

*Parallelism at the page level.* A SAS7BDAT file is a sequence of
independent pages. After a sequential metadata pass, data pages are decoded
in parallel with work-stealing (rayon), for both compressed and uncompressed
files.

*SIMD string handling.* Fixed-width string cells are trimmed, validated, and
transcoded with explicitly vectorized routines built on the portable
`fearless_simd` crate, on stable Rust. A `std::simd` backend is available
behind a feature flag for nightly toolchains. Cells that are pure ASCII are
borrowed directly
from the memory-mapped file; other cells are transcoded from the file's
declared encoding. Optional dictionary encoding, a cardinality sketch gating
a string interner, speeds up low-cardinality character columns into R
`factor`s and Polars `Categorical`s.

Numeric columns are IEEE-754 doubles stored in 3 to 8 bytes and are widened to
8 on read. SAS dates, datetimes, and times are converted from the 1960 epoch
to Arrow's temporal types. By default, bytes that are invalid in the declared
encoding are replaced and common double-encoding is repaired; a strict mode in
the Rust API fails on the first such cell instead. The Python and R bindings
use the default and do not yet expose strict mode. Encoding labels the reader
does not recognise are read as UTF-8.

In the core crate and the Polars plugin, a workspace lint flags every use of
unsafe code and continuous integration fails on it, except at five marked
places, each with a safety comment: the read-only memory map, two lines that
call the Windows API to detect network drives, and two in the Polars plugin at
the Arrow boundary. The R bindings sit outside the workspace and use unsafe
code where Rust writes into R vectors and reads R's interrupt flags.
Continuous integration also replays past fuzzing crashes under
AddressSanitizer and runs the R package's tests against haven on six bundled
SAS files. The tests that need the full corpus are run locally, because the
corpus is not in the repository: the oracle comparison, parity between the
parallel and sequential decoders, a check of every column's format string
against ReadStat, and a row-by-row comparison with the expected CSV files from
pandas' SAS test suite.

## Performance

Single-process wall-clock time to fully materialize three real public-use
survey files (Apple M3 Pro, 12 cores; warm cache, one untimed warm-up, minimum
of repeated runs; versions: ReadStat 1.1.9, pyreadstat 1.3.5, pandas 3.0.3,
haven 2.5.5). ReadStat-based readers and pandas are single-threaded by design.
All readers returned the same row counts on these files.

| Reader | AP VoteCast (99 MB) | NY Youth Tobacco (193 MB) | AHS 2013 (2.15 GB) |
|---|---:|---:|---:|
| `sas7bdat` core (12 threads) | **0.03 s** | **0.05 s** | **0.67 s** |
| Polars plugin (12 threads) | 0.04 s | 0.08 s | 1.19 s |
| `fastsas` R package (12 threads) | 0.13 s | 0.13 s | 4.47 s |
| `sas7bdat` core (1 thread) | 0.17 s | 0.19 s | 1.79 s |
| pandas | 0.38 s | 0.90 s | 5.02 s |
| pyreadstat | 1.10 s | 1.52 s | 20.95 s |
| haven | 2.10 s | 3.20 s | 66.83 s |

On one thread the core is 6 to 12 times faster than pyreadstat and 2 to 5
times faster than pandas, so parallel decoding is not the only source of the
difference. The R package's times include building R vectors.

# Research impact statement

The library is used for register-based epidemiological research on Statistics
Denmark's secure research servers, where it replaced ReadStat-based conversion
of SAS register extracts as the entry point of Polars-based analysis
pipelines. It is distributed on crates.io and PyPI. Outside users have
requested features, for example storing SAS labels and formats as Parquet
key-value metadata, which has since been implemented. The repository ships the
benchmark scripts and a corpus-profiling toolchain (`sas7bdat-corpus-profile`,
`sas7bdat-fixture-profile`), so the measurements reported here can be repeated
on any SAS7BDAT corpus.

# AI usage disclosure

AI coding assistants were used throughout development, from October 2025 to
November 2026, for code generation, refactoring, tests, benchmark scripts, and
documentation: OpenAI Codex (GPT-5-Codex to GPT-5.5) until May 2026,
Anthropic's Claude Code (Claude Opus 4.6 to 4.8, Opus 5 and 5.5, Fable 5, and
Sonnet 4.6) from April 2026, and Google's Gemini CLI (Gemini 3 Flash preview)
in a few sessions. This paper was drafted and revised with Claude. The author
made the design decisions and reviewed, modified, and validated all
AI-assisted output.

Development started in October 2025 from a prototype that used a c2rust
translation of ReadStat's C reader as its starting point and was rewritten in
Rust within two weeks. The present code base is a second rewrite, begun in
March 2026. Its catalog reader, file probe, and encoding lookup descend from
the first rewrite, and none of the translated code remains. ReadStat's MIT
licence is included in the repository's third-party notices. pandas, Parso,
and Shotwell's documentation served as references for the file format.
Correctness rests on the comparison with ReadStat described under State of the
field.

# Acknowledgements

This parser builds on the public reverse-engineering of the SAS7BDAT format,
in particular Matt Shotwell's format documentation [@shotwell2013], and on
ReadStat, pyreadstat, and haven as correctness oracles. The R binding is built
with extendr [@extendr]. This work was supported by the Novo Nordisk
Foundation (grant NNF23OC0083603) and BørneRiget Fonden.

SAS and SAS7BDAT are trademarks of SAS Institute Inc. This project is
independent of and not endorsed by SAS Institute Inc.

# References
