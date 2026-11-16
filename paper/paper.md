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
date: 16 November 2026
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
page decoding. On a 12-core laptop it reads a 2.15 GB file in 0.47 s, where
pandas, pyreadstat, and haven take 13 to 65 s. The core crate streams rows or Apache Arrow [@arrow] record
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
Housing Survey file takes 65 s with haven and 16 s with pyreadstat. In
national register research, where one project may read hundreds of SAS
extracts, this conversion step sits in front of analysis stages (Polars,
Arrow, DuckDB) that are themselves parallel and vectorized.

The same 2.15 GB file loads in 0.47 s with the Rust core and 0.65 s through
the Polars plugin. The library was written for, and is used in, register
research on Statistics Denmark's secure research servers, where it reads SAS
register extracts directly into Polars pipelines. Deployment there is simple
because the Python package is published on PyPI as prebuilt wheels
(`sas7bdat-polars`, Python 3.12 or later, Polars 1.41.1 or later) for Linux on
x86-64 and ARM64, Windows on x86-64, and Apple Silicon macOS. They install
from a PyPI mirror without a compiler or a system C library, and a separate
wheel (`sas7bdat-cli`) installs the command-line converter with no Python
dependencies. The R packages are not on CRAN; R-universe builds them as binary
packages for Windows, macOS, and Linux.

# State of the field

ReadStat [@readstat] is the reference C implementation and the backend of
haven and pyreadstat. pandas ships an independent reader, written in Python
with its decompression in Cython. Parso [@parso] is an independent Java reader
used by Spark connectors, and cppsas7bdat [@cppsas7bdat] is a C++17 reader.
polars-readstat [@polarsreadstat] is a Polars IO plugin for SAS, Stata, and
SPSS files, built on its own multi-threaded Rust reader.

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


`sas7bdat` differs from these in three ways. It decodes pages in parallel, as
only polars-readstat among them also does, and uses vectorized string handling
throughout. It produces Arrow buffers
directly, so columnar consumers avoid a row-to-column transpose. It has
bindings for both of the ecosystems where SAS data is most used: a Polars
plugin with projection and slice pushdown, and an R package with
haven-compatible types, including `factor` conversion backed by a dictionary
encoder. A few small test files come from the pyreadstat and Parso test suites
and are listed with their licences in the repository. The code's relation to
ReadStat is described under AI usage disclosure.

Correctness is checked against ReadStat as an external oracle. A harness in
the repository (`examples/verify_readstat.rs`) decodes each file in a local
corpus of 331 public SAS files with both readers and compares every cell,
numbers within a relative tolerance of 5 × 10^-7^ plus an absolute tolerance
of 10^-6^, and text after trimming trailing spaces, with this reader's repair
of double-encoded text turned off because ReadStat does not repair it. Against
ReadStat built from its development branch in November 2026 (commit f1f6826),
327 files match on every cell, 502 million cells in all, and the record of the
run is in the repository's `benchmarks/readstat/`. Four files are not
compared. ReadStat rejects three of them: a deliberately corrupt test file, which this reader also rejects; a
file with no columns; and a file in the CP1381 encoding, which ReadStat does
not support. The fourth is left out because ReadStat maps ISO-8859-1 strictly,
where this reader follows the WHATWG mapping to WINDOWS-1252. ReadStat 1.1.9,
the release that haven and pyreadstat ship, also returns rows that SAS has
marked as deleted; this reader excludes them, as ReadStat's development branch
now does [@readstat366]. The Polars plugin is also compared with pyreadstat on the
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

*Parallelism at the page level.* A SAS7BDAT file is a sequence of independent
pages. After a sequential metadata pass, data pages are cut into chunks of
about one output batch each and decoded on worker threads, for both compressed
and uncompressed files. Two choices bound the memory this takes. Batches are
delivered in file order, and decoding may run ahead of delivery by at most
twice as many chunks as there are workers. A caller that does not need file
order can switch ordering off, which the R package does unless a column
becomes a factor. With the default thread count, the number of workers is also
limited so that their estimated batches together stay under 1 GiB, so very
wide files decode on fewer threads. On the widest benchmark file this was
faster than using every core.

*SIMD string handling.* Fixed-width string cells are trimmed, validated, and
transcoded with explicitly vectorized routines built on the portable
`fearless_simd` crate, on stable Rust. Cells that are pure ASCII are
borrowed directly from the memory-mapped file; other cells are transcoded from
the file's declared encoding. The vector kernels are written for 64-byte
vectors, the width of one AVX-512 register, and fearless_simd builds each for
several instruction sets and picks AVX2 or AVX-512 at run time. The rest of
the code in the published x86-64 wheels is compiled for AVX2 (x86-64-v3),
because recent Intel consumer processors lack AVX-512 and a binary compiled
for it fails at import there. Apple Silicon builds target the M1 instruction
set. Optional dictionary encoding, a cardinality sketch gating a string
interner, speeds up turning low-cardinality character columns into R
`factor`s; the Polars plugin instead writes Arrow dictionary columns, one
dictionary per batch, which Polars reads as `Categorical`.

Numeric columns are IEEE-754 doubles stored in 3 to 8 bytes and are widened to
8 on read. SAS dates, datetimes, and times are converted from the 1960 epoch
to Arrow's temporal types, or left as the stored numbers if the caller asks.
By default, bytes that are invalid in the declared encoding are replaced and
common double-encoding is repaired. A strict mode, in the Rust API, both
bindings, and the command-line tool, fails on the first such cell instead. A
file whose header names the wrong encoding can be read in another one.
Encoding labels the reader does not recognise are read as UTF-8.

Local files are memory-mapped by default. On Windows the reader detects
network drives and reads them sequentially instead, since each access to a
mapped remote file is a network round trip; on other systems the caller
chooses with an `io_backend` option, which the Python, R and command-line
interfaces all expose. A file shorter than its header declares is rejected
when it is opened, before any rows are returned. A memory-mapped file that
another process truncates during a read can stop the process on Unix systems,
so files that may change while being read should use the buffered backend.

In the core crate and the Polars plugin, a workspace lint flags every use of
unsafe code and continuous integration fails on it, except at five marked
places, each with a safety comment: the read-only memory map, two lines that
call the Windows API to detect network drives, and two in the Polars plugin at
the Arrow boundary. The R bindings sit outside the workspace and use unsafe
code where Rust writes into R vectors and reads R's interrupt flags.
Continuous integration also replays past fuzzing crashes under
AddressSanitizer and runs the R package's tests against haven on six bundled
SAS files. It also runs the decoding tests on 22 files from the pandas and
Parso test suites, one for each format feature (both byte orders and
bitnesses, both compression schemes, deleted rows, multi-byte encodings, an
empty file), including parity between the parallel and sequential decoders and
a row-by-row comparison with pandas' expected CSV output. The comparison with
ReadStat needs the full corpus, which is not redistributed, and is run
locally.

## Performance

Each measurement is one process that reads one file once into the reader's
own in-memory table, timed inside the process so that interpreter start-up and
imports are excluded. The page cache is warm, the readers run interleaved in
an order that rotates between repetitions, and the table gives the median of
10 runs. Every reader is used with its defaults. The files are four public-use
survey files that `benchmarks/fetch.py` downloads: the 2013 American Housing
Survey national file (2.15 GB, 70,044 rows by 4,041 columns) and its household
(613 MB, 2,167 columns) and person files (26 MB, 148,342 rows by 82 columns),
and the 2021 Medical Expenditure Panel Survey full-year file (338 MB, 28,336
rows by 1,488 columns). After the timed runs, each reader's result is compared
with the Rust core's by row and column counts, per-column sums and missing
counts, and string lengths, leaving out date and time columns. All of them
agree. The machine is an Apple M3 Pro with 12 cores and 18 GB of memory. The
`sas7bdat` readers and polars-readstat run with their default thread counts.
ReadStat, pyreadstat, pandas, and haven are single-threaded. The versions are
ReadStat 1.1.9, pyreadstat 1.3.7, polars-readstat 0.25.0, pandas 3.1.1, Polars
2.1.0, haven 2.5.5, and R 4.6.2.

| Reader | AHS national | AHS household | MEPS | AHS person |
|---|---:|---:|---:|---:|
| `sas7bdat` core | **0.47 s** | **0.23 s** | **0.058 s** | **0.013 s** |
| `sas7bdat` core, one thread | 1.53 s | 0.71 s | 0.085 s | 0.077 s |
| Polars plugin | 0.65 s | 0.27 s | 0.071 s | 0.020 s |
| polars-readstat | 0.82 s | 0.38 s | 0.113 s | 0.027 s |
| `fastsas` (R) | 2.74 s | 1.34 s | 0.27 s | 0.31 s |
| ReadStat C library | 5.07 s | 2.33 s | 0.55 s | 0.23 s |
| pandas | 13.1 s | 5.37 s | 0.21 s | 0.73 s |
| pyreadstat | 15.7 s | 6.90 s | 1.66 s | 0.55 s |
| pyreadstat, 12 processes | 9.17 s | 3.34 s | 0.96 s | 0.59 s |
| haven (R) | 64.7 s | 28.6 s | 2.98 s | 3.41 s |

On one thread the core is 3 to 6 times faster than the ReadStat C library,
which stores every value in a column array as the core does, so parallel
decoding is not the only source of the difference. The Polars plugin is 1.3 to
1.6 times faster than polars-readstat. By default polars-readstat does not
keep the file's row order, which the plugin does. `fastsas` is 11 to 24 times
faster than haven. Its times include building the R data frame, which takes
longer than the decoding. pyreadstat's multiprocessing mode, which reads row
ranges in separate processes, cuts its time by 42 to 52% on the three larger
files. Converting to CSV with the command-line tools, `sas7bdat` is 10 to 27
times faster than the `readstat` program. On the national file the Polars
plugin's peak physical memory is 4.1 GB, against 4.4 GB for polars-readstat,
and `fastsas` peaks at 3.7 GB, against 2.4 GB for haven. The benchmark
scripts are in the repository's `benchmarks/` directory.

# Research impact statement

The library is used in register research by Mary Elizabeth's Hospital's Data
Powerhouse at Rigshospitalet, Copenhagen, which studies the health, social,
and economic consequences of severe illness in children, young people,
pregnant women, and their families using nationwide Danish registers
[@datapowerhouse]. The author has used it on Statistics Denmark's research
servers since its first release in January 2026. In November 2026 it converted
all of the group's SAS register data there, 1,469 files and 2.76 TB, to
Parquet in about two hours, and the group's researchers now work from that
Parquet. Before, they mostly loaded the SAS files into R with haven, which
reads them through ReadStat. Outside users have requested features, for
example storing SAS labels and formats as Parquet key-value metadata, which
has since been implemented. By 13 November 2026 the crate had been downloaded
about 7,600 times from crates.io and the Python package about 4,000 times from
PyPI, mirrors excluded. The repository ships the benchmark scripts and a
corpus-profiling toolchain (`sas7bdat-corpus-profile`,
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
March 2026. It replaced the repository's earlier history, so the public commit
history starts on 29 March 2026, while releases on crates.io go back to
January 2026. In the present code base, the catalog reader, file probe, and
encoding lookup descend from the first rewrite, and none of the translated
code remains. ReadStat's MIT licence is included in the repository's
third-party notices. pandas, Parso, and Shotwell's documentation served as
references for the file format. Correctness rests on the comparison with
ReadStat described under State of the field.

# Acknowledgements

This parser builds on the public reverse-engineering of the SAS7BDAT format,
in particular Matt Shotwell's format documentation [@shotwell2013], and on
ReadStat, pyreadstat, and haven as correctness oracles. The R binding is built
with extendr [@extendr]. This work was supported by the Novo Nordisk
Foundation (grant NNF23OC0083603) and BørneRiget Fonden.

SAS and SAS7BDAT are trademarks of SAS Institute Inc. This project is
independent of and not endorsed by SAS Institute Inc.

# References
