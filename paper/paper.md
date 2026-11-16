---
title: 'The sas7bdat crate: a parallel SAS7BDAT reader for Rust, Python and R'
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

SAS7BDAT is the binary dataset format of SAS software. Government, clinical,
and survey data are stored in it: national statistical agencies distribute
register extracts as `.sas7bdat` files, and public-use survey data is
routinely published in it. The format is proprietary and undocumented, and
open implementations rest on public reverse engineering [@shotwell2013].

`sas7bdat` is a SAS7BDAT reader written in Rust. It decodes pages in parallel,
handles strings with vector instructions, and produces Apache Arrow [@arrow]
record batches or rows. On a 12-core laptop it reads a 2.15 GB file in 0.47 s,
where pandas, pyreadstat, and haven take 13 to 65 s. A Polars [@polars] IO
plugin (`scan_sas`) brings lazy scanning with projection and slice pushdown to
Python, and an R package (`fastsas`) returns haven-compatible tibbles. The
reader handles both SAS compression schemes, 32- and 64-bit files of either
byte order, tables with thousands of columns, tagged missing values, and
value-label catalogs. It reads SAS7BDAT only.

# Statement of need

Researchers working with administrative registers, pharmacoepidemiological
databases, and public-use surveys often receive data only as SAS7BDAT files,
and increasingly analyse them outside SAS. In Denmark, both national providers
of register data for research require SAS data files: Statistics Denmark in
its research project databases [@dstprojektdb], and the Danish Health Data
Authority on its research platform [@sdsprojektdb]. Statistics Sweden also
delivers research extracts in SAS [@scbleverans], and Statistics Finland has
made SAS a paid option on its remote-access system [@statfisas]. Dropping the
SAS software does not convert the files already delivered, so they still need
a reader.

In R and Python, the most widely used routes into these files are ReadStat
[@readstat], the single-threaded C library behind R's haven [@haven] and
Python's pyreadstat [@pyreadstat], and the reader built into pandas [@pandas;
@mckinney2010]. Both are mature, and neither decodes in parallel. In national
register research, where one project may read hundreds of SAS extracts, this
conversion sits in front of analysis tools (Polars, Arrow, DuckDB) that are
themselves parallel. The library was written for register research on
Statistics Denmark's secure research servers, where it reads SAS extracts
directly into Polars pipelines. Its Python packages are prebuilt wheels for
Linux, Windows, and macOS that install from a PyPI mirror without a compiler,
and R-universe builds the R packages as binaries.

# State of the field

ReadStat [@readstat] is the reference C implementation and the backend of
haven and pyreadstat. pandas ships an independent reader written in Python and
Cython. Parso [@parso] is a Java reader used by Spark connectors,
cppsas7bdat [@cppsas7bdat] a C++17 reader, and polars-readstat
[@polarsreadstat] a Polars IO plugin for SAS, Stata, and SPSS files built on
its own multi-threaded Rust reader.

ReadStat decodes a file in one pass on one thread and hands each value to a
callback. pyreadstat's `read_file_multiprocessing` works around this by
reading row ranges in separate processes and joining the results. Decoding
pages in parallel inside ReadStat would mean reworking the core of a library
that haven, pyreadstat, and other readers share, and speeding up the pandas
reader would help pandas users only. `sas7bdat` began as a translation of
ReadStat into Rust and is built around Arrow's columnar format.

It differs from the readers above in three ways. It decodes pages in parallel,
as only polars-readstat also does, with vectorized string handling throughout.
It writes Arrow buffers directly, so columnar consumers avoid a row-to-column
transpose. And it has bindings for both ecosystems where SAS data is most
used: a Polars plugin with projection and slice pushdown, and an R package
with haven-compatible types, including `factor` columns. A second
implementation also lets each reader be checked against the other, as
described below.

# Software design

The library is a small workspace: a core crate that owns all format logic,
with thin bindings on top, a Polars plugin built with PyO3 and an R package
built with extendr [@extendr]. Three decisions shape its performance; the
repository's `DESIGN.md` describes them in full.

*Columnar decoding.* SAS7BDAT stores rows, but the decoder fills column
buffers directly, so Arrow, Polars, and R consumers need no transpose and no
per-row allocation. Row-oriented visitors remain for streaming use.

*Parallelism at the page level.* A SAS7BDAT file is a sequence of independent
pages. After a sequential metadata pass, data pages are cut into chunks of
about one output batch and decoded on worker threads, for compressed and
uncompressed files alike. Batches arrive in file order, and the memory that
decoding ahead of delivery may hold is bounded.

*Vectorized strings.* Fixed-width string cells are trimmed, validated, and
transcoded with kernels built on the `fearless_simd` crate, which compiles
each kernel for several instruction sets and picks AVX2 or AVX-512 at run
time, on stable Rust. ASCII cells are borrowed from the memory-mapped file;
others are transcoded from the declared encoding. The R package turns
low-cardinality text columns into `factor`s through a dictionary encoder, and
the Polars plugin writes Arrow dictionary columns, which Polars reads as
`Categorical`.

Dates, datetimes, and times are converted from SAS's 1960 epoch to Arrow's
temporal types, or left as the stored numbers. Bytes that are invalid in the
declared encoding are replaced and a common double encoding is repaired,
unless a strict mode is set, which every interface offers. Local files are
memory-mapped, except network drives on Windows, which are read sequentially.

The reader is checked against ReadStat as an external oracle. A harness in the
repository decodes each file in a local corpus of 331 public SAS files with
both readers and compares every cell. Against ReadStat's development branch
(commit f1f6826, November 2026), 327 files match on every cell, 502 million
cells in all. ReadStat rejects three of the others, and one is left out
because the two readers map ISO-8859-1 differently. ReadStat 1.1.9, the
release that haven and pyreadstat ship, also returns rows that SAS has marked
as deleted, which this reader leaves out, as ReadStat's development branch now
does [@readstat366]. The record of the run and its tolerances are in
`benchmarks/readstat/`. Continuous integration runs the decoding tests on 22
tracked files, one for each format feature, replays past fuzzing crashes under
AddressSanitizer, and keeps unsafe code in the core and the Polars plugin to
five marked places.

## Performance

Each measurement is one fresh process that reads one file once into the
reader's own in-memory table, timed inside the process so that interpreter
start-up is excluded. The page cache is warm, the readers run interleaved,
and the table gives the median of 10 runs. Every reader runs with its
defaults, and its result is checked against the Rust core's. The files are
four public-use survey files: the 2013 American Housing Survey national file
(2.15 GB, 70,044 rows by 4,041 columns), household file (613 MB, 2,167
columns), and person file (26 MB, 82 columns), and the 2021 Medical
Expenditure Panel Survey full-year file (338 MB, 1,488 columns). The machine
is an Apple M3 Pro with 12 cores. The method, versions, and records are in
the repository's `benchmarks/` directory.

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

On one thread the core is 3 to 6 times faster than the ReadStat C library, so
parallel decoding is not the only source of the difference. The Polars plugin
is 1.3 to 1.6 times faster than polars-readstat, which by default does not
keep the file's row order. `fastsas` is 11 to 24 times faster than haven;
building the R data frame takes longer than decoding the file. On the
national file, `fastsas` peaks at 3.7 GB of physical memory, against 2.4 GB
for haven.

# Research impact statement

The library is used in register research by Mary Elizabeth's Hospital's Data
Powerhouse at Rigshospitalet, Copenhagen, which studies the health, social,
and economic consequences of severe illness in children, young people,
pregnant women, and their families using nationwide Danish registers
[@datapowerhouse]. The author has used it on Statistics Denmark's research
servers since its first release in January 2026. In November 2026 it
converted all of the group's SAS register data there, 1,469 files and
2.76 TB, to Parquet in about two hours, and the group's researchers now work
from that Parquet instead of loading the SAS files into R with haven. Outside
users have requested features, such as storing SAS labels and formats as
Parquet metadata, which has since been implemented. By 13 November 2026 the
crate had been downloaded about 7,600 times from crates.io and the Python
package about 4,000 times from PyPI, mirrors excluded.

# AI usage disclosure

AI coding assistants were used throughout development, from October 2025 to
November 2026, for code generation, refactoring, tests, benchmark scripts,
and documentation: OpenAI Codex until May 2026, Anthropic's Claude Code from
April 2026, and Google's Gemini CLI in a few sessions. This paper was drafted
and revised with Claude. The author made the design decisions and reviewed,
modified, and validated all AI-assisted output.

Development began from a prototype based on a c2rust translation of
ReadStat's C reader, rewritten in Rust within two weeks. The present code base
is a second rewrite, begun in March 2026, whose public history starts on 29
March 2026; releases on crates.io go back to January 2026. Three of its
modules descend from the first rewrite, none of the translated code remains,
and ReadStat's MIT licence is included in the repository's third-party
notices. pandas, Parso, and Shotwell's documentation served as references for
the format.

# Acknowledgements

This parser builds on public reverse engineering of the SAS7BDAT format, in
particular Matt Shotwell's documentation [@shotwell2013], and on ReadStat,
pyreadstat, and haven as correctness oracles. This work was supported by the
Novo Nordisk Foundation (grant NNF23OC0083603) and BørneRiget Fonden.

SAS and SAS7BDAT are trademarks of SAS Institute Inc. This project is
independent of and not endorsed by SAS Institute Inc.

# References
