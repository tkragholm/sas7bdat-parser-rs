# Design

How the reader is built, and why. Each section states a decision, what it
costs, what was tried instead where the history records it, and where the code
is. Commit hashes point to the message that gives the measurement.

## Scope

The reader turns SAS7BDAT files, and the `.sas7bcat` catalogs that hold their
value labels, into rows or Arrow columns. It does not write files and does not
read SAS transport (XPORT), Stata or SPSS files. It was built for one job:
reading large SAS extracts on remote research servers, often over a network
share, into columnar tools. Most of the decisions below follow from that.

## Layers

- `crates/sas7bdat` is the core. It owns everything about the file format:
  the header, the metadata subheaders, page classification, decompression,
  decoding, text encodings and catalogs.
- `crates/sas7bdat-convert` writes Parquet, CSV and TSV from the core's
  batches, with no user interface of its own.
- `crates/sas7bdat-cli`, `crates/polars-plugin` and the two R packages
  (`crates/r-plugin`, `crates/r-convert-plugin`) are bindings. They choose
  options and move batches into their host's data structures. They do not
  interpret the format.

Keeping the format in one crate means a fix to decoding reaches every
interface at once, and the bindings stay small enough to read in one sitting.

## Opening a file

`Dataset::open` reads the header and the metadata pages and stops there
(`src/layout.rs`). The column table is assembled from several kinds of
subheader: names, attributes, formats and labels arrive separately and are
joined by column index. Counts the file declares are treated as claims to
check, not sizes to allocate: a column count larger than the file's bytes
could describe is rejected at once (`column_ceiling` in `src/layout.rs`), and
every allocation sized from the header is bounded. A file shorter than its
header says is rejected when it is opened.

Before rows are read, each page is classified once into a descriptor
(`src/pages.rs`): where its rows start, how many there are, whether they are
compressed, and whether the page can be read positionally. The scan plans its
work from these descriptors rather than from the declared row counts, which
are wrong for compressed files (see Parallelism).

## From rows on pages to columns in batches

SAS stores rows; Arrow, Polars and R want columns. The transpose has to
happen somewhere, and doing it inside the decoder avoids a second pass and a
per-row allocation in every consumer.

The default fill is column-major (566be10). For uncompressed pages whose rows
sit back to back, each column's buffer is filled in cache-sized tiles, so the
per-cell choice of column type is made once per tile rather than once per
cell. Where that does not apply (compressed rows, row selections, columns the
tiled path does not stage), the same batch is built row by row. Both fills
give byte-identical batches, and `examples/verify_columnar.rs` checks that.

Numbers are stored in 3 to 8 bytes and widened to 8 bytes on read. A SAS
special missing value (`.A` to `.Z`, `._`) is a NaN with a tag in its bits;
the raw bits are kept so that bindings can recover the tag. The R package
does; Arrow has no slot for it, so the Polars plugin returns a null.

Several pipelines exist, and which one runs depends on the method called as
well as on the file. The table at the top of `src/scan/mod.rs` lists them and
the conditions that select each. `ScanBuilder::predict_path` reports the
choice without decoding anything, and benchmarks should be labelled with it,
since an optimisation to one pipeline measures as zero through another.

## Parallelism and memory

Pages are independent once the metadata is known, so the unit of parallel
work is a chunk of pages. Four rules keep that fast without letting memory
grow with the file.

**Chunks are batches.** On a memory-mapped file a chunk is the pages for one
output batch, cut by the descriptors' row counts (a0f251f). Compressed files
declare their rows per page for uncompressed rows, which understates what a
compressed page holds by up to seven times on the AHS survey files; chunks
sized from the declared figure held several batches each and left threads
idle. On a buffered source a chunk is one read extent of 4 MB, or one batch if
that is more (`src/scan/extent.rs`).

**Ordered delivery has a bounded window.** Callers usually want rows in file
order. A batch that finishes early waits in a reorder buffer until the
batches before it are delivered, so one slow chunk could let the other
threads fill memory with everything after it. A thread may therefore claim
chunk `k` only while `k` is less than twice the worker count ahead of the next
chunk to deliver (`src/scan/gate.rs`). The wait happens at the claim, before
the thread holds anything the collector needs, and chunks are claimed in
order, so the gate cannot deadlock. On the 2 GiB AHS file this halved the live
heap. A window of one chunk per worker, and spreading chunks over whole rounds
of workers, were tried and gave no further gain (a0f251f). A caller that does
not need file order can switch ordering off, and then there is no reorder
buffer at all.

**Batches have a byte ceiling.** The default batch is 4,096 rows or more, but
never more than 32 MB of decoded data (`AUTO_BATCH_TARGET_BYTES` in
`src/scan/plan.rs`). A row-count rule alone gave 200 MB batches on a table
with 4,041 columns, and peak memory then grew with every thread added.

**The default thread count adapts.** `Parallelism::Auto` stays on one thread
below 8,192 rows or 16 pages, where spawning threads cost more than it saved
on every small file measured (`src/scan/builder.rs`). Above that it takes
every core, but no more threads than fit 1 GiB of estimated batches in flight
(86524e4). That cap only binds when a caller asks for large batches of a wide
table, as the Polars plugin does; on that read it cut the time from 0.98 to
0.75 s. An explicit thread count is always honoured.

Each scan starts its own scoped threads, so two scans running at once each
take the full budget. Inside a batch, the final step of building columns uses
rayon.

## Input and output

A local file is memory-mapped by default (`src/dataset.rs`). Mapping costs
nothing up front and lets ASCII text be borrowed straight from the file. On a
network share every access to a mapped page is a round trip with no
read-ahead, so on Windows the reader asks the operating system whether the
path is on a network drive (`src/netpath.rs`) and reads it sequentially in
4 MB extents instead, with at most four reads in flight. Other systems cannot
tell a share from its path, so there the caller chooses with the I/O backend
option, which every binding exposes.

The cost of mapping: if another process truncates the file during a read, a
Unix process can be stopped by the operating system. Files that may change
while being read should use the buffered backend.

## Text

SAS pads strings with spaces to a fixed width. Cells of up to 16 bytes, which
covers every string column in a typical register extract, are trimmed and
checked for ASCII with two word-sized operations and no loop
(`src/scan/string.rs`). Cells of 64 bytes or more use the vector kernels.
ASCII cells are borrowed from the file. Other cells are transcoded from the
declared encoding with `encoding_rs`, which follows the WHATWG mappings, so
ISO-8859-1 is read as windows-1252. An encoding label the reader does not
know is read as UTF-8.

By default, bytes that are invalid in the declared encoding are replaced, and
a file that declares a single-byte encoding but holds UTF-8 is read as UTF-8.
That second case is common in Nordic register data. The repair reads the
row's bytes as UTF-8 directly instead of undoing a wrong decode, which an
earlier version did and which failed on the whole upper half of Latin-1
(2568491). A strict mode in the Rust API fails on the first cell that does not
decode; the bindings do not expose it yet.

## Vector instructions

The vector kernels (missing-value tests, integer conversion, trimming, ASCII
checks) sit behind one interface with three backends (`src/simd/mod.rs`). The
scalar backend is always compiled and is the reference the others are tested
against. The default, `fearless_simd`, builds each kernel for several
instruction sets and picks one at run time, so a wheel or an R package built
for a baseline CPU still uses AVX2 or AVX-512 where the machine has them.
`std::simd`, available on nightly behind a feature flag, does not dispatch at
run time and lowers to whatever the build enabled, which for a portable build
on x86-64 is SSE2.

Two lessons from measuring are recorded there. Kernel rankings change between
ARM and x86-64, so neither is taken as general. And calling a dispatched
kernel per chunk of 8 rows costs a real function call each time, because a
function compiled for a specific instruction set cannot be inlined into one
that is not; the column-level entry points exist to dispatch once per column.

The rest of the code in the x86-64 wheels is compiled for AVX2
(`.cargo/config.toml`), since a binary compiled for AVX-512 fails at import
on recent Intel consumer processors.

## Bindings

**Polars.** The plugin hands each batch to Python as an Arrow C stream
through the PyCapsule interface and builds the Polars API in Python on top.
It does not link Polars, so it works with any Polars from 1.41 on rather than
one pinned version. Projection and row limits are pushed into the scan;
filters are applied by Polars per batch. Columns requested as categorical
become Arrow dictionary columns with one dictionary per batch.

**R.** The R package allocates each column at its final length and writes
every batch into place, then drops it, so peak memory is the finished data
frame plus the scan's window, without a copy of the whole decoded file beside
it. Batches
can arrive in any order and are placed by their row offset. Two cases read in
file order instead: factor columns, which need one dictionary over the whole
column, and files with deleted rows, where the batch's row offset counts the
deleted rows before it. Text is interned once per distinct value and column,
since a register extract has millions of cells over few values.

## Deleted rows

SAS deletes a row by marking it, not by removing it, and the header's row
count still includes it. Uncompressed pages carry a bitmap of deleted rows;
compressed rows carry a marker in their pointer. The reader skips both, and
`Metadata::deleted_row_count` says how many there are. The bitmap is read
only when the file declares deletions, so the common case keeps the fast
positional path (`src/pages.rs`). ReadStat 1.1.9, the version inside haven
and pyreadstat, returns deleted rows; its main branch now skips them too.

## Correctness and safety

The reference is ReadStat. `examples/verify_readstat.rs` decodes each file in
a local corpus with both readers and compares every cell. Where the two
differ on purpose, the README says so: deleted rows against ReadStat 1.1.9,
and ISO-8859-1 text. That comparison needs the full corpus and runs locally.

Continuous integration runs the rest. Twenty-two files from the pandas and
Parso test suites are tracked, one for each format feature: both byte orders
and bitnesses, RLE and RDC compression, deleted rows, a wide table, multi-byte
encodings and an empty file (`crates/sas7bdat/tests/fixtures/corpus/`). Their
manifest records each file's source, licence and header facts, and a test
checks every file against it. The parity tests between pipelines, the
comparison with pandas' expected CSV output, and the deleted-row and
row-selection tests run on those files. CI also runs unit tests on synthetic
files built in code, replays past fuzzing crashes under AddressSanitizer, and
runs the R package's tests against haven on its bundled files.

Unsafe code is limited by a workspace lint that continuous integration turns
into an error. Five places are allowed, each with a safety comment: the
memory map, two calls that ask Windows for a drive's type, and two in the
Polars plugin at the Arrow boundary. The R packages are outside the workspace
and use unsafe code where Rust writes into R vectors.

## Open questions

- On a 6 + 6 core Apple M3 Pro, decoding stops getting faster past about 8
  threads even with small batches, which may be the efficiency cores. Telling
  the two kinds of core apart is platform-specific and has not been measured
  off macOS.
- The pipelines in `src/scan/mod.rs` overlap. Two of them plan page sources
  separately, and their measured difference is small; whether both earn
  their keep is not settled.
- Strict decoding is not yet reachable from Python, R or the command line.
