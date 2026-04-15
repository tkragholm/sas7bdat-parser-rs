I’m checking the current Polars extension/plugin APIs and Rust-facing options first, then I’ll map that to a design that preserves your parser’s throughput.

There are two different extension paths in current Polars: expression plugins for new compute kernels, and IO plugins for new file formats. For a SAS reader, the IO plugin path is the relevant one; expression plugins would only help for extra SAS-specific transforms after loading.

Use an **I/O plugin**, not an expression plugin, as the main integration point.

Polars’ current plugin model distinguishes between **expression plugins** for custom compute and **I/O plugins** for custom data sources. For a SAS `.sas7bdat` reader, the I/O plugin path is the one that gives you scan-time features like **projection pushdown, predicate pushdown, early stopping, and streaming-engine compatibility**. The Polars docs also note that I/O plugins can move data **zero-copy via Arrow FFI**, with only a short Python rendezvous point. docs.pola.rs+2docs.pola.rs+2

The highest-performance design is:

1. **Keep the parser core in pure Rust**

2. **Expose a very thin Python wrapper only for registration**

3. **Emit Arrow-native batches**

4. **Let Polars consume those batches lazily**

That preserves almost all of the performance-critical work in Rust while still fitting Polars’ current extension API. docs.pola.rs+2docs.pola.rs+2

## Recommended architecture

### 1) Split the project into three layers

**Layer A: `sas7bdat-core`**

- pure Rust

- no Python

- parses pages, metadata, column descriptors, compression, row decoding

- exposes:

  - `read_schema(path) -> ArrowSchema`

  - `scan(path, projection, predicate_hint, n_rows, batch_size) -> impl Iterator<Item = RecordBatch>`

**Layer B: `sas7bdat-polars-py`**

- thin PyO3 module

- converts your Rust `RecordBatch` stream into Python Arrow-compatible objects

- registers an I/O source with `polars.io.plugins.register_io_source(...)` docs.pola.rs+1

**Layer C: Python user API**

- something like:

  ```
  <div id="code-block-viewer" dir="ltr"><p><span>import</span><span> </span><span>polars</span><span> </span><span>as</span><span> </span><span>pl</span></p><br><p><span>from</span><span> </span><span>sas7bdat_polars</span><span> </span><span>import</span><span> </span><span>scan_sas</span></p><br><br><p><span>lf</span><span> </span><span>=</span><span> </span><span>scan_sas</span><span>(</span><span>"file.sas7bdat"</span><span>)</span></p><br><p><span>df</span><span> </span><span>=</span><span> </span><span>lf</span><span>.</span><span>select([</span><span>"a"</span><span>, </span><span>"b"</span><span>])</span><span>.</span><span>filter(</span><span>pl</span><span>.</span><span>col(</span><span>"a"</span><span>) </span><span>&gt;</span><span> </span><span>10</span><span>)</span><span>.</span><span>collect()</span></p></div>
  ```

- this should return a `LazyFrame`, not an eager `DataFrame`, so the optimizer can push work down. The registered I/O source API is explicitly built for `with_columns`, `predicate`, `n_rows`, and `batch_size`. docs.pola.rs

## 2) Use Arrow as the interchange boundary

Polars is Arrow-based, and its docs recommend Arrow/PyCapsule-based interchange for zero-copy integration. `pl.from_arrow` is “zero copy for the most part,” and Polars supports the Arrow PyCapsule Interface for sharing Arrow data across Python libraries. For library authors, Polars specifically suggests implementing Arrow interchange because it enables zero-copy exchange and avoids a hard dependency on Polars internals. docs.pola.rs+2docs.pola.rs+2

So your parser should ideally output:

- `arrow-rs` arrays

- `RecordBatch`

- `Schema`

not intermediate row structs, `Vec<HashMap<...>>`, or Python lists.

That means:

- decode directly into column builders

- finish builders into Arrow arrays

- yield `RecordBatch`es

- hand those batches to Polars through Arrow interchange

## 3) Prefer streaming `RecordBatch` production over whole-file materialization

The Arrow Rust docs recommend streaming approaches instead of bulk `pyarrow.Table` conversion when possible. That lines up perfectly with Polars’ I/O plugin interface, which asks your source to produce an iterator/generator of `DataFrame`s and gives you a `batch_size` hint. Apache Arrow+1

So do **not** parse the whole SAS file into memory first unless the file is tiny.

Better:

- parse file metadata once

- iterate over row groups/pages

- produce batches of, say, 32K–256K rows

- adapt batch size to row width

That helps:

- memory use

- cache locality

- downstream streaming

- avoiding one huge allocation spike

## 4) Push projection down immediately

Polars’ I/O source API passes `with_columns`, and the reader “must project these columns if applied.” docs.pola.rs

For performance, projection pushdown should happen as early as possible:

- parse full schema metadata once

- map requested column names to SAS column IDs

- skip decoding all unneeded columns

- ideally skip even allocating builders for unrequested columns

This is probably the single biggest win after “stay in Rust.”

For SAS specifically, even if page traversal still has to happen, avoiding:

- string decoding,

- decompression work for unused vars,

- temporal conversions,

- Arrow builder writes

can massively reduce CPU.

## 5) Support `n_rows` as an early-stop fast path

The I/O API passes `n_rows`, and the source can stop when that many rows are read. docs.pola.rs

Implement this carefully in the Rust iterator:

- maintain emitted row count

- stop batch construction early

- do not continue scanning pages once satisfied

That makes:

```
<div id="code-block-viewer" dir="ltr"><p><span>scan_sas</span><span>(...)</span><span>.</span><span>head(</span><span>1000</span><span>)</span><span>.</span><span>collect()</span></p></div>
```

cheap instead of whole-file expensive.

## 6) Treat predicate pushdown in phases

Polars passes a `predicate` expression to the I/O source and says the reader must filter rows accordingly. docs.pola.rs

In practice, for a first version, I would do this in **three levels**:

### Phase 1: no true pushdown, but correct behavior

- parse batches in Rust

- convert to DataFrame / Arrow

- let Polars apply the filter downstream

### Phase 2: row-level pushdown for simple predicates

Support a small subset only:

- `col == literal`

- `col != literal`

- numeric comparisons

- `is_null`

- conjunctions of the above

Translate the incoming predicate tree into your own internal evaluator.

### Phase 3: page/block pruning

If SAS page metadata lets you infer min/max/null counts or partition-like elimination, prune entire chunks before decoding.

This staged approach keeps complexity under control. Projection and early-stop usually matter more at first than sophisticated predicate pushdown.

## 7) Keep the Python boundary extremely thin

Polars notes that I/O plugins currently interface via Python, but the intended design is that the data-reading work happens in Rust and only the handoff uses Python briefly. docs.pola.rs

That means your Python should mostly do:

- validate args

- compute schema callback

- call `register_io_source`

- wrap/forward batches from Rust

Avoid:

- Python-side row loops

- Python-side string/date parsing

- converting rows to dicts/tuples

- pandas/pyarrow detours unless absolutely necessary

## 8) Use Arrow builders efficiently

For high throughput in Rust:

- pre-size builders when row count estimate is known

- use typed builders per physical SAS type

- decode fixed-width numerics directly into primitive builders

- use dictionary encoding only if it actually helps your data

- avoid per-row dynamic dispatch

- avoid `String` allocation churn where possible

Good pattern:

- one decode function per physical SAS column type

- one hot loop over rows/pages

- write directly into typed Arrow builders

Bad pattern:

- decode each row into an enum-based intermediate

- then transpose rows into columns

Column-first decoding is what Polars and Arrow want.

## 9) Be careful with strings and temporals

These are often where throughput disappears.

### Strings

- decode only projected columns

- reuse scratch buffers

- avoid UTF-8 validation work twice

- if SAS encoding is not UTF-8, convert once, at the edge

### Dates/times

- keep them in Arrow-native physical representations

- map to Polars logical dtypes only once

- avoid repeated chrono object construction in inner loops

In other words: prefer writing raw `i32`/`i64` date-time values into Arrow arrays and attach the logical type/schema, instead of constructing rich date objects row by row.

## 10) Handle nulls with bitmaps, not sentinel branching everywhere

For each column:

- accumulate validity bitmap efficiently

- write values to the value buffer

- set null bits separately when possible

That keeps the hot path tighter than mixing “is null?” branches deeply into every conversion.

## 11) Keep batches moderately large

Too-small batches increase overhead from:

- Python handoff

- Arrow batch metadata

- scheduler churn

Too-large batches hurt:

- cache locality

- latency to first rows

- memory spikes

A good starting heuristic:

- target **4–32 MB per batch**

- or **64K rows** for narrow tables

- reduce for very wide string-heavy schemas

Because `register_io_source` provides a `batch_size` hint, use it as a soft target, not a rigid rule. docs.pola.rs

## 12) Mark the source pure if it really is

The Polars API says repeated occurrences of the same I/O source in a lazy plan can be de-duplicated if `is_pure=True`. docs.pola.rs

For a local `.sas7bdat` file reader, that is often true if:

- output depends only on file contents and args

- no time-dependent or random behavior

- no mutable external state

Set it when valid. That can avoid duplicate scans in some optimized plans.

## Concrete implementation shape

### Python side

Something like:

```
<div id="code-block-viewer" dir="ltr"><p><span>import</span><span> </span><span>polars</span><span> </span><span>as</span><span> </span><span>pl</span></p><br><p><span>from</span><span> </span><span>polars</span><span>.</span><span>io</span><span>.</span><span>plugins</span><span> </span><span>import</span><span> </span><span>register_io_source</span></p><br><p><span>from</span><span> </span><span>.</span><span>_native</span><span> </span><span>import</span><span> </span><span>schema_for_file</span><span>, </span><span>batch_reader</span></p><br><br><p><span>def</span><span> </span><span>scan_sas</span><span>(</span><span>path</span><span>: </span><span>str</span><span>) -&gt; </span><span>pl</span><span>.</span><span>LazyFrame:</span></p><br><p><span>    </span><span>schema</span><span> </span><span>=</span><span> </span><span>schema_for_file</span><span>(</span><span>path</span><span>)</span></p><br><br><p><span>    </span><span>def</span><span> </span><span>io_source</span><span>(</span><span>with_columns</span><span>=</span><span>None</span><span>, </span><span>predicate</span><span>=</span><span>None</span><span>, </span><span>n_rows</span><span>=</span><span>None</span><span>, </span><span>batch_size</span><span>=</span><span>None</span><span>):</span></p><br><p><span>        </span><span># returns an iterator/generator of Polars DataFrames or Arrow-backed frames</span></p><br><p><span>        </span><span>return</span><span> </span><span>batch_reader</span><span>(</span></p><br><p><span>            </span><span>path</span><span>=</span><span>path</span><span>,</span></p><br><p><span>            </span><span>with_columns</span><span>=</span><span>with_columns</span><span>,</span></p><br><p><span>            </span><span>predicate</span><span>=</span><span>predicate</span><span>,</span></p><br><p><span>            </span><span>n_rows</span><span>=</span><span>n_rows</span><span>,</span></p><br><p><span>            </span><span>batch_size</span><span>=</span><span>batch_size</span><span>,</span></p><br><p><span>        )</span></p><br><br><p><span>    </span><span>return</span><span> </span><span>register_io_source</span><span>(</span></p><br><p><span>        </span><span>io_source</span><span>=</span><span>io_source</span><span>,</span></p><br><p><span>        </span><span>schema</span><span>=</span><span>schema</span><span>,</span></p><br><p><span>        </span><span>validate_schema</span><span>=</span><span>True</span><span>,</span></p><br><p><span>        </span><span>is_pure</span><span>=</span><span>True</span><span>,</span></p><br><p><span>    )</span></p></div>
```

This matches the documented I/O source contract: callable receives `with_columns`, `predicate`, `n_rows`, `batch_size`, returns an iterator/generator producing batches, and is registered with schema plus purity/validation flags. docs.pola.rs

### Rust side

Expose:

- `schema_for_file(path) -> Python Arrow schema or Polars schema`

- `batch_reader(...) -> Python iterator`

The Rust core already has the batch-level bridge needed for this shape:

- `visit_arrow_batches(...)` streams `RecordBatch` values one at a time
- `collect_arrow_batches(...)` stays as the convenience wrapper for eager callers
- `ColumnarBatch` and `OwnedColumnarBatch` can both convert directly to Arrow

That keeps the plugin wrapper thin and avoids forcing a full `Vec<RecordBatch>` materialization when the caller only needs streaming.

## Current implementation

The repo now has an initial `sas7bdat-polars` extension crate under [`crates/polars_plugin/`](./crates/polars_plugin):

- `scan_sas(path)` registers a Polars IO source and returns a `LazyFrame`
- `schema_for_file(path)` exposes the file schema as a Polars `Schema`
- `batch_reader(...)` streams Arrow-backed record batches through the Rust core
- projections and batch-size hints are pushed into the Rust scan plan
- predicate handling is accepted at the IO-source boundary and applied in the thin wrapper layer for now
- `just build-polars-plugin` builds the crate into a wheel with `maturin`

This is the first cut of the Phase 8 integration. It is intentionally small and keeps the parser core free of Python-specific logic.

Internally:

- open file

- read metadata

- map projection

- construct `SasBatchIter`

- each `next()` returns one Arrow `RecordBatch`

## Should you implement this as a native Polars Rust reader instead?

Only if you want to maintain tight coupling to Polars internals.

You _could_ integrate more deeply on the Rust side using Polars’ Rust I/O ecosystem, which includes reader traits and scan-related facilities, but that is a heavier maintenance burden than the supported Python-registered I/O plugin route. The public plugin docs explicitly position I/O plugins as the extension mechanism for unsupported file formats. docs.pola.rs+1

So my recommendation is:

- **public package:** Python-facing I/O plugin

- **engine:** Rust parser crate

- **interop:** Arrow batches

- **optional later:** native Rust `polars-io` integration if you need tighter optimizer hooks

## Practical performance checklist

Use this as your bar for “fast enough”:

- Rust parser does all decode work

- no Python row iteration

- no whole-file materialization by default

- Arrow `RecordBatch` output

- projection pushdown implemented

- `n_rows` early stop implemented

- simple predicate pushdown optional in v2

- batch sizes tuned

- strings decoded only when needed

- benchmarked against:

  - your raw Rust parser

  - `pandas.read_sas`

  - `pyreadstat` + `pl.from_arrow`

## Biggest mistakes to avoid

The common ways to accidentally ruin performance are:

- building Python objects per row

- parsing into row structs first, then transposing

- always decoding all columns

- returning one tiny batch at a time

- converting Rust → pandas → pyarrow → Polars

- eagerly collecting instead of exposing a lazy scan

- doing fancy date/string conversion in the innermost loop

## What I would build first

I would ship this in this order:

1. **Lazy scan + schema**

2. **Projection pushdown**

3. **Early stop**

4. **Streaming Arrow batches**

5. **Benchmarks**

6. **Simple predicate pushdown**

7. **Write path / sink support only if needed**

That gets you a useful, fast extension quickly.

## Bottom line

The best high-performance path is:

**Rust SAS parser → Arrow `RecordBatch` stream → thin Python I/O plugin → Polars `LazyFrame`.** This aligns with Polars’ documented I/O plugin model, keeps the expensive work out of Python, and gives you access to scan-time optimizations like projection pushdown, predicates, early stopping, and streaming. docs.pola.rs+2docs.pola.rs+2

I can sketch a minimal project layout with `Cargo.toml`, PyO3 bindings, and a `scan_sas()` prototype next.
