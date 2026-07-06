# String-column dictionary encoding — what was built and measured

This records what shipped for the string-column dictionary-encoding feature, the
numbers, and the decisions the measurements forced. (The pre-implementation
design exploration lives in `docs/interner/`, local scratch.)

## What shipped

A feature-gated core module plus an opt-in flag in each binding.

- **Core** (`sas7bdat`, `--features dictionary`): `src/dictionary.rs`
  - `dictionary_encode(buffers, policy) -> Option<DictionaryColumn>` — probe
    cardinality on a stride **seek**-sample with `cardinality-estimator` (HLL); if
    low-cardinality, build a dense `u32` dictionary, else return `None` (caller
    keeps plain `Utf8`).
  - `DictBuilder` — incremental builder used during decode. Starts in a **no-hash
    byte-direct mode** (a 257-entry table; SAS char columns are usually 1 byte
    wide) and promotes to a `lasso2` hash interner only if a multi-byte value
    appears.
  - `read_dictionary_columns(ds)` — builds dictionaries as the scan decodes.
  - Consumes the core's already-decoded, trimmed `Utf8` buffers (so the
    transcode/trim/empty-as-null work from `idea.md`'s `normalize.rs` is already
    done by the decoder and is not repeated).
- **R** (`fastsas`): `read_sas(path, categorical = TRUE)` → plain character
  columns become **`factor`** (integer codes + `levels`), built from the core dict.
- **Polars** (`sas7bdat_polars`): `scan_sas(path, categorical = True)` → character
  columns become **`Categorical`** via Polars' own lazy cast.

The cardinality "switch" from `idea.md` is the HLL gate (low-card → encode,
high-card → veto), already present; `DictionaryStaging::{Auto,On,Off}` is the
core-level control.

## Measured results (Apple M3 Pro; 2.15 GB AHS file, 2,574 character columns, 180M string cells)

| Axis | R `factor` | Polars `Categorical` |
|---|---|---|
| read speed | **2.6× faster** (4.36s → 1.66s) | +0.57s **slower** (1.18s → 1.75s) |
| memory | **0.68× (~32% smaller)** | **larger** (887 → 1552 MB) |
| downstream group-by | ~11× faster (46→4 ms) | ~11× faster (500→45 ms) |

**The two bindings win differently, and that is the headline.** R improves on all
three axes; Polars only downstream.

## The decisions the numbers forced

- **R's `character` path was bottlenecked on per-cell `SET_STRING_ELT` (CHARSXP
  interning).** A `factor`'s integer codes eliminate that, so the core dictionary
  encoder is a genuine read-speed + memory win for R.
- **Polars' native `String` is already compact and its `Utf8→Categorical` cast is
  fast (~0.6s for 180M cells).** Emitting an Arrow `DictionaryArray` ourselves
  would need fragile cross-batch dictionary reconciliation and would **not** beat
  the cast — so `scan_sas(categorical=True)` just appends Polars' cast to the lazy
  plan. The core encoder is **not** the lever for Polars.
- **Reusing the decode-time `StagedDictionary` hash doesn't help** the hot
  (single-byte) workload: by default those columns take the zero-copy borrowed
  path and are never interned (`dictionary_ids` is `None`). The real lever was the
  **no-hash byte-direct table**, which roughly halved the build cost
  (+87% → +43% during decode).
- **Building the dictionary is not free.** During decode it adds +6% on a normal
  file and +43% on the string-heavy AHS file — the per-cell hashing/lookup is
  intrinsic. It's an opt-in, not a default.
- **Cost of dictionary-encoding (build), not the downstream win, is what makes it
  opt-in.** The downstream group-by/join/sort speedup (~10–15×) is real for both.

## When to use it

- **R**: turn it on for survey/register data you'll read repeatedly or group on —
  it's faster *and* smaller, with the HLL gate keeping high-cardinality columns as
  `character`.
- **Polars**: turn it on only when you'll **group-by/join/sort** on the string
  columns; it costs a little read time and memory otherwise.

## Verification

Default builds are byte-identical (everything is feature-gated / opt-in).
Confirmed: core lib tests (87 default, 90 with `dictionary`), `verify_columnar`
(0 mismatches), `verify_readstat` (557.8M cells, 0 failures), R testthat (incl.
factor round-trip + HLL-gate), Polars pyreadstat comparison (690 fixtures, 6.8M
cells, 0 failures), clippy clean throughout. `factor`/`Categorical` reconstruct
the original strings exactly.
