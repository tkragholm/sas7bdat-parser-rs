# Tracked test fixtures

Small SAS files used by this crate's tests. The larger corpus at the
repository root is not tracked; see `fixtures/README.md`. Attribution for
tracked third-party files is in `THIRD_PARTY_NOTICES.md`.

## `people_nonascii.sas7bdat`

A copy of `crates/r-plugin/inst/extdata/people.sas7bdat` (WINDOWS-1252, 5
rows) with one byte changed: row 0's `GENDER` cell is `0xE9` (`é`) instead of
`M`. The original is pure ASCII, so it never reaches the transcoding path; this
copy does. Used by
`dataset::tests::batch_decode_transcodes_non_ascii_windows_1252_string`. It
does not cover the compressed single-byte path
(`append_non_ascii_single_byte_utf8`), which would need a compressed
WINDOWS-1252 file with non-ASCII text.

To regenerate it:

```python
data = bytearray(open("crates/r-plugin/inst/extdata/people.sas7bdat", "rb").read())
assert data[1249] == ord("M")   # row 0 GENDER cell
data[1249] = 0xE9               # windows-1252 'é'
open("crates/sas7bdat/tests/fixtures/people_nonascii.sas7bdat", "wb").write(data)
```

## `fuzz/`

Inputs found by `cargo fuzz` (`crates/sas7bdat/fuzz`). They are mutations of
the local corpus, not third-party data. `tests/fuzz_regressions.rs` opens each
one, visits its rows and, separately, its batches, and expects an error
rather than a crash or a huge allocation.

| File | What it declares | Guard |
|---|---|---|
| `oom_declared_column_count_8k.sas7bdat` | 1,085,348,864 columns | column count checked against the bytes that describe columns |
| `oom_declared_column_count_32k.sas7bdat` | 2,863,311,531 columns | same |
| `panic_numeric_width_over_8.sas7bdat` | a numeric column 67 bytes wide | numeric width validated at open |
| `oom_declared_row_length.sas7bdat` | a 4,261,413,064-byte row | row length bounded before decompression; reached only through a scan |
| `oom_batch_capacity_hint.sas7bdat` | `rows_per_page` and a row count that inflate the batch size hint | the hint is clamped; reached only through `visit_batches` |
| `catalog/oom_declared_label_count.sas7bcat` | 1,313,169,229 value labels | label count bounded by what the block can hold |

To add one: run `just fuzz`, copy the file from
`crates/sas7bdat/fuzz/artifacts/<target>/` into this directory, and add it to
`EXPECTED` in `tests/fuzz_regressions.rs`.
