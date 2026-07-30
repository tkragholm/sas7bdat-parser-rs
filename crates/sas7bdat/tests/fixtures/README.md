# sas7bdat test fixtures

Small, git-tracked SAS7BDAT files used by unit/integration tests in this crate.
(The larger `fixtures/` corpus at the repo root is untracked — see its README.)
Provenance/attribution for all tracked binary fixtures is documented in the
repo-root `THIRD_PARTY_NOTICES.md`.

## `people_nonascii.sas7bdat`

A byte-patched copy of `crates/r-plugin/inst/extdata/people.sas7bdat`
(WINDOWS-1252, 5 rows; a public synthetic teaching dataset — see
`THIRD_PARTY_NOTICES.md`). Row 0's `GENDER` cell byte was changed from `M` (0x4D)
to `0xE9`, which is `é` in windows-1252. Everything else is identical.

Purpose: exercise the columnar decoder's **non-ASCII transcode branch** (the
direct UTF-8 owned path) end-to-end. The original fixture is entirely ASCII, so
the scanner takes the ASCII fast path and never transcodes; the patched high
byte forces a real windows-1252 → UTF-8 conversion. Used by
`dataset::tests::batch_decode_transcodes_non_ascii_windows_1252_string`.

(Note: the *compressed-only* single-byte family
`append_non_ascii_single_byte_utf8` is a different decode path that this
uncompressed fixture does not reach — covering it would need a compressed
windows-1252 fixture with non-ASCII data.)

## `fuzz/`

Crash artifacts produced by `crates/sas7bdat/fuzz` (libFuzzer, via `cargo fuzz`).
These are machine-generated mutations of the repo-root corpus, not third-party
data, so they need no attribution entry. Used by `tests/fuzz_regressions.rs`.

- `oom_declared_column_count_8k.sas7bdat` (18 KB) declares 1,085,348,864 columns
- `oom_declared_column_count_32k.sas7bdat` (34 KB) declares 2,863,311,531 columns

Both once drove `MetadataState::ensure_column` to ask the allocator for 2.5 GiB
during *metadata* parsing, before any row was read — reachable from `Dataset::open`
as well as `from_bytes`, so the CLI and both language plugins inherited it. The
declared count is now checked against how many columns the file has bytes to
describe, and neither reaches an allocation.

- `panic_numeric_width_over_8.sas7bdat` (122 KB) declares column 5 as numeric and
  67 bytes wide

Surfaced on the next fuzz run after the OOM was fixed, which is the point: the OOM
had been ending every run after ~9k executions, so nothing downstream of it was being
explored. A numeric wider than 8 bytes reached `scan::numeric::numeric_bits`, whose
`_ => unreachable!()` arm panics in release as well as debug (the `slice.len() <= 8`
check above it is only a `debug_assert!`). Width is now validated at open time.

- `oom_declared_row_length.sas7bdat` (44 KB) declares a 4,261,413,064-byte row

`decompress_row` reserves the declared row length once per row, so the claim was an
allocation primitive: `malloc(4261413064)`. Found by seeding a corpus with only the
34 compressed fixtures — they are 9% of the corpus, so a general run almost never
built a valid compressed page. Note this one is reachable *only through a scan*, not
through `Dataset::open`, and not through `sas7bdat convert` (which decodes columnar
while the fuzz target uses `visit_rows`). That is why `fuzz_regressions.rs` scans
each artifact rather than stopping at open.

To add more: run `just fuzz`, then copy anything that lands in
`crates/sas7bdat/fuzz/artifacts/<target>/` into this directory and add it to
`EXPECTED` in `tests/fuzz_regressions.rs`.

Regenerate `people_nonascii.sas7bdat` (if the base fixture ever changes):

```python
data = bytearray(open("crates/r-plugin/inst/extdata/people.sas7bdat", "rb").read())
assert data[1249] == ord("M")   # row 0 GENDER cell
data[1249] = 0xE9               # windows-1252 'é'
open("crates/sas7bdat/tests/fixtures/people_nonascii.sas7bdat", "wb").write(data)
```
