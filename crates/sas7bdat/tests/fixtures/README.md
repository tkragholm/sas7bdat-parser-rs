# sas7bdat test fixtures

Small, git-tracked SAS7BDAT files used by unit/integration tests in this crate.
(The larger `fixtures/` corpus at the repo root is untracked — see its README.)

## `people_nonascii.sas7bdat`

A byte-patched copy of `crates/r-plugin/inst/extdata/people.sas7bdat`
(WINDOWS-1252, 5 rows). Row 0's `GENDER` cell byte was changed from `M` (0x4D)
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

Regenerate (if the base fixture ever changes):

```python
data = bytearray(open("crates/r-plugin/inst/extdata/people.sas7bdat", "rb").read())
assert data[1249] == ord("M")   # row 0 GENDER cell
data[1249] = 0xE9               # windows-1252 'é'
open("crates/sas7bdat/tests/fixtures/people_nonascii.sas7bdat", "wb").write(data)
```
