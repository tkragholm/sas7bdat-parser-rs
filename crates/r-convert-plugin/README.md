# fastsasconvert

Convert `.sas7bdat` files to Parquet, CSV or TSV from R — without the data
entering R.

```r
library(fastsasconvert)

# Mirror a tree of SAS files into a tree of Parquet files.
result <- convert_sas("//server/share/sas", "D:/parquet")
sum(result$status == "error")
```

Companion to [`fastsas`](../r-plugin), which reads SAS files *into* R. This
package does the opposite: it converts files on disk and returns only a summary,
so trees far larger than the session's memory convert fine. It has no R-level
dependency on `fastsas` — the two share only the Rust core.

Returns one row per input:

| column | meaning |
|--------|---------|
| `input` / `output` | paths |
| `rows` / `columns` | what was written |
| `input_bytes` / `output_bytes` | sizes |
| `seconds` | per-file elapsed |
| `status` | `"ok"` or `"error"` |
| `error` | reason, `NA` on success |

## Failures are rows, not errors

One unreadable file does not lose a run over a large tree — it comes back with
`status = "error"` and the rest still convert. Only argument mistakes throw.

```r
convert_sas(result$input[result$status == "error"], "D:/parquet")
```

## Interrupting

Ctrl-C is checked between files. The frame returned covers what finished, and
`attr(result, "interrupted")` is `TRUE`; `attr(result, "discovered")` says how
many files were found, so the difference is what was skipped.

Interrupting cannot corrupt the output. Each file is written to a temporary and
moved into place only when complete, so a partially converted file never appears
at its destination — which also means a resumed run cannot mistake one for
finished work.

## Network drives

`io_backend = "auto"` (the default) memory-maps local files and reads network
shares sequentially, but it can only tell them apart on Windows. On a mounted
share elsewhere, pass `"buffered"` — memory-mapping a remote file turns every
access into a round-trip with no readahead.

`tmp_dir` stages outputs on a local disk and moves the finished file across,
keeping the write off the network link until there is something complete to send.

## Build / install

```sh
R CMD INSTALL --no-staged-install crates/r-convert-plugin
```

Same arrangement as `fastsas`: the Rust crate depends on `sas7bdat-convert` by
version so a built tarball resolves it from crates.io, and `src/.cargo/config.toml`
redirects to this checkout for in-repo builds. `.Rbuildignore` strips that
redirect, so it never reaches a tarball.

## License

MIT.
