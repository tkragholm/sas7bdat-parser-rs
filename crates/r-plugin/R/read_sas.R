#' Read a SAS7BDAT file
#'
#' Reads a `.sas7bdat` file into a data frame using the Rust parsing core.
#' Column types follow `haven` conventions: SAS numerics become `double`,
#' character columns become UTF-8 `character`, SAS dates become `Date`,
#' datetimes become `POSIXct` (UTC), and times become `hms`.
#'
#' SAS variable labels are attached as the column `label` attribute, with any
#' surrounding whitespace preserved exactly as `haven` reports it. When a
#' value-label catalog is available, labelled columns are returned as
#' `haven_labelled` vectors (a `labels` named vector plus the haven/vctrs class).
#'
#' SAS special missing values (`.A`-`.Z`, `._`) become `haven::tagged_na()`
#' values on every numeric and temporal column.
#'
#' @section Name clash with haven:
#' `haven` also exports a `read_sas()`, so whichever package is attached second
#' masks the other and `library(fastsas); library(haven)` silently changes which
#' one `read_sas()` means. [read_sas7bdat()] is the same function under a name
#' nothing else claims; prefer it in scripts, or qualify the call as
#' `fastsas::read_sas()`.
#'
#' @section Network drives:
#' `"auto"` decides by asking the operating system whether the path is remote,
#' which it can only do on Windows — a UNC path, or a mapped drive that reports
#' as remote. Elsewhere every path looks local and is memory-mapped.
#'
#' That distinction matters because memory-mapping a file on a share turns each
#' access into a network round-trip with no readahead. If a read from a share is
#' unexpectedly slow, or you are on Linux or macOS with a mounted share, pass
#' `io_backend = "buffered"`.
#'
#' @section Text encoding:
#' Files that declare a single-byte Western encoding are decoded as
#' Windows-1252 rather than strict ISO-8859-1. The two agree everywhere except
#' bytes `0x80`-`0x9F`, which ISO-8859-1 leaves as control characters and which
#' real SAS-on-Windows files use for curly quotes, dashes and the euro sign.
#' `haven` decodes those as controls, so the two packages differ on exactly
#' those bytes.
#'
#' @param path Path to a `.sas7bdat` file.
#' @param catalog Optional path to a `.sas7bcat` value-label catalog. If `NULL`
#'   (the default), a same-stem `.sas7bcat` sibling next to `path` is used when
#'   present.
#' @param categorical If `TRUE`, plain (non-value-labelled) character columns are
#'   returned as `factor` instead of `character`. This is faster to build and
#'   uses less memory on low-cardinality columns, and speeds up downstream
#'   grouping/joining. Default `FALSE` (haven-style `character`).
#' @param io_backend How to read the file: `"auto"` (default) memory-maps local
#'   files and reads network shares sequentially, `"mmap"` always memory-maps,
#'   `"buffered"` always reads sequentially. See the "Network drives" section.
#' @param threads Decode threads. `NULL` (default) uses every logical core. Set a
#'   smaller number to leave the machine room for other work, or to bound memory:
#'   in-flight batches scale with this. Read concurrency is capped separately by
#'   the reader and is not affected.
#' @return A tibble (or a base `data.frame` if the `tibble` package is not
#'   installed).
#' @export
#' @examples
#' \dontrun{
#' df <- read_sas7bdat("path/to/file.sas7bdat")
#' df <- read_sas7bdat("data.sas7bdat", catalog = "formats.sas7bcat")
#' df <- read_sas7bdat("survey.sas7bdat", categorical = TRUE)
#'
#' # Reading from a network share on a platform where "auto" cannot detect one:
#' df <- read_sas7bdat("//server/share/data.sas7bdat", io_backend = "buffered")
#'
#' # Leave half the machine free:
#' df <- read_sas7bdat("big.sas7bdat", threads = 8)
#' }
read_sas7bdat <- function(path, catalog = NULL, categorical = FALSE,
                          io_backend = c("auto", "mmap", "buffered"),
                          threads = NULL) {
  path <- check_path(path, "path")
  if (!is.null(catalog)) {
    catalog <- check_path(catalog, "catalog")
  }
  if (!is.logical(categorical) || length(categorical) != 1L || is.na(categorical)) {
    stop("`categorical` must be TRUE or FALSE, not ",
         deparse1(categorical), call. = FALSE)
  }
  io_backend <- match.arg(io_backend)
  threads <- check_threads(threads)

  df <- sas7bdat_read_impl(path, catalog, categorical, io_backend, threads)
  if (requireNamespace("tibble", quietly = TRUE)) {
    tibble::new_tibble(df, nrow = nrow(df))
  } else {
    df
  }
}

#' @rdname read_sas7bdat
#' @export
read_sas <- read_sas7bdat

# Validate before crossing into Rust, so the message names the argument the
# caller actually passed. `path.expand()` reports every bad value as "invalid
# 'path' argument" regardless of which argument it came from, which made a bad
# `catalog` look like a bad `path`.
# `threads` reaches Rust as an integer, so reject the values R would otherwise
# coerce silently: 0, negatives, fractions and NA all mean something the reader
# cannot act on.
check_threads <- function(value) {
  if (is.null(value)) {
    return(NULL)
  }
  if (!is.numeric(value) || length(value) != 1L || is.na(value) ||
      value != trunc(value) || value < 1) {
    stop("`threads` must be NULL or a whole number >= 1, not ",
         deparse1(value), call. = FALSE)
  }
  as.integer(value)
}

check_path <- function(value, arg) {
  if (!is.character(value) || length(value) != 1L || is.na(value)) {
    stop("`", arg, "` must be a single file path, not ",
         deparse1(value), call. = FALSE)
  }
  path.expand(value)
}
