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
#' @return A tibble (or a base `data.frame` if the `tibble` package is not
#'   installed).
#' @export
#' @examples
#' \dontrun{
#' df <- read_sas7bdat("path/to/file.sas7bdat")
#' df <- read_sas7bdat("data.sas7bdat", catalog = "formats.sas7bcat")
#' df <- read_sas7bdat("survey.sas7bdat", categorical = TRUE)
#' }
read_sas7bdat <- function(path, catalog = NULL, categorical = FALSE) {
  path <- check_path(path, "path")
  if (!is.null(catalog)) {
    catalog <- check_path(catalog, "catalog")
  }
  if (!is.logical(categorical) || length(categorical) != 1L || is.na(categorical)) {
    stop("`categorical` must be TRUE or FALSE, not ",
         deparse1(categorical), call. = FALSE)
  }

  df <- sas7bdat_read_impl(path, catalog, categorical)
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
check_path <- function(value, arg) {
  if (!is.character(value) || length(value) != 1L || is.na(value)) {
    stop("`", arg, "` must be a single file path, not ",
         deparse1(value), call. = FALSE)
  }
  path.expand(value)
}
