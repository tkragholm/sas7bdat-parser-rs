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
#' @param path Path to a `.sas7bdat` file, or several paths to files with the
#'   same columns, which are read in order and stacked; `skip` and `n_max` then
#'   count across them. Stacking needs the vctrs package.
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
#' @param threads Decode threads. `NULL` (the default) lets the reader decide from
#'   the file: it uses every logical core on a file with enough rows and pages to
#'   repay the threads, and one otherwise. That is not a slower path for small
#'   files, it is a faster one -- spawning twelve threads for a few thousand rows
#'   costs more than it saves. Set a number to override, to leave the machine room
#'   for other work, or to bound memory: in-flight batches scale with it. Read
#'   concurrency is capped separately by the reader and is not affected.
#' @param col_select Columns to read: a character vector of names, or a numeric
#'   vector of 1-based positions, in the order they should appear. `NULL` (the
#'   default) reads every column. Unlike haven, tidyselect helpers are not
#'   accepted.
#' @param skip Rows to skip before reading. Rows SAS marked as deleted are not
#'   counted.
#' @param n_max The most rows to read. `Inf` (the default) reads to the end;
#'   `0` reads no rows and returns the columns with their types and labels.
#' @param encoding Read the file's text in this encoding instead of the one its
#'   header declares, for files whose header is wrong, a common case being UTF-8
#'   data in a file marked latin1. A WHATWG label (`"utf-8"`, `"windows-1252"`)
#'   or SAS's name for a Windows code page (`"wlatin1"`). `NULL` (the default)
#'   uses the declared encoding.
#' @param strict If `TRUE`, fail on the first cell that does not decode in the
#'   file's encoding. By default such bytes are replaced and the read goes on.
#' @param id The name of a column to add first, holding each row's file path.
#'   `NULL` (the default) adds none.
#' @param temporal_as_numbers If `TRUE`, read date, datetime and time columns
#'   as the plain numbers SAS stores: days since 1 January 1960 for a date,
#'   seconds since then for a datetime, and seconds since midnight for a time.
#'   By default they are `Date`, `POSIXct` (UTC) and `hms`.
#' @return A tibble (or a base `data.frame` if the `tibble` package is not
#'   installed).
#' @export
#' @examples
#' # A small file that ships with the package
#' path <- system.file("extdata", "people.sas7bdat", package = "fastsas")
#' df <- read_sas7bdat(path)
#' df
#'
#' # Value labels from a format catalog
#' data <- system.file("extdata", "test_data_win.sas7bdat", package = "fastsas")
#' formats <- system.file("extdata", "test_formats_win.sas7bcat", package = "fastsas")
#' read_sas7bdat(data, catalog = formats)
#'
#' # Text columns as factors
#' str(read_sas7bdat(path, categorical = TRUE))
#'
#' \dontrun{
#' # Reading from a network share on a platform where "auto" cannot detect one:
#' df <- read_sas7bdat("//server/share/data.sas7bdat", io_backend = "buffered")
#'
#' # Leave half the machine free:
#' df <- read_sas7bdat("big.sas7bdat", threads = 8)
#' }
read_sas7bdat <- function(path, catalog = NULL, categorical = FALSE,
                          io_backend = c("auto", "mmap", "buffered"),
                          threads = NULL, col_select = NULL, skip = 0,
                          n_max = Inf, encoding = NULL, strict = FALSE,
                          id = NULL, temporal_as_numbers = FALSE) {
  if (!is.character(path) || length(path) == 0L || anyNA(path)) {
    abort_argument("`path` must be one or more file paths, not ", deparse1(path))
  }
  path <- path.expand(path)
  if (!is.null(id) && (!is.character(id) || length(id) != 1L || is.na(id))) {
    abort_argument("`id` must be NULL or a single column name, not ", deparse1(id))
  }
  if (!is.null(catalog)) {
    catalog <- check_path(catalog, "catalog")
  }
  if (!is.logical(categorical) || length(categorical) != 1L || is.na(categorical)) {
    abort_argument("`categorical` must be TRUE or FALSE, not ",
         deparse1(categorical))
  }
  io_backend <- match.arg(io_backend)
  threads <- check_threads(threads)

  col_select <- check_col_select(col_select)
  skip <- check_row_count(skip, "skip", allow_inf = FALSE)
  n_max <- check_row_count(n_max, "n_max", allow_inf = TRUE)

  if (!is.null(encoding) &&
      (!is.character(encoding) || length(encoding) != 1L || is.na(encoding))) {
    abort_argument("`encoding` must be NULL or a single encoding name, not ",
         deparse1(encoding))
  }
  if (!is.logical(strict) || length(strict) != 1L || is.na(strict)) {
    abort_argument("`strict` must be TRUE or FALSE, not ", deparse1(strict))
  }
  if (!is.logical(temporal_as_numbers) || length(temporal_as_numbers) != 1L ||
      is.na(temporal_as_numbers)) {
    abort_argument("`temporal_as_numbers` must be TRUE or FALSE, not ",
                   deparse1(temporal_as_numbers))
  }

  read_one <- function(file, skip, n_max) {
    df <- raise_classed(sas7bdat_read_impl(file, catalog, categorical, io_backend,
                                           threads, col_select, skip, n_max,
                                           encoding, strict,
                                           temporal_as_numbers))
    problem <- attr(df, "fastsas_warning", exact = TRUE)
    if (!is.null(problem)) {
      attr(df, "fastsas_warning") <- NULL
      warning(problem, call. = FALSE)
    }
    if (!is.null(id)) {
      df <- add_id_column(df, id, file)
    }
    df
  }

  df <- if (length(path) == 1L) {
    read_one(path, skip, n_max)
  } else {
    read_many(path, read_one, skip, n_max)
  }
  if (requireNamespace("tibble", quietly = TRUE)) {
    tibble::new_tibble(df, nrow = nrow(df))
  } else {
    df
  }
}

#' @rdname read_sas7bdat
#' @export
read_sas <- read_sas7bdat

# `threads` reaches Rust as an integer, so reject the values R would otherwise
# coerce silently: 0, negatives, fractions and NA all mean something the reader
# cannot act on.
# Several files read in order and stacked. `skip` and `n_max` count across them, and
# each file's row count is read from its header first, so a file wholly outside the
# range is not read.
read_many <- function(paths, read_one, skip, n_max) {
  if (!requireNamespace("vctrs", quietly = TRUE)) {
    abort_argument("reading several files needs the vctrs package, to stack ",
                   "labelled and factor columns: install.packages(\"vctrs\")")
  }
  pieces <- list()
  for (file in paths) {
    if (n_max <= 0) {
      break
    }
    rows <- sas_info(file)$n_rows
    if (skip >= rows) {
      skip <- skip - rows
      next
    }
    piece <- read_one(file, skip, n_max)
    skip <- 0
    n_max <- n_max - nrow(piece)
    pieces[[length(pieces) + 1L]] <- piece
  }
  if (length(pieces) == 0L) {
    return(read_one(paths[[1]], 0, 0))
  }
  df <- do.call(vctrs::vec_rbind, pieces)
  attr(df, "row.names") <- c(NA_integer_, -nrow(df))
  df
}

# `df` with a first column `name` holding `file` on every row.
add_id_column <- function(df, name, file) {
  rows <- nrow(df)
  out <- c(stats::setNames(list(rep(file, rows)), name), as.list(df))
  attributes(out) <- c(attributes(out),
                       list(row.names = c(NA_integer_, -rows), class = "data.frame"))
  out
}

check_col_select <- function(value) {
  if (is.null(value)) {
    return(NULL)
  }
  if (is.character(value) && length(value) > 0L && !anyNA(value)) {
    return(value)
  }
  if (is.numeric(value) && length(value) > 0L && !anyNA(value) &&
      all(value == trunc(value)) && all(value >= 1)) {
    return(as.double(value))
  }
  abort_argument("`col_select` must be NULL, column names, or 1-based column positions, not ",
       deparse1(value))
}

check_row_count <- function(value, arg, allow_inf) {
  ok <- is.numeric(value) && length(value) == 1L && !is.na(value) && value >= 0 &&
    if (is.infinite(value)) allow_inf else value == trunc(value)
  if (!ok) {
    abort_argument("`", arg, "` must be a whole number >= 0",
         if (allow_inf) " or Inf" else "", ", not ", deparse1(value))
  }
  as.double(value)
}

check_threads <- function(value) {
  if (is.null(value)) {
    return(NULL)
  }
  if (!is.numeric(value) || length(value) != 1L || is.na(value) ||
      value != trunc(value) || value < 1) {
    abort_argument("`threads` must be NULL or a whole number >= 1, not ",
         deparse1(value))
  }
  as.integer(value)
}

# Validate before crossing into Rust, so the message names the argument the
# caller actually passed. `path.expand()` reports every bad value as "invalid
# 'path' argument" regardless of which argument it came from.
check_path <- function(value, arg) {
  if (!is.character(value) || length(value) != 1L || is.na(value)) {
    abort_argument("`", arg, "` must be a single file path, not ",
         deparse1(value))
  }
  path.expand(value)
}
