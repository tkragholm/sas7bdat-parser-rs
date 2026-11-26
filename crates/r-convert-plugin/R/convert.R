#' Convert SAS7BDAT files to Parquet, CSV or TSV
#'
#' Converts one file, several files, or every `.sas7bdat` under a directory,
#' mirroring the input tree into `output`. The data never enters R: conversion
#' happens entirely in Rust and only the per-file summary comes back, so trees far
#' larger than the session's memory convert fine.
#'
#' @section Failures are rows, not errors:
#' A file that cannot be read does not stop the run. It comes back with
#' `status = "error"` and the reason in `error`, and the remaining files still
#' convert. Only argument mistakes throw. Retry a failed subset by filtering the
#' returned frame and passing `result$input` back in.
#'
#' @section Interrupting a long run:
#' Ctrl-C is checked between files, so a large tree can be stopped without losing
#' the work already done — the frame returned covers the files that finished, and
#' its `interrupted` attribute is `TRUE`. Interrupting cannot corrupt the output:
#' each file is written to a temporary and moved into place only when complete, so
#' a partially converted file never appears at its destination.
#'
#' @section Resuming:
#' With `overwrite = FALSE` (the default) an existing output is an error rather
#' than a silent skip, which keeps a genuine name collision visible. To resume an
#' interrupted run, filter out what already converted:
#'
#' ```r
#' out <- file.path("out", sub("\\.sas7bdat$", ".parquet", basename(files)))
#' convert_sas(files[!file.exists(out)], "out", flatten = TRUE)
#' ```
#'
#' @param input Paths to `.sas7bdat` files, directories, or glob patterns.
#'   Directories are searched for `.sas7bdat` files.
#' @param output Output root. The tree below each input directory is recreated
#'   here. `NULL` writes each output beside its input.
#' @param recursive Search directories recursively. Default `TRUE`.
#' @param flatten Ignore the input tree and write every output directly into
#'   `output`. Default `FALSE`.
#' @param overwrite Replace existing outputs instead of reporting them as errors.
#' @param sink Output format: `"parquet"` (default), `"csv"` or `"tsv"`.
#' @param compression Parquet codec: `"zstd"` (default), `"lz4"`, `"snappy"` or
#'   `"none"`. Ignored for CSV and TSV.
#' @param io_backend How inputs are read: `"auto"` (default) memory-maps local
#'   files and reads network shares sequentially, `"mmap"` always memory-maps,
#'   `"buffered"` always reads sequentially. `"auto"` can only detect a share on
#'   Windows, so pass `"buffered"` for a mounted share elsewhere.
#' @param threads Decode threads per file. `NULL` (default) uses every logical
#'   core.
#' @param tmp_dir Stage outputs here before moving them into place. `NULL`
#'   (default) stages beside the destination, where the move is a rename. Point
#'   this at a local disk to keep the write off a network link until there is a
#'   finished file to send — at the cost of copying it afterwards.
#' @param encoding Read each file's text in this encoding instead of the one its
#'   header declares, for files whose header is wrong. A WHATWG label
#'   (`"utf-8"`, `"windows-1252"`) or SAS's name for a Windows code page
#'   (`"wlatin1"`). `NULL` (the default) uses the declared encoding.
#' @param strict If `TRUE`, a file with a cell that does not decode in its
#'   encoding fails, with the reason in `error`. By default such bytes are
#'   replaced.
#' @param labels If `TRUE`, store the SAS labels, formats and widths as JSON in
#'   the Parquet file's key-value metadata under `sas7bdat.metadata`. Ignored
#'   for CSV and TSV.
#' @param columns How the written columns are named and typed, from
#'   [sas_columns()]. `NULL` (the default) is `sas_columns()`: the names as read,
#'   and SAS times as Parquet `TIME`.
#' @param output_files One output path per input, in the same order, instead of
#'   names derived from `output`. Every input must then be a file, and `output`
#'   and `flatten` are not used.
#' @return A data frame with one row per converted input — zero rows if nothing
#'   matched, which is not an error: `input`, `output`, `rows`,
#'   `columns`, `input_bytes`, `output_bytes`, `seconds`, `status` and `error`.
#'   Carries an `interrupted` attribute, and `discovered` — the number of files
#'   found, which exceeds `nrow()` when a run was stopped early.
#' @export
#' @examples
#' # Convert a small file that ships with the package to Parquet
#' path <- system.file("extdata", "people.sas7bdat", package = "fastsasconvert")
#' out <- file.path(tempdir(), "fastsas-example")
#' result <- convert_sas(path, out, threads = 1)
#' result[, c("input", "output", "status", "rows")]
#'
#' \dontrun{
#' # Mirror a tree of SAS files into a tree of Parquet files.
#' result <- convert_sas("//server/share/sas", "D:/parquet")
#' sum(result$status == "error")
#'
#' # On a share where "auto" cannot tell the path is remote:
#' convert_sas("/mnt/share/sas", "out", io_backend = "buffered")
#'
#' # Retry only what failed.
#' convert_sas(result$input[result$status == "error"], "D:/parquet")
#' }
#'
#' # Lower-case names and the input's path in a column of its own
#' shaped <- convert_sas(
#'   path, out, overwrite = TRUE, threads = 1,
#'   columns = sas_columns(names = "lower", source_column = "source_file")
#' )
convert_sas <- function(input,
                        output = NULL,
                        recursive = TRUE,
                        flatten = FALSE,
                        overwrite = FALSE,
                        sink = c("parquet", "csv", "tsv"),
                        compression = c("zstd", "lz4", "snappy", "none"),
                        io_backend = c("auto", "mmap", "buffered"),
                        threads = NULL,
                        tmp_dir = NULL,
                        encoding = NULL,
                        strict = FALSE,
                        labels = FALSE,
                        columns = NULL,
                        output_files = NULL) {
  if (!is.character(input) || length(input) == 0L || anyNA(input)) {
    stop("`input` must be one or more file paths", call. = FALSE)
  }
  input <- path.expand(input)
  if (!is.null(output)) {
    output <- check_scalar_path(output, "output")
  }
  if (!is.null(tmp_dir)) {
    tmp_dir <- check_scalar_path(tmp_dir, "tmp_dir")
  }
  for (arg in c("recursive", "flatten", "overwrite", "strict", "labels")) {
    check_flag(get(arg), arg)
  }
  if (!is.null(encoding) &&
      (!is.character(encoding) || length(encoding) != 1L || is.na(encoding))) {
    stop("`encoding` must be NULL or a single encoding name, not ",
         deparse1(encoding), call. = FALSE)
  }
  sink <- match.arg(sink)
  compression <- match.arg(compression)
  io_backend <- match.arg(io_backend)
  if (is.null(columns)) {
    columns <- sas_columns()
  } else if (!inherits(columns, "fastsasconvert_columns")) {
    stop("`columns` must be NULL or made by sas_columns()", call. = FALSE)
  }
  if (!is.null(output_files)) {
    if (!is.character(output_files) || anyNA(output_files) ||
        length(output_files) != length(input)) {
      stop("`output_files` must be one path per input, not ",
           deparse1(output_files), call. = FALSE)
    }
    if (!is.null(output) || flatten) {
      stop("`output_files` names each destination, so `output` and `flatten` ",
           "must be left unset", call. = FALSE)
    }
    output_files <- path.expand(output_files)
  }

  convert_sas_impl(
    input, output, recursive, flatten, overwrite,
    sink, compression, io_backend, check_threads(threads), tmp_dir,
    encoding, strict, labels, output_files,
    columns$names, names(columns$rename), unname(columns$rename),
    names(columns$add), unname(columns$add),
    columns$source_column, columns$strings, columns$timestamps, columns$times,
    columns$time_unit
  )
}

#' Name and type the columns that convert_sas() writes
#'
#' Builds the `columns` argument of [convert_sas()]. The rules apply to every
#' file in a run, so one call fits files with different columns.
#'
#' @param names Case of the written column names: `"asis"` (default),
#'   `"lower"` or `"upper"`. `rename` overrides it for the columns it names.
#' @param rename A named character vector, `c(OLD = "new")`, writing column
#'   `OLD` as `new`. SAS names are case-insensitive, so `OLD` matches whatever
#'   case the file uses. A name that matches no column fails that file.
#' @param add A named character vector of constant columns, appended after the
#'   file's own, holding the same value in every row.
#' @param source_column Name of a constant column holding the input file's path,
#'   as given to [convert_sas()]. `NULL` adds none.
#' @param strings Arrow type of text columns in Parquet: `"string"` (default)
#'   or `"large_string"`.
#' @param timestamps Unit of timestamps in Parquet: `"s"`, `"ms"`, `"us"` or
#'   `"ns"`. `NULL` (default) keeps microseconds. A coarser unit rounds each
#'   instant down, towards the past.
#' @param times How SAS times are written in Parquet. `"time_of_day"` (the
#'   default) writes Parquet's `TIME`, which `arrow::read_parquet()` reads as
#'   `hms` and DuckDB as `TIME`. A file holding a time outside one day, such as an
#'   elapsed 25:30 or a negative time, then fails, and its `error` says so.
#'   `"duration"` keeps such values, but `arrow` reads them as `difftime` and
#'   readers other than Arrow see integers.
#' @param time_unit Unit for `times`: `"s"`, `"ms"`, `"us"` or `"ns"`. `NULL`
#'   (default) is `"us"` for times of day and `"ns"` for durations.
#' @return An object to pass as `columns`.
#' @details Two written columns may not share a name. A rule that would give
#'   them one fails that file, with both source columns named in `error`.
#'   `strings`, `timestamps` and `times` apply only to Parquet, and a CSV or TSV
#'   run that sets them fails.
#' @export
#' @examples
#' sas_columns(names = "lower", source_column = "source_file")
#' sas_columns(rename = c(PNR = "person_id"), times = "duration")
sas_columns <- function(names = c("asis", "lower", "upper"),
                        rename = NULL,
                        add = NULL,
                        source_column = NULL,
                        strings = c("string", "large_string"),
                        timestamps = NULL,
                        times = c("time_of_day", "duration"),
                        time_unit = NULL) {
  names <- match.arg(names)
  strings <- match.arg(strings)
  rename <- check_named(rename, "rename")
  add <- check_named(add, "add")
  if (!is.null(source_column) &&
      (!is.character(source_column) || length(source_column) != 1L ||
       is.na(source_column) || !nzchar(source_column))) {
    stop("`source_column` must be NULL or a single column name, not ",
         deparse1(source_column), call. = FALSE)
  }
  units <- c("s", "ms", "us", "ns")
  if (!is.null(timestamps)) {
    timestamps <- match.arg(timestamps, units)
  }
  times <- match.arg(times)
  if (!is.null(time_unit)) {
    time_unit <- match.arg(time_unit, units)
  }
  structure(
    list(names = names, rename = rename, add = add,
         source_column = source_column, strings = strings,
         timestamps = timestamps, times = times, time_unit = time_unit),
    class = "fastsasconvert_columns"
  )
}

#' @export
print.fastsasconvert_columns <- function(x, ...) {
  cat("<sas_columns>\n")
  cat("  names:", x$names, "\n")
  if (length(x$rename)) {
    cat("  rename:", paste(names(x$rename), x$rename, sep = " -> ", collapse = ", "), "\n")
  }
  if (length(x$add)) {
    cat("  add:", paste(names(x$add), collapse = ", "), "\n")
  }
  if (!is.null(x$source_column)) cat("  source_column:", x$source_column, "\n")
  cat("  strings:", x$strings, "\n")
  if (!is.null(x$timestamps)) cat("  timestamps:", x$timestamps, "\n")
  cat("  times:", x$times, if (!is.null(x$time_unit)) paste0("(", x$time_unit, ")"), "\n")
  invisible(x)
}

check_named <- function(value, arg) {
  if (is.null(value) || length(value) == 0L) {
    return(structure(character(), names = character()))
  }
  keys <- names(value)
  if (!is.character(value) || anyNA(value) || is.null(keys) || anyNA(keys) ||
      !all(nzchar(keys)) || anyDuplicated(keys)) {
    stop("`", arg, "` must be a named character vector with unique names, not ",
         deparse1(value), call. = FALSE)
  }
  value
}

# Validate here rather than letting values coerce on the way into Rust, so a
# mistake names the argument that caused it.

check_flag <- function(value, arg) {
  if (!is.logical(value) || length(value) != 1L || is.na(value)) {
    stop("`", arg, "` must be TRUE or FALSE, not ", deparse1(value), call. = FALSE)
  }
  invisible(value)
}

check_scalar_path <- function(value, arg) {
  if (!is.character(value) || length(value) != 1L || is.na(value)) {
    stop("`", arg, "` must be a single directory path, not ",
         deparse1(value), call. = FALSE)
  }
  path.expand(value)
}

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
