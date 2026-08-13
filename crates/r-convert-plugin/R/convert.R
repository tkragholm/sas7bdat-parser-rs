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
#' done <- file.exists(sub("\\.sas7bdat$", ".parquet", files))
#' convert_sas(files[!done], "out")
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
#' @return A data frame with one row per converted input — zero rows if nothing
#'   matched, which is not an error: `input`, `output`, `rows`,
#'   `columns`, `input_bytes`, `output_bytes`, `seconds`, `status` and `error`.
#'   Carries an `interrupted` attribute, and `discovered` — the number of files
#'   found, which exceeds `nrow()` when a run was stopped early.
#' @export
#' @examples
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
convert_sas <- function(input,
                        output = NULL,
                        recursive = TRUE,
                        flatten = FALSE,
                        overwrite = FALSE,
                        sink = c("parquet", "csv", "tsv"),
                        compression = c("zstd", "lz4", "snappy", "none"),
                        io_backend = c("auto", "mmap", "buffered"),
                        threads = NULL,
                        tmp_dir = NULL) {
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
  for (arg in c("recursive", "flatten", "overwrite")) {
    check_flag(get(arg), arg)
  }
  sink <- match.arg(sink)
  compression <- match.arg(compression)
  io_backend <- match.arg(io_backend)

  convert_sas_impl(
    input, output, recursive, flatten, overwrite,
    sink, compression, io_backend, check_threads(threads), tmp_dir
  )
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
