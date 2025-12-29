# Friendly R-side helpers around the Rust-backed readers.

#' Read a SAS7BDAT file as a data frame or tibble
#'
#' A lightweight R wrapper around the Rust-backed [read_sas()] that returns an
#' R `data.frame` (or tibble when available) and applies basic temporal type
#' coercions based on the column metadata emitted by the parser.
#'
#' @param path Path to a `.sas7bdat` file.
#' @param as_tibble If `TRUE` (default) and the `tibble` package is installed,
#'   return a tibble; otherwise falls back to a base `data.frame`.
#' @param name_repair Passed to `tibble::as_tibble()` when `as_tibble = TRUE`.
#' @param convert_datetimes If `TRUE`, coerce columns labelled as `date`,
#'   `datetime`, or `time` into `Date`, `POSIXct` (UTC), or `difftime`/`hms`
#'   objects respectively.
#' @param tz Time zone used when building `POSIXct` values for `datetime`
#'   columns. Defaults to `"UTC"`.
#' @param stringsAsFactors Passed to `as.data.frame()` when `as_tibble = FALSE`.
#'
#' @return A tibble or `data.frame` with `column_types` and `row_count`
#'   attributes preserved from the underlying Rust reader.
#' @export
read_sas_df <- function(
  path,
  as_tibble = TRUE,
  name_repair = "check_unique",
  convert_datetimes = TRUE,
  tz = "UTC",
  stringsAsFactors = FALSE
) {
  # Keep a handle to the low-level binding for testability/extensibility.
  raw_cols <- read_sas(path)
  column_types <- attr(raw_cols, "column_types", exact = TRUE)
  n_rows <- attr(raw_cols, "row_count", exact = TRUE)
  if (!length(n_rows) || is.na(n_rows)) {
    n_rows <- length(raw_cols[[1]])
  }

  if (convert_datetimes && length(column_types)) {
    raw_cols <- .sas_coerce_temporal_columns(raw_cols, column_types, tz = tz)
  }

  df <- if (isTRUE(as_tibble) && requireNamespace("tibble", quietly = TRUE)) {
    tibble::new_tibble(raw_cols, nrow = n_rows, .name_repair = name_repair)
  } else {
    if (isTRUE(as_tibble)) {
      message("Package 'tibble' not installed; returning a base data.frame instead.")
    }
    if (isTRUE(stringsAsFactors)) {
      raw_cols <- lapply(
        raw_cols,
        function(col) if (is.character(col)) factor(col) else col
      )
    }
    structure(
      raw_cols,
      class = "data.frame",
      row.names = .set_row_names(n_rows),
      stringsAsFactors = stringsAsFactors
    )
  }

  attr(df, "column_types") <- column_types
  attr(df, "row_count") <- n_rows
  df
}

.sas_coerce_temporal_columns <- function(columns, column_types, tz = "UTC") {
  limit <- min(length(columns), length(column_types))

  for (idx in seq_len(limit)) {
    kind <- column_types[[idx]]
    if (!length(kind)) next

    col <- columns[[idx]]
    if (identical(kind, "date")) {
      columns[[idx]] <- as.Date(col, origin = "1970-01-01")
      next
    }

    if (identical(kind, "datetime")) {
      columns[[idx]] <- as.POSIXct(col, origin = "1970-01-01", tz = tz)
      next
    }

    if (identical(kind, "time")) {
      if (requireNamespace("hms", quietly = TRUE)) {
        columns[[idx]] <- hms::hms(seconds = col)
      } else {
        columns[[idx]] <- structure(col, class = "difftime", units = "secs")
      }
    }
  }

  columns
}
