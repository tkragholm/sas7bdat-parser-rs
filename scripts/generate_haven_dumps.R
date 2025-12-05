#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(haven)
  library(jsonlite)
  library(hms)
})

SAS_EPOCH <- as.POSIXct("1960-01-01 00:00:00", tz = "UTC")

DATE_FORMATS <- c(
  "date",
  "date9",
  "yymmdd",
  "ddmmyy",
  "mmddyy",
  "mmddyy10",
  "e8601da",
  "minguo",
  "monname"
)
DATETIME_FORMATS <- c("datetime", "datetime20", "datetime22.3")
TIME_FORMATS <- c("time")

SKIP_FIXTURES <- c(
  "fixtures/raw_data/pandas/corrupt.sas7bdat",
  "fixtures/raw_data/pandas/zero_variables.sas7bdat",
  "fixtures/raw_data/csharp/54-class.sas7bdat",
  "fixtures/raw_data/csharp/54-cookie.sas7bdat",
  "fixtures/raw_data/csharp/charset_zpce.sas7bdat",
  "fixtures/raw_data/csharp/date_format_dtdate.sas7bdat",
  "fixtures/raw_data/csharp/date_formats.sas7bdat",
  "fixtures/raw_data/ahs2013/topical.sas7bdat"
)

main <- function() {
  args <- parse_args()
  script_dir <- dirname(get_script_path())
  repo_root <- normalizePath(
    file.path(script_dir, ".."),
    winslash = "/",
    mustWork = TRUE
  )

  base_output <- args$output_dir
  if (is.null(base_output)) {
    base_output <- file.path(repo_root, "tests", "reference")
  } else if (!is_absolute_path(base_output)) {
    base_output <- file.path(repo_root, base_output)
  }
  output_dir <- file.path(base_output, "haven")

  fixture_dirs <- args$fixtures
  if (length(fixture_dirs) == 0) {
    fixture_dirs <- c("fixtures/raw_data")
  }
  fixture_dirs <- vapply(
    fixture_dirs,
    function(path) {
      if (!is_absolute_path(path)) {
        normalizePath(
          file.path(repo_root, path),
          winslash = "/",
          mustWork = TRUE
        )
      } else {
        normalizePath(path, winslash = "/", mustWork = TRUE)
      }
    },
    character(1)
  )

  entries <- list()
  for (dir in fixture_dirs) {
    if (!dir.exists(dir)) {
      stop(sprintf("fixtures directory %s does not exist", dir))
    }
    files <- sort(list.files(
      dir,
      pattern = "\\.sas7bdat$",
      full.names = TRUE,
      recursive = TRUE
    ))
    for (path in files) {
      key <- normalized_key(path, repo_root)
      if (key %in% SKIP_FIXTURES) {
        message(sprintf("[skip] %s", key))
        next
      }
      if (!is.null(entries[[key]])) {
        stop(sprintf(
          "duplicate fixture key detected: %s clashes with %s",
          path,
          entries[[key]]
        ))
      }
      entries[[key]] <- normalizePath(path, winslash = "/", mustWork = TRUE)
    }
  }

  keys <- sort(names(entries))
  if (length(keys) == 0) {
    message("No fixtures found; nothing to do.")
    quit(save = "no", status = 0)
  }

  for (key in keys) {
    path <- entries[[key]]
    snapshot <- collect_snapshot(path)
    write_snapshot(output_dir, key, snapshot)
  }
}

collect_snapshot <- function(path) {
  data <- read_sas(path)
  columns <- names(data)
  format_hints <- lapply(data, function(column) attr(column, "format"))
  data <- strip_labelled_df(data)

  column_snapshots <- lapply(seq_along(columns), function(index) {
    convert_column(data[[index]], format_hints[[index]])
  })

  row_count <- nrow(data)
  rows <- lapply(seq_len(row_count), function(i) {
    lapply(column_snapshots, function(column) column[[i]])
  })

  list(
    columns = as.list(columns),
    row_count = row_count,
    rows = rows
  )
}

convert_column <- function(column, format_hint) {
  values <- as.list(column)
  lapply(values, function(value) convert_cell(value, format_hint))
}

convert_cell <- function(value, format_hint) {
  if (length(value) == 0 || (length(value) == 1 && is.na(value))) {
    return(list(kind = "missing", value = NULL))
  }

  if (inherits(value, "labelled")) {
    value <- strip_labelled(value)[1]
  }

  if (is.factor(value)) {
    value <- as.character(value)
  }

  if (inherits(value, "POSIXct")) {
    return(list(kind = "datetime", value = convert_datetime(value)))
  }

  if (inherits(value, "Date")) {
    return(list(kind = "date", value = convert_date(value)))
  }

  if (inherits(value, "hms") || inherits(value, "difftime")) {
    return(list(kind = "time", value = convert_time(value)))
  }

  if (is.raw(value)) {
    return(list(kind = "bytes", value = as.integer(value)))
  }

  if (is.character(value)) {
    return(list(kind = "string", value = value))
  }

  target_kind <- infer_kind_from_format(format_hint)
  formatted <- convert_with_format(value, target_kind)
  if (!is.null(formatted)) {
    return(formatted)
  }

  if (is.logical(value) || is.integer(value) || is.double(value)) {
    return(list(kind = "number", value = as.numeric(value)))
  }

  list(kind = "string", value = as.character(value))
}

convert_with_format <- function(value, target_kind) {
  if (is.null(target_kind)) {
    return(NULL)
  }

  if (target_kind == "datetime") {
    if (inherits(value, "POSIXct")) {
      return(list(kind = "datetime", value = convert_datetime(value)))
    }
    if (is.numeric(value)) {
      return(list(kind = "datetime", value = as.numeric(value)))
    }
    return(NULL)
  }

  if (target_kind == "date") {
    if (inherits(value, "Date")) {
      return(list(kind = "date", value = convert_date(value)))
    }
    if (is.numeric(value)) {
      return(list(kind = "date", value = as.numeric(value)))
    }
    return(NULL)
  }

  if (target_kind == "time") {
    if (inherits(value, "hms") || inherits(value, "difftime")) {
      return(list(kind = "time", value = convert_time(value)))
    }
    if (is.numeric(value)) {
      return(list(kind = "time", value = as.numeric(value)))
    }
    return(NULL)
  }

  NULL
}

convert_datetime <- function(value) {
  as.numeric(difftime(value, SAS_EPOCH, units = "secs"))
}

convert_date <- function(value) {
  datetime_value <- as.POSIXct(value, tz = "UTC")
  as.numeric(difftime(datetime_value, SAS_EPOCH, units = "days"))
}

convert_time <- function(value) {
  if (inherits(value, "difftime")) {
    return(as.numeric(value, units = "secs"))
  }
  as.numeric(value)
}

infer_kind_from_format <- function(format_hint) {
  if (is.null(format_hint) || is.na(format_hint)) {
    return(NULL)
  }
  fmt <- tolower(format_hint)
  if (fmt %in% DATETIME_FORMATS) {
    return("datetime")
  }
  if (fmt %in% TIME_FORMATS) {
    return("time")
  }
  if (fmt %in% DATE_FORMATS) {
    return("date")
  }
  NULL
}

write_snapshot <- function(base_output, key, snapshot) {
  components <- strsplit(key, "/", fixed = TRUE)[[1]]
  components[length(components)] <- sub(
    "\\.sas7bdat$",
    ".json",
    components[length(components)],
    ignore.case = TRUE
  )
  target <- do.call(file.path, c(list(base_output), as.list(components)))
  dir.create(dirname(target), recursive = TRUE, showWarnings = FALSE)
  write_json(snapshot, target, pretty = TRUE, auto_unbox = TRUE, digits = NA)
}

normalized_key <- function(path, repo_root) {
  relative <- ensure_relative(path, repo_root)
  gsub("\\\\", "/", relative)
}

ensure_relative <- function(path, base) {
  path <- normalizePath(path, winslash = "/", mustWork = TRUE)
  base <- normalizePath(base, winslash = "/", mustWork = TRUE)
  prefix <- paste0(base, "/")
  if (startsWith(path, prefix)) {
    substring(path, nchar(prefix) + 1)
  } else {
    path
  }
}

parse_args <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  fixtures <- character()
  output_dir <- NULL

  i <- 1
  while (i <= length(args)) {
    arg <- args[[i]]
    if (startsWith(arg, "--fixtures-dir=")) {
      fixtures <- c(fixtures, substring(arg, nchar("--fixtures-dir=") + 1))
    } else if (arg == "--fixtures-dir") {
      if (i == length(args)) {
        stop("--fixtures-dir requires a value")
      }
      i <- i + 1
      fixtures <- c(fixtures, args[[i]])
    } else if (startsWith(arg, "--output-dir=")) {
      output_dir <- substring(arg, nchar("--output-dir=") + 1)
    } else if (arg == "--output-dir") {
      if (i == length(args)) {
        stop("--output-dir requires a value")
      }
      i <- i + 1
      output_dir <- args[[i]]
    } else {
      stop(sprintf("unrecognized argument: %s", arg))
    }
    i <- i + 1
  }

  list(fixtures = fixtures, output_dir = output_dir)
}

is_absolute_path <- function(path) {
  grepl("^(/|[A-Za-z]:)", path)
}

get_script_path <- function() {
  cmd_args <- commandArgs(trailingOnly = FALSE)
  file_arg <- "--file="
  matches <- grep(file_arg, cmd_args, fixed = TRUE)
  if (length(matches) > 0) {
    return(normalizePath(
      sub(file_arg, "", cmd_args[matches[length(matches)]]),
      winslash = "/",
      mustWork = TRUE
    ))
  }
  normalizePath(".", winslash = "/", mustWork = TRUE)
}

strip_labelled_df <- function(df) {
  as.data.frame(lapply(df, strip_labelled))
}

strip_labelled <- function(x) {
  if (inherits(x, "labelled")) {
    attr(x, "labels") <- NULL
    attr(x, "label") <- NULL
    class(x) <- setdiff(class(x), "labelled")
  }
  x
}

main()
