quiet_require <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop("Install R package `", pkg, "` to run this script.", call. = FALSE)
  }
}

quiet_require("here")
quiet_require("haven")
quiet_require("arrow")

normalize_root <- function(path) {
  normalizePath(path, winslash = "/", mustWork = TRUE)
}

parse_path_env <- function(env_name, default) {
  env_value <- Sys.getenv(env_name, unset = "")
  if (!nzchar(env_value)) {
    return(default)
  }
  unique(strsplit(env_value, .Platform$path.sep, fixed = TRUE)[[1]])
}

normalize_existing_roots <- function(paths) {
  existing <- Filter(dir.exists, paths)
  if (!length(existing)) {
    return(character(0))
  }
  unique(vapply(existing, normalize_root, character(1), USE.NAMES = FALSE))
}

relative_to_root <- function(path, root) {
  prefix <- paste0(root, "/")
  vapply(
    path,
    function(p) {
      if (identical(p, root)) {
        "."
      } else if (startsWith(p, prefix)) {
        substring(p, nchar(prefix) + 1L)
      } else {
        p
      }
    },
    character(1),
    USE.NAMES = FALSE
  )
}

collect_sas_files <- function(root) {
  files <- list.files(
    root,
    pattern = "\\.sas7bdat$",
    recursive = TRUE,
    full.names = TRUE,
    ignore.case = TRUE
  )
  files[file.info(files, extra_cols = FALSE)$isdir %in% c(FALSE, NA)]
}

collect_sas_info <- function(root) {
  files <- collect_sas_files(root)
  if (!length(files)) {
    return(NULL)
  }
  root_norm <- normalize_root(root)
  files_norm <- normalizePath(files, winslash = "/", mustWork = TRUE)
  rel <- relative_to_root(files_norm, root_norm)
  data.frame(
    sas_path = files_norm,
    sas_root = root_norm,
    rel_path = rel,
    dataset = tools::file_path_sans_ext(rel),
    stringsAsFactors = FALSE
  )
}

build_parquet_index <- function(roots) {
  files <- unlist(
    lapply(
      roots,
      function(root) {
        list.files(
          root,
          pattern = "\\.parquet$",
          recursive = TRUE,
          full.names = TRUE,
          ignore.case = TRUE
        )
      }
    ),
    use.names = FALSE
  )
  if (!length(files)) {
    return(list(files = character(0), by_basename = list()))
  }
  files_norm <- normalizePath(files, winslash = "/", mustWork = TRUE)
  by_basename <- split(
    files_norm,
    tolower(tools::file_path_sans_ext(basename(files_norm)))
  )
  list(files = files_norm, by_basename = by_basename)
}

find_parquet_match <- function(info, parquet_roots, parquet_index) {
  base <- tools::file_path_sans_ext(basename(info$rel_path))
  rel_dir <- dirname(info$rel_path)
  if (identical(rel_dir, ".") || identical(rel_dir, "")) {
    rel_dir <- ""
  }
  candidate_rel <- if (nzchar(rel_dir)) {
    file.path(rel_dir, paste0(base, ".parquet"))
  } else {
    paste0(base, ".parquet")
  }

  for (root in parquet_roots) {
    candidate <- file.path(root, candidate_rel)
    if (file.exists(candidate)) {
      return(list(
        path = normalizePath(candidate, winslash = "/", mustWork = TRUE),
        note = NULL
      ))
    }
  }

  base_matches <- parquet_index$by_basename[[tolower(base)]]
  if (is.null(base_matches) || !length(base_matches)) {
    return(NULL)
  }
  note <- if (length(base_matches) > 1) {
    paste0("multiple parquet candidates for ", base, "; using ", base_matches[[1]])
  } else {
    paste0("matched parquet by base name: ", base_matches[[1]])
  }
  list(path = base_matches[[1]], note = note)
}

normalize_df <- function(df) {
  df <- haven::zap_widths(haven::zap_missing(haven::zap_formats(haven::zap_labels(df))))
  df <- as.data.frame(df, stringsAsFactors = FALSE)
  for (name in names(df)) {
    column <- df[[name]]
    if (inherits(column, "integer64")) {
      if (requireNamespace("bit64", quietly = TRUE)) {
        column <- bit64::as.integer64(column)
        column <- as.numeric(column)
      } else {
        column <- as.numeric(column)
      }
    } else if (is.factor(column)) {
      column <- as.character(column)
    } else if (is.logical(column)) {
      column <- as.logical(column)
    } else if (inherits(column, "POSIXct")) {
      attr(column, "tzone") <- "UTC"
    } else if (is.integer(column)) {
      column <- as.numeric(column)
    }
    attr(column, "label") <- NULL
    attr(column, "format.sas") <- NULL
    attr(column, "display_width") <- NULL
    df[[name]] <- column
  }
  df
}

compare_frames <- function(left, right, numeric_tolerance = 1e-8) {
  left_norm <- normalize_df(left)
  right_norm <- normalize_df(right)

  left_only <- setdiff(names(left_norm), names(right_norm))
  right_only <- setdiff(names(right_norm), names(left_norm))
  common <- intersect(names(left_norm), names(right_norm))

  left_view <- left_norm[common]
  right_view <- right_norm[common]

  same_structure <- identical(
    lapply(left_view, class),
    lapply(right_view, class)
  )
  same_rows <- nrow(left_view) == nrow(right_view)
  same_cols <- length(common) == length(names(left_norm)) &&
    length(common) == length(names(right_norm))

  mismatched_cols <- character()
  if (same_rows && length(common) > 0) {
    columns_equal <- function(left_col, right_col) {
      if (is.numeric(left_col) && is.numeric(right_col)) {
        if (!all(is.na(left_col) == is.na(right_col))) {
          return(FALSE)
        }
        both_na <- is.na(left_col) & is.na(right_col)
        diff <- abs(left_col - right_col)
        diff[both_na] <- 0
        return(all(diff <= numeric_tolerance))
      }
      identical(left_col, right_col)
    }
    for (col in common) {
      if (!columns_equal(left_view[[col]], right_view[[col]])) {
        mismatched_cols <- c(mismatched_cols, col)
      }
    }
  }

  identical_values <- same_rows &&
    same_cols &&
    length(left_only) == 0 &&
    length(right_only) == 0 &&
    length(mismatched_cols) == 0

  list(
    identical = identical_values,
    left_only = left_only,
    right_only = right_only,
    mismatched_cols = mismatched_cols,
    same_rows = same_rows,
    same_cols = same_cols,
    same_structure = same_structure,
    row_count = nrow(left_norm),
    col_count = length(names(left_norm))
  )
}

root <- Sys.getenv("SAS7BDAT_PARSER_RS_ROOT", unset = "")
if (!nzchar(root)) {
  root <- here::here()
}

verbose_env <- Sys.getenv("HAVEN_TEST_VERBOSE", unset = "1")
verbose <- !(tolower(verbose_env) %in% c("0", "false", "no"))
emit <- function(...) {
  if (verbose) {
    cat(...)
  }
}

default_sas_roots <- c(
  file.path(root, "tests", "data_AHS2013"),
  file.path(root, "fixtures", "raw_data")
)
default_parquet_roots <- c(
  file.path(root, "ahs-parquet"),
  file.path(root, "parquet-fixtures")
)

sas_roots <- normalize_existing_roots(parse_path_env("HAVEN_TEST_SAS_ROOTS", default_sas_roots))
parquet_roots <- normalize_existing_roots(parse_path_env("HAVEN_TEST_PARQUET_ROOTS", default_parquet_roots))

if (!length(sas_roots)) {
  stop(
    "No SAS root directories found. Configure HAVEN_TEST_SAS_ROOTS or ensure defaults exist:\n  ",
    paste(default_sas_roots, collapse = "\n  ")
  )
}
if (!length(parquet_roots)) {
  stop(
    "No Parquet root directories found. Configure HAVEN_TEST_PARQUET_ROOTS or ensure defaults exist:\n  ",
    paste(default_parquet_roots, collapse = "\n  ")
  )
}

sas_info_list <- lapply(sas_roots, collect_sas_info)
sas_info_list <- Filter(Negate(is.null), sas_info_list)

if (!length(sas_info_list)) {
  stop("No SAS files found under: ", paste(sas_roots, collapse = ", "))
}

all_sas_info <- do.call(rbind, sas_info_list)
all_sas_info <- all_sas_info[order(all_sas_info$dataset), , drop = FALSE]

parquet_index <- build_parquet_index(parquet_roots)

results <- vector("list", nrow(all_sas_info))

emit("Comparing Haven vs Rust Parquet outputs\n")
emit("SAS root directories  :", paste(sas_roots, collapse = ", "), "\n")
emit("Parquet root dirs     :", paste(parquet_roots, collapse = ", "), "\n\n")

for (i in seq_len(nrow(all_sas_info))) {
  info <- all_sas_info[i, ]
  dataset <- info$dataset
  emit("=== ", dataset, " ===\n", sep = "")
  emit("  • SAS file     :", info$sas_path, "\n")

  parquet_match <- find_parquet_match(info, parquet_roots, parquet_index)
  if (is.null(parquet_match)) {
    emit("  ❌ Missing parquet counterpart\n\n")
    results[[i]] <- list(dataset = dataset, status = "missing_parquet")
    next
  }

  if (!is.null(parquet_match$note)) {
    emit("  ⚠️ ", parquet_match$note, "\n", sep = "")
  }
  emit("  • Parquet file :", parquet_match$path, "\n")

  haven_df <- tryCatch(haven::read_sas(info$sas_path), error = identity)
  parquet_df <- tryCatch(
    arrow::read_parquet(parquet_match$path, as_data_frame = TRUE),
    error = identity
  )

  if (inherits(haven_df, "error")) {
    emit("  ❌ Haven failed: ", conditionMessage(haven_df), "\n\n", sep = "")
    results[[i]] <- list(dataset = dataset, status = "haven_error")
    next
  }
  if (inherits(parquet_df, "error")) {
    emit("  ❌ Parquet read failed: ", conditionMessage(parquet_df), "\n\n", sep = "")
    results[[i]] <- list(dataset = dataset, status = "parquet_error")
    next
  }

  comparison <- compare_frames(haven_df, parquet_df)
  results[[i]] <- list(dataset = dataset, status = "ok", comparison = comparison)

  if (comparison$identical) {
    emit(
      sprintf(
        "  ✅ Match (%d rows, %d columns)\n",
        comparison$row_count,
        comparison$col_count
      )
    )
  } else {
    emit("  ⚠️ Differences detected\n")
    if (!comparison$same_rows) {
      emit("    • Row count mismatch: haven =", nrow(haven_df),
          "vs parquet =", nrow(parquet_df), "\n")
    }
    if (!comparison$same_cols) {
      emit("    • Column count mismatch: haven =", ncol(haven_df),
          "vs parquet =", ncol(parquet_df), "\n")
    }
    if (!comparison$same_structure) {
      emit("    • Column class differences detected\n")
    }
    if (length(comparison$left_only)) {
      emit("    • Columns only in haven:",
          paste(comparison$left_only, collapse = ", "), "\n")
    }
    if (length(comparison$right_only)) {
      emit("    • Columns only in parquet:",
          paste(comparison$right_only, collapse = ", "), "\n")
    }
    if (length(comparison$mismatched_cols)) {
      emit("    • Mismatched column values:",
          paste(comparison$mismatched_cols, collapse = ", "), "\n")
    }
  }

  emit("\n")
}

statuses <- vapply(results, function(x) x$status, character(1), USE.NAMES = FALSE)
summary_status <- table(statuses)
cat("Summary:\n")
for (name in names(summary_status)) {
  cat(" -", name, ":", summary_status[[name]], "\n")
}
