env_root <- Sys.getenv("SAS7BDAT_PARSER_RS_ROOT", unset = "")
if (nzchar(env_root)) {
  source(file.path(env_root, "scripts", "R", "utils.R"))
} else {
  if (!requireNamespace("here", quietly = TRUE)) {
    stop("Install R package `here` or set SAS7BDAT_PARSER_RS_ROOT env var.")
  }
  source(here::here("scripts", "R", "utils.R"))
}
rm(env_root)

library(SASreaderRUST)

project_root <- get_project_root()

default_sample <- file.path(project_root, "ahs2013n.sas7bdat")
sas_files_env <- Sys.getenv("SASREADER_RS_TEST_SAS_FILES", unset = "")
if (nzchar(sas_files_env)) {
  SAS_FILES <- strsplit(sas_files_env, .Platform$path.sep, fixed = TRUE)[[1]]
  SAS_FILES <- SAS_FILES[nzchar(SAS_FILES)]
} else {
  SAS_FILES <- default_sample
}

if (!length(SAS_FILES)) {
  stop("No SAS input files configured; set SASREADER_RS_TEST_SAS_FILES.")
}

SAS_FILES <- vapply(
  SAS_FILES,
  function(path) normalizePath(path, winslash = "/", mustWork = FALSE),
  character(1),
  USE.NAMES = FALSE
)

output_dir_env <- Sys.getenv("SASREADER_RS_TEST_OUTPUT_DIR", unset = "")
OUTPUT_DIR <- if (nzchar(output_dir_env)) {
  normalizePath(output_dir_env, winslash = "/", mustWork = FALSE)
} else {
  normalizePath(file.path(project_root, "sas7bdat-exports"), winslash = "/", mustWork = FALSE)
}

SINK <- tolower(Sys.getenv("SASREADER_RS_TEST_SINK", unset = "parquet"))

if (!SINK %in% c("parquet", "csv")) {
  stop("SINK must be either 'parquet' or 'csv'")
}

dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

for (sas_path in SAS_FILES) {
  if (!file.exists(sas_path)) {
    warning("Skipping missing file: ", sas_path)
    next
  }

  cat("\nProcessing:", sas_path, "\n", sep = "")
  rc <- sas_row_count(sas_path)
  cat("  Reported rows:", rc, "\n")

  base_name <- tools::file_path_sans_ext(basename(sas_path))
  output_path <- switch(
    SINK,
    parquet = file.path(OUTPUT_DIR, paste0(base_name, ".parquet")),
    csv = file.path(OUTPUT_DIR, paste0(base_name, ".csv"))
  )

  cat("  Writing to:", output_path, " using sink: ", SINK, "\n", sep = "")
  write_sas(sas_path, output_path, sink = SINK)

  if (identical(SINK, "parquet") && requireNamespace("arrow", quietly = TRUE)) {
    cat("  Preview via arrow::read_parquet():\n")
    preview <- arrow::read_parquet(output_path)
    print(utils::head(preview))
  }
}

cat("\nDone.\n")
