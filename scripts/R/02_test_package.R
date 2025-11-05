library(SASreaderRUST)

# Configure the inputs to process and the sink to use.
SAS_FILES <- c(
  "/home/tkragholm/Downloads/AHS 2013 National PUF v2.0 Flat SAS/ahs2013n.sas7bdat"
)
OUTPUT_DIR <- "/home/tkragholm/Downloads/sas7bdat-exports"
SINK <- "parquet" # choose between "parquet" and "csv"

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

  cat("  Writing to:", output_path, "using sink:", SINK, "\n")
  write_sas(sas_path, output_path, sink = SINK)

  if (identical(SINK, "parquet") && requireNamespace("arrow", quietly = TRUE)) {
    cat("  Preview via arrow::read_parquet():\n")
    preview <- arrow::read_parquet(output_path)
    print(utils::head(preview))
  }
}

cat("\nDone.\n")
