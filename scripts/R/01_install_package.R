R_PACKAGE_PATH <- "/home/tkragholm/Development/sas7bdat-parser-rs/R-package"

if (!requireNamespace("devtools", quietly = TRUE)) {
  stop("devtools must be installed to build the R package")
}

devtools::install_local(R_PACKAGE_PATH, dependencies = FALSE, upgrade = "never", force = TRUE)

library(SASreaderRUST)

if (!exists("write_sas")) {
  stop("write_sas() not found after installation; did you run scripts/R/00_update_package.R?")
}
