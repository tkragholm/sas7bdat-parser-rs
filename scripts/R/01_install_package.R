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

r_package_path <- get_r_package_path()

if (!requireNamespace("devtools", quietly = TRUE)) {
  stop("devtools must be installed to build the R package")
}

devtools::install_local(
  r_package_path,
  dependencies = FALSE,
  upgrade = "never",
  force = TRUE
)

library(SASreaderRUST)

if (!exists("write_sas")) {
  stop("write_sas() not found after installation; did you run scripts/R/00_update_package.R?")
}
