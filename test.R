env_root <- Sys.getenv("SAS7BDAT_PARSER_RS_ROOT", unset = "")
if (nzchar(env_root)) {
  scripts_dir <- file.path(env_root, "scripts", "R")
} else {
  if (!requireNamespace("here", quietly = TRUE)) {
    stop("Install R package `here` or set SAS7BDAT_PARSER_RS_ROOT env var.")
  }
  scripts_dir <- here::here("scripts", "R")
}
rm(env_root)

script_sequence <- c(
  "00_update_package.R",
  "01_install_package.R",
  "02_test_package.R"
)

for (script in script_sequence) {
  source(file.path(scripts_dir, script))
}

rm(scripts_dir, script_sequence)
