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

savvy::savvy_update(r_package_path)
devtools::document(r_package_path)
