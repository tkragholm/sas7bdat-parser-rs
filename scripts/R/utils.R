get_project_root <- function() {
  env_root <- Sys.getenv("SAS7BDAT_PARSER_RS_ROOT", unset = "")
  if (nzchar(env_root)) {
    return(normalizePath(env_root, winslash = "/", mustWork = TRUE))
  }

  if (!requireNamespace("here", quietly = TRUE)) {
    stop("Install R package `here` or set SAS7BDAT_PARSER_RS_ROOT env var.")
  }

  normalizePath(here::here(), winslash = "/", mustWork = TRUE)
}

get_r_package_path <- function() {
  file.path(get_project_root(), "R")
}
